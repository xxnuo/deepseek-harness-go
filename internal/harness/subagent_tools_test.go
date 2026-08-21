package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

type subagentToolTestProvider struct {
	name  string
	start func(context.Context, SubagentStartRequest) (*SubagentRun, error)
}

func (p *subagentToolTestProvider) Name() string { return p.name }
func (*subagentToolTestProvider) Capabilities() SubagentCapabilities {
	return NoSubagentStartCapabilities()
}
func (*subagentToolTestProvider) InheritsParentContext() bool { return false }
func (p *subagentToolTestProvider) Start(ctx context.Context, request SubagentStartRequest) (*SubagentRun, error) {
	return p.start(ctx, request)
}

func settledSubagentToolRun(id string, result SubagentResult, dispose func() error) *SubagentRun {
	_, cancel := context.WithCancel(context.Background())
	run := newSubagentRun(id, cancel, dispose)
	run.settle(result)
	return run
}

func TestSubagentDiagnosticFormattingAndUTF8Limit(t *testing.T) {
	run := settledSubagentToolRun("diagnostic-run", SubagentResult{
		StopReason: SubagentError,
		Diagnostic: strings.Repeat("\u754c", 2000),
		Output:     []ContentBlock{{Type: "text", Text: "partial answer"}},
	}, nil)
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Diagnostic) > maxSubagentDiagnosticBytes || !utf8.ValidString(result.Diagnostic) {
		t.Fatalf("limited diagnostic bytes=%d valid=%v", len(result.Diagnostic), utf8.ValidString(result.Diagnostic))
	}
	if !strings.HasSuffix(result.Diagnostic, diagnosticTruncationSuffix) {
		t.Fatalf("limited diagnostic does not end with truncation suffix")
	}

	result.Diagnostic = "provider detail"
	want := "subagent run failed\nDiagnostic: provider detail\nPartial output before the run ended:\npartial answer"
	if got := subagentStopError(result); got == nil || got.Error() != want {
		t.Fatalf("subagentStopError() = %v, want %q", got, want)
	}
	outcome := backgroundSubagentOutcome(result, nil)
	if outcome.Status != jobFailed || outcome.Detail != "error; diagnostic: provider detail" {
		t.Fatalf("background outcome = %#v", outcome)
	}
}

func TestSubagentProviderToolForegroundStrictSettlement(t *testing.T) {
	var disposed atomic.Int32
	provider := &subagentToolTestProvider{name: "strict", start: func(context.Context, SubagentStartRequest) (*SubagentRun, error) {
		return settledSubagentToolRun("strict-run", SubagentResult{
			StopReason: SubagentRefusal,
			Output:     []ContentBlock{{Type: "text", Text: "partial answer"}},
		}, func() error {
			disposed.Add(1)
			return errors.New("release broke")
		}), nil
	}}
	engine, err := New(
		WithPersistence(false), WithSubagentProviders(provider),
		WithSubagentTools(SubagentToolConfig{Provider: provider.Name(), ToolName: "subagent_strict"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	sessionID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "strict", "")
	if err != nil {
		t.Fatal(err)
	}
	engine.mu.RLock()
	tool := engine.tools["subagent_strict"]
	engine.mu.RUnlock()
	args, _ := json.Marshal(map[string]any{"description": "strict child", "prompt": "do work"})
	_, err = tool.Execute(context.Background(), ToolCall{Name: tool.Schema.Name, SessionID: sessionID, Arguments: args})
	if err == nil || !strings.Contains(err.Error(), "subagent declined the task") || !strings.Contains(err.Error(), "Partial output before the run ended:\npartial answer") || !strings.Contains(err.Error(), "dispose failed: release broke") {
		t.Fatalf("foreground error = %v", err)
	}
	if disposed.Load() != 1 {
		t.Fatalf("dispose count = %d", disposed.Load())
	}
}

func TestSubagentProviderToolForegroundCancellationCollectsPartialOutput(t *testing.T) {
	started := make(chan struct{})
	provider := &subagentToolTestProvider{name: "cancel-foreground", start: func(ctx context.Context, _ SubagentStartRequest) (*SubagentRun, error) {
		runCtx, cancel := context.WithCancel(ctx)
		run := newSubagentRun("cancel-run", cancel, nil)
		close(started)
		go func() {
			<-runCtx.Done()
			run.settle(SubagentResult{StopReason: SubagentAborted, Output: []ContentBlock{{Type: "text", Text: "saved partial"}}})
		}()
		return run, nil
	}}
	engine, err := New(
		WithPersistence(false), WithSubagentProviders(provider),
		WithSubagentTools(SubagentToolConfig{Provider: provider.Name(), ToolName: "subagent_cancel_foreground"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	sessionID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "cancel", "")
	if err != nil {
		t.Fatal(err)
	}
	engine.mu.RLock()
	tool := engine.tools["subagent_cancel_foreground"]
	engine.mu.RUnlock()
	args, _ := json.Marshal(map[string]any{"description": "cancel child", "prompt": "do work"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, executeErr := tool.Execute(ctx, ToolCall{Name: tool.Schema.Name, SessionID: sessionID, Arguments: args})
		done <- executeErr
	}()
	<-started
	cancel()
	select {
	case executeErr := <-done:
		if executeErr == nil || !strings.Contains(executeErr.Error(), "subagent run was cancelled") || !strings.Contains(executeErr.Error(), "saved partial") {
			t.Fatalf("foreground cancellation error = %v", executeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("foreground cancellation did not settle")
	}
}

func TestCodexSubagentToolRealProcessForegroundAndBackground(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test wrapper uses a POSIX shell")
	}
	dir := t.TempDir()
	writeHelperWrapper(t, filepath.Join(dir, "codex"), "codex")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	provider, err := NewCodexSubagentProvider(CodexSubagentConfig{DisposeGrace: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(
		WithWorkspace(dir), WithPersistence(false), WithSubagentProviders(provider),
		WithSubagentTools(SubagentToolConfig{Provider: "codex", ToolName: "subagent_codex"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	sessionID, err := engine.CreateSession(context.Background(), dir, "codex-tool", "")
	if err != nil {
		t.Fatal(err)
	}

	foreground := executeRegisteredTool(t, engine, "subagent_codex", sessionID, map[string]any{
		"description": "codex foreground", "prompt": "do foreground work",
	})
	if got := modelToolResultText(foreground); got != "codex-final" {
		t.Fatalf("foreground output = %q", got)
	}
	value, ok := foreground.Value.(map[string]any)
	if !ok || value["kind"] != "foreground" || value["runId"] == "" {
		t.Fatalf("foreground value = %#v", foreground.Value)
	}

	background := executeRegisteredTool(t, engine, "subagent_codex", sessionID, map[string]any{
		"description": "codex background", "prompt": "do background work", "run_in_background": true,
	})
	value, ok = background.Value.(map[string]any)
	jobID, _ := value["jobId"].(string)
	if !ok || value["kind"] != "background" || jobID == "" {
		t.Fatalf("background value = %#v", background.Value)
	}
	first := executeRegisteredTool(t, engine, "job_output", sessionID, map[string]any{"job_id": jobID, "wait": true, "timeout_ms": 5000})
	second := executeRegisteredTool(t, engine, "job_output", sessionID, map[string]any{"job_id": jobID})
	for index, result := range []ToolResult{first, second} {
		text := modelToolResultText(result)
		if !strings.Contains(text, "codex-final") || !strings.Contains(text, "[status: completed]") {
			t.Fatalf("job output %d = %q", index, text)
		}
	}
}

func TestClaudeCodeSubagentToolRealProcess(t *testing.T) {
	provider, err := NewClaudeCodeSubagentProvider(ClaudeCodeSubagentConfig{
		Executable: helperExecutable(t), Env: map[string]string{subagentHelperEnv: "1"}, DisposeGrace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	provider.executable = writeClaudeHelperWrapper(t)
	engine, err := New(
		WithPersistence(false), WithSubagentProviders(provider),
		WithSubagentTools(SubagentToolConfig{Provider: "claude-code", ToolName: "subagent_claude_code"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	sessionID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "claude-tool", "")
	if err != nil {
		t.Fatal(err)
	}
	result := executeRegisteredTool(t, engine, "subagent_claude_code", sessionID, map[string]any{
		"description": "claude foreground", "prompt": "hello claude",
	})
	if got := modelToolResultText(result); got != "claude|hello claude|args-ok|entry=sdk-ts" {
		t.Fatalf("Claude Code tool output = %q", got)
	}
}

func TestBackgroundSubagentStartupCancellationAndFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		startError error
		wantStatus string
	}{
		{name: "cancelled", startError: context.Canceled, wantStatus: "killed"},
		{name: "provider cancellation", startError: errors.New("subagent-codex: request was aborted before run publication"), wantStatus: "killed"},
		{name: "rollback failure", startError: errors.New("rollback failed"), wantStatus: "failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
			cause := make(chan error, 1)
			provider := &subagentToolTestProvider{name: "blocking", start: func(ctx context.Context, _ SubagentStartRequest) (*SubagentRun, error) {
				close(started)
				<-ctx.Done()
				cause <- context.Cause(ctx)
				return nil, test.startError
			}}
			engine, err := New(
				WithPersistence(false), WithSubagentProviders(provider),
				WithSubagentTools(SubagentToolConfig{Provider: provider.Name(), ToolName: "subagent_blocking"}),
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = engine.Close() })
			sessionID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "blocking", "")
			if err != nil {
				t.Fatal(err)
			}
			background := executeRegisteredTool(t, engine, "subagent_blocking", sessionID, map[string]any{
				"description": "blocking child", "prompt": "wait", "run_in_background": true,
			})
			jobID := background.Value.(map[string]any)["jobId"].(string)
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("provider startup did not begin")
			}
			executeRegisteredTool(t, engine, "job_kill", sessionID, map[string]any{"job_id": jobID, "reason": "stop startup"})
			select {
			case got := <-cause:
				if got == nil || got.Error() != "stop startup" {
					t.Fatalf("cancel cause = %v", got)
				}
			case <-time.After(time.Second):
				t.Fatal("provider did not observe cancellation")
			}
			output := executeRegisteredTool(t, engine, "job_output", sessionID, map[string]any{"job_id": jobID, "wait": true, "timeout_ms": 1000})
			text := modelToolResultText(output)
			if !strings.Contains(text, "[status: "+test.wantStatus) {
				t.Fatalf("job output = %q", text)
			}
			if test.wantStatus == "failed" && !strings.Contains(text, "rollback failed") {
				t.Fatalf("job failure detail = %q", text)
			}
		})
	}
}

func TestOptionalProductSubagentToolDoesNotLeakIntoShippedPreset(t *testing.T) {
	provider := &subagentToolTestProvider{name: "codex", start: func(context.Context, SubagentStartRequest) (*SubagentRun, error) {
		return settledSubagentToolRun("unused", SubagentResult{StopReason: SubagentCompleted}, nil), nil
	}}
	engine := newIntegrationEngine(t)
	if err := engine.RegisterSubagentProvider(provider); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterSubagentTool(SubagentToolConfig{Provider: "codex", ToolName: "subagent_codex"}); err != nil {
		t.Fatal(err)
	}
	plainID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "plain-product-tool", "")
	if err != nil {
		t.Fatal(err)
	}
	standardID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "standard-product-tool", "standard")
	if err != nil {
		t.Fatal(err)
	}
	visible := func(id string) bool {
		session, _ := engine.getSession(id)
		tools, toolErr := engine.toolsForSession(session)
		if toolErr != nil {
			t.Fatal(toolErr)
		}
		for _, tool := range tools {
			if tool.Name == "subagent_codex" {
				return true
			}
		}
		return false
	}
	if !visible(plainID) || visible(standardID) {
		t.Fatalf("subagent_codex visibility plain=%v standard=%v", visible(plainID), visible(standardID))
	}
}
