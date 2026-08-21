package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type hookLoopProvider struct {
	mu          sync.Mutex
	completions []Completion
	requests    []ChatRequest
}

func (p *hookLoopProvider) ID() string   { return "hook-loop" }
func (p *hookLoopProvider) Name() string { return "Hook Loop" }
func (p *hookLoopProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: p.ID(), Name: p.Name()}}, nil
}
func (p *hookLoopProvider) Complete(_ context.Context, request ChatRequest, onDelta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	index := len(p.requests)
	p.requests = append(p.requests, request)
	if index >= len(p.completions) {
		p.mu.Unlock()
		return Completion{Text: "done", Finish: "stop"}, nil
	}
	completion := p.completions[index]
	p.mu.Unlock()
	if completion.Text != "" {
		if err := onDelta(Delta{Text: completion.Text, Finish: completion.Finish}); err != nil {
			return Completion{}, err
		}
	}
	return completion, nil
}
func (p *hookLoopProvider) snapshot() []ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...)
}

func newHookRuntimeEngine(t *testing.T, dialect HookDialect, hooks map[string]any, completions ...Completion) (*Engine, *hookLoopProvider, string) {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "hooks.json")
	data, err := json.Marshal(map[string]any{"hooks": hooks})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	provider := &hookLoopProvider{completions: completions}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dir, dir
	cfg.Persist = false
	cfg.Provider, cfg.Model = provider.ID(), provider.ID()
	cfg.SessionTitleLLM.Enabled = false
	cfg.Hooks = []HookBridgeConfig{{Dialect: dialect, ConfigPath: configPath, Model: "test-model", DefaultTimeout: 3 * time.Second}}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	engine.RegisterProvider(provider)
	t.Cleanup(func() { _ = engine.Close() })
	return engine, provider, dir
}

func writeHookRuntimeScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/usr/bin/env bash\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	return strconv.Quote(path)
}

func TestHookRuntimeRejectsPromptBeforeAdmission(t *testing.T) {
	dir := t.TempDir()
	deny := writeHookRuntimeScript(t, dir, "deny.sh", "echo 'prompt denied' >&2\nexit 2\n")
	engine, provider, _ := newHookRuntimeEngine(t, HookDialectClaudeCode, map[string]any{
		"UserPromptSubmit": []any{map[string]any{"hooks": []any{map[string]any{"command": deny}}}},
	}, Completion{Text: "must not run", Finish: "stop"})
	id, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "blocked"}}})
	if !errors.Is(err, errHookPromptRejected) {
		t.Fatalf("Run() error = %v", err)
	}
	if len(provider.snapshot()) != 0 {
		t.Fatal("provider ran after UserPromptSubmit rejection")
	}
	session := mustSession(t, engine, id)
	session.mu.Lock()
	defer session.mu.Unlock()
	var sequence []string
	for _, event := range session.Events {
		if event.Type == "user/message" || event.Type == "step/start" {
			t.Fatalf("blocked prompt leaked event %q", event.Type)
		}
		if event.Type == "turn/start" || event.Type == "hook/invoked" || event.Type == "hook/result" || event.Type == "turn/end" {
			sequence = append(sequence, event.Type)
		}
	}
	if strings.Join(sequence, ",") != "turn/start,hook/invoked,hook/result,turn/end" {
		t.Fatalf("hook event order = %v", sequence)
	}
}

func TestHookRuntimeToolDecisionsAndContext(t *testing.T) {
	t.Run("pre deny", func(t *testing.T) {
		dir := t.TempDir()
		deny := writeHookRuntimeScript(t, dir, "deny.sh", "echo 'tool denied' >&2\nexit 2\n")
		engine, _, _ := newHookRuntimeEngine(t, HookDialectClaudeCode, map[string]any{
			"PreToolUse": []any{map[string]any{"matcher": "hook_unit", "hooks": []any{map[string]any{"command": deny}}}},
		}, Completion{ToolCalls: []ToolCall{{ID: "call-1", Name: "hook_unit", Arguments: json.RawMessage(`{}`)}}, Finish: "tool_calls"}, Completion{Text: "done", Finish: "stop"})
		ran := false
		if err := engine.RegisterTool(Tool{Schema: ToolSchema{Name: "hook_unit", Parameters: map[string]any{"type": "object"}}, Execute: func(context.Context, ToolCall) (ToolResult, error) {
			ran = true
			return textToolResult("raw"), nil
		}}); err != nil {
			t.Fatal(err)
		}
		id, _ := engine.CreateSession(context.Background(), engine.Config().Workspace, "", "")
		if _, err := engine.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}}); err != nil {
			t.Fatal(err)
		}
		if ran {
			t.Fatal("denied tool executed")
		}
		if result := firstToolResult(mustSession(t, engine, id)); !result.IsError || !strings.Contains(blockText(result.Content), "tool denied") {
			t.Fatalf("tool result = %#v", result)
		}
	})

	t.Run("post block and context", func(t *testing.T) {
		dir := t.TempDir()
		post := writeHookRuntimeScript(t, dir, "post.sh", `echo '{"decision":"block","reason":"retry safely","hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":"post note"}}'`+"\n")
		engine, provider, _ := newHookRuntimeEngine(t, HookDialectClaudeCode, map[string]any{
			"PostToolUse": []any{map[string]any{"matcher": "hook_unit", "hooks": []any{map[string]any{"command": post}}}},
		}, Completion{ToolCalls: []ToolCall{{ID: "call-1", Name: "hook_unit", Arguments: json.RawMessage(`{}`)}}, Finish: "tool_calls"}, Completion{Text: "done", Finish: "stop"})
		ran := false
		if err := engine.RegisterTool(Tool{Schema: ToolSchema{Name: "hook_unit", Parameters: map[string]any{"type": "object"}}, Execute: func(context.Context, ToolCall) (ToolResult, error) {
			ran = true
			return textToolResult("raw output"), nil
		}}); err != nil {
			t.Fatal(err)
		}
		id, _ := engine.CreateSession(context.Background(), engine.Config().Workspace, "", "")
		if _, err := engine.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}}); err != nil {
			t.Fatal(err)
		}
		if !ran {
			t.Fatal("PostToolUse ran before the tool")
		}
		result := firstToolResult(mustSession(t, engine, id))
		if !result.IsError || blockText(result.Content) != "retry safely" {
			t.Fatalf("rewritten result = %#v", result)
		}
		requests := provider.snapshot()
		if len(requests) != 2 || !strings.Contains(requests[1].Messages[len(requests[1].Messages)-1].Content, "post note") {
			t.Fatalf("post context missing from second request: %#v", requests)
		}
		session := mustSession(t, engine, id)
		session.mu.Lock()
		defer session.mu.Unlock()
		resultIndex, contextIndex := -1, -1
		for index, event := range session.Events {
			if event.Type == "tool/result" {
				resultIndex = index
			}
			if event.Type == "user/message" && eventSourceKind(event.Data) == "plugin" && strings.Contains(contentValueText(event.Data), "post note") {
				contextIndex = index
			}
		}
		if resultIndex < 0 || contextIndex <= resultIndex {
			t.Fatalf("tool/context event order = %d/%d", resultIndex, contextIndex)
		}
	})
}

func TestHookRuntimeAskFailsClosedWithoutAnswerer(t *testing.T) {
	dir := t.TempDir()
	ask := writeHookRuntimeScript(t, dir, "ask.sh", `echo '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"ask","permissionDecisionReason":"needs approval"}}'`+"\n")
	engine, _, _ := newHookRuntimeEngine(t, HookDialectClaudeCode, map[string]any{
		"PreToolUse": []any{map[string]any{"matcher": "hook_unit", "hooks": []any{map[string]any{"command": ask}}}},
	}, Completion{ToolCalls: []ToolCall{{ID: "call-1", Name: "hook_unit", Arguments: json.RawMessage(`{}`)}}, Finish: "tool_calls"}, Completion{Text: "done", Finish: "stop"})
	ran := false
	if err := engine.RegisterTool(Tool{Schema: ToolSchema{Name: "hook_unit", Parameters: map[string]any{"type": "object"}}, Execute: func(context.Context, ToolCall) (ToolResult, error) {
		ran = true
		return textToolResult("raw"), nil
	}}); err != nil {
		t.Fatal(err)
	}
	id, _ := engine.CreateSession(context.Background(), engine.Config().Workspace, "", "")
	if _, err := engine.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}}); err != nil {
		t.Fatal(err)
	}
	if ran {
		t.Fatal("tool executed without approval answerer")
	}
	session := mustSession(t, engine, id)
	result := firstToolResult(session)
	if !result.IsError || !strings.Contains(blockText(result.Content), "needs approval") {
		t.Fatalf("approval result = %#v", result)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	var audit []string
	for _, event := range session.Events {
		if event.Type == "approval/asked" || event.Type == "approval/decided" {
			audit = append(audit, event.Type)
			if event.Type == "approval/decided" {
				data, _ := event.Data.(map[string]any)
				if data["outcome"] != "unavailable" {
					t.Fatalf("approval outcome = %#v", data)
				}
			}
		}
	}
	if strings.Join(audit, ",") != "approval/asked,approval/decided" {
		t.Fatalf("approval audit = %v", audit)
	}
}

func TestHookRuntimeStopForcesAnotherStep(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "stop-fired")
	stop := writeHookRuntimeScript(t, dir, "stop.sh", "if [ -e "+strconv.Quote(marker)+" ]; then exit 0; fi\ntouch "+strconv.Quote(marker)+"\necho 'continue with the goal' >&2\nexit 2\n")
	engine, provider, _ := newHookRuntimeEngine(t, HookDialectCodex, map[string]any{
		"Stop": []any{map[string]any{"hooks": []any{map[string]any{"command": stop}}}},
	}, Completion{Text: "first", Finish: "stop"}, Completion{Text: "second", Finish: "stop"})
	id, _ := engine.CreateSession(context.Background(), engine.Config().Workspace, "", "")
	text, err := engine.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}})
	if err != nil || text != "second" {
		t.Fatalf("Run() = %q, %v", text, err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 || !strings.Contains(requests[1].Messages[len(requests[1].Messages)-1].Content, "continue with the goal") {
		t.Fatalf("stop steering missing: %#v", requests)
	}
}

func TestHookRuntimeSessionAndSubagentLifecycle(t *testing.T) {
	dir := t.TempDir()
	startMarker, stopMarker := filepath.Join(dir, "sub-start"), filepath.Join(dir, "sub-stop")
	session := writeHookRuntimeScript(t, dir, "session.sh", `echo '{"hookSpecificOutput":{"hookEventName":"SessionStart","additionalContext":"startup context"}}'`+"\n")
	subStart := writeHookRuntimeScript(t, dir, "sub-start.sh", "touch "+strconv.Quote(startMarker)+"\n")
	subStop := writeHookRuntimeScript(t, dir, "sub-stop.sh", "touch "+strconv.Quote(stopMarker)+"\n")
	engine, _, _ := newHookRuntimeEngine(t, HookDialectClaudeCode, map[string]any{
		"SessionStart":  []any{map[string]any{"matcher": "startup", "hooks": []any{map[string]any{"command": session}}}},
		"SubagentStart": []any{map[string]any{"hooks": []any{map[string]any{"command": subStart}}}},
		"SubagentStop":  []any{map[string]any{"hooks": []any{map[string]any{"command": subStop}}}},
	}, Completion{Text: "child done", Finish: "stop"})
	parent, _ := engine.CreateSession(context.Background(), engine.Config().Workspace, "", "")
	waitHookRuntime(t, func() bool {
		s := mustSession(t, engine, parent)
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, event := range s.Events {
			if event.Type == "hook/invoked" || event.Type == "hook/result" {
				t.Fatalf("SessionStart wrote %s outside a turn", event.Type)
			}
			if event.Type == "user/message" && strings.Contains(contentValueText(event.Data), "startup context") {
				return true
			}
		}
		return false
	})
	child, err := engine.CreateSubagent(context.Background(), parent, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), child, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}}); err != nil {
		t.Fatal(err)
	}
	waitHookRuntime(t, func() bool {
		_, startErr := os.Stat(startMarker)
		_, stopErr := os.Stat(stopMarker)
		return startErr == nil && stopErr == nil
	})
}

func firstToolResult(session *Session) ContentBlock {
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if event.Type != "tool/result" {
			continue
		}
		message := nestedMessage(event.Data)
		blocks := contentBlocks(message["content"])
		if len(blocks) == 1 && blocks[0].Type == "tool-result" {
			return blocks[0]
		}
	}
	return ContentBlock{}
}

func waitHookRuntime(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("hook effect did not arrive before timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
