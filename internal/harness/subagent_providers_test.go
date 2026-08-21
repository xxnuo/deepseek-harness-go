package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const subagentHelperEnv = "GO_WANT_SUBAGENT_PROVIDER_HELPER"

func TestACPSubagentProviderRealProcess(t *testing.T) {
	t.Setenv("AMBIENT_TOKEN", "must-not-leak")
	cwd := t.TempDir()
	provider, err := NewACPSubagentProvider(ACPSubagentConfig{
		Command: helperExecutable(t), Args: helperArgs("acp"), Permission: "allow",
		Env:             map[string]string{subagentHelperEnv: "1", "EXPLICIT_TOKEN": "explicit", "DSH_CHILD_FACT": "child"},
		DisposeEOFGrace: time.Second, DisposeGrace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := provider.Start(context.Background(), SubagentStartRequest{
		CWD: cwd, Prompt: []ContentBlock{{Type: "text", Text: "one"}, {Type: "image"}, {Type: "text", Text: "two"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Dispose() })
	result, err := run.Wait(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentCompleted {
		t.Fatalf("stop reason = %q", result.StopReason)
	}
	want := fmt.Sprintf("acp|%s|onetwo|ambient=|explicit=explicit|dsh=child|permission=yes", cwd)
	if got := contentValueText(result.Output); got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestACPSubagentProviderCancellation(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	provider, err := NewACPSubagentProvider(ACPSubagentConfig{
		Command: helperExecutable(t), Args: helperArgs("acp-hang"),
		Env:             map[string]string{subagentHelperEnv: "1", "SUBAGENT_READY_FILE": ready},
		DisposeEOFGrace: time.Second, DisposeGrace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	run, err := provider.Start(ctx, SubagentStartRequest{CWD: t.TempDir(), Prompt: []ContentBlock{{Type: "text", Text: "wait"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitForTestFile(t, ready)
	cancel()
	result, err := run.Wait(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentAborted || contentValueText(result.Output) != "partial" {
		t.Fatalf("result = %#v", result)
	}
	if err := run.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestCodexSubagentProviderRealProtocol(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test wrapper uses a POSIX shell")
	}
	dir := t.TempDir()
	launcher := filepath.Join(dir, "codex")
	writeHelperWrapper(t, launcher, "codex")
	provider, err := NewCodexSubagentProvider(CodexSubagentConfig{Executable: launcher, DisposeGrace: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	run, err := provider.Start(context.Background(), SubagentStartRequest{
		CWD: dir, Prompt: []ContentBlock{{Type: "text", Text: "first"}, {Type: "text", Text: "second"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Dispose() })
	result, err := run.Wait(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentCompleted || contentValueText(result.Output) != "codex-final" {
		t.Fatalf("result = %#v", result)
	}
}

func TestCodexSubagentProviderPassesSelectedPermissionModeToRealProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test wrapper uses a POSIX shell")
	}
	dir := t.TempDir()
	launcher := filepath.Join(dir, "codex")
	writeHelperWrapper(t, launcher, "codex")
	t.Setenv("SUBAGENT_EXPECT_CODEX_PERMISSION_MODE", string(CodexPermissionDangerouslyBypassApprovalsAndSandbox))
	provider, err := NewCodexSubagentProvider(CodexSubagentConfig{
		ProviderName: "codex-bypass", PermissionMode: CodexPermissionDangerouslyBypassApprovalsAndSandbox, Executable: launcher, DisposeGrace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := provider.Start(context.Background(), SubagentStartRequest{CWD: dir, Prompt: []ContentBlock{{Type: "text", Text: "test bypass"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Dispose() })
	result, err := run.Wait(testContext(t))
	if err != nil || result.StopReason != SubagentCompleted {
		t.Fatalf("result = %#v, %v", result, err)
	}
}

func TestProductSubagentProviderNamesAndPermissionModes(t *testing.T) {
	codex, err := NewCodexSubagentProvider(CodexSubagentConfig{
		ProviderName: "codex-safe", PermissionMode: CodexPermissionApproveForMe,
	})
	if err != nil {
		t.Fatal(err)
	}
	if codex.Name() != "codex-safe" || codex.permissionMode != CodexPermissionApproveForMe {
		t.Fatalf("codex provider = %#v", codex)
	}
	for mode, want := range map[CodexPermissionMode]map[string]any{
		CodexPermissionApproveForMe:                         {"approvalPolicy": "on-request", "approvalsReviewer": "auto_review", "sandbox": "workspace-write"},
		CodexPermissionDangerouslyBypassApprovalsAndSandbox: {"approvalPolicy": "never", "sandbox": "danger-full-access"},
	} {
		for key, value := range want {
			if got := codexThreadPermissionParams(mode)[key]; got != value {
				t.Fatalf("%s thread permission %s = %#v, want %#v", mode, key, got, value)
			}
		}
	}
	claude, err := NewClaudeCodeSubagentProvider(ClaudeCodeSubagentConfig{
		ProviderName: "claude-safe", PermissionMode: ClaudeCodePermissionAcceptEdits,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claude.Name() != "claude-safe" || claude.permissionMode != ClaudeCodePermissionAcceptEdits {
		t.Fatalf("Claude provider = %#v", claude)
	}
	for _, test := range []struct {
		name string
		run  func() error
	}{
		{"codex", func() error {
			_, err := NewCodexSubagentProvider(CodexSubagentConfig{PermissionMode: "unsafe"})
			return err
		}},
		{"claude", func() error {
			_, err := NewClaudeCodeSubagentProvider(ClaudeCodeSubagentConfig{PermissionMode: "unsafe"})
			return err
		}},
	} {
		t.Run("reject invalid "+test.name, func(t *testing.T) {
			if err := test.run(); err == nil || !strings.Contains(err.Error(), "permissionMode") {
				t.Fatalf("invalid permission mode error = %v", err)
			}
		})
	}
}

func TestClaudeCodeSubagentProviderRealProcess(t *testing.T) {
	provider, err := NewClaudeCodeSubagentProvider(ClaudeCodeSubagentConfig{
		Executable: helperExecutable(t), Env: map[string]string{subagentHelperEnv: "1"}, DisposeGrace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The test binary needs its test selector before the fixed Claude flags.
	provider.executable = writeClaudeHelperWrapper(t)
	run, err := provider.Start(context.Background(), SubagentStartRequest{
		CWD: t.TempDir(), Prompt: []ContentBlock{{Type: "text", Text: "hello "}, {Type: "text", Text: "claude"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Dispose() })
	result, err := run.Wait(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentCompleted || contentValueText(result.Output) != "claude|hello claude|args-ok|entry=sdk-ts" {
		t.Fatalf("result = %#v", result)
	}
}

func TestClaudeCodeSubagentProviderPassesSelectedPermissionModeToRealProcess(t *testing.T) {
	t.Setenv("SUBAGENT_EXPECT_CLAUDE_PERMISSION_MODE", string(ClaudeCodePermissionBypassPermissions))
	provider, err := NewClaudeCodeSubagentProvider(ClaudeCodeSubagentConfig{
		ProviderName: "claude-bypass", PermissionMode: ClaudeCodePermissionBypassPermissions,
		Executable: helperExecutable(t), Env: map[string]string{subagentHelperEnv: "1"}, DisposeGrace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	provider.executable = writeClaudeHelperWrapper(t)
	run, err := provider.Start(context.Background(), SubagentStartRequest{CWD: t.TempDir(), Prompt: []ContentBlock{{Type: "text", Text: "test bypass"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Dispose() })
	result, err := run.Wait(testContext(t))
	if err != nil || result.StopReason != SubagentCompleted {
		t.Fatalf("result = %#v, %v", result, err)
	}
}

func TestClaudeCodeSubagentProviderRealProductLocalAPI(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("fixed upstream checkout vendors the linux-amd64 Claude Code binary")
	}
	executable := filepath.Join(
		"deepseek-harness", "node_modules", ".pnpm", "@anthropic-ai+claude-agent-sdk-linux-x64@0.3.220",
		"node_modules", "@anthropic-ai", "claude-agent-sdk-linux-x64", "claude",
	)
	executable, err := filepath.Abs(executable)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(executable); err != nil {
		t.Skipf("vendored Claude Code binary unavailable: %v", err)
	}

	task := "Return the local fixture sentinel exactly."
	sentinel := "GO_REAL_CLAUDE_CODE_SENTINEL"
	type observedRequest struct {
		Header http.Header
		Body   map[string]any
	}
	requests := make(chan observedRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/messages") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)
		requests <- observedRequest{Header: r.Header.Clone(), Body: payload}
		w.Header().Set("Content-Type", "text/event-stream")
		model, _ := payload["model"].(string)
		writeClaudeSSE(w, "message_start", map[string]any{"type": "message_start", "message": map[string]any{
			"id": "msg_fixture", "type": "message", "role": "assistant", "model": model,
			"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
			"usage": map[string]any{"input_tokens": 7, "output_tokens": 0, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0},
		}})
		writeClaudeSSE(w, "content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}})
		writeClaudeSSE(w, "content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": sentinel}})
		writeClaudeSSE(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
		writeClaudeSSE(w, "message_delta", map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil}, "usage": map[string]any{"output_tokens": 1}})
		writeClaudeSSE(w, "message_stop", map[string]any{"type": "message_stop"})
	}))
	t.Cleanup(server.Close)

	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	configDir := filepath.Join(root, "claude-config")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "settings.json"), []byte("{\n  \"model\": \"fixture-model\"\n}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := NewClaudeCodeSubagentProvider(ClaudeCodeSubagentConfig{
		Executable: executable,
		Env: map[string]string{
			"ANTHROPIC_API_KEY": "fake-local-key", "ANTHROPIC_BASE_URL": server.URL,
			"CLAUDE_CONFIG_DIR": configDir, "HOME": root, "XDG_CONFIG_HOME": filepath.Join(root, "xdg"),
			"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL": "1",
			"DISABLE_TELEMETRY": "1", "DISABLE_ERROR_REPORTING": "1",
			"HTTP_PROXY": "", "HTTPS_PROXY": "", "ALL_PROXY": "", "NO_PROXY": "127.0.0.1,localhost",
		},
		DisposeGrace: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := provider.Start(context.Background(), SubagentStartRequest{CWD: workspace, Prompt: []ContentBlock{{Type: "text", Text: task}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Dispose() })
	result, err := run.Wait(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentCompleted || contentValueText(result.Output) != sentinel {
		t.Fatalf("result = %#v", result)
	}
	select {
	case request := <-requests:
		if request.Header.Get("x-api-key") != "fake-local-key" {
			t.Fatalf("x-api-key = %q", request.Header.Get("x-api-key"))
		}
		encoded, _ := json.Marshal(request.Body["messages"])
		if !strings.Contains(string(encoded), task) {
			t.Fatalf("messages did not contain task: %s", encoded)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Claude Code made no local Messages request")
	}
}

func writeClaudeSSE(w http.ResponseWriter, event string, payload any) {
	data, _ := json.Marshal(payload)
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func TestDSHSDKSubagentProviderRealProtocol(t *testing.T) {
	cwd := t.TempDir()
	provider, err := NewDSHSDKSubagentProvider(DSHSDKSubagentConfig{
		Command: helperExecutable(t), Args: helperArgs("sdk"), Provider: "fixture-provider", Model: "fixture-model", MaxTokens: 123,
		Env:             map[string]string{subagentHelperEnv: "1"},
		ShutdownTimeout: time.Second, DisposeEOFGrace: time.Second, DisposeGrace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := provider.Start(context.Background(), SubagentStartRequest{
		CWD: cwd, Prompt: []ContentBlock{{Type: "text", Text: "sdk prompt"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Wait(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("sdk|%s|fixture-provider|fixture-model|123|sdk prompt", cwd)
	if result.StopReason != SubagentCompleted || contentValueText(result.Output) != want {
		t.Fatalf("result = %#v, want text %q", result, want)
	}
	if err := run.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestEngineSubagentProviderRegistryAndCapabilityGate(t *testing.T) {
	engine, err := New(WithPersistence(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	provider, err := NewACPSubagentProvider(ACPSubagentConfig{Command: "unused"})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterSubagentProvider(provider); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterSubagentProvider(provider); err == nil {
		t.Fatal("duplicate provider was accepted")
	}
	rows := engine.ListSubagentProviders()
	if len(rows) != 1 || rows[0].Name != "acp" || rows[0].InheritsParentContext {
		t.Fatalf("providers = %#v", rows)
	}
	_, err = engine.StartSubagent(context.Background(), "acp", SubagentStartRequest{
		CWD: t.TempDir(), Prompt: []ContentBlock{{Type: "text", Text: "x"}}, OutputSchema: map[string]any{"type": "object"},
	})
	if err == nil || !strings.Contains(err.Error(), "does not support outputSchema") {
		t.Fatalf("capability error = %v", err)
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitForTestFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func helperExecutable(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func helperArgs(mode string) []string {
	return []string{"-test.run=^TestSubagentProviderHelper$", "--", mode}
}

func writeClaudeHelperWrapper(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("test wrapper uses a POSIX shell")
	}
	path := filepath.Join(t.TempDir(), "claude")
	writeHelperWrapper(t, path, "claude")
	return path
}

func writeHelperWrapper(t *testing.T, path, mode string) {
	t.Helper()
	script := "#!/bin/sh\n" + subagentHelperEnv + "=1 exec " + shellTestQuote(helperExecutable(t)) + " -test.run='^TestSubagentProviderHelper$' -- " + shellTestQuote(mode) + " \"$@\"\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func shellTestQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func TestSubagentProviderHelper(t *testing.T) {
	if os.Getenv(subagentHelperEnv) != "1" {
		return
	}
	mode, rest := helperMode(os.Args)
	switch mode {
	case "acp", "acp-hang":
		runACPHelper(mode == "acp-hang")
	case "codex":
		runCodexHelper(rest)
	case "claude":
		runClaudeHelper(rest)
	case "sdk":
		runSDKHelper()
	default:
		os.Exit(2)
	}
	os.Exit(0)
}

func helperMode(args []string) (string, []string) {
	for index, arg := range args {
		if arg == "--" && index+1 < len(args) {
			return args[index+1], args[index+2:]
		}
	}
	return "", nil
}

type helperFrame struct {
	ID     any            `json:"id"`
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
	Result map[string]any `json:"result"`
}

func helperWrite(value any) {
	data, _ := json.Marshal(value)
	_, _ = os.Stdout.Write(append(data, '\n'))
}

func helperReply(frame helperFrame, result any) {
	helperWrite(map[string]any{"jsonrpc": "2.0", "id": frame.ID, "result": result})
}

func helperNotify(method string, params any) {
	helperWrite(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func runACPHelper(hang bool) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var frame helperFrame
		if json.Unmarshal(scanner.Bytes(), &frame) != nil {
			continue
		}
		switch frame.Method {
		case "initialize":
			helperReply(frame, map[string]any{"protocolVersion": 1, "agentCapabilities": map[string]any{}, "authMethods": []any{}})
		case "session/new":
			helperReply(frame, map[string]any{"sessionId": "acp-session"})
		case "session/prompt":
			if hang {
				helperNotify("session/update", map[string]any{
					"sessionId": "acp-session",
					"update":    map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "partial"}},
				})
				helperWrite(map[string]any{"jsonrpc": "2.0", "id": 701, "method": "session/request_permission", "params": map[string]any{"sessionId": "acp-session", "options": []map[string]any{{"optionId": "no", "kind": "reject_once"}}}})
				if scanner.Scan() {
					if ready := os.Getenv("SUBAGENT_READY_FILE"); ready != "" {
						_ = os.WriteFile(ready, []byte("ready"), 0o600)
					}
				}
				continue
			}
			helperWrite(map[string]any{"jsonrpc": "2.0", "id": 700, "method": "session/request_permission", "params": map[string]any{"sessionId": "acp-session", "options": []map[string]any{{"optionId": "yes", "kind": "allow_once"}, {"optionId": "no", "kind": "reject_once"}}}})
			permission := "no"
			if scanner.Scan() {
				var response map[string]any
				_ = json.Unmarshal(scanner.Bytes(), &response)
				result, _ := response["result"].(map[string]any)
				outcome, _ := result["outcome"].(map[string]any)
				if outcome["optionId"] == "yes" {
					permission = "yes"
				}
			}
			prompt := ""
			for _, raw := range frame.Params["prompt"].([]any) {
				block, _ := raw.(map[string]any)
				prompt += fmt.Sprint(block["text"])
			}
			// session/prompt does not carry cwd; the process cwd equals the ACP session cwd.
			text := fmt.Sprintf("acp|%s|%s|ambient=%s|explicit=%s|dsh=%s|permission=%s", mustGetwd(), prompt, os.Getenv("AMBIENT_TOKEN"), os.Getenv("EXPLICIT_TOKEN"), os.Getenv("DSH_CHILD_FACT"), permission)
			helperNotify("session/update", map[string]any{
				"sessionId": "acp-session",
				"update":    map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "ignored"}},
			})
			helperNotify("session/update", map[string]any{
				"sessionId": "acp-session",
				"update":    map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": text}},
			})
			helperReply(frame, map[string]any{"stopReason": "end_turn"})
		case "session/cancel":
			if hang {
				return
			}
		}
	}
}

func runCodexHelper(args []string) {
	if strings.Join(args, " ") != "app-server --stdio" {
		os.Exit(3)
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var frame helperFrame
		if json.Unmarshal(scanner.Bytes(), &frame) != nil {
			continue
		}
		switch frame.Method {
		case "initialize":
			helperReply(frame, map[string]any{})
		case "initialized":
		case "thread/start":
			if !matchesCodexPermissionMode(frame.Params, os.Getenv("SUBAGENT_EXPECT_CODEX_PERMISSION_MODE")) {
				os.Exit(6)
			}
			helperReply(frame, map[string]any{"thread": map[string]any{"id": "thread-1", "ephemeral": true}})
		case "turn/start":
			helperWrite(map[string]any{"jsonrpc": "2.0", "method": "turn/started", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1"}}})
			helperWrite(map[string]any{"jsonrpc": "2.0", "id": 900, "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "availableDecisions": []any{"cancel", "decline"}}})
			if !scanner.Scan() {
				return
			}
			var approval map[string]any
			_ = json.Unmarshal(scanner.Bytes(), &approval)
			result, _ := approval["result"].(map[string]any)
			if result["decision"] != "cancel" {
				os.Exit(4)
			}
			helperReply(frame, map[string]any{"turn": map[string]any{"id": "turn-1"}})
			helperWrite(map[string]any{"jsonrpc": "2.0", "method": "item/completed", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "item": map[string]any{"type": "agentMessage", "phase": "commentary", "text": "ignore"}}})
			helperWrite(map[string]any{"jsonrpc": "2.0", "method": "item/completed", "params": map[string]any{"threadId": "thread-1", "turnId": "turn-1", "item": map[string]any{"type": "agentMessage", "phase": "final_answer", "text": "codex-final"}}})
			helperWrite(map[string]any{"jsonrpc": "2.0", "method": "turn/completed", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "completed"}}})
		case "turn/interrupt":
			helperReply(frame, map[string]any{})
		}
	}
}

func matchesCodexPermissionMode(params map[string]any, mode string) bool {
	if mode == "" {
		mode = string(CodexPermissionNever)
	}
	want := codexThreadPermissionParams(CodexPermissionMode(mode))
	for _, key := range []string{"approvalPolicy", "approvalsReviewer", "sandbox"} {
		got, present := params[key]
		expected, wanted := want[key]
		if present != wanted || wanted && got != expected {
			return false
		}
	}
	return true
}

func runClaudeHelper(args []string) {
	mode := os.Getenv("SUBAGENT_EXPECT_CLAUDE_PERMISSION_MODE")
	if mode == "" {
		mode = string(ClaudeCodePermissionDontAsk)
	}
	want := "--output-format stream-json --verbose --input-format stream-json --disallowedTools AskUserQuestion --permission-mode " + mode + " --no-session-persistence"
	if mode == string(ClaudeCodePermissionBypassPermissions) {
		want += " --dangerously-skip-permissions"
	}
	argsOK := strings.Join(args, " ") == want
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		os.Exit(5)
	}
	var input map[string]any
	_ = json.Unmarshal(scanner.Bytes(), &input)
	message, _ := input["message"].(map[string]any)
	content, _ := message["content"].([]any)
	block, _ := content[0].(map[string]any)
	text := fmt.Sprintf("claude|%s|args-%s|entry=%s", block["text"], map[bool]string{true: "ok", false: "bad"}[argsOK], os.Getenv("CLAUDE_CODE_ENTRYPOINT"))
	helperWrite(map[string]any{"type": "system", "subtype": "init"})
	helperWrite(map[string]any{"type": "result", "subtype": "success", "is_error": false, "result": text})
}

func runSDKHelper() {
	scanner := bufio.NewScanner(os.Stdin)
	var initialize map[string]any
	for scanner.Scan() {
		var frame helperFrame
		if json.Unmarshal(scanner.Bytes(), &frame) != nil {
			continue
		}
		switch frame.Method {
		case "initialize":
			initialize = frame.Params
			helperReply(frame, map[string]any{"serverInfo": map[string]any{"name": "deepseek-harness-sdk-runtime", "version": "test"}})
		case "session/prompt":
			sessionID := fmt.Sprint(frame.Params["sessionId"])
			blocks, _ := frame.Params["contentBlocks"].([]any)
			block, _ := blocks[0].(map[string]any)
			text := fmt.Sprintf("sdk|%s|%s|%s|%v|%s", initialize["cwd"], initialize["provider"], initialize["model"], initialize["maxTokens"], block["text"])
			helperWrite(map[string]any{"jsonrpc": "2.0", "method": "session.event", "params": map[string]any{"sessionId": sessionID, "event": map[string]any{"type": "agent/inbox/spliced", "data": map[string]any{"inserted": []map[string]any{{"id": "message-1"}}}}}})
			helperWrite(map[string]any{"jsonrpc": "2.0", "method": "session.event", "params": map[string]any{"sessionId": sessionID, "event": map[string]any{"type": "assistant/message", "data": map[string]any{"message": map[string]any{"content": []map[string]any{{"type": "text", "text": text}}}}}}})
			helperWrite(map[string]any{"jsonrpc": "2.0", "method": "session.event", "params": map[string]any{"sessionId": sessionID, "event": map[string]any{"type": "turn/end", "data": map[string]any{"reason": map[string]any{"kind": "completed"}}}}})
			helperWrite(map[string]any{"jsonrpc": "2.0", "method": "session.status", "params": map[string]any{"sessionId": sessionID, "status": "idle"}})
			helperReply(frame, map[string]any{"messageId": "message-1"})
		case "shutdown":
			helperReply(frame, map[string]any{})
			return
		}
	}
}

func mustGetwd() string {
	cwd, _ := os.Getwd()
	return cwd
}
