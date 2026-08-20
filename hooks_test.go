package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHookProtocolParity(t *testing.T) {
	claude, codex := HookDialectClaudeCode, HookDialectCodex
	literal, regex, invalid := "Bash", "^Bash$", "("
	if !MatchesHookMatcher(nil, "anything", claude) || !MatchesHookMatcher(&literal, "Bash", claude) || MatchesHookMatcher(&literal, "BashOutput", claude) {
		t.Fatal("Claude literal matcher semantics differ")
	}
	if !MatchesHookMatcher(&literal, "BashOutput", codex) || !MatchesHookMatcher(&regex, "Bash", claude) || MatchesHookMatcher(&invalid, "x", codex) {
		t.Fatal("regex matcher semantics differ")
	}
	if got := HookMatcherDiagnostic(&invalid, claude); got != `invalid claude-code regex matcher "("` {
		t.Fatalf("matcher diagnostic = %q", got)
	}

	exit := 0
	out := ParseHookOutput(&exit, `{"decision":"approve","continue":false,"stopReason":"halt","hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"policy","additionalContext":"ctx"}}`, "", "PreToolUse")
	if out.Decision != "deny" || out.Reason != "policy" || out.Continue == nil || *out.Continue || out.AdditionalContext != "ctx" {
		t.Fatalf("parsed output = %#v", out)
	}
	mismatched := ParseHookOutput(&exit, `{"decision":"block","reason":"top","hookSpecificOutput":{"hookEventName":"Stop","permissionDecision":"allow"}}`, "", "PreToolUse")
	if mismatched.Decision != "block" || mismatched.Reason != "top" {
		t.Fatalf("mismatched event discarded top-level fields: %#v", mismatched)
	}
	blockingExit := 2
	blocked := ParseHookOutput(&blockingExit, `{"decision":"approve"}`, " denied ")
	if blocked.Decision != "block" || blocked.Reason != "denied" {
		t.Fatalf("exit 2 = %#v", blocked)
	}

	ask := false
	merged := MergeHookOutputs([]HookOutput{
		{Decision: "allow", AdditionalContext: "a"},
		{Decision: "ask", Reason: "confirm", Continue: &ask},
		{Decision: "deny", Reason: "blocked", AdditionalContext: "b"},
	})
	if merged.Decision != "deny" || merged.Reason != "blocked" || !merged.Stop || strings.Join(merged.AdditionalContext, ",") != "a,b" {
		t.Fatalf("merged output = %#v", merged)
	}
	if got := SummarizeHookStderr("  "+strings.Repeat("x", 6)+"  ", 4); got != "xxxx…" {
		t.Fatalf("stderr summary = %q", got)
	}
}

func TestHookBridgeParsesDialects(t *testing.T) {
	dir := t.TempDir()
	claudePath := filepath.Join(dir, "claude.json")
	claude := map[string]any{"hooks": map[string]any{
		"PreToolUse": []any{map[string]any{"matcher": "Bash", "hooks": []any{
			map[string]any{"type": "prompt", "prompt": "skip"},
			map[string]any{"command": "${CLAUDE_PLUGIN_ROOT}/check ${CLAUDE_PROJECT_DIR}", "timeout": 3},
		}}},
		"Stop": []any{map[string]any{"matcher": "(", "hooks": []any{map[string]any{"command": "stop"}}}},
	}}
	writeHookConfig(t, claudePath, claude)
	bridge, err := NewHookBridge(HookBridgeConfig{Dialect: HookDialectClaudeCode, ConfigPath: claudePath, PluginRoot: "/plugin", ProjectDir: "/project"})
	if err != nil {
		t.Fatal(err)
	}
	pre := bridge.groups["PreToolUse"]
	if len(pre) != 1 || len(pre[0].hooks) != 1 || pre[0].hooks[0].Command != "/plugin/check /project" || pre[0].hooks[0].Timeout != 3*time.Second {
		t.Fatalf("Claude config = %#v", pre)
	}
	if stop := bridge.groups["Stop"]; len(stop) != 1 || stop[0].matcher != nil {
		t.Fatalf("Stop matcher was not discarded: %#v", stop)
	}

	codexPath := filepath.Join(dir, "codex.json")
	writeHookConfig(t, codexPath, map[string]any{"PreToolUse": []any{map[string]any{"matcher": "Bash", "hooks": []any{
		map[string]any{"command": "skip", "async": true},
		map[string]any{"command": "run", "timeoutSec": 2},
	}}}})
	codexBridge, err := NewHookBridge(HookBridgeConfig{Dialect: HookDialectCodex, ConfigPath: codexPath})
	if err != nil {
		t.Fatal(err)
	}
	group := codexBridge.groups["PreToolUse"]
	if len(group) != 1 || len(group[0].hooks) != 1 || group[0].hooks[0].Timeout != 2*time.Second || !MatchesHookMatcher(group[0].matcher, "BashOutput", HookDialectCodex) {
		t.Fatalf("Codex config = %#v", group)
	}

	invalidPath := filepath.Join(dir, "invalid.json")
	writeHookConfig(t, invalidPath, map[string]any{"PreToolUse": []any{map[string]any{"matcher": "(", "hooks": []any{map[string]any{"command": "x"}}}}})
	if _, err := NewHookBridge(HookBridgeConfig{Dialect: HookDialectCodex, ConfigPath: invalidPath}); err == nil || !strings.Contains(err.Error(), `on event "PreToolUse"`) {
		t.Fatalf("invalid matcher error = %v", err)
	}
}

func TestHookBridgeRunsRealCommands(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "hooks.json")
	payloadPath := filepath.Join(dir, "payload.json")
	projectPath := filepath.Join(dir, "project-dir")
	command := fmt.Sprintf("cat > %q; test \"$CLAUDE_PROJECT_DIR\" = %q; printf '%%s' '{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"permissionDecision\":\"deny\",\"permissionDecisionReason\":\"blocked\"}}'", payloadPath, projectPath)
	writeHookConfig(t, configPath, map[string]any{"PreToolUse": []any{map[string]any{"hooks": []any{map[string]any{"command": command}}}}})
	bridge, err := NewHookBridge(HookBridgeConfig{Dialect: HookDialectClaudeCode, ConfigPath: configPath, ProjectDir: projectPath, DefaultTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	result, err := bridge.Run(context.Background(), HookPointRequest{Point: "PreToolUse", MatchQuery: "bash", Payload: map[string]any{"hook_event_name": "PreToolUse", "tool_name": "bash"}, CWD: dir})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome.Decision != "deny" || result.Outcome.Reason != "blocked" || len(result.Runs) != 1 || !strings.HasPrefix(result.Runs[0].HandlerID, "claude-code:PreToolUse:") {
		t.Fatalf("hook result = %#v", result)
	}
	payload, err := os.ReadFile(payloadPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "{\"hook_event_name\":\"PreToolUse\",\"tool_name\":\"bash\"}\n" {
		t.Fatalf("hook stdin = %q", payload)
	}

	timeoutPath := filepath.Join(dir, "timeout.json")
	writeHookConfig(t, timeoutPath, map[string]any{"Stop": []any{map[string]any{"hooks": []any{map[string]any{"command": "sleep 10"}}}}})
	timeoutBridge, err := NewHookBridge(HookBridgeConfig{Dialect: HookDialectCodex, ConfigPath: timeoutPath, DefaultTimeout: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	timedOut, err := timeoutBridge.Run(context.Background(), HookPointRequest{Point: "Stop", Payload: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second || len(timedOut.Runs) != 1 || timedOut.Runs[0].Output.ExitCode != nil || timedOut.Outcome.Decision != "none" {
		t.Fatalf("timed out hook = %#v after %s", timedOut, time.Since(started))
	}
}

func writeHookConfig(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
