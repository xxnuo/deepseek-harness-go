package harness

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func remoteValue(t *testing.T, envelope map[string]any) any {
	t.Helper()
	result := rpcResult(t, envelope)
	if result["ok"] != true {
		t.Fatalf("Remote result = %#v, want ok", result)
	}
	return result["value"]
}

func TestTypertRemoteCommandsAndProtocol(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	status, envelope := postRPC(t, server.Client(), server.URL, "", "commands/list", map[string]any{
		"args": map[string]any{"agentId": id},
	})
	commands, ok := remoteValue(t, envelope).([]any)
	if status != http.StatusOK || !ok || len(commands) < 7 {
		t.Fatalf("commands/list = %d %#v", status, envelope)
	}

	status, envelope = postRPC(t, server.Client(), server.URL, "", "commands/execute", map[string]any{
		"args": map[string]any{"agentId": id, "line": "/clear", "images": []any{}},
	})
	execution, ok := remoteValue(t, envelope).(map[string]any)
	if status != http.StatusOK || !ok || execution["commandId"] == "" {
		t.Fatalf("commands/execute = %d %#v", status, envelope)
	}
	s, _ := e.getSession(id)
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	if len(events) != 2 || events[0].Type != "command/run" || events[1].Type != "command/done" {
		t.Fatalf("command events = %#v", events)
	}

	_, envelope = postRPC(t, server.Client(), server.URL, "", "commands/execute", map[string]any{
		"args": map[string]any{"agentId": id, "line": "/unknown", "images": []any{}},
	})
	result := rpcResult(t, envelope)
	if result["ok"] != true {
		t.Fatalf("unknown command = %#v", result)
	}
	if _, present := result["value"]; present {
		t.Fatalf("unknown command carried value = %#v", result)
	}

	_, envelope = postRPC(t, server.Client(), server.URL, "", "commands/list", map[string]any{})
	result = rpcResult(t, envelope)
	if result["ok"] != false {
		t.Fatalf("malformed Remote payload = %#v", result)
	}
}

func TestTypertRemoteGoals(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	_, envelope := postRPC(t, server.Client(), server.URL, "", "goals/create", map[string]any{
		"args": map[string]any{"agentId": id, "request": map[string]any{"objective": "ship", "maxGoalRounds": 3}},
	})
	created := remoteValue(t, envelope).(map[string]any)
	ref := created["ref"].(map[string]any)

	_, envelope = postRPC(t, server.Client(), server.URL, "", "goals/pause", map[string]any{
		"args": map[string]any{"agentId": id, "ref": ref},
	})
	paused := remoteValue(t, envelope).(map[string]any)
	if paused["phase"] != "paused" || paused["createdAt"] == nil || paused["updatedAt"] == nil {
		t.Fatalf("goals/pause = %#v", paused)
	}

	_, envelope = postRPC(t, server.Client(), server.URL, "", "goals/clear", map[string]any{
		"args": map[string]any{"agentId": id, "ref": map[string]any{"id": ref["id"], "revision": paused["revision"]}},
	})
	cleared := remoteValue(t, envelope).(map[string]any)
	if cleared["id"] != ref["id"] || cleared["revision"].(float64) != paused["revision"].(float64)+1 {
		t.Fatalf("goals/clear = %#v", cleared)
	}
}

func TestTypertRemoteMessageFeedback(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	if _, err := e.appendEvent(s, "assistant/message", map[string]any{
		"message": map[string]any{"id": "msg-1", "role": "assistant", "content": []ContentBlock{{Type: "text", Text: "answer"}}},
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	_, envelope := postRPC(t, server.Client(), server.URL, "", "messageFeedback/put", map[string]any{
		"args": map[string]any{"request": map[string]any{
			"sessionId": id, "messageId": "msg-1", "rating": "positive", "note": "useful", "ifVersion": nil,
		}},
	})
	put := remoteValue(t, envelope).(map[string]any)
	if put["ok"] != true {
		t.Fatalf("messageFeedback/put = %#v", put)
	}
	item := put["value"].(map[string]any)
	if version, _ := item["version"].(string); !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(version) {
		t.Fatalf("messageFeedback version = %q", version)
	}
	_, envelope = postRPC(t, server.Client(), server.URL, "", "messageFeedback/put", map[string]any{
		"args": map[string]any{"request": map[string]any{
			"sessionId": id, "messageId": "msg-1", "rating": "negative", "ifVersion": nil,
		}},
	})
	conflict := remoteValue(t, envelope).(map[string]any)
	if conflict["ok"] != false || conflict["error"].(map[string]any)["code"] != "version-conflict" {
		t.Fatalf("messageFeedback stale put = %#v", conflict)
	}

	_, envelope = postRPC(t, server.Client(), server.URL, "", "messageFeedback/list", map[string]any{
		"args": map[string]any{"request": map[string]any{"sessionId": id}},
	})
	listed := remoteValue(t, envelope).(map[string]any)
	if listed["ok"] != true || len(listed["value"].(map[string]any)["items"].([]any)) != 1 {
		t.Fatalf("messageFeedback/list = %#v", listed)
	}

	_, envelope = postRPC(t, server.Client(), server.URL, "", "messageFeedback/delete", map[string]any{
		"args": map[string]any{"request": map[string]any{"sessionId": id, "messageId": "msg-1", "ifVersion": item["version"]}},
	})
	deleted := remoteValue(t, envelope).(map[string]any)
	if deleted["ok"] != true {
		t.Fatalf("messageFeedback/delete = %#v", deleted)
	}
}

func TestTypertRemoteMessageFeedbackRejectsInvalidWireValue(t *testing.T) {
	e := newIntegrationEngine(t)
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	_, envelope := postRPC(t, server.Client(), server.URL, "", "messageFeedback/put", map[string]any{
		"args": map[string]any{"request": map[string]any{
			"sessionId": "missing", "messageId": "msg-1", "rating": "invalid-rating", "ifVersion": nil,
		}},
	})
	result := rpcResult(t, envelope)
	errValue := result["error"].(map[string]any)
	if result["ok"] != false || errValue["code"] != "internal" || errValue["message"] != `typert gateway: messageFeedback/put: wire field "request" failed boundary validation` {
		t.Fatalf("invalid feedback wire response = %#v", result)
	}
}

func TestTypertRemoteMessageFeedbackPersistsAcrossEngineReload(t *testing.T) {
	dir := t.TempDir()
	workspace := t.TempDir()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir, cfg.Workspace, cfg.Provider, cfg.Model, cfg.Persist = dir, workspace, "echo", "echo", true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(t.Context(), workspace, "feedback-persist", "")
	if err != nil {
		e.Close()
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	if _, err := e.appendEvent(s, "assistant/message", map[string]any{
		"message": map[string]any{"id": "msg-persist", "role": "assistant", "content": []ContentBlock{{Type: "text", Text: "answer"}}},
	}); err != nil {
		e.Close()
		t.Fatal(err)
	}
	server := httptest.NewServer(e.Handler())
	_, envelope := postRPC(t, server.Client(), server.URL, "", "messageFeedback/put", map[string]any{
		"args": map[string]any{"request": map[string]any{
			"sessionId": id, "messageId": "msg-persist", "rating": "positive", "ifVersion": nil,
		}},
	})
	if result := remoteValue(t, envelope).(map[string]any); result["ok"] != true {
		server.Close()
		e.Close()
		t.Fatalf("put = %#v", result)
	}
	stored, err := os.ReadFile(filepath.Join(dir, "storages", "message_feedback.json"))
	if err != nil {
		server.Close()
		e.Close()
		t.Fatal(err)
	}
	var document struct {
		Unit   map[string]any            `json:"unit"`
		Tables map[string]map[string]any `json:"tables"`
	}
	if json.Unmarshal(stored, &document) != nil || document.Unit["name"] != "message_feedback" || document.Tables["sessions"][id] == nil {
		server.Close()
		e.Close()
		t.Fatalf("feedback storage document = %s", stored)
	}
	server.Close()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	reloaded, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reloaded.Close() })
	server = httptest.NewServer(reloaded.Handler())
	t.Cleanup(server.Close)
	_, envelope = postRPC(t, server.Client(), server.URL, "", "messageFeedback/list", map[string]any{
		"args": map[string]any{"request": map[string]any{"sessionId": id}},
	})
	listed := remoteValue(t, envelope).(map[string]any)
	items := listed["value"].(map[string]any)["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["messageId"] != "msg-persist" {
		t.Fatalf("reloaded feedback = %#v", listed)
	}
}

func TestTypertRemoteKnownUnsupportedDynamicMethodIsNot404(t *testing.T) {
	e := newIntegrationEngine(t)
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	status, envelope := postRPC(t, server.Client(), server.URL, "", "dynamicCordisRunner/invoke", map[string]any{
		"args": map[string]any{"pluginId": "plugin-1", "pluginRunId": "run-1", "method": "read", "args": map[string]any{}},
	})
	result := rpcResult(t, envelope)
	errValue, _ := result["error"].(map[string]any)
	if status != http.StatusOK || result["ok"] != false || errValue["code"] != "invocation-unavailable" {
		t.Fatalf("dynamic invoke = %d %#v", status, envelope)
	}

	_, envelope = postRPC(t, server.Client(), server.URL, "", "pluginInventory/list", map[string]any{"args": map[string]any{}})
	inventory := remoteValue(t, envelope).(map[string]any)
	if entries, ok := inventory["entries"].([]any); !ok || len(entries) == 0 {
		t.Fatalf("plugin inventory = %#v", inventory)
	}
}
