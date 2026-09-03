package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func newIntegrationEngine(t *testing.T) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func postRPC(t *testing.T, client *http.Client, endpoint, host, method string, payload any) (int, map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"type":    "client-request",
		"rpcId":   "test-rpc-1",
		"method":  method,
		"payload": payload,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, endpoint+"/api/"+method, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if host != "" {
		req.Host = host
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST /api/%s: %v", method, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /api/%s response: %v", method, err)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		if resp.StatusCode >= 400 {
			return resp.StatusCode, nil
		}
		t.Fatalf("decode /api/%s response %q: %v", method, data, err)
	}
	return resp.StatusCode, value
}

func rpcResult(t *testing.T, envelope map[string]any) map[string]any {
	t.Helper()
	if got := envelope["type"]; got != "server-response" {
		t.Fatalf("response type = %v, want server-response", got)
	}
	result, ok := envelope["result"].(map[string]any)
	if !ok {
		t.Fatalf("response result = %T, want object", envelope["result"])
	}
	return result
}

func TestHTTPRPCSessionLifecycle(t *testing.T) {
	e := newIntegrationEngine(t)
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	status, envelope := postRPC(t, server.Client(), server.URL, "", "host.describe", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("host.describe status = %d, want 200", status)
	}
	describe := rpcResult(t, envelope)
	if describe["ok"] != true {
		t.Fatalf("host.describe result = %#v, want ok", describe)
	}
	value, ok := describe["value"].(map[string]any)
	if !ok {
		t.Fatalf("host.describe value = %T, want object", describe["value"])
	}
	if value["provider"] != "echo" || value["model"] != "echo" {
		t.Fatalf("host.describe provider/model = %v/%v, want echo/echo", value["provider"], value["model"])
	}
	if home, _ := os.UserHomeDir(); value["home"] != home {
		t.Fatalf("host.describe home = %v, want %q", value["home"], home)
	}

	status, envelope = postRPC(t, server.Client(), server.URL, "", "session.create", map[string]any{"cwd": e.Config().Workspace})
	if status != http.StatusOK {
		t.Fatalf("session.create status = %d, want 200", status)
	}
	createResult := rpcResult(t, envelope)
	if createResult["ok"] != true {
		t.Fatalf("session.create result = %#v, want ok", createResult)
	}
	created, ok := createResult["value"].(map[string]any)
	if !ok {
		t.Fatalf("session.create value = %T, want object", createResult["value"])
	}
	sessionID, ok := created["sessionId"].(string)
	if !ok || sessionID == "" {
		t.Fatalf("session.create sessionId = %v, want non-empty string", created["sessionId"])
	}

	status, envelope = postRPC(t, server.Client(), server.URL, "", "session.prompt", map[string]any{
		"sessionId": sessionID,
		"mode":      "queue",
		"content":   []map[string]any{{"type": "text", "text": "hello over HTTP"}},
	})
	if status != http.StatusOK {
		t.Fatalf("session.prompt status = %d, want 200", status)
	}
	promptResult := rpcResult(t, envelope)
	if promptResult["ok"] != true {
		t.Fatalf("session.prompt result = %#v, want ok", promptResult)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var history map[string]any
	for {
		status, envelope = postRPC(t, server.Client(), server.URL, "", "session.history", map[string]any{"sessionId": sessionID, "maxMessages": 20})
		if status != http.StatusOK {
			t.Fatalf("session.history status = %d, want 200", status)
		}
		result := rpcResult(t, envelope)
		if result["ok"] != true {
			t.Fatalf("session.history result = %#v, want ok", result)
		}
		history, _ = result["value"].(map[string]any)
		if hasEventType(history, "assistant/message") {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for assistant/message; history = %#v", history)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !hasEventType(history, "user/message") {
		t.Fatalf("history does not contain user/message: %#v", history)
	}
}

func hasEventType(history map[string]any, want string) bool {
	entries, ok := history["events"].([]any)
	if !ok {
		return false
	}
	for _, raw := range entries {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		event, ok := entry["event"].(map[string]any)
		if ok && event["type"] == want {
			return true
		}
	}
	return false
}

func TestHTTPRPCBusinessErrorEnvelope(t *testing.T) {
	e := newIntegrationEngine(t)
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	status, envelope := postRPC(t, server.Client(), server.URL, "", "session.history", map[string]any{"sessionId": "missing"})
	if status != http.StatusOK {
		t.Fatalf("session.history missing status = %d, want 200", status)
	}
	result := rpcResult(t, envelope)
	if result["ok"] != false {
		t.Fatalf("missing session result = %#v, want ok=false", result)
	}
	errValue, ok := result["error"].(map[string]any)
	if !ok || errValue["code"] != "session/not-found" {
		t.Fatalf("missing session error = %#v, want session-not-found", result["error"])
	}

	status, envelope = postRPC(t, server.Client(), server.URL, "", "no.such.method", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("unknown method status = %d, want 200", status)
	}
	result = rpcResult(t, envelope)
	if result["ok"] != false {
		t.Fatalf("unknown method result = %#v, want ok=false", result)
	}
	errValue, ok = result["error"].(map[string]any)
	if !ok || errValue["code"] != "gateway/bad-request" {
		t.Fatalf("unknown method error = %#v, want bad-request", result["error"])
	}
}

func TestHTTPRPCDynamicCordisRoutes(t *testing.T) {
	e := newIntegrationEngine(t)
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	for method, want := range map[string]any{
		"dynamicCordisRunner/syncInspectManifest": nil,
		"dynamicCordisRunner/inventory":           []any{},
	} {
		status, envelope := postRPC(t, server.Client(), server.URL, "", method, map[string]any{})
		if status != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", method, status)
		}
		result := rpcResult(t, envelope)
		if result["ok"] != true {
			t.Fatalf("%s result = %#v, want ok", method, result)
		}
		value := result["value"]
		if want == nil {
			if value != nil {
				t.Fatalf("%s value = %#v, want nil", method, value)
			}
			continue
		}
		if items, ok := value.([]any); !ok || len(items) != 0 {
			t.Fatalf("%s value = %#v, want []", method, value)
		}
	}

	status, _ := postRPC(t, server.Client(), server.URL, "", "dynamicCordisRunner/noSuchMethod", map[string]any{})
	if status != http.StatusNotFound {
		t.Fatalf("unknown dynamic route status = %d, want 404", status)
	}
}

func TestHTTPSettingsMutateAndPersist(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = dir
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(e.Handler())
	t.Cleanup(func() { server.Close(); _ = e.Close() })

	status, envelope := postRPC(t, server.Client(), server.URL, "", "settings.mutate", map[string]any{
		"ns":  "ui-onboarding",
		"ops": []map[string]any{{"op": "set", "path": []string{"welcomeNoticeVersion"}, "value": "test"}},
	})
	if status != http.StatusOK || rpcResult(t, envelope)["ok"] != true {
		t.Fatalf("settings.mutate response = %d %#v", status, envelope)
	}
	_ = e.Close()
	e2, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e2.Close() })
	value := e2.settingsDescribe()
	namespaces := value["namespaces"].([]any)
	found := false
	for _, raw := range namespaces {
		row := raw.(map[string]any)
		if row["ns"] == "ui-onboarding" && row["value"].(map[string]any)["welcomeNoticeVersion"] == "test" {
			found = true
		}
	}
	if !found {
		t.Fatalf("settings after reload = %#v", value)
	}
}

func TestWorkspaceWireKeepsEmptySessionIDsAsArray(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace, _, err := e.CreateWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(workspace)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if ids, ok := decoded["sessionIds"].([]any); !ok || len(ids) != 0 {
		t.Fatalf("sessionIds wire value = %#v, want []", decoded["sessionIds"])
	}
}

func TestWorkspaceWireKeepsEmptySessionIDsAfterReload(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = dataDir
	cfg.Workspace = dataDir
	cfg.Persist = true
	first, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.CreateWorkspace(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	workspaces, _ := second.ListWorkspaces()
	if len(workspaces) != 1 {
		t.Fatalf("workspaces after reload = %#v", workspaces)
	}
	encoded, err := json.Marshal(workspaces[0])
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if ids, ok := decoded["sessionIds"].([]any); !ok || len(ids) != 0 {
		t.Fatalf("reloaded sessionIds wire value = %#v, want []", decoded["sessionIds"])
	}
}

func TestHTTPTrustFence(t *testing.T) {
	e := newIntegrationEngine(t)
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/host.describe", bytes.NewReader([]byte(`{"type":"client-request","rpcId":"trust-1","method":"host.describe","payload":{}}`)))
	if err != nil {
		t.Fatalf("new trust-fence request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = "evil.example:443"
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("trust-fence request: %v", err)
	}
	defer resp.Body.Close()
	status := resp.StatusCode
	if status != http.StatusForbidden {
		t.Fatalf("untrusted host status = %d, want 403", status)
	}
}
