package harness

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSessionCreateRejectsRemovedReuseWorkspaceBlank(t *testing.T) {
	e := newIntegrationEngine(t)
	_, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId": "workspace", "sessionId": "session", "reuseWorkspaceBlank": true,
	})
	if rpcErr == nil || rpcErr.Code != "bad-request" || !strings.Contains(rpcErr.Message, "does not accept reuseWorkspaceBlank") {
		t.Fatalf("removed reuse option error = %#v", rpcErr)
	}
	if got := len(e.ListSessions()); got != 0 {
		t.Fatalf("rejected request created %d sessions", got)
	}
}

func TestSessionCreateDoesNotRefreshPermissionDefaultForExistingBlank(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", "workspace-write")
	e := newIntegrationEngine(t)
	workspace, _, err := e.CreateWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = "workspace-blank"
	if _, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId": workspace.WorkspaceID, "sessionId": sessionID,
	}); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	session, _ := e.getSession(sessionID)
	initial := permissionEventValues(session)
	if !reflect.DeepEqual(initial, []string{"workspace-write", "workspace-write", "ask"}) {
		t.Fatalf("initial permissions = %#v", initial)
	}
	if _, rpcErr := e.settingsUpdate("permission", map[string]any{"defaultPreset": "danger-full-access"}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	value, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId": workspace.WorkspaceID, "sessionId": sessionID,
	})
	if rpcErr != nil || value["sessionId"] != sessionID {
		t.Fatalf("ordinary idempotent create = %#v, %#v", value, rpcErr)
	}
	if got := permissionEventValues(session); !reflect.DeepEqual(got, initial) {
		t.Fatalf("existing blank permissions changed = %#v, want %#v", got, initial)
	}
	for _, event := range session.Events {
		if event.Type != "permission/preset" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		if _, exists := data["origin"]; exists {
			t.Fatalf("permission event retained removed origin: %#v", data)
		}
	}
}

func TestSessionCreateAdoptsExistingAgentPresetWithoutReuseFlag(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace, _, err := e.CreateWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = "rpc-preset-adoption"
	created, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId": workspace.WorkspaceID, "sessionId": sessionID, "agentPreset": "standard",
	})
	if rpcErr != nil || created["agentPreset"] != "standard" {
		t.Fatalf("initial create = %#v, %#v", created, rpcErr)
	}
	if _, rpcErr := dispatchTestRPC(t, e, "agentPreset.select", map[string]any{
		"sessionId": sessionID, "agentPreset": "minimal",
	}); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	adopted, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId": workspace.WorkspaceID, "sessionId": sessionID,
	})
	if rpcErr != nil || adopted["agentPreset"] != "minimal" {
		t.Fatalf("preset adoption = %#v, %#v", adopted, rpcErr)
	}
}

func dispatchTestRPC(t *testing.T, e *Engine, method string, payload map[string]any) (map[string]any, *RPCError) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	value, rpcErr := e.dispatch(context.Background(), method, raw)
	if value == nil {
		return nil, rpcErr
	}
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s value = %T", method, value)
	}
	return object, rpcErr
}
