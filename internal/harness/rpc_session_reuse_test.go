package harness

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSessionCreateReuseWorkspaceBlankValidation(t *testing.T) {
	e := newIntegrationEngine(t)
	tests := []struct {
		name    string
		payload map[string]any
		message string
	}{
		{"false", map[string]any{"workspaceId": "w", "sessionId": "s", "reuseWorkspaceBlank": false}, "must be true"},
		{"string", map[string]any{"workspaceId": "w", "sessionId": "s", "reuseWorkspaceBlank": "true"}, "must be true"},
		{"missing session", map[string]any{"workspaceId": "w", "reuseWorkspaceBlank": true}, "requires workspaceId and sessionId"},
		{"missing workspace", map[string]any{"sessionId": "s", "reuseWorkspaceBlank": true}, "requires workspaceId and sessionId"},
		{"workspace and cwd", map[string]any{"workspaceId": "w", "cwd": "/tmp", "sessionId": "s", "reuseWorkspaceBlank": true}, "not both"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, rpcErr := dispatchTestRPC(t, e, "session.create", test.payload)
			if rpcErr == nil || rpcErr.Code != "bad-request" || !strings.Contains(rpcErr.Message, test.message) {
				t.Fatalf("session.create error = %#v, want bad-request containing %q", rpcErr, test.message)
			}
		})
	}
	if got := len(e.ListSessions()); got != 0 {
		t.Fatalf("invalid requests created %d sessions", got)
	}
}

func TestSessionCreateReuseWorkspaceBlankRefreshesEligibleDefault(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", "workspace-write")
	e := newIntegrationEngine(t)
	workspace, _, err := e.CreateWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = "workspace-blank"
	if _, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId": workspace.WorkspaceID,
		"sessionId":   sessionID,
	}); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	session, _ := e.getSession(sessionID)
	if got := permissionEventValues(session); !reflect.DeepEqual(got, []string{"workspace-write", "workspace-write", "ask"}) {
		t.Fatalf("initial permissions = %#v", got)
	}
	if got := latestPermissionOrigin(session); got != "default" {
		t.Fatalf("initial permission origin = %q", got)
	}
	if _, rpcErr := e.settingsUpdate("permission", map[string]any{"defaultPreset": "danger-full-access"}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	value, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId":         workspace.WorkspaceID,
		"sessionId":           sessionID,
		"reuseWorkspaceBlank": true,
	})
	if rpcErr != nil || value["sessionId"] != sessionID {
		t.Fatalf("reuse = %#v, %#v", value, rpcErr)
	}
	if got := permissionEventValues(session); !reflect.DeepEqual(got, []string{
		"workspace-write", "workspace-write", "ask",
		"danger-full-access", "danger-full-access", "never",
	}) {
		t.Fatalf("refreshed permissions = %#v", got)
	}
	if got := latestPermissionOrigin(session); got != "default" {
		t.Fatalf("refreshed permission origin = %q", got)
	}
}

func TestSessionCreateReuseWorkspaceBlankRefreshesColdPersistedDefault(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", "workspace-write")
	dataDir := t.TempDir()
	workspaceDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dataDir
	cfg.Workspace = workspaceDir
	cfg.AgentsHome = t.TempDir()
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = true
	first, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	firstClosed := false
	t.Cleanup(func() {
		if !firstClosed {
			_ = first.Close()
		}
	})
	workspace, _, err := first.CreateWorkspace(workspaceDir)
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = "workspace-cold-blank"
	if _, rpcErr := dispatchTestRPC(t, first, "session.create", map[string]any{
		"workspaceId": workspace.WorkspaceID,
		"sessionId":   sessionID,
	}); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if _, rpcErr := first.settingsUpdate("permission", map[string]any{"defaultPreset": "danger-full-access"}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	firstClosed = true

	second, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	session, err := second.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	attached := session.attached
	session.mu.Unlock()
	if attached {
		t.Fatal("persisted session was already attached before reuse")
	}
	if _, rpcErr := dispatchTestRPC(t, second, "session.create", map[string]any{
		"workspaceId":         workspace.WorkspaceID,
		"sessionId":           sessionID,
		"reuseWorkspaceBlank": true,
	}); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if got := permissionEventValues(session); !reflect.DeepEqual(got, []string{
		"workspace-write", "workspace-write", "ask",
		"danger-full-access", "danger-full-access", "never",
	}) {
		t.Fatalf("cold refreshed permissions = %#v", got)
	}
}

func TestSessionCreateReuseWorkspaceBlankRejectsIneligibleRefresh(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", "workspace-write")
	e := newIntegrationEngine(t)
	workspace, _, err := e.CreateWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	create := func(payload map[string]any) *Session {
		t.Helper()
		value, rpcErr := dispatchTestRPC(t, e, "session.create", payload)
		if rpcErr != nil {
			t.Fatal(rpcErr)
		}
		session, err := e.getSession(value["sessionId"].(string))
		if err != nil {
			t.Fatal(err)
		}
		return session
	}
	member := func(id string) *Session {
		return create(map[string]any{"workspaceId": workspace.WorkspaceID, "sessionId": id})
	}

	started := member("reuse-started")
	if _, err := e.appendEvent(started, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	archived := member("reuse-archived")
	if _, err := e.ArchiveSession(archived.Header.ID); err != nil {
		t.Fatal(err)
	}
	nonMember := create(map[string]any{"cwd": workspace.Path, "sessionId": "reuse-non-member"})
	explicit := member("reuse-explicit")
	if _, err := e.commandPermission(context.Background(), commandInvocation{Session: explicit, RawInput: "read-only"}); err != nil {
		t.Fatal(err)
	}
	otherCWD := create(map[string]any{"cwd": t.TempDir(), "sessionId": "reuse-cwd-conflict"})
	if err := e.AttachSession(workspace.WorkspaceID, otherCWD.Header.ID); err != nil {
		t.Fatal(err)
	}

	if _, rpcErr := e.settingsUpdate("permission", map[string]any{"defaultPreset": "danger-full-access"}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	initial := map[string][]string{}
	for _, session := range []*Session{started, archived, nonMember, explicit, otherCWD} {
		initial[session.Header.ID] = permissionEventValues(session)
	}
	for _, session := range []*Session{started, archived, nonMember, explicit} {
		if _, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
			"workspaceId":         workspace.WorkspaceID,
			"sessionId":           session.Header.ID,
			"reuseWorkspaceBlank": true,
		}); rpcErr != nil {
			t.Fatalf("reuse %s: %v", session.Header.ID, rpcErr)
		}
		if got := permissionEventValues(session); !reflect.DeepEqual(got, initial[session.Header.ID]) {
			t.Fatalf("ineligible %s permissions = %#v, want %#v", session.Header.ID, got, initial[session.Header.ID])
		}
	}
	if !containsString(workspaceSessionIDs(e, workspace.WorkspaceID), nonMember.Header.ID) {
		t.Fatalf("non-member session was not attached by idempotent create")
	}
	if _, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId":         workspace.WorkspaceID,
		"sessionId":           otherCWD.Header.ID,
		"reuseWorkspaceBlank": true,
	}); rpcErr == nil || rpcErr.Code != "session-conflict" {
		t.Fatalf("cwd mismatch error = %#v", rpcErr)
	}
	if got := permissionEventValues(otherCWD); !reflect.DeepEqual(got, initial[otherCWD.Header.ID]) {
		t.Fatalf("cwd-conflict permissions = %#v, want %#v", got, initial[otherCWD.Header.ID])
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

func latestPermissionOrigin(session *Session) string {
	session.mu.Lock()
	defer session.mu.Unlock()
	for index := len(session.Events) - 1; index >= 0; index-- {
		if session.Events[index].Type != "permission/preset" {
			continue
		}
		data, _ := session.Events[index].Data.(map[string]any)
		origin, _ := data["origin"].(string)
		return origin
	}
	return ""
}

func workspaceSessionIDs(e *Engine, workspaceID string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]string(nil), e.workspaces[workspaceID].SessionIDs...)
}
