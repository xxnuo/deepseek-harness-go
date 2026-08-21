package harness

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestAgentPresetSelectPersistsEffectivePresetAndForwardsEvent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}

	sessionID, err := e.CreateSession(t.Context(), cfg.Workspace, "preset-selection", "standard")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	host := e.SubscribeHost(ctx)
	payload, err := json.Marshal(map[string]any{"sessionId": sessionID, "agentPreset": "minimal"})
	if err != nil {
		t.Fatal(err)
	}
	value, rpcErr := e.dispatch(ctx, "agentPreset.select", payload)
	if rpcErr != nil || value.(map[string]any)["agentPreset"] != "minimal" {
		t.Fatalf("agentPreset.select = %#v, %#v", value, rpcErr)
	}

	select {
	case frame := <-host:
		if frame["type"] != "host/remote-event" || frame["event"] != "agent-preset/selected" {
			t.Fatalf("forwarded frame = %#v", frame)
		}
		if got, _ := frame["args"].([]any); len(got) != 2 || got[0] != sessionID || got[1] != "minimal" {
			t.Fatalf("forwarded args = %#v", frame["args"])
		}
	case <-ctx.Done():
		t.Fatal("agent preset selection was not forwarded")
	}

	session, err := e.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	headerPreset := session.Header.AgentPreset
	effectivePreset := sessionAgentPreset(session.Header, session.Events)
	last := session.Events[len(session.Events)-1]
	session.mu.Unlock()
	if headerPreset != "standard" || effectivePreset != "minimal" {
		t.Fatalf("header=%q effective=%q", headerPreset, effectivePreset)
	}
	if last.Type != "agent-preset/selected" || last.Data.(map[string]any)["agentPreset"] != "minimal" {
		t.Fatalf("selection event = %#v", last)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	reloaded, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reloaded.Close() })
	rows := reloaded.ListSessions()
	if len(rows) != 1 || rows[0].AgentPreset != "minimal" {
		t.Fatalf("reloaded sessions = %#v", rows)
	}
}

func TestCreateSessionAdoptsOnlyMatchingEffectivePreset(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "preset-adoption", "standard")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	if _, err := e.appendEvent(s, "agent-preset/selected", map[string]any{"agentPreset": "minimal"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CreateSession(t.Context(), e.Config().Workspace, id, "minimal"); err != nil {
		t.Fatalf("matching effective preset was refused: %v", err)
	}
	if _, err := e.CreateSession(t.Context(), e.Config().Workspace, id, ""); err != nil {
		t.Fatalf("preset-less adoption was refused: %v", err)
	}
	_, err = e.CreateSession(t.Context(), e.Config().Workspace, id, "standard")
	var conflict *AgentPresetConflictError
	if !errors.As(err, &conflict) || conflict.ExistingPreset != "minimal" {
		t.Fatalf("stale preset conflict = %#v", err)
	}
	if got := errorToRPC(err); got.Code != "agent-preset-conflict" || got.Details.(map[string]any)["existingPreset"] != "minimal" {
		t.Fatalf("RPC conflict = %#v", got)
	}
}

func TestSessionCreateRPCPreservesOmittedPresetDuringBlankReuse(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace, _, err := e.CreateWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = "rpc-preset-adoption"
	created, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId": workspace.WorkspaceID,
		"sessionId":   sessionID,
		"agentPreset": "standard",
	})
	if rpcErr != nil || created["agentPreset"] != "standard" {
		t.Fatalf("initial create = %#v, %#v", created, rpcErr)
	}
	if _, rpcErr := dispatchTestRPC(t, e, "agentPreset.select", map[string]any{
		"sessionId": sessionID, "agentPreset": "minimal",
	}); rpcErr != nil {
		t.Fatal(rpcErr)
	}

	reused, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId":         workspace.WorkspaceID,
		"sessionId":           sessionID,
		"reuseWorkspaceBlank": true,
	})
	if rpcErr != nil || reused["agentPreset"] != "minimal" {
		t.Fatalf("preset-less reuse = %#v, %#v", reused, rpcErr)
	}
	if _, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId": workspace.WorkspaceID,
		"sessionId":   sessionID,
		"agentPreset": "standard",
	}); rpcErr == nil || rpcErr.Code != "agent-preset-conflict" {
		t.Fatalf("explicit stale preset = %#v", rpcErr)
	}
	if _, rpcErr := dispatchTestRPC(t, e, "session.create", map[string]any{
		"workspaceId": workspace.WorkspaceID,
		"agentPreset": true,
	}); rpcErr == nil || rpcErr.Code != "bad-request" {
		t.Fatalf("non-string preset = %#v", rpcErr)
	}
}
