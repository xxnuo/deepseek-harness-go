package harness

import (
	"context"
	"encoding/json"
	"testing"
)

func TestForkSessionUsesTurnBoundaryAndCarriesTailMetadata(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "fork-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	source, err := e.getSession(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		typ  string
		data map[string]any
	}{
		{"turn/start", map[string]any{"turn": 1}},
		{"user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "seed"}}}},
		{"turn/end", map[string]any{"turn": 1, "reason": map[string]any{"kind": "error"}}},
		{"session/title", map[string]any{"title": "Seed title"}},
	} {
		if _, err := e.appendEvent(source, item.typ, item.data); err != nil {
			t.Fatal(err)
		}
	}
	childID, err := e.ForkSession(context.Background(), parent)
	if err != nil {
		t.Fatalf("ForkSession: %v", err)
	}
	child, err := e.getSession(childID)
	if err != nil {
		t.Fatal(err)
	}
	child.mu.Lock()
	seedLength, title, parentID := child.Header.SeedLength, child.Title, child.Header.ParentSession
	events := append([]Event(nil), child.Events...)
	child.mu.Unlock()
	if seedLength != 7 || len(events) != 8 || events[7].Type != "session/end-seed" || title != "Seed title" || parentID != parent {
		t.Fatalf("fork metadata = seed=%d events=%d title=%q parent=%q", seedLength, len(events), title, parentID)
	}

	titleSeq := 6
	if _, err := e.ForkSessionAt(context.Background(), parent, &titleSeq); err == nil {
		t.Fatal("fork anchored on trailing title unexpectedly succeeded")
	}
}

func TestForkSubagentUsesNearestWorkspaceAncestorAndOrdinaryOrigin(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(context.Background(), e.Config().Workspace, "fork-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	workspace, _, err := e.CreateWorkspace(e.Config().Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AttachSession(workspace.WorkspaceID, owner); err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSubagent(context.Background(), owner, "fork-child", "")
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := e.CreateSubagent(context.Background(), child, "fork-grandchild", "")
	if err != nil {
		t.Fatal(err)
	}
	source, err := e.getSession(grandchild)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		typ  string
		data map[string]any
	}{
		{"turn/start", map[string]any{"turn": 1}},
		{"user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "seed"}}}},
		{"turn/end", map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}},
	} {
		if _, err := e.appendEvent(source, item.typ, item.data); err != nil {
			t.Fatal(err)
		}
	}

	raw, err := json.Marshal(map[string]any{"sessionId": grandchild})
	if err != nil {
		t.Fatal(err)
	}
	value, rpcErr := e.dispatch(context.Background(), "session.fork", raw)
	if rpcErr != nil {
		t.Fatalf("session.fork subagent: %v", rpcErr)
	}
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("session.fork value = %T", value)
	}
	forkedID, _ := result["sessionId"].(string)
	forked, err := e.getSession(forkedID)
	if err != nil {
		t.Fatal(err)
	}
	forked.mu.Lock()
	parentID, origin := forked.Header.ParentSession, forked.Header.Origin
	forked.mu.Unlock()
	if parentID != grandchild || origin != "" {
		t.Fatalf("fork lineage = parent %q origin %q", parentID, origin)
	}
	items, _ := e.ListWorkspaces()
	if len(items) != 1 || !containsString(items[0].SessionIDs, forkedID) {
		t.Fatalf("workspace sessions = %#v, want fork %q", items, forkedID)
	}
}

func TestForkUsesLatestLoggedModelSelection(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "fork-routed", "")
	if err != nil {
		t.Fatal(err)
	}
	source, err := e.getSession(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		typ  string
		data map[string]any
	}{
		{"turn/start", map[string]any{"turn": 1}},
		{"user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "seed"}}}},
		{"turn/end", map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}},
		{"request/header", map[string]any{"header": map[string]any{"config": map[string]any{
			"provider": "inherited-provider", "model": "inherited-model", "reasoningEffort": "high",
		}}}},
	} {
		if _, err := e.appendEvent(source, item.typ, item.data); err != nil {
			t.Fatal(err)
		}
	}
	childID, err := e.ForkSession(context.Background(), parent)
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.getSession(childID)
	if err != nil {
		t.Fatal(err)
	}
	child.mu.Lock()
	model := child.Model
	child.mu.Unlock()
	if model != (ModelSelection{Provider: "inherited-provider", Model: "inherited-model", ReasoningEffort: "high"}) {
		t.Fatalf("fork model = %#v", model)
	}
}
