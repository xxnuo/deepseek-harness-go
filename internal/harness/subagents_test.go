package harness

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCreateSubagentPublicAPI(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "subagent-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(parent, ModelSelection{Provider: "echo", Model: "echo"}); err != nil {
		t.Fatal(err)
	}

	child, err := e.CreateSubagent(context.Background(), parent, "subagent-child", "")
	if err != nil {
		t.Fatalf("CreateSubagent: %v", err)
	}
	if child != "subagent-child" {
		t.Fatalf("child id = %q, want subagent-child", child)
	}
	session, err := e.getSession(child)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	gotParent := session.Header.ParentSession
	gotOrigin := session.Header.Origin
	gotMode := session.Header.Mode
	gotModel := session.Model
	session.mu.Unlock()
	if gotParent != parent || gotOrigin != "subagent" || gotMode != "continuable" {
		t.Fatalf("child metadata = parent %q origin %q mode %q", gotParent, gotOrigin, gotMode)
	}
	if gotModel != (ModelSelection{Provider: "echo", Model: "echo"}) {
		t.Fatalf("child model = %#v, want echo/echo", gotModel)
	}

	value, rpcErr := e.subagentList(context.Background(), map[string]any{"parentSessionId": parent})
	if rpcErr != nil {
		t.Fatalf("subagentList: %v", rpcErr)
	}
	list, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("subagentList value = %T", value)
	}
	entries, ok := list["entries"].([]map[string]any)
	if !ok || len(entries) != 1 || entries[0]["id"] != child {
		t.Fatalf("subagent entries = %#v", list["entries"])
	}
	value, rpcErr = e.subagentPrompt(context.Background(), map[string]any{
		"parentSessionId": parent,
		"childSessionId":  child,
		"content":         []any{map[string]any{"type": "text", "text": "continue"}},
	})
	if rpcErr != nil {
		t.Fatalf("subagentPrompt: %v", rpcErr)
	}
	messageID, _ := value.(map[string]any)["messageId"].(string)
	if messageID == "" {
		t.Fatalf("subagentPrompt receipt = %#v", value)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.WaitForIdle(waitCtx, child); err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	found := false
	for _, event := range session.Events {
		if event.Type != "user/message" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		if data["id"] != messageID {
			continue
		}
		source, _ := data["source"].(map[string]any)
		found = source["kind"] == "coordinator" && source["form"] == "relay" && source["senderSessionId"] == parent
	}
	if !found {
		t.Fatal("continued subagent message was not attributed to its coordinator")
	}
}

func TestSubagentPromptErrorDetailsUseStableAddressShape(t *testing.T) {
	const parentID, childID = "parent-address", "child-address"
	cases := []struct {
		name       string
		err        error
		code       string
		message    string
		wantDetail map[string]any
	}{
		{
			name:       "parent unavailable",
			err:        subagentServiceError("PARENT_UNAVAILABLE", "private parent detail", nil),
			code:       "subagent/parent-unavailable",
			message:    "private parent detail",
			wantDetail: map[string]any{"parentSessionId": parentID},
		},
		{
			name:       "not found",
			err:        rpcError("subagent-not-found", "private missing detail", nil),
			code:       "subagent/not-found",
			message:    "private missing detail",
			wantDetail: map[string]any{"parentSessionId": parentID, "childSessionId": childID},
		},
		{
			name:       "not resumable",
			err:        subagentServiceError("NOT_RESUMABLE", "private child detail", nil),
			code:       "subagent/not-resumable",
			message:    "subagent cannot be resumed",
			wantDetail: map[string]any{"childSessionId": childID},
		},
		{
			name:       "unauthorized",
			err:        subagentServiceError("UNAUTHORIZED", "private ownership detail", nil),
			code:       "subagent/unauthorized",
			message:    "subagent does not belong to this parent",
			wantDetail: map[string]any{"childSessionId": childID},
		},
		{
			name:       "delivery unavailable",
			err:        subagentServiceError("DRAINING", "private drain detail", nil),
			code:       "subagent/delivery-unavailable",
			message:    "subagent follow-up is temporarily unavailable",
			wantDetail: map[string]any{"childSessionId": childID},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := subagentPromptError(tc.err, parentID, childID)
			if got == nil || got.Code != tc.code || got.Message != tc.message {
				t.Fatalf("error = %#v, want %s/%q", got, tc.code, tc.message)
			}
			if details, ok := got.Details.(map[string]any); !ok || !reflect.DeepEqual(details, tc.wantDetail) {
				t.Fatalf("details = %#v, want %#v", got.Details, tc.wantDetail)
			}
		})
	}
}

func TestOrdinarySessionFenceIncludesBusyReason(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "ordinary-fence-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSubagent(t.Context(), parent, "ordinary-fence-child", "")
	if err != nil {
		t.Fatal(err)
	}
	_, rpcErr := dispatchTestRPC(t, e, "session.history", map[string]any{"sessionId": child})
	if rpcErr == nil || rpcErr.Code != "session/agent-busy" {
		t.Fatalf("ordinary child history error = %#v", rpcErr)
	}
	if details, ok := rpcErr.Details.(map[string]any); !ok || details["reason"] != "use subagent delivery for this child session" {
		t.Fatalf("ordinary child history details = %#v", rpcErr.Details)
	}
}

func TestDrainSubagentChildrenReleasesSelectedBranchOnly(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "drain-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	target, err := e.CreateSubagent(context.Background(), parent, "drain-target", "")
	if err != nil {
		t.Fatal(err)
	}
	sibling, err := e.CreateSubagent(context.Background(), parent, "drain-sibling", "")
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := e.CreateSubagent(context.Background(), target, "drain-grandchild", "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.DrainSubagentChildren(ctx, parent, []string{target, target, "missing"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{target, grandchild} {
		s := mustSession(t, e, id)
		s.mu.Lock()
		attached := s.attached
		s.mu.Unlock()
		if attached {
			t.Fatalf("released session %q remains attached", id)
		}
	}
	s := mustSession(t, e, sibling)
	s.mu.Lock()
	siblingAttached := s.attached
	s.mu.Unlock()
	if !siblingAttached {
		t.Fatal("unselected sibling was released")
	}
}

func TestDrainSubagentChildrenUsesModelActivationLifecycle(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	provider := newQueuedTestProvider()
	e.RegisterProvider(provider)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "activation-drain-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(parent, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	result := executeRegisteredTool(t, e, "subagent", parent, map[string]any{
		"description": "activation drain child", "prompt": "blocked child work",
	})
	childID, _ := result.Value.(map[string]any)["subagentId"].(string)
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("child model request did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := e.DrainSubagentChildren(ctx, parent, []string{childID}); err != nil {
		t.Fatal(err)
	}
	e.modelSubagentMu.Lock()
	activation := e.modelSubagentActivations[childID]
	e.modelSubagentMu.Unlock()
	if activation != nil {
		t.Fatal("selected drain returned before removing the model activation")
	}
	child := mustSession(t, e, childID)
	child.mu.Lock()
	attached := child.attached
	child.mu.Unlock()
	if attached {
		t.Fatal("selected drain returned before detaching the child session")
	}
	parentSession := mustSession(t, e, parent)
	parentSession.mu.Lock()
	defer parentSession.mu.Unlock()
	for _, event := range parentSession.Events {
		if event.Type != "agent/inbox/spliced" && event.Type != "user/message" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		if message, ok := data["message"].(map[string]any); ok {
			data = message
		}
		source, _ := data["source"].(map[string]any)
		if source["kind"] == "subagent-settled" && source["senderSessionId"] == childID {
			return
		}
		if inserted, ok := data["inserted"].([]any); ok {
			for _, value := range inserted {
				message, _ := value.(map[string]any)
				source, _ := message["source"].(map[string]any)
				if source["kind"] == "subagent-settled" && source["senderSessionId"] == childID {
					return
				}
			}
		}
	}
	t.Fatal("selected drain did not deliver a settlement notice")
}

func TestDrainSubagentChildrenAuthorizationAndColdTargets(t *testing.T) {
	e := newIntegrationEngine(t)
	first, err := e.CreateSession(context.Background(), e.Config().Workspace, "drain-first", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.CreateSession(context.Background(), e.Config().Workspace, "drain-second", "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSubagent(context.Background(), first, "drain-owned", "")
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := e.CreateSubagent(context.Background(), child, "drain-not-direct", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.DrainSubagentChildren(context.Background(), second, []string{child}); err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("foreign parent drain = %v", err)
	}
	if err := e.DrainSubagentChildren(context.Background(), first, []string{grandchild}); err == nil || !strings.Contains(err.Error(), "direct child") {
		t.Fatalf("grandchild drain = %v", err)
	}
	cold := mustSession(t, e, child)
	cold.mu.Lock()
	cold.attached = false
	cold.mu.Unlock()
	if err := e.DrainSubagentChildren(context.Background(), first, []string{child, "missing"}); err != nil {
		t.Fatalf("cold/missing drain = %v", err)
	}
}

func TestDrainSubagentChildrenRejectsNewWorkDuringRelease(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "drain-gate-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSubagent(context.Background(), parent, "drain-gate-child", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, child)
	if err := e.beginSubagentDrain(s); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.mu.Lock()
		s.draining = false
		s.mu.Unlock()
	})
	if _, err := e.Prompt(context.Background(), child, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "must not queue"}}}); err == nil || !strings.Contains(err.Error(), "session-draining") {
		t.Fatalf("prompt during drain = %v", err)
	}
	if _, err := e.CreateSubagent(context.Background(), child, "drain-gate-grandchild", ""); err == nil || !strings.Contains(err.Error(), "parent-unavailable") {
		t.Fatalf("subagent creation during drain = %v", err)
	}
}
