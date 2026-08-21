package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestCreateSubagentPublicAPI(t *testing.T) {
	e := newIntegrationEngine(t)
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

	value, rpcErr := e.subagentList(map[string]any{"parentSessionId": parent})
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

func TestSubagentReportQuietAndNextStep(t *testing.T) {
	e := newIntegrationEngine(t)
	create := func(parentID, childID string) (*Session, *Session) {
		t.Helper()
		parent, err := e.CreateSession(context.Background(), e.Config().Workspace, parentID, "")
		if err != nil {
			t.Fatal(err)
		}
		if err := e.SelectModel(parent, ModelSelection{Provider: "echo", Model: "echo"}); err != nil {
			t.Fatal(err)
		}
		child, err := e.CreateSubagent(context.Background(), parent, childID, "")
		if err != nil {
			t.Fatal(err)
		}
		return mustSession(t, e, parent), mustSession(t, e, child)
	}
	hasReport := func(session *Session) bool {
		t.Helper()
		tools, err := e.toolsForSession(session)
		if err != nil {
			t.Fatal(err)
		}
		for _, tool := range tools {
			if tool.Name == "report" {
				return true
			}
		}
		return false
	}

	quietParent, quietChild := create("report-quiet-parent", "report-quiet-child")
	if hasReport(quietParent) || !hasReport(quietChild) {
		t.Fatalf("report visibility parent=%v child=%v", hasReport(quietParent), hasReport(quietChild))
	}
	messageID, err := e.ReportFromSubagent(context.Background(), quietChild.Header.ID, []ContentBlock{{Type: "text", Text: "quiet finding"}}, SubagentReportQuiet)
	if err != nil {
		t.Fatal(err)
	}
	quietParent.mu.Lock()
	if quietParent.Running || len(quietParent.steering) != 1 {
		quietParent.mu.Unlock()
		t.Fatalf("quiet report state running=%v steering=%d", quietParent.Running, len(quietParent.steering))
	}
	queued := quietParent.steering[0]
	quietParent.mu.Unlock()
	if queued.id != messageID || queued.source["kind"] != "subagent-report" || queued.source["form"] != "relay" ||
		queued.source["senderSessionId"] != quietChild.Header.ID {
		t.Fatalf("quiet report = %#v", queued.message())
	}

	wakingParent, wakingChild := create("report-waking-parent", "report-waking-child")
	tool := e.tools["report"]
	result, err := tool.Execute(context.Background(), ToolCall{
		Name: "report", SessionID: wakingChild.Header.ID,
		Arguments: json.RawMessage(`{"output":"wake finding"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	value, _ := result.Value.(map[string]any)
	wakingMessageID, _ := value["messageId"].(string)
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := e.WaitForIdle(waitCtx, wakingParent.Header.ID); err != nil {
		t.Fatal(err)
	}
	wakingParent.mu.Lock()
	events := append([]Event(nil), wakingParent.Events...)
	wakingParent.mu.Unlock()
	found := false
	for _, event := range events {
		if event.Type != "user/message" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		source, _ := data["source"].(map[string]any)
		if data["id"] == wakingMessageID && source["kind"] == "subagent-report" && source["form"] == "relay" &&
			source["senderSessionId"] == wakingChild.Header.ID && strings.Contains(contentValueText(data), "wake finding") {
			found = true
		}
	}
	if !found {
		t.Fatalf("waking report %q missing from events %#v", wakingMessageID, events)
	}
}

func TestSubagentReportRejectsLegacyWakeup(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.SubagentReportDelivery = "wakeup"
	if _, err := New(WithConfig(cfg)); err == nil || !strings.Contains(err.Error(), "quiet or next-step") {
		t.Fatalf("New(reportDelivery=wakeup) = %v", err)
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
