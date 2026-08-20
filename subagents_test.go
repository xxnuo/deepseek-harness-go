package harness

import (
	"context"
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
