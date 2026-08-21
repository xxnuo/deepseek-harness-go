package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestSessionQueryToolsAreRegisteredWithIndependentSchemas(t *testing.T) {
	e := newIntegrationEngine(t)
	for _, name := range sessionQueryToolNames {
		tool := registeredTool(t, e, name)
		if tool.Schema.Name != name || tool.Execute == nil {
			t.Fatalf("tool %q = %#v", name, tool)
		}
		if tool.Schema.Parameters["type"] != "object" {
			t.Fatalf("tool %q parameters = %#v", name, tool.Schema.Parameters)
		}
	}
}

func TestSessionQueryToolsSearchTraceAndRead(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "query-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSubagent(context.Background(), parent, "query-child", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, parent)
	e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "find the durable needle"}}, "source": map[string]any{"kind": "user"}})
	e.appendEvent(s, "assistant/message", map[string]any{"message": map[string]any{"content": []ContentBlock{{Type: "text", Text: "done"}}}})
	rows, err := e.sessionQuerySearch(parent, "needle", 10)
	if err != nil || len(rows) != 1 || !strings.Contains(rows[0]["snippet"].(string), "needle") {
		t.Fatalf("search = %#v, %v", rows, err)
	}
	trace, err := e.sessionQueryTrace(parent, child)
	if err != nil || len(trace["ancestors"].([]map[string]any)) != 1 {
		t.Fatalf("trace = %#v, %v", trace, err)
	}
	read, err := e.sessionQueryEventRead(parent, parent, 0, 1, 1)
	if err != nil || read["event"] == nil {
		t.Fatalf("read = %#v, %v", read, err)
	}
}

func TestSessionQueryRejectsOtherWorkspace(t *testing.T) {
	e := newIntegrationEngine(t)
	a, _ := e.CreateSession(context.Background(), e.Config().Workspace, "query-a", "")
	b, _ := e.CreateSession(context.Background(), t.TempDir(), "query-b", "")
	if _, err := e.sessionQueryEventSearch(a, b, "x", 1); err == nil || !strings.Contains(err.Error(), "SESSION_QUERY_UNAUTHORIZED") {
		t.Fatalf("unexpected auth result: %v", err)
	}
}

func TestSessionQueryEventTraceTracksSurfaceReplacementChain(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "trace-chain", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	first, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "original"}}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.appendEventWithMetadata(s, "assistant/message", map[string]any{"message": map[string]any{"content": []ContentBlock{{Type: "text", Text: "replacement one"}}}}, map[string]any{"op": "replace", "start": first.Seq, "end": first.Seq}, []int{first.Seq}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.appendEventWithMetadata(s, "assistant/message", map[string]any{"message": map[string]any{"content": []ContentBlock{{Type: "text", Text: "replacement two"}}}}, map[string]any{"op": "replace", "start": second.Seq, "end": second.Seq}, []int{second.Seq}, false)
	if err != nil {
		t.Fatal(err)
	}
	trace, err := e.sessionQueryEventTrace(id, id, first.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if got := trace["replacementChain"].([]int); len(got) != 2 || got[0] != second.Seq || got[1] != second.Seq+1 {
		t.Fatalf("replacement chain = %#v", got)
	}
	if got := trace["target"].(map[string]any)["surface"]; got != string(sessionSurfaceShadowed) {
		t.Fatalf("target surface = %#v", got)
	}
}

func TestSessionQueryEventSearchExcludesCurrentActiveStep(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "active-step", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	if _, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "before step needle"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "step/start", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "active step needle"}}}); err != nil {
		t.Fatal(err)
	}
	tool := registeredTool(t, e, "session_event_search")
	result, err := tool.Execute(context.Background(), ToolCall{Name: "session_event_search", SessionID: id, Arguments: json.RawMessage(`{"query":"needle"}`)})
	if err != nil {
		t.Fatal(err)
	}
	text := toolResultText(result)
	if !strings.Contains(text, "before step needle") || strings.Contains(text, "active step needle") {
		t.Fatalf("active-step search output = %s", text)
	}
}

func TestSessionQueryEventReadUsesBeforeAndAfterArguments(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "read-window", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	for _, text := range []string{"zero", "one", "two"} {
		if _, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: text}}}); err != nil {
			t.Fatal(err)
		}
	}
	tool := registeredTool(t, e, "session_event_read")
	result, err := tool.Execute(context.Background(), ToolCall{Name: "session_event_read", SessionID: id, Arguments: json.RawMessage(`{"seq":1,"before":1,"after":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	value, ok := result.Value.(string)
	if !ok || value != toolResultText(result) {
		t.Fatalf("read value = %#v", result.Value)
	}
	if !strings.Contains(value, "Before\n- seq 0") || !strings.Contains(value, "After\n- seq 2") {
		t.Fatalf("read window = %q", value)
	}
}
