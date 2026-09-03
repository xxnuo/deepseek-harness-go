package harness

import (
	"context"
	"reflect"
	"testing"
)

func TestBuiltinTurnOutlineProjectionSnapshotAndFeed(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "turn-outline-registry", "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	frames := e.SubscribeMux(ctx)
	if _, err := e.appendEvent(session, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(session, "user/message", map[string]any{
		"content": []ContentBlock{{Type: "text", Text: "registry prompt"}},
		"source":  map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(session, "assistant/message", map[string]any{
		"message": map[string]any{"content": []ContentBlock{{Type: "text", Text: "registry answer"}}},
	}); err != nil {
		t.Fatal(err)
	}
	end, err := e.appendEvent(session, "turn/end", map[string]any{"turn": 1})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := e.SessionProjectionSnapshot(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"turn": 1, "seq": 0, "prompt": "registry prompt", "response": "registry answer"}}
	if got := snapshot.Values["turnOutline"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("turn outline snapshot = %#v, want %#v", got, want)
	}
	foundEnd := false
	for {
		select {
		case frame := <-frames:
			if frame["type"] == "session/projection" && frame["key"] == "turnOutline" && frame["seq"] == int(end.Seq) {
				foundEnd = true
			}
		default:
			if !foundEnd {
				t.Fatal("missing turnOutline feed")
			}
			return
		}
	}
}
