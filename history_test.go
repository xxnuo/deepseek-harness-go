package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestHistoryPagesByMessageAndUsesEventSequenceCursor(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "history-page", "")
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "m"}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.appendEvent(s, "assistant/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "a"}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": i}); err != nil {
			t.Fatal(err)
		}
	}
	page, more, err := e.History(id, -1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !more || len(page) == 0 || page[0].Event.Seq != 6 {
		t.Fatalf("tail page = first seq %d len %d more %v", page[0].Event.Seq, len(page), more)
	}
	older, more, err := e.History(id, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	if more || len(older) != 3 || older[len(older)-1].Event.Seq != 2 {
		t.Fatalf("older page = %#v more %v", older, more)
	}
}

func TestHistorySkipsMarkerlessMessages(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "history-markerless", "")
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "append"}}}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.Events = append(s.Events, Event{Type: "assistant/message", Seq: len(s.Events), Data: map[string]any{"content": []ContentBlock{{Type: "text", Text: "markerless"}}}})
	s.mu.Unlock()
	page, more, err := e.History(id, -1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if more || len(page) != 2 || page[0].Event.Type != "user/message" || page[1].Event.Type != "assistant/message" {
		t.Fatalf("history = %#v, more=%v", page, more)
	}
	page, more, err = e.History(id, -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if more || len(page) != 2 {
		t.Fatalf("default max history = %#v, more=%v", page, more)
	}
}

func TestReadSessionPreservesReplacementSurfaceObject(t *testing.T) {
	log := bytes.NewBufferString(
		`{"type":"session","version":0,"id":"surface-object","createdAt":1}` + "\n" +
			`{"type":"user/message","seq":0,"time":1,"data":{},"surfaceOp":"append"}` + "\n" +
			`{"type":"user/message","seq":1,"time":2,"data":{},"surfaceOp":{"op":"replace","start":0,"end":0},"sourceEventSeqs":[0]}` + "\n",
	)
	s, err := readSession(log)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Events) != 2 {
		t.Fatalf("events = %#v", s.Events)
	}
	op, ok := s.Events[1].SurfaceOp.(map[string]any)
	if !ok || op["op"] != "replace" {
		t.Fatalf("surfaceOp = %#v", s.Events[1].SurfaceOp)
	}
	if _, err := json.Marshal(s.Events[1]); err != nil {
		t.Fatalf("replacement event is not serializable: %v", err)
	}
}

func TestReadUpstreamPackedSessionLog(t *testing.T) {
	f, err := os.Open("testdata/upstream/examples/jsonrpc-agent/tests/snapshots/text-turn/session.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s, err := readSession(f)
	if err != nil {
		t.Fatal(err)
	}
	if s.Header.ID != "sdk-snapshot-text" || len(s.Events) != 40 {
		t.Fatalf("upstream session = id %q, events %d", s.Header.ID, len(s.Events))
	}
	for seq, event := range s.Events {
		if event.Seq != seq {
			t.Fatalf("event %d has seq %d", seq, event.Seq)
		}
	}
	chunk, _ := s.Events[29].Data.(map[string]any)["chunk"].(map[string]any)
	if chunk["type"] != "text-delta" || chunk["text"] != "SD" {
		t.Fatalf("expanded chunk = %#v", chunk)
	}
	message := s.Events[37]
	if len(message.SourceEventSeqs) != 29 || message.SourceEventSeqs[0] != 8 || message.SourceEventSeqs[len(message.SourceEventSeqs)-1] != 36 || !isAppendSurfaceEvent(message) {
		t.Fatalf("assistant provenance = %#v", message)
	}

	toolLog, err := os.Open("testdata/upstream/examples/jsonrpc-agent/tests/snapshots/bash-tool/session.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer toolLog.Close()
	toolSession, err := readSession(toolLog)
	if err != nil {
		t.Fatal(err)
	}
	foundToolDelta := false
	for _, event := range toolSession.Events {
		data, _ := event.Data.(map[string]any)
		chunk, _ := data["chunk"].(map[string]any)
		if event.Type == "assistant/chunk" && chunk["type"] == "tool-call-delta" && chunk["name"] == "bash" {
			foundToolDelta = true
			break
		}
	}
	if !foundToolDelta {
		t.Fatal("packed tool-call chunks were not expanded")
	}
}

func TestReadUpstreamCompactionRebuildsLiveSurface(t *testing.T) {
	f, err := os.Open("testdata/upstream/examples/headless-agent/tests/snapshots/compaction-recovery/session.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s, err := readSession(f)
	if err != nil {
		t.Fatal(err)
	}
	replacement := s.Events[21]
	start, end, ok := surfaceReplaceBounds(replacement.SurfaceOp)
	if !ok || start != 4 || end != 4 || len(replacement.SourceEventSeqs) != 3 {
		t.Fatalf("replacement = %#v", replacement)
	}
	messages := transcriptMessages(s.Events, 1)
	if len(messages) != 4 || !strings.Contains(messages[0].Content, "automatically generated checkpoint") || messages[3].Content != "COMPACTION RECOVERED" {
		t.Fatalf("live transcript = %#v", messages)
	}
	if strings.Contains(messages[0].Content, "Establish a durable compaction premise before continuing") {
		t.Fatalf("shadowed prompt remained live: %#v", messages[0])
	}
}

func TestReadSessionAllowsOnlyMarkedUnknownEvents(t *testing.T) {
	header := `{"type":"session","version":0,"id":"future-events","createdAt":1}` + "\n"
	marked, err := readSession(bytes.NewBufferString(header + `{"type":"future/event","seq":0,"time":1,"data":null,"ignorable":true}` + "\n"))
	if err != nil || len(marked.Events) != 1 || !marked.Events[0].Ignorable {
		t.Fatalf("marked unknown event = %#v, %v", marked, err)
	}
	for _, line := range []string{
		`{"type":"future/event","seq":0,"time":1,"data":null}`,
		`{"type":"future/event","seq":0,"time":1,"data":null,"ignorable":false}`,
		`{"type":"future/event","seq":0,"time":1,"data":null,"ignorable":true,"surfaceOp":"append"}`,
		`{"type":"turn/start","seq":0,"time":1,"data":{"turn":1},"surfaceOp":"append"}`,
		`{"type":"turn/start","seq":0,"time":1,"data":{"turn":1},"sourceEventSeqs":[]}`,
		`{"type":"user/message","seq":0,"time":1,"data":{}}`,
	} {
		if _, err := readSession(bytes.NewBufferString(header + line + "\n")); err == nil {
			t.Fatalf("unknown event unexpectedly loaded: %s", line)
		}
	}
}

func TestEventMarshalPreservesExplicitEmptySourceEventSeqs(t *testing.T) {
	event := Event{Type: "assistant/message", Seq: 1, Time: 1, Data: map[string]any{}, SurfaceOp: "append", SourceEventSeqs: []int{}}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"sourceEventSeqs":[]`)) {
		t.Fatalf("event JSON = %s", raw)
	}
	var roundTrip Event
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip.SourceEventSeqs == nil || len(roundTrip.SourceEventSeqs) != 0 {
		t.Fatalf("sourceEventSeqs = %#v", roundTrip.SourceEventSeqs)
	}
}

func TestAppendEventWritesSurfaceMetadataOnlyOnSurfaceEvents(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "surface-write", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	boundary, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 1})
	if err != nil {
		t.Fatal(err)
	}
	message, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	if boundary.SurfaceOp != nil || !isAppendSurfaceEvent(message) {
		t.Fatalf("boundary/message surface ops = %#v / %#v", boundary.SurfaceOp, message.SurfaceOp)
	}
}
