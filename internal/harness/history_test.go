package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readUpstreamSessionSnapshot(t *testing.T, path string) *Session {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), filepath.FromSlash(path)))
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) == 0 {
		t.Fatal("empty upstream session snapshot")
	}
	var restored bytes.Buffer
	restored.Write(lines[0])
	restored.WriteByte('\n')
	seq := 0
	for _, line := range lines[1:] {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		typ, _ := record["type"].(string)
		if typ == "text-chunks" || typ == "reasoning-chunks" || typ == "tool-call-chunks" {
			record["seq0"], record["time0"] = seq, seq
		} else {
			record["seq"], record["time"] = seq, seq
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		events, err := decodeSessionStorageRecord(encoded)
		if err != nil {
			t.Fatal(err)
		}
		seq += len(events)
		restored.Write(encoded)
		restored.WriteByte('\n')
	}
	session, err := readSession(&restored)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

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
	s.Events = append(s.Events, Event{Type: "assistant/message", Seq: SessionSeq(len(s.Events)), Data: map[string]any{"content": []ContentBlock{{Type: "text", Text: "markerless"}}}})
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
	s := readUpstreamSessionSnapshot(t, "testdata/upstream/snapshots/sdk/text-turn/session.v2.jsonl")
	if s.Header.ID != "{{session:1}}" || len(s.Events) != 22 {
		t.Fatalf("upstream session = id %q, events %d", s.Header.ID, len(s.Events))
	}
	for seq, event := range s.Events {
		if int(event.Seq) != seq {
			t.Fatalf("event %d has seq %d", seq, event.Seq)
		}
	}
	foundTextDelta := false
	for _, event := range s.Events {
		data, _ := event.Data.(map[string]any)
		if event.Type == "assistant/message" && streamContainsText(data["stream"], "SD") {
			foundTextDelta = true
			break
		}
	}
	if !foundTextDelta {
		t.Fatal("expanded text chunk was not found")
	}
	var message Event
	for _, event := range s.Events {
		if event.Type == "assistant/message" {
			message = event
			break
		}
	}
	if message.SourceEventSeqs != nil || !isAppendSurfaceEvent(message) {
		t.Fatalf("assistant provenance = %#v", message)
	}

	toolSession := readUpstreamSessionSnapshot(t, "testdata/upstream/snapshots/sdk/bash-tool/session.v2.jsonl")
	foundToolDelta := false
	for _, event := range toolSession.Events {
		data, _ := event.Data.(map[string]any)
		if event.Type == "assistant/message" {
			for _, member := range assistantStreamChunks(data["stream"]) {
				if member.chunk["type"] == "tool-call-delta" && member.chunk["name"] == "bash" {
					foundToolDelta = true
				}
			}
		}
	}
	if !foundToolDelta {
		t.Fatal("packed tool-call chunks were not expanded")
	}
}

func TestReadUpstreamCompactionRebuildsLiveSurface(t *testing.T) {
	s := readUpstreamSessionSnapshot(t, "testdata/upstream/snapshots/session/compaction-recovery/session.v2.jsonl")
	var replacement Event
	for _, event := range s.Events {
		if event.Type == "user/message" {
			if _, _, ok := surfaceReplaceBounds(event.SurfaceOp); ok {
				replacement = event
				break
			}
		}
	}
	start, end, ok := surfaceReplaceBounds(replacement.SurfaceOp)
	if !ok || start != 7 || end != 8 || len(replacement.SourceEventSeqs) != 4 {
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
