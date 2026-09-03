package harness

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSessionExplicitLogReadAPI(t *testing.T) {
	session := &Session{Events: []Event{
		{Type: "one", Seq: 0, Data: map[string]any{"value": "original"}, SourceEventSeqs: []int{}},
		{Type: "two", Seq: 1, Data: map[string]any{"value": "second"}, SourceEventSeqs: []int{0}},
	}}
	if got := session.Seq(); got != SessionLogOffset(2) {
		t.Fatalf("Seq() = %d, want 2", got)
	}
	if event, ok := session.EventAt(SessionSeq(1)); !ok || event.Type != "two" {
		t.Fatalf("EventAt(1) = %#v, %v", event, ok)
	}
	for _, seq := range []SessionSeq{-1, 2} {
		if event, ok := session.EventAt(seq); ok {
			t.Fatalf("EventAt(%d) = %#v, true", seq, event)
		}
	}
	snapshot, err := session.SnapshotEvents(1, 2)
	if err != nil || len(snapshot) != 1 || snapshot[0].Type != "two" {
		t.Fatalf("SnapshotEvents(1,2) = %#v, %v", snapshot, err)
	}
	snapshot[0].Data.(map[string]any)["value"] = "mutated"
	snapshot[0].SourceEventSeqs[0] = 99
	if original, _ := session.EventAt(1); original.Data.(map[string]any)["value"] != "second" || original.SourceEventSeqs[0] != 0 {
		t.Fatalf("snapshot mutated live event = %#v", original)
	}
	for _, bounds := range [][]SessionLogOffset{{-1}, {2, 1}, {0, 3}} {
		if _, err := session.SnapshotEvents(bounds...); err == nil {
			t.Fatalf("SnapshotEvents(%v) accepted invalid range", bounds)
		}
	}
}

func TestSessionHeaderSeparatesSeedPresenceFromInheritedCount(t *testing.T) {
	seeded := SessionHeader{Version: SessionFormatVersion, ID: "seeded-empty", CreatedAt: 1, IsSeeded: true}
	wire, err := marshalSessionHeader(seeded, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wire), `"seedLength":0`) || strings.Contains(string(wire), "isSeeded") {
		t.Fatalf("seeded wire header = %s", wire)
	}
	parsed, ok, err := parseSessionHeader(wire)
	if err != nil || !ok || !parsed.IsSeeded || parsed.SeedLength != 0 {
		t.Fatalf("parsed seeded-empty header = %#v, %v, %v", parsed, ok, err)
	}
	unseededWire, err := marshalSessionHeader(SessionHeader{Version: SessionFormatVersion, ID: "plain", CreatedAt: 1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(unseededWire), "seedLength") {
		t.Fatalf("unseeded wire header = %s", unseededWire)
	}
	logical, err := json.Marshal(seeded)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logical), `"isSeeded":true`) || strings.Contains(string(logical), "seedLength") {
		t.Fatalf("logical header = %s", logical)
	}
}
