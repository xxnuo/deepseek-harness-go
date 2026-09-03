package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestJSONLSessionStoreLazyMaterializationAndSafeLocation(t *testing.T) {
	root := t.TempDir()
	store, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta := SessionHeader{Version: SessionFormatVersion, ID: "../../escape😀", CreatedAt: 1, CWD: "/work/demo"}
	if err := store.Create(context.Background(), meta, SessionLogOffset(meta.SeedLength)); err != nil {
		t.Fatal(err)
	}
	location, ok := store.Locate(meta)
	if !ok || location.Kind != "jsonl" {
		t.Fatalf("location = %#v", location)
	}
	if filepath.Clean(location.Path) == filepath.Clean(filepath.Join(root, "..", "..", "escape😀")) || !strings.HasPrefix(location.Path, filepath.Clean(root)+string(os.PathSeparator)) {
		t.Fatalf("unsafe location %q", location.Path)
	}
	if !strings.Contains(location.Path, "~D83D~DE00") || !strings.Contains(location.Path, "~002F") {
		t.Fatalf("location was not UTF-16/path encoded: %q", location.Path)
	}
	if _, err := os.Stat(location.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("blank session materialized: %v", err)
	}
	listed, err := store.List(context.Background())
	if err != nil || len(listed) != 0 {
		t.Fatalf("list=%#v err=%v", listed, err)
	}
	start := Event{Type: "turn/start", Seq: 0, Time: 10, Data: map[string]any{"turn": 1}}
	if err := store.Append(context.Background(), meta.ID, []Event{start}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(location.Path); err != nil {
		t.Fatal(err)
	}
	listed, err = store.List(context.Background())
	if err != nil || len(listed) != 1 || listed[0].ID != meta.ID {
		t.Fatalf("list=%#v err=%v", listed, err)
	}
}

func TestJSONLSessionStoreCrashRepairAndLiveOwnership(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	meta := SessionHeader{Version: SessionFormatVersion, ID: "repair", CreatedAt: 1, CWD: "/work"}
	if err := store.Create(ctx, meta, SessionLogOffset(meta.SeedLength)); err != nil {
		t.Fatal(err)
	}
	events := []Event{
		{Type: "turn/start", Seq: 0, Time: 10, Data: map[string]any{"turn": 1}},
		{Type: "step/start", Seq: 1, Time: 11, Data: map[string]any{"turn": 1, "step": 1}},
		{Type: "assistant/message", Seq: 2, Time: 12, SurfaceOp: "append", Data: map[string]any{
			"turn": 1, "step": 1,
			"message": map[string]any{"content": []ContentBlock{{Type: "tool-call", ID: "call-1", Name: "write", Arguments: `{}`}}},
		}},
		{Type: "tool/call", Seq: 3, Time: 13, Data: map[string]any{"turn": 1, "step": 1, "callId": "call-1", "name": "write", "arguments": `{}`}},
	}
	for _, event := range events {
		if err := store.Append(ctx, meta.ID, []Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	live, err := store.Inspect(ctx, meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(live.Events) != len(events) {
		t.Fatalf("live inspection repaired active turn: %#v", live.Events)
	}
	if _, err := store.Load(ctx, meta.ID); err == nil || !strings.Contains(err.Error(), "live session") {
		t.Fatalf("live load error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	inspection, err := store.Inspect(ctx, meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(inspection.Events) != 7 || inspection.Events[4].Type != "tool/result" || inspection.Events[5].Type != "step/end" || inspection.Events[6].Type != "turn/end" {
		t.Fatalf("inspection repair = %#v", inspection.Events)
	}
	if errorValue := nestedMap(inspection.Events[4].Data, "error"); errorValue["code"] != "TOOL_OUTCOME_UNKNOWN" {
		t.Fatalf("synthetic tool error = %#v", errorValue)
	}
	raw, found, err := store.ReadRaw(ctx, meta.ID)
	if err != nil || !found || strings.Count(raw.Content, "\n") != 5 {
		t.Fatalf("raw before repair found=%v lines=%d err=%v", found, strings.Count(raw.Content, "\n"), err)
	}
	loaded, err := store.Load(ctx, meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.Events, inspection.Events) {
		t.Fatalf("loaded=%#v inspection=%#v", loaded.Events, inspection.Events)
	}
	raw, _, err = store.ReadRaw(ctx, meta.ID)
	if err != nil || strings.Count(raw.Content, "\n") != 8 {
		t.Fatalf("raw after repair lines=%d err=%v", strings.Count(raw.Content, "\n"), err)
	}
	next := Event{Type: "turn/start", Seq: 7, Time: 20, Data: map[string]any{"turn": 2}}
	if err := store.Append(ctx, meta.ID, []Event{next}); err != nil {
		t.Fatal(err)
	}
	if inspected, err := store.Inspect(ctx, meta.ID); err != nil || len(inspected.Events) != 8 {
		t.Fatalf("append after repair = %#v err=%v", inspected.Events, err)
	}
}

func TestJSONLSessionStoreTruncatesTornTailAndTracksRevision(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	meta := SessionHeader{Version: SessionFormatVersion, ID: "torn", CreatedAt: 1}
	if err := store.Create(ctx, meta, SessionLogOffset(meta.SeedLength)); err != nil {
		t.Fatal(err)
	}
	events := []Event{
		{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": 1}},
		{Type: "turn/end", Seq: 1, Time: 2, Data: map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}},
	}
	if err := store.Append(ctx, meta.ID, events); err != nil {
		t.Fatal(err)
	}
	before, err := store.ListSnapshots(ctx)
	if err != nil || len(before) != 1 {
		t.Fatalf("snapshots=%#v err=%v", before, err)
	}
	location, _ := store.Locate(meta)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(location.Path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"type":"turn/start"`); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	store, err = NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	loaded, err := store.Load(ctx, meta.ID)
	if err != nil || len(loaded.Events) != 2 || loaded.Events[0].Type != "turn/start" || loaded.Events[1].Type != "turn/end" {
		t.Fatalf("loaded=%#v err=%v", loaded.Events, err)
	}
	raw, _, err := store.ReadRaw(ctx, meta.ID)
	if err != nil || !strings.HasSuffix(raw.Content, "\n") || strings.Count(raw.Content, "\n") != 3 {
		t.Fatalf("raw=%q err=%v", raw.Content, err)
	}
	after, err := store.ListSnapshots(ctx)
	if err != nil || len(after) != 1 || before[0].Revision == after[0].Revision {
		t.Fatalf("before=%#v after=%#v err=%v", before, after, err)
	}
}

func TestJSONLSessionStoreRejectsCommittedCorruption(t *testing.T) {
	root := t.TempDir()
	store, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	meta := SessionHeader{Version: SessionFormatVersion, ID: "corrupt", CreatedAt: 1}
	location, _ := store.Locate(meta)
	if err := os.MkdirAll(filepath.Dir(location.Path), 0o700); err != nil {
		t.Fatal(err)
	}
	header, _ := marshalSessionHeader(meta, SessionLogOffset(meta.SeedLength))
	end, _ := json.Marshal(Event{Type: "turn/end", Seq: 1, Time: 2, Data: map[string]any{"turn": 1}})
	content := append(append(append(header, '\n'), []byte("not json\n")...), append(end, '\n')...)
	if err := os.WriteFile(location.Path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Load(context.Background(), meta.ID); err == nil || !strings.Contains(err.Error(), "unparsable committed event") {
		t.Fatalf("corruption error = %v", err)
	}
}

func TestJSONLSessionStoreRejectsInvalidAppendWithoutMutation(t *testing.T) {
	ctx := context.Background()
	store, err := NewJSONLSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta := SessionHeader{Version: SessionFormatVersion, ID: "invalid", CreatedAt: 1}
	if err := store.Create(ctx, meta, SessionLogOffset(meta.SeedLength)); err != nil {
		t.Fatal(err)
	}
	bad := Event{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": make(chan int)}}
	if err := store.Append(ctx, meta.ID, []Event{bad}); err == nil || !strings.Contains(err.Error(), "JSON-serializable") {
		t.Fatalf("invalid append error = %v", err)
	}
	location, _ := store.Locate(meta)
	if _, err := os.Stat(location.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid append materialized a file: %v", err)
	}
}

func nestedMap(value any, key string) map[string]any {
	data, _ := value.(map[string]any)
	nested, _ := data[key].(map[string]any)
	return nested
}
