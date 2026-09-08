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

func testSessionHeader(id string) SessionHeader {
	return SessionHeader{Version: SessionFormatVersion, ID: id, CreatedAt: 1, CWD: "/work"}
}

func testClosedTurn() []Event {
	return []Event{
		{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": 1}},
		{Type: "turn/end", Seq: 1, Time: 2, Data: map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}},
	}
}

func TestJSONLSessionHandleLazyMaterializationAndSafePath(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta := SessionHeader{Version: SessionFormatVersion, ID: "../../escape😀", CreatedAt: 1, CWD: "/work/demo"}
	handle, err := store.Create(ctx, meta, 0)
	if err != nil {
		t.Fatal(err)
	}
	path := store.pathFor(meta)
	if filepath.Clean(path) == filepath.Clean(filepath.Join(root, "..", "..", "escape😀")) || !strings.HasPrefix(path, filepath.Clean(root)+string(os.PathSeparator)) {
		t.Fatalf("unsafe path %q", path)
	}
	if !strings.Contains(path, "~D83D~DE00") || !strings.Contains(path, "~002F") {
		t.Fatalf("path was not UTF-16/path encoded: %q", path)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("blank session materialized: %v", err)
	}
	if snapshot, ok, err := store.Stat(ctx, meta.ID); err != nil || !ok || snapshot.Header.ID != meta.ID {
		t.Fatalf("stat=%#v ok=%v err=%v", snapshot, ok, err)
	}
	listed, err := store.List(ctx)
	if err != nil || len(listed) != 1 || listed[0].Header.ID != meta.ID {
		t.Fatalf("list=%#v err=%v", listed, err)
	}
	reader, err := store.Open(ctx, meta.ID, SessionAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	if events, err := reader.Read(ctx); err != nil || len(events) != 0 {
		t.Fatalf("pending read=%#v err=%v", events, err)
	}
	_ = reader.Close()
	if err := handle.Append(ctx, []Event{{Type: "turn/start", Seq: 0, Time: 10, Data: map[string]any{"turn": 1}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestJSONLSessionHandleOwnershipReadOnlyAndClosed(t *testing.T) {
	ctx := context.Background()
	store, err := NewJSONLSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta := testSessionHeader("ownership")
	writer, err := store.Create(ctx, meta, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, meta, 0); err == nil {
		t.Fatal("duplicate create succeeded")
	} else {
		var duplicate *SessionAlreadyExistsError
		if !errors.As(err, &duplicate) {
			t.Fatalf("duplicate create error = %T %v", err, err)
		}
	}
	if _, err := store.Open(ctx, meta.ID, SessionAccessWrite); err == nil {
		t.Fatal("second writer succeeded")
	} else {
		var owned *SessionAlreadyOwnedError
		if !errors.As(err, &owned) {
			t.Fatalf("ownership error = %T %v", err, err)
		}
	}
	if err := writer.Append(ctx, testClosedTurn()); err != nil {
		t.Fatal(err)
	}
	reader, err := store.Open(ctx, meta.ID, SessionAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Append(ctx, nil); err == nil {
		t.Fatal("read handle append succeeded")
	} else {
		var readOnly *SessionReadOnlyError
		if !errors.As(err, &readOnly) {
			t.Fatalf("read-only append error = %T %v", err, err)
		}
	}
	if err := reader.Flush(ctx); err == nil {
		t.Fatal("read handle flush succeeded")
	}
	_ = reader.Close()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Read(ctx); err == nil {
		t.Fatal("closed read succeeded")
	} else {
		var closed *SessionHandleClosedError
		if !errors.As(err, &closed) {
			t.Fatalf("closed error = %T %v", err, err)
		}
	}
	reopened, err := store.Open(ctx, meta.ID, SessionAccessWrite)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Append(ctx, []Event{
		{Type: "turn/start", Seq: 2, Time: 3, Data: map[string]any{"turn": 2}},
		{Type: "turn/end", Seq: 3, Time: 4, Data: map[string]any{"turn": 2, "reason": map[string]any{"kind": "completed"}}},
	}); err != nil {
		t.Fatal(err)
	}
	if events, err := reopened.Read(ctx, 1, 2); err != nil || len(events) != 2 || events[0].Seq != 1 || events[1].Seq != 2 {
		t.Fatalf("sliced read=%#v err=%v", events, err)
	}
	_ = reopened.Close()
}

func TestJSONLSessionColdReadBalancesWithoutRepairUntilWriteResume(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	meta := testSessionHeader("repair")
	writer, err := store.Create(ctx, meta, 0)
	if err != nil {
		t.Fatal(err)
	}
	events := []Event{
		{Type: "turn/start", Seq: 0, Time: 10, Data: map[string]any{"turn": 1}},
		{Type: "step/start", Seq: 1, Time: 11, Data: map[string]any{"turn": 1, "step": 1}},
		{Type: "assistant/message", Seq: 2, Time: 12, SurfaceOp: "append", Data: map[string]any{
			"turn": 1, "step": 1, "message": map[string]any{"content": []ContentBlock{{Type: "tool-call", ID: "call-1", Name: "write", Arguments: `{}`}}},
		}},
		{Type: "tool/call", Seq: 3, Time: 13, Data: map[string]any{"turn": 1, "step": 1, "callId": "call-1", "name": "write", "arguments": `{}`}},
	}
	if err := writer.Append(ctx, events); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	path := store.pathFor(meta)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := inspectStoredSession(ctx, store, meta.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(inspection.Events) != 7 || inspection.Events[4].Type != "tool/result" || inspection.Events[5].Type != "step/end" || inspection.Events[6].Type != "turn/end" {
		t.Fatalf("cold balance = %#v", inspection.Events)
	}
	afterRead, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, afterRead) {
		t.Fatal("read-only observation repaired the durable log")
	}
	resumed, loaded, err := openStoredSessionForWrite(ctx, store, meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.Events, inspection.Events) {
		t.Fatalf("resumed=%#v inspection=%#v", loaded.Events, inspection.Events)
	}
	_ = resumed.Close()
	afterWrite, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(afterWrite), "\n") != strings.Count(string(before), "\n")+3 {
		t.Fatalf("repair lines before=%d after=%d", strings.Count(string(before), "\n"), strings.Count(string(afterWrite), "\n"))
	}
}

func TestJSONLSessionWriteHandleTruncatesTornTailAndTracksRevision(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	meta := testSessionHeader("torn")
	writer, err := store.Create(ctx, meta, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(ctx, testClosedTurn()); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	before, ok, err := store.Stat(ctx, meta.ID)
	if err != nil || !ok {
		t.Fatalf("stat=%#v ok=%v err=%v", before, ok, err)
	}
	path := store.pathFor(meta)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`{"type":"turn/start"`)
	_ = file.Close()
	reader, err := store.Open(ctx, meta.ID, SessionAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	if events, err := reader.Read(ctx); err != nil || len(events) != 2 || events[0].Type != "turn/start" || events[1].Type != "turn/end" {
		t.Fatalf("torn read=%#v err=%v", events, err)
	}
	_ = reader.Close()
	if data, _ := os.ReadFile(path); !strings.HasSuffix(string(data), `{"type":"turn/start"`) {
		t.Fatal("read handle truncated torn tail")
	}
	resumed, _, err := openStoredSessionForWrite(ctx, store, meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.Append(ctx, []Event{
		{Type: "turn/start", Seq: 2, Time: 3, Data: map[string]any{"turn": 2}},
		{Type: "turn/end", Seq: 3, Time: 4, Data: map[string]any{"turn": 2, "reason": map[string]any{"kind": "completed"}}},
	}); err != nil {
		t.Fatal(err)
	}
	_ = resumed.Close()
	after, ok, err := store.Stat(ctx, meta.ID)
	if err != nil || !ok || before.Revision == after.Revision {
		t.Fatalf("before=%#v after=%#v ok=%v err=%v", before, after, ok, err)
	}
	verify, err := store.Open(ctx, meta.ID, SessionAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	if events, err := verify.Read(ctx); err != nil || len(events) != 4 {
		t.Fatalf("verified=%#v err=%v", events, err)
	}
	_ = verify.Close()
	_ = store.Close()
}

func TestJSONLSessionServiceFlushMaterializesActiveWritersOnly(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := store.Create(ctx, testSessionHeader("durable-empty"), 0)
	if err != nil {
		t.Fatal(err)
	}
	abandoned, err := store.Create(ctx, testSessionHeader("never-was"), 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = abandoned.Close()
	if err := store.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_ = durable.Close()
	if _, ok, err := store.Stat(ctx, "durable-empty"); err != nil || !ok {
		t.Fatalf("durable empty ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.Stat(ctx, "never-was"); err != nil || ok {
		t.Fatalf("abandoned ok=%v err=%v", ok, err)
	}
	_ = store.Close()
}

func TestJSONLSessionStoreRejectsCommittedCorruption(t *testing.T) {
	root := t.TempDir()
	store, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	meta := testSessionHeader("corrupt")
	path := store.pathFor(meta)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	header, _ := marshalSessionHeader(meta, 0)
	end, _ := json.Marshal(Event{Type: "turn/end", Seq: 1, Time: 2, Data: map[string]any{"turn": 1}})
	content := append(append(append(header, '\n'), []byte("not json\n")...), append(end, '\n')...)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Open(context.Background(), meta.ID, SessionAccessWrite); err == nil || !strings.Contains(err.Error(), "unparsable committed event") {
		t.Fatalf("corruption error = %v", err)
	}
}

func TestJSONLSessionHandleRejectsInvalidAppendWithoutMutation(t *testing.T) {
	ctx := context.Background()
	store, err := NewJSONLSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta := testSessionHeader("invalid")
	handle, err := store.Create(ctx, meta, 0)
	if err != nil {
		t.Fatal(err)
	}
	bad := Event{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": make(chan int)}}
	if err := handle.Append(ctx, []Event{bad}); err == nil || !strings.Contains(err.Error(), "JSON-serializable") {
		t.Fatalf("invalid append error = %v", err)
	}
	if _, err := os.Stat(store.pathFor(meta)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid append materialized a file: %v", err)
	}
	_ = handle.Close()
}
