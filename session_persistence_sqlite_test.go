package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSQLiteSessionStoreRoundTripRepairAndRevision(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sessions.db")
	store, err := NewSQLiteSessionStore(path, SQLiteJournalWAL)
	if err != nil {
		t.Fatal(err)
	}
	meta := SessionHeader{
		Version: SessionFormatVersion, ID: "sqlite-round-trip", CreatedAt: 1000,
		CWD: "/work", ParentSession: "parent", SeedLength: 2, Origin: "subagent",
		DelegationDepth: 3, AgentPreset: "minimal",
	}
	if err := store.Create(ctx, meta); err != nil {
		t.Fatal(err)
	}
	if location, ok := store.Locate(meta); ok || location != (SessionLocation{}) {
		t.Fatalf("location = %#v, %v", location, ok)
	}
	if store.SupportsRawArtifacts() {
		t.Fatal("SQLite unexpectedly exposes raw artifacts")
	}
	if _, found, err := store.ReadRaw(ctx, meta.ID); found || err == nil {
		t.Fatalf("raw artifact found=%v err=%v", found, err)
	}
	if listed, err := store.List(ctx); err != nil || len(listed) != 0 {
		t.Fatalf("lazy list = %#v, %v", listed, err)
	}

	events := []Event{
		{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": 1}},
		{Type: "user/message", Seq: 1, Time: 2, SurfaceOp: "append", Data: map[string]any{"role": "user"}},
		{Type: "assistant/message", Seq: 2, Time: 3, SourceEventSeqs: []int{1}, SurfaceOp: map[string]any{"op": "replace", "start": 1, "end": 1}, Data: map[string]any{"turn": 1, "step": 1}},
		{Type: "turn/end", Seq: 3, Time: 4, Data: map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}},
	}
	if err := store.Append(ctx, meta.ID, events); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx, meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertSessionEventsJSON(t, loaded.Events, events)
	if loaded.Meta.CWD != meta.CWD || loaded.Meta.ParentSession != meta.ParentSession ||
		loaded.Meta.SeedLength != meta.SeedLength || loaded.Meta.Origin != meta.Origin ||
		loaded.Meta.DelegationDepth != meta.DelegationDepth || loaded.Meta.AgentPreset != meta.AgentPreset {
		t.Fatalf("metadata = %#v", loaded.Meta)
	}
	suffix, err := store.ReadFrom(ctx, meta.ID, 2)
	if err != nil || len(suffix.Events) != 2 || suffix.Events[0].Seq != 2 {
		t.Fatalf("suffix = %#v, %v", suffix.Events, err)
	}
	before, err := store.ListSnapshots(ctx)
	if err != nil || len(before) != 1 {
		t.Fatalf("snapshots = %#v, %v", before, err)
	}
	repeated, err := store.ListSnapshots(ctx)
	if err != nil || repeated[0].Revision != before[0].Revision {
		t.Fatalf("unstable revision: %#v %#v, %v", before, repeated, err)
	}
	if err := store.Append(ctx, meta.ID, []Event{{Type: "turn/start", Seq: 4, Time: 5, Data: map[string]any{"turn": 2}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(ctx, meta.ID); err == nil || !strings.Contains(err.Error(), "live session") {
		t.Fatalf("live load error = %v", err)
	}
	if inspected, err := store.Inspect(ctx, meta.ID); err != nil || len(inspected.Events) != 5 {
		t.Fatalf("live inspection = %#v, %v", inspected.Events, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	alias := path
	if runtime.GOOS != "windows" {
		alias = path + ".alias"
		if err := os.Symlink(path, alias); err != nil {
			t.Fatal(err)
		}
	}
	store, err = NewSQLiteSessionStore(alias, SQLiteJournalWAL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	reopened, err := store.Load(ctx, meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.Events) != 6 || reopened.Events[5].Type != "turn/end" {
		t.Fatalf("repaired events = %#v", reopened.Events)
	}
	after, err := store.ListSnapshots(ctx)
	if err != nil || len(after) != 1 || after[0].Revision == before[0].Revision {
		t.Fatalf("repair revision = %#v, %v", after, err)
	}
	if runtime.GOOS != "windows" && !strings.HasPrefix(string(after[0].Revision), strings.Split(string(before[0].Revision), ":incarnation:")[0]+":incarnation:") {
		t.Fatalf("same database changed source identity: %q -> %q", before[0].Revision, after[0].Revision)
	}
}

func TestSQLiteSessionStoreTornTailAndCommittedCorruption(t *testing.T) {
	ctx := context.Background()
	complete := []Event{
		{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": 1}},
		{Type: "turn/end", Seq: 1, Time: 2, Data: map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}},
	}

	t.Run("torn tail", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "sessions.db")
		store, err := NewSQLiteSessionStore(path, SQLiteJournalDelete)
		if err != nil {
			t.Fatal(err)
		}
		meta := SessionHeader{Version: SessionFormatVersion, ID: "torn", CreatedAt: 1}
		if err := store.Create(ctx, meta); err != nil {
			t.Fatal(err)
		}
		if err := store.Append(ctx, meta.ID, complete); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		db := openSQLiteProbe(t, path)
		if _, err := db.Exec(`INSERT INTO events (session_id, seq, type, time, data) VALUES (?, 2, 'turn/start', 3, '{not json')`, meta.ID); err != nil {
			t.Fatal(err)
		}
		_ = db.Close()

		store, err = NewSQLiteSessionStore(path, SQLiteJournalDelete)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		loaded, err := store.Load(ctx, meta.ID)
		if err != nil {
			t.Fatal(err)
		}
		assertSessionEventsJSON(t, loaded.Events, complete)
		var count int
		if err := store.db.QueryRow("SELECT COUNT(*) FROM events WHERE session_id = ?", meta.ID).Scan(&count); err != nil || count != 2 {
			t.Fatalf("stored event count = %d, %v", count, err)
		}
	})

	t.Run("committed corruption", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "sessions.db")
		store, err := NewSQLiteSessionStore(path, SQLiteJournalDelete)
		if err != nil {
			t.Fatal(err)
		}
		meta := SessionHeader{Version: SessionFormatVersion, ID: "corrupt", CreatedAt: 1}
		if err := store.Create(ctx, meta); err != nil {
			t.Fatal(err)
		}
		if err := store.Append(ctx, meta.ID, complete); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		db := openSQLiteProbe(t, path)
		if _, err := db.Exec("UPDATE events SET data = '{not json' WHERE session_id = ? AND seq = 0", meta.ID); err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
		store, err = NewSQLiteSessionStore(path, SQLiteJournalDelete)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if _, err := store.Load(ctx, meta.ID); err == nil || !strings.Contains(err.Error(), "unparsable committed event") {
			t.Fatalf("corruption error = %v", err)
		}
	})
}

func TestSQLiteSessionStoreSchemaPermissionsAndOwnership(t *testing.T) {
	parent := t.TempDir()
	if runtime.GOOS != "windows" {
		if err := os.Chmod(parent, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(parent, "sessions.db")
	store, err := NewSQLiteSessionStore(path, SQLiteJournalWAL)
	if err != nil {
		t.Fatal(err)
	}
	var applicationID, version int
	var journal string
	if err := store.db.QueryRow("PRAGMA application_id").Scan(&applicationID); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if applicationID != SessionSQLiteApplicationID || version != SessionSQLiteSchemaVersion || journal != "wal" {
		t.Fatalf("schema identity = app %d version %d journal %q", applicationID, version, journal)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("new database mode = %v, %v", infoMode(info), err)
		}
		if info, err := os.Stat(parent); err != nil || info.Mode().Perm() != 0o755 {
			t.Fatalf("parent mode = %v, %v", infoMode(info), err)
		}
	}

	existing := filepath.Join(t.TempDir(), "existing.db")
	if err := os.WriteFile(existing, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	store, err = NewSQLiteSessionStore(existing, SQLiteJournalDelete)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if info, err := os.Stat(existing); err != nil || info.Mode().Perm() != 0o644 {
			t.Fatalf("existing database mode = %v, %v", infoMode(info), err)
		}
	}

	unowned := filepath.Join(t.TempDir(), "unowned.db")
	db := openSQLiteProbe(t, unowned)
	if _, err := db.Exec("CREATE TABLE sqliteX (value TEXT)"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	result := make(chan error, 1)
	go func() {
		candidate, err := NewSQLiteSessionStore(unowned, SQLiteJournalWAL)
		if candidate != nil {
			_ = candidate.Close()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "unversioned schema") {
			t.Fatalf("ownership error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ownership rejection deadlocked")
	}
	db = openSQLiteProbe(t, unowned)
	defer db.Close()
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 0 {
		t.Fatalf("unowned user_version = %d, %v", version, err)
	}
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || journal != "delete" {
		t.Fatalf("unowned journal = %q, %v", journal, err)
	}
}

func TestSQLiteSessionStoreAppendTransactionRollsBack(t *testing.T) {
	ctx := context.Background()
	store, err := NewSQLiteSessionStore(":memory:", SQLiteJournalWAL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta := SessionHeader{Version: SessionFormatVersion, ID: "rollback", CreatedAt: 1}
	events := []Event{
		{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": 1}},
		{Type: "turn/end", Seq: 0, Time: 2, Data: map[string]any{"turn": 1}},
	}
	if err := store.appendTransaction(ctx, meta, events, true); err == nil {
		t.Fatal("duplicate seq transaction unexpectedly committed")
	}
	var sessions, storedEvents int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM sessions").Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("SELECT COUNT(*) FROM events").Scan(&storedEvents); err != nil {
		t.Fatal(err)
	}
	if sessions != 0 || storedEvents != 0 {
		t.Fatalf("partial transaction: sessions=%d events=%d", sessions, storedEvents)
	}
	if err := store.Create(ctx, meta); err != nil {
		t.Fatal(err)
	}
	bad := Event{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": make(chan int)}}
	if err := store.Append(ctx, meta.ID, []Event{bad}); err == nil || !strings.Contains(err.Error(), "JSON-serializable") {
		t.Fatalf("invalid append error = %v", err)
	}
	if listed, err := store.List(ctx); err != nil || len(listed) != 0 {
		t.Fatalf("invalid append materialized session: %#v, %v", listed, err)
	}
}

func TestSQLiteSessionCoordinatorBatchesAndDrains(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "coordinated.db")
	store, err := NewSQLiteSessionStoreWithOptions(SQLiteSessionStoreOptions{
		Path: path, JournalMode: SQLiteJournalDelete,
		PreparedSessionCacheSize: 2, WriteBatchMaxDelay: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	meta := SessionHeader{Version: SessionFormatVersion, ID: "coordinated", CreatedAt: 1}
	if err := store.Create(ctx, meta); err != nil {
		t.Fatal(err)
	}
	first := []Event{
		{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": 1}},
		{Type: "turn/end", Seq: 1, Time: 2, Data: map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}},
	}
	for _, event := range first {
		if err := store.Append(ctx, meta.ID, []Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	var rows int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM sessions").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("rows before drain = %d, %v", rows, err)
	}
	inspection, err := store.Inspect(ctx, meta.ID)
	if err != nil || len(inspection.Events) != 2 {
		t.Fatalf("drained inspection = %#v, %v", inspection.Events, err)
	}
	var revision int
	if err := store.db.QueryRow("SELECT revision FROM sessions WHERE id = ?", meta.ID).Scan(&revision); err != nil || revision != 1 {
		t.Fatalf("batched revision = %d, %v", revision, err)
	}
	second := []Event{
		{Type: "turn/start", Seq: 2, Time: 3, Data: map[string]any{"turn": 2}},
		{Type: "turn/end", Seq: 3, Time: 4, Data: map[string]any{"turn": 2, "reason": map[string]any{"kind": "completed"}}},
	}
	if err := store.Append(ctx, meta.ID, second); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSQLiteSessionStore(path, SQLiteJournalDelete)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, err := reopened.Load(ctx, meta.ID)
	if err != nil || len(loaded.Events) != 4 {
		t.Fatalf("close-drained events = %#v, %v", loaded.Events, err)
	}
}

func TestSQLiteSessionCoordinatorAutomaticBatch(t *testing.T) {
	ctx := context.Background()
	store, err := NewSQLiteSessionStoreWithOptions(SQLiteSessionStoreOptions{
		Path: ":memory:", PreparedSessionCacheSize: 1, WriteBatchMaxDelay: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta := SessionHeader{Version: SessionFormatVersion, ID: "automatic-batch", CreatedAt: 1}
	if err := store.Create(ctx, meta); err != nil {
		t.Fatal(err)
	}
	for _, event := range []Event{
		{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": 1}},
		{Type: "turn/end", Seq: 1, Time: 2, Data: map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}},
	} {
		if err := store.Append(ctx, meta.ID, []Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		_, found, err := store.revision(ctx, meta.ID)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("automatic write batch did not flush")
		}
		time.Sleep(5 * time.Millisecond)
	}
	var revision int
	if err := store.db.QueryRow("SELECT revision FROM sessions WHERE id = ?", meta.ID).Scan(&revision); err != nil || revision != 1 {
		t.Fatalf("automatic batch revision = %d, %v", revision, err)
	}
}

func TestSQLiteSessionCoordinatorPreparedCache(t *testing.T) {
	ctx := context.Background()
	store, err := NewSQLiteSessionStoreWithOptions(SQLiteSessionStoreOptions{
		Path: ":memory:", PreparedSessionCacheSize: 1, WriteBatchMaxDelay: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	create := func(id string, turn int) SessionInspection {
		t.Helper()
		meta := SessionHeader{Version: SessionFormatVersion, ID: id, CreatedAt: int64(turn)}
		if err := store.Create(ctx, meta); err != nil {
			t.Fatal(err)
		}
		events := []Event{
			{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": turn}},
			{Type: "turn/end", Seq: 1, Time: 2, Data: map[string]any{"turn": turn, "reason": map[string]any{"kind": "completed"}}},
		}
		if err := store.Append(ctx, id, events); err != nil {
			t.Fatal(err)
		}
		inspection, err := store.Inspect(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return inspection
	}
	first := create("cache-first", 1)
	first.Events[0].Data.(map[string]any)["turn"] = float64(99)
	again, err := store.Inspect(ctx, "cache-first")
	if err != nil || again.Events[0].Data.(map[string]any)["turn"] != float64(1) {
		t.Fatalf("cached inspection = %#v, %v", again.Events, err)
	}
	create("cache-second", 2)
	store.coordinator.mu.Lock()
	_, firstCached := store.coordinator.cache["cache-first"]
	_, secondCached := store.coordinator.cache["cache-second"]
	cacheLen := len(store.coordinator.cache)
	store.coordinator.mu.Unlock()
	if cacheLen != 1 || firstCached || !secondCached {
		t.Fatalf("prepared cache len=%d first=%v second=%v", cacheLen, firstCached, secondCached)
	}
	if err := store.appendImmediate(ctx, "cache-second", []Event{
		{Type: "turn/start", Seq: 2, Time: 3, Data: map[string]any{"turn": 3}},
		{Type: "turn/end", Seq: 3, Time: 4, Data: map[string]any{"turn": 3, "reason": map[string]any{"kind": "completed"}}},
	}); err != nil {
		t.Fatal(err)
	}
	refreshed, err := store.Inspect(ctx, "cache-second")
	if err != nil || len(refreshed.Events) != 4 {
		t.Fatalf("revision-refreshed inspection = %#v, %v", refreshed.Events, err)
	}
}

func openSQLiteProbe(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}

func assertSessionEventsJSON(t *testing.T, got, want []Event) {
	t.Helper()
	gotJSON, gotErr := json.Marshal(got)
	wantJSON, wantErr := json.Marshal(want)
	if gotErr != nil || wantErr != nil || string(gotJSON) != string(wantJSON) {
		t.Fatalf("events = %s, want %s (errors: %v, %v)", gotJSON, wantJSON, gotErr, wantErr)
	}
}

func infoMode(info os.FileInfo) any {
	if info == nil {
		return nil
	}
	return info.Mode().Perm()
}
