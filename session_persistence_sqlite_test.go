package harness

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
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

	store, err = NewSQLiteSessionStore(path, SQLiteJournalWAL)
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
	if !strings.HasPrefix(string(after[0].Revision), strings.Split(string(before[0].Revision), ":incarnation:")[0]+":incarnation:") {
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
		if _, err := db.Exec(`INSERT INTO events (session_id, seq, type, time, data, ignorable) VALUES (?, 2, 'text-chunks', 3, '{not json', 0)`, meta.ID); err != nil {
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
		if _, err := store.Load(ctx, meta.ID); err == nil || !strings.Contains(err.Error(), "invalid committed physical row") {
			t.Fatalf("corruption error = %v", err)
		}
	})
}

func TestSQLiteSessionStoreSchema17PackingAndReadFrom(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "packed.db")
	store, err := NewSQLiteSessionStore(path, SQLiteJournalDelete)
	if err != nil {
		t.Fatal(err)
	}
	meta := SessionHeader{Version: SessionFormatVersion, ID: "packed", CreatedAt: 1}
	if err := store.Create(ctx, meta); err != nil {
		t.Fatal(err)
	}
	events := make([]Event, 8)
	for seq := range events {
		events[seq] = sqliteTestTextChunk(seq, int64(100+seq), "text-delta", 0, fmt.Sprintf("token-%d", seq))
	}
	if err := store.Append(ctx, meta.ID, events[:3]); err != nil {
		t.Fatal(err)
	}
	var originalRowID int64
	var originalData, storageType, physicalType string
	var marker int
	if err := store.db.QueryRow(`SELECT rowid, data, typeof(data), type, ignorable FROM events WHERE session_id = ? AND seq = 0`, meta.ID).
		Scan(&originalRowID, &originalData, &storageType, &physicalType, &marker); err != nil {
		t.Fatal(err)
	}
	if storageType != "text" || physicalType != "text-chunks" || marker != 0 {
		t.Fatalf("first physical row type=%q storage=%q marker=%d", physicalType, storageType, marker)
	}
	if err := store.Append(ctx, meta.ID, events[3:4]); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, meta.ID, events[4:]); err != nil {
		t.Fatal(err)
	}
	var rowID int64
	var data string
	if err := store.db.QueryRow(`SELECT rowid, data FROM events WHERE session_id = ? AND seq = 0`, meta.ID).Scan(&rowID, &data); err != nil {
		t.Fatal(err)
	}
	if rowID != originalRowID || data != originalData {
		t.Fatalf("earlier packed row was rewritten: rowid %d -> %d", originalRowID, rowID)
	}
	var rows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM events WHERE session_id = ?`, meta.ID).Scan(&rows); err != nil || rows != 3 {
		t.Fatalf("physical rows = %d, %v", rows, err)
	}
	suffix, err := store.ReadFrom(ctx, meta.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertSessionEventsJSON(t, suffix.Events, events[1:])
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewSQLiteSessionStore(path, SQLiteJournalDelete)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	suffix, err = reopened.ReadFrom(ctx, meta.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertSessionEventsJSON(t, suffix.Events, events[2:])
}

func TestSQLiteSessionStoreSchema17TagsCompressionAndProvenance(t *testing.T) {
	ctx := context.Background()
	store, err := NewSQLiteSessionStore(":memory:", SQLiteJournalWAL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta := SessionHeader{Version: SessionFormatVersion, ID: "codec", CreatedAt: 1}
	if err := store.Create(ctx, meta); err != nil {
		t.Fatal(err)
	}
	events := []Event{{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": 1}}}
	for seq := 1; seq <= 3; seq++ {
		events = append(events, sqliteTestTextChunk(seq, int64(seq+10), "text-delta", 0, fmt.Sprintf("text-%d", seq)))
	}
	for seq := 4; seq <= 6; seq++ {
		events = append(events, sqliteTestTextChunk(seq, int64(20-seq), "reasoning-delta", 1, fmt.Sprintf("reason-%d", seq)))
	}
	for seq := 7; seq <= 9; seq++ {
		events = append(events, sqliteTestToolChunk(seq, int64(seq+20), 2, "call-1", "write", fmt.Sprintf("{%d", seq)))
	}
	events = append(events,
		Event{Type: "assistant/message", Seq: 10, Time: 40, Data: map[string]any{"text": strings.Repeat("x", sqliteZstdThresholdBytes*2)}, SourceEventSeqs: []int{9, 1, 8}, SurfaceOp: "append"},
		Event{Type: "assistant/message", Seq: 11, Time: 41, Data: map[string]any{"text": "empty provenance"}, SourceEventSeqs: []int{}, SurfaceOp: "append"},
		Event{Type: "turn/end", Seq: 12, Time: 42, Data: map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}},
	)
	if err := store.Append(ctx, meta.ID, events); err != nil {
		t.Fatal(err)
	}
	rows, err := store.db.Query(`SELECT type FROM events WHERE session_id = ? AND ignorable = 0 ORDER BY seq`, meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			t.Fatal(err)
		}
		tags = append(tags, tag)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(tags, ",") != "text-chunks,reasoning-chunks,tool-call-chunks" {
		t.Fatalf("packed tags = %#v", tags)
	}
	var dataType, sourceType string
	if err := store.db.QueryRow(`SELECT typeof(data), typeof(source_event_seqs) FROM events WHERE session_id = ? AND seq = 10`, meta.ID).
		Scan(&dataType, &sourceType); err != nil {
		t.Fatal(err)
	}
	if dataType != "blob" || sourceType != "blob" {
		t.Fatalf("compressed/source storage types = %q/%q", dataType, sourceType)
	}
	var emptySourceType string
	var emptySourceBytes int
	if err := store.db.QueryRow(`SELECT typeof(source_event_seqs), length(source_event_seqs) FROM events WHERE session_id = ? AND seq = 11`, meta.ID).
		Scan(&emptySourceType, &emptySourceBytes); err != nil {
		t.Fatal(err)
	}
	if emptySourceType != "blob" || emptySourceBytes != 0 {
		t.Fatalf("empty provenance storage = %q/%d", emptySourceType, emptySourceBytes)
	}
	loaded, err := store.Load(ctx, meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertSessionEventsJSON(t, loaded.Events, events)
}

func TestSQLiteSchema17CodecBoundsAndMalformedValues(t *testing.T) {
	long := make([]Event, maxSQLitePackedRowMembers+3)
	for seq := range long {
		long[seq] = sqliteTestTextChunk(seq, int64(seq), "text-delta", 0, "x")
	}
	records, err := packSQLiteChunkRuns(long)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].packed == nil || records[1].packed == nil {
		t.Fatalf("long run records = %#v", records)
	}
	for index, want := range []int{maxSQLitePackedRowMembers, 3} {
		row := records[index].packed
		if len(row.data) > maxSQLitePackedDataBytes {
			t.Fatalf("packed data bytes = %d", len(row.data))
		}
		decoded, err := sqliteRowEvents(sqliteEventRow{
			seq: row.seq, typ: row.typ, time: row.time, data: string(row.data),
			ignorable: sql.NullInt64{Int64: 0, Valid: true},
		})
		if err != nil || len(decoded) != want {
			t.Fatalf("record %d members = %d, %v", index, len(decoded), err)
		}
	}

	large := make([]Event, 4)
	for seq := range large {
		large[seq] = sqliteTestTextChunk(seq, int64(seq), "text-delta", 0, strings.Repeat("x", 300_000))
	}
	records, err = packSQLiteChunkRuns(large)
	if err != nil || len(records) != 2 || records[0].packed == nil || len(records[0].packed.data) > maxSQLitePackedDataBytes {
		t.Fatalf("byte-bounded records = %d, %v", len(records), err)
	}
	html := []Event{
		sqliteTestTextChunk(0, 0, "text-delta", 0, "<"),
		sqliteTestTextChunk(1, 1, "text-delta", 0, ">"),
		sqliteTestTextChunk(2, 2, "text-delta", 0, "&"),
	}
	records, err = packSQLiteChunkRuns(html)
	if err != nil || len(records) != 1 || records[0].packed == nil || bytes.Contains(records[0].packed.data, []byte(`\u003c`)) {
		t.Fatalf("packed JSON does not match JSON.stringify escaping: %#v, %v", records, err)
	}

	compressible := bytes.Repeat([]byte("x"), sqliteZstdThresholdBytes)
	if _, ok := encodeSQLiteData(compressible).([]byte); !ok {
		t.Fatal("compressible threshold value was not stored as a blob")
	}
	incompressible := make([]byte, sqliteZstdThresholdBytes)
	state := uint32(1)
	for index := range incompressible {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		incompressible[index] = byte(state)
	}
	if _, ok := encodeSQLiteData(incompressible).(string); !ok {
		t.Fatal("unprofitable compression did not fall back to text")
	}

	sourceCases := [][]int{{}}
	if strconv.IntSize == 64 {
		maximum := int64(maxJSONSafeInteger)
		sourceCases = append(sourceCases, []int{int(maximum - 1), 0, int(maximum - 2)})
	}
	for _, sources := range sourceCases {
		encoded, err := encodeSQLiteSourceSeqs(sources)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeSQLiteSourceSeqs(encoded)
		if err != nil || !slices.Equal(decoded, sources) {
			t.Fatalf("source seqs = %#v, want %#v, %v", decoded, sources, err)
		}
	}
	for _, malformed := range [][]byte{{0x80}, {0x80, 0x00}, {0x00, 0x01}, bytes.Repeat([]byte{0x80}, 9)} {
		if _, err := decodeSQLiteSourceSeqs(malformed); err == nil {
			t.Fatalf("malformed source seqs %x accepted", malformed)
		}
	}
}

func TestSQLiteSessionStoreRejectsPreSchema17(t *testing.T) {
	for _, version := range []int{15, 16} {
		t.Run(fmt.Sprintf("schema-%d", version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "old.db")
			db := openSQLiteProbe(t, path)
			if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if runtime.GOOS != "windows" {
				if err := os.Chmod(path, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			store, err := NewSQLiteSessionStore(path, SQLiteJournalDelete)
			if store != nil {
				_ = store.Close()
			}
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("schema version %d", version)) {
				t.Fatalf("schema %d error = %v", version, err)
			}
		})
	}
}

func TestSQLiteSessionStoreRejectsChangedSchema17(t *testing.T) {
	ctx := context.Background()
	t.Run("open", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "changed.db")
		store, err := NewSQLiteSessionStore(path, SQLiteJournalDelete)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		db := openSQLiteProbe(t, path)
		if _, err := db.Exec("ALTER TABLE events ADD COLUMN extra TEXT"); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		store, err = NewSQLiteSessionStore(path, SQLiteJournalDelete)
		if store != nil {
			_ = store.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "required schema objects") {
			t.Fatalf("changed schema error = %v", err)
		}
	})

	t.Run("mutation", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "mutation.db")
		store, err := NewSQLiteSessionStore(path, SQLiteJournalDelete)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		meta := SessionHeader{Version: SessionFormatVersion, ID: "mutation", CreatedAt: 1}
		if err := store.Create(ctx, meta); err != nil {
			t.Fatal(err)
		}
		db := openSQLiteProbe(t, path)
		if _, err := db.Exec("PRAGMA user_version = 16"); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		err = store.Append(ctx, meta.ID, []Event{{Type: "turn/start", Seq: 0, Time: 1, Data: map[string]any{"turn": 1}}})
		if err == nil || !strings.Contains(err.Error(), "schema changed before mutation") {
			t.Fatalf("mutation schema error = %v", err)
		}
	})
}

func TestSQLiteSessionStoreRejectsStaleAppendAndRepair(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "stale.db")
	first, err := NewSQLiteSessionStore(path, SQLiteJournalWAL)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := NewSQLiteSessionStore(path, SQLiteJournalWAL)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	meta := SessionHeader{Version: SessionFormatVersion, ID: "stale", CreatedAt: 1}
	if err := first.Create(ctx, meta); err != nil {
		t.Fatal(err)
	}
	if err := first.Append(ctx, meta.ID, []Event{sqliteTestTextChunk(0, 1, "text-delta", 0, "a")}); err != nil {
		t.Fatal(err)
	}
	if err := second.Append(ctx, meta.ID, []Event{sqliteTestTextChunk(1, 2, "text-delta", 0, "b")}); err != nil {
		t.Fatal(err)
	}
	if err := first.Append(ctx, meta.ID, []Event{sqliteTestTextChunk(1, 2, "text-delta", 0, "loser")}); err == nil ||
		!strings.Contains(err.Error(), "stored next seq is 2") {
		t.Fatalf("stale append error = %v", err)
	}

	probe := openSQLiteProbe(t, path)
	if _, err := probe.Exec(`INSERT INTO events (session_id, seq, type, time, data, ignorable) VALUES (?, 2, 'text-chunks', 3, '{not json', 0)`, meta.ID); err != nil {
		t.Fatal(err)
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	prefix, found, err := first.readPrefix(ctx, meta.ID, 0)
	if err != nil || !found || prefix.tornFrom == nil || *prefix.tornFrom != 2 {
		t.Fatalf("stale prefix = %#v, %v, %v", prefix, found, err)
	}
	repairer, err := NewSQLiteSessionStore(path, SQLiteJournalWAL)
	if err != nil {
		t.Fatal(err)
	}
	defer repairer.Close()
	if _, err := repairer.Load(ctx, meta.ID); err != nil {
		t.Fatal(err)
	}
	if err := repairer.Append(ctx, meta.ID, []Event{sqliteTestTextChunk(2, 3, "text-delta", 0, "winner")}); err != nil {
		t.Fatal(err)
	}
	if err := first.repair(ctx, meta, prefix.tornFrom, nil); err == nil || !strings.Contains(err.Error(), "repair is stale") {
		t.Fatalf("stale repair error = %v", err)
	}
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
	var busyTimeout, trustedSchema, mmapSize, synchronous int
	for name, target := range map[string]*int{
		"busy_timeout":   &busyTimeout,
		"trusted_schema": &trustedSchema,
		"mmap_size":      &mmapSize,
		"synchronous":    &synchronous,
	} {
		if err := store.db.QueryRow("PRAGMA " + name).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if busyTimeout != int(DefaultSQLiteBusyTimeout/time.Millisecond) || trustedSchema != 0 || mmapSize != 0 || synchronous != 2 {
		t.Fatalf("connection pragmas = busy %d trusted %d mmap %d synchronous %d", busyTimeout, trustedSchema, mmapSize, synchronous)
	}
	store.db.SetMaxIdleConns(0)
	for name, want := range map[string]int{
		"busy_timeout":   int(DefaultSQLiteBusyTimeout / time.Millisecond),
		"trusted_schema": 0,
		"mmap_size":      0,
		"synchronous":    2,
	} {
		var got int
		if err := store.db.QueryRow("PRAGMA " + name).Scan(&got); err != nil || got != want {
			t.Fatalf("reopened connection %s = %d, %v", name, got, err)
		}
	}
	store.db.SetMaxIdleConns(1)
	var storeID, dataColumnType, sourcesColumnType, eventsSQL string
	if err := store.db.QueryRow("SELECT store_id FROM persistence_state WHERE singleton = 1").Scan(&storeID); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("SELECT type FROM pragma_table_info('events') WHERE name = 'data'").Scan(&dataColumnType); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("SELECT type FROM pragma_table_info('events') WHERE name = 'source_event_seqs'").Scan(&sourcesColumnType); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'events'").Scan(&eventsSQL); err != nil {
		t.Fatal(err)
	}
	if !isSQLiteUUID(storeID) || dataColumnType != "ANY" || sourcesColumnType != "ANY" ||
		!strings.Contains(eventsSQL, "ignorable IN (0, 1)") {
		t.Fatalf("schema17 layout = store %q data %q sources %q sql %q", storeID, dataColumnType, sourcesColumnType, eventsSQL)
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
	if err := os.WriteFile(existing, nil, 0o600); err != nil {
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
		if info, err := os.Stat(existing); err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("existing database mode = %v, %v", infoMode(info), err)
		}
	}

	unowned := filepath.Join(t.TempDir(), "unowned.db")
	db := openSQLiteProbe(t, unowned)
	if _, err := db.Exec("CREATE TABLE sqliteX (value TEXT)"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if runtime.GOOS != "windows" {
		if err := os.Chmod(unowned, 0o600); err != nil {
			t.Fatal(err)
		}
	}
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

func TestSQLiteSessionStoreConnectionOptions(t *testing.T) {
	for _, test := range []struct {
		name    string
		timeout time.Duration
		set     bool
		want    int
	}{
		{name: "default", want: int(DefaultSQLiteBusyTimeout / time.Millisecond)},
		{name: "explicit zero", set: true, want: 0},
		{name: "custom", timeout: 73 * time.Millisecond, set: true, want: 73},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewSQLiteSessionStoreWithOptions(SQLiteSessionStoreOptions{
				Path: ":memory:", BusyTimeout: test.timeout, BusyTimeoutSet: test.set,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			var busyTimeout, trustedSchema, synchronous int
			for name, target := range map[string]*int{
				"busy_timeout":   &busyTimeout,
				"trusted_schema": &trustedSchema,
				"synchronous":    &synchronous,
			} {
				if err := store.db.QueryRow("PRAGMA " + name).Scan(target); err != nil {
					t.Fatal(err)
				}
			}
			if busyTimeout != test.want || trustedSchema != 0 || synchronous != 2 {
				t.Fatalf("connection pragmas = busy %d trusted %d synchronous %d", busyTimeout, trustedSchema, synchronous)
			}
			if store.Options().BusyTimeout != time.Duration(test.want)*time.Millisecond || store.Options().BusyTimeoutSet != test.set {
				t.Fatalf("options = %#v", store.Options())
			}
		})
	}

	for _, timeout := range []time.Duration{-time.Millisecond, time.Microsecond, MaxSQLiteBusyTimeout + time.Millisecond} {
		store, err := NewSQLiteSessionStoreWithOptions(SQLiteSessionStoreOptions{
			Path: ":memory:", BusyTimeout: timeout, BusyTimeoutSet: true,
		})
		if store != nil {
			_ = store.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "busyTimeoutMs must be between") {
			t.Fatalf("timeout %s error = %v", timeout, err)
		}
	}
}

func TestSQLiteSessionStoreBusyTimeoutWaitsForWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy.db")
	seed, err := NewSQLiteSessionStore(path, SQLiteJournalWAL)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	holder := openSQLiteProbe(t, path)
	defer holder.Close()
	holder.SetMaxOpenConns(1)
	holder.SetMaxIdleConns(1)
	if _, err := holder.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	released := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, err := holder.Exec("COMMIT")
		released <- err
	}()
	started := time.Now()
	store, err := NewSQLiteSessionStoreWithOptions(SQLiteSessionStoreOptions{
		Path: path, JournalMode: SQLiteJournalWAL,
		BusyTimeout: time.Second, BusyTimeoutSet: true,
	})
	elapsed := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("store did not wait for competing writer: %s", elapsed)
	}
}

func TestSQLiteSessionStoreRejectsUnsafePaths(t *testing.T) {
	t.Run("database directory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "sessions.db")
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if store, err := NewSQLiteSessionStore(path, SQLiteJournalWAL); store != nil || err == nil || !strings.Contains(err.Error(), "regular file") {
			if store != nil {
				_ = store.Close()
			}
			t.Fatalf("directory error = %v", err)
		}
	})

	if runtime.GOOS != "windows" {
		t.Run("database symlink", func(t *testing.T) {
			root := t.TempDir()
			target := filepath.Join(root, "target.db")
			link := filepath.Join(root, "sessions.db")
			if err := os.WriteFile(target, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, link); err != nil {
				t.Fatal(err)
			}
			if store, err := NewSQLiteSessionStore(link, SQLiteJournalWAL); store != nil || err == nil || !strings.Contains(err.Error(), "symbolic link") {
				if store != nil {
					_ = store.Close()
				}
				t.Fatalf("symlink error = %v", err)
			}
		})

		t.Run("parent symlink", func(t *testing.T) {
			root := t.TempDir()
			realParent := filepath.Join(root, "real")
			linkedParent := filepath.Join(root, "linked")
			if err := os.Mkdir(realParent, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(realParent, linkedParent); err != nil {
				t.Fatal(err)
			}
			if store, err := NewSQLiteSessionStore(filepath.Join(linkedParent, "sessions.db"), SQLiteJournalWAL); store != nil || err == nil || !strings.Contains(err.Error(), "real directory") {
				if store != nil {
					_ = store.Close()
				}
				t.Fatalf("parent symlink error = %v", err)
			}
		})

		t.Run("permissive database", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sessions.db")
			if err := os.WriteFile(path, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			if store, err := NewSQLiteSessionStore(path, SQLiteJournalWAL); store != nil || err == nil || !strings.Contains(err.Error(), "accessible only by that user") {
				if store != nil {
					_ = store.Close()
				}
				t.Fatalf("permissive database error = %v", err)
			}
		})

		t.Run("writable parent", func(t *testing.T) {
			parent := t.TempDir()
			if err := os.Chmod(parent, 0o770); err != nil {
				t.Fatal(err)
			}
			if store, err := NewSQLiteSessionStore(filepath.Join(parent, "sessions.db"), SQLiteJournalWAL); store != nil || err == nil || !strings.Contains(err.Error(), "not group/world-writable") {
				if store != nil {
					_ = store.Close()
				}
				t.Fatalf("writable parent error = %v", err)
			}
		})
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

func sqliteTestTextChunk(seq int, timestamp int64, kind string, index int, text string) Event {
	return Event{Type: "assistant/chunk", Seq: seq, Time: timestamp, Data: map[string]any{
		"turn": 1, "step": 1,
		"chunk": map[string]any{"type": kind, "index": index, "text": text},
	}}
}

func sqliteTestToolChunk(seq int, timestamp int64, index int, id, name, arguments string) Event {
	return Event{Type: "assistant/chunk", Seq: seq, Time: timestamp, Data: map[string]any{
		"turn": 1, "step": 1,
		"chunk": map[string]any{
			"type": "tool-call-delta", "index": index, "id": id,
			"name": name, "argumentsDelta": arguments,
		},
	}}
}
