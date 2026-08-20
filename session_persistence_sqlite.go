package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	SessionSQLiteSchemaVersion            = 15
	SessionSQLiteApplicationID            = 0x44534850
	DefaultSQLitePreparedSessionCacheSize = 5
	DefaultSQLiteWriteBatchMaxDelay       = 200 * time.Millisecond
	MaxSQLiteWriteBatchDelay              = time.Duration(2_147_483_647) * time.Millisecond
)

type SQLiteSessionStoreOptions struct {
	Path                     string
	JournalMode              SQLiteJournalMode
	PreparedSessionCacheSize int
	WriteBatchMaxDelay       time.Duration
}

type SQLiteSessionStore struct {
	db       *sql.DB
	identity string

	mu          sync.Mutex
	states      map[string]sqliteSessionState
	ops         sync.WaitGroup
	opMu        sync.Mutex
	closing     bool
	closed      bool
	done        chan struct{}
	closeErr    error
	options     SQLiteSessionStoreOptions
	coordinator *sqliteSessionCoordinator
}

type sqliteSessionState struct {
	meta         SessionHeader
	cursor       int
	materialized bool
}

func NewSQLiteSessionStore(path string, journalMode SQLiteJournalMode) (*SQLiteSessionStore, error) {
	options, err := normalizeSQLiteSessionStoreOptions(SQLiteSessionStoreOptions{Path: path, JournalMode: journalMode})
	if err != nil {
		return nil, err
	}
	return newSQLiteSessionStore(options, false)
}

func NewSQLiteSessionStoreWithOptions(options SQLiteSessionStoreOptions) (*SQLiteSessionStore, error) {
	options, err := normalizeSQLiteSessionStoreOptions(options)
	if err != nil {
		return nil, err
	}
	return newSQLiteSessionStore(options, true)
}

func normalizeSQLiteSessionStoreOptions(options SQLiteSessionStoreOptions) (SQLiteSessionStoreOptions, error) {
	if strings.TrimSpace(options.Path) == "" {
		return SQLiteSessionStoreOptions{}, errors.New("SQLite session path is required")
	}
	if options.JournalMode == "" {
		options.JournalMode = SQLiteJournalWAL
	}
	switch options.JournalMode {
	case SQLiteJournalWAL, SQLiteJournalDelete, SQLiteJournalTruncate, SQLiteJournalPersist:
	default:
		return SQLiteSessionStoreOptions{}, fmt.Errorf("unsupported sqlite journal mode %q", options.JournalMode)
	}
	if options.PreparedSessionCacheSize == 0 {
		options.PreparedSessionCacheSize = DefaultSQLitePreparedSessionCacheSize
	}
	if options.PreparedSessionCacheSize < 1 {
		return SQLiteSessionStoreOptions{}, errors.New("preparedSessionCacheSize must be a positive integer")
	}
	if options.WriteBatchMaxDelay == 0 {
		options.WriteBatchMaxDelay = DefaultSQLiteWriteBatchMaxDelay
	}
	if options.WriteBatchMaxDelay < time.Millisecond || options.WriteBatchMaxDelay > MaxSQLiteWriteBatchDelay || options.WriteBatchMaxDelay%time.Millisecond != 0 {
		return SQLiteSessionStoreOptions{}, errors.New("writeBatchMaxDelay must be between 1ms and 2147483647ms")
	}
	return options, nil
}

func newSQLiteSessionStore(options SQLiteSessionStoreOptions, coordinated bool) (*SQLiteSessionStore, error) {
	db, identity, err := openSessionSQLite(options.Path, options.JournalMode)
	if err != nil {
		return nil, err
	}
	store := &SQLiteSessionStore{
		db: db, identity: identity, states: map[string]sqliteSessionState{}, done: make(chan struct{}), options: options,
	}
	if coordinated {
		store.coordinator = newSQLiteSessionCoordinator(store, options)
	}
	return store, nil
}

func (s *SQLiteSessionStore) Options() SQLiteSessionStoreOptions { return s.options }

func (s *SQLiteSessionStore) Locate(SessionHeader) (SessionLocation, bool) {
	return SessionLocation{}, false
}

func (s *SQLiteSessionStore) SupportsRawArtifacts() bool { return false }

func (s *SQLiteSessionStore) ReadRaw(ctx context.Context, _ string) (SessionRawArtifact, bool, error) {
	if err := ctx.Err(); err != nil {
		return SessionRawArtifact{}, false, err
	}
	return SessionRawArtifact{}, false, errors.New("SQLite session persistence does not expose raw per-session artifacts")
}

func (s *SQLiteSessionStore) Create(ctx context.Context, meta SessionHeader) error {
	if s.coordinator != nil {
		s.coordinator.invalidate(meta.ID)
	}
	if err := validateSessionHeader(meta); err != nil {
		return err
	}
	end, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer end()
	s.mu.Lock()
	_, live := s.states[meta.ID]
	s.mu.Unlock()
	if live {
		return fmt.Errorf("session %q is already registered in persistence", meta.ID)
	}
	var exists int
	err = s.db.QueryRowContext(ctx, "SELECT 1 FROM sessions WHERE id = ?", meta.ID).Scan(&exists)
	if err == nil {
		return fmt.Errorf("session %q already exists in persistence", meta.ID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	s.mu.Lock()
	s.states[meta.ID] = sqliteSessionState{meta: meta}
	s.mu.Unlock()
	return nil
}

func (s *SQLiteSessionStore) Delete(ctx context.Context, id string) error {
	if s.coordinator != nil {
		s.coordinator.drop(id)
	}
	end, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer end()
	if _, err := s.db.ExecContext(ctx, "DELETE FROM sessions WHERE id = ?", id); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.states, id)
	s.mu.Unlock()
	return nil
}

func (s *SQLiteSessionStore) Append(ctx context.Context, id string, events []Event) error {
	if s.coordinator != nil {
		return s.coordinator.append(ctx, id, events)
	}
	return s.appendImmediate(ctx, id, events)
}

func (s *SQLiteSessionStore) appendImmediate(ctx context.Context, id string, events []Event) error {
	end, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer end()
	s.mu.Lock()
	state, live := s.states[id]
	s.mu.Unlock()
	if live {
		if len(events) == 0 {
			return nil
		}
		if err := validateSessionEventBatch(events, state.cursor); err != nil {
			return err
		}
		if err := s.appendTransaction(ctx, state.meta, events, !state.materialized); err != nil {
			return err
		}
		state.materialized = true
		state.cursor += len(events)
		s.mu.Lock()
		s.states[id] = state
		s.mu.Unlock()
		return nil
	}
	prefix, found, err := s.readPrefix(ctx, id, 0)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("session %q is not registered in persistence", id)
	}
	if len(events) == 0 {
		return nil
	}
	closers := interruptedSessionClosers(prefix.events)
	if err := s.repair(ctx, prefix.meta, prefix.tornFrom, closers); err != nil {
		return err
	}
	expected := len(prefix.events) + len(closers)
	if err := validateSessionEventBatch(events, expected); err != nil {
		return err
	}
	if err := s.appendTransaction(ctx, prefix.meta, events, false); err != nil {
		return err
	}
	s.mu.Lock()
	s.states[id] = sqliteSessionState{meta: prefix.meta, cursor: expected + len(events), materialized: true}
	s.mu.Unlock()
	return nil
}

func (s *SQLiteSessionStore) Load(ctx context.Context, id string) (SessionInspection, error) {
	if s.coordinator != nil {
		return s.coordinator.load(ctx, id)
	}
	return s.loadImmediate(ctx, id)
}

func (s *SQLiteSessionStore) loadImmediate(ctx context.Context, id string) (SessionInspection, error) {
	end, err := s.begin(ctx)
	if err != nil {
		return SessionInspection{}, err
	}
	defer end()
	prefix, found, err := s.readPrefix(ctx, id, 0)
	if err != nil {
		return SessionInspection{}, err
	}
	if !found {
		return SessionInspection{}, fmt.Errorf("session-not-found: %s", id)
	}
	s.mu.Lock()
	state, live := s.states[id]
	s.mu.Unlock()
	closers := interruptedSessionClosers(prefix.events)
	if live && state.materialized {
		if prefix.tornFrom != nil || len(closers) > 0 {
			return SessionInspection{}, fmt.Errorf("cannot crash-repair live session %q", id)
		}
		return SessionInspection{Meta: prefix.meta, Events: append([]Event(nil), prefix.events...)}, nil
	}
	if err := s.repair(ctx, prefix.meta, prefix.tornFrom, closers); err != nil {
		return SessionInspection{}, err
	}
	events := append(append([]Event(nil), prefix.events...), closers...)
	return SessionInspection{Meta: prefix.meta, Events: events}, nil
}

func (s *SQLiteSessionStore) Inspect(ctx context.Context, id string) (SessionInspection, error) {
	if s.coordinator != nil {
		return s.coordinator.inspect(ctx, id)
	}
	return s.inspectImmediate(ctx, id)
}

func (s *SQLiteSessionStore) inspectImmediate(ctx context.Context, id string) (SessionInspection, error) {
	end, err := s.begin(ctx)
	if err != nil {
		return SessionInspection{}, err
	}
	defer end()
	prefix, found, err := s.readPrefix(ctx, id, 0)
	if err != nil {
		return SessionInspection{}, err
	}
	if !found {
		return SessionInspection{}, fmt.Errorf("session-not-found: %s", id)
	}
	s.mu.Lock()
	state, live := s.states[id]
	s.mu.Unlock()
	events := append([]Event(nil), prefix.events...)
	if !live || !state.materialized {
		events = append(events, interruptedSessionClosers(prefix.events)...)
	}
	return SessionInspection{Meta: prefix.meta, Events: events}, nil
}

func (s *SQLiteSessionStore) ReadFrom(ctx context.Context, id string, fromSeq int) (SessionInspection, error) {
	if s.coordinator != nil {
		if err := s.coordinator.flush(ctx, id); err != nil {
			return SessionInspection{}, err
		}
	}
	return s.readFromImmediate(ctx, id, fromSeq)
}

func (s *SQLiteSessionStore) readFromImmediate(ctx context.Context, id string, fromSeq int) (SessionInspection, error) {
	if fromSeq < 0 {
		return SessionInspection{}, errors.New("fromSeq must be non-negative")
	}
	end, err := s.begin(ctx)
	if err != nil {
		return SessionInspection{}, err
	}
	defer end()
	prefix, found, err := s.readPrefix(ctx, id, fromSeq)
	if err != nil {
		return SessionInspection{}, err
	}
	if !found {
		return SessionInspection{}, fmt.Errorf("session-not-found: %s", id)
	}
	return SessionInspection{Meta: prefix.meta, Events: prefix.events}, nil
}

func (s *SQLiteSessionStore) List(ctx context.Context) ([]SessionHeader, error) {
	if s.coordinator != nil {
		if err := s.coordinator.flushAll(ctx); err != nil {
			return nil, err
		}
	}
	end, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	rows, err := s.db.QueryContext(ctx, sessionSQLiteSelectHeaders+" ORDER BY created_at, id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var headers []SessionHeader
	for rows.Next() {
		row, err := scanSQLiteSessionHeader(rows)
		if err != nil {
			return nil, err
		}
		headers = append(headers, row.meta)
	}
	return headers, rows.Err()
}

func (s *SQLiteSessionStore) ListSnapshots(ctx context.Context) ([]SessionPersistenceSnapshot, error) {
	if s.coordinator != nil {
		if err := s.coordinator.flushAll(ctx); err != nil {
			return nil, err
		}
	}
	return s.listSnapshotsImmediate(ctx)
}

func (s *SQLiteSessionStore) listSnapshotsImmediate(ctx context.Context) ([]SessionPersistenceSnapshot, error) {
	end, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	rows, err := s.db.QueryContext(ctx, sessionSQLiteSelectHeaders+" ORDER BY created_at, id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var snapshots []SessionPersistenceSnapshot
	for rows.Next() {
		row, err := scanSQLiteSessionHeader(rows)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, SessionPersistenceSnapshot{
			Header: row.meta,
			Revision: SessionPersistenceRevision(fmt.Sprintf(
				"%s:incarnation:%s:revision:%d", s.identity, row.incarnation, row.revision,
			)),
		})
	}
	return snapshots, rows.Err()
}

func (s *SQLiteSessionStore) Close() error {
	var coordinatorErr error
	if s.coordinator != nil {
		coordinatorErr = s.coordinator.close()
	}
	s.mu.Lock()
	if s.closing || s.closed {
		done := s.done
		s.mu.Unlock()
		<-done
		return s.closeErr
	}
	s.closing = true
	s.mu.Unlock()
	s.ops.Wait()
	err := errors.Join(coordinatorErr, s.db.Close())
	s.mu.Lock()
	s.closeErr = err
	s.closed = true
	close(s.done)
	s.mu.Unlock()
	return err
}

func (s *SQLiteSessionStore) begin(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closing || s.closed {
		s.mu.Unlock()
		return nil, storageError(StorageClosed, "SQLite session store is closed")
	}
	s.ops.Add(1)
	s.mu.Unlock()
	s.opMu.Lock()
	if err := ctx.Err(); err != nil {
		s.opMu.Unlock()
		s.ops.Done()
		return nil, err
	}
	return func() {
		s.opMu.Unlock()
		s.ops.Done()
	}, nil
}

type sqliteStoredPrefix struct {
	meta     SessionHeader
	events   []Event
	tornFrom *int
}

func (s *SQLiteSessionStore) readPrefix(ctx context.Context, id string, fromSeq int) (sqliteStoredPrefix, bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return sqliteStoredPrefix{}, false, err
	}
	defer tx.Rollback()
	row, err := scanSQLiteSessionHeader(tx.QueryRowContext(ctx, sessionSQLiteSelectHeaders+" WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteStoredPrefix{}, false, nil
	}
	if err != nil {
		return sqliteStoredPrefix{}, false, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT seq, type, time, data, source_event_seqs, surface_op, ignorable
		FROM events WHERE session_id = ? AND seq >= ? ORDER BY seq`, id, fromSeq)
	if err != nil {
		return sqliteStoredPrefix{}, false, err
	}
	defer rows.Close()
	var stored []sqliteEventRow
	for rows.Next() {
		var event sqliteEventRow
		if err := rows.Scan(&event.seq, &event.typ, &event.time, &event.data, &event.sources, &event.surface, &event.ignorable); err != nil {
			return sqliteStoredPrefix{}, false, err
		}
		stored = append(stored, event)
	}
	if err := rows.Err(); err != nil {
		return sqliteStoredPrefix{}, false, err
	}
	if err := rows.Close(); err != nil {
		return sqliteStoredPrefix{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return sqliteStoredPrefix{}, false, err
	}
	events, tornFrom, err := scanSQLiteSessionEvents(stored, fromSeq)
	if err != nil {
		return sqliteStoredPrefix{}, false, err
	}
	return sqliteStoredPrefix{meta: row.meta, events: events, tornFrom: tornFrom}, true, nil
}

func (s *SQLiteSessionStore) appendTransaction(ctx context.Context, meta SessionHeader, events []Event, materialize bool) error {
	bindings, err := sqliteEventBindingsFor(events)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if materialize {
		if err := insertSQLiteSessionHeader(ctx, tx, meta); err != nil {
			return err
		}
	}
	for _, event := range bindings {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO events (session_id, seq, type, time, data, source_event_seqs, surface_op, ignorable)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, meta.ID, event.seq, event.typ, event.time, event.data, event.sources, event.surface, event.ignorable); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE sessions SET revision = revision + 1 WHERE id = ?", meta.ID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteSessionStore) repair(ctx context.Context, meta SessionHeader, tornFrom *int, closers []Event) error {
	if tornFrom == nil && len(closers) == 0 {
		return nil
	}
	bindings, err := sqliteEventBindingsFor(closers)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if tornFrom != nil {
		if _, err := tx.ExecContext(ctx, "DELETE FROM events WHERE session_id = ? AND seq >= ?", meta.ID, *tornFrom); err != nil {
			return err
		}
	}
	for _, event := range bindings {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO events (session_id, seq, type, time, data, source_event_seqs, surface_op, ignorable)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, meta.ID, event.seq, event.typ, event.time, event.data, event.sources, event.surface, event.ignorable); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE sessions SET revision = revision + 1 WHERE id = ?", meta.ID); err != nil {
		return err
	}
	return tx.Commit()
}

type sqliteEventRow struct {
	seq       int
	typ       string
	time      int64
	data      string
	sources   sql.NullString
	surface   sql.NullString
	ignorable sql.NullInt64
}

type sqliteEventBinding struct {
	seq       int
	typ       string
	time      int64
	data      string
	sources   any
	surface   any
	ignorable any
}

func sqliteEventBindingsFor(events []Event) ([]sqliteEventBinding, error) {
	bindings := make([]sqliteEventBinding, 0, len(events))
	for _, event := range events {
		data, err := json.Marshal(event.Data)
		if err != nil {
			return nil, fmt.Errorf("session event %q is not JSON-serializable: %w", event.Type, err)
		}
		var sources, surface any
		if event.SourceEventSeqs != nil {
			encoded, err := json.Marshal(event.SourceEventSeqs)
			if err != nil {
				return nil, err
			}
			sources = string(encoded)
		}
		if event.SurfaceOp != nil {
			encoded, err := json.Marshal(event.SurfaceOp)
			if err != nil {
				return nil, err
			}
			surface = string(encoded)
		}
		var ignorable any
		if event.Ignorable {
			ignorable = 1
		}
		bindings = append(bindings, sqliteEventBinding{
			seq: event.Seq, typ: event.Type, time: event.Time, data: string(data),
			sources: sources, surface: surface, ignorable: ignorable,
		})
	}
	return bindings, nil
}

func scanSQLiteSessionEvents(rows []sqliteEventRow, base int) ([]Event, *int, error) {
	parsed := make([]*Event, len(rows))
	lastTurnEnd := -1
	for index, row := range rows {
		event, err := sqliteRowEvent(row)
		if err == nil {
			parsed[index] = &event
			if event.Type == "turn/end" {
				lastTurnEnd = index
			}
		}
	}
	events := make([]Event, 0, len(rows))
	for index, event := range parsed {
		if event == nil {
			if index <= lastTurnEnd {
				return nil, nil, fmt.Errorf("corrupt session log: unparsable committed event at seq %d", rows[index].seq)
			}
			break
		}
		if event.Seq != base+index {
			if index <= lastTurnEnd {
				return nil, nil, fmt.Errorf("corrupt session log: seq gap in committed region (expected %d, got %d)", base+index, event.Seq)
			}
			break
		}
		events = append(events, *event)
	}
	if base == 0 {
		if _, err := foldSurfaceEvents(events, true); err != nil {
			return nil, nil, err
		}
	}
	if len(events) < len(rows) {
		torn := base + len(events)
		return events, &torn, nil
	}
	return events, nil, nil
}

func sqliteRowEvent(row sqliteEventRow) (Event, error) {
	var data any
	if err := json.Unmarshal([]byte(row.data), &data); err != nil {
		return Event{}, err
	}
	event := Event{Type: row.typ, Seq: row.seq, Time: row.time, Data: data}
	if row.sources.Valid {
		if err := json.Unmarshal([]byte(row.sources.String), &event.SourceEventSeqs); err != nil {
			return Event{}, err
		}
	}
	if row.surface.Valid {
		if err := json.Unmarshal([]byte(row.surface.String), &event.SurfaceOp); err != nil {
			return Event{}, err
		}
	}
	event.Ignorable = row.ignorable.Valid && row.ignorable.Int64 == 1
	encoded, err := json.Marshal(event)
	if err != nil {
		return Event{}, err
	}
	decoded, err := decodeSessionStorageRecord(encoded)
	if err != nil {
		return Event{}, err
	}
	if len(decoded) != 1 {
		return Event{}, fmt.Errorf("decoded into %d events", len(decoded))
	}
	return decoded[0], nil
}

const sessionSQLiteSelectHeaders = `SELECT id, version, created_at, cwd, parent_session, seed_length, origin, delegation_depth, agent_preset, incarnation, revision FROM sessions`

type sqliteSessionHeaderRow struct {
	meta        SessionHeader
	incarnation string
	revision    int64
}

type sqliteScanner interface{ Scan(...any) error }

func scanSQLiteSessionHeader(scanner sqliteScanner) (sqliteSessionHeaderRow, error) {
	var id, incarnation string
	var version int
	var createdAt, revision int64
	var cwd, parent, origin, preset sql.NullString
	var seedLength, delegationDepth sql.NullInt64
	if err := scanner.Scan(&id, &version, &createdAt, &cwd, &parent, &seedLength, &origin, &delegationDepth, &preset, &incarnation, &revision); err != nil {
		return sqliteSessionHeaderRow{}, err
	}
	meta := SessionHeader{Version: version, ID: id, CreatedAt: createdAt}
	if cwd.Valid {
		meta.CWD = cwd.String
	}
	if parent.Valid {
		meta.ParentSession = parent.String
	}
	if seedLength.Valid {
		meta.SeedLength = int(seedLength.Int64)
	}
	if origin.Valid {
		meta.Origin = origin.String
	}
	if delegationDepth.Valid {
		meta.DelegationDepth = int(delegationDepth.Int64)
	}
	if preset.Valid {
		meta.AgentPreset = preset.String
	}
	if err := validateSessionHeader(meta); err != nil {
		return sqliteSessionHeaderRow{}, err
	}
	return sqliteSessionHeaderRow{meta: meta, incarnation: incarnation, revision: revision}, nil
}

func insertSQLiteSessionHeader(ctx context.Context, tx *sql.Tx, meta SessionHeader) error {
	var cwd, parent, seed, origin, depth, preset any
	if meta.CWD != "" {
		cwd = meta.CWD
	}
	if meta.ParentSession != "" {
		parent = meta.ParentSession
	}
	if meta.SeedLength != 0 {
		seed = meta.SeedLength
	}
	if meta.Origin != "" {
		origin = meta.Origin
	}
	if meta.DelegationDepth != 0 {
		depth = meta.DelegationDepth
	}
	if meta.AgentPreset != "" {
		preset = meta.AgentPreset
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO sessions
		(id, version, created_at, cwd, parent_session, seed_length, origin, delegation_depth, agent_preset, incarnation, revision)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		meta.ID, meta.Version, meta.CreatedAt, cwd, parent, seed, origin, depth, preset, newID("incarnation"))
	return err
}

func openSessionSQLite(path string, journalMode SQLiteJournalMode) (*sql.DB, string, error) {
	actual := path
	if path != ":memory:" {
		var err error
		actual, err = filepath.Abs(path)
		if err != nil {
			return nil, "", err
		}
		if err := os.MkdirAll(filepath.Dir(actual), 0o700); err != nil {
			return nil, "", err
		}
		file, err := os.OpenFile(actual, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			if err := file.Close(); err != nil {
				return nil, "", err
			}
		} else if !errors.Is(err, os.ErrExist) {
			return nil, "", err
		}
	}
	db, err := sql.Open("sqlite", actual)
	if err != nil {
		return nil, "", err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	fail := func(err error) (*sql.DB, string, error) {
		_ = db.Close()
		return nil, "", err
	}
	if err := db.Ping(); err != nil {
		return fail(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		return fail(err)
	}
	closeConn := func(err error) (*sql.DB, string, error) {
		_ = conn.Close()
		return fail(err)
	}
	if _, err := conn.ExecContext(context.Background(), "PRAGMA foreign_keys = ON"); err != nil {
		return closeConn(err)
	}
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		return closeConn(err)
	}
	rollback := func(err error) (*sql.DB, string, error) {
		if _, rollbackErr := conn.ExecContext(context.Background(), "ROLLBACK"); rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}
		return closeConn(err)
	}
	var onDisk, applicationID, objects int
	if err := conn.QueryRowContext(context.Background(), "PRAGMA user_version").Scan(&onDisk); err != nil {
		return rollback(err)
	}
	if err := conn.QueryRowContext(context.Background(), "PRAGMA application_id").Scan(&applicationID); err != nil {
		return rollback(err)
	}
	if err := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").Scan(&objects); err != nil {
		return rollback(err)
	}
	if onDisk == 0 && (applicationID != 0 || objects > 0) {
		return rollback(fmt.Errorf("session database at %q has an unversioned schema or application identity", actual))
	}
	if onDisk != 0 && onDisk != SessionSQLiteSchemaVersion {
		return rollback(fmt.Errorf("session database at %q has schema version %d, incompatible with this build (%d)", actual, onDisk, SessionSQLiteSchemaVersion))
	}
	if onDisk == SessionSQLiteSchemaVersion && applicationID != SessionSQLiteApplicationID {
		return rollback(fmt.Errorf("session database at %q has application id %d, expected %d", actual, applicationID, SessionSQLiteApplicationID))
	}
	if _, err := conn.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS persistence_state (
			singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
			store_id TEXT NOT NULL
		) STRICT;
		CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY,
			version INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			cwd TEXT,
			parent_session TEXT,
			seed_length INTEGER,
			origin TEXT,
			delegation_depth INTEGER,
			agent_preset TEXT,
			incarnation TEXT NOT NULL,
			revision INTEGER NOT NULL
		) STRICT;
		CREATE TABLE IF NOT EXISTS events (
			session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
			seq INTEGER NOT NULL,
			type TEXT NOT NULL,
			time INTEGER NOT NULL,
			data TEXT NOT NULL,
			source_event_seqs TEXT,
			surface_op TEXT,
			ignorable INTEGER,
			PRIMARY KEY (session_id, seq)
		) STRICT`); err != nil {
		return rollback(err)
	}
	if _, err := conn.ExecContext(context.Background(), "INSERT OR IGNORE INTO persistence_state (singleton, store_id) VALUES (1, ?)", newID("store")); err != nil {
		return rollback(err)
	}
	if onDisk == 0 {
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA application_id = %d", SessionSQLiteApplicationID)); err != nil {
			return rollback(err)
		}
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA user_version = %d", SessionSQLiteSchemaVersion)); err != nil {
			return rollback(err)
		}
	}
	if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil {
		return rollback(err)
	}
	if _, err := conn.ExecContext(context.Background(), "PRAGMA journal_mode = "+strings.ToUpper(string(journalMode))); err != nil {
		return closeConn(err)
	}
	var storeID string
	if err := conn.QueryRowContext(context.Background(), "SELECT store_id FROM persistence_state WHERE singleton = 1").Scan(&storeID); err != nil {
		return closeConn(err)
	}
	if storeID == "" {
		return closeConn(errors.New("session database has no valid store identity"))
	}
	if err := conn.Close(); err != nil {
		return fail(err)
	}
	if actual == ":memory:" {
		return db, fmt.Sprintf("memory:store:%s", storeID), nil
	}
	if canonical, err := filepath.EvalSymlinks(actual); err == nil {
		actual = canonical
	}
	return db, fmt.Sprintf("file:%s:store:%s", actual, storeID), nil
}
