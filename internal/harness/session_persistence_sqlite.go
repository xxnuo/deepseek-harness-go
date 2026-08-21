package harness

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	modernsqlite "modernc.org/sqlite"
)

const (
	SessionSQLiteSchemaVersion            = 17
	SessionSQLiteApplicationID            = 0x44534850
	DefaultSQLiteBusyTimeout              = 5 * time.Second
	MaxSQLiteBusyTimeout                  = time.Duration(2_147_483_647) * time.Millisecond
	DefaultSQLitePreparedSessionCacheSize = 5
	DefaultSQLiteWriteBatchMaxDelay       = 200 * time.Millisecond
	MaxSQLiteWriteBatchDelay              = time.Duration(2_147_483_647) * time.Millisecond
)

type SQLiteSessionStoreOptions struct {
	Path                     string
	JournalMode              SQLiteJournalMode
	BusyTimeout              time.Duration
	BusyTimeoutSet           bool
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
	if !options.BusyTimeoutSet && options.BusyTimeout == 0 {
		options.BusyTimeout = DefaultSQLiteBusyTimeout
	}
	if options.BusyTimeout < 0 || options.BusyTimeout > MaxSQLiteBusyTimeout || options.BusyTimeout%time.Millisecond != 0 {
		return SQLiteSessionStoreOptions{}, errors.New("busyTimeoutMs must be between 0 and 2147483647")
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
	db, identity, err := openSessionSQLite(options.Path, options.JournalMode, options.BusyTimeout)
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
	base, err := sqlitePackedReadBase(ctx, tx, id, fromSeq)
	if err != nil {
		return sqliteStoredPrefix{}, false, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT seq, type, time, data, source_event_seqs, surface_op, ignorable
		FROM events WHERE session_id = ? AND seq >= ? ORDER BY seq`, id, base)
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
	events, tornFrom, err := scanSQLiteSessionEvents(stored, base)
	if err != nil {
		return sqliteStoredPrefix{}, false, err
	}
	if base < fromSeq {
		first := len(events)
		for index, event := range events {
			if event.Seq >= fromSeq {
				first = index
				break
			}
		}
		events = events[first:]
	}
	return sqliteStoredPrefix{meta: row.meta, events: events, tornFrom: tornFrom}, true, nil
}

func sqlitePackedReadBase(ctx context.Context, tx *sql.Tx, id string, fromSeq int) (int, error) {
	if fromSeq == 0 {
		return 0, nil
	}
	floor := fromSeq - maxSQLitePackedRowMembers + 1
	if floor < 0 {
		floor = 0
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT seq, type, time, data, source_event_seqs, surface_op, ignorable
		FROM events
		WHERE session_id = ? AND seq >= ? AND seq < ?
		  AND type IN ('text-chunks', 'reasoning-chunks', 'tool-call-chunks')
		  AND ignorable = 0
		ORDER BY seq`, id, floor, fromSeq)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	base := fromSeq
	for rows.Next() {
		var row sqliteEventRow
		if err := rows.Scan(&row.seq, &row.typ, &row.time, &row.data, &row.sources, &row.surface, &row.ignorable); err != nil {
			return 0, err
		}
		events, err := sqliteRowEvents(row)
		if err != nil || len(events) > 0 && events[len(events)-1].Seq >= fromSeq {
			if row.seq < base {
				base = row.seq
			}
		}
	}
	return base, rows.Err()
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
	if err := validateSQLiteSchemaForMutation(ctx, tx); err != nil {
		return err
	}
	var storedNext int
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE((SELECT seq FROM events WHERE session_id = ? ORDER BY seq DESC LIMIT 1), -1)`, meta.ID).
		Scan(&storedNext); err != nil {
		return err
	}
	if storedNext >= 0 {
		lastRows, err := querySQLiteEventRows(ctx, tx, `
			SELECT seq, type, time, data, source_event_seqs, surface_op, ignorable
			FROM events WHERE session_id = ? ORDER BY seq DESC LIMIT 1`, meta.ID)
		if err != nil {
			return err
		}
		lastEvents, err := sqliteRowEvents(lastRows[0])
		if err != nil {
			return fmt.Errorf("session %s has an invalid physical tail at seq %d", meta.ID, lastRows[0].seq)
		}
		storedNext = lastEvents[len(lastEvents)-1].Seq + 1
	} else {
		storedNext = 0
	}
	if bindings[0].seq != storedNext {
		return fmt.Errorf("session %s append starts at seq %d, stored next seq is %d", meta.ID, bindings[0].seq, storedNext)
	}
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
	if err := validateSQLiteSchemaForMutation(ctx, tx); err != nil {
		return err
	}
	currentRows, err := querySQLiteEventRows(ctx, tx, `
		SELECT seq, type, time, data, source_event_seqs, surface_op, ignorable
		FROM events WHERE session_id = ? ORDER BY seq`, meta.ID)
	if err != nil {
		return err
	}
	current, currentTorn, err := scanSQLiteSessionEvents(currentRows, 0)
	if err != nil {
		return err
	}
	if tornFrom != nil {
		if currentTorn == nil || *currentTorn != *tornFrom {
			return fmt.Errorf("session %s repair is stale: physical tail no longer starts at seq %d", meta.ID, *tornFrom)
		}
	} else if currentTorn != nil {
		return fmt.Errorf("session %s repair omitted current torn tail at seq %d", meta.ID, *currentTorn)
	}
	if len(bindings) > 0 {
		expected := 0
		if len(current) > 0 {
			expected = current[len(current)-1].Seq + 1
		}
		if bindings[0].seq != expected {
			return fmt.Errorf("session %s repair is stale: closer starts at seq %d, stored next seq is %d", meta.ID, bindings[0].seq, expected)
		}
	}
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

func querySQLiteEventRows(ctx context.Context, queryer sqliteQueryer, query string, args ...any) ([]sqliteEventRow, error) {
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []sqliteEventRow
	for rows.Next() {
		var event sqliteEventRow
		if err := rows.Scan(&event.seq, &event.typ, &event.time, &event.data, &event.sources, &event.surface, &event.ignorable); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

const sessionSQLiteSelectHeaders = `SELECT id, version, created_at, cwd, parent_session, seed_length, origin, delegation_depth, agent_preset, incarnation, revision FROM sessions`

const (
	sessionSQLitePersistenceTable = `CREATE TABLE IF NOT EXISTS persistence_state (
		singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
		store_id TEXT NOT NULL
	) STRICT`
	sessionSQLiteSessionsTable = `CREATE TABLE IF NOT EXISTS sessions (
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
	) STRICT`
	sessionSQLiteEventsTable = `CREATE TABLE IF NOT EXISTS events (
		session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
		seq INTEGER NOT NULL,
		type TEXT NOT NULL,
		time INTEGER NOT NULL,
		data ANY NOT NULL,
		source_event_seqs ANY,
		surface_op TEXT,
		ignorable INTEGER CHECK (ignorable IS NULL OR ignorable IN (0, 1)),
		PRIMARY KEY (session_id, seq)
	) STRICT`
)

var sessionSQLiteExpectedSchema = map[string]string{
	"events":            normalizeSQLiteSQL(strings.Replace(sessionSQLiteEventsTable, " IF NOT EXISTS", "", 1)),
	"persistence_state": normalizeSQLiteSQL(strings.Replace(sessionSQLitePersistenceTable, " IF NOT EXISTS", "", 1)),
	"sessions":          normalizeSQLiteSQL(strings.Replace(sessionSQLiteSessionsTable, " IF NOT EXISTS", "", 1)),
}

type sqliteQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func validateSQLiteRequiredSchema(ctx context.Context, queryer sqliteQueryer) error {
	rows, err := queryer.QueryContext(ctx, `
		SELECT type, name, tbl_name, sql
		FROM sqlite_schema
		WHERE name NOT GLOB 'sqlite_*'
		ORDER BY type, name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var typ, name, table, statement string
		if err := rows.Scan(&typ, &name, &table, &statement); err != nil {
			return err
		}
		expected, ok := sessionSQLiteExpectedSchema[name]
		if !ok || typ != "table" || table != name || normalizeSQLiteSQL(statement) != expected {
			return errors.New("session database does not contain the required schema objects")
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if seen != len(sessionSQLiteExpectedSchema) {
		return errors.New("session database does not contain the required schema objects")
	}
	return nil
}

func normalizeSQLiteSQL(value string) string { return strings.Join(strings.Fields(value), " ") }

func validateSQLiteSchemaForMutation(ctx context.Context, tx *sql.Tx) error {
	var version, applicationID int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA application_id").Scan(&applicationID); err != nil {
		return err
	}
	if applicationID != SessionSQLiteApplicationID {
		return fmt.Errorf("session database application id changed before mutation (expected %d, got %d)", SessionSQLiteApplicationID, applicationID)
	}
	if err := validateSQLiteRequiredSchema(ctx, tx); err != nil {
		return err
	}
	if version != SessionSQLiteSchemaVersion {
		return fmt.Errorf("session database schema changed before mutation (expected %d, got %d)", SessionSQLiteSchemaVersion, version)
	}
	return nil
}

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
	if !isSQLiteUUID(incarnation) {
		return sqliteSessionHeaderRow{}, errors.New("stored session incarnation must be a UUID")
	}
	if revision < 0 || revision > maxJSONSafeInteger {
		return sqliteSessionHeaderRow{}, errors.New("stored session revision must be a non-negative safe integer")
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
	incarnation, err := newSQLiteUUID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO sessions
		(id, version, created_at, cwd, parent_session, seed_length, origin, delegation_depth, agent_preset, incarnation, revision)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		meta.ID, meta.Version, meta.CreatedAt, cwd, parent, seed, origin, depth, preset, incarnation)
	return err
}

func newSQLiteUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[:4], value[4:6], value[6:8], value[8:10], value[10:]), nil
}

func isSQLiteUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil && len(decoded) == 16 && decoded[6]>>4 >= 1 && decoded[6]>>4 <= 8 && decoded[8]&0xc0 == 0x80
}

type sessionSQLiteConnector struct {
	driver.Connector
	busyTimeoutMS int64
}

func (c sessionSQLiteConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	execer, ok := conn.(driver.ExecerContext)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("sqlite driver connection does not support context execution")
	}
	statements := []string{
		fmt.Sprintf("PRAGMA busy_timeout = %d", c.busyTimeoutMS),
		"PRAGMA trusted_schema = OFF",
		"PRAGMA mmap_size = 0",
		"PRAGMA foreign_keys = ON",
		"PRAGMA synchronous = FULL",
	}
	for _, statement := range statements {
		if _, err := execer.ExecContext(ctx, statement, nil); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func prepareSessionSQLitePath(path string) (string, error) {
	if path == ":memory:" {
		return path, nil
	}
	actual, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parent := filepath.Dir(actual)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", err
	}
	if err := validateSessionSQLiteParentDirectory(parent); err != nil {
		return "", err
	}
	if err := validateSessionSQLiteDatabaseFileIfPresent(actual); err != nil {
		return "", err
	}
	file, err := os.OpenFile(actual, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		if err := file.Close(); err != nil {
			return "", err
		}
	} else if !errors.Is(err, os.ErrExist) {
		return "", err
	}
	if err := validateSessionSQLiteDatabaseFile(actual); err != nil {
		return "", err
	}
	return actual, nil
}

func validateSessionSQLiteParentDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("session database parent %q must be a real directory", path)
	}
	return validateSessionSQLiteParentAccess(path, info)
}

func validateSessionSQLiteDatabaseFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("session database %q must be a regular file, not a symbolic link", path)
	}
	return validateSessionSQLiteFileAccess(path, info)
}

func validateSessionSQLiteDatabaseFileIfPresent(path string) error {
	err := validateSessionSQLiteDatabaseFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func openSessionSQLite(path string, journalMode SQLiteJournalMode, busyTimeout time.Duration) (*sql.DB, string, error) {
	actual, err := prepareSessionSQLitePath(path)
	if err != nil {
		return nil, "", err
	}
	base, err := modernsqlite.NewConnector(actual)
	if err != nil {
		return nil, "", err
	}
	db := sql.OpenDB(sessionSQLiteConnector{Connector: base, busyTimeoutMS: busyTimeout.Milliseconds()})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	fail := func(err error) (*sql.DB, string, error) {
		_ = db.Close()
		return nil, "", err
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fail(err)
	}
	closeConn := func(err error) (*sql.DB, string, error) {
		_ = conn.Close()
		return fail(err)
	}
	if err := requireSessionSQLitePragma(ctx, conn, actual, "trusted_schema", 0, "0"); err != nil {
		return closeConn(err)
	}
	if actual != ":memory:" {
		if err := requireSessionSQLitePragma(ctx, conn, actual, "mmap_size", 0, "0"); err != nil {
			return closeConn(err)
		}
	}
	if err := requireSessionSQLitePragma(ctx, conn, actual, "foreign_keys", 1, "1"); err != nil {
		return closeConn(err)
	}
	if err := requireSessionSQLitePragma(ctx, conn, actual, "busy_timeout", busyTimeout.Milliseconds(), fmt.Sprint(busyTimeout.Milliseconds())); err != nil {
		return closeConn(err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return closeConn(err)
	}
	rollback := func(err error) (*sql.DB, string, error) {
		if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}
		return closeConn(err)
	}
	var onDisk, applicationID, objects int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&onDisk); err != nil {
		return rollback(err)
	}
	if err := conn.QueryRowContext(ctx, "PRAGMA application_id").Scan(&applicationID); err != nil {
		return rollback(err)
	}
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").Scan(&objects); err != nil {
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
	if onDisk == 0 {
		if _, err := conn.ExecContext(ctx, strings.Join([]string{
			sessionSQLitePersistenceTable,
			sessionSQLiteSessionsTable,
			sessionSQLiteEventsTable,
		}, ";\n")); err != nil {
			return rollback(err)
		}
		storeID, err := newSQLiteUUID()
		if err != nil {
			return rollback(err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO persistence_state (singleton, store_id) VALUES (1, ?)", storeID); err != nil {
			return rollback(err)
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id = %d", SessionSQLiteApplicationID)); err != nil {
			return rollback(err)
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", SessionSQLiteSchemaVersion)); err != nil {
			return rollback(err)
		}
	}
	if err := validateSQLiteRequiredSchema(ctx, conn); err != nil {
		return rollback(err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return rollback(err)
	}
	var selectedJournal string
	if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode = "+strings.ToUpper(string(journalMode))).Scan(&selectedJournal); err != nil {
		return closeConn(err)
	}
	expectedJournal := string(journalMode)
	if actual == ":memory:" {
		expectedJournal = "memory"
	}
	if !strings.EqualFold(selectedJournal, expectedJournal) {
		return closeConn(fmt.Errorf("session database at %q selected journal mode %s, expected %s", actual, selectedJournal, expectedJournal))
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA synchronous = FULL"); err != nil {
		return closeConn(err)
	}
	if err := requireSessionSQLitePragma(ctx, conn, actual, "synchronous", 2, "FULL (2)"); err != nil {
		return closeConn(err)
	}
	var storeID string
	if err := conn.QueryRowContext(ctx, "SELECT store_id FROM persistence_state WHERE singleton = 1").Scan(&storeID); err != nil {
		return closeConn(err)
	}
	if storeID == "" {
		return closeConn(errors.New("session database has no valid store identity"))
	}
	if !isSQLiteUUID(storeID) {
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

func requireSessionSQLitePragma(ctx context.Context, conn *sql.Conn, path, name string, want int64, expected string) error {
	var got int64
	if err := conn.QueryRowContext(ctx, "PRAGMA "+name).Scan(&got); err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("session database at %q retained %s=%d, expected %s", path, name, got, expected)
	}
	return nil
}
