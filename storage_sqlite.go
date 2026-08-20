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

	_ "modernc.org/sqlite"
)

const StorageSQLiteSchemaVersion = 1

type SQLiteJournalMode string

const (
	SQLiteJournalWAL      SQLiteJournalMode = "wal"
	SQLiteJournalDelete   SQLiteJournalMode = "delete"
	SQLiteJournalTruncate SQLiteJournalMode = "truncate"
	SQLiteJournalPersist  SQLiteJournalMode = "persist"
)

type SQLiteStorageBackend struct {
	db *sql.DB

	mu        sync.Mutex
	open      map[string]*sqliteKVUnit
	opening   map[string]struct{}
	openWG    sync.WaitGroup
	closing   bool
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

func NewSQLiteStorageBackend(path string, journalMode SQLiteJournalMode) (*SQLiteStorageBackend, error) {
	if path == "" {
		return nil, errors.New("sqlite storage path is required")
	}
	if journalMode == "" {
		journalMode = SQLiteJournalWAL
	}
	switch journalMode {
	case SQLiteJournalWAL, SQLiteJournalDelete, SQLiteJournalTruncate, SQLiteJournalPersist:
	default:
		return nil, fmt.Errorf("unsupported sqlite journal mode %q", journalMode)
	}
	db, err := openStorageSQLite(path, journalMode)
	if err != nil {
		return nil, err
	}
	return &SQLiteStorageBackend{
		db:        db,
		open:      map[string]*sqliteKVUnit{},
		opening:   map[string]struct{}{},
		closeDone: make(chan struct{}),
	}, nil
}

func (b *SQLiteStorageBackend) KV() KVFacet { return b }

func (b *SQLiteStorageBackend) Open(ctx context.Context, descriptor KVUnitDescriptor) (KVUnit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateStorageDescriptor(descriptor); err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closing || b.closed {
		b.mu.Unlock()
		return nil, storageError(StorageClosed, "sqlite storage backend is closed")
	}
	if b.open[descriptor.Name] != nil {
		b.mu.Unlock()
		return nil, fmt.Errorf("kv unit %q is already open (double-open is a caller bug)", descriptor.Name)
	}
	if _, ok := b.opening[descriptor.Name]; ok {
		b.mu.Unlock()
		return nil, fmt.Errorf("kv unit %q is already open (double-open is a caller bug)", descriptor.Name)
	}
	b.opening[descriptor.Name] = struct{}{}
	b.openWG.Add(1)
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.opening, descriptor.Name)
		b.mu.Unlock()
		b.openWG.Done()
	}()

	var storedVersion int
	err := b.db.QueryRowContext(ctx, "SELECT version FROM units WHERE name = ?", descriptor.Name).Scan(&storedVersion)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := b.db.ExecContext(ctx, "INSERT INTO units (name, version) VALUES (?, ?)", descriptor.Name, descriptor.Version); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	case storedVersion != descriptor.Version:
		return nil, storageError(StorageVersionMismatch, "kv unit %q is stamped version %d on the medium, incompatible with descriptor version %d", descriptor.Name, storedVersion, descriptor.Version)
	}
	for _, table := range descriptor.Tables {
		name := sqliteRecordTableName(descriptor.Name, table)
		if _, err := b.db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT`, name)); err != nil {
			return nil, err
		}
	}
	var unit *sqliteKVUnit
	unit = &sqliteKVUnit{db: b.db, descriptor: descriptor, onClose: func() {
		b.mu.Lock()
		if b.open[descriptor.Name] == unit {
			delete(b.open, descriptor.Name)
		}
		b.mu.Unlock()
	}}
	b.mu.Lock()
	if b.closing || b.closed {
		b.mu.Unlock()
		_ = unit.Close()
		return nil, storageError(StorageClosed, "sqlite storage backend is closed")
	}
	b.open[descriptor.Name] = unit
	b.mu.Unlock()
	return unit, nil
}

func (b *SQLiteStorageBackend) Close() error {
	b.mu.Lock()
	if b.closing || b.closed {
		done := b.closeDone
		b.mu.Unlock()
		<-done
		return b.closeErr
	}
	b.closing = true
	b.mu.Unlock()

	b.openWG.Wait()
	b.mu.Lock()
	units := make([]*sqliteKVUnit, 0, len(b.open))
	for _, unit := range b.open {
		units = append(units, unit)
	}
	b.mu.Unlock()
	var closeErr error
	for _, unit := range units {
		if err := unit.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if err := b.db.Close(); err != nil && closeErr == nil {
		closeErr = err
	}
	b.mu.Lock()
	b.closeErr = closeErr
	b.closed = true
	close(b.closeDone)
	b.mu.Unlock()
	return closeErr
}

type sqliteKVUnit struct {
	db         *sql.DB
	descriptor KVUnitDescriptor
	onClose    func()

	mu     sync.Mutex
	closed bool
}

func (u *sqliteKVUnit) LoadAll(ctx context.Context) (KVSnapshot, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return KVSnapshot{}, err
	}
	tables := make(map[string]map[string]any, len(u.descriptor.Tables))
	for _, table := range u.descriptor.Tables {
		rows, err := u.db.QueryContext(ctx, fmt.Sprintf(`SELECT key, value FROM %q`, sqliteRecordTableName(u.descriptor.Name, table)))
		if err != nil {
			return KVSnapshot{}, err
		}
		records := map[string]any{}
		for rows.Next() {
			var key, text string
			if err := rows.Scan(&key, &text); err != nil {
				_ = rows.Close()
				return KVSnapshot{}, err
			}
			value, err := parseSQLiteJSON(u.descriptor.Name, fmt.Sprintf("table %q key %q", table, key), text)
			if err != nil {
				_ = rows.Close()
				return KVSnapshot{}, err
			}
			records[key] = value
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return KVSnapshot{}, err
		}
		_ = rows.Close()
		tables[table] = records
	}
	var global any
	if u.descriptor.HasGlobal {
		var text string
		err := u.db.QueryRowContext(ctx, "SELECT value FROM unit_globals WHERE unit = ?", u.descriptor.Name).Scan(&text)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return KVSnapshot{}, err
		}
		if err == nil {
			value, err := parseSQLiteJSON(u.descriptor.Name, "global slot", text)
			if err != nil {
				return KVSnapshot{}, err
			}
			global = value
		}
	}
	return KVSnapshot{Tables: tables, Global: global}, nil
}

func (u *sqliteKVUnit) PutRecord(ctx context.Context, table, key string, value any) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return err
	}
	if !u.hasTable(table) {
		return fmt.Errorf("kv unit %q declared no table %q", u.descriptor.Name, table)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = u.db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %q (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, sqliteRecordTableName(u.descriptor.Name, table)), key, string(data))
	return err
}

func (u *sqliteKVUnit) DeleteRecord(ctx context.Context, table, key string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return err
	}
	if !u.hasTable(table) {
		return fmt.Errorf("kv unit %q declared no table %q", u.descriptor.Name, table)
	}
	_, err := u.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %q WHERE key = ?`, sqliteRecordTableName(u.descriptor.Name, table)), key)
	return err
}

func (u *sqliteKVUnit) SetGlobal(ctx context.Context, value any) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return err
	}
	if !u.descriptor.HasGlobal {
		return fmt.Errorf("kv unit %q declared no global slot", u.descriptor.Name)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = u.db.ExecContext(ctx, "INSERT INTO unit_globals (unit, value) VALUES (?, ?) ON CONFLICT(unit) DO UPDATE SET value = excluded.value", u.descriptor.Name, string(data))
	return err
}

func (u *sqliteKVUnit) Close() error {
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return nil
	}
	u.closed = true
	u.mu.Unlock()
	u.onClose()
	return nil
}

func (u *sqliteKVUnit) assertOpen() error {
	if u.closed {
		return storageError(StorageClosed, "kv unit %q is closed", u.descriptor.Name)
	}
	return nil
}

func (u *sqliteKVUnit) hasTable(name string) bool {
	for _, table := range u.descriptor.Tables {
		if table == name {
			return true
		}
	}
	return false
}

func parseSQLiteJSON(unit, slot, text string) (any, error) {
	var value any
	if err := json.Unmarshal([]byte(text), &value); err != nil {
		return nil, storageErrorWrap(StorageMalformedMedium, err, "kv unit %q holds unparsable JSON at %s", unit, slot)
	}
	return value, nil
}

func openStorageSQLite(path string, journalMode SQLiteJournalMode) (*sql.DB, error) {
	actual := path
	if path != ":memory:" {
		var err error
		actual, err = filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(actual), 0o700); err != nil {
			return nil, err
		}
		file, err := os.OpenFile(actual, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			if err := file.Close(); err != nil {
				return nil, err
			}
		} else if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", actual)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	closeOnError := func(err error) (*sql.DB, error) {
		_ = db.Close()
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return closeOnError(err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return closeOnError(err)
	}
	if _, err := db.Exec("PRAGMA journal_mode = " + strings.ToUpper(string(journalMode))); err != nil {
		return closeOnError(err)
	}
	var onDisk int
	if err := db.QueryRow("PRAGMA user_version").Scan(&onDisk); err != nil {
		return closeOnError(err)
	}
	if onDisk != 0 && onDisk != StorageSQLiteSchemaVersion {
		return closeOnError(storageError(StorageVersionMismatch, "storage database at %q has schema version %d, incompatible with this build (%d)", actual, onDisk, StorageSQLiteSchemaVersion))
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS units (name TEXT PRIMARY KEY, version INTEGER NOT NULL) STRICT`); err != nil {
		return closeOnError(err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS unit_globals (unit TEXT PRIMARY KEY REFERENCES units(name), value TEXT NOT NULL) STRICT`); err != nil {
		return closeOnError(err)
	}
	if onDisk == 0 {
		if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", StorageSQLiteSchemaVersion)); err != nil {
			return closeOnError(err)
		}
	}
	return db, nil
}

func sqliteRecordTableName(unit, table string) string { return "u_" + unit + "_" + table }
