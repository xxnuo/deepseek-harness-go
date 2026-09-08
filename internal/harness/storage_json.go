package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

type JSONStorageBackend struct {
	root string

	mu        sync.Mutex
	open      map[string]KVUnit
	opening   map[string]struct{}
	openWG    sync.WaitGroup
	closing   bool
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

func NewJSONStorageBackend(root string) (*JSONStorageBackend, error) {
	if root == "" {
		return nil, errors.New("json storage root is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	return &JSONStorageBackend{
		root:      abs,
		open:      map[string]KVUnit{},
		opening:   map[string]struct{}{},
		closeDone: make(chan struct{}),
	}, nil
}

func (b *JSONStorageBackend) KV() KVFacet { return b }

func (b *JSONStorageBackend) Open(ctx context.Context, descriptor KVUnitDescriptor) (KVUnit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateStorageDescriptor(descriptor); err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closing || b.closed {
		b.mu.Unlock()
		return nil, storageError(StorageClosed, "json storage backend is closed")
	}
	if b.open[descriptor.Name] != nil {
		b.mu.Unlock()
		return nil, fmt.Errorf("unit %q is already open; a unit has exactly one live handle", descriptor.Name)
	}
	if _, ok := b.opening[descriptor.Name]; ok {
		b.mu.Unlock()
		return nil, fmt.Errorf("unit %q is already open; a unit has exactly one live handle", descriptor.Name)
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
	if err := os.MkdirAll(b.root, 0o700); err != nil {
		return nil, err
	}
	var unit KVUnit
	onClose := func() {
		b.mu.Lock()
		if b.open[descriptor.Name] == unit {
			delete(b.open, descriptor.Name)
		}
		b.mu.Unlock()
	}
	if descriptor.Layout == KVLayoutPerRecord {
		unit = &perRecordJSONUnit{descriptor: descriptor, dir: filepath.Join(b.root, descriptor.Name), onClose: onClose}
	} else {
		path := filepath.Join(b.root, descriptor.Name+".json")
		state, err := loadJSONUnit(path, descriptor)
		if err != nil {
			return nil, err
		}
		unit = &jsonKVUnit{descriptor: descriptor, path: path, state: state, onClose: onClose}
	}
	b.mu.Lock()
	if b.closing || b.closed {
		b.mu.Unlock()
		_ = unit.Close()
		return nil, storageError(StorageClosed, "json storage backend is closed")
	}
	b.open[descriptor.Name] = unit
	b.mu.Unlock()
	return unit, nil
}

func (b *JSONStorageBackend) Close() error {
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
	units := make([]KVUnit, 0, len(b.open))
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
	b.mu.Lock()
	b.closeErr = closeErr
	b.closed = true
	close(b.closeDone)
	b.mu.Unlock()
	return closeErr
}

type jsonUnitState struct {
	version int
	global  any
	tables  map[string]map[string]any
}

type jsonKVUnit struct {
	mu         sync.Mutex
	descriptor KVUnitDescriptor
	path       string
	state      jsonUnitState
	closed     bool
	onClose    func()
}

func (u *jsonKVUnit) LoadAll(ctx context.Context) (KVSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return KVSnapshot{}, err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return KVSnapshot{}, err
	}
	return cloneKVSnapshot(u.state.tables, u.state.global), nil
}

func (u *jsonKVUnit) PutRecord(ctx context.Context, table, key string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return err
	}
	records, err := u.records(table)
	if err != nil {
		return err
	}
	previous, existed := records[key]
	records[key] = value
	if err := u.publishLocked(); err != nil {
		if existed {
			records[key] = previous
		} else {
			delete(records, key)
		}
		return err
	}
	return nil
}

func (u *jsonKVUnit) DeleteRecord(ctx context.Context, table, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return err
	}
	records, err := u.records(table)
	if err != nil {
		return err
	}
	previous, existed := records[key]
	if !existed {
		return nil
	}
	delete(records, key)
	if err := u.publishLocked(); err != nil {
		records[key] = previous
		return err
	}
	return nil
}

func (u *jsonKVUnit) SetGlobal(ctx context.Context, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return err
	}
	if !u.descriptor.HasGlobal {
		return fmt.Errorf("unit %q does not declare a global slot", u.descriptor.Name)
	}
	previous := u.state.global
	u.state.global = value
	if err := u.publishLocked(); err != nil {
		u.state.global = previous
		return err
	}
	return nil
}

func (u *jsonKVUnit) Close() error {
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

func (u *jsonKVUnit) assertOpen() error {
	if u.closed {
		return storageError(StorageClosed, "unit %q is closed", u.descriptor.Name)
	}
	return nil
}

func (u *jsonKVUnit) records(table string) (map[string]any, error) {
	records := u.state.tables[table]
	if records == nil {
		return nil, fmt.Errorf("unit %q does not declare table %q", u.descriptor.Name, table)
	}
	return records, nil
}

func (u *jsonKVUnit) publishLocked() error {
	document := struct {
		Unit struct {
			Name    string `json:"name"`
			Version int    `json:"version"`
		} `json:"unit"`
		Global any                       `json:"global"`
		Tables map[string]map[string]any `json:"tables"`
	}{Global: u.state.global, Tables: u.state.tables}
	document.Unit.Name = u.descriptor.Name
	document.Unit.Version = u.state.version
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeAtomicFile(u.path, data)
}

func loadJSONUnit(path string, descriptor KVUnitDescriptor) (jsonUnitState, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		tables := make(map[string]map[string]any, len(descriptor.Tables))
		for _, table := range descriptor.Tables {
			tables[table] = map[string]any{}
		}
		return jsonUnitState{version: descriptor.Version, tables: tables}, nil
	}
	if err != nil {
		return jsonUnitState{}, err
	}
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return jsonUnitState{}, storageErrorWrap(StorageMalformedMedium, err, "unit %q: file is not valid JSON", descriptor.Name)
	}
	document, ok := raw.(map[string]any)
	if !ok {
		return jsonUnitState{}, storageError(StorageMalformedMedium, "unit %q: file is not a JSON object", descriptor.Name)
	}
	header, ok := document["unit"].(map[string]any)
	if !ok || header["name"] != descriptor.Name {
		return jsonUnitState{}, storageError(StorageMalformedMedium, "unit %q: missing or foreign unit header", descriptor.Name)
	}
	storedVersion, ok := header["version"].(float64)
	if !ok || storedVersion != float64(int(storedVersion)) {
		return jsonUnitState{}, storageError(StorageMalformedMedium, "unit %q: missing or foreign unit header", descriptor.Name)
	}
	if int(storedVersion) != descriptor.Version {
		return jsonUnitState{}, storageError(StorageVersionMismatch, "unit %q: stored version %d != expected %d", descriptor.Name, int(storedVersion), descriptor.Version)
	}
	rawTables, ok := document["tables"].(map[string]any)
	if !ok {
		return jsonUnitState{}, storageError(StorageMalformedMedium, "unit %q: tables is not an object", descriptor.Name)
	}
	tables := make(map[string]map[string]any, len(descriptor.Tables))
	for _, table := range descriptor.Tables {
		rawRecords, exists := rawTables[table]
		if !exists {
			tables[table] = map[string]any{}
			continue
		}
		records, ok := rawRecords.(map[string]any)
		if !ok {
			return jsonUnitState{}, storageError(StorageMalformedMedium, "unit %q: table %q is not an object", descriptor.Name, table)
		}
		tables[table] = records
	}
	return jsonUnitState{version: descriptor.Version, global: document["global"], tables: tables}, nil
}

func validateStorageDescriptor(descriptor KVUnitDescriptor) error {
	if !ValidStorageUnitName(descriptor.Name) {
		return storageError(StorageMalformedMedium, "invalid unit name %q", descriptor.Name)
	}
	for _, table := range descriptor.Tables {
		if !ValidStorageUnitName(table) {
			return storageError(StorageMalformedMedium, "invalid table name %q in unit %q", table, descriptor.Name)
		}
	}
	if descriptor.Layout != "" && descriptor.Layout != KVLayoutSingle && descriptor.Layout != KVLayoutPerRecord {
		return storageError(StorageMalformedMedium, "invalid layout %q in unit %q", descriptor.Layout, descriptor.Name)
	}
	return nil
}

var perRecordSafeKeyRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type perRecordJSONUnit struct {
	mu         sync.Mutex
	descriptor KVUnitDescriptor
	dir        string
	closed     bool
	onClose    func()
}

func (u *perRecordJSONUnit) LoadAll(ctx context.Context) (KVSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return KVSnapshot{}, err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return KVSnapshot{}, err
	}
	return loadPerRecordJSONUnit(u.descriptor, u.dir)
}

func (u *perRecordJSONUnit) PutRecord(ctx context.Context, table, key string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return err
	}
	path, err := u.recordPath(table, key)
	if err != nil {
		return err
	}
	return writePerRecordDocument(path, u.descriptor.Version, value)
}

func (u *perRecordJSONUnit) DeleteRecord(ctx context.Context, table, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return err
	}
	path, err := u.recordPath(table, key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (u *perRecordJSONUnit) BackupRecord(ctx context.Context, table, key string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return "", err
	}
	path, err := u.recordPath(table, key)
	if err != nil {
		return "", err
	}
	moved := path + ".bak." + time.Now().Format("200601021504")
	_ = os.Remove(moved)
	if err := os.Rename(path, moved); err != nil {
		return "", err
	}
	return moved, nil
}

func (u *perRecordJSONUnit) SetGlobal(ctx context.Context, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.assertOpen(); err != nil {
		return err
	}
	if !u.descriptor.HasGlobal {
		return fmt.Errorf("unit %q does not declare a global slot", u.descriptor.Name)
	}
	return writePerRecordDocument(filepath.Join(u.dir, "global.json"), u.descriptor.Version, value)
}

func (u *perRecordJSONUnit) Close() error {
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

func (u *perRecordJSONUnit) assertOpen() error {
	if u.closed {
		return storageError(StorageClosed, "unit %q is closed", u.descriptor.Name)
	}
	return nil
}

func (u *perRecordJSONUnit) recordPath(table, key string) (string, error) {
	declared := false
	for _, candidate := range u.descriptor.Tables {
		if candidate == table {
			declared = true
			break
		}
	}
	if !declared {
		return "", fmt.Errorf("unit %q does not declare table %q", u.descriptor.Name, table)
	}
	if !perRecordSafeKeyRE.MatchString(key) {
		return "", fmt.Errorf("unit %q: per-record key %q is not path-safe (must match %s)", u.descriptor.Name, key, perRecordSafeKeyRE)
	}
	return filepath.Join(u.dir, table, key+".json"), nil
}

func loadPerRecordJSONUnit(descriptor KVUnitDescriptor, dir string) (KVSnapshot, error) {
	tables := make(map[string]map[string]any, len(descriptor.Tables))
	for _, table := range descriptor.Tables {
		tables[table] = map[string]any{}
	}
	entries, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return KVSnapshot{}, err
	}
	hasDocuments := false
	var global any
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				records := tables[entry.Name()]
				if records != nil {
					found, err := loadPerRecordTable(filepath.Join(dir, entry.Name()), descriptor, records)
					if err != nil {
						return KVSnapshot{}, err
					}
					hasDocuments = hasDocuments || found
					continue
				}
			}
			if entry.Name() == "global.json" && descriptor.HasGlobal {
				hasDocuments = true
				if value, ok := readPerRecordDocument(filepath.Join(dir, entry.Name()), descriptor); ok {
					global = value
				}
			}
		}
	}
	if !hasDocuments {
		if err := bootstrapLegacyJSONUnit(descriptor, dir, tables); err != nil {
			return KVSnapshot{}, err
		}
	}
	return KVSnapshot{Tables: tables, Global: global}, nil
}

func loadPerRecordTable(dir string, descriptor KVUnitDescriptor, records map[string]any) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	hasDocuments := false
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		hasDocuments = true
		key := strings.TrimSuffix(entry.Name(), ".json")
		if !perRecordSafeKeyRE.MatchString(key) {
			continue
		}
		if value, ok := readPerRecordDocument(filepath.Join(dir, entry.Name()), descriptor); ok {
			records[key] = value
		}
	}
	return hasDocuments, nil
}

func readPerRecordDocument(path string, descriptor KVUnitDescriptor) (any, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var document struct {
		Version *int            `json:"version"`
		Record  json.RawMessage `json:"record"`
	}
	if json.Unmarshal(data, &document) != nil || document.Version == nil || document.Record == nil {
		return nil, false
	}
	if !acceptsStorageVersion(descriptor, *document.Version) {
		return nil, false
	}
	var value any
	if json.Unmarshal(document.Record, &value) != nil {
		return nil, false
	}
	return value, true
}

func bootstrapLegacyJSONUnit(descriptor KVUnitDescriptor, dir string, tables map[string]map[string]any) error {
	data, err := os.ReadFile(filepath.Join(filepath.Dir(dir), descriptor.Name+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var document struct {
		Unit struct {
			Name    string `json:"name"`
			Version *int   `json:"version"`
		} `json:"unit"`
		Tables json.RawMessage `json:"tables"`
	}
	if json.Unmarshal(data, &document) != nil || document.Unit.Name != descriptor.Name || document.Unit.Version == nil || document.Tables == nil {
		return nil
	}
	if !acceptsStorageVersion(descriptor, *document.Unit.Version) {
		return nil
	}
	var rawTables map[string]json.RawMessage
	if json.Unmarshal(document.Tables, &rawTables) != nil || rawTables == nil {
		return nil
	}
	for _, table := range descriptor.Tables {
		rawRecords, ok := rawTables[table]
		if !ok {
			continue
		}
		var records map[string]any
		if json.Unmarshal(rawRecords, &records) != nil || records == nil {
			return nil
		}
		target := tables[table]
		for key, value := range records {
			if !perRecordSafeKeyRE.MatchString(key) {
				continue
			}
			if err := writePerRecordDocument(filepath.Join(dir, table, key+".json"), descriptor.Version, value); err != nil {
				return err
			}
			target[key] = value
		}
	}
	return nil
}

func acceptsStorageVersion(descriptor KVUnitDescriptor, stored int) bool {
	if stored == descriptor.Version {
		return true
	}
	for _, version := range descriptor.CompatibleVersions {
		if stored == version {
			return true
		}
	}
	return false
}

func writePerRecordDocument(path string, version int, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	document := struct {
		Version int `json:"version"`
		Record  any `json:"record"`
	}{Version: version, Record: value}
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomicFile(path, append(data, '\n'))
}

func cloneKVSnapshot(tables map[string]map[string]any, global any) KVSnapshot {
	copyTables := make(map[string]map[string]any, len(tables))
	for table, records := range tables {
		copyRecords := make(map[string]any, len(records))
		for key, value := range records {
			copyRecords[key] = value
		}
		copyTables[table] = copyRecords
	}
	return KVSnapshot{Tables: copyTables, Global: global}
}

func writeAtomicFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".dsh-storage-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
