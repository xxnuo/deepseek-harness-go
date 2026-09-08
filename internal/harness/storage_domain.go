package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
)

type DomainErrorCode string

const (
	DomainAlreadyOpen      DomainErrorCode = "already-open"
	DomainFacetUnsupported DomainErrorCode = "facet-unsupported"
	DomainInvalidRecord    DomainErrorCode = "invalid-record"
	DomainMissingKey       DomainErrorCode = "missing-key"
	DomainClosed           DomainErrorCode = "closed"
)

type InvalidDomainRecord struct {
	Table string
	Key   string
}

type DomainError struct {
	Code   DomainErrorCode
	Msg    string
	Detail *InvalidDomainRecord
	Err    error
}

func (e *DomainError) Error() string { return e.Msg }
func (e *DomainError) Unwrap() error { return e.Err }

func IsDomainError(err error, code DomainErrorCode) bool {
	var target *DomainError
	return errors.As(err, &target) && target.Code == code
}

type DomainValueParser func(any) (any, error)

func JSONDomainParser[T any](validate func(T) error) DomainValueParser {
	return func(value any) (any, error) {
		if value == nil {
			return nil, errors.New("null is not accepted")
		}
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		var parsed T
		if err := json.Unmarshal(data, &parsed); err != nil {
			return nil, err
		}
		if validate != nil {
			if err := validate(parsed); err != nil {
				return nil, err
			}
		}
		return parsed, nil
	}
}

type DomainGlobalSpec struct {
	Parse   DomainValueParser
	Initial any
}

type DomainTableSpec struct {
	Parse DomainValueParser
}

type DomainInvalidRecordsPolicy string

const DomainInvalidRecordsBackupAndSkip DomainInvalidRecordsPolicy = "backup-and-skip"

type DomainSpec struct {
	Name               string
	Version            int
	Layout             KVLayout
	CompatibleVersions []int
	InvalidRecords     DomainInvalidRecordsPolicy
	Global             *DomainGlobalSpec
	Tables             map[string]DomainTableSpec
}

func ValidateDomainSpec(spec DomainSpec) error {
	if !ValidStorageUnitName(spec.Name) {
		return fmt.Errorf("domain name %q must match %s", spec.Name, storageUnitNameRE)
	}
	if spec.Version < 0 {
		return fmt.Errorf("domain %q version must be a non-negative integer, got %d", spec.Name, spec.Version)
	}
	for _, version := range spec.CompatibleVersions {
		if version < 0 || version >= spec.Version {
			return fmt.Errorf("domain %q compatibleVersions entries must be non-negative integers below version %d, got %d", spec.Name, spec.Version, version)
		}
	}
	if spec.Layout != "" && spec.Layout != KVLayoutSingle && spec.Layout != KVLayoutPerRecord {
		return fmt.Errorf("domain %q layout must be 'single' or 'per-record', got %q", spec.Name, spec.Layout)
	}
	if spec.InvalidRecords != "" && spec.InvalidRecords != DomainInvalidRecordsBackupAndSkip {
		return fmt.Errorf("domain %q invalidRecords must be 'backup-and-skip' when present, got %q", spec.Name, spec.InvalidRecords)
	}
	for name, table := range spec.Tables {
		if !ValidStorageUnitName(name) {
			return fmt.Errorf("domain %q table name %q must match %s", spec.Name, name, storageUnitNameRE)
		}
		if table.Parse == nil {
			return fmt.Errorf("domain %q table %q requires a parser", spec.Name, name)
		}
	}
	if spec.Global != nil {
		if spec.Global.Parse == nil {
			return fmt.Errorf("domain %q global requires a parser", spec.Name)
		}
		if _, err := spec.Global.Parse(nil); err == nil {
			return fmt.Errorf("domain %q global parser must not accept null", spec.Name)
		}
		if _, err := spec.Global.Parse(spec.Global.Initial); err != nil {
			return fmt.Errorf("domain %q initial global value: %w", spec.Name, err)
		}
	}
	return nil
}

func DomainDescriptor(spec DomainSpec) KVUnitDescriptor {
	tables := make([]string, 0, len(spec.Tables))
	for table := range spec.Tables {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return KVUnitDescriptor{
		Name: spec.Name, Version: spec.Version, Tables: tables, HasGlobal: spec.Global != nil,
		Layout: spec.Layout, CompatibleVersions: append([]int(nil), spec.CompatibleVersions...),
	}
}

type DomainChange struct {
	Domain    string
	Table     string
	Key       string
	Operation string
	Value     any
}

type DomainFacility struct {
	hub     *StorageHub
	backend string
	routes  map[string]string
	unmount func()

	mu        sync.RWMutex
	domains   map[string]*Domain
	reserved  map[string]bool
	listeners map[uint64]func(DomainChange)
	nextID    uint64
	closing   bool
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

func NewDomainFacility(hub *StorageHub, backend string, routes map[string]string) (*DomainFacility, error) {
	if hub == nil {
		return nil, errors.New("storage hub is required")
	}
	if backend == "" {
		return nil, errors.New("default domain backend is required")
	}
	copyRoutes := make(map[string]string, len(routes))
	for domain, route := range routes {
		copyRoutes[domain] = route
	}
	facility := &DomainFacility{
		hub: hub, backend: backend, routes: copyRoutes,
		domains: map[string]*Domain{}, reserved: map[string]bool{},
		listeners: map[uint64]func(DomainChange){}, closeDone: make(chan struct{}),
	}
	unmount, err := hub.Mount("domain", facility)
	if err != nil {
		return nil, err
	}
	facility.unmount = unmount
	return facility, nil
}

func (f *DomainFacility) Open(ctx context.Context, spec DomainSpec) (*Domain, error) {
	if err := ValidateDomainSpec(spec); err != nil {
		return nil, err
	}
	f.mu.Lock()
	if f.closing || f.closed {
		f.mu.Unlock()
		return nil, &DomainError{Code: DomainClosed, Msg: "domain facility is closed"}
	}
	if f.reserved[spec.Name] {
		f.mu.Unlock()
		return nil, &DomainError{Code: DomainAlreadyOpen, Msg: fmt.Sprintf("domain %q is already open", spec.Name)}
	}
	f.reserved[spec.Name] = true
	f.mu.Unlock()

	release := true
	defer func() {
		if release {
			f.mu.Lock()
			delete(f.reserved, spec.Name)
			f.mu.Unlock()
		}
	}()
	backendName := f.backend
	if route := f.routes[spec.Name]; route != "" {
		backendName = route
	}
	backend, err := f.hub.Backend.Get(backendName)
	if err != nil {
		return nil, err
	}
	facet := backend.KV()
	if facet == nil {
		return nil, &DomainError{Code: DomainFacetUnsupported, Msg: fmt.Sprintf("backend %q routed for domain %q has no kv facet", backendName, spec.Name)}
	}
	unit, err := facet.Open(ctx, DomainDescriptor(spec))
	if err != nil {
		return nil, err
	}
	keepUnit := false
	defer func() {
		if !keepUnit {
			_ = unit.Close()
		}
	}()
	snapshot, err := unit.LoadAll(ctx)
	if err != nil {
		return nil, err
	}
	tables := make(map[string]map[string]any, len(spec.Tables))
	for table, tableSpec := range spec.Tables {
		records := map[string]any{}
		for key, raw := range snapshot.Tables[table] {
			value, err := tableSpec.Parse(raw)
			if err != nil {
				invalid := invalidDomainRecord(spec.Name, table, key, err)
				backupper, ok := unit.(KVRecordBackupper)
				if spec.InvalidRecords != DomainInvalidRecordsBackupAndSkip || !ok {
					return nil, invalid
				}
				moved, backupErr := backupper.BackupRecord(ctx, table, key)
				if backupErr != nil {
					return nil, backupErr
				}
				log.Printf("domain %q: stored record %q in table %q failed schema validation; moved to %q and treated as absent. Cause: %v", spec.Name, key, table, moved, err)
				continue
			}
			records[key] = value
		}
		tables[table] = records
	}
	var global any
	if spec.Global != nil {
		global = spec.Global.Initial
		if snapshot.Global != nil {
			global, err = spec.Global.Parse(snapshot.Global)
			if err != nil {
				return nil, invalidDomainRecord(spec.Name, "", "", err)
			}
		}
	}
	domain := newDomain(spec.Name, unit, tables, spec.Global != nil, global, f.emit, func() {
		f.mu.Lock()
		delete(f.domains, spec.Name)
		delete(f.reserved, spec.Name)
		f.mu.Unlock()
	})
	f.mu.Lock()
	if f.closing || f.closed {
		f.mu.Unlock()
		return nil, &DomainError{Code: DomainClosed, Msg: "domain facility is closed"}
	}
	f.domains[spec.Name] = domain
	f.mu.Unlock()
	keepUnit = true
	release = false
	return domain, nil
}

func (f *DomainFacility) Get(name string) *Domain {
	f.mu.RLock()
	domain := f.domains[name]
	f.mu.RUnlock()
	return domain
}

func (f *DomainFacility) OnChange(listener func(DomainChange)) func() {
	if listener == nil {
		return func() {}
	}
	f.mu.Lock()
	f.nextID++
	id := f.nextID
	f.listeners[id] = listener
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		delete(f.listeners, id)
		f.mu.Unlock()
	}
}

func (f *DomainFacility) emit(change DomainChange) {
	f.mu.RLock()
	listeners := make([]func(DomainChange), 0, len(f.listeners))
	for _, listener := range f.listeners {
		listeners = append(listeners, listener)
	}
	f.mu.RUnlock()
	for _, listener := range listeners {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					log.Printf("domain %q: change listener failed: %v", change.Domain, recovered)
				}
			}()
			listener(change)
		}()
	}
}

func (f *DomainFacility) Close() error {
	f.mu.Lock()
	if f.closing || f.closed {
		done := f.closeDone
		f.mu.Unlock()
		<-done
		return f.closeErr
	}
	f.closing = true
	domains := make([]*Domain, 0, len(f.domains))
	for _, domain := range f.domains {
		domains = append(domains, domain)
	}
	f.mu.Unlock()
	var errs []error
	for _, domain := range domains {
		if err := domain.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if f.unmount != nil {
		f.unmount()
	}
	f.mu.Lock()
	f.closeErr = errors.Join(errs...)
	f.closed = true
	close(f.closeDone)
	f.mu.Unlock()
	return f.closeErr
}

func invalidDomainRecord(domain, table, key string, err error) error {
	slot := "global"
	if table != "" {
		slot = fmt.Sprintf("table %q key %q", table, key)
	}
	return &DomainError{
		Code:   DomainInvalidRecord,
		Msg:    fmt.Sprintf("domain %q has an invalid stored record at %s", domain, slot),
		Detail: &InvalidDomainRecord{Table: table, Key: key}, Err: err,
	}
}

type Domain struct {
	Name string
	unit KVUnit

	dataMu    sync.RWMutex
	tables    map[string]*DomainTable
	hasGlobal bool
	global    any
	emit      func(DomainChange)
	onClosed  func()

	lifeMu    sync.Mutex
	lifeCond  *sync.Cond
	writeMu   sync.Mutex
	pending   int
	closing   bool
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

func newDomain(name string, unit KVUnit, records map[string]map[string]any, hasGlobal bool, global any, emit func(DomainChange), onClosed func()) *Domain {
	domain := &Domain{
		Name: name, unit: unit, hasGlobal: hasGlobal, global: global,
		tables: map[string]*DomainTable{}, emit: emit, onClosed: onClosed,
		closeDone: make(chan struct{}),
	}
	domain.lifeCond = sync.NewCond(&domain.lifeMu)
	for table, values := range records {
		domain.tables[table] = &DomainTable{domain: domain, name: table, records: values}
	}
	return domain
}

func (d *Domain) Table(name string) (*DomainTable, error) {
	if err := d.assertReadable(); err != nil {
		return nil, err
	}
	d.dataMu.RLock()
	table := d.tables[name]
	d.dataMu.RUnlock()
	if table == nil {
		return nil, fmt.Errorf("domain %q declares no table %q", d.Name, name)
	}
	return table, nil
}

func (d *Domain) Global() (*DomainGlobal, error) {
	if err := d.assertReadable(); err != nil {
		return nil, err
	}
	if !d.hasGlobal {
		return nil, fmt.Errorf("domain %q declares no global", d.Name)
	}
	return &DomainGlobal{domain: d}, nil
}

func (d *Domain) Close() error {
	d.lifeMu.Lock()
	if d.closing || d.closed {
		done := d.closeDone
		d.lifeMu.Unlock()
		<-done
		return d.closeErr
	}
	d.closing = true
	for d.pending > 0 {
		d.lifeCond.Wait()
	}
	d.lifeMu.Unlock()
	err := d.unit.Close()
	d.lifeMu.Lock()
	d.closeErr = err
	if err == nil {
		d.closed = true
	}
	close(d.closeDone)
	d.lifeMu.Unlock()
	if err == nil {
		d.onClosed()
	}
	return err
}

func (d *Domain) beginWrite() error {
	d.lifeMu.Lock()
	if d.closing || d.closed {
		d.lifeMu.Unlock()
		return &DomainError{Code: DomainClosed, Msg: fmt.Sprintf("domain %q is closed", d.Name)}
	}
	d.pending++
	d.lifeMu.Unlock()
	d.writeMu.Lock()
	return nil
}

func (d *Domain) endWrite() {
	d.writeMu.Unlock()
	d.lifeMu.Lock()
	d.pending--
	if d.pending == 0 {
		d.lifeCond.Broadcast()
	}
	d.lifeMu.Unlock()
}

func (d *Domain) assertReadable() error {
	d.lifeMu.Lock()
	closed := d.closed
	d.lifeMu.Unlock()
	if closed {
		return &DomainError{Code: DomainClosed, Msg: fmt.Sprintf("domain %q is closed", d.Name)}
	}
	return nil
}

type DomainTable struct {
	domain  *Domain
	name    string
	records map[string]any
}

func (t *DomainTable) Get(key string) (any, bool, error) {
	if err := t.domain.assertReadable(); err != nil {
		return nil, false, err
	}
	t.domain.dataMu.RLock()
	value, ok := t.records[key]
	t.domain.dataMu.RUnlock()
	return value, ok, nil
}

func (t *DomainTable) Entries() (map[string]any, error) {
	if err := t.domain.assertReadable(); err != nil {
		return nil, err
	}
	t.domain.dataMu.RLock()
	entries := make(map[string]any, len(t.records))
	for key, value := range t.records {
		entries[key] = value
	}
	t.domain.dataMu.RUnlock()
	return entries, nil
}

func (t *DomainTable) Size() (int, error) {
	if err := t.domain.assertReadable(); err != nil {
		return 0, err
	}
	t.domain.dataMu.RLock()
	size := len(t.records)
	t.domain.dataMu.RUnlock()
	return size, nil
}

func (t *DomainTable) Put(ctx context.Context, key string, value any) error {
	if err := t.domain.beginWrite(); err != nil {
		return err
	}
	defer t.domain.endWrite()
	if err := t.domain.unit.PutRecord(ctx, t.name, key, value); err != nil {
		return err
	}
	t.domain.dataMu.Lock()
	t.records[key] = value
	t.domain.dataMu.Unlock()
	t.domain.emit(DomainChange{Domain: t.domain.Name, Table: t.name, Key: key, Operation: "put", Value: value})
	return nil
}

func (t *DomainTable) Delete(ctx context.Context, key string) (bool, error) {
	if err := t.domain.beginWrite(); err != nil {
		return false, err
	}
	defer t.domain.endWrite()
	t.domain.dataMu.RLock()
	_, exists := t.records[key]
	t.domain.dataMu.RUnlock()
	if !exists {
		return false, nil
	}
	if err := t.domain.unit.DeleteRecord(ctx, t.name, key); err != nil {
		return false, err
	}
	t.domain.dataMu.Lock()
	delete(t.records, key)
	t.domain.dataMu.Unlock()
	t.domain.emit(DomainChange{Domain: t.domain.Name, Table: t.name, Key: key, Operation: "deleted"})
	return true, nil
}

func (t *DomainTable) Update(ctx context.Context, key string, transform func(any) (any, error)) (any, error) {
	if transform == nil {
		return nil, errors.New("domain update transform is nil")
	}
	if err := t.domain.beginWrite(); err != nil {
		return nil, err
	}
	defer t.domain.endWrite()
	t.domain.dataMu.RLock()
	current, exists := t.records[key]
	t.domain.dataMu.RUnlock()
	if !exists {
		return nil, &DomainError{Code: DomainMissingKey, Msg: fmt.Sprintf("domain %q table %q has no record %q to update", t.domain.Name, t.name, key)}
	}
	next, err := transform(current)
	if err != nil {
		return nil, err
	}
	if err := t.domain.unit.PutRecord(ctx, t.name, key, next); err != nil {
		return nil, err
	}
	t.domain.dataMu.Lock()
	t.records[key] = next
	t.domain.dataMu.Unlock()
	t.domain.emit(DomainChange{Domain: t.domain.Name, Table: t.name, Key: key, Operation: "put", Value: next})
	return next, nil
}

type DomainGlobal struct{ domain *Domain }

func (g *DomainGlobal) Get() (any, error) {
	if err := g.domain.assertReadable(); err != nil {
		return nil, err
	}
	g.domain.dataMu.RLock()
	value := g.domain.global
	g.domain.dataMu.RUnlock()
	return value, nil
}

func (g *DomainGlobal) Set(ctx context.Context, value any) error {
	if err := g.domain.beginWrite(); err != nil {
		return err
	}
	defer g.domain.endWrite()
	if err := g.domain.unit.SetGlobal(ctx, value); err != nil {
		return err
	}
	g.domain.dataMu.Lock()
	g.domain.global = value
	g.domain.dataMu.Unlock()
	g.domain.emit(DomainChange{Domain: g.domain.Name, Operation: "put", Value: value})
	return nil
}
