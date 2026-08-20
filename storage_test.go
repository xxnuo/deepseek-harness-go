package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
)

var storageContractDescriptor = KVUnitDescriptor{
	Name: "contract_unit", Version: 3, Tables: []string{"alpha", "beta"}, HasGlobal: true,
}

func TestStorageBackendsContract(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		runKVBackendContract(t, func() (StorageBackend, func() (StorageBackend, error), error) {
			root := t.TempDir()
			backend, err := NewJSONStorageBackend(root)
			return backend, func() (StorageBackend, error) { return NewJSONStorageBackend(root) }, err
		})
	})
	t.Run("sqlite", func(t *testing.T) {
		runKVBackendContract(t, func() (StorageBackend, func() (StorageBackend, error), error) {
			path := filepath.Join(t.TempDir(), "storage.db")
			backend, err := NewSQLiteStorageBackend(path, SQLiteJournalWAL)
			return backend, func() (StorageBackend, error) {
				return NewSQLiteStorageBackend(path, SQLiteJournalWAL)
			}, err
		})
	})
}

func runKVBackendContract(t *testing.T, create func() (StorageBackend, func() (StorageBackend, error), error)) {
	t.Helper()
	ctx := context.Background()
	t.Run("empty", func(t *testing.T) {
		backend, _, err := create()
		if err != nil {
			t.Fatal(err)
		}
		unit, err := backend.KV().Open(ctx, storageContractDescriptor)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := unit.LoadAll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Tables["alpha"]) != 0 || len(snapshot.Tables["beta"]) != 0 || snapshot.Global != nil {
			t.Fatalf("unexpected empty snapshot: %#v", snapshot)
		}
		if err := backend.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("durable-reopen", func(t *testing.T) {
		backend, reopen, err := create()
		if err != nil {
			t.Fatal(err)
		}
		unit, err := backend.KV().Open(ctx, storageContractDescriptor)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range []struct {
			table, key string
			value      any
		}{{"alpha", "k1", map[string]any{"value": "one"}}, {"alpha", "k2", true}, {"beta", "weird key / with:stuff", []any{"x"}}} {
			if err := unit.PutRecord(ctx, item.table, item.key, item.value); err != nil {
				t.Fatal(err)
			}
		}
		if err := unit.SetGlobal(ctx, map[string]any{"state": "ready"}); err != nil {
			t.Fatal(err)
		}
		if err := backend.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := reopen()
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		unit, err = reopened.KV().Open(ctx, storageContractDescriptor)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := unit.LoadAll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(snapshot.Tables["alpha"], map[string]any{"k1": map[string]any{"value": "one"}, "k2": true}) {
			t.Fatalf("alpha = %#v", snapshot.Tables["alpha"])
		}
		if !reflect.DeepEqual(snapshot.Global, map[string]any{"state": "ready"}) {
			t.Fatalf("global = %#v", snapshot.Global)
		}
	})
	t.Run("overwrite-delete", func(t *testing.T) {
		backend, _, err := create()
		if err != nil {
			t.Fatal(err)
		}
		defer backend.Close()
		unit, err := backend.KV().Open(ctx, storageContractDescriptor)
		if err != nil {
			t.Fatal(err)
		}
		if err := unit.PutRecord(ctx, "alpha", "k", "old"); err != nil {
			t.Fatal(err)
		}
		if err := unit.PutRecord(ctx, "alpha", "k", "new"); err != nil {
			t.Fatal(err)
		}
		if err := unit.DeleteRecord(ctx, "alpha", "k"); err != nil {
			t.Fatal(err)
		}
		if err := unit.DeleteRecord(ctx, "alpha", "k"); err != nil {
			t.Fatal(err)
		}
		snapshot, err := unit.LoadAll(ctx)
		if err != nil || len(snapshot.Tables["alpha"]) != 0 {
			t.Fatalf("snapshot=%#v err=%v", snapshot, err)
		}
	})
	t.Run("version-and-close", func(t *testing.T) {
		backend, reopen, err := create()
		if err != nil {
			t.Fatal(err)
		}
		unit, err := backend.KV().Open(ctx, storageContractDescriptor)
		if err != nil {
			t.Fatal(err)
		}
		if err := unit.PutRecord(ctx, "alpha", "k", "kept"); err != nil {
			t.Fatal(err)
		}
		if err := backend.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := reopen()
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		mismatch := storageContractDescriptor
		mismatch.Version++
		if _, err := reopened.KV().Open(ctx, mismatch); !IsStorageError(err, StorageVersionMismatch) {
			t.Fatalf("version mismatch error = %v", err)
		}
		unit, err = reopened.KV().Open(ctx, storageContractDescriptor)
		if err != nil {
			t.Fatal(err)
		}
		if err := unit.Close(); err != nil {
			t.Fatal(err)
		}
		if err := unit.Close(); err != nil {
			t.Fatal(err)
		}
		if err := unit.PutRecord(ctx, "alpha", "x", true); !IsStorageError(err, StorageClosed) {
			t.Fatalf("closed unit error = %v", err)
		}
	})
}

func TestJSONStorageBackendSpecifics(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := NewJSONStorageBackend(root)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := KVUnitDescriptor{Name: "shape", Version: 1, Tables: []string{"t"}, HasGlobal: true}
	unit, err := backend.Open(ctx, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "shape.json")
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lazy file stat = %v", err)
	}
	if err := unit.PutRecord(ctx, "t", "k", map[string]any{"hello": "world"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(data), "\n") || !strings.Contains(string(data), "\n  \"unit\"") {
		t.Fatalf("file is not pretty printed: %q", data)
	}
	backup := filepath.Join(root, "committed.json")
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := unit.PutRecord(ctx, "t", "k", "rejected"); err == nil {
		t.Fatal("publish unexpectedly succeeded")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backup, path); err != nil {
		t.Fatal(err)
	}
	snapshot, err := unit.LoadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot.Tables["t"]["k"], map[string]any{"hello": "world"}) {
		t.Fatalf("rollback failed: %#v", snapshot)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	backend, err = NewJSONStorageBackend(root)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if _, err := backend.Open(ctx, descriptor); !IsStorageError(err, StorageMalformedMedium) {
		t.Fatalf("malformed error = %v", err)
	}
}

func TestSQLiteStorageBackendSpecifics(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "storage.db")
	backend, err := NewSQLiteStorageBackend(path, SQLiteJournalDelete)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := KVUnitDescriptor{Name: "shape", Version: 1, Tables: []string{"records"}, HasGlobal: true}
	unit, err := backend.Open(ctx, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := unit.PutRecord(ctx, "records", "k", map[string]any{"n": 1}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("database mode = %o", info.Mode().Perm())
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != StorageSQLiteSchemaVersion {
		t.Fatalf("schema version = %d", version)
	}
	if _, err := db.Exec(`UPDATE "u_shape_records" SET value = 'not json' WHERE key = 'k'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	backend, err = NewSQLiteStorageBackend(path, SQLiteJournalDelete)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	unit, err = backend.Open(ctx, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unit.LoadAll(ctx); !IsStorageError(err, StorageMalformedMedium) {
		t.Fatalf("malformed row error = %v", err)
	}
}

type storageNoKVBackend struct{}

func (storageNoKVBackend) KV() KVFacet  { return nil }
func (storageNoKVBackend) Close() error { return nil }

func TestStorageHubRegistry(t *testing.T) {
	hub := NewStorageHub()
	backend := storageNoKVBackend{}
	dispose, err := hub.Backend.Register("none", backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Backend.Register("none", backend); !IsStorageError(err, StorageDuplicateBackend) {
		t.Fatalf("duplicate backend error = %v", err)
	}
	dispose()
	stale := dispose
	dispose, err = hub.Backend.Register("none", backend)
	if err != nil {
		t.Fatal(err)
	}
	stale()
	if _, err := hub.Backend.Get("none"); err != nil {
		t.Fatal(err)
	}
	dispose()
	if _, err := hub.Backend.Get("none"); !IsStorageError(err, StorageBackendNotFound) {
		t.Fatalf("missing backend error = %v", err)
	}
	mount, err := hub.Mount("domain", map[string]bool{"first": true})
	if err != nil {
		t.Fatal(err)
	}
	mount()
	staleMount := mount
	mount, err = hub.Mount("domain", map[string]bool{"second": true})
	if err != nil {
		t.Fatal(err)
	}
	staleMount()
	if _, err := hub.Form("domain"); err != nil {
		t.Fatal(err)
	}
	mount()
}

type storageTestItem struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

type storageTestSettings struct {
	Theme string `json:"theme"`
}

func storageDomainSpec() DomainSpec {
	return DomainSpec{
		Name: "demo", Version: 1,
		Global: &DomainGlobalSpec{
			Parse: JSONDomainParser(func(value storageTestSettings) error {
				if value.Theme == "" {
					return errors.New("theme is required")
				}
				return nil
			}),
			Initial: storageTestSettings{Theme: "plain"},
		},
		Tables: map[string]DomainTableSpec{
			"items": {Parse: JSONDomainParser(func(value storageTestItem) error {
				if value.Label == "" {
					return errors.New("label is required")
				}
				return nil
			})},
		},
	}
}

func TestDomainFacilityRealJSONLifecycle(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := NewJSONStorageBackend(root)
	if err != nil {
		t.Fatal(err)
	}
	hub := NewStorageHub()
	if _, err := hub.Backend.Register("json", backend); err != nil {
		t.Fatal(err)
	}
	facility, err := NewDomainFacility(hub, "json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	defer facility.Close()
	var changesMu sync.Mutex
	var changes []DomainChange
	facility.OnChange(func(change DomainChange) {
		changesMu.Lock()
		changes = append(changes, change)
		changesMu.Unlock()
	})
	domain, err := facility.Open(ctx, storageDomainSpec())
	if err != nil {
		t.Fatal(err)
	}
	if form, err := hub.Form("domain"); err != nil || form != facility {
		t.Fatalf("mounted form=%v err=%v", form, err)
	}
	table, err := domain.Table("items")
	if err != nil {
		t.Fatal(err)
	}
	disposePanic := facility.OnChange(func(DomainChange) { panic("observer failure") })
	if err := table.Put(ctx, "counter", storageTestItem{Label: "counter"}); err != nil {
		t.Fatal(err)
	}
	disposePanic()
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, updateErr := table.Update(ctx, "counter", func(current any) (any, error) {
				item := current.(storageTestItem)
				item.Count++
				return item, nil
			})
			if updateErr != nil {
				t.Errorf("update: %v", updateErr)
			}
		}()
	}
	wg.Wait()
	value, ok, err := table.Get("counter")
	if err != nil || !ok || value.(storageTestItem).Count != 50 {
		t.Fatalf("counter=%#v ok=%v err=%v", value, ok, err)
	}
	global, err := domain.Global()
	if err != nil {
		t.Fatal(err)
	}
	if value, err := global.Get(); err != nil || value != (storageTestSettings{Theme: "plain"}) {
		t.Fatalf("initial global=%#v err=%v", value, err)
	}
	if err := global.Set(ctx, storageTestSettings{Theme: "dark"}); err != nil {
		t.Fatal(err)
	}
	if err := domain.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := table.Get("counter"); !IsDomainError(err, DomainClosed) {
		t.Fatalf("closed read error = %v", err)
	}
	reopened, err := facility.Open(ctx, storageDomainSpec())
	if err != nil {
		t.Fatal(err)
	}
	table, _ = reopened.Table("items")
	value, ok, err = table.Get("counter")
	if err != nil || !ok || value.(storageTestItem).Count != 50 {
		t.Fatalf("reopened counter=%#v ok=%v err=%v", value, ok, err)
	}
	global, _ = reopened.Global()
	if value, err := global.Get(); err != nil || value != (storageTestSettings{Theme: "dark"}) {
		t.Fatalf("reopened global=%#v err=%v", value, err)
	}
	changesMu.Lock()
	if len(changes) != 52 || changes[0].Operation != "put" || changes[len(changes)-1].Table != "" {
		t.Fatalf("changes=%d first=%#v last=%#v", len(changes), changes[0], changes[len(changes)-1])
	}
	changesMu.Unlock()
}

func TestDomainFacilityDurabilityFailureLeavesMemoryUntouched(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := NewJSONStorageBackend(root)
	if err != nil {
		t.Fatal(err)
	}
	hub := NewStorageHub()
	_, _ = hub.Backend.Register("json", backend)
	facility, err := NewDomainFacility(hub, "json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer facility.Close()
	defer backend.Close()
	var changes int
	facility.OnChange(func(DomainChange) { changes++ })
	domain, err := facility.Open(ctx, storageDomainSpec())
	if err != nil {
		t.Fatal(err)
	}
	table, _ := domain.Table("items")
	committed := storageTestItem{Label: "kept", Count: 1}
	if err := table.Put(ctx, "a", committed); err != nil {
		t.Fatal(err)
	}
	seen := changes
	path := filepath.Join(root, "demo.json")
	backup := filepath.Join(root, "demo.committed.json")
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := table.Put(ctx, "a", storageTestItem{Label: "rejected", Count: 99}); err == nil {
		t.Fatal("domain write unexpectedly succeeded")
	}
	value, ok, err := table.Get("a")
	if err != nil || !ok || value != committed || changes != seen {
		t.Fatalf("value=%#v ok=%v err=%v changes=%d", value, ok, err, changes)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backup, path); err != nil {
		t.Fatal(err)
	}
	if err := table.Put(ctx, "b", storageTestItem{Label: "later"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "rejected") {
		t.Fatalf("rejected value leaked to disk: %s", data)
	}
}

func TestDomainFacilityRejectsInvalidStoredRecordAndRoutes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := NewJSONStorageBackend(root)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := DomainDescriptor(storageDomainSpec())
	unit, err := backend.Open(ctx, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := unit.PutRecord(ctx, "items", "bad", map[string]any{"count": "NaN"}); err != nil {
		t.Fatal(err)
	}
	if err := unit.Close(); err != nil {
		t.Fatal(err)
	}
	hub := NewStorageHub()
	_, _ = hub.Backend.Register("json", backend)
	facility, err := NewDomainFacility(hub, "json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer facility.Close()
	defer backend.Close()
	_, err = facility.Open(ctx, storageDomainSpec())
	var domainErr *DomainError
	if !errors.As(err, &domainErr) || domainErr.Code != DomainInvalidRecord || domainErr.Detail == nil || domainErr.Detail.Table != "items" || domainErr.Detail.Key != "bad" {
		t.Fatalf("invalid record error = %#v", err)
	}
	otherHub := NewStorageHub()
	routed, err := NewDomainFacility(otherHub, "missing", map[string]string{"demo": "also_missing"})
	if err != nil {
		t.Fatal(err)
	}
	defer routed.Close()
	if _, err := routed.Open(ctx, storageDomainSpec()); !IsStorageError(err, StorageBackendNotFound) {
		t.Fatalf("route error = %v", err)
	}
	if err := ValidateDomainSpec(DomainSpec{Name: "Bad-Name", Version: 1}); err == nil {
		t.Fatal("invalid domain name accepted")
	}
	nullable := storageDomainSpec()
	nullable.Name = "nullable"
	nullable.Global.Parse = func(value any) (any, error) { return value, nil }
	if err := ValidateDomainSpec(nullable); err == nil {
		t.Fatal("nullable global accepted")
	}
}

func TestDomainFacilityUnsupportedFacet(t *testing.T) {
	hub := NewStorageHub()
	_, _ = hub.Backend.Register("none", storageNoKVBackend{})
	facility, err := NewDomainFacility(hub, "none", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer facility.Close()
	if _, err := facility.Open(context.Background(), storageDomainSpec()); !IsDomainError(err, DomainFacetUnsupported) {
		t.Fatalf("facet error = %v", err)
	}
}

func TestStorageJSONDocumentRoundTrip(t *testing.T) {
	state := jsonUnitState{version: 1, global: map[string]any{"x": true}, tables: map[string]map[string]any{"t": {"k": "v"}}}
	path := filepath.Join(t.TempDir(), "doc.json")
	unit := &jsonKVUnit{descriptor: KVUnitDescriptor{Name: "doc", Version: 1, Tables: []string{"t"}, HasGlobal: true}, path: path, state: state, onClose: func() {}}
	if err := unit.publishLocked(); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadJSONUnit(path, unit.descriptor)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(loaded.tables)
	if string(data) != `{"t":{"k":"v"}}` {
		t.Fatalf("tables = %s", data)
	}
	if _, err := loadJSONUnit(path, KVUnitDescriptor{Name: "doc", Version: 2, Tables: []string{"t"}}); !IsStorageError(err, StorageVersionMismatch) {
		t.Fatalf("version error = %v", err)
	}
	if err := os.WriteFile(path, []byte(fmt.Sprintf(`{"unit":{"name":"doc","version":1},"tables":{"t":[]}}`)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadJSONUnit(path, unit.descriptor); !IsStorageError(err, StorageMalformedMedium) {
		t.Fatalf("table shape error = %v", err)
	}
}
