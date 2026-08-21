package harness

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
)

type StorageErrorCode string

const (
	StorageBackendNotFound  StorageErrorCode = "backend-not-found"
	StorageFormNotMounted   StorageErrorCode = "form-not-mounted"
	StorageDuplicateBackend StorageErrorCode = "duplicate-backend"
	StorageDuplicateMount   StorageErrorCode = "duplicate-mount"
	StorageVersionMismatch  StorageErrorCode = "version-mismatch"
	StorageMalformedMedium  StorageErrorCode = "malformed-medium"
	StorageClosed           StorageErrorCode = "closed"
)

type StorageError struct {
	Code StorageErrorCode
	Msg  string
	Err  error
}

func (e *StorageError) Error() string { return e.Msg }
func (e *StorageError) Unwrap() error { return e.Err }

func storageError(code StorageErrorCode, format string, args ...any) error {
	return &StorageError{Code: code, Msg: fmt.Sprintf(format, args...)}
}

func storageErrorWrap(code StorageErrorCode, err error, format string, args ...any) error {
	return &StorageError{Code: code, Msg: fmt.Sprintf(format, args...), Err: err}
}

func IsStorageError(err error, code StorageErrorCode) bool {
	var target *StorageError
	return errors.As(err, &target) && target.Code == code
}

var storageUnitNameRE = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func ValidStorageUnitName(name string) bool { return storageUnitNameRE.MatchString(name) }

type KVUnitDescriptor struct {
	Name      string
	Version   int
	Tables    []string
	HasGlobal bool
}

type KVSnapshot struct {
	Tables map[string]map[string]any
	Global any
}

type KVUnit interface {
	LoadAll(context.Context) (KVSnapshot, error)
	PutRecord(context.Context, string, string, any) error
	DeleteRecord(context.Context, string, string) error
	SetGlobal(context.Context, any) error
	Close() error
}

type KVFacet interface {
	Open(context.Context, KVUnitDescriptor) (KVUnit, error)
}

type StorageBackend interface {
	KV() KVFacet
	Close() error
}

type JSONStorageConfig struct {
	Root string
}

type SQLiteStorageConfig struct {
	Path        string
	JournalMode SQLiteJournalMode
}

type StorageDomainConfig struct {
	Backend string
	Routes  map[string]string
}

type StorageRuntimeConfig struct {
	JSON   *JSONStorageConfig
	SQLite *SQLiteStorageConfig
	Domain *StorageDomainConfig
}

func cloneStorageRuntimeConfig(config *StorageRuntimeConfig) *StorageRuntimeConfig {
	if config == nil {
		return nil
	}
	cloned := *config
	if config.JSON != nil {
		value := *config.JSON
		cloned.JSON = &value
	}
	if config.SQLite != nil {
		value := *config.SQLite
		cloned.SQLite = &value
	}
	if config.Domain != nil {
		value := *config.Domain
		value.Routes = make(map[string]string, len(config.Domain.Routes))
		for domain, backend := range config.Domain.Routes {
			value.Routes[domain] = backend
		}
		cloned.Domain = &value
	}
	return &cloned
}

type BackendRegistry struct {
	mu       sync.RWMutex
	backends map[string]registeredBackend
}

type registeredBackend struct {
	backend StorageBackend
	token   *byte
}

func NewBackendRegistry() *BackendRegistry {
	return &BackendRegistry{backends: map[string]registeredBackend{}}
}

func (r *BackendRegistry) Register(name string, backend StorageBackend) (func(), error) {
	if backend == nil {
		return nil, errors.New("storage backend is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.backends[name]; ok {
		return nil, storageError(StorageDuplicateBackend, "storage backend %q is already registered", name)
	}
	entry := registeredBackend{backend: backend, token: new(byte)}
	r.backends[name] = entry
	return func() {
		r.mu.Lock()
		if current, ok := r.backends[name]; ok && current.token == entry.token {
			delete(r.backends, name)
		}
		r.mu.Unlock()
	}, nil
}

func (r *BackendRegistry) Get(name string) (StorageBackend, error) {
	r.mu.RLock()
	entry, ok := r.backends[name]
	names := make([]string, 0, len(r.backends))
	for registered := range r.backends {
		names = append(names, registered)
	}
	r.mu.RUnlock()
	if ok {
		return entry.backend, nil
	}
	sort.Strings(names)
	registered := "none"
	if len(names) > 0 {
		registered = strings.Join(names, ", ")
	}
	return nil, storageError(StorageBackendNotFound, "storage backend %q is not registered (registered: %s)", name, registered)
}

func (r *BackendRegistry) Names() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.backends))
	for name := range r.backends {
		names = append(names, name)
	}
	r.mu.RUnlock()
	sort.Strings(names)
	return names
}

type StorageHub struct {
	Backend *BackendRegistry

	mu    sync.RWMutex
	forms map[string]storageForm
}

type storageForm struct {
	value any
	token *byte
}

func NewStorageHub() *StorageHub {
	return &StorageHub{Backend: NewBackendRegistry(), forms: map[string]storageForm{}}
}

func (h *StorageHub) Mount(name string, facility any) (func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.forms[name]; ok {
		return nil, storageError(StorageDuplicateMount, "storage form %q is already mounted", name)
	}
	entry := storageForm{value: facility, token: new(byte)}
	h.forms[name] = entry
	return func() {
		h.mu.Lock()
		if current, ok := h.forms[name]; ok && current.token == entry.token {
			delete(h.forms, name)
		}
		h.mu.Unlock()
	}, nil
}

func (h *StorageHub) Form(name string) (any, error) {
	h.mu.RLock()
	form, ok := h.forms[name]
	h.mu.RUnlock()
	if !ok {
		return nil, storageError(StorageFormNotMounted, "storage form %q is not mounted", name)
	}
	return form.value, nil
}
