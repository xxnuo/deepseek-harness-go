package harness

import (
	"errors"
	"fmt"
)

func (e *Engine) initStorageRuntime(config *StorageRuntimeConfig) error {
	if config == nil {
		return nil
	}
	register := func(name string, backend StorageBackend) error {
		dispose, err := e.storage.Backend.Register(name, backend)
		if err != nil {
			_ = backend.Close()
			return err
		}
		e.storageBackends = append(e.storageBackends, backend)
		e.storageDisposers = append(e.storageDisposers, dispose)
		return nil
	}
	if config.JSON != nil {
		backend, err := NewJSONStorageBackend(config.JSON.Root)
		if err != nil {
			return err
		}
		if err := register("json", backend); err != nil {
			return err
		}
	}
	if config.SQLite != nil {
		backend, err := NewSQLiteStorageBackend(config.SQLite.Path, config.SQLite.JournalMode)
		if err != nil {
			return err
		}
		if err := register("sqlite", backend); err != nil {
			return err
		}
	}
	if config.Domain != nil {
		if _, err := e.storage.Backend.Get(config.Domain.Backend); err != nil {
			return fmt.Errorf("storage-domain: %w", err)
		}
		for domain, backend := range config.Domain.Routes {
			if _, err := e.storage.Backend.Get(backend); err != nil {
				return fmt.Errorf("storage-domain route %q: %w", domain, err)
			}
		}
		facility, err := NewDomainFacility(e.storage, config.Domain.Backend, config.Domain.Routes)
		if err != nil {
			return err
		}
		e.storageDomain = facility
		facility.OnChange(func(change DomainChange) {
			payload := map[string]any{
				"domain": change.Domain, "table": change.Table, "key": change.Key, "operation": change.Operation,
			}
			if change.Operation == "put" {
				payload["value"] = change.Value
			}
			e.emitDynamicCordisScopedContained("", "domain/changed", payload)
		})
	}
	return nil
}

func (e *Engine) closeStorageRuntime() error {
	var errs []error
	if e.storageDomain != nil {
		errs = append(errs, e.storageDomain.Close())
	}
	for index := len(e.storageDisposers) - 1; index >= 0; index-- {
		e.storageDisposers[index]()
	}
	for index := len(e.storageBackends) - 1; index >= 0; index-- {
		errs = append(errs, e.storageBackends[index].Close())
	}
	return errors.Join(errs...)
}

// Storage exposes the runtime storage hub to custom Go frontends and apps.
func (e *Engine) Storage() *StorageHub { return e.storage }

// StorageDomain returns the optional mounted domain data form.
func (e *Engine) StorageDomain() *DomainFacility { return e.storageDomain }
