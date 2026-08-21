package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	sessionProjectionCacheVersion = 3
	projectionValuesVersion       = 3
)

type SessionProjectionCacheConfig struct {
	WriteEveryEvents int
	WriteInterval    time.Duration
}

type ProjectionSnapshot struct {
	AsOfSeq int            `json:"asOfSeq"`
	Values  map[string]any `json:"values"`
}

func WithSessionProjectionCache(config SessionProjectionCacheConfig) Option {
	return func(c *Config) { c.SessionProjectionCache = &config }
}

func validateSessionProjectionCacheConfig(config *SessionProjectionCacheConfig) error {
	if config == nil {
		return nil
	}
	if config.WriteEveryEvents < 1 {
		return errors.New("session-projection-cache: writeEveryEvents must be a positive integer")
	}
	if config.WriteInterval <= 0 {
		return errors.New("session-projection-cache: writeIntervalMs must be positive")
	}
	return nil
}

type projectionCacheIdentity struct {
	CreatedAt int64  `json:"createdAt"`
	CWD       string `json:"cwd,omitempty"`
}

type projectionCacheRecord struct {
	Identity    projectionCacheIdentity `json:"identity"`
	Seq         int                     `json:"seq"`
	Version     int                     `json:"version,omitempty"`
	Composition string                  `json:"composition"`
	Values      map[string]any          `json:"values"`
}

func projectionIdentity(header SessionHeader) projectionCacheIdentity {
	return projectionCacheIdentity{CreatedAt: header.CreatedAt, CWD: header.CWD}
}

func sameProjectionIdentity(a, b projectionCacheIdentity) bool {
	return a.CreatedAt == b.CreatedAt && a.CWD == b.CWD
}

type projectionCacheMedium struct {
	path    string
	backend *JSONStorageBackend
	unit    KVUnit

	mu      sync.RWMutex
	writeMu sync.Mutex
	records map[string]projectionCacheRecord
	refs    int
}

var sharedProjectionCacheMedia = struct {
	sync.Mutex
	byPath map[string]*projectionCacheMedium
}{byPath: map[string]*projectionCacheMedium{}}

func acquireProjectionCacheMedium(root string) (*projectionCacheMedium, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(abs, "session_projcache.json")
	sharedProjectionCacheMedia.Lock()
	defer sharedProjectionCacheMedia.Unlock()
	if medium := sharedProjectionCacheMedia.byPath[path]; medium != nil {
		medium.refs++
		return medium, nil
	}
	backend, unit, err := openProjectionCacheStorage(abs)
	if IsStorageError(err, StorageVersionMismatch) {
		_ = backend.Close()
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return nil, removeErr
		}
		backend, unit, err = openProjectionCacheStorage(abs)
	}
	if err != nil {
		if backend != nil {
			_ = backend.Close()
		}
		return nil, err
	}
	snapshot, err := unit.LoadAll(context.Background())
	if err != nil {
		_ = backend.Close()
		return nil, err
	}
	records := make(map[string]projectionCacheRecord, len(snapshot.Tables["sessions"]))
	for id, raw := range snapshot.Tables["sessions"] {
		record, decodeErr := decodeProjectionCacheRecord(raw)
		if decodeErr == nil {
			records[id] = record
		}
	}
	medium := &projectionCacheMedium{path: path, backend: backend, unit: unit, records: records, refs: 1}
	sharedProjectionCacheMedia.byPath[path] = medium
	return medium, nil
}

func openProjectionCacheStorage(root string) (*JSONStorageBackend, KVUnit, error) {
	backend, err := NewJSONStorageBackend(root)
	if err != nil {
		return nil, nil, err
	}
	unit, err := backend.Open(context.Background(), KVUnitDescriptor{
		Name: "session_projcache", Version: sessionProjectionCacheVersion, Tables: []string{"sessions"},
	})
	return backend, unit, err
}

func releaseProjectionCacheMedium(medium *projectionCacheMedium) error {
	sharedProjectionCacheMedia.Lock()
	defer sharedProjectionCacheMedia.Unlock()
	medium.refs--
	if medium.refs > 0 {
		return nil
	}
	delete(sharedProjectionCacheMedia.byPath, medium.path)
	return medium.backend.Close()
}

func decodeProjectionCacheRecord(value any) (projectionCacheRecord, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return projectionCacheRecord{}, err
	}
	var record projectionCacheRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return projectionCacheRecord{}, err
	}
	if record.Identity.CreatedAt < 0 || record.Seq < -1 || record.Version != projectionValuesVersion || record.Composition == "" || record.Values == nil {
		return projectionCacheRecord{}, errors.New("invalid projection cache record")
	}
	return record, nil
}

func cloneProjectionRecord(record projectionCacheRecord) (projectionCacheRecord, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return projectionCacheRecord{}, err
	}
	var detached projectionCacheRecord
	if err := json.Unmarshal(data, &detached); err != nil {
		return projectionCacheRecord{}, err
	}
	return detached, nil
}

func (medium *projectionCacheMedium) put(ctx context.Context, id string, record projectionCacheRecord) error {
	detached, err := cloneProjectionRecord(record)
	if err != nil {
		return fmt.Errorf("projection snapshot is not JSON-serializable: %w", err)
	}
	medium.writeMu.Lock()
	defer medium.writeMu.Unlock()
	medium.mu.RLock()
	current, exists := medium.records[id]
	medium.mu.RUnlock()
	if exists {
		if sameProjectionIdentity(current.Identity, detached.Identity) && current.Seq > detached.Seq {
			return nil
		}
		if !sameProjectionIdentity(current.Identity, detached.Identity) && current.Identity.CreatedAt > detached.Identity.CreatedAt {
			return nil
		}
	}
	if err := medium.unit.PutRecord(ctx, "sessions", id, detached); err != nil {
		return err
	}
	medium.mu.Lock()
	medium.records[id] = detached
	medium.mu.Unlock()
	return nil
}

func (medium *projectionCacheMedium) snapshot(header SessionHeader, lastSeq int, exact bool, composition string) (ProjectionSnapshot, bool) {
	medium.mu.RLock()
	record, ok := medium.records[header.ID]
	medium.mu.RUnlock()
	if !ok || record.Composition != composition || !sameProjectionIdentity(record.Identity, projectionIdentity(header)) || record.Seq > lastSeq || exact && record.Seq != lastSeq {
		return ProjectionSnapshot{}, false
	}
	detached, err := cloneProjectionRecord(record)
	if err != nil {
		return ProjectionSnapshot{}, false
	}
	return ProjectionSnapshot{AsOfSeq: detached.Seq, Values: detached.Values}, true
}

type projectionCacheDirty struct {
	pending int
	timer   *time.Timer
}

type sessionProjectionCache struct {
	config   SessionProjectionCacheConfig
	medium   *projectionCacheMedium
	registry *SessionProjectionRegistry

	mu     sync.Mutex
	dirty  map[*Session]*projectionCacheDirty
	closed bool
	writes sync.WaitGroup
}

func newSessionProjectionCache(root string, config SessionProjectionCacheConfig, registry *SessionProjectionRegistry) (*sessionProjectionCache, error) {
	medium, err := acquireProjectionCacheMedium(root)
	if err != nil {
		return nil, err
	}
	return &sessionProjectionCache{config: config, medium: medium, registry: registry, dirty: map[*Session]*projectionCacheDirty{}}, nil
}

func (cache *sessionProjectionCache) observe(session *Session, event Event) {
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return
	}
	state := cache.dirty[session]
	if state == nil {
		state = &projectionCacheDirty{}
		cache.dirty[session] = state
	}
	if event.Type == "turn/end" {
		cache.markCleanLocked(state)
		cache.startWriteLocked(session, "turn/end")
		cache.mu.Unlock()
		return
	}
	state.pending++
	if state.pending >= cache.config.WriteEveryEvents {
		cache.markCleanLocked(state)
		cache.startWriteLocked(session, "count threshold")
		cache.mu.Unlock()
		return
	}
	if state.timer == nil {
		var timer *time.Timer
		timer = time.AfterFunc(cache.config.WriteInterval, func() { cache.flushTimer(session, timer) })
		state.timer = timer
	}
	cache.mu.Unlock()
}

func (cache *sessionProjectionCache) flushTimer(session *Session, timer *time.Timer) {
	cache.mu.Lock()
	state := cache.dirty[session]
	if cache.closed || state == nil || state.timer != timer {
		cache.mu.Unlock()
		return
	}
	state.timer = nil
	state.pending = 0
	cache.startWriteLocked(session, "interval")
	cache.mu.Unlock()
}

func (cache *sessionProjectionCache) startWriteLocked(session *Session, trigger string) {
	cache.writes.Add(1)
	go func() {
		defer cache.writes.Done()
		if err := cache.writeSession(context.Background(), session); err != nil {
			log.Printf("deepseek-harness: session projection cache %s write for %q failed: %v", trigger, session.Header.ID, err)
		}
	}()
}

func (cache *sessionProjectionCache) markCleanLocked(state *projectionCacheDirty) {
	state.pending = 0
	if state.timer != nil {
		state.timer.Stop()
		state.timer = nil
	}
}

func (cache *sessionProjectionCache) writeSession(ctx context.Context, session *Session) error {
	snapshot, composition, err := cache.registry.snapshotForCache(session)
	if err != nil {
		return err
	}
	session.mu.Lock()
	record := projectionCacheRecord{
		Identity: projectionIdentity(session.Header), Seq: snapshot.AsOfSeq,
		Version: projectionValuesVersion, Composition: composition, Values: snapshot.Values,
	}
	id := session.Header.ID
	session.mu.Unlock()
	return cache.medium.put(ctx, id, record)
}

func (cache *sessionProjectionCache) putSnapshot(ctx context.Context, header SessionHeader, snapshot ProjectionSnapshot, composition string) error {
	return cache.medium.put(ctx, header.ID, projectionCacheRecord{
		Identity: projectionIdentity(header), Seq: snapshot.AsOfSeq, Version: projectionValuesVersion,
		Composition: composition, Values: snapshot.Values,
	})
}

func (cache *sessionProjectionCache) close(sessions []*Session) error {
	cache.mu.Lock()
	cache.closed = true
	for _, state := range cache.dirty {
		cache.markCleanLocked(state)
	}
	cache.dirty = nil
	cache.mu.Unlock()
	cache.writes.Wait()
	for _, session := range sessions {
		if err := cache.writeSession(context.Background(), session); err != nil {
			log.Printf("deepseek-harness: session projection cache close write for %q failed: %v", session.Header.ID, err)
		}
	}
	return releaseProjectionCacheMedium(cache.medium)
}

func (e *Engine) SessionProjectionSnapshot(ctx context.Context, id string) (ProjectionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ProjectionSnapshot{}, err
	}
	session, err := e.getSession(id)
	if err != nil {
		return ProjectionSnapshot{}, err
	}
	session.mu.Lock()
	header := session.Header
	lastSeq := len(session.Events) - 1
	if e.projectionCache != nil {
		if cached, ok := e.projectionCache.medium.snapshot(header, lastSeq, true, e.sessionProjections.Signature()); ok {
			session.mu.Unlock()
			return cached, nil
		}
	}
	session.mu.Unlock()
	snapshot, composition, err := e.sessionProjections.snapshotForCache(session)
	if err != nil {
		return ProjectionSnapshot{}, err
	}
	if e.projectionCache != nil {
		if err := e.projectionCache.putSnapshot(ctx, header, snapshot, composition); err != nil {
			log.Printf("deepseek-harness: session projection cache cold-read write-back for %q failed: %v", id, err)
		}
	}
	return snapshot, nil
}
