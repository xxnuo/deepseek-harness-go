package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"
)

const sessionProjectionCacheVersion = 6

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
	CreatedAt           int64             `json:"createdAt"`
	CWD                 string            `json:"cwd,omitempty"`
	IsSeeded            *bool             `json:"isSeeded,omitempty"`
	InheritedEventCount *SessionLogOffset `json:"inheritedEventCount,omitempty"`
}

type projectionCacheRecord struct {
	Identity projectionCacheIdentity `json:"identity"`
	Rows     ProjectionCheckpoint    `json:"rows"`
}

func projectionIdentity(header SessionHeader, inheritedEventCount SessionLogOffset) (projectionCacheIdentity, error) {
	if !header.IsSeeded && inheritedEventCount != 0 {
		return projectionCacheIdentity{}, errors.New("unseeded projection-cache identity inherited event count must be 0")
	}
	seeded := header.IsSeeded
	return projectionCacheIdentity{
		CreatedAt: header.CreatedAt, CWD: header.CWD,
		IsSeeded: &seeded, InheritedEventCount: &inheritedEventCount,
	}, nil
}

func sameProjectionIdentity(a, b projectionCacheIdentity) bool {
	seeded := func(value *bool) bool { return value != nil && *value }
	inherited := func(value *SessionLogOffset) SessionLogOffset {
		if value == nil {
			return 0
		}
		return *value
	}
	return a.CreatedAt == b.CreatedAt && a.CWD == b.CWD &&
		seeded(a.IsSeeded) == seeded(b.IsSeeded) && inherited(a.InheritedEventCount) == inherited(b.InheritedEventCount)
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
	path := filepath.Join(abs, "session_projcache")
	sharedProjectionCacheMedia.Lock()
	defer sharedProjectionCacheMedia.Unlock()
	if medium := sharedProjectionCacheMedia.byPath[path]; medium != nil {
		medium.refs++
		return medium, nil
	}
	backend, unit, err := openProjectionCacheStorage(abs)
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
		if decodeErr != nil {
			backupper, ok := unit.(KVRecordBackupper)
			if !ok {
				_ = backend.Close()
				return nil, decodeErr
			}
			moved, backupErr := backupper.BackupRecord(context.Background(), "sessions", id)
			if backupErr != nil {
				_ = backend.Close()
				return nil, backupErr
			}
			log.Printf("deepseek-harness: projection cache record %q failed schema validation; moved to %q and treated as absent: %v", id, moved, decodeErr)
			continue
		}
		records[id] = record
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
		Layout: KVLayoutPerRecord, CompatibleVersions: []int{3, 4, 5},
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
	var raw struct {
		Identity json.RawMessage            `json:"identity"`
		Rows     map[string]json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(data, &raw); err != nil || raw.Identity == nil || raw.Rows == nil {
		return projectionCacheRecord{}, errors.New("invalid projection cache record")
	}
	var identity struct {
		CreatedAt           *int64            `json:"createdAt"`
		CWD                 string            `json:"cwd,omitempty"`
		IsSeeded            *bool             `json:"isSeeded,omitempty"`
		InheritedEventCount *SessionLogOffset `json:"inheritedEventCount,omitempty"`
	}
	if err := json.Unmarshal(raw.Identity, &identity); err != nil || identity.CreatedAt == nil || *identity.CreatedAt < 0 || identity.InheritedEventCount != nil && *identity.InheritedEventCount < 0 {
		return projectionCacheRecord{}, errors.New("invalid projection cache identity")
	}
	record := projectionCacheRecord{
		Identity: projectionCacheIdentity{
			CreatedAt: *identity.CreatedAt, CWD: identity.CWD,
			IsSeeded: identity.IsSeeded, InheritedEventCount: identity.InheritedEventCount,
		},
		Rows: make(ProjectionCheckpoint, len(raw.Rows)),
	}
	for key, rawRow := range raw.Rows {
		var row struct {
			Version *int            `json:"ver"`
			Seq     *int            `json:"seq"`
			Value   json.RawMessage `json:"val"`
		}
		if err := json.Unmarshal(rawRow, &row); err != nil || row.Version == nil || *row.Version < 0 || row.Seq == nil || *row.Seq < -1 || row.Value == nil {
			return projectionCacheRecord{}, fmt.Errorf("invalid projection cache row %q", key)
		}
		var value any
		if err := json.Unmarshal(row.Value, &value); err != nil {
			return projectionCacheRecord{}, fmt.Errorf("invalid projection cache row %q: %w", key, err)
		}
		record.Rows[key] = ProjectionCheckpointRow{Version: *row.Version, Seq: *row.Seq, Value: value}
	}
	return record, nil
}

func decodeProjectionCacheRecordJSON(data []byte) (projectionCacheRecord, error) {
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return projectionCacheRecord{}, err
	}
	return decodeProjectionCacheRecord(value)
}

func cloneProjectionRecord(record projectionCacheRecord) (projectionCacheRecord, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return projectionCacheRecord{}, err
	}
	return decodeProjectionCacheRecordJSON(data)
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
		if sameProjectionIdentity(current.Identity, detached.Identity) && projectionCheckpointCut(current.Rows) > projectionCheckpointCut(detached.Rows) {
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

func projectionCheckpointCut(rows ProjectionCheckpoint) int {
	cut := -1
	for _, row := range rows {
		if row.Seq > cut {
			cut = row.Seq
		}
	}
	return cut
}

func (medium *projectionCacheMedium) record(header SessionHeader, inheritedEventCount SessionLogOffset) (projectionCacheRecord, bool) {
	expected, err := projectionIdentity(header, inheritedEventCount)
	if err != nil {
		return projectionCacheRecord{}, false
	}
	medium.mu.RLock()
	record, ok := medium.records[header.ID]
	medium.mu.RUnlock()
	if !ok || !sameProjectionIdentity(record.Identity, expected) {
		return projectionCacheRecord{}, false
	}
	detached, err := cloneProjectionRecord(record)
	if err != nil {
		return projectionCacheRecord{}, false
	}
	return detached, true
}

func (medium *projectionCacheMedium) snapshot(registry *SessionProjectionRegistry, header SessionHeader, inheritedEventCount SessionLogOffset, keys ...string) (ProjectionSnapshot, bool) {
	record, ok := medium.record(header, inheritedEventCount)
	if !ok {
		return ProjectionSnapshot{}, false
	}
	values, err := registry.ViewCheckpoint(record.Rows, keys...)
	if err != nil || len(values) == 0 {
		return ProjectionSnapshot{}, false
	}
	asOfSeq := -1
	first := true
	for key := range values {
		row, ok := record.Rows[key]
		if !ok {
			return ProjectionSnapshot{}, false
		}
		if first || row.Seq < asOfSeq {
			asOfSeq = row.Seq
			first = false
		}
	}
	return ProjectionSnapshot{AsOfSeq: asOfSeq, Values: values}, true
}

func (medium *projectionCacheMedium) identitySnapshot(registry *SessionProjectionRegistry, header SessionHeader, inheritedEventCount SessionLogOffset) (ProjectionSnapshot, bool) {
	return medium.snapshot(registry, header, inheritedEventCount, "subagent")
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

func (cache *sessionProjectionCache) created(session *Session) {
	cache.mu.Lock()
	if !cache.closed {
		cache.startWriteLocked(session, "create")
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
	rows, err := cache.registry.Checkpoint(session)
	if err != nil {
		return err
	}
	session.mu.Lock()
	header := session.Header
	inheritedEventCount := session.InheritedEventCount
	id := session.Header.ID
	session.mu.Unlock()
	identity, err := projectionIdentity(header, inheritedEventCount)
	if err != nil {
		return err
	}
	session.mu.Lock()
	handle := session.store
	session.mu.Unlock()
	if handle != nil {
		if err := handle.Flush(ctx); err != nil {
			return err
		}
	}
	return cache.medium.put(ctx, id, projectionCacheRecord{Identity: identity, Rows: rows})
}

func (cache *sessionProjectionCache) cachedSnapshot(header SessionHeader, inheritedEventCount SessionLogOffset, keys ...string) (ProjectionSnapshot, bool) {
	return cache.medium.snapshot(cache.registry, header, inheritedEventCount, keys...)
}

func (cache *sessionProjectionCache) hydratePrepared(session *Session, events []Event) (ProjectionSnapshot, error) {
	session.mu.Lock()
	header := session.Header
	inheritedEventCount := session.InheritedEventCount
	session.mu.Unlock()
	checkpoint := ProjectionCheckpoint{}
	if record, ok := cache.medium.record(header, inheritedEventCount); ok {
		checkpoint = record.Rows
	}
	snapshot, err := cache.registry.Hydrate(session, checkpoint, events, 0)
	if err == nil || len(checkpoint) == 0 {
		return snapshot, err
	}
	return cache.registry.Hydrate(session, ProjectionCheckpoint{}, events, 0)
}

func (cache *sessionProjectionCache) coldSnapshot(ctx context.Context, header SessionHeader, inheritedEventCount SessionLogOffset, events []Event) (ProjectionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ProjectionSnapshot{}, err
	}
	checkpoint := ProjectionCheckpoint{}
	if record, ok := cache.medium.record(header, inheritedEventCount); ok {
		checkpoint = record.Rows
	}
	restored, err := cache.registry.Restore(checkpoint, events, 0, header, inheritedEventCount)
	if err != nil && len(checkpoint) > 0 {
		restored, err = cache.registry.Restore(ProjectionCheckpoint{}, events, 0, header, inheritedEventCount)
	}
	if err != nil {
		return ProjectionSnapshot{}, err
	}
	identity, identityErr := projectionIdentity(header, inheritedEventCount)
	if identityErr == nil {
		if putErr := cache.medium.put(ctx, header.ID, projectionCacheRecord{Identity: identity, Rows: restored.Checkpoint}); putErr != nil {
			log.Printf("deepseek-harness: session projection cache cold-read write-back for %q failed: %v", header.ID, putErr)
		}
	}
	return restored.Snapshot, nil
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
	snapshot, err := e.sessionProjections.Snapshot(session)
	if err != nil {
		return ProjectionSnapshot{}, err
	}
	if e.projectionCache != nil {
		if err := e.projectionCache.writeSession(ctx, session); err != nil {
			log.Printf("deepseek-harness: session projection cache cold-read write-back for %q failed: %v", id, err)
		}
	}
	return snapshot, nil
}
