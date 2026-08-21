package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type sqliteSessionCoordinator struct {
	store     *SQLiteSessionStore
	cacheSize int
	delay     time.Duration

	mu       sync.Mutex
	queues   map[string]*sqliteWriteQueue
	cache    map[string]sqlitePreparedInspection
	cacheLRU []string
	closing  bool
	closed   bool
	done     chan struct{}
	closeErr error
}

type sqliteWriteQueue struct {
	mu          sync.Mutex
	flushMu     sync.Mutex
	pending     []Event
	timer       *time.Timer
	generation  uint64
	paused      bool
	initialized bool
	nextSeq     int
}

type sqlitePreparedInspection struct {
	revision   SessionPersistenceRevision
	inspection SessionInspection
}

func newSQLiteSessionCoordinator(store *SQLiteSessionStore, options SQLiteSessionStoreOptions) *sqliteSessionCoordinator {
	return &sqliteSessionCoordinator{
		store: store, cacheSize: options.PreparedSessionCacheSize,
		delay: options.WriteBatchMaxDelay, queues: map[string]*sqliteWriteQueue{},
		cache: map[string]sqlitePreparedInspection{}, done: make(chan struct{}),
	}
}

func (c *sqliteSessionCoordinator) append(ctx context.Context, id string, events []Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	batch, err := snapshotSQLiteEventBatch(events)
	if err != nil || len(batch) == 0 {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed {
		return storageError(StorageClosed, "SQLite session store is closed")
	}
	queue := c.queues[id]
	if queue == nil {
		queue = &sqliteWriteQueue{}
		c.queues[id] = queue
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if !queue.initialized {
		queue.nextSeq, err = c.store.appendCursor(ctx, id)
		if err != nil {
			return err
		}
		queue.initialized = true
	}
	if err := validateSessionEventBatch(batch, queue.nextSeq); err != nil {
		return err
	}
	queue.nextSeq += len(batch)
	wasEmpty := len(queue.pending) == 0
	queue.pending = append(queue.pending, batch...)
	c.invalidateLocked(id)
	if queue.paused {
		queue.paused = false
		c.armLocked(id, queue)
	} else if wasEmpty {
		c.armLocked(id, queue)
	}
	return nil
}

func (c *sqliteSessionCoordinator) armLocked(id string, queue *sqliteWriteQueue) {
	queue.generation++
	generation := queue.generation
	queue.timer = time.AfterFunc(c.delay, func() { c.backgroundFlush(id, queue, generation) })
}

func (c *sqliteSessionCoordinator) backgroundFlush(id string, queue *sqliteWriteQueue, generation uint64) {
	queue.mu.Lock()
	if generation != queue.generation {
		queue.mu.Unlock()
		return
	}
	queue.timer = nil
	queue.mu.Unlock()

	queue.flushMu.Lock()
	defer queue.flushMu.Unlock()
	queue.mu.Lock()
	if generation != queue.generation || queue.paused || len(queue.pending) == 0 {
		queue.mu.Unlock()
		return
	}
	batch := append([]Event(nil), queue.pending...)
	queue.pending = nil
	queue.mu.Unlock()

	err := c.store.appendImmediate(context.Background(), id, batch)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if err != nil {
		queue.pending = append(batch, queue.pending...)
		queue.paused = true
		c.stopTimerLocked(queue)
	}
}

func (c *sqliteSessionCoordinator) flush(ctx context.Context, id string) error {
	c.mu.Lock()
	if c.closing || c.closed {
		c.mu.Unlock()
		return storageError(StorageClosed, "SQLite session store is closed")
	}
	queue := c.queues[id]
	c.mu.Unlock()
	if queue == nil {
		return nil
	}
	return c.flushQueue(ctx, id, queue)
}

func (c *sqliteSessionCoordinator) flushQueue(ctx context.Context, id string, queue *sqliteWriteQueue) error {
	queue.flushMu.Lock()
	defer queue.flushMu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		queue.mu.Lock()
		c.stopTimerLocked(queue)
		queue.paused = false
		if len(queue.pending) == 0 {
			queue.mu.Unlock()
			return nil
		}
		batch := append([]Event(nil), queue.pending...)
		queue.pending = nil
		queue.mu.Unlock()
		if err := c.store.appendImmediate(ctx, id, batch); err != nil {
			queue.mu.Lock()
			queue.pending = append(batch, queue.pending...)
			queue.paused = true
			queue.mu.Unlock()
			return err
		}
	}
}

func (c *sqliteSessionCoordinator) stopTimerLocked(queue *sqliteWriteQueue) {
	queue.generation++
	if queue.timer != nil {
		queue.timer.Stop()
		queue.timer = nil
	}
}

func (c *sqliteSessionCoordinator) flushAll(ctx context.Context) error {
	c.mu.Lock()
	if c.closing || c.closed {
		c.mu.Unlock()
		return storageError(StorageClosed, "SQLite session store is closed")
	}
	queues := make(map[string]*sqliteWriteQueue, len(c.queues))
	for id, queue := range c.queues {
		queues[id] = queue
	}
	c.mu.Unlock()
	var errs []error
	for id, queue := range queues {
		if err := c.flushQueue(ctx, id, queue); err != nil {
			errs = append(errs, fmt.Errorf("session %q: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

func (c *sqliteSessionCoordinator) load(ctx context.Context, id string) (SessionInspection, error) {
	if err := c.flush(ctx, id); err != nil {
		return SessionInspection{}, err
	}
	c.invalidate(id)
	inspection, err := c.store.loadImmediate(ctx, id)
	c.invalidate(id)
	return inspection, err
}

func (c *sqliteSessionCoordinator) inspect(ctx context.Context, id string) (SessionInspection, error) {
	if err := c.flush(ctx, id); err != nil {
		return SessionInspection{}, err
	}
	revision, found, err := c.store.revision(ctx, id)
	if err != nil {
		return SessionInspection{}, err
	}
	if found {
		if inspection, ok, err := c.cached(id, revision); ok || err != nil {
			return inspection, err
		}
	}
	inspection, err := c.store.inspectImmediate(ctx, id)
	if err != nil {
		return SessionInspection{}, err
	}
	after, stillFound, err := c.store.revision(ctx, id)
	if err != nil {
		return SessionInspection{}, err
	}
	if found && stillFound && after == revision {
		if err := c.remember(id, revision, inspection); err != nil {
			return SessionInspection{}, err
		}
	}
	return inspection, nil
}

func (c *sqliteSessionCoordinator) cached(id string, revision SessionPersistenceRevision) (SessionInspection, bool, error) {
	c.mu.Lock()
	entry, ok := c.cache[id]
	if ok && entry.revision != revision {
		c.invalidateLocked(id)
		ok = false
	}
	if ok {
		c.touchLocked(id)
	}
	c.mu.Unlock()
	if !ok {
		return SessionInspection{}, false, nil
	}
	inspection, err := cloneSQLiteInspection(entry.inspection)
	return inspection, true, err
}

func (c *sqliteSessionCoordinator) remember(id string, revision SessionPersistenceRevision, inspection SessionInspection) error {
	copy, err := cloneSQLiteInspection(inspection)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[id] = sqlitePreparedInspection{revision: revision, inspection: copy}
	c.touchLocked(id)
	for len(c.cacheLRU) > c.cacheSize {
		oldest := c.cacheLRU[0]
		c.cacheLRU = c.cacheLRU[1:]
		delete(c.cache, oldest)
	}
	return nil
}

func (c *sqliteSessionCoordinator) invalidate(id string) {
	c.mu.Lock()
	c.invalidateLocked(id)
	c.mu.Unlock()
}

func (c *sqliteSessionCoordinator) drop(id string) {
	c.mu.Lock()
	if queue := c.queues[id]; queue != nil {
		queue.mu.Lock()
		c.stopTimerLocked(queue)
		queue.pending = nil
		queue.mu.Unlock()
		delete(c.queues, id)
	}
	c.invalidateLocked(id)
	c.mu.Unlock()
}

func (c *sqliteSessionCoordinator) invalidateLocked(id string) {
	delete(c.cache, id)
	for index, candidate := range c.cacheLRU {
		if candidate == id {
			c.cacheLRU = append(c.cacheLRU[:index], c.cacheLRU[index+1:]...)
			return
		}
	}
}

func (c *sqliteSessionCoordinator) touchLocked(id string) {
	for index, candidate := range c.cacheLRU {
		if candidate == id {
			c.cacheLRU = append(c.cacheLRU[:index], c.cacheLRU[index+1:]...)
			break
		}
	}
	c.cacheLRU = append(c.cacheLRU, id)
}

func (c *sqliteSessionCoordinator) close() error {
	c.mu.Lock()
	if c.closing || c.closed {
		done := c.done
		c.mu.Unlock()
		<-done
		return c.closeErr
	}
	c.closing = true
	queues := make(map[string]*sqliteWriteQueue, len(c.queues))
	for id, queue := range c.queues {
		queues[id] = queue
	}
	c.mu.Unlock()
	var errs []error
	for id, queue := range queues {
		if err := c.flushQueue(context.Background(), id, queue); err != nil {
			errs = append(errs, fmt.Errorf("session %q: %w", id, err))
		}
	}
	err := errors.Join(errs...)
	c.mu.Lock()
	c.closeErr = err
	c.closed = true
	close(c.done)
	c.mu.Unlock()
	return err
}

func (s *SQLiteSessionStore) appendCursor(ctx context.Context, id string) (int, error) {
	end, err := s.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer end()
	s.mu.Lock()
	state, live := s.states[id]
	s.mu.Unlock()
	if live {
		return state.cursor, nil
	}
	prefix, found, err := s.readPrefix(ctx, id, 0)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("session %q is not registered in persistence", id)
	}
	return len(prefix.events) + len(interruptedSessionClosers(prefix.events)), nil
}

func (s *SQLiteSessionStore) revision(ctx context.Context, id string) (SessionPersistenceRevision, bool, error) {
	end, err := s.begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer end()
	var incarnation string
	var revision int64
	err = s.db.QueryRowContext(ctx, "SELECT incarnation, revision FROM sessions WHERE id = ?", id).Scan(&incarnation, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return SessionPersistenceRevision(fmt.Sprintf("%s:incarnation:%s:revision:%d", s.identity, incarnation, revision)), true, nil
}

func snapshotSQLiteEventBatch(events []Event) ([]Event, error) {
	batch := make([]Event, 0, len(events))
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("session event %q is not JSON-serializable: %w", event.Type, err)
		}
		decoded, err := decodeSessionStorageRecord(encoded)
		if err != nil || len(decoded) != 1 {
			return nil, fmt.Errorf("invalid session event %q", event.Type)
		}
		batch = append(batch, decoded[0])
	}
	return batch, nil
}

func cloneSQLiteInspection(inspection SessionInspection) (SessionInspection, error) {
	events, err := snapshotSQLiteEventBatch(inspection.Events)
	if err != nil {
		return SessionInspection{}, err
	}
	return SessionInspection{Meta: inspection.Meta, Events: events}, nil
}
