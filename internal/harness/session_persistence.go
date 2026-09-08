package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unicode/utf16"
)

type SessionPersistenceRevision string

type SessionPersistenceSnapshot struct {
	Header     SessionHeader
	Revision   SessionPersistenceRevision
	EventCount *int
	SizeBytes  *int64
}

type SessionInspection struct {
	Meta                SessionHeader
	InheritedEventCount SessionLogOffset
	Events              []Event
}

type SessionAccess string

const (
	SessionAccessRead  SessionAccess = "read"
	SessionAccessWrite SessionAccess = "write"
)

type SessionHandle interface {
	ID() string
	Header() SessionHeader
	InheritedEventCount() SessionLogOffset
	Access() SessionAccess
	Read(context.Context, ...SessionLogOffset) ([]Event, error)
	Append(context.Context, []Event) error
	Flush(context.Context) error
	Close() error
}

type SessionStore interface {
	Create(context.Context, SessionHeader, SessionLogOffset) (SessionHandle, error)
	Open(context.Context, string, SessionAccess) (SessionHandle, error)
	Flush(context.Context) error
	Stat(context.Context, string) (SessionPersistenceSnapshot, bool, error)
	List(context.Context) ([]SessionPersistenceSnapshot, error)
	Close() error
}

type SessionPersistenceNotFoundError struct{ SessionID string }

func (e *SessionPersistenceNotFoundError) Error() string {
	return fmt.Sprintf("session %q not found", e.SessionID)
}

type SessionAlreadyExistsError struct{ SessionID string }

func (e *SessionAlreadyExistsError) Error() string {
	return fmt.Sprintf("session %q already exists", e.SessionID)
}

type SessionAlreadyOwnedError struct{ SessionID string }

func (e *SessionAlreadyOwnedError) Error() string {
	return fmt.Sprintf("session %q is already owned by an active write handle", e.SessionID)
}

type SessionReadOnlyError struct {
	SessionID string
	Operation string
}

func (e *SessionReadOnlyError) Error() string {
	return fmt.Sprintf("session %q: %s is not available on a read handle", e.SessionID, e.Operation)
}

type SessionOwnershipLostError struct{ SessionID string }

func (e *SessionOwnershipLostError) Error() string {
	return fmt.Sprintf("session %q: write ownership was lost; close this handle and reopen", e.SessionID)
}

type SessionHandleClosedError struct {
	SessionID string
	Operation string
}

func (e *SessionHandleClosedError) Error() string {
	return fmt.Sprintf("session %q: %s on a closed handle", e.SessionID, e.Operation)
}

type JSONLSessionStore struct {
	root string

	mu       sync.Mutex
	writers  map[string]*JSONLSessionHandle
	pending  map[string]jsonlPendingSession
	handles  map[*JSONLSessionHandle]struct{}
	paths    map[string]string
	locks    map[string]*sync.Mutex
	memo     map[string]jsonlSessionMemo
	memoLRU  []string
	revision uint64
	ops      sync.WaitGroup
	closing  bool
	closed   bool
	done     chan struct{}
}

type jsonlPendingSession struct {
	header              SessionHeader
	inheritedEventCount SessionLogOffset
	revision            SessionPersistenceRevision
}

type jsonlSessionMemo struct {
	revision SessionPersistenceRevision
	scan     jsonlSessionScan
}

func NewJSONLSessionStore(root string) (*JSONLSessionStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("JSONL session root is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	return &JSONLSessionStore{
		root: abs, writers: map[string]*JSONLSessionHandle{}, pending: map[string]jsonlPendingSession{},
		handles: map[*JSONLSessionHandle]struct{}{}, paths: map[string]string{}, locks: map[string]*sync.Mutex{},
		memo: map[string]jsonlSessionMemo{}, done: make(chan struct{}),
	}, nil
}

func (s *JSONLSessionStore) Create(ctx context.Context, meta SessionHeader, inheritedEventCount SessionLogOffset) (SessionHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if meta.SeedLength != 0 && inheritedEventCount == 0 {
		inheritedEventCount = SessionLogOffset(meta.SeedLength)
	}
	if inheritedEventCount != 0 {
		meta.IsSeeded = true
	}
	meta.SeedLength = int(inheritedEventCount)
	if err := validateSessionHeader(meta); err != nil {
		return nil, err
	}
	if err := validateSessionLogOffset(inheritedEventCount); err != nil {
		return nil, err
	}
	if !meta.IsSeeded && inheritedEventCount != 0 {
		return nil, errors.New("unseeded session cannot have inherited events")
	}
	unlock, err := s.begin(ctx, meta.ID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, _, found, err := s.findArtifact(ctx, meta.ID); err != nil {
		return nil, err
	} else if found {
		return nil, &SessionAlreadyExistsError{SessionID: meta.ID}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.writers[meta.ID]; exists {
		return nil, &SessionAlreadyExistsError{SessionID: meta.ID}
	}
	s.revision++
	s.pending[meta.ID] = jsonlPendingSession{
		header: meta, inheritedEventCount: inheritedEventCount,
		revision: SessionPersistenceRevision(fmt.Sprintf("memory:jsonl:%d", s.revision)),
	}
	handle := &JSONLSessionHandle{
		store: s, id: meta.ID, header: meta, inheritedEventCount: inheritedEventCount,
		access: SessionAccessWrite,
	}
	s.writers[meta.ID] = handle
	s.handles[handle] = struct{}{}
	return handle, nil
}

func (s *JSONLSessionStore) Open(ctx context.Context, id string, access SessionAccess) (SessionHandle, error) {
	if access != SessionAccessRead && access != SessionAccessWrite {
		return nil, fmt.Errorf("unsupported session access %q", access)
	}
	unlock, err := s.begin(ctx, id)
	if err != nil {
		return nil, err
	}
	defer unlock()
	s.mu.Lock()
	pending, isPending := s.pending[id]
	_, owned := s.writers[id]
	if access == SessionAccessWrite && owned {
		s.mu.Unlock()
		return nil, &SessionAlreadyOwnedError{SessionID: id}
	}
	if access == SessionAccessWrite {
		s.writers[id] = nil
	}
	s.mu.Unlock()
	if isPending {
		if access == SessionAccessWrite {
			s.releaseWriteClaim(id)
			return nil, &SessionAlreadyOwnedError{SessionID: id}
		}
		handle := &JSONLSessionHandle{
			store: s, id: id, header: pending.header, inheritedEventCount: pending.inheritedEventCount,
			access: SessionAccessRead,
		}
		s.adoptHandle(handle)
		return handle, nil
	}
	scan, path, err := s.readStoredLogLocked(ctx, id)
	if err != nil {
		if access == SessionAccessWrite {
			s.releaseWriteClaim(id)
		}
		return nil, err
	}
	handle := &JSONLSessionHandle{
		store: s, id: id, header: scan.meta, inheritedEventCount: scan.inheritedEventCount,
		access: access, materialized: true, path: path,
	}
	if access == SessionAccessWrite {
		handle.events = cloneSessionEvents(scan.events)
		handle.cursor = len(scan.events)
		if scan.committedBytes < len(scan.raw) {
			handle.tornTruncateTo = scan.committedBytes
			handle.hasTornTail = true
		}
	}
	s.adoptHandle(handle)
	return handle, nil
}

func (s *JSONLSessionStore) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	writers := make([]*JSONLSessionHandle, 0, len(s.writers))
	for _, writer := range s.writers {
		if writer != nil {
			writers = append(writers, writer)
		}
	}
	s.mu.Unlock()
	var failures []error
	for _, writer := range writers {
		if err := writer.Flush(ctx); err != nil {
			var closed *SessionHandleClosedError
			if !errors.As(err, &closed) {
				failures = append(failures, fmt.Errorf("session %q: %w", writer.id, err))
			}
		}
	}
	return errors.Join(failures...)
}

func (s *JSONLSessionStore) Stat(ctx context.Context, id string) (SessionPersistenceSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return SessionPersistenceSnapshot{}, false, err
	}
	s.mu.Lock()
	pending, ok := s.pending[id]
	s.mu.Unlock()
	if ok {
		return SessionPersistenceSnapshot{Header: pending.header, Revision: pending.revision}, true, nil
	}
	unlock, err := s.begin(ctx, id)
	if err != nil {
		return SessionPersistenceSnapshot{}, false, err
	}
	defer unlock()
	header, path, found, err := s.findArtifact(ctx, id)
	if err != nil || !found {
		return SessionPersistenceSnapshot{}, false, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return SessionPersistenceSnapshot{}, false, nil
		}
		return SessionPersistenceSnapshot{}, false, err
	}
	size := info.Size()
	return SessionPersistenceSnapshot{Header: header, Revision: jsonlFileRevision(path, info), SizeBytes: &size}, true, nil
}

func (s *JSONLSessionStore) List(ctx context.Context) ([]SessionPersistenceSnapshot, error) {
	unlock, err := s.begin(ctx, "\x00list")
	if err != nil {
		return nil, err
	}
	defer unlock()
	s.mu.Lock()
	pending := make(map[string]jsonlPendingSession, len(s.pending))
	for id, entry := range s.pending {
		pending[id] = entry
	}
	s.mu.Unlock()
	artifacts, err := s.listArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	snapshots := make([]SessionPersistenceSnapshot, 0, len(artifacts)+len(pending))
	for _, artifact := range artifacts {
		info, err := os.Stat(artifact.path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		size := info.Size()
		snapshots = append(snapshots, SessionPersistenceSnapshot{
			Header: artifact.header, Revision: jsonlFileRevision(artifact.path, info), SizeBytes: &size,
		})
		delete(pending, artifact.header.ID)
	}
	for _, entry := range pending {
		snapshots = append(snapshots, SessionPersistenceSnapshot{Header: entry.header, Revision: entry.revision})
	}
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].Header.CreatedAt != snapshots[j].Header.CreatedAt {
			return snapshots[i].Header.CreatedAt < snapshots[j].Header.CreatedAt
		}
		return snapshots[i].Header.ID < snapshots[j].Header.ID
	})
	return snapshots, nil
}

func (s *JSONLSessionStore) Close() error {
	s.mu.Lock()
	if s.closing || s.closed {
		done := s.done
		s.mu.Unlock()
		<-done
		return nil
	}
	s.closing = true
	handles := make([]*JSONLSessionHandle, 0, len(s.handles))
	for handle := range s.handles {
		handles = append(handles, handle)
	}
	s.mu.Unlock()
	var failures []error
	for _, handle := range handles {
		if err := handle.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	s.ops.Wait()
	s.mu.Lock()
	s.closed = true
	close(s.done)
	s.mu.Unlock()
	return errors.Join(failures...)
}

func (s *JSONLSessionStore) begin(ctx context.Context, id string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closing || s.closed {
		s.mu.Unlock()
		return nil, storageError(StorageClosed, "JSONL session store is closed")
	}
	lock := s.locks[id]
	if lock == nil {
		lock = &sync.Mutex{}
		s.locks[id] = lock
	}
	s.ops.Add(1)
	s.mu.Unlock()
	lock.Lock()
	if err := ctx.Err(); err != nil {
		lock.Unlock()
		s.ops.Done()
		return nil, err
	}
	return func() {
		lock.Unlock()
		s.ops.Done()
	}, nil
}

func jsonlFileRevision(path string, info fs.FileInfo) SessionPersistenceRevision {
	return SessionPersistenceRevision(fmt.Sprintf("jsonl:%s:%d:%d:%v", path, info.Size(), info.ModTime().UnixNano(), info.Sys()))
}

func (s *JSONLSessionStore) adoptHandle(handle *JSONLSessionHandle) {
	s.mu.Lock()
	s.handles[handle] = struct{}{}
	if handle.access == SessionAccessWrite {
		s.writers[handle.id] = handle
	}
	s.mu.Unlock()
}

func (s *JSONLSessionStore) releaseWriteClaim(id string) {
	s.mu.Lock()
	delete(s.writers, id)
	s.mu.Unlock()
}

func (s *JSONLSessionStore) releaseHandle(handle *JSONLSessionHandle, materialized bool) {
	s.mu.Lock()
	delete(s.handles, handle)
	if handle.access == SessionAccessWrite && s.writers[handle.id] == handle {
		delete(s.writers, handle.id)
		if !materialized {
			delete(s.pending, handle.id)
		}
	}
	s.mu.Unlock()
}

func (s *JSONLSessionStore) invalidateMemo(id string) {
	s.mu.Lock()
	delete(s.memo, id)
	for index, candidate := range s.memoLRU {
		if candidate == id {
			s.memoLRU = append(s.memoLRU[:index], s.memoLRU[index+1:]...)
			break
		}
	}
	s.mu.Unlock()
}

func (s *JSONLSessionStore) readStoredLog(ctx context.Context, id string) (jsonlSessionScan, string, error) {
	unlock, err := s.begin(ctx, id)
	if err != nil {
		return jsonlSessionScan{}, "", err
	}
	defer unlock()
	return s.readStoredLogLocked(ctx, id)
}

func (s *JSONLSessionStore) readStoredLogLocked(ctx context.Context, id string) (jsonlSessionScan, string, error) {
	_, path, found, err := s.findArtifact(ctx, id)
	if err != nil {
		return jsonlSessionScan{}, "", err
	}
	if !found {
		return jsonlSessionScan{}, "", &SessionPersistenceNotFoundError{SessionID: id}
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return jsonlSessionScan{}, "", &SessionPersistenceNotFoundError{SessionID: id}
		}
		return jsonlSessionScan{}, "", err
	}
	revision := jsonlFileRevision(path, info)
	s.mu.Lock()
	if memo, ok := s.memo[id]; ok && memo.revision == revision {
		s.mu.Unlock()
		return cloneJSONLSessionScan(memo.scan), path, nil
	}
	s.mu.Unlock()
	var scan jsonlSessionScan
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return jsonlSessionScan{}, "", err
		}
		before, err := os.Stat(path)
		if err != nil {
			return jsonlSessionScan{}, "", err
		}
		revision = jsonlFileRevision(path, before)
		scan, err = scanJSONLSession(path, id)
		if err != nil {
			return jsonlSessionScan{}, "", err
		}
		after, err := os.Stat(path)
		if err != nil {
			return jsonlSessionScan{}, "", err
		}
		if revision == jsonlFileRevision(path, after) || attempt == 1 {
			break
		}
	}
	s.mu.Lock()
	s.memo[id] = jsonlSessionMemo{revision: revision, scan: cloneJSONLSessionScan(scan)}
	for index, candidate := range s.memoLRU {
		if candidate == id {
			s.memoLRU = append(s.memoLRU[:index], s.memoLRU[index+1:]...)
			break
		}
	}
	s.memoLRU = append(s.memoLRU, id)
	for len(s.memoLRU) > 2 {
		oldest := s.memoLRU[0]
		s.memoLRU = s.memoLRU[1:]
		delete(s.memo, oldest)
	}
	s.mu.Unlock()
	return scan, path, nil
}

func cloneSessionEvents(events []Event) []Event {
	result := make([]Event, len(events))
	for index, event := range events {
		result[index] = cloneSessionEvent(event)
	}
	return result
}

func cloneJSONLSessionScan(scan jsonlSessionScan) jsonlSessionScan {
	scan.events = cloneSessionEvents(scan.events)
	scan.raw = append([]byte(nil), scan.raw...)
	return scan
}

type JSONLSessionHandle struct {
	store               *JSONLSessionStore
	id                  string
	header              SessionHeader
	inheritedEventCount SessionLogOffset
	access              SessionAccess

	mu             sync.Mutex
	closed         bool
	materialized   bool
	path           string
	events         []Event
	cursor         int
	hasTornTail    bool
	tornTruncateTo int
	observedLength int
}

func (h *JSONLSessionHandle) ID() string { return h.id }

func (h *JSONLSessionHandle) Header() SessionHeader { return h.header }

func (h *JSONLSessionHandle) InheritedEventCount() SessionLogOffset { return h.inheritedEventCount }

func (h *JSONLSessionHandle) Access() SessionAccess { return h.access }

func (h *JSONLSessionHandle) Read(ctx context.Context, bounds ...SessionLogOffset) ([]Event, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.assertOpen("read"); err != nil {
		return nil, err
	}
	if len(bounds) > 2 {
		return nil, errors.New("session handle Read accepts at most offset and length")
	}
	offset := SessionLogOffset(0)
	length := SessionLogOffset(maxJSONSafeInteger)
	if len(bounds) > 0 {
		offset = bounds[0]
	}
	if len(bounds) > 1 {
		length = bounds[1]
	}
	if offset < 0 || length < 0 {
		return nil, errors.New("session handle read offset and length must be non-negative")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var events []Event
	if h.access == SessionAccessWrite {
		events = cloneSessionEvents(h.events)
	} else {
		scan, path, err := h.store.readStoredLog(ctx, h.id)
		if err != nil {
			h.store.mu.Lock()
			_, pending := h.store.pending[h.id]
			h.store.mu.Unlock()
			if pending {
				events = []Event{}
			} else {
				return nil, err
			}
		} else {
			h.path = path
			h.materialized = true
			events = scan.events
		}
	}
	if len(events) < h.observedLength {
		return nil, fmt.Errorf("session %q: stored log shrank below a previously observed prefix (%d < %d)", h.id, len(events), h.observedLength)
	}
	h.observedLength = len(events)
	if int(offset) >= len(events) || length == 0 {
		return []Event{}, nil
	}
	end := int(offset + length)
	if end < int(offset) || end > len(events) {
		end = len(events)
	}
	return cloneSessionEvents(events[int(offset):end]), nil
}

func (h *JSONLSessionHandle) Append(ctx context.Context, events []Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.assertOpen("append"); err != nil {
		return err
	}
	if h.access != SessionAccessWrite {
		return &SessionReadOnlyError{SessionID: h.id, Operation: "append"}
	}
	batch, err := materializeSessionEventBatch(events, h.cursor)
	if err != nil {
		return err
	}
	if len(batch) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock, err := h.store.begin(ctx, h.id)
	if err != nil {
		return err
	}
	defer unlock()
	if h.hasTornTail {
		if err := truncateJSONLTail(h.path, int64(h.tornTruncateTo)); err != nil {
			return err
		}
		h.hasTornTail = false
	}
	h.store.invalidateMemo(h.id)
	if h.materialized {
		if h.path == "" {
			_, path, found, err := h.store.findArtifact(ctx, h.id)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("live session %q disappeared from persistence", h.id)
			}
			h.path = path
		}
		if err := appendJSONLLines(h.path, batch); err != nil {
			return err
		}
	} else {
		path := h.store.pathFor(h.header)
		if err := h.store.materialize(path, h.header, h.inheritedEventCount, batch); err != nil {
			_ = os.Remove(path)
			return err
		}
		h.path = path
		h.materialized = true
		h.store.mu.Lock()
		h.store.paths[h.id] = path
		delete(h.store.pending, h.id)
		h.store.mu.Unlock()
	}
	h.events = append(h.events, cloneSessionEvents(batch)...)
	h.cursor += len(batch)
	h.observedLength = h.cursor
	return nil
}

func (h *JSONLSessionHandle) Flush(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.assertOpen("flush"); err != nil {
		return err
	}
	if h.access != SessionAccessWrite {
		return &SessionReadOnlyError{SessionID: h.id, Operation: "flush"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if h.materialized {
		return nil
	}
	unlock, err := h.store.begin(ctx, h.id)
	if err != nil {
		return err
	}
	defer unlock()
	path := h.store.pathFor(h.header)
	h.store.invalidateMemo(h.id)
	if err := h.store.materialize(path, h.header, h.inheritedEventCount, nil); err != nil {
		_ = os.Remove(path)
		return err
	}
	h.path = path
	h.materialized = true
	h.store.mu.Lock()
	h.store.paths[h.id] = path
	delete(h.store.pending, h.id)
	h.store.mu.Unlock()
	return nil
}

func (h *JSONLSessionHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	materialized := h.materialized
	h.mu.Unlock()
	h.store.releaseHandle(h, materialized)
	return nil
}

func (h *JSONLSessionHandle) assertOpen(operation string) error {
	if h.closed {
		return &SessionHandleClosedError{SessionID: h.id, Operation: operation}
	}
	return nil
}

func materializeSessionEventBatch(events []Event, expected int) ([]Event, error) {
	if len(events) == 0 {
		return []Event{}, nil
	}
	batch := make([]Event, len(events))
	for index, event := range events {
		if int(event.Seq) != expected+index {
			return nil, fmt.Errorf("session append expected seq %d, got %d", expected+index, event.Seq)
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("session event %q is not losslessly JSON-serializable: %w", event.Type, err)
		}
		var detached Event
		if err := json.Unmarshal(encoded, &detached); err != nil {
			return nil, fmt.Errorf("session event %q is not losslessly JSON-serializable: %w", event.Type, err)
		}
		if detached.Type == "" || detached.Seq < 0 || detached.Time < -maxJSONSafeInteger || detached.Time > maxJSONSafeInteger {
			return nil, fmt.Errorf("invalid session event %q", event.Type)
		}
		batch[index] = detached
	}
	return batch, nil
}

func truncateJSONLTail(path string, size int64) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func inspectStoredSession(ctx context.Context, store SessionStore, id string, balanceInterrupted bool) (SessionInspection, error) {
	handle, err := store.Open(ctx, id, SessionAccessRead)
	if err != nil {
		return SessionInspection{}, err
	}
	events, readErr := handle.Read(ctx)
	closeErr := handle.Close()
	if readErr != nil || closeErr != nil {
		return SessionInspection{}, errors.Join(readErr, closeErr)
	}
	if balanceInterrupted {
		events = append(events, interruptedSessionClosers(events)...)
	}
	return SessionInspection{
		Meta: handle.Header(), InheritedEventCount: handle.InheritedEventCount(), Events: events,
	}, nil
}

func openStoredSessionForWrite(ctx context.Context, store SessionStore, id string) (SessionHandle, SessionInspection, error) {
	handle, err := store.Open(ctx, id, SessionAccessWrite)
	if err != nil {
		return nil, SessionInspection{}, err
	}
	events, err := handle.Read(ctx)
	if err != nil {
		_ = handle.Close()
		return nil, SessionInspection{}, err
	}
	closers := interruptedSessionClosers(events)
	if len(closers) > 0 {
		if err := handle.Append(ctx, closers); err != nil {
			_ = handle.Close()
			return nil, SessionInspection{}, err
		}
		events = append(events, closers...)
	}
	return handle, SessionInspection{
		Meta: handle.Header(), InheritedEventCount: handle.InheritedEventCount(), Events: events,
	}, nil
}

func (e *Engine) flushSessionPersistence(ctx context.Context, id string) error {
	session, err := e.getSession(id)
	if err != nil {
		return err
	}
	session.mu.Lock()
	handle := session.store
	session.mu.Unlock()
	if handle == nil {
		return fmt.Errorf("session %q has no active persistence handle", id)
	}
	return handle.Flush(ctx)
}

type jsonlArtifact struct {
	header SessionHeader
	path   string
}

func (s *JSONLSessionStore) listArtifacts(ctx context.Context) ([]jsonlArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var artifacts []jsonlArtifact
	seen := map[string]string{}
	err := filepath.WalkDir(s.root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) && path == s.root {
				return fs.SkipAll
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			return nil
		}
		header, ok, err := readJSONLHeader(path)
		if err != nil || !ok {
			return err
		}
		if previous := seen[header.ID]; previous != "" && previous != path {
			return fmt.Errorf("duplicate JSONL session id %q appears in %q and %q", header.ID, previous, path)
		}
		seen[header.ID] = path
		artifacts = append(artifacts, jsonlArtifact{header: header, path: path})
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	sort.Slice(artifacts, func(i, j int) bool {
		if artifacts[i].header.CreatedAt != artifacts[j].header.CreatedAt {
			return artifacts[i].header.CreatedAt < artifacts[j].header.CreatedAt
		}
		return artifacts[i].header.ID < artifacts[j].header.ID
	})
	s.mu.Lock()
	for _, artifact := range artifacts {
		s.paths[artifact.header.ID] = artifact.path
	}
	s.mu.Unlock()
	return artifacts, nil
}

func (s *JSONLSessionStore) findArtifact(ctx context.Context, id string) (SessionHeader, string, bool, error) {
	s.mu.Lock()
	known := s.paths[id]
	s.mu.Unlock()
	if known != "" {
		header, ok, err := readJSONLHeader(known)
		if err == nil && ok && header.ID == id {
			return header, known, true, nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return SessionHeader{}, "", false, err
		}
	}
	artifacts, err := s.listArtifacts(ctx)
	if err != nil {
		return SessionHeader{}, "", false, err
	}
	for _, artifact := range artifacts {
		if artifact.header.ID == id {
			return artifact.header, artifact.path, true, nil
		}
	}
	return SessionHeader{}, "", false, nil
}

func (s *JSONLSessionStore) pathFor(meta SessionHeader) string {
	project := "_no-cwd"
	if meta.CWD != "" {
		project = SessionProjectKey(meta.CWD)
	}
	return filepath.Join(s.root, project, EncodeSessionPathSegment(meta.ID), "session.jsonl")
}

func EncodeSessionPathSegment(raw string) string {
	if raw == "" {
		panic("cannot encode an empty path segment")
	}
	if raw == "." {
		return "~002E"
	}
	if raw == ".." {
		return "~002E~002E"
	}
	var out strings.Builder
	for _, code := range utf16.Encode([]rune(raw)) {
		ch := byte(code)
		if code < 128 && ch != '~' && (ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '.' || ch == '_' || ch == '-') {
			out.WriteByte(ch)
		} else {
			fmt.Fprintf(&out, "~%04X", code)
		}
	}
	return out.String()
}

func SessionProjectKey(cwd string) string {
	if cwd == "" {
		panic("cannot encode an empty project path")
	}
	var out strings.Builder
	separatorRun := false
	for _, code := range utf16.Encode([]rune(cwd)) {
		ch := byte(code)
		if code < 128 && (ch == '/' || ch == '\\' || ch == ':') {
			if !separatorRun {
				out.WriteByte('-')
			}
			separatorRun = true
			continue
		}
		if code < 128 && ch != '~' && (ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '.' || ch == '_' || ch == '-') {
			out.WriteByte(ch)
		} else {
			fmt.Fprintf(&out, "~%04X", code)
		}
		separatorRun = false
	}
	slug := strings.TrimLeft(out.String(), "-")
	if slug == "" {
		slug = "root"
	}
	if len(slug) > 251 {
		slug = slug[:251]
	}
	return "--" + slug + "--"
}

func (s *JSONLSessionStore) materialize(path string, meta SessionHeader, inheritedEventCount SessionLogOffset, events []Event) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	header, err := marshalSessionHeader(meta, inheritedEventCount)
	if err != nil {
		return err
	}
	body, err := marshalSessionEvents(events)
	if err != nil {
		return err
	}
	content := append(append(header, '\n'), body...)
	tmp, err := os.CreateTemp(filepath.Dir(path), ".session-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(content); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmpPath, path); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		dir, err := os.Open(filepath.Dir(path))
		if err != nil {
			return err
		}
		err = dir.Sync()
		_ = dir.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *JSONLSessionStore) repair(path string, scan jsonlSessionScan, closers []Event) error {
	if scan.committedBytes == len(scan.raw) && len(closers) == 0 {
		return nil
	}
	if scan.committedBytes != len(scan.raw) {
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		if err := file.Truncate(int64(scan.committedBytes)); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	if len(closers) > 0 {
		return appendJSONLLines(path, closers)
	}
	return nil
}

func appendJSONLLines(path string, events []Event) error {
	content, err := marshalSessionEvents(events)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	before := info.Size()
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return rollbackJSONLAppend(path, before, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return rollbackJSONLAppend(path, before, err)
	}
	return file.Close()
}

func rollbackJSONLAppend(path string, size int64, appendErr error) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return errors.Join(appendErr, err)
	}
	defer file.Close()
	if err := file.Truncate(size); err != nil {
		return errors.Join(appendErr, err)
	}
	if err := file.Sync(); err != nil {
		return errors.Join(appendErr, err)
	}
	return appendErr
}

type jsonlSessionScan struct {
	meta                SessionHeader
	inheritedEventCount SessionLogOffset
	events              []Event
	raw                 []byte
	committedBytes      int
}

func scanJSONLSession(path, expectedID string) (jsonlSessionScan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return jsonlSessionScan{}, err
	}
	headerEnd := bytes.IndexByte(data, '\n')
	if headerEnd < 0 {
		return jsonlSessionScan{}, errors.New("empty or header-less session log")
	}
	meta, ok, err := parseSessionHeader(data[:headerEnd])
	if err != nil {
		return jsonlSessionScan{}, err
	}
	if !ok {
		return jsonlSessionScan{}, errors.New("corrupt session log: first line is not a session header")
	}
	if expectedID != "" && meta.ID != expectedID {
		return jsonlSessionScan{}, fmt.Errorf("session log header id %q does not match requested id %q", meta.ID, expectedID)
	}
	events := []Event{}
	committed := headerEnd + 1
	issue := error(nil)
	offset := committed
	line := 1
	for offset < len(data) {
		relative := bytes.IndexByte(data[offset:], '\n')
		if relative < 0 {
			break
		}
		end := offset + relative
		line++
		decoded, decodeErr := decodeSessionStorageRecord(data[offset:end])
		if decodeErr != nil {
			if issue == nil {
				issue = fmt.Errorf("corrupt session log: unparsable committed event at line %d: %w", line, decodeErr)
			}
			offset = end + 1
			continue
		}
		containsTurnEnd := false
		for _, event := range decoded {
			containsTurnEnd = containsTurnEnd || event.Type == "turn/end"
		}
		if issue != nil {
			if containsTurnEnd {
				return jsonlSessionScan{}, issue
			}
			offset = end + 1
			continue
		}
		rowStart := len(events)
		for _, event := range decoded {
			if int(event.Seq) != len(events) {
				events = events[:rowStart]
				issue = fmt.Errorf("corrupt session log: seq gap in committed region at line %d (expected %d, got %d)", line, len(events), event.Seq)
				if containsTurnEnd {
					return jsonlSessionScan{}, issue
				}
				break
			}
			events = append(events, event)
		}
		if issue == nil {
			committed = end + 1
		}
		offset = end + 1
	}
	if _, err := foldSurfaceEvents(events, true); err != nil {
		return jsonlSessionScan{}, err
	}
	return jsonlSessionScan{meta: meta, inheritedEventCount: SessionLogOffset(meta.SeedLength), events: events, raw: data, committedBytes: committed}, nil
}

func readJSONLHeader(path string) (SessionHeader, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return SessionHeader{}, false, err
	}
	defer file.Close()
	buffer := make([]byte, 8192)
	var data []byte
	for {
		n, err := file.Read(buffer)
		if n > 0 {
			data = append(data, buffer[:n]...)
			if index := bytes.IndexByte(data, '\n'); index >= 0 {
				return parseSessionHeader(data[:index])
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return SessionHeader{}, false, nil
			}
			return SessionHeader{}, false, err
		}
		if len(data) > 1<<20 {
			return SessionHeader{}, false, errors.New("session header exceeds 1 MiB")
		}
	}
}

type sessionHeaderLine struct {
	Type            string `json:"type"`
	Version         int    `json:"version"`
	ID              string `json:"id"`
	CreatedAt       int64  `json:"createdAt"`
	CWD             string `json:"cwd,omitempty"`
	ParentSession   string `json:"parentSession,omitempty"`
	SeedLength      *int   `json:"seedLength,omitempty"`
	Origin          string `json:"origin,omitempty"`
	DelegationDepth int    `json:"delegationDepth"`
	AgentPreset     string `json:"agentPreset,omitempty"`
	Mode            string `json:"mode,omitempty"`
}

func marshalSessionHeader(meta SessionHeader, inheritedEventCount SessionLogOffset) ([]byte, error) {
	var seedLength *int
	if meta.IsSeeded {
		value := int(inheritedEventCount)
		seedLength = &value
	}
	return json.Marshal(sessionHeaderLine{
		Type: "session", Version: meta.Version, ID: meta.ID, CreatedAt: meta.CreatedAt,
		CWD: meta.CWD, ParentSession: meta.ParentSession, SeedLength: seedLength,
		Origin: meta.Origin, DelegationDepth: meta.DelegationDepth,
		AgentPreset: meta.AgentPreset, Mode: meta.Mode,
	})
}

func parseSessionHeader(line []byte) (SessionHeader, bool, error) {
	var header sessionHeaderLine
	if err := json.Unmarshal(line, &header); err != nil {
		return SessionHeader{}, false, nil
	}
	if header.Type != "session" || header.ID == "" {
		return SessionHeader{}, false, nil
	}
	if header.Version != SessionFormatVersion {
		return SessionHeader{}, false, fmt.Errorf("unsupported session format version %d", header.Version)
	}
	meta := SessionHeader{
		Version: header.Version, ID: header.ID, CreatedAt: header.CreatedAt, CWD: header.CWD,
		ParentSession: header.ParentSession, IsSeeded: header.SeedLength != nil, Origin: header.Origin,
		DelegationDepth: header.DelegationDepth, AgentPreset: header.AgentPreset, Mode: header.Mode,
	}
	if header.SeedLength != nil {
		meta.SeedLength = *header.SeedLength
	}
	if err := validateSessionHeader(meta); err != nil {
		return SessionHeader{}, false, err
	}
	return meta, true, nil
}

func validateSessionHeader(meta SessionHeader) error {
	if meta.ID == "" {
		return errors.New("session id is required")
	}
	if meta.Version != SessionFormatVersion {
		return fmt.Errorf("unsupported session format version %d", meta.Version)
	}
	if meta.CreatedAt < 0 || meta.CreatedAt > maxJSONSafeInteger {
		return errors.New("session createdAt must be a non-negative safe integer")
	}
	if meta.CWD != "" && !filepath.IsAbs(meta.CWD) {
		return fmt.Errorf("session cwd must be an absolute path, got %q", meta.CWD)
	}
	if meta.SeedLength < 0 || int64(meta.SeedLength) > maxJSONSafeInteger {
		return errors.New("session seedLength must be a non-negative safe integer")
	}
	if !meta.IsSeeded && meta.SeedLength != 0 {
		return errors.New("unseeded session cannot have inherited events")
	}
	if meta.Origin != "" && meta.Origin != "subagent" {
		return fmt.Errorf("unsupported session origin %q", meta.Origin)
	}
	if meta.DelegationDepth < 0 || int64(meta.DelegationDepth) > maxJSONSafeInteger {
		return errors.New("session delegationDepth must be a non-negative safe integer")
	}
	return nil
}

func validateSessionEventBatch(events []Event, expected int) error {
	for index, event := range events {
		if int(event.Seq) != expected+index {
			return fmt.Errorf("session append expected seq %d, got %d", expected+index, event.Seq)
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("session event %q is not JSON-serializable: %w", event.Type, err)
		}
		decoded, err := decodeSessionStorageRecord(encoded)
		if err != nil {
			return fmt.Errorf("invalid session event %q: %w", event.Type, err)
		}
		if len(decoded) != 1 {
			return fmt.Errorf("invalid session event %q: decoded into %d events", event.Type, len(decoded))
		}
	}
	return nil
}

func marshalSessionEvents(events []Event) ([]byte, error) {
	var buffer bytes.Buffer
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("session event %q is not JSON-serializable: %w", event.Type, err)
		}
		buffer.Write(encoded)
		buffer.WriteByte('\n')
	}
	return buffer.Bytes(), nil
}

func interruptedSessionClosers(events []Event) []Event {
	openTurn, openStep := -1, -1
	type pendingCall struct {
		id      string
		step    int
		callSeq int
		started bool
	}
	order := []string{}
	pending := map[string]*pendingCall{}
	for _, event := range events {
		switch event.Type {
		case "turn/start":
			openTurn, _ = eventTurn(event.Data)
			openStep = -1
			order = order[:0]
			clear(pending)
		case "turn/end":
			openTurn, openStep = -1, -1
			order = order[:0]
			clear(pending)
		case "step/start":
			openStep, _ = eventFieldInt(event.Data, "step")
		case "step/end":
			openStep = -1
			order = order[:0]
			clear(pending)
		case "assistant/message":
			step, _ := eventFieldInt(event.Data, "step")
			for _, block := range contentBlocks(nestedMessage(event.Data)["content"]) {
				if block.Type == "tool-call" && block.ID != "" {
					if pending[block.ID] == nil {
						order = append(order, block.ID)
					}
					pending[block.ID] = &pendingCall{id: block.ID, step: step}
				}
			}
		case "tool/call":
			data, _ := event.Data.(map[string]any)
			if call := pending[stringValue(data["callId"])]; call != nil {
				call.callSeq, call.started = int(event.Seq), true
			}
		case "tool/result":
			source, _ := nestedMessage(event.Data)["source"].(map[string]any)
			delete(pending, stringValue(source["callId"]))
		}
	}
	if openTurn < 0 || len(events) == 0 {
		return nil
	}
	seq := events[len(events)-1].Seq + 1
	timestamp := events[len(events)-1].Time
	closers := make([]Event, 0, len(pending)+2)
	for _, id := range order {
		call := pending[id]
		if call == nil {
			continue
		}
		code, name, text := "TOOL_NOT_STARTED", "ToolNotStartedError", "The tool call was interrupted before the Harness recorded it as started. Retry it if it is still needed."
		var sources []int
		if call.started {
			code, name = "TOOL_OUTCOME_UNKNOWN", "ToolOutcomeUnknownError"
			text = "The tool call was interrupted after it was recorded, but no result was durably recorded. Its outcome is unknown. Decide whether to retry from the tool semantics: retry only if the operation is read-only or idempotent; if it may have side effects, first verify external state or ask the user. Do not retry blindly."
			sources = []int{call.callSeq}
		}
		message := map[string]any{
			"id": fmt.Sprintf("interrupted-tool-result-%s-%d", id, seq), "role": "user",
			"source":  map[string]any{"kind": "tool", "callId": id},
			"content": []ContentBlock{{Type: "tool-result", ToolCallID: id, IsError: true, Content: []ContentBlock{{Type: "text", Text: text}}}},
		}
		closers = append(closers, Event{
			Type: "tool/result", Seq: seq, Time: timestamp, SurfaceOp: "append", SourceEventSeqs: sources,
			Data: map[string]any{"turn": openTurn, "step": call.step, "message": message, "error": map[string]any{"name": name, "code": code}},
		})
		seq++
	}
	if openStep >= 0 {
		closers = append(closers, Event{Type: "step/end", Seq: seq, Time: timestamp, Data: map[string]any{"turn": openTurn, "step": openStep}})
		seq++
	}
	closers = append(closers, Event{Type: "turn/end", Seq: seq, Time: timestamp, Data: map[string]any{"turn": openTurn, "reason": map[string]any{"kind": "interrupted"}}})
	return closers
}

func eventFieldInt(value any, field string) (int, bool) {
	data, ok := value.(map[string]any)
	if !ok {
		return 0, false
	}
	switch number := data[field].(type) {
	case int:
		return number, true
	case int64:
		return int(number), true
	case float64:
		return int(number), number == float64(int(number))
	case json.Number:
		parsed, err := number.Int64()
		return int(parsed), err == nil
	default:
		return 0, false
	}
}
