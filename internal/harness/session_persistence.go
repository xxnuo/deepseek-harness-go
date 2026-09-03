package harness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	Header   SessionHeader
	Revision SessionPersistenceRevision
}

type SessionInspection struct {
	Meta                SessionHeader
	InheritedEventCount SessionLogOffset
	Events              []Event
}

type SessionLocation struct {
	Kind string
	Path string
}

type SessionRawArtifact struct {
	Meta                SessionHeader
	InheritedEventCount SessionLogOffset
	Filename            string
	Content             string
}

type SessionStore interface {
	Locate(SessionHeader) (SessionLocation, bool)
	SupportsRawArtifacts() bool
	ReadRaw(context.Context, string) (SessionRawArtifact, bool, error)
	Create(context.Context, SessionHeader, SessionLogOffset) error
	Append(context.Context, string, []Event) error
	Load(context.Context, string) (SessionInspection, error)
	Inspect(context.Context, string) (SessionInspection, error)
	ReadFrom(context.Context, string, SessionLogOffset) (SessionInspection, error)
	List(context.Context) ([]SessionHeader, error)
	ListSnapshots(context.Context) ([]SessionPersistenceSnapshot, error)
	Close() error
}

// SessionPersistenceFlusher proves that accepted writes for one live session
// reached the store's durable backend.
type SessionPersistenceFlusher interface {
	Flush(context.Context, string) error
}

type sessionStoreCreateRollback interface {
	Delete(context.Context, string) error
}

func rollbackSessionStoreCreate(store SessionStore, id string) {
	if rollback, ok := store.(sessionStoreCreateRollback); ok {
		_ = rollback.Delete(context.Background(), id)
	}
}

type JSONLSessionStore struct {
	root string

	mu      sync.Mutex
	states  map[string]jsonlSessionState
	paths   map[string]string
	locks   map[string]*sync.Mutex
	ops     sync.WaitGroup
	closing bool
	closed  bool
	done    chan struct{}
}

type jsonlSessionState struct {
	meta                SessionHeader
	inheritedEventCount SessionLogOffset
	cursor              int
	materialized        bool
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
		root: abs, states: map[string]jsonlSessionState{}, paths: map[string]string{},
		locks: map[string]*sync.Mutex{}, done: make(chan struct{}),
	}, nil
}

func (s *JSONLSessionStore) Locate(meta SessionHeader) (SessionLocation, bool) {
	return SessionLocation{Kind: "jsonl", Path: s.pathFor(meta)}, true
}

func (s *JSONLSessionStore) SupportsRawArtifacts() bool { return true }

func (s *JSONLSessionStore) Flush(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	_, live := s.states[id]
	closed := s.closing || s.closed
	s.mu.Unlock()
	if closed {
		return storageError(StorageClosed, "JSONL session store is closed")
	}
	if !live {
		return fmt.Errorf("session-not-found: %s", id)
	}
	// JSONL Append is synchronous, so there is no deferred write queue.
	return nil
}

func (s *JSONLSessionStore) Create(ctx context.Context, meta SessionHeader, inheritedEventCount SessionLogOffset) error {
	if meta.SeedLength != 0 && inheritedEventCount == 0 {
		inheritedEventCount = SessionLogOffset(meta.SeedLength)
	}
	if inheritedEventCount != 0 {
		meta.IsSeeded = true
	}
	meta.SeedLength = int(inheritedEventCount)
	if err := validateSessionHeader(meta); err != nil {
		return err
	}
	if err := validateSessionLogOffset(inheritedEventCount); err != nil {
		return err
	}
	if !meta.IsSeeded && inheritedEventCount != 0 {
		return errors.New("unseeded session cannot have inherited events")
	}
	unlock, err := s.begin(ctx, meta.ID)
	if err != nil {
		return err
	}
	defer unlock()
	if _, _, found, err := s.findArtifact(ctx, meta.ID); err != nil {
		return err
	} else if found {
		return fmt.Errorf("session %q already exists in persistence", meta.ID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.states[meta.ID]; exists {
		return fmt.Errorf("session %q is already registered in persistence", meta.ID)
	}
	s.states[meta.ID] = jsonlSessionState{meta: meta, inheritedEventCount: inheritedEventCount}
	return nil
}

func (s *JSONLSessionStore) Delete(ctx context.Context, id string) error {
	unlock, err := s.begin(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	s.mu.Lock()
	state, live := s.states[id]
	path := s.paths[id]
	delete(s.states, id)
	delete(s.paths, id)
	s.mu.Unlock()
	if live && path == "" {
		path = s.pathFor(state.meta)
	}
	if path != "" {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (s *JSONLSessionStore) Append(ctx context.Context, id string, events []Event) error {
	unlock, err := s.begin(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	s.mu.Lock()
	state, live := s.states[id]
	s.mu.Unlock()
	if live {
		if len(events) == 0 {
			return nil
		}
		if err := validateSessionEventBatch(events, state.cursor); err != nil {
			return err
		}
		if !state.materialized {
			path := s.pathFor(state.meta)
			if err := s.materialize(path, state.meta, state.inheritedEventCount, events); err != nil {
				return err
			}
			state.materialized = true
			s.mu.Lock()
			s.paths[id] = path
			s.mu.Unlock()
		} else {
			_, path, found, err := s.findArtifact(ctx, id)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("live session %q disappeared from persistence", id)
			}
			if err := appendJSONLLines(path, events); err != nil {
				return err
			}
		}
		state.cursor += len(events)
		s.mu.Lock()
		s.states[id] = state
		s.mu.Unlock()
		return nil
	}
	meta, path, found, err := s.findArtifact(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("session %q is not registered in persistence", id)
	}
	if len(events) == 0 {
		return nil
	}
	scan, err := scanJSONLSession(path, id)
	if err != nil {
		return err
	}
	closers := interruptedSessionClosers(scan.events)
	if err := s.repair(path, scan, closers); err != nil {
		return err
	}
	expected := len(scan.events) + len(closers)
	if err := validateSessionEventBatch(events, expected); err != nil {
		return err
	}
	if err := appendJSONLLines(path, events); err != nil {
		return err
	}
	s.mu.Lock()
	s.states[id] = jsonlSessionState{meta: meta, inheritedEventCount: SessionLogOffset(meta.SeedLength), cursor: expected + len(events), materialized: true}
	s.mu.Unlock()
	return nil
}

func (s *JSONLSessionStore) Load(ctx context.Context, id string) (SessionInspection, error) {
	unlock, err := s.begin(ctx, id)
	if err != nil {
		return SessionInspection{}, err
	}
	defer unlock()
	_, path, found, err := s.findArtifact(ctx, id)
	if err != nil {
		return SessionInspection{}, err
	}
	if !found {
		return SessionInspection{}, fmt.Errorf("session-not-found: %s", id)
	}
	scan, err := scanJSONLSession(path, id)
	if err != nil {
		return SessionInspection{}, err
	}
	s.mu.Lock()
	state, live := s.states[id]
	s.mu.Unlock()
	closers := interruptedSessionClosers(scan.events)
	if live && state.materialized {
		if scan.committedBytes != len(scan.raw) || len(closers) > 0 {
			return SessionInspection{}, fmt.Errorf("cannot crash-repair live session %q", id)
		}
		return SessionInspection{Meta: scan.meta, InheritedEventCount: scan.inheritedEventCount, Events: append([]Event(nil), scan.events...)}, nil
	}
	if err := s.repair(path, scan, closers); err != nil {
		return SessionInspection{}, err
	}
	events := append(append([]Event(nil), scan.events...), closers...)
	return SessionInspection{Meta: scan.meta, InheritedEventCount: scan.inheritedEventCount, Events: events}, nil
}

func (s *JSONLSessionStore) Inspect(ctx context.Context, id string) (SessionInspection, error) {
	unlock, err := s.begin(ctx, id)
	if err != nil {
		return SessionInspection{}, err
	}
	defer unlock()
	_, path, found, err := s.findArtifact(ctx, id)
	if err != nil {
		return SessionInspection{}, err
	}
	if !found {
		return SessionInspection{}, fmt.Errorf("session-not-found: %s", id)
	}
	scan, err := scanJSONLSession(path, id)
	if err != nil {
		return SessionInspection{}, err
	}
	s.mu.Lock()
	state, live := s.states[id]
	s.mu.Unlock()
	events := append([]Event(nil), scan.events...)
	if !live || !state.materialized {
		events = append(events, interruptedSessionClosers(scan.events)...)
	}
	return SessionInspection{Meta: scan.meta, InheritedEventCount: scan.inheritedEventCount, Events: events}, nil
}

func (s *JSONLSessionStore) ReadFrom(ctx context.Context, id string, fromSeq SessionLogOffset) (SessionInspection, error) {
	if fromSeq < 0 {
		return SessionInspection{}, errors.New("fromSeq must be non-negative")
	}
	unlock, err := s.begin(ctx, id)
	if err != nil {
		return SessionInspection{}, err
	}
	defer unlock()
	_, path, found, err := s.findArtifact(ctx, id)
	if err != nil {
		return SessionInspection{}, err
	}
	if !found {
		return SessionInspection{}, fmt.Errorf("session-not-found: %s", id)
	}
	scan, err := scanJSONLSession(path, id)
	if err != nil {
		return SessionInspection{}, err
	}
	if int(fromSeq) >= len(scan.events) {
		return SessionInspection{Meta: scan.meta, InheritedEventCount: scan.inheritedEventCount, Events: []Event{}}, nil
	}
	return SessionInspection{Meta: scan.meta, InheritedEventCount: scan.inheritedEventCount, Events: append([]Event(nil), scan.events[int(fromSeq):]...)}, nil
}

func (s *JSONLSessionStore) ReadRaw(ctx context.Context, id string) (SessionRawArtifact, bool, error) {
	unlock, err := s.begin(ctx, id)
	if err != nil {
		return SessionRawArtifact{}, false, err
	}
	defer unlock()
	_, path, found, err := s.findArtifact(ctx, id)
	if err != nil || !found {
		return SessionRawArtifact{}, false, err
	}
	scan, err := scanJSONLSession(path, id)
	if err != nil {
		return SessionRawArtifact{}, false, err
	}
	return SessionRawArtifact{
		Meta: scan.meta, InheritedEventCount: scan.inheritedEventCount,
		Filename: "session.jsonl", Content: string(scan.raw[:scan.committedBytes]),
	}, true, nil
}

func (s *JSONLSessionStore) List(ctx context.Context) ([]SessionHeader, error) {
	unlock, err := s.begin(ctx, "\x00list")
	if err != nil {
		return nil, err
	}
	defer unlock()
	artifacts, err := s.listArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	headers := make([]SessionHeader, 0, len(artifacts))
	for _, artifact := range artifacts {
		headers = append(headers, artifact.header)
	}
	return headers, nil
}

func (s *JSONLSessionStore) ListSnapshots(ctx context.Context) ([]SessionPersistenceSnapshot, error) {
	unlock, err := s.begin(ctx, "\x00list")
	if err != nil {
		return nil, err
	}
	defer unlock()
	artifacts, err := s.listArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	snapshots := make([]SessionPersistenceSnapshot, 0, len(artifacts))
	for _, artifact := range artifacts {
		data, err := os.ReadFile(artifact.path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		info, err := os.Stat(artifact.path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		digest := sha256.Sum256(data)
		snapshots = append(snapshots, SessionPersistenceSnapshot{
			Header: artifact.header,
			Revision: SessionPersistenceRevision(fmt.Sprintf(
				"jsonl:%s:%d:%d:%v:%s",
				artifact.path, info.Size(), info.ModTime().UnixNano(), info.Sys(), hex.EncodeToString(digest[:]),
			)),
		})
	}
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
	s.mu.Unlock()
	s.ops.Wait()
	s.mu.Lock()
	s.closed = true
	close(s.done)
	s.mu.Unlock()
	return nil
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
