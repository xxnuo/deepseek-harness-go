package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type projectionCacheCloseBlockingProvider struct {
	started  chan struct{}
	canceled chan struct{}
}

func (p *projectionCacheCloseBlockingProvider) ID() string   { return "projection-cache-close" }
func (p *projectionCacheCloseBlockingProvider) Name() string { return "Projection Cache Close" }
func (p *projectionCacheCloseBlockingProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: p.ID(), Name: p.Name()}}, nil
}
func (p *projectionCacheCloseBlockingProvider) Complete(ctx context.Context, _ ChatRequest, _ func(Delta) error) (Completion, error) {
	select {
	case p.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	select {
	case p.canceled <- struct{}{}:
	default:
	}
	return Completion{}, ctx.Err()
}

func projectionCacheTestConfig(t *testing.T) Config {
	t.Helper()
	t.Setenv("DSH_PERMISSION_MODE", "")
	config := DefaultConfig()
	config.DataDir = t.TempDir()
	config.Workspace = config.DataDir
	config.Provider, config.Model = "echo", "echo"
	config.Persist = true
	config.SessionTitleLLM.Enabled = false
	config.SessionProjectionCache = &SessionProjectionCacheConfig{WriteEveryEvents: 200, WriteInterval: time.Hour}
	return config
}

func waitProjectionCacheSeq(t *testing.T, engine *Engine, id string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		engine.projectionCache.medium.mu.RLock()
		record, ok := engine.projectionCache.medium.records[id]
		engine.projectionCache.medium.mu.RUnlock()
		if ok && projectionCheckpointCut(record.Rows) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("projection cache %q did not reach seq %d", id, want)
}

func TestSessionProjectionCacheMandatoryWritesColdReadAndStaleWriteBack(t *testing.T) {
	config := projectionCacheTestConfig(t)
	engine, err := New(WithConfig(config))
	if err != nil {
		t.Fatal(err)
	}
	id, err := engine.CreateSession(context.Background(), config.Workspace, "projection-cache", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	if _, err := engine.appendEvent(session, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEvent(session, "user/message", map[string]any{
		"id": "prompt", "role": "user", "content": []ContentBlock{{Type: "text", Text: "hello"}},
		"source": map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.RenameSession(id, "Cached title"); err != nil {
		t.Fatal(err)
	}
	end, err := engine.appendEvent(session, "turn/end", map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}})
	if err != nil {
		t.Fatal(err)
	}
	waitProjectionCacheSeq(t, engine, id, int(end.Seq))

	if _, _, err := engine.RenameSession(id, "Fresh title"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := engine.SessionProjectionSnapshot(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.AsOfSeq != int(end.Seq)+1 || snapshot.Values["title"] != "Fresh title" {
		t.Fatalf("stale cache refresh = %#v", snapshot)
	}
	waitProjectionCacheSeq(t, engine, id, snapshot.AsOfSeq)

	closeOnly, err := engine.CreateSession(context.Background(), config.Workspace, "projection-cache-close", "")
	if err != nil {
		t.Fatal(err)
	}
	closeSession, _ := engine.getSession(closeOnly)
	waitProjectionCacheSeq(t, engine, closeOnly, -1)
	if _, err := engine.appendEvent(closeSession, "user/message", map[string]any{
		"id": "close-prompt", "role": "user", "content": []ContentBlock{{Type: "text", Text: "close"}},
		"source": map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}
	if snapshot, ok := engine.projectionCache.medium.snapshot(engine.sessionProjections, closeSession.Header, closeSession.InheritedEventCount); !ok || snapshot.AsOfSeq != -1 {
		t.Fatalf("non-mandatory event advanced the creation checkpoint: %#v, ok=%t", snapshot, ok)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(WithConfig(config))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	cold, _ := reopened.getSession(id)
	cold.mu.Lock()
	cold.Title = "mutated in-memory title"
	cold.mu.Unlock()
	cached, err := reopened.SessionProjectionSnapshot(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if cached.AsOfSeq != snapshot.AsOfSeq || cached.Values["title"] != "Fresh title" {
		t.Fatalf("cold cache snapshot = %#v", cached)
	}
	closeCold, _ := reopened.getSession(closeOnly)
	closeCold.mu.Lock()
	closeLastSeq := len(closeCold.Events) - 1
	closeHeader := closeCold.Header
	closeCold.mu.Unlock()
	if snapshot, ok := reopened.projectionCache.medium.snapshot(reopened.sessionProjections, closeHeader, closeCold.InheritedEventCount); !ok || snapshot.AsOfSeq != closeLastSeq {
		t.Fatal("engine close did not write the mandatory projection checkpoint")
	}
	rows := reopened.ListSessions()
	found := false
	for _, row := range rows {
		if row.SessionID != id {
			continue
		}
		found = true
		projection := row.Projections.(map[string]any)
		if projection["values"].(map[string]any)["title"] != "Fresh title" {
			t.Fatalf("cold list projection = %#v", projection)
		}
	}
	if !found {
		t.Fatal("cached session missing from list")
	}
}

func TestSessionProjectionCacheRequiresValidPersistentConfig(t *testing.T) {
	config := DefaultConfig()
	config.Persist = false
	config.SessionProjectionCache = &SessionProjectionCacheConfig{WriteEveryEvents: 1, WriteInterval: time.Second}
	if _, err := New(WithConfig(config)); err == nil {
		t.Fatal("projection cache without persistence was accepted")
	}
	config.Persist = true
	config.SessionProjectionCache.WriteEveryEvents = 0
	if _, err := New(WithConfig(config)); err == nil {
		t.Fatal("invalid projection cache threshold was accepted")
	}
}

func TestEngineCloseCheckpointsDynamicProjectionAfterWorkerCancellation(t *testing.T) {
	config := projectionCacheTestConfig(t)
	config.Terminal.Disabled = true
	engine, err := New(WithConfig(config))
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = engine.Close()
		}
	})

	provider := &projectionCacheCloseBlockingProvider{
		started:  make(chan struct{}, 1),
		canceled: make(chan struct{}, 1),
	}
	engine.RegisterProvider(provider)
	id, err := engine.CreateSession(t.Context(), config.Workspace, "projection-cache-close-order", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	runDynamicBuiltinPlugin(t, engine, id, "cpc", `
return {
  inject: ['sessionProjections'],
  apply(ctx) {
    ctx.sessionProjections.register({
      key: 'dynamic/close-count',
      stateVersion: 1,
      init: () => ({ count: 0 }),
      apply: (state, event) => event.type === 'turn/end'
        ? { count: state.count + 1 }
        : state,
      wire: { view: state => state },
    })
  },
}`)
	if composition := engine.sessionProjections.Signature(); !strings.Contains(composition, "dynamic/close-count") {
		t.Fatalf("dynamic projection missing from composition %q", composition)
	}
	session, err := engine.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	medium := engine.projectionCache.medium

	if _, err := engine.Prompt(t.Context(), id, PromptRequest{
		Mode: "queue", Content: []PromptContentPart{{Type: "text", Text: "wait"}}, Literal: true,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("provider did not start")
	}

	var logs bytes.Buffer
	previousLogWriter := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previousLogWriter)
	closeResult := make(chan error, 1)
	go func() { closeResult <- engine.Close() }()
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
		closed = true
	case <-time.After(2 * time.Second):
		t.Fatal("Engine.Close did not finish")
	}
	select {
	case <-provider.canceled:
	default:
		t.Fatal("provider did not observe shutdown cancellation")
	}
	if output := logs.String(); strings.Contains(output, "dynamic Cordis runtime is closed") || strings.Contains(output, "session projection cache close write") {
		t.Fatalf("shutdown logged a projection/runtime failure:\n%s", output)
	}

	session.mu.Lock()
	lastSeq := len(session.Events) - 1
	session.mu.Unlock()
	medium.mu.RLock()
	record, ok := medium.records[id]
	medium.mu.RUnlock()
	if !ok {
		t.Fatal("final projection cache record is missing")
	}
	row, ok := record.Rows["dynamic/close-count"]
	if !ok {
		t.Fatal("final dynamic projection cache row is missing")
	}
	if row.Seq != lastSeq {
		t.Fatalf("final projection cache seq = %d, want %d", row.Seq, lastSeq)
	}
	want := map[string]any{"count": float64(1)}
	if got := row.Value; !reflect.DeepEqual(got, want) {
		t.Fatalf("final dynamic projection = %#v, want %#v", got, want)
	}
	if after := engine.sessionProjections.Signature(); strings.Contains(after, "dynamic/close-count") {
		t.Fatalf("dynamic projection survived run disposal: %q", after)
	}
}

func TestSessionProjectionCacheRecoversCompatibleVersionsAndBacksUpInvalidRecords(t *testing.T) {
	titleDefinition := ProjectionDefinition{
		Key: "title", StateVersion: 1,
		Init: func() any { return "" },
		Apply: func(state any, event Event) ProjectionResult {
			if event.Type == "test/title" {
				return ProjectionResult{State: event.Data.(string), Changed: true}
			}
			return ProjectionResult{State: state}
		},
		View: func(state any) any { return state },
	}
	oldRecord := func(title string) map[string]any {
		return map[string]any{
			"identity": map[string]any{"createdAt": 1},
			"rows":     map[string]any{"title": map[string]any{"ver": 1, "seq": 4, "val": title}},
		}
	}
	writeJSON := func(t *testing.T, path string, value any) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	for _, fixture := range []struct {
		name    string
		version int
		legacy  bool
	}{
		{name: "v3", version: 3, legacy: true},
		{name: "v4", version: 4},
		{name: "v5", version: 5},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			root := t.TempDir()
			id := "fixture-session"
			if fixture.legacy {
				writeJSON(t, filepath.Join(root, "session_projcache.json"), map[string]any{
					"unit":   map[string]any{"name": "session_projcache", "version": fixture.version},
					"global": nil,
					"tables": map[string]any{"sessions": map[string]any{id: oldRecord(fixture.name)}},
				})
			} else {
				writeJSON(t, filepath.Join(root, "session_projcache", "sessions", id+".json"), map[string]any{
					"version": fixture.version, "record": oldRecord(fixture.name),
				})
			}
			registry := NewSessionProjectionRegistry()
			if _, err := registry.Register(titleDefinition); err != nil {
				t.Fatal(err)
			}
			cache, err := newSessionProjectionCache(root, SessionProjectionCacheConfig{WriteEveryEvents: 100, WriteInterval: time.Hour}, registry)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cache.close(nil) })
			header := SessionHeader{ID: id, CreatedAt: 1}
			snapshot, ok := cache.cachedSnapshot(header, 0, "title")
			if !ok || snapshot.Values["title"] != fixture.name {
				t.Fatalf("compatible snapshot = %#v, ok=%t", snapshot, ok)
			}
			session := &Session{Header: SessionHeader{ID: id, CreatedAt: 2}}
			event := Event{Type: "test/title", Seq: 0, Time: 1, Data: "current"}
			session.Events = append(session.Events, event)
			if _, err := registry.Drive(session, event); err != nil {
				t.Fatal(err)
			}
			if err := cache.writeSession(t.Context(), session); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(root, "session_projcache", "sessions", id+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var current struct {
				Version int `json:"version"`
				Record  struct {
					Identity projectionCacheIdentity `json:"identity"`
					Rows     ProjectionCheckpoint    `json:"rows"`
				} `json:"record"`
			}
			if err := json.Unmarshal(data, &current); err != nil {
				t.Fatal(err)
			}
			if current.Version != sessionProjectionCacheVersion || current.Record.Identity.IsSeeded == nil || *current.Record.Identity.IsSeeded || current.Record.Rows["title"].Value != "current" {
				t.Fatalf("rewritten document = %#v", current)
			}
		})
	}

	t.Run("lineageless-seeded", func(t *testing.T) {
		root := t.TempDir()
		id := "lineageless"
		writeJSON(t, filepath.Join(root, "session_projcache", "sessions", id+".json"), map[string]any{
			"version": 5, "record": oldRecord("old"),
		})
		registry := NewSessionProjectionRegistry()
		if _, err := registry.Register(titleDefinition); err != nil {
			t.Fatal(err)
		}
		cache, err := newSessionProjectionCache(root, SessionProjectionCacheConfig{WriteEveryEvents: 100, WriteInterval: time.Hour}, registry)
		if err != nil {
			t.Fatal(err)
		}
		defer cache.close(nil)
		if _, ok := cache.cachedSnapshot(SessionHeader{ID: id, CreatedAt: 1, IsSeeded: true}, 2, "title"); ok {
			t.Fatal("lineage-less record was accepted for a seeded session")
		}
	})

	t.Run("invalid-record", func(t *testing.T) {
		root := t.TempDir()
		sessionsDir := filepath.Join(root, "session_projcache", "sessions")
		writeJSON(t, filepath.Join(sessionsDir, "broken.json"), map[string]any{
			"version": sessionProjectionCacheVersion,
			"record":  map[string]any{"identity": map[string]any{"createdAt": "bad"}, "rows": "bad"},
		})
		writeJSON(t, filepath.Join(sessionsDir, "good.json"), map[string]any{
			"version": 5, "record": oldRecord("survivor"),
		})
		registry := NewSessionProjectionRegistry()
		if _, err := registry.Register(titleDefinition); err != nil {
			t.Fatal(err)
		}
		cache, err := newSessionProjectionCache(root, SessionProjectionCacheConfig{WriteEveryEvents: 100, WriteInterval: time.Hour}, registry)
		if err != nil {
			t.Fatal(err)
		}
		defer cache.close(nil)
		if _, err := os.Stat(filepath.Join(sessionsDir, "broken.json")); !os.IsNotExist(err) {
			t.Fatalf("invalid record was not moved aside: %v", err)
		}
		entries, err := os.ReadDir(sessionsDir)
		if err != nil {
			t.Fatal(err)
		}
		backedUp := false
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "broken.json.bak.") {
				backedUp = true
			}
		}
		if !backedUp {
			t.Fatalf("invalid record backup missing: %#v", entries)
		}
		if snapshot, ok := cache.cachedSnapshot(SessionHeader{ID: "good", CreatedAt: 1}, 0, "title"); !ok || snapshot.Values["title"] != "survivor" {
			t.Fatalf("neighbor record did not survive: %#v, ok=%t", snapshot, ok)
		}
	})
}
