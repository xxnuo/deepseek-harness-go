package harness

import (
	"context"
	"testing"
	"time"
)

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
		if ok && record.Seq == want {
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
	waitProjectionCacheSeq(t, engine, id, end.Seq)

	if _, _, err := engine.RenameSession(id, "Fresh title"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := engine.SessionProjectionSnapshot(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.AsOfSeq != end.Seq+1 || snapshot.Values["title"] != "Fresh title" {
		t.Fatalf("stale cache refresh = %#v", snapshot)
	}
	waitProjectionCacheSeq(t, engine, id, snapshot.AsOfSeq)

	closeOnly, err := engine.CreateSession(context.Background(), config.Workspace, "projection-cache-close", "")
	if err != nil {
		t.Fatal(err)
	}
	closeSession, _ := engine.getSession(closeOnly)
	if _, err := engine.appendEvent(closeSession, "user/message", map[string]any{
		"id": "close-prompt", "role": "user", "content": []ContentBlock{{Type: "text", Text: "close"}},
		"source": map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := engine.projectionCache.medium.snapshot(closeSession.Header, 0, true); ok {
		t.Fatal("non-mandatory event was written before the configured threshold")
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
	if _, ok := reopened.projectionCache.medium.snapshot(closeHeader, closeLastSeq, true); !ok {
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
