package harness

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEngineDefaultSessionStoreLazyPersistenceAndRestart(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", "")
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = true

	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	id, err := engine.CreateSession(context.Background(), cfg.Workspace, "default-store", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	location, ok := engine.sessionStore.Locate(session.Header)
	if !ok || location.Kind != "jsonl" {
		t.Fatalf("location = %#v, %v", location, ok)
	}
	if _, err := os.Stat(location.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("blank session materialized: %v", err)
	}
	if got := engine.hookPayload(session, HookBridgeConfig{Dialect: HookDialectClaudeCode}, hookPointInput{})["transcript_path"]; got != location.Path {
		t.Fatalf("hook transcript = %#v, want %q", got, location.Path)
	}
	if snapshots := engine.sessionQuerySnapshots(); len(snapshots) != 1 || snapshots[0].persisted {
		t.Fatalf("blank snapshots = %#v", snapshots)
	}
	if _, err := engine.appendEvent(session, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(location.Path); err != nil {
		t.Fatal(err)
	}
	if snapshots := engine.sessionQuerySnapshots(); len(snapshots) != 1 || !snapshots[0].persisted {
		t.Fatalf("materialized snapshots = %#v", snapshots)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored, err := reopened.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	restored.mu.Lock()
	events := append([]Event(nil), restored.Events...)
	restored.mu.Unlock()
	if len(events) != 2 || events[0].Type != "turn/start" || events[1].Type != "turn/end" {
		t.Fatalf("restart repair = %#v", events)
	}
}

func TestEngineSQLiteSessionStoreRestoresSubagentMetadata(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", "")
	root := t.TempDir()
	path := filepath.Join(root, "sessions.db")
	store, err := NewSQLiteSessionStore(path, SQLiteJournalDelete)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = root, root
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	engine, err := New(WithConfig(cfg), WithSessionStore(store))
	if err != nil {
		t.Fatal(err)
	}
	if engine.Config().SessionStore != store || !engine.Config().Persist {
		t.Fatalf("session store config = %#v", engine.Config().SessionStore)
	}
	parent, err := engine.CreateSession(context.Background(), root, "sqlite-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := engine.CreateSubagent(context.Background(), parent, "sqlite-child", "")
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := engine.CreateSubagent(context.Background(), child, "sqlite-grandchild", "")
	if err != nil {
		t.Fatal(err)
	}
	for index, id := range []string{parent, child, grandchild} {
		session, _ := engine.getSession(id)
		if _, err := engine.appendEvent(session, "turn/start", map[string]any{"turn": index + 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.appendEvent(session, "turn/end", map[string]any{"turn": index + 1, "reason": map[string]any{"kind": "completed"}}); err != nil {
			t.Fatal(err)
		}
	}
	childSession, _ := engine.getSession(child)
	if payload := engine.hookPayload(childSession, HookBridgeConfig{Dialect: HookDialectClaudeCode}, hookPointInput{}); payload["transcript_path"] != "" {
		t.Fatalf("SQLite hook transcript = %#v", payload["transcript_path"])
	}
	if snapshots := engine.sessionQuerySnapshots(); len(snapshots) != 3 {
		t.Fatalf("SQLite snapshots = %#v", snapshots)
	} else {
		for _, snapshot := range snapshots {
			if !snapshot.persisted {
				t.Fatalf("session %q was not marked persisted", snapshot.header.ID)
			}
		}
	}
	var exported bytes.Buffer
	if err := engine.ExportSession(grandchild, &exported); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(exported.String(), `"delegationDepth":2`) {
		t.Fatalf("exported header = %s", exported.String())
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.List(context.Background()); err == nil {
		t.Fatal("Engine.Close did not close the injected store")
	}

	store, err = NewSQLiteSessionStore(path, SQLiteJournalDelete)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(WithConfig(cfg), WithSessionStore(store))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	checks := []struct {
		id, parent, origin, mode string
		depth                    int
	}{
		{parent, "", "", "", 0},
		{child, parent, "subagent", "continuable", 1},
		{grandchild, child, "subagent", "continuable", 2},
	}
	for _, check := range checks {
		session, err := reopened.getSession(check.id)
		if err != nil {
			t.Fatal(err)
		}
		session.mu.Lock()
		header := session.Header
		session.mu.Unlock()
		if header.ParentSession != check.parent || header.Origin != check.origin ||
			header.Mode != check.mode || header.DelegationDepth != check.depth {
			t.Fatalf("restored %q header = %#v", check.id, header)
		}
	}
}

type appendFailingSessionStore struct {
	SessionStore
	err error
}

func (s *appendFailingSessionStore) Append(context.Context, string, []Event) error { return s.err }

func TestEngineDoesNotPublishFailedPersistentAppend(t *testing.T) {
	backend, err := NewJSONLSessionStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("forced persistence failure")
	store := &appendFailingSessionStore{SessionStore: backend, err: want}
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	engine, err := New(WithConfig(cfg), WithSessionStore(store))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	id, err := engine.CreateSession(context.Background(), cfg.Workspace, "append-failure", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	if _, err := engine.appendEvent(session, "turn/start", map[string]any{"turn": 1}); !errors.Is(err, want) {
		t.Fatalf("append error = %v", err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if len(session.Events) != 0 {
		t.Fatalf("failed append mutated memory: %#v", session.Events)
	}
}
