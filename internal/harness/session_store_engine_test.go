package harness

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
