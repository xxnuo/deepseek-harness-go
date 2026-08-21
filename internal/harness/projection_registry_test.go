package harness

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestSessionProjectionRegistryUsesExplicitChangedResultAndRefCounts(t *testing.T) {
	registry := NewSessionProjectionRegistry()
	definition := ProjectionDefinition{
		Key: "test/equal", StateVersion: 1,
		Init: func() any { return map[string]any{"value": 1} },
		Apply: func(state any, event Event) ProjectionResult {
			if event.Type != "test/change" {
				return ProjectionResult{State: state}
			}
			return ProjectionResult{State: map[string]any{"value": 1}, Changed: true}
		},
		View: func(state any) any { return state },
	}
	first, err := registry.Register(definition)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Register(definition)
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{Header: SessionHeader{ID: "projection-explicit"}}
	var changes []ProjectionChange
	registry.OnChanged(func(changedSession *Session, change ProjectionChange) {
		if changedSession != session {
			t.Fatalf("changed session = %p, want %p", changedSession, session)
		}
		changes = append(changes, change)
	})
	session.Events = append(session.Events, Event{Type: "test/change", Seq: 0, Time: 1})
	if _, err := registry.Drive(session, session.Events[0]); err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Key != "test/equal" || changes[0].Seq != 0 {
		t.Fatalf("changes = %#v", changes)
	}
	first()
	if snapshot, err := registry.Snapshot(session); err != nil || snapshot.Values["test/equal"] == nil {
		t.Fatalf("snapshot after first dispose = %#v, %v", snapshot, err)
	}
	second()
	if snapshot, err := registry.Snapshot(session); err != nil || len(snapshot.Values) != 0 {
		t.Fatalf("snapshot after final dispose = %#v, %v", snapshot, err)
	}
}

func TestEngineProjectionFeedSkipsUnchangedEvents(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "projection-feed", "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := e.SubscribeMux(ctx)
	for _, item := range []struct {
		typ  string
		data map[string]any
	}{
		{"command/done", map[string]any{"commandId": "unmatched", "kind": "success"}},
		{"assistant/message", map[string]any{}},
		{"tool/result", map[string]any{}},
		{"turn/end", map[string]any{"turn": 1}},
	} {
		if _, err := e.appendEvent(session, item.typ, item.data); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case frame := <-frames:
		if frame["type"] == "session/projection" {
			t.Fatalf("unchanged event emitted projection frame: %#v", frame)
		}
	default:
	}
}

func TestDynamicCordisSessionProjectionRegisterStateAndDispose(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "dynamic-projection", "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := e.SubscribeMux(ctx)
	pluginID, runID := runDynamicBuiltinPlugin(t, e, id, "prj", `
return {
  inject: ['sessionProjections'],
  apply(ctx) {
    ctx.sessionProjections.register({
      key: 'dynamic/count',
      stateVersion: 1,
      init: () => ({ count: 0 }),
      apply: (state, event) => event.type === 'feedback/record'
        ? { count: state.count + 1 }
        : state,
      wire: { view: state => state },
    })
    harness.handle('state', args => ctx.sessionProjections.stateOf(args.sessionId, 'dynamic/count'))
    harness.handle('snapshot', args => ctx.sessionProjections.snapshot(args.sessionId))
  },
}`)

	snapshot, err := e.SessionProjectionSnapshot(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.Values["dynamic/count"]; !reflect.DeepEqual(got, map[string]any{"count": float64(0)}) {
		t.Fatalf("initial dynamic projection = %#v", got)
	}
	session, _ := e.getSession(id)
	if _, err := e.appendEvent(session, "feedback/record", map[string]any{"value": true}); err != nil {
		t.Fatal(err)
	}
	frame := waitProjection(t, frames, "dynamic/count")
	if got := frame["value"]; !reflect.DeepEqual(got, map[string]any{"count": float64(1)}) {
		t.Fatalf("dynamic projection frame = %#v", frame)
	}
	state := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "state", map[string]any{"sessionId": id})
	if !state.OK || !reflect.DeepEqual(state.Value, map[string]any{"count": float64(1)}) {
		t.Fatalf("dynamic stateOf = %#v", state)
	}
	if stopped, err := e.DynamicCordisStop(id, pluginID); err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	snapshot, err = e.SessionProjectionSnapshot(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := snapshot.Values["dynamic/count"]; found {
		t.Fatalf("dynamic projection survived stop: %#v", snapshot.Values)
	}
}

func TestProjectionCacheRejectsDifferentRegistryComposition(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.Persist = true
	cfg.SessionTitleLLM.Enabled = false
	cfg.SessionProjectionCache = &SessionProjectionCacheConfig{WriteEveryEvents: 100, WriteInterval: time.Hour}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "projection-cache-composition", "")
	if err != nil {
		t.Fatal(err)
	}
	dispose, err := e.SessionProjections().Register(ProjectionDefinition{
		Key: "test/cache-composition", StateVersion: 1,
		Init:  func() any { return nil },
		Apply: func(state any, _ Event) ProjectionResult { return ProjectionResult{State: state} },
		View:  func(state any) any { return state },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.SessionProjectionSnapshot(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession(id)
	session.mu.Lock()
	header := session.Header
	lastSeq := len(session.Events) - 1
	session.mu.Unlock()
	dispose()
	if _, ok := e.projectionCache.medium.snapshot(header, lastSeq, true, e.SessionProjections().Signature()); ok {
		t.Fatal("cache row from the old projection composition was accepted")
	}
}
