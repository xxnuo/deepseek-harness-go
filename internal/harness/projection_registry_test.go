package harness

import (
	"context"
	"reflect"
	"strings"
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

func TestSessionProjectionRegistrySuppressesDeepEqualWireViewAndPublishesLaterChange(t *testing.T) {
	registry := NewSessionProjectionRegistry()
	type state struct {
		value  int
		secret int
	}
	_, err := registry.Register(ProjectionDefinition{
		Key: "test/view-gate", StateVersion: 1,
		Init: func() any { return state{} },
		Apply: func(current any, event Event) ProjectionResult {
			next := current.(state)
			switch event.Type {
			case "test/value":
				next.value++
			case "test/secret":
				next.secret++
			default:
				return ProjectionResult{State: current}
			}
			return ProjectionResult{State: next, Changed: true}
		},
		View: func(current any) any {
			return map[string]any{"value": current.(state).value}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{Header: SessionHeader{ID: "projection-view-gate"}}
	var changes []ProjectionChange
	registry.OnChanged(func(_ *Session, change ProjectionChange) { changes = append(changes, change) })
	for seq, typ := range []string{"test/value", "test/secret", "test/value"} {
		event := Event{Type: typ, Seq: SessionSeq(seq), Time: int64(seq + 1)}
		session.Events = append(session.Events, event)
		if _, err := registry.Drive(session, event); err != nil {
			t.Fatal(err)
		}
	}
	if len(changes) != 2 {
		t.Fatalf("changes = %#v, want first and final visible values only", changes)
	}
	if changes[0].Seq != 0 || changes[1].Seq != 2 {
		t.Fatalf("change seqs = %d,%d, want 0,2", changes[0].Seq, changes[1].Seq)
	}
	if !reflect.DeepEqual(changes[0].Value, map[string]any{"value": 1}) ||
		!reflect.DeepEqual(changes[1].Value, map[string]any{"value": 2}) {
		t.Fatalf("change values = %#v, want value 1 then 2", changes)
	}
}

func TestSessionProjectionRegistryRetriesAnApplyFailureWithoutSkippingTheEvent(t *testing.T) {
	registry := NewSessionProjectionRegistry()
	attempts := 0
	_, err := registry.Register(ProjectionDefinition{
		Key: "test/retry", StateVersion: 1,
		Init: func() any { return 0 },
		Apply: func(state any, event Event) ProjectionResult {
			if event.Type != "test/retry" {
				return ProjectionResult{State: state}
			}
			attempts++
			if attempts == 1 {
				panic("transient projection failure")
			}
			return ProjectionResult{State: state.(int) + 1, Changed: true}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{Header: SessionHeader{ID: "projection-retry"}}
	event := Event{Type: "test/retry", Seq: 0, Time: 1}
	session.Events = append(session.Events, event)
	if _, err := registry.Drive(session, event); err == nil || !strings.Contains(err.Error(), "transient projection failure") {
		t.Fatalf("first drive error = %v", err)
	}
	state, found, err := registry.StateOf(session, "test/retry")
	if err != nil {
		t.Fatal(err)
	}
	if !found || state.(int) != 1 || attempts != 2 {
		t.Fatalf("retry state=%#v found=%t attempts=%d", state, found, attempts)
	}
}

func TestSessionProjectionRegistryRetriesLateHistoryBeforeCurrentEvent(t *testing.T) {
	registry := NewSessionProjectionRegistry()
	attempts := 0
	_, err := registry.Register(ProjectionDefinition{
		Key: "test/late-history-retry", StateVersion: 1,
		Init: func() any { return 0 },
		Apply: func(state any, event Event) ProjectionResult {
			if event.Type == "test/history" {
				attempts++
				if attempts == 1 {
					panic("transient history failure")
				}
			}
			return ProjectionResult{State: state.(int) + 1, Changed: true}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{Header: SessionHeader{ID: "projection-late-history-retry"}}
	history := Event{Type: "test/history", Seq: 0, Time: 1}
	current := Event{Type: "test/current", Seq: 1, Time: 2}
	session.Events = append(session.Events, history, current)
	if _, err := registry.Drive(session, current); err == nil || !strings.Contains(err.Error(), "transient history failure") {
		t.Fatalf("first late drive error = %v", err)
	}
	if _, err := registry.Drive(session, current); err != nil {
		t.Fatalf("retry late drive error = %v", err)
	}
	state, found, err := registry.StateOf(session, "test/late-history-retry")
	if err != nil {
		t.Fatal(err)
	}
	if !found || state.(int) != 2 || attempts != 2 {
		t.Fatalf("late history retry state=%#v found=%t attempts=%d, want 2/true/2", state, found, attempts)
	}
}

func TestSessionProjectionRegistryRejectsGapWhenAdvancingCachedCell(t *testing.T) {
	registry := NewSessionProjectionRegistry()
	_, err := registry.Register(ProjectionDefinition{
		Key: "test/sparse-snapshot", StateVersion: 1,
		Init: func() any { return 0 },
		Apply: func(state any, _ Event) ProjectionResult {
			return ProjectionResult{State: state.(int) + 1, Changed: true}
		},
		View: func(state any) any { return state },
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{Header: SessionHeader{ID: "projection-sparse-snapshot"}, Events: []Event{{Type: "test/one", Seq: 0, Time: 1}}}
	if _, err := registry.Snapshot(session); err != nil {
		t.Fatal(err)
	}
	session.Events = append(session.Events, Event{Type: "test/three", Seq: 2, Time: 3})
	if _, err := registry.Snapshot(session); err == nil || !strings.Contains(err.Error(), "missing seq 1") {
		t.Fatalf("sparse snapshot error = %v", err)
	}
	session.Events = []Event{
		{Type: "test/one", Seq: 0, Time: 1},
		{Type: "test/two", Seq: 1, Time: 2},
		{Type: "test/three", Seq: 2, Time: 3},
	}
	snapshot, err := registry.Snapshot(session)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Values["test/sparse-snapshot"] != 3 {
		t.Fatalf("recovered snapshot=%#v, want value 3", snapshot)
	}
}

func TestSessionProjectionRegistryDriveRejectsGapBeforeCurrentEvent(t *testing.T) {
	registry := NewSessionProjectionRegistry()
	_, err := registry.Register(ProjectionDefinition{
		Key: "test/sparse-drive", StateVersion: 1,
		Init: func() any { return 0 },
		Apply: func(state any, _ Event) ProjectionResult {
			return ProjectionResult{State: state.(int) + 1, Changed: true}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{Header: SessionHeader{ID: "projection-sparse-drive"}}
	first := Event{Type: "test/one", Seq: 0, Time: 1}
	session.Events = append(session.Events, first)
	if _, err := registry.Drive(session, first); err != nil {
		t.Fatal(err)
	}
	gap := Event{Type: "test/three", Seq: 2, Time: 3}
	session.Events = append(session.Events, gap)
	if _, err := registry.Drive(session, gap); err == nil || !strings.Contains(err.Error(), "missing seq 1") {
		t.Fatalf("gap drive error = %v", err)
	}
	session.Events = []Event{
		first,
		{Type: "test/two", Seq: 1, Time: 2},
		gap,
	}
	if _, err := registry.Drive(session, gap); err != nil {
		t.Fatalf("repaired gap drive error = %v", err)
	}
	state, found, err := registry.StateOf(session, "test/sparse-drive")
	if err != nil || !found || state.(int) != 3 {
		t.Fatalf("state after repaired gap state=%#v found=%t err=%v", state, found, err)
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

type projectionRestoreTestState struct {
	Inherited int   `json:"inherited"`
	Seen      []int `json:"seen"`
}

func projectionRestoreTestDefinition(key string, visible bool, applies *int) ProjectionDefinition {
	definition := ProjectionDefinition{
		Key: key, StateVersion: 1,
		InitWithHeader: func(header SessionHeader) any {
			return projectionRestoreTestState{Inherited: header.SeedLength}
		},
		Apply: func(state any, event Event) ProjectionResult {
			if applies != nil {
				*applies++
			}
			next := state.(projectionRestoreTestState)
			next.Seen = append(next.Seen, int(event.Seq))
			return ProjectionResult{State: next, Changed: true}
		},
	}
	if visible {
		definition.View = func(state any) any { return state }
	}
	return definition
}

func TestSessionProjectionRegistryCheckpointRestoreAndHydrate(t *testing.T) {
	registry := NewSessionProjectionRegistry()
	if _, err := registry.Register(projectionRestoreTestDefinition("test/visible", true, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(projectionRestoreTestDefinition("test/host", false, nil)); err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{ID: "projection-restore", IsSeeded: true, SeedLength: 2}
	session := &Session{Header: header, InheritedEventCount: 2}
	event := Event{Type: "test/0", Seq: 0, Time: 1}
	session.Events = append(session.Events, event)
	if _, err := registry.Drive(session, event); err != nil {
		t.Fatal(err)
	}

	checkpoint, err := registry.Checkpoint(session)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint["test/visible"].Seq != 0 || checkpoint["test/host"].Seq != 0 {
		t.Fatalf("checkpoint rows = %#v", checkpoint)
	}
	detached := checkpoint["test/visible"].Value.(projectionRestoreTestState)
	detached.Seen[0] = 99
	snapshot, err := registry.Snapshot(session)
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.Values["test/visible"].(projectionRestoreTestState).Seen; !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("checkpoint mutation corrupted live state: %#v", got)
	}
	if _, found := snapshot.Values["test/host"]; found {
		t.Fatalf("host-only state leaked into snapshot: %#v", snapshot.Values)
	}

	floorRows := ProjectionCheckpoint{
		"test/visible": {Version: 1, Seq: 10, Value: projectionRestoreTestState{}},
		"test/host":    {Version: 1, Seq: 5, Value: projectionRestoreTestState{}},
	}
	if floor, ok := registry.RestoreFloor(floorRows); !ok || floor != 5 {
		t.Fatalf("restore floor = %d, %t, want 5, true", floor, ok)
	}
	floorRows["test/visible"] = ProjectionCheckpointRow{Version: 2, Seq: 10, Value: projectionRestoreTestState{}}
	if floor, ok := registry.RestoreFloor(floorRows); !ok || floor != 0 {
		t.Fatalf("mismatched restore floor = %d, %t, want 0, true", floor, ok)
	}

	malformed := ProjectionCheckpoint{
		"test/visible": {Version: 1, Seq: 0, Value: map[string]any{"inherited": 2, "seen": "bad"}},
	}
	if values, err := registry.ViewCheckpoint(malformed); err != nil || len(values) != 0 {
		t.Fatalf("malformed checkpoint view = %#v, %v", values, err)
	}

	rows := ProjectionCheckpoint{
		"test/visible": {Version: 1, Seq: 0, Value: projectionRestoreTestState{Inherited: 2, Seen: []int{0}}},
		"test/host":    {Version: 1, Seq: 0, Value: projectionRestoreTestState{Inherited: 2, Seen: []int{0}}},
	}
	tail := []Event{{Type: "test/1", Seq: 1, Time: 2}, {Type: "test/2", Seq: 2, Time: 3}}
	restored, err := registry.Restore(rows, tail, 1, header, 2)
	if err != nil {
		t.Fatal(err)
	}
	wantState := projectionRestoreTestState{Inherited: 2, Seen: []int{0, 1, 2}}
	if got := restored.Snapshot.Values["test/visible"]; !reflect.DeepEqual(got, wantState) {
		t.Fatalf("suffix restore visible state = %#v, want %#v", got, wantState)
	}
	if _, found := restored.Snapshot.Values["test/host"]; found {
		t.Fatalf("host-only restored state leaked: %#v", restored.Snapshot.Values)
	}
	if got := restored.Checkpoint["test/host"].Value; !reflect.DeepEqual(got, wantState) {
		t.Fatalf("host-only checkpoint state = %#v, want %#v", got, wantState)
	}

	mismatched := cloneProjectionCheckpointForTest(rows)
	mismatched["test/host"] = ProjectionCheckpointRow{Version: 2, Seq: 0, Value: projectionRestoreTestState{}}
	if _, err := registry.Restore(mismatched, tail, 1, header, 2); err == nil || !strings.Contains(err.Error(), "re-read from seq 0") {
		t.Fatalf("suffix restore mismatch error = %v", err)
	}
	full := append([]Event{{Type: "test/0", Seq: 0, Time: 1}}, tail...)
	refolded, err := registry.Restore(mismatched, full, 0, header, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := refolded.Checkpoint["test/host"].Value; !reflect.DeepEqual(got, wantState) {
		t.Fatalf("full refolded host state = %#v, want %#v", got, wantState)
	}

	hydrateRegistry := NewSessionProjectionRegistry()
	applies := 0
	if _, err := hydrateRegistry.Register(projectionRestoreTestDefinition("test/hydrate", true, &applies)); err != nil {
		t.Fatal(err)
	}
	hydratedSession := &Session{Header: header, InheritedEventCount: 2, Events: full}
	hydrateRows := ProjectionCheckpoint{
		"test/hydrate": {Version: 1, Seq: 0, Value: projectionRestoreTestState{Inherited: 2, Seen: []int{0}}},
	}
	first, err := hydrateRegistry.Hydrate(hydratedSession, hydrateRows, full, 0)
	if err != nil {
		t.Fatal(err)
	}
	if applies != 2 || first.AsOfSeq != 2 {
		t.Fatalf("first hydrate applies=%d snapshot=%#v", applies, first)
	}
	second, err := hydrateRegistry.Hydrate(hydratedSession, hydrateRows, full, 0)
	if err != nil {
		t.Fatal(err)
	}
	if applies != 2 || !reflect.DeepEqual(first, second) {
		t.Fatalf("repeat hydrate reapplied events: applies=%d first=%#v second=%#v", applies, first, second)
	}
}

func cloneProjectionCheckpointForTest(source ProjectionCheckpoint) ProjectionCheckpoint {
	clone := make(ProjectionCheckpoint, len(source))
	for key, row := range source {
		clone[key] = row
	}
	return clone
}

func TestProjectionCacheSkipsOnlyVersionMismatchedRows(t *testing.T) {
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
	stableDispose, err := e.SessionProjections().Register(ProjectionDefinition{
		Key: "test/cache-stable", StateVersion: 1,
		Init:  func() any { return 1 },
		Apply: func(state any, _ Event) ProjectionResult { return ProjectionResult{State: state} },
		View:  func(state any) any { return state },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stableDispose()
	driftDispose, err := e.SessionProjections().Register(ProjectionDefinition{
		Key: "test/cache-drift", StateVersion: 1,
		Init:  func() any { return 1 },
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
	inheritedEventCount := session.InheritedEventCount
	session.mu.Unlock()
	driftDispose()
	driftDispose, err = e.SessionProjections().Register(ProjectionDefinition{
		Key: "test/cache-drift", StateVersion: 2,
		Init:  func() any { return 2 },
		Apply: func(state any, _ Event) ProjectionResult { return ProjectionResult{State: state} },
		View:  func(state any) any { return state },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer driftDispose()
	snapshot, ok := e.projectionCache.medium.snapshot(e.SessionProjections(), header, inheritedEventCount)
	if !ok || snapshot.Values["test/cache-stable"] != 1 {
		t.Fatalf("version-scoped cache snapshot = %#v, ok=%t", snapshot, ok)
	}
	if _, found := snapshot.Values["test/cache-drift"]; found {
		t.Fatalf("version-mismatched row was served: %#v", snapshot.Values)
	}
}
