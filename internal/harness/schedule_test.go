package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type scheduleFlushStore struct {
	SessionStore
	mu       sync.Mutex
	flushes  int
	outcomes []error
}

type scheduleFlushHandle struct {
	SessionHandle
	store *scheduleFlushStore
}

func (h *scheduleFlushHandle) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s := h.store
	s.mu.Lock()
	s.flushes++
	if len(s.outcomes) == 0 {
		s.mu.Unlock()
		return h.SessionHandle.Flush(ctx)
	}
	err := s.outcomes[0]
	s.outcomes = s.outcomes[1:]
	s.mu.Unlock()
	if err == nil {
		return h.SessionHandle.Flush(ctx)
	}
	return err
}

func (s *scheduleFlushStore) wrap(handle SessionHandle, err error) (SessionHandle, error) {
	if err != nil {
		return nil, err
	}
	return &scheduleFlushHandle{SessionHandle: handle, store: s}, nil
}

func (s *scheduleFlushStore) Create(ctx context.Context, header SessionHeader, inheritedEventCount SessionLogOffset) (SessionHandle, error) {
	return s.wrap(s.SessionStore.Create(ctx, header, inheritedEventCount))
}

func (s *scheduleFlushStore) Open(ctx context.Context, id string, access SessionAccess) (SessionHandle, error) {
	return s.wrap(s.SessionStore.Open(ctx, id, access))
}

func (s *scheduleFlushStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushes
}

func (s *scheduleFlushStore) setOutcomes(outcomes ...error) {
	s.mu.Lock()
	s.outcomes = append([]error(nil), outcomes...)
	s.mu.Unlock()
}

func newScheduleEngine(t *testing.T, store SessionStore) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = t.TempDir(), t.TempDir()
	cfg.Persist, cfg.ScheduleEnabled = true, true
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.RuntimeInvariants = &RuntimeInvariantConfig{}
	if store != nil {
		cfg.SessionStore = store
	}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func newScheduleFlushEngine(t *testing.T, outcomes ...error) (*Engine, *scheduleFlushStore) {
	t.Helper()
	base, err := NewJSONLSessionStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	store := &scheduleFlushStore{SessionStore: base, outcomes: append([]error(nil), outcomes...)}
	return newScheduleEngine(t, store), store
}

func stopScheduleRuntimeForTest(t *testing.T, e *Engine, id string) {
	t.Helper()
	e.scheduleRuntimeMu.Lock()
	runtime := e.scheduleRuntimes[id]
	e.scheduleRuntimeMu.Unlock()
	if runtime == nil {
		return
	}
	runtime.close()
	select {
	case <-runtime.done:
	case <-time.After(time.Second):
		t.Fatal("schedule runtime did not stop")
	}
}

func scheduleResultValue(t *testing.T, result ToolResult) any {
	t.Helper()
	if result.Error != nil || result.IsError || len(result.Content) != 1 {
		t.Fatalf("schedule tool result = %#v", result)
	}
	var value any
	if err := json.Unmarshal([]byte(result.Content[0].Text), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func scheduleSchemaStrings(value any) []string {
	switch values := value.(type) {
	case []string:
		return append([]string(nil), values...)
	case []any:
		result := make([]string, 0, len(values))
		for _, value := range values {
			text, _ := value.(string)
			result = append(result, text)
		}
		return result
	default:
		return nil
	}
}

func TestScheduleRulesReplayAndTimeZones(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC).UnixMilli()
	after, err := scheduleAfterRecord("schedule-1", " check logs ", 30, now)
	if err != nil || after.Prompt != "check logs" || after.ScheduledAt != "2026-08-05T12:00:30.000Z" {
		t.Fatalf("after = %#v, %v", after, err)
	}
	offset, err := scheduleAtRecord("schedule-2", "join", json.RawMessage(`"2026-08-06T09:00:00+08:00"`), now)
	if err != nil || offset.ScheduledAt != "2026-08-06T01:00:00.000Z" {
		t.Fatalf("offset = %#v, %v", offset, err)
	}
	local, err := scheduleAtRecord("schedule-3", "local", json.RawMessage(`{"date":"2026-08-07","time":"09:30:00","time_zone":"Asia/Shanghai"}`), now)
	if err != nil || local.ScheduledAt != "2026-08-07T01:30:00.000Z" {
		t.Fatalf("local = %#v, %v", local, err)
	}
	every, err := scheduleEveryRecord("schedule-4", "metrics", 300, now)
	if err != nil {
		t.Fatal(err)
	}
	accepted := time.Date(2026, 8, 5, 12, 16, 0, 0, time.UTC).UnixMilli()
	occurrence, next, err := resolveEverySchedule(every, accepted)
	if err != nil || occurrence != "2026-08-05T12:15:00.000Z" || next != "2026-08-05T12:20:00.000Z" {
		t.Fatalf("occurrence = %q, next = %q, err = %v", occurrence, next, err)
	}
	events := []Event{
		{Type: "schedule/change", Data: map[string]any{"version": 1, "operation": "create", "schedule": after.value()}},
		{Type: "schedule/change", Data: map[string]any{"version": 1, "operation": "create", "schedule": every.value()}},
		{Type: "schedule/change", Data: map[string]any{"version": 1, "operation": "dispatch", "id": after.ID}},
		{Type: "schedule/change", Data: map[string]any{"version": 1, "operation": "dispatch", "id": every.ID, "acceptedAt": "2026-08-05T12:16:00.000Z"}},
	}
	folded, err := foldScheduleEvents(events, 0)
	if err != nil || len(folded.active) != 1 || folded.active[0].ScheduledAt != next {
		t.Fatalf("folded = %#v, %v", folded, err)
	}
	if inherited, err := foldScheduleEvents(events, len(events)); err != nil || len(inherited.active) != 0 {
		t.Fatalf("fork suffix = %#v, %v", inherited, err)
	}
}

func TestScheduleProjectionMatchesReplayAndHonorsSeedBoundary(t *testing.T) {
	definition := scheduleProjectionDefinition()
	parent, err := scheduleAfterRecord("schedule-parent", "parent", 30, time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	record, err := scheduleAfterRecord("schedule-projection", "remind", 30, time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	events := []Event{
		{Type: "schedule/change", Seq: 0, Data: map[string]any{"version": 1, "operation": "create", "schedule": parent.value()}},
		{Type: "schedule/change", Seq: 1, Data: map[string]any{"version": 1, "operation": "create", "schedule": record.value()}},
		{Type: "schedule/change", Seq: 2, Data: map[string]any{"version": 1, "operation": "delete", "id": record.ID}},
		{Type: "turn/start", Seq: 3},
	}
	header := SessionHeader{ID: "schedule-projection", SeedLength: 1}
	state := definition.InitWithHeader(header).(scheduleProjectionState)
	for _, event := range events {
		state = definition.Apply(state, event).State.(scheduleProjectionState)
	}
	folded, err := foldScheduleEvents(events, header.SeedLength)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Active) != len(folded.active) || len(state.SeenIDs) != len(folded.seenOrder) {
		t.Fatalf("projection state = %#v, replay = %#v", state, folded)
	}
	if got := definition.View(state); len(got.([]map[string]any)) != 0 {
		t.Fatalf("projection view = %#v, want empty child suffix", got)
	}
	before := state
	if got := definition.Apply(state, events[3]); !reflect.DeepEqual(got.State, before) {
		t.Fatal("unrelated event changed schedule projection state")
	}
}

func TestScheduleProjectionIsConditionallyRegisteredOnRuntimeConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = t.TempDir(), t.TempDir()
	cfg.Persist = true
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.ScheduleEnabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(context.Background(), cfg.Workspace, "schedule-projection-toggle", "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := e.SessionProjectionSnapshot(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := snapshot.Values["schedule"]; found {
		t.Fatal("disabled Schedule projection was published")
	}
	next := e.Config()
	next.ScheduleEnabled = true
	if err := e.ApplyRuntimeConfig(next); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(session, "schedule/change", map[string]any{
		"version": 1, "operation": "create", "schedule": map[string]any{
			"id": "schedule-toggle", "kind": "after", "prompt": "toggle",
			"afterSeconds": 30, "scheduledAt": "2026-08-05T12:00:30.000Z",
		},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = e.SessionProjectionSnapshot(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if values, ok := snapshot.Values["schedule"].([]map[string]any); !ok || len(values) != 1 || values[0]["id"] != "schedule-toggle" {
		t.Fatalf("enabled Schedule projection = %#v", snapshot.Values["schedule"])
	}
	next.ScheduleEnabled = false
	if err := e.ApplyRuntimeConfig(next); err != nil {
		t.Fatal(err)
	}
	snapshot, err = e.SessionProjectionSnapshot(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := snapshot.Values["schedule"]; found {
		t.Fatal("disabled Schedule projection survived runtime config update")
	}
}

func TestScheduleDecisionOrdersOneShotsAndBatchesLatestRecurringOccurrences(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 17, 34, 0, time.UTC).UnixMilli()
	first := scheduleRecord{ID: "schedule-first", Kind: "after", Prompt: "first", AfterSeconds: 1, ScheduledAt: "2026-08-05T12:00:00.000Z"}
	second := scheduleRecord{ID: "schedule-second", Kind: "at", Prompt: "second", ScheduledAt: first.ScheduledAt}
	everyFast := scheduleRecord{ID: "schedule-fast", Kind: "every", Prompt: "fast", EverySeconds: 300, ScheduledAt: "2026-08-05T11:30:00.000Z"}
	everySlow := scheduleRecord{ID: "schedule-slow", Kind: "every", Prompt: "slow", EverySeconds: 600, ScheduledAt: "2026-08-05T11:49:00.000Z"}
	decision, err := decideSchedules(foldedSchedules{active: []scheduleRecord{first, everyFast, second, everySlow}}, now)
	if err != nil || decision.kind != "one-shot" || decision.oneShot.ID != first.ID {
		t.Fatalf("one-shot decision = %#v, %v", decision, err)
	}
	decision, err = decideSchedules(foldedSchedules{active: []scheduleRecord{everyFast, everySlow}}, now)
	if err != nil || decision.kind != "every" || len(decision.recurring) != 2 ||
		decision.recurring[0].occurrenceAt != "2026-08-05T12:15:00.000Z" ||
		decision.recurring[1].occurrenceAt != "2026-08-05T12:09:00.000Z" {
		t.Fatalf("recurring decision = %#v, %v", decision, err)
	}
	framing := renderRecurringSchedules(decision.recurring)
	if !strings.Contains(framing, `"schedule_id":"schedule-fast"`) || !strings.Contains(framing, `"occurrence_at":"2026-08-05T12:15:00.000Z"`) {
		t.Fatalf("recurring framing = %q", framing)
	}
}

func TestScheduleToolsCRUDAndNeverReuseIDs(t *testing.T) {
	e := newScheduleEngine(t, nil)
	id, err := e.CreateSession(context.Background(), t.TempDir(), "schedule-tools", "")
	if err != nil {
		t.Fatal(err)
	}
	created := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_create", id, map[string]any{"prompt": "later", "after_seconds": 3600})).(map[string]any)
	if created["id"] != "schedule-1" || created["state"] != "scheduled" {
		t.Fatalf("created = %#v", created)
	}
	listed := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_list", id, map[string]any{})).([]any)
	if len(listed) != 1 {
		t.Fatalf("listed = %#v", listed)
	}
	deleted := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_delete", id, map[string]any{"id": "schedule-1"})).(map[string]any)
	if deleted["deleted"] != true {
		t.Fatalf("deleted = %#v", deleted)
	}
	again := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_create", id, map[string]any{"prompt": "next", "after_seconds": 3600})).(map[string]any)
	if again["id"] != "schedule-2" {
		t.Fatalf("second create = %#v", again)
	}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "seed"}}}); err != nil {
		t.Fatal(err)
	}
	child, err := e.ForkSession(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	childList := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_list", child, map[string]any{})).([]any)
	if len(childList) != 0 {
		t.Fatalf("fork inherited schedules = %#v", childList)
	}
}

func TestScheduleRuntimeDispatchesRealAgentTurn(t *testing.T) {
	e := newScheduleEngine(t, nil)
	id, err := e.CreateSession(context.Background(), t.TempDir(), "schedule-runtime", "")
	if err != nil {
		t.Fatal(err)
	}
	created := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_create", id, map[string]any{"prompt": `quote " safely`, "after_seconds": 1})).(map[string]any)
	if created["id"] != "schedule-1" {
		t.Fatalf("created = %#v", created)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		session, _ := e.getSession(id)
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		running := session.Running
		session.mu.Unlock()
		dispatched, pluginPrompt, completed := false, false, false
		for _, event := range events {
			if event.Type == "schedule/change" {
				data, _ := event.Data.(map[string]any)
				dispatched = dispatched || data["operation"] == "dispatch"
			}
			if event.Type == "user/message" && eventSourceKind(event.Data) == "plugin" && strings.Contains(contentValueText(event.Data), "[SCHEDULE REMINDER]") {
				pluginPrompt = true
			}
			if event.Type == "turn/end" {
				data, _ := event.Data.(map[string]any)
				reason, _ := data["reason"].(map[string]any)
				completed = completed || reason["kind"] == "completed"
			}
		}
		if dispatched && pluginPrompt && completed && !running {
			listed := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_list", id, map[string]any{})).([]any)
			if len(listed) != 0 {
				t.Fatalf("completed one-shot remained active = %#v", listed)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("scheduled agent turn did not complete")
}

func TestScheduleToolsUsePersistencePreflightAndBarrier(t *testing.T) {
	e, store := newScheduleFlushEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "schedule-persistence", "")
	if err != nil {
		t.Fatal(err)
	}
	stopScheduleRuntimeForTest(t, e, id)
	created := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_create", id, map[string]any{
		"prompt": "persist me", "after_seconds": 3600,
	})).(map[string]any)
	if created["id"] != "schedule-1" || store.count() != 2 {
		t.Fatalf("create=%#v flushes=%d", created, store.count())
	}
	listed := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_list", id, map[string]any{})).([]any)
	if len(listed) != 1 || store.count() != 3 {
		t.Fatalf("list=%#v flushes=%d", listed, store.count())
	}
	deleted := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_delete", id, map[string]any{"id": "schedule-1"})).(map[string]any)
	if deleted["deleted"] != true || store.count() != 5 {
		t.Fatalf("delete=%#v flushes=%d", deleted, store.count())
	}
}

func TestScheduleToolSchemasExposeClosedPersistenceUnion(t *testing.T) {
	e := newScheduleEngine(t, nil)
	for _, schema := range e.ListTools() {
		if schema.Name != "schedule_create" {
			continue
		}
		variants, _ := schema.Output["oneOf"].([]any)
		for _, raw := range variants {
			variant, _ := raw.(map[string]any)
			properties, _ := variant["properties"].(map[string]any)
			code, _ := properties["code"].(map[string]any)
			if code["const"] == "persistence_uncertain" {
				operation, _ := properties["operation"].(map[string]any)
				if strings.Join(scheduleSchemaStrings(operation["enum"]), ",") != "create,list,delete" {
					t.Fatalf("persistence operation schema = %#v", operation)
				}
				return
			}
		}
		t.Fatalf("schedule_create output = %#v", schema.Output)
	}
	t.Fatal("missing schedule_create schema")
}

func TestSchedulePersistenceFailuresReturnUncertaintyAtExactBoundary(t *testing.T) {
	t.Run("create preflight", func(t *testing.T) {
		e, _ := newScheduleFlushEngine(t, errors.New("disk unavailable"))
		id, err := e.CreateSession(context.Background(), e.Config().Workspace, "schedule-preflight", "")
		if err != nil {
			t.Fatal(err)
		}
		stopScheduleRuntimeForTest(t, e, id)
		value := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_create", id, map[string]any{
			"prompt": "must not append", "after_seconds": 3600,
		})).(map[string]any)
		if value["code"] != "persistence_uncertain" || value["operation"] != "create" || value["id"] != nil {
			t.Fatalf("preflight value = %#v", value)
		}
		s := mustSession(t, e, id)
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, event := range s.Events {
			if event.Type == "schedule/change" {
				t.Fatalf("preflight failure appended %#v", event)
			}
		}
	})

	t.Run("create barrier", func(t *testing.T) {
		e, _ := newScheduleFlushEngine(t, nil, errors.New("disk unavailable"))
		id, err := e.CreateSession(context.Background(), e.Config().Workspace, "schedule-barrier", "")
		if err != nil {
			t.Fatal(err)
		}
		stopScheduleRuntimeForTest(t, e, id)
		value := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_create", id, map[string]any{
			"prompt": "already appended", "after_seconds": 3600,
		})).(map[string]any)
		if value["code"] != "persistence_uncertain" || value["operation"] != "create" || value["id"] != "schedule-1" {
			t.Fatalf("barrier value = %#v", value)
		}
		listed := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_list", id, map[string]any{})).([]any)
		if len(listed) != 1 || listed[0].(map[string]any)["id"] != "schedule-1" {
			t.Fatalf("clarified list = %#v", listed)
		}
	})
}

func TestScheduleTransactionCancellationWhileWaitingDoesNotAppend(t *testing.T) {
	e := newScheduleEngine(t, nil)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "schedule-cancel", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	s.scheduleMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	tool := registeredTool(t, e, "schedule_create")
	go func() {
		_, executeErr := tool.Execute(ctx, ToolCall{
			Name: "schedule_create", SessionID: id,
			Arguments: json.RawMessage(`{"prompt":"cancelled","after_seconds":3600}`),
		})
		done <- executeErr
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled transaction error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled schedule transaction remained in FIFO")
	}
	s.scheduleMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.Events {
		if event.Type == "schedule/change" {
			t.Fatalf("cancelled transaction appended %#v", event)
		}
	}
}

func TestScheduleInvariantAndRootOwnership(t *testing.T) {
	e := newScheduleEngine(t, nil)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "schedule-invariant", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	if _, err := e.appendEvent(s, "schedule/change", map[string]any{
		"version": 1, "operation": "delete", "id": "missing",
	}); err == nil {
		t.Fatal("invalid schedule transition passed the package invariant")
	} else {
		var invariant *InvariantError
		if !errors.As(err, &invariant) || invariant.PackageName != invariantPackageSchedule {
			t.Fatalf("schedule invariant error = %v", err)
		}
	}

	child, err := e.CreateSession(context.Background(), e.Config().Workspace, "schedule-child", "")
	if err != nil {
		t.Fatal(err)
	}
	childSession := mustSession(t, e, child)
	stopScheduleRuntimeForTest(t, e, child)
	childSession.mu.Lock()
	childSession.Header.Origin = "subagent"
	childSession.mu.Unlock()
	for _, name := range []string{"schedule_create", "schedule_list", "schedule_delete"} {
		visible, err := e.toolVisibleForSession(childSession, name)
		if err != nil || visible {
			t.Fatalf("subagent tool %s visible=%v err=%v", name, visible, err)
		}
	}
	e.startScheduleRuntime(childSession)
	e.scheduleRuntimeMu.Lock()
	runtime := e.scheduleRuntimes[child]
	e.scheduleRuntimeMu.Unlock()
	if runtime != nil {
		t.Fatal("schedule runtime started for subagent")
	}
	value := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_list", child, map[string]any{})).(map[string]any)
	if value["code"] != "internal_error" {
		t.Fatalf("cross-owner schedule result = %#v", value)
	}
}

func TestSchedulePresetCompositionExposesTools(t *testing.T) {
	presetRoot := t.TempDir()
	presetDir := filepath.Join(presetRoot, "schedule-only")
	if err := os.Mkdir(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte("- id: schedule\n  name: '@deepseek-ai/dsh-schedule'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir = t.TempDir(), t.TempDir(), presetRoot
	cfg.Persist, cfg.ScheduleEnabled = true, true
	cfg.Provider, cfg.Model = "echo", "echo"
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(context.Background(), cfg.Workspace, "schedule-preset", "schedule-only")
	if err != nil {
		t.Fatal(err)
	}
	tools, err := e.toolsForSession(mustSession(t, e, id))
	if err != nil {
		t.Fatal(err)
	}
	visible := map[string]bool{}
	for _, tool := range tools {
		visible[tool.Name] = true
	}
	for _, name := range []string{"schedule_create", "schedule_list", "schedule_delete"} {
		if !visible[name] {
			t.Fatalf("preset omitted %s: %#v", name, visible)
		}
	}
}

func TestScheduleRuntimePersistenceFencesDoNotLoseOrRepeatDueWork(t *testing.T) {
	waitForDispatch := func(t *testing.T, e *Engine, id string, provider *promptCaptureProvider) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			s := mustSession(t, e, id)
			s.mu.Lock()
			running := s.Running
			dispatches := 0
			for _, event := range s.Events {
				if event.Type == "schedule/change" {
					data, _ := event.Data.(map[string]any)
					if data["operation"] == "dispatch" {
						dispatches++
					}
				}
			}
			s.mu.Unlock()
			if dispatches == 1 && len(provider.snapshot()) == 1 && !running {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("due schedule did not settle exactly once")
	}
	appendOverdue := func(t *testing.T, e *Engine, id string) {
		t.Helper()
		record, err := scheduleAfterRecord("schedule-1", "due reminder", 1, time.Now().Add(-2*time.Second).UnixMilli())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.appendEvent(mustSession(t, e, id), "schedule/change", map[string]any{
			"version": scheduleChangeVersion, "operation": "create", "schedule": record.value(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("preflight retry", func(t *testing.T) {
		e, store := newScheduleFlushEngine(t)
		provider := &promptCaptureProvider{}
		e.RegisterProvider(provider)
		id, err := e.CreateSession(context.Background(), e.Config().Workspace, "schedule-runtime-preflight", "")
		if err != nil {
			t.Fatal(err)
		}
		if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: "model-a"}); err != nil {
			t.Fatal(err)
		}
		stopScheduleRuntimeForTest(t, e, id)
		appendOverdue(t, e, id)
		store.setOutcomes(errors.New("disk unavailable"))
		e.startScheduleRuntime(mustSession(t, e, id))
		time.Sleep(30 * time.Millisecond)
		if len(provider.snapshot()) != 0 {
			t.Fatal("runtime dispatched without a successful preflight")
		}
		store.setOutcomes(nil, nil)
		e.scheduleWake(id)
		waitForDispatch(t, e, id, provider)
	})

	t.Run("barrier no repeat", func(t *testing.T) {
		e, store := newScheduleFlushEngine(t)
		provider := &promptCaptureProvider{}
		e.RegisterProvider(provider)
		id, err := e.CreateSession(context.Background(), e.Config().Workspace, "schedule-runtime-barrier", "")
		if err != nil {
			t.Fatal(err)
		}
		if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: "model-a"}); err != nil {
			t.Fatal(err)
		}
		stopScheduleRuntimeForTest(t, e, id)
		appendOverdue(t, e, id)
		store.setOutcomes(nil, errors.New("disk unavailable"), nil)
		e.startScheduleRuntime(mustSession(t, e, id))
		waitForDispatch(t, e, id, provider)
		e.scheduleWake(id)
		time.Sleep(30 * time.Millisecond)
		if len(provider.snapshot()) != 1 {
			t.Fatalf("barrier failure repeated reminder: %d requests", len(provider.snapshot()))
		}
	})
}

func TestScheduleRuntimeDoesNotCheckpointUnrelatedIdleSession(t *testing.T) {
	e, store := newScheduleFlushEngine(t)
	if _, err := e.CreateSession(context.Background(), e.Config().Workspace, "schedule-empty-runtime", ""); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if store.count() != 0 {
		t.Fatalf("unrelated session flushes = %d", store.count())
	}
}
