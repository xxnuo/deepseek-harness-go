package harness

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

const cordisEventRecorder = `
const names = [
  'settings/updated', 'domain/changed', 'goal/changed',
  'subagent/start', 'subagent/end', 'subagent/provider-added', 'subagent/provider-removed',
  'session/disposed',
  'workflow/start', 'workflow/phase', 'workflow/log',
  'workflow/agent-start', 'workflow/agent-end', 'workflow/end',
]
const seen = Object.fromEntries(names.map(name => [name, []]))
harness.handle('seen', name => seen[name] || [])
return { apply(ctx) {
  for (const name of names) ctx.on(name, (...args) => { seen[name].push(args) })
} }
`

type cordisEventSubagentProvider struct{}

func (*cordisEventSubagentProvider) Name() string { return "event-test" }
func (*cordisEventSubagentProvider) Capabilities() SubagentCapabilities {
	return NoSubagentStartCapabilities()
}
func (*cordisEventSubagentProvider) InheritsParentContext() bool { return false }
func (*cordisEventSubagentProvider) Start(context.Context, SubagentStartRequest) (*SubagentRun, error) {
	return newSubagentRun("external-child", nil, nil), nil
}

func runCordisEventRecorder(t *testing.T, e *Engine, sessionID, prefix string) (string, string) {
	t.Helper()
	return runDynamicBuiltinPlugin(t, e, sessionID, prefix, cordisEventRecorder)
}

func cordisRecordedEvents(t *testing.T, e *Engine, pluginID, runID, name string) []any {
	t.Helper()
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "seen", name)
	if !result.OK {
		t.Fatalf("read %s events = %#v", name, result)
	}
	events, ok := result.Value.([]any)
	if !ok {
		t.Fatalf("%s events = %#v", name, result.Value)
	}
	return events
}

func waitCordisRecordedEvents(t *testing.T, e *Engine, pluginID, runID, name string, count int) []any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		events := cordisRecordedEvents(t, e, pluginID, runID, name)
		if len(events) >= count {
			return events
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s events = %#v, want at least %d", name, events, count)
		}
		time.Sleep(time.Millisecond)
	}
}

func cordisEventArgs(t *testing.T, events []any, index int) []any {
	t.Helper()
	if index >= len(events) {
		t.Fatalf("event index %d outside %#v", index, events)
	}
	args, ok := events[index].([]any)
	if !ok {
		t.Fatalf("event %d args = %#v", index, events[index])
	}
	return args
}

func cordisJSONValue(t *testing.T, value any) any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		t.Fatal(err)
	}
	return normalized
}

func requireCordisArgs(t *testing.T, got []any, want ...any) {
	t.Helper()
	normalized := cordisJSONValue(t, want).([]any)
	if !reflect.DeepEqual(got, normalized) {
		t.Fatalf("event args = %#v, want %#v", got, normalized)
	}
}

func TestDynamicCordisSettingsUpdatedProducerUsesResolvedValues(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", "workspace-write")
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "settings-events", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runCordisEventRecorder(t, e, sessionID, "set")

	if _, rpcErr := e.settingsUpdate("permission", map[string]any{"defaultPreset": "workspace-write"}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if events := cordisRecordedEvents(t, e, pluginID, runID, "settings/updated"); len(events) != 0 {
		t.Fatalf("resolved no-op emitted settings/updated: %#v", events)
	}
	if _, rpcErr := e.settingsMutate("permission", []any{map[string]any{
		"op": "set", "path": []any{"defaultPreset"}, "value": "danger-full-access",
	}}, nil); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	events := cordisRecordedEvents(t, e, pluginID, runID, "settings/updated")
	if len(events) != 1 {
		t.Fatalf("settings/updated events = %#v", events)
	}
	requireCordisArgs(t, cordisEventArgs(t, events, 0),
		"permission",
		map[string]any{"defaultPreset": "danger-full-access"},
		map[string]any{"defaultPreset": "workspace-write"},
		"update",
	)
	if _, rpcErr := e.settingsUpdate("permission", map[string]any{"defaultPreset": "danger-full-access"}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if count := len(cordisRecordedEvents(t, e, pluginID, runID, "settings/updated")); count != 1 {
		t.Fatalf("raw no-op emitted settings/updated: count=%d", count)
	}
}

func TestDynamicCordisDomainChangedProducerUsesCommittedWireShape(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Persist = false
	cfg.SessionTitleLLM.Enabled = false
	cfg.Storage = &StorageRuntimeConfig{
		JSON:   &JSONStorageConfig{Root: filepath.Join(cfg.DataDir, "storage")},
		Domain: &StorageDomainConfig{Backend: "json"},
	}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	sessionID, err := e.CreateSession(t.Context(), cfg.Workspace, "domain-events", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runCordisEventRecorder(t, e, sessionID, "dom")
	domain, err := e.StorageDomain().Open(t.Context(), storageDomainSpec())
	if err != nil {
		t.Fatal(err)
	}
	table, err := domain.Table("items")
	if err != nil {
		t.Fatal(err)
	}
	value := storageTestItem{Label: "landed", Count: 2}
	if err := table.Put(t.Context(), "item-1", value); err != nil {
		t.Fatal(err)
	}
	if deleted, err := table.Delete(t.Context(), "item-1"); err != nil || !deleted {
		t.Fatalf("delete = %v, %v", deleted, err)
	}
	events := cordisRecordedEvents(t, e, pluginID, runID, "domain/changed")
	if len(events) != 2 {
		t.Fatalf("domain/changed events = %#v", events)
	}
	requireCordisArgs(t, cordisEventArgs(t, events, 0), map[string]any{
		"domain": "demo", "table": "items", "key": "item-1", "operation": "put", "value": value,
	})
	requireCordisArgs(t, cordisEventArgs(t, events, 1), map[string]any{
		"domain": "demo", "table": "items", "key": "item-1", "operation": "deleted",
	})
}

func TestDynamicCordisGoalChangedProducerIsScopedAndComplete(t *testing.T) {
	e := newIntegrationEngine(t)
	first, err := e.CreateSession(t.Context(), e.Config().Workspace, "goal-events-a", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.CreateSession(t.Context(), e.Config().Workspace, "goal-events-b", "")
	if err != nil {
		t.Fatal(err)
	}
	firstPlugin, firstRun := runCordisEventRecorder(t, e, first, "goa")
	secondPlugin, secondRun := runCordisEventRecorder(t, e, second, "gob")

	created, err := e.GoalMutation(first, "create", "ship it", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	ref := created["ref"].(map[string]any)
	goalID := ref["id"].(string)
	firstSession, _ := e.getSession(first)
	goal, err := e.GetGoal(first)
	if err != nil {
		t.Fatal(err)
	}
	events := cordisRecordedEvents(t, e, firstPlugin, firstRun, "goal/changed")
	if len(events) != 1 {
		t.Fatalf("goal/changed create events = %#v", events)
	}
	requireCordisArgs(t, cordisEventArgs(t, events, 0), map[string]any{
		"agent": map[string]any{
			"id": first, "options": firstSession.Model, "session": dynamicSessionView(firstSession), "status": "idle",
		},
		"change": map[string]any{"operation": "create", "ref": ref, "goal": goal},
	})

	mutations := []struct {
		op        string
		objective string
		revision  int
		maxRounds int
	}{
		{op: "edit", objective: "ship all of it", revision: 1, maxRounds: 4},
		{op: "pause", revision: 2},
		{op: "resume", revision: 3},
	}
	for _, mutation := range mutations {
		if _, err := e.GoalMutation(first, mutation.op, mutation.objective, mutation.revision, mutation.maxRounds); err != nil {
			t.Fatalf("%s: %v", mutation.op, err)
		}
	}
	if _, err := e.goalMutation(first, goalID, "block", "", "need input", 4, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(first, "resume", "", 5, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(first, "complete", "", 6, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(first, "clear", "", 7, 0); err != nil {
		t.Fatal(err)
	}
	events = cordisRecordedEvents(t, e, firstPlugin, firstRun, "goal/changed")
	operations := make([]string, 0, len(events))
	for index := range events {
		args := cordisEventArgs(t, events, index)
		payload := args[0].(map[string]any)
		change := payload["change"].(map[string]any)
		operations = append(operations, change["operation"].(string))
	}
	wantOperations := []string{"create", "edit", "pause", "resume", "block", "resume", "complete", "clear"}
	if !reflect.DeepEqual(operations, wantOperations) {
		t.Fatalf("goal operations = %#v, want %#v", operations, wantOperations)
	}
	clearArgs := cordisEventArgs(t, events, len(events)-1)
	clearChange := clearArgs[0].(map[string]any)["change"].(map[string]any)
	requireCordisArgs(t, []any{clearChange}, map[string]any{
		"operation": "clear", "ref": map[string]any{"id": goalID, "revision": 8},
	})
	if other := cordisRecordedEvents(t, e, secondPlugin, secondRun, "goal/changed"); len(other) != 0 {
		t.Fatalf("cross-session goal events = %#v", other)
	}

	secondCreated, err := e.GoalMutation(second, "create", "one round", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	secondRef := secondCreated["ref"].(map[string]any)
	e.mu.Lock()
	state := e.goals[second]
	state.RoundsStarted = state.MaxRounds
	e.goals[second] = state
	e.mu.Unlock()
	secondSession, _ := e.getSession(second)
	if scheduled, err := e.scheduleGoalRound(secondSession); err != nil || scheduled {
		t.Fatalf("round-limit schedule = %v, %v", scheduled, err)
	}
	secondEvents := cordisRecordedEvents(t, e, secondPlugin, secondRun, "goal/changed")
	if len(secondEvents) != 2 {
		t.Fatalf("round-limit goal events = %#v", secondEvents)
	}
	blocked := cordisEventArgs(t, secondEvents, 1)[0].(map[string]any)["change"].(map[string]any)
	blockedGoal := blocked["goal"].(map[string]any)
	if blocked["operation"] != "block" || blockedGoal["phase"] != "blocked" ||
		blockedGoal["blockedReason"].(map[string]any)["code"] != "round-limit" ||
		blocked["ref"].(map[string]any)["id"] != secondRef["id"] {
		t.Fatalf("round-limit payload = %#v", blocked)
	}
}

func TestDynamicCordisSubagentLifecycleProducersAreScopedAndOrdered(t *testing.T) {
	e := newIntegrationEngine(t)
	first, err := e.CreateSession(t.Context(), e.Config().Workspace, "subagent-events-a", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.CreateSession(t.Context(), e.Config().Workspace, "subagent-events-b", "")
	if err != nil {
		t.Fatal(err)
	}
	firstPlugin, firstRun := runCordisEventRecorder(t, e, first, "saa")
	secondPlugin, secondRun := runCordisEventRecorder(t, e, second, "sab")
	provider := &cordisEventSubagentProvider{}
	if err := e.RegisterSubagentProvider(provider); err != nil {
		t.Fatal(err)
	}
	for _, target := range []struct{ plugin, run string }{{firstPlugin, firstRun}, {secondPlugin, secondRun}} {
		added := cordisRecordedEvents(t, e, target.plugin, target.run, "subagent/provider-added")
		if len(added) != 1 {
			t.Fatalf("provider-added events = %#v", added)
		}
		requireCordisArgs(t, cordisEventArgs(t, added, 0), SubagentProviderInfo{
			Name: provider.Name(), Capabilities: provider.Capabilities(),
			InheritsParentContext: provider.InheritsParentContext(),
		})
	}
	run, err := e.StartSubagent(t.Context(), provider.Name(), SubagentStartRequest{ParentSessionID: first})
	if err != nil {
		t.Fatal(err)
	}
	starts := cordisRecordedEvents(t, e, firstPlugin, firstRun, "subagent/start")
	if len(starts) != 1 {
		t.Fatalf("subagent/start events = %#v", starts)
	}
	start := cordisEventArgs(t, starts, 0)[0].(map[string]any)
	if start["provider"] != provider.Name() || start["id"] != run.ID || start["local"] != false {
		t.Fatalf("subagent/start payload = %#v", start)
	}
	if other := cordisRecordedEvents(t, e, secondPlugin, secondRun, "subagent/start"); len(other) != 0 {
		t.Fatalf("cross-session subagent/start = %#v", other)
	}

	run.settle(SubagentResult{
		Output: []ContentBlock{{Type: "text", Text: "done"}}, StopReason: SubagentCompleted,
	})
	ends := waitCordisRecordedEvents(t, e, firstPlugin, firstRun, "subagent/end", 1)
	end := cordisEventArgs(t, ends, 0)[0].(map[string]any)
	requireCordisArgs(t, []any{end}, map[string]any{
		"runId": start["runId"], "provider": provider.Name(), "id": run.ID, "local": false,
		"stopReason":           SubagentCompleted,
		"lastAssistantMessage": []ContentBlock{{Type: "text", Text: "done"}},
	})
	if other := cordisRecordedEvents(t, e, secondPlugin, secondRun, "subagent/end"); len(other) != 0 {
		t.Fatalf("cross-session subagent/end = %#v", other)
	}
	if !e.UnregisterSubagentProvider(provider.Name()) {
		t.Fatal("provider was not removed")
	}
	for _, target := range []struct{ plugin, run string }{{firstPlugin, firstRun}, {secondPlugin, secondRun}} {
		removed := cordisRecordedEvents(t, e, target.plugin, target.run, "subagent/provider-removed")
		if len(removed) != 1 {
			t.Fatalf("provider-removed events = %#v", removed)
		}
		requireCordisArgs(t, cordisEventArgs(t, removed, 0), provider.Name())
	}
}

func TestDynamicCordisSDKSessionDisposedProducerIsScopedAndIdempotent(t *testing.T) {
	e := newIntegrationEngine(t)
	first, err := e.CreateSession(t.Context(), e.Config().Workspace, "disposed-events-a", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.CreateSession(t.Context(), e.Config().Workspace, "disposed-events-b", "")
	if err != nil {
		t.Fatal(err)
	}
	firstPlugin, firstRun := runCordisEventRecorder(t, e, first, "dsa")
	secondPlugin, secondRun := runCordisEventRecorder(t, e, second, "dsb")
	if err := detachSDKSession(e, first); err != nil {
		t.Fatal(err)
	}
	firstSession, _ := e.getSession(first)
	events := cordisRecordedEvents(t, e, firstPlugin, firstRun, "session/disposed")
	if len(events) != 1 {
		t.Fatalf("session/disposed events = %#v", events)
	}
	requireCordisArgs(t, cordisEventArgs(t, events, 0), dynamicSessionView(firstSession))
	if other := cordisRecordedEvents(t, e, secondPlugin, secondRun, "session/disposed"); len(other) != 0 {
		t.Fatalf("cross-session disposed events = %#v", other)
	}
	if err := detachSDKSession(e, first); err != nil {
		t.Fatal(err)
	}
	if count := len(cordisRecordedEvents(t, e, firstPlugin, firstRun, "session/disposed")); count != 1 {
		t.Fatalf("session/disposed count = %d, want 1", count)
	}
}

func TestDynamicCordisWorkflowLifecycleProducersRunRealScript(t *testing.T) {
	e, sessionID := newWorkflowEngine(t)
	pluginID, runID := runCordisEventRecorder(t, e, sessionID, "wfl")
	executeWorkflowTestTool(t, e, "workflow", sessionID, `{
		"meta":{"name":"event-flow","description":"event coverage"},
		"script":"phase('Inspect'); log('working'); const reply = await agent('one', {label:'child', phase:'Build'}); return {reply};"
	}`)

	names := []string{
		"workflow/start", "workflow/phase", "workflow/log", "workflow/agent-start", "workflow/agent-end", "workflow/end",
	}
	seen := make(map[string][]any, len(names))
	for _, name := range names {
		seen[name] = cordisRecordedEvents(t, e, pluginID, runID, name)
		if len(seen[name]) != 1 {
			t.Fatalf("%s events = %#v", name, seen[name])
		}
	}
	startInfo := cordisEventArgs(t, seen["workflow/start"], 0)[0].(map[string]any)
	meta := startInfo["meta"].(map[string]any)
	if startInfo["id"] == "" || meta["name"] != "event-flow" || meta["description"] != "event coverage" {
		t.Fatalf("workflow/start payload = %#v", startInfo)
	}
	requireCordisArgs(t, cordisEventArgs(t, seen["workflow/phase"], 0), startInfo, "Inspect")
	requireCordisArgs(t, cordisEventArgs(t, seen["workflow/log"], 0), startInfo, "working")
	agentStartArgs := cordisEventArgs(t, seen["workflow/agent-start"], 0)
	if !reflect.DeepEqual(agentStartArgs[0], startInfo) {
		t.Fatalf("workflow/agent-start run = %#v, want %#v", agentStartArgs[0], startInfo)
	}
	agentStart := agentStartArgs[1].(map[string]any)
	if agentStart["seq"] != float64(1) || agentStart["label"] != "child" || agentStart["phase"] != "Build" || agentStart["childId"] == "" {
		t.Fatalf("workflow/agent-start payload = %#v", agentStart)
	}
	requireCordisArgs(t, cordisEventArgs(t, seen["workflow/agent-end"], 0), startInfo, map[string]any{
		"seq": 1, "label": "child", "phase": "Build", "childId": agentStart["childId"], "outcome": "completed",
	})
	requireCordisArgs(t, cordisEventArgs(t, seen["workflow/end"], 0), startInfo, map[string]any{
		"stopReason": "completed", "agentsStarted": 1,
	})
}
