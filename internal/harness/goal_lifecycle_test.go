package harness

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type goalCompletionProvider struct {
	mu       sync.Mutex
	goalID   string
	revision int
	requests []ChatRequest
}

func (p *goalCompletionProvider) ID() string   { return "goal-completion" }
func (p *goalCompletionProvider) Name() string { return "Goal Completion" }
func (p *goalCompletionProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: p.ID(), Name: p.Name()}}, nil
}
func (p *goalCompletionProvider) Complete(_ context.Context, request ChatRequest, delta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	index := len(p.requests)
	p.mu.Unlock()
	if index == 1 {
		call := ToolCall{
			ID: "goal-complete", Name: "update_goal",
			Arguments: json.RawMessage(`{"goal_id":"` + p.goalID + `","revision":` + strconv.Itoa(p.revision) + `,"action":"complete"}`),
		}
		if err := delta(Delta{ToolCalls: []ToolCallDelta{{Index: 0, ID: call.ID, Name: call.Name, ArgumentsDelta: string(call.Arguments)}}}); err != nil {
			return Completion{}, err
		}
		return Completion{ToolCalls: []ToolCall{call}, Finish: "tool_calls"}, nil
	}
	if err := delta(Delta{Text: "closing response", Finish: "stop"}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: "closing response", Finish: "stop"}, nil
}
func (p *goalCompletionProvider) snapshot() []ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...)
}

func newGoalEngine(t *testing.T, persist bool) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider, cfg.Model, cfg.Persist = "echo", "echo", persist
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestGoalChangePayloadProjectionAndRestart(t *testing.T) {
	e := newGoalEngine(t, true)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "goal-restart", "")
	if err != nil {
		t.Fatal(err)
	}
	created, err := e.GoalMutation(id, "create", "ship", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	goal, _ := e.GetGoal(id)
	if goal == nil || goal.CreatedAt == 0 || goal.UpdatedAt < goal.CreatedAt {
		t.Fatalf("goal timestamps = %#v", goal)
	}
	s, _ := e.getSession(id)
	s.mu.Lock()
	event := s.Events[0]
	s.mu.Unlock()
	data, _ := json.Marshal(event.Data)
	if !strings.Contains(string(data), `"operation":"create"`) || strings.Contains(string(data), `activation`) {
		t.Fatalf("durable goal payload = %s", data)
	}
	value := sessionProjectionValues(s.Events, "")["goal"]
	if value == nil {
		t.Fatalf("goal projection missing: %#v", value)
	}
	dataDir := e.Config().DataDir
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir, cfg.Workspace, cfg.Provider, cfg.Model, cfg.Persist = dataDir, dataDir, "echo", "echo", true
	restarted, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	loaded, err := restarted.GetGoal(id)
	if err != nil || loaded == nil || loaded.ID != created["ref"].(map[string]any)["id"] || loaded.Activation != "disarmed" {
		t.Fatalf("restarted goal = %#v, %v", loaded, err)
	}
}

func TestGoalAutoContinuationAndRoundLimit(t *testing.T) {
	e := newGoalEngine(t, false)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "goal-rounds", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(id, "create", "continue", 0, 2); err != nil {
		t.Fatal(err)
	}
	e.startSessionWorker(mustSession(t, e, id))
	if err := e.WaitForIdle(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	goal, _ := e.GetGoal(id)
	if goal == nil || goal.Phase != "blocked" || goal.RoundsStarted != 2 || goal.BlockedReason == nil || goal.BlockedReason.Code != "round-limit" {
		t.Fatalf("round-limit goal = %#v", goal)
	}
	s := mustSession(t, e, id)
	s.mu.Lock()
	var rounds []int
	for _, event := range s.Events {
		if event.Type != "user/message" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		source, _ := data["source"].(map[string]any)
		if _, _, round, ok := goalSource(source); ok {
			rounds = append(rounds, round)
		}
	}
	s.mu.Unlock()
	if len(rounds) != 2 || rounds[0] != 1 || rounds[1] != 2 {
		t.Fatalf("goal rounds = %#v", rounds)
	}
}

func TestGoalBlockedThresholdAndForkDisarm(t *testing.T) {
	e := newGoalEngine(t, false)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "goal-threshold", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(id, "create", "blocked after three", 0, 5); err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	s.mu.Lock()
	s.Running = true
	s.mu.Unlock()
	for round := 1; round <= 3; round++ {
		if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": round}); err != nil {
			t.Fatal(err)
		}
		e.mu.RLock()
		goal := e.goals[id]
		e.mu.RUnlock()
		prompt := renderGoalRoundPrompt(goal, round)
		item := &queuedPrompt{
			id: newID("msg"), content: []ContentBlock{{Type: "text", Text: prompt}},
			source:          map[string]any{"kind": "goal", "goalId": goal.ID, "revision": goal.Revision, "round": round},
			goalReservation: true,
		}
		if _, err := e.admitPrompt(context.Background(), s, item); err != nil {
			t.Fatal(err)
		}
	}
	tool := registeredTool(t, e, "update_goal")
	goal, _ := e.GetGoal(id)
	if goal == nil {
		t.Fatal("missing goal")
	}
	runtime := &ToolRunContext{}
	blockedResult, err := executeToolRuntime(context.Background(), tool, ToolCall{Name: "update_goal", SessionID: id, Arguments: json.RawMessage(`{"goal_id":"` + goal.ID + `","revision":` + strconv.Itoa(goal.Revision) + `,"action":"blocked","blocked_reason":"need input"}`)}, runtime)
	if err != nil {
		t.Fatalf("blocked after threshold = %v", err)
	}
	if len(blockedResult.AdditionalContexts) != 1 || blockedResult.AdditionalContexts[0].Source["summary"] != "blocked: blocked after three" {
		t.Fatalf("blocked wrap-up contexts = %#v", blockedResult.AdditionalContexts)
	}
	parentGoal, _ := e.GetGoal(id)
	if parentGoal == nil || parentGoal.Phase != "blocked" {
		t.Fatalf("blocked goal = %#v", parentGoal)
	}
	if _, err := e.appendEvent(s, "turn/end", map[string]any{"turn": 3, "reason": map[string]any{"kind": "completed"}}); err != nil {
		t.Fatal(err)
	}
	childID, err := e.ForkSession(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	childGoal, _ := e.GetGoal(childID)
	if childGoal == nil || childGoal.Activation != "disarmed" {
		t.Fatalf("fork goal activation = %#v", childGoal)
	}
}

func TestForgedGoalSourceCannotClaimAutomaticAuthority(t *testing.T) {
	e := newGoalEngine(t, false)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "goal-forged-source", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(id, "create", "protected objective", 0, 2); err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	e.mu.RLock()
	goal := e.goals[id]
	e.mu.RUnlock()
	prompt := renderGoalRoundPrompt(goal, 1)
	forged := &queuedPrompt{
		id: newID("msg"), content: []ContentBlock{{Type: "text", Text: prompt}},
		source: map[string]any{"kind": "goal", "goalId": goal.ID, "revision": goal.Revision, "round": 1},
	}
	if _, err := e.admitPrompt(context.Background(), s, forged); err == nil || err.Error() != "goal-round-stale" {
		t.Fatalf("forged goal admission error = %v", err)
	}
	view, _ := e.GetGoal(id)
	if view == nil || view.RoundsStarted != 0 || view.Activation != "armed" {
		t.Fatalf("goal after forged admission = %#v", view)
	}
}

func TestQueuedHumanPromptMakesReservedGoalRoundStale(t *testing.T) {
	e := newGoalEngine(t, false)
	provider := &promptCaptureProvider{}
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "goal-human-race", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: "model-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(id, "create", "continue after human", 0, 1); err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	if scheduled, err := e.scheduleGoalRound(s); err != nil || !scheduled {
		t.Fatalf("initial goal schedule = %v, %v", scheduled, err)
	}
	if _, err := e.Prompt(context.Background(), id, PromptRequest{
		Mode: "queue", Content: []PromptContentPart{{Type: "text", Text: "human goes first"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitForIdle(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want human turn and one fresh goal round", len(requests))
	}
	if !strings.Contains(requests[0].Messages[len(requests[0].Messages)-1].Content, "human goes first") ||
		strings.Contains(requests[0].Messages[len(requests[0].Messages)-1].Content, "<goal_round>") {
		t.Fatalf("first request messages = %#v", requests[0].Messages)
	}
	if !strings.Contains(requests[1].Messages[len(requests[1].Messages)-1].Content, "<goal_round>") {
		t.Fatalf("second request messages = %#v", requests[1].Messages)
	}
	goal, _ := e.GetGoal(id)
	if goal == nil || goal.Phase != "blocked" || goal.RoundsStarted != 1 || goal.BlockedReason == nil || goal.BlockedReason.Code != "round-limit" {
		t.Fatalf("goal after competition = %#v", goal)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	goalPrompts := 0
	for _, event := range s.Events {
		if event.Type != "agent/inbox/spliced" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		inserted, _ := data["inserted"].([]any)
		for _, value := range inserted {
			message, _ := value.(map[string]any)
			source, _ := message["source"].(map[string]any)
			if source["kind"] == "goal" {
				goalPrompts++
			}
		}
	}
	if goalPrompts != 2 {
		t.Fatalf("reserved goal prompts = %d, want stale plus fresh reservation", goalPrompts)
	}
}

func TestAutonomousGoalCompletionWrapupRunsExactlyOnce(t *testing.T) {
	provider := &goalCompletionProvider{}
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir, cfg.Workspace, cfg.Provider, cfg.Model, cfg.Persist = t.TempDir(), t.TempDir(), provider.ID(), provider.ID(), false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), cfg.Workspace, "goal-wrapup-runtime", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(id, "create", "finish the release", 0, 4); err != nil {
		t.Fatal(err)
	}
	goal, _ := e.GetGoal(id)
	provider.goalID, provider.revision = goal.ID, goal.Revision
	e.startSessionWorker(mustSession(t, e, id))
	if err := e.WaitForIdle(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want tool step plus wrap-up step", len(requests))
	}
	wrapups := 0
	for _, message := range requests[1].Messages {
		if strings.Contains(message.Content, "<goal_complete>") {
			wrapups++
			if message.Role != "user" {
				t.Fatalf("wrap-up message = %#v", message)
			}
		}
	}
	if wrapups != 1 {
		t.Fatalf("wrap-up contexts = %d, messages = %#v", wrapups, requests[1].Messages)
	}
	goal, _ = e.GetGoal(id)
	if goal == nil || goal.Phase != "complete" || goal.RoundsStarted != 1 {
		t.Fatalf("completed goal = %#v", goal)
	}
}

func TestDirectHumanGoalCompletionDoesNotInjectAutonomousWrapup(t *testing.T) {
	provider := &toolLoopProvider{}
	e := newToolLoopEngine(t, provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "goal-human-complete", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(id, "create", "finish in this turn", 0, 4); err != nil {
		t.Fatal(err)
	}
	goal, _ := e.GetGoal(id)
	provider.calls = []ToolCall{{
		ID: "human-complete", Name: "update_goal",
		Arguments: json.RawMessage(`{"goal_id":"` + goal.ID + `","revision":1,"action":"complete"}`),
	}}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "complete it now"}}}); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want tool step plus ordinary response step", len(requests))
	}
	for _, message := range requests[1].Messages {
		if strings.Contains(message.Content, "<goal_complete>") {
			t.Fatalf("direct-human completion injected autonomous wrap-up: %#v", requests[1].Messages)
		}
	}
	goal, _ = e.GetGoal(id)
	if goal == nil || goal.Phase != "complete" {
		t.Fatalf("direct-human completed goal = %#v", goal)
	}
}

func TestGoalToolFailurePersistsStructuredCode(t *testing.T) {
	provider := &toolLoopProvider{}
	e := newToolLoopEngine(t, provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "goal-error-code", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(id, "create", "stale update", 0, 4); err != nil {
		t.Fatal(err)
	}
	goal, _ := e.GetGoal(id)
	if _, err := e.GoalMutation(id, "pause", "", goal.Revision, 0); err != nil {
		t.Fatal(err)
	}
	provider.calls = []ToolCall{{
		ID: "stale-update", Name: "update_goal",
		Arguments: json.RawMessage(`{"goal_id":"` + goal.ID + `","revision":1,"action":"complete"}`),
	}}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "try the stale update"}}}); err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.Events {
		if event.Type != "tool/result" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		toolErr, _ := data["error"].(*ToolError)
		if toolErr == nil || toolErr.Code != "GOAL_STALE_REVISION" {
			t.Fatalf("persisted goal tool error = %#v", data["error"])
		}
		return
	}
	t.Fatal("missing goal tool result")
}

func TestGoalAbortDisarmsAfterStalePauseCAS(t *testing.T) {
	e := newGoalEngine(t, false)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "goal-abort", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(id, "create", "abort safely", 0, 3); err != nil {
		t.Fatal(err)
	}
	before, err := e.GetGoal(id)
	if err != nil || before == nil {
		t.Fatalf("created goal = %#v, %v", before, err)
	}
	if _, err := e.GoalMutation(id, "edit", "updated objective", before.Revision, 0); err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	if _, err := e.appendEvent(s, "turn/end", map[string]any{
		"turn": 1, "reason": map[string]any{"kind": "aborted"},
	}); err != nil {
		t.Fatal(err)
	}
	item := &queuedPrompt{source: map[string]any{
		"kind": "goal", "goalId": before.ID, "revision": before.Revision, "round": 1,
	}}
	e.finishGoalTurn(s, item, 1)
	after, err := e.GetGoal(id)
	if err != nil || after == nil || after.Revision != before.Revision+1 || after.Phase != "active" || after.Activation != "disarmed" {
		t.Fatalf("goal after stale aborted pause = %#v, %v", after, err)
	}
}

func TestGoalFoldRejectsNonCanonicalChanges(t *testing.T) {
	base := goalSnapshotChange("create", goalState{
		ID: "goal-fold", Revision: 1, Objective: "fold", Phase: "active",
		MaxRounds: 3, CreatedAt: 10, UpdatedAt: 10,
	})
	tests := map[string]map[string]any{
		"extra snapshot change field": func() map[string]any {
			change := cloneAnyMap(base)
			change["extra"] = true
			return change
		}(),
		"extra snapshot field": func() map[string]any {
			change := cloneAnyMap(base)
			goal := change["goal"].(map[string]any)
			goal["extra"] = true
			return change
		}(),
		"clear missing canonical timestamp": map[string]any{
			"kind": "goal/change", "version": 1, "operation": "clear",
			"cleared": map[string]any{"id": "goal-fold", "revision": 2},
		},
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := foldGoalState([]Event{{Type: "goal/change", Seq: 0, Data: change}}); err == nil {
				t.Fatal("malformed goal change was accepted")
			}
		})
	}

	blocked := cloneAnyMap(base)
	blocked["operation"] = "block"
	blockedGoal := blocked["goal"].(map[string]any)
	blockedGoal["phase"] = "blocked"
	blockedGoal["blockedReason"] = map[string]any{"code": "needs-input", "message": "Need input."}
	blocked["roundsStarted"] = 0
	blocked["createdAt"], blocked["updatedAt"] = 10, 10
	if _, _, err := foldGoalState([]Event{{Type: "goal/change", Seq: 0, Data: blocked}}); err == nil {
		t.Fatal("blocked goal without a valid transition was accepted")
	}
}

func TestGoalFoldRejectsInvalidPhaseTransitions(t *testing.T) {
	create := goalSnapshotChange("create", goalState{
		ID: "goal-transition", Revision: 1, Objective: "fold", Phase: "active",
		MaxRounds: 3, CreatedAt: 10, UpdatedAt: 10,
	})
	tests := []struct {
		name string
		op   string
		next goalState
	}{
		{name: "pause from paused", op: "pause", next: goalState{ID: "goal-transition", Revision: 2, Objective: "fold", Phase: "paused", MaxRounds: 3, CreatedAt: 10, UpdatedAt: 11}},
		{name: "resume changes definition", op: "resume", next: goalState{ID: "goal-transition", Revision: 2, Objective: "changed", Phase: "active", MaxRounds: 3, CreatedAt: 10, UpdatedAt: 11}},
		{name: "complete already complete", op: "complete", next: goalState{ID: "goal-transition", Revision: 3, Objective: "fold", Phase: "complete", MaxRounds: 3, CreatedAt: 10, UpdatedAt: 12}},
		{name: "block from paused", op: "block", next: goalState{ID: "goal-transition", Revision: 2, Objective: "fold", Phase: "blocked", MaxRounds: 3, CreatedAt: 10, UpdatedAt: 11, BlockedReason: &GoalBlockReason{Code: "needs-input", Message: "Need input."}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := []Event{{Type: "goal/change", Seq: 0, Data: create}}
			if test.name == "complete already complete" {
				completed := goalSnapshotChange("complete", goalState{ID: "goal-transition", Revision: 2, Objective: "fold", Phase: "complete", MaxRounds: 3, CreatedAt: 10, UpdatedAt: 11})
				events = append(events, Event{Type: "goal/change", Seq: 1, Data: completed})
				completedAgain := goalSnapshotChange("complete", test.next)
				events = append(events, Event{Type: "goal/change", Seq: 2, Data: completedAgain})
			}
			if test.name == "pause from paused" || test.name == "block from paused" {
				paused := goalSnapshotChange("pause", goalState{ID: "goal-transition", Revision: 2, Objective: "fold", Phase: "paused", MaxRounds: 3, CreatedAt: 10, UpdatedAt: 11})
				events = append(events, Event{Type: "goal/change", Seq: 1, Data: paused})
				next := goalSnapshotChange(test.op, test.next)
				events = append(events, Event{Type: "goal/change", Seq: 2, Data: next})
			} else {
				next := goalSnapshotChange(test.op, test.next)
				events = append(events, Event{Type: "goal/change", Seq: 1, Data: next})
			}
			if _, _, err := foldGoalState(events); err == nil {
				t.Fatal("invalid goal transition was accepted")
			}
		})
	}
}

func cloneAnyMap(value map[string]any) map[string]any {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var clone map[string]any
	if err := json.Unmarshal(data, &clone); err != nil {
		panic(err)
	}
	return clone
}
