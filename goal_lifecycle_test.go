package harness

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

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
	for round := 1; round <= 3; round++ {
		if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": round}); err != nil {
			t.Fatal(err)
		}
		e.mu.RLock()
		goal := e.goals[id]
		e.mu.RUnlock()
		item := &queuedPrompt{id: newID("msg"), content: []ContentBlock{{Type: "text", Text: "round"}}, source: map[string]any{"kind": "goal", "goalId": goal.ID, "revision": goal.Revision, "round": round}}
		if _, err := e.admitPrompt(context.Background(), s, item); err != nil {
			t.Fatal(err)
		}
	}
	tool := registeredTool(t, e, "update_goal")
	goal, _ := e.GetGoal(id)
	if goal == nil {
		t.Fatal("missing goal")
	}
	_, err = tool.Execute(context.Background(), ToolCall{Name: "update_goal", SessionID: id, Arguments: json.RawMessage(`{"goal_id":"` + goal.ID + `","revision":` + strconv.Itoa(goal.Revision) + `,"action":"blocked","blocked_reason":"need input"}`)})
	if err != nil {
		t.Fatalf("blocked after threshold = %v", err)
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
