package harness

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	defaultMaxGoalRounds      = 256
	defaultGoalBlockThreshold = 3
)

var goalBlockCodePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)

func (g goalState) snapshot() map[string]any {
	value := map[string]any{
		"id": g.ID, "revision": g.Revision, "objective": g.Objective,
		"phase": g.Phase, "maxGoalRounds": g.MaxRounds,
	}
	if g.Phase == "blocked" && g.BlockedReason != nil {
		value["blockedReason"] = map[string]any{"code": g.BlockedReason.Code, "message": g.BlockedReason.Message}
	}
	return value
}

func goalSnapshotChange(operation string, goal goalState) map[string]any {
	return map[string]any{
		"kind": "goal/change", "version": 1, "operation": operation,
		"goal": goal.snapshot(), "roundsStarted": goal.RoundsStarted,
		"createdAt": goal.CreatedAt, "updatedAt": goal.UpdatedAt,
	}
}

func goalClearChange(goal goalState, clearedAt int64) map[string]any {
	return map[string]any{
		"kind": "goal/change", "version": 1, "operation": "clear",
		"cleared":   map[string]any{"id": goal.ID, "revision": goal.Revision + 1},
		"clearedAt": clearedAt,
	}
}

func (e *Engine) emitGoalChanged(session *Session, operation string, goal goalState) {
	e.emitGoalChangedFrom(nil, session, operation, goal)
}

func (e *Engine) emitGoalChangedFrom(origin *dynamicCordisRun, session *Session, operation string, goal goalState) {
	revision := goal.Revision
	change := map[string]any{"operation": operation}
	if operation == "clear" {
		revision++
	} else {
		change["goal"] = goalPhaseView(goal)
	}
	change["ref"] = map[string]any{"id": goal.ID, "revision": revision}
	session.mu.Lock()
	id := session.Header.ID
	selection := cloneModelSelection(session.Model)
	status := "idle"
	if session.Running {
		status = "running"
	}
	session.mu.Unlock()
	if origin != nil {
		_ = e.dispatchDynamicCordisEvent(origin, id, true, "goal/changed", map[string]any{
			"agent":  map[string]any{"id": id, "options": selection, "session": dynamicSessionView(session), "status": status},
			"change": change,
		})
		return
	}
	e.emitDynamicCordisScopedContained(id, "goal/changed", map[string]any{
		"agent":  map[string]any{"id": id, "options": selection, "session": dynamicSessionView(session), "status": status},
		"change": change,
	})
}

func nextGoalMutationTime(goal goalState) int64 {
	now := time.Now().UnixMilli()
	if now < goal.UpdatedAt {
		return goal.UpdatedAt
	}
	return now
}

func normalizeGoalBlockReason(reason GoalBlockReason) (*GoalBlockReason, error) {
	reason.Message = strings.TrimSpace(reason.Message)
	if !goalBlockCodePattern.MatchString(reason.Code) || reason.Message == "" {
		return nil, errors.New("goal-invalid-block")
	}
	return &reason, nil
}

func goalToolGuidance(blockedAfter int) string {
	if blockedAfter < 1 {
		blockedAfter = defaultGoalBlockThreshold
	}
	return "Use goal tools for one long-running completion objective in the current session. " +
		"create_goal may infer goal intent from a direct human request in any language; do not create a goal for routine single-turn work. " +
		"Call get_goal before update_goal and copy its exact goal_id and revision. After session resume or fork, an active goal is disarmed: " +
		"when a human asks to continue or resume in any wording or language, use update_goal action resume to rearm it. " +
		"Mark complete only when the objective is actually achieved. Mark blocked only after the same blocking condition persists for at least " +
		fmt.Sprintf("%d consecutive goal rounds", blockedAfter) +
		", and report that concrete condition in blocked_reason; difficulty, uncertainty, or useful remaining work is not blocked."
}

func (e *Engine) GoalMutation(id, op, objective string, rev, maxRounds int) (map[string]any, error) {
	return e.goalMutationWithOrigin(nil, id, "", op, objective, nil, rev, maxRounds)
}

func (e *Engine) goalMutation(id, goalID, op, objective, blockedReason string, rev, maxRounds int) (map[string]any, error) {
	var reason *GoalBlockReason
	if op == "block" {
		reason = &GoalBlockReason{Code: "model-reported", Message: blockedReason}
	}
	return e.goalMutationWithOrigin(nil, id, goalID, op, objective, reason, rev, maxRounds)
}

func (e *Engine) goalMutationWithReason(id, goalID, op, objective string, reason *GoalBlockReason, rev, maxRounds int) (map[string]any, error) {
	return e.goalMutationWithOrigin(nil, id, goalID, op, objective, reason, rev, maxRounds)
}

func (e *Engine) goalMutationWithOrigin(origin *dynamicCordisRun, id, goalID, op, objective string, reason *GoalBlockReason, rev, maxRounds int) (map[string]any, error) {
	s, err := e.getSession(id)
	if err != nil {
		return nil, err
	}
	createDefaultMaxRounds := defaultMaxGoalRounds
	if op == "create" {
		runtimeConfig, runtimeErr := e.runtimeForSession(s)
		if runtimeErr != nil {
			return nil, runtimeErr
		}
		createDefaultMaxRounds = runtimeConfig.goalDefaultMaxRounds
	}
	e.mu.Lock()
	s.mu.Lock()
	goal, exists := e.goals[id]
	if op != "create" {
		if !exists || goal.ID == "" {
			s.mu.Unlock()
			e.mu.Unlock()
			return nil, errors.New("goal-not-found")
		}
		if goalID != "" && goalID != goal.ID || rev != goal.Revision {
			s.mu.Unlock()
			e.mu.Unlock()
			return nil, errors.New("goal-conflict")
		}
	} else if exists && goal.ID != "" && goal.Phase != "complete" {
		s.mu.Unlock()
		e.mu.Unlock()
		return nil, errors.New("goal-already-exists")
	}

	objectiveProvided := objective != ""
	if objectiveProvided {
		objective = strings.TrimSpace(objective)
		if objective == "" {
			s.mu.Unlock()
			e.mu.Unlock()
			return nil, errors.New("goal-invalid-objective")
		}
	}
	if maxRounds < 0 {
		s.mu.Unlock()
		e.mu.Unlock()
		return nil, errors.New("goal-invalid-max-rounds")
	}

	if op == "create" {
		if !objectiveProvided {
			s.mu.Unlock()
			e.mu.Unlock()
			return nil, errors.New("goal-invalid-objective")
		}
		if maxRounds == 0 {
			maxRounds = createDefaultMaxRounds
		}
		if maxRounds < 1 {
			s.mu.Unlock()
			e.mu.Unlock()
			return nil, errors.New("goal-invalid-max-rounds")
		}
		now := time.Now().UnixMilli()
		goal = goalState{
			ID: newID("goal"), Revision: 1, Objective: objective, Phase: "active",
			MaxRounds: maxRounds, CreatedAt: now, UpdatedAt: now, Activation: "armed",
		}
	} else {
		switch op {
		case "edit":
			if !objectiveProvided && maxRounds == 0 {
				s.mu.Unlock()
				e.mu.Unlock()
				return nil, errors.New("goal-invalid-edit")
			}
			if objectiveProvided {
				goal.Objective = objective
			}
			if maxRounds != 0 {
				if maxRounds < 1 {
					s.mu.Unlock()
					e.mu.Unlock()
					return nil, errors.New("goal-invalid-max-rounds")
				}
				goal.MaxRounds = maxRounds
			}
		case "pause":
			if goal.Phase != "active" {
				s.mu.Unlock()
				e.mu.Unlock()
				return nil, errors.New("goal-invalid-transition")
			}
			goal.Phase, goal.Activation, goal.BlockedReason = "paused", "disarmed", nil
		case "resume":
			if goal.Phase != "active" && goal.Phase != "paused" && goal.Phase != "blocked" ||
				goal.Phase == "active" && goal.Activation == "armed" || goal.RoundsStarted >= goal.MaxRounds {
				s.mu.Unlock()
				e.mu.Unlock()
				return nil, errors.New("goal-invalid-transition")
			}
			goal.Phase, goal.Activation, goal.BlockedReason = "active", "armed", nil
		case "complete":
			if goal.Phase == "complete" {
				s.mu.Unlock()
				e.mu.Unlock()
				return nil, errors.New("goal-invalid-transition")
			}
			goal.Phase, goal.Activation, goal.BlockedReason = "complete", "disarmed", nil
		case "block":
			if goal.Phase != "active" || reason == nil {
				s.mu.Unlock()
				e.mu.Unlock()
				return nil, errors.New("goal-invalid-block")
			}
			normalized, normalizeErr := normalizeGoalBlockReason(*reason)
			if normalizeErr != nil {
				s.mu.Unlock()
				e.mu.Unlock()
				return nil, normalizeErr
			}
			goal.Phase, goal.Activation, goal.BlockedReason = "blocked", "disarmed", normalized
		case "clear":
			clearedAt := nextGoalMutationTime(goal)
			change := goalClearChange(goal, clearedAt)
			previous := goal
			delete(e.goals, id)
			s.mu.Unlock()
			stateErr := e.saveStateLocked()
			s.mu.Lock()
			if stateErr != nil {
				e.goals[id] = previous
				s.mu.Unlock()
				e.mu.Unlock()
				return nil, fmt.Errorf("goal-persist-failed: %w", stateErr)
			}
			event, appendErr := appendEventLocked(s, "goal/change", change, nil, nil, false)
			if appendErr != nil {
				e.goals[id] = previous
			}
			s.mu.Unlock()
			e.mu.Unlock()
			if appendErr != nil {
				return nil, fmt.Errorf("goal-persist-failed: %w", appendErr)
			}
			e.publishEventFrom(origin, id, event)
			e.emitGoalChangedFrom(origin, s, "clear", goal)
			return map[string]any{"cleared": true}, nil
		default:
			s.mu.Unlock()
			e.mu.Unlock()
			return nil, errors.New("goal-invalid-operation")
		}
		goal.Revision++
		goal.UpdatedAt = nextGoalMutationTime(goal)
	}

	change := goalSnapshotChange(op, goal)
	previous := e.goals[id]
	e.goals[id] = goal
	s.mu.Unlock()
	stateErr := e.saveStateLocked()
	s.mu.Lock()
	if stateErr != nil {
		e.goals[id] = previous
		s.mu.Unlock()
		e.mu.Unlock()
		return nil, fmt.Errorf("goal-persist-failed: %w", stateErr)
	}
	event, appendErr := appendEventLocked(s, "goal/change", change, nil, nil, false)
	if appendErr != nil {
		e.goals[id] = previous
	}
	s.mu.Unlock()
	e.mu.Unlock()
	if appendErr != nil {
		return nil, fmt.Errorf("goal-persist-failed: %w", appendErr)
	}
	e.publishEventFrom(origin, id, event)
	e.emitGoalChangedFrom(origin, s, op, goal)
	return map[string]any{"ref": map[string]any{"id": goal.ID, "revision": goal.Revision}}, nil
}

func (e *Engine) disarmGoal(id string) {
	e.mu.Lock()
	if goal, ok := e.goals[id]; ok && goal.Phase == "active" {
		goal.Activation = "disarmed"
		e.goals[id] = goal
	}
	e.mu.Unlock()
}

func goalPhaseView(goal goalState) *GoalView {
	phase := goal.Phase
	if phase == "completed" {
		phase = "complete"
	}
	view := &GoalView{
		ID: goal.ID, Revision: goal.Revision, Objective: goal.Objective, Phase: phase,
		RoundsStarted: goal.RoundsStarted, MaxGoalRounds: goal.MaxRounds,
		CreatedAt: goal.CreatedAt, UpdatedAt: goal.UpdatedAt,
		Activation: goal.Activation,
	}
	if view.Activation == "" {
		view.Activation = "disarmed"
	}
	if goal.BlockedReason != nil {
		reason := *goal.BlockedReason
		view.BlockedReason = &reason
	}
	return view
}

func (e *Engine) GetGoal(sessionID string) (*GoalView, error) {
	if _, err := e.getSession(sessionID); err != nil {
		return nil, err
	}
	e.mu.RLock()
	goal, ok := e.goals[sessionID]
	e.mu.RUnlock()
	if !ok || goal.ID == "" {
		return nil, nil
	}
	return goalPhaseView(goal), nil
}

func renderGoalRoundPrompt(goal goalState, round int) string {
	return "<goal_round>\n" +
		fmt.Sprintf("Objective: %q\nRound: %d/%d\n\n", goal.Objective, round, goal.MaxRounds) +
		"Continue working toward the objective in this same session. Treat the current workspace, " +
		"tool results, and durable session state as authoritative; inspect them instead of assuming " +
		"earlier narration is still current. Make concrete progress and verify the result. Before " +
		"claiming completion, gather evidence that the whole objective is achieved, read the current " +
		"goal, and mark it complete. If work remains, leave the goal active for the next round. Follow " +
		"the configured goal-tool policy before reporting a blocker.\n</goal_round>"
}

func goalSource(source map[string]any) (string, int, int, bool) {
	if source["kind"] != "goal" {
		return "", 0, 0, false
	}
	goalID, _ := source["goalId"].(string)
	revision, revisionOK := eventSeqNumber(source["revision"])
	round, roundOK := eventSeqNumber(source["round"])
	return goalID, revision, round, goalID != "" && revisionOK && revision > 0 && roundOK && round > 0
}

func isGoalPrompt(item *queuedPrompt) bool {
	_, _, _, ok := goalSource(item.source)
	return ok
}

func (e *Engine) scheduleGoalRound(s *Session) (bool, error) {
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		return false, err
	}
	if !runtimeConfig.goalRoundDriver {
		return false, nil
	}
	id := s.Header.ID
	e.mu.Lock()
	s.mu.Lock()
	if len(s.pending) > 0 || len(s.steering) > 0 {
		s.mu.Unlock()
		e.mu.Unlock()
		return true, nil
	}
	goal, ok := e.goals[id]
	if !ok || goal.Phase != "active" || goal.Activation != "armed" {
		s.mu.Unlock()
		e.mu.Unlock()
		return false, nil
	}
	if goal.RoundsStarted >= goal.MaxRounds {
		next := goal
		next.Revision++
		next.Phase, next.Activation = "blocked", "disarmed"
		next.BlockedReason = &GoalBlockReason{
			Code: "round-limit", Message: fmt.Sprintf("Goal reached its configured limit of %d rounds.", goal.MaxRounds),
		}
		next.UpdatedAt = nextGoalMutationTime(goal)
		event, err := appendEventLocked(s, "goal/change", goalSnapshotChange("block", next), nil, nil, false)
		if err == nil {
			e.goals[id] = next
		} else {
			goal.Activation = "disarmed"
			e.goals[id] = goal
		}
		s.mu.Unlock()
		e.mu.Unlock()
		if err != nil {
			return false, fmt.Errorf("goal-persist-failed: %w", err)
		}
		e.publishEvent(id, event)
		e.emitGoalChanged(s, "block", next)
		return false, nil
	}
	round := goal.RoundsStarted + 1
	prompt := renderGoalRoundPrompt(goal, round)
	item := &queuedPrompt{
		id: newID("msg"), text: prompt,
		content:         []ContentBlock{{Type: "text", Text: prompt}},
		source:          map[string]any{"kind": "goal", "goalId": goal.ID, "revision": goal.Revision, "round": round},
		goalReservation: true,
	}
	event, err := appendEventLocked(s, "agent/inbox/spliced", map[string]any{
		"target": "next-turn", "start": len(s.pending), "inserted": []any{item.message()},
	}, nil, nil, false)
	if err == nil {
		s.pending = append(s.pending, item)
	} else {
		goal.Activation = "disarmed"
		e.goals[id] = goal
	}
	s.mu.Unlock()
	e.mu.Unlock()
	if err != nil {
		return false, err
	}
	e.publishEvent(id, event)
	e.emitQueue(s)
	return true, nil
}

func (e *Engine) admitPrompt(ctx context.Context, s *Session, item *queuedPrompt) (Event, error) {
	if err := ctx.Err(); err != nil {
		return Event{}, err
	}
	id := s.Header.ID
	e.mu.Lock()
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		e.mu.Unlock()
		return Event{}, err
	}
	goalID, revision, round, isGoal := goalSource(item.source)
	goal := e.goals[id]
	if isGoal && (!item.goalReservation || item.goalStale ||
		blockText(item.content) != renderGoalRoundPrompt(goal, round) ||
		goal.ID != goalID || goal.Revision != revision || goal.Phase != "active" ||
		goal.Activation != "armed" || round != goal.RoundsStarted+1 || round > goal.MaxRounds) {
		s.mu.Unlock()
		e.mu.Unlock()
		return Event{}, errors.New("goal-round-stale")
	}
	if isGoal {
		for _, queued := range append(append([]*queuedPrompt(nil), s.pending...), s.steering...) {
			if queued != nil && !isGoalPrompt(queued) {
				s.mu.Unlock()
				e.mu.Unlock()
				return Event{}, errors.New("goal-round-stale")
			}
		}
	}
	messages := make([]map[string]any, 0, len(item.additionalContexts)+1)
	messages = append(messages, map[string]any{
		"id": item.id, "role": "user", "content": item.content, "source": item.source,
	})
	for _, additional := range item.additionalContexts {
		messages = append(messages, map[string]any{
			"id": newID("msg"), "role": "user", "content": additional.Content, "source": additional.Source,
		})
	}
	events, err := appendUserMessagesLocked(s, messages)
	if err == nil && isGoal {
		goal.RoundsStarted = round
		e.goals[id] = goal
	}
	s.mu.Unlock()
	e.mu.Unlock()
	if err != nil {
		return Event{}, err
	}
	for _, event := range events {
		e.publishEvent(id, event)
		e.observeSessionTitleEvent(s, event)
	}
	return events[len(events)-1], nil
}

func appendUserMessagesLocked(s *Session, messages []map[string]any) ([]Event, error) {
	events := make([]Event, len(messages))
	for index, message := range messages {
		events[index] = Event{
			Type: "user/message", Seq: len(s.Events) + index, Time: time.Now().UnixMilli(),
			Data: message, SurfaceOp: "append",
		}
	}
	if s.invariants != nil && len(events) > 0 {
		proposed := make([]Event, 0, len(s.Events)+len(events))
		proposed = append(proposed, s.Events...)
		proposed = append(proposed, events...)
		if err := s.invariants.ValidateSession(s.Header, proposed); err != nil {
			return nil, err
		}
	}
	if s.store != nil {
		if err := s.store.Append(context.Background(), s.Header.ID, events); err != nil {
			return nil, err
		}
	}
	for _, event := range events {
		resetRepeatToolChain(s, event.Type, event.Data)
		s.Events = append(s.Events, event)
	}
	return events, nil
}

func (e *Engine) finishGoalTurn(s *Session, item *queuedPrompt, turn int) {
	goalID, revision, _, ok := goalSource(item.source)
	if !ok {
		return
	}
	reason := ""
	s.mu.Lock()
	for i := len(s.Events) - 1; i >= 0; i-- {
		event := s.Events[i]
		if event.Type != "turn/end" {
			continue
		}
		eventTurn, turnOK := eventTurn(event.Data)
		if !turnOK || eventTurn != turn {
			continue
		}
		data, _ := event.Data.(map[string]any)
		rawReason, _ := data["reason"].(map[string]any)
		reason, _ = rawReason["kind"].(string)
		break
	}
	s.mu.Unlock()
	switch reason {
	case "aborted":
		_, _ = e.goalMutation(s.Header.ID, goalID, "pause", "", "", revision, 0)
		// A concurrent human mutation can make the pause CAS stale. The
		// aborted turn must still lose process-local continuation authority.
		e.disarmGoal(s.Header.ID)
	case "error", "max-tokens":
		e.disarmGoal(s.Header.ID)
	}
}

func (e *Engine) blockRejectedGoalRound(s *Session, item *queuedPrompt) {
	goalID, revision, round, ok := goalSource(item.source)
	if !ok || !item.goalReservation || item.goalStale {
		return
	}
	e.mu.RLock()
	goal, exists := e.goals[s.Header.ID]
	e.mu.RUnlock()
	if !exists || goal.ID != goalID || goal.Revision != revision || goal.Phase != "active" ||
		goal.Activation != "armed" || round != goal.RoundsStarted+1 ||
		blockText(item.content) != renderGoalRoundPrompt(goal, round) {
		return
	}
	_, err := e.goalMutationWithReason(s.Header.ID, goalID, "block", "", &GoalBlockReason{
		Code: "prompt-rejected", Message: "Goal round was rejected before entering its step.",
	}, revision, 0)
	if err != nil {
		e.disarmGoal(s.Header.ID)
	}
}

func foldGoalState(events []Event) (goalState, bool, error) {
	var goal goalState
	hasGoal := false
	seen := map[string]bool{}
	for _, event := range events {
		switch event.Type {
		case "goal/change":
			change, ok := event.Data.(map[string]any)
			version, versionOK := eventSeqNumber(change["version"])
			operation, _ := change["operation"].(string)
			if !ok || change["kind"] != "goal/change" || !versionOK || version != 1 {
				return goalState{}, false, fmt.Errorf("invalid goal change at session seq %d", event.Seq)
			}
			if operation == "clear" {
				if !hasExactGoalMapKeys(change, "cleared", "clearedAt", "kind", "operation", "version") {
					return goalState{}, false, fmt.Errorf("invalid goal clear at session seq %d: invalid fields", event.Seq)
				}
				cleared, _ := change["cleared"].(map[string]any)
				revision, revisionOK := eventSeqNumber(cleared["revision"])
				id, _ := cleared["id"].(string)
				clearedAt, clearedAtOK := eventSeqNumber(change["clearedAt"])
				if !hasGoal || id != goal.ID || !revisionOK || revision != goal.Revision+1 ||
					!clearedAtOK || clearedAt < 0 || int64(clearedAt) < goal.UpdatedAt ||
					!hasExactGoalMapKeys(cleared, "id", "revision") {
					return goalState{}, false, fmt.Errorf("invalid goal clear at session seq %d", event.Seq)
				}
				hasGoal, goal = false, goalState{}
				continue
			}
			if operation != "create" && operation != "edit" && operation != "pause" &&
				operation != "resume" && operation != "complete" && operation != "block" {
				return goalState{}, false, fmt.Errorf("invalid goal operation at session seq %d", event.Seq)
			}
			next, decodeErr := decodeGoalSnapshot(change)
			if decodeErr != nil {
				return goalState{}, false, fmt.Errorf("invalid goal change at session seq %d: %w", event.Seq, decodeErr)
			}
			if operation == "create" {
				if next.Revision != 1 || next.Phase != "active" || next.RoundsStarted != 0 ||
					hasGoal && goal.Phase != "complete" || seen[next.ID] {
					return goalState{}, false, fmt.Errorf("invalid goal create at session seq %d", event.Seq)
				}
				seen[next.ID] = true
			} else {
				if !hasGoal || next.ID != goal.ID || next.Revision != goal.Revision+1 ||
					next.RoundsStarted != goal.RoundsStarted || next.CreatedAt != goal.CreatedAt ||
					next.UpdatedAt < goal.UpdatedAt {
					return goalState{}, false, fmt.Errorf("invalid goal transition at session seq %d", event.Seq)
				}
				if err := validateGoalFoldTransition(operation, goal, next, goal.RoundsStarted); err != nil {
					return goalState{}, false, fmt.Errorf("invalid goal transition at session seq %d: %w", event.Seq, err)
				}
			}
			next.Activation = "disarmed"
			goal, hasGoal = next, true
		case "user/message":
			data, _ := event.Data.(map[string]any)
			if message, _ := data["message"].(map[string]any); message != nil {
				data = message
			}
			source, _ := data["source"].(map[string]any)
			if source["kind"] == "goal" {
				if _, _, _, ok := goalSource(source); !ok {
					return goalState{}, false, fmt.Errorf("invalid goal round at session seq %d", event.Seq)
				}
			}
			goalID, revision, round, isGoal := goalSource(source)
			if !isGoal {
				continue
			}
			if !hasGoal || goal.Phase != "active" || goal.ID != goalID || goal.Revision != revision ||
				round != goal.RoundsStarted+1 || round > goal.MaxRounds {
				return goalState{}, false, fmt.Errorf("invalid goal round at session seq %d", event.Seq)
			}
			goal.RoundsStarted = round
		}
	}
	return goal, hasGoal, nil
}

func hasExactGoalMapKeys(value map[string]any, keys ...string) bool {
	if value == nil || len(value) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := value[key]; !ok {
			return false
		}
	}
	return true
}

func sameGoalDefinition(current, next goalState) bool {
	return current.Objective == next.Objective && current.MaxRounds == next.MaxRounds
}

func validateGoalFoldTransition(operation string, current, next goalState, roundsStarted int) error {
	switch operation {
	case "edit":
		if next.Phase != current.Phase || !sameGoalBlockReason(current.BlockedReason, next.BlockedReason) {
			return errors.New("edit cannot change phase or blocked reason")
		}
	case "pause":
		if !sameGoalDefinition(current, next) || current.Phase != "active" || next.Phase != "paused" {
			return errors.New("pause has an invalid phase transition")
		}
	case "resume":
		if !sameGoalDefinition(current, next) ||
			(current.Phase != "active" && current.Phase != "paused" && current.Phase != "blocked") ||
			next.Phase != "active" || roundsStarted >= next.MaxRounds {
			return errors.New("resume has an invalid phase transition or exhausted round budget")
		}
	case "complete":
		if !sameGoalDefinition(current, next) || current.Phase == "complete" || next.Phase != "complete" {
			return errors.New("complete has an invalid phase transition")
		}
	case "block":
		if !sameGoalDefinition(current, next) || current.Phase != "active" || next.Phase != "blocked" {
			return errors.New("block has an invalid phase transition")
		}
	default:
		return errors.New("unknown goal transition")
	}
	return nil
}

func sameGoalBlockReason(current, next *GoalBlockReason) bool {
	if current == nil || next == nil {
		return current == nil && next == nil
	}
	return *current == *next
}

func decodeGoalSnapshot(change map[string]any) (goalState, error) {
	if !hasExactGoalMapKeys(change, "createdAt", "goal", "kind", "operation", "roundsStarted", "updatedAt", "version") {
		return goalState{}, errors.New("invalid snapshot change fields")
	}
	snapshot, ok := change["goal"].(map[string]any)
	if !ok {
		return goalState{}, errors.New("missing snapshot")
	}
	id, _ := snapshot["id"].(string)
	objective, _ := snapshot["objective"].(string)
	phase, _ := snapshot["phase"].(string)
	revision, revisionOK := eventSeqNumber(snapshot["revision"])
	maxRounds, maxOK := eventSeqNumber(snapshot["maxGoalRounds"])
	rounds, roundsOK := eventSeqNumber(change["roundsStarted"])
	createdAt, createdOK := eventSeqNumber(change["createdAt"])
	updatedAt, updatedOK := eventSeqNumber(change["updatedAt"])
	if id == "" || strings.TrimSpace(objective) != objective || objective == "" ||
		!revisionOK || revision < 1 || !maxOK || maxRounds < 1 || !roundsOK || rounds < 0 ||
		!createdOK || createdAt < 0 || !updatedOK || updatedAt < createdAt {
		return goalState{}, errors.New("invalid snapshot fields")
	}
	if phase != "active" && phase != "paused" && phase != "blocked" && phase != "complete" {
		return goalState{}, errors.New("invalid phase")
	}
	expectedSnapshotKeys := []string{"id", "maxGoalRounds", "objective", "phase", "revision"}
	if phase == "blocked" {
		expectedSnapshotKeys = append(expectedSnapshotKeys, "blockedReason")
	}
	if !hasExactGoalMapKeys(snapshot, expectedSnapshotKeys...) {
		return goalState{}, errors.New("invalid snapshot fields")
	}
	goal := goalState{
		ID: id, Revision: revision, Objective: objective, Phase: phase, MaxRounds: maxRounds,
		RoundsStarted: rounds, CreatedAt: int64(createdAt), UpdatedAt: int64(updatedAt), Activation: "disarmed",
	}
	if phase == "blocked" {
		rawReason, _ := snapshot["blockedReason"].(map[string]any)
		if !hasExactGoalMapKeys(rawReason, "code", "message") {
			return goalState{}, errors.New("invalid blocked reason")
		}
		code, _ := rawReason["code"].(string)
		message, _ := rawReason["message"].(string)
		reason, err := normalizeGoalBlockReason(GoalBlockReason{Code: code, Message: message})
		if err != nil || message != strings.TrimSpace(message) {
			return goalState{}, errors.New("invalid blocked reason")
		}
		goal.BlockedReason = reason
	} else if snapshot["blockedReason"] != nil {
		return goalState{}, errors.New("unexpected blocked reason")
	}
	return goal, nil
}
