package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const defaultPlanModeSection = `You are in plan mode. Stay in plan mode until exit_plan_mode succeeds or the user switches the session mode. Imperative language to implement changes means plan the implementation, not execute it. A user's conversational agreement - including an answer confirming something you asked - approves nothing and does not end plan mode; fold the confirmed decision into the plan and submit it through exit_plan_mode.

Explore first. Use non-mutating reads, searches, static analysis, and checks to ground the plan in the actual repository. Do not edit or write files, change configuration, run formatters or code generation that rewrites tracked files, commit, or otherwise carry out the plan. Prefer existing functions and patterns over new machinery.

The tool catalog stays the same across modes for request-cache stability. These plan-mode rules override any later tool description or guidance that suggests using mutation tools; those tools remain listed to keep the tool catalog unchanged. Do not use todo_write to track this planning phase: it tracks implementation after an approved plan, while the plan itself belongs in exit_plan_mode.

Resolve discoverable facts by inspection. Use ask_user_question only for user-owned choices or material ambiguity that inspection cannot answer. Do not ask the user where code lives or how current behavior works when you can find out.

Make the plan decision-complete: state the goal and success criteria; group implementation changes by subsystem; identify public API, schema, and data-flow changes; cover edge cases, failure modes, tests, acceptance criteria, and explicit assumptions. Keep it concise enough to review but detailed enough that another engineer can implement it without making design decisions.

When ready, call exit_plan_mode with the complete plan markdown, starting with a # title. Make exit_plan_mode the only and final tool call in that assistant response: it presents the plan for approval, and implementation begins only in a later step after approval. Do not paste the final plan as a plain reply or ask "should I proceed?" through prose or ask_user_question. If review rejects it, incorporate the feedback and present again. If the review channel is unavailable or aborted, stay in plan mode and ask the user to switch modes manually; do not proceed with implementation.`

const (
	exitPlanModeName       = "exit_plan_mode"
	planReviewID           = "plan-review"
	planApproveLabel       = "Approve"
	planKeepPlanningLabel  = "Keep planning"
	planModeEnabledNotice  = "The user switched this session to plan mode."
	planModeDisabledNotice = "The user switched this session back to the default mode."
)

var planHeadingPattern = regexp.MustCompile(`^#\s+\S`)

type planModeIntent struct {
	active  bool
	narrate bool
}

type planModeSetOutcome string

const (
	planModeCommitted planModeSetOutcome = "committed"
	planModeQueued    planModeSetOutcome = "queued"
	planModeCancelled planModeSetOutcome = "cancelled"
	planModeNoop      planModeSetOutcome = "noop"
)

func planModeActiveForSession(s *Session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.planIntent != nil {
		return s.planIntent.active
	}
	return planModeActive(s.Events)
}

func hasOpenPlanTurn(events []Event) bool {
	open := false
	for _, event := range events {
		switch event.Type {
		case "turn/start":
			open = true
		case "turn/end":
			open = false
		}
	}
	return open
}

func planModeAtLastHeader(events []Event) (bool, bool) {
	active := false
	told := false
	known := false
	for _, event := range events {
		if event.Type == "plan/mode" {
			data, _ := event.Data.(map[string]any)
			if next, ok := data["active"].(bool); ok {
				active = next
			}
		}
		if event.Type == "request/header" {
			told, known = active, true
		}
	}
	return told, known
}

func planModeNarration(events []Event, target bool) string {
	told, known := planModeAtLastHeader(events)
	if !known || told == target {
		return ""
	}
	if target {
		return planModeEnabledNotice
	}
	return planModeDisabledNotice
}

func planModeNarrationData(text string) map[string]any {
	return map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: text}},
		"source": map[string]any{"kind": "plugin", "plugin": "plan-mode", "form": "notice", "summary": text},
	}
}

func (e *Engine) setPlanMode(s *Session, active, narrate bool) (planModeSetOutcome, error) {
	s.mu.Lock()
	logged := planModeActive(s.Events)
	target := logged
	if s.planIntent != nil {
		target = s.planIntent.active
	}
	if active == target {
		s.mu.Unlock()
		return planModeNoop, nil
	}
	if hasOpenPlanTurn(s.Events) {
		s.planIntent = &planModeIntent{active: active, narrate: narrate}
		s.mu.Unlock()
		if logged == active {
			return planModeCancelled, nil
		}
		return planModeQueued, nil
	}
	if active == logged {
		s.planIntent = nil
		s.mu.Unlock()
		return planModeCancelled, nil
	}
	notice := ""
	if narrate {
		notice = planModeNarration(s.Events, active)
	}
	event, err := appendEventLocked(s, "plan/mode", map[string]any{"active": active}, nil, nil, false)
	if err == nil {
		s.planIntent = nil
	}
	var narrationEvent Event
	var narrationErr error
	if err == nil && notice != "" {
		narrationEvent, narrationErr = appendEventLocked(s, "user/message", planModeNarrationData(notice), "append", nil, false)
	}
	id := s.Header.ID
	s.mu.Unlock()
	if err != nil {
		return "", err
	}
	e.publishEvent(id, event)
	e.observeSessionTitleEvent(s, event)
	if narrationErr != nil {
		return "", narrationErr
	}
	if notice != "" {
		e.publishEvent(id, narrationEvent)
		e.observeSessionTitleEvent(s, narrationEvent)
	}
	return planModeCommitted, nil
}

func (e *Engine) applyPendingPlanMode(s *Session) error {
	s.mu.Lock()
	pending := s.planIntent
	if pending == nil {
		s.mu.Unlock()
		return nil
	}
	logged := planModeActive(s.Events)
	if pending.active == logged {
		s.planIntent = nil
		s.mu.Unlock()
		return nil
	}
	notice := ""
	if pending.narrate {
		notice = planModeNarration(s.Events, pending.active)
	}
	event, err := appendEventLocked(s, "plan/mode", map[string]any{"active": pending.active}, nil, nil, false)
	if err == nil {
		s.planIntent = nil
	}
	var narrationEvent Event
	var narrationErr error
	if err == nil && notice != "" {
		narrationEvent, narrationErr = appendEventLocked(s, "user/message", planModeNarrationData(notice), "append", nil, false)
	}
	id := s.Header.ID
	s.mu.Unlock()
	if err != nil {
		return err
	}
	e.publishEvent(id, event)
	e.observeSessionTitleEvent(s, event)
	if narrationErr != nil {
		return narrationErr
	}
	if notice != "" {
		e.publishEvent(id, narrationEvent)
		e.observeSessionTitleEvent(s, narrationEvent)
	}
	return nil
}

func builtinExitPlanModeTool(e *Engine) Tool {
	type input struct {
		Plan string `json:"plan"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        exitPlanModeName,
			Description: "Use only in plan mode. Present your complete markdown plan for user review and, on approval, leave plan mode. The plan must start with a # heading. If the user keeps planning, revise it using their feedback and present it again.",
			Parameters: objectSchema(map[string]any{
				"plan": map[string]any{"type": "string", "description": "The complete plan, as markdown, starting with a # heading that names it."},
			}, "plan"),
			Output: objectSchema(map[string]any{
				"approved": map[string]any{"type": "boolean", "const": true},
			}, "approved"),
		},
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(exec.Call, &in); err != nil {
				return ToolResult{}, err
			}
			if exec.Call.SessionID == "" {
				return ToolResult{}, errors.New("exit_plan_mode requires a calling agent (no session to switch)")
			}
			s, err := e.getSession(exec.Call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			s.mu.Lock()
			active := planModeActive(s.Events)
			delegated := s.Header.Origin == "subagent"
			s.mu.Unlock()
			if !active {
				return ToolResult{}, errors.New("exit_plan_mode is only available in plan mode")
			}
			plan := strings.TrimSpace(in.Plan)
			if !planHeadingPattern.MatchString(plan) {
				return ToolResult{}, errors.New("exit_plan_mode requires a non-empty markdown plan starting with a # heading")
			}
			if delegated {
				return ToolResult{}, errors.New("no user-questions channel is available to review the plan; ask the user to switch the session mode instead")
			}
			value, err := e.RequestInteraction(exec, exec.Call.SessionID, "question/requested", map[string]any{
				"type": "question/requested", "sessionId": exec.Call.SessionID,
				"questions": []any{map[string]any{
					"id": planReviewID, "header": "Plan review", "question": "Approve this plan and leave plan mode?", "detail": plan,
					"options": []any{
						map[string]any{"label": planApproveLabel, "description": "Leave plan mode; the plan is carried out from the next step."},
						map[string]any{"label": planKeepPlanningLabel, "description": "Stay in plan mode; feedback goes back to the model."},
					},
					"intent": map[string]any{"kind": "plan-review", "approve": planApproveLabel},
				}},
			})
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return ToolResult{}, err
				}
				if strings.Contains(err.Error(), "ASK_CANCELLED") {
					return ToolResult{}, errors.New("the user dismissed the plan review to speak instead; stay in plan mode, stop here, and wait for their message")
				}
				return ToolResult{}, err
			}
			approved, feedback := approvedPlanReview(value)
			if !approved {
				if feedback == "" {
					return ToolResult{}, errors.New("the user chose to keep planning; revise the plan and present it again")
				}
				return ToolResult{}, fmt.Errorf("the user chose to keep planning; their feedback: %s", feedback)
			}
			s.mu.Lock()
			s.planIntent = &planModeIntent{active: false, narrate: false}
			s.mu.Unlock()
			return ToolResult{Value: map[string]any{"approved": true}}, nil
		},
		RenderOutput: func(_ ToolCall, _ any) ([]ContentBlock, error) {
			return []ContentBlock{{Type: "text", Text: "Plan approved - plan mode exited; carry out the plan starting with your next step."}}, nil
		},
		PresentationMeta: func(call ToolCall, _ any) (any, error) {
			var in input
			if err := json.Unmarshal(call.Arguments, &in); err != nil {
				return nil, err
			}
			return map[string]any{"card": "generic", "title": "Plan review", "content": []ContentBlock{{Type: "text", Text: in.Plan}}}, nil
		},
	}
}

func approvedPlanReview(value any) (bool, string) {
	if wrapper, ok := value.(map[string]any); ok {
		if answer, exists := wrapper["answer"]; exists {
			value = answer
		}
	}
	answer, _ := value.(map[string]any)
	items, _ := answer["answers"].([]any)
	var matches []map[string]any
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["id"] == planReviewID {
			matches = append(matches, item)
		}
	}
	if len(matches) != 1 {
		return false, ""
	}
	item := matches[0]
	custom, hasCustom := item["custom"].(string)
	selected, _ := item["selected"].([]any)
	if len(selected) != 1 || selected[0] != planApproveLabel || hasCustom {
		return false, custom
	}
	return true, ""
}
