package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type GoalBlockReason struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type GoalView struct {
	ID            string           `json:"id"`
	Revision      int              `json:"revision"`
	Objective     string           `json:"objective"`
	Phase         string           `json:"phase"`
	RoundsStarted int              `json:"roundsStarted"`
	MaxGoalRounds int              `json:"maxGoalRounds"`
	CreatedAt     int64            `json:"createdAt"`
	UpdatedAt     int64            `json:"updatedAt"`
	BlockedReason *GoalBlockReason `json:"blockedReason,omitempty"`
	Activation    string           `json:"activation"`
}

type SkillDefinition struct {
	Name         string `json:"name"`
	Description  string `json:"description,omitempty"`
	Provider     string `json:"provider"`
	ResourceBase string `json:"resourceBase"`
	Content      string `json:"content"`
}

func registerModelTools(e *Engine) error {
	for _, tool := range []Tool{
		builtinAskUserTool(e),
		builtinGetGoalTool(e),
		builtinCreateGoalTool(e),
		builtinUpdateGoalTool(e),
		builtinSendMessageTool(e),
		builtinInterruptAgentTool(e),
		builtinListAgentsTool(e),
	} {
		if err := e.RegisterTool(tool); err != nil {
			return err
		}
	}
	configs := make([]SubagentToolConfig, 0, 2)
	for _, config := range e.cfg.SubagentTools {
		if config.Provider == "spawn" || config.Provider == "fork" {
			configs = append(configs, config)
		}
	}
	if len(configs) == 0 {
		foregroundOnly := false
		maxDepth := 3
		configs = []SubagentToolConfig{
			{Provider: "spawn", ToolName: "subagent", BackgroundMode: "continuable", MaxDepth: &maxDepth},
			{Provider: "fork", ToolName: "subagent_fork", BackgroundMode: "continuable", EnableRunInBackground: &foregroundOnly, MaxDepth: &maxDepth},
		}
	}
	for _, config := range configs {
		if err := e.RegisterSubagentTool(config); err != nil {
			return err
		}
	}
	return registerDynamicCordisTools(e)
}

func jsonToolResult(value any) (ToolResult, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return ToolResult{}, err
	}
	result := textToolResult(string(data))
	result.Value = value
	return result, nil
}

func goalToolOutputSchema() map[string]any {
	goal := objectSchema(map[string]any{
		"id": map[string]any{"type": "string"}, "revision": map[string]any{"type": "integer"},
		"objective": map[string]any{"type": "string"}, "phase": map[string]any{"type": "string", "enum": []string{"active", "paused", "blocked", "complete"}},
		"roundsStarted": map[string]any{"type": "integer"}, "maxGoalRounds": map[string]any{"type": "integer"},
		"blockedReason": objectSchema(map[string]any{"code": map[string]any{"type": "string"}, "message": map[string]any{"type": "string"}}, "code", "message"),
	}, "id", "revision", "objective", "phase", "roundsStarted", "maxGoalRounds")
	return map[string]any{"oneOf": []any{
		objectSchema(map[string]any{"goal": map[string]any{"type": "null"}}, "goal"),
		objectSchema(map[string]any{"goal": goal, "activation": map[string]any{"type": "string", "enum": []string{"armed", "disarmed"}}}, "goal", "activation"),
	}}
}

func (e *Engine) LoadSkill(sessionID, name string) (SkillDefinition, error) {
	name = strings.TrimSpace(name)
	if !validSkillName(name) {
		return SkillDefinition{}, fmt.Errorf("invalid skill name %q", name)
	}
	records, rpcErr := e.skillRecordsForSession(sessionID)
	if rpcErr != nil {
		return SkillDefinition{}, rpcErr
	}
	for _, record := range records {
		if record.name != name {
			continue
		}
		if !record.modelInvocable {
			return SkillDefinition{}, fmt.Errorf("skill %q is not available for model invocation", name)
		}
		return SkillDefinition{Name: record.name, Description: record.description, Provider: record.provider, ResourceBase: filepath.Dir(record.path), Content: record.content}, nil
	}
	return SkillDefinition{}, fmt.Errorf("skill %q is unknown or no longer available", name)
}

type askQuestionOption struct {
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

type askQuestionItem struct {
	ID          string              `json:"id"`
	Question    string              `json:"question"`
	Header      string              `json:"header,omitempty"`
	Options     []askQuestionOption `json:"options,omitempty"`
	MultiSelect bool                `json:"multi_select,omitempty"`
}

func builtinAskUserTool(e *Engine) Tool {
	type input struct {
		Questions []askQuestionItem `json:"questions"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "ask_user_question",
			Description: "Ask the user a concise question when you need confirmation, a choice, or missing information before proceeding.",
			Parameters: objectSchema(map[string]any{
				"questions": map[string]any{
					"type": "array", "description": "Questions to ask the user before continuing.",
					"items": map[string]any{"type": "object", "additionalProperties": true, "properties": map[string]any{
						"id": map[string]any{"type": "string"}, "question": map[string]any{"type": "string"}, "header": map[string]any{"type": "string"},
						"options":      map[string]any{"type": "array", "items": map[string]any{"type": "object", "additionalProperties": true, "properties": map[string]any{"label": map[string]any{"type": "string"}, "description": map[string]any{"type": "string"}}, "required": []string{"label"}}},
						"multi_select": map[string]any{"type": "boolean"},
					}, "required": []string{"id", "question"}},
				},
			}, "questions"),
			Output: objectSchema(map[string]any{"answers": map[string]any{"type": "array", "items": objectSchema(map[string]any{
				"id": map[string]any{"type": "string"}, "selected": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, "custom": map[string]any{"type": "string"},
			}, "id", "selected")}}, "answers"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if len(in.Questions) == 0 {
				return ToolResult{}, errors.New("ask_user_question requires at least one question")
			}
			s, err := e.getSession(call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			s.mu.Lock()
			delegated := s.Header.Origin == "subagent"
			s.mu.Unlock()
			if delegated {
				return ToolResult{}, errors.New("human interaction is unavailable while the calling agent is owned by another agent")
			}
			seen := map[string]bool{}
			questions := make([]map[string]any, 0, len(in.Questions))
			for _, question := range in.Questions {
				question.ID = strings.TrimSpace(question.ID)
				question.Question = strings.TrimSpace(question.Question)
				if question.ID == "" || question.Question == "" || seen[question.ID] {
					return ToolResult{}, errors.New("question ids and text must be non-empty and ids must be unique")
				}
				seen[question.ID] = true
				item := map[string]any{"id": question.ID, "question": question.Question}
				if question.Header != "" {
					item["header"] = question.Header
				}
				if len(question.Options) > 0 {
					item["options"] = question.Options
				}
				if question.MultiSelect {
					item["multiSelect"] = true
				}
				questions = append(questions, item)
			}
			value, err := e.RequestInteraction(ctx, call.SessionID, "question/requested", map[string]any{"type": "question/requested", "sessionId": call.SessionID, "questions": questions})
			if err != nil {
				return ToolResult{}, err
			}
			if wrapper, ok := value.(map[string]any); ok {
				if answer, exists := wrapper["answer"]; exists {
					value = answer
				}
			}
			return jsonToolResult(value)
		},
	}
}

func builtinGetGoalTool(e *Engine) Tool {
	return Tool{
		Schema: ToolSchema{Name: "get_goal", Description: "Read the current same-session goal, including its exact id and revision.", Parameters: objectSchema(map[string]any{}), Output: goalToolOutputSchema()},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			if err := ctx.Err(); err != nil {
				return ToolResult{}, err
			}
			goal, err := e.GetGoal(call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			if goal == nil {
				return jsonToolResult(map[string]any{"goal": nil})
			}
			return jsonToolResult(map[string]any{"goal": goalWithoutActivation(*goal), "activation": goal.Activation})
		},
	}
}

func builtinCreateGoalTool(e *Engine) Tool {
	type input struct {
		Objective     string `json:"objective"`
		MaxGoalRounds int    `json:"max_goal_rounds"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "create_goal",
			Description: "Create one persisted same-session completion goal for a long-running objective. Execution rejects subagent authority.",
			Parameters: objectSchema(map[string]any{
				"objective":       map[string]any{"type": "string"},
				"max_goal_rounds": map[string]any{"type": "number"},
			}, "objective"),
			Output: goalToolOutputSchema(),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if err := requireDirectHumanGoalTurn(e, call.SessionID); err != nil {
				return ToolResult{}, err
			}
			if _, err := e.goalMutation(call.SessionID, "", "create", in.Objective, "", 0, in.MaxGoalRounds); err != nil {
				return ToolResult{}, err
			}
			goal, _ := e.GetGoal(call.SessionID)
			return jsonToolResult(map[string]any{"goal": goalWithoutActivation(*goal), "activation": goal.Activation})
		},
	}
}

func builtinUpdateGoalTool(e *Engine) Tool {
	type input struct {
		GoalID        string `json:"goal_id"`
		Revision      int    `json:"revision"`
		Action        string `json:"action"`
		Objective     string `json:"objective"`
		MaxGoalRounds int    `json:"max_goal_rounds"`
		BlockedReason string `json:"blocked_reason"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "update_goal",
			Description: "Update the exact current goal revision. Call get_goal first and copy its goal_id and revision.",
			Parameters: objectSchema(map[string]any{
				"goal_id":         map[string]any{"type": "string"},
				"revision":        map[string]any{"type": "number"},
				"action":          map[string]any{"type": "string", "enum": []string{"edit", "pause", "resume", "complete", "blocked"}},
				"objective":       map[string]any{"type": "string"},
				"max_goal_rounds": map[string]any{"type": "number"},
				"blocked_reason":  map[string]any{"type": "string"},
			}, "goal_id", "revision", "action"),
			Output: goalToolOutputSchema(),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			authority, err := goalToolAuthority(e, call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			op := in.Action
			if op == "blocked" {
				op = "block"
			}
			switch op {
			case "edit":
				if authority != "direct-human" {
					return ToolResult{}, errors.New("this goal operation requires a direct human turn on a top-level agent")
				}
				if in.BlockedReason != "" {
					return ToolResult{}, errors.New("blocked_reason is valid only with action blocked")
				}
			case "pause", "resume":
				if authority != "direct-human" {
					return ToolResult{}, errors.New("this goal operation requires a direct human turn on a top-level agent")
				}
				if in.Objective != "" || in.MaxGoalRounds != 0 || in.BlockedReason != "" {
					return ToolResult{}, errors.New("objective and max_goal_rounds are valid only with action edit; blocked_reason is valid only with action blocked")
				}
			case "complete":
				if in.Objective != "" || in.MaxGoalRounds != 0 || in.BlockedReason != "" {
					return ToolResult{}, errors.New("complete accepts no replacement fields")
				}
			case "block":
				if in.Objective != "" || in.MaxGoalRounds != 0 {
					return ToolResult{}, errors.New("objective and max_goal_rounds are valid only with action edit")
				}
				if strings.TrimSpace(in.BlockedReason) == "" {
					return ToolResult{}, errors.New("blocked_reason is required with action blocked")
				}
				if authority == "goal-round" {
					goal, getErr := e.GetGoal(call.SessionID)
					if getErr != nil {
						return ToolResult{}, getErr
					}
					if goal == nil || goal.RoundsStarted < goalBlockThreshold {
						return ToolResult{}, fmt.Errorf("blocked requires at least %d consecutive goal rounds; current round is %d", goalBlockThreshold, goal.RoundsStarted)
					}
				}
			default:
				return ToolResult{}, errors.New("goal-invalid-operation")
			}
			if _, err := e.goalMutation(call.SessionID, in.GoalID, op, in.Objective, in.BlockedReason, in.Revision, in.MaxGoalRounds); err != nil {
				return ToolResult{}, err
			}
			goal, _ := e.GetGoal(call.SessionID)
			return jsonToolResult(map[string]any{"goal": goalWithoutActivation(*goal), "activation": goal.Activation})
		},
	}
}

func goalToolAuthority(e *Engine, id string) (string, error) {
	s, err := e.getSession(id)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	if s.Header.Origin == "subagent" {
		s.mu.Unlock()
		return "", errors.New("this goal operation requires a direct human turn on a top-level agent")
	}
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	start := -1
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == "turn/end" {
			break
		}
		if events[i].Type == "turn/start" {
			start = i
			break
		}
	}
	if start < 0 {
		return "direct-human", nil
	}
	goal, _ := e.GetGoal(id)
	for _, event := range events[start+1:] {
		if event.Type != "user/message" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		source, _ := data["source"].(map[string]any)
		if source["kind"] == "user" {
			return "direct-human", nil
		}
		goalID, revision, round, ok := goalSource(source)
		if ok && goal != nil && goal.ID == goalID && goal.Revision == revision && goal.RoundsStarted == round {
			return "goal-round", nil
		}
	}
	return "", errors.New("complete and blocked require a direct human turn or the current goal round")
}

func requireDirectHumanGoalTurn(e *Engine, id string) error {
	authority, err := goalToolAuthority(e, id)
	if err != nil {
		return err
	}
	if authority != "direct-human" {
		return errors.New("this goal operation requires a direct human turn on a top-level agent")
	}
	return nil
}

func goalWithoutActivation(goal GoalView) map[string]any {
	value := map[string]any{"id": goal.ID, "revision": goal.Revision, "objective": goal.Objective, "phase": goal.Phase, "roundsStarted": goal.RoundsStarted, "maxGoalRounds": goal.MaxGoalRounds}
	if goal.BlockedReason != nil {
		value["blockedReason"] = goal.BlockedReason
	}
	return value
}

func inProcessSubagentTool(e *Engine, config SubagentToolConfig, fork bool) Tool {
	type input struct {
		Description     string `json:"description"`
		Prompt          string `json:"prompt"`
		RunInBackground *bool  `json:"run_in_background"`
	}
	name := config.ToolName
	continuable := config.BackgroundMode == "continuable"
	backgroundEnabled := config.EnableRunInBackground == nil || *config.EnableRunInBackground
	description := "Delegate a self-contained task to a separate subagent."
	properties := map[string]any{
		"description": map[string]any{"type": "string"},
		"prompt":      map[string]any{"type": "string"},
	}
	if fork {
		description = "Delegate a task to a subagent seeded with all completed turns from this conversation."
	}
	if backgroundEnabled {
		properties["run_in_background"] = map[string]any{"type": "boolean"}
		if continuable {
			description += " It runs in the background by default and remains available for later messages."
		} else {
			description += " It waits by default; set run_in_background to return a job id."
		}
	} else {
		description += " This call waits for the result."
	}
	return Tool{
		Schema: ToolSchema{Name: name, Description: description, Parameters: objectSchema(properties, "description", "prompt"), Output: subagentToolOutputSchema()},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			in.Description, in.Prompt = strings.TrimSpace(in.Description), strings.TrimSpace(in.Prompt)
			if in.Description == "" || in.Prompt == "" {
				return ToolResult{}, errors.New("subagent description and prompt are required")
			}
			if in.RunInBackground != nil && *in.RunInBackground && !backgroundEnabled {
				return ToolResult{}, errors.New("run_in_background is disabled for this tool")
			}
			background := false
			if backgroundEnabled {
				if in.RunInBackground != nil {
					background = *in.RunInBackground
				} else {
					background = continuable
				}
			}
			if background && !continuable {
				jobID, err := startBackgroundInProcessSubagent(e, call.SessionID, in.Description, in.Prompt, fork, config)
				if err != nil {
					return ToolResult{}, err
				}
				result := textToolResult("started background subagent job " + jobID)
				result.Value = map[string]any{"kind": "background", "jobId": jobID}
				return result, nil
			}
			mode := "one-shot"
			if continuable {
				mode = "continuable"
			}
			child, err := e.createModelSubagent(ctx, call.SessionID, in.Description, fork, mode, config)
			if err != nil {
				return ToolResult{}, err
			}
			request := PromptRequest{SessionID: child, Mode: "queue", Literal: true, Content: []PromptContentPart{{Type: "text", Text: in.Prompt}}}
			if background {
				go e.runContinuableModelSubagent(call.SessionID, child, request)
				result := textToolResult("started subagent " + child)
				result.Value = map[string]any{"kind": "continuable", "subagentId": child}
				return result, nil
			}
			output, err := e.Run(ctx, child, request)
			if err != nil {
				return ToolResult{}, err
			}
			blocks := []ContentBlock{}
			if output != "" {
				blocks = append(blocks, ContentBlock{Type: "text", Text: output})
			}
			result := textToolResult(output)
			result.Value = map[string]any{"kind": "foreground", "runId": child, "output": blocks}
			return result, nil
		},
	}
}

type sessionToolRestriction struct {
	allow    map[string]bool
	deny     map[string]bool
	allowSet bool
	denySet  bool
}

func (restriction *sessionToolRestriction) allows(name string) bool {
	if restriction == nil {
		return true
	}
	if restriction.allowSet && !restriction.allow[name] {
		return false
	}
	return !restriction.denySet || !restriction.deny[name]
}

func (e *Engine) resolveSessionToolRestriction(filter *SubagentToolFilter) (*sessionToolRestriction, error) {
	if filter == nil {
		return nil, nil
	}
	result := &sessionToolRestriction{
		allow: map[string]bool{}, deny: map[string]bool{},
		allowSet: filter.Allow != nil, denySet: filter.Deny != nil,
	}
	e.mu.RLock()
	known := make(map[string]bool, len(e.tools))
	for name := range e.tools {
		if e.toolOwners[name] == "" && name != "run_code" {
			known[name] = true
		}
	}
	e.mu.RUnlock()
	unknown := []string{}
	for _, row := range []struct {
		values []string
		target map[string]bool
	}{{filter.Allow, result.allow}, {filter.Deny, result.deny}} {
		for _, name := range row.values {
			name = strings.TrimSpace(name)
			if !known[name] {
				unknown = append(unknown, name)
				continue
			}
			row.target[name] = true
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("tool-subagent: toolFilter names unknown global tools %q", unknown)
	}
	return result, nil
}

func (e *Engine) createModelSubagent(ctx context.Context, parentID, label string, fork bool, mode string, config SubagentToolConfig) (string, error) {
	if config.MaxDepth != nil {
		depth, err := e.sessionDepth(parentID)
		if err != nil {
			return "", err
		}
		if depth >= *config.MaxDepth {
			return "", fmt.Errorf("subagent maximum depth %d reached", *config.MaxDepth)
		}
	}
	parent, err := e.getSession(parentID)
	if err != nil {
		return "", err
	}
	parent.mu.Lock()
	cwd, preset, depth, selection := parent.Header.CWD, sessionAgentPreset(parent.Header, parent.Events), parent.Header.DelegationDepth, parent.Model
	var events []Event
	if fork {
		events = append([]Event(nil), parent.Events...)
	}
	parent.mu.Unlock()
	if fork {
		cut := completedTurnCut(events, nil)
		if cut >= 0 {
			events = events[:cut+1]
		} else {
			events = nil
		}
	}
	restriction, err := e.resolveSessionToolRestriction(config.ToolFilter)
	if err != nil {
		return "", err
	}
	if config.AgentOptions != nil {
		if provider := strings.TrimSpace(config.AgentOptions.Provider); provider != "" {
			selection.Provider = provider
		}
		if model := strings.TrimSpace(config.AgentOptions.Model); model != "" {
			selection.Model = model
		}
		if config.AgentOptions.MaxTokens > 0 {
			selection.MaxTokens = config.AgentOptions.MaxTokens
		}
	}
	pinPermission := shouldPinPermissionSnapshot(preset)
	if fork {
		pinPermission = len(events) == 0
	}
	childID, err := e.createSession(ctx, SessionHeader{
		CWD: cwd, ParentSession: parentID, SeedLength: len(events), Origin: "subagent",
		DelegationDepth: depth + 1, AgentPreset: preset, Mode: mode,
	}, pinPermission)
	if err != nil {
		return "", err
	}
	child, _ := e.getSession(childID)
	child.mu.Lock()
	child.Model = selection
	child.Title = label
	child.personaOverride = config.Persona
	child.toolRestriction = restriction
	child.mu.Unlock()
	if err := e.saveState(); err != nil {
		return "", err
	}
	for _, event := range events {
		if _, err := e.appendSeedEvent(child, event); err != nil {
			return "", err
		}
	}
	if label = NormalizeSessionTitle(label, defaultTitleMaxBytes); label != "" {
		if _, err := e.appendSessionTitle(child, label, nil, SessionTitleSource{Kind: "subagent"}); err != nil {
			return "", err
		}
	}
	return childID, nil
}

func startBackgroundInProcessSubagent(e *Engine, owner, label, prompt string, fork bool, config SubagentToolConfig) (string, error) {
	return e.jobs.startManaged(owner, "subagent", label, 0, func() (*managedJobHandle, error) {
		ctx, cancel := context.WithCancelCause(context.Background())
		child, err := e.createModelSubagent(ctx, owner, label, fork, "one-shot", config)
		if err != nil {
			cancel(err)
			return nil, err
		}
		done := make(chan managedJobResult, 1)
		var mu sync.Mutex
		finished := false
		go func() {
			output, runErr := e.Run(ctx, child, PromptRequest{SessionID: child, Mode: "queue", Literal: true, Content: []PromptContentPart{{Type: "text", Text: prompt}}})
			result := managedJobResult{Status: jobCompleted, Output: output}
			if runErr != nil {
				result.Status, result.Output, result.Detail = jobFailed, "", runErr.Error()
				if ctx.Err() != nil {
					result.Status, result.Detail = jobKilled, "killed"
				}
			}
			mu.Lock()
			finished = true
			mu.Unlock()
			done <- result
			close(done)
		}()
		return &managedJobHandle{Done: done, Cancel: func(reason string) error {
			mu.Lock()
			defer mu.Unlock()
			if finished || ctx.Err() != nil {
				return nil
			}
			if strings.TrimSpace(reason) == "" {
				reason = "background subagent task killed"
			}
			cancel(errors.New(reason))
			_ = e.CancelSession(child)
			return nil
		}}, nil
	})
}

func (e *Engine) runContinuableModelSubagent(parentID, childID string, request PromptRequest) {
	output, err := e.Run(context.Background(), childID, request)
	e.notifyModelSubagentSettlement(parentID, childID, output, err)
}

func (e *Engine) notifyModelSubagentSettlement(parentID, childID, output string, runErr error) {
	reason := e.modelSubagentStopReason(childID, runErr)
	subject := "Background subagent " + childID
	summary := map[string]string{
		"completed":  subject + " finished and will do no further work unless you send it more.",
		"aborted":    subject + " was stopped before it finished.",
		"max-tokens": subject + " ran out of room before it finished.",
		"refusal":    subject + " declined the task.",
		"error":      subject + " failed before it finished.",
	}[reason]
	if summary == "" {
		summary = subject + " ended abnormally (" + reason + ") before it finished."
	}
	content := []PromptContentPart{{Type: "text", Text: summary}}
	if strings.TrimSpace(output) == "" {
		content = append(content, PromptContentPart{Type: "text", Text: "It left no closing message."})
	} else {
		content = append(content, PromptContentPart{Type: "text", Text: "Its closing message:"}, PromptContentPart{Type: "text", Text: output})
	}
	parent, err := e.getSession(parentID)
	if err != nil {
		return
	}
	parent.mu.Lock()
	running := parent.Running
	parent.mu.Unlock()
	request := PromptRequest{SessionID: parentID, Mode: "queue", Literal: true, Content: content, Source: map[string]any{
		"kind": "subagent-settled", "form": "notice", "summary": summary, "senderSessionId": childID,
	}}
	if running {
		request.Mode = "steer"
	}
	if _, _, err := e.enqueuePrompt(context.Background(), parentID, request, false); err != nil && request.Mode == "steer" {
		request.Mode = "queue"
		_, _, _ = e.enqueuePrompt(context.Background(), parentID, request, false)
	}
}

func (e *Engine) modelSubagentStopReason(childID string, runErr error) string {
	if errors.Is(runErr, context.Canceled) {
		return "aborted"
	}
	child, err := e.getSession(childID)
	if err == nil {
		child.mu.Lock()
		for index := len(child.Events) - 1; index >= 0; index-- {
			event := child.Events[index]
			if event.Type != "turn/end" {
				continue
			}
			data, _ := event.Data.(map[string]any)
			reason, _ := data["reason"].(map[string]any)
			kind, _ := reason["kind"].(string)
			if kind != "" {
				child.mu.Unlock()
				return kind
			}
		}
		child.mu.Unlock()
	}
	if runErr != nil {
		return "error"
	}
	return "completed"
}

func (e *Engine) setSessionTitle(id, title string) {
	s, err := e.getSession(id)
	if err != nil {
		return
	}
	title = NormalizeSessionTitle(title, defaultTitleMaxBytes)
	if title == "" {
		return
	}
	s.mu.Lock()
	s.Title = title
	s.mu.Unlock()
	_, _ = e.appendEvent(s, "session/title", map[string]any{"title": title, "messageSeqs": []int{}, "source": map[string]any{"kind": "user"}})
}

func (e *Engine) sessionDepth(id string) (int, error) {
	depth := 0
	seen := map[string]bool{}
	for id != "" {
		if seen[id] {
			return 0, errors.New("subagent lineage cycle")
		}
		seen[id] = true
		s, err := e.getSession(id)
		if err != nil {
			return 0, err
		}
		s.mu.Lock()
		parent := s.Header.ParentSession
		s.mu.Unlock()
		if parent == "" {
			return depth, nil
		}
		depth++
		id = parent
	}
	return depth, nil
}

func builtinSendMessageTool(e *Engine) Tool {
	type input struct {
		SubagentID string `json:"subagent_id"`
		Message    string `json:"message"`
	}
	return Tool{
		Schema: ToolSchema{Name: "send_message", Description: "Send a message to a direct continuable subagent as its next turn.", Parameters: objectSchema(map[string]any{"subagent_id": map[string]any{"type": "string"}, "message": map[string]any{"type": "string"}}, "subagent_id", "message"), Output: objectSchema(map[string]any{"messageId": map[string]any{"type": "string"}}, "messageId")},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			value, rpcErr := e.subagentPrompt(ctx, map[string]any{"parentSessionId": call.SessionID, "childSessionId": in.SubagentID, "content": []any{map[string]any{"type": "text", "text": in.Message}}})
			if rpcErr != nil {
				return ToolResult{}, rpcErr
			}
			messageID, _ := value.(map[string]any)["messageId"].(string)
			result := textToolResult(fmt.Sprintf("message queued as the next turn for subagent %s (%s)", in.SubagentID, messageID))
			result.Value = map[string]any{"messageId": messageID}
			return result, nil
		},
	}
}

func builtinInterruptAgentTool(e *Engine) Tool {
	type input struct {
		AgentID string `json:"agent_id"`
	}
	return Tool{
		Schema: ToolSchema{Name: "interrupt_agent", Description: "Request cancellation of a descendant agent's current turn by its agent id.", Parameters: objectSchema(map[string]any{"agent_id": map[string]any{"type": "string"}}, "agent_id"), Output: objectSchema(map[string]any{"accepted": map[string]any{"type": "boolean"}}, "accepted")},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if err := ctx.Err(); err != nil {
				return ToolResult{}, err
			}
			ok, err := e.isDescendant(call.SessionID, in.AgentID)
			if err != nil {
				return ToolResult{}, err
			}
			if !ok {
				return ToolResult{}, errors.New("target agent is not a descendant of the caller")
			}
			if err := e.CancelSession(in.AgentID); err != nil {
				return ToolResult{}, err
			}
			result := textToolResult("interrupt requested for agent " + in.AgentID)
			result.Value = map[string]any{"accepted": true}
			return result, nil
		},
	}
}

func (e *Engine) isDescendant(parentID, childID string) (bool, error) {
	if parentID == "" || childID == "" || parentID == childID {
		return false, nil
	}
	seen := map[string]bool{}
	for childID != "" {
		if seen[childID] {
			return false, errors.New("subagent lineage cycle")
		}
		seen[childID] = true
		s, err := e.getSession(childID)
		if err != nil {
			return false, err
		}
		s.mu.Lock()
		owner, origin := s.Header.ParentSession, s.Header.Origin
		s.mu.Unlock()
		if origin != "subagent" {
			return false, nil
		}
		if owner == parentID {
			return true, nil
		}
		childID = owner
	}
	return false, nil
}

type agentListEntry struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Label  string `json:"label"`
	Status string `json:"status"`
	Parent string `json:"parent,omitempty"`
	Depth  int    `json:"depth,omitempty"`
}

func builtinListAgentsTool(e *Engine) Tool {
	type input struct {
		Scope string `json:"scope"`
	}
	return Tool{
		Schema: ToolSchema{Name: "list_agents", Description: "List continuable background subagents by durable id and label.", Parameters: objectSchema(map[string]any{"scope": map[string]any{"type": "string", "enum": []string{"children", "descendants"}}}), Output: map[string]any{"type": "array", "items": objectSchema(map[string]any{
			"kind": map[string]any{"type": "string"}, "id": map[string]any{"type": "string"}, "label": map[string]any{"type": "string"}, "status": map[string]any{"type": "string"},
			"parent": map[string]any{"type": "string"}, "depth": map[string]any{"type": "integer"},
		}, "kind", "id", "label", "status")}},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if in.Scope == "" {
				in.Scope = "children"
			}
			if in.Scope != "children" && in.Scope != "descendants" {
				return ToolResult{}, errors.New("scope must be children or descendants")
			}
			entries, err := e.listModelAgents(ctx, call.SessionID, in.Scope == "descendants")
			if err != nil {
				return ToolResult{}, err
			}
			if len(entries) == 0 {
				result := textToolResult("(no subagents)")
				result.Value = []agentListEntry{}
				return result, nil
			}
			lines := make([]string, 0, len(entries))
			for _, entry := range entries {
				at := ""
				if in.Scope == "descendants" {
					at = fmt.Sprintf(" parent=%s depth=%d", entry.Parent, entry.Depth)
				}
				lines = append(lines, fmt.Sprintf("%s [%s]%s — %s", entry.ID, entry.Status, at, entry.Label))
			}
			result := textToolResult(strings.Join(lines, "\n"))
			result.Value = entries
			return result, nil
		},
	}
}

func (e *Engine) listModelAgents(ctx context.Context, parentID string, descendants bool) ([]agentListEntry, error) {
	if _, err := e.getSession(parentID); err != nil {
		return nil, err
	}
	type row struct {
		id, parent, origin, mode, label string
		created                         int64
		running, attached               bool
	}
	e.mu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.mu.RUnlock()
	rows := make([]row, 0, len(sessions))
	for _, session := range sessions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		session.mu.Lock()
		rows = append(rows, row{id: session.Header.ID, parent: session.Header.ParentSession, origin: session.Header.Origin, mode: session.Header.Mode, label: session.Title, created: session.Header.CreatedAt, running: session.Running, attached: session.attached})
		session.mu.Unlock()
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].created != rows[j].created {
			return rows[i].created < rows[j].created
		}
		return rows[i].id < rows[j].id
	})
	children := map[string][]row{}
	for _, item := range rows {
		if item.origin == "subagent" && item.mode == "continuable" {
			children[item.parent] = append(children[item.parent], item)
		}
	}
	result := []agentListEntry{}
	var visit func(string, int)
	visit = func(parent string, depth int) {
		for _, item := range children[parent] {
			status := "ready"
			if item.running {
				status = "running"
			} else if item.attached {
				status = "idle"
			}
			entry := agentListEntry{Kind: "child", ID: item.id, Label: item.label, Status: status}
			if descendants {
				entry.Parent, entry.Depth = parent, depth
			}
			result = append(result, entry)
			if descendants {
				visit(item.id, depth+1)
			}
		}
	}
	visit(parentID, 1)
	return result, nil
}
