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
	tools := make([]Tool, 0, 12)
	if e.hostPluginActive("@deepseek-ai/dsh-user-questions") {
		tools = append(tools, builtinAskUserTool(e))
	}
	if e.hostPluginActive("@deepseek-ai/dsh-plan-mode") {
		tools = append(tools, builtinExitPlanModeTool(e))
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tool-goal") {
		tools = append(tools, builtinGetGoalTool(e), builtinCreateGoalTool(e), builtinUpdateGoalTool(e))
	}
	if e.agentTeams == nil && e.hostPluginActive("@deepseek-ai/dsh-tool-subagent-control") {
		tools = append(tools, builtinSendMessageTool(e), builtinInterruptAgentTool(e), builtinListAgentsTool(e))
	} else if e.agentTeams != nil && e.hostPluginActive("@deepseek-ai/dsh-experimental-tool-agent-team") {
		for _, teamTool := range builtinAgentTeamTools(e) {
			if legacy, ok := legacyToolShadowedByAgentTeam(e, teamTool.Schema.Name); ok {
				teamTool = muxAgentTeamTool(e, teamTool, legacy)
			}
			tools = append(tools, teamTool)
		}
	}
	if err := registerTools(e, tools); err != nil {
		return err
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tool-subagent") {
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
	}
	return registerDynamicCordisTools(e)
}

func registerTools(e *Engine, tools []Tool) error {
	for _, tool := range tools {
		if err := e.RegisterTool(tool); err != nil {
			return err
		}
	}
	return nil
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
			if err := ctx.Err(); err != nil {
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
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			if err := decodeToolArguments(exec.Call, &struct{}{}); err != nil {
				return ToolResult{}, err
			}
			if _, _, _, err := goalToolExecution(e, exec.Call.SessionID); err != nil {
				return ToolResult{}, err
			}
			goal, err := e.GetGoal(exec.Call.SessionID)
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
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(exec.Call, &in); err != nil {
				return ToolResult{}, err
			}
			if err := requireDirectHumanGoalTurn(e, exec.Call.SessionID); err != nil {
				return ToolResult{}, err
			}
			if _, err := e.goalMutation(exec.Call.SessionID, "", "create", in.Objective, "", 0, in.MaxGoalRounds); err != nil {
				return ToolResult{}, goalToolDomainError(err)
			}
			goal, _ := e.GetGoal(exec.Call.SessionID)
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
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(exec.Call, &in); err != nil {
				return ToolResult{}, err
			}
			authority, err := goalToolAuthority(e, exec.Call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			if in.GoalID == "" || strings.TrimSpace(in.GoalID) != in.GoalID || in.Revision < 1 {
				return ToolResult{}, goalToolPolicyError("GOAL_TOOL_INVALID_UPDATE", "goal_id must be non-empty and revision must be a positive integer")
			}
			op := in.Action
			if op == "blocked" {
				op = "block"
			}
			switch op {
			case "edit":
				if authority != "direct-human" {
					return ToolResult{}, goalToolPolicyError("GOAL_TOOL_AUTHORITY_REQUIRED", "this goal operation requires a direct human turn on a top-level agent")
				}
				if in.BlockedReason != "" {
					return ToolResult{}, goalToolPolicyError("GOAL_TOOL_INVALID_UPDATE", "blocked_reason is valid only with action blocked")
				}
			case "pause", "resume":
				if authority != "direct-human" {
					return ToolResult{}, goalToolPolicyError("GOAL_TOOL_AUTHORITY_REQUIRED", "this goal operation requires a direct human turn on a top-level agent")
				}
				if in.Objective != "" || in.MaxGoalRounds != 0 || in.BlockedReason != "" {
					return ToolResult{}, goalToolPolicyError("GOAL_TOOL_INVALID_UPDATE", "objective and max_goal_rounds are valid only with action edit; blocked_reason is valid only with action blocked")
				}
			case "complete":
				if in.Objective != "" || in.MaxGoalRounds != 0 || in.BlockedReason != "" {
					return ToolResult{}, goalToolPolicyError("GOAL_TOOL_INVALID_UPDATE", "complete accepts no replacement fields")
				}
			case "block":
				if in.Objective != "" || in.MaxGoalRounds != 0 {
					return ToolResult{}, goalToolPolicyError("GOAL_TOOL_INVALID_UPDATE", "objective and max_goal_rounds are valid only with action edit")
				}
				if strings.TrimSpace(in.BlockedReason) == "" {
					return ToolResult{}, goalToolPolicyError("GOAL_TOOL_INVALID_UPDATE", "blocked_reason is required with action blocked")
				}
				if authority == "goal-round" {
					goal, getErr := e.GetGoal(exec.Call.SessionID)
					if getErr != nil {
						return ToolResult{}, getErr
					}
					s, _ := e.getSession(exec.Call.SessionID)
					runtimeConfig, runtimeErr := e.runtimeForSession(s)
					if runtimeErr != nil {
						return ToolResult{}, runtimeErr
					}
					threshold := runtimeConfig.goalBlockThreshold
					currentRound := 0
					if goal != nil {
						currentRound = goal.RoundsStarted
					}
					if currentRound < threshold {
						return ToolResult{}, goalToolPolicyError("GOAL_TOOL_BLOCK_THRESHOLD", fmt.Sprintf("blocked requires at least %d consecutive goal rounds; current round is %d", threshold, currentRound))
					}
				}
			default:
				return ToolResult{}, goalToolPolicyError("GOAL_TOOL_INVALID_UPDATE", "goal-invalid-operation")
			}
			before, getErr := e.GetGoal(exec.Call.SessionID)
			if getErr != nil {
				return ToolResult{}, getErr
			}
			if _, err := e.goalMutation(exec.Call.SessionID, in.GoalID, op, in.Objective, in.BlockedReason, in.Revision, in.MaxGoalRounds); err != nil {
				return ToolResult{}, goalToolDomainError(err)
			}
			goal, _ := e.GetGoal(exec.Call.SessionID)
			if authority == "goal-round" && before != nil && (op == "complete" || op == "block") {
				exec.DeferContext(goalWrapupContext(before.Objective, in.Action, in.BlockedReason))
			}
			return jsonToolResult(map[string]any{"goal": goalWithoutActivation(*goal), "activation": goal.Activation})
		},
	}
}

func goalToolAuthority(e *Engine, id string) (string, error) {
	s, events, start, err := goalToolExecution(e, id)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	isRoot := s.Header.Origin != "subagent"
	s.mu.Unlock()
	goal, _ := e.GetGoal(id)
	for _, event := range events[start+1:] {
		if event.Type != "user/message" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		source, _ := data["source"].(map[string]any)
		if isRoot && source["kind"] == "user" {
			return "direct-human", nil
		}
		goalID, revision, round, ok := goalSource(source)
		if ok && goal != nil && goal.ID == goalID && goal.Revision == revision && goal.RoundsStarted == round {
			return "goal-round", nil
		}
	}
	return "", goalToolPolicyError("GOAL_TOOL_AUTHORITY_REQUIRED", "complete and blocked require a direct human turn or the current goal round")
}

func goalToolExecution(e *Engine, id string) (*Session, []Event, int, error) {
	s, err := e.getSession(id)
	if err != nil {
		return nil, nil, -1, err
	}
	s.mu.Lock()
	running := s.Running
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	if !running {
		return nil, nil, -1, goalToolPolicyError("GOAL_TOOL_DRIVER_REQUIRED", "goal tools require the exact live calling agent inside its active driver")
	}
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
		return nil, nil, -1, goalToolPolicyError("GOAL_TOOL_DRIVER_REQUIRED", "goal tools require an open model turn")
	}
	return s, events, start, nil
}

func requireDirectHumanGoalTurn(e *Engine, id string) error {
	authority, err := goalToolAuthority(e, id)
	if err != nil {
		return err
	}
	if authority != "direct-human" {
		return goalToolPolicyError("GOAL_TOOL_AUTHORITY_REQUIRED", "this goal operation requires a direct human turn on a top-level agent")
	}
	return nil
}

func goalToolPolicyError(code, message string) error {
	return fmt.Errorf("%s: %s", code, message)
}

func goalToolDomainError(err error) error {
	code := map[string]string{
		"goal-not-found":          "GOAL_NOT_FOUND",
		"goal-conflict":           "GOAL_STALE_REVISION",
		"goal-already-exists":     "GOAL_ALREADY_EXISTS",
		"goal-invalid-objective":  "GOAL_INVALID_OBJECTIVE",
		"goal-invalid-max-rounds": "GOAL_INVALID_MAX_ROUNDS",
		"goal-invalid-edit":       "GOAL_INVALID_EDIT",
		"goal-invalid-transition": "GOAL_INVALID_TRANSITION",
		"goal-invalid-block":      "GOAL_INVALID_BLOCK_REASON",
		"goal-invalid-operation":  "GOAL_INVALID_OPERATION",
	}[err.Error()]
	if code == "" {
		if strings.HasPrefix(err.Error(), "goal-persist-failed:") {
			code = "GOAL_PERSIST_FAILED"
		} else {
			return err
		}
	}
	return goalToolPolicyError(code, err.Error())
}

func goalWrapupContext(objective, operation, blockedReason string) ToolContext {
	heading := fmt.Sprintf("Objective: %q\n", objective)
	text := ""
	if operation == "complete" {
		text = "<goal_complete>\n" + heading +
			"The goal is marked complete and this autonomous run is ending. Write the closing message to the user now: state the outcome, summarize what was done and how it was verified, and point to the concrete results (files, commits, or other artifacts). " +
			"Report only what earlier rounds and tool results in this session actually establish; when a detail is not in the session, say so instead of inventing it. " +
			"Note anything the user should review or do next. Address the user directly. Do not call any more tools in this run; further work waits for the user's next instruction.\n</goal_complete>"
	} else {
		text = "<goal_blocked>\n" + heading + fmt.Sprintf("Blocked: %q\n", strings.TrimSpace(blockedReason)) +
			"The goal is marked blocked and this autonomous run is ending. Write the closing message to the user now: state what has been completed so far, describe the concrete blocking condition and what you tried, and say exactly what you need from the user to continue. " +
			"Report only what earlier rounds and tool results in this session actually establish; when a detail is not in the session, say so instead of inventing it. " +
			"Address the user directly. Do not call any more tools in this run; further work waits for the user's next instruction.\n</goal_blocked>"
	}
	return ToolContext{
		Content: []ContentBlock{{Type: "text", Text: text}},
		Source: map[string]any{
			"kind": "plugin", "plugin": "tool-goal", "form": "notice", "summary": operation + ": " + objective,
		},
	}
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
		Description     string  `json:"description"`
		Prompt          string  `json:"prompt"`
		RunInBackground *bool   `json:"run_in_background"`
		Provider        *string `json:"provider"`
		Model           *string `json:"model"`
		ReasoningEffort *string `json:"reasoning_effort"`
	}
	name := config.ToolName
	continuable := config.BackgroundMode == "continuable"
	backgroundEnabled := config.EnableRunInBackground == nil || *config.EnableRunInBackground
	description := "Delegate a self-contained task to a separate subagent."
	properties := map[string]any{
		"description": map[string]any{"type": "string"},
		"prompt":      map[string]any{"type": "string"},
	}
	if config.ModelSelectionSettings {
		properties["provider"] = map[string]any{"type": "string", "description": "LLM provider route for the child. Supply together with model; omit both to use configured child defaults or inherit the parent route."}
		properties["model"] = map[string]any{"type": "string", "description": "Model id interpreted by provider. Supply together with provider; omit both to use configured child defaults or inherit the parent route."}
		properties["reasoning_effort"] = map[string]any{"type": "string", "description": "Adapter-owned reasoning effort for the effective child route."}
		description += " Child LLM selection is optional; use list_subagent_models to inspect allowed routes and efforts."
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
		Schema:                        ToolSchema{Name: name, Description: description, Parameters: objectSchema(properties, "description", "prompt"), Output: subagentToolOutputSchema()},
		subagentModelSelectionCapable: config.ModelSelectionSettings,
		IsConcurrencySafe:             alwaysConcurrencySafe,
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if err := ctx.Err(); err != nil {
				return ToolResult{}, err
			}
			in.Description, in.Prompt = strings.TrimSpace(in.Description), strings.TrimSpace(in.Prompt)
			if in.Description == "" || in.Prompt == "" {
				return ToolResult{}, errors.New("subagent description and prompt are required")
			}
			parent, err := e.getSession(call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			var routes []AllowedModelRoute
			modelSelectionEnabled := false
			if config.ModelSelectionSettings {
				routes, modelSelectionEnabled, err = e.sampleSubagentModelSelection(parent)
				if err != nil {
					return ToolResult{}, err
				}
			}
			modelRequest := subagentModelRequest{Provider: in.Provider, Model: in.Model, ReasoningEffort: in.ReasoningEffort}
			parentSelection := parentModelSelectionForDelegation(parent)
			requiresRoutePreflight := hasSubagentModelRequest(modelRequest) || hasConfiguredSubagentLlm(config.AgentOptions)
			var providerBinding *subagentProviderBinding
			if requiresRoutePreflight {
				binding, err := e.snapshotSubagentProvider(config.Provider)
				if err != nil {
					return ToolResult{}, err
				}
				providerBinding = &binding
			}
			agentOptions, err := requestedSubagentAgentOptions(parentSelection, config.AgentOptions, modelRequest, config.ModelSelectionSettings && modelSelectionEnabled)
			if err != nil {
				return ToolResult{}, err
			}
			if hasSubagentModelRequest(modelRequest) {
				providerID, modelID := parentSelection.Provider, parentSelection.Model
				if agentOptions != nil {
					if agentOptions.Provider != "" {
						providerID = agentOptions.Provider
					}
					if agentOptions.Model != "" {
						modelID = agentOptions.Model
					}
				}
				if providerID == "" || modelID == "" {
					return ToolResult{}, errors.New("cannot select child LLM values without an effective provider and model")
				}
				if !allowedSubagentRoute(routes, providerID, modelID) {
					return ToolResult{}, fmt.Errorf("child LLM route %q/%q is not allowed for this Session", providerID, modelID)
				}
			}
			if requiresRoutePreflight {
				if err := e.preflightSubagentLlm(ctx, parentSelection, agentOptions, true); err != nil {
					return ToolResult{}, err
				}
			}
			callConfig := config
			callConfig.AgentOptions = agentOptions
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
				request := SubagentStartRequest{
					ParentSessionID: call.SessionID, CWD: call.Workspace, Label: in.Description,
					Prompt: []ContentBlock{{Type: "text", Text: in.Prompt}}, MaxDepth: config.MaxDepth,
					AgentOptions: agentOptions, Persona: config.Persona, ToolFilter: config.ToolFilter,
				}
				jobID, err := startBackgroundSubagent(e, call.SessionID, in.Description, config.Provider, providerBinding, request)
				if err != nil {
					return ToolResult{}, err
				}
				result := textToolResult("started background subagent job " + jobID)
				result.Value = map[string]any{"kind": "background", "jobId": jobID}
				return result, nil
			}
			if background {
				if providerBinding != nil {
					if err := e.validateSubagentProviderBinding(*providerBinding); err != nil {
						return ToolResult{}, err
					}
				}
				child, _, err := e.startContinuableModelSubagent(ctx, call.SessionID, in.Description, in.Prompt, fork, callConfig)
				if err != nil {
					return ToolResult{}, err
				}
				result := textToolResult("started subagent " + child)
				result.Value = map[string]any{"kind": "continuable", "subagentId": child}
				return result, nil
			}
			request := SubagentStartRequest{
				ParentSessionID: call.SessionID, CWD: call.Workspace, Label: in.Description,
				Prompt: []ContentBlock{{Type: "text", Text: in.Prompt}}, MaxDepth: config.MaxDepth,
				AgentOptions: agentOptions, Persona: config.Persona, ToolFilter: config.ToolFilter,
			}
			var run *SubagentRun
			if providerBinding != nil {
				run, err = e.startSubagentWithBinding(ctx, *providerBinding, request)
			} else {
				run, err = e.StartSubagent(ctx, config.Provider, request)
			}
			if err != nil {
				return ToolResult{}, err
			}
			settled, err := settleForegroundSubagent(run)
			if err != nil {
				return ToolResult{}, err
			}
			output := contentValueText(settled.Output)
			result := textToolResult(output)
			result.Value = map[string]any{"kind": "foreground", "runId": run.ID, "output": settled.Output}
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

type delegatedPolicyOverrides struct {
	sandboxMode string
}

func captureDelegatedPolicyOverrides(events []Event) delegatedPolicyOverrides {
	overrides := delegatedPolicyOverrides{}
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type != "sandbox/mode" {
			continue
		}
		data, _ := events[index].Data.(map[string]any)
		mode, _ := data["mode"].(string)
		if sandboxPresetModes[mode] != "" {
			overrides.sandboxMode = mode
		}
		break
	}
	return overrides
}

func (e *Engine) appendDelegatedPolicyOverrides(child *Session, overrides delegatedPolicyOverrides) error {
	if overrides.sandboxMode != "" {
		if _, err := e.appendEvent(child, "sandbox/mode", map[string]any{"mode": overrides.sandboxMode, "source": "delegation"}); err != nil {
			return err
		}
	}
	_, err := e.appendEvent(child, "approval/policy", map[string]any{"policy": "never", "source": "delegation"})
	return err
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
	return e.createModelSubagentWithSetup(ctx, parentID, label, fork, mode, config, nil)
}

func (e *Engine) createModelSubagentWithSetup(ctx context.Context, parentID, label string, fork bool, mode string, config SubagentToolConfig, setup func(*Session) error) (string, error) {
	childID := newID("ses")
	unlock := e.lockModelSubagent(childID)
	defer unlock()
	return e.createModelSubagentWithIDLocked(ctx, parentID, childID, label, fork, mode, config, setup)
}

func modelSubagentDescriptor(mode, label string, selection ModelSelection, config SubagentToolConfig) (SubagentDescriptorData, error) {
	descriptor := SubagentDescriptorData{Mode: mode, Provider: config.Provider}
	if label != "" || mode == "continuable" {
		descriptor.Label = descriptorString(label)
	}
	if mode == "continuable" {
		descriptor.AgentProvider = descriptorString(selection.Provider)
		descriptor.AgentModel = descriptorString(selection.Model)
		if config.Persona != "" {
			descriptor.Persona = descriptorString(config.Persona)
		}
		descriptor.ToolFilter = config.ToolFilter
	}
	return SnapshotSubagentDescriptor(descriptor)
}

func (e *Engine) createModelSubagentWithID(ctx context.Context, parentID, childID, label string, fork bool, mode string, config SubagentToolConfig) (string, error) {
	if childID == "" {
		childID = newID("ses")
	}
	unlock := e.lockModelSubagent(childID)
	defer unlock()
	return e.createModelSubagentWithIDLocked(ctx, parentID, childID, label, fork, mode, config, nil)
}

func (e *Engine) createModelSubagentWithIDLocked(ctx context.Context, parentID, childID, label string, fork bool, mode string, config SubagentToolConfig, setup func(*Session) error) (string, error) {
	parent, err := e.getSession(parentID)
	if err != nil {
		return "", err
	}
	parent.mu.Lock()
	cwd, preset, depth, selection := parent.Header.CWD, sessionAgentPreset(parent.Header, parent.Events), parent.Header.DelegationDepth, parent.Model
	if latest, ok := latestLoggedModel(parent.Events); ok {
		selection = latest
	}
	inheritedPolicy := captureDelegatedPolicyOverrides(parent.Events)
	parentEvents := append([]Event(nil), parent.Events...)
	available := parent.attached && !parent.draining
	var events []Event
	if fork {
		events = append([]Event(nil), parentEvents...)
	}
	parent.mu.Unlock()
	if !available {
		return "", errors.New("subagent-parent-unavailable: parent session is not resident")
	}
	if int64(depth) >= maxJSONSafeInteger {
		return "", errors.New("subagent child depth exceeds the safe-integer range")
	}
	childDepth := depth + 1
	if config.MaxDepth != nil && childDepth > *config.MaxDepth {
		return "", fmt.Errorf("subagent depth %d exceeds maxDepth %d", childDepth, *config.MaxDepth)
	}
	if fork {
		cut := completedTurnCut(events, nil)
		if cut >= 0 {
			events = events[:cut+1]
		} else {
			events = nil
		}
	}
	e.mu.RLock()
	existing := e.sessions[childID]
	e.mu.RUnlock()
	if existing != nil {
		return "", fmt.Errorf("subagent %q already exists", childID)
	}
	restriction, err := e.resolveSessionToolRestriction(config.ToolFilter)
	if err != nil {
		return "", err
	}
	if config.AgentOptions != nil {
		originalProvider, originalModel := selection.Provider, selection.Model
		if provider := strings.TrimSpace(config.AgentOptions.Provider); provider != "" {
			selection.Provider = provider
		}
		if model := strings.TrimSpace(config.AgentOptions.Model); model != "" {
			selection.Model = model
		}
		if config.AgentOptions.MaxTokens > 0 {
			selection.MaxTokens = config.AgentOptions.MaxTokens
		}
		if config.AgentOptions.ReasoningEffort != "" {
			selection.ReasoningEffort = config.AgentOptions.ReasoningEffort
		} else if selection.Provider != originalProvider || selection.Model != originalModel {
			selection.ReasoningEffort = ""
		}
	}
	childID, err = e.createSessionWithPresetAdoption(ctx, SessionHeader{
		ID: childID, CWD: cwd, ParentSession: parentID, SeedLength: len(events), Origin: "subagent",
		DelegationDepth: childDepth, AgentPreset: preset, Mode: mode,
	}, false, false, false)
	if err != nil {
		return "", err
	}
	child, _ := e.getSession(childID)
	child.mu.Lock()
	detachSessionLocked(child)
	child.mu.Unlock()
	committed := false
	defer func() {
		if !committed {
			e.rollbackUnpublishedModelSubagent(childID, child)
		}
	}()
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
	if err := e.inheritSubagentModelSelection(child, parentEvents); err != nil {
		return "", err
	}
	if err := e.appendDelegatedPolicyOverrides(child, inheritedPolicy); err != nil {
		return "", err
	}
	descriptor, err := modelSubagentDescriptor(mode, label, selection, config)
	if err != nil {
		return "", err
	}
	if mode == "one-shot" {
		child.mu.Lock()
		child.initialSubagentDescriptor = &descriptor
		child.mu.Unlock()
	} else if _, err := e.appendEvent(child, "subagent/descriptor", descriptor.eventData()); err != nil {
		return "", err
	}
	if label = NormalizeSessionTitle(label, defaultTitleMaxBytes); label != "" {
		if _, err := e.appendSessionTitle(child, label, nil, SessionTitleSource{Kind: "subagent"}); err != nil {
			return "", err
		}
	}
	if setup != nil {
		if err := setup(child); err != nil {
			return "", err
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	committed = true
	e.publishDeferredSession(child)
	return childID, nil
}

func (e *Engine) rollbackUnpublishedModelSubagent(childID string, child *Session) {
	if child == nil {
		return
	}
	child.mu.Lock()
	detachSessionLocked(child)
	child.mu.Unlock()
	e.releaseSessionScopedTools(childID)
	_ = e.terminals.closeOwner(childID)
	e.shells.closeOwner(childID)
	e.mu.Lock()
	if e.sessions[childID] == child {
		delete(e.sessions, childID)
	}
	_ = e.saveStateLocked()
	e.mu.Unlock()
	rollbackSessionStoreCreate(child.store, childID)
}

func (e *Engine) rollbackCreatedModelSubagent(childID string, child *Session) {
	if child == nil {
		return
	}
	_ = detachSDKSession(e, childID)
	e.mu.Lock()
	if e.sessions[childID] == child {
		delete(e.sessions, childID)
	}
	_ = e.saveStateLocked()
	e.mu.Unlock()
	rollbackSessionStoreCreate(child.store, childID)
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
			startSeq := e.modelSubagentEventCount(child)
			runOutput, runErr := e.Run(ctx, child, PromptRequest{SessionID: child, Mode: "queue", Literal: true, Content: []PromptContentPart{{Type: "text", Text: prompt}}})
			blocks := e.modelSubagentOutputSince(child, startSeq)
			output := contentValueText(blocks)
			if output == "" {
				output = runOutput
			}
			disposeErr := e.disposeOneShotModelSubagent(child)
			runErr = errors.Join(runErr, disposeErr)
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

func (e *Engine) modelSubagentEventCount(id string) int {
	session, err := e.getSession(id)
	if err != nil {
		return 0
	}
	session.mu.Lock()
	count := len(session.Events)
	session.mu.Unlock()
	return count
}

func (e *Engine) modelSubagentOutputSince(id string, start int) []ContentBlock {
	session, err := e.getSession(id)
	if err != nil {
		return nil
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	if start < 0 || start > len(events) {
		return nil
	}
	return finalAssistantOutput(events[start:])
}

func (e *Engine) notifyModelSubagentSettlement(parentID, childID string, output []ContentBlock, reason string) {
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
	content := []ContentBlock{{Type: "text", Text: summary}}
	if len(output) == 0 {
		content = append(content, ContentBlock{Type: "text", Text: "It left no closing message."})
	} else {
		content = append(content, ContentBlock{Type: "text", Text: "Its closing message:"})
		content = append(content, cloneContentBlocks(output)...)
	}
	parent, err := e.getSession(parentID)
	if err != nil {
		return
	}
	parent.mu.Lock()
	attached := parent.attached
	parent.mu.Unlock()
	if !attached {
		return
	}
	e.modelSubagentMu.Lock()
	parentActivation := e.modelSubagentActivations[parentID]
	parentDisposing := parentActivation != nil && parentActivation.disposing
	e.modelSubagentMu.Unlock()
	source := map[string]any{
		"kind": "subagent-settled", "form": "notice", "summary": summary, "senderSessionId": childID,
	}
	if parentDisposing {
		_, _ = e.enqueueTeamPrompt(parent, content, source, "next-step", false)
		return
	}
	_, _ = e.enqueueTeamPrompt(parent, content, source, "next-step", true)
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

func builtinSendMessageTool(e *Engine) Tool {
	type input struct {
		AgentID string `json:"agent_id"`
		Message string `json:"message"`
	}
	return Tool{
		Schema: ToolSchema{Name: "send_message", Description: "Send a message to a direct continuable child by its agent id, or from a resident continuable child to its direct parent. A running target receives it at the nearest step boundary; an idle target starts a turn.", Parameters: objectSchema(map[string]any{"agent_id": map[string]any{"type": "string"}, "message": map[string]any{"type": "string"}}, "agent_id", "message"), Output: objectSchema(map[string]any{"messageId": map[string]any{"type": "string"}}, "messageId")},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			messageID, err := e.SendAdjacentAgentMessage(ctx, call.SessionID, in.AgentID, []ContentBlock{{Type: "text", Text: in.Message}})
			if err != nil {
				return ToolResult{}, err
			}
			result := textToolResult(fmt.Sprintf("message delivered to agent %s (%s)", in.AgentID, messageID))
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
			e.modelSubagentMu.Lock()
			activation := e.modelSubagentActivations[in.AgentID]
			e.modelSubagentMu.Unlock()
			if activation == nil || activation.disposing {
				result := textToolResult("interrupt requested for agent " + in.AgentID)
				result.Value = map[string]any{"accepted": true}
				return result, nil
			}
			ok, err := e.isDescendant(call.SessionID, in.AgentID)
			if err != nil {
				return ToolResult{}, err
			}
			if !ok {
				return ToolResult{}, errors.New("target agent is not a descendant of the caller")
			}
			if err := e.CancelAgent(in.AgentID, AgentCancelCause{Kind: "parent"}, CancelAgentOptions{KeepInbox: true}); err != nil {
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
	Reason string `json:"reason"`
	Parent string `json:"parent,omitempty"`
	Depth  int    `json:"depth,omitempty"`
}

func (entry agentListEntry) value(descendants bool) map[string]any {
	value := map[string]any{"kind": entry.Kind, "id": entry.ID}
	if entry.Kind == "diagnostic" {
		value["reason"] = entry.Reason
	} else {
		value["label"], value["status"] = entry.Label, entry.Status
	}
	if descendants {
		value["parent"], value["depth"] = entry.Parent, entry.Depth
	}
	return value
}

func builtinListAgentsTool(e *Engine) Tool {
	type input struct {
		Scope string `json:"scope"`
	}
	return Tool{
		Schema: ToolSchema{Name: "list_agents", Description: "List continuable background subagents by durable id and label. Status is running for active work, idle for a resident agent between turns, and ready for a resumable conversation that is only in storage. Children that cannot be interpreted are returned as diagnostics. Scope descendants walks the complete session tree in stable pre-order; only depth-1 children accept send_message.", Parameters: objectSchema(map[string]any{"scope": map[string]any{"type": "string", "enum": []string{"children", "descendants"}}}), Output: map[string]any{"type": "array", "items": map[string]any{"oneOf": []any{
			objectSchema(map[string]any{
				"kind": map[string]any{"type": "string", "const": "child"}, "id": map[string]any{"type": "string"}, "label": map[string]any{"type": "string"},
				"status": map[string]any{"type": "string", "enum": []string{"running", "idle", "ready"}}, "parent": map[string]any{"type": "string"}, "depth": map[string]any{"type": "integer"},
			}, "kind", "id", "label", "status"),
			objectSchema(map[string]any{
				"kind": map[string]any{"type": "string", "const": "diagnostic"}, "id": map[string]any{"type": "string"},
				"reason": map[string]any{"type": "string", "enum": []string{"corrupt", "unsupported", "unavailable"}}, "parent": map[string]any{"type": "string"}, "depth": map[string]any{"type": "integer"},
			}, "kind", "id", "reason"),
		}}}},
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
			if call.SessionID == "" {
				return ToolResult{}, errors.New("list_agents requires a calling agent session")
			}
			entries, err := e.listModelAgents(ctx, call.SessionID, in.Scope == "descendants")
			if err != nil {
				return ToolResult{}, err
			}
			if len(entries) == 0 {
				result := textToolResult("(no subagents)")
				result.Value = []map[string]any{}
				return result, nil
			}
			lines := make([]string, 0, len(entries))
			values := make([]map[string]any, 0, len(entries))
			for _, entry := range entries {
				at := ""
				if in.Scope == "descendants" {
					at = fmt.Sprintf(" parent=%s depth=%d", entry.Parent, entry.Depth)
				}
				if entry.Kind == "diagnostic" {
					lines = append(lines, fmt.Sprintf("%s [diagnostic: %s]%s", entry.ID, entry.Reason, at))
				} else {
					lines = append(lines, fmt.Sprintf("%s [%s]%s — %s", entry.ID, entry.Status, at, entry.Label))
				}
				values = append(values, entry.value(in.Scope == "descendants"))
			}
			result := textToolResult(strings.Join(lines, "\n"))
			result.Value = values
			return result, nil
		},
	}
}

func (e *Engine) listModelAgents(ctx context.Context, parentID string, descendants bool) ([]agentListEntry, error) {
	rows, _, err := e.listSubagentEntries(ctx, parentID, descendants)
	if err != nil {
		return nil, err
	}
	result := make([]agentListEntry, 0, len(rows))
	for _, row := range rows {
		entry := agentListEntry{Kind: row.Kind, ID: row.ID}
		if descendants {
			entry.Parent, entry.Depth = row.ParentID, row.Depth
		}
		if row.Kind == "diagnostic" {
			entry.Reason = row.Reason
			result = append(result, entry)
			continue
		}
		if row.Mode != "continuable" {
			continue
		}
		entry.Label = row.Label
		entry.Status = "ready"
		if row.running {
			entry.Status = "running"
		} else if row.attached {
			entry.Status = "idle"
		}
		result = append(result, entry)
	}
	return result, nil
}
