package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
)

var errHookPromptRejected = errors.New("prompt rejected by UserPromptSubmit hook")

type hookPointInput struct {
	point     string
	match     string
	turn      int
	prompt    string
	call      *ToolCall
	result    *ToolResult
	source    string
	childID   string
	stop      bool
	plainText bool
}

type hookInjectedContext struct {
	dialect HookDialect
	texts   []string
}

type hookPointOutcome struct {
	MergedHookOutcome
	contexts       []hookInjectedContext
	decisionSource HookDialect
}

func (e *Engine) initHooks() {
	e.hookCtx, e.hookCancel = context.WithCancel(context.Background())
	for _, config := range e.cfg.Hooks {
		bridge, err := NewHookBridge(config)
		if err != nil {
			log.Printf("deepseek-harness-go: %v; skipping hook bridge", err)
			continue
		}
		e.hooks = append(e.hooks, bridge)
	}
}

func (e *Engine) closeHooks() {
	if e.hookCancel != nil {
		e.hookCancel()
	}
	e.hookWG.Wait()
}

func (e *Engine) runHookPoint(ctx context.Context, session *Session, input hookPointInput) (hookPointOutcome, error) {
	allOutputs := make([]HookOutput, 0)
	contexts := make([]hookInjectedContext, 0)
	decisionRank := 0
	var decisionSource HookDialect
	for _, bridge := range e.hooks {
		config := bridge.Config()
		payload := e.hookPayload(session, config, input)
		cwd, _ := payload["cwd"].(string)
		request := HookPointRequest{
			Point: input.point, MatchQuery: input.match,
			Payload: payload, CWD: cwd,
			PlainStdoutAsContext: input.plainText && config.Dialect == HookDialectCodex,
		}
		before := func(run HookRunResult) error {
			if input.turn <= 0 {
				return nil
			}
			data := map[string]any{
				"turn": input.turn, "point": input.point, "dialect": config.Dialect, "handlerId": run.HandlerID,
			}
			if run.Matcher != nil {
				data["matcher"] = *run.Matcher
			}
			_, err := e.appendEvent(session, "hook/invoked", data)
			return err
		}
		after := func(run HookRunResult) error {
			if input.turn <= 0 {
				return nil
			}
			decision := run.Output.Decision
			if decision == "" {
				decision = "pass"
				if run.Output.Continue != nil && !*run.Output.Continue {
					decision = "stop"
				}
			}
			data := map[string]any{
				"turn": input.turn, "point": input.point, "handlerId": run.HandlerID,
				"decision": decision, "durationMs": run.Duration.Milliseconds(),
			}
			if run.Output.ExitCode != nil {
				data["exitCode"] = *run.Output.ExitCode
			}
			if stderr := SummarizeHookStderr(run.Output.Stderr, config.StderrSummaryMaxChars); stderr != "" {
				data["stderrSummary"] = stderr
			}
			_, err := e.appendEvent(session, "hook/result", data)
			return err
		}
		result, err := bridge.run(ctx, request, before, after)
		if err != nil {
			return hookPointOutcome{}, err
		}
		if len(result.Outcome.AdditionalContext) > 0 {
			contexts = append(contexts, hookInjectedContext{dialect: config.Dialect, texts: result.Outcome.AdditionalContext})
		}
		for _, run := range result.Runs {
			output := run.Output
			// Codex records permissionDecision:"ask", but its bridge only honors blocks.
			if config.Dialect == HookDialectCodex && output.Decision == "ask" {
				output.Decision = ""
			}
			rank := hookDecisionRank(output.Decision)
			if rank > decisionRank {
				decisionRank, decisionSource = rank, config.Dialect
			}
			allOutputs = append(allOutputs, output)
		}
	}
	return hookPointOutcome{
		MergedHookOutcome: MergeHookOutputs(allOutputs),
		contexts:          contexts, decisionSource: decisionSource,
	}, nil
}

func (e *Engine) hookPayload(session *Session, config HookBridgeConfig, input hookPointInput) map[string]any {
	session.mu.Lock()
	header := session.Header
	session.mu.Unlock()
	id, cwd := header.ID, header.CWD
	transcript := any("")
	if config.Dialect == HookDialectCodex {
		transcript = nil
	}
	if e.sessionStore != nil {
		if location, ok := e.sessionStore.Locate(header); ok && location.Path != "" {
			transcript = location.Path
		}
	}
	base := map[string]any{
		"session_id": id, "transcript_path": transcript, "cwd": cwd, "hook_event_name": input.point,
	}
	if config.Dialect == HookDialectCodex {
		base["model"] = config.Model
		base["permission_mode"] = "default"
		if input.turn > 0 {
			base["turn_id"] = fmt.Sprint(input.turn)
		}
	}
	switch input.point {
	case "SessionStart":
		base["source"] = input.source
	case "UserPromptSubmit":
		base["prompt"] = input.prompt
	case "PreToolUse", "PostToolUse":
		base["tool_name"] = input.call.Name
		base["tool_use_id"] = input.call.ID
		base["tool_input"] = hookToolInput(config.Dialect, input.call.Arguments)
		if input.point == "PostToolUse" && input.result != nil {
			base["tool_response"] = blockText(input.result.Content)
		}
	case "Stop":
		base["stop_hook_active"] = false
		if config.Dialect == HookDialectCodex {
			base["last_assistant_message"] = nil
		}
	case "SubagentStart", "SubagentStop":
		base["agent_id"] = input.childID
		base["agent_type"] = "general-purpose"
		if input.point == "SubagentStop" {
			base["stop_hook_active"] = false
		}
	}
	return base
}

func hookToolInput(dialect HookDialect, raw json.RawMessage) any {
	var value any = map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &value)
	}
	if dialect != HookDialectCodex {
		return value
	}
	command := ""
	if object, ok := value.(map[string]any); ok {
		command, _ = object["command"].(string)
	}
	return map[string]any{"command": command}
}

func (e *Engine) appendHookContexts(session *Session, contexts []hookInjectedContext) error {
	for _, context := range contexts {
		blocks := make([]ContentBlock, 0, len(context.texts))
		for _, text := range context.texts {
			if text != "" {
				blocks = append(blocks, ContentBlock{Type: "text", Text: text})
			}
		}
		if len(blocks) == 0 {
			continue
		}
		if _, err := e.appendEvent(session, "user/message", map[string]any{
			"id": newID("msg"), "role": "user", "content": blocks,
			"source": map[string]any{"kind": "plugin", "plugin": hookPluginName(context.dialect)},
		}); err != nil {
			return err
		}
	}
	return nil
}

func hookPluginName(dialect HookDialect) string {
	if dialect == HookDialectCodex {
		return "hooks-codex"
	}
	return "hooks-claude-code"
}

func (e *Engine) startDetachedHook(session *Session, input hookPointInput, inject bool) {
	e.mu.Lock()
	if e.closed || e.hookCtx == nil || len(e.hooks) == 0 {
		e.mu.Unlock()
		return
	}
	e.hookWG.Add(1)
	ctx := e.hookCtx
	e.mu.Unlock()
	go func() {
		defer e.hookWG.Done()
		outcome, err := e.runHookPoint(ctx, session, input)
		if err != nil || ctx.Err() != nil || !inject {
			return
		}
		_ = e.appendHookContexts(session, outcome.contexts)
	}()
}

func (e *Engine) startSessionHooks(session *Session, source string) {
	e.startDetachedHook(session, hookPointInput{
		point: "SessionStart", match: source, source: source, plainText: true,
	}, true)
}

func (e *Engine) startSubagentHooks(session *Session) {
	session.mu.Lock()
	id := session.Header.ID
	session.mu.Unlock()
	e.startDetachedHook(session, hookPointInput{
		point: "SubagentStart", match: "general-purpose", childID: id,
	}, true)
}

func (e *Engine) stopSubagentHooks(session *Session) {
	session.mu.Lock()
	id := session.Header.ID
	session.mu.Unlock()
	e.startDetachedHook(session, hookPointInput{
		point: "SubagentStop", match: "general-purpose", childID: id, stop: true,
	}, false)
}

func (e *Engine) requestHookApproval(ctx context.Context, session *Session, call ToolCall, reason string) (bool, error) {
	id := newID("approval")
	asked := map[string]any{"id": id, "toolName": call.Name, "callId": call.ID}
	if strings.TrimSpace(reason) != "" {
		asked["reason"] = reason
	}
	if _, err := e.appendEvent(session, "approval/asked", asked); err != nil {
		return false, err
	}
	session.mu.Lock()
	policy := effectiveEventString(session.Events, "approval/policy", "policy", "ask")
	sessionID := session.Header.ID
	session.mu.Unlock()
	outcome := "rejected"
	if policy == "ask" {
		e.mu.RLock()
		available := len(e.muxSubs) > 0
		e.mu.RUnlock()
		outcome = "unavailable"
		if available {
			payload := map[string]any{
				"type": "approval/requested", "sessionId": sessionID, "approvalId": id,
				"toolName": call.Name, "callId": call.ID, "reason": reason,
			}
			value, err := e.RequestInteraction(ctx, sessionID, "approval/requested", payload)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					outcome = "cancelled"
				}
			} else if response, ok := value.(map[string]any); ok {
				responseSession, _ := response["sessionId"].(string)
				responseID, _ := response["approvalId"].(string)
				responseOutcome, _ := response["outcome"].(string)
				if responseSession == sessionID && responseID == id && (responseOutcome == "allowed-once" || responseOutcome == "rejected" || responseOutcome == "cancelled") {
					outcome = responseOutcome
				}
			}
		}
	}
	if _, err := e.appendEvent(session, "approval/decided", map[string]any{"id": id, "outcome": outcome}); err != nil {
		return false, err
	}
	return outcome == "allowed-once", nil
}
