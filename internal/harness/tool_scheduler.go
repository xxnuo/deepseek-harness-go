package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// ToolRunContext is the common execution object for runtime-aware tools.
// Context cancellation remains cooperative; deferred contexts and the turn
// conclusion flag are committed only through the ordered finalization stage.
type ToolRunContext struct {
	context.Context
	Call ToolCall

	mu                 sync.Mutex
	additionalContexts []ToolContext
	concludesTurn      bool
}

func (r *ToolRunContext) DeferContext(context ToolContext) {
	r.mu.Lock()
	r.additionalContexts = append(r.additionalContexts, cloneToolContext(context))
	r.mu.Unlock()
}

func (r *ToolRunContext) ConcludeTurn() {
	r.mu.Lock()
	r.concludesTurn = true
	r.mu.Unlock()
}

func (r *ToolRunContext) outcome() ([]ToolContext, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	contexts := make([]ToolContext, len(r.additionalContexts))
	for index := range r.additionalContexts {
		contexts[index] = cloneToolContext(r.additionalContexts[index])
	}
	return contexts, r.concludesTurn
}

func cloneToolContext(value ToolContext) ToolContext {
	cloned := ToolContext{Content: cloneContentBlocks(value.Content)}
	if value.Source != nil {
		if source, ok := cloneJSON(value.Source).(map[string]any); ok {
			cloned.Source = source
		}
	}
	return cloned
}

func cloneToolContexts(values []ToolContext) []ToolContext {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]ToolContext, len(values))
	for index, value := range values {
		cloned[index] = cloneToolContext(value)
	}
	return cloned
}

// preparedToolCall is the ordered part of a tool invocation. Only the body
// dispatch is allowed to overlap with another prepared call.
type preparedToolCall struct {
	call       ToolCall
	tool       Tool
	callSeq    int
	result     ToolResult
	needsPost  bool
	started    bool
	preContext []hookInjectedContext
	runtime    *ToolRunContext
}

type toolDispatchOutcome struct {
	index  int
	result ToolResult
	err    error
}

func alwaysConcurrencySafe(ToolCall) bool { return true }

func toolConcurrencySafe(tool Tool, call ToolCall) (safe bool) {
	if tool.IsConcurrencySafe == nil {
		return false
	}
	defer func() {
		if recover() != nil {
			// Classifiers are extension code. A panic must fail closed.
			safe = false
		}
	}()
	return tool.IsConcurrencySafe(call)
}

func (e *Engine) toolIsParallel(s *Session, call ToolCall) bool {
	tool, ok := e.toolForSession(s, call.Name)
	return ok && toolConcurrencySafe(tool, call)
}

// executeToolCalls schedules one model response's calls. Preparation,
// post-processing, durable events, and context admission remain model ordered;
// only started tool bodies use the bounded rolling pool.
func (e *Engine) executeToolCalls(ctx context.Context, s *Session, turn, step int, calls []ToolCall) (bool, error) {
	concluded := false
	for index := 0; index < len(calls); {
		if ctx.Err() != nil {
			if err := e.appendSkippedCalls(s, turn, step, calls[index:]); err != nil {
				return concluded, err
			}
			return concluded, ctx.Err()
		}
		if !e.toolIsParallel(s, calls[index]) {
			prepared, err := e.prepareToolCall(ctx, s, turn, step, calls[index])
			if err != nil {
				return concluded, err
			}
			if prepared.started {
				prepared.result = e.dispatchToolCall(ctx, prepared)
			}
			if err := e.finalizeToolCall(ctx, s, turn, step, prepared); err != nil {
				return concluded, err
			}
			concluded = concluded || prepared.result.ConcludesTurn
			index++
			if ctx.Err() != nil {
				if err := e.appendSkippedCalls(s, turn, step, calls[index:]); err != nil {
					return concluded, err
				}
				return concluded, ctx.Err()
			}
			continue
		}
		consumed, aborted, groupConcluded, err := e.runParallelToolGroup(ctx, s, turn, step, calls[index:])
		if err != nil {
			return concluded, err
		}
		index += consumed
		concluded = concluded || groupConcluded
		if aborted {
			if err := e.appendSkippedCalls(s, turn, step, calls[index:]); err != nil {
				return concluded, err
			}
			return concluded, ctx.Err()
		}
	}
	return concluded, nil
}

func (e *Engine) prepareToolCall(ctx context.Context, s *Session, turn, step int, call ToolCall) (*preparedToolCall, error) {
	if len(call.Arguments) == 0 {
		call.Arguments = json.RawMessage(`{}`)
	}
	call.Workspace = s.Header.CWD
	call.SessionID = s.Header.ID
	event, err := e.appendEvent(s, "tool/call", map[string]any{"turn": turn, "step": step, "callId": call.ID, "name": call.Name, "arguments": string(call.Arguments)})
	if err != nil {
		return nil, err
	}
	prepared := &preparedToolCall{call: call, callSeq: int(event.Seq)}
	visible, visibilityErr := e.toolVisibleForSession(s, call.Name)
	if visibilityErr != nil {
		prepared.result = toolFailureResult("TOOL_CATALOG", visibilityErr.Error())
		return prepared, nil
	}
	tool, ok := e.toolForSession(s, call.Name)
	if !ok || !visible {
		prepared.result = toolFailureResult("UNKNOWN_TOOL", fmt.Sprintf("unknown tool: %s", call.Name))
		prepared.needsPost = true
		return prepared, nil
	}
	prepared.tool = tool
	prepared.runtime = &ToolRunContext{Context: ctx, Call: prepared.call}
	pre, err := e.runHookPoint(ctx, s, hookPointInput{point: "PreToolUse", match: call.Name, turn: turn, call: &prepared.call})
	if err != nil {
		prepared.result = ToolResult{Content: []ContentBlock{{Type: "text", Text: err.Error()}}, IsError: true, Error: toolExecutionError(err)}
		return prepared, nil
	}
	prepared.preContext = pre.contexts
	if pre.Decision == "deny" {
		reason := pre.Reason
		if reason == "" {
			reason = "blocked by PreToolUse hook"
		}
		prepared.result = toolFailureResult("HOOK_DENIED", reason)
		prepared.needsPost = true
		return prepared, nil
	}
	if pre.Decision == "ask" {
		reason := pre.Reason
		if reason == "" {
			reason = "approval requested by PreToolUse hook"
		}
		allowed, approvalErr := e.requestHookApproval(ctx, s, prepared.call, reason)
		if approvalErr != nil {
			return nil, approvalErr
		}
		if !allowed {
			prepared.result = toolFailureResult("HOOK_APPROVAL_DENIED", reason)
			prepared.needsPost = true
			return prepared, nil
		}
	}
	if reason := e.structuredToolGuard(prepared.call.SessionID); reason != "" {
		prepared.result = toolFailureResult("STRUCTURED_OUTPUT_RECORDED", reason)
		prepared.needsPost = true
		return prepared, nil
	}
	if ctx.Err() != nil {
		prepared.result = abortedBeforeDispatchResult()
		return prepared, nil
	}
	prepared.started = true
	prepared.needsPost = true
	return prepared, nil
}

// prepareNestedToolCall runs the same runtime policy stages as a native call
// without creating top-level tool/call or tool/result events. Composite
// transports own their nested dispatch events and forward contexts explicitly.
func (e *Engine) prepareNestedToolCall(ctx context.Context, s *Session, call ToolCall) *preparedToolCall {
	if len(call.Arguments) == 0 {
		call.Arguments = json.RawMessage(`{}`)
	}
	call.Workspace = s.Header.CWD
	call.SessionID = s.Header.ID
	prepared := &preparedToolCall{call: call}
	tools, visibilityErr := e.codeToolsForSession(s)
	if visibilityErr != nil {
		prepared.result = toolFailureResult("TOOL_CATALOG", visibilityErr.Error())
		return prepared
	}
	tool, ok := tools[call.Name]
	if !ok || call.Name == "run_code" {
		prepared.result = toolFailureResult("UNKNOWN_TOOL", fmt.Sprintf("unknown tool: %s", call.Name))
		prepared.needsPost = true
		return prepared
	}
	prepared.tool = tool
	prepared.runtime = &ToolRunContext{Context: ctx, Call: prepared.call}
	pre, err := e.runHookPoint(ctx, s, hookPointInput{point: "PreToolUse", match: call.Name, call: &prepared.call})
	if err != nil {
		prepared.result = toolRuntimeFailureResult(err)
		return prepared
	}
	prepared.preContext = pre.contexts
	if pre.Decision == "deny" {
		reason := pre.Reason
		if reason == "" {
			reason = "blocked by PreToolUse hook"
		}
		prepared.result = toolFailureResult("HOOK_DENIED", reason)
		prepared.needsPost = true
		return prepared
	}
	if pre.Decision == "ask" {
		reason := pre.Reason
		if reason == "" {
			reason = "approval requested by PreToolUse hook"
		}
		allowed, err := e.requestHookApproval(ctx, s, prepared.call, reason)
		if err != nil {
			prepared.result = toolRuntimeFailureResult(err)
			return prepared
		}
		if !allowed {
			prepared.result = toolFailureResult("HOOK_APPROVAL_DENIED", reason)
			prepared.needsPost = true
			return prepared
		}
	}
	if reason := e.structuredToolGuard(prepared.call.SessionID); reason != "" {
		prepared.result = toolFailureResult("STRUCTURED_OUTPUT_RECORDED", reason)
		prepared.needsPost = true
		return prepared
	}
	if ctx.Err() != nil {
		prepared.result = abortedBeforeDispatchResult()
		return prepared
	}
	prepared.started = true
	prepared.needsPost = true
	return prepared
}

func (e *Engine) dispatchToolCall(ctx context.Context, prepared *preparedToolCall) ToolResult {
	result, err := executeToolRuntime(ctx, prepared.tool, prepared.call, prepared.runtime)
	if err != nil {
		if result.Error == nil {
			result.Error = toolExecutionError(err)
		}
		result.IsError = true
		result.ConcludesTurn = false
	}
	if result.Error != nil {
		result.IsError = true
		result.ConcludesTurn = false
	}
	if result.Content == nil && result.Error != nil {
		result.Content = []ContentBlock{{Type: "text", Text: result.Error.Message}}
	}
	if ctx.Err() != nil && !result.IsError && result.Error == nil {
		return abortedToolResult(result)
	}
	return result
}

func (e *Engine) finalizeToolCall(ctx context.Context, s *Session, turn, step int, prepared *preparedToolCall) error {
	e.finishToolRuntime(ctx, s, turn, prepared, true)
	result := prepared.result
	data := map[string]any{"turn": turn, "step": step, "message": toolResultMessage(prepared.call.ID, result.Content, result.IsError)}
	if result.Error != nil {
		data["error"] = result.Error
	}
	if result.Meta != nil {
		data["meta"] = result.Meta
	}
	if _, err := e.appendEvent(s, "tool/result", data, prepared.callSeq); err != nil {
		return err
	}
	if err := e.appendHookContexts(s, prepared.preContext); err != nil {
		return err
	}
	if err := e.deferToolContexts(s, result.AdditionalContexts); err != nil {
		return err
	}
	if prepared.needsPost {
		return e.appendRepeatToolReminder(s, prepared.call)
	}
	return nil
}

func (e *Engine) finishNestedToolCall(ctx context.Context, s *Session, prepared *preparedToolCall) (ToolResult, []hookInjectedContext) {
	e.finishToolRuntime(ctx, s, 0, prepared, false)
	return prepared.result, append([]hookInjectedContext(nil), prepared.preContext...)
}

func (e *Engine) finishToolRuntime(ctx context.Context, s *Session, turn int, prepared *preparedToolCall, spill bool) {
	result := prepared.result
	if prepared.needsPost {
		post, err := e.runHookPoint(context.WithoutCancel(ctx), s, hookPointInput{point: "PostToolUse", match: prepared.call.Name, turn: turn, call: &prepared.call, result: &result})
		if err != nil {
			result = toolRuntimeFailureResult(err)
		} else if post.Decision == "deny" {
			reason := post.Reason
			if reason == "" {
				reason = "blocked by PostToolUse hook"
			}
			result = ToolResult{Content: []ContentBlock{{Type: "text", Text: reason}}, IsError: true, Error: &ToolError{Name: "ToolError", Code: "HOOK_DENIED", Message: reason}}
		}
		prepared.preContext = append(prepared.preContext, post.contexts...)
		if spill {
			searchSpilled := false
			if prepared.call.Name == "glob" || prepared.call.Name == "grep" {
				result, searchSpilled = e.applySearchSpillPolicyResult(s, prepared.call, result)
			}
			if !searchSpilled {
				result = e.applySpillPolicy(s, prepared.call, result)
			}
		}
	}
	if ctx.Err() != nil && !result.IsError && result.Error == nil {
		result = abortedToolResult(result)
	}
	if prepared.tool.FinalizeContent != nil {
		content, err := callToolContentFinalizer(prepared.tool, prepared.call, result)
		if err != nil {
			result = toolRuntimeFailureResult(err)
		} else if content != nil {
			result.Content = cloneContentBlocks(content)
		}
	}
	e.observeStructuredToolResult(s, prepared, result)
	prepared.result = result
}

func toolRuntimeFailureResult(err error) ToolResult {
	toolErr := toolExecutionError(err)
	return ToolResult{
		Content: []ContentBlock{{Type: "text", Text: "Error: " + toolErr.Message}},
		IsError: true,
		Error:   toolErr,
	}
}

func (e *Engine) runParallelToolGroup(ctx context.Context, s *Session, turn, step int, calls []ToolCall) (int, bool, bool, error) {
	cap := e.cfg.MaxParallelToolCalls
	if cap < 1 {
		cap = 1
	}
	started := make(map[int]*preparedToolCall)
	slots := make(map[int]*preparedToolCall)
	active := make(map[int]struct{})
	done := make(chan toolDispatchOutcome, len(calls))
	next, committed := 0, 0
	aborted := ctx.Err() != nil
	concluded := false

	commitReady := func() error {
		for {
			prepared := slots[committed]
			if prepared == nil {
				return nil
			}
			if err := e.finalizeToolCall(ctx, s, turn, step, prepared); err != nil {
				return err
			}
			concluded = concluded || prepared.result.ConcludesTurn
			delete(slots, committed)
			committed++
		}
	}

	fill := func() error {
		for !aborted && next < len(calls) && len(active) < cap {
			if next > 0 && !e.toolIsParallel(s, calls[next]) {
				break
			}
			prepared, err := e.prepareToolCall(ctx, s, turn, step, calls[next])
			if err != nil {
				return err
			}
			started[next] = prepared
			next++
			if prepared.started {
				active[next-1] = struct{}{}
				index := next - 1
				go func() {
					done <- toolDispatchOutcome{index: index, result: e.dispatchToolCall(ctx, prepared)}
				}()
			} else {
				slots[next-1] = prepared
			}
			if err := commitReady(); err != nil {
				return err
			}
			if ctx.Err() != nil {
				aborted = true
			}
		}
		return nil
	}

	if err := fill(); err != nil {
		for range active {
			<-done
		}
		return 0, false, false, err
	}
	for len(active) > 0 {
		outcome := <-done
		delete(active, outcome.index)
		prepared := started[outcome.index]
		prepared.result = outcome.result
		slots[outcome.index] = prepared
		if err := commitReady(); err != nil {
			for range active {
				<-done
			}
			return 0, false, false, err
		}
		if ctx.Err() != nil {
			aborted = true
		}
		if err := fill(); err != nil {
			for range active {
				<-done
			}
			return 0, false, false, err
		}
	}
	if aborted {
		// Any prepared but uncommitted entries are settled before the synthetic
		// results for calls that never reached preparation.
		if err := commitReady(); err != nil {
			return 0, false, false, err
		}
		if next < len(calls) {
			if err := e.appendSkippedCalls(s, turn, step, calls[next:]); err != nil {
				return 0, false, false, err
			}
		}
		return len(calls), true, concluded, nil
	}
	if err := commitReady(); err != nil {
		return 0, false, false, err
	}
	return next, false, concluded, nil
}

func toolFailureResult(code, message string) ToolResult {
	return ToolResult{Content: []ContentBlock{{Type: "text", Text: message}}, IsError: true, Error: &ToolError{Name: "ToolError", Code: code, Message: message}}
}

func abortedToolResult(prior ToolResult) ToolResult {
	return ToolResult{
		Content:            []ContentBlock{{Type: "text", Text: "Error: tool call aborted"}},
		IsError:            true,
		Error:              &ToolError{Name: "AbortError", Code: "ABORTED", Message: "tool call aborted"},
		AdditionalContexts: cloneToolContexts(prior.AdditionalContexts),
	}
}

func abortedBeforeDispatchResult() ToolResult {
	return ToolResult{Content: []ContentBlock{{Type: "text", Text: "Error: tool call aborted before dispatch"}}, IsError: true, Error: &ToolError{Name: "AbortError", Code: "ABORTED_BEFORE_DISPATCH", Message: "tool call aborted before dispatch"}}
}

func (e *Engine) deferToolContexts(s *Session, contexts []ToolContext) error {
	for _, value := range contexts {
		context := cloneToolContext(value)
		if len(context.Content) == 0 {
			continue
		}
		if context.Source == nil {
			context.Source = map[string]any{"kind": "plugin", "plugin": "tool-runtime"}
		}
		if _, err := e.enqueueTeamPrompt(s, context.Content, context.Source, "next-step", false); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) appendSkippedCalls(s *Session, turn, step int, calls []ToolCall) error {
	for _, call := range calls {
		if len(call.Arguments) == 0 {
			call.Arguments = json.RawMessage(`{}`)
		}
		call.Workspace = s.Header.CWD
		call.SessionID = s.Header.ID
		event, err := e.appendEvent(s, "tool/call", map[string]any{"turn": turn, "step": step, "callId": call.ID, "name": call.Name, "arguments": string(call.Arguments)})
		if err != nil {
			return err
		}
		result := abortedBeforeDispatchResult()
		data := map[string]any{"turn": turn, "step": step, "message": toolResultMessage(call.ID, result.Content, true), "error": result.Error}
		if _, err := e.appendEvent(s, "tool/result", data, int(event.Seq)); err != nil {
			return err
		}
	}
	return nil
}
