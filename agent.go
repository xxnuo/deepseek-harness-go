package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type PromptResult struct {
	Accepted bool           `json:"accepted"`
	Command  *CommandResult `json:"command,omitempty"`
}
type CommandResult struct {
	Kind           string `json:"kind"`
	Text           string `json:"text,omitempty"`
	SourceEventSeq int    `json:"sourceEventSeq,omitempty"`
	// SourceEventSeqSet preserves an explicit sequence of zero, which is a
	// valid upstream source reference but indistinguishable from Go's int zero.
	SourceEventSeqSet bool `json:"-"`
}

// queuedPrompt keeps the durable user-message boundary separate from the
// worker that claims it. A later prompt must not leak into an earlier model
// request merely because its user/message event was already appended.
type queuedPrompt struct {
	id                 string
	text               string
	content            []ContentBlock
	source             map[string]any
	additionalContexts []SessionReferenceContext
	done               chan promptOutcome
}

func (p *queuedPrompt) message() map[string]any {
	message := map[string]any{"id": p.id, "role": "user", "content": p.content, "source": p.source}
	if len(p.additionalContexts) > 0 {
		message["additionalContexts"] = p.additionalContexts
	}
	return message
}

func restorePromptQueues(events []Event) ([]*queuedPrompt, []*queuedPrompt, error) {
	queues := map[string][]*queuedPrompt{"next-turn": {}, "next-step": {}}
	for _, event := range events {
		if event.Type != "agent/inbox/spliced" {
			continue
		}
		data, ok := event.Data.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("invalid persisted inbox splice at session seq %d", event.Seq)
		}
		target, _ := data["target"].(string)
		queue, ok := queues[target]
		if !ok {
			return nil, nil, fmt.Errorf("invalid persisted inbox target at session seq %d", event.Seq)
		}
		start, ok := eventSeqNumber(data["start"])
		if !ok || start < 0 || start > len(queue) {
			return nil, nil, fmt.Errorf("invalid persisted inbox start at session seq %d", event.Seq)
		}
		removed := 0
		if raw, exists := data["removedCount"]; exists {
			removed, ok = eventSeqNumber(raw)
			if !ok || removed < 0 || start+removed > len(queue) {
				return nil, nil, fmt.Errorf("invalid persisted inbox removal at session seq %d", event.Seq)
			}
		}
		rawInserted, ok := data["inserted"].([]any)
		if !ok {
			return nil, nil, fmt.Errorf("invalid persisted inbox insertion at session seq %d", event.Seq)
		}
		inserted := make([]*queuedPrompt, 0, len(rawInserted))
		for _, raw := range rawInserted {
			var message struct {
				ID                 string                    `json:"id"`
				Role               string                    `json:"role"`
				Content            []ContentBlock            `json:"content"`
				Source             map[string]any            `json:"source"`
				AdditionalContexts []SessionReferenceContext `json:"additionalContexts"`
			}
			encoded, err := json.Marshal(raw)
			if err != nil || json.Unmarshal(encoded, &message) != nil || message.ID == "" || message.Role != "user" || message.Source["kind"] == nil {
				return nil, nil, fmt.Errorf("invalid persisted inbox message at session seq %d", event.Seq)
			}
			inserted = append(inserted, &queuedPrompt{
				id: message.ID, text: strings.TrimSpace(blockText(message.Content)), content: message.Content,
				source: message.Source, additionalContexts: message.AdditionalContexts,
			})
		}
		next := make([]*queuedPrompt, 0, len(queue)-removed+len(inserted))
		next = append(next, queue[:start]...)
		next = append(next, inserted...)
		next = append(next, queue[start+removed:]...)
		queues[target] = next
	}
	return queues["next-turn"], queues["next-step"], nil
}

type promptOutcome struct {
	text string
	err  error
}

func contentText(parts []PromptContentPart) string {
	var b strings.Builder
	for _, part := range parts {
		if part.Type == "text" {
			b.WriteString(part.Text)
		}
	}
	return b.String()
}

func blockText(blocks []ContentBlock) string {
	var b strings.Builder
	for _, block := range blocks {
		if block.Type == "text" {
			b.WriteString(block.Text)
		}
	}
	return b.String()
}

func (e *Engine) Prompt(ctx context.Context, id string, req PromptRequest) (PromptResult, error) {
	job, command, err := e.enqueuePrompt(ctx, id, req, false)
	if err != nil {
		return PromptResult{}, err
	}
	if command != nil {
		return PromptResult{Accepted: true, Command: command}, nil
	}
	_ = job
	return PromptResult{Accepted: true}, nil
}

func (e *Engine) enqueuePrompt(ctx context.Context, id string, req PromptRequest, wait bool) (*queuedPrompt, *CommandResult, error) {
	return e.enqueuePromptFrom(ctx, id, req, wait, nil)
}

func (e *Engine) enqueuePromptFrom(ctx context.Context, id string, req PromptRequest, wait bool, origin *dynamicCordisRun) (*queuedPrompt, *CommandResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s, err := e.getSession(id)
	if err != nil {
		return nil, nil, err
	}
	if req.SessionID != "" && req.SessionID != id {
		return nil, nil, errors.New("bad-request: sessionId does not match target session")
	}
	if len(req.Content) == 0 {
		return nil, nil, errors.New("bad-request: prompt content is empty")
	}
	text := strings.TrimSpace(contentText(req.Content))
	if req.Mode == "" {
		req.Mode = "queue"
	}
	if req.Mode != "queue" && req.Mode != "steer" {
		return nil, nil, errors.New("bad-request: mode must be queue or steer")
	}
	// Slash commands remain host-side and never enter the model history.
	if !req.Literal && len(req.Content) == 1 && strings.HasPrefix(text, "/") {
		execution, admitted, err := e.executeCommand(ctx, s, text)
		if err != nil {
			return nil, nil, err
		}
		if !admitted {
			return nil, nil, fmt.Errorf("unknown-command: %s", text)
		}
		return nil, execution.Result, nil
	}
	content, err := e.durablePromptContent(req.Content)
	if err != nil {
		return nil, nil, err
	}
	prepared, err := e.PrepareSessionReferences(ctx, id, content, req.References, e.cfg.SessionReference)
	if err != nil {
		return nil, nil, err
	}
	content = prepared.Content
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	source := map[string]any{"kind": "user"}
	for key, value := range req.Source {
		source[key] = value
	}
	if req.RPCID != "" && source["rpcId"] == nil {
		source["rpcId"] = req.RPCID
	}
	if req.ClientTimeZone != "" {
		canonical, ok := canonicalClientTimeZone(req.ClientTimeZone)
		if !ok {
			return nil, nil, &ClientTimeZoneError{Value: req.ClientTimeZone}
		}
		source["clientTimeZone"] = canonical
	}
	job := &queuedPrompt{id: newID("msg"), text: text, content: content, source: source}
	if prepared.AdditionalContext != nil {
		job.additionalContexts = []SessionReferenceContext{*prepared.AdditionalContext}
	}
	if wait {
		job.done = make(chan promptOutcome, 1)
	}
	s.mu.Lock()
	target := "next-turn"
	start := len(s.pending)
	if req.Mode == "steer" {
		if !s.Running {
			s.mu.Unlock()
			return nil, nil, errors.New("steer-unavailable: current turn no longer accepts steering")
		}
		target = "next-step"
		start = len(s.steering)
	} else if source["kind"] != "goal" {
		for i, pending := range s.pending {
			if isGoalPrompt(pending) {
				start = i
				break
			}
		}
	}
	event, err := appendEventLocked(s, "agent/inbox/spliced", map[string]any{
		"target": target, "start": start, "inserted": []any{job.message()},
	}, nil, nil, false)
	if err != nil {
		s.mu.Unlock()
		return nil, nil, err
	}
	if req.Mode == "steer" {
		s.steering = append(s.steering, job)
		s.mu.Unlock()
		e.publishEventFrom(origin, id, event)
		e.emitQueue(s)
		return job, nil, nil
	}
	s.pending = append(s.pending, nil)
	copy(s.pending[start+1:], s.pending[start:])
	s.pending[start] = job
	if s.maintenance {
		s.maintenanceWake = true
	}
	startWorker := !s.Running && !s.maintenance
	if startWorker {
		s.Running = true
	}
	s.mu.Unlock()
	e.publishEventFrom(origin, id, event)
	e.emitQueue(s)
	if startWorker {
		e.emitHost(map[string]any{"type": "host/session-status", "sessionId": id, "running": true})
		e.launchSessionWorker(s)
	}
	return job, nil, nil
}

func (e *Engine) startSessionWorker(s *Session) {
	s.mu.Lock()
	if s.maintenance {
		s.maintenanceWake = true
	}
	start := !s.Running && !s.maintenance
	if start {
		s.Running = true
	}
	id := s.Header.ID
	s.mu.Unlock()
	if start {
		e.emitHost(map[string]any{"type": "host/session-status", "sessionId": id, "running": true})
		e.launchSessionWorker(s)
	}
}

func (e *Engine) launchSessionWorker(s *Session) {
	e.workerMu.Lock()
	if e.workersClosing {
		e.workerMu.Unlock()
		e.finishClosedSessionWorker(s)
		return
	}
	e.workerWG.Add(1)
	e.workerMu.Unlock()
	go func() {
		defer e.workerWG.Done()
		e.runSessionWorker(s)
	}()
}

func (e *Engine) finishClosedSessionWorker(s *Session) {
	s.mu.Lock()
	pending := append(append([]*queuedPrompt(nil), s.pending...), s.steering...)
	s.pending = nil
	s.steering = nil
	s.Running = false
	s.Cancel = nil
	s.mu.Unlock()
	for _, item := range pending {
		if item.done != nil {
			item.done <- promptOutcome{err: errors.New("engine-closed")}
		}
	}
}

func (e *Engine) Run(ctx context.Context, id string, req PromptRequest) (string, error) {
	job, command, err := e.enqueuePrompt(ctx, id, req, true)
	if err != nil {
		return "", err
	}
	if command != nil {
		return command.Text, nil
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case outcome := <-job.done:
		return outcome.text, outcome.err
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func (e *Engine) nextTurn(s *Session) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return nextTurnLocked(s)
}

func nextTurnLocked(s *Session) int {
	if s.turnCounter > 0 {
		s.turnCounter++
		return s.turnCounter
	}
	maxTurn := 0
	for _, ev := range s.Events {
		if ev.Type == "turn/start" {
			if d, ok := ev.Data.(map[string]any); ok {
				switch v := d["turn"].(type) {
				case int:
					if v > maxTurn {
						maxTurn = v
					}
				case float64:
					if int(v) > maxTurn {
						maxTurn = int(v)
					}
				}
			}
		}
	}
	s.turnCounter = maxTurn + 1
	return s.turnCounter
}

// runSessionWorker owns a session's active-turn state. It claims prompts in
// FIFO order, creates a fresh cancellation context for each turn, and keeps
// pending prompts after cancellation so they can resume after the aborted
// turn settles.
func (e *Engine) runSessionWorker(s *Session) {
	if isSubagentSession(s) {
		e.startSubagentHooks(s)
		defer e.stopSubagentHooks(s)
	}
	for {
		e.mu.RLock()
		closed := e.closed
		e.mu.RUnlock()
		if closed {
			e.finishClosedSessionWorker(s)
			return
		}

		item, claimErr := e.claimNextPrompt(s)
		if claimErr != nil {
			e.disarmGoal(s.Header.ID)
			s.mu.Lock()
			s.Running = false
			s.Cancel = nil
			id := s.Header.ID
			s.mu.Unlock()
			e.emitHost(map[string]any{"type": "host/session-status", "sessionId": id, "running": false})
			e.scheduleWake(id)
			return
		}
		if item == nil {
			work, scheduleErr := e.scheduleGoalRound(s)
			if scheduleErr != nil {
				e.disarmGoal(s.Header.ID)
			}
			if work {
				continue
			}
			s.mu.Lock()
			if len(s.pending) > 0 || len(s.steering) > 0 {
				s.mu.Unlock()
				continue
			}
			s.Running = false
			s.Cancel = nil
			id := s.Header.ID
			s.mu.Unlock()
			e.emitHost(map[string]any{"type": "host/session-status", "sessionId": id, "running": false})
			e.scheduleWake(id)
			return
		}

		turn := e.nextTurn(s)
		turnCtx, cancel := context.WithCancel(context.Background())
		s.mu.Lock()
		s.Cancel = cancel
		s.mu.Unlock()
		_, _ = e.appendEvent(s, "turn/start", map[string]any{"turn": turn})
		hookOutcome, userErr := e.runHookPoint(turnCtx, s, hookPointInput{
			point: "UserPromptSubmit", turn: turn, prompt: blockText(item.content), plainText: true,
		})
		if userErr == nil && hookOutcome.Decision == "deny" {
			userErr = errHookPromptRejected
			if hookOutcome.Reason != "" {
				userErr = fmt.Errorf("%w: %s", errHookPromptRejected, hookOutcome.Reason)
			}
		}
		if userErr == nil {
			_, userErr = e.admitPrompt(turnCtx, s, item)
		}
		if userErr == nil {
			userErr = e.appendHookContexts(s, hookOutcome.contexts)
		}
		var output string
		var runErr error
		if userErr != nil {
			runErr = userErr
			reason := map[string]any{"kind": "error", "error": map[string]any{"message": userErr.Error(), "code": "SESSION"}}
			if errors.Is(userErr, context.Canceled) {
				reason = map[string]any{"kind": "aborted"}
			} else if userErr.Error() == "goal-round-stale" || errors.Is(userErr, errHookPromptRejected) {
				reason = map[string]any{"kind": "rejected"}
			}
			_, _ = e.appendEvent(s, "turn/end", map[string]any{"turn": turn, "reason": reason})
		} else {
			output, runErr = e.runTurnSync(turnCtx, s, turn)
		}
		if runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(turnCtx.Err(), context.Canceled) &&
			!errors.Is(runErr, ErrEngineClosed) && !errors.Is(runErr, errHookPromptRejected) && runErr.Error() != "goal-round-stale" {
			step := 0
			s.mu.Lock()
			for index := len(s.Events) - 1; index >= 0; index-- {
				event := s.Events[index]
				data, _ := event.Data.(map[string]any)
				if event.Type == "turn/start" {
					if eventInt(data["turn"]) == turn {
						break
					}
					continue
				}
				if event.Type != "step/start" {
					continue
				}
				if eventInt(data["turn"]) == turn {
					step = eventInt(data["step"])
					break
				}
			}
			s.mu.Unlock()
			e.telemetry.relayAgentError(s, turn, step, runErr)
		}
		e.finishGoalTurn(s, item, turn)
		if item.done != nil {
			item.done <- promptOutcome{text: output, err: runErr}
		}
		cancel()
	}
}

func (e *Engine) claimNextPrompt(s *Session) (*queuedPrompt, error) {
	s.mu.Lock()
	target := "next-turn"
	queue := &s.pending
	if len(*queue) == 0 {
		target = "next-step"
		queue = &s.steering
	}
	if len(*queue) == 0 {
		s.mu.Unlock()
		return nil, nil
	}
	item := (*queue)[0]
	event, err := appendEventLocked(s, "agent/inbox/spliced", map[string]any{
		"target": target, "start": 0, "removedCount": 1, "inserted": []any{},
	}, nil, nil, false)
	if err == nil {
		*queue = (*queue)[1:]
	}
	id := s.Header.ID
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	e.publishEvent(id, event)
	e.emitQueue(s)
	return item, nil
}

func openStepForTurn(events []Event, turn int) (bool, int) {
	openTurn, openStep := false, 0
	for _, event := range events {
		eventTurn, turnOK := eventFieldInt(event.Data, "turn")
		switch event.Type {
		case "turn/start":
			if turnOK && eventTurn == turn {
				openTurn, openStep = true, 0
			}
		case "step/start":
			if openTurn && turnOK && eventTurn == turn {
				openStep, _ = eventFieldInt(event.Data, "step")
			}
		case "step/end":
			if openTurn && turnOK && eventTurn == turn {
				openStep = 0
			}
		case "turn/end":
			if turnOK && eventTurn == turn {
				openTurn, openStep = false, 0
			}
		}
	}
	return openTurn, openStep
}

func (e *Engine) closeOpenTurn(s *Session, turn int, reason map[string]any) {
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	open, step := openStepForTurn(events, turn)
	if !open {
		return
	}
	if step > 0 {
		if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": turn, "step": step}); err != nil {
			return
		}
	}
	_, _ = e.appendEvent(s, "turn/end", map[string]any{"turn": turn, "reason": reason})
}

func (e *Engine) runTurnSync(ctx context.Context, s *Session, turn int) (output string, resultErr error) {
	defer func() {
		if resultErr == nil {
			return
		}
		reason := map[string]any{"kind": "error", "error": map[string]any{"message": resultErr.Error(), "code": "AGENT"}}
		if errors.Is(resultErr, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) || errors.Is(resultErr, ErrEngineClosed) {
			reason = map[string]any{"kind": "aborted"}
		}
		e.closeOpenTurn(s, turn, reason)
	}()
	s.mu.Lock()
	selection := s.Model
	s.mu.Unlock()
	agentRuntime, err := e.runtimeForSession(s)
	if err != nil {
		e.closeOpenTurn(s, turn, map[string]any{"kind": "error", "error": map[string]any{"message": err.Error(), "code": "PRESET"}})
		return "", err
	}
	if err := e.ensureInstructionBaseline(ctx, s, agentRuntime); err != nil {
		e.closeOpenTurn(s, turn, map[string]any{"kind": "error", "error": map[string]any{"message": err.Error(), "code": "CONTEXT"}})
		return "", err
	}
	messages := e.durableMessages(s, turn)
	config := map[string]any{"provider": selection.Provider, "model": selection.Model}
	if selection.ReasoningEffort != "" {
		config["reasoningEffort"] = selection.ReasoningEffort
	}
	if selection.Temperature != nil {
		config["temperature"] = *selection.Temperature
	}
	thinking, effort, maxTokens := e.requestPolicy(selection)
	if maxTokens > 0 {
		config["maxTokens"] = maxTokens
	}
	routeProvider := selection.Provider
	if routeProvider == "" {
		routeProvider = e.cfg.Provider
	}
	requestContext := map[string]any{"provider": routeProvider, "model": selection.Model}
	// The bundled DeepSeek catalog advertises a fixed context capacity. Other
	// providers may not expose one through the small Go Provider interface, but
	// the route record is still useful and mirrors the upstream log contract.
	if routeProvider == "deepseek-official" {
		requestContext["contextWindow"] = deepSeekDefaultContext
	}
	e.mu.RLock()
	provider := e.providers[selection.Provider]
	if provider == nil && selection.Provider == "" {
		provider = e.providers[e.cfg.Provider]
	}
	e.mu.RUnlock()
	if provider == nil {
		err := fmt.Errorf("model-unavailable: %s/%s", selection.Provider, selection.Model)
		e.closeOpenTurn(s, turn, map[string]any{"kind": "error", "error": map[string]any{"message": err.Error(), "code": "NO_ADAPTER"}})
		return "", err
	}
	var lastText string
	const maxToolSteps = 16
	for step := 1; step <= maxToolSteps; step++ {
		if steering := e.claimSteering(s); len(steering) > 0 {
			for _, item := range steering {
				if _, err := e.admitPrompt(ctx, s, item); err != nil {
					return "", err
				}
			}
			messages = e.durableMessages(s, turn)
		}
		assembly, err := e.promptAssemblyForSession(ctx, s, selection, agentRuntime)
		if err != nil {
			e.closeOpenTurn(s, turn, map[string]any{"kind": "error", "error": map[string]any{"message": err.Error(), "code": "PROMPT"}})
			return "", err
		}
		system, dynamicContexts, err := renderResolvedPromptAssembly(assembly)
		if err != nil {
			e.closeOpenTurn(s, turn, map[string]any{"kind": "error", "error": map[string]any{"message": err.Error(), "code": "PROMPT"}})
			return "", err
		}
		tools := assembly.Tools
		if changed, _ := e.compactForPressure(ctx, s, turn, selection, system, tools); changed {
			messages = e.durableMessages(s, turn)
		}
		stepStart, err := e.appendEvent(s, "step/start", map[string]any{"turn": turn, "step": step})
		if err != nil {
			return "", err
		}
		if err := e.appendDynamicPromptContext(s, dynamicContexts); err != nil {
			return "", err
		}
		if err := e.appendStepContexts(ctx, s, turn, step); err != nil {
			return "", err
		}
		messages = e.durableMessages(s, turn)
		header := map[string]any{"config": config}
		if system != "" {
			header["system"] = system
		}
		if len(tools) > 0 {
			header["tools"] = tools
		}
		if err := e.ensureRequestHeader(s, header); err != nil {
			return "", err
		}
		if !e.sameRequestContext(s, requestContext) {
			if _, err := e.appendEvent(s, "request/context", requestContext); err != nil {
				return "", err
			}
		}
		e.startPendingSessionTitle(s, SessionTitleModelProvenance{Provider: routeProvider, Model: selection.Model})
		var completion Completion
		overflowRetries := 0
		for {
			request := ChatRequest{
				SessionID: s.Header.ID, Model: selection.Model, System: system, Messages: messages, Tools: tools,
				Thinking: thinking, ReasoningEffort: effort, Temperature: selection.Temperature, MaxTokens: maxTokens,
			}
			completion, _, err = e.completeWithRetrySink(ctx, provider, request, s, turn, step, func(delta Delta) error {
				if delta.Text != "" {
					_, err := e.appendEvent(s, "assistant/chunk", map[string]any{"turn": turn, "step": step, "chunk": map[string]any{"type": "text-delta", "index": 0, "text": delta.Text}})
					if err != nil {
						return err
					}
				}
				if delta.Reasoning != "" {
					_, err := e.appendEvent(s, "assistant/chunk", map[string]any{"turn": turn, "step": step, "chunk": map[string]any{"type": "reasoning-delta", "index": 0, "text": delta.Reasoning}})
					if err != nil {
						return err
					}
				}
				for _, call := range delta.ToolCalls {
					_, err := e.appendEvent(s, "assistant/chunk", map[string]any{"turn": turn, "step": step, "chunk": map[string]any{"type": "tool-call-delta", "index": call.Index, "id": call.ID, "name": call.Name, "argumentsDelta": call.ArgumentsDelta}})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err == nil {
				break
			}
			failure := retryFailure(err)
			if failure.Code == "CONTEXT_WINDOW_EXCEEDED" && overflowRetries < e.cfg.Compaction.MaxOverflowRetries && ctx.Err() == nil {
				changed, _ := e.compactForOverflow(ctx, s, turn, selection, system, tools)
				if changed && ctx.Err() == nil {
					overflowRetries++
					messages = e.durableMessages(s, turn)
					continue
				}
			}
			reason := map[string]any{"kind": "error", "error": retryFailurePayload(failure)}
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, ErrEngineClosed) {
				reason = map[string]any{"kind": "aborted"}
			}
			e.closeOpenTurn(s, turn, reason)
			return "", err
		}
		text, reasoning, finish := completion.Text, completion.Reasoning, completion.Finish
		if finish == "" {
			finish = "stop"
		}
		toolCalls := completion.ToolCalls
		if finish == "length" {
			toolCalls = nil
		} else if finish != "stop" && finish != "tool_calls" {
			err := fmt.Errorf("model stopped: %s", finish)
			e.closeOpenTurn(s, turn, map[string]any{"kind": "error", "error": map[string]any{"message": err.Error(), "code": "PROVIDER"}})
			return "", err
		}
		content := make([]ContentBlock, 0, 2+len(toolCalls))
		if reasoning != "" {
			content = append(content, ContentBlock{Type: "reasoning", Text: reasoning})
		}
		if text != "" || len(toolCalls) == 0 {
			content = append(content, ContentBlock{Type: "text", Text: text})
		}
		for _, call := range toolCalls {
			content = append(content, ContentBlock{Type: "tool-call", ID: call.ID, Name: call.Name, Arguments: string(call.Arguments)})
		}
		assistant := map[string]any{"id": newID("msg"), "role": "assistant", "content": content, "source": map[string]any{"kind": "model", "provider": selection.Provider, "model": selection.Model}}
		message := map[string]any{"turn": turn, "step": step, "message": assistant}
		if len(completion.Usage) > 0 {
			message["usage"] = completion.Usage
		}
		chunkSeqs := successfulAttemptChunkSeqs(s, turn, step, stepStart.Seq)
		if _, err := e.appendEvent(s, "assistant/message", message, chunkSeqs...); err != nil {
			return "", err
		}
		lastText = text
		if finish == "length" {
			if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": turn, "step": step}); err != nil {
				return "", err
			}
			if _, err := e.appendEvent(s, "turn/end", map[string]any{"turn": turn, "reason": map[string]any{"kind": "max-tokens"}}); err != nil {
				return "", err
			}
			return lastText, nil
		}
		steering := e.claimSteering(s)
		if len(toolCalls) == 0 && len(steering) == 0 {
			stop, err := e.runHookPoint(ctx, s, hookPointInput{point: "Stop", turn: turn})
			if err != nil {
				return "", err
			}
			if stop.Decision != "deny" {
				if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": turn, "step": step}); err != nil {
					return "", err
				}
				if _, err := e.appendEvent(s, "turn/end", map[string]any{"turn": turn, "reason": map[string]any{"kind": "completed"}}); err != nil {
					return "", err
				}
				return lastText, nil
			}
			text := stop.Reason
			if text == "" {
				text = "continue: blocked by Stop hook"
			}
			dialect := stop.decisionSource
			if dialect == "" {
				dialect = HookDialectClaudeCode
			}
			if err := e.appendHookContexts(s, []hookInjectedContext{{dialect: dialect, texts: []string{text}}}); err != nil {
				return "", err
			}
		}
		for _, call := range toolCalls {
			if err := e.executeToolCall(ctx, s, turn, step, call); err != nil {
				return "", err
			}
		}
		for _, item := range steering {
			if _, err := e.admitPrompt(ctx, s, item); err != nil {
				return "", err
			}
		}
		if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": turn, "step": step}); err != nil {
			return "", err
		}
		// Rebuild from durable events so the next provider request includes the
		// assistant tool calls and role=tool results in exact order.
		messages = e.durableMessages(s, turn)
	}
	err = fmt.Errorf("agent: maximum tool steps (%d) exceeded", maxToolSteps)
	e.closeOpenTurn(s, turn, map[string]any{"kind": "error", "error": map[string]any{"message": err.Error(), "code": "MAX_STEPS"}})
	return "", err
}

func (e *Engine) claimSteering(s *Session) []*queuedPrompt {
	s.mu.Lock()
	items := append([]*queuedPrompt(nil), s.steering...)
	var event Event
	if len(items) > 0 {
		event, _ = appendEventLocked(s, "agent/inbox/spliced", map[string]any{
			"target": "next-step", "start": 0, "removedCount": len(items), "inserted": []any{},
		}, nil, nil, false)
	}
	s.steering = nil
	id := s.Header.ID
	s.mu.Unlock()
	if len(items) > 0 {
		e.publishEvent(id, event)
		e.emitQueue(s)
	}
	return items
}

func contentValueText(value any) string {
	var b strings.Builder
	switch content := value.(type) {
	case map[string]any:
		if message, ok := content["message"].(map[string]any); ok {
			return contentValueText(message["content"])
		}
		return contentValueText(content["content"])
	case []ContentBlock:
		for _, block := range content {
			if block.Type == "text" {
				b.WriteString(block.Text)
			}
		}
	case []any:
		for _, item := range content {
			block, ok := item.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := block["type"].(string)
			if typ != "" && typ != "text" {
				continue
			}
			if text, _ := block["text"].(string); text != "" {
				b.WriteString(text)
			}
		}
	}
	return b.String()
}

func eventTurn(value any) (int, bool) {
	data, ok := value.(map[string]any)
	if !ok {
		return 0, false
	}
	v, ok := data["turn"]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return n, true
	case float64:
		return int(n), true
	case json.Number:
		value, err := n.Int64()
		return int(value), err == nil
	default:
		return 0, false
	}
}

func dMap(v any) (map[string]any, bool) { m, ok := v.(map[string]any); return m, ok }

// WaitForIdle is useful to custom frontends and integration tests.
func (e *Engine) WaitForIdle(ctx context.Context, id string) error {
	s, err := e.getSession(id)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		running := s.Running || s.maintenance
		s.mu.Unlock()
		if !running {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
