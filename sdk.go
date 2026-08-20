package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"
)

const sdkReconcileInterval = 100 * time.Millisecond

// ServeJSONRPC implements the upstream newline-delimited JSON-RPC SDK protocol
// over caller-owned streams. It keeps stdout-safe transport separate from HTTP.
func (e *Engine) ServeJSONRPC(ctx context.Context, input io.Reader, output io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	server := newSDKServer(e, ctx, cancel, output)
	defer func() { _ = server.close() }()
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	for scanner.Scan() {
		var request jsonRPCRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			continue
		}
		if request.Method == "" {
			continue
		}
		if err := server.handle(request); err != nil {
			return err
		}
		if request.Method == "shutdown" {
			return nil
		}
	}
	return scanner.Err()
}

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}
type jsonRPCResponse struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      any           `json:"id"`
	Result  any           `json:"result,omitempty"`
	Error   *jsonRPCError `json:"error,omitempty"`
}
type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}
type sdkServer struct {
	engine               *Engine
	ctx                  context.Context
	cancel               context.CancelFunc
	output               io.Writer
	mu                   sync.Mutex
	stateMu              sync.Mutex
	subscriptions        map[string]context.CancelFunc
	ownedSessions        map[string]struct{}
	startedSubagents     map[string]string
	sessionStatuses      map[string]bool
	initialEventCounts   map[string]int
	wg                   sync.WaitGroup
	closeOnce            sync.Once
	closeErr             error
	closing              bool
	initialized          bool
	cwd, provider, model string
	maxTokens            int
}

func newSDKServer(e *Engine, ctx context.Context, cancel context.CancelFunc, output io.Writer) *sdkServer {
	host := e.SubscribeHost(ctx)
	e.mu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.mu.RUnlock()
	initialEventCounts := make(map[string]int, len(sessions))
	startedSubagents := map[string]string{}
	sessionStatuses := map[string]bool{}
	for _, session := range sessions {
		session.mu.Lock()
		initialEventCounts[session.Header.ID] = len(session.Events)
		sessionStatuses[session.Header.ID] = session.Running
		if session.Header.ParentSession != "" {
			startedSubagents[session.Header.ID] = session.Header.ParentSession
		}
		session.mu.Unlock()
	}
	server := &sdkServer{
		engine: e, ctx: ctx, cancel: cancel, output: output,
		subscriptions: map[string]context.CancelFunc{}, ownedSessions: map[string]struct{}{}, startedSubagents: startedSubagents,
		sessionStatuses: sessionStatuses, initialEventCounts: initialEventCounts,
	}
	server.watchHost(host)
	for _, session := range sessions {
		server.subscribe(session.Header.ID)
	}
	return server
}

func (s *sdkServer) write(value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeJSONLine(s.output, value)
}
func (s *sdkServer) notify(method string, params any) {
	_ = s.write(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}
func (s *sdkServer) reply(id any, result any, err *jsonRPCError) {
	if id == nil {
		return
	}
	_ = s.write(jsonRPCResponse{JSONRPC: "2.0", ID: id, Result: result, Error: err})
}

func (s *sdkServer) handle(request jsonRPCRequest) error {
	switch request.Method {
	case "initialize":
		var params struct {
			CWD       string       `json:"cwd"`
			Provider  string       `json:"provider"`
			Model     string       `json:"model"`
			MaxTokens *json.Number `json:"maxTokens"`
		}
		if err := decode(request.Params, &params); err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: err.Error()})
			return nil
		}
		if params.CWD == "" || params.Provider == "" || params.Model == "" {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: "invalid initialize parameters"})
			return nil
		}
		maxTokens := 0
		if params.MaxTokens != nil {
			var ok bool
			maxTokens, ok = safePositiveInteger(*params.MaxTokens)
			if !ok {
				s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: "initialize maxTokens must be a positive safe integer"})
				return nil
			}
		}
		cwd, err := filepath.Abs(params.CWD)
		if err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: "initialize cwd is invalid"})
			return nil
		}
		s.engine.mu.RLock()
		_, providerRegistered := s.engine.providers[params.Provider]
		s.engine.mu.RUnlock()
		if !providerRegistered {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: fmt.Sprintf("no adapter registered for provider %q", params.Provider)})
			return nil
		}
		s.cwd = cwd
		s.provider = params.Provider
		s.model = params.Model
		s.maxTokens = maxTokens
		s.initialized = true
		s.reply(request.ID, map[string]any{"serverInfo": map[string]any{"name": "deepseek-harness-sdk-runtime", "version": s.engine.cfg.Version}}, nil)
	case "session/prompt":
		if !s.initialized {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: "initialize is required"})
			return nil
		}
		var params struct {
			SessionID     string         `json:"sessionId"`
			ContentBlocks []ContentBlock `json:"contentBlocks"`
		}
		if err := decode(request.Params, &params); err != nil || params.SessionID == "" {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: "invalid session/prompt parameters"})
			return nil
		}
		if err := s.ensureSession(params.SessionID); err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: err.Error()})
			return nil
		}
		parts := make([]PromptContentPart, 0, len(params.ContentBlocks))
		for _, b := range params.ContentBlocks {
			parts = append(parts, PromptContentPart{Type: b.Type, Text: b.Text})
		}
		job, command, err := s.engine.enqueuePrompt(s.ctx, params.SessionID, PromptRequest{SessionID: params.SessionID, Mode: "queue", Content: parts, Literal: true}, false)
		if err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: err.Error()})
			return nil
		}
		if command != nil || job == nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: "session/prompt did not enqueue a message"})
			return nil
		}
		s.reply(request.ID, map[string]any{"messageId": job.id}, nil)
	case "shutdown":
		if err := s.close(); err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: err.Error()})
			return nil
		}
		s.reply(request.ID, map[string]any{}, nil)
	default:
		s.reply(request.ID, nil, &jsonRPCError{Code: -32601, Message: "method not found"})
	}
	return nil
}

const maxSafeInteger = int64(1<<53 - 1)

func isSafePositiveInteger(value json.Number) bool {
	_, ok := safePositiveInteger(value)
	return ok
}

func safePositiveInteger(value json.Number) (int, bool) {
	integer, err := value.Int64()
	if err != nil || integer <= 0 || integer > maxSafeInteger || integer > int64(^uint(0)>>1) {
		return 0, false
	}
	return int(integer), true
}

func (s *sdkServer) ensureSession(id string) error {
	s.stateMu.Lock()
	if s.closing {
		s.stateMu.Unlock()
		return errors.New("SDK server is shutting down")
	}
	if _, ok := s.ownedSessions[id]; ok {
		s.stateMu.Unlock()
		return nil
	}
	s.stateMu.Unlock()

	session, err := s.engine.getSession(id)
	if err == nil {
		session.mu.Lock()
		attached := session.attached
		session.mu.Unlock()
		if attached {
			return fmt.Errorf("session agent exists outside the SDK server: %s", id)
		}
	}
	if _, err := s.engine.CreateSession(s.ctx, s.cwd, id, ""); err != nil {
		return err
	}
	selection := ModelSelection{Provider: s.provider, Model: s.model, MaxTokens: s.maxTokens}
	if err := s.engine.SelectModel(id, selection); err != nil {
		_ = detachSDKSession(s.engine, id)
		return err
	}
	s.stateMu.Lock()
	if s.ownedSessions == nil {
		s.ownedSessions = map[string]struct{}{}
	}
	s.ownedSessions[id] = struct{}{}
	s.stateMu.Unlock()
	s.subscribe(id)
	s.reconcileSubagents()
	return nil
}

func (s *sdkServer) close() error {
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closing = true
		cancels := make([]context.CancelFunc, 0, len(s.subscriptions))
		for _, cancel := range s.subscriptions {
			cancels = append(cancels, cancel)
		}
		s.subscriptions = map[string]context.CancelFunc{}
		owned := make([]string, 0, len(s.ownedSessions))
		for id := range s.ownedSessions {
			owned = append(owned, id)
		}
		s.ownedSessions = map[string]struct{}{}
		s.stateMu.Unlock()

		for _, cancel := range cancels {
			cancel()
		}
		if s.cancel != nil {
			s.cancel()
		}
		s.wg.Wait()
		s.stateMu.Lock()
		s.startedSubagents = map[string]string{}
		s.stateMu.Unlock()
		failures := make([]error, 0)
		for _, id := range owned {
			if err := detachSDKSession(s.engine, id); err != nil {
				failures = append(failures, err)
			}
		}
		s.closeErr = errors.Join(failures...)
	})
	return s.closeErr
}

func detachSDKSession(e *Engine, id string) error {
	return detachSDKSessionWithDynamicOrigin(e, nil, id)
}

// detachSDKSessionWithDynamicOrigin is used by dynamic Cordis teardown. A
// run disposer executes on the Cordis loop, so its contained disposal event
// must dispatch inline instead of calling back into that same loop.
func detachSDKSessionWithDynamicOrigin(e *Engine, origin *dynamicCordisRun, id string) error {
	session, err := e.getSession(id)
	if err != nil {
		return nil
	}
	session.mu.Lock()
	if !session.attached {
		session.mu.Unlock()
		return nil
	}
	var events []Event
	if len(session.pending) > 0 {
		event, appendErr := appendEventLocked(session, "agent/inbox/spliced", map[string]any{
			"target": "next-turn", "start": 0, "removedCount": len(session.pending), "inserted": []any{},
		}, nil, nil, false)
		if appendErr != nil {
			session.mu.Unlock()
			return appendErr
		}
		events = append(events, event)
	}
	if len(session.steering) > 0 {
		event, appendErr := appendEventLocked(session, "agent/inbox/spliced", map[string]any{
			"target": "next-step", "start": 0, "removedCount": len(session.steering), "inserted": []any{},
		}, nil, nil, false)
		if appendErr != nil {
			session.mu.Unlock()
			return appendErr
		}
		events = append(events, event)
	}
	pending := append(append([]*queuedPrompt(nil), session.pending...), session.steering...)
	session.pending = nil
	session.steering = nil
	session.attached = false
	session.requestHeaderLogged = false
	cancel := session.Cancel
	maintenanceCancel := session.maintenanceCancel
	session.mu.Unlock()
	e.scheduleWake(id)
	for _, event := range events {
		e.publishEvent(id, event)
	}
	if len(events) > 0 {
		e.emitQueue(session)
	}
	if cancel != nil {
		cancel()
	}
	if maintenanceCancel != nil {
		maintenanceCancel()
	}
	for _, item := range pending {
		if item.done != nil {
			item.done <- promptOutcome{err: errors.New("SDK server shut down before the prompt ran")}
		}
	}
	terminalErr := e.terminals.closeOwner(id)
	if origin != nil {
		_ = e.dispatchDynamicCordisEvent(origin, id, true, "session/disposed", dynamicSessionView(session))
	} else {
		e.emitDynamicCordisScopedContained(id, "session/disposed", dynamicSessionView(session))
	}
	return terminalErr
}

type sdkSessionLineage struct {
	id, parent string
}

func (s *sdkServer) reconcileSubagents() {
	s.engine.mu.RLock()
	sessions := make([]*Session, 0, len(s.engine.sessions))
	for _, session := range s.engine.sessions {
		sessions = append(sessions, session)
	}
	s.engine.mu.RUnlock()
	lineage := make([]sdkSessionLineage, 0, len(sessions))
	for _, session := range sessions {
		session.mu.Lock()
		row := sdkSessionLineage{id: session.Header.ID, parent: session.Header.ParentSession}
		session.mu.Unlock()
		if row.id != "" && row.parent != "" {
			lineage = append(lineage, row)
		}
	}

	s.stateMu.Lock()
	if s.closing {
		s.stateMu.Unlock()
		return
	}
	if s.ownedSessions == nil {
		s.ownedSessions = map[string]struct{}{}
	}
	if s.startedSubagents == nil {
		s.startedSubagents = map[string]string{}
	}
	discovered := make([]sdkSessionLineage, 0)
	for _, row := range lineage {
		if _, seen := s.startedSubagents[row.id]; !seen {
			s.startedSubagents[row.id] = row.parent
			discovered = append(discovered, row)
		}
	}
	inheritSDKOwnedSessions(s.ownedSessions, lineage)
	s.stateMu.Unlock()

	for _, row := range discovered {
		s.notify("subagent.started", map[string]any{"parentSessionId": row.parent, "childSessionId": row.id})
		s.subscribe(row.id)
	}
}

func inheritSDKOwnedSessions(owned map[string]struct{}, lineage []sdkSessionLineage) {
	for changed := true; changed; {
		changed = false
		for _, row := range lineage {
			if _, ok := owned[row.id]; ok {
				continue
			}
			if _, parentOwned := owned[row.parent]; parentOwned {
				owned[row.id] = struct{}{}
				changed = true
			}
		}
	}
}

func (s *sdkServer) reconcileSessions() {
	s.reconcileSubagents()
	s.engine.mu.RLock()
	ids := make([]string, 0, len(s.engine.sessions))
	for id := range s.engine.sessions {
		ids = append(ids, id)
	}
	s.engine.mu.RUnlock()
	for _, id := range ids {
		s.subscribe(id)
	}
}

func (s *sdkServer) notifySessionStatus(id string, running bool) {
	s.stateMu.Lock()
	previous, seen := s.sessionStatuses[id]
	if s.closing {
		s.stateMu.Unlock()
		return
	}
	if s.sessionStatuses == nil {
		s.sessionStatuses = map[string]bool{}
	}
	s.sessionStatuses[id] = running
	s.stateMu.Unlock()
	if seen && previous == running || !seen && !running {
		return
	}
	status := "idle"
	if running {
		status = "running"
	}
	s.notify("session.status", map[string]any{"sessionId": id, "status": status})
}

func (s *sdkServer) watchHost(host <-chan map[string]any) {
	s.stateMu.Lock()
	if s.closing {
		s.stateMu.Unlock()
		return
	}
	s.wg.Add(1)
	s.stateMu.Unlock()
	go func() {
		defer s.wg.Done()
		// ponytail: polling repairs the Engine's intentionally lossy host stream;
		// replace it only if a future lossless global subscription makes this scan measurable.
		ticker := time.NewTicker(sdkReconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case _, ok := <-host:
				if !ok {
					return
				}
				s.reconcileSessions()
			case <-ticker.C:
				s.reconcileSessions()
			}
		}
	}()
}

func (s *sdkServer) subscribe(id string) {
	if id == "" {
		return
	}
	session, err := s.engine.getSession(id)
	if err != nil {
		return
	}
	session.mu.Lock()
	cursor := len(session.Events)
	if baseline, ok := s.initialEventCounts[id]; ok {
		cursor = baseline
	} else if s.initialEventCounts != nil {
		cursor = 0
	}
	session.mu.Unlock()
	ctx, cancel := context.WithCancel(s.ctx)
	s.stateMu.Lock()
	if s.closing {
		s.stateMu.Unlock()
		cancel()
		return
	}
	if s.subscriptions == nil {
		s.subscriptions = map[string]context.CancelFunc{}
	}
	if s.subscriptions[id] != nil {
		s.stateMu.Unlock()
		cancel()
		return
	}
	s.subscriptions[id] = cancel
	s.wg.Add(1)
	s.stateMu.Unlock()
	events := s.engine.Subscribe(ctx, id)
	go func() {
		ticker := time.NewTicker(sdkReconcileInterval)
		defer ticker.Stop()
		defer func() {
			s.stateMu.Lock()
			delete(s.subscriptions, id)
			s.stateMu.Unlock()
			s.wg.Done()
		}()
		flush := func() {
			session.mu.Lock()
			batch := append([]Event(nil), session.Events[cursor:]...)
			allEvents := append([]Event(nil), session.Events...)
			currentRunning := session.Running
			provider := session.Model.Provider
			isSubagent := session.Header.Origin == "subagent"
			session.mu.Unlock()

			for _, event := range batch {
				if event.Type == "turn/start" {
					s.notifySessionStatus(id, true)
				}
				s.notify("session.event", map[string]any{"sessionId": id, "event": event})
				if event.Type == "turn/end" && isSubagent {
					s.stateMu.Lock()
					parent := s.startedSubagents[id]
					s.stateMu.Unlock()
					if parent != "" {
						s.notify("subagent.finished", sdkSubagentFinished(parent, id, provider, allEvents, event))
					}
				}
				cursor++
			}
			s.notifySessionStatus(id, currentRunning)
			s.reconcileSubagents()
		}
		flush()
		for {
			select {
			case <-ctx.Done():
				flush()
				return
			case _, ok := <-events:
				if !ok {
					flush()
					return
				}
				flush()
			case <-ticker.C:
				flush()
			}
		}
	}()
}

func sdkSubagentFinished(parent, child, provider string, events []Event, end Event) map[string]any {
	stopReason := sdkSubagentStopReason(end)
	params := map[string]any{
		"provider": provider, "agentId": child, "parentSessionId": parent, "childSessionId": child,
		"status": "error", "stopReason": stopReason,
	}
	if stopReason == "completed" {
		params["status"] = "ok"
	}
	if output := sdkAssistantOutput(events, end.Seq); output != nil {
		params["lastAssistantMessage"] = output
	}
	return params
}

func sdkSubagentStopReason(end Event) string {
	data, _ := end.Data.(map[string]any)
	reason, _ := data["reason"].(map[string]any)
	kind, _ := reason["kind"].(string)
	switch kind {
	case "completed", "max-tokens", "aborted":
		return kind
	case "blocked", "rejected":
		return "refusal"
	default:
		return "error"
	}
}

func sdkAssistantOutput(events []Event, endSeq int) any {
	if endSeq < 0 || endSeq > len(events) {
		return nil
	}
	start := 0
	for index := endSeq - 1; index >= 0; index-- {
		if events[index].Type == "turn/end" {
			start = index + 1
			break
		}
	}
	var message any
	var partial string
	for _, event := range events[start:endSeq] {
		data, _ := event.Data.(map[string]any)
		switch event.Type {
		case "assistant/message":
			value, _ := data["message"].(map[string]any)
			content := value["content"]
			if sdkContentLength(content) > 0 {
				message = content
			}
		case "assistant/chunk":
			chunk, _ := data["chunk"].(map[string]any)
			if chunk["type"] == "text-delta" {
				text, _ := chunk["text"].(string)
				partial += text
			}
		}
	}
	if message != nil {
		return message
	}
	if partial != "" {
		return []ContentBlock{{Type: "text", Text: partial}}
	}
	return nil
}

func sdkContentLength(value any) int {
	switch content := value.(type) {
	case []ContentBlock:
		return len(content)
	case []any:
		return len(content)
	case []map[string]any:
		return len(content)
	default:
		return 0
	}
}

func (e *Engine) JSONRPCError(method string) error {
	return fmt.Errorf("unknown DeepSeek Harness SDK runtime method: %s", method)
}
