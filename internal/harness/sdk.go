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

const (
	sdkReconcileInterval    = 100 * time.Millisecond
	sdkShutdownRequestDrain = 10 * time.Millisecond
)

// SDKServerOptions controls deployment-specific JSON-RPC result semantics.
type SDKServerOptions struct {
	MaxTokensAsSuccess bool
}

// ServeJSONRPC implements the upstream newline-delimited JSON-RPC SDK protocol
// over caller-owned streams. It keeps stdout-safe transport separate from HTTP.
func (e *Engine) ServeJSONRPC(ctx context.Context, input io.Reader, output io.Writer) error {
	return e.ServeJSONRPCWithOptions(ctx, input, output, SDKServerOptions{})
}

// ServeJSONRPCWithOptions serves the SDK protocol with deployment-specific
// result mapping while preserving ServeJSONRPC's default behavior.
func (e *Engine) ServeJSONRPCWithOptions(ctx context.Context, input io.Reader, output io.Writer, options SDKServerOptions) error {
	serveCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	server := newSDKServerWithOptions(e, ctx, cancel, output, options)
	defer func() { _ = server.close() }()
	type scanResult struct {
		request jsonRPCRequest
	}
	requests := make(chan scanResult, 64)
	scanDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(input)
		scanner.Buffer(make([]byte, 4096), 16<<20)
		for scanner.Scan() {
			var request jsonRPCRequest
			if err := json.Unmarshal(scanner.Bytes(), &request); err != nil || request.Method == "" || !isJSONRPCRequestID(request.ID) {
				continue
			}
			select {
			case requests <- scanResult{request: request}:
			case <-serveCtx.Done():
				close(requests)
				scanDone <- context.Cause(serveCtx)
				return
			}
		}
		close(requests)
		scanDone <- scanner.Err()
	}()

	var handlers sync.WaitGroup
	var beforeShutdown sync.WaitGroup
	var handlerErrOnce sync.Once
	var handlerErr error
	var shutdownOnce sync.Once
	shutdownComplete := make(chan struct{})
	shuttingDown := false
	dispatch := func(request jsonRPCRequest) {
		if shuttingDown && request.Method != "shutdown" {
			return
		}
		handlers.Add(1)
		if request.Method != "shutdown" {
			beforeShutdown.Add(1)
		}
		go func() {
			defer handlers.Done()
			if request.Method == "shutdown" {
				beforeShutdown.Wait()
			} else {
				defer beforeShutdown.Done()
			}
			if err := server.handle(request); err != nil {
				handlerErrOnce.Do(func() { handlerErr = err })
			}
			if request.Method == "shutdown" {
				shutdownOnce.Do(func() { close(shutdownComplete) })
			}
		}()
	}
	waitHandlers := func(scanErr error) error {
		handlers.Wait()
		if scanErr != nil {
			return scanErr
		}
		return handlerErr
	}
	for {
		select {
		case scanned, ok := <-requests:
			if !ok {
				return waitHandlers(<-scanDone)
			}
			if scanned.request.Method == "shutdown" {
				shuttingDown = true
			}
			dispatch(scanned.request)
		case <-shutdownComplete:
			timer := time.NewTimer(sdkShutdownRequestDrain)
			for {
				select {
				case scanned, ok := <-requests:
					if !ok {
						if !timer.Stop() {
							<-timer.C
						}
						return waitHandlers(<-scanDone)
					}
					if scanned.request.Method == "shutdown" {
						dispatch(scanned.request)
					}
				case <-timer.C:
					return waitHandlers(nil)
				case <-serveCtx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return waitHandlers(context.Cause(serveCtx))
				}
			}
		case <-serveCtx.Done():
			return waitHandlers(context.Cause(serveCtx))
		}
	}
}

// isJSONRPCRequestID mirrors the SDK transport's request boundary: only
// string and number ids identify requests. Frames with a missing/null,
// boolean, array, or object id are notifications (or malformed frames) and
// must not be dispatched to the server handler.
func isJSONRPCRequestID(id any) bool {
	switch id.(type) {
	case string, float64, json.Number:
		return true
	default:
		return false
	}
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
	ownedRecords         map[string]sdkOwnedSession
	sessionCreations     map[string]*sdkSessionCreation
	startedSubagents     map[string]string
	sessionStatuses      map[string]bool
	initialEventCounts   map[string]int
	wg                   sync.WaitGroup
	closeOnce            sync.Once
	closeErr             error
	closing              bool
	initialized          bool
	cwd, provider, model string
	reasoningEffort      string
	maxTokens            int
	maxTokensAsSuccess   bool
	subagentEndDispose   func()
}

type sdkSessionCreation struct {
	done   chan struct{}
	record *sdkOwnedSession
	err    error
}

type sdkOwnedSession struct {
	session    *Session
	generation uint64
}

func newSDKServer(e *Engine, ctx context.Context, cancel context.CancelFunc, output io.Writer) *sdkServer {
	return newSDKServerWithOptions(e, ctx, cancel, output, SDKServerOptions{})
}

func newSDKServerWithOptions(e *Engine, ctx context.Context, cancel context.CancelFunc, output io.Writer, options SDKServerOptions) *sdkServer {
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
		subscriptions: map[string]context.CancelFunc{}, ownedSessions: map[string]struct{}{}, ownedRecords: map[string]sdkOwnedSession{}, sessionCreations: map[string]*sdkSessionCreation{}, startedSubagents: startedSubagents,
		sessionStatuses: sessionStatuses, initialEventCounts: initialEventCounts, maxTokensAsSuccess: options.MaxTokensAsSuccess,
	}
	server.subagentEndDispose = e.subscribeSDKSubagentEnd(ctx, server.handleSubagentEnd)
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
			CWD             string          `json:"cwd"`
			Provider        string          `json:"provider"`
			Model           string          `json:"model"`
			ReasoningEffort json.RawMessage `json:"reasoningEffort"`
			MaxTokens       json.RawMessage `json:"maxTokens"`
		}
		if err := decode(request.Params, &params); err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: err.Error()})
			return nil
		}
		if params.CWD == "" || params.Provider == "" || params.Model == "" {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: "invalid initialize parameters"})
			return nil
		}
		var reasoningEffort string
		if len(params.ReasoningEffort) > 0 {
			if string(params.ReasoningEffort) == "null" {
				s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: "initialize reasoningEffort must be a non-empty string"})
				return nil
			}
			if err := json.Unmarshal(params.ReasoningEffort, &reasoningEffort); err != nil || reasoningEffort == "" {
				s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: "initialize reasoningEffort must be a non-empty string"})
				return nil
			}
		}
		var maxTokens int
		if len(params.MaxTokens) > 0 {
			if string(params.MaxTokens) == "null" {
				s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: "initialize maxTokens must be a positive safe integer"})
				return nil
			}
			var ok bool
			maxTokens, ok = safePositiveInteger(json.Number(string(params.MaxTokens)))
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
		if err := s.engine.preflightSubagentLlm(s.ctx, ModelSelection{}, &SubagentAgentOptions{
			Provider: params.Provider, Model: params.Model, ReasoningEffort: reasoningEffort, MaxTokens: maxTokens,
		}, false); err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: err.Error()})
			return nil
		}
		s.stateMu.Lock()
		if s.closing {
			s.stateMu.Unlock()
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: "SDK server is shutting down"})
			return nil
		}
		s.cwd = cwd
		s.provider = params.Provider
		s.model = params.Model
		s.reasoningEffort = reasoningEffort
		s.maxTokens = maxTokens
		s.initialized = true
		s.stateMu.Unlock()
		s.reply(request.ID, map[string]any{"serverInfo": map[string]any{"name": "deepseek-harness-sdk-runtime", "version": s.engine.cfg.Version}}, nil)
	case "session/prompt":
		s.stateMu.Lock()
		initialized := s.initialized
		s.stateMu.Unlock()
		if !initialized {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: "initialize is required"})
			return nil
		}
		var params struct {
			SessionID     string            `json:"sessionId"`
			ContentBlocks []json.RawMessage `json:"contentBlocks"`
		}
		if err := decode(request.Params, &params); err != nil || params.SessionID == "" {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: "invalid session/prompt parameters"})
			return nil
		}
		if err := s.ensureSession(params.SessionID); err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: err.Error()})
			return nil
		}
		s.stateMu.Lock()
		record, owned := s.ownedRecords[params.SessionID]
		s.stateMu.Unlock()
		if !owned {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: "SDK server is shutting down"})
			return nil
		}
		if err := s.assertLiveOwnedSession(params.SessionID, record); err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: err.Error()})
			return nil
		}
		parts, content, err := s.preparePromptContent(params.ContentBlocks)
		if err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32602, Message: err.Error()})
			return nil
		}
		if err := s.assertLiveOwnedSession(params.SessionID, record); err != nil {
			s.reply(request.ID, nil, &jsonRPCError{Code: -32603, Message: err.Error()})
			return nil
		}
		job, command, err := s.engine.enqueuePrompt(s.ctx, params.SessionID, PromptRequest{
			SessionID: params.SessionID, Mode: "queue", Content: parts, Literal: true,
			preparedContent: content, attachmentGuard: &sessionAttachmentGuard{session: record.session, generation: record.generation},
		}, false)
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

func (s *sdkServer) preparePromptContent(raw []json.RawMessage) ([]PromptContentPart, []ContentBlock, error) {
	parts := make([]PromptContentPart, len(raw))
	content := make([]ContentBlock, len(raw))
	inline := make([]PromptContentPart, 0)
	inlineIndexes := make([]int, 0)
	for index, encoded := range raw {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &object); err != nil || object == nil {
			return nil, nil, errors.New("prompt content must be an object")
		}
		var typ string
		if err := json.Unmarshal(object["type"], &typ); err != nil || typ == "" {
			return nil, nil, errors.New("prompt content type is required")
		}
		if typ == "image" && object["data"] != nil {
			var image struct {
				Type     string `json:"type"`
				Data     string `json:"data"`
				MimeType string `json:"mimeType"`
			}
			if err := json.Unmarshal(encoded, &image); err != nil || image.Data == "" || image.MimeType == "" {
				return nil, nil, errors.New("image content requires data and mimeType")
			}
			parts[index] = PromptContentPart{Type: "image", Data: image.Data, MediaType: image.MimeType}
			inline = append(inline, parts[index])
			inlineIndexes = append(inlineIndexes, index)
			continue
		}
		var block ContentBlock
		if err := json.Unmarshal(encoded, &block); err != nil {
			return nil, nil, err
		}
		parts[index] = PromptContentPart{Type: block.Type, Text: block.Text}
		content[index] = block
	}
	if len(inline) == 0 {
		return parts, content, nil
	}
	admitted, err := s.engine.durablePromptContentContext(s.ctx, inline)
	if err != nil {
		return nil, nil, err
	}
	for index, target := range inlineIndexes {
		content[target] = admitted[index]
	}
	return parts, content, nil
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
	if record, ok := s.ownedRecords[id]; ok {
		s.stateMu.Unlock()
		return s.assertLiveOwnedSession(id, record)
	}
	if pending := s.sessionCreations[id]; pending != nil {
		s.stateMu.Unlock()
		<-pending.done
		if pending.err != nil {
			return pending.err
		}
		return s.assertLiveOwnedSession(id, *pending.record)
	}
	pending := &sdkSessionCreation{done: make(chan struct{})}
	if s.sessionCreations == nil {
		s.sessionCreations = map[string]*sdkSessionCreation{}
	}
	s.sessionCreations[id] = pending
	s.stateMu.Unlock()

	record, err := s.createOwnedSession(id)
	s.stateMu.Lock()
	if err == nil && s.closing {
		err = errors.New("SDK server is shutting down")
	}
	if err == nil {
		pending.record = &record
		if s.ownedSessions == nil {
			s.ownedSessions = map[string]struct{}{}
		}
		if s.ownedRecords == nil {
			s.ownedRecords = map[string]sdkOwnedSession{}
		}
		s.ownedSessions[id] = struct{}{}
		s.ownedRecords[id] = record
	}
	pending.err = err
	delete(s.sessionCreations, id)
	s.stateMu.Unlock()
	if err != nil {
		if record.session != nil {
			_ = detachSDKOwnedSession(s.engine, id, record)
		}
		close(pending.done)
		return err
	}
	close(pending.done)
	s.subscribe(id)
	s.reconcileSubagents()
	return nil
}

func (s *sdkServer) createOwnedSession(id string) (sdkOwnedSession, error) {
	s.stateMu.Lock()
	cwd, provider, model := s.cwd, s.provider, s.model
	reasoningEffort, maxTokens := s.reasoningEffort, s.maxTokens
	s.stateMu.Unlock()
	if session, err := s.engine.getSession(id); err == nil {
		session.mu.Lock()
		attached := session.attached
		session.mu.Unlock()
		if attached {
			return sdkOwnedSession{}, fmt.Errorf("session agent exists outside the SDK server: %s", id)
		}
	}
	if _, err := s.engine.CreateSession(s.ctx, cwd, id, ""); err != nil {
		return sdkOwnedSession{}, err
	}
	session, err := s.engine.getSession(id)
	if err != nil {
		return sdkOwnedSession{}, err
	}
	session.mu.Lock()
	record := sdkOwnedSession{session: session, generation: session.attachmentGeneration}
	session.mu.Unlock()
	selection := ModelSelection{Provider: provider, Model: model, ReasoningEffort: reasoningEffort, MaxTokens: maxTokens}
	if err := s.engine.SelectModel(id, selection); err != nil {
		_ = detachSDKOwnedSession(s.engine, id, record)
		return sdkOwnedSession{}, err
	}
	return record, nil
}

func (s *sdkServer) assertLiveOwnedSession(id string, record sdkOwnedSession) error {
	current, err := s.engine.getSession(id)
	if err != nil || current != record.session {
		return fmt.Errorf("session agent was disposed outside the server: %s", id)
	}
	current.mu.Lock()
	live := current.attached && current.attachmentGeneration == record.generation
	current.mu.Unlock()
	if !live {
		return fmt.Errorf("session agent was disposed outside the server: %s", id)
	}
	return nil
}

func (s *sdkServer) close() error {
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closing = true
		pending := make([]*sdkSessionCreation, 0, len(s.sessionCreations))
		for _, creation := range s.sessionCreations {
			pending = append(pending, creation)
		}
		cancels := make([]context.CancelFunc, 0, len(s.subscriptions))
		for _, cancel := range s.subscriptions {
			cancels = append(cancels, cancel)
		}
		s.subscriptions = map[string]context.CancelFunc{}
		owned := make([]string, 0, len(s.ownedSessions))
		for id := range s.ownedSessions {
			owned = append(owned, id)
		}
		records := make(map[string]sdkOwnedSession, len(s.ownedRecords))
		for id, record := range s.ownedRecords {
			records[id] = record
		}
		s.ownedSessions = map[string]struct{}{}
		s.ownedRecords = map[string]sdkOwnedSession{}
		s.stateMu.Unlock()
		if s.subagentEndDispose != nil {
			s.subagentEndDispose()
		}
		for _, creation := range pending {
			<-creation.done
		}

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
		if err := s.engine.drainModelSubagentDescendants(context.Background(), owned); err != nil {
			failures = append(failures, err)
		}
		for _, id := range owned {
			var err error
			if record, ok := records[id]; ok {
				err = detachSDKOwnedSession(s.engine, id, record)
			} else {
				err = detachSDKSession(s.engine, id)
			}
			if err != nil {
				failures = append(failures, err)
			}
		}
		s.closeErr = errors.Join(failures...)
	})
	return s.closeErr
}

func detachSDKSession(e *Engine, id string) error {
	return detachSDKSessionRecordWithDynamicOrigin(e, nil, id, nil)
}

func detachSDKOwnedSession(e *Engine, id string, record sdkOwnedSession) error {
	return detachSDKSessionRecordWithDynamicOrigin(e, nil, id, &record)
}

// detachSDKSessionWithDynamicOrigin is used by dynamic Cordis teardown. A
// run disposer executes on the Cordis loop, so its contained disposal event
// must dispatch inline instead of calling back into that same loop.
func detachSDKSessionWithDynamicOrigin(e *Engine, origin *dynamicCordisRun, id string) error {
	return detachSDKSessionRecordWithDynamicOrigin(e, origin, id, nil)
}

func detachSDKSessionRecordWithDynamicOrigin(e *Engine, origin *dynamicCordisRun, id string, expected *sdkOwnedSession) error {
	session, err := e.getSession(id)
	if err != nil {
		return nil
	}
	session.mu.Lock()
	if expected != nil && (session != expected.session || session.attachmentGeneration != expected.generation) {
		session.mu.Unlock()
		return nil
	}
	if !session.attached {
		session.mu.Unlock()
		return nil
	}
	var events []Event
	if len(session.pending) > 0 {
		event, appendErr := appendEventLocked(session, "agent/inbox/spliced", map[string]any{
			"target": "next-turn", "start": 0, "removedCount": len(session.pending), "inserted": []any{}, "outcome": "canceled",
		}, nil, nil, false)
		if appendErr != nil {
			session.mu.Unlock()
			return appendErr
		}
		events = append(events, event)
	}
	if len(session.steering) > 0 {
		event, appendErr := appendEventLocked(session, "agent/inbox/spliced", map[string]any{
			"target": "next-step", "start": 0, "removedCount": len(session.steering), "inserted": []any{}, "outcome": "canceled",
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
	detachSessionLocked(session)
	session.requestHeaderLogged = false
	activity := session.activity
	cancel := session.Cancel
	session.activity = nil
	session.Cancel = nil
	maintenanceCancel := session.maintenanceCancel
	session.mu.Unlock()
	e.releaseSessionScopedTools(id)
	e.releaseFileReferenceSearch(id)
	e.scheduleWake(id)
	for _, event := range events {
		e.publishEvent(id, event)
	}
	if len(events) > 0 {
		e.emitQueue(session)
	}
	if activity != nil {
		activity.cancel(&agentCancelError{cause: AgentCancelCause{Kind: "disposed"}})
	} else if cancel != nil {
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
	setupErr := e.subagentActivationSetups.releaseChild(session)
	jobsErr := e.jobs.disposeOwner(id, "owner disposed")
	terminalErr := e.terminals.closeOwner(id)
	e.shells.closeOwner(id)
	if origin != nil {
		_ = e.dispatchDynamicCordisEvent(origin, id, true, "session/disposed", dynamicSessionView(session))
	} else {
		e.emitDynamicCordisScopedContained(id, "session/disposed", dynamicSessionView(session))
	}
	return errors.Join(setupErr, jobsErr, terminalErr)
}

type sdkSessionLineage struct {
	id, parent string
	session    *Session
	generation uint64
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
		row := sdkSessionLineage{
			id: session.Header.ID, parent: session.Header.ParentSession,
			session: session, generation: session.attachmentGeneration,
		}
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
	if s.ownedRecords == nil {
		s.ownedRecords = map[string]sdkOwnedSession{}
	}
	for _, row := range lineage {
		if _, owned := s.ownedSessions[row.id]; owned {
			if _, recorded := s.ownedRecords[row.id]; !recorded && row.session != nil {
				s.ownedRecords[row.id] = sdkOwnedSession{session: row.session, generation: row.generation}
			}
		}
	}
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
			currentRunning := session.Running
			session.mu.Unlock()

			for _, event := range batch {
				if event.Type == "turn/start" {
					s.notifySessionStatus(id, true)
				}
				s.notify("session.event", map[string]any{"sessionId": id, "event": event})
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

func (s *sdkServer) handleSubagentEnd(event sdkSubagentEnd) {
	local, _ := event.Payload["local"].(bool)
	if !local {
		return
	}
	provider, _ := event.Payload["provider"].(string)
	child, _ := event.Payload["id"].(string)
	stopReason, _ := event.Payload["stopReason"].(string)
	if event.ParentSessionID == "" || provider == "" || child == "" || stopReason == "" {
		return
	}
	status := "error"
	if stopReason == "completed" || stopReason == "max-tokens" && s.maxTokensAsSuccess {
		status = "ok"
	}
	params := map[string]any{
		"provider": provider, "agentId": child, "parentSessionId": event.ParentSessionID, "childSessionId": child,
		"status": status, "stopReason": stopReason,
	}
	if output, ok := event.Payload["lastAssistantMessage"]; ok {
		params["lastAssistantMessage"] = output
	}
	s.stateMu.Lock()
	closing := s.closing
	s.stateMu.Unlock()
	if !closing {
		s.notify("subagent.finished", params)
	}
}

func (e *Engine) JSONRPCError(method string) error {
	return fmt.Errorf("unknown DeepSeek Harness SDK runtime method: %s", method)
}
