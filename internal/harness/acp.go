package harness

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
)

// ACPProtocolVersion is the Agent Client Protocol version implemented here.
const ACPProtocolVersion = 1

type acpMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Result  json.RawMessage `json:"result"`
	Error   json.RawMessage `json:"error"`
}

type acpError struct {
	Code    int
	Message string
	Data    any
}

type acpPromptState struct {
	cancel    context.CancelFunc
	cancelled bool
	job       *queuedPrompt
	removed   bool
}

type acpSession struct {
	id       string
	mu       sync.Mutex
	inflight *acpPromptState

	outputMu     sync.Mutex
	outputCond   *sync.Cond
	outputIndex  int
	outputClosed bool
	outputErrors map[int]error
}

type acpClientRequest struct {
	interactionID string
	sessionID     string
	approvalID    string
}

type acpServer struct {
	engine *Engine
	ctx    context.Context
	cancel context.CancelFunc
	output io.Writer

	writeMu  sync.Mutex
	stateMu  sync.Mutex
	sessions map[string]*acpSession
	pending  map[string]acpClientRequest
	nextID   int
	closed   bool
	writeErr error
	wg       sync.WaitGroup

	imagePromptEnabled bool
}

// ServeACP serves the upstream Agent Client Protocol as newline-delimited
// JSON-RPC over caller-owned streams. Each connection owns the sessions it
// creates and releases them when the stream closes.
func (e *Engine) ServeACP(ctx context.Context, input io.Reader, output io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	server := &acpServer{
		engine: e, ctx: ctx, cancel: cancel, output: output,
		sessions: map[string]*acpSession{}, pending: map[string]acpClientRequest{},
	}
	server.wg.Add(1)
	go server.forwardInteractions()

	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var message acpMessage
		if err := json.Unmarshal(line, &message); err != nil {
			// The upstream NDJSON stream drops malformed frames and keeps reading.
			continue
		}
		if message.Method == "" {
			server.handleClientResponse(message)
			continue
		}
		if message.Method == "session/prompt" {
			server.wg.Add(1)
			go func() {
				defer server.wg.Done()
				server.handle(message)
			}()
			continue
		}
		server.handle(message)
	}
	scanErr := scanner.Err()
	closeErr := server.close()
	return errors.Join(scanErr, closeErr)
}

func (s *acpServer) handle(message acpMessage) {
	switch message.Method {
	case "initialize":
		var params struct {
			ProtocolVersion *int `json:"protocolVersion"`
		}
		if err := decodeACPParams(message.Params, &params); err != nil || params.ProtocolVersion == nil {
			s.replyError(message.ID, -32602, "invalid initialize parameters", nil)
			return
		}
		s.stateMu.Lock()
		s.imagePromptEnabled = s.supportsImagePrompts()
		imageEnabled := s.imagePromptEnabled
		s.stateMu.Unlock()
		s.reply(message.ID, map[string]any{
			"protocolVersion": ACPProtocolVersion,
			"agentInfo":       map[string]any{"name": "deepseek-harness-acp", "version": "0.0.1"},
			"agentCapabilities": map[string]any{"promptCapabilities": map[string]any{
				"image": imageEnabled, "audio": false, "embeddedContext": false,
			}},
			"authMethods": []any{},
		})
	case "authenticate":
		var params struct {
			MethodID *string `json:"methodId"`
		}
		if err := decodeACPParams(message.Params, &params); err != nil || params.MethodID == nil {
			s.replyError(message.ID, -32602, "invalid authenticate parameters", nil)
			return
		}
		s.reply(message.ID, map[string]any{})
	case "session/new":
		s.handleNewSession(message)
	case "session/prompt":
		s.handlePrompt(message)
	case "session/cancel":
		s.handleCancel(message.Params)
	default:
		s.replyError(message.ID, -32601, "method not found", map[string]any{"method": message.Method})
	}
}

func (s *acpServer) handleNewSession(message acpMessage) {
	var params struct {
		CWD                   string             `json:"cwd"`
		MCPServers            *[]json.RawMessage `json:"mcpServers"`
		AdditionalDirectories []string           `json:"additionalDirectories"`
	}
	if err := decodeACPParams(message.Params, &params); err != nil || params.CWD == "" || params.MCPServers == nil {
		s.replyError(message.ID, -32602, "invalid session/new parameters", nil)
		return
	}
	if !filepath.IsAbs(params.CWD) {
		s.replyError(message.ID, -32602, "cwd must be an absolute path", nil)
		return
	}
	if len(*params.MCPServers) > 0 {
		s.replyError(message.ID, -32602, "mcpServers are not supported by this ACP bridge", nil)
		return
	}
	if len(params.AdditionalDirectories) > 0 {
		s.replyError(message.ID, -32602, "additionalDirectories are not supported by this ACP bridge", nil)
		return
	}
	s.stateMu.Lock()
	closed := s.closed
	s.stateMu.Unlock()
	if closed {
		s.replyError(message.ID, -32603, "the ACP bridge has been disposed", nil)
		return
	}
	id, err := newACPSessionID()
	if err != nil {
		s.replyError(message.ID, -32603, "unable to allocate session id", nil)
		return
	}
	if _, err := s.engine.CreateSession(s.ctx, params.CWD, id, ""); err != nil {
		s.replyError(message.ID, -32603, err.Error(), nil)
		return
	}
	config := s.engine.Config()
	if config.Provider != "" && config.Model != "" {
		if err := s.engine.SelectModel(id, ModelSelection{Provider: config.Provider, Model: config.Model}); err != nil {
			_ = detachSDKSession(s.engine, id)
			s.replyError(message.ID, -32603, err.Error(), nil)
			return
		}
	}
	initialEvents := s.sessionEventCount(id)
	record := &acpSession{id: id, outputIndex: initialEvents, outputErrors: map[int]error{}}
	record.outputCond = sync.NewCond(&record.outputMu)
	updates := s.engine.Subscribe(s.ctx, id)
	s.stateMu.Lock()
	if s.closed {
		s.stateMu.Unlock()
		_ = detachSDKSession(s.engine, id)
		s.replyError(message.ID, -32603, "connection closed during session/new", nil)
		return
	}
	s.sessions[id] = record
	s.stateMu.Unlock()
	s.wg.Add(1)
	go s.forwardSessionEvents(record, updates)
	s.reply(message.ID, map[string]any{"sessionId": id})
}

func (s *acpServer) handlePrompt(message acpMessage) {
	var params struct {
		SessionID string             `json:"sessionId"`
		Prompt    *[]json.RawMessage `json:"prompt"`
	}
	if err := decodeACPParams(message.Params, &params); err != nil || params.SessionID == "" || params.Prompt == nil {
		s.replyError(message.ID, -32602, "invalid session/prompt parameters", nil)
		return
	}
	record := s.session(params.SessionID)
	if record == nil {
		s.replyError(message.ID, -32602, "unknown session: "+params.SessionID, nil)
		return
	}
	runCtx, cancel := context.WithCancel(s.ctx)
	inflight := &acpPromptState{cancel: cancel}
	record.mu.Lock()
	if record.inflight != nil {
		record.mu.Unlock()
		cancel()
		s.replyError(message.ID, -32602, "a prompt is already in flight for this session", nil)
		return
	}
	record.inflight = inflight
	record.mu.Unlock()
	defer func() {
		cancel()
		record.mu.Lock()
		if record.inflight == inflight {
			record.inflight = nil
		}
		record.mu.Unlock()
	}()

	s.stateMu.Lock()
	imageEnabled := s.imagePromptEnabled
	s.stateMu.Unlock()
	parts, contentErr := decodeACPPrompt(*params.Prompt, imageEnabled)
	if contentErr != nil {
		s.replyError(message.ID, -32602, contentErr.Error(), nil)
		return
	}
	before := s.sessionEventCount(params.SessionID)
	job, command, runErr := s.engine.enqueuePrompt(runCtx, params.SessionID, PromptRequest{
		SessionID: params.SessionID, Mode: "queue", Content: parts, Literal: true,
		Source: map[string]any{"kind": "user", "transport": "acp"},
	}, true)
	record.mu.Lock()
	inflight.job = job
	cancelled := inflight.cancelled
	record.mu.Unlock()
	if cancelled && job != nil {
		if s.removeQueuedPrompt(params.SessionID, job) {
			s.reply(message.ID, map[string]any{"stopReason": "cancelled"})
			return
		}
		_ = s.engine.CancelSession(params.SessionID)
	}
	if runErr == nil && command == nil && job != nil {
		select {
		case <-runCtx.Done():
			runErr = runCtx.Err()
		case outcome := <-job.done:
			runErr = outcome.err
		}
	}
	record.mu.Lock()
	inflight.job = nil
	cancelled = inflight.cancelled
	removed := inflight.removed
	record.mu.Unlock()
	if cancelled && (job == nil || removed) {
		s.reply(message.ID, map[string]any{"stopReason": "cancelled"})
		return
	}
	if waitErr := s.engine.WaitForIdle(s.ctx, params.SessionID); waitErr != nil && !cancelled {
		s.replyError(message.ID, -32603, "prompt settlement failed: "+waitErr.Error(), nil)
		return
	}
	events := s.sessionEventsFrom(params.SessionID, before)
	if err := record.waitForOutput(s.ctx, before+len(events)); err != nil && !cancelled {
		s.replyError(message.ID, -32603, "assistant output settlement failed: "+err.Error(), nil)
		return
	}
	if err := record.outputError(before, before+len(events)); err != nil {
		s.replyError(message.ID, -32603, "assistant output delivery failed: "+err.Error(), nil)
		return
	}
	if cancelled {
		s.reply(message.ID, map[string]any{"stopReason": "cancelled"})
		return
	}
	reason, detail := acpTurnOutcome(events)
	if reason == "error" {
		if detail == "" && runErr != nil {
			detail = runErr.Error()
		}
		if detail == "" {
			detail = "unknown turn failure"
		}
		s.replyError(message.ID, -32603, "turn failed: "+detail, nil)
		return
	}
	if runErr != nil && reason != "aborted" && !errors.Is(runErr, context.Canceled) {
		s.replyError(message.ID, -32603, "turn failed: "+runErr.Error(), nil)
		return
	}
	if reason == "interrupted" {
		s.reply(message.ID, map[string]any{"stopReason": "cancelled"})
		return
	}
	s.reply(message.ID, map[string]any{"stopReason": "end_turn"})
}

func (s *acpServer) handleCancel(raw json.RawMessage) {
	var params struct {
		SessionID string `json:"sessionId"`
	}
	if decodeACPParams(raw, &params) != nil || params.SessionID == "" {
		return
	}
	record := s.session(params.SessionID)
	if record == nil {
		return
	}
	record.mu.Lock()
	if record.inflight != nil {
		inflight := record.inflight
		inflight.cancelled = true
		inflight.cancel()
		if inflight.job != nil {
			inflight.removed = s.removeQueuedPrompt(params.SessionID, inflight.job)
			if !inflight.removed {
				_ = s.engine.CancelSession(params.SessionID)
			}
		}
		record.mu.Unlock()
		return
	}
	record.mu.Unlock()
}

func (s *acpServer) removeQueuedPrompt(sessionID string, job *queuedPrompt) bool {
	session, err := s.engine.getSession(sessionID)
	if err != nil {
		return false
	}
	session.mu.Lock()
	for index, queued := range session.pending {
		if queued != job {
			continue
		}
		event, appendErr := appendEventLocked(session, "agent/inbox/spliced", map[string]any{
			"target": "next-turn", "start": index, "removedCount": 1, "inserted": []any{}, "outcome": "canceled",
		}, nil, nil, false)
		if appendErr != nil {
			session.mu.Unlock()
			return false
		}
		session.pending = append(session.pending[:index], session.pending[index+1:]...)
		session.mu.Unlock()
		s.engine.publishEvent(sessionID, event)
		s.engine.emitQueue(session)
		return true
	}
	session.mu.Unlock()
	return false
}

func (s *acpServer) supportsImagePrompts() bool {
	config := s.engine.Config()
	s.engine.mu.RLock()
	provider := s.engine.providers[config.Provider]
	s.engine.mu.RUnlock()
	if provider == nil || config.Model == "" {
		return false
	}
	models, err := provider.Models(s.ctx)
	if err != nil {
		return false
	}
	for _, model := range models {
		if model.ID != config.Model {
			continue
		}
		for _, modality := range model.InputModalities {
			if modality == "image" {
				return true
			}
		}
	}
	return false
}

func decodeACPPrompt(raw []json.RawMessage, imageEnabled bool) ([]PromptContentPart, error) {
	parts := make([]PromptContentPart, 0, len(raw))
	pendingText := ""
	imageCount, imageBytes := 0, 0
	flushText := func() {
		if pendingText == "" {
			return
		}
		parts = append(parts, PromptContentPart{Type: "text", Text: pendingText})
		pendingText = ""
	}
	for _, item := range raw {
		var block map[string]json.RawMessage
		if err := json.Unmarshal(item, &block); err != nil {
			return nil, errors.New("prompt content must be an object")
		}
		typ, ok := acpRequiredString(block, "type")
		if !ok {
			return nil, errors.New("prompt content type is required")
		}
		switch typ {
		case "text":
			text, ok := acpRequiredString(block, "text")
			if !ok {
				return nil, errors.New("text content requires text")
			}
			pendingText += text
		case "resource_link":
			name, nameOK := acpRequiredString(block, "name")
			uri, uriOK := acpRequiredString(block, "uri")
			if !nameOK || !uriOK {
				return nil, errors.New("resource_link content requires name and uri")
			}
			encodedName, _ := json.Marshal(name)
			encodedURI, _ := json.Marshal(uri)
			pendingText += "\n[resource_link name=" + string(encodedName) + " uri=" + string(encodedURI) + "]\n"
		case "image":
			if !imageEnabled {
				return nil, errors.New("inline image prompts were not advertised by this connection")
			}
			data, dataOK := acpRequiredString(block, "data")
			mediaType, mediaOK := acpRequiredString(block, "mimeType")
			if !dataOK || !mediaOK {
				return nil, errors.New("image content requires data and mimeType")
			}
			image, err := prepareImage(mediaType, data, "")
			if err != nil {
				return nil, err
			}
			imageCount++
			imageBytes += len(image.data)
			flushText()
			parts = append(parts, PromptContentPart{Type: "image", Data: data, MediaType: mediaType})
		case "audio":
			return nil, errors.New("audio prompt content is not supported")
		case "resource":
			return nil, errors.New("embedded resource prompt content is not supported")
		default:
			return nil, fmt.Errorf("unsupported ACP prompt content %q", typ)
		}
	}
	if imageCount > maxImagesPerMessage {
		return nil, errors.New("attachment-error: image batch exceeds the configured image-count limit")
	}
	if imageBytes > maxMessageImageBytes {
		return nil, errors.New("attachment-error: image batch exceeds the configured aggregate image-byte limit")
	}
	flushText()
	hasContent := false
	for _, part := range parts {
		if part.Type == "image" || strings.TrimSpace(part.Text) != "" {
			hasContent = true
			break
		}
	}
	if !hasContent {
		return nil, errors.New("empty prompt")
	}
	return parts, nil
}

func acpRequiredString(object map[string]json.RawMessage, key string) (string, bool) {
	raw, ok := object[key]
	if !ok {
		return "", false
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

func (s *acpServer) forwardSessionEvents(record *acpSession, updates <-chan Event) {
	defer s.wg.Done()
	for range updates {
		s.drainSessionOutput(record)
	}
	s.drainSessionOutput(record)
	record.outputMu.Lock()
	record.outputClosed = true
	record.outputCond.Broadcast()
	record.outputMu.Unlock()
}

func (s *acpServer) drainSessionOutput(record *acpSession) {
	for {
		record.outputMu.Lock()
		start := record.outputIndex
		record.outputMu.Unlock()
		events := s.sessionEventsFrom(record.id, start)
		if len(events) == 0 {
			return
		}
		for _, event := range events {
			err := s.sendAssistantEvent(record.id, event)
			record.outputMu.Lock()
			index := record.outputIndex
			if err != nil {
				record.outputErrors[index] = err
			}
			record.outputIndex++
			record.outputCond.Broadcast()
			record.outputMu.Unlock()
		}
	}
}

func (record *acpSession) waitForOutput(ctx context.Context, target int) error {
	record.outputMu.Lock()
	defer record.outputMu.Unlock()
	for record.outputIndex < target && !record.outputClosed && ctx.Err() == nil {
		record.outputCond.Wait()
	}
	if record.outputIndex >= target {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("ACP session output stream closed")
}

func (record *acpSession) outputError(start, end int) error {
	record.outputMu.Lock()
	defer record.outputMu.Unlock()
	for index := start; index < end; index++ {
		if err := record.outputErrors[index]; err != nil {
			return err
		}
	}
	return nil
}

func (s *acpServer) sendAssistantEvent(sessionID string, event Event) error {
	if event.Type != "assistant/message" {
		return nil
	}
	for _, block := range acpAssistantBlocks(event.Data) {
		var content map[string]any
		switch block.Type {
		case "text":
			if block.Text == "" {
				continue
			}
			content = map[string]any{"type": "text", "text": block.Text}
		case "image":
			if block.Attachment == nil {
				continue
			}
			data, err := s.engine.readImage(*block.Attachment)
			if err != nil {
				return errors.New("the attachment is unavailable or corrupt")
			}
			content = map[string]any{
				"type": "image", "data": base64.StdEncoding.EncodeToString(data),
				"mimeType": block.Attachment.MediaType,
			}
		default:
			continue
		}
		if err := s.write(map[string]any{
			"jsonrpc": "2.0", "method": "session/update",
			"params": map[string]any{
				"sessionId": sessionID,
				"update":    map[string]any{"sessionUpdate": "agent_message_chunk", "content": content},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func acpAssistantBlocks(value any) []ContentBlock {
	data, _ := value.(map[string]any)
	message, _ := data["message"].(map[string]any)
	content := message["content"]
	if content == nil {
		content = data["content"]
	}
	switch blocks := content.(type) {
	case []ContentBlock:
		return append([]ContentBlock(nil), blocks...)
	case []any:
		result := make([]ContentBlock, 0, len(blocks))
		for _, raw := range blocks {
			encoded, err := json.Marshal(raw)
			if err != nil {
				continue
			}
			var block ContentBlock
			if json.Unmarshal(encoded, &block) == nil {
				result = append(result, block)
			}
		}
		return result
	default:
		return nil
	}
}

func acpTurnOutcome(events []Event) (string, string) {
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type != "turn/end" {
			continue
		}
		data, _ := events[index].Data.(map[string]any)
		reason, _ := data["reason"].(map[string]any)
		kind, _ := reason["kind"].(string)
		if kind != "error" {
			return kind, ""
		}
		errorValue, _ := reason["error"].(map[string]any)
		detail, _ := errorValue["message"].(string)
		return kind, detail
	}
	return "", ""
}

func (s *acpServer) sessionEventCount(id string) int {
	session, err := s.engine.getSession(id)
	if err != nil {
		return 0
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return len(session.Events)
}

func (s *acpServer) sessionEventsFrom(id string, start int) []Event {
	session, err := s.engine.getSession(id)
	if err != nil {
		return nil
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if start < 0 || start > len(session.Events) {
		start = len(session.Events)
	}
	return append([]Event(nil), session.Events[start:]...)
}

func (s *acpServer) forwardInteractions() {
	defer s.wg.Done()
	updates := s.engine.SubscribeMux(s.ctx)
	for frame := range updates {
		if frame["type"] != "host/pending-request" || frame["method"] != "approval/requested" {
			continue
		}
		sessionID, _ := frame["sessionId"].(string)
		if s.session(sessionID) == nil {
			continue
		}
		interactionID, _ := frame["rpcId"].(string)
		payload, _ := frame["payload"].(map[string]any)
		approvalID, _ := payload["approvalId"].(string)
		callID, _ := payload["callId"].(string)
		if interactionID == "" || approvalID == "" || callID == "" {
			s.engine.ResolveInteraction(interactionID, map[string]any{
				"ok": false, "error": map[string]any{"code": "cancelled", "message": "ACP permission request has no tool-call identity"},
			})
			continue
		}
		s.stateMu.Lock()
		s.nextID++
		requestID := fmt.Sprintf("acp-%d", s.nextID)
		s.pending[requestID] = acpClientRequest{interactionID: interactionID, sessionID: sessionID, approvalID: approvalID}
		s.stateMu.Unlock()
		if err := s.write(map[string]any{
			"jsonrpc": "2.0", "id": requestID, "method": "session/request_permission",
			"params": map[string]any{
				"sessionId": sessionID,
				"toolCall":  map[string]any{"toolCallId": callID},
				"options": []any{
					map[string]any{"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
					map[string]any{"optionId": "reject-once", "name": "Reject", "kind": "reject_once"},
				},
			},
		}); err != nil {
			s.resolveClientRequest(requestID, "unavailable")
		}
	}
}

func (s *acpServer) handleClientResponse(message acpMessage) {
	if len(message.ID) == 0 {
		return
	}
	var requestID string
	if json.Unmarshal(message.ID, &requestID) != nil || requestID == "" {
		return
	}
	if len(message.Error) > 0 && string(message.Error) != "null" {
		s.resolveClientRequest(requestID, "unavailable")
		return
	}
	var result struct {
		Outcome struct {
			Outcome  string `json:"outcome"`
			OptionID string `json:"optionId"`
		} `json:"outcome"`
	}
	if json.Unmarshal(message.Result, &result) != nil {
		s.resolveClientRequest(requestID, "unavailable")
		return
	}
	outcome := "unavailable"
	switch result.Outcome.Outcome {
	case "cancelled":
		outcome = "cancelled"
	case "selected":
		outcome = "rejected"
		if result.Outcome.OptionID == "allow-once" {
			outcome = "allowed-once"
		}
	}
	s.resolveClientRequest(requestID, outcome)
}

func (s *acpServer) resolveClientRequest(requestID, outcome string) {
	s.stateMu.Lock()
	request, ok := s.pending[requestID]
	if ok {
		delete(s.pending, requestID)
	}
	s.stateMu.Unlock()
	if !ok {
		return
	}
	if outcome == "unavailable" {
		s.engine.ResolveInteraction(request.interactionID, map[string]any{
			"ok": false, "error": map[string]any{"code": "unavailable", "message": "ACP permission client is unavailable"},
		})
		return
	}
	s.engine.ResolveInteraction(request.interactionID, map[string]any{
		"ok": true,
		"value": map[string]any{
			"sessionId": request.sessionID, "approvalId": request.approvalID, "outcome": outcome,
		},
	})
}

func (s *acpServer) session(id string) *acpSession {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.sessions[id]
}

func (s *acpServer) reply(id json.RawMessage, result any) {
	if len(id) == 0 || string(id) == "null" {
		return
	}
	s.write(map[string]any{"jsonrpc": "2.0", "id": rawACPValue(id), "result": result})
}

func (s *acpServer) replyError(id json.RawMessage, code int, message string, data any) {
	if len(id) == 0 || string(id) == "null" {
		return
	}
	errorValue := map[string]any{"code": code, "message": message}
	if data != nil {
		errorValue["data"] = data
	}
	s.write(map[string]any{"jsonrpc": "2.0", "id": rawACPValue(id), "error": errorValue})
}

func rawACPValue(raw json.RawMessage) any {
	var value any
	_ = json.Unmarshal(raw, &value)
	return value
}

func decodeACPParams(raw json.RawMessage, target any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return errors.New("missing params")
	}
	return json.Unmarshal(raw, target)
}

func (s *acpServer) write(value any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeErr != nil {
		return s.writeErr
	}
	data, err := json.Marshal(value)
	if err == nil {
		_, err = fmt.Fprintf(s.output, "%s\n", data)
	}
	if err != nil {
		s.writeErr = err
		s.cancel()
	}
	return err
}

func (s *acpServer) close() error {
	s.stateMu.Lock()
	if !s.closed {
		s.closed = true
	}
	records := make([]*acpSession, 0, len(s.sessions))
	for _, record := range s.sessions {
		records = append(records, record)
	}
	pending := make([]acpClientRequest, 0, len(s.pending))
	for _, request := range s.pending {
		pending = append(pending, request)
	}
	s.sessions = map[string]*acpSession{}
	s.pending = map[string]acpClientRequest{}
	s.stateMu.Unlock()
	for _, request := range pending {
		s.engine.ResolveInteraction(request.interactionID, map[string]any{
			"ok": false, "error": map[string]any{"code": "unavailable", "message": "ACP connection closed"},
		})
	}
	for _, record := range records {
		record.mu.Lock()
		if record.inflight != nil {
			record.inflight.cancelled = true
			record.inflight.cancel()
		}
		record.mu.Unlock()
		_ = s.engine.CancelSession(record.id)
	}
	s.cancel()
	s.wg.Wait()
	failures := make([]error, 0, len(records)+1)
	for _, record := range records {
		if err := detachSDKSession(s.engine, record.id); err != nil {
			failures = append(failures, err)
		}
	}
	s.writeMu.Lock()
	writeErr := s.writeErr
	s.writeMu.Unlock()
	if writeErr != nil {
		failures = append(failures, writeErr)
	}
	return errors.Join(failures...)
}

func newACPSessionID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	bytes[6] = bytes[6]&0x0f | 0x40
	bytes[8] = bytes[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16]), nil
}
