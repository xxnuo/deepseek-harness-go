package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type acpTestCapture struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	frames []map[string]any
	wake   chan struct{}
}

func newACPTestCapture() *acpTestCapture {
	return &acpTestCapture{wake: make(chan struct{}, 1)}
}

func (c *acpTestCapture) Write(data []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	written := len(data)
	_, _ = c.buffer.Write(data)
	for {
		line := c.buffer.Bytes()
		newline := bytes.IndexByte(line, '\n')
		if newline < 0 {
			break
		}
		frameData := append([]byte(nil), line[:newline]...)
		c.buffer.Next(newline + 1)
		if len(bytes.TrimSpace(frameData)) == 0 {
			continue
		}
		var frame map[string]any
		if err := json.Unmarshal(frameData, &frame); err != nil {
			return 0, err
		}
		c.frames = append(c.frames, frame)
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
	return written, nil
}

func (c *acpTestCapture) snapshot() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.frames...)
}

type acpTestClient struct {
	input   *io.PipeWriter
	capture *acpTestCapture
}

func newACPTestClient(t *testing.T, engine *Engine) *acpTestClient {
	t.Helper()
	inputReader, inputWriter := io.Pipe()
	capture := newACPTestCapture()
	done := make(chan error, 1)
	go func() {
		done <- engine.ServeACP(context.Background(), inputReader, capture)
	}()
	t.Cleanup(func() {
		_ = inputWriter.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ServeACP: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("ServeACP did not stop after input closed")
		}
	})
	return &acpTestClient{input: inputWriter, capture: capture}
}

func (c *acpTestClient) send(t *testing.T, frame any) int {
	t.Helper()
	start := len(c.capture.snapshot())
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if _, err := c.input.Write(data); err != nil {
		t.Fatal(err)
	}
	return start
}

func (c *acpTestClient) waitFrame(t *testing.T, start int, match func(map[string]any) bool) (map[string]any, int) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		frames := c.capture.snapshot()
		for index := start; index < len(frames); index++ {
			if match(frames[index]) {
				return frames[index], index
			}
		}
		select {
		case <-c.capture.wake:
		case <-timer.C:
			t.Fatalf("ACP frame did not arrive: %#v", frames)
		}
	}
}

func (c *acpTestClient) request(t *testing.T, id, method string, params any) map[string]any {
	t.Helper()
	start := c.send(t, map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	frame, _ := c.waitFrame(t, start, func(frame map[string]any) bool { return frame["id"] == id })
	return frame
}

func acpTestResult(t *testing.T, frame map[string]any) map[string]any {
	t.Helper()
	if frame["error"] != nil {
		t.Fatalf("ACP response error: %#v", frame["error"])
	}
	result, ok := frame["result"].(map[string]any)
	if !ok {
		t.Fatalf("ACP result = %#v, want object", frame["result"])
	}
	return result
}

func acpTestNewSession(t *testing.T, client *acpTestClient, cwd string) string {
	t.Helper()
	result := acpTestResult(t, client.request(t, "new", "session/new", map[string]any{"cwd": cwd, "mcpServers": []any{}}))
	sessionID, ok := result["sessionId"].(string)
	if !ok || sessionID == "" {
		t.Fatalf("sessionId = %#v", result["sessionId"])
	}
	return sessionID
}

func TestACPRoundTrip(t *testing.T) {
	engine := newIntegrationEngine(t)
	client := newACPTestClient(t, engine)

	initialized := acpTestResult(t, client.request(t, "init", "initialize", map[string]any{
		"protocolVersion": ACPProtocolVersion, "clientCapabilities": map[string]any{},
	}))
	if initialized["protocolVersion"] != float64(ACPProtocolVersion) {
		t.Fatalf("protocolVersion = %#v", initialized["protocolVersion"])
	}
	agentInfo, _ := initialized["agentInfo"].(map[string]any)
	if agentInfo["name"] != "deepseek-harness-acp" || agentInfo["version"] != "0.0.1" {
		t.Fatalf("agentInfo = %#v", agentInfo)
	}
	capabilities, _ := initialized["agentCapabilities"].(map[string]any)
	promptCapabilities, _ := capabilities["promptCapabilities"].(map[string]any)
	if promptCapabilities["image"] != false || promptCapabilities["audio"] != false || promptCapabilities["embeddedContext"] != false {
		t.Fatalf("promptCapabilities = %#v", promptCapabilities)
	}
	if result := acpTestResult(t, client.request(t, "auth", "authenticate", map[string]any{"methodId": "unused"})); len(result) != 0 {
		t.Fatalf("authenticate result = %#v", result)
	}

	sessionID := acpTestNewSession(t, client, engine.Config().Workspace)
	start := client.send(t, map[string]any{
		"jsonrpc": "2.0", "id": "prompt", "method": "session/prompt",
		"params": map[string]any{"sessionId": sessionID, "prompt": []any{map[string]any{"type": "text", "text": "hello ACP"}}},
	})
	update, updateIndex := client.waitFrame(t, start, func(frame map[string]any) bool {
		return frame["method"] == "session/update"
	})
	response, responseIndex := client.waitFrame(t, start, func(frame map[string]any) bool { return frame["id"] == "prompt" })
	if updateIndex >= responseIndex {
		t.Fatalf("session/update index %d, prompt response index %d", updateIndex, responseIndex)
	}
	params, _ := update["params"].(map[string]any)
	updateValue, _ := params["update"].(map[string]any)
	content, _ := updateValue["content"].(map[string]any)
	if params["sessionId"] != sessionID || updateValue["sessionUpdate"] != "agent_message_chunk" || content["type"] != "text" || content["text"] != "hello ACP" {
		t.Fatalf("session/update = %#v", update)
	}
	if result := acpTestResult(t, response); result["stopReason"] != "end_turn" {
		t.Fatalf("prompt result = %#v", result)
	}
}

func TestACPForwardsOwnedExternalSessionOutput(t *testing.T) {
	engine := newIntegrationEngine(t)
	client := newACPTestClient(t, engine)
	acpTestResult(t, client.request(t, "init", "initialize", map[string]any{"protocolVersion": ACPProtocolVersion}))
	sessionID := acpTestNewSession(t, client, engine.Config().Workspace)

	start := len(client.capture.snapshot())
	done := make(chan error, 1)
	go func() {
		_, err := engine.Run(context.Background(), sessionID, PromptRequest{
			Content: []PromptContentPart{{Type: "text", Text: "external"}},
		})
		done <- err
	}()
	update, _ := client.waitFrame(t, start, func(frame map[string]any) bool {
		return frame["method"] == "session/update"
	})
	params, _ := update["params"].(map[string]any)
	updateValue, _ := params["update"].(map[string]any)
	content, _ := updateValue["content"].(map[string]any)
	if params["sessionId"] != sessionID || content["type"] != "text" || content["text"] != "external" {
		t.Fatalf("external session/update = %#v", update)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("external turn did not settle")
	}
}

func TestACPRejectsInvalidSessionAndPromptParameters(t *testing.T) {
	engine := newIntegrationEngine(t)
	client := newACPTestClient(t, engine)
	acpTestResult(t, client.request(t, "init", "initialize", map[string]any{"protocolVersion": ACPProtocolVersion}))

	invalidSessions := []struct {
		name    string
		params  map[string]any
		message string
	}{
		{name: "relative cwd", params: map[string]any{"cwd": "relative", "mcpServers": []any{}}, message: "absolute path"},
		{name: "MCP server", params: map[string]any{"cwd": engine.Config().Workspace, "mcpServers": []any{map[string]any{"name": "fs"}}}, message: "mcpServers"},
	}
	for index, test := range invalidSessions {
		t.Run(test.name, func(t *testing.T) {
			frame := client.request(t, "invalid-"+string(rune('0'+index)), "session/new", test.params)
			errorValue, _ := frame["error"].(map[string]any)
			if errorValue["code"] != float64(-32602) || !strings.Contains(errorValue["message"].(string), test.message) {
				t.Fatalf("session/new error = %#v", errorValue)
			}
		})
	}

	sessionID := acpTestNewSession(t, client, engine.Config().Workspace)
	frame := client.request(t, "empty", "session/prompt", map[string]any{
		"sessionId": sessionID, "prompt": []any{map[string]any{"type": "text", "text": "  "}},
	})
	errorValue, _ := frame["error"].(map[string]any)
	if errorValue["code"] != float64(-32602) || !strings.Contains(errorValue["message"].(string), "empty prompt") {
		t.Fatalf("empty prompt error = %#v", errorValue)
	}
	session, err := engine.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if event.Type == "turn/start" {
			t.Fatal("empty prompt started a turn")
		}
	}
}

type acpBlockingProvider struct {
	started    chan struct{}
	cancelled  chan struct{}
	once       sync.Once
	cancelOnce sync.Once
	modalities []string
}

func (p *acpBlockingProvider) ID() string   { return "acp-blocking" }
func (p *acpBlockingProvider) Name() string { return "ACP Blocking" }
func (p *acpBlockingProvider) Models(context.Context) ([]ModelInfo, error) {
	modalities := p.modalities
	if len(modalities) == 0 {
		modalities = []string{"text"}
	}
	return []ModelInfo{{ID: p.ID(), Name: p.Name(), InputModalities: modalities}}, nil
}
func (p *acpBlockingProvider) Complete(ctx context.Context, _ ChatRequest, _ func(Delta) error) (Completion, error) {
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	if p.cancelled != nil {
		p.cancelOnce.Do(func() { close(p.cancelled) })
	}
	return Completion{}, ctx.Err()
}

func newACPProviderEngine(t *testing.T, provider Provider, model string) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = provider.ID()
	cfg.Model = model
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = false
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	engine.RegisterProvider(provider)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func TestACPCancelPrompt(t *testing.T) {
	provider := &acpBlockingProvider{started: make(chan struct{})}
	engine := newACPProviderEngine(t, provider, provider.ID())
	client := newACPTestClient(t, engine)
	acpTestResult(t, client.request(t, "init", "initialize", map[string]any{"protocolVersion": ACPProtocolVersion}))
	sessionID := acpTestNewSession(t, client, engine.Config().Workspace)

	start := client.send(t, map[string]any{
		"jsonrpc": "2.0", "id": "prompt", "method": "session/prompt",
		"params": map[string]any{"sessionId": sessionID, "prompt": []any{map[string]any{"type": "text", "text": "wait"}}},
	})
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("provider did not start")
	}
	client.send(t, map[string]any{
		"jsonrpc": "2.0", "method": "session/cancel", "params": map[string]any{"sessionId": sessionID},
	})
	response, _ := client.waitFrame(t, start, func(frame map[string]any) bool { return frame["id"] == "prompt" })
	if result := acpTestResult(t, response); result["stopReason"] != "cancelled" {
		t.Fatalf("cancel result = %#v", result)
	}
}

func TestACPCancelQueuedPromptDoesNotCancelUnrelatedTurn(t *testing.T) {
	provider := &acpBlockingProvider{started: make(chan struct{}), cancelled: make(chan struct{})}
	engine := newACPProviderEngine(t, provider, provider.ID())
	client := newACPTestClient(t, engine)
	acpTestResult(t, client.request(t, "init", "initialize", map[string]any{"protocolVersion": ACPProtocolVersion}))
	sessionID := acpTestNewSession(t, client, engine.Config().Workspace)

	externalDone := make(chan error, 1)
	go func() {
		_, err := engine.Run(context.Background(), sessionID, PromptRequest{
			Content: []PromptContentPart{{Type: "text", Text: "unrelated"}},
		})
		externalDone <- err
	}()
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated provider turn did not start")
	}

	start := client.send(t, map[string]any{
		"jsonrpc": "2.0", "id": "queued", "method": "session/prompt",
		"params": map[string]any{"sessionId": sessionID, "prompt": []any{map[string]any{"type": "text", "text": "queued"}}},
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		session, err := engine.getSession(sessionID)
		if err != nil {
			t.Fatal(err)
		}
		session.mu.Lock()
		queued := len(session.pending) > 0
		session.mu.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ACP prompt was not queued behind unrelated work")
		}
		time.Sleep(time.Millisecond)
	}
	client.send(t, map[string]any{
		"jsonrpc": "2.0", "method": "session/cancel", "params": map[string]any{"sessionId": sessionID},
	})
	response, _ := client.waitFrame(t, start, func(frame map[string]any) bool { return frame["id"] == "queued" })
	if result := acpTestResult(t, response); result["stopReason"] != "cancelled" {
		t.Fatalf("queued cancel result = %#v", result)
	}
	select {
	case <-provider.cancelled:
		t.Fatal("queued ACP cancellation cancelled unrelated work")
	default:
	}

	if err := engine.CancelSession(sessionID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-externalDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unrelated turn cleanup = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated turn did not stop during cleanup")
	}
}

func TestACPRejectsInvalidImagesAsInvalidParams(t *testing.T) {
	provider := &acpBlockingProvider{started: make(chan struct{}), modalities: []string{"text", "image"}}
	engine := newACPProviderEngine(t, provider, provider.ID())
	client := newACPTestClient(t, engine)
	initialized := acpTestResult(t, client.request(t, "init", "initialize", map[string]any{"protocolVersion": ACPProtocolVersion}))
	capabilities := initialized["agentCapabilities"].(map[string]any)["promptCapabilities"].(map[string]any)
	if capabilities["image"] != true {
		t.Fatalf("image capability = %#v", capabilities["image"])
	}
	sessionID := acpTestNewSession(t, client, engine.Config().Workspace)

	for name, block := range map[string]map[string]any{
		"mime":       {"type": "image", "data": "AQ==", "mimeType": "image/tiff"},
		"base64":     {"type": "image", "data": "not base64", "mimeType": "image/png"},
		"image-data": {"type": "image", "data": "AQ==", "mimeType": "image/png"},
	} {
		t.Run(name, func(t *testing.T) {
			frame := client.request(t, name, "session/prompt", map[string]any{"sessionId": sessionID, "prompt": []any{block}})
			errorValue, _ := frame["error"].(map[string]any)
			if errorValue["code"] != float64(-32602) {
				t.Fatalf("image error = %#v", errorValue)
			}
		})
	}
	select {
	case <-provider.started:
		t.Fatal("invalid image reached the provider")
	default:
	}
}

func TestACPCancelDuringImageAdmissionDoesNotQueueLatePrompt(t *testing.T) {
	vision := &readImageProvider{id: "acp-cancel-vision", model: "vision", modalities: []string{"text", "image"}}
	engine := newACPProviderEngine(t, vision, vision.model)
	for index := 0; index < cap(engine.imageCompression); index++ {
		engine.imageCompression <- struct{}{}
	}
	defer func() {
		for len(engine.imageCompression) > 0 {
			<-engine.imageCompression
		}
	}()
	client := newACPTestClient(t, engine)
	acpTestResult(t, client.request(t, "init-admission-cancel", "initialize", map[string]any{"protocolVersion": ACPProtocolVersion}))
	sessionID := acpTestNewSession(t, client, engine.Config().Workspace)
	start := client.send(t, map[string]any{
		"jsonrpc": "2.0", "id": "prompt-admission-cancel", "method": "session/prompt",
		"params": map[string]any{"sessionId": sessionID, "prompt": []any{map[string]any{"type": "image", "mimeType": "image/png", "data": readImagePNG}}},
	})
	time.Sleep(20 * time.Millisecond)
	client.send(t, map[string]any{"jsonrpc": "2.0", "method": "session/cancel", "params": map[string]any{"sessionId": sessionID}})
	response, _ := client.waitFrame(t, start, func(frame map[string]any) bool { return frame["id"] == "prompt-admission-cancel" })
	result := acpTestResult(t, response)
	if result["stopReason"] != "cancelled" {
		t.Fatalf("admission cancellation response = %#v", response)
	}
	session, err := engine.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if event.Type == "user/message" {
			t.Fatalf("cancelled admission queued a late prompt: %#v", event)
		}
	}
}

type acpSwitchingImageProvider struct {
	*readImageProvider
	mu       sync.Mutex
	calls    int
	onSecond func()
}

func (p *acpSwitchingImageProvider) ResolveModelInfo(ctx context.Context, model string) (ModelInfo, error) {
	p.mu.Lock()
	p.calls++
	call := p.calls
	callback := p.onSecond
	p.mu.Unlock()
	if call == 2 && callback != nil {
		callback()
	}
	return p.readImageProvider.ResolveModelInfo(ctx, model)
}

func TestACPImagesUseExactDefaultAndLatestSessionRoutes(t *testing.T) {
	vision := &acpSwitchingImageProvider{readImageProvider: &readImageProvider{id: "acp-vision", model: "hidden-vision", modalities: []string{"text", "image"}, catalog: []ModelInfo{}}}
	textOnly := &readImageProvider{id: "acp-text", model: "text", modalities: []string{"text"}}
	engine := newACPProviderEngine(t, vision, vision.readImageProvider.model)
	engine.RegisterProvider(textOnly)
	client := newACPTestClient(t, engine)
	initialized := acpTestResult(t, client.request(t, "init-exact", "initialize", map[string]any{"protocolVersion": ACPProtocolVersion}))
	capabilities, _ := initialized["agentCapabilities"].(map[string]any)
	promptCapabilities, _ := capabilities["promptCapabilities"].(map[string]any)
	if promptCapabilities["image"] != true {
		t.Fatalf("exact visual route was not advertised: %#v", promptCapabilities)
	}
	sessionID := acpTestNewSession(t, client, engine.Config().Workspace)
	session, err := engine.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	vision.mu.Lock()
	vision.onSecond = func() {
		if _, appendErr := engine.appendEvent(session, "request/header", map[string]any{"header": map[string]any{"config": map[string]any{
			"provider": textOnly.ID(), "model": textOnly.model,
		}}, "reason": "change"}); appendErr != nil {
			t.Errorf("append changed request header: %v", appendErr)
		}
	}
	vision.mu.Unlock()
	response := client.request(t, "prompt-latest-route", "session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt":    []any{map[string]any{"type": "image", "mimeType": "image/png", "data": readImagePNG}},
	})
	failure, _ := response["error"].(map[string]any)
	if failure["code"] != float64(-32602) || !strings.Contains(fmt.Sprint(failure["message"]), "does not declare image input") {
		t.Fatalf("latest text route response = %#v", response)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if event.Type == "user/message" {
			t.Fatalf("route changed during admission but prompt was queued: %#v", event)
		}
	}
}

func TestACPForwardsPermissionRequest(t *testing.T) {
	engine := newIntegrationEngine(t)
	client := newACPTestClient(t, engine)
	acpTestResult(t, client.request(t, "init", "initialize", map[string]any{"protocolVersion": ACPProtocolVersion}))
	sessionID := acpTestNewSession(t, client, engine.Config().Workspace)

	deadline := time.Now().Add(2 * time.Second)
	for {
		engine.mu.RLock()
		subscribed := len(engine.muxSubs) > 0
		engine.mu.RUnlock()
		if subscribed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ACP mux subscription was not established")
		}
		time.Sleep(time.Millisecond)
	}

	type interactionOutcome struct {
		value any
		err   error
	}
	requestPermission := func(approvalID, callID string) (chan interactionOutcome, string) {
		t.Helper()
		outcome := make(chan interactionOutcome, 1)
		start := len(client.capture.snapshot())
		go func() {
			value, err := engine.RequestInteraction(context.Background(), sessionID, "approval/requested", map[string]any{
				"type": "approval/requested", "sessionId": sessionID, "approvalId": approvalID, "callId": callID,
			})
			outcome <- interactionOutcome{value: value, err: err}
		}()
		request, _ := client.waitFrame(t, start, func(frame map[string]any) bool {
			return frame["method"] == "session/request_permission"
		})
		requestID, ok := request["id"].(string)
		if !ok || requestID == "" {
			t.Fatalf("permission request id = %#v", request["id"])
		}
		params, _ := request["params"].(map[string]any)
		toolCall, _ := params["toolCall"].(map[string]any)
		options, _ := params["options"].([]any)
		if params["sessionId"] != sessionID || toolCall["toolCallId"] != callID || len(options) != 2 {
			t.Fatalf("permission request = %#v", request)
		}
		return outcome, requestID
	}

	outcome, requestID := requestPermission("approval-1", "call-1")
	client.send(t, map[string]any{
		"jsonrpc": "2.0", "id": requestID,
		"result": map[string]any{"outcome": map[string]any{"outcome": "selected", "optionId": "allow-once"}},
	})
	select {
	case result := <-outcome:
		if result.err != nil {
			t.Fatal(result.err)
		}
		value, _ := result.value.(map[string]any)
		if value["sessionId"] != sessionID || value["approvalId"] != "approval-1" || value["outcome"] != "allowed-once" {
			t.Fatalf("permission outcome = %#v", result.value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("permission response was not resolved")
	}

	outcome, requestID = requestPermission("approval-2", "call-2")
	client.send(t, map[string]any{
		"jsonrpc": "2.0", "id": requestID,
		"result": map[string]any{"outcome": map[string]any{"outcome": "cancelled"}},
	})
	select {
	case result := <-outcome:
		value, _ := result.value.(map[string]any)
		if result.err != nil || value["outcome"] != "cancelled" {
			t.Fatalf("cancelled permission outcome = %#v, %v", result.value, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled permission response was not resolved")
	}

	outcome, requestID = requestPermission("approval-3", "call-3")
	client.send(t, map[string]any{
		"jsonrpc": "2.0", "id": requestID,
		"error": map[string]any{"code": -32603, "message": "client gone"},
	})
	select {
	case result := <-outcome:
		if result.err == nil || !strings.Contains(result.err.Error(), "unavailable") {
			t.Fatalf("permission client error = %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("errored permission response was not resolved")
	}
}
