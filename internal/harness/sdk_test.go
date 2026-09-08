package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type sdkFrameCapture struct {
	mu     sync.Mutex
	frames []map[string]any
	wake   chan struct{}
}

type sdkReasoningCaptureProvider struct {
	promptCaptureProvider
}

type sdkBlockingModelsProvider struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*sdkBlockingModelsProvider) ID() string   { return "sdk-blocking-models" }
func (*sdkBlockingModelsProvider) Name() string { return "SDK Blocking Models" }
func (p *sdkBlockingModelsProvider) Models(ctx context.Context) ([]ModelInfo, error) {
	p.once.Do(func() { close(p.entered) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.release:
		return []ModelInfo{{ID: "model-a", Name: "A"}}, nil
	}
}
func (*sdkBlockingModelsProvider) Complete(context.Context, ChatRequest, func(Delta) error) (Completion, error) {
	return Completion{Text: "done", Finish: "stop"}, nil
}

type sdkSubagentRunProvider struct {
	name string
	mu   sync.Mutex
	runs []*SubagentRun
}

func (p *sdkSubagentRunProvider) Name() string { return p.name }
func (*sdkSubagentRunProvider) Capabilities() SubagentCapabilities {
	return NoSubagentStartCapabilities()
}
func (*sdkSubagentRunProvider) InheritsParentContext() bool { return false }
func (p *sdkSubagentRunProvider) Start(context.Context, SubagentStartRequest) (*SubagentRun, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.runs) == 0 {
		return nil, errors.New("no SDK subagent run queued")
	}
	run := p.runs[0]
	p.runs = p.runs[1:]
	return run, nil
}

func newSDKSubagentRun(id string, local bool) *SubagentRun {
	run := newSubagentRun(id, nil, nil)
	run.local = local
	return run
}

type sdkCreateGateStore struct {
	SessionStore
	mu      sync.Mutex
	creates int
	fail    int
	entered chan struct{}
	release chan struct{}
}

func (s *sdkCreateGateStore) Create(ctx context.Context, header SessionHeader, inheritedEventCount SessionLogOffset) (SessionHandle, error) {
	s.mu.Lock()
	s.creates++
	fail := s.fail > 0
	if fail {
		s.fail--
	}
	s.mu.Unlock()
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
	}
	if s.release != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.release:
		}
	}
	if fail {
		return nil, errors.New("creation failed")
	}
	return s.SessionStore.Create(ctx, header, inheritedEventCount)
}

func (s *sdkCreateGateStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.creates
}

func newSDKStoreEngine(t *testing.T, gate *sdkCreateGateStore) *Engine {
	t.Helper()
	store, err := NewJSONLSessionStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	gate.SessionStore = store
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.SessionStore = gate
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func (*sdkReasoningCaptureProvider) ID() string   { return "sdk-reasoning-capture" }
func (*sdkReasoningCaptureProvider) Name() string { return "SDK Reasoning Capture" }
func (*sdkReasoningCaptureProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{
		ID: "model-a", Name: "A",
		Reasoning: &ModelReasoningInfo{
			Efforts:       []ReasoningEffortInfo{{ID: "low", Name: "Low"}, {ID: "high", Name: "High"}},
			DefaultEffort: "low",
		},
	}}, nil
}

func newSDKFrameCapture() *sdkFrameCapture {
	return &sdkFrameCapture{wake: make(chan struct{}, 1)}
}

func (c *sdkFrameCapture) Write(data []byte) (int, error) {
	var frame map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(data), &frame); err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.frames = append(c.frames, frame)
	c.mu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return len(data), nil
}

func (c *sdkFrameCapture) sessionEventSeqs() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	seqs := make([]int, 0, len(c.frames))
	for _, frame := range c.frames {
		if frame["method"] != "session.event" {
			continue
		}
		params, _ := frame["params"].(map[string]any)
		event, _ := params["event"].(map[string]any)
		if seq, ok := event["seq"].(float64); ok {
			seqs = append(seqs, int(seq))
		}
	}
	return seqs
}

func (c *sdkFrameCapture) snapshot() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.frames...)
}

func decodeSDKFrames(t *testing.T, output string) []map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	frames := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var frame map[string]any
		if err := json.Unmarshal([]byte(line), &frame); err != nil {
			t.Fatalf("decode SDK frame %q: %v", line, err)
		}
		frames = append(frames, frame)
	}
	return frames
}

func sdkResponseByID(t *testing.T, frames []map[string]any, id any) map[string]any {
	t.Helper()
	want := fmt.Sprint(id)
	for _, frame := range frames {
		if fmt.Sprint(frame["id"]) == want {
			return frame
		}
	}
	t.Fatalf("SDK response %v missing from %#v", id, frames)
	return nil
}

func waitSDKFrames(t *testing.T, capture *sdkFrameCapture, ready func([]map[string]any) bool) []map[string]any {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		frames := capture.snapshot()
		if ready(frames) {
			return frames
		}
		select {
		case <-capture.wake:
		case <-timer.C:
			t.Fatalf("SDK frames did not reach expected state: %#v", frames)
		}
	}
}

func sdkRawParams(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestJSONRPCSDKRoundTrip(t *testing.T) {
	e := newIntegrationEngine(t)
	input, writer := io.Pipe()
	capture := newSDKFrameCapture()
	done := make(chan error, 1)
	go func() { done <- e.ServeJSONRPC(context.Background(), input, capture) }()
	write := func(line string) {
		t.Helper()
		if _, err := io.WriteString(writer, line+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"cwd":".","provider":"echo","model":"echo"}}`)
	frames := waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["id"] == float64(1) {
				return true
			}
		}
		return false
	})
	initialized := sdkResponseByID(t, frames, 1)
	result, _ := initialized["result"].(map[string]any)
	info, _ := result["serverInfo"].(map[string]any)
	if info["name"] != "deepseek-harness-sdk-runtime" {
		t.Fatalf("serverInfo = %#v", info)
	}
	write(`{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"sdk-session","contentBlocks":[{"type":"text","text":"SDK works"}]}}`)
	frames = waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		hasPrompt, hasReceipt := false, false
		for _, frame := range frames {
			hasPrompt = hasPrompt || frame["id"] == float64(2)
			if frame["method"] == "session.event" {
				params, _ := frame["params"].(map[string]any)
				event, _ := params["event"].(map[string]any)
				hasReceipt = hasReceipt || event["type"] == "agent/inbox/spliced"
			}
		}
		return hasPrompt && hasReceipt
	})
	var prompt map[string]any
	for _, frame := range frames {
		if frame["id"] == float64(2) {
			prompt = frame
		}
	}
	if prompt == nil {
		t.Fatalf("prompt receipt missing: %#v", frames)
	}
	result, ok := prompt["result"].(map[string]any)
	if !ok {
		t.Fatalf("prompt result = %#v", prompt["result"])
	}
	if len(result) != 1 {
		t.Fatalf("prompt result keys = %#v, want only messageId", result)
	}
	messageID, ok := result["messageId"].(string)
	if !ok || messageID == "" {
		t.Fatalf("prompt messageId = %#v", result["messageId"])
	}
	receipt := false
	for _, frame := range frames {
		if frame["method"] != "session.event" {
			continue
		}
		params, _ := frame["params"].(map[string]any)
		event, _ := params["event"].(map[string]any)
		if event["type"] != "agent/inbox/spliced" {
			continue
		}
		data, _ := event["data"].(map[string]any)
		inserted, _ := data["inserted"].([]any)
		for _, raw := range inserted {
			message, _ := raw.(map[string]any)
			if message["id"] == messageID {
				receipt = true
			}
		}
	}
	if !receipt {
		t.Fatalf("no durable inbox receipt for %q in %#v", messageID, frames)
	}
	write(`{"jsonrpc":"2.0","id":3,"method":"shutdown"}`)
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["id"] == float64(3) {
				return true
			}
		}
		return false
	})
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("ServeJSONRPC: %v", err)
	}
}

func TestJSONRPCSDKDropsNotificationsAndInvalidRequestIDs(t *testing.T) {
	e := newIntegrationEngine(t)
	input, writer := io.Pipe()
	capture := newSDKFrameCapture()
	done := make(chan error, 1)
	go func() { done <- e.ServeJSONRPC(context.Background(), input, capture) }()
	write := func(line string) {
		t.Helper()
		if _, err := io.WriteString(writer, line+"\n"); err != nil {
			t.Fatal(err)
		}
	}

	// Establish the request path before sending frames whose ids must be
	// ignored; this also makes the later probe a deterministic ordering barrier.
	write(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"cwd":".","provider":"echo","model":"echo"}}`)
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["id"] == float64(1) {
				return true
			}
		}
		return false
	})

	prompt := `{"sessionId":"sdk-notification","contentBlocks":[{"type":"text","text":"must be ignored"}]}`
	for _, line := range []string{
		`{"jsonrpc":"2.0","method":"session/prompt","params":` + prompt + `}`,
		`{"jsonrpc":"2.0","id":null,"method":"session/prompt","params":` + prompt + `}`,
		`{"jsonrpc":"2.0","id":true,"method":"session/prompt","params":` + prompt + `}`,
		`{"jsonrpc":"2.0","id":{},"method":"session/prompt","params":` + prompt + `}`,
		`{"jsonrpc":"2.0","id":[],"method":"session/prompt","params":` + prompt + `}`,
		`{"jsonrpc":"2.0","method":"shutdown"}`,
		`{"jsonrpc":"2.0","id":null,"method":"shutdown"}`,
	} {
		write(line)
	}

	// A valid string-id request proves that the notification shutdown frames
	// above did not close the server and provides an ordering barrier for all
	// preceding ignored frames.
	write(`{"jsonrpc":"2.0","id":"probe","method":"unknown"}`)
	frames := waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["id"] == "probe" {
				return true
			}
		}
		return false
	})
	if _, err := e.getSession("sdk-notification"); err == nil {
		t.Fatalf("notification session/prompt was dispatched; frames=%#v", frames)
	}

	write(`{"jsonrpc":"2.0","id":"shutdown","method":"shutdown"}`)
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["id"] == "shutdown" {
				return true
			}
		}
		return false
	})
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("ServeJSONRPC: %v", err)
	}
}

func TestJSONRPCSDKDispatchesRequestsConcurrently(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &sdkBlockingModelsProvider{entered: make(chan struct{}), release: make(chan struct{})}
	e.RegisterProvider(provider)
	input, writer := io.Pipe()
	capture := newSDKFrameCapture()
	done := make(chan error, 1)
	go func() { done <- e.ServeJSONRPC(context.Background(), input, capture) }()

	if _, err := fmt.Fprintf(writer, "{\"jsonrpc\":\"2.0\",\"id\":\"init\",\"method\":\"initialize\",\"params\":{\"cwd\":%q,\"provider\":%q,\"model\":\"model-a\"}}\n", e.Config().Workspace, provider.ID()); err != nil {
		t.Fatal(err)
	}
	<-provider.entered
	if _, err := io.WriteString(writer, "{\"jsonrpc\":\"2.0\",\"id\":\"probe\",\"method\":\"unknown\"}\n"); err != nil {
		t.Fatal(err)
	}
	frames := waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["id"] == "probe" {
				return true
			}
		}
		return false
	})
	probe := sdkResponseByID(t, frames, "probe")
	errorValue, _ := probe["error"].(map[string]any)
	if errorValue["code"] != float64(-32601) {
		t.Fatalf("probe response = %#v", probe)
	}
	for _, frame := range frames {
		if frame["id"] == "init" {
			t.Fatalf("initialize completed before its provider was released: %#v", frames)
		}
	}

	close(provider.release)
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["id"] == "init" {
				return true
			}
		}
		return false
	})
	if _, err := io.WriteString(writer, "{\"jsonrpc\":\"2.0\",\"id\":\"shutdown\",\"method\":\"shutdown\"}\n"); err != nil {
		t.Fatal(err)
	}
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["id"] == "shutdown" {
				return true
			}
		}
		return false
	})
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestJSONRPCSDKConcurrentShutdownRepliesAndClosesOnce(t *testing.T) {
	e := newIntegrationEngine(t)
	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, capture)
	server.stateMu.Lock()
	server.cwd, server.provider, server.model = e.Config().Workspace, "echo", "echo"
	server.initialized = true
	server.stateMu.Unlock()
	if err := server.ensureSession("sdk-double-shutdown"); err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession("sdk-double-shutdown")
	session.mu.Lock()
	before := session.attachmentGeneration
	session.mu.Unlock()

	var handlers sync.WaitGroup
	for _, id := range []string{"shutdown-1", "shutdown-2"} {
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			if err := server.handle(jsonRPCRequest{ID: id, Method: "shutdown"}); err != nil {
				t.Errorf("shutdown %s: %v", id, err)
			}
		}()
	}
	handlers.Wait()
	frames := capture.snapshot()
	for _, id := range []string{"shutdown-1", "shutdown-2"} {
		response := sdkResponseByID(t, frames, id)
		if response["error"] != nil {
			t.Fatalf("shutdown %s response = %#v", id, response)
		}
	}
	session.mu.Lock()
	after, attached := session.attachmentGeneration, session.attached
	session.mu.Unlock()
	if attached || after != before+1 {
		t.Fatalf("shutdown detach state = attached %v generation %d -> %d", attached, before, after)
	}
}

func TestJSONRPCSDKPipeConcurrentShutdownRepliesWithoutEOF(t *testing.T) {
	e := newIntegrationEngine(t)
	input, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	capture := newSDKFrameCapture()
	done := make(chan error, 1)
	go func() { done <- e.ServeJSONRPC(context.Background(), input, capture) }()

	if _, err := fmt.Fprintf(writer, "{\"jsonrpc\":\"2.0\",\"id\":\"init\",\"method\":\"initialize\",\"params\":{\"cwd\":%q,\"provider\":\"echo\",\"model\":\"echo\"}}\n", e.Config().Workspace); err != nil {
		t.Fatal(err)
	}
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		return sdkFrameHasID(frames, "init")
	})
	if _, err := io.WriteString(writer, "{\"jsonrpc\":\"2.0\",\"id\":\"prompt\",\"method\":\"session/prompt\",\"params\":{\"sessionId\":\"sdk-pipe-shutdown\",\"contentBlocks\":[{\"type\":\"text\",\"text\":\"work\"}]}}\n"); err != nil {
		t.Fatal(err)
	}
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		return sdkFrameHasID(frames, "prompt")
	})
	session, err := e.getSession("sdk-pipe-shutdown")
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	before := session.attachmentGeneration
	session.mu.Unlock()

	if _, err := io.WriteString(writer, strings.Join([]string{
		`{"jsonrpc":"2.0","id":"shutdown-1","method":"shutdown"}`,
		`{"jsonrpc":"2.0","id":"shutdown-2","method":"shutdown"}`,
	}, "\n")+"\n"); err != nil {
		t.Fatal(err)
	}
	frames := waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		return sdkFrameHasID(frames, "shutdown-1") && sdkFrameHasID(frames, "shutdown-2")
	})
	for _, id := range []string{"shutdown-1", "shutdown-2"} {
		if response := sdkResponseByID(t, frames, id); response["error"] != nil {
			t.Fatalf("%s response = %#v", id, response)
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeJSONRPC waited for stdin EOF after protocol shutdown")
	}
	session.mu.Lock()
	after, attached := session.attachmentGeneration, session.attached
	session.mu.Unlock()
	if attached || after != before+1 {
		t.Fatalf("shutdown detach state = attached %v generation %d -> %d", attached, before, after)
	}
}

func TestJSONRPCSDKContextCancellationReturnsWithoutEOF(t *testing.T) {
	e := newIntegrationEngine(t)
	input, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.ServeJSONRPC(ctx, input, io.Discard) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ServeJSONRPC cancellation = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeJSONRPC ignored context cancellation while stdin remained open")
	}
}

func TestJSONRPCSDKInitializeValidatesExactModelMaxTokens(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &orderedSubagentModelProvider{
		id: "sdk-invalid-max-token-model", name: "Invalid Max Token Model",
		models: []ModelInfo{{ID: "model", Name: "Model", MaxTokens: -1}},
	}
	e.RegisterProvider(provider)
	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, capture)
	defer func() { _ = server.close() }()
	if err := server.handle(jsonRPCRequest{ID: "initialize", Method: "initialize", Params: sdkRawParams(t, map[string]any{
		"cwd": e.Config().Workspace, "provider": provider.ID(), "model": "model", "maxTokens": 8192,
	})}); err != nil {
		t.Fatal(err)
	}
	response := sdkResponseByID(t, capture.snapshot(), "initialize")
	errorValue, _ := response["error"].(map[string]any)
	if !strings.Contains(fmt.Sprint(errorValue["message"]), "invalid default maxTokens") {
		t.Fatalf("initialize response = %#v", response)
	}
}

func sdkFrameHasID(frames []map[string]any, id any) bool {
	want := fmt.Sprint(id)
	for _, frame := range frames {
		if fmt.Sprint(frame["id"]) == want {
			return true
		}
	}
	return false
}

func TestStandaloneSDKBinaryJSONRPCRoundTrip(t *testing.T) {
	repository := moduleRoot(t)
	binary := filepath.Join(t.TempDir(), "dsh-sdk")
	build := runtimeAssetGoCommand(moduleRoot(t), "build", "-o", binary, "./cmd/dsh-sdk")
	build.Dir = repository
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build dsh-sdk: %v\n%s", err, output)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Dir = t.TempDir()
	command.Env = append(os.Environ(), "DSH_HOME="+t.TempDir())
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start dsh-sdk: %v", err)
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	frames := make([]map[string]any, 0)
	readUntil := func(ready func([]map[string]any) bool) {
		t.Helper()
		for !ready(frames) {
			if !scanner.Scan() {
				t.Fatalf("dsh-sdk output ended before expected frame: %v\nframes:\n%#v\nstderr:\n%s", scanner.Err(), frames, stderr.String())
			}
			line := scanner.Text()
			var frame map[string]any
			if err := json.Unmarshal([]byte(line), &frame); err != nil {
				t.Fatalf("decode dsh-sdk frame %q: %v", line, err)
			}
			frames = append(frames, frame)
		}
	}
	write := func(line string) {
		t.Helper()
		if _, err := io.WriteString(stdin, line+"\n"); err != nil {
			t.Fatalf("write dsh-sdk request: %v", err)
		}
	}

	write(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"cwd":".","provider":"echo","model":"echo"}}`)
	readUntil(func(frames []map[string]any) bool { return sdkFrameHasID(frames, 1) })
	if response := sdkResponseByID(t, frames, 1); response["error"] != nil {
		t.Fatalf("initialize response = %#v", response)
	}

	write(`{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"sdk-process-session","contentBlocks":[{"type":"text","text":"SDK process works"}]}}`)
	readUntil(func(frames []map[string]any) bool {
		if !sdkFrameHasID(frames, 2) {
			return false
		}
		for _, frame := range frames {
			if frame["method"] != "session.event" {
				continue
			}
			params, _ := frame["params"].(map[string]any)
			event, _ := params["event"].(map[string]any)
			if event["type"] == "agent/inbox/spliced" {
				return true
			}
		}
		return false
	})
	if response := sdkResponseByID(t, frames, 2); response["error"] != nil {
		t.Fatalf("session/prompt response = %#v", response)
	}

	write(`{"jsonrpc":"2.0","id":3,"method":"shutdown"}`)
	readUntil(func(frames []map[string]any) bool { return sdkFrameHasID(frames, 3) })
	if response := sdkResponseByID(t, frames, 3); response["error"] != nil {
		t.Fatalf("shutdown response = %#v", response)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	for scanner.Scan() {
		line := scanner.Text()
		var frame map[string]any
		if err := json.Unmarshal([]byte(line), &frame); err != nil {
			t.Fatalf("decode dsh-sdk frame %q: %v", line, err)
		}
		frames = append(frames, frame)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read dsh-sdk output: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("run dsh-sdk: %v\nframes:\n%#v\nstderr:\n%s", err, frames, stderr.String())
	}
}

func TestJSONRPCSDKRejectsInvalidMaxTokens(t *testing.T) {
	for _, value := range []string{"0", "-1", "1.5", "9007199254740992"} {
		t.Run(value, func(t *testing.T) {
			e := newIntegrationEngine(t)
			input := strings.NewReader(fmt.Sprintf("%s\n%s\n", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"cwd":".","provider":"echo","model":"echo","maxTokens":`+value+`}}`, `{"jsonrpc":"2.0","id":2,"method":"shutdown"}`))
			var output bytes.Buffer
			if err := e.ServeJSONRPC(context.Background(), input, &output); err != nil {
				t.Fatalf("ServeJSONRPC: %v", err)
			}
			lines := strings.Split(strings.TrimSpace(output.String()), "\n")
			if len(lines) < 2 {
				t.Fatalf("responses = %q", output.String())
			}
			response := sdkResponseByID(t, decodeSDKFrames(t, output.String()), 1)
			if response["error"] == nil {
				t.Fatalf("initialize response = %#v, want error", response)
			}
			errorValue, _ := response["error"].(map[string]any)
			if errorValue["message"] != "initialize maxTokens must be a positive safe integer" {
				t.Fatalf("initialize error = %#v", errorValue)
			}
		})
	}
}

func TestJSONRPCSDKRejectsEmptyReasoningEffortWithoutMaxTokens(t *testing.T) {
	e := newIntegrationEngine(t)
	input := strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"cwd":".","provider":"echo","model":"echo","reasoningEffort":""}}`,
		`{"jsonrpc":"2.0","id":2,"method":"shutdown"}`,
	}, "\n") + "\n")
	var output bytes.Buffer
	if err := e.ServeJSONRPC(context.Background(), input, &output); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("responses = %q", output.String())
	}
	response := sdkResponseByID(t, decodeSDKFrames(t, output.String()), 1)
	errorValue, _ := response["error"].(map[string]any)
	if errorValue["message"] != "initialize reasoningEffort must be a non-empty string" {
		t.Fatalf("initialize response = %#v", response)
	}
}

func TestJSONRPCSDKRejectsNullAndWrongInitializeOptionTypes(t *testing.T) {
	tests := []struct {
		name, field, value, message string
	}{
		{name: "reasoning-null", field: "reasoningEffort", value: "null", message: "initialize reasoningEffort must be a non-empty string"},
		{name: "reasoning-number", field: "reasoningEffort", value: "42", message: "initialize reasoningEffort must be a non-empty string"},
		{name: "max-tokens-null", field: "maxTokens", value: "null", message: "initialize maxTokens must be a positive safe integer"},
		{name: "max-tokens-string", field: "maxTokens", value: `"42"`, message: "initialize maxTokens must be a positive safe integer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := newIntegrationEngine(t)
			input := strings.NewReader(fmt.Sprintf("%s\n%s\n",
				`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"cwd":".","provider":"echo","model":"echo","`+test.field+`":`+test.value+`}}`,
				`{"jsonrpc":"2.0","id":2,"method":"shutdown"}`,
			))
			var output bytes.Buffer
			if err := e.ServeJSONRPC(context.Background(), input, &output); err != nil {
				t.Fatal(err)
			}
			response := sdkResponseByID(t, decodeSDKFrames(t, output.String()), 1)
			errorValue, _ := response["error"].(map[string]any)
			if errorValue["message"] != test.message {
				t.Fatalf("initialize response = %#v", response)
			}
		})
	}
}

func TestJSONRPCSDKCoalescesConcurrentSessionCreationAndRetriesFailure(t *testing.T) {
	t.Run("coalesces", func(t *testing.T) {
		gate := &sdkCreateGateStore{entered: make(chan struct{}, 1), release: make(chan struct{})}
		e := newSDKStoreEngine(t, gate)
		ctx, cancel := context.WithCancel(context.Background())
		server := newSDKServer(e, ctx, cancel, newSDKFrameCapture())
		defer func() { _ = server.close() }()
		server.cwd, server.provider, server.model = e.Config().Workspace, "echo", "echo"

		results := make(chan error, 2)
		go func() { results <- server.ensureSession("sdk-shared-create") }()
		<-gate.entered
		go func() { results <- server.ensureSession("sdk-shared-create") }()
		close(gate.release)
		for range 2 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if gate.count() != 1 {
			t.Fatalf("session creates = %d, want 1", gate.count())
		}
	})

	t.Run("retries", func(t *testing.T) {
		gate := &sdkCreateGateStore{fail: 1}
		e := newSDKStoreEngine(t, gate)
		ctx, cancel := context.WithCancel(context.Background())
		server := newSDKServer(e, ctx, cancel, newSDKFrameCapture())
		defer func() { _ = server.close() }()
		server.cwd, server.provider, server.model = e.Config().Workspace, "echo", "echo"

		if err := server.ensureSession("sdk-retry-create"); err == nil || !strings.Contains(err.Error(), "creation failed") {
			t.Fatalf("first creation error = %v", err)
		}
		if err := server.ensureSession("sdk-retry-create"); err != nil {
			t.Fatal(err)
		}
		if gate.count() != 2 {
			t.Fatalf("session creates = %d, want 2", gate.count())
		}
	})
}

func TestJSONRPCSDKShutdownWaitsForInflightCreationAndLeavesNoAttachedSession(t *testing.T) {
	gate := &sdkCreateGateStore{entered: make(chan struct{}, 1), release: make(chan struct{})}
	e := newSDKStoreEngine(t, gate)
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, newSDKFrameCapture())
	server.cwd, server.provider, server.model = e.Config().Workspace, "echo", "echo"

	creation := make(chan error, 1)
	go func() { creation <- server.ensureSession("sdk-close-race") }()
	<-gate.entered
	closed := make(chan error, 1)
	go func() { closed <- server.close() }()
	select {
	case err := <-closed:
		t.Fatalf("close returned before creation settled: %v", err)
	default:
	}
	deadline := time.Now().Add(time.Second)
	for {
		server.stateMu.Lock()
		closing := server.closing
		server.stateMu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("close did not enter closing state")
		}
		time.Sleep(time.Millisecond)
	}
	close(gate.release)
	if err := <-creation; err == nil || err.Error() != "SDK server is shutting down" {
		t.Fatalf("creation error = %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession("sdk-close-race")
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	attached := session.attached
	session.mu.Unlock()
	if attached {
		t.Fatal("in-flight SDK session remained attached after shutdown")
	}
}

func TestJSONRPCSDKRejectsExternallyReattachedOwnedSession(t *testing.T) {
	e := newIntegrationEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, newSDKFrameCapture())
	server.cwd, server.provider, server.model = e.Config().Workspace, "echo", "echo"
	if err := server.ensureSession("sdk-replaced"); err != nil {
		t.Fatal(err)
	}
	if err := detachSDKSession(e, "sdk-replaced"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CreateSession(context.Background(), e.Config().Workspace, "sdk-replaced", ""); err != nil {
		t.Fatal(err)
	}
	if err := server.ensureSession("sdk-replaced"); err == nil || err.Error() != "session agent was disposed outside the server: sdk-replaced" {
		t.Fatalf("replacement error = %v", err)
	}
	if err := server.close(); err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession("sdk-replaced")
	session.mu.Lock()
	attached := session.attached
	session.mu.Unlock()
	if !attached {
		t.Fatal("SDK shutdown detached the externally reattached replacement")
	}
}

func TestJSONRPCSDKAdmitsInlineImageBeforeDurablePrompt(t *testing.T) {
	e := newIntegrationEngine(t)
	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, capture)
	defer func() { _ = server.close() }()
	if err := server.handle(jsonRPCRequest{ID: 1, Method: "initialize", Params: sdkRawParams(t, map[string]any{
		"cwd": e.Config().Workspace, "provider": "echo", "model": "echo",
	})}); err != nil {
		t.Fatal(err)
	}
	if err := server.handle(jsonRPCRequest{ID: 2, Method: "session/prompt", Params: sdkRawParams(t, map[string]any{
		"sessionId": "sdk-inline-image",
		"contentBlocks": []map[string]any{
			{"type": "text", "text": "inspect"},
			{"type": "image", "data": readImagePNG, "mimeType": "image/png"},
		},
	})}); err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := e.WaitForIdle(waitCtx, "sdk-inline-image"); err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession("sdk-inline-image")
	session.mu.Lock()
	encoded, err := json.Marshal(session.Events)
	session.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(readImagePNG)) {
		t.Fatal("inline image bytes were persisted in the session log")
	}
	refs := map[string]ImageAttachmentRef{}
	collectExportImageRefs(encoded, refs)
	if len(refs) != 1 {
		t.Fatalf("durable image refs = %#v", refs)
	}
	for _, ref := range refs {
		if data, err := e.readImage(ref); err != nil || len(data) == 0 {
			t.Fatalf("stored image = %d bytes, %v", len(data), err)
		}
	}
}

func TestJSONRPCSDKInitializeRejectsUnknownProvider(t *testing.T) {
	e := newIntegrationEngine(t)
	input := strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"cwd":".","provider":"missing","model":"model"}}`,
		`{"jsonrpc":"2.0","id":2,"method":"shutdown"}`,
	}, "\n") + "\n")
	var output bytes.Buffer
	if err := e.ServeJSONRPC(context.Background(), input, &output); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("responses = %q", output.String())
	}
	response := sdkResponseByID(t, decodeSDKFrames(t, output.String()), 1)
	errorValue, _ := response["error"].(map[string]any)
	if errorValue["message"] != `no adapter registered for provider "missing"` || response["result"] != nil {
		t.Fatalf("initialize response = %#v", response)
	}
}

func TestJSONRPCSDKAppliesInitializeMaxTokens(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &promptCaptureProvider{}
	e.RegisterProvider(provider)
	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, capture)
	defer func() { _ = server.close() }()
	if err := server.handle(jsonRPCRequest{ID: 1, Method: "initialize", Params: sdkRawParams(t, map[string]any{
		"cwd": e.Config().Workspace, "provider": provider.ID(), "model": "model-a", "maxTokens": 321,
	})}); err != nil {
		t.Fatal(err)
	}
	if err := server.handle(jsonRPCRequest{ID: 2, Method: "session/prompt", Params: sdkRawParams(t, map[string]any{
		"sessionId": "sdk-max-tokens", "contentBlocks": []map[string]any{{"type": "text", "text": "root"}},
	})}); err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := e.WaitForIdle(waitCtx, "sdk-max-tokens"); err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSubagent(context.Background(), "sdk-max-tokens", "sdk-max-tokens-child", "")
	if err != nil {
		t.Fatal(err)
	}
	server.reconcileSubagents()
	if _, err := e.Run(context.Background(), child, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "child"}}}); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 || requests[0].MaxTokens != 321 || requests[1].MaxTokens != 321 {
		t.Fatalf("provider max tokens = %#v, want [321 321]", requests)
	}
}

func TestJSONRPCSDKAppliesInitializeReasoningEffort(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &sdkReasoningCaptureProvider{}
	e.RegisterProvider(provider)
	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, capture)
	defer func() { _ = server.close() }()
	if err := server.handle(jsonRPCRequest{ID: 1, Method: "initialize", Params: sdkRawParams(t, map[string]any{
		"cwd": e.Config().Workspace, "provider": provider.ID(), "model": "model-a", "reasoningEffort": "high",
	})}); err != nil {
		t.Fatal(err)
	}
	if err := server.handle(jsonRPCRequest{ID: 2, Method: "session/prompt", Params: sdkRawParams(t, map[string]any{
		"sessionId": "sdk-reasoning", "contentBlocks": []map[string]any{{"type": "text", "text": "root"}},
	})}); err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := e.WaitForIdle(waitCtx, "sdk-reasoning"); err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSubagent(context.Background(), "sdk-reasoning", "sdk-reasoning-child", "")
	if err != nil {
		t.Fatal(err)
	}
	server.reconcileSubagents()
	if _, err := e.Run(context.Background(), child, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "child"}}}); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 || requests[0].ReasoningEffort != "high" || requests[1].ReasoningEffort != "high" {
		t.Fatalf("provider reasoning efforts = %#v, want [high high]", requests)
	}
}

func TestJSONRPCSDKShutdownReleasesOnlyOwnedSessions(t *testing.T) {
	e := newIntegrationEngine(t)
	external, err := e.CreateSession(context.Background(), e.Config().Workspace, "sdk-external", "")
	if err != nil {
		t.Fatal(err)
	}
	cold, err := e.CreateSession(context.Background(), e.Config().Workspace, "sdk-cold", "")
	if err != nil {
		t.Fatal(err)
	}
	coldSession, _ := e.getSession(cold)
	coldSession.mu.Lock()
	coldSession.attached = false
	coldSession.mu.Unlock()

	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, capture)
	if err := server.handle(jsonRPCRequest{ID: 1, Method: "initialize", Params: sdkRawParams(t, map[string]any{
		"cwd": e.Config().Workspace, "provider": "echo", "model": "echo",
	})}); err != nil {
		t.Fatal(err)
	}
	if err := server.ensureSession("sdk-owned"); err != nil {
		t.Fatal(err)
	}
	if err := server.ensureSession(cold); err != nil {
		t.Fatal(err)
	}
	if err := server.ensureSession(external); err == nil {
		t.Fatal("attached external session was accepted by SDK server")
	}
	child, err := e.CreateSubagent(context.Background(), "sdk-owned", "sdk-owned-child", "")
	if err != nil {
		t.Fatal(err)
	}
	server.reconcileSubagents()
	if err := server.close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"sdk-owned", cold, child} {
		session, _ := e.getSession(id)
		session.mu.Lock()
		attached := session.attached
		session.mu.Unlock()
		if attached {
			t.Fatalf("owned session %q remained attached after shutdown", id)
		}
	}
	externalSession, _ := e.getSession(external)
	externalSession.mu.Lock()
	externalAttached := externalSession.attached
	externalSession.mu.Unlock()
	if !externalAttached {
		t.Fatal("external session was detached by SDK shutdown")
	}
}

func TestSDKOwnedSessionLineageIsOrderIndependent(t *testing.T) {
	owned := map[string]struct{}{"root": {}}
	inheritSDKOwnedSessions(owned, []sdkSessionLineage{
		{id: "grandchild", parent: "child"},
		{id: "external-child", parent: "external"},
		{id: "child", parent: "root"},
	})
	for _, id := range []string{"root", "child", "grandchild"} {
		if _, ok := owned[id]; !ok {
			t.Fatalf("owned lineage is missing %q: %#v", id, owned)
		}
	}
	if _, ok := owned["external-child"]; ok {
		t.Fatalf("external lineage became SDK-owned: %#v", owned)
	}
}

func TestJSONRPCSDKSubagentLifecycleNotifications(t *testing.T) {
	e := newIntegrationEngine(t)
	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, capture)
	defer func() { _ = server.close() }()
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "sdk-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	localFirst := newSDKSubagentRun("sdk-child", true)
	remote := newSDKSubagentRun("sdk-child", false)
	localSecond := newSDKSubagentRun("sdk-child", true)
	provider := &sdkSubagentRunProvider{name: "sdk-provider", runs: []*SubagentRun{localFirst, remote, localSecond}}
	if err := e.RegisterSubagentProvider(provider); err != nil {
		t.Fatal(err)
	}
	first, err := e.StartSubagent(t.Context(), provider.Name(), SubagentStartRequest{ParentSessionID: parent})
	if err != nil {
		t.Fatal(err)
	}
	remoteRun, err := e.StartSubagent(t.Context(), provider.Name(), SubagentStartRequest{ParentSessionID: parent})
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.StartSubagent(t.Context(), provider.Name(), SubagentStartRequest{ParentSessionID: parent})
	if err != nil {
		t.Fatal(err)
	}
	localFirst.settle(SubagentResult{Output: []ContentBlock{{Type: "text", Text: "first"}}, StopReason: SubagentCompleted})
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["method"] == "subagent.finished" {
				params, _ := frame["params"].(map[string]any)
				return params["lastAssistantMessage"] != nil
			}
		}
		return false
	})
	remote.settle(SubagentResult{Output: []ContentBlock{{Type: "text", Text: "remote"}}, StopReason: SubagentCompleted})
	localSecond.settle(SubagentResult{Output: []ContentBlock{{Type: "text", Text: "second"}}, StopReason: SubagentMaxTokens})
	for _, run := range []*SubagentRun{first, remoteRun, second} {
		if _, err := run.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	frames := waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		count := 0
		for _, frame := range frames {
			if frame["method"] == "subagent.finished" {
				params, _ := frame["params"].(map[string]any)
				if params["childSessionId"] == "sdk-child" {
					count++
				}
			}
		}
		return count == 2
	})
	var finished []map[string]any
	for _, frame := range frames {
		if frame["method"] != "subagent.finished" {
			continue
		}
		params, _ := frame["params"].(map[string]any)
		if params["childSessionId"] == "sdk-child" {
			finished = append(finished, params)
		}
	}
	for index, want := range []struct {
		text, status, reason string
	}{{"first", "ok", "completed"}, {"second", "error", "max-tokens"}} {
		params := finished[index]
		if params["provider"] != provider.Name() || params["agentId"] != "sdk-child" || params["status"] != want.status || params["stopReason"] != want.reason {
			t.Fatalf("finished[%d] = %#v", index, params)
		}
		content, _ := params["lastAssistantMessage"].([]any)
		block, _ := content[0].(map[string]any)
		if block["text"] != want.text {
			t.Fatalf("finished[%d] output = %#v, want %q", index, content, want)
		}
	}
}

func TestJSONRPCSDKMaxTokensAsSuccess(t *testing.T) {
	e := newIntegrationEngine(t)
	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServerWithOptions(e, ctx, cancel, capture, SDKServerOptions{MaxTokensAsSuccess: true})
	defer func() { _ = server.close() }()
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "sdk-max-success-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	run := newSDKSubagentRun("sdk-max-success-child", true)
	provider := &sdkSubagentRunProvider{name: "sdk-max-success-provider", runs: []*SubagentRun{run}}
	if err := e.RegisterSubagentProvider(provider); err != nil {
		t.Fatal(err)
	}
	started, err := e.StartSubagent(t.Context(), provider.Name(), SubagentStartRequest{ParentSessionID: parent})
	if err != nil {
		t.Fatal(err)
	}
	run.settle(SubagentResult{StopReason: SubagentMaxTokens})
	if _, err := started.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	frames := waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["method"] == "subagent.finished" {
				return true
			}
		}
		return false
	})
	for _, frame := range frames {
		if frame["method"] != "subagent.finished" {
			continue
		}
		params, _ := frame["params"].(map[string]any)
		if params["status"] != "ok" || params["stopReason"] != "max-tokens" {
			t.Fatalf("max-token finished = %#v", params)
		}
		if _, present := params["lastAssistantMessage"]; present {
			t.Fatalf("empty output was included: %#v", params)
		}
	}
}

func TestJSONRPCSDKEOFReleasesOwnedSession(t *testing.T) {
	e := newIntegrationEngine(t)
	input, writer := io.Pipe()
	capture := newSDKFrameCapture()
	done := make(chan error, 1)
	go func() { done <- e.ServeJSONRPC(context.Background(), input, capture) }()
	if _, err := io.WriteString(writer, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"cwd":".","provider":"echo","model":"echo"}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["id"] == float64(1) {
				return true
			}
		}
		return false
	})
	if _, err := io.WriteString(writer, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"sdk-eof","contentBlocks":[{"type":"text","text":"EOF"}]}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["id"] == float64(2) {
				return true
			}
		}
		return false
	})
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession("sdk-eof")
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	attached := session.attached
	session.mu.Unlock()
	if attached {
		t.Fatal("SDK-owned session remained attached after EOF")
	}
}

func TestJSONRPCSDKSubscriptionDoesNotDropBurstEvents(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "sdk-burst", "")
	if err != nil {
		t.Fatal(err)
	}
	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, capture)
	session, _ := e.getSession(id)
	for index := 0; index < 128; index++ {
		if _, err := e.appendEvent(session, "feedback/record", map[string]any{"index": index}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for len(capture.sessionEventSeqs()) < 128 {
		select {
		case <-capture.wake:
		case <-deadline.C:
			t.Fatalf("received %d/128 session events", len(capture.sessionEventSeqs()))
		}
	}
	if err := server.close(); err != nil {
		t.Fatal(err)
	}
	seqs := capture.sessionEventSeqs()
	for index, seq := range seqs {
		if seq != index {
			t.Fatalf("event %d has seq %d", index, seq)
		}
	}
}

func TestJSONRPCSDKForwardsSharedEngineSessions(t *testing.T) {
	e := newIntegrationEngine(t)
	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, capture)
	defer func() { _ = server.close() }()

	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "sdk-shared-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background(), parent, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "parent"}}}); err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSubagent(context.Background(), parent, "sdk-shared-child", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background(), child, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "child"}}}); err != nil {
		t.Fatal(err)
	}

	frames := waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		hasParentEvent, hasRunning, hasStarted := false, false, false
		for _, frame := range frames {
			params, _ := frame["params"].(map[string]any)
			switch frame["method"] {
			case "session.event":
				hasParentEvent = hasParentEvent || params["sessionId"] == parent
			case "session.status":
				hasRunning = hasRunning || params["sessionId"] == parent && params["status"] == "running"
			case "subagent.started":
				hasStarted = hasStarted || params["parentSessionId"] == parent && params["childSessionId"] == child
			}
		}
		return hasParentEvent && hasRunning && hasStarted
	})
	if len(frames) == 0 {
		t.Fatal("shared Engine activity produced no SDK notifications")
	}

	if err := server.close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{parent, child} {
		session, _ := e.getSession(id)
		session.mu.Lock()
		attached := session.attached
		session.mu.Unlock()
		if !attached {
			t.Fatalf("shared session %q was detached by SDK shutdown", id)
		}
	}
}

func TestJSONRPCSDKRepairsDroppedHostFrames(t *testing.T) {
	e := newIntegrationEngine(t)
	capture := newSDKFrameCapture()
	ctx, cancel := context.WithCancel(context.Background())
	server := newSDKServer(e, ctx, cancel, capture)
	defer func() { _ = server.close() }()

	server.stateMu.Lock()
	ids := make([]string, 0, 48)
	var runErr error
	for index := 0; index < cap(ids); index++ {
		id := fmt.Sprintf("sdk-host-burst-%02d", index)
		if _, runErr = e.CreateSession(context.Background(), e.Config().Workspace, id, ""); runErr != nil {
			break
		}
		ids = append(ids, id)
		if _, runErr = e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: id}}}); runErr != nil {
			break
		}
	}
	server.stateMu.Unlock()
	if runErr != nil {
		t.Fatal(runErr)
	}

	frames := waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		events := map[string]bool{}
		running := map[string]bool{}
		idle := map[string]bool{}
		for _, frame := range frames {
			params, _ := frame["params"].(map[string]any)
			id, _ := params["sessionId"].(string)
			switch frame["method"] {
			case "session.event":
				events[id] = true
			case "session.status":
				if params["status"] == "running" {
					running[id] = true
				} else if params["status"] == "idle" {
					idle[id] = true
				}
			}
		}
		for _, id := range ids {
			if !events[id] || !running[id] || !idle[id] {
				return false
			}
		}
		return true
	})
	statusCounts := map[string]map[string]int{}
	for _, frame := range frames {
		if frame["method"] != "session.status" {
			continue
		}
		params, _ := frame["params"].(map[string]any)
		id, _ := params["sessionId"].(string)
		status, _ := params["status"].(string)
		if statusCounts[id] == nil {
			statusCounts[id] = map[string]int{}
		}
		statusCounts[id][status]++
	}
	for _, id := range ids {
		if statusCounts[id]["running"] != 1 || statusCounts[id]["idle"] != 1 {
			t.Fatalf("session %q status notifications = %#v", id, statusCounts[id])
		}
	}
}
