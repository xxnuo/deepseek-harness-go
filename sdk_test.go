package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	input := strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"cwd":".","provider":"echo","model":"echo"}}`,
		`{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"sdk-session","contentBlocks":[{"type":"text","text":"SDK works"}]}}`,
		`{"jsonrpc":"2.0","id":3,"method":"shutdown"}`,
	}, "\n") + "\n")
	var output bytes.Buffer
	if err := e.ServeJSONRPC(context.Background(), input, &output); err != nil {
		t.Fatalf("ServeJSONRPC: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) < 3 {
		t.Fatalf("responses = %q, want initialization, prompt, and shutdown", output.String())
	}
	var initialized map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &initialized); err != nil {
		t.Fatal(err)
	}
	result, _ := initialized["result"].(map[string]any)
	info, _ := result["serverInfo"].(map[string]any)
	if info["name"] != "deepseek-harness-sdk-runtime" {
		t.Fatalf("serverInfo = %#v", info)
	}
	var prompt map[string]any
	var frames []map[string]any
	for _, line := range lines {
		var frame map[string]any
		if err := json.Unmarshal([]byte(line), &frame); err != nil {
			t.Fatal(err)
		}
		frames = append(frames, frame)
		if frame["id"] == float64(2) {
			prompt = frame
		}
	}
	if prompt == nil {
		t.Fatalf("prompt receipt missing: %s", output.String())
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
		t.Fatalf("no durable inbox receipt for %q in %s", messageID, output.String())
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
			var response map[string]any
			if err := json.Unmarshal([]byte(lines[0]), &response); err != nil {
				t.Fatal(err)
			}
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
	var response map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &response); err != nil {
		t.Fatal(err)
	}
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
	if err := server.handle(jsonRPCRequest{ID: 1, Method: "initialize", Params: sdkRawParams(t, map[string]any{
		"cwd": e.Config().Workspace, "provider": "echo", "model": "echo",
	})}); err != nil {
		t.Fatal(err)
	}
	if err := server.ensureSession("sdk-parent"); err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSubagent(context.Background(), "sdk-parent", "sdk-child", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background(), child, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "first"}}}); err != nil {
		t.Fatal(err)
	}
	server.reconcileSubagents()
	waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		for _, frame := range frames {
			if frame["method"] != "subagent.started" {
				continue
			}
			params, _ := frame["params"].(map[string]any)
			if params["parentSessionId"] == "sdk-parent" && params["childSessionId"] == child {
				return true
			}
		}
		return false
	})
	if _, err := e.Run(context.Background(), child, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "second"}}}); err != nil {
		t.Fatal(err)
	}
	if err := server.close(); err != nil {
		t.Fatal(err)
	}
	frames := waitSDKFrames(t, capture, func(frames []map[string]any) bool {
		count := 0
		for _, frame := range frames {
			if frame["method"] == "subagent.finished" {
				params, _ := frame["params"].(map[string]any)
				if params["childSessionId"] == child {
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
		if params["childSessionId"] == child {
			finished = append(finished, params)
		}
	}
	for index, want := range []string{"first", "second"} {
		params := finished[index]
		if params["provider"] != "echo" || params["agentId"] != child || params["status"] != "ok" || params["stopReason"] != "completed" {
			t.Fatalf("finished[%d] = %#v", index, params)
		}
		content, _ := params["lastAssistantMessage"].([]any)
		block, _ := content[0].(map[string]any)
		if block["text"] != want {
			t.Fatalf("finished[%d] output = %#v, want %q", index, content, want)
		}
	}
}

func TestJSONRPCSDKEOFReleasesOwnedSession(t *testing.T) {
	e := newIntegrationEngine(t)
	input := strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"cwd":".","provider":"echo","model":"echo"}}`,
		`{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"sdk-eof","contentBlocks":[{"type":"text","text":"EOF"}]}}`,
	}, "\n") + "\n")
	var output bytes.Buffer
	if err := e.ServeJSONRPC(context.Background(), input, &output); err != nil {
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
		hasParentEvent, hasRunning, hasStarted, hasFinished := false, false, false, false
		for _, frame := range frames {
			params, _ := frame["params"].(map[string]any)
			switch frame["method"] {
			case "session.event":
				hasParentEvent = hasParentEvent || params["sessionId"] == parent
			case "session.status":
				hasRunning = hasRunning || params["sessionId"] == parent && params["status"] == "running"
			case "subagent.started":
				hasStarted = hasStarted || params["parentSessionId"] == parent && params["childSessionId"] == child
			case "subagent.finished":
				hasFinished = hasFinished || params["parentSessionId"] == parent && params["childSessionId"] == child
			}
		}
		return hasParentEvent && hasRunning && hasStarted && hasFinished
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
