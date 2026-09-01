package harness

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type inProcessModelProvider struct {
	mu       sync.Mutex
	requests []ChatRequest
	counts   map[string]int
	complete func(ChatRequest, int) Completion
}

func (provider *inProcessModelProvider) ID() string   { return "in-process-model" }
func (provider *inProcessModelProvider) Name() string { return "In Process Model" }
func (provider *inProcessModelProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "in-process-model", Name: "In Process Model"}}, nil
}
func (provider *inProcessModelProvider) Complete(_ context.Context, request ChatRequest, _ func(Delta) error) (Completion, error) {
	provider.mu.Lock()
	if provider.counts == nil {
		provider.counts = map[string]int{}
	}
	index := provider.counts[request.SessionID]
	provider.counts[request.SessionID] = index + 1
	provider.requests = append(provider.requests, request)
	complete := provider.complete
	provider.mu.Unlock()
	return complete(request, index), nil
}

func newInProcessProviderEngine(t *testing.T, provider *inProcessModelProvider, options ...Option) (*Engine, string) {
	return newInProcessEngine(t, provider, options...)
}

func newInProcessEngine(t *testing.T, provider Provider, options ...Option) (*Engine, string) {
	t.Helper()
	options = append(options, WithPersistence(false), func(config *Config) {
		config.SessionTitleLLM.Enabled = false
	})
	engine, err := New(options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	engine.RegisterProvider(provider)
	parentID, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "in-process-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(parentID, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	return engine, parentID
}

type cancellableInProcessProvider struct {
	started chan struct{}
	once    sync.Once
}

func (provider *cancellableInProcessProvider) ID() string   { return "cancellable-in-process" }
func (provider *cancellableInProcessProvider) Name() string { return "Cancellable In Process" }
func (provider *cancellableInProcessProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: provider.ID(), Name: provider.Name()}}, nil
}
func (provider *cancellableInProcessProvider) Complete(ctx context.Context, _ ChatRequest, onDelta func(Delta) error) (Completion, error) {
	provider.once.Do(func() { close(provider.started) })
	if err := onDelta(Delta{Text: "partial"}); err != nil {
		return Completion{}, err
	}
	<-ctx.Done()
	return Completion{}, ctx.Err()
}

func structuredSchema(property string) map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{property: map[string]any{"type": "integer"}},
		"required":   []any{property},
	}
}

func findToolSchema(request ChatRequest, name string) (ToolSchema, bool) {
	for _, schema := range request.Tools {
		if schema.Name == name {
			return schema, true
		}
	}
	return ToolSchema{}, false
}

func TestInProcessProvidersCaptureDelegatedPolicyAfterForkSeed(t *testing.T) {
	engine, err := New(WithPersistence(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	parentID, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "policy-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(parentID, ModelSelection{Provider: "echo", Model: "echo"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(t.Context(), parentID, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "seed"}}, Literal: true}); err != nil {
		t.Fatal(err)
	}
	parent, _ := engine.getSession(parentID)
	if _, err := engine.appendEvent(parent, "sandbox/mode", map[string]any{"mode": sandboxReadOnly}); err != nil {
		t.Fatal(err)
	}
	run, err := engine.StartSubagent(t.Context(), "fork", SubagentStartRequest{
		ParentSessionID: parentID, Label: "policy child", Prompt: []ContentBlock{{Type: "text", Text: "inspect"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	child, _ := engine.getSession(run.ID)
	child.mu.Lock()
	seedLength := child.Header.SeedLength
	events := append([]Event(nil), child.Events...)
	child.mu.Unlock()
	if seedLength == 0 || seedLength+2 > len(events) {
		t.Fatalf("fork seed boundary = %d, events = %d", seedLength, len(events))
	}
	want := []Event{
		{Type: "sandbox/mode", Data: map[string]any{"mode": sandboxReadOnly, "source": "delegation"}},
		{Type: "approval/policy", Data: map[string]any{"policy": "never", "source": "delegation"}},
	}
	for index := range want {
		if events[seedLength+index].Type != want[index].Type || !reflect.DeepEqual(events[seedLength+index].Data, want[index].Data) {
			t.Fatalf("delegated policy event %d = %#v", index, events[seedLength+index])
		}
	}
	if _, err := engine.appendEvent(parent, "sandbox/mode", map[string]any{"mode": sandboxDangerFull}); err != nil {
		t.Fatal(err)
	}
	mode, err := engine.sandboxModeForCall(ToolCall{SessionID: run.ID})
	if err != nil || mode != sandboxReadOnly {
		t.Fatalf("child sandbox after parent switch = %q, %v", mode, err)
	}
	if err := run.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestInProcessSetupFailureNeverPublishesChild(t *testing.T) {
	provider := &inProcessModelProvider{complete: func(ChatRequest, int) Completion {
		return Completion{Text: "unused", Finish: "stop"}
	}}
	engine, parentID := newInProcessProviderEngine(t, provider)
	hostCtx, cancelHost := context.WithCancel(t.Context())
	defer cancelHost()
	host := engine.SubscribeHost(hostCtx)
	before := len(engine.ListSessions())
	_, err := engine.createModelSubagentWithSetup(t.Context(), parentID, "failed child", false, "one-shot", SubagentToolConfig{Provider: "spawn"}, func(*Session) error {
		return errors.New("setup failed")
	})
	if err == nil || err.Error() != "setup failed" {
		t.Fatalf("setup error = %v", err)
	}
	if got := len(engine.ListSessions()); got != before {
		t.Fatalf("session count = %d, want %d", got, before)
	}
	select {
	case frame := <-host:
		t.Fatalf("unpublished child emitted host frame %#v", frame)
	default:
	}
}

func TestInProcessRejectedPromptDoesNotAppendOneShotDescriptor(t *testing.T) {
	dir := t.TempDir()
	hook := writeHookRuntimeScript(t, dir, "reject.sh", `echo '{"decision":"block","reason":"blocked"}'`+"\n")
	configPath := dir + "/hooks.json"
	writeHookConfig(t, configPath, map[string]any{"UserPromptSubmit": []any{map[string]any{"hooks": []any{map[string]any{"command": hook}}}}})
	provider := &inProcessModelProvider{complete: func(ChatRequest, int) Completion {
		return Completion{Text: "must not run", Finish: "stop"}
	}}
	engine, parentID := newInProcessProviderEngine(t, provider, WithHooks(HookBridgeConfig{Dialect: HookDialectClaudeCode, ConfigPath: configPath, DefaultTimeout: time.Second}))
	run, err := engine.StartSubagent(t.Context(), "spawn", SubagentStartRequest{ParentSessionID: parentID, Label: "child", Prompt: []ContentBlock{{Type: "text", Text: "p"}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Wait(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentRefusal {
		child, _ := engine.getSession(run.ID)
		child.mu.Lock()
		t.Logf("child events=%#v", child.Events)
		child.mu.Unlock()
		t.Fatalf("result = %#v", result)
	}
	child, _ := engine.getSession(run.ID)
	child.mu.Lock()
	seedLength := child.Header.SeedLength
	events := append([]Event(nil), child.Events...)
	child.mu.Unlock()
	if descriptor, err := FoldSubagentDescriptor(events[seedLength:]); err != nil || descriptor != nil {
		t.Fatal("rejected one-shot prompt unexpectedly has descriptor")
	}
	_ = run.Dispose()
}

func TestInProcessStructuredOutputIsScopedAuthoritativeAndTerminal(t *testing.T) {
	var sideEffectRan bool
	provider := &inProcessModelProvider{complete: func(request ChatRequest, index int) Completion {
		if index != 0 {
			return Completion{Text: "unexpected second step", Finish: "stop"}
		}
		schema, ok := findToolSchema(request, structuredOutputToolName)
		if !ok || !reflect.DeepEqual(schema.Parameters, structuredSchema("answer")) {
			return Completion{Text: "structured tool missing", Finish: "stop"}
		}
		return Completion{Finish: "tool_calls", ToolCalls: []ToolCall{
			{ID: "capture", Name: structuredOutputToolName, Arguments: json.RawMessage(`{"answer":42}`)},
			{ID: "after", Name: "structured_side_effect", Arguments: json.RawMessage(`{}`)},
		}}
	}}
	engine, parentID := newInProcessProviderEngine(t, provider)
	if err := engine.RegisterTool(Tool{
		Schema: ToolSchema{Name: "structured_side_effect", Parameters: objectSchema(map[string]any{})},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			sideEffectRan = true
			return textToolResult("ran"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	run, err := engine.StartSubagent(t.Context(), "spawn", SubagentStartRequest{
		ParentSessionID: parentID, Prompt: []ContentBlock{{Type: "text", Text: "answer"}}, OutputSchema: structuredSchema("answer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentCompleted || !reflect.DeepEqual(result.Structured, map[string]any{"answer": json.Number("42")}) {
		t.Fatalf("structured result = %#v", result)
	}
	if sideEffectRan {
		t.Fatal("tool after structured capture executed")
	}
	parent, _ := engine.getSession(parentID)
	parentTools, err := engine.toolsForSession(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, schema := range parentTools {
		if schema.Name == structuredOutputToolName {
			t.Fatal("parent saw child-scoped structured tool")
		}
	}
	child, _ := engine.getSession(run.ID)
	if visible, err := engine.toolVisibleForSession(child, structuredOutputToolName); err != nil || !visible {
		t.Fatalf("child structured tool visibility = %v, %v", visible, err)
	}
	provider.mu.Lock()
	request := provider.requests[0]
	provider.mu.Unlock()
	if !strings.Contains(request.System, structuredOutputInstruction) {
		t.Fatalf("structured instruction missing from system prompt: %q", request.System)
	}
	foundDelegation := false
	for _, message := range request.Messages {
		if strings.Contains(message.Content, "permission scope was fixed") {
			foundDelegation = true
		}
	}
	if !foundDelegation {
		t.Fatal("delegation runtime context missing from child request")
	}
	if err := run.Dispose(); err != nil {
		t.Fatal(err)
	}
	if _, ok := engine.toolForSession(child, structuredOutputToolName); ok {
		t.Fatal("structured tool survived child disposal")
	}
}

func TestInProcessStructuredOutputRetriesInvalidArgumentsInTurn(t *testing.T) {
	provider := &inProcessModelProvider{complete: func(_ ChatRequest, index int) Completion {
		arguments := json.RawMessage(`{"answer":"wrong"}`)
		if index > 0 {
			arguments = json.RawMessage(`{"answer":7}`)
		}
		return Completion{Finish: "tool_calls", ToolCalls: []ToolCall{{ID: "capture", Name: structuredOutputToolName, Arguments: arguments}}}
	}}
	engine, parentID := newInProcessProviderEngine(t, provider)
	run, err := engine.StartSubagent(t.Context(), "spawn", SubagentStartRequest{
		ParentSessionID: parentID, Prompt: []ContentBlock{{Type: "text", Text: "answer"}}, OutputSchema: structuredSchema("answer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentCompleted || !reflect.DeepEqual(result.Structured, map[string]any{"answer": json.Number("7")}) {
		t.Fatalf("structured retry result = %#v", result)
	}
	provider.mu.Lock()
	requestCount := provider.counts[run.ID]
	provider.mu.Unlock()
	if requestCount != 2 {
		t.Fatalf("model request count = %d, want 2", requestCount)
	}
	if err := run.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestInProcessStructuredOutputRequiresCaptureOnCompletedTurn(t *testing.T) {
	provider := &inProcessModelProvider{complete: func(_ ChatRequest, _ int) Completion {
		return Completion{Text: "plain prose", Finish: "stop"}
	}}
	engine, parentID := newInProcessProviderEngine(t, provider)
	run, err := engine.StartSubagent(t.Context(), "spawn", SubagentStartRequest{
		ParentSessionID: parentID, Prompt: []ContentBlock{{Type: "text", Text: "answer"}}, OutputSchema: structuredSchema("answer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentError || result.Structured != nil || contentValueText(result.Output) != "plain prose" {
		t.Fatalf("capture-less completed result = %#v", result)
	}
	provider.mu.Lock()
	requestCount := provider.counts[run.ID]
	provider.mu.Unlock()
	if requestCount != 1 {
		t.Fatalf("capture-less request count = %d, want 1", requestCount)
	}
	if err := run.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestInProcessCancellationAfterPublicationPreservesPartialOutput(t *testing.T) {
	provider := &cancellableInProcessProvider{started: make(chan struct{})}
	engine, parentID := newInProcessEngine(t, provider)
	requestCtx, cancel := context.WithCancel(t.Context())
	run, err := engine.StartSubagent(requestCtx, "spawn", SubagentStartRequest{
		ParentSessionID: parentID, Prompt: []ContentBlock{{Type: "text", Text: "stream an answer"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(5 * time.Second):
		t.Fatal("published child did not start its model request")
	}
	cancel()
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	result, err := run.Wait(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentAborted || contentValueText(result.Output) != "partial" {
		t.Fatalf("cancelled in-process result = %#v", result)
	}
	child, err := engine.getSession(run.ID)
	if err != nil {
		t.Fatalf("published child disappeared before disposal: %v", err)
	}
	child.mu.Lock()
	attached := child.attached
	child.mu.Unlock()
	if !attached {
		t.Fatal("cancellation detached the published child before its owner disposed it")
	}
	if err := run.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestInProcessDisposeConcurrentlyWaitsForResultAndDetachesOnce(t *testing.T) {
	provider := &cancellableInProcessProvider{started: make(chan struct{})}
	engine, parentID := newInProcessEngine(t, provider)
	run, err := engine.StartSubagent(t.Context(), "spawn", SubagentStartRequest{
		ParentSessionID: parentID, Prompt: []ContentBlock{{Type: "text", Text: "stream an answer"}},
		OutputSchema: structuredSchema("answer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(5 * time.Second):
		t.Fatal("published child did not start its model request")
	}
	resultCh := make(chan SubagentResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, waitErr := run.Wait(context.Background())
		resultCh <- result
		errCh <- waitErr
	}()
	const callers = 8
	disposeErrs := make(chan error, callers)
	for range callers {
		go func() { disposeErrs <- run.Dispose() }()
	}
	for range callers {
		if disposeErr := <-disposeErrs; disposeErr != nil {
			t.Fatalf("concurrent dispose: %v", disposeErr)
		}
	}
	if waitErr := <-errCh; waitErr != nil {
		t.Fatal(waitErr)
	}
	result := <-resultCh
	if result.StopReason != SubagentAborted || contentValueText(result.Output) != "partial" {
		t.Fatalf("dispose/result race = %#v", result)
	}
	child, err := engine.getSession(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	child.mu.Lock()
	attached := child.attached
	child.mu.Unlock()
	if attached {
		t.Fatal("disposed child remains attached")
	}
	if _, ok := engine.toolForSession(child, structuredOutputToolName); ok {
		t.Fatal("disposed child retained its session-scoped structured tool")
	}
}

func TestInProcessStructuredOutputPostHookBlockRejectsCapture(t *testing.T) {
	dir := t.TempDir()
	post := writeHookRuntimeScript(t, dir, "post-structured.sh", `echo '{"decision":"block","reason":"capture rejected by hook","hookSpecificOutput":{"hookEventName":"PostToolUse"}}'`+"\n")
	configPath := dir + "/hooks.json"
	writeHookConfig(t, configPath, map[string]any{
		"PostToolUse": []any{map[string]any{
			"matcher": structuredOutputToolName,
			"hooks":   []any{map[string]any{"command": post}},
		}},
	})
	provider := &inProcessModelProvider{complete: func(_ ChatRequest, index int) Completion {
		if index == 0 {
			return Completion{Finish: "tool_calls", ToolCalls: []ToolCall{{
				ID: "capture", Name: structuredOutputToolName, Arguments: json.RawMessage(`{"answer":7}`),
			}}}
		}
		return Completion{Text: "continued after blocked capture", Finish: "stop"}
	}}
	engine, parentID := newInProcessProviderEngine(t, provider, WithHooks(HookBridgeConfig{
		Dialect: HookDialectClaudeCode, ConfigPath: configPath, DefaultTimeout: 3 * time.Second,
	}))
	run, err := engine.StartSubagent(t.Context(), "spawn", SubagentStartRequest{
		ParentSessionID: parentID, Prompt: []ContentBlock{{Type: "text", Text: "answer"}}, OutputSchema: structuredSchema("answer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentError || result.Structured != nil || contentValueText(result.Output) != "continued after blocked capture" {
		t.Fatalf("post-blocked structured result = %#v", result)
	}
	provider.mu.Lock()
	requestCount := provider.counts[run.ID]
	provider.mu.Unlock()
	if requestCount != 2 {
		t.Fatalf("post-blocked request count = %d, want 2", requestCount)
	}
	toolResult := firstToolResult(mustSession(t, engine, run.ID))
	if !toolResult.IsError || contentValueText(toolResult.Content) != "capture rejected by hook" {
		t.Fatalf("authoritative structured tool result = %#v", toolResult)
	}
	if err := run.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentInProcessStructuredChildrenOwnIndependentSchemas(t *testing.T) {
	provider := &inProcessModelProvider{complete: func(request ChatRequest, _ int) Completion {
		schema, _ := findToolSchema(request, structuredOutputToolName)
		properties, _ := schema.Parameters["properties"].(map[string]any)
		arguments := json.RawMessage(`{"answer":1}`)
		if _, ok := properties["verdict"]; ok {
			arguments = json.RawMessage(`{"verdict":2}`)
		}
		return Completion{Finish: "tool_calls", ToolCalls: []ToolCall{{ID: "capture", Name: structuredOutputToolName, Arguments: arguments}}}
	}}
	engine, parentID := newInProcessProviderEngine(t, provider)
	runA, err := engine.StartSubagent(t.Context(), "spawn", SubagentStartRequest{
		ParentSessionID: parentID, Prompt: []ContentBlock{{Type: "text", Text: "answer"}}, OutputSchema: structuredSchema("answer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	runB, err := engine.StartSubagent(t.Context(), "spawn", SubagentStartRequest{
		ParentSessionID: parentID, Prompt: []ContentBlock{{Type: "text", Text: "verdict"}}, OutputSchema: structuredSchema("verdict"),
	})
	if err != nil {
		t.Fatal(err)
	}
	resultA, errA := runA.Wait(t.Context())
	resultB, errB := runB.Wait(t.Context())
	if errA != nil || errB != nil {
		t.Fatalf("concurrent waits = %v, %v", errA, errB)
	}
	if !reflect.DeepEqual(resultA.Structured, map[string]any{"answer": json.Number("1")}) ||
		!reflect.DeepEqual(resultB.Structured, map[string]any{"verdict": json.Number("2")}) {
		t.Fatalf("concurrent structured results = %#v, %#v", resultA, resultB)
	}
	if err := runA.Dispose(); err != nil {
		t.Fatal(err)
	}
	if err := runB.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestInProcessStructuredOutputCommitsSuccessfulRunCodeCapture(t *testing.T) {
	provider := &inProcessModelProvider{complete: func(_ ChatRequest, _ int) Completion {
		return Completion{Finish: "tool_calls", ToolCalls: []ToolCall{{
			ID: "outer", Name: "run_code", Arguments: json.RawMessage(`{"code":"return await tools.structured_output({answer: 12})","description":"capture result"}`),
		}}}
	}}
	engine, parentID := newInProcessProviderEngine(t, provider, func(config *Config) { config.ToolPresentation = "ptc" })
	run, err := engine.StartSubagent(t.Context(), "spawn", SubagentStartRequest{
		ParentSessionID: parentID, Prompt: []ContentBlock{{Type: "text", Text: "answer"}}, OutputSchema: structuredSchema("answer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentCompleted || !reflect.DeepEqual(result.Structured, map[string]any{"answer": json.Number("12")}) {
		t.Fatalf("successful run_code capture result = %#v", result)
	}
	if err := run.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestInProcessStructuredOutputWaitsForRunCodeResult(t *testing.T) {
	provider := &inProcessModelProvider{complete: func(_ ChatRequest, index int) Completion {
		if index == 0 {
			return Completion{Finish: "tool_calls", ToolCalls: []ToolCall{{
				ID: "outer", Name: "run_code", Arguments: json.RawMessage(`{"code":"await tools.structured_output({answer: 12}); throw new Error('boom')","description":"capture then fail"}`),
			}}}
		}
		return Completion{Text: "outer failed", Finish: "stop"}
	}}
	engine, parentID := newInProcessProviderEngine(t, provider, func(config *Config) { config.ToolPresentation = "ptc" })
	run, err := engine.StartSubagent(t.Context(), "spawn", SubagentStartRequest{
		ParentSessionID: parentID, Prompt: []ContentBlock{{Type: "text", Text: "answer"}}, OutputSchema: structuredSchema("answer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != SubagentError || result.Structured != nil {
		t.Fatalf("failed run_code capture result = %#v", result)
	}
	if err := run.Dispose(); err != nil {
		t.Fatal(err)
	}
}
