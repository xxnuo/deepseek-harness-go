package harness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type toolLoopProvider struct {
	mu       sync.Mutex
	requests []ChatRequest
	unknown  bool
	finish   string
	calls    []ToolCall
}

func (p *toolLoopProvider) ID() string   { return "tool-loop" }
func (p *toolLoopProvider) Name() string { return "Tool Loop" }
func (p *toolLoopProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "tool-loop", Name: "Tool Loop"}}, nil
}
func (p *toolLoopProvider) Complete(_ context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	callRound := len(p.requests) == 1
	p.mu.Unlock()
	if !callRound {
		if err := onDelta(Delta{Text: "done"}); err != nil {
			return Completion{}, err
		}
		return Completion{Text: "done", Finish: "stop"}, nil
	}
	calls := append([]ToolCall(nil), p.calls...)
	if len(calls) == 0 {
		name := "unit_tool"
		if p.unknown {
			name = "missing_tool"
		}
		calls = []ToolCall{{ID: "call-1", Name: name, Arguments: json.RawMessage(`{"value":"ok"}`)}}
	}
	deltas := make([]ToolCallDelta, 0, len(calls))
	for index, call := range calls {
		deltas = append(deltas, ToolCallDelta{Index: index, ID: call.ID, Name: call.Name, ArgumentsDelta: string(call.Arguments)})
	}
	if err := onDelta(Delta{ToolCalls: deltas}); err != nil {
		return Completion{}, err
	}
	finish := p.finish
	if finish == "" {
		finish = "tool_calls"
	}
	return Completion{ToolCalls: calls, Finish: finish}, nil
}

func TestMaxTokensDoesNotExecuteToolCall(t *testing.T) {
	provider := &toolLoopProvider{finish: "length"}
	e := newToolLoopEngine(t, provider)
	executed := false
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "unit_tool", Parameters: map[string]any{"type": "object"}},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			executed = true
			return textToolResult("unexpected"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "truncated call"}}}); err != nil {
		t.Fatal(err)
	}
	if executed {
		t.Fatal("tool executed after finish_reason=length")
	}
	if len(provider.snapshot()) != 1 {
		t.Fatalf("provider requests = %d, want 1", len(provider.snapshot()))
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if event.Type != "assistant/message" {
			continue
		}
		for _, block := range contentBlocks(nestedMessage(event.Data)["content"]) {
			if block.Type == "tool-call" {
				t.Fatal("truncated tool call was persisted")
			}
		}
	}
	last := session.Events[len(session.Events)-1]
	data, _ := last.Data.(map[string]any)
	reason, _ := data["reason"].(map[string]any)
	if last.Type != "turn/end" || reason["kind"] != "max-tokens" {
		t.Fatalf("last event = %#v", last)
	}
}

func TestExecuteToolAppliesCooperativeTimeout(t *testing.T) {
	cleaned := false
	result, err := executeTool(context.Background(), Tool{
		Timeout: 10 * time.Millisecond,
		Execute: func(ctx context.Context, _ ToolCall) (ToolResult, error) {
			<-ctx.Done()
			cleaned = true
			return textToolResult("cleanup complete"), nil
		},
	}, ToolCall{Name: "slow"})
	if err != nil {
		t.Fatal(err)
	}
	if !cleaned || !result.IsError || result.Error == nil || result.Error.Code != "TOOL_TIMEOUT" || !strings.Contains(result.Content[0].Text, "10ms") {
		t.Fatalf("timeout result = %#v, cleaned=%v", result, cleaned)
	}
}

func TestExecuteToolPreservesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	want := context.Canceled
	_, err := executeTool(ctx, Tool{
		Timeout: time.Second,
		Execute: func(ctx context.Context, _ ToolCall) (ToolResult, error) { return ToolResult{}, ctx.Err() },
	}, ToolCall{Name: "cancelled"})
	if err != want {
		t.Fatalf("executeTool() error = %v, want %v", err, want)
	}
}

func TestExecuteToolValidatesDeclaredOutput(t *testing.T) {
	tool := Tool{
		Schema: ToolSchema{
			Name:       "typed",
			Parameters: objectSchema(map[string]any{}),
			Output:     objectSchema(map[string]any{"answer": map[string]any{"type": "string"}}, "answer"),
		},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Value: map[string]any{"answer": 42}}, nil
		},
	}
	result, err := executeTool(context.Background(), tool, ToolCall{Name: "typed"})
	if err == nil {
		t.Fatalf("executeTool() = %#v, nil; want invalid output", result)
	}
	metadata := toolExecutionError(err)
	if metadata.Name != "ToolOutputError" || metadata.Code != "INVALID_TOOL_OUTPUT" {
		t.Fatalf("tool error = %#v", metadata)
	}
}

func TestCanonicalCodeToolValueUsesJSONFieldNames(t *testing.T) {
	type value struct {
		SessionID string `json:"sessionId"`
	}
	got, ok := canonicalCodeToolValue("typed", ToolResult{Value: value{SessionID: "term-1"}}).(map[string]any)
	if !ok || got["sessionId"] != "term-1" || got["SessionID"] != nil {
		t.Fatalf("canonical value = %#v", got)
	}
}

func TestToolRuntimeMaterializesOutputAndPresentationOnce(t *testing.T) {
	provider := &toolLoopProvider{}
	e := newToolLoopEngine(t, provider)
	rendered, projected, finalized := 0, 0, 0
	tool := Tool{
		Schema: ToolSchema{Name: "unit_tool", Parameters: objectSchema(map[string]any{}), Output: objectSchema(map[string]any{"value": map[string]any{"type": "string"}}, "value")},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Value: map[string]any{"value": "ok"}}, nil
		},
		RenderOutput: func(_ ToolCall, value any) ([]ContentBlock, error) {
			rendered++
			return []ContentBlock{{Type: "text", Text: value.(map[string]any)["value"].(string)}}, nil
		},
		PresentationMeta: func(_ ToolCall, value any) (any, error) {
			projected++
			return map[string]any{"projected": value.(map[string]any)["value"]}, nil
		},
		FinalizeContent: func(_ ToolCall, result ToolResult) ([]ContentBlock, error) {
			finalized++
			return append(result.Content, ContentBlock{Type: "text", Text: "final"}), nil
		},
	}
	if err := e.RegisterTool(tool); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "project"}}}); err != nil {
		t.Fatal(err)
	}
	if rendered != 1 || projected != 1 || finalized != 1 {
		t.Fatalf("runtime callbacks rendered=%d projected=%d finalized=%d", rendered, projected, finalized)
	}
	requests := provider.snapshot()
	if len(requests) != 2 || requests[1].Messages[2].Content != "okfinal" {
		t.Fatalf("finalized replay = %#v", requests)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if event.Type != "tool/result" {
			continue
		}
		data := event.Data.(map[string]any)
		meta := data["meta"].(map[string]any)
		if meta["projected"] != "ok" {
			t.Fatalf("persisted metadata = %#v", meta)
		}
		return
	}
	t.Fatal("missing tool/result")
}

func TestToolRuntimeDefersContextsInModelOrderForNextStep(t *testing.T) {
	provider := &toolLoopProvider{calls: []ToolCall{
		{ID: "call-1", Name: "context_one", Arguments: json.RawMessage(`{}`)},
		{ID: "call-2", Name: "context_two", Arguments: json.RawMessage(`{}`)},
	}}
	e := newToolLoopEngine(t, provider)
	for _, name := range []string{"context_one", "context_two"} {
		name := name
		if err := e.RegisterTool(Tool{
			Schema: ToolSchema{Name: name, Parameters: objectSchema(map[string]any{})},
			ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
				exec.DeferContext(ToolContext{Content: []ContentBlock{{Type: "text", Text: name}}, Source: map[string]any{"kind": "plugin", "plugin": name}})
				return textToolResult(name + " result"), nil
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "contexts"}}}); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(requests))
	}
	messages := requests[1].Messages
	if len(messages) != 6 || messages[4].Role != "user" || messages[4].Content != "context_one" || messages[5].Role != "user" || messages[5].Content != "context_two" {
		t.Fatalf("next-step contexts = %#v", messages)
	}
}

func TestToolRuntimeConcludesTurnAfterEntireBatch(t *testing.T) {
	provider := &toolLoopProvider{calls: []ToolCall{
		{ID: "call-1", Name: "conclude", Arguments: json.RawMessage(`{}`)},
		{ID: "call-2", Name: "after_conclude", Arguments: json.RawMessage(`{}`)},
	}}
	e := newToolLoopEngine(t, provider)
	executed := []string{}
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "conclude", Parameters: objectSchema(map[string]any{})},
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			executed = append(executed, "conclude")
			exec.ConcludeTurn()
			return textToolResult("concluded"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "after_conclude", Parameters: objectSchema(map[string]any{})},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			executed = append(executed, "after_conclude")
			return textToolResult("after"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "conclude"}}}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(executed, ",") != "conclude,after_conclude" {
		t.Fatalf("executed calls = %v", executed)
	}
	if len(provider.snapshot()) != 1 {
		t.Fatalf("provider requests = %d, want 1", len(provider.snapshot()))
	}
}

func TestToolRuntimeFailureCannotConcludeTurn(t *testing.T) {
	provider := &toolLoopProvider{}
	e := newToolLoopEngine(t, provider)
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{
			Name:       "unit_tool",
			Parameters: objectSchema(map[string]any{}),
			Output:     objectSchema(map[string]any{"value": map[string]any{"type": "string"}}, "value"),
		},
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			exec.ConcludeTurn()
			return ToolResult{Value: map[string]any{"value": 42}}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	text, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "invalid"}}})
	if err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v; want done", text, err)
	}
	if len(provider.snapshot()) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(provider.snapshot()))
	}
}

func TestToolRuntimeFinalizerFailureBecomesToolResult(t *testing.T) {
	provider := &toolLoopProvider{}
	e := newToolLoopEngine(t, provider)
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "unit_tool", Parameters: objectSchema(map[string]any{})},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return textToolResult("body"), nil
		},
		FinalizeContent: func(ToolCall, ToolResult) ([]ContentBlock, error) {
			return nil, errors.New("finalizer failed")
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	text, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "finalize"}}})
	if err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v; want done", text, err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 || !strings.Contains(requests[1].Messages[2].Content, "finalizer failed") {
		t.Fatalf("finalizer failure replay = %#v", requests)
	}
}

func TestToolRuntimeInvalidJSONReachesToolPipeline(t *testing.T) {
	provider := &toolLoopProvider{calls: []ToolCall{{ID: "call-1", Name: "invalid_json", Arguments: json.RawMessage(`{"broken"`)}}}
	e := newToolLoopEngine(t, provider)
	executed, finalized := 0, 0
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "invalid_json", Parameters: objectSchema(map[string]any{})},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			executed++
			return ToolResult{}, errors.New("invalid arguments")
		},
		FinalizeContent: func(_ ToolCall, result ToolResult) ([]ContentBlock, error) {
			finalized++
			return result.Content, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "invalid json"}}}); err != nil {
		t.Fatal(err)
	}
	if executed != 1 || finalized != 1 {
		t.Fatalf("pipeline calls executed=%d finalized=%d", executed, finalized)
	}
}

func (p *toolLoopProvider) snapshot() []ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...)
}

func newToolLoopEngine(t *testing.T, provider *toolLoopProvider) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = provider.ID()
	cfg.Model = "tool-loop"
	cfg.Persist = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestToolLoopExecutesAndReplaysToolResult(t *testing.T) {
	provider := &toolLoopProvider{}
	e := newToolLoopEngine(t, provider)
	var executed ToolCall
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "unit_tool", Description: "test tool", Parameters: map[string]any{"type": "object"}},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			executed = call
			return textToolResult("tool output"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	text, err := e.Run(context.Background(), id, PromptRequest{Mode: "queue", Content: []PromptContentPart{{Type: "text", Text: "call the tool"}}})
	if err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v; want done", text, err)
	}
	if executed.ID != "call-1" || executed.Name != "unit_tool" || string(executed.Arguments) != `{"value":"ok"}` {
		t.Fatalf("tool call = %#v", executed)
	}
	if executed.SessionID != id {
		t.Fatalf("tool session = %q, want %q", executed.SessionID, id)
	}
	requests := provider.snapshot()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(requests))
	}
	foundBash := false
	for _, schema := range requests[0].Tools {
		foundBash = foundBash || schema.Name == "bash"
	}
	if !foundBash {
		t.Fatalf("first request tools = %#v, want built-in schemas", requests[0].Tools)
	}
	if len(requests[1].Messages) != 3 {
		t.Fatalf("second request messages = %#v, want user/assistant/tool", requests[1].Messages)
	}
	assistant := requests[1].Messages[1]
	if assistant.Role != "assistant" || len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].Name != "unit_tool" {
		t.Fatalf("assistant replay = %#v", assistant)
	}
	toolMessage := requests[1].Messages[2]
	if toolMessage.Role != "tool" || toolMessage.ToolCallID != "call-1" || toolMessage.Content != "tool output" {
		t.Fatalf("tool replay = %#v", toolMessage)
	}

	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	var sequence []string
	toolCallSeq := -1
	for _, event := range session.Events {
		switch event.Type {
		case "assistant/message":
			if len(event.SourceEventSeqs) != 1 {
				t.Fatalf("assistant provenance = %#v", event.SourceEventSeqs)
			}
		case "tool/call":
			toolCallSeq = int(event.Seq)
			sequence = append(sequence, event.Type)
		case "tool/result":
			sequence = append(sequence, event.Type)
			if len(event.SourceEventSeqs) != 1 || event.SourceEventSeqs[0] != toolCallSeq {
				t.Fatalf("tool result provenance = %#v, call seq %d", event.SourceEventSeqs, toolCallSeq)
			}
		}
	}
	if strings.Join(sequence, ",") != "tool/call,tool/result" {
		t.Fatalf("tool event order = %v", sequence)
	}
}

func TestUnknownToolProducesErrorResultAndContinues(t *testing.T) {
	provider := &toolLoopProvider{unknown: true}
	e := newToolLoopEngine(t, provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "unknown"}}}); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 || len(requests[1].Messages) != 3 {
		t.Fatalf("requests = %#v, want two rounds with tool result", requests)
	}
	if !strings.Contains(requests[1].Messages[2].Content, "unknown tool") {
		t.Fatalf("unknown tool result = %#v", requests[1].Messages[2])
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if event.Type != "tool/result" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		message, _ := data["message"].(map[string]any)
		blocks, _ := message["content"].([]ContentBlock)
		if len(blocks) != 1 || !blocks[0].IsError {
			t.Fatalf("unknown tool result = %#v", event.Data)
		}
		return
	}
	t.Fatal("missing tool/result event")
}

func TestToolLoopReplaysPersistedToolHistory(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = dataDir
	cfg.Workspace = dataDir
	cfg.Provider = "tool-loop"
	cfg.Model = "tool-loop"
	cfg.Persist = true

	firstProvider := &toolLoopProvider{}
	first, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	first.RegisterProvider(firstProvider)
	if err := first.RegisterTool(Tool{
		Schema: ToolSchema{Name: "unit_tool", Parameters: map[string]any{"type": "object"}},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return textToolResult("persisted output"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := first.CreateSession(context.Background(), dataDir, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "first turn"}}}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	secondProvider := &toolLoopProvider{}
	second, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	second.RegisterProvider(secondProvider)
	if err := second.RegisterTool(Tool{
		Schema: ToolSchema{Name: "unit_tool", Parameters: map[string]any{"type": "object"}},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return textToolResult("second output"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "second turn"}}}); err != nil {
		t.Fatal(err)
	}

	requests := secondProvider.snapshot()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(requests))
	}
	messages := requests[0].Messages
	if len(messages) != 5 {
		t.Fatalf("persisted messages = %#v, want first user/assistant/tool/assistant plus second user", messages)
	}
	if messages[1].Role != "assistant" || len(messages[1].ToolCalls) != 1 || messages[1].ToolCalls[0].ID != "call-1" {
		t.Fatalf("persisted assistant tool call = %#v", messages[1])
	}
	if messages[2].Role != "tool" || messages[2].ToolCallID != "call-1" || messages[2].Content != "persisted output" {
		t.Fatalf("persisted tool result = %#v", messages[2])
	}
	if messages[3].Role != "assistant" || messages[3].Content != "done" || messages[4].Role != "user" || messages[4].Content != "second turn" {
		t.Fatalf("persisted transcript tail = %#v", messages[3:])
	}
}

func TestRegisterToolRejectsDuplicateAndMissingExecutor(t *testing.T) {
	e := newIntegrationEngine(t)
	tool := Tool{Schema: ToolSchema{Name: "duplicate", Parameters: map[string]any{"type": "object"}}, Execute: func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil }}
	if err := e.RegisterTool(tool); err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterTool(tool); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate registration error = %v", err)
	}
	if err := e.RegisterTool(Tool{Schema: ToolSchema{Name: "no-executor"}}); err == nil {
		t.Fatal("missing executor accepted")
	}
	if err := e.RegisterTool(Tool{
		Schema:  ToolSchema{Name: "bad-output", Output: map[string]any{"type": "object", "$ref": "#/$defs/result"}},
		Execute: func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil },
	}); err == nil || !strings.Contains(err.Error(), "unsupported keyword") {
		t.Fatalf("invalid output schema error = %v", err)
	}
}

func TestListToolsReturnsDetachedSchemas(t *testing.T) {
	e := newIntegrationEngine(t)
	if err := e.RegisterTool(Tool{
		Schema:  ToolSchema{Name: "detached", Parameters: map[string]any{"type": "object"}},
		Execute: func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil },
	}); err != nil {
		t.Fatal(err)
	}
	for _, schema := range e.ListTools() {
		if schema.Name == "detached" {
			schema.Parameters["type"] = "array"
		}
	}
	for _, schema := range e.ListTools() {
		if schema.Name == "detached" && schema.Parameters["type"] != "object" {
			t.Fatalf("registered schema mutated through ListTools result: %#v", schema.Parameters)
		}
	}
}
