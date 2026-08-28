package harness

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type toolSchedulerProvider struct {
	mu    sync.Mutex
	calls []ToolCall
	round int
}

func (p *toolSchedulerProvider) ID() string   { return "tool-scheduler" }
func (p *toolSchedulerProvider) Name() string { return "Tool Scheduler" }
func (p *toolSchedulerProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: p.ID(), Name: p.Name()}}, nil
}
func (p *toolSchedulerProvider) Complete(_ context.Context, _ ChatRequest, onDelta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.round++
	round := p.round
	calls := append([]ToolCall(nil), p.calls...)
	p.mu.Unlock()
	if round > 1 {
		if err := onDelta(Delta{Text: "done"}); err != nil {
			return Completion{}, err
		}
		return Completion{Text: "done", Finish: "stop"}, nil
	}
	deltas := make([]ToolCallDelta, 0, len(calls))
	for index, call := range calls {
		deltas = append(deltas, ToolCallDelta{Index: index, ID: call.ID, Name: call.Name, ArgumentsDelta: string(call.Arguments)})
	}
	if err := onDelta(Delta{ToolCalls: deltas}); err != nil {
		return Completion{}, err
	}
	return Completion{ToolCalls: calls, Finish: "tool_calls"}, nil
}

type schedulerGate struct {
	once sync.Once
	ch   chan struct{}
}

func newSchedulerGate() *schedulerGate { return &schedulerGate{ch: make(chan struct{})} }
func (g *schedulerGate) release()      { g.once.Do(func() { close(g.ch) }) }

type schedulerProbe struct {
	mu      sync.Mutex
	started chan string
	active  int
	max     int
	gates   map[string]*schedulerGate
}

func newSchedulerProbe(ids ...string) *schedulerProbe {
	probe := &schedulerProbe{started: make(chan string, len(ids)), gates: map[string]*schedulerGate{}}
	for _, id := range ids {
		probe.gates[id] = newSchedulerGate()
	}
	return probe
}

func (p *schedulerProbe) tool(name string, classifier func(ToolCall) bool) Tool {
	return Tool{
		Schema:            ToolSchema{Name: name, Parameters: objectSchema(map[string]any{"id": map[string]any{"type": "string"}}, "id")},
		IsConcurrencySafe: classifier,
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var input struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(call.Arguments, &input); err != nil {
				return ToolResult{}, err
			}
			p.mu.Lock()
			p.active++
			if p.active > p.max {
				p.max = p.active
			}
			gate := p.gates[input.ID]
			p.mu.Unlock()
			p.started <- input.ID
			<-gate.ch
			p.mu.Lock()
			p.active--
			p.mu.Unlock()
			return textToolResult("done-" + input.ID), nil
		},
	}
}

func (p *schedulerProbe) release(id string) { p.gates[id].release() }

func (p *schedulerProbe) maxActive() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.max
}

func schedulerCall(id, name string) ToolCall {
	return ToolCall{ID: "call-" + id, Name: name, Arguments: json.RawMessage(`{"id":"` + id + `"}`)}
}

func newToolSchedulerEngine(t *testing.T, calls []ToolCall, cap int) (*Engine, string) {
	t.Helper()
	provider := &toolSchedulerProvider{calls: calls}
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = provider.ID()
	cfg.Model = provider.ID()
	cfg.Persist = false
	cfg.MaxParallelToolCalls = cap
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	engine.RegisterProvider(provider)
	t.Cleanup(func() { _ = engine.Close() })
	id, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	return engine, id
}

func runSchedulerTurn(engine *Engine, id string) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := engine.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}})
		done <- err
	}()
	return done
}

func waitSchedulerStart(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case id := <-started:
		return id
	case <-time.After(2 * time.Second):
		t.Fatal("tool body did not start")
		return ""
	}
}

func assertNoSchedulerStart(t *testing.T, started <-chan string) {
	t.Helper()
	select {
	case id := <-started:
		t.Fatalf("unexpected tool body start: %s", id)
	case <-time.After(30 * time.Millisecond):
	}
}

func waitSchedulerDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("agent turn did not settle")
		return nil
	}
}

func toolSchedulerEvents(t *testing.T, engine *Engine, id string) []Event {
	t.Helper()
	session, err := engine.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return append([]Event(nil), session.Events...)
}

func toolResultCallID(event Event) string {
	message := nestedMessage(event.Data)
	source, _ := message["source"].(map[string]any)
	return stringValue(source["callId"])
}

func toolResultError(event Event) *ToolError {
	data, _ := event.Data.(map[string]any)
	switch value := data["error"].(type) {
	case *ToolError:
		return value
	case ToolError:
		return &value
	case map[string]any:
		return &ToolError{Name: stringValue(value["name"]), Code: stringValue(value["code"]), Message: stringValue(value["message"])}
	default:
		return nil
	}
}

func TestToolSchedulerRunsBoundedParallelAndCommitsModelOrder(t *testing.T) {
	calls := []ToolCall{schedulerCall("1", "parallel_probe"), schedulerCall("2", "parallel_probe"), schedulerCall("3", "parallel_probe")}
	engine, id := newToolSchedulerEngine(t, calls, 2)
	probe := newSchedulerProbe("1", "2", "3")
	if err := engine.RegisterTool(probe.tool("parallel_probe", alwaysConcurrencySafe)); err != nil {
		t.Fatal(err)
	}
	done := runSchedulerTurn(engine, id)
	if first, second := waitSchedulerStart(t, probe.started), waitSchedulerStart(t, probe.started); first != "1" || second != "2" {
		t.Fatalf("initial starts = %q, %q", first, second)
	}
	assertNoSchedulerStart(t, probe.started)
	probe.release("2")
	if third := waitSchedulerStart(t, probe.started); third != "3" {
		t.Fatalf("third start = %q", third)
	}
	probe.release("3")
	time.Sleep(20 * time.Millisecond)
	for _, event := range toolSchedulerEvents(t, engine, id) {
		if event.Type == "tool/result" {
			t.Fatalf("result committed before the first model call settled: %#v", event)
		}
	}
	probe.release("1")
	if err := waitSchedulerDone(t, done); err != nil {
		t.Fatal(err)
	}
	if probe.maxActive() != 2 {
		t.Fatalf("max active bodies = %d, want 2", probe.maxActive())
	}
	var results []string
	for _, event := range toolSchedulerEvents(t, engine, id) {
		if event.Type == "tool/result" {
			results = append(results, toolResultCallID(event))
		}
	}
	want := []string{"call-1", "call-2", "call-3"}
	if len(results) != len(want) {
		t.Fatalf("result order = %#v", results)
	}
	for index := range want {
		if results[index] != want[index] {
			t.Fatalf("result order = %#v, want %#v", results, want)
		}
	}
}

func TestToolSchedulerExclusiveBarrier(t *testing.T) {
	calls := []ToolCall{schedulerCall("1", "parallel_barrier"), schedulerCall("2", "exclusive_barrier"), schedulerCall("3", "parallel_barrier")}
	engine, id := newToolSchedulerEngine(t, calls, 3)
	parallel := newSchedulerProbe("1", "3")
	exclusive := newSchedulerProbe("2")
	if err := engine.RegisterTool(parallel.tool("parallel_barrier", alwaysConcurrencySafe)); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterTool(exclusive.tool("exclusive_barrier", nil)); err != nil {
		t.Fatal(err)
	}
	done := runSchedulerTurn(engine, id)
	if got := waitSchedulerStart(t, parallel.started); got != "1" {
		t.Fatalf("first start = %q", got)
	}
	assertNoSchedulerStart(t, exclusive.started)
	assertNoSchedulerStart(t, parallel.started)
	parallel.release("1")
	if got := waitSchedulerStart(t, exclusive.started); got != "2" {
		t.Fatalf("exclusive start = %q", got)
	}
	assertNoSchedulerStart(t, parallel.started)
	exclusive.release("2")
	if got := waitSchedulerStart(t, parallel.started); got != "3" {
		t.Fatalf("post-barrier start = %q", got)
	}
	parallel.release("3")
	if err := waitSchedulerDone(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestToolSchedulerClassifierFailsClosed(t *testing.T) {
	tests := []struct {
		name       string
		classifier func(ToolCall) bool
	}{
		{name: "missing"},
		{name: "false", classifier: func(ToolCall) bool { return false }},
		{name: "panic", classifier: func(ToolCall) bool { panic("classifier exploded") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			toolName := "classifier_" + test.name
			engine, id := newToolSchedulerEngine(t, []ToolCall{schedulerCall("1", toolName), schedulerCall("2", toolName)}, 10)
			probe := newSchedulerProbe("1", "2")
			if err := engine.RegisterTool(probe.tool(toolName, test.classifier)); err != nil {
				t.Fatal(err)
			}
			done := runSchedulerTurn(engine, id)
			if got := waitSchedulerStart(t, probe.started); got != "1" {
				t.Fatalf("first start = %q", got)
			}
			assertNoSchedulerStart(t, probe.started)
			probe.release("1")
			if got := waitSchedulerStart(t, probe.started); got != "2" {
				t.Fatalf("second start = %q", got)
			}
			probe.release("2")
			if err := waitSchedulerDone(t, done); err != nil {
				t.Fatal(err)
			}
			if probe.maxActive() != 1 {
				t.Fatalf("max active bodies = %d, want 1", probe.maxActive())
			}
		})
	}
}

func TestToolSchedulerCancellationDrainsAndSynthesizesSkippedCalls(t *testing.T) {
	calls := []ToolCall{schedulerCall("1", "cancel_probe"), schedulerCall("2", "cancel_probe"), schedulerCall("3", "cancel_probe"), schedulerCall("4", "cancel_probe")}
	engine, id := newToolSchedulerEngine(t, calls, 2)
	probe := newSchedulerProbe("1", "2", "3", "4")
	if err := engine.RegisterTool(probe.tool("cancel_probe", alwaysConcurrencySafe)); err != nil {
		t.Fatal(err)
	}
	done := runSchedulerTurn(engine, id)
	if first, second := waitSchedulerStart(t, probe.started), waitSchedulerStart(t, probe.started); first != "1" || second != "2" {
		t.Fatalf("initial starts = %q, %q", first, second)
	}
	if err := engine.CancelSession(id); err != nil {
		t.Fatal(err)
	}
	assertNoSchedulerStart(t, probe.started)
	probe.release("2")
	probe.release("1")
	if err := waitSchedulerDone(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	assertNoSchedulerStart(t, probe.started)

	callSeqs := map[string]int{}
	resultSeqs := map[string]int{}
	resultErrors := map[string]*ToolError{}
	var callOrder, resultOrder []string
	for _, event := range toolSchedulerEvents(t, engine, id) {
		switch event.Type {
		case "tool/call":
			data, _ := event.Data.(map[string]any)
			callID := stringValue(data["callId"])
			callSeqs[callID] = event.Seq
			callOrder = append(callOrder, callID)
		case "tool/result":
			callID := toolResultCallID(event)
			resultOrder = append(resultOrder, callID)
			resultErrors[callID] = toolResultError(event)
			if len(event.SourceEventSeqs) != 1 {
				t.Fatalf("result provenance for %s = %#v", callID, event.SourceEventSeqs)
			}
			resultSeqs[callID] = event.SourceEventSeqs[0]
		}
	}
	wantOrder := []string{"call-1", "call-2", "call-3", "call-4"}
	for label, got := range map[string][]string{"calls": callOrder, "results": resultOrder} {
		if len(got) != len(wantOrder) {
			t.Fatalf("%s order = %#v", label, got)
		}
		for index := range wantOrder {
			if got[index] != wantOrder[index] {
				t.Fatalf("%s order = %#v, want %#v", label, got, wantOrder)
			}
		}
	}
	for _, callID := range wantOrder {
		if resultSeqs[callID] != callSeqs[callID] {
			t.Fatalf("provenance for %s = %d, want %d", callID, resultSeqs[callID], callSeqs[callID])
		}
	}
	for _, callID := range []string{"call-1", "call-2"} {
		if resultErrors[callID] == nil || resultErrors[callID].Code != "ABORTED" || resultErrors[callID].Name != "AbortError" {
			t.Fatalf("started cancellation result for %s = %#v", callID, resultErrors[callID])
		}
	}
	for _, callID := range []string{"call-3", "call-4"} {
		if resultErrors[callID] == nil || resultErrors[callID].Code != "ABORTED_BEFORE_DISPATCH" || resultErrors[callID].Name != "AbortError" {
			t.Fatalf("skipped cancellation result for %s = %#v", callID, resultErrors[callID])
		}
	}
}

func TestToolSchedulerRejectsNegativeParallelCap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxParallelToolCalls = -1
	if _, err := New(WithConfig(cfg)); err == nil {
		t.Fatal("New() accepted a negative maxParallelToolCalls")
	}
}
