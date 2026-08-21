package harness

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

type interruptedStreamProvider struct {
	mu       sync.Mutex
	attempts int
	first    []Delta
	started  chan struct{}
	requests []ChatRequest
}

func (p *interruptedStreamProvider) ID() string   { return "interrupted-stream" }
func (p *interruptedStreamProvider) Name() string { return "Interrupted Stream" }
func (p *interruptedStreamProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: p.ID(), Name: p.Name()}}, nil
}
func (p *interruptedStreamProvider) Complete(ctx context.Context, request ChatRequest, onDelta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.attempts++
	attempt := p.attempts
	p.requests = append(p.requests, request)
	p.mu.Unlock()
	if attempt > 1 {
		if err := onDelta(Delta{Text: "after"}); err != nil {
			return Completion{}, err
		}
		return Completion{Text: "after", Finish: "stop"}, nil
	}
	for _, delta := range p.first {
		if err := onDelta(delta); err != nil {
			return Completion{}, err
		}
	}
	p.started <- struct{}{}
	<-ctx.Done()
	return Completion{}, ctx.Err()
}

func (p *interruptedStreamProvider) snapshot() []ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...)
}

func startInterruptedStream(t *testing.T, deltas []Delta) (*Engine, *interruptedStreamProvider, string) {
	t.Helper()
	engine := newIntegrationEngine(t)
	provider := &interruptedStreamProvider{first: deltas, started: make(chan struct{}, 1)}
	engine.RegisterProvider(provider)
	id, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Prompt(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("stream did not start")
	}
	if err := engine.CancelSession(id); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := engine.WaitForIdle(waitCtx, id); err != nil {
		t.Fatal(err)
	}
	return engine, provider, id
}

func TestCancelFinalizesVisibleStreamPrefix(t *testing.T) {
	engine, provider, id := startInterruptedStream(t, []Delta{
		{Reasoning: "thinking "},
		{Reasoning: "hard", Usage: map[string]any{"inputTokens": 7, "outputTokens": 4}},
		{Text: "reading the file"},
		{ToolCalls: []ToolCallDelta{{Index: 1, ID: "call-1", Name: "read", ArgumentsDelta: `{"pa`}}},
	})
	session, err := engine.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	chunkSeqs := make([]int, 0)
	messageIndex, stepEndIndex, turnEndIndex := -1, -1, -1
	var message Event
	for index, event := range events {
		switch event.Type {
		case "assistant/chunk":
			chunkSeqs = append(chunkSeqs, event.Seq)
		case "assistant/message":
			message, messageIndex = event, index
		case "step/end":
			stepEndIndex = index
		case "turn/end":
			turnEndIndex = index
		case "tool/call":
			t.Fatalf("partial tool call was dispatched: %#v", event)
		}
	}
	if messageIndex < 0 || stepEndIndex <= messageIndex || turnEndIndex <= stepEndIndex {
		t.Fatalf("cancel event ordering = message:%d step-end:%d turn-end:%d", messageIndex, stepEndIndex, turnEndIndex)
	}
	data, _ := message.Data.(map[string]any)
	blocks := contentBlocks(nestedMessage(data)["content"])
	want := []ContentBlock{{Type: "reasoning", Text: "thinking hard"}, {Type: "text", Text: "reading the file"}}
	if !reflect.DeepEqual(blocks, want) || data["interrupted"] != true {
		t.Fatalf("interrupted message = %#v", data)
	}
	usage, _ := data["usage"].(map[string]any)
	if eventInt(usage["inputTokens"]) != 7 || eventInt(usage["outputTokens"]) != 4 {
		t.Fatalf("interrupted usage = %#v", usage)
	}
	if !reflect.DeepEqual(message.SourceEventSeqs, chunkSeqs) {
		t.Fatalf("source refs = %#v, want %#v", message.SourceEventSeqs, chunkSeqs)
	}

	if text, err := engine.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "continue"}}}); err != nil || text != "after" {
		t.Fatalf("follow-up Run() = %q, %v", text, err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(requests))
	}
	var replayed bool
	for _, item := range requests[1].Messages {
		if item.Role == "assistant" && item.Reasoning == "thinking hard" && item.Content == "reading the file" {
			replayed = true
		}
	}
	if !replayed {
		t.Fatalf("follow-up request omitted interrupted prefix: %#v", requests[1].Messages)
	}
}

func TestCancelWithoutVisibleContentFinalizesNothing(t *testing.T) {
	engine, _, id := startInterruptedStream(t, []Delta{
		{ToolCalls: []ToolCallDelta{{Index: 0, ID: "call-1", Name: "read", ArgumentsDelta: `{"pa`}}},
		{Usage: map[string]any{"inputTokens": 3, "outputTokens": 0}},
	})
	session, err := engine.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if event.Type == "assistant/message" {
			t.Fatalf("invisible stream finalized a message: %#v", event)
		}
	}
}
