package harness

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type retryScriptProvider struct {
	mu       sync.Mutex
	attempts int
	steps    []func(func(Delta) error) (Completion, error)
	policy   RetryPolicy
}

type mutatingRetryProvider struct {
	attempts int
	second   ChatRequest
}

func (p *mutatingRetryProvider) ID() string   { return "mutating-retry" }
func (p *mutatingRetryProvider) Name() string { return "Mutating Retry" }
func (p *mutatingRetryProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "mutating-retry", Name: "Mutating Retry"}}, nil
}
func (p *mutatingRetryProvider) RetryPolicy() RetryPolicy { return retryPolicyForTest(1) }
func (p *mutatingRetryProvider) Complete(_ context.Context, request ChatRequest, _ func(Delta) error) (Completion, error) {
	p.attempts++
	if p.attempts == 1 {
		*request.Temperature = 9
		request.Stop[0] = "mutated"
		request.Messages[0].Blocks[0].Content[0].Text = "mutated"
		request.Messages[0].ToolCalls[0].Arguments[0] = 'x'
		request.Tools[0].Parameters["type"] = "array"
		return Completion{}, &ProviderError{Code: "SERVER", Message: "retry"}
	}
	p.second = request
	return Completion{Text: "ok", Finish: "stop"}, nil
}

func (p *retryScriptProvider) ID() string   { return "retry-script" }
func (p *retryScriptProvider) Name() string { return "Retry Script" }
func (p *retryScriptProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "retry-script", Name: "Retry Script"}}, nil
}
func (p *retryScriptProvider) RetryPolicy() RetryPolicy { return p.policy }
func (p *retryScriptProvider) Complete(_ context.Context, _ ChatRequest, onDelta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	index := p.attempts
	p.attempts++
	p.mu.Unlock()
	if index >= len(p.steps) {
		return Completion{}, errors.New("unexpected extra attempt")
	}
	return p.steps[index](onDelta)
}

func retryTestEngine(t *testing.T, provider *retryScriptProvider, policy RetryPolicy) (*Engine, string) {
	t.Helper()
	provider.policy = policy
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = provider.ID()
	cfg.Model = "retry-script"
	cfg.Persist = false
	cfg.SessionTitleLLM.Enabled = false
	cfg.RetryPolicy = policy
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(context.Background(), cfg.Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	return e, id
}

func retryPolicyForTest(max int) RetryPolicy {
	return RetryPolicy{
		Mode:           RetryNormal,
		MaxRetries:     max,
		RetryableCodes: []string{"SERVER", "RATE_LIMIT", "TRANSPORT", "TIMEOUT", "EMPTY_RESPONSE"},
		InitialDelay:   time.Millisecond,
		MaxDelay:       time.Millisecond,
		JitterRatio:    0,
	}
}

func TestRetryProtectsCanonicalRequestFromProviderMutation(t *testing.T) {
	engine := newIntegrationEngine(t)
	provider := &mutatingRetryProvider{}
	temperature := 0.2
	request := ChatRequest{
		Temperature: &temperature, Stop: []string{"END"},
		Messages: []ChatMessage{{
			Blocks:    []ContentBlock{{Type: "tool-result", Content: []ContentBlock{{Type: "text", Text: "original"}}}},
			ToolCalls: []ToolCall{{Arguments: json.RawMessage(`{"value":1}`)}},
		}},
		Tools: []ToolSchema{{Parameters: map[string]any{"type": "object"}}},
	}
	completion, _, err := engine.completeWithRetry(context.Background(), provider, request, nil, 0, 0)
	if err != nil || completion.Text != "ok" || provider.attempts != 2 {
		t.Fatalf("retry = %#v, %v, attempts=%d", completion, err, provider.attempts)
	}
	if provider.second.Temperature == nil || *provider.second.Temperature != 0.2 || provider.second.Stop[0] != "END" ||
		provider.second.Messages[0].Blocks[0].Content[0].Text != "original" ||
		string(provider.second.Messages[0].ToolCalls[0].Arguments) != `{"value":1}` ||
		provider.second.Tools[0].Parameters["type"] != "object" {
		t.Fatalf("second request was polluted: %#v", provider.second)
	}
	if temperature != 0.2 || request.Stop[0] != "END" || request.Messages[0].Blocks[0].Content[0].Text != "original" ||
		string(request.Messages[0].ToolCalls[0].Arguments) != `{"value":1}` || request.Tools[0].Parameters["type"] != "object" {
		t.Fatalf("canonical request was polluted: %#v", request)
	}
}

func TestRetryRecoversWithinOneStepAndLinksOnlySuccessfulChunks(t *testing.T) {
	provider := &retryScriptProvider{steps: []func(func(Delta) error) (Completion, error){
		func(delta func(Delta) error) (Completion, error) {
			if err := delta(Delta{Text: "discarded"}); err != nil {
				return Completion{}, err
			}
			return Completion{}, &ProviderError{Code: "SERVER", Message: "temporary"}
		},
		func(delta func(Delta) error) (Completion, error) {
			if err := delta(Delta{Text: "recovered"}); err != nil {
				return Completion{}, err
			}
			return Completion{Text: "recovered", Finish: "stop"}, nil
		},
	}}
	e, id := retryTestEngine(t, provider, retryPolicyForTest(2))
	if text, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}}); err != nil || text != "recovered" {
		t.Fatalf("Run() = %q, %v", text, err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	var starts, retries, retryStarted, messages []Event
	for _, event := range events {
		switch event.Type {
		case "step/start":
			starts = append(starts, event)
		case "llm/retry":
			retries = append(retries, event)
		case "llm/retry-started":
			retryStarted = append(retryStarted, event)
		case "assistant/message":
			messages = append(messages, event)
		}
	}
	if len(starts) != 1 || len(retries) != 1 || len(retryStarted) != 1 || len(messages) != 1 {
		t.Fatalf("event counts: step=%d retry=%d started=%d message=%d", len(starts), len(retries), len(retryStarted), len(messages))
	}
	if retries[0].Seq >= retryStarted[0].Seq {
		t.Fatalf("retry events out of order: %#v %#v", retries[0], retryStarted[0])
	}
	if len(messages[0].SourceEventSeqs) != 1 {
		t.Fatalf("message source refs = %#v", messages[0].SourceEventSeqs)
	}
	refs := messages[0].SourceEventSeqs
	for _, event := range events {
		if event.Type != "assistant/chunk" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		chunk, _ := data["chunk"].(map[string]any)
		text, _ := chunk["text"].(string)
		if text == "discarded" && containsInt(refs, event.Seq) {
			t.Fatalf("failed chunk linked to assistant message: %#v", event)
		}
	}
}

func containsInt(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestRetryDoesNotRetryAuth(t *testing.T) {
	provider := &retryScriptProvider{steps: []func(func(Delta) error) (Completion, error){
		func(func(Delta) error) (Completion, error) {
			return Completion{}, &ProviderError{Code: "AUTH", Message: "bad key"}
		},
	}}
	e, id := retryTestEngine(t, provider, retryPolicyForTest(2))
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}}); err == nil {
		t.Fatal("Run() unexpectedly succeeded")
	}
	provider.mu.Lock()
	attempts := provider.attempts
	provider.mu.Unlock()
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	s, _ := e.getSession(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.Events {
		if event.Type == "llm/retry" {
			t.Fatalf("AUTH scheduled retry: %#v", event)
		}
	}
}

func TestRetryBudgetExhaustion(t *testing.T) {
	provider := &retryScriptProvider{steps: []func(func(Delta) error) (Completion, error){
		func(func(Delta) error) (Completion, error) { return Completion{}, retryError("SERVER", "one") },
		func(func(Delta) error) (Completion, error) { return Completion{}, retryError("SERVER", "two") },
	}}
	e, id := retryTestEngine(t, provider, retryPolicyForTest(1))
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}}); err == nil {
		t.Fatal("Run() unexpectedly succeeded")
	}
	s, _ := e.getSession(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, event := range s.Events {
		if event.Type == "llm/retry" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("retry events = %d, want 1", count)
	}
}

func TestRetryCancellationDuringBackoff(t *testing.T) {
	entered := make(chan struct{})
	provider := &retryScriptProvider{steps: []func(func(Delta) error) (Completion, error){
		func(func(Delta) error) (Completion, error) {
			return Completion{}, retryError("SERVER", "temporary")
		},
	}}
	e, id := retryTestEngine(t, provider, RetryPolicy{
		Mode: RetryNormal, MaxRetries: 2, RetryableCodes: []string{"SERVER"},
		InitialDelay: time.Hour, MaxDelay: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := e.Subscribe(ctx, id)
	go func() {
		for event := range sub {
			if event.Type == "llm/retry" {
				close(entered)
				cancel()
				return
			}
		}
	}()
	done := make(chan error, 1)
	go func() {
		_, err := e.Run(ctx, id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "go"}}})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("retry event was not persisted")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}
	provider.mu.Lock()
	attempts := provider.attempts
	provider.mu.Unlock()
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}
