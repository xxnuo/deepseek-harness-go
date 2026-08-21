package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type queuedTestProvider struct {
	mu        sync.Mutex
	requests  []ChatRequest
	started   chan int
	firstGate chan struct{}
}

func newQueuedTestProvider() *queuedTestProvider {
	return &queuedTestProvider{
		started:   make(chan int, 8),
		firstGate: make(chan struct{}),
	}
}

func (p *queuedTestProvider) ID() string   { return "queue-test" }
func (p *queuedTestProvider) Name() string { return "Queue Test" }
func (p *queuedTestProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "queue-test", Name: "Queue Test"}}, nil
}
func (p *queuedTestProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	index := len(p.requests)
	p.mu.Unlock()
	p.started <- index
	if index == 1 {
		select {
		case <-p.firstGate:
		case <-ctx.Done():
			return Completion{}, ctx.Err()
		}
	}
	text := ""
	if len(req.Messages) > 0 {
		text = req.Messages[len(req.Messages)-1].Content
	}
	if err := onDelta(Delta{Text: text}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: text}, nil
}

func (p *queuedTestProvider) requestSnapshot() []ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...)
}

func TestPromptQueuesDistinctTurns(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := newQueuedTestProvider()
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "queue-session", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: "queue-test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Prompt(context.Background(), id, PromptRequest{SessionID: id, Mode: "queue", Content: []PromptContentPart{{Type: "text", Text: "first"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-provider.started:
		if got != 1 {
			t.Fatalf("first provider call = %d, want 1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("first provider call did not start")
	}
	if _, err := e.Prompt(context.Background(), id, PromptRequest{SessionID: id, Mode: "queue", Content: []PromptContentPart{{Type: "text", Text: "second"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-provider.started:
		t.Fatalf("provider call %d started before first turn released", got)
	case <-time.After(30 * time.Millisecond):
	}
	close(provider.firstGate)
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.WaitForIdle(waitCtx, id); err != nil {
		t.Fatal(err)
	}
	requests := provider.requestSnapshot()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(requests))
	}
	if len(requests[0].Messages) != 1 || requests[0].Messages[0].Content != "first" {
		t.Fatalf("first request messages = %#v, want only first prompt", requests[0].Messages)
	}
	if len(requests[1].Messages) != 3 || requests[1].Messages[0].Content != "first" || requests[1].Messages[2].Content != "second" {
		t.Fatalf("second request messages = %#v, want first turn plus second prompt", requests[1].Messages)
	}

	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	var turns []int
	for _, event := range session.Events {
		if event.Type != "turn/start" {
			continue
		}
		if data, ok := event.Data.(map[string]any); ok {
			if turn, ok := data["turn"].(int); ok {
				turns = append(turns, turn)
			}
		}
	}
	if len(turns) != 2 || turns[0] != 1 || turns[1] != 2 {
		t.Fatalf("turn starts = %#v, want [1 2]", turns)
	}
}

func TestCancelPreservesQueuedPrompt(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := newQueuedTestProvider()
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "cancel-session", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: "queue-test"}); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"cancel me", "run after cancel"} {
		if _, err := e.Prompt(context.Background(), id, PromptRequest{SessionID: id, Mode: "queue", Content: []PromptContentPart{{Type: "text", Text: text}}}); err != nil {
			t.Fatal(err)
		}
		if text == "cancel me" {
			select {
			case <-provider.started:
			case <-time.After(time.Second):
				t.Fatal("first provider call did not start")
			}
		}
	}
	if err := e.CancelSession(id); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-provider.started:
		if got != 2 {
			t.Fatalf("second provider call = %d, want 2", got)
		}
	case <-time.After(time.Second):
		t.Fatal("queued prompt did not resume after cancellation")
	}
	close(provider.firstGate)
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.WaitForIdle(waitCtx, id); err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	var aborted, assistant int
	for _, event := range session.Events {
		switch event.Type {
		case "assistant/message":
			assistant++
		case "turn/end":
			if data, ok := event.Data.(map[string]any); ok {
				if reason, ok := data["reason"].(map[string]any); ok && reason["kind"] == "aborted" {
					aborted++
				}
			}
		}
	}
	if aborted != 1 || assistant != 1 {
		t.Fatalf("cancelled turn/assistant counts = %d/%d, want 1/1", aborted, assistant)
	}
}

func TestPromptRejectsMismatchedSessionID(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "session-id-check", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Prompt(context.Background(), id, PromptRequest{SessionID: "other", Mode: "queue", Content: []PromptContentPart{{Type: "text", Text: "x"}}})
	if err == nil || err.Error() != "bad-request: sessionId does not match target session" {
		t.Fatalf("mismatched session id error = %v", err)
	}
}

func TestPersistedInboxPromptRunsAfterSessionAttach(t *testing.T) {
	root := t.TempDir()
	workspace := t.TempDir()
	sessions := filepath.Join(root, "sessions")
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatal(err)
	}
	log := `{"type":"session","version":0,"id":"durable-queue","createdAt":1,"cwd":` + fmt.Sprintf("%q", workspace) + `}` + "\n" +
		`{"type":"agent/inbox/spliced","seq":0,"time":1,"data":{"target":"next-turn","start":0,"inserted":[{"id":"msg-durable","role":"user","content":[{"type":"text","text":"restored prompt"}],"source":{"kind":"user"}}]}}` + "\n"
	if err := os.WriteFile(filepath.Join(sessions, "durable-queue.jsonl"), []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}
	e, err := New(WithDataDir(root), WithWorkspace(workspace), WithPersistence(true), WithProvider("echo"), WithModel("echo"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if _, err := e.CreateSession(context.Background(), workspace, "durable-queue", ""); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.WaitForIdle(waitCtx, "durable-queue"); err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession("durable-queue")
	s.mu.Lock()
	defer s.mu.Unlock()
	var claimed, answered bool
	for _, event := range s.Events {
		if event.Type == "agent/inbox/spliced" {
			data, _ := event.Data.(map[string]any)
			claimed = claimed || data["removedCount"] == 1
		}
		if event.Type == "assistant/message" && contentValueText(event.Data) == "restored prompt" {
			answered = true
		}
	}
	if !claimed || !answered {
		t.Fatalf("restored queue claimed/answered = %v/%v", claimed, answered)
	}
}
