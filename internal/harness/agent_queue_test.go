package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestCancelAgentClearsQueuedTailAndRunsPostCancelReplacement(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := newQueuedTestProvider()
	e.RegisterProvider(provider)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "cancel-clear-session", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: "queue-test"}); err != nil {
		t.Fatal(err)
	}
	prompt := func(text string) {
		t.Helper()
		if _, promptErr := e.Prompt(t.Context(), id, PromptRequest{SessionID: id, Mode: "queue", Content: []PromptContentPart{{Type: "text", Text: text}}}); promptErr != nil {
			t.Fatal(promptErr)
		}
	}
	prompt("active")
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("active provider request did not start")
	}
	prompt("discarded tail")
	if err := e.CancelAgent(id, AgentCancelCause{Kind: "user"}, CancelAgentOptions{}); err != nil {
		t.Fatal(err)
	}
	prompt("replacement")
	select {
	case request := <-provider.started:
		if request != 2 {
			t.Fatalf("replacement provider request = %d, want 2", request)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement prompt did not replay after cancellation")
	}
	waitCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := e.WaitForIdle(waitCtx, id); err != nil {
		t.Fatal(err)
	}
	requests := provider.requestSnapshot()
	if len(requests) != 2 || requests[1].Messages[len(requests[1].Messages)-1].Content != "replacement" {
		t.Fatalf("provider requests = %#v", requests)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	var canceledSplice bool
	var userTexts []string
	for _, event := range session.Events {
		data, _ := event.Data.(map[string]any)
		if event.Type == "agent/inbox/spliced" && data["target"] == "next-turn" && data["outcome"] == "canceled" && eventInt(data["removedCount"]) == 1 {
			canceledSplice = true
		}
		if event.Type == "user/message" {
			userTexts = append(userTexts, contentValueText(event.Data))
		}
	}
	if !canceledSplice {
		t.Fatalf("missing durable canceled splice: %#v", session.Events)
	}
	if got := strings.Join(userTexts, ","); got != "active,replacement" {
		t.Fatalf("admitted user messages = %q", got)
	}
}

func TestCancelAgentIdleDoesNotLeakOntoNextPrompt(t *testing.T) {
	e := newIntegrationEngine(t)
	if err := e.CancelAgent("missing", AgentCancelCause{Kind: "user"}, CancelAgentOptions{}); err == nil {
		t.Fatal("missing session cancellation succeeded")
	}
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "cancel-idle-session", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CancelAgent(id, AgentCancelCause{Kind: "user"}, CancelAgentOptions{}); err != nil {
		t.Fatal(err)
	}
	output, err := e.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "real prompt"}}})
	if err != nil || output != "real prompt" {
		t.Fatalf("prompt after idle cancel = %q, %v", output, err)
	}
}

func TestCancelAgentFirstTypedCauseWins(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := newQueuedTestProvider()
	e.RegisterProvider(provider)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "cancel-cause-session", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: "queue-test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Prompt(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "active"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("provider request did not start")
	}
	if err := e.CancelAgent(id, AgentCancelCause{Kind: "parent"}, CancelAgentOptions{KeepInbox: true}); err != nil {
		t.Fatal(err)
	}
	if err := e.CancelAgent(id, AgentCancelCause{Kind: "user"}, CancelAgentOptions{KeepInbox: true}); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(t.Context(), time.Second)
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
	for index := len(session.Events) - 1; index >= 0; index-- {
		if session.Events[index].Type != "turn/end" {
			continue
		}
		data, _ := session.Events[index].Data.(map[string]any)
		reason, _ := data["reason"].(map[string]any)
		cause, _ := reason["reason"].(map[string]any)
		if reason["kind"] != "aborted" || cause["kind"] != "parent" {
			t.Fatalf("turn cancellation reason = %#v", reason)
		}
		return
	}
	t.Fatal("missing turn/end")
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

func TestFinishClosedSessionWorkerDoesNotQueueNilClaim(t *testing.T) {
	e := newIntegrationEngine(t)
	done := make(chan promptOutcome, 1)
	session := &Session{pending: []*queuedPrompt{{done: done}}}
	e.finishClosedSessionWorker(session)
	select {
	case outcome := <-done:
		if outcome.err == nil || outcome.err.Error() != "engine-closed" {
			t.Fatalf("closed outcome = %#v", outcome)
		}
	default:
		t.Fatal("pending prompt was not settled")
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
