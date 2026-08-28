package harness

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

type testSessionTitleProvider struct {
	id        string
	automatic SessionTitleAutomaticMode
	started   chan SessionTitleProviderRequest
	release   chan struct{}
	result    string
	generate  func(SessionTitleProviderRequest) SessionTitleProviderResult
}

func (provider *testSessionTitleProvider) ID() string { return provider.id }
func (provider *testSessionTitleProvider) Automatic() SessionTitleAutomaticMode {
	return provider.automatic
}
func (provider *testSessionTitleProvider) Generate(ctx context.Context, request SessionTitleProviderRequest) (SessionTitleProviderResult, error) {
	provider.started <- request
	if provider.release != nil {
		<-provider.release
	}
	if provider.generate != nil {
		return provider.generate(request), nil
	}
	seqs := make([]int, len(request.Messages))
	for index, message := range request.Messages {
		seqs[index] = message.Seq
	}
	return SessionTitleProviderResult{Title: provider.result, MessageSeqs: seqs}, nil
}

func TestSessionTitleAllPromptsRunsOnUnchangedRoute(t *testing.T) {
	engine := newIntegrationEngine(t)
	provider := &testSessionTitleProvider{
		id: "all-title", automatic: SessionTitleAllPrompts,
		started: make(chan SessionTitleProviderRequest, 2),
		generate: func(request SessionTitleProviderRequest) SessionTitleProviderResult {
			seqs := make([]int, len(request.Messages))
			for index, message := range request.Messages {
				seqs[index] = message.Seq
			}
			return SessionTitleProviderResult{Title: "Updated title", MessageSeqs: seqs}
		},
	}
	dispose, err := engine.RegisterSessionTitleProvider(provider)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dispose() })
	id, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "all-title-provider", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, prompt := range []string{"first prompt", "second prompt"} {
		if _, err := engine.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: prompt}}}); err != nil {
			t.Fatal(err)
		}
		request := <-provider.started
		want := 1
		if prompt == "second prompt" {
			want = 2
		}
		if len(request.Messages) != want || request.Route == nil || request.Route.Provider != "echo" {
			t.Fatalf("revision request = %#v", request)
		}
		waitSessionTitle(t, engine, id, func(snapshot SessionTitleSnapshot) bool {
			return snapshot.Source.Kind == "provider" && len(snapshot.MessageSeqs) == want
		})
	}
}

func TestApplyRuntimeConfigReconcilesSessionTitleProvider(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if e.titleProvider != nil {
		t.Fatal("title provider unexpectedly enabled")
	}
	next := cfg
	next.SessionTitleLLM = defaultSessionTitleLLMConfig()
	if err := e.ApplyRuntimeConfig(next); err != nil {
		t.Fatal(err)
	}
	e.titleMu.Lock()
	if e.titleProvider == nil {
		e.titleMu.Unlock()
		t.Fatal("title provider was not mounted")
	}
	e.titleMu.Unlock()
	if err := e.ApplyRuntimeConfig(cfg); err != nil {
		t.Fatal(err)
	}
	e.titleMu.Lock()
	deferred := e.titleProvider
	e.titleMu.Unlock()
	if deferred != nil {
		t.Fatal("disabled title provider survived runtime update")
	}
}

func waitSessionTitle(t *testing.T, engine *Engine, id string, predicate func(SessionTitleSnapshot) bool) SessionTitleSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, ok, err := engine.SessionTitle(id)
		if err != nil {
			t.Fatal(err)
		}
		if ok && predicate(snapshot) {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for session title")
	return SessionTitleSnapshot{}
}

func TestSessionTitleProviderAutomaticRouteAndUserSupersession(t *testing.T) {
	engine := newIntegrationEngine(t)
	provider := &testSessionTitleProvider{
		id: "custom-title", automatic: SessionTitleFirstPrompt,
		started: make(chan SessionTitleProviderRequest, 1), release: make(chan struct{}), result: "late provider title",
	}
	dispose, err := engine.RegisterSessionTitleProvider(provider)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dispose() })
	id, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "title-provider", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "Build provider-backed titles"}}}); err != nil {
		t.Fatal(err)
	}
	request := <-provider.started
	if request.Route == nil || request.Route.Provider != "echo" || request.Route.Model != "echo" {
		t.Fatalf("title route = %#v", request.Route)
	}
	if len(request.Messages) != 1 || request.Messages[0].Text != "Build provider-backed titles" {
		t.Fatalf("title messages = %#v", request.Messages)
	}
	if _, _, err := engine.RenameSession(id, "  User\tchoice  "); err != nil {
		t.Fatal(err)
	}
	close(provider.release)
	snapshot := waitSessionTitle(t, engine, id, func(snapshot SessionTitleSnapshot) bool { return snapshot.Source.Kind == "user" })
	if snapshot.Title != "User choice" || len(snapshot.MessageSeqs) != 0 {
		t.Fatalf("renamed title = %#v", snapshot)
	}
	time.Sleep(10 * time.Millisecond)
	latest, _, err := engine.SessionTitle(id)
	if err != nil || latest.Source.Kind != "user" || latest.Title != "User choice" {
		t.Fatalf("late provider overwrote user title: %#v err=%v", latest, err)
	}
}

type titleLLMCaptureProvider struct {
	mu       sync.Mutex
	requests []ChatRequest
}

func (*titleLLMCaptureProvider) ID() string   { return "title-llm-capture" }
func (*titleLLMCaptureProvider) Name() string { return "Title LLM Capture" }
func (*titleLLMCaptureProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "capture", Name: "Capture"}}, nil
}
func (provider *titleLLMCaptureProvider) Complete(ctx context.Context, request ChatRequest, onDelta func(Delta) error) (Completion, error) {
	if err := ctx.Err(); err != nil {
		return Completion{}, err
	}
	provider.mu.Lock()
	provider.requests = append(provider.requests, request)
	provider.mu.Unlock()
	text := "main response"
	if strings.HasPrefix(request.System, "Create a concise title") {
		text = "Concise generated title"
	}
	if err := onDelta(Delta{Text: text, Finish: "stop"}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: text, Finish: "stop"}, nil
}

func TestDefaultSessionTitleLLMRecordsAndDispatchesAuxiliaryRequest(t *testing.T) {
	provider := &titleLLMCaptureProvider{}
	config := DefaultConfig()
	config.DataDir = t.TempDir()
	config.Workspace = config.DataDir
	config.Provider = provider.ID()
	config.Model = "capture"
	config.Persist = false
	engine, err := New(WithConfig(config))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	engine.RegisterProvider(provider)
	id, err := engine.CreateSession(t.Context(), config.Workspace, "default-title-llm", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "Explain the build failure"}}}); err != nil {
		t.Fatal(err)
	}
	snapshot := waitSessionTitle(t, engine, id, func(snapshot SessionTitleSnapshot) bool { return snapshot.Source.Kind == "provider" })
	if snapshot.Title != "Concise generated title" || snapshot.Source.Provider != "session-title-first-prompt-llm" {
		t.Fatalf("generated title = %#v", snapshot)
	}
	if snapshot.Source.Model == nil || snapshot.Source.Model.Provider != provider.ID() || snapshot.Source.Model.Model != "capture" {
		t.Fatalf("generated title model = %#v", snapshot.Source.Model)
	}
	session := mustSession(t, engine, id)
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	foundRequest := false
	for _, event := range events {
		if event.Type == "session/title-llm-request" {
			foundRequest = true
			break
		}
	}
	if !foundRequest {
		t.Fatal("session/title-llm-request was not recorded")
	}
	provider.mu.Lock()
	requests := append([]ChatRequest(nil), provider.requests...)
	provider.mu.Unlock()
	foundAuxiliary := false
	for _, request := range requests {
		if strings.HasPrefix(request.System, "Create a concise title") {
			foundAuxiliary = request.Thinking == "disabled" && request.MaxTokens == 64 && len(request.Tools) == 0
		}
	}
	if !foundAuxiliary {
		t.Fatalf("auxiliary request policy = %#v", requests)
	}
}
