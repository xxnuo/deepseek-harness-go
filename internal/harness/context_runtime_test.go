package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type contextStepProvider struct {
	mu       sync.Mutex
	requests []ChatRequest
}

func (p *contextStepProvider) ID() string   { return "context-step" }
func (p *contextStepProvider) Name() string { return "Context Step" }
func (p *contextStepProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "context-model", Name: "Context Model"}}, nil
}
func (p *contextStepProvider) Complete(_ context.Context, request ChatRequest, _ func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	call := len(p.requests)
	p.mu.Unlock()
	if call == 1 {
		return Completion{Finish: "tool_calls", ToolCalls: []ToolCall{{ID: "context-call", Name: "context_step", Arguments: json.RawMessage(`{}`)}}}, nil
	}
	return Completion{Text: "done", Finish: "stop"}, nil
}
func (p *contextStepProvider) snapshot() []ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...)
}

func newContextTestEngine(t *testing.T, provider Provider, configure func(*Config)) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = t.TempDir(), t.TempDir()
	cfg.Provider, cfg.Persist = provider.ID(), false
	models, err := provider.Models(context.Background())
	if err != nil || len(models) == 0 {
		t.Fatalf("provider models = %#v, %v", models, err)
	}
	cfg.Model = models[0].ID
	cfg.SessionTitleLLM.Enabled = false
	if configure != nil {
		configure(&cfg)
	}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	engine.RegisterProvider(provider)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func TestRPCPromptPersistsCanonicalRequestProvenanceAndTimeContext(t *testing.T) {
	provider := &promptCaptureProvider{}
	engine := newContextTestEngine(t, provider, func(config *Config) {
		config.TimeContext = &TimeContextConfig{TimeZone: "UTC"}
	})
	id, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "context-rpc", "")
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"sessionId": id, "mode": "queue", "clientTimeZone": "US/Pacific",
		"content": []map[string]any{{"type": "text", "text": "when is noon?"}},
	})
	if _, rpcErr := engine.dispatch(context.Background(), "session.prompt", payload, "rpc-zone"); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if err := engine.WaitForIdle(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	userSeq, timeSeq, headerSeq := -1, -1, -1
	for _, event := range events {
		if event.Type == "request/header" {
			headerSeq = event.Seq
		}
		if event.Type != "user/message" {
			continue
		}
		message := nestedMessage(event.Data)
		source, _ := message["source"].(map[string]any)
		if source["kind"] == "user" {
			userSeq = event.Seq
			if source["rpcId"] != "rpc-zone" || source["clientTimeZone"] != "America/Los_Angeles" {
				t.Fatalf("user source = %#v", source)
			}
		}
		if source["plugin"] == "time-context" {
			timeSeq = event.Seq
			text := contentValueText(message["content"])
			if !strings.Contains(text, "Browser time zone for this request: America/Los_Angeles.") || !strings.Contains(text, "turn 1, step 1") {
				t.Fatalf("time context = %q", text)
			}
		}
	}
	if userSeq < 0 || timeSeq <= userSeq || headerSeq <= timeSeq {
		t.Fatalf("event order user=%d time=%d header=%d", userSeq, timeSeq, headerSeq)
	}
	requests := provider.snapshot()
	if len(requests) != 1 || !strings.Contains(requests[0].Messages[len(requests[0].Messages)-1].Content, "turn 1, step 1") {
		t.Fatalf("provider messages = %#v", requests)
	}
	before := len(events)
	invalid, _ := json.Marshal(map[string]any{
		"sessionId": id, "mode": "queue", "clientTimeZone": "",
		"content": []map[string]any{{"type": "text", "text": "invalid"}},
	})
	if _, rpcErr := engine.dispatch(context.Background(), "session.prompt", invalid, "rpc-invalid"); rpcErr == nil || rpcErr.Code != "invalid-time-zone" {
		t.Fatalf("invalid zone error = %#v", rpcErr)
	}
	session.mu.Lock()
	after := len(session.Events)
	session.mu.Unlock()
	if after != before {
		t.Fatalf("invalid prompt appended events: before=%d after=%d", before, after)
	}
}

func TestTimeContextRunsAtEachEligibleStepAndHonorsRefresh(t *testing.T) {
	for _, test := range []struct {
		name     string
		refresh  time.Duration
		contexts int
	}{{name: "each step", contexts: 2}, {name: "refresh floor", refresh: time.Hour, contexts: 1}} {
		t.Run(test.name, func(t *testing.T) {
			provider := &contextStepProvider{}
			engine := newContextTestEngine(t, provider, func(config *Config) {
				config.TimeContext = &TimeContextConfig{TimeZone: "UTC", RefreshInterval: test.refresh}
			})
			if err := engine.RegisterTool(Tool{
				Schema:  ToolSchema{Name: "context_step", Parameters: objectSchema(map[string]any{})},
				Execute: func(context.Context, ToolCall) (ToolResult, error) { return textToolResult("ok"), nil },
			}); err != nil {
				t.Fatal(err)
			}
			id, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "context-steps-"+strings.ReplaceAll(test.name, " ", "-"), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := engine.Run(context.Background(), id, PromptRequest{
				Content: []PromptContentPart{{Type: "text", Text: "run"}}, RPCID: "rpc-step", ClientTimeZone: "UTC",
			}); err != nil {
				t.Fatal(err)
			}
			session, _ := engine.getSession(id)
			session.mu.Lock()
			events := append([]Event(nil), session.Events...)
			session.mu.Unlock()
			var texts []string
			for _, event := range events {
				if isPluginMessage(event, "time-context") {
					texts = append(texts, contentValueText(nestedMessage(event.Data)["content"]))
				}
			}
			if len(texts) != test.contexts || !strings.Contains(texts[0], "turn 1, step 1") {
				t.Fatalf("time contexts = %#v", texts)
			}
			if strings.Contains(texts[0], "preceding model-visible message: unavailable") {
				t.Fatalf("first-step baseline ignored the current user message: %q", texts[0])
			}
			if test.contexts == 2 && !strings.Contains(texts[1], "turn 1, step 2") {
				t.Fatalf("second context = %q", texts[1])
			}
			if requests := provider.snapshot(); len(requests) != 2 {
				t.Fatalf("provider requests = %d", len(requests))
			}
		})
	}
}

func TestTimeContextRetainsSystemZoneResolvedAtEngineCreation(t *testing.T) {
	t.Setenv("TZ", "Asia/Shanghai")
	provider := &promptCaptureProvider{}
	engine := newContextTestEngine(t, provider, func(config *Config) {
		config.TimeContext = &TimeContextConfig{}
	})
	t.Setenv("TZ", "America/New_York")
	id, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "context-system-zone", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "what time is it?"}}}); err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if !isPluginMessage(event, "time-context") {
			continue
		}
		text := contentValueText(nestedMessage(event.Data)["content"])
		if !strings.Contains(text, "[Asia/Shanghai]") {
			t.Fatalf("time context did not retain its startup zone: %q", text)
		}
		return
	}
	t.Fatal("missing time-context reading")
}

func TestStepContextsSkipCanceledPreparation(t *testing.T) {
	provider := &promptCaptureProvider{}
	engine := newContextTestEngine(t, provider, func(config *Config) {
		config.TimeContext = &TimeContextConfig{TimeZone: "UTC"}
	})
	id, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "context-canceled", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.appendStepContexts(ctx, session, 1, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("appendStepContexts error = %v", err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if isPluginMessage(event, "time-context") || isPluginMessage(event, "tmux-context") {
			t.Fatalf("canceled preparation persisted context: %#v", event)
		}
	}
}

func TestPromptSessionReferencesAreSnapshottedAndPersistedAfterMessage(t *testing.T) {
	provider := &promptCaptureProvider{}
	engine := newContextTestEngine(t, provider, nil)
	sourceID, _ := engine.CreateSession(context.Background(), engine.Config().Workspace, "reference-source-prompt", "")
	targetID, _ := engine.CreateSession(context.Background(), engine.Config().Workspace, "reference-target-prompt", "")
	source, _ := engine.getSession(sourceID)
	if _, err := engine.appendEvent(source, "user/message", map[string]any{
		"id": "source-message", "role": "user", "content": []ContentBlock{{Type: "text", Text: "source fact"}},
		"source": map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), targetID, PromptRequest{
		Content: []PromptContentPart{{Type: "text", Text: "use " + FormatSessionReferenceMention(SessionReferenceInput{
			SessionID: sourceID, Label: "source",
		})}},
	}); err != nil {
		t.Fatal(err)
	}
	target, _ := engine.getSession(targetID)
	target.mu.Lock()
	events := append([]Event(nil), target.Events...)
	target.mu.Unlock()
	referenceSeq, userSeq := -1, -1
	for _, event := range events {
		if event.Type != "user/message" {
			continue
		}
		message := nestedMessage(event.Data)
		source, _ := message["source"].(map[string]any)
		if source["kind"] == "session-reference" {
			referenceSeq = event.Seq
			if !strings.Contains(contentValueText(message["content"]), "source fact") {
				t.Fatalf("reference content = %#v", message["content"])
			}
		} else if source["kind"] == "user" {
			userSeq = event.Seq
		}
	}
	if userSeq < 0 || referenceSeq <= userSeq {
		t.Fatalf("reference/user order = %d/%d", referenceSeq, userSeq)
	}
	requests := provider.snapshot()
	if len(requests) != 1 || len(requests[0].Messages) < 2 || requests[0].Messages[0].Content != "use @source" || !strings.Contains(requests[0].Messages[1].Content, "source fact") {
		t.Fatalf("provider messages = %#v", requests)
	}
}

func TestPromptRejectsMalformedCanonicalSessionMentionBeforeAdmission(t *testing.T) {
	provider := &promptCaptureProvider{}
	engine := newContextTestEngine(t, provider, nil)
	targetID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "reference-malformed-target", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Run(context.Background(), targetID, PromptRequest{
		Content: []PromptContentPart{{Type: "text", Text: "@[bad](dsh-session:not-canonical)"}},
	})
	if !IsSessionReferenceError(err, SessionReferenceInvalidReference) {
		t.Fatalf("malformed mention error = %#v", err)
	}
	if requests := provider.snapshot(); len(requests) != 0 {
		t.Fatalf("malformed mention reached provider: %#v", requests)
	}
	target, _ := engine.getSession(targetID)
	target.mu.Lock()
	defer target.mu.Unlock()
	for _, event := range target.Events {
		if event.Type == "user/message" {
			t.Fatalf("malformed mention was admitted: %#v", event)
		}
	}
}

type tmuxHelperResult struct {
	Texts []string `json:"texts"`
}

func TestTmuxContextRealProcess(t *testing.T) {
	if os.Getenv("DSH_TMUX_CONTEXT_HELPER") == "1" {
		runTmuxContextHelper(t)
		return
	}
	if runtime.GOOS == "windows" {
		t.Skip("tmux is unavailable on Windows")
	}
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux is not installed")
	}
	resultPath := filepath.Join(t.TempDir(), "result.json")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	sessionName := fmt.Sprintf("dsh-go-test-%d-%d", os.Getpid(), time.Now().UnixNano())
	command := strings.Join([]string{
		"env", "DSH_TMUX_CONTEXT_HELPER=1", "DSH_TMUX_CONTEXT_RESULT=" + shellTestQuote(resultPath),
		shellTestQuote(executable), "-test.run", shellTestQuote("^TestTmuxContextRealProcess$"),
	}, " ")
	started := exec.Command("tmux", "new-session", "-d", "-s", sessionName, command)
	if output, err := started.CombinedOutput(); err != nil {
		t.Fatalf("start tmux helper: %v: %s", err, output)
	}
	t.Cleanup(func() { _ = exec.Command("tmux", "kill-session", "-t", sessionName).Run() })
	deadline := time.Now().Add(15 * time.Second)
	for {
		data, err := os.ReadFile(resultPath)
		if err == nil {
			var result tmuxHelperResult
			if err := json.Unmarshal(data, &result); err != nil {
				t.Fatal(err)
			}
			if len(result.Texts) != 2 || !strings.Contains(result.Texts[0], "tmux location (turn 1):") || !strings.Contains(result.Texts[0], "pane ") ||
				!strings.Contains(result.Texts[1], "tmux location (turn 2):") || !strings.Contains(result.Texts[1], `"dsh<&context"`) {
				t.Fatalf("tmux result = %#v", result)
			}
			return
		}
		if time.Now().After(deadline) {
			pane, _ := exec.Command("tmux", "capture-pane", "-p", "-t", sessionName).CombinedOutput()
			t.Fatalf("tmux helper timed out: %s", pane)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func runTmuxContextHelper(t *testing.T) {
	provider := &promptCaptureProvider{}
	engine := newContextTestEngine(t, provider, func(config *Config) {
		config.TmuxContext = &TmuxContextConfig{}
	})
	id, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "tmux-helper", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "where am I?"}}}); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("tmux", "rename-window", "-t", os.Getenv("TMUX_PANE"), "dsh<&context").CombinedOutput(); err != nil {
		t.Fatalf("rename tmux window: %v: %s", err, output)
	}
	for _, prompt := range []string{"where did I move?", "has anything changed?"} {
		if _, err := engine.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: prompt}}}); err != nil {
			t.Fatal(err)
		}
	}
	session, _ := engine.getSession(id)
	session.mu.Lock()
	var result tmuxHelperResult
	for _, event := range session.Events {
		if isPluginMessage(event, "tmux-context") {
			result.Texts = append(result.Texts, contentValueText(nestedMessage(event.Data)["content"]))
		}
	}
	session.mu.Unlock()
	wire, _ := json.Marshal(result)
	if err := os.WriteFile(os.Getenv("DSH_TMUX_CONTEXT_RESULT"), wire, 0o600); err != nil {
		t.Fatal(err)
	}
}
