package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

type runtimePolicyProvider struct {
	mu           sync.Mutex
	requests     []ChatRequest
	overflowOnce bool
	mainCalls    int
}

func (p *runtimePolicyProvider) ID() string   { return "deepseek-official" }
func (p *runtimePolicyProvider) Name() string { return "Runtime Policy Test" }
func (p *runtimePolicyProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "deepseek-v4-flash", Name: "DeepSeek", ContextWindow: 30000}}, nil
}
func (p *runtimePolicyProvider) Complete(_ context.Context, request ChatRequest, onDelta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	compaction := len(request.Messages) > 0 && request.Messages[len(request.Messages)-1].Content == compactionInstruction
	if !compaction {
		p.mainCalls++
	}
	mainCall := p.mainCalls
	p.mu.Unlock()
	if compaction {
		return Completion{Text: "durable checkpoint", Finish: "stop"}, nil
	}
	if p.overflowOnce && mainCall == 1 {
		if err := onDelta(Delta{Text: "discarded partial"}); err != nil {
			return Completion{}, err
		}
		return Completion{}, &ProviderError{Code: "CONTEXT_WINDOW_EXCEEDED", Message: "maximum context length exceeded"}
	}
	if err := onDelta(Delta{Text: "done"}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: "done", Finish: "stop"}, nil
}

func (p *runtimePolicyProvider) snapshot() ([]ChatRequest, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...), p.mainCalls
}

func newRuntimePolicyEngine(t *testing.T, provider *runtimePolicyProvider, contextWindow int) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = provider.ID()
	cfg.Model = "deepseek-v4-flash"
	cfg.Persist = false
	cfg.SessionTitleLLM.Enabled = false
	cfg.Compaction = CompactionConfig{
		ThresholdRatio: 0.8, RetainTokens: 100, MaxTokens: 256,
		CompactionRetries: 1, MaxOverflowRetries: 1,
	}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	e.mu.Lock()
	e.settings["llm-deepseek"] = map[string]any{"models": []any{map[string]any{
		"id": "deepseek-v4-flash", "name": "DeepSeek", "contextWindow": contextWindow,
	}}}
	e.mu.Unlock()
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func seedRuntimePolicyHistory(t *testing.T, e *Engine, id string, pairs, chars int) {
	t.Helper()
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < pairs; index++ {
		text := strings.Repeat(string(rune('a'+index)), chars)
		if _, err := e.appendEvent(s, "user/message", map[string]any{
			"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: text}},
			"source": map[string]any{"kind": "user"},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.appendEvent(s, "assistant/message", map[string]any{
			"turn": 0, "step": index + 1,
			"message": map[string]any{"id": newID("msg"), "role": "assistant", "content": []ContentBlock{{Type: "text", Text: "ack"}}, "source": map[string]any{"kind": "model"}},
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAutomaticPressureCompactionRunsBeforeStep(t *testing.T) {
	provider := &runtimePolicyProvider{}
	e := newRuntimePolicyEngine(t, provider, 30000)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	seedRuntimePolicyHistory(t, e, id, 3, 50000)
	text, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "continue"}}})
	if err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v", text, err)
	}
	requests, mainCalls := provider.snapshot()
	if len(requests) < 2 || requests[0].Messages[len(requests[0].Messages)-1].Content != compactionInstruction || mainCalls != 1 {
		t.Fatalf("provider requests = %d, main calls = %d; want compaction before one main call", len(requests), mainCalls)
	}
	foundCheckpoint := false
	for _, message := range requests[len(requests)-1].Messages {
		foundCheckpoint = foundCheckpoint || strings.Contains(message.Content, "durable checkpoint")
	}
	if !foundCheckpoint {
		t.Fatalf("main request did not rebuild from compacted surface: %#v", requests[len(requests)-1].Messages)
	}
}

func TestContextOverflowCompactsAndRetriesSameStep(t *testing.T) {
	provider := &runtimePolicyProvider{overflowOnce: true}
	e := newRuntimePolicyEngine(t, provider, 1000000)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	seedRuntimePolicyHistory(t, e, id, 2, 5000)
	text, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "continue"}}})
	if err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v", text, err)
	}
	_, mainCalls := provider.snapshot()
	if mainCalls != 2 {
		t.Fatalf("main calls = %d, want overflow plus retry", mainCalls)
	}
	s, _ := e.getSession(id)
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	steps := 0
	var assistant Event
	for _, event := range events {
		if event.Type == "step/start" {
			steps++
		}
		if event.Type == "assistant/message" {
			eventTurnValue, ok := eventTurn(event.Data)
			if ok && eventTurnValue == 1 {
				assistant = event
			}
		}
	}
	if steps != 1 {
		t.Fatalf("step/start count = %d, want 1", steps)
	}
	if len(assistant.SourceEventSeqs) != 1 {
		t.Fatalf("assistant sources = %#v, want only successful retry chunk", assistant.SourceEventSeqs)
	}
	chunk, _ := events[assistant.SourceEventSeqs[0]].Data.(map[string]any)["chunk"].(map[string]any)
	if chunk["text"] != "done" {
		t.Fatalf("assistant source chunk = %#v", chunk)
	}
}

func TestToolResultPrunerPreservesUnicodeEdges(t *testing.T) {
	input := strings.Repeat("甲乙丙丁戊己庚辛", 12)
	blocks, changed := pruneToolResultContent([]ContentBlock{{Type: "text", Text: input}}, ToolResultPruneConfig{
		ThresholdChars: 64, HeadChars: 8, TailChars: 8,
	})
	if !changed || len(blocks) != 1 || !utf8.ValidString(blocks[0].Text) {
		t.Fatalf("pruned blocks = %#v, changed = %v", blocks, changed)
	}
	if !strings.HasPrefix(blocks[0].Text, string([]rune(input)[:8])) || !strings.HasSuffix(blocks[0].Text, string([]rune(input)[len([]rune(input))-8:])) || strings.Count(blocks[0].Text, toolResultPruneMarker) != 1 {
		t.Fatalf("unicode edges were not preserved: %q", blocks[0].Text)
	}
}

func TestSpillSavesFullTextAndFailureKeepsOriginal(t *testing.T) {
	provider := &runtimePolicyProvider{}
	e := newRuntimePolicyEngine(t, provider, 30000)
	e.cfg.Spill = SpillConfig{Root: filepath.Join(t.TempDir(), "spill"), MaxInlineBytes: 256}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	body := strings.Repeat("完整结果", 200)
	original := ToolResult{Content: []ContentBlock{{Type: "text", Text: body}}}
	spilled := e.applySpillPolicy(s, ToolCall{Name: "grep"}, original)
	if len(spilled.Content) != 1 || spilled.Content[0].Text == body || len([]byte(spilled.Content[0].Text)) > e.cfg.Spill.MaxInlineBytes {
		t.Fatalf("spill replacement = %#v", spilled.Content)
	}
	const prefix = "Full formatted result stored at: "
	start := strings.Index(spilled.Content[0].Text, prefix)
	if start < 0 {
		t.Fatalf("spill notice = %q", spilled.Content[0].Text)
	}
	end := strings.Index(spilled.Content[0].Text[start+len(prefix):], ". Use read")
	if end < 0 {
		t.Fatalf("spill notice = %q", spilled.Content[0].Text)
	}
	path := spilled.Content[0].Text[start+len(prefix) : start+len(prefix)+end]
	stored, err := os.ReadFile(path)
	if err != nil || string(stored) != body {
		t.Fatalf("stored spill = %d bytes, %v", len(stored), err)
	}
	badRoot := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(badRoot, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.cfg.Spill.Root = badRoot
	failed := e.applySpillPolicy(s, ToolCall{Name: "grep"}, original)
	if len(failed.Content) != 1 || failed.Content[0].Text != body {
		t.Fatalf("failed spill changed result: %#v", failed)
	}
}

func TestRepeatToolReminderThresholdsAndUserReset(t *testing.T) {
	provider := &runtimePolicyProvider{}
	e := newRuntimePolicyEngine(t, provider, 30000)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	call := ToolCall{Name: "read", Arguments: json.RawMessage(`{"b":2,"a":1}`)}
	for count := 1; count <= 8; count++ {
		if count == 2 {
			call.Arguments = json.RawMessage(`{"a":1,"b":2}`)
		}
		if err := e.appendRepeatToolReminder(s, call); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.appendEvent(s, "user/message", map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: "new direction"}},
		"source": map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}
	for count := 0; count < 3; count++ {
		if err := e.appendRepeatToolReminder(s, call); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var summaries []string
	for _, event := range s.Events {
		if event.Type != "user/message" {
			continue
		}
		source, _ := nestedMessage(event.Data)["source"].(map[string]any)
		if source["plugin"] == "repeat-tool-reminder" {
			summaries = append(summaries, stringValue(source["summary"]))
		}
	}
	if strings.Join(summaries, ",") != "read x 3,read x 5,read x 8,read x 3" {
		t.Fatalf("reminders = %v", summaries)
	}
}

func TestRepeatToolReminderIncludeExcludeWildcards(t *testing.T) {
	if !repeatToolTracked("mcp_search", []string{"mcp_*"}, nil) {
		t.Fatal("include wildcard did not match")
	}
	if repeatToolTracked("bash", []string{"mcp_*"}, nil) {
		t.Fatal("include wildcard matched unrelated tool")
	}
	if repeatToolTracked("mcp_secret", nil, []string{"mcp_*"}) {
		t.Fatal("exclude wildcard did not filter tool")
	}
}
