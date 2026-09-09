package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

type routedCompactionProvider struct {
	id, model   string
	context     int
	summaryText string
	mu          sync.Mutex
	requests    []ChatRequest
}

func (p *routedCompactionProvider) ID() string   { return p.id }
func (p *routedCompactionProvider) Name() string { return p.id }
func (p *routedCompactionProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: p.model, Name: p.model, ContextWindow: p.context}}, nil
}
func (p *routedCompactionProvider) ResolveModelInfo(_ context.Context, model string) (ModelInfo, error) {
	if model != p.model {
		return ModelInfo{}, errors.New("model unavailable")
	}
	return ModelInfo{ID: model, Name: model, ContextWindow: p.context}, nil
}
func (p *routedCompactionProvider) Complete(_ context.Context, request ChatRequest, onDelta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	p.mu.Unlock()
	text := "done"
	if p.summaryText != "" {
		text = p.summaryText
	}
	if err := onDelta(Delta{Text: text, Finish: "stop"}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: text, Finish: "stop"}, nil
}

func (p *routedCompactionProvider) snapshot() []ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...)
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
		if _, err := e.appendEvent(s, "step/start", map[string]any{"turn": 0, "step": index + 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.appendEvent(s, "assistant/message", map[string]any{
			"turn": 0, "step": index + 1,
			"message": map[string]any{"id": newID("msg"), "role": "assistant", "content": []ContentBlock{{Type: "text", Text: "ack"}}, "source": map[string]any{"kind": "model"}},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": 0, "step": index + 1}); err != nil {
			t.Fatal(err)
		}
	}
}

func seedRuntimePolicyRoute(t *testing.T, e *Engine, id, provider, model string) {
	t.Helper()
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "request/header", map[string]any{
		"header": map[string]any{"config": map[string]any{"provider": provider, "model": model}},
		"reason": "initial",
	}); err != nil {
		t.Fatal(err)
	}
}

func newPresetCompactionEngine(t *testing.T, composition, providerID, model string, providers ...Provider) (*Engine, string) {
	t.Helper()
	root := t.TempDir()
	presetDir := filepath.Join(root, "compact")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.Provider, cfg.Model = providerID, model
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	for _, provider := range providers {
		e.RegisterProvider(provider)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "", "compact")
	if err != nil {
		t.Fatal(err)
	}
	return e, id
}

func TestAutomaticPressureCompactionRunsBeforeStep(t *testing.T) {
	provider := &runtimePolicyProvider{}
	e := newRuntimePolicyEngine(t, provider, 30000)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	seedRuntimePolicyHistory(t, e, id, 3, 50000)
	seedRuntimePolicyRoute(t, e, id, provider.ID(), "deepseek-v4-flash")
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

func TestTokenMeasurementUsesProviderAnchorAndSignedSurfaceDelta(t *testing.T) {
	provider := &runtimePolicyProvider{}
	e := newRuntimePolicyEngine(t, provider, 30000)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	seedRuntimePolicyRoute(t, e, id, provider.ID(), "deepseek-v4-flash")
	user, err := e.appendEvent(s, "user/message", map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: "small prompt"}},
		"source": map[string]any{"kind": "user"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "step/start", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	textChunk, err := e.appendEvent(s, "assistant/chunk", map[string]any{"turn": 1, "step": 1, "chunk": map[string]any{"type": "text-delta", "index": 0, "text": "ok"}})
	if err != nil {
		t.Fatal(err)
	}
	usageChunk, err := e.appendEvent(s, "assistant/chunk", map[string]any{"turn": 1, "step": 1, "chunk": map[string]any{"type": "usage", "usage": map[string]any{"inputTokens": 900, "outputTokens": 10}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "assistant/message", map[string]any{
		"turn": 1, "step": 1,
		"message": map[string]any{"id": newID("msg"), "role": "assistant", "content": []ContentBlock{{Type: "text", Text: "listener expanded durable output"}}},
		"usage":   map[string]any{"inputTokens": 900, "outputTokens": 10},
	}, int(textChunk.Seq), int(usageChunk.Seq)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	anchored, err := measureSessionTokens(s)
	if err != nil {
		t.Fatal(err)
	}
	if anchored.totalTokens <= 900 {
		t.Fatalf("provider anchor total = %#v", anchored)
	}
	replacement := map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: "x"}},
		"source": map[string]any{"kind": "plugin", "plugin": "test"},
	}
	if _, err := e.appendEventWithMetadata(s, "user/message", replacement, map[string]any{"op": "replace", "start": user.Seq, "end": user.Seq}, []int{int(user.Seq)}, false); err != nil {
		t.Fatal(err)
	}
	shrunken, err := measureSessionTokens(s)
	if err != nil {
		t.Fatal(err)
	}
	if shrunken.totalTokens >= anchored.totalTokens {
		t.Fatalf("signed replacement did not reduce anchor: before=%#v after=%#v", anchored, shrunken)
	}
}

func TestTokenMeasurementDoesNotTrustUsageBelowHeuristicAnchor(t *testing.T) {
	provider := &runtimePolicyProvider{}
	e := newRuntimePolicyEngine(t, provider, 30000)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	seedRuntimePolicyRoute(t, e, id, provider.ID(), "deepseek-v4-flash")
	long := strings.Repeat("context ", 200)
	if _, err := e.appendEvent(s, "user/message", map[string]any{"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: long}}, "source": map[string]any{"kind": "user"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "step/start", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "assistant/message", map[string]any{
		"turn": 1, "step": 1,
		"message": map[string]any{"id": newID("msg"), "role": "assistant", "content": []ContentBlock{{Type: "text", Text: "ok"}}},
		"usage":   map[string]any{"inputTokens": 1, "outputTokens": 1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	measurement, err := measureSessionTokens(s)
	if err != nil {
		t.Fatal(err)
	}
	if measurement.totalTokens <= 2 {
		t.Fatalf("low provider usage undercut heuristic anchor: %#v", measurement)
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
			data, _ := event.Data.(map[string]any)
			if eventInt(data["turn"]) == 1 {
				steps++
			}
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
	if assistant.SourceEventSeqs != nil {
		t.Fatalf("assistant sources = %#v, want nil", assistant.SourceEventSeqs)
	}
	assistantData, _ := assistant.Data.(map[string]any)
	if !streamContainsText(assistantData["stream"], "done") {
		t.Fatalf("assistant embedded stream = %#v", assistantData["stream"])
	}
}

func TestPresetWithoutCompactionBasicDoesNotAutoCompact(t *testing.T) {
	provider := &routedCompactionProvider{id: "arbitrary", model: "model", context: 1000}
	e, id := newPresetCompactionEngine(t, "- name: '@deepseek-ai/dsh-tool-bash'\n", provider.id, provider.model, provider)
	seedRuntimePolicyHistory(t, e, id, 3, 4000)
	if text, err := e.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "continue"}}}); err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v", text, err)
	}
	requests := provider.snapshot()
	if len(requests) != 1 || requests[0].Messages[len(requests[0].Messages)-1].Content == compactionInstruction {
		t.Fatalf("unmounted compaction requests = %#v", requests)
	}
	s, _ := e.getSession(id)
	if catalog, err := e.commandCatalogForSession(s); err != nil || slices.ContainsFunc(catalog, func(command commandDescriptor) bool { return command.Name == "compact" }) {
		t.Fatalf("unmounted compact command catalog = %#v, %v", catalog, err)
	}
	if _, err := e.runCommand(s, "/compact"); err == nil || !strings.Contains(err.Error(), "unknown-command") {
		t.Fatalf("unmounted /compact error = %v", err)
	}
}

func TestPresetCompactionAutoFalseKeepsManualService(t *testing.T) {
	provider := &routedCompactionProvider{id: "manual-route", model: "model", context: 1000}
	e, id := newPresetCompactionEngine(t, "- name: '@deepseek-ai/dsh-compaction-basic'\n  config:\n    auto: false\n- name: '@deepseek-ai/dsh-command-compact'\n", provider.id, provider.model, provider)
	seedRuntimePolicyHistory(t, e, id, 3, 4000)
	if text, err := e.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "continue"}}}); err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v", text, err)
	}
	if got := len(provider.snapshot()); got != 1 {
		t.Fatalf("auto:false request count = %d, want one main request", got)
	}
	s, _ := e.getSession(id)
	result, err := e.runCommand(s, "/compact")
	if err != nil || result.Command == nil || result.Command.Kind != "success" {
		t.Fatalf("manual compact = %#v, %v", result, err)
	}
	requests := provider.snapshot()
	if len(requests) != 2 || requests[1].Messages[len(requests[1].Messages)-1].Content != compactionInstruction {
		t.Fatalf("manual compaction requests = %#v", requests)
	}
}

func TestAutomaticPressureWaitsForDurableRoutedRequest(t *testing.T) {
	provider := &routedCompactionProvider{id: "first-route", model: "model", context: 1000}
	e, id := newPresetCompactionEngine(t, "- name: '@deepseek-ai/dsh-compaction-basic'\n", provider.id, provider.model, provider)
	seedRuntimePolicyHistory(t, e, id, 3, 4000)
	if text, err := e.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "continue"}}}); err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v", text, err)
	}
	requests := provider.snapshot()
	if len(requests) != 1 || requests[0].Messages[len(requests[0].Messages)-1].Content == compactionInstruction {
		t.Fatalf("first routed turn compacted before request/header existed: %#v", requests)
	}
}

func TestPresetCompactionExactPolicyRoutesSummaryAndUsesArbitraryCapacity(t *testing.T) {
	main := &routedCompactionProvider{id: "custom-main", model: "chat", context: 1000}
	summary := &routedCompactionProvider{id: "summary-route", model: "summarizer", context: 2000, summaryText: "small checkpoint"}
	composition := `- name: '@deepseek-ai/dsh-compaction-basic'
  config:
    thresholdRatio: 1
    retainTokens: 999
    modelPolicies:
      - provider: custom-main
        model: chat
        thresholdRatio: 0.2
        retainTokens: 0
        summarizationProvider: summary-route
        summarizationModel: summarizer
        maxTokens: 77
        compactionRetries: 0
        maxOverflowRetries: 0
`
	e, id := newPresetCompactionEngine(t, composition, main.id, main.model, main, summary)
	seedRuntimePolicyHistory(t, e, id, 3, 4000)
	seedRuntimePolicyRoute(t, e, id, main.id, main.model)
	if text, err := e.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "continue"}}}); err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v", text, err)
	}
	summaryRequests := summary.snapshot()
	if len(summaryRequests) != 1 || summaryRequests[0].Model != "summarizer" || summaryRequests[0].MaxTokens != 77 || summaryRequests[0].Messages[len(summaryRequests[0].Messages)-1].Content != compactionInstruction {
		t.Fatalf("summary route requests = %#v", summaryRequests)
	}
	mainRequests := main.snapshot()
	if len(mainRequests) != 1 || !slices.ContainsFunc(mainRequests[0].Messages, func(message ChatMessage) bool { return strings.Contains(message.Content, "small checkpoint") }) {
		t.Fatalf("main route did not use compacted checkpoint: %#v", mainRequests)
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

func TestToolResultPrunerPreservesRichBlockAndEventData(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.SessionTitleLLM.Enabled = false
	cfg.ToolResultPruner = ToolResultPruneConfig{ThresholdChars: 50, HeadChars: 4, TailChars: 3}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "prune-rich", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "step/start", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "tool/call", map[string]any{"turn": 1, "step": 1, "callId": "rich", "name": "bash", "arguments": "{}"}); err != nil {
		t.Fatal(err)
	}
	original, err := e.appendEvent(s, "tool/result", map[string]any{
		"turn": 1, "step": 1, "isError": true,
		"error":       map[string]any{"name": "ExitError", "code": "EXIT_1"},
		"meta":        map[string]any{"diff": []any{"a", "b"}},
		"futureField": map[string]any{"nested": true},
		"message": map[string]any{
			"id": "message-rich", "role": "user", "futureMessageField": "keep",
			"source": map[string]any{"kind": "tool", "callId": "rich"},
			"content": []any{map[string]any{
				"type": "tool-result", "toolCallId": "rich", "isError": true, "futureResultField": "keep",
				"content": []any{
					map[string]any{"type": "text", "text": strings.Repeat("A", 40), "format": "ansi"},
					map[string]any{"type": "reasoning", "text": "private-rich-block", "signature": "sig", "futureReasoningField": true},
					map[string]any{"type": "text", "text": strings.Repeat("B", 30), "middleField": "removed-with-block"},
					map[string]any{"type": "provider-artifact", "uri": "artifact://one", "count": 2},
					map[string]any{"type": "text", "text": strings.Repeat("C", 30), "tailField": "keep"},
				},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "turn/end", map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 2}); err != nil {
		t.Fatal(err)
	}
	if pruned, err := e.pruneToolResults(s); err != nil || pruned != 1 {
		t.Fatalf("prune = %d, %v", pruned, err)
	}
	if pruned, err := e.pruneToolResults(s); err != nil || pruned != 0 {
		t.Fatalf("second prune = %d, %v", pruned, err)
	}

	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	replacement := events[len(events)-1]
	price := events[len(events)-2]
	if replacement.Type != "tool/result" || replacement.Seq != price.Seq+1 || price.Type != "compaction/prune" {
		t.Fatalf("prune pair = %#v, %#v", price, replacement)
	}
	if len(replacement.SourceEventSeqs) != 1 || replacement.SourceEventSeqs[0] != int(original.Seq) {
		t.Fatalf("replacement sources = %v", replacement.SourceEventSeqs)
	}
	start, end, ok := surfaceReplaceBounds(replacement.SurfaceOp)
	if !ok || start != int(original.Seq) || end != int(original.Seq) {
		t.Fatalf("replacement surface op = %#v", replacement.SurfaceOp)
	}
	priceData := price.Data.(map[string]any)
	shadowedTokens, shadowedTokensOK := eventSeqNumber(priceData["shadowedTokenCount"])
	if !shadowedTokensOK || shadowedTokens != estimateProjectionEvent(original) {
		t.Fatalf("shadow price = %#v", priceData)
	}

	data := replacement.Data.(map[string]any)
	if data["isError"] != true || data["futureField"].(map[string]any)["nested"] != true {
		t.Fatalf("replacement event data = %#v", data)
	}
	message := data["message"].(map[string]any)
	if message["futureMessageField"] != "keep" {
		t.Fatalf("replacement message = %#v", message)
	}
	outer := message["content"].([]any)[0].(map[string]any)
	if outer["futureResultField"] != "keep" || outer["toolCallId"] != "rich" {
		t.Fatalf("replacement tool-result block = %#v", outer)
	}
	content := outer["content"].([]any)
	if len(content) != 4 {
		t.Fatalf("replacement content = %#v", content)
	}
	head := content[0].(map[string]any)
	reasoning := content[1].(map[string]any)
	artifact := content[2].(map[string]any)
	tail := content[3].(map[string]any)
	if head["format"] != "ansi" || head["text"] != "AAAA"+toolResultPruneMarker || reasoning["futureReasoningField"] != true || artifact["uri"] != "artifact://one" || tail["tailField"] != "keep" || tail["text"] != "CCC" {
		t.Fatalf("replacement rich content = %#v", content)
	}
	if len([]rune(head["text"].(string)))+len([]rune(tail["text"].(string))) > cfg.ToolResultPruner.ThresholdChars {
		t.Fatalf("replacement exceeds threshold: %#v", content)
	}
	originalData := events[original.Seq].Data.(map[string]any)
	originalOuter := originalData["message"].(map[string]any)["content"].([]any)[0].(map[string]any)
	if originalOuter["content"].([]any)[0].(map[string]any)["text"] != strings.Repeat("A", 40) {
		t.Fatalf("original event mutated = %#v", originalData)
	}
	surface, err := foldSurfaceEvents(events, true)
	if err != nil || slices.ContainsFunc(surface, func(event Event) bool { return event.Seq == original.Seq }) || !slices.ContainsFunc(surface, func(event Event) bool { return event.Seq == replacement.Seq }) {
		t.Fatalf("surface = %#v, %v", surface, err)
	}
}

type toolResultPruneFailStore struct {
	testSessionHandleDefaults
	mu     sync.Mutex
	calls  int
	failAt int
	err    error
}

func (s *toolResultPruneFailStore) Append(context.Context, []Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == s.failAt {
		return s.err
	}
	return nil
}

func TestToolResultPrunerKeepsCommittedPrefixWhenLaterReplacementFails(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.SessionTitleLLM.Enabled = false
	cfg.ToolResultPruner = ToolResultPruneConfig{ThresholdChars: 50, HeadChars: 4, TailChars: 3}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "prune-failure", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	appendResult := func(turn int, callID string) Event {
		t.Helper()
		for _, item := range []struct {
			typ  string
			data map[string]any
		}{
			{"turn/start", map[string]any{"turn": turn}},
			{"step/start", map[string]any{"turn": turn, "step": 1}},
			{"tool/call", map[string]any{"turn": turn, "step": 1, "callId": callID, "name": "bash", "arguments": "{}"}},
		} {
			if _, err := e.appendEvent(s, item.typ, item.data); err != nil {
				t.Fatal(err)
			}
		}
		result, err := e.appendEvent(s, "tool/result", map[string]any{
			"turn": turn, "step": 1,
			"message": toolResultMessage(callID, []ContentBlock{{Type: "text", Text: strings.Repeat(callID, 100)}}, false),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": turn, "step": 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.appendEvent(s, "turn/end", map[string]any{"turn": turn, "reason": map[string]any{"kind": "completed"}}); err != nil {
			t.Fatal(err)
		}
		return result
	}
	first, second := appendResult(1, "a"), appendResult(2, "b")
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 3}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	base := len(s.Events)
	want := errors.New("forced replacement failure")
	s.store = &toolResultPruneFailStore{failAt: 4, err: want}
	s.mu.Unlock()
	if pruned, err := e.pruneToolResults(s); pruned != 1 || !errors.Is(err, want) {
		t.Fatalf("prune = %d, %v", pruned, err)
	}
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	if len(events) != base+3 || events[base].Type != "compaction/prune" || events[base+1].Type != "tool/result" || events[base+2].Type != "compaction/prune" {
		t.Fatalf("committed suffix = %#v", events[base:])
	}
	shadowedSeqs, _ := events[base+2].Data.(map[string]any)["shadowedSeqs"].([]int)
	if events[base+1].SourceEventSeqs[0] != int(first.Seq) || len(shadowedSeqs) != 1 || shadowedSeqs[0] != int(second.Seq) {
		t.Fatalf("committed provenance = %#v", events[base:])
	}
	surface, err := foldSurfaceEvents(events, true)
	if err != nil || slices.ContainsFunc(surface, func(event Event) bool { return event.Seq == first.Seq }) || !slices.ContainsFunc(surface, func(event Event) bool { return event.Seq == events[base+1].Seq }) || !slices.ContainsFunc(surface, func(event Event) bool { return event.Seq == second.Seq }) {
		t.Fatalf("surface after failure = %#v, %v", surface, err)
	}
}

type blockingToolResultPruneStore struct {
	testSessionHandleDefaults
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (s *blockingToolResultPruneStore) Append(context.Context, []Event) error {
	s.once.Do(func() {
		close(s.started)
		<-s.release
	})
	return nil
}

func TestToolResultPrunerCommitsPriceAndReplacementAdjacently(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.SessionTitleLLM.Enabled = false
	cfg.ToolResultPruner = ToolResultPruneConfig{ThresholdChars: 50, HeadChars: 4, TailChars: 3}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "prune-adjacent", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	for _, item := range []struct {
		typ  string
		data map[string]any
	}{
		{"turn/start", map[string]any{"turn": 1}},
		{"step/start", map[string]any{"turn": 1, "step": 1}},
		{"tool/call", map[string]any{"turn": 1, "step": 1, "callId": "a", "name": "bash", "arguments": "{}"}},
	} {
		if _, err := e.appendEvent(s, item.typ, item.data); err != nil {
			t.Fatal(err)
		}
	}
	original, err := e.appendEvent(s, "tool/result", map[string]any{
		"turn": 1, "step": 1,
		"message": toolResultMessage("a", []ContentBlock{{Type: "text", Text: strings.Repeat("x", 100)}}, false),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "step/end", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	store := &blockingToolResultPruneStore{started: make(chan struct{}), release: make(chan struct{})}
	s.mu.Lock()
	base := len(s.Events)
	s.store = store
	s.mu.Unlock()
	pruneDone := make(chan error, 1)
	go func() {
		pruned, err := e.pruneToolResults(s)
		if err == nil && pruned != 1 {
			err = fmt.Errorf("pruned %d results", pruned)
		}
		pruneDone <- err
	}()
	<-store.started
	appendDone := make(chan error, 1)
	go func() {
		_, err := e.appendEvent(s, "request/context", map[string]any{"turn": 1, "source": "concurrent"})
		appendDone <- err
	}()
	close(store.release)
	if err := <-pruneDone; err != nil {
		t.Fatal(err)
	}
	if err := <-appendDone; err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	if len(events) != base+3 || events[base].Type != "compaction/prune" || events[base+1].Type != "tool/result" || events[base+2].Type != "request/context" || events[base+1].SourceEventSeqs[0] != int(original.Seq) {
		t.Fatalf("concurrent suffix = %#v", events[base:])
	}
}

func TestSpillSavesFullTextAndFailureKeepsOriginal(t *testing.T) {
	provider := &runtimePolicyProvider{}
	e := newRuntimePolicyEngine(t, provider, 30000)
	// The notice carries an absolute, session-scoped path. Keep the cap large
	// enough for that mandatory notice so this test exercises preview spilling;
	// a separate failure branch below still verifies the original is preserved.
	e.cfg.Spill = SpillConfig{Root: filepath.Join(t.TempDir(), "spill"), MaxInlineBytes: 512}
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
