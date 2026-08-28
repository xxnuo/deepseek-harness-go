package harness

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func countPlanModeEvents(events []Event) int {
	count := 0
	for _, event := range events {
		if event.Type == "plan/mode" {
			count++
		}
	}
	return count
}

func planModeNoticeTexts(events []Event) []string {
	var texts []string
	for _, event := range events {
		if event.Type != "user/message" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		source, _ := data["source"].(map[string]any)
		if source["plugin"] == "plan-mode" {
			texts = append(texts, contentValueText(data))
		}
	}
	return texts
}

func TestPlanModeIdleCommitAndOpenTurnBoundary(t *testing.T) {
	e, s := commandTestSession(t)
	result, err := e.runCommand(s, "/plan")
	if err != nil || result.Command == nil || result.Command.Text != "Plan mode on. Use /plan off to leave." {
		t.Fatalf("idle /plan = %#v, %v", result, err)
	}
	if !planModeActiveForSession(s) {
		t.Fatal("idle /plan did not commit immediately")
	}
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	result, err = e.runCommand(s, "/plan off")
	if err != nil || result.Command == nil || result.Command.Text != "Leaving plan mode (applies from the next step)." {
		t.Fatalf("open-turn /plan off = %#v, %v", result, err)
	}
	s.mu.Lock()
	before := countPlanModeEvents(s.Events)
	logged := planModeActive(s.Events)
	pending := s.planIntent
	s.mu.Unlock()
	if !logged || pending == nil || pending.active || before != 1 {
		t.Fatalf("queued exit logged=%v pending=%#v events=%d", logged, pending, before)
	}
	if err := e.applyPendingPlanMode(s); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	after := countPlanModeEvents(s.Events)
	logged = planModeActive(s.Events)
	pending = s.planIntent
	s.mu.Unlock()
	if logged || pending != nil || after != 2 {
		t.Fatalf("boundary exit logged=%v pending=%#v events=%d", logged, pending, after)
	}
}

func TestPlanModeOppositeSelectionCancelsQueuedChange(t *testing.T) {
	e, s := commandTestSession(t)
	if _, err := e.setPlanMode(s, true, true); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if outcome, err := e.setPlanMode(s, false, true); err != nil || outcome != planModeQueued {
		t.Fatalf("queue exit = %q, %v", outcome, err)
	}
	if outcome, err := e.setPlanMode(s, true, true); err != nil || outcome != planModeCancelled {
		t.Fatalf("cancel exit = %q, %v", outcome, err)
	}
	if err := e.applyPendingPlanMode(s); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !planModeActive(s.Events) || s.planIntent != nil || countPlanModeEvents(s.Events) != 1 {
		t.Fatalf("cancelled state active=%v pending=%#v events=%#v", planModeActive(s.Events), s.planIntent, s.Events)
	}
}

func TestPlanModeNarratesOnlyWhenLastHeaderToldOtherMode(t *testing.T) {
	e, s := commandTestSession(t)
	if _, err := e.appendEvent(s, "request/header", map[string]any{"header": map[string]any{"config": map[string]any{"provider": "echo", "model": "echo"}}, "reason": "initial"}); err != nil {
		t.Fatal(err)
	}
	if outcome, err := e.setPlanMode(s, true, true); err != nil || outcome != planModeCommitted {
		t.Fatalf("set plan mode = %q, %v", outcome, err)
	}
	s.mu.Lock()
	notices := planModeNoticeTexts(s.Events)
	s.mu.Unlock()
	if !reflect.DeepEqual(notices, []string{planModeEnabledNotice}) {
		t.Fatalf("plan notices = %#v", notices)
	}
}

type planReviewProvider struct {
	mu       sync.Mutex
	requests []ChatRequest
}

func (p *planReviewProvider) ID() string   { return "plan-review" }
func (p *planReviewProvider) Name() string { return "Plan Review" }
func (p *planReviewProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "plan-review", Name: "Plan Review"}}, nil
}
func (p *planReviewProvider) Complete(_ context.Context, request ChatRequest, onDelta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	first := len(p.requests) == 1
	p.mu.Unlock()
	if first {
		call := ToolCall{ID: "plan-call", Name: exitPlanModeName, Arguments: json.RawMessage(`{"plan":"# Ship parity\n\nImplement and verify it."}`)}
		if err := onDelta(Delta{ToolCalls: []ToolCallDelta{{Index: 0, ID: call.ID, Name: call.Name, ArgumentsDelta: string(call.Arguments)}}}); err != nil {
			return Completion{}, err
		}
		return Completion{ToolCalls: []ToolCall{call}, Finish: "tool_calls"}, nil
	}
	if err := onDelta(Delta{Text: "implementation starts now"}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: "implementation starts now", Finish: "stop"}, nil
}
func (p *planReviewProvider) snapshot() []ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...)
}

func waitForPlanReview(t *testing.T, e *Engine, sessionID string) *pendingInteraction {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, pending := range e.pendingInteractions() {
			if pending.sessionID == sessionID && pending.method == "question/requested" {
				return pending
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("plan review interaction was not published")
	return nil
}

func toolSchemaNames(tools []ToolSchema) []string {
	names := make([]string, len(tools))
	for index, tool := range tools {
		names[index] = tool.Name
	}
	sort.Strings(names)
	return names
}

func TestExitPlanModeApprovalAppliesOnNextProductionStep(t *testing.T) {
	provider := &planReviewProvider{}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = t.TempDir(), t.TempDir()
	cfg.Provider, cfg.Model, cfg.Persist = provider.ID(), "plan-review", false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "plan-review-session", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	if outcome, err := e.setPlanMode(s, true, true); err != nil || outcome != planModeCommitted {
		t.Fatalf("enable plan mode = %q, %v", outcome, err)
	}
	type runResult struct {
		text string
		err  error
	}
	done := make(chan runResult, 1)
	go func() {
		text, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "prepare the plan"}}})
		done <- runResult{text: text, err: err}
	}()
	pending := waitForPlanReview(t, e, id)
	payload, _ := pending.payload.(map[string]any)
	questions, _ := payload["questions"].([]any)
	question, _ := questions[0].(map[string]any)
	intent, _ := question["intent"].(map[string]any)
	if question["id"] != planReviewID || intent["kind"] != "plan-review" || intent["approve"] != planApproveLabel || !strings.Contains(question["detail"].(string), "# Ship parity") {
		t.Fatalf("plan review payload = %#v", pending.payload)
	}
	if !e.ResolveInteraction(pending.id, map[string]any{"ok": true, "value": map[string]any{
		"answer": map[string]any{"answers": []any{map[string]any{"id": planReviewID, "selected": []any{planApproveLabel}}}},
	}}) {
		t.Fatal("plan review interaction was not resolved")
	}
	select {
	case result := <-done:
		if result.err != nil || result.text != "implementation starts now" {
			t.Fatalf("Run() = %q, %v", result.text, result.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("plan review run did not finish")
	}
	requests := provider.snapshot()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d", len(requests))
	}
	if !strings.Contains(requests[0].System, "You are in plan mode.") || strings.Contains(requests[1].System, "You are in plan mode.") {
		t.Fatalf("plan policy transition systems = %#v", []string{requests[0].System, requests[1].System})
	}
	if before, after := toolSchemaNames(requests[0].Tools), toolSchemaNames(requests[1].Tools); !reflect.DeepEqual(before, after) {
		t.Fatalf("tool catalog changed across plan transition:\nbefore=%v\nafter=%v", before, after)
	}
	s.mu.Lock()
	active := planModeActive(s.Events)
	notices := planModeNoticeTexts(s.Events)
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	if active || len(notices) != 0 {
		t.Fatalf("approved plan state active=%v notices=%#v", active, notices)
	}
	planSeq, nextStepSeq := -1, -1
	for _, event := range events {
		if event.Type == "plan/mode" {
			data, _ := event.Data.(map[string]any)
			if data["active"] == false {
				planSeq = event.Seq
			}
		}
		if planSeq >= 0 && event.Type == "step/start" && event.Seq > planSeq {
			nextStepSeq = event.Seq
			break
		}
	}
	if planSeq < 0 || nextStepSeq != planSeq+1 {
		t.Fatalf("exit was not committed immediately before the next step: plan=%d step=%d", planSeq, nextStepSeq)
	}
}

func TestExitPlanModeRejectionKeepsPlanMode(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "plan-reject", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	if _, err := e.setPlanMode(s, true, false); err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool := e.tools[exitPlanModeName]
	e.mu.RUnlock()
	done := make(chan error, 1)
	go func() {
		_, err := executeToolRuntime(context.Background(), tool, ToolCall{
			ID: "reject", Name: exitPlanModeName, SessionID: id, Arguments: json.RawMessage(`{"plan":"# Revise\n\nFirst draft."}`),
		}, nil)
		done <- err
	}()
	pending := waitForPlanReview(t, e, id)
	e.ResolveInteraction(pending.id, map[string]any{"ok": true, "value": map[string]any{
		"answer": map[string]any{"answers": []any{map[string]any{
			"id": planReviewID, "selected": []any{planKeepPlanningLabel}, "custom": "cover rollback",
		}}},
	}})
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "cover rollback") {
			t.Fatalf("rejection error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rejected review did not finish")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !planModeActive(s.Events) || s.planIntent != nil {
		t.Fatalf("rejection changed plan state active=%v pending=%#v", planModeActive(s.Events), s.planIntent)
	}
}

type planRetryProvider struct {
	mu       sync.Mutex
	requests []ChatRequest
	started  chan struct{}
	release  chan struct{}
}

func (p *planRetryProvider) ID() string   { return "plan-retry" }
func (p *planRetryProvider) Name() string { return "Plan Retry" }
func (p *planRetryProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "plan-retry", Name: "Plan Retry"}}, nil
}
func (p *planRetryProvider) RetryPolicy() RetryPolicy { return retryPolicyForTest(1) }
func (p *planRetryProvider) Complete(_ context.Context, request ChatRequest, _ func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	attempt := len(p.requests)
	p.mu.Unlock()
	if attempt == 1 {
		close(p.started)
		<-p.release
		return Completion{}, &ProviderError{Code: "SERVER", Message: "retry"}
	}
	return Completion{Text: "done", Finish: "stop"}, nil
}
func (p *planRetryProvider) snapshot() []ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...)
}

func TestPlanModeSelectionDoesNotChangeInFlightRetryAssembly(t *testing.T) {
	provider := &planRetryProvider{started: make(chan struct{}), release: make(chan struct{})}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = t.TempDir(), t.TempDir()
	cfg.Provider, cfg.Model, cfg.Persist = provider.ID(), "plan-retry", false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "plan-retry-session", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	firstDone := make(chan error, 1)
	go func() {
		_, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "first"}}})
		firstDone <- err
	}()
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first provider attempt did not start")
	}
	result, err := e.runCommand(s, "/plan")
	if err != nil || result.Command == nil || !strings.Contains(result.Command.Text, "next step") {
		t.Fatalf("in-flight /plan = %#v, %v", result, err)
	}
	close(provider.release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retrying turn did not finish")
	}
	requests := provider.snapshot()
	if len(requests) != 2 || strings.Contains(requests[0].System, "You are in plan mode.") || strings.Contains(requests[1].System, "You are in plan mode.") {
		t.Fatalf("retry assembly changed after pending selection: %#v", requests)
	}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "second"}}}); err != nil {
		t.Fatal(err)
	}
	requests = provider.snapshot()
	if len(requests) != 3 || !strings.Contains(requests[2].System, "You are in plan mode.") {
		t.Fatalf("next accepted step did not apply plan mode: %#v", requests)
	}
}

func TestExitPlanModeValidatesLoggedModeAndPlanHeading(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "plan-validation", "")
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool := e.tools[exitPlanModeName]
	e.mu.RUnlock()
	_, err = executeToolRuntime(context.Background(), tool, ToolCall{
		Name: exitPlanModeName, SessionID: id, Arguments: json.RawMessage(`{"plan":"# Valid"}`),
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "only available in plan mode") {
		t.Fatalf("inactive error = %v", err)
	}
	s, _ := e.getSession(id)
	if _, err := e.setPlanMode(s, true, false); err != nil {
		t.Fatal(err)
	}
	_, err = executeToolRuntime(context.Background(), tool, ToolCall{
		Name: exitPlanModeName, SessionID: id, Arguments: json.RawMessage(`{"plan":"## Not level one"}`),
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "starting with a # heading") {
		t.Fatalf("heading error = %v", err)
	}
}

func TestApprovedPlanReviewRequiresExactlyOneCleanApprove(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		approved bool
		feedback string
	}{
		{"approve", map[string]any{"answers": []any{map[string]any{"id": planReviewID, "selected": []any{planApproveLabel}}}}, true, ""},
		{"custom-rejects-approve", map[string]any{"answers": []any{map[string]any{"id": planReviewID, "selected": []any{planApproveLabel}, "custom": "change it"}}}, false, "change it"},
		{"duplicate", map[string]any{"answers": []any{map[string]any{"id": planReviewID, "selected": []any{planApproveLabel}}, map[string]any{"id": planReviewID, "selected": []any{planApproveLabel}}}}, false, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			approved, feedback := approvedPlanReview(test.value)
			if approved != test.approved || feedback != test.feedback {
				t.Fatalf("approvedPlanReview() = %v, %q", approved, feedback)
			}
		})
	}
}
