package harness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type promptGateProvider struct {
	mu      sync.Mutex
	gates   map[string]chan struct{}
	started chan string
}

type imageRaceProvider struct {
	promptGateProvider
	capabilityStarted chan struct{}
	capability        chan struct{}
}

func (p *imageRaceProvider) ResolveModelInfo(ctx context.Context, model string) (ModelInfo, error) {
	select {
	case <-p.capabilityStarted:
	default:
		close(p.capabilityStarted)
	}
	select {
	case <-p.capability:
		return ModelInfo{ID: model, Name: model, InputModalities: []string{"text", "image"}}, nil
	case <-ctx.Done():
		return ModelInfo{}, ctx.Err()
	}
}

func (p *promptGateProvider) ID() string   { return "prompt-gate" }
func (p *promptGateProvider) Name() string { return "Prompt Gate" }
func (p *promptGateProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: p.ID(), Name: p.Name()}}, nil
}
func (p *promptGateProvider) Complete(ctx context.Context, request ChatRequest, onDelta func(Delta) error) (Completion, error) {
	text := ""
	for index := len(request.Messages) - 1; index >= 0; index-- {
		kind, _ := request.Messages[index].Source["kind"].(string)
		if kind != "plugin" {
			text = request.Messages[index].Content
			break
		}
	}
	p.started <- text
	p.mu.Lock()
	gate := p.gates[text]
	p.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return Completion{}, ctx.Err()
		}
	}
	if err := onDelta(Delta{Text: text}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: text}, nil
}

func executeModelToolWithContext(e *Engine, ctx context.Context, name, sessionID string, args any) (ToolResult, error) {
	data, err := json.Marshal(args)
	if err != nil {
		return ToolResult{}, err
	}
	e.mu.RLock()
	tool := e.tools[name]
	e.mu.RUnlock()
	return tool.Execute(ctx, ToolCall{Name: name, SessionID: sessionID, Arguments: data})
}

func waitForModelSubagentDetached(t *testing.T, e *Engine, childID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		child := mustSession(t, e, childID)
		child.mu.Lock()
		attached := child.attached
		child.mu.Unlock()
		e.modelSubagentMu.Lock()
		activation := e.modelSubagentActivations[childID]
		e.modelSubagentMu.Unlock()
		if !attached && activation == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	child := mustSession(t, e, childID)
	child.mu.Lock()
	running, attached, parked := child.Running, child.attached, child.parked
	pending, steering, events := len(child.pending), len(child.steering), len(child.Events)
	child.mu.Unlock()
	e.modelSubagentMu.Lock()
	activation := e.modelSubagentActivations[childID]
	disposing, owned := false, 0
	if activation != nil {
		disposing, owned = activation.disposing, len(activation.owned)
	}
	e.modelSubagentMu.Unlock()
	t.Fatalf("subagent %q did not settle: running=%v attached=%v parked=%v pending=%d steering=%d events=%d activation=%v disposing=%v owned=%d", childID, running, attached, parked, pending, steering, events, activation != nil, disposing, owned)
}

func waitForModelSubagentDisposing(t *testing.T, e *Engine, activation *modelSubagentActivation) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		e.modelSubagentMu.Lock()
		disposing := activation.disposing
		e.modelSubagentMu.Unlock()
		if disposing {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("model subagent activation did not enter disposing state")
}

func TestContinuableModelSubagentRequiresPersistence(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "continuable-no-persistence", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = executeModelToolWithContext(e, t.Context(), "subagent", parent, map[string]any{
		"description": "durable child", "prompt": "work",
	})
	if err == nil || !strings.Contains(err.Error(), "require session persistence") {
		t.Fatalf("continuable error = %v", err)
	}
	entries, listErr := e.listModelAgents(t.Context(), parent, false)
	if listErr != nil || len(entries) != 0 {
		t.Fatalf("children after rejected start = %#v, %v", entries, listErr)
	}
}

func TestContinuableModelSubagentReturnsAtAdmissionAndIgnoresLaterCallerCancel(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	provider := newQueuedTestProvider()
	e.RegisterProvider(provider)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "continuable-admission-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(parent, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}

	callCtx, cancel := context.WithCancel(t.Context())
	result, err := executeModelToolWithContext(e, callCtx, "subagent", parent, map[string]any{
		"description": "gated child", "prompt": "initial work",
	})
	if err != nil {
		t.Fatal(err)
	}
	value, _ := result.Value.(map[string]any)
	childID, _ := value["subagentId"].(string)
	if value["kind"] != "continuable" || childID == "" {
		t.Fatalf("continuable result = %#v", result.Value)
	}
	child := mustSession(t, e, childID)
	child.mu.Lock()
	accepted := len(child.Events) > 0 && child.Running
	child.mu.Unlock()
	if !accepted {
		t.Fatal("tool returned before durable inbox admission")
	}
	cancel()
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("accepted child turn did not start")
	}
	child.mu.Lock()
	stillRunning := child.Running
	child.mu.Unlock()
	if !stillRunning {
		t.Fatal("caller cancellation after admission cancelled the child")
	}
	close(provider.firstGate)
	waitForModelSubagentDetached(t, e, childID)
}

func TestContinuableModelSubagentColdResumesAfterSettlement(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "continuable-resume-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	result := executeRegisteredTool(t, e, "subagent", parent, map[string]any{
		"description": "resumable child", "prompt": "first answer",
	})
	value, _ := result.Value.(map[string]any)
	childID, _ := value["subagentId"].(string)
	waitForModelSubagentDetached(t, e, childID)

	messageID, err := e.promptContinuableModelSubagent(t.Context(), parent, childID,
		[]ContentBlock{{Type: "text", Text: "second answer"}},
		map[string]any{"kind": "coordinator", "form": "relay", "senderSessionId": parent})
	if err != nil || messageID == "" {
		t.Fatalf("cold resume = %q, %v", messageID, err)
	}
	waitForModelSubagentDetached(t, e, childID)
	history, _, err := e.History(childID, -1, 200)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range history {
		if entry.Event.Type == "user/message" && strings.Contains(contentValueText(entry.Event.Data), "second answer") {
			found = true
		}
	}
	if !found {
		t.Fatal("cold-resumed turn was not committed")
	}
}

func TestContinuableModelSubagentColdResumeUsesFirstDescriptorAndRestoresComposition(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	gate := make(chan struct{})
	provider := &promptGateProvider{gates: map[string]chan struct{}{"resume work": gate}, started: make(chan string, 2)}
	e.RegisterProvider(provider)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "descriptor-resume-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(parent, ModelSelection{Provider: provider.ID(), Model: provider.ID(), MaxTokens: 999}); err != nil {
		t.Fatal(err)
	}
	childID, err := e.createModelSubagent(t.Context(), parent, "descriptor child", false, "continuable", SubagentToolConfig{
		Provider: "spawn", Persona: "restored persona", ToolFilter: &SubagentToolFilter{Deny: []string{"bash"}},
		AgentOptions: &SubagentAgentOptions{Provider: provider.ID(), Model: provider.ID(), MaxTokens: 123},
	})
	if err != nil {
		t.Fatal(err)
	}
	child := mustSession(t, e, childID)
	if _, err := e.appendEvent(child, "subagent/descriptor", map[string]any{
		"version": SubagentDescriptorVersion, "mode": "one-shot", "provider": "later",
	}); err != nil {
		t.Fatal(err)
	}
	if err := detachSDKSession(e, childID); err != nil {
		t.Fatal(err)
	}
	child.mu.Lock()
	child.Model = ModelSelection{Provider: "stale", Model: "stale", MaxTokens: 777}
	child.personaOverride = ""
	child.toolRestriction = nil
	child.mu.Unlock()

	messageID, err := e.promptContinuableModelSubagent(t.Context(), parent, childID,
		[]ContentBlock{{Type: "text", Text: "resume work"}}, map[string]any{"kind": "coordinator"})
	if err != nil || messageID == "" {
		t.Fatalf("cold resume = %q, %v", messageID, err)
	}
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("cold-resumed turn did not start")
	}
	child.mu.Lock()
	selection, persona, restriction := child.Model, child.personaOverride, child.toolRestriction
	child.mu.Unlock()
	if selection.Provider != provider.ID() || selection.Model != provider.ID() || selection.MaxTokens != 0 {
		t.Fatalf("restored model selection = %#v", selection)
	}
	if persona != "restored persona" || restriction == nil || !restriction.denySet || !restriction.deny["bash"] {
		t.Fatalf("restored composition = persona %q restriction %#v", persona, restriction)
	}
	close(gate)
	waitForModelSubagentDetached(t, e, childID)
}

func TestContinuableModelSubagentLiveImageRaceReturnsDraining(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	provider := &imageRaceProvider{
		promptGateProvider: promptGateProvider{gates: map[string]chan struct{}{"child work": make(chan struct{})}, started: make(chan string, 2)},
		capabilityStarted:  make(chan struct{}), capability: make(chan struct{}),
	}
	e.RegisterProvider(provider)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "image-race-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(parent, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	result := executeRegisteredTool(t, e, "subagent", parent, map[string]any{
		"description": "image race child", "prompt": "child work",
	})
	childID, _ := result.Value.(map[string]any)["subagentId"].(string)
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("child model request did not start")
	}

	image := ContentBlock{Type: "image"}
	delivery := make(chan error, 1)
	go func() {
		_, deliveryErr := e.promptContinuableModelSubagent(t.Context(), parent, childID, []ContentBlock{image}, map[string]any{"kind": "user"})
		delivery <- deliveryErr
	}()
	select {
	case <-provider.capabilityStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("image capability lookup did not start")
	}
	e.modelSubagentMu.Lock()
	activation := e.modelSubagentActivations[childID]
	e.modelSubagentMu.Unlock()
	if activation == nil {
		t.Fatal("live image race activation disappeared before drain")
	}
	drainDone := make(chan error, 1)
	go func() { drainDone <- e.drainModelSubagentDescendants(t.Context(), []string{parent}) }()
	waitForModelSubagentDisposing(t, e, activation)
	close(provider.capability)
	select {
	case deliveryErr := <-delivery:
		if !IsSubagentServiceError(deliveryErr, "DRAINING") {
			t.Fatalf("live image race error = %v", deliveryErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("live image race did not return")
	}
	if err := <-drainDone; err != nil {
		t.Fatal(err)
	}
}

func TestContinuableModelSubagentMaterializedImageRaceRollsBack(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	provider := &imageRaceProvider{
		promptGateProvider: promptGateProvider{gates: map[string]chan struct{}{}, started: make(chan string, 2)},
		capabilityStarted:  make(chan struct{}), capability: make(chan struct{}),
	}
	e.RegisterProvider(provider)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "materialized-image-race-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(parent, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	result := executeRegisteredTool(t, e, "subagent", parent, map[string]any{
		"description": "materialized image race child", "prompt": "child work",
	})
	childID, _ := result.Value.(map[string]any)["subagentId"].(string)
	waitForModelSubagentDetached(t, e, childID)

	delivery := make(chan error, 1)
	go func() {
		_, deliveryErr := e.promptContinuableModelSubagent(t.Context(), parent, childID, []ContentBlock{{Type: "image"}}, map[string]any{"kind": "user"})
		delivery <- deliveryErr
	}()
	select {
	case <-provider.capabilityStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("materialized image capability lookup did not start")
	}
	e.modelSubagentMu.Lock()
	activation := e.modelSubagentActivations[childID]
	e.modelSubagentMu.Unlock()
	if activation == nil {
		t.Fatal("materialized image race activation disappeared before drain")
	}
	drainDone := make(chan error, 1)
	go func() { drainDone <- e.drainModelSubagentDescendants(t.Context(), []string{parent}) }()
	waitForModelSubagentDisposing(t, e, activation)
	close(provider.capability)
	select {
	case deliveryErr := <-delivery:
		if !IsSubagentServiceError(deliveryErr, "ACTIVATION_CLOSING") {
			t.Fatalf("materialized image race error = %v", deliveryErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("materialized image race did not return")
	}
	if err := <-drainDone; err != nil {
		t.Fatal(err)
	}
	child := mustSession(t, e, childID)
	child.mu.Lock()
	defer child.mu.Unlock()
	for _, event := range child.Events {
		if event.Type != "user/message" {
			continue
		}
		if strings.Contains(contentValueText(event.Data), "image") {
			t.Fatal("materialized image race persisted a user message")
		}
	}
}

func TestModelSubagentDepthUsesPersistedMonotoneFloor(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	parentID, err := e.CreateSession(t.Context(), e.Config().Workspace, "depth-floor-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	parent := mustSession(t, e, parentID)
	parent.mu.Lock()
	parent.Header.DelegationDepth = 4
	parent.mu.Unlock()
	maxDepth := 4
	if _, err := e.createModelSubagent(t.Context(), parentID, "too deep", false, "one-shot", SubagentToolConfig{Provider: "spawn", MaxDepth: &maxDepth}); err == nil || !strings.Contains(err.Error(), "depth 5 exceeds maxDepth 4") {
		t.Fatalf("depth limit error = %v", err)
	}
	parent.mu.Lock()
	parent.Header.DelegationDepth = int(maxJSONSafeInteger)
	parent.mu.Unlock()
	if _, err := e.createModelSubagent(t.Context(), parentID, "overflow", false, "one-shot", SubagentToolConfig{Provider: "spawn"}); err == nil || !strings.Contains(err.Error(), "safe-integer range") {
		t.Fatalf("safe integer error = %v", err)
	}
}

func TestContinuableModelSubagentInterruptParksInboxUntilWakingSend(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	provider := newQueuedTestProvider()
	e.RegisterProvider(provider)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "continuable-interrupt-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(parent, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	result := executeRegisteredTool(t, e, "subagent", parent, map[string]any{
		"description": "interrupt child", "prompt": "first blocked turn",
	})
	value, _ := result.Value.(map[string]any)
	childID, _ := value["subagentId"].(string)
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("initial child turn did not start")
	}
	if _, err := e.promptContinuableModelSubagent(t.Context(), parent, childID,
		[]ContentBlock{{Type: "text", Text: "parked second turn"}},
		map[string]any{"kind": "coordinator", "form": "relay", "senderSessionId": parent}); err != nil {
		t.Fatal(err)
	}
	if _, rpcErr := e.subagentInterrupt(map[string]any{"parentSessionId": parent, "childSessionId": childID}); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := e.WaitForIdle(waitCtx, childID); err != nil {
		t.Fatal(err)
	}
	child := mustSession(t, e, childID)
	child.mu.Lock()
	events := append([]Event(nil), child.Events...)
	var cancelKind string
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type != "turn/end" {
			continue
		}
		data, _ := events[index].Data.(map[string]any)
		reason, _ := data["reason"].(map[string]any)
		if reason["kind"] != "aborted" {
			continue
		}
		cause, _ := reason["reason"].(map[string]any)
		cancelKind, _ = cause["kind"].(string)
		break
	}
	child.mu.Unlock()
	if cancelKind != "user" {
		t.Fatalf("RPC interrupt cancel kind = %q, want user", cancelKind)
	}
	provider.mu.Lock()
	requestsBeforeWake := len(provider.requests)
	provider.mu.Unlock()
	if requestsBeforeWake != 1 {
		t.Fatalf("parked inbox already ran: requests=%d", requestsBeforeWake)
	}
	e.modelSubagentMu.Lock()
	resident := e.modelSubagentActivations[childID] != nil
	e.modelSubagentMu.Unlock()
	if !resident {
		t.Fatal("parked inbox did not retain the activation")
	}

	close(provider.firstGate)
	if _, err := e.promptContinuableModelSubagent(t.Context(), parent, childID,
		[]ContentBlock{{Type: "text", Text: "waking third turn"}},
		map[string]any{"kind": "coordinator", "form": "relay", "senderSessionId": parent}); err != nil {
		t.Fatal(err)
	}
	waitForModelSubagentDetached(t, e, childID)
	provider.mu.Lock()
	requestsAfterWake := len(provider.requests)
	provider.mu.Unlock()
	if requestsAfterWake < 3 {
		t.Fatalf("waking send did not release parked FIFO: requests=%d", requestsAfterWake)
	}
}

func TestModelSubagentActivationTerminalAccountsForConsumedWork(t *testing.T) {
	session := &Session{Events: []Event{
		{Type: "turn/start", Data: map[string]any{"turn": 1}},
		{Type: "step/start", Data: map[string]any{"turn": 1}},
		{Type: "assistant/message", Data: map[string]any{"message": map[string]any{"content": []ContentBlock{{Type: "text", Text: "first answer"}}}}},
		{Type: "turn/end", Data: map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}},
		{Type: "agent/inbox/spliced", Data: map[string]any{"target": "next-turn", "start": 0, "removedCount": 1, "inserted": []any{}, "outcome": "canceled"}},
	}}
	activation := &modelSubagentActivation{session: session, startSeq: 0}
	reason, output := (&Engine{}).modelSubagentActivationTerminal(activation)
	if reason != "aborted" || contentValueText(output) != "first answer" {
		t.Fatalf("terminal = %q, %#v", reason, output)
	}

	session.Events = append(session.Events,
		Event{Type: "turn/start", Data: map[string]any{"turn": 2}},
		Event{Type: "step/start", Data: map[string]any{"turn": 2}},
		Event{Type: "turn/end", Data: map[string]any{"turn": 2, "reason": map[string]any{"kind": "completed"}}},
	)
	reason, _ = (&Engine{}).modelSubagentActivationTerminal(activation)
	if reason != "completed" {
		t.Fatalf("later accounting turn reason = %q", reason)
	}
}

func TestFoldModelSubagentConsumedWorkAttributesPreTurnClaim(t *testing.T) {
	tests := []struct {
		name       string
		reason     map[string]any
		wantEnd    bool
		wantReason string
	}{
		{name: "completed empty claim", reason: map[string]any{"kind": "completed"}},
		{name: "rejected claim", reason: map[string]any{"kind": "rejected"}, wantEnd: true, wantReason: "rejected"},
		{name: "failed claim", reason: map[string]any{"kind": "error"}, wantEnd: true, wantReason: "error"},
		{name: "aborted claim", reason: map[string]any{"kind": "aborted"}, wantEnd: true, wantReason: "aborted"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := []Event{
				{Type: "agent/inbox/spliced", Data: map[string]any{"removedCount": 1, "inserted": []any{}}},
				{Type: "turn/start", Data: map[string]any{"turn": 1}},
				{Type: "turn/end", Data: map[string]any{"turn": 1, "reason": test.reason}},
			}
			end, dropped := foldModelSubagentConsumedWork(events)
			if dropped {
				t.Fatal("claimed work reported as dropped unrun")
			}
			if (end != nil) != test.wantEnd {
				t.Fatalf("end = %#v, want present %v", end, test.wantEnd)
			}
			if end != nil {
				data, _ := end.Data.(map[string]any)
				reason, _ := data["reason"].(map[string]any)
				if reason["kind"] != test.wantReason {
					t.Fatalf("reason = %#v, want %q", reason, test.wantReason)
				}
			}
		})
	}

	end, dropped := foldModelSubagentConsumedWork([]Event{
		{Type: "agent/inbox/spliced", Data: map[string]any{"removedCount": 1, "inserted": []any{}}},
	})
	if end != nil || dropped {
		t.Fatalf("unowned pre-turn claim = %#v, dropped=%v", end, dropped)
	}
}

func TestDrainModelSubagentDescendantsIsScopedToSelectedRoots(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	firstGate := make(chan struct{})
	secondGate := make(chan struct{})
	provider := &promptGateProvider{
		gates:   map[string]chan struct{}{"first child": firstGate, "second child": secondGate},
		started: make(chan string, 4),
	}
	e.RegisterProvider(provider)
	firstParent, err := e.CreateSession(t.Context(), e.Config().Workspace, "first-scope-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	secondParent, err := e.CreateSession(t.Context(), e.Config().Workspace, "second-scope-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, parent := range []string{firstParent, secondParent} {
		if err := e.SelectModel(parent, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
			t.Fatal(err)
		}
	}
	first := executeRegisteredTool(t, e, "subagent", firstParent, map[string]any{"description": "first", "prompt": "first child"})
	second := executeRegisteredTool(t, e, "subagent", secondParent, map[string]any{"description": "second", "prompt": "second child"})
	firstID, _ := first.Value.(map[string]any)["subagentId"].(string)
	secondID, _ := second.Value.(map[string]any)["subagentId"].(string)
	for range 2 {
		select {
		case <-provider.started:
		case <-time.After(2 * time.Second):
			t.Fatal("scoped child request did not start")
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := e.drainModelSubagentDescendants(ctx, []string{firstParent}); err != nil {
		t.Fatal(err)
	}
	e.modelSubagentMu.Lock()
	firstActivation := e.modelSubagentActivations[firstID]
	secondActivation := e.modelSubagentActivations[secondID]
	e.modelSubagentMu.Unlock()
	if firstActivation != nil {
		t.Fatal("selected root descendant remains resident")
	}
	if secondActivation == nil || secondActivation.disposing {
		t.Fatal("unselected root descendant was disturbed")
	}
	close(secondGate)
	waitForModelSubagentDetached(t, e, secondID)
}

func TestContinuableModelSubagentParentWaitsForOwnedDescendant(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	parentGate, childGate := make(chan struct{}), make(chan struct{})
	provider := &promptGateProvider{
		gates:   map[string]chan struct{}{"parent child work": parentGate, "grandchild work": childGate},
		started: make(chan string, 8),
	}
	e.RegisterProvider(provider)
	root, err := e.CreateSession(t.Context(), e.Config().Workspace, "owned-root", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(root, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	result := executeRegisteredTool(t, e, "subagent", root, map[string]any{
		"description": "parent child", "prompt": "parent child work",
	})
	value, _ := result.Value.(map[string]any)
	parentChildID, _ := value["subagentId"].(string)
	select {
	case started := <-provider.started:
		if started != "parent child work" {
			t.Fatalf("first started prompt = %q", started)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parent child did not start")
	}
	grandchildID, _, err := e.startContinuableModelSubagent(t.Context(), parentChildID, "grandchild", "grandchild work", false, SubagentToolConfig{Provider: "spawn", BackgroundMode: "continuable"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case started := <-provider.started:
		if started != "grandchild work" {
			t.Fatalf("second started prompt = %q", started)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("grandchild did not start")
	}
	close(parentGate)
	waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := e.WaitForIdle(waitCtx, parentChildID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	parentChild := mustSession(t, e, parentChildID)
	parentChild.mu.Lock()
	parentAttached := parentChild.attached
	parentChild.mu.Unlock()
	e.modelSubagentMu.Lock()
	parentActivation := e.modelSubagentActivations[parentChildID]
	ownsGrandchild := false
	if parentActivation != nil {
		_, ownsGrandchild = parentActivation.owned[grandchildID]
	}
	e.modelSubagentMu.Unlock()
	if !parentAttached || parentActivation == nil || !ownsGrandchild {
		t.Fatalf("parent settled before descendant: attached=%v activation=%v owns=%v", parentAttached, parentActivation != nil, ownsGrandchild)
	}
	close(childGate)
	waitForModelSubagentDetached(t, e, grandchildID)
	waitForModelSubagentDetached(t, e, parentChildID)
}

type admissionFailingStore struct {
	SessionStore
}

type admissionFailingHandle struct{ SessionHandle }

func (h *admissionFailingHandle) Append(ctx context.Context, events []Event) error {
	for _, event := range events {
		if event.Type == "agent/inbox/spliced" {
			return errors.New("inbox admission failed")
		}
	}
	return h.SessionHandle.Append(ctx, events)
}

func (s *admissionFailingStore) wrap(handle SessionHandle, err error) (SessionHandle, error) {
	if err != nil {
		return nil, err
	}
	return &admissionFailingHandle{SessionHandle: handle}, nil
}

func (s *admissionFailingStore) Create(ctx context.Context, meta SessionHeader, inheritedEventCount SessionLogOffset) (SessionHandle, error) {
	return s.wrap(s.SessionStore.Create(ctx, meta, inheritedEventCount))
}

func (s *admissionFailingStore) Open(ctx context.Context, id string, access SessionAccess) (SessionHandle, error) {
	return s.wrap(s.SessionStore.Open(ctx, id, access))
}

func TestContinuableModelSubagentRollsBackFailedAdmission(t *testing.T) {
	root := t.TempDir()
	base, err := NewJSONLSessionStore(root + "/sessions")
	if err != nil {
		t.Fatal(err)
	}
	store := &admissionFailingStore{SessionStore: base}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = root, root
	cfg.Provider, cfg.Model, cfg.Persist, cfg.SessionStore = "echo", "echo", true, store
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	parent, err := e.CreateSession(t.Context(), root, "rollback-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = executeModelToolWithContext(e, t.Context(), "subagent", parent, map[string]any{
		"description": "rollback child", "prompt": "must not survive",
	})
	if err == nil || !strings.Contains(err.Error(), "inbox admission failed") {
		t.Fatalf("admission error = %v", err)
	}
	e.mu.RLock()
	count := len(e.sessions)
	e.mu.RUnlock()
	if count != 1 {
		t.Fatalf("resident sessions after rollback = %d", count)
	}
	snapshots, err := store.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range snapshots {
		if snapshot.Header.ParentSession == parent {
			t.Fatalf("persisted child after rollback = %#v", snapshot)
		}
	}
}
