package harness

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func executeRegisteredTool(t *testing.T, e *Engine, name, sessionID string, args any) ToolResult {
	t.Helper()
	data, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool, ok := e.tools[name]
	e.mu.RUnlock()
	if !ok {
		t.Fatalf("tool %q is not registered", name)
	}
	workspace := ""
	if session, getErr := e.getSession(sessionID); getErr == nil {
		session.mu.Lock()
		workspace = session.Header.CWD
		session.mu.Unlock()
	}
	call := ToolCall{ID: "call-test", Name: name, Arguments: data, SessionID: sessionID, Workspace: workspace}
	var result ToolResult
	if tool.Execute != nil {
		result, err = tool.Execute(context.Background(), call)
	} else {
		result, err = executeToolRuntime(context.Background(), tool, call, nil)
	}
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return result
}

func modelToolResultText(result ToolResult) string {
	return contentValueText(result.Content)
}

func newPersistentModelSubagentEngine(t *testing.T) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestAskUserQuestionRoundTripAndSubagentFence(t *testing.T) {
	e := newIntegrationEngine(t)
	root, err := e.CreateSession(context.Background(), e.Config().Workspace, "question-root", "")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan ToolResult, 1)
	go func() {
		done <- executeRegisteredTool(t, e, "ask_user_question", root, map[string]any{
			"questions": []any{map[string]any{"id": "mode", "question": "Choose mode", "options": []any{map[string]any{"label": "Fast"}}}},
		})
	}()

	var rpcID string
	deadline := time.Now().Add(2 * time.Second)
	for rpcID == "" && time.Now().Before(deadline) {
		for _, pending := range e.pendingInteractions() {
			if pending.sessionID == root && pending.method == "question/requested" {
				rpcID = pending.id
				break
			}
		}
		if rpcID == "" {
			time.Sleep(time.Millisecond)
		}
	}
	if rpcID == "" {
		t.Fatal("question interaction was not published")
	}
	if !e.ResolveInteraction(rpcID, map[string]any{"ok": true, "value": map[string]any{
		"sessionId": root,
		"answer":    map[string]any{"answers": []any{map[string]any{"id": "mode", "selected": []any{"Fast"}}}},
	}}) {
		t.Fatal("question interaction was not resolved")
	}
	select {
	case result := <-done:
		if text := modelToolResultText(result); !strings.Contains(text, `"selected":["Fast"]`) {
			t.Fatalf("question result = %q", text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ask_user_question did not resume")
	}

	child, err := e.CreateSubagent(context.Background(), root, "question-child", "")
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool := e.tools["ask_user_question"]
	e.mu.RUnlock()
	_, err = tool.Execute(context.Background(), ToolCall{Name: "ask_user_question", SessionID: child, Arguments: json.RawMessage(`{"questions":[{"id":"q","question":"Q?"}]}`)})
	if err == nil || !strings.Contains(err.Error(), "human interaction is unavailable") {
		t.Fatalf("delegated question error = %v", err)
	}
}

func TestGoalToolsPersistAndCompareRevision(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "goal-tools", "")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	s.mu.Lock()
	s.Running = true
	s.mu.Unlock()
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "user/message", map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: "set the goal"}},
		"source": map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}

	created := executeRegisteredTool(t, e, "create_goal", id, map[string]any{"objective": "ship parity", "max_goal_rounds": 4})
	if text := modelToolResultText(created); !strings.Contains(text, `"objective":"ship parity"`) || !strings.Contains(text, `"activation":"armed"`) {
		t.Fatalf("create_goal = %q", text)
	}
	goal, err := e.GetGoal(id)
	if err != nil || goal == nil {
		t.Fatalf("GetGoal() = %#v, %v", goal, err)
	}
	if goal.Revision != 1 || goal.MaxGoalRounds != 4 || goal.Phase != "active" {
		t.Fatalf("goal = %#v", goal)
	}

	executeRegisteredTool(t, e, "update_goal", id, map[string]any{
		"goal_id": goal.ID, "revision": goal.Revision, "action": "blocked", "blocked_reason": "external service is unavailable",
	})
	goal, _ = e.GetGoal(id)
	if goal.Phase != "blocked" || goal.Revision != 2 || goal.Activation != "disarmed" || goal.BlockedReason == nil || goal.BlockedReason.Message != "external service is unavailable" {
		t.Fatalf("blocked goal = %#v", goal)
	}

	e.mu.RLock()
	tool := e.tools["update_goal"]
	e.mu.RUnlock()
	_, err = executeToolRuntime(context.Background(), tool, ToolCall{Name: "update_goal", SessionID: id, Arguments: json.RawMessage(`{"goal_id":"` + goal.ID + `","revision":1,"action":"complete"}`)}, nil)
	if err == nil || !strings.Contains(err.Error(), "goal-conflict") {
		t.Fatalf("stale goal update error = %v", err)
	}
}

func TestGoalToolsRequireOpenDriverAndOperationAuthority(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "goal-authority", "")
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	getTool := e.tools["get_goal"]
	createTool := e.tools["create_goal"]
	e.mu.RUnlock()
	_, err = executeToolRuntime(context.Background(), getTool, ToolCall{Name: "get_goal", SessionID: id, Arguments: json.RawMessage(`{}`)}, nil)
	if err == nil || !strings.Contains(err.Error(), "GOAL_TOOL_DRIVER_REQUIRED") {
		t.Fatalf("get_goal outside driver error = %v", err)
	}
	s := mustSession(t, e, id)
	s.mu.Lock()
	s.Running = true
	s.mu.Unlock()
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := executeToolRuntime(context.Background(), getTool, ToolCall{Name: "get_goal", SessionID: id, Arguments: json.RawMessage(`{}`)}, nil); err != nil {
		t.Fatalf("get_goal in open driver = %v", err)
	}
	_, err = executeToolRuntime(context.Background(), createTool, ToolCall{Name: "create_goal", SessionID: id, Arguments: json.RawMessage(`{"objective":"ship"}`)}, nil)
	if err == nil || !strings.Contains(err.Error(), "GOAL_TOOL_AUTHORITY_REQUIRED") {
		t.Fatalf("create_goal without human source error = %v", err)
	}
	if _, err := e.appendEvent(s, "user/message", map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: "ship it"}},
		"source": map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := executeToolRuntime(context.Background(), createTool, ToolCall{Name: "create_goal", SessionID: id, Arguments: json.RawMessage(`{"objective":"ship"}`)}, nil); err != nil {
		t.Fatalf("create_goal with human source = %v", err)
	}
	if _, err := e.appendEvent(s, "turn/end", map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}); err != nil {
		t.Fatal(err)
	}
	_, err = executeToolRuntime(context.Background(), getTool, ToolCall{Name: "get_goal", SessionID: id, Arguments: json.RawMessage(`{}`)}, nil)
	if err == nil || !strings.Contains(err.Error(), "GOAL_TOOL_DRIVER_REQUIRED") {
		t.Fatalf("get_goal after turn error = %v", err)
	}
}

func TestAutonomousGoalTerminalUpdateDefersWrapupContext(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "goal-wrapup", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(id, "create", "finish the release", 0, 4); err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	s.mu.Lock()
	s.Running = true
	s.mu.Unlock()
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	goal, _ := e.GetGoal(id)
	item := &queuedPrompt{
		id: newID("msg"), content: []ContentBlock{{Type: "text", Text: renderGoalRoundPrompt(e.goals[id], 1)}},
		source:          map[string]any{"kind": "goal", "goalId": goal.ID, "revision": goal.Revision, "round": 1},
		goalReservation: true,
	}
	if _, err := e.admitPrompt(context.Background(), s, item); err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool := e.tools["update_goal"]
	e.mu.RUnlock()
	runtime := &ToolRunContext{}
	result, err := executeToolRuntime(context.Background(), tool, ToolCall{
		Name: "update_goal", SessionID: id,
		Arguments: json.RawMessage(`{"goal_id":"` + goal.ID + `","revision":1,"action":"complete"}`),
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if result.ConcludesTurn || len(result.AdditionalContexts) != 1 {
		t.Fatalf("terminal goal result = %#v", result)
	}
	context := result.AdditionalContexts[0]
	if context.Source["plugin"] != "tool-goal" || context.Source["summary"] != "complete: finish the release" ||
		!strings.Contains(contentValueText(context.Content), "<goal_complete>") || !strings.Contains(contentValueText(context.Content), "Do not call any more tools") {
		t.Fatalf("goal wrapup context = %#v", context)
	}
}

func TestSubagentToolsRunContinueForkAndList(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "subagent-tools", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(parent, ModelSelection{Provider: "echo", Model: "echo"}); err != nil {
		t.Fatal(err)
	}
	if text, err := e.Run(context.Background(), parent, PromptRequest{Literal: true, Content: []PromptContentPart{{Type: "text", Text: "parent context"}}}); err != nil || text != "parent context" {
		t.Fatalf("parent Run() = %q, %v", text, err)
	}

	result := executeRegisteredTool(t, e, "subagent", parent, map[string]any{
		"description": "echo child", "prompt": "child task", "run_in_background": false,
	})
	if text := modelToolResultText(result); text != "child task" {
		t.Fatalf("subagent result = %q", text)
	}
	value, _ := result.Value.(map[string]any)
	child, _ := value["runId"].(string)
	childSession := mustSession(t, e, child)
	childSession.mu.Lock()
	childMode, childAttached := childSession.Header.Mode, childSession.attached
	childSession.mu.Unlock()
	if childMode != "one-shot" || childAttached {
		t.Fatalf("foreground child mode=%q attached=%v", childMode, childAttached)
	}
	entries, err := e.listModelAgents(context.Background(), parent, false)
	if err != nil || len(entries) != 0 {
		t.Fatalf("list agents = %#v, %v", entries, err)
	}

	forked := executeRegisteredTool(t, e, "subagent_fork", parent, map[string]any{"description": "fork child", "prompt": "fork task", "run_in_background": false})
	if text := modelToolResultText(forked); text != "fork task" {
		t.Fatalf("subagent_fork result = %q", text)
	}
	forkValue, _ := forked.Value.(map[string]any)
	forkID, _ := forkValue["runId"].(string)
	forkSession := mustSession(t, e, forkID)
	forkSession.mu.Lock()
	seeded := forkSession.Header.SeedLength > 0
	forkSession.mu.Unlock()
	if !seeded {
		t.Fatal("forked subagent did not inherit completed turns")
	}
}

func TestModelSubagentConfigRestrictsOnlyGlobalTools(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "configured-subagent-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	maxDepth := 3
	childID, err := e.createModelSubagent(context.Background(), parent, "configured child", false, "continuable", SubagentToolConfig{
		Provider: "spawn", MaxDepth: &maxDepth,
		AgentOptions: &SubagentAgentOptions{Provider: "echo", Model: "child-model", MaxTokens: 321},
		Persona:      "You are the focused child.",
		ToolFilter:   &SubagentToolFilter{Allow: []string{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.getSession(childID)
	if err != nil {
		t.Fatal(err)
	}
	child.mu.Lock()
	selection := child.Model
	events := append([]Event(nil), child.Events...)
	child.mu.Unlock()
	if selection.Provider != "echo" || selection.Model != "child-model" || selection.MaxTokens != 321 {
		t.Fatalf("child model = %#v", selection)
	}
	identity := currentSubagentIdentity(events)
	if !reflect.DeepEqual(identity, map[string]any{"mode": "continuable", "label": "configured child", "seq": 1}) {
		t.Fatalf("child descriptor projection = %#v", identity)
	}
	descriptor, _ := events[1].Data.(map[string]any)
	if events[0].Type != "approval/policy" || events[1].Type != "subagent/descriptor" || descriptor["provider"] != "spawn" || descriptor["agentProvider"] != "echo" || descriptor["agentModel"] != "child-model" || descriptor["persona"] != "You are the focused child." {
		t.Fatalf("child descriptor = %#v", events[:2])
	}
	runtimeConfig, err := e.runtimeForSession(child)
	if err != nil || runtimeConfig.persona != "You are the focused child." {
		t.Fatalf("child runtime = %#v, %v", runtimeConfig, err)
	}
	if err := e.registerTool(Tool{
		Schema:  ToolSchema{Name: "child_private", Parameters: map[string]any{"type": "object"}},
		Execute: func(context.Context, ToolCall) (ToolResult, error) { return textToolResult("ok"), nil },
	}, childID); err != nil {
		t.Fatal(err)
	}
	visible, err := e.toolVisibleForSession(child, "read")
	if err != nil || visible {
		t.Fatalf("global read visibility = %v, %v", visible, err)
	}
	visible, err = e.toolVisibleForSession(child, "child_private")
	if err != nil || !visible {
		t.Fatalf("owner tool visibility = %v, %v", visible, err)
	}
	schemas, err := e.toolsForSession(child)
	if err != nil {
		t.Fatal(err)
	}
	if len(schemas) != 1 || schemas[0].Name != "child_private" {
		t.Fatalf("child schemas = %#v", schemas)
	}
	codeTools, err := e.codeToolsForSession(child)
	if err != nil {
		t.Fatal(err)
	}
	if len(codeTools) != 1 || codeTools["child_private"].Execute == nil {
		t.Fatalf("child code tools = %#v", codeTools)
	}

	zero := 0
	if _, err := e.createModelSubagent(context.Background(), parent, "too deep", false, "continuable", SubagentToolConfig{Provider: "spawn", MaxDepth: &zero}); err == nil || !strings.Contains(err.Error(), "depth 1 exceeds maxDepth 0") {
		t.Fatalf("maxDepth error = %v", err)
	}
}

func TestModelSubagentCompositionPersistsAcrossRestart(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = root, workspace
	cfg.Provider, cfg.Model, cfg.Persist = "echo", "echo", true
	cfg.SessionTitleLLM.Enabled = false
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	parent, err := engine.CreateSession(context.Background(), workspace, "persistent-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	maxDepth := 3
	childID, err := engine.createModelSubagent(context.Background(), parent, "persistent child", false, "continuable", SubagentToolConfig{
		Provider: "spawn", MaxDepth: &maxDepth, Persona: "Persistent child persona",
		ToolFilter: &SubagentToolFilter{Allow: []string{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	child, err := restarted.getSession(childID)
	if err != nil {
		t.Fatal(err)
	}
	child.mu.Lock()
	label, restriction := child.Title, child.toolRestriction
	child.mu.Unlock()
	if label != "persistent child" {
		t.Fatalf("restored label = %q", label)
	}
	if restriction == nil || !restriction.allowSet || len(restriction.allow) != 0 || restriction.denySet {
		t.Fatalf("restored restriction = %#v", restriction)
	}
	runtimeConfig, err := restarted.runtimeForSession(child)
	if err != nil || runtimeConfig.persona != "Persistent child persona" {
		t.Fatalf("restored runtime = %#v, %v", runtimeConfig, err)
	}
	visible, err := restarted.toolVisibleForSession(child, "read")
	if err != nil || visible {
		t.Fatalf("restored read visibility = %v, %v", visible, err)
	}
}

func TestContinuableModelSubagentNotifiesParent(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "settlement-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	result := executeRegisteredTool(t, e, "subagent", parent, map[string]any{
		"description": "notice child", "prompt": "closing answer",
	})
	value, ok := result.Value.(map[string]any)
	if !ok || value["kind"] != "continuable" {
		t.Fatalf("subagent result = %#v", result.Value)
	}
	childID, _ := value["subagentId"].(string)
	deadline := time.Now().Add(3 * time.Second)
	found := false
	for !found && time.Now().Before(deadline) {
		session, _ := e.getSession(parent)
		session.mu.Lock()
		for _, event := range session.Events {
			if event.Type != "user/message" {
				continue
			}
			data, _ := event.Data.(map[string]any)
			source, _ := data["source"].(map[string]any)
			if source["kind"] == "subagent-settled" && source["senderSessionId"] == childID && strings.Contains(contentValueText(data["content"]), "closing answer") {
				found = true
				break
			}
		}
		session.mu.Unlock()
		if !found {
			time.Sleep(time.Millisecond)
		}
	}
	if !found {
		t.Fatal("parent did not receive the subagent settlement notice")
	}
}

func TestModelSubagentSettlementPreservesCompleteAssistantBlocks(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	provider := newQueuedTestProvider()
	e.RegisterProvider(provider)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "settlement-block-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(parent, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	e.notifyModelSubagentSettlement(parent, "custom-child", []ContentBlock{
		{Type: "reasoning", Text: "complete reasoning"},
		{Type: "provider-artifact", Extra: map[string]any{"uri": "artifact://one", "count": float64(2)}},
	}, "completed")
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("settlement notice did not wake the parent")
	}
	requests := provider.requestSnapshot()
	if len(requests) != 1 || len(requests[0].Messages) == 0 {
		t.Fatalf("settlement requests = %#v", requests)
	}
	blocks := requests[0].Messages[len(requests[0].Messages)-1].Blocks
	want := []ContentBlock{
		{Type: "text", Text: "Background subagent custom-child finished and will do no further work unless you send it more."},
		{Type: "text", Text: "Its closing message:"},
		{Type: "reasoning", Text: "complete reasoning"},
		{Type: "provider-artifact", Extra: map[string]any{"uri": "artifact://one", "count": json.Number("2")}},
	}
	if !reflect.DeepEqual(blocks, want) {
		t.Fatalf("settlement blocks = %#v", blocks)
	}
}
