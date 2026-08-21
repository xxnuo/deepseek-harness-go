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
	result, err := tool.Execute(context.Background(), ToolCall{ID: "call-test", Name: name, Arguments: data, SessionID: sessionID, Workspace: workspace})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return result
}

func modelToolResultText(result ToolResult) string {
	return contentValueText(result.Content)
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
	_, err = tool.Execute(context.Background(), ToolCall{Name: "update_goal", SessionID: id, Arguments: json.RawMessage(`{"goal_id":"` + goal.ID + `","revision":1,"action":"complete"}`)})
	if err == nil || !strings.Contains(err.Error(), "goal-conflict") {
		t.Fatalf("stale goal update error = %v", err)
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
	entries, err := e.listModelAgents(context.Background(), parent, false)
	if err != nil || len(entries) != 1 {
		t.Fatalf("list agents = %#v, %v", entries, err)
	}
	child := entries[0].ID
	if entries[0].Label != "echo child" || entries[0].Status != "idle" {
		t.Fatalf("child entry = %#v", entries[0])
	}

	executeRegisteredTool(t, e, "send_message", parent, map[string]any{"subagent_id": child, "message": "follow up"})
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := e.WaitForIdle(waitCtx, child); err != nil {
		t.Fatal(err)
	}
	history, _, err := e.History(child, -1, 100)
	if err != nil {
		t.Fatal(err)
	}
	foundFollowup := false
	for _, entry := range history {
		if entry.Event.Type == "user/message" && strings.Contains(contentValueText(entry.Event.Data), "follow up") {
			foundFollowup = true
		}
	}
	if !foundFollowup {
		t.Fatal("continued subagent history does not contain follow up")
	}

	forked := executeRegisteredTool(t, e, "subagent_fork", parent, map[string]any{"description": "fork child", "prompt": "fork task"})
	if text := modelToolResultText(forked); text != "fork task" {
		t.Fatalf("subagent_fork result = %q", text)
	}
	entries, err = e.listModelAgents(context.Background(), parent, false)
	if err != nil || len(entries) != 2 {
		t.Fatalf("children after fork = %#v, %v", entries, err)
	}
	var seeded bool
	for _, entry := range entries {
		if entry.Label != "fork child" {
			continue
		}
		s, _ := e.getSession(entry.ID)
		s.mu.Lock()
		seeded = s.Header.SeedLength > 0
		s.mu.Unlock()
	}
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
	if !reflect.DeepEqual(identity, map[string]any{"mode": "continuable", "label": "configured child", "seq": 0}) {
		t.Fatalf("child descriptor projection = %#v", identity)
	}
	descriptor, _ := events[0].Data.(map[string]any)
	if events[0].Type != "subagent/descriptor" || descriptor["provider"] != "spawn" || descriptor["agentProvider"] != "echo" || descriptor["agentModel"] != "child-model" || descriptor["persona"] != "You are the focused child." {
		t.Fatalf("child descriptor = %#v", events[0])
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
	if _, err := e.createModelSubagent(context.Background(), parent, "too deep", false, "continuable", SubagentToolConfig{Provider: "spawn", MaxDepth: &zero}); err == nil || !strings.Contains(err.Error(), "maximum depth 0") {
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
	e := newIntegrationEngine(t)
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
