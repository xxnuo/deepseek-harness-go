package harness

import (
	"context"
	"strings"
	"testing"
)

func selectableSubagentToolForRollbackTest(name string) Tool {
	return Tool{
		Schema: ToolSchema{
			Name: name,
			Parameters: objectSchema(map[string]any{
				"provider":         map[string]any{"type": "string"},
				"model":            map[string]any{"type": "string"},
				"reasoning_effort": map[string]any{"type": "string"},
			}),
		},
		subagentModelSelectionCapable: true,
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return textToolResult("ok"), nil
		},
	}
}

func installToolsChangeFailOnceForRollbackTest(t *testing.T, e *Engine, sessionID string) {
	t.Helper()
	_, _ = runDynamicBuiltinPlugin(t, e, sessionID, "msrb", `
return {
  apply(ctx) {
    let failed = false
    ctx.on('tools/change', () => {
      if (!failed) {
        failed = true
        throw new Error('tools/change rollback once')
      }
    })
  }
}`)
}

func modelSelectionSnapshotStateForRollbackTest(t *testing.T, e *Engine, sessionID string) (bool, bool) {
	t.Helper()
	s, err := e.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subagentModelSelectionSnapshot != nil, s.subagentModelSelectionSampled
}

func TestSubagentModelSelectionFailedDisabledToolInstallRollsBackSnapshot(t *testing.T) {
	e := newIntegrationEngine(t)
	setSubagentModelSelectionForTest(t, e, false)
	sessionID := createModelSelectionSessionForTest(t, e, "model-selection-failed-disabled-install")
	installToolsChangeFailOnceForRollbackTest(t, e, sessionID)

	err := e.RegisterTool(selectableSubagentToolForRollbackTest("selectable-subagent"))
	if err == nil || !strings.Contains(err.Error(), "tools/change rollback once") {
		t.Fatalf("failed registration error = %v", err)
	}
	for _, schema := range e.ListTools() {
		if schema.Name == "selectable-subagent" {
			t.Fatal("failed selectable registration leaked the tool")
		}
	}
	if snapshot, sampled := modelSelectionSnapshotStateForRollbackTest(t, e, sessionID); snapshot || sampled {
		t.Fatalf("disabled failed install retained transient state: snapshot=%v sampled=%v", snapshot, sampled)
	}

	setSubagentModelSelectionForTest(t, e, true)
	if err := e.RegisterTool(selectableSubagentToolForRollbackTest("selectable-subagent")); err != nil {
		t.Fatalf("retry registration: %v", err)
	}
	selectable, present, list := selectableSubagentSchemaForTest(t, e, sessionID)
	if !present || !hasSchemaProperty(selectable, "provider") || !hasSchemaProperty(selectable, "model") || !hasSchemaProperty(selectable, "reasoning_effort") || !list {
		t.Fatalf("retry did not resample enabled settings: schema=%#v list=%v", selectable, list)
	}
}

func TestSubagentModelSelectionFailedEnabledToolInstallKeepsSnapshot(t *testing.T) {
	e := newIntegrationEngine(t)
	setSubagentModelSelectionForTest(t, e, true)
	sessionID := createModelSelectionSessionForTest(t, e, "model-selection-failed-enabled-install")
	installToolsChangeFailOnceForRollbackTest(t, e, sessionID)

	err := e.RegisterTool(selectableSubagentToolForRollbackTest("selectable-subagent"))
	if err == nil || !strings.Contains(err.Error(), "tools/change rollback once") {
		t.Fatalf("failed registration error = %v", err)
	}
	for _, schema := range e.ListTools() {
		if schema.Name == "selectable-subagent" {
			t.Fatal("failed selectable registration leaked the tool")
		}
	}
	routes, present, policyErr := subagentModelSelectionPolicyForSession(mustSessionForModelSelectionRollbackTest(t, e, sessionID))
	if policyErr != nil || !present || len(routes) != 1 || routes[0] != (AllowedModelRoute{Provider: "prompt-capture", Model: "model-a"}) {
		t.Fatalf("failed enabled install policy = %#v, present=%v err=%v", routes, present, policyErr)
	}
	setSubagentModelSelectionForTest(t, e, false)
	if err := e.RegisterTool(selectableSubagentToolForRollbackTest("selectable-subagent")); err != nil {
		t.Fatalf("retry registration: %v", err)
	}
	selectable, present, list := selectableSubagentSchemaForTest(t, e, sessionID)
	if !present || !hasSchemaProperty(selectable, "provider") || !hasSchemaProperty(selectable, "model") || !hasSchemaProperty(selectable, "reasoning_effort") || !list {
		t.Fatalf("enabled snapshot was lost after failed install: schema=%#v list=%v", selectable, list)
	}
}

func mustSessionForModelSelectionRollbackTest(t *testing.T, e *Engine, sessionID string) *Session {
	t.Helper()
	session, err := e.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	return session
}
