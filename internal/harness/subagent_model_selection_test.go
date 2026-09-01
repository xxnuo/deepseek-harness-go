package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type orderedSubagentModelProvider struct {
	id     string
	name   string
	models []ModelInfo
}

func (p *orderedSubagentModelProvider) ID() string   { return p.id }
func (p *orderedSubagentModelProvider) Name() string { return p.name }
func (p *orderedSubagentModelProvider) Models(context.Context) ([]ModelInfo, error) {
	return append([]ModelInfo(nil), p.models...), nil
}
func (*orderedSubagentModelProvider) Complete(context.Context, ChatRequest, func(Delta) error) (Completion, error) {
	return Completion{}, nil
}

func callListSubagentModelsForTest(t *testing.T, e *Engine, sessionID string, args map[string]any) (ToolResult, error) {
	t.Helper()
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	return listSubagentModelsTool(e).Execute(context.Background(), ToolCall{
		ID: "list-subagent-models", Name: "list_subagent_models", SessionID: sessionID, Arguments: encoded,
	})
}

func sessionWithSubagentModelPolicyForTest(t *testing.T, e *Engine, routes []AllowedModelRoute) string {
	t.Helper()
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "subagent-model-policy", "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(session, subagentModelSelectionPolicyEvent, map[string]any{
		"allowedModels": cloneAllowedModelRoutes(routes),
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

func enableSubagentModelSelectionForTest(t *testing.T, e *Engine) {
	t.Helper()
	_, rpcErr := e.settingsUpdate(subagentModelSelectionSettingsNamespace, map[string]any{
		"enabled": true,
		"allowedModels": []any{
			map[string]any{"provider": "prompt-capture", "model": "model-a"},
		},
	}, nil, true)
	if rpcErr != nil {
		t.Fatalf("settingsUpdate: %v", rpcErr)
	}
}

func setSubagentModelSelectionForTest(t *testing.T, e *Engine, enabled bool) {
	t.Helper()
	patch := map[string]any{"enabled": enabled}
	if enabled {
		patch["allowedModels"] = []any{
			map[string]any{"provider": "prompt-capture", "model": "model-a"},
		}
	}
	if _, rpcErr := e.settingsUpdate(subagentModelSelectionSettingsNamespace, patch, nil, false); rpcErr != nil {
		t.Fatalf("settingsUpdate(%v): %v", enabled, rpcErr)
	}
}

func registerSelectableSubagentToolForTest(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.RegisterSubagentTool(SubagentToolConfig{
		Provider: "spawn", ToolName: "selectable-subagent", ModelSelectionSettings: true,
	}); err != nil {
		t.Fatalf("RegisterSubagentTool: %v", err)
	}
}

func selectableSubagentSchemaForTest(t *testing.T, e *Engine, sessionID string) (ToolSchema, bool, bool) {
	t.Helper()
	session, err := e.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := e.toolsForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	var selectable ToolSchema
	selectablePresent, listPresent := false, false
	for _, row := range rows {
		switch row.Name {
		case "selectable-subagent":
			selectable, selectablePresent = row, true
		case "list_subagent_models":
			listPresent = true
		}
	}
	return selectable, selectablePresent, listPresent
}

func hasSchemaProperty(schema ToolSchema, name string) bool {
	properties, _ := schema.Parameters["properties"].(map[string]any)
	_, ok := properties[name]
	return ok
}

func createModelSelectionSessionForTest(t *testing.T, e *Engine, id string) string {
	t.Helper()
	created, err := e.CreateSession(t.Context(), e.Config().Workspace, id, "")
	if err != nil {
		t.Fatal(err)
	}
	return created
}

func TestSubagentModelSelectionSnapshotIsBoundAtSessionPublication(t *testing.T) {
	e := newIntegrationEngine(t)
	registerSelectableSubagentToolForTest(t, e)
	setSubagentModelSelectionForTest(t, e, false)
	disabledID := createModelSelectionSessionForTest(t, e, "model-selection-disabled-at-publication")

	setSubagentModelSelectionForTest(t, e, true)
	enabledID := createModelSelectionSessionForTest(t, e, "model-selection-enabled-at-publication")
	setSubagentModelSelectionForTest(t, e, false)

	disabled, present, list := selectableSubagentSchemaForTest(t, e, disabledID)
	if !present || hasSchemaProperty(disabled, "provider") || hasSchemaProperty(disabled, "model") || hasSchemaProperty(disabled, "reasoning_effort") || list {
		t.Fatalf("disabled session leaked selectable fields/list tool: schema=%#v list=%v", disabled, list)
	}
	enabled, present, list := selectableSubagentSchemaForTest(t, e, enabledID)
	if !present || !hasSchemaProperty(enabled, "provider") || !hasSchemaProperty(enabled, "model") || !hasSchemaProperty(enabled, "reasoning_effort") || !list {
		t.Fatalf("enabled session lost selectable fields/list tool: schema=%#v list=%v", enabled, list)
	}
}

func TestSubagentModelSelectionSnapshotDoesNotChangeWhenSettingsFlipBeforeFirstAccess(t *testing.T) {
	e := newIntegrationEngine(t)
	registerSelectableSubagentToolForTest(t, e)
	setSubagentModelSelectionForTest(t, e, true)
	enabledID := createModelSelectionSessionForTest(t, e, "model-selection-enabled-before-flip")
	setSubagentModelSelectionForTest(t, e, false)

	enabled, present, list := selectableSubagentSchemaForTest(t, e, enabledID)
	if !present || !hasSchemaProperty(enabled, "provider") || !hasSchemaProperty(enabled, "model") || !hasSchemaProperty(enabled, "reasoning_effort") || !list {
		t.Fatalf("session lost publication snapshot after settings flip: schema=%#v list=%v", enabled, list)
	}
}

func TestSubagentModelSelectionLateToolInstallSamplesExistingSessionOnce(t *testing.T) {
	e := newIntegrationEngine(t)
	setSubagentModelSelectionForTest(t, e, false)
	existingID := createModelSelectionSessionForTest(t, e, "model-selection-late-install")

	setSubagentModelSelectionForTest(t, e, true)
	registerSelectableSubagentToolForTest(t, e)
	setSubagentModelSelectionForTest(t, e, false)

	enabled, present, list := selectableSubagentSchemaForTest(t, e, existingID)
	if !present || !hasSchemaProperty(enabled, "provider") || !hasSchemaProperty(enabled, "model") || !hasSchemaProperty(enabled, "reasoning_effort") || !list {
		t.Fatalf("late installation did not capture setting at install boundary: schema=%#v list=%v", enabled, list)
	}

	// A second settings flip must not rewrite the already-installed definition.
	setSubagentModelSelectionForTest(t, e, false)
	enabled, present, list = selectableSubagentSchemaForTest(t, e, existingID)
	if !present || !hasSchemaProperty(enabled, "provider") || !hasSchemaProperty(enabled, "model") || !hasSchemaProperty(enabled, "reasoning_effort") || !list {
		t.Fatalf("late-install snapshot was resampled: schema=%#v list=%v", enabled, list)
	}
}

func TestSubagentModelSelectionChildInheritsParentPublicationSnapshot(t *testing.T) {
	e := newIntegrationEngine(t)
	registerSelectableSubagentToolForTest(t, e)
	setSubagentModelSelectionForTest(t, e, true)
	parentID := createModelSelectionSessionForTest(t, e, "model-selection-parent-snapshot")

	// The parent has not queried its tool directory yet. Its child must still
	// inherit the decision captured at parent publication, not this later value.
	setSubagentModelSelectionForTest(t, e, false)
	childID, err := e.createModelSubagent(t.Context(), parentID, "snapshot child", false, "one-shot", SubagentToolConfig{Provider: "spawn"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.getSession(childID)
	if err != nil {
		t.Fatal(err)
	}
	routes, present, err := subagentModelSelectionPolicyForSession(child)
	if err != nil {
		t.Fatal(err)
	}
	want := []AllowedModelRoute{{Provider: "prompt-capture", Model: "model-a"}}
	if !present || len(routes) != len(want) || routes[0] != want[0] {
		t.Fatalf("child policy = %#v, present=%v", routes, present)
	}
}

func TestSubagentModelSelectionRestoredSeedNeverSamplesCurrentSettings(t *testing.T) {
	e := newIntegrationEngine(t)
	registerSelectableSubagentToolForTest(t, e)
	setSubagentModelSelectionForTest(t, e, true)
	seed := []Event{{Type: "user/message", Seq: 0}}
	s := &Session{
		Header:       SessionHeader{ID: "restored-seeded-model-selection"},
		Events:       seed,
		firstLiveSeq: len(seed),
		invariants:   e.invariants,
	}

	if _, err := e.ensureSubagentModelSelectionTools(s); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	sampled := s.subagentModelSelectionSampled
	snapshot := s.subagentModelSelectionSnapshot
	s.mu.Unlock()
	if !sampled || snapshot != nil {
		t.Fatalf("restored session sampled current settings: sampled=%v snapshot=%#v", sampled, snapshot)
	}
	if _, present, err := subagentModelSelectionPolicyForSession(s); err != nil || present {
		t.Fatalf("restored policy = present=%v err=%v", present, err)
	}
}

func TestSubagentModelSelectionSamplesNewPresetSessionWithInitialEvents(t *testing.T) {
	e := newIntegrationEngine(t)
	enableSubagentModelSelectionForTest(t, e)

	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "model-selection-preset", "standard")
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	initialEvents := len(s.Events)
	s.mu.Unlock()
	if initialEvents == 0 {
		t.Fatal("preset session did not contain the expected initial policy events")
	}

	routes, enabled, err := e.sampleSubagentModelSelection(s)
	if err != nil {
		t.Fatal(err)
	}
	if !enabled || len(routes) != 1 || routes[0] != (AllowedModelRoute{Provider: "prompt-capture", Model: "model-a"}) {
		t.Fatalf("sample = %#v, enabled=%v", routes, enabled)
	}
}

func TestSubagentModelSelectionDoesNotSampleRestoredSession(t *testing.T) {
	e := newIntegrationEngine(t)
	enableSubagentModelSelectionForTest(t, e)
	s := &Session{
		Header:       SessionHeader{ID: "restored-model-selection"},
		Events:       []Event{{Type: "user/message", Seq: 0}},
		firstLiveSeq: 1,
		invariants:   e.invariants,
	}

	routes, enabled, err := e.sampleSubagentModelSelection(s)
	if err != nil {
		t.Fatal(err)
	}
	if enabled || len(routes) != 0 {
		t.Fatalf("restored sample = %#v, enabled=%v", routes, enabled)
	}
}

func TestPreflightSubagentLlmRejectsEffortWithoutReasoningMetadata(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &promptCaptureProvider{}
	e.RegisterProvider(provider)

	err := e.preflightSubagentLlm(context.Background(), ModelSelection{}, &SubagentAgentOptions{
		Provider: provider.ID(), Model: "model-a", ReasoningEffort: "high",
	}, false)
	if err == nil || !strings.Contains(err.Error(), `model "model-a" does not support reasoning effort "high"`) {
		t.Fatalf("preflight error = %v", err)
	}
}

func TestPreflightSubagentLlmValidatesMaxTokensCallConfig(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &orderedSubagentModelProvider{
		id: "max-token-config", name: "Max Token Config",
		models: []ModelInfo{{ID: "valid", Name: "Valid", MaxTokens: 256000}, {ID: "invalid", Name: "Invalid", MaxTokens: -1}},
	}
	e.RegisterProvider(provider)

	if err := e.preflightSubagentLlm(context.Background(), ModelSelection{}, &SubagentAgentOptions{
		Provider: provider.ID(), Model: "valid", MaxTokens: 8192,
	}, false); err != nil {
		t.Fatalf("explicit maxTokens preflight: %v", err)
	}
	if err := e.preflightSubagentLlm(context.Background(), ModelSelection{}, &SubagentAgentOptions{
		Provider: provider.ID(), Model: "valid", MaxTokens: -1,
	}, false); err == nil || !strings.Contains(err.Error(), "positive safe integer") {
		t.Fatalf("invalid explicit maxTokens error = %v", err)
	}
	if err := e.preflightSubagentLlm(context.Background(), ModelSelection{}, &SubagentAgentOptions{
		Provider: provider.ID(), Model: "invalid", MaxTokens: 8192,
	}, false); err == nil || !strings.Contains(err.Error(), "invalid default maxTokens") {
		t.Fatalf("invalid model maxTokens error = %v", err)
	}
}

func TestListSubagentModelsPreservesProviderAndModelOrder(t *testing.T) {
	e := newIntegrationEngine(t)
	e.RegisterProvider(&orderedSubagentModelProvider{
		id: "zeta", name: "Zeta", models: []ModelInfo{
			{ID: "second", Name: "Second"},
			{ID: "first", Name: "First"},
		},
	})
	e.RegisterProvider(&orderedSubagentModelProvider{
		id: "alpha", name: "Alpha", models: []ModelInfo{{ID: "only", Name: "Only"}},
	})
	sessionID := sessionWithSubagentModelPolicyForTest(t, e, []AllowedModelRoute{
		{Provider: "zeta", Model: "second"},
		{Provider: "zeta", Model: "first"},
		{Provider: "alpha", Model: "only"},
	})

	providers, err := callListSubagentModelsForTest(t, e, sessionID, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if got := modelToolResultText(providers); got != "zeta — Zeta\nalpha — Alpha" {
		t.Fatalf("provider order = %q", got)
	}
	models, err := callListSubagentModelsForTest(t, e, sessionID, map[string]any{"provider": "zeta"})
	if err != nil {
		t.Fatal(err)
	}
	if got := modelToolResultText(models); got != "zeta/second — Second\nzeta/first — First" {
		t.Fatalf("model order = %q", got)
	}
}

func TestListSubagentModelsReportsRegisteredAlternatives(t *testing.T) {
	tests := []struct {
		name string
		add  []Provider
		want string
	}{
		{
			name: "registered policy intersection",
			add: []Provider{
				&orderedSubagentModelProvider{id: "zeta", name: "Zeta"},
				&orderedSubagentModelProvider{id: "alpha", name: "Alpha"},
			},
			want: "available providers: zeta, alpha",
		},
		{name: "empty intersection", want: "available providers: (none)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := newIntegrationEngine(t)
			for _, provider := range test.add {
				e.RegisterProvider(provider)
			}
			sessionID := sessionWithSubagentModelPolicyForTest(t, e, []AllowedModelRoute{
				{Provider: "missing", Model: "missing-model"},
				{Provider: "zeta", Model: "zeta-model"},
				{Provider: "unregistered", Model: "unregistered-model"},
				{Provider: "alpha", Model: "alpha-model"},
			})
			_, err := callListSubagentModelsForTest(t, e, sessionID, map[string]any{"provider": "missing"})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("missing provider error = %v", err)
			}
			if strings.Contains(err.Error(), "unregistered") {
				t.Fatalf("missing provider leaked unavailable alternative: %v", err)
			}
		})
	}
}
