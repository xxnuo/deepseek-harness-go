package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestCordisInspectListsHostThenClientProviders(t *testing.T) {
	e := newIntegrationEngine(t)
	if err := e.SyncInspectManifest([]CordisInspectProviderManifest{{
		ID: "Client", Description: "client facts", Methods: []CordisInspectMethodManifest{{
			Name: "read", Description: "read facts",
			InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	providers := e.ListInspectProviders()
	want := []string{"host:Service", "host:Event", "host:Builtin", "host:Tool", "client:Client"}
	if len(providers) != len(want) {
		t.Fatalf("providers = %#v", providers)
	}
	for index, provider := range providers {
		if got := provider.Platform + ":" + provider.ID; got != want[index] {
			t.Fatalf("providers[%d] = %q, want %q", index, got, want[index])
		}
	}
	providers[0].Methods[0].InputSchema.(map[string]any)["type"] = "string"
	if got := e.ListInspectProviders()[0].Methods[0].InputSchema.(map[string]any)["type"]; got != "object" {
		t.Fatalf("host manifest mutated through list result: %v", got)
	}
}

func TestCordisHostInspectProviders(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-host-inspect", "")
	if err != nil {
		t.Fatal(err)
	}

	serviceCatalog := hostInspectQuery(t, e, sessionID, "Service", "listService", nil)
	if serviceCatalog["mode"] != "catalog" || !catalogHasNamedEntry(serviceCatalog["services"], "key", "tools") {
		t.Fatalf("service catalog = %#v", serviceCatalog)
	}
	service := hostInspectQuery(t, e, sessionID, "Service", "listService", map[string]any{"service": "tools"})
	serviceEntry, _ := service["service"].(map[string]any)
	referencedTypes, _ := service["referencedTypes"].([]any)
	if service["mode"] != "service" || serviceEntry["key"] != "tools" || len(referencedTypes) == 0 {
		t.Fatalf("service result = %#v", service)
	}
	if _, err := e.QueryHostInspectProvider(context.Background(), sessionID, "Service", "listService", map[string]any{"service": ""}); err == nil || !strings.Contains(err.Error(), "no catalogued Service") {
		t.Fatalf("empty exact service error = %v", err)
	}
	if _, err := e.QueryHostInspectProvider(context.Background(), sessionID, "Service", "listService", map[string]any{"extra": true}); err == nil || !strings.Contains(err.Error(), "rejected input") {
		t.Fatalf("invalid service input error = %v", err)
	}

	eventCatalog := hostInspectQuery(t, e, sessionID, "Event", "listEvents", nil)
	if eventCatalog["mode"] != "catalog" || catalogHasPrefix(eventCatalog["events"], "name", "cordis/") {
		t.Fatalf("event catalog = %#v", eventCatalog)
	}
	event := hostInspectQuery(t, e, sessionID, "Event", "listEvents", map[string]any{"event": "agent/created"})
	eventEntry, _ := event["event"].(map[string]any)
	if event["mode"] != "event" || eventEntry["name"] != "agent/created" {
		t.Fatalf("event result = %#v", event)
	}

	builtins := hostInspectQuery(t, e, sessionID, "Builtin", "listBuiltins", nil)
	if !catalogHasNamedEntry(builtins["builtins"], "name", "ctx") || !catalogHasNamedEntry(builtins["builtins"], "name", "harness") {
		t.Fatalf("builtins = %#v", builtins)
	}
	if _, err := e.QueryHostInspectProvider(context.Background(), sessionID, "Builtin", "unknown", nil); err == nil || !strings.Contains(err.Error(), "has no method") {
		t.Fatalf("unknown method error = %v", err)
	}
}

func TestCordisHostToolInspectUsesSessionVisibilityAndTool(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-tool-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	other, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-tool-other", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.registerTool(Tool{
		Schema:  ToolSchema{Name: "owner_only", Description: "private", Parameters: objectSchema(map[string]any{})},
		Execute: func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil },
	}, owner); err != nil {
		t.Fatal(err)
	}
	ownerTools := hostInspectQuery(t, e, owner, "Tool", "listTools", nil)
	otherTools := hostInspectQuery(t, e, other, "Tool", "listTools", nil)
	if !catalogHasNamedEntry(ownerTools["tools"], "name", "owner_only") || catalogHasNamedEntry(otherTools["tools"], "name", "owner_only") {
		t.Fatalf("owner tools = %#v, other tools = %#v", ownerTools, otherTools)
	}

	e.mu.RLock()
	tool := e.tools["cordis_inspect_query"]
	e.mu.RUnlock()
	arguments, _ := json.Marshal(map[string]any{
		"platform": "host", "provider": "Builtin", "method": "listBuiltins", "input": map[string]any{},
	})
	result, err := tool.Execute(context.Background(), ToolCall{SessionID: owner, Arguments: arguments})
	if err != nil {
		t.Fatal(err)
	}
	value, _ := result.Value.(map[string]any)
	if value["platform"] != "host" || value["provider"] != "Builtin" {
		t.Fatalf("tool result = %#v", result.Value)
	}
}

func hostInspectQuery(t *testing.T, e *Engine, sessionID, provider, method string, input any) map[string]any {
	t.Helper()
	value, err := e.QueryHostInspectProvider(context.Background(), sessionID, provider, method, input)
	if err != nil {
		t.Fatal(err)
	}
	record, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s.%s = %#v", provider, method, value)
	}
	return record
}

func catalogHasNamedEntry(value any, field, want string) bool {
	entries, _ := value.([]any)
	for _, value := range entries {
		entry, _ := value.(map[string]any)
		if entry[field] == want {
			return true
		}
	}
	return false
}

func catalogHasPrefix(value any, field, prefix string) bool {
	entries, _ := value.([]any)
	for _, value := range entries {
		entry, _ := value.(map[string]any)
		name, _ := entry[field].(string)
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
