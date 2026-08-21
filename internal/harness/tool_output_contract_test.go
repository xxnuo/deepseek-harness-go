package harness

import (
	"context"
	"strings"
	"testing"
)

func TestShippedToolsDeclareCanonicalOutputs(t *testing.T) {
	e := newIntegrationEngine(t)
	if err := e.EnableLSPTool(LSPToolConfig{}); err != nil {
		t.Fatal(err)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	for name, tool := range e.tools {
		if shippedToolNames[name] && tool.Schema.Output == nil {
			t.Errorf("shipped tool %q has no output schema", name)
		}
	}
}

func TestPersistentBashAdvertisesStringOutput(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "persistent-output", "minimal")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}

	schemas, err := e.toolsForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	for _, schema := range schemas {
		if schema.Name == "bash" && schema.Output["type"] != "string" {
			t.Fatalf("persistent native bash output = %#v", schema.Output)
		}
	}

	tools, err := e.codeToolsForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	if output := tools["bash"].Schema.Output; output["type"] != "string" {
		t.Fatalf("persistent Code Mode bash output = %#v", output)
	}
}

func TestCustomToolWithoutOutputFallsBackToJsonValue(t *testing.T) {
	e := newIntegrationEngine(t)
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "external_unknown", Parameters: objectSchema(map[string]any{})},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return textToolResult("opaque"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "custom-output", "code")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	runtimeConfig, err := e.runtimeForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	sdk := e.codeModePrompt(session, runtimeConfig)
	if !strings.Contains(sdk, `"external_unknown": (args: {  }) => Promise<JsonValue>`) {
		t.Fatalf("custom tool SDK did not fall back to JsonValue:\n%s", sdk)
	}
}
