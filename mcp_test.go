package harness

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const mcpFixtureEnv = "GO_DSH_MCP_FIXTURE"

func newMCPFixtureServer() *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "dsh-go-fixture", Version: "1.0.0"}, nil)
	server.AddTool(&mcp.Tool{
		Name:         "add",
		Description:  "Adds two numbers.",
		InputSchema:  json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}},"required":["a","b"]}`),
		OutputSchema: json.RawMessage(`{"type":"object","properties":{"sum":{"type":"number"}},"required":["sum"],"additionalProperties":false}`),
	}, func(_ context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var arguments struct {
			A float64 `json:"a"`
			B float64 `json:"b"`
		}
		if err := json.Unmarshal(request.Params.Arguments, &arguments); err != nil {
			return nil, err
		}
		sum := arguments.A + arguments.B
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("%g", sum)}}, StructuredContent: map[string]any{"sum": sum}}, nil
	})
	server.AddTool(&mcp.Tool{Name: "admin.reset", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "reset done"}}}, nil
	})
	server.AddTool(&mcp.Tool{Name: "fail", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "fixture failure"}}, IsError: true}, nil
	})
	server.AddTool(&mcp.Tool{Name: "environment", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		text := fmt.Sprintf("visible=%s dsh=%t secret=%t", os.Getenv("MCP_VISIBLE"), os.Getenv("DSH_SHOULD_NOT_LEAK") != "", os.Getenv("MCP_SECRET_TOKEN") != "")
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil
	})
	server.AddTool(&mcp.Tool{Name: "crash", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		time.AfterFunc(100*time.Millisecond, func() { os.Exit(7) })
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "crashing"}}}, nil
	})
	server.AddTool(&mcp.Tool{Name: "image", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		data, _ := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC")
		return &mcp.CallToolResult{Content: []mcp.Content{
			&mcp.TextContent{Text: "before image"},
			&mcp.ImageContent{Data: data, MIMEType: "image/png"},
			&mcp.TextContent{Text: "after image"},
		}}, nil
	})
	return server
}

func TestMCPStdioFixture(t *testing.T) {
	if os.Getenv(mcpFixtureEnv) != "1" {
		return
	}
	server := newMCPFixtureServer()
	session, err := server.Connect(context.Background(), &mcp.StdioTransport{}, nil)
	if err != nil {
		os.Exit(2)
	}
	_ = session.Wait()
	os.Exit(0)
}

func newMCPTestEngine(t *testing.T) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = t.TempDir()
	cfg.Persist = false
	cfg.SessionTitleLLM.Enabled = false
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func callMCPTool(t *testing.T, engine *Engine, name, arguments string) (ToolResult, error) {
	t.Helper()
	tool := registeredTool(t, engine, name)
	return tool.Execute(t.Context(), ToolCall{Name: name, Arguments: json.RawMessage(arguments)})
}

func TestMCPPublicToolNameMatchesUpstreamContract(t *testing.T) {
	if got := MCPPublicToolName("memory", "search"); got != "mcp__memory__search" {
		t.Fatalf("clean name = %q", got)
	}
	first := MCPPublicToolName("memory", "admin.reset")
	second := MCPPublicToolName("memory", "admin/reset")
	if first == second || len(first) > 64 || !regexp.MustCompile(`^[A-Za-z0-9_-]+$`).MatchString(first) {
		t.Fatalf("normalized names = %q, %q", first, second)
	}
	if first != MCPPublicToolName("memory", "admin.reset") {
		t.Fatal("public name is not deterministic")
	}
	if got := MCPPublicToolName("memory", "x😀y"); !strings.HasPrefix(got, "mcp__memory__x__y_") {
		t.Fatalf("UTF-16 normalization = %q", got)
	}
}

func TestMCPStdioDiscoveryExecutionEnvironmentAndReconnect(t *testing.T) {
	t.Setenv("DSH_SHOULD_NOT_LEAK", "private")
	t.Setenv("MCP_SECRET_TOKEN", "private")
	engine := newMCPTestEngine(t)
	connection, err := engine.ConnectMCP(t.Context(), MCPConfig{
		Transport:          MCPTransportStdio,
		ServerName:         "fixture",
		Command:            os.Args[0],
		Args:               []string{"-test.run=^TestMCPStdioFixture$"},
		Env:                map[string]string{mcpFixtureEnv: "1", "MCP_VISIBLE": "yes"},
		ToolCallTimeout:    time.Second,
		FailOnStartupError: true,
		Reconnect: MCPReconnectConfig{
			InitialDelay: 20 * time.Millisecond,
			MaxDelay:     80 * time.Millisecond,
			MaxAttempts:  10,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	result, err := callMCPTool(t, engine, "mcp__fixture__add", `{"a":2,"b":3}`)
	if err != nil || len(result.Content) != 1 || result.Content[0].Text != "5" {
		t.Fatalf("add result = %#v, err=%v", result, err)
	}
	value, ok := result.Value.(map[string]any)
	structured, _ := value["structuredContent"].(map[string]any)
	if !ok || len(value["content"].([]any)) != 1 || structured["sum"] != float64(5) {
		t.Fatalf("canonical MCP value = %#v", result.Value)
	}
	tool := registeredTool(t, engine, "mcp__fixture__add")
	properties, _ := tool.Schema.Output["properties"].(map[string]any)
	structuredSchema, _ := properties["structuredContent"].(map[string]any)
	structuredProperties, _ := structuredSchema["properties"].(map[string]any)
	if structuredSchema["type"] != "object" || structuredProperties["sum"] == nil {
		t.Fatalf("MCP output schema = %#v", tool.Schema.Output)
	}

	normalized := MCPPublicToolName("fixture", "admin.reset")
	result, err = callMCPTool(t, engine, normalized, `{}`)
	if err != nil || result.Content[0].Text != "reset done" {
		t.Fatalf("normalized call = %#v, err=%v", result, err)
	}
	result, err = callMCPTool(t, engine, "mcp__fixture__environment", `{}`)
	if err != nil || result.Content[0].Text != "visible=yes dsh=false secret=false" {
		t.Fatalf("environment = %#v, err=%v", result, err)
	}
	if _, err := callMCPTool(t, engine, "mcp__fixture__fail", `{}`); err == nil || !strings.Contains(err.Error(), "fixture failure") {
		t.Fatalf("MCP error = %v", err)
	}
	result, err = callMCPTool(t, engine, "mcp__fixture__image", `{}`)
	if err != nil || len(result.Content) != 3 || !strings.Contains(result.Content[1].Text, "image unavailable") {
		t.Fatalf("image fallback = %#v, err=%v", result, err)
	}
	imageValue := result.Value.(map[string]any)["content"].([]any)[1].(map[string]any)
	if imageValue["data"] == "" || imageValue["mimeType"] != "image/png" {
		t.Fatalf("canonical image block = %#v", imageValue)
	}

	result, err = callMCPTool(t, engine, "mcp__fixture__crash", `{}`)
	if err != nil || result.Content[0].Text != "crashing" {
		t.Fatalf("crash result = %#v, err=%v", result, err)
	}
	// The fixture exits after its reply; wait past that edge before accepting a
	// successful call so this proves a new child generation was connected.
	time.Sleep(200 * time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for {
		engine.mu.RLock()
		tool, present := engine.tools["mcp__fixture__add"]
		engine.mu.RUnlock()
		if present {
			result, err = tool.Execute(t.Context(), ToolCall{Name: tool.Schema.Name, Arguments: json.RawMessage(`{"a":4,"b":5}`)})
			if err == nil && len(result.Content) == 1 && result.Content[0].Text == "9" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("stdio server did not reconnect: result=%#v err=%v", result, err)
		}
		time.Sleep(25 * time.Millisecond)
	}

	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	for _, schema := range engine.ListTools() {
		if strings.HasPrefix(schema.Name, "mcp__fixture__") {
			t.Fatalf("tool remained after close: %s", schema.Name)
		}
	}
}

func TestMCPCanonicalOutputSchemaFallsBackForUnsupportedStructuredSchema(t *testing.T) {
	schema := mcpCanonicalOutputSchema(map[string]any{"type": "object", "$ref": "#/$defs/result"})
	properties, _ := schema["properties"].(map[string]any)
	structured, _ := properties["structuredContent"].(map[string]any)
	required, _ := schema["required"].([]string)
	if len(structured) != 0 || len(required) != 1 || required[0] != "content" {
		t.Fatalf("fallback schema = %#v", schema)
	}
}

func TestMCPStreamableHTTPHeadersCallsAndListChanged(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "http-fixture", Version: "1.0.0"}, nil)
	server.AddTool(&mcp.Tool{Name: "shout", Description: "Uppercase text.", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`)}, func(_ context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var arguments struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(request.Params.Arguments, &arguments); err != nil {
			return nil, err
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: strings.ToUpper(arguments.Text)}}}, nil
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	var seen struct {
		sync.Mutex
		values []string
	}
	httpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		seen.Lock()
		seen.values = append(seen.values, request.Header.Get("Authorization"))
		seen.Unlock()
		handler.ServeHTTP(writer, request)
	}))
	t.Cleanup(httpServer.Close)

	engine := newMCPTestEngine(t)
	connection, err := engine.ConnectMCP(t.Context(), MCPConfig{
		Transport:          MCPTransportStreamableHTTP,
		ServerName:         "http",
		URL:                httpServer.URL,
		Headers:            map[string]string{"Authorization": "Bearer fixture-token"},
		ToolCallTimeout:    2 * time.Second,
		FailOnStartupError: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	result, err := callMCPTool(t, engine, "mcp__http__shout", `{"text":"hello"}`)
	if err != nil || result.Content[0].Text != "HELLO" {
		t.Fatalf("HTTP call = %#v, err=%v", result, err)
	}
	seen.Lock()
	headers := append([]string(nil), seen.values...)
	seen.Unlock()
	if len(headers) == 0 {
		t.Fatal("HTTP fixture saw no requests")
	}
	for _, header := range headers {
		if header != "Bearer fixture-token" {
			t.Fatalf("authorization header = %q", header)
		}
	}

	server.AddTool(&mcp.Tool{Name: "late", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "late tool"}}}, nil
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		engine.mu.RLock()
		_, found := engine.tools["mcp__http__late"]
		engine.mu.RUnlock()
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tools/list_changed did not resynchronize the registry")
		}
		time.Sleep(25 * time.Millisecond)
	}
	result, err = callMCPTool(t, engine, "mcp__http__late", `{}`)
	if err != nil || result.Content[0].Text != "late tool" {
		t.Fatalf("late tool = %#v, err=%v", result, err)
	}
}

func TestMCPRejectsInvalidAndDuplicateServerConfigs(t *testing.T) {
	engine := newMCPTestEngine(t)
	if _, err := engine.ConnectMCP(t.Context(), MCPConfig{Transport: MCPTransportStdio, ServerName: "bad name", Command: os.Args[0]}); err == nil {
		t.Fatal("invalid serverName was accepted")
	}
	connection, err := engine.ConnectMCP(t.Context(), MCPConfig{
		Transport:          MCPTransportStdio,
		ServerName:         "duplicate",
		Command:            os.Args[0],
		Args:               []string{"-test.run=^TestMCPStdioFixture$"},
		Env:                map[string]string{mcpFixtureEnv: "1"},
		FailOnStartupError: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := engine.ConnectMCP(t.Context(), MCPConfig{Transport: MCPTransportStdio, ServerName: "duplicate", Command: os.Args[0]}); err == nil {
		t.Fatal("duplicate serverName was accepted")
	}
}

func TestEngineStartsAndClosesConfiguredMCPServers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = t.TempDir()
	cfg.Persist = false
	cfg.SessionTitleLLM.Enabled = false
	cfg.MCPServers = []MCPConfig{{
		Transport:          MCPTransportStdio,
		ServerName:         "configured",
		Command:            os.Args[0],
		Args:               []string{"-test.run=^TestMCPStdioFixture$"},
		Env:                map[string]string{mcpFixtureEnv: "1"},
		FailOnStartupError: true,
	}}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	result, err := callMCPTool(t, engine, "mcp__configured__add", `{"a":6,"b":7}`)
	if err != nil || result.Content[0].Text != "13" {
		t.Fatalf("configured MCP call = %#v, err=%v", result, err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ConnectMCP(t.Context(), cfg.MCPServers[0]); err == nil {
		t.Fatal("closed engine accepted a new MCP connection")
	}
}
