package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func mcpTestComposition(t *testing.T, source string) *composition {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(source), &document); err != nil {
		t.Fatal(err)
	}
	composed := &composition{entries: document.Content[0].Content, index: map[string]entryRef{}}
	for index, entry := range composed.entries {
		composed.buildIndex(entry, index)
	}
	return composed
}

func TestMCPProfileConfigMapsLiteralAndJSValues(t *testing.T) {
	t.Chdir(t.TempDir())
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("MCP_PROFILE_TOKEN", "profile-token")
	composed := mcpTestComposition(t, `
- id: memory-mcp
  name: '@deepseek-ai/dsh-mcp-client'
  config:
    transport: stdio
    serverName: memory
    command: memory-server
    args: [serve]
    cwd: !!js process.cwd()
    env:
      MEMORY_TOKEN: !!js process.env.MCP_PROFILE_TOKEN
      MEMORY_FILE_PATH: !!js >-
        process.env.MCP_PROFILE_MISSING?.trim() || process.getBuiltinModule('node:path').join(process.getBuiltinModule('node:os').homedir(), '.dsh-mcp-test.jsonl')
    toolCallTimeoutMs: 1500
    reconnect:
      enabled: false
      initialDelayMs: 20
      maxDelayMs: 80
      maxAttempts: 4
- id: remote-mcp
  name: '@deepseek-ai/dsh-mcp-client'
  config:
    transport: streamable-http
    serverName: remote
    url: http://127.0.0.1:4321/mcp
    headers:
      Authorization: !!js '`+"`Bearer ${process.env.MCP_PROFILE_TOKEN}`"+`'
    failOnStartupError: true
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	if len(composed.mcpConfigs) != 2 {
		t.Fatalf("MCP configs = %#v", composed.mcpConfigs)
	}
	stdio := composed.mcpConfigs[0]
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	if stdio.ServerName != "memory" || stdio.CWD != cwd || stdio.Env["MEMORY_TOKEN"] != "profile-token" || stdio.Env["MEMORY_FILE_PATH"] != filepath.Join(home, ".dsh-mcp-test.jsonl") || stdio.ToolCallTimeout != 1500*time.Millisecond {
		t.Fatalf("stdio config = %#v", stdio)
	}
	if stdio.Reconnect.Enabled == nil || *stdio.Reconnect.Enabled || stdio.Reconnect.InitialDelay != 20*time.Millisecond || stdio.Reconnect.MaxAttempts != 4 {
		t.Fatalf("reconnect config = %#v", stdio.Reconnect)
	}
	http := composed.mcpConfigs[1]
	if http.URL != "http://127.0.0.1:4321/mcp" || http.Headers["Authorization"] != "Bearer profile-token" || !http.FailOnStartupError {
		t.Fatalf("HTTP config = %#v", http)
	}
}

func TestMCPProfileConfigRejectsNonStringEnvironment(t *testing.T) {
	composed := mcpTestComposition(t, `
- id: bad-mcp
  name: '@deepseek-ai/dsh-mcp-client'
  config:
    transport: stdio
    serverName: bad
    command: server
    env:
      BAD: 1
`)
	if err := composed.validate(); err == nil {
		t.Fatal("non-string MCP environment value was accepted")
	}
}
