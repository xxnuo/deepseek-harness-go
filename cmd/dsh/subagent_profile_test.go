package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	harness "github.com/xxnuo/deepseek-harness-go"
	"gopkg.in/yaml.v3"
)

func TestProfileSubagentProvidersReachEngine(t *testing.T) {
	composed := testComposition(t, `
- id: subagent-acp
  name: '@deepseek-ai/dsh-subagent-acp'
  config:
    providerName: acp-profile
    command: fake-acp
    args: [serve]
    permission: allow
    env: {ACP_FLAG: enabled}
    disposeEofGraceMs: 11
    disposeGraceMs: 12
- id: subagent-codex
  name: '@deepseek-ai/dsh-subagent-codex'
  config:
    providerName: codex-safe
    permissionMode: approve-for-me
    env: {CODEX_FLAG: enabled}
    disposeGraceMs: 13
- id: subagent-claude-code
  name: '@deepseek-ai/dsh-subagent-claude-code'
  config:
    providerName: claude-safe
    permissionMode: acceptEdits
    env: {CLAUDE_FLAG: enabled}
    disposeGraceMs: 14
- id: subagent-dsh-sdk
  name: '@deepseek-ai/dsh-subagent-dsh-sdk'
  config:
    providerName: sdk-profile
    command: fake-sdk
    args: [child]
    provider: echo
    model: echo
    maxTokens: 15
    env: {SDK_FLAG: enabled}
    shutdownTimeoutMs: 16
    disposeEofGraceMs: 17
    disposeGraceMs: 18
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	profileDir := t.TempDir()
	bin := filepath.Join(profileDir, "node_modules", ".bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"codex", "claude"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	composed.profileDir = profileDir
	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	cfg.DataDir = t.TempDir()
	cfg.Persist = false
	if len(cfg.SubagentProviders) != 4 {
		t.Fatalf("profile providers = %d, want 4", len(cfg.SubagentProviders))
	}
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	rows := engine.ListSubagentProviders()
	names := make([]string, len(rows))
	for index, row := range rows {
		names[index] = row.Name
	}
	// SubagentRuntime.list() preserves provider registration order, matching
	// the Loader effect order from the profile composition.
	want := []string{"acp-profile", "codex-safe", "claude-safe", "sdk-profile"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("registered providers = %q, want %q", names, want)
	}
}

func TestProfileProductExecutableRequiresProfileLauncher(t *testing.T) {
	composed := testComposition(t, `
- id: codex
  name: '@deepseek-ai/dsh-subagent-codex'
`)
	composed.profileDir = t.TempDir()
	_, err := composed.resolveSubagentProviders()
	if err == nil || !strings.Contains(err.Error(), "package-local codex launcher") {
		t.Fatalf("resolveSubagentProviders() = %v", err)
	}
}

func TestResolveSubagentProvidersIgnoresOtherPluginConfig(t *testing.T) {
	composed := testComposition(t, `
- id: unrelated
  name: '@deepseek-ai/dsh-session-persistence-jsonl'
  config:
    root: !!js missingProfileGlobal
`)
	providers, err := composed.resolveSubagentProviders()
	if err != nil || len(providers) != 0 {
		t.Fatalf("resolveSubagentProviders() = %d, %v", len(providers), err)
	}
}

func TestDisabledGroupJSExpressionSuppressesAllPluginResolvers(t *testing.T) {
	composed := testComposition(t, `
- id: disabled-group
  name: cordis:group
  group: true
  disabled: !!js process.platform === process.platform
  config:
    - id: provider
      name: '@deepseek-ai/dsh-subagent-acp'
      config:
        command: ''
    - id: tool
      name: '@deepseek-ai/dsh-tool-subagent'
      config: {}
    - id: mcp
      name: '@deepseek-ai/dsh-mcp-client'
      config:
        transport: invalid
    - id: lsp
      name: '@deepseek-ai/dsh-lsp-stdio'
      config:
        servers: {}
`)
	if err := composed.validate(); err != nil {
		t.Fatalf("disabled group should suppress child resolver validation: %v", err)
	}
	if len(composed.subagentProviders) != 0 || len(composed.subagentTools) != 0 || len(composed.mcpConfigs) != 0 || len(composed.lspServers) != 0 {
		t.Fatalf("disabled group leaked child configuration: providers=%d tools=%d mcp=%d lsp=%d", len(composed.subagentProviders), len(composed.subagentTools), len(composed.mcpConfigs), len(composed.lspServers))
	}
}

func TestSubagentToolResolverEvaluatesJSConfigValues(t *testing.T) {
	composed := testComposition(t, `
- id: tool-subagent
  name: '@deepseek-ai/dsh-tool-subagent'
  config:
    provider: !!js "'spawn'"
    toolName: !!js "'delegated'"
    backgroundMode: !!js "'continuable'"
    enableRunInBackground: !!js false
    maxDepth: !!js 2
    persona: !!js "'profile persona'"
`)
	if err := composed.validate(); err != nil {
		t.Fatalf("JS subagent tool config rejected: %v", err)
	}
	if len(composed.subagentTools) != 1 {
		t.Fatalf("resolved subagent tools = %#v", composed.subagentTools)
	}
	tool := composed.subagentTools[0]
	if tool.Provider != "spawn" || tool.ToolName != "delegated" || tool.BackgroundMode != "continuable" || tool.Persona != "profile persona" || tool.MaxDepth == nil || *tool.MaxDepth != 2 {
		t.Fatalf("resolved JS subagent tool = %#v", tool)
	}
	if tool.EnableRunInBackground == nil || *tool.EnableRunInBackground {
		t.Fatalf("resolved JS background flag = %#v", tool.EnableRunInBackground)
	}
}

func TestResolveSubagentProvidersRejectsInvalidExplicitValues(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		want   string
	}{
		{"zero duration", "disposeEofGraceMs: 0", "disposeEofGraceMs"},
		{"empty cwd", "cwd: ''", "cwd must not be empty"},
		{"zero max tokens", "maxTokens: 0", "maxTokens"},
	} {
		t.Run(test.name, func(t *testing.T) {
			plugin := "@deepseek-ai/dsh-subagent-acp"
			base := "    command: fake-acp\n"
			if test.name == "zero max tokens" {
				plugin = "@deepseek-ai/dsh-subagent-dsh-sdk"
				base = "    command: fake-sdk\n"
			}
			composed := testComposition(t, "- id: provider\n  name: '"+plugin+"'\n  config:\n"+base+"    "+test.config+"\n")
			_, err := composed.resolveSubagentProviders()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("resolveSubagentProviders() = %v, want %q", err, test.want)
			}
		})
	}
}

func testComposition(t *testing.T, source string) *composition {
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
