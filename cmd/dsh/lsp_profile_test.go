package main

import (
	"reflect"
	"testing"
)

func TestLSPProfileConfigMapsServersAndToolLimits(t *testing.T) {
	composed := mcpTestComposition(t, `
- id: lsp
  name: '@deepseek-ai/dsh-lsp'
- id: lsp-stdio
  name: '@deepseek-ai/dsh-lsp-stdio'
  config:
    servers:
      typescript:
        command: node
        args: [server.mjs, --stdio]
        env: {NODE_OPTIONS: --no-warnings}
        extensionToLanguage: {.ts: typescript, .tsx: typescriptreact}
        initializationOptions: {preferences: {includeInlayParameterNameHints: all}}
        configuration: {typescript: {format: {semicolons: remove}}}
        maxMessageBytes: 1234
        maxStderrBytes: 2345
        maxDocumentBytes: 3456
        shutdownTimeoutMs: 4567
        killGraceMs: 5678
- id: tool-lsp
  name: '@deepseek-ai/dsh-tool-lsp'
  config: {maxLocations: 7, maxResultChars: 800, timeoutMs: 900}
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(&profileLoader{}, composed)
	server, ok := cfg.LSPServers["typescript"]
	if !ok {
		t.Fatalf("LSP servers = %#v", cfg.LSPServers)
	}
	if server.Command != "node" || !reflect.DeepEqual(server.Args, []string{"server.mjs", "--stdio"}) || server.Env["NODE_OPTIONS"] != "--no-warnings" || server.ExtensionToLanguage[".tsx"] != "typescriptreact" {
		t.Fatalf("LSP server = %#v", server)
	}
	if server.MaxMessageBytes != 1234 || server.MaxStderrBytes != 2345 || server.MaxDocumentBytes != 3456 || server.ShutdownTimeoutMillis != 4567 || server.KillGraceMillis != 5678 {
		t.Fatalf("LSP server limits = %#v", server)
	}
	if !cfg.LSPTool.Enabled || cfg.LSPTool.MaxLocations != 7 || cfg.LSPTool.MaxResultChars != 800 || cfg.LSPTool.TimeoutMillis != 900 {
		t.Fatalf("LSP tool = %#v", cfg.LSPTool)
	}
	if cfg.LSPTool.Timeout != 0 {
		t.Fatalf("profile should preserve timeoutMs for engine normalization: %#v", cfg.LSPTool)
	}
}

func TestLSPProfileConfigRejectsInvalidServers(t *testing.T) {
	for _, source := range []string{
		`- id: lsp-stdio
  name: '@deepseek-ai/dsh-lsp-stdio'
  config: {servers: {}}`,
		`- id: lsp-stdio
  name: '@deepseek-ai/dsh-lsp-stdio'
  config:
    servers:
      bad:
        command: node
        env: {BAD: 1}
        extensionToLanguage: {.go: go}`,
	} {
		if err := mcpTestComposition(t, source).validate(); err == nil {
			t.Fatalf("invalid LSP config was accepted:\n%s", source)
		}
	}
}
