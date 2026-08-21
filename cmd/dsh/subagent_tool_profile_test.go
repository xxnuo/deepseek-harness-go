package main

import (
	"testing"

	harness "github.com/xxnuo/deepseek-harness-go"
)

func TestProductSubagentToolProfileConfig(t *testing.T) {
	composed := mcpTestComposition(t, `
- id: subagent-codex
  name: '@deepseek-ai/dsh-subagent-codex'
- id: subagent-claude-code
  name: '@deepseek-ai/dsh-subagent-claude-code'
- id: delegation
  name: cordis:group
  group: true
  config:
    - id: tool-subagent-codex
      name: '@deepseek-ai/dsh-tool-subagent'
      config:
        provider: codex
        toolName: subagent_codex
        backgroundMode: one-shot
        maxDepth: provider-managed
        agentOptions: {provider: child-provider, model: child-model, maxTokens: 321}
    - id: tool-subagent-claude-code
      name: '@deepseek-ai/dsh-tool-subagent'
      config:
        provider: claude-code
        toolName: subagent_claude_code
        backgroundMode: one-shot
        maxDepth: provider-managed
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	if len(cfg.SubagentProviders) != 2 || len(cfg.SubagentTools) != 2 {
		t.Fatalf("providers=%d tools=%d", len(cfg.SubagentProviders), len(cfg.SubagentTools))
	}
	if cfg.SubagentTools[0].Provider != "codex" || cfg.SubagentTools[0].ToolName != "subagent_codex" || cfg.SubagentTools[0].MaxDepth != nil {
		t.Fatalf("codex tool = %#v", cfg.SubagentTools[0])
	}
	if options := cfg.SubagentTools[0].AgentOptions; options == nil || options.Provider != "child-provider" || options.Model != "child-model" || options.MaxTokens != 321 {
		t.Fatalf("codex agent options = %#v", options)
	}
	if cfg.SubagentTools[1].Provider != "claude-code" || cfg.SubagentTools[1].ToolName != "subagent_claude_code" || cfg.SubagentTools[1].MaxDepth != nil {
		t.Fatalf("claude tool = %#v", cfg.SubagentTools[1])
	}
	cfg.Workspace, cfg.Persist = t.TempDir(), false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	registered := map[string]bool{}
	for _, schema := range engine.ListTools() {
		registered[schema.Name] = true
	}
	if !registered["subagent_codex"] || !registered["subagent_claude_code"] {
		t.Fatalf("registered tools = %#v", registered)
	}
}

func TestSubagentToolProfileMapsPersonaAndToolFilter(t *testing.T) {
	composed := mcpTestComposition(t, `
- id: tool
  name: '@deepseek-ai/dsh-tool-subagent'
  config:
    provider: custom
    backgroundMode: one-shot
    maxDepth: provider-managed
    persona: focused child
    toolFilter: {allow: [read, grep], deny: [bash]}
`)
	tools, err := composed.resolveSubagentTools()
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 1 || tools[0].Persona != "focused child" || tools[0].ToolFilter == nil || len(tools[0].ToolFilter.Allow) != 2 || len(tools[0].ToolFilter.Deny) != 1 {
		t.Fatalf("subagent tool config = %#v", tools)
	}
}

func TestInProcessSubagentToolProfileMapsRuntimePolicy(t *testing.T) {
	composed := mcpTestComposition(t, `
- id: spawn-tool
  name: '@deepseek-ai/dsh-tool-subagent'
  config:
    provider: spawn
    toolName: subagent
    backgroundMode: continuable
    agentOptions: {provider: echo, model: child-model, maxTokens: 321}
    persona: focused child
    toolFilter: {allow: []}
- id: fork-tool
  name: '@deepseek-ai/dsh-tool-subagent'
  config:
    provider: fork
    toolName: subagent_fork
    backgroundMode: continuable
`)
	tools, err := composed.resolveSubagentTools()
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 2 {
		t.Fatalf("subagent tools = %#v", tools)
	}
	spawn, fork := tools[0], tools[1]
	if spawn.Provider != "spawn" || spawn.BackgroundMode != "continuable" || spawn.MaxDepth == nil || *spawn.MaxDepth != 3 {
		t.Fatalf("spawn tool = %#v", spawn)
	}
	if spawn.AgentOptions == nil || spawn.AgentOptions.Provider != "echo" || spawn.AgentOptions.Model != "child-model" || spawn.AgentOptions.MaxTokens != 321 {
		t.Fatalf("spawn agent options = %#v", spawn.AgentOptions)
	}
	if spawn.Persona != "focused child" || spawn.ToolFilter == nil || spawn.ToolFilter.Allow == nil || len(spawn.ToolFilter.Allow) != 0 {
		t.Fatalf("spawn child composition = %#v", spawn)
	}
	if fork.Provider != "fork" || fork.BackgroundMode != "continuable" || fork.MaxDepth == nil || *fork.MaxDepth != 3 {
		t.Fatalf("fork tool = %#v", fork)
	}

	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	cfg.Workspace, cfg.Persist = t.TempDir(), false
	cfg.Provider, cfg.Model = "echo", "echo"
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	registered := map[string]bool{}
	for _, schema := range engine.ListTools() {
		registered[schema.Name] = true
	}
	if !registered["subagent"] || !registered["subagent_fork"] {
		t.Fatalf("registered tools = %#v", registered)
	}
}

func TestProductSubagentToolProfileRejectsUnsupportedMode(t *testing.T) {
	composed := mcpTestComposition(t, `
- id: tool-subagent-codex
  name: '@deepseek-ai/dsh-tool-subagent'
  config:
    provider: codex
    toolName: subagent_codex
    backgroundMode: continuable
    maxDepth: provider-managed
`)
	if err := composed.validate(); err == nil {
		t.Fatal("continuable external provider tool was accepted")
	}
}

func TestSubagentReportProfileDelivery(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		want   string
	}{
		{name: "default", want: harness.SubagentReportNextStep},
		{name: "quiet", config: "config: {reportDelivery: quiet}", want: harness.SubagentReportQuiet},
	} {
		t.Run(test.name, func(t *testing.T) {
			composed := mcpTestComposition(t, "\n- id: report\n  name: '@deepseek-ai/dsh-tool-subagent-report'\n  "+test.config+"\n")
			if err := composed.validate(); err != nil {
				t.Fatal(err)
			}
			if composed.subagentReportDelivery != test.want {
				t.Fatalf("resolved report delivery = %q, want %q", composed.subagentReportDelivery, test.want)
			}
			if got := engineConfig(&profileLoader{home: t.TempDir()}, composed).SubagentReportDelivery; got != test.want {
				t.Fatalf("engine report delivery = %q, want %q", got, test.want)
			}
		})
	}
	invalid := mcpTestComposition(t, `
- id: report
  name: '@deepseek-ai/dsh-tool-subagent-report'
  config: {reportDelivery: wakeup}
`)
	if err := invalid.validate(); err == nil {
		t.Fatal("legacy reportDelivery=wakeup was accepted")
	}
}
