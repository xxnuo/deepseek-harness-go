package main

import (
	"reflect"
	"strings"
	"testing"
	"time"

	harness "github.com/xxnuo/deepseek-harness-go"
)

func TestTerminalProfileConfigMapsAndAppliesAllFields(t *testing.T) {
	t.Setenv("DSH_TEST_SHELL", "/bin/bash")
	composed := mcpTestComposition(t, `
- id: terminal-group
  name: cordis:group
  group: true
  config:
    - id: custom-terminal
      name: '@deepseek-ai/dsh-terminal-bash'
      config:
        backendType: profile-shell
        shellPath: !!js process.env.DSH_TEST_SHELL
        shellArgs: [--noprofile, --norc, -i]
        rows: 21
        cols: 81
        scrollbackLines: 123
        scrollbackMaxBytes: 8192
        maxReadBytes: 4096
        pollIntervalMs: 11
        exactProbeAfterMs: 22
        idleSilenceMs: 33
        handoffGraceMs: 44
        timeoutMs: 55
        disposeGraceMs: 66
    - id: custom-terminal-tools
      name: '@deepseek-ai/dsh-tool-terminal'
      config:
        enableRunInBackground: false
        maxResultBytes: 128
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	cfg.Workspace, cfg.Persist = t.TempDir(), false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	got := engine.Config()
	terminal := got.Terminal
	if terminal.BackendType != "profile-shell" || terminal.ShellPath != "/bin/bash" || !reflect.DeepEqual(terminal.ShellArgs, []string{"--noprofile", "--norc", "-i"}) {
		t.Fatalf("terminal executable config = %#v", terminal)
	}
	if terminal.Rows != 21 || terminal.Cols != 81 || terminal.ScrollbackLines != 123 || terminal.ScrollbackMaxBytes != 8192 || terminal.MaxReadBytes != 4096 {
		t.Fatalf("terminal size config = %#v", terminal)
	}
	if terminal.PollInterval != 11*time.Millisecond || terminal.ExactProbeAfter != 22*time.Millisecond || terminal.IdleSilence != 33*time.Millisecond || terminal.HandoffGrace != 44*time.Millisecond || terminal.Timeout != 55*time.Millisecond || terminal.DisposeGrace != 66*time.Millisecond {
		t.Fatalf("terminal timing config = %#v", terminal)
	}
	if got.TerminalTool.EnableRunInBackground == nil || *got.TerminalTool.EnableRunInBackground || got.TerminalTool.MaxResultBytes != 128 {
		t.Fatalf("terminal tool config = %#v", got.TerminalTool)
	}
	for _, schema := range engine.ListTools() {
		if schema.Name != "terminal_send" {
			continue
		}
		properties, _ := schema.Parameters["properties"].(map[string]any)
		if _, ok := properties["run_in_background"]; ok {
			t.Fatalf("terminal_send exposed disabled run_in_background: %#v", schema.Parameters)
		}
		return
	}
	t.Fatal("terminal_send was not registered")
}

func TestTerminalProfileConfigAppliesExplicitDisable(t *testing.T) {
	composed := mcpTestComposition(t, `
- id: terminal-bash
  name: '@deepseek-ai/dsh-terminal-bash'
  disabled: true
- id: tool-terminal
  name: '@deepseek-ai/dsh-tool-terminal'
  disabled: !!js true
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	if !cfg.Terminal.Disabled || !cfg.TerminalTool.Disabled {
		t.Fatalf("disabled terminal config = %#v %#v", cfg.Terminal, cfg.TerminalTool)
	}
	cfg.Workspace, cfg.Persist = t.TempDir(), false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	for _, schema := range engine.ListTools() {
		if strings.HasPrefix(schema.Name, "terminal_") {
			t.Fatalf("disabled terminal tool was registered: %s", schema.Name)
		}
	}
}

func TestTerminalProfileConfigRejectsInvalidOrDuplicateEntries(t *testing.T) {
	for name, source := range map[string]string{
		"invalid backend config": `
- id: terminal-bash
  name: '@deepseek-ai/dsh-terminal-bash'
  config: {rows: many}
`,
		"invalid tool config": `
- id: tool-terminal
  name: '@deepseek-ai/dsh-tool-terminal'
  config: {enableRunInBackground: sometimes}
`,
		"duplicate backend": `
- id: terminal-bash-one
  name: '@deepseek-ai/dsh-terminal-bash'
- id: terminal-bash-two
  name: '@deepseek-ai/dsh-terminal-bash'
`,
		"duplicate tools": `
- id: tool-terminal-one
  name: '@deepseek-ai/dsh-tool-terminal'
- id: tool-terminal-two
  name: '@deepseek-ai/dsh-tool-terminal'
`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := mcpTestComposition(t, source).validate(); err == nil {
				t.Fatal("invalid terminal profile config was accepted")
			}
		})
	}
}
