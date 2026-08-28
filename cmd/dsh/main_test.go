package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	harness "github.com/xxnuo/deepseek-harness-go"
	"gopkg.in/yaml.v3"
)

func TestParseArgsMatchesUpstreamLauncherBoundary(t *testing.T) {
	tests := []struct {
		args []string
		want invocation
	}{
		{[]string{"--profile", "tui"}, invocation{mode: "profile", profile: "tui"}},
		{[]string{"--profile", "tui", "--patch", "a.yml", "--patch=b.yml"}, invocation{mode: "profile", profile: "tui", patches: []string{"a.yml", "b.yml"}}},
		{[]string{"web", "--patch", "web.yml"}, invocation{mode: "profile", profile: "web", patches: []string{"web.yml"}}},
		{[]string{"--", "web"}, invocation{mode: "profile", profile: "web"}},
		{[]string{"web", "--", "--trusted-host", "lab.internal"}, invocation{mode: "profile", profile: "web", args: []string{"--trusted-host", "lab.internal"}}},
		{[]string{"--profile", "tui", "-V"}, invocation{mode: "version"}},
		{[]string{"--profile", "headless", "--", "--", "-literal"}, invocation{mode: "profile", profile: "headless", args: []string{"--", "-literal"}}},
		{[]string{"--profile", "tui", "--patch", "a.yml", "--resume", "b", "--patch", "late.yml"}, invocation{mode: "profile", profile: "tui", patches: []string{"a.yml"}, args: []string{"--resume", "b", "--patch", "late.yml"}}},
		{[]string{"--profile", "web", "--dump-config", "--patch", "x.yml"}, invocation{mode: "dump", profile: "web", patches: []string{"x.yml"}}},
		{[]string{"web", "--dump-default-config"}, invocation{mode: "dump", profile: "web", defaultOnly: true}},
		{[]string{"plugin", "--profile", "tui", "add", "--save-dev", "x"}, invocation{mode: "plugin", profile: "tui", args: []string{"add", "--save-dev", "x"}}},
		{[]string{"plugin", "add", "--save-dev", "x", "--profile", "tui"}, invocation{mode: "plugin", profile: "tui", args: []string{"add", "--save-dev", "x"}}},
	}
	for _, test := range tests {
		got, err := parseArgs(test.args)
		if err != nil {
			t.Fatalf("parseArgs(%q): %v", test.args, err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("parseArgs(%q) = %#v, want %#v", test.args, got, test.want)
		}
	}
}

func TestUnknownCommandHelpUsesLauncherHelp(t *testing.T) {
	inv, err := parseArgs([]string{"foo", "-h"})
	if err != nil || inv.mode != "help" {
		t.Fatalf("foo -h = %#v, %v", inv, err)
	}
}

func TestRunHeadlessUsesLiteralTaskAndTurnOutcome(t *testing.T) {
	cfg := harness.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = t.TempDir()
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.Persist = false
	var stdout, stderr bytes.Buffer
	if err := runHeadless([]string{"--", "/clear"}, nil, cfg, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "/clear\n" || stderr.Len() != 0 {
		t.Fatalf("headless literal output = %q, stderr = %q", stdout.String(), stderr.String())
	}
	stdout.Reset()
	if err := runHeadless([]string{"--", "--help"}, nil, cfg, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "--help\n" {
		t.Fatalf("headless literal help output = %q", stdout.String())
	}
	stdout.Reset()
	if err := runHeadless([]string{"  spaced task  "}, nil, cfg, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "  spaced task  \n" {
		t.Fatalf("headless trimmed task = %q", stdout.String())
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	cfg.DataDir = t.TempDir()
	cfg.Provider = "test-provider"
	cfg.Model = "test-model"
	cfg.APIKey = "test-key"
	cfg.BaseURL = server.URL
	stdout.Reset()
	stderr.Reset()
	err := runHeadless([]string{"task"}, nil, cfg, &stdout, &stderr)
	var exit exitError
	if !errors.As(err, &exit) || exit.code != 1 {
		t.Fatalf("max-token headless error = %#v", err)
	}
	if stdout.String() != "partial\n" || stderr.Len() != 0 {
		t.Fatalf("max-token output = %q, stderr = %q", stdout.String(), stderr.String())
	}
}

func TestSignalExitCode(t *testing.T) {
	if signalExitCode(os.Interrupt) != 130 || signalExitCode(os.Kill) != 0 {
		t.Fatal("signal exit-code mapping changed")
	}
}

func TestValidateWebCLIHostRejectsWildcardOnlyWhenExplicit(t *testing.T) {
	options, err := parseWebOptions([]string{"--host", "0.0.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := options.apply(harness.DefaultConfig()); err == nil || !strings.Contains(err.Error(), "intentionally not supported yet for safety") {
		t.Fatalf("--host 0.0.0.0 error = %v", err)
	}

	cfg := harness.DefaultConfig()
	cfg.Host = "0.0.0.0"
	cfg, err = (webOptions{}).apply(cfg)
	if err != nil || cfg.Host != "0.0.0.0" {
		t.Fatalf("composed host 0.0.0.0 = %#v, %v", cfg, err)
	}
}

func TestParseWebOptionsOpensByDefaultAndSupportsNoOpen(t *testing.T) {
	options, err := parseWebOptions(nil)
	if err != nil || !options.openBrowser {
		t.Fatalf("default browser option = %#v, %v", options, err)
	}
	options, err = parseWebOptions([]string{"--no-open", "--port", "0"})
	if err != nil || options.openBrowser || !options.portSet || options.port != 0 {
		t.Fatalf("--no-open browser option = %#v, %v", options, err)
	}
}

func TestParseArgsRejectsContradictions(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"headless", "task"},
		{"--profile", ""},
		{"--profile", "x", "--patch="},
		{"--profile", "x", "--dump-config", "--dump-default-config"},
		{"--profile", "x", "--dump-default-config", "--patch", "p.yml"},
		{"--profile", "x", "--dump-config", "task"},
		{"--profile", "x", "web"},
		{"plugin", "add", "x"},
		{"plugin", "--profile", "x"},
	} {
		if _, err := parseArgs(args); err == nil {
			t.Fatalf("parseArgs(%q) unexpectedly succeeded", args)
		}
	}
}

func TestProfileDumpInitializesAndComposesRc7Bundles(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DSH_HOME", home)
	loader, err := newProfileLoader(true)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := loader.dump("web", true, nil, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected warnings: %s", stderr.String())
	}
	dump := stdout.String()
	for _, expected := range []string{
		"# == " + baseBundle,
		"name: '@deepseek-ai/dsh-agent-loop'",
		"name: '@deepseek-ai/dsh-host-webserver'",
		"!!js ctx.webStartup.host ?? '127.0.0.1'",
	} {
		if !strings.Contains(dump, expected) {
			t.Fatalf("dump missing %q", expected)
		}
	}
	var parsed yaml.Node
	if err := yaml.Unmarshal([]byte(dump), &parsed); err != nil {
		t.Fatalf("dump is not loadable YAML: %v", err)
	}
	profileDir := filepath.Join(home, profilesDir, "web")
	for _, name := range []string{"package.json", profilePatchFile, "pnpm-workspace.yaml", profileRootFile} {
		if _, err := os.Stat(filepath.Join(profileDir, name)); err != nil {
			t.Fatalf("profile did not initialize %s: %v", name, err)
		}
	}
	document, _, err := readManifest(profileDir)
	if err != nil {
		t.Fatal(err)
	}
	bundles, err := profileBundles(document)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bundles, profileTemplates["web"]) {
		t.Fatalf("web bundles = %q", bundles)
	}
	composed, err := loader.compose("web", nil, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(loader, composed)
	if cfg.Host != "127.0.0.1" || cfg.Port != 3080 {
		t.Fatalf("expression fallbacks changed Go defaults: host=%q port=%d", cfg.Host, cfg.Port)
	}
	if cfg.SessionProjectionCache == nil || cfg.SessionProjectionCache.WriteEveryEvents != 200 || cfg.SessionProjectionCache.WriteInterval != 5*time.Second {
		t.Fatalf("web projection cache config = %#v", cfg.SessionProjectionCache)
	}
}

func TestEngineConfigUsesActiveClientRoster(t *testing.T) {
	home := t.TempDir()
	loader := &profileLoader{home: home, upstream: filepath.Join(home, "upstream")}
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`
- id: active
  name: '@example/active'
- id: disabled
  name: '@example/disabled'
  disabled: true
`), &document); err != nil {
		t.Fatal(err)
	}
	profileDir := filepath.Join(home, "profiles", "web")
	composed := &composition{entries: document.Content[0].Content, profileDir: profileDir, index: map[string]entryRef{}}
	cfg := engineConfig(loader, composed)
	if !reflect.DeepEqual(cfg.ClientPlugins, []string{"@example/active"}) {
		t.Fatalf("client plugins = %#v", cfg.ClientPlugins)
	}
	if !reflect.DeepEqual(cfg.PluginDirs, []string{filepath.Join(profileDir, "node_modules")}) {
		t.Fatalf("plugin dirs = %#v", cfg.PluginDirs)
	}
	if len(cfg.PluginInventory) != 2 || cfg.PluginInventory[0].EntryID != "active" || !cfg.PluginInventory[0].Enabled || cfg.PluginInventory[0].FiberPhase == nil || *cfg.PluginInventory[0].FiberPhase != "active" {
		t.Fatalf("active inventory = %#v", cfg.PluginInventory)
	}
	if cfg.PluginInventory[1].EntryID != "disabled" || cfg.PluginInventory[1].Enabled || cfg.PluginInventory[1].FiberPhase != nil {
		t.Fatalf("disabled inventory = %#v", cfg.PluginInventory)
	}
}

func TestCompositionPluginInventoryUsesLoaderOrderAndEffectiveDisablement(t *testing.T) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`
- id: first
  name: '@example/first'
- id: disabled-group
  name: 'cordis:group'
  group: true
  disabled: !!js true
  config:
    - id: inherited-disabled
      name: '@example/inherited-disabled'
- id: last
  name: '@example/last'
`), &document); err != nil {
		t.Fatal(err)
	}
	composed := &composition{entries: document.Content[0].Content}
	entries := composed.pluginInventoryEntries()
	if len(entries) != 3 {
		t.Fatalf("inventory = %#v", entries)
	}
	wantIDs := []string{"first", "disabled-group:inherited-disabled", "last"}
	for i, id := range wantIDs {
		if entries[i].EntryID != id {
			t.Fatalf("inventory order = %#v", entries)
		}
	}
	if entries[1].Enabled || entries[1].FiberPhase != nil {
		t.Fatalf("inherited disabled inventory = %#v", entries[1])
	}
}

func TestCompositionDisabledJSExpressionControlsActiveRoster(t *testing.T) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`
- id: off
  name: '@example/off'
  disabled: !!js true
- id: on
  name: '@example/on'
  disabled: !!js false
`), &document); err != nil {
		t.Fatal(err)
	}
	composed := &composition{entries: document.Content[0].Content}
	active := composed.activePluginEntries()
	if len(active) != 1 || active[0].id != "on" {
		t.Fatalf("active entries = %#v", active)
	}
}

func TestCompositionValidationRejectsDuplicateQualifiedLoaderIDs(t *testing.T) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`
- id: duplicate
  name: '@example/one'
- id: duplicate
  name: '@example/two'
`), &document); err != nil {
		t.Fatal(err)
	}
	composed := &composition{entries: document.Content[0].Content}
	if err := composed.validateEntryIDs(); err == nil || !strings.Contains(err.Error(), "duplicate loader entry id: duplicate") {
		t.Fatalf("duplicate entry validation = %v", err)
	}
}

func TestCompositionValidationAllowsSameIDInSeparateGroups(t *testing.T) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`
- id: first
  name: cordis:group
  group: true
  config:
    - id: child
      name: '@example/one'
- id: second
  name: cordis:group
  group: true
  config:
    - id: child
      name: '@example/two'
`), &document); err != nil {
		t.Fatal(err)
	}
	composed := &composition{entries: document.Content[0].Content}
	if err := composed.validateEntryIDs(); err != nil {
		t.Fatalf("same local IDs in separate groups rejected: %v", err)
	}
}

func TestEngineHonorsDisabledHostToolPlugin(t *testing.T) {
	active := "active"
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	cfg.PluginInventory = []harness.PluginInventoryEntry{
		{EntryID: "tool-fs", ModuleName: "@deepseek-ai/dsh-tool-fs", Enabled: false},
		{EntryID: "tool-fs-search", ModuleName: "@deepseek-ai/dsh-tool-fs-search", Enabled: true, FiberPhase: &active},
	}
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	names := make(map[string]bool)
	for _, schema := range engine.ListTools() {
		names[schema.Name] = true
	}
	if names["read"] || names["write"] || names["edit"] || names["read_image"] {
		t.Fatalf("disabled tool-fs still registered: %#v", names)
	}
	if !names["glob"] || !names["grep"] {
		t.Fatalf("enabled tool-fs-search tools missing: %#v", names)
	}
}

func TestApplyRuntimeConfigReconcilesDisabledHostTools(t *testing.T) {
	active := "active"
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	cfg.PluginInventory = []harness.PluginInventoryEntry{
		{EntryID: "tool-fs", ModuleName: "@deepseek-ai/dsh-tool-fs", Enabled: true, FiberPhase: &active},
		{EntryID: "tool-fs-search", ModuleName: "@deepseek-ai/dsh-tool-fs-search", Enabled: true, FiberPhase: &active},
	}
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	if err := engine.ApplyRuntimeConfig(func() harness.Config {
		next := cfg
		next.PluginInventory = []harness.PluginInventoryEntry{
			{EntryID: "tool-fs", ModuleName: "@deepseek-ai/dsh-tool-fs", Enabled: false},
			{EntryID: "tool-fs-search", ModuleName: "@deepseek-ai/dsh-tool-fs-search", Enabled: true, FiberPhase: &active},
		}
		return next
	}()); err != nil {
		t.Fatal(err)
	}
	for _, schema := range engine.ListTools() {
		if schema.Name == "read" || schema.Name == "write" || schema.Name == "edit" || schema.Name == "read_image" {
			t.Fatalf("disabled fs tool survived runtime update: %s", schema.Name)
		}
	}
	if err := engine.ApplyRuntimeConfig(cfg); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, schema := range engine.ListTools() {
		names[schema.Name] = true
	}
	if !names["read"] || !names["write"] || !names["edit"] || !names["read_image"] {
		t.Fatalf("re-enabled fs tools missing: %#v", names)
	}
}

func TestCompositionRejectsUnknownEnabledPlugin(t *testing.T) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`
- id: unknown
  name: '@example/unimplemented'
`), &document); err != nil {
		t.Fatal(err)
	}
	composed := &composition{entries: document.Content[0].Content}
	if err := composed.validateSupportedPlugins(); err == nil || !strings.Contains(err.Error(), "@example/unimplemented") {
		t.Fatalf("validateSupportedPlugins() = %v", err)
	}
}

func TestCompositionRejectsInvalidRuntimeConfig(t *testing.T) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`
- id: tools
  name: '@deepseek-ai/dsh-tools'
  config:
    mode: invalid
- id: llm-pi-ai
  name: '@deepseek-ai/dsh-llm-pi-ai'
  config:
    providers:
      - provider: openai
`), &document); err != nil {
		t.Fatal(err)
	}
	composed := &composition{entries: document.Content[0].Content, index: map[string]entryRef{}}
	for index, entry := range composed.entries {
		composed.buildIndex(entry, index)
	}
	if err := composed.validate(); err == nil || !strings.Contains(err.Error(), "tools") {
		t.Fatalf("invalid tools mode = %v", err)
	}
	setMappingValue(mappingValue(composed.index["tools"].node, "config"), "mode", &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "native"})
	if err := composed.validate(); err == nil || !strings.Contains(err.Error(), "llm-pi-ai") {
		t.Fatalf("invalid llm-pi-ai config = %v", err)
	}
}

func TestCompositionConfigHelpersEvaluateIndependentJSValues(t *testing.T) {
	t.Setenv("DSH_TEST_TOOLS_MODE", "code")
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`
- id: tools
  name: '@deepseek-ai/dsh-tools'
  config:
    mode: !!js process.env.DSH_TEST_TOOLS_MODE
- id: limits
  name: '@example/limits'
  config:
    values: [!!js 2 + 1, 4]
`), &document); err != nil {
		t.Fatal(err)
	}
	composed := &composition{entries: document.Content[0].Content, index: map[string]entryRef{}}
	for index, entry := range composed.entries {
		composed.buildIndex(entry, index)
	}
	if mode, ok := composed.configString("tools", "mode"); !ok || mode != "code" {
		t.Fatalf("JS config string = %q, %v", mode, ok)
	}
	if values, ok := composed.configInts("limits", "values"); !ok || !reflect.DeepEqual(values, []int{3, 4}) {
		t.Fatalf("JS config ints = %#v, %v", values, ok)
	}
}

func TestEngineConfigAppliesSupportedProfileConfiguration(t *testing.T) {
	home := t.TempDir()
	loader := &profileLoader{home: home}
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`
- id: session-persistence-jsonl
  name: '@deepseek-ai/dsh-session-persistence-jsonl'
- id: session-title
  name: '@deepseek-ai/dsh-session-title'
  config: {fallbackMaxWords: 7, fallbackMaxBytes: 48, maxTitleBytes: 96}
- id: session-title-llm
  name: '@deepseek-ai/dsh-session-title-first-prompt-llm'
  config: {targetWords: 8, targetCjkCharacters: 12, maxInputBytes: 2048, maxOutputTokens: 32, timeoutMs: 1234}
- id: compaction-basic
  name: '@deepseek-ai/dsh-compaction-basic'
- id: spill-policy
  name: '@deepseek-ai/dsh-spill-policy'
  config: {maxInlineBytes: 321}
- id: tool-result-pruner
  name: '@deepseek-ai/dsh-compaction-tool-result-pruner'
  config: {thresholdChars: 200, headChars: 80, tailChars: 40}
- id: repeat-tool-reminder
  name: '@deepseek-ai/dsh-repeat-tool-reminder'
  config: {thresholds: [2, 4], include: [bash], exclude: [read], argumentsPreviewChars: 99}
- id: tools
  name: '@deepseek-ai/dsh-tools'
  config: {mode: both}
- id: system-prompt
  name: '@deepseek-ai/dsh-system-prompt'
  config: {persona: custom persona}
- id: agent-instructions
  name: '@deepseek-ai/dsh-agent-instructions'
  config: {maxBytes: 12345}
- id: web
  name: '@deepseek-ai/dsh-web'
  config: {searchProvider: exa, fetchProvider: http}
`), &document); err != nil {
		t.Fatal(err)
	}
	composed := &composition{entries: document.Content[0].Content, index: map[string]entryRef{}}
	for index, entry := range composed.entries {
		composed.buildIndex(entry, index)
	}
	cfg := engineConfig(loader, composed)
	if !cfg.Persist || !cfg.SessionTitleLLM.Enabled || cfg.Compaction.Disabled || cfg.Spill.Disabled || cfg.ToolResultPruner.Disabled || cfg.RepeatToolReminder.Disabled {
		t.Fatalf("enabled profile services were not applied: %#v", cfg)
	}
	if cfg.SessionTitle.FallbackMaxWords != 7 || cfg.SessionTitleLLM.Timeout != 1234*time.Millisecond || cfg.Spill.MaxInlineBytes != 321 || cfg.ToolResultPruner.HeadChars != 80 {
		t.Fatalf("profile limits were not applied: %#v", cfg)
	}
	if !reflect.DeepEqual(cfg.RepeatToolReminder.Thresholds, []int{2, 4}) || cfg.ToolPresentation != "both" || cfg.Persona != "custom persona" || cfg.InstructionMaxBytes != 12345 || cfg.WebSearchProvider != "exa" {
		t.Fatalf("profile values were not applied: %#v", cfg)
	}
}

func TestShippedProfilesOnlyEnableImplementedPlugins(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DSH_HOME", home)
	loader, err := newProfileLoader(true)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"web", "headless"} {
		composed, err := loader.compose(name, nil, &bytes.Buffer{})
		if err != nil {
			t.Fatal(err)
		}
		if err := composed.validateSupportedPlugins(); err != nil {
			t.Fatalf("%s profile: %v", name, err)
		}
	}
}

func TestProfileUserAndOverlayPrecedence(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DSH_HOME", home)
	loader, err := newProfileLoader(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loader.loadProfile("web", false); err != nil {
		t.Fatal(err)
	}
	profilePatch := filepath.Join(home, profilesDir, "web", profilePatchFile)
	if err := os.WriteFile(profilePatch, []byte(`
- id: agent-default-model
  config:
    provider: personal-provider
    model: personal-model
- id: absent-row
  config:
    value: ignored
`), 0o644); err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(home, "overlay.yml")
	if err := os.WriteFile(overlay, []byte(`
- id: agent-default-model
  config:
    provider: configured-provider
    model: configured-model
- id: webserver
  config:
    host: 127.0.0.2
    port: 0
`), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := loader.dump("web", false, []string{overlay}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "provider: configured-provider") || strings.Contains(stdout.String(), "personal-provider") {
		t.Fatalf("overlay did not win:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "patched by "+profilePatch+", "+overlay) {
		t.Fatalf("dump missed layer provenance")
	}
	if !strings.Contains(stderr.String(), `patch: entry "absent-row" not found`) {
		t.Fatalf("missing skipped-patch warning: %s", stderr.String())
	}
	composed, err := loader.compose("web", []string{overlay}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(loader, composed)
	if cfg.Provider != "configured-provider" || cfg.Model != "configured-model" || cfg.Host != "127.0.0.2" || cfg.Port != 0 {
		t.Fatalf("composed engine config = %#v", cfg)
	}
}

func TestDefaultDumpSkipsBrokenUserLayer(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DSH_HOME", home)
	loader, err := newProfileLoader(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loader.loadProfile("headless", false); err != nil {
		t.Fatal(err)
	}
	patch := filepath.Join(home, profilesDir, "headless", profilePatchFile)
	if err := os.WriteFile(patch, []byte("invalid: [unclosed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := loader.dump("headless", true, nil, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatalf("default dump parsed the user layer: %v", err)
	}
	if err := loader.dump("headless", false, nil, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("full dump accepted broken user layer")
	}
}

func TestPluginInitializesAndForwardsToPnpm(t *testing.T) {
	home := t.TempDir()
	invoking := t.TempDir()
	capture := t.TempDir()
	bin := t.TempDir()
	script := filepath.Join(bin, "pnpm")
	if err := os.WriteFile(script, []byte("#!/bin/sh\npwd > \"$CAPTURE/cwd\"\nprintf '%s\\n' \"$@\" > \"$CAPTURE/args\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DSH_HOME", home)
	t.Setenv("CAPTURE", capture)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(invoking); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	loader, err := newProfileLoader(false)
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	if err := loader.runPlugin("custom", []string{"add", "."}, strings.NewReader(""), &bytes.Buffer{}, &stderr); err != nil {
		t.Fatal(err)
	}
	profileDir := filepath.Join(home, profilesDir, "custom")
	cwd, err := os.ReadFile(filepath.Join(capture, "cwd"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(cwd)) != profileDir {
		t.Fatalf("pnpm cwd = %q", cwd)
	}
	args, err := os.ReadFile(filepath.Join(capture, "args"))
	if err != nil {
		t.Fatal(err)
	}
	if string(args) != "add\n"+invoking+"\n" {
		t.Fatalf("pnpm args = %q", args)
	}
	if !strings.Contains(stderr.String(), "initialized profile custom") {
		t.Fatalf("missing init diagnostic: %s", stderr.String())
	}
	document, _, err := readManifest(profileDir)
	if err != nil {
		t.Fatal(err)
	}
	bundles, err := profileBundles(document)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bundles, []string{baseBundle}) {
		t.Fatalf("custom bundles = %q", bundles)
	}
}

func TestReconcilePluginsTracksInstalledBundles(t *testing.T) {
	home := t.TempDir()
	loader := &profileLoader{home: home, bundleDirs: map[string]string{}}
	dir, err := loader.profileDir("custom")
	if err != nil {
		t.Fatal(err)
	}
	if err := loader.initProfile(dir, []string{baseBundle}); err != nil {
		t.Fatal(err)
	}
	before, _, err := readManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	after := cloneJSONMap(before)
	after["dependencies"] = map[string]any{"extra-bundle": "1.0.0", "plain": "1.0.0"}
	if err := writeManifest(dir, after); err != nil {
		t.Fatal(err)
	}
	for name, bundle := range map[string]bool{"extra-bundle": true, "plain": false} {
		packageDir := filepath.Join(dir, "node_modules", name)
		if err := os.MkdirAll(packageDir, 0o755); err != nil {
			t.Fatal(err)
		}
		manifest := map[string]any{"name": name, "version": "1.0.0"}
		if bundle {
			manifest["dsh"] = map[string]any{"bundle": map[string]any{"patch": "./cordis.patch.yml"}}
			if err := os.WriteFile(filepath.Join(packageDir, profilePatchFile), []byte("[]\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := writeManifest(packageDir, manifest); err != nil {
			t.Fatal(err)
		}
	}
	var stderr bytes.Buffer
	if err := loader.reconcilePlugins(before, dir, &stderr); err != nil {
		t.Fatal(err)
	}
	reconciled, _, err := readManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	bundles, err := profileBundles(reconciled)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bundles, []string{baseBundle, "extra-bundle"}) {
		t.Fatalf("reconciled bundles = %q", bundles)
	}
	if !strings.Contains(stderr.String(), "plain declares no dsh.bundle") {
		t.Fatalf("missing plain dependency warning: %s", stderr.String())
	}
}

func cloneJSONMap(input map[string]any) map[string]any {
	data, _ := json.Marshal(input)
	var output map[string]any
	_ = json.Unmarshal(data, &output)
	return output
}

func TestPnpmMissingUsesExit127(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DSH_HOME", home)
	t.Setenv("PATH", t.TempDir())
	loader := &profileLoader{home: home, bundleDirs: map[string]string{}}
	err := loader.runPlugin("custom", []string{"root"}, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	var exit exitError
	if !errors.As(err, &exit) || exit.code != 127 {
		t.Fatalf("missing pnpm error = %#v", err)
	}
}

func TestGitPluginFailureKeepsPnpmExitAndAllowBuildsHint(t *testing.T) {
	home := t.TempDir()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "pnpm"), []byte("#!/bin/sh\nexit 42\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DSH_HOME", home)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	loader := &profileLoader{home: home, bundleDirs: map[string]string{}}
	var stderr bytes.Buffer
	err := loader.runPlugin("custom", []string{"add", "github:owner/plugin"}, strings.NewReader(""), &bytes.Buffer{}, &stderr)
	var exit exitError
	if !errors.As(err, &exit) || exit.code != 42 {
		t.Fatalf("pnpm error = %#v", err)
	}
	if !strings.Contains(stderr.String(), "allowBuilds") || !strings.Contains(stderr.String(), filepath.Join(home, profilesDir, "custom", "pnpm-workspace.yaml")) {
		t.Fatalf("missing git install hint: %s", stderr.String())
	}
}

func TestStandaloneBinaryServesEmbeddedRuntime(t *testing.T) {
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "dsh")
	build := runtimeAssetGoCommand(repository, "build", "-o", binary, "./cmd/dsh")
	build.Dir = repository
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build standalone dsh: %v\n%s", err, output)
	}

	outside := t.TempDir()
	home := filepath.Join(outside, "home")
	environment := append(os.Environ(),
		"DSH_HOME="+home,
		"DSH_UPSTREAM_DIR="+filepath.Join(outside, "missing-upstream"),
	)
	dump := exec.Command(binary, "web", "--dump-default-config")
	dump.Dir = outside
	dump.Env = environment
	output, err := dump.CombinedOutput()
	if err != nil {
		t.Fatalf("standalone profile dump: %v\n%s", err, output)
	}
	if !bytes.Contains(output, []byte("@deepseek-ai/dsh-host-webserver")) {
		t.Fatalf("standalone profile dump missed embedded web bundle:\n%s", output)
	}

	requests := make(chan string, 4)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		requests <- request.URL.Path + "\n" + request.Header.Get("Authorization") + "\n" + string(body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"standalone headless reached the model\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(model.Close)
	headless := exec.Command(binary, "--profile", "headless", "run", "the", "standalone", "test")
	headless.Dir = outside
	headless.Env = append(environment,
		"DEEPSEEK_API_KEY=standalone-test-key",
		"DEEPSEEK_BASE_URL="+model.URL,
	)
	headlessOutput, err := headless.CombinedOutput()
	if err != nil {
		t.Fatalf("standalone headless: %v\n%s", err, headlessOutput)
	}
	if string(headlessOutput) != "standalone headless reached the model\n" {
		t.Fatalf("standalone headless output = %q", headlessOutput)
	}
	select {
	case request := <-requests:
		if !strings.HasPrefix(request, "/chat/completions\nBearer standalone-test-key\n") || !strings.Contains(request, "run the standalone test") {
			t.Fatalf("standalone headless request = %q", request)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("standalone headless made no model request")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "web", "--no-open", "--port", "0", "--trusted-host", "lab.internal")
	command.Dir = outside
	command.Env = environment
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	t.Cleanup(func() {
		if command.Process == nil {
			return
		}
		_ = command.Process.Signal(os.Interrupt)
		select {
		case <-wait:
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-wait
		}
	})

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read standalone address: %v; stderr=%s", err, stderr.String())
	}
	endpoint := strings.TrimSpace(strings.TrimPrefix(line, "dsh web: "))
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") {
		t.Fatalf("standalone address = %q; stderr=%s", line, stderr.String())
	}

	response, err := http.Get(endpoint + "/")
	if err != nil {
		t.Fatalf("GET standalone UI: %v; stderr=%s", err, stderr.String())
	}
	html, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("standalone UI response = %d, %v", response.StatusCode, err)
	}
	const bootPrefix = `globalThis["__DSH_BOOT__"] = `
	start := bytes.Index(html, []byte(bootPrefix))
	if start < 0 {
		t.Fatal("standalone UI missed boot graph")
	}
	start += len(bootPrefix)
	end := bytes.Index(html[start:], []byte("</script>"))
	if end < 0 {
		t.Fatal("standalone UI boot graph is unterminated")
	}
	var graph harness.BootGraph
	if err := json.Unmarshal(html[start:start+end], &graph); err != nil {
		t.Fatalf("decode standalone boot graph: %v", err)
	}
	if len(graph.Entries) != 42 {
		t.Fatalf("standalone boot graph has %d entries, want 42", len(graph.Entries))
	}
	plugin, err := http.Get(endpoint + graph.Entries[0].URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, plugin.Body)
	_ = plugin.Body.Close()
	if plugin.StatusCode != http.StatusOK {
		t.Fatalf("standalone plugin status = %d", plugin.StatusCode)
	}

	trustedRequest, err := http.NewRequest(http.MethodPost, endpoint+"/api/host.describe", strings.NewReader(`{"type":"client-request","rpcId":"trusted-host","method":"host.describe","payload":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	trustedRequest.Host = "lab.internal"
	trustedRequest.Header.Set("Content-Type", "application/json")
	trustedResponse, err := http.DefaultClient.Do(trustedRequest)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, trustedResponse.Body)
	_ = trustedResponse.Body.Close()
	if trustedResponse.StatusCode != http.StatusOK {
		t.Fatalf("standalone trusted-host RPC status = %d", trustedResponse.StatusCode)
	}

	request, err := http.NewRequest(http.MethodPost, endpoint+"/api/agentPreset.list", strings.NewReader(`{"type":"client-request","rpcId":"standalone","method":"agentPreset.list","payload":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	presetResponse, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Result struct {
			Value struct {
				Presets []any `json:"presets"`
			} `json:"value"`
		} `json:"result"`
	}
	err = json.NewDecoder(presetResponse.Body).Decode(&envelope)
	_ = presetResponse.Body.Close()
	if err != nil || presetResponse.StatusCode != http.StatusOK || len(envelope.Result.Value.Presets) != 4 {
		t.Fatalf("standalone presets = %d, status=%d, err=%v", len(envelope.Result.Value.Presets), presetResponse.StatusCode, err)
	}
}
