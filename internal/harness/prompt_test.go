package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type promptCaptureProvider struct {
	mu       sync.Mutex
	requests []ChatRequest
}

func (p *promptCaptureProvider) ID() string   { return "prompt-capture" }
func (p *promptCaptureProvider) Name() string { return "Prompt Capture" }
func (p *promptCaptureProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "model-a", Name: "A"}, {ID: "model-b", Name: "B"}}, nil
}
func (p *promptCaptureProvider) Complete(_ context.Context, request ChatRequest, delta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	p.mu.Unlock()
	if err := delta(Delta{Text: "done", Finish: "stop"}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: "done", Finish: "stop"}, nil
}
func (p *promptCaptureProvider) snapshot() []ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ChatRequest(nil), p.requests...)
}

func TestPresetPromptAndWorkspaceInstructionsReachProvider(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(root, "nested")
	if err := os.Mkdir(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	for path, content := range map[string]string{
		filepath.Join(dataDir, "AGENTS.md"):         "global rule",
		filepath.Join(root, "AGENTS.md"):            "root rule",
		filepath.Join(workspace, "AGENTS.local.md"): "nested rule",
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	presetRoot := t.TempDir()
	presetDir := filepath.Join(presetRoot, "test")
	if err := os.Mkdir(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := `
- id: persona
  name: '@deepseek-ai/dsh-persona'
  config:
    text: Agent {{model}} in {{cwd}}.
- id: instructions
  name: '@deepseek-ai/dsh-agent-instructions'
  config:
    maxBytes: 65536
`
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	provider := &promptCaptureProvider{}
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir, cfg.Workspace, cfg.PresetDir = dataDir, workspace, presetRoot
	cfg.Provider, cfg.Model, cfg.Persist = provider.ID(), "model-a", false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(context.Background(), workspace, "prompt-session", "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "first"}}}); err != nil {
		t.Fatal(err)
	}

	requests := provider.snapshot()
	if len(requests) != 1 {
		t.Fatalf("requests = %d", len(requests))
	}
	wantSystem := harnessIdentity + "\n\nAgent model-a in " + workspace + "."
	if requests[0].System != wantSystem {
		t.Fatalf("system = %q, want %q", requests[0].System, wantSystem)
	}
	if len(requests[0].Messages) != 2 || requests[0].Messages[0].Content != "first" {
		t.Fatalf("messages = %#v", requests[0].Messages)
	}
	instructions := requests[0].Messages[1].Content
	for _, want := range []string{"$DSH_HOME/AGENTS.md", "global rule", "AGENTS.md", "root rule", "nested/AGENTS.local.md", "nested rule"} {
		if !strings.Contains(instructions, want) {
			t.Fatalf("instructions missing %q: %s", want, instructions)
		}
	}
	temperature := 0.2
	selection := ModelSelection{
		Provider: provider.ID(), Model: "model-b", ReasoningEffort: "high",
		Temperature: &temperature, MaxTokens: 1234,
	}
	if err := e.SelectModel(id, selection); err != nil {
		t.Fatal(err)
	}
	temperature = 0.9
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "second"}}}); err != nil {
		t.Fatal(err)
	}
	requests = provider.snapshot()
	if len(requests) != 2 || !strings.Contains(requests[1].System, "Agent model-b") ||
		requests[1].Temperature == nil || *requests[1].Temperature != 0.2 || requests[1].MaxTokens != 1234 ||
		requests[1].ReasoningEffort != "high" {
		t.Fatalf("second request = %#v", requests)
	}
	s, _ := e.getSession(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	baselines, headers := 0, 0
	for _, event := range s.Events {
		if event.Type == "user/message" && eventSourceKind(event.Data) == "agent-instructions" {
			baselines++
		}
		if event.Type == "request/header" {
			headers++
		}
	}
	if baselines != 1 || headers != 2 {
		t.Fatalf("baseline/header counts = %d/%d", baselines, headers)
	}
}

func TestPresetEditorConfigIsPinnedIntoSessionRuntime(t *testing.T) {
	presetRoot := t.TempDir()
	presetDir := filepath.Join(presetRoot, "editor")
	if err := os.Mkdir(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := `
- id: editor
  name: '@deepseek-ai/dsh-tool-str-replace-editor'
  config:
    maxOutputChars: 32
    description: custom editor
`
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	path := filepath.Join(workspace, "long.txt")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 200)), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), workspace, presetRoot, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), workspace, "editor-session", "editor")
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.editorMaxOutputChars != 32 || runtimeConfig.editorDescription != "custom editor" {
		t.Fatalf("editor runtime = %#v", runtimeConfig)
	}
	e.mu.RLock()
	tool := e.tools["str_replace_editor"]
	e.mu.RUnlock()
	result, err := tool.Execute(t.Context(), ToolCall{
		Name: "str_replace_editor", SessionID: id, Workspace: workspace,
		Arguments: []byte(`{"command":"view","path":"` + path + `"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Content) != 1 || !strings.Contains(result.Content[0].Text, "<response clipped>") || len(result.Content[0].Text) <= 32 {
		t.Fatalf("editor result = %#v", result)
	}
}

func TestPresetFilesystemConfigsArePinnedIntoSessionRuntime(t *testing.T) {
	presetRoot := t.TempDir()
	presetDir := filepath.Join(presetRoot, "fs-config")
	if err := os.Mkdir(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := `
- id: bash
  name: '@deepseek-ai/dsh-tool-bash'
  config:
    enableRunInBackground: false
- id: fs
  name: '@deepseek-ai/dsh-tool-fs'
  config:
    readLimit: 1
    readMaxLineLength: 4
    readMaxBytes: 128
    readStreamMinSize: 1
- id: search
  name: '@deepseek-ai/dsh-tool-fs-search'
  config:
    sampleOverCapGlobResults: true
    globMaxResults: 1
    grepMaxMatches: 1
    grepMaxLineBytes: 5
    timeoutMs: 42
    graceMs: 43
    stderrMaxBytes: 44
    rawOutputMaxBytes: 1024
    searchMetaMaxBytes: 45
`
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "read.txt"), []byte("abcdefgh\nsecond\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		if err := os.WriteFile(filepath.Join(workspace, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), workspace, presetRoot, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), workspace, "fs-config-session", "fs-config")
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.readLimit != 1 || runtimeConfig.readMaxLineLength != 4 || runtimeConfig.readMaxBytes != 128 || runtimeConfig.readStreamMinSize != 1 || !runtimeConfig.globSampleOverCapResults || runtimeConfig.globMaxResults != 1 || runtimeConfig.grepMaxMatches != 1 || runtimeConfig.grepMaxLineBytes != 5 || runtimeConfig.searchTimeout != 42*time.Millisecond || runtimeConfig.searchGrace != 43*time.Millisecond || runtimeConfig.searchStderrMaxBytes != 44 || runtimeConfig.rawOutputMaxBytes != 1024 || runtimeConfig.searchMetaMaxBytes != 45 || runtimeConfig.bashEnableRunInBackground {
		t.Fatalf("filesystem runtime = %#v", runtimeConfig)
	}
	if tool, ok := e.toolForSession(s, "glob"); !ok || tool.Timeout != 42*time.Millisecond {
		t.Fatalf("glob timeout = %v/%v", tool.Timeout, ok)
	}
	schemas, err := e.toolsForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, schema := range schemas {
		if schema.Name == "read" {
			properties := schema.Parameters["properties"].(map[string]any)
			if properties["limit"].(map[string]any)["maximum"] != 1 {
				t.Fatalf("read schema = %#v", schema.Parameters)
			}
		}
		if schema.Name == "bash" {
			properties := schema.Parameters["properties"].(map[string]any)
			if _, ok := properties["run_in_background"]; ok {
				t.Fatalf("bash schema retained disabled background option: %#v", schema.Parameters)
			}
		}
	}
	e.mu.RLock()
	readTool, globTool, bashTool := e.tools["read"], e.tools["glob"], e.tools["bash"]
	e.mu.RUnlock()
	if bashTool.Execute != nil {
		args, _ := json.Marshal(map[string]any{"command": "true", "description": "test", "run_in_background": true})
		if _, err := bashTool.Execute(t.Context(), ToolCall{Name: "bash", Arguments: args, SessionID: id, Workspace: workspace}); err == nil || !strings.Contains(err.Error(), "run_in_background is disabled") {
			t.Fatalf("disabled background execution error = %v", err)
		}
	}
	args, _ := json.Marshal(map[string]any{"file_path": "read.txt"})
	result, err := readTool.Execute(t.Context(), ToolCall{Name: "read", Arguments: args, SessionID: id, Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(contentValueText(result.Content), "line truncated to 4 chars") {
		t.Fatalf("read output = %q", contentValueText(result.Content))
	}
	readArgs := []byte(`{"file_path":"read.txt"}`)
	result, err = readTool.Execute(t.Context(), ToolCall{Name: "read", Arguments: readArgs, SessionID: id, Workspace: workspace})
	if err != nil || len(result.Content) != 1 || !strings.Contains(result.Content[0].Text, "1: abcd") {
		t.Fatalf("streamed read result = %#v, err=%v", result, err)
	}
	args, _ = json.Marshal(map[string]any{"pattern": "*.txt"})
	result, err = globTool.Execute(t.Context(), ToolCall{Name: "glob", Arguments: args, SessionID: id, Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(contentValueText(result.Content), "Showing 1 of 4 paths") {
		t.Fatalf("glob output = %q", contentValueText(result.Content))
	}
}

func TestPresetFilesystemSearchRejectsInvalidConfig(t *testing.T) {
	tests := []struct{ name, config, contains string }{
		{"required sample", "    globMaxResults: 1\n", "sampleOverCapGlobResults is required"},
		{"fractional", "    sampleOverCapGlobResults: true\n    stderrMaxBytes: 1.5\n", "stderrMaxBytes must be a positive integer"},
		{"zero", "    sampleOverCapGlobResults: true\n    searchMetaMaxBytes: 0\n", "searchMetaMaxBytes must be a positive integer"},
		{"grace overflow", "    sampleOverCapGlobResults: true\n    graceMs: 2147483648\n", "graceMs must be no greater than 2147483647"},
		{"unknown", "    sampleOverCapGlobResults: true\n    stale: true\n", "tool-fs-search unknown key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			presetDir := filepath.Join(root, "search")
			if err := os.MkdirAll(presetDir, 0o755); err != nil {
				t.Fatal(err)
			}
			content := "- name: '@deepseek-ai/dsh-tool-fs-search'\n  config:\n" + test.config
			if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := DefaultConfig()
			cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
			cfg.SessionTitleLLM.Enabled = false
			e, err := New(WithConfig(cfg))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			if _, err := e.CreateSession(t.Context(), cfg.Workspace, "", "search"); err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("CreateSession() error = %v, want %q", err, test.contains)
			}
		})
	}
}

func TestPresetPersistentShellConfigIsPinnedIntoSessionRuntime(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "persistent")
	if err := os.Mkdir(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := "- id: shell\n  name: '@deepseek-ai/dsh-tool-bash-persistent'\n  config:\n    timeoutMs: 1234\n    maxOutputChars: 77\n"
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "persistent-config", "persistent")
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.persistentBashTimeout != 1234*time.Millisecond || runtimeConfig.persistentBashMaxOutputChars != 77 {
		t.Fatalf("persistent runtime = %#v", runtimeConfig)
	}
}

func TestSkillCatalogIsPersistedAndDescriptionCapped(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "catalog")
	if err := os.Mkdir(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "skills", "alpha-skill"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "skills", "alpha-skill", "SKILL.md"), []byte("---\nname: alpha-skill\ndescription: A very long skill description\n---\n\nbody\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	composition := "- id: skill\n  name: '@deepseek-ai/dsh-tool-skill'\n  config:\n    catalogDescriptionMaxLength: 10\n"
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	provider := &promptCaptureProvider{}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.SkillDir, cfg.Persist = t.TempDir(), t.TempDir(), root, filepath.Join(root, "skills"), false
	cfg.Provider, cfg.Model = provider.ID(), "model-a"
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.RegisterProvider(provider)
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "catalog-session", "catalog")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "first"}}}); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 1 {
		t.Fatalf("requests = %d", len(requests))
	}
	catalogSeen := false
	for _, message := range requests[0].Messages {
		if strings.Contains(message.Content, "<available_skills>") {
			catalogSeen = strings.Contains(message.Content, "- `alpha-skill`: A very ...")
		}
	}
	if !catalogSeen {
		t.Fatalf("catalog messages = %#v", requests[0].Messages)
	}
	if _, err := e.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "second"}}}); err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	s.mu.Lock()
	catalogCount := 0
	for _, event := range s.Events {
		if event.Type == "user/message" && eventSourceKind(event.Data) == "skill-catalog" {
			catalogCount++
		}
	}
	s.mu.Unlock()
	if catalogCount != 1 {
		t.Fatalf("catalog count after unchanged turn = %d", catalogCount)
	}
}

func TestSkillUserInvocationUsesIndependentPolicy(t *testing.T) {
	root := t.TempDir()
	workspace := t.TempDir()
	skills := filepath.Join(root, "skills")
	for name, body := range map[string]string{
		"model-skill":  "---\nname: model-skill\ndescription: Model skill\n---\n\nModel body.",
		"direct-skill": "---\nname: direct-skill\ndescription: Direct skill\ndisable-model-invocation: true\n---\n\nDirect body.",
		"hidden-skill": "---\nname: hidden-skill\ndescription: Hidden skill\nuser-invocable: false\n---\n\nHidden body.",
	} {
		dir := filepath.Join(skills, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(skills, "flat-skill.md"), []byte("---\nname: flat-skill\ndescription: Flat skill\ndisable-model-invocation: off\nuser-invocable: YES\n---\n\nFlat body."), 0o600); err != nil {
		t.Fatal(err)
	}
	presetRoot := t.TempDir()
	preset := filepath.Join(presetRoot, "skills")
	if err := os.MkdirAll(preset, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(preset, "agent.cordis.yml"), []byte("- id: skill\n  name: '@deepseek-ai/dsh-tool-skill'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	provider := &promptCaptureProvider{}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.SkillDir, cfg.Persist = t.TempDir(), workspace, presetRoot, skills, false
	cfg.Provider, cfg.Model = provider.ID(), "model-a"
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.RegisterProvider(provider)
	id, err := e.CreateSession(t.Context(), workspace, "skill-user-policy", "skills")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "Please use /direct-skill now, but ignore /hidden-skill."}}}); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 1 {
		t.Fatalf("requests = %d", len(requests))
	}
	var catalog, invocation ChatMessage
	for _, message := range requests[0].Messages {
		if strings.Contains(message.Content, "<available_skills>") {
			catalog = message
		}
		if message.Source["kind"] == "skill-invocation" {
			invocation = message
		}
	}
	if !strings.Contains(catalog.Content, "model-skill") || !strings.Contains(catalog.Content, "hidden-skill") || !strings.Contains(catalog.Content, "flat-skill") || strings.Contains(catalog.Content, "direct-skill") {
		t.Fatalf("catalog policy = %q", catalog.Content)
	}
	if !strings.Contains(invocation.Content, "<skill_content name=\"direct-skill\">") || !strings.Contains(invocation.Content, "Direct body.") {
		t.Fatalf("invocation = %#v", invocation)
	}
	if strings.Contains(invocation.Content, "hidden-skill") {
		t.Fatalf("user-disabled skill was injected: %q", invocation.Content)
	}
	if len(requests[0].Messages) == 0 || requests[0].Messages[len(requests[0].Messages)-1].Source["kind"] != "skill-invocation" {
		t.Fatalf("invocation was not appended last: %#v", requests[0].Messages)
	}
}

func TestPresetSearchRawOutputCapFailsWithStableCode(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "search-cap")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := "- id: search\n  name: '@deepseek-ai/dsh-tool-fs-search'\n  config:\n    sampleOverCapGlobResults: false\n    rawOutputMaxBytes: 10\n"
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	for _, name := range []string{"first.txt", "second.txt"} {
		if err := os.WriteFile(filepath.Join(workspace, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), workspace, root, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), workspace, "search-cap-session", "search-cap")
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool := e.tools["glob"]
	e.mu.RUnlock()
	args, _ := json.Marshal(map[string]any{"pattern": "*.txt"})
	if _, err := tool.Execute(t.Context(), ToolCall{Name: "glob", Arguments: args, SessionID: id, Workspace: workspace}); err == nil || !strings.Contains(err.Error(), "SEARCH_RAW_OUTPUT_OVERFLOW") {
		t.Fatalf("raw output cap error = %v", err)
	}
}

func TestPresetToolResultPrunerConfigIsPinnedIntoSessionRuntime(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "pruner")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := `- id: pruner
  name: '@deepseek-ai/dsh-compaction-tool-result-pruner'
  config:
    thresholdChars: 64
    headChars: 4
    tailChars: 4
`
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "pruner", "pruner")
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.toolResultPruneThresholdChars != 64 || runtimeConfig.toolResultPruneHeadChars != 4 || runtimeConfig.toolResultPruneTailChars != 4 {
		t.Fatalf("pruner runtime = %#v", runtimeConfig)
	}
	body := strings.Repeat("x", 80)
	if _, err := e.appendEvent(s, "tool/result", map[string]any{
		"message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool-result", "content": []any{map[string]any{"type": "text", "text": body}}}}},
	}); err != nil {
		t.Fatal(err)
	}
	if pruned, err := e.pruneToolResults(s); err != nil || pruned != 1 {
		t.Fatalf("prune = %d, %v", pruned, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	last := s.Events[len(s.Events)-1]
	data, _ := last.Data.(map[string]any)
	message, _ := data["message"].(map[string]any)
	blocks := contentBlocks(message["content"])
	if len(blocks) != 1 || len(blocks[0].Content) != 1 || !strings.HasPrefix(blocks[0].Content[0].Text, "xxxx") || !strings.HasSuffix(blocks[0].Content[0].Text, "xxxx") || !strings.Contains(blocks[0].Content[0].Text, toolResultPruneMarker) {
		t.Fatalf("pruned content = %#v", blocks)
	}
}

func TestPresetWebToolConfigIsPinnedIntoSchemaPromptAndExecution(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "web")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := `- id: web-tools
  name: '@deepseek-ai/dsh-tool-web'
  config:
    search: true
    fetch: false
    searchMaxResults: 2
    searchMaxQueries: 2
    searchTimeoutMs: 1234
    fetchTimeoutMs: 2345
    fetchMaxOutputChars: 3456
`
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "web", "web")
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.webTools == nil || runtimeConfig.webTools.SearchMaxResults != 2 || runtimeConfig.webTools.SearchMaxQueries != 2 || runtimeConfig.webTools.SearchTimeout != 1234*time.Millisecond || runtimeConfig.webTools.FetchEnabled {
		t.Fatalf("web runtime = %#v", runtimeConfig.webTools)
	}
	tool, ok := e.toolForSession(s, "web_search")
	if !ok || tool.Timeout != 1234*time.Millisecond || !strings.Contains(tool.Schema.Description, "1–2 queries") {
		t.Fatalf("session web_search = %#v, ok=%v", tool, ok)
	}
	if _, err := tool.Execute(t.Context(), ToolCall{Name: "web_search", SessionID: id, Arguments: json.RawMessage(`{"queries":["one","two","three"]}`)}); err == nil || !strings.Contains(err.Error(), "at most 2 queries") {
		t.Fatalf("session query cap error = %v", err)
	}
	prompt, err := e.systemPromptForSession(s, ModelSelection{Model: "m"}, runtimeConfig)
	if err != nil || !strings.Contains(prompt, "accepts 1–2 non-empty search queries") || strings.Contains(prompt, "Follow up with web_fetch") {
		t.Fatalf("session web prompt = %q, %v", prompt, err)
	}
}

func TestPresetWebToolRejectsInvalidConfig(t *testing.T) {
	tests := []struct{ name, config, contains string }{
		{"unknown", "    other: 1\n", "tool-web unknown key"},
		{"boolean", "    search: yes\n", "search must be boolean"},
		{"fractional queries", "    searchMaxQueries: 1.5\n", "searchMaxQueries must be a positive integer"},
		{"zero timeout", "    searchTimeoutMs: 0\n", "searchTimeoutMs must be a positive integer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			presetDir := filepath.Join(root, "web")
			if err := os.MkdirAll(presetDir, 0o755); err != nil {
				t.Fatal(err)
			}
			content := "- name: '@deepseek-ai/dsh-tool-web'\n  config:\n" + test.config
			if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := DefaultConfig()
			cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
			cfg.SessionTitleLLM.Enabled = false
			e, err := New(WithConfig(cfg))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			if _, err := e.CreateSession(t.Context(), cfg.Workspace, "", "web"); err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("CreateSession() error = %v, want %q", err, test.contains)
			}
		})
	}
}

func TestPresetToolResultPrunerRejectsInvalidConfig(t *testing.T) {
	tests := []struct{ name, config, contains string }{
		{"unknown", "    maxChars: 100\n", "compaction-tool-result-pruner unknown key"},
		{"zero threshold", "    thresholdChars: 0\n", "thresholdChars must be positive integer"},
		{"negative head", "    headChars: -1\n", "headChars must be non-negative integer"},
		{"fractional tail", "    tailChars: 1.5\n", "tailChars must be non-negative integer"},
		{"budget", "    thresholdChars: 50\n    headChars: 20\n    tailChars: 20\n", "headChars + marker + tailChars must fit thresholdChars"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			presetDir := filepath.Join(root, "pruner")
			if err := os.MkdirAll(presetDir, 0o755); err != nil {
				t.Fatal(err)
			}
			content := "- name: '@deepseek-ai/dsh-compaction-tool-result-pruner'\n  config:\n" + test.config
			if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := DefaultConfig()
			cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
			cfg.SessionTitleLLM.Enabled = false
			e, err := New(WithConfig(cfg))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			if _, err := e.CreateSession(t.Context(), cfg.Workspace, "", "pruner"); err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("CreateSession() error = %v, want %q", err, test.contains)
			}
		})
	}
}

func TestPresetRalphConfigIsPinnedIntoSessionRuntime(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "ralph")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := `- id: ralph
  name: '@deepseek-ai/dsh-tool-ralph'
  config:
    subagentProvider: custom-fresh
    maxRounds: 7
    maxHandoffChars: 99
    maxResultChars: 123
`
	compositionPath := filepath.Join(presetDir, "agent.cordis.yml")
	if err := os.WriteFile(compositionPath, []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "ralph", "ralph")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.ralphSubagentProvider != "custom-fresh" || runtimeConfig.ralphMaxRounds != 7 || runtimeConfig.ralphMaxHandoffChars != 99 || runtimeConfig.ralphMaxResultChars != 123 {
		t.Fatalf("Ralph runtime = %#v", runtimeConfig)
	}
	if err := os.WriteFile(compositionPath, []byte(strings.ReplaceAll(composition, "maxRounds: 7", "maxRounds: 2")), 0o600); err != nil {
		t.Fatal(err)
	}
	pinned, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.ralphMaxRounds != 7 {
		t.Fatalf("Ralph runtime changed in existing session: %#v", pinned)
	}
}

func TestPresetRalphRejectsInvalidConfig(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "ralph")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte("- id: ralph\n  name: '@deepseek-ai/dsh-tool-ralph'\n  config:\n    maxRounds: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if _, err := e.CreateSession(t.Context(), cfg.Workspace, "ralph", "ralph"); err == nil || !strings.Contains(err.Error(), "tool-ralph maxRounds must be a positive safe integer") {
		t.Fatalf("invalid Ralph config error = %v", err)
	}
}

func TestPresetWorkflowConfigsArePinnedIntoSessionRuntime(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "workflow")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := `- id: engine
  name: '@deepseek-ai/dsh-workflow-worker-thread'
  config:
    provider: custom-flow
    maxConcurrentAgents: 3
    maxTotalAgents: 7
    maxItemsPerCall: 11
    syncTimeoutMs: 13
    disposeGraceMs: 17
- id: tool
  name: '@deepseek-ai/dsh-tool-workflow'
  config:
    toolName: orchestrate
    maxResultChars: 19
`
	path := filepath.Join(presetDir, "agent.cordis.yml")
	if err := os.WriteFile(path, []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "workflow", "workflow")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.workflowToolName != "orchestrate" || runtimeConfig.workflowMaxResultChars != 19 || runtimeConfig.workflowProvider != "custom-flow" || runtimeConfig.workflowMaxConcurrentAgents != 3 || runtimeConfig.workflowMaxAgents != 7 || runtimeConfig.workflowMaxItems != 11 || runtimeConfig.workflowSyncTimeout != 13*time.Millisecond || runtimeConfig.workflowDisposeGrace != 17*time.Millisecond || !runtimeConfig.toolNames["orchestrate"] || runtimeConfig.toolNames["workflow"] {
		t.Fatalf("workflow runtime = %#v", runtimeConfig)
	}
	if err := os.WriteFile(path, []byte(strings.ReplaceAll(composition, "maxTotalAgents: 7", "maxTotalAgents: 2")), 0o600); err != nil {
		t.Fatal(err)
	}
	pinned, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.workflowMaxAgents != 7 {
		t.Fatalf("workflow generation changed in existing session: %#v", pinned)
	}
}

func TestPresetCompactionConfigIsPinnedIntoSessionRuntime(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "compact")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := `- name: '@deepseek-ai/dsh-compaction-basic'
  config:
    thresholdRatio: 0.7
    retainTokens: 123
    summarizationProvider: summary
    summarizationModel: small
    maxTokens: 456
    compactionRetries: 2
    maxOverflowRetries: 3
    auto: false
    modelPolicies:
      - provider: route
        model: model
        thresholdRatio: 0.6
        retainRatio: 0.2
        summarizationProvider: alternate
        summarizationModel: compact
        maxTokens: 99
        compactionRetries: 4
        maxOverflowRetries: 5
`
	path := filepath.Join(presetDir, "agent.cordis.yml")
	if err := os.WriteFile(path, []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "", "compact")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	policy := compactionPolicyFor(runtimeConfig.compactionConfig, ModelSelection{Provider: "route", Model: "model"})
	if !runtimeConfig.compactionEnabled || runtimeConfig.compactionAuto || policy.ThresholdRatio != 0.6 || policy.RetainRatio != 0.2 || policy.RetainTokens != 0 || policy.SummarizationProvider != "alternate" || policy.SummarizationModel != "compact" || policy.MaxTokens != 99 || policy.CompactionRetries != 4 || policy.MaxOverflowRetries != 5 {
		t.Fatalf("compaction runtime = %#v, policy = %#v", runtimeConfig, policy)
	}
	if err := os.WriteFile(path, []byte(strings.ReplaceAll(composition, "maxTokens: 456", "maxTokens: 7")), 0o600); err != nil {
		t.Fatal(err)
	}
	pinned, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.compactionConfig.MaxTokens != 456 {
		t.Fatalf("compaction generation changed in existing session: %#v", pinned.compactionConfig)
	}
}

func TestPresetCompactionRejectsInvalidConfig(t *testing.T) {
	tests := []struct{ name, config, contains string }{
		{"unknown", "    stale: true\n", "compaction-basic unknown key"},
		{"retain conflict", "    retainRatio: 0.2\n    retainTokens: 10\n", "retainRatio and retainTokens are mutually exclusive"},
		{"summary pair", "    summarizationProvider: summary\n", "summarizationProvider and summarizationModel must be set together"},
		{"duplicate target", "    modelPolicies:\n      - provider: route\n        model: model\n      - provider: route\n        model: model\n", "duplicate model policy for route/model"},
		{"merged ratio", "    retainRatio: 0.2\n    modelPolicies:\n      - provider: route\n        model: model\n        thresholdRatio: 0.1\n", "retainRatio (0.2) must be less than the resolved thresholdRatio (0.1)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			presetDir := filepath.Join(root, "compact")
			if err := os.MkdirAll(presetDir, 0o755); err != nil {
				t.Fatal(err)
			}
			content := "- name: '@deepseek-ai/dsh-compaction-basic'\n  config:\n" + test.config
			if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := DefaultConfig()
			cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
			cfg.SessionTitleLLM.Enabled = false
			e, err := New(WithConfig(cfg))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			if _, err := e.CreateSession(t.Context(), cfg.Workspace, "", "compact"); err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("invalid compaction config error = %v, want %q", err, test.contains)
			}
		})
	}
}

func TestPresetCompactCommandRequiresBackend(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "compact")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte("- name: '@deepseek-ai/dsh-command-compact'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if _, err := e.CreateSession(t.Context(), cfg.Workspace, "", "compact"); err == nil || !strings.Contains(err.Error(), "command-compact requires a mounted compaction service") {
		t.Fatalf("command-only preset error = %v", err)
	}
}

func TestPresetWithoutToolResultPrunerDoesNotPrune(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "plain")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte("- id: bash\n  name: '@deepseek-ai/dsh-tool-bash'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "plain", "plain")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	body := strings.Repeat("x", 80)
	if _, err := e.appendEvent(s, "tool/result", map[string]any{"message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool-result", "content": []any{map[string]any{"type": "text", "text": body}}}}}}); err != nil {
		t.Fatal(err)
	}
	if pruned, err := e.pruneToolResults(s); err != nil || pruned != 0 {
		t.Fatalf("unmounted pruner = %d, %v", pruned, err)
	}
}

func TestPresetPrunerUsesPluginDefaultsInsteadOfHostOverride(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "defaults")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte("- id: pruner\n  name: '@deepseek-ai/dsh-compaction-tool-result-pruner'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.ToolResultPruner.ThresholdChars = 10000
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "defaults", "defaults")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := e.getSession(id)
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	defaults := defaultToolResultPruneConfig()
	if runtimeConfig.toolResultPruneThresholdChars != defaults.ThresholdChars || runtimeConfig.toolResultPruneHeadChars != defaults.HeadChars || runtimeConfig.toolResultPruneTailChars != defaults.TailChars {
		t.Fatalf("plugin defaults = %#v, want %#v", runtimeConfig, defaults)
	}
}

func TestPresetRuntimeGenerationsStayPinnedAndChildrenInheritParent(t *testing.T) {
	presetRoot := t.TempDir()
	presetDir := filepath.Join(presetRoot, "edited")
	if err := os.Mkdir(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := filepath.Join(presetDir, "agent.cordis.yml")
	write := func(tool string) {
		t.Helper()
		body := "- id: tool\n  name: '" + tool + "'\n"
		if err := os.WriteFile(composition, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("@deepseek-ai/dsh-tool-bash")
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), presetRoot, false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	firstID, err := e.CreateSession(t.Context(), cfg.Workspace, "preset-first", "edited")
	if err != nil {
		t.Fatal(err)
	}
	write("@deepseek-ai/dsh-tool-fs")
	secondID, err := e.CreateSession(t.Context(), cfg.Workspace, "preset-second", "edited")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(composition); err != nil {
		t.Fatal(err)
	}
	childID, err := e.createSession(t.Context(), SessionHeader{
		ID: "preset-child", CWD: cfg.Workspace, AgentPreset: "edited",
		ParentSession: firstID, Origin: "subagent", Mode: "continuable",
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	runtimeFor := func(id string) agentRuntime {
		session, err := e.getSession(id)
		if err != nil {
			t.Fatal(err)
		}
		runtimeConfig, err := e.runtimeForSession(session)
		if err != nil {
			t.Fatal(err)
		}
		return runtimeConfig
	}
	first := runtimeFor(firstID)
	second := runtimeFor(secondID)
	child := runtimeFor(childID)
	if !first.toolNames["bash"] || first.toolNames["read"] {
		t.Fatalf("first generation = %#v", first.toolNames)
	}
	if !second.toolNames["read"] || second.toolNames["bash"] {
		t.Fatalf("second generation = %#v", second.toolNames)
	}
	if !child.toolNames["bash"] || child.toolNames["read"] {
		t.Fatalf("child generation = %#v", child.toolNames)
	}
}

func TestCompletePresetPersonaSuppressesHarnessIdentity(t *testing.T) {
	e := newIntegrationEngine(t)
	s := &Session{Header: SessionHeader{CWD: "/work"}}
	got, err := e.systemPromptForSession(s, ModelSelection{Model: "m"}, agentRuntime{persona: "Only {{model}}.", completePersona: true})
	if err != nil || got != "Only m." {
		t.Fatalf("system prompt = %q, %v", got, err)
	}
}

func TestGoalPresetConfigControlsPolicyDefaultsAndDriver(t *testing.T) {
	presetRoot := t.TempDir()
	presetDir := filepath.Join(presetRoot, "goal-config")
	if err := os.Mkdir(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := `
- id: goal
  name: '@deepseek-ai/dsh-goal'
  config:
    defaultMaxGoalRounds: 7
- id: goal-tool
  name: '@deepseek-ai/dsh-tool-goal'
  config:
    blockedAfterConsecutiveRounds: 5
`
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), presetRoot, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "goal-config-session", "goal-config")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.goalDefaultMaxRounds != 7 || runtimeConfig.goalBlockThreshold != 5 || runtimeConfig.goalRoundDriver {
		t.Fatalf("goal runtime config = %#v", runtimeConfig)
	}
	if _, err := e.GoalMutation(id, "create", "configured goal", 0, 0); err != nil {
		t.Fatal(err)
	}
	goal, _ := e.GetGoal(id)
	if goal == nil || goal.MaxGoalRounds != 7 {
		t.Fatalf("configured goal = %#v", goal)
	}
	if scheduled, err := e.scheduleGoalRound(s); err != nil || scheduled {
		t.Fatalf("uncomposed driver scheduled=%v err=%v", scheduled, err)
	}
	system, err := e.systemPromptForSession(s, ModelSelection{Model: "m"}, runtimeConfig)
	if err != nil || !strings.Contains(system, "at least 5 consecutive goal rounds") {
		t.Fatalf("goal policy system = %q, %v", system, err)
	}
}

func TestPresetSkillFilesystemCustomDirsAffectSessionDiscovery(t *testing.T) {
	presetRoot := t.TempDir()
	presetDir := filepath.Join(presetRoot, "custom-skills")
	customRoot := filepath.Join(t.TempDir(), "extra-skills")
	if err := os.MkdirAll(filepath.Join(presetDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(customRoot, "extra"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(customRoot, "extra", "SKILL.md"), []byte("---\nname: extra\ndescription: Extra preset skill\n---\n\nUse the extra skill.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	composition := "- id: skill-filesystem\n  name: '@deepseek-ai/dsh-skill-filesystem'\n  config:\n    customSkillDirs:\n      - " + customRoot + "\n- id: tool-skill\n  name: '@deepseek-ai/dsh-tool-skill'\n"
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), presetRoot, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "custom-skill-session", "custom-skills")
	if err != nil {
		t.Fatal(err)
	}
	rows, rpcErr := e.skillRecordsForSession(id)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	found := false
	for _, row := range rows {
		if row.name == "extra" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("custom preset skill root was not discovered: %#v", rows)
	}
	newRoot := filepath.Join(t.TempDir(), "replacement-skills")
	if err := os.MkdirAll(filepath.Join(newRoot, "replacement"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(newRoot, "replacement", "SKILL.md"), []byte("---\nname: replacement\ndescription: Replacement skill\n---\n\nReplacement.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	replacement := "- id: skill-filesystem\n  name: '@deepseek-ai/dsh-skill-filesystem'\n  config:\n    customSkillDirs:\n      - " + newRoot + "\n- id: tool-skill\n  name: '@deepseek-ai/dsh-tool-skill'\n"
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(replacement), 0o600); err != nil {
		t.Fatal(err)
	}
	newID, err := e.CreateSession(t.Context(), cfg.Workspace, "replacement-skill-session", "custom-skills")
	if err != nil {
		t.Fatal(err)
	}
	newRows, rpcErr := e.skillRecordsForSession(newID)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	oldStillPresent, replacementPresent := false, false
	for _, row := range rows {
		oldStillPresent = oldStillPresent || row.name == "extra"
	}
	for _, row := range newRows {
		replacementPresent = replacementPresent || row.name == "replacement"
	}
	if !oldStillPresent || !replacementPresent {
		t.Fatalf("preset skill generation pinning failed: old=%#v new=%#v", rows, newRows)
	}
}

func TestSkillDiscoveryUsesProjectBeforeCustomAndUserRoots(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(root, "src")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	projectSkills := filepath.Join(root, ".dsh", "skills", "same")
	customSkills := filepath.Join(t.TempDir(), "same")
	userSkills := filepath.Join(t.TempDir(), "skills", "same")
	for _, dir := range []string{projectSkills, customSkills, userSkills} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, description, body string) {
		t.Helper()
		data := []byte("---\nname: same\ndescription: " + description + "\n---\n\n" + body)
		if err := os.WriteFile(filepath.Join(path, "SKILL.md"), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(projectSkills, "project", "project body")
	write(customSkills, "custom", "custom body")
	write(userSkills, "user", "user body")

	cfg := DefaultConfig()
	cfg.DataDir = filepath.Dir(filepath.Dir(userSkills))
	cfg.AgentsHome = t.TempDir()
	cfg.Workspace, cfg.SkillDir, cfg.Persist = workspace, filepath.Dir(customSkills), false
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), workspace, "skill-precedence", "")
	if err != nil {
		t.Fatal(err)
	}
	definition, err := e.LoadSkill(id, "same")
	if err != nil {
		t.Fatal(err)
	}
	if definition.Content != "project body" || definition.Description != "project" {
		t.Fatalf("winner did not follow project precedence: %#v", definition)
	}
}

func TestSkillDiscoveryFollowsSymlinkedSkillEntries(t *testing.T) {
	root := t.TempDir()
	targetDir := filepath.Join(t.TempDir(), "linked-bundle")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, "SKILL.md"), []byte("---\nname: linked-bundle\ndescription: Linked bundle\n---\n\nBundle body."), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(targetDir, filepath.Join(root, "bundle-link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	flatTarget := filepath.Join(t.TempDir(), "flat.md")
	if err := os.WriteFile(flatTarget, []byte("---\nname: linked-flat\ndescription: Linked flat\n---\n\nFlat body."), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(flatTarget, filepath.Join(root, "linked-flat.md")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.SkillDir, cfg.Persist = t.TempDir(), t.TempDir(), root, false
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "symlink-skills", "")
	if err != nil {
		t.Fatal(err)
	}
	rows, rpcErr := e.skillRecordsForSession(id)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	found := map[string]bool{}
	for _, row := range rows {
		found[row.name] = true
	}
	if !found["linked-bundle"] || !found["linked-flat"] {
		t.Fatalf("symlinked skills were not discovered: %#v", rows)
	}
}

func TestPresetSkillFilesystemCanDisableDefaultRootsAndSelectBundledRoot(t *testing.T) {
	presetRoot := t.TempDir()
	presetDir := filepath.Join(presetRoot, "isolated")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bundled := filepath.Join(t.TempDir(), "bundled")
	if err := os.MkdirAll(filepath.Join(bundled, "shipped"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundled, "shipped", "SKILL.md"), []byte("---\nname: shipped\ndescription: Shipped skill\n---\n\nShipped body."), 0o600); err != nil {
		t.Fatal(err)
	}
	composition := "- id: fs\n  name: '@deepseek-ai/dsh-skill-filesystem'\n  config:\n    includeDefaultRoots: false\n    bundledSkillDir: " + bundled + "\n- id: tool\n  name: '@deepseek-ai/dsh-tool-skill'\n"
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, ".dsh", "skills", "project-only"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, ".dsh", "skills", "project-only", "SKILL.md"), []byte("---\nname: project-only\ndescription: Project skill\n---\n\nProject body."), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), workspace, presetRoot, false
	cfg.AgentsHome, cfg.SkillDir = t.TempDir(), t.TempDir()
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), workspace, "isolated-skills", "isolated")
	if err != nil {
		t.Fatal(err)
	}
	rows, rpcErr := e.skillRecordsForSession(id)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	foundShipped, foundProject := false, false
	for _, row := range rows {
		foundShipped = foundShipped || row.name == "shipped"
		foundProject = foundProject || row.name == "project-only"
	}
	if !foundShipped || foundProject {
		t.Fatalf("isolated skill roots mismatch: shipped=%v project=%v rows=%#v", foundShipped, foundProject, rows)
	}
}

func TestPresetWithoutSkillFilesystemDoesNotDiscoverProjectSkills(t *testing.T) {
	root := t.TempDir()
	presetDir := filepath.Join(root, "minimal")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte("- id: tool\n  name: '@deepseek-ai/dsh-tool-bash'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, ".dsh", "skills", "project-only"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, ".dsh", "skills", "project-only", "SKILL.md"), []byte("---\nname: project-only\ndescription: Project skill\n---\n\nProject body."), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), workspace, root, false
	cfg.SkillDir, cfg.AgentsHome = t.TempDir(), t.TempDir()
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), workspace, "minimal-skills", "minimal")
	if err != nil {
		t.Fatal(err)
	}
	rows, rpcErr := e.skillRecordsForSession(id)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	for _, row := range rows {
		if row.name == "project-only" {
			t.Fatalf("project skill leaked without scoped filesystem provider: %#v", rows)
		}
	}
}

func TestDeliverablesClientPluginRegistersHostPromptSection(t *testing.T) {
	e := newIntegrationEngine(t)
	s := &Session{Header: SessionHeader{CWD: "/work"}}
	without, err := e.systemPromptForSession(s, ModelSelection{Model: "m"}, agentRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(without, deliverableFilePrompt) {
		t.Fatal("deliverables guidance was active without the composed client plugin")
	}
	e.cfg.ClientPlugins = []string{"@deepseek-ai/dsh-client-ui-deliverables"}
	with, err := e.systemPromptForSession(s, ModelSelection{Model: "m"}, agentRuntime{})
	if err != nil || !strings.Contains(with, deliverableFilePrompt) {
		t.Fatalf("deliverables system prompt = %q, %v", with, err)
	}
}

func TestShippedPresetFiltersModelToolCatalog(t *testing.T) {
	e := newIntegrationEngine(t)
	if err := e.RegisterTool(Tool{
		Schema:  ToolSchema{Name: "embedding_tool", Parameters: objectSchema(nil)},
		Execute: func(context.Context, ToolCall) (ToolResult, error) { return textToolResult("ok"), nil },
	}); err != nil {
		t.Fatal(err)
	}
	toolNames := func(preset string) map[string]bool {
		id, err := e.CreateSession(context.Background(), e.Config().Workspace, "tools-"+preset, preset)
		if err != nil {
			t.Fatal(err)
		}
		s, _ := e.getSession(id)
		tools, err := e.toolsForSession(s)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, tool := range tools {
			out[tool.Name] = true
		}
		return out
	}
	standard := toolNames("standard")
	for _, name := range []string{"bash", "read", "write", "edit", "glob", "grep", "embedding_tool"} {
		if !standard[name] {
			t.Fatalf("standard preset omitted %q: %#v", name, standard)
		}
	}
	if standard["str_replace_editor"] {
		t.Fatalf("standard preset retained opt-in str_replace_editor: %#v", standard)
	}
	minimal := toolNames("minimal")
	if !minimal["bash"] || !minimal["embedding_tool"] || !minimal["str_replace_editor"] {
		t.Fatalf("minimal preset tools = %#v", minimal)
	}
	for _, name := range []string{"read", "write", "edit", "glob", "grep"} {
		if minimal[name] {
			t.Fatalf("minimal preset exposed %q: %#v", name, minimal)
		}
	}
}

func TestPresetToolWebNamesFollowIndependentEnablement(t *testing.T) {
	tests := []struct {
		config string
		want   string
	}{
		{config: "{}", want: "web_search,web_fetch"},
		{config: "search: false\nfetch: true", want: "web_fetch"},
		{config: "search: true\nfetch: false", want: "web_search"},
		{config: "search: false\nfetch: false", want: ""},
	}
	for _, test := range tests {
		var document yaml.Node
		if err := yaml.Unmarshal([]byte(test.config), &document); err != nil {
			t.Fatal(err)
		}
		config := document.Content[0]
		if got := strings.Join(presetToolNames("@deepseek-ai/dsh-tool-web", config), ","); got != test.want {
			t.Fatalf("tool-web config %q names = %q, want %q", test.config, got, test.want)
		}
	}
}
