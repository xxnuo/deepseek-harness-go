package harness

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
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

func TestCompletePresetPersonaSuppressesHarnessIdentity(t *testing.T) {
	e := newIntegrationEngine(t)
	s := &Session{Header: SessionHeader{CWD: "/work"}}
	got, err := e.systemPromptForSession(s, ModelSelection{Model: "m"}, agentRuntime{persona: "Only {{model}}.", completePersona: true})
	if err != nil || got != "Only m." {
		t.Fatalf("system prompt = %q, %v", got, err)
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
	minimal := toolNames("minimal")
	if !minimal["bash"] || !minimal["embedding_tool"] {
		t.Fatalf("minimal preset tools = %#v", minimal)
	}
	for _, name := range []string{"read", "write", "edit", "glob", "grep"} {
		if minimal[name] {
			t.Fatalf("minimal preset exposed %q: %#v", name, minimal)
		}
	}
}
