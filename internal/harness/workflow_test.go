package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

type workflowTestProvider struct {
	mu          sync.Mutex
	ralphRounds int
	prompts     []string
}

type ralphProviderFixture struct {
	name       string
	caps       SubagentCapabilities
	inherits   bool
	result     SubagentResult
	request    SubagentStartRequest
	starts     int
	disposeCnt int
}

type concurrentWorkflowProvider struct {
	name      string
	started   chan struct{}
	release   chan struct{}
	mu        sync.Mutex
	active    int
	maxActive int
}

type workflowRecordingFailStore struct {
	SessionStore
	mu   sync.Mutex
	fail bool
}

func (s *workflowRecordingFailStore) Append(ctx context.Context, id string, events []Event) error {
	s.mu.Lock()
	fail := s.fail
	s.mu.Unlock()
	if fail {
		for _, event := range events {
			if strings.HasPrefix(event.Type, "tool-workflow/") {
				return errors.New("forced workflow recording failure")
			}
		}
	}
	return s.SessionStore.Append(ctx, id, events)
}

func (p *concurrentWorkflowProvider) Name() string                     { return p.name }
func (*concurrentWorkflowProvider) Capabilities() SubagentCapabilities { return SubagentCapabilities{} }
func (*concurrentWorkflowProvider) InheritsParentContext() bool        { return false }
func (p *concurrentWorkflowProvider) Start(_ context.Context, _ SubagentStartRequest) (*SubagentRun, error) {
	p.mu.Lock()
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	p.mu.Unlock()
	p.started <- struct{}{}
	run := newSubagentRun(newID("workflow-concurrent"), nil, nil)
	go func() {
		<-p.release
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
		run.settle(SubagentResult{StopReason: SubagentCompleted, Output: []ContentBlock{{Type: "text", Text: "done"}}})
	}()
	return run, nil
}

func (p *ralphProviderFixture) Name() string                       { return p.name }
func (p *ralphProviderFixture) Capabilities() SubagentCapabilities { return p.caps }
func (p *ralphProviderFixture) InheritsParentContext() bool        { return p.inherits }
func (p *ralphProviderFixture) Start(_ context.Context, request SubagentStartRequest) (*SubagentRun, error) {
	p.request = request
	p.starts++
	run := newSubagentRun("ralph-fixture", nil, func() error { p.disposeCnt++; return nil })
	run.settle(p.result)
	return run, nil
}

func completeRalphFixture(name string) *ralphProviderFixture {
	return &ralphProviderFixture{
		name: name, caps: SubagentCapabilities{OutputSchema: true},
		result: SubagentResult{StopReason: SubagentCompleted, Structured: map[string]any{
			"status": "complete", "summary": "done", "evidence": []any{"fixture"}, "nextSteps": []any{}, "blocker": "",
		}},
	}
}

func (p *workflowTestProvider) ID() string   { return "workflow-test" }
func (p *workflowTestProvider) Name() string { return "Workflow Test" }
func (p *workflowTestProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "workflow-test", Name: "Workflow Test"}}, nil
}
func (p *workflowTestProvider) Complete(_ context.Context, request ChatRequest, delta func(Delta) error) (Completion, error) {
	prompt := ""
	for index := len(request.Messages) - 1; index >= 0; index-- {
		kind, _ := request.Messages[index].Source["kind"].(string)
		if kind != "plugin" {
			prompt = request.Messages[index].Content
			break
		}
	}
	p.mu.Lock()
	p.prompts = append(p.prompts, prompt)
	if strings.Contains(prompt, "Ralph round:") {
		p.ralphRounds++
		round := p.ralphRounds
		p.mu.Unlock()
		text := `{"status":"continue","summary":"round one","evidence":["checked workspace"],"nextSteps":["finish work"],"blocker":""}`
		if round == 2 {
			text = `{"status":"complete","summary":"done","evidence":["tests passed"],"nextSteps":[],"blocker":""}`
		}
		for _, tool := range request.Tools {
			if tool.Name == structuredOutputToolName {
				return Completion{Finish: "tool_calls", ToolCalls: []ToolCall{{ID: "ralph-report", Name: structuredOutputToolName, Arguments: json.RawMessage(text)}}}, nil
			}
		}
		return Completion{Text: text, Finish: "stop"}, nil
	}
	p.mu.Unlock()
	for _, tool := range request.Tools {
		if tool.Name == structuredOutputToolName {
			return Completion{Finish: "tool_calls", ToolCalls: []ToolCall{{ID: "workflow-structured", Name: structuredOutputToolName, Arguments: json.RawMessage(`{"answer":"structured"}`)}}}, nil
		}
	}
	text := "child:" + strings.TrimSpace(strings.Split(prompt, "\n\n")[0])
	if strings.Contains(prompt, "matching this schema exactly") {
		text = `{"answer":"structured"}`
	}
	if err := delta(Delta{Text: text}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: text, Finish: "stop"}, nil
}

func newWorkflowEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	provider := &workflowTestProvider{}
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = provider.ID()
	cfg.Model = provider.ID()
	cfg.Persist = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(context.Background(), cfg.Workspace, "", "standard")
	if err != nil {
		t.Fatal(err)
	}
	return e, id
}

func executeWorkflowTestTool(t *testing.T, e *Engine, name, sessionID, arguments string) ToolResult {
	t.Helper()
	e.mu.RLock()
	tool := e.tools[name]
	e.mu.RUnlock()
	result, err := executeTool(context.Background(), tool, ToolCall{ID: "call-1", Name: name, SessionID: sessionID, Workspace: e.Config().Workspace, Arguments: json.RawMessage(arguments)})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestWorkflowRunsAsyncAgentsAndRecordsDurableLifecycle(t *testing.T) {
	e, id := newWorkflowEngine(t)
	arguments := `{
		"meta":{"name":"snapshot-flow","description":"two children"},
		"script":"phase('Run'); const replies = await parallel([() => agent('one'), () => agent('two')]); return { replies };"
	}`
	result := executeWorkflowTestTool(t, e, "workflow", id, arguments)
	if len(result.Content) != 1 || !strings.Contains(result.Content[0].Text, `workflow "snapshot-flow" completed (2 agents)`) || !strings.Contains(result.Content[0].Text, `"child:one"`) || !strings.Contains(result.Content[0].Text, `"child:two"`) {
		t.Fatalf("workflow result = %#v", result)
	}
	session, _ := e.getSession(id)
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
	}
	if counts["tool-workflow/run-start"] != 1 || counts["tool-workflow/agent-start"] != 2 || counts["tool-workflow/agent-end"] != 2 || counts["tool-workflow/run-end"] != 1 {
		t.Fatalf("workflow lifecycle counts = %#v", counts)
	}
}

func TestWorkflowRecordingFailureDoesNotAffectExecution(t *testing.T) {
	backend, err := NewJSONLSessionStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	store := &workflowRecordingFailStore{SessionStore: backend}
	provider := &workflowTestProvider{}
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir, cfg.Workspace = t.TempDir(), t.TempDir()
	cfg.Provider, cfg.Model, cfg.Persist, cfg.SessionStore = provider.ID(), provider.ID(), true, store
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), cfg.Workspace, "record-failure", "standard")
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.fail = true
	store.mu.Unlock()
	result := executeWorkflowTestTool(t, e, "workflow", id, `{
		"meta":{"name":"record-failure","description":"recording is optional"},
		"script":"return await agent('one');"
	}`)
	if len(result.Content) != 1 || !strings.Contains(result.Content[0].Text, "child:one") {
		t.Fatalf("workflow result after recording failure = %#v", result)
	}
	session, _ := e.getSession(id)
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if strings.HasPrefix(event.Type, "tool-workflow/") {
			t.Fatalf("disabled recorder appended event %#v", event)
		}
	}
}

func TestWorkflowStructuredAgentOutput(t *testing.T) {
	e, id := newWorkflowEngine(t)
	arguments := `{
		"meta":{"name":"structured","description":"structured child"},
		"script":"const reply = await agent('return data', {schema:{type:'object',properties:{answer:{type:'string'}},required:['answer'],additionalProperties:false}}); return reply;"
	}`
	result := executeWorkflowTestTool(t, e, "workflow", id, arguments)
	if !strings.Contains(result.Content[0].Text, `"answer": "structured"`) {
		t.Fatalf("workflow structured result = %q", result.Content[0].Text)
	}
}

func TestWorkflowUsesPinnedToolNameProviderAndLimits(t *testing.T) {
	e, id := newWorkflowEngine(t)
	provider := completeRalphFixture("custom-flow")
	provider.caps.AgentOptions = true
	provider.result = SubagentResult{StopReason: SubagentCompleted, Output: []ContentBlock{{Type: "text", Text: "custom child"}}}
	if err := e.RegisterSubagentProvider(provider); err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession(id)
	session.mu.Lock()
	session.presetRuntime = &presetRuntimeGeneration{runtime: agentRuntime{
		toolNames: map[string]bool{"orchestrate": true}, toolPresentation: "native",
		workflowToolName: "orchestrate", workflowMaxResultChars: 24, workflowProvider: provider.Name(),
		workflowMaxConcurrentAgents: 1, workflowMaxAgents: 1, workflowMaxItems: 1,
		workflowSyncTimeout: time.Second, workflowDisposeGrace: time.Second,
	}}
	session.mu.Unlock()
	schemas, err := e.toolsForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, schema := range schemas {
		seen[schema.Name] = true
	}
	if !seen["orchestrate"] || seen["workflow"] {
		t.Fatalf("workflow aliases = %#v", seen)
	}
	tool, ok := e.toolForSession(session, "orchestrate")
	if !ok || tool.Schema.Name != "orchestrate" {
		t.Fatalf("configured workflow tool = %#v, %v", tool.Schema, ok)
	}
	result, err := executeTool(t.Context(), tool, ToolCall{ID: "flow", Name: "orchestrate", SessionID: id, Arguments: json.RawMessage(`{
		"meta":{"name":"configured","description":"configured workflow"},
		"script":"const value = await agent('one', {provider:'model-provider', model:'model-id'}); return {value, padding:'abcdefghijklmnopqrstuvwxyz'};"
	}`)})
	if err != nil {
		t.Fatal(err)
	}
	if provider.starts != 1 || provider.disposeCnt != 1 || provider.request.AgentOptions == nil || provider.request.AgentOptions.Provider != "model-provider" || provider.request.AgentOptions.Model != "model-id" || len(result.Content) != 1 || !strings.Contains(result.Content[0].Text, "[truncated:") {
		t.Fatalf("configured workflow result = %#v provider=%#v", result, provider)
	}
	_, err = executeTool(t.Context(), tool, ToolCall{ID: "items", Name: "orchestrate", SessionID: id, Arguments: json.RawMessage(`{
		"meta":{"name":"items","description":"too many items"},
		"script":"return await parallel([() => 1, () => 2]);"
	}`)})
	if err == nil || !strings.Contains(err.Error(), "over the per-call cap (1)") {
		t.Fatalf("workflow item cap error = %v", err)
	}
	_, err = executeTool(t.Context(), tool, ToolCall{ID: "agents", Name: "orchestrate", SessionID: id, Arguments: json.RawMessage(`{
		"meta":{"name":"agents","description":"too many agents"},
		"script":"await agent('one'); return await agent('two');"
	}`)})
	if err == nil || !strings.Contains(err.Error(), "total agent cap (1)") {
		t.Fatalf("workflow agent cap error = %v", err)
	}
}

func TestWorkflowHonorsConfiguredConcurrencyAndSyncTimeout(t *testing.T) {
	e, id := newWorkflowEngine(t)
	provider := &concurrentWorkflowProvider{name: "bounded-flow", started: make(chan struct{}, 3), release: make(chan struct{})}
	if err := e.RegisterSubagentProvider(provider); err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession(id)
	session.mu.Lock()
	session.presetRuntime = &presetRuntimeGeneration{runtime: agentRuntime{
		toolNames: map[string]bool{"workflow": true}, toolPresentation: "native",
		workflowToolName: "workflow", workflowMaxResultChars: 500,
		workflowProvider: provider.Name(), workflowMaxConcurrentAgents: 1,
		workflowMaxAgents: 3, workflowMaxItems: 3, workflowSyncTimeout: 20 * time.Millisecond,
		workflowDisposeGrace: time.Second,
	}}
	session.mu.Unlock()
	e.mu.RLock()
	tool := e.tools["workflow"]
	e.mu.RUnlock()
	done := make(chan error, 1)
	go func() {
		_, err := executeTool(t.Context(), tool, ToolCall{ID: "bounded", Name: "workflow", SessionID: id, Arguments: json.RawMessage(`{
			"meta":{"name":"bounded","description":"bounded concurrency"},
			"script":"return await parallel([() => agent('one'), () => agent('two'), () => agent('three')]);"
		}`)})
		done <- err
	}()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("first workflow child did not start")
	}
	select {
	case <-provider.started:
		t.Fatal("second child exceeded configured workflow concurrency")
	case <-time.After(50 * time.Millisecond):
	}
	close(provider.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	maxActive := provider.maxActive
	provider.mu.Unlock()
	if maxActive != 1 {
		t.Fatalf("workflow max active = %d, want 1", maxActive)
	}
	_, err := executeTool(t.Context(), tool, ToolCall{ID: "timeout", Name: "workflow", SessionID: id, Arguments: json.RawMessage(`{
		"meta":{"name":"timeout","description":"sync timeout"},
		"script":"while (true) {}"
	}`)})
	if err == nil || !strings.Contains(err.Error(), "synchronous execution timed out") {
		t.Fatalf("workflow sync timeout error = %v", err)
	}
}

func TestWorkflowResultCapsUseUTF16WithoutSplittingUTF8(t *testing.T) {
	text := "中😀文"
	clipped, omitted, truncated := truncateUTF16Units(text, 3)
	if !truncated || clipped != "中😀" || omitted != 1 || !utf8.ValidString(clipped) {
		t.Fatalf("UTF-16 truncation = %q omitted=%d truncated=%v", clipped, omitted, truncated)
	}
	if got := boundText("中😀文", 13); !utf8.ValidString(got) || jsonUTF16Length([]byte(got)) > 13 {
		t.Fatalf("bounded UTF-16 text = %q (%d units)", got, jsonUTF16Length([]byte(got)))
	}
}

func TestRalphUsesFreshStructuredRounds(t *testing.T) {
	e, id := newWorkflowEngine(t)
	result := executeWorkflowTestTool(t, e, "ralph", id, `{"objective":"finish the task","maxRounds":3}`)
	if len(result.Content) != 1 || !strings.Contains(result.Content[0].Text, "reported completion after 2 rounds") || !strings.Contains(result.Content[0].Text, "tests passed") {
		t.Fatalf("ralph result = %#v", result)
	}
	e.mu.RLock()
	children := 0
	for _, session := range e.sessions {
		session.mu.Lock()
		if session.Header.ParentSession == id && session.Header.Origin == "subagent" {
			children++
		}
		session.mu.Unlock()
	}
	e.mu.RUnlock()
	if children != 2 {
		t.Fatalf("Ralph children = %d, want 2 fresh sessions", children)
	}
}

func TestRalphUsesConfiguredFreshProviderAndCaps(t *testing.T) {
	e, id := newWorkflowEngine(t)
	provider := completeRalphFixture("custom-fresh")
	if err := e.RegisterSubagentProvider(provider); err != nil {
		t.Fatal(err)
	}
	result, err := e.runRalph(t.Context(), id, provider.Name(), "finish the task", 1, 99, 999)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "complete" || result.RoundsStarted != 1 || result.Report.Summary != "done" {
		t.Fatalf("Ralph result = %#v", result)
	}
	if provider.starts != 1 || provider.disposeCnt != 1 || provider.request.ParentSessionID != id || provider.request.Label != "Ralph round 1" || provider.request.OutputSchema == nil || len(provider.request.Prompt) != 1 {
		t.Fatalf("provider request = %#v starts=%d dispose=%d", provider.request, provider.starts, provider.disposeCnt)
	}
}

func TestRalphRejectsProviderWithoutFreshStructuredCapabilities(t *testing.T) {
	e, id := newWorkflowEngine(t)
	provider := completeRalphFixture("no-schema")
	provider.caps.OutputSchema = false
	if err := e.RegisterSubagentProvider(provider); err != nil {
		t.Fatal(err)
	}
	if _, err := e.runRalph(t.Context(), id, provider.Name(), "finish", 1, 99, 999); err == nil || !strings.Contains(err.Error(), "does not support structured output") {
		t.Fatalf("missing structured capability error = %v", err)
	}
	inheriting := completeRalphFixture("inherits")
	inheriting.inherits = true
	if err := e.RegisterSubagentProvider(inheriting); err != nil {
		t.Fatal(err)
	}
	if _, err := e.runRalph(t.Context(), id, inheriting.Name(), "finish", 1, 99, 999); err == nil || !strings.Contains(err.Error(), "inherits parent context") {
		t.Fatalf("inheriting provider error = %v", err)
	}
	if provider.starts != 0 || inheriting.starts != 0 {
		t.Fatalf("rejected providers were started: no-schema=%d inherits=%d", provider.starts, inheriting.starts)
	}
}

func TestRalphBuiltinUsesPinnedProviderCeilingAndResultCap(t *testing.T) {
	e, id := newWorkflowEngine(t)
	provider := completeRalphFixture("pinned-fresh")
	provider.result.Structured.(map[string]any)["summary"] = strings.Repeat("x", 80)
	if err := e.RegisterSubagentProvider(provider); err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession(id)
	session.mu.Lock()
	session.presetRuntime = &presetRuntimeGeneration{runtime: agentRuntime{
		toolNames: map[string]bool{"ralph": true}, ralphSubagentProvider: provider.Name(),
		ralphMaxRounds: 1, ralphMaxHandoffChars: 256, ralphMaxResultChars: 48,
	}}
	session.mu.Unlock()
	e.mu.RLock()
	tool := e.tools["ralph"]
	e.mu.RUnlock()
	_, err := executeTool(t.Context(), tool, ToolCall{ID: "too-many", Name: "ralph", SessionID: id, Arguments: json.RawMessage(`{"objective":"finish","maxRounds":2}`)})
	if err == nil || !strings.Contains(err.Error(), "between 1 and 1") {
		t.Fatalf("ceiling error = %v", err)
	}
	result, err := executeTool(t.Context(), tool, ToolCall{ID: "run", Name: "ralph", SessionID: id, Arguments: json.RawMessage(`{"objective":"finish"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if provider.starts != 1 || len(result.Content) != 1 || jsonUTF16Length([]byte(result.Content[0].Text)) > 48 || !strings.Contains(result.Content[0].Text, "[truncated]") {
		t.Fatalf("pinned Ralph result = %#v starts=%d", result, provider.starts)
	}
}

func TestRalphDefensivelyRejectsOversizedAndMalformedStructuredReports(t *testing.T) {
	e, id := newWorkflowEngine(t)
	oversized := completeRalphFixture("oversized")
	oversized.result.Structured.(map[string]any)["summary"] = strings.Repeat("x", 80)
	if err := e.RegisterSubagentProvider(oversized); err != nil {
		t.Fatal(err)
	}
	if _, err := e.runRalph(t.Context(), id, oversized.Name(), "finish", 1, 40, 999); err == nil || !strings.Contains(err.Error(), "exceeds maxHandoffChars") {
		t.Fatalf("oversized handoff error = %v", err)
	}
	malformed := completeRalphFixture("malformed")
	malformed.result.Structured.(map[string]any)["extra"] = true
	if err := e.RegisterSubagentProvider(malformed); err != nil {
		t.Fatal(err)
	}
	if _, err := e.runRalph(t.Context(), id, malformed.Name(), "finish", 1, 999, 999); err == nil || !strings.Contains(err.Error(), "malformed round report") {
		t.Fatalf("malformed report error = %v", err)
	}
}

func TestRunCodeExecutesTypeScriptAgainstGoTools(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "echo_json", Description: "Echo JSON", Parameters: objectSchema(map[string]any{"value": map[string]any{"type": "string"}}, "value")},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var input struct {
				Value string `json:"value"`
			}
			if err := decodeToolArguments(call, &input); err != nil {
				return ToolResult{}, err
			}
			return jsonToolResult(map[string]any{"value": input.Value})
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession(id)
	schemas, err := e.toolsForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	if len(schemas) != 1 || schemas[0].Name != "run_code" {
		t.Fatalf("code preset schemas = %#v", schemas)
	}
	arguments := `{
		"description":"Echo typed value through Go tool",
		"code":"const input: string = 'typed'; const out = await tools.echo_json({value: input}); console.log('captured output'); return out.value + '!';"
	}`
	result := executeWorkflowTestTool(t, e, "run_code", id, arguments)
	if len(result.Content) == 0 || result.Content[0].Text != "captured output\ntyped!" {
		t.Fatalf("run_code result = %#v", result)
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	var starts, settles int
	for _, event := range events {
		switch event.Type {
		case "tool/code-dispatch-start":
			starts++
		case "tool/code-dispatch":
			settles++
		}
	}
	if starts != 1 || settles != 1 {
		t.Fatalf("code dispatch events: starts=%d settles=%d", starts, settles)
	}
}

func TestRunCodeExecutesBuiltinBashResultShape(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bwrap is unavailable")
	}
	e, _ := newWorkflowEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	arguments := `{
		"description":"Run two echo commands and join outputs",
		"code":"const out1 = await tools.bash({command:'echo CODE_ONE',description:'Print CODE_ONE'}); const out2 = await tools.bash({command:'echo CODE_TWO',description:'Print CODE_TWO'}); return out1.stdout.text.trim() + '+' + out2.stdout.text.trim();"
	}`
	result := executeWorkflowTestTool(t, e, "run_code", id, arguments)
	if len(result.Content) == 0 || result.Content[0].Text != "CODE_ONE+CODE_TWO" {
		t.Fatalf("run_code bash result = %#v", result)
	}
}

func TestRunCodeExecutesBuiltinFileToolsWithCanonicalValues(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession(id)
	runtimeConfig, err := e.runtimeForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	sdk := e.codeModePrompt(session, runtimeConfig)
	for _, want := range []string{
		`Promise<{ "lines": ({ "number": number; "text": string })[]; "offset": number; "path": string; "totalLines": number }>`,
		`Promise<{ "after": string; "before": string | null; "operation": "create" | "update"; "path": string }>`,
		`Promise<{ "after": string; "before": string; "path": string }>`,
		`Promise<{ "paths": (string)[]; "root": string }>`,
		`Promise<{ "matches": ({ "line": string; "lineNumber": number; "path": string })[] }>`,
	} {
		if !strings.Contains(sdk, want) {
			t.Fatalf("Code Mode SDK missing %q:\n%s", want, sdk)
		}
	}
	path := filepath.Join(e.Config().Workspace, "code-tools.txt")
	if err := os.WriteFile(path, []byte("alpha\nbeta\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	arguments := `{
		"description":"Exercise canonical file tool results",
		"code":"const first = await tools.read({file_path:'code-tools.txt'}); const edited = await tools.edit({file_path:'code-tools.txt',old_string:'beta',new_string:'gamma'}); const written = await tools.write({file_path:'created.txt',content:'needle'}); const files = await tools.glob({pattern:'*.txt'}); const hits = await tools.grep({pattern:'needle',include:'*.txt'}); return {line:first.lines[1].text, edit:[edited.before,edited.after], write:[written.operation,written.before,written.after], paths:files.paths, match:hits.matches[0]};"
	}`
	result := executeWorkflowTestTool(t, e, "run_code", id, arguments)
	if len(result.Content) == 0 {
		t.Fatalf("run_code file result = %#v", result)
	}
	var value struct {
		Line  string   `json:"line"`
		Edit  []string `json:"edit"`
		Write []any    `json:"write"`
		Paths []string `json:"paths"`
		Match struct {
			Path       string `json:"path"`
			LineNumber int    `json:"lineNumber"`
			Line       string `json:"line"`
		} `json:"match"`
	}
	if err := json.Unmarshal([]byte(result.Content[0].Text), &value); err != nil {
		t.Fatal(err)
	}
	if value.Line != "beta" || len(value.Edit) != 2 || value.Edit[0] != "alpha\nbeta\n" || value.Edit[1] != "alpha\ngamma\n" {
		t.Fatalf("read/edit values = %#v", value)
	}
	if len(value.Write) != 3 || value.Write[0] != "create" || value.Write[1] != nil || value.Write[2] != "needle" {
		t.Fatalf("write value = %#v", value.Write)
	}
	if !containsPath(value.Paths, "code-tools.txt") || !containsPath(value.Paths, "created.txt") || value.Match.Path != "created.txt" || value.Match.LineNumber != 1 || value.Match.Line != "needle" {
		t.Fatalf("search values = %#v", value)
	}
}

func containsPath(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestRunCodePTCOmitsWorkflowTool(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	arguments := `{
		"description":"Run one nested workflow child",
		"code":"const out = await tools.workflow({meta:{name:'nested',description:'nested workflow'},script:\"const reply = await agent('one'); return {reply};\"}); return out.result.reply;"
	}`
	result := executeWorkflowTestTool(t, e, "run_code", id, arguments)
	if !result.IsError || len(result.Content) == 0 || !strings.Contains(result.Content[0].Text, "Object has no member 'workflow'") {
		t.Fatalf("PTC workflow omission result = %#v", result)
	}
	session, _ := e.getSession(id)
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, event := range session.Events {
		if strings.HasPrefix(event.Type, "tool-workflow/") {
			t.Fatalf("nested Code Mode workflow leaked top-level panel event: %#v", event)
		}
	}
}

func TestRunCodeToolFailureIsCatchable(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "always_fail", Parameters: objectSchema(map[string]any{})},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{}, context.DeadlineExceeded
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	arguments := `{
		"description":"Catch one failed Go tool call",
		"code":"try { await tools.always_fail({}); } catch (error) { return error.name + ':' + error.toolName; }"
	}`
	result := executeWorkflowTestTool(t, e, "run_code", id, arguments)
	if len(result.Content) == 0 || result.Content[0].Text != "ToolCallError:always_fail" {
		t.Fatalf("caught failure = %#v", result)
	}
}

func TestRunCodeProgramFailureIsStructured(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	result := executeWorkflowTestTool(t, e, "run_code", id, `{"description":"Fail after writing one log","code":"console.log('got this far'); throw new Error('program exploded');"}`)
	if !result.IsError || result.Error == nil || result.Error.Name != "CodeRunFailedError" || result.Error.Code != "CODE_RUN_FAILED" {
		t.Fatalf("structured failure = %#v", result)
	}
	if len(result.Content) != 1 || !strings.Contains(result.Content[0].Text, "code run failed (exception)") || !strings.Contains(result.Content[0].Text, "program exploded") || !strings.Contains(result.Content[0].Text, "got this far") {
		t.Fatalf("failure content = %#v", result.Content)
	}
}

func TestRunCodeCommitsNestedFinalizersAndContextsInSubmissionOrder(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	started := make(chan string, 2)
	completed := make(chan string, 2)
	releases := map[string]chan struct{}{"parallel_a": make(chan struct{}), "parallel_b": make(chan struct{})}
	var orderMu sync.Mutex
	finalized := []string{}
	for _, name := range []string{"parallel_a", "parallel_b"} {
		name := name
		if err := e.RegisterTool(Tool{
			Schema: ToolSchema{
				Name: name, Parameters: objectSchema(map[string]any{}),
				Output: objectSchema(map[string]any{"name": map[string]any{"type": "string"}}, "name"),
			},
			IsConcurrencySafe: alwaysConcurrencySafe,
			ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
				started <- name
				select {
				case <-releases[name]:
				case <-exec.Done():
					return ToolResult{}, exec.Err()
				}
				exec.DeferContext(ToolContext{Content: []ContentBlock{{Type: "text", Text: "context:" + name}}, Source: map[string]any{"kind": "plugin", "plugin": "order"}})
				completed <- name
				return ToolResult{Value: map[string]any{"name": name}}, nil
			},
			RenderOutput: func(_ ToolCall, value any) ([]ContentBlock, error) {
				return []ContentBlock{{Type: "text", Text: value.(map[string]any)["name"].(string)}}, nil
			},
			FinalizeContent: func(call ToolCall, result ToolResult) ([]ContentBlock, error) {
				orderMu.Lock()
				finalized = append(finalized, call.Name)
				orderMu.Unlock()
				return result.Content, nil
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	runCode := e.tools["run_code"]
	e.mu.RUnlock()
	type outcome struct {
		result ToolResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := executeTool(context.Background(), runCode, ToolCall{
			ID: "call-1", Name: "run_code", SessionID: id, Workspace: e.Config().Workspace,
			Arguments: json.RawMessage(`{"description":"Run two ordered parallel calls","code":"const out = await Promise.all([tools.parallel_a({}), tools.parallel_b({})]); return out.map(x => x.name);"}`),
		})
		done <- outcome{result: result, err: err}
	}()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case name := <-started:
			seen[name] = true
		case <-time.After(2 * time.Second):
			t.Fatal("parallel nested calls did not both start")
		}
	}
	close(releases["parallel_b"])
	select {
	case name := <-completed:
		if name != "parallel_b" {
			t.Fatalf("first completed = %q", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parallel_b did not complete")
	}
	close(releases["parallel_a"])
	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	orderMu.Lock()
	gotFinalized := strings.Join(finalized, ",")
	orderMu.Unlock()
	if gotFinalized != "parallel_a,parallel_b" {
		t.Fatalf("finalizer order = %q", gotFinalized)
	}
	if len(got.result.AdditionalContexts) != 2 || blockText(got.result.AdditionalContexts[0].Content) != "context:parallel_a" || blockText(got.result.AdditionalContexts[1].Content) != "context:parallel_b" {
		t.Fatalf("context order = %#v", got.result.AdditionalContexts)
	}
}

func TestRunCodeBoundsNestedParallelBodies(t *testing.T) {
	provider := &workflowTestProvider{}
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = provider.ID()
	cfg.Model = provider.ID()
	cfg.Persist = false
	cfg.MaxParallelToolCalls = 2
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var mu sync.Mutex
	active, maxActive := 0, 0
	if err := e.RegisterTool(Tool{
		Schema:            ToolSchema{Name: "bounded_parallel", Parameters: objectSchema(map[string]any{})},
		IsConcurrencySafe: alwaysConcurrencySafe,
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			started <- struct{}{}
			<-release
			mu.Lock()
			active--
			mu.Unlock()
			return textToolResult("done"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), cfg.Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	runCode := e.tools["run_code"]
	e.mu.RUnlock()
	done := make(chan error, 1)
	go func() {
		_, err := executeTool(context.Background(), runCode, ToolCall{
			ID: "call-1", Name: "run_code", SessionID: id, Workspace: cfg.Workspace,
			Arguments: json.RawMessage(`{"description":"Run three bounded parallel calls","code":"await Promise.all([tools.bounded_parallel({}), tools.bounded_parallel({}), tools.bounded_parallel({})]); return 'done';"}`),
		})
		done <- err
	}()
	for index := 0; index < 2; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("bounded nested call did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("third nested body exceeded maxParallelToolCalls")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bounded run_code did not settle")
	}
	mu.Lock()
	gotMax := maxActive
	mu.Unlock()
	if gotMax != 2 {
		t.Fatalf("max active nested bodies = %d, want 2", gotMax)
	}
}

func TestRunCodeCancellationDrainsStartedAndAbandonsQueued(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	started := make(chan string, 2)
	aborted := make(chan struct{}, 1)
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{
			Name: "nested_cancel", Parameters: objectSchema(map[string]any{"id": map[string]any{"type": "string"}}, "id"),
		},
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			var input struct {
				ID string `json:"id"`
			}
			if err := decodeToolArguments(exec.Call, &input); err != nil {
				return ToolResult{}, err
			}
			started <- input.ID
			<-exec.Done()
			aborted <- struct{}{}
			return textToolResult(input.ID), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	runCode := e.tools["run_code"]
	e.mu.RUnlock()
	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		result ToolResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := executeTool(ctx, runCode, ToolCall{
			ID: "call-1", Name: "run_code", SessionID: id, Workspace: e.Config().Workspace,
			Arguments: json.RawMessage(`{"description":"Cancel one queued nested call","code":"await Promise.all([tools.nested_cancel({id:'first'}), tools.nested_cancel({id:'second'})]);"}`),
		})
		done <- outcome{result: result, err: err}
	}()
	select {
	case name := <-started:
		if name != "first" {
			t.Fatalf("first nested start = %q", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first nested call did not start")
	}
	cancel()
	select {
	case <-aborted:
	case <-time.After(2 * time.Second):
		t.Fatal("started nested call did not observe cancellation")
	}
	select {
	case name := <-started:
		t.Fatalf("queued nested call started after cancellation: %s", name)
	case <-time.After(50 * time.Millisecond):
	}
	var got outcome
	select {
	case got = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled run_code did not settle")
	}
	if got.err != nil || !got.result.IsError || got.result.Error == nil || got.result.Error.Code != "CODE_RUN_FAILED" {
		t.Fatalf("cancelled run_code = %#v, %v", got.result, got.err)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	starts, settles := 0, 0
	for _, event := range session.Events {
		switch event.Type {
		case "tool/code-dispatch-start":
			starts++
		case "tool/code-dispatch":
			settles++
		}
	}
	if starts != 1 || settles != 1 {
		t.Fatalf("cancelled nested events starts=%d settles=%d", starts, settles)
	}
}

func TestRunCodeForwardsOnlySuccessfulNestedConclusion(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "nested_conclude", Parameters: objectSchema(map[string]any{})},
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			exec.ConcludeTurn()
			return textToolResult("done"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{
			Name: "nested_invalid_conclude", Parameters: objectSchema(map[string]any{}),
			Output: objectSchema(map[string]any{"value": map[string]any{"type": "string"}}, "value"),
		},
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			exec.ConcludeTurn()
			return ToolResult{Value: map[string]any{"value": 42}}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	success := executeWorkflowTestTool(t, e, "run_code", id, `{"description":"Forward nested terminal result","code":"await tools.nested_conclude({}); return 'ok';"}`)
	if !success.ConcludesTurn {
		t.Fatalf("successful nested conclusion = %#v", success)
	}
	recovered := executeWorkflowTestTool(t, e, "run_code", id, `{"description":"Recover failed nested terminal result","code":"await tools.nested_invalid_conclude({}).catch(() => undefined); return 'recovered';"}`)
	if recovered.ConcludesTurn {
		t.Fatalf("failed nested conclusion leaked = %#v", recovered)
	}
}

func TestRunCodeUsesNestedFinalizerFailurePipeline(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "nested_finalize_failure", Parameters: objectSchema(map[string]any{})},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return textToolResult("body"), nil
		},
		FinalizeContent: func(ToolCall, ToolResult) ([]ContentBlock, error) {
			return nil, errors.New("nested finalizer failed")
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	result := executeWorkflowTestTool(t, e, "run_code", id, `{"description":"Catch nested finalizer failure","code":"try { await tools.nested_finalize_failure({}); } catch (error) { return error.name + ':' + error.toolName; }"}`)
	if len(result.Content) == 0 || result.Content[0].Text != "ToolCallError:nested_finalize_failure" {
		t.Fatalf("nested finalizer failure = %#v", result)
	}
}

func TestRunCodeDefersNestedImageContent(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	attachment := &ImageAttachmentRef{AttachmentID: "sha256:" + strings.Repeat("a", 64), MediaType: "image/png", Bytes: 1, Width: 1, Height: 1}
	if err := e.RegisterTool(Tool{
		Schema: ToolSchema{Name: "nested_image", Parameters: objectSchema(map[string]any{})},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Content: []ContentBlock{{Type: "text", Text: "image"}, {Type: "image", Attachment: attachment}}, Value: "ok"}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "ptc")
	if err != nil {
		t.Fatal(err)
	}
	result := executeWorkflowTestTool(t, e, "run_code", id, `{"description":"Forward nested image context","code":"await tools.nested_image({}); return 'done';"}`)
	if len(result.Content) != 1 || result.Content[0].Type != "text" || len(result.AdditionalContexts) != 1 || !contentHasImage(result.AdditionalContexts[0].Content) {
		t.Fatalf("nested image forwarding = %#v", result)
	}
}
