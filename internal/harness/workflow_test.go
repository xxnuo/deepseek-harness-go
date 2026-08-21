package harness

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type workflowTestProvider struct {
	mu          sync.Mutex
	ralphRounds int
	prompts     []string
}

func (p *workflowTestProvider) ID() string   { return "workflow-test" }
func (p *workflowTestProvider) Name() string { return "Workflow Test" }
func (p *workflowTestProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "workflow-test", Name: "Workflow Test"}}, nil
}
func (p *workflowTestProvider) Complete(_ context.Context, request ChatRequest, delta func(Delta) error) (Completion, error) {
	prompt := request.Messages[len(request.Messages)-1].Content
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
		return Completion{Text: text, Finish: "stop"}, nil
	}
	p.mu.Unlock()
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
	result, err := tool.Execute(context.Background(), ToolCall{ID: "call-1", Name: name, SessionID: sessionID, Workspace: e.Config().Workspace, Arguments: json.RawMessage(arguments)})
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
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "code")
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
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "code")
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
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "code")
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

func TestRunCodeReceivesWorkflowCanonicalValueWithoutNestedPanel(t *testing.T) {
	e, _ := newWorkflowEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "code")
	if err != nil {
		t.Fatal(err)
	}
	arguments := `{
		"description":"Run one nested workflow child",
		"code":"const out = await tools.workflow({meta:{name:'nested',description:'nested workflow'},script:\"const reply = await agent('one'); return {reply};\"}); return out.result.reply;"
	}`
	result := executeWorkflowTestTool(t, e, "run_code", id, arguments)
	if len(result.Content) == 0 || result.Content[0].Text != "child:one" {
		t.Fatalf("nested workflow result = %#v", result)
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
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "code")
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
