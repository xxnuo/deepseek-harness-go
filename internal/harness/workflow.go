package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"github.com/dop251/goja"
	"github.com/evanw/esbuild/pkg/api"
)

const (
	workflowDefaultSyncTimeout = 5 * time.Second
)

const workflowDescription = "Run a JavaScript workflow script that orchestrates subagents at scale. Use this for work that fans out across many independent pieces, where orchestration as a script is clearer than delegating turn by turn. The script is a plain JavaScript async-function body and must end with return <value>. Available globals are agent(prompt, opts?), pipeline(items, ...stages), parallel(thunks), phase(title), log(message), and args. agent options are label, phase, schema, provider, and model. No filesystem, network, timers, or Node.js APIs are exposed; agents do the work and the script only coordinates them."

const ralphDescription = "Run a foreground fresh-agent Ralph loop toward one immutable objective. Use only when the direct human explicitly asks for Ralph or fresh-agent iteration. Each round opens a new child with no parent conversation or prior child session; the shared workspace is long-term memory, and only a bounded structured report crosses rounds."

const runCodeDescription = "Execute a TypeScript program against the available tools. Takes two required arguments: code, the body of an async function (erasable syntax only; top-level await and return work), and description, a short summary of what the program does. Call tools as await tools.name(args). Only console.log output and the returned value are shown."

var workflowMetaStatement = regexp.MustCompile(`(?m)^\s*export\s+const\s+meta\b`)

type workflowMeta struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	WhenToUse   string          `json:"whenToUse,omitempty"`
	Phases      []workflowPhase `json:"phases,omitempty"`
}

type workflowPhase struct {
	Title    string `json:"title"`
	Detail   string `json:"detail,omitempty"`
	Provider string `json:"provider,omitempty"`
	Model    string `json:"model,omitempty"`
}

type workflowAgentOptions struct {
	Label    string
	Phase    string
	Provider string
	Model    string
	Schema   map[string]any
}

type workflowChildRequest struct {
	parentID         string
	runID            string
	seq              int
	label            string
	phase            string
	prompt           string
	subagentProvider string
	disposeGrace     time.Duration
	provider         string
	model            string
	schema           map[string]any
	recorder         *workflowRecordState
	runInfo          map[string]any
}

type workflowRecordState struct {
	mu      sync.Mutex
	enabled bool
}

func (state *workflowRecordState) append(e *Engine, session *Session, event string, data map[string]any) bool {
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.enabled {
		return false
	}
	if _, err := e.appendEvent(session, event, data); err != nil {
		state.enabled = false
		return false
	}
	return true
}

type jsTask func() error

func registerWorkflowTools(e *Engine) error {
	tools := make([]Tool, 0, 3)
	if e.hostPluginActive("@deepseek-ai/dsh-tool-workflow") {
		tools = append(tools, builtinWorkflowTool(e))
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tool-ralph") {
		tools = append(tools, builtinRalphTool(e))
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tools") {
		tools = append(tools, builtinRunCodeTool(e))
	}
	for _, tool := range tools {
		if err := e.RegisterTool(tool); err != nil {
			return err
		}
	}
	return nil
}

func builtinWorkflowTool(e *Engine) Tool {
	type input struct {
		Script *string         `json:"script"`
		Meta   json.RawMessage `json:"meta"`
		Args   json.RawMessage `json:"args"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "workflow", Description: workflowDescription,
			Parameters: objectSchema(map[string]any{
				"script": map[string]any{"type": "string", "description": "Plain JavaScript async-function body; end with return <json-value>."},
				"meta": map[string]any{"type": "object", "additionalProperties": true, "properties": map[string]any{
					"name":        map[string]any{"type": "string"},
					"description": map[string]any{"type": "string"},
					"whenToUse":   map[string]any{"type": "string"},
					"phases": map[string]any{"type": "array", "items": map[string]any{"type": "object", "additionalProperties": true, "properties": map[string]any{
						"title": map[string]any{"type": "string"}, "detail": map[string]any{"type": "string"},
						"provider": map[string]any{"type": "string"}, "model": map[string]any{"type": "string"},
					}, "required": []string{"title"}}},
				}, "required": []string{"name", "description"}},
				"args": map[string]any{"type": "object", "additionalProperties": true},
			}, "script", "meta"),
			Output: objectSchema(map[string]any{"runId": map[string]any{"type": "string"}, "agentsStarted": map[string]any{"type": "integer"}, "result": map[string]any{}}, "runId", "agentsStarted", "result"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if in.Script == nil {
				return ToolResult{}, errors.New("workflow: `script` is required")
			}
			if call.SessionID == "" {
				return ToolResult{}, errors.New("workflow tool requires a calling agent")
			}
			meta, err := parseWorkflowMeta(in.Meta)
			if err != nil {
				return ToolResult{}, err
			}
			args, hasArgs, err := decodeOptionalJSON(in.Args)
			if err != nil {
				return ToolResult{}, fmt.Errorf("workflow args: %w", err)
			}
			program, err := compileWorkflowProgram(*in.Script, meta.Name)
			if err != nil {
				return ToolResult{}, err
			}
			session, err := e.getSession(call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			runtimeConfig, err := e.runtimeForSession(session)
			if err != nil {
				return ToolResult{}, err
			}
			if e.GetSubagentProvider(runtimeConfig.workflowProvider) == nil {
				return ToolResult{}, fmt.Errorf("no subagent provider registered for %q", runtimeConfig.workflowProvider)
			}
			runID := newID("workflow")
			recorder := &workflowRecordState{enabled: call.ParentCallID == ""}
			recorder.append(e, session, "tool-workflow/run-start", map[string]any{"runId": runID, "name": meta.Name})
			runInfo := map[string]any{"id": runID, "meta": meta}
			e.emitDynamicCordisScopedContained("", "workflow/start", runInfo)
			value, agents, runErr := e.runWorkflowProgram(ctx, session, runID, runInfo, program, args, hasArgs, runtimeConfig, recorder)
			stopReason := "completed"
			if runErr != nil {
				stopReason = "error"
				if errors.Is(runErr, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
					stopReason = "cancelled"
				}
			}
			resultInfo := map[string]any{"stopReason": stopReason, "agentsStarted": agents}
			if runErr != nil {
				resultInfo["error"] = runErr.Error()
			}
			e.emitDynamicCordisScopedContained("", "workflow/end", runInfo, resultInfo)
			recorder.append(e, session, "tool-workflow/run-end", map[string]any{"runId": runID, "stopReason": stopReason})
			if runErr != nil {
				if stopReason == "cancelled" {
					return ToolResult{}, fmt.Errorf("workflow run was cancelled: %w", runErr)
				}
				return ToolResult{}, fmt.Errorf("workflow run failed: %w", runErr)
			}
			rendered, err := json.MarshalIndent(value, "", "  ")
			if err != nil {
				return ToolResult{}, err
			}
			text := string(rendered)
			if clipped, omitted, truncated := truncateUTF16Units(text, runtimeConfig.workflowMaxResultChars); truncated {
				text = fmt.Sprintf("%s\n… [truncated: %d more characters]", clipped, omitted)
			}
			plural := "agents"
			if agents == 1 {
				plural = "agent"
			}
			result := textToolResult(fmt.Sprintf("workflow %q completed (%d %s).\nReturn value:\n%s", meta.Name, agents, plural, text))
			result.Value = map[string]any{"runId": runID, "agentsStarted": agents, "result": value}
			return result, nil
		},
	}
}

func parseWorkflowMeta(raw json.RawMessage) (workflowMeta, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return workflowMeta{}, errors.New("workflow: `meta` is required")
	}
	var record map[string]json.RawMessage
	if err := json.Unmarshal(raw, &record); err != nil || record == nil {
		return workflowMeta{}, errors.New("invalid meta: meta must be an object")
	}
	known := map[string]bool{"name": true, "description": true, "whenToUse": true, "phases": true}
	for key := range record {
		if !known[key] {
			return workflowMeta{}, fmt.Errorf("invalid meta: meta.%s is not a recognized field", key)
		}
	}
	var meta workflowMeta
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&meta); err != nil {
		return workflowMeta{}, fmt.Errorf("invalid meta: %w", err)
	}
	if meta.Name == "" || meta.Description == "" {
		return workflowMeta{}, errors.New("invalid meta: name and description must be non-empty strings")
	}
	for index, phase := range meta.Phases {
		if phase.Title == "" {
			return workflowMeta{}, fmt.Errorf("invalid meta: phases[%d].title must be a non-empty string", index)
		}
	}
	return meta, nil
}

func decodeOptionalJSON(raw json.RawMessage) (any, bool, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, false, nil
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func compileWorkflowProgram(body, name string) (*goja.Program, error) {
	if workflowMetaStatement.MatchString(body) {
		return nil, errors.New("workflow meta rides the `meta` request field; remove the export const meta statement")
	}
	source := workflowPrelude + "\n(async () => {\n" + body + "\n})()"
	program, err := goja.Compile("workflow:"+name, source, false)
	if err != nil {
		return nil, fmt.Errorf("workflow script does not parse: %w", err)
	}
	return program, nil
}

const workflowPrelude = `
const parallel = async (thunks) => {
  if (!Array.isArray(thunks)) throw __workflowError('parallel() requires an array of zero-argument functions');
  __assertWorkflowItems(thunks.length, 'parallel()');
  for (let i = 0; i < thunks.length; i++) if (typeof thunks[i] !== 'function') throw __workflowError('parallel() item ' + i + ' is not a function');
  return Promise.all(thunks.map(async (thunk) => { try { return await thunk(); } catch (error) { if (error && error.__workflowFatal) throw error; return null; } }));
};
const pipeline = async (items, ...stages) => {
  if (!Array.isArray(items)) throw __workflowError('pipeline() requires an items array');
  __assertWorkflowItems(items.length, 'pipeline()');
  if (stages.length === 0) throw __workflowError('pipeline() requires at least one stage function');
  for (let i = 0; i < stages.length; i++) if (typeof stages[i] !== 'function') throw __workflowError('pipeline() stage ' + i + ' is not a function');
  return Promise.all(items.map(async (item, index) => { let value = item; try { for (const stage of stages) value = await stage(value, item, index); return value; } catch (error) { if (error && error.__workflowFatal) throw error; return null; } }));
};`

func (e *Engine) runWorkflowProgram(ctx context.Context, session *Session, runID string, runInfo map[string]any, program *goja.Program, args any, hasArgs bool, runtimeConfig agentRuntime, recorder *workflowRecordState) (any, int, error) {
	vm := goja.New()
	tasks := make(chan jsTask, runtimeConfig.workflowMaxAgents+16)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var active sync.WaitGroup
	concurrency := runtimeConfig.workflowMaxConcurrentAgents
	if concurrency == 0 {
		concurrency = runtime.GOMAXPROCS(0) - 2
		if concurrency < 1 {
			concurrency = 1
		}
		if concurrency > 16 {
			concurrency = 16
		}
	}
	sem := make(chan struct{}, concurrency)
	started := 0
	currentPhase := ""

	_ = vm.Set("__workflowError", func(call goja.FunctionCall) goja.Value {
		return workflowJSError(vm, call.Argument(0).String(), true)
	})
	_ = vm.Set("__assertWorkflowItems", func(call goja.FunctionCall) goja.Value {
		length := int(call.Argument(0).ToInteger())
		if length > runtimeConfig.workflowMaxItems {
			panic(workflowJSError(vm, fmt.Sprintf("%s received %d items, over the per-call cap (%d)", call.Argument(1).String(), length, runtimeConfig.workflowMaxItems), true))
		}
		return goja.Undefined()
	})
	_ = vm.Set("phase", func(call goja.FunctionCall) goja.Value {
		if goja.IsUndefined(call.Argument(0)) || call.Argument(0).ExportType() == nil {
			panic(workflowJSError(vm, "phase() requires a non-empty title string", true))
		}
		title, ok := call.Argument(0).Export().(string)
		if !ok || title == "" {
			panic(workflowJSError(vm, "phase() requires a non-empty title string", true))
		}
		currentPhase = title
		e.emitDynamicCordisScopedContained("", "workflow/phase", runInfo, title)
		return goja.Undefined()
	})
	_ = vm.Set("log", func(call goja.FunctionCall) goja.Value {
		message, ok := call.Argument(0).Export().(string)
		if !ok {
			panic(workflowJSError(vm, "log() requires a message string", true))
		}
		e.emitDynamicCordisScopedContained("", "workflow/log", runInfo, message)
		return goja.Undefined()
	})
	_ = vm.Set("agent", func(call goja.FunctionCall) goja.Value {
		promise, resolve, reject := vm.NewPromise()
		prompt, ok := call.Argument(0).Export().(string)
		if !ok || prompt == "" {
			if err := reject(workflowJSError(vm, "agent() requires a non-empty prompt string", true)); err != nil {
				panic(err)
			}
			return vm.ToValue(promise)
		}
		opts, err := readWorkflowAgentOptions(call.Argument(1))
		if err != nil {
			if rejectErr := reject(workflowJSError(vm, err.Error(), true)); rejectErr != nil {
				panic(rejectErr)
			}
			return vm.ToValue(promise)
		}
		if started >= runtimeConfig.workflowMaxAgents {
			if rejectErr := reject(workflowJSError(vm, fmt.Sprintf("this run reached its total agent cap (%d)", runtimeConfig.workflowMaxAgents), true)); rejectErr != nil {
				panic(rejectErr)
			}
			return vm.ToValue(promise)
		}
		started++
		seq := started
		label := opts.Label
		if label == "" {
			label = workflowDefaultLabel(prompt)
		}
		phase := opts.Phase
		if phase == "" {
			phase = currentPhase
		}
		request := workflowChildRequest{parentID: session.Header.ID, runID: runID, seq: seq, label: label, phase: phase, prompt: prompt, subagentProvider: runtimeConfig.workflowProvider, disposeGrace: runtimeConfig.workflowDisposeGrace, provider: opts.Provider, model: opts.Model, schema: opts.Schema, recorder: recorder, runInfo: runInfo}
		active.Add(1)
		go func() {
			defer active.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-runCtx.Done():
				sendJSTask(runCtx, tasks, func() error { return reject(workflowJSError(vm, runCtx.Err().Error(), true)) })
				return
			}
			value, fatal := e.executeWorkflowChild(runCtx, request)
			sendJSTask(runCtx, tasks, func() error {
				if fatal != nil {
					return reject(workflowJSError(vm, fatal.Error(), true))
				}
				return resolve(value)
			})
		}()
		return vm.ToValue(promise)
	})
	if hasArgs {
		_ = vm.Set("args", args)
	} else {
		_ = vm.Set("args", goja.Undefined())
	}

	promise, err := runJSProgram(ctx, vm, program, tasks, runtimeConfig.workflowSyncTimeout)
	cancel()
	active.Wait()
	if err != nil {
		return nil, started, err
	}
	value, err := materializeJSON(promise.Result().Export())
	return value, started, err
}

func sendJSTask(ctx context.Context, tasks chan<- jsTask, task jsTask) {
	select {
	case tasks <- task:
	case <-ctx.Done():
		select {
		case tasks <- task:
		default:
		}
	}
}

func runJSProgram(ctx context.Context, vm *goja.Runtime, program *goja.Program, tasks <-chan jsTask, syncTimeout time.Duration) (*goja.Promise, error) {
	var value goja.Value
	err := runVMOperation(vm, syncTimeout, func() error {
		var runErr error
		value, runErr = vm.RunProgram(program)
		return runErr
	})
	if err != nil {
		return nil, err
	}
	promise, ok := value.Export().(*goja.Promise)
	if !ok {
		return nil, errors.New("JavaScript program did not return a promise")
	}
	for promise.State() == goja.PromiseStatePending {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case task := <-tasks:
			if err := runVMOperation(vm, syncTimeout, task); err != nil {
				return nil, err
			}
		}
	}
	if promise.State() == goja.PromiseStateRejected {
		return nil, fmt.Errorf("%s", jsErrorText(promise.Result()))
	}
	return promise, nil
}

func runVMOperation(vm *goja.Runtime, timeout time.Duration, operation func() error) error {
	if timeout <= 0 {
		timeout = workflowDefaultSyncTimeout
	}
	finished := make(chan struct{})
	watchdogDone := make(chan struct{})
	go func() {
		select {
		case <-time.After(timeout):
			vm.Interrupt(errors.New("JavaScript synchronous execution timed out"))
		case <-finished:
		}
		close(watchdogDone)
	}()
	err := operation()
	close(finished)
	<-watchdogDone
	vm.ClearInterrupt()
	return err
}

func workflowJSError(vm *goja.Runtime, message string, fatal bool) *goja.Object {
	object := vm.NewGoError(errors.New(message))
	_ = object.Set("__workflowFatal", fatal)
	return object
}

func jsErrorText(value goja.Value) string {
	if object, ok := value.(*goja.Object); ok {
		if stack := object.Get("stack"); stack != nil && !goja.IsUndefined(stack) {
			return stack.String()
		}
	}
	return value.String()
}

func readWorkflowAgentOptions(value goja.Value) (workflowAgentOptions, error) {
	if goja.IsUndefined(value) {
		return workflowAgentOptions{}, nil
	}
	normalized, err := materializeJSON(value.Export())
	if err != nil {
		return workflowAgentOptions{}, fmt.Errorf("agent() options must be plain JSON data: %w", err)
	}
	record, ok := normalized.(map[string]any)
	if !ok {
		return workflowAgentOptions{}, errors.New("agent() options must be an object")
	}
	allowed := map[string]bool{"label": true, "phase": true, "schema": true, "provider": true, "model": true}
	for key := range record {
		if !allowed[key] {
			return workflowAgentOptions{}, fmt.Errorf("agent() option %q is not recognized", key)
		}
	}
	var opts workflowAgentOptions
	for key, target := range map[string]*string{"label": &opts.Label, "phase": &opts.Phase, "provider": &opts.Provider, "model": &opts.Model} {
		if raw, exists := record[key]; exists {
			text, ok := raw.(string)
			if !ok {
				return workflowAgentOptions{}, fmt.Errorf("agent() option %q must be a string", key)
			}
			*target = text
		}
	}
	if raw, exists := record["schema"]; exists {
		schema, ok := raw.(map[string]any)
		if !ok {
			return workflowAgentOptions{}, errors.New("agent() schema must be an object")
		}
		if err := validateWorkflowSchema(schema, true); err != nil {
			return workflowAgentOptions{}, fmt.Errorf("agent() schema is outside the supported subset: %w", err)
		}
		opts.Schema = schema
	}
	return opts, nil
}

func workflowDefaultLabel(prompt string) string {
	if index := strings.IndexByte(prompt, '\n'); index >= 0 {
		prompt = prompt[:index]
	}
	runes := []rune(prompt)
	if len(runes) <= 48 {
		return prompt
	}
	return string(runes[:47]) + "..."
}

func (e *Engine) executeWorkflowChild(ctx context.Context, request workflowChildRequest) (any, error) {
	parent, err := e.getSession(request.parentID)
	if err != nil {
		return nil, err
	}
	provider := e.GetSubagentProvider(request.subagentProvider)
	if provider == nil {
		return nil, fmt.Errorf("agent() could not start a child: no subagent provider registered for %q", request.subagentProvider)
	}
	if request.schema != nil && !provider.Capabilities().OutputSchema {
		return nil, fmt.Errorf("agent() could not start a child: subagent provider %q does not support structured output", request.subagentProvider)
	}
	var agentOptions *SubagentAgentOptions
	if request.provider != "" || request.model != "" {
		agentOptions = &SubagentAgentOptions{Provider: request.provider, Model: request.model}
	}
	run, err := e.StartSubagent(ctx, request.subagentProvider, SubagentStartRequest{
		ParentSessionID: request.parentID,
		Label:           request.label,
		Prompt:          []ContentBlock{{Type: "text", Text: request.prompt}},
		OutputSchema:    request.schema,
		AgentOptions:    agentOptions,
	})
	if err != nil {
		return nil, fmt.Errorf("agent() could not start a child: %w", err)
	}
	childID := run.ID
	agentInfo := map[string]any{"seq": request.seq, "label": request.label, "childId": childID}
	if request.phase != "" {
		agentInfo["phase"] = request.phase
	}
	if request.runInfo != nil {
		e.emitDynamicCordisScopedContained("", "workflow/agent-start", request.runInfo, agentInfo)
	}
	outcome := "failed"
	defer func() {
		if request.runInfo == nil {
			return
		}
		end := map[string]any{"seq": request.seq, "label": request.label, "childId": childID, "outcome": outcome}
		if request.phase != "" {
			end["phase"] = request.phase
		}
		e.emitDynamicCordisScopedContained("", "workflow/agent-end", request.runInfo, end)
	}()
	data := map[string]any{"runId": request.runID, "seq": request.seq, "label": request.label, "childId": childID}
	if request.phase != "" {
		data["phase"] = request.phase
	}
	recordStarted := request.recorder.append(e, parent, "tool-workflow/agent-start", data)
	defer func() {
		if recordStarted {
			request.recorder.append(e, parent, "tool-workflow/agent-end", map[string]any{"runId": request.runID, "seq": request.seq, "outcome": outcome})
		}
	}()
	result, waitErr := run.Wait(ctx)
	disposeErr := disposeSubagentRun(run, request.disposeGrace)
	if waitErr != nil {
		outcome = "cancelled"
		return nil, waitErr
	}
	if disposeErr != nil {
		return nil, disposeErr
	}
	if result.StopReason != SubagentCompleted {
		outcome = "failed"
		return nil, nil
	}
	if request.schema == nil {
		outcome = "completed"
		return contentValueText(result.Output), nil
	}
	if result.Structured == nil || validateJSONAgainstSchema(result.Structured, request.schema) != nil {
		outcome = "failed"
		return nil, nil
	}
	outcome = "completed"
	return result.Structured, nil
}

func disposeSubagentRun(run *SubagentRun, grace time.Duration) error {
	if grace <= 0 {
		return run.Dispose()
	}
	done := make(chan error, 1)
	go func() { done <- run.Dispose() }()
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errors.New("subagent disposal exceeded workflow dispose grace")
	}
}

// executeRalphChild uses the provider registry directly. Ralph requires a
// genuinely fresh child and structured output; embedding the schema in a
// prompt would make those deployment capabilities advisory instead of real.
func (e *Engine) executeRalphChild(ctx context.Context, parentID, providerName, label, prompt string, schema map[string]any) (any, error) {
	provider := e.GetSubagentProvider(providerName)
	if provider == nil {
		return nil, fmt.Errorf("Ralph subagent provider %q is not registered", providerName)
	}
	if !provider.Capabilities().OutputSchema {
		return nil, fmt.Errorf("Ralph subagent provider %q does not support structured output", providerName)
	}
	if provider.InheritsParentContext() {
		return nil, fmt.Errorf("Ralph subagent provider %q inherits parent context; Ralph requires a fresh provider", providerName)
	}
	run, err := e.StartSubagent(ctx, providerName, SubagentStartRequest{
		ParentSessionID: parentID,
		Label:           label,
		Prompt:          []ContentBlock{{Type: "text", Text: prompt}},
		OutputSchema:    schema,
	})
	if err != nil {
		return nil, err
	}
	result, waitErr := run.Wait(ctx)
	disposeErr := run.Dispose()
	if waitErr != nil {
		return nil, waitErr
	}
	if disposeErr != nil {
		return nil, disposeErr
	}
	if result.StopReason != SubagentCompleted || result.Structured == nil {
		if result.Diagnostic != "" {
			return nil, fmt.Errorf("Ralph child did not produce structured output: stopReason=%s diagnostic=%s", result.StopReason, result.Diagnostic)
		}
		return nil, nil
	}
	return result.Structured, nil
}

func materializeJSON(value any) (any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("value is not lossless JSON: %w", err)
	}
	var normalized any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	return normalized, nil
}

func validateWorkflowSchema(schema map[string]any, root bool) error {
	allowed := map[string]bool{"type": true, "properties": true, "required": true, "additionalProperties": true, "items": true, "enum": true, "const": true, "oneOf": true, "anyOf": true, "description": true, "title": true, "default": true, "examples": true}
	for key := range schema {
		if !allowed[key] {
			return fmt.Errorf("unsupported keyword %q", key)
		}
	}
	if root && schema["type"] != "object" {
		return errors.New("schema.type must be object")
	}
	if branches, ok := schema["oneOf"]; ok {
		items, ok := branches.([]any)
		if !ok || len(items) < 2 {
			return errors.New("oneOf must contain at least two schemas")
		}
		for _, item := range items {
			child, ok := item.(map[string]any)
			if !ok {
				return errors.New("oneOf entries must be schemas")
			}
			if err := validateWorkflowSchema(child, false); err != nil {
				return err
			}
		}
		return nil
	}
	if branches, ok := schema["anyOf"]; ok {
		items, ok := branches.([]any)
		if !ok || len(items) < 2 {
			return errors.New("anyOf must contain at least two schemas")
		}
		for _, item := range items {
			child, ok := item.(map[string]any)
			if !ok {
				return errors.New("anyOf entries must be schemas")
			}
			if err := validateWorkflowSchema(child, false); err != nil {
				return err
			}
		}
		return nil
	}
	typ, _ := schema["type"].(string)
	if typ == "" {
		return nil
	}
	if !map[string]bool{"object": true, "array": true, "string": true, "number": true, "integer": true, "boolean": true, "null": true}[typ] {
		return fmt.Errorf("unsupported type %q", typ)
	}
	if properties, ok := schema["properties"]; ok {
		record, ok := properties.(map[string]any)
		if !ok || typ != "object" {
			return errors.New("properties requires object type")
		}
		for _, raw := range record {
			child, ok := raw.(map[string]any)
			if !ok {
				return errors.New("property schema must be an object")
			}
			if err := validateWorkflowSchema(child, false); err != nil {
				return err
			}
		}
	}
	if items, ok := schema["items"]; ok {
		child, ok := items.(map[string]any)
		if !ok || typ != "array" {
			return errors.New("items requires array type")
		}
		return validateWorkflowSchema(child, false)
	}
	return nil
}

func validateJSONAgainstSchema(value any, schema map[string]any) error {
	if branches, ok := schema["oneOf"].([]any); ok {
		matches := 0
		for _, raw := range branches {
			if child, ok := raw.(map[string]any); ok && validateJSONAgainstSchema(value, child) == nil {
				matches++
			}
		}
		if matches != 1 {
			return errors.New("value must match exactly one oneOf branch")
		}
		return nil
	}
	if branches, ok := schema["anyOf"].([]any); ok {
		for _, raw := range branches {
			if child, ok := raw.(map[string]any); ok && validateJSONAgainstSchema(value, child) == nil {
				return nil
			}
		}
		return errors.New("value must match one anyOf branch")
	}
	typ, _ := schema["type"].(string)
	switch typ {
	case "object":
		record, ok := value.(map[string]any)
		if !ok {
			return errors.New("expected object")
		}
		properties, _ := schema["properties"].(map[string]any)
		if required, ok := schema["required"].([]any); ok {
			for _, raw := range required {
				name, _ := raw.(string)
				if _, exists := record[name]; !exists {
					return fmt.Errorf("missing required property %q", name)
				}
			}
		}
		for key, item := range record {
			raw, exists := properties[key]
			if !exists {
				if additional, ok := schema["additionalProperties"].(bool); ok && !additional {
					return fmt.Errorf("unexpected property %q", key)
				}
				continue
			}
			if child, ok := raw.(map[string]any); ok {
				if err := validateJSONAgainstSchema(item, child); err != nil {
					return fmt.Errorf("property %s: %w", key, err)
				}
			}
		}
	case "array":
		items, ok := value.([]any)
		if !ok {
			return errors.New("expected array")
		}
		if child, ok := schema["items"].(map[string]any); ok {
			for _, item := range items {
				if err := validateJSONAgainstSchema(item, child); err != nil {
					return err
				}
			}
		}
	case "string":
		if _, ok := value.(string); !ok {
			return errors.New("expected string")
		}
	case "number":
		if !isJSONNumber(value, false) {
			return errors.New("expected number")
		}
	case "integer":
		if !isJSONNumber(value, true) {
			return errors.New("expected integer")
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return errors.New("expected boolean")
		}
	case "null":
		if value != nil {
			return errors.New("expected null")
		}
	}
	if expected, exists := schema["const"]; exists && !jsonValuesEqual(value, expected) {
		return errors.New("value does not match const")
	}
	if values, ok := schema["enum"].([]any); ok {
		for _, expected := range values {
			if jsonValuesEqual(value, expected) {
				return nil
			}
		}
		return errors.New("value is outside enum")
	}
	return nil
}

func isJSONNumber(value any, integer bool) bool {
	switch number := value.(type) {
	case json.Number:
		if integer {
			_, err := strconv.ParseInt(number.String(), 10, 64)
			return err == nil
		}
		_, err := strconv.ParseFloat(number.String(), 64)
		return err == nil
	case float64:
		return !integer || number == float64(int64(number))
	case int64, int, int32:
		return true
	default:
		return false
	}
}

func jsonValuesEqual(left, right any) bool {
	l, lerr := json.Marshal(left)
	r, rerr := json.Marshal(right)
	return lerr == nil && rerr == nil && bytes.Equal(l, r)
}

type ralphReport struct {
	Status    string   `json:"status"`
	Summary   string   `json:"summary"`
	Evidence  []string `json:"evidence"`
	NextSteps []string `json:"nextSteps"`
	Blocker   string   `json:"blocker"`
}

func builtinRalphTool(e *Engine) Tool {
	type input struct {
		Objective *string `json:"objective"`
		MaxRounds *int    `json:"maxRounds"`
	}
	return Tool{
		Schema: ToolSchema{Name: "ralph", Description: ralphDescription, Parameters: objectSchema(map[string]any{
			"objective": map[string]any{"type": "string", "description": "The immutable completion objective for every fresh Ralph round."},
			"maxRounds": map[string]any{"type": "number", "description": "Optional positive safe-integer round cap, bounded by the deployment ceiling."},
		}, "objective"), Output: objectSchema(map[string]any{"runId": map[string]any{"type": "string"}, "agentsStarted": map[string]any{"type": "integer"}, "result": map[string]any{}}, "runId", "agentsStarted", "result")},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if in.Objective == nil || strings.TrimSpace(*in.Objective) == "" {
				return ToolResult{}, errors.New("Ralph objective must be a non-empty string")
			}
			if call.SessionID == "" {
				return ToolResult{}, errors.New("Ralph tool requires a calling agent")
			}
			session, err := e.getSession(call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			runtimeConfig, err := e.runtimeForSession(session)
			if err != nil {
				return ToolResult{}, err
			}
			maxRounds := runtimeConfig.ralphMaxRounds
			if in.MaxRounds != nil {
				maxRounds = *in.MaxRounds
			}
			if maxRounds < 1 || int64(maxRounds) > maxJSONSafeInteger || maxRounds > runtimeConfig.ralphMaxRounds {
				return ToolResult{}, fmt.Errorf("Ralph maxRounds must be between 1 and %d", runtimeConfig.ralphMaxRounds)
			}
			result, err := e.runRalph(ctx, call.SessionID, runtimeConfig.ralphSubagentProvider, strings.TrimSpace(*in.Objective), maxRounds, runtimeConfig.ralphMaxHandoffChars, runtimeConfig.ralphMaxResultChars)
			if err != nil {
				return ToolResult{}, err
			}
			toolResult := textToolResult(boundText(renderRalphResult(result), runtimeConfig.ralphMaxResultChars))
			toolResult.Value = map[string]any{"runId": newID("workflow"), "agentsStarted": result.RoundsStarted, "result": result}
			return toolResult, nil
		},
	}
}

type ralphResult struct {
	Status        string      `json:"status"`
	RoundsStarted int         `json:"roundsStarted"`
	Report        ralphReport `json:"report"`
}

func (e *Engine) runRalph(ctx context.Context, parentID, providerName, objective string, maxRounds, maxHandoffChars, maxResultChars int) (ralphResult, error) {
	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"status":    map[string]any{"type": "string", "enum": []any{"continue", "complete", "blocked"}},
			"summary":   map[string]any{"type": "string"},
			"evidence":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"nextSteps": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"blocker":   map[string]any{"type": "string"},
		},
		"required": []any{"status", "summary", "evidence", "nextSteps", "blocker"},
	}
	var previous *ralphReport
	for round := 1; round <= maxRounds; round++ {
		prior := "(none - this is the first round)"
		if previous != nil {
			data, _ := json.Marshal(previous)
			prior = string(data)
		}
		prompt := strings.Join([]string{
			"You are one fresh worker in a foreground Ralph loop. You receive no parent conversation and no prior child session. Do not call the ralph tool.",
			"Immutable objective:\n" + objective,
			fmt.Sprintf("Ralph round: %d of %d.", round, maxRounds),
			"The shared workspace and its current working tree are the long-term memory and source of truth. Inspect them before acting, preserve existing work, perform concrete in-scope work, and verify what you change.",
			"Previous structured handoff:\n" + prior,
			"Use status continue with at least one nextSteps entry while useful work remains; complete only with concrete evidence and no nextSteps; blocked only when no meaningful progress is possible without human input or an external-state change. blocker must be empty unless blocked.",
		}, "\n\n")
		value, fatal := e.executeRalphChild(ctx, parentID, providerName, fmt.Sprintf("Ralph round %d", round), prompt, schema)
		if fatal != nil {
			return ralphResult{}, fatal
		}
		if value == nil {
			if previous == nil {
				return ralphResult{}, errors.New(boundText(fmt.Sprintf("Ralph round %d child failed before producing a structured report.\nNo previous handoff was available.", round), maxResultChars))
			}
			data, _ := json.MarshalIndent(previous, "", "  ")
			return ralphResult{}, errors.New(boundText(fmt.Sprintf("Ralph round %d child failed before producing a structured report.\nLast successful handoff:\n%s", round, data), maxResultChars))
		}
		if err := validateJSONAgainstSchema(value, schema); err != nil {
			return ralphResult{}, errors.New("Ralph workflow returned a malformed round report")
		}
		data, _ := json.Marshal(value)
		var report ralphReport
		if err := json.Unmarshal(data, &report); err != nil {
			return ralphResult{}, errors.New("Ralph workflow returned a malformed round report")
		}
		if err := validateRalphReport(report); err != nil {
			return ralphResult{}, err
		}
		if jsonUTF16Length(data) > maxHandoffChars {
			return ralphResult{}, fmt.Errorf("Ralph round report exceeds maxHandoffChars (%d > %d)", jsonUTF16Length(data), maxHandoffChars)
		}
		switch report.Status {
		case "complete", "blocked":
			return ralphResult{Status: report.Status, RoundsStarted: round, Report: report}, nil
		case "continue":
			previous = &report
		}
	}
	return ralphResult{Status: "budget-limited", RoundsStarted: maxRounds, Report: *previous}, nil
}

func validateRalphReport(report ralphReport) error {
	normalized := func(value string, allowEmpty bool) bool {
		return value == strings.TrimSpace(value) && (allowEmpty || value != "")
	}
	if !normalized(report.Summary, false) || !normalized(report.Blocker, true) {
		return errors.New("Ralph round report strings must be normalized")
	}
	for _, list := range [][]string{report.Evidence, report.NextSteps} {
		for _, value := range list {
			if !normalized(value, false) {
				return errors.New("Ralph round report lists must contain normalized non-empty strings")
			}
		}
	}
	switch report.Status {
	case "continue":
		if len(report.NextSteps) == 0 || report.Blocker != "" {
			return errors.New("a continuing Ralph report needs nextSteps and an empty blocker")
		}
	case "complete":
		if len(report.Evidence) == 0 || len(report.NextSteps) != 0 || report.Blocker != "" {
			return errors.New("a complete Ralph report needs evidence, no nextSteps, and an empty blocker")
		}
	case "blocked":
		if !normalized(report.Blocker, false) {
			return errors.New("a blocked Ralph report needs a concrete blocker")
		}
	default:
		return errors.New("Ralph round report status is invalid")
	}
	return nil
}

func jsonUTF16Length(data []byte) int {
	return len(utf16.Encode([]rune(string(data))))
}

func renderRalphResult(result ralphResult) string {
	rounds := fmt.Sprintf("%d rounds", result.RoundsStarted)
	if result.RoundsStarted == 1 {
		rounds = "1 round"
	}
	report, _ := json.MarshalIndent(result.Report, "", "  ")
	switch result.Status {
	case "complete":
		return fmt.Sprintf("Ralph worker reported completion after %s.\nFinal report:\n%s", rounds, report)
	case "blocked":
		return fmt.Sprintf("Ralph worker reported a blocker after %s.\nFinal report:\n%s", rounds, report)
	default:
		return fmt.Sprintf("Ralph reached its %s limit; the worker reported work remaining.\nFinal report:\n%s", rounds, report)
	}
}

func boundText(text string, max int) string {
	const notice = "\n… [truncated]"
	if jsonUTF16Length([]byte(text)) <= max {
		return text
	}
	noticeUnits := jsonUTF16Length([]byte(notice))
	if max <= noticeUnits {
		clipped, _, _ := truncateUTF16Units(notice, max)
		return clipped
	}
	clipped, _, _ := truncateUTF16Units(text, max-noticeUnits)
	return clipped + notice
}

func truncateUTF16Units(text string, max int) (string, int, bool) {
	total := len(utf16.Encode([]rune(text)))
	if total <= max {
		return text, 0, false
	}
	if max <= 0 {
		return "", total, true
	}
	used, end := 0, 0
	for index, value := range text {
		units := 1
		if value > 0xffff {
			units = 2
		}
		if used+units > max {
			break
		}
		used += units
		end = index + len(string(value))
	}
	return text[:end], total - max, true
}

func builtinRunCodeTool(e *Engine) Tool {
	type input struct {
		Code        *string `json:"code"`
		Description *string `json:"description"`
	}
	return Tool{
		Schema: ToolSchema{Name: "run_code", Description: runCodeDescription, Parameters: objectSchema(map[string]any{
			"code":        map[string]any{"type": "string", "description": "The program: the body of an async TypeScript function."},
			"description": map[string]any{"type": "string", "description": "Clear, concise description of what this program does in active voice, 5-10 words."},
		}, "code", "description"), Output: objectSchema(map[string]any{"logs": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, "result": map[string]any{}}, "logs")},
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			call := exec.Call
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if in.Code == nil || in.Description == nil {
				return ToolResult{}, errors.New("run_code requires code and description")
			}
			if strings.TrimSpace(*in.Description) == "" {
				return ToolResult{}, errors.New("invalid description: expected a non-empty string")
			}
			return e.executeRunCode(exec, call, *in.Code)
		},
	}
}

func (e *Engine) executeRunCode(outer *ToolRunContext, call ToolCall, code string) (ToolResult, error) {
	ctx := outer.Context
	if call.SessionID == "" {
		return ToolResult{}, errors.New("run_code requires a calling agent")
	}
	session, err := e.getSession(call.SessionID)
	if err != nil {
		return ToolResult{}, err
	}
	tools, err := e.codeToolsForSession(session)
	if err != nil {
		return ToolResult{}, err
	}
	program, err := compileRunCode(code)
	if err != nil {
		return ToolResult{}, err
	}
	vm := goja.New()
	tasks := make(chan jsTask, 1024)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	dispatcher := newCodeToolDispatcher(e, session, outer, runCtx, tasks, call)
	sequence := int64(0)
	logs := []string{}

	console := vm.NewObject()
	_ = console.Set("log", func(call goja.FunctionCall) goja.Value {
		parts := make([]string, len(call.Arguments))
		for index, value := range call.Arguments {
			parts[index] = renderConsoleValue(value.Export())
		}
		logs = append(logs, strings.Join(parts, " "))
		return goja.Undefined()
	})
	_ = vm.Set("console", console)

	toolObject := vm.NewObject()
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		name := name
		_ = toolObject.Set(name, func(invocation goja.FunctionCall) goja.Value {
			promise, resolve, reject := vm.NewPromise()
			arguments, normalizeErr := materializeJSON(invocation.Argument(0).Export())
			if normalizeErr != nil {
				if err := reject(toolCallJSError(vm, name, "tool arguments must be lossless JSON: "+normalizeErr.Error())); err != nil {
					panic(err)
				}
				return vm.ToValue(promise)
			}
			argumentObject, ok := arguments.(map[string]any)
			if !ok {
				if err := reject(toolCallJSError(vm, name, "tool arguments must be an object")); err != nil {
					panic(err)
				}
				return vm.ToValue(promise)
			}
			rawArguments, _ := json.Marshal(argumentObject)
			sequence++
			subCallID := fmt.Sprintf("%s:code:%d", call.ID, sequence)
			entry := &codeToolDispatchEntry{
				call: ToolCall{
					ID: subCallID, Name: name, Arguments: rawArguments,
					Workspace: call.Workspace, SessionID: call.SessionID, ParentCallID: call.ID,
				},
				arguments: argumentObject, vm: vm, resolve: resolve, reject: reject,
			}
			if !dispatcher.submit(entry) {
				if err := reject(toolCallJSError(vm, name, fmt.Sprintf("run_code run is over (run_code settled); %s not dispatched", name))); err != nil {
					panic(err)
				}
			}
			return vm.ToValue(promise)
		})
	}
	_ = vm.Set("tools", toolObject)
	promise, runErr := runJSProgram(ctx, vm, program, tasks, workflowDefaultSyncTimeout)
	cancel()
	dispatcher.closeAndDrain()
	if runErr != nil {
		kind := "exception"
		if ctx.Err() != nil {
			kind = "abort"
		} else if strings.Contains(strings.ToLower(runErr.Error()), "timed out") {
			kind = "timeout"
		}
		return codeRunFailureResult(kind, runErr.Error(), logs), nil
	}
	value, err := materializeJSON(promise.Result().Export())
	if err != nil {
		return codeRunFailureResult("exception", "result is not lossless JSON: "+err.Error(), logs), nil
	}
	parts := append([]string(nil), logs...)
	if promise.Result() != nil && !goja.IsUndefined(promise.Result()) {
		parts = append(parts, renderCodeValue(value))
	}
	text := "(run_code completed with no output)"
	if len(parts) > 0 {
		text = strings.Join(parts, "\n")
	}
	content := []ContentBlock{{Type: "text", Text: text}}
	result := ToolResult{Content: content, Value: map[string]any{"logs": append([]string{}, logs...)}}
	if promise.Result() != nil && !goja.IsUndefined(promise.Result()) {
		result.Value.(map[string]any)["result"] = value
	}
	return result, nil
}

func codeRunFailureResult(kind, message string, logs []string) ToolResult {
	text := fmt.Sprintf("code run failed (%s): %s", kind, message)
	if len(logs) > 0 {
		text += "\nCaptured output:\n" + strings.Join(logs, "\n")
	}
	return ToolResult{
		Content: []ContentBlock{{Type: "text", Text: "Error: " + text}},
		IsError: true,
		Error:   &ToolError{Name: "CodeRunFailedError", Code: "CODE_RUN_FAILED", Message: text},
	}
}

func compileRunCode(body string) (*goja.Program, error) {
	source := "async function __dsh_run(tools, console) {\n'use strict';\n" + body + "\n}\n__dsh_run(tools, console)"
	result := api.Transform(source, api.TransformOptions{Loader: api.LoaderTS, Target: api.ESNext, LogLevel: api.LogLevelSilent, Sourcefile: "run_code.ts", LegalComments: api.LegalCommentsNone})
	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("run_code TypeScript does not parse: %s", result.Errors[0].Text)
	}
	program, err := goja.Compile("run_code.js", string(result.Code), false)
	if err != nil {
		return nil, fmt.Errorf("run_code JavaScript does not parse: %w", err)
	}
	return program, nil
}

func toolCallJSError(vm *goja.Runtime, name, message string) *goja.Object {
	object := vm.NewGoError(errors.New(message))
	_ = object.Set("name", "ToolCallError")
	_ = object.Set("toolName", name)
	return object
}

func (e *Engine) codeToolsForSession(session *Session) (map[string]Tool, error) {
	runtimeConfig, err := e.runtimeForSession(session)
	if err != nil {
		return nil, err
	}
	session.mu.Lock()
	sessionID, restriction := session.Header.ID, session.toolRestriction
	session.mu.Unlock()
	e.mu.RLock()
	tools := make(map[string]Tool, len(e.tools)+len(e.scopedTools[sessionID]))
	for name, tool := range e.tools {
		if name == "run_code" {
			continue
		}
		owner := e.toolOwners[name]
		if owner != "" && owner != sessionID {
			continue
		}
		if owner == "" && !restriction.allows(name) {
			continue
		}
		if !shippedToolNames[name] || runtimeConfig.toolNames == nil || runtimeConfig.toolNames[name] {
			if runtimeConfig.webTools != nil && (name == "web_search" || name == "web_fetch") {
				var enabled bool
				tool, enabled = sessionWebTool(e, name, runtimeConfig.webTools)
				if !enabled {
					continue
				}
			}
			if name == shellToolName && runtimeConfig.persistentBash {
				tool.Schema.Output = map[string]any{"type": "string"}
				tool.Schema.Description = runtimeConfig.persistentBashDesc
				if tool.Schema.Description == "" {
					tool.Schema.Description = persistentShellDefaultDescription
				}
				tool.Schema.Parameters = objectSchema(map[string]any{"command": map[string]any{"type": "string", "description": shellCommandDescription}}, "command")
			}
			tools[name] = tool
		}
	}
	if runtimeConfig.workflowToolName != "" && runtimeConfig.workflowToolName != "workflow" && runtimeConfig.toolNames[runtimeConfig.workflowToolName] {
		if workflow, ok := e.tools["workflow"]; ok && restriction.allows(runtimeConfig.workflowToolName) {
			workflow.Schema.Name = runtimeConfig.workflowToolName
			delete(tools, "workflow")
			tools[runtimeConfig.workflowToolName] = workflow
		}
	}
	for name, tool := range e.scopedTools[sessionID] {
		if name != "run_code" {
			tools[name] = tool
		}
	}
	e.mu.RUnlock()
	return tools, nil
}

func canonicalCodeToolValue(_ string, result ToolResult) any {
	if result.Value != nil {
		if value, err := canonicalToolValue(result.Value); err == nil {
			return value
		}
		return result.Value
	}
	if len(result.Content) == 0 {
		return nil
	}
	if len(result.Content) == 1 && result.Content[0].Type == "text" {
		text := result.Content[0].Text
		var value any
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		if decoder.Decode(&value) == nil {
			return value
		}
		return text
	}
	var text strings.Builder
	for _, block := range result.Content {
		if block.Type == "text" {
			text.WriteString(block.Text)
		}
	}
	return text.String()
}

func canonicalToolValue(value any) (any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var canonical any
	if err := json.Unmarshal(data, &canonical); err != nil {
		return nil, err
	}
	return canonical, nil
}

func renderConsoleValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(data)
}

func renderCodeValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(data)
}

func (e *Engine) codeModePrompt(session *Session, runtimeConfig agentRuntime) string {
	tools, err := e.codeToolsForSession(session)
	if err != nil {
		return ""
	}
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, name)
	}
	sort.Strings(names)
	var sdk strings.Builder
	sdk.WriteString("## Writing code for run_code\n\n`run_code` takes two required arguments: `code`, the body of an async TypeScript function, and `description`, a short summary of what the program does. The declarations below are SDK bindings for this program. A declaration does not make its name a directly callable tool; only names supplied as separate tool schemas may be called directly.\n\nInside the program, call tools as `await tools.name(args)`. Failed calls reject with ToolCallError. Emit only curated output with console.log and/or return.\n\nProgram-only SDK bindings:\n\n```ts\ntype JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };\ndeclare class ToolCallError extends Error { readonly toolName: string; }\ndeclare const tools: {\n")
	for _, name := range names {
		tool := tools[name]
		if description := strings.TrimSpace(tool.Schema.Description); description != "" {
			fmt.Fprintf(&sdk, "  /** %s */\n", strings.ReplaceAll(description, "*/", "* /"))
		}
		fmt.Fprintf(&sdk, "  %q: (args: %s) => Promise<%s>;\n", name, jsonSchemaTypeScript(tool.Schema.Parameters), jsonSchemaTypeScript(tool.Schema.Output))
	}
	sdk.WriteString("};\n```")
	return sdk.String()
}

func jsonSchemaTypeScript(schema map[string]any) string {
	if branches, ok := schema["oneOf"].([]any); ok {
		parts := make([]string, 0, len(branches))
		for _, raw := range branches {
			if child, ok := raw.(map[string]any); ok {
				parts = append(parts, jsonSchemaTypeScript(child))
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, " | ")
		}
	}
	if branches, ok := schema["anyOf"].([]any); ok {
		parts := make([]string, 0, len(branches))
		for _, raw := range branches {
			if child, ok := raw.(map[string]any); ok {
				parts = append(parts, jsonSchemaTypeScript(child))
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, " | ")
		}
	}
	if values, ok := schema["enum"].([]any); ok && len(values) > 0 {
		parts := make([]string, 0, len(values))
		for _, value := range values {
			data, err := json.Marshal(value)
			if err == nil {
				parts = append(parts, string(data))
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, " | ")
		}
	}
	switch schema["type"] {
	case "string":
		return "string"
	case "number", "integer":
		return "number"
	case "boolean":
		return "boolean"
	case "null":
		return "null"
	case "array":
		if items, ok := schema["items"].(map[string]any); ok {
			return "(" + jsonSchemaTypeScript(items) + ")[]"
		}
		return "JsonValue[]"
	case "object":
		properties, _ := schema["properties"].(map[string]any)
		required := map[string]bool{}
		switch values := schema["required"].(type) {
		case []any:
			for _, value := range values {
				if name, ok := value.(string); ok {
					required[name] = true
				}
			}
		case []string:
			for _, name := range values {
				required[name] = true
			}
		}
		names := make([]string, 0, len(properties))
		for name := range properties {
			names = append(names, name)
		}
		sort.Strings(names)
		parts := make([]string, 0, len(names)+1)
		for _, name := range names {
			child, _ := properties[name].(map[string]any)
			optional := "?"
			if required[name] {
				optional = ""
			}
			parts = append(parts, fmt.Sprintf("%q%s: %s", name, optional, jsonSchemaTypeScript(child)))
		}
		if additional, ok := schema["additionalProperties"].(bool); !ok || additional {
			parts = append(parts, "[key: string]: JsonValue")
		}
		return "{ " + strings.Join(parts, "; ") + " }"
	default:
		return "JsonValue"
	}
}
