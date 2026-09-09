package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runDynamicBuiltinPlugin(t *testing.T, e *Engine, sessionID, prefix, code string) (string, string) {
	t.Helper()
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: prefix},
		Name:      prefix + " builtins",
		Purpose:   "exercise dynamic Host built-in services",
		Code:      DynamicCordisCode{Host: code},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.DynamicCordisRun(t.Context(), sessionID, receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	return receipt.PluginID, run.PluginRunID
}

func TestDynamicCordisSystemPromptSectionAssembleAndStop(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-system-prompt", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "prm", `
return {
  inject: ['systemPrompt'],
  apply(ctx) {
	    ctx.systemPrompt.section({
	      name: 'dynamic:identity',
	      order: -50,
	      text: context => 'Dynamic {{model}} in {{cwd}} for ' + (context.label || 'runtime') + '.'
    })
	    const disposeVariableSection = ctx.systemPrompt.section({
	      name: 'dynamic:variable',
	      order: -40,
	      text: 'Variable {{dynamic_value}}.'
	    })
    const disposeContext = ctx.systemPrompt.context({
      name: 'dynamic:context',
      order: 10,
      text: context => 'Context ' + context.label
    })
	    const disposeVariable = ctx.systemPrompt.variable('dynamic_value', context => context.label || 'runtime')
    const disposeTools = ctx.systemPrompt.tools(context => ({
      schemas: [{
        name: 'dynamic_schema_' + context.label,
        description: 'Dynamic schema',
        parameters: { type: 'object', properties: {} }
      }]
    }))
    const releaseSuppression = ctx.systemPrompt.suppressRuntimeContext()
    harness.handle('assemble', args => ctx.systemPrompt.assemble(args))
    harness.handle('unsuppress', args => {
      releaseSuppression()
      return ctx.systemPrompt.assemble(args)
    })
	    harness.handle('disposeInputs', args => {
	      disposeContext()
	      disposeVariableSection()
	      disposeVariable()
      disposeTools()
      return ctx.systemPrompt.assemble(args)
    })
  }
}`)

	assembled := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "assemble", map[string]any{"label": "hidden"})
	if !assembled.OK {
		t.Fatalf("assemble = %#v", assembled)
	}
	value, ok := assembled.Value.(map[string]any)
	if !ok {
		t.Fatalf("assembly = %#v", assembled.Value)
	}
	sections, ok := value["sections"].([]any)
	if !ok || len(sections) < 4 {
		t.Fatalf("sections = %#v", value["sections"])
	}
	names := make([]string, 0, len(sections))
	for _, raw := range sections {
		section, _ := raw.(map[string]any)
		names = append(names, section["name"].(string))
	}
	if strings.Join(names[:4], ",") != "harness:identity,dynamic:identity,dynamic:variable,deployment:persona-prefix" {
		t.Fatalf("section order = %#v", names)
	}
	if sections[1].(map[string]any)["text"] != "Dynamic {{model}} in {{cwd}} for hidden." {
		t.Fatalf("section context = %#v", sections[1])
	}
	if sections[2].(map[string]any)["text"] != "Variable {{dynamic_value}}." {
		t.Fatalf("section variable = %#v", sections[2])
	}
	if contexts, _ := value["contexts"].([]any); len(contexts) != 0 {
		t.Fatalf("suppressed contexts = %#v", contexts)
	}
	variables := value["variables"].(map[string]any)
	if variables["dynamic_value"] != "hidden" {
		t.Fatalf("variables = %#v", variables)
	}
	tools := value["tools"].([]any)
	foundTool := false
	for _, raw := range tools {
		tool := raw.(map[string]any)
		if tool["name"] == "dynamic_schema_hidden" {
			foundTool = true
		}
	}
	if !foundTool {
		t.Fatalf("dynamic tool missing from %#v", tools)
	}

	visible := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "unsuppress", map[string]any{"label": "visible"})
	if !visible.OK {
		t.Fatalf("unsuppress = %#v", visible)
	}
	visibleContexts := visible.Value.(map[string]any)["contexts"].([]any)
	if len(visibleContexts) != 1 || visibleContexts[0].(map[string]any)["text"] != "Context visible" {
		t.Fatalf("visible contexts = %#v", visibleContexts)
	}
	session, _ := e.getSession(sessionID)
	agent, err := e.runtimeForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := e.systemPromptForSession(session, session.Model, agent)
	if err != nil || !strings.Contains(prompt, "Dynamic "+session.Model.Model+" in "+e.Config().Workspace+" for runtime.") || !strings.Contains(prompt, "Variable runtime.") {
		t.Fatalf("prompt = %q, %v", prompt, err)
	}
	disposed := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "disposeInputs", map[string]any{"label": "gone"})
	if !disposed.OK {
		t.Fatalf("dispose inputs = %#v", disposed)
	}
	disposedValue := disposed.Value.(map[string]any)
	if len(disposedValue["contexts"].([]any)) != 0 {
		t.Fatalf("contexts after dispose = %#v", disposedValue["contexts"])
	}
	if _, exists := disposedValue["variables"].(map[string]any)["dynamic_value"]; exists {
		t.Fatalf("variable survived dispose = %#v", disposedValue["variables"])
	}
	for _, raw := range disposedValue["tools"].([]any) {
		if strings.HasPrefix(raw.(map[string]any)["name"].(string), "dynamic_schema_") {
			t.Fatalf("tool survived dispose = %#v", raw)
		}
	}
	prompt, err = e.systemPromptForSession(session, session.Model, agent)
	if err != nil || strings.Contains(prompt, "Variable ") {
		t.Fatalf("prompt after input dispose = %q, %v", prompt, err)
	}

	if stopped, stopErr := e.DynamicCordisStop(sessionID, pluginID); stopErr != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, stopErr)
	}
	prompt, err = e.systemPromptForSession(session, session.Model, agent)
	if err != nil || strings.Contains(prompt, "Dynamic ") {
		t.Fatalf("prompt after stop = %q, %v", prompt, err)
	}
}

func TestDynamicCordisSystemPromptChangeFailureRollsBackSection(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-system-prompt-rollback", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "prb", `
let ctxRef
return {
  inject: ['systemPrompt'],
  apply(ctx) {
    ctxRef = ctx
    ctx.on('system-prompt/change', () => { throw new Error('reject prompt change') })
    harness.handle('install', () => ctxRef.systemPrompt.section({
      name: 'dynamic:rollback', order: 5, text: 'must not survive'
    }))
    harness.handle('assemble', () => ctxRef.systemPrompt.assemble())
  }
}`)
	installed := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "install", nil)
	if installed.OK || installed.Code != "handler-error" || !strings.Contains(installed.Message, "reject prompt change") {
		t.Fatalf("install = %#v", installed)
	}
	assembled := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "assemble", nil)
	if !assembled.OK {
		t.Fatalf("assemble = %#v", assembled)
	}
	for _, raw := range assembled.Value.(map[string]any)["sections"].([]any) {
		if raw.(map[string]any)["name"] == "dynamic:rollback" {
			t.Fatalf("failed prompt change leaked section: %#v", raw)
		}
	}
}

func TestDynamicCordisSystemPromptAssembleWaterfall(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-system-prompt-waterfall", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "prw", `
let expectedContext
const seen = []
return {
  inject: ['systemPrompt'],
  apply(ctx) {
    ctx.systemPrompt.section({ name: 'dynamic:base', order: 5, text: 'base' })
    ctx.on('system-prompt/assemble', async (assembly, context, next) => {
      if (context !== expectedContext) throw new Error('assemble context identity changed')
      assembly.sections.push({ name: 'dynamic:first', text: context.label })
      const transformed = await next()
      transformed.variables.after = 'first'
      return transformed
    })
    ctx.on('system-prompt/assemble', async (assembly, context, next) => {
      seen.push(assembly.sections.map(section => section.name))
      if (context.short) {
        return { sections: [{ name: 'short', text: 'only' }], contexts: [], tools: [], variables: { short: 'yes' } }
      }
      return next()
    })
    harness.handle('assemble', args => {
      expectedContext = args
      return ctx.systemPrompt.assemble(args)
    })
    harness.handle('seen', () => seen)
  }
}`)
	runDynamicBuiltinPlugin(t, e, sessionID, "prx", `
return {
  inject: ['systemPrompt'],
  apply(ctx) {
    ctx.on('system-prompt/assemble', async (assembly, context, next) => {
      assembly.sections.push({ name: 'dynamic:cross-run', text: context.label })
      return next()
    })
  }
}`)
	otherSession, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-system-prompt-waterfall-other", "")
	if err != nil {
		t.Fatal(err)
	}
	runDynamicBuiltinPlugin(t, e, otherSession, "pro", `
return {
  inject: ['systemPrompt'],
  apply(ctx) {
    ctx.on('system-prompt/assemble', () => { throw new Error('wrong session listener ran') })
  }
}`)

	assembled := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "assemble", map[string]any{"label": "normal"})
	if !assembled.OK {
		t.Fatalf("assemble = %#v", assembled)
	}
	value := assembled.Value.(map[string]any)
	sections := value["sections"].([]any)
	found, crossRun := false, false
	for _, raw := range sections {
		section := raw.(map[string]any)
		if section["name"] == "dynamic:first" && section["text"] == "normal" {
			found = true
		}
		if section["name"] == "dynamic:cross-run" && section["text"] == "normal" {
			crossRun = true
		}
	}
	if !found || !crossRun || value["variables"].(map[string]any)["after"] != "first" {
		t.Fatalf("waterfall assembly = %#v", value)
	}

	short := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "assemble", map[string]any{"label": "short", "short": true})
	if !short.OK {
		t.Fatalf("short assemble = %#v", short)
	}
	shortValue := short.Value.(map[string]any)
	shortSections := shortValue["sections"].([]any)
	if len(shortSections) != 1 || shortSections[0].(map[string]any)["name"] != "short" || shortValue["variables"].(map[string]any)["after"] != "first" {
		t.Fatalf("short assembly = %#v", shortValue)
	}
	seen := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "seen", nil)
	if !seen.OK || len(seen.Value.([]any)) != 2 {
		t.Fatalf("listener order observations = %#v", seen)
	}
}

func TestDynamicCordisSystemPromptCompleteRestoredAfterWaterfall(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-system-prompt-complete", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "prc", `
return {
  inject: ['systemPrompt'],
  apply(ctx) {
    ctx.systemPrompt.section({ name: 'dynamic:complete', order: 5, text: 'Exact prompt.', complete: true })
    ctx.systemPrompt.section({ name: 'dynamic:extra', order: 6, text: 'extra' })
    ctx.on('system-prompt/assemble', async (assembly, _context, next) => {
      assembly.sections.find(section => section.name === 'dynamic:complete').text = 'mutated'
      assembly.sections.push({ name: 'dynamic:late', text: 'late' })
      const transformed = await next()
      transformed.sections.push({ name: 'dynamic:after', text: 'after' })
      return transformed
    })
    harness.handle('assemble', () => ctx.systemPrompt.assemble())
  }
}`)
	assembled := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "assemble", nil)
	if !assembled.OK {
		t.Fatalf("assemble = %#v", assembled)
	}
	sections := assembled.Value.(map[string]any)["sections"].([]any)
	if len(sections) != 1 || sections[0].(map[string]any)["name"] != "dynamic:complete" || sections[0].(map[string]any)["text"] != "Exact prompt." {
		t.Fatalf("complete sections = %#v", sections)
	}
}

func TestDynamicCordisSystemPromptWaterfallDrivesAgentRequests(t *testing.T) {
	provider := &promptCaptureProvider{}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.Provider, cfg.Model = provider.ID(), "model-a"
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	sessionID, err := e.CreateSession(t.Context(), cfg.Workspace, "cordis-agent-waterfall", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "pra", `
let label = 'first'
return {
  inject: ['systemPrompt'],
  apply(ctx) {
    ctx.systemPrompt.section({ name: 'dynamic:agent', order: 5, text: 'Section {{dynamic_value}}.' })
    ctx.systemPrompt.context({ name: 'dynamic:state', order: 5, text: 'State {{dynamic_value}}.' })
    ctx.systemPrompt.variable('dynamic_value', () => 'provider')
    ctx.systemPrompt.tools(() => ({ schemas: [{
      name: 'dynamic_schema', description: 'Dynamic schema', parameters: { type: 'object', properties: {} }
    }] }))
    ctx.on('system-prompt/assemble', async (assembly, _context, next) => {
      assembly.variables.dynamic_value = label
      assembly.sections.push({ name: 'dynamic:waterfall', text: 'Waterfall {{dynamic_value}}.' })
      return next()
    })
    harness.handle('setLabel', value => { label = value; return label })
  }
}`)

	if _, err := e.Run(t.Context(), sessionID, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "first"}}}); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 1 || !strings.Contains(requests[0].System, "Section first.") || !strings.Contains(requests[0].System, "Waterfall first.") {
		t.Fatalf("first request system = %#v", requests)
	}
	toolFound, contextFound := false, false
	for _, tool := range requests[0].Tools {
		toolFound = toolFound || tool.Name == "dynamic_schema"
	}
	for _, message := range requests[0].Messages {
		contextFound = contextFound || strings.Contains(message.Content, "State first.")
	}
	if !toolFound || !contextFound {
		t.Fatalf("first request tools/messages = %#v / %#v", requests[0].Tools, requests[0].Messages)
	}

	changed := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "setLabel", "second")
	if !changed.OK {
		t.Fatalf("set label = %#v", changed)
	}
	if _, err := e.Run(t.Context(), sessionID, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "second"}}}); err != nil {
		t.Fatal(err)
	}
	requests = provider.snapshot()
	if len(requests) != 2 || !strings.Contains(requests[1].System, "Section second.") || !strings.Contains(requests[1].System, "Waterfall second.") {
		t.Fatalf("second request system = %#v", requests)
	}
	contextSnapshots := 0
	for _, message := range requests[1].Messages {
		if strings.Contains(message.Content, "Current runtime context.") {
			contextSnapshots++
		}
	}
	if contextSnapshots != 2 || !strings.Contains(requests[1].Messages[len(requests[1].Messages)-1].Content, "State second.") {
		t.Fatalf("second request contexts = %#v", requests[1].Messages)
	}
}

func TestDynamicCordisSystemPromptWaterfallCancellationStopsBeforeStep(t *testing.T) {
	provider := &promptCaptureProvider{}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.Provider, cfg.Model = provider.ID(), "model-a"
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	sessionID, err := e.CreateSession(t.Context(), cfg.Workspace, "cordis-agent-waterfall-cancel", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "prk", `
let entered = false
return {
  apply(ctx) {
    ctx.on('system-prompt/assemble', () => {
      entered = true
      return new Promise(() => {})
    })
    harness.handle('entered', () => entered)
  }
}`)

	done := make(chan error, 1)
	go func() {
		_, runErr := e.Run(context.Background(), sessionID, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "cancel"}}})
		done <- runErr
	}()
	deadline := time.Now().Add(2 * time.Second)
	enteredWaterfall := false
	for time.Now().Before(deadline) {
		entered := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "entered", nil)
		if entered.OK && entered.Value == true {
			enteredWaterfall = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !enteredWaterfall {
		t.Fatal("waterfall listener was not entered")
	}
	if err := e.CancelSession(sessionID); err != nil {
		t.Fatal(err)
	}
	select {
	case runErr := <-done:
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf("run error = %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
	if len(provider.snapshot()) != 0 {
		t.Fatalf("provider was called: %#v", provider.snapshot())
	}
	session, _ := e.getSession(sessionID)
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	for _, event := range events {
		if event.Type == "step/start" || event.Type == "assistant/message" {
			t.Fatalf("unexpected event after assembly cancellation: %#v", event)
		}
	}
}

func TestDynamicCordisSystemPromptWaterfallCanClearAgentInput(t *testing.T) {
	provider := &promptCaptureProvider{}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.Provider, cfg.Model = provider.ID(), "model-a"
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterProvider(provider)
	t.Cleanup(func() { _ = e.Close() })
	sessionID, err := e.CreateSession(t.Context(), cfg.Workspace, "cordis-agent-waterfall-empty", "")
	if err != nil {
		t.Fatal(err)
	}
	runDynamicBuiltinPlugin(t, e, sessionID, "pre", `
return { apply(ctx) {
  ctx.on('system-prompt/assemble', async () => ({ sections: [], contexts: [], tools: [], variables: {} }))
} }
`)
	if _, err := e.Run(t.Context(), sessionID, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "empty"}}}); err != nil {
		t.Fatal(err)
	}
	requests := provider.snapshot()
	if len(requests) != 1 || requests[0].System != "" || len(requests[0].Tools) != 0 || len(requests[0].Messages) != 1 {
		t.Fatalf("request = %#v", requests)
	}
}

func TestDynamicCordisSectionShadowsCompletePresetPersona(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-system-prompt-persona-shadow", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, _ := runDynamicBuiltinPlugin(t, e, sessionID, "prs", `
return {
  inject: ['systemPrompt'],
  apply(ctx) {
    ctx.systemPrompt.section({ name: 'deployment:persona-prefix', order: 0, text: 'Scoped persona.' })
  }
}`)
	e.dynamicCordis.RLock()
	run := e.dynamicCordis.plugins[pluginID].run
	e.dynamicCordis.RUnlock()
	session, _ := e.getSession(sessionID)
	sections, err := e.resolvedSystemPromptSections(session, session.Model, agentRuntime{persona: "Complete deployment.", completePersona: true}, run)
	if err != nil {
		t.Fatal(err)
	}
	if len(sections) < 2 || sections[0].Name != "harness:identity" || sections[1].Name != "deployment:persona-prefix" || sections[1].Text != "Scoped persona." || sections[1].Complete {
		t.Fatalf("shadowed complete persona = %#v", sections)
	}
}

func TestDynamicCordisFSFacadeUsesWorkspacePolicyAndAtomicHelpers(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "note.txt"), []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	streamText := strings.Repeat("界", 12000)
	if err := os.WriteFile(filepath.Join(workspace, "stream.txt"), []byte(streamText), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "binary.bin"), []byte{0x89, 0, 0xff, 0x47}, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("note.txt", filepath.Join(workspace, "note-link")); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), workspace, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	sessionID, err := e.CreateSession(t.Context(), workspace, "cordis-fs", "")
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(string(filepath.Separator), "etc", "dsh-cordis-denied.txt")
	code := `
return {
  inject: ['fs'],
  apply(ctx) {
	    harness.handle('exercise', async () => {
	      const note = await ctx.fs.resolve('note.txt')
	      const stat = await ctx.fs.stat(note)
	      const text = await ctx.fs.readText(note)
	      const root = await ctx.fs.resolve('.')
	      const stream = await ctx.fs.resolve('stream.txt')
	      let streamed = ''
	      const iterator = await ctx.fs.streamText(stream)
	      if (iterator[Symbol.asyncIterator]() !== iterator) throw new Error('stream iterator identity')
	      while (true) {
	        const item = await iterator.next()
	        if (item.done) break
	        streamed += item.value
	      }
	      const binary = await ctx.fs.resolve('binary.bin')
	      const bytes = Array.from(await ctx.fs.readBytes(binary, undefined, 4))
	      let tooLarge = ''
	      let tooLargeCode
	      try { await ctx.fs.readBytes(binary, undefined, 3) } catch (error) { tooLarge = String(error); tooLargeCode = error.code }
	      let binaryText = ''
	      let binaryCode
	      try {
	        const binaryIterator = await ctx.fs.streamText(binary)
	        while (true) {
	          const item = await binaryIterator.next()
	          if (item.done) break
	          binaryText += item.value
	        }
	      } catch (error) { binaryText = String(error); binaryCode = error.code }
	      const link = await ctx.fs.lstat('note-link')
	      const missing = await ctx.fs.lstat('missing')
	      const output = await ctx.fs.resolve('output.txt')
	      const wrote = await ctx.fs.writeText(output, 'first', { kind: 'createIfAbsent' })
	      const edited = await ctx.fs.editText(output, { oldString: 'first', newString: 'second', replaceAll: false }, { version: wrote.version })
	      const names = (await ctx.fs.listDir(root)).map(entry => entry.name)
      let denied = ''
      let deniedCode
      try {
        const target = await ctx.fs.resolve(` + "`" + outside + "`" + `)
        await ctx.fs.writeText(target, 'escape')
      } catch (error) {
        denied = String(error)
        deniedCode = error.code
      }
	      return {
	        text, type: stat.type, wrote, edited, names, denied, deniedCode, streamed, bytes,
	        tooLarge, tooLargeCode, binaryText, binaryCode,
	        processPath: ctx.fs.processPath(note), fileUrl: ctx.fs.fileUrl(note),
	        contains: ctx.fs.contains(root, note), reverseContains: ctx.fs.contains(note, root),
	        linkType: link.type, missing: missing === undefined
	      }
	    })
  }
}`
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "fsv", code)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	if value["text"] != "original" || value["type"] != "file" {
		t.Fatalf("read/stat = %#v", value)
	}
	if value["streamed"] != streamText {
		t.Fatalf("streamed text length = %d", len(value["streamed"].(string)))
	}
	bytes := value["bytes"].([]any)
	if fmt.Sprint(bytes) != "[137 0 255 71]" {
		t.Fatalf("read bytes = %#v", bytes)
	}
	if !strings.Contains(value["tooLarge"].(string), "FS_TOO_LARGE") || !strings.Contains(value["binaryText"].(string), "FS_NOT_TEXT") {
		t.Fatalf("bounded/binary reads = %#v / %#v", value["tooLarge"], value["binaryText"])
	}
	if value["tooLargeCode"] != "FS_TOO_LARGE" || value["binaryCode"] != "FS_NOT_TEXT" {
		t.Fatalf("bounded/binary codes = %#v / %#v", value["tooLargeCode"], value["binaryCode"])
	}
	if value["processPath"] != filepath.Join(workspace, "note.txt") || value["fileUrl"] != lspFileURI(filepath.Join(workspace, "note.txt")) {
		t.Fatalf("path projections = %#v / %#v", value["processPath"], value["fileUrl"])
	}
	if value["contains"] != true || value["reverseContains"] != false || value["linkType"] != "symlink" || value["missing"] != true {
		t.Fatalf("containment/lstat = %#v", value)
	}
	edited := value["edited"].(map[string]any)
	if edited["before"] != "first" || edited["after"] != "second" {
		t.Fatalf("edit = %#v", edited)
	}
	if denied, _ := value["denied"].(string); !strings.Contains(denied, "FS_SANDBOX_DENIED") {
		t.Fatalf("outside write = %q", denied)
	}
	if value["deniedCode"] != "FS_SANDBOX_DENIED" {
		t.Fatalf("outside write code = %#v", value["deniedCode"])
	}
	if data, readErr := os.ReadFile(filepath.Join(workspace, "output.txt")); readErr != nil || string(data) != "second" {
		t.Fatalf("output = %q, %v", data, readErr)
	}
	if _, statErr := os.Stat(outside); !os.IsNotExist(statErr) {
		t.Fatalf("outside file exists: %v", statErr)
	}
}

func TestDynamicCordisFSFacadeHonorsAbortSignals(t *testing.T) {
	workspace := t.TempDir()
	path := filepath.Join(workspace, "note.txt")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 96*1024)), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), workspace, false
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	sessionID, err := e.CreateSession(t.Context(), workspace, "cordis-fs-abort", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "fsa", `
return {
  inject: ['fs'],
  apply(ctx) {
    harness.handle('exercise', async () => {
      const aborted = { aborted: true }
	      const errors = {}
	      const codes = {}
	      const capture = async (name, fn) => {
	        try { await fn() } catch (error) { errors[name] = String(error); codes[name] = error.code }
      }
      await capture('resolve', () => ctx.fs.resolve('note.txt', { signal: aborted }))
      const resolveSignal = { aborted: false }
      const pendingResolve = ctx.fs.resolve('note.txt', { signal: resolveSignal })
      resolveSignal.aborted = true
      await capture('resolveInFlight', () => pendingResolve)
      const note = await ctx.fs.resolve('note.txt')
      const root = await ctx.fs.resolve('.')
      const output = await ctx.fs.resolve('output.txt')
      await capture('stat', () => ctx.fs.stat(note, aborted))
      await capture('lstat', () => ctx.fs.lstat('note.txt', undefined, aborted))
      const statSignal = { aborted: false }
      const pendingStat = ctx.fs.stat(note, statSignal)
      statSignal.aborted = true
      await capture('statInFlight', () => pendingStat)
      const lstatSignal = { aborted: false }
      const pendingLstat = ctx.fs.lstat('note.txt', undefined, lstatSignal)
      lstatSignal.aborted = true
      await capture('lstatInFlight', () => pendingLstat)
      await capture('readText', () => ctx.fs.readText(note, aborted))
      await capture('readBytes', () => ctx.fs.readBytes(note, aborted, 200000))
      await capture('listDir', () => ctx.fs.listDir(root, aborted))
      await capture('writeText', () => ctx.fs.writeText(output, 'never', undefined, aborted))
      await capture('editText', () => ctx.fs.editText(note, { oldString: 'x', newString: 'y' }, undefined, aborted))
      const live = { aborted: false }
      const stream = await ctx.fs.streamText(note, live)
      const first = await stream.next()
      live.aborted = true
      await capture('streamText', () => stream.next())
	      return { errors, codes, firstDone: first.done }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("abort exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	errors := value["errors"].(map[string]any)
	codes := value["codes"].(map[string]any)
	for _, name := range []string{"resolve", "resolveInFlight", "stat", "statInFlight", "lstat", "lstatInFlight", "readText", "readBytes", "listDir", "writeText", "editText", "streamText"} {
		if !strings.Contains(fmt.Sprint(errors[name]), "FS_ABORTED") {
			t.Fatalf("%s abort = %#v", name, errors[name])
		}
		if codes[name] != "FS_ABORTED" {
			t.Fatalf("%s abort code = %#v", name, codes[name])
		}
	}
	if value["firstDone"] != false {
		t.Fatalf("stream first result = %#v", value)
	}
	if _, err := os.Stat(filepath.Join(workspace, "output.txt")); !os.IsNotExist(err) {
		t.Fatalf("aborted write published output: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || strings.Contains(string(data), "y") {
		t.Fatalf("aborted edit changed input: %v", err)
	}
}

func TestDynamicCordisShellResolveRunAndStop(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", sandboxDangerFull)
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-shell", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "shl", `
	let background
	let longBackground
	return {
	  inject: ['shell'],
	  apply(ctx) {
    harness.handle('run', () => {
      const spec = ctx.shell.resolve({ command: 'printf cordis-shell', timeoutMs: 2000 })
      return ctx.shell.run(spec)
    })
    harness.handle('long', () => ctx.shell.run(ctx.shell.resolve({
	      command: 'printf started > cordis-shell-started; sleep 30',
	      timeoutMs: 60000
	    })))
	    harness.handle('start', () => {
	      background = ctx.shell.start(ctx.shell.resolve({
	        command: 'printf first; sleep 0.2; printf second; printf problem >&2; exit 7',
	        timeoutMs: 1
	      }))
	      return { status: background.status, exitCode: background.exitCode }
	    })
	    harness.handle('read', () => background.readOutput())
	    harness.handle('wait', async () => {
	      await background.done
	      return {
	        status: background.status,
	        exitCode: background.exitCode,
	        signal: background.signal,
	        output: background.readOutput(),
	        sandbox: background.sandbox
	      }
	    })
	    harness.handle('startLong', () => {
	      longBackground = ctx.shell.start(ctx.shell.resolve({ command: 'sleep 30', timeoutMs: 1 }))
	      return longBackground.status
	    })
	    harness.handle('killLong', async () => {
	      const killed = longBackground.kill()
	      await longBackground.done
	      return { killed, secondKill: longBackground.kill(), status: longBackground.status, signal: longBackground.signal }
	    })
	  }
	}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "run", nil)
	if !result.OK {
		t.Fatalf("shell run = %#v", result)
	}
	value := result.Value.(map[string]any)
	stdout := value["stdout"].(map[string]any)
	if value["exitCode"] != float64(0) || stdout["text"] != "cordis-shell" || value["timedOut"] != false {
		t.Fatalf("shell result = %#v", value)
	}
	started := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "start", nil)
	if !started.OK || started.Value.(map[string]any)["status"] != "running" || started.Value.(map[string]any)["exitCode"] != nil {
		t.Fatalf("background start = %#v", started)
	}
	first := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "read", nil)
	if !first.OK {
		t.Fatalf("background first read = %#v", first)
	}
	settled := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "wait", nil)
	if !settled.OK {
		t.Fatalf("background wait = %#v", settled)
	}
	settledValue := settled.Value.(map[string]any)
	if settledValue["status"] != "completed" || settledValue["exitCode"] != float64(7) || settledValue["signal"] != nil {
		t.Fatalf("background outcome = %#v", settledValue)
	}
	combined := first.Value.(map[string]any)["delta"].(string) + settledValue["output"].(map[string]any)["delta"].(string)
	if !strings.Contains(combined, "firstsecond") || !strings.Contains(combined, "[stderr]\nproblem") {
		t.Fatalf("background output = %q", combined)
	}
	if again := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "read", nil); !again.OK || again.Value.(map[string]any)["delta"] != "" {
		t.Fatalf("background repeated output = %#v", again)
	}
	if longStart := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "startLong", nil); !longStart.OK || longStart.Value != "running" {
		t.Fatalf("long background start = %#v", longStart)
	}
	killed := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "killLong", nil)
	if !killed.OK {
		t.Fatalf("long background kill = %#v", killed)
	}
	killedValue := killed.Value.(map[string]any)
	if killedValue["killed"] != true || killedValue["secondKill"] != false || killedValue["status"] != "killed" {
		t.Fatalf("long background outcome = %#v", killedValue)
	}
	longResult := make(chan DynamicCordisInvokeResult, 1)
	go func() { longResult <- e.DynamicCordisInvoke(context.Background(), pluginID, runID, "long", nil) }()
	marker := filepath.Join(e.Config().Workspace, "cordis-shell-started")
	for attempts := 0; attempts < 200; attempts++ {
		if _, statErr := os.Stat(marker); statErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("long command did not start: %v", statErr)
	}
	if stopped, stopErr := e.DynamicCordisStop(sessionID, pluginID); stopErr != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, stopErr)
	}
	if long := <-longResult; !long.OK || long.Value.(map[string]any)["aborted"] != true {
		t.Fatalf("long command after stop = %#v", long)
	}
	if after := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "run", nil); after.Code != "plugin-not-running" {
		t.Fatalf("invoke after stop = %#v", after)
	}
}

func TestDynamicCordisShellSpillPreservesFullOutput(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", sandboxDangerFull)
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-shell-spill", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "shs", `
let background
return {
  inject: ['shell'],
  apply(ctx) {
    harness.handle('foreground', () => ctx.shell.run(ctx.shell.resolve({
      command: "printf HEAD; printf '%0200d' 0; printf TAIL",
      stdoutMaxBytes: 32,
      timeoutMs: 5000
    })))
    harness.handle('background', async () => {
      background = ctx.shell.start(ctx.shell.resolve({ command: "printf '%01048600d' 0" }))
      await background.done
      return background.readOutput()
    })
  }
}`)
	foreground := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "foreground", nil)
	if !foreground.OK {
		t.Fatalf("foreground spill = %#v", foreground)
	}
	stdout := foreground.Value.(map[string]any)["stdout"].(map[string]any)
	path, _ := stdout["spillPath"].(string)
	full := "HEAD" + strings.Repeat("0", 200) + "TAIL"
	stored, err := os.ReadFile(path)
	if err != nil || string(stored) != full || stdout["truncated"] != true || !strings.HasSuffix(stdout["text"].(string), "TAIL") || strings.HasPrefix(stdout["text"].(string), "HEAD") {
		t.Fatalf("foreground output = %#v, stored=%d, err=%v", stdout, len(stored), err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("foreground spill permissions = %v, %v", info, err)
	}
	if info, err := os.Stat(filepath.Dir(path)); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("spill directory permissions = %v, %v", info, err)
	}

	background := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "background", nil)
	if !background.OK {
		t.Fatalf("background spill = %#v", background)
	}
	read := background.Value.(map[string]any)
	backgroundPath, _ := read["stdoutSpillPath"].(string)
	backgroundInfo, err := os.Stat(backgroundPath)
	if err != nil || backgroundInfo.Size() != 1048600 || read["lossy"] != true || len(read["delta"].(string)) != toolOutputLimit {
		t.Fatalf("background output = %#v, info=%v, err=%v", read, backgroundInfo, err)
	}
}
