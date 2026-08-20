package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCordisPresetSelectsDynamicTools(t *testing.T) {
	presets := t.TempDir()
	for name, composition := range map[string]string{
		"standard": "- id: bash\n  name: '@deepseek-ai/dsh-tool-bash'\n",
		"cordis":   "- id: cordis\n  name: '@deepseek-ai/dsh-tool-cordis'\n",
	} {
		dir := filepath.Join(presets, name)
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir, cfg.Workspace, cfg.PresetDir, cfg.Persist = t.TempDir(), t.TempDir(), presets, false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	cordisID, err := e.CreateSession(context.Background(), cfg.Workspace, "cordis-tools", "cordis")
	if err != nil {
		t.Fatal(err)
	}
	standardID, err := e.CreateSession(context.Background(), cfg.Workspace, "standard-tools", "standard")
	if err != nil {
		t.Fatal(err)
	}

	visible := func(sessionID string) map[string]bool {
		session, getErr := e.getSession(sessionID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		rows, listErr := e.toolsForSession(session)
		if listErr != nil {
			t.Fatal(listErr)
		}
		out := map[string]bool{}
		for _, row := range rows {
			out[row.Name] = true
		}
		return out
	}
	cordisTools, standardTools := visible(cordisID), visible(standardID)
	for _, name := range dynamicCordisToolNames {
		if !cordisTools[name] {
			t.Errorf("cordis preset is missing %s", name)
		}
		if standardTools[name] {
			t.Errorf("standard preset unexpectedly exposes %s", name)
		}
	}
}

func TestDynamicCordisModelToolsRunHostHandler(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-model-tools", "")
	if err != nil {
		t.Fatal(err)
	}
	defined := executeRegisteredTool(t, e, "cordis_define", sessionID, map[string]any{
		"plugin": map[string]any{"kind": "new", "idPrefix": "dyn"},
		"name":   "doubler", "purpose": "double a number",
		"code": map[string]any{"host": "harness.handle('double', async (args) => args.value * 2)\nreturn { apply(ctx) {} }"},
	})
	receipt, ok := defined.Value.(DynamicCordisDefineReceipt)
	if !ok {
		t.Fatalf("cordis_define value = %#v", defined.Value)
	}
	running := executeRegisteredTool(t, e, "cordis_run", sessionID, map[string]any{
		"pluginId": receipt.PluginID, "packageId": receipt.PackageID, "mode": "run",
	})
	run, ok := running.Value.(DynamicCordisRunResponse)
	if !ok || !run.OK || run.Status != "running" {
		t.Fatalf("cordis_run value = %#v", running.Value)
	}
	invoked := e.DynamicCordisInvoke(context.Background(), receipt.PluginID, run.PluginRunID, "double", map[string]any{"value": 21})
	if !invoked.OK || invoked.Value != float64(42) && invoked.Value != int64(42) && invoked.Value != 42 {
		t.Fatalf("invoke = %#v", invoked)
	}
	inspected := executeRegisteredTool(t, e, "cordis_inspect_self", sessionID, map[string]any{
		"pluginId": receipt.PluginID, "packageId": receipt.PackageID,
	})
	if inspected.Value == nil {
		t.Fatal("cordis_inspect_self returned no structured value")
	}
	executeRegisteredTool(t, e, "cordis_stop", sessionID, map[string]any{"pluginId": receipt.PluginID})
	if result := e.DynamicCordisInvoke(context.Background(), receipt.PluginID, run.PluginRunID, "double", nil); result.Code != "plugin-not-running" {
		t.Fatalf("invoke after stop = %#v", result)
	}
	executeRegisteredTool(t, e, "cordis_undefine", sessionID, map[string]any{"pluginId": receipt.PluginID})
}

func TestDynamicCordisHostToolRegistrationAndLifecycle(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-dynamic-tool-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	other, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-dynamic-tool-other", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "tool"},
		Name:      "reverse tool",
		Purpose:   "register a model-visible tool",
		Code: DynamicCordisCode{Host: `
await Promise.resolve()
return {
	async apply(ctx) {
		await Promise.resolve()
    harness.registerTool(ctx, harness.defineTool({
      name: 'reverse_text',
      description: 'Reverse text.',
      parameters: { text: { type: 'string', required: true } },
      output: {
        schema: { type: 'string' },
        render(_args, value) { return [{ type: 'text', text: value }] },
        presentationMeta(args, value) { return { input: args.text, output: value } },
      },
      async execute(args) { return args.text.split('').reverse().join('') },
    }))
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.DynamicCordisRun(context.Background(), owner, receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}

	requireVisible := func(sessionID string, want bool) {
		t.Helper()
		session, getErr := e.getSession(sessionID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		rows, listErr := e.toolsForSession(session)
		if listErr != nil {
			t.Fatal(listErr)
		}
		found := false
		for _, row := range rows {
			found = found || row.Name == "reverse_text"
		}
		if found != want {
			t.Fatalf("reverse_text visibility for %s = %v, want %v", sessionID, found, want)
		}
	}
	requireVisible(owner, true)
	requireVisible(other, false)

	e.mu.RLock()
	tool := e.tools["reverse_text"]
	e.mu.RUnlock()
	arguments, _ := json.Marshal(map[string]any{"text": "abc"})
	result, err := tool.Execute(context.Background(), ToolCall{ID: "dynamic-call", Name: "reverse_text", SessionID: owner, Arguments: arguments})
	if err != nil || toolResultText(result) != "cba" || result.Value != "cba" {
		t.Fatalf("dynamic tool result = %#v, %v", result, err)
	}
	meta, _ := result.Meta.(map[string]any)
	if meta["input"] != "abc" || meta["output"] != "cba" {
		t.Fatalf("dynamic tool presentation meta = %#v", result.Meta)
	}
	if _, err := tool.Execute(context.Background(), ToolCall{ID: "foreign-call", Name: "reverse_text", SessionID: other, Arguments: arguments}); err == nil {
		t.Fatal("dynamic tool executed outside its owning session")
	}

	updated, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "existing", PluginID: receipt.PluginID},
		Name:      "uppercase tool",
		Purpose:   "replace a model-visible tool",
		Code: DynamicCordisCode{Host: `
return {
  apply(ctx) {
    harness.registerTool(ctx, harness.defineTool({
      name: 'reverse_text',
      description: 'Uppercase text.',
      parameters: { text: { type: 'string', required: true } },
      output: {
        schema: { type: 'string' },
        render(_args, value) { return [{ type: 'text', text: value }] },
      },
      async execute(args) { return args.text.toUpperCase() },
    }))
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	updateRun, err := e.DynamicCordisRun(context.Background(), owner, updated.PluginID, updated.PackageID, "update")
	if err != nil || !updateRun.OK {
		t.Fatalf("update = %#v, %v", updateRun, err)
	}
	e.mu.RLock()
	tool = e.tools["reverse_text"]
	e.mu.RUnlock()
	result, err = tool.Execute(context.Background(), ToolCall{ID: "updated-call", Name: "reverse_text", SessionID: owner, Arguments: arguments})
	if err != nil || toolResultText(result) != "ABC" {
		t.Fatalf("updated dynamic tool result = %#v, %v", result, err)
	}
	receipt.PackageID = updated.PackageID

	if stopped, err := e.DynamicCordisStop(owner, receipt.PluginID); err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	e.mu.RLock()
	_, exists := e.tools["reverse_text"]
	e.mu.RUnlock()
	if exists {
		t.Fatal("dynamic tool survived stop")
	}
	if rerun, err := e.DynamicCordisRun(context.Background(), owner, receipt.PluginID, receipt.PackageID, "run"); err != nil || !rerun.OK {
		t.Fatalf("rerun = %#v, %v", rerun, err)
	}
	requireVisible(owner, true)
	if removed, err := e.DynamicCordisUndefine(owner, receipt.PluginID); err != nil || !removed.OK {
		t.Fatalf("undefine = %#v, %v", removed, err)
	}
	e.mu.RLock()
	_, exists = e.tools["reverse_text"]
	e.mu.RUnlock()
	if exists {
		t.Fatal("dynamic tool survived undefine")
	}
}

func TestDynamicCordisProvideInjectLifecycle(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-services", "")
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "cons"},
		Name:      "consumer",
		Purpose:   "consume a dynamic service",
		Code: DynamicCordisCode{Host: `
return {
  inject: ['greeter', 'tools'],
  apply(ctx) {
    harness.registerTool(ctx, harness.defineTool({
      name: 'greet_dynamic',
      description: 'Use the injected greeter.',
      parameters: { name: { type: 'string', required: true } },
      output: {
        schema: { type: 'string' },
        render(_args, value) { return [{ type: 'text', text: value }] },
      },
      execute(args) { return ctx.greeter.greet(args.name) },
    }))
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	consumerRun, err := e.DynamicCordisRun(t.Context(), owner, consumer.PluginID, consumer.PackageID, "run")
	if err != nil || !consumerRun.OK || len(consumerRun.WaitingFor) != 1 || consumerRun.WaitingFor[0] != "greeter" {
		t.Fatalf("consumer run = %#v, %v", consumerRun, err)
	}
	if tool := dynamicRegisteredTool(e, "greet_dynamic"); tool.Execute != nil {
		t.Fatal("waiting consumer registered its tool")
	}

	provider, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "prov"},
		Name:      "provider",
		Purpose:   "provide a dynamic service",
		Code: DynamicCordisCode{Host: `
return {
  apply(ctx) {
    ctx.provide('greeter', { greet(name) { return 'hi ' + name } })
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	providerRun, err := e.DynamicCordisRun(t.Context(), owner, provider.PluginID, provider.PackageID, "run")
	if err != nil || !providerRun.OK {
		t.Fatalf("provider run = %#v, %v", providerRun, err)
	}
	result := executeRegisteredTool(t, e, "greet_dynamic", owner, map[string]any{"name": "harness"})
	if got := toolResultText(result); got != "hi harness" {
		t.Fatalf("greet result = %q", got)
	}

	if stopped, err := e.DynamicCordisStop(owner, provider.PluginID); err != nil || !stopped.OK {
		t.Fatalf("stop provider = %#v, %v", stopped, err)
	}
	if tool := dynamicRegisteredTool(e, "greet_dynamic"); tool.Execute != nil {
		t.Fatal("consumer tool survived provider stop")
	}
	providerRun, err = e.DynamicCordisRun(t.Context(), owner, provider.PluginID, provider.PackageID, "run")
	if err != nil || !providerRun.OK {
		t.Fatalf("restart provider = %#v, %v", providerRun, err)
	}
	result = executeRegisteredTool(t, e, "greet_dynamic", owner, map[string]any{"name": "again"})
	if got := toolResultText(result); got != "hi again" {
		t.Fatalf("greet after restart = %q", got)
	}
}

func TestDynamicCordisSandboxGlobalsAndTimerService(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-sandbox-runtime", "")
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "late"},
		Name:      "late consumer",
		Purpose:   "wait for a timer-provided service",
		Code: DynamicCordisCode{Host: `
const codec = new TextDecoder().decode(new TextEncoder().encode(atob(btoa('ready'))))
return {
  inject: ['lateValue', 'tools'],
  apply(ctx) {
    harness.registerTool(ctx, harness.defineTool({
      name: 'late_dynamic',
      description: 'Read a delayed service.',
      parameters: {},
      output: {
        schema: { type: 'string' },
        render(_args, value) { return [{ type: 'text', text: value }] },
      },
      execute() { return codec + ':' + ctx.lateValue },
    }))
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run, err := e.DynamicCordisRun(t.Context(), owner, consumer.PluginID, consumer.PackageID, "run"); err != nil || !run.OK || len(run.WaitingFor) != 1 {
		t.Fatalf("consumer run = %#v, %v", run, err)
	}
	provider, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "tick"},
		Name:      "timer provider",
		Purpose:   "provide after a timer fires",
		Code: DynamicCordisCode{Host: `
return {
  inject: ['timer'],
  apply(ctx) { ctx.setTimeout(() => ctx.provide('lateValue', 7), 10) },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run, err := e.DynamicCordisRun(t.Context(), owner, provider.PluginID, provider.PackageID, "run"); err != nil || !run.OK {
		t.Fatalf("provider run = %#v, %v", run, err)
	}
	deadline := time.Now().Add(time.Second)
	for dynamicRegisteredTool(e, "late_dynamic").Execute == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	result := executeRegisteredTool(t, e, "late_dynamic", owner, map[string]any{})
	if got := toolResultText(result); got != "ready:7" {
		t.Fatalf("late result = %q", got)
	}
}

func TestDynamicCordisEventListenersAndTimerWrappersDispose(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-events", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "event"},
		Name:      "event listener",
		Purpose:   "exercise disposable event and timer helpers",
		Code: DynamicCordisCode{Host: `
globalThis.__changes = 0
globalThis.__throttled = 0
globalThis.__debounced = 0
let throttleHit
let debounceHit
harness.handle('hit', () => { throttleHit(); throttleHit(); debounceHit(); debounceHit(); return null })
harness.handle('counts', () => ({ changes: globalThis.__changes, throttled: globalThis.__throttled, debounced: globalThis.__debounced }))
return {
  inject: ['timer'],
  apply(ctx) {
    ctx.on('tools/change', () => { globalThis.__changes += 1 })
    ctx.once('tools/change', () => { globalThis.__changes += 100 })
    throttleHit = ctx.throttle(() => { globalThis.__throttled += 1 }, 20)
    debounceHit = ctx.debounce(() => { globalThis.__debounced += 1 }, 20)
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run, runErr := e.DynamicCordisRun(t.Context(), owner, receipt.PluginID, receipt.PackageID, "run"); runErr != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, runErr)
	}
	probe := Tool{Schema: ToolSchema{Name: "cordis_event_probe", Parameters: objectSchema(map[string]any{})}, Execute: func(context.Context, ToolCall) (ToolResult, error) {
		return textToolResult("ok"), nil
	}}
	if err := e.RegisterTool(probe); err != nil {
		t.Fatal(err)
	}
	if !e.UnregisterTool(probe.Schema.Name) {
		t.Fatal("probe tool was not unregistered")
	}
	if result := e.DynamicCordisInvoke(t.Context(), receipt.PluginID, runIDForPlugin(t, e, receipt.PluginID), "hit", nil); !result.OK {
		t.Fatalf("hit = %#v", result)
	}
	deadline := time.Now().Add(time.Second)
	var counts map[string]any
	for time.Now().Before(deadline) {
		result := e.DynamicCordisInvoke(t.Context(), receipt.PluginID, runIDForPlugin(t, e, receipt.PluginID), "counts", nil)
		counts, _ = result.Value.(map[string]any)
		if counts["changes"] == float64(102) && counts["throttled"] == float64(2) && counts["debounced"] == float64(1) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if counts["changes"] != float64(102) || counts["throttled"] != float64(2) || counts["debounced"] != float64(1) {
		t.Fatalf("counts = %#v", counts)
	}

	e.dynamicCordis.RLock()
	run := e.dynamicCordis.plugins[receipt.PluginID].run
	e.dynamicCordis.RUnlock()
	if stopped, stopErr := e.DynamicCordisStop(owner, receipt.PluginID); stopErr != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, stopErr)
	}
	run.eventMu.RLock()
	listeners := len(run.listeners)
	run.eventMu.RUnlock()
	if listeners != 0 {
		t.Fatalf("listeners survived stop: %d", listeners)
	}
}

func TestDynamicCordisTimerPromiseAndAsyncIterator(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-async-timer", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "async"},
		Name:      "async timer",
		Purpose:   "exercise promise and iterator timer overloads",
		Code: DynamicCordisCode{Host: `
let timer
harness.handle('wait', async ({ delay }) => { await timer.timeout(delay); return 'done' })
harness.handle('ticks', async () => {
  let count = 0
  for await (const _ of timer.interval(2)) {
    count += 1
    if (count === 3) break
  }
  return count
})
return { inject: ['timer'], apply(ctx) { timer = ctx } }
`},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.DynamicCordisRun(t.Context(), owner, receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	started := time.Now()
	waited := e.DynamicCordisInvoke(t.Context(), receipt.PluginID, run.PluginRunID, "wait", map[string]any{"delay": 10})
	if !waited.OK || waited.Value != "done" || time.Since(started) < 8*time.Millisecond {
		t.Fatalf("wait = %#v in %s", waited, time.Since(started))
	}
	ticks := e.DynamicCordisInvoke(t.Context(), receipt.PluginID, run.PluginRunID, "ticks", nil)
	if !ticks.OK || ticks.Value != float64(3) {
		t.Fatalf("ticks = %#v", ticks)
	}
}

func TestDynamicCordisToolsChangeIsOrderedAndRollsBack(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-event-rollback", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "order"},
		Name:      "ordered event",
		Purpose:   "preserve synchronous Cordis event semantics",
		Code: DynamicCordisCode{Host: `
let ctxRef
const calls = []
const tool = harness.defineTool({
  name: 'rollback_dynamic', description: 'rollback probe', parameters: {},
  output: { schema: { type: 'null' }, render() { return [] } },
  execute() { return null },
})
harness.handle('calls', () => calls)
harness.handle('install', () => ctxRef.tools.register(tool))
return {
  inject: ['tools'],
  apply(ctx) {
    ctxRef = ctx
    ctx.on('tools/change', () => { calls.push('first') })
    ctx.on('tools/change', () => { calls.push('second'); throw new Error('boom change listener') })
  },
}
`},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.DynamicCordisRun(t.Context(), owner, receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	installed := e.DynamicCordisInvoke(t.Context(), receipt.PluginID, run.PluginRunID, "install", nil)
	if installed.OK || installed.Code != "handler-error" || !strings.Contains(installed.Message, "boom change listener") {
		t.Fatalf("install = %#v", installed)
	}
	for _, schema := range e.ListTools() {
		if schema.Name == "rollback_dynamic" {
			t.Fatal("failed tools/change leaked the dynamic tool")
		}
	}
	calls := e.DynamicCordisInvoke(t.Context(), receipt.PluginID, run.PluginRunID, "calls", nil)
	want := []any{"first", "second"}
	if !calls.OK || !reflect.DeepEqual(calls.Value, want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}
}

func TestDynamicCordisHandlerDisposerAndFailureSteering(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-handler-failure", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "handle"},
		Name:      "handler failure",
		Purpose:   "dispose handlers and steer runtime failures",
		Code: DynamicCordisCode{Host: `
const off = harness.handle('gone', () => 1)
off()
harness.handle('fail', async () => { throw new Error('handler exploded') })
return { apply(ctx) {} }
`},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.DynamicCordisRun(t.Context(), owner, receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	session, _ := e.getSession(owner)
	session.mu.Lock()
	session.Running = true
	session.mu.Unlock()
	if gone := e.DynamicCordisInvoke(t.Context(), receipt.PluginID, run.PluginRunID, "gone", nil); gone.Code != "method-not-found" {
		t.Fatalf("gone = %#v", gone)
	}
	for range 2 {
		failed := e.DynamicCordisInvoke(t.Context(), receipt.PluginID, run.PluginRunID, "fail", nil)
		if failed.OK || failed.Code != "handler-error" || failed.Message != "handler exploded" || failed.Stack == "" {
			t.Fatalf("fail = %#v", failed)
		}
	}
	session.mu.Lock()
	steering := append([]*queuedPrompt(nil), session.steering...)
	session.mu.Unlock()
	if len(steering) != 1 || !strings.Contains(steering[0].text, `host.call("fail")`) || !strings.Contains(steering[0].text, "handler exploded") {
		t.Fatalf("steering = %#v", steering)
	}
}

func runIDForPlugin(t *testing.T, e *Engine, pluginID string) string {
	t.Helper()
	e.dynamicCordis.RLock()
	defer e.dynamicCordis.RUnlock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.run == nil {
		t.Fatalf("plugin %s is not running", pluginID)
	}
	return plugin.run.runID
}

func TestDynamicCordisVMTimeoutAndParseTeaching(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.DynamicCordisVMTimeout = 20 * time.Millisecond
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner, err := e.CreateSession(t.Context(), cfg.Workspace, "cordis-timeout", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "limit"},
		Name: "timeout", Purpose: "bound synchronous evaluation", Code: DynamicCordisCode{Host: `while (true) {}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	run, err := e.DynamicCordisRun(t.Context(), owner, receipt.PluginID, receipt.PackageID, "run")
	if err != nil || run.OK || !strings.Contains(run.Message, "timed out") || time.Since(started) > time.Second {
		t.Fatalf("timeout run = %#v, %v in %s", run, err, time.Since(started))
	}

	for _, test := range []struct {
		name string
		code string
		want string
	}{
		{name: "typescript", code: `return { name: 'x' as const, apply(ctx) {} }`, want: "plain JavaScript, not TypeScript"},
		{name: "bracket", code: "return {\n  apply(ctx) {}\n});", want: "Check bracket balance"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, defineErr := e.DynamicCordisDefine(DynamicCordisDefineRequest{
				SessionID: owner, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "parse"},
				Name: test.name, Purpose: "parse teaching", Code: DynamicCordisCode{Host: test.code},
			})
			if defineErr == nil || !strings.Contains(defineErr.Error(), test.want) || !strings.Contains(defineErr.Error(), "^") {
				t.Fatalf("parse error = %v; want %q and caret", defineErr, test.want)
			}
		})
	}
}

type dynamicCordisWebProvider struct{}

func (dynamicCordisWebProvider) ID() string      { return "dynamic-test" }
func (dynamicCordisWebProvider) Available() bool { return true }
func (dynamicCordisWebProvider) Search(context.Context, WebSearchRequest) (WebSearchResult, error) {
	return WebSearchResult{Content: "answer", Sources: []WebSearchSource{{URL: "https://one.example"}, {URL: "https://two.example"}}}, nil
}
func (dynamicCordisWebProvider) Fetch(context.Context, WebFetchRequest) (WebFetchResult, error) {
	return WebFetchResult{URL: "https://fetch.example/final", StatusCode: 200, Body: WebFetchBody{Kind: "text", Content: "fetched"}}, nil
}

func TestDynamicCordisWebServiceCallsGoProviders(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := dynamicCordisWebProvider{}
	if err := e.RegisterWebSearchProvider(provider); err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterWebFetchProvider(provider); err != nil {
		t.Fatal(err)
	}
	e.cfg.WebSearchProvider = provider.ID()
	e.cfg.WebFetchProvider = provider.ID()
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-web-service", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "websvc"},
		Name:      "web service consumer",
		Purpose:   "call Go web providers",
		Code: DynamicCordisCode{Host: `
return {
  inject: ['web', 'tools'],
  apply(ctx) {
    harness.registerTool(ctx, harness.defineTool({
      name: 'dynamic_web_probe',
      description: 'Call the injected web service.',
      parameters: {},
      output: { schema: { type: 'string' }, render(_args, value) { return [{ type: 'text', text: value }] } },
      async execute() {
        const search = await ctx.web.search({ query: 'harness', maxResults: 1 })
        const fetched = await ctx.web.fetch({ url: 'https://fetch.example' })
        return JSON.stringify({ answer: search.content, sources: search.sources.length, truncated: search.truncated, body: fetched.body.content })
      },
    }))
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run, err := e.DynamicCordisRun(t.Context(), owner, receipt.PluginID, receipt.PackageID, "run"); err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	result := executeRegisteredTool(t, e, "dynamic_web_probe", owner, map[string]any{})
	if got := toolResultText(result); got != `{"answer":"answer","sources":1,"truncated":true,"body":"fetched"}` {
		t.Fatalf("web result = %q", got)
	}
}

func TestDynamicCordisContextFacadeGuards(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{name: "root", body: `return { inject: ['tools'], apply(ctx) { const value = ctx.root } }`, want: `sandbox ctx does not expose "root"`},
		{name: "assignment", body: `return { apply(ctx) { ctx.stash = 1 } }`, want: "sandbox ctx is read-only"},
		{name: "timer injection", body: `return { apply(ctx) { ctx.setTimeout(() => {}, 1) } }`, want: `service "timer" is not injected`},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newIntegrationEngine(t)
			owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-guard-"+test.name, "")
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
				SessionID: owner,
				Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "guard"},
				Name:      test.name,
				Purpose:   "exercise the sandbox facade",
				Code:      DynamicCordisCode{Host: test.body},
			})
			if err != nil {
				t.Fatal(err)
			}
			run, err := e.DynamicCordisRun(t.Context(), owner, receipt.PluginID, receipt.PackageID, "run")
			if err != nil || run.OK || !strings.Contains(run.Message, test.want) {
				t.Fatalf("run = %#v, %v; want %q", run, err, test.want)
			}
		})
	}
}

func dynamicRegisteredTool(e *Engine, name string) Tool {
	e.mu.RLock()
	tool := e.tools[name]
	e.mu.RUnlock()
	return tool
}

func TestDynamicCordisHostToolValidatesOutputAndCleansFailedApply(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-dynamic-tool-validation", "")
	if err != nil {
		t.Fatal(err)
	}
	define := func(name, code string) DynamicCordisDefineReceipt {
		t.Helper()
		receipt, defineErr := e.DynamicCordisDefine(DynamicCordisDefineRequest{
			SessionID: owner, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "guard"},
			Name: name, Purpose: "exercise dynamic tool guards", Code: DynamicCordisCode{Host: code},
		})
		if defineErr != nil {
			t.Fatal(defineErr)
		}
		return receipt
	}
	invalid := define("invalid output", `return { apply(ctx) {
  harness.registerTool(ctx, harness.defineTool({
    name: 'invalid_output', description: 'Return the wrong type.', parameters: {},
    output: { schema: { type: 'string' }, render(_args, value) { return [{ type: 'text', text: String(value) }] } },
    execute() { return 42 },
  }))
} }`)
	if run, runErr := e.DynamicCordisRun(context.Background(), owner, invalid.PluginID, invalid.PackageID, "run"); runErr != nil || !run.OK {
		t.Fatalf("invalid-output package run = %#v, %v", run, runErr)
	}
	e.mu.RLock()
	tool := e.tools["invalid_output"]
	e.mu.RUnlock()
	if tool.Schema.Output["type"] != "string" {
		t.Fatalf("dynamic output schema = %#v", tool.Schema.Output)
	}
	if _, err := tool.Execute(context.Background(), ToolCall{SessionID: owner, Arguments: json.RawMessage(`{}`)}); err == nil || !strings.Contains(err.Error(), "expected string") {
		t.Fatalf("invalid dynamic output error = %v", err)
	}
	_, _ = e.DynamicCordisStop(owner, invalid.PluginID)

	leaking := define("failed apply", `return { apply(ctx) {
  harness.registerTool(ctx, harness.defineTool({
    name: 'leaked_tool', description: 'Must be cleaned up.', parameters: {},
    output: { schema: { type: 'null' }, render() { return [] } }, execute() { return null },
  }))
  throw new Error('apply failed')
} }`)
	if run, runErr := e.DynamicCordisRun(context.Background(), owner, leaking.PluginID, leaking.PackageID, "run"); runErr != nil || run.OK || run.Reason != "host-half-failed" {
		t.Fatalf("failed-apply run = %#v, %v", run, runErr)
	}
	e.mu.RLock()
	_, exists := e.tools["leaked_tool"]
	e.mu.RUnlock()
	if exists {
		t.Fatal("failed dynamic apply leaked a registered tool")
	}
}

func TestCordisInspectQueryUsesForwardedRemoteEvent(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-inspect-query", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SyncInspectManifest([]CordisInspectProviderManifest{{
		ID: "Client", Description: "client facts", Methods: []CordisInspectMethodManifest{{
			Name: "read", Description: "read a fact",
			InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	host := e.SubscribeHost(ctx)
	e.mu.RLock()
	tool := e.tools["cordis_inspect_query"]
	e.mu.RUnlock()
	arguments, _ := json.Marshal(map[string]any{
		"platform": "client", "provider": "Client", "method": "read", "input": map[string]any{},
	})
	type outcome struct {
		result ToolResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, runErr := tool.Execute(ctx, ToolCall{Name: "cordis_inspect_query", SessionID: sessionID, Arguments: arguments})
		done <- outcome{result: result, err: runErr}
	}()

	var requestID string
	select {
	case frame := <-host:
		if frame["type"] != "host/remote-event" || frame["event"] != "cordis/inspect-query" {
			t.Fatalf("inspect event = %#v", frame)
		}
		args, _ := frame["args"].([]any)
		request, _ := args[0].(map[string]any)
		requestID, _ = request["requestId"].(string)
	case <-ctx.Done():
		t.Fatal("inspect query event was not forwarded")
	}
	if ack := e.ResolveInspectQuery(sessionID, requestID, CordisInspectQueryResolution{
		OK: true, Data: map[string]any{"answer": "ok"},
	}); !ack.Accepted {
		t.Fatal("inspect resolution was not accepted")
	}
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		value, _ := got.result.Value.(map[string]any)
		data, _ := value["data"].(map[string]any)
		if data["answer"] != "ok" {
			t.Fatalf("inspect result = %#v", got.result.Value)
		}
	case <-ctx.Done():
		t.Fatal("inspect query did not settle")
	}
}
