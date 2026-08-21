package harness

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestDynamicCordisConcurrentRunsEmit(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-concurrent-events", "")
	if err != nil {
		t.Fatal(err)
	}
	start := func(prefix, toolName string) (string, string) {
		return runDynamicBuiltinPlugin(t, e, sessionID, prefix, `
const tool = harness.defineTool({
  name: '`+toolName+`', description: 'event probe', parameters: {},
  output: { schema: { type: 'null' }, render() { return [] } },
  execute() { return null },
})
let changes = 0
harness.handle('install', () => { ctxRef.tools.register(tool); return null })
harness.handle('count', () => changes)
let ctxRef
return { inject: ['tools'], apply(ctx) { ctxRef = ctx; ctx.on('tools/change', () => { changes += 1 }) } }
`)
	}
	firstPlugin, firstRun := start("cea", "concurrent_event_a")
	secondPlugin, secondRun := start("ceb", "concurrent_event_b")

	var wg sync.WaitGroup
	results := make(chan DynamicCordisInvokeResult, 2)
	for _, item := range []struct{ plugin, run string }{{firstPlugin, firstRun}, {secondPlugin, secondRun}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- e.DynamicCordisInvoke(t.Context(), item.plugin, item.run, "install", nil)
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		if !result.OK {
			t.Fatalf("install = %#v", result)
		}
	}
	for _, item := range []struct{ plugin, run string }{{firstPlugin, firstRun}, {secondPlugin, secondRun}} {
		count := e.DynamicCordisInvoke(t.Context(), item.plugin, item.run, "count", nil)
		if !count.OK || count.Value != float64(2) {
			t.Fatalf("event count = %#v", count)
		}
	}
}

func TestDynamicCordisReciprocalServicePendingPromise(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-reciprocal-service", "")
	if err != nil {
		t.Fatal(err)
	}
	firstPlugin, firstRun := runDynamicBuiltinPlugin(t, e, sessionID, "rsa", `
let ctxRef
harness.handle('roundtrip', ({ value }) => ctxRef.get('beta').back(value))
return {
  inject: ['timer'],
  apply(ctx) {
    ctxRef = ctx
    ctx.provide('alpha', { finish(value) { return ctx.timeout(5).then(() => value + 1) } })
  },
}
`)
	runDynamicBuiltinPlugin(t, e, sessionID, "rsb", `
return {
  inject: ['alpha'],
  apply(ctx) { ctx.provide('beta', { back(value) { return ctx.alpha.finish(value) } }) },
}
`)
	result := e.DynamicCordisInvoke(t.Context(), firstPlugin, firstRun, "roundtrip", map[string]any{"value": 41})
	if !result.OK || result.Value != float64(42) {
		t.Fatalf("roundtrip = %#v", result)
	}
}

func TestDynamicCordisInvokeAndStopPendingPromise(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-invoke-stop", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "ist", `
let timer
let waiting = false
harness.handle('wait', async () => { waiting = true; await timer.timeout(30000); return 'late' })
harness.handle('waiting', () => waiting)
return { inject: ['timer'], apply(ctx) { timer = ctx } }
`)
	result := make(chan DynamicCordisInvokeResult, 1)
	go func() { result <- e.DynamicCordisInvoke(context.Background(), pluginID, runID, "wait", nil) }()
	waitDynamicCordisFlag(t, e, pluginID, runID, "waiting")
	if stopped, err := e.DynamicCordisStop(sessionID, pluginID); err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	select {
	case resolved := <-result:
		if resolved.OK || resolved.Code != "handler-error" {
			t.Fatalf("pending invoke = %#v", resolved)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending invoke did not settle after stop")
	}
}

func TestDynamicCordisBackgroundProcessWaitAndStop(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", sandboxDangerFull)
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-process-stop", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "pst", `
let process
let waiting = false
harness.handle('start', () => {
  process = shell.start(shell.resolve({ command: 'printf ready > cordis-process-stop; sleep 30' }))
  return process.status
})
harness.handle('wait', async () => { waiting = true; await process.done; return process.status })
harness.handle('waiting', () => waiting)
let shell
return { inject: ['shell'], apply(ctx) { shell = ctx.shell } }
`)
	if started := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "start", nil); !started.OK || started.Value != "running" {
		t.Fatalf("start = %#v", started)
	}
	marker := filepath.Join(e.Config().Workspace, "cordis-process-stop")
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("process did not start: %v", err)
	}
	result := make(chan DynamicCordisInvokeResult, 1)
	go func() { result <- e.DynamicCordisInvoke(context.Background(), pluginID, runID, "wait", nil) }()
	waitDynamicCordisFlag(t, e, pluginID, runID, "waiting")
	started := time.Now()
	if stopped, err := e.DynamicCordisStop(sessionID, pluginID); err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("stop blocked for %s", time.Since(started))
	}
	select {
	case resolved := <-result:
		if resolved.OK || resolved.Code != "handler-error" {
			t.Fatalf("process wait = %#v", resolved)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("process wait did not settle after stop")
	}
}

func TestDynamicCordisDependencyReactivationKeepsHandler(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-handler-reactivation", "")
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "rhc"},
		Name: "consumer", Purpose: "retain handlers", Code: DynamicCordisCode{Host: `
let value
harness.handle('read', () => value)
return { inject: ['reactive'], apply(ctx) { value = ctx.reactive.value } }
`},
	})
	if err != nil {
		t.Fatal(err)
	}
	consumerRun, err := e.DynamicCordisRun(t.Context(), sessionID, consumer.PluginID, consumer.PackageID, "run")
	if err != nil || !consumerRun.OK || !reflect.DeepEqual(consumerRun.WaitingFor, []string{"reactive"}) {
		t.Fatalf("consumer run = %#v, %v", consumerRun, err)
	}
	provider, providerRun := runDynamicBuiltinPlugin(t, e, sessionID, "rhp", `
return { apply(ctx) { ctx.provide('reactive', { value: 7 }) } }
`)
	assertDynamicCordisValue(t, e, consumer.PluginID, consumerRun.PluginRunID, "read", float64(7))
	if stopped, err := e.DynamicCordisStop(sessionID, provider); err != nil || !stopped.OK {
		t.Fatalf("provider stop = %#v, %v", stopped, err)
	}
	restarted, err := e.DynamicCordisRun(t.Context(), sessionID, provider, e.dynamicCordisPackageID(provider), "run")
	if err != nil || !restarted.OK || restarted.PluginRunID == providerRun {
		t.Fatalf("provider restart = %#v, %v", restarted, err)
	}
	assertDynamicCordisValue(t, e, consumer.PluginID, consumerRun.PluginRunID, "read", float64(7))
}

func TestDynamicCordisEventSnapshotIncludesDisposedListener(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-event-snapshot", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "snp", `
const calls = []
let offSecond
harness.handle('calls', () => calls)
return {
  apply(ctx) {
    ctx.on('snapshot/test', () => { calls.push('first'); offSecond() })
    offSecond = ctx.on('snapshot/test', () => { calls.push('second') })
  },
}
`)
	if err := e.emitDynamicCordisEvent("snapshot/test"); err != nil {
		t.Fatal(err)
	}
	first := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "calls", nil)
	if !first.OK || !reflect.DeepEqual(first.Value, []any{"first", "second"}) {
		t.Fatalf("first snapshot = %#v", first)
	}
	if err := e.emitDynamicCordisEvent("snapshot/test"); err != nil {
		t.Fatal(err)
	}
	second := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "calls", nil)
	if !second.OK || !reflect.DeepEqual(second.Value, []any{"first", "second", "first"}) {
		t.Fatalf("second snapshot = %#v", second)
	}
}

func waitDynamicCordisFlag(t *testing.T, e *Engine, pluginID, runID, method string) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, method, nil)
		if result.OK && result.Value == true {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s did not become true", method)
}

func assertDynamicCordisValue(t *testing.T, e *Engine, pluginID, runID, method string, want any) {
	t.Helper()
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, method, nil)
	if !result.OK || !reflect.DeepEqual(result.Value, want) {
		t.Fatalf("%s = %#v, want %#v", method, result, want)
	}
}

func (e *Engine) dynamicCordisPackageID(pluginID string) string {
	e.dynamicCordis.RLock()
	defer e.dynamicCordis.RUnlock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil {
		return ""
	}
	return plugin.current
}
