package harness

import (
	"context"
	"reflect"
	"testing"
)

func TestDynamicCordisSessionEventIsScopedAndContained(t *testing.T) {
	e := newIntegrationEngine(t)
	first, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-session-event-a", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-session-event-b", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, first, "sev", `
const seen = []
harness.handle('seen', () => seen)
return { apply(ctx) {
  ctx.on('session/event', () => { throw new Error('observer failed') })
  ctx.on('session/event', (session, event) => { seen.push([session.id, event.type, event.seq]) })
} }
`)
	otherPlugin, otherRun := runDynamicBuiltinPlugin(t, e, second, "seo", `
let count = 0
harness.handle('count', () => count)
return { apply(ctx) { ctx.on('session/event', () => { count += 1 }) } }
`)

	firstSession, _ := e.getSession(first)
	if _, err := e.appendEvent(firstSession, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}

	seen := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "seen", nil)
	if !seen.OK || !reflect.DeepEqual(seen.Value, []any{[]any{first, "turn/start", float64(0)}}) {
		t.Fatalf("session event = %#v", seen)
	}
	other := e.DynamicCordisInvoke(t.Context(), otherPlugin, otherRun, "count", nil)
	if !other.OK || other.Value != float64(0) {
		t.Fatalf("cross-session event = %#v", other)
	}
}

func TestDynamicCordisAsyncEventRejectionDoesNotStopRuntime(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-async-event", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "aev", `
let heard = 0
harness.handle('heard', () => heard)
return { apply(ctx) {
  ctx.on('async/test', async () => { await Promise.resolve(); throw new Error('async observer failed') })
  ctx.on('async/test', () => { heard += 1 })
} }
`)
	if err := e.dispatchDynamicCordisEvent(nil, "", true, "async/test"); err != nil {
		t.Fatal(err)
	}
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "heard", nil)
	if !result.OK || result.Value != float64(1) {
		t.Fatalf("listener after rejection = %#v", result)
	}
}

func TestDynamicCordisEventPrependOrder(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-event-prepend", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "evp", `
const seen = []
harness.handle('seen', () => seen)
return { apply(ctx) {
  ctx.on('ordered/test', () => { seen.push('normal-1') })
  ctx.on('ordered/test', () => { seen.push('prepend-object') }, { prepend: true })
  ctx.on('ordered/test', () => { seen.push('prepend-bool') }, true)
  ctx.on('ordered/test', () => { seen.push('normal-2') })
} }
`)
	if err := e.emitDynamicCordisEvent("ordered/test"); err != nil {
		t.Fatal(err)
	}
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "seen", nil)
	want := []any{"prepend-bool", "prepend-object", "normal-1", "normal-2"}
	if !result.OK || !reflect.DeepEqual(result.Value, want) {
		t.Fatalf("listener order = %#v, want %#v", result, want)
	}
}
