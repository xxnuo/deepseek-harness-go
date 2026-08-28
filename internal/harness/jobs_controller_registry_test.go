package harness

import (
	"context"
	"strings"
	"testing"
)

func TestDynamicCordisJobsControllerReferencesAreIndependentlyDisposable(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "jobs-controller-refs", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "jref", `
return {
  inject: ['jobs'],
  apply(ctx) {
    const first = ctx.jobs.attachController('duplicate')
    const second = ctx.jobs.attachController('duplicate')
    harness.handle('exercise', () => {
      first()
      let afterFirst = ''
      try { ctx.jobs.start({ kind: 'refs', label: 'still-served', owner: { id: '`+sessionID+`' }, run() { return { done: Promise.resolve({ status: 'completed' }) } } }) }
      catch (error) { afterFirst = String(error) }
      second()
      let afterSecond = ''
      try { ctx.jobs.start({ kind: 'refs', label: 'detached', owner: { id: '`+sessionID+`' }, run() { return { done: Promise.resolve({ status: 'completed' }) } } }) }
      catch (error) { afterSecond = String(error) }
      return { afterFirst, afterSecond }
    })
  }
}`)
	result := e.DynamicCordisInvoke(context.Background(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("controller refs invoke = %#v", result)
	}
	value, ok := result.Value.(map[string]any)
	if !ok {
		t.Fatalf("controller refs result = %#v", result.Value)
	}
	if value["afterFirst"] != "" || !strings.Contains(value["afterSecond"].(string), "no job controller") {
		t.Fatalf("controller refs admission = %#v", value)
	}
}

func TestDynamicCordisJobsControllerCleanupClosesAdmission(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "jobs-controller-cleanup", "")
	if err != nil {
		t.Fatal(err)
	}
	controllerID, _ := runDynamicBuiltinPlugin(t, e, sessionID, "jaut", `
return {
  inject: ['jobs'],
  apply(ctx) { ctx.jobs.attachController('forgotten-disposer') }
}`)
	producerID, producerRun := runDynamicBuiltinPlugin(t, e, sessionID, "jautp", `
return {
  inject: ['jobs'],
  apply(ctx) {
    harness.handle('start', () => {
      try {
        return ctx.jobs.start({ kind: 'cleanup', label: 'should-fail', run() { return { done: Promise.resolve({ status: 'completed' }) } } })
      } catch (error) {
        return String(error)
      }
    })
  }
}`)
	if stopped, err := e.DynamicCordisStop(sessionID, controllerID); err != nil || !stopped.OK {
		t.Fatalf("controller stop = %#v, %v", stopped, err)
	}
	result := e.DynamicCordisInvoke(context.Background(), producerID, producerRun, "start", nil)
	if !result.OK || !strings.Contains(result.Value.(string), "no job controller") {
		t.Fatalf("controller cleanup admission = %#v", result)
	}
}

func TestDynamicCordisJobsCancelMayReadRegistry(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "jobs-cancel-reentrant", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "jren", `
return {
  inject: ['jobs'],
  apply(ctx) {
    ctx.jobs.attachController('reentrant-cancel')
    harness.handle('exercise', async () => {
      let settle
      let id
      id = ctx.jobs.start({
        kind: 'reentrant', label: 'cancel reads registry', owner: { id: '`+sessionID+`' },
        run() {
          return {
            done: new Promise(resolve => { settle = resolve }),
            cancel() {
              const before = ctx.jobs.get(id, { id: '`+sessionID+`' })
              settle({ status: 'killed', detail: before.status })
            },
          }
        },
      })
      const requested = ctx.jobs.kill(id, { id: '`+sessionID+`' })
      const terminal = await ctx.jobs.wait(id, 1000, { id: '`+sessionID+`' })
      return { requested: requested.status, terminal }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("reentrant cancel = %#v", result)
	}
	value := result.Value.(map[string]any)
	terminal := value["terminal"].(map[string]any)
	if value["requested"] != "requested" || terminal["status"] != "killed" || terminal["detail"] != "running" {
		t.Fatalf("reentrant cancel result = %#v", value)
	}
}

func TestDynamicCordisJobsParityEdgeCases(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "jobs-parity", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "jpar", `
return {
  inject: ['jobs'],
  apply(ctx) {
    ctx.jobs.attachController('parity')
    const changed = []
    const done = []
	ctx.jobs.onJobsChanged(owner => changed.push(owner === undefined ? 'unowned' : owner.id))
    ctx.jobs.onJobDone((job, owner) => done.push([job.id, job.status, job.reported, owner === undefined]))
    const controller = () => {
      const listeners = new Set()
      const signal = {
        aborted: false,
        addEventListener(name, fn) { if (name === 'abort') listeners.add(fn) },
        removeEventListener(name, fn) { if (name === 'abort') listeners.delete(fn) },
        abort() { this.aborted = true; for (const fn of [...listeners]) fn() },
      }
      return signal
    }
    harness.handle('unowned', () => {
      try {
        ctx.jobs.start({
          kind: 'open', label: 'shared',
	      run() { return { done: Promise.resolve({ status: 'completed' }), cancel() {} } },
        })
        return ''
      } catch (error) {
        return String(error)
      }
    })
    harness.handle('abort', async () => {
      let settle
      const id = ctx.jobs.start({
        kind: 'abort', label: 'waiting', owner: { id: '`+sessionID+`' },
        run() { return { done: new Promise(resolve => { settle = resolve }), cancel() {} } },
      })
      const signal = controller()
      const wait = ctx.jobs.wait(id, 5000, { id: '`+sessionID+`' }, signal)
      signal.abort()
      let error = ''
      try { await wait } catch (caught) { error = String(caught) }
      const after = ctx.jobs.get(id, { id: '`+sessionID+`' })
      settle({ status: 'completed' })
      await Promise.resolve()
      const settled = ctx.jobs.get(id, { id: '`+sessionID+`' })
      return { error, after, settled }
    })
    harness.handle('cancelThrow', async () => {
      let settle
      const id = ctx.jobs.start({
        kind: 'cancel', label: 'throwing', owner: { id: '`+sessionID+`' },
        run() {
          return {
            done: new Promise(resolve => { settle = resolve }),
            cancel() { throw new Error('cancel boom') },
          }
        },
      })
      let error = ''
      try { ctx.jobs.kill(id, { id: '`+sessionID+`' }) } catch (caught) { error = String(caught) }
      const after = ctx.jobs.get(id, { id: '`+sessionID+`' })
      settle({ status: 'completed' })
      await Promise.resolve()
      return { error, after, terminal: ctx.jobs.get(id, { id: '`+sessionID+`' }), done }
    })
    harness.handle('invalidOutcomes', async () => {
      const outcomes = [{}, { status: 'running' }, { status: 1 }, { status: 'completed', detail: 1 }]
      const ids = outcomes.map((outcome, index) => ctx.jobs.start({
        kind: 'bad', label: 'invalid-' + index, owner: { id: '`+sessionID+`' },
        run() { return { done: Promise.resolve(outcome), cancel() {} } },
      }))
      return Promise.all(ids.map(id => ctx.jobs.wait(id, 1000, { id: '`+sessionID+`' })))
    })
    harness.handle('invalidLimits', () => [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1].map(outputLimitBytes => {
      try {
        ctx.jobs.start({ kind: 'limit', label: 'invalid', owner: { id: '`+sessionID+`' }, outputLimitBytes, run() { return { done: Promise.resolve({ status: 'completed' }), cancel() {} } } })
        return ''
      } catch (error) { return String(error) }
    }))
  }
}`)

	aborted := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "abort", nil)
	if !aborted.OK {
		t.Fatalf("abort = %#v", aborted)
	}
	abortValue := aborted.Value.(map[string]any)
	if !strings.Contains(abortValue["error"].(string), "wait aborted") {
		t.Fatalf("abort error = %#v", abortValue)
	}
	afterAbort := abortValue["after"].(map[string]any)
	if afterAbort["status"] != "running" || afterAbort["reported"] != false {
		t.Fatalf("aborted wait mutated job = %#v", afterAbort)
	}

	cancelled := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "cancelThrow", nil)
	if !cancelled.OK {
		t.Fatalf("cancel throw = %#v", cancelled)
	}
	cancelValue := cancelled.Value.(map[string]any)
	if !strings.Contains(cancelValue["error"].(string), "cancel boom") {
		t.Fatalf("cancel error = %#v", cancelValue)
	}
	afterCancel := cancelValue["after"].(map[string]any)
	if afterCancel["status"] != "running" || afterCancel["reported"] != false {
		t.Fatalf("throwing cancel mutated job = %#v", afterCancel)
	}

	invalid := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "invalidOutcomes", nil)
	if !invalid.OK {
		t.Fatalf("invalid outcomes = %#v", invalid)
	}
	for _, raw := range invalid.Value.([]any) {
		if row := raw.(map[string]any); row["status"] != "failed" {
			t.Fatalf("invalid outcome accepted = %#v", row)
		}
	}

	limits := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "invalidLimits", nil)
	if !limits.OK {
		t.Fatalf("invalid limits = %#v", limits)
	}
	for _, raw := range limits.Value.([]any) {
		if !strings.Contains(raw.(string), "invalid outputLimitBytes") {
			t.Fatalf("invalid limit accepted = %#v", limits.Value)
		}
	}

	unowned := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "unowned", nil)
	if !unowned.OK || !strings.Contains(unowned.Value.(string), "no job controller") {
		t.Fatalf("unowned = %#v", unowned)
	}
}
