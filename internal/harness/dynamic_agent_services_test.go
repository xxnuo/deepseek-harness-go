package harness

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestDynamicCordisAgentServicesCreateQueryResumeAndDispose(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-agent-services", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "ags", `
return {
  inject: ['agents', 'agentLoop'],
  apply(ctx) {
    harness.handle('exercise', async () => {
      const loopAgent = ctx.agentLoop.create('cordis-loop-agent', { provider: 'echo', model: 'echo' }, { cwd: '/tmp' })
      const created = await ctx.agents.create({
        sessionId: 'cordis-owned-agent',
        meta: { origin: 'subagent', delegationDepth: 1 },
        agentOptions: { provider: 'echo', model: 'echo', maxTokens: 64 }
      })
      const listed = ctx.agents.list().map(agent => agent.id)
      const found = ctx.agents.get(created.agent.id)
      const roots = ctx.agents.roots().map(agent => agent.id)
      const beforeDispose = { handle: created.agent.id, found: found && found.id, listed, roots, loop: loopAgent.id }
      await created.dispose()
      const afterDispose = { found: ctx.agents.get('cordis-owned-agent'), listed: ctx.agents.list().map(agent => agent.id) }
      const resumed = await ctx.agents.resume({ resumeSessionId: 'cordis-owned-agent', agentOptions: { provider: 'echo', model: 'echo' } })
      const resumedID = resumed.agent.id
      await resumed.dispose()
      const loopResumed = await ctx.agentLoop.resume({ resumeSessionId: 'cordis-owned-agent' })
      const loopResumedID = loopResumed.agent.id
      await loopResumed.dispose()
      return { beforeDispose, afterDispose, resumedID, loopResumedID }
    })
  }
}`)

	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("exercise = %#v", result)
	}
	value, ok := result.Value.(map[string]any)
	if !ok {
		t.Fatalf("exercise value = %#v", result.Value)
	}
	before, ok := value["beforeDispose"].(map[string]any)
	if !ok {
		t.Fatalf("beforeDispose = %#v", value["beforeDispose"])
	}
	if before["handle"] != "cordis-owned-agent" || before["found"] != "cordis-owned-agent" || before["loop"] != "cordis-loop-agent" {
		t.Fatalf("agent lookup = %#v", before)
	}
	listed, _ := before["listed"].([]any)
	if !reflect.DeepEqual(listed, []any{"cordis-agent-services", "cordis-loop-agent", "cordis-owned-agent"}) {
		t.Fatalf("agent list = %#v", listed)
	}
	roots, _ := before["roots"].([]any)
	if !reflect.DeepEqual(roots, []any{"cordis-agent-services", "cordis-loop-agent", "cordis-owned-agent"}) {
		t.Fatalf("agent roots = %#v", roots)
	}
	after, ok := value["afterDispose"].(map[string]any)
	if !ok || after["found"] != nil {
		t.Fatalf("disposed agent = %#v", after)
	}
	resumedID, _ := value["resumedID"].(string)
	if resumedID != "cordis-owned-agent" {
		t.Fatalf("resumed id = %q", resumedID)
	}
	loopResumedID, _ := value["loopResumedID"].(string)
	if loopResumedID != "cordis-owned-agent" {
		t.Fatalf("agentLoop resumed id = %q", loopResumedID)
	}
}

func TestDynamicCordisAgentResumeReservationSerializesAndReleases(t *testing.T) {
	store, err := NewJSONLSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg), WithSessionStore(store))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })

	const targetID = "cordis-resume-reservation-target"
	if _, err := e.CreateSession(t.Context(), cfg.Workspace, targetID, ""); err != nil {
		t.Fatal(err)
	}
	target, err := e.getSession(targetID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(target, "feedback/record", map[string]any{"persisted": true}); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	target.attached = false
	target.mu.Unlock()
	if _, err := store.Load(t.Context(), targetID); err != nil {
		t.Fatalf("persisted target: %v", err)
	}

	ownerID, err := e.CreateSession(t.Context(), cfg.Workspace, "cordis-resume-reservation-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, ownerID, "agres", `
return {
  inject: ['agents'],
  apply(ctx) {
    const options = setup => ({
      resumeSessionId: 'cordis-resume-reservation-target',
      agentOptions: { provider: 'echo', model: 'echo' },
      ...(setup === undefined ? {} : { setup }),
    })
    harness.handle('exercise', async () => {
      let releaseSetup
      let markSetupStarted
      const setupGate = new Promise(resolve => { releaseSetup = resolve })
      const setupStarted = new Promise(resolve => { markSetupStarted = resolve })
      const first = ctx.agents.resume(options(async () => {
        markSetupStarted()
        await setupGate
      }))
      await setupStarted

      let concurrentError = ''
      try {
        await ctx.agents.resume(options())
      } catch (error) {
        concurrentError = String(error)
      }
      releaseSetup()
      const firstHandle = await first
      const firstID = firstHandle.agent.id
      await firstHandle.dispose()

      let setupError = ''
      try {
        await ctx.agents.resume(options(async () => {
          await Promise.resolve()
          throw new Error('reservation setup failed')
        }))
      } catch (error) {
        setupError = String(error)
      }

      const afterFailure = await ctx.agents.resume(options())
      const afterFailureID = afterFailure.agent.id
      await afterFailure.dispose()
      const afterTeardown = await ctx.agents.resume(options())
      const afterTeardownID = afterTeardown.agent.id
      await afterTeardown.dispose()
      return { concurrentError, setupError, firstID, afterFailureID, afterTeardownID }
    })
  }
}`)

	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("reservation exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	if !strings.Contains(value["concurrentError"].(string), "already live or being resumed") {
		t.Fatalf("concurrent resume = %#v", value)
	}
	if !strings.Contains(value["setupError"].(string), "reservation setup failed") {
		t.Fatalf("setup failure = %#v", value)
	}
	for _, field := range []string{"firstID", "afterFailureID", "afterTeardownID"} {
		if value[field] != targetID {
			t.Fatalf("%s = %#v, want %q", field, value[field], targetID)
		}
	}
}

func TestDynamicCordisAgentCreateDisposesWithPluginRun(t *testing.T) {
	e := newIntegrationEngine(t)
	ownerID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-agent-cleanup-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, ownerID, "agc", `
return {
  inject: ['agents'],
  apply(ctx) {
    harness.handle('create', async () => {
      const handle = await ctx.agents.create({
        sessionId: 'cordis-agent-cleanup-child',
        meta: { origin: 'subagent', delegationDepth: 1 }
      })
      return handle.agent.id
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "create", nil)
	if !result.OK || result.Value != "cordis-agent-cleanup-child" {
		t.Fatalf("agent create = %#v", result)
	}
	child, err := e.getSession("cordis-agent-cleanup-child")
	if err != nil {
		t.Fatal(err)
	}
	if stopped, err := e.DynamicCordisStop(ownerID, pluginID); err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	child.mu.Lock()
	attached := child.attached
	child.mu.Unlock()
	if attached {
		t.Fatal("agent session remained attached after plugin cleanup")
	}
}

func TestDynamicCordisAgentServicesRegistryLifecycle(t *testing.T) {
	e := newIntegrationEngine(t)
	ownerID, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-agent-registry-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	childID, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-agent-registry-child", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, ownerID, "agu", `
return {
  inject: ['agents'],
  apply(ctx) {
    let entered
    const disposeFactory = ctx.agents.setFactory({ create() {} })
    harness.handle('exercise', () => {
      entered = ctx.agents.enter({ id: 'cordis-agent-registry-child', session: { id: 'cordis-agent-registry-child' } }, { id: 'cordis-agent-registry-owner' })
      const before = ctx.agents.get('cordis-agent-registry-child')
      const owned = ctx.agents.isOwnedBy('cordis-agent-registry-child', { id: 'cordis-agent-registry-owner' })
      ctx.agents.announce({ id: 'cordis-agent-registry-child' })
      const registered = ctx.agents.register({ id: 'cordis-agent-registry-owner', session: { id: 'cordis-agent-registry-owner' } })
      registered()
      entered()
      disposeFactory()
      return { before: before && before.id, owned, missingAfter: ctx.agents.get('cordis-agent-registry-child') === undefined }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("registry lifecycle = %#v", result)
	}
	value, ok := result.Value.(map[string]any)
	if !ok || value["before"] != childID || value["owned"] != true || value["missingAfter"] != true {
		t.Fatalf("registry lifecycle = %#v", result.Value)
	}
}

func TestDynamicCordisAgentFactoryDelegatesCreateAndDisposesWithProvider(t *testing.T) {
	e := newIntegrationEngine(t)
	ownerID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-agent-factory-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	providerID, providerRun := runDynamicBuiltinPlugin(t, e, ownerID, "agfp", `
return {
  inject: ['agents'],
  apply(ctx) {
	const disposeFactory = ctx.agents.setFactory({
      createAgent(_ownerCtx, options) {
        return Promise.resolve({ agent: { id: options.sessionId }, dispose() {} })
      }
    })
	harness.handle('disposeFactory', () => { disposeFactory(); return true })
  }
}`)
	consumerID, consumerRun := runDynamicBuiltinPlugin(t, e, ownerID, "agfc", `
return {
  inject: ['agents'],
  apply(ctx) {
    harness.handle('create', async () => (await ctx.agents.create({ sessionId: 'delegated-agent' })).agent.id)
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), consumerID, consumerRun, "create", nil)
	if !result.OK || result.Value != "delegated-agent" {
		t.Fatalf("delegated create = %#v", result)
	}
	if disposed := e.DynamicCordisInvoke(t.Context(), providerID, providerRun, "disposeFactory", nil); !disposed.OK {
		t.Fatalf("dispose factory = %#v", disposed)
	}
	e.dynamicCordis.RLock()
	if e.dynamicCordis.factoryRun != nil || e.dynamicCordis.factoryValue != nil {
		t.Fatalf("factory remained registered: run=%p value=%T", e.dynamicCordis.factoryRun, e.dynamicCordis.factoryValue)
	}
	e.dynamicCordis.RUnlock()
	result = e.DynamicCordisInvoke(t.Context(), consumerID, consumerRun, "create", nil)
	if !result.OK || result.Value != "delegated-agent" {
		t.Fatalf("create after factory disposal = %#v", result)
	}
	if session, err := e.getSession("delegated-agent"); err != nil {
		t.Fatal(err)
	} else {
		session.mu.Lock()
		attached := session.attached
		session.mu.Unlock()
		if !attached {
			t.Fatal("fallback-created agent is not attached")
		}
	}
}

func TestDynamicCordisAgentInitiatorSurvivesPromiseBoundary(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-agent-initiator", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "agini", `
return {
  inject: ['agents'],
  apply(ctx) {
    harness.handle('exercise', async () => {
      const inside = await ctx.agents.withInitiator({ id: 'cordis-agent-initiator' }, async () => {
        await Promise.resolve()
        return ctx.agents.requireInitiator().id
      })
      const cleared = await ctx.agents.withInitiator({ id: 'cordis-agent-initiator' }, async () =>
        ctx.agents.withoutInitiator(async () => {
          await Promise.resolve()
          return ctx.agents.currentInitiator() === undefined
        }))
      return { inside, cleared, after: ctx.agents.currentInitiator() }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("initiator = %#v", result)
	}
	value := result.Value.(map[string]any)
	if value["inside"] != sessionID || value["cleared"] != true || value["after"] != nil {
		t.Fatalf("initiator scope = %#v", value)
	}
}

func TestDynamicCordisAgentInitiatorIsolatedAcrossPendingPromises(t *testing.T) {
	e := newIntegrationEngine(t)
	firstID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-agent-initiator-first", "")
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-agent-initiator-second", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, firstID, "agii", `
return {
  inject: ['agents'],
  apply(ctx) {
    harness.handle('exercise', async () => {
      let releaseFirst
      let releaseSecond
      const firstGate = new Promise(resolve => { releaseFirst = resolve })
      const secondGate = new Promise(resolve => { releaseSecond = resolve })
      const first = ctx.agents.withInitiator({ id: 'cordis-agent-initiator-first' }, async () => {
        await firstGate
        return ctx.agents.requireInitiator().id
      })
      const second = ctx.agents.withInitiator({ id: 'cordis-agent-initiator-second' }, async () => {
        await secondGate
        return ctx.agents.requireInitiator().id
      })
      releaseFirst()
      const firstResult = await first
      releaseSecond()
      const secondResult = await second
      return { firstResult, secondResult, after: ctx.agents.currentInitiator() }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("initiator = %#v", result)
	}
	value := result.Value.(map[string]any)
	if value["firstResult"] != firstID || value["secondResult"] != secondID || value["after"] != nil {
		t.Fatalf("initiator scopes = %#v", value)
	}
}

func TestDynamicCordisAgentCreateSeedSetupCommitAndPublishOrder(t *testing.T) {
	e := newIntegrationEngine(t)
	ownerID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-agent-setup-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, ownerID, "agstp", `
return {
  inject: ['agents'],
  apply(ctx) {
    const order = []
    ctx.on('session/created', session => { if (session.id === 'cordis-agent-setup') order.push('session/created') })
    ctx.on('agent/created', ({ agent }) => { if (agent.id === 'cordis-agent-setup') order.push('agent/created') })
    ctx.on('agent/session-start', ({ agent }) => { if (agent.id === 'cordis-agent-setup') order.push('agent/session-start') })
    harness.handle('exercise', async () => {
      const handle = await ctx.agents.create({
        sessionId: 'cordis-agent-setup',
        meta: { isSeeded: true, origin: 'subagent' },
        inheritedEventCount: 1,
        seed: [{ type: 'plugin/seed', seq: 0, time: 1, data: { ready: true }, ignorable: true }],
        setup: async agentCtx => {
          order.push('setup:' + agentCtx.agent.id)
          agentCtx.on('agent/session-start', ({ agent }) => {
            if (agent.id === 'cordis-agent-setup') order.push('setup-listener')
          })
          await Promise.resolve()
          return { commit() { order.push('setup:commit') } }
        }
      })
      const value = {
        order: [...order],
        header: handle.agent.session.header,
        firstLiveSeq: handle.agent.session.firstLiveSeq,
        inheritedEventCount: handle.agent.session.inheritedEventCount,
        events: handle.agent.session.events.map(event => event.type),
        ctxSame: handle.agent.ctx.agent === handle.agent
      }
      await handle.dispose()
      return value
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("setup create = %#v", result)
	}
	value := result.Value.(map[string]any)
	wantOrder := []any{"setup:cordis-agent-setup", "setup:commit", "session/created", "agent/created", "agent/session-start", "setup-listener"}
	if !reflect.DeepEqual(value["order"], wantOrder) {
		t.Fatalf("publish order = %#v, want %#v", value["order"], wantOrder)
	}
	header := value["header"].(map[string]any)
	if header["isSeeded"] != true || value["inheritedEventCount"] != float64(1) || header["origin"] != "subagent" || value["ctxSame"] != true {
		t.Fatalf("agent setup surface = %#v", value)
	}
	if !reflect.DeepEqual(value["events"], []any{"plugin/seed", "session/end-seed"}) || value["firstLiveSeq"] != float64(1) {
		t.Fatalf("seed boundary = %#v", value)
	}
}

func TestDynamicCordisAgentCreationFailuresRollbackAndHonorSignals(t *testing.T) {
	e := newIntegrationEngine(t)
	ownerID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-agent-rollback-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, ownerID, "agrbk", `
return {
  inject: ['agents', 'sessions'],
  apply(ctx) {
    harness.handle('exercise', async () => {
      const errors = {}
      const live = id => ctx.agents.get(id) !== undefined || ctx.sessions.get(id) !== undefined
      let leakedSetupListener = 0
      try {
        await ctx.agents.create({
          sessionId: 'agent-setup-reject',
          setup(agentCtx) {
            agentCtx.on('agent/created', ({ agent }) => {
              if (agent.id === 'agent-setup-reject') leakedSetupListener++
            })
            return Promise.reject(new Error('setup rejected'))
          }
        })
      } catch (error) { errors.setup = String(error) }
      const setupLive = live('agent-setup-reject')
      const setupReplacement = await ctx.agents.create({ sessionId: 'agent-setup-reject' })
      await setupReplacement.dispose()

      try {
        await ctx.agents.create({
          sessionId: 'agent-session-created-reject',
          setup(agentCtx) {
            agentCtx.on('session/created', session => {
              if (session.id === 'agent-session-created-reject') throw new Error('session created rejected')
            })
          }
        })
      } catch (error) { errors.sessionCreated = String(error) }
      const sessionCreatedLive = live('agent-session-created-reject')
      const sessionCreatedReplacement = await ctx.agents.create({ sessionId: 'agent-session-created-reject' })
      await sessionCreatedReplacement.dispose()

      try {
        await ctx.agents.create({
          sessionId: 'agent-session-start-reject',
          setup(agentCtx) {
            agentCtx.on('agent/session-start', ({ agent }) => {
              if (agent.id === 'agent-session-start-reject') throw new Error('session start rejected')
            })
          }
        })
      } catch (error) { errors.sessionStart = String(error) }
      const sessionStartLive = live('agent-session-start-reject')
      const sessionStartReplacement = await ctx.agents.create({ sessionId: 'agent-session-start-reject' })
      await sessionStartReplacement.dispose()

      const preAborted = { aborted: true, reason: new Error('pre aborted') }
      try {
        await ctx.agents.create({ sessionId: 'agent-pre-aborted', signal: preAborted })
      } catch (error) { errors.preAborted = String(error) }
      const preAbortedLive = live('agent-pre-aborted')

      const duringSignal = {
        aborted: false,
        reason: undefined,
        listeners: [],
        addEventListener(_type, listener) { this.listeners.push(listener) },
        removeEventListener(_type, listener) { this.listeners = this.listeners.filter(item => item !== listener) },
        abort(reason) {
          this.aborted = true
          this.reason = reason
          for (const listener of [...this.listeners]) listener()
        }
      }
      try {
        await ctx.agents.create({
          sessionId: 'agent-during-abort',
          signal: duringSignal,
          setup: async () => {
            await Promise.resolve()
            duringSignal.abort(new Error('during setup aborted'))
            await new Promise(() => {})
          }
        })
      } catch (error) { errors.during = String(error) }
      const duringLive = live('agent-during-abort')
      const duringReplacement = await ctx.agents.create({ sessionId: 'agent-during-abort' })
      await duringReplacement.dispose()

      return {
        errors, leakedSetupListener, setupLive, sessionCreatedLive,
        sessionStartLive, preAbortedLive, duringLive
      }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("rollback exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	errorsValue := value["errors"].(map[string]any)
	for key, want := range map[string]string{
		"setup":          "setup rejected",
		"sessionCreated": "session created rejected",
		"sessionStart":   "session start rejected",
		"preAborted":     "pre aborted",
		"during":         "during setup aborted",
	} {
		if message, _ := errorsValue[key].(string); !strings.Contains(message, want) {
			t.Fatalf("%s error = %#v, want %q", key, errorsValue[key], want)
		}
	}
	for _, key := range []string{"setupLive", "sessionCreatedLive", "sessionStartLive", "preAbortedLive", "duringLive"} {
		if value[key] != false {
			t.Fatalf("%s = %#v", key, value[key])
		}
	}
	if value["leakedSetupListener"] != float64(0) {
		t.Fatalf("setup listener leaked = %#v", value["leakedSetupListener"])
	}
}

func TestDynamicCordisAgentServicesValidateCreationBoundary(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-agent-validation", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "agv", `
return {
  inject: ['agents'],
  apply(ctx) {
    harness.handle('unknown', async () => await ctx.agents.create({ sessionId: 'bad', unsupported: [] }))
    harness.handle('missing', async () => await ctx.agents.create({}))
    harness.handle('badTokens', async () => await ctx.agents.create({ sessionId: 'bad-tokens', agentOptions: { maxTokens: 0 } }))
  }
}`)
	for _, method := range []string{"unknown", "missing", "badTokens"} {
		result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, method, nil)
		if result.OK || !strings.Contains(result.Message, "agents") {
			t.Fatalf("%s = %#v", method, result)
		}
	}
}

func TestDynamicCordisAgentFacadeDeliversInboxAndMaintenanceWork(t *testing.T) {
	e := newIntegrationEngine(t)
	ownerID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-agent-facade-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, ownerID, "agfac", `
return {
  inject: ['agents'],
  apply(ctx) {
    const message = (id, text) => ({ id, role: 'user', content: [{ type: 'text', text }], source: { kind: 'plugin', plugin: 'test' } })
    harness.handle('exercise', async () => {
      const first = await ctx.agents.create({ sessionId: 'cordis-agent-facade-first' })
      first.agent.inject(message('inject-1', 'injected context'))
      const injected = first.agent.inbox.nextStep.map(item => item.id)
      first.agent.followup(message('followup-1', 'followup prompt'))
      await first.agent.whenIdle()
      first.agent.send(message('parked-1', 'parked prompt'), 'next-turn', false)
      const parked = first.agent.inbox.nextTurn.map(item => item.id)
      const maintenance = await first.agent.runMaintenance(async signal => {
        await Promise.resolve()
        return signal.aborted
      })
      const second = await ctx.agents.create({ sessionId: 'cordis-agent-facade-second' })
      second.agent.steer(message('steer-1', 'idle steering prompt'))
      await second.agent.whenIdle()
      const result = { injected, parked, maintenance, first: first.agent.session.events, second: second.agent.session.events }
      await first.dispose()
      await second.dispose()
      return result
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("facade exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	if !reflect.DeepEqual(value["injected"], []any{"inject-1"}) || !reflect.DeepEqual(value["parked"], []any{"parked-1"}) || value["maintenance"] != false {
		t.Fatalf("facade values = %#v", value)
	}
	for name, want := range map[string]string{"first": "injected context", "second": "idle steering prompt"} {
		events, ok := value[name].([]any)
		if !ok || !dynamicEventsContainText(events, want) {
			t.Fatalf("%s events = %#v, want %q", name, value[name], want)
		}
	}
}

func TestDynamicCordisAgentInboxMatchesUpstreamMutationSemantics(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-agent-inbox-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "aginbx", `
const observed = []
return {
  inject: ['agents'],
  apply(ctx) {
    ctx.on('agent/inbox/inserted', payload => observed.push(['inserted', payload.message.id]))
    ctx.on('agent/inbox/discarded', payload => observed.push(['discarded', payload.message.id]))
    const message = id => ({ id, role: 'user', content: [{ type: 'text', text: id }], source: { kind: 'plugin' } })
    harness.handle('exercise', async () => {
      const handle = await ctx.agents.create({ sessionId: 'cordis-agent-inbox' })
      const { agent } = handle
      agent.inbox.append('next-turn', message('a'))
      agent.inbox.prepend('next-turn', message('b'))
      agent.inbox.append('next-step', message('step'))
      const removed = agent.inbox.splice('next-turn', -1, Infinity, [message('c')]).map(item => item.id)
      const replaced = agent.inbox.replace('b', message('d'))
      const missingReplace = agent.inbox.replace('missing', message('e'))
      const removedByID = agent.inbox.remove('c')
      const missingRemove = agent.inbox.remove('missing')
      let duplicate = ''
      try { agent.inbox.append('next-step', message('d')) } catch (error) { duplicate = String(error) }
      const beforeClear = { hasPending: agent.inbox.hasPending, turn: agent.inbox.nextTurn.map(item => item.id), step: agent.inbox.nextStep.map(item => item.id) }
      agent.inbox.clear()
      const afterClear = { hasPending: agent.inbox.hasPending, turn: agent.inbox.nextTurn.map(item => item.id), step: agent.inbox.nextStep.map(item => item.id) }
      const splices = agent.session.events.filter(event => event.type === 'agent/inbox/spliced').map(event => event.data)
      await handle.dispose()
      return { removed, replaced, missingReplace, removedByID, missingRemove, duplicate, beforeClear, afterClear, splices, observed }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("inbox exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	if !reflect.DeepEqual(value["removed"], []any{"a"}) || value["replaced"] != true || value["missingReplace"] != false || value["removedByID"] != true || value["missingRemove"] != false {
		t.Fatalf("inbox mutation results = %#v", value)
	}
	before, after := value["beforeClear"].(map[string]any), value["afterClear"].(map[string]any)
	if before["hasPending"] != true || !reflect.DeepEqual(before["turn"], []any{"d"}) || !reflect.DeepEqual(before["step"], []any{"step"}) ||
		after["hasPending"] != false || !reflect.DeepEqual(after["turn"], []any{}) || !reflect.DeepEqual(after["step"], []any{}) {
		t.Fatalf("inbox queue state = before %#v after %#v", before, after)
	}
	if !strings.Contains(value["duplicate"].(string), "already pending") {
		t.Fatalf("duplicate message id was accepted: %#v", value)
	}
	splices, ok := value["splices"].([]any)
	if !ok || len(splices) != 8 {
		t.Fatalf("splices = %#v", value["splices"])
	}
	for _, index := range []int{3, 4, 5, 6, 7} {
		splice := splices[index].(map[string]any)
		if splice["outcome"] != "canceled" || splice["removedCount"] == nil {
			t.Fatalf("discard splice %d = %#v", index, splice)
		}
	}
	observed, ok := value["observed"].([]any)
	if !ok || len(observed) != 10 {
		t.Fatalf("inbox notifications = %#v", value["observed"])
	}
}

func TestDynamicCordisAgentMaintenanceWakeAndCancelSemantics(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-agent-maintenance-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "agmnt", `
let cancelAgent
let cancelTask
return {
  inject: ['agents'],
  apply(ctx) {
    const message = id => ({ id, role: 'user', content: [{ type: 'text', text: id }], source: { kind: 'plugin' } })
    harness.handle('exercise', async () => {
      const quiet = await ctx.agents.create({ sessionId: 'cordis-agent-maintenance-quiet' })
      await quiet.agent.runMaintenance(async () => {
        quiet.agent.inject(message('parked'))
        return quiet.agent.status
      })
      const quietState = { status: quiet.agent.status, step: quiet.agent.inbox.nextStep.map(item => item.id) }
      const waking = await ctx.agents.create({ sessionId: 'cordis-agent-maintenance-waking' })
      let release
      const waiting = waking.agent.runMaintenance(() => new Promise(resolve => { release = resolve }))
      waking.agent.followup(message('wake'))
      release()
      await waiting
      const runningAfterWake = waking.agent.status
      await waking.agent.whenIdle()
      const wakeEvents = waking.agent.session.events
      await quiet.dispose()
      await waking.dispose()
      return { quietState, runningAfterWake, wakeEvents }
    })
    harness.handle('startCancel', async () => {
      const handle = await ctx.agents.create({ sessionId: 'cordis-agent-maintenance-cancel' })
      cancelAgent = handle.agent
      cancelTask = cancelAgent.runMaintenance(signal => new Promise(resolve => {
        const removed = () => { throw new Error('removed abort listener ran') }
        signal.addEventListener('abort', removed)
        signal.removeEventListener('abort', removed)
        signal.addEventListener('abort', () => resolve({ aborted: signal.aborted, reason: String(signal.reason) }))
      }))
      return cancelAgent.id
    })
    harness.handle('finishCancel', async () => await cancelTask)
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("maintenance exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	quietState := value["quietState"].(map[string]any)
	if quietState["status"] != "idle" || !reflect.DeepEqual(quietState["step"], []any{"parked"}) || value["runningAfterWake"] != "running" || !dynamicEventsContainText(value["wakeEvents"].([]any), "wake") {
		t.Fatalf("maintenance wake behavior = %#v", value)
	}
	started := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "startCancel", nil)
	if !started.OK || started.Value != "cordis-agent-maintenance-cancel" {
		t.Fatalf("start maintenance cancellation = %#v", started)
	}
	if err := e.CancelSession("cordis-agent-maintenance-cancel"); err != nil {
		t.Fatal(err)
	}
	finished := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "finishCancel", nil)
	if !finished.OK {
		t.Fatalf("finish maintenance cancellation = %#v", finished)
	}
	canceled := finished.Value.(map[string]any)
	if canceled["aborted"] != true || !strings.Contains(canceled["reason"].(string), "maintenance canceled") {
		t.Fatalf("maintenance cancel signal = %#v", canceled)
	}
}

func dynamicEventsContainText(events []any, want string) bool {
	for _, raw := range events {
		event, ok := raw.(map[string]any)
		if !ok || event["type"] != "user/message" {
			continue
		}
		data, _ := event["data"].(map[string]any)
		message, _ := data["message"].(map[string]any)
		if message == nil {
			message = data
		}
		content, _ := message["content"].([]any)
		for _, rawBlock := range content {
			block, _ := rawBlock.(map[string]any)
			if block["text"] == want {
				return true
			}
		}
	}
	return false
}
