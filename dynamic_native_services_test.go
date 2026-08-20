package harness

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDynamicCordisNativeConfigurationServicesFromJavaScript(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "native-service-config", "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()

	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "nsvc", `
return {
  inject: ['agentDefaultModel', 'credentials', 'permissionPresets', 'sandboxPolicy'],
  apply(ctx) {
	const events = []
	ctx.on('settings/document-updated', (namespace, revision) => events.push({ name: 'document', namespace, revision }))
	ctx.on('settings/updated', (namespace) => events.push({ name: 'settings', namespace }))
	ctx.on('credentials/updated', ref => events.push({ name: 'credential', ref }))
    harness.handle('exercise', async args => {
      const before = ctx.agentDefaultModel.currentSelection()
      const save = ctx.agentDefaultModel.saveSelection({ provider: 'custom', model: 'model-v2', reasoningEffort: 'high' })
      if (!save || typeof save.then !== 'function') throw new Error('saveSelection must return a Promise')
      await save
      const after = ctx.agentDefaultModel.currentSelection()

      await ctx.credentials.set('CORDIS_TEST_CREDENTIAL', 'secret-value')
      const resolve = ctx.credentials.resolve('CORDIS_TEST_CREDENTIAL')
      if (!resolve || typeof resolve.then !== 'function') throw new Error('credentials.resolve must return a Promise')
      const resolved = await resolve
      const described = await ctx.credentials.describe('CORDIS_TEST_CREDENTIAL')
      await ctx.credentials.unset('CORDIS_TEST_CREDENTIAL')
      const missing = await ctx.credentials.resolve('CORDIS_TEST_CREDENTIAL')

      const currentPreset = ctx.permissionPresets.current(args.events)
      const customSelect = ctx.permissionPresets.selectFor({
        preset: null, sandbox: 'danger-full-access', approval: 'ask'
      })
      const preset = ctx.permissionPresets.resolve('read-only')
      const customOption = ctx.permissionPresets.optionOf('custom')
      ctx.permissionPresets.set({ id: args.sessionId }, 'read-only')

      return {
        before,
        after,
        resolved,
        described,
        missing: missing === undefined,
        currentPreset,
        customSelect,
        preset,
        customOption,
        defaultMode: ctx.sandboxPolicy.defaultMode,
        workspaceRoot: ctx.sandboxPolicy.workspaceRoot,
        policy: ctx.sandboxPolicy.resolve({ session: { id: args.sessionId } }),
        explicitPolicy: ctx.sandboxPolicy.resolve({ session: { id: args.sessionId }, mode: 'danger-full-access' }),
		override: ctx.sandboxPolicy.overrideOf({ id: args.sessionId }),
		events
      }
    })
  }
}`)

	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", map[string]any{
		"sessionId": sessionID,
		"events":    events,
	})
	if !result.OK {
		t.Fatalf("exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	before := value["before"].(map[string]any)
	if before["provider"] != "echo" || before["model"] != "echo" {
		t.Fatalf("before = %#v", before)
	}
	after := value["after"].(map[string]any)
	if after["provider"] != "custom" || after["model"] != "model-v2" || after["reasoningEffort"] != "high" {
		t.Fatalf("after = %#v", after)
	}
	resolved := value["resolved"].(map[string]any)
	if resolved["value"] != "secret-value" || resolved["source"] != "file" || value["missing"] != true {
		t.Fatalf("credentials = %#v, missing=%#v", resolved, value["missing"])
	}
	described := value["described"].(map[string]any)
	if described["configured"] != true || described["writable"] != true {
		t.Fatalf("credential description = %#v", described)
	}
	if value["currentPreset"] != "workspace-write" {
		t.Fatalf("current preset = %#v", value["currentPreset"])
	}
	selection := value["customSelect"].(map[string]any)
	if selection["currentValue"] != "custom" || len(selection["options"].([]any)) != 4 {
		t.Fatalf("custom select = %#v", selection)
	}
	preset := value["preset"].(map[string]any)
	if preset["sandbox"] != "read-only" || preset["approval"] != "ask" {
		t.Fatalf("preset = %#v", preset)
	}
	if value["customOption"].(map[string]any)["value"] != "custom" {
		t.Fatalf("custom option = %#v", value["customOption"])
	}
	if value["defaultMode"] != "workspace-write" || value["workspaceRoot"] != e.Config().Workspace {
		t.Fatalf("sandbox defaults = %#v / %#v", value["defaultMode"], value["workspaceRoot"])
	}
	policy := value["policy"].(map[string]any)
	if policy["mode"] != "read-only" || policy["workspaceRoot"] != e.Config().Workspace || policy["sessionId"] != sessionID {
		t.Fatalf("policy = %#v", policy)
	}
	if value["explicitPolicy"].(map[string]any)["mode"] != "danger-full-access" || value["override"] != "read-only" {
		t.Fatalf("sandbox override = %#v / %#v", value["explicitPolicy"], value["override"])
	}
	eventRows := value["events"].([]any)
	wantEvents := []struct {
		name, field, value string
	}{
		{"document", "namespace", "agent-default-model"},
		{"settings", "namespace", "agent-default-model"},
		{"credential", "ref", "CORDIS_TEST_CREDENTIAL"},
		{"credential", "ref", "CORDIS_TEST_CREDENTIAL"},
	}
	if len(eventRows) != len(wantEvents) {
		t.Fatalf("events = %#v", eventRows)
	}
	for index, want := range wantEvents {
		row := eventRows[index].(map[string]any)
		if row["name"] != want.name || row[want.field] != want.value {
			t.Fatalf("events[%d] = %#v", index, row)
		}
	}
}

func TestDynamicCordisNativeSpillAndReferencesFromJavaScript(t *testing.T) {
	e := newIntegrationEngine(t)
	targetID, err := e.CreateSession(context.Background(), e.Config().Workspace, "native-service-target", "")
	if err != nil {
		t.Fatal(err)
	}
	sourceID, err := e.CreateSession(context.Background(), e.Config().Workspace, "native-service-source", "")
	if err != nil {
		t.Fatal(err)
	}
	source, err := e.getSession(sourceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(source, "user/message", map[string]any{
		"id": "source-message", "role": "user",
		"content": []ContentBlock{{Type: "text", Text: "durable source context"}},
		"source":  map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}

	pluginID, runID := runDynamicBuiltinPlugin(t, e, targetID, "nspill", `
return {
  inject: ['spillStore', 'sessionReferenceResolver'],
  apply(ctx) {
    harness.handle('exercise', async args => {
      const spill = await ctx.spillStore.saveText({
        owner: { sessionId: args.targetId },
        source: { toolName: 'dynamic', callId: 'call-1', label: 'result' },
        suggestedName: '../result.txt',
        content: args.content
      })
      const candidates = await ctx.sessionReferenceResolver.listCandidates(
        { id: args.targetId }, args.sourceId, 5
      )
      const prepared = await ctx.sessionReferenceResolver.prepare(
        { id: args.targetId },
        [{ type: 'text', text: 'compare source' }],
        [{ sessionId: args.sourceId, label: 'source' }]
      )
      return { spill, candidates, prepared }
    })
  }
}`)

	content := "full spill text\nsecond line"
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", map[string]any{
		"targetId": targetID,
		"sourceId": sourceID,
		"content":  content,
	})
	if !result.OK {
		t.Fatalf("exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	spill := value["spill"].(map[string]any)
	locator := spill["locator"].(string)
	if spill["bytes"] != float64(len([]byte(content))) || !strings.Contains(spill["retrievalHint"].(string), "grep") {
		t.Fatalf("spill = %#v", spill)
	}
	stored, err := os.ReadFile(locator)
	if err != nil || string(stored) != content {
		t.Fatalf("stored spill = %q, %v", stored, err)
	}
	if strings.Contains(filepath.Base(locator), "/") || !strings.Contains(filepath.Base(locator), "~002F") {
		t.Fatalf("unsafe spill locator = %q", locator)
	}
	candidates := value["candidates"].([]any)
	if len(candidates) != 1 || candidates[0].(map[string]any)["sessionId"] != sourceID {
		t.Fatalf("candidates = %#v", candidates)
	}
	prepared := value["prepared"].(map[string]any)
	if prepared["content"].([]any)[0].(map[string]any)["text"] != "compare source" {
		t.Fatalf("prepared content = %#v", prepared)
	}
	additional := prepared["additionalContext"].(map[string]any)
	prompt := additional["content"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(prompt, "durable source context") || !strings.Contains(prompt, "untrusted, read-only snapshot") {
		t.Fatalf("reference prompt = %q", prompt)
	}
}

func TestDynamicCordisNativeMessageFeedbackFromJavaScript(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "native-service-feedback", "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(session, "assistant/message", map[string]any{
		"message": map[string]any{
			"id": "feedback-message", "role": "assistant",
			"content": []ContentBlock{{Type: "text", Text: "answer"}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "nfeed", `
return {
  inject: ['messageFeedback'],
  apply(ctx) {
    harness.handle('exercise', async args => {
      const put = await ctx.messageFeedback.put({
        sessionId: args.sessionId,
        messageId: 'feedback-message',
        rating: 'positive',
        note: 'useful',
        ifVersion: null
      })
      const listed = await ctx.messageFeedback.list({ sessionId: args.sessionId })
      const deleted = await ctx.messageFeedback.delete({
        sessionId: args.sessionId,
        messageId: 'feedback-message',
        ifVersion: put.value.version
      })
      const empty = await ctx.messageFeedback.list({ sessionId: args.sessionId })
      return { put, listed, deleted, empty }
    })
  }
}`)

	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", map[string]any{"sessionId": sessionID})
	if !result.OK {
		t.Fatalf("exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	put := value["put"].(map[string]any)
	if put["ok"] != true || put["value"].(map[string]any)["rating"] != "positive" {
		t.Fatalf("put = %#v", put)
	}
	listed := value["listed"].(map[string]any)
	if listed["ok"] != true || len(listed["value"].(map[string]any)["items"].([]any)) != 1 {
		t.Fatalf("listed = %#v", listed)
	}
	if value["deleted"].(map[string]any)["ok"] != true {
		t.Fatalf("deleted = %#v", value["deleted"])
	}
	empty := value["empty"].(map[string]any)
	if empty["ok"] != true || len(empty["value"].(map[string]any)["items"].([]any)) != 0 {
		t.Fatalf("empty = %#v", empty)
	}
}

func TestDynamicCordisNativeSessionsGoalsAndJobsFromJavaScript(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "native-lifecycle", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "nlife", `
return {
  inject: ['sessions', 'goals', 'jobs'],
  apply(ctx) {
	ctx.jobs.attachController('native-lifecycle')
    const lifecycle = []
    ctx.on('session/created', session => lifecycle.push('created:' + session.id))
    ctx.on('session/disposed', session => lifecycle.push('disposed:' + session.id))
    harness.handle('exercise', async args => {
      const child = ctx.sessions.create('native-lifecycle-child', { meta: { cwd: args.cwd } })
      const prepared = ctx.sessions.prepare('native-lifecycle-prepared', { meta: { cwd: args.cwd } })
      const hidden = ctx.sessions.get(prepared.id) === undefined
      const detach = ctx.sessions.enter(prepared)
      const entered = ctx.sessions.get(prepared.id) && ctx.sessions.get(prepared.id).id === prepared.id
      ctx.sessions.announce(prepared)
      detach()
      const detached = ctx.sessions.get(prepared.id) === undefined
	  const recycled = ctx.sessions.prepare('native-lifecycle-prepared')
      const header = child.header
      const title = child.append('session/title', { title: 'child' })
      const fetched = ctx.sessions.get(child.id)
      const listed = ctx.sessions.list().map(item => item.id)
      const flushed = await ctx.sessions.flush(child)
      const source = ctx.sessions.get(args.sessionId)
      source.append('turn/start', { turn: 1 })
      source.append('turn/end', { turn: 1, reason: { kind: 'completed' } })
      const forked = ctx.sessions.fork(source, undefined, 'native-lifecycle-fork')
      const goal = ctx.goals.create({ id: args.sessionId }, { objective: 'ship', maxGoalRounds: 2 })
      const edited = ctx.goals.edit({ id: args.sessionId }, { id: goal.id, revision: goal.revision }, { objective: 'ship v2' })
      const paused = ctx.goals.pause({ id: args.sessionId }, { id: edited.id, revision: edited.revision })
      const resumed = ctx.goals.resume({ id: args.sessionId }, { id: paused.id, revision: paused.revision })
      const blocked = ctx.goals.block({ id: args.sessionId }, { id: resumed.id, revision: resumed.revision }, { code: 'needs-input', message: 'waiting' })
      const cleared = ctx.goals.clear({ id: args.sessionId }, { id: blocked.id, revision: blocked.revision })
      const jobId = ctx.jobs.start({
        kind: 'dynamic', label: 'instant',
        run() { return { done: Promise.resolve({ status: 'completed', detail: 'ok', output: 'done' }), readOutput() { return 'delta' }, cancel() {} } }
      })
      const before = ctx.jobs.get(jobId)
      const waited = await ctx.jobs.wait(jobId, 1000)
      const read = ctx.jobs.read(jobId)
      return {
        childId: child.id, header, seq: child.seq, title, fetched: fetched.id, forkedId: forked.id,
				hidden, entered: !!entered, detached, recycled: recycled.id, lifecycle,
        listed, flushed, goal: goal.phase, edited: edited.objective, paused: paused.phase,
        resumed: resumed.activation, blocked: blocked.blockedReason, cleared,
        jobId, before: before.status, waited: waited.status, read: read.text
      }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", map[string]any{
		"sessionId": sessionID,
		"cwd":       e.Config().Workspace,
	})
	if !result.OK {
		t.Fatalf("exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	if value["childId"] != "native-lifecycle-child" || value["fetched"] != "native-lifecycle-child" || value["forkedId"] != "native-lifecycle-fork" || value["seq"] != float64(1) {
		t.Fatalf("session identity = %#v", value)
	}
	if value["hidden"] != true || value["entered"] != true || value["detached"] != true || value["recycled"] != "native-lifecycle-prepared" {
		t.Fatalf("prepared lifecycle = %#v", value)
	}
	lifecycle := value["lifecycle"].([]any)
	if len(lifecycle) != 4 || lifecycle[0] != "created:native-lifecycle-child" || lifecycle[1] != "created:native-lifecycle-prepared" || lifecycle[2] != "disposed:native-lifecycle-prepared" || lifecycle[3] != "created:native-lifecycle-fork" {
		t.Fatalf("lifecycle events = %#v", lifecycle)
	}
	header := value["header"].(map[string]any)
	if header["id"] != "native-lifecycle-child" || header["cwd"] != e.Config().Workspace || header["createdAt"] == nil {
		t.Fatalf("session header = %#v", header)
	}
	if value["title"].(map[string]any)["type"] != "session/title" || value["flushed"] != false {
		t.Fatalf("session append/flush = %#v", value)
	}
	if value["goal"] != "active" || value["edited"] != "ship v2" || value["paused"] != "paused" || value["resumed"] != "armed" {
		t.Fatalf("goal transitions = %#v", value)
	}
	blocked := value["blocked"].(map[string]any)
	if blocked["code"] != "needs-input" || value["cleared"].(map[string]any)["revision"] != float64(6) {
		t.Fatalf("goal block/clear = %#v", value)
	}
	if (value["before"] != "running" && value["before"] != "completed") || value["waited"] != "completed" || value["read"] != "delta" || value["jobId"] != "dynamic-1" {
		t.Fatalf("jobs = %#v", value)
	}
}

func TestDynamicCordisNativeSessionAppendRejectsMissingSurfaceMetadata(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "native-surface", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "nsurf", `
return {
  inject: ['sessions'],
  apply(ctx) {
    harness.handle('exercise', args => {
      const session = ctx.sessions.get(args.sessionId)
      let message = ''
      try { session.append('user/message', { role: 'user', content: [] }) } catch (error) { message = String(error) }
      const event = session.append('user/message', { role: 'user', content: [] }, { surfaceOp: 'append' })
      return { message, event }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", map[string]any{"sessionId": sessionID})
	if !result.OK {
		t.Fatalf("exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	if !strings.Contains(value["message"].(string), "requires surfaceOp") || value["event"].(map[string]any)["surfaceOp"] != "append" {
		t.Fatalf("surface validation = %#v", value)
	}
}

func TestDynamicCordisNativeSessionFlushAwaitsListeners(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "native-flush", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "nflush", `
return {
  inject: ['sessions'],
  apply(ctx) {
    let calls = 0
    ctx.on('session/flush', async session => { calls += session.id === 'native-flush' ? 1 : 0; await Promise.resolve() })
    harness.handle('exercise', async args => ({ flushed: await ctx.sessions.flush({ id: args.sessionId }), calls }))
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", map[string]any{"sessionId": sessionID})
	if !result.OK {
		t.Fatalf("exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	if value["flushed"] != true || value["calls"] != float64(1) {
		t.Fatalf("flush = %#v", value)
	}
}

func TestDynamicCordisNativeSessionForksEmptyAndExactBoundary(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "native-fork-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "nfork", `
return {
  inject: ['sessions'],
  apply(ctx) {
    harness.handle('exercise', args => {
      const source = ctx.sessions.get(args.sessionId)
      const empty = ctx.sessions.fork(source, undefined, 'native-fork-empty')
      const start = source.append('turn/start', { turn: 1 })
      const end = source.append('turn/end', { turn: 1, reason: { kind: 'completed' } })
      source.append('session/title', { title: 'after turn' })
      const exact = ctx.sessions.fork(source, end.seq, 'native-fork-exact')
      let openError = ''
      try { ctx.sessions.fork(source, start.seq, 'native-fork-open') } catch (error) { openError = String(error) }
      return {
        empty: { seq: empty.seq, seedLength: empty.header.seedLength },
        exact: exact.events.map(event => event.type), openError
      }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", map[string]any{"sessionId": sessionID})
	if !result.OK {
		t.Fatalf("exercise = %#v", result)
	}
	value := result.Value.(map[string]any)
	empty := value["empty"].(map[string]any)
	if empty["seedLength"] != float64(0) || empty["seq"] != float64(1) {
		t.Fatalf("empty fork = %#v", empty)
	}
	exact := value["exact"].([]any)
	if len(exact) != 3 || exact[0] != "turn/start" || exact[1] != "turn/end" || exact[2] != "session/end-seed" {
		t.Fatalf("exact fork = %#v", exact)
	}
	if !strings.Contains(value["openError"].(string), "inside an open turn") {
		t.Fatalf("open fork error = %#v", value["openError"])
	}
}
