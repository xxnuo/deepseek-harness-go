package harness

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDynamicCordisSessionFlushAllSettledAcrossRuntimes(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "flush-all-settled", "")
	if err != nil {
		t.Fatal(err)
	}
	ownerID, ownerRun := runDynamicBuiltinPlugin(t, e, sessionID, "flo", `
return {
  inject: ['sessions'],
  apply(ctx) {
    harness.handle('flush', async args => {
      try { return { ok: true, value: await ctx.sessions.flush({ id: args.sessionId }) } }
      catch (error) { return { ok: false, error: String(error) } }
    })
  }
}`)
	listenerID, listenerRun := runDynamicBuiltinPlugin(t, e, sessionID, "fli", `
const calls = []
return {
  inject: ['sessions'],
  apply(ctx) {
    ctx.on('session/flush', () => { calls.push('failure'); throw new Error('disk full') })
    ctx.on('session/flush', async session => { await Promise.resolve(); calls.push(session.id) })
    harness.handle('calls', () => calls)
  }
}`)
	result := e.DynamicCordisInvoke(context.Background(), ownerID, ownerRun, "flush", map[string]any{"sessionId": sessionID})
	if !result.OK {
		t.Fatalf("flush invoke = %#v", result)
	}
	value, ok := result.Value.(map[string]any)
	if !ok || value["ok"] != false || !strings.Contains(value["error"].(string), "disk full") {
		t.Fatalf("flush result = %#v", result.Value)
	}
	calls := e.DynamicCordisInvoke(context.Background(), listenerID, listenerRun, "calls", nil)
	if !calls.OK || !reflect.DeepEqual(calls.Value, []any{"failure", sessionID}) {
		t.Fatalf("flush listeners = %#v", calls)
	}
}

func TestDynamicCordisSessionFlushRejectsDetachedSession(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "flush-detached", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "fldet", `
return {
  inject: ['sessions'],
  apply(ctx) {
    harness.handle('exercise', async args => {
      const prepared = ctx.sessions.prepare('flush-detached-child')
      const detach = ctx.sessions.enter(prepared)
      ctx.sessions.announce(prepared)
      detach()
      try { await ctx.sessions.flush(prepared); return 'unexpected' }
      catch (error) { return String(error) }
    })
  }
}`)
	result := e.DynamicCordisInvoke(context.Background(), pluginID, runID, "exercise", nil)
	if !result.OK || result.Value != "session-not-found: flush-detached-child" {
		t.Fatalf("detached flush = %#v", result)
	}
}

func TestDynamicCordisSeedPreservesEventEnvelope(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "seed-envelope-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "seed", `
return {
  inject: ['sessions'],
  apply(ctx) {
    harness.handle('exercise', () => {
      const prepared = ctx.sessions.prepare('seed-envelope-child', {
        seed: [{ type: 'plugin/test', seq: 0, time: 123, data: { value: 1 } }]
      })
      const detach = ctx.sessions.enter(prepared)
      ctx.sessions.announce(prepared)
      const event = prepared.events[0]
      detach()
      return event
    })
  }
}`)
	result := e.DynamicCordisInvoke(context.Background(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("seed envelope invoke = %#v", result)
	}
	row, ok := result.Value.(map[string]any)
	if !ok || row["seq"] != float64(0) || row["time"] != float64(123) {
		t.Fatalf("seed envelope = %#v", result.Value)
	}
}

func TestDynamicCordisSeedValidatesEnvelopeAndMarker(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "seed-validation-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "sdval", `
return {
  inject: ['sessions'],
  apply(ctx) {
    harness.handle('exercise', () => {
      const attempt = (id, seed) => {
        try { ctx.sessions.prepare(id, { seed }); return '' }
        catch (error) { return String(error) }
      }
	  const attemptOptions = (id, options) => {
	    try { return ctx.sessions.prepare(id, options).header }
	    catch (error) { return String(error) }
	  }
      const fresh = ctx.sessions.prepare('seed-fresh')
      const empty = ctx.sessions.prepare('seed-empty', { seed: [] })
      const existing = ctx.sessions.prepare('seed-existing', { seed: [
        { type: 'session/end-seed', seq: 0, time: -1, data: {} }
      ] })
      return {
        fresh: fresh.events.map(event => event.type),
        empty: { firstLiveSeq: empty.firstLiveSeq, events: empty.events.map(event => event.type) },
        existing: { firstLiveSeq: existing.firstLiveSeq, events: existing.events.map(event => event.type) },
        missingSeq: attempt('seed-missing-seq', [{ type: 'plugin/test', time: 0, data: null }]),
        missingTime: attempt('seed-missing-time', [{ type: 'plugin/test', seq: 0, data: null }]),
        missingData: attempt('seed-missing-data', [{ type: 'plugin/test', seq: 0, time: 0 }]),
        zero: attempt('seed-zero', [{ type: 'plugin/test', seq: 0, time: 0, data: null }]),
        falseIgnorable: attempt('seed-false-ignorable', [{ type: 'plugin/test', seq: 0, time: 0, data: null, ignorable: false }]),
        extra: attempt('seed-extra', [{ type: 'plugin/test', seq: 0, time: 0, data: null, extra: true }]),
		nullSeed: (() => { try { ctx.sessions.prepare('seed-null', { seed: null }); return '' } catch (error) { return String(error) } })(),
		zeroCreatedAt: attemptOptions('meta-zero-created', { meta: { createdAt: 0 } }),
		relativeCwd: attemptOptions('meta-relative-cwd', { meta: { cwd: 'relative' } }),
		nullCwd: attemptOptions('meta-null-cwd', { meta: { cwd: null } }),
		nullCreatedAt: attemptOptions('meta-null-created', { meta: { createdAt: null } })
      }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("seed validation invoke = %#v", result)
	}
	value := result.Value.(map[string]any)
	if len(value["fresh"].([]any)) != 0 {
		t.Fatalf("fresh seed marker = %#v", value["fresh"])
	}
	empty := value["empty"].(map[string]any)
	if empty["firstLiveSeq"] != float64(0) || !reflect.DeepEqual(empty["events"], []any{"session/end-seed"}) {
		t.Fatalf("empty seed = %#v", empty)
	}
	existing := value["existing"].(map[string]any)
	if existing["firstLiveSeq"] != float64(1) || !reflect.DeepEqual(existing["events"], []any{"session/end-seed"}) {
		t.Fatalf("existing marker = %#v", existing)
	}
	for _, key := range []string{"missingSeq", "missingTime", "missingData", "falseIgnorable", "extra"} {
		if !strings.Contains(value[key].(string), "invalid event envelope") {
			t.Fatalf("%s error = %#v", key, value[key])
		}
	}
	if value["zero"] != "" || !strings.Contains(value["nullSeed"].(string), "seed must be an array") {
		t.Fatalf("zero/null seed = %#v", value)
	}
	if value["zeroCreatedAt"].(map[string]any)["createdAt"] != float64(0) ||
		!strings.Contains(value["relativeCwd"].(string), "absolute path") ||
		!strings.Contains(value["nullCwd"].(string), "meta.cwd is invalid") ||
		!strings.Contains(value["nullCreatedAt"].(string), "meta.createdAt is invalid") {
		t.Fatalf("session metadata = %#v", value)
	}
}

func TestDynamicCordisSeedValidatesSurfaceFoldAndPersistenceSource(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "seed-surface-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "sdsurf", `
return {
  inject: ['sessions'],
  apply(ctx) {
    const result = (seq, content, op = 'append', sourceEventSeqs) => ({
      type: 'tool/result', seq, time: seq,
      data: { message: { id: 'result-message', role: 'user', source: { kind: 'tool', callId: 'call-1' }, content: [{ type: 'tool-result', toolCallId: 'call-1', content }] } },
      surfaceOp: op,
      ...(sourceEventSeqs === undefined ? {} : { sourceEventSeqs })
    })
    const attempt = (id, options) => {
      try { return { events: ctx.sessions.prepare(id, options).events.map(event => event.type) } }
      catch (error) { return String(error) }
    }
    harness.handle('exercise', () => ({
      persistence: attempt('seed-persistence', { seedSource: 'persistence', meta: { version: 0, id: 'seed-persistence', createdAt: 1 }, seed: [result(0, [{ type: 'text', text: 'original' }])] }),
      missingPersistenceSeed: attempt('seed-persistence-missing', { seedSource: 'persistence' }),
      missingPersistenceMeta: attempt('seed-persistence-meta', { seedSource: 'persistence', seed: [] }),
      mismatchedPersistenceId: attempt('seed-persistence-id', { seedSource: 'persistence', meta: { version: 0, id: 'different', createdAt: 1 }, seed: [] }),
      unknownSource: attempt('seed-source-unknown', { seedSource: 'import', seed: [] }),
      invalidRange: attempt('seed-invalid-range', { seed: [
        result(0, [{ type: 'text', text: 'original' }]),
        result(1, [], { op: 'replace', start: 5, end: 5 }, [0])
      ] }),
      changedCall: attempt('seed-changed-call', { seed: [
        result(0, [{ type: 'text', text: 'original' }]),
        { ...result(1, [{ type: 'text', text: 'replacement' }], { op: 'replace', start: 0, end: 0 }, [0]),
          data: { message: { id: 'result-message', role: 'user', source: { kind: 'tool', callId: 'call-2' }, content: [{ type: 'tool-result', toolCallId: 'call-2', content: [{ type: 'text', text: 'replacement' }] }] } } }
      ] }),
      contentOnly: attempt('seed-content-only', { seed: [
        result(0, [{ type: 'text', text: 'original' }]),
        result(1, [{ type: 'text', text: 'replacement' }], { op: 'replace', start: 0, end: 0 }, [0])
      ] })
    }))
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("seed surface invoke = %#v", result)
	}
	value := result.Value.(map[string]any)
	for _, key := range []string{"persistence", "contentOnly"} {
		row, ok := value[key].(map[string]any)
		if !ok || len(row["events"].([]any)) == 0 {
			t.Fatalf("%s = %#v", key, value[key])
		}
	}
	for key, want := range map[string]string{
		"missingPersistenceSeed":  "requires seed",
		"missingPersistenceMeta":  "requires meta",
		"mismatchedPersistenceId": "does not match",
		"unknownSource":           "unsupported seedSource",
		"invalidRange":            "invalid range",
		"changedCall":             "may change only content",
	} {
		if message, ok := value[key].(string); !ok || !strings.Contains(message, want) {
			t.Fatalf("%s = %#v, want %q", key, value[key], want)
		}
	}
}

type rollbackTrackingSessionStore struct {
	SessionStore
	appendErr error
	deleted   []string
}

func (s *rollbackTrackingSessionStore) Append(context.Context, string, []Event) error {
	return s.appendErr
}

func (s *rollbackTrackingSessionStore) Delete(ctx context.Context, id string) error {
	s.deleted = append(s.deleted, id)
	return s.SessionStore.(sessionStoreCreateRollback).Delete(ctx, id)
}

func TestDynamicCordisEnterRollsBackFailedPersistentCreate(t *testing.T) {
	backend, err := NewJSONLSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := &rollbackTrackingSessionStore{SessionStore: backend, appendErr: errors.New("forced append failure")}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Provider, cfg.Model = t.TempDir(), t.TempDir(), "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg), WithSessionStore(store))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	owner, err := e.CreateSession(t.Context(), cfg.Workspace, "rollback-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, owner, "rbk", `
return {
  inject: ['sessions'],
  apply(ctx) {
    harness.handle('exercise', () => {
      try {
        ctx.sessions.create('rollback-child', { seed: [{ type: 'plugin/test', seq: 0, time: 0, data: null }] })
        return ''
      } catch (error) { return String(error) }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK || !strings.Contains(result.Value.(string), "forced append failure") {
		t.Fatalf("failed create = %#v", result)
	}
	if !reflect.DeepEqual(store.deleted, []string{"rollback-child"}) {
		t.Fatalf("rolled back ids = %#v", store.deleted)
	}
	if err := backend.Create(t.Context(), SessionHeader{Version: SessionFormatVersion, ID: "rollback-child", CreatedAt: 1}); err != nil {
		t.Fatalf("persistence registration remained: %v", err)
	}
}

func TestDynamicCordisPersistenceRestoreReusesArtifactAndQueues(t *testing.T) {
	openers := []struct {
		name string
		open func(string) (SessionStore, error)
	}{
		{name: "jsonl", open: func(root string) (SessionStore, error) { return NewJSONLSessionStore(filepath.Join(root, "sessions")) }},
		{name: "sqlite", open: func(root string) (SessionStore, error) {
			return NewSQLiteSessionStore(filepath.Join(root, "sessions.db"), SQLiteJournalDelete)
		}},
	}
	for _, opener := range openers {
		for _, existingMarker := range []bool{false, true} {
			name := opener.name + "/missing-marker"
			if existingMarker {
				name = opener.name + "/existing-marker"
			}
			t.Run(name, func(t *testing.T) {
				root, workspace := t.TempDir(), t.TempDir()
				id := "restore-" + opener.name
				meta := SessionHeader{Version: SessionFormatVersion, ID: id, CreatedAt: 1, CWD: workspace}
				events := []Event{
					{Type: "agent/inbox/spliced", Seq: 0, Time: 1, Data: map[string]any{
						"target": "next-turn", "start": 0, "inserted": []any{map[string]any{
							"id": "pending-1", "role": "user", "content": []any{map[string]any{"type": "text", "text": "pending prompt"}}, "source": map[string]any{"kind": "user"},
						}},
					}},
					{Type: "agent/inbox/spliced", Seq: 1, Time: 2, Data: map[string]any{
						"target": "next-step", "start": 0, "inserted": []any{map[string]any{
							"id": "steering-1", "role": "user", "content": []any{map[string]any{"type": "text", "text": "steering prompt"}}, "source": map[string]any{"kind": "user"},
						}},
					}},
				}
				if existingMarker {
					events = append(events, Event{Type: "session/end-seed", Seq: 2, Time: 3, Data: map[string]any{}})
				}
				initial, err := opener.open(root)
				if err != nil {
					t.Fatal(err)
				}
				if err := initial.Create(t.Context(), meta); err != nil {
					t.Fatal(err)
				}
				if err := initial.Append(t.Context(), id, events); err != nil {
					t.Fatal(err)
				}
				if err := initial.Close(); err != nil {
					t.Fatal(err)
				}

				store, err := opener.open(root)
				if err != nil {
					t.Fatal(err)
				}
				cfg := DefaultConfig()
				cfg.DataDir, cfg.Workspace, cfg.Provider, cfg.Model = t.TempDir(), workspace, "echo", "echo"
				cfg.SessionTitleLLM.Enabled = false
				e, err := New(WithConfig(cfg), WithSessionStore(store))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = e.Close() })
				owner, err := e.CreateSession(t.Context(), workspace, "restore-owner-"+opener.name, "")
				if err != nil {
					t.Fatal(err)
				}
				pluginID, runID := runDynamicBuiltinPlugin(t, e, owner, "rst", `
let detach
return {
  inject: ['sessions', 'agents'],
  apply(ctx) {
    harness.handle('resume', args => {
      const prepared = ctx.sessions.prepare(args.meta.id, { meta: args.meta, seed: args.seed, seedSource: 'persistence' })
      const firstLiveSeq = prepared.firstLiveSeq
      detach = ctx.sessions.enter(prepared)
      ctx.sessions.announce(prepared)
      prepared.append('plugin/test', { live: true }, { ignorable: true })
      return { firstLiveSeq, types: prepared.events.map(event => event.type) }
    })
    harness.handle('detach', () => { detach(); return true })
    harness.handle('resumeAgent', async args => {
      const handle = await ctx.agents.resume({
        resumeSessionId: args.id,
        agentOptions: { provider: 'echo', model: 'echo' }
      })
      const value = {
        firstLiveSeq: handle.agent.session.firstLiveSeq,
        types: handle.agent.session.events.map(event => event.type)
      }
      await handle.dispose()
      return value
    })
  }
}`)
				seed := make([]any, len(events))
				for index, event := range events {
					seed[index] = dynamicSessionEventValue(event)
				}
				result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "resume", map[string]any{
					"meta": dynamicSessionHeaderValue(meta), "seed": seed,
				})
				if !result.OK {
					t.Fatalf("resume = %#v", result)
				}
				value := result.Value.(map[string]any)
				if value["firstLiveSeq"] != float64(len(events)) || !reflect.DeepEqual(value["types"], []any{
					"agent/inbox/spliced", "agent/inbox/spliced", "session/end-seed", "plugin/test",
				}) {
					t.Fatalf("restored session = %#v", value)
				}
				session, err := e.getSession(id)
				if err != nil {
					t.Fatal(err)
				}
				session.mu.Lock()
				pending, steering := append([]*queuedPrompt(nil), session.pending...), append([]*queuedPrompt(nil), session.steering...)
				session.mu.Unlock()
				if len(pending) != 1 || pending[0].text != "pending prompt" || len(steering) != 1 || steering[0].text != "steering prompt" {
					t.Fatalf("restored queues = pending %#v, steering %#v", pending, steering)
				}
				if detached := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "detach", nil); !detached.OK || detached.Value != true {
					t.Fatalf("detach = %#v", detached)
				}
				if _, err := e.getSession(id); err == nil {
					t.Fatal("detached restored session remained live")
				}
				inspection, err := store.Load(t.Context(), id)
				if err != nil {
					t.Fatalf("artifact was removed on detach: %v", err)
				}
				if len(inspection.Events) != 4 {
					t.Fatalf("persisted events = %#v", inspection.Events)
				}
				resumed := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "resumeAgent", map[string]any{"id": id})
				if !resumed.OK {
					t.Fatalf("persisted agent resume = %#v", resumed)
				}
				resumedValue := resumed.Value.(map[string]any)
				if resumedValue["firstLiveSeq"] != float64(4) || !reflect.DeepEqual(resumedValue["types"], []any{
					"agent/inbox/spliced", "agent/inbox/spliced", "session/end-seed", "plugin/test", "session/end-seed",
				}) {
					t.Fatalf("persisted agent = %#v", resumedValue)
				}
				inspection, err = store.Load(t.Context(), id)
				if err != nil {
					t.Fatal(err)
				}
				if len(inspection.Events) != 7 || inspection.Events[5].Type != "agent/inbox/spliced" || inspection.Events[6].Type != "agent/inbox/spliced" {
					t.Fatalf("persisted agent events = %#v", inspection.Events)
				}
			})
		}
	}
}

func TestJSONLSessionStoreDeleteRemovesMaterializedSession(t *testing.T) {
	store, err := NewJSONLSessionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta := SessionHeader{Version: SessionFormatVersion, ID: "delete-materialized", CreatedAt: 1}
	if err := store.Create(t.Context(), meta); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(t.Context(), meta.ID, []Event{{Type: "plugin/test", Seq: 0, Time: 1, Data: nil, Ignorable: true}}); err != nil {
		t.Fatal(err)
	}
	location, _ := store.Locate(meta)
	if err := store.Delete(t.Context(), meta.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(location.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("materialized artifact remained: %v", err)
	}
	if err := store.Create(t.Context(), meta); err != nil {
		t.Fatalf("session remained registered: %v", err)
	}
}

func TestDynamicCordisAnnounceFailureRollsBackAndDisposes(t *testing.T) {
	e := newIntegrationEngine(t)
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "announce-rollback-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, owner, "annrb", `
let failed = false
const disposed = []
return {
  inject: ['sessions'],
  apply(ctx) {
    ctx.on('session/created', session => {
      if (session.id === 'announce-rollback-child' && !failed) {
        failed = true
        throw new Error('created veto')
      }
    })
    ctx.on('session/disposed', session => disposed.push(session.id))
    harness.handle('exercise', () => {
      let error = ''
      try { ctx.sessions.create('announce-rollback-child') } catch (caught) { error = String(caught) }
      const liveAfterFailure = ctx.sessions.get('announce-rollback-child') !== undefined
      const created = ctx.sessions.create('announce-rollback-child')
      return { error, liveAfterFailure, created: created.id, disposed }
    })
  }
}`)
	result := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "exercise", nil)
	if !result.OK {
		t.Fatalf("announce rollback invoke = %#v", result)
	}
	value := result.Value.(map[string]any)
	if !strings.Contains(value["error"].(string), "created veto") || value["liveAfterFailure"] != false || value["created"] != "announce-rollback-child" ||
		!reflect.DeepEqual(value["disposed"], []any{"announce-rollback-child"}) {
		t.Fatalf("announce rollback = %#v", value)
	}
}

func TestDynamicCordisRunCleanupDetachesOwnedSessionsAndAgents(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "cleanup-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "clean", `
return {
  inject: ['sessions', 'agents'],
  apply(ctx) {
    harness.handle('make', async () => {
      ctx.sessions.create('cleanup-child')
      await ctx.agents.create({ sessionId: 'cleanup-agent', agentOptions: { provider: 'echo', model: 'echo' } })
      return true
    })
  }
}`)
	result := e.DynamicCordisInvoke(context.Background(), pluginID, runID, "make", nil)
	if !result.OK {
		t.Fatalf("cleanup setup = %#v", result)
	}
	if stopped, err := e.DynamicCordisStop(sessionID, pluginID); err != nil || !stopped.OK {
		t.Fatalf("stop = %#v / %v", stopped, err)
	}
	if _, err := e.getSession("cleanup-child"); err == nil {
		t.Fatal("owned dynamic session remained after plugin stop")
	}
	if agent, err := e.getSession("cleanup-agent"); err == nil {
		agent.mu.Lock()
		live := agent.attached
		agent.mu.Unlock()
		if live {
			t.Fatal("owned dynamic agent remained live after plugin stop")
		}
	}
}

func TestDynamicCordisJobsControllerAndOwnerCallbacks(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "jobs-controller", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "jctl", `
return {
  inject: ['jobs'],
  apply(ctx) {
    const events = []
    ctx.jobs.onJobsChanged(owner => events.push('changed:' + owner.id))
    ctx.jobs.onJobDone((job, owner) => events.push('done:' + job.id + ':' + owner.id))
    harness.handle('exercise', async args => {
      let rejected = ''
      const spec = {
        kind: 'dynamic', label: 'controlled', owner: { id: args.sessionId },
        run() { return { done: Promise.resolve({ status: 'completed', detail: 'ok' }), cancel() {} } }
      }
      try { ctx.jobs.start(spec) } catch (error) { rejected = String(error) }
      const detach = ctx.jobs.attachController('test-controller')
      const jobId = ctx.jobs.start(spec)
      await ctx.jobs.wait(jobId, 1000, { id: args.sessionId })
      for (let index = 0; index < 8 && events.length < 3; index++) await Promise.resolve()
      detach()
      let detached = ''
      try { ctx.jobs.start(spec) } catch (error) { detached = String(error) }
      return { rejected, detached, events }
    })
  }
}`)
	result := e.DynamicCordisInvoke(context.Background(), pluginID, runID, "exercise", map[string]any{"sessionId": sessionID})
	if !result.OK {
		t.Fatalf("jobs invoke = %#v", result)
	}
	value := result.Value.(map[string]any)
	if !strings.Contains(value["rejected"].(string), "no job controller") || !strings.Contains(value["detached"].(string), "no job controller") {
		t.Fatalf("controller admission = %#v", value)
	}
	events := value["events"].([]any)
	want := []any{"changed:" + sessionID, "changed:" + sessionID, "done:dynamic-1:" + sessionID}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("job callbacks = %#v, want %#v", events, want)
	}
}

func TestDynamicCordisJobsControllerComposesAcrossPluginRuns(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "jobs-cross-plugin", "")
	if err != nil {
		t.Fatal(err)
	}
	controllerID, _ := runDynamicBuiltinPlugin(t, e, sessionID, "jown", `
return {
  inject: ['jobs'],
  apply(ctx) { ctx.jobs.attachController('cross-plugin') }
}`)
	producerID, producerRun := runDynamicBuiltinPlugin(t, e, sessionID, "jpro", `
return {
  inject: ['jobs'],
  apply(ctx) {
    harness.handle('exercise', () => ctx.jobs.start({
      kind: 'dynamic', label: 'cross-plugin', owner: { id: '`+sessionID+`' },
      run() { return { done: Promise.resolve({ status: 'completed' }), cancel() {} } }
    }))
  }
}`)
	result := e.DynamicCordisInvoke(context.Background(), producerID, producerRun, "exercise", nil)
	if !result.OK {
		t.Fatalf("cross-plugin controller = %#v", result)
	}
	if _, ok := result.Value.(string); !ok {
		t.Fatalf("cross-plugin job id = %#v", result.Value)
	}
	_, _ = e.DynamicCordisStop(sessionID, producerID)
	_, _ = e.DynamicCordisStop(sessionID, controllerID)
}

func TestDynamicCordisJobsControllerDoesNotServeOtherPluginOwner(t *testing.T) {
	e := newIntegrationEngine(t)
	controllerOwner, err := e.CreateSession(t.Context(), e.Config().Workspace, "jobs-controller-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	producerOwner, err := e.CreateSession(t.Context(), e.Config().Workspace, "jobs-producer-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	controllerID, _ := runDynamicBuiltinPlugin(t, e, controllerOwner, "jcc", `
return {
  inject: ['jobs'],
  apply(ctx) { ctx.jobs.attachController('cross-owner-controller') }
}`)
	producerID, producerRun := runDynamicBuiltinPlugin(t, e, producerOwner, "jcp", `
return {
  inject: ['jobs'],
  apply(ctx) {
    harness.handle('start', () => ctx.jobs.start({
      kind: 'cross', label: 'served',
      run() { return { done: Promise.resolve({ status: 'completed', detail: 'ok' }) } }
    }))
    harness.handle('foreign', args => {
      try {
        ctx.jobs.start({
          kind: 'cross', label: 'foreign', owner: { id: args.owner },
          run() { return { done: Promise.resolve({ status: 'completed' }) } }
        })
        return 'unexpected'
      } catch (error) {
        return String(error)
      }
    })
  }
}`)
	started := e.DynamicCordisInvoke(t.Context(), producerID, producerRun, "start", nil)
	if started.OK || !strings.Contains(started.Message, "no job controller") {
		t.Fatalf("cross-owner start = %#v", started)
	}
	foreign := e.DynamicCordisInvoke(t.Context(), producerID, producerRun, "foreign", map[string]any{"owner": controllerOwner})
	if !foreign.OK || !strings.Contains(foreign.Value.(string), "job operation is limited to the current session") {
		t.Fatalf("foreign owner admission = %#v", foreign)
	}
	if stopped, err := e.DynamicCordisStop(controllerOwner, controllerID); err != nil || !stopped.OK {
		t.Fatalf("controller stop = %#v, %v", stopped, err)
	}
	if stopped, err := e.DynamicCordisStop(producerOwner, producerID); err != nil || !stopped.OK {
		t.Fatalf("producer stop = %#v, %v", stopped, err)
	}
}

func TestDynamicCordisJobListenersAreOwnerRelative(t *testing.T) {
	e := newIntegrationEngine(t)
	ownerA, err := e.CreateSession(t.Context(), e.Config().Workspace, "jobs-listener-a", "")
	if err != nil {
		t.Fatal(err)
	}
	ownerB, err := e.CreateSession(t.Context(), e.Config().Workspace, "jobs-listener-b", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginA, runA := runDynamicBuiltinPlugin(t, e, ownerA, "jla", `
return {
  inject: ['jobs'],
  apply(ctx) {
    const events = []
    ctx.jobs.onJobsChanged(owner => events.push('changed:' + owner.id))
    ctx.jobs.onJobDone((job, owner) => events.push('done:' + job.id + ':' + owner.id))
    harness.handle('events', () => events)
  }
}`)
	pluginB, runB := runDynamicBuiltinPlugin(t, e, ownerB, "jlb", `
return {
  inject: ['jobs'],
  apply(ctx) {
    const events = []
    ctx.jobs.attachController('owner-b')
    ctx.jobs.onJobsChanged(owner => events.push('changed:' + owner.id))
    ctx.jobs.onJobDone((job, owner) => events.push('done:' + job.id + ':' + owner.id))
    harness.handle('exercise', async owner => {
      const id = ctx.jobs.start({
        kind: 'scope', label: 'owner-relative', owner: { id: owner },
        run() { return { done: Promise.resolve({ status: 'completed' }), cancel() {} } },
      })
      await ctx.jobs.wait(id, 1000, { id: owner })
      for (let index = 0; index < 8 && events.length < 3; index++) await Promise.resolve()
      return events
    })
  }
}`)
	eventsB := e.DynamicCordisInvoke(t.Context(), pluginB, runB, "exercise", ownerB)
	if !eventsB.OK {
		t.Fatalf("owner B exercise = %#v", eventsB)
	}
	wantB := []any{"changed:" + ownerB, "changed:" + ownerB, "done:scope-1:" + ownerB}
	if !reflect.DeepEqual(eventsB.Value, wantB) {
		t.Fatalf("owner B events = %#v, want %#v", eventsB.Value, wantB)
	}
	eventsA := e.DynamicCordisInvoke(t.Context(), pluginA, runA, "events", nil)
	if !eventsA.OK || len(eventsA.Value.([]any)) != 0 {
		t.Fatalf("foreign owner listener received events = %#v", eventsA)
	}
}
