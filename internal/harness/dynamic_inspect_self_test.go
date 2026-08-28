package harness

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDynamicCordisInspectSelfMatchesLayeredContract(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false), WithSessionTitleLLM(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-self-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	other, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-self-other", "")
	if err != nil {
		t.Fatal(err)
	}
	first, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "first"},
		Name:      "first v1", Purpose: "first package",
		Code: DynamicCordisCode{Host: `return { inject: ['zeta', 'alpha'], apply() {} }`},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "second"},
		Name:      "second v1", Purpose: "second package",
		Code: DynamicCordisCode{Host: `return { apply() {} }`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: other,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "other"},
		Name:      "other v1", Purpose: "other package",
		Code: DynamicCordisCode{Host: `return { apply() {} }`},
	}); err != nil {
		t.Fatal(err)
	}

	run, err := e.DynamicCordisRun(context.Background(), owner, first.PluginID, first.PackageID, "run")
	if err != nil || !run.OK || !reflect.DeepEqual(run.WaitingFor, []string{"zeta", "alpha"}) {
		t.Fatalf("first run = %#v, %v", run, err)
	}
	update, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "existing", PluginID: first.PluginID},
		Name:      "first v2", Purpose: "client update",
		Code: DynamicCordisCode{Client: `return { apply() {} }`},
	})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := e.DynamicCordisRun(context.Background(), owner, first.PluginID, update.PackageID, "update")
	if err != nil || !pending.OK || pending.Status != "awaiting-approval" {
		t.Fatalf("pending update = %#v, %v", pending, err)
	}

	listed, err := e.DynamicCordisInspectSelf(owner, "", "")
	if err != nil {
		t.Fatal(err)
	}
	plugins, ok := listed["plugins"].([]map[string]any)
	if !ok || len(plugins) != 2 {
		t.Fatalf("plugins = %#v", listed["plugins"])
	}
	if plugins[0]["pluginId"] != first.PluginID || plugins[1]["pluginId"] != second.PluginID {
		t.Fatalf("plugin order = %#v", plugins)
	}
	if _, exists := plugins[0]["agentId"]; exists {
		t.Fatalf("self summary leaked agentId: %#v", plugins[0])
	}
	if _, exists := plugins[0]["latestRun"]; exists {
		t.Fatalf("self summary leaked latestRun: %#v", plugins[0])
	}
	if plugins[0]["name"] != "first v2" || plugins[0]["state"] != "awaiting-approval" {
		t.Fatalf("first summary = %#v", plugins[0])
	}

	pluginView, err := e.DynamicCordisInspectSelf(owner, first.PluginID, "")
	if err != nil {
		t.Fatal(err)
	}
	if pluginView["mode"] != "plugin" || pluginView["plugin"] != nil {
		t.Fatalf("plugin view shape = %#v", pluginView)
	}
	packages, ok := pluginView["packages"].([]map[string]any)
	if !ok || len(packages) != 2 {
		t.Fatalf("package summaries = %#v", pluginView["packages"])
	}
	if packages[0]["isCurrent"] != true || packages[0]["isNext"] != false || packages[1]["isNext"] != true {
		t.Fatalf("version pointers = %#v", packages)
	}

	packageView, err := e.DynamicCordisInspectSelf(owner, first.PluginID, update.PackageID)
	if err != nil {
		t.Fatal(err)
	}
	if packageView["mode"] != "package" || packageView["plugin"] == nil {
		t.Fatalf("package view shape = %#v", packageView)
	}
	code, ok := packageView["code"].(map[string]any)
	if !ok || code["client"] == nil {
		t.Fatalf("package code = %#v", packageView["code"])
	}
	if _, exists := code["host"]; exists {
		t.Fatalf("absent host source was serialized: %#v", code)
	}
	runtime, ok := packageView["runtime"].(map[string]any)
	if !ok || runtime["state"] != "awaiting-approval" {
		t.Fatalf("package runtime = %#v", packageView["runtime"])
	}
	host := runtime["host"].(map[string]any)
	client := runtime["client"].(map[string]any)
	if host["status"] != "absent" || client["status"] != "pending" {
		t.Fatalf("half runtime = host %#v client %#v", host, client)
	}
}

func TestDynamicCordisEffectAwaitsAsyncSetupAndCleanup(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false), WithSessionTitleLLM(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-effect-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	defined, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "effect"},
		Name:      "async effect", Purpose: "await setup and cleanup",
		Code: DynamicCordisCode{Host: `
let releaseCleanup
const cleanupGate = new Promise(resolve => { releaseCleanup = resolve })
return {
  inject: ['timer'],
  apply(ctx) {
    ctx.setTimeout(releaseCleanup, 80)
    ctx.effect(async () => {
      await ctx.timeout(20)
      return async () => { await cleanupGate }
    }, 'async-effect')
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	run, err := e.DynamicCordisRun(t.Context(), owner, defined.PluginID, defined.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	if elapsed := time.Since(started); elapsed < 15*time.Millisecond {
		t.Fatalf("run returned before async effect setup settled: %s", elapsed)
	}
	started = time.Now()
	stopped, err := e.DynamicCordisStop(owner, defined.PluginID)
	if err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	if elapsed := time.Since(started); elapsed < 35*time.Millisecond {
		t.Fatalf("stop returned before async effect cleanup settled: %s", elapsed)
	}
}

func TestDynamicCordisEffectConsumesSyncAndAsyncIterables(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false), WithSessionTitleLLM(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-effect-iterables", "")
	if err != nil {
		t.Fatal(err)
	}
	recorderID, recorderRun := runDynamicBuiltinPlugin(t, e, owner, "rec", `
const calls = []
harness.handle('calls', () => calls)
return { apply(ctx) { ctx.provide('effectRecorder', { push(value) { calls.push(value) } }) } }
`)
	effect, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "iter"},
		Name:      "iterable effect", Purpose: "consume generator effect shapes",
		Code: DynamicCordisCode{Host: `
return {
  inject: ['effectRecorder', 'timer'],
  apply(ctx) {
    ctx.effect(function* () {
      yield () => ctx.effectRecorder.push('sync-1')
      yield async () => { await ctx.timeout(5); ctx.effectRecorder.push('sync-2') }
    })
    ctx.effect(async function* () {
      await ctx.timeout(5)
      yield () => ctx.effectRecorder.push('async-1')
      yield async () => { await ctx.timeout(5); ctx.effectRecorder.push('async-2') }
    })
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	started, err := e.DynamicCordisRun(t.Context(), owner, effect.PluginID, effect.PackageID, "run")
	if err != nil || !started.OK {
		t.Fatalf("iterable effect run = %#v, %v", started, err)
	}
	if stopped, err := e.DynamicCordisStop(owner, effect.PluginID); err != nil || !stopped.OK {
		t.Fatalf("iterable effect stop = %#v, %v", stopped, err)
	}
	calls := e.DynamicCordisInvoke(t.Context(), recorderID, recorderRun, "calls", nil)
	want := []any{"async-2", "async-1", "sync-2", "sync-1"}
	if !calls.OK || !reflect.DeepEqual(calls.Value, want) {
		t.Fatalf("iterable cleanup order = %#v, want %#v", calls, want)
	}
}

func TestDynamicCordisEffectRejectsCleanupTimeRegistration(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false), WithSessionTitleLLM(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-effect-unload", "")
	if err != nil {
		t.Fatal(err)
	}
	defined, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "unload"},
		Name:      "unload guard", Purpose: "reject late effects",
		Code: DynamicCordisCode{Host: `return { apply(ctx) {
  ctx.effect(() => () => {
    try { ctx.effect(() => () => {}, 'too-late') } catch {}
  }, 'cleanup')
} }`},
	})
	if err != nil {
		t.Fatal(err)
	}
	runResult, err := e.DynamicCordisRun(t.Context(), owner, defined.PluginID, defined.PackageID, "run")
	if err != nil || !runResult.OK {
		t.Fatalf("run = %#v, %v", runResult, err)
	}
	e.dynamicCordis.RLock()
	run := e.dynamicCordis.plugins[defined.PluginID].run
	e.dynamicCordis.RUnlock()
	if run == nil {
		t.Fatal("run was not installed")
	}
	stopped, err := e.DynamicCordisStop(owner, defined.PluginID)
	if err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	if len(run.disposers) != 0 {
		t.Fatalf("cleanup-time effect registration survived unload: %d disposers", len(run.disposers))
	}
}

func TestDynamicCordisFailedUpdateRetainsNextAndAllowsRollback(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false), WithSessionTitleLLM(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-version-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	first, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "clock"},
		Name:      "clock v1", Purpose: "working version",
		Code: DynamicCordisCode{Host: `return { apply() {} }`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run, runErr := e.DynamicCordisRun(t.Context(), owner, first.PluginID, first.PackageID, "run"); runErr != nil || !run.OK {
		t.Fatalf("first run = %#v, %v", run, runErr)
	}
	second, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "existing", PluginID: first.PluginID},
		Name:      "clock v2", Purpose: "broken update",
		Code: DynamicCordisCode{Host: `throw new Error('broken update')`},
	})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := e.DynamicCordisRun(t.Context(), owner, first.PluginID, second.PackageID, "update")
	if err != nil || failed.OK || failed.Reason != "host-half-failed" {
		t.Fatalf("failed update = %#v, %v", failed, err)
	}
	before := e.DynamicCordisInventory(owner)
	if len(before) != 1 || before[0].CurrentPackageID != first.PackageID || before[0].NextPackageID != second.PackageID || before[0].ActiveRun != nil {
		t.Fatalf("failed version pointers = %#v", before)
	}
	rollback, err := e.DynamicCordisRun(t.Context(), owner, first.PluginID, first.PackageID, "run")
	if err != nil || !rollback.OK {
		t.Fatalf("rollback = %#v, %v", rollback, err)
	}
	after := e.DynamicCordisInventory(owner)
	if len(after) != 1 || after[0].CurrentPackageID != first.PackageID || after[0].NextPackageID != "" || after[0].ActiveRun == nil || after[0].ActiveRun.PackageID != first.PackageID {
		t.Fatalf("rollback version pointers = %#v", after)
	}
}

func TestDynamicCordisClientFailureRetractsOwnedHostActivation(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false), WithSessionTitleLLM(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-client-failure", "")
	if err != nil {
		t.Fatal(err)
	}
	define := func(prefix, service string) DynamicCordisDefineReceipt {
		t.Helper()
		receipt, defineErr := e.DynamicCordisDefine(DynamicCordisDefineRequest{
			SessionID: owner,
			Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: prefix},
			Name:      prefix, Purpose: "client failure cleanup",
			Code: DynamicCordisCode{
				Host:   `return { apply(ctx) { ctx.provide('` + service + `', { ok: true }) } }`,
				Client: `return { apply() {} }`,
			},
		})
		if defineErr != nil {
			t.Fatal(defineErr)
		}
		return receipt
	}

	model := define("model", "modelFailureService")
	pending, err := e.DynamicCordisRun(t.Context(), owner, model.PluginID, model.PackageID, "run")
	if err != nil || !pending.OK || pending.Status != "awaiting-approval" {
		t.Fatalf("pending model run = %#v, %v", pending, err)
	}
	e.dynamicCordis.RLock()
	requestID := dynamicCordisAttemptString(e.dynamicCordis.plugins[model.PluginID].latest, "approvalRequestId")
	e.dynamicCordis.RUnlock()
	started, err := e.DynamicCordisRunHostHalf(t.Context(), owner, model.PluginID, model.PackageID, "run", requestID, false)
	if err != nil || !started.OK || !started.StartedHere {
		t.Fatalf("model host start = %#v, %v", started, err)
	}
	if !e.dynamicCordisServiceExists("modelFailureService") {
		t.Fatal("model Host service was not installed")
	}
	startedHere := true
	ack, err := e.DynamicCordisResolveRequestRun(requestID, DynamicCordisRunResolution{
		Reason: "client-half-failed", Message: "browser failed", PluginRunID: started.PluginRunID, StartedHere: &startedHere,
	})
	if err != nil || ack["accepted"] != true {
		t.Fatalf("model failure resolution = %#v, %v", ack, err)
	}
	if e.dynamicCordisServiceExists("modelFailureService") {
		t.Fatal("model-owned Host service survived Client failure")
	}

	panel := define("panel", "panelFailureService")
	panelStart, err := e.DynamicCordisRunHostHalf(t.Context(), owner, panel.PluginID, panel.PackageID, "run", "", false)
	if err != nil || !panelStart.OK || !panelStart.StartedHere {
		t.Fatalf("panel host start = %#v, %v", panelStart, err)
	}
	if !e.dynamicCordisServiceExists("panelFailureService") {
		t.Fatal("panel Host service was not installed")
	}
	settled, err := e.DynamicCordisSettleUserRun(owner, panel.PluginID, DynamicCordisRunResolution{
		Reason: "client-half-failed", Message: "page failed", PluginRunID: panelStart.PluginRunID, StartedHere: &startedHere,
	})
	if err != nil || settled.OK || settled.Reason != "client-half-failed" {
		t.Fatalf("panel failure resolution = %#v, %v", settled, err)
	}
	if e.dynamicCordisServiceExists("panelFailureService") {
		t.Fatal("panel-owned Host service survived Client failure")
	}
}

func TestDynamicCordisReferencesUseOnlyDirectUserMessages(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false), WithSessionTitleLLM(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-reference-owner", "")
	if err != nil {
		t.Fatal(err)
	}
	defined, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: owner,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "refer"},
		Name:      "reference base", Purpose: "reference context",
		Code: DynamicCordisCode{Host: `return { apply() {} }`},
	})
	if err != nil {
		t.Fatal(err)
	}
	messages := []ChatMessage{
		{Role: "user", Content: "modify @" + defined.PluginID + " and @" + defined.PluginID + "\nthen inspect @lost-404", Source: map[string]any{"kind": "user"}},
		{Role: "user", Content: "ignore @fake-9", Source: map[string]any{"kind": "plugin"}},
		{Role: "assistant", Content: "ignore @fake-10"},
	}
	got := e.dynamicCordisReferenceMessages(owner, messages, agentRuntime{toolNames: map[string]bool{"cordis_inspect_self": true}})
	if len(got) != len(messages)+2 {
		t.Fatalf("reference contexts = %#v", got)
	}
	available := got[len(messages)].Content
	if !strings.Contains(available, `"pluginId": "`+defined.PluginID+`"`) || !strings.Contains(available, "Use Package "+defined.PackageID) {
		t.Fatalf("available reference = %q", available)
	}
	if strings.Contains(available, `return { apply() {} }`) {
		t.Fatalf("reference leaked source: %q", available)
	}
	unavailable := got[len(messages)+1].Content
	if !strings.Contains(unavailable, "@lost-404") || !strings.Contains(unavailable, "unavailable in the current Session") {
		t.Fatalf("unavailable reference = %q", unavailable)
	}
	if stringValue(got[len(messages)].Source["kind"]) != "plugin" {
		t.Fatalf("reference source = %#v", got[len(messages)].Source)
	}
}
