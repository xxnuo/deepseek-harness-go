package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	harness "github.com/xxnuo/deepseek-harness-go"
	"gopkg.in/yaml.v3"
)

func TestProfileRuntimeUsesLoaderDefaultExportUnwrapSemantics(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed.mjs")
	if err := os.WriteFile(path, []byte(`
export const inject = ['neverReady']
export default function (ctx) { ctx.provide('profileDefaultUnwrapWitness', { active: true }) }
`), 0o644); err != nil {
		t.Fatal(err)
	}
	composed := testComposition(t, "- id: mixed\n  name: "+path+"\n")
	composed.profileDir = dir
	plugins, err := buildProfileRuntimePlugins(composed)
	if err != nil || len(plugins) != 1 {
		t.Fatalf("built plugins = %#v, %v", plugins, err)
	}
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	mounted, err := mountProfileRuntimePluginSet(engine, nil, plugins)
	if err != nil {
		t.Fatal(err)
	}
	plugin := mounted.plugins["mixed"]
	view, err := engine.DynamicCordisInspectSelf("", plugin.pluginID, plugin.packageID)
	if err != nil {
		t.Fatal(err)
	}
	runtimeView, _ := view["runtime"].(map[string]any)
	host, _ := runtimeView["host"].(map[string]any)
	if host["status"] != "running" {
		t.Fatalf("mixed default/named export status = %#v; named inject should be discarded by Loader unwrap", host)
	}
}

func TestProfileRuntimeValidatesAndNormalizesPluginConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "configured.mjs")
	if err := os.WriteFile(path, []byte(`
export const Config = {
  '~standard': {
    validate(value) {
      if (value.count === 3) return Promise.resolve({ value })
      if (value.count !== 2) return { issues: [{ message: 'count must equal 2', path: ['count'] }] }
      return { value: { normalized: value.count * 3 } }
    },
  },
}
export function apply(ctx, config) {
  if (config.normalized !== 6) throw new Error('config was not normalized')
  ctx.provide('profileConfigWitness', config)
}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	build := func(count int) []profileRuntimePlugin {
		composed := testComposition(t, "- id: configured\n  name: "+path+"\n  config: {count: "+fmt.Sprint(count)+"}\n")
		composed.profileDir = dir
		plugins, err := buildProfileRuntimePlugins(composed)
		if err != nil {
			t.Fatal(err)
		}
		return plugins
	}
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if _, err := mountProfileRuntimePluginSet(engine, nil, build(1)); err == nil || !strings.Contains(err.Error(), "count must equal 2 (at count)") {
		t.Fatalf("invalid plugin config error = %v", err)
	}
	if _, err := mountProfileRuntimePluginSet(engine, nil, build(3)); err == nil || !strings.Contains(err.Error(), "Async config validation is not supported") {
		t.Fatalf("async plugin config error = %v", err)
	}
	if _, err := mountProfileRuntimePluginSet(engine, nil, build(2)); err != nil {
		t.Fatalf("normalized plugin config failed: %v", err)
	}
}

func TestResolveProfilePluginPathUsesNodeExportsSemantics(t *testing.T) {
	profileDir := t.TempDir()
	packageDir := filepath.Join(profileDir, "node_modules", "ordered-exports")
	if err := os.MkdirAll(filepath.Join(packageDir, "features"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `{
  "name": "ordered-exports",
  "exports": {
    ".": {"node": "./node.mjs", "import": "./import.mjs", "default": "./default.mjs"},
    "./features/*": "./features/*.mjs"
  }
}`
	for path, content := range map[string]string{
		"package.json":      manifest,
		"node.mjs":          "export default () => {}",
		"import.mjs":        "export default () => {}",
		"default.mjs":       "export default () => {}",
		"features/tool.mjs": "export default () => {}",
		"private.mjs":       "export default () => {}",
	} {
		if err := os.WriteFile(filepath.Join(packageDir, filepath.FromSlash(path)), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	root, err := resolveProfilePluginPath(profileDir, "ordered-exports")
	if err != nil || root != filepath.Join(packageDir, "node.mjs") {
		t.Fatalf("ordered root export = %q, %v", root, err)
	}
	feature, err := resolveProfilePluginPath(profileDir, "ordered-exports/features/tool")
	if err != nil || feature != filepath.Join(packageDir, "features", "tool.mjs") {
		t.Fatalf("wildcard export = %q, %v", feature, err)
	}
	if _, err := resolveProfilePluginPath(profileDir, "ordered-exports/private"); err == nil || !strings.Contains(err.Error(), "is not exported") {
		t.Fatalf("sealed subpath error = %v", err)
	}
}

func TestProfileRuntimeConstructsClassPluginsAndRunsInit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "class-plugin.mjs")
	if err := os.WriteFile(path, []byte(`
export default class ClassPlugin {
  constructor(ctx, config) {
    this.ctx = ctx
    this.config = config
    ctx.reflect.provide('profileClassWitness', this)
    this[Symbol.for('cordis.initHooks')] = [() => { this.hooked = true }]
  }
  [Symbol.for('cordis.init')]() {
    if (!this.hooked || this.config.value !== 7) throw new Error('class lifecycle was not initialized')
  }
  read() { return this.config.value }
}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	composed := testComposition(t, "- id: class-plugin\n  name: "+path+"\n  config: {value: 7}\n")
	composed.profileDir = dir
	plugins, err := buildProfileRuntimePlugins(composed)
	if err != nil {
		t.Fatal(err)
	}
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	mounted, err := mountProfileRuntimePluginSet(engine, nil, plugins)
	if err != nil {
		t.Fatal(err)
	}
	plugin := mounted.plugins["class-plugin"]
	view, err := engine.DynamicCordisInspectSelf("", plugin.pluginID, plugin.packageID)
	if err != nil {
		t.Fatal(err)
	}
	runtimeView, _ := view["runtime"].(map[string]any)
	host, _ := runtimeView["host"].(map[string]any)
	if host["status"] != "running" {
		t.Fatalf("class plugin status = %#v", host)
	}
	consumer, err := engine.DynamicCordisDefine(harness.DynamicCordisDefineRequest{
		Plugin:  harness.DynamicCordisPluginSelector{Kind: "new", IDPrefix: "cons"},
		Name:    "class service consumer",
		Purpose: "verify class service ownership",
		Code: harness.DynamicCordisCode{Host: `
let service
harness.handle('read', () => service.read())
return { inject: ['profileClassWitness'], apply(ctx) { service = ctx.profileClassWitness } }
`},
		Global: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumerRun, err := engine.DynamicCordisRun(t.Context(), "", consumer.PluginID, consumer.PackageID, "run")
	if err != nil || !consumerRun.OK || len(consumerRun.WaitingFor) != 0 {
		t.Fatalf("class consumer run = %#v, %v", consumerRun, err)
	}
	if result := engine.DynamicCordisInvoke(t.Context(), consumer.PluginID, consumerRun.PluginRunID, "read", nil); !result.OK || result.Value != float64(7) {
		t.Fatalf("class service read = %#v", result)
	}
	if err := mounted.reconcile(nil); err != nil {
		t.Fatal(err)
	}
	if result := engine.DynamicCordisInvoke(t.Context(), consumer.PluginID, consumerRun.PluginRunID, "read", nil); result.OK || result.Code != "plugin-not-running" {
		t.Fatalf("class consumer survived provider removal = %#v", result)
	}
	if err := mounted.reconcile(plugins); err != nil {
		t.Fatal(err)
	}
	if result := engine.DynamicCordisInvoke(t.Context(), consumer.PluginID, consumerRun.PluginRunID, "read", nil); !result.OK || result.Value != float64(7) {
		t.Fatalf("class service did not reactivate = %#v", result)
	}
}

func TestProfileRuntimeSupportsCordisServiceImports(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cordis-service.mjs")
	if err := os.WriteFile(path, []byte(`
import { Service } from '@deepseek-ai/cordis'

export default class ImportedService extends Service {
  static provide = 'profileImportedService'
  static inject = { profileImportedDependency: { mode: 'test' } }
  constructor(ctx, config) {
    super(ctx)
    this.value = config.value
    this.dependency = ctx.profileImportedDependency
  }
  [Service.init]() {
    if (this.value !== 11) throw new Error('imported Service config was not preserved')
  }
  read() { return this.value + this.dependency.value }
}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	providerPath := filepath.Join(dir, "cordis-dependency.mjs")
	if err := os.WriteFile(providerPath, []byte(`
export default function (ctx) { ctx.provide('profileImportedDependency', { value: 2 }) }
`), 0o644); err != nil {
		t.Fatal(err)
	}
	composed := testComposition(t, "- id: cordis-service\n  name: "+path+"\n  config: {value: 11}\n- id: cordis-dependency\n  name: "+providerPath+"\n")
	composed.profileDir = dir
	plugins, err := buildProfileRuntimePlugins(composed)
	if err != nil {
		t.Fatal(err)
	}
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	mounted, err := mountProfileRuntimePluginSet(engine, nil, plugins)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := engine.DynamicCordisDefine(harness.DynamicCordisDefineRequest{
		Plugin:  harness.DynamicCordisPluginSelector{Kind: "new", IDPrefix: "impc"},
		Name:    "imported service consumer",
		Purpose: "verify Cordis Service import compatibility",
		Code: harness.DynamicCordisCode{Host: `
let service
harness.handle('read', () => service.read())
return { inject: ['profileImportedService'], apply(ctx) { service = ctx.profileImportedService } }
`},
		Global: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumerRun, err := engine.DynamicCordisRun(t.Context(), "", consumer.PluginID, consumer.PackageID, "run")
	if err != nil || !consumerRun.OK || len(consumerRun.WaitingFor) != 0 {
		t.Fatalf("imported service consumer run = %#v, %v", consumerRun, err)
	}
	if result := engine.DynamicCordisInvoke(t.Context(), consumer.PluginID, consumerRun.PluginRunID, "read", nil); !result.OK || result.Value != float64(13) {
		t.Fatalf("imported service read = %#v", result)
	}
	if err := mounted.reconcile(nil); err != nil {
		t.Fatal(err)
	}
	if result := engine.DynamicCordisInvoke(t.Context(), consumer.PluginID, consumerRun.PluginRunID, "read", nil); result.OK || result.Code != "plugin-not-running" {
		t.Fatalf("imported service survived unload = %#v", result)
	}
}

func TestProfileEntryInjectAcceptsObjectDependencyMap(t *testing.T) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte("id: mapped\nname: plugin\ninject:\n  alpha: {mode: one}\n  beta: null\n"), &document); err != nil {
		t.Fatal(err)
	}
	names, err := profileEntryInject(document.Content[0])
	if err != nil || strings.Join(names, ",") != "alpha,beta" {
		t.Fatalf("object inject = %#v, %v", names, err)
	}
}

func TestProfileRuntimeCollectsPluginApplyDisposer(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "disposed.txt")
	path := filepath.Join(dir, "disposer.mjs")
	source := fmt.Sprintf(`
import { writeFileSync } from 'node:fs'
export default () => {
  return async () => {
    await Promise.resolve()
    writeFileSync(%q, 'disposed')
  }
}
`, marker)
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	composed := testComposition(t, "- id: disposer\n  name: "+path+"\n")
	composed.profileDir = dir
	plugins, err := buildProfileRuntimePlugins(composed)
	if err != nil {
		t.Fatal(err)
	}
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	mounted, err := mountProfileRuntimePluginSet(engine, nil, plugins)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("disposer marker exists before unload: %v", err)
	}
	if err := mounted.reconcile(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := mounted.flush(&strings.Builder{}, &strings.Builder{}); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(marker)
	if err != nil || string(content) != "disposed" {
		t.Fatalf("plugin disposer marker = %q, %v", content, err)
	}
}

func TestProfileRuntimeReconcileRollsBackFailedUpdate(t *testing.T) {
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	oldBody := `return { apply(ctx) { ctx.provide('profileRollbackWitness', { value: 'old' }) } }`
	mounted, err := mountProfileRuntimePluginSet(engine, nil, []profileRuntimePlugin{{id: "rollback", body: oldBody}})
	if err != nil {
		t.Fatal(err)
	}
	old := mounted.plugins["rollback"]
	if old.packageID == "" {
		t.Fatal("initial profile package was not recorded")
	}
	failedBody := `return { apply() { throw new Error('candidate failed') } }`
	if err := mounted.reconcile([]profileRuntimePlugin{{id: "rollback", body: failedBody}}); err == nil {
		t.Fatal("failed profile update unexpectedly succeeded")
	}
	current := mounted.plugins["rollback"]
	if current.packageID != old.packageID || current.body != old.body {
		t.Fatalf("rollback changed mounted package: got %#v want %#v", current, old)
	}
	view, err := engine.DynamicCordisInspectSelf("", old.pluginID, old.packageID)
	if err != nil {
		t.Fatal(err)
	}
	runtimeView, _ := view["runtime"].(map[string]any)
	host, _ := runtimeView["host"].(map[string]any)
	if host["status"] != "running" {
		t.Fatalf("old profile package status = %#v, want running", host["status"])
	}
}

func TestProfileRuntimeReconcileRollsBackEarlierUpdates(t *testing.T) {
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	firstOldBody := `return { apply(ctx) { ctx.provide('profileRollbackWitnessFirst', { value: 'old' }) } }`
	secondOldBody := `return { apply(ctx) { ctx.provide('profileRollbackWitnessSecond', { value: 'old' }) } }`
	first, err := mountProfileRuntimePluginSet(engine, nil, []profileRuntimePlugin{{id: "first", body: firstOldBody}, {id: "second", body: secondOldBody}})
	if err != nil {
		t.Fatal(err)
	}
	firstOld, secondOld := first.plugins["first"], first.plugins["second"]
	updated := `return { apply(ctx) { ctx.provide('profileRollbackWitnessFirst', { value: 'updated' }) } }`
	failed := `return { apply() { throw new Error('second candidate failed') } }`
	if err := first.reconcile([]profileRuntimePlugin{{id: "first", body: updated}, {id: "second", body: failed}}); err == nil {
		t.Fatal("failed multi-plugin update unexpectedly succeeded")
	}
	for _, want := range []mountedProfilePlugin{firstOld, secondOld} {
		current := first.plugins[want.entryID]
		if current.packageID != want.packageID || current.body != want.body {
			t.Fatalf("plugin %q was not restored: got %#v want %#v", want.entryID, current, want)
		}
		view, inspectErr := engine.DynamicCordisInspectSelf("", want.pluginID, want.packageID)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		runtimeView, _ := view["runtime"].(map[string]any)
		host, _ := runtimeView["host"].(map[string]any)
		if host["status"] != "running" {
			t.Fatalf("plugin %q status = %#v, want running", want.entryID, host["status"])
		}
	}
}

func TestProfileRuntimeMountCleansPartialStartup(t *testing.T) {
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	_, err = mountProfileRuntimePluginSet(engine, nil, []profileRuntimePlugin{
		{id: "first", body: `return { apply(ctx) { ctx.provide('partialStartupWitness', true) } }`},
		{id: "broken", body: `return { apply() { throw new Error('startup failed') } }`},
	})
	if err == nil {
		t.Fatal("partial profile startup unexpectedly succeeded")
	}
	listed, inspectErr := engine.DynamicCordisInspectSelf("", "", "")
	if inspectErr != nil {
		t.Fatal(inspectErr)
	}
	plugins, ok := listed["plugins"].([]map[string]any)
	if !ok || len(plugins) != 0 {
		t.Fatalf("partial profile plugins survived failed mount: %#v", listed)
	}
}

func TestProfileRuntimeReconcileRestoresExternallyRemovedStalePlugin(t *testing.T) {
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	body := `return { apply(ctx) { ctx.provide('staleRollbackWitness', { value: 'old' }) } }`
	mounted, err := mountProfileRuntimePluginSet(engine, nil, []profileRuntimePlugin{{id: "stale", body: body}})
	if err != nil {
		t.Fatal(err)
	}
	old := mounted.plugins["stale"]
	if receipt, undefineErr := engine.DynamicCordisUndefine("", old.pluginID); undefineErr != nil || !receipt.OK {
		t.Fatalf("external undefine = %#v, %v", receipt, undefineErr)
	}
	if err := mounted.reconcile(nil); err == nil {
		t.Fatal("reconcile unexpectedly ignored missing stale plugin")
	}
	restored, ok := mounted.plugins["stale"]
	if !ok || restored.body != body || restored.pluginID == old.pluginID || restored.packageID == old.packageID {
		t.Fatalf("stale plugin was not recreated: got %#v", restored)
	}
	view, inspectErr := engine.DynamicCordisInspectSelf("", restored.pluginID, restored.packageID)
	if inspectErr != nil {
		t.Fatal(inspectErr)
	}
	runtimeView, _ := view["runtime"].(map[string]any)
	host, _ := runtimeView["host"].(map[string]any)
	if host["status"] != "running" {
		t.Fatalf("restored stale plugin status = %#v, want running", host["status"])
	}
}

func TestBuildProfileRuntimePluginsRejectsDuplicateLoaderIDs(t *testing.T) {
	composed := &composition{
		profileDir: t.TempDir(),
		entries:    []*yaml.Node{},
	}
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(`
- id: duplicate
  name: '@example/one'
- id: duplicate
  name: '@example/two'
`), &document); err != nil {
		t.Fatal(err)
	}
	composed.entries = document.Content[0].Content
	if entries := composed.externalPluginEntries(); len(entries) != 2 {
		t.Fatalf("external entries = %#v", entries)
	}
	if _, err := buildProfileRuntimePlugins(composed); err == nil || !strings.Contains(err.Error(), "duplicate loader entry id: duplicate") {
		t.Fatalf("duplicate loader IDs error = %v", err)
	}
}
