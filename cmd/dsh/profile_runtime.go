package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/evanw/esbuild/pkg/api"
	harness "github.com/xxnuo/deepseek-harness-go"
	"gopkg.in/yaml.v3"
)

const profileOutputPollInterval = 20 * time.Millisecond

type customProfileWatch struct {
	paths *profileWatchPaths
	load  func() (*composition, harness.Config, error)
}

type profileRuntimePlugin struct {
	id, body string
	paths    []string
}

type mountedProfilePlugin struct {
	entryID, pluginID, packageID, body string
}

type profileRuntimeMount struct {
	engine       *harness.Engine
	bootstrapID  string
	bootstrapRun string
	cwd          string
	watchPaths   []string
	plugins      map[string]mountedProfilePlugin
	pluginOrder  []string
}

type profileRuntimeResult struct {
	code *int
	err  error
}

type customProfileGeneration struct {
	engine    *harness.Engine
	runtime   *profileRuntimeMount
	closeOnce sync.Once
	closeErr  error
}

func runCustomProfile(args []string, composed *composition, cfg harness.Config, stdout, stderr io.Writer, watch *customProfileWatch) error {
	generation, err := startCustomProfileGeneration(args, composed, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = generation.close() }()

	signals := newProcessSignals()
	defer signals.stop()
	watchContext, stopWatcher := context.WithCancel(signals)
	reloads := make(chan struct{}, 1)
	var watcher sync.WaitGroup
	if watch != nil {
		watch.paths.setRuntime(generation.runtime)
		ready := make(chan struct{})
		watcher.Add(1)
		go func() {
			defer watcher.Done()
			watchProfilePatchesReady(watchContext, watch.paths, func() error {
				select {
				case reloads <- struct{}{}:
				default:
				}
				return nil
			}, io.Discard, ready)
		}()
		<-ready
	}
	defer func() {
		stopWatcher()
		watcher.Wait()
	}()
	if code, err := generation.flush(stdout, stderr); err != nil {
		return err
	} else if code != nil {
		return profileExit(*code)
	}

	ticker := time.NewTicker(profileOutputPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-signals.Done():
			return signals.translate(signals.Err())
		case <-reloads:
			nextComposition, nextConfig, err := watch.load()
			if err != nil {
				fmt.Fprintf(stderr, "dsh: config reload failed: %v\n", err)
				continue
			}
			plugins, err := buildProfileRuntimePlugins(nextComposition)
			previousConfig := generation.engine.Config()
			if err == nil {
				err = generation.engine.ApplyRuntimeConfig(nextConfig)
			}
			if err == nil {
				err = generation.runtime.reconcile(plugins)
			}
			if err != nil {
				_ = generation.engine.ApplyRuntimeConfig(previousConfig)
				fmt.Fprintf(stderr, "dsh: config reload failed: %v\n", err)
				continue
			}
			watch.paths.setRuntime(generation.runtime)
			if code, err := generation.flush(stdout, stderr); err != nil {
				return err
			} else if code != nil {
				return profileExit(*code)
			}
		case <-ticker.C:
			if code, err := generation.flush(stdout, stderr); err != nil {
				return err
			} else if code != nil {
				return profileExit(*code)
			}
		}
	}
}

func profileExit(code int) error {
	if code == 0 {
		return nil
	}
	return exitError{code: code}
}

func startCustomProfileGeneration(args []string, composed *composition, cfg harness.Config) (*customProfileGeneration, error) {
	plugins, err := buildProfileRuntimePlugins(composed)
	if err != nil {
		return nil, err
	}
	if len(plugins) == 0 {
		return nil, errors.New("dsh: custom app surface has no external plugins")
	}
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		return nil, err
	}
	generation := &customProfileGeneration{engine: engine}
	failed := true
	defer func() {
		if failed {
			_ = generation.close()
		}
	}()
	generation.runtime, err = mountProfileRuntimePluginSet(engine, args, plugins)
	if err != nil {
		return nil, err
	}
	failed = false
	return generation, nil
}

func defineAndRunProfilePlugin(engine *harness.Engine, prefix, name, body string) (harness.DynamicCordisRunResponse, error) {
	receipt, err := engine.DynamicCordisDefine(harness.DynamicCordisDefineRequest{
		Plugin:  harness.DynamicCordisPluginSelector{Kind: "new", IDPrefix: prefix},
		Name:    name,
		Purpose: "run an installed profile plugin",
		Code:    harness.DynamicCordisCode{Host: body},
		Global:  true,
	})
	if err != nil {
		return harness.DynamicCordisRunResponse{}, err
	}
	started, err := engine.DynamicCordisRun(context.Background(), "", receipt.PluginID, receipt.PackageID, "run")
	if err != nil {
		_, cleanupErr := engine.DynamicCordisUndefine("", receipt.PluginID)
		return harness.DynamicCordisRunResponse{}, errors.Join(err, cleanupErr)
	}
	if !started.OK {
		startErr := fmt.Errorf("dsh: profile plugin %q failed to start: %s", name, started.Message)
		_, cleanupErr := engine.DynamicCordisUndefine("", receipt.PluginID)
		return harness.DynamicCordisRunResponse{}, errors.Join(startErr, cleanupErr)
	}
	return started, nil
}

func mountProfileRuntimePlugins(engine *harness.Engine, composed *composition, args []string) (*profileRuntimeMount, error) {
	if composed == nil {
		return nil, nil
	}
	plugins, err := buildProfileRuntimePlugins(composed)
	if err != nil {
		return nil, err
	}
	if len(plugins) == 0 {
		return nil, nil
	}
	return mountProfileRuntimePluginSet(engine, args, plugins)
}

func mountProfileRuntimePluginSet(engine *harness.Engine, args []string, plugins []profileRuntimePlugin) (*profileRuntimeMount, error) {
	bootstrap, err := defineAndRunProfilePlugin(engine, "boot", "profile command line", profileBootstrapBody(args))
	if err != nil {
		return nil, err
	}
	cwd, _ := os.Getwd()
	mounted := &profileRuntimeMount{engine: engine, bootstrapID: bootstrap.PluginID, bootstrapRun: bootstrap.PluginRunID, cwd: cwd, plugins: map[string]mountedProfilePlugin{}, pluginOrder: make([]string, 0, len(plugins))}
	running := make([]mountedProfilePlugin, 0, len(plugins))
	for _, plugin := range plugins {
		started, err := defineAndRunProfilePlugin(engine, "prof", plugin.id, plugin.body)
		if err != nil {
			cleanupErrs := []error{err}
			for index := len(running) - 1; index >= 0; index-- {
				if _, cleanupErr := engine.DynamicCordisUndefine("", running[index].pluginID); cleanupErr != nil {
					cleanupErrs = append(cleanupErrs, cleanupErr)
				}
			}
			if _, cleanupErr := engine.DynamicCordisUndefine("", bootstrap.PluginID); cleanupErr != nil {
				cleanupErrs = append(cleanupErrs, cleanupErr)
			}
			return nil, errors.Join(cleanupErrs...)
		}
		mountedPlugin := mountedProfilePlugin{entryID: plugin.id, pluginID: started.PluginID, packageID: started.PackageID, body: plugin.body}
		running = append(running, mountedPlugin)
		mounted.plugins[plugin.id] = mountedPlugin
		mounted.pluginOrder = append(mounted.pluginOrder, plugin.id)
		mounted.watchPaths = append(mounted.watchPaths, plugin.paths...)
	}
	for _, plugin := range running {
		phase := profilePluginFiberPhase(engine, plugin)
		engine.SetPluginInventoryEntryState(plugin.entryID, true, &phase)
	}
	sort.Strings(mounted.watchPaths)
	mounted.watchPaths = compactStrings(mounted.watchPaths)
	return mounted, nil
}

// reconcile replaces only the external profile plugin layer. The bootstrap
// plugin and Engine remain alive, so sessions, subscriptions, and Host-owned
// services retain their identity across a profile patch reload.
func (mounted *profileRuntimeMount) reconcile(plugins []profileRuntimePlugin) error {
	if mounted == nil {
		return nil
	}
	if mounted.plugins == nil {
		mounted.plugins = map[string]mountedProfilePlugin{}
	}
	desired := make(map[string]profileRuntimePlugin, len(plugins))
	for _, plugin := range plugins {
		desired[plugin.id] = plugin
	}
	previousOrder := append([]string(nil), mounted.pluginOrder...)
	previousWatchPaths := append([]string(nil), mounted.watchPaths...)
	created := make([]mountedProfilePlugin, 0)
	changed := make([]mountedProfilePlugin, 0)
	removed := make([]mountedProfilePlugin, 0)
	rollbackCreated := func() error {
		var errs []error
		for index := len(created) - 1; index >= 0; index-- {
			added := created[index]
			receipt, err := mounted.engine.DynamicCordisUndefine("", added.pluginID)
			if err != nil {
				errs = append(errs, fmt.Errorf("rollback created profile plugin %q: %w", added.entryID, err))
			} else if !receipt.OK && receipt.Message != "" {
				errs = append(errs, fmt.Errorf("rollback created profile plugin %q failed: %s", added.entryID, receipt.Message))
			}
			delete(mounted.plugins, added.entryID)
		}
		return errors.Join(errs...)
	}
	rollbackChanged := func() error {
		var errs []error
		for index := len(changed) - 1; index >= 0; index-- {
			previous := changed[index]
			// DynamicCordisRun disposes the previous fiber before loading the
			// candidate package. Re-activate the known-good package when the
			// candidate fails, matching Loader's rollback contract.
			started, err := mounted.engine.DynamicCordisRun(context.Background(), "", previous.pluginID, previous.packageID, "update")
			if err != nil {
				errs = append(errs, fmt.Errorf("rollback changed profile plugin %q: %w", previous.entryID, err))
			}
			if !started.OK {
				// A failed candidate may never have committed, in which case the
				// previous package is still current and needs a normal restart.
				started, err = mounted.engine.DynamicCordisRun(context.Background(), "", previous.pluginID, previous.packageID, "run")
				if err != nil {
					errs = append(errs, fmt.Errorf("rollback changed profile plugin %q: %w", previous.entryID, err))
				}
			}
			if started.OK {
				mounted.plugins[previous.entryID] = previous
			} else if started.Message != "" {
				errs = append(errs, fmt.Errorf("rollback changed profile plugin %q failed: %s", previous.entryID, started.Message))
			}
		}
		return errors.Join(errs...)
	}
	rollbackRemoved := func() error {
		var errs []error
		for index := len(removed) - 1; index >= 0; index-- {
			previous := removed[index]
			started, err := defineAndRunProfilePlugin(mounted.engine, "prof", previous.entryID, previous.body)
			if err != nil {
				errs = append(errs, fmt.Errorf("rollback removed profile plugin %q: %w", previous.entryID, err))
				continue
			}
			restored := previous
			restored.pluginID, restored.packageID = started.PluginID, started.PackageID
			mounted.plugins[previous.entryID] = restored
			phase := profilePluginFiberPhase(mounted.engine, restored)
			mounted.engine.SetPluginInventoryEntryState(previous.entryID, true, &phase)
		}
		return errors.Join(errs...)
	}
	rollback := func(cause error) error {
		errs := []error{cause}
		if err := rollbackCreated(); err != nil {
			errs = append(errs, err)
		}
		if err := rollbackChanged(); err != nil {
			errs = append(errs, err)
		}
		if err := rollbackRemoved(); err != nil {
			errs = append(errs, err)
		}
		mounted.pluginOrder = previousOrder
		mounted.watchPaths = previousWatchPaths
		return errors.Join(errs...)
	}
	for _, plugin := range plugins {
		current, exists := mounted.plugins[plugin.id]
		if exists && current.body == plugin.body {
			continue
		}
		if !exists {
			started, err := defineAndRunProfilePlugin(mounted.engine, "prof", plugin.id, plugin.body)
			if err != nil {
				return rollback(err)
			}
			current = mountedProfilePlugin{entryID: plugin.id, pluginID: started.PluginID, packageID: started.PackageID}
			created = append(created, current)
		} else {
			changed = append(changed, current)
			defined, err := mounted.engine.DynamicCordisDefine(harness.DynamicCordisDefineRequest{
				Plugin:  harness.DynamicCordisPluginSelector{Kind: "existing", PluginID: current.pluginID, IDPrefix: "prof"},
				Name:    plugin.id,
				Purpose: "update an installed profile plugin",
				Code:    harness.DynamicCordisCode{Host: plugin.body},
				Global:  true,
			})
			if err != nil {
				return rollback(err)
			}
			started, err := mounted.engine.DynamicCordisRun(context.Background(), "", current.pluginID, defined.PackageID, "update")
			if err != nil {
				return rollback(err)
			}
			if !started.OK {
				return rollback(fmt.Errorf("dsh: profile plugin %q failed to update: %s", plugin.id, started.Message))
			}
			current.packageID = started.PackageID
		}
		current.entryID, current.body = plugin.id, plugin.body
		mounted.plugins[plugin.id] = current
		phase := profilePluginFiberPhase(mounted.engine, current)
		mounted.engine.SetPluginInventoryEntryState(plugin.id, true, &phase)
	}
	// Loader removes stale entries in their previous tree order, not map order.
	for _, id := range append([]string(nil), mounted.pluginOrder...) {
		current, exists := mounted.plugins[id]
		if !exists {
			continue
		}
		if _, ok := desired[id]; ok {
			continue
		}
		removed = append(removed, current)
		receipt, err := mounted.engine.DynamicCordisUndefine("", current.pluginID)
		if err != nil {
			return rollback(err)
		}
		if !receipt.OK {
			message := receipt.Message
			if message == "" {
				message = receipt.Reason
			}
			return rollback(fmt.Errorf("dsh: profile plugin %q could not be removed: %s", id, message))
		}
		delete(mounted.plugins, id)
	}
	mounted.pluginOrder = mounted.pluginOrder[:0]
	for _, plugin := range plugins {
		mounted.pluginOrder = append(mounted.pluginOrder, plugin.id)
	}
	mounted.watchPaths = mounted.watchPaths[:0]
	for _, plugin := range plugins {
		mounted.watchPaths = append(mounted.watchPaths, plugin.paths...)
	}
	sort.Strings(mounted.watchPaths)
	mounted.watchPaths = compactStrings(mounted.watchPaths)
	return nil
}

func profilePluginFiberPhase(engine *harness.Engine, plugin mountedProfilePlugin) string {
	view, err := engine.DynamicCordisInspectSelf("", plugin.pluginID, plugin.packageID)
	if err != nil {
		return "failed"
	}
	runtimeView, _ := view["runtime"].(map[string]any)
	host, _ := runtimeView["host"].(map[string]any)
	status, _ := host["status"].(string)
	switch status {
	case "waiting":
		return "pending"
	case "failed":
		return "failed"
	default:
		return "active"
	}
}

func (generation *customProfileGeneration) flush(stdout, stderr io.Writer) (*int, error) {
	return generation.runtime.flush(stdout, stderr)
}

func (mounted *profileRuntimeMount) flush(stdout, stderr io.Writer) (*int, error) {
	return mounted.flushContext(context.Background(), stdout, stderr)
}

func (mounted *profileRuntimeMount) flushContext(ctx context.Context, stdout, stderr io.Writer) (*int, error) {
	result := mounted.engine.DynamicCordisInvoke(ctx, mounted.bootstrapID, mounted.bootstrapRun, "drain", nil)
	if !result.OK {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("dsh: profile output bridge failed: %s", result.Message)
	}
	state, ok := result.Value.(map[string]any)
	if !ok {
		return nil, errors.New("dsh: profile output bridge returned an invalid state")
	}
	if events, ok := state["events"].([]any); ok {
		for _, raw := range events {
			event, _ := raw.(map[string]any)
			text, _ := event["text"].(string)
			switch event["stream"] {
			case "file":
				path, _ := event["path"].(string)
				if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
					return nil, fmt.Errorf("dsh: profile plugin write %s: %w", path, err)
				}
			case "stderr":
				_, _ = io.WriteString(stderr, text)
			default:
				_, _ = io.WriteString(stdout, text)
			}
		}
	}
	if rawPaths, ok := state["exists"].([]any); ok && len(rawPaths) > 0 {
		resolved := make(map[string]bool, len(rawPaths))
		for _, raw := range rawPaths {
			path, ok := raw.(string)
			if ok {
				resolved[path] = profilePathExists(mounted.cwd, path)
			}
		}
		if len(resolved) > 0 {
			result = mounted.engine.DynamicCordisInvoke(ctx, mounted.bootstrapID, mounted.bootstrapRun, "resolveExists", resolved)
			if !result.OK {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return nil, fmt.Errorf("dsh: profile filesystem bridge failed: %s", result.Message)
			}
		}
	}
	if value := state["exitCode"]; value != nil {
		code := int(value.(float64))
		return &code, nil
	}
	return nil, nil
}

func (mounted *profileRuntimeMount) pump(ctx context.Context, stdout, stderr io.Writer) (profileRuntimeResult, bool) {
	ticker := time.NewTicker(profileOutputPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			code, err := mounted.flushContext(context.Background(), stdout, stderr)
			if err != nil {
				return profileRuntimeResult{err: err}, true
			}
			if code != nil {
				return profileRuntimeResult{code: code}, true
			}
			return profileRuntimeResult{}, false
		case <-ticker.C:
		}
		code, err := mounted.flushContext(context.Background(), stdout, stderr)
		if err != nil {
			return profileRuntimeResult{err: err}, true
		}
		if code != nil {
			return profileRuntimeResult{code: code}, true
		}
	}
}

func profilePathExists(cwd, raw string) bool {
	path := raw
	if strings.HasPrefix(strings.ToLower(raw), "file:") {
		parsed, err := url.Parse(raw)
		if err != nil || !strings.EqualFold(parsed.Scheme, "file") || parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") {
			return false
		}
		path, err = url.PathUnescape(parsed.Path)
		if err != nil {
			return false
		}
		if runtime.GOOS == "windows" && len(path) >= 3 && path[0] == '/' && path[2] == ':' {
			path = path[1:]
		}
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(cwd, filepath.FromSlash(path))
	}
	_, err := os.Stat(path)
	return err == nil
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

func (generation *customProfileGeneration) close() error {
	generation.closeOnce.Do(func() { generation.closeErr = generation.engine.Close() })
	return generation.closeErr
}

func profileBootstrapBody(args []string) string {
	if args == nil {
		args = []string{}
	}
	encoded, _ := json.Marshal(args)
	return fmt.Sprintf(`
const args = %s
let exitCode = null
let events = []
const knownFiles = Object.create(null)
let pendingExists = Object.create(null)
harness.handle('drain', () => {
  const state = { exitCode, events, exists: Object.keys(pendingExists) }
  events = []
  pendingExists = Object.create(null)
  return state
})
harness.handle('resolveExists', values => {
  for (const [path, exists] of Object.entries(values ?? {})) knownFiles[path] = exists === true
  return null
})
return {
  apply(ctx) {
    const snapshot = Object.freeze([...args])
    ctx.provide('cmdlineArgs', { get: () => snapshot })
    ctx.provide('appExit', code => {
      if (exitCode !== null) return
      const value = Number(code)
      exitCode = Number.isFinite(value) ? Math.trunc(value) : 1
    })
    ctx.provide('appIO', {
      writeOut: text => events.push({ stream: 'stdout', text: String(text) }),
      writeErr: text => events.push({ stream: 'stderr', text: String(text) }),
      writeFile: (path, text) => {
        knownFiles[String(path)] = true
        events.push({ stream: 'file', path: String(path), text: String(text) })
      },
      existsSync: path => {
        path = String(path)
        pendingExists[path] = true
        return knownFiles[path] === true
      },
    })
  },
}`, encoded)
}

func buildProfileRuntimePlugins(composed *composition) ([]profileRuntimePlugin, error) {
	entries := composed.externalPluginEntries()
	plugins := make([]profileRuntimePlugin, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if _, exists := seen[entry.id]; exists {
			return nil, fmt.Errorf("dsh: duplicate loader entry id: %s", entry.id)
		}
		seen[entry.id] = struct{}{}
	}
	for _, entry := range entries {
		path, err := resolveProfilePluginPath(composed.profileDir, entry.name)
		if err != nil {
			return nil, err
		}
		body, paths, err := buildProfilePluginBody(path, entry.node)
		if err != nil {
			return nil, fmt.Errorf("dsh: build profile plugin %q (id %q): %w", entry.name, entry.id, err)
		}
		plugins = append(plugins, profileRuntimePlugin{id: entry.id, body: body, paths: paths})
	}
	return plugins, nil
}

func buildProfilePluginBody(path string, entry *yaml.Node) (string, []string, error) {
	workingDir := filepath.Dir(path)
	result := api.Build(api.BuildOptions{
		EntryPoints:   []string{path},
		AbsWorkingDir: workingDir,
		Bundle:        true,
		Metafile:      true,
		Write:         false,
		Format:        api.FormatIIFE,
		GlobalName:    "__dshProfileModule",
		Platform:      api.PlatformNeutral,
		Target:        api.ES2017,
		LegalComments: api.LegalCommentsNone,
		LogLevel:      api.LogLevelSilent,
		Plugins:       profileRuntimeBuildPlugins(),
	})
	if len(result.Errors) > 0 {
		messages := api.FormatMessages(result.Errors, api.FormatMessagesOptions{Kind: api.ErrorMessage, Color: false})
		return "", nil, errors.New(strings.TrimSpace(strings.Join(messages, "\n")))
	}
	if len(result.OutputFiles) != 1 {
		return "", nil, errors.New("esbuild returned no runnable JavaScript")
	}
	var metadata struct {
		Inputs map[string]json.RawMessage `json:"inputs"`
	}
	if err := json.Unmarshal([]byte(result.Metafile), &metadata); err != nil {
		return "", nil, fmt.Errorf("decode esbuild metafile: %w", err)
	}
	paths := make([]string, 0, len(metadata.Inputs))
	for input := range metadata.Inputs {
		if strings.HasPrefix(input, "dsh-profile-runtime:") {
			continue
		}
		input = filepath.FromSlash(input)
		if !filepath.IsAbs(input) {
			input = filepath.Join(workingDir, input)
		}
		absolute, err := filepath.Abs(input)
		if err != nil {
			return "", nil, err
		}
		paths = append(paths, filepath.Clean(absolute))
	}
	sort.Strings(paths)
	paths = compactStrings(paths)
	config, err := profileNodeJS(mappingValue(entry, "config"))
	if err != nil {
		return "", nil, err
	}
	inject, err := profileEntryInject(entry)
	if err != nil {
		return "", nil, err
	}
	if inject == nil {
		inject = []string{}
	}
	rowInject, _ := json.Marshal(inject)
	return fmt.Sprintf(`
%s
%s
const profileModule = __dshProfileModule
let plugin = profileModule
if (plugin !== null && plugin !== undefined) {
  plugin = plugin.default ?? plugin
  if (plugin.__esModule) plugin = plugin.default ?? plugin
}
const rowInject = %s
const pluginInject = plugin?.inject
const apply = typeof plugin === 'function' ? plugin : plugin?.apply
function resolveInjectNames(value) {
  if (Array.isArray(value)) return value
  if (value && typeof value === 'object') {
    const names = []
    for (const name in value) names.push(name)
    return names
  }
  return []
}
function resolvePluginConfig(config) {
  if (!plugin?.Config) return config
  const result = plugin.Config['~standard'].validate(config)
  if (result && typeof result.then === 'function') {
    throw new TypeError('Async config validation is not supported')
  }
  if (result?.issues) {
    const lines = result.issues.map(issue => issue.path
      ? '  - ' + issue.message + ' (at ' + issue.path.join('.') + ')'
      : '  - ' + issue.message)
    throw new TypeError('invalid config:\n' + lines.join('\n'))
  }
  return result?.value
}
const GeneratorFunction = function* () {}.constructor
const AsyncGeneratorFunction = async function* () {}.constructor
function isPluginConstructor(callback) {
  if (!callback.prototype) return false
  if (callback instanceof GeneratorFunction) return false
  if (AsyncGeneratorFunction !== Function && callback instanceof AsyncGeneratorFunction) return false
  return true
}
function executePlugin(ctx, config) {
  if (!isPluginConstructor(apply)) return apply(ctx, config)
  const reflectedContext = Object.create(ctx)
  Object.defineProperty(reflectedContext, 'reflect', {
    value: Object.freeze({ provide(name, value) { return ctx.provide(name, value) } }),
  })
  const instance = new apply(reflectedContext, config)
  for (const hook of instance?.[Symbol.for('cordis.initHooks')] ?? []) hook()
  return instance?.[Symbol.for('cordis.init')]?.()
}
return {
  inject: [...new Set([...resolveInjectNames(pluginInject), ...rowInject])],
  apply(ctx) {
    __dshProfileContext = ctx
    if (typeof apply !== 'function') throw new Error('profile plugin must export apply(ctx, config) or a default function')
    return ctx.effect(() => executePlugin(ctx, resolvePluginConfig(%s)))
  },
}`, profileProcessPrelude(), result.OutputFiles[0].Contents, rowInject, config), paths, nil
}

func profileProcessPrelude() string {
	environment := map[string]string{}
	for _, entry := range os.Environ() {
		if key, value, ok := strings.Cut(entry, "="); ok {
			environment[key] = value
		}
	}
	cwd, _ := os.Getwd()
	platform := runtime.GOOS
	if platform == "windows" {
		platform = "win32"
	}
	envJSON, _ := json.Marshal(environment)
	cwdJSON, _ := json.Marshal(cwd)
	platformJSON, _ := json.Marshal(platform)
	return fmt.Sprintf(`
let __dshProfileContext
const process = {
  env: %s,
  platform: %s,
  pid: 0,
  cwd: () => %s,
  stdout: { write: text => __dshProfileContext.get('appIO').writeOut(String(text)) },
  stderr: { write: text => __dshProfileContext.get('appIO').writeErr(String(text)) },
  emit: signal => {
    if (signal === 'SIGINT') __dshProfileContext.get('appExit')(130)
    else if (signal === 'SIGTERM') __dshProfileContext.get('appExit')(0)
  },
  kill: (_pid, signal) => {
    if (signal === 'SIGINT') __dshProfileContext.get('appExit')(130)
    else if (signal === 'SIGTERM') __dshProfileContext.get('appExit')(0)
  },
}
const setTimeout = (fn, delay) => __dshProfileContext.setTimeout(fn, delay)
const clearTimeout = handle => { if (typeof handle === 'function') handle() }
const setInterval = (fn, delay) => __dshProfileContext.setInterval(fn, delay)
const clearInterval = handle => { if (typeof handle === 'function') handle() }
`, envJSON, platformJSON, cwdJSON)
}

func profileEntryInject(entry *yaml.Node) ([]string, error) {
	node := mappingValue(entry, "inject")
	if node == nil {
		return nil, nil
	}
	value, err := profileConfigValue(node)
	if err != nil {
		return nil, err
	}
	switch value := value.(type) {
	case string:
		return []string{value}, nil
	case []any:
		result := make([]string, 0, len(value))
		for _, raw := range value {
			name, ok := raw.(string)
			if !ok || strings.TrimSpace(name) == "" {
				return nil, errors.New("profile plugin inject must contain service names")
			}
			result = append(result, name)
		}
		return result, nil
	case map[string]any:
		result := make([]string, 0, len(value))
		for index := 0; index < len(node.Content); index += 2 {
			name := strings.TrimSpace(node.Content[index].Value)
			if name == "" {
				return nil, errors.New("profile plugin inject must contain service names")
			}
			result = append(result, name)
		}
		return result, nil
	default:
		return nil, errors.New("profile plugin inject must be a service name, array, or object")
	}
}

func profileNodeJS(node *yaml.Node) (string, error) {
	if node == nil {
		return "{}", nil
	}
	if isJSExpr(node) {
		return "(" + node.Value + ")", nil
	}
	switch node.Kind {
	case yaml.MappingNode:
		parts := make([]string, 0, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			key, _ := json.Marshal(node.Content[index].Value)
			value, err := profileNodeJS(node.Content[index+1])
			if err != nil {
				return "", err
			}
			parts = append(parts, string(key)+":"+value)
		}
		return "{" + strings.Join(parts, ",") + "}", nil
	case yaml.SequenceNode:
		parts := make([]string, len(node.Content))
		for index, child := range node.Content {
			value, err := profileNodeJS(child)
			if err != nil {
				return "", err
			}
			parts[index] = value
		}
		return "[" + strings.Join(parts, ",") + "]", nil
	default:
		var value any
		if err := node.Decode(&value); err != nil {
			return "", err
		}
		encoded, err := json.Marshal(value)
		return string(encoded), err
	}
}

func resolveProfilePluginPath(profileDir, name string) (string, error) {
	if strings.HasPrefix(name, "file://") {
		parsed, err := url.Parse(name)
		if err != nil || parsed.Host != "" && parsed.Host != "localhost" {
			return "", fmt.Errorf("invalid file URL %q", name)
		}
		path, err := url.PathUnescape(parsed.Path)
		if err != nil {
			return "", err
		}
		if runtime.GOOS == "windows" && len(path) >= 3 && path[0] == '/' && path[2] == ':' {
			path = path[1:]
		}
		return resolveProfileJSFile(filepath.FromSlash(path))
	}
	if filepath.IsAbs(name) {
		return resolveProfileJSFile(name)
	}
	if strings.HasPrefix(name, "./") || strings.HasPrefix(name, "../") {
		return resolveProfileJSFile(filepath.Join(profileDir, filepath.FromSlash(name)))
	}
	packageName, subpath := splitProfilePackageName(name)
	if packageName == "" {
		return "", fmt.Errorf("cannot resolve plugin module %q", name)
	}
	root := filepath.Join(profileDir, "node_modules", filepath.FromSlash(packageName))
	manifest, err := os.ReadFile(filepath.Join(root, "package.json"))
	if err != nil {
		return "", fmt.Errorf("cannot resolve plugin module %q: %w", name, err)
	}
	document, err := decodeProfileJSON(manifest)
	if err != nil {
		return "", fmt.Errorf("invalid package.json for %q: %w", name, err)
	}
	object, ok := document.(profileJSONObject)
	if !ok {
		return "", fmt.Errorf("invalid package.json for %q: root must be an object", name)
	}
	exportKey := "."
	if subpath != "" {
		exportKey = "./" + filepath.ToSlash(subpath)
	}
	if exports, exists := object.get("exports"); exists {
		target, matched, err := profileManifestExportTarget(exports, exportKey)
		if err != nil {
			return "", fmt.Errorf("invalid package exports for %q: %w", name, err)
		}
		if !matched || target == "" {
			return "", fmt.Errorf("package subpath %q is not exported by %q", exportKey, packageName)
		}
		path, err := profileExportPath(root, target)
		if err != nil {
			return "", fmt.Errorf("invalid package exports for %q: %w", name, err)
		}
		return resolveProfileJSFile(path)
	}
	if subpath != "" {
		return resolveProfileJSFile(filepath.Join(root, filepath.FromSlash(subpath)))
	}
	target, _ := object.string("module")
	if target == "" {
		target, _ = object.string("main")
	}
	if target == "" {
		target = "index.js"
	}
	return resolveProfileJSFile(filepath.Join(root, filepath.FromSlash(target)))
}

func splitProfilePackageName(name string) (string, string) {
	parts := strings.Split(name, "/")
	if strings.HasPrefix(name, "@") {
		if len(parts) < 2 {
			return "", ""
		}
		return strings.Join(parts[:2], "/"), strings.Join(parts[2:], "/")
	}
	if len(parts) == 0 || parts[0] == "" {
		return "", ""
	}
	return parts[0], strings.Join(parts[1:], "/")
}

type profileJSONPair struct {
	key   string
	value any
}

type profileJSONObject []profileJSONPair
type profileJSONArray []any

func (object profileJSONObject) get(key string) (any, bool) {
	for _, pair := range object {
		if pair.key == key {
			return pair.value, true
		}
	}
	return nil, false
}

func (object profileJSONObject) string(key string) (string, bool) {
	value, ok := object.get(key)
	if !ok {
		return "", false
	}
	result, ok := value.(string)
	return result, ok
}

func decodeProfileJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	value, err := decodeProfileJSONValue(decoder)
	if err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, errors.New("multiple JSON values")
	}
	return value, nil
}

func decodeProfileJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := profileJSONObject{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			value, err := decodeProfileJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			object = append(object, profileJSONPair{key: key.(string), value: value})
		}
		_, err = decoder.Token()
		return object, err
	case '[':
		array := profileJSONArray{}
		for decoder.More() {
			value, err := decodeProfileJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		_, err = decoder.Token()
		return array, err
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func profileManifestExportTarget(value any, key string) (string, bool, error) {
	object, isObject := value.(profileJSONObject)
	if !isObject {
		if key != "." {
			return "", false, nil
		}
		target, matched, err := profileManifestConditionTarget(value, "")
		return target, matched, err
	}
	hasSubpaths := false
	hasConditions := false
	for _, pair := range object {
		if strings.HasPrefix(pair.key, ".") {
			hasSubpaths = true
		} else {
			hasConditions = true
		}
	}
	if hasSubpaths && hasConditions {
		return "", false, errors.New("exports cannot mix subpath and condition keys")
	}
	if !hasSubpaths {
		if key != "." {
			return "", false, nil
		}
		return profileManifestConditionTarget(object, "")
	}
	if value, ok := object.get(key); ok {
		return profileManifestConditionTarget(value, "")
	}
	pattern, replacement := profileManifestPattern(object, key)
	if pattern == "" {
		return "", false, nil
	}
	value, _ = object.get(pattern)
	return profileManifestConditionTarget(value, replacement)
}

func profileManifestPattern(object profileJSONObject, key string) (string, string) {
	best, replacement := "", ""
	for _, pair := range object {
		star := strings.IndexByte(pair.key, '*')
		if star < 0 || strings.IndexByte(pair.key[star+1:], '*') >= 0 {
			continue
		}
		prefix, suffix := pair.key[:star], pair.key[star+1:]
		if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) || len(key) < len(prefix)+len(suffix) {
			continue
		}
		if best != "" {
			bestStar := strings.IndexByte(best, '*')
			bestPrefix, bestSuffix := best[:bestStar], best[bestStar+1:]
			if len(prefix) < len(bestPrefix) || len(prefix) == len(bestPrefix) && len(suffix) <= len(bestSuffix) {
				continue
			}
		}
		best = pair.key
		replacement = key[len(prefix) : len(key)-len(suffix)]
	}
	return best, replacement
}

func profileManifestConditionTarget(value any, replacement string) (string, bool, error) {
	switch value := value.(type) {
	case nil:
		return "", true, nil
	case string:
		return strings.ReplaceAll(value, "*", replacement), true, nil
	case profileJSONArray:
		for _, candidate := range value {
			target, matched, err := profileManifestConditionTarget(candidate, replacement)
			if err != nil {
				continue
			}
			if matched && target != "" {
				return target, true, nil
			}
		}
		return "", true, nil
	case profileJSONObject:
		for _, pair := range value {
			switch pair.key {
			case "node-addons", "node", "import", "default":
				target, matched, err := profileManifestConditionTarget(pair.value, replacement)
				if err != nil || matched {
					return target, matched, err
				}
			}
		}
		return "", false, nil
	default:
		return "", false, fmt.Errorf("unsupported exports target %T", value)
	}
}

func profileExportPath(root, target string) (string, error) {
	if !strings.HasPrefix(target, "./") {
		return "", fmt.Errorf("target %q must start with ./", target)
	}
	path := filepath.Join(root, filepath.FromSlash(target))
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("target %q escapes package root", target)
	}
	return path, nil
}

func resolveProfileJSFile(path string) (string, error) {
	candidates := []string{path}
	if filepath.Ext(path) == "" {
		candidates = append(candidates, path+".mjs", path+".js", path+".cjs")
	}
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() {
			return filepath.Abs(candidate)
		}
	}
	return "", fmt.Errorf("cannot resolve plugin module at %s", path)
}

func profileRuntimeBuildPlugins() []api.Plugin {
	modules := map[string]string{
		"commander":                profileCommanderModule,
		"@deepseek-ai/cordis":      profileCordisModule,
		"@deepseek-ai/dsh-cmdline": profileCmdlineModule,
		"node:fs":                  profileFSModule,
		"node:path":                profilePathModule,
	}
	return []api.Plugin{{
		Name: "dsh-profile-runtime",
		Setup: func(build api.PluginBuild) {
			build.OnResolve(api.OnResolveOptions{Filter: `^(?:commander|@deepseek-ai/cordis|@deepseek-ai/dsh-cmdline|node:fs|node:path)$`}, func(args api.OnResolveArgs) (api.OnResolveResult, error) {
				return api.OnResolveResult{Path: args.Path, Namespace: "dsh-profile-runtime"}, nil
			})
			build.OnLoad(api.OnLoadOptions{Filter: `.*`, Namespace: "dsh-profile-runtime"}, func(args api.OnLoadArgs) (api.OnLoadResult, error) {
				source := modules[args.Path]
				return api.OnLoadResult{Contents: &source, Loader: api.LoaderJS}, nil
			})
		},
	}}
}

const profileCordisModule = `
export const symbols = Object.freeze({
  effect: Symbol.for('cordis.effect'),
  filter: Symbol.for('cordis.filter'),
  isolate: Symbol.for('cordis.isolate'),
  intercept: Symbol.for('cordis.intercept'),
  initHooks: Symbol.for('cordis.initHooks'),
  init: Symbol.for('cordis.init'),
  check: Symbol.for('cordis.check'),
  config: Symbol.for('cordis.config'),
  invoke: Symbol.for('cordis.invoke'),
  extend: Symbol.for('cordis.extend'),
  tracker: Symbol.for('cordis.tracker'),
  resolveConfig: Symbol.for('cordis.resolveConfig'),
})

export class Context {
  static effect = symbols.effect
  static filter = symbols.filter
  static isolate = symbols.isolate
  static intercept = symbols.intercept
}

export class Service {
  static init = symbols.init
  static check = symbols.check
  static config = symbols.config
  static invoke = symbols.invoke
  static extend = symbols.extend
  static tracker = symbols.tracker
  static resolveConfig = symbols.resolveConfig

  constructor(ctx, name) {
    name ??= this.constructor.provide
    if (typeof name !== 'string' || !name) throw new TypeError('Service requires a non-empty service name')
    this.ctx = ctx
    this.name = name
    ctx.reflect.provide(name, this, this[Service.check])
  }
}

export function Inject(name, config) {
  return function (value, decorator) {
    if (decorator?.kind === 'class') {
      if (!Object.hasOwn(value, 'inject')) value.inject = { ...(value.inject ?? {}) }
      value.inject[name] = config
      return
    }
    if (decorator?.kind === 'method') {
      decorator.addInitializer(function () {
        ;(this[symbols.initHooks] ??= []).push(() => value.call(this))
      })
      return
    }
    throw new Error('@Inject() can only be used on class or class methods')
  }
}

export function isConstructor(callback) {
  if (!callback?.prototype) return false
  const GeneratorFunction = function* () {}.constructor
  const AsyncGeneratorFunction = async function* () {}.constructor
  if (callback instanceof GeneratorFunction) return false
  if (AsyncGeneratorFunction !== Function && callback instanceof AsyncGeneratorFunction) return false
  return true
}
`

const profileFSModule = `
export function writeFileSync(path, data) {
  __dshProfileContext.get('appIO').writeFile(String(path), String(data))
}
export function existsSync(path) {
  return __dshProfileContext.get('appIO').existsSync(String(path))
}
`

const profilePathModule = `
export function join(...parts) {
  const absolute = String(parts[0] ?? '').startsWith('/')
  const values = []
  for (const part of parts) {
    for (const value of String(part).split(/[\\/]+/)) {
      if (!value || value === '.') continue
      if (value === '..') values.pop()
      else values.push(value)
    }
  }
  return (absolute ? '/' : '') + values.join('/')
}
`

const profileCmdlineModule = `
function hasAction(command) {
  if (typeof command._actionHandler === 'function') return true
  return command.commands.some(hasAction)
}
export function parseCmdline(ctx, program) {
  const args = ctx.get('cmdlineArgs')
  const exit = ctx.get('appExit')
  const io = ctx.get('appIO')
  if (args === undefined || exit === undefined || io === undefined) {
    throw new Error(program.name() + ': the launcher must provide ctx.cmdlineArgs and ctx.appExit before the tree mounts')
  }
  if (!hasAction(program)) throw new Error(program.name() + ': no command in the program declares an action')
  program.exitOverride().configureOutput({ writeOut: io.writeOut, writeErr: io.writeErr })
  try {
    program.parse(args.get(), { from: 'user' })
  } catch (error) {
    if (!error || typeof error.code !== 'string' || !error.code.startsWith('commander.') || typeof error.exitCode !== 'number') throw error
    exit(error.exitCode)
  }
}
`

const profileCommanderModule = `
class CommanderError extends Error {
  constructor(exitCode, code, message) { super(message); this.exitCode = exitCode; this.code = code }
}
function optionKey(flags) {
  const match = flags.match(/--(?:no-)?([a-zA-Z0-9-]+)/)
  return match ? match[1].replace(/-([a-z])/g, (_, c) => c.toUpperCase()) : ''
}
export class Command {
  constructor(name = '') {
    this._name = name
    this._description = ''
    this._options = []
    this._arguments = []
    this._values = {}
    this._actionHandler = undefined
    this._output = { writeOut() {}, writeErr() {} }
    this.commands = []
  }
  name(value) { if (value === undefined) return this._name; this._name = value; return this }
  description(value) { this._description = value; return this }
  option(flags, description, parser, defaultValue) { return this._addOption(flags, description, false, parser, defaultValue) }
  requiredOption(flags, description, parser, defaultValue) { return this._addOption(flags, description, true, parser, defaultValue) }
  _addOption(flags, description, required, parser, defaultValue) {
    if (typeof parser !== 'function') { defaultValue = parser === undefined ? defaultValue : parser; parser = undefined }
    const key = optionKey(flags)
    const long = (flags.match(/--[a-zA-Z0-9-]+/) || [])[0]
    const short = (flags.match(/(?:^|[, ]+)(-[a-zA-Z])(?:[, ]|$)/) || [])[1]
    const valueRequired = /<[^>]+>/.test(flags)
    const valueOptional = /\[[^\]]+\]/.test(flags)
    const negated = /--no-/.test(flags)
    this._options.push({ flags, description, required, parser, key, long, short, valueRequired, valueOptional, negated })
    if (defaultValue !== undefined) this._values[key] = defaultValue
    else if (!valueRequired && !valueOptional) this._values[key] = negated
    return this
  }
  argument(spec, description) { this._arguments.push({ spec, description }); return this }
  action(handler) { this._actionHandler = handler; return this }
  command(spec) { const child = new Command(spec.split(/[ <\[]/, 1)[0]); this.commands.push(child); return child }
  addCommand(command) { this.commands.push(command); return this }
  opts() { return { ...this._values } }
  exitOverride() { return this }
  configureOutput(output) { this._output = { ...this._output, ...output }; for (const child of this.commands) child.configureOutput(output); return this }
  helpOption() { return this }
  showHelpAfterError() { return this }
  allowUnknownOption() { return this }
  version(value, flags = '-V, --version') { this._version = value; this._versionFlags = flags; return this }
  error(message) { this._output.writeErr('error: ' + message + '\n'); throw new CommanderError(1, 'commander.error', message) }
  _help() {
    let text = 'Usage: ' + (this._name || 'command')
    if (this._options.length || this._version !== undefined) text += ' [options]'
    for (const argument of this._arguments) text += ' ' + argument.spec
    if (this._description) text += '\n\n' + this._description
    const options = [...this._options]
    if (this._version !== undefined) options.push({ flags: this._versionFlags, description: 'output the version number' })
    options.push({ flags: '-h, --help', description: 'display help for command' })
    if (options.length) {
      text += '\n\nOptions:\n'
      for (const option of options) text += '  ' + option.flags + (option.description ? '\t' + option.description : '') + '\n'
    } else text += '\n'
    return text
  }
  parse(args) {
    const positionals = []
    for (let index = 0; index < args.length; index++) {
      const argument = args[index]
      if (argument === '-h' || argument === '--help') {
        this._output.writeOut(this._help())
        throw new CommanderError(0, 'commander.helpDisplayed', '')
      }
      if (this._version !== undefined && this._versionFlags.split(/[, ]+/).includes(argument)) {
        this._output.writeOut(String(this._version) + '\n')
        throw new CommanderError(0, 'commander.version', '')
      }
      if (argument === '--') { positionals.push(...args.slice(index + 1)); break }
      if (argument.startsWith('-')) {
        const [flag, attached] = argument.split('=', 2)
        const option = this._options.find(value => value.long === flag || value.short === flag)
        if (!option) this.error('unknown option ' + argument)
        let value = option.negated ? false : true
        if (option.valueRequired || option.valueOptional) {
          if (attached !== undefined) value = attached
          else if (index + 1 < args.length && (option.valueRequired || !args[index + 1].startsWith('-'))) value = args[++index]
          else if (option.valueRequired) this.error('option ' + flag + ' argument missing')
          else value = undefined
        }
        if (option.parser && value !== undefined) value = option.parser(value, this._values[option.key])
        this._values[option.key] = value
        continue
      }
      positionals.push(argument)
    }
    for (const option of this._options) if (option.required && this._values[option.key] === undefined) this.error('required option ' + option.flags + ' not specified')
    if (positionals.length > 0 && this._arguments.length === 0) this.error('too many arguments')
    if (this._actionHandler) this._actionHandler(...positionals, this)
    return this
  }
}
`
