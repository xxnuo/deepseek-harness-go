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

type profileRuntimeMount struct {
	engine       *harness.Engine
	bootstrapID  string
	bootstrapRun string
	cwd          string
	watchPaths   []string
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
			next, err := startCustomProfileGeneration(args, nextComposition, nextConfig)
			if err != nil {
				fmt.Fprintf(stderr, "dsh: config reload failed: %v\n", err)
				continue
			}
			_ = generation.close()
			generation = next
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
		return harness.DynamicCordisRunResponse{}, err
	}
	if !started.OK {
		return harness.DynamicCordisRunResponse{}, fmt.Errorf("dsh: profile plugin %q failed to start: %s", name, started.Message)
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
	mounted := &profileRuntimeMount{engine: engine, bootstrapID: bootstrap.PluginID, bootstrapRun: bootstrap.PluginRunID, cwd: cwd}
	for _, plugin := range plugins {
		if _, err := defineAndRunProfilePlugin(engine, "prof", plugin.id, plugin.body); err != nil {
			return nil, err
		}
		mounted.watchPaths = append(mounted.watchPaths, plugin.paths...)
	}
	sort.Strings(mounted.watchPaths)
	mounted.watchPaths = compactStrings(mounted.watchPaths)
	return mounted, nil
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
const plugin = profileModule.default ?? profileModule
const rowInject = %s
const pluginInject = Array.isArray(profileModule.inject) ? profileModule.inject : plugin.inject
const apply = typeof plugin === 'function' ? plugin : plugin.apply ?? profileModule.apply
return {
  inject: [...new Set([...(Array.isArray(pluginInject) ? pluginInject : []), ...rowInject])],
  apply(ctx) {
    __dshProfileContext = ctx
    if (typeof apply !== 'function') throw new Error('profile plugin must export apply(ctx, config) or a default function')
    return apply(ctx, %s)
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
	default:
		return nil, errors.New("profile plugin inject must be a service name or array")
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
	var document map[string]any
	if err := json.Unmarshal(manifest, &document); err != nil {
		return "", fmt.Errorf("invalid package.json for %q: %w", name, err)
	}
	exportKey := "."
	if subpath != "" {
		exportKey = "./" + filepath.ToSlash(subpath)
	}
	target := profileManifestExportTarget(document["exports"], exportKey)
	if target != "" {
		return resolveProfileJSFile(filepath.Join(root, filepath.FromSlash(target)))
	}
	if subpath != "" {
		return resolveProfileJSFile(filepath.Join(root, filepath.FromSlash(subpath)))
	}
	if target == "" {
		target, _ = document["module"].(string)
	}
	if target == "" {
		target, _ = document["main"].(string)
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

func profileManifestExportTarget(value any, key string) string {
	switch value := value.(type) {
	case string:
		if key == "." {
			return value
		}
	case map[string]any:
		if target, ok := value[key]; ok {
			return profileManifestConditionTarget(target)
		}
		if key == "." {
			return profileManifestConditionTarget(value)
		}
	}
	return ""
}

func profileManifestConditionTarget(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case map[string]any:
		for _, key := range []string{"import", "node", "default", "require"} {
			if target := profileManifestConditionTarget(value[key]); target != "" {
				return target
			}
		}
	}
	return ""
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
		"@deepseek-ai/dsh-cmdline": profileCmdlineModule,
		"node:fs":                  profileFSModule,
		"node:path":                profilePathModule,
	}
	return []api.Plugin{{
		Name: "dsh-profile-runtime",
		Setup: func(build api.PluginBuild) {
			build.OnResolve(api.OnResolveOptions{Filter: `^(?:commander|@deepseek-ai/dsh-cmdline|node:fs|node:path)$`}, func(args api.OnResolveArgs) (api.OnResolveResult, error) {
				return api.OnResolveResult{Path: args.Path, Namespace: "dsh-profile-runtime"}, nil
			})
			build.OnLoad(api.OnLoadOptions{Filter: `.*`, Namespace: "dsh-profile-runtime"}, func(args api.OnLoadArgs) (api.OnLoadResult, error) {
				source := modules[args.Path]
				return api.OnLoadResult{Contents: &source, Loader: api.LoaderJS}, nil
			})
		},
	}}
}

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
