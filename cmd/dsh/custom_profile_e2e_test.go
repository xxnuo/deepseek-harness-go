package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	harness "github.com/xxnuo/deepseek-harness-go"
)

func TestStandaloneCustomProfileMJSArgsHelpAndHMR(t *testing.T) {
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "dsh")
	build := runtimeAssetGoCommand(repository, "build", "-o", binary, "./cmd/dsh")
	build.Dir = repository
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build dsh: %v\n%s", err, output)
	}

	home, profilePatch, marker := writeCustomMJSProfile(t)
	environment := append(os.Environ(), "DSH_HOME="+home)
	command := exec.Command(binary, "--profile", "startup", "--generation", "flagged")
	command.Env = environment
	command.Dir = home
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	t.Cleanup(func() {
		if command.Process == nil {
			return
		}
		_ = command.Process.Kill()
		select {
		case <-wait:
		default:
		}
	})

	if err := waitForProfileMarker(marker, "flagged:bundle"); err != nil {
		t.Fatalf("%v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(profilePatch, []byte("- id: fixture-app\n  config:\n    generation: !!js ctx.fixtureStartup.generation ?? 'bundle-default'\n    patch: hot\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := waitForProfileMarker(marker, "flagged:hot"); err != nil {
		t.Fatalf("%v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-wait:
		if err != nil {
			t.Fatalf("custom profile exit: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("custom profile did not stop\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}

	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	help := exec.Command(binary, "--profile", "startup", "--help")
	help.Env, help.Dir = environment, home
	helpOutput, err := help.CombinedOutput()
	if err != nil {
		t.Fatalf("custom profile help: %v\n%s", err, helpOutput)
	}
	if text := string(helpOutput); !strings.Contains(text, "Usage: fixture [options]") || !strings.Contains(text, "--generation <value>") {
		t.Fatalf("custom help = %q", text)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("app consumer started while rendering help: %v", err)
	}
}

func TestCompositionAcceptsMixedExternalHostPlugin(t *testing.T) {
	plugin := filepath.Join(t.TempDir(), "plugin.mjs")
	if err := os.WriteFile(plugin, []byte("export function apply() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	composed := testComposition(t, fmt.Sprintf(`
- id: webserver
  name: '@deepseek-ai/dsh-host-webserver'
- id: extension
  name: %q
`, plugin))
	if err := composed.validateSupportedPlugins(); err != nil {
		t.Fatalf("mixed external plugin validation = %v", err)
	}
	plugins, err := buildProfileRuntimePlugins(composed)
	if err != nil || len(plugins) != 1 || plugins[0].id != "extension" {
		t.Fatalf("mixed external plugins = %#v, %v", plugins, err)
	}
}

func TestExternalPluginEntriesUseQualifiedNestedIDs(t *testing.T) {
	plugin := filepath.Join(t.TempDir(), "plugin.mjs")
	if err := os.WriteFile(plugin, []byte("export function apply() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	composed := testComposition(t, fmt.Sprintf(`
- id: outer
  name: cordis:group
  group: true
  config:
    - id: child
      name: %q
- id: sibling
  name: cordis:group
  group: true
  config:
    - id: child
      name: %q
`, plugin, plugin))
	entries := composed.externalPluginEntries()
	if len(entries) != 2 || entries[0].id != "outer:child" || entries[1].id != "sibling:child" {
		t.Fatalf("qualified external plugin entries = %#v", entries)
	}
}

func TestProfileRuntimePublishesPendingPluginInventoryPhase(t *testing.T) {
	active := "active"
	cfg := harness.DefaultConfig()
	cfg.Persist = false
	cfg.PluginInventory = []harness.PluginInventoryEntry{
		{EntryID: "waiting", ModuleName: "fixture:waiting", Enabled: true, FiberPhase: &active},
		{EntryID: "consumer", ModuleName: "fixture:consumer", Enabled: true, FiberPhase: &active},
		{EntryID: "provider", ModuleName: "fixture:provider", Enabled: true, FiberPhase: &active},
	}
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	if _, err := mountProfileRuntimePluginSet(engine, nil, []profileRuntimePlugin{
		{id: "waiting", body: "return { inject: ['neverReady'], apply() {} }"},
		{id: "consumer", body: "return { inject: ['readyLater'], apply() {} }"},
		{id: "provider", body: "return { apply(ctx) { ctx.provide('readyLater', {}) } }"},
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(engine.Handler())
	t.Cleanup(server.Close)
	snapshot := profileRPC(t, server.URL, "pluginInventory/list", map[string]any{"args": map[string]any{}})
	entries, _ := snapshot["entries"].([]any)
	if len(entries) != 3 {
		t.Fatalf("plugin inventory = %#v", snapshot)
	}
	waiting, _ := entries[0].(map[string]any)
	consumer, _ := entries[1].(map[string]any)
	provider, _ := entries[2].(map[string]any)
	if waiting["entryId"] != "waiting" || waiting["moduleName"] != "fixture:waiting" || waiting["enabled"] != true || waiting["fiberPhase"] != "pending" {
		t.Fatalf("pending inventory entry = %#v", waiting)
	}
	if consumer["fiberPhase"] != "active" || provider["fiberPhase"] != "active" {
		t.Fatalf("reactivated inventory entries = %#v", entries)
	}
}

func TestStandaloneMixedExternalProfileReachesHeadlessAndWeb(t *testing.T) {
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "dsh")
	build := runtimeAssetGoCommand(repository, "build", "-o", binary, "./cmd/dsh")
	build.Dir = repository
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build dsh: %v\n%s", err, output)
	}

	home := t.TempDir()
	plugin := filepath.Join(home, "profile-plugin.mjs")
	values := filepath.Join(home, "profile-values.mjs")
	launcherMarker := filepath.Join(home, "profile-launcher.json")
	preexisting := filepath.Join(home, "preexisting.flag")
	external := filepath.Join(home, "external.flag")
	if err := os.WriteFile(preexisting, []byte("ready"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeProfileValues := func(version, marker string) {
		t.Helper()
		body := fmt.Sprintf("export const version = %q\nexport const promptMarker = %q\n", version, marker)
		if err := os.WriteFile(values, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeProfileValues("cold", "GLOBAL PROFILE E2E MARKER")
	pluginBody := fmt.Sprintf(`
import { existsSync, writeFileSync } from 'node:fs'
import { version, promptMarker } from './profile-values.mjs'
export const inject = ['cmdlineArgs', 'tools', 'systemPrompt', 'timer']
export function apply(ctx) {
  const args = ctx.get('cmdlineArgs').get()
  writeFileSync(%q, JSON.stringify({
    args,
    hasExit: typeof ctx.get('appExit') === 'function',
    version,
  }))
  if (args.includes('profile-exit')) {
    const timer = setInterval(() => {
      if (!existsSync('preexisting.flag') || !existsSync(%q)) return
      clearInterval(timer)
      ctx.get('appIO').writeOut('async profile exit\n')
      ctx.get('appExit')(7)
    }, 10)
    return
  }
  ctx.systemPrompt.section({ name: 'profile:e2e', order: 1, text: promptMarker })
  ctx.tools.register(harness.defineTool({
    name: 'profile_probe',
    description: 'Verify the external profile plugin is active.',
    parameters: {},
    output: {
      schema: { type: 'string' },
      render(_args, value) { return [{ type: 'text', text: value }] },
    },
    execute() { return 'profile active' },
  }))
}
`, launcherMarker, external)
	if err := os.WriteFile(plugin, []byte(pluginBody), 0o644); err != nil {
		t.Fatal(err)
	}
	patch := fmt.Sprintf("- insert:\n    - id: external-profile-e2e\n      name: %q\n", plugin)
	for _, name := range []string{"headless", "web"} {
		dir := filepath.Join(home, profilesDir, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, profilePatchFile), []byte(patch), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	requests := make(chan string, 16)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		requests <- string(body)
		if strings.Contains(string(body), "profile-exit") {
			<-request.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"profile e2e response\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(model.Close)
	environment := append(os.Environ(),
		"DSH_HOME="+home,
		"DEEPSEEK_API_KEY=profile-e2e-key",
		"DEEPSEEK_BASE_URL="+model.URL,
	)
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "headless", args: []string{"--profile", "headless", "profile-exit"}},
		{name: "web", args: []string{"--profile", "web", "--no-open", "--port", "0", "--trusted-host", "profile-exit"}},
	} {
		if err := os.Remove(external); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		createExternal := time.AfterFunc(150*time.Millisecond, func() {
			_ = os.WriteFile(external, []byte("ready"), 0o644)
		})
		commandContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		command := exec.CommandContext(commandContext, binary, test.args...)
		command.Env, command.Dir = environment, home
		output, err := command.CombinedOutput()
		createExternal.Stop()
		cancel()
		if commandContext.Err() == context.DeadlineExceeded {
			t.Fatalf("%s profile timed out: %q", test.name, output)
		}
		exit, ok := err.(*exec.ExitError)
		if !ok || exit.ExitCode() != 7 || !strings.Contains(string(output), "async profile exit\n") {
			t.Fatalf("%s profile appExit = %q, %v", test.name, output, err)
		}
	}

	headless := exec.Command(binary, "--profile", "headless", "verify", "headless", "profile")
	headless.Env, headless.Dir = environment, home
	if output, err := headless.CombinedOutput(); err != nil || string(output) != "profile e2e response\n" {
		t.Fatalf("headless profile = %q, %v", output, err)
	}
	requireProfileLauncherMarker(t, launcherMarker, []string{"verify", "headless", "profile"})
	requireProfileModelRequest(t, requests, "verify headless profile", "GLOBAL PROFILE E2E MARKER")

	web := exec.Command(binary, "--profile", "web", "--no-open", "--port", "0")
	web.Env, web.Dir = environment, home
	stdout, err := web.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	web.Stderr = &stderr
	if err := web.Start(); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- web.Wait() }()
	t.Cleanup(func() {
		if web.Process == nil {
			return
		}
		_ = web.Process.Signal(syscall.SIGTERM)
		select {
		case <-wait:
		case <-time.After(5 * time.Second):
			_ = web.Process.Kill()
			<-wait
		}
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read web address: %v; stderr=%s", err, stderr.String())
	}
	requireProfileLauncherMarker(t, launcherMarker, []string{"--no-open", "--port", "0"})
	endpoint := strings.TrimSpace(strings.TrimPrefix(line, "dsh web: "))
	session := profileRPC(t, endpoint, "session.create", map[string]any{"cwd": home})
	sessionID, _ := session["sessionId"].(string)
	if sessionID == "" {
		t.Fatalf("session.create = %#v", session)
	}
	profileRPC(t, endpoint, "session.prompt", map[string]any{
		"sessionId": sessionID,
		"mode":      "queue",
		"content":   []map[string]any{{"type": "text", "text": "verify web profile"}},
	})
	requireProfileModelRequest(t, requests, "verify web profile", "GLOBAL PROFILE E2E MARKER")

	writeProfileValues("hot", "GLOBAL PROFILE E2E HOT")
	waitFor(t, func() bool {
		data, err := os.ReadFile(launcherMarker)
		return err == nil && strings.Contains(string(data), `"version":"hot"`)
	})
	time.Sleep(2 * profileWatchInterval)
	hotSession := profileRPC(t, endpoint, "session.create", map[string]any{"cwd": home})
	hotSessionID, _ := hotSession["sessionId"].(string)
	profileRPC(t, endpoint, "session.prompt", map[string]any{
		"sessionId": hotSessionID,
		"mode":      "queue",
		"content":   []map[string]any{{"type": "text", "text": "verify hot web profile"}},
	})
	requireProfileModelRequest(t, requests, "verify hot web profile", "GLOBAL PROFILE E2E HOT")
}

func requireProfileLauncherMarker(t *testing.T, path string, wantArgs []string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var marker struct {
		Args    []string `json:"args"`
		HasExit bool     `json:"hasExit"`
		Version string   `json:"version"`
	}
	if err := json.Unmarshal(data, &marker); err != nil {
		t.Fatalf("decode profile launcher marker: %v: %s", err, data)
	}
	if !marker.HasExit || strings.Join(marker.Args, "\x00") != strings.Join(wantArgs, "\x00") {
		t.Fatalf("profile launcher marker = %#v, want args %#v and appExit", marker, wantArgs)
	}
}

func requireProfileModelRequest(t *testing.T, requests <-chan string, task, marker string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	last := ""
	for {
		select {
		case body := <-requests:
			if !strings.Contains(body, task) {
				continue
			}
			last = body
			if !strings.Contains(body, marker) || !strings.Contains(body, "profile_probe") {
				continue
			}
			return
		case <-deadline:
			t.Fatalf("no external-profile model request for %q; last matching request: %s", task, last)
		}
	}
}

func profileRPC(t *testing.T, endpoint, method string, payload any) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"type": "client-request", "rpcId": method, "method": method, "payload": payload})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(endpoint+"/api/"+method, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope struct {
		Result struct {
			OK    bool           `json:"ok"`
			Value map[string]any `json:"value"`
			Error any            `json:"error"`
		} `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !envelope.Result.OK {
		t.Fatalf("%s status=%d result=%#v", method, response.StatusCode, envelope.Result)
	}
	return envelope.Result.Value
}

func writeCustomMJSProfile(t *testing.T) (home, patch, marker string) {
	t.Helper()
	home = t.TempDir()
	profileDir := filepath.Join(home, "profiles", "startup")
	bundleDir := filepath.Join(profileDir, "node_modules", "dsh-startup-bundle")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	startup := filepath.Join(bundleDir, "startup.mjs")
	app := filepath.Join(bundleDir, "app.mjs")
	if err := os.WriteFile(startup, []byte(`
import { Command } from 'commander'
import { parseCmdline } from '@deepseek-ai/dsh-cmdline'
export const inject = ['cmdlineArgs']
export function apply(ctx) {
  const program = new Command().name('fixture').option('--generation <value>', 'echoed generation')
  program.action(() => ctx.provide('fixtureStartup', { generation: program.opts().generation }))
  parseCmdline(ctx, program)
}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(app, []byte(`
import { writeFileSync } from 'node:fs'
import { join } from 'node:path'
export default function apply(_ctx, config = {}) {
  writeFileSync(join(process.env.DSH_HOME, 'profile-marker'), String(config.generation) + ':' + String(config.patch))
}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	bundlePatch := `
- insert:
    - id: fixture-app
      name: dsh-startup-bundle/app
      inject: [fixtureStartup]
      config:
        generation: !!js ctx.fixtureStartup.generation ?? 'bundle-default'
        patch: bundle
    - id: fixture-startup
      name: dsh-startup-bundle/startup
`
	if err := os.WriteFile(filepath.Join(bundleDir, profilePatchFile), []byte(bundlePatch), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundleDir, "package.json"), []byte(`{
  "name": "dsh-startup-bundle",
  "version": "0.0.0",
  "type": "module",
  "exports": {"./app": "./app.mjs", "./startup": {"import": "./startup.mjs"}},
  "dsh": {"bundle": {"patch": "./cordis.patch.yml"}}
}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(profileDir, "package.json"), []byte(`{
  "name": "dsh-profile-startup",
  "private": true,
  "dependencies": {},
  "dsh": {"profile": {"bundles": ["dsh-startup-bundle"]}}
}`), 0o644); err != nil {
		t.Fatal(err)
	}
	patch = filepath.Join(profileDir, profilePatchFile)
	if err := os.WriteFile(patch, []byte("[]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return home, patch, filepath.Join(home, "profile-marker")
}

func waitForProfileMarker(path, want string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && string(data) == want {
			return nil
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	data, err := os.ReadFile(path)
	return fmt.Errorf("profile marker = %q, err=%v, want %q", data, err, want)
}
