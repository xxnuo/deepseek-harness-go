package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	harness "github.com/xxnuo/deepseek-harness-go"
)

type synchronizedBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (buffer *synchronizedBuffer) Write(data []byte) (int, error) {
	buffer.Lock()
	defer buffer.Unlock()
	return buffer.Buffer.Write(data)
}

func (buffer *synchronizedBuffer) String() string {
	buffer.Lock()
	defer buffer.Unlock()
	return buffer.Buffer.String()
}

func TestProfilePatchWatchReloadsWebAndKeepsLastGoodEngine(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DSH_HOME", home)
	t.Setenv("DSH_PROVIDER", "")
	t.Setenv("DSH_MODEL", "")
	loader, err := newProfileLoader(true)
	if err != nil {
		t.Fatal(err)
	}
	load := func() (*composition, harness.Config, error) {
		composed, err := loader.compose("web", nil, io.Discard)
		if err != nil {
			return nil, harness.Config{}, err
		}
		if err := composed.validate(); err != nil {
			return nil, harness.Config{}, err
		}
		cfg := engineConfig(loader, composed)
		cfg.Port = 0
		cfg.Persist = false
		cfg.SessionProjectionCache = nil
		return composed, cfg, nil
	}
	_, cfg, err := load()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := newReloadableWebServer(engine, nil, cfg, io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.close() })
	endpoint := "http://" + runtime.listener.Addr().String()
	if _, err := engine.CreateSession(context.Background(), cfg.Workspace, "reload-session", ""); err != nil {
		t.Fatal(err)
	}
	patch := filepath.Join(home, profilesDir, "web", profilePatchFile)
	ctx, cancel := context.WithCancel(context.Background())
	var watcher sync.WaitGroup
	var diagnostics synchronizedBuffer
	watcher.Add(1)
	paths := newProfileWatchPaths(patch)
	go func() {
		defer watcher.Done()
		watchProfilePatches(ctx, paths, func() error {
			nextComposition, nextConfig, err := load()
			if err != nil {
				return err
			}
			runtime.mu.RLock()
			generation := runtime.generation
			runtime.mu.RUnlock()
			plugins, err := buildProfileRuntimePlugins(nextComposition)
			if err != nil {
				return err
			}
			if generation.profileRuntime == nil {
				if len(plugins) > 0 {
					mounted, mountErr := mountProfileRuntimePluginSet(engine, nil, plugins)
					if mountErr != nil {
						return mountErr
					}
					generation.profileRuntime = mounted
				}
			} else if err := generation.profileRuntime.reconcile(plugins); err != nil {
				return err
			}
			if err := engine.ApplyRuntimeConfig(nextConfig); err != nil {
				return err
			}
			paths.setRuntime(generation.profileRuntime)
			return nil
		}, &diagnostics)
	}()
	t.Cleanup(func() {
		cancel()
		watcher.Wait()
	})
	time.Sleep(2 * profileWatchInterval)

	writeWebPatch(t, patch, "first-hot-model", "first-hot-provider")
	waitForWebModel(t, endpoint, "first-hot-model")
	waitForWebSubagentProvider(t, runtime, "first-hot-provider")
	runtime.mu.RLock()
	activeEngine := runtime.generation.engine
	runtime.mu.RUnlock()
	if activeEngine != engine {
		t.Fatal("profile reload replaced the Engine instead of updating it in place")
	}
	foundSession := false
	for _, session := range engine.ListSessions() {
		if session.SessionID == "reload-session" {
			foundSession = true
			break
		}
	}
	if !foundSession {
		t.Fatal("profile reload lost an existing session")
	}

	if err := os.WriteFile(patch, []byte("invalid: [\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return strings.Contains(diagnostics.String(), "config reload failed") })
	if model := webModel(t, endpoint); model != "first-hot-model" {
		t.Fatalf("invalid reload replaced last good model with %q", model)
	}
	if provider := webSubagentProvider(runtime, "first-hot-provider"); provider != "first-hot-provider" {
		t.Fatalf("invalid reload replaced last good subagent provider with %q", provider)
	}

	writeWebPatch(t, patch, "second-hot-model", "second-hot-provider")
	waitForWebModel(t, endpoint, "second-hot-model")
	waitForWebSubagentProvider(t, runtime, "second-hot-provider")
}

func TestProfileWatchTracksRuntimeModuleSources(t *testing.T) {
	source := filepath.Join(t.TempDir(), "plugin.mjs")
	if err := os.WriteFile(source, []byte("export const value = 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	paths := newProfileWatchPaths()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	reloaded := make(chan struct{}, 1)
	go watchProfilePatchesReady(ctx, paths, func() error {
		reloaded <- struct{}{}
		return nil
	}, io.Discard, ready)
	<-ready
	paths.setRuntime(&profileRuntimeMount{watchPaths: []string{source}})
	time.Sleep(2 * profileWatchInterval)
	if err := os.WriteFile(source, []byte("export const value = 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reloaded:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime module source change did not reload")
	}
}

func writeWebPatch(t *testing.T, path, model, provider string) {
	t.Helper()
	content := "- id: agent-default-model\n  config:\n    provider: echo\n    model: " + model + "\n" +
		"- insert:\n    - id: hot-subagent-acp\n      name: '@deepseek-ai/dsh-subagent-acp'\n      config:\n" +
		"        providerName: " + provider + "\n        command: fake-acp\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func waitForWebModel(t *testing.T, endpoint, want string) {
	t.Helper()
	waitFor(t, func() bool { return webModel(t, endpoint) == want })
}

func waitForWebSubagentProvider(t *testing.T, runtime *reloadableWebServer, want string) {
	t.Helper()
	waitFor(t, func() bool { return webSubagentProvider(runtime, want) == want })
}

func webSubagentProvider(runtime *reloadableWebServer, want ...string) string {
	runtime.mu.RLock()
	generation := runtime.generation
	runtime.mu.RUnlock()
	if generation == nil {
		return ""
	}
	for _, provider := range generation.engine.ListSubagentProviders() {
		for _, expected := range want {
			if provider.Name == expected {
				return provider.Name
			}
		}
	}
	return ""
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func webModel(t *testing.T, endpoint string) string {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, endpoint+"/api/host.describe", strings.NewReader(`{"type":"client-request","rpcId":"reload","method":"host.describe","payload":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return ""
	}
	defer response.Body.Close()
	var envelope struct {
		Result struct {
			Value struct {
				Model string `json:"model"`
			} `json:"value"`
		} `json:"result"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&envelope) != nil {
		return ""
	}
	return envelope.Result.Value.Model
}
