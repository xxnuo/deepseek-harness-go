package harness

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeepSeekExtensionRegistryLifecycle(t *testing.T) {
	registry := newDeepSeekLlmAPIExtensionRegistry()
	started := make(chan string, 2)
	release := make(chan struct{})
	mutable := map[string]any{"value": "original"}
	alphaFailure := errors.New("alpha acceptance failed")
	betaFailure := errors.New("beta acceptance failed")
	var alphaAccepted, betaAccepted atomic.Int32

	disposeAlpha, err := registry.register("alpha", func(_ context.Context, request DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
		request.Body["model"] = "mutated-provider-view"
		started <- "alpha"
		<-release
		return DeepSeekLlmAPIExtensionContribution{Value: mutable, Accept: func() error {
			alphaAccepted.Add(1)
			return alphaFailure
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.register("alpha", func(context.Context, DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
		return DeepSeekLlmAPIExtensionContribution{}, nil
	}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate registration error = %v", err)
	}
	if _, err := registry.register("beta", func(_ context.Context, request DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
		started <- "beta"
		<-release
		return DeepSeekLlmAPIExtensionContribution{Present: true, Value: []any{request.Body["model"]}, Accept: func() error {
			betaAccepted.Add(1)
			return betaFailure
		}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.register("null", func(context.Context, DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
		return DeepSeekLlmAPIExtensionContribution{Present: true, Value: nil}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.register("omitted", func(context.Context, DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
		return DeepSeekLlmAPIExtensionContribution{}, nil
	}); err != nil {
		t.Fatal(err)
	}

	body := map[string]any{"model": "base"}
	type prepareResult struct {
		prepared *PreparedDeepSeekLlmAPIExtensions
		err      error
	}
	preparedCh := make(chan prepareResult, 1)
	go func() {
		prepared, prepareErr := registry.prepare(t.Context(), DeepSeekLlmAPIExtensionRequest{Body: body})
		preparedCh <- prepareResult{prepared: prepared, err: prepareErr}
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("providers were not prepared concurrently")
		}
	}
	close(release)
	result := <-preparedCh
	if result.err != nil {
		t.Fatal(result.err)
	}
	if body["model"] != "base" {
		t.Fatalf("base body was mutated: %#v", body)
	}
	mutable["value"] = "changed"
	if got := result.prepared.Fields["alpha"]; !reflect.DeepEqual(got, map[string]any{"value": "original"}) {
		t.Fatalf("detached alpha field = %#v", got)
	}
	if got := result.prepared.Fields["beta"]; !reflect.DeepEqual(got, []any{"base"}) {
		t.Fatalf("isolated beta field = %#v", got)
	}
	if value, exists := result.prepared.Fields["null"]; !exists || value != nil {
		t.Fatalf("JSON null contribution = %#v, exists=%v", value, exists)
	}
	if _, exists := result.prepared.Fields["omitted"]; exists {
		t.Fatal("zero contribution was not omitted")
	}
	for range 2 {
		acceptErr := result.prepared.Accept()
		if !errors.Is(acceptErr, alphaFailure) || !errors.Is(acceptErr, betaFailure) {
			t.Fatalf("accept error = %v", acceptErr)
		}
	}
	if alphaAccepted.Load() != 1 || betaAccepted.Load() != 1 {
		t.Fatalf("accept counts = alpha:%d beta:%d", alphaAccepted.Load(), betaAccepted.Load())
	}

	if err := disposeAlpha(); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.register("alpha", func(context.Context, DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
		return DeepSeekLlmAPIExtensionContribution{Present: true, Value: "replacement"}, nil
	}); err != nil {
		t.Fatalf("register after dispose: %v", err)
	}
}

func TestDeepSeekExtensionRegistryCancellationStopsWaiting(t *testing.T) {
	registry := newDeepSeekLlmAPIExtensionRegistry()
	started := make(chan struct{})
	release := make(chan struct{})
	if _, err := registry.register("blocked", func(context.Context, DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
		close(started)
		<-release
		return DeepSeekLlmAPIExtensionContribution{Present: true, Value: true}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := registry.prepare(ctx, DeepSeekLlmAPIExtensionRequest{Body: map[string]any{}})
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("prepare kept waiting for a provider after cancellation")
	}
	close(release)
}

func TestDeepSeekSessionLogIncrementalAcceptance(t *testing.T) {
	enabled, inventoryDisabled := true, false
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.Provider = "echo"
	cfg.Workspace = t.TempDir()
	cfg.Terminal.Disabled = true
	cfg.DeepSeekSessionLogEnabled = &enabled
	cfg.DeepSeekPluginInventoryEnabled = &inventoryDisabled
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	sessionID, err := engine.CreateSession(t.Context(), cfg.Workspace, "deepseek-session-log", "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := engine.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEvent(session, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	first, err := engine.prepareDeepSeekSessionLog(ChatRequest{SessionID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	firstValue, ok := first.Value.(DeepSeekSessionLogExtension)
	if !first.Present || !ok || firstValue.AfterSeq != -1 || firstValue.ThroughSeq != 0 || len(firstValue.Events) != 1 {
		t.Fatalf("first session log = %#v", first)
	}
	if err := first.Accept(); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEvent(session, "step/start", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	second, err := engine.prepareDeepSeekSessionLog(ChatRequest{SessionID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	secondValue := second.Value.(DeepSeekSessionLogExtension)
	if secondValue.AfterSeq != 0 || secondValue.ThroughSeq != 2 || len(secondValue.Events) != 2 || secondValue.Events[0].Type != "session-log-deepseek/delivery-accepted" {
		t.Fatalf("second session log = %#v", secondValue)
	}

	forkHeader := SessionHeader{ID: "child", ParentSession: sessionID, SeedLength: 2}
	forkEvents := []Event{
		{Type: "turn/start", Seq: 0, Data: map[string]any{"turn": 1}},
		{Type: "session-log-deepseek/delivery-accepted", Seq: 1, Data: map[string]any{"sessionId": sessionID, "throughSeq": 0}},
	}
	if through, err := acceptedDeepSeekSessionLogThrough(forkHeader, forkEvents); err != nil || through != -1 {
		t.Fatalf("fork watermark = %d, %v", through, err)
	}
	malformed := []Event{{Type: "session-log-deepseek/delivery-accepted", Seq: 1, Data: map[string]any{"sessionId": "", "throughSeq": 0}}}
	if _, err := acceptedDeepSeekSessionLogThrough(SessionHeader{ID: "malformed"}, malformed); err == nil {
		t.Fatal("malformed watermark was accepted")
	}
}

func TestDeepSeekPluginPackageInventoryResolution(t *testing.T) {
	root := t.TempDir()
	writePackage := func(dir, name, version string) string {
		t.Helper()
		packageDir := filepath.Join(root, dir)
		if err := os.MkdirAll(packageDir, 0o755); err != nil {
			t.Fatal(err)
		}
		manifest, err := json.Marshal(map[string]any{"type": "module", "name": name, "version": version})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(packageDir, "package.json"), manifest, 0o600); err != nil {
			t.Fatal(err)
		}
		module := filepath.Join(packageDir, "plugin.mjs")
		if err := os.WriteFile(module, []byte("export default () => {}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return module
	}
	one := writePackage("one", "one", "1.0.0")
	two := writePackage("two", "two", "2.0.0")
	if err := os.MkdirAll(filepath.Join(root, "loose"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "loose", "package.json"), []byte(`{"type":"module"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	loose := filepath.Join(root, "loose", "plugin.mjs")
	if err := os.WriteFile(loose, []byte("export default () => {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	active, pending := "active", "pending"
	enabled := true
	engine := &Engine{
		cfg: Config{Workspace: root, DeepSeekPluginInventoryEnabled: &enabled},
		pluginInventory: []PluginInventoryEntry{
			{EntryID: "two", ModuleName: two, Enabled: true, FiberPhase: &active},
			{EntryID: "one", ModuleName: "./" + filepath.ToSlash(strings.TrimPrefix(one, root+string(filepath.Separator))), Enabled: true, FiberPhase: &active},
			{EntryID: "one-duplicate", ModuleName: "file://" + one, Enabled: true, FiberPhase: &active},
			{EntryID: "disabled", PackageName: "disabled", PackageVersion: "1", Enabled: false, FiberPhase: &active},
			{EntryID: "pending", PackageName: "pending", PackageVersion: "1", Enabled: true, FiberPhase: &pending},
			{EntryID: "no-fiber", PackageName: "no-fiber", PackageVersion: "1", Enabled: true},
			{EntryID: "loose", ModuleName: loose, Enabled: true, FiberPhase: &active},
			{EntryID: "url", ModuleName: "https://plugins.example/plugin.mjs", Enabled: true, FiberPhase: &active},
			{EntryID: "cordis", ModuleName: "cordis:noop", Enabled: true, FiberPhase: &active},
		},
	}
	contribution, err := engine.prepareDeepSeekPluginInventory(ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	value, ok := contribution.Value.(DeepSeekPluginPackageInventoryExtension)
	if !contribution.Present || !ok {
		t.Fatalf("inventory contribution = %#v", contribution)
	}
	want := []DeepSeekPluginPackageIdentity{{Name: "one", Version: "1.0.0"}, {Name: "two", Version: "2.0.0"}}
	if !reflect.DeepEqual(value.Packages, want) {
		t.Fatalf("inventory packages = %#v, want %#v", value.Packages, want)
	}

	badDir := filepath.Join(root, "bad")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "package.json"), []byte(`{"name":"bad"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	badModule := filepath.Join(badDir, "plugin.mjs")
	if err := os.WriteFile(badModule, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	engine.pluginInventory = []PluginInventoryEntry{{EntryID: "bad", ModuleName: badModule, Enabled: true, FiberPhase: &active}}
	if _, err := engine.prepareDeepSeekPluginInventory(ChatRequest{}); err == nil || !strings.Contains(err.Error(), "must declare non-empty name and version") {
		t.Fatalf("malformed manifest error = %v", err)
	}
}

func TestDeepSeekPluginPackageInventoryUsesLoaderBasesAndImmutableManifestCache(t *testing.T) {
	root := t.TempDir()
	hostBase := filepath.Join(root, "host")
	presetBase := filepath.Join(root, "preset")
	workspace := filepath.Join(root, "workspace")
	writeManifest := func(path, name, version string) string {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		manifest, err := json.Marshal(map[string]any{"name": name, "version": version})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, manifest, 0o600); err != nil {
			t.Fatal(err)
		}
		module := filepath.Join(filepath.Dir(path), "plugin.mjs")
		if err := os.WriteFile(module, []byte("export default () => {}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return module
	}
	hostModule := writeManifest(filepath.Join(hostBase, "node_modules", "shared", "package.json"), "shared", "1.0.0")
	_ = writeManifest(filepath.Join(presetBase, "node_modules", "shared", "package.json"), "shared", "2.0.0")
	relativeModule := writeManifest(filepath.Join(presetBase, "relative", "package.json"), "relative", "3.0.0")
	active := "active"
	enabled := true
	engine := &Engine{
		cfg: Config{
			Workspace: workspace, PluginDir: hostBase,
			PluginDirs:                     []string{filepath.Join(presetBase, "node_modules")},
			DeepSeekPluginInventoryEnabled: &enabled,
		},
		pluginInventory: []PluginInventoryEntry{
			{EntryID: "bare", ModuleName: "shared/plugin.mjs", ModuleBase: presetBase, BarePackageBase: hostBase, Enabled: true, FiberPhase: &active},
			{EntryID: "relative", ModuleName: "./relative/plugin.mjs", ModuleBase: presetBase, Enabled: true, FiberPhase: &active},
		},
	}
	packagesFor := func() []DeepSeekPluginPackageIdentity {
		t.Helper()
		contribution, err := engine.prepareDeepSeekPluginInventory(ChatRequest{})
		if err != nil {
			t.Fatal(err)
		}
		value, ok := contribution.Value.(DeepSeekPluginPackageInventoryExtension)
		if !contribution.Present || !ok {
			t.Fatalf("inventory contribution = %#v", contribution)
		}
		return value.Packages
	}
	want := []DeepSeekPluginPackageIdentity{{Name: "relative", Version: "3.0.0"}, {Name: "shared", Version: "1.0.0"}}
	if got := packagesFor(); !reflect.DeepEqual(got, want) {
		t.Fatalf("initial inventory = %#v, want %#v", got, want)
	}
	if err := os.WriteFile(filepath.Join(hostBase, "node_modules", "shared", "package.json"), []byte(`{"name":"shared","version":"9.0.0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(presetBase, "relative", "package.json"), []byte(`{"name":"relative","version":"8.0.0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := packagesFor(); !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory after manifest replacement = %#v, want cached %#v", got, want)
	}
	_ = hostModule
	_ = relativeModule
}

func TestDeepSeekPluginPackageInventoryResolvesMaterializedPresetPackages(t *testing.T) {
	root := t.TempDir()
	packageRoot := filepath.Join(root, "packages")
	packageDir := filepath.Join(packageRoot, "core", "fixture")
	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packageDir, "package.json"), []byte(`{"name":"@deepseek-ai/dsh-fixture","version":"0.1.2-alpha.4"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	presetRoot := filepath.Join(root, "presets")
	presetDir := filepath.Join(presetRoot, "standard")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte("- id: fixture\n  name: '@deepseek-ai/dsh-fixture'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	active, enabled := "active", true
	session := &Session{Header: SessionHeader{ID: "preset-session", AgentPreset: "standard"}}
	engine := &Engine{
		cfg: Config{
			Workspace: root, PluginDir: packageRoot, PresetDir: presetRoot,
			DeepSeekPluginInventoryEnabled: &enabled,
		},
		sessions: map[string]*Session{"preset-session": session},
		pluginInventory: []PluginInventoryEntry{{
			EntryID: "host", PackageName: "host-package", PackageVersion: "1.0.0",
			Enabled: true, FiberPhase: &active,
		}},
	}
	contribution, err := engine.prepareDeepSeekPluginInventory(ChatRequest{SessionID: "preset-session"})
	if err != nil {
		t.Fatal(err)
	}
	value, ok := contribution.Value.(DeepSeekPluginPackageInventoryExtension)
	if !contribution.Present || !ok {
		t.Fatalf("inventory contribution = %#v", contribution)
	}
	want := []DeepSeekPluginPackageIdentity{
		{Name: "@deepseek-ai/dsh-fixture", Version: "0.1.2-alpha.4"},
		{Name: "host-package", Version: "1.0.0"},
	}
	if !reflect.DeepEqual(value.Packages, want) {
		t.Fatalf("inventory packages = %#v, want %#v", value.Packages, want)
	}
}

func TestDeepSeekPluginPackageInventoryUsesPinnedPresetGeneration(t *testing.T) {
	root := t.TempDir()
	packageRoot := filepath.Join(root, "packages")
	writePackage := func(dir, name, version string) {
		t.Helper()
		packageDir := filepath.Join(packageRoot, "core", dir)
		if err := os.MkdirAll(packageDir, 0o755); err != nil {
			t.Fatal(err)
		}
		manifest, err := json.Marshal(map[string]any{"name": name, "version": version})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(packageDir, "package.json"), manifest, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	const firstPackage = "@deepseek-ai/dsh-generation-first"
	const secondPackage = "@deepseek-ai/dsh-generation-second-longer"
	writePackage("first", firstPackage, "1.0.0")
	writePackage("second", secondPackage, "2.0.0")

	presetRoot := filepath.Join(root, "presets")
	presetDir := filepath.Join(presetRoot, "edited")
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := filepath.Join(presetDir, "agent.cordis.yml")
	writePreset := func(packageName string) {
		t.Helper()
		body := "- id: package\n  name: '" + packageName + "'\n"
		if err := os.WriteFile(composition, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writePreset(firstPackage)

	enabled := true
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir = t.TempDir(), t.TempDir(), presetRoot
	cfg.PluginDir, cfg.Persist = packageRoot, false
	cfg.DeepSeekPluginInventoryEnabled = &enabled
	cfg.SessionTitleLLM.Enabled = false
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	firstID, err := engine.CreateSession(t.Context(), cfg.Workspace, "inventory-generation-first", "edited")
	if err != nil {
		t.Fatal(err)
	}
	packagesFor := func(sessionID string) []DeepSeekPluginPackageIdentity {
		t.Helper()
		contribution, err := engine.prepareDeepSeekPluginInventory(ChatRequest{SessionID: sessionID})
		if err != nil {
			t.Fatal(err)
		}
		value, ok := contribution.Value.(DeepSeekPluginPackageInventoryExtension)
		if !contribution.Present || !ok {
			t.Fatalf("inventory contribution = %#v", contribution)
		}
		return value.Packages
	}
	firstWant := []DeepSeekPluginPackageIdentity{{Name: firstPackage, Version: "1.0.0"}}
	if got := packagesFor(firstID); !reflect.DeepEqual(got, firstWant) {
		t.Fatalf("first inventory before edit = %#v, want %#v", got, firstWant)
	}
	firstSession, err := engine.getSession(firstID)
	if err != nil {
		t.Fatal(err)
	}
	firstEntries := engine.presetPluginInventoryEntries(firstSession)
	if len(firstEntries) != 1 || firstEntries[0].ModuleBase != presetDir || firstEntries[0].BarePackageBase != packageRoot {
		t.Fatalf("preset loader bases = %#v, want module=%q bare=%q", firstEntries, presetDir, packageRoot)
	}

	writePreset(secondPackage)
	if got := packagesFor(firstID); !reflect.DeepEqual(got, firstWant) {
		t.Fatalf("first inventory after edit = %#v, want pinned %#v", got, firstWant)
	}
	secondID, err := engine.CreateSession(t.Context(), cfg.Workspace, "inventory-generation-second", "edited")
	if err != nil {
		t.Fatal(err)
	}
	secondWant := []DeepSeekPluginPackageIdentity{{Name: secondPackage, Version: "2.0.0"}}
	if got := packagesFor(secondID); !reflect.DeepEqual(got, secondWant) {
		t.Fatalf("second inventory = %#v, want %#v", got, secondWant)
	}
}

func TestOfficialDeepSeekRequestExtensionHTTPBoundary(t *testing.T) {
	t.Run("prepare failure and collision do not dispatch", func(t *testing.T) {
		for _, test := range []struct {
			name  string
			field string
			value DeepSeekLlmAPIExtensionContribution
			err   error
		}{
			{name: "prepare", field: "custom", err: errors.New("prepare failed")},
			{name: "collision", field: "model", value: DeepSeekLlmAPIExtensionContribution{Present: true, Value: "collision"}},
		} {
			t.Run(test.name, func(t *testing.T) {
				var requests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
				defer server.Close()
				engine := &Engine{deepSeekExtensions: newDeepSeekLlmAPIExtensionRegistry()}
				if _, err := engine.RegisterDeepSeekLlmAPIExtension(test.field, func(context.Context, DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
					return test.value, test.err
				}); err != nil {
					t.Fatal(err)
				}
				provider := NewOpenAIProvider("deepseek-official", server.URL, "key", "model")
				provider.deepSeekExtensions = engine
				_, err := provider.Complete(t.Context(), ChatRequest{}, func(Delta) error { return nil })
				var providerErr *ProviderError
				if !errors.As(err, &providerErr) || providerErr.Code != "REQUEST_EXTENSION" {
					t.Fatalf("request error = %v", err)
				}
				if requests.Load() != 0 {
					t.Fatalf("requests = %d", requests.Load())
				}
			})
		}
	})

	t.Run("non-2xx does not accept", func(t *testing.T) {
		var accepted atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"rejected"}}`)
		}))
		defer server.Close()
		engine := &Engine{deepSeekExtensions: newDeepSeekLlmAPIExtensionRegistry()}
		_, _ = engine.RegisterDeepSeekLlmAPIExtension("custom", func(context.Context, DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
			return DeepSeekLlmAPIExtensionContribution{Present: true, Value: true, Accept: func() error { accepted.Add(1); return nil }}, nil
		})
		provider := NewOpenAIProvider("deepseek-official", server.URL, "key", "model")
		provider.deepSeekExtensions = engine
		if _, err := provider.Complete(t.Context(), ChatRequest{}, func(Delta) error { return nil }); err == nil {
			t.Fatal("non-2xx request unexpectedly succeeded")
		}
		if accepted.Load() != 0 {
			t.Fatalf("accept count = %d", accepted.Load())
		}
	})

	t.Run("2xx accepts before stream parsing", func(t *testing.T) {
		var accepted atomic.Int32
		var body map[string]any
		var compactHeader string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			compactHeader = request.Header.Get("X-DeepSeek-Harness-Compact")
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode request: %v", err)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\n\n")
		}))
		defer server.Close()
		engine := &Engine{deepSeekExtensions: newDeepSeekLlmAPIExtensionRegistry()}
		_, _ = engine.RegisterDeepSeekLlmAPIExtension("custom_null", func(context.Context, DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
			return DeepSeekLlmAPIExtensionContribution{Present: true, Value: nil, Accept: func() error { accepted.Add(1); return nil }}, nil
		})
		provider := NewOpenAIProvider("deepseek-official", server.URL, "key", "model")
		provider.deepSeekExtensions = engine
		if _, err := provider.Complete(t.Context(), ChatRequest{Purpose: "compaction"}, func(Delta) error { return nil }); err == nil || !strings.Contains(err.Error(), "malformed SSE payload") {
			t.Fatalf("stream error = %v", err)
		}
		if value, exists := body["custom_null"]; !exists || value != nil {
			t.Fatalf("wire null field = %#v, exists=%v", value, exists)
		}
		if accepted.Load() != 1 || compactHeader != "1" {
			t.Fatalf("accept/header = %d/%q", accepted.Load(), compactHeader)
		}
	})

	t.Run("accept failure is request-extension error", func(t *testing.T) {
		acceptFailure := errors.New("accept failed")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		}))
		defer server.Close()
		engine := &Engine{deepSeekExtensions: newDeepSeekLlmAPIExtensionRegistry()}
		_, _ = engine.RegisterDeepSeekLlmAPIExtension("custom", func(context.Context, DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
			return DeepSeekLlmAPIExtensionContribution{Present: true, Value: true, Accept: func() error { return acceptFailure }}, nil
		})
		provider := NewOpenAIProvider("deepseek-official", server.URL, "key", "model")
		provider.deepSeekExtensions = engine
		_, err := provider.Complete(t.Context(), ChatRequest{}, func(Delta) error { return nil })
		var providerErr *ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "REQUEST_EXTENSION" || !errors.Is(err, acceptFailure) {
			t.Fatalf("acceptance error = %v", err)
		}
	})
}
