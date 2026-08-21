package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStaticOriginalFrontendIndexAssetsAndMissingPaths(t *testing.T) {
	e := newIntegrationEngine(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("<html><head></head><body>original UI</body></html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "app.js"), []byte("console.log('original')"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.cfg.FrontendDir = root
	server := httptest.NewServer(e.Handler())
	defer server.Close()
	response, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body := readAll(t, response)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.Contains(body, "original UI") || !strings.Contains(body, `globalThis["__DSH_BOOT__"]`) {
		t.Fatalf("index = %d %q", response.StatusCode, body)
	}
	asset, err := server.Client().Get(server.URL + "/app.js")
	if err != nil {
		t.Fatal(err)
	}
	defer asset.Body.Close()
	if got := readAll(t, asset); !strings.Contains(got, "original") {
		t.Fatalf("asset = %q", got)
	}
	missing, err := server.Client().Get(server.URL + "/nested/route")
	if err != nil {
		t.Fatal(err)
	}
	defer missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("missing path = %d, want 404", missing.StatusCode)
	}
}

func TestIndexHTMLGetsBootGraphAndCurrentTheme(t *testing.T) {
	e := newIntegrationEngine(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("<html><head></head><body>original UI</body></html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.cfg.FrontendDir = root
	e.cfg.ClientPlugins = []string{"@deepseek-ai/dsh-client-ui-theme"}
	if _, rpcErr := e.settingsUpdate("ui-theme", map[string]any{"preference": "dark"}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	response, err := server.Client().Get(server.URL + "/index.html")
	if err != nil {
		t.Fatal(err)
	}
	body := readAll(t, response)
	_ = response.Body.Close()
	for _, want := range []string{"window.__ModuleLoader__", `globalThis["__DSH_BOOT__"]`, `const preference = "dark"`, `<body><script>`} {
		if !strings.Contains(body, want) {
			t.Fatalf("transformed index missing %q: %s", want, body)
		}
	}
}

func TestBootGraphOrdersExternalDependenciesAndRejectsCycles(t *testing.T) {
	entries := []BootEntry{
		{ID: "@example/a", External: []string{"@example/c/client"}},
		{ID: "@example/b"},
		{ID: "@example/c", External: []string{"react"}},
	}
	ordered, err := orderBootEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{ordered[0].ID, ordered[1].ID, ordered[2].ID}; strings.Join(got, ",") != "@example/c,@example/a,@example/b" {
		t.Fatalf("ordered graph = %v", got)
	}
	if _, err := orderBootEntries([]BootEntry{{ID: "@example/self", External: []string{"@example/self/client"}}}); err == nil || !strings.Contains(err.Error(), "answers itself") {
		t.Fatalf("self dependency error = %v", err)
	}
	if _, err := orderBootEntries([]BootEntry{{ID: "@example/a", External: []string{"@example/b"}}, {ID: "@example/b", External: []string{"@example/a"}}}); err == nil || !strings.Contains(err.Error(), "@example/a -> @example/b -> @example/a") {
		t.Fatalf("cycle error = %v", err)
	}
}

func readAll(t *testing.T, r *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestPluginExportResolution(t *testing.T) {
	e := newIntegrationEngine(t)
	root := t.TempDir()
	pkgDir := filepath.Join(root, "client", "example")
	if err := os.MkdirAll(filepath.Join(pkgDir, "dist"), 0o755); err != nil {
		t.Fatal(err)
	}
	pkg := map[string]any{
		"name":    "@example/client",
		"exports": map[string]any{"./client": map[string]string{"default": "./dist/custom.js"}},
		"dsh":     map[string]any{"client": map[string]any{"platform": "web", "inject": []string{"dep"}}},
	}
	data, _ := json.Marshal(pkg)
	if err := os.WriteFile(filepath.Join(pkgDir, "package.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "dist", "custom.js"), []byte("export const ok = true"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "dist", "custom.js.map"), []byte(`{"version":3}`), 0o600); err != nil {
		t.Fatal(err)
	}
	e.cfg.PluginDir = root
	graph, paths, err := e.buildBootGraph()
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Entries) != 1 || graph.Entries[0].ID != "@example/client" {
		t.Fatalf("graph entries = %#v", graph.Entries)
	}
	if graph.Entries[0].Inject == nil || graph.Entries[0].Inject[0] != "dep" {
		t.Fatalf("inject = %#v", graph.Entries[0].Inject)
	}
	if paths["@example/client"] != filepath.Join(pkgDir, "dist", "custom.js") {
		t.Fatalf("resolved path = %q", paths["@example/client"])
	}
}

func TestBootGraphHonorsActiveClientRoster(t *testing.T) {
	e := newIntegrationEngine(t)
	root := filepath.Join(t.TempDir(), "node_modules")
	for _, name := range []string{"one", "two"} {
		packageDir := filepath.Join(root, "@example", name)
		if err := os.MkdirAll(filepath.Join(packageDir, "lib"), 0o755); err != nil {
			t.Fatal(err)
		}
		manifest, _ := json.Marshal(map[string]any{
			"name":    "@example/" + name,
			"exports": map[string]any{"./client": map[string]string{"default": "./lib/client.js"}},
			"dsh":     map[string]any{"client": map[string]any{"platform": "web"}},
		})
		if err := os.WriteFile(filepath.Join(packageDir, "package.json"), manifest, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(packageDir, "lib", "client.js"), []byte("export {}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	e.cfg.PluginDir = ""
	e.cfg.PluginDirs = []string{root}
	e.cfg.ClientPlugins = []string{"@example/two"}
	graph, paths, err := e.buildBootGraph()
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Entries) != 1 || graph.Entries[0].ID != "@example/two" || len(paths) != 1 {
		t.Fatalf("active boot graph = %#v, paths = %#v", graph.Entries, paths)
	}
}

func TestOriginalFrontendBootsEveryPluginAsset(t *testing.T) {
	e := newIntegrationEngine(t)
	assets, err := defaultAssetPaths()
	if err != nil {
		t.Fatal(err)
	}
	e.cfg.FrontendDir = assets.FrontendDir
	e.cfg.PluginDir = assets.PluginDir
	e.cfg.PresetDir = assets.PresetDir
	graph, _, err := e.buildBootGraph()
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Entries) != 42 {
		t.Fatalf("boot graph has %d entries, want 42", len(graph.Entries))
	}
	if presets := scanPresets(e); len(presets) != 4 {
		t.Fatalf("embedded preset roster has %d entries, want 4", len(presets))
	}
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	index, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if index.StatusCode != http.StatusOK {
		_ = index.Body.Close()
		t.Fatalf("index status = %d", index.StatusCode)
	}
	html := readAll(t, index)
	_ = index.Body.Close()
	if !strings.Contains(html, `globalThis["__DSH_BOOT__"] = {"rev":"`+graph.Rev+`"`) {
		t.Fatal("index does not contain the current boot graph")
	}
	modules := strings.Index(html, `<script src="/plugins/@deepseek-ai/dsh-client-modules/client.js?rev=`)
	runtime := strings.Index(html, `<script src="/plugins/@deepseek-ai/dsh-client-runtime/client.js?rev=`)
	boot := strings.Index(html, `globalThis["__DSH_BOOT__"] = `)
	if queue := strings.Index(html, "window.__ModuleLoader__"); queue < 0 || modules < queue || runtime < modules || boot < runtime {
		t.Fatalf("bootstrap ordering queue=%d modules=%d runtime=%d boot=%d", queue, modules, runtime, boot)
	}

	for _, entry := range graph.Entries {
		for _, suffix := range []string{"", ".map"} {
			url := server.URL + strings.SplitN(entry.URL, "?", 2)[0] + suffix
			resp, err := server.Client().Get(url)
			if err != nil {
				t.Fatalf("GET %s: %v", entry.ID+suffix, err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s status = %d", entry.ID+suffix, resp.StatusCode)
			}
		}
	}
}

func TestNewUsesEmbeddedRuntimeAssetsByDefault(t *testing.T) {
	e := newIntegrationEngine(t)
	for name, path := range map[string]string{
		"frontend": e.cfg.FrontendDir,
		"plugins":  e.cfg.PluginDir,
		"presets":  e.cfg.PresetDir,
	} {
		if stat, err := os.Stat(path); err != nil || !stat.IsDir() {
			t.Fatalf("default %s directory = %q, %v", name, path, err)
		}
	}
	if presets := scanPresets(e); len(presets) != 4 {
		t.Fatalf("default embedded preset roster has %d entries, want 4", len(presets))
	}
	graph, _, err := e.buildBootGraph()
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Entries) != 42 {
		t.Fatalf("default embedded boot graph has %d entries, want 42", len(graph.Entries))
	}
}

func TestPluginEventsPublishesBundleRebuild(t *testing.T) {
	e := newIntegrationEngine(t)
	root := t.TempDir()
	packageDir := filepath.Join(root, "client", "hmr-test")
	if err := os.MkdirAll(filepath.Join(packageDir, "lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest, _ := json.Marshal(map[string]any{
		"name":    "@example/hmr-test",
		"exports": map[string]any{"./client": map[string]string{"default": "./lib/client.js"}},
		"dsh":     map[string]any{"client": map[string]any{"platform": "web"}},
	})
	if err := os.WriteFile(filepath.Join(packageDir, "package.json"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(packageDir, "lib", "client.js")
	if err := os.WriteFile(bundle, []byte("export const value = 1"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.cfg.PluginDir = root
	e.cfg.ClientPlugins = []string{"@example/hmr-test"}
	e.cfg.ClientHMRPollInterval = 10 * time.Millisecond
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/plugins/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	readFrame := func(want string) map[string]any {
		t.Helper()
		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil {
				t.Fatalf("read %s frame: %v", want, readErr)
			}
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var frame map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data: "))), &frame); err != nil {
				t.Fatal(err)
			}
			if frame["type"] == want {
				return frame
			}
		}
	}
	readFrame("graph")
	changed := []byte("export const value = 200")
	if err := os.WriteFile(bundle, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	frame := readFrame("rebuilt")
	if frame["id"] != "@example/hmr-test" || frame["rev"] != shortRevisionBytes(changed) {
		t.Fatalf("rebuilt frame = %#v", frame)
	}
}
