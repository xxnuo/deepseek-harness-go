package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
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

func writeBootPluginFixture(t *testing.T, root, name, body string) string {
	t.Helper()
	packageDir := filepath.Join(root, strings.TrimPrefix(name, "@"))
	if err := os.MkdirAll(filepath.Join(packageDir, "lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest, err := json.Marshal(map[string]any{
		"name":    name,
		"exports": map[string]any{"./client": map[string]string{"default": "./lib/client.js"}},
		"dsh":     map[string]any{"client": map[string]any{"platform": "web"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packageDir, "package.json"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(packageDir, "lib", "client.js")
	if err := os.WriteFile(bundle, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestBootGraphUsesOpaqueStartupRevisionsAndPreReadBaselines(t *testing.T) {
	e := newIntegrationEngine(t)
	root := filepath.Join(t.TempDir(), "node_modules")
	names := []string{"@fixture/startup-revision-first", "@fixture/startup-revision-second"}
	paths := make(map[string]string, len(names))
	for _, name := range names {
		paths[name] = writeBootPluginFixture(t, root, name, "module.exports = {}\n")
	}
	e.cfg.PluginDir = ""
	e.cfg.PluginDirs = []string{root}
	e.cfg.ClientPlugins = names
	snapshot, err := e.buildBootSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.graph.Entries) != len(names) {
		t.Fatalf("startup entries = %#v", snapshot.graph.Entries)
	}
	pattern := regexp.MustCompile(`^([a-f\d]{16})-(\d+)$`)
	var nonce string
	for index, entry := range snapshot.graph.Entries {
		match := pattern.FindStringSubmatch(entry.Rev)
		if match == nil || match[2] != fmt.Sprint(index) {
			t.Fatalf("startup revision %q at index %d", entry.Rev, index)
		}
		if index == 0 {
			nonce = match[1]
		} else if match[1] != nonce {
			t.Fatalf("startup nonce = %q, want %q", match[1], nonce)
		}
		stat, err := os.Stat(paths[entry.ID])
		if err != nil {
			t.Fatal(err)
		}
		baseline := snapshot.baselines[entry.ID]
		if baseline.path != paths[entry.ID] || baseline.mtimeNS != stat.ModTime().UnixNano() || baseline.size != stat.Size() {
			t.Fatalf("baseline for %s = %#v, stat = %#v", entry.ID, baseline, stat)
		}
		content, err := os.ReadFile(paths[entry.ID])
		if err != nil {
			t.Fatal(err)
		}
		if entry.Rev == clientArtifactRevision(content, nil) {
			t.Fatalf("startup revision for %s was derived from artifact bytes", entry.ID)
		}
	}
	repeated, err := e.buildBootSnapshot()
	if err != nil || repeated.graph.Rev != snapshot.graph.Rev || repeated.graph.Entries[0].Rev != snapshot.graph.Entries[0].Rev {
		t.Fatalf("repeated startup snapshot = %#v, %v", repeated.graph, err)
	}
}

func TestBootGraphRetainsOnePreviousBatchGeneration(t *testing.T) {
	e := newIntegrationEngine(t)
	root := filepath.Join(t.TempDir(), "node_modules")
	const name = "@fixture/batch-rebuild-race"
	bundle := writeBootPluginFixture(t, root, name, "module.exports = { generation: 1 }\n")
	e.cfg.PluginDir = ""
	e.cfg.PluginDirs = []string{root}
	e.cfg.ClientPlugins = []string{name}
	firstSnapshot, err := e.buildBootSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	first := firstSnapshot.graph.Batches[0].URL
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	get := func(resource string) (int, string) {
		t.Helper()
		response, err := server.Client().Get(server.URL + resource)
		if err != nil {
			t.Fatal(err)
		}
		body := readAll(t, response)
		_ = response.Body.Close()
		return response.StatusCode, body
	}

	secondBody := "module.exports = { generation: 200 }\n"
	if err := os.WriteFile(bundle, []byte(secondBody), 0o600); err != nil {
		t.Fatal(err)
	}
	secondRev, changed, err := e.rebuildBootArtifact(name)
	if err != nil || !changed || secondRev != clientArtifactRevision([]byte(secondBody), nil) {
		t.Fatalf("second rebuild = rev %q changed %v err %v", secondRev, changed, err)
	}
	secondSnapshot, err := e.buildBootSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	second := secondSnapshot.graph.Batches[0].URL
	if first == second {
		t.Fatal("second generation reused first batch URL")
	}
	if status, body := get(first); status != http.StatusOK || !strings.Contains(body, "generation: 1") {
		t.Fatalf("first retained batch = %d %q", status, body)
	}
	if status, body := get(second); status != http.StatusOK || !strings.Contains(body, "generation: 200") {
		t.Fatalf("second batch = %d %q", status, body)
	}

	thirdBody := "module.exports = { generation: 3 }\n"
	if err := os.WriteFile(bundle, []byte(thirdBody), 0o600); err != nil {
		t.Fatal(err)
	}
	thirdRev, changed, err := e.rebuildBootArtifact(name)
	if err != nil || !changed || thirdRev != clientArtifactRevision([]byte(thirdBody), nil) {
		t.Fatalf("third rebuild = rev %q changed %v err %v", thirdRev, changed, err)
	}
	thirdSnapshot, err := e.buildBootSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	third := thirdSnapshot.graph.Batches[0].URL
	if status, _ := get(first); status != http.StatusNotFound {
		t.Fatalf("first generation status after third = %d", status)
	}
	if status, body := get(second); status != http.StatusOK || !strings.Contains(body, "generation: 200") {
		t.Fatalf("second retained batch after third = %d %q", status, body)
	}
	if status, body := get(third); status != http.StatusOK || !strings.Contains(body, "generation: 3") {
		t.Fatalf("third batch = %d %q", status, body)
	}
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
	if len(graph.Entries) != 49 {
		t.Fatalf("boot graph has %d entries, want 49", len(graph.Entries))
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
	modules := strings.Index(html, `<script src="/plugins/??@deepseek-ai/dsh-client-modules/client.js&amp;rev=`)
	runtime := strings.Index(html, `<script src="/plugins/@deepseek-ai/dsh-client-runtime/client.js`)
	preload := strings.Index(html, `<link rel="preload" as="script" href="/plugins/??`)
	boot := strings.Index(html, `globalThis["__DSH_BOOT__"] = `)
	if queue := strings.Index(html, "window.__ModuleLoader__"); queue < 0 || preload < queue || modules < preload || boot < modules || runtime >= 0 {
		t.Fatalf("bootstrap ordering queue=%d preload=%d modules=%d boot=%d runtime=%d", queue, preload, modules, boot, runtime)
	}

	seen := map[string]int{}
	for _, batch := range graph.Batches {
		seenBatch := 0
		for _, id := range batch.Entries {
			seen[id]++
			seenBatch++
		}
		mapURL := comboURL(batch.Entries, batch.Rev, true)
		if seenBatch == 0 || len([]byte(batch.URL)) > maxComboURLBytes || len([]byte(mapURL)) > maxComboURLBytes {
			t.Fatalf("invalid batch %#v", batch)
		}
		for _, resource := range []string{batch.URL, mapURL} {
			resp, err := server.Client().Get(server.URL + resource)
			if err != nil {
				t.Fatalf("GET %s: %v", resource, err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s status = %d", resource, resp.StatusCode)
			}
			if resp.Header.Get("Cache-Control") != "public, max-age=31536000, immutable" {
				t.Fatalf("GET %s cache = %q", resource, resp.Header.Get("Cache-Control"))
			}
		}
	}
	for _, entry := range graph.Entries {
		if seen[entry.ID] != 1 {
			t.Fatalf("entry %q assigned %d batches", entry.ID, seen[entry.ID])
		}
		for _, resource := range []string{entry.URL, comboURL([]string{entry.ID}, entry.Rev, true)} {
			resp, err := server.Client().Get(server.URL + resource)
			if err != nil {
				t.Fatalf("GET %s: %v", resource, err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s status = %d", resource, resp.StatusCode)
			}
		}
	}
}

func TestBootGraphSplitsComboURLsAndServesOnlyAdvertisedResources(t *testing.T) {
	e := newIntegrationEngine(t)
	root := filepath.Join(t.TempDir(), "node_modules")
	packageNames := make([]string, 48)
	for index := range packageNames {
		name := fmt.Sprintf("@fixture/combo-url-%03d-%s", index, strings.Repeat("x", 40))
		packageNames[index] = name
		packageDir := filepath.Join(root, "@fixture", strings.TrimPrefix(name, "@fixture/"))
		if err := os.MkdirAll(filepath.Join(packageDir, "lib"), 0o755); err != nil {
			t.Fatal(err)
		}
		manifest, _ := json.Marshal(map[string]any{
			"name":    name,
			"exports": map[string]any{"./client": map[string]string{"default": "./lib/client.js"}},
			"dsh":     map[string]any{"client": map[string]any{"platform": "web"}},
		})
		if err := os.WriteFile(filepath.Join(packageDir, "package.json"), manifest, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(packageDir, "lib", "client.js"), []byte("window.__combo = true\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	e.cfg.PluginDir = ""
	e.cfg.PluginDirs = []string{root}
	e.cfg.ClientPlugins = packageNames
	graph, _, err := e.buildBootGraph()
	if err != nil {
		t.Fatal(err)
	}
	var batches []BootBatch
	for _, batch := range graph.Batches {
		if batch.Phase == BootBatchApplication {
			batches = append(batches, batch)
		}
	}
	if len(batches) < 2 {
		t.Fatalf("application batches = %d, want split", len(batches))
	}
	var assigned []string
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	for index, batch := range batches {
		assigned = append(assigned, batch.Entries...)
		mapURL := comboURL(batch.Entries, batch.Rev, true)
		if len([]byte(batch.URL)) > maxComboURLBytes || len([]byte(mapURL)) > maxComboURLBytes {
			t.Fatalf("batch %d exceeds URL limit: %d/%d", index, len([]byte(batch.URL)), len([]byte(mapURL)))
		}
		for _, resource := range []string{batch.URL, mapURL} {
			response, err := server.Client().Get(server.URL + resource)
			if err != nil {
				t.Fatal(err)
			}
			body := readAll(t, response)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK || body == "" {
				t.Fatalf("GET %s = %d %q", resource, response.StatusCode, body)
			}
		}
		if index+1 < len(batches) {
			candidate := append(append([]string(nil), batch.Entries...), batches[index+1].Entries[0])
			if len([]byte(comboURL(candidate, comboRevisionPlaceholder, true))) <= maxComboURLBytes {
				t.Fatalf("batch %d was split before the URL limit", index)
			}
		}
	}
	if strings.Join(assigned, "\x00") != strings.Join(packageNames, "\x00") {
		t.Fatalf("batch assignment differs\n got: %q\nwant: %q", assigned, packageNames)
	}
	unknown, err := server.Client().Get(server.URL + comboURL([]string{packageNames[0], packageNames[1]}, "stale", false))
	if err != nil {
		t.Fatal(err)
	}
	_ = unknown.Body.Close()
	if unknown.StatusCode != http.StatusNotFound {
		t.Fatalf("unadvertised combo status = %d", unknown.StatusCode)
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
	if len(graph.Entries) != 49 {
		t.Fatalf("default embedded boot graph has %d entries, want 49", len(graph.Entries))
	}
}

func TestPluginEventsCatchesWriteBetweenStartupSnapshotAndWatchInstall(t *testing.T) {
	e := newIntegrationEngine(t)
	root := filepath.Join(t.TempDir(), "node_modules")
	const name = "@fixture/hmr-startup-window"
	initialBody := "export const value = 1\n"
	bundle := writeBootPluginFixture(t, root, name, initialBody)
	e.cfg.PluginDir = ""
	e.cfg.PluginDirs = []string{root}
	e.cfg.ClientPlugins = []string{name}
	e.cfg.ClientHMRPollInterval = 10 * time.Millisecond
	initial, err := e.buildBootSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	initialBatch := initial.graph.Batches[0].URL
	changed := []byte("export const value = 200\n")
	if err := os.WriteFile(bundle, changed, 0o600); err != nil {
		t.Fatal(err)
	}

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
	var graph BootGraph
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatalf("read startup graph: %v", readErr)
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var frame struct {
			Type  string    `json:"type"`
			Graph BootGraph `json:"graph"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data: "))), &frame); err != nil {
			t.Fatal(err)
		}
		if frame.Type == "graph" {
			graph = frame.Graph
			break
		}
	}
	wantRev := clientArtifactRevision(changed, nil)
	if len(graph.Entries) != 1 || graph.Entries[0].Rev != wantRev || len(graph.Batches) != 1 {
		t.Fatalf("catch-up graph = %#v, want artifact rev %q", graph, wantRev)
	}
	current, err := server.Client().Get(server.URL + graph.Batches[0].URL)
	if err != nil {
		t.Fatal(err)
	}
	currentBody := readAll(t, current)
	_ = current.Body.Close()
	if current.StatusCode != http.StatusOK || !strings.Contains(currentBody, string(changed)) {
		t.Fatalf("catch-up batch = %d %q", current.StatusCode, currentBody)
	}
	previous, err := server.Client().Get(server.URL + initialBatch)
	if err != nil {
		t.Fatal(err)
	}
	previousBody := readAll(t, previous)
	_ = previous.Body.Close()
	if previous.StatusCode != http.StatusOK || !strings.Contains(previousBody, initialBody) {
		t.Fatalf("startup batch retained during catch-up = %d %q", previous.StatusCode, previousBody)
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
	initial := readFrame("graph")
	encodedGraph, _ := json.Marshal(initial["graph"])
	var initialGraph BootGraph
	if err := json.Unmarshal(encodedGraph, &initialGraph); err != nil {
		t.Fatal(err)
	}
	if len(initialGraph.Entries) != 1 || len(initialGraph.Batches) != 1 || initialGraph.Batches[0].Entries[0] != "@example/hmr-test" {
		t.Fatalf("initial HMR graph = %#v", initialGraph)
	}
	changed := []byte("export const value = 200")
	if err := os.WriteFile(bundle, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	frame := readFrame("rebuilt")
	wantRev := clientArtifactRevision(changed, nil)
	if frame["id"] != "@example/hmr-test" || frame["rev"] != wantRev {
		t.Fatalf("rebuilt frame = %#v", frame)
	}
	rebuilt, err := server.Client().Get(server.URL + comboURL([]string{"@example/hmr-test"}, wantRev, false))
	if err != nil {
		t.Fatal(err)
	}
	rebuiltBody := readAll(t, rebuilt)
	_ = rebuilt.Body.Close()
	if rebuilt.StatusCode != http.StatusOK || !strings.Contains(rebuiltBody, string(changed)) {
		t.Fatalf("rebuilt bundle = %d %q", rebuilt.StatusCode, rebuiltBody)
	}
}

func TestPluginEventsPublishesRebuildToEveryConnection(t *testing.T) {
	e := newIntegrationEngine(t)
	root := filepath.Join(t.TempDir(), "node_modules")
	const name = "@fixture/hmr-multiple-connections"
	bundle := writeBootPluginFixture(t, root, name, "export const value = 1\n")
	e.cfg.PluginDir = ""
	e.cfg.PluginDirs = []string{root}
	e.cfg.ClientPlugins = []string{name}
	e.cfg.ClientHMRPollInterval = 10 * time.Millisecond
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	type eventStream struct {
		response *http.Response
		reader   *bufio.Reader
	}
	openStream := func() eventStream {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		t.Cleanup(cancel)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/plugins/events", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = response.Body.Close() })
		return eventStream{response: response, reader: bufio.NewReader(response.Body)}
	}
	readFrame := func(stream eventStream, want string) map[string]any {
		t.Helper()
		for {
			line, err := stream.reader.ReadString('\n')
			if err != nil {
				t.Fatalf("read %s frame: %v", want, err)
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

	first := openStream()
	second := openStream()
	readFrame(first, "graph")
	readFrame(second, "graph")
	changed := []byte("export const value = 200\n")
	if err := os.WriteFile(bundle, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	wantRev := clientArtifactRevision(changed, nil)
	for index, stream := range []eventStream{first, second} {
		frame := readFrame(stream, "rebuilt")
		if frame["id"] != name || frame["rev"] != wantRev {
			t.Fatalf("connection %d rebuilt frame = %#v", index+1, frame)
		}
	}
}
