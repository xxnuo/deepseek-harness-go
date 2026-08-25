package harness

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootMaterializeAssetsInvalidatesLegacyCommitOnlyCache(t *testing.T) {
	revision, err := embeddedAssetRevision()
	if err != nil {
		t.Fatal(err)
	}
	commit := strings.SplitN(revision, "-", 2)[0]
	dataDir := t.TempDir()
	legacyRoot := filepath.Join(dataDir, "runtime-assets", commit)
	legacy := assetPaths(legacyRoot)
	for _, path := range []string{
		filepath.Join(legacy.FrontendDir, "index.html"),
		filepath.Join(legacy.PluginDir, "client", "runtime", "lib", "client.js"),
		filepath.Join(legacy.PluginDir, "bundle", "base", "cordis.patch.yml"),
		filepath.Join(legacy.PresetDir, "standard", "agent.cordis.yml"),
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(legacyRoot, ".complete"), []byte(commit+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	materialized, err := MaterializeAssets(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if materialized.UpstreamDir == legacy.UpstreamDir {
		t.Fatal("legacy commit-only cache was reused")
	}
	if _, err := os.Stat(filepath.Join(materialized.PluginDir, "client", "ui-brand-official", "lib", "client.js")); err != nil {
		t.Fatalf("current rc.2 client assets were not materialized: %v", err)
	}
}

func TestRootNewUsesEmbeddedRuntimeAssetsByDefault(t *testing.T) {
	e, err := New(
		WithDataDir(t.TempDir()),
		WithWorkspace(t.TempDir()),
		WithProvider("echo"),
		WithModel("echo"),
		WithPersistence(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	config := e.Config()
	for name, path := range map[string]string{
		"frontend": config.FrontendDir,
		"plugins":  config.PluginDir,
		"presets":  config.PresetDir,
	} {
		if stat, err := os.Stat(path); err != nil || !stat.IsDir() {
			t.Fatalf("default %s directory = %q, %v", name, path, err)
		}
	}
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	response, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), `globalThis["__DSH_BOOT__"]`) {
		t.Fatalf("embedded frontend response = %d, %v", response.StatusCode, readErr)
	}
}
