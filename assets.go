package harness

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

//go:embed upstream.lock runtime-assets/deepseek-harness
var embeddedAssets embed.FS

var defaultAssets struct {
	sync.Once
	paths AssetPaths
	err   error
}

type AssetPaths struct {
	UpstreamDir string
	FrontendDir string
	PluginDir   string
	PresetDir   string
}

// MaterializeAssets makes the original UI and its runtime configuration
// available to filesystem-based hosts without requiring an upstream checkout.
func MaterializeAssets(dataDir string) (AssetPaths, error) {
	if strings.TrimSpace(dataDir) == "" {
		return AssetPaths{}, errors.New("asset data directory is required")
	}
	dataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return AssetPaths{}, fmt.Errorf("resolve asset data directory: %w", err)
	}
	revision, err := embeddedAssetRevision()
	if err != nil {
		return AssetPaths{}, err
	}
	root := filepath.Join(dataDir, "runtime-assets", revision)
	paths := assetPaths(root)
	marker := filepath.Join(root, ".complete")
	if assetsReady(paths, marker, revision) {
		return paths, nil
	}
	runtimeAssets, err := fs.Sub(embeddedAssets, "runtime-assets")
	if err != nil {
		return AssetPaths{}, fmt.Errorf("open embedded assets: %w", err)
	}
	if err := fs.WalkDir(runtimeAssets, "deepseek-harness", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		destination := filepath.Join(root, filepath.FromSlash(path))
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		data, err := fs.ReadFile(runtimeAssets, path)
		if err != nil {
			return err
		}
		return writeAsset(destination, data)
	}); err != nil {
		return AssetPaths{}, fmt.Errorf("materialize embedded assets: %w", err)
	}
	if err := writeAsset(marker, []byte(revision+"\n")); err != nil {
		return AssetPaths{}, fmt.Errorf("complete embedded assets: %w", err)
	}
	return paths, nil
}

func defaultAssetPaths() (AssetPaths, error) {
	defaultAssets.Do(func() {
		root, err := os.UserCacheDir()
		if err != nil || strings.TrimSpace(root) == "" {
			root = os.TempDir()
		}
		defaultAssets.paths, defaultAssets.err = MaterializeAssets(filepath.Join(root, "deepseek-harness-go"))
	})
	return defaultAssets.paths, defaultAssets.err
}

func embeddedAssetRevision() (string, error) {
	data, err := embeddedAssets.ReadFile("upstream.lock")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if revision := strings.TrimPrefix(line, "commit="); revision != line && revision != "" {
			return revision, nil
		}
	}
	return "", errors.New("embedded upstream commit is missing")
}

func assetPaths(root string) AssetPaths {
	upstream := filepath.Join(root, "deepseek-harness")
	return AssetPaths{
		UpstreamDir: upstream,
		FrontendDir: filepath.Join(upstream, "apps", "web", "dist"),
		PluginDir:   filepath.Join(upstream, "packages"),
		PresetDir:   filepath.Join(upstream, "apps", "cli", "config", "agent-presets"),
	}
}

func assetsReady(paths AssetPaths, marker, revision string) bool {
	data, err := os.ReadFile(marker)
	if err != nil || strings.TrimSpace(string(data)) != revision {
		return false
	}
	for _, path := range []string{
		filepath.Join(paths.FrontendDir, "index.html"),
		filepath.Join(paths.PluginDir, "client", "runtime", "lib", "client.js"),
		filepath.Join(paths.PluginDir, "bundle", "base", "cordis.patch.yml"),
		filepath.Join(paths.PresetDir, "standard", "agent.cordis.yml"),
	} {
		if stat, err := os.Stat(path); err != nil || stat.IsDir() {
			return false
		}
	}
	return true
}

func writeAsset(path string, data []byte) error {
	if current, err := os.ReadFile(path); err == nil && bytes.Equal(current, data) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".asset-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
