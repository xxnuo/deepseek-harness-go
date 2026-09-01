package harness

import (
	"os"
	"path/filepath"
	"strings"
	"sync"

	core "github.com/xxnuo/deepseek-harness-go/internal/harness"
)

var defaultAssets struct {
	sync.Once
	paths AssetPaths
	err   error
}

type AssetPaths = core.AssetPaths

func assetPaths(root string) AssetPaths {
	upstream := filepath.Join(root, "deepseek-harness")
	return AssetPaths{UpstreamDir: upstream, FrontendDir: filepath.Join(upstream, "apps", "web", "dist"), PluginDir: filepath.Join(upstream, "packages"), PresetDir: filepath.Join(upstream, "packages", "preset", "agent-presets", "presets")}
}

func MaterializeAssets(dataDir string) (AssetPaths, error) {
	return core.MaterializeEmbeddedAssets(dataDir)
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

func embeddedAssetRevision() (string, error) { return core.EmbeddedAssetRevision() }
