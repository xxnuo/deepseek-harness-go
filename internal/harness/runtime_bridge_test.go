package harness

import (
	"path/filepath"
	"runtime"
)

func init() {
	_, file, _, _ := runtime.Caller(0)
	repository := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	upstream := filepath.Join(repository, "runtime-assets", "deepseek-harness")
	ConfigureRuntime("0.1.1-rc.2", func() (AssetPaths, error) {
		return AssetPaths{
			UpstreamDir: upstream,
			FrontendDir: filepath.Join(upstream, "apps", "web", "dist"),
			PluginDir:   filepath.Join(upstream, "packages"),
			PresetDir:   filepath.Join(upstream, "apps", "cli", "config", "agent-presets"),
		}, nil
	})
}
