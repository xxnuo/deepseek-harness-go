package harness

import (
	"os"
	"path/filepath"
)

func init() {
	ConfigureRuntime("0.1.1-rc.2", func() (AssetPaths, error) {
		root := filepath.Join(os.TempDir(), "deepseek-harness-go-test-assets")
		return MaterializeEmbeddedAssets(root)
	})
}
