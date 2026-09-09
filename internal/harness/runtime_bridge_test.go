package harness

import (
	"os"
	"path/filepath"
)

func init() {
	ConfigureRuntime("0.1.3-alpha.2", func() (AssetPaths, error) {
		root := filepath.Join(os.TempDir(), "deepseek-harness-go-test-assets")
		return MaterializeEmbeddedAssets(root)
	})
}
