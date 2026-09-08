package harness

import (
	"os"
	"path/filepath"
)

func init() {
	ConfigureRuntime("0.1.2-alpha.5", func() (AssetPaths, error) {
		root := filepath.Join(os.TempDir(), "deepseek-harness-go-test-assets")
		return MaterializeEmbeddedAssets(root)
	})
}
