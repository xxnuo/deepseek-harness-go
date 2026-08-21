package harness

import core "github.com/xxnuo/deepseek-harness-go/internal/harness"

func init() {
	core.ConfigureRuntime(Version(), defaultAssetPaths)
}
