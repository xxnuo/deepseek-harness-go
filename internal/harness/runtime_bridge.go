package harness

import (
	"errors"
	"strings"
)

type AssetPaths struct {
	UpstreamDir string
	FrontendDir string
	PluginDir   string
	PresetDir   string
}

var (
	runtimeVersion = "0.0.0"
	assetResolver  = func() (AssetPaths, error) {
		return AssetPaths{}, errors.New("runtime asset resolver is not configured")
	}
)

func ConfigureRuntime(version string, resolver func() (AssetPaths, error)) {
	if strings.TrimSpace(version) != "" {
		runtimeVersion = version
	}
	if resolver != nil {
		assetResolver = resolver
	}
}

func Version() string { return runtimeVersion }

func defaultAssetPaths() (AssetPaths, error) { return assetResolver() }
