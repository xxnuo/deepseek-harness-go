package harness

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	embeddedAssetOnce sync.Once
	embeddedAssetData []bundleFile
	embeddedAssetErr  error
)

type bundleFile struct {
	name string
	data []byte
}

// MaterializeEmbeddedAssets extracts the embedded upstream checkout into a
// content-addressed cache directory and returns the filesystem paths used by
// the runtime.
func MaterializeEmbeddedAssets(dataDir string) (AssetPaths, error) {
	if strings.TrimSpace(dataDir) == "" {
		return AssetPaths{}, errors.New("asset data directory is required")
	}
	dataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return AssetPaths{}, fmt.Errorf("resolve asset data directory: %w", err)
	}
	files, revision, err := loadEmbeddedAssetBundle()
	if err != nil {
		return AssetPaths{}, err
	}
	root := filepath.Join(dataDir, "runtime-assets", revision)
	paths := assetPaths(root)
	marker := filepath.Join(root, ".complete")
	if assetsReady(paths, marker, revision) {
		return paths, nil
	}
	for _, file := range files {
		destination, err := safeBundlePath(root, file.name)
		if err != nil {
			return AssetPaths{}, err
		}
		if err := writeAsset(destination, file.data); err != nil {
			return AssetPaths{}, fmt.Errorf("materialize embedded assets: %w", err)
		}
	}
	if err := writeAsset(marker, []byte(revision+"\n")); err != nil {
		return AssetPaths{}, fmt.Errorf("complete embedded assets: %w", err)
	}
	return paths, nil
}

func EmbeddedAssetRevision() (string, error) {
	_, revision, err := loadEmbeddedAssetBundle()
	return revision, err
}

func loadEmbeddedAssetBundle() ([]bundleFile, string, error) {
	embeddedAssetOnce.Do(func() {
		bundle := embeddedAssetBundleBytes()
		if len(bundle) == 0 {
			embeddedAssetErr = errors.New("runtime assets were not embedded; use the Makefile build targets")
			return
		}
		decoder, err := zstd.NewReader(bytes.NewReader(bundle))
		if err != nil {
			embeddedAssetErr = fmt.Errorf("open embedded asset bundle: %w", err)
			return
		}
		defer decoder.Close()
		reader := tar.NewReader(decoder)
		var lock []byte
		for {
			header, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				embeddedAssetErr = fmt.Errorf("read embedded asset bundle: %w", err)
				return
			}
			name := filepath.ToSlash(filepath.Clean(header.Name))
			if name == "." || strings.HasPrefix(name, "../") || strings.Contains(name, "/../") || filepath.IsAbs(name) {
				embeddedAssetErr = fmt.Errorf("unsafe embedded asset path: %s", header.Name)
				return
			}
			switch header.Typeflag {
			case tar.TypeDir:
				continue
			case tar.TypeReg, tar.TypeRegA:
				data, readErr := io.ReadAll(reader)
				if readErr != nil {
					embeddedAssetErr = fmt.Errorf("read embedded asset %s: %w", name, readErr)
					return
				}
				if name == "upstream.lock" {
					lock = data
					continue
				}
				if !strings.HasPrefix(name, "deepseek-harness/") {
					embeddedAssetErr = fmt.Errorf("unexpected embedded asset path: %s", name)
					return
				}
				embeddedAssetData = append(embeddedAssetData, bundleFile{name: strings.TrimPrefix(name, "deepseek-harness/"), data: data})
			default:
				embeddedAssetErr = fmt.Errorf("unsupported embedded asset type %d: %s", header.Typeflag, name)
				return
			}
		}
		if len(lock) == 0 {
			embeddedAssetErr = errors.New("embedded upstream lock is missing")
			return
		}
		sort.Slice(embeddedAssetData, func(i, j int) bool { return embeddedAssetData[i].name < embeddedAssetData[j].name })
		commit := ""
		for _, line := range strings.Split(string(lock), "\n") {
			if value := strings.TrimPrefix(line, "commit="); value != line && value != "" {
				commit = value
				break
			}
		}
		if commit == "" {
			embeddedAssetErr = errors.New("embedded upstream commit is missing")
			return
		}
		digest := sha256.New()
		for _, file := range embeddedAssetData {
			_, _ = io.WriteString(digest, "deepseek-harness/"+file.name+"\x00")
			_, _ = digest.Write(file.data)
			_, _ = io.WriteString(digest, "\x00")
		}
		embeddedAssetRevisionValue := commit + "-" + hex.EncodeToString(digest.Sum(nil))[:12]
		embeddedAssetRevisionCache = embeddedAssetRevisionValue
	})
	return embeddedAssetData, embeddedAssetRevisionCache, embeddedAssetErr
}

var embeddedAssetRevisionCache string

func safeBundlePath(root, name string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(name))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe embedded asset path: %s", name)
	}
	return filepath.Join(root, "deepseek-harness", clean), nil
}

func assetPaths(root string) AssetPaths {
	upstream := filepath.Join(root, "deepseek-harness")
	return AssetPaths{UpstreamDir: upstream, FrontendDir: filepath.Join(upstream, "apps", "web", "dist"), PluginDir: filepath.Join(upstream, "packages"), PresetDir: filepath.Join(upstream, "packages", "preset", "agent-presets", "presets")}
}

func assetsReady(paths AssetPaths, marker, revision string) bool {
	data, err := os.ReadFile(marker)
	if err != nil || strings.TrimSpace(string(data)) != revision {
		return false
	}
	for _, path := range []string{filepath.Join(paths.FrontendDir, "index.html"), filepath.Join(paths.PluginDir, "client", "connection", "lib", "client.js"), filepath.Join(paths.PluginDir, "bundle", "base", "cordis.patch.yml"), filepath.Join(paths.PresetDir, "standard", "agent.cordis.yml")} {
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
