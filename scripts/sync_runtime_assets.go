package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

type asset struct {
	source   string
	relative string
}

const assetDestination = "runtime-assets/deepseek-harness"
const testAssetDestination = "testdata/upstream"

var testAssetPaths = []string{
	"examples/jsonrpc-agent/tests/snapshots/text-turn/session.jsonl",
	"examples/jsonrpc-agent/tests/snapshots/bash-tool/session.jsonl",
	"examples/headless-agent/tests/snapshots/compaction-recovery/session.jsonl",
}

func main() {
	check := flag.Bool("check", false, "verify that the tracked runtime assets match upstream")
	upstream := flag.String("upstream", "deepseek-harness", "upstream checkout")
	flag.Parse()

	assets, err := collectRuntimeAssets(*upstream)
	if err == nil {
		testAssets, testErr := collectFiles(*upstream, testAssetPaths)
		if testErr != nil {
			err = testErr
		} else if *check {
			err = verify(assetDestination, assets)
			if err == nil {
				err = verify(testAssetDestination, testAssets)
			}
		} else {
			err = syncAssets(assetDestination, assets)
			if err == nil {
				err = syncAssets(testAssetDestination, testAssets)
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func collectRuntimeAssets(upstream string) ([]asset, error) {
	upstream, err := filepath.Abs(upstream)
	if err != nil {
		return nil, err
	}
	var assets []asset
	addTree := func(relative string) error {
		root := filepath.Join(upstream, filepath.FromSlash(relative))
		return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("runtime asset must not be a symlink: %s", path)
			}
			rel, err := filepath.Rel(upstream, path)
			if err != nil {
				return err
			}
			assets = append(assets, asset{source: path, relative: rel})
			return nil
		})
	}
	for _, tree := range []string{
		"apps/web/dist",
		"apps/cli/config/agent-presets",
		"packages/skill/skill-badge/assets",
	} {
		if err := addTree(tree); err != nil {
			return nil, err
		}
	}
	for _, pattern := range []string{
		"packages/*/*/package.json",
		"packages/*/*/lib/client.js",
		"packages/*/*/lib/client.js.map",
		"packages/bundle/*/cordis.patch.yml",
	} {
		matches, err := filepath.Glob(filepath.Join(upstream, filepath.FromSlash(pattern)))
		if err != nil {
			return nil, err
		}
		for _, path := range matches {
			rel, err := filepath.Rel(upstream, path)
			if err != nil {
				return nil, err
			}
			assets = append(assets, asset{source: path, relative: rel})
		}
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i].relative < assets[j].relative })
	return assets, nil
}

func collectFiles(upstream string, paths []string) ([]asset, error) {
	upstream, err := filepath.Abs(upstream)
	if err != nil {
		return nil, err
	}
	assets := make([]asset, 0, len(paths))
	for _, relative := range paths {
		path := filepath.Join(upstream, filepath.FromSlash(relative))
		info, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("upstream asset is not a regular file: %s", path)
		}
		assets = append(assets, asset{source: path, relative: filepath.FromSlash(relative)})
	}
	return assets, nil
}

func syncAssets(destination string, assets []asset) error {
	clean := filepath.Clean(destination)
	if clean == "." || clean == string(filepath.Separator) {
		return errors.New("refusing to replace an unsafe asset destination")
	}
	if err := os.RemoveAll(clean); err != nil {
		return err
	}
	for _, asset := range assets {
		data, err := os.ReadFile(asset.source)
		if err != nil {
			return err
		}
		path := filepath.Join(clean, asset.relative)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func verify(destination string, assets []asset) error {
	expected := make(map[string]string, len(assets))
	for _, asset := range assets {
		expected[filepath.Clean(asset.relative)] = asset.source
	}
	actual := map[string]struct{}{}
	err := filepath.WalkDir(destination, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(destination, path)
		if err != nil {
			return err
		}
		actual[rel] = struct{}{}
		source, ok := expected[rel]
		if !ok {
			return fmt.Errorf("unexpected tracked runtime asset: %s", filepath.ToSlash(rel))
		}
		want, err := os.ReadFile(source)
		if err != nil {
			return err
		}
		got, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("stale tracked runtime asset: %s", filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return err
	}
	for rel := range expected {
		if _, ok := actual[rel]; !ok {
			return fmt.Errorf("missing tracked runtime asset: %s", filepath.ToSlash(rel))
		}
	}
	return nil
}
