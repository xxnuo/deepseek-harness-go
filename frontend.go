package harness

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// BootEntry mirrors the upstream client module manifest consumed by the
// original web shell. Keep this wire shape stable; the TS shell rejects a
// missing rev/id/url/rev field before it renders the application.
type BootEntry struct {
	ID          string   `json:"id"`
	URL         string   `json:"url"`
	Rev         string   `json:"rev"`
	Inject      []string `json:"inject,omitempty"`
	Immediately bool     `json:"immediately,omitempty"`
	External    []string `json:"external,omitempty"`
}

type BootGraph struct {
	Rev     string      `json:"rev"`
	Entries []BootEntry `json:"entries"`
}

type clientPackageManifest struct {
	Name    string                     `json:"name"`
	Exports map[string]json.RawMessage `json:"exports"`
	Dsh     struct {
		Client struct {
			Platform    string   `json:"platform"`
			Inject      []string `json:"inject"`
			Immediately bool     `json:"immediately"`
			External    []string `json:"external"`
		} `json:"client"`
	} `json:"dsh"`
}

// clientExport resolves the package export used by the upstream client build.
// Packages currently use an object with a "default" condition, while custom
// packages may expose the target as a plain string. Restrict the target to the
// package directory so an untrusted package.json cannot escape PluginDir.
func clientExport(pkg clientPackageManifest, packageDir string) (string, bool, error) {
	raw, ok := pkg.Exports["./client"]
	if !ok {
		return "", false, nil
	}
	var direct string
	if err := json.Unmarshal(raw, &direct); err == nil && direct != "" {
		return resolveClientTarget(packageDir, direct)
	}
	var conditional struct {
		Default string `json:"default"`
	}
	if err := json.Unmarshal(raw, &conditional); err != nil || conditional.Default == "" {
		return "", false, errors.New("exports[\"./client\"] must be a string or an object with a default string")
	}
	return resolveClientTarget(packageDir, conditional.Default)
}

func resolveClientTarget(packageDir, target string) (string, bool, error) {
	if filepath.IsAbs(target) {
		return "", false, errors.New("client export must be relative")
	}
	clean := filepath.Clean(target)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", false, errors.New("client export escapes package directory")
	}
	return filepath.Join(packageDir, clean), true, nil
}

// frontendPluginRoots resolves the checked-out TS package trees from the web
// dist directory. Explicit roots let CLI profiles combine embedded packages
// with profile-local client plugins.
func (e *Engine) frontendPluginRoots() []string {
	var roots []string
	add := func(root string) {
		if root == "" {
			return
		}
		for _, existing := range roots {
			if existing == root {
				return
			}
		}
		roots = append(roots, root)
	}
	for _, root := range e.cfg.PluginDirs {
		add(root)
	}
	add(e.cfg.PluginDir)
	if len(roots) > 0 {
		return roots
	}
	root := e.frontendRoot()
	if root != "" {
		if abs, err := filepath.Abs(root); err == nil {
			// apps/web/dist -> repository root/packages
			candidate := filepath.Join(abs, "..", "..", "..", "packages")
			if st, err := os.Stat(candidate); err == nil && st.IsDir() {
				return []string{candidate}
			}
		}
	}
	for _, candidate := range []string{"deepseek-harness/packages", "packages"} {
		if st, err := os.Stat(candidate); err == nil && st.IsDir() {
			return []string{candidate}
		}
	}
	return nil
}

func (e *Engine) clientPluginActive(name string) bool {
	if e.cfg.ClientPlugins == nil {
		return true
	}
	for _, active := range e.cfg.ClientPlugins {
		if active == name {
			return true
		}
	}
	return false
}

func (e *Engine) clientPluginComposed(name string) bool {
	return e.cfg.ClientPlugins != nil && e.clientPluginActive(name)
}

// buildBootGraph derives the client roster from the upstream package.json
// dsh.client declarations and built lib/client.js artifacts. The browse picker
// is intentionally omitted because the web profile composes the native picker.
func (e *Engine) buildBootGraph() (BootGraph, map[string]string, error) {
	roots := e.frontendPluginRoots()
	if len(roots) == 0 {
		return BootGraph{Rev: shortRevision("[]"), Entries: []BootEntry{}}, map[string]string{}, nil
	}
	var allowed map[string]struct{}
	if e.cfg.ClientPlugins != nil {
		allowed = make(map[string]struct{}, len(e.cfg.ClientPlugins))
		for _, name := range e.cfg.ClientPlugins {
			allowed[name] = struct{}{}
		}
	}
	entries := make([]BootEntry, 0)
	paths := make(map[string]string)
	for _, root := range roots {
		if stat, err := os.Stat(root); err != nil || !stat.IsDir() {
			continue
		}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				if path != root && (d.Name() == "node_modules" || d.Name() == ".git" || d.Name() == "dist") {
					return fs.SkipDir
				}
				return nil
			}
			if d.Name() != "package.json" {
				return nil
			}
			var pkg clientPackageManifest
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &pkg); err != nil || pkg.Name == "" || pkg.Dsh.Client.Platform != "web" {
				return nil
			}
			if allowed != nil {
				if _, ok := allowed[pkg.Name]; !ok {
					return nil
				}
			} else if strings.HasSuffix(pkg.Name, "ui-directory-picker-browse") {
				return nil
			}
			if _, exists := paths[pkg.Name]; exists {
				return nil
			}
			bundle, ok, err := clientExport(pkg, filepath.Dir(path))
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if _, err := os.Stat(bundle); err != nil {
				return nil
			}
			content, err := os.ReadFile(bundle)
			if err != nil {
				return err
			}
			rev := shortRevisionBytes(content)
			inject := append([]string(nil), pkg.Dsh.Client.Inject...)
			external := append([]string(nil), pkg.Dsh.Client.External...)
			entries = append(entries, BootEntry{ID: pkg.Name, URL: "/plugins/" + pkg.Name + "/client.js?rev=" + rev, Rev: rev, Inject: inject, Immediately: pkg.Dsh.Client.Immediately, External: external})
			paths[pkg.Name] = bundle
			return nil
		})
		if err != nil {
			return BootGraph{}, nil, err
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	entries, err := orderBootEntries(entries)
	if err != nil {
		return BootGraph{}, nil, err
	}
	if len(entries) == 0 {
		return BootGraph{Rev: shortRevision("[]"), Entries: entries}, paths, nil
	}
	encoded, _ := json.Marshal(entries)
	return BootGraph{Rev: shortRevisionBytes(encoded), Entries: entries}, paths, nil
}

func orderBootEntries(entries []BootEntry) ([]BootEntry, error) {
	byID := make(map[string]int, len(entries))
	for index := range entries {
		byID[entries[index].ID] = index
	}
	ordered := make([]BootEntry, 0, len(entries))
	state := make([]uint8, len(entries))
	open := make([]string, 0, len(entries))
	var visit func(int) error
	visit = func(index int) error {
		switch state[index] {
		case 2:
			return nil
		case 1:
			start := 0
			for start < len(open) && open[start] != entries[index].ID {
				start++
			}
			cycle := append(append([]string(nil), open[start:]...), entries[index].ID)
			return errors.New("client-modules: module graph cycle " + strings.Join(cycle, " -> ") + " — a requested package row must precede its consumers, and factory-form CJS cannot deliver partial exports")
		}
		state[index] = 1
		open = append(open, entries[index].ID)
		for _, request := range entries[index].External {
			dependencyID := strings.TrimSuffix(request, "/client")
			dependency, ok := byID[request]
			if !ok {
				dependency, ok = byID[dependencyID]
			}
			if !ok {
				continue
			}
			if dependency == index {
				return errors.New("client-modules: \"" + entries[index].ID + "\" requests module \"" + request + "\" that it answers itself — a row must not declare its own package in dsh.client.external")
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		open = open[:len(open)-1]
		state[index] = 2
		ordered = append(ordered, entries[index])
		return nil
	}
	for index := range entries {
		if err := visit(index); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

func shortRevision(value string) string { return shortRevisionBytes([]byte(value)) }

func shortRevisionBytes(value []byte) string {
	sum := sha1.Sum(value)
	return hex.EncodeToString(sum[:])[:12]
}

func findPluginPath(e *Engine, id string) (string, bool) {
	_, paths, err := e.buildBootGraph()
	if err != nil {
		return "", false
	}
	path, ok := paths[id]
	return path, ok
}

var errPluginNotFound = errors.New("plugin bundle not found")
