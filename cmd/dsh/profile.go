package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/dop251/goja"
	harness "github.com/xxnuo/deepseek-harness-go"
	"gopkg.in/yaml.v3"
)

const (
	profilesDir      = "profiles"
	profilePatchFile = "cordis.patch.yml"
	profileRootFile  = "cordis.yml"
	baseBundle       = "@deepseek-ai/dsh-base"
	webBundle        = "@deepseek-ai/dsh-web-app"
	headlessBundle   = "@deepseek-ai/dsh-headless"
)

var profileTemplates = map[string][]string{
	"web":      {baseBundle, webBundle},
	"headless": {baseBundle, headlessBundle},
}

const profilePatchTemplate = `# Your patch layer for this dsh profile, applied after every bundle layer:
# a top-level YAML array of loader patch entries.
[]
`

const profileWorkspace = `packages:
  - .

nodeLinker: hoisted
autoInstallPeers: false
`

const profileRoot = `# dsh profile root; edit cordis.patch.yml instead.
[]
`

var packageNamePattern = regexp.MustCompile(`^(?:@[A-Za-z0-9._~-]+/)?[A-Za-z0-9._~-]+$`)

type profileLoader struct {
	home              string
	upstream          string
	bundleDirs        map[string]string
	launchEnvironment *harness.LaunchEnvironmentSnapshot
}

type profile struct {
	name        string
	dir         string
	layers      []patchLayer
	patchPath   string
	patches     []*yaml.Node
	patchExists bool
}

type patchLayer struct {
	label   string
	patches []*yaml.Node
}

type provenance struct {
	origin      string
	originLayer int
	patchedBy   []string
}

type entryRef struct {
	node *yaml.Node
	top  int
}

type composition struct {
	entries                []*yaml.Node
	provenance             []provenance
	index                  map[string]entryRef
	profileDir             string
	e2b                    *harness.E2BConfig
	hooks                  []harness.HookBridgeConfig
	mcpConfigs             []harness.MCPConfig
	lspServers             map[string]harness.LSPStdioConfig
	lspTool                harness.LSPToolConfig
	terminalConfig         harness.TerminalConfig
	terminalToolConfig     harness.TerminalToolConfig
	persist                bool
	sessionStore           harness.SessionStore
	sessionTitleLLM        *harness.SessionTitleLLMConfig
	sessionTelemetry       *harness.SessionTelemetryConfig
	subagentProviders      []harness.SubagentProvider
	subagentTools          []harness.SubagentToolConfig
	subagentReportDelivery string
	exaSearch              *harness.ExaSearchProviderOptions
	perplexitySearch       *harness.PerplexitySearchProviderOptions
	storage                *harness.StorageRuntimeConfig
	fileReference          *harness.FileReferenceConfig
	agentTeams             *harness.AgentTeamConfig
	clientHMRPollInterval  time.Duration
	httpFetch              bool
	httpFetchConfig        *harness.HTTPWebFetchConfig
	webTools               *harness.WebToolConfig
	webSearchProvider      string
	webSearchSet           bool
	webFetchProvider       string
	webFetchSet            bool
}

type pluginEntry struct {
	id, name string
	node     *yaml.Node
}

var supportedPluginNames = map[string]bool{
	"@deepseek-ai/cordis-plugin-hmr":                       true,
	"@deepseek-ai/cordis-plugin-timer":                     true,
	"@deepseek-ai/dsh-agent":                               true,
	"@deepseek-ai/dsh-agent-default-model":                 true,
	"@deepseek-ai/dsh-agent-instructions":                  true,
	"@deepseek-ai/dsh-agent-loop":                          true,
	"@deepseek-ai/dsh-agent-presets":                       true,
	"@deepseek-ai/dsh-attachment-local":                    true,
	"@deepseek-ai/dsh-api-gateway":                         true,
	"@deepseek-ai/dsh-api-remotes":                         true,
	"@deepseek-ai/dsh-bash-sandbox":                        true,
	"@deepseek-ai/dsh-client-connection":                   true,
	"@deepseek-ai/dsh-client-hmr":                          true,
	"@deepseek-ai/dsh-client-locale":                       true,
	"@deepseek-ai/dsh-client-modules":                      true,
	"@deepseek-ai/dsh-client-runtime":                      true,
	"@deepseek-ai/dsh-client-ui-conversation":              true,
	"@deepseek-ai/dsh-client-ui-deliverables":              true,
	"@deepseek-ai/dsh-client-ui-directory-picker-native":   true,
	"@deepseek-ai/dsh-client-ui-agent-preset":              true,
	"@deepseek-ai/dsh-client-ui-attachment":                true,
	"@deepseek-ai/dsh-client-ui-commands":                  true,
	"@deepseek-ai/dsh-client-ui-brand-official":            true,
	"@deepseek-ai/dsh-client-ui-cordis":                    true,
	"@deepseek-ai/dsh-client-ui-goal":                      true,
	"@deepseek-ai/dsh-client-ui-input-trigger":             true,
	"@deepseek-ai/dsh-client-ui-jobs":                      true,
	"@deepseek-ai/dsh-client-ui-layout":                    true,
	"@deepseek-ai/dsh-client-ui-message-feedback":          true,
	"@deepseek-ai/dsh-client-ui-model-selection":           true,
	"@deepseek-ai/dsh-client-ui-permission-presets":        true,
	"@deepseek-ai/dsh-client-ui-plan":                      true,
	"@deepseek-ai/dsh-client-ui-reference":                 true,
	"@deepseek-ai/dsh-client-ui-renderer":                  true,
	"@deepseek-ai/dsh-client-ui-settings":                  true,
	"@deepseek-ai/dsh-client-ui-settings-general":          true,
	"@deepseek-ai/dsh-client-ui-settings-models":           true,
	"@deepseek-ai/dsh-client-ui-settings-plugin-inventory": true,
	"@deepseek-ai/dsh-client-ui-settings-plugins":          true,
	"@deepseek-ai/dsh-client-ui-sidebar":                   true,
	"@deepseek-ai/dsh-client-ui-skill":                     true,
	"@deepseek-ai/dsh-client-ui-subagent":                  true,
	"@deepseek-ai/dsh-client-ui-theme":                     true,
	"@deepseek-ai/dsh-client-ui-tool":                      true,
	"@deepseek-ai/dsh-client-ui-trajectory":                true,
	"@deepseek-ai/dsh-client-ui-user-questions":            true,
	"@deepseek-ai/dsh-client-ui-workflow-run":              true,
	"@deepseek-ai/dsh-client-ui-workspace":                 true,
	"@deepseek-ai/dsh-code-runtime-worker-thread":          true,
	"@deepseek-ai/dsh-command-compact":                     true,
	"@deepseek-ai/dsh-command-feedback":                    true,
	"@deepseek-ai/dsh-command-goal":                        true,
	"@deepseek-ai/dsh-commands":                            true,
	"@deepseek-ai/dsh-compaction-basic":                    true,
	"@deepseek-ai/dsh-compaction-tool-result-pruner":       true,
	"@deepseek-ai/dsh-session-reference":                   true,
	"@deepseek-ai/dsh-time-context":                        true,
	"@deepseek-ai/dsh-tmux-context":                        true,
	"@deepseek-ai/dsh-credentials-local":                   true,
	"@deepseek-ai/dsh-cordis-client-runner":                true,
	"@deepseek-ai/dsh-cordis-host-runner":                  true,
	"@deepseek-ai/dsh-e2b":                                 true,
	"@deepseek-ai/dsh-experimental-agent-team":             true,
	"@deepseek-ai/dsh-experimental-tool-agent-team":        true,
	"@deepseek-ai/dsh-fs-e2b":                              true,
	"@deepseek-ai/dsh-file-reference":                      true,
	"@deepseek-ai/dsh-file-reference-local":                true,
	"@deepseek-ai/dsh-fs-observation-policy":               true,
	"@deepseek-ai/dsh-fs-sandbox":                          true,
	"@deepseek-ai/dsh-goal":                                true,
	"@deepseek-ai/dsh-goal-round-driver":                   true,
	"@deepseek-ai/dsh-headless":                            true,
	"@deepseek-ai/dsh-headless/startup":                    true,
	"@deepseek-ai/dsh-host-apiproxy":                       true,
	"@deepseek-ai/dsh-host-directory-picker-auto":          true,
	"@deepseek-ai/dsh-host-plugin-inventory":               true,
	"@deepseek-ai/dsh-host-webserver":                      true,
	"@deepseek-ai/dsh-hooks-claude-code":                   true,
	"@deepseek-ai/dsh-hooks-codex":                         true,
	"@deepseek-ai/dsh-jobs-local":                          true,
	"@deepseek-ai/dsh-llm":                                 true,
	"@deepseek-ai/dsh-llm-deepseek":                        true,
	"@deepseek-ai/dsh-llm-pi-ai":                           true,
	"@deepseek-ai/dsh-llm-retry":                           true,
	"@deepseek-ai/dsh-lsp":                                 true,
	"@deepseek-ai/dsh-lsp-stdio":                           true,
	"@deepseek-ai/dsh-message-feedback":                    true,
	"@deepseek-ai/dsh-mcp-client":                          true,
	"@deepseek-ai/dsh-permission-presets":                  true,
	"@deepseek-ai/dsh-plan-mode":                           true,
	"@deepseek-ai/dsh-pwsh-sandbox":                        true,
	"@deepseek-ai/dsh-repeat-tool-reminder":                true,
	"@deepseek-ai/dsh-sandbox-local":                       true,
	"@deepseek-ai/dsh-sandbox-policy":                      true,
	"@deepseek-ai/dsh-schedule":                            true,
	"@deepseek-ai/dsh-session":                             true,
	"@deepseek-ai/dsh-session-checkpoint-policy":           true,
	"@deepseek-ai/dsh-session-log-export":                  true,
	"@deepseek-ai/dsh-session-persistence-jsonl":           true,
	"@deepseek-ai/dsh-session-persistence-sqlite":          true,
	"@deepseek-ai/dsh-session-projection":                  true,
	"@deepseek-ai/dsh-session-projection-cache":            true,
	"@deepseek-ai/dsh-session-query-sqlite":                true,
	"@deepseek-ai/dsh-session-stats":                       true,
	"@deepseek-ai/dsh-session-telemetry-otel":              true,
	"@deepseek-ai/dsh-session-title":                       true,
	"@deepseek-ai/dsh-session-title-all-prompts-llm":       true,
	"@deepseek-ai/dsh-session-title-first-prompt-llm":      true,
	"@deepseek-ai/dsh-settings-file":                       true,
	"@deepseek-ai/dsh-shell-env":                           true,
	"@deepseek-ai/dsh-skill":                               true,
	"@deepseek-ai/dsh-skill-badge":                         true,
	"@deepseek-ai/dsh-skill-filesystem":                    true,
	"@deepseek-ai/dsh-spill-local":                         true,
	"@deepseek-ai/dsh-spill-policy":                        true,
	"@deepseek-ai/dsh-storage":                             true,
	"@deepseek-ai/dsh-storage-domain":                      true,
	"@deepseek-ai/dsh-storage-json":                        true,
	"@deepseek-ai/dsh-storage-sqlite":                      true,
	"@deepseek-ai/dsh-subagent":                            true,
	"@deepseek-ai/dsh-subagent-acp":                        true,
	"@deepseek-ai/dsh-subagent-claude-code":                true,
	"@deepseek-ai/dsh-subagent-codex":                      true,
	"@deepseek-ai/dsh-subagent-dsh-sdk":                    true,
	"@deepseek-ai/dsh-subagent-fork-in-process":            true,
	"@deepseek-ai/dsh-subagent-spawn-in-process":           true,
	"@deepseek-ai/dsh-subprocess-local":                    true,
	"@deepseek-ai/dsh-subprocess-e2b":                      true,
	"@deepseek-ai/dsh-system-prompt":                       true,
	"@deepseek-ai/dsh-terminal":                            true,
	"@deepseek-ai/dsh-terminal-bash":                       true,
	"@deepseek-ai/dsh-token-meter":                         true,
	"@deepseek-ai/dsh-tool-bash":                           true,
	"@deepseek-ai/dsh-tool-bash-persistent":                true,
	"@deepseek-ai/dsh-tool-call-timeout-policy":            true,
	"@deepseek-ai/dsh-tool-fs":                             true,
	"@deepseek-ai/dsh-tool-fs-search":                      true,
	"@deepseek-ai/dsh-tool-goal":                           true,
	"@deepseek-ai/dsh-tool-jobs":                           true,
	"@deepseek-ai/dsh-tool-lsp":                            true,
	"@deepseek-ai/dsh-tool-pwsh":                           true,
	"@deepseek-ai/dsh-tool-pwsh-persistent":                true,
	"@deepseek-ai/dsh-tool-ralph":                          true,
	"@deepseek-ai/dsh-tool-skill":                          true,
	"@deepseek-ai/dsh-tool-str-replace-editor":             true,
	"@deepseek-ai/dsh-tool-subagent":                       true,
	"@deepseek-ai/dsh-tool-subagent-control":               true,
	"@deepseek-ai/dsh-tool-subagent-control/list-agents":   true,
	"@deepseek-ai/dsh-tool-subagent-report":                true,
	"@deepseek-ai/dsh-tool-todo":                           true,
	"@deepseek-ai/dsh-tool-terminal":                       true,
	"@deepseek-ai/dsh-tool-web":                            true,
	"@deepseek-ai/dsh-tool-workflow":                       true,
	"@deepseek-ai/dsh-tools":                               true,
	"@deepseek-ai/dsh-typert-loader":                       true,
	"@deepseek-ai/dsh-typert-registry":                     true,
	"@deepseek-ai/dsh-user-approval":                       true,
	"@deepseek-ai/dsh-user-questions":                      true,
	"@deepseek-ai/dsh-web":                                 true,
	"@deepseek-ai/dsh-web-app":                             true,
	"@deepseek-ai/dsh-web-app/startup":                     true,
	"@deepseek-ai/dsh-web-fetch-http":                      true,
	"@deepseek-ai/dsh-web-search-deepseek":                 true,
	"@deepseek-ai/dsh-web-search-exa":                      true,
	"@deepseek-ai/dsh-web-search-perplexity":               true,
	"@deepseek-ai/dsh-workspace":                           true,
	"@deepseek-ai/dsh-workflow-worker-thread":              true,
}

func newProfileLoader(_ bool) (*profileLoader, error) {
	home, err := resolveDshHome()
	if err != nil {
		return nil, err
	}
	assets, err := harness.MaterializeAssets(home)
	if err != nil {
		return nil, err
	}
	loader := &profileLoader{home: home, upstream: assets.UpstreamDir}
	loader.bundleDirs, err = scanBundleDirs(loader.upstream)
	if err != nil {
		return nil, err
	}
	return loader, nil
}

func resolveDshHome() (string, error) {
	if home := os.Getenv("DSH_HOME"); home != "" {
		return filepath.Abs(home)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("dsh: resolve home: %w", err)
	}
	return filepath.Join(home, ".dsh"), nil
}

func scanBundleDirs(upstream string) (map[string]string, error) {
	root := filepath.Join(upstream, "packages", "bundle")
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("dsh: read upstream bundles: %w", err)
	}
	bundles := map[string]string{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		doc, _, err := readManifest(dir)
		if err != nil {
			continue
		}
		name, _ := doc["name"].(string)
		if name != "" {
			bundles[name] = dir
		}
	}
	return bundles, nil
}

func (loader *profileLoader) frontendDir() string {
	if loader.upstream == "" {
		return ""
	}
	dist := filepath.Join(loader.upstream, "apps", "web", "dist")
	if stat, err := os.Stat(dist); err == nil && stat.IsDir() {
		return dist
	}
	return filepath.Join(loader.upstream, "apps", "web")
}

func (loader *profileLoader) packagesDir() string {
	if loader.upstream == "" {
		return ""
	}
	return filepath.Join(loader.upstream, "packages")
}

func (loader *profileLoader) presetsDir() string {
	if loader.upstream == "" {
		return ""
	}
	return filepath.Join(loader.upstream, "apps", "cli", "config", "agent-presets")
}

func (loader *profileLoader) profileDir(name string) (string, error) {
	if name == "" || name == "." || name == ".." || name == "node_modules" || strings.ContainsAny(name, `/\\`) {
		return "", fmt.Errorf("dsh: invalid profile name %q", name)
	}
	return filepath.Join(loader.home, profilesDir, name), nil
}

func (loader *profileLoader) initProfile(dir string, bundles []string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	manifestPath := filepath.Join(dir, "package.json")
	if _, err := os.Stat(manifestPath); errors.Is(err, os.ErrNotExist) {
		manifest := map[string]any{
			"name":         "dsh-profile-" + filepath.Base(dir),
			"private":      true,
			"dependencies": map[string]any{},
			"dsh":          map[string]any{"profile": map[string]any{"bundles": stringsToAny(bundles)}},
		}
		if err := writeManifest(dir, manifest); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	for path, content := range map[string]string{
		filepath.Join(dir, profilePatchFile):      profilePatchTemplate,
		filepath.Join(dir, "pnpm-workspace.yaml"): profileWorkspace,
	} {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (loader *profileLoader) loadProfile(name string, userLayer bool) (*profile, error) {
	dir, err := loader.profileDir(name)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dir, "package.json")); errors.Is(err, os.ErrNotExist) {
		template, ok := profileTemplates[name]
		if !ok {
			return nil, fmt.Errorf("dsh: profile %q does not exist; create it with 'dsh plugin --profile %s add <package>'", name, name)
		}
		if err := loader.initProfile(dir, template); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	doc, _, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	bundles, err := profileBundles(doc)
	if err != nil {
		return nil, fmt.Errorf("dsh: profile manifest %s: %w", filepath.Join(dir, "package.json"), err)
	}
	if name == "headless" && reflect.DeepEqual(bundles, []string{baseBundle, webBundle, headlessBundle}) {
		bundles = append([]string(nil), profileTemplates["headless"]...)
		setProfileBundles(doc, bundles)
		if err := writeManifest(dir, doc); err != nil {
			return nil, err
		}
	}
	layers := make([]patchLayer, 0, len(bundles))
	for _, packageName := range bundles {
		dir, err := loader.resolveBundleDir(packageName, dir)
		if err != nil {
			return nil, err
		}
		manifest, _, err := readManifest(dir)
		if err != nil {
			return nil, err
		}
		patch, ok := bundlePatch(manifest)
		if !ok {
			return nil, fmt.Errorf("dsh: profile bundle %q declares no dsh.bundle in its package.json", packageName)
		}
		patchPath := filepath.Join(dir, filepath.FromSlash(patch))
		patches, _, err := loadPatchList(patchPath, false, "overlay")
		if err != nil {
			return nil, err
		}
		layers = append(layers, patchLayer{label: packageName, patches: patches})
	}
	patchPath := filepath.Join(dir, profilePatchFile)
	var patches []*yaml.Node
	patchExists := false
	if userLayer {
		patches, patchExists, err = loadPatchList(patchPath, true, "patches")
		if err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(filepath.Join(dir, profileRootFile), []byte(profileRoot), 0o644); err != nil {
		return nil, err
	}
	return &profile{name: name, dir: dir, layers: layers, patchPath: patchPath, patches: patches, patchExists: patchExists}, nil
}

func (loader *profileLoader) resolveBundleDir(packageName, profileDir string) (string, error) {
	if !packageNamePattern.MatchString(packageName) {
		return "", fmt.Errorf("dsh: invalid bundle package name %q", packageName)
	}
	if dir := loader.bundleDirs[packageName]; dir != "" {
		return dir, nil
	}
	dir := filepath.Join(profileDir, "node_modules", filepath.FromSlash(packageName))
	if stat, err := os.Stat(filepath.Join(dir, "package.json")); err == nil && !stat.IsDir() {
		return dir, nil
	}
	return "", fmt.Errorf("dsh: cannot resolve profile bundle %q from the dsh installation or %s; run 'dsh plugin --profile %s install'", packageName, profileDir, filepath.Base(profileDir))
}

func (loader *profileLoader) compose(name string, patchFiles []string, stderr io.Writer) (*composition, error) {
	loaded, err := loader.loadProfile(name, true)
	if err != nil {
		return nil, err
	}
	layers := append([]patchLayer(nil), loaded.layers...)
	if loaded.patchExists {
		layers = append(layers, patchLayer{label: loaded.patchPath, patches: loaded.patches})
	}
	homePatch := filepath.Join(loader.home, profilePatchFile)
	if patches, exists, err := loadPatchList(homePatch, true, "patches"); err != nil {
		return nil, err
	} else if exists {
		layers = append(layers, patchLayer{label: homePatch, patches: patches})
	}
	for _, file := range patchFiles {
		absolute, err := filepath.Abs(file)
		if err != nil {
			return nil, err
		}
		patches, _, err := loadPatchList(absolute, false, "overlay")
		if err != nil {
			return nil, err
		}
		layers = append(layers, patchLayer{label: absolute, patches: patches})
	}
	result := composeLayers(layers, func(layer, warning string) {
		fmt.Fprintf(stderr, "dsh: [%s] %s\n", layer, warning)
	})
	result.profileDir = loaded.dir
	return result, nil
}

func (loader *profileLoader) dump(name string, defaultOnly bool, patchFiles []string, stdout, stderr io.Writer) error {
	loaded, err := loader.loadProfile(name, !defaultOnly)
	if err != nil {
		return err
	}
	layers := append([]patchLayer(nil), loaded.layers...)
	if !defaultOnly {
		if loaded.patchExists {
			layers = append(layers, patchLayer{label: loaded.patchPath, patches: loaded.patches})
		}
		homePatch := filepath.Join(loader.home, profilePatchFile)
		if patches, exists, err := loadPatchList(homePatch, true, "patches"); err != nil {
			return err
		} else if exists {
			layers = append(layers, patchLayer{label: homePatch, patches: patches})
		}
		for _, file := range patchFiles {
			absolute, err := filepath.Abs(file)
			if err != nil {
				return err
			}
			patches, _, err := loadPatchList(absolute, false, "overlay")
			if err != nil {
				return err
			}
			layers = append(layers, patchLayer{label: absolute, patches: patches})
		}
	}
	composed := composeLayers(layers, func(layer, warning string) {
		fmt.Fprintf(stderr, "dsh: [%s] %s\n", layer, warning)
	})
	dump, err := composed.dump()
	if err != nil {
		return err
	}
	_, err = io.WriteString(stdout, dump)
	return err
}

func loadPatchList(path string, optional bool, label string) ([]*yaml.Node, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if optional && errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		verb := "read " + label
		if !optional {
			verb = "read overlay"
		}
		return nil, false, fmt.Errorf("dsh: failed to %s %s: %w", verb, path, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, true, fmt.Errorf("dsh: failed to parse %s %s: %w", label, path, err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.SequenceNode {
		return nil, true, fmt.Errorf("dsh: %s %s must be a top-level YAML array of loader patch entries", label, path)
	}
	patches := document.Content[0].Content
	for index, patch := range patches {
		if patch.Kind != yaml.MappingNode {
			return nil, true, fmt.Errorf("dsh: %s entry %d in %s must be a mapping", label, index+1, path)
		}
	}
	var invalidJS bool
	var visit func(*yaml.Node)
	visit = func(node *yaml.Node) {
		if node == nil || invalidJS {
			return
		}
		if isJSExpr(node) && strings.TrimSpace(node.Value) == "" {
			invalidJS = true
			return
		}
		for _, child := range node.Content {
			visit(child)
		}
	}
	visit(document.Content[0])
	if invalidJS {
		return nil, true, fmt.Errorf("dsh: failed to parse %s %s: !!js expression must not be empty", label, path)
	}
	return patches, true, nil
}

func composeLayers(layers []patchLayer, warn func(layer, warning string)) *composition {
	result := &composition{index: map[string]entryRef{}}
	for layerIndex, layer := range layers {
		for _, patch := range layer.patches {
			result.applyPatch(patch, layer.label, layerIndex, warn)
		}
	}
	return result
}

func (composition *composition) applyPatch(patch *yaml.Node, label string, layer int, warn func(string, string)) {
	id := scalarValue(mappingValue(patch, "id"))
	insert := mappingValue(patch, "insert")
	if insert != nil && insert.Tag != "tag:yaml.org,2002:null" {
		if insert.Kind != yaml.SequenceNode {
			warn(label, "patch insert must be a sequence")
			return
		}
		if id == "" {
			for _, entry := range insert.Content {
				cloned := cloneNode(entry)
				composition.entries = append(composition.entries, cloned)
				composition.provenance = append(composition.provenance, provenance{origin: label, originLayer: layer})
				composition.buildIndex(cloned, len(composition.entries)-1)
			}
			return
		}
		target, ok := composition.index[id]
		if !ok {
			warn(label, fmt.Sprintf("patch insert: entry %q not found", id))
			return
		}
		if scalarValue(mappingValue(target.node, "group")) != "true" {
			warn(label, fmt.Sprintf("patch insert: entry %q is not a group", id))
			return
		}
		config := mappingValue(target.node, "config")
		if config == nil || config.Kind != yaml.SequenceNode {
			config = &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
			setMappingValue(target.node, "config", config)
		}
		for _, entry := range insert.Content {
			cloned := cloneNode(entry)
			config.Content = append(config.Content, cloned)
			composition.buildIndex(cloned, target.top)
		}
		composition.markPatched(target.top, label, layer)
		return
	}
	if id == "" {
		warn(label, "patch: id is required for non-insert patches")
		return
	}
	target, ok := composition.index[id]
	if !ok {
		warn(label, fmt.Sprintf("patch: entry %q not found", id))
		return
	}
	if expected := scalarValue(mappingValue(patch, "name")); expected != "" {
		actual := scalarValue(mappingValue(target.node, "name"))
		if expected != actual {
			warn(label, fmt.Sprintf("patch: name mismatch for %q (expected %q, got %q), skipping", id, actual, expected))
			return
		}
	}
	changed := false
	for index := 0; index < len(patch.Content); index += 2 {
		key := patch.Content[index].Value
		if key == "id" || key == "insert" || key == "name" {
			continue
		}
		value := cloneNode(patch.Content[index+1])
		if current := mappingValue(target.node, key); current == nil || !reflect.DeepEqual(current, value) {
			setMappingValue(target.node, key, value)
			changed = true
		}
	}
	if changed {
		composition.markPatched(target.top, label, layer)
	}
}

func (composition *composition) buildIndex(entry *yaml.Node, top int) {
	if entry.Kind != yaml.MappingNode {
		return
	}
	if id := scalarValue(mappingValue(entry, "id")); id != "" {
		composition.index[id] = entryRef{node: entry, top: top}
	}
	if scalarValue(mappingValue(entry, "group")) == "true" {
		if config := mappingValue(entry, "config"); config != nil && config.Kind == yaml.SequenceNode {
			for _, child := range config.Content {
				composition.buildIndex(child, top)
			}
		}
	}
}

func (composition *composition) markPatched(top int, label string, layer int) {
	record := &composition.provenance[top]
	if record.originLayer == layer {
		return
	}
	if len(record.patchedBy) == 0 || record.patchedBy[len(record.patchedBy)-1] != label {
		record.patchedBy = append(record.patchedBy, label)
	}
}

func (composition *composition) dump() (string, error) {
	if len(composition.entries) == 0 {
		return "\n", nil
	}
	var output strings.Builder
	for start := 0; start < len(composition.entries); {
		label := composition.provenance[start].label()
		end := start + 1
		for end < len(composition.entries) && composition.provenance[end].label() == label {
			end++
		}
		sequence := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: composition.entries[start:end]}
		data, err := yaml.Marshal(sequence)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&output, "# == %s\n%s", label, data)
		start = end
	}
	return output.String(), nil
}

func (provenance provenance) label() string {
	if len(provenance.patchedBy) == 0 {
		return provenance.origin
	}
	return provenance.origin + ", patched by " + strings.Join(provenance.patchedBy, ", ")
}

func (composition *composition) surface() string {
	_, web := composition.index["webserver"]
	_, headless := composition.index["headless-runner"]
	if web && headless {
		return ""
	}
	if web {
		return "web"
	}
	if headless {
		return "headless"
	}
	if len(composition.externalPluginEntries()) > 0 {
		return "custom"
	}
	return ""
}

func (composition *composition) activePluginEntries() []pluginEntry {
	entries := make([]pluginEntry, 0, len(composition.entries))
	var walk func(*yaml.Node)
	walk = func(entry *yaml.Node) {
		if entry == nil || entry.Kind != yaml.MappingNode || scalarValue(mappingValue(entry, "disabled")) == "true" {
			return
		}
		id := scalarValue(mappingValue(entry, "id"))
		name := scalarValue(mappingValue(entry, "name"))
		if id != "" && name != "" && name != "cordis:group" {
			entries = append(entries, pluginEntry{id: id, name: name, node: entry})
		}
		if scalarValue(mappingValue(entry, "group")) == "true" {
			if config := mappingValue(entry, "config"); config != nil && config.Kind == yaml.SequenceNode {
				for _, child := range config.Content {
					walk(child)
				}
			}
		}
	}
	for _, entry := range composition.entries {
		walk(entry)
	}
	return entries
}

func (composition *composition) externalPluginEntries() []pluginEntry {
	entries := make([]pluginEntry, 0)
	for _, entry := range composition.activePluginEntries() {
		if !supportedPluginNames[entry.name] {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (composition *composition) activePluginNames() []string {
	names := make([]string, 0)
	seen := map[string]bool{}
	for _, entry := range composition.activePluginEntries() {
		if !seen[entry.name] {
			seen[entry.name] = true
			names = append(names, entry.name)
		}
	}
	return names
}

func (composition *composition) validateSupportedPlugins() error {
	external := composition.externalPluginEntries()
	for _, entry := range external {
		if _, err := resolveProfilePluginPath(composition.profileDir, entry.name); err != nil {
			return fmt.Errorf("dsh: enabled external plugin %q (id %q): %w", entry.name, entry.id, err)
		}
	}
	return nil
}

func (composition *composition) walkActiveEntries(visit func(id, name string, entry *yaml.Node) error) error {
	var walk func(*yaml.Node) error
	walk = func(entry *yaml.Node) error {
		if entry == nil || entry.Kind != yaml.MappingNode {
			return nil
		}
		id := scalarValue(mappingValue(entry, "id"))
		name := scalarValue(mappingValue(entry, "name"))
		if node := mappingValue(entry, "disabled"); node != nil {
			value, err := profileConfigValue(node)
			if err != nil {
				return fmt.Errorf("%s: disabled: %w", id, err)
			}
			if value != nil {
				disabled, ok := value.(bool)
				if !ok {
					return fmt.Errorf("%s: disabled must be a boolean", id)
				}
				if disabled {
					return nil
				}
			}
		}
		if err := visit(id, name, entry); err != nil {
			return err
		}
		if scalarValue(mappingValue(entry, "group")) == "true" {
			if children := mappingValue(entry, "config"); children != nil && children.Kind == yaml.SequenceNode {
				for _, child := range children.Content {
					if err := walk(child); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	for _, entry := range composition.entries {
		if err := walk(entry); err != nil {
			return err
		}
	}
	return nil
}

func decodeProfileEntryConfig(entry *yaml.Node, target any) error {
	return decodeProfileNode(mappingValue(entry, "config"), target)
}

func decodeProfileNode(node *yaml.Node, target any) error {
	value, err := profileConfigValue(node)
	if err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func (composition *composition) resolveE2BConfig() (*harness.E2BConfig, error) {
	var resolved *harness.E2BConfig
	usesAdapter := false
	err := composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		switch name {
		case "@deepseek-ai/dsh-fs-e2b", "@deepseek-ai/dsh-subprocess-e2b":
			usesAdapter = true
			return nil
		case "@deepseek-ai/dsh-e2b":
			if resolved != nil {
				return errors.New("e2b is configured more than once")
			}
			var raw struct {
				APIKey    *string `json:"apiKey"`
				CWD       *string `json:"cwd"`
				TimeoutMS *int    `json:"timeoutMs"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("e2b(%s): invalid config: %w", id, err)
			}
			config := &harness.E2BConfig{}
			if raw.APIKey != nil {
				if *raw.APIKey == "" {
					return fmt.Errorf("e2b(%s): apiKey must not be empty when supplied", id)
				}
				config.APIKey = *raw.APIKey
			}
			if raw.CWD != nil {
				if !strings.HasPrefix(*raw.CWD, "/") {
					return fmt.Errorf("e2b(%s): cwd must be an absolute Linux path", id)
				}
				config.CWD = *raw.CWD
			}
			if raw.TimeoutMS != nil {
				duration, err := profileSubagentDuration("timeoutMs", raw.TimeoutMS)
				if err != nil {
					return fmt.Errorf("e2b(%s): %w", id, err)
				}
				config.Timeout = duration
			}
			resolved = config
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if usesAdapter && resolved == nil {
		return nil, errors.New("e2b adapters require an enabled @deepseek-ai/dsh-e2b owner")
	}
	return resolved, nil
}

func (composition *composition) resolveClientHMRPollInterval() (time.Duration, error) {
	var resolved time.Duration
	seen := false
	err := composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		if name != "@deepseek-ai/dsh-client-hmr" {
			return nil
		}
		if seen {
			return errors.New("client-hmr is configured more than once")
		}
		seen = true
		var raw struct {
			PollIntervalMS *int `json:"pollIntervalMs"`
		}
		if err := decodeProfileEntryConfig(entry, &raw); err != nil {
			return fmt.Errorf("client-hmr(%s): invalid config: %w", id, err)
		}
		if raw.PollIntervalMS == nil {
			return nil
		}
		if *raw.PollIntervalMS <= 0 {
			return fmt.Errorf("client-hmr(%s): pollIntervalMs must be a positive integer", id)
		}
		resolved = time.Duration(*raw.PollIntervalMS) * time.Millisecond
		return nil
	})
	return resolved, err
}

func (composition *composition) resolveHookConfigs() ([]harness.HookBridgeConfig, error) {
	configs := make([]harness.HookBridgeConfig, 0)
	err := composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		if name != "@deepseek-ai/dsh-hooks-claude-code" && name != "@deepseek-ai/dsh-hooks-codex" {
			return nil
		}
		var raw struct {
			ConfigPath            string `json:"configPath"`
			PluginRoot            string `json:"pluginRoot"`
			ProjectDir            string `json:"projectDir"`
			Model                 string `json:"model"`
			DefaultTimeoutMS      *int   `json:"defaultTimeoutMs"`
			StderrSummaryMaxChars *int   `json:"stderrSummaryMaxChars"`
		}
		if err := decodeProfileEntryConfig(entry, &raw); err != nil {
			return fmt.Errorf("%s(%s): invalid config: %w", name, id, err)
		}
		if strings.TrimSpace(raw.ConfigPath) == "" {
			return fmt.Errorf("%s(%s): configPath is required", name, id)
		}
		config := harness.HookBridgeConfig{
			ConfigPath: raw.ConfigPath, PluginRoot: raw.PluginRoot, ProjectDir: raw.ProjectDir, Model: raw.Model,
		}
		if name == "@deepseek-ai/dsh-hooks-claude-code" {
			config.Dialect = harness.HookDialectClaudeCode
		} else {
			config.Dialect = harness.HookDialectCodex
		}
		if raw.DefaultTimeoutMS != nil {
			duration, err := profileSubagentDuration("defaultTimeoutMs", raw.DefaultTimeoutMS)
			if err != nil {
				return fmt.Errorf("%s(%s): %w", name, id, err)
			}
			config.DefaultTimeout = duration
		}
		if raw.StderrSummaryMaxChars != nil {
			if *raw.StderrSummaryMaxChars <= 0 {
				return fmt.Errorf("%s(%s): stderrSummaryMaxChars must be a positive integer", name, id)
			}
			config.StderrSummaryMaxChars = *raw.StderrSummaryMaxChars
		}
		configs = append(configs, config)
		return nil
	})
	return configs, err
}

func (composition *composition) resolveSessionTitleLLMConfig() (*harness.SessionTitleLLMConfig, error) {
	var resolved *harness.SessionTitleLLMConfig
	err := composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		var automatic harness.SessionTitleAutomaticMode
		switch name {
		case "@deepseek-ai/dsh-session-title-first-prompt-llm":
			automatic = harness.SessionTitleFirstPrompt
		case "@deepseek-ai/dsh-session-title-all-prompts-llm":
			automatic = harness.SessionTitleAllPrompts
		default:
			return nil
		}
		if resolved != nil {
			return errors.New("session-title-llm provider is configured more than once")
		}
		var raw struct {
			TargetWords         int    `json:"targetWords"`
			TargetCJKCharacters int    `json:"targetCjkCharacters"`
			MaxInputBytes       int    `json:"maxInputBytes"`
			MaxOutputTokens     int    `json:"maxOutputTokens"`
			TimeoutMS           int    `json:"timeoutMs"`
			Provider            string `json:"provider"`
			Model               string `json:"model"`
		}
		if err := decodeProfileEntryConfig(entry, &raw); err != nil {
			return fmt.Errorf("session-title-llm(%s): invalid config: %w", id, err)
		}
		if raw.TargetWords <= 0 || raw.TargetCJKCharacters <= 0 || raw.MaxInputBytes <= 0 || raw.MaxOutputTokens <= 0 || raw.TimeoutMS <= 0 || raw.TimeoutMS > 2_147_483_647 {
			return fmt.Errorf("session-title-llm(%s): limits and timeoutMs must be positive integers", id)
		}
		if (raw.Provider == "") != (raw.Model == "") {
			return fmt.Errorf("session-title-llm(%s): provider and model must be supplied together", id)
		}
		resolved = &harness.SessionTitleLLMConfig{
			Enabled: true, Automatic: automatic, TargetWords: raw.TargetWords,
			TargetCJKCharacters: raw.TargetCJKCharacters, MaxInputBytes: raw.MaxInputBytes,
			MaxOutputTokens: raw.MaxOutputTokens, Timeout: time.Duration(raw.TimeoutMS) * time.Millisecond,
			Provider: raw.Provider, Model: raw.Model,
		}
		return nil
	})
	return resolved, err
}

func (composition *composition) resolveWebConfigs() error {
	composition.exaSearch = nil
	composition.perplexitySearch = nil
	composition.httpFetch = false
	composition.httpFetchConfig = nil
	composition.webTools = nil
	composition.webSearchProvider, composition.webSearchSet = "", false
	composition.webFetchProvider, composition.webFetchSet = "", false
	searchPlugins := map[string]bool{}
	var webSeen bool
	err := composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		switch name {
		case "@deepseek-ai/dsh-web":
			if webSeen {
				return errors.New("web is configured more than once")
			}
			webSeen = true
			var raw struct {
				SearchProvider *string `json:"searchProvider"`
				FetchProvider  *string `json:"fetchProvider"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("web(%s): invalid config: %w", id, err)
			}
			if raw.SearchProvider != nil {
				if *raw.SearchProvider == "" {
					return fmt.Errorf("web(%s): searchProvider must not be empty when supplied", id)
				}
				composition.webSearchProvider, composition.webSearchSet = *raw.SearchProvider, true
			}
			if raw.FetchProvider != nil {
				if *raw.FetchProvider == "" {
					return fmt.Errorf("web(%s): fetchProvider must not be empty when supplied", id)
				}
				composition.webFetchProvider, composition.webFetchSet = *raw.FetchProvider, true
			}
		case "@deepseek-ai/dsh-web-search-deepseek":
			searchPlugins["deepseek-official"] = true
		case "@deepseek-ai/dsh-web-search-exa":
			if composition.exaSearch != nil {
				return errors.New("web-search-exa is configured more than once")
			}
			var raw struct {
				APIKey              *string `json:"apiKey"`
				BaseURL             string  `json:"baseURL"`
				SearchType          string  `json:"searchType"`
				NumResults          *int    `json:"numResults"`
				HighlightsPerResult *int    `json:"highlightsPerResult"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("web-search-exa(%s): invalid config: %w", id, err)
			}
			if raw.APIKey != nil && *raw.APIKey == "" {
				return fmt.Errorf("web-search-exa(%s): apiKey must not be empty when supplied", id)
			}
			if raw.SearchType != "" && raw.SearchType != "auto" && raw.SearchType != "keyword" && raw.SearchType != "neural" {
				return fmt.Errorf("web-search-exa(%s): searchType must be auto, keyword, or neural", id)
			}
			if raw.NumResults != nil && *raw.NumResults <= 0 {
				return fmt.Errorf("web-search-exa(%s): numResults must be a positive integer", id)
			}
			if raw.HighlightsPerResult != nil && *raw.HighlightsPerResult <= 0 {
				return fmt.Errorf("web-search-exa(%s): highlightsPerResult must be a positive integer", id)
			}
			options := &harness.ExaSearchProviderOptions{BaseURL: raw.BaseURL, SearchType: raw.SearchType}
			if raw.APIKey != nil {
				options.APIKey = *raw.APIKey
			}
			if raw.NumResults != nil {
				options.NumResults = *raw.NumResults
			}
			if raw.HighlightsPerResult != nil {
				options.HighlightsPerResult = *raw.HighlightsPerResult
			}
			composition.exaSearch = options
			searchPlugins["exa"] = true
		case "@deepseek-ai/dsh-web-search-perplexity":
			if composition.perplexitySearch != nil {
				return errors.New("web-search-perplexity is configured more than once")
			}
			var raw struct {
				APIKey        *string `json:"apiKey"`
				BaseURL       string  `json:"baseURL"`
				Model         string  `json:"model"`
				MaxTokens     *int    `json:"maxTokens"`
				SearchRecency string  `json:"searchRecency"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("web-search-perplexity(%s): invalid config: %w", id, err)
			}
			if raw.APIKey != nil && *raw.APIKey == "" {
				return fmt.Errorf("web-search-perplexity(%s): apiKey must not be empty when supplied", id)
			}
			if raw.MaxTokens != nil && *raw.MaxTokens <= 0 {
				return fmt.Errorf("web-search-perplexity(%s): maxTokens must be a positive integer", id)
			}
			if raw.SearchRecency != "" && raw.SearchRecency != "day" && raw.SearchRecency != "week" && raw.SearchRecency != "month" && raw.SearchRecency != "year" {
				return fmt.Errorf("web-search-perplexity(%s): searchRecency must be day, week, month, or year", id)
			}
			options := &harness.PerplexitySearchProviderOptions{BaseURL: raw.BaseURL, Model: raw.Model, SearchRecency: raw.SearchRecency}
			if raw.APIKey != nil {
				options.APIKey = *raw.APIKey
			}
			if raw.MaxTokens != nil {
				options.MaxTokens = *raw.MaxTokens
			}
			composition.perplexitySearch = options
			searchPlugins["perplexity"] = true
		case "@deepseek-ai/dsh-web-fetch-http":
			if composition.httpFetch {
				return errors.New("web-fetch-http is configured more than once")
			}
			var raw struct {
				MaxURLLength     *int    `json:"maxUrlLength"`
				MaxResponseBytes *int64  `json:"maxResponseBytes"`
				MaxBodyChars     *int    `json:"maxBodyChars"`
				TimeoutMS        *int    `json:"timeoutMs"`
				MaxRedirects     *int    `json:"maxRedirects"`
				UserAgent        *string `json:"userAgent"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("web-fetch-http(%s): invalid config: %w", id, err)
			}
			config := harness.DefaultHTTPWebFetchConfig()
			if raw.MaxURLLength != nil {
				config.MaxURLLength = *raw.MaxURLLength
			}
			if raw.MaxResponseBytes != nil {
				config.MaxResponseBytes = *raw.MaxResponseBytes
			}
			if raw.MaxBodyChars != nil {
				config.MaxBodyChars = *raw.MaxBodyChars
			}
			if raw.TimeoutMS != nil {
				config.Timeout = time.Duration(*raw.TimeoutMS) * time.Millisecond
			}
			if raw.MaxRedirects != nil {
				config.MaxRedirects = *raw.MaxRedirects
			}
			if raw.UserAgent != nil {
				config.UserAgent = *raw.UserAgent
			}
			if config.MaxURLLength <= 0 || config.MaxResponseBytes <= 0 || config.MaxBodyChars <= 0 || config.Timeout <= 0 || config.Timeout > time.Duration(2_147_483_647)*time.Millisecond {
				return fmt.Errorf("web-fetch-http(%s): length, size, and timeout limits must be positive and timeoutMs must not exceed 2147483647", id)
			}
			if config.MaxRedirects < 0 {
				return fmt.Errorf("web-fetch-http(%s): maxRedirects must be a non-negative integer", id)
			}
			composition.httpFetch = true
			composition.httpFetchConfig = &config
		case "@deepseek-ai/dsh-tool-web":
			if composition.webTools != nil {
				return errors.New("tool-web is configured more than once")
			}
			var raw struct {
				Search              *bool `json:"search"`
				Fetch               *bool `json:"fetch"`
				SearchMaxResults    *int  `json:"searchMaxResults"`
				FetchTimeoutMS      *int  `json:"fetchTimeoutMs"`
				SearchTimeoutMS     *int  `json:"searchTimeoutMs"`
				FetchMaxOutputChars *int  `json:"fetchMaxOutputChars"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("tool-web(%s): invalid config: %w", id, err)
			}
			config := harness.DefaultWebToolConfig()
			config.FetchEnabled = true
			if raw.Search != nil {
				config.SearchEnabled = *raw.Search
			}
			if raw.Fetch != nil {
				config.FetchEnabled = *raw.Fetch
			}
			if raw.SearchMaxResults != nil {
				config.SearchMaxResults = *raw.SearchMaxResults
			}
			if raw.FetchTimeoutMS != nil {
				config.FetchTimeout = time.Duration(*raw.FetchTimeoutMS) * time.Millisecond
			}
			if raw.SearchTimeoutMS != nil {
				config.SearchTimeout = time.Duration(*raw.SearchTimeoutMS) * time.Millisecond
			}
			if raw.FetchMaxOutputChars != nil {
				config.FetchMaxOutputChars = *raw.FetchMaxOutputChars
			}
			if config.SearchMaxResults < 1 || config.FetchTimeout <= 0 || config.SearchTimeout <= 0 || config.FetchMaxOutputChars < 1 {
				return fmt.Errorf("tool-web(%s): result, timeout, and output limits must be positive integers", id)
			}
			composition.webTools = &config
		}
		return nil
	})
	if err != nil {
		return err
	}
	if composition.webSearchSet {
		if !searchPlugins[composition.webSearchProvider] {
			return fmt.Errorf("web: configured search provider %q is not mounted", composition.webSearchProvider)
		}
	} else if len(searchPlugins) == 1 {
		for provider := range searchPlugins {
			composition.webSearchProvider, composition.webSearchSet = provider, true
		}
	}
	if composition.webFetchSet && composition.webFetchProvider != "http" {
		return fmt.Errorf("web: configured fetch provider %q is not implemented by the Go runtime", composition.webFetchProvider)
	}
	if composition.webFetchSet && !composition.httpFetch {
		return fmt.Errorf("web: configured fetch provider %q is not mounted", composition.webFetchProvider)
	}
	if !composition.webFetchSet && composition.httpFetch {
		composition.webFetchProvider, composition.webFetchSet = "http", true
	}
	return nil
}

func (composition *composition) resolveFileReferenceConfig() (*harness.FileReferenceConfig, error) {
	var resolved *harness.FileReferenceConfig
	err := composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		if name != "@deepseek-ai/dsh-file-reference-local" {
			return nil
		}
		if resolved != nil {
			return errors.New("file-reference-local is configured more than once")
		}
		var raw struct {
			MaxResults          *int64    `json:"maxResults"`
			MaxEntries          *int64    `json:"maxEntries"`
			ExcludedDirectories *[]string `json:"excludedDirectories"`
		}
		if err := decodeProfileEntryConfig(entry, &raw); err != nil {
			return fmt.Errorf("file-reference-local(%s): invalid config: %w", id, err)
		}
		const maxSafeInteger = int64(1<<53 - 1)
		maxInt := int64(^uint(0) >> 1)
		for field, value := range map[string]*int64{"maxResults": raw.MaxResults, "maxEntries": raw.MaxEntries} {
			if value != nil && (*value < 1 || *value > maxSafeInteger || *value > maxInt) {
				return fmt.Errorf("file-reference-local(%s): %s must be a positive safe integer", id, field)
			}
		}
		config := harness.DefaultConfig().FileReference
		if raw.MaxResults != nil {
			config.MaxResults = int(*raw.MaxResults)
		}
		if raw.MaxEntries != nil {
			config.MaxEntries = int(*raw.MaxEntries)
		}
		if raw.ExcludedDirectories != nil {
			for _, directory := range *raw.ExcludedDirectories {
				if directory == "" || strings.ContainsAny(directory, `/\\`) {
					return fmt.Errorf("file-reference-local(%s): excludedDirectories entries must be non-empty directory basenames", id)
				}
			}
			config.ExcludedDirectories = append([]string(nil), (*raw.ExcludedDirectories)...)
		}
		resolved = &config
		return nil
	})
	return resolved, err
}

func (composition *composition) resolveAgentTeamConfig() (*harness.AgentTeamConfig, error) {
	config := harness.DefaultAgentTeamConfig()
	ownerSeen := false
	toolSeen := false
	err := composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		switch name {
		case "@deepseek-ai/dsh-experimental-agent-team":
			if ownerSeen {
				return errors.New("experimental-agent-team is configured more than once")
			}
			ownerSeen = true
			var raw struct {
				MaxMembers                  *int64 `json:"maxMembers"`
				MaxTasks                    *int64 `json:"maxTasks"`
				MaxPendingMessagesPerMember *int64 `json:"maxPendingMessagesPerMember"`
				MaxMessageBytes             *int64 `json:"maxMessageBytes"`
				DisposalTimeoutMS           *int64 `json:"disposalTimeoutMs"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("experimental-agent-team(%s): invalid config: %w", id, err)
			}
			const maxSafeInteger = int64(1<<53 - 1)
			maxInt := int64(^uint(0) >> 1)
			limits := []struct {
				name   string
				value  *int64
				target *int
			}{
				{"maxMembers", raw.MaxMembers, &config.MaxMembers},
				{"maxTasks", raw.MaxTasks, &config.MaxTasks},
				{"maxPendingMessagesPerMember", raw.MaxPendingMessagesPerMember, &config.MaxPendingMessagesPerMember},
				{"maxMessageBytes", raw.MaxMessageBytes, &config.MaxMessageBytes},
			}
			for _, limit := range limits {
				if limit.value == nil {
					continue
				}
				if *limit.value < 1 || *limit.value > maxSafeInteger || *limit.value > maxInt {
					return fmt.Errorf("experimental-agent-team(%s): %s must be a positive safe integer", id, limit.name)
				}
				*limit.target = int(*limit.value)
			}
			if raw.DisposalTimeoutMS != nil {
				maxDurationMS := int64(^uint64(0)>>1) / int64(time.Millisecond)
				if *raw.DisposalTimeoutMS < 1 || *raw.DisposalTimeoutMS > maxSafeInteger || *raw.DisposalTimeoutMS > maxDurationMS {
					return fmt.Errorf("experimental-agent-team(%s): disposalTimeoutMs must be a positive safe integer within the Go duration range", id)
				}
				config.DisposalTimeout = time.Duration(*raw.DisposalTimeoutMS) * time.Millisecond
			}
		case "@deepseek-ai/dsh-experimental-tool-agent-team":
			if toolSeen {
				return errors.New("experimental-tool-agent-team is configured more than once")
			}
			toolSeen = true
			var raw struct {
				FreshProvider *string `json:"freshProvider"`
				ForkProvider  *string `json:"forkProvider"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("experimental-tool-agent-team(%s): invalid config: %w", id, err)
			}
			if raw.FreshProvider != nil {
				config.FreshProvider = strings.TrimSpace(*raw.FreshProvider)
				if config.FreshProvider == "" {
					return fmt.Errorf("experimental-tool-agent-team(%s): freshProvider must be non-empty", id)
				}
			}
			if raw.ForkProvider != nil {
				config.ForkProvider = strings.TrimSpace(*raw.ForkProvider)
				if config.ForkProvider == "" {
					return fmt.Errorf("experimental-tool-agent-team(%s): forkProvider must be non-empty", id)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if toolSeen && !ownerSeen {
		return nil, errors.New("experimental-tool-agent-team requires an enabled @deepseek-ai/dsh-experimental-agent-team owner")
	}
	if !ownerSeen {
		return nil, nil
	}
	return &config, nil
}

func (composition *composition) resolveSessionPersistence() error {
	var kind string
	var sqliteConfig struct {
		Path                     string `json:"path"`
		JournalMode              string `json:"journalMode"`
		BusyTimeoutMS            *int64 `json:"busyTimeoutMs"`
		PreparedSessionCacheSize *int   `json:"preparedSessionCacheSize"`
		WriteBatchMaxDelayMS     *int   `json:"writeBatchMaxDelayMs"`
	}
	err := composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		if name != "@deepseek-ai/dsh-session-persistence-jsonl" && name != "@deepseek-ai/dsh-session-persistence-sqlite" {
			return nil
		}
		if kind != "" {
			return errors.New("session persistence is configured more than once")
		}
		if name == "@deepseek-ai/dsh-session-persistence-jsonl" {
			kind = "jsonl"
			return nil
		}
		kind = "sqlite"
		if err := decodeProfileEntryConfig(entry, &sqliteConfig); err != nil {
			return fmt.Errorf("session-persistence-sqlite(%s): invalid config: %w", id, err)
		}
		if strings.TrimSpace(sqliteConfig.Path) == "" {
			return fmt.Errorf("session-persistence-sqlite(%s): path is required", id)
		}
		switch sqliteConfig.JournalMode {
		case "", "wal", "delete", "truncate", "persist":
		default:
			return fmt.Errorf("session-persistence-sqlite(%s): unsupported journalMode %q", id, sqliteConfig.JournalMode)
		}
		if sqliteConfig.PreparedSessionCacheSize != nil && *sqliteConfig.PreparedSessionCacheSize < 1 {
			return fmt.Errorf("session-persistence-sqlite(%s): preparedSessionCacheSize must be a positive integer", id)
		}
		if sqliteConfig.BusyTimeoutMS != nil && (*sqliteConfig.BusyTimeoutMS < 0 || *sqliteConfig.BusyTimeoutMS > int64(harness.MaxSQLiteBusyTimeout/time.Millisecond)) {
			return fmt.Errorf("session-persistence-sqlite(%s): busyTimeoutMs must be between 0 and 2147483647", id)
		}
		if sqliteConfig.WriteBatchMaxDelayMS != nil && (*sqliteConfig.WriteBatchMaxDelayMS < 1 || int64(*sqliteConfig.WriteBatchMaxDelayMS) > int64(harness.MaxSQLiteWriteBatchDelay/time.Millisecond)) {
			return fmt.Errorf("session-persistence-sqlite(%s): writeBatchMaxDelayMs must be between 1 and 2147483647", id)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if composition.sessionStore != nil {
		_ = composition.sessionStore.Close()
		composition.sessionStore = nil
	}
	composition.persist = kind != ""
	if kind != "sqlite" {
		return nil
	}
	options := harness.SQLiteSessionStoreOptions{Path: sqliteConfig.Path, JournalMode: harness.SQLiteJournalMode(sqliteConfig.JournalMode)}
	if sqliteConfig.BusyTimeoutMS != nil {
		options.BusyTimeout = time.Duration(*sqliteConfig.BusyTimeoutMS) * time.Millisecond
		options.BusyTimeoutSet = true
	}
	if sqliteConfig.PreparedSessionCacheSize != nil {
		options.PreparedSessionCacheSize = *sqliteConfig.PreparedSessionCacheSize
	}
	if sqliteConfig.WriteBatchMaxDelayMS != nil {
		options.WriteBatchMaxDelay = time.Duration(*sqliteConfig.WriteBatchMaxDelayMS) * time.Millisecond
	}
	store, err := harness.NewSQLiteSessionStoreWithOptions(options)
	if err != nil {
		return fmt.Errorf("session-persistence-sqlite: %w", err)
	}
	composition.sessionStore = store
	return nil
}

func (composition *composition) resolveStorageRuntime() (*harness.StorageRuntimeConfig, error) {
	storageEnabled := false
	var runtimeConfig harness.StorageRuntimeConfig
	err := composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		switch name {
		case "@deepseek-ai/dsh-storage":
			storageEnabled = true
		case "@deepseek-ai/dsh-storage-json":
			if runtimeConfig.JSON != nil {
				return errors.New("storage-json is configured more than once")
			}
			var raw struct {
				Root string `json:"root"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("storage-json(%s): invalid config: %w", id, err)
			}
			if strings.TrimSpace(raw.Root) == "" {
				return fmt.Errorf("storage-json(%s): root is required", id)
			}
			runtimeConfig.JSON = &harness.JSONStorageConfig{Root: raw.Root}
		case "@deepseek-ai/dsh-storage-sqlite":
			if runtimeConfig.SQLite != nil {
				return errors.New("storage-sqlite is configured more than once")
			}
			var raw struct {
				Path        string `json:"path"`
				JournalMode string `json:"journalMode"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("storage-sqlite(%s): invalid config: %w", id, err)
			}
			if strings.TrimSpace(raw.Path) == "" {
				return fmt.Errorf("storage-sqlite(%s): path is required", id)
			}
			switch raw.JournalMode {
			case "", "wal", "delete", "truncate", "persist":
			default:
				return fmt.Errorf("storage-sqlite(%s): unsupported journalMode %q", id, raw.JournalMode)
			}
			runtimeConfig.SQLite = &harness.SQLiteStorageConfig{Path: raw.Path, JournalMode: harness.SQLiteJournalMode(raw.JournalMode)}
		case "@deepseek-ai/dsh-storage-domain":
			if runtimeConfig.Domain != nil {
				return errors.New("storage-domain is configured more than once")
			}
			var raw struct {
				Backend string            `json:"backend"`
				Routes  map[string]string `json:"routes"`
			}
			if err := decodeProfileEntryConfig(entry, &raw); err != nil {
				return fmt.Errorf("storage-domain(%s): invalid config: %w", id, err)
			}
			if strings.TrimSpace(raw.Backend) == "" {
				return fmt.Errorf("storage-domain(%s): backend is required", id)
			}
			runtimeConfig.Domain = &harness.StorageDomainConfig{Backend: raw.Backend, Routes: raw.Routes}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	configured := runtimeConfig.JSON != nil || runtimeConfig.SQLite != nil || runtimeConfig.Domain != nil
	if configured && !storageEnabled {
		return nil, errors.New("storage backends and forms require an enabled @deepseek-ai/dsh-storage service")
	}
	if !configured {
		return nil, nil
	}
	available := map[string]bool{"json": runtimeConfig.JSON != nil, "sqlite": runtimeConfig.SQLite != nil}
	if runtimeConfig.Domain != nil {
		if !available[runtimeConfig.Domain.Backend] {
			return nil, fmt.Errorf("storage-domain: backend %q is not mounted", runtimeConfig.Domain.Backend)
		}
		for domain, backend := range runtimeConfig.Domain.Routes {
			if strings.TrimSpace(domain) == "" || strings.TrimSpace(backend) == "" {
				return nil, errors.New("storage-domain: routes require non-empty domain and backend names")
			}
			if !available[backend] {
				return nil, fmt.Errorf("storage-domain: route %q selects backend %q which is not mounted", domain, backend)
			}
		}
	}
	return &runtimeConfig, nil
}

func (composition *composition) validate() error {
	if err := composition.validateSupportedPlugins(); err != nil {
		return err
	}
	e2b, err := composition.resolveE2BConfig()
	if err != nil {
		return err
	}
	composition.e2b = e2b
	clientHMRPollInterval, err := composition.resolveClientHMRPollInterval()
	if err != nil {
		return err
	}
	composition.clientHMRPollInterval = clientHMRPollInterval
	hooks, err := composition.resolveHookConfigs()
	if err != nil {
		return err
	}
	composition.hooks = hooks
	title, err := composition.resolveSessionTitleLLMConfig()
	if err != nil {
		return err
	}
	composition.sessionTitleLLM = title
	if err := composition.resolveWebConfigs(); err != nil {
		return err
	}
	fileReference, err := composition.resolveFileReferenceConfig()
	if err != nil {
		return err
	}
	composition.fileReference = fileReference
	agentTeams, err := composition.resolveAgentTeamConfig()
	if err != nil {
		return err
	}
	composition.agentTeams = agentTeams
	mcpConfigs, err := composition.resolveMCPConfigs()
	if err != nil {
		return err
	}
	composition.mcpConfigs = mcpConfigs
	lspServers, lspTool, err := composition.resolveLSPConfig()
	if err != nil {
		return err
	}
	composition.lspServers, composition.lspTool = lspServers, lspTool
	terminalConfig, terminalToolConfig, err := composition.resolveTerminalConfigs()
	if err != nil {
		return err
	}
	composition.terminalConfig, composition.terminalToolConfig = terminalConfig, terminalToolConfig
	sessionTelemetry, err := composition.resolveSessionTelemetryConfig()
	if err != nil {
		return err
	}
	composition.sessionTelemetry = sessionTelemetry
	subagentProviders, err := composition.resolveSubagentProviders()
	if err != nil {
		return err
	}
	composition.subagentProviders = subagentProviders
	subagentTools, err := composition.resolveSubagentTools()
	if err != nil {
		return err
	}
	composition.subagentTools = subagentTools
	reportDelivery, err := composition.resolveSubagentReportDelivery()
	if err != nil {
		return err
	}
	composition.subagentReportDelivery = reportDelivery
	toolsMode := strings.TrimSpace(os.Getenv("DSH_TOOLS_MODE"))
	if configured, ok := composition.configString("tools", "mode"); ok {
		toolsMode = configured
	}
	if toolsMode != "" && toolsMode != "native" && toolsMode != "code" && toolsMode != "both" {
		return fmt.Errorf("tools: mode must be native, code, or both, got %q", toolsMode)
	}
	if entry, ok := composition.index["llm-pi-ai"]; ok && composition.enabled("llm-pi-ai") {
		providers := mappingValue(mappingValue(entry.node, "config"), "providers")
		if providers != nil && providers.Kind != yaml.MappingNode {
			return errors.New("llm-pi-ai: providers is now a dict keyed by provider route, not an array of profiles")
		}
	}
	storage, err := composition.resolveStorageRuntime()
	if err != nil {
		return err
	}
	composition.storage = storage
	return composition.resolveSessionPersistence()
}

func (composition *composition) resolveSubagentProviders() ([]harness.SubagentProvider, error) {
	var providers []harness.SubagentProvider
	var walk func(*yaml.Node) error
	walk = func(entry *yaml.Node) error {
		if entry == nil || entry.Kind != yaml.MappingNode || scalarValue(mappingValue(entry, "disabled")) == "true" {
			return nil
		}
		name := scalarValue(mappingValue(entry, "name"))
		id := scalarValue(mappingValue(entry, "id"))
		var provider harness.SubagentProvider
		if name == "@deepseek-ai/dsh-subagent-acp" || name == "@deepseek-ai/dsh-subagent-codex" ||
			name == "@deepseek-ai/dsh-subagent-claude-code" || name == "@deepseek-ai/dsh-subagent-dsh-sdk" {
			value, err := profileConfigValue(mappingValue(entry, "config"))
			if err != nil {
				return fmt.Errorf("%s: %w", id, err)
			}
			data, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("%s: %w", id, err)
			}
			switch name {
			case "@deepseek-ai/dsh-subagent-acp":
				var raw struct {
					ProviderName      string            `json:"providerName"`
					Command           string            `json:"command"`
					Args              []string          `json:"args"`
					CWD               *string           `json:"cwd"`
					Permission        string            `json:"permission"`
					Env               map[string]string `json:"env"`
					DisposeEOFGraceMS *int              `json:"disposeEofGraceMs"`
					DisposeGraceMS    *int              `json:"disposeGraceMs"`
				}
				if err := json.Unmarshal(data, &raw); err != nil {
					return fmt.Errorf("subagent-acp(%s): invalid config: %w", id, err)
				}
				cwd, err := profileSubagentCWD(raw.CWD)
				if err != nil {
					return fmt.Errorf("subagent-acp(%s): %w", id, err)
				}
				disposeEOFGrace, err := profileSubagentDuration("disposeEofGraceMs", raw.DisposeEOFGraceMS)
				if err != nil {
					return fmt.Errorf("subagent-acp(%s): %w", id, err)
				}
				disposeGrace, err := profileSubagentDuration("disposeGraceMs", raw.DisposeGraceMS)
				if err != nil {
					return fmt.Errorf("subagent-acp(%s): %w", id, err)
				}
				provider, err = harness.NewACPSubagentProvider(harness.ACPSubagentConfig{
					ProviderName: raw.ProviderName, Command: raw.Command, Args: raw.Args, CWD: cwd,
					Permission: raw.Permission, Env: raw.Env, DisposeEOFGrace: disposeEOFGrace, DisposeGrace: disposeGrace,
				})
			case "@deepseek-ai/dsh-subagent-codex":
				var raw struct {
					ProviderName   *string           `json:"providerName"`
					PermissionMode *string           `json:"permissionMode"`
					Env            map[string]string `json:"env"`
					DisposeGraceMS *int              `json:"disposeGraceMs"`
				}
				if err := json.Unmarshal(data, &raw); err != nil {
					return fmt.Errorf("subagent-codex(%s): invalid config: %w", id, err)
				}
				if raw.ProviderName != nil && strings.TrimSpace(*raw.ProviderName) == "" {
					return fmt.Errorf("subagent-codex(%s): providerName must not be empty", id)
				}
				disposeGrace, err := profileSubagentDuration("disposeGraceMs", raw.DisposeGraceMS)
				if err != nil {
					return fmt.Errorf("subagent-codex(%s): %w", id, err)
				}
				providerName, permissionMode := "", harness.CodexPermissionMode("")
				if raw.ProviderName != nil {
					providerName = *raw.ProviderName
				}
				if raw.PermissionMode != nil {
					permissionMode = harness.CodexPermissionMode(*raw.PermissionMode)
				}
				executable, resolveErr := profileProductExecutable(composition.profileDir, name, "codex")
				if resolveErr != nil {
					return fmt.Errorf("subagent-codex(%s): %w", id, resolveErr)
				}
				provider, err = harness.NewCodexSubagentProvider(harness.CodexSubagentConfig{
					ProviderName: providerName, PermissionMode: permissionMode, Executable: executable, Env: raw.Env, DisposeGrace: disposeGrace,
				})
			case "@deepseek-ai/dsh-subagent-claude-code":
				var raw struct {
					ProviderName   *string           `json:"providerName"`
					PermissionMode *string           `json:"permissionMode"`
					Env            map[string]string `json:"env"`
					DisposeGraceMS *int              `json:"disposeGraceMs"`
				}
				if err := json.Unmarshal(data, &raw); err != nil {
					return fmt.Errorf("subagent-claude-code(%s): invalid config: %w", id, err)
				}
				if raw.ProviderName != nil && strings.TrimSpace(*raw.ProviderName) == "" {
					return fmt.Errorf("subagent-claude-code(%s): providerName must not be empty", id)
				}
				disposeGrace, err := profileSubagentDuration("disposeGraceMs", raw.DisposeGraceMS)
				if err != nil {
					return fmt.Errorf("subagent-claude-code(%s): %w", id, err)
				}
				providerName, permissionMode := "", harness.ClaudeCodePermissionMode("")
				if raw.ProviderName != nil {
					providerName = *raw.ProviderName
				}
				if raw.PermissionMode != nil {
					permissionMode = harness.ClaudeCodePermissionMode(*raw.PermissionMode)
				}
				executable, resolveErr := profileProductExecutable(composition.profileDir, name, "claude")
				if resolveErr != nil {
					return fmt.Errorf("subagent-claude-code(%s): %w", id, resolveErr)
				}
				provider, err = harness.NewClaudeCodeSubagentProvider(harness.ClaudeCodeSubagentConfig{
					ProviderName: providerName, PermissionMode: permissionMode, Executable: executable, Env: raw.Env, DisposeGrace: disposeGrace,
				})
			case "@deepseek-ai/dsh-subagent-dsh-sdk":
				var raw struct {
					ProviderName      string            `json:"providerName"`
					Command           string            `json:"command"`
					Args              []string          `json:"args"`
					CWD               *string           `json:"cwd"`
					Provider          string            `json:"provider"`
					Model             string            `json:"model"`
					MaxTokens         *int              `json:"maxTokens"`
					Env               map[string]string `json:"env"`
					ShutdownTimeoutMS *int              `json:"shutdownTimeoutMs"`
					DisposeEOFGraceMS *int              `json:"disposeEofGraceMs"`
					DisposeGraceMS    *int              `json:"disposeGraceMs"`
				}
				if err := json.Unmarshal(data, &raw); err != nil {
					return fmt.Errorf("subagent-dsh-sdk(%s): invalid config: %w", id, err)
				}
				cwd, err := profileSubagentCWD(raw.CWD)
				if err != nil {
					return fmt.Errorf("subagent-dsh-sdk(%s): %w", id, err)
				}
				maxTokens := 0
				if raw.MaxTokens != nil {
					if *raw.MaxTokens <= 0 {
						return fmt.Errorf("subagent-dsh-sdk(%s): maxTokens must be a positive integer", id)
					}
					maxTokens = *raw.MaxTokens
				}
				shutdownTimeout, err := profileSubagentDuration("shutdownTimeoutMs", raw.ShutdownTimeoutMS)
				if err != nil {
					return fmt.Errorf("subagent-dsh-sdk(%s): %w", id, err)
				}
				disposeEOFGrace, err := profileSubagentDuration("disposeEofGraceMs", raw.DisposeEOFGraceMS)
				if err != nil {
					return fmt.Errorf("subagent-dsh-sdk(%s): %w", id, err)
				}
				disposeGrace, err := profileSubagentDuration("disposeGraceMs", raw.DisposeGraceMS)
				if err != nil {
					return fmt.Errorf("subagent-dsh-sdk(%s): %w", id, err)
				}
				provider, err = harness.NewDSHSDKSubagentProvider(harness.DSHSDKSubagentConfig{
					ProviderName: raw.ProviderName, Command: raw.Command, Args: raw.Args, CWD: cwd,
					Provider: raw.Provider, Model: raw.Model, MaxTokens: maxTokens, Env: raw.Env,
					ShutdownTimeout: shutdownTimeout, DisposeEOFGrace: disposeEOFGrace, DisposeGrace: disposeGrace,
				})
			}
			if err != nil {
				return fmt.Errorf("%s(%s): %w", name, id, err)
			}
			providers = append(providers, provider)
		}
		if scalarValue(mappingValue(entry, "group")) == "true" {
			if children := mappingValue(entry, "config"); children != nil && children.Kind == yaml.SequenceNode {
				for _, child := range children.Content {
					if err := walk(child); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	for _, entry := range composition.entries {
		if err := walk(entry); err != nil {
			return nil, err
		}
	}
	return providers, nil
}

func profileProductExecutable(profileDir, packageName, command string) (string, error) {
	if profileDir == "" {
		return command, nil
	}
	name := command
	if runtime.GOOS == "windows" {
		name += ".cmd"
	}
	path := filepath.Join(profileDir, "node_modules", ".bin", name)
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return "", fmt.Errorf("cannot resolve package-local %s launcher at %s; install the matching Profile Bundle", command, path)
	}
	return path, nil
}

func (composition *composition) resolveSubagentTools() ([]harness.SubagentToolConfig, error) {
	var tools []harness.SubagentToolConfig
	var walk func(*yaml.Node) error
	walk = func(entry *yaml.Node) error {
		if entry == nil || entry.Kind != yaml.MappingNode || scalarValue(mappingValue(entry, "disabled")) == "true" {
			return nil
		}
		if scalarValue(mappingValue(entry, "name")) == "@deepseek-ai/dsh-tool-subagent" {
			id := scalarValue(mappingValue(entry, "id"))
			config := mappingValue(entry, "config")
			provider := strings.TrimSpace(scalarValue(mappingValue(config, "provider")))
			if provider == "" {
				return fmt.Errorf("tool-subagent(%s): provider is required", id)
			}
			mode := scalarValue(mappingValue(config, "backgroundMode"))
			if mode == "" {
				mode = "one-shot"
			}
			if mode != "one-shot" && mode != "continuable" {
				return fmt.Errorf("tool-subagent(%s): backgroundMode must be one-shot or continuable", id)
			}
			if provider != "spawn" && provider != "fork" && mode != "one-shot" {
				return fmt.Errorf("tool-subagent(%s): Go external providers support only backgroundMode one-shot", id)
			}
			toolName := strings.TrimSpace(scalarValue(mappingValue(config, "toolName")))
			if toolName == "" {
				toolName = "subagent"
			}
			enabled := true
			var enabledPtr *bool
			if node := mappingValue(config, "enableRunInBackground"); node != nil {
				if err := node.Decode(&enabled); err != nil {
					return fmt.Errorf("tool-subagent(%s): enableRunInBackground must be boolean", id)
				}
				enabledPtr = &enabled
			}
			var maxDepth *int
			if node := mappingValue(config, "maxDepth"); node == nil {
				value := 3
				maxDepth = &value
			} else if scalarValue(node) != "provider-managed" {
				var value int
				if err := node.Decode(&value); err != nil || value < 0 {
					return fmt.Errorf("tool-subagent(%s): maxDepth must be a non-negative integer or provider-managed", id)
				}
				maxDepth = &value
			}
			var agentOptions *harness.SubagentAgentOptions
			if node := mappingValue(config, "agentOptions"); node != nil {
				var value harness.SubagentAgentOptions
				if err := decodeProfileNode(node, &value); err != nil || mappingValue(node, "maxTokens") != nil && value.MaxTokens <= 0 {
					return fmt.Errorf("tool-subagent(%s): agentOptions must contain strings and a positive maxTokens", id)
				}
				agentOptions = &value
			}
			persona := scalarValue(mappingValue(config, "persona"))
			var toolFilter *harness.SubagentToolFilter
			if node := mappingValue(config, "toolFilter"); node != nil {
				var value harness.SubagentToolFilter
				if err := decodeProfileNode(node, &value); err != nil {
					return fmt.Errorf("tool-subagent(%s): toolFilter must contain allow and deny string arrays", id)
				}
				if mappingValue(node, "allow") == nil && mappingValue(node, "deny") == nil {
					return fmt.Errorf("tool-subagent(%s): toolFilter must name allow or deny tools", id)
				}
				toolFilter = &value
			}
			tools = append(tools, harness.SubagentToolConfig{
				Provider: provider, ToolName: toolName, BackgroundMode: mode,
				EnableRunInBackground: enabledPtr, MaxDepth: maxDepth,
				AgentOptions: agentOptions, Persona: persona, ToolFilter: toolFilter,
			})
		}
		if scalarValue(mappingValue(entry, "group")) == "true" {
			if children := mappingValue(entry, "config"); children != nil && children.Kind == yaml.SequenceNode {
				for _, child := range children.Content {
					if err := walk(child); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	for _, entry := range composition.entries {
		if err := walk(entry); err != nil {
			return nil, err
		}
	}
	return tools, nil
}

func (composition *composition) resolveSubagentReportDelivery() (string, error) {
	delivery := ""
	err := composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		if name != "@deepseek-ai/dsh-tool-subagent-report" {
			return nil
		}
		if delivery != "" {
			return errors.New("tool-subagent-report is configured more than once")
		}
		var raw struct {
			ReportDelivery *string `json:"reportDelivery"`
		}
		if err := decodeProfileEntryConfig(entry, &raw); err != nil {
			return fmt.Errorf("tool-subagent-report(%s): invalid config: %w", id, err)
		}
		delivery = harness.SubagentReportNextStep
		if raw.ReportDelivery != nil {
			delivery = *raw.ReportDelivery
		}
		if delivery != harness.SubagentReportQuiet && delivery != harness.SubagentReportNextStep {
			return fmt.Errorf("tool-subagent-report(%s): reportDelivery must be quiet or next-step", id)
		}
		return nil
	})
	return delivery, err
}

func profileSubagentCWD(configured *string) (string, error) {
	if configured == nil {
		return "", nil
	}
	if *configured == "" {
		return "", errors.New("config cwd must not be empty")
	}
	return *configured, nil
}

func profileSubagentDuration(name string, milliseconds *int) (time.Duration, error) {
	if milliseconds == nil {
		return 0, nil
	}
	if *milliseconds <= 0 || *milliseconds > 2_147_483_647 {
		return 0, fmt.Errorf("%s must be a positive integer no greater than 2147483647", name)
	}
	return time.Duration(*milliseconds) * time.Millisecond, nil
}

func (composition *composition) resolveMCPConfigs() ([]harness.MCPConfig, error) {
	configs := make([]harness.MCPConfig, 0)
	var walk func(*yaml.Node) error
	walk = func(entry *yaml.Node) error {
		if entry == nil || entry.Kind != yaml.MappingNode || scalarValue(mappingValue(entry, "disabled")) == "true" {
			return nil
		}
		if scalarValue(mappingValue(entry, "name")) == "@deepseek-ai/dsh-mcp-client" {
			id := scalarValue(mappingValue(entry, "id"))
			value, err := profileConfigValue(mappingValue(entry, "config"))
			if err != nil {
				return fmt.Errorf("mcp-client(%s): %w", id, err)
			}
			data, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("mcp-client(%s): %w", id, err)
			}
			var raw struct {
				Transport          string            `json:"transport"`
				ServerName         string            `json:"serverName"`
				Command            string            `json:"command"`
				Args               []string          `json:"args"`
				Env                map[string]string `json:"env"`
				CWD                string            `json:"cwd"`
				URL                string            `json:"url"`
				Headers            map[string]string `json:"headers"`
				ToolCallTimeoutMS  int               `json:"toolCallTimeoutMs"`
				FailOnStartupError bool              `json:"failOnStartupError"`
				Reconnect          struct {
					Enabled        *bool `json:"enabled"`
					InitialDelayMS int   `json:"initialDelayMs"`
					MaxDelayMS     int   `json:"maxDelayMs"`
					MaxAttempts    int   `json:"maxAttempts"`
				} `json:"reconnect"`
			}
			if err := json.Unmarshal(data, &raw); err != nil {
				return fmt.Errorf("mcp-client(%s): invalid config: %w", id, err)
			}
			configs = append(configs, harness.MCPConfig{
				Transport: raw.Transport, ServerName: raw.ServerName, Command: raw.Command,
				Args: raw.Args, Env: raw.Env, CWD: raw.CWD, URL: raw.URL, Headers: raw.Headers,
				ToolCallTimeout:    time.Duration(raw.ToolCallTimeoutMS) * time.Millisecond,
				FailOnStartupError: raw.FailOnStartupError,
				Reconnect: harness.MCPReconnectConfig{
					Enabled:      raw.Reconnect.Enabled,
					InitialDelay: time.Duration(raw.Reconnect.InitialDelayMS) * time.Millisecond,
					MaxDelay:     time.Duration(raw.Reconnect.MaxDelayMS) * time.Millisecond,
					MaxAttempts:  raw.Reconnect.MaxAttempts,
				},
			})
		}
		if scalarValue(mappingValue(entry, "group")) == "true" {
			if children := mappingValue(entry, "config"); children != nil && children.Kind == yaml.SequenceNode {
				for _, child := range children.Content {
					if err := walk(child); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	for _, entry := range composition.entries {
		if err := walk(entry); err != nil {
			return nil, err
		}
	}
	return configs, nil
}

func (composition *composition) resolveLSPConfig() (map[string]harness.LSPStdioConfig, harness.LSPToolConfig, error) {
	servers := map[string]harness.LSPStdioConfig{}
	tool := harness.LSPToolConfig{}
	toolSeen := false
	var walk func(*yaml.Node) error
	walk = func(entry *yaml.Node) error {
		if entry == nil || entry.Kind != yaml.MappingNode || scalarValue(mappingValue(entry, "disabled")) == "true" {
			return nil
		}
		name := scalarValue(mappingValue(entry, "name"))
		if name == "@deepseek-ai/dsh-lsp-stdio" {
			value, err := profileConfigValue(mappingValue(entry, "config"))
			if err != nil {
				return fmt.Errorf("lsp-stdio: %w", err)
			}
			data, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("lsp-stdio: %w", err)
			}
			var raw struct {
				Servers map[string]harness.LSPStdioConfig `json:"servers"`
			}
			if err := json.Unmarshal(data, &raw); err != nil {
				return fmt.Errorf("lsp-stdio: invalid config: %w", err)
			}
			if len(raw.Servers) == 0 {
				return errors.New("lsp-stdio: servers must contain at least one entry")
			}
			for id, config := range raw.Servers {
				if strings.TrimSpace(id) == "" {
					return errors.New("lsp-stdio: server id must be non-empty")
				}
				if _, exists := servers[id]; exists {
					return fmt.Errorf("lsp-stdio: server %q is configured more than once", id)
				}
				servers[id] = config
			}
		}
		if name == "@deepseek-ai/dsh-tool-lsp" {
			if toolSeen {
				return errors.New("tool-lsp is configured more than once")
			}
			toolSeen = true
			value, err := profileConfigValue(mappingValue(entry, "config"))
			if err != nil {
				return fmt.Errorf("tool-lsp: %w", err)
			}
			data, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("tool-lsp: %w", err)
			}
			if err := json.Unmarshal(data, &tool); err != nil {
				return fmt.Errorf("tool-lsp: invalid config: %w", err)
			}
			tool.Enabled = true
		}
		if scalarValue(mappingValue(entry, "group")) == "true" {
			if children := mappingValue(entry, "config"); children != nil && children.Kind == yaml.SequenceNode {
				for _, child := range children.Content {
					if err := walk(child); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	for _, entry := range composition.entries {
		if err := walk(entry); err != nil {
			return nil, harness.LSPToolConfig{}, err
		}
	}
	return servers, tool, nil
}

func (composition *composition) resolveTerminalConfigs() (harness.TerminalConfig, harness.TerminalToolConfig, error) {
	var terminal harness.TerminalConfig
	var tool harness.TerminalToolConfig
	terminalSeen, toolSeen := false, false
	decode := func(label string, entry *yaml.Node, target any) error {
		value, err := profileConfigValue(mappingValue(entry, "config"))
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		data, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		if err := json.Unmarshal(data, target); err != nil {
			return fmt.Errorf("%s: invalid config: %w", label, err)
		}
		return nil
	}
	var walk func(*yaml.Node) error
	walk = func(entry *yaml.Node) error {
		if entry == nil || entry.Kind != yaml.MappingNode {
			return nil
		}
		id := scalarValue(mappingValue(entry, "id"))
		name := scalarValue(mappingValue(entry, "name"))
		label := id
		if label == "" {
			label = name
		}
		disabled := false
		if node := mappingValue(entry, "disabled"); node != nil {
			value, err := profileConfigValue(node)
			if err != nil {
				return fmt.Errorf("%s: disabled: %w", label, err)
			}
			if value != nil {
				var ok bool
				disabled, ok = value.(bool)
				if !ok {
					return fmt.Errorf("%s: disabled must be a boolean", label)
				}
			}
		}
		switch name {
		case "@deepseek-ai/dsh-terminal-bash":
			if disabled {
				if !terminalSeen {
					terminal.Disabled = true
				}
				break
			}
			if terminalSeen {
				return errors.New("terminal-bash is configured more than once")
			}
			terminalSeen, terminal.Disabled = true, false
			if err := decode("terminal-bash("+label+")", entry, &terminal); err != nil {
				return err
			}
			terminal.Disabled = false
		case "@deepseek-ai/dsh-tool-terminal":
			if disabled {
				if !toolSeen {
					tool.Disabled = true
				}
				break
			}
			if toolSeen {
				return errors.New("tool-terminal is configured more than once")
			}
			toolSeen, tool.Disabled = true, false
			if err := decode("tool-terminal("+label+")", entry, &tool); err != nil {
				return err
			}
			tool.Disabled = false
		}
		if disabled {
			return nil
		}
		if scalarValue(mappingValue(entry, "group")) == "true" {
			if children := mappingValue(entry, "config"); children != nil && children.Kind == yaml.SequenceNode {
				for _, child := range children.Content {
					if err := walk(child); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	for _, entry := range composition.entries {
		if err := walk(entry); err != nil {
			return harness.TerminalConfig{}, harness.TerminalToolConfig{}, err
		}
	}
	return terminal, tool, nil
}

func (composition *composition) resolveSessionTelemetryConfig() (*harness.SessionTelemetryConfig, error) {
	if os.Getenv("DSH_TELEMETRY_DISABLED") != "" {
		return nil, nil
	}
	var resolved *harness.SessionTelemetryConfig
	var walk func(*yaml.Node) error
	walk = func(entry *yaml.Node) error {
		if entry == nil || entry.Kind != yaml.MappingNode {
			return nil
		}
		disabled := false
		if node := mappingValue(entry, "disabled"); node != nil {
			value, err := profileConfigValue(node)
			if err != nil {
				return fmt.Errorf("session-telemetry-otel: disabled: %w", err)
			}
			if value != nil {
				var ok bool
				disabled, ok = value.(bool)
				if !ok {
					return errors.New("session-telemetry-otel: disabled must be a boolean")
				}
			}
		}
		if scalarValue(mappingValue(entry, "name")) == "@deepseek-ai/dsh-session-telemetry-otel" && !disabled {
			if resolved != nil {
				return errors.New("session-telemetry-otel is configured more than once")
			}
			value, err := profileConfigValue(mappingValue(entry, "config"))
			if err != nil {
				return fmt.Errorf("session-telemetry-otel: %w", err)
			}
			data, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("session-telemetry-otel: %w", err)
			}
			var raw struct {
				Mode                  string `json:"mode"`
				ShutdownTimeoutMillis *int   `json:"shutdownTimeoutMillis"`
				Exporter              *struct {
					URL              string            `json:"url"`
					Headers          map[string]string `json:"headers"`
					Compression      string            `json:"compression"`
					TimeoutMillis    *int              `json:"timeoutMillis"`
					KeepAlive        *bool             `json:"keepAlive"`
					ConcurrencyLimit *int              `json:"concurrencyLimit"`
					UserAgent        string            `json:"userAgent"`
				} `json:"exporter"`
				Processor *struct {
					ScheduledDelayMillis *int `json:"scheduledDelayMillis"`
					MaxQueueSize         *int `json:"maxQueueSize"`
					MaxExportBatchSize   *int `json:"maxExportBatchSize"`
					ExportTimeoutMillis  *int `json:"exportTimeoutMillis"`
				} `json:"processor"`
			}
			if err := json.Unmarshal(data, &raw); err != nil {
				return fmt.Errorf("session-telemetry-otel: invalid config: %w", err)
			}
			mode := harness.SessionTelemetryMode(raw.Mode)
			if mode == "" {
				mode = harness.SessionTelemetryModeDisabled
			}
			switch mode {
			case harness.SessionTelemetryModeDisabled, harness.SessionTelemetryModeFull, harness.SessionTelemetryModeFeedbackOnly:
			default:
				return fmt.Errorf("session-telemetry-otel: mode must be DISABLED, FULL, or FEEDBACK_ONLY, got %q", raw.Mode)
			}
			config := &harness.SessionTelemetryConfig{Mode: mode}
			if raw.ShutdownTimeoutMillis != nil {
				if *raw.ShutdownTimeoutMillis <= 0 || *raw.ShutdownTimeoutMillis > 2_147_483_647 {
					return fmt.Errorf("session-telemetry-otel: shutdownTimeoutMillis must be a positive integer no greater than 2147483647, got %d", *raw.ShutdownTimeoutMillis)
				}
				config.ShutdownTimeout = time.Duration(*raw.ShutdownTimeoutMillis) * time.Millisecond
			}
			if raw.Exporter != nil {
				config.Exporter.URL = raw.Exporter.URL
				config.Exporter.Headers = raw.Exporter.Headers
				config.Exporter.Compression = raw.Exporter.Compression
				config.Exporter.KeepAlive = raw.Exporter.KeepAlive
				config.Exporter.UserAgent = raw.Exporter.UserAgent
				if raw.Exporter.TimeoutMillis != nil {
					config.Exporter.Timeout = time.Duration(*raw.Exporter.TimeoutMillis) * time.Millisecond
				}
				if raw.Exporter.ConcurrencyLimit != nil {
					config.Exporter.ConcurrencyLimit = *raw.Exporter.ConcurrencyLimit
				}
			}
			if raw.Processor != nil {
				if raw.Processor.MaxExportBatchSize != nil && *raw.Processor.MaxExportBatchSize < 1 {
					return fmt.Errorf("session-telemetry-otel: processor.maxExportBatchSize must be a positive integer, got %d", *raw.Processor.MaxExportBatchSize)
				}
				if raw.Processor.ScheduledDelayMillis != nil {
					config.Processor.ScheduledDelay = time.Duration(*raw.Processor.ScheduledDelayMillis) * time.Millisecond
				}
				if raw.Processor.MaxQueueSize != nil {
					config.Processor.MaxQueueSize = *raw.Processor.MaxQueueSize
				}
				if raw.Processor.MaxExportBatchSize != nil {
					config.Processor.MaxExportBatchSize = *raw.Processor.MaxExportBatchSize
				}
				if raw.Processor.ExportTimeoutMillis != nil {
					config.Processor.ExportTimeout = time.Duration(*raw.Processor.ExportTimeoutMillis) * time.Millisecond
				}
			}
			resolved = config
		}
		if disabled {
			return nil
		}
		if scalarValue(mappingValue(entry, "group")) == "true" {
			if children := mappingValue(entry, "config"); children != nil && children.Kind == yaml.SequenceNode {
				for _, child := range children.Content {
					if err := walk(child); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	for _, entry := range composition.entries {
		if err := walk(entry); err != nil {
			return nil, err
		}
	}
	return resolved, nil
}

func profileConfigValue(node *yaml.Node) (any, error) {
	if node == nil {
		return map[string]any{}, nil
	}
	if isJSExpr(node) {
		return evaluateProfileJS(node.Value)
	}
	switch node.Kind {
	case yaml.MappingNode:
		value := make(map[string]any, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			child, err := profileConfigValue(node.Content[index+1])
			if err != nil {
				return nil, err
			}
			value[node.Content[index].Value] = child
		}
		return value, nil
	case yaml.SequenceNode:
		value := make([]any, len(node.Content))
		for index, child := range node.Content {
			resolved, err := profileConfigValue(child)
			if err != nil {
				return nil, err
			}
			value[index] = resolved
		}
		return value, nil
	case yaml.ScalarNode:
		var value any
		if err := node.Decode(&value); err != nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported YAML node kind %d", node.Kind)
	}
}

func evaluateProfileJS(expression string) (any, error) {
	vm := goja.New()
	process := vm.NewObject()
	env := vm.NewObject()
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			_ = env.Set(key, value)
		}
	}
	_ = process.Set("env", env)
	_ = process.Set("platform", runtime.GOOS)
	_ = process.Set("cwd", func() string {
		cwd, _ := os.Getwd()
		return cwd
	})
	_ = process.Set("getBuiltinModule", func(name string) any {
		module := vm.NewObject()
		switch name {
		case "node:path", "path":
			_ = module.Set("join", filepath.Join)
		case "node:os", "os":
			_ = module.Set("homedir", func() string {
				home, _ := os.UserHomeDir()
				return home
			})
		}
		return module
	})
	_ = vm.Set("process", process)
	_ = vm.Set("dshHomePath", func(parts ...string) string {
		home, _ := resolveDshHome()
		return filepath.Join(append([]string{home}, parts...)...)
	})
	value, err := vm.RunString("(" + expression + "\n)")
	if err != nil {
		return nil, fmt.Errorf("!!js evaluation failed: %w", err)
	}
	if goja.IsUndefined(value) || goja.IsNull(value) {
		return nil, nil
	}
	return value.Export(), nil
}

func (composition *composition) enabled(id string) bool {
	entry, ok := composition.index[id]
	if !ok {
		return false
	}
	disabled := mappingValue(entry.node, "disabled")
	return disabled == nil || isJSExpr(disabled) || scalarValue(disabled) != "true"
}

func (composition *composition) configString(id, key string) (string, bool) {
	entry, ok := composition.index[id]
	if !ok {
		return "", false
	}
	config := mappingValue(entry.node, "config")
	value := mappingValue(config, key)
	if value == nil || value.Kind != yaml.ScalarNode || isJSExpr(value) {
		return "", false
	}
	return value.Value, value.Value != ""
}

func (composition *composition) configInt(id, key string) (int, bool) {
	entry, ok := composition.index[id]
	if !ok {
		return 0, false
	}
	config := mappingValue(entry.node, "config")
	value := mappingValue(config, key)
	if value == nil || value.Kind != yaml.ScalarNode || isJSExpr(value) {
		return 0, false
	}
	var result int
	if err := value.Decode(&result); err != nil {
		return 0, false
	}
	return result, true
}

func (composition *composition) configInts(id, key string) ([]int, bool) {
	entry, ok := composition.index[id]
	if !ok {
		return nil, false
	}
	value := mappingValue(mappingValue(entry.node, "config"), key)
	if value == nil || value.Kind != yaml.SequenceNode {
		return nil, false
	}
	result := make([]int, len(value.Content))
	for index, item := range value.Content {
		if item.Decode(&result[index]) != nil {
			return nil, false
		}
	}
	return result, true
}

func (composition *composition) configStrings(id, key string) ([]string, bool) {
	entry, ok := composition.index[id]
	if !ok {
		return nil, false
	}
	value := mappingValue(mappingValue(entry.node, "config"), key)
	if value == nil || value.Kind != yaml.SequenceNode {
		return nil, false
	}
	result := make([]string, len(value.Content))
	for index, item := range value.Content {
		if item.Kind != yaml.ScalarNode || isJSExpr(item) {
			return nil, false
		}
		result[index] = item.Value
	}
	return result, true
}

func mappingValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	return nil
}

func setMappingValue(mapping *yaml.Node, key string, value *yaml.Node) {
	for index := 0; index < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			mapping.Content[index+1] = value
			return
		}
	}
	mapping.Content = append(mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, value)
}

func scalarValue(node *yaml.Node) string {
	if node == nil || node.Kind != yaml.ScalarNode {
		return ""
	}
	return node.Value
}

func isJSExpr(node *yaml.Node) bool {
	return node.Tag == "!!js" || node.Tag == "tag:yaml.org,2002:js"
}

func cloneNode(node *yaml.Node) *yaml.Node {
	if node == nil {
		return nil
	}
	cloned := *node
	cloned.Content = make([]*yaml.Node, len(node.Content))
	for index, child := range node.Content {
		cloned.Content[index] = cloneNode(child)
	}
	if node.Alias != nil {
		cloned.Alias = cloneNode(node.Alias)
	}
	return &cloned
}

func readManifest(dir string) (map[string]any, []string, error) {
	path := filepath.Join(dir, "package.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("dsh: failed to read profile manifest %s: %w", path, err)
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, nil, fmt.Errorf("dsh: failed to parse manifest %s: %w", path, err)
	}
	document, ok := value.(map[string]any)
	if !ok || document == nil {
		return nil, nil, fmt.Errorf("dsh: profile manifest %s must hold a JSON object", path)
	}
	return document, orderedDependencyNames(data), nil
}

func orderedDependencyNames(data []byte) []string {
	var document yaml.Node
	if yaml.Unmarshal(data, &document) != nil || len(document.Content) != 1 {
		return nil
	}
	dependencies := mappingValue(document.Content[0], "dependencies")
	if dependencies == nil || dependencies.Kind != yaml.MappingNode {
		return nil
	}
	names := make([]string, 0, len(dependencies.Content)/2)
	for index := 0; index < len(dependencies.Content); index += 2 {
		names = append(names, dependencies.Content[index].Value)
	}
	return names
}

func writeManifest(dir string, document map[string]any) error {
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(dir, "package.json"), data, 0o644)
}

func profileBundles(document map[string]any) ([]string, error) {
	dsh, ok := document["dsh"].(map[string]any)
	if !ok {
		return nil, nil
	}
	profile, ok := dsh["profile"].(map[string]any)
	if !ok {
		return nil, nil
	}
	value, ok := profile["bundles"]
	if !ok {
		return nil, nil
	}
	raw, ok := value.([]any)
	if !ok {
		return nil, errors.New("dsh.profile.bundles must be an array of package names")
	}
	bundles := make([]string, len(raw))
	for index, item := range raw {
		name, ok := item.(string)
		if !ok || name == "" {
			return nil, errors.New("dsh.profile.bundles must be an array of package names")
		}
		bundles[index] = name
	}
	return bundles, nil
}

func setProfileBundles(document map[string]any, bundles []string) {
	dsh, _ := document["dsh"].(map[string]any)
	if dsh == nil {
		dsh = map[string]any{}
		document["dsh"] = dsh
	}
	profile, _ := dsh["profile"].(map[string]any)
	if profile == nil {
		profile = map[string]any{}
		dsh["profile"] = profile
	}
	profile["bundles"] = stringsToAny(bundles)
}

func stringsToAny(values []string) []any {
	result := make([]any, len(values))
	for index, value := range values {
		result[index] = value
	}
	return result
}

func bundlePatch(document map[string]any) (string, bool) {
	dsh, _ := document["dsh"].(map[string]any)
	bundle, _ := dsh["bundle"].(map[string]any)
	patch, ok := bundle["patch"].(string)
	return patch, ok && patch != ""
}

func dependencySet(document map[string]any) map[string]bool {
	result := map[string]bool{}
	dependencies, _ := document["dependencies"].(map[string]any)
	for name := range dependencies {
		result[name] = true
	}
	return result
}

func (loader *profileLoader) runPlugin(profileName string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	dir, err := loader.profileDir(profileName)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(dir, "package.json")); errors.Is(err, os.ErrNotExist) {
		bundles := profileTemplates[profileName]
		if bundles == nil {
			bundles = []string{baseBundle}
		}
		if err := loader.initProfile(dir, bundles); err != nil {
			return err
		}
		fmt.Fprintf(stderr, "dsh: initialized profile %s at %s\n", profileName, dir)
	} else if err != nil {
		return err
	}
	before, _, err := readManifest(dir)
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	forwarded := make([]string, len(args))
	for index, argument := range args {
		forwarded[index] = anchorPathSpec(argument, cwd)
	}
	command := exec.Command("pnpm", forwarded...)
	if runtime.GOOS == "windows" {
		command = exec.Command("cmd.exe", append([]string{"/d", "/s", "/c", "pnpm"}, forwarded...)...)
	}
	command.Dir = dir
	command.Stdin = stdin
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Run(); err != nil {
		var missing *exec.Error
		if errors.As(err, &missing) && errors.Is(missing.Err, exec.ErrNotFound) {
			fmt.Fprintln(stderr, "dsh: pnpm not found on PATH; install pnpm to manage profile plugins")
			return exitError{code: 127}
		}
		var status *exec.ExitError
		if errors.As(err, &status) {
			fmt.Fprintf(stderr, "dsh: pnpm failed in profile directory %s\n", dir)
			for _, argument := range args {
				if strings.HasPrefix(argument, "git+") || strings.HasPrefix(argument, "github:") || strings.Contains(argument, ".git#") || strings.HasSuffix(argument, ".git") {
					fmt.Fprintf(stderr, "dsh: git-hosted plugins may need their prepare script allowlisted under allowBuilds in %s, then re-run\n", filepath.Join(dir, "pnpm-workspace.yaml"))
					break
				}
			}
			return exitError{code: status.ExitCode()}
		}
		return err
	}
	return loader.reconcilePlugins(before, dir, stderr)
}

var relativeSpec = regexp.MustCompile(`^(?:(file|link):)?(\.{1,2}(?:[/\\].*)?)$`)

func anchorPathSpec(argument, cwd string) string {
	match := relativeSpec.FindStringSubmatch(argument)
	if match == nil {
		return argument
	}
	prefix := ""
	if match[1] != "" {
		prefix = match[1] + ":"
	}
	return prefix + filepath.Join(cwd, filepath.FromSlash(match[2]))
}

func (loader *profileLoader) reconcilePlugins(before map[string]any, profileDir string, stderr io.Writer) error {
	after, order, err := readManifest(profileDir)
	if err != nil {
		return err
	}
	beforeDeps := dependencySet(before)
	afterDeps := dependencySet(after)
	plugins, err := profileBundles(after)
	if err != nil {
		return err
	}
	changed := false
	for _, packageName := range order {
		isBundle := loader.exportsPatch(packageName, profileDir)
		if isBundle && !contains(plugins, packageName) {
			plugins = append(plugins, packageName)
			changed = true
		} else if !isBundle && !beforeDeps[packageName] {
			fmt.Fprintf(stderr, "dsh: warning: %s declares no dsh.bundle; installed as a plain dependency, not a profile layer\n", packageName)
		}
	}
	filtered := plugins[:0]
	for _, packageName := range plugins {
		wasDependency := beforeDeps[packageName] || afterDeps[packageName]
		if wasDependency && (!afterDeps[packageName] || !loader.exportsPatch(packageName, profileDir)) {
			changed = true
			continue
		}
		filtered = append(filtered, packageName)
	}
	if !changed {
		return nil
	}
	setProfileBundles(after, filtered)
	return writeManifest(profileDir, after)
}

func (loader *profileLoader) exportsPatch(packageName, profileDir string) bool {
	dir, err := loader.resolveBundleDir(packageName, profileDir)
	if err != nil {
		return false
	}
	document, _, err := readManifest(dir)
	if err != nil {
		return false
	}
	_, ok := bundlePatch(document)
	return ok
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
