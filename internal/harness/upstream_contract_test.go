package harness

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var noHostApplyPattern = regexp.MustCompile(`export function apply\([^)]*\): void \{\}`)

// Each row is id=package. Bundle rows are derived from the pinned alpha.4
// bundle patches; the web profile also mounts the shipped standard preset.
var backendPluginContract = map[string][]string{
	"@deepseek-ai/dsh-acp-app": {
		"acp-app-startup=@deepseek-ai/dsh-acp-app",
		"acp=@deepseek-ai/dsh-acp",
	},
	"@deepseek-ai/dsh-base": {
		"agent-default-model=@deepseek-ai/dsh-agent-default-model",
		"agent-instructions=@deepseek-ai/dsh-agent-instructions",
		"agent-loop=@deepseek-ai/dsh-agent-loop",
		"agent=@deepseek-ai/dsh-agent",
		"approval=@deepseek-ai/dsh-user-approval",
		"attachment-local=@deepseek-ai/dsh-attachment-local",
		"bash-sandbox=@deepseek-ai/dsh-bash-sandbox",
		"command-compact=@deepseek-ai/dsh-command-compact",
		"command-feedback=@deepseek-ai/dsh-command-feedback",
		"command-goal=@deepseek-ai/dsh-command-goal",
		"commands=@deepseek-ai/dsh-commands",
		"compaction-basic=@deepseek-ai/dsh-compaction-basic",
		"credentials=@deepseek-ai/dsh-credentials-local",
		"deepseek-llm-api-extensions=@deepseek-ai/dsh-deepseek-llm-api-extensions",
		"fs-observation-policy=@deepseek-ai/dsh-fs-observation-policy",
		"fs-sandbox=@deepseek-ai/dsh-fs-sandbox",
		"goal-round-driver=@deepseek-ai/dsh-goal-round-driver",
		"goal=@deepseek-ai/dsh-goal",
		"jobs=@deepseek-ai/dsh-jobs-local",
		"llm-deepseek=@deepseek-ai/dsh-llm-deepseek",
		"llm-pi-ai=@deepseek-ai/dsh-llm-pi-ai",
		"llm-retry=@deepseek-ai/dsh-llm-retry",
		"llm=@deepseek-ai/dsh-llm",
		"permission=@deepseek-ai/dsh-permission-presets",
		"plan-mode=@deepseek-ai/dsh-plan-mode",
		"plugin-package-inventory-deepseek=@deepseek-ai/dsh-plugin-package-inventory-deepseek",
		"pwsh-sandbox=@deepseek-ai/dsh-pwsh-sandbox",
		"repeat-tool-reminder=@deepseek-ai/dsh-repeat-tool-reminder",
		"sandbox-policy=@deepseek-ai/dsh-sandbox-policy",
		"sandbox=@deepseek-ai/dsh-sandbox-local",
		"session-checkpoint-policy=@deepseek-ai/dsh-session-checkpoint-policy",
		"session-log-deepseek=@deepseek-ai/dsh-session-log-deepseek",
		"session-persistence-jsonl=@deepseek-ai/dsh-session-persistence-jsonl",
		"session-projection-cache=@deepseek-ai/dsh-session-projection-cache",
		"session-projection=@deepseek-ai/dsh-session-projection",
		"session-query-sqlite=@deepseek-ai/dsh-session-query-sqlite",
		"session-telemetry-otel=@deepseek-ai/dsh-session-telemetry-otel",
		"session-title-llm=@deepseek-ai/dsh-session-title-first-prompt-llm",
		"session-title=@deepseek-ai/dsh-session-title",
		"session=@deepseek-ai/dsh-session",
		"settings=@deepseek-ai/dsh-settings-file",
		"shell-env=@deepseek-ai/dsh-shell-env",
		"skill-filesystem=@deepseek-ai/dsh-skill-filesystem",
		"skill=@deepseek-ai/dsh-skill",
		"spill-local=@deepseek-ai/dsh-spill-local",
		"spill-policy=@deepseek-ai/dsh-spill-policy",
		"storage-domain=@deepseek-ai/dsh-storage-domain",
		"storage-json=@deepseek-ai/dsh-storage-json",
		"storage=@deepseek-ai/dsh-storage",
		"subagent-fork-in-process=@deepseek-ai/dsh-subagent-fork-in-process",
		"subagent-spawn-in-process=@deepseek-ai/dsh-subagent-spawn-in-process",
		"subagent=@deepseek-ai/dsh-subagent",
		"subprocess=@deepseek-ai/dsh-subprocess-local",
		"system-prompt=@deepseek-ai/dsh-system-prompt",
		"timeout-policy=@deepseek-ai/dsh-tool-call-timeout-policy",
		"timer=@deepseek-ai/cordis-plugin-timer",
		"token-meter=@deepseek-ai/dsh-token-meter",
		"tool-bash=@deepseek-ai/dsh-tool-bash",
		"tool-fs-search=@deepseek-ai/dsh-tool-fs-search",
		"tool-fs=@deepseek-ai/dsh-tool-fs",
		"tool-goal=@deepseek-ai/dsh-tool-goal",
		"tool-jobs=@deepseek-ai/dsh-tool-jobs",
		"tool-pwsh=@deepseek-ai/dsh-tool-pwsh",
		"tool-ralph=@deepseek-ai/dsh-tool-ralph",
		"tool-result-pruner=@deepseek-ai/dsh-compaction-tool-result-pruner",
		"tool-skill=@deepseek-ai/dsh-tool-skill",
		"tool-str-replace-editor=@deepseek-ai/dsh-tool-str-replace-editor",
		"tool-subagent-control=@deepseek-ai/dsh-tool-subagent-control",
		"tool-subagent-fork=@deepseek-ai/dsh-tool-subagent",
		"tool-subagent-list-agents=@deepseek-ai/dsh-tool-subagent-control/list-agents",
		"tool-subagent=@deepseek-ai/dsh-tool-subagent",
		"tool-todo=@deepseek-ai/dsh-tool-todo",
		"tool-web=@deepseek-ai/dsh-tool-web",
		"tool-workflow=@deepseek-ai/dsh-tool-workflow",
		"tools=@deepseek-ai/dsh-tools",
		"typert-gateway=@deepseek-ai/dsh-api-gateway",
		"typert-loader=@deepseek-ai/dsh-typert-loader",
		"typert=@deepseek-ai/dsh-typert-registry",
		"user-questions=@deepseek-ai/dsh-user-questions",
		"web-fetch-http=@deepseek-ai/dsh-web-fetch-http",
		"web-search-deepseek=@deepseek-ai/dsh-web-search-deepseek",
		"web=@deepseek-ai/dsh-web",
		"workflow-worker-thread=@deepseek-ai/dsh-workflow-worker-thread",
	},
	"@deepseek-ai/dsh-headless": {
		"code-runtime=@deepseek-ai/dsh-code-runtime-worker-thread",
		"headless-runner=@deepseek-ai/dsh-headless",
		"headless-startup=@deepseek-ai/dsh-headless/startup",
	},
	"@deepseek-ai/dsh-sdk-app": {
		"sdk-app-startup=@deepseek-ai/dsh-sdk-app",
		"sdk-jsonrpc-server=@deepseek-ai/dsh-sdk-jsonrpc-server",
	},
	"@deepseek-ai/dsh-sdk-minimal": {
		"agent-invariant=@deepseek-ai/dsh-agent/invariant",
		"agent-loop-invariant=@deepseek-ai/dsh-agent-loop/invariant",
		"agent-loop=@deepseek-ai/dsh-agent-loop",
		"agent=@deepseek-ai/dsh-agent",
		"deepseek-llm-api-extensions=@deepseek-ai/dsh-deepseek-llm-api-extensions",
		"fs-local=@deepseek-ai/dsh-fs-local",
		"invariants=@deepseek-ai/dsh-invariants",
		"jobs=@deepseek-ai/dsh-jobs-local",
		"llm-deepseek=@deepseek-ai/dsh-llm-deepseek",
		"llm-retry=@deepseek-ai/dsh-llm-retry",
		"llm=@deepseek-ai/dsh-llm",
		"persistent-bash=@deepseek-ai/dsh-tool-bash-persistent",
		"persistent-pwsh=@deepseek-ai/dsh-tool-pwsh-persistent",
		"plugin-package-inventory-deepseek=@deepseek-ai/dsh-plugin-package-inventory-deepseek",
		"pty=@deepseek-ai/dsh-terminal",
		"sandbox-policy=@deepseek-ai/dsh-sandbox-policy",
		"sandbox=@deepseek-ai/dsh-sandbox-local",
		"scope-invariant=@deepseek-ai/dsh-scope/invariant",
		"sdk-app-startup=@deepseek-ai/dsh-sdk-app",
		"sdk-jsonrpc-server=@deepseek-ai/dsh-sdk-jsonrpc-server",
		"session-invariant=@deepseek-ai/dsh-session/invariant",
		"session-log-deepseek=@deepseek-ai/dsh-session-log-deepseek",
		"session-projection=@deepseek-ai/dsh-session-projection",
		"session-title=@deepseek-ai/dsh-session-title",
		"session=@deepseek-ai/dsh-session",
		"sessions=@deepseek-ai/dsh-session-persistence-jsonl",
		"str-replace-editor=@deepseek-ai/dsh-tool-str-replace-editor",
		"subprocess=@deepseek-ai/dsh-subprocess-local",
		"system-prompt=@deepseek-ai/dsh-system-prompt",
		"terminal-bash=@deepseek-ai/dsh-terminal-bash",
		"terminal-pwsh=@deepseek-ai/dsh-terminal-bash",
		"timer=@deepseek-ai/cordis-plugin-timer",
		"tools=@deepseek-ai/dsh-tools",
	},
	"@deepseek-ai/dsh-web-app": {
		"agent-presets=@deepseek-ai/dsh-agent-presets",
		"api-remotes=@deepseek-ai/dsh-api-remotes",
		"client-hmr=@deepseek-ai/dsh-client-hmr",
		"code-runtime=@deepseek-ai/dsh-code-runtime-worker-thread",
		"connection=@deepseek-ai/dsh-client-connection",
		"cordis-host-runner=@deepseek-ai/dsh-cordis-host-runner",
		"directory-picker=@deepseek-ai/dsh-host-directory-picker-auto",
		"file-reference-local=@deepseek-ai/dsh-file-reference-local",
		"locale=@deepseek-ai/dsh-client-locale",
		"message-feedback=@deepseek-ai/dsh-message-feedback",
		"modules=@deepseek-ai/dsh-client-modules",
		"plugin-inventory=@deepseek-ai/dsh-host-plugin-inventory",
		"session-controller=@deepseek-ai/dsh-api-session-controller",
		"session-log-download=@deepseek-ai/dsh-session-log-export",
		"session-reference=@deepseek-ai/dsh-session-reference",
		"session-stats=@deepseek-ai/dsh-session-stats",
		"session-turn-outline=@deepseek-ai/dsh-session-turn-outline",
		"settings-controller=@deepseek-ai/dsh-api-settings-controller",
		"subagent-model-selection-settings=@deepseek-ai/dsh-tool-subagent/model-selection-settings",
		"ui-chat=@deepseek-ai/dsh-client-ui-chat",
		"ui-conversation=@deepseek-ai/dsh-client-ui-conversation",
		"ui-deliverables=@deepseek-ai/dsh-client-ui-deliverables",
		"ui-settings-general=@deepseek-ai/dsh-client-ui-settings-general",
		"ui-theme=@deepseek-ai/dsh-client-ui-theme",
		"web-runtime=@deepseek-ai/dsh-web-app",
		"web-startup=@deepseek-ai/dsh-web-app/startup",
		"webserver=@deepseek-ai/dsh-host-webserver",
		"workspace-controller=@deepseek-ai/dsh-api-workspace-controller",
		"workspace=@deepseek-ai/dsh-workspace",
	},
	"preset:standard": {
		"agent-instructions=@deepseek-ai/dsh-agent-instructions",
		"command-compact=@deepseek-ai/dsh-command-compact",
		"command-goal=@deepseek-ai/dsh-command-goal",
		"compaction-basic=@deepseek-ai/dsh-compaction-basic",
		"persona=@deepseek-ai/dsh-persona",
		"plan-mode=@deepseek-ai/dsh-plan-mode",
		"skill-filesystem=@deepseek-ai/dsh-skill-filesystem",
		"tool-ask-user=@deepseek-ai/dsh-tool-ask-user",
		"tool-bash=@deepseek-ai/dsh-tool-bash",
		"tool-fs-search=@deepseek-ai/dsh-tool-fs-search",
		"tool-fs=@deepseek-ai/dsh-tool-fs",
		"tool-goal=@deepseek-ai/dsh-tool-goal",
		"tool-jobs=@deepseek-ai/dsh-tool-jobs",
		"tool-pwsh=@deepseek-ai/dsh-tool-pwsh",
		"tool-ralph=@deepseek-ai/dsh-tool-ralph",
		"tool-result-pruner=@deepseek-ai/dsh-compaction-tool-result-pruner",
		"tool-skill=@deepseek-ai/dsh-tool-skill",
		"tool-subagent-control=@deepseek-ai/dsh-tool-subagent-control",
		"tool-subagent-fork=@deepseek-ai/dsh-tool-subagent",
		"tool-subagent-list-agents=@deepseek-ai/dsh-tool-subagent-control/list-agents",
		"tool-subagent=@deepseek-ai/dsh-tool-subagent",
		"tool-todo=@deepseek-ai/dsh-tool-todo",
		"tool-web=@deepseek-ai/dsh-tool-web",
		"tool-workflow=@deepseek-ai/dsh-tool-workflow",
		"workflow-worker-thread=@deepseek-ai/dsh-workflow-worker-thread",
	},
}

func TestUpstreamContractConnectionTransport(t *testing.T) {
	root := repositoryRoot(t)
	connection := string(readTestFile(t, filepath.Join(root, "deepseek-harness/packages/client/connection/src/rpc.ts")))
	apiPath := string(readTestFile(t, filepath.Join(root, "deepseek-harness/packages/client/connection/src/api-path.ts")))
	gateway := string(readTestFile(t, filepath.Join(root, "deepseek-harness/packages/api/gateway/src/stream-protocol.ts")))
	for label, source := range map[string]string{
		"client request envelope":  "readonly type: 'client-request'",
		"server response envelope": "readonly type: 'server-response'",
		"shared API channel":       "channel: '/api'",
	} {
		if !strings.Contains(connection, source) {
			t.Fatalf("upstream Connection %s contract %q not found", label, source)
		}
	}
	if !strings.Contains(apiPath, "export const API_PATH = '/api'") {
		t.Fatal("upstream Connection API_PATH contract changed")
	}
	if !strings.Contains(gateway, "export const REMOTE_STREAM_MUX_PATH = '/api/remote.mux'") {
		t.Fatal("upstream Gateway Remote stream path contract changed")
	}
}

func TestUpstreamContractTypertInvocations(t *testing.T) {
	root := repositoryRoot(t)
	paths := upstreamTypertHostPaths(t, root)
	assertStringSet(t, "Typert Remote descriptors", remoteDescriptorRows(t, filepath.Join(root, "internal", "harness", "remote.go")), typertInvocationRows(t, paths))
}

func TestUpstreamContractOptionalProductSubagentTools(t *testing.T) {
	path := filepath.Join(repositoryRoot(t), "deepseek-harness", "packages", "preset", "agent-presets", "presets", "standard", "agent.cordis.yml")
	for _, test := range []struct {
		id, provider, toolName string
	}{
		{id: "tool-subagent-codex", provider: "codex", toolName: "subagent_codex"},
		{id: "tool-subagent-claude-code", provider: "claude-code", toolName: "subagent_claude_code"},
	} {
		row := yamlNodeByID(t, path, test.id)
		if yamlMapScalar(row, "name") != "@deepseek-ai/dsh-tool-subagent" || yamlMapScalar(row, "disabled") != "true" {
			t.Fatalf("%s package/disabled contract changed", test.id)
		}
		config := yamlMapValueNode(row, "config")
		if yamlMapScalar(config, "provider") != test.provider || yamlMapScalar(config, "toolName") != test.toolName || yamlMapScalar(config, "backgroundMode") != "one-shot" || yamlMapScalar(config, "maxDepth") != "provider-managed" {
			t.Fatalf("%s config contract changed", test.id)
		}
	}
}

func upstreamTypertHostPaths(t *testing.T, root string) []string {
	t.Helper()
	packages := filepath.Join(root, "deepseek-harness", "packages")
	var paths []string
	err := filepath.WalkDir(packages, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == "node_modules" {
			return filepath.SkipDir
		}
		if !entry.IsDir() && entry.Name() == "typert.host.js" && filepath.Base(filepath.Dir(path)) == "lib" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("upstream generated no Typert host artifacts")
	}
	sort.Strings(paths)
	return paths
}

func TestUpstreamContractForwardedRemoteEvents(t *testing.T) {
	root := repositoryRoot(t)
	data := string(readTestFile(t, filepath.Join(root, "deepseek-harness/packages/api/remotes/src/remote-events.ts")))
	start := strings.Index(data, "export const API_REMOTE_FORWARDED_EVENTS = [")
	if start < 0 {
		t.Fatal("upstream forwarded remote event allowlist not found")
	}
	block := data[start:]
	end := strings.Index(block, "] as const")
	if end < 0 {
		t.Fatal("upstream forwarded remote event allowlist is not closed")
	}
	arrayBlock := block[:end]
	upstream := make([]string, 0)
	// alpha.4 stores every forwarded event as an explicit object entry. The
	// session-controller family is intentionally listed in this application
	// allowlist rather than imported as a runtime array.
	eventPattern := regexp.MustCompile(`\bevent:\s*'([^']+)'`)
	for _, match := range eventPattern.FindAllStringSubmatch(arrayBlock, -1) {
		upstream = append(upstream, match[1])
	}
	if len(upstream) == 0 {
		t.Fatal("upstream forwarded remote event allowlist is empty")
	}
	got := make([]string, 0, len(forwardedRemoteEvents))
	for event := range forwardedRemoteEvents {
		got = append(got, event)
	}
	sort.Strings(got)
	sort.Strings(upstream)
	assertStringSet(t, "forwarded remote events", got, upstream)
}

func TestUpstreamContractDefaultProfiles(t *testing.T) {
	root := repositoryRoot(t)
	upstreamBundles, upstreamReload := upstreamProfileTemplates(t, filepath.Join(root, "deepseek-harness/packages/boot/app-boot/src/profile.ts"))
	gotBundles, gotReload := goProfileTemplates(t, filepath.Join(root, "cmd/dsh/profile.go"))
	if !reflect.DeepEqual(gotBundles, upstreamBundles) {
		t.Fatalf("default profile bundles differ\nGo:       %#v\nupstream: %#v", gotBundles, upstreamBundles)
	}
	if !reflect.DeepEqual(gotReload, upstreamReload) {
		t.Fatalf("default profile patch reload policies differ\nGo:       %#v\nupstream: %#v", gotReload, upstreamReload)
	}

	sources := backendPluginSources(t, root, upstreamBundles)
	if !reflect.DeepEqual(sources, backendPluginContract) {
		t.Fatalf("default profile backend plugin contract differs\nGo contract:\n%s\nupstream:\n%s", formatContract(backendPluginContract), formatContract(sources))
	}
}

func TestUpstreamContractSessionTelemetryConfig(t *testing.T) {
	path := filepath.Join(repositoryRoot(t), "deepseek-harness/packages/bundle/base/cordis.patch.yml")
	row := yamlNodeByID(t, path, "session-telemetry-otel")
	config := yamlMapValueNode(row, "config")
	if got, want := yamlMappingKeys(config), []string{"exporter", "mode", "processor", "shutdownTimeoutMillis"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("telemetry config fields = %v, want %v", got, want)
	}
	if got, want := yamlMappingKeys(yamlMapValueNode(config, "exporter")), []string{"compression", "timeoutMillis", "url"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("telemetry exporter fields = %v, want %v", got, want)
	}
	if got, want := yamlMappingKeys(yamlMapValueNode(config, "processor")), []string{"exportTimeoutMillis", "maxExportBatchSize", "maxQueueSize", "scheduledDelayMillis"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("telemetry processor fields = %v, want %v", got, want)
	}
	want := map[string]string{
		"shutdownTimeoutMillis":          "3000",
		"exporter.compression":           "gzip",
		"exporter.timeoutMillis":         "1000",
		"processor.scheduledDelayMillis": "10000",
		"processor.maxQueueSize":         "2048",
		"processor.maxExportBatchSize":   "2048",
		"processor.exportTimeoutMillis":  "1500",
	}
	for dotted, expected := range want {
		current := config
		for _, key := range strings.Split(dotted, ".") {
			current = yamlMapValueNode(current, key)
		}
		if current == nil || current.Value != expected {
			t.Fatalf("telemetry %s = %#v, want %q", dotted, current, expected)
		}
	}
	if mode := yamlMapValueNode(config, "mode"); mode == nil || !isJSTag(mode) || mode.Value != "process.env.DSH_TELEMETRY_MODE || 'FEEDBACK_ONLY'" {
		t.Fatalf("telemetry mode expression = %#v", mode)
	}
	if endpoint := yamlMapValueNode(yamlMapValueNode(config, "exporter"), "url"); endpoint == nil || !isJSTag(endpoint) || endpoint.Value != "process.env.DSH_TELEMETRY_OTLP_URL ?? 'https://harness-telemetry.deepseeksvc.com/v1/logs'" {
		t.Fatalf("telemetry endpoint expression = %#v", endpoint)
	}
}

func isJSTag(node *yaml.Node) bool {
	return node != nil && node.Kind == yaml.ScalarNode && (node.Tag == "!!js" || node.Tag == "tag:yaml.org,2002:js")
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	dir := moduleRoot(t)
	if _, err := os.Stat(filepath.Join(dir, "deepseek-harness", ".git")); os.IsNotExist(err) {
		t.Skip("upstream checkout is absent; run make prepare to enable upstream contract tests")
	} else if err != nil {
		t.Fatal(err)
	}
	return dir
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root not found")
		}
		dir = parent
	}
}

func readTestFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertStringSet(t *testing.T, label string, got, want []string) {
	t.Helper()
	missing, extra := difference(want, got), difference(got, want)
	if len(missing) != 0 || len(extra) != 0 {
		t.Fatalf("%s differ\nmissing: %q\nextra:   %q", label, missing, extra)
	}
}

func difference(left, right []string) []string {
	present := make(map[string]bool, len(right))
	for _, value := range right {
		present[value] = true
	}
	var result []string
	for _, value := range left {
		if !present[value] {
			result = append(result, value)
		}
	}
	return result
}

type typertWireField struct {
	name     string
	required bool
}

func typertInvocationRows(t *testing.T, paths []string) []string {
	t.Helper()
	seen := map[string]string{}
	var rows []string
	for _, path := range paths {
		data := string(readTestFile(t, path))
		inInvocations, inParameters, inInvocation := false, false, false
		namespace, method := "", ""
		var fields []typertWireField
		count := 0
		for _, line := range strings.Split(data, "\n") {
			switch {
			case !inInvocations && line == "  invocations: [":
				inInvocations = true
			case inInvocations && namespace == "" && method == "" && line == "    {":
				fields = nil
			case inInvocations && strings.HasPrefix(line, "      namespace: '"):
				namespace = jsSingleQuotedValue(t, path, line)
			case inInvocations && strings.HasPrefix(line, "      method: '"):
				method = jsSingleQuotedValue(t, path, line)
			case inInvocations && line == "      invocation: {":
				inInvocation = true
			case inInvocation && strings.HasPrefix(line, "        wire: '"):
				fields = append(fields, typertWireField{name: jsSingleQuotedValue(t, path, line), required: true})
			case inInvocation && line == "      },":
				inInvocation = false
			case inInvocations && line == "      parameters: [":
				inParameters = true
			case inParameters && strings.HasPrefix(line, "          wire: '"):
				fields = append(fields, typertWireField{name: jsSingleQuotedValue(t, path, line), required: true})
			case inParameters && line == "          acceptsUndefined: true,":
				if len(fields) == 0 {
					t.Fatalf("%s: acceptsUndefined without a wire field", path)
				}
				fields[len(fields)-1].required = false
			case inParameters && line == "      ],":
				inParameters = false
			case inInvocations && line == "    },":
				if namespace == "" || method == "" {
					t.Fatalf("%s: incomplete Typert invocation", path)
				}
				endpoint := namespace + "/" + method
				if previous := seen[endpoint]; previous != "" {
					t.Fatalf("duplicate Typert endpoint %q in %s and %s", endpoint, previous, path)
				}
				seen[endpoint] = path
				rows = append(rows, formatRemoteContract(endpoint, fields))
				count++
				namespace, method, fields = "", "", nil
			case inInvocations && line == "  ],":
				inInvocations = false
			}
		}
		if inInvocations || count == 0 {
			t.Fatalf("%s: Typert invocations were not parsed completely", path)
		}
	}
	sort.Strings(rows)
	return rows
}

func jsSingleQuotedValue(t *testing.T, path, line string) string {
	t.Helper()
	start := strings.IndexByte(line, '\'')
	end := strings.LastIndexByte(line, '\'')
	if start < 0 || end <= start {
		t.Fatalf("%s: invalid generated Typert field %q", path, line)
	}
	return line[start+1 : end]
}

func remoteDescriptorRows(t *testing.T, path string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var descriptors *ast.CompositeLit
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.VAR {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 || value.Names[0].Name != "remoteDescriptors" || len(value.Values) != 1 {
				continue
			}
			descriptors, _ = value.Values[0].(*ast.CompositeLit)
		}
	}
	if descriptors == nil {
		t.Fatalf("%s: remoteDescriptors declaration not found", path)
	}
	rows := make([]string, 0, len(descriptors.Elts))
	for _, entry := range descriptors.Elts {
		pair, ok := entry.(*ast.KeyValueExpr)
		if !ok {
			t.Fatalf("%s: remote descriptor entry is %T, want key-value", path, entry)
		}
		endpoint := astString(t, path, pair.Key)
		// Gateway-internal carrier endpoints are intentionally absent from the
		// generated Typert business descriptor set.
		if strings.HasPrefix(endpoint, "$") {
			continue
		}
		descriptor, ok := pair.Value.(*ast.CompositeLit)
		if !ok {
			t.Fatalf("%s: remote descriptor %q is %T, want composite literal", path, endpoint, pair.Value)
		}
		allowed := astStringField(t, path, descriptor, "allowed")
		requiredFields := astStringField(t, path, descriptor, "required")
		required := make(map[string]bool, len(requiredFields))
		for _, field := range requiredFields {
			required[field] = true
		}
		fields := make([]typertWireField, 0, len(allowed))
		allowedSet := make(map[string]bool, len(allowed))
		for _, field := range allowed {
			allowedSet[field] = true
			fields = append(fields, typertWireField{name: field, required: required[field]})
		}
		for field := range required {
			if !allowedSet[field] {
				t.Fatalf("remote descriptor %q requires disallowed field %q", endpoint, field)
			}
		}
		rows = append(rows, formatRemoteContract(endpoint, fields))
	}
	sort.Strings(rows)
	return rows
}

func astStringField(t *testing.T, path string, literal *ast.CompositeLit, name string) []string {
	t.Helper()
	for _, entry := range literal.Elts {
		pair, ok := entry.(*ast.KeyValueExpr)
		if !ok || astIdent(pair.Key) != name {
			continue
		}
		values, ok := pair.Value.(*ast.CompositeLit)
		if !ok {
			t.Fatalf("%s: remote descriptor field %q is %T, want string slice", path, name, pair.Value)
		}
		result := make([]string, 0, len(values.Elts))
		for _, item := range values.Elts {
			result = append(result, astString(t, path, item))
		}
		return result
	}
	return nil
}

func astIdent(expression ast.Expr) string {
	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return identifier.Name
}

func astString(t *testing.T, path string, expression ast.Expr) string {
	t.Helper()
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		t.Fatalf("%s: expected string literal, got %T", path, expression)
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func formatRemoteContract(endpoint string, fields []typertWireField) string {
	values := make([]string, len(fields))
	for index, field := range fields {
		suffix := "?"
		if field.required {
			suffix = "!"
		}
		values[index] = field.name + suffix
	}
	sort.Strings(values)
	return endpoint + "(" + strings.Join(values, ",") + ")"
}

func upstreamProfileTemplates(t *testing.T, path string) (map[string][]string, map[string]string) {
	t.Helper()
	data := string(readTestFile(t, path))
	profiles := map[string][]string{}
	reload := map[string]string{}
	start := strings.Index(data, "export const PROFILE_TEMPLATES")
	if start < 0 {
		t.Fatal("upstream PROFILE_TEMPLATES not found")
	}
	block, ok := balancedTSObject(data[start:])
	if !ok {
		t.Fatal("upstream PROFILE_TEMPLATES object is not closed")
	}
	rowPattern := regexp.MustCompile(`(?ms)^\s*['\"]?([A-Za-z0-9_-]+)['\"]?\s*:\s*\{\s*bundles\s*:\s*\[([^\]]*)\]\s*,\s*patchReload\s*:\s*'([^']+)'\s*,?\s*\}`)
	stringPattern := regexp.MustCompile(`'([^']+)'`)
	for _, row := range rowPattern.FindAllStringSubmatch(block, -1) {
		for _, value := range stringPattern.FindAllStringSubmatch(row[2], -1) {
			profiles[row[1]] = append(profiles[row[1]], value[1])
		}
		reload[row[1]] = row[3]
	}
	if len(profiles) == 0 || len(profiles) != len(reload) {
		t.Fatal("upstream PROFILE_TEMPLATES rows were not parsed")
	}
	return profiles, reload
}

func goProfileTemplates(t *testing.T, path string) (map[string][]string, map[string]string) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	constants := map[string]string{}
	profiles := map[string][]string{}
	reload := map[string]string{}
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			if general.Tok == token.CONST {
				for index, name := range value.Names {
					if index >= len(value.Values) {
						continue
					}
					if literal, ok := value.Values[index].(*ast.BasicLit); ok && literal.Kind == token.STRING {
						constants[name.Name], _ = strconv.Unquote(literal.Value)
					}
				}
				continue
			}
			if len(value.Names) != 1 || len(value.Values) != 1 {
				continue
			}
			name := value.Names[0].Name
			outer, ok := value.Values[0].(*ast.CompositeLit)
			if !ok {
				continue
			}
			switch name {
			case "profileTemplates":
				for _, element := range outer.Elts {
					pair, ok := element.(*ast.KeyValueExpr)
					if !ok {
						t.Fatalf("profileTemplates entry is %T, want key-value", element)
					}
					key := goStringExpr(t, path, pair.Key)
					inner, ok := pair.Value.(*ast.CompositeLit)
					if !ok {
						t.Fatalf("profileTemplates[%q] is %T, want string slice", key, pair.Value)
					}
					for _, item := range inner.Elts {
						identifier, ok := item.(*ast.Ident)
						if !ok || constants[identifier.Name] == "" {
							t.Fatalf("profileTemplates[%q] has unresolved bundle %T", key, item)
						}
						profiles[key] = append(profiles[key], constants[identifier.Name])
					}
				}
			case "profilePatchReload":
				for _, element := range outer.Elts {
					pair, ok := element.(*ast.KeyValueExpr)
					if !ok {
						t.Fatalf("profilePatchReload entry is %T, want key-value", element)
					}
					reload[goStringExpr(t, path, pair.Key)] = goStringExpr(t, path, pair.Value)
				}
			}
		}
	}
	if len(profiles) == 0 || len(profiles) != len(reload) {
		t.Fatal("Go profile templates not found")
	}
	return profiles, reload
}

func goStringExpr(t *testing.T, path string, expression ast.Expr) string {
	t.Helper()
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		t.Fatalf("%s: expected string literal, got %T", path, expression)
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil {
		t.Fatalf("%s: invalid string literal: %v", path, err)
	}
	return value
}

// balancedTSObject returns the contents between the first top-level braces of
// a TypeScript object declaration. It handles quoted strings and comments so
// nested profile objects do not terminate the scan early.
func balancedTSObject(source string) (string, bool) {
	open := strings.IndexByte(source, '{')
	if open < 0 {
		return "", false
	}
	depth := 0
	inSingle, inDouble, inTemplate, lineComment, blockComment, escaped := false, false, false, false, false, false
	for index := open; index < len(source); index++ {
		current := source[index]
		if lineComment {
			if current == '\n' {
				lineComment = false
			}
			continue
		}
		if blockComment {
			if current == '*' && index+1 < len(source) && source[index+1] == '/' {
				blockComment = false
				index++
			}
			continue
		}
		if inSingle || inDouble || inTemplate {
			if escaped {
				escaped = false
				continue
			}
			if current == '\\' {
				escaped = true
				continue
			}
			if (inSingle && current == '\'') || (inDouble && current == '"') || (inTemplate && current == '`') {
				inSingle, inDouble, inTemplate = false, false, false
			}
			continue
		}
		if current == '/' && index+1 < len(source) {
			switch source[index+1] {
			case '/':
				lineComment = true
				index++
				continue
			case '*':
				blockComment = true
				index++
				continue
			}
		}
		switch current {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inTemplate = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return source[open+1 : index], true
			}
		}
	}
	return "", false
}

type bundleManifest struct {
	Name string `json:"name"`
	DSH  struct {
		Bundle struct {
			Patch string `json:"patch"`
		} `json:"bundle"`
	} `json:"dsh"`
}

func backendPluginSources(t *testing.T, root string, profiles map[string][]string) map[string][]string {
	t.Helper()
	upstream := filepath.Join(root, "deepseek-harness")
	packageDirs := packageDirectories(t, filepath.Join(upstream, "packages"))
	sources := map[string][]string{}
	bundles := map[string]bool{}
	for _, names := range profiles {
		for _, name := range names {
			bundles[name] = true
		}
	}
	for name := range bundles {
		dir := packageDirs[name]
		if dir == "" {
			t.Fatalf("bundle package %q not found", name)
		}
		var manifest bundleManifest
		if err := json.Unmarshal(readTestFile(t, filepath.Join(dir, "package.json")), &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.DSH.Bundle.Patch == "" {
			t.Fatalf("bundle package %q declares no patch", name)
		}
		sources[name] = backendRows(t, filepath.Join(dir, manifest.DSH.Bundle.Patch), packageDirs)
	}

	webDir := packageDirs["@deepseek-ai/dsh-web-app"]
	defaultPreset := yamlScalarByID(t, filepath.Join(webDir, "cordis.patch.yml"), "agent-presets", "config", "default")
	if defaultPreset == "" {
		t.Fatal("web default agent preset not found")
	}
	presetPath := filepath.Join(upstream, "packages", "preset", "agent-presets", "presets", defaultPreset, "agent.cordis.yml")
	sources["preset:"+defaultPreset] = backendRows(t, presetPath, packageDirs)
	return sources
}

func packageDirectories(t *testing.T, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() != "package.json" {
			return nil
		}
		var manifest struct {
			Name string `json:"name"`
		}
		data, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(data, &manifest) != nil || manifest.Name == "" {
			return nil
		}
		result[manifest.Name] = filepath.Dir(path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func backendRows(t *testing.T, path string, packageDirs map[string]string) []string {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal(readTestFile(t, path), &document); err != nil {
		t.Fatal(err)
	}
	var rows []string
	walkPluginRows(&document, false, func(id, name string) {
		packageName := rootPackageName(name)
		if packageName == "cordis:group" || frontendOnlyPackage(packageDirs[packageName]) {
			return
		}
		rows = append(rows, id+"="+name)
	})
	sort.Strings(rows)
	return rows
}

func walkPluginRows(node *yaml.Node, disabled bool, visit func(id, name string)) {
	if node == nil {
		return
	}
	if node.Kind == yaml.MappingNode {
		disabled = disabled || yamlMapScalar(node, "disabled") == "true"
		if !disabled {
			id, name := yamlMapScalar(node, "id"), yamlMapScalar(node, "name")
			if id != "" && name != "" {
				visit(id, name)
			}
		}
	}
	for _, child := range node.Content {
		walkPluginRows(child, disabled, visit)
	}
}

func yamlMapScalar(node *yaml.Node, key string) string {
	if node == nil || node.Kind != yaml.MappingNode {
		return ""
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key && node.Content[index+1].Kind == yaml.ScalarNode {
			return node.Content[index+1].Value
		}
	}
	return ""
}

func yamlScalarByID(t *testing.T, path, id string, keys ...string) string {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal(readTestFile(t, path), &document); err != nil {
		t.Fatal(err)
	}
	var value string
	var visit func(*yaml.Node)
	visit = func(node *yaml.Node) {
		if value != "" || node == nil {
			return
		}
		if node.Kind == yaml.MappingNode && yamlMapScalar(node, "id") == id {
			current := node
			for _, key := range keys {
				current = yamlMapValueNode(current, key)
				if current == nil {
					break
				}
			}
			if current != nil && current.Kind == yaml.ScalarNode {
				value = current.Value
				return
			}
		}
		for _, child := range node.Content {
			visit(child)
		}
	}
	visit(&document)
	return value
}

func yamlMapValueNode(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key {
			return node.Content[index+1]
		}
	}
	return nil
}

func yamlNodeByID(t *testing.T, path, id string) *yaml.Node {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal(readTestFile(t, path), &document); err != nil {
		t.Fatal(err)
	}
	var found *yaml.Node
	var visit func(*yaml.Node)
	visit = func(node *yaml.Node) {
		if found != nil || node == nil {
			return
		}
		if node.Kind == yaml.MappingNode && yamlMapScalar(node, "id") == id {
			found = node
			return
		}
		for _, child := range node.Content {
			visit(child)
		}
	}
	visit(&document)
	if found == nil {
		t.Fatalf("YAML row %q not found in %s", id, path)
	}
	return found
}

func yamlMappingKeys(node *yaml.Node) []string {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	keys := make([]string, 0, len(node.Content)/2)
	for index := 0; index+1 < len(node.Content); index += 2 {
		keys = append(keys, node.Content[index].Value)
	}
	sort.Strings(keys)
	return keys
}

func rootPackageName(name string) string {
	if strings.HasPrefix(name, "@") {
		parts := strings.Split(name, "/")
		if len(parts) >= 2 {
			return strings.Join(parts[:2], "/")
		}
	}
	if index := strings.IndexByte(name, '/'); index >= 0 {
		return name[:index]
	}
	return name
}

func frontendOnlyPackage(dir string) bool {
	if dir == "" {
		return false
	}
	// Browser-only packages keep an empty Host apply so Loader can still list them.
	data, err := os.ReadFile(filepath.Join(dir, "src", "index.ts"))
	return err == nil && noHostApplyPattern.Match(data)
}

func formatContract(values map[string][]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	out.WriteString("map[string][]string{\n")
	for _, key := range keys {
		fmt.Fprintf(&out, "\t%q: {\n", key)
		for _, value := range values[key] {
			fmt.Fprintf(&out, "\t\t%q,\n", value)
		}
		out.WriteString("\t},\n")
	}
	out.WriteString("}")
	return out.String()
}
