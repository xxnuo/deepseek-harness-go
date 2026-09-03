package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	harness "github.com/xxnuo/deepseek-harness-go"
)

func stringPointerValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func intPointerValue(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func TestRuntimeProfileMapsE2BHooksWebAndAllPromptsTitle(t *testing.T) {
	composed := testComposition(t, `
- id: e2b-owner
  name: '@deepseek-ai/dsh-e2b'
  config: {apiKey: e2b-key, cwd: /workspace, timeoutMs: 1234}
- id: e2b-fs
  name: '@deepseek-ai/dsh-fs-e2b'
- id: e2b-subprocess
  name: '@deepseek-ai/dsh-subprocess-e2b'
- id: claude-hooks
  name: '@deepseek-ai/dsh-hooks-claude-code'
  config: {configPath: claude-hooks.json, pluginRoot: /plugin, projectDir: /project, defaultTimeoutMs: 2000, stderrSummaryMaxChars: 321}
- id: codex-hooks
  name: '@deepseek-ai/dsh-hooks-codex'
  config: {configPath: codex-hooks.json, model: codex-model}
- id: title-all
  name: '@deepseek-ai/dsh-session-title-all-prompts-llm'
  config: {targetWords: 6, targetCjkCharacters: 12, maxInputBytes: 2048, maxOutputTokens: 32, timeoutMs: 3456, provider: title-provider, model: title-model}
- id: persist
  name: '@deepseek-ai/dsh-session-persistence-jsonl'
- id: web
  name: '@deepseek-ai/dsh-web'
  config: {searchProvider: exa, fetchProvider: http}
- id: exa
  name: '@deepseek-ai/dsh-web-search-exa'
  config: {apiKey: exa-key, baseURL: https://exa.test, searchType: neural, numResults: 9, highlightsPerResult: 2}
- id: deepseek-search
  name: '@deepseek-ai/dsh-web-search-deepseek'
  config: {apiKey: search-key, apiKeyEnv: SEARCH_KEY, baseURL: https://deepseek-search.test/v1, model: search-model, apiVersion: '2025-01-01', maxTokens: 123, maxUses: 4}
- id: perplexity
  name: '@deepseek-ai/dsh-web-search-perplexity'
  config: {apiKey: pplx-key, baseURL: https://pplx.test, model: sonar-pro, maxTokens: 2048, searchRecency: week}
- id: fetch-http
  name: '@deepseek-ai/dsh-web-fetch-http'
  config: {maxUrlLength: 4096, maxResponseBytes: 12345, maxBodyChars: 678, timeoutMs: 1234, maxRedirects: 0, userAgent: custom-fetch-agent}
- id: tool-web
  name: '@deepseek-ai/dsh-tool-web'
  config: {search: false, fetch: true, searchMaxResults: 7, searchMaxQueries: 3, fetchTimeoutMs: 1500, searchTimeoutMs: 1600, fetchMaxOutputChars: 900}
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	if cfg.E2B == nil || cfg.E2B.APIKey != "e2b-key" || cfg.E2B.CWD != "/workspace" || cfg.E2B.Timeout != 1234*time.Millisecond {
		t.Fatalf("E2B config = %#v", cfg.E2B)
	}
	if len(cfg.Hooks) != 2 || cfg.Hooks[0].Dialect != harness.HookDialectClaudeCode || cfg.Hooks[0].DefaultTimeout != 2*time.Second || cfg.Hooks[0].StderrSummaryMaxChars != 321 || cfg.Hooks[1].Dialect != harness.HookDialectCodex || cfg.Hooks[1].Model != "codex-model" {
		t.Fatalf("hook configs = %#v", cfg.Hooks)
	}
	if !cfg.SessionTitleLLM.Enabled || cfg.SessionTitleLLM.Automatic != harness.SessionTitleAllPrompts || cfg.SessionTitleLLM.Provider != "title-provider" || cfg.SessionTitleLLM.Model != "title-model" || cfg.SessionTitleLLM.Timeout != 3456*time.Millisecond {
		t.Fatalf("title config = %#v", cfg.SessionTitleLLM)
	}
	if !cfg.Persist || cfg.SessionStore != nil || cfg.WebSearchProvider != "exa" || cfg.WebFetchProvider != "http" {
		t.Fatalf("runtime selection = %#v", cfg)
	}
	if cfg.ExaSearch.APIKey != "exa-key" || cfg.ExaSearch.BaseURL != "https://exa.test" || cfg.ExaSearch.SearchType != "neural" || cfg.ExaSearch.NumResults != 9 || cfg.ExaSearch.HighlightsPerResult != 2 {
		t.Fatalf("Exa config = %#v", cfg.ExaSearch)
	}
	deepseek := cfg.DeepSeekWebSearch
	if deepseek.APIKey == nil || *deepseek.APIKey != "search-key" || deepseek.APIKeyEnv == nil || *deepseek.APIKeyEnv != "SEARCH_KEY" || deepseek.BaseURL == nil || *deepseek.BaseURL != "https://deepseek-search.test/v1" || deepseek.Model == nil || *deepseek.Model != "search-model" || deepseek.APIVersion == nil || *deepseek.APIVersion != "2025-01-01" || deepseek.MaxTokens == nil || *deepseek.MaxTokens != 123 || deepseek.MaxUses == nil || *deepseek.MaxUses != 4 {
		t.Fatalf("DeepSeek web search config = apiKey=%v apiKeyEnv=%v baseURL=%v model=%v apiVersion=%v maxTokens=%v maxUses=%v", stringPointerValue(deepseek.APIKey), stringPointerValue(deepseek.APIKeyEnv), stringPointerValue(deepseek.BaseURL), stringPointerValue(deepseek.Model), stringPointerValue(deepseek.APIVersion), intPointerValue(deepseek.MaxTokens), intPointerValue(deepseek.MaxUses))
	}
	if cfg.PerplexitySearch.APIKey != "pplx-key" || cfg.PerplexitySearch.BaseURL != "https://pplx.test" || cfg.PerplexitySearch.Model != "sonar-pro" || cfg.PerplexitySearch.MaxTokens != 2048 || cfg.PerplexitySearch.SearchRecency != "week" {
		t.Fatalf("Perplexity config = %#v", cfg.PerplexitySearch)
	}
	if cfg.HTTPWebFetch.MaxURLLength != 4096 || cfg.HTTPWebFetch.MaxResponseBytes != 12345 || cfg.HTTPWebFetch.MaxBodyChars != 678 || cfg.HTTPWebFetch.Timeout != 1234*time.Millisecond || cfg.HTTPWebFetch.MaxRedirects != 0 || cfg.HTTPWebFetch.UserAgent != "custom-fetch-agent" {
		t.Fatalf("HTTP fetch config = %#v", cfg.HTTPWebFetch)
	}
	if cfg.WebTools == nil || cfg.WebTools.SearchEnabled || !cfg.WebTools.FetchEnabled || cfg.WebTools.SearchMaxResults != 7 || cfg.WebTools.SearchMaxQueries != 3 || cfg.WebTools.FetchTimeout != 1500*time.Millisecond || cfg.WebTools.SearchTimeout != 1600*time.Millisecond || cfg.WebTools.FetchMaxOutputChars != 900 {
		t.Fatalf("web tool config = %#v", cfg.WebTools)
	}

	cfg.E2B = nil
	cfg.Hooks = nil
	cfg.Persist = false
	cfg.SessionTitleLLM.Enabled = false
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if !hasTool(engine.ListTools(), "web_fetch") {
		t.Fatal("HTTP fetch profile did not register web_fetch")
	}
}

func TestRuntimeProfileMapsOptionalToolPolicies(t *testing.T) {
	composed := testComposition(t, `
- id: timeout
  name: '@deepseek-ai/dsh-tool-call-timeout-policy'
- id: todo
  name: '@deepseek-ai/dsh-tool-todo'
  config: {allowParallelInProgress: false}
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	if cfg.ToolTimeoutPolicyEnabled == nil || !*cfg.ToolTimeoutPolicyEnabled {
		t.Fatal("timeout policy should be enabled")
	}
	if cfg.TodoAllowParallelInProgress == nil || *cfg.TodoAllowParallelInProgress {
		t.Fatal("todo parallel policy should be disabled")
	}
}

func TestRuntimeProfileRequiresTodoParallelPolicy(t *testing.T) {
	composed := testComposition(t, `
- id: todo
  name: '@deepseek-ai/dsh-tool-todo'
`)
	if err := composed.validate(); err == nil || !strings.Contains(err.Error(), "allowParallelInProgress is required") {
		t.Fatalf("missing todo policy error = %v", err)
	}
}

func TestRuntimeProfileMapsJobDeliveryPolicy(t *testing.T) {
	composed := testComposition(t, `
- id: jobs
  name: '@deepseek-ai/dsh-tool-jobs'
  config: {waitTimeoutMs: 1200, maxWaitTimeoutMs: 5000, completionDelivery: quiet, maxConsecutiveWakes: 4}
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	config := engineConfig(&profileLoader{home: t.TempDir()}, composed).Jobs
	if config.WaitTimeoutMs != 1200*time.Millisecond || config.MaxWaitTimeoutMs != 5*time.Second || config.CompletionDelivery != "quiet" || config.MaxConsecutiveWakes != 4 {
		t.Fatalf("jobs config = %#v", config)
	}
}

func TestClientHMRProfileConfigMapsPollInterval(t *testing.T) {
	composed := testComposition(t, `
- id: client-hmr
  name: '@deepseek-ai/dsh-client-hmr'
  config: {pollIntervalMs: 17}
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	if got := engineConfig(&profileLoader{home: t.TempDir()}, composed).ClientHMRPollInterval; got != 17*time.Millisecond {
		t.Fatalf("client HMR poll interval = %s", got)
	}
	invalid := testComposition(t, `
- id: client-hmr
  name: '@deepseek-ai/dsh-client-hmr'
  config: {pollIntervalMs: 0}
`)
	if err := invalid.validate(); err == nil || !strings.Contains(err.Error(), "pollIntervalMs must be a positive integer") {
		t.Fatalf("invalid client HMR config = %v", err)
	}
}

func TestFileReferenceProfileConfigPreservesExplicitLimits(t *testing.T) {
	composed := testComposition(t, `
- id: file-reference-local
  name: '@deepseek-ai/dsh-file-reference-local'
  config:
    maxResults: 7
    maxEntries: 321
    excludedDirectories: [.git, vendor]
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	config := engineConfig(&profileLoader{home: t.TempDir()}, composed).FileReference
	if config.MaxResults != 7 || config.MaxEntries != 321 || strings.Join(config.ExcludedDirectories, ",") != ".git,vendor" {
		t.Fatalf("file-reference-local config = %#v", config)
	}
}

func TestAgentTeamProfileConfigMapsLimitsAndProviders(t *testing.T) {
	composed := testComposition(t, `
- id: agent-team
  name: '@deepseek-ai/dsh-experimental-agent-team'
  config:
    maxMembers: 3
    maxTasks: 21
    maxPendingMessagesPerMember: 5
    maxMessageBytes: 4096
    disposalTimeoutMs: 2500
- id: tool-agent-team
  name: '@deepseek-ai/dsh-experimental-tool-agent-team'
  config:
    freshProvider: custom-spawn
    forkProvider: custom-fork
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	config := engineConfig(&profileLoader{home: t.TempDir()}, composed).AgentTeams
	if config == nil || config.MaxMembers != 3 || config.MaxTasks != 21 || config.MaxPendingMessagesPerMember != 5 || config.MaxMessageBytes != 4096 || config.DisposalTimeout != 2500*time.Millisecond || config.FreshProvider != "custom-spawn" || config.ForkProvider != "custom-fork" {
		t.Fatalf("Agent Teams config = %#v", config)
	}
}

func TestSQLiteStorageProfileMountsAndPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage.sqlite")
	source := fmt.Sprintf(`
- id: storage
  name: '@deepseek-ai/dsh-storage'
- id: sqlite
  name: '@deepseek-ai/dsh-storage-sqlite'
  config: {path: %q, journalMode: delete}
- id: domain
  name: '@deepseek-ai/dsh-storage-domain'
  config: {backend: sqlite, routes: {audit: sqlite}}
`, path)
	newEngine := func() *harness.Engine {
		composed := testComposition(t, source)
		if err := composed.validate(); err != nil {
			t.Fatal(err)
		}
		cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
		if cfg.Storage == nil || cfg.Storage.SQLite == nil || cfg.Storage.SQLite.Path != path || cfg.Storage.SQLite.JournalMode != harness.SQLiteJournalDelete || cfg.Storage.Domain == nil || cfg.Storage.Domain.Backend != "sqlite" || cfg.Storage.Domain.Routes["audit"] != "sqlite" {
			t.Fatalf("storage runtime config = %#v", cfg.Storage)
		}
		cfg.Provider, cfg.Model = "echo", "echo"
		cfg.Workspace = t.TempDir()
		cfg.SessionTitleLLM.Enabled = false
		engine, err := harness.New(harness.WithConfig(cfg))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Storage().Backend.Get("sqlite"); err != nil {
			_ = engine.Close()
			t.Fatal(err)
		}
		if form, err := engine.Storage().Form("domain"); err != nil || form != engine.StorageDomain() {
			_ = engine.Close()
			t.Fatalf("domain form = %T, %v", form, err)
		}
		return engine
	}
	spec := harness.DomainSpec{
		Name: "audit", Version: 1,
		Tables: map[string]harness.DomainTableSpec{"records": {Parse: harness.JSONDomainParser[map[string]string](nil)}},
	}
	engine := newEngine()
	domain, err := engine.StorageDomain().Open(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	table, err := domain.Table("records")
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Put(context.Background(), "first", map[string]string{"value": "persisted"}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := newEngine()
	defer reopened.Close()
	domain, err = reopened.StorageDomain().Open(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	table, err = domain.Table("records")
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := table.Get("first")
	if err != nil || !ok || value.(map[string]string)["value"] != "persisted" {
		t.Fatalf("reopened record = %#v, ok=%v, err=%v", value, ok, err)
	}
}

func TestRuntimeProfileRejectsUnsupportedOrIncompleteConfig(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{"e2b adapter without owner", "- id: fs\n  name: '@deepseek-ai/dsh-fs-e2b'\n", "require an enabled"},
		{"hook path", "- id: hooks\n  name: '@deepseek-ai/dsh-hooks-codex'\n", "configPath is required"},
		{"title route pair", "- id: title\n  name: '@deepseek-ai/dsh-session-title-all-prompts-llm'\n  config: {targetWords: 5, targetCjkCharacters: 10, maxInputBytes: 1024, maxOutputTokens: 32, timeoutMs: 1000, provider: only-provider}\n", "supplied together"},
		{"storage service", "- id: sqlite\n  name: '@deepseek-ai/dsh-storage-sqlite'\n  config: {path: ':memory:'}\n", "require an enabled"},
		{"storage route", "- id: storage\n  name: '@deepseek-ai/dsh-storage'\n- id: domain\n  name: '@deepseek-ai/dsh-storage-domain'\n  config: {backend: sqlite}\n", "is not mounted"},
		{"HTTP limits", "- id: fetch\n  name: '@deepseek-ai/dsh-web-fetch-http'\n  config: {timeoutMs: 0}\n", "must be positive"},
		{"web query limit", "- id: tools\n  name: '@deepseek-ai/dsh-tool-web'\n  config: {searchMaxQueries: 0}\n", "must be positive integers"},
		{"file reference max results", "- id: refs\n  name: '@deepseek-ai/dsh-file-reference-local'\n  config: {maxResults: 0}\n", "maxResults must be a positive safe integer"},
		{"file reference max entries", "- id: refs\n  name: '@deepseek-ai/dsh-file-reference-local'\n  config: {maxEntries: 0}\n", "maxEntries must be a positive safe integer"},
		{"file reference excluded directory", "- id: refs\n  name: '@deepseek-ai/dsh-file-reference-local'\n  config: {excludedDirectories: ['bad/path']}\n", "must be non-empty directory basenames"},
		{"Agent Teams member limit", "- id: teams\n  name: '@deepseek-ai/dsh-experimental-agent-team'\n  config: {maxMembers: 0}\n", "maxMembers must be a positive safe integer"},
		{"Agent Teams fractional task limit", "- id: teams\n  name: '@deepseek-ai/dsh-experimental-agent-team'\n  config: {maxTasks: 1.5}\n", "invalid config"},
		{"Agent Teams tool owner", "- id: tools\n  name: '@deepseek-ai/dsh-experimental-tool-agent-team'\n", "requires an enabled"},
		{"Agent Teams provider", "- id: teams\n  name: '@deepseek-ai/dsh-experimental-agent-team'\n- id: tools\n  name: '@deepseek-ai/dsh-experimental-tool-agent-team'\n  config: {freshProvider: ''}\n", "freshProvider must be non-empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			composed := testComposition(t, test.source)
			if err := composed.validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validate() = %v, want %q", err, test.want)
			}
		})
	}
}

func TestProfileWebProviderSelectionDefersToRuntime(t *testing.T) {
	t.Setenv("DSH_WEB_SEARCH_PROVIDER", "env-provider")
	t.Setenv("DSH_WEB_FETCH_PROVIDER", "")
	composed := testComposition(t, "- id: web\n  name: '@deepseek-ai/dsh-web'\n")
	if err := composed.validate(); err != nil {
		t.Fatalf("profile with no provider config was rejected: %v", err)
	}
	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	if cfg.WebSearchProvider != "env-provider" || cfg.WebFetchProvider != "" {
		t.Fatalf("environment provider selection = %q/%q", cfg.WebSearchProvider, cfg.WebFetchProvider)
	}

	composed = testComposition(t, "- id: web\n  name: '@deepseek-ai/dsh-web'\n  config: {searchProvider: custom-search, fetchProvider: custom-fetch}\n")
	if err := composed.validate(); err != nil {
		t.Fatalf("custom provider profile was rejected during composition: %v", err)
	}
	cfg = engineConfig(&profileLoader{home: t.TempDir()}, composed)
	if cfg.WebSearchProvider != "custom-search" || cfg.WebFetchProvider != "custom-fetch" {
		t.Fatalf("profile provider selection = %q/%q", cfg.WebSearchProvider, cfg.WebFetchProvider)
	}

	t.Setenv("DSH_WEB_SEARCH_PROVIDER", "")
	composed = testComposition(t, "- id: web\n  name: '@deepseek-ai/dsh-web'\n")
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	cfg = engineConfig(&profileLoader{home: t.TempDir()}, composed)
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if got := engine.Config().WebSearchProvider; got != "" {
		t.Fatalf("runtime normalized an unset profile provider to %q", got)
	}
}

func hasTool(tools []harness.ToolSchema, name string) bool {
	for _, tool := range tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}
