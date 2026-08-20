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
- id: perplexity
  name: '@deepseek-ai/dsh-web-search-perplexity'
  config: {apiKey: pplx-key, baseURL: https://pplx.test, model: sonar-pro, maxTokens: 2048, searchRecency: week}
- id: fetch-http
  name: '@deepseek-ai/dsh-web-fetch-http'
  config: {maxUrlLength: 4096, maxResponseBytes: 12345, maxBodyChars: 678, timeoutMs: 1234, maxRedirects: 0, userAgent: custom-fetch-agent}
- id: tool-web
  name: '@deepseek-ai/dsh-tool-web'
  config: {search: false, fetch: true, searchMaxResults: 7, fetchTimeoutMs: 1500, searchTimeoutMs: 1600, fetchMaxOutputChars: 900}
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
	if cfg.PerplexitySearch.APIKey != "pplx-key" || cfg.PerplexitySearch.BaseURL != "https://pplx.test" || cfg.PerplexitySearch.Model != "sonar-pro" || cfg.PerplexitySearch.MaxTokens != 2048 || cfg.PerplexitySearch.SearchRecency != "week" {
		t.Fatalf("Perplexity config = %#v", cfg.PerplexitySearch)
	}
	if cfg.HTTPWebFetch.MaxURLLength != 4096 || cfg.HTTPWebFetch.MaxResponseBytes != 12345 || cfg.HTTPWebFetch.MaxBodyChars != 678 || cfg.HTTPWebFetch.Timeout != 1234*time.Millisecond || cfg.HTTPWebFetch.MaxRedirects != 0 || cfg.HTTPWebFetch.UserAgent != "custom-fetch-agent" {
		t.Fatalf("HTTP fetch config = %#v", cfg.HTTPWebFetch)
	}
	if cfg.WebTools == nil || cfg.WebTools.SearchEnabled || !cfg.WebTools.FetchEnabled || cfg.WebTools.SearchMaxResults != 7 || cfg.WebTools.FetchTimeout != 1500*time.Millisecond || cfg.WebTools.SearchTimeout != 1600*time.Millisecond || cfg.WebTools.FetchMaxOutputChars != 900 {
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

func TestSQLiteSessionProfilePersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.sqlite")
	source := fmt.Sprintf("- id: sqlite\n  name: '@deepseek-ai/dsh-session-persistence-sqlite'\n  config: {path: %q, journalMode: delete, preparedSessionCacheSize: 9, writeBatchMaxDelayMs: 123}\n", path)
	newConfig := func() harness.Config {
		composed := testComposition(t, source)
		if err := composed.validate(); err != nil {
			t.Fatal(err)
		}
		cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
		store, ok := cfg.SessionStore.(*harness.SQLiteSessionStore)
		if !ok || store.Options().PreparedSessionCacheSize != 9 || store.Options().WriteBatchMaxDelay != 123*time.Millisecond {
			t.Fatalf("SQLite coordinator options = %#v", cfg.SessionStore)
		}
		cfg.Provider, cfg.Model = "echo", "echo"
		cfg.Workspace = t.TempDir()
		cfg.SessionTitleLLM.Enabled = false
		return cfg
	}

	cfg := newConfig()
	if !cfg.Persist || cfg.SessionStore == nil {
		t.Fatalf("SQLite persistence config = %#v", cfg)
	}
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		_ = cfg.SessionStore.Close()
		t.Fatal(err)
	}
	id, err := engine.CreateSession(context.Background(), cfg.Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if text, err := engine.Run(context.Background(), id, harness.PromptRequest{Mode: "queue", Content: []harness.PromptContentPart{{Type: "text", Text: "persist me"}}, Literal: true}); err != nil || text != "persist me" {
		t.Fatalf("Run() = %q, %v", text, err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedConfig := newConfig()
	reopened, err := harness.New(harness.WithConfig(reopenedConfig))
	if err != nil {
		_ = reopenedConfig.SessionStore.Close()
		t.Fatal(err)
	}
	defer reopened.Close()
	history, _, err := reopened.History(id, -1, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range history {
		if entry.Event.Type == "user/message" && strings.Contains(fmt.Sprint(entry.Event.Data), "persist me") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("reopened history = %#v", history)
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
		{"SQLite tuning", "- id: sqlite\n  name: '@deepseek-ai/dsh-session-persistence-sqlite'\n  config: {path: ':memory:', preparedSessionCacheSize: 0}\n", "must be a positive integer"},
		{"SQLite batch delay", "- id: sqlite\n  name: '@deepseek-ai/dsh-session-persistence-sqlite'\n  config: {path: ':memory:', writeBatchMaxDelayMs: 0}\n", "must be between 1 and 2147483647"},
		{"storage service", "- id: sqlite\n  name: '@deepseek-ai/dsh-storage-sqlite'\n  config: {path: ':memory:'}\n", "require an enabled"},
		{"storage route", "- id: storage\n  name: '@deepseek-ai/dsh-storage'\n- id: domain\n  name: '@deepseek-ai/dsh-storage-domain'\n  config: {backend: sqlite}\n", "is not mounted"},
		{"HTTP limits", "- id: fetch\n  name: '@deepseek-ai/dsh-web-fetch-http'\n  config: {timeoutMs: 0}\n", "must be positive"},
		{"missing selected provider", "- id: web\n  name: '@deepseek-ai/dsh-web'\n  config: {searchProvider: exa}\n", "is not mounted"},
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

func hasTool(tools []harness.ToolSchema, name string) bool {
	for _, tool := range tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}
