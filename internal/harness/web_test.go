package harness

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestWebFetchIsDisabledByDefaultAndExplicitlyEnabled(t *testing.T) {
	t.Setenv("DSH_WEB_FETCH_PROVIDER", "")
	for _, test := range []struct {
		name     string
		provider string
		wantTool bool
		wantHTTP bool
	}{
		{name: "default", wantHTTP: true},
		{name: "http", provider: "http", wantTool: true, wantHTTP: true},
		{name: "custom", provider: "custom", wantTool: true, wantHTTP: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, err := New(WithPersistence(false), WithProvider("echo"), WithWebFetchProvider(test.provider))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = engine.Close() })
			_, hasTool := engine.tools["web_fetch"]
			_, hasHTTP := engine.webFetchProviders["http"]
			if hasTool != test.wantTool || hasHTTP != test.wantHTTP {
				t.Fatalf("web fetch tool/http = %v/%v, want %v/%v", hasTool, hasHTTP, test.wantTool, test.wantHTTP)
			}
		})
	}
}

func TestHTTPWebFetchProviderPolicyAndMarkdown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/page":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte("<html><body><h1>Title</h1><p>Hello <strong>world</strong>.</p><script>drop()</script></body></html>"))
		case "/large":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(strings.Repeat("x", 5_000_010)))
		case "/binary":
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte("binary"))
		case "/redirect":
			http.Redirect(w, r, "/page", http.StatusFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	provider := NewHTTPWebFetchProvider()
	result, err := provider.Fetch(context.Background(), WebFetchRequest{URL: server.URL + "/page"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Body.Kind != "html" || !strings.Contains(formatWebFetch(result), "# Title") || strings.Contains(formatWebFetch(result), "drop()") {
		t.Fatalf("markdown result = %#v", result)
	}
	redirected, err := provider.Fetch(context.Background(), WebFetchRequest{URL: server.URL + "/redirect"})
	if err != nil || redirected.StatusCode != http.StatusOK {
		t.Fatalf("same-origin redirect = %#v, %v", redirected, err)
	}
	if _, err := provider.Fetch(context.Background(), WebFetchRequest{URL: server.URL + "/binary"}); err == nil {
		t.Fatal("binary response unexpectedly accepted")
	}
	large, err := provider.Fetch(context.Background(), WebFetchRequest{URL: server.URL + "/large"})
	if err != nil || !large.Truncated || len(large.Body.Content) != 100_000 {
		t.Fatalf("large response = len %d truncated %v err %v", len(large.Body.Content), large.Truncated, err)
	}
}

func TestHTTPWebFetchProviderRedirectSizeAndCancellation(t *testing.T) {
	var targetHits int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHits++
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("should not be fetched"))
	}))
	defer target.Close()

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirect.Close()
	provider := NewHTTPWebFetchProvider()
	if _, err := provider.Fetch(context.Background(), WebFetchRequest{URL: redirect.URL}); err == nil || !strings.Contains(err.Error(), "cross-origin") {
		t.Fatalf("cross-origin redirect error = %v", err)
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_REDIRECT_BLOCKED" {
		t.Fatalf("cross-origin redirect code = %#v", err)
	}
	if targetHits != 0 {
		t.Fatalf("cross-origin target was contacted %d times", targetHits)
	}

	large := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("small body"))
	}))
	defer large.Close()
	limited := NewHTTPWebFetchProvider()
	limited.MaxBodyBytes = 10
	if _, err := limited.Fetch(context.Background(), WebFetchRequest{URL: large.URL}); err == nil {
		t.Fatal("declared oversized response was accepted")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_FETCH_TOO_LARGE" {
		t.Fatalf("declared oversized response = %#v", err)
	}

	hanging := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("late"))
	}))
	defer hanging.Close()
	ctx, stop := context.WithCancel(context.Background())
	stop()
	if _, err := provider.Fetch(ctx, WebFetchRequest{URL: hanging.URL}); err == nil {
		t.Fatal("cancelled fetch unexpectedly succeeded")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_ABORTED" {
		t.Fatalf("cancelled fetch = %#v", err)
	}

	timed := NewHTTPWebFetchProvider()
	timed.Timeout = 20 * time.Millisecond
	if _, err := timed.Fetch(context.Background(), WebFetchRequest{URL: hanging.URL}); err == nil {
		t.Fatal("timed fetch unexpectedly succeeded")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_FETCH_TIMEOUT" {
		t.Fatalf("timed fetch = %#v", err)
	}
}

func TestHTTPWebFetchProviderUsesConfiguredLimitsAndUserAgent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "custom-fetch-agent" {
			t.Errorf("User-Agent = %q", got)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("abcdef"))
	}))
	defer server.Close()

	provider := NewHTTPWebFetchProvider(HTTPWebFetchConfig{
		MaxURLLength: 256, MaxResponseBytes: 10, MaxBodyChars: 3,
		Timeout: time.Second, MaxRedirects: 0, UserAgent: "custom-fetch-agent",
	})
	result, err := provider.Fetch(context.Background(), WebFetchRequest{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if result.Body.Content != "abc" || !result.Truncated {
		t.Fatalf("configured fetch = %#v", result)
	}
	if _, err := provider.Fetch(context.Background(), WebFetchRequest{URL: server.URL + strings.Repeat("x", 256)}); err == nil {
		t.Fatal("configured URL limit was not enforced")
	}
}

func TestHTTPWebFetchProviderCountsUTF16Characters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("a😀b"))
	}))
	defer server.Close()
	provider := NewHTTPWebFetchProvider(HTTPWebFetchConfig{
		MaxURLLength: 256, MaxResponseBytes: 100, MaxBodyChars: 3,
		Timeout: time.Second, MaxRedirects: 0, UserAgent: "test",
	})
	result, err := provider.Fetch(context.Background(), WebFetchRequest{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if result.Body.Content != "a😀" || !result.Truncated || utf16Length(result.Body.Content) != 3 {
		t.Fatalf("UTF-16 capped fetch = %#v", result)
	}
}

func TestRenderWebFetchCapsCompleteOutput(t *testing.T) {
	text, truncated := renderWebFetch(WebFetchResult{
		URL: "https://example.test", StatusCode: http.StatusOK,
		Body: WebFetchBody{Kind: "text", Content: strings.Repeat("x", 100)},
	}, 80)
	if !truncated || utf16Length(text) != 80 || !strings.HasSuffix(text, "(Content truncated. Fetch a more specific URL or section for the full text.)") {
		t.Fatalf("rendered fetch = %q, truncated=%v", text, truncated)
	}
}

func TestRenderWebFetchSkipsPathologicalHTMLConversion(t *testing.T) {
	pathological := strings.Repeat("<div><!-- </div> --></span>", 600) + "x"
	text, truncated := renderWebFetch(WebFetchResult{
		URL:        "https://example.com",
		StatusCode: http.StatusOK,
		Body:       WebFetchBody{Kind: "html", Content: pathological},
	}, len([]rune(pathological))+100)
	if truncated || !strings.HasSuffix(text, pathological) {
		t.Fatalf("pathological HTML was converted or truncated: truncated=%v suffix=%q", truncated, text[len(text)-min(len(text), 80):])
	}

	ordinary := strings.Repeat(`<p title='>'>x<br><img src="x"><input/></p>`, 600) + `<script>const fake = '` + strings.Repeat("<div>", 600) + `'</script>`
	text, _ = renderWebFetch(WebFetchResult{
		URL:        "https://example.com",
		StatusCode: http.StatusOK,
		Body:       WebFetchBody{Kind: "html", Content: ordinary},
	}, len([]rune(ordinary))+100)
	if strings.Contains(text, "<p") || strings.Contains(text, "const fake") {
		t.Fatalf("ordinary HTML did not pass through the converter: %q", text[:min(len(text), 200)])
	}
}

func TestDeepSeekWebSearchProviderParsesNativeBlocks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("request JSON: %v", err)
		}
		if body["tools"].([]any)[0].(map[string]any)["type"] != "web_search_20250305" {
			t.Errorf("tools = %#v", body["tools"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"web_search_tool_result","content":[{"type":"web_search_result","url":"https://a.test","title":"A","page_age":"today"},{"type":"web_search_result","url":"https://a.test","title":"duplicate"}]},{"type":"text","citations":[{"url":"https://a.test","cited_text":"excerpt"}]}]}`))
	}))
	defer server.Close()
	e, err := New(WithPersistence(false), WithProvider("echo"), WithWebSearchProvider("deepseek-official"))
	if err != nil {
		t.Fatal(err)
	}
	e.cfg.APIKey = "test-key"
	t.Setenv("DEEPSEEK_SEARCH_BASE_URL", server.URL)
	provider := &deepSeekWebSearchProvider{engine: e}
	result, err := provider.Search(context.Background(), WebSearchRequest{Query: "hello", MaxResults: 5})
	if err != nil || len(result.Sources) != 1 || result.Sources[0].Snippet != "excerpt" || result.Sources[0].PublishedAt != "today" {
		t.Fatalf("search result = %#v, %v", result, err)
	}
}

func TestDeepSeekWebSearchProviderUsesIndependentSettingsSection(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "")
	t.Setenv("DEEPSEEK_SEARCH_BASE_URL", "")
	type requestView struct {
		APIKey     string
		APIVersion string
		UserAgent  string
		Body       map[string]any
	}
	requests := make(chan requestView, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("request JSON: %v", err)
		}
		requests <- requestView{APIKey: r.Header.Get("x-api-key"), APIVersion: r.Header.Get("anthropic-version"), UserAgent: r.Header.Get("User-Agent"), Body: body}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"web_search_tool_result","content":[{"type":"web_search_result","url":"https://result.test"}]}]}`))
	}))
	defer server.Close()
	e, err := New(WithPersistence(false), WithProvider("echo"), WithAPIKey("llm-key"), WithWebSearchProvider("deepseek-official"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, rpcErr := e.settingsUpdate("llm-deepseek", map[string]any{"webSearchBaseURL": "https://wrong.test", "webSearchModel": "wrong-model"}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if _, rpcErr := e.settingsUpdate("web-search-deepseek", map[string]any{
		"apiKey": "search-key", "baseURL": server.URL, "model": "search-model",
		"apiVersion": "2026-08-19", "maxTokens": 1234, "maxUses": 7,
	}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if _, err := e.webSearch(context.Background(), WebSearchRequest{Query: "settings"}); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	tool := request.Body["tools"].([]any)[0].(map[string]any)
	if request.APIKey != "search-key" || request.APIVersion != "2026-08-19" || request.UserAgent != "deepseek-harness/0.0.1" ||
		request.Body["model"] != "search-model" || request.Body["max_tokens"] != float64(1234) && request.Body["max_tokens"] != 1234 || tool["max_uses"] != float64(7) && tool["max_uses"] != 7 {
		t.Fatalf("DeepSeek search request = %#v / %#v", request, tool)
	}
	view := e.settingsViewLocked("web-search-deepseek")
	value := view["value"].(map[string]any)
	user := view["user"].(map[string]any)
	secrets := view["secrets"].([]any)
	if _, leaked := value["apiKey"]; leaked {
		t.Fatalf("settings value leaked apiKey: %#v", value)
	}
	if _, leaked := user["apiKey"]; leaked || len(secrets) != 1 || secrets[0].(map[string]any)["set"] != true {
		t.Fatalf("redacted settings view = %#v", view)
	}
	if value["apiKeyEnv"] != "DEEPSEEK_API_KEY" || positiveIntSetting(value["maxUses"], 0) != 7 {
		t.Fatalf("resolved web-search settings = %#v", value)
	}
}

func TestDeepSeekWebSearchMissingCredentialIsExecutionError(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "")
	e, err := New(WithPersistence(false), WithProvider("echo"), WithAPIKey(""), WithWebSearchProvider("deepseek-official"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_, err = e.webSearch(context.Background(), WebSearchRequest{Query: "missing key"})
	webErr, ok := err.(*WebError)
	if !ok || webErr.Code != "WEB_PROVIDER_CREDENTIAL_MISSING" || !strings.Contains(webErr.Message, "web Models page") {
		t.Fatalf("missing credential error = %#v", err)
	}
	if strings.Contains(webErr.Message, "Search endpoint configuration") {
		t.Fatalf("pre-dispatch credential error included endpoint recovery guidance: %q", webErr.Message)
	}
}

func TestDeepSeekWebSearchProviderErrorsAndRequestEvent(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantCode   string
		wantErrSub string
	}{
		{name: "http error", status: http.StatusTooManyRequests, body: `{"error":{"message":"rate limited"}}`, wantCode: "WEB_PROVIDER_ERROR", wantErrSub: "rate limited"},
		{name: "malformed success", status: http.StatusOK, body: `{not-json}`, wantCode: "WEB_PROVIDER_ERROR"},
		{name: "no native result", status: http.StatusOK, body: `{"content":[{"type":"text","text":"fallback"}]}`, wantCode: "WEB_PROVIDER_ERROR"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			e, err := New(WithPersistence(false), WithProvider("echo"), WithWebSearchProvider("deepseek-official"))
			if err != nil {
				t.Fatal(err)
			}
			e.cfg.APIKey = "test-key"
			t.Setenv("DEEPSEEK_SEARCH_BASE_URL", server.URL)
			provider := &deepSeekWebSearchProvider{engine: e}
			_, err = provider.Search(context.Background(), WebSearchRequest{Query: "hello"})
			if err == nil {
				t.Fatal("search unexpectedly succeeded")
			}
			webErr, ok := err.(*WebError)
			if !ok || webErr.Code != test.wantCode || test.wantErrSub != "" && !strings.Contains(webErr.Message, test.wantErrSub) {
				t.Fatalf("search error = %#v", err)
			}
			for _, want := range []string{strconv.Quote(server.URL + "/messages"), "Settings > Plugins > Plugin configuration > Web search", "DEEPSEEK_SEARCH_BASE_URL", "web-search-deepseek.baseURL"} {
				if !strings.Contains(webErr.Message, want) {
					t.Fatalf("search error omits endpoint recovery %q: %q", want, webErr.Message)
				}
			}
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"web_search_tool_result","content":[{"type":"web_search_result","url":"https://event.test","title":"event"}]}]}`))
	}))
	defer server.Close()
	e, err := New(WithPersistence(false), WithProvider("echo"), WithWebSearchProvider("deepseek-official"))
	if err != nil {
		t.Fatal(err)
	}
	e.cfg.APIKey = "secret-key"
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEEPSEEK_SEARCH_BASE_URL", server.URL)
	e.mu.RLock()
	tool := e.tools["web_search"]
	e.mu.RUnlock()
	if _, err := tool.Execute(context.Background(), ToolCall{
		Name: "web_search", SessionID: id, Arguments: json.RawMessage(`{"queries":["logged"]}`),
	}); err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	var found *Event
	for i := range session.Events {
		if session.Events[i].Type == "web/deepseek-search-llm-request" {
			found = &session.Events[i]
		}
	}
	if found == nil {
		t.Fatal("search request event was not recorded")
	}
	encoded, _ := json.Marshal(found.Data)
	if strings.Contains(string(encoded), "secret-key") {
		t.Fatalf("request event leaked API key: %s", encoded)
	}
}

func TestDeepSeekWebSearchProviderAbortAndRedirectPolicy(t *testing.T) {
	targetHits := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[]}`))
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirect.Close()
	e, err := New(WithPersistence(false), WithProvider("echo"), WithWebSearchProvider("deepseek-official"))
	if err != nil {
		t.Fatal(err)
	}
	e.cfg.APIKey = "test-key"
	t.Setenv("DEEPSEEK_SEARCH_BASE_URL", redirect.URL)
	provider := &deepSeekWebSearchProvider{engine: e}
	if _, err := provider.Search(context.Background(), WebSearchRequest{Query: "redirect"}); err == nil {
		t.Fatal("redirecting search unexpectedly succeeded")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_PROVIDER_ERROR" {
		t.Fatalf("redirecting search = %#v", err)
	}
	if targetHits != 0 {
		t.Fatalf("search redirect target was contacted %d times", targetHits)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.Search(ctx, WebSearchRequest{Query: "cancelled"}); err == nil {
		t.Fatal("cancelled search unexpectedly succeeded")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_ABORTED" {
		t.Fatalf("cancelled search = %#v", err)
	}

	started := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-time.After(200 * time.Millisecond):
		}
	}))
	defer slow.Close()
	t.Setenv("DEEPSEEK_SEARCH_BASE_URL", slow.URL)
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := provider.Search(ctx, WebSearchRequest{Query: "in flight"})
		done <- err
	}()
	<-started
	cancel()
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("in-flight search did not stop after cancellation")
	}
	if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_ABORTED" {
		t.Fatalf("in-flight cancelled search = %#v", err)
	}
}

type stubWebSearchProvider struct {
	id        string
	available bool
}

type scriptedWebSeamProvider struct {
	search func(context.Context, WebSearchRequest) (WebSearchResult, error)
	fetch  func(context.Context, WebFetchRequest) (WebFetchResult, error)
}

func (scriptedWebSeamProvider) ID() string      { return "scripted-seam" }
func (scriptedWebSeamProvider) Available() bool { return true }
func (p scriptedWebSeamProvider) Search(ctx context.Context, request WebSearchRequest) (WebSearchResult, error) {
	return p.search(ctx, request)
}
func (p scriptedWebSeamProvider) Fetch(ctx context.Context, request WebFetchRequest) (WebFetchResult, error) {
	return p.fetch(ctx, request)
}

func (p stubWebSearchProvider) ID() string      { return p.id }
func (p stubWebSearchProvider) Available() bool { return p.available }
func (p stubWebSearchProvider) Search(context.Context, WebSearchRequest) (WebSearchResult, error) {
	return WebSearchResult{Sources: []WebSearchSource{}, Truncated: false}, nil
}

func TestWebProviderSelectionAndDuplicates(t *testing.T) {
	e, err := New(WithPersistence(false), WithProvider("echo"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	dispose, err := e.RegisterWebSearchProvider(stubWebSearchProvider{id: "custom", available: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.RegisterWebSearchProvider(stubWebSearchProvider{id: "custom", available: true}); err == nil {
		t.Fatal("duplicate search provider was accepted")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_DUPLICATE_PROVIDER" {
		t.Fatalf("duplicate provider error = %#v", err)
	}

	e.cfg.WebSearchProvider = "missing"
	if _, err := e.webSearch(context.Background(), WebSearchRequest{Query: "q"}); err == nil {
		t.Fatal("missing configured provider was accepted")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_PROVIDER_CONFIGURED_MISSING" {
		t.Fatalf("missing provider error = %#v", err)
	}

	e.cfg.WebSearchProvider = ""
	e.settings["web-search-deepseek"] = map[string]any{"baseURL": "not a url"}
	if _, err := e.webSearch(context.Background(), WebSearchRequest{Query: "q"}); err != nil {
		t.Fatalf("single usable provider was not auto-selected: %v", err)
	}
	if _, err := e.RegisterWebSearchProvider(stubWebSearchProvider{id: "other", available: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.webSearch(context.Background(), WebSearchRequest{Query: "q"}); err == nil {
		t.Fatal("ambiguous providers were accepted")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_PROVIDER_AMBIGUOUS" {
		t.Fatalf("ambiguous provider error = %#v", err)
	}
	dispose()
	if _, err := e.webSearch(context.Background(), WebSearchRequest{Query: "q"}); err != nil {
		t.Fatalf("search after provider disposal: %v", err)
	}
}

func TestEmptyConfiguredProviderEnablesRuntimeAutoSelection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.Provider = "echo"
	cfg.PluginInventory = []PluginInventoryEntry{}
	cfg.WebSearchProvider = ""
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := e.Config().WebSearchProvider; got != "" {
		t.Fatalf("empty provider was normalized to %q", got)
	}
	provider := stubWebSearchProvider{id: "only", available: true}
	if _, err := e.RegisterWebSearchProvider(provider); err != nil {
		t.Fatal(err)
	}
	selected, err := e.selectWebSearchProvider()
	if err != nil || selected != provider {
		t.Fatalf("auto-selected provider = %#v, %v", selected, err)
	}
}

func TestWebSeamForwardsRequestsWithoutConsumerValidation(t *testing.T) {
	e, err := New(WithPersistence(false), WithProvider("echo"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	var searchRequest WebSearchRequest
	var fetchRequest WebFetchRequest
	provider := scriptedWebSeamProvider{
		search: func(_ context.Context, request WebSearchRequest) (WebSearchResult, error) {
			searchRequest = request
			return WebSearchResult{Sources: []WebSearchSource{}, Truncated: false}, nil
		},
		fetch: func(_ context.Context, request WebFetchRequest) (WebFetchResult, error) {
			fetchRequest = request
			return WebFetchResult{URL: request.URL, StatusCode: 200, Body: WebFetchBody{Kind: "text"}}, nil
		},
	}
	if _, err := e.RegisterWebSearchProvider(provider); err != nil {
		t.Fatal(err)
	}
	if _, err := e.RegisterWebFetchProvider(provider); err != nil {
		t.Fatal(err)
	}
	e.cfg.WebSearchProvider = provider.ID()
	e.cfg.WebFetchProvider = provider.ID()
	if _, err := e.webSearch(context.Background(), WebSearchRequest{Query: ""}); err != nil {
		t.Fatalf("search forwarding: %v", err)
	}
	if _, err := e.webFetch(context.Background(), WebFetchRequest{URL: ""}); err != nil {
		t.Fatalf("fetch forwarding: %v", err)
	}
	if searchRequest.Query != "" || fetchRequest.URL != "" {
		t.Fatalf("forwarded requests = %#v, %#v", searchRequest, fetchRequest)
	}
}

func TestWebProviderDisposerSupportsReloadAndPreservesRegistrationOrder(t *testing.T) {
	e, err := New(WithPersistence(false), WithProvider("echo"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	first := stubWebSearchProvider{id: "reloadable", available: true}
	disposeFirst, err := e.RegisterWebSearchProvider(first)
	if err != nil {
		t.Fatal(err)
	}
	disposeFirst()
	second := stubWebSearchProvider{id: "reloadable", available: true}
	if _, err := e.RegisterWebSearchProvider(second); err != nil {
		t.Fatal(err)
	}
	disposeFirst()
	e.cfg.WebSearchProvider = "reloadable"
	selected, err := e.selectWebSearchProvider()
	if err != nil || selected != second {
		t.Fatalf("provider after reload = %#v, %v", selected, err)
	}

	e.cfg.WebSearchProvider = ""
	e.settings["web-search-deepseek"] = map[string]any{"baseURL": "not a url"}
	if _, err := e.RegisterWebSearchProvider(stubWebSearchProvider{id: "last", available: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.selectWebSearchProvider(); err == nil || !strings.Contains(err.Error(), "reloadable, last") {
		t.Fatalf("ambiguous provider order error = %v", err)
	}
}

func TestHostWebProviderRegistryFollowsPluginInventoryAndReload(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.Provider = "echo"
	cfg.PluginInventory = []PluginInventoryEntry{}
	cfg.WebSearchProvider = "exa"
	cfg.ExaSearch = ExaSearchProviderOptions{APIKey: "key"}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.selectWebSearchProvider(); err == nil {
		t.Fatal("provider registered without its Host plugin")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_PROVIDER_CONFIGURED_MISSING" {
		t.Fatalf("missing provider error = %#v", err)
	}

	next := e.Config()
	next.PluginInventory = []PluginInventoryEntry{{EntryID: "exa", ModuleName: "@deepseek-ai/dsh-web-search-exa", Enabled: true}}
	if err := e.ApplyRuntimeConfig(next); err != nil {
		t.Fatal(err)
	}
	if provider, err := e.selectWebSearchProvider(); err != nil || provider.ID() != "exa" {
		t.Fatalf("provider after activation = %#v, %v", provider, err)
	}

	next = e.Config()
	next.PluginInventory = []PluginInventoryEntry{}
	if err := e.ApplyRuntimeConfig(next); err != nil {
		t.Fatal(err)
	}
	if _, err := e.selectWebSearchProvider(); err == nil {
		t.Fatal("provider survived Host plugin disposal")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_PROVIDER_CONFIGURED_MISSING" {
		t.Fatalf("disposed provider error = %#v", err)
	}
}

func TestHostHTTPWebProviderRegistersIndependentlyOfSelection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.Provider = "echo"
	cfg.WebFetchProvider = "custom-fetch"
	cfg.PluginInventory = []PluginInventoryEntry{{
		EntryID: "http", ModuleName: "@deepseek-ai/dsh-web-fetch-http", Enabled: true,
	}}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, ok := e.webFetchProviders["http"]; !ok {
		t.Fatal("mounted HTTP provider was not registered when another provider was selected")
	}
	if _, err := e.selectWebFetchProvider(); err == nil {
		t.Fatal("configured missing provider unexpectedly resolved")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_PROVIDER_CONFIGURED_MISSING" {
		t.Fatalf("selection error = %#v", err)
	}
}

func TestHostWebProviderWaitsForActivePluginFiber(t *testing.T) {
	pending := "pending"
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.Provider = "echo"
	cfg.WebSearchProvider = "exa"
	cfg.ExaSearch = ExaSearchProviderOptions{APIKey: "key"}
	cfg.PluginInventory = []PluginInventoryEntry{{
		EntryID: "exa", ModuleName: "@deepseek-ai/dsh-web-search-exa", Enabled: true, FiberPhase: &pending,
	}}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.selectWebSearchProvider(); err == nil {
		t.Fatal("pending provider was registered before its fiber became active")
	}
	if settingsDescribeContains(e, "web-search-deepseek") {
		t.Fatal("pending provider exposed its settings namespace")
	}
	if _, rpcErr := e.settingsUpdate("web-search-deepseek", map[string]any{"model": "pending"}, nil, false); rpcErr == nil || rpcErr.Code != "settings/rejected" {
		t.Fatalf("pending provider settings update = %#v", rpcErr)
	}
	active := "active"
	next := e.Config()
	next.PluginInventory = []PluginInventoryEntry{{
		EntryID: "exa", ModuleName: "@deepseek-ai/dsh-web-search-exa", Enabled: true, FiberPhase: &active,
	}}
	if err := e.ApplyRuntimeConfig(next); err != nil {
		t.Fatal(err)
	}
	if provider, err := e.selectWebSearchProvider(); err != nil || provider.ID() != "exa" {
		t.Fatalf("provider after fiber activation = %#v, %v", provider, err)
	}
}

func TestDeepSeekWebSettingsNamespaceFollowsPluginLifecycle(t *testing.T) {
	active := "active"
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.Provider = "echo"
	cfg.PluginInventory = []PluginInventoryEntry{{
		EntryID: "deepseek", ModuleName: "@deepseek-ai/dsh-web-search-deepseek", Enabled: true, FiberPhase: &active,
	}}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if !settingsDescribeContains(e, "web-search-deepseek") {
		t.Fatal("active DeepSeek provider did not expose its settings namespace")
	}
	if _, rpcErr := e.settingsUpdate("web-search-deepseek", map[string]any{"model": "saved-model"}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	next := e.Config()
	next.PluginInventory = []PluginInventoryEntry{}
	if err := e.ApplyRuntimeConfig(next); err != nil {
		t.Fatal(err)
	}
	if settingsDescribeContains(e, "web-search-deepseek") {
		t.Fatal("disposed DeepSeek provider kept its settings namespace visible")
	}
	if _, rpcErr := e.settingsUpdate("web-search-deepseek", map[string]any{"model": "hidden"}, nil, false); rpcErr == nil || rpcErr.Code != "settings/rejected" {
		t.Fatalf("disposed provider settings update = %#v", rpcErr)
	}
	next = e.Config()
	next.PluginInventory = cfg.PluginInventory
	if err := e.ApplyRuntimeConfig(next); err != nil {
		t.Fatal(err)
	}
	if !settingsDescribeContains(e, "web-search-deepseek") {
		t.Fatal("reloaded DeepSeek provider did not restore its settings namespace")
	}
	e.mu.RLock()
	view := e.settingsViewLocked("web-search-deepseek")
	e.mu.RUnlock()
	if value := view["value"].(map[string]any); value["model"] != "saved-model" {
		t.Fatalf("reloaded provider settings = %#v", value)
	}
}

func settingsDescribeContains(e *Engine, namespace string) bool {
	for _, raw := range e.settingsDescribe()["namespaces"].([]any) {
		if raw.(map[string]any)["ns"] == namespace {
			return true
		}
	}
	return false
}
