package harness

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExaSearchProviderRequestAndMapping(t *testing.T) {
	requests := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" || r.Header.Get("Authorization") != "Bearer exa-key" || r.Header.Get("User-Agent") != webSearchProviderUserAgent {
			t.Errorf("Exa request = %s %#v", r.URL.Path, r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode Exa request: %v", err)
		}
		requests <- body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"url":"https://a.test","title":"A","publishedDate":"today","highlights":["","excerpt"]},{"url":"https://drop.test","highlights":[" "]}]}`))
	}))
	defer server.Close()
	provider := NewExaSearchProvider(ExaSearchProviderOptions{APIKey: "exa-key", BaseURL: server.URL, SearchType: "neural", NumResults: 9, HighlightsPerResult: 2})
	result, err := provider.Search(context.Background(), WebSearchRequest{Query: "q", MaxResults: 3})
	if err != nil || len(result.Sources) != 1 || result.Sources[0].Snippet != "excerpt" || result.Sources[0].PublishedAt != "today" {
		t.Fatalf("Exa result = %#v, %v", result, err)
	}
	body := <-requests
	highlights := body["contents"].(map[string]any)["highlights"].(map[string]any)
	if body["type"] != "neural" || body["numResults"] != float64(3) || highlights["highlightsPerUrl"] != float64(2) {
		t.Fatalf("Exa body = %#v", body)
	}
}

func TestPerplexitySearchProviderStructuredAndCitationFallback(t *testing.T) {
	responses := []string{
		`{"choices":[{"message":{"content":"answer"}}],"search_results":[{"url":"https://a.test","title":"A","snippet":"excerpt","date":"today"}],"citations":["https://ignored.test"]}`,
		`{"choices":[],"citations":["https://fallback.test"]}`,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" || r.Header.Get("Authorization") != "Bearer pplx-key" {
			t.Errorf("Perplexity request = %s %#v", r.URL.Path, r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode Perplexity request: %v", err)
		}
		if body["model"] != "sonar-pro" || body["max_tokens"] != float64(2048) || body["search_recency_filter"] != "week" {
			t.Errorf("Perplexity body = %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(responses[0]))
		responses = responses[1:]
	}))
	defer server.Close()
	provider := NewPerplexitySearchProvider(PerplexitySearchProviderOptions{APIKey: "pplx-key", BaseURL: server.URL, Model: "sonar-pro", MaxTokens: 2048, SearchRecency: "week"})
	structured, err := provider.Search(context.Background(), WebSearchRequest{Query: "q"})
	if err != nil || structured.Content != "answer" || len(structured.Sources) != 1 || structured.Sources[0].Snippet != "excerpt" {
		t.Fatalf("Perplexity structured result = %#v, %v", structured, err)
	}
	fallback, err := provider.Search(context.Background(), WebSearchRequest{Query: "q2"})
	if err != nil || fallback.Content != "" || len(fallback.Sources) != 1 || fallback.Sources[0].URL != "https://fallback.test" {
		t.Fatalf("Perplexity fallback result = %#v, %v", fallback, err)
	}
}

func TestOptionalSearchProvidersErrorsAndSelection(t *testing.T) {
	redirectTargetHits := 0
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirectTargetHits++ }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer redirect.Close()
	provider := NewExaSearchProvider(ExaSearchProviderOptions{APIKey: "key", BaseURL: redirect.URL})
	if _, err := provider.Search(context.Background(), WebSearchRequest{Query: "q"}); err == nil {
		t.Fatal("Exa redirect unexpectedly succeeded")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_PROVIDER_ERROR" {
		t.Fatalf("Exa redirect error = %#v", err)
	}
	if redirectTargetHits != 0 {
		t.Fatalf("redirect target was contacted %d times", redirectTargetHits)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.Search(ctx, WebSearchRequest{Query: "q"}); err == nil {
		t.Fatal("cancelled Exa request unexpectedly succeeded")
	} else if webErr, ok := err.(*WebError); !ok || webErr.Code != "WEB_ABORTED" {
		t.Fatalf("cancelled Exa error = %#v", err)
	}

	t.Setenv("EXA_API_KEY", "exa-env-key")
	e, err := New(WithPersistence(false), WithProvider("echo"), WithWebSearchProvider("exa"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	selected, err := e.selectWebSearchProvider()
	if err != nil || selected.ID() != "exa" {
		t.Fatalf("selected provider = %#v, %v", selected, err)
	}
	if !strings.Contains(providerHTTPError(&http.Response{StatusCode: 429, Body: ioNopCloser(`{"error":"rate limited"}`)}, context.Background(), "Exa").Error(), "rate limited") {
		t.Fatal("Exa error detail was not preserved")
	}
}

func ioNopCloser(value string) *testReadCloser {
	return &testReadCloser{Reader: strings.NewReader(value)}
}

type testReadCloser struct{ *strings.Reader }

func (c *testReadCloser) Close() error { return nil }
