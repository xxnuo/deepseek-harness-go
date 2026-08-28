package harness

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type scriptedWebSearchProvider struct {
	search func(context.Context, WebSearchRequest) (WebSearchResult, error)
}

func (scriptedWebSearchProvider) ID() string      { return "scripted" }
func (scriptedWebSearchProvider) Available() bool { return true }
func (p scriptedWebSearchProvider) Search(ctx context.Context, request WebSearchRequest) (WebSearchResult, error) {
	return p.search(ctx, request)
}

func webSearchToolForTest(t *testing.T, provider WebSearchProvider, configure func(*WebToolConfig)) (*Engine, Tool) {
	t.Helper()
	webTools := DefaultWebToolConfig()
	if configure != nil {
		configure(&webTools)
	}
	e, err := New(
		WithPersistence(false),
		WithProvider("echo"),
		WithWebSearchProvider(provider.ID()),
		func(config *Config) { config.WebTools = &webTools },
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if _, err := e.RegisterWebSearchProvider(provider); err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool := e.tools["web_search"]
	e.mu.RUnlock()
	return e, tool
}

func TestWebSearchQueryValidationMatchesRC8(t *testing.T) {
	queries, err := parseWebSearchQueries([]string{"one", "one", " two "}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(queries, []string{"one", " two "}) {
		t.Fatalf("queries = %#v", queries)
	}
	for _, test := range []struct {
		queries []string
		max     int
		want    string
	}{
		{queries: nil, max: 4, want: "at least one query"},
		{queries: []string{"one", "one", "two"}, max: 2, want: "at most 2 queries"},
		{queries: []string{"ok", " "}, max: 4, want: "each query must be a non-empty string"},
	} {
		if _, err := parseWebSearchQueries(test.queries, test.max); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("parseWebSearchQueries(%#v, %d) error = %v", test.queries, test.max, err)
		}
	}
	if got := DefaultWebToolConfig().SearchMaxQueries; got != 4 {
		t.Fatalf("default SearchMaxQueries = %d, want 4", got)
	}
	if !DefaultWebToolConfig().FetchEnabled {
		t.Fatal("tool-web plugin default should enable web_fetch")
	}
}

func TestWebSearchRunsQueriesConcurrentlyAndRoundRobinMerges(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	provider := scriptedWebSearchProvider{search: func(_ context.Context, request WebSearchRequest) (WebSearchResult, error) {
		started <- request.Query
		<-release
		if request.Query == "one" {
			return WebSearchResult{Content: "answer one", Sources: []WebSearchSource{{URL: "https://a.test"}, {URL: "https://shared.test"}}}, nil
		}
		return WebSearchResult{Content: "answer two", Sources: []WebSearchSource{{URL: "https://b.test"}, {URL: "https://shared.test"}, {URL: "https://c.test"}}}, nil
	}}
	_, tool := webSearchToolForTest(t, provider, nil)
	done := make(chan struct {
		result ToolResult
		err    error
	}, 1)
	go func() {
		result, err := tool.Execute(context.Background(), ToolCall{Name: "web_search", Arguments: json.RawMessage(`{"queries":["one","one","two"]}`)})
		done <- struct {
			result ToolResult
			err    error
		}{result, err}
	}()
	seen := []string{<-started, <-started}
	sort.Strings(seen)
	if !reflect.DeepEqual(seen, []string{"one", "two"}) {
		t.Fatalf("provider queries = %#v", seen)
	}
	close(release)
	outcome := <-done
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	result, ok := outcome.result.Value.(WebSearchResult)
	if !ok {
		t.Fatalf("result value = %#v", outcome.result.Value)
	}
	wantSources := []WebSearchSource{{URL: "https://a.test"}, {URL: "https://b.test"}, {URL: "https://shared.test"}, {URL: "https://c.test"}}
	if result.Content != "### one\n\nanswer one\n\n### two\n\nanswer two" || !reflect.DeepEqual(result.Sources, wantSources) || result.Truncated {
		t.Fatalf("merged result = %#v", result)
	}
}

func TestWebSearchCapsMergedResultsAndCancelsFailedBatch(t *testing.T) {
	provider := scriptedWebSearchProvider{search: func(_ context.Context, request WebSearchRequest) (WebSearchResult, error) {
		if request.Query == "one" {
			return WebSearchResult{Sources: []WebSearchSource{{URL: "https://a.test"}, {URL: "https://b.test"}}}, nil
		}
		return WebSearchResult{Sources: []WebSearchSource{{URL: "https://c.test"}, {URL: "https://d.test"}}}, nil
	}}
	_, tool := webSearchToolForTest(t, provider, func(config *WebToolConfig) { config.SearchMaxResults = 2 })
	result, err := tool.Execute(context.Background(), ToolCall{Name: "web_search", Arguments: json.RawMessage(`{"queries":["one","two"]}`)})
	if err != nil {
		t.Fatal(err)
	}
	value := result.Value.(WebSearchResult)
	if !value.Truncated || !reflect.DeepEqual(value.Sources, []WebSearchSource{{URL: "https://a.test"}, {URL: "https://c.test"}}) {
		t.Fatalf("capped result = %#v", value)
	}

	siblingStarted := make(chan struct{})
	siblingAborted := make(chan struct{})
	releaseSibling := make(chan struct{})
	failing := scriptedWebSearchProvider{search: func(ctx context.Context, request WebSearchRequest) (WebSearchResult, error) {
		if request.Query == "one" {
			<-siblingStarted
			return WebSearchResult{}, errors.New("first search failed")
		}
		close(siblingStarted)
		<-ctx.Done()
		close(siblingAborted)
		<-releaseSibling
		return WebSearchResult{}, ctx.Err()
	}}
	_, failingTool := webSearchToolForTest(t, failing, nil)
	errDone := make(chan error, 1)
	go func() {
		_, err := failingTool.Execute(context.Background(), ToolCall{Name: "web_search", Arguments: json.RawMessage(`{"queries":["one","two"]}`)})
		errDone <- err
	}()
	select {
	case <-siblingAborted:
	case <-time.After(time.Second):
		t.Fatal("sibling search was not cancelled")
	}
	select {
	case err := <-errDone:
		t.Fatalf("batch returned before sibling settled: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseSibling)
	if err := <-errDone; err == nil || err.Error() != "first search failed" {
		t.Fatalf("batch error = %v", err)
	}
}
