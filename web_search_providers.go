package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

const webSearchProviderUserAgent = "deepseek-harness/0.0.1"

type ExaSearchProviderOptions struct {
	APIKey              string
	BaseURL             string
	SearchType          string
	NumResults          int
	HighlightsPerResult int
}

type ExaSearchProvider struct {
	options ExaSearchProviderOptions
	client  *http.Client
}

func NewExaSearchProvider(options ExaSearchProviderOptions) *ExaSearchProvider {
	if options.APIKey == "" {
		options.APIKey = os.Getenv("EXA_API_KEY")
	}
	if options.BaseURL == "" {
		options.BaseURL = "https://api.exa.ai"
	}
	if options.SearchType == "" {
		options.SearchType = "auto"
	}
	if options.HighlightsPerResult == 0 {
		options.HighlightsPerResult = 1
	}
	return &ExaSearchProvider{options: options, client: webSearchProviderHTTPClient()}
}

func (p *ExaSearchProvider) ID() string { return "exa" }

func (p *ExaSearchProvider) Available() bool {
	return p.options.APIKey != "" && validAbsoluteURL(p.options.BaseURL) &&
		(p.options.SearchType == "auto" || p.options.SearchType == "keyword" || p.options.SearchType == "neural") &&
		p.options.HighlightsPerResult > 0 && p.options.NumResults >= 0
}

func (p *ExaSearchProvider) Search(ctx context.Context, request WebSearchRequest) (WebSearchResult, error) {
	numResults := request.MaxResults
	if numResults <= 0 {
		numResults = p.options.NumResults
	}
	body := map[string]any{
		"query": request.Query,
		"type":  p.options.SearchType,
		"contents": map[string]any{
			"highlights": map[string]any{"highlightsPerUrl": p.options.HighlightsPerResult},
		},
	}
	if numResults > 0 {
		body["numResults"] = numResults
	}
	response, err := p.post(ctx, p.options.BaseURL+"/search", body, p.options.APIKey)
	if err != nil {
		return WebSearchResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return WebSearchResult{}, providerHTTPError(response, ctx, "Exa")
	}
	var payload struct {
		Results []struct {
			URL           string   `json:"url"`
			Title         string   `json:"title"`
			PublishedDate string   `json:"publishedDate"`
			Highlights    []string `json:"highlights"`
		} `json:"results"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return WebSearchResult{}, providerBodyError("Exa", err, ctx)
	}
	sources := make([]WebSearchSource, 0, len(payload.Results))
	for _, result := range payload.Results {
		snippet := ""
		for _, highlight := range result.Highlights {
			if strings.TrimSpace(highlight) != "" {
				snippet = highlight
				break
			}
		}
		if snippet == "" {
			continue
		}
		sources = append(sources, WebSearchSource{URL: result.URL, Title: result.Title, Snippet: snippet, PublishedAt: result.PublishedDate})
	}
	return WebSearchResult{Sources: sources}, nil
}

func (p *ExaSearchProvider) post(ctx context.Context, endpoint string, body map[string]any, key string) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, &WebError{Code: "WEB_PROVIDER_ERROR", Message: "Exa search request failed: " + err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", webSearchProviderUserAgent)
	response, err := p.client.Do(req)
	if err != nil {
		return nil, namedSearchRequestError("Exa", err, ctx)
	}
	return response, nil
}

type PerplexitySearchProviderOptions struct {
	APIKey        string
	BaseURL       string
	Model         string
	MaxTokens     int
	SearchRecency string
}

type PerplexitySearchProvider struct {
	options PerplexitySearchProviderOptions
	client  *http.Client
}

func NewPerplexitySearchProvider(options PerplexitySearchProviderOptions) *PerplexitySearchProvider {
	if options.APIKey == "" {
		options.APIKey = os.Getenv("PERPLEXITY_API_KEY")
	}
	if options.BaseURL == "" {
		options.BaseURL = "https://api.perplexity.ai"
	}
	if options.Model == "" {
		options.Model = "sonar"
	}
	if options.MaxTokens == 0 {
		options.MaxTokens = 1024
	}
	return &PerplexitySearchProvider{options: options, client: webSearchProviderHTTPClient()}
}

func (p *PerplexitySearchProvider) ID() string { return "perplexity" }

func (p *PerplexitySearchProvider) Available() bool {
	return p.options.APIKey != "" && validAbsoluteURL(p.options.BaseURL) && p.options.Model != "" && p.options.MaxTokens > 0 &&
		(p.options.SearchRecency == "" || p.options.SearchRecency == "day" || p.options.SearchRecency == "week" || p.options.SearchRecency == "month" || p.options.SearchRecency == "year")
}

func (p *PerplexitySearchProvider) Search(ctx context.Context, request WebSearchRequest) (WebSearchResult, error) {
	body := map[string]any{
		"model":      p.options.Model,
		"max_tokens": p.options.MaxTokens,
		"messages":   []any{map[string]any{"role": "user", "content": request.Query}},
	}
	if p.options.SearchRecency != "" {
		body["search_recency_filter"] = p.options.SearchRecency
	}
	data, err := json.Marshal(body)
	if err != nil {
		return WebSearchResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.options.BaseURL+"/chat/completions", bytes.NewReader(data))
	if err != nil {
		return WebSearchResult{}, &WebError{Code: "WEB_PROVIDER_ERROR", Message: "Perplexity search request failed: " + err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+p.options.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", webSearchProviderUserAgent)
	response, err := p.client.Do(req)
	if err != nil {
		return WebSearchResult{}, namedSearchRequestError("Perplexity", err, ctx)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return WebSearchResult{}, providerHTTPError(response, ctx, "Perplexity")
	}
	var payload struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		SearchResults json.RawMessage `json:"search_results"`
		Citations     []string        `json:"citations"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return WebSearchResult{}, providerBodyError("Perplexity", err, ctx)
	}
	sources := []WebSearchSource{}
	if len(payload.SearchResults) > 0 {
		if bytes.Equal(bytes.TrimSpace(payload.SearchResults), []byte("null")) {
			return WebSearchResult{}, providerBodyError("Perplexity", fmt.Errorf("search_results is null"), ctx)
		}
		var results []struct {
			URL     string `json:"url"`
			Title   string `json:"title"`
			Snippet string `json:"snippet"`
			Date    string `json:"date"`
		}
		if err := json.Unmarshal(payload.SearchResults, &results); err != nil {
			return WebSearchResult{}, providerBodyError("Perplexity", err, ctx)
		}
		for _, result := range results {
			sources = append(sources, WebSearchSource{URL: result.URL, Title: result.Title, Snippet: result.Snippet, PublishedAt: result.Date})
		}
	} else {
		for _, citation := range payload.Citations {
			sources = append(sources, WebSearchSource{URL: citation})
		}
	}
	content := ""
	if len(payload.Choices) > 0 {
		content = payload.Choices[0].Message.Content
	}
	return WebSearchResult{Content: content, Sources: sources}, nil
}

func webSearchProviderHTTPClient() *http.Client {
	return &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
}

func validAbsoluteURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme != "" && parsed.Host != ""
}

func namedSearchRequestError(provider string, err error, ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return &WebError{Code: "WEB_ABORTED", Message: provider + " search aborted"}
	}
	return &WebError{Code: "WEB_PROVIDER_ERROR", Message: fmt.Sprintf("%s search request failed: %v", provider, err)}
}

func providerBodyError(provider string, err error, ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return &WebError{Code: "WEB_ABORTED", Message: provider + " search aborted"}
	}
	return &WebError{Code: "WEB_PROVIDER_ERROR", Message: fmt.Sprintf("%s returned an unprocessable response body: %v", provider, err)}
}

func providerHTTPError(response *http.Response, ctx context.Context, provider string) error {
	body, err := io.ReadAll(io.LimitReader(response.Body, 16<<10))
	if err != nil {
		return providerBodyError(provider, err, ctx)
	}
	message := fmt.Sprintf("%s API error (HTTP %d)", provider, response.StatusCode)
	var payload struct {
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
	}
	if json.Unmarshal(body, &payload) == nil {
		if strings.TrimSpace(payload.Message) != "" {
			message = strings.TrimSpace(payload.Message)
		}
		if len(payload.Error) > 0 {
			var detail struct {
				Message string `json:"message"`
			}
			var text string
			switch {
			case json.Unmarshal(payload.Error, &detail) == nil && strings.TrimSpace(detail.Message) != "":
				message = strings.TrimSpace(detail.Message)
			case json.Unmarshal(payload.Error, &text) == nil && strings.TrimSpace(text) != "":
				message = strings.TrimSpace(text)
			}
		}
	}
	return &WebError{Code: "WEB_PROVIDER_ERROR", Message: message}
}
