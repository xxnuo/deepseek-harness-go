package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	md "github.com/JohannesKaufmann/html-to-markdown"
	"github.com/JohannesKaufmann/html-to-markdown/plugin"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/htmlindex"
)

type WebSearchRequest struct {
	Query      string `json:"query"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type WebSearchSource struct {
	URL         string `json:"url"`
	Title       string `json:"title,omitempty"`
	Snippet     string `json:"snippet,omitempty"`
	PublishedAt string `json:"publishedAt,omitempty"`
}

type WebSearchResult struct {
	Content   string            `json:"content,omitempty"`
	Sources   []WebSearchSource `json:"sources"`
	Truncated bool              `json:"truncated"`
}

type WebSearchProvider interface {
	ID() string
	Available() bool
	Search(context.Context, WebSearchRequest) (WebSearchResult, error)
}

type WebFetchRequest struct {
	URL string `json:"url"`
}
type WebFetchBody struct {
	Kind    string `json:"kind"`
	Content string `json:"content"`
}
type WebFetchResult struct {
	URL        string       `json:"url"`
	StatusCode int          `json:"statusCode"`
	Body       WebFetchBody `json:"body"`
	Truncated  bool         `json:"truncated"`
}

type WebFetchProvider interface {
	ID() string
	Available() bool
	Fetch(context.Context, WebFetchRequest) (WebFetchResult, error)
}

type WebToolConfig struct {
	SearchEnabled       bool
	FetchEnabled        bool
	SearchMaxResults    int
	SearchMaxQueries    int
	SearchTimeout       time.Duration
	FetchTimeout        time.Duration
	FetchMaxOutputChars int
}

type HTTPWebFetchConfig struct {
	MaxURLLength     int
	MaxResponseBytes int64
	MaxBodyChars     int
	Timeout          time.Duration
	MaxRedirects     int
	UserAgent        string
}

func defaultWebToolConfig() *WebToolConfig {
	return &WebToolConfig{
		SearchEnabled: true, SearchMaxResults: 8, SearchMaxQueries: 4,
		SearchTimeout: 30 * time.Second, FetchTimeout: 30 * time.Second,
		FetchMaxOutputChars: 200_000,
	}
}

func DefaultWebToolConfig() WebToolConfig { return *defaultWebToolConfig() }

func normalizeWebToolConfig(config *WebToolConfig) *WebToolConfig {
	defaults := defaultWebToolConfig()
	if config == nil {
		return defaults
	}
	clone := *config
	if clone.SearchMaxResults == 0 {
		clone.SearchMaxResults = defaults.SearchMaxResults
	}
	if clone.SearchMaxQueries == 0 {
		clone.SearchMaxQueries = defaults.SearchMaxQueries
	}
	if clone.SearchTimeout == 0 {
		clone.SearchTimeout = defaults.SearchTimeout
	}
	if clone.FetchTimeout == 0 {
		clone.FetchTimeout = defaults.FetchTimeout
	}
	if clone.FetchMaxOutputChars == 0 {
		clone.FetchMaxOutputChars = defaults.FetchMaxOutputChars
	}
	return &clone
}

func defaultHTTPWebFetchConfig() HTTPWebFetchConfig {
	return HTTPWebFetchConfig{
		MaxURLLength: 2048, MaxResponseBytes: 5_000_000, MaxBodyChars: 100_000,
		Timeout: 30 * time.Second, MaxRedirects: 5,
		UserAgent: "deepseek-harness/0.0.1 (+https://github.com/deepseek-ai)",
	}
}

func DefaultHTTPWebFetchConfig() HTTPWebFetchConfig { return defaultHTTPWebFetchConfig() }

func normalizeHTTPWebFetchConfig(config HTTPWebFetchConfig) HTTPWebFetchConfig {
	if config == (HTTPWebFetchConfig{}) {
		return defaultHTTPWebFetchConfig()
	}
	return config
}

func validateWebRuntimeConfig(config Config) error {
	if config.WebTools == nil || config.WebTools.SearchMaxResults < 1 || config.WebTools.SearchMaxQueries < 1 || config.WebTools.SearchTimeout <= 0 || config.WebTools.FetchTimeout <= 0 || config.WebTools.FetchMaxOutputChars < 1 {
		return errors.New("tool-web limits must be positive")
	}
	fetch := config.HTTPWebFetch
	if fetch.MaxURLLength <= 0 || fetch.MaxResponseBytes <= 0 || fetch.MaxBodyChars <= 0 || fetch.Timeout <= 0 {
		return errors.New("web-fetch-http limits must be positive")
	}
	if fetch.Timeout > time.Duration(2_147_483_647)*time.Millisecond {
		return errors.New("web-fetch-http timeout must be no greater than 2147483647ms")
	}
	if fetch.MaxRedirects < 0 {
		return errors.New("web-fetch-http maxRedirects must be a non-negative integer")
	}
	return nil
}

type WebError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *WebError) Error() string { return e.Message }

// webSessionContextKey keeps the owning session out of the public provider
// interface while allowing model-visible auxiliary requests to be logged by
// the built-in provider.
type webSessionContextKey struct{}

func withWebSession(ctx context.Context, sessionID string) context.Context {
	if sessionID == "" {
		return ctx
	}
	return context.WithValue(ctx, webSessionContextKey{}, sessionID)
}

func webSessionID(ctx context.Context) string {
	if value, ok := ctx.Value(webSessionContextKey{}).(string); ok {
		return value
	}
	return ""
}

func (e *Engine) RegisterWebSearchProvider(provider WebSearchProvider) error {
	if provider == nil {
		return errors.New("web provider is nil")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	id := strings.TrimSpace(provider.ID())
	if id == "" {
		return errors.New("web provider id is required")
	}
	if _, exists := e.webSearchProviders[id]; exists {
		return &WebError{Code: "WEB_DUPLICATE_PROVIDER", Message: fmt.Sprintf("a web provider with id %q is already registered", id)}
	}
	e.webSearchProviders[id] = provider
	return nil
}
func (e *Engine) RegisterWebFetchProvider(provider WebFetchProvider) error {
	if provider == nil {
		return errors.New("web provider is nil")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	id := strings.TrimSpace(provider.ID())
	if id == "" {
		return errors.New("web provider id is required")
	}
	if _, exists := e.webFetchProviders[id]; exists {
		return &WebError{Code: "WEB_DUPLICATE_PROVIDER", Message: fmt.Sprintf("a web provider with id %q is already registered", id)}
	}
	e.webFetchProviders[id] = provider
	return nil
}

func (e *Engine) webSearch(ctx context.Context, req WebSearchRequest) (WebSearchResult, error) {
	if strings.TrimSpace(req.Query) == "" {
		return WebSearchResult{}, errors.New("query must be a non-empty string")
	}
	provider, err := e.selectWebSearchProvider()
	if err != nil {
		return WebSearchResult{}, err
	}
	result, err := provider.Search(ctx, req)
	if err != nil {
		return WebSearchResult{}, err
	}
	if req.MaxResults > 0 && len(result.Sources) > req.MaxResults {
		result.Truncated = true
		result.Sources = result.Sources[:req.MaxResults]
	}
	return result, nil
}

func (e *Engine) webFetch(ctx context.Context, req WebFetchRequest) (WebFetchResult, error) {
	if strings.TrimSpace(req.URL) == "" {
		return WebFetchResult{}, errors.New("url must be a non-empty string")
	}
	provider, err := e.selectWebFetchProvider()
	if err != nil {
		return WebFetchResult{}, err
	}
	return provider.Fetch(ctx, req)
}

func (e *Engine) selectWebSearchProvider() (WebSearchProvider, error) {
	e.mu.RLock()
	configured := strings.TrimSpace(e.cfg.WebSearchProvider)
	providers := make(map[string]WebSearchProvider, len(e.webSearchProviders))
	for id, provider := range e.webSearchProviders {
		providers[id] = provider
	}
	e.mu.RUnlock()
	if configured != "" {
		provider, ok := providers[configured]
		if !ok {
			return nil, &WebError{Code: "WEB_PROVIDER_CONFIGURED_MISSING", Message: fmt.Sprintf("configured web provider %q is not registered", configured)}
		}
		if !provider.Available() {
			return nil, &WebError{Code: "WEB_PROVIDER_CONFIGURED_UNAVAILABLE", Message: fmt.Sprintf("configured web provider %q is registered but unavailable", configured)}
		}
		return provider, nil
	}
	return chooseWebProvider(providers)
}

func (e *Engine) selectWebFetchProvider() (WebFetchProvider, error) {
	e.mu.RLock()
	configured := strings.TrimSpace(e.cfg.WebFetchProvider)
	providers := make(map[string]WebFetchProvider, len(e.webFetchProviders))
	for id, provider := range e.webFetchProviders {
		providers[id] = provider
	}
	e.mu.RUnlock()
	if configured != "" {
		provider, ok := providers[configured]
		if !ok {
			return nil, &WebError{Code: "WEB_PROVIDER_CONFIGURED_MISSING", Message: fmt.Sprintf("configured web provider %q is not registered", configured)}
		}
		if !provider.Available() {
			return nil, &WebError{Code: "WEB_PROVIDER_CONFIGURED_UNAVAILABLE", Message: fmt.Sprintf("configured web provider %q is registered but unavailable", configured)}
		}
		return provider, nil
	}
	return chooseWebProvider(providers)
}

func chooseWebProvider[P interface {
	ID() string
	Available() bool
}](providers map[string]P) (P, error) {
	var zero P
	ids := make([]string, 0, len(providers))
	for id, provider := range providers {
		if provider.Available() {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return zero, &WebError{Code: "WEB_PROVIDER_UNAVAILABLE", Message: "no usable web provider is registered"}
	}
	if len(ids) > 1 {
		return zero, &WebError{Code: "WEB_PROVIDER_AMBIGUOUS", Message: fmt.Sprintf("multiple usable web providers are registered (%s); configure one explicitly", strings.Join(ids, ", "))}
	}
	return providers[ids[0]], nil
}

func registerWebTools(e *Engine) error {
	config := e.cfg.WebTools
	if config.SearchEnabled {
		description := fmt.Sprintf("Search the web for current information. Provide 1-%d queries in the required queries array.", config.SearchMaxQueries)
		if err := e.RegisterTool(Tool{Schema: ToolSchema{Name: "web_search", Description: description, Parameters: objectSchema(map[string]any{
			"queries": map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": fmt.Sprintf("Required search queries; accepts 1-%d items and merges their results.", config.SearchMaxQueries)},
		}, "queries"), Output: objectSchema(map[string]any{
			"content": map[string]any{"type": "string"}, "sources": map[string]any{"type": "array", "items": objectSchema(map[string]any{"url": map[string]any{"type": "string"}, "title": map[string]any{"type": "string"}, "snippet": map[string]any{"type": "string"}, "publishedAt": map[string]any{"type": "string"}}, "url")}, "truncated": map[string]any{"type": "boolean"},
		}, "sources", "truncated")}, Timeout: config.SearchTimeout, Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in struct {
				Queries []string `json:"queries"`
			}
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			queries, err := parseWebSearchQueries(in.Queries, config.SearchMaxQueries)
			if err != nil {
				return ToolResult{}, err
			}
			result, err := e.runWebSearchQueries(ctx, call.SessionID, queries, config.SearchMaxResults)
			if err != nil {
				return ToolResult{}, err
			}
			meta := map[string]any{"sources": result.Sources, "truncated": result.Truncated}
			if result.Content != "" {
				meta["answer"] = result.Content
			}
			return ToolResult{Content: []ContentBlock{{Type: "text", Text: formatWebSearch(result)}}, Value: result, Meta: meta}, nil
		}}); err != nil {
			return err
		}
	}
	if !config.FetchEnabled {
		return nil
	}
	return e.RegisterTool(Tool{Schema: ToolSchema{Name: "web_fetch", Description: "Fetch a public web URL.", Parameters: objectSchema(map[string]any{"url": map[string]any{"type": "string"}}, "url"), Output: objectSchema(map[string]any{
		"url": map[string]any{"type": "string"}, "statusCode": map[string]any{"type": "integer"}, "body": objectSchema(map[string]any{"kind": map[string]any{"type": "string", "enum": []string{"html", "text"}}, "content": map[string]any{"type": "string"}}, "kind", "content"), "truncated": map[string]any{"type": "boolean"},
	}, "url", "statusCode", "body", "truncated")}, Timeout: config.FetchTimeout, Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
		var in struct {
			URL string `json:"url"`
		}
		if err := decodeToolArguments(call, &in); err != nil {
			return ToolResult{}, err
		}
		result, err := e.webFetch(ctx, WebFetchRequest{URL: in.URL})
		if err != nil {
			return ToolResult{}, err
		}
		text, truncated := renderWebFetch(result, config.FetchMaxOutputChars)
		return ToolResult{Content: []ContentBlock{{Type: "text", Text: text}}, Value: result, Meta: map[string]any{"url": result.URL, "statusCode": result.StatusCode, "truncated": truncated}}, nil
	}})
}

func parseWebSearchQueries(queries []string, maxQueries int) ([]string, error) {
	if len(queries) == 0 {
		return nil, errors.New("queries must contain at least one query")
	}
	if len(queries) > maxQueries {
		noun := "queries"
		if maxQueries == 1 {
			noun = "query"
		}
		return nil, fmt.Errorf("queries must contain at most %d %s", maxQueries, noun)
	}
	seen := make(map[string]struct{}, len(queries))
	unique := make([]string, 0, len(queries))
	for _, query := range queries {
		if strings.TrimSpace(query) == "" {
			return nil, errors.New("each query must be a non-empty string")
		}
		if _, exists := seen[query]; exists {
			continue
		}
		seen[query] = struct{}{}
		unique = append(unique, query)
	}
	return unique, nil
}

func (e *Engine) runWebSearchQueries(ctx context.Context, sessionID string, queries []string, maxResults int) (WebSearchResult, error) {
	if len(queries) == 1 {
		return e.webSearch(withWebSession(ctx, sessionID), WebSearchRequest{Query: queries[0], MaxResults: maxResults})
	}
	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]WebSearchResult, len(queries))
	var wg sync.WaitGroup
	var failureMu sync.Mutex
	var firstFailure error
	for index, query := range queries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := e.webSearch(withWebSession(batchCtx, sessionID), WebSearchRequest{Query: query, MaxResults: maxResults})
			if err != nil {
				failureMu.Lock()
				if firstFailure == nil {
					firstFailure = err
					cancel()
				}
				failureMu.Unlock()
				return
			}
			results[index] = result
		}()
	}
	wg.Wait()
	if firstFailure != nil {
		return WebSearchResult{}, firstFailure
	}
	return mergeWebSearchResults(queries, results, maxResults), nil
}

func mergeWebSearchResults(queries []string, results []WebSearchResult, maxResults int) WebSearchResult {
	maxRank := 0
	truncated := false
	for _, result := range results {
		if len(result.Sources) > maxRank {
			maxRank = len(result.Sources)
		}
		truncated = truncated || result.Truncated
	}
	seen := make(map[string]struct{})
	sources := make([]WebSearchSource, 0, maxResults)
	mergeDone := false
	for rank := 0; rank < maxRank && !mergeDone; rank++ {
		for _, result := range results {
			if rank >= len(result.Sources) {
				continue
			}
			source := result.Sources[rank]
			if _, exists := seen[source.URL]; exists {
				continue
			}
			seen[source.URL] = struct{}{}
			if len(sources) == maxResults {
				truncated = true
				mergeDone = true
				break
			}
			sources = append(sources, source)
		}
	}
	contents := make([]string, 0, len(results))
	for index, result := range results {
		if result.Content != "" {
			contents = append(contents, "### "+queries[index]+"\n\n"+result.Content)
		}
	}
	return WebSearchResult{Content: strings.Join(contents, "\n\n"), Sources: sources, Truncated: truncated}
}

func formatWebSearch(result WebSearchResult) string {
	parts := []string{}
	if result.Content != "" {
		parts = append(parts, result.Content)
	}
	if len(result.Sources) == 0 && result.Content == "" {
		parts = append(parts, "No results found.")
	}
	if len(result.Sources) > 0 {
		lines := make([]string, 0, len(result.Sources))
		for _, source := range result.Sources {
			label := source.Title
			if label == "" {
				if parsed, err := url.Parse(source.URL); err == nil {
					label = parsed.Host
				}
				if label == "" {
					label = source.URL
				}
			}
			suffix := source.Snippet
			if source.PublishedAt != "" {
				if suffix != "" {
					suffix += " "
				}
				suffix += "(" + source.PublishedAt + ")"
			}
			if suffix != "" {
				suffix = " — " + suffix
			}
			lines = append(lines, fmt.Sprintf("- [%s](%s)%s", label, source.URL, suffix))
		}
		parts = append(parts, "Sources:\n"+strings.Join(lines, "\n"))
	}
	if result.Truncated {
		parts = append(parts, fmt.Sprintf("(Showing the first %d sources. Refine the query for more.)", len(result.Sources)))
	}
	parts = append(parts, "Cite the relevant URLs above as markdown links in your answer.")
	return strings.Join(parts, "\n\n")
}

func formatWebFetch(result WebFetchResult) string {
	text, _ := renderWebFetch(result, defaultWebToolConfig().FetchMaxOutputChars)
	return text
}

const maxHTMLConversionDepth = 512

var htmlVoidElements = map[string]bool{
	"area": true, "base": true, "br": true, "col": true, "embed": true,
	"hr": true, "img": true, "input": true, "link": true, "meta": true,
	"param": true, "source": true, "track": true, "wbr": true,
}

var htmlRawTextElements = map[string]bool{"script": true, "style": true, "noscript": true}

func exceedsHTMLConversionDepth(source string) bool {
	lower := strings.ToLower(source)
	open := make([]string, 0, 32)
	offset := 0
	inComment := false
	for offset < len(source) {
		start := strings.IndexByte(source[offset:], '<')
		if start >= 0 {
			start += offset
		}
		if inComment {
			end := strings.Index(source[offset:], "-->")
			if end >= 0 {
				end += offset
			}
			if end >= 0 && (start < 0 || end < start) {
				inComment = false
				offset = end + 3
				continue
			}
		}
		if start < 0 {
			break
		}
		if !inComment && strings.HasPrefix(source[start:], "<!--") {
			inComment = true
			offset = start + 4
			continue
		}

		cursor := start + 1
		closing := cursor < len(source) && source[cursor] == '/'
		if closing {
			cursor++
		}
		nameStart := cursor
		for cursor < len(source) && isHTMLTagNameByte(source[cursor]) {
			cursor++
		}
		if cursor == nameStart || !isASCIIAlpha(source[nameStart]) {
			offset = start + 1
			continue
		}
		name := lower[nameStart:cursor]
		var quote byte
		for cursor < len(source) {
			char := source[cursor]
			cursor++
			if quote != 0 {
				if char == quote {
					quote = 0
				}
			} else if char == '\'' || char == '"' {
				quote = char
			} else if char == '>' {
				break
			}
		}
		if cursor == 0 || source[cursor-1] != '>' {
			break
		}

		if closing {
			if !inComment && len(open) > 0 && open[len(open)-1] == name {
				open = open[:len(open)-1]
			}
		} else {
			last := cursor - 2
			for last >= 0 && isASCIIWhitespace(source[last]) {
				last--
			}
			if !htmlVoidElements[name] && (last < 0 || source[last] != '/') {
				open = append(open, name)
				if len(open) > maxHTMLConversionDepth {
					return true
				}
				if !inComment && htmlRawTextElements[name] {
					end := findHTMLRawTextEnd(lower, name, cursor)
					if end < 0 {
						break
					}
					offset = end
					continue
				}
			}
		}
		offset = cursor
	}
	return false
}

func isASCIIAlpha(char byte) bool {
	return char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z'
}

func isHTMLTagNameByte(char byte) bool {
	return isASCIIAlpha(char) || char >= '0' && char <= '9' || char == '-'
}

func isASCIIWhitespace(char byte) bool {
	return char == ' ' || char == '\t' || char == '\n' || char == '\r' || char == '\f'
}

func findHTMLRawTextEnd(lower, name string, offset int) int {
	prefix := "</" + name
	for {
		relative := strings.Index(lower[offset:], prefix)
		if relative < 0 {
			return -1
		}
		candidate := offset + relative
		boundary := candidate + len(prefix)
		if boundary == len(lower) || lower[boundary] == '>' || lower[boundary] == '/' || isASCIIWhitespace(lower[boundary]) {
			return candidate
		}
		offset = boundary
	}
}

func renderWebFetch(result WebFetchResult, maxChars int) (string, bool) {
	content := result.Body.Content
	sourceTruncated := false
	runes := []rune(content)
	if len(runes) > maxChars {
		content = string(runes[:maxChars])
		sourceTruncated = true
	}
	if result.Body.Kind == "html" && !exceedsHTMLConversionDepth(content) {
		converter := md.NewConverter(result.URL, true, nil).Use(plugin.GitHubFlavored())
		if converted, err := converter.ConvertString(content); err == nil {
			content = converted
		}
	}
	prefix := fmt.Sprintf("Fetched %s (HTTP %d)\n\n%s", result.URL, result.StatusCode, content)
	footer := "\n\n(Content truncated. Fetch a more specific URL or section for the full text.)"
	truncated := result.Truncated || sourceTruncated || len([]rune(prefix)) > maxChars
	output := prefix
	if truncated {
		output += footer
	}
	outputRunes := []rune(output)
	if len(outputRunes) <= maxChars {
		return output, truncated
	}
	footerRunes := []rune(footer)
	if maxChars < len(footerRunes) {
		return string(outputRunes[:maxChars]), true
	}
	prefixRunes := []rune(prefix)
	return string(prefixRunes[:maxChars-len(footerRunes)]) + footer, true
}

type deepSeekWebSearchProvider struct{ engine *Engine }

func (p *deepSeekWebSearchProvider) ID() string { return "deepseek-official" }
func (p *deepSeekWebSearchProvider) Available() bool {
	return p.snapshot().valid
}
func (p *deepSeekWebSearchProvider) Search(ctx context.Context, req WebSearchRequest) (WebSearchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return WebSearchResult{}, webSearchContextError(err, ctx)
	}
	options := p.snapshot()
	if !options.valid {
		return WebSearchResult{}, &WebError{Code: "WEB_PROVIDER_ERROR", Message: "DeepSeek search provider configuration is invalid"}
	}
	if options.apiKey == "" {
		return WebSearchResult{}, &WebError{Code: "WEB_PROVIDER_CREDENTIAL_MISSING", Message: fmt.Sprintf(
			"DeepSeek search has no API key for %q; store it through the credentials service (the web Models page writes it), export it in the launching environment, or set a literal \"apiKey\" in the web-search-deepseek config",
			options.apiKeyEnv,
		)}
	}
	body := map[string]any{
		"model":      options.model,
		"max_tokens": options.maxTokens,
		"messages": []any{map[string]any{
			"role": "user",
			"content": []any{map[string]any{
				"type": "text",
				"text": "Perform a web search for the query: " + req.Query,
			}},
		}},
		"tools": []any{map[string]any{
			"type":     "web_search_20250305",
			"name":     "web_search",
			"max_uses": options.maxUses,
		}},
	}
	data, _ := json.Marshal(body)
	endpoint := options.baseURL + "/messages"
	if sessionID := webSessionID(ctx); sessionID != "" {
		session, sessionErr := p.engine.getSession(sessionID)
		if sessionErr != nil {
			return WebSearchResult{}, sessionErr
		}
		// Keep this event secret-free and append it immediately before the
		// network dispatch. The request body is exactly what the provider sends.
		if _, eventErr := p.engine.appendEvent(session, "web/deepseek-search-llm-request", map[string]any{
			"endpoint":   endpoint,
			"apiVersion": options.apiVersion,
			"body":       body,
		}); eventErr != nil {
			return WebSearchResult{}, eventErr
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return WebSearchResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("x-api-key", options.apiKey)
	request.Header.Set("Authorization", "Bearer "+options.apiKey)
	request.Header.Set("anthropic-version", options.apiVersion)
	request.Header.Set("User-Agent", "deepseek-harness/0.0.1")
	client := &http.Client{
		Timeout: 60 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			// Search requests carry credentials and must never be forwarded to a
			// different origin (or even silently follow a same-origin redirect).
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(request)
	if err != nil {
		return WebSearchResult{}, webSearchContextError(err, ctx)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return WebSearchResult{}, deepSeekHTTPError(resp, ctx)
	}
	var payload struct {
		Content []json.RawMessage `json:"content"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return WebSearchResult{}, webSearchContextError(err, ctx)
	}
	// Citations and result blocks are independent content blocks. Collect all
	// citations first so a citation appearing after its result still joins.
	snippets := map[string]string{}
	for _, raw := range payload.Content {
		var block struct {
			Type      string `json:"type"`
			Citations []struct {
				URL  string `json:"url"`
				Text string `json:"cited_text"`
			} `json:"citations"`
			Content []struct {
				Type    string `json:"type"`
				URL     string `json:"url"`
				Title   string `json:"title"`
				PageAge string `json:"page_age"`
			} `json:"content"`
		}
		if json.Unmarshal(raw, &block) != nil {
			continue
		}
		if block.Type == "text" {
			for _, citation := range block.Citations {
				if citation.URL != "" && citation.Text != "" && snippets[citation.URL] == "" {
					snippets[citation.URL] = citation.Text
				}
			}
		}
	}
	sources := []WebSearchSource{}
	seen := map[string]bool{}
	found := false
	for _, raw := range payload.Content {
		var block struct {
			Type    string `json:"type"`
			Content []struct {
				Type    string `json:"type"`
				URL     string `json:"url"`
				Title   string `json:"title"`
				PageAge string `json:"page_age"`
			} `json:"content"`
		}
		if json.Unmarshal(raw, &block) != nil || block.Type != "web_search_tool_result" {
			continue
		}
		found = true
		for _, item := range block.Content {
			if item.Type != "web_search_result" || item.URL == "" || seen[item.URL] {
				continue
			}
			seen[item.URL] = true
			sources = append(sources, WebSearchSource{URL: item.URL, Title: item.Title, Snippet: snippets[item.URL], PublishedAt: item.PageAge})
		}
	}
	if !found {
		return WebSearchResult{}, &WebError{Code: "WEB_PROVIDER_ERROR", Message: "DeepSeek returned no web_search_tool_result blocks; the request may not have triggered native web search"}
	}
	return WebSearchResult{Sources: sources}, nil
}

const (
	deepSeekSearchDefaultBaseURL    = "https://api.deepseek.com/anthropic/v1"
	deepSeekSearchDefaultModel      = "deepseek-v4-flash"
	deepSeekSearchDefaultAPIVersion = "2023-06-01"
	deepSeekSearchDefaultMaxTokens  = 4096
	deepSeekSearchDefaultMaxUses    = 5
)

func deepSeekWebSearchBaseSettings() map[string]any {
	return map[string]any{
		"apiKeyEnv":  "DEEPSEEK_API_KEY",
		"model":      deepSeekSearchDefaultModel,
		"apiVersion": deepSeekSearchDefaultAPIVersion,
		"maxTokens":  deepSeekSearchDefaultMaxTokens,
		"maxUses":    deepSeekSearchDefaultMaxUses,
	}
}

func deepSeekWebSearchSettingsSchema() map[string]any {
	return map[string]any{
		"uid": 8,
		"refs": map[string]any{
			"1": map[string]any{"type": "string", "meta": map[string]any{"role": "secret"}},
			"2": map[string]any{"type": "string", "meta": map[string]any{"role": "credential-ref", "default": "DEEPSEEK_API_KEY"}},
			"3": map[string]any{"type": "string", "meta": map[string]any{}},
			"4": map[string]any{"type": "string", "meta": map[string]any{"default": deepSeekSearchDefaultModel}},
			"5": map[string]any{"type": "string", "meta": map[string]any{"default": deepSeekSearchDefaultAPIVersion}},
			"6": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekSearchDefaultMaxTokens}},
			"7": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekSearchDefaultMaxUses}},
			"8": map[string]any{"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{"apiKey": 1, "apiKeyEnv": 2, "baseURL": 3, "model": 4, "apiVersion": 5, "maxTokens": 6, "maxUses": 7}},
		},
	}
}

type deepSeekWebSearchOptions struct {
	apiKey     string
	apiKeyEnv  string
	baseURL    string
	model      string
	apiVersion string
	maxTokens  int
	maxUses    int
	valid      bool
}

func (p *deepSeekWebSearchProvider) snapshot() deepSeekWebSearchOptions {
	p.engine.mu.RLock()
	settings := mergeSettings(deepSeekWebSearchBaseSettings(), p.engine.settings["web-search-deepseek"])
	ref := stringSetting(settings["apiKeyEnv"])
	if ref == "" {
		ref = "DEEPSEEK_API_KEY"
	}
	p.engine.mu.RUnlock()

	apiKey := stringSetting(settings["apiKey"])
	if apiKey == "" {
		apiKey, _, _ = p.engine.resolveCredential(ref)
	}
	baseURL := stringSetting(settings["baseURL"])
	if baseURL == "" {
		baseURL = strings.TrimSpace(os.Getenv("DEEPSEEK_SEARCH_BASE_URL"))
	}
	if baseURL == "" {
		baseURL = deepSeekSearchDefaultBaseURL
	}
	model := stringSetting(settings["model"])
	apiVersion := stringSetting(settings["apiVersion"])
	maxTokens, tokensOK := strictPositiveIntSetting(settings["maxTokens"])
	maxUses, usesOK := strictPositiveIntSetting(settings["maxUses"])
	parsed, urlErr := url.Parse(baseURL)
	validURL := urlErr == nil && parsed.Scheme != "" && parsed.Host != ""
	return deepSeekWebSearchOptions{
		apiKey: apiKey, apiKeyEnv: ref, baseURL: baseURL, model: model, apiVersion: apiVersion,
		maxTokens: maxTokens, maxUses: maxUses,
		valid: validURL && model != "" && apiVersion != "" && tokensOK && usesOK,
	}
}

func strictPositiveIntSetting(value any) (int, bool) {
	switch number := value.(type) {
	case int:
		return number, number > 0
	case int64:
		return int(number), number > 0 && number <= int64(^uint(0)>>1)
	case float64:
		return int(number), number > 0 && number == float64(int(number))
	default:
		return 0, false
	}
}

func webSearchContextError(err error, ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return &WebError{Code: "WEB_ABORTED", Message: "web search aborted"}
	}
	if webErr, ok := err.(*WebError); ok {
		return webErr
	}
	return &WebError{Code: "WEB_PROVIDER_ERROR", Message: err.Error()}
}

func deepSeekHTTPError(resp *http.Response, ctx context.Context) error {
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
	if err != nil {
		return webSearchContextError(err, ctx)
	}
	message := ""
	var payload struct {
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
	}
	if json.Unmarshal(body, &payload) == nil {
		message = strings.TrimSpace(payload.Message)
		if len(payload.Error) > 0 {
			var detail struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(payload.Error, &detail) == nil && strings.TrimSpace(detail.Message) != "" {
				message = strings.TrimSpace(detail.Message)
			} else {
				var text string
				if json.Unmarshal(payload.Error, &text) == nil {
					message = strings.TrimSpace(text)
				}
			}
		}
	}
	if message == "" {
		message = fmt.Sprintf("DeepSeek API error (HTTP %d)", resp.StatusCode)
	}
	return &WebError{Code: "WEB_PROVIDER_ERROR", Message: message}
}

type HTTPWebFetchProvider struct {
	client       *http.Client
	MaxURLLength int
	MaxBodyBytes int64
	MaxBodyChars int
	MaxRedirects int
	Timeout      time.Duration
	UserAgent    string
}

func NewHTTPWebFetchProvider(configs ...HTTPWebFetchConfig) *HTTPWebFetchProvider {
	config := defaultHTTPWebFetchConfig()
	if len(configs) > 0 {
		config = normalizeHTTPWebFetchConfig(configs[0])
	}
	return &HTTPWebFetchProvider{
		client: &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			// Redirects are resolved by Fetch so origin and hop policies are
			// applied before another request is made.
			return http.ErrUseLastResponse
		}},
		MaxURLLength: config.MaxURLLength,
		MaxBodyBytes: config.MaxResponseBytes,
		MaxBodyChars: config.MaxBodyChars,
		MaxRedirects: config.MaxRedirects,
		Timeout:      config.Timeout,
		UserAgent:    config.UserAgent,
	}
}
func (p *HTTPWebFetchProvider) ID() string      { return "http" }
func (p *HTTPWebFetchProvider) Available() bool { return true }
func (p *HTTPWebFetchProvider) Fetch(ctx context.Context, req WebFetchRequest) (WebFetchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return WebFetchResult{}, webFetchContextError(err, ctx, ctx)
	}
	parsed, err := validateWebFetchURL(req.URL, p.MaxURLLength)
	if err != nil {
		return WebFetchResult{}, err
	}
	operationCtx := ctx
	var cancel context.CancelFunc
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	operationCtx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := p.fetchURL(operationCtx, parsed)
	if err != nil {
		return WebFetchResult{}, webFetchContextError(err, ctx, operationCtx)
	}
	return result, nil
}

func (p *HTTPWebFetchProvider) fetchURL(ctx context.Context, current *url.URL) (WebFetchResult, error) {
	client := p.client
	if client == nil {
		client = &http.Client{}
	}
	// Clone the client so a caller-provided transport is retained while the
	// provider's manual redirect policy cannot be overridden accidentally.
	clientCopy := *client
	clientCopy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	clientCopy.Timeout = 0 // the operation context owns timeout classification
	maxRedirects := p.MaxRedirects
	if maxRedirects < 0 {
		maxRedirects = 0
	}
	for redirects := 0; ; {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, current.String(), nil)
		if err != nil {
			return WebFetchResult{}, err
		}
		request.Header.Set("Accept", "text/html,application/xhtml+xml,text/*;q=.9,application/json;q=.8")
		request.Header.Set("User-Agent", p.UserAgent)
		response, err := clientCopy.Do(request)
		if err != nil {
			return WebFetchResult{}, err
		}
		if isHTTPRedirect(response.StatusCode) {
			if redirects >= maxRedirects {
				_ = response.Body.Close()
				return WebFetchResult{}, &WebError{Code: "WEB_REDIRECT_BLOCKED", Message: fmt.Sprintf("exceeded the maximum of %d redirects", maxRedirects)}
			}
			location := strings.TrimSpace(response.Header.Get("Location"))
			if location == "" {
				_ = response.Body.Close()
				return WebFetchResult{}, &WebError{Code: "WEB_PROVIDER_ERROR", Message: fmt.Sprintf("redirect response (HTTP %d) without a Location header", response.StatusCode)}
			}
			target, resolveErr := current.Parse(location)
			_ = response.Body.Close()
			if resolveErr != nil {
				return WebFetchResult{}, &WebError{Code: "WEB_PROVIDER_ERROR", Message: fmt.Sprintf("invalid redirect Location %q", location)}
			}
			validated, validateErr := validateWebFetchURL(target.String(), p.MaxURLLength)
			if validateErr != nil {
				return WebFetchResult{}, validateErr
			}
			if !sameWebOrigin(current, validated) {
				return WebFetchResult{}, &WebError{Code: "WEB_REDIRECT_BLOCKED", Message: fmt.Sprintf("cross-origin redirect to %s is not followed automatically", webOrigin(validated))}
			}
			current = validated
			redirects++
			continue
		}
		return p.readWebFetchResponse(response, current)
	}
}

func (p *HTTPWebFetchProvider) readWebFetchResponse(response *http.Response, requestURL *url.URL) (WebFetchResult, error) {
	defer response.Body.Close()
	contentType := response.Header.Get("Content-Type")
	kind, decoder, decodeErr := fetchContentDecoder(contentType)
	if decodeErr != nil {
		return WebFetchResult{}, decodeErr
	}
	maxBytes := p.MaxBodyBytes
	if maxBytes <= 0 {
		maxBytes = 5_000_000
	}
	if declared := strings.TrimSpace(response.Header.Get("Content-Length")); declared != "" {
		if length, err := strconv.ParseInt(declared, 10, 64); err == nil && length > maxBytes {
			return WebFetchResult{}, &WebError{Code: "WEB_FETCH_TOO_LARGE", Message: fmt.Sprintf("response exceeds the maximum of %d bytes", maxBytes)}
		}
	}
	limited := io.LimitReader(response.Body, maxBytes+1)
	body, err := io.ReadAll(limited)
	truncated := int64(len(body)) > maxBytes
	if truncated {
		body = body[:maxBytes]
	}
	if err != nil {
		return WebFetchResult{}, err
	}
	decoded, err := decoder.NewDecoder().Bytes(body)
	if err != nil {
		return WebFetchResult{}, err
	}
	content := string(decoded)
	maxChars := p.MaxBodyChars
	if maxChars <= 0 {
		maxChars = 100_000
	}
	runes := []rune(content)
	if len(runes) > maxChars {
		content = string(runes[:maxChars])
		truncated = true
	}
	return WebFetchResult{URL: requestURL.String(), StatusCode: response.StatusCode, Body: WebFetchBody{Kind: kind, Content: content}, Truncated: truncated}, nil
}

func validateWebFetchURL(raw string, maxLength int) (*url.URL, error) {
	if len(raw) > maxLength {
		return nil, &WebError{Code: "WEB_INVALID_URL", Message: fmt.Sprintf("URL exceeds the maximum length of %d", maxLength)}
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, &WebError{Code: "WEB_INVALID_URL", Message: "invalid URL"}
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, &WebError{Code: "WEB_INVALID_URL", Message: "only http and https URLs are allowed"}
	}
	if parsed.User != nil {
		return nil, &WebError{Code: "WEB_BLOCKED_URL", Message: "credentials in URLs are not allowed"}
	}
	return parsed, nil
}

func sameWebOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Hostname(), b.Hostname()) && effectiveWebPort(a) == effectiveWebPort(b)
}

func effectiveWebPort(value *url.URL) string {
	if port := value.Port(); port != "" {
		return port
	}
	if strings.EqualFold(value.Scheme, "https") {
		return "443"
	}
	return "80"
}

func webOrigin(value *url.URL) string {
	return value.Scheme + "://" + value.Host
}

func isHTTPRedirect(status int) bool {
	return status == http.StatusMovedPermanently || status == http.StatusFound || status == http.StatusSeeOther || status == http.StatusTemporaryRedirect || status == http.StatusPermanentRedirect
}

func webFetchContextError(err error, parent, operation context.Context) error {
	if parent != nil && parent.Err() != nil {
		return &WebError{Code: "WEB_ABORTED", Message: "web fetch aborted"}
	}
	if operation != nil && operation.Err() == context.DeadlineExceeded {
		return &WebError{Code: "WEB_FETCH_TIMEOUT", Message: "web fetch timed out"}
	}
	if webErr, ok := err.(*WebError); ok {
		return webErr
	}
	return &WebError{Code: "WEB_PROVIDER_ERROR", Message: err.Error()}
}

func fetchContentDecoder(contentType string) (string, encoding.Encoding, error) {
	parts := strings.Split(contentType, ";")
	mimeType := strings.ToLower(strings.TrimSpace(parts[0]))
	kind := "text"
	if mimeType == "text/html" || mimeType == "application/xhtml+xml" {
		kind = "html"
	}
	if !strings.HasPrefix(mimeType, "text/") && mimeType != "application/json" && mimeType != "application/xml" && !strings.HasSuffix(mimeType, "+json") && !strings.HasSuffix(mimeType, "+xml") {
		return "", nil, &WebError{Code: "WEB_UNSUPPORTED_CONTENT_TYPE", Message: fmt.Sprintf("unsupported content type %q", contentType)}
	}
	charset := "utf-8"
	for _, part := range parts[1:] {
		keyValue := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(keyValue) == 2 && strings.EqualFold(strings.TrimSpace(keyValue[0]), "charset") {
			charset = strings.Trim(strings.TrimSpace(keyValue[1]), "\"")
		}
	}
	decoder, err := htmlindex.Get(charset)
	if err != nil {
		return "", nil, &WebError{Code: "WEB_UNSUPPORTED_CONTENT_TYPE", Message: fmt.Sprintf("unsupported charset %q", charset)}
	}
	return kind, decoder, nil
}
