package harness

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"
)

type catalogResponsesProvider struct {
	id, baseURL, apiKey, model string
	client                     *http.Client
	headers                    map[string]string
	cacheRetention             string
	transport                  string
	websocketConnectTimeout    time.Duration
	websocketConnectTimeoutSet bool
	streamIdleTimeout          time.Duration
	modelSpec                  piAIModel
	websockets                 *openAIResponsesWebSocketPool
	codex                      bool
}

func newCatalogResponsesProvider(id, baseURL, apiKey, model string, codex bool) *catalogResponsesProvider {
	return &catalogResponsesProvider{
		id: id, baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, model: model,
		client: &http.Client{}, websockets: newOpenAIResponsesWebSocketPool(), codex: codex,
	}
}

func (p *catalogResponsesProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	if p.codex {
		return p.completeCodex(ctx, req, onDelta)
	}
	return p.completeAzure(ctx, req, onDelta)
}

func (p *catalogResponsesProvider) completeAzure(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	key := firstNonBlank(p.apiKey, os.Getenv("AZURE_OPENAI_API_KEY"))
	if key == "" {
		return Completion{}, &ProviderError{Code: "MISSING_CREDENTIAL", Message: "Azure OpenAI requires AZURE_OPENAI_API_KEY or apiKeyEnv"}
	}
	endpoint, err := azureResponsesEndpoint(p.baseURL)
	if err != nil {
		return Completion{}, err
	}
	provider := NewOpenAIResponsesProvider(p.id, "", key, p.model)
	provider.modelSpec = p.modelSpec
	provider.cacheRetention = p.cacheRetention
	provider.strictDefault = true
	supportsStrict := true
	if p.modelSpec.Compat.SupportsStrictMode != nil {
		supportsStrict = *p.modelSpec.Compat.SupportsStrictMode
	}
	if err := validateToolSampling(req.Tools, supportsStrict); err != nil {
		return Completion{}, err
	}
	body := provider.requestBody(req)
	body["model"] = azureDeploymentName(firstNonBlank(req.Model, p.model))
	headers := make(http.Header)
	for name, value := range p.headers {
		headers.Set(name, value)
	}
	headers.Set("Accept", "text/event-stream")
	headers.Set("Content-Type", "application/json")
	headers.Set("Api-Key", key)
	return completeResponsesSSE(ctx, p.client, endpoint, headers, body, p.streamIdleTimeout, onDelta)
}

func azureResponsesEndpoint(modelBaseURL string) (string, error) {
	base := firstNonBlank(modelBaseURL, os.Getenv("AZURE_OPENAI_BASE_URL"))
	if base == "" {
		if resource := strings.TrimSpace(os.Getenv("AZURE_OPENAI_RESOURCE_NAME")); resource != "" {
			base = "https://" + resource + ".openai.azure.com/openai/v1"
		}
	}
	if base == "" {
		return "", &ProviderError{Code: "MISSING_CREDENTIAL", Message: "Azure OpenAI requires AZURE_OPENAI_BASE_URL or AZURE_OPENAI_RESOURCE_NAME"}
	}
	parsed, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", &ProviderError{Code: "PROVIDER", Message: "invalid Azure OpenAI base URL " + base, Err: err}
	}
	azureHost := strings.HasSuffix(parsed.Hostname(), ".openai.azure.com") || strings.HasSuffix(parsed.Hostname(), ".cognitiveservices.azure.com") || strings.HasSuffix(parsed.Hostname(), ".ai.azure.com")
	path := strings.TrimRight(parsed.Path, "/")
	if azureHost && (path == "" || path == "/openai" || path == "/openai/v1/responses") {
		path = "/openai/v1"
	}
	if !strings.HasSuffix(path, "/responses") {
		path += "/responses"
	}
	parsed.Path = path
	query := parsed.Query()
	if query.Get("api-version") == "" {
		query.Set("api-version", firstNonBlank(os.Getenv("AZURE_OPENAI_API_VERSION"), "v1"))
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func azureDeploymentName(model string) string {
	for _, entry := range strings.Split(os.Getenv("AZURE_OPENAI_DEPLOYMENT_NAME_MAP"), ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == model && strings.TrimSpace(parts[1]) != "" {
			return strings.TrimSpace(parts[1])
		}
	}
	return model
}

func (p *catalogResponsesProvider) completeCodex(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	token := strings.TrimSpace(p.apiKey)
	if token == "" {
		return Completion{}, &ProviderError{Code: "MISSING_CREDENTIAL", Message: "OpenAI Codex requires an OAuth access token through apiKeyEnv"}
	}
	supportsStrict := true
	if p.modelSpec.Compat.SupportsStrictMode != nil {
		supportsStrict = *p.modelSpec.Compat.SupportsStrictMode
	}
	if err := validateToolSampling(req.Tools, supportsStrict); err != nil {
		return Completion{}, err
	}
	accountID, err := codexAccountID(token)
	if err != nil {
		return Completion{}, err
	}
	endpoint := codexResponsesEndpoint(p.baseURL)
	body := p.codexRequestBody(req)
	sseHeaders := p.codexHeaders(token, accountID, req.SessionID, false)
	if p.transport != "" && p.transport != "sse" {
		wsHeaders := p.codexHeaders(token, accountID, req.SessionID, true)
		provider := &OpenAIResponsesProvider{
			transport: p.transport, cacheRetention: p.cacheRetention,
			websocketConnectTimeout: p.websocketConnectTimeout, websocketConnectTimeoutSet: p.websocketConnectTimeoutSet,
			streamIdleTimeout: p.streamIdleTimeout, websockets: p.websockets,
		}
		completion, started, wsErr := provider.completeWebSocketPrepared(ctx, req, body, endpoint, wsHeaders, onDelta)
		if wsErr == nil || started || ctx.Err() != nil {
			return completion, wsErr
		}
	}
	return completeResponsesSSE(ctx, p.client, endpoint, sseHeaders, body, p.streamIdleTimeout, onDelta)
}

func (p *catalogResponsesProvider) codexRequestBody(req ChatRequest) map[string]any {
	input := openAIResponsesInput(req, p.modelSpec)
	filtered := input[:0]
	for _, raw := range input {
		row, _ := raw.(map[string]any)
		if role := stringSetting(row["role"]); role != "system" && role != "developer" {
			filtered = append(filtered, raw)
		}
	}
	supportsStrict := true
	if p.modelSpec.Compat.SupportsStrictMode != nil {
		supportsStrict = *p.modelSpec.Compat.SupportsStrictMode
	}
	tools := openAIResponsesTools(req.Tools, supportsStrict, nil)
	body := map[string]any{
		"model": firstNonBlank(req.Model, p.model), "store": false, "stream": true,
		"instructions": firstNonBlank(req.System, "You are a helpful assistant."), "input": filtered,
		"text": map[string]any{"verbosity": "low"}, "include": []string{"reasoning.encrypted_content"},
		"tool_choice": "auto", "parallel_tool_calls": true,
	}
	if len(tools) > 0 {
		body["tools"] = tools
	}
	if req.Temperature != nil {
		body["temperature"] = *req.Temperature
	}
	if key := openAIPromptCacheKey(req.SessionID); key != "" && resolvedPiAICacheRetention(p.cacheRetention) != "none" {
		body["prompt_cache_key"] = key
	}
	if req.ReasoningEffort != "" && req.ReasoningEffort != "off" {
		if effort, ok := piAIReasoningWire(p.modelSpec, req.ReasoningEffort); ok {
			body["reasoning"] = map[string]any{"effort": effort, "summary": "auto"}
		}
	}
	return body
}

func codexResponsesEndpoint(baseURL string) string {
	baseURL = strings.TrimRight(firstNonBlank(baseURL, "https://chatgpt.com/backend-api"), "/")
	if strings.HasSuffix(baseURL, "/codex/responses") {
		return baseURL
	}
	if strings.HasSuffix(baseURL, "/codex") {
		return baseURL + "/responses"
	}
	return baseURL + "/codex/responses"
}

func codexAccountID(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", &ProviderError{Code: "AUTH", Message: "OpenAI Codex token is not a JWT"}
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", &ProviderError{Code: "AUTH", Message: "decode OpenAI Codex token", Err: err}
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", &ProviderError{Code: "AUTH", Message: "decode OpenAI Codex token claims", Err: err}
	}
	auth, _ := claims["https://api.openai.com/auth"].(map[string]any)
	accountID := stringSetting(auth["chatgpt_account_id"])
	if accountID == "" {
		return "", &ProviderError{Code: "AUTH", Message: "OpenAI Codex token has no ChatGPT account ID"}
	}
	return accountID, nil
}

func (p *catalogResponsesProvider) codexHeaders(token, accountID, sessionID string, websocket bool) http.Header {
	headers := make(http.Header)
	for name, value := range p.headers {
		headers.Set(name, value)
	}
	headers.Set("Authorization", "Bearer "+token)
	headers.Set("ChatGPT-Account-Id", accountID)
	headers.Set("Originator", "pi")
	headers.Set("User-Agent", fmt.Sprintf("pi (%s; %s)", runtime.GOOS, runtime.GOARCH))
	requestID := openAIPromptCacheKey(sessionID)
	if websocket {
		headers.Set("OpenAI-Beta", openAIResponsesWebSocketBeta)
		if requestID != "" {
			headers.Set("Session-Id", requestID)
			headers.Set("X-Client-Request-Id", requestID)
		}
		return headers
	}
	headers.Set("Accept", "text/event-stream")
	headers.Set("Content-Type", "application/json")
	headers.Set("OpenAI-Beta", "responses=experimental")
	if requestID != "" && resolvedPiAICacheRetention(p.cacheRetention) != "none" {
		headers.Set("Session-Id", requestID)
		headers.Set("X-Client-Request-Id", requestID)
	}
	return headers
}
