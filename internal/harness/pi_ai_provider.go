package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	piAISettingsNamespace           = "llm-pi-ai"
	piAIDefaultContext              = 262144
	piAIDefaultMaxTokens            = 32768
	piAIDefaultIdleMillis           = 300000
	piAIDefaultMaxRequestImageBytes = DefaultMaxRequestImageBytes
	piAIMaxTimerMillis              = 2147483647
)

var piAISupportedProfileKeys = map[string]bool{
	"apiKeyEnv": true, "displayName": true, "api": true, "baseURL": true,
	"models": true, "modelOverrides": true, "compat": true,
	"defaultContextWindow": true, "defaultMaxTokens": true, "defaultInput": true,
	"headers": true, "reasoning": true, "thinkingBudgets": true,
	"cacheRetention": true, "transport": true, "timeoutMs": true,
	"websocketConnectTimeoutMs": true, "streamIdleTimeoutMs": true,
	"maxRequestImageBytes": true, "retryPolicy": true,
}

var piAISupportedModelKeys = map[string]bool{
	"id": true, "name": true, "contextWindow": true, "maxTokens": true,
	"input": true, "reasoningEfforts": true, "compat": true,
}

var piAIConfigurableProtocols = map[string]bool{
	"openai-completions": true,
	"openai-responses":   true,
	"anthropic-messages": true,
}

var piAICatalogProtocols = map[string]bool{
	"anthropic-messages":      true,
	"azure-openai-responses":  true,
	"bedrock-converse-stream": true,
	"google-generative-ai":    true,
	"google-vertex":           true,
	"mistral-conversations":   true,
	"openai-codex-responses":  true,
	"openai-completions":      true,
	"openai-responses":        true,
}

var piAICompatProtocolFields = map[string]map[string]bool{
	"openai-completions": {
		"supportsStore": true, "supportsDeveloperRole": true, "supportsReasoningEffort": true,
		"supportsUsageInStreaming": true, "maxTokensField": true, "requiresToolResultName": true,
		"requiresAssistantAfterToolResult": true, "requiresThinkingAsText": true,
		"requiresReasoningContentOnAssistantMessages": true, "thinkingFormat": true,
		"chatTemplateKwargs": true, "supportsStrictMode": true, "cacheControlFormat": true,
		"supportsLongCacheRetention": true,
	},
	"openai-responses": {
		"supportsDeveloperRole": true, "supportsStrictMode": true, "supportsLongCacheRetention": true,
	},
	"azure-openai-responses": {
		"supportsDeveloperRole": true, "supportsStrictMode": true, "supportsLongCacheRetention": true,
	},
	"openai-codex-responses": {
		"supportsDeveloperRole": true, "supportsStrictMode": true, "supportsLongCacheRetention": true,
	},
	"anthropic-messages": {
		"supportsEagerToolInputStreaming": true, "supportsLongCacheRetention": true,
		"supportsCacheControlOnTools": true, "supportsTemperature": true,
		"forceAdaptiveThinking": true, "allowEmptySignature": true, "supportsStrictTools": true,
	},
	"bedrock-converse-stream": {"supportsStrictMode": true},
}

var piAIWithheldCompatFields = map[string]bool{
	"openRouterRouting": true, "vercelGatewayRouting": true, "zaiToolStream": true,
	"supportsOpenAIGrammarTools": true, "sendSessionAffinityHeaders": true,
	"deferredToolsMode": true, "sessionAffinityFormat": true, "supportsToolSearch": true,
	"supportsExplicitPromptCacheMode": true, "supportsToolReferences": true,
}

var piAIBooleanCompatFields = map[string]bool{
	"supportsStore": true, "supportsDeveloperRole": true, "supportsReasoningEffort": true,
	"supportsUsageInStreaming": true, "requiresToolResultName": true,
	"requiresAssistantAfterToolResult": true, "requiresThinkingAsText": true,
	"requiresReasoningContentOnAssistantMessages": true, "supportsStrictMode": true,
	"supportsLongCacheRetention": true, "supportsEagerToolInputStreaming": true,
	"supportsCacheControlOnTools": true, "supportsTemperature": true,
	"forceAdaptiveThinking": true, "allowEmptySignature": true, "supportsStrictTools": true,
}

type piAIProviderProfile struct {
	route                string
	displayName          string
	apiKeyEnv            string
	models               []piAIModel
	modelByID            map[string]piAIModel
	headers              map[string]string
	reasoning            string
	thinkingBudgets      map[string]int
	cacheRetention       string
	transport            string
	timeout              time.Duration
	websocketTimeout     time.Duration
	websocketTimeoutSet  bool
	streamIdleTimeout    time.Duration
	maxRequestImageBytes int
	configuredMaxTokens  map[string]int
	retryPolicy          RetryPolicy
	configuredModel      string
}

type managedPiAIProvider struct {
	engine     *Engine
	profile    piAIProviderProfile
	websockets *openAIResponsesWebSocketPool
}

func (p *managedPiAIProvider) ID() string   { return p.profile.route }
func (p *managedPiAIProvider) Name() string { return p.profile.displayName }
func (p *managedPiAIProvider) RetryPolicy() RetryPolicy {
	return p.profile.retryPolicy
}

func (p *managedPiAIProvider) Models(ctx context.Context) ([]ModelInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return piAIModelInfos(p.profile.models), nil
}

func (p *managedPiAIProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	if req.Model == "" {
		req.Model = p.profile.configuredModel
	}
	model, ok := p.profile.modelByID[req.Model]
	if !ok {
		return Completion{}, &ProviderError{Code: "MODEL_NOT_FOUND", Message: fmt.Sprintf("llm-pi-ai: provider route %q has no model %q", p.profile.route, req.Model)}
	}
	auth, err := p.engine.resolvePiAIAuth(ctx, p.profile.route, p.profile.apiKeyEnv, model.BaseURL)
	if err != nil {
		return Completion{}, err
	}
	apiKey := auth.APIKey
	baseURL := firstNonBlank(auth.BaseURL, model.BaseURL)
	if req.ReasoningEffort == "" {
		req.ReasoningEffort = p.profile.reasoning
	}
	if req.ReasoningEffort != "" && !piAIModelSupportsReasoning(model, req.ReasoningEffort) {
		return Completion{}, &ProviderError{Code: "UNSUPPORTED_REASONING_EFFORT", Message: fmt.Sprintf("pi-ai provider %q model %q does not support reasoning effort %q", p.profile.route, model.ID, req.ReasoningEffort)}
	}
	hasImages := false
	for _, message := range req.Messages {
		messageHasImages := message.HadImages || chatMessageHasImage(message) || contentBlocksHaveImage(message.Blocks)
		if messageHasImages && message.Role != "user" && message.Role != "tool" {
			return Completion{}, &ProviderError{Code: "UNSUPPORTED_CONTENT", Message: fmt.Sprintf("pi-ai cannot represent an image in an in-history %s message", message.Role)}
		}
		hasImages = hasImages || messageHasImages
	}
	if hasImages && !slices.Contains(model.Input, "image") {
		return Completion{}, &ProviderError{Code: "UNSUPPORTED_CONTENT", Message: fmt.Sprintf("pi-ai model %q does not support image input", model.ID)}
	}
	if req.MaxTokens == 0 {
		req.MaxTokens = p.profile.configuredMaxTokens[model.ID]
	}
	req.Messages = offloadRequestImages(req.Messages, p.profile.maxRequestImageBytes)
	headers := mergePIAIHeaders(model.Headers, p.profile.headers)
	if p.profile.route == "cloudflare-ai-gateway" && auth.Headers["cf-aig-authorization"] != "" {
		headers = removePIAIHeaders(headers, "authorization", "x-api-key")
	}
	headers = mergePIAIHeaders(headers, auth.Headers)
	switch model.API {
	case "openai-completions":
		provider := NewOpenAIProvider(p.profile.route, baseURL, apiKey, model.ID)
		provider.headers = headers
		provider.cacheRetention = p.profile.cacheRetention
		provider.modelSpec = model
		provider.streamIdleTimeout = p.profile.streamIdleTimeout
		if p.profile.timeout > 0 {
			provider.client.Timeout = p.profile.timeout
		}
		return provider.Complete(ctx, req, onDelta)
	case "openai-responses":
		provider := NewOpenAIResponsesProvider(p.profile.route, baseURL, apiKey, model.ID)
		provider.headers = headers
		provider.cacheRetention = p.profile.cacheRetention
		provider.transport = p.profile.transport
		provider.websocketConnectTimeout = p.profile.websocketTimeout
		provider.websocketConnectTimeoutSet = p.profile.websocketTimeoutSet
		provider.modelSpec = model
		provider.websockets = p.websockets
		provider.streamIdleTimeout = p.profile.streamIdleTimeout
		if p.profile.timeout > 0 {
			provider.client.Timeout = p.profile.timeout
		}
		return provider.Complete(ctx, req, onDelta)
	case "anthropic-messages":
		provider := NewAnthropicProvider(p.profile.route, baseURL, apiKey, model.ID)
		provider.headers = headers
		provider.oauth = auth.OAuth && p.profile.route == "anthropic"
		provider.cacheRetention = p.profile.cacheRetention
		provider.thinkingBudgets = cloneIntMap(p.profile.thinkingBudgets)
		provider.modelSpec = model
		provider.streamIdleTimeout = p.profile.streamIdleTimeout
		if req.MaxTokens == 0 {
			req.MaxTokens = model.MaxTokens
		}
		if p.profile.timeout > 0 {
			provider.client.Timeout = p.profile.timeout
		}
		return provider.Complete(ctx, req, onDelta)
	case "google-generative-ai", "google-vertex":
		provider := newGoogleProvider(p.profile.route, baseURL, apiKey, model.ID, model.API == "google-vertex")
		provider.headers = headers
		provider.env = clonePIAIHeaders(auth.Env)
		provider.modelSpec = model
		provider.thinkingBudgets = cloneIntMap(p.profile.thinkingBudgets)
		provider.streamIdleTimeout = p.profile.streamIdleTimeout
		if p.profile.timeout > 0 {
			provider.client.Timeout = p.profile.timeout
		}
		return provider.Complete(ctx, req, onDelta)
	case "mistral-conversations":
		provider := newMistralProvider(p.profile.route, baseURL, apiKey, model.ID)
		provider.headers = headers
		provider.cacheRetention = p.profile.cacheRetention
		provider.modelSpec = model
		provider.streamIdleTimeout = p.profile.streamIdleTimeout
		if p.profile.timeout > 0 {
			provider.client.Timeout = p.profile.timeout
		}
		return provider.Complete(ctx, req, onDelta)
	case "azure-openai-responses", "openai-codex-responses":
		provider := newCatalogResponsesProvider(p.profile.route, baseURL, apiKey, model.ID, model.API == "openai-codex-responses")
		provider.headers = headers
		provider.cacheRetention = p.profile.cacheRetention
		provider.transport = p.profile.transport
		provider.websocketConnectTimeout = p.profile.websocketTimeout
		provider.websocketConnectTimeoutSet = p.profile.websocketTimeoutSet
		provider.streamIdleTimeout = p.profile.streamIdleTimeout
		provider.modelSpec = model
		provider.websockets = p.websockets
		if p.profile.timeout > 0 {
			provider.client.Timeout = p.profile.timeout
		}
		return provider.Complete(ctx, req, onDelta)
	case "bedrock-converse-stream":
		provider := newBedrockProvider(p.profile.route, baseURL, apiKey, model.ID)
		provider.headers = headers
		provider.env = clonePIAIHeaders(auth.Env)
		provider.cacheRetention = p.profile.cacheRetention
		provider.modelSpec = model
		provider.thinkingBudgets = cloneIntMap(p.profile.thinkingBudgets)
		provider.streamIdleTimeout = p.profile.streamIdleTimeout
		if p.profile.timeout > 0 {
			provider.client.Timeout = p.profile.timeout
		}
		return provider.Complete(ctx, req, onDelta)
	default:
		return Completion{}, &ProviderError{Code: "PROVIDER", Message: fmt.Sprintf("llm-pi-ai: unsupported api %q", model.API)}
	}
}

func mergePIAIHeaders(base, override map[string]string) map[string]string {
	merged := clonePIAIHeaders(base)
	if merged == nil && len(override) > 0 {
		merged = make(map[string]string, len(override))
	}
	for name, value := range override {
		for existing := range merged {
			if strings.EqualFold(existing, name) {
				delete(merged, existing)
			}
		}
		merged[name] = value
	}
	return merged
}

func removePIAIHeaders(headers map[string]string, names ...string) map[string]string {
	for existing := range headers {
		for _, name := range names {
			if strings.EqualFold(existing, name) {
				delete(headers, existing)
				break
			}
		}
	}
	return headers
}

func piAIModelSupportsReasoning(model piAIModel, level string) bool {
	if !model.Reasoning {
		return level == "off"
	}
	value, exists := model.ThinkingLevelMap[level]
	if exists && value == nil {
		return false
	}
	if level == "xhigh" || level == "max" {
		return exists
	}
	return piAIReasoningLevel(level)
}

func cloneIntMap(source map[string]int) map[string]int {
	if len(source) == 0 {
		return nil
	}
	copy := make(map[string]int, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func (e *Engine) resolvePiAICredential(route, ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	value, _, ok := e.resolveCredential(ref)
	if !ok || strings.TrimSpace(value) == "" {
		return "", &ProviderError{
			Code:    "MISSING_CREDENTIAL",
			Message: fmt.Sprintf("llm-pi-ai: no credential for provider route %q; set %s or remove apiKeyEnv for unauthenticated provider discovery", route, ref),
		}
	}
	return usablePiAIAPIKey(route, ref, value)
}

func usablePiAIAPIKey(route, ref, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", &ProviderError{Code: "MISSING_CREDENTIAL", Message: fmt.Sprintf("llm-pi-ai: credential %s for provider route %q is blank", ref, route)}
	}
	for _, b := range []byte(value) {
		if b < 0x20 || b == 0x7f {
			return "", &ProviderError{Code: "AUTH", Message: fmt.Sprintf("llm-pi-ai: credential %s for provider route %q contains invalid HTTP header characters", ref, route)}
		}
	}
	return value, nil
}

func clonePIAIHeaders(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	copy := make(map[string]string, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func cloneModelInfos(source []ModelInfo) []ModelInfo {
	copy := make([]ModelInfo, len(source))
	for index, model := range source {
		copy[index] = model
		copy[index].InputModalities = append([]string(nil), model.InputModalities...)
	}
	return copy
}

func piAISettingsBase() map[string]any {
	return map[string]any{"providers": map[string]any{}}
}

func piAISettingsSchema() map[string]any {
	compatFields := map[string]any{
		"supportsStore": 24, "supportsDeveloperRole": 24, "supportsReasoningEffort": 24,
		"supportsUsageInStreaming": 24, "maxTokensField": 2, "requiresToolResultName": 24,
		"requiresAssistantAfterToolResult": 24, "requiresThinkingAsText": 24,
		"requiresReasoningContentOnAssistantMessages": 24, "thinkingFormat": 2,
		"chatTemplateKwargs": 27, "supportsStrictMode": 24, "cacheControlFormat": 2,
		"supportsLongCacheRetention": 24, "supportsEagerToolInputStreaming": 24,
		"supportsCacheControlOnTools": 24, "supportsTemperature": 24,
		"forceAdaptiveThinking": 24, "allowEmptySignature": 24, "supportsStrictTools": 24,
	}
	return map[string]any{
		"uid": 16,
		"refs": map[string]any{
			"1":  map[string]any{"type": "string", "meta": map[string]any{"role": "credential-ref"}},
			"2":  map[string]any{"type": "string", "meta": map[string]any{}},
			"3":  map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "openai-completions"},
			"4":  map[string]any{"type": "union", "meta": map[string]any{}, "list": []any{3, 17, 18}},
			"5":  map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1}},
			"6":  map[string]any{"type": "string", "meta": map[string]any{"required": true}},
			"7":  map[string]any{"type": "string", "meta": map[string]any{}},
			"8":  map[string]any{"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{"id": 6, "name": 7, "contextWindow": 5, "maxTokens": 5, "input": 10, "reasoningEfforts": 23, "compat": 19}},
			"9":  map[string]any{"type": "array", "meta": map[string]any{}, "inner": 8},
			"10": map[string]any{"type": "array", "meta": map[string]any{"default": []any{"text"}}, "inner": 7},
			"11": map[string]any{"type": "dict", "meta": map[string]any{}, "inner": 7},
			"12": map[string]any{"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{
				"apiKeyEnv": 1, "displayName": 2, "api": 4, "baseURL": 2, "models": 9,
				"modelOverrides": 22, "compat": 19,
				"defaultContextWindow": 5, "defaultMaxTokens": 5, "defaultInput": 10,
				"headers": 11, "reasoning": 2, "thinkingBudgets": 20,
				"cacheRetention": 2, "transport": 2, "timeoutMs": 21,
				"websocketConnectTimeoutMs": 21, "streamIdleTimeoutMs": 5, "maxRequestImageBytes": 26,
			}},
			"15": map[string]any{"type": "dict", "meta": map[string]any{"default": map[string]any{}}, "inner": 12},
			"16": map[string]any{"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{"providers": 15}},
			"17": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "openai-responses"},
			"18": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "anthropic-messages"},
			"19": map[string]any{"type": "object", "meta": map[string]any{}, "dict": compatFields},
			"20": map[string]any{"type": "object", "meta": map[string]any{}, "dict": map[string]any{"minimal": 5, "low": 5, "medium": 5, "high": 5}},
			"21": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 0}},
			"22": map[string]any{"type": "dict", "meta": map[string]any{}, "inner": 8},
			"23": map[string]any{"type": "union", "meta": map[string]any{}, "list": []any{24, 25}},
			"24": map[string]any{"type": "boolean", "meta": map[string]any{}},
			"25": map[string]any{"type": "dict", "meta": map[string]any{}, "inner": 2},
			"26": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": piAIDefaultMaxRequestImageBytes}},
			"27": map[string]any{"type": "dict", "meta": map[string]any{}, "inner": 28},
			"28": map[string]any{"type": "union", "meta": map[string]any{}, "list": []any{2, 24, 29, 30, 31}},
			"29": map[string]any{"type": "number", "meta": map[string]any{}},
			"30": map[string]any{"type": "const", "meta": map[string]any{}, "value": nil},
			"31": map[string]any{"type": "object", "meta": map[string]any{}, "dict": map[string]any{"$var": 2, "omitWhenOff": 24}},
		},
	}
}

func (e *Engine) resolvePiAIProvidersLocked(section map[string]any) (map[string]*managedPiAIProvider, error) {
	providersRaw, exists := section["providers"]
	if !exists || providersRaw == nil {
		return map[string]*managedPiAIProvider{}, nil
	}
	providers, ok := providersRaw.(map[string]any)
	if !ok {
		return nil, errors.New("llm-pi-ai.providers must be an object keyed by provider route")
	}
	routes := make([]string, 0, len(providers))
	for route := range providers {
		routes = append(routes, route)
	}
	sort.Strings(routes)
	resolved := make(map[string]*managedPiAIProvider, len(routes))
	for _, route := range routes {
		if route == "" || route != strings.TrimSpace(route) {
			return nil, errors.New("llm-pi-ai provider names must be non-empty and must not contain surrounding whitespace")
		}
		profile, err := resolvePiAIProfile(route, providers[route])
		if err != nil {
			return nil, err
		}
		if existing := e.providers[route]; existing != nil {
			current := e.piAIProviders[route]
			if current == nil || existing != current {
				return nil, fmt.Errorf("llm-pi-ai provider route %q is already registered by another adapter", route)
			}
		}
		resolved[route] = &managedPiAIProvider{engine: e, profile: profile, websockets: newOpenAIResponsesWebSocketPool()}
	}
	return resolved, nil
}

func resolvePiAIProfile(route string, raw any) (piAIProviderProfile, error) {
	value, ok := raw.(map[string]any)
	if !ok {
		return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q must be an object", route)
	}
	for key := range value {
		if !piAISupportedProfileKeys[key] {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q uses unsupported field %q", route, key)
		}
	}
	catalog, catalogued := piAICatalog[route]
	displayName := route
	if rawName, exists := value["displayName"]; exists {
		name, ok := rawName.(string)
		if !ok || name == "" || name != strings.TrimSpace(name) {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q displayName must be a non-empty string without surrounding whitespace", route)
		}
		displayName = name
	}
	api := ""
	if rawAPI, exists := value["api"]; exists {
		var ok bool
		api, ok = rawAPI.(string)
		if !ok || api == "" {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q api must be a non-empty string", route)
		}
	}
	if api != "" && !piAIConfigurableProtocols[api] {
		return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q names unsupported api %q", route, api)
	}
	baseURL := ""
	if rawURL, exists := value["baseURL"]; exists {
		var ok bool
		baseURL, ok = rawURL.(string)
		if !ok || baseURL == "" || baseURL != strings.TrimSpace(baseURL) {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q baseURL must be a non-empty string without surrounding whitespace", route)
		}
	}
	apiKeyEnv := ""
	if rawRef, exists := value["apiKeyEnv"]; exists {
		var ok bool
		apiKeyEnv, ok = rawRef.(string)
		if !ok || !validCredentialRef(apiKeyEnv) {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q apiKeyEnv must be a valid credential reference", route)
		}
	}
	defaultContext, err := optionalPositiveInteger(value, "defaultContextWindow", piAIDefaultContext, "llm-pi-ai provider "+fmt.Sprintf("%q", route))
	if err != nil {
		return piAIProviderProfile{}, err
	}
	defaultMaxTokens, err := optionalPositiveInteger(value, "defaultMaxTokens", piAIDefaultMaxTokens, "llm-pi-ai provider "+fmt.Sprintf("%q", route))
	if err != nil {
		return piAIProviderProfile{}, err
	}
	defaultInput, err := piAIModalities(value["defaultInput"], []string{"text"}, fmt.Sprintf("llm-pi-ai provider %q defaultInput", route))
	if err != nil {
		return piAIProviderProfile{}, err
	}
	if err := validatePiAICompat(value["compat"], fmt.Sprintf("llm-pi-ai provider %q compat", route)); err != nil {
		return piAIProviderProfile{}, err
	}
	models, configuredMaxTokens, err := resolvePiAIModels(route, api, baseURL, value["compat"], value["models"], value["modelOverrides"], catalog, catalogued, defaultContext, defaultMaxTokens, defaultInput)
	if err != nil {
		return piAIProviderProfile{}, err
	}
	headers, err := piAIHeaders(value["headers"], route)
	if err != nil {
		return piAIProviderProfile{}, err
	}
	reasoning := ""
	if rawReasoning, exists := value["reasoning"]; exists {
		var ok bool
		reasoning, ok = rawReasoning.(string)
		if !ok || !piAIReasoningLevel(reasoning) {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q reasoning is not a supported level", route)
		}
	}
	thinkingBudgets, err := piAIThinkingBudgets(value["thinkingBudgets"], route)
	if err != nil {
		return piAIProviderProfile{}, err
	}
	cacheRetention := ""
	if raw, exists := value["cacheRetention"]; exists {
		cacheRetention, _ = raw.(string)
		if cacheRetention != "none" && cacheRetention != "short" && cacheRetention != "long" {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q cacheRetention must be none, short, or long", route)
		}
	}
	transport := ""
	if raw, exists := value["transport"]; exists {
		transport, _ = raw.(string)
		if transport != "sse" && transport != "websocket" && transport != "websocket-cached" && transport != "auto" {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q transport must be sse, websocket, websocket-cached, or auto", route)
		}
	}
	timeout := time.Duration(0)
	if rawTimeout, exists := value["timeoutMs"]; exists {
		milliseconds, ok := nonNegativeInteger(rawTimeout)
		if !ok || milliseconds > piAIMaxTimerMillis {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q timeoutMs must be a natural integer no greater than %d", route, piAIMaxTimerMillis)
		}
		timeout = time.Duration(milliseconds) * time.Millisecond
	}
	websocketTimeout := time.Duration(0)
	websocketTimeoutSet := false
	if rawTimeout, exists := value["websocketConnectTimeoutMs"]; exists {
		websocketTimeoutSet = true
		milliseconds, ok := nonNegativeInteger(rawTimeout)
		if !ok || milliseconds > piAIMaxTimerMillis {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q websocketConnectTimeoutMs must be a natural integer no greater than %d", route, piAIMaxTimerMillis)
		}
		websocketTimeout = time.Duration(milliseconds) * time.Millisecond
	}
	idleMillis := float64(piAIDefaultIdleMillis)
	if rawTimeout, exists := value["streamIdleTimeoutMs"]; exists {
		var ok bool
		idleMillis, ok = positiveFiniteMilliseconds(rawTimeout)
		if !ok {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q streamIdleTimeoutMs must be a positive finite number no greater than %d", route, piAIMaxTimerMillis)
		}
	}
	maxRequestImageBytes := piAIDefaultMaxRequestImageBytes
	if rawMax, exists := value["maxRequestImageBytes"]; exists {
		var ok bool
		maxRequestImageBytes, ok = piAIPositiveInteger(rawMax)
		if !ok {
			return piAIProviderProfile{}, fmt.Errorf("llm-pi-ai provider %q maxRequestImageBytes must be a positive integer", route)
		}
	}
	retryPolicy, err := resolvePiAIRetryPolicy(value["retryPolicy"], route)
	if err != nil {
		return piAIProviderProfile{}, err
	}
	modelByID := make(map[string]piAIModel, len(models))
	for _, model := range models {
		modelByID[model.ID] = model
	}
	return piAIProviderProfile{
		route: route, displayName: displayName,
		apiKeyEnv: apiKeyEnv, models: models, modelByID: modelByID, headers: headers,
		reasoning: reasoning, thinkingBudgets: thinkingBudgets, cacheRetention: cacheRetention, transport: transport,
		timeout: timeout, websocketTimeout: websocketTimeout, websocketTimeoutSet: websocketTimeoutSet, streamIdleTimeout: millisecondsDuration(idleMillis),
		maxRequestImageBytes: maxRequestImageBytes, configuredMaxTokens: configuredMaxTokens, retryPolicy: retryPolicy,
		configuredModel: models[0].ID,
	}, nil
}

func resolvePiAIModels(route, apiOverride, baseURLOverride string, rawRouteCompat, raw, rawOverrides any, catalog piAICatalogRoute, catalogued bool, defaultContext, defaultMaxTokens int, defaultInput []string) ([]piAIModel, map[string]int, error) {
	var rows []any
	if raw != nil {
		var ok bool
		rows, ok = anySlice(raw)
		if !ok {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q models must be an array", route)
		}
	}
	overrides := map[string]any{}
	if rawOverrides != nil {
		var ok bool
		overrides, ok = rawOverrides.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q modelOverrides must be an object keyed by model id", route)
		}
	}
	if len(rows) > 0 && len(overrides) > 0 {
		return nil, nil, fmt.Errorf("llm-pi-ai provider %q cannot set modelOverrides beside models", route)
	}
	if len(overrides) > 0 {
		if !catalogued || len(catalog.Models) == 0 {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q sets modelOverrides for a route absent from the installed catalog", route)
		}
		rows = make([]any, 0, len(catalog.Models))
		for _, model := range catalog.Models {
			row := map[string]any{"id": model.ID}
			if override, exists := overrides[model.ID]; exists {
				value, ok := override.(map[string]any)
				if !ok {
					return nil, nil, fmt.Errorf("llm-pi-ai provider %q modelOverrides entry %q must be an object", route, model.ID)
				}
				for key, item := range value {
					if key == "id" {
						return nil, nil, fmt.Errorf("llm-pi-ai provider %q modelOverrides entry %q must not set id", route, model.ID)
					}
					row[key] = item
				}
			}
			rows = append(rows, row)
		}
		for id := range overrides {
			found := false
			for _, model := range catalog.Models {
				if model.ID == id {
					found = true
					break
				}
			}
			if !found {
				return nil, nil, fmt.Errorf("llm-pi-ai provider %q modelOverrides names unknown model %q", route, id)
			}
		}
	}
	if len(rows) == 0 {
		if !catalogued || len(catalog.Models) == 0 {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q resolves no models; a declared route must list every model in models", route)
		}
		rows = make([]any, 0, len(catalog.Models))
		for _, model := range catalog.Models {
			rows = append(rows, map[string]any{"id": model.ID})
		}
	}
	catalogModels := make(map[string]piAIModel, len(catalog.Models))
	for _, model := range catalog.Models {
		catalogModels[model.ID] = model
	}
	sharedAPI := sharedPiAICatalogAPI(catalog.Models)
	models := make([]piAIModel, 0, len(rows))
	configuredMaxTokens := make(map[string]int)
	seen := make(map[string]bool, len(rows))
	for index, rawModel := range rows {
		row, ok := rawModel.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q model %d must be an object", route, index+1)
		}
		for key := range row {
			if !piAISupportedModelKeys[key] {
				return nil, nil, fmt.Errorf("llm-pi-ai provider %q model %d uses unsupported field %q", route, index+1, key)
			}
		}
		id, ok := row["id"].(string)
		if !ok || id == "" || id != strings.TrimSpace(id) {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q model %d id must be a non-empty string without surrounding whitespace", route, index+1)
		}
		if seen[id] {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q model id %q is duplicated", route, id)
		}
		seen[id] = true
		base, catalogModel := catalogModels[id]
		model := piAIModel{ID: id}
		if catalogModel {
			model = clonePiAIModel(base)
		}
		api := apiOverride
		if api == "" {
			api = base.API
		}
		if api == "" {
			api = sharedAPI
		}
		if api == "" {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q model %q needs api because the installed catalog has no shared protocol", route, id)
		}
		if !piAICatalogProtocols[api] {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q model %q names unsupported api %q", route, id, api)
		}
		baseURL := strings.TrimRight(baseURLOverride, "/")
		if baseURL == "" {
			baseURL = strings.TrimRight(base.BaseURL, "/")
		}
		if baseURL == "" {
			baseURL = strings.TrimRight(catalog.BaseURL, "/")
		}
		if baseURL == "" && api != "azure-openai-responses" {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q model %q needs baseURL", route, id)
		}
		name := base.Name
		if name == "" {
			name = id
		}
		if rawName, exists := row["name"]; exists {
			var valid bool
			name, valid = rawName.(string)
			if !valid || name == "" {
				return nil, nil, fmt.Errorf("llm-pi-ai provider %q model %q name must be a non-empty string", route, id)
			}
		}
		contextWindow := base.ContextWindow
		if contextWindow <= 0 {
			contextWindow = defaultContext
		}
		if rawContext, exists := row["contextWindow"]; exists {
			var valid bool
			contextWindow, valid = piAIPositiveInteger(rawContext)
			if !valid {
				return nil, nil, fmt.Errorf("llm-pi-ai provider %q model %q contextWindow must be a positive integer", route, id)
			}
		}
		maxTokens := base.MaxTokens
		if maxTokens <= 0 {
			maxTokens = defaultMaxTokens
		}
		if rawMaxTokens, exists := row["maxTokens"]; exists {
			var valid bool
			maxTokens, valid = piAIPositiveInteger(rawMaxTokens)
			if !valid {
				return nil, nil, fmt.Errorf("llm-pi-ai provider %q model %q maxTokens must be a positive integer", route, id)
			}
			configuredMaxTokens[id] = maxTokens
		}
		inputFallback := base.Input
		if len(inputFallback) == 0 {
			inputFallback = defaultInput
		}
		rawInput := row["input"]
		if configured, ok := anySlice(rawInput); ok && len(configured) == 0 {
			rawInput = nil
		}
		input, err := piAIModalities(rawInput, inputFallback, fmt.Sprintf("llm-pi-ai provider %q model %q input", route, id))
		if err != nil {
			return nil, nil, err
		}
		if err := validatePiAIReasoningEfforts(row["reasoningEfforts"], fmt.Sprintf("llm-pi-ai provider %q model %q reasoningEfforts", route, id)); err != nil {
			return nil, nil, err
		}
		if err := validatePiAICompat(row["compat"], fmt.Sprintf("llm-pi-ai provider %q model %q compat", route, id)); err != nil {
			return nil, nil, err
		}
		for field := range configuredPiAICompat(row["compat"]) {
			if !piAICompatProtocolFields[api][field] {
				return nil, nil, fmt.Errorf("llm-pi-ai provider %q model %q sets compat %q, but api %q does not take it", route, id, field, api)
			}
		}
		model.ID, model.Name, model.API, model.BaseURL = id, name, api, baseURL
		model.Input, model.ContextWindow, model.MaxTokens = input, contextWindow, maxTokens
		if rawEfforts, exists := row["reasoningEfforts"]; exists {
			model.Reasoning, model.ThinkingLevelMap = resolvePiAIReasoningEfforts(rawEfforts)
		}
		if api != base.API {
			model.Compat = piAIModelCompat{}
		}
		applyPiAICompat(&model.Compat, rawRouteCompat, api)
		applyPiAICompat(&model.Compat, row["compat"], api)
		models = append(models, model)
	}
	for field := range configuredPiAICompat(rawRouteCompat) {
		used := false
		for _, model := range models {
			if piAICompatProtocolFields[model.API][field] {
				used = true
				break
			}
		}
		if !used {
			return nil, nil, fmt.Errorf("llm-pi-ai provider %q sets compat %q, but no model on the route speaks a protocol that takes it", route, field)
		}
	}
	return models, configuredMaxTokens, nil
}

func sharedPiAICatalogAPI(models []piAIModel) string {
	api := ""
	for _, model := range models {
		if api == "" {
			api = model.API
			continue
		}
		if model.API != api {
			return ""
		}
	}
	return api
}

func configuredPiAICompat(raw any) map[string]any {
	value, _ := raw.(map[string]any)
	out := make(map[string]any, len(value))
	for field, item := range value {
		if object, ok := item.(map[string]any); ok && len(object) == 0 {
			continue
		}
		out[field] = item
	}
	return out
}

func piAIBool(value any) *bool {
	parsed := value.(bool)
	return &parsed
}

func applyPiAICompat(compat *piAIModelCompat, raw any, api string) {
	for field, value := range configuredPiAICompat(raw) {
		if !piAICompatProtocolFields[api][field] {
			continue
		}
		switch field {
		case "thinkingFormat":
			compat.ThinkingFormat = value.(string)
		case "cacheControlFormat":
			compat.CacheControlFormat = value.(string)
		case "maxTokensField":
			compat.MaxTokensField = value.(string)
		case "chatTemplateKwargs":
			compat.ChatTemplateKwargs = cloneSettingsValue(value.(map[string]any))
		case "supportsStore":
			compat.SupportsStore = piAIBool(value)
		case "supportsDeveloperRole":
			compat.SupportsDeveloperRole = piAIBool(value)
		case "supportsReasoningEffort":
			compat.SupportsReasoningEffort = piAIBool(value)
		case "supportsUsageInStreaming":
			compat.SupportsUsageInStreaming = piAIBool(value)
		case "requiresToolResultName":
			compat.RequiresToolResultName = piAIBool(value)
		case "requiresAssistantAfterToolResult":
			compat.RequiresAssistantAfterToolResult = piAIBool(value)
		case "requiresThinkingAsText":
			compat.RequiresThinkingAsText = piAIBool(value)
		case "requiresReasoningContentOnAssistantMessages":
			compat.RequiresReasoningContent = piAIBool(value)
		case "supportsStrictMode":
			compat.SupportsStrictMode = piAIBool(value)
		case "supportsLongCacheRetention":
			compat.SupportsLongCacheRetention = piAIBool(value)
		case "supportsEagerToolInputStreaming":
			compat.SupportsEagerToolInputStreaming = piAIBool(value)
		case "supportsCacheControlOnTools":
			compat.SupportsCacheControlOnTools = piAIBool(value)
		case "supportsTemperature":
			compat.SupportsTemperature = piAIBool(value)
		case "forceAdaptiveThinking":
			compat.ForceAdaptiveThinking = piAIBool(value)
		case "allowEmptySignature":
			compat.AllowEmptySignature = piAIBool(value)
		case "supportsStrictTools":
			compat.SupportsStrictTools = piAIBool(value)
		}
	}
}

func resolvePiAIReasoningEfforts(raw any) (bool, map[string]*string) {
	if disabled, ok := raw.(bool); ok && !disabled {
		return false, nil
	}
	value := raw.(map[string]any)
	levels := make(map[string]*string, 7)
	for _, level := range []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"} {
		wire, exists := value[level]
		if !exists {
			levels[level] = nil
			continue
		}
		if wire == nil && level == "off" {
			delete(levels, level)
			continue
		}
		text := wire.(string)
		levels[level] = &text
	}
	return true, levels
}

func validatePiAICompat(raw any, path string) error {
	if raw == nil {
		return nil
	}
	value, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("%s must be an object", path)
	}
	for key, rawValue := range value {
		declared := false
		for _, fields := range piAICompatProtocolFields {
			if fields[key] {
				declared = true
				break
			}
		}
		if !declared {
			if piAIWithheldCompatFields[key] {
				return fmt.Errorf("%s sets %q, which is not configurable here", path, key)
			}
			return fmt.Errorf("%s uses unsupported field %q", path, key)
		}
		if rawValue == nil {
			return fmt.Errorf("%s sets %q with no value", path, key)
		}
		switch {
		case key == "thinkingFormat":
			format, ok := rawValue.(string)
			if !ok || !map[string]bool{
				"openai": true, "deepseek": true, "openrouter": true, "together": true,
				"zai": true, "qwen": true, "chat-template": true, "qwen-chat-template": true,
				"string-thinking": true, "ant-ling": true,
			}[format] {
				return fmt.Errorf("%s thinkingFormat is unsupported", path)
			}
		case key == "maxTokensField":
			field, ok := rawValue.(string)
			if !ok || field != "max_completion_tokens" && field != "max_tokens" {
				return fmt.Errorf("%s maxTokensField is unsupported", path)
			}
		case key == "cacheControlFormat":
			field, ok := rawValue.(string)
			if !ok || field != "anthropic" {
				return fmt.Errorf("%s cacheControlFormat is unsupported", path)
			}
		case key == "chatTemplateKwargs":
			kwargs, ok := rawValue.(map[string]any)
			if !ok {
				return fmt.Errorf("%s chatTemplateKwargs must be an object", path)
			}
			for name, item := range kwargs {
				if err := validatePiAIChatTemplateKwarg(item); err != nil {
					return fmt.Errorf("%s chatTemplateKwargs.%s %w", path, name, err)
				}
			}
		case piAIBooleanCompatFields[key]:
			if _, ok := rawValue.(bool); !ok {
				return fmt.Errorf("%s %s must be boolean", path, key)
			}
		}
	}
	return nil
}

func validatePiAIChatTemplateKwarg(value any) error {
	switch value := value.(type) {
	case nil, string, bool, float64, int, int64, json.Number:
		return nil
	case map[string]any:
		for key := range value {
			if key != "$var" && key != "omitWhenOff" {
				return fmt.Errorf("uses unsupported field %q", key)
			}
		}
		variable, ok := value["$var"].(string)
		if !ok || variable != "thinking.enabled" && variable != "thinking.effort" {
			return errors.New("must name $var thinking.enabled or thinking.effort")
		}
		if raw, exists := value["omitWhenOff"]; exists {
			if _, ok := raw.(bool); !ok {
				return errors.New("omitWhenOff must be boolean")
			}
		}
		return nil
	default:
		return fmt.Errorf("must be a scalar or request variable, got %T", value)
	}
}

func validatePiAIReasoningEfforts(raw any, path string) error {
	if raw == nil {
		return nil
	}
	if disabled, ok := raw.(bool); ok {
		if !disabled {
			return nil
		}
		return fmt.Errorf("%s must be false or an object", path)
	}
	value, ok := raw.(map[string]any)
	if !ok || len(value) == 0 {
		return fmt.Errorf("%s must declare at least one thinking level", path)
	}
	nonOff := false
	for level, wire := range value {
		if !piAIReasoningLevel(level) {
			return fmt.Errorf("%s uses unsupported level %q", path, level)
		}
		if level != "off" {
			nonOff = true
		}
		if wire == nil {
			if level != "off" {
				return fmt.Errorf("%s.%s needs a wire value", path, level)
			}
			continue
		}
		text, ok := wire.(string)
		if !ok || text == "" {
			return fmt.Errorf("%s.%s must be a non-empty string", path, level)
		}
	}
	if !nonOff {
		return fmt.Errorf("%s must offer a level beyond off", path)
	}
	return nil
}

func piAIThinkingBudgets(raw any, route string) (map[string]int, error) {
	if raw == nil {
		return nil, nil
	}
	value, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("llm-pi-ai provider %q thinkingBudgets must be an object", route)
	}
	result := make(map[string]int, len(value))
	for level, rawBudget := range value {
		if level != "minimal" && level != "low" && level != "medium" && level != "high" {
			return nil, fmt.Errorf("llm-pi-ai provider %q thinkingBudgets uses unsupported level %q", route, level)
		}
		budget, ok := piAIPositiveInteger(rawBudget)
		if !ok {
			return nil, fmt.Errorf("llm-pi-ai provider %q thinkingBudgets.%s must be a positive integer", route, level)
		}
		result[level] = budget
	}
	return result, nil
}

func optionalPositiveInteger(value map[string]any, key string, fallback int, path string) (int, error) {
	raw, exists := value[key]
	if !exists {
		return fallback, nil
	}
	parsed, ok := piAIPositiveInteger(raw)
	if !ok {
		return 0, fmt.Errorf("%s %s must be a positive integer", path, key)
	}
	return parsed, nil
}

func piAIPositiveInteger(raw any) (int, bool) {
	switch value := raw.(type) {
	case int:
		return value, value > 0
	case int64:
		if value <= 0 || uint64(value) > uint64(^uint(0)>>1) {
			return 0, false
		}
		return int(value), true
	case float64:
		if value <= 0 || value > float64(^uint(0)>>1) || value != float64(int(value)) {
			return 0, false
		}
		return int(value), true
	case json.Number:
		integer, err := value.Int64()
		if err != nil || integer <= 0 || uint64(integer) > uint64(^uint(0)>>1) {
			return 0, false
		}
		return int(integer), true
	default:
		return 0, false
	}
}

func anySlice(raw any) ([]any, bool) {
	switch value := raw.(type) {
	case []any:
		return value, true
	case []string:
		out := make([]any, len(value))
		for index, item := range value {
			out[index] = item
		}
		return out, true
	default:
		return nil, false
	}
}

func piAIModalities(raw any, fallback []string, path string) ([]string, error) {
	if raw == nil {
		return append([]string(nil), fallback...), nil
	}
	rows, ok := anySlice(raw)
	if !ok || len(rows) == 0 {
		return nil, fmt.Errorf("%s must be a non-empty array", path)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		modality, ok := row.(string)
		if !ok || modality != "text" && modality != "image" {
			return nil, fmt.Errorf("%s supports only text and image", path)
		}
		out = append(out, modality)
	}
	return out, nil
}

func piAIHeaders(raw any, route string) (map[string]string, error) {
	if raw == nil {
		return nil, nil
	}
	rows, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("llm-pi-ai provider %q headers must be an object", route)
	}
	headers := make(map[string]string, len(rows))
	for name, rawValue := range rows {
		value, ok := rawValue.(string)
		if !ok || !httpgutsValidHeaderName(name) || !httpgutsValidHeaderValue(value) {
			return nil, fmt.Errorf("llm-pi-ai provider %q has an invalid HTTP header %q", route, name)
		}
		headers[name] = value
	}
	return headers, nil
}

func httpgutsValidHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for index := 0; index < len(name); index++ {
		b := name[index]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(b))) {
			return false
		}
	}
	return true
}

func httpgutsValidHeaderValue(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 && value[index] != '\t' || value[index] == 0x7f {
			return false
		}
	}
	return true
}

func piAIReasoningLevel(value string) bool {
	switch value {
	case "off", "minimal", "low", "medium", "high", "xhigh", "max":
		return true
	default:
		return false
	}
}

func resolvePiAIRetryPolicy(raw any, route string) (RetryPolicy, error) {
	if raw == nil {
		return defaultRetryPolicy(), nil
	}
	value, ok := raw.(map[string]any)
	if !ok {
		return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy must be an object", route)
	}
	mode, ok := value["mode"].(string)
	if !ok || mode != RetryNormal && mode != RetryAlways {
		return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy.mode must be normal or always", route)
	}
	allowed := map[string]bool{"mode": true, "backoff": true, "maxRetries": true, "retryableCodes": true}
	for key := range value {
		if !allowed[key] {
			return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy uses unsupported field %q", route, key)
		}
	}
	policy := defaultRetryPolicy()
	policy.Mode = mode
	if mode == RetryAlways {
		policy.MaxRetries = 0
	}
	if rawMax, exists := value["maxRetries"]; exists && mode == RetryNormal {
		maxRetries, valid := nonNegativeInteger(rawMax)
		if !valid {
			return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy.maxRetries must be a non-negative integer", route)
		}
		policy.MaxRetries = maxRetries
	}
	if rawCodes, exists := value["retryableCodes"]; exists && mode == RetryNormal {
		rows, valid := anySlice(rawCodes)
		if !valid || len(rows) == 0 {
			return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy.retryableCodes must be a non-empty array", route)
		}
		codes := make([]string, 0, len(rows))
		seen := map[string]bool{}
		for _, row := range rows {
			code, valid := row.(string)
			code = strings.ToUpper(strings.TrimSpace(code))
			if !valid || code == "" || seen[code] {
				return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy.retryableCodes must contain unique non-empty strings", route)
			}
			seen[code] = true
			codes = append(codes, code)
		}
		policy.RetryableCodes = codes
	}
	if rawBackoff, exists := value["backoff"]; exists {
		backoff, valid := rawBackoff.(map[string]any)
		if !valid {
			return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy.backoff must be an object", route)
		}
		for key := range backoff {
			if key != "initialDelayMs" && key != "maxDelayMs" && key != "jitterRatio" {
				return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy.backoff uses unsupported field %q", route, key)
			}
		}
		if rawInitial, exists := backoff["initialDelayMs"]; exists {
			initial, valid := piAIPositiveInteger(rawInitial)
			if !valid {
				return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy.backoff.initialDelayMs must be positive", route)
			}
			policy.InitialDelay = time.Duration(initial) * time.Millisecond
		}
		if rawMax, exists := backoff["maxDelayMs"]; exists {
			maximum, valid := piAIPositiveInteger(rawMax)
			if !valid {
				return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy.backoff.maxDelayMs must be positive", route)
			}
			policy.MaxDelay = time.Duration(maximum) * time.Millisecond
		}
		if rawJitter, exists := backoff["jitterRatio"]; exists {
			jitter, valid := numericSetting(rawJitter)
			if !valid || jitter < 0 || jitter > 1 {
				return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy.backoff.jitterRatio must be between 0 and 1", route)
			}
			policy.JitterRatio = jitter
		}
	}
	if policy.InitialDelay <= 0 || policy.MaxDelay <= 0 || policy.InitialDelay > policy.MaxDelay {
		return RetryPolicy{}, fmt.Errorf("llm-pi-ai provider %q retryPolicy backoff must use positive delays with initialDelayMs <= maxDelayMs", route)
	}
	return normalizeRetryPolicy(policy), nil
}

func nonNegativeInteger(raw any) (int, bool) {
	if raw == float64(0) || raw == int(0) || raw == int64(0) {
		return 0, true
	}
	return piAIPositiveInteger(raw)
}

func numericSetting(raw any) (float64, bool) {
	switch value := raw.(type) {
	case float64:
		return value, true
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	case json.Number:
		parsed, err := value.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func positiveFiniteMilliseconds(raw any) (float64, bool) {
	milliseconds, ok := numericSetting(raw)
	if !ok || math.IsNaN(milliseconds) || math.IsInf(milliseconds, 0) || milliseconds <= 0 || milliseconds > piAIMaxTimerMillis {
		return 0, false
	}
	return milliseconds, true
}

func millisecondsDuration(milliseconds float64) time.Duration {
	duration := time.Duration(milliseconds * float64(time.Millisecond))
	if duration <= 0 {
		return time.Nanosecond
	}
	return duration
}

func (e *Engine) replacePiAIProvidersLocked(next map[string]*managedPiAIProvider) {
	for route, current := range e.piAIProviders {
		if e.providers[route] == current {
			delete(e.providers, route)
		}
	}
	for route, provider := range next {
		e.providers[route] = provider
	}
	e.piAIProviders = next
}
