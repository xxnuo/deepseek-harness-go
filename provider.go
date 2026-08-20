package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type EchoProvider struct{ id, name string }

func NewEchoProvider(id, name string) *EchoProvider {
	if id == "" {
		id = "echo"
	}
	if name == "" {
		name = "Echo"
	}
	return &EchoProvider{id: id, name: name}
}
func (p *EchoProvider) ID() string   { return p.id }
func (p *EchoProvider) Name() string { return p.name }
func (p *EchoProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "echo", Name: "Echo", InputModalities: []string{"text"}}}, nil
}
func (p *EchoProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	if err := ctx.Err(); err != nil {
		return Completion{}, err
	}
	var text string
	if len(req.Messages) > 0 {
		text = req.Messages[len(req.Messages)-1].Content
	}
	if err := onDelta(Delta{Text: text, Finish: "stop"}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: text, Finish: "stop"}, nil
}

type OpenAIProvider struct {
	id, baseURL, apiKey, model string
	client                     *http.Client
	headers                    map[string]string
	modelSpec                  piAIModel
	cacheRetention             string
	streamIdleTimeout          time.Duration
}

type openAIWireTool struct {
	Type         string         `json:"type"`
	CacheControl map[string]any `json:"cache_control,omitempty"`
	Function     struct {
		Name        string         `json:"name"`
		Description string         `json:"description,omitempty"`
		Parameters  map[string]any `json:"parameters"`
	} `json:"function"`
}

type openAIWireToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type openAIWireMessage struct {
	Role             string               `json:"role"`
	Content          any                  `json:"content"`
	ReasoningContent string               `json:"reasoning_content,omitempty"`
	ToolCalls        []openAIWireToolCall `json:"tool_calls,omitempty"`
	ToolCallID       string               `json:"tool_call_id,omitempty"`
}

type openAIWireThinking struct {
	Type string `json:"type"`
}

func openAIWireMessages(messages []ChatMessage) []openAIWireMessage {
	out := make([]openAIWireMessage, 0, len(messages))
	for _, message := range messages {
		var content any = message.Content
		if len(message.Images) > 0 {
			parts := make([]map[string]any, 0, len(message.Images)+1)
			if message.Content != "" {
				parts = append(parts, map[string]any{"type": "text", "text": message.Content})
			}
			for _, image := range message.Images {
				parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:" + image.MediaType + ";base64," + image.Data}})
			}
			content = parts
		}
		wire := openAIWireMessage{Role: message.Role, Content: content, ToolCallID: message.ToolCallID}
		if message.Reasoning != "" && len(message.ToolCalls) > 0 {
			wire.ReasoningContent = message.Reasoning
		}
		for _, call := range message.ToolCalls {
			encoded := string(call.Arguments)
			if encoded == "" {
				encoded = "{}"
			}
			item := openAIWireToolCall{ID: call.ID, Type: "function"}
			item.Function.Name, item.Function.Arguments = call.Name, encoded
			wire.ToolCalls = append(wire.ToolCalls, item)
		}
		// Provider APIs reject null content for tool-call assistant turns.
		if wire.Role == "assistant" && message.Content == "" && len(message.Images) == 0 {
			wire.Content = ""
		}
		if wire.Role == "tool" && message.Content == "" {
			wire.Content = "(no output)"
		}
		out = append(out, wire)
	}
	return out
}

func NewOpenAIProvider(id, baseURL, apiKey, model string) *OpenAIProvider {
	if id == "" {
		id = "deepseek"
	}
	if baseURL == "" {
		baseURL = "https://api.deepseek.com"
	}
	if model == "" {
		model = "deepseek-chat"
	}
	return &OpenAIProvider{id: id, baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, model: model, client: &http.Client{}}
}

type openAICompletionsCompat struct {
	thinkingFormat             string
	cacheControlFormat         string
	maxTokensField             string
	sessionAffinityFormat      string
	supportsReasoningEffort    bool
	supportsLongCacheRetention bool
	supportsStore              bool
	supportsUsageInStreaming   bool
	sendSessionAffinityHeaders bool
}

func resolveOpenAICompletionsCompat(provider, baseURL string, model piAIModel) openAICompletionsCompat {
	isOpenRouter := provider == "openrouter" || strings.Contains(baseURL, "openrouter.ai")
	isTogether := provider == "together" || strings.Contains(baseURL, "together.xyz")
	isZAI := provider == "zai" || provider == "zai-coding-cn" || strings.Contains(baseURL, "bigmodel.cn") || strings.Contains(baseURL, "api.z.ai")
	isMoonshot := provider == "moonshotai" || provider == "moonshotai-cn" || strings.Contains(baseURL, "api.moonshot.")
	isNVIDIA := provider == "nvidia" || strings.Contains(baseURL, "integrate.api.nvidia.com")
	isAntLing := provider == "ant-ling" || strings.Contains(baseURL, "api.ant-ling.com")
	isCloudflare := provider == "cloudflare-ai-gateway" || strings.Contains(baseURL, "gateway.ai.cloudflare.com")
	isDeepSeek := provider == "deepseek" || strings.Contains(baseURL, "deepseek.com")
	isGrok := provider == "xai" || strings.Contains(baseURL, "api.x.ai")
	isNonStandard := isNVIDIA || provider == "cerebras" || strings.Contains(baseURL, "cerebras.ai") || isGrok || isTogether || strings.Contains(baseURL, "chutes.ai") || isDeepSeek || isZAI || isMoonshot || provider == "opencode" || strings.Contains(baseURL, "opencode.ai") || provider == "cloudflare-workers-ai" || strings.Contains(baseURL, "api.cloudflare.com") || isCloudflare || isAntLing
	sessionAffinityFormat := "openai"
	if isOpenRouter {
		sessionAffinityFormat = "openrouter"
	}
	compat := openAICompletionsCompat{
		thinkingFormat: "openai", maxTokensField: "max_completion_tokens",
		sessionAffinityFormat:      sessionAffinityFormat,
		supportsReasoningEffort:    !isGrok && !isZAI && !isMoonshot && !isTogether && !isCloudflare && !isNVIDIA && !isAntLing,
		supportsLongCacheRetention: !isTogether && provider != "cloudflare-workers-ai" && !isCloudflare && !isNVIDIA && !isAntLing,
		supportsStore:              !isNonStandard, supportsUsageInStreaming: true,
	}
	switch {
	case isDeepSeek:
		compat.thinkingFormat = "deepseek"
	case isZAI:
		compat.thinkingFormat = "zai"
	case isTogether:
		compat.thinkingFormat = "together"
	case isAntLing:
		compat.thinkingFormat = "ant-ling"
	case isOpenRouter:
		compat.thinkingFormat = "openrouter"
	}
	if strings.Contains(baseURL, "chutes.ai") || isMoonshot || isCloudflare || isTogether || isNVIDIA || isAntLing {
		compat.maxTokensField = "max_tokens"
	}
	if isOpenRouter && strings.HasPrefix(model.ID, "anthropic/") {
		compat.cacheControlFormat = "anthropic"
	}
	if model.ID == "" {
		compat.maxTokensField = "max_tokens"
		compat.supportsStore = false
	}
	if model.Compat.ThinkingFormat != "" {
		compat.thinkingFormat = model.Compat.ThinkingFormat
	}
	if model.Compat.CacheControlFormat != "" {
		compat.cacheControlFormat = model.Compat.CacheControlFormat
	}
	if model.Compat.MaxTokensField != "" {
		compat.maxTokensField = model.Compat.MaxTokensField
	}
	if model.Compat.SessionAffinityFormat != "" {
		compat.sessionAffinityFormat = model.Compat.SessionAffinityFormat
	}
	if value := model.Compat.SupportsReasoningEffort; value != nil {
		compat.supportsReasoningEffort = *value
	}
	if value := model.Compat.SupportsLongCacheRetention; value != nil {
		compat.supportsLongCacheRetention = *value
	}
	if value := model.Compat.SupportsStore; value != nil {
		compat.supportsStore = *value
	}
	if value := model.Compat.SupportsUsageInStreaming; value != nil {
		compat.supportsUsageInStreaming = *value
	}
	if value := model.Compat.SendSessionAffinityHeaders; value != nil {
		compat.sendSessionAffinityHeaders = *value
	}
	return compat
}

func applyOpenAICompletionsSessionHeaders(headers http.Header, sessionID string, compat openAICompletionsCompat, cacheRetention string) {
	if sessionID == "" || cacheRetention == "none" || !compat.sendSessionAffinityHeaders {
		return
	}
	if compat.sessionAffinityFormat == "openrouter" {
		headers.Set("X-Session-Id", sessionID)
		return
	}
	if compat.sessionAffinityFormat == "openai" {
		headers.Set("Session-Id", sessionID)
	}
	headers.Set("X-Client-Request-Id", sessionID)
	headers.Set("X-Session-Affinity", sessionID)
}

func resolvedPiAICacheRetention(value string) string {
	if value == "" {
		return "short"
	}
	return value
}

func openAIPromptCacheKey(value string) string {
	runes := []rune(value)
	if len(runes) > 64 {
		runes = runes[:64]
	}
	return string(runes)
}

func piAIReasoningWire(model piAIModel, level string) (string, bool) {
	if value, exists := model.ThinkingLevelMap[level]; exists {
		if value == nil {
			return "", false
		}
		return *value, true
	}
	return level, level != "off"
}

func applyOpenAICompletionsReasoning(body map[string]any, model piAIModel, compat openAICompletionsCompat, req ChatRequest) {
	if model.ID == "" {
		if req.Thinking != "" {
			body["thinking"] = &openAIWireThinking{Type: req.Thinking}
		}
		if req.ReasoningEffort != "" {
			body["reasoning_effort"] = req.ReasoningEffort
		}
		return
	}
	if !model.Reasoning {
		return
	}
	enabled := req.ReasoningEffort != "" && req.ReasoningEffort != "off"
	wire, hasWire := piAIReasoningWire(model, req.ReasoningEffort)
	switch compat.thinkingFormat {
	case "zai":
		body["thinking"] = map[string]any{"type": map[bool]string{true: "enabled", false: "disabled"}[enabled]}
		if enabled && compat.supportsReasoningEffort && hasWire {
			body["reasoning_effort"] = wire
		}
	case "qwen":
		body["enable_thinking"] = enabled
	case "deepseek":
		if enabled {
			body["thinking"] = map[string]any{"type": "enabled"}
		} else if off, exists := model.ThinkingLevelMap["off"]; !exists || off != nil {
			body["thinking"] = map[string]any{"type": "disabled"}
		}
		if enabled && compat.supportsReasoningEffort && hasWire {
			body["reasoning_effort"] = wire
		}
	case "openrouter":
		if enabled && hasWire {
			body["reasoning"] = map[string]any{"effort": wire}
		} else if off, exists := model.ThinkingLevelMap["off"]; !exists || off != nil {
			value := "none"
			if exists && off != nil {
				value = *off
			}
			body["reasoning"] = map[string]any{"effort": value}
		}
	case "ant-ling":
		if enabled && hasWire {
			body["reasoning"] = map[string]any{"effort": wire}
		}
	case "together":
		body["reasoning"] = map[string]any{"enabled": enabled}
		if enabled && compat.supportsReasoningEffort && hasWire {
			body["reasoning_effort"] = wire
		}
	case "string-thinking":
		if enabled && hasWire {
			body["thinking"] = wire
		} else if off, exists := model.ThinkingLevelMap["off"]; !exists || off != nil {
			value := "none"
			if exists && off != nil {
				value = *off
			}
			body["thinking"] = value
		}
	default:
		if enabled && compat.supportsReasoningEffort && hasWire {
			body["reasoning_effort"] = wire
		} else if !enabled && compat.supportsReasoningEffort {
			if off, exists := model.ThinkingLevelMap["off"]; exists && off != nil {
				body["reasoning_effort"] = *off
			}
		}
	}
}

func openAICompletionsCacheControl(compat openAICompletionsCompat, retention string) map[string]any {
	if compat.cacheControlFormat != "anthropic" || retention == "none" {
		return nil
	}
	control := map[string]any{"type": "ephemeral"}
	if retention == "long" && compat.supportsLongCacheRetention {
		control["ttl"] = "1h"
	}
	return control
}

func applyOpenAICompletionsCacheControl(messages []openAIWireMessage, tools []openAIWireTool, control map[string]any) {
	if len(tools) > 0 {
		tools[len(tools)-1].CacheControl = control
	}
	apply := func(index int) {
		switch content := messages[index].Content.(type) {
		case string:
			messages[index].Content = []any{map[string]any{"type": "text", "text": content, "cache_control": control}}
		case []map[string]any:
			if len(content) > 0 {
				content[len(content)-1]["cache_control"] = control
			}
		}
	}
	for index := range messages {
		if messages[index].Role == "system" {
			apply(index)
			break
		}
	}
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].Role == "user" || messages[index].Role == "assistant" || messages[index].Role == "tool" {
			apply(index)
			break
		}
	}
}

func (p *OpenAIProvider) applyHeaders(req *http.Request) {
	for name, value := range p.headers {
		req.Header.Set(name, value)
	}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
		return time.Duration(seconds * float64(time.Second))
	}
	if at, err := http.ParseTime(value); err == nil {
		delay := at.Sub(now)
		if delay > 0 {
			return delay
		}
	}
	return 0
}

func providerHTTPFailure(resp *http.Response, body string) error {
	status := resp.StatusCode
	code := "PROVIDER"
	detail := body
	var payload struct {
		Error struct {
			Code    any    `json:"code"`
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(body), &payload) == nil {
		detail = fmt.Sprint(payload.Error.Code) + " " + payload.Error.Type + " " + payload.Error.Message
	}
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = "AUTH"
	case status == http.StatusTooManyRequests:
		code = "RATE_LIMIT"
	case status == http.StatusBadRequest && isContextWindowExceeded(detail):
		code = "CONTEXT_WINDOW_EXCEEDED"
	case status == http.StatusRequestTimeout || status == 524:
		code = "TIMEOUT"
	case status >= 500 && status <= 599:
		code = "SERVER"
	}
	message := fmt.Sprintf("provider HTTP %d", status)
	if strings.TrimSpace(body) != "" {
		message += ": " + strings.TrimSpace(body)
	}
	return &ProviderError{
		Code:       code,
		Message:    message,
		Status:     status,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		RequestID:  strings.TrimSpace(resp.Header.Get("x-request-id")),
	}
}

func isContextWindowExceeded(detail string) bool {
	detail = strings.ToLower(detail)
	for _, marker := range []string{
		"context_length_exceeded", "context window exceeded", "context length exceeded",
		"maximum context length", "maximum context window", "max context length", "max context window",
		"request too large for model context", "prompt too long for this model", "input too long for this model",
	} {
		if strings.Contains(detail, marker) {
			return true
		}
	}
	return false
}

func (p *OpenAIProvider) ID() string   { return p.id }
func (p *OpenAIProvider) Name() string { return p.id }
func (p *OpenAIProvider) Models(ctx context.Context) ([]ModelInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/models", nil)
	if err != nil {
		return nil, err
	}
	p.applyHeaders(req)
	if p.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, providerHTTPFailure(resp, string(b))
	}
	const maxDiscoveryBytes = 4 << 20
	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxDiscoveryBytes+1))
	if err != nil {
		return nil, err
	}
	if len(bodyBytes) > maxDiscoveryBytes {
		return nil, fmt.Errorf("model discovery: response exceeds %d bytes", maxDiscoveryBytes)
	}
	var body struct {
		Data []struct {
			ID              string `json:"id"`
			Name            string `json:"name"`
			DisplayName     string `json:"display_name"`
			ContextWindow   int    `json:"context_window"`
			ContextLength   int    `json:"context_length"`
			MaxTokens       int    `json:"max_tokens"`
			MaxOutputTokens int    `json:"max_output_tokens"`
		} `json:"data"`
	}
	if err := json.Unmarshal(bodyBytes, &body); err != nil {
		return nil, err
	}
	out := make([]ModelInfo, 0, len(body.Data))
	seen := make(map[string]struct{}, len(body.Data))
	for _, m := range body.Data {
		if strings.TrimSpace(m.ID) == "" {
			continue
		}
		if _, exists := seen[m.ID]; exists {
			continue
		}
		seen[m.ID] = struct{}{}
		name := m.Name
		if name == "" {
			name = m.DisplayName
		}
		if name == "" {
			name = m.ID
		}
		contextWindow := m.ContextWindow
		if contextWindow <= 0 {
			contextWindow = m.ContextLength
		}
		maxTokens := m.MaxTokens
		if maxTokens <= 0 {
			maxTokens = m.MaxOutputTokens
		}
		out = append(out, ModelInfo{ID: m.ID, Name: name, ContextWindow: contextWindow, MaxTokens: maxTokens})
	}
	return out, nil
}

func (p *OpenAIProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	model := req.Model
	if model == "" {
		model = p.model
	}
	tools := make([]openAIWireTool, 0, len(req.Tools))
	for _, schema := range req.Tools {
		item := openAIWireTool{Type: "function"}
		item.Function.Name, item.Function.Description, item.Function.Parameters = schema.Name, schema.Description, schema.Parameters
		tools = append(tools, item)
	}
	messages := openAIWireMessages(req.Messages)
	if req.System != "" {
		messages = append([]openAIWireMessage{{Role: "system", Content: req.System}}, messages...)
	}
	compat := resolveOpenAICompletionsCompat(p.id, p.baseURL, p.modelSpec)
	cacheRetention := resolvedPiAICacheRetention(p.cacheRetention)
	if cache := openAICompletionsCacheControl(compat, cacheRetention); cache != nil {
		applyOpenAICompletionsCacheControl(messages, tools, cache)
	}
	body := map[string]any{"model": model, "messages": messages, "stream": true}
	if compat.supportsUsageInStreaming {
		body["stream_options"] = map[string]bool{"include_usage": true}
	}
	if len(tools) > 0 {
		body["tools"] = tools
	}
	if compat.supportsStore {
		body["store"] = false
	}
	if req.MaxTokens > 0 {
		body[compat.maxTokensField] = req.MaxTokens
	}
	if req.Temperature != nil {
		body["temperature"] = *req.Temperature
	}
	if key := openAIPromptCacheKey(req.SessionID); key != "" && ((strings.Contains(p.baseURL, "api.openai.com") && cacheRetention != "none") || cacheRetention == "long" && compat.supportsLongCacheRetention) {
		body["prompt_cache_key"] = key
	}
	if cacheRetention == "long" && compat.supportsLongCacheRetention {
		body["prompt_cache_retention"] = "24h"
	}
	applyOpenAICompletionsReasoning(body, p.modelSpec, compat, req)
	data, err := json.Marshal(body)
	if err != nil {
		return Completion{}, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(data))
	if err != nil {
		return Completion{}, err
	}
	applyOpenAICompletionsSessionHeaders(hreq.Header, req.SessionID, compat, cacheRetention)
	p.applyHeaders(hreq)
	hreq.Header.Set("Content-Type", "application/json")
	if p.apiKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+p.apiKey)
	}
	resp, err := p.client.Do(hreq)
	if err != nil {
		if ctx.Err() != nil {
			return Completion{}, ctx.Err()
		}
		return Completion{}, &ProviderError{Code: "TRANSPORT", Message: err.Error(), Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return Completion{}, providerHTTPFailure(resp, string(b))
	}
	var text, reasoning, finish string
	callParts := map[int]*ToolCall{}
	callOrder := []int{}
	usage := map[string]any{}
	done := false
	err = scanProviderSSE(ctx, resp.Body, p.streamIdleTimeout, func(line string) error {
		if line == "" || !strings.HasPrefix(line, "data:") {
			return nil
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			done = true
			return nil
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content   string `json:"content"`
					Reasoning string `json:"reasoning_content"`
					ToolCalls []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage map[string]any `json:"usage"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			return fmt.Errorf("malformed SSE payload: %w", err)
		}
		if len(chunk.Choices) > 0 {
			d := chunk.Choices[0].Delta
			if d.Content != "" {
				text += d.Content
			}
			if d.Reasoning != "" {
				reasoning += d.Reasoning
			}
			if chunk.Choices[0].FinishReason != "" {
				finish = chunk.Choices[0].FinishReason
			}
			toolDeltas := make([]ToolCallDelta, 0, len(d.ToolCalls))
			for _, part := range d.ToolCalls {
				call, ok := callParts[part.Index]
				if !ok {
					call = &ToolCall{}
					callParts[part.Index] = call
					callOrder = append(callOrder, part.Index)
				}
				if part.ID != "" {
					call.ID = part.ID
				}
				if part.Function.Name != "" {
					call.Name = part.Function.Name
				}
				if part.Function.Arguments != "" {
					call.Arguments = append(call.Arguments, part.Function.Arguments...)
				}
				toolDeltas = append(toolDeltas, ToolCallDelta{Index: part.Index, ID: part.ID, Name: part.Function.Name, ArgumentsDelta: part.Function.Arguments})
			}
			if d.Content != "" || d.Reasoning != "" || len(toolDeltas) > 0 || finish != "" {
				if err := onDelta(Delta{Text: d.Content, Reasoning: d.Reasoning, ToolCalls: toolDeltas, Finish: finish}); err != nil {
					return err
				}
			}
		}
		if chunk.Usage != nil {
			usage = chunk.Usage
		}
		return nil
	})
	if err != nil {
		return Completion{}, err
	}
	if !done {
		return Completion{}, &ProviderError{Code: "STREAM_CLOSED", Message: "provider stream ended without [DONE]"}
	}
	if finish != "" && finish != "stop" && finish != "tool_calls" && finish != "length" {
		return Completion{}, &ProviderError{Code: "PROVIDER", Message: fmt.Sprintf("model stopped: %s", finish)}
	}
	if finish == "" {
		finish = "stop"
	}
	calls := make([]ToolCall, 0, len(callOrder))
	for _, index := range callOrder {
		if call := callParts[index]; call != nil {
			calls = append(calls, *call)
		}
	}
	if text == "" && reasoning == "" && len(calls) == 0 {
		return Completion{}, &ProviderError{Code: "EMPTY_RESPONSE", Message: "model returned a completed response with no content"}
	}
	return Completion{Text: text, Reasoning: reasoning, ToolCalls: calls, Usage: usage, Finish: finish}, nil
}
