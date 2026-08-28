package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"
)

type OpenAIResponsesProvider struct {
	id, baseURL, apiKey, model string
	client                     *http.Client
	headers                    map[string]string
	cacheRetention             string
	transport                  string
	websocketConnectTimeout    time.Duration
	websocketConnectTimeoutSet bool
	modelSpec                  piAIModel
	websockets                 *openAIResponsesWebSocketPool
	streamIdleTimeout          time.Duration
	strictDefault              bool
}

func NewOpenAIResponsesProvider(id, baseURL, apiKey, model string) *OpenAIResponsesProvider {
	if id == "" {
		id = "openai"
	}
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	if model == "" {
		model = "gpt-4.1"
	}
	return &OpenAIResponsesProvider{
		id: id, baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, model: model,
		client: &http.Client{}, websockets: newOpenAIResponsesWebSocketPool(),
	}
}

func (p *OpenAIResponsesProvider) ID() string   { return p.id }
func (p *OpenAIResponsesProvider) Name() string { return p.id }
func (p *OpenAIResponsesProvider) ResolveModelInfo(ctx context.Context, model string) (ModelInfo, error) {
	provider := NewOpenAIProvider(p.id, p.baseURL, p.apiKey, p.model)
	provider.modelSpec = p.modelSpec
	return provider.ResolveModelInfo(ctx, model)
}
func (p *OpenAIResponsesProvider) Models(ctx context.Context) ([]ModelInfo, error) {
	provider := NewOpenAIProvider(p.id, p.baseURL, p.apiKey, p.model)
	provider.client, provider.headers = p.client, clonePIAIHeaders(p.headers)
	return provider.Models(ctx)
}

func openAIResponsesInput(req ChatRequest, model piAIModel) []any {
	input := make([]any, 0, len(req.Messages)+1)
	if req.System != "" {
		role := "system"
		if model.Reasoning && (model.Compat.SupportsDeveloperRole == nil || *model.Compat.SupportsDeveloperRole) {
			role = "developer"
		}
		input = append(input, map[string]any{"role": role, "content": req.System})
	}
	for _, message := range req.Messages {
		switch message.Role {
		case "user":
			content := make([]any, 0, len(chatContentParts(message)))
			for _, part := range chatContentParts(message) {
				if part.Type == "text" {
					content = append(content, map[string]any{"type": "input_text", "text": part.Text})
				} else if part.Type == "image" {
					content = append(content, map[string]any{
						"type": "input_image", "detail": "auto",
						"image_url": "data:" + part.MediaType + ";base64," + part.Data,
					})
				}
			}
			if len(content) > 0 {
				input = append(input, map[string]any{"role": "user", "content": content})
			}
		case "assistant":
			if message.Content != "" {
				input = append(input, map[string]any{
					"type": "message", "role": "assistant", "status": "completed",
					"content": []any{map[string]any{"type": "output_text", "text": message.Content, "annotations": []any{}}},
				})
			}
			for _, call := range message.ToolCalls {
				arguments := string(call.Arguments)
				if arguments == "" {
					arguments = "{}"
				}
				input = append(input, map[string]any{
					"type": "function_call", "call_id": call.ID, "name": call.Name, "arguments": arguments,
				})
			}
		case "tool":
			var output any = message.Content
			if chatMessageHasImage(message) && (len(model.Input) == 0 || slices.Contains(model.Input, "image")) {
				parts := make([]any, 0, len(chatContentParts(message)))
				for _, part := range chatContentParts(message) {
					if part.Type == "text" {
						parts = append(parts, map[string]any{"type": "input_text", "text": part.Text})
					} else if part.Type == "image" {
						parts = append(parts, map[string]any{"type": "input_image", "detail": "auto", "image_url": "data:" + part.MediaType + ";base64," + part.Data})
					}
				}
				output = parts
			} else if output == "" {
				if chatMessageHasImage(message) {
					output = "(see attached image)"
				} else {
					output = "(no output)"
				}
			}
			input = append(input, map[string]any{
				"type": "function_call_output", "call_id": message.ToolCallID, "output": output,
			})
		case "system":
			if message.Content != "" {
				input = append(input, map[string]any{"role": "system", "content": message.Content})
			}
		}
	}
	return input
}

func openAIResponsesTools(schemas []ToolSchema, supportsStrict bool, strict any) []any {
	tools := make([]any, 0, len(schemas))
	for _, schema := range schemas {
		tool := map[string]any{
			"type": "function", "name": schema.Name, "description": schema.Description, "parameters": schema.Parameters,
		}
		if supportsStrict {
			if constrained, requested, _ := resolveJSONSchemaStrictSampling(schema, supportsStrict); requested {
				tool["strict"] = constrained
			} else {
				tool["strict"] = strict
			}
		}
		tools = append(tools, tool)
	}
	return tools
}

func (p *OpenAIResponsesProvider) requestBody(req ChatRequest) map[string]any {
	model := req.Model
	if model == "" {
		model = p.model
	}
	supportsStrict := p.strictDefault
	if p.modelSpec.Compat.SupportsStrictMode != nil {
		supportsStrict = *p.modelSpec.Compat.SupportsStrictMode
	}
	tools := openAIResponsesTools(req.Tools, supportsStrict, false)
	body := map[string]any{
		"model": model, "input": openAIResponsesInput(req, p.modelSpec), "stream": true, "store": false,
	}
	if len(tools) > 0 {
		body["tools"] = tools
	}
	if req.MaxTokens > 0 {
		body["max_output_tokens"] = max(req.MaxTokens, 16)
	}
	if req.Temperature != nil {
		body["temperature"] = *req.Temperature
	}
	if p.modelSpec.ID == "" {
		if req.ReasoningEffort != "" && req.ReasoningEffort != "off" {
			body["reasoning"] = map[string]any{"effort": req.ReasoningEffort, "summary": "auto"}
			body["include"] = []string{"reasoning.encrypted_content"}
		}
	} else if p.modelSpec.Reasoning {
		enabled := req.ReasoningEffort != "" && req.ReasoningEffort != "off"
		if enabled {
			if effort, ok := piAIReasoningWire(p.modelSpec, req.ReasoningEffort); ok {
				body["reasoning"] = map[string]any{"effort": effort, "summary": "auto"}
				body["include"] = []string{"reasoning.encrypted_content"}
			}
		} else if p.id != "github-copilot" {
			if off, exists := p.modelSpec.ThinkingLevelMap["off"]; !exists || off != nil {
				effort := "none"
				if exists && off != nil {
					effort = *off
				}
				body["reasoning"] = map[string]any{"effort": effort}
			}
		}
	}
	retention := resolvedPiAICacheRetention(p.cacheRetention)
	if key := openAIPromptCacheKey(req.SessionID); key != "" && retention != "none" {
		body["prompt_cache_key"] = key
	}
	supportsLong := p.modelSpec.Compat.SupportsLongCacheRetention == nil || *p.modelSpec.Compat.SupportsLongCacheRetention
	switch {
	case retention == "long" && supportsLong:
		body["prompt_cache_retention"] = "24h"
	case retention == "none" && p.modelSpec.Compat.SupportsExplicitPromptCacheMode != nil && *p.modelSpec.Compat.SupportsExplicitPromptCacheMode:
		body["prompt_cache_options"] = map[string]any{"mode": "explicit"}
	}
	return body
}

func (p *OpenAIResponsesProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	supportsStrict := p.strictDefault
	if p.modelSpec.Compat.SupportsStrictMode != nil {
		supportsStrict = *p.modelSpec.Compat.SupportsStrictMode
	}
	if err := validateToolSampling(req.Tools, supportsStrict); err != nil {
		return Completion{}, err
	}
	body := p.requestBody(req)
	if p.transport != "" && p.transport != "sse" {
		completion, started, err := p.completeWebSocket(ctx, req, body, onDelta)
		if err == nil || started || ctx.Err() != nil {
			return completion, err
		}
	}
	return p.completeSSE(ctx, req, body, onDelta)
}

func (p *OpenAIResponsesProvider) completeSSE(ctx context.Context, req ChatRequest, body map[string]any, onDelta func(Delta) error) (Completion, error) {
	headers := make(http.Header)
	headerRequest := &http.Request{Header: headers}
	p.applyRequestHeaders(headerRequest, req)
	headers.Set("Content-Type", "application/json")
	if p.apiKey != "" {
		headers.Set("Authorization", "Bearer "+p.apiKey)
	}
	return completeResponsesSSE(ctx, p.client, p.baseURL+"/responses", headers, body, p.streamIdleTimeout, onDelta)
}

func completeResponsesSSE(ctx context.Context, client *http.Client, endpoint string, headers http.Header, body map[string]any, idleTimeout time.Duration, onDelta func(Delta) error) (Completion, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return Completion{}, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return Completion{}, err
	}
	for name, values := range headers {
		for _, value := range values {
			hreq.Header.Add(name, value)
		}
	}
	resp, err := client.Do(hreq)
	if err != nil {
		if ctx.Err() != nil {
			return Completion{}, ctx.Err()
		}
		return Completion{}, &ProviderError{Code: "TRANSPORT", Message: err.Error(), Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return Completion{}, providerHTTPFailure(resp, string(payload))
	}

	state := newOpenAIResponsesState(onDelta)
	err = scanProviderSSE(ctx, resp.Body, idleTimeout, func(line string) error {
		if line == "" || !strings.HasPrefix(line, "data:") {
			return nil
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			return nil
		}
		return state.handleJSON([]byte(payload), "SSE")
	})
	if err != nil {
		return Completion{}, err
	}
	return state.completion()
}

func (p *OpenAIResponsesProvider) applyRequestHeaders(req *http.Request, chat ChatRequest) {
	if resolvedPiAICacheRetention(p.cacheRetention) != "none" && chat.SessionID != "" {
		format := p.modelSpec.Compat.SessionAffinityFormat
		if format == "" {
			if p.id == "openrouter" || strings.Contains(p.baseURL, "openrouter.ai") {
				format = "openrouter"
			} else {
				format = "openai"
			}
		}
		if format == "openrouter" {
			req.Header.Set("X-Session-Id", chat.SessionID)
		} else {
			if format == "openai" {
				req.Header.Set("Session-Id", chat.SessionID)
			}
			req.Header.Set("X-Client-Request-Id", chat.SessionID)
		}
	}
	for name, value := range p.headers {
		req.Header.Set(name, value)
	}
}

type openAIResponsesState struct {
	onDelta     func(Delta) error
	text        string
	reasoning   string
	finish      string
	usage       map[string]any
	calls       map[int]*ToolCall
	order       []int
	outputItems map[int]map[string]any
	terminal    bool
	responseID  string
}

func newOpenAIResponsesState(onDelta func(Delta) error) *openAIResponsesState {
	return &openAIResponsesState{onDelta: onDelta, usage: map[string]any{}, calls: map[int]*ToolCall{}, outputItems: map[int]map[string]any{}}
}

func (s *openAIResponsesState) call(index int) *ToolCall {
	if call := s.calls[index]; call != nil {
		return call
	}
	call := &ToolCall{}
	s.calls[index] = call
	s.order = append(s.order, index)
	return call
}

func (s *openAIResponsesState) handleJSON(payload []byte, transport string) error {
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("malformed Responses %s payload: %w", transport, err)
	}
	typeName := stringSetting(event["type"])
	outputIndex := jsonInt(event["output_index"])
	item, _ := event["item"].(map[string]any)
	switch typeName {
	case "response.output_item.added":
		if item["type"] == "function_call" {
			call := s.call(outputIndex)
			call.ID, call.Name = stringSetting(item["call_id"]), stringSetting(item["name"])
			if arguments := stringSetting(item["arguments"]); arguments != "" {
				call.Arguments = append(call.Arguments, arguments...)
			}
		}
	case "response.output_text.delta", "response.refusal.delta":
		delta := stringSetting(event["delta"])
		s.text += delta
		if delta != "" {
			return s.onDelta(Delta{Text: delta})
		}
	case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
		delta := stringSetting(event["delta"])
		s.reasoning += delta
		if delta != "" {
			return s.onDelta(Delta{Reasoning: delta})
		}
	case "response.function_call_arguments.delta":
		call := s.call(outputIndex)
		delta := stringSetting(event["delta"])
		call.Arguments = append(call.Arguments, delta...)
		return s.onDelta(Delta{ToolCalls: []ToolCallDelta{{Index: outputIndex, ID: call.ID, Name: call.Name, ArgumentsDelta: delta}}})
	case "response.output_item.done":
		if item != nil {
			s.outputItems[outputIndex] = item
		}
		if item["type"] == "function_call" {
			call := s.call(outputIndex)
			call.ID, call.Name = stringSetting(item["call_id"]), stringSetting(item["name"])
			if arguments := stringSetting(item["arguments"]); arguments != "" {
				call.Arguments = json.RawMessage(arguments)
			}
		}
	case "response.completed", "response.done", "response.incomplete":
		s.terminal = true
		response, _ := event["response"].(map[string]any)
		s.responseID = stringSetting(response["id"])
		if typeName == "response.incomplete" || stringSetting(response["status"]) == "incomplete" {
			s.finish = "length"
		} else {
			s.finish = "stop"
		}
		if value, ok := response["usage"].(map[string]any); ok {
			s.usage = value
		}
	case "response.failed", "error":
		return &ProviderError{Code: "PROVIDER", Message: "OpenAI Responses stream failed: " + string(payload)}
	}
	return nil
}

func (s *openAIResponsesState) completion() (Completion, error) {
	if !s.terminal {
		return Completion{}, &ProviderError{Code: "STREAM_CLOSED", Message: "OpenAI Responses stream ended before a terminal response event"}
	}
	toolCalls := make([]ToolCall, 0, len(s.order))
	for _, index := range s.order {
		call := s.calls[index]
		if call == nil {
			continue
		}
		if len(call.Arguments) == 0 {
			call.Arguments = json.RawMessage("{}")
		}
		toolCalls = append(toolCalls, *call)
	}
	if len(toolCalls) > 0 && s.finish == "stop" {
		s.finish = "tool_calls"
	}
	if s.finish == "" {
		s.finish = "stop"
	}
	if err := s.onDelta(Delta{Usage: cloneStringMap(s.usage), Finish: s.finish}); err != nil {
		return Completion{}, err
	}
	if s.text == "" && s.reasoning == "" && len(toolCalls) == 0 {
		return Completion{}, &ProviderError{Code: "EMPTY_RESPONSE", Message: "model returned a completed response with no content"}
	}
	return Completion{Text: s.text, Reasoning: s.reasoning, ToolCalls: toolCalls, Usage: s.usage, Finish: s.finish}, nil
}
