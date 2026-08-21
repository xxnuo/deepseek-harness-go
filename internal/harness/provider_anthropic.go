package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type AnthropicProvider struct {
	id, baseURL, apiKey, model string
	client                     *http.Client
	headers                    map[string]string
	modelSpec                  piAIModel
	cacheRetention             string
	thinkingBudgets            map[string]int
	streamIdleTimeout          time.Duration
	oauth                      bool
}

func NewAnthropicProvider(id, baseURL, apiKey, model string) *AnthropicProvider {
	if id == "" {
		id = "anthropic"
	}
	if baseURL == "" {
		baseURL = "https://api.anthropic.com"
	}
	if model == "" {
		model = "claude-sonnet-4-5"
	}
	return &AnthropicProvider{
		id: id, baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, model: model,
		client: &http.Client{},
	}
}

func (p *AnthropicProvider) ID() string   { return p.id }
func (p *AnthropicProvider) Name() string { return p.id }
func (p *AnthropicProvider) Models(ctx context.Context) ([]ModelInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []ModelInfo{{ID: p.model, Name: p.model, InputModalities: []string{"text", "image"}}}, nil
}

func anthropicCacheControl(retention string, model piAIModel) map[string]any {
	retention = resolvedPiAICacheRetention(retention)
	if retention == "none" {
		return nil
	}
	cache := map[string]any{"type": "ephemeral"}
	supportsLong := model.Compat.SupportsLongCacheRetention == nil || *model.Compat.SupportsLongCacheRetention
	if retention == "long" && supportsLong {
		cache["ttl"] = "1h"
	}
	return cache
}

func anthropicMessages(messages []ChatMessage, cacheControl map[string]any, allowEmptySignature bool) []map[string]any {
	out := make([]map[string]any, 0, len(messages))
	appendMessage := func(role string, blocks []any) {
		if len(blocks) == 0 {
			return
		}
		if len(out) > 0 && out[len(out)-1]["role"] == role {
			out[len(out)-1]["content"] = append(out[len(out)-1]["content"].([]any), blocks...)
			return
		}
		out = append(out, map[string]any{"role": role, "content": blocks})
	}
	for _, message := range messages {
		switch message.Role {
		case "user":
			blocks := make([]any, 0, len(chatContentParts(message)))
			for _, part := range chatContentParts(message) {
				if part.Type == "text" && strings.TrimSpace(part.Text) != "" {
					blocks = append(blocks, map[string]any{"type": "text", "text": part.Text})
				} else if part.Type == "image" {
					blocks = append(blocks, map[string]any{
						"type": "image", "source": map[string]any{
							"type": "base64", "media_type": part.MediaType, "data": part.Data,
						},
					})
				}
			}
			appendMessage("user", blocks)
		case "assistant":
			blocks := make([]any, 0, len(message.ToolCalls)+2)
			if message.Reasoning != "" {
				if strings.TrimSpace(message.ReasoningSignature) != "" {
					blocks = append(blocks, map[string]any{"type": "thinking", "thinking": message.Reasoning, "signature": message.ReasoningSignature})
				} else if allowEmptySignature {
					blocks = append(blocks, map[string]any{"type": "thinking", "thinking": message.Reasoning, "signature": ""})
				} else {
					blocks = append(blocks, map[string]any{"type": "text", "text": message.Reasoning})
				}
			}
			if strings.TrimSpace(message.Content) != "" {
				blocks = append(blocks, map[string]any{"type": "text", "text": message.Content})
			}
			for _, call := range message.ToolCalls {
				input := map[string]any{}
				if len(call.Arguments) > 0 {
					_ = json.Unmarshal(call.Arguments, &input)
				}
				blocks = append(blocks, map[string]any{"type": "tool_use", "id": call.ID, "name": call.Name, "input": input})
			}
			appendMessage("assistant", blocks)
		case "tool":
			var content any = message.Content
			if chatMessageHasImage(message) {
				blocks := make([]any, 0, len(chatContentParts(message))+1)
				hasText := false
				for _, part := range chatContentParts(message) {
					if part.Type == "text" && strings.TrimSpace(part.Text) != "" {
						hasText = true
						blocks = append(blocks, map[string]any{"type": "text", "text": part.Text})
					} else if part.Type == "image" {
						blocks = append(blocks, map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": part.MediaType, "data": part.Data}})
					}
				}
				if !hasText {
					blocks = append([]any{map[string]any{"type": "text", "text": "(see attached image)"}}, blocks...)
				}
				content = blocks
			} else if content == "" {
				content = "(no output)"
			}
			appendMessage("user", []any{map[string]any{
				"type": "tool_result", "tool_use_id": message.ToolCallID, "content": content,
			}})
		}
	}
	if cacheControl != nil && len(out) > 0 && out[len(out)-1]["role"] == "user" {
		blocks := out[len(out)-1]["content"].([]any)
		if len(blocks) > 0 {
			if last, ok := blocks[len(blocks)-1].(map[string]any); ok {
				last["cache_control"] = cacheControl
			}
		}
	}
	return out
}

func anthropicInputSchema(tool ToolSchema, strict bool) map[string]any {
	legacy := map[string]any{"type": "object", "properties": map[string]any{}, "required": []any{}}
	if value, ok := tool.Parameters["properties"]; ok {
		legacy["properties"] = value
	}
	if value, ok := tool.Parameters["required"]; ok {
		legacy["required"] = value
	}
	if !strict {
		return legacy
	}
	result := make(map[string]any, len(tool.Parameters)+3)
	for key, value := range tool.Parameters {
		result[key] = value
	}
	for key, value := range legacy {
		result[key] = value
	}
	return result
}

func anthropicAdaptiveEffort(model piAIModel, level string) string {
	if effort, ok := piAIReasoningWire(model, level); ok {
		return effort
	}
	switch level {
	case "minimal", "low":
		return "low"
	case "medium":
		return "medium"
	case "max", "xhigh":
		return level
	default:
		return "high"
	}
}

func (p *AnthropicProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	model := req.Model
	if model == "" {
		model = p.model
	}
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}
	cacheControl := anthropicCacheControl(p.cacheRetention, p.modelSpec)
	allowEmptySignature := p.modelSpec.Compat.AllowEmptySignature != nil && *p.modelSpec.Compat.AllowEmptySignature
	supportsEagerToolInput := p.modelSpec.Compat.SupportsEagerToolInputStreaming == nil || *p.modelSpec.Compat.SupportsEagerToolInputStreaming
	supportsStrictTools := p.modelSpec.Compat.SupportsStrictTools != nil && *p.modelSpec.Compat.SupportsStrictTools
	if err := validateToolSampling(req.Tools, supportsStrictTools); err != nil {
		return Completion{}, err
	}
	body := map[string]any{
		"model": model, "messages": anthropicMessages(req.Messages, cacheControl, allowEmptySignature),
		"max_tokens": maxTokens, "stream": true,
	}
	if p.oauth {
		blocks := []any{map[string]any{"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude."}}
		if req.System != "" {
			blocks = append(blocks, map[string]any{"type": "text", "text": req.System})
		}
		if cacheControl != nil {
			for _, raw := range blocks {
				raw.(map[string]any)["cache_control"] = cacheControl
			}
		}
		body["system"] = blocks
	} else if req.System != "" {
		block := map[string]any{"type": "text", "text": req.System}
		if cacheControl != nil {
			block["cache_control"] = cacheControl
		}
		body["system"] = []any{block}
	}
	if len(req.Tools) > 0 {
		tools := make([]any, 0, len(req.Tools))
		for _, schema := range req.Tools {
			strict, requested, _ := resolveJSONSchemaStrictSampling(schema, supportsStrictTools)
			tool := map[string]any{
				"name": schema.Name, "description": schema.Description, "input_schema": anthropicInputSchema(schema, strict),
			}
			if requested {
				tool["strict"] = true
			}
			if supportsEagerToolInput {
				tool["eager_input_streaming"] = true
			}
			tools = append(tools, tool)
		}
		supportsToolCache := p.modelSpec.Compat.SupportsCacheControlOnTools == nil || *p.modelSpec.Compat.SupportsCacheControlOnTools
		if cacheControl != nil && supportsToolCache {
			tools[len(tools)-1].(map[string]any)["cache_control"] = cacheControl
		}
		body["tools"] = tools
	}
	thinkingEnabled := false
	if p.modelSpec.ID == "" {
		if req.ReasoningEffort == "off" || req.Thinking == "disabled" {
			body["thinking"] = map[string]any{"type": "disabled"}
		} else if req.ReasoningEffort != "" || req.Thinking == "enabled" {
			thinkingEnabled = true
			budget := p.thinkingBudgets[req.ReasoningEffort]
			if budget <= 0 {
				budget = 1024
			}
			body["thinking"] = map[string]any{"type": "enabled", "budget_tokens": budget}
		}
	} else if p.modelSpec.Reasoning {
		enabled := req.ReasoningEffort != "" && req.ReasoningEffort != "off" || req.Thinking == "enabled"
		thinkingEnabled = enabled
		if enabled && p.modelSpec.Compat.ForceAdaptiveThinking != nil && *p.modelSpec.Compat.ForceAdaptiveThinking {
			body["thinking"] = map[string]any{"type": "adaptive", "display": "summarized"}
			body["output_config"] = map[string]any{"effort": anthropicAdaptiveEffort(p.modelSpec, req.ReasoningEffort)}
		} else if enabled {
			budget := p.thinkingBudgets[req.ReasoningEffort]
			if budget <= 0 {
				budget = 1024
			}
			body["thinking"] = map[string]any{"type": "enabled", "budget_tokens": budget, "display": "summarized"}
		} else if off, exists := p.modelSpec.ThinkingLevelMap["off"]; !exists || off != nil {
			body["thinking"] = map[string]any{"type": "disabled"}
		}
	}
	if req.Temperature != nil && !thinkingEnabled && (p.modelSpec.Compat.SupportsTemperature == nil || *p.modelSpec.Compat.SupportsTemperature) {
		body["temperature"] = *req.Temperature
	}
	data, err := json.Marshal(body)
	if err != nil {
		return Completion{}, err
	}
	endpoint := p.baseURL + "/v1/messages"
	if strings.HasSuffix(p.baseURL, "/v1") {
		endpoint = p.baseURL + "/messages"
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return Completion{}, err
	}
	if req.SessionID != "" && resolvedPiAICacheRetention(p.cacheRetention) != "none" && p.modelSpec.Compat.SendSessionAffinityHeaders != nil && *p.modelSpec.Compat.SendSessionAffinityHeaders {
		hreq.Header.Set("X-Session-Affinity", req.SessionID)
	}
	betas := make([]string, 0, 4)
	if p.oauth {
		betas = append(betas, "claude-code-20250219", "oauth-2025-04-20")
	}
	if len(req.Tools) > 0 && !supportsEagerToolInput {
		betas = append(betas, "fine-grained-tool-streaming-2025-05-14")
	}
	if p.modelSpec.Compat.ForceAdaptiveThinking == nil || !*p.modelSpec.Compat.ForceAdaptiveThinking {
		betas = append(betas, "interleaved-thinking-2025-05-14")
	}
	if len(betas) > 0 {
		hreq.Header.Set("Anthropic-Beta", strings.Join(betas, ","))
	}
	for name, value := range p.headers {
		hreq.Header.Set(name, value)
	}
	hreq.Header.Set("Accept", "application/json")
	hreq.Header.Set("Anthropic-Dangerous-Direct-Browser-Access", "true")
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Anthropic-Version", "2023-06-01")
	if p.apiKey != "" {
		hreq.Header.Set("X-Api-Key", p.apiKey)
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
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return Completion{}, providerHTTPFailure(resp, string(payload))
	}

	var text, reasoning, reasoningSignature, finish string
	usage := map[string]any{}
	calls := map[int]*ToolCall{}
	order := make([]int, 0)
	terminal := false
	err = scanProviderSSE(ctx, resp.Body, p.streamIdleTimeout, func(line string) error {
		if line == "" || !strings.HasPrefix(line, "data:") {
			return nil
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			return nil
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			return fmt.Errorf("malformed Anthropic SSE payload: %w", err)
		}
		typeName := stringSetting(event["type"])
		index := jsonInt(event["index"])
		switch typeName {
		case "message_start":
			if message, ok := event["message"].(map[string]any); ok {
				if value, ok := message["usage"].(map[string]any); ok {
					for key, raw := range value {
						usage[key] = raw
					}
					if err := onDelta(Delta{Usage: cloneStringMap(usage)}); err != nil {
						return err
					}
				}
			}
		case "content_block_start":
			block, _ := event["content_block"].(map[string]any)
			if block["type"] == "tool_use" {
				call := &ToolCall{ID: stringSetting(block["id"]), Name: stringSetting(block["name"])}
				calls[index] = call
				order = append(order, index)
			}
		case "content_block_delta":
			delta, _ := event["delta"].(map[string]any)
			switch delta["type"] {
			case "text_delta":
				piece := stringSetting(delta["text"])
				text += piece
				if piece != "" {
					return onDelta(Delta{Text: piece})
				}
			case "thinking_delta":
				piece := stringSetting(delta["thinking"])
				reasoning += piece
				if piece != "" {
					return onDelta(Delta{Reasoning: piece})
				}
			case "signature_delta":
				piece := stringSetting(delta["signature"])
				reasoningSignature += piece
				if piece != "" {
					return onDelta(Delta{ReasoningSignature: piece})
				}
			case "input_json_delta":
				call := calls[index]
				if call == nil {
					call = &ToolCall{}
					calls[index] = call
					order = append(order, index)
				}
				piece := stringSetting(delta["partial_json"])
				call.Arguments = append(call.Arguments, piece...)
				return onDelta(Delta{ToolCalls: []ToolCallDelta{{Index: index, ID: call.ID, Name: call.Name, ArgumentsDelta: piece}}})
			}
		case "message_delta":
			delta, _ := event["delta"].(map[string]any)
			switch stringSetting(delta["stop_reason"]) {
			case "tool_use":
				finish = "tool_calls"
			case "max_tokens", "model_context_window_exceeded":
				finish = "length"
			case "end_turn", "stop_sequence", "pause_turn", "refusal", "":
				finish = "stop"
			default:
				return &ProviderError{Code: "PROVIDER", Message: "unsupported Anthropic stop reason: " + stringSetting(delta["stop_reason"])}
			}
			if value, ok := event["usage"].(map[string]any); ok {
				for key, raw := range value {
					usage[key] = raw
				}
				if err := onDelta(Delta{Usage: cloneStringMap(usage)}); err != nil {
					return err
				}
			}
		case "message_stop":
			terminal = true
		case "error":
			return &ProviderError{Code: "PROVIDER", Message: "Anthropic stream failed: " + payload}
		}
		return nil
	})
	if err != nil {
		return Completion{}, err
	}
	if !terminal {
		return Completion{}, &ProviderError{Code: "STREAM_CLOSED", Message: "Anthropic stream ended before message_stop"}
	}
	toolCalls := make([]ToolCall, 0, len(order))
	for _, index := range order {
		call := calls[index]
		if call == nil {
			continue
		}
		if len(call.Arguments) == 0 {
			call.Arguments = json.RawMessage("{}")
		}
		toolCalls = append(toolCalls, *call)
	}
	if len(toolCalls) > 0 && finish == "stop" {
		finish = "tool_calls"
	}
	if finish == "" {
		finish = "stop"
	}
	if err := onDelta(Delta{Finish: finish}); err != nil {
		return Completion{}, err
	}
	if text == "" && reasoning == "" && len(toolCalls) == 0 {
		return Completion{}, &ProviderError{Code: "EMPTY_RESPONSE", Message: "model returned a completed response with no content"}
	}
	return Completion{Text: text, Reasoning: reasoning, ReasoningSignature: reasoningSignature, ToolCalls: toolCalls, Usage: usage, Finish: finish}, nil
}

func jsonInt(value any) int {
	number, ok := numericSetting(value)
	if !ok {
		return 0
	}
	return int(number)
}
