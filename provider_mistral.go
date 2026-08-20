package harness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type mistralProvider struct {
	id, baseURL, apiKey, model string
	client                     *http.Client
	headers                    map[string]string
	cacheRetention             string
	modelSpec                  piAIModel
	streamIdleTimeout          time.Duration
}

func newMistralProvider(id, baseURL, apiKey, model string) *mistralProvider {
	return &mistralProvider{
		id: id, baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, model: model,
		client: &http.Client{},
	}
}

func (p *mistralProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	key := firstNonBlank(p.apiKey, os.Getenv("MISTRAL_API_KEY"))
	if key == "" {
		return Completion{}, &ProviderError{Code: "MISSING_CREDENTIAL", Message: "Mistral requires MISTRAL_API_KEY or apiKeyEnv"}
	}
	body := p.requestBody(req)
	data, err := json.Marshal(body)
	if err != nil {
		return Completion{}, err
	}
	endpoint := mistralEndpoint(p.baseURL)
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return Completion{}, err
	}
	hreq.Header.Set("Accept", "text/event-stream")
	hreq.Header.Set("Content-Type", "application/json")
	for name, value := range p.headers {
		hreq.Header.Set(name, value)
	}
	if resolvedPiAICacheRetention(p.cacheRetention) != "none" && req.SessionID != "" && hreq.Header.Get("X-Affinity") == "" {
		hreq.Header.Set("X-Affinity", req.SessionID)
	}
	hreq.Header.Set("Authorization", "Bearer "+key)
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

	state := mistralStreamState{onDelta: onDelta, calls: map[int]*ToolCall{}, usage: map[string]any{}}
	err = scanProviderSSE(ctx, resp.Body, p.streamIdleTimeout, func(line string) error {
		if line == "" || !strings.HasPrefix(line, "data:") {
			return nil
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			return nil
		}
		if payload == "" {
			return nil
		}
		return state.handle([]byte(payload))
	})
	if err != nil {
		return Completion{}, err
	}
	return state.completion()
}

func mistralEndpoint(baseURL string) string {
	baseURL = strings.TrimRight(firstNonBlank(baseURL, "https://api.mistral.ai"), "/")
	if strings.HasSuffix(baseURL, "/chat/completions") {
		return baseURL
	}
	if strings.HasSuffix(baseURL, "/v1") {
		return baseURL + "/chat/completions"
	}
	return baseURL + "/v1/chat/completions"
}

func (p *mistralProvider) requestBody(req ChatRequest) map[string]any {
	model := firstNonBlank(req.Model, p.model)
	body := map[string]any{
		"model": model, "stream": true, "messages": mistralMessages(req),
	}
	if req.MaxTokens > 0 {
		body["max_tokens"] = req.MaxTokens
	}
	if req.Temperature != nil {
		body["temperature"] = *req.Temperature
	}
	if len(req.Tools) > 0 {
		tools := make([]any, 0, len(req.Tools))
		for _, tool := range req.Tools {
			tools = append(tools, map[string]any{"type": "function", "function": map[string]any{
				"name": tool.Name, "description": tool.Description, "parameters": tool.Parameters, "strict": false,
			}})
		}
		body["tools"] = tools
	}
	if resolvedPiAICacheRetention(p.cacheRetention) != "none" && req.SessionID != "" {
		body["prompt_cache_key"] = req.SessionID
	}
	if p.modelSpec.Reasoning && req.ReasoningEffort != "" && req.ReasoningEffort != "off" {
		switch model {
		case "mistral-small-2603", "mistral-small-latest", "mistral-medium-3.5":
			effort, ok := piAIReasoningWire(p.modelSpec, req.ReasoningEffort)
			if !ok {
				effort = "high"
			}
			body["reasoning_effort"] = effort
		default:
			body["prompt_mode"] = "reasoning"
		}
	}
	return body
}

func mistralMessages(req ChatRequest) []any {
	messages := make([]any, 0, len(req.Messages)+1)
	if req.System != "" {
		messages = append(messages, map[string]any{"role": "system", "content": req.System})
	}
	normalizedIDs := map[string]string{}
	for _, message := range req.Messages {
		switch message.Role {
		case "user", "system":
			if len(message.Images) == 0 {
				messages = append(messages, map[string]any{"role": message.Role, "content": message.Content})
				continue
			}
			content := make([]any, 0, len(message.Images)+1)
			if message.Content != "" {
				content = append(content, map[string]any{"type": "text", "text": message.Content})
			}
			for _, image := range message.Images {
				content = append(content, map[string]any{"type": "image_url", "image_url": "data:" + image.MediaType + ";base64," + image.Data})
			}
			messages = append(messages, map[string]any{"role": message.Role, "content": content})
		case "assistant":
			row := map[string]any{"role": "assistant"}
			content := make([]any, 0, 2)
			if message.Reasoning != "" {
				content = append(content, map[string]any{"type": "thinking", "thinking": []any{map[string]any{"type": "text", "text": message.Reasoning}}})
			}
			if message.Content != "" {
				content = append(content, map[string]any{"type": "text", "text": message.Content})
			}
			if len(content) == 1 && message.Reasoning == "" {
				row["content"] = message.Content
			} else if len(content) > 0 {
				row["content"] = content
			}
			if len(message.ToolCalls) > 0 {
				calls := make([]any, 0, len(message.ToolCalls))
				for _, call := range message.ToolCalls {
					id := mistralToolCallID(call.ID)
					normalizedIDs[call.ID] = id
					arguments := string(call.Arguments)
					if arguments == "" {
						arguments = "{}"
					}
					calls = append(calls, map[string]any{"id": id, "type": "function", "function": map[string]any{"name": call.Name, "arguments": arguments}})
				}
				row["tool_calls"] = calls
			}
			if len(row) > 1 {
				messages = append(messages, row)
			}
		case "tool":
			id := normalizedIDs[message.ToolCallID]
			if id == "" {
				id = mistralToolCallID(message.ToolCallID)
			}
			output := message.Content
			if output == "" {
				output = "(no output)"
			}
			messages = append(messages, map[string]any{"role": "tool", "tool_call_id": id, "content": output})
		}
	}
	return messages
}

func mistralToolCallID(id string) string {
	var normalized strings.Builder
	for _, r := range id {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			normalized.WriteRune(r)
		}
	}
	if normalized.Len() == 9 {
		return normalized.String()
	}
	digest := sha256.Sum256([]byte(firstNonBlank(normalized.String(), id, "tool-call")))
	return hex.EncodeToString(digest[:])[:9]
}

type mistralStreamState struct {
	onDelta   func(Delta) error
	text      string
	reasoning string
	finish    string
	usage     map[string]any
	calls     map[int]*ToolCall
	order     []int
}

func (s *mistralStreamState) call(index int) *ToolCall {
	if call := s.calls[index]; call != nil {
		return call
	}
	call := &ToolCall{}
	s.calls[index] = call
	s.order = append(s.order, index)
	return call
}

func (s *mistralStreamState) handle(payload []byte) error {
	var chunk map[string]any
	if err := json.Unmarshal(payload, &chunk); err != nil {
		return fmt.Errorf("malformed Mistral stream payload: %w", err)
	}
	if usage, ok := chunk["usage"].(map[string]any); ok {
		prompt := firstJSONInt(usage, "prompt_tokens", "promptTokens")
		cached := mistralCachedTokens(usage)
		output := firstJSONInt(usage, "completion_tokens", "completionTokens")
		total := firstJSONInt(usage, "total_tokens", "totalTokens")
		if total == 0 {
			total = prompt + output
		}
		s.usage = map[string]any{"input_tokens": max(0, prompt-cached), "output_tokens": output, "cache_read_tokens": cached, "total_tokens": total}
	}
	choices, _ := chunk["choices"].([]any)
	if len(choices) == 0 {
		return nil
	}
	choice, _ := choices[0].(map[string]any)
	switch stringSetting(choice["finish_reason"]) {
	case "stop", "":
		if stringSetting(choice["finish_reason"]) != "" {
			s.finish = "stop"
		}
	case "length", "model_length":
		s.finish = "length"
	case "tool_calls":
		s.finish = "tool_calls"
	case "error":
		return &ProviderError{Code: "PROVIDER", Message: "Mistral stream stopped with error"}
	}
	delta, _ := choice["delta"].(map[string]any)
	if err := s.handleContent(delta["content"]); err != nil {
		return err
	}
	calls, _ := delta["tool_calls"].([]any)
	if len(calls) == 0 {
		calls, _ = delta["toolCalls"].([]any)
	}
	for _, raw := range calls {
		item, _ := raw.(map[string]any)
		index := jsonInt(item["index"])
		call := s.call(index)
		if id := stringSetting(item["id"]); id != "" && id != "null" {
			call.ID = id
		}
		function, _ := item["function"].(map[string]any)
		if name := stringSetting(function["name"]); name != "" {
			call.Name = name
		}
		arguments := stringSetting(function["arguments"])
		if arguments == "" && function["arguments"] != nil {
			encoded, _ := json.Marshal(function["arguments"])
			arguments = string(encoded)
		}
		call.Arguments = append(call.Arguments, arguments...)
		if err := s.onDelta(Delta{ToolCalls: []ToolCallDelta{{Index: index, ID: call.ID, Name: call.Name, ArgumentsDelta: arguments}}}); err != nil {
			return err
		}
	}
	return nil
}

func (s *mistralStreamState) handleContent(value any) error {
	switch content := value.(type) {
	case string:
		if content != "" {
			s.text += content
			return s.onDelta(Delta{Text: content})
		}
	case []any:
		for _, raw := range content {
			item, _ := raw.(map[string]any)
			switch stringSetting(item["type"]) {
			case "text":
				text := stringSetting(item["text"])
				if text != "" {
					s.text += text
					if err := s.onDelta(Delta{Text: text}); err != nil {
						return err
					}
				}
			case "thinking":
				parts, _ := item["thinking"].([]any)
				for _, part := range parts {
					row, _ := part.(map[string]any)
					text := stringSetting(row["text"])
					if text != "" {
						s.reasoning += text
						if err := s.onDelta(Delta{Reasoning: text}); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func (s *mistralStreamState) completion() (Completion, error) {
	toolCalls := make([]ToolCall, 0, len(s.order))
	for _, index := range s.order {
		call := s.calls[index]
		if call == nil {
			continue
		}
		if call.ID == "" {
			call.ID = mistralToolCallID(fmt.Sprintf("toolcall:%d", index))
		}
		if len(call.Arguments) == 0 {
			call.Arguments = json.RawMessage("{}")
		}
		toolCalls = append(toolCalls, *call)
	}
	if len(toolCalls) > 0 {
		s.finish = "tool_calls"
	}
	if s.finish == "" {
		s.finish = "stop"
	}
	if err := s.onDelta(Delta{Finish: s.finish}); err != nil {
		return Completion{}, err
	}
	if s.text == "" && s.reasoning == "" && len(toolCalls) == 0 {
		return Completion{}, &ProviderError{Code: "EMPTY_RESPONSE", Message: "model returned a completed response with no content"}
	}
	return Completion{Text: s.text, Reasoning: s.reasoning, ToolCalls: toolCalls, Usage: s.usage, Finish: s.finish}, nil
}

func firstJSONInt(value map[string]any, keys ...string) int {
	for _, key := range keys {
		if parsed := jsonInt(value[key]); parsed != 0 {
			return parsed
		}
	}
	return 0
}

func mistralCachedTokens(usage map[string]any) int {
	for _, key := range []string{"prompt_tokens_details", "promptTokensDetails", "prompt_token_details", "promptTokenDetails"} {
		if details, ok := usage[key].(map[string]any); ok {
			if cached := firstJSONInt(details, "cached_tokens", "cachedTokens"); cached > 0 {
				return cached
			}
		}
	}
	return firstJSONInt(usage, "num_cached_tokens", "numCachedTokens")
}
