package harness

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestOpenAIResponsesProviderWireAndStream(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.reasoning_summary_text.delta\",\"output_index\":0,\"delta\":\"thinking\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":1,\"delta\":\"answer\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.added\",\"output_index\":2,\"item\":{\"type\":\"function_call\",\"call_id\":\"call-new\",\"name\":\"unit_tool\",\"arguments\":\"\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":2,\"delta\":\"{\\\"value\\\":\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":2,\"delta\":\"\\\"ok\\\"}\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.done\",\"output_index\":2,\"item\":{\"type\":\"function_call\",\"call_id\":\"call-new\",\"name\":\"unit_tool\",\"arguments\":\"{\\\"value\\\":\\\"ok\\\"}\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":4,\"total_tokens\":14}}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	provider := NewOpenAIResponsesProvider("test", server.URL+"/v1", "test-key", "fallback-model")
	provider.modelSpec = piAIModel{ID: "test-model", Reasoning: true, Compat: piAIModelCompat{
		SupportsDeveloperRole: boolPointer(true), SupportsStrictMode: boolPointer(true),
	}}
	var deltas []Delta
	completion, err := provider.Complete(context.Background(), ChatRequest{
		Model: "test-model", System: "system prompt", ReasoningEffort: "high", Temperature: float64Pointer(0), MaxTokens: 1234,
		Messages: []ChatMessage{
			{Role: "user", Content: "use the tool", Images: []ChatImage{{MediaType: "image/png", Data: "aW1hZ2U="}}},
			{Role: "assistant", Content: "prior answer", ToolCalls: []ToolCall{{ID: "call-old", Name: "unit_tool", Arguments: json.RawMessage(`{"value":"old"}`)}}},
			{Role: "tool", ToolCallID: "call-old", Content: "old result"},
		},
		Tools: []ToolSchema{{Name: "unit_tool", Description: "unit test tool", Parameters: map[string]any{"type": "object"}}},
	}, func(delta Delta) error {
		deltas = append(deltas, delta)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if request["model"] != "test-model" || request["stream"] != true || request["store"] != false || request["temperature"] != float64(0) || request["max_output_tokens"] != float64(1234) {
		t.Fatalf("request = %#v", request)
	}
	if got := request["reasoning"]; !reflect.DeepEqual(got, map[string]any{"effort": "high", "summary": "auto"}) {
		t.Fatalf("reasoning = %#v", got)
	}
	if got := request["include"]; !reflect.DeepEqual(got, []any{"reasoning.encrypted_content"}) {
		t.Fatalf("include = %#v", got)
	}
	tools := request["tools"].([]any)
	if len(tools) != 1 || tools[0].(map[string]any)["name"] != "unit_tool" || tools[0].(map[string]any)["type"] != "function" || tools[0].(map[string]any)["strict"] != false {
		t.Fatalf("tools = %#v", tools)
	}
	input := request["input"].([]any)
	if len(input) != 5 || input[0].(map[string]any)["role"] != "developer" || input[2].(map[string]any)["type"] != "message" || input[3].(map[string]any)["type"] != "function_call" || input[4].(map[string]any)["type"] != "function_call_output" {
		t.Fatalf("input = %#v", input)
	}
	userContent := input[1].(map[string]any)["content"].([]any)
	if len(userContent) != 2 || userContent[0].(map[string]any)["text"] != "use the tool" || userContent[1].(map[string]any)["image_url"] != "data:image/png;base64,aW1hZ2U=" {
		t.Fatalf("user content = %#v", userContent)
	}

	if completion.Text != "answer" || completion.Reasoning != "thinking" || completion.Finish != "tool_calls" || len(completion.ToolCalls) != 1 {
		t.Fatalf("completion = %#v", completion)
	}
	call := completion.ToolCalls[0]
	if call.ID != "call-new" || call.Name != "unit_tool" || string(call.Arguments) != `{"value":"ok"}` {
		t.Fatalf("tool call = %#v", call)
	}
	if completion.Usage["input_tokens"] != float64(10) || completion.Usage["output_tokens"] != float64(4) || completion.Usage["total_tokens"] != float64(14) {
		t.Fatalf("usage = %#v", completion.Usage)
	}
	if len(deltas) != 5 || deltas[0].Reasoning != "thinking" || deltas[1].Text != "answer" || deltas[2].ToolCalls[0].ArgumentsDelta != `{"value":` || deltas[3].ToolCalls[0].ArgumentsDelta != `"ok"}` || deltas[4].Finish != "tool_calls" || deltas[4].Usage["input_tokens"] != float64(10) {
		t.Fatalf("deltas = %#v", deltas)
	}
}

func TestOpenAIResponsesCompatCanOmitMaxOutputTokens(t *testing.T) {
	disabled := false
	provider := NewOpenAIResponsesProvider("test", "https://example.test/v1", "key", "model")
	provider.modelSpec = piAIModel{ID: "model", Compat: piAIModelCompat{SupportsMaxOutputTokens: &disabled}}
	body := provider.requestBody(ChatRequest{MaxTokens: 1234})
	if _, exists := body["max_output_tokens"]; exists {
		t.Fatalf("request body retained disabled max_output_tokens: %#v", body)
	}
}

func TestAnthropicProviderWireAndStream(t *testing.T) {
	var request map[string]any
	var requestHeader http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/messages" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("X-Api-Key"); got != "test-key" {
			t.Errorf("X-Api-Key = %q", got)
		}
		if got := r.Header.Get("Anthropic-Version"); got != "2023-06-01" {
			t.Errorf("Anthropic-Version = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		requestHeader = r.Header.Clone()
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":7,\"cache_read_input_tokens\":2}}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"thinking\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"answer\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call-new\",\"name\":\"unit_tool\",\"input\":{}}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"value\\\":\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"ok\\\"}\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":5}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	provider := NewAnthropicProvider("test", server.URL, "test-key", "fallback-model")
	provider.modelSpec = piAIModel{ID: "test-model", Reasoning: true, Compat: piAIModelCompat{
		SupportsEagerToolInputStreaming: boolPointer(false), AllowEmptySignature: boolPointer(true),
	}}
	var deltas []Delta
	completion, err := provider.Complete(context.Background(), ChatRequest{
		Model: "test-model", System: "system prompt", ReasoningEffort: "high", MaxTokens: 1234,
		Messages: []ChatMessage{
			{Role: "user", Content: "use the tool", Images: []ChatImage{{MediaType: "image/png", Data: "aW1hZ2U="}}},
			{Role: "assistant", Reasoning: "prior thought", Content: "prior answer", ToolCalls: []ToolCall{{ID: "call-old", Name: "unit_tool", Arguments: json.RawMessage(`{"value":"old"}`)}}},
			{Role: "tool", ToolCallID: "call-old", Content: "old result"},
		},
		Tools: []ToolSchema{{Name: "unit_tool", Description: "unit test tool", Parameters: map[string]any{"type": "object"}}},
	}, func(delta Delta) error {
		deltas = append(deltas, delta)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if request["model"] != "test-model" || request["stream"] != true || request["max_tokens"] != float64(1234) {
		t.Fatalf("request = %#v", request)
	}
	if got := requestHeader.Get("Anthropic-Beta"); got != "fine-grained-tool-streaming-2025-05-14,interleaved-thinking-2025-05-14" {
		t.Fatalf("Anthropic-Beta = %q", got)
	}
	if got := request["thinking"]; !reflect.DeepEqual(got, map[string]any{"type": "enabled", "budget_tokens": float64(1024), "display": "summarized"}) {
		t.Fatalf("thinking = %#v", got)
	}
	system := request["system"].([]any)
	if len(system) != 1 || system[0].(map[string]any)["text"] != "system prompt" {
		t.Fatalf("system = %#v", system)
	}
	tools := request["tools"].([]any)
	if len(tools) != 1 || tools[0].(map[string]any)["name"] != "unit_tool" || tools[0].(map[string]any)["eager_input_streaming"] != nil || !reflect.DeepEqual(tools[0].(map[string]any)["input_schema"], map[string]any{"type": "object", "properties": map[string]any{}, "required": []any{}}) {
		t.Fatalf("tools = %#v", tools)
	}
	messages := request["messages"].([]any)
	if len(messages) != 3 || messages[0].(map[string]any)["role"] != "user" || messages[1].(map[string]any)["role"] != "assistant" || messages[2].(map[string]any)["role"] != "user" {
		t.Fatalf("messages = %#v", messages)
	}
	userContent := messages[0].(map[string]any)["content"].([]any)
	if len(userContent) != 2 || userContent[0].(map[string]any)["text"] != "use the tool" || userContent[1].(map[string]any)["source"].(map[string]any)["data"] != "aW1hZ2U=" {
		t.Fatalf("user content = %#v", userContent)
	}
	assistantContent := messages[1].(map[string]any)["content"].([]any)
	if len(assistantContent) != 3 || assistantContent[0].(map[string]any)["type"] != "thinking" || assistantContent[0].(map[string]any)["thinking"] != "prior thought" || assistantContent[0].(map[string]any)["signature"] != "" || assistantContent[2].(map[string]any)["type"] != "tool_use" {
		t.Fatalf("assistant content = %#v", assistantContent)
	}

	if completion.Text != "answer" || completion.Reasoning != "thinking" || completion.Finish != "tool_calls" || len(completion.ToolCalls) != 1 {
		t.Fatalf("completion = %#v", completion)
	}
	call := completion.ToolCalls[0]
	if call.ID != "call-new" || call.Name != "unit_tool" || string(call.Arguments) != `{"value":"ok"}` {
		t.Fatalf("tool call = %#v", call)
	}
	if completion.Usage["input_tokens"] != float64(7) || completion.Usage["cache_read_input_tokens"] != float64(2) || completion.Usage["output_tokens"] != float64(5) {
		t.Fatalf("usage = %#v", completion.Usage)
	}
	if len(deltas) != 7 || deltas[0].Usage["input_tokens"] != float64(7) || deltas[1].Reasoning != "thinking" || deltas[2].Text != "answer" || deltas[3].ToolCalls[0].ArgumentsDelta != `{"value":` || deltas[4].ToolCalls[0].ArgumentsDelta != `"ok"}` || deltas[5].Usage["output_tokens"] != float64(5) || deltas[6].Finish != "tool_calls" {
		t.Fatalf("deltas = %#v", deltas)
	}
}

func TestAnthropicTemperatureCompat(t *testing.T) {
	for _, test := range []struct {
		name        string
		reasoning   string
		supported   *bool
		wantPresent bool
	}{
		{name: "supported", wantPresent: true},
		{name: "thinking enabled", reasoning: "high"},
		{name: "unsupported", supported: boolPointer(false)},
	} {
		t.Run(test.name, func(t *testing.T) {
			var request map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n"))
				_, _ = w.Write([]byte("data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n"))
				_, _ = w.Write([]byte("data: {\"type\":\"message_stop\"}\n\n"))
			}))
			defer server.Close()

			provider := NewAnthropicProvider("anthropic", server.URL, "test-key", "claude-test")
			provider.modelSpec = piAIModel{ID: "claude-test", Reasoning: true, Compat: piAIModelCompat{SupportsTemperature: test.supported}}
			if _, err := provider.Complete(context.Background(), ChatRequest{
				Temperature: float64Pointer(0), ReasoningEffort: test.reasoning,
			}, func(Delta) error { return nil }); err != nil {
				t.Fatal(err)
			}
			_, present := request["temperature"]
			if present != test.wantPresent {
				t.Fatalf("temperature present = %v, request = %#v", present, request)
			}
		})
	}
}

func TestProviderProtocolStreamFailures(t *testing.T) {
	t.Run("OpenAI Responses closes before terminal event", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial\"}\n\n"))
		}))
		defer server.Close()

		_, err := NewOpenAIResponsesProvider("test", server.URL, "", "test-model").Complete(context.Background(), ChatRequest{}, func(Delta) error { return nil })
		var providerErr *ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "STREAM_CLOSED" || !strings.Contains(err.Error(), "terminal response event") {
			t.Fatalf("Complete() error = %#v", err)
		}
	})

	t.Run("Anthropic returns provider error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n"))
		}))
		defer server.Close()

		_, err := NewAnthropicProvider("test", server.URL, "", "test-model").Complete(context.Background(), ChatRequest{}, func(Delta) error { return nil })
		var providerErr *ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "PROVIDER" || !strings.Contains(err.Error(), "overloaded_error") {
			t.Fatalf("Complete() error = %#v", err)
		}
	})
}
