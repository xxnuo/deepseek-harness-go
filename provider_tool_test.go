package harness

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestOpenAIProviderFunctionCallingWireAndStream(t *testing.T) {
	t.Helper()
	var request struct {
		Model           string              `json:"model"`
		Messages        []openAIWireMessage `json:"messages"`
		Tools           []openAIWireTool    `json:"tools"`
		Thinking        *openAIWireThinking `json:"thinking"`
		ReasoningEffort string              `json:"reasoning_effort"`
		MaxTokens       int                 `json:"max_tokens"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("request path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("Authorization = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-2\",\"type\":\"function\",\"function\":{\"name\":\"unit_tool\",\"arguments\":\"{\\\"value\\\":\"}},{\"index\":1,\"id\":\"call-3\",\"type\":\"function\",\"function\":{\"name\":\"other_tool\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"ok\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":4}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	provider := NewOpenAIProvider("test", server.URL, "test-key", "fallback-model")
	var deltas []Delta
	completion, err := provider.Complete(context.Background(), ChatRequest{
		Model: "test-model", System: "system prompt", Thinking: "enabled", ReasoningEffort: "high", MaxTokens: 1234,
		Messages: []ChatMessage{
			{Role: "user", Content: "use the tool"},
			{Role: "assistant", Reasoning: "prior thought", ToolCalls: []ToolCall{{ID: "call-1", Name: "unit_tool", Arguments: json.RawMessage(`{"value":"first"}`)}}},
			{Role: "tool", ToolCallID: "call-1", Content: "first result"},
		},
		Tools: []ToolSchema{{Name: "unit_tool", Description: "unit test tool", Parameters: map[string]any{"type": "object"}}},
	}, func(delta Delta) error {
		deltas = append(deltas, delta)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if request.Model != "test-model" || len(request.Tools) != 1 || request.Thinking == nil || request.Thinking.Type != "enabled" || request.ReasoningEffort != "high" || request.MaxTokens != 1234 {
		t.Fatalf("request = %#v", request)
	}
	tool := request.Tools[0]
	if tool.Type != "function" || tool.Function.Name != "unit_tool" || tool.Function.Description != "unit test tool" || !reflect.DeepEqual(tool.Function.Parameters, map[string]any{"type": "object"}) {
		t.Fatalf("wire tool = %#v", tool)
	}
	if len(request.Messages) != 4 || request.Messages[0].Role != "system" || request.Messages[0].Content != "system prompt" {
		t.Fatalf("wire messages = %#v", request.Messages)
	}
	assistant := request.Messages[2]
	if assistant.Role != "assistant" || assistant.Content != "" || assistant.ReasoningContent != "prior thought" || len(assistant.ToolCalls) != 1 {
		t.Fatalf("assistant wire message = %#v", assistant)
	}
	call := assistant.ToolCalls[0]
	if call.ID != "call-1" || call.Type != "function" || call.Function.Name != "unit_tool" || call.Function.Arguments != `{"value":"first"}` {
		t.Fatalf("assistant wire tool call = %#v", call)
	}
	toolResult := request.Messages[3]
	if toolResult.Role != "tool" || toolResult.ToolCallID != "call-1" || toolResult.Content != "first result" {
		t.Fatalf("tool wire message = %#v", toolResult)
	}

	if completion.Finish != "tool_calls" || completion.Reasoning != "thinking" || len(completion.ToolCalls) != 2 || completion.ToolCalls[0].ID != "call-2" || completion.ToolCalls[0].Name != "unit_tool" || string(completion.ToolCalls[0].Arguments) != `{"value":"ok"}` {
		t.Fatalf("completion = %#v", completion)
	}
	if completion.ToolCalls[1].ID != "call-3" || completion.ToolCalls[1].Name != "other_tool" || string(completion.ToolCalls[1].Arguments) != `{}` {
		t.Fatalf("second completion tool call = %#v", completion.ToolCalls[1])
	}
	if completion.Usage["prompt_tokens"] != float64(10) || completion.Usage["completion_tokens"] != float64(4) {
		t.Fatalf("usage = %#v", completion.Usage)
	}
	if len(deltas) != 3 || deltas[2].Finish != "tool_calls" {
		t.Fatalf("deltas = %#v", deltas)
	}
}

func TestOpenAIProviderModelDiscoveryKeepsGatewayMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Fatalf("request path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[{"id":"large","display_name":"Large","context_length":65536,"max_output_tokens":4096},{"id":"large"},{"id":"small"},{"name":"missing-id"},null]}`)
	}))
	defer server.Close()
	models, err := NewOpenAIProvider("draft", server.URL+"/v1", "", "").Models(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 2 {
		t.Fatalf("models = %#v", models)
	}
	if models[0].ID != "large" || models[0].Name != "Large" || models[0].ContextWindow != 65536 || models[0].MaxTokens != 4096 {
		t.Fatalf("large metadata = %#v", models[0])
	}
	if models[1].ID != "small" || models[1].Name != "small" {
		t.Fatalf("small metadata = %#v", models[1])
	}
}

func TestOpenAIProviderOmitsReasoningWithoutToolCalls(t *testing.T) {
	messages := openAIWireMessages([]ChatMessage{{Role: "assistant", Content: "answer", Reasoning: "private"}})
	if len(messages) != 1 || messages[0].ReasoningContent != "" {
		t.Fatalf("wire messages = %#v", messages)
	}
}

func TestOpenAIProviderRejectsInvalidStream(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "malformed payload", body: "data: {not-json}\n\ndata: [DONE]\n\n", want: "malformed SSE payload"},
		{name: "missing done", body: "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"unit_tool\",\"arguments\":\"{}\"}}]}}]}\n\n", want: "without [DONE]"},
		{name: "unsupported finish", body: "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"content_filter\"}]}\n\ndata: [DONE]\n\n", want: "model stopped: content_filter"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			provider := NewOpenAIProvider("test", server.URL, "test-key", "test-model")
			_, err := provider.Complete(context.Background(), ChatRequest{}, func(Delta) error { return nil })
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Complete() error = %v, want %q", err, test.want)
			}
		})
	}
}
