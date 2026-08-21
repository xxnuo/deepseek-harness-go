package harness

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
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
	if assistant.Role != "assistant" || assistant.Content != "" || assistant.ReasoningContent == nil || *assistant.ReasoningContent != "prior thought" || len(assistant.ToolCalls) != 1 {
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
	if len(deltas) != 4 || deltas[2].Finish != "tool_calls" || deltas[3].Usage["prompt_tokens"] != float64(10) {
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

func TestOpenAIProviderPassesBackReasoningWithoutToolCalls(t *testing.T) {
	messages := openAIWireMessages([]ChatMessage{{Role: "assistant", Content: "answer", Reasoning: "private"}})
	if len(messages) != 1 || messages[0].ReasoningContent == nil || *messages[0].ReasoningContent != "private" {
		t.Fatalf("wire messages = %#v", messages)
	}
}

func TestOpenAICompletionsDetectedCompatWire(t *testing.T) {
	deepseek := resolveOpenAICompletionsCompat("deepseek", "https://api.deepseek.com", piAIModel{Reasoning: true})
	message := openAIWireMessages([]ChatMessage{{Role: "assistant", Content: "answer"}}, deepseek)[0]
	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if value, exists := wire["reasoning_content"]; !exists || value != "" {
		t.Fatalf("DeepSeek reasoning_content = %#v", wire)
	}

	openrouter := resolveOpenAICompletionsCompat("openrouter", "https://openrouter.ai/api/v1", piAIModel{ID: "anthropic/test", Reasoning: true})
	if !openrouter.supportsDeveloperRole {
		t.Fatal("OpenRouter Anthropic model did not enable developer role")
	}
	thinking := openAIWireMessages([]ChatMessage{{Role: "assistant", Reasoning: "thought", Content: "answer"}}, openAICompletionsCompat{requiresThinkingAsText: true})[0]
	parts, ok := thinking.Content.([]map[string]any)
	if !ok || len(parts) != 2 || parts[0]["text"] != "thought" || parts[1]["text"] != "answer" || thinking.ReasoningContent != nil {
		t.Fatalf("thinking-as-text = %#v", thinking)
	}
}

func TestProviderHTTP413IsInvalidRequest(t *testing.T) {
	err := providerHTTPFailure(&http.Response{StatusCode: http.StatusRequestEntityTooLarge, Header: make(http.Header)}, "too large")
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "INVALID_REQUEST" {
		t.Fatalf("error = %#v", err)
	}
}

func TestOffloadRequestImagesDropsOldestOccurrences(t *testing.T) {
	original := []ChatMessage{
		{Role: "user", Content: "first", Images: []ChatImage{{MediaType: "image/png", Data: "AAAA"}, {MediaType: "image/png", Data: "BBBB"}}},
		{Role: "user", Content: "latest", Images: []ChatImage{{MediaType: "image/png", Data: "CCCC"}}},
	}
	offloaded := offloadRequestImages(original, 5)
	if len(offloaded[0].Images) != 0 || len(offloaded[1].Images) != 1 || offloaded[1].Images[0].Data != "CCCC" {
		t.Fatalf("offloaded images = %#v", offloaded)
	}
	if strings.Count(offloaded[0].Content, OffloadedImageText) != 2 {
		t.Fatalf("oldest replacements = %q", offloaded[0].Content)
	}
	if len(original[0].Images) != 2 || original[0].Content != "first" {
		t.Fatalf("durable request was mutated: %#v", original)
	}
}

func TestOffloadDurableImagesUsesMetadataBeforeHydration(t *testing.T) {
	oldRef := ImageAttachmentRef{AttachmentID: "sha256:" + strings.Repeat("a", 64), MediaType: "image/png", Bytes: 4}
	newRef := ImageAttachmentRef{AttachmentID: "sha256:" + strings.Repeat("b", 64), MediaType: "image/png", Bytes: 4}
	original := []ChatMessage{
		{Role: "user", Blocks: []ContentBlock{{Type: "text", Text: "old"}, {Type: "image", Attachment: &oldRef}}, Content: "old"},
		{Role: "assistant", Content: "keep assistant"},
		{Role: "user", Blocks: []ContentBlock{{Type: "text", Text: "new"}, {Type: "image", Attachment: &newRef}}, Content: "new"},
	}
	offloaded := offloadDurableMessageImages(original, 8)
	if len(offloaded[0].Blocks) != 2 || offloaded[0].Blocks[1].Type != "text" || offloaded[0].Blocks[1].Text != OffloadedImageText {
		t.Fatalf("old durable image = %#v", offloaded[0])
	}
	if offloaded[1].Content != "keep assistant" {
		t.Fatalf("unrelated assistant changed: %#v", offloaded[1])
	}
	if offloaded[2].Blocks[1].Attachment == nil || offloaded[2].Blocks[1].Attachment.AttachmentID != newRef.AttachmentID {
		t.Fatalf("new durable image = %#v", offloaded[2])
	}
	if original[0].Blocks[1].Attachment == nil {
		t.Fatal("durable source was mutated")
	}
}

func TestManagedDeepSeekVisionUsesModelCapabilityAndImageBudget(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "vision-key")
	var request struct {
		Messages []openAIWireMessage `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer vision-key" {
			t.Fatalf("Authorization = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.BaseURL, cfg.Persist = t.TempDir(), t.TempDir(), server.URL, false
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	engine.mu.Lock()
	engine.settings["llm-deepseek"] = map[string]any{
		"baseURL": server.URL, "maxRequestImageBytes": float64(4),
		"models": []any{map[string]any{"id": "vision", "inputModalities": []any{"text", "image"}}},
	}
	engine.mu.Unlock()

	provider := &managedDeepSeekProvider{engine: engine}
	completion, err := provider.Complete(context.Background(), ChatRequest{
		Model: "vision",
		Messages: []ChatMessage{
			{Role: "user", Content: "old", Images: []ChatImage{{MediaType: "image/png", Data: "AAAA"}}},
			{Role: "user", Content: "new", Images: []ChatImage{{MediaType: "image/png", Data: "BBBB"}}},
		},
	}, func(Delta) error { return nil })
	if err != nil || completion.Text != "ok" {
		t.Fatalf("Complete = %#v, %v", completion, err)
	}
	if len(request.Messages) != 2 {
		t.Fatalf("wire messages = %#v", request.Messages)
	}
	first, ok := request.Messages[0].Content.(string)
	if !ok || !strings.Contains(first, OffloadedImageText) {
		t.Fatalf("old image was not replaced: %#v", request.Messages[0].Content)
	}
	encoded, err := json.Marshal(request.Messages[1].Content)
	if err != nil {
		t.Fatal(err)
	}
	var parts []map[string]any
	if err := json.Unmarshal(encoded, &parts); err != nil || len(parts) != 2 {
		t.Fatalf("new image content = %#v", request.Messages[1].Content)
	}
	imageURL, _ := parts[1]["image_url"].(map[string]any)
	if imageURL["url"] != "data:image/png;base64,BBBB" {
		t.Fatalf("new image URL = %#v", imageURL)
	}
}

func TestManagedDeepSeekRejectsImagesForTextOnlyModelBeforeCredential(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "")
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	_, err = (&managedDeepSeekProvider{engine: engine}).Complete(context.Background(), ChatRequest{
		Model:    "deepseek-v4-flash",
		Messages: []ChatMessage{{Role: "user", Content: OffloadedImageText, HadImages: true}},
	}, func(Delta) error { return nil })
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "UNSUPPORTED_CONTENT" {
		t.Fatalf("error = %#v", err)
	}
}

func TestManagedDeepSeekAppliesFractionalStreamIdleTimeout(t *testing.T) {
	e := newIntegrationEngine(t)
	if _, rpcErr := e.settingsUpdate("llm-deepseek", map[string]any{"streamIdleTimeoutMs": 12.5}, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	provider, _, err := (&managedDeepSeekProvider{engine: e}).snapshot(deepSeekEffectiveSettings(e))
	if err != nil {
		t.Fatal(err)
	}
	if provider.streamIdleTimeout != 12*time.Millisecond+500*time.Microsecond {
		t.Fatalf("stream idle timeout = %s", provider.streamIdleTimeout)
	}
	if _, rpcErr := e.settingsUpdate("llm-deepseek", map[string]any{"streamIdleTimeoutMs": piAIMaxTimerMillis + 1}, nil, false); rpcErr == nil {
		t.Fatal("oversized stream idle timeout was accepted")
	}
}

func TestDeepSeekImageMessagesKeepToolContentTextual(t *testing.T) {
	messages, err := deepSeekImageMessages([]ChatMessage{
		{Role: "tool", ToolCallID: "one", Images: []ChatImage{{MediaType: "image/png", Data: "AAAA"}}},
		{Role: "tool", ToolCallID: "two", Content: "text", Images: []ChatImage{{MediaType: "image/jpeg", Data: "BBBB"}}},
		{Role: "assistant", Content: "after"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 4 || messages[0].Role != "tool" || messages[0].Content != "(see attached image)" || len(messages[0].Images) != 0 {
		t.Fatalf("first tool message = %#v", messages)
	}
	if messages[1].Role != "tool" || messages[1].Content != "text" || len(messages[1].Images) != 0 {
		t.Fatalf("second tool message = %#v", messages[1])
	}
	if messages[2].Role != "user" || messages[2].Content != "Attached image(s) from tool result:" || len(messages[2].Images) != 2 {
		t.Fatalf("tool image message = %#v", messages[2])
	}
	if messages[3].Role != "assistant" || messages[3].Content != "after" {
		t.Fatalf("following message = %#v", messages[3])
	}

	_, err = deepSeekImageMessages([]ChatMessage{{Role: "assistant", Images: []ChatImage{{MediaType: "image/png", Data: "AAAA"}}}})
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "UNSUPPORTED_CONTENT" {
		t.Fatalf("assistant image error = %#v", err)
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
