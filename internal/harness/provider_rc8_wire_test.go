package harness

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

func TestProviderWirePreservesOrderedPartsAndToolImages(t *testing.T) {
	userParts := []ChatContentPart{
		{Type: "text", Text: "before"},
		{Type: "image", MediaType: "image/png", Data: "USER"},
		{Type: "text", Text: "after"},
	}
	toolParts := []ChatContentPart{
		{Type: "text", Text: "tool before"},
		{Type: "image", MediaType: "image/jpeg", Data: "TOOL"},
		{Type: "text", Text: "tool after"},
	}
	messages := []ChatMessage{
		{Role: "user", Content: "legacy text", Parts: userParts},
		{Role: "assistant", ToolCalls: []ToolCall{{ID: "call-1", Name: "inspect", Arguments: json.RawMessage(`{}`)}}},
		{Role: "tool", ToolCallID: "call-1", Content: "tool before\ntool after", Parts: toolParts},
	}

	t.Run("OpenAI completions", func(t *testing.T) {
		var request map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode request: %v", err)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
		}))
		defer server.Close()

		completion, err := NewOpenAIProvider("test", server.URL, "key", "model").Complete(t.Context(), ChatRequest{Messages: messages}, func(Delta) error { return nil })
		if err != nil || completion.Text != "ok" {
			t.Fatalf("Complete() = %#v, %v", completion, err)
		}
		wire := request["messages"].([]any)
		content := wire[0].(map[string]any)["content"].([]any)
		if len(content) != 3 || content[0].(map[string]any)["text"] != "before" || content[1].(map[string]any)["image_url"].(map[string]any)["url"] != "data:image/png;base64,USER" || content[2].(map[string]any)["text"] != "after" {
			t.Fatalf("ordered user content = %#v", content)
		}
		toolImage := wire[3].(map[string]any)["content"].([]any)
		if len(toolImage) != 2 || toolImage[1].(map[string]any)["image_url"].(map[string]any)["url"] != "data:image/jpeg;base64,TOOL" {
			t.Fatalf("tool result image = %#v", toolImage)
		}
	})

	t.Run("OpenAI Responses", func(t *testing.T) {
		var request map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode request: %v", err)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"))
		}))
		defer server.Close()

		provider := NewOpenAIResponsesProvider("test", server.URL, "key", "model")
		provider.modelSpec = piAIModel{ID: "model", Input: []string{"text", "image"}}
		completion, err := provider.Complete(t.Context(), ChatRequest{Messages: messages}, func(Delta) error { return nil })
		if err != nil || completion.Text != "ok" {
			t.Fatalf("Complete() = %#v, %v", completion, err)
		}
		input := request["input"].([]any)
		user := input[0].(map[string]any)["content"].([]any)
		if len(user) != 3 || user[0].(map[string]any)["text"] != "before" || user[1].(map[string]any)["image_url"] != "data:image/png;base64,USER" || user[2].(map[string]any)["text"] != "after" {
			t.Fatalf("ordered Responses user content = %#v", user)
		}
		output := input[2].(map[string]any)["output"].([]any)
		if len(output) != 3 || output[0].(map[string]any)["text"] != "tool before" || output[1].(map[string]any)["image_url"] != "data:image/jpeg;base64,TOOL" || output[2].(map[string]any)["text"] != "tool after" {
			t.Fatalf("ordered Responses tool output = %#v", output)
		}
	})

	t.Run("Anthropic", func(t *testing.T) {
		var request map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode request: %v", err)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n"))
		}))
		defer server.Close()

		completion, err := NewAnthropicProvider("test", server.URL, "key", "model").Complete(t.Context(), ChatRequest{Messages: messages}, func(Delta) error { return nil })
		if err != nil || completion.Text != "ok" {
			t.Fatalf("Complete() = %#v, %v", completion, err)
		}
		wire := request["messages"].([]any)
		user := wire[0].(map[string]any)["content"].([]any)
		if len(user) != 3 || user[0].(map[string]any)["text"] != "before" || user[1].(map[string]any)["source"].(map[string]any)["data"] != "USER" || user[2].(map[string]any)["text"] != "after" {
			t.Fatalf("ordered Anthropic user content = %#v", user)
		}
		toolResult := wire[2].(map[string]any)["content"].([]any)[0].(map[string]any)["content"].([]any)
		if len(toolResult) != 3 || toolResult[0].(map[string]any)["text"] != "tool before" || toolResult[1].(map[string]any)["source"].(map[string]any)["data"] != "TOOL" || toolResult[2].(map[string]any)["text"] != "tool after" {
			t.Fatalf("ordered Anthropic tool result = %#v", toolResult)
		}
	})
}

func TestAnthropicSignatureDeltaPersistsAndReplays(t *testing.T) {
	var mu sync.Mutex
	requests := make([]map[string]any, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		mu.Lock()
		requestIndex := len(requests)
		requests = append(requests, request)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if requestIndex == 0 {
			_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"plan\"}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig-1\"}}\n\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call-1\",\"name\":\"signature_probe\",\"input\":{}}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n"))
			return
		}
		_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	provider := NewAnthropicProvider("anthropic-signature", server.URL, "key", "model")
	provider.modelSpec = piAIModel{ID: "model", Reasoning: true}
	engine := newIntegrationEngine(t)
	engine.RegisterProvider(provider)
	if err := engine.RegisterTool(Tool{
		Schema:  ToolSchema{Name: "signature_probe", Parameters: map[string]any{"type": "object"}},
		Execute: func(context.Context, ToolCall) (ToolResult, error) { return textToolResult("tool result"), nil },
	}); err != nil {
		t.Fatal(err)
	}
	id, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: "model", ReasoningEffort: "high"}); err != nil {
		t.Fatal(err)
	}
	if text, err := engine.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "start"}}}); err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v", text, err)
	}

	mu.Lock()
	deferred := append([]map[string]any(nil), requests...)
	mu.Unlock()
	if len(deferred) != 2 {
		t.Fatalf("provider requests = %d", len(deferred))
	}
	var replay map[string]any
	for _, item := range deferred[1]["messages"].([]any) {
		message := item.(map[string]any)
		if message["role"] == "assistant" {
			replay = message
			break
		}
	}
	if replay == nil {
		t.Fatalf("second request did not replay assistant: %#v", deferred[1])
	}
	thinking := replay["content"].([]any)[0].(map[string]any)
	if thinking["type"] != "thinking" || thinking["thinking"] != "plan" || thinking["signature"] != "sig-1" {
		t.Fatalf("replayed thinking = %#v", thinking)
	}
	session, err := engine.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	persisted := false
	for _, event := range events {
		if event.Type != "assistant/message" {
			continue
		}
		for _, block := range contentBlocks(nestedMessage(event.Data)["content"]) {
			persisted = persisted || block.Type == "reasoning" && block.Text == "plan" && block.Signature == "sig-1"
		}
	}
	if !persisted {
		t.Fatalf("signature was not persisted: %#v", events)
	}
}

func TestAnthropicReplayPreservesResolvedModelAndMidConversationEffort(t *testing.T) {
	var mu sync.Mutex
	requests := make([]map[string]any, 0, 2)
	betas := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		mu.Lock()
		requestIndex := len(requests)
		requests = append(requests, request)
		betas = append(betas, r.Header.Get("Anthropic-Beta"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if requestIndex == 0 {
			_, _ = w.Write([]byte("data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-native\",\"model\":\"claude-native-20261001\",\"usage\":{\"input_tokens\":4}}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"native thought\"}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"native-signature\"}}\n\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call-native\",\"name\":\"native_probe\",\"input\":{}}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n"))
			return
		}
		_, _ = w.Write([]byte("data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-final\",\"model\":\"claude-native-20261001\"}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	valueTrue := true
	provider := NewAnthropicProvider("anthropic-native", server.URL, "key", "claude-alias")
	provider.modelSpec = piAIModel{
		ID: "claude-alias", API: "anthropic-messages", Reasoning: true,
		ThinkingLevelMap: map[string]*string{"high": stringPointer("high")},
		Compat:           piAIModelCompat{SupportsMidConvoEffort: &valueTrue, ForceAdaptiveThinking: &valueTrue},
	}
	engine := newIntegrationEngine(t)
	engine.RegisterProvider(provider)
	if err := engine.RegisterTool(Tool{
		Schema:  ToolSchema{Name: "native_probe", Parameters: map[string]any{"type": "object"}},
		Execute: func(context.Context, ToolCall) (ToolResult, error) { return textToolResult("tool result"), nil },
	}); err != nil {
		t.Fatal(err)
	}
	id, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: "claude-alias", ReasoningEffort: "high"}); err != nil {
		t.Fatal(err)
	}
	if text, err := engine.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "start"}}}); err != nil || text != "done" {
		t.Fatalf("Run() = %q, %v", text, err)
	}

	mu.Lock()
	deferredRequests := append([]map[string]any(nil), requests...)
	deferredBetas := append([]string(nil), betas...)
	mu.Unlock()
	if len(deferredRequests) != 2 {
		t.Fatalf("requests = %d", len(deferredRequests))
	}
	for index, beta := range deferredBetas {
		if !strings.Contains(beta, "mid-conversation-output-config-2026-07-01") || !strings.Contains(beta, "thinking-binding-controls-2026-08-01") {
			t.Fatalf("request %d beta = %q", index, beta)
		}
	}
	thinking := deferredRequests[0]["thinking"].(map[string]any)
	if thinking["type"] != "adaptive" || thinking["block_binding"].(map[string]any)["prefix_mismatch_behavior"] != "drop_block" {
		t.Fatalf("adaptive thinking = %#v", thinking)
	}
	messages := deferredRequests[1]["messages"].([]any)
	var historicalEffort, replayedReasoning any
	effortMessages := 0
	for _, raw := range messages {
		message := raw.(map[string]any)
		if message["role"] == "system" {
			if output, ok := message["output_config"].(map[string]any); ok {
				historicalEffort = output["effort"]
				effortMessages++
			}
		}
		if message["role"] == "assistant" {
			content := message["content"].([]any)
			replayedReasoning = content[0].(map[string]any)
		}
	}
	if historicalEffort != "high" || effortMessages != 2 {
		t.Fatalf("historical effort = %#v, messages=%d in %#v", historicalEffort, effortMessages, messages)
	}
	if block := replayedReasoning.(map[string]any); block["type"] != "text" || block["text"] != "native thought" || block["signature"] != nil {
		t.Fatalf("cross-model replay block = %#v", block)
	}

	session, err := engine.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	for _, event := range events {
		if event.Type != "assistant/message" {
			continue
		}
		source, _ := nestedMessage(event.Data)["source"].(map[string]any)
		replay, _ := source["replayState"].(map[string]any)
		response, _ := replay["response"].(map[string]any)
		if response["responseId"] == "msg-native" {
			if response["model"] != "claude-alias" || response["responseModel"] != "claude-native-20261001" || response["providerThinkingLevel"] != "high" {
				t.Fatalf("replay response = %#v", response)
			}
			return
		}
	}
	t.Fatal("native replay state was not persisted")
}

func TestResponsesStrictSamplingRequirement(t *testing.T) {
	tool := ToolSchema{
		Name: "strict_tool", Parameters: map[string]any{"type": "object"},
		ConstrainedSampling: &ToolConstrainedSampling{Type: "json_schema", Strict: "require"},
	}
	t.Run("unsupported rejects before request", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
		defer server.Close()
		provider := NewOpenAIResponsesProvider("test", server.URL, "key", "model")
		provider.modelSpec = piAIModel{ID: "model", Compat: piAIModelCompat{SupportsStrictMode: boolPointer(false)}}
		_, err := provider.Complete(t.Context(), ChatRequest{Tools: []ToolSchema{tool}}, func(Delta) error { return nil })
		if err == nil || requests.Load() != 0 {
			t.Fatalf("Complete() error=%v requests=%d", err, requests.Load())
		}
	})
	t.Run("supported sends strict", func(t *testing.T) {
		var request map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode request: %v", err)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"))
		}))
		defer server.Close()
		provider := NewOpenAIResponsesProvider("test", server.URL, "key", "model")
		provider.modelSpec = piAIModel{ID: "model", Compat: piAIModelCompat{SupportsStrictMode: boolPointer(true)}}
		completion, err := provider.Complete(t.Context(), ChatRequest{Tools: []ToolSchema{tool}}, func(Delta) error { return nil })
		if err != nil || completion.Text != "ok" {
			t.Fatalf("Complete() = %#v, %v", completion, err)
		}
		if strict := request["tools"].([]any)[0].(map[string]any)["strict"]; strict != true {
			t.Fatalf("strict = %#v", strict)
		}
	})
}

func TestProviderStreamIdleTimeouts(t *testing.T) {
	t.Run("SSE comments do not reset idle timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			flusher := w.(http.Flusher)
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				_, _ = w.Write([]byte(": keepalive\n\n"))
				flusher.Flush()
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
				}
			}
		}))
		defer server.Close()
		provider := NewOpenAIProvider("test", server.URL, "key", "model")
		provider.streamIdleTimeout = 80 * time.Millisecond
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err := provider.Complete(ctx, ChatRequest{}, func(Delta) error { return nil })
		var providerErr *ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "TIMEOUT" {
			t.Fatalf("Complete() error = %#v", err)
		}
	})

	t.Run("WebSocket read idle timeout", func(t *testing.T) {
		received := make(chan struct{}, 1)
		ws := websocket.Server{
			Handshake: func(_ *websocket.Config, _ *http.Request) error { return nil },
			Handler: func(conn *websocket.Conn) {
				var request string
				if err := websocket.Message.Receive(conn, &request); err != nil {
					return
				}
				received <- struct{}{}
				for {
					var ignored string
					if websocket.Message.Receive(conn, &ignored) != nil {
						return
					}
				}
			},
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Upgrade") == "websocket" {
				ws.ServeHTTP(w, r)
				return
			}
			http.Error(w, "unexpected SSE fallback", http.StatusMethodNotAllowed)
		}))
		defer server.Close()
		provider := NewOpenAIResponsesProvider("test", server.URL, "key", "model")
		provider.transport = "websocket-cached"
		provider.cacheRetention = "short"
		provider.streamIdleTimeout = 80 * time.Millisecond
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err := provider.Complete(ctx, ChatRequest{SessionID: "idle-session"}, func(Delta) error { return nil })
		select {
		case <-received:
		case <-time.After(time.Second):
			t.Fatal("WebSocket server did not receive response.create")
		}
		var providerErr *ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "TIMEOUT" {
			t.Fatalf("Complete() error = %#v", err)
		}
	})
}
