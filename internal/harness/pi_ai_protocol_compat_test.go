package harness

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

func TestPiAICatalogMatchesPinnedUpstream(t *testing.T) {
	if piAICatalogPackageVersion != "0.84.2" || piAICatalogManifestHash != "012f19b2e2c92706bc700f4c9dd80a21f1f43d68959bc90c78d2cdb51374d5cc" {
		t.Fatalf("catalog gate = %s %s", piAICatalogPackageVersion, piAICatalogManifestHash)
	}
	models := 0
	byAPI := map[string]int{}
	for _, route := range piAICatalog {
		models += len(route.Models)
		for _, model := range route.Models {
			byAPI[model.API]++
		}
	}
	if len(piAICatalog) != 40 || models != 1267 {
		t.Fatalf("catalog size = %d providers, %d models", len(piAICatalog), models)
	}
	wantUnsupported := []string{}
	if !slices.Equal(piAIUnsupportedCatalogProtocols, wantUnsupported) {
		t.Fatalf("unsupported protocols = %#v", piAIUnsupportedCatalogProtocols)
	}
	wantByAPI := map[string]int{
		"anthropic-messages": 280, "azure-openai-responses": 38, "bedrock-converse-stream": 114,
		"google-generative-ai": 28, "google-vertex": 13, "mistral-conversations": 31,
		"openai-codex-responses": 7, "openai-completions": 644, "openai-responses": 112,
	}
	if !reflect.DeepEqual(byAPI, wantByAPI) {
		t.Fatalf("catalog APIs = %#v", byAPI)
	}
	for route, catalog := range piAICatalog {
		if len(catalog.Models) == 0 {
			continue
		}
		profile, err := resolvePiAIProfile(route, map[string]any{})
		if err != nil {
			t.Fatalf("resolve catalog route %q: %v", route, err)
		}
		if len(profile.models) != len(catalog.Models) {
			t.Fatalf("resolved catalog route %q has %d models, want %d", route, len(profile.models), len(catalog.Models))
		}
	}
	openAI := piAICatalog["openai"]
	index := slices.IndexFunc(openAI.Models, func(model piAIModel) bool { return model.ID == "gpt-5.5" })
	if index < 0 {
		t.Fatal("catalog is missing openai/gpt-5.5")
	}
	model := openAI.Models[index]
	if model.API != "openai-responses" || model.ContextWindow != 272000 || model.MaxTokens != 128000 || !model.Reasoning || model.ThinkingLevelMap["xhigh"] == nil || *model.ThinkingLevelMap["xhigh"] != "xhigh" {
		t.Fatalf("openai/gpt-5.5 = %#v", model)
	}
	opencode := piAICatalog["opencode"]
	index = slices.IndexFunc(opencode.Models, func(model piAIModel) bool { return model.ID == "gpt-5.5" })
	if index < 0 {
		t.Fatal("catalog is missing opencode/gpt-5.5")
	}
	if opencode.Models[index].Compat.SessionAffinityFormat != "openai-nosession" {
		t.Fatalf("opencode/gpt-5.5 compat = %#v", opencode.Models[index].Compat)
	}
	fireworks := piAICatalog["fireworks"]
	index = slices.IndexFunc(fireworks.Models, func(model piAIModel) bool { return model.ID == "accounts/fireworks/models/deepseek-v4-flash" })
	if index < 0 {
		t.Fatal("catalog is missing fireworks/deepseek-v4-flash")
	}
	if fireworks.Models[index].Compat.SendSessionAffinityHeaders == nil || !*fireworks.Models[index].Compat.SendSessionAffinityHeaders || fireworks.Models[index].Compat.SupportsCacheControlOnTools == nil || *fireworks.Models[index].Compat.SupportsCacheControlOnTools {
		t.Fatalf("fireworks/deepseek-v4-flash compat = %#v", fireworks.Models[index].Compat)
	}
	baseten, err := resolvePiAIProfile("baseten", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	index = slices.IndexFunc(baseten.models, func(model piAIModel) bool { return model.ID == "moonshotai/Kimi-K2.5" })
	if index < 0 || baseten.models[index].Compat.ThinkingFormat != "baseten" || !reflect.DeepEqual(
		baseten.models[index].Compat.ChatTemplateArgs,
		map[string]any{"enable_thinking": map[string]any{"$var": "thinking.enabled"}},
	) {
		t.Fatalf("baseten/moonshotai/Kimi-K2.5 compat = %#v", baseten.models[index].Compat)
	}
}

func TestPiAIRouteCompatRequiresCompletionsModel(t *testing.T) {
	_, err := resolvePiAIProfile("openai", map[string]any{
		"compat": map[string]any{"thinkingFormat": "openai"},
	})
	if err == nil || !containsText(err.Error(), "no model on the route speaks a protocol that takes it") {
		t.Fatalf("resolvePiAIProfile() error = %v", err)
	}
}

func TestPiAICompatIsMergedPerProtocolAndPerField(t *testing.T) {
	profile, err := resolvePiAIProfile("custom-responses", map[string]any{
		"api": "openai-responses", "baseURL": "https://example.test/v1",
		"compat": map[string]any{"supportsDeveloperRole": false, "supportsStrictMode": false},
		"models": []any{map[string]any{
			"id": "model", "compat": map[string]any{"supportsStrictMode": true},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	compat := profile.models[0].Compat
	if compat.SupportsDeveloperRole == nil || *compat.SupportsDeveloperRole || compat.SupportsStrictMode == nil || !*compat.SupportsStrictMode {
		t.Fatalf("merged compat = %#v", compat)
	}

	_, err = resolvePiAIProfile("wrong-protocol", map[string]any{
		"api": "openai-responses", "baseURL": "https://example.test/v1",
		"models": []any{map[string]any{
			"id": "model", "compat": map[string]any{"thinkingFormat": "openai"},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "does not take it") {
		t.Fatalf("wrong protocol error = %v", err)
	}
}

func TestPiAIAlphaCompletionsCompatFields(t *testing.T) {
	profile, err := resolvePiAIProfile("private-baseten", map[string]any{
		"api": "openai-completions", "baseURL": "https://example.test/v1",
		"models": []any{map[string]any{
			"id": "reasoning-local", "reasoningEfforts": map[string]any{"off": "off", "high": "provider-high"},
			"compat": map[string]any{
				"supportsFinishReason": false, "thinkingFormat": "baseten",
				"chatTemplateArgs":            map[string]any{"enable_thinking": map[string]any{"$var": "thinking.enabled"}},
				"supportsThinkingTokenBudget": true,
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	compat := profile.models[0].Compat
	if compat.SupportsFinishReason == nil || *compat.SupportsFinishReason ||
		compat.SupportsThinkingTokenBudget == nil || !*compat.SupportsThinkingTokenBudget ||
		compat.ThinkingFormat != "baseten" || !reflect.DeepEqual(
		compat.ChatTemplateArgs,
		map[string]any{"enable_thinking": map[string]any{"$var": "thinking.enabled"}},
	) {
		t.Fatalf("alpha compat = %#v", compat)
	}
}

func TestPiAICompatRejectsWithheldEmptyAndInvalidKwargs(t *testing.T) {
	for _, test := range []struct {
		name   string
		compat map[string]any
		want   string
	}{
		{name: "withheld", compat: map[string]any{"sessionAffinityFormat": "openai"}, want: "not configurable"},
		{name: "withheld additional tools", compat: map[string]any{"supportsAdditionalTools": true}, want: "not configurable"},
		{name: "empty", compat: map[string]any{"supportsStore": nil}, want: "with no value"},
		{name: "variable", compat: map[string]any{"chatTemplateKwargs": map[string]any{"thinking": map[string]any{"$var": "unknown"}}}, want: "thinking.enabled or thinking.effort"},
		{name: "argument variable", compat: map[string]any{"chatTemplateArgs": map[string]any{"thinking": map[string]any{"$var": "unknown"}}}, want: "thinking.enabled or thinking.effort"},
		{name: "finish reason type", compat: map[string]any{"supportsFinishReason": "no"}, want: "must be boolean"},
		{name: "thinking budget type", compat: map[string]any{"supportsThinkingTokenBudget": "yes"}, want: "must be boolean"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolvePiAIProfile("custom", map[string]any{
				"api": "openai-completions", "baseURL": "https://example.test/v1",
				"compat": test.compat, "models": []any{map[string]any{"id": "model"}},
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestPiAIWebSocketConnectTimeoutPreservesExplicitZero(t *testing.T) {
	profile, err := resolvePiAIProfile("custom-responses", map[string]any{
		"api": "openai-responses", "baseURL": "https://example.test/v1",
		"websocketConnectTimeoutMs": 0,
		"models":                    []any{map[string]any{"id": "model"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !profile.websocketTimeoutSet || profile.websocketTimeout != 0 {
		t.Fatalf("websocket timeout = %s, set=%v", profile.websocketTimeout, profile.websocketTimeoutSet)
	}
}

func TestPiAIReasoningEffortMapping(t *testing.T) {
	reasoning, levels := resolvePiAIReasoningEfforts(map[string]any{"off": nil, "high": "provider-high"})
	model := piAIModel{ID: "mapped", Reasoning: reasoning, ThinkingLevelMap: levels}
	if !piAIModelSupportsReasoning(model, "off") || !piAIModelSupportsReasoning(model, "high") || piAIModelSupportsReasoning(model, "low") {
		t.Fatalf("mapped reasoning levels = %#v", levels)
	}
	if wire, ok := piAIReasoningWire(model, "high"); !ok || wire != "provider-high" {
		t.Fatalf("high wire = %q, %v", wire, ok)
	}

	_, levels = resolvePiAIReasoningEfforts(map[string]any{"high": "high"})
	model.ThinkingLevelMap = levels
	if piAIModelSupportsReasoning(model, "off") {
		t.Fatalf("omitted off level was reported supported: %#v", levels)
	}
}

func TestOpenAIResponsesWebSocketCachedContinuation(t *testing.T) {
	var connections atomic.Int32
	var mu sync.Mutex
	requests := make([]map[string]any, 0, 2)
	done := make(chan struct{})
	server := httptest.NewServer(websocket.Server{
		Handshake: func(_ *websocket.Config, request *http.Request) error {
			if request.Header.Get("OpenAI-Beta") != openAIResponsesWebSocketBeta {
				t.Errorf("OpenAI-Beta = %q", request.Header.Get("OpenAI-Beta"))
			}
			return nil
		},
		Handler: func(conn *websocket.Conn) {
			connections.Add(1)
			defer close(done)
			for index := 0; index < 2; index++ {
				var payload string
				if err := websocket.Message.Receive(conn, &payload); err != nil {
					t.Errorf("receive request %d: %v", index, err)
					return
				}
				var request map[string]any
				if err := json.Unmarshal([]byte(payload), &request); err != nil {
					t.Errorf("decode request %d: %v", index, err)
					return
				}
				mu.Lock()
				requests = append(requests, request)
				mu.Unlock()

				text := "answer-1"
				responseID := "resp-1"
				if index == 1 {
					text, responseID = "answer-2", "resp-2"
				}
				item, _ := json.Marshal(map[string]any{
					"type": "response.output_item.done", "output_index": 0,
					"item": map[string]any{
						"type": "message", "role": "assistant", "status": "completed",
						"content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}},
					},
				})
				for _, event := range []string{
					`{"type":"response.output_text.delta","output_index":0,"delta":"` + text + `"}`,
					string(item),
					`{"type":"response.completed","response":{"id":"` + responseID + `","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}`,
				} {
					if err := websocket.Message.Send(conn, event); err != nil {
						t.Errorf("send response %d: %v", index, err)
						return
					}
				}
			}
		},
	})
	defer server.Close()

	provider := NewOpenAIResponsesProvider("openai", server.URL, "test-key", "test-model")
	provider.transport = "websocket-cached"
	provider.cacheRetention = "short"
	provider.streamIdleTimeout = 2 * time.Second
	t.Cleanup(provider.websockets.Close)

	first, err := provider.Complete(context.Background(), ChatRequest{
		SessionID: "session-1", Messages: []ChatMessage{{Role: "user", Content: "first"}},
	}, func(Delta) error { return nil })
	if err != nil || first.Text != "answer-1" {
		t.Fatalf("first completion = %#v, %v", first, err)
	}
	second, err := provider.Complete(context.Background(), ChatRequest{
		SessionID: "session-1",
		Messages: []ChatMessage{
			{Role: "user", Content: "first"},
			{Role: "assistant", Content: "answer-1"},
			{Role: "user", Content: "second"},
		},
	}, func(Delta) error { return nil })
	if err != nil || second.Text != "answer-2" {
		t.Fatalf("second completion = %#v, %v", second, err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WebSocket server did not finish")
	}

	mu.Lock()
	defer mu.Unlock()
	if connections.Load() != 1 || len(requests) != 2 {
		t.Fatalf("connections=%d requests=%d", connections.Load(), len(requests))
	}
	if requests[0]["type"] != "response.create" || requests[0]["previous_response_id"] != nil {
		t.Fatalf("first request = %#v", requests[0])
	}
	if requests[1]["previous_response_id"] != "resp-1" {
		t.Fatalf("second previous_response_id = %#v", requests[1]["previous_response_id"])
	}
	delta, ok := requests[1]["input"].([]any)
	if !ok || len(delta) != 1 || delta[0].(map[string]any)["role"] != "user" {
		t.Fatalf("second input delta = %#v", requests[1]["input"])
	}
}

func TestOpenAIResponsesWebSocketBusyUsesTemporaryConnection(t *testing.T) {
	var connections atomic.Int32
	firstReceived := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(websocket.Server{
		Handshake: func(_ *websocket.Config, _ *http.Request) error { return nil },
		Handler: func(conn *websocket.Conn) {
			connection := connections.Add(1)
			var payload string
			if err := websocket.Message.Receive(conn, &payload); err != nil {
				t.Errorf("receive request: %v", err)
				return
			}
			text := "second"
			if connection == 1 {
				close(firstReceived)
				<-releaseFirst
				text = "first"
			}
			for _, event := range []string{
				`{"type":"response.output_text.delta","output_index":0,"delta":"` + text + `"}`,
				`{"type":"response.completed","response":{"id":"response-` + text + `","status":"completed"}}`,
			} {
				if err := websocket.Message.Send(conn, event); err != nil {
					t.Errorf("send response: %v", err)
					return
				}
			}
		},
	})
	defer server.Close()

	provider := NewOpenAIResponsesProvider("openai", server.URL, "test-key", "test-model")
	provider.transport = "websocket"
	provider.streamIdleTimeout = 2 * time.Second
	t.Cleanup(provider.websockets.Close)
	type result struct {
		completion Completion
		err        error
	}
	firstResult := make(chan result, 1)
	go func() {
		completion, err := provider.Complete(context.Background(), ChatRequest{SessionID: "session-1"}, func(Delta) error { return nil })
		firstResult <- result{completion: completion, err: err}
	}()
	select {
	case <-firstReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("first WebSocket request did not arrive")
	}
	second, err := provider.Complete(context.Background(), ChatRequest{SessionID: "session-1"}, func(Delta) error { return nil })
	if err != nil || second.Text != "second" {
		t.Fatalf("second completion = %#v, %v", second, err)
	}
	close(releaseFirst)
	first := <-firstResult
	if first.err != nil || first.completion.Text != "first" {
		t.Fatalf("first completion = %#v, %v", first.completion, first.err)
	}
	if connections.Load() != 2 {
		t.Fatalf("connections = %d", connections.Load())
	}
}

func TestOpenAIResponsesWebSocketFallsBackBeforeFirstEvent(t *testing.T) {
	var websocketAttempts atomic.Int32
	var sseRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Upgrade") == "websocket" {
			websocketAttempts.Add(1)
			http.Error(response, "websocket unavailable", http.StatusServiceUnavailable)
			return
		}
		sseRequests.Add(1)
		response.Header().Set("Content-Type", "text/event-stream")
		_, _ = response.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"fallback\"}\n\n"))
		_, _ = response.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-sse\",\"status\":\"completed\"}}\n\n"))
	}))
	defer server.Close()

	provider := NewOpenAIResponsesProvider("openai", server.URL, "test-key", "test-model")
	provider.transport = "auto"
	completion, err := provider.Complete(context.Background(), ChatRequest{SessionID: "fallback-session"}, func(Delta) error { return nil })
	if err != nil || completion.Text != "fallback" {
		t.Fatalf("completion = %#v, %v", completion, err)
	}
	if websocketAttempts.Load() != 1 || sseRequests.Load() != 1 {
		t.Fatalf("websocket attempts=%d sse requests=%d", websocketAttempts.Load(), sseRequests.Load())
	}
}

func TestOpenAIResponsesWebSocketRecoverySignals(t *testing.T) {
	for _, payload := range []string{
		`{"type":"error","error":{"code":"previous_response_not_found"}}`,
		`{"type":"response.failed","response":{"error":{"code":"websocket_connection_limit_reached"}}}`,
	} {
		if !recoverableOpenAIResponsesWebSocketError([]byte(payload)) {
			t.Fatalf("payload was not recoverable: %s", payload)
		}
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer first")
	first := openAIResponsesWebSocketCacheKey("session-1", "wss://example.test/responses", headers)
	headers.Set("Authorization", "Bearer second")
	second := openAIResponsesWebSocketCacheKey("session-1", "wss://example.test/responses", headers)
	if first == second {
		t.Fatal("credential rotation reused the same WebSocket cache key")
	}
}

func TestOpenAIResponsesCacheAndSessionAffinity(t *testing.T) {
	longUnsupported := false
	explicitCache := true
	for _, test := range []struct {
		name      string
		retention string
		compat    piAIModelCompat
		wantKey   bool
		wantLong  bool
		wantMode  bool
	}{
		{name: "default short", wantKey: true},
		{name: "long", retention: "long", wantKey: true, wantLong: true},
		{name: "long unsupported", retention: "long", compat: piAIModelCompat{SupportsLongCacheRetention: &longUnsupported}, wantKey: true},
		{name: "none explicit", retention: "none", compat: piAIModelCompat{SupportsExplicitPromptCacheMode: &explicitCache}, wantMode: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := NewOpenAIResponsesProvider("openai", "https://api.openai.com/v1", "", "test-model")
			provider.cacheRetention = test.retention
			provider.modelSpec = piAIModel{ID: "test-model", Compat: test.compat}
			body := provider.requestBody(ChatRequest{SessionID: "session-1"})
			if (body["prompt_cache_key"] == "session-1") != test.wantKey || (body["prompt_cache_retention"] == "24h") != test.wantLong {
				t.Fatalf("cache body = %#v", body)
			}
			_, hasMode := body["prompt_cache_options"]
			if hasMode != test.wantMode {
				t.Fatalf("prompt_cache_options = %#v", body["prompt_cache_options"])
			}
		})
	}

	request := &http.Request{Header: make(http.Header)}
	provider := NewOpenAIResponsesProvider("opencode", "https://opencode.ai/zen/v1", "", "gpt-5.5")
	provider.modelSpec = piAIModel{ID: "gpt-5.5", Compat: piAIModelCompat{SessionAffinityFormat: "openai-nosession"}}
	provider.applyRequestHeaders(request, ChatRequest{SessionID: "session-1"})
	if request.Header.Get("Session-Id") != "" || request.Header.Get("X-Client-Request-Id") != "session-1" {
		t.Fatalf("openai-nosession headers = %#v", request.Header)
	}
	request = &http.Request{Header: make(http.Header)}
	provider.modelSpec.Compat.SessionAffinityFormat = "openrouter"
	provider.applyRequestHeaders(request, ChatRequest{SessionID: "session-1"})
	if request.Header.Get("X-Session-Id") != "session-1" || request.Header.Get("X-Client-Request-Id") != "" {
		t.Fatalf("openrouter headers = %#v", request.Header)
	}
}

func TestOpenAICompletionsPiAICompatWire(t *testing.T) {
	var request map[string]any
	var headers http.Header
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, incoming *http.Request) {
		headers = incoming.Header.Clone()
		if err := json.NewDecoder(incoming.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		response.Header().Set("Content-Type", "text/event-stream")
		_, _ = response.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = response.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	valueFalse, valueTrue := false, true
	high := "high"
	provider := NewOpenAIProvider("openrouter", server.URL, "test-key", "anthropic/test-model")
	provider.cacheRetention = "long"
	provider.modelSpec = piAIModel{
		ID: "anthropic/test-model", Reasoning: true,
		ThinkingLevelMap: map[string]*string{"high": &high},
		Compat: piAIModelCompat{
			ThinkingFormat: "openrouter", CacheControlFormat: "anthropic", MaxTokensField: "max_tokens",
			SupportsStore: &valueFalse, SupportsUsageInStreaming: &valueFalse,
			SupportsLongCacheRetention: &valueTrue, SendSessionAffinityHeaders: &valueTrue,
			RequiresReasoningContent: &valueTrue, SupportsStrictMode: &valueTrue,
			SessionAffinityFormat: "openrouter",
		},
	}
	completion, err := provider.Complete(context.Background(), ChatRequest{
		SessionID: "session-1", System: "system", ReasoningEffort: "high", Temperature: float64Pointer(0), MaxTokens: 32,
		Messages: []ChatMessage{
			{Role: "user", Content: "before"},
			{Role: "assistant", Content: "prior"},
			{Role: "user", Content: "hello"},
		},
		Tools: []ToolSchema{{Name: "unit_tool", Parameters: map[string]any{"type": "object"}}},
	}, func(Delta) error { return nil })
	if err != nil || completion.Text != "ok" {
		t.Fatalf("completion = %#v, %v", completion, err)
	}
	if headers.Get("X-Session-Id") != "session-1" || headers.Get("X-Client-Request-Id") != "" {
		t.Fatalf("session affinity headers = %#v", headers)
	}
	if request["max_tokens"] != float64(32) || request["temperature"] != float64(0) || request["store"] != nil || request["stream_options"] != nil || request["prompt_cache_key"] != "session-1" || request["prompt_cache_retention"] != "24h" {
		t.Fatalf("compat request = %#v", request)
	}
	if !reflect.DeepEqual(request["reasoning"], map[string]any{"effort": "high"}) {
		t.Fatalf("reasoning = %#v", request["reasoning"])
	}
	messages := request["messages"].([]any)
	assistant := messages[2].(map[string]any)
	if value, exists := assistant["reasoning_content"]; !exists || value != "" {
		t.Fatalf("assistant reasoning_content = %#v", assistant)
	}
	tools := request["tools"].([]any)
	if tools[0].(map[string]any)["function"].(map[string]any)["strict"] != false {
		t.Fatalf("tool strict = %#v", tools[0])
	}
	controls := openAICompletionsRequestCacheControls(request)
	if len(controls) != 3 {
		t.Fatalf("cache controls = %#v", controls)
	}
	for _, control := range controls {
		if control["type"] != "ephemeral" || control["ttl"] != "1h" {
			t.Fatalf("cache control = %#v", control)
		}
	}
}

func TestOpenAICompletionsAlphaBasetenBudgetAndFinishInference(t *testing.T) {
	var requests []map[string]any
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		mu.Lock()
		requests = append(requests, request)
		index := len(requests)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if index == 1 {
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
		} else {
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"probe\",\"arguments\":\"{}\"}}]}}]}\n\n"))
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	model := piAIModel{
		ID: "baseten-local", MaxTokens: 5000, Reasoning: true,
		ThinkingLevelMap: map[string]*string{"off": stringPointer("off"), "high": stringPointer("high")},
		Compat: piAIModelCompat{
			ThinkingFormat:              "baseten",
			ChatTemplateArgs:            map[string]any{"enable_thinking": map[string]any{"$var": "thinking.enabled"}},
			SupportsReasoningEffort:     boolPointer(true),
			SupportsFinishReason:        boolPointer(false),
			SupportsThinkingTokenBudget: boolPointer(true),
			MaxTokensField:              "max_tokens",
		},
	}
	provider := NewOpenAIProvider("baseten", server.URL, "key", model.ID)
	provider.modelSpec = model
	first, err := provider.Complete(t.Context(), ChatRequest{
		Model: model.ID, ReasoningEffort: "high", MaxTokens: 5000,
		Messages: []ChatMessage{{Role: "user", Content: "hello"}},
	}, func(Delta) error { return nil })
	if err != nil || first.Finish != "stop" {
		t.Fatalf("inferred stop completion = %#v, %v", first, err)
	}
	second, err := provider.Complete(t.Context(), ChatRequest{
		Model: model.ID, ReasoningEffort: "xhigh", MaxTokens: 2000,
		Messages: []ChatMessage{{Role: "user", Content: "call"}},
	}, func(Delta) error { return nil })
	if err != nil || second.Finish != "tool_calls" || len(second.ToolCalls) != 1 {
		t.Fatalf("inferred tool completion = %#v, %v", second, err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("requests = %d", len(requests))
	}
	firstBody := requests[0]
	if firstBody["chat_template_args"].(map[string]any)["enable_thinking"] != true ||
		firstBody["reasoning_effort"] != "high" || firstBody["thinking_token_budget"] != float64(3976) {
		t.Fatalf("baseten request = %#v", firstBody)
	}
	secondBody := requests[1]
	if secondBody["thinking_token_budget"] != float64(976) {
		t.Fatalf("clamped budget request = %#v", secondBody)
	}
}

func TestOpenAICompletionsFinishReasonRequiredByDefault(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	provider := NewOpenAIProvider("strict-finish", server.URL, "key", "model")
	provider.modelSpec = piAIModel{ID: "model", Input: []string{"text"}}
	_, err := provider.Complete(t.Context(), ChatRequest{Model: "model"}, func(Delta) error { return nil })
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "TRANSPORT" || !strings.Contains(providerErr.Message, "Stream ended without finish_reason") {
		t.Fatalf("missing finish_reason error = %#v", err)
	}
}

func TestOpenAIThinkingTokenBudgetDefaultsAndCustomCeiling(t *testing.T) {
	for _, test := range []struct {
		effort        string
		configured    map[string]int
		ceiling, want int
	}{
		{effort: "minimal", ceiling: 10000, want: 1024},
		{effort: "low", configured: map[string]int{"low": 777}, ceiling: 10000, want: 777},
		{effort: "xhigh", ceiling: 2000, want: 976},
		{effort: "high", ceiling: 1024, want: 0},
	} {
		t.Run(test.effort, func(t *testing.T) {
			budget, ok := openAIThinkingTokenBudget(test.effort, test.configured, test.ceiling)
			if budget != test.want || (test.want > 0 && !ok) || (test.want == 0 && ok) {
				t.Fatalf("budget = %d, ok=%v, want %d", budget, ok, test.want)
			}
		})
	}
}

func TestAnthropicPiAICacheAndAdaptiveThinkingWire(t *testing.T) {
	xhigh := "xhigh"
	adaptive := true
	model := piAIModel{
		ID: "claude-fable-5", Reasoning: true,
		ThinkingLevelMap: map[string]*string{"off": nil, "xhigh": &xhigh},
		Compat:           piAIModelCompat{ForceAdaptiveThinking: &adaptive},
	}
	for _, test := range []struct {
		name         string
		retention    string
		toolCache    *bool
		wantControls int
		wantTTL      string
		wantAffinity bool
	}{
		{name: "default short", wantControls: 3, wantAffinity: true},
		{name: "long", retention: "long", wantControls: 3, wantTTL: "1h", wantAffinity: true},
		{name: "tool cache unsupported", toolCache: boolPointer(false), wantControls: 2, wantAffinity: true},
		{name: "none", retention: "none"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var request map[string]any
			var affinity string
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, incoming *http.Request) {
				affinity = incoming.Header.Get("X-Session-Affinity")
				if err := json.NewDecoder(incoming.Body).Decode(&request); err != nil {
					t.Errorf("decode request: %v", err)
				}
				response.Header().Set("Content-Type", "text/event-stream")
				_, _ = response.Write([]byte("data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n"))
				_, _ = response.Write([]byte("data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n"))
				_, _ = response.Write([]byte("data: {\"type\":\"message_stop\"}\n\n"))
			}))
			defer server.Close()

			provider := NewAnthropicProvider("anthropic", server.URL, "test-key", model.ID)
			modelSpec := model
			modelSpec.Compat.SupportsCacheControlOnTools = test.toolCache
			modelSpec.Compat.SendSessionAffinityHeaders = boolPointer(true)
			provider.modelSpec = modelSpec
			provider.cacheRetention = test.retention
			completion, err := provider.Complete(context.Background(), ChatRequest{
				SessionID: "session-1", ReasoningEffort: "xhigh", System: "system",
				Messages: []ChatMessage{{Role: "user", Content: "hello"}},
				Tools:    []ToolSchema{{Name: "unit_tool", Parameters: map[string]any{"type": "object"}}},
			}, func(Delta) error { return nil })
			if err != nil || completion.Text != "ok" {
				t.Fatalf("completion = %#v, %v", completion, err)
			}
			if !reflect.DeepEqual(request["thinking"], map[string]any{"type": "adaptive", "display": "summarized"}) || !reflect.DeepEqual(request["output_config"], map[string]any{"effort": "xhigh"}) {
				t.Fatalf("adaptive thinking = %#v %#v", request["thinking"], request["output_config"])
			}
			if (affinity == "session-1") != test.wantAffinity {
				t.Fatalf("X-Session-Affinity = %q", affinity)
			}
			controls := anthropicRequestCacheControls(request)
			if len(controls) != test.wantControls {
				t.Fatalf("cache controls = %#v", controls)
			}
			for _, control := range controls {
				if control["type"] != "ephemeral" || stringSetting(control["ttl"]) != test.wantTTL {
					t.Fatalf("cache control = %#v", control)
				}
			}
			tools := request["tools"].([]any)
			if tools[0].(map[string]any)["eager_input_streaming"] != true {
				t.Fatalf("tool eager_input_streaming = %#v", tools[0])
			}
		})
	}
}

func TestResponsesCompatCanDisableDeveloperAndStrict(t *testing.T) {
	disabled := false
	model := piAIModel{ID: "reasoning-model", Reasoning: true, Compat: piAIModelCompat{
		SupportsDeveloperRole: &disabled, SupportsStrictMode: &disabled,
	}}
	request := ChatRequest{
		System: "system", Messages: []ChatMessage{{Role: "user", Content: "hello"}},
		Tools: []ToolSchema{{Name: "unit_tool", Parameters: map[string]any{"type": "object"}}},
	}
	provider := NewOpenAIResponsesProvider("openai", "https://api.openai.com/v1", "", model.ID)
	provider.modelSpec = model
	body := provider.requestBody(request)
	input := body["input"].([]any)
	if input[0].(map[string]any)["role"] != "system" {
		t.Fatalf("system role = %#v", input[0])
	}
	tools := body["tools"].([]any)
	if _, exists := tools[0].(map[string]any)["strict"]; exists {
		t.Fatalf("OpenAI Responses strict was not disabled: %#v", tools[0])
	}

	codex := newCatalogResponsesProvider("openai-codex", "https://chatgpt.com/backend-api", "token", model.ID, true)
	codex.modelSpec = model
	codexBody := codex.codexRequestBody(request)
	codexTools := codexBody["tools"].([]any)
	if _, exists := codexTools[0].(map[string]any)["strict"]; exists {
		t.Fatalf("Codex strict was not disabled: %#v", codexTools[0])
	}
}

func openAICompletionsRequestCacheControls(request map[string]any) []map[string]any {
	controls := make([]map[string]any, 0, 3)
	messages, _ := request["messages"].([]any)
	for _, index := range []int{0, len(messages) - 1} {
		if index < 0 || index >= len(messages) {
			continue
		}
		content, _ := messages[index].(map[string]any)["content"].([]any)
		if len(content) > 0 {
			if control, ok := content[len(content)-1].(map[string]any)["cache_control"].(map[string]any); ok {
				controls = append(controls, control)
			}
		}
	}
	tools, _ := request["tools"].([]any)
	if len(tools) > 0 {
		if control, ok := tools[len(tools)-1].(map[string]any)["cache_control"].(map[string]any); ok {
			controls = append(controls, control)
		}
	}
	return controls
}

func anthropicRequestCacheControls(request map[string]any) []map[string]any {
	controls := make([]map[string]any, 0, 3)
	system, _ := request["system"].([]any)
	if len(system) > 0 {
		if control, ok := system[0].(map[string]any)["cache_control"].(map[string]any); ok {
			controls = append(controls, control)
		}
	}
	messages, _ := request["messages"].([]any)
	if len(messages) > 0 {
		blocks, _ := messages[len(messages)-1].(map[string]any)["content"].([]any)
		if len(blocks) > 0 {
			if control, ok := blocks[len(blocks)-1].(map[string]any)["cache_control"].(map[string]any); ok {
				controls = append(controls, control)
			}
		}
	}
	tools, _ := request["tools"].([]any)
	if len(tools) > 0 {
		if control, ok := tools[len(tools)-1].(map[string]any)["cache_control"].(map[string]any); ok {
			controls = append(controls, control)
		}
	}
	return controls
}

func containsText(value, part string) bool {
	return strings.Contains(value, part)
}

func boolPointer(value bool) *bool { return &value }

func stringPointer(value string) *string { return &value }
