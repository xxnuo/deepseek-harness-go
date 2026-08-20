package harness

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

func TestGoogleGenerativeAIProtocol(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1beta/models/gemini-2.5-flash:streamGenerateContent" || r.URL.Query().Get("alt") != "sse" {
			t.Errorf("request URL = %s", r.URL.String())
		}
		if r.Header.Get("X-Goog-Api-Key") != "gemini-key" {
			t.Errorf("api key = %q", r.Header.Get("X-Goog-Api-Key"))
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		generation, _ := body["generationConfig"].(map[string]any)
		thinking, _ := generation["thinkingConfig"].(map[string]any)
		if generation["temperature"] != float64(0) || generation["maxOutputTokens"] != float64(1024) || thinking["thinkingBudget"] != float64(2048) || thinking["includeThoughts"] != true {
			t.Errorf("generationConfig = %#v", generation)
		}
		if len(body["tools"].([]any)) != 1 || body["systemInstruction"] == nil {
			t.Errorf("request body = %#v", body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSEData(t, w, map[string]any{
			"candidates": []any{map[string]any{"content": map[string]any{"parts": []any{
				map[string]any{"thought": true, "text": "reason"}, map[string]any{"text": "answer"},
				map[string]any{"functionCall": map[string]any{"id": "call-1", "name": "lookup", "args": map[string]any{"q": "x"}}},
			}}, "finishReason": "STOP"}},
			"usageMetadata": map[string]any{"promptTokenCount": 10, "cachedContentTokenCount": 2, "candidatesTokenCount": 3, "thoughtsTokenCount": 1, "totalTokenCount": 14},
		})
	}))
	defer server.Close()

	provider := newGoogleProvider("google", server.URL+"/v1beta", "gemini-key", "gemini-2.5-flash", false)
	provider.modelSpec = piAIModel{ID: "gemini-2.5-flash", Reasoning: true}
	provider.thinkingBudgets = map[string]int{"low": 2048}
	completion, err := provider.Complete(context.Background(), ChatRequest{
		SessionID: "session", Model: "gemini-2.5-flash", System: "system", ReasoningEffort: "low", Temperature: float64Pointer(0), MaxTokens: 1024,
		Messages: []ChatMessage{{Role: "user", Content: "hello"}},
		Tools:    []ToolSchema{{Name: "lookup", Parameters: map[string]any{"type": "object"}}},
	}, func(Delta) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if completion.Text != "answer" || completion.Reasoning != "reason" || completion.Finish != "tool_calls" || len(completion.ToolCalls) != 1 || string(completion.ToolCalls[0].Arguments) != `{"q":"x"}` {
		t.Fatalf("completion = %#v", completion)
	}
}

func TestGoogleVertexProtocol(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "project-one")
	t.Setenv("GOOGLE_CLOUD_LOCATION", "us-central1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantPath := "/v1/projects/project-one/locations/us-central1/publishers/google/models/gemini-3-flash-preview:streamGenerateContent"
		if r.URL.Path != wantPath || r.URL.Query().Get("alt") != "sse" {
			t.Errorf("request URL = %s", r.URL.String())
		}
		if r.Header.Get("X-Goog-Api-Key") != "vertex-key" {
			t.Errorf("api key = %q", r.Header.Get("X-Goog-Api-Key"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSEData(t, w, map[string]any{"candidates": []any{map[string]any{
			"content": map[string]any{"parts": []any{map[string]any{"text": "vertex"}}}, "finishReason": "STOP",
		}}})
	}))
	defer server.Close()

	provider := newGoogleProvider("google-vertex", server.URL, "vertex-key", "gemini-3-flash-preview", true)
	provider.modelSpec = piAIModel{ID: "gemini-3-flash-preview", Reasoning: true}
	completion, err := provider.Complete(context.Background(), ChatRequest{Model: provider.model, Messages: []ChatMessage{{Role: "user", Content: "hello"}}}, func(Delta) error { return nil })
	if err != nil || completion.Text != "vertex" {
		t.Fatalf("Complete() = %#v, %v", completion, err)
	}
}

func TestGoogleVertexADCProtocol(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "project-adc")
	t.Setenv("GOOGLE_CLOUD_LOCATION", "europe-west1")
	t.Setenv("GOOGLE_CLOUD_API_KEY", "")
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != "refresh-one" {
			t.Errorf("token form = %#v", r.Form)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "adc-token", "expires_in": 3600})
	}))
	defer tokenServer.Close()
	path := t.TempDir() + "/adc.json"
	credential, _ := json.Marshal(map[string]any{
		"type": "authorized_user", "token_uri": tokenServer.URL,
		"refresh_token": "refresh-one", "client_id": "client-one", "client_secret": "secret-one",
	})
	if err := os.WriteFile(path, credential, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer adc-token" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSEData(t, w, map[string]any{"candidates": []any{map[string]any{
			"content": map[string]any{"parts": []any{map[string]any{"text": "adc"}}}, "finishReason": "STOP",
		}}})
	}))
	defer modelServer.Close()

	provider := newGoogleProvider("google-vertex", modelServer.URL, "", "gemini-2.5-flash", true)
	completion, err := provider.Complete(context.Background(), ChatRequest{Model: provider.model, Messages: []ChatMessage{{Role: "user", Content: "hello"}}}, func(Delta) error { return nil })
	if err != nil || completion.Text != "adc" {
		t.Fatalf("Complete() = %#v, %v", completion, err)
	}
}

func TestMistralConversationsProtocol(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer mistral-key" || r.Header.Get("X-Affinity") != "session-one" {
			t.Errorf("request = %s auth=%q affinity=%q", r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("X-Affinity"))
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["prompt_cache_key"] != "session-one" || body["prompt_mode"] != "reasoning" || body["temperature"] != float64(0) {
			t.Errorf("request body = %#v", body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		writeSSEData(t, w, map[string]any{
			"choices": []any{map[string]any{"delta": map[string]any{
				"content":    []any{map[string]any{"type": "thinking", "thinking": []any{map[string]any{"type": "text", "text": "think"}}}, map[string]any{"type": "text", "text": "mistral"}},
				"tool_calls": []any{map[string]any{"index": 0, "id": "abc123xyz", "function": map[string]any{"name": "lookup", "arguments": `{"q":"x"}`}}},
			}, "finish_reason": "tool_calls"}},
			"usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14, "prompt_tokens_details": map[string]any{"cached_tokens": 3}},
		})
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	provider := newMistralProvider("mistral", server.URL, "mistral-key", "magistral-medium-latest")
	provider.cacheRetention = "short"
	provider.modelSpec = piAIModel{ID: provider.model, Reasoning: true}
	completion, err := provider.Complete(context.Background(), ChatRequest{
		SessionID: "session-one", Model: provider.model, ReasoningEffort: "low", Temperature: float64Pointer(0),
		Messages: []ChatMessage{{Role: "user", Content: "hello"}},
	}, func(Delta) error { return nil })
	if err != nil || completion.Text != "mistral" || completion.Reasoning != "think" || completion.Finish != "tool_calls" || len(completion.ToolCalls) != 1 {
		t.Fatalf("Complete() = %#v, %v", completion, err)
	}
}

func TestAzureOpenAIResponsesProtocol(t *testing.T) {
	t.Setenv("AZURE_OPENAI_API_VERSION", "2026-01-01-preview")
	t.Setenv("AZURE_OPENAI_DEPLOYMENT_NAME_MAP", "gpt-5=deployment-five")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" || r.URL.Query().Get("api-version") != "2026-01-01-preview" || r.Header.Get("Api-Key") != "azure-key" {
			t.Errorf("request = %s headers=%v", r.URL.String(), r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["model"] != "deployment-five" || body["store"] != false || body["temperature"] != float64(0) {
			t.Errorf("request body = %#v", body)
		}
		writeResponsesStream(t, w, "azure")
	}))
	defer server.Close()

	provider := newCatalogResponsesProvider("azure-openai-responses", server.URL, "azure-key", "gpt-5", false)
	provider.modelSpec = piAIModel{ID: "gpt-5", Reasoning: true}
	completion, err := provider.Complete(context.Background(), ChatRequest{Model: "gpt-5", Temperature: float64Pointer(0), Messages: []ChatMessage{{Role: "user", Content: "hello"}}}, func(Delta) error { return nil })
	if err != nil || completion.Text != "azure" {
		t.Fatalf("Complete() = %#v, %v", completion, err)
	}
}

func TestOpenAICodexResponsesProtocol(t *testing.T) {
	claims, _ := json.Marshal(map[string]any{"https://api.openai.com/auth": map[string]any{"chatgpt_account_id": "account-one"}})
	token := "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".signature"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/backend-api/codex/responses" || r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("ChatGPT-Account-Id") != "account-one" || r.Header.Get("OpenAI-Beta") != "responses=experimental" {
			t.Errorf("request = %s headers=%v", r.URL.Path, r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["instructions"] != "system" || body["prompt_cache_key"] != "session-codex" || body["store"] != false || body["temperature"] != float64(0) {
			t.Errorf("request body = %#v", body)
		}
		writeResponsesStream(t, w, "codex")
	}))
	defer server.Close()

	provider := newCatalogResponsesProvider("openai-codex", server.URL+"/backend-api", token, "gpt-5.4", true)
	provider.cacheRetention = "short"
	provider.modelSpec = piAIModel{ID: "gpt-5.4", Reasoning: true}
	completion, err := provider.Complete(context.Background(), ChatRequest{
		SessionID: "session-codex", Model: "gpt-5.4", System: "system", ReasoningEffort: "high", Temperature: float64Pointer(0),
		Messages: []ChatMessage{{Role: "user", Content: "hello"}},
	}, func(Delta) error { return nil })
	if err != nil || completion.Text != "codex" {
		t.Fatalf("Complete() = %#v, %v", completion, err)
	}
}

func TestOpenAICodexResponsesWebSocketProtocol(t *testing.T) {
	claims, _ := json.Marshal(map[string]any{"https://api.openai.com/auth": map[string]any{"chatgpt_account_id": "account-ws"}})
	token := "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".signature"
	done := make(chan struct{})
	server := httptest.NewServer(websocket.Server{
		Handshake: func(_ *websocket.Config, request *http.Request) error {
			if request.URL.Path != "/backend-api/codex/responses" || request.Header.Get("OpenAI-Beta") != openAIResponsesWebSocketBeta || request.Header.Get("ChatGPT-Account-Id") != "account-ws" {
				t.Errorf("handshake = %s headers=%v", request.URL.Path, request.Header)
			}
			return nil
		},
		Handler: func(conn *websocket.Conn) {
			defer close(done)
			var payload string
			if err := websocket.Message.Receive(conn, &payload); err != nil {
				t.Error(err)
				return
			}
			var request map[string]any
			if err := json.Unmarshal([]byte(payload), &request); err != nil {
				t.Error(err)
				return
			}
			if request["type"] != "response.create" || request["store"] != false {
				t.Errorf("request = %#v", request)
			}
			for _, event := range []string{
				`{"type":"response.output_text.delta","output_index":0,"delta":"codex-ws"}`,
				`{"type":"response.completed","response":{"id":"codex-response","status":"completed"}}`,
			} {
				if err := websocket.Message.Send(conn, event); err != nil {
					t.Error(err)
					return
				}
			}
		},
	})
	defer server.Close()

	provider := newCatalogResponsesProvider("openai-codex", server.URL+"/backend-api", token, "gpt-5.4", true)
	provider.transport = "websocket"
	provider.cacheRetention = "short"
	provider.streamIdleTimeout = 2 * time.Second
	t.Cleanup(provider.websockets.Close)
	completion, err := provider.Complete(context.Background(), ChatRequest{SessionID: "codex-ws", Model: provider.model, Messages: []ChatMessage{{Role: "user", Content: "hello"}}}, func(Delta) error { return nil })
	if err != nil || completion.Text != "codex-ws" {
		t.Fatalf("Complete() = %#v, %v", completion, err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Codex WebSocket server did not finish")
	}
}

func TestBedrockConverseStreamProtocol(t *testing.T) {
	t.Setenv("AWS_BEDROCK_SKIP_AUTH", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret-example")
	t.Setenv("AWS_REGION", "us-west-2")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/model/anthropic.claude-sonnet-4-6/converse-stream" || !strings.Contains(r.Header.Get("Authorization"), "/us-west-2/bedrock/aws4_request") {
			t.Errorf("request = %s auth=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		inference, _ := body["inferenceConfig"].(map[string]any)
		if body["messages"] == nil || body["toolConfig"] == nil || body["additionalModelRequestFields"] == nil || inference["temperature"] != float64(0) {
			t.Errorf("request body = %#v", body)
		}
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		writeBedrockTestEvent(t, w, "messageStart", map[string]any{"role": "assistant"})
		writeBedrockTestEvent(t, w, "contentBlockDelta", map[string]any{"contentBlockIndex": 0, "delta": map[string]any{"reasoningContent": map[string]any{"text": "reason"}}})
		writeBedrockTestEvent(t, w, "contentBlockDelta", map[string]any{"contentBlockIndex": 1, "delta": map[string]any{"text": "bedrock"}})
		writeBedrockTestEvent(t, w, "contentBlockStart", map[string]any{"contentBlockIndex": 2, "start": map[string]any{"toolUse": map[string]any{"toolUseId": "call-1", "name": "lookup"}}})
		writeBedrockTestEvent(t, w, "contentBlockDelta", map[string]any{"contentBlockIndex": 2, "delta": map[string]any{"toolUse": map[string]any{"input": `{"q":"x"}`}}})
		writeBedrockTestEvent(t, w, "contentBlockStop", map[string]any{"contentBlockIndex": 2})
		writeBedrockTestEvent(t, w, "messageStop", map[string]any{"stopReason": "tool_use"})
		writeBedrockTestEvent(t, w, "metadata", map[string]any{"usage": map[string]any{"inputTokens": 10, "outputTokens": 4, "totalTokens": 14}})
	}))
	defer server.Close()

	provider := newBedrockProvider("amazon-bedrock", server.URL, "", "anthropic.claude-sonnet-4-6")
	provider.modelSpec = piAIModel{ID: provider.model, Name: "Claude Sonnet 4.6", Reasoning: true, MaxTokens: 8192}
	completion, err := provider.Complete(context.Background(), ChatRequest{
		Model: provider.model, System: "system", ReasoningEffort: "high", Temperature: float64Pointer(0),
		Messages: []ChatMessage{{Role: "user", Content: "hello"}},
		Tools:    []ToolSchema{{Name: "lookup", Parameters: map[string]any{"type": "object"}}},
	}, func(Delta) error { return nil })
	if err != nil || completion.Text != "bedrock" || completion.Reasoning != "reason" || completion.Finish != "tool_calls" || len(completion.ToolCalls) != 1 {
		t.Fatalf("Complete() = %#v, %v", completion, err)
	}
}

func writeSSEData(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write(append(append([]byte("data: "), data...), '\n', '\n'))
}

func writeResponsesStream(t *testing.T, w http.ResponseWriter, text string) {
	t.Helper()
	w.Header().Set("Content-Type", "text/event-stream")
	writeSSEData(t, w, map[string]any{"type": "response.output_text.delta", "delta": text})
	writeSSEData(t, w, map[string]any{"type": "response.completed", "response": map[string]any{"id": "response-1", "status": "completed", "usage": map[string]any{"input_tokens": 1, "output_tokens": 1}}})
}

func writeBedrockTestEvent(t *testing.T, w http.ResponseWriter, eventType string, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	headers := append(eventStreamStringHeader(":message-type", "event"), eventStreamStringHeader(":event-type", eventType)...)
	total := 16 + len(headers) + len(payload)
	frame := make([]byte, total)
	binary.BigEndian.PutUint32(frame[:4], uint32(total))
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(headers)))
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(frame[:8]))
	copy(frame[12:], headers)
	copy(frame[12+len(headers):], payload)
	binary.BigEndian.PutUint32(frame[total-4:], crc32.ChecksumIEEE(frame[:total-4]))
	_, _ = w.Write(frame)
}

func eventStreamStringHeader(name, value string) []byte {
	out := make([]byte, 1+len(name)+1+2+len(value))
	out[0] = byte(len(name))
	copy(out[1:], name)
	offset := 1 + len(name)
	out[offset] = 7
	binary.BigEndian.PutUint16(out[offset+1:offset+3], uint16(len(value)))
	copy(out[offset+3:], value)
	return out
}

func float64Pointer(value float64) *float64 { return &value }
