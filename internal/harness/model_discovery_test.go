package harness

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestDiscoveredModelsListingFormats(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want []ModelInfo
		fail bool
	}{
		{"gateway aliases", `{"models":{"alias":{"id":"canonical","displayName":"Display","limit":{"context":131072,"output":8192}},"":{"id":"fallback"},"metadata":4,"invalid":[],"null":null}}`, []ModelInfo{{ID: "alias", Name: "Display", ContextWindow: 131072, MaxTokens: 8192}, {ID: "fallback", Name: "fallback"}}, false},
		{"array precedence", `{"data":[],"models":{"ignored":{}}}`, []ModelInfo{}, false},
		{"capacity precedence", `{"data":[{"id":"m","contextWindow":100,"context_window":200,"maxOutputTokens":300,"max_tokens":400}]}`, []ModelInfo{{ID: "m", Name: "m", ContextWindow: 100, MaxTokens: 300}}, false},
		{"invalid fields skipped", `{"data":[null,5,[],{"id":4},{"id":"m","name":4,"contextWindow":0.5,"context_window":"100","max_input_tokens":100,"max_tokens":-1,"top_provider":{"max_completion_tokens":200}}]}`, []ModelInfo{{ID: "m", Name: "m", ContextWindow: 100, MaxTokens: 200}}, false},
		{"duplicate rows retained", `{"data":[{"id":"m"},{"id":"m"}]}`, []ModelInfo{{ID: "m", Name: "m"}, {ID: "m", Name: "m"}}, false},
		{"missing listing", `{}`, nil, true},
		{"null listing", `null`, nil, true},
		{"array map rejected", `{"models":[]}`, nil, true},
		{"invalid json", `{"data":[]`, nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			models, err := parseDiscoveredModels([]byte(test.body))
			if (err != nil) != test.fail || !reflect.DeepEqual(models, test.want) {
				t.Fatalf("models = %#v, err = %v; want %#v, fail %v", models, err, test.want, test.fail)
			}
		})
	}
}

func TestDiscoverModelsAnthropicNativeListing(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if request.URL.String() != "/gateway/v1/models?limit=1000" || request.Header.Get("x-api-key") != "draft" || request.Header.Get("anthropic-version") != "2023-06-01" || request.Header.Get("Authorization") != "" {
			t.Errorf("request = %s, headers = %#v", request.URL, request.Header)
		}
		_, _ = writer.Write([]byte(`{"data":[{"id":"claude-custom","display_name":"Custom"}],"has_more":true,"last_id":"not-followed"}`))
	}))
	defer server.Close()
	engine := newIntegrationEngine(t)
	for _, suffix := range []string{"/gateway", "/gateway/", "/gateway/v1", "/gateway/v1///"} {
		value, rpcErr := engine.discoverModels(t.Context(), map[string]any{
			"settingsNs": "llm-pi-ai", "api": "anthropic-messages", "apiKey": "draft", "baseURL": server.URL + suffix,
		})
		if rpcErr != nil {
			t.Fatal(rpcErr)
		}
		models := value.(map[string]any)["models"].([]map[string]any)
		if len(models) != 1 || models[0]["id"] != "claude-custom" {
			t.Fatalf("models = %#v", models)
		}
	}
	if requests != 4 {
		t.Fatalf("requests = %d, want one page per discovery", requests)
	}
}

func TestDiscoverModelsBoundAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(strings.Repeat(" ", (4<<20)+1)))
	}))
	defer server.Close()
	provider := NewOpenAIProvider("custom", server.URL, "", "")
	if _, err := provider.Models(t.Context()); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized response error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := provider.discoverModels(ctx, "anthropic-messages"); err == nil {
		t.Fatal("cancelled discovery succeeded")
	}
}
