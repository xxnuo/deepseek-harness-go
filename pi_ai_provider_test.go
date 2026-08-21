package harness

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

func piAITestProfile(baseURL string) map[string]any {
	return map[string]any{
		"displayName": "Local Gateway",
		"apiKeyEnv":   "LOCAL_GATEWAY_KEY",
		"api":         "openai-completions",
		"baseURL":     baseURL,
		"headers":     map[string]any{"X-Harness-Route": "local"},
		"models": []any{map[string]any{
			"id": "local-model", "name": "Local Model", "contextWindow": 65536,
			"maxTokens": 4096, "input": []any{"text", "image"},
		}},
		"retryPolicy": map[string]any{
			"mode": "normal", "maxRetries": 0,
			"backoff": map[string]any{"initialDelayMs": 1, "maxDelayMs": 1, "jitterRatio": 0},
		},
	}
}

func TestPiAIAlwaysRetryIgnoresInactiveNormalFields(t *testing.T) {
	policy, err := resolvePiAIRetryPolicy(map[string]any{
		"mode":           "always",
		"maxRetries":     "ignored",
		"retryableCodes": []any{},
	}, "route")
	if err != nil {
		t.Fatal(err)
	}
	if policy.Mode != RetryAlways || policy.MaxRetries != 0 {
		t.Fatalf("policy = %#v", policy)
	}
}

func TestPiAIStreamIdleTimeoutAcceptsPositiveFiniteMilliseconds(t *testing.T) {
	profile := piAITestProfile("https://example.test/v1")
	profile["streamIdleTimeoutMs"] = 12.5
	resolved, err := resolvePiAIProfile("local-gateway", profile)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.streamIdleTimeout != 12*time.Millisecond+500*time.Microsecond {
		t.Fatalf("stream idle timeout = %s", resolved.streamIdleTimeout)
	}
}

func TestPiAIProviderAppliesConfiguredImageBudget(t *testing.T) {
	t.Setenv("LOCAL_GATEWAY_KEY", "budget-key")
	var request struct {
		Messages []openAIWireMessage `json:"messages"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()

	e := newIntegrationEngine(t)
	profile := piAITestProfile(server.URL)
	profile["maxRequestImageBytes"] = 4
	resolved, err := resolvePiAIProfile("local-gateway", profile)
	if err != nil {
		t.Fatal(err)
	}
	provider := &managedPiAIProvider{engine: e, profile: resolved, websockets: newOpenAIResponsesWebSocketPool()}
	completion, err := provider.Complete(context.Background(), ChatRequest{
		Model: "local-model",
		Messages: []ChatMessage{
			{Role: "user", Content: "old", Images: []ChatImage{{MediaType: "image/png", Data: "AAAA"}}},
			{Role: "user", Content: "new", Images: []ChatImage{{MediaType: "image/png", Data: "BBBB"}}},
		},
	}, func(Delta) error { return nil })
	if err != nil || completion.Text != "ok" {
		t.Fatalf("Complete = %#v, %v", completion, err)
	}
	first, ok := request.Messages[0].Content.(string)
	if !ok || !strings.Contains(first, OffloadedImageText) {
		t.Fatalf("old image was not replaced: %#v", request.Messages)
	}
	encoded, err := json.Marshal(request.Messages[1].Content)
	if err != nil || !strings.Contains(string(encoded), "data:image/png;base64,BBBB") {
		t.Fatalf("new image was not retained: %s, %v", encoded, err)
	}
}

func TestPiAIProfileUsesRC8CatalogFallbacksAndConfiguredRequestCap(t *testing.T) {
	profile, err := resolvePiAIProfile("openai", map[string]any{
		"models":         []any{},
		"modelOverrides": map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if profile.displayName != "openai" || len(profile.models) != len(piAICatalog["openai"].Models) {
		t.Fatalf("catalog profile = name %q, models %d", profile.displayName, len(profile.models))
	}

	configured, err := resolvePiAIProfile("custom", map[string]any{
		"api": "openai-completions", "baseURL": "https://example.test/v1",
		"defaultInput": []any{"text", "image"},
		"models":       []any{map[string]any{"id": "model", "maxTokens": 77, "input": []any{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(configured.models[0].Input, []string{"text", "image"}) || configured.configuredMaxTokens["model"] != 77 {
		t.Fatalf("configured profile = %#v, caps %#v", configured.models[0], configured.configuredMaxTokens)
	}
}

func TestPiAIProviderRejectsUnsupportedImageUse(t *testing.T) {
	t.Setenv("LOCAL_GATEWAY_KEY", "image-key")
	e := newIntegrationEngine(t)
	tests := []struct {
		name    string
		profile map[string]any
		message ChatMessage
		want    string
	}{
		{
			name: "text only model", profile: map[string]any{
				"apiKeyEnv": "LOCAL_GATEWAY_KEY", "api": "openai-completions", "baseURL": "https://example.test/v1",
				"models": []any{map[string]any{"id": "text-model", "input": []any{"text"}}},
			},
			message: ChatMessage{Role: "user", Images: []ChatImage{{MediaType: "image/png", Data: "AAAA"}}},
			want:    "does not support image input",
		},
		{
			name: "assistant role", profile: map[string]any{
				"apiKeyEnv": "LOCAL_GATEWAY_KEY", "api": "openai-completions", "baseURL": "https://example.test/v1",
				"models": []any{map[string]any{"id": "vision-model", "input": []any{"text", "image"}}},
			},
			message: ChatMessage{Role: "assistant", Images: []ChatImage{{MediaType: "image/png", Data: "AAAA"}}},
			want:    "in-history assistant message",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile, err := resolvePiAIProfile("route", test.profile)
			if err != nil {
				t.Fatal(err)
			}
			_, err = (&managedPiAIProvider{engine: e, profile: profile, websockets: newOpenAIResponsesWebSocketPool()}).Complete(
				context.Background(), ChatRequest{Model: profile.models[0].ID, Messages: []ChatMessage{test.message}}, func(Delta) error { return nil },
			)
			var providerErr *ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != "UNSUPPORTED_CONTENT" || !strings.Contains(providerErr.Message, test.want) {
				t.Fatalf("error = %#v", err)
			}
		})
	}
}

func TestPiAISettingsSchemaExposesRC8CompatAndImageLimit(t *testing.T) {
	refs := piAISettingsSchema()["refs"].(map[string]any)
	profile := refs["12"].(map[string]any)["dict"].(map[string]any)
	compat := refs["19"].(map[string]any)["dict"].(map[string]any)
	for _, field := range []string{"maxRequestImageBytes", "defaultInput", "compat"} {
		if profile[field] == nil {
			t.Fatalf("profile schema is missing %q", field)
		}
	}
	for _, field := range []string{
		"supportsDeveloperRole", "requiresToolResultName", "requiresAssistantAfterToolResult",
		"requiresThinkingAsText", "chatTemplateKwargs", "supportsStrictMode",
		"supportsEagerToolInputStreaming", "supportsStrictTools",
	} {
		if compat[field] == nil {
			t.Fatalf("compat schema is missing %q", field)
		}
	}
}

func TestPiAISettingsLifecycleUsesLiveCredentialAndUIAddress(t *testing.T) {
	t.Setenv("LOCAL_GATEWAY_KEY", "")
	type requestView struct {
		authorization string
		routeHeader   string
		model         string
	}
	requests := make(chan requestView, 2)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode provider request: %v", err)
		}
		requests <- requestView{authorization: r.Header.Get("Authorization"), routeHeader: r.Header.Get("X-Harness-Route"), model: stringSetting(body["model"])}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"configured\"},\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer provider.Close()

	e := newIntegrationEngine(t)
	initial := e.providerViews()
	deepseek := slices.IndexFunc(initial, func(row map[string]any) bool { return row["provider"] == "deepseek" })
	if deepseek < 0 || initial[deepseek]["active"] != false || initial[deepseek]["settingsNs"] != piAISettingsNamespace {
		t.Fatalf("initial pi-ai directory = %#v", initial)
	}

	updated, rpcErr := e.settingsMutate(piAISettingsNamespace, []any{map[string]any{
		"op": "set", "path": []any{"providers", "local-gateway"}, "value": piAITestProfile(provider.URL + "/v1"),
	}}, nil)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	view := updated.(map[string]any)
	if view["applies"] != "live" || view["revision"] != 1 {
		t.Fatalf("settings view = %#v", view)
	}
	refs := view["schema"].(map[string]any)["refs"].(map[string]any)
	apiUnion := refs["4"].(map[string]any)
	if !slices.Equal(apiUnion["list"].([]any), []any{3, 17, 18}) ||
		refs["3"].(map[string]any)["value"] != "openai-completions" ||
		refs["17"].(map[string]any)["value"] != "openai-responses" ||
		refs["18"].(map[string]any)["value"] != "anthropic-messages" {
		t.Fatalf("protocol schema = %#v", apiUnion)
	}
	rows := e.providerViews()
	index := slices.IndexFunc(rows, func(row map[string]any) bool { return row["provider"] == "local-gateway" })
	if index < 0 || rows[index]["active"] != true || rows[index]["declared"] != true {
		t.Fatalf("configured provider directory = %#v", rows)
	}
	if path, ok := rows[index]["settingsPath"].([]string); !ok || !slices.Equal(path, []string{"providers", "local-gateway"}) {
		t.Fatalf("settingsPath = %#v", rows[index]["settingsPath"])
	}

	if rpcErr := e.setCredential("LOCAL_GATEWAY_KEY", "key-one"); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "pi-ai-live", "minimal")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(sessionID, ModelSelection{Provider: "local-gateway", Model: "local-model"}); err != nil {
		t.Fatal(err)
	}
	for index, key := range []string{"key-one", "key-two"} {
		if index == 1 {
			if rpcErr := e.setCredential("LOCAL_GATEWAY_KEY", key); rpcErr != nil {
				t.Fatal(rpcErr)
			}
		}
		text, runErr := e.Run(context.Background(), sessionID, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "hello"}}})
		if runErr != nil || text != "configured" {
			t.Fatalf("Run(%d) = %q, %v", index, text, runErr)
		}
		select {
		case request := <-requests:
			if request.authorization != "Bearer "+key || request.routeHeader != "local" || request.model != "local-model" {
				t.Fatalf("provider request %d = %#v", index, request)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("provider request %d did not arrive", index)
		}
	}

	if _, rpcErr := e.settingsMutate(piAISettingsNamespace, []any{map[string]any{
		"op": "unset", "path": []any{"providers", "local-gateway"},
	}}, nil); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if err := e.SelectModel(sessionID, ModelSelection{Provider: "local-gateway", Model: "local-model"}); err == nil {
		t.Fatal("removed pi-ai route remained selectable")
	}
}

func TestPiAISettingsRejectInvalidSwapAndKeepLastRoute(t *testing.T) {
	e := newIntegrationEngine(t)
	if _, rpcErr := e.settingsMutate(piAISettingsNamespace, []any{map[string]any{
		"op": "set", "path": []any{"providers", "local-gateway"}, "value": piAITestProfile("https://gateway.example/v1"),
	}}, nil); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	e.mu.RLock()
	before := e.providers["local-gateway"]
	revision := e.settingsRev[piAISettingsNamespace]
	e.mu.RUnlock()

	_, rejected := e.settingsMutate(piAISettingsNamespace, []any{map[string]any{
		"op": "set", "path": []any{"providers", "local-gateway", "api"}, "value": "unsupported",
	}}, nil)
	if rejected == nil || rejected.Code != "settings-rejected" || !strings.Contains(rejected.Message, "unsupported api") {
		t.Fatalf("unsupported protocol rejection = %#v", rejected)
	}
	_, collision := e.settingsMutate(piAISettingsNamespace, []any{map[string]any{
		"op": "set", "path": []any{"providers", "echo"}, "value": piAITestProfile("https://gateway.example/v1"),
	}}, nil)
	if collision == nil || collision.Code != "settings-rejected" || !strings.Contains(collision.Message, "already registered") {
		t.Fatalf("route collision rejection = %#v", collision)
	}
	e.mu.RLock()
	after := e.providers["local-gateway"]
	afterRevision := e.settingsRev[piAISettingsNamespace]
	storedAPI := e.settings[piAISettingsNamespace]["providers"].(map[string]any)["local-gateway"].(map[string]any)["api"]
	e.mu.RUnlock()
	if after != before || afterRevision != revision || storedAPI != "openai-completions" {
		t.Fatalf("rejected swap changed live state: provider=%v revision=%d api=%v", after != before, afterRevision, storedAPI)
	}
}

func TestPiAISettingsPersistAndRestoreRoute(t *testing.T) {
	t.Setenv("LOCAL_GATEWAY_KEY", "")
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dir, dir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if _, rpcErr := e.settingsMutate(piAISettingsNamespace, []any{map[string]any{
		"op": "set", "path": []any{"providers", "local-gateway"}, "value": piAITestProfile("https://gateway.example/v1"),
	}}, nil); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	reloaded, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reloaded.Close() })
	reloaded.mu.RLock()
	provider := reloaded.piAIProviders["local-gateway"]
	reloaded.mu.RUnlock()
	if provider == nil || provider.Name() != "Local Gateway" {
		t.Fatalf("reloaded pi-ai route = %#v", provider)
	}
}
