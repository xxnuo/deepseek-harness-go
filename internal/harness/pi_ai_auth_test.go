package harness

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func piAIAuthTestEngine(t *testing.T, dataDir string, persist bool) *Engine {
	t.Helper()
	e, err := New(
		WithDataDir(dataDir),
		WithPersistence(persist),
		WithWorkspace(t.TempDir()),
		WithProvider("echo"),
		WithModel("echo"),
		WithSessionTitleLLM(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func piAIAuthManagedProvider(e *Engine, route string, model piAIModel) *managedPiAIProvider {
	return &managedPiAIProvider{
		engine: e,
		profile: piAIProviderProfile{
			route: route, displayName: route, models: []piAIModel{model}, modelByID: map[string]piAIModel{model.ID: model},
			configuredModel: model.ID, configuredMaxTokens: map[string]int{model.ID: model.MaxTokens}, maxRequestImageBytes: DefaultMaxRequestImageBytes,
		},
		websockets: newOpenAIResponsesWebSocketPool(),
	}
}

func writeAnthropicAuthTestSSE(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(w, "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n")
}

func TestPiAIAuthOAuthRefreshIsSingleFlightPersistedAndUsed(t *testing.T) {
	var refreshes atomic.Int32
	requests := make(chan http.Header, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/oauth/token":
			refreshes.Add(1)
			if err := r.ParseForm(); err != nil || r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != "refresh-old" {
				t.Errorf("refresh form = %#v, %v", r.Form, err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":"access-new","refresh_token":"refresh-new","expires_in":3600}`)
		case "/coding/v1/messages":
			requests <- r.Header.Clone()
			writeAnthropicAuthTestSSE(w)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("KIMI_CODE_OAUTH_HOST", server.URL)

	dataDir := filepath.Join(t.TempDir(), "state")
	e := piAIAuthTestEngine(t, dataDir, true)
	key := CredentialKey("llm-pi-ai/kimi-coding")
	if _, err := e.Credentials().ModifyRecord(t.Context(), key, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
		return &CredentialRecord{Kind: CredentialRecordGrant, Payload: map[string]any{
			"type": "oauth", "access": "access-old", "refresh": "refresh-old", "expires": time.Now().Add(-time.Minute).UnixMilli(),
		}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	model := piAIModel{ID: "kimi-for-coding", Name: "Kimi", API: "anthropic-messages", BaseURL: server.URL + "/coding", Input: []string{"text"}, MaxTokens: 1024}
	provider := piAIAuthManagedProvider(e, "kimi-coding", model)

	var group sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := provider.Complete(context.Background(), ChatRequest{Model: model.ID, Messages: []ChatMessage{{Role: "user", Content: "hello"}}}, func(Delta) error { return nil })
			errs <- err
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := refreshes.Load(); got != 1 {
		t.Fatalf("refresh requests = %d, want 1", got)
	}
	for range 2 {
		header := <-requests
		if header.Get("Authorization") != "Bearer access-new" || header.Get("X-Api-Key") != "" {
			t.Fatalf("provider auth headers = %#v", header)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	reloaded := piAIAuthTestEngine(t, dataDir, true)
	defer reloaded.Close()
	record, err := reloaded.Credentials().ReadRecord(key)
	if err != nil {
		t.Fatal(err)
	}
	payload, ok := piAIOAuthPayload(record)
	if !ok || payload["access"] != "access-new" || payload["refresh"] != "refresh-new" {
		t.Fatalf("persisted credential = %#v", record)
	}
}

func TestPiAIAuthCloudflareExpandsEndpointAndOwnsHeaders(t *testing.T) {
	request := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request <- r.Clone(r.Context())
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()
	e := piAIAuthTestEngine(t, t.TempDir(), false)
	defer e.Close()
	key := CredentialKey("llm-pi-ai/cloudflare-ai-gateway")
	if _, err := e.Credentials().ModifyRecord(t.Context(), key, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
		return &CredentialRecord{Kind: CredentialRecordAPIKey, Key: "cf-secret", Env: map[string]string{
			"CLOUDFLARE_ACCOUNT_ID": "account-1", "CLOUDFLARE_GATEWAY_ID": "gateway-1",
		}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	model := piAIModel{
		ID: "cf-model", Name: "Cloudflare", API: "openai-completions",
		BaseURL: server.URL + "/{CLOUDFLARE_ACCOUNT_ID}/{CLOUDFLARE_GATEWAY_ID}", Input: []string{"text"}, MaxTokens: 1024,
		Headers: map[string]string{"Authorization": "Bearer wrong", "X-Api-Key": "wrong"},
	}
	provider := piAIAuthManagedProvider(e, "cloudflare-ai-gateway", model)
	if _, err := provider.Complete(t.Context(), ChatRequest{Model: model.ID, Messages: []ChatMessage{{Role: "user", Content: "hello"}}}, func(Delta) error { return nil }); err != nil {
		t.Fatal(err)
	}
	got := <-request
	if got.URL.Path != "/account-1/gateway-1/chat/completions" {
		t.Fatalf("request path = %q", got.URL.Path)
	}
	if got.Header.Get("Cf-Aig-Authorization") != "Bearer cf-secret" || got.Header.Get("Authorization") != "" || got.Header.Get("X-Api-Key") != "" {
		t.Fatalf("request headers = %#v", got.Header)
	}
}

func TestPiAIAuthAnthropicOAuthUsesClaudeCodeWireAuth(t *testing.T) {
	type captured struct {
		header http.Header
		body   map[string]any
	}
	requests := make(chan captured, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		requests <- captured{header: r.Header.Clone(), body: body}
		writeAnthropicAuthTestSSE(w)
	}))
	defer server.Close()
	e := piAIAuthTestEngine(t, t.TempDir(), false)
	defer e.Close()
	key := CredentialKey("llm-pi-ai/anthropic")
	if _, err := e.Credentials().ModifyRecord(t.Context(), key, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
		return &CredentialRecord{Kind: CredentialRecordGrant, Payload: map[string]any{
			"type": "oauth", "access": "sk-ant-oat-live", "refresh": "refresh", "expires": time.Now().Add(time.Hour).UnixMilli(),
		}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	model := piAIModel{ID: "claude-test", Name: "Claude", API: "anthropic-messages", BaseURL: server.URL, Input: []string{"text"}, MaxTokens: 1024}
	provider := piAIAuthManagedProvider(e, "anthropic", model)
	if _, err := provider.Complete(t.Context(), ChatRequest{Model: model.ID, System: "project instructions", Messages: []ChatMessage{{Role: "user", Content: "hello"}}}, func(Delta) error { return nil }); err != nil {
		t.Fatal(err)
	}
	got := <-requests
	if got.header.Get("Authorization") != "Bearer sk-ant-oat-live" || got.header.Get("X-Api-Key") != "" {
		t.Fatalf("auth headers = %#v", got.header)
	}
	if !strings.Contains(got.header.Get("Anthropic-Beta"), "claude-code-20250219") || !strings.Contains(got.header.Get("Anthropic-Beta"), "oauth-2025-04-20") || got.header.Get("X-App") != "cli" {
		t.Fatalf("Claude Code headers = %#v", got.header)
	}
	system, _ := got.body["system"].([]any)
	first, _ := system[0].(map[string]any)
	if len(system) != 2 || first["text"] != "You are Claude Code, Anthropic's official CLI for Claude." {
		t.Fatalf("OAuth system = %#v", got.body["system"])
	}
}

func TestPiAIAuthExplicitReferencePrecedesStoredAndAmbient(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "ambient")
	e := piAIAuthTestEngine(t, t.TempDir(), false)
	defer e.Close()
	if err := e.Credentials().Set(t.Context(), CredentialRef("ROUTE_KEY"), "explicit"); err != nil {
		t.Fatal(err)
	}
	key := CredentialKey("llm-pi-ai/openai")
	if _, err := e.Credentials().ModifyRecord(t.Context(), key, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
		return &CredentialRecord{Kind: CredentialRecordAPIKey, Key: "stored"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	resolution, err := e.resolvePiAIAuth(t.Context(), "openai", "ROUTE_KEY", "https://api.openai.com/v1")
	if err != nil || resolution.APIKey != "explicit" {
		t.Fatalf("resolution = %#v, %v", resolution, err)
	}
}

func TestPiAIAuthRejectsInvalidStoredKeyWithoutAmbientFallback(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "ambient")
	e := piAIAuthTestEngine(t, t.TempDir(), false)
	defer e.Close()
	key := CredentialKey("llm-pi-ai/openai")
	if _, err := e.Credentials().ModifyRecord(t.Context(), key, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
		return &CredentialRecord{Kind: CredentialRecordAPIKey, Key: "broken\nkey"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	resolution, err := e.resolvePiAIAuth(t.Context(), "openai", "", "https://api.openai.com/v1")
	if err == nil || !strings.Contains(err.Error(), "invalid HTTP header characters") {
		t.Fatalf("resolution = %#v, error = %v", resolution, err)
	}
}

func TestPiAIInteractiveAPIKeyFlowPersistsAndAuthenticatesRequest(t *testing.T) {
	requests := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Header.Clone()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()

	e := piAIAuthTestEngine(t, t.TempDir(), false)
	defer e.Close()
	key := CredentialKey("llm-pi-ai/openai")
	var prompts []AuthorizationPrompt
	outcome, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{
		Key: key, Method: "api-key",
		Interaction: AuthorizationInteraction{Prompt: func(_ context.Context, prompt AuthorizationPrompt) (string, error) {
			prompts = append(prompts, prompt)
			return "interactive-openai-key", nil
		}},
	})
	if err != nil || outcome.Status != AuthorizationAuthorized {
		t.Fatalf("API-key authorization = %#v, %v", outcome, err)
	}
	if len(prompts) != 1 || prompts[0].Kind != AuthorizationPromptSecret || !strings.Contains(prompts[0].Message, "OpenAI API key") {
		t.Fatalf("API-key prompts = %#v", prompts)
	}
	record, err := e.Credentials().ReadRecord(key)
	if err != nil || record == nil || record.Kind != CredentialRecordAPIKey || record.Key != "interactive-openai-key" {
		t.Fatalf("API-key credential = %#v, %v", record, err)
	}

	model := piAIModel{ID: "interactive-openai", Name: "OpenAI", API: "openai-completions", BaseURL: server.URL, Input: []string{"text"}, MaxTokens: 1024}
	provider := piAIAuthManagedProvider(e, "openai", model)
	if _, err := provider.Complete(t.Context(), ChatRequest{Model: model.ID, Messages: []ChatMessage{{Role: "user", Content: "hello"}}}, func(Delta) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if header := <-requests; header.Get("Authorization") != "Bearer interactive-openai-key" {
		t.Fatalf("API-key request headers = %#v", header)
	}
}

func TestPiAIInteractiveKimiOAuthDeviceFlowPersistsAndAuthenticatesRequest(t *testing.T) {
	oldInterval := piAIOAuthMinimumPollInterval
	piAIOAuthMinimumPollInterval = time.Millisecond
	t.Cleanup(func() { piAIOAuthMinimumPollInterval = oldInterval })

	requests := make(chan http.Header, 1)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/oauth/device_authorization":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"device_code":"device-1","user_code":"KIMI-1234","verification_uri":"`+server.URL+`/verify","verification_uri_complete":"`+server.URL+`/verify?code=KIMI-1234","interval":0.001,"expires_in":60}`)
		case "/api/oauth/token":
			if err := r.ParseForm(); err != nil || r.Form.Get("device_code") != "device-1" || r.Form.Get("grant_type") != piAIOAuthDeviceGrant {
				t.Errorf("Kimi token form = %#v, %v", r.Form, err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":"kimi-access","refresh_token":"kimi-refresh","expires_in":3600}`)
		case "/coding/v1/messages":
			requests <- r.Header.Clone()
			writeAnthropicAuthTestSSE(w)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("KIMI_CODE_OAUTH_HOST", server.URL)

	e := piAIAuthTestEngine(t, t.TempDir(), false)
	defer e.Close()
	key := CredentialKey("llm-pi-ai/kimi-coding")
	var notices []AuthorizationNotice
	outcome, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{
		Key: key, Method: "oauth",
		Interaction: AuthorizationInteraction{Notify: func(notice AuthorizationNotice) { notices = append(notices, notice) }},
	})
	if err != nil || outcome.Status != AuthorizationAuthorized {
		t.Fatalf("Kimi OAuth authorization = %#v, %v", outcome, err)
	}
	if len(notices) != 1 || notices[0].Code != "KIMI-1234" || notices[0].URL != server.URL+"/verify?code=KIMI-1234" {
		t.Fatalf("Kimi OAuth notices = %#v", notices)
	}
	record, err := e.Credentials().ReadRecord(key)
	payload, ok := piAIOAuthPayload(record)
	if err != nil || !ok || payload["access"] != "kimi-access" || payload["refresh"] != "kimi-refresh" {
		t.Fatalf("Kimi OAuth credential = %#v, %v", record, err)
	}

	model := piAIModel{ID: "kimi-interactive", Name: "Kimi", API: "anthropic-messages", BaseURL: server.URL + "/coding", Input: []string{"text"}, MaxTokens: 1024}
	provider := piAIAuthManagedProvider(e, "kimi-coding", model)
	if _, err := provider.Complete(t.Context(), ChatRequest{Model: model.ID, Messages: []ChatMessage{{Role: "user", Content: "hello"}}}, func(Delta) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if header := <-requests; header.Get("Authorization") != "Bearer kimi-access" || header.Get("X-Api-Key") != "" {
		t.Fatalf("Kimi OAuth request headers = %#v", header)
	}
}
