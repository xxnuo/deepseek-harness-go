package harness

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDiscoverModelsUsesDraftEndpointAndDoesNotEchoKey(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"draft-model","name":"Draft Model"}]}`))
	}))
	defer server.Close()
	e := newIntegrationEngine(t)
	value, rpcErr := e.discoverModels(context.Background(), map[string]any{
		"settingsNs": "llm-pi-ai",
		"baseURL":    server.URL + "/v1",
		"apiKey":     "probe-secret",
	})
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if gotAuth != "Bearer probe-secret" {
		t.Fatalf("authorization = %q", gotAuth)
	}
	data, _ := json.Marshal(value)
	if strings.Contains(string(data), "probe-secret") {
		t.Fatalf("discovery response leaked the draft key: %s", data)
	}
	models := value.(map[string]any)["models"].([]map[string]any)
	if len(models) != 1 || models[0]["id"] != "draft-model" {
		t.Fatalf("models = %#v", models)
	}
}

func TestDiscoverModelsInheritsProfileHeadersAndCredential(t *testing.T) {
	t.Setenv("DISCOVERY_PROFILE_KEY", "stored-secret")
	requests := make(chan http.Header, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Header.Clone()
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"custom-model"}]}`))
	}))
	defer server.Close()
	e := newIntegrationEngine(t)
	e.mu.Lock()
	e.piAIProviders["custom"] = &managedPiAIProvider{engine: e, profile: piAIProviderProfile{
		route: "custom", apiKeyEnv: "DISCOVERY_PROFILE_KEY",
		headers: map[string]string{"X-Profile": "configured", "Authorization": "Bearer profile-value"},
	}}
	e.mu.Unlock()

	for _, test := range []struct {
		name, draftKey, wantAuth string
	}{
		{name: "stored credential", wantAuth: "Bearer stored-secret"},
		{name: "draft credential wins", draftKey: "draft-secret", wantAuth: "Bearer draft-secret"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, rpcErr := e.discoverModels(t.Context(), map[string]any{
				"settingsNs": "llm-pi-ai", "provider": "custom",
				"baseURL": server.URL, "apiKey": test.draftKey,
			})
			if rpcErr != nil {
				t.Fatal(rpcErr)
			}
			header := <-requests
			if header.Get("X-Profile") != "configured" || header.Get("Authorization") != test.wantAuth {
				t.Fatalf("discovery headers = %#v", header)
			}
		})
	}
}

func TestPiAIProfileRejectsInvalidHTTPHeaders(t *testing.T) {
	for _, headers := range []map[string]any{
		{"Bad Header": "value"},
		{"X-Test": "value\nsmuggled"},
	} {
		_, err := resolvePiAIProfile("custom", map[string]any{
			"api": "openai-completions", "baseURL": "https://example.invalid/v1",
			"headers": headers, "models": []any{map[string]any{"id": "custom-model"}},
		})
		if err == nil || !strings.Contains(err.Error(), "invalid HTTP header") {
			t.Fatalf("resolvePiAIProfile(%#v) = %v", headers, err)
		}
	}
}

func TestDiscoverModelsSupportsOpenAIResponsesListing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"responses-model","display_name":"Responses Model","context_length":131072,"max_output_tokens":32768}]}`))
	}))
	defer server.Close()
	e := newIntegrationEngine(t)
	value, rpcErr := e.discoverModels(context.Background(), map[string]any{
		"settingsNs": "llm-pi-ai",
		"baseURL":    server.URL + "/v1",
		"api":        "openai-responses",
	})
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	models := value.(map[string]any)["models"].([]map[string]any)
	if len(models) != 1 || models[0]["id"] != "responses-model" || models[0]["name"] != "Responses Model" || models[0]["contextWindow"] != 131072 || models[0]["maxTokens"] != 32768 {
		t.Fatalf("models = %#v", models)
	}
}

func TestDiscoverModelsPrefersInstalledPiAICatalog(t *testing.T) {
	e := newIntegrationEngine(t)
	value, rpcErr := e.discoverModels(context.Background(), map[string]any{
		"settingsNs": "llm-pi-ai",
		"provider":   "openai",
		"baseURL":    "http://127.0.0.1:9/v1",
		"api":        "openai-responses",
	})
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	models := value.(map[string]any)["models"].([]map[string]any)
	if len(models) != len(piAICatalog["openai"].Models) {
		t.Fatalf("models = %d, want catalog size %d", len(models), len(piAICatalog["openai"].Models))
	}
}

func TestSettingsOpenDocumentPreparesAndInvokesOpener(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "opened")
	script := filepath.Join(bin, "xdg-open")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' \"$1\" > \""+marker+"\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = dir
	cfg.Workspace = dir
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	value, rpcErr := e.dispatch(context.Background(), "settings.openDocument", nil)
	if rpcErr != nil || value.(map[string]any)["opened"] != true {
		t.Fatalf("openDocument = %#v, %v", value, rpcErr)
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if data, readErr := os.ReadFile(marker); readErr == nil {
			if string(data) == filepath.Join(dir, "settings.yaml") {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("opener was not invoked")
}

func TestDeepSeekDefaultCatalogIncludesVisionModel(t *testing.T) {
	models := deepSeekCatalog(nil)
	if len(models) != 3 {
		t.Fatalf("default models = %#v", models)
	}
	vision := models[2]
	if vision.ID != "deepseek-v4-flash-vision-exp" || strings.Join(vision.InputModalities, ",") != "text,image" {
		t.Fatalf("vision model = %#v", vision)
	}
}
