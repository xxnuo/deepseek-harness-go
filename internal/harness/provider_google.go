package harness

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type googleProvider struct {
	id, baseURL, apiKey, model string
	client                     *http.Client
	headers                    map[string]string
	modelSpec                  piAIModel
	thinkingBudgets            map[string]int
	vertex                     bool
	streamIdleTimeout          time.Duration
	env                        map[string]string
}

func newGoogleProvider(id, baseURL, apiKey, model string, vertex bool) *googleProvider {
	return &googleProvider{
		id: id, baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, model: model,
		client: &http.Client{}, vertex: vertex,
	}
}

func (p *googleProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	endpoint, authHeader, apiKey, err := p.endpointAndAuth(ctx, req.Model)
	if err != nil {
		return Completion{}, err
	}
	body := p.requestBody(req)
	data, err := json.Marshal(body)
	if err != nil {
		return Completion{}, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return Completion{}, err
	}
	hreq.Header.Set("Accept", "text/event-stream")
	hreq.Header.Set("Content-Type", "application/json")
	for name, value := range p.headers {
		hreq.Header.Set(name, value)
	}
	if authHeader != "" {
		hreq.Header.Set("Authorization", authHeader)
	}
	if apiKey != "" {
		hreq.Header.Set("X-Goog-Api-Key", apiKey)
	}
	resp, err := p.client.Do(hreq)
	if err != nil {
		if ctx.Err() != nil {
			return Completion{}, ctx.Err()
		}
		return Completion{}, &ProviderError{Code: "TRANSPORT", Message: err.Error(), Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return Completion{}, providerHTTPFailure(resp, string(payload))
	}

	state := googleStreamState{onDelta: onDelta, usage: map[string]any{}}
	err = scanProviderSSE(ctx, resp.Body, p.streamIdleTimeout, func(line string) error {
		if line == "" || !strings.HasPrefix(line, "data:") {
			return nil
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			return nil
		}
		return state.handle([]byte(payload))
	})
	if err != nil {
		return Completion{}, err
	}
	return state.completion()
}

func (p *googleProvider) endpointAndAuth(ctx context.Context, model string) (string, string, string, error) {
	if model == "" {
		model = p.model
	}
	if !p.vertex {
		key := strings.TrimSpace(p.apiKey)
		if key == "" {
			key = piAIRequestEnv(p.env, "GEMINI_API_KEY")
		}
		if key == "" {
			return "", "", "", &ProviderError{Code: "MISSING_CREDENTIAL", Message: "Google Generative AI requires GEMINI_API_KEY or apiKeyEnv"}
		}
		base := p.baseURL
		if base == "" {
			base = "https://generativelanguage.googleapis.com/v1beta"
		}
		return strings.TrimRight(base, "/") + "/models/" + url.PathEscape(model) + ":streamGenerateContent?alt=sse", "", key, nil
	}

	project := firstNonBlank(piAIRequestEnv(p.env, "GOOGLE_CLOUD_PROJECT"), piAIRequestEnv(p.env, "GCLOUD_PROJECT"))
	location := piAIRequestEnv(p.env, "GOOGLE_CLOUD_LOCATION")
	if project == "" || location == "" {
		return "", "", "", &ProviderError{Code: "MISSING_CREDENTIAL", Message: "Google Vertex AI requires GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_LOCATION"}
	}
	base := p.baseURL
	if base == "" || strings.Contains(base, "{location}") {
		base = strings.ReplaceAll(firstNonBlank(base, "https://{location}-aiplatform.googleapis.com"), "{location}", location)
	}
	endpoint := strings.TrimRight(base, "/") + "/v1/projects/" + url.PathEscape(project) + "/locations/" + url.PathEscape(location) + "/publishers/google/models/" + url.PathEscape(model) + ":streamGenerateContent?alt=sse"
	key := strings.TrimSpace(p.apiKey)
	if key == "" {
		key = piAIRequestEnv(p.env, "GOOGLE_CLOUD_API_KEY")
	}
	if key != "" && !strings.HasPrefix(key, "<") {
		return endpoint, "", key, nil
	}
	token, err := googleADCAccessToken(ctx, p.client, p.env)
	if err != nil {
		return "", "", "", err
	}
	return endpoint, "Bearer " + token, "", nil
}

func (p *googleProvider) requestBody(req ChatRequest) map[string]any {
	body := map[string]any{"contents": googleContents(req.Messages, p.modelSpec)}
	if req.System != "" {
		body["systemInstruction"] = map[string]any{"parts": []any{map[string]any{"text": req.System}}}
	}
	if len(req.Tools) > 0 {
		declarations := make([]any, 0, len(req.Tools))
		for _, tool := range req.Tools {
			declarations = append(declarations, map[string]any{
				"name": tool.Name, "description": tool.Description, "parametersJsonSchema": tool.Parameters,
			})
		}
		body["tools"] = []any{map[string]any{"functionDeclarations": declarations}}
	}
	generation := map[string]any{}
	if req.Temperature != nil {
		generation["temperature"] = *req.Temperature
	}
	if req.MaxTokens > 0 {
		generation["maxOutputTokens"] = req.MaxTokens
	}
	if p.modelSpec.Reasoning {
		generation["thinkingConfig"] = googleThinkingConfig(p.modelSpec, req.ReasoningEffort, p.thinkingBudgets)
	}
	if len(generation) > 0 {
		body["generationConfig"] = generation
	}
	return body
}

func googleContents(messages []ChatMessage, model piAIModel) []any {
	contents := make([]any, 0, len(messages))
	toolNames := map[string]string{}
	for _, message := range messages {
		if message.Role == "assistant" {
			for _, call := range message.ToolCalls {
				toolNames[call.ID] = call.Name
			}
		}
	}
	for _, message := range messages {
		parts := make([]any, 0, len(chatContentParts(message))+len(message.ToolCalls))
		switch message.Role {
		case "user", "system":
			for _, part := range chatContentParts(message) {
				if part.Type == "text" {
					parts = append(parts, map[string]any{"text": part.Text})
				} else if part.Type == "image" {
					parts = append(parts, map[string]any{"inlineData": map[string]any{"mimeType": part.MediaType, "data": part.Data}})
				}
			}
			if len(parts) > 0 {
				contents = append(contents, map[string]any{"role": "user", "parts": parts})
			}
		case "assistant":
			if message.Reasoning != "" {
				parts = append(parts, map[string]any{"thought": true, "text": message.Reasoning})
			}
			if message.Content != "" {
				parts = append(parts, map[string]any{"text": message.Content})
			}
			for _, call := range message.ToolCalls {
				arguments := map[string]any{}
				if len(call.Arguments) > 0 {
					_ = json.Unmarshal(call.Arguments, &arguments)
				}
				parts = append(parts, map[string]any{"functionCall": map[string]any{"id": call.ID, "name": call.Name, "args": arguments}})
			}
			if len(parts) > 0 {
				contents = append(contents, map[string]any{"role": "model", "parts": parts})
			}
		case "tool":
			output := message.Content
			if output == "" && chatMessageHasImage(message) {
				output = "(see attached image)"
			} else if output == "" {
				output = "(no output)"
			}
			name := toolNames[message.ToolCallID]
			if name == "" {
				name = "tool"
			}
			response := map[string]any{"id": message.ToolCallID, "name": name, "response": map[string]any{"output": output}}
			imageParts := make([]any, 0)
			for _, part := range chatContentParts(message) {
				if part.Type == "image" {
					imageParts = append(imageParts, map[string]any{"inlineData": map[string]any{"mimeType": part.MediaType, "data": part.Data}})
				}
			}
			if len(imageParts) > 0 && googleSupportsMultimodalFunctionResponse(model.ID) {
				response["parts"] = imageParts
			}
			contents = append(contents, map[string]any{"role": "user", "parts": []any{map[string]any{"functionResponse": response}}})
			if len(imageParts) > 0 && !googleSupportsMultimodalFunctionResponse(model.ID) {
				contents = append(contents, map[string]any{"role": "user", "parts": append([]any{map[string]any{"text": "Tool result image:"}}, imageParts...)})
			}
		}
	}
	return contents
}

func googleSupportsMultimodalFunctionResponse(model string) bool {
	model = strings.ToLower(model)
	if !strings.HasPrefix(model, "gemini-") && !strings.HasPrefix(model, "gemini-live-") {
		return true
	}
	var major int
	_, _ = fmt.Sscanf(strings.TrimPrefix(strings.TrimPrefix(model, "gemini-live-"), "gemini-"), "%d", &major)
	return major >= 3
}

func googleThinkingConfig(model piAIModel, effort string, budgets map[string]int) map[string]any {
	id := strings.ToLower(model.ID)
	if effort == "" || effort == "off" {
		switch {
		case googleGemini3Pro(id):
			return map[string]any{"thinkingLevel": "LOW"}
		case googleGemini3Flash(id), strings.Contains(id, "gemma-4"), strings.Contains(id, "gemma4"):
			return map[string]any{"thinkingLevel": "MINIMAL"}
		default:
			return map[string]any{"thinkingBudget": 0}
		}
	}
	config := map[string]any{"includeThoughts": true}
	if googleGemini3Pro(id) || googleGemini3Flash(id) || strings.Contains(id, "gemma-4") || strings.Contains(id, "gemma4") {
		level := strings.ToUpper(effort)
		if googleGemini3Pro(id) && (effort == "minimal" || effort == "low") {
			level = "LOW"
		} else if googleGemini3Pro(id) {
			level = "HIGH"
		}
		config["thinkingLevel"] = level
		return config
	}
	budget := budgets[effort]
	if budget == 0 {
		switch {
		case strings.Contains(id, "2.5-pro"):
			budget = map[string]int{"minimal": 128, "low": 2048, "medium": 8192, "high": 32768}[effort]
		case strings.Contains(id, "2.5-flash-lite"):
			budget = map[string]int{"minimal": 512, "low": 2048, "medium": 8192, "high": 24576}[effort]
		case strings.Contains(id, "2.5-flash"):
			budget = map[string]int{"minimal": 128, "low": 2048, "medium": 8192, "high": 24576}[effort]
		default:
			budget = -1
		}
	}
	config["thinkingBudget"] = budget
	return config
}

func googleGemini3Pro(id string) bool {
	return strings.Contains(id, "gemini-3-pro") || strings.Contains(id, "gemini-3.1-pro") || strings.Contains(id, "gemini-3.5-pro")
}

func googleGemini3Flash(id string) bool {
	return strings.Contains(id, "gemini-3-flash") || strings.Contains(id, "gemini-3.1-flash") || strings.Contains(id, "gemini-3.5-flash") || id == "gemini-flash-latest" || id == "gemini-flash-lite-latest"
}

type googleStreamState struct {
	onDelta   func(Delta) error
	text      string
	reasoning string
	finish    string
	usage     map[string]any
	calls     []ToolCall
	failed    string
}

func (s *googleStreamState) handle(payload []byte) error {
	var chunk map[string]any
	if err := json.Unmarshal(payload, &chunk); err != nil {
		return fmt.Errorf("malformed Google stream payload: %w", err)
	}
	candidates, _ := chunk["candidates"].([]any)
	if len(candidates) > 0 {
		candidate, _ := candidates[0].(map[string]any)
		content, _ := candidate["content"].(map[string]any)
		parts, _ := content["parts"].([]any)
		for _, raw := range parts {
			part, _ := raw.(map[string]any)
			if text := stringSetting(part["text"]); text != "" {
				if value, _ := part["thought"].(bool); value {
					s.reasoning += text
					if err := s.onDelta(Delta{Reasoning: text}); err != nil {
						return err
					}
				} else {
					s.text += text
					if err := s.onDelta(Delta{Text: text}); err != nil {
						return err
					}
				}
			}
			if call, ok := part["functionCall"].(map[string]any); ok {
				arguments, _ := json.Marshal(call["args"])
				if len(arguments) == 0 || string(arguments) == "null" {
					arguments = []byte("{}")
				}
				id := stringSetting(call["id"])
				name := stringSetting(call["name"])
				if id == "" {
					id = fmt.Sprintf("%s_%d", name, len(s.calls)+1)
				}
				index := len(s.calls)
				s.calls = append(s.calls, ToolCall{ID: id, Name: name, Arguments: arguments})
				if err := s.onDelta(Delta{ToolCalls: []ToolCallDelta{{Index: index, ID: id, Name: name, ArgumentsDelta: string(arguments)}}}); err != nil {
					return err
				}
			}
		}
		switch stringSetting(candidate["finishReason"]) {
		case "STOP":
			s.finish = "stop"
		case "MAX_TOKENS":
			s.finish = "length"
		case "":
		default:
			s.failed = "Google model stopped: " + stringSetting(candidate["finishReason"])
		}
	}
	if usage, ok := chunk["usageMetadata"].(map[string]any); ok {
		prompt := jsonInt(usage["promptTokenCount"])
		cached := jsonInt(usage["cachedContentTokenCount"])
		output := jsonInt(usage["candidatesTokenCount"])
		reasoning := jsonInt(usage["thoughtsTokenCount"])
		s.usage = map[string]any{
			"input_tokens": max(0, prompt-cached), "output_tokens": output + reasoning,
			"cache_read_tokens": cached, "reasoning_tokens": reasoning,
			"total_tokens": jsonInt(usage["totalTokenCount"]),
		}
		if err := s.onDelta(Delta{Usage: cloneStringMap(s.usage)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *googleStreamState) completion() (Completion, error) {
	if s.failed != "" {
		return Completion{}, &ProviderError{Code: "PROVIDER", Message: s.failed}
	}
	if len(s.calls) > 0 {
		s.finish = "tool_calls"
	}
	if s.finish == "" {
		s.finish = "stop"
	}
	if err := s.onDelta(Delta{Finish: s.finish}); err != nil {
		return Completion{}, err
	}
	if s.text == "" && s.reasoning == "" && len(s.calls) == 0 {
		return Completion{}, &ProviderError{Code: "EMPTY_RESPONSE", Message: "model returned a completed response with no content"}
	}
	return Completion{Text: s.text, Reasoning: s.reasoning, ToolCalls: s.calls, Usage: s.usage, Finish: s.finish}, nil
}

type googleADCFile struct {
	Type         string `json:"type"`
	ClientEmail  string `json:"client_email"`
	PrivateKey   string `json:"private_key"`
	TokenURI     string `json:"token_uri"`
	RefreshToken string `json:"refresh_token"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

func googleADCAccessToken(ctx context.Context, client *http.Client, env map[string]string) (string, error) {
	path := piAIRequestEnv(env, "GOOGLE_APPLICATION_CREDENTIALS")
	if path == "" {
		if home, err := os.UserHomeDir(); err == nil {
			path = filepath.Join(home, ".config", "gcloud", "application_default_credentials.json")
		}
	}
	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			var credential googleADCFile
			if err := json.Unmarshal(data, &credential); err != nil {
				return "", &ProviderError{Code: "AUTH", Message: "decode Google ADC: " + err.Error(), Err: err}
			}
			return credential.accessToken(ctx, client)
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", &ProviderError{Code: "AUTH", Message: "read Google ADC: " + err.Error(), Err: err}
		}
	}
	host := firstNonBlank(piAIRequestEnv(env, "GCE_METADATA_HOST"), "metadata.google.internal")
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+host+"/computeMetadata/v1/instance/service-accounts/default/token", nil)
	if err != nil {
		return "", err
	}
	hreq.Header.Set("Metadata-Flavor", "Google")
	resp, err := client.Do(hreq)
	if err != nil {
		return "", &ProviderError{Code: "MISSING_CREDENTIAL", Message: "Google Vertex AI requires GOOGLE_CLOUD_API_KEY or Application Default Credentials", Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", &ProviderError{Code: "MISSING_CREDENTIAL", Message: "Google Vertex AI requires GOOGLE_CLOUD_API_KEY or Application Default Credentials"}
	}
	return readGoogleAccessToken(resp.Body)
}

func (c googleADCFile) accessToken(ctx context.Context, client *http.Client) (string, error) {
	tokenURL := firstNonBlank(c.TokenURI, "https://oauth2.googleapis.com/token")
	values := url.Values{}
	switch c.Type {
	case "authorized_user":
		values.Set("grant_type", "refresh_token")
		values.Set("refresh_token", c.RefreshToken)
		values.Set("client_id", c.ClientID)
		values.Set("client_secret", c.ClientSecret)
	case "service_account":
		assertion, err := googleServiceAccountAssertion(c, tokenURL)
		if err != nil {
			return "", err
		}
		values.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
		values.Set("assertion", assertion)
	default:
		return "", &ProviderError{Code: "AUTH", Message: "unsupported Google ADC type " + c.Type}
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(values.Encode()))
	if err != nil {
		return "", err
	}
	hreq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(hreq)
	if err != nil {
		return "", &ProviderError{Code: "TRANSPORT", Message: "refresh Google ADC: " + err.Error(), Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return "", providerHTTPFailure(resp, string(payload))
	}
	return readGoogleAccessToken(resp.Body)
}

func googleServiceAccountAssertion(c googleADCFile, audience string) (string, error) {
	block, _ := pem.Decode([]byte(c.PrivateKey))
	if block == nil {
		return "", &ProviderError{Code: "AUTH", Message: "Google service account private key is not PEM"}
	}
	var key *rsa.PrivateKey
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err == nil {
		key, _ = parsed.(*rsa.PrivateKey)
	} else {
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	}
	if err != nil || key == nil {
		return "", &ProviderError{Code: "AUTH", Message: "parse Google service account private key", Err: err}
	}
	now := time.Now().Unix()
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{
		"iss": c.ClientEmail, "scope": "https://www.googleapis.com/auth/cloud-platform",
		"aud": audience, "iat": now, "exp": now + 3600,
	})
	encoded := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(encoded))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return encoded + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func readGoogleAccessToken(body io.Reader) (string, error) {
	var response struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	if err := json.NewDecoder(io.LimitReader(body, 1<<20)).Decode(&response); err != nil {
		return "", &ProviderError{Code: "AUTH", Message: "decode Google access token: " + err.Error(), Err: err}
	}
	if response.AccessToken == "" {
		return "", &ProviderError{Code: "AUTH", Message: firstNonBlank(response.Description, response.Error, "Google credential response has no access token")}
	}
	return response.AccessToken, nil
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
