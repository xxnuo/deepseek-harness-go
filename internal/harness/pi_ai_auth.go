package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const piAICredentialScope = "llm-pi-ai"

type piAIAuthResolution struct {
	APIKey  string
	Headers map[string]string
	Env     map[string]string
	BaseURL string
	OAuth   bool
}

var (
	piAIAuthHTTPClient       = &http.Client{Timeout: 30 * time.Second}
	piAIAnthropicTokenURL    = "https://platform.claude.com/v1/oauth/token"
	piAIOpenAICodexTokenURL  = "https://auth.openai.com/oauth/token"
	piAIXAITokenURL          = "https://auth.x.ai/oauth2/token"
	piAIRadiusGatewayURL     = "https://radius.pi.dev"
	piAIGitHubCopilotVersion = "2026-06-01"
)

var piAIProviderAPIKeyEnvs = map[string][]string{
	"ant-ling":               {"ANT_LING_API_KEY"},
	"qwen-token-plan":        {"QWEN_TOKEN_PLAN_API_KEY"},
	"qwen-token-plan-cn":     {"QWEN_TOKEN_PLAN_CN_API_KEY"},
	"openai":                 {"OPENAI_API_KEY"},
	"azure-openai-responses": {"AZURE_OPENAI_API_KEY"},
	"nvidia":                 {"NVIDIA_API_KEY"},
	"deepseek":               {"DEEPSEEK_API_KEY"},
	"google":                 {"GEMINI_API_KEY"},
	"google-vertex":          {"GOOGLE_CLOUD_API_KEY"},
	"groq":                   {"GROQ_API_KEY"},
	"cerebras":               {"CEREBRAS_API_KEY"},
	"xai":                    {"XAI_API_KEY"},
	"radius":                 {"RADIUS_API_KEY"},
	"openrouter":             {"OPENROUTER_API_KEY"},
	"vercel-ai-gateway":      {"AI_GATEWAY_API_KEY"},
	"zai":                    {"ZAI_API_KEY"},
	"zai-coding-cn":          {"ZAI_CODING_CN_API_KEY"},
	"mistral":                {"MISTRAL_API_KEY"},
	"minimax":                {"MINIMAX_API_KEY"},
	"minimax-cn":             {"MINIMAX_CN_API_KEY"},
	"moonshotai":             {"MOONSHOT_API_KEY"},
	"moonshotai-cn":          {"MOONSHOT_API_KEY"},
	"huggingface":            {"HF_TOKEN"},
	"fireworks":              {"FIREWORKS_API_KEY"},
	"together":               {"TOGETHER_API_KEY"},
	"opencode":               {"OPENCODE_API_KEY"},
	"opencode-go":            {"OPENCODE_API_KEY"},
	"kimi-coding":            {"KIMI_API_KEY"},
	"cloudflare-workers-ai":  {"CLOUDFLARE_API_KEY"},
	"cloudflare-ai-gateway":  {"CLOUDFLARE_API_KEY"},
	"xiaomi":                 {"XIAOMI_API_KEY"},
	"xiaomi-token-plan-cn":   {"XIAOMI_TOKEN_PLAN_CN_API_KEY"},
	"xiaomi-token-plan-ams":  {"XIAOMI_TOKEN_PLAN_AMS_API_KEY"},
	"xiaomi-token-plan-sgp":  {"XIAOMI_TOKEN_PLAN_SGP_API_KEY"},
	"github-copilot":         {"COPILOT_GITHUB_TOKEN"},
}

func piAIRecordKey(route string) (CredentialKey, bool) {
	if !IsCredentialKeySegment(route) {
		return "", false
	}
	key, err := NewCredentialKey(piAICredentialScope, route)
	return key, err == nil
}

func (e *Engine) resolvePiAIAuth(ctx context.Context, route, explicitRef, baseURL string) (piAIAuthResolution, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	catalog, catalogued := piAICatalog[route]
	if explicitRef != "" {
		key, err := e.resolvePiAICredential(route, explicitRef)
		if err != nil {
			return piAIAuthResolution{}, err
		}
		if !catalogued {
			return piAIAuthResolution{APIKey: key, BaseURL: e.expandPiAIBaseURL(baseURL, nil)}, nil
		}
		if catalog.Auth.APIKey != nil {
			resolution, _ := e.resolvePiAIAPIKey(route, &CredentialRecord{Kind: CredentialRecordAPIKey, Key: key})
			if resolution.BaseURL == "" {
				resolution.BaseURL = e.expandPiAIBaseURL(baseURL, resolution.Env)
			}
			return resolution, nil
		}
	}

	if catalogued {
		if key, ok := piAIRecordKey(route); ok {
			record, err := e.Credentials().ReadRecord(key)
			if err != nil {
				return piAIAuthResolution{}, &ProviderError{Code: "AUTH", Message: fmt.Sprintf("read pi-ai credential for %s: %v", route, err), Err: err}
			}
			if record != nil {
				var resolution piAIAuthResolution
				switch record.Kind {
				case CredentialRecordAPIKey:
					if catalog.Auth.APIKey != nil {
						if stored := strings.TrimSpace(record.Key); stored != "" {
							if _, err := usablePiAIAPIKey(route, "stored credential", stored); err != nil {
								return piAIAuthResolution{}, err
							}
						}
						resolution, _ = e.resolvePiAIAPIKey(route, record)
					}
				case CredentialRecordGrant:
					if catalog.Auth.OAuth != nil {
						resolution, err = e.resolvePiAIOAuth(ctx, route, key, record)
						if err != nil {
							return piAIAuthResolution{}, err
						}
					}
				}
				if resolution.BaseURL == "" {
					resolution.BaseURL = e.expandPiAIBaseURL(baseURL, resolution.Env)
				}
				return resolution, nil
			}
		}
		if catalog.Auth.APIKey != nil {
			resolution, _ := e.resolvePiAIAPIKey(route, nil)
			if resolution.BaseURL == "" {
				resolution.BaseURL = e.expandPiAIBaseURL(baseURL, resolution.Env)
			}
			return resolution, nil
		}
	}
	return piAIAuthResolution{BaseURL: e.expandPiAIBaseURL(baseURL, nil)}, nil
}

func (e *Engine) resolvePiAIAPIKey(route string, record *CredentialRecord) (piAIAuthResolution, bool) {
	overrides := map[string]string(nil)
	if record != nil {
		overrides = clonePIAIHeaders(record.Env)
	}
	lookup := func(name string) string {
		if value := strings.TrimSpace(overrides[name]); value != "" {
			return value
		}
		value, _, _ := e.resolveCredential(name)
		return strings.TrimSpace(value)
	}
	storedKey := ""
	if record != nil {
		storedKey = strings.TrimSpace(record.Key)
	}
	validated := func(ref, value string) (string, bool) {
		if value == "" {
			return "", false
		}
		key, err := usablePiAIAPIKey(route, ref, value)
		return key, err == nil
	}

	switch route {
	case "anthropic":
		if key, ok := validated("stored credential", storedKey); ok {
			return piAIAuthResolution{APIKey: key, Env: overrides}, true
		}
		if token, ok := validated("ANTHROPIC_AUTH_TOKEN", lookup("ANTHROPIC_AUTH_TOKEN")); ok {
			return piAIAuthResolution{Headers: map[string]string{"Authorization": "Bearer " + token}}, true
		}
		for _, name := range []string{"ANTHROPIC_OAUTH_TOKEN", "ANTHROPIC_API_KEY"} {
			if key, ok := validated(name, lookup(name)); ok {
				return piAIAuthResolution{APIKey: key}, true
			}
		}
		return piAIAuthResolution{}, false
	case "cloudflare-ai-gateway", "cloudflare-workers-ai":
		key := storedKey
		if key == "" {
			key = lookup("CLOUDFLARE_API_KEY")
		}
		accountID := lookup("CLOUDFLARE_ACCOUNT_ID")
		gatewayID := lookup("CLOUDFLARE_GATEWAY_ID")
		if key == "" || accountID == "" || route == "cloudflare-ai-gateway" && gatewayID == "" {
			return piAIAuthResolution{}, false
		}
		key, ok := validated("CLOUDFLARE_API_KEY", key)
		if !ok {
			return piAIAuthResolution{}, false
		}
		env := map[string]string{"CLOUDFLARE_ACCOUNT_ID": accountID}
		if gatewayID != "" {
			env["CLOUDFLARE_GATEWAY_ID"] = gatewayID
		}
		if route == "cloudflare-ai-gateway" {
			return piAIAuthResolution{Headers: map[string]string{"cf-aig-authorization": "Bearer " + key}, Env: env}, true
		}
		return piAIAuthResolution{APIKey: key, Env: env}, true
	case "google-vertex":
		key := storedKey
		if key == "" {
			key = lookup("GOOGLE_CLOUD_API_KEY")
		}
		if key != "" {
			key, ok := validated("GOOGLE_CLOUD_API_KEY", key)
			return piAIAuthResolution{APIKey: key, Env: overrides}, ok
		}
		env := collectPIAIEnv(lookup, overrides, "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GOOGLE_CLOUD_LOCATION", "GCE_METADATA_HOST")
		project := firstNonBlank(env["GOOGLE_CLOUD_PROJECT"], env["GCLOUD_PROJECT"])
		location := env["GOOGLE_CLOUD_LOCATION"]
		path := env["GOOGLE_APPLICATION_CREDENTIALS"]
		if path == "" {
			if home, err := os.UserHomeDir(); err == nil {
				path = filepath.Join(home, ".config", "gcloud", "application_default_credentials.json")
			}
		}
		if project != "" && location != "" && piAIFileExists(path) {
			return piAIAuthResolution{Env: env}, true
		}
		return piAIAuthResolution{}, false
	case "amazon-bedrock":
		if key, ok := validated("stored credential", storedKey); ok {
			return piAIAuthResolution{APIKey: key, Env: overrides}, true
		}
		env := collectPIAIEnv(lookup, overrides,
			"AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY",
			"AWS_SESSION_TOKEN", "AWS_BEARER_TOKEN_BEDROCK", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
			"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_SHARED_CREDENTIALS_FILE",
			"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_BEDROCK_SKIP_AUTH")
		configured := env["AWS_BEARER_TOKEN_BEDROCK"] != "" || env["AWS_PROFILE"] != "" ||
			firstNonBlank(env["AWS_ACCESS_KEY_ID"], env["AWS_ACCESS_KEY"]) != "" && firstNonBlank(env["AWS_SECRET_ACCESS_KEY"], env["AWS_SECRET_KEY"]) != "" ||
			env["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"] != "" || env["AWS_CONTAINER_CREDENTIALS_FULL_URI"] != "" || env["AWS_WEB_IDENTITY_TOKEN_FILE"] != ""
		return piAIAuthResolution{Env: env}, configured
	}

	if key, ok := validated("stored credential", storedKey); ok {
		return piAIAuthResolution{APIKey: key, Env: overrides}, true
	}
	for _, name := range piAIProviderAPIKeyEnvs[route] {
		if key, ok := validated(name, lookup(name)); ok {
			return piAIAuthResolution{APIKey: key}, true
		}
	}
	return piAIAuthResolution{}, false
}

func collectPIAIEnv(lookup func(string) string, overrides map[string]string, names ...string) map[string]string {
	env := clonePIAIHeaders(overrides)
	if env == nil {
		env = map[string]string{}
	}
	for _, name := range names {
		if value := lookup(name); value != "" {
			env[name] = value
		}
	}
	return env
}

func piAIFileExists(path string) bool {
	if path == "" {
		return false
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	_, err := os.Stat(path)
	return err == nil
}

func (e *Engine) expandPiAIBaseURL(baseURL string, env map[string]string) string {
	for offset := 0; offset < len(baseURL); {
		start := strings.IndexByte(baseURL[offset:], '{')
		if start < 0 {
			break
		}
		start += offset
		end := strings.IndexByte(baseURL[start+1:], '}')
		if end < 0 {
			break
		}
		end += start + 1
		name := baseURL[start+1 : end]
		value := strings.TrimSpace(env[name])
		if value == "" && validCredentialRef(name) {
			value, _, _ = e.resolveCredential(name)
		}
		if value == "" {
			offset = end + 1
			continue
		}
		baseURL = baseURL[:start] + value + baseURL[end+1:]
		offset = start + len(value)
	}
	return baseURL
}

func (e *Engine) resolvePiAIOAuth(ctx context.Context, route string, key CredentialKey, record *CredentialRecord) (piAIAuthResolution, error) {
	credential, ok := piAIOAuthPayload(record)
	if !ok {
		return piAIAuthResolution{}, nil
	}
	if piAIOAuthExpired(credential) {
		post, err := e.Credentials().ModifyRecord(ctx, key, func(lockCtx context.Context, current *CredentialRecord) (*CredentialRecord, error) {
			locked, ok := piAIOAuthPayload(current)
			if !ok || !piAIOAuthExpired(locked) {
				return nil, nil
			}
			refreshed, err := e.refreshPiAIOAuth(lockCtx, route, locked)
			if err != nil {
				return nil, err
			}
			return &CredentialRecord{Kind: CredentialRecordGrant, Payload: refreshed}, nil
		})
		if err != nil {
			return piAIAuthResolution{}, &ProviderError{Code: "AUTH", Message: fmt.Sprintf("OAuth refresh failed for %s: %v", route, err), Err: err}
		}
		credential, ok = piAIOAuthPayload(post)
		if !ok {
			return piAIAuthResolution{}, nil
		}
	}
	access, _ := credential["access"].(string)
	access, err := usablePiAIAPIKey(route, "OAuth access token", access)
	if err != nil {
		return piAIAuthResolution{}, err
	}
	resolution := piAIAuthResolution{OAuth: true}
	switch route {
	case "anthropic":
		resolution.Headers = map[string]string{
			"Authorization": "Bearer " + access,
			"Anthropic-Dangerous-Direct-Browser-Access": "true",
			"User-Agent": "claude-cli/2.1.75",
			"X-App":      "cli",
		}
	case "github-copilot":
		resolution.Headers = map[string]string{"Authorization": "Bearer " + access}
		resolution.BaseURL = piAIGitHubCopilotBaseURL(access, piAIAuthStringValue(credential["enterpriseUrl"]))
	case "kimi-coding":
		resolution.Headers = map[string]string{"Authorization": "Bearer " + access}
	case "openai-codex", "openrouter", "radius", "xai":
		resolution.APIKey = access
	default:
		return piAIAuthResolution{}, nil
	}
	return resolution, nil
}

func piAIOAuthPayload(record *CredentialRecord) (map[string]any, bool) {
	if record == nil || record.Kind != CredentialRecordGrant {
		return nil, false
	}
	payload, ok := piAIStringMap(record.Payload)
	if !ok || piAIAuthStringValue(payload["type"]) != "oauth" {
		return nil, false
	}
	return payload, true
}

func piAIStringMap(value any) (map[string]any, bool) {
	if value == nil {
		return nil, false
	}
	if mapped, ok := value.(map[string]any); ok {
		copy := make(map[string]any, len(mapped))
		for key, item := range mapped {
			copy[key] = item
		}
		return copy, true
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	var mapped map[string]any
	if json.Unmarshal(data, &mapped) != nil {
		return nil, false
	}
	return mapped, true
}

func piAIOAuthExpired(credential map[string]any) bool {
	expires, ok := piAINumber(credential["expires"])
	return !ok || time.Now().UnixMilli() >= int64(expires)
}

func piAINumber(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case float32:
		return float64(number), true
	case int:
		return float64(number), true
	case int64:
		return float64(number), true
	case json.Number:
		parsed, err := number.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(number, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func piAIAuthStringValue(value any) string {
	string, _ := value.(string)
	return string
}

func (e *Engine) refreshPiAIOAuth(ctx context.Context, route string, credential map[string]any) (map[string]any, error) {
	refresh := piAIAuthStringValue(credential["refresh"])
	switch route {
	case "anthropic":
		var response struct {
			AccessToken  string  `json:"access_token"`
			RefreshToken string  `json:"refresh_token"`
			ExpiresIn    float64 `json:"expires_in"`
		}
		if err := piAIPostJSON(ctx, piAIAnthropicTokenURL, map[string]any{
			"grant_type": "refresh_token", "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e", "refresh_token": refresh,
		}, &response); err != nil {
			return nil, err
		}
		return piAIRefreshedCredential(credential, response.AccessToken, response.RefreshToken, response.ExpiresIn, 5*time.Minute)
	case "github-copilot":
		domain := piAIGitHubDomain(piAIAuthStringValue(credential["enterpriseUrl"]))
		if domain == "" {
			domain = "github.com"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api."+domain+"/copilot_internal/v2/token", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "Bearer "+refresh)
		applyPiAICopilotHeaders(req.Header)
		var response struct {
			Token     string `json:"token"`
			ExpiresAt int64  `json:"expires_at"`
		}
		if err := piAIDoJSON(req, &response); err != nil {
			return nil, err
		}
		if response.Token == "" || response.ExpiresAt <= 0 {
			return nil, errors.New("invalid Copilot token response")
		}
		out := clonePIAIPayload(credential)
		out["access"] = response.Token
		out["expires"] = response.ExpiresAt*1000 - int64(5*time.Minute/time.Millisecond)
		return out, nil
	case "kimi-coding":
		host := firstNonBlank(e.piAIEnv("KIMI_CODE_OAUTH_HOST", nil), e.piAIEnv("KIMI_OAUTH_HOST", nil), "https://auth.kimi.com")
		return e.refreshPiAIFormOAuth(ctx, strings.TrimRight(host, "/")+"/api/oauth/token", credential, url.Values{
			"client_id": {"17e5f671-d194-4dfb-9706-5516cb48c098"}, "grant_type": {"refresh_token"}, "refresh_token": {refresh},
		}, 0)
	case "openai-codex":
		out, err := e.refreshPiAIFormOAuth(ctx, piAIOpenAICodexTokenURL, credential, url.Values{
			"grant_type": {"refresh_token"}, "refresh_token": {refresh}, "client_id": {"app_EMoamEEZ73f0CkXaXp7hrann"},
		}, 0)
		if err != nil {
			return nil, err
		}
		if account := codexAccountIDFromToken(piAIAuthStringValue(out["access"])); account != "" {
			out["accountId"] = account
		}
		return out, nil
	case "openrouter":
		return clonePIAIPayload(credential), nil
	case "radius":
		return e.refreshPiAIFormOAuth(ctx, strings.TrimRight(piAIRadiusGatewayURL, "/")+"/v1/oauth/token", credential, url.Values{
			"grant_type": {"refresh_token"}, "client_id": {"pi-gateway"}, "refresh_token": {refresh},
		}, time.Minute)
	case "xai":
		return e.refreshPiAIFormOAuth(ctx, piAIXAITokenURL, credential, url.Values{
			"grant_type": {"refresh_token"}, "client_id": {"b1a00492-073a-47ea-816f-4c329264a828"}, "refresh_token": {refresh},
		}, 5*time.Minute)
	default:
		return nil, fmt.Errorf("provider %s has no OAuth refresh implementation", route)
	}
}

func (e *Engine) refreshPiAIFormOAuth(ctx context.Context, endpoint string, previous map[string]any, values url.Values, skew time.Duration) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	var response struct {
		AccessToken  string  `json:"access_token"`
		RefreshToken string  `json:"refresh_token"`
		ExpiresIn    float64 `json:"expires_in"`
		Scope        string  `json:"scope"`
	}
	if err := piAIDoJSON(req, &response); err != nil {
		return nil, err
	}
	if response.RefreshToken == "" {
		response.RefreshToken = piAIAuthStringValue(previous["refresh"])
	}
	out, err := piAIRefreshedCredential(previous, response.AccessToken, response.RefreshToken, response.ExpiresIn, skew)
	if err != nil {
		return nil, err
	}
	if response.Scope != "" {
		out["scope"] = response.Scope
	}
	return out, nil
}

func piAIRefreshedCredential(previous map[string]any, access, refresh string, expiresIn float64, skew time.Duration) (map[string]any, error) {
	if access == "" || refresh == "" || expiresIn <= 0 {
		return nil, errors.New("OAuth token response is missing access_token, refresh_token, or expires_in")
	}
	out := clonePIAIPayload(previous)
	out["type"] = "oauth"
	out["access"] = access
	out["refresh"] = refresh
	out["expires"] = time.Now().UnixMilli() + int64(expiresIn*1000) - int64(skew/time.Millisecond)
	return out, nil
}

func clonePIAIPayload(source map[string]any) map[string]any {
	copy := make(map[string]any, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func piAIPostJSON(ctx context.Context, endpoint string, body any, target any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	return piAIDoJSON(req, target)
}

func piAIDoJSON(req *http.Request, target any) error {
	response, err := piAIAuthHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %d from %s: %s", response.StatusCode, req.URL, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("decode response from %s: %w", req.URL, err)
	}
	return nil
}

func (e *Engine) piAIEnv(name string, overrides map[string]string) string {
	if value := strings.TrimSpace(overrides[name]); value != "" {
		return value
	}
	value, _, _ := e.resolveCredential(name)
	return strings.TrimSpace(value)
}

func piAIRequestEnv(env map[string]string, name string) string {
	if value := strings.TrimSpace(env[name]); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv(name))
}

func applyPiAICopilotHeaders(headers http.Header) {
	headers.Set("User-Agent", "GitHubCopilotChat/0.35.0")
	headers.Set("Editor-Version", "vscode/1.107.0")
	headers.Set("Editor-Plugin-Version", "copilot-chat/0.35.0")
	headers.Set("Copilot-Integration-Id", "vscode-chat")
	headers.Set("X-GitHub-Api-Version", piAIGitHubCopilotVersion)
}

func piAIGitHubDomain(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func piAIGitHubCopilotBaseURL(token, enterprise string) string {
	for _, part := range strings.Split(token, ";") {
		part = strings.TrimSpace(part)
		if !strings.HasPrefix(part, "proxy-ep=") {
			continue
		}
		host := strings.TrimSpace(strings.TrimPrefix(part, "proxy-ep="))
		if parsed, err := url.Parse("https://" + host); err == nil && parsed.Hostname() == host {
			return "https://" + strings.Replace(host, "proxy.", "api.", 1)
		}
	}
	if domain := piAIGitHubDomain(enterprise); domain != "" {
		return "https://copilot-api." + domain
	}
	return "https://api.individual.githubcopilot.com"
}

func codexAccountIDFromToken(token string) string {
	account, _ := codexAccountID(token)
	return account
}
