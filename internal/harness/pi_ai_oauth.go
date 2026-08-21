package harness

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	piAIOAuthDeviceGrant = "urn:ietf:params:oauth:grant-type:device_code"
	piAICodexClientID    = "app_EMoamEEZ73f0CkXaXp7hrann"
	piAIKimiClientID     = "17e5f671-d194-4dfb-9706-5516cb48c098"
	piAIXAIClientID      = "b1a00492-073a-47ea-816f-4c329264a828"
)

var (
	piAIAnthropicAuthorizeURL    = "https://claude.ai/oauth/authorize"
	piAICodexAuthorizeURL        = "https://auth.openai.com/oauth/authorize"
	piAICodexDeviceCodeURL       = "https://auth.openai.com/api/accounts/deviceauth/usercode"
	piAICodexDeviceTokenURL      = "https://auth.openai.com/api/accounts/deviceauth/token"
	piAICodexDeviceVerifyURL     = "https://auth.openai.com/codex/device"
	piAICodexDeviceRedirectURL   = "https://auth.openai.com/deviceauth/callback"
	piAIOpenRouterAuthorizeURL   = "https://openrouter.ai/auth"
	piAIOpenRouterTokenURL       = "https://openrouter.ai/api/v1/auth/keys"
	piAIXAIDeviceCodeURL         = "https://auth.x.ai/oauth2/device/code"
	piAIOAuthMinimumPollInterval = time.Second
	piAIGitHubURL                = func(domain, path string) string { return "https://" + domain + path }
)

type piAIDevicePoll struct {
	Status          string
	Message         string
	IntervalSeconds float64
	Value           any
}

type piAICallbackServer struct {
	server *http.Server
	code   chan string
	url    string
	once   sync.Once
}

func (e *Engine) loginPiAIOAuth(session AuthorizationSession, route string) (map[string]any, error) {
	switch route {
	case "anthropic":
		return e.loginPiAIAnthropic(session)
	case "github-copilot":
		return e.loginPiAIGitHubCopilot(session)
	case "kimi-coding":
		return e.loginPiAIKimi(session)
	case "openai-codex":
		return e.loginPiAICodex(session)
	case "openrouter":
		return e.loginPiAIOpenRouter(session)
	case "radius":
		return e.loginPiAIRadius(session)
	case "xai":
		return e.loginPiAIXAI(session)
	default:
		return nil, fmt.Errorf("provider %s has no OAuth login implementation", route)
	}
}

func piAIPKCE() (string, string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", "", err
	}
	verifier := base64.RawURLEncoding.EncodeToString(bytes)
	digest := sha256.Sum256([]byte(verifier))
	return verifier, base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func piAIRandomState() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func startPiAICallback(ctx context.Context, host string, port int, path, publicHost, expectedState string) (*piAICallbackServer, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	actualPort := listener.Addr().(*net.TCPAddr).Port
	callback := &piAICallbackServer{code: make(chan string, 1)}
	callback.url = "http://" + net.JoinHostPort(publicHost, strconv.Itoa(actualPort)) + path
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		query := request.URL.Query()
		if expectedState != "" && query.Get("state") != expectedState {
			http.Error(response, "OAuth state mismatch", http.StatusBadRequest)
			return
		}
		if failure := query.Get("error"); failure != "" {
			http.Error(response, firstNonBlank(query.Get("error_description"), failure), http.StatusBadRequest)
			return
		}
		code := query.Get("code")
		if code == "" {
			http.Error(response, "missing authorization code", http.StatusBadRequest)
			return
		}
		response.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(response, "<!doctype html><title>Authentication complete</title><p>Authentication completed. You can close this window.</p>")
		select {
		case callback.code <- code:
		default:
		}
	})
	callback.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = callback.server.Serve(listener) }()
	go func() {
		<-ctx.Done()
		callback.Close()
	}()
	return callback, nil
}

func (callback *piAICallbackServer) Close() {
	if callback == nil {
		return
	}
	callback.once.Do(func() { _ = callback.server.Close() })
}

func (callback *piAICallbackServer) Wait(ctx context.Context) (string, error) {
	select {
	case code := <-callback.code:
		return code, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func piAIManualCode(input string) (code, state string) {
	value := strings.TrimSpace(input)
	if value == "" {
		return "", ""
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" {
		return parsed.Query().Get("code"), parsed.Query().Get("state")
	}
	if strings.Contains(value, "#") {
		parts := strings.SplitN(value, "#", 2)
		return parts[0], parts[1]
	}
	if strings.Contains(value, "code=") {
		values, _ := url.ParseQuery(value)
		return values.Get("code"), values.Get("state")
	}
	return value, ""
}

func waitPiAIBrowserOrManual(session AuthorizationSession, callback *piAICallbackServer, expectedState, message, placeholder string) (string, error) {
	promptCtx, cancelPrompt := context.WithCancel(session.Context)
	defer cancelPrompt()
	type result struct {
		value string
		err   error
	}
	manual := make(chan result, 1)
	go func() {
		value, err := session.Prompt(AuthorizationPrompt{Kind: AuthorizationPromptText, Message: message, Placeholder: placeholder, Context: promptCtx})
		manual <- result{value: value, err: err}
	}()
	select {
	case code := <-callback.code:
		cancelPrompt()
		return code, nil
	case answer := <-manual:
		if answer.err != nil {
			return "", answer.err
		}
		code, state := piAIManualCode(answer.value)
		if state != "" && state != expectedState {
			return "", errors.New("OAuth state mismatch")
		}
		if code == "" {
			return "", errors.New("missing authorization code")
		}
		return code, nil
	case <-session.Context.Done():
		return "", session.Context.Err()
	}
}

func pollPiAIDevice(ctx context.Context, intervalSeconds, expiresSeconds float64, waitFirst bool, poll func() (piAIDevicePoll, error)) (any, error) {
	interval := time.Duration(intervalSeconds * float64(time.Second))
	if interval < piAIOAuthMinimumPollInterval {
		interval = piAIOAuthMinimumPollInterval
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	deadline := time.Time{}
	if expiresSeconds > 0 {
		deadline = time.Now().Add(time.Duration(expiresSeconds * float64(time.Second)))
	}
	wait := func() error {
		delay := interval
		if !deadline.IsZero() && time.Now().Add(delay).After(deadline) {
			delay = time.Until(deadline)
		}
		if delay <= 0 {
			return errors.New("device flow timed out")
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if waitFirst {
		if err := wait(); err != nil {
			return nil, err
		}
	}
	slowed := false
	for deadline.IsZero() || time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := poll()
		if err != nil {
			return nil, err
		}
		switch result.Status {
		case "complete":
			return result.Value, nil
		case "failed":
			return nil, errors.New(result.Message)
		case "slow_down":
			slowed = true
			if result.IntervalSeconds > 0 {
				interval = max(piAIOAuthMinimumPollInterval, time.Duration(result.IntervalSeconds*float64(time.Second)))
			} else {
				interval += 5 * time.Second
			}
		case "pending":
		default:
			return nil, fmt.Errorf("unknown device poll status %q", result.Status)
		}
		if err := wait(); err != nil {
			if slowed && strings.Contains(err.Error(), "timed out") {
				return nil, errors.New("device flow timed out after slow_down; check system clock")
			}
			return nil, err
		}
	}
	return nil, errors.New("device flow timed out")
}

func (e *Engine) loginPiAIAnthropic(session AuthorizationSession) (map[string]any, error) {
	verifier, challenge, err := piAIPKCE()
	if err != nil {
		return nil, err
	}
	host := firstNonBlank(e.piAIEnv("PI_OAUTH_CALLBACK_HOST", nil), "127.0.0.1")
	callback, err := startPiAICallback(session.Context, host, 53692, "/callback", "localhost", verifier)
	if err != nil {
		return nil, err
	}
	defer callback.Close()
	query := url.Values{
		"code": {"true"}, "client_id": {"9d1c250a-e61b-44d9-88ed-5944d1962f5e"}, "response_type": {"code"},
		"redirect_uri": {callback.url}, "scope": {"org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload"},
		"code_challenge": {challenge}, "code_challenge_method": {"S256"}, "state": {verifier},
	}
	session.Notify(AuthorizationNotice{Message: "Complete login in your browser. If the browser is on another machine, paste the final redirect URL here.", URL: piAIAnthropicAuthorizeURL + "?" + query.Encode()})
	code, err := waitPiAIBrowserOrManual(session, callback, verifier, "Complete login in your browser, or paste the authorization code / redirect URL here:", callback.url)
	if err != nil {
		return nil, err
	}
	session.Notify(AuthorizationNotice{Message: "Exchanging authorization code for tokens..."})
	var response struct {
		AccessToken  string  `json:"access_token"`
		RefreshToken string  `json:"refresh_token"`
		ExpiresIn    float64 `json:"expires_in"`
	}
	if err := piAIPostJSON(session.Context, piAIAnthropicTokenURL, map[string]any{
		"grant_type": "authorization_code", "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
		"code": code, "state": verifier, "redirect_uri": callback.url, "code_verifier": verifier,
	}, &response); err != nil {
		return nil, err
	}
	return piAIRefreshedCredential(map[string]any{"type": "oauth"}, response.AccessToken, response.RefreshToken, response.ExpiresIn, 5*time.Minute)
}

func (e *Engine) loginPiAIGitHubCopilot(session AuthorizationSession) (map[string]any, error) {
	enterprise, err := session.Prompt(AuthorizationPrompt{Kind: AuthorizationPromptText, Message: "GitHub Enterprise URL/domain (blank for github.com)", Placeholder: "company.ghe.com"})
	if err != nil {
		return nil, err
	}
	domain := piAIGitHubDomain(enterprise)
	if strings.TrimSpace(enterprise) != "" && domain == "" {
		return nil, errors.New("invalid GitHub Enterprise URL/domain")
	}
	if domain == "" {
		domain = "github.com"
	}
	device, err := requestPiAIGitHubDevice(session.Context, domain)
	if err != nil {
		return nil, err
	}
	session.Notify(AuthorizationNotice{Message: "Enter this code on the verification page to finish signing in.", URL: device.VerificationURI, Code: device.UserCode})
	value, err := pollPiAIDevice(session.Context, device.Interval, device.ExpiresIn, true, func() (piAIDevicePoll, error) {
		return pollPiAIGitHubAccessToken(session.Context, domain, device.DeviceCode)
	})
	if err != nil {
		return nil, err
	}
	access, _ := value.(string)
	credential, err := fetchPiAIGitHubCopilotToken(session.Context, domain, access, enterprise)
	if err != nil {
		return nil, err
	}
	session.Notify(AuthorizationNotice{Message: "Enabling models..."})
	enablePiAIGitHubModels(session.Context, credential)
	available, err := fetchPiAIGitHubModels(session.Context, credential)
	if err != nil {
		return nil, err
	}
	credential["availableModelIds"] = available
	return credential, nil
}

type piAIGitHubDevice struct {
	DeviceCode      string
	UserCode        string
	VerificationURI string
	Interval        float64
	ExpiresIn       float64
}

func requestPiAIGitHubDevice(ctx context.Context, domain string) (piAIGitHubDevice, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, piAIGitHubURL(domain, "/login/device/code"), strings.NewReader(url.Values{
		"client_id": {"Iv1.b507a08c87ecfe98"}, "scope": {"read:user"},
	}.Encode()))
	if err != nil {
		return piAIGitHubDevice{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "GitHubCopilotChat/0.35.0")
	var response struct {
		DeviceCode      string  `json:"device_code"`
		UserCode        string  `json:"user_code"`
		VerificationURI string  `json:"verification_uri"`
		Interval        float64 `json:"interval"`
		ExpiresIn       float64 `json:"expires_in"`
	}
	if err := piAIDoJSON(req, &response); err != nil {
		return piAIGitHubDevice{}, err
	}
	if response.DeviceCode == "" || response.UserCode == "" || !piAITrustedHTTPURL(response.VerificationURI) || response.ExpiresIn <= 0 {
		return piAIGitHubDevice{}, errors.New("invalid GitHub device code response")
	}
	return piAIGitHubDevice(response), nil
}

func pollPiAIGitHubAccessToken(ctx context.Context, domain, deviceCode string) (piAIDevicePoll, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, piAIGitHubURL(domain, "/login/oauth/access_token"), strings.NewReader(url.Values{
		"client_id": {"Iv1.b507a08c87ecfe98"}, "device_code": {deviceCode}, "grant_type": {piAIOAuthDeviceGrant},
	}.Encode()))
	if err != nil {
		return piAIDevicePoll{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "GitHubCopilotChat/0.35.0")
	var response struct {
		AccessToken      string  `json:"access_token"`
		Error            string  `json:"error"`
		ErrorDescription string  `json:"error_description"`
		Interval         float64 `json:"interval"`
	}
	if err := piAIDoJSON(req, &response); err != nil {
		return piAIDevicePoll{}, err
	}
	if response.AccessToken != "" {
		return piAIDevicePoll{Status: "complete", Value: response.AccessToken}, nil
	}
	switch response.Error {
	case "authorization_pending":
		return piAIDevicePoll{Status: "pending"}, nil
	case "slow_down":
		return piAIDevicePoll{Status: "slow_down", IntervalSeconds: response.Interval}, nil
	default:
		return piAIDevicePoll{Status: "failed", Message: "device flow failed: " + firstNonBlank(response.ErrorDescription, response.Error, "invalid response")}, nil
	}
}

func fetchPiAIGitHubCopilotToken(ctx context.Context, domain, githubToken, enterprise string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, piAIGitHubURL("api."+domain, "/copilot_internal/v2/token"), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+githubToken)
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
	credential := map[string]any{
		"type": "oauth", "access": response.Token, "refresh": githubToken,
		"expires": response.ExpiresAt*1000 - int64(5*time.Minute/time.Millisecond),
	}
	if strings.TrimSpace(enterprise) != "" {
		credential["enterpriseUrl"] = strings.TrimSpace(enterprise)
	}
	return credential, nil
}

func enablePiAIGitHubModels(ctx context.Context, credential map[string]any) {
	access := piAIAuthStringValue(credential["access"])
	base := piAIGitHubCopilotBaseURL(access, piAIAuthStringValue(credential["enterpriseUrl"]))
	models := piAICatalog["github-copilot"].Models
	var group sync.WaitGroup
	for _, model := range models {
		model := model
		group.Add(1)
		go func() {
			defer group.Done()
			body := strings.NewReader(`{"state":"enabled"}`)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/models/"+url.PathEscape(model.ID)+"/policy", body)
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+access)
			applyPiAICopilotHeaders(req.Header)
			req.Header.Set("Openai-Intent", "chat-policy")
			req.Header.Set("X-Interaction-Type", "chat-policy")
			response, err := piAIAuthHTTPClient.Do(req)
			if err == nil {
				response.Body.Close()
			}
		}()
	}
	group.Wait()
}

func fetchPiAIGitHubModels(ctx context.Context, credential map[string]any) ([]string, error) {
	access := piAIAuthStringValue(credential["access"])
	base := piAIGitHubCopilotBaseURL(access, piAIAuthStringValue(credential["enterpriseUrl"]))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/models", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+access)
	applyPiAICopilotHeaders(req.Header)
	var response struct {
		Data []struct {
			ID                 string `json:"id"`
			ModelPickerEnabled bool   `json:"model_picker_enabled"`
			Policy             struct {
				State string `json:"state"`
			} `json:"policy"`
			Capabilities struct {
				Supports struct {
					ToolCalls *bool `json:"tool_calls"`
				} `json:"supports"`
			} `json:"capabilities"`
		} `json:"data"`
	}
	if err := piAIDoJSON(req, &response); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(response.Data))
	for _, model := range response.Data {
		if model.ID != "" && model.ModelPickerEnabled && model.Policy.State != "disabled" && (model.Capabilities.Supports.ToolCalls == nil || *model.Capabilities.Supports.ToolCalls) {
			ids = append(ids, model.ID)
		}
	}
	return ids, nil
}

type piAIKimiDevice struct {
	DeviceCode              string
	UserCode                string
	VerificationURI         string
	VerificationURIComplete string
	Interval                float64
	ExpiresIn               float64
}

func (e *Engine) loginPiAIKimi(session AuthorizationSession) (map[string]any, error) {
	host := strings.TrimRight(firstNonBlank(e.piAIEnv("KIMI_CODE_OAUTH_HOST", nil), e.piAIEnv("KIMI_OAUTH_HOST", nil), "https://auth.kimi.com"), "/")
	device, err := requestPiAIKimiDevice(session.Context, host)
	if err != nil {
		return nil, err
	}
	session.Notify(AuthorizationNotice{Message: "Enter this code on the verification page to finish signing in.", URL: device.VerificationURIComplete, Code: device.UserCode})
	value, err := pollPiAIDevice(session.Context, device.Interval, device.ExpiresIn, true, func() (piAIDevicePoll, error) {
		return pollPiAIKimiToken(session.Context, host, device.DeviceCode)
	})
	if err != nil {
		return nil, err
	}
	payload, _ := value.(map[string]any)
	return payload, nil
}

func requestPiAIKimiDevice(ctx context.Context, host string) (piAIKimiDevice, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, host+"/api/oauth/device_authorization", strings.NewReader(url.Values{"client_id": {piAIKimiClientID}}.Encode()))
	if err != nil {
		return piAIKimiDevice{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	var response struct {
		DeviceCode              string  `json:"device_code"`
		UserCode                string  `json:"user_code"`
		VerificationURI         string  `json:"verification_uri"`
		VerificationURIComplete string  `json:"verification_uri_complete"`
		Interval                float64 `json:"interval"`
		ExpiresIn               float64 `json:"expires_in"`
	}
	if err := piAIDoJSON(req, &response); err != nil {
		return piAIKimiDevice{}, err
	}
	if response.DeviceCode == "" || response.UserCode == "" || !piAITrustedHTTPURL(response.VerificationURI) || !piAITrustedHTTPURL(response.VerificationURIComplete) {
		return piAIKimiDevice{}, errors.New("invalid Kimi Code device authorization response")
	}
	if response.Interval <= 0 {
		response.Interval = 5
	}
	if response.ExpiresIn <= 0 {
		response.ExpiresIn = 15 * 60
	}
	return piAIKimiDevice(response), nil
}

func pollPiAIKimiToken(ctx context.Context, host, deviceCode string) (piAIDevicePoll, error) {
	response, status, err := piAIPostFormMap(ctx, host+"/api/oauth/token", url.Values{
		"client_id": {piAIKimiClientID}, "device_code": {deviceCode}, "grant_type": {piAIOAuthDeviceGrant},
	})
	if err != nil {
		return piAIDevicePoll{}, err
	}
	if status/100 == 2 && piAIAuthStringValue(response["access_token"]) != "" {
		credential, err := piAITokenCredential(response, nil, 0)
		return piAIDevicePoll{Status: "complete", Value: credential}, err
	}
	switch piAIAuthStringValue(response["error"]) {
	case "authorization_pending":
		return piAIDevicePoll{Status: "pending"}, nil
	case "slow_down":
		interval, _ := piAINumber(response["interval"])
		return piAIDevicePoll{Status: "slow_down", IntervalSeconds: interval}, nil
	case "expired_token":
		return piAIDevicePoll{Status: "failed", Message: "Kimi Code device authorization expired. Please restart login."}, nil
	case "access_denied":
		return piAIDevicePoll{Status: "failed", Message: "Kimi Code login was denied."}, nil
	default:
		return piAIDevicePoll{Status: "failed", Message: fmt.Sprintf("Kimi Code device token request failed (status %d)", status)}, nil
	}
}

func (e *Engine) loginPiAICodex(session AuthorizationSession) (map[string]any, error) {
	method, err := session.Prompt(AuthorizationPrompt{
		Kind: AuthorizationPromptSelect, Message: "Select OpenAI Codex login method:",
		Options: []AuthorizationPromptOption{{ID: "browser", Label: "Browser login (default)"}, {ID: "device_code", Label: "Device code login (headless)"}},
	})
	if err != nil {
		return nil, err
	}
	switch strings.TrimSpace(method) {
	case "browser":
		return e.loginPiAICodexBrowser(session)
	case "device_code":
		return e.loginPiAICodexDevice(session)
	default:
		return nil, fmt.Errorf("unknown OpenAI Codex login method: %s", method)
	}
}

func (e *Engine) loginPiAICodexBrowser(session AuthorizationSession) (map[string]any, error) {
	verifier, challenge, err := piAIPKCE()
	if err != nil {
		return nil, err
	}
	state, err := piAIRandomState()
	if err != nil {
		return nil, err
	}
	host := firstNonBlank(e.piAIEnv("PI_OAUTH_CALLBACK_HOST", nil), "127.0.0.1")
	callback, err := startPiAICallback(session.Context, host, 1455, "/auth/callback", "localhost", state)
	if err != nil {
		return nil, err
	}
	defer callback.Close()
	query := url.Values{
		"response_type": {"code"}, "client_id": {piAICodexClientID}, "redirect_uri": {callback.url}, "scope": {"openid profile email offline_access"},
		"code_challenge": {challenge}, "code_challenge_method": {"S256"}, "state": {state}, "id_token_add_organizations": {"true"},
		"codex_cli_simplified_flow": {"true"}, "originator": {"pi"},
	}
	session.Notify(AuthorizationNotice{Message: "A browser window should open. Complete login to finish.", URL: piAICodexAuthorizeURL + "?" + query.Encode()})
	code, err := waitPiAIBrowserOrManual(session, callback, state, "Complete login in your browser, or paste the authorization code / redirect URL here:", callback.url)
	if err != nil {
		return nil, err
	}
	return exchangePiAICodexCode(session.Context, code, verifier, callback.url)
}

func (e *Engine) loginPiAICodexDevice(session AuthorizationSession) (map[string]any, error) {
	data, err := json.Marshal(map[string]string{"client_id": piAICodexClientID})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(session.Context, http.MethodPost, piAICodexDeviceCodeURL, strings.NewReader(string(data)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	var device struct {
		DeviceAuthID string `json:"device_auth_id"`
		UserCode     string `json:"user_code"`
		Interval     any    `json:"interval"`
	}
	if err := piAIDoJSON(req, &device); err != nil {
		return nil, err
	}
	interval, ok := piAINumber(device.Interval)
	if device.DeviceAuthID == "" || device.UserCode == "" || !ok || interval < 0 {
		return nil, errors.New("invalid OpenAI Codex device code response")
	}
	session.Notify(AuthorizationNotice{Message: "Enter this code on the verification page to finish signing in.", URL: piAICodexDeviceVerifyURL, Code: device.UserCode})
	value, err := pollPiAIDevice(session.Context, interval, 15*60, false, func() (piAIDevicePoll, error) {
		body, _ := json.Marshal(map[string]string{"device_auth_id": device.DeviceAuthID, "user_code": device.UserCode})
		request, err := http.NewRequestWithContext(session.Context, http.MethodPost, piAICodexDeviceTokenURL, strings.NewReader(string(body)))
		if err != nil {
			return piAIDevicePoll{}, err
		}
		request.Header.Set("Content-Type", "application/json")
		response, status, err := piAIDoMap(request)
		if err != nil {
			return piAIDevicePoll{}, err
		}
		if status/100 == 2 {
			code := piAIAuthStringValue(response["authorization_code"])
			verifier := piAIAuthStringValue(response["code_verifier"])
			if code == "" || verifier == "" {
				return piAIDevicePoll{Status: "failed", Message: "invalid OpenAI Codex device token response"}, nil
			}
			return piAIDevicePoll{Status: "complete", Value: []string{code, verifier}}, nil
		}
		if status == http.StatusForbidden || status == http.StatusNotFound || piAICodexErrorCode(response) == "deviceauth_authorization_pending" {
			return piAIDevicePoll{Status: "pending"}, nil
		}
		if piAICodexErrorCode(response) == "slow_down" {
			return piAIDevicePoll{Status: "slow_down"}, nil
		}
		return piAIDevicePoll{Status: "failed", Message: fmt.Sprintf("OpenAI Codex device auth failed with status %d", status)}, nil
	})
	if err != nil {
		return nil, err
	}
	code := value.([]string)
	return exchangePiAICodexCode(session.Context, code[0], code[1], piAICodexDeviceRedirectURL)
}

func piAICodexErrorCode(response map[string]any) string {
	if code := piAIAuthStringValue(response["error"]); code != "" {
		return code
	}
	if nested, ok := response["error"].(map[string]any); ok {
		return piAIAuthStringValue(nested["code"])
	}
	return ""
}

func exchangePiAICodexCode(ctx context.Context, code, verifier, redirect string) (map[string]any, error) {
	response, status, err := piAIPostFormMap(ctx, piAIOpenAICodexTokenURL, url.Values{
		"grant_type": {"authorization_code"}, "client_id": {piAICodexClientID}, "code": {code}, "code_verifier": {verifier}, "redirect_uri": {redirect},
	})
	if err != nil {
		return nil, err
	}
	if status/100 != 2 {
		return nil, fmt.Errorf("OpenAI Codex token exchange failed (%d): %v", status, response)
	}
	credential, err := piAITokenCredential(response, nil, 0)
	if err != nil {
		return nil, err
	}
	account := codexAccountIDFromToken(piAIAuthStringValue(credential["access"]))
	if account == "" {
		return nil, errors.New("failed to extract accountId from OpenAI Codex token")
	}
	credential["accountId"] = account
	return credential, nil
}

func (e *Engine) loginPiAIOpenRouter(session AuthorizationSession) (map[string]any, error) {
	verifier, challenge, err := piAIPKCE()
	if err != nil {
		return nil, err
	}
	host := firstNonBlank(e.piAIEnv("PI_OAUTH_CALLBACK_HOST", nil), "127.0.0.1")
	path := "/oauth/callback/"
	state, err := piAIRandomState()
	if err != nil {
		return nil, err
	}
	path += state
	callback, err := startPiAICallback(session.Context, host, 0, path, host, "")
	if err != nil {
		return nil, err
	}
	defer callback.Close()
	session.Notify(AuthorizationNotice{Message: "Listening for OpenRouter OAuth callback on " + callback.url})
	authorize := piAIOpenRouterAuthorizeURL + "?" + url.Values{
		"callback_url": {callback.url}, "code_challenge": {challenge}, "code_challenge_method": {"S256"},
	}.Encode()
	session.Notify(AuthorizationNotice{Message: "Complete sign-in in your browser.", URL: authorize})
	code, err := callback.Wait(session.Context)
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(map[string]string{"code": code, "code_verifier": verifier, "code_challenge_method": "S256"})
	req, err := http.NewRequestWithContext(session.Context, http.MethodPost, piAIOpenRouterTokenURL, strings.NewReader(string(data)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	response, status, err := piAIDoMap(req)
	if err != nil {
		return nil, err
	}
	key := piAIAuthStringValue(response["key"])
	if status/100 != 2 || key == "" {
		return nil, fmt.Errorf("OpenRouter OAuth key exchange failed (HTTP %d)", status)
	}
	return map[string]any{"type": "oauth", "access": key, "refresh": "", "expires": float64(1<<53 - 1)}, nil
}

func (e *Engine) loginPiAIRadius(session AuthorizationSession) (map[string]any, error) {
	method, err := session.Prompt(AuthorizationPrompt{
		Kind: AuthorizationPromptSelect, Message: "Sign in to Radius:",
		Options: []AuthorizationPromptOption{{ID: "browser", Label: "Sign in with browser (recommended)"}, {ID: "device-code", Label: "Sign in with device code (when signing in from another device)"}},
	})
	if err != nil {
		return nil, err
	}
	switch strings.TrimSpace(method) {
	case "browser":
		return e.loginPiAIRadiusBrowser(session)
	case "device-code":
		return e.loginPiAIRadiusDevice(session)
	default:
		return nil, fmt.Errorf("unknown Radius sign-in method: %s", method)
	}
}

func (e *Engine) loginPiAIRadiusBrowser(session AuthorizationSession) (map[string]any, error) {
	discoveryReq, _ := http.NewRequestWithContext(session.Context, http.MethodGet, strings.TrimRight(piAIRadiusGatewayURL, "/")+"/v1/oauth", nil)
	var discovery struct {
		AuthorizationEndpoint string `json:"authorizationEndpoint"`
	}
	if err := piAIDoJSON(discoveryReq, &discovery); err != nil {
		return nil, err
	}
	if discovery.AuthorizationEndpoint == "" {
		return nil, errors.New("invalid Radius OAuth config")
	}
	verifier, challenge, err := piAIPKCE()
	if err != nil {
		return nil, err
	}
	state, err := piAIRandomState()
	if err != nil {
		return nil, err
	}
	callback, err := startPiAICallback(session.Context, "127.0.0.1", 1456, "/oauth/callback", "127.0.0.1", state)
	if err != nil {
		return nil, err
	}
	defer callback.Close()
	query := url.Values{
		"response_type": {"code"}, "client_id": {"pi-gateway"}, "redirect_uri": {callback.url}, "scope": {"gateway offline_access"},
		"code_challenge": {challenge}, "code_challenge_method": {"S256"}, "handoff": {"url"}, "state": {state},
	}
	session.Notify(AuthorizationNotice{Message: "Listening for OAuth callback on " + callback.url})
	session.Notify(AuthorizationNotice{Message: "Continue in your browser.", URL: discovery.AuthorizationEndpoint + "?" + query.Encode()})
	code, err := callback.Wait(session.Context)
	if err != nil {
		return nil, err
	}
	return requestPiAIRadiusToken(session.Context, url.Values{
		"grant_type": {"authorization_code"}, "client_id": {"pi-gateway"}, "redirect_uri": {callback.url}, "code": {code}, "code_verifier": {verifier},
	})
}

func (e *Engine) loginPiAIRadiusDevice(session AuthorizationSession) (map[string]any, error) {
	response, status, err := piAIPostFormMap(session.Context, strings.TrimRight(piAIRadiusGatewayURL, "/")+"/v1/oauth/device", url.Values{
		"client_id": {"pi-gateway"}, "scope": {"gateway offline_access"},
	})
	if err != nil {
		return nil, err
	}
	deviceCode := piAIAuthStringValue(response["device_code"])
	userCode := piAIAuthStringValue(response["user_code"])
	verification := piAIAuthStringValue(response["verification_uri"])
	expires, _ := piAINumber(response["expires_in"])
	interval, _ := piAINumber(response["interval"])
	if status/100 != 2 || deviceCode == "" || userCode == "" || !piAITrustedHTTPURL(verification) || expires <= 0 {
		return nil, errors.New("Radius OAuth device authorization response is missing required fields")
	}
	session.Notify(AuthorizationNotice{Message: "Enter this code on the verification page to finish signing in.", URL: verification, Code: userCode})
	value, err := pollPiAIDevice(session.Context, interval, expires, false, func() (piAIDevicePoll, error) {
		credential, errorCode, err := requestPiAIRadiusTokenPoll(session.Context, url.Values{
			"grant_type": {piAIOAuthDeviceGrant}, "client_id": {"pi-gateway"}, "device_code": {deviceCode},
		})
		if err != nil {
			return piAIDevicePoll{}, err
		}
		if credential != nil {
			return piAIDevicePoll{Status: "complete", Value: credential}, nil
		}
		switch errorCode {
		case "authorization_pending":
			return piAIDevicePoll{Status: "pending"}, nil
		case "slow_down":
			return piAIDevicePoll{Status: "slow_down"}, nil
		case "expired_token":
			return piAIDevicePoll{Status: "failed", Message: "Device authorization expired."}, nil
		case "access_denied":
			return piAIDevicePoll{Status: "failed", Message: "Device authorization was denied."}, nil
		default:
			return piAIDevicePoll{Status: "failed", Message: "Radius OAuth token request failed: " + errorCode}, nil
		}
	})
	if err != nil {
		return nil, err
	}
	credential, _ := value.(map[string]any)
	return credential, nil
}

func requestPiAIRadiusToken(ctx context.Context, values url.Values) (map[string]any, error) {
	credential, errorCode, err := requestPiAIRadiusTokenPoll(ctx, values)
	if err != nil {
		return nil, err
	}
	if credential == nil {
		return nil, errors.New("Radius OAuth token request failed: " + errorCode)
	}
	return credential, nil
}

func requestPiAIRadiusTokenPoll(ctx context.Context, values url.Values) (map[string]any, string, error) {
	response, status, err := piAIPostFormMap(ctx, strings.TrimRight(piAIRadiusGatewayURL, "/")+"/v1/oauth/token", values)
	if err != nil {
		return nil, "", err
	}
	if status/100 != 2 {
		return nil, piAIAuthStringValue(response["error"]), nil
	}
	credential, err := piAITokenCredential(response, nil, time.Minute)
	return credential, "", err
}

func (e *Engine) loginPiAIXAI(session AuthorizationSession) (map[string]any, error) {
	response, status, err := piAIPostFormMap(session.Context, piAIXAIDeviceCodeURL, url.Values{
		"client_id": {piAIXAIClientID}, "scope": {"openid profile email offline_access grok-cli:access api:access"}, "referrer": {"pi"},
	})
	if err != nil {
		return nil, err
	}
	deviceCode := piAIAuthStringValue(response["device_code"])
	userCode := piAIAuthStringValue(response["user_code"])
	verification := piAIAuthStringValue(response["verification_uri_complete"])
	if verification == "" {
		verification = piAIAuthStringValue(response["verification_uri"])
	}
	interval, _ := piAINumber(response["interval"])
	expires, ok := piAINumber(response["expires_in"])
	if status/100 != 2 || deviceCode == "" || userCode == "" || !piAITrustedHTTPSURL(verification) || !ok || expires <= 0 {
		return nil, errors.New("invalid xAI OAuth device authorization response")
	}
	session.Notify(AuthorizationNotice{Message: "Enter this code on the verification page to finish signing in.", URL: verification, Code: userCode})
	value, err := pollPiAIDevice(session.Context, interval, expires, true, func() (piAIDevicePoll, error) {
		body, code, err := piAIPostFormMap(session.Context, piAIXAITokenURL, url.Values{
			"grant_type": {piAIOAuthDeviceGrant}, "client_id": {piAIXAIClientID}, "device_code": {deviceCode},
		})
		if err != nil {
			return piAIDevicePoll{}, err
		}
		if code/100 == 2 {
			credential, err := piAITokenCredential(body, nil, 5*time.Minute)
			return piAIDevicePoll{Status: "complete", Value: credential}, err
		}
		switch piAIAuthStringValue(body["error"]) {
		case "authorization_pending":
			return piAIDevicePoll{Status: "pending"}, nil
		case "slow_down":
			interval, _ := piAINumber(body["interval"])
			return piAIDevicePoll{Status: "slow_down", IntervalSeconds: interval}, nil
		case "access_denied", "authorization_denied":
			return piAIDevicePoll{Status: "failed", Message: "xAI device authorization was denied"}, nil
		case "expired_token":
			return piAIDevicePoll{Status: "failed", Message: "xAI device code expired"}, nil
		default:
			return piAIDevicePoll{Status: "failed", Message: fmt.Sprintf("xAI OAuth device token polling failed (HTTP %d)", code)}, nil
		}
	})
	if err != nil {
		return nil, err
	}
	credential, _ := value.(map[string]any)
	return credential, nil
}

func piAITrustedHTTPURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Host != "" && (parsed.Scheme == "https" || parsed.Scheme == "http")
}

func piAITrustedHTTPSURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Host != "" && parsed.Scheme == "https"
}

func piAIPostFormMap(ctx context.Context, endpoint string, values url.Values) (map[string]any, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return piAIDoMap(req)
}

func piAIDoMap(req *http.Request) (map[string]any, int, error) {
	response, err := piAIAuthHTTPClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, response.StatusCode, err
	}
	mapped := map[string]any{}
	if len(strings.TrimSpace(string(body))) > 0 {
		if err := json.Unmarshal(body, &mapped); err != nil {
			return nil, response.StatusCode, fmt.Errorf("decode response from %s: %w", req.URL, err)
		}
	}
	return mapped, response.StatusCode, nil
}

func piAITokenCredential(response map[string]any, previous map[string]any, skew time.Duration) (map[string]any, error) {
	access := piAIAuthStringValue(response["access_token"])
	refresh := piAIAuthStringValue(response["refresh_token"])
	if refresh == "" {
		refresh = piAIAuthStringValue(previous["refresh"])
	}
	expires, ok := piAINumber(response["expires_in"])
	if access == "" || refresh == "" || !ok || expires <= 0 {
		return nil, errors.New("OAuth token response is missing access_token, refresh_token, or expires_in")
	}
	credential := clonePIAIPayload(previous)
	credential["type"] = "oauth"
	credential["access"] = access
	credential["refresh"] = refresh
	credential["expires"] = time.Now().UnixMilli() + int64(expires*1000) - int64(skew/time.Millisecond)
	if scope := piAIAuthStringValue(response["scope"]); scope != "" {
		credential["scope"] = scope
	}
	return credential, nil
}
