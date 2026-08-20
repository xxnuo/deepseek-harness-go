package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	settingsYAMLName    = "settings.yaml"
	credentialsYAMLName = ".credentials.yaml"
)

func settingsYAMLPathFor(e *Engine) string {
	return filepath.Join(e.cfg.DataDir, settingsYAMLName)
}

func credentialsYAMLPathFor(e *Engine) string {
	return filepath.Join(e.cfg.DataDir, credentialsYAMLName)
}

// normalizeYAMLValue converts yaml.v3's interface maps into the JSON-shaped
// maps used throughout the host. It also rejects non-string map keys instead
// of silently stringifying a user's configuration.
func normalizeYAMLValue(value any) (any, error) {
	switch value := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, item := range value {
			normalized, err := normalizeYAMLValue(item)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	case map[any]any:
		out := make(map[string]any, len(value))
		for key, item := range value {
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("YAML map key must be a string, got %T", key)
			}
			normalized, err := normalizeYAMLValue(item)
			if err != nil {
				return nil, err
			}
			out[name] = normalized
		}
		return out, nil
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			normalized, err := normalizeYAMLValue(item)
			if err != nil {
				return nil, err
			}
			out[i] = normalized
		}
		return out, nil
	default:
		return value, nil
	}
}

func readYAMLDocument(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return map[string]any{}, nil
	}
	var raw any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid YAML document %s: %w", path, err)
	}
	normalized, err := normalizeYAMLValue(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML document %s: %w", path, err)
	}
	root, ok := normalized.(map[string]any)
	if !ok || root == nil {
		return nil, fmt.Errorf("YAML document %s must contain a mapping", path)
	}
	return root, nil
}

func writeOwnerOnlyFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_ = f.Chmod(0o600)
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if closeErr := f.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return writeErr
	}
	return os.Rename(tmp, path)
}

func assertOwnerOnlyCompat(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("file %s must not be readable beyond its owner", path)
	}
	return nil
}

func loadSettingsYAML(e *Engine) (bool, error) {
	path := settingsYAMLPathFor(e)
	doc, err := readYAMLDocument(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	for ns, raw := range doc {
		section, ok := raw.(map[string]any)
		if !ok {
			return true, fmt.Errorf("settings namespace %q must be a mapping", ns)
		}
		e.settings[ns] = cloneSettingsValue(section)
	}
	return true, nil
}

func saveSettingsYAMLLocked(e *Engine) error {
	data, err := yaml.Marshal(e.settings)
	if err != nil {
		return err
	}
	if err := writeOwnerOnlyFile(settingsYAMLPathFor(e), data); err != nil {
		return err
	}
	return nil
}

func loadCredentialsYAML(e *Engine) (bool, error) {
	path := credentialsYAMLPathFor(e)
	if err := assertOwnerOnlyCompat(path); err != nil {
		return true, err
	}
	doc, err := readYAMLDocument(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	values := make(map[string]string, len(doc))
	for ref, raw := range doc {
		value, ok := raw.(string)
		if !ok || value == "" || !validCredentialRef(ref) {
			return true, errors.New("invalid credentials YAML document")
		}
		values[ref] = value
	}
	e.credentials = values
	return true, nil
}

func saveCredentialsYAMLLocked(e *Engine) error {
	data, err := yaml.Marshal(e.credentials)
	if err != nil {
		return err
	}
	if err := writeOwnerOnlyFile(credentialsYAMLPathFor(e), data); err != nil {
		return err
	}
	return nil
}

const (
	deepSeekDefaultBaseURL      = "https://api.deepseek.com"
	deepSeekDefaultContext      = 1000000
	deepSeekDefaultMaxTokens    = 256000
	deepSeekDefaultStreamIdleMs = 300000
)

func deepSeekDefaultModels() []any {
	return []any{
		map[string]any{"id": "deepseek-v4-flash", "name": "DeepSeek-V4-Flash", "contextWindow": deepSeekDefaultContext},
		map[string]any{"id": "deepseek-v4-pro", "name": "DeepSeek-V4-Pro", "contextWindow": deepSeekDefaultContext},
	}
}

func deepSeekBaseSettings(e *Engine) map[string]any {
	baseURL := strings.TrimSpace(os.Getenv("DEEPSEEK_BASE_URL"))
	if baseURL == "" {
		baseURL = e.cfg.BaseURL
	}
	if baseURL == "" {
		baseURL = deepSeekDefaultBaseURL
	}
	return map[string]any{
		"apiKeyEnv":            "DEEPSEEK_API_KEY",
		"baseURL":              baseURL,
		"maxTokens":            deepSeekDefaultMaxTokens,
		"defaultContextWindow": deepSeekDefaultContext,
		"models":               deepSeekDefaultModels(),
		"streamIdleTimeoutMs":  deepSeekDefaultStreamIdleMs,
	}
}

// deepSeekSettingsSchema is the schemastery wire envelope used by the
// original form renderer. It deliberately contains only the stable fields of
// the shipped adapter; unknown future fields remain editable through YAML.
func deepSeekSettingsSchema() map[string]any {
	return map[string]any{
		"uid": 43,
		"refs": map[string]any{
			"2":  map[string]any{"type": "string", "meta": map[string]any{"role": "credential-ref", "default": "DEEPSEEK_API_KEY"}},
			"3":  map[string]any{"type": "string", "meta": map[string]any{}},
			"4":  map[string]any{"type": "union", "meta": map[string]any{}, "list": []any{6, 8}},
			"6":  map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "enabled"},
			"8":  map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "disabled"},
			"9":  map[string]any{"type": "union", "meta": map[string]any{}, "list": []any{11, 13, 15, 17}},
			"11": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "off"},
			"13": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "low"},
			"15": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "high"},
			"17": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "max"},
			"22": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "max": 9007199254740991, "default": deepSeekDefaultMaxTokens}},
			"26": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultContext}},
			"28": map[string]any{"type": "string", "meta": map[string]any{"required": true}},
			"29": map[string]any{"type": "string", "meta": map[string]any{}},
			"30": map[string]any{"type": "string", "meta": map[string]any{}},
			"33": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1}},
			"36": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1}},
			"37": map[string]any{"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{"id": 28, "name": 29, "description": 30, "contextWindow": 33, "maxTokens": 36}},
			"39": map[string]any{"type": "array", "meta": map[string]any{"default": deepSeekDefaultModels()}, "inner": 37},
			"42": map[string]any{"type": "number", "meta": map[string]any{"min": 1, "default": deepSeekDefaultStreamIdleMs}},
			"43": map[string]any{"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{"apiKeyEnv": 2, "baseURL": 3, "thinking": 4, "reasoningEffort": 9, "maxTokens": 22, "defaultContextWindow": 26, "models": 39, "streamIdleTimeoutMs": 42}},
		},
	}
}

func deepSeekEffectiveSettings(e *Engine) map[string]any {
	base := deepSeekBaseSettings(e)
	e.mu.RLock()
	user := cloneSettingsValue(e.settings["llm-deepseek"])
	e.mu.RUnlock()
	return mergeSettings(base, user)
}

func stringSetting(value any) string {
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return ""
}

func positiveIntSetting(value any, fallback int) int {
	switch number := value.(type) {
	case int:
		if number > 0 {
			return number
		}
	case int64:
		if number > 0 && number <= int64(^uint(0)>>1) {
			return int(number)
		}
	case float64:
		if number > 0 && number == float64(int(number)) {
			return int(number)
		}
	}
	return fallback
}

func deepSeekCatalog(value any) []ModelInfo {
	settings, _ := value.(map[string]any)
	rows, ok := settings["models"].([]any)
	if !ok || len(rows) == 0 {
		rows = deepSeekDefaultModels()
	}
	defaultContext := positiveIntSetting(settings["defaultContextWindow"], deepSeekDefaultContext)
	defaultMaxTokens := positiveIntSetting(settings["maxTokens"], deepSeekDefaultMaxTokens)
	out := make([]ModelInfo, 0, len(rows))
	seen := map[string]bool{}
	for _, raw := range rows {
		row, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id := stringSetting(row["id"])
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		name := stringSetting(row["name"])
		if name == "" {
			name = id
		}
		out = append(out, ModelInfo{
			ID: id, Name: name, Description: stringSetting(row["description"]), InputModalities: []string{"text"},
			ContextWindow: positiveIntSetting(row["contextWindow"], defaultContext),
			MaxTokens:     positiveIntSetting(row["maxTokens"], defaultMaxTokens),
		})
	}
	if len(out) == 0 {
		return []ModelInfo{{ID: "deepseek-v4-flash", Name: "DeepSeek-V4-Flash", InputModalities: []string{"text"}, ContextWindow: deepSeekDefaultContext, MaxTokens: deepSeekDefaultMaxTokens}, {ID: "deepseek-v4-pro", Name: "DeepSeek-V4-Pro", InputModalities: []string{"text"}, ContextWindow: deepSeekDefaultContext, MaxTokens: deepSeekDefaultMaxTokens}}
	}
	return out
}

// managedDeepSeekProvider resolves endpoint, model catalog, and credentials
// for every request. This mirrors the upstream adapter's live settings
// composition and makes a credentials.set visible without rebuilding Engine.
type managedDeepSeekProvider struct{ engine *Engine }

func (p *managedDeepSeekProvider) ID() string   { return "deepseek-official" }
func (p *managedDeepSeekProvider) Name() string { return "DeepSeek" }

func (p *managedDeepSeekProvider) snapshot() (*OpenAIProvider, []ModelInfo) {
	settings := deepSeekEffectiveSettings(p.engine)
	apiKeyRef := stringSetting(settings["apiKeyEnv"])
	if apiKeyRef == "" {
		apiKeyRef = "DEEPSEEK_API_KEY"
	}
	p.engine.mu.RLock()
	cfg := p.engine.cfg
	p.engine.mu.RUnlock()
	apiKey, _, _ := p.engine.resolveCredential(apiKeyRef)
	baseURL := stringSetting(settings["baseURL"])
	if baseURL == "" {
		baseURL = strings.TrimSpace(os.Getenv("DEEPSEEK_BASE_URL"))
	}
	if baseURL == "" {
		baseURL = cfg.BaseURL
	}
	if baseURL == "" {
		baseURL = deepSeekDefaultBaseURL
	}
	model := cfg.Model
	if model == "" {
		model = "deepseek-chat"
	}
	return NewOpenAIProvider("deepseek-official", baseURL, apiKey, model), deepSeekCatalog(settings)
}

func (p *managedDeepSeekProvider) Models(ctx context.Context) ([]ModelInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_, catalog := p.snapshot()
	return catalog, nil
}

func (p *managedDeepSeekProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	snapshot, _ := p.snapshot()
	if snapshot.apiKey == "" {
		return Completion{}, &ProviderError{Code: "AUTH", Message: "missing credential: DEEPSEEK_API_KEY"}
	}
	settings := deepSeekEffectiveSettings(p.engine)
	effort := req.ReasoningEffort
	if effort == "" {
		effort = stringSetting(settings["reasoningEffort"])
	}
	switch effort {
	case "":
	case "off":
		req.Thinking = "disabled"
		effort = ""
	case "low", "high", "max":
		if stringSetting(settings["thinking"]) == "disabled" {
			return Completion{}, fmt.Errorf("DeepSeek deployment does not support reasoning effort %q", effort)
		}
		req.Thinking = "enabled"
	default:
		return Completion{}, fmt.Errorf("DeepSeek does not support reasoning effort %q", effort)
	}
	if req.Thinking == "" {
		req.Thinking = stringSetting(settings["thinking"])
	}
	req.ReasoningEffort = effort
	if req.MaxTokens <= 0 {
		req.MaxTokens = positiveIntSetting(settings["maxTokens"], deepSeekDefaultMaxTokens)
	}
	return snapshot.Complete(ctx, req, onDelta)
}
