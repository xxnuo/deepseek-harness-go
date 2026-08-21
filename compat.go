package harness

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"time"

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
	randomSuffix := make([]byte, 6)
	if _, err := rand.Read(randomSuffix); err != nil {
		return fmt.Errorf("create atomic temporary name: %w", err)
	}
	tmp := path + "." + hex.EncodeToString(randomSuffix) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp) }()
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

const (
	fileLockInitialDelay = 20 * time.Millisecond
	fileLockMaxDelay     = 200 * time.Millisecond
	fileLockTimeout      = 2 * time.Second
)

// withOwnerFileLock serializes cross-process read-modify-write operations.
// The lock is intentionally never treated as stale: age cannot prove that
// another process stopped, so recovery remains an explicit operator action.
func withOwnerFileLock(path string, operation func() error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lockPath := path + ".lock"
	deadline := time.Now().Add(fileLockTimeout)
	delay := fileLockInitialDelay
	for {
		lock, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			_, _ = fmt.Fprintf(lock, "%d\n", os.Getpid())
			_ = lock.Sync()
			_ = lock.Close()
			break
		}
		contention := os.IsExist(err)
		if !contention && runtime.GOOS == "windows" && errors.Is(err, syscall.EPERM) {
			_, statErr := os.Lstat(lockPath)
			contention = statErr == nil
		}
		if !contention {
			return err
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("timed out waiting for the writer lock at %s", lockPath)
		}
		time.Sleep(delay)
		if delay < fileLockMaxDelay {
			delay *= 2
			if delay > fileLockMaxDelay {
				delay = fileLockMaxDelay
			}
		}
	}
	defer func() { _ = os.Remove(lockPath) }()
	return operation()
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

// prepareSettingsYAMLLocked materializes an absent editable document without
// replacing an existing document that another process may have just updated.
func prepareSettingsYAMLLocked(e *Engine) error {
	path := settingsYAMLPathFor(e)
	return withOwnerFileLock(path, func() error {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return writeOwnerOnlyFile(path, nil)
	})
}

// updateSettingsYAMLLocked applies one namespace edit to the latest document
// while the caller holds e.mu. It returns the committed user section and
// refreshes e.settings with the complete document observed under the lock.
func updateSettingsYAMLLocked(e *Engine, ns string, apply func(map[string]any) (map[string]any, error)) (map[string]any, error) {
	if !e.cfg.Persist {
		next, err := apply(cloneSettingsValue(e.settings[ns]))
		if err == nil {
			e.settings[ns] = cloneSettingsValue(next)
		}
		return next, err
	}
	path := settingsYAMLPathFor(e)
	var next map[string]any
	err := withOwnerFileLock(path, func() error {
		doc, err := readYAMLDocument(path)
		if errors.Is(err, os.ErrNotExist) {
			doc = map[string]any{}
		} else if err != nil {
			return err
		}
		for name, raw := range doc {
			if _, ok := raw.(map[string]any); !ok {
				return fmt.Errorf("settings namespace %q must be a mapping", name)
			}
		}
		current := cloneSettingsValue(e.settings[ns])
		if raw, ok := doc[ns]; ok {
			current, ok = raw.(map[string]any)
			if !ok {
				return fmt.Errorf("settings namespace %q must be a mapping", ns)
			}
			current = cloneSettingsValue(current)
		}
		next, err = apply(current)
		if err != nil {
			return err
		}
		doc[ns] = cloneSettingsValue(next)
		if !reflect.DeepEqual(next, current) {
			data, err := yaml.Marshal(doc)
			if err != nil {
				return err
			}
			if err := writeOwnerOnlyFile(path, data); err != nil {
				return err
			}
		}
		settings := make(map[string]map[string]any, len(doc))
		for name, raw := range doc {
			section := raw.(map[string]any)
			settings[name] = cloneSettingsValue(section)
		}
		e.settings = settings
		return nil
	})
	return next, err
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

// updateCredentialsYAMLLocked edits one credential against the latest file
// while the caller holds e.mu, then refreshes the in-memory snapshot.
func updateCredentialsYAMLLocked(e *Engine, ref string, value *string) (bool, error) {
	if !e.cfg.Persist {
		previous, exists := e.credentials[ref]
		if value == nil {
			delete(e.credentials, ref)
		} else {
			e.credentials[ref] = *value
		}
		return value == nil && exists || value != nil && (!exists || previous != *value), nil
	}
	path := credentialsYAMLPathFor(e)
	changed := false
	err := withOwnerFileLock(path, func() error {
		if err := assertOwnerOnlyCompat(path); err != nil {
			return err
		}
		doc, err := readYAMLDocument(path)
		if errors.Is(err, os.ErrNotExist) {
			doc = map[string]any{}
		} else if err != nil {
			return err
		}
		values := make(map[string]string, len(doc))
		for name, raw := range doc {
			text, ok := raw.(string)
			if !ok || text == "" || !validCredentialRef(name) {
				return errors.New("invalid credentials YAML document")
			}
			values[name] = text
		}
		previous, exists := values[ref]
		changed = value == nil && exists || value != nil && (!exists || previous != *value)
		if value == nil {
			delete(values, ref)
		} else {
			values[ref] = *value
		}
		if changed {
			data, err := yaml.Marshal(values)
			if err != nil {
				return err
			}
			if err := writeOwnerOnlyFile(path, data); err != nil {
				return err
			}
		}
		e.credentials = values
		return nil
	})
	return changed, err
}

const (
	deepSeekDefaultBaseURL      = "https://api.deepseek.com"
	deepSeekDefaultContext      = 1000000
	deepSeekDefaultMaxTokens    = 256000
	deepSeekDefaultStreamIdleMs = 300000
	deepSeekDefaultImageBytes   = DefaultMaxRequestImageBytes
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
		"maxRequestImageBytes": deepSeekDefaultImageBytes,
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
			"37": map[string]any{"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{"id": 28, "name": 29, "description": 30, "contextWindow": 33, "maxTokens": 36, "inputModalities": 47}},
			"39": map[string]any{"type": "array", "meta": map[string]any{"default": deepSeekDefaultModels()}, "inner": 37},
			"42": map[string]any{"type": "number", "meta": map[string]any{"min": 1, "default": deepSeekDefaultStreamIdleMs}},
			"43": map[string]any{"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{"apiKeyEnv": 2, "baseURL": 3, "thinking": 4, "reasoningEffort": 9, "maxTokens": 22, "defaultContextWindow": 26, "models": 39, "streamIdleTimeoutMs": 42, "maxRequestImageBytes": 48}},
			"44": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "text"},
			"45": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "image"},
			"46": map[string]any{"type": "union", "meta": map[string]any{}, "list": []any{44, 45}},
			"47": map[string]any{"type": "array", "meta": map[string]any{"default": []any{"text"}, "min": 1}, "inner": 46},
			"48": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultImageBytes}},
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

func deepSeekModalities(value any) []string {
	rows, ok := anySlice(value)
	if !ok || len(rows) == 0 {
		return []string{"text"}
	}
	out := make([]string, 0, len(rows))
	for _, raw := range rows {
		if modality, ok := raw.(string); ok {
			out = append(out, modality)
		}
	}
	if len(out) == 0 {
		return []string{"text"}
	}
	return out
}

func deepSeekCatalog(value any) []ModelInfo {
	settings, _ := value.(map[string]any)
	rawModels, exists := settings["models"]
	rows, ok := anySlice(rawModels)
	if !exists || !ok {
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
			ID: id, Name: name, Description: stringSetting(row["description"]), InputModalities: deepSeekModalities(row["inputModalities"]),
			ContextWindow: positiveIntSetting(row["contextWindow"], defaultContext),
			MaxTokens:     positiveIntSetting(row["maxTokens"], defaultMaxTokens),
		})
	}
	if len(out) == 0 && !exists {
		return []ModelInfo{{ID: "deepseek-v4-flash", Name: "DeepSeek-V4-Flash", InputModalities: []string{"text"}, ContextWindow: deepSeekDefaultContext, MaxTokens: deepSeekDefaultMaxTokens}, {ID: "deepseek-v4-pro", Name: "DeepSeek-V4-Pro", InputModalities: []string{"text"}, ContextWindow: deepSeekDefaultContext, MaxTokens: deepSeekDefaultMaxTokens}}
	}
	return out
}

func validateDeepSeekSettings(value map[string]any) error {
	for _, field := range []string{"maxTokens", "defaultContextWindow", "maxRequestImageBytes"} {
		if raw, exists := value[field]; exists {
			if parsed, ok := piAIPositiveInteger(raw); !ok || parsed <= 0 {
				return fmt.Errorf("llm-deepseek.%s must be a positive integer", field)
			}
		}
	}
	if raw, exists := value["streamIdleTimeoutMs"]; exists {
		if _, ok := positiveFiniteMilliseconds(raw); !ok {
			return fmt.Errorf("llm-deepseek.streamIdleTimeoutMs must be a positive finite number no greater than %d", piAIMaxTimerMillis)
		}
	}
	rawModels, exists := value["models"]
	if !exists {
		return nil
	}
	rows, ok := anySlice(rawModels)
	if !ok {
		return errors.New("llm-deepseek.models must be an array")
	}
	seen := make(map[string]bool, len(rows))
	for index, raw := range rows {
		model, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("llm-deepseek.models[%d] must be an object", index)
		}
		id, ok := model["id"].(string)
		if !ok || id == "" || id != strings.TrimSpace(id) {
			return fmt.Errorf("llm-deepseek.models[%d].id must be a non-empty string without surrounding whitespace", index)
		}
		if seen[id] {
			return fmt.Errorf("llm-deepseek model %q is duplicated", id)
		}
		seen[id] = true
		for _, field := range []string{"contextWindow", "maxTokens"} {
			if raw, exists := model[field]; exists {
				if parsed, ok := piAIPositiveInteger(raw); !ok || parsed <= 0 {
					return fmt.Errorf("llm-deepseek model %q %s must be a positive integer", id, field)
				}
			}
		}
		rawModalities, exists := model["inputModalities"]
		if !exists {
			continue
		}
		modalities, ok := anySlice(rawModalities)
		if !ok || len(modalities) == 0 {
			return fmt.Errorf("llm-deepseek model %q inputModalities must be a non-empty array", id)
		}
		seenModalities := map[string]bool{}
		for _, raw := range modalities {
			modality, ok := raw.(string)
			if !ok || modality != "text" && modality != "image" {
				return fmt.Errorf("llm-deepseek model %q inputModalities must contain only text and image", id)
			}
			if seenModalities[modality] {
				return fmt.Errorf("llm-deepseek model %q inputModalities must not contain duplicates", id)
			}
			seenModalities[modality] = true
		}
	}
	return nil
}

// managedDeepSeekProvider resolves endpoint, model catalog, and credentials
// for every request. This mirrors the upstream adapter's live settings
// composition and makes a credentials.set visible without rebuilding Engine.
type managedDeepSeekProvider struct{ engine *Engine }

func (p *managedDeepSeekProvider) ID() string   { return "deepseek-official" }
func (p *managedDeepSeekProvider) Name() string { return "DeepSeek" }

func (p *managedDeepSeekProvider) snapshot(settings map[string]any) (*OpenAIProvider, []ModelInfo, error) {
	if err := validateDeepSeekSettings(settings); err != nil {
		return nil, nil, err
	}
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
	provider := NewOpenAIProvider("deepseek-official", baseURL, apiKey, model)
	idleMillis, ok := positiveFiniteMilliseconds(settings["streamIdleTimeoutMs"])
	if !ok {
		idleMillis = deepSeekDefaultStreamIdleMs
	}
	provider.streamIdleTimeout = millisecondsDuration(idleMillis)
	return provider, deepSeekCatalog(settings), nil
}

func (p *managedDeepSeekProvider) Models(ctx context.Context) ([]ModelInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_, catalog, err := p.snapshot(deepSeekEffectiveSettings(p.engine))
	if err != nil {
		return nil, err
	}
	return catalog, nil
}

func deepSeekImageMessages(messages []ChatMessage) ([]ChatMessage, error) {
	out := make([]ChatMessage, 0, len(messages)+1)
	pendingToolImages := make([]ChatImage, 0)
	flushToolImages := func() {
		if len(pendingToolImages) == 0 {
			return
		}
		out = append(out, ChatMessage{
			Role: "user", Content: "Attached image(s) from tool result:",
			Images: append([]ChatImage(nil), pendingToolImages...),
		})
		pendingToolImages = pendingToolImages[:0]
	}
	for _, message := range messages {
		if chatMessageHasImage(message) && message.Role != "user" && message.Role != "tool" {
			return nil, &ProviderError{Code: "UNSUPPORTED_CONTENT", Message: fmt.Sprintf("DeepSeek cannot represent image content in a %s message", message.Role)}
		}
		if message.Role == "tool" {
			tool := message
			if chatMessageHasImage(tool) {
				for _, part := range chatContentParts(tool) {
					if part.Type == "image" {
						pendingToolImages = append(pendingToolImages, ChatImage{MediaType: part.MediaType, Data: part.Data})
					}
				}
				tool.Images = nil
				tool.Parts = nil
				if tool.Content == "" {
					tool.Content = "(see attached image)"
				}
			}
			out = append(out, tool)
			continue
		}
		flushToolImages()
		out = append(out, message)
	}
	flushToolImages()
	return out, nil
}

func (p *managedDeepSeekProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	settings := deepSeekEffectiveSettings(p.engine)
	snapshot, catalog, err := p.snapshot(settings)
	if err != nil {
		return Completion{}, err
	}
	hasImages := false
	for _, message := range req.Messages {
		messageHasImages := message.HadImages || chatMessageHasImage(message) || contentBlocksHaveImage(message.Blocks)
		if messageHasImages && message.Role != "user" && message.Role != "tool" {
			return Completion{}, &ProviderError{Code: "UNSUPPORTED_CONTENT", Message: fmt.Sprintf("DeepSeek cannot represent image content in a %s message", message.Role)}
		}
		if messageHasImages {
			hasImages = true
		}
	}
	if hasImages {
		acceptsImages := false
		for _, model := range catalog {
			if model.ID != req.Model {
				continue
			}
			for _, modality := range model.InputModalities {
				if modality == "image" {
					acceptsImages = true
					break
				}
			}
			break
		}
		if !acceptsImages {
			return Completion{}, &ProviderError{Code: "UNSUPPORTED_CONTENT", Message: fmt.Sprintf("DeepSeek model %q does not accept image input", req.Model)}
		}
	}
	req.Messages = offloadRequestImages(req.Messages, positiveIntSetting(settings["maxRequestImageBytes"], deepSeekDefaultImageBytes))
	messages, err := deepSeekImageMessages(req.Messages)
	if err != nil {
		return Completion{}, err
	}
	req.Messages = messages
	if snapshot.apiKey == "" {
		return Completion{}, &ProviderError{Code: "AUTH", Message: "missing credential: DEEPSEEK_API_KEY"}
	}
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
