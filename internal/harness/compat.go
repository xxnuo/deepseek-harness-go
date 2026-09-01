package harness

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

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
	return withOwnerFileLockContext(context.Background(), path, fileLockTimeout, operation)
}

func withOwnerFileLockContext(ctx context.Context, path string, wait time.Duration, operation func() error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lockPath := path + ".lock"
	deadline := time.Now().Add(wait)
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
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
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

const (
	deepSeekDefaultBaseURL                    = "https://api.deepseek.com"
	deepSeekDefaultContext                    = 1000000
	deepSeekDefaultMaxTokens                  = 256000
	deepSeekDefaultStreamIdleMs               = 300000
	deepSeekDefaultMaxRequestFilesBytes       = 128 << 20
	deepSeekDefaultMaxInlineRequestImageBytes = DefaultMaxRequestImageBytes
	deepSeekDefaultMaxImagesPerRequest        = 600
	deepSeekDefaultRequestImagePixels         = 640000
	deepSeekDefaultLowImagePixels             = 512 * 512
	deepSeekDefaultRequestImageMaxBytes       = 1 << 20
	deepSeekDefaultImageOffloadByteQuantum    = 64 << 20
	deepSeekDefaultInlineOffloadByteQuantum   = 10 << 20
	deepSeekDefaultImageOffloadCountQuantum   = 20
	deepSeekDefaultFilesAPITimeoutMs          = 60000
	deepSeekDefaultFileExpirySeconds          = DeepSeekFileExpirySeconds
	deepSeekDefaultFileRefreshMarginSeconds   = DeepSeekFileRefreshSeconds
	deepSeekDefaultFileQuotaCleanupBatch      = DeepSeekQuotaCleanupBatch
)

func deepSeekDefaultModels() []any {
	return []any{
		map[string]any{"id": "deepseek-v4-flash", "name": "DeepSeek-V4-Flash", "contextWindow": deepSeekDefaultContext},
		map[string]any{"id": "deepseek-v4-pro", "name": "DeepSeek-V4-Pro", "contextWindow": deepSeekDefaultContext},
		map[string]any{"id": "deepseek-v4-flash-vision-exp", "name": "DeepSeek-V4-Flash-Vision-Exp", "contextWindow": deepSeekDefaultContext, "inputModalities": []any{"text", "image"}, "imagePixelBudget": deepSeekDefaultRequestImagePixels, "imageMaxBytes": deepSeekDefaultRequestImageMaxBytes},
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
		"apiKeyEnv":                     "DEEPSEEK_API_KEY",
		"baseURL":                       baseURL,
		"maxTokens":                     deepSeekDefaultMaxTokens,
		"defaultContextWindow":          deepSeekDefaultContext,
		"models":                        deepSeekDefaultModels(),
		"streamIdleTimeoutMs":           deepSeekDefaultStreamIdleMs,
		"maxRequestFilesBytes":          deepSeekDefaultMaxRequestFilesBytes,
		"maxInlineRequestImageBytes":    deepSeekDefaultMaxInlineRequestImageBytes,
		"maxImagesPerRequest":           deepSeekDefaultMaxImagesPerRequest,
		"imageOffloadByteQuantum":       deepSeekDefaultImageOffloadByteQuantum,
		"inlineImageOffloadByteQuantum": deepSeekDefaultInlineOffloadByteQuantum,
		"imageOffloadCountQuantum":      deepSeekDefaultImageOffloadCountQuantum,
		"filesApiTimeoutMs":             deepSeekDefaultFilesAPITimeoutMs,
		"fileExpiresAfterSeconds":       deepSeekDefaultFileExpirySeconds,
		"fileRefreshMarginSeconds":      deepSeekDefaultFileRefreshMarginSeconds,
		"fileQuotaCleanupBatch":         deepSeekDefaultFileQuotaCleanupBatch,
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
			"37": map[string]any{"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{"id": 28, "name": 29, "description": 30, "contextWindow": 33, "maxTokens": 36, "inputModalities": 47, "imagePixelBudget": 59, "imageMaxBytes": 60, "imageDetail": 61}},
			"39": map[string]any{"type": "array", "meta": map[string]any{"default": deepSeekDefaultModels()}, "inner": 37},
			"42": map[string]any{"type": "number", "meta": map[string]any{"min": 1, "default": deepSeekDefaultStreamIdleMs}},
			"43": map[string]any{"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{"apiKeyEnv": 2, "baseURL": 3, "thinking": 4, "reasoningEffort": 9, "maxTokens": 22, "defaultContextWindow": 26, "models": 39, "streamIdleTimeoutMs": 42, "maxRequestFilesBytes": 49, "maxInlineRequestImageBytes": 50, "maxImagesPerRequest": 51, "imageOffloadByteQuantum": 52, "inlineImageOffloadByteQuantum": 53, "imageOffloadCountQuantum": 54, "filesApiTimeoutMs": 55, "fileExpiresAfterSeconds": 56, "fileRefreshMarginSeconds": 57, "fileQuotaCleanupBatch": 58}},
			"44": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "text"},
			"45": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "image"},
			"46": map[string]any{"type": "union", "meta": map[string]any{}, "list": []any{44, 45}},
			"47": map[string]any{"type": "array", "meta": map[string]any{"default": []any{"text"}, "min": 1}, "inner": 46},
			"48": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultMaxInlineRequestImageBytes}},
			"49": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultMaxRequestFilesBytes}},
			"50": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultMaxInlineRequestImageBytes}},
			"51": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultMaxImagesPerRequest}},
			"52": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultImageOffloadByteQuantum}},
			"53": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultInlineOffloadByteQuantum}},
			"54": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultImageOffloadCountQuantum}},
			"55": map[string]any{"type": "number", "meta": map[string]any{"min": 1, "default": deepSeekDefaultFilesAPITimeoutMs}},
			"56": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": DeepSeekMinFileExpirySeconds, "max": DeepSeekMaxFileExpirySeconds, "default": deepSeekDefaultFileExpirySeconds}},
			"57": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 0, "default": deepSeekDefaultFileRefreshMarginSeconds}},
			"58": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "max": 1000, "default": deepSeekDefaultFileQuotaCleanupBatch}},
			"59": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultRequestImagePixels}},
			"60": map[string]any{"type": "number", "meta": map[string]any{"step": 1, "min": 1, "default": deepSeekDefaultRequestImageMaxBytes}},
			"61": map[string]any{"type": "union", "meta": map[string]any{}, "list": []any{62, 63}},
			"62": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "auto"},
			"63": map[string]any{"type": "const", "meta": map[string]any{"required": true}, "value": "low"},
		},
	}
}

func deepSeekEffectiveSettings(e *Engine) map[string]any {
	base := deepSeekBaseSettings(e)
	e.mu.RLock()
	user := cloneSettingsValue(e.settings["llm-deepseek"])
	e.mu.RUnlock()
	merged := mergeSettings(base, user)
	// Keep old hand-authored settings readable during the rc.1 -> rc.2
	// transition; the rc.2 schema no longer exposes this name.
	if _, hasNew := user["maxInlineRequestImageBytes"]; !hasNew {
		if legacy, hasLegacy := user["maxRequestImageBytes"]; hasLegacy {
			merged["maxInlineRequestImageBytes"] = legacy
			if _, hasQuantum := user["inlineImageOffloadByteQuantum"]; !hasQuantum {
				// rc.1 removed the oldest inline image one byte-budget crossing at
				// a time; preserve that behavior for legacy settings.
				merged["inlineImageOffloadByteQuantum"] = 1
			}
		}
	}
	return merged
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
			Reasoning:     deepSeekReasoningInfo(settings),
		})
	}
	if len(out) == 0 && !exists {
		return []ModelInfo{{ID: "deepseek-v4-flash", Name: "DeepSeek-V4-Flash", InputModalities: []string{"text"}, ContextWindow: deepSeekDefaultContext, MaxTokens: deepSeekDefaultMaxTokens, Reasoning: deepSeekReasoningInfo(settings)}, {ID: "deepseek-v4-pro", Name: "DeepSeek-V4-Pro", InputModalities: []string{"text"}, ContextWindow: deepSeekDefaultContext, MaxTokens: deepSeekDefaultMaxTokens, Reasoning: deepSeekReasoningInfo(settings)}, {ID: "deepseek-v4-flash-vision-exp", Name: "DeepSeek-V4-Flash-Vision-Exp", InputModalities: []string{"text", "image"}, ContextWindow: deepSeekDefaultContext, MaxTokens: deepSeekDefaultMaxTokens, Reasoning: deepSeekReasoningInfo(settings)}}
	}
	return out
}

func deepSeekReasoningInfo(settings map[string]any) *ModelReasoningInfo {
	if stringSetting(settings["thinking"]) == "disabled" {
		return &ModelReasoningInfo{Efforts: []ReasoningEffortInfo{{ID: "off", Name: "Off"}}, DefaultEffort: "off"}
	}
	defaultEffort := stringSetting(settings["reasoningEffort"])
	if defaultEffort == "" {
		defaultEffort = "high"
	}
	return &ModelReasoningInfo{
		Efforts:       []ReasoningEffortInfo{{ID: "off", Name: "Off"}, {ID: "low", Name: "Low"}, {ID: "high", Name: "High"}, {ID: "max", Name: "Max"}},
		DefaultEffort: defaultEffort,
	}
}

func deepSeekImagePolicies(value map[string]any) map[string]ImageRequestPolicy {
	policies := make(map[string]ImageRequestPolicy)
	rows, ok := anySlice(value["models"])
	if !ok {
		rows, _ = anySlice(deepSeekDefaultModels())
	}
	for _, raw := range rows {
		model, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id := stringSetting(model["id"])
		if id == "" {
			continue
		}
		pixels := deepSeekDefaultRequestImagePixels
		if stringSetting(model["imageDetail"]) == "low" {
			pixels = deepSeekDefaultLowImagePixels
		}
		pixels = positiveIntSetting(model["imagePixelBudget"], pixels)
		bytes := positiveIntSetting(model["imageMaxBytes"], deepSeekDefaultRequestImageMaxBytes)
		policies[id] = ImageRequestPolicy{MaxPixels: pixels, MaxBytes: bytes}
	}
	return policies
}

func validateDeepSeekSettings(value map[string]any) error {
	positive := func(field string, fallback int) (int, error) {
		raw, exists := value[field]
		if !exists {
			return fallback, nil
		}
		parsed, ok := piAIPositiveInteger(raw)
		if !ok {
			return 0, fmt.Errorf("llm-deepseek.%s must be a positive safe integer", field)
		}
		return parsed, nil
	}
	for _, field := range []string{"maxTokens", "defaultContextWindow"} {
		if _, err := positive(field, 1); err != nil {
			return err
		}
	}
	if raw, exists := value["streamIdleTimeoutMs"]; exists {
		if _, ok := positiveFiniteMilliseconds(raw); !ok {
			return fmt.Errorf("llm-deepseek.streamIdleTimeoutMs must be a positive finite number no greater than %d", piAIMaxTimerMillis)
		}
	}
	if raw, exists := value["filesApiTimeoutMs"]; exists {
		if _, ok := positiveFiniteMilliseconds(raw); !ok {
			return fmt.Errorf("llm-deepseek.filesApiTimeoutMs must be a positive finite number no greater than %d", piAIMaxTimerMillis)
		}
	}
	maxFiles, err := positive("maxRequestFilesBytes", deepSeekDefaultMaxRequestFilesBytes)
	if err != nil {
		return err
	}
	maxInline, err := positive("maxInlineRequestImageBytes", deepSeekDefaultMaxInlineRequestImageBytes)
	if err != nil {
		return err
	}
	maxImages, err := positive("maxImagesPerRequest", deepSeekDefaultMaxImagesPerRequest)
	if err != nil {
		return err
	}
	fileQuantum, err := positive("imageOffloadByteQuantum", deepSeekDefaultImageOffloadByteQuantum)
	if err != nil {
		return err
	}
	if fileQuantum > maxFiles {
		return errors.New("llm-deepseek.imageOffloadByteQuantum must not exceed maxRequestFilesBytes")
	}
	inlineQuantum, err := positive("inlineImageOffloadByteQuantum", deepSeekDefaultInlineOffloadByteQuantum)
	if err != nil {
		return err
	}
	if inlineQuantum > maxInline {
		return errors.New("llm-deepseek.inlineImageOffloadByteQuantum must not exceed maxInlineRequestImageBytes")
	}
	countQuantum, err := positive("imageOffloadCountQuantum", deepSeekDefaultImageOffloadCountQuantum)
	if err != nil {
		return err
	}
	if countQuantum > maxImages {
		return errors.New("llm-deepseek.imageOffloadCountQuantum must not exceed maxImagesPerRequest")
	}
	expiry, err := positive("fileExpiresAfterSeconds", deepSeekDefaultFileExpirySeconds)
	if err != nil {
		return err
	}
	if expiry < DeepSeekMinFileExpirySeconds || expiry > DeepSeekMaxFileExpirySeconds {
		return fmt.Errorf("llm-deepseek.fileExpiresAfterSeconds must be an integer from %d through %d", DeepSeekMinFileExpirySeconds, DeepSeekMaxFileExpirySeconds)
	}
	refresh := deepSeekDefaultFileRefreshMarginSeconds
	if raw, exists := value["fileRefreshMarginSeconds"]; exists {
		var ok bool
		refresh, ok = nonNegativeInteger(raw)
		if !ok {
			return errors.New("llm-deepseek.fileRefreshMarginSeconds must be a non-negative integer")
		}
	}
	if refresh >= expiry {
		return errors.New("llm-deepseek.fileRefreshMarginSeconds must be a non-negative integer below fileExpiresAfterSeconds")
	}
	quota, err := positive("fileQuotaCleanupBatch", deepSeekDefaultFileQuotaCleanupBatch)
	if err != nil {
		return err
	}
	if quota > 1000 {
		return errors.New("llm-deepseek.fileQuotaCleanupBatch must be an integer from 1 through 1000")
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
		rawModalities, hasModalities := model["inputModalities"]
		modalities := []any{"text"}
		if hasModalities {
			parsed, valid := anySlice(rawModalities)
			if !valid || len(parsed) == 0 {
				return fmt.Errorf("llm-deepseek model %q inputModalities must be a non-empty array", id)
			}
			modalities = parsed
		}
		seenModalities := map[string]bool{}
		hasImage := false
		for _, raw := range modalities {
			modality, valid := raw.(string)
			if !valid || modality != "text" && modality != "image" {
				return fmt.Errorf("llm-deepseek model %q inputModalities must contain only text and image", id)
			}
			if seenModalities[modality] {
				return fmt.Errorf("llm-deepseek model %q inputModalities must not contain duplicates", id)
			}
			seenModalities[modality] = true
			hasImage = hasImage || modality == "image"
		}
		_, hasPixelBudget := model["imagePixelBudget"]
		_, hasMaxBytes := model["imageMaxBytes"]
		_, hasImageDetail := model["imageDetail"]
		if !hasImage && (hasPixelBudget || hasMaxBytes || hasImageDetail) {
			return fmt.Errorf("llm-deepseek text-only catalog model %q cannot declare image request limits", id)
		}
		for _, field := range []string{"imagePixelBudget", "imageMaxBytes"} {
			if raw, exists := model[field]; exists {
				if parsed, valid := piAIPositiveInteger(raw); !valid || parsed <= 0 {
					return fmt.Errorf("llm-deepseek model %q %s must be a positive safe integer", id, field)
				}
			}
		}
		if raw, exists := model["imageDetail"]; exists {
			if detail, valid := raw.(string); !valid || detail != "auto" && detail != "low" {
				return fmt.Errorf("llm-deepseek model %q imageDetail must be auto or low", id)
			}
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

func (e *Engine) deepSeekFileStoreFor(cfg Config) *DeepSeekFileStore {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.deepSeekFileStore == nil {
		indexPath := filepath.Join(cfg.DataDir, "llm-deepseek", "files-v3.json")
		e.deepSeekFileStore = NewDeepSeekFileStore(NewDeepSeekUploadIndex(indexPath), &http.Client{})
	}
	return e.deepSeekFileStore
}

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
	provider.deepSeekExtensions = p.engine
	provider.deepSeekFiles = p.engine.deepSeekFileStoreFor(cfg)
	provider.deepSeekFileConnection = DeepSeekFileConnection{BaseURL: baseURL, APIKey: apiKey}
	provider.deepSeekFilePolicy = DeepSeekFilePolicy{ExpiresAfterSeconds: DeepSeekFileExpirySeconds, RefreshMargin: DeepSeekFileRefreshSeconds * time.Second, QuotaCleanupBatch: DeepSeekQuotaCleanupBatch, APITimeout: time.Minute}
	provider.deepSeekMaxRequestFilesBytes = positiveIntSetting(settings["maxRequestFilesBytes"], deepSeekDefaultMaxRequestFilesBytes)
	provider.deepSeekMaxInlineRequestImageBytes = positiveIntSetting(settings["maxInlineRequestImageBytes"], deepSeekDefaultMaxInlineRequestImageBytes)
	provider.deepSeekMaxImagesPerRequest = positiveIntSetting(settings["maxImagesPerRequest"], deepSeekDefaultMaxImagesPerRequest)
	provider.deepSeekImageOffloadByteQuantum = positiveIntSetting(settings["imageOffloadByteQuantum"], deepSeekDefaultImageOffloadByteQuantum)
	provider.deepSeekInlineImageOffloadByteQuantum = positiveIntSetting(settings["inlineImageOffloadByteQuantum"], deepSeekDefaultInlineOffloadByteQuantum)
	provider.deepSeekImageOffloadCountQuantum = positiveIntSetting(settings["imageOffloadCountQuantum"], deepSeekDefaultImageOffloadCountQuantum)
	if timeout, ok := positiveFiniteMilliseconds(settings["filesApiTimeoutMs"]); ok {
		provider.deepSeekFilesAPITimeout = millisecondsDuration(timeout)
		provider.deepSeekFilePolicy.APITimeout = provider.deepSeekFilesAPITimeout
	}
	provider.deepSeekFilePolicy.ExpiresAfterSeconds = positiveIntSetting(settings["fileExpiresAfterSeconds"], deepSeekDefaultFileExpirySeconds)
	provider.deepSeekFilePolicy.QuotaCleanupBatch = positiveIntSetting(settings["fileQuotaCleanupBatch"], deepSeekDefaultFileQuotaCleanupBatch)
	if raw, exists := settings["fileRefreshMarginSeconds"]; exists {
		provider.deepSeekFilePolicy.RefreshMargin = time.Duration(positiveIntSetting(raw, 0)) * time.Second
	}
	provider.deepSeekModelImagePolicies = deepSeekImagePolicies(settings)
	provider.deepSeekRequestImagePolicy = ImageRequestPolicy{MaxPixels: deepSeekDefaultRequestImagePixels, MaxBytes: deepSeekDefaultRequestImageMaxBytes}
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

func (p *managedDeepSeekProvider) ResolveModelInfo(ctx context.Context, model string) (ModelInfo, error) {
	models, err := p.Models(ctx)
	if err != nil {
		return ModelInfo{}, err
	}
	for _, info := range models {
		if info.ID == model {
			return info, nil
		}
	}
	return ModelInfo{}, fmt.Errorf("model %q is unavailable", model)
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
						pendingToolImages = append(pendingToolImages, ChatImage{MediaType: part.MediaType, Data: part.Data, FileID: part.FileID, AttachmentID: part.AttachmentID})
					}
				}
				tool.Images = nil
				tool.Parts = nil
				if tool.Content == "" {
					tool.Content = "(no output)"
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
	durableImageRefs := collectDeepSeekImageRefs(req.Messages)
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
	requestImagePolicy := snapshot.deepSeekRequestImagePolicy
	if modelPolicy, ok := snapshot.deepSeekModelImagePolicies[req.Model]; ok {
		requestImagePolicy = modelPolicy
	}
	if !req.deepSeekForceInline && snapshot.deepSeekFiles != nil {
		// Apply the raw durable-byte/count projection before reading attachments.
		// The request-version byte cap is usually smaller than the durable cap, so
		// this conservative pass is what keeps omitted history out of the image
		// transform and Files API paths entirely.
		refBytes := make(map[string]int, len(durableImageRefs))
		refByID := make(map[string]ImageAttachmentRef, len(durableImageRefs))
		for _, ref := range durableImageRefs {
			conservativeBytes := ref.Bytes
			if requestImagePolicy.MaxBytes > 0 && conservativeBytes > requestImagePolicy.MaxBytes {
				conservativeBytes = requestImagePolicy.MaxBytes
			}
			refBytes[ref.AttachmentID] = conservativeBytes
			refByID[ref.AttachmentID] = ref
		}
		req.Messages = offloadDeepSeekImagesWithRefs(req.Messages, nil, refBytes,
			snapshot.deepSeekMaxRequestFilesBytes, 0, snapshot.deepSeekMaxImagesPerRequest,
			snapshot.deepSeekImageOffloadByteQuantum, 0, snapshot.deepSeekImageOffloadCountQuantum)
		retainedIDs := deepSeekRetainedImageIDs(req.Messages)
		versions := map[string]RequestImageAttachment{}
		for attachmentID, ref := range refByID {
			if !retainedIDs[attachmentID] {
				continue
			}
			if _, exists := versions[ref.AttachmentID]; exists {
				continue
			}
			version, requestErr := p.engine.ReadImageRequest(ref, requestImagePolicy)
			if requestErr != nil {
				return Completion{}, requestErr
			}
			versions[ref.AttachmentID] = version
		}
		if len(versions) > 0 {
			fileMessages := cloneChatMessages(req.Messages)
			resolvedFiles := make(map[string]string, len(versions))
			fileResolutionFailed := false
		resolveFiles:
			for index := range fileMessages {
				for partIndex := range fileMessages[index].Parts {
					part := &fileMessages[index].Parts[partIndex]
					version, exists := versions[part.AttachmentID]
					if !exists || part.Type != "image" {
						continue
					}
					fileID, alreadyResolved := resolvedFiles[part.AttachmentID]
					if alreadyResolved {
						part.FileID, part.Data = fileID, ""
						continue
					}
					file, fileErr := snapshot.deepSeekFiles.EnsureUploaded(ctx, version, snapshot.deepSeekFileConnection, snapshot.deepSeekFilePolicy)
					if fileErr != nil {
						if ctx.Err() != nil {
							return Completion{}, ctx.Err()
						}
						fileResolutionFailed = true
						break resolveFiles
					}
					resolvedFiles[part.AttachmentID] = file.Record.FileID
					part.FileID, part.Data = file.Record.FileID, ""
				}
				for imageIndex := range fileMessages[index].Images {
					image := &fileMessages[index].Images[imageIndex]
					if image.AttachmentID == "" {
						continue
					}
					if _, exists := versions[image.AttachmentID]; !exists {
						continue
					}
					if fileID, exists := resolvedFiles[image.AttachmentID]; exists {
						image.FileID = fileID
						image.Data = ""
						continue
					}
					version := versions[image.AttachmentID]
					file, fileErr := snapshot.deepSeekFiles.EnsureUploaded(ctx, version, snapshot.deepSeekFileConnection, snapshot.deepSeekFilePolicy)
					if fileErr == nil {
						resolvedFiles[image.AttachmentID] = file.Record.FileID
						image.FileID = file.Record.FileID
						image.Data = ""
					} else {
						if ctx.Err() != nil {
							return Completion{}, ctx.Err()
						}
						fileResolutionFailed = true
						break resolveFiles
					}
				}
			}
			if !fileResolutionFailed {
				req.Messages = fileMessages
			} else {
				// A failed Files API resolution switches the entire request to
				// the base64 representation. Restore every retained image from
				// its verified request version so the fallback cannot send the
				// original hydrated bytes or a partially resolved file id. Do not
				// retain a Files-byte budget here: rc.2 applies only the inline
				// budget after this fallback.
				req.Messages = deepSeekRestoreInlineMessages(req.Messages, versions)
				req.deepSeekForceInline = true
			}
			req.deepSeekFileVersions = versions
		}
	}
	// Apply count and raw/inline byte budgets after file resolution. File IDs
	// remain represented in the request but their bytes are counted against the
	// Files budget; only inline parts count against the fallback base64 budget.
	maxRequestFilesBytes := snapshot.deepSeekMaxRequestFilesBytes
	if req.deepSeekForceInline {
		maxRequestFilesBytes = 0
	}
	req.Messages = offloadDeepSeekImages(req.Messages, req.deepSeekFileVersions,
		maxRequestFilesBytes, snapshot.deepSeekMaxInlineRequestImageBytes,
		snapshot.deepSeekMaxImagesPerRequest, snapshot.deepSeekImageOffloadByteQuantum,
		snapshot.deepSeekInlineImageOffloadByteQuantum, snapshot.deepSeekImageOffloadCountQuantum)
	req.Messages = deepSeekAddImageHandles(req.Messages, req.deepSeekFileVersions)
	completion, completeErr := snapshot.Complete(ctx, req, onDelta)
	if completeErr == nil || len(req.deepSeekFileVersions) == 0 || len(deepSeekMessageFileIDs(req.Messages)) == 0 || !deepSeekFileReferenceFailure(completeErr) {
		return completion, completeErr
	}
	// One precise stale-file recovery: invalidate only named mappings, then
	// resolve those variants once more. If any replacement fails, retry the
	// whole request inline so one request never mixes representations.
	originalFileIDs := deepSeekMessageFileIDs(req.Messages)
	staleIDs := deepSeekStaleFileIDs(completeErr, req.Messages)
	for _, message := range req.Messages {
		for _, part := range chatContentParts(message) {
			if part.FileID == "" || !staleIDs[part.FileID] {
				continue
			}
			if version, ok := req.deepSeekFileVersions[part.AttachmentID]; ok {
				_ = snapshot.deepSeekFiles.Invalidate(version, part.FileID, snapshot.deepSeekFileConnection)
			}
		}
	}
	retryMessages := deepSeekRestoreInlineMessages(req.Messages, req.deepSeekFileVersions)
	deepSeekApplyFileIDs(retryMessages, originalFileIDs, staleIDs)
	needsInlineFallback := false
	resolvedRetry := make(map[string]string)
	resolveRetryFile := func(attachmentID string) (string, bool, error) {
		if attachmentID == "" || !staleIDs[originalFileIDs[attachmentID]] || originalFileIDs[attachmentID] == "" {
			return "", false, nil
		}
		version, ok := req.deepSeekFileVersions[attachmentID]
		if !ok {
			return "", false, nil
		}
		if fileID, exists := resolvedRetry[attachmentID]; exists {
			return fileID, true, nil
		}
		file, fileErr := snapshot.deepSeekFiles.EnsureUploaded(ctx, version, snapshot.deepSeekFileConnection, snapshot.deepSeekFilePolicy)
		if fileErr != nil {
			if ctx.Err() != nil {
				return "", false, ctx.Err()
			}
			needsInlineFallback = true
			return "", false, nil
		}
		resolvedRetry[attachmentID] = file.Record.FileID
		return file.Record.FileID, true, nil
	}
	for index := range retryMessages {
		for partIndex := range retryMessages[index].Parts {
			part := &retryMessages[index].Parts[partIndex]
			if part.Type != "image" {
				continue
			}
			fileID, replace, resolveErr := resolveRetryFile(part.AttachmentID)
			if resolveErr != nil {
				return Completion{}, resolveErr
			}
			if replace {
				part.FileID, part.Data = fileID, ""
			}
		}
		if needsInlineFallback {
			break
		}
		for imageIndex := range retryMessages[index].Images {
			image := &retryMessages[index].Images[imageIndex]
			fileID, replace, resolveErr := resolveRetryFile(image.AttachmentID)
			if resolveErr != nil {
				return Completion{}, resolveErr
			}
			if replace {
				image.FileID, image.Data = fileID, ""
			}
		}
		if needsInlineFallback {
			break
		}
	}
	if needsInlineFallback {
		retryMessages = deepSeekRestoreInlineMessages(req.Messages, req.deepSeekFileVersions)
		req.deepSeekForceInline = true
	} else {
		deepSeekApplyFileIDs(retryMessages, resolvedRetry, nil)
	}
	retryFilesBytes := snapshot.deepSeekMaxRequestFilesBytes
	if req.deepSeekForceInline {
		retryFilesBytes = 0
	}
	retryMessages = offloadDeepSeekImages(retryMessages, req.deepSeekFileVersions,
		retryFilesBytes, snapshot.deepSeekMaxInlineRequestImageBytes,
		snapshot.deepSeekMaxImagesPerRequest, snapshot.deepSeekImageOffloadByteQuantum,
		snapshot.deepSeekInlineImageOffloadByteQuantum, snapshot.deepSeekImageOffloadCountQuantum)
	req.Messages = deepSeekAddImageHandles(retryMessages, req.deepSeekFileVersions)
	return snapshot.Complete(ctx, req, onDelta)
}

func collectDeepSeekImageRefs(messages []ChatMessage) []ImageAttachmentRef {
	refs := make([]ImageAttachmentRef, 0)
	seen := map[string]bool{}
	var walk func([]ContentBlock)
	walk = func(blocks []ContentBlock) {
		for _, block := range blocks {
			if block.Type == "image" && block.Attachment != nil && !seen[block.Attachment.AttachmentID] {
				seen[block.Attachment.AttachmentID] = true
				refs = append(refs, *block.Attachment)
			}
			walk(block.Content)
		}
	}
	for _, message := range messages {
		walk(message.Blocks)
	}
	return refs
}

func findDeepSeekImagePart(parts []ChatContentPart, attachmentID string) int {
	for index, part := range parts {
		if part.Type == "image" && part.AttachmentID == attachmentID {
			return index
		}
	}
	return -1
}

func deepSeekRetainedImageIDs(messages []ChatMessage) map[string]bool {
	retained := make(map[string]bool)
	for _, message := range messages {
		for _, part := range chatContentParts(message) {
			if part.Type == "image" && part.AttachmentID != "" {
				retained[part.AttachmentID] = true
			}
		}
	}
	return retained
}

func deepSeekAddImageHandles(messages []ChatMessage, versions map[string]RequestImageAttachment) []ChatMessage {
	if len(versions) == 0 {
		return messages
	}
	out := cloneChatMessages(messages)
	for index := range out {
		parts := chatContentParts(out[index])
		changed := false
		for partIndex := 0; partIndex < len(parts); partIndex++ {
			part := parts[partIndex]
			if part.Type != "image" {
				continue
			}
			version, ok := versions[part.AttachmentID]
			if !ok {
				continue
			}
			handle := fmt.Sprintf("Image %s; request image %dx%dpx.", version.Attachment.AttachmentID, version.Width, version.Height)
			if partIndex > 0 {
				handle = "\n" + handle
			}
			if partIndex > 0 && parts[partIndex-1].Type == "text" && parts[partIndex-1].Text == handle {
				continue
			}
			parts = append(parts[:partIndex], append([]ChatContentPart{{Type: "text", Text: handle}}, parts[partIndex:]...)...)
			partIndex++
			changed = true
		}
		if changed {
			out[index].Parts = parts
			out[index].Images = nil
		}
	}
	return out
}

func deepSeekMessageFileIDs(messages []ChatMessage) map[string]string {
	ids := make(map[string]string)
	for _, message := range messages {
		for _, part := range chatContentParts(message) {
			if part.Type == "image" && part.AttachmentID != "" && part.FileID != "" {
				ids[part.AttachmentID] = part.FileID
			}
		}
	}
	return ids
}

// deepSeekApplyFileIDs restores the one-representation invariant after a
// stale-file retry. staleIDs nil means every supplied mapping is reusable.
func deepSeekApplyFileIDs(messages []ChatMessage, ids map[string]string, staleIDs map[string]bool) {
	for index := range messages {
		for partIndex := range messages[index].Parts {
			part := &messages[index].Parts[partIndex]
			if part.Type != "image" || part.AttachmentID == "" {
				continue
			}
			fileID := ids[part.AttachmentID]
			if fileID == "" || staleIDs != nil && staleIDs[fileID] {
				continue
			}
			part.FileID, part.Data = fileID, ""
		}
		for imageIndex := range messages[index].Images {
			image := &messages[index].Images[imageIndex]
			fileID := ids[image.AttachmentID]
			if fileID == "" || staleIDs != nil && staleIDs[fileID] {
				continue
			}
			image.FileID, image.Data = fileID, ""
		}
	}
}

func deepSeekRestoreInlineMessages(messages []ChatMessage, versions map[string]RequestImageAttachment) []ChatMessage {
	out := cloneChatMessages(messages)
	for index := range out {
		for partIndex := range out[index].Parts {
			part := &out[index].Parts[partIndex]
			if version, ok := versions[part.AttachmentID]; ok && part.Type == "image" {
				part.FileID = ""
				part.Data = base64.StdEncoding.EncodeToString(version.Data)
				part.MediaType = version.MediaType
			}
		}
		for imageIndex := range out[index].Images {
			image := &out[index].Images[imageIndex]
			if version, ok := versions[image.AttachmentID]; ok {
				image.FileID = ""
				image.Data = base64.StdEncoding.EncodeToString(version.Data)
				image.MediaType = version.MediaType
			}
		}
	}
	return out
}

func deepSeekFileReferenceFailure(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	hasFile := strings.Contains(message, "file")
	missing := strings.Contains(message, "expired") ||
		strings.Contains(message, "not found") ||
		strings.Contains(message, "file_not_found") ||
		strings.Contains(message, "deleted") ||
		strings.Contains(message, "does not exist") ||
		strings.Contains(message, "does_not_exist") ||
		strings.Contains(message, "do not exist") ||
		strings.Contains(message, "not created under this account") ||
		strings.Contains(message, "not created under your account")
	invalidID := strings.Contains(message, "invalid") &&
		(strings.Contains(message, "file id") || strings.Contains(message, "file_id") || strings.Contains(message, "file-api"))
	return hasFile && (missing || invalidID)
}

func deepSeekStaleFileIDs(err error, messages []ChatMessage) map[string]bool {
	if err == nil {
		return nil
	}
	message := err.Error()
	used := make(map[string]bool)
	for _, chatMessage := range messages {
		for _, part := range chatContentParts(chatMessage) {
			if part.Type == "image" && part.FileID != "" {
				used[part.FileID] = true
			}
		}
	}
	named := make(map[string]bool)
	for fileID := range used {
		if deepSeekDetailNamesFileID(message, fileID) {
			named[fileID] = true
		}
	}
	if len(named) > 0 {
		return named
	}
	return used
}

func deepSeekDetailNamesFileID(detail, fileID string) bool {
	if fileID == "" {
		return false
	}
	for start := 0; ; {
		index := strings.Index(detail[start:], fileID)
		if index < 0 {
			return false
		}
		index += start
		if deepSeekFileIDBoundaryBefore(detail, index) && deepSeekFileIDBoundaryAfter(detail, index+len(fileID)) {
			return true
		}
		start = index + 1
		if start > len(detail)-len(fileID) {
			return false
		}
	}
}

func deepSeekFileIDTokenRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-'
}

func deepSeekFileIDBoundaryBefore(detail string, index int) bool {
	if index <= 0 {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(detail[:index])
	return !deepSeekFileIDTokenRune(r)
}

func deepSeekFileIDBoundaryAfter(detail string, index int) bool {
	if index >= len(detail) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(detail[index:])
	return !deepSeekFileIDTokenRune(r)
}
