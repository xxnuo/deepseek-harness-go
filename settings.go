package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var builtinSettingsNamespaces = []string{
	"agent-default-model", "agent-loop", "agent-presets", "llm-deepseek", "llm-pi-ai",
	"locale", "permission", "shell", "ui-conversation", "ui-onboarding", "ui-theme",
	"web-search-deepseek",
}

var settingsNamespacePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

var builtinSettingsNamespaceSet = func() map[string]struct{} {
	set := make(map[string]struct{}, len(builtinSettingsNamespaces))
	for _, ns := range builtinSettingsNamespaces {
		set[ns] = struct{}{}
	}
	return set
}()

func cloneSettingsValue(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	b, _ := json.Marshal(value)
	var copy map[string]any
	_ = json.Unmarshal(b, &copy)
	if copy == nil {
		return map[string]any{}
	}
	return copy
}

func (e *Engine) resolvedSettingsValueLocked(ns string) (map[string]any, map[string]any) {
	user := cloneSettingsValue(e.settings[ns])
	value := user
	base := map[string]any{}
	switch ns {
	case "permission":
		base = permissionBaseSettings()
		value = mergeSettings(base, user)
	case "ui-conversation":
		value = mergeSettings(map[string]any{"busyEnter": "queue"}, user)
	case "ui-theme":
		value = mergeSettings(map[string]any{"preference": "system"}, user)
	case "llm-deepseek":
		base = deepSeekBaseSettings(e)
		value = mergeSettings(base, user)
	case piAISettingsNamespace:
		base = piAISettingsBase()
		value = mergeSettings(base, user)
	case "web-search-deepseek":
		base = deepSeekWebSearchBaseSettings()
		value = mergeSettings(base, user)
	}
	return value, base
}

func (e *Engine) settingsViewLocked(ns string) map[string]any {
	user := cloneSettingsValue(e.settings[ns])
	value, base := e.resolvedSettingsValueLocked(ns)
	view := map[string]any{
		"ns":       ns,
		"schema":   map[string]any{"type": "object"},
		"value":    value,
		"applies":  "live",
		"secrets":  []any{},
		"revision": e.settingsRev[ns],
	}
	switch ns {
	case "locale":
		view["schema"] = stringChoiceSettingsSchema("preference", []string{"zh", "en"}, "", true)
	case "ui-conversation":
		view["schema"] = stringChoiceSettingsSchema("busyEnter", []string{"queue", "steer"}, "queue", false)
	case "ui-onboarding":
		view["schema"] = onboardingSettingsSchema()
	case "ui-theme":
		view["schema"] = stringChoiceSettingsSchema("preference", []string{"light", "dark", "system"}, "system", false)
	case "permission":
		view["schema"] = permissionSettingsSchema()
		view["base"] = base
	case "llm-deepseek":
		view["schema"] = deepSeekSettingsSchema()
		view["base"] = base
	case piAISettingsNamespace:
		view["schema"] = piAISettingsSchema()
		view["base"] = base
	case "web-search-deepseek":
		view["schema"] = deepSeekWebSearchSettingsSchema()
		view["base"] = base
		_, secretSet := value["apiKey"]
		delete(value, "apiKey")
		delete(user, "apiKey")
		view["secrets"] = []any{map[string]any{"path": []any{"apiKey"}, "set": secretSet}}
	}
	if _, ok := e.settings[ns]; ok {
		view["user"] = user
	}
	return view
}

func permissionBaseSettings() map[string]any {
	preset := strings.TrimSpace(os.Getenv("DSH_PERMISSION_MODE"))
	if _, ok := commandPermissionPresets[preset]; !ok {
		preset = "workspace-write"
	}
	return map[string]any{"defaultPreset": preset}
}

func permissionSettingsSchema() map[string]any {
	refs := map[string]any{}
	choices := make([]any, 0, len(permissionPresetNames()))
	for index, preset := range permissionPresetNames() {
		uid := index + 1
		choices = append(choices, uid)
		refs[strconv.Itoa(uid)] = map[string]any{"type": "const", "value": preset}
	}
	unionUID := len(choices) + 1
	objectUID := unionUID + 1
	refs[strconv.Itoa(unionUID)] = map[string]any{"type": "union", "list": choices}
	refs[strconv.Itoa(objectUID)] = map[string]any{
		"type": "object",
		"dict": map[string]any{"defaultPreset": unionUID},
	}
	return map[string]any{"uid": objectUID, "refs": refs}
}

func stringChoiceSettingsSchema(field string, choices []string, defaultValue string, optional bool) map[string]any {
	refs := make(map[string]any, len(choices)+2)
	list := make([]any, len(choices))
	for index, choice := range choices {
		uid := index + 1
		list[index] = uid
		refs[strconv.Itoa(uid)] = map[string]any{
			"type": "const", "meta": map[string]any{"required": true}, "value": choice,
		}
	}
	unionUID := len(choices) + 1
	meta := map[string]any{}
	if defaultValue != "" {
		meta["default"] = defaultValue
	} else if optional {
		meta["required"] = false
	}
	refs[strconv.Itoa(unionUID)] = map[string]any{"type": "union", "meta": meta, "list": list}
	objectUID := unionUID + 1
	refs[strconv.Itoa(objectUID)] = map[string]any{
		"type": "object", "meta": map[string]any{"default": map[string]any{}}, "dict": map[string]any{field: unionUID},
	}
	return map[string]any{"uid": objectUID, "refs": refs}
}

func onboardingSettingsSchema() map[string]any {
	return map[string]any{
		"uid": 1,
		"refs": map[string]any{
			"0": map[string]any{"type": "string", "meta": map[string]any{}},
			"1": map[string]any{
				"type": "object", "meta": map[string]any{"default": map[string]any{}},
				"dict": map[string]any{"welcomeNoticeVersion": 0},
			},
		},
	}
}

func validateSettingsValue(ns string, value map[string]any) error {
	switch ns {
	case "locale":
		return validateStringChoiceSetting(ns, value, "preference", []string{"zh", "en"})
	case "ui-conversation":
		return validateStringChoiceSetting(ns, value, "busyEnter", []string{"queue", "steer"})
	case "ui-onboarding":
		if raw, exists := value["welcomeNoticeVersion"]; exists {
			if _, ok := raw.(string); !ok {
				return errors.New("ui-onboarding.welcomeNoticeVersion must be a string")
			}
		}
	case "ui-theme":
		return validateStringChoiceSetting(ns, value, "preference", []string{"light", "dark", "system"})
	case "permission":
		raw, exists := value["defaultPreset"]
		if !exists {
			return nil
		}
		preset, ok := raw.(string)
		if _, known := commandPermissionPresets[preset]; !ok || !known {
			return fmt.Errorf("permission.defaultPreset must be one of %s", strings.Join(permissionPresetNames(), ", "))
		}
	}
	return nil
}

func validateStringChoiceSetting(ns string, value map[string]any, field string, choices []string) error {
	raw, exists := value[field]
	if !exists {
		return nil
	}
	selected, ok := raw.(string)
	if ok {
		for _, choice := range choices {
			if selected == choice {
				return nil
			}
		}
	}
	return fmt.Errorf("%s.%s must be one of %s", ns, field, strings.Join(choices, ", "))
}

func (e *Engine) validateSettingsValueLocked(ns string, value map[string]any) (map[string]*managedPiAIProvider, error) {
	if err := validateSettingsValue(ns, value); err != nil {
		return nil, err
	}
	if ns != piAISettingsNamespace {
		return nil, nil
	}
	return e.resolvePiAIProvidersLocked(value)
}

// permissionDefaultPresetLocked resolves the setting used to pin a new
// session while the caller owns e.mu.
func (e *Engine) permissionDefaultPresetLocked() string {
	base := permissionBaseSettings()["defaultPreset"].(string)
	if preset, ok := e.settings["permission"]["defaultPreset"].(string); ok {
		if _, known := commandPermissionPresets[preset]; known {
			return preset
		}
	}
	return base
}

func (e *Engine) settingsDescribe() map[string]any {
	e.mu.RLock()
	keys := make([]string, 0, len(builtinSettingsNamespaces))
	seen := make(map[string]bool, len(builtinSettingsNamespaces))
	for _, ns := range builtinSettingsNamespaces {
		keys = append(keys, ns)
		seen[ns] = true
	}
	sort.Strings(keys)
	namespaces := make([]any, 0, len(keys))
	for _, ns := range keys {
		namespaces = append(namespaces, e.settingsViewLocked(ns))
	}
	e.mu.RUnlock()
	return map[string]any{"writable": true, "hasDocument": e.cfg.Persist, "namespaces": namespaces}
}

func validSettingsNS(ns string) bool {
	if ns != strings.TrimSpace(ns) || !settingsNamespacePattern.MatchString(ns) {
		return false
	}
	_, ok := builtinSettingsNamespaceSet[ns]
	return ok
}

func (e *Engine) checkSettingsRevisionLocked(ns string, expected *int) *RPCError {
	if expected != nil && *expected != e.settingsRev[ns] {
		return rpcError("settings-conflict", "settings revision does not match", map[string]any{"ns": ns, "expected": *expected, "actual": e.settingsRev[ns]})
	}
	return nil
}

func (e *Engine) settingsUpdate(ns string, patch map[string]any, expected *int, replace bool) (any, *RPCError) {
	return e.settingsUpdateFrom(nil, ns, patch, expected, replace)
}

func (e *Engine) settingsUpdateFrom(origin *dynamicCordisRun, ns string, patch map[string]any, expected *int, replace bool) (any, *RPCError) {
	if !validSettingsNS(ns) {
		return nil, rpcError("settings-rejected", fmt.Sprintf("settings namespace %q is not registered", ns), map[string]any{"ns": ns})
	}
	if patch == nil {
		patch = map[string]any{}
	}
	e.mu.Lock()
	if err := e.checkSettingsRevisionLocked(ns, expected); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	previousResolved, _ := e.resolvedSettingsValueLocked(ns)
	previous, existed := e.settings[ns]
	previousRevision := e.settingsRev[ns]
	var next map[string]any
	if replace {
		next = cloneSettingsValue(patch)
	} else {
		next = mergeSettings(e.settings[ns], patch)
	}
	piAIProviders, validationErr := e.validateSettingsValueLocked(ns, next)
	if validationErr != nil {
		e.mu.Unlock()
		return nil, rpcError("settings-rejected", validationErr.Error(), map[string]any{"ns": ns})
	}
	if reflect.DeepEqual(next, e.settings[ns]) {
		view := e.settingsViewLocked(ns)
		e.mu.Unlock()
		return view, nil
	}
	e.settings[ns] = next
	e.settingsRev[ns]++
	if err := e.saveSettingsLocked(); err != nil {
		if existed {
			e.settings[ns] = previous
		} else {
			delete(e.settings, ns)
		}
		e.settingsRev[ns] = previousRevision
		e.mu.Unlock()
		return nil, rpcError("settings-rejected", err.Error(), map[string]any{"ns": ns})
	}
	if ns == piAISettingsNamespace {
		e.replacePiAIProvidersLocked(piAIProviders)
	}
	revision := e.settingsRev[ns]
	nextResolved, _ := e.resolvedSettingsValueLocked(ns)
	view := e.settingsViewLocked(ns)
	e.mu.Unlock()
	e.emitRemoteEventFrom(origin, "settings/document-updated", ns, revision)
	if !reflect.DeepEqual(nextResolved, previousResolved) {
		_ = e.dispatchDynamicCordisEvent(origin, "", true, "settings/updated", ns, nextResolved, previousResolved, "update")
	}
	if ns == piAISettingsNamespace {
		e.emitRemoteEventFrom(origin, "llm/adapters-updated")
	}
	return view, nil
}

func (e *Engine) settingsMutate(ns string, rawOps []any, expected *int) (any, *RPCError) {
	if !validSettingsNS(ns) {
		return nil, rpcError("settings-rejected", fmt.Sprintf("settings namespace %q is not registered", ns), map[string]any{"ns": ns})
	}
	e.mu.Lock()
	if err := e.checkSettingsRevisionLocked(ns, expected); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	previousResolved, _ := e.resolvedSettingsValueLocked(ns)
	value := cloneSettingsValue(e.settings[ns])
	for _, raw := range rawOps {
		op, ok := raw.(map[string]any)
		if !ok {
			e.mu.Unlock()
			return nil, rpcError("bad-request", "settings operation must be an object", nil)
		}
		name, _ := op["op"].(string)
		path, ok := stringPath(op["path"])
		if !ok {
			e.mu.Unlock()
			return nil, rpcError("bad-request", "settings operation path must be string segments", nil)
		}
		switch name {
		case "set":
			if err := settingsSet(value, path, op["value"]); err != nil {
				e.mu.Unlock()
				return nil, rpcError("bad-request", err.Error(), nil)
			}
		case "unset":
			settingsUnset(value, path)
		default:
			e.mu.Unlock()
			return nil, rpcError("bad-request", "settings operation must be set or unset", nil)
		}
	}
	piAIProviders, validationErr := e.validateSettingsValueLocked(ns, value)
	if validationErr != nil {
		e.mu.Unlock()
		return nil, rpcError("settings-rejected", validationErr.Error(), map[string]any{"ns": ns})
	}
	previous, existed := e.settings[ns]
	previousRevision := e.settingsRev[ns]
	if reflect.DeepEqual(value, e.settings[ns]) {
		view := e.settingsViewLocked(ns)
		e.mu.Unlock()
		return view, nil
	}
	e.settings[ns] = value
	e.settingsRev[ns]++
	if err := e.saveSettingsLocked(); err != nil {
		if existed {
			e.settings[ns] = previous
		} else {
			delete(e.settings, ns)
		}
		e.settingsRev[ns] = previousRevision
		e.mu.Unlock()
		return nil, rpcError("settings-rejected", err.Error(), map[string]any{"ns": ns})
	}
	if ns == piAISettingsNamespace {
		e.replacePiAIProvidersLocked(piAIProviders)
	}
	revision := e.settingsRev[ns]
	nextResolved, _ := e.resolvedSettingsValueLocked(ns)
	view := e.settingsViewLocked(ns)
	e.mu.Unlock()
	e.emitRemoteEvent("settings/document-updated", ns, revision)
	if !reflect.DeepEqual(nextResolved, previousResolved) {
		e.emitDynamicCordisScopedContained("", "settings/updated", ns, nextResolved, previousResolved, "update")
	}
	if ns == piAISettingsNamespace {
		e.emitRemoteEvent("llm/adapters-updated")
	}
	return view, nil
}

func mergeSettings(base, patch map[string]any) map[string]any {
	merged := cloneSettingsValue(base)
	for key, value := range cloneSettingsValue(patch) {
		if patchObject, ok := value.(map[string]any); ok {
			if baseObject, ok := merged[key].(map[string]any); ok {
				merged[key] = mergeSettings(baseObject, patchObject)
			} else {
				merged[key] = cloneSettingsValue(patchObject)
			}
			continue
		}
		merged[key] = value
	}
	return merged
}

func stringPath(value any) ([]string, bool) {
	items, ok := value.([]any)
	if !ok {
		return nil, false
	}
	path := make([]string, len(items))
	for i, item := range items {
		path[i], ok = item.(string)
		if !ok || path[i] == "" {
			return nil, false
		}
	}
	return path, true
}

func settingsSet(root map[string]any, path []string, value any) error {
	if len(path) == 0 {
		obj, ok := value.(map[string]any)
		if !ok {
			return errors.New("root settings value must be an object")
		}
		for key := range root {
			delete(root, key)
		}
		for key, item := range obj {
			root[key] = item
		}
		return nil
	}
	current := root
	for _, key := range path[:len(path)-1] {
		next, ok := current[key].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[key] = next
		}
		current = next
	}
	current[path[len(path)-1]] = value
	return nil
}

func settingsUnset(root map[string]any, path []string) {
	if len(path) == 0 {
		for key := range root {
			delete(root, key)
		}
		return
	}
	current := root
	for _, key := range path[:len(path)-1] {
		next, ok := current[key].(map[string]any)
		if !ok {
			return
		}
		current = next
	}
	delete(current, path[len(path)-1])
}
