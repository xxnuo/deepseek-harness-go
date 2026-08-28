package harness

import (
	"errors"
	"fmt"
)

// SubagentDescriptorData is the durable versioned identity and resumable
// composition recorded by a subagent/descriptor event.
type SubagentDescriptorData struct {
	Version       int                 `json:"version"`
	Mode          string              `json:"mode"`
	Provider      string              `json:"provider"`
	Label         *string             `json:"label,omitempty"`
	AgentProvider *string             `json:"agentProvider,omitempty"`
	AgentModel    *string             `json:"agentModel,omitempty"`
	Persona       *string             `json:"persona,omitempty"`
	ToolFilter    *SubagentToolFilter `json:"toolFilter,omitempty"`
}

// SnapshotSubagentDescriptor validates and detaches one descriptor before it
// crosses the session-log boundary.
func SnapshotSubagentDescriptor(input SubagentDescriptorData) (SubagentDescriptorData, error) {
	input.Version = SubagentDescriptorVersion
	if input.Mode != "one-shot" && input.Mode != "continuable" {
		return SubagentDescriptorData{}, errors.New(`subagent descriptor mode must be "one-shot" or "continuable"`)
	}
	if input.Mode == "one-shot" {
		if input.AgentProvider != nil || input.AgentModel != nil || input.Persona != nil || input.ToolFilter != nil {
			return SubagentDescriptorData{}, errors.New("one-shot subagent descriptor contains continuable-only fields")
		}
	} else if input.Label == nil {
		return SubagentDescriptorData{}, errors.New("continuable subagent descriptor label is required")
	}
	if input.ToolFilter != nil {
		if input.ToolFilter.Allow == nil && input.ToolFilter.Deny == nil {
			return SubagentDescriptorData{}, errors.New("subagent descriptor toolFilter must declare allow and/or deny")
		}
		filter := &SubagentToolFilter{}
		if input.ToolFilter.Allow != nil {
			filter.Allow = cloneDescriptorStrings(input.ToolFilter.Allow)
		}
		if input.ToolFilter.Deny != nil {
			filter.Deny = cloneDescriptorStrings(input.ToolFilter.Deny)
		}
		input.ToolFilter = filter
	}
	input.Label = cloneDescriptorString(input.Label)
	input.AgentProvider = cloneDescriptorString(input.AgentProvider)
	input.AgentModel = cloneDescriptorString(input.AgentModel)
	input.Persona = cloneDescriptorString(input.Persona)
	return input, nil
}

func cloneDescriptorString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func descriptorString(value string) *string { return &value }

func cloneDescriptorStrings(values []string) []string {
	if values == nil {
		return nil
	}
	result := make([]string, len(values))
	copy(result, values)
	return result
}

func (descriptor SubagentDescriptorData) eventData() map[string]any {
	data := map[string]any{"version": descriptor.Version, "mode": descriptor.Mode, "provider": descriptor.Provider}
	if descriptor.Label != nil {
		data["label"] = *descriptor.Label
	}
	if descriptor.AgentProvider != nil {
		data["agentProvider"] = *descriptor.AgentProvider
	}
	if descriptor.AgentModel != nil {
		data["agentModel"] = *descriptor.AgentModel
	}
	if descriptor.Persona != nil {
		data["persona"] = *descriptor.Persona
	}
	if descriptor.ToolFilter != nil {
		filter := map[string]any{}
		if descriptor.ToolFilter.Allow != nil {
			filter["allow"] = cloneDescriptorStrings(descriptor.ToolFilter.Allow)
		}
		if descriptor.ToolFilter.Deny != nil {
			filter["deny"] = cloneDescriptorStrings(descriptor.ToolFilter.Deny)
		}
		data["toolFilter"] = filter
	}
	return data
}

// FoldSubagentDescriptor returns the first descriptor event. Unsupported
// versions classify as absent; malformed current-version payloads fail loud.
func FoldSubagentDescriptor(events []Event) (*SubagentDescriptorData, error) {
	for _, event := range events {
		if event.Type != "subagent/descriptor" {
			continue
		}
		descriptor, supported, err := parseSubagentDescriptor(event.Data)
		if err != nil || !supported {
			return nil, err
		}
		return &descriptor, nil
	}
	return nil, nil
}

func parseSubagentDescriptor(value any) (SubagentDescriptorData, bool, error) {
	data, ok := value.(map[string]any)
	if !ok || data == nil {
		return SubagentDescriptorData{}, false, errors.New("persisted subagent descriptor payload must be an object")
	}
	version, ok := subagentJSONNumber(data["version"])
	if !ok {
		return SubagentDescriptorData{}, false, errors.New("persisted subagent descriptor version must be a number")
	}
	if version != SubagentDescriptorVersion {
		return SubagentDescriptorData{}, false, nil
	}
	mode, ok := data["mode"].(string)
	if !ok || mode != "one-shot" && mode != "continuable" {
		return SubagentDescriptorData{}, false, errors.New(`persisted subagent descriptor mode must be "one-shot" or "continuable"`)
	}
	allowed := map[string]bool{"version": true, "mode": true, "provider": true, "label": true}
	if mode == "continuable" {
		allowed["agentProvider"], allowed["agentModel"], allowed["persona"], allowed["toolFilter"] = true, true, true, true
	}
	for key := range data {
		if !allowed[key] {
			return SubagentDescriptorData{}, false, fmt.Errorf("persisted subagent descriptor payload has unknown field %q", key)
		}
	}
	provider, ok := data["provider"].(string)
	if !ok {
		return SubagentDescriptorData{}, false, errors.New("persisted subagent descriptor provider must be a string")
	}
	descriptor := SubagentDescriptorData{Version: SubagentDescriptorVersion, Mode: mode, Provider: provider}
	var err error
	if descriptor.Label, err = optionalDescriptorString(data, "label"); err != nil {
		return SubagentDescriptorData{}, false, err
	}
	if mode == "one-shot" {
		return descriptor, true, nil
	}
	if descriptor.Label == nil {
		return SubagentDescriptorData{}, false, errors.New("persisted subagent descriptor label must be a string")
	}
	if descriptor.AgentProvider, err = optionalDescriptorString(data, "agentProvider"); err != nil {
		return SubagentDescriptorData{}, false, err
	}
	if descriptor.AgentModel, err = optionalDescriptorString(data, "agentModel"); err != nil {
		return SubagentDescriptorData{}, false, err
	}
	if descriptor.Persona, err = optionalDescriptorString(data, "persona"); err != nil {
		return SubagentDescriptorData{}, false, err
	}
	if raw, exists := data["toolFilter"]; exists {
		filter, err := parseDescriptorToolFilter(raw)
		if err != nil {
			return SubagentDescriptorData{}, false, err
		}
		descriptor.ToolFilter = filter
	}
	return descriptor, true, nil
}

func optionalDescriptorString(data map[string]any, key string) (*string, error) {
	raw, exists := data[key]
	if !exists {
		return nil, nil
	}
	value, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("persisted subagent descriptor %s must be a string", key)
	}
	return &value, nil
}

func parseDescriptorToolFilter(value any) (*SubagentToolFilter, error) {
	data, ok := value.(map[string]any)
	if !ok || data == nil {
		return nil, errors.New("persisted subagent descriptor toolFilter must be an object")
	}
	for key := range data {
		if key != "allow" && key != "deny" {
			return nil, fmt.Errorf("persisted subagent descriptor toolFilter has unknown field %q", key)
		}
	}
	filter := &SubagentToolFilter{}
	var err error
	if filter.Allow, err = optionalDescriptorStringSlice(data, "allow"); err != nil {
		return nil, err
	}
	if filter.Deny, err = optionalDescriptorStringSlice(data, "deny"); err != nil {
		return nil, err
	}
	if filter.Allow == nil && filter.Deny == nil {
		return nil, errors.New("persisted subagent descriptor toolFilter must declare allow and/or deny")
	}
	return filter, nil
}

func optionalDescriptorStringSlice(data map[string]any, key string) ([]string, error) {
	raw, exists := data[key]
	if !exists {
		return nil, nil
	}
	values, ok := subagentStringSlice(raw)
	if !ok {
		return nil, fmt.Errorf("persisted subagent descriptor toolFilter.%s must be an array of strings", key)
	}
	return values, nil
}
