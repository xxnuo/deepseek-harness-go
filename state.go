package harness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

type persistedSession struct {
	Model           ModelSelection            `json:"model"`
	Mode            string                    `json:"mode,omitempty"`
	Persona         string                    `json:"persona,omitempty"`
	ToolRestriction *persistedToolRestriction `json:"toolRestriction,omitempty"`
}

type persistedToolRestriction struct {
	Allow *[]string `json:"allow,omitempty"`
	Deny  *[]string `json:"deny,omitempty"`
}

type persistedState struct {
	Version        int                         `json:"version"`
	WorkspaceOrder []string                    `json:"workspaceOrder"`
	Workspaces     map[string]Workspace        `json:"workspaces"`
	Archived       []string                    `json:"archived"`
	Sessions       map[string]persistedSession `json:"sessions"`
}

func (e *Engine) statePath() string { return filepath.Join(e.cfg.DataDir, "state.json") }

// saveStateLocked snapshots host registries while e.mu is held.
func (e *Engine) saveStateLocked() error {
	if !e.cfg.Persist {
		return nil
	}
	state := persistedState{
		Version: 1, WorkspaceOrder: append([]string(nil), e.workspaceOrder...),
		Workspaces: map[string]Workspace{}, Sessions: map[string]persistedSession{},
	}
	for id, workspace := range e.workspaces {
		copy := *workspace
		copy.SessionIDs = append([]string{}, workspace.SessionIDs...)
		state.Workspaces[id] = copy
	}
	for id := range e.archived {
		state.Archived = append(state.Archived, id)
	}
	sort.Strings(state.Archived)
	for id, session := range e.sessions {
		session.mu.Lock()
		state.Sessions[id] = persistedSession{
			Model: session.Model, Mode: session.Header.Mode, Persona: session.personaOverride,
			ToolRestriction: persistToolRestriction(session.toolRestriction),
		}
		session.mu.Unlock()
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(e.cfg.DataDir, 0o700); err != nil {
		return err
	}
	tmp := e.statePath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, e.statePath())
}

func (e *Engine) saveState() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.saveStateLocked()
}

func (e *Engine) loadState() error {
	data, err := os.ReadFile(e.statePath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Version != 1 {
		return nil
	}
	for id, workspace := range state.Workspaces {
		copy := workspace
		copy.SessionIDs = append([]string{}, workspace.SessionIDs...)
		e.workspaces[id] = &copy
	}
	e.workspaceOrder = append([]string(nil), state.WorkspaceOrder...)
	seen := map[string]bool{}
	for _, id := range e.workspaceOrder {
		seen[id] = e.workspaces[id] != nil
	}
	missing := make([]string, 0)
	for id := range e.workspaces {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	e.workspaceOrder = append(e.workspaceOrder, missing...)
	for _, id := range state.Archived {
		e.archived[id] = true
	}
	for id, metadata := range state.Sessions {
		if session := e.sessions[id]; session != nil {
			session.Model = metadata.Model
			session.Header.Mode = metadata.Mode
			session.personaOverride = metadata.Persona
			session.toolRestriction = restoreToolRestriction(metadata.ToolRestriction)
		}
	}
	return nil
}

func persistToolRestriction(restriction *sessionToolRestriction) *persistedToolRestriction {
	if restriction == nil {
		return nil
	}
	result := &persistedToolRestriction{}
	if restriction.allowSet {
		values := sortedRestrictionNames(restriction.allow)
		result.Allow = &values
	}
	if restriction.denySet {
		values := sortedRestrictionNames(restriction.deny)
		result.Deny = &values
	}
	return result
}

func sortedRestrictionNames(values map[string]bool) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func restoreToolRestriction(value *persistedToolRestriction) *sessionToolRestriction {
	if value == nil {
		return nil
	}
	result := &sessionToolRestriction{
		allow: map[string]bool{}, deny: map[string]bool{},
		allowSet: value.Allow != nil, denySet: value.Deny != nil,
	}
	if value.Allow != nil {
		for _, name := range *value.Allow {
			result.allow[name] = true
		}
	}
	if value.Deny != nil {
		for _, name := range *value.Deny {
			result.deny[name] = true
		}
	}
	return result
}
