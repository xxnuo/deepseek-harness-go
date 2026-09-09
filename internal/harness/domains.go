package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (e *Engine) SearchSessions(query string) ([]map[string]any, bool) {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return nil, false
	}
	rows := e.ListSessions()
	out := make([]map[string]any, 0, min(20, len(rows)))
	matched := 0
	for _, row := range rows {
		if row.Blank || row.Origin == "subagent" {
			continue
		}
		s, _ := e.getSession(row.SessionID)
		if s == nil {
			continue
		}
		s.mu.Lock()
		events := append([]Event(nil), s.Events...)
		s.mu.Unlock()
		snippet := ""
		current, _ := foldSurfaceEvents(events, false)
		for _, ev := range current {
			if ev.Type != "user/message" && ev.Type != "assistant/message" {
				continue
			}
			text := contentValueText(ev.Data)
			if strings.Contains(strings.ToLower(text), query) {
				snippet = text
				break
			}
		}
		if snippet == "" {
			continue
		}
		runes := []rune(snippet)
		if len(runes) > 240 {
			snippet = string(runes[:240])
		}
		matched++
		if len(out) < 20 {
			out = append(out, map[string]any{"sessionId": row.SessionID, "snippet": snippet})
		}
	}
	return out, matched > len(out)
}

func (e *Engine) Models(ctx context.Context) ([]map[string]any, []map[string]any) {
	e.mu.RLock()
	providers := make([]Provider, 0, len(e.providers))
	for _, p := range e.providers {
		providers = append(providers, p)
	}
	e.mu.RUnlock()
	groups := []map[string]any{}
	failures := []map[string]any{}
	for _, p := range providers {
		models, err := p.Models(ctx)
		if err != nil {
			failures = append(failures, map[string]any{"id": p.ID(), "name": p.Name(), "message": err.Error()})
			continue
		}
		rows := []map[string]any{}
		for _, m := range models {
			row := map[string]any{"id": m.ID, "name": m.Name, "inputModalities": m.InputModalities}
			if m.Description != "" {
				row["description"] = m.Description
			}
			if m.Reasoning != nil {
				row["reasoning"] = m.Reasoning
			}
			rows = append(rows, row)
		}
		groups = append(groups, map[string]any{"id": p.ID(), "name": p.Name(), "models": rows})
	}
	return groups, failures
}

// discoverModels probes the provider route described by a settings form. A
// draft endpoint/key is intentionally kept request-local; only the returned
// model metadata is exposed to the caller.
func (e *Engine) discoverModels(ctx context.Context, p map[string]any) (any, *RPCError) {
	ns, _ := p["settingsNs"].(string)
	if ns != "llm-deepseek" && ns != "llm-pi-ai" {
		return nil, rpcError("model-discovery-failed", "no model discovery is registered for settings namespace", map[string]any{"settingsNs": ns})
	}
	providerID, _ := p["provider"].(string)
	baseURL, _ := p["baseURL"].(string)
	apiKey, _ := p["apiKey"].(string)
	api, _ := p["api"].(string)
	if providerID == "" {
		if ns == "llm-deepseek" {
			providerID = "deepseek-official"
		} else {
			providerID = ""
		}
	}
	if ns == piAISettingsNamespace {
		if catalog, ok := piAICatalog[providerID]; ok && len(catalog.Models) > 0 {
			return discoveredModelsValue(piAIModelInfos(catalog.Models)), nil
		}
		if api != "" && api != "openai-completions" && api != "openai-responses" && api != "anthropic-messages" {
			return nil, rpcError("model-discovery-failed", fmt.Sprintf("pi-ai protocol %q has no model listing this Go build can read; enter this provider's models by hand", api), map[string]any{"settingsNs": ns, "baseURL": baseURL})
		}
	}
	if strings.TrimSpace(baseURL) != "" {
		var configured *managedPiAIProvider
		if ns == piAISettingsNamespace && providerID != "" {
			e.mu.RLock()
			configured = e.piAIProviders[providerID]
			e.mu.RUnlock()
			if apiKey == "" && configured != nil && configured.profile.apiKeyEnv != "" {
				var credentialErr error
				apiKey, credentialErr = e.resolvePiAICredential(providerID, configured.profile.apiKeyEnv)
				if credentialErr != nil {
					return nil, rpcError("model-discovery-failed", credentialErr.Error(), map[string]any{"settingsNs": ns, "baseURL": baseURL})
				}
			}
		}
		probe := NewOpenAIProvider(providerID, baseURL, apiKey, "")
		if configured != nil {
			probe.headers = clonePIAIHeaders(configured.profile.headers)
		}
		models, err := probe.discoverModels(ctx, api)
		if err != nil {
			return nil, rpcError("model-discovery-failed", err.Error(), map[string]any{"settingsNs": ns, "baseURL": baseURL})
		}
		return discoveredModelsValue(models), nil
	}
	e.mu.RLock()
	provider := e.providers[providerID]
	e.mu.RUnlock()
	if provider == nil {
		return nil, rpcError("model-discovery-failed", "provider is not configured and no draft baseURL was supplied", map[string]any{"settingsNs": ns})
	}
	models, err := provider.Models(ctx)
	if err != nil {
		return nil, rpcError("model-discovery-failed", err.Error(), map[string]any{"settingsNs": ns})
	}
	return discoveredModelsValue(models), nil
}

func discoveredModelsValue(models []ModelInfo) map[string]any {
	rows := make([]map[string]any, 0, len(models))
	for _, model := range models {
		row := map[string]any{"id": model.ID}
		if model.Name != "" {
			row["name"] = model.Name
		}
		if model.ContextWindow > 0 {
			row["contextWindow"] = model.ContextWindow
		}
		if model.MaxTokens > 0 {
			row["maxTokens"] = model.MaxTokens
		}
		rows = append(rows, row)
	}
	return map[string]any{"models": rows}
}

func (e *Engine) ListWorkspaces() ([]Workspace, []string) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	items := make([]Workspace, 0, len(e.workspaces))
	for _, id := range e.workspaceOrder {
		w := e.workspaces[id]
		if w == nil {
			continue
		}
		copy := *w
		copy.SessionIDs = append([]string{}, w.SessionIDs...)
		items = append(items, copy)
	}
	if len(items) != len(e.workspaces) {
		missing := make([]*Workspace, 0)
		seen := make(map[string]bool, len(items))
		for _, item := range items {
			seen[item.WorkspaceID] = true
		}
		for id, w := range e.workspaces {
			if !seen[id] {
				missing = append(missing, w)
			}
		}
		sort.Slice(missing, func(i, j int) bool { return missing[i].CreatedAt < missing[j].CreatedAt })
		for _, w := range missing {
			copy := *w
			copy.SessionIDs = append([]string{}, w.SessionIDs...)
			items = append(items, copy)
		}
	}
	arch := []string{}
	for id := range e.archived {
		arch = append(arch, id)
	}
	sort.Strings(arch)
	return items, arch
}

func (e *Engine) CreateWorkspace(path string) (Workspace, bool, error) {
	if !filepath.IsAbs(path) {
		return Workspace{}, false, fmt.Errorf("workspace-invalid-path: %s", path)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return Workspace{}, false, fmt.Errorf("workspace-invalid-path: %s", path)
	}
	st, err := os.Stat(canonical)
	if err != nil || !st.IsDir() {
		return Workspace{}, false, fmt.Errorf("workspace-invalid-path: %s", path)
	}
	e.mu.Lock()
	for _, w := range e.workspaces {
		if w.Path == canonical {
			copy := *w
			copy.SessionIDs = append([]string{}, w.SessionIDs...)
			e.mu.Unlock()
			return copy, false, nil
		}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	w := &Workspace{WorkspaceID: newID("ws"), Path: canonical, Title: filepath.Base(canonical), SessionIDs: []string{}, CreatedAt: now, UpdatedAt: now}
	e.workspaces[w.WorkspaceID] = w
	e.workspaceOrder = append(e.workspaceOrder, w.WorkspaceID)
	if err := e.saveStateLocked(); err != nil {
		delete(e.workspaces, w.WorkspaceID)
		e.workspaceOrder = e.workspaceOrder[:len(e.workspaceOrder)-1]
		e.mu.Unlock()
		return Workspace{}, false, fmt.Errorf("workspace-persist-failed: %w", err)
	}
	copy := *w
	copy.SessionIDs = append([]string{}, w.SessionIDs...)
	e.mu.Unlock()
	e.emitHost(map[string]any{"type": "host/workspace-changed", "workspace": copy})
	return copy, true, nil
}

func (e *Engine) RenameWorkspace(id, title string) (Workspace, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return Workspace{}, errors.New("bad-request: empty title")
	}
	e.mu.Lock()
	w := e.workspaces[id]
	if w == nil {
		e.mu.Unlock()
		return Workspace{}, errors.New("workspace-not-found")
	}
	for otherID, other := range e.workspaces {
		if otherID != id && other.Title == title {
			e.mu.Unlock()
			return Workspace{}, errors.New("workspace-name-conflict")
		}
	}
	if w.Title == title {
		copy := *w
		copy.SessionIDs = append([]string{}, w.SessionIDs...)
		e.mu.Unlock()
		return copy, nil
	}
	previous := *w
	previous.SessionIDs = append([]string(nil), w.SessionIDs...)
	w.Title = title
	w.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := e.saveStateLocked(); err != nil {
		*w = previous
		e.mu.Unlock()
		return Workspace{}, fmt.Errorf("workspace-persist-failed: %w", err)
	}
	copy := *w
	copy.SessionIDs = append([]string{}, w.SessionIDs...)
	e.mu.Unlock()
	e.emitHost(map[string]any{"type": "host/workspace-changed", "workspace": copy})
	return copy, nil
}
func (e *Engine) DeleteWorkspace(id string) error {
	e.mu.Lock()
	if e.workspaces[id] == nil {
		e.mu.Unlock()
		return errors.New("workspace-not-found")
	}
	previous := *e.workspaces[id]
	previous.SessionIDs = append([]string(nil), previous.SessionIDs...)
	previousOrder := append([]string(nil), e.workspaceOrder...)
	delete(e.workspaces, id)
	for i, item := range e.workspaceOrder {
		if item == id {
			e.workspaceOrder = append(e.workspaceOrder[:i], e.workspaceOrder[i+1:]...)
			break
		}
	}
	if err := e.saveStateLocked(); err != nil {
		e.workspaces[id] = &previous
		e.workspaceOrder = previousOrder
		e.mu.Unlock()
		return fmt.Errorf("workspace-persist-failed: %w", err)
	}
	e.mu.Unlock()
	e.emitHost(map[string]any{"type": "host/workspace-removed", "workspaceId": id})
	return nil
}
func (e *Engine) ArchiveSession(id string) ([]string, error) {
	if _, err := e.getSession(id); err != nil {
		return nil, err
	}
	e.mu.Lock()
	previous, existed := e.archived[id]
	e.archived[id] = true
	if err := e.saveStateLocked(); err != nil {
		if existed {
			e.archived[id] = previous
		} else {
			delete(e.archived, id)
		}
		e.mu.Unlock()
		return nil, fmt.Errorf("workspace-persist-failed: %w", err)
	}
	e.mu.Unlock()
	_, ids := e.ListWorkspaces()
	e.emitHost(map[string]any{"type": "host/archived-sessions-changed", "archivedSessionIds": ids})
	return ids, nil
}
func (e *Engine) AttachSession(workspaceID, sessionID string) error {
	if _, err := e.getSession(sessionID); err != nil {
		return err
	}
	e.mu.Lock()
	w := e.workspaces[workspaceID]
	if w == nil {
		e.mu.Unlock()
		return errors.New("workspace-not-found")
	}
	for _, id := range w.SessionIDs {
		if id == sessionID {
			e.mu.Unlock()
			return nil
		}
	}
	previousIDs := append([]string(nil), w.SessionIDs...)
	previousUpdatedAt := w.UpdatedAt
	w.SessionIDs = append([]string{sessionID}, w.SessionIDs...)
	w.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := e.saveStateLocked(); err != nil {
		w.SessionIDs = previousIDs
		w.UpdatedAt = previousUpdatedAt
		e.mu.Unlock()
		return fmt.Errorf("workspace-persist-failed: %w", err)
	}
	copy := *w
	copy.SessionIDs = append([]string{}, w.SessionIDs...)
	e.mu.Unlock()
	e.emitHost(map[string]any{"type": "host/workspace-changed", "workspace": copy})
	return nil
}
