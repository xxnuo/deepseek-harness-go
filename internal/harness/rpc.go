package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"
)

type RPCError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details"`
}

func (e *RPCError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

type clientRequest struct {
	Type    string          `json:"type"`
	RPCID   string          `json:"rpcId"`
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload"`
}
type serverResponse struct {
	Type   string `json:"type"`
	RPCID  string `json:"rpcId"`
	Result any    `json:"result"`
}

func okResult(id string, value any) serverResponse {
	return serverResponse{Type: "server-response", RPCID: id, Result: map[string]any{"ok": true, "value": value}}
}
func errResult(id string, err *RPCError) serverResponse {
	return serverResponse{Type: "server-response", RPCID: id, Result: map[string]any{"ok": false, "error": normalizeRPCError(err)}}
}
func rpcError(code, message string, details any) *RPCError {
	return normalizeRPCError(&RPCError{Code: code, Message: message, Details: details})
}

func normalizeRPCError(err *RPCError) *RPCError {
	if err == nil {
		return nil
	}
	normalized := *err
	normalized.Code = canonicalRPCErrorCode(err.Code)
	normalized.Details = detailsOrEmpty(err.Details)
	return &normalized
}

// canonicalRPCErrorCode keeps older internal call sites source-compatible
// while exposing the namespaced RemoteError vocabulary on every wire path.
func canonicalRPCErrorCode(code string) string {
	if strings.Contains(code, "/") {
		return code
	}
	switch code {
	case "bad-request":
		return "gateway/bad-request"
	case "cancelled":
		return "gateway/cancelled"
	case "internal":
		return "gateway/internal"
	case "input-invalid":
		return "gateway/input-invalid"
	case "invocation-unavailable":
		return "gateway/invocation-unavailable"
	case "session-conflict":
		return "session/conflict"
	case "session-not-found":
		return "session/not-found"
	case "model-unavailable":
		return "session/model-unavailable"
	case "attachment-error":
		return "session/attachment-invalid"
	case "queue-item-not-found":
		return "session/queue-item-not-found"
	case "steer-unavailable":
		return "session/steer-unavailable"
	case "title-invalid":
		return "session/title-invalid"
	case "fork-unavailable":
		return "session/fork-unavailable"
	case "workspace-attach-failed":
		return "session/workspace-attach-failed"
	case "agent-busy":
		return "session/agent-busy"
	case "agent-preset-conflict":
		return "agent-preset/conflict"
	case "agent-preset-not-found":
		return "agent-preset/not-found"
	case "agent-preset-invalid":
		return "agent-preset/invalid"
	case "agent-preset-read-only":
		return "agent-preset/read-only"
	case "agent-preset-locked":
		return "agent-preset/locked"
	case "workspace-not-found":
		return "workspace/not-found"
	case "workspace-invalid-path":
		return "workspace/invalid-path"
	case "workspace-name-conflict":
		return "workspace/name-conflict"
	case "workspace-move-invalid":
		return "workspace/move-invalid"
	case "directory-exists":
		return "directory-picker/exists"
	case "directory-unreadable":
		return "directory-picker/unreadable"
	case "directory-create-failed":
		return "directory-picker/create-failed"
	case "settings-conflict":
		return "settings/conflict"
	case "settings-rejected":
		return "settings/rejected"
	case "credential-rejected":
		return "credential/rejected"
	case "subagent-not-found":
		return "subagent/not-found"
	case "subagent-unauthorized":
		return "subagent/unauthorized"
	case "subagent-not-resumable":
		return "subagent/not-resumable"
	case "subagent-parent-unavailable":
		return "subagent/parent-unavailable"
	case "subagent-delivery-unavailable":
		return "subagent/delivery-unavailable"
	case "subagent-projections-unavailable":
		return "subagent/projections-unavailable"
	case "subagent-attachment-invalid":
		return "subagent/attachment-invalid"
	case "subagent-invalid-time-zone":
		return "subagent/invalid-time-zone"
	case "subagent-list-failed":
		return "gateway/internal"
	case "model-discovery-failed":
		return "llm/model-discovery-rejected"
	case "inventory-unavailable", "goal-not-found", "workspace-persist-failed":
		return "gateway/internal"
	case "invalid-time-zone":
		return "session/invalid-time-zone"
	default:
		return "gateway/internal"
	}
}
func detailsOrEmpty(v any) any {
	if v == nil {
		return map[string]any{}
	}
	encoded, err := json.Marshal(v)
	if err != nil || len(encoded) == 0 || encoded[0] != '{' {
		return map[string]any{}
	}
	if object, ok := v.(map[string]any); ok {
		return object
	}
	var object map[string]any
	if err := json.Unmarshal(encoded, &object); err != nil || object == nil {
		return map[string]any{}
	}
	return object
}

func rpcWithDetails(err *RPCError, details any) *RPCError {
	if err == nil {
		return nil
	}
	copy := *err
	copy.Details = details
	return normalizeRPCError(&copy)
}

func rpcMergeDetails(err *RPCError, fields map[string]any) *RPCError {
	if err == nil {
		return nil
	}
	details := map[string]any{}
	if existing, ok := detailsOrEmpty(err.Details).(map[string]any); ok {
		for key, value := range existing {
			details[key] = value
		}
	}
	for key, value := range fields {
		details[key] = value
	}
	return rpcWithDetails(err, details)
}

func withSubagentAddressDetails(err *RPCError, parentID, childID string) *RPCError {
	if err == nil {
		return nil
	}
	fields := map[string]any{}
	if parentID != "" {
		fields["parentSessionId"] = parentID
	}
	if childID != "" {
		fields["childSessionId"] = childID
	}
	switch err.Code {
	case "subagent/not-found", "subagent/catalog-diagnostic":
		return rpcMergeDetails(err, fields)
	case "subagent/parent-unavailable":
		delete(fields, "childSessionId")
		return rpcMergeDetails(err, fields)
	case "subagent/not-resumable", "subagent/unauthorized", "subagent/delivery-unavailable":
		delete(fields, "parentSessionId")
		return rpcMergeDetails(err, fields)
	default:
		return err
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
func errorToRPC(err error) *RPCError {
	if err == nil {
		return nil
	}
	var remote *RPCError
	if errors.As(err, &remote) {
		return normalizeRPCError(remote)
	}
	var sessionConflict *SessionConflictError
	if errors.As(err, &sessionConflict) {
		return rpcError("session-conflict", sessionConflict.Error(), map[string]any{
			"sessionId": sessionConflict.SessionID, "requestedCwd": sessionConflict.RequestedCWD,
			"existingCwd": sessionConflict.ExistingCWD,
		})
	}
	var presetConflict *AgentPresetConflictError
	if errors.As(err, &presetConflict) {
		details := map[string]any{"sessionId": presetConflict.SessionID, "requestedPreset": presetConflict.RequestedPreset}
		if presetConflict.ExistingPreset != "" {
			details["existingPreset"] = presetConflict.ExistingPreset
		}
		return rpcError("agent-preset-conflict", presetConflict.Error(), details)
	}
	var timeZone *ClientTimeZoneError
	if errors.As(err, &timeZone) {
		return rpcError("session/invalid-time-zone", timeZone.Error(), map[string]any{"value": timeZone.Value})
	}
	var reference *SessionReferenceError
	if errors.As(err, &reference) {
		return rpcError(string(reference.Code), reference.Message, map[string]any{})
	}
	var subagentService *SubagentServiceError
	if errors.As(err, &subagentService) {
		switch subagentService.Code {
		case "PARENT_UNAVAILABLE":
			return rpcError("subagent-parent-unavailable", subagentService.Message, map[string]any{})
		case "NOT_RESUMABLE":
			return rpcError("subagent-not-resumable", "subagent cannot be resumed", map[string]any{})
		case "UNAUTHORIZED":
			return rpcError("subagent-unauthorized", "subagent does not belong to this parent", map[string]any{})
		case "MODEL_DOES_NOT_SUPPORT_IMAGES":
			return rpcError("subagent-attachment-invalid", subagentService.Message, map[string]any{"reason": subagentService.Code})
		case "DRAINING", "ACTIVATION_CLOSING", "CONTINUATION_UNAVAILABLE", "PERSISTENCE_UNAVAILABLE":
			return rpcError("subagent-delivery-unavailable", "subagent follow-up is temporarily unavailable", map[string]any{})
		case "SUBAGENT_CONTROL_PROJECTIONS_UNAVAILABLE":
			return rpcError("subagent-projections-unavailable", subagentService.Message, map[string]any{})
		}
	}
	msg := err.Error()
	code := "internal"
	if i := strings.IndexByte(msg, ':'); i > 0 {
		candidate := msg[:i]
		if candidate != "" {
			code = candidate
		}
		msg = strings.TrimSpace(msg[i+1:])
	}
	return rpcError(code, msg, map[string]any{})
}

func decode[T any](raw json.RawMessage, dst *T) error {
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid payload: " + err.Error())
	}
	return nil
}

func (e *Engine) dispatch(ctx context.Context, method string, raw json.RawMessage, rpcIDs ...string) (any, *RPCError) {
	var p map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, rpcError("bad-request", "payload must be a JSON object", map[string]any{})
		}
	}
	if p == nil {
		p = map[string]any{}
	}
	switch method {
	case "host.describe":
		e.mu.RLock()
		sessions := make([]*Session, 0, len(e.sessions))
		for _, s := range e.sessions {
			sessions = append(sessions, s)
		}
		e.mu.RUnlock()
		attached := 0
		for _, s := range sessions {
			s.mu.Lock()
			if s.attached {
				attached++
			}
			s.mu.Unlock()
		}
		home, _ := os.UserHomeDir()
		cfg := e.Config()
		return map[string]any{"version": cfg.Version, "cwd": cfg.Workspace, "provider": cfg.Provider, "model": cfg.Model, "attachedSessions": attached, "home": home, "canOpenPath": openCommand() != ""}, nil
	case "host.listDirectory":
		path, _ := p["path"].(string)
		if path == "" {
			path, _ = os.UserHomeDir()
		}
		return e.listDirectory(path)
	case "host.createDirectory":
		path, _ := p["path"].(string)
		name, _ := p["name"].(string)
		if path == "" || name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\") {
			return nil, rpcError("bad-request", "name must be one path segment", map[string]any{})
		}
		target := filepath.Join(path, name)
		if _, err := os.Stat(target); err == nil {
			return nil, rpcError("directory-exists", "directory already exists", map[string]any{"path": target})
		}
		if err := os.Mkdir(target, 0o755); err != nil {
			return nil, rpcError("directory-create-failed", err.Error(), map[string]any{"path": target})
		}
		return map[string]any{"path": target}, nil
	case "host.openPath":
		path, _ := p["path"].(string)
		if path == "" {
			return nil, rpcError("bad-request", "path is required", nil)
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, rpcError("directory-unreadable", err.Error(), map[string]any{"path": path})
		}
		if _, err := os.Stat(abs); err != nil {
			return nil, rpcError("directory-unreadable", err.Error(), map[string]any{"path": path})
		}
		opener := openCommand()
		if opener == "" {
			return nil, rpcError("internal", "no desktop opener is available", map[string]any{"path": abs})
		}
		if err := exec.CommandContext(ctx, opener, abs).Start(); err != nil {
			return nil, rpcError("internal", err.Error(), map[string]any{"path": abs})
		}
		return map[string]any{"opened": true}, nil
	case "host.pickDirectory":
		path, err := e.pickDirectory(ctx)
		if err != nil {
			return nil, errorToRPC(err)
		}
		if path == "" {
			return map[string]any{"path": nil}, nil
		}
		return map[string]any{"path": path}, nil
	case "dynamicCordisRunner/syncInspectManifest":
		providers, present, err := decodeDynamicProviders(p)
		if err != nil {
			return nil, rpcError("bad-request", err.Error(), nil)
		}
		if present {
			if err := e.SyncInspectManifest(providers); err != nil {
				return nil, rpcError("bad-request", err.Error(), nil)
			}
		}
		return nil, nil
	case "dynamicCordisRunner/inventory":
		sessionID, _ := p["agentId"].(string)
		return e.DynamicCordisInventory(sessionID), nil
	case "dynamicCordisRunner/resolveInspectQuery":
		agentID, requestID, resolution, err := decodeDynamicResolution(p)
		if err != nil {
			return nil, rpcError("bad-request", err.Error(), nil)
		}
		ack := e.ResolveInspectQuery(agentID, requestID, resolution)
		return ack, nil
	case "session.list":
		return map[string]any{"items": e.ListSessions()}, nil
	case "session.search":
		q, _ := p["query"].(string)
		items, more := e.SearchSessions(q)
		return map[string]any{"items": items, "hasMore": more}, nil
	case "session.create":
		workspaceID, workspaceProvided := p["workspaceId"].(string)
		if _, exists := p["workspaceId"]; exists && (!workspaceProvided || workspaceID == "") {
			return nil, rpcError("bad-request", "session.create workspaceId must be a non-empty string", nil)
		}
		cwd, cwdProvided := p["cwd"].(string)
		if _, exists := p["cwd"]; exists && !cwdProvided {
			return nil, rpcError("bad-request", "session.create cwd must be a string", nil)
		}
		id, sessionProvided := p["sessionId"].(string)
		if _, exists := p["sessionId"]; exists && (!sessionProvided || id == "") {
			return nil, rpcError("bad-request", "session.create sessionId must be a non-empty string", nil)
		}
		if workspaceProvided && cwdProvided {
			return nil, rpcError("bad-request", "session.create accepts workspaceId or cwd, not both", nil)
		}
		if _, exists := p["reuseWorkspaceBlank"]; exists {
			return nil, rpcError("bad-request", "session.create does not accept reuseWorkspaceBlank", nil)
		}
		preset, presetProvided := p["agentPreset"].(string)
		if _, exists := p["agentPreset"]; exists && !presetProvided {
			return nil, rpcError("bad-request", "session.create agentPreset must be a string", nil)
		}
		resolvedPreset, presetErr := e.resolvePreset(preset)
		if presetErr != nil {
			return nil, presetErr
		}
		preset = resolvedPreset
		if workspaceProvided {
			e.mu.RLock()
			w := e.workspaces[workspaceID]
			e.mu.RUnlock()
			if w == nil {
				return nil, rpcError("workspace-not-found", "workspace not found", map[string]any{"workspaceId": workspaceID})
			}
			cwd = w.Path
		}
		var sid string
		var err error
		if presetProvided {
			sid, err = e.CreateSession(ctx, cwd, id, preset)
		} else {
			sid, err = e.createSessionWithPresetAdoption(ctx, SessionHeader{ID: id, CWD: cwd, AgentPreset: preset}, shouldPinPermissionSnapshot(preset), true)
			if err == nil && e.agentTeams != nil {
				e.agentTeams.recoverSession(sid)
			}
		}
		if err != nil {
			return nil, errorToRPC(err)
		}
		if workspaceProvided {
			if attachErr := e.AttachSession(workspaceID, sid); attachErr != nil {
				return nil, rpcError("workspace-attach-failed", attachErr.Error(), map[string]any{"sessionId": sid, "workspaceId": workspaceID})
			}
		}
		out := map[string]any{"sessionId": sid}
		if session, sessionErr := e.getSession(sid); sessionErr == nil {
			session.mu.Lock()
			effectivePreset := sessionAgentPreset(session.Header, session.Events)
			session.mu.Unlock()
			if effectivePreset != "" {
				out["agentPreset"] = effectivePreset
			}
		}
		return out, nil
	case "session.history":
		id, _ := p["sessionId"].(string)
		if _, fenceErr := e.requireOrdinary(id); fenceErr != nil {
			return nil, fenceErr
		}
		before := -1
		if v, ok := p["beforeSeq"].(float64); ok {
			before = int(v)
		}
		max := 50
		if v, ok := p["maxMessages"].(float64); ok {
			max = int(v)
		}
		events, more, err := e.History(id, before, max)
		if err != nil {
			return nil, errorToRPC(err)
		}
		value := map[string]any{"events": events, "hasMore": more}
		if before < 0 {
			snapshot, snapshotErr := e.SessionProjectionSnapshot(ctx, id)
			if snapshotErr != nil {
				return nil, errorToRPC(snapshotErr)
			}
			value["projections"] = map[string]any{"asOfSeq": snapshot.AsOfSeq, "values": snapshot.Values}
		}
		return value, nil
	case "session.models":
		id, _ := p["sessionId"].(string)
		s, fenceErr := e.requireOrdinary(id)
		if fenceErr != nil {
			return nil, fenceErr
		}
		s.mu.Lock()
		sel := s.Model
		if logged, ok := latestLoggedModel(s.Events); ok {
			sel = logged
		}
		s.mu.Unlock()
		groups, failures := e.Models(ctx)
		e.mu.RLock()
		_, routable := e.providers[sel.Provider]
		e.mu.RUnlock()
		return map[string]any{"current": sel, "routable": routable, "groups": groups, "failures": failures}, nil
	case "session.selectModel":
		id, _ := p["sessionId"].(string)
		if _, fenceErr := e.requireOrdinary(id); fenceErr != nil {
			return nil, fenceErr
		}
		provider, _ := p["provider"].(string)
		model, _ := p["model"].(string)
		effort, _ := p["reasoningEffort"].(string)
		if err := e.SelectModel(id, ModelSelection{Provider: provider, Model: model, ReasoningEffort: effort}); err != nil {
			return nil, errorToRPC(err)
		}
		selected := ModelSelection{Provider: provider, Model: model, ReasoningEffort: effort}
		return map[string]any{"selected": selected}, nil
	case "session.rename":
		id, _ := p["sessionId"].(string)
		if _, fenceErr := e.requireOrdinary(id); fenceErr != nil {
			return nil, fenceErr
		}
		title, _ := p["title"].(string)
		value, seq, err := e.RenameSession(id, title)
		if err != nil {
			return nil, errorToRPC(err)
		}
		return map[string]any{"title": value, "seq": seq}, nil
	case "session.prompt":
		id, _ := p["sessionId"].(string)
		var wire struct {
			SessionID      string                  `json:"sessionId"`
			Mode           string                  `json:"mode"`
			Content        []PromptContentPart     `json:"content"`
			ClientTimeZone *string                 `json:"clientTimeZone"`
			References     []SessionReferenceInput `json:"references"`
		}
		if err := decode(raw, &wire); err != nil {
			return nil, rpcError("bad-request", err.Error(), nil)
		}
		if id == "" {
			id = wire.SessionID
		}
		if _, fenceErr := e.requireOrdinary(id); fenceErr != nil {
			return nil, fenceErr
		}
		req := PromptRequest{Mode: wire.Mode, Content: wire.Content, References: wire.References, RPCID: firstRPCID(rpcIDs)}
		if wire.ClientTimeZone != nil {
			canonical, ok := canonicalClientTimeZone(*wire.ClientTimeZone)
			if !ok {
				return nil, errorToRPC(&ClientTimeZoneError{Value: *wire.ClientTimeZone})
			}
			req.ClientTimeZone = canonical
		}
		result, err := e.Prompt(ctx, id, req)
		if err != nil {
			return nil, errorToRPC(err)
		}
		return result, nil
	case "session.cancel":
		id, _ := p["sessionId"].(string)
		if _, fenceErr := e.requireOrdinary(id); fenceErr != nil {
			return nil, fenceErr
		}
		if err := e.CancelSession(id); err != nil {
			return nil, errorToRPC(err)
		}
		return map[string]any{"accepted": true}, nil
	case "session.fork":
		id, _ := p["sessionId"].(string)
		var atSeq *int
		if value, ok := p["atSeq"].(float64); ok {
			n := int(value)
			atSeq = &n
		}
		// Forking a subagent is an ordinary-session operation in the upstream
		// API: the resulting child is detached from the subagent role. Only
		// prompt/cancel/rename paths use the ordinary-session fence.
		if _, err := e.getSession(id); err != nil {
			forkErr := errorToRPC(err)
			forkErr.Details = map[string]any{"sessionId": id}
			return nil, forkErr
		}
		sid, err := e.ForkSessionAt(ctx, id, atSeq)
		if err != nil {
			forkErr := errorToRPC(err)
			forkErr.Details = map[string]any{"sessionId": id}
			return nil, forkErr
		}
		return map[string]any{"sessionId": sid}, nil
	case "session.updateQueue":
		if id, _ := p["sessionId"].(string); id != "" {
			if _, fenceErr := e.requireOrdinary(id); fenceErr != nil {
				return nil, fenceErr
			}
		}
		return e.updateQueue(p)
	case "session.attachment":
		return e.sessionAttachment(p)
	case "workspace.list":
		items, arch := e.ListWorkspaces()
		return map[string]any{"items": items, "archivedSessionIds": arch}, nil
	case "workspace.create":
		path, _ := p["path"].(string)
		w, created, err := e.CreateWorkspace(path)
		if err != nil {
			return nil, rpcWithDetails(errorToRPC(err), map[string]any{"path": path})
		}
		return map[string]any{"workspace": w, "created": created}, nil
	case "workspace.rename":
		id, _ := p["workspaceId"].(string)
		title, _ := p["title"].(string)
		w, err := e.RenameWorkspace(id, title)
		if err != nil {
			mapped := errorToRPC(err)
			switch mapped.Code {
			case "workspace/not-found":
				mapped = rpcWithDetails(mapped, map[string]any{"workspaceId": id})
			case "workspace/name-conflict":
				mapped = rpcWithDetails(mapped, map[string]any{"name": strings.TrimSpace(title)})
			}
			return nil, mapped
		}
		return map[string]any{"workspace": w}, nil
	case "workspace.delete":
		id, _ := p["workspaceId"].(string)
		if err := e.DeleteWorkspace(id); err != nil {
			return nil, rpcWithDetails(errorToRPC(err), map[string]any{"workspaceId": id})
		}
		return map[string]any{"deleted": true}, nil
	case "workspace.insertBefore":
		return e.reorderWorkspace(p)
	case "workspace.insertSessionBefore":
		return e.reorderSession(p)
	case "workspace.archiveSession":
		id, _ := p["sessionId"].(string)
		ids, err := e.ArchiveSession(id)
		if err != nil {
			return nil, rpcWithDetails(errorToRPC(err), map[string]any{"sessionId": id})
		}
		return map[string]any{"archivedSessionIds": ids}, nil
	case "skill.list":
		sessionID, _ := p["sessionId"].(string)
		skills, skillErr := e.skillsForSession(sessionID)
		if skillErr != nil {
			return nil, skillErr
		}
		return map[string]any{"skills": skills}, nil
	case "agentPreset.list":
		rows := e.presetEntries()
		authorable := false
		for _, root := range e.presetRoots() {
			if root[1] == "user" {
				authorable = true
				break
			}
		}
		return map[string]any{"presets": rows, "authorable": authorable, "hasDocument": openCommand() != ""}, nil
	case "agentPreset.select":
		id, _ := p["sessionId"].(string)
		preset, _ := p["agentPreset"].(string)
		resolved, presetErr := e.resolvePreset(preset)
		if presetErr != nil {
			return nil, presetErr
		}
		preset = resolved
		s, err := e.getSession(id)
		if err != nil {
			return nil, errorToRPC(err)
		}
		s.mu.Lock()
		blank, _ := sessionListMetadata(s.Events)
		if !blank {
			s.mu.Unlock()
			return nil, rpcError("agent-preset-locked", "session already started", map[string]any{"sessionId": id, "agentPreset": preset})
		}
		event, appendErr := appendEventLocked(s, "agent-preset/selected", map[string]any{"agentPreset": preset}, nil, nil, false)
		s.mu.Unlock()
		if appendErr != nil {
			return nil, agentPresetInvalid(preset, appendErr.Error())
		}
		e.publishEvent(id, event)
		e.emitRemoteEvent("agent-preset/selected", id, preset)
		return map[string]any{"agentPreset": preset}, nil
	case "agentPreset.read":
		id, _ := p["agentPreset"].(string)
		return e.readPresetContent(id)
	case "agentPreset.copy":
		from, _ := p["from"].(string)
		id, _ := p["agentPreset"].(string)
		name, _ := p["name"].(string)
		return e.copyPreset(from, id, name)
	case "agentPreset.openDocument":
		id, _ := p["agentPreset"].(string)
		row, ok := e.findPreset(id)
		if !ok {
			return nil, e.presetNotFound(id)
		}
		if row.trust != "user" {
			return nil, agentPresetReadOnly(id, "shipped presets are read-only")
		}
		if opener := openCommand(); opener != "" {
			if err := exec.CommandContext(ctx, opener, row.dir).Start(); err != nil {
				return nil, agentPresetInvalid(id, err.Error())
			}
			return map[string]any{"opened": true}, nil
		}
		return map[string]any{"opened": false, "path": row.dir}, nil
	case "agentPreset.remove":
		if err := e.removePreset(func() string { v, _ := p["agentPreset"].(string); return v }()); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "llm.models":
		groups, failures := e.Models(ctx)
		return map[string]any{"groups": groups, "failures": failures}, nil
	case "llm.providers":
		return map[string]any{"providers": e.providerViews()}, nil
	case "llm.discoverModels":
		return e.discoverModels(ctx, p)
	case "settings.describe":
		return e.settingsDescribe(), nil
	case "settings.openDocument":
		if ctx.Err() != nil {
			return nil, rpcError("cancelled", "settings document open was aborted", nil)
		}
		if !e.cfg.Persist {
			return nil, rpcError("internal", "settings provider has no local document to open", nil)
		}
		e.mu.Lock()
		prepareErr := e.prepareSettingsLocked()
		e.mu.Unlock()
		if prepareErr != nil {
			return nil, rpcError("internal", "settings document preparation failed: "+prepareErr.Error(), nil)
		}
		if ctx.Err() != nil {
			return nil, rpcError("cancelled", "settings document open was aborted", nil)
		}
		opener := openCommand()
		if opener == "" {
			return nil, rpcError("internal", "no desktop opener is available for the settings document", nil)
		}
		if err := exec.CommandContext(ctx, opener, settingsYAMLPathFor(e)).Start(); err != nil {
			if ctx.Err() != nil {
				return nil, rpcError("cancelled", "settings document open was aborted", nil)
			}
			return nil, rpcError("internal", "settings document open failed: "+err.Error(), nil)
		}
		return map[string]any{"opened": true}, nil
	case "settings.update":
		ns, _ := p["ns"].(string)
		patch, ok := p["patch"].(map[string]any)
		if !ok {
			return nil, rpcError("bad-request", "settings patch must be an object", nil)
		}
		var expected *int
		if v, ok := p["expectedRevision"].(float64); ok {
			n := int(v)
			expected = &n
		}
		return e.settingsUpdate(ns, patch, expected, false)
	case "settings.replace":
		ns, _ := p["ns"].(string)
		section, ok := p["section"].(map[string]any)
		if !ok {
			return nil, rpcError("bad-request", "settings section must be an object", nil)
		}
		var expected *int
		if v, ok := p["expectedRevision"].(float64); ok {
			n := int(v)
			expected = &n
		}
		return e.settingsUpdate(ns, section, expected, true)
	case "settings.mutate":
		ns, _ := p["ns"].(string)
		ops, ok := p["ops"].([]any)
		if !ok {
			return nil, rpcError("bad-request", "settings ops must be an array", nil)
		}
		var expected *int
		if v, ok := p["expectedRevision"].(float64); ok {
			n := int(v)
			expected = &n
		}
		return e.settingsMutate(ns, ops, expected)
	case "credentials.describe":
		return e.describeCredentials(p)
	case "credentials.set":
		ref, _ := p["ref"].(string)
		value, _ := p["value"].(string)
		if err := e.setCredential(ref, value); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "credentials.unset":
		ref, _ := p["ref"].(string)
		if err := e.unsetCredential(ref); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "goal.create":
		return e.goalFromPayload(p, "create")
	case "goal.edit":
		return e.goalFromPayload(p, "edit")
	case "goal.pause":
		return e.goalFromPayload(p, "pause")
	case "goal.resume":
		return e.goalFromPayload(p, "resume")
	case "goal.complete":
		return e.goalFromPayload(p, "complete")
	case "goal.clear":
		return e.goalFromPayload(p, "clear")
	case "subagent.list":
		return e.subagentList(ctx, p)
	case "subagent.history":
		return e.subagentHistory(p)
	case "subagent.prompt":
		return e.subagentPrompt(ctx, p, firstRPCID(rpcIDs))
	case "subagent.interrupt":
		return e.subagentInterrupt(p)
	default:
		return nil, rpcError("bad-request", "unknown method: "+method, map[string]any{})
	}
}

func firstRPCID(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
}

func (e *Engine) listDirectory(path string) (any, *RPCError) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, rpcError("directory-unreadable", err.Error(), map[string]any{"path": path})
	}
	entries, err := os.ReadDir(abs)
	if err != nil {
		return nil, rpcError("directory-unreadable", err.Error(), map[string]any{"path": abs})
	}
	rows := []map[string]any{}
	for _, entry := range entries {
		isDir := entry.IsDir()
		if entry.Type()&os.ModeSymlink != 0 {
			info, statErr := os.Stat(filepath.Join(abs, entry.Name()))
			isDir = statErr == nil && info.IsDir()
		}
		if !isDir {
			continue
		}
		rows = append(rows, map[string]any{"name": entry.Name(), "path": filepath.Join(abs, entry.Name()), "hidden": strings.HasPrefix(entry.Name(), ".")})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i]["name"].(string) < rows[j]["name"].(string) })
	home, _ := os.UserHomeDir()
	return map[string]any{"path": abs, "home": home, "crumbs": crumbs(abs), "entries": rows, "truncated": false}, nil
}
func crumbs(path string) []map[string]any {
	path = filepath.Clean(path)
	vol := filepath.VolumeName(path)
	root := vol + string(filepath.Separator)
	rest := strings.TrimPrefix(path, root)
	parts := strings.Split(strings.Trim(rest, string(filepath.Separator)), string(filepath.Separator))
	out := []map[string]any{{"name": root, "path": root, "hidden": false}}
	if path == root {
		return out
	}
	cur := root
	for _, part := range parts {
		if part == "" {
			continue
		}
		cur = filepath.Join(cur, part)
		out = append(out, map[string]any{"name": part, "path": cur, "hidden": false})
	}
	return out
}
func openCommand() string {
	for _, name := range []string{"xdg-open", "open", "start"} {
		if _, err := exec.LookPath(name); err == nil {
			return name
		}
	}
	return ""
}

func (e *Engine) pickDirectory(ctx context.Context) (string, error) {
	if configured := strings.TrimSpace(os.Getenv("DSH_PICK_DIRECTORY")); configured != "" {
		st, err := os.Stat(configured)
		if err != nil || !st.IsDir() {
			return "", fmt.Errorf("directory-unreadable: %s", configured)
		}
		return filepath.Abs(configured)
	}
	commands := make([][]string, 0, 3)
	switch runtime.GOOS {
	case "linux":
		commands = append(commands, []string{"zenity", "--file-selection", "--directory", "--title=选择工作区"}, []string{"kdialog", "--getexistingdirectory", e.cfg.Workspace})
	case "darwin":
		commands = append(commands, []string{"osascript", "-e", `POSIX path of (choose folder with prompt "选择工作区")`})
	case "windows":
		commands = append(commands, []string{"powershell", "-NoProfile", "-Command", "Add-Type -AssemblyName System.Windows.Forms; $d=New-Object System.Windows.Forms.FolderBrowserDialog; if($d.ShowDialog() -eq 'OK'){[Console]::Write($d.SelectedPath)}"})
	}
	for _, command := range commands {
		if _, err := exec.LookPath(command[0]); err != nil {
			continue
		}
		out, err := exec.CommandContext(ctx, command[0], command[1:]...).Output()
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		path := strings.TrimSpace(string(out))
		if err == nil && path != "" {
			st, statErr := os.Stat(path)
			if statErr == nil && st.IsDir() {
				return filepath.Abs(path)
			}
		}
	}
	return "", nil
}

func (e *Engine) reorderWorkspace(p map[string]any) (any, *RPCError) {
	id, _ := p["workspaceId"].(string)
	before, _ := p["beforeWorkspaceId"].(string)
	e.mu.Lock()
	if e.workspaces[id] == nil {
		e.mu.Unlock()
		return nil, rpcError("workspace-not-found", "workspace not found", map[string]any{"workspaceId": id})
	}
	previousOrder := append([]string(nil), e.workspaceOrder...)
	ids := append([]string(nil), e.workspaceOrder...)
	if len(ids) == 0 {
		for k := range e.workspaces {
			ids = append(ids, k)
		}
		sort.Strings(ids)
	}
	remove := func(xs []string, v string) []string {
		for i, x := range xs {
			if x == v {
				return append(xs[:i], xs[i+1:]...)
			}
		}
		return xs
	}
	if before == id {
		e.mu.Unlock()
		return map[string]any{"workspaceIds": ids}, nil
	}
	ids = remove(ids, id)
	if before == "" {
		ids = append(ids, id)
	} else {
		at := -1
		for i, x := range ids {
			if x == before {
				at = i
				break
			}
		}
		if at < 0 {
			e.mu.Unlock()
			return nil, rpcError("workspace-move-invalid", "anchor not found", map[string]any{"workspaceId": id})
		}
		ids = append(ids[:at], append([]string{id}, ids[at:]...)...)
	}
	if reflect.DeepEqual(ids, previousOrder) {
		e.mu.Unlock()
		return map[string]any{"workspaceIds": ids}, nil
	}
	e.workspaceOrder = ids
	if err := e.saveStateLocked(); err != nil {
		e.workspaceOrder = previousOrder
		e.mu.Unlock()
		return nil, rpcError("workspace-persist-failed", err.Error(), map[string]any{"workspaceId": id})
	}
	e.mu.Unlock()
	e.emitHost(map[string]any{"type": "host/workspace-order-changed", "workspaceIds": ids})
	return map[string]any{"workspaceIds": ids}, nil
}
func (e *Engine) reorderSession(p map[string]any) (any, *RPCError) {
	wid, _ := p["workspaceId"].(string)
	sid, _ := p["sessionId"].(string)
	before, _ := p["beforeSessionId"].(string)
	e.mu.Lock()
	w := e.workspaces[wid]
	if w == nil {
		e.mu.Unlock()
		return nil, rpcError("workspace-not-found", "workspace not found", map[string]any{"workspaceId": wid})
	}
	previousIDs := append([]string(nil), w.SessionIDs...)
	previousUpdatedAt := w.UpdatedAt
	find := func(v string) int {
		for i, x := range w.SessionIDs {
			if x == v {
				return i
			}
		}
		return -1
	}
	at := find(sid)
	if at < 0 {
		e.mu.Unlock()
		return nil, rpcError("workspace-move-invalid", "session is not in workspace", map[string]any{
			"workspaceId": wid, "sessionId": sid,
		})
	}
	if before != "" && find(before) < 0 {
		e.mu.Unlock()
		return nil, rpcError("workspace-move-invalid", "anchor is not in workspace", map[string]any{
			"workspaceId": wid, "sessionId": sid, "beforeSessionId": before,
		})
	}
	if before == sid {
		copy := *w
		copy.SessionIDs = append([]string{}, w.SessionIDs...)
		e.mu.Unlock()
		return map[string]any{"workspace": copy}, nil
	}
	ids := append([]string(nil), w.SessionIDs...)
	ids = append(ids[:at], ids[at+1:]...)
	if before == "" {
		ids = append(ids, sid)
	} else {
		pos := -1
		for i, value := range ids {
			if value == before {
				pos = i
				break
			}
		}
		ids = append(ids[:pos], append([]string{sid}, ids[pos:]...)...)
	}
	if reflect.DeepEqual(ids, previousIDs) {
		copy := *w
		copy.SessionIDs = append([]string{}, w.SessionIDs...)
		e.mu.Unlock()
		return map[string]any{"workspace": copy}, nil
	}
	w.SessionIDs = ids
	w.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := e.saveStateLocked(); err != nil {
		w.SessionIDs = previousIDs
		w.UpdatedAt = previousUpdatedAt
		e.mu.Unlock()
		return nil, rpcError("workspace-persist-failed", err.Error(), map[string]any{"workspaceId": wid, "sessionId": sid})
	}
	copy := *w
	copy.SessionIDs = append([]string{}, w.SessionIDs...)
	e.mu.Unlock()
	e.emitHost(map[string]any{"type": "host/workspace-changed", "workspace": copy})
	return map[string]any{"workspace": copy}, nil
}

func (e *Engine) providerViews() []map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()
	rows := make([]map[string]any, 0, 1+len(piAICatalog)+len(e.piAIProviders))
	if e.providers["deepseek-official"] != nil {
		rows = append(rows, map[string]any{
			"provider": "deepseek-official", "displayName": "DeepSeek",
			"settingsNs": "llm-deepseek", "settingsPath": []string{}, "active": true,
		})
	}
	piRoutes := make(map[string]bool, len(piAICatalog)+len(e.piAIProviders))
	for route := range piAICatalog {
		piRoutes[route] = true
	}
	for route := range e.piAIProviders {
		piRoutes[route] = true
	}
	ordered := make([]string, 0, len(piRoutes))
	for route := range piRoutes {
		ordered = append(ordered, route)
	}
	sort.Strings(ordered)
	for _, route := range ordered {
		profile := e.piAIProviders[route]
		displayName := route
		if profile != nil {
			displayName = profile.Name()
		} else if catalog := piAICatalog[route]; catalog.DisplayName != "" {
			displayName = catalog.DisplayName
		}
		_, catalogued := piAICatalog[route]
		rows = append(rows, map[string]any{
			"provider": route, "displayName": displayName,
			"settingsNs": piAISettingsNamespace, "settingsPath": []string{"providers", route},
			"active": profile != nil, "declared": !catalogued,
		})
	}
	return rows
}
func (e *Engine) describeCredentials(p map[string]any) (any, *RPCError) {
	refs, ok := p["refs"].([]any)
	if !ok || len(refs) > 64 {
		return nil, rpcError("bad-request", "refs must be an array of at most 64 credential references", nil)
	}
	out := map[string]any{}
	for _, v := range refs {
		ref, ok := v.(string)
		if !ok || !validCredentialRef(ref) {
			return nil, rpcError("bad-request", "credential reference is invalid", nil)
		}
		configured, source, writable := e.credentialInfo(ref)
		entry := map[string]any{"configured": configured, "writable": writable}
		if source != "" {
			entry["source"] = source
		}
		out[ref] = entry
	}
	return map[string]any{"credentials": out}, nil
}
func (e *Engine) goalFromPayload(p map[string]any, op string) (any, *RPCError) {
	id, _ := p["sessionId"].(string)
	s, fenceErr := e.requireOrdinary(id)
	if fenceErr != nil {
		return nil, fenceErr
	}
	objective, _ := p["objective"].(string)
	goalID, rev := "", 0
	if r, ok := p["ref"].(map[string]any); ok {
		goalID, _ = r["id"].(string)
		var valid bool
		rev, valid = eventSeqNumber(r["revision"])
		if goalID == "" || !valid || rev < 1 {
			return nil, rpcError("bad-request", "ref requires a non-empty id and positive integer revision", nil)
		}
	} else if op != "create" {
		return nil, rpcError("bad-request", "ref is required", nil)
	}
	max := 0
	if value, present := p["maxGoalRounds"]; present {
		var valid bool
		max, valid = eventSeqNumber(value)
		if !valid || max < 1 {
			return nil, rpcError("bad-request", "maxGoalRounds must be a positive integer", nil)
		}
	}
	value, err := e.goalMutation(id, goalID, op, objective, "", rev, max)
	if err != nil {
		return nil, errorToRPC(err)
	}
	if op == "create" || op == "resume" {
		e.startSessionWorker(s)
	}
	return value, nil
}

func (e *Engine) ForkSession(ctx context.Context, id string) (string, error) {
	return e.ForkSessionAt(ctx, id, nil)
}

// ForkSessionAt copies only a completed-turn prefix. The anchor is a message
// or event seq; an anchor inside an open turn is rejected rather than silently
// clipping the conversation to an older point.
func (e *Engine) ForkSessionAt(ctx context.Context, id string, atSeq *int) (string, error) {
	return e.forkSessionAtFrom(ctx, id, atSeq, nil, "")
}

func (e *Engine) forkSessionAtFrom(ctx context.Context, id string, atSeq *int, caller *dynamicCordisRun, childID string) (string, error) {
	source, err := e.getSession(id)
	if err != nil {
		return "", err
	}
	if err := e.ensurePermissionSnapshotFrom(source, caller); err != nil {
		return "", err
	}
	source.mu.Lock()
	cwd := source.Header.CWD
	parentID := source.Header.ParentSession
	events := append([]Event(nil), source.Events...)
	model := source.Model
	preset := sessionAgentPreset(source.Header, events)
	origin := source.Header.Origin
	title := source.Title
	source.mu.Unlock()
	if title == "" {
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].Type != "session/title" {
				continue
			}
			if data, ok := events[i].Data.(map[string]any); ok {
				title, _ = data["title"].(string)
			}
			if title != "" {
				break
			}
		}
	}
	if logged, ok := latestLoggedModel(events); ok {
		model = logged
	}
	cut := completedTurnCut(events, atSeq)
	if cut < 0 {
		lastSeq := -1
		if len(events) > 0 {
			lastSeq = int(events[len(events)-1].Seq)
		}
		if atSeq != nil && *atSeq >= 0 && *atSeq <= lastSeq {
			return "", fmt.Errorf("fork-unavailable: session %q has not completed the turn containing event %d", id, *atSeq)
		}
		return "", fmt.Errorf("fork-unavailable: session %q has no completed turn to fork from", id)
	}
	if cut+1 < len(events) {
		events = events[:cut+1]
	}
	// A fork carries the source log, including any pinned permissions. If the
	// source is a core/library session without that snapshot, seed the child
	// with the current default so the new Web session has a complete policy.
	seenPermission := map[string]bool{}
	for _, event := range events {
		switch event.Type {
		case "permission/preset", "sandbox/mode", "approval/policy":
			seenPermission[event.Type] = true
		}
	}
	pinPermission := len(seenPermission) != 3
	seedLength := len(events)
	if pinPermission {
		seedLength += 3
	}
	child, err := e.createSession(ctx, SessionHeader{
		ID: childID, CWD: cwd, ParentSession: id, SeedLength: seedLength, AgentPreset: preset,
	}, pinPermission)
	if err != nil {
		return "", err
	}
	s, _ := e.getSession(child)
	s.mu.Lock()
	s.Model = model
	s.Title = title
	s.mu.Unlock()
	workspaceID := e.forkWorkspaceID(id, parentID, origin)
	e.mu.Lock()
	// Preserve the source's workspace membership, or the nearest owning
	// ancestor for a subagent source.
	if workspaceID != "" {
		if workspace := e.workspaces[workspaceID]; workspace != nil && !containsString(workspace.SessionIDs, child) {
			workspace.SessionIDs = append(workspace.SessionIDs, child)
			workspace.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
	}
	_ = e.saveStateLocked()
	e.mu.Unlock()
	for _, ev := range events {
		if _, appendErr := e.appendSeedEventFrom(caller, s, ev); appendErr != nil {
			return "", appendErr
		}
	}
	s.mu.Lock()
	s.Header.SeedLength = len(s.Events)
	s.Header.IsSeeded = true
	s.InheritedEventCount = SessionLogOffset(len(s.Events))
	s.mu.Unlock()
	goal, hasGoal, goalErr := foldGoalState(events)
	if goalErr != nil {
		return "", goalErr
	}
	if hasGoal {
		goal.Activation = "disarmed"
		e.mu.Lock()
		e.goals[child] = goal
		e.mu.Unlock()
	}
	return child, nil
}

// ensurePermissionSnapshot mirrors the permission-presets provider's sweep of
// already-live sessions. Core/library sessions may start without the optional
// policy events; a Web operation that forks such a live session must first
// make its effective policy durable so anchors and child replay see one log.
func (e *Engine) ensurePermissionSnapshot(s *Session) error {
	return e.ensurePermissionSnapshotFrom(s, nil)
}

func (e *Engine) ensurePermissionSnapshotFrom(s *Session, origin *dynamicCordisRun) error {
	s.mu.Lock()
	if !s.attached {
		s.mu.Unlock()
		return nil
	}
	seen := map[string]bool{}
	for _, event := range s.Events {
		switch event.Type {
		case "permission/preset", "sandbox/mode", "approval/policy":
			seen[event.Type] = true
		}
	}
	s.mu.Unlock()
	if len(seen) == 3 {
		return nil
	}
	e.mu.RLock()
	preset := e.permissionDefaultPresetLocked()
	e.mu.RUnlock()
	spec := commandPermissionPresets[preset]
	items := []struct {
		typ  string
		data map[string]any
	}{
		{"permission/preset", map[string]any{"preset": preset}},
		{"sandbox/mode", map[string]any{"mode": spec.sandbox}},
		{"approval/policy", map[string]any{"policy": spec.approval}},
	}
	for _, item := range items {
		if seen[item.typ] {
			continue
		}
		var err error
		if origin == nil {
			_, err = e.appendEvent(s, item.typ, item.data)
		} else {
			_, err = e.dynamicCordisAppendSessionEvent(origin, s, item.typ, item.data, nil, nil, false)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// forkWorkspaceID returns the direct workspace for an ordinary source, or the
// nearest workspace-owning ancestor for a subagent source. The snapshots keep
// Engine and Session locks separate, matching the rest of the lifecycle code.
func (e *Engine) forkWorkspaceID(sourceID, parentID, origin string) string {
	find := func(sessionID string) string {
		if sessionID == "" {
			return ""
		}
		e.mu.RLock()
		defer e.mu.RUnlock()
		for workspaceID, workspace := range e.workspaces {
			if containsString(workspace.SessionIDs, sessionID) {
				return workspaceID
			}
		}
		return ""
	}
	if workspaceID := find(sourceID); workspaceID != "" || origin != "subagent" {
		return workspaceID
	}
	seen := map[string]bool{sourceID: true}
	for ancestorID := parentID; ancestorID != "" && !seen[ancestorID]; {
		seen[ancestorID] = true
		if workspaceID := find(ancestorID); workspaceID != "" {
			return workspaceID
		}
		ancestor, err := e.getSession(ancestorID)
		if err != nil {
			break
		}
		ancestor.mu.Lock()
		ancestorID = ancestor.Header.ParentSession
		ancestor.mu.Unlock()
	}
	return ""
}

func completedTurnCut(events []Event, atSeq *int) int {
	if len(events) == 0 {
		return -1
	}
	boundary := -1
	if atSeq == nil || *atSeq > int(events[len(events)-1].Seq) {
		// Omitted and past-end anchors use the latest turn boundary.
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].Type == "turn/end" {
				boundary = i
				break
			}
		}
	} else if *atSeq >= 0 {
		// An in-log anchor belongs to the first turn ending at or after it.
		for i, event := range events {
			if int(event.Seq) >= *atSeq && event.Type == "turn/end" {
				boundary = i
				break
			}
		}
	}
	if boundary < 0 {
		return -1
	}
	// Preserve standalone title/injection events until the next turn starts.
	cut := boundary
	for cut+1 < len(events) && events[cut+1].Type != "turn/start" {
		cut++
	}
	return cut
}
