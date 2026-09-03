package harness

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type remoteDescriptor struct {
	allowed  []string
	required []string
	legacy   bool
}

var remoteDescriptors = map[string]remoteDescriptor{
	"$events/result": {allowed: []string{"clientId", "eventId", "outcome"}, required: []string{"clientId", "eventId", "outcome"}},

	"agentPresets/copy":         {allowed: []string{"from", "id", "name"}, required: []string{"from", "id"}},
	"agentPresets/deletePreset": {allowed: []string{"id"}, required: []string{"id"}},
	"agentPresets/list":         {allowed: []string{}},
	"agentPresets/read":         {allowed: []string{"agentPreset"}, required: []string{"agentPreset"}},
	"agentPresets/select":       {allowed: []string{"agentId", "agentPreset"}, required: []string{"agentId", "agentPreset"}},

	"agentTeams/createTask": {allowed: []string{"agentId", "request"}, required: []string{"agentId", "request"}},
	"agentTeams/updateTask": {allowed: []string{"agentId", "request"}, required: []string{"agentId", "request"}},
	"agentTeams/view":       {allowed: []string{"agentId"}, required: []string{"agentId"}},

	"commands/execute":     {allowed: []string{"agentId", "images", "line"}, required: []string{"agentId", "images", "line"}},
	"commands/list":        {allowed: []string{"agentId"}, required: []string{"agentId"}},
	"credentials/describe": {allowed: []string{"refs"}, required: []string{"refs"}},
	"credentials/set":      {allowed: []string{"ref", "value"}, required: []string{"ref", "value"}},
	"credentials/unset":    {allowed: []string{"ref"}, required: []string{"ref"}},

	"directoryPicker/createDirectory": {allowed: []string{"name", "path"}, required: []string{"name", "path"}},
	"directoryPicker/list":            {allowed: []string{"path"}},
	"directoryPicker/pick":            {allowed: []string{}},

	"fileReferences/list": {allowed: []string{"agentId", "query"}, required: []string{"agentId", "query"}},
	"sessionReferenceResolver/candidates": {
		allowed: []string{"agentId", "query"}, required: []string{"agentId", "query"},
	},

	"goals/clear":    {allowed: []string{"agentId", "ref"}, required: []string{"agentId", "ref"}},
	"goals/complete": {allowed: []string{"agentId", "ref"}, required: []string{"agentId", "ref"}},
	"goals/create":   {allowed: []string{"agentId", "request"}, required: []string{"agentId", "request"}},
	"goals/edit":     {allowed: []string{"agentId", "ref", "request"}, required: []string{"agentId", "ref", "request"}},
	"goals/pause":    {allowed: []string{"agentId", "ref"}, required: []string{"agentId", "ref"}},
	"goals/resume":   {allowed: []string{"agentId", "ref"}, required: []string{"agentId", "ref"}},

	"messageFeedback/delete": {allowed: []string{"request"}, required: []string{"request"}},
	"messageFeedback/list":   {allowed: []string{"request"}, required: []string{"request"}},
	"messageFeedback/put":    {allowed: []string{"request"}, required: []string{"request"}},
	"pluginInventory/list":   {allowed: []string{}},

	"llm/discoverModels":            {allowed: []string{"request", "settingsNs"}, required: []string{"request", "settingsNs"}},
	"llm/listConfigurableProviders": {allowed: []string{}},
	"llm/listProviders":             {allowed: []string{}},

	"session/attachment":                   {allowed: []string{"request"}, required: []string{"request"}},
	"session/cancel":                       {allowed: []string{"request"}, required: []string{"request"}},
	"session/canOpenWorkspacePath":         {allowed: []string{}},
	"session/control":                      {allowed: []string{}},
	"session/create":                       {allowed: []string{"request"}, required: []string{"request"}},
	"session/follow":                       {allowed: []string{"request"}, required: []string{"request"}},
	"session/fork":                         {allowed: []string{"request"}, required: []string{"request"}},
	"session/list":                         {allowed: []string{"_request"}, required: []string{"_request"}},
	"session/modelCatalog":                 {allowed: []string{}},
	"session/openWorkspacePath":            {allowed: []string{"request"}, required: []string{"request"}},
	"session/page":                         {allowed: []string{"request"}, required: []string{"request"}},
	"session/prompt":                       {allowed: []string{"request"}, required: []string{"request"}},
	"session/rename":                       {allowed: []string{"request"}, required: []string{"request"}},
	"session/search":                       {allowed: []string{"request"}, required: []string{"request"}},
	"session/selectModel":                  {allowed: []string{"request"}, required: []string{"request"}},
	"session/updateQueue":                  {allowed: []string{"request"}, required: []string{"request"}},
	"settings/canOpenAgentPresetDirectory": {allowed: []string{}},
	"settings/describe":                    {allowed: []string{}},
	"settings/mutate":                      {allowed: []string{"expectedRevision", "ns", "ops"}, required: []string{"ns", "ops"}},
	"settings/openAgentPresetDirectory":    {allowed: []string{"agentPreset"}, required: []string{"agentPreset"}},
	"settings/openSettingsDocument":        {allowed: []string{}},
	"settings/replace":                     {allowed: []string{"expectedRevision", "ns", "section"}, required: []string{"ns", "section"}},
	"settings/update":                      {allowed: []string{"expectedRevision", "ns", "patch"}, required: []string{"ns", "patch"}},
	"skills/list":                          {allowed: []string{"request"}, required: []string{"request"}},
	"subagents/interruptByParent":          {allowed: []string{"childSessionId", "mode", "parentSessionId"}, required: []string{"childSessionId", "mode", "parentSessionId"}},
	"subagents/list":                       {allowed: []string{"parentSessionId"}, required: []string{"parentSessionId"}},
	"subagents/prompt":                     {allowed: []string{"request"}, required: []string{"request"}},

	"workspace/archiveSession":      {allowed: []string{"request"}, required: []string{"request"}},
	"workspace/create":              {allowed: []string{"request"}, required: []string{"request"}},
	"workspace/delete":              {allowed: []string{"request"}, required: []string{"request"}},
	"workspace/follow":              {allowed: []string{}},
	"workspace/insertBefore":        {allowed: []string{"request"}, required: []string{"request"}},
	"workspace/insertSessionBefore": {allowed: []string{"request"}, required: []string{"request"}},
	"workspace/rename":              {allowed: []string{"request"}, required: []string{"request"}},

	"dynamicCordisRunner/getClientCode":            {allowed: []string{"agentId", "pluginId", "pluginRunId"}, required: []string{"agentId", "pluginId", "pluginRunId"}},
	"dynamicCordisRunner/inventory":                {allowed: []string{}, legacy: true},
	"dynamicCordisRunner/invoke":                   {allowed: []string{"pluginId", "pluginRunId", "method", "args"}, required: []string{"pluginId", "pluginRunId", "method", "args"}},
	"dynamicCordisRunner/reportClientGuardFailure": {allowed: []string{"agentId", "pluginId", "pluginRunId", "failure"}, required: []string{"agentId", "pluginId", "pluginRunId", "failure"}},
	"dynamicCordisRunner/reportRenderFailure":      {allowed: []string{"agentId", "pluginId", "pluginRunId", "failure"}, required: []string{"agentId", "pluginId", "pluginRunId", "failure"}},
	"dynamicCordisRunner/resolveInspectQuery":      {allowed: []string{"agentId", "requestId", "resolution"}, required: []string{"agentId", "requestId", "resolution"}, legacy: true},
	"dynamicCordisRunner/resolveRequestRun":        {allowed: []string{"requestId", "resolution"}, required: []string{"requestId", "resolution"}},
	"dynamicCordisRunner/runHostHalf":              {allowed: []string{"agentId", "pluginId", "packageId", "mode", "requestId", "approveFutureVersions"}, required: []string{"agentId", "pluginId", "packageId", "mode", "requestId", "approveFutureVersions"}},
	"dynamicCordisRunner/settleUserRun":            {allowed: []string{"agentId", "pluginId", "resolution"}, required: []string{"agentId", "pluginId", "resolution"}},
	"dynamicCordisRunner/stopFromPanel":            {allowed: []string{"agentId", "pluginId"}, required: []string{"agentId", "pluginId"}},
	"dynamicCordisRunner/syncInspectManifest":      {allowed: []string{"providers"}, required: []string{"providers"}, legacy: true},
	"dynamicCordisRunner/undefineFromPanel":        {allowed: []string{"agentId", "pluginId"}, required: []string{"agentId", "pluginId"}},
}

func isRemoteEndpoint(endpoint string) bool {
	_, ok := remoteDescriptors[endpoint]
	return ok
}

func okRemoteResult(id string, value any, present bool) serverResponse {
	result := map[string]any{"ok": true}
	if present {
		result["value"] = value
	}
	return serverResponse{Type: "server-response", RPCID: id, Result: result}
}

func (e *Engine) dispatchRemote(ctx context.Context, endpoint string, raw json.RawMessage) (any, bool, *RPCError) {
	descriptor := remoteDescriptors[endpoint]
	args, err := remoteArgs(endpoint, raw, descriptor)
	if err != nil {
		return nil, false, err
	}
	switch endpoint {
	case "$events/result":
		return e.settleRemoteEventResult(endpoint, args)
	case "agentPresets/list":
		value, rpcErr := e.dispatch(ctx, "agentPreset.list", []byte(`{}`))
		return value, true, rpcErr
	case "agentPresets/read":
		id, rpcErr := remoteString(endpoint, args, "agentPreset")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		value, rpcErr := e.readPresetContent(id)
		return value, true, rpcErr
	case "agentPresets/copy":
		from, rpcErr := remoteString(endpoint, args, "from")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		id, rpcErr := remoteString(endpoint, args, "id")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		name := ""
		if _, present := args["name"]; present {
			name, rpcErr = remoteString(endpoint, args, "name")
			if rpcErr != nil {
				return nil, false, rpcErr
			}
		}
		value, rpcErr := e.copyPreset(from, id, name)
		return value, true, rpcErr
	case "agentPresets/deletePreset":
		id, rpcErr := remoteString(endpoint, args, "id")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		return map[string]any{}, true, e.removePreset(id)
	case "agentPresets/select":
		agentID, rpcErr := remoteString(endpoint, args, "agentId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		preset, rpcErr := remoteString(endpoint, args, "agentPreset")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		payload, _ := json.Marshal(map[string]any{"sessionId": agentID, "agentPreset": preset})
		value, rpcErr := e.dispatch(ctx, "agentPreset.select", payload)
		return value, true, rpcErr
	case "directoryPicker/pick":
		path, runErr := e.pickDirectory(ctx)
		if runErr != nil {
			return nil, false, errorToRPC(runErr)
		}
		if path == "" {
			return nil, true, nil
		}
		return path, true, nil
	case "directoryPicker/list":
		path := ""
		if _, present := args["path"]; present {
			var rpcErr *RPCError
			path, rpcErr = remoteString(endpoint, args, "path")
			if rpcErr != nil {
				return nil, false, rpcErr
			}
		}
		value, rpcErr := e.listDirectory(path)
		return value, true, rpcErr
	case "directoryPicker/createDirectory":
		path, rpcErr := remoteString(endpoint, args, "path")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		name, rpcErr := remoteString(endpoint, args, "name")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		payload, _ := json.Marshal(map[string]any{"path": path, "name": name})
		value, rpcErr := e.dispatch(ctx, "host.createDirectory", payload)
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		if object, ok := value.(map[string]any); ok {
			return object["path"], true, nil
		}
		return value, true, nil
	case "subagents/list":
		parentID, rpcErr := remoteString(endpoint, args, "parentSessionId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		value, rpcErr := e.subagentList(ctx, map[string]any{"parentSessionId": parentID})
		return value, true, rpcErr
	case "subagents/prompt":
		requestValue, requestErr := decodeRemoteSubagentPromptRequest(args["request"])
		if requestErr != nil {
			return nil, false, requestErr
		}
		parentID := requestValue["parentSessionId"].(string)
		childID := requestValue["childSessionId"].(string)
		parent, parentErr := e.getSession(parentID)
		if parentErr != nil {
			return nil, false, rpcError("subagent-parent-unavailable", "parent session is not live", map[string]any{"parentSessionId": parentID})
		}
		parent.mu.Lock()
		parentLive := parent.attached && !parent.draining
		parent.mu.Unlock()
		if !parentLive {
			return nil, false, rpcError("subagent-parent-unavailable", "parent session is not live", map[string]any{"parentSessionId": parentID})
		}
		requestID := requestValue["requestId"].(string)
		value, rpcErr := e.subagentPromptWithSource(ctx, requestValue, "user", requestID)
		if rpcErr != nil {
			rpcErr = remoteSubagentPromptError(rpcErr, parentID, childID)
		}
		return value, true, rpcErr
	case "subagents/interruptByParent":
		parentID, rpcErr := remoteString(endpoint, args, "parentSessionId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		childID, rpcErr := remoteString(endpoint, args, "childSessionId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		mode, rpcErr := remoteString(endpoint, args, "mode")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		if mode != "continuable" {
			return nil, false, rpcError("bad-request", "invalid payload for subagent.interrupt", map[string]any{})
		}
		payload := map[string]any{"parentSessionId": parentID, "childSessionId": childID, "mode": mode}
		value, rpcErr := e.subagentInterrupt(payload)
		return value, true, rpcErr
	case "workspace/create", "workspace/rename", "workspace/delete", "workspace/insertBefore", "workspace/insertSessionBefore", "workspace/archiveSession":
		request, requestErr := decodeRemoteWorkspaceRequest(endpoint, args["request"])
		if requestErr != nil {
			return nil, false, requestErr
		}
		method := strings.Replace(endpoint, "/", ".", 1)
		requestRaw, _ := json.Marshal(request)
		value, rpcErr := e.dispatch(ctx, method, requestRaw)
		return value, true, rpcErr
	case "commands/list":
		value, rpcErr := e.remoteCommandsList(args)
		return value, true, rpcErr
	case "commands/execute":
		return e.remoteCommandsExecute(ctx, args)
	case "fileReferences/list":
		return e.remoteFileReferencesList(ctx, args)
	case "sessionReferenceResolver/candidates":
		return e.remoteSessionReferenceCandidates(ctx, args)
	case "goals/create", "goals/edit", "goals/pause", "goals/resume", "goals/complete", "goals/clear":
		value, rpcErr := e.remoteGoal(endpoint, args)
		return value, true, rpcErr
	case "messageFeedback/list", "messageFeedback/put", "messageFeedback/delete":
		value, rpcErr := e.remoteMessageFeedback(endpoint, args)
		return value, true, rpcErr
	case "pluginInventory/list":
		value, rpcErr := e.remotePluginInventory()
		return value, true, rpcErr
	case "dynamicCordisRunner/inventory", "dynamicCordisRunner/syncInspectManifest", "dynamicCordisRunner/resolveInspectQuery":
		payload, _ := json.Marshal(map[string]any{"args": rawObject(args)})
		value, rpcErr := e.dispatch(ctx, endpoint, payload)
		return value, true, rpcErr
	case "dynamicCordisRunner/invoke":
		pluginID, rpcErr := remoteString(endpoint, args, "pluginId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		runID, rpcErr := remoteString(endpoint, args, "pluginRunId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		method, rpcErr := remoteString(endpoint, args, "method")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		var value any
		if err := json.Unmarshal(args["args"], &value); err != nil {
			return nil, false, remoteInputError(endpoint, "args", err)
		}
		invocation := e.DynamicCordisInvoke(ctx, pluginID, runID, method, value)
		if !invocation.OK {
			message := invocation.Message
			if message == "" {
				message = "dynamic Cordis invocation is unavailable"
			}
			return nil, false, rpcError("invocation-unavailable", message, map[string]any{
				"endpoint": endpoint, "code": invocation.Code,
			})
		}
		return invocation, true, nil
	case "dynamicCordisRunner/getClientCode":
		agentID, rpcErr := remoteString(endpoint, args, "agentId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		pluginID, rpcErr := remoteString(endpoint, args, "pluginId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		runID, rpcErr := remoteString(endpoint, args, "pluginRunId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		value, runErr := e.DynamicCordisGetClientCode(agentID, pluginID, runID)
		if runErr != nil {
			return nil, false, rpcError("invocation-unavailable", runErr.Error(), map[string]any{"endpoint": endpoint})
		}
		return value, true, nil
	case "dynamicCordisRunner/runHostHalf":
		agentID, rpcErr := remoteString(endpoint, args, "agentId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		pluginID, rpcErr := remoteString(endpoint, args, "pluginId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		packageID, rpcErr := remoteString(endpoint, args, "packageId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		mode, rpcErr := remoteString(endpoint, args, "mode")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		var requestID *string
		if !bytes.Equal(bytes.TrimSpace(args["requestId"]), []byte("null")) {
			var v string
			if json.Unmarshal(args["requestId"], &v) != nil {
				return nil, false, remoteInputError(endpoint, "requestId", errors.New("must be a string or null"))
			}
			requestID = &v
		}
		var approve bool
		if json.Unmarshal(args["approveFutureVersions"], &approve) != nil {
			return nil, false, remoteInputError(endpoint, "approveFutureVersions", errors.New("must be a boolean"))
		}
		value, runErr := e.DynamicCordisRunHostHalf(ctx, agentID, pluginID, packageID, mode, optionalString(requestID), approve)
		if runErr != nil {
			return nil, false, rpcError("internal", runErr.Error(), nil)
		}
		return value, true, nil
	case "dynamicCordisRunner/resolveRequestRun":
		requestID, rpcErr := remoteString(endpoint, args, "requestId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		var resolution DynamicCordisRunResolution
		if err := json.Unmarshal(args["resolution"], &resolution); err != nil {
			return nil, false, remoteInputError(endpoint, "resolution", err)
		}
		value, runErr := e.DynamicCordisResolveRequestRun(requestID, resolution)
		if runErr != nil {
			return nil, false, rpcError("internal", runErr.Error(), nil)
		}
		return value, true, nil
	case "dynamicCordisRunner/settleUserRun":
		agentID, rpcErr := remoteString(endpoint, args, "agentId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		pluginID, rpcErr := remoteString(endpoint, args, "pluginId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		var resolution DynamicCordisRunResolution
		if err := json.Unmarshal(args["resolution"], &resolution); err != nil {
			return nil, false, remoteInputError(endpoint, "resolution", err)
		}
		value, runErr := e.DynamicCordisSettleUserRun(agentID, pluginID, resolution)
		if runErr != nil {
			return nil, false, rpcError("internal", runErr.Error(), nil)
		}
		return value, true, nil
	case "dynamicCordisRunner/stopFromPanel":
		agentID, rpcErr := remoteString(endpoint, args, "agentId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		pluginID, rpcErr := remoteString(endpoint, args, "pluginId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		value, runErr := e.DynamicCordisStop(agentID, pluginID)
		if runErr != nil {
			return nil, false, rpcError("internal", runErr.Error(), nil)
		}
		return value, true, nil
	case "dynamicCordisRunner/undefineFromPanel":
		agentID, rpcErr := remoteString(endpoint, args, "agentId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		pluginID, rpcErr := remoteString(endpoint, args, "pluginId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		value, runErr := e.DynamicCordisUndefine(agentID, pluginID)
		if runErr != nil {
			return nil, false, rpcError("internal", runErr.Error(), nil)
		}
		return value, true, nil
	case "dynamicCordisRunner/reportClientGuardFailure", "dynamicCordisRunner/reportRenderFailure":
		agentID, rpcErr := remoteString(endpoint, args, "agentId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		pluginID, rpcErr := remoteString(endpoint, args, "pluginId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		runID, rpcErr := remoteString(endpoint, args, "pluginRunId")
		if rpcErr != nil {
			return nil, false, rpcErr
		}
		data, _ := json.Marshal(args["failure"])
		if endpoint == "dynamicCordisRunner/reportRenderFailure" {
			var failure DynamicCordisRenderFailure
			if json.Unmarshal(data, &failure) == nil && strings.TrimSpace(failure.Message) != "" {
				e.DynamicCordisReportRenderFailure(agentID, pluginID, runID, failure)
			}
		} else {
			var failure DynamicCordisErrorDetails
			if json.Unmarshal(data, &failure) == nil && strings.TrimSpace(failure.Message) != "" {
				e.DynamicCordisReportClientGuardFailure(agentID, pluginID, runID, failure)
			}
		}
		// Diagnostics are best-effort. Stale and foreign reports intentionally
		// settle successfully so a browser render failure cannot cascade.
		return nil, true, nil
	default:
		return nil, false, rpcError(
			"invocation-unavailable",
			"the Go host does not provide the dynamic Cordis package runtime",
			map[string]any{"endpoint": endpoint},
		)
	}
}

func optionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// remotePluginInventory projects the live Host Loader entries. Client boot
// graph entries are only a compatibility fallback for callers that did not
// provide composition metadata; they are not the Host inventory source when a
// profile has been composed.
func (e *Engine) remotePluginInventory() (map[string]any, *RPCError) {
	live := e.pluginInventorySnapshot()
	if live == nil {
		graph, _, err := e.buildBootGraph()
		if err != nil {
			return nil, rpcError("inventory-unavailable", err.Error(), nil)
		}
		live = make([]PluginInventoryEntry, 0, len(graph.Entries))
		for _, entry := range graph.Entries {
			phase := "active"
			live = append(live, PluginInventoryEntry{EntryID: entry.ID, ModuleName: entry.ID, Enabled: true, FiberPhase: &phase})
		}
	}
	entries := make([]map[string]any, 0, len(live))
	for _, entry := range live {
		if entry.Group {
			continue
		}
		entries = append(entries, map[string]any{
			"entryId": entry.EntryID, "moduleName": entry.ModuleName,
			"enabled": entry.Enabled, "fiberPhase": entry.FiberPhase,
		})
	}
	result := map[string]any{"entries": entries}
	presets := scanPresets(e)
	if len(presets) == 0 {
		return result, nil
	}
	groups := make([]map[string]any, 0, len(presets))
	defaultID := ""
	for _, item := range e.presetRows() {
		if value, _ := item["isDefault"].(bool); value {
			defaultID, _ = item["id"].(string)
			break
		}
	}
	for _, preset := range presets {
		group := map[string]any{
			"id": preset.id, "trust": preset.trust, "isDefault": preset.id == defaultID,
			"rows": []map[string]any{},
		}
		if preset.name != "" {
			group["name"] = preset.name
		}
		generation := e.livePresetRuntimeForInventory(preset.id)
		if generation != nil {
			group["rows"] = presetInventoryWireRows(generation.pluginInventory, true)
		} else if preset.broken != "" {
			group["broken"] = preset.broken
		} else {
			rows, err := compilePresetCompositionRows(preset.content, false, false)
			if err != nil {
				group["broken"] = err.Error()
			} else {
				group["rows"] = compositionRowsWire(rows)
			}
		}
		groups = append(groups, group)
	}
	result["agentPresets"] = groups
	return result, nil
}

func (e *Engine) livePresetRuntimeForInventory(id string) *presetRuntimeGeneration {
	id = canonicalPresetID(id)
	e.mu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.mu.RUnlock()

	var latest *presetRuntimeGeneration
	for _, session := range sessions {
		session.mu.Lock()
		attached := session.attached
		preset := sessionAgentPreset(session.Header, session.Events)
		generation := session.presetRuntime
		session.mu.Unlock()
		if !attached || preset != id || generation == nil || canonicalPresetID(generation.presetID) != id {
			continue
		}
		if latest == nil || generation.mtimeMs > latest.mtimeMs {
			latest = generation
		}
	}
	return latest
}

func presetInventoryWireRows(entries []PluginInventoryEntry, live bool) []map[string]any {
	rows := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		row := map[string]any{
			"entryId":    entry.EntryID,
			"moduleName": entry.ModuleName,
			"enabled":    any(entry.Enabled),
			"fiberPhase": entry.FiberPhase,
		}
		if entry.EntryID == "" {
			row["entryId"] = nil
		}
		if entry.conditional {
			row["enabled"] = "conditional"
		}
		if entry.condition != "" {
			row["condition"] = entry.condition
		}
		if !live {
			row["fiberPhase"] = nil
		}
		rows = append(rows, row)
	}
	return rows
}

func compositionRowsWire(rows []presetCompositionRow) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		entryID := any(row.entryID)
		if row.entryID == "" {
			entryID = nil
		}
		item := map[string]any{
			"entryId": entryID, "moduleName": row.moduleName, "enabled": row.enabled,
			"fiberPhase": row.fiberPhase,
		}
		if row.condition != "" {
			item["condition"] = row.condition
		}
		out = append(out, item)
	}
	return out
}

func remoteArgs(endpoint string, raw json.RawMessage, descriptor remoteDescriptor) (map[string]json.RawMessage, *RPCError) {
	payload, err := remoteObject(raw, nil, nil)
	if err != nil {
		return nil, remoteInputError(endpoint, "payload", err)
	}
	if descriptor.legacy {
		if wrapped, exists := payload["args"]; !exists {
			return payload, nil
		} else if len(payload) != 1 {
			return nil, remoteInputError(endpoint, "payload", errors.New("must contain only args"))
		} else {
			args, objectErr := remoteObject(wrapped, descriptor.allowed, descriptor.required)
			if objectErr != nil {
				return nil, remoteInputError(endpoint, "args", objectErr)
			}
			return args, nil
		}
	}
	wrapped, exists := payload["args"]
	if !exists || len(payload) != 1 {
		return nil, remoteInputError(endpoint, "payload", errors.New("must contain exactly one args object"))
	}
	args, objectErr := remoteObject(wrapped, descriptor.allowed, descriptor.required)
	if objectErr != nil {
		return nil, remoteInputError(endpoint, "args", objectErr)
	}
	return args, nil
}

func remoteObject(raw json.RawMessage, allowed, required []string) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, errors.New("must be an object")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New("must be an object")
	}
	if allowed != nil {
		allowedSet := make(map[string]bool, len(allowed))
		for _, field := range allowed {
			allowedSet[field] = true
		}
		for field := range object {
			if !allowedSet[field] {
				return nil, fmt.Errorf("contains unknown field %q", field)
			}
		}
	}
	for _, field := range required {
		if _, ok := object[field]; !ok {
			return nil, fmt.Errorf("is missing field %q", field)
		}
	}
	return object, nil
}

func remoteInputError(endpoint, field string, err error) *RPCError {
	return rpcError("input-invalid", "invalid Remote "+field+": "+err.Error(), map[string]any{"endpoint": endpoint, "field": field})
}

func remoteBoundaryError(endpoint, field string) *RPCError {
	return rpcError("internal", fmt.Sprintf("typert gateway: %s: wire field %q failed boundary validation", endpoint, field), map[string]any{})
}

func rawObject(values map[string]json.RawMessage) map[string]any {
	out := make(map[string]any, len(values))
	for key, raw := range values {
		var value any
		if json.Unmarshal(raw, &value) == nil {
			out[key] = value
		}
	}
	return out
}

func remoteString(endpoint string, object map[string]json.RawMessage, field string) (string, *RPCError) {
	var value string
	if err := json.Unmarshal(object[field], &value); err != nil || bytes.Equal(bytes.TrimSpace(object[field]), []byte("null")) {
		return "", remoteInputError(endpoint, field, errors.New("must be a string"))
	}
	return value, nil
}

func remoteBadRequest(field, message string) *RPCError {
	return rpcError("bad-request", "invalid Remote "+field+": "+message, nil)
}

func decodeRemoteSubagentPromptRequest(raw json.RawMessage) (map[string]any, *RPCError) {
	allowed := []string{"requestId", "parentSessionId", "childSessionId", "mode", "content", "clientTimeZone"}
	required := []string{"requestId", "parentSessionId", "childSessionId", "mode", "content"}
	object, err := remoteObject(raw, allowed, required)
	if err != nil {
		return nil, remoteBadRequest("request", err.Error())
	}
	result := make(map[string]any, len(object))
	for _, field := range []string{"requestId", "parentSessionId", "childSessionId"} {
		value, ok := nonEmptyRemoteString(object[field])
		if !ok {
			return nil, remoteBadRequest("request."+field, "must be a non-empty string")
		}
		result[field] = value
	}
	mode, ok := nonEmptyRemoteString(object["mode"])
	if !ok || mode != "continuable" {
		return nil, remoteBadRequest("request.mode", "must be \"continuable\"")
	}
	result["mode"] = mode

	contentRaw := bytes.TrimSpace(object["content"])
	if len(contentRaw) == 0 || contentRaw[0] != '[' {
		return nil, remoteBadRequest("request.content", "must be an array")
	}
	var rawBlocks []json.RawMessage
	if err := json.Unmarshal(contentRaw, &rawBlocks); err != nil || rawBlocks == nil {
		return nil, remoteBadRequest("request.content", "must be an array")
	}
	content := make([]any, 0, len(rawBlocks))
	for index, rawBlock := range rawBlocks {
		block, blockErr := decodeRemoteSubagentPromptBlock(rawBlock)
		if blockErr != nil {
			return nil, remoteBadRequest(fmt.Sprintf("request.content[%d]", index), blockErr.Error())
		}
		content = append(content, block)
	}
	result["content"] = content
	if rawZone, present := object["clientTimeZone"]; present {
		zone, ok := remoteJSON[string](rawZone)
		if !ok {
			return nil, remoteBadRequest("request.clientTimeZone", "must be a string")
		}
		result["clientTimeZone"] = zone
	}
	return result, nil
}

func decodeRemoteSubagentPromptBlock(raw json.RawMessage) (map[string]any, error) {
	object, err := remoteObject(raw, nil, nil)
	if err != nil {
		return nil, err
	}
	typ, ok := remoteJSON[string](object["type"])
	if !ok {
		return nil, errors.New("type must be a string")
	}
	switch typ {
	case "text":
		object, err = remoteObject(raw, []string{"type", "text"}, []string{"type", "text"})
		if err != nil {
			return nil, err
		}
		text, ok := remoteJSON[string](object["text"])
		if !ok {
			return nil, errors.New("text must be a string")
		}
		return map[string]any{"type": typ, "text": text}, nil
	case "image":
		object, err = remoteObject(raw, []string{"type", "mediaType", "data", "name"}, []string{"type", "mediaType", "data"})
		if err != nil {
			return nil, err
		}
		mediaType, mediaOK := remoteJSON[string](object["mediaType"])
		data, dataOK := remoteJSON[string](object["data"])
		if !mediaOK || !dataOK {
			return nil, errors.New("mediaType and data must be strings")
		}
		switch mediaType {
		case "image/png", "image/jpeg", "image/webp", "image/gif":
		default:
			return nil, errors.New("mediaType is not a supported image type")
		}
		result := map[string]any{"type": typ, "mediaType": mediaType, "data": data}
		if rawName, present := object["name"]; present {
			name, ok := remoteJSON[string](rawName)
			if !ok {
				return nil, errors.New("name must be a string")
			}
			result["name"] = name
		}
		return result, nil
	default:
		return nil, errors.New("type must be \"text\" or \"image\"")
	}
}

func nonEmptyRemoteString(raw json.RawMessage) (string, bool) {
	value, ok := remoteJSON[string](raw)
	return value, ok && value != ""
}

func remoteJSON[T any](raw json.RawMessage) (T, bool) {
	var value T
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &value) != nil {
		return value, false
	}
	return value, true
}

func remoteSubagentPromptError(err *RPCError, parentID, childID string) *RPCError {
	if err == nil {
		return nil
	}
	switch err.Code {
	case "subagent/parent-unavailable", "subagent/not-found", "subagent/catalog-diagnostic", "subagent/not-resumable", "subagent/unauthorized", "subagent/delivery-unavailable":
		return withSubagentAddressDetails(err, parentID, childID)
	case "subagent/attachment-invalid":
		if details, ok := err.Details.(map[string]any); ok && details["reason"] != nil {
			return err
		}
		return rpcWithDetails(err, map[string]any{"reason": "IMAGE_INVALID"})
	default:
		return err
	}
}

func decodeRemoteWorkspaceRequest(endpoint string, raw json.RawMessage) (map[string]any, *RPCError) {
	specs := map[string]struct {
		allowed  []string
		required []string
	}{
		"workspace/create":              {[]string{"path"}, []string{"path"}},
		"workspace/rename":              {[]string{"workspaceId", "title"}, []string{"workspaceId", "title"}},
		"workspace/delete":              {[]string{"workspaceId"}, []string{"workspaceId"}},
		"workspace/insertBefore":        {[]string{"workspaceId", "beforeWorkspaceId"}, []string{"workspaceId"}},
		"workspace/insertSessionBefore": {[]string{"workspaceId", "sessionId", "beforeSessionId"}, []string{"workspaceId", "sessionId"}},
		"workspace/archiveSession":      {[]string{"sessionId"}, []string{"sessionId"}},
	}
	spec := specs[endpoint]
	object, err := remoteObject(raw, spec.allowed, spec.required)
	if err != nil {
		return nil, remoteBadRequest("request", err.Error())
	}
	result := make(map[string]any, len(object))
	for _, field := range spec.required {
		value, ok := remoteJSON[string](object[field])
		if !ok || (field != "path" && value == "") {
			return nil, remoteBadRequest("request."+field, "must be a non-empty string")
		}
		result[field] = value
	}
	for _, field := range []string{"beforeWorkspaceId", "beforeSessionId"} {
		if rawValue, present := object[field]; present {
			value, ok := remoteJSON[string](rawValue)
			if !ok || value == "" {
				return nil, remoteBadRequest("request."+field, "must be a non-empty string")
			}
			result[field] = value
		}
	}
	return result, nil
}

func remoteInt(endpoint string, object map[string]json.RawMessage, field string) (int, *RPCError) {
	var value int
	if err := json.Unmarshal(object[field], &value); err != nil {
		return 0, remoteInputError(endpoint, field, errors.New("must be an integer"))
	}
	return value, nil
}

func (e *Engine) remoteCommandsList(args map[string]json.RawMessage) (any, *RPCError) {
	id, err := remoteString("commands/list", args, "agentId")
	if err != nil {
		return nil, err
	}
	session, sessionErr := e.getSession(id)
	if sessionErr != nil {
		return nil, errorToRPC(sessionErr)
	}
	catalog, catalogErr := e.commandCatalogForSession(session)
	if catalogErr != nil {
		return nil, errorToRPC(catalogErr)
	}
	return catalog, nil
}

func (e *Engine) remoteCommandsExecute(ctx context.Context, args map[string]json.RawMessage) (any, bool, *RPCError) {
	id, err := remoteString("commands/execute", args, "agentId")
	if err != nil {
		return nil, false, err
	}
	line, err := remoteString("commands/execute", args, "line")
	if err != nil {
		return nil, false, err
	}
	images, err := remoteEncodedImages(args["images"])
	if err != nil {
		return nil, false, remoteBoundaryError("commands/execute", "images")
	}
	s, sessionErr := e.getSession(id)
	if sessionErr != nil {
		return nil, false, errorToRPC(sessionErr)
	}
	execution, admitted, runErr := e.executeCommand(ctx, s, line, images)
	if runErr != nil {
		return nil, false, errorToRPC(runErr)
	}
	if !admitted || execution == nil {
		return nil, false, nil
	}
	return execution, true, nil
}

func remoteEncodedImages(raw json.RawMessage) ([]EncodedImageAttachment, *RPCError) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, remoteBoundaryError("commands/execute", "images")
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(raw, &rows); err != nil || rows == nil {
		return nil, remoteBoundaryError("commands/execute", "images")
	}
	images := make([]EncodedImageAttachment, len(rows))
	for index, row := range rows {
		object, err := remoteObject(row, []string{"mediaType", "data", "name"}, []string{"mediaType", "data"})
		if err != nil {
			return nil, remoteBoundaryError("commands/execute", "images")
		}
		mediaType, rpcErr := remoteString("commands/execute", object, "mediaType")
		if rpcErr != nil || imageExtension(mediaType) == "" {
			return nil, remoteBoundaryError("commands/execute", "images")
		}
		data, rpcErr := remoteString("commands/execute", object, "data")
		if rpcErr != nil {
			return nil, remoteBoundaryError("commands/execute", "images")
		}
		name := ""
		if _, present := object["name"]; present {
			name, rpcErr = remoteString("commands/execute", object, "name")
			if rpcErr != nil {
				return nil, remoteBoundaryError("commands/execute", "images")
			}
		}
		images[index] = EncodedImageAttachment{MediaType: mediaType, Data: data, Name: name}
	}
	return images, nil
}

func (e *Engine) remoteFileReferencesList(ctx context.Context, args map[string]json.RawMessage) (any, bool, *RPCError) {
	agentID, err := remoteString("fileReferences/list", args, "agentId")
	if err != nil {
		return nil, false, err
	}
	query, err := remoteString("fileReferences/list", args, "query")
	if err != nil {
		return nil, false, err
	}
	candidates, listErr := e.ListFileReferenceCandidates(ctx, agentID, query)
	if listErr != nil {
		return nil, false, errorToRPC(listErr)
	}
	return candidates, true, nil
}

func (e *Engine) remoteSessionReferenceCandidates(ctx context.Context, args map[string]json.RawMessage) (any, bool, *RPCError) {
	agentID, err := remoteString("sessionReferenceResolver/candidates", args, "agentId")
	if err != nil {
		return nil, false, err
	}
	query, err := remoteString("sessionReferenceResolver/candidates", args, "query")
	if err != nil {
		return nil, false, err
	}
	candidates, listErr := e.ListSessionReferenceMentionCandidates(ctx, agentID, query)
	if listErr != nil {
		return nil, false, errorToRPC(listErr)
	}
	return candidates, true, nil
}

type remoteGoalRef struct {
	ID       string `json:"id"`
	Revision int    `json:"revision"`
}

func decodeGoalRef(endpoint string, args map[string]json.RawMessage) (remoteGoalRef, *RPCError) {
	object, err := remoteObject(args["ref"], []string{"id", "revision"}, []string{"id", "revision"})
	if err != nil {
		return remoteGoalRef{}, remoteInputError(endpoint, "ref", err)
	}
	id, rpcErr := remoteString(endpoint, object, "id")
	if rpcErr != nil {
		return remoteGoalRef{}, rpcErr
	}
	revision, rpcErr := remoteInt(endpoint, object, "revision")
	if rpcErr != nil {
		return remoteGoalRef{}, rpcErr
	}
	return remoteGoalRef{ID: id, Revision: revision}, nil
}

func (e *Engine) remoteGoal(endpoint string, args map[string]json.RawMessage) (any, *RPCError) {
	agentID, err := remoteString(endpoint, args, "agentId")
	if err != nil {
		return nil, err
	}
	op := strings.TrimPrefix(endpoint, "goals/")
	if op == "create" {
		request, objectErr := remoteObject(args["request"], []string{"objective", "maxGoalRounds"}, []string{"objective"})
		if objectErr != nil {
			return nil, remoteInputError(endpoint, "request", objectErr)
		}
		objective, rpcErr := remoteString(endpoint, request, "objective")
		if rpcErr != nil {
			return nil, rpcErr
		}
		maxRounds := 0
		if _, present := request["maxGoalRounds"]; present {
			maxRounds, rpcErr = remoteInt(endpoint, request, "maxGoalRounds")
			if rpcErr != nil {
				return nil, rpcErr
			}
			if maxRounds < 1 {
				return nil, remoteInputError(endpoint, "maxGoalRounds", errors.New("must be positive"))
			}
		}
		value, mutationErr := e.goalMutation(agentID, "", "create", objective, "", 0, maxRounds)
		if mutationErr != nil {
			return nil, errorToRPC(mutationErr)
		}
		return value, nil
	}
	ref, rpcErr := decodeGoalRef(endpoint, args)
	if rpcErr != nil {
		return nil, rpcErr
	}
	objective, maxRounds := "", 0
	if op == "edit" {
		request, objectErr := remoteObject(args["request"], []string{"objective", "maxGoalRounds"}, nil)
		if objectErr != nil {
			return nil, remoteInputError(endpoint, "request", objectErr)
		}
		if len(request) == 0 {
			return nil, remoteInputError(endpoint, "request", errors.New("must change objective or maxGoalRounds"))
		}
		if _, present := request["objective"]; present {
			objective, rpcErr = remoteString(endpoint, request, "objective")
			if rpcErr != nil {
				return nil, rpcErr
			}
			if strings.TrimSpace(objective) == "" {
				return nil, remoteInputError(endpoint, "objective", errors.New("must not be blank"))
			}
		}
		if _, present := request["maxGoalRounds"]; present {
			maxRounds, rpcErr = remoteInt(endpoint, request, "maxGoalRounds")
			if rpcErr != nil {
				return nil, rpcErr
			}
			if maxRounds < 1 {
				return nil, remoteInputError(endpoint, "maxGoalRounds", errors.New("must be positive"))
			}
		}
	}
	before, getErr := e.GetGoal(agentID)
	if getErr != nil {
		return nil, errorToRPC(getErr)
	}
	if _, mutationErr := e.goalMutation(agentID, ref.ID, op, objective, "", ref.Revision, maxRounds); mutationErr != nil {
		return nil, errorToRPC(mutationErr)
	}
	if op == "clear" {
		return map[string]any{"id": before.ID, "revision": before.Revision + 1}, nil
	}
	return e.remoteGoalView(agentID)
}

func (e *Engine) remoteGoalView(sessionID string) (map[string]any, *RPCError) {
	goal, err := e.GetGoal(sessionID)
	if err != nil {
		return nil, errorToRPC(err)
	}
	if goal == nil {
		return nil, rpcError("goal-not-found", "goal not found", map[string]any{"sessionId": sessionID})
	}
	e.mu.RLock()
	state := e.goals[sessionID]
	e.mu.RUnlock()
	value := goalWithoutActivation(*goal)
	value["activation"] = goal.Activation
	value["createdAt"] = state.CreatedAt
	value["updatedAt"] = state.UpdatedAt
	return value, nil
}

const maxFeedbackNoteBytes = 8192

type feedbackItem struct {
	MessageID string `json:"messageId"`
	Rating    string `json:"rating"`
	Note      string `json:"note,omitempty"`
	Version   string `json:"version"`
	CreatedAt int64  `json:"createdAt"`
	UpdatedAt int64  `json:"updatedAt"`
}

type feedbackRow struct {
	Session feedbackSessionIdentity `json:"session"`
	Items   []feedbackItem          `json:"items"`
}

type feedbackSessionIdentity struct {
	CreatedAt int64  `json:"createdAt"`
	CWD       string `json:"cwd,omitempty"`
}

// ponytail: one process-wide lock is enough here; use per-session locks if feedback traffic becomes concurrent.
var feedbackFileMu sync.Mutex

func (e *Engine) remoteMessageFeedback(endpoint string, args map[string]json.RawMessage) (any, *RPCError) {
	request, err := remoteObject(args["request"], feedbackAllowed(endpoint), feedbackRequired(endpoint))
	if err != nil {
		return nil, remoteBoundaryError(endpoint, "request")
	}
	sessionID, rpcErr := remoteString(endpoint, request, "sessionId")
	if rpcErr != nil {
		return nil, remoteBoundaryError(endpoint, "request")
	}
	messageID, rating, note, ifVersion := "", "", "", ""
	var requestedVersion *string
	if endpoint != "messageFeedback/list" {
		messageID, rpcErr = remoteString(endpoint, request, "messageId")
		if rpcErr != nil {
			return nil, remoteBoundaryError(endpoint, "request")
		}
	}
	if endpoint == "messageFeedback/delete" {
		ifVersion, rpcErr = remoteString(endpoint, request, "ifVersion")
		if rpcErr != nil {
			return nil, remoteBoundaryError(endpoint, "request")
		}
	}
	if endpoint == "messageFeedback/put" {
		rating, rpcErr = remoteString(endpoint, request, "rating")
		if rpcErr != nil || rating != "positive" && rating != "negative" {
			return nil, remoteBoundaryError(endpoint, "request")
		}
		if rawNote, present := request["note"]; present {
			if bytes.Equal(bytes.TrimSpace(rawNote), []byte("null")) || json.Unmarshal(rawNote, &note) != nil {
				return nil, remoteBoundaryError(endpoint, "request")
			}
			if strings.TrimSpace(note) == "" {
				return feedbackRejected(map[string]any{"code": "note-blank"}), nil
			}
			if len([]byte(note)) > maxFeedbackNoteBytes {
				return feedbackRejected(map[string]any{"code": "note-too-large", "maxBytes": maxFeedbackNoteBytes, "actualBytes": len([]byte(note))}), nil
			}
		}
		if !bytes.Equal(bytes.TrimSpace(request["ifVersion"]), []byte("null")) {
			value, versionErr := remoteString(endpoint, request, "ifVersion")
			if versionErr != nil {
				return nil, remoteBoundaryError(endpoint, "request")
			}
			requestedVersion = &value
		}
	}
	s, sessionErr := e.getSession(sessionID)
	if sessionErr != nil {
		return feedbackRejected(map[string]any{"code": "session-not-found", "sessionId": sessionID}), nil
	}
	s.mu.Lock()
	header := s.Header
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()

	feedbackFileMu.Lock()
	defer feedbackFileMu.Unlock()
	row, loadErr := e.loadFeedback(sessionID, header)
	if loadErr != nil {
		return nil, rpcError("internal", loadErr.Error(), map[string]any{"endpoint": endpoint})
	}
	if endpoint == "messageFeedback/list" {
		sort.SliceStable(row.Items, func(i, j int) bool { return row.Items[i].CreatedAt < row.Items[j].CreatedAt })
		return feedbackSuccess(map[string]any{"items": row.Items}), nil
	}
	index := -1
	for i := range row.Items {
		if row.Items[i].MessageID == messageID {
			index = i
			break
		}
	}
	if endpoint == "messageFeedback/delete" {
		if index < 0 {
			return feedbackSuccess(map[string]any{"absent": true}), nil
		}
		if row.Items[index].Version != ifVersion {
			return feedbackRejected(feedbackVersionConflict(&row.Items[index])), nil
		}
		row.Items = append(row.Items[:index], row.Items[index+1:]...)
		if saveErr := e.saveFeedback(sessionID, row); saveErr != nil {
			return nil, rpcError("internal", saveErr.Error(), map[string]any{"endpoint": endpoint})
		}
		return feedbackSuccess(map[string]any{"absent": true}), nil
	}

	if !feedbackTarget(events, messageID) {
		return feedbackRejected(map[string]any{"code": "target-not-found", "sessionId": sessionID, "messageId": messageID}), nil
	}
	if index < 0 && requestedVersion != nil || index >= 0 && (requestedVersion == nil || *requestedVersion != row.Items[index].Version) {
		if index < 0 {
			return feedbackRejected(feedbackVersionConflict(nil)), nil
		}
		return feedbackRejected(feedbackVersionConflict(&row.Items[index])), nil
	}
	if index >= 0 && row.Items[index].Rating == rating && row.Items[index].Note == note {
		return feedbackSuccess(row.Items[index]), nil
	}
	now := time.Now().UnixMilli()
	item := feedbackItem{MessageID: messageID, Rating: rating, Note: note, Version: feedbackVersion(), CreatedAt: now, UpdatedAt: now}
	if index < 0 {
		row.Items = append(row.Items, item)
	} else {
		item.CreatedAt = row.Items[index].CreatedAt
		if now < row.Items[index].UpdatedAt {
			item.UpdatedAt = row.Items[index].UpdatedAt
		}
		row.Items[index] = item
	}
	if saveErr := e.saveFeedback(sessionID, row); saveErr != nil {
		return nil, rpcError("internal", saveErr.Error(), map[string]any{"endpoint": endpoint})
	}
	return feedbackSuccess(item), nil
}

func feedbackVersion() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return newID("feedback")
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

func feedbackAllowed(endpoint string) []string {
	switch endpoint {
	case "messageFeedback/list":
		return []string{"sessionId"}
	case "messageFeedback/delete":
		return []string{"sessionId", "messageId", "ifVersion"}
	default:
		return []string{"sessionId", "messageId", "rating", "note", "ifVersion"}
	}
}

func feedbackRequired(endpoint string) []string {
	switch endpoint {
	case "messageFeedback/list":
		return []string{"sessionId"}
	case "messageFeedback/delete":
		return []string{"sessionId", "messageId", "ifVersion"}
	default:
		return []string{"sessionId", "messageId", "rating", "ifVersion"}
	}
}

func feedbackSuccess(value any) map[string]any {
	return map[string]any{"ok": true, "value": value}
}

func feedbackRejected(err any) map[string]any {
	return map[string]any{"ok": false, "error": err}
}

func feedbackVersionConflict(current *feedbackItem) map[string]any {
	var value any
	if current != nil {
		value = *current
	}
	return map[string]any{"code": "version-conflict", "current": value}
}

func feedbackTarget(events []Event, messageID string) bool {
	for _, event := range events {
		if event.Type != "assistant/message" || event.SurfaceOp != "append" {
			continue
		}
		data, ok := event.Data.(map[string]any)
		if !ok {
			continue
		}
		if nested, ok := data["message"].(map[string]any); ok {
			data = nested
		}
		id, _ := data["id"].(string)
		role, _ := data["role"].(string)
		if id == messageID && role == "assistant" {
			return true
		}
	}
	return false
}

func (e *Engine) feedbackPath(sessionID string) string {
	sum := sha256.Sum256([]byte(sessionID))
	return filepath.Join(e.cfg.DataDir, "message-feedback", hex.EncodeToString(sum[:])+".json")
}

func (e *Engine) feedbackStorePath() string {
	return filepath.Join(e.cfg.DataDir, "storages", "message_feedback.json")
}

type feedbackStoreDocument struct {
	Unit   map[string]any                        `json:"unit"`
	Global any                                   `json:"global"`
	Tables map[string]map[string]json.RawMessage `json:"tables"`
}

func (e *Engine) loadFeedback(sessionID string, header SessionHeader) (feedbackRow, error) {
	empty := feedbackRow{Session: feedbackSessionIdentity{CreatedAt: header.CreatedAt, CWD: header.CWD}, Items: []feedbackItem{}}
	data, err := os.ReadFile(e.feedbackStorePath())
	if err == nil {
		var document feedbackStoreDocument
		if decodeErr := json.Unmarshal(data, &document); decodeErr != nil {
			return feedbackRow{}, decodeErr
		}
		if raw := document.Tables["sessions"][sessionID]; len(raw) > 0 {
			var row feedbackRow
			if decodeErr := json.Unmarshal(raw, &row); decodeErr != nil {
				return feedbackRow{}, decodeErr
			}
			if row.Session.CreatedAt == header.CreatedAt && row.Session.CWD == header.CWD {
				if row.Items == nil {
					row.Items = []feedbackItem{}
				}
				return row, nil
			}
		}
	} else if !os.IsNotExist(err) {
		return feedbackRow{}, err
	}
	// Read the pre-storage-domain Go sidecar layout when upgrading an existing
	// development checkout; new writes always use the upstream storage shape.
	legacyData, legacyErr := os.ReadFile(e.feedbackPath(sessionID))
	if legacyErr == nil {
		var row feedbackRow
		if decodeErr := json.Unmarshal(legacyData, &row); decodeErr != nil {
			return feedbackRow{}, decodeErr
		}
		if row.Session.CreatedAt == header.CreatedAt && row.Session.CWD == header.CWD {
			if row.Items == nil {
				row.Items = []feedbackItem{}
			}
			return row, nil
		}
	} else if !os.IsNotExist(legacyErr) {
		return feedbackRow{}, legacyErr
	}
	return empty, nil
}

func (e *Engine) saveFeedback(sessionID string, row feedbackRow) error {
	path := e.feedbackStorePath()
	document := feedbackStoreDocument{
		Unit:   map[string]any{"name": "message_feedback", "version": 0},
		Global: nil,
		Tables: map[string]map[string]json.RawMessage{"sessions": {}},
	}
	if existing, readErr := os.ReadFile(path); readErr == nil {
		if err := json.Unmarshal(existing, &document); err != nil {
			return err
		}
	} else if !os.IsNotExist(readErr) {
		return readErr
	}
	if document.Unit == nil {
		document.Unit = map[string]any{"name": "message_feedback", "version": 0}
	}
	if document.Tables == nil {
		document.Tables = map[string]map[string]json.RawMessage{}
	}
	if document.Tables["sessions"] == nil {
		document.Tables["sessions"] = map[string]json.RawMessage{}
	}
	rowData, err := json.Marshal(row)
	if err != nil {
		return err
	}
	document.Tables["sessions"][sessionID] = rowData
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
