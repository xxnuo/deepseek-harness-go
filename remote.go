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
	"commands/execute":    {allowed: []string{"agentId", "images", "line"}, required: []string{"agentId", "images", "line"}},
	"commands/list":       {allowed: []string{"agentId"}, required: []string{"agentId"}},
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

// remotePluginInventory projects the packages that are actually available to
// the Go web host. The upstream Loader exposes richer Fiber state; boot graph
// entries are the same deployment boundary in the Go port, so report them as
// active host entries and keep the wire shape stable for the original UI.
func (e *Engine) remotePluginInventory() (map[string]any, *RPCError) {
	graph, _, err := e.buildBootGraph()
	if err != nil {
		return nil, rpcError("inventory-unavailable", err.Error(), nil)
	}
	entries := make([]map[string]any, 0, len(graph.Entries))
	for _, entry := range graph.Entries {
		entries = append(entries, map[string]any{
			"entryId": entry.ID, "moduleName": entry.ID, "enabled": true, "fiberPhase": "active",
		})
	}
	return map[string]any{"entries": entries}, nil
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
	if _, sessionErr := e.getSession(id); sessionErr != nil {
		return nil, errorToRPC(sessionErr)
	}
	return commandCatalog(), nil
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
