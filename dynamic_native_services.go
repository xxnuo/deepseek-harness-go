package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dop251/goja"
)

func dynamicCordisNativeService(name string) bool {
	switch name {
	case "agentDefaultModel", "credentials", "goals", "jobs", "messageFeedback", "permissionPresets", "sandboxPolicy", "sessionReferenceResolver", "sessions", "spillStore":
		return true
	default:
		return false
	}
}

func (e *Engine) dynamicCordisNativeServiceValue(run *dynamicCordisRun, name string) goja.Value {
	switch name {
	case "agentDefaultModel":
		return e.dynamicCordisAgentDefaultModelFacade(run)
	case "credentials":
		return e.dynamicCordisCredentialsFacade(run)
	case "goals":
		return e.dynamicCordisGoalsFacade(run)
	case "jobs":
		return e.dynamicCordisJobsFacade(run)
	case "messageFeedback":
		return e.dynamicCordisMessageFeedbackFacade(run)
	case "permissionPresets":
		return e.dynamicCordisPermissionPresetsFacade(run)
	case "sandboxPolicy":
		return e.dynamicCordisSandboxPolicyFacade(run)
	case "sessionReferenceResolver":
		return e.dynamicCordisSessionReferenceFacade(run)
	case "sessions":
		return e.dynamicCordisSessionsFacade(run)
	case "spillStore":
		return e.dynamicCordisSpillStoreFacade(run)
	default:
		return goja.Undefined()
	}
}

func dynamicCordisSessionID(vm *goja.Runtime, value goja.Value) string {
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return ""
	}
	if id, ok := value.Export().(string); ok {
		return strings.TrimSpace(id)
	}
	object, ok := value.(*goja.Object)
	if !ok {
		panic(vm.ToValue("expected an Agent, Session, or session id"))
	}
	for _, key := range []string{"id", "sessionId"} {
		if member := object.Get(key); member != nil && !goja.IsUndefined(member) && !goja.IsNull(member) {
			return strings.TrimSpace(member.String())
		}
	}
	if header, ok := object.Get("header").(*goja.Object); ok {
		if id := header.Get("id"); id != nil && !goja.IsUndefined(id) && !goja.IsNull(id) {
			return strings.TrimSpace(id.String())
		}
	}
	panic(vm.ToValue("expected an Agent or Session with a non-empty id"))
}

func (e *Engine) dynamicCordisAgentDefaultModelFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("currentSelection", func(goja.FunctionCall) goja.Value {
		selection := map[string]any{"provider": e.cfg.Provider, "model": e.cfg.Model}
		e.mu.RLock()
		stored := cloneSettingsValue(e.settings["agent-default-model"])
		e.mu.RUnlock()
		for _, key := range []string{"provider", "model", "reasoningEffort"} {
			if value, ok := stored[key].(string); ok {
				selection[key] = value
			}
		}
		return vm.ToValue(selection)
	})
	_ = service.Set("saveSelection", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			var selection ModelSelection
			if err := dynamicCordisDecode(call.Argument(0), &selection); err != nil {
				panic(vm.ToValue("ctx.agentDefaultModel.saveSelection: " + err.Error()))
			}
			if selection.Provider == "" || selection.Model == "" {
				panic(vm.ToValue("default model provider and model must be non-empty"))
			}
			section := map[string]any{"provider": selection.Provider, "model": selection.Model}
			if selection.ReasoningEffort != "" {
				section["reasoningEffort"] = selection.ReasoningEffort
			}
			if _, rpcErr := e.settingsUpdateFrom(run, "agent-default-model", section, nil, true); rpcErr != nil {
				panic(vm.ToValue(rpcErr.Error()))
			}
			return goja.Undefined()
		})
	})
	return service
}

func (e *Engine) dynamicCordisCredentialsFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("resolve", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			value, source, ok := e.resolveCredential(strings.TrimSpace(call.Argument(0).String()))
			if !ok {
				return goja.Undefined()
			}
			return vm.ToValue(map[string]any{"value": value, "source": source})
		})
	})
	_ = service.Set("describe", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			configured, source, writable := e.credentialInfo(strings.TrimSpace(call.Argument(0).String()))
			info := map[string]any{"configured": configured, "writable": writable}
			if source != "" {
				info["source"] = source
			}
			return vm.ToValue(info)
		})
	})
	_ = service.Set("set", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if err := e.setCredentialFrom(run, strings.TrimSpace(call.Argument(0).String()), call.Argument(1).String()); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return goja.Undefined()
		})
	})
	_ = service.Set("unset", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if err := e.unsetCredentialFrom(run, strings.TrimSpace(call.Argument(0).String())); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return goja.Undefined()
		})
	})
	return service
}

func (e *Engine) dynamicCordisMessageFeedbackFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	for _, method := range []string{"list", "put", "delete"} {
		method := method
		_ = service.Set(method, func(call goja.FunctionCall) goja.Value {
			return e.dynamicCordisAsyncValue(run, func() goja.Value {
				request, err := json.Marshal(call.Argument(0).Export())
				if err != nil {
					panic(vm.ToValue("ctx.messageFeedback." + method + ": request must be JSON"))
				}
				value, rpcErr := e.remoteMessageFeedback("messageFeedback/"+method, map[string]json.RawMessage{"request": request})
				if rpcErr != nil {
					panic(vm.ToValue(rpcErr.Error()))
				}
				return vm.ToValue(cloneJSON(value))
			})
		})
	}
	return service
}

func dynamicCordisPermissionPreset(name string) (commandPermissionPreset, string, bool) {
	spec, ok := commandPermissionPresets[name]
	if !ok {
		return commandPermissionPreset{}, "", false
	}
	for _, candidate := range permissionPresets {
		if candidate.name == name {
			return spec, candidate.spec.description, true
		}
	}
	return spec, "", true
}

func dynamicCordisPermissionOption(name string) (map[string]any, bool) {
	if name == "custom" {
		return map[string]any{
			"value": "custom", "name": "Custom",
			"description": "Current sandbox and approval settings do not match a preset.",
		}, true
	}
	_, description, ok := dynamicCordisPermissionPreset(name)
	if !ok {
		return nil, false
	}
	return map[string]any{"value": name, "name": name, "description": description}, true
}

func (e *Engine) dynamicCordisAppendEvent(run *dynamicCordisRun, session *Session, typ string, data map[string]any) (Event, error) {
	session.mu.Lock()
	event, err := appendEventLocked(session, typ, data, nil, nil, false)
	session.mu.Unlock()
	if err != nil {
		return Event{}, err
	}
	e.publishEventFrom(run, session.Header.ID, event)
	e.observeSessionTitleEvent(session, event)
	return event, nil
}

func (e *Engine) dynamicCordisPermissionPresetsFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("current", func(call goja.FunctionCall) goja.Value {
		var events []Event
		if err := dynamicCordisDecode(call.Argument(0), &events); err != nil {
			panic(vm.ToValue("ctx.permissionPresets.current: " + err.Error()))
		}
		return vm.ToValue(currentPermissions(events)["currentValue"])
	})
	_ = service.Set("selectFor", func(call goja.FunctionCall) goja.Value {
		var state struct {
			Preset   *string `json:"preset"`
			Sandbox  *string `json:"sandbox"`
			Approval *string `json:"approval"`
		}
		if err := dynamicCordisDecode(call.Argument(0), &state); err != nil {
			panic(vm.ToValue("ctx.permissionPresets.selectFor: " + err.Error()))
		}
		events := make([]Event, 0, 3)
		if state.Preset != nil {
			events = append(events, Event{Type: "permission/preset", Data: map[string]any{"preset": *state.Preset}})
		}
		if state.Sandbox != nil {
			events = append(events, Event{Type: "sandbox/mode", Data: map[string]any{"mode": *state.Sandbox}})
		}
		if state.Approval != nil {
			events = append(events, Event{Type: "approval/policy", Data: map[string]any{"policy": *state.Approval}})
		}
		return vm.ToValue(currentPermissions(events))
	})
	_ = service.Set("resolve", func(call goja.FunctionCall) goja.Value {
		name := strings.TrimSpace(call.Argument(0).String())
		spec, description, ok := dynamicCordisPermissionPreset(name)
		if !ok {
			panic(vm.ToValue(fmt.Sprintf("unknown permission preset %q", name)))
		}
		return vm.ToValue(map[string]any{"sandbox": spec.sandbox, "approval": spec.approval, "name": name, "description": description})
	})
	_ = service.Set("optionOf", func(call goja.FunctionCall) goja.Value {
		name := strings.TrimSpace(call.Argument(0).String())
		option, ok := dynamicCordisPermissionOption(name)
		if !ok {
			panic(vm.ToValue(fmt.Sprintf("unknown permission preset %q", name)))
		}
		return vm.ToValue(option)
	})
	_ = service.Set("set", func(call goja.FunctionCall) goja.Value {
		sessionID := dynamicCordisSessionID(vm, call.Argument(0))
		session, err := e.getSession(sessionID)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		name := strings.TrimSpace(call.Argument(1).String())
		spec, ok := commandPermissionPresets[name]
		if !ok {
			panic(vm.ToValue(fmt.Sprintf("unknown permission preset %q", name)))
		}
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		session.mu.Unlock()
		currentSandbox := effectiveEventString(events, "sandbox/mode", "mode", sandboxWorkspaceWrite)
		if currentSandbox != spec.sandbox && e.HasTerminalActivity(sessionID) {
			panic(vm.ToValue(fmt.Sprintf("cannot change sandbox mode from %q to %q while terminal sessions are active", currentSandbox, spec.sandbox)))
		}
		changes := []struct {
			needed bool
			typ    string
			data   map[string]any
		}{
			{currentPermissionPreset(events) != name, "permission/preset", map[string]any{"preset": name}},
			{currentSandbox != spec.sandbox, "sandbox/mode", map[string]any{"mode": spec.sandbox}},
			{effectiveEventString(events, "approval/policy", "policy", "ask") != spec.approval, "approval/policy", map[string]any{"policy": spec.approval}},
		}
		for _, change := range changes {
			if !change.needed {
				continue
			}
			if _, err := e.dynamicCordisAppendEvent(run, session, change.typ, change.data); err != nil {
				panic(vm.ToValue(err.Error()))
			}
		}
		return goja.Undefined()
	})
	return service
}

func (e *Engine) dynamicCordisSandboxPolicyFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	defaultPreset := permissionBaseSettings()["defaultPreset"].(string)
	defaultMode := commandPermissionPresets[defaultPreset].sandbox
	_ = service.Set("defaultMode", defaultMode)
	_ = service.Set("workspaceRoot", e.cfg.Workspace)
	_ = service.Set("resolve", func(call goja.FunctionCall) goja.Value {
		request := call.Argument(0)
		sessionID, mode := "", ""
		if request != nil && !goja.IsUndefined(request) && !goja.IsNull(request) {
			object, ok := request.(*goja.Object)
			if !ok {
				panic(vm.ToValue("ctx.sandboxPolicy.resolve request must be an object"))
			}
			if session := object.Get("session"); session != nil && !goja.IsUndefined(session) && !goja.IsNull(session) {
				sessionID = dynamicCordisSessionID(vm, session)
			}
			if requested := object.Get("mode"); requested != nil && !goja.IsUndefined(requested) && !goja.IsNull(requested) {
				mode = requested.String()
				if _, ok := sandboxPresetModes[mode]; !ok {
					panic(vm.ToValue(fmt.Sprintf("unsupported sandbox mode %q", mode)))
				}
			}
		}
		workspaceRoot := e.cfg.Workspace
		if sessionID != "" {
			session, err := e.getSession(sessionID)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			session.mu.Lock()
			workspaceRoot = session.Header.CWD
			session.mu.Unlock()
		}
		if mode == "" {
			var err error
			mode, err = e.sandboxModeForCall(ToolCall{SessionID: sessionID})
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
		}
		policy := map[string]any{"mode": mode, "workspaceRoot": workspaceRoot}
		if sessionID != "" {
			policy["sessionId"] = sessionID
		}
		return vm.ToValue(policy)
	})
	_ = service.Set("overrideOf", func(call goja.FunctionCall) goja.Value {
		sessionID := dynamicCordisSessionID(vm, call.Argument(0))
		session, err := e.getSession(sessionID)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		session.mu.Unlock()
		mode := ""
		for _, event := range events {
			if event.Type != "sandbox/mode" {
				continue
			}
			data, _ := event.Data.(map[string]any)
			if next, ok := data["mode"].(string); ok {
				if _, valid := sandboxPresetModes[next]; valid {
					mode = next
				}
			}
		}
		if mode == "" {
			return goja.Undefined()
		}
		return vm.ToValue(mode)
	})
	return service
}

func (e *Engine) dynamicCordisSessionReferenceFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("listCandidates", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			targetID := dynamicCordisSessionID(vm, call.Argument(0))
			query := ""
			if value := call.Argument(1); value != nil && !goja.IsUndefined(value) && !goja.IsNull(value) {
				query = value.String()
			}
			limit := 0
			if value := call.Argument(2); value != nil && !goja.IsUndefined(value) && !goja.IsNull(value) {
				limit = int(value.ToInteger())
			}
			rows, err := e.ListSessionReferenceCandidates(context.Background(), targetID, query, limit, e.cfg.SessionReference)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return vm.ToValue(cloneJSON(rows))
		})
	})
	_ = service.Set("prepare", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			targetID := dynamicCordisSessionID(vm, call.Argument(0))
			var content []ContentBlock
			if err := dynamicCordisDecode(call.Argument(1), &content); err != nil {
				panic(vm.ToValue("ctx.sessionReferenceResolver.prepare content: " + err.Error()))
			}
			var references []SessionReferenceInput
			if err := dynamicCordisDecode(call.Argument(2), &references); err != nil {
				panic(vm.ToValue("ctx.sessionReferenceResolver.prepare references: " + err.Error()))
			}
			prepared, err := e.PrepareSessionReferences(context.Background(), targetID, content, references, e.cfg.SessionReference)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return vm.ToValue(cloneJSON(prepared))
		})
	})
	return service
}

func (e *Engine) dynamicCordisSpillStoreFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("saveText", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			var input struct {
				Owner struct {
					SessionID string `json:"sessionId"`
				} `json:"owner"`
				Source        map[string]any `json:"source"`
				SuggestedName string         `json:"suggestedName"`
				Content       string         `json:"content"`
			}
			if err := dynamicCordisDecode(call.Argument(0), &input); err != nil {
				panic(vm.ToValue("ctx.spillStore.saveText: " + err.Error()))
			}
			session, err := e.getSession(input.Owner.SessionID)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			path, err := e.saveSpillText(session, input.SuggestedName, input.Content)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return vm.ToValue(map[string]any{
				"locator":       path,
				"bytes":         len([]byte(input.Content)),
				"retrievalHint": "Use read with offset/limit, or grep this path to search within it.",
			})
		})
	})
	return service
}
