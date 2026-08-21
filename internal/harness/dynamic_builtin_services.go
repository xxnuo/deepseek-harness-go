package harness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/dop251/goja"
)

type dynamicPromptSection struct {
	run      *dynamicCordisRun
	session  string
	name     string
	order    float64
	text     string
	provider goja.Callable
	complete bool
	sequence uint64
}

type dynamicPromptProvider struct {
	run      *dynamicCordisRun
	session  string
	provider goja.Callable
	sequence uint64
}

type dynamicPromptVariable struct {
	run      *dynamicCordisRun
	session  string
	name     string
	provider goja.Callable
}

type resolvedPromptSection struct {
	Name     string  `json:"name"`
	Order    float64 `json:"-"`
	Text     string  `json:"text"`
	Complete bool    `json:"-"`
}

type resolvedPromptAssembly struct {
	Sections  []resolvedPromptSection `json:"sections"`
	Contexts  []resolvedPromptSection `json:"contexts"`
	Tools     []ToolSchema            `json:"tools"`
	Variables map[string]any          `json:"variables"`
}

var dynamicPromptRegistry = struct {
	sync.RWMutex
	sections    map[*Engine]map[string]*dynamicPromptSection
	contexts    map[*Engine]map[string]*dynamicPromptSection
	variables   map[*Engine]map[string]*dynamicPromptVariable
	tools       map[*Engine]map[*dynamicPromptProvider]struct{}
	suppressors map[*Engine]map[*dynamicPromptProvider]struct{}
	next        uint64
}{
	sections:    map[*Engine]map[string]*dynamicPromptSection{},
	contexts:    map[*Engine]map[string]*dynamicPromptSection{},
	variables:   map[*Engine]map[string]*dynamicPromptVariable{},
	tools:       map[*Engine]map[*dynamicPromptProvider]struct{}{},
	suppressors: map[*Engine]map[*dynamicPromptProvider]struct{}{},
}

var dynamicPromptVariableName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func (e *Engine) dynamicCordisBuiltinServiceValue(run *dynamicCordisRun, name string) goja.Value {
	switch name {
	case "systemPrompt":
		return e.dynamicCordisSystemPromptFacade(run)
	case "fs":
		return e.dynamicCordisFSFacade(run)
	case "shell":
		return e.dynamicCordisShellFacade(run)
	default:
		return run.runtime.NewObject()
	}
}

func dynamicPromptRegistryKey(session, name string) string { return session + "\x00" + name }

func dynamicPromptTextEntry(vm *goja.Runtime, run *dynamicCordisRun, kind string, value goja.Value) (*dynamicPromptSection, error) {
	object, ok := value.(*goja.Object)
	if !ok {
		return nil, fmt.Errorf("ctx.systemPrompt.%s requires an object", kind)
	}
	name, order, text := object.Get("name"), object.Get("order"), object.Get("text")
	if name == nil || order == nil {
		return nil, fmt.Errorf("prompt %s requires name and order", kind)
	}
	entry := &dynamicPromptSection{
		run: run, session: run.ownerSessionID(), name: strings.TrimSpace(name.String()), order: order.ToFloat(),
	}
	if entry.name == "" {
		return nil, fmt.Errorf("prompt %s name must be non-empty", kind)
	}
	if math.IsNaN(entry.order) || math.IsInf(entry.order, 0) {
		return nil, fmt.Errorf("prompt %s %q order must be a finite number", kind, entry.name)
	}
	if provider, ok := goja.AssertFunction(text); ok {
		entry.provider = provider
	} else if text == nil || goja.IsUndefined(text) || goja.IsNull(text) {
		return nil, fmt.Errorf("prompt %s %q text must be a string or function", kind, entry.name)
	} else {
		entry.text = text.String()
	}
	if kind == "section" {
		if complete := object.Get("complete"); complete != nil && !goja.IsUndefined(complete) && !goja.IsNull(complete) {
			entry.complete = complete.ToBoolean()
		}
	}
	return entry, nil
}

func dynamicPromptInvoke(owner, caller *dynamicCordisRun, provider goja.Callable, context any) (goja.Value, bool, error) {
	_ = caller
	owner.mu.Lock()
	active := !owner.disposed && (owner.active || owner.activating)
	owner.mu.Unlock()
	if !active {
		return goja.Undefined(), false, nil
	}
	if provider == nil {
		return goja.Undefined(), true, nil
	}
	value, err := provider(goja.Undefined(), owner.runtime.ToValue(context))
	if err != nil {
		return nil, true, fmt.Errorf("%s", dynamicJSMessage(err))
	}
	return value, true, nil
}

func (e *Engine) dynamicPromptDisposers(run *dynamicCordisRun, remove func()) (func(), func()) {
	var once sync.Once
	take := func() bool {
		removed := false
		once.Do(func() {
			remove()
			removed = true
		})
		return removed
	}
	host := func() {
		if take() {
			if err := e.emitDynamicCordisEventFrom(run, "system-prompt/change"); err != nil {
				panic(run.runtime.ToValue(err.Error()))
			}
		}
	}
	cleanup := func() {
		if take() {
			_ = e.emitDynamicCordisEventFrom(run, "system-prompt/change")
		}
	}
	return host, cleanup
}

func (e *Engine) dynamicPromptAssemblyListeners(session string) []*dynamicCordisListener {
	e.dynamicCordis.RLock()
	listeners := make([]*dynamicCordisListener, 0)
	seen := map[*dynamicCordisRun]struct{}{}
	for _, plugin := range e.dynamicCordis.plugins {
		run := plugin.run
		if run == nil || !run.appliesToSession(session) {
			continue
		}
		if _, ok := seen[run]; ok {
			continue
		}
		seen[run] = struct{}{}
		run.eventMu.RLock()
		listeners = append(listeners, run.listeners["system-prompt/assemble"]...)
		run.eventMu.RUnlock()
	}
	e.dynamicCordis.RUnlock()
	sort.SliceStable(listeners, func(i, j int) bool { return dynamicCordisListenerLess(listeners[i], listeners[j]) })
	return listeners
}

func dynamicPromptJSValue(vm *goja.Runtime, value any) (goja.Value, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	parse, ok := goja.AssertFunction(vm.Get("JSON").ToObject(vm).Get("parse"))
	if !ok {
		return nil, errors.New("dynamic JavaScript JSON.parse is unavailable")
	}
	return parse(vm.Get("JSON"), vm.ToValue(string(data)))
}

func (e *Engine) dynamicPromptWaterfall(run *dynamicCordisRun, session string, assembly, context map[string]any, contextValue goja.Value, complete *resolvedPromptSection, suppressed bool) goja.Value {
	vm := run.runtime
	promise, resolve, reject := vm.NewPromise()
	finish := func(value map[string]any, err error) {
		if err != nil {
			_ = reject(vm.NewGoError(err))
			return
		}
		_ = resolve(vm.ToValue(value))
	}
	if !e.dynamicCordis.loop.post(func() {
		e.dynamicPromptWaterfallDispatch(session, run, assembly, context, contextValue, complete, suppressed, finish)
	}) {
		_ = reject(vm.NewGoError(errors.New("dynamic Cordis runtime is closed")))
	}
	return vm.ToValue(promise)
}

func dynamicPromptAssemblySessionID(run *dynamicCordisRun, vm *goja.Runtime, value goja.Value) string {
	if !run.global {
		return run.sessionID
	}
	object, ok := value.(*goja.Object)
	if !ok {
		panic(vm.ToValue("ctx.systemPrompt.assemble requires context.scope or context.agent in global scope"))
	}
	target := ""
	for _, key := range []string{"scope", "agent"} {
		member := object.Get(key)
		if member == nil || goja.IsUndefined(member) || goja.IsNull(member) {
			continue
		}
		id := dynamicCordisSessionID(vm, member)
		if target != "" && id != target {
			panic(vm.ToValue("ctx.systemPrompt.assemble context.scope and context.agent must identify the same session"))
		}
		target = id
	}
	if target == "" {
		panic(vm.ToValue("ctx.systemPrompt.assemble requires context.scope or context.agent in global scope"))
	}
	return target
}

func (e *Engine) dynamicPromptWaterfallDispatch(session string, caller *dynamicCordisRun, assembly, context map[string]any, contextValue goja.Value, complete *resolvedPromptSection, suppressed bool, finish func(map[string]any, error)) {
	listeners := e.dynamicPromptAssemblyListeners(session)
	var dispatch func(int, any, func(any, error))
	dispatch = func(index int, current any, done func(any, error)) {
		if index == len(listeners) {
			done(current, nil)
			return
		}
		listener := listeners[index]
		owner := listener.run
		if listener.once {
			owner.eventMu.Lock()
			for position, candidate := range owner.listeners["system-prompt/assemble"] {
				if candidate == listener {
					remaining := owner.listeners["system-prompt/assemble"]
					remaining = append(remaining[:position], remaining[position+1:]...)
					if len(remaining) == 0 {
						delete(owner.listeners, "system-prompt/assemble")
					} else {
						owner.listeners["system-prompt/assemble"] = remaining
					}
					break
				}
			}
			owner.eventMu.Unlock()
		}
		owner.mu.Lock()
		active := !owner.disposed && (owner.active || owner.activating)
		owner.mu.Unlock()
		if !active {
			dispatch(index+1, current, done)
			return
		}

		ownerVM := owner.runtime
		assemblyValue, err := dynamicPromptJSValue(ownerVM, current)
		if err != nil {
			done(nil, err)
			return
		}
		var nextPromise *goja.Promise
		next := ownerVM.ToValue(func(goja.FunctionCall) goja.Value {
			if nextPromise != nil {
				return ownerVM.ToValue(nextPromise)
			}
			downstream, downstreamResolve, downstreamReject := ownerVM.NewPromise()
			nextPromise = downstream
			nextAssembly, err := dynamicCordisJSONValue(assemblyValue)
			if err != nil {
				_ = downstreamReject(ownerVM.NewGoError(err))
				return ownerVM.ToValue(downstream)
			}
			if !e.dynamicCordis.loop.post(func() {
				dispatch(index+1, nextAssembly, func(value any, err error) {
					if err != nil {
						_ = downstreamReject(ownerVM.NewGoError(err))
						return
					}
					resolved, err := dynamicPromptJSValue(ownerVM, value)
					if err != nil {
						_ = downstreamReject(ownerVM.NewGoError(err))
						return
					}
					_ = downstreamResolve(resolved)
				})
			}) {
				_ = downstreamReject(ownerVM.NewGoError(errors.New("dynamic Cordis runtime is closed")))
			}
			return ownerVM.ToValue(downstream)
		})
		listenerContext, err := dynamicPromptJSValue(ownerVM, context)
		if err != nil {
			done(nil, err)
			return
		}
		if owner == caller && contextValue != nil {
			listenerContext = contextValue
		}
		value, err := listener.fn(goja.Undefined(), assemblyValue, listenerContext, next)
		if err != nil {
			e.reportDynamicCordisHostFailure(owner, "event system-prompt/assemble", err)
			done(nil, errors.New(dynamicJSMessage(err)))
			return
		}
		dynamicCordisAwaitOnLoop(owner, value, func(value goja.Value, err error) {
			if err != nil {
				e.reportDynamicCordisHostFailure(owner, "event system-prompt/assemble", err)
				done(nil, err)
				return
			}
			canonical, err := dynamicCordisJSONValue(value)
			if err != nil {
				done(nil, err)
				return
			}
			done(canonical, nil)
		})
	}
	completeDispatch := func(value any, err error) {
		if err != nil {
			finish(nil, err)
			return
		}
		transformed, ok := value.(map[string]any)
		if !ok {
			finish(nil, errors.New("system-prompt/assemble listener must return a PromptAssembly object"))
			return
		}
		if complete != nil {
			transformed["sections"] = []any{map[string]any{"name": complete.Name, "text": complete.Text}}
		}
		if suppressed {
			transformed["contexts"] = []any{}
		}
		finish(transformed, nil)
	}
	dispatch(0, cloneJSON(assembly), completeDispatch)
}

func (e *Engine) dynamicPromptWaterfallForSession(ctx context.Context, session string, assembly, assemblyContext map[string]any, complete *resolvedPromptSection, suppressed bool) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type outcome struct {
		value map[string]any
		err   error
	}
	done := make(chan outcome, 1)
	if !e.dynamicCordis.loop.post(func() {
		e.dynamicPromptWaterfallDispatch(session, nil, assembly, assemblyContext, nil, complete, suppressed, func(value map[string]any, err error) {
			done <- outcome{value: value, err: err}
		})
	}) {
		return nil, errors.New("dynamic Cordis runtime is closed")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-done:
		return result.value, result.err
	}
}

func (e *Engine) resolvedPromptAssemblyForSession(session *Session, selection ModelSelection, agent agentRuntime, caller *dynamicCordisRun, assemblyContext map[string]any) (resolvedPromptAssembly, *resolvedPromptSection, bool, error) {
	sections, variables, err := e.resolvedSystemPromptAssembly(session, selection, agent, caller, assemblyContext)
	if err != nil {
		return resolvedPromptAssembly{}, nil, false, err
	}
	tools, err := e.toolsForSession(session)
	if err != nil {
		return resolvedPromptAssembly{}, nil, false, err
	}
	contexts, suppressed, err := e.dynamicPromptContexts(session.Header.ID, caller, assemblyContext)
	if err != nil {
		return resolvedPromptAssembly{}, nil, false, err
	}
	providedTools, err := e.dynamicPromptTools(session.Header.ID, caller, assemblyContext)
	if err != nil {
		return resolvedPromptAssembly{}, nil, false, err
	}
	tools = append(tools, providedTools...)
	sort.SliceStable(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })
	var complete *resolvedPromptSection
	for _, section := range sections {
		if section.Complete {
			copy := section
			complete = &copy
			break
		}
	}
	if suppressed {
		contexts = []resolvedPromptSection{}
	}
	return resolvedPromptAssembly{Sections: sections, Contexts: contexts, Tools: tools, Variables: variables}, complete, suppressed, nil
}

func projectResolvedPromptAssembly(assembly resolvedPromptAssembly) map[string]any {
	sections := make([]map[string]any, len(assembly.Sections))
	for index, section := range assembly.Sections {
		sections[index] = map[string]any{"name": section.Name, "text": section.Text}
	}
	contexts := make([]map[string]any, len(assembly.Contexts))
	for index, context := range assembly.Contexts {
		contexts[index] = map[string]any{"name": context.Name, "text": context.Text}
	}
	tools := assembly.Tools
	if tools == nil {
		tools = []ToolSchema{}
	}
	return map[string]any{
		"sections": sections, "contexts": contexts, "tools": tools, "variables": assembly.Variables,
	}
}

func decodeResolvedPromptAssembly(value map[string]any) (resolvedPromptAssembly, error) {
	for _, name := range []string{"sections", "contexts", "tools"} {
		if _, ok := value[name].([]any); !ok {
			return resolvedPromptAssembly{}, fmt.Errorf("system-prompt/assemble returned invalid %s", name)
		}
	}
	if _, ok := value["variables"].(map[string]any); !ok {
		return resolvedPromptAssembly{}, errors.New("system-prompt/assemble returned invalid variables")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return resolvedPromptAssembly{}, err
	}
	var assembly resolvedPromptAssembly
	if err := json.Unmarshal(data, &assembly); err != nil {
		return resolvedPromptAssembly{}, err
	}
	for index := range assembly.Tools {
		if assembly.Tools[index].Parameters == nil {
			assembly.Tools[index].Parameters = map[string]any{}
		}
	}
	return assembly, nil
}

func (e *Engine) dynamicCordisSystemPromptFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("section", func(call goja.FunctionCall) goja.Value {
		section, err := dynamicPromptTextEntry(vm, run, "section", call.Argument(0))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}

		key := dynamicPromptRegistryKey(section.session, section.name)
		dynamicPromptRegistry.Lock()
		entries := dynamicPromptRegistry.sections[e]
		if entries == nil {
			entries = map[string]*dynamicPromptSection{}
			dynamicPromptRegistry.sections[e] = entries
		}
		if entries[key] != nil {
			dynamicPromptRegistry.Unlock()
			panic(vm.ToValue(fmt.Sprintf("prompt section %q is already registered in this scope", section.name)))
		}
		dynamicPromptRegistry.next++
		section.sequence = dynamicPromptRegistry.next
		entries[key] = section
		dynamicPromptRegistry.Unlock()

		remove := func() {
			dynamicPromptRegistry.Lock()
			if entries := dynamicPromptRegistry.sections[e]; entries != nil && entries[key] == section {
				delete(entries, key)
				if len(entries) == 0 {
					delete(dynamicPromptRegistry.sections, e)
				}
			}
			dynamicPromptRegistry.Unlock()
		}
		if err := e.emitDynamicCordisEventFrom(run, "system-prompt/change"); err != nil {
			remove()
			panic(vm.ToValue(err.Error()))
		}
		dispose, cleanup := e.dynamicPromptDisposers(run, remove)
		run.disposers = append(run.disposers, cleanup)
		return vm.ToValue(dispose)
	})
	_ = service.Set("context", func(call goja.FunctionCall) goja.Value {
		entry, err := dynamicPromptTextEntry(vm, run, "context", call.Argument(0))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		key := dynamicPromptRegistryKey(entry.session, entry.name)
		dynamicPromptRegistry.Lock()
		entries := dynamicPromptRegistry.contexts[e]
		if entries == nil {
			entries = map[string]*dynamicPromptSection{}
			dynamicPromptRegistry.contexts[e] = entries
		}
		if entries[key] != nil {
			dynamicPromptRegistry.Unlock()
			panic(vm.ToValue(fmt.Sprintf("prompt context %q is already registered in this scope", entry.name)))
		}
		dynamicPromptRegistry.next++
		entry.sequence = dynamicPromptRegistry.next
		entries[key] = entry
		dynamicPromptRegistry.Unlock()
		remove := func() {
			dynamicPromptRegistry.Lock()
			if entries := dynamicPromptRegistry.contexts[e]; entries != nil && entries[key] == entry {
				delete(entries, key)
				if len(entries) == 0 {
					delete(dynamicPromptRegistry.contexts, e)
				}
			}
			dynamicPromptRegistry.Unlock()
		}
		if err := e.emitDynamicCordisEventFrom(run, "system-prompt/change"); err != nil {
			remove()
			panic(vm.ToValue(err.Error()))
		}
		dispose, cleanup := e.dynamicPromptDisposers(run, remove)
		run.disposers = append(run.disposers, cleanup)
		return vm.ToValue(dispose)
	})
	_ = service.Set("variable", func(call goja.FunctionCall) goja.Value {
		name := call.Argument(0).String()
		if !dynamicPromptVariableName.MatchString(name) {
			panic(vm.ToValue(fmt.Sprintf("invalid prompt variable name %q (must match %s)", name, dynamicPromptVariableName)))
		}
		provider, ok := goja.AssertFunction(call.Argument(1))
		if !ok {
			panic(vm.ToValue("ctx.systemPrompt.variable provider must be a function"))
		}
		entry := &dynamicPromptVariable{run: run, session: run.ownerSessionID(), name: name, provider: provider}
		key := dynamicPromptRegistryKey(entry.session, name)
		dynamicPromptRegistry.Lock()
		entries := dynamicPromptRegistry.variables[e]
		if entries == nil {
			entries = map[string]*dynamicPromptVariable{}
			dynamicPromptRegistry.variables[e] = entries
		}
		if entries[key] != nil {
			dynamicPromptRegistry.Unlock()
			panic(vm.ToValue(fmt.Sprintf("prompt variable %q is already registered in this scope", name)))
		}
		entries[key] = entry
		dynamicPromptRegistry.Unlock()
		remove := func() {
			dynamicPromptRegistry.Lock()
			if entries := dynamicPromptRegistry.variables[e]; entries != nil && entries[key] == entry {
				delete(entries, key)
				if len(entries) == 0 {
					delete(dynamicPromptRegistry.variables, e)
				}
			}
			dynamicPromptRegistry.Unlock()
		}
		if err := e.emitDynamicCordisEventFrom(run, "system-prompt/change"); err != nil {
			remove()
			panic(vm.ToValue(err.Error()))
		}
		dispose, cleanup := e.dynamicPromptDisposers(run, remove)
		run.disposers = append(run.disposers, cleanup)
		return vm.ToValue(dispose)
	})
	_ = service.Set("tools", func(call goja.FunctionCall) goja.Value {
		provider, ok := goja.AssertFunction(call.Argument(0))
		if !ok {
			panic(vm.ToValue("ctx.systemPrompt.tools provider must be a function"))
		}
		entry := &dynamicPromptProvider{run: run, session: run.ownerSessionID(), provider: provider}
		dynamicPromptRegistry.Lock()
		entries := dynamicPromptRegistry.tools[e]
		if entries == nil {
			entries = map[*dynamicPromptProvider]struct{}{}
			dynamicPromptRegistry.tools[e] = entries
		}
		dynamicPromptRegistry.next++
		entry.sequence = dynamicPromptRegistry.next
		entries[entry] = struct{}{}
		dynamicPromptRegistry.Unlock()
		remove := func() {
			dynamicPromptRegistry.Lock()
			if entries := dynamicPromptRegistry.tools[e]; entries != nil {
				delete(entries, entry)
				if len(entries) == 0 {
					delete(dynamicPromptRegistry.tools, e)
				}
			}
			dynamicPromptRegistry.Unlock()
		}
		if err := e.emitDynamicCordisEventFrom(run, "system-prompt/change"); err != nil {
			remove()
			panic(vm.ToValue(err.Error()))
		}
		dispose, cleanup := e.dynamicPromptDisposers(run, remove)
		run.disposers = append(run.disposers, cleanup)
		return vm.ToValue(dispose)
	})
	_ = service.Set("suppressRuntimeContext", func(goja.FunctionCall) goja.Value {
		entry := &dynamicPromptProvider{run: run, session: run.ownerSessionID()}
		dynamicPromptRegistry.Lock()
		entries := dynamicPromptRegistry.suppressors[e]
		if entries == nil {
			entries = map[*dynamicPromptProvider]struct{}{}
			dynamicPromptRegistry.suppressors[e] = entries
		}
		dynamicPromptRegistry.next++
		entry.sequence = dynamicPromptRegistry.next
		entries[entry] = struct{}{}
		dynamicPromptRegistry.Unlock()
		remove := func() {
			dynamicPromptRegistry.Lock()
			if entries := dynamicPromptRegistry.suppressors[e]; entries != nil {
				delete(entries, entry)
				if len(entries) == 0 {
					delete(dynamicPromptRegistry.suppressors, e)
				}
			}
			dynamicPromptRegistry.Unlock()
		}
		if err := e.emitDynamicCordisEventFrom(run, "system-prompt/change"); err != nil {
			remove()
			panic(vm.ToValue(err.Error()))
		}
		dispose, cleanup := e.dynamicPromptDisposers(run, remove)
		run.disposers = append(run.disposers, cleanup)
		return vm.ToValue(dispose)
	})
	_ = service.Set("assemble", func(call goja.FunctionCall) goja.Value {
		assemblyContext := map[string]any{}
		assemblyContextValue := call.Argument(0)
		if assemblyContextValue == nil || goja.IsUndefined(assemblyContextValue) || goja.IsNull(assemblyContextValue) {
			assemblyContextValue = vm.NewObject()
		} else {
			exported, ok := assemblyContextValue.Export().(map[string]any)
			if !ok {
				panic(vm.ToValue("ctx.systemPrompt.assemble context must be an object"))
			}
			for _, key := range []string{"scope", "agent", "signal"} {
				delete(exported, key)
			}
			assemblyContext, ok = cloneJSON(exported).(map[string]any)
			if !ok {
				panic(vm.ToValue("ctx.systemPrompt.assemble context must contain JSON-compatible custom fields"))
			}
		}
		sessionID := dynamicPromptAssemblySessionID(run, vm, assemblyContextValue)
		session, err := e.getSession(sessionID)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		session.mu.Lock()
		selection := session.Model
		session.mu.Unlock()
		agent, err := e.runtimeForSession(session)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		assembly, complete, suppressed, err := e.resolvedPromptAssemblyForSession(session, selection, agent, run, assemblyContext)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return e.dynamicPromptWaterfall(run, sessionID, projectResolvedPromptAssembly(assembly), assemblyContext, assemblyContextValue, complete, suppressed)
	})
	return service
}

func (e *Engine) dynamicPromptContexts(session string, caller *dynamicCordisRun, context any) ([]resolvedPromptSection, bool, error) {
	if caller != nil {
		return e.dynamicPromptContextsNow(session, caller, context)
	}
	var sections []resolvedPromptSection
	var suppressed bool
	var err error
	if !e.dynamicCordis.loop.call(func() { sections, suppressed, err = e.dynamicPromptContextsNow(session, nil, context) }) {
		return nil, false, errors.New("dynamic Cordis runtime is closed")
	}
	return sections, suppressed, err
}

func (e *Engine) dynamicPromptContextsNow(session string, caller *dynamicCordisRun, context any) ([]resolvedPromptSection, bool, error) {
	dynamicPromptRegistry.RLock()
	registered := make([]*dynamicPromptSection, 0)
	for _, entry := range dynamicPromptRegistry.contexts[e] {
		if entry.session == "" || entry.session == session {
			registered = append(registered, entry)
		}
	}
	suppressors := make([]*dynamicPromptProvider, 0)
	for entry := range dynamicPromptRegistry.suppressors[e] {
		if entry.session == "" || entry.session == session {
			suppressors = append(suppressors, entry)
		}
	}
	dynamicPromptRegistry.RUnlock()
	sort.SliceStable(registered, func(i, j int) bool {
		if registered[i].order == registered[j].order {
			return registered[i].sequence < registered[j].sequence
		}
		return registered[i].order < registered[j].order
	})
	for _, entry := range suppressors {
		if _, active, _ := dynamicPromptInvoke(entry.run, caller, nil, nil); active {
			return nil, true, nil
		}
	}
	resolved := make([]resolvedPromptSection, 0, len(registered))
	for _, entry := range registered {
		text := entry.text
		value, active, err := dynamicPromptInvoke(entry.run, caller, entry.provider, context)
		if err != nil {
			return nil, false, fmt.Errorf("prompt context %q: %w", entry.name, err)
		}
		if !active {
			continue
		}
		if entry.provider != nil {
			text = value.String()
		}
		resolved = append(resolved, resolvedPromptSection{Name: entry.name, Order: entry.order, Text: text})
	}
	return resolved, false, nil
}

func (e *Engine) dynamicPromptVariables(session string, caller *dynamicCordisRun, context any) (map[string]any, error) {
	if caller != nil {
		return e.dynamicPromptVariablesNow(session, caller, context)
	}
	var variables map[string]any
	var err error
	if !e.dynamicCordis.loop.call(func() { variables, err = e.dynamicPromptVariablesNow(session, nil, context) }) {
		return nil, errors.New("dynamic Cordis runtime is closed")
	}
	return variables, err
}

func (e *Engine) dynamicPromptVariablesNow(session string, caller *dynamicCordisRun, context any) (map[string]any, error) {
	dynamicPromptRegistry.RLock()
	registered := make([]*dynamicPromptVariable, 0)
	for _, entry := range dynamicPromptRegistry.variables[e] {
		if entry.session == "" || entry.session == session {
			registered = append(registered, entry)
		}
	}
	dynamicPromptRegistry.RUnlock()
	variables := make(map[string]any, len(registered))
	for _, entry := range registered {
		value, active, err := dynamicPromptInvoke(entry.run, caller, entry.provider, context)
		if err != nil {
			return nil, fmt.Errorf("prompt variable %q: %w", entry.name, err)
		}
		if !active {
			continue
		}
		if goja.IsUndefined(value) {
			variables[entry.name] = goja.Undefined()
		} else {
			variables[entry.name] = value.String()
		}
	}
	return variables, nil
}

func (e *Engine) dynamicPromptTools(session string, caller *dynamicCordisRun, context any) ([]ToolSchema, error) {
	if caller != nil {
		return e.dynamicPromptToolsNow(session, caller, context)
	}
	var tools []ToolSchema
	var err error
	if !e.dynamicCordis.loop.call(func() { tools, err = e.dynamicPromptToolsNow(session, nil, context) }) {
		return nil, errors.New("dynamic Cordis runtime is closed")
	}
	return tools, err
}

func (e *Engine) dynamicPromptToolsNow(session string, caller *dynamicCordisRun, context any) ([]ToolSchema, error) {
	dynamicPromptRegistry.RLock()
	registered := make([]*dynamicPromptProvider, 0)
	for entry := range dynamicPromptRegistry.tools[e] {
		if entry.session == "" || entry.session == session {
			registered = append(registered, entry)
		}
	}
	dynamicPromptRegistry.RUnlock()
	sort.Slice(registered, func(i, j int) bool { return registered[i].sequence < registered[j].sequence })
	tools := make([]ToolSchema, 0)
	for _, entry := range registered {
		value, active, err := dynamicPromptInvoke(entry.run, caller, entry.provider, context)
		if err != nil {
			return nil, fmt.Errorf("prompt tool provider: %w", err)
		}
		if !active {
			continue
		}
		var result struct {
			Schemas    []ToolSchema `json:"schemas"`
			KnownNames []string     `json:"knownNames,omitempty"`
		}
		if err := dynamicCordisDecode(value, &result); err != nil {
			return nil, fmt.Errorf("prompt tool provider result: %w", err)
		}
		for _, schema := range result.Schemas {
			if schema.Name == "<unlisted-tools>" {
				return nil, errors.New(`prompt tool provider returned reserved tool name "<unlisted-tools>"`)
			}
			if strings.TrimSpace(schema.Name) == "" {
				return nil, errors.New("prompt tool provider returned an empty tool name")
			}
			if schema.Parameters == nil {
				schema.Parameters = map[string]any{}
			}
			tools = append(tools, schema)
		}
	}
	return tools, nil
}

func (e *Engine) dynamicPromptSections(session string, caller *dynamicCordisRun, context any) ([]resolvedPromptSection, error) {
	if caller != nil {
		return e.dynamicPromptSectionsNow(session, caller, context)
	}
	var sections []resolvedPromptSection
	var err error
	if !e.dynamicCordis.loop.call(func() { sections, err = e.dynamicPromptSectionsNow(session, nil, context) }) {
		return nil, errors.New("dynamic Cordis runtime is closed")
	}
	return sections, err
}

func (e *Engine) dynamicPromptSectionsNow(session string, caller *dynamicCordisRun, context any) ([]resolvedPromptSection, error) {
	dynamicPromptRegistry.RLock()
	registered := make([]*dynamicPromptSection, 0)
	for _, section := range dynamicPromptRegistry.sections[e] {
		if section.session == "" || section.session == session {
			registered = append(registered, section)
		}
	}
	dynamicPromptRegistry.RUnlock()
	sort.Slice(registered, func(i, j int) bool {
		if registered[i].order == registered[j].order {
			return registered[i].sequence < registered[j].sequence
		}
		return registered[i].order < registered[j].order
	})

	resolved := make([]resolvedPromptSection, 0, len(registered))
	for _, section := range registered {
		text := section.text
		value, active, err := dynamicPromptInvoke(section.run, caller, section.provider, context)
		if err != nil {
			return nil, fmt.Errorf("prompt section %q: %w", section.name, err)
		}
		if !active {
			continue
		}
		if section.provider != nil {
			text = value.String()
		}
		resolved = append(resolved, resolvedPromptSection{Name: section.name, Order: section.order, Text: text, Complete: section.complete})
	}
	return resolved, nil
}

type dynamicFSTarget struct {
	TargetKey   string `json:"targetKey"`
	DisplayPath string `json:"displayPath"`
}

func (e *Engine) dynamicCordisWorkspace(run *dynamicCordisRun) (string, error) {
	if run.global {
		return e.cfg.Workspace, nil
	}
	session, err := e.getSession(run.sessionID)
	if err != nil {
		return "", err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.Header.CWD, nil
}

func (e *Engine) dynamicCordisResolveFSTarget(run *dynamicCordisRun, value goja.Value) (fsTarget, error) {
	var input dynamicFSTarget
	if err := dynamicCordisDecode(value, &input); err != nil {
		return fsTarget{}, fmt.Errorf("fs target: %w", err)
	}
	workspace, err := e.dynamicCordisWorkspace(run)
	if err != nil {
		return fsTarget{}, err
	}
	return resolveFSTarget(workspace, input.DisplayPath)
}

func dynamicFSVersion(info fs.FileInfo, data []byte) string {
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x:%d:%d:%o", digest, info.Size(), info.ModTime().UnixNano(), info.Mode())
}

func dynamicFSInfo(path string) (map[string]any, fsFileVersion, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fsFileVersion{}, err
	}
	typeName := "other"
	var data []byte
	var version fsFileVersion
	if info.Mode().IsRegular() {
		typeName = "file"
		data, version, _, err = readVersionedFile(path)
		if err != nil {
			return nil, fsFileVersion{}, err
		}
	} else if info.IsDir() {
		typeName = "directory"
	}
	return map[string]any{"version": dynamicFSVersion(info, data), "type": typeName, "size": info.Size()}, version, nil
}

type dynamicTextStream struct {
	engine  *Engine
	run     *dynamicCordisRun
	target  fsTarget
	signal  goja.Value
	file    *os.File
	info    fs.FileInfo
	digest  hash.Hash
	pending []byte
	readErr error
	eof     bool
	done    bool
}

func dynamicFSCheckAbort(signal goja.Value, verb string) error {
	if signal == nil || goja.IsUndefined(signal) || goja.IsNull(signal) {
		return nil
	}
	object, ok := signal.(*goja.Object)
	if ok && object.Get("aborted").ToBoolean() {
		return fsPolicyError("FS_ABORTED", verb+" aborted")
	}
	return nil
}

func dynamicFSErrorValue(vm *goja.Runtime, message string) goja.Value {
	code, _, ok := strings.Cut(message, ":")
	if !ok || !strings.HasPrefix(code, "FS_") {
		return vm.NewGoError(errors.New(message))
	}
	value := vm.NewGoError(errors.New(message))
	_ = value.Set("name", "FsError")
	_ = value.Set("code", code)
	return value
}

func dynamicFSRejectedValue(vm *goja.Runtime, value goja.Value) goja.Value {
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return value
	}
	if object, ok := value.(*goja.Object); ok {
		if code := object.Get("code"); code != nil && !goja.IsUndefined(code) {
			return value
		}
	}
	message := value.String()
	if code, _, ok := strings.Cut(message, ":"); ok && strings.HasPrefix(code, "FS_") {
		return dynamicFSErrorValue(vm, message)
	}
	return value
}

func (s *dynamicTextStream) close() {
	if s.done {
		return
	}
	s.done = true
	_ = s.file.Close()
}

func (s *dynamicTextStream) finish() {
	if s.done {
		return
	}
	s.done = true
	_ = s.file.Close()
	var digest [sha256.Size]byte
	copy(digest[:], s.digest.Sum(nil))
	s.engine.fsState.observe(s.run.sessionID, s.target, fsObservation{present: true, version: fsFileVersion{info: s.info, digest: digest}})
}

func dynamicUTF8Prefix(data []byte) (int, bool) {
	if utf8.Valid(data) {
		return len(data), true
	}
	for tail := 1; tail < utf8.UTFMax && tail <= len(data); tail++ {
		prefix := data[:len(data)-tail]
		if utf8.Valid(prefix) && !utf8.FullRune(data[len(data)-tail:]) {
			return len(prefix), true
		}
	}
	return 0, false
}

func (e *Engine) dynamicCordisAsyncValue(run *dynamicCordisRun, operation func() goja.Value) goja.Value {
	vm := run.runtime
	promise, resolve, reject := vm.NewPromise()
	settleError := func(value any) {
		if err := reject(value); err != nil {
			e.reportDynamicCordisHostFailure(run, "async built-in service", err)
		}
	}
	if !e.dynamicCordis.loop.post(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				switch value := recovered.(type) {
				case goja.Value:
					settleError(dynamicFSRejectedValue(vm, value))
				case *goja.Exception:
					settleError(dynamicFSRejectedValue(vm, value.Value()))
				case error:
					settleError(dynamicFSErrorValue(vm, value.Error()))
				default:
					settleError(dynamicFSRejectedValue(vm, vm.ToValue(fmt.Sprint(value))))
				}
			}
		}()
		if err := resolve(operation()); err != nil {
			e.reportDynamicCordisHostFailure(run, "async built-in service", err)
		}
	}) {
		settleError(vm.NewGoError(errors.New("dynamic Cordis runtime is closed")))
	}
	return vm.ToValue(promise)
}

func dynamicAsyncIteratorSymbol(vm *goja.Runtime) (*goja.Symbol, error) {
	symbols := vm.Get("Symbol").ToObject(vm)
	if symbol, ok := symbols.Get("asyncIterator").(*goja.Symbol); ok {
		return symbol, nil
	}
	symbol := goja.NewSymbol("Symbol.asyncIterator")
	if err := symbols.Set("asyncIterator", symbol); err != nil {
		return nil, err
	}
	return symbol, nil
}

func (s *dynamicTextStream) next() (string, bool, error) {
	for {
		if err := dynamicFSCheckAbort(s.signal, "read"); err != nil {
			s.close()
			return "", false, err
		}
		if s.done {
			return "", true, nil
		}
		if s.eof {
			if len(s.pending) > 0 {
				if !utf8.Valid(s.pending) || bytes.IndexByte(s.pending, 0) >= 0 {
					s.close()
					return "", false, errors.New("FS_NOT_TEXT: file is not UTF-8 text")
				}
				text := string(s.pending)
				s.pending = nil
				return text, false, nil
			}
			s.finish()
			return "", true, nil
		}
		if s.readErr != nil {
			err := s.readErr
			s.close()
			return "", false, err
		}
		buffer := make([]byte, 32*1024)
		n, readErr := s.file.Read(buffer)
		if n == 0 {
			if errors.Is(readErr, io.EOF) {
				s.eof = true
				continue
			}
			if readErr != nil {
				s.readErr = readErr
			}
			continue
		}
		chunk := append(s.pending, buffer[:n]...)
		s.pending = nil
		_, _ = s.digest.Write(buffer[:n])
		if bytes.IndexByte(chunk, 0) >= 0 {
			s.close()
			return "", false, errors.New("FS_NOT_TEXT: file is not UTF-8 text")
		}
		prefix, valid := dynamicUTF8Prefix(chunk)
		if !valid {
			s.close()
			return "", false, errors.New("FS_NOT_TEXT: file is not UTF-8 text")
		}
		s.pending = append(s.pending, chunk[prefix:]...)
		if errors.Is(readErr, io.EOF) {
			s.eof = true
		} else if readErr != nil {
			s.readErr = readErr
		}
		if prefix > 0 {
			return string(chunk[:prefix]), false, nil
		}
	}
}

func (e *Engine) dynamicCordisFSFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("resolve", func(call goja.FunctionCall) goja.Value {
		call.Arguments = append([]goja.Value(nil), call.Arguments...)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			workspace, err := e.dynamicCordisWorkspace(run)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			cwd := workspace
			var signal goja.Value
			if options, ok := call.Argument(1).(*goja.Object); ok {
				if value := options.Get("cwd"); value != nil && !goja.IsUndefined(value) && !goja.IsNull(value) {
					cwd = value.String()
				}
				signal = options.Get("signal")
			}
			if err := dynamicFSCheckAbort(signal, "resolve"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			target, err := resolveFSTarget(cwd, call.Argument(0).String())
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			if err := dynamicFSCheckAbort(signal, "resolve"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return vm.ToValue(map[string]any{"targetKey": target.targetKey, "displayPath": target.displayPath})
		})
	})
	_ = service.Set("processPath", func(call goja.FunctionCall) goja.Value {
		target, err := e.dynamicCordisResolveFSTarget(run, call.Argument(0))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(target.targetKey)
	})
	_ = service.Set("fileUrl", func(call goja.FunctionCall) goja.Value {
		target, err := e.dynamicCordisResolveFSTarget(run, call.Argument(0))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(lspFileURI(target.targetKey))
	})
	_ = service.Set("contains", func(call goja.FunctionCall) goja.Value {
		parent, err := e.dynamicCordisResolveFSTarget(run, call.Argument(0))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		child, err := e.dynamicCordisResolveFSTarget(run, call.Argument(1))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(sandboxPathUnder(child.targetKey, parent.targetKey))
	})
	_ = service.Set("stat", func(call goja.FunctionCall) goja.Value {
		call.Arguments = append([]goja.Value(nil), call.Arguments...)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if err := dynamicFSCheckAbort(call.Argument(1), "stat"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			target, err := e.dynamicCordisResolveFSTarget(run, call.Argument(0))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			info, _, err := dynamicFSInfo(target.targetKey)
			if abortErr := dynamicFSCheckAbort(call.Argument(1), "stat"); abortErr != nil {
				panic(vm.ToValue(abortErr.Error()))
			}
			if errors.Is(err, fs.ErrNotExist) {
				return goja.Undefined()
			}
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return vm.ToValue(info)
		})
	})
	_ = service.Set("lstat", func(call goja.FunctionCall) goja.Value {
		call.Arguments = append([]goja.Value(nil), call.Arguments...)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if err := dynamicFSCheckAbort(call.Argument(2), "lstat"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			path := strings.TrimSpace(call.Argument(0).String())
			if path == "" {
				panic(vm.ToValue("FS_NOT_FOUND: file_path must be a non-empty string"))
			}
			cwd, err := e.dynamicCordisWorkspace(run)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			if options, ok := call.Argument(1).(*goja.Object); ok {
				if value := options.Get("cwd"); value != nil && !goja.IsUndefined(value) && !goja.IsNull(value) {
					cwd = value.String()
				}
			}
			if !filepath.IsAbs(path) {
				path = filepath.Join(cwd, path)
			}
			path, err = filepath.Abs(filepath.Clean(path))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			info, err := os.Lstat(path)
			if abortErr := dynamicFSCheckAbort(call.Argument(2), "lstat"); abortErr != nil {
				panic(vm.ToValue(abortErr.Error()))
			}
			if errors.Is(err, fs.ErrNotExist) {
				return goja.Undefined()
			}
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			typeName := "other"
			switch {
			case info.Mode()&fs.ModeSymlink != 0:
				typeName = "symlink"
			case info.Mode().IsRegular():
				typeName = "file"
			case info.IsDir():
				typeName = "directory"
			}
			return vm.ToValue(map[string]any{"version": dynamicFSVersion(info, nil), "type": typeName, "size": info.Size()})
		})
	})
	_ = service.Set("readText", func(call goja.FunctionCall) goja.Value {
		call.Arguments = append([]goja.Value(nil), call.Arguments...)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if err := dynamicFSCheckAbort(call.Argument(1), "read"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			target, err := e.dynamicCordisResolveFSTarget(run, call.Argument(0))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			data, version, _, err := readVersionedFile(target.targetKey)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					e.fsState.observe(run.sessionID, target, fsObservation{})
					err = fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("cannot read %q: not found", target.displayPath))
				}
				panic(vm.ToValue(err.Error()))
			}
			if !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
				panic(vm.ToValue("FS_NOT_TEXT: file is not UTF-8 text"))
			}
			if err := dynamicFSCheckAbort(call.Argument(1), "read"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			e.fsState.observe(run.sessionID, target, fsObservation{present: true, version: version})
			return vm.ToValue(string(data))
		})
	})
	_ = service.Set("streamText", func(call goja.FunctionCall) goja.Value {
		call.Arguments = append([]goja.Value(nil), call.Arguments...)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if err := dynamicFSCheckAbort(call.Argument(1), "read"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			target, err := e.dynamicCordisResolveFSTarget(run, call.Argument(0))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			file, err := os.Open(target.targetKey)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					e.fsState.observe(run.sessionID, target, fsObservation{})
					err = fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("cannot read %q: not found", target.displayPath))
				}
				panic(vm.ToValue(err.Error()))
			}
			info, err := file.Stat()
			if err != nil {
				_ = file.Close()
				panic(vm.ToValue(err.Error()))
			}
			if !info.Mode().IsRegular() {
				_ = file.Close()
				panic(vm.ToValue(fsPolicyError("FS_NOT_REGULAR_FILE", fmt.Sprintf("cannot read %q: not a regular file", target.displayPath)).Error()))
			}
			stream := &dynamicTextStream{engine: e, run: run, target: target, signal: call.Argument(1), file: file, info: info, digest: sha256.New()}
			iterator := vm.NewObject()
			_ = iterator.Set("next", func(goja.FunctionCall) goja.Value {
				text, done, err := stream.next()
				if err != nil {
					panic(dynamicFSErrorValue(vm, err.Error()))
				}
				if done {
					return vm.ToValue(map[string]any{"done": true})
				}
				return vm.ToValue(map[string]any{"done": false, "value": text})
			})
			_ = iterator.Set("return", func(goja.FunctionCall) goja.Value {
				stream.close()
				return vm.ToValue(map[string]any{"done": true})
			})
			asyncIterator, err := dynamicAsyncIteratorSymbol(vm)
			if err != nil {
				stream.close()
				panic(vm.ToValue(err.Error()))
			}
			if err := iterator.SetSymbol(asyncIterator, func(goja.FunctionCall) goja.Value { return iterator }); err != nil {
				stream.close()
				panic(vm.ToValue(err.Error()))
			}
			run.disposers = append(run.disposers, stream.close)
			return iterator
		})
	})
	_ = service.Set("readBytes", func(call goja.FunctionCall) goja.Value {
		call.Arguments = append([]goja.Value(nil), call.Arguments...)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if err := dynamicFSCheckAbort(call.Argument(1), "read"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			target, err := e.dynamicCordisResolveFSTarget(run, call.Argument(0))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			maxBytes := call.Argument(2).ToFloat()
			if math.IsNaN(maxBytes) || math.IsInf(maxBytes, 0) || maxBytes < 0 || math.Trunc(maxBytes) != maxBytes || maxBytes > 1<<53-1 {
				panic(vm.ToValue("ctx.fs.readBytes maxBytes must be a non-negative safe integer"))
			}
			file, err := os.Open(target.targetKey)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					e.fsState.observe(run.sessionID, target, fsObservation{})
					err = fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("cannot read %q: not found", target.displayPath))
				}
				panic(vm.ToValue(err.Error()))
			}
			defer file.Close()
			info, err := file.Stat()
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			if !info.Mode().IsRegular() {
				panic(vm.ToValue(fsPolicyError("FS_NOT_REGULAR_FILE", fmt.Sprintf("cannot read %q: not a regular file", target.displayPath)).Error()))
			}
			limit := int64(maxBytes)
			if info.Size() > limit {
				panic(vm.ToValue(fsPolicyError("FS_TOO_LARGE", fmt.Sprintf("cannot read %q: %d bytes exceeds the %d-byte limit", target.displayPath, info.Size(), limit)).Error()))
			}
			data, err := io.ReadAll(io.LimitReader(file, limit+1))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			if int64(len(data)) > limit {
				panic(vm.ToValue(fsPolicyError("FS_TOO_LARGE", fmt.Sprintf("cannot read %q: content exceeds the %d-byte limit", target.displayPath, limit)).Error()))
			}
			if err := dynamicFSCheckAbort(call.Argument(1), "read"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			version := fsFileVersion{info: info, digest: sha256.Sum256(data)}
			e.fsState.observe(run.sessionID, target, fsObservation{present: true, version: version})
			array, err := vm.New(vm.Get("Uint8Array"), vm.ToValue(vm.NewArrayBuffer(data)))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return array
		})
	})
	_ = service.Set("listDir", func(call goja.FunctionCall) goja.Value {
		call.Arguments = append([]goja.Value(nil), call.Arguments...)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if err := dynamicFSCheckAbort(call.Argument(1), "list"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			target, err := e.dynamicCordisResolveFSTarget(run, call.Argument(0))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			entries, err := os.ReadDir(target.targetKey)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					err = fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("cannot list %q: not found", target.displayPath))
				}
				panic(vm.ToValue(err.Error()))
			}
			rows := make([]map[string]any, 0, len(entries))
			for _, entry := range entries {
				child, resolveErr := resolveFSTarget(target.displayPath, entry.Name())
				if resolveErr != nil {
					panic(vm.ToValue(resolveErr.Error()))
				}
				info, _, infoErr := dynamicFSInfo(child.targetKey)
				if infoErr != nil {
					panic(vm.ToValue(infoErr.Error()))
				}
				rows = append(rows, map[string]any{
					"name": entry.Name(), "type": info["type"], "target": map[string]any{"targetKey": child.targetKey, "displayPath": child.displayPath},
					"version": info["version"], "size": info["size"],
				})
			}
			if err := dynamicFSCheckAbort(call.Argument(1), "list"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			sort.Slice(rows, func(i, j int) bool { return rows[i]["name"].(string) < rows[j]["name"].(string) })
			return vm.ToValue(rows)
		})
	})
	_ = service.Set("writeText", func(call goja.FunctionCall) goja.Value {
		call.Arguments = append([]goja.Value(nil), call.Arguments...)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if err := dynamicFSCheckAbort(call.Argument(3), "write"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			target, err := e.dynamicCordisResolveFSTarget(run, call.Argument(0))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			content := call.Argument(1).String()
			workspace, err := e.dynamicCordisWorkspace(run)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			mode, err := e.sandboxModeForCall(ToolCall{Name: "dynamic.fs.writeText", Workspace: workspace, SessionID: run.sessionID})
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			path, err := e.sandboxMutationPathWithMode(ToolCall{Name: "dynamic.fs.writeText", Workspace: workspace, SessionID: run.sessionID}, target.displayPath, mode)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			target.targetKey = path
			before, current, _, readErr := readVersionedFile(path)
			exists := readErr == nil
			if readErr != nil && !errors.Is(readErr, fs.ErrNotExist) {
				panic(vm.ToValue(readErr.Error()))
			}
			intent := fsWriteIntent{create: !exists, version: current}
			if expected, ok := call.Argument(2).(*goja.Object); ok {
				kind := expected.Get("kind")
				if kind == nil {
					panic(vm.ToValue("writeText expected.kind is required"))
				}
				switch kind.String() {
				case "createIfAbsent":
					intent = fsWriteIntent{create: true}
				case "replaceIfVersion":
					version := expected.Get("version")
					if version == nil || !exists || version.String() != dynamicFSVersion(current.info, before) {
						panic(vm.ToValue(fsPolicyError("FS_STALE_VERSION", fmt.Sprintf("cannot write %q: file changed since it was read", target.displayPath)).Error()))
					}
					intent = fsWriteIntent{version: current}
				default:
					panic(vm.ToValue("writeText expected.kind must be createIfAbsent or replaceIfVersion"))
				}
			}
			if err := dynamicFSCheckAbort(call.Argument(3), "write"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			version, wasPresent, _, err := e.guardedFSWrite(target, []byte(content), intent)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			e.fsState.observe(run.sessionID, target, fsObservation{present: true, version: version})
			afterInfo, statErr := os.Stat(path)
			if statErr != nil {
				panic(vm.ToValue(statErr.Error()))
			}
			var beforeValue any
			if wasPresent && utf8.Valid(before) && bytes.IndexByte(before, 0) < 0 {
				beforeValue = string(before)
			}
			return vm.ToValue(map[string]any{
				"operation": map[bool]string{true: "update", false: "create"}[wasPresent],
				"version":   dynamicFSVersion(afterInfo, []byte(content)), "before": beforeValue, "after": content,
			})
		})
	})
	_ = service.Set("editText", func(call goja.FunctionCall) goja.Value {
		call.Arguments = append([]goja.Value(nil), call.Arguments...)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if err := dynamicFSCheckAbort(call.Argument(3), "edit"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			target, err := e.dynamicCordisResolveFSTarget(run, call.Argument(0))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			var edit struct {
				OldString  string `json:"oldString"`
				NewString  string `json:"newString"`
				ReplaceAll bool   `json:"replaceAll"`
			}
			if err := dynamicCordisDecode(call.Argument(1), &edit); err != nil {
				panic(vm.ToValue("ctx.fs.editText request: " + err.Error()))
			}
			if edit.OldString == "" {
				panic(vm.ToValue("FS_EDIT_NOT_FOUND: oldString must not be empty"))
			}
			workspace, err := e.dynamicCordisWorkspace(run)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			mode, err := e.sandboxModeForCall(ToolCall{Name: "dynamic.fs.editText", Workspace: workspace, SessionID: run.sessionID})
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			path, err := e.sandboxMutationPathWithMode(ToolCall{Name: "dynamic.fs.editText", Workspace: workspace, SessionID: run.sessionID}, target.displayPath, mode)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			target.targetKey = path
			before, current, _, err := readVersionedFile(path)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			if expected, ok := call.Argument(2).(*goja.Object); ok {
				version := expected.Get("version")
				if version == nil || version.String() != dynamicFSVersion(current.info, before) {
					panic(vm.ToValue(fsPolicyError("FS_STALE_VERSION", fmt.Sprintf("cannot edit %q: file changed since it was read", target.displayPath)).Error()))
				}
			}
			if err := dynamicFSCheckAbort(call.Argument(3), "edit"); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			var after string
			version, err := e.guardedFSEdit(target, current, func(input string) (string, error) {
				count := strings.Count(input, edit.OldString)
				if count == 0 {
					return "", fsPolicyError("FS_EDIT_NOT_FOUND", "editText: oldString was not found")
				}
				if count > 1 && !edit.ReplaceAll {
					return "", fsPolicyError("FS_AMBIGUOUS_EDIT", fmt.Sprintf("editText: oldString occurs %d times", count))
				}
				n := 1
				if edit.ReplaceAll {
					n = -1
				}
				after = strings.Replace(input, edit.OldString, edit.NewString, n)
				return after, nil
			})
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			e.fsState.observe(run.sessionID, target, fsObservation{present: true, version: version})
			afterInfo, statErr := os.Stat(path)
			if statErr != nil {
				panic(vm.ToValue(statErr.Error()))
			}
			return vm.ToValue(map[string]any{"version": dynamicFSVersion(afterInfo, []byte(after)), "before": string(before), "after": after})
		})
	})
	return service
}

type dynamicShellSpec struct {
	Command        string            `json:"command"`
	Workdir        string            `json:"workdir"`
	TimeoutMS      int               `json:"timeoutMs"`
	StdoutMaxBytes int               `json:"stdoutMaxBytes"`
	Stdin          string            `json:"stdin,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	SandboxPolicy  map[string]any    `json:"sandboxPolicy,omitempty"`
}

const dynamicShellMaxSpillBytes = 64 << 20

var dynamicShellSpillDirectory struct {
	sync.Once
	path string
	err  error
}

type dynamicShellOutput struct {
	mu            sync.Mutex
	data          []byte
	base          int64
	end           int64
	cursor        int64
	limit         int
	label         string
	truncated     bool
	spill         *os.File
	spillPath     string
	spillDisabled bool
}

func dynamicShellCreateSpill(label string) (*os.File, string, error) {
	dynamicShellSpillDirectory.Do(func() {
		dynamicShellSpillDirectory.path, dynamicShellSpillDirectory.err = os.MkdirTemp("", "dsh-subprocess-")
	})
	if dynamicShellSpillDirectory.err != nil {
		return nil, "", dynamicShellSpillDirectory.err
	}
	file, err := os.CreateTemp(dynamicShellSpillDirectory.path, "dsh-subprocess-*-"+label+".log")
	if err != nil {
		return nil, "", err
	}
	return file, file.Name(), nil
}

func (s *dynamicShellOutput) discardSpillLocked() {
	if s.spill != nil {
		_ = s.spill.Close()
	}
	if s.spillPath != "" {
		_ = os.Remove(s.spillPath)
	}
	s.spill, s.spillPath, s.spillDisabled = nil, "", true
}

func (s *dynamicShellOutput) Write(input []byte) (int, error) {
	written := len(input)
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := s.limit
	if limit <= 0 {
		limit = toolOutputLimit
	}
	nextEnd := s.end + int64(len(input))
	overflow := len(s.data)+len(input) > limit
	if s.spill != nil {
		if nextEnd > dynamicShellMaxSpillBytes {
			s.discardSpillLocked()
		} else if _, err := s.spill.Write(input); err != nil {
			s.discardSpillLocked()
		}
	} else if overflow && !s.spillDisabled {
		if nextEnd > dynamicShellMaxSpillBytes {
			s.spillDisabled = true
		} else if file, path, err := dynamicShellCreateSpill(s.label); err != nil {
			s.spillDisabled = true
		} else {
			s.spill, s.spillPath = file, path
			_, priorErr := s.spill.Write(s.data)
			_, inputErr := s.spill.Write(input)
			if priorErr != nil || inputErr != nil {
				s.discardSpillLocked()
			}
		}
	}
	s.data = append(s.data, input...)
	s.end = nextEnd
	if len(s.data) > limit {
		drop := len(s.data) - limit
		s.data = append([]byte(nil), s.data[drop:]...)
		s.base += int64(drop)
		s.truncated = true
	}
	return written, nil
}

func (s *dynamicShellOutput) seal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.spill == nil {
		return
	}
	if err := s.spill.Sync(); err != nil {
		s.discardSpillLocked()
		return
	}
	if err := s.spill.Close(); err != nil {
		_ = os.Remove(s.spillPath)
		s.spillPath = ""
	}
	s.spill = nil
}

func (s *dynamicShellOutput) read() (string, bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lossy := s.cursor < s.base
	if lossy {
		s.cursor = s.base
	}
	start := int(s.cursor - s.base)
	text := string(append([]byte(nil), s.data[start:]...))
	s.cursor = s.end
	return text, lossy, s.spillPath
}

func (s *dynamicShellOutput) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := map[string]any{"text": string(append([]byte(nil), s.data...)), "truncated": s.truncated}
	if s.spillPath != "" {
		result["spillPath"] = s.spillPath
	}
	return result
}

func (s *dynamicShellOutput) text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.data)
}

type dynamicShellProcess struct {
	mu            sync.Mutex
	command       *exec.Cmd
	stdout        *dynamicShellOutput
	stderr        *dynamicShellOutput
	done          chan struct{}
	status        string
	exitCode      *int
	signal        string
	sandboxMode   string
	sandboxDenied bool
}

func dynamicShellExit(err error) (*int, string, error) {
	if err == nil {
		code := 0
		return &code, "", nil
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return nil, "", err
	}
	if wait, ok := exit.Sys().(syscall.WaitStatus); ok && wait.Signaled() {
		return nil, jobSignalName(wait.Signal()), nil
	}
	if signal := shellSignal(exit.ExitCode()); signal != 0 {
		return nil, jobSignalName(signal), nil
	}
	code := exit.ExitCode()
	return &code, "", nil
}

func (p *dynamicShellProcess) wait(state shellChildState) {
	exitCode, signal, waitErr := dynamicShellExit(waitShellChild(p.command, state))
	if waitErr != nil {
		_, _ = p.stderr.Write([]byte("process wait failed: " + waitErr.Error()))
	}
	p.stdout.seal()
	p.stderr.seal()
	stdout, stderr := p.stdout.text(), p.stderr.text()
	p.mu.Lock()
	if p.status == "running" {
		if signal != "" || waitErr != nil {
			p.status = "killed"
		} else {
			p.status = "completed"
		}
	}
	p.exitCode, p.signal = exitCode, signal
	p.sandboxDenied = sandboxOutputDenied(stdout + "\n" + stderr)
	close(p.done)
	p.mu.Unlock()
}

func (p *dynamicShellProcess) kill() bool {
	p.mu.Lock()
	if p.status != "running" {
		p.mu.Unlock()
		return false
	}
	p.status = "killed"
	command, done := p.command, p.done
	pid := command.Process.Pid
	p.mu.Unlock()
	if err := terminateChildProcess(command); err != nil {
		_ = killChildProcess(command)
	}
	go func() {
		timer := time.NewTimer(jobStopGrace)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			_ = killChildProcessPID(pid)
		}
	}()
	return true
}

func (p *dynamicShellProcess) readOutput() map[string]any {
	stdout, stdoutLossy, stdoutSpillPath := p.stdout.read()
	stderr, stderrLossy, stderrSpillPath := p.stderr.read()
	if stderr != "" {
		if stdout != "" && !strings.HasSuffix(stdout, "\n") {
			stdout += "\n"
		}
		stdout += "[stderr]\n" + stderr
	}
	result := map[string]any{"delta": stdout, "lossy": stdoutLossy || stderrLossy}
	if stdoutSpillPath != "" {
		result["stdoutSpillPath"] = stdoutSpillPath
	}
	if stderrSpillPath != "" {
		result["stderrSpillPath"] = stderrSpillPath
	}
	return result
}

func dynamicShellProcessObject(run *dynamicCordisRun, process *dynamicShellProcess) *goja.Object {
	vm := run.runtime
	object := vm.NewObject()
	_ = object.DefineAccessorProperty("status", vm.ToValue(func() string {
		process.mu.Lock()
		defer process.mu.Unlock()
		return process.status
	}), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = object.DefineAccessorProperty("exitCode", vm.ToValue(func() goja.Value {
		process.mu.Lock()
		defer process.mu.Unlock()
		if process.exitCode == nil {
			return goja.Null()
		}
		return vm.ToValue(*process.exitCode)
	}), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = object.DefineAccessorProperty("signal", vm.ToValue(func() goja.Value {
		process.mu.Lock()
		defer process.mu.Unlock()
		if process.signal == "" {
			return goja.Null()
		}
		return vm.ToValue(process.signal)
	}), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = object.DefineAccessorProperty("sandbox", vm.ToValue(func() goja.Value {
		process.mu.Lock()
		defer process.mu.Unlock()
		return vm.ToValue(map[string]any{"mode": process.sandboxMode, "denied": process.sandboxDenied})
	}), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = object.Set("readOutput", func(goja.FunctionCall) goja.Value { return vm.ToValue(process.readOutput()) })
	_ = object.Set("kill", func(goja.FunctionCall) goja.Value { return vm.ToValue(process.kill()) })

	done, resolve, _ := vm.NewPromise()
	go func() {
		<-process.done
		run.ctxFacade.engine.dynamicCordis.loop.post(func() {
			if err := resolve(goja.Undefined()); err != nil {
				run.ctxFacade.engine.reportDynamicCordisHostFailure(run, "shell process", err)
			}
		})
	}()
	_ = object.DefineDataProperty("done", vm.ToValue(done), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE)
	return object
}

func (e *Engine) dynamicCordisShellFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	resolve := func(value goja.Value) (dynamicShellSpec, string, error) {
		var request dynamicShellSpec
		if err := dynamicCordisDecode(value, &request); err != nil {
			return request, "", fmt.Errorf("ctx.shell.resolve request: %w", err)
		}
		if strings.TrimSpace(request.Command) == "" {
			return request, "", errors.New("ctx.shell command is required")
		}
		workspace, err := e.dynamicCordisWorkspace(run)
		if err != nil {
			return request, "", err
		}
		request.Workdir, err = shellWorkdir(workspace, request.Workdir)
		if err != nil {
			return request, "", err
		}
		info, err := os.Stat(request.Workdir)
		if err != nil || !info.IsDir() {
			return request, "", fmt.Errorf("ctx.shell invalid workdir %q", request.Workdir)
		}
		if request.TimeoutMS <= 0 {
			request.TimeoutMS = 60000
		} else if request.TimeoutMS > 120000 {
			request.TimeoutMS = 120000
		}
		if request.StdoutMaxBytes <= 0 || request.StdoutMaxBytes > toolOutputLimit {
			request.StdoutMaxBytes = toolOutputLimit
		}
		mode, err := e.sandboxModeForCall(ToolCall{Name: "dynamic.shell.run", Workspace: workspace, SessionID: run.sessionID})
		return request, mode, err
	}
	_ = service.Set("resolve", func(call goja.FunctionCall) goja.Value {
		spec, mode, err := resolve(call.Argument(0))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		result := map[string]any{
			"command": spec.Command, "workdir": spec.Workdir, "timeoutMs": spec.TimeoutMS,
			"stdoutMaxBytes": spec.StdoutMaxBytes, "sandboxPolicy": map[string]any{"mode": mode},
		}
		if spec.Stdin != "" {
			result["stdin"] = spec.Stdin
		}
		if len(spec.Env) > 0 {
			result["env"] = spec.Env
		}
		return vm.ToValue(result)
	})
	_ = service.Set("start", func(call goja.FunctionCall) goja.Value {
		spec, mode, err := resolve(call.Argument(0))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		workspace, err := e.dynamicCordisWorkspace(run)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		program, args, err := shellInvocation(spec.Command, mode, workspace, spec.Workdir)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		command := exec.Command(program, args...)
		command.Dir = spec.Workdir
		environment := shellEnvironment()
		for name, value := range spec.Env {
			environment[name] = value
		}
		command.Env = scrubbedChildEnv(environment)
		if spec.Stdin != "" {
			command.Stdin = strings.NewReader(spec.Stdin)
		}
		configureChildProcess(command)
		stdout := &dynamicShellOutput{limit: toolOutputLimit, label: "stdout"}
		stderr := &dynamicShellOutput{limit: toolOutputLimit, label: "stderr"}
		command.Stdout, command.Stderr = stdout, stderr
		childState, err := prepareShellChild(command, mode, workspace)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		process := &dynamicShellProcess{
			command: command, stdout: stdout, stderr: stderr, done: make(chan struct{}), status: "running", sandboxMode: mode,
		}
		if err := startShellChild(command, childState); err != nil {
			process.status = "killed"
			_, _ = stderr.Write([]byte("spawn failed: " + err.Error()))
			stdout.seal()
			stderr.seal()
			close(process.done)
		} else {
			go process.wait(childState)
		}
		run.disposers = append(run.disposers, func() {
			process.kill()
		})
		return dynamicShellProcessObject(run, process)
	})
	_ = service.Set("run", func(call goja.FunctionCall) goja.Value {
		spec, mode, err := resolve(call.Argument(0))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		workspace, err := e.dynamicCordisWorkspace(run)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		program, args, err := shellInvocation(spec.Command, mode, workspace, spec.Workdir)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(spec.TimeoutMS)*time.Millisecond)
		defer cancel()
		stopped, monitorDone := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(monitorDone)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					e.dynamicCordis.RLock()
					plugin := e.dynamicCordis.plugins[run.pluginID]
					active := plugin != nil && plugin.run == run
					e.dynamicCordis.RUnlock()
					if !active {
						close(stopped)
						cancel()
						return
					}
				}
			}
		}()
		defer func() { cancel(); <-monitorDone }()
		command := exec.CommandContext(ctx, program, args...)
		command.Dir = spec.Workdir
		environment := shellEnvironment()
		for name, value := range spec.Env {
			environment[name] = value
		}
		command.Env = scrubbedChildEnv(environment)
		if spec.Stdin != "" {
			command.Stdin = strings.NewReader(spec.Stdin)
		}
		configureChildProcess(command)
		stdout := &dynamicShellOutput{limit: spec.StdoutMaxBytes, label: "stdout"}
		stderr := &dynamicShellOutput{limit: toolOutputLimit, label: "stderr"}
		command.Stdout, command.Stderr = stdout, stderr
		childState, err := prepareShellChild(command, mode, workspace)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		err = startShellChild(command, childState)
		if err == nil {
			err = waitShellChild(command, childState)
		}
		stdout.seal()
		stderr.seal()
		exitCode := 0
		if err != nil {
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				exitCode = exit.ExitCode()
			} else if ctx.Err() == nil {
				panic(vm.ToValue(err.Error()))
			}
		}
		timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
		aborted := false
		select {
		case <-stopped:
			aborted, timedOut = true, false
		default:
		}
		if timedOut || aborted {
			exitCode = -1
		}
		stdoutResult, stderrResult := stdout.snapshot(), stderr.snapshot()
		result := map[string]any{
			"exitCode": exitCode, "signal": nil, "timedOut": timedOut, "aborted": aborted, "timeoutMs": spec.TimeoutMS,
			"stdout":  stdoutResult,
			"stderr":  stderrResult,
			"sandbox": map[string]any{"mode": mode, "denied": sandboxOutputDenied(stdoutResult["text"].(string) + "\n" + stderrResult["text"].(string))},
		}
		return vm.ToValue(result)
	})
	return service
}
