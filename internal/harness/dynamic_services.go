package harness

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
)

type dynamicCordisContextFacade struct {
	engine *Engine
	run    *dynamicCordisRun
	values map[string]goja.Value
}

type dynamicCordisListener struct {
	run     *dynamicCordisRun
	fn      goja.Callable
	once    bool
	prepend bool
	seq     uint64
}

func dynamicCordisListenerLess(left, right *dynamicCordisListener) bool {
	if left.prepend != right.prepend {
		return left.prepend
	}
	if left.prepend {
		return left.seq > right.seq
	}
	return left.seq < right.seq
}

func (f *dynamicCordisContextFacade) Get(key string) goja.Value {
	if dynamicCordisDeniedContextMember(key) {
		panic(f.run.runtime.ToValue(fmt.Sprintf("sandbox ctx does not expose %q; framework internals are withheld by design", key)))
	}
	if dynamicCordisTimerMember(key) && !f.injects("timer") {
		panic(f.run.runtime.ToValue("service \"timer\" is not injected; add inject: ['timer', …]"))
	}
	if f.engine.dynamicCordisBuiltinService(key) || f.engine.dynamicCordisServiceExists(key) {
		if !f.injects(key) {
			panic(f.run.runtime.ToValue(fmt.Sprintf("service %q is not injected; add inject: ['%s', …]", key, key)))
		}
		return f.engine.dynamicCordisServiceValue(f.run, key)
	}
	return f.values[key]
}

func (f *dynamicCordisContextFacade) Set(string, goja.Value) bool {
	panic(f.run.runtime.ToValue("sandbox ctx is read-only"))
}

func (f *dynamicCordisContextFacade) Has(key string) bool {
	if dynamicCordisDeniedContextMember(key) {
		return false
	}
	if dynamicCordisTimerMember(key) {
		return f.injects("timer")
	}
	if f.engine.dynamicCordisBuiltinService(key) || f.engine.dynamicCordisServiceExists(key) {
		return f.injects(key)
	}
	_, ok := f.values[key]
	return ok
}

func (f *dynamicCordisContextFacade) Delete(string) bool {
	panic(f.run.runtime.ToValue("sandbox ctx is read-only"))
}

func (f *dynamicCordisContextFacade) Keys() []string {
	keys := make([]string, 0, len(f.values)+len(f.run.inject))
	for key := range f.values {
		if !dynamicCordisTimerMember(key) || f.injects("timer") {
			keys = append(keys, key)
		}
	}
	for _, key := range f.run.inject {
		if f.engine.dynamicCordisBuiltinService(key) || f.engine.dynamicCordisServiceExists(key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func (f *dynamicCordisContextFacade) injects(name string) bool {
	for _, candidate := range f.run.inject {
		if candidate == name {
			return true
		}
	}
	return false
}

func dynamicCordisDeniedContextMember(name string) bool {
	switch name {
	case "root", "parent", "scope", "fiber", "reflect", "registry", "events", "extend", "isolate", "intercept", "plugin", "set", "mixin":
		return true
	default:
		return false
	}
}

func dynamicCordisTimerMember(name string) bool {
	switch name {
	case "setTimeout", "timeout", "setInterval", "interval", "throttle", "debounce":
		return true
	default:
		return false
	}
}

func (e *Engine) dynamicCordisServiceExists(name string) bool {
	e.dynamicCordis.RLock()
	_, ok := e.dynamicCordis.services[name]
	e.dynamicCordis.RUnlock()
	return ok
}

func installDynamicCordisGlobals(run *dynamicCordisRun, pluginID string) error {
	vm := run.runtime
	trap := func(name, replacement string) func(goja.FunctionCall) goja.Value {
		return func(goja.FunctionCall) goja.Value {
			panic(vm.ToValue(fmt.Sprintf("%s is not available in the dynamic package sandbox; use %s", name, replacement)))
		}
	}
	for name, value := range map[string]any{
		"require":    trap("require", "an injected Cordis service"),
		"fetch":      trap("fetch", "ctx.web"),
		"setTimeout": trap("setTimeout", "ctx.setTimeout / ctx.interval"),
		"btoa": func(call goja.FunctionCall) goja.Value {
			return vm.ToValue(base64.StdEncoding.EncodeToString([]byte(call.Argument(0).String())))
		},
		"atob": func(call goja.FunctionCall) goja.Value {
			data, err := base64.StdEncoding.DecodeString(call.Argument(0).String())
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return vm.ToValue(string(data))
		},
	} {
		if err := vm.Set(name, value); err != nil {
			return err
		}
	}
	console := vm.NewObject()
	for _, level := range []string{"log", "info", "warn", "error", "debug"} {
		name := level
		_ = console.Set(name, func(call goja.FunctionCall) goja.Value {
			parts := make([]string, len(call.Arguments))
			for index, value := range call.Arguments {
				parts[index] = value.String()
			}
			log.Printf("[cordis:%s] %s", pluginID, strings.Join(parts, " "))
			return goja.Undefined()
		})
	}
	if err := vm.Set("console", console); err != nil {
		return err
	}
	if err := vm.Set("TextEncoder", func(call goja.ConstructorCall) *goja.Object {
		_ = call.This.Set("encode", func(call goja.FunctionCall) goja.Value {
			return vm.ToValue([]byte(call.Argument(0).String()))
		})
		return nil
	}); err != nil {
		return err
	}
	symbol := vm.Get("Symbol").ToObject(vm)
	if value := symbol.Get("asyncIterator"); value == nil || goja.IsUndefined(value) {
		if err := symbol.Set("asyncIterator", goja.NewSymbol("Symbol.asyncIterator")); err != nil {
			return err
		}
	}
	return vm.Set("TextDecoder", func(call goja.ConstructorCall) *goja.Object {
		_ = call.This.Set("decode", func(call goja.FunctionCall) goja.Value {
			switch value := call.Argument(0).Export().(type) {
			case []byte:
				return vm.ToValue(string(value))
			default:
				return vm.ToValue(call.Argument(0).String())
			}
		})
		return nil
	})
}

func dynamicCordisCallBounded(vm *goja.Runtime, timeout time.Duration, call func() (goja.Value, error)) (goja.Value, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	timedOut := errors.New("dynamic Host synchronous evaluation timed out")
	timer := time.AfterFunc(timeout, func() { vm.Interrupt(timedOut) })
	value, err := call()
	if !timer.Stop() {
		vm.ClearInterrupt()
	}
	return value, err
}

func (e *Engine) dynamicCordisMissingServices(run *dynamicCordisRun) []string {
	e.dynamicCordis.RLock()
	missing := dynamicCordisMissingServicesLocked(e, run)
	e.dynamicCordis.RUnlock()
	return missing
}

func (e *Engine) dynamicCordisBuiltinService(name string) bool {
	if dynamicCordisNativeService(name) || dynamicCordisAgentService(name) {
		return true
	}
	switch name {
	case "tools", "systemPrompt", "timer", "fs", "shell", "shellEnv", "webServer":
		return true
	case "web":
		return true
	default:
		return false
	}
}

func dynamicCordisInjectList(plugin *goja.Object) ([]string, error) {
	value := plugin.Get("inject")
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return nil, nil
	}
	raw, ok := value.Export().([]any)
	if !ok {
		if strings, ok := value.Export().([]string); ok {
			return append([]string(nil), strings...), nil
		}
		return nil, errors.New("dynamic Host Plugin inject must be an array of service names")
	}
	seen := map[string]bool{}
	names := make([]string, 0, len(raw))
	for _, item := range raw {
		name, ok := item.(string)
		name = strings.TrimSpace(name)
		if !ok || name == "" {
			return nil, errors.New("dynamic Host Plugin inject must contain non-empty service names")
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return names, nil
}

func (e *Engine) configureDynamicCordisContext(run *dynamicCordisRun, owner string) error {
	vm := run.runtime
	set := func(name string, value any) { run.ctxFacade.values[name] = vm.ToValue(value) }
	set("get", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisServiceValue(run, strings.TrimSpace(call.Argument(0).String()))
	})
	set("provide", func(call goja.FunctionCall) goja.Value {
		name := strings.TrimSpace(call.Argument(0).String())
		value := call.Argument(1)
		if name == "" || value == nil || goja.IsUndefined(value) {
			panic(vm.ToValue("ctx.provide requires a service name and value"))
		}
		e.dynamicCordis.Lock()
		if _, exists := e.dynamicCordis.services[name]; exists {
			e.dynamicCordis.Unlock()
			panic(vm.ToValue(fmt.Sprintf("service %q has been registered", name)))
		}
		e.dynamicCordis.services[name] = dynamicCordisService{owner: run, value: value}
		run.provided[name] = value
		e.dynamicCordis.Unlock()
		go e.activateAvailableDynamicCordisRuns()
		return vm.ToValue(func() { e.removeDynamicCordisService(run, name) })
	})
	set("effect", func(call goja.FunctionCall) goja.Value {
		run.mu.Lock()
		inactive := run.cleaning || run.disposed
		run.mu.Unlock()
		if inactive {
			panic(vm.ToValue("cannot register a Cordis effect while its owner is unloading"))
		}
		fn, ok := goja.AssertFunction(call.Argument(0))
		if !ok {
			panic(vm.ToValue("ctx.effect requires a function"))
		}
		value, err := fn(goja.Undefined())
		if err != nil {
			panic(vm.ToValue(dynamicJSMessage(err)))
		}
		state := &dynamicCordisEffectState{}
		dispose := func() goja.Value {
			return dynamicCordisDisposeEffect(run, state)
		}
		run.disposers = append(run.disposers, func() {
			dynamicCordisTrackCleanup(run, dispose())
		})
		if _, pending := value.Export().(*goja.Promise); pending {
			run.effectSetups++
			dynamicCordisAwaitOnLoop(run, value, func(resolved goja.Value, setupErr error) {
				if setupErr == nil {
					setupErr = dynamicCordisCollectEffectCleanup(run, state, resolved)
				}
				dynamicCordisFinishEffectSetup(run, setupErr)
			})
		} else if iterator, ok, iteratorErr := dynamicCordisEffectIterator(vm, value, false); iteratorErr != nil {
			panic(vm.ToValue(iteratorErr.Error()))
		} else if ok {
			if err := dynamicCordisConsumeEffectIterator(run, state, iterator); err != nil {
				panic(vm.ToValue(err.Error()))
			}
		} else if iterator, ok, iteratorErr := dynamicCordisEffectIterator(vm, value, true); iteratorErr != nil {
			panic(vm.ToValue(iteratorErr.Error()))
		} else if ok {
			run.effectSetups++
			dynamicCordisConsumeAsyncEffectIterator(run, state, iterator, func(setupErr error) {
				dynamicCordisFinishEffectSetup(run, setupErr)
			})
		} else {
			if err := dynamicCordisCollectEffectCleanup(run, state, value); err != nil {
				panic(vm.ToValue(err.Error()))
			}
		}
		return vm.ToValue(func(goja.FunctionCall) goja.Value { return dispose() })
	})
	listen := func(once bool) func(goja.FunctionCall) goja.Value {
		return func(call goja.FunctionCall) goja.Value {
			name := strings.TrimSpace(call.Argument(0).String())
			fn, ok := goja.AssertFunction(call.Argument(1))
			if name == "" || !ok {
				panic(vm.ToValue("ctx.on/once requires a non-empty event name and listener function"))
			}
			e.dynamicCordis.Lock()
			e.dynamicCordis.nextListener++
			seq := e.dynamicCordis.nextListener
			e.dynamicCordis.Unlock()
			options := call.Argument(2)
			prepend := false
			if options != nil && !goja.IsUndefined(options) && !goja.IsNull(options) {
				if object, ok := options.(*goja.Object); ok {
					value := object.Get("prepend")
					prepend = value != nil && !goja.IsUndefined(value) && value.ToBoolean()
				} else {
					prepend = options.ToBoolean()
				}
			}
			listener := &dynamicCordisListener{run: run, fn: fn, once: once, prepend: prepend, seq: seq}
			run.eventMu.Lock()
			run.listeners[name] = append(run.listeners[name], listener)
			run.eventMu.Unlock()
			var disposeOnce sync.Once
			dispose := func() {
				disposeOnce.Do(func() {
					run.eventMu.Lock()
					listeners := run.listeners[name]
					for index, current := range listeners {
						if current == listener {
							listeners = append(listeners[:index], listeners[index+1:]...)
							break
						}
					}
					if len(listeners) == 0 {
						delete(run.listeners, name)
					} else {
						run.listeners[name] = listeners
					}
					run.eventMu.Unlock()
				})
			}
			run.disposers = append(run.disposers, dispose)
			return vm.ToValue(dispose)
		}
	}
	set("on", listen(false))
	set("once", listen(true))
	callbackTimer := func(repeat bool) func(goja.FunctionCall) goja.Value {
		return func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(vm.ToValue("timer callback must be a function"))
			}
			delay := time.Duration(call.Argument(1).ToInteger()) * time.Millisecond
			if delay < 0 {
				delay = 0
			}
			var stateMu sync.Mutex
			stopped := false
			var timer *time.Timer
			var fire func()
			fire = func() {
				stateMu.Lock()
				if stopped {
					stateMu.Unlock()
					return
				}
				if !repeat {
					stopped = true
				}
				stateMu.Unlock()
				e.dynamicCordis.loop.post(func() {
					run.mu.Lock()
					active := !run.disposed && (run.active || run.activating || run.cleaning)
					run.mu.Unlock()
					if active {
						if _, err := fn(goja.Undefined()); err != nil {
							e.reportDynamicCordisHostFailure(run, "timer callback", err)
						}
					}
					if repeat {
						stateMu.Lock()
						if !stopped {
							timer.Reset(delay)
						}
						stateMu.Unlock()
					}
				})
			}
			timer = time.AfterFunc(delay, fire)
			var once sync.Once
			dispose := func() {
				once.Do(func() {
					stateMu.Lock()
					stopped = true
					timer.Stop()
					stateMu.Unlock()
				})
			}
			run.disposers = append(run.disposers, dispose)
			return vm.ToValue(dispose)
		}
	}
	timeoutCallback := callbackTimer(false)
	intervalCallback := callbackTimer(true)
	set("setTimeout", timeoutCallback)
	set("setInterval", intervalCallback)
	set("timeout", func(call goja.FunctionCall) goja.Value {
		if _, ok := goja.AssertFunction(call.Argument(0)); ok {
			return timeoutCallback(call)
		}
		delay := time.Duration(call.Argument(0).ToInteger()) * time.Millisecond
		if delay < 0 {
			delay = 0
		}
		promise, resolve, reject := vm.NewPromise()
		var stateMu sync.Mutex
		settled := false
		timer := time.AfterFunc(delay, func() {
			e.dynamicCordis.loop.post(func() {
				stateMu.Lock()
				if settled {
					stateMu.Unlock()
					return
				}
				settled = true
				stateMu.Unlock()
				if err := resolve(goja.Undefined()); err != nil {
					e.reportDynamicCordisHostFailure(run, "timer promise", err)
				}
			})
		})
		var once sync.Once
		dispose := func() {
			once.Do(func() {
				stateMu.Lock()
				if settled {
					stateMu.Unlock()
					return
				}
				settled = true
				timer.Stop()
				stateMu.Unlock()
				err := reject(vm.NewGoError(errors.New("Context has been disposed")))
				if err != nil {
					e.reportDynamicCordisHostFailure(run, "timer promise", err)
				}
			})
		}
		run.disposers = append(run.disposers, dispose)
		return vm.ToValue(promise)
	})
	set("interval", func(call goja.FunctionCall) goja.Value {
		if _, ok := goja.AssertFunction(call.Argument(0)); ok {
			return intervalCallback(call)
		}
		return e.dynamicCordisIntervalIterator(run, call.Argument(0))
	})
	set("throttle", e.dynamicCordisRateLimit(run, false))
	set("debounce", e.dynamicCordisRateLimit(run, true))
	if tools := e.dynamicCordisToolsFacade(run, owner); tools != nil {
		run.ctxFacade.values["tools"] = tools
	}
	return nil
}

func (e *Engine) dynamicCordisRateLimit(run *dynamicCordisRun, debounce bool) func(goja.FunctionCall) goja.Value {
	vm := run.runtime
	return func(call goja.FunctionCall) goja.Value {
		fn, ok := goja.AssertFunction(call.Argument(0))
		if !ok {
			panic(vm.ToValue("ctx.throttle/debounce requires a callback function"))
		}
		delay := time.Duration(call.Argument(1).ToInteger()) * time.Millisecond
		if delay < 0 {
			delay = 0
		}
		noTrailing := !debounce && call.Argument(2).ToBoolean()
		var stateMu sync.Mutex
		var timer *time.Timer
		var last time.Time
		var pending []goja.Value
		hasPending := false
		disposed := false

		invokeLater := func() {
			e.dynamicCordis.loop.post(func() {
				stateMu.Lock()
				if disposed || !hasPending {
					timer = nil
					stateMu.Unlock()
					return
				}
				args := append([]goja.Value(nil), pending...)
				pending = nil
				hasPending = false
				timer = nil
				last = time.Now()
				stateMu.Unlock()
				run.mu.Lock()
				active := !run.disposed && (run.active || run.activating)
				run.mu.Unlock()
				if !active {
					return
				}
				if _, err := fn(goja.Undefined(), args...); err != nil {
					e.reportDynamicCordisHostFailure(run, "timer callback", err)
				}
			})
		}

		wrapped := vm.ToValue(func(inner goja.FunctionCall) goja.Value {
			args := append([]goja.Value(nil), inner.Arguments...)
			stateMu.Lock()
			if disposed {
				stateMu.Unlock()
				return goja.Undefined()
			}
			now := time.Now()
			if !debounce && (last.IsZero() || now.Sub(last) >= delay) {
				last = now
				stateMu.Unlock()
				value, err := fn(goja.Undefined(), args...)
				if err != nil {
					panic(vm.ToValue(dynamicJSMessage(err)))
				}
				return value
			}
			pending = args
			hasPending = true
			if noTrailing {
				pending = nil
				hasPending = false
				stateMu.Unlock()
				return goja.Undefined()
			}
			wait := delay
			if !debounce && !last.IsZero() {
				wait = max(delay-now.Sub(last), 0)
			}
			if timer == nil {
				timer = time.AfterFunc(wait, invokeLater)
			} else if debounce {
				timer.Reset(wait)
			}
			stateMu.Unlock()
			return goja.Undefined()
		}).ToObject(vm)
		var disposeOnce sync.Once
		dispose := func() {
			disposeOnce.Do(func() {
				stateMu.Lock()
				disposed = true
				pending = nil
				hasPending = false
				if timer != nil {
					timer.Stop()
				}
				stateMu.Unlock()
			})
		}
		_ = wrapped.Set("dispose", dispose)
		run.disposers = append(run.disposers, dispose)
		return wrapped
	}
}

func (e *Engine) dynamicCordisIntervalIterator(run *dynamicCordisRun, delayValue goja.Value) goja.Value {
	vm := run.runtime
	delay := time.Duration(delayValue.ToInteger()) * time.Millisecond
	if delay < 0 {
		delay = 0
	}
	iterator := vm.NewObject()
	var stateMu sync.Mutex
	var timer *time.Timer
	var nextResolve, nextReject func(any) error
	done := ""
	var doneValue any
	result := func(finished bool, value goja.Value) *goja.Object {
		object := vm.NewObject()
		_ = object.Set("done", finished)
		_ = object.Set("value", value)
		return object
	}
	wake := func(resolve func(any) error, value any, phase string) {
		if resolve == nil {
			return
		}
		if err := resolve(value); err != nil {
			e.reportDynamicCordisHostFailure(run, phase, err)
		}
	}
	var tick func()
	tick = func() {
		e.dynamicCordis.loop.post(func() {
			stateMu.Lock()
			if done != "" {
				stateMu.Unlock()
				return
			}
			resolve := nextResolve
			stateMu.Unlock()
			run.mu.Lock()
			active := !run.disposed && (run.active || run.activating)
			run.mu.Unlock()
			if active {
				wake(resolve, result(false, goja.Undefined()), "timer iterator")
			}
			stateMu.Lock()
			if done == "" {
				timer.Reset(delay)
			}
			stateMu.Unlock()
		})
	}
	timer = time.AfterFunc(delay, tick)

	_ = iterator.Set("next", func(goja.FunctionCall) goja.Value {
		stateMu.Lock()
		defer stateMu.Unlock()
		promise, resolve, reject := vm.NewPromise()
		switch done {
		case "return":
			_ = resolve(result(true, vm.ToValue(doneValue)))
		case "throw":
			_ = reject(doneValue)
		default:
			nextResolve, nextReject = resolve, reject
		}
		return vm.ToValue(promise)
	})
	_ = iterator.Set("return", func(call goja.FunctionCall) goja.Value {
		value := call.Argument(0)
		stateMu.Lock()
		if done == "" {
			done = "return"
			doneValue = value
			timer.Stop()
		}
		resolve := nextResolve
		nextResolve, nextReject = nil, nil
		stateMu.Unlock()
		wake(resolve, result(true, value), "timer iterator")
		promise, finish, _ := vm.NewPromise()
		_ = finish(result(true, value))
		return vm.ToValue(promise)
	})
	_ = iterator.Set("throw", func(call goja.FunctionCall) goja.Value {
		reason := call.Argument(0)
		stateMu.Lock()
		if done == "" {
			done = "throw"
			doneValue = reason
			timer.Stop()
		}
		reject := nextReject
		nextResolve, nextReject = nil, nil
		stateMu.Unlock()
		wake(reject, reason, "timer iterator")
		promise, finish, _ := vm.NewPromise()
		_ = finish(result(true, goja.Undefined()))
		return vm.ToValue(promise)
	})
	symbol := vm.Get("Symbol").ToObject(vm).Get("asyncIterator")
	if asyncIterator, ok := symbol.(*goja.Symbol); ok {
		if err := iterator.SetSymbol(asyncIterator, func(goja.FunctionCall) goja.Value { return iterator }); err != nil {
			panic(vm.ToValue(err.Error()))
		}
	} else {
		panic(vm.ToValue(fmt.Sprintf("Symbol.asyncIterator has type %T", symbol)))
	}
	var disposeOnce sync.Once
	dispose := func() {
		disposeOnce.Do(func() {
			reason := vm.NewGoError(errors.New("Context has been disposed"))
			stateMu.Lock()
			if done != "" {
				stateMu.Unlock()
				return
			}
			done = "throw"
			doneValue = reason
			timer.Stop()
			reject := nextReject
			nextResolve, nextReject = nil, nil
			stateMu.Unlock()
			wake(reject, reason, "timer iterator")
		})
	}
	run.disposers = append(run.disposers, dispose)
	return iterator
}

func (e *Engine) emitDynamicCordisEvent(name string, args ...any) error {
	return e.emitDynamicCordisEventFrom(nil, name, args...)
}

func (e *Engine) emitDynamicCordisEventFrom(origin *dynamicCordisRun, name string, args ...any) error {
	return e.dispatchDynamicCordisEvent(origin, "", false, name, args...)
}

func (e *Engine) emitDynamicCordisScopedContained(scope, name string, args ...any) {
	_ = e.dispatchDynamicCordisEvent(nil, scope, true, name, args...)
}

func (e *Engine) dispatchDynamicCordisEvent(origin *dynamicCordisRun, scope string, contained bool, name string, args ...any) error {
	if origin != nil {
		return e.emitDynamicCordisEventNow(scope, contained, name, args...)
	}
	var err error
	if !e.dynamicCordis.loop.call(func() { err = e.emitDynamicCordisEventNow(scope, contained, name, args...) }) {
		return errors.New("dynamic Cordis runtime is closed")
	}
	return err
}

func (e *Engine) emitDynamicCordisEventNow(scope string, contained bool, name string, args ...any) error {
	e.dynamicCordis.RLock()
	listeners := make([]*dynamicCordisListener, 0)
	seen := map[*dynamicCordisRun]struct{}{}
	for _, plugin := range e.dynamicCordis.plugins {
		run := plugin.run
		if run == nil || scope != "" && !run.appliesToSession(scope) {
			continue
		}
		if _, ok := seen[run]; ok {
			continue
		}
		seen[run] = struct{}{}
		run.eventMu.RLock()
		listeners = append(listeners, run.listeners[name]...)
		run.eventMu.RUnlock()
	}
	e.dynamicCordis.RUnlock()
	sort.Slice(listeners, func(i, j int) bool { return dynamicCordisListenerLess(listeners[i], listeners[j]) })
	data, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("event %q arguments are not JSON values: %w", name, err)
	}
	var values []any
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	var invariantErr error
	for _, listener := range listeners {
		run := listener.run
		if listener.once {
			run.eventMu.Lock()
			for index, current := range run.listeners[name] {
				if current != listener {
					continue
				}
				remaining := run.listeners[name]
				remaining = append(remaining[:index], remaining[index+1:]...)
				if len(remaining) == 0 {
					delete(run.listeners, name)
				} else {
					run.listeners[name] = remaining
				}
				break
			}
			run.eventMu.Unlock()
		}
		run.mu.Lock()
		if run.disposed || (!run.active && !run.activating) {
			run.mu.Unlock()
			continue
		}
		run.mu.Unlock()
		jsArgs := make([]goja.Value, len(values))
		for index, value := range values {
			jsArgs[index] = run.runtime.ToValue(value)
		}
		value, callErr := listener.fn(goja.Undefined(), jsArgs...)
		if callErr != nil {
			if failure := dynamicCordisInvariantFailure(run, callErr); failure != nil {
				if invariantErr == nil {
					invariantErr = failure
				}
				if contained {
					continue
				}
				return failure
			}
			e.reportDynamicCordisHostFailure(run, "event "+name, callErr)
			if !contained {
				return errors.New(dynamicJSMessage(callErr))
			}
			continue
		}
		if promise, ok := value.Export().(*goja.Promise); ok {
			dynamicCordisAwaitOnLoop(run, run.runtime.ToValue(promise), func(_ goja.Value, err error) {
				if err != nil {
					e.reportDynamicCordisHostFailure(run, "event "+name, err)
				}
			})
		}
	}
	return invariantErr
}

func dynamicCordisInvariantFailure(run *dynamicCordisRun, err error) error {
	var exception *goja.Exception
	if !errors.As(err, &exception) || dynamicJSValueCode(exception.Value()) != InvariantErrorCode {
		return nil
	}
	message, _ := dynamicJSValueDetails(exception.Value())
	return &InvariantError{Code: InvariantErrorCode, PackageName: run.pluginID, Detail: message}
}

func (e *Engine) reportDynamicCordisHostFailure(run *dynamicCordisRun, phase string, err error) {
	message := dynamicJSMessage(err)
	if !run.claimDynamicCordisRuntimeError("Host\x00" + phase + "\x00" + message) {
		return
	}
	e.steerDynamicCordisFrom(run, run.sessionID, fmt.Sprintf(
		"Cordis Host runtime code in %s/%s (%s) failed during %s.\nmessage: %s\nThe Plugin remains running. Inspect this Package, define a corrected Package on the same Plugin, and activate it autonomously with cordis_run mode:\"update\".",
		run.pluginID, run.packageID, run.runID, phase, message,
	))
}

func (e *Engine) dynamicCordisToolsFacade(run *dynamicCordisRun, owner string) *goja.Object {
	vm := run.runtime
	tools := vm.NewObject()
	_ = tools.Set("get", func(call goja.FunctionCall) goja.Value {
		name := strings.TrimSpace(call.Argument(0).String())
		e.mu.RLock()
		tool, ok := e.tools[name]
		e.mu.RUnlock()
		if !ok {
			return goja.Undefined()
		}
		return vm.ToValue(map[string]any{"name": tool.Schema.Name, "description": tool.Schema.Description, "parameters": cloneJSON(tool.Schema.Parameters)})
	})
	_ = tools.Set("schemas", func(goja.FunctionCall) goja.Value { return vm.ToValue(e.ListTools()) })
	_ = tools.Set("register", func(call goja.FunctionCall) goja.Value {
		object, ok := call.Argument(0).(*goja.Object)
		definition := run.toolDefinitions[object]
		if !ok || definition == nil {
			panic(vm.ToValue("ctx.tools.register requires a tool returned by harness.defineTool(...)"))
		}
		if err := e.registerDynamicCordisTool(run, owner, definition); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(func() { e.unregisterDynamicCordisTool(run, definition.name) })
	})
	return tools
}

func (e *Engine) dynamicCordisServiceValue(target *dynamicCordisRun, name string) goja.Value {
	if dynamicCordisNativeService(name) {
		return e.dynamicCordisNativeServiceValue(target, name)
	}
	if dynamicCordisAgentService(name) {
		return e.dynamicCordisAgentServiceValue(target, name)
	}
	switch name {
	case "tools":
		return e.dynamicCordisToolsFacade(target, target.ownerSessionID())
	case "web":
		return e.dynamicCordisWebFacade(target)
	case "webServer":
		return e.dynamicCordisWebServerFacade(target)
	case "systemPrompt", "timer", "fs", "shell", "shellEnv":
		return e.dynamicCordisBuiltinServiceValue(target, name)
	}
	e.dynamicCordis.RLock()
	service, ok := e.dynamicCordis.services[name]
	e.dynamicCordis.RUnlock()
	if !ok {
		return goja.Undefined()
	}
	return dynamicCordisProxyValue(target, service.owner, service.value)
}

func (e *Engine) dynamicCordisWebFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("search", func(call goja.FunctionCall) goja.Value {
		var request WebSearchRequest
		if err := dynamicCordisDecode(call.Argument(0), &request); err != nil {
			panic(vm.ToValue("ctx.web.search request: " + err.Error()))
		}
		return e.dynamicCordisWebCall(run, "search", request, call.Argument(1))
	})
	_ = service.Set("fetch", func(call goja.FunctionCall) goja.Value {
		var request WebFetchRequest
		if err := dynamicCordisDecode(call.Argument(0), &request); err != nil {
			panic(vm.ToValue("ctx.web.fetch request: " + err.Error()))
		}
		return e.dynamicCordisWebCall(run, "fetch", request, call.Argument(1))
	})
	_ = service.Set("registerSearchProvider", func(call goja.FunctionCall) goja.Value {
		dispose, err := e.registerDynamicCordisWebProvider(run, call.Argument(0), true)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(dispose)
	})
	_ = service.Set("registerFetchProvider", func(call goja.FunctionCall) goja.Value {
		dispose, err := e.registerDynamicCordisWebProvider(run, call.Argument(0), false)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(dispose)
	})
	return service
}

// dynamicCordisWebCall keeps the JS runtime responsive while a provider (possibly
// another dynamic package) performs asynchronous work in the host.
func (e *Engine) dynamicCordisWebCall(run *dynamicCordisRun, kind string, request any, signalValue goja.Value) goja.Value {
	vm := run.runtime
	ctx, cancel, removeAbort, err := dynamicContextFromSignal(run, signalValue, "web "+kind)
	if err != nil {
		panic(vm.ToValue(err.Error()))
	}
	promise, resolve, reject := vm.NewPromise()
	go func() {
		if ctx.Err() != nil {
			_ = e.dynamicCordis.loop.post(func() {
				removeAbort()
				cancel(ctx.Err())
				_ = reject(vm.NewGoError(ctx.Err()))
			})
			return
		}
		var value any
		var callErr error
		switch kind {
		case "search":
			value, callErr = e.webSearch(withWebSession(ctx, run.sessionID), request.(WebSearchRequest))
		case "fetch":
			value, callErr = e.webFetch(ctx, request.(WebFetchRequest))
		default:
			callErr = fmt.Errorf("unknown web operation %q", kind)
		}
		_ = e.dynamicCordis.loop.post(func() {
			removeAbort()
			cancel(callErr)
			if callErr != nil {
				_ = reject(dynamicCordisWebErrorValue(vm, callErr))
				return
			}
			_ = resolve(vm.ToValue(cloneJSON(value)))
		})
	}()
	return vm.ToValue(promise)
}

func (e *Engine) registerDynamicCordisWebProvider(run *dynamicCordisRun, value goja.Value, search bool) (func(), error) {
	object, ok := value.(*goja.Object)
	if !ok {
		return nil, errors.New("web provider must be an object")
	}
	idValue := object.Get("id")
	id, idOK := idValue.Export().(string)
	id = strings.TrimSpace(id)
	if !idOK || id == "" {
		return nil, errors.New("web provider requires a non-empty string id")
	}
	available, ok := goja.AssertFunction(object.Get("available"))
	if !ok {
		return nil, errors.New("web provider requires an available() function")
	}
	operationName := "fetch"
	if search {
		operationName = "search"
	}
	operation, ok := goja.AssertFunction(object.Get(operationName))
	if !ok {
		return nil, fmt.Errorf("web provider requires a %s() function", operationName)
	}
	provider := &dynamicCordisJSWebProvider{id: id, engine: e, run: run, object: object, available: available, operation: operation, search: search}
	var dispose func()
	var err error
	if search {
		dispose, err = e.RegisterWebSearchProvider(provider)
	} else {
		dispose, err = e.RegisterWebFetchProvider(provider)
	}
	if err != nil {
		return nil, err
	}
	run.disposers = append(run.disposers, dispose)
	return dispose, nil
}

type dynamicCordisJSWebProvider struct {
	id        string
	engine    *Engine
	run       *dynamicCordisRun
	object    *goja.Object
	available goja.Callable
	operation goja.Callable
	search    bool
}

func (p *dynamicCordisJSWebProvider) ID() string {
	return p.id
}

func (p *dynamicCordisJSWebProvider) Available() bool {
	var available bool
	if !p.engine.dynamicCordis.loop.call(func() {
		p.run.mu.Lock()
		active := !p.run.disposed && (p.run.active || p.run.activating)
		p.run.mu.Unlock()
		if !active {
			return
		}
		value, err := p.available(p.object)
		available = err == nil && value.ToBoolean()
	}) {
		return false
	}
	return available
}

func (p *dynamicCordisJSWebProvider) Search(ctx context.Context, request WebSearchRequest) (WebSearchResult, error) {
	if !p.search {
		return WebSearchResult{}, errors.New("dynamic web provider does not support search")
	}
	value, err := p.invoke(ctx, request)
	if err != nil {
		return WebSearchResult{}, err
	}
	var result WebSearchResult
	if err := dynamicCordisDecodeJSON(value, &result); err != nil {
		return WebSearchResult{}, &WebError{Code: "WEB_PROVIDER_ERROR", Message: "dynamic web search returned an invalid result"}
	}
	return result, nil
}

func (p *dynamicCordisJSWebProvider) Fetch(ctx context.Context, request WebFetchRequest) (WebFetchResult, error) {
	if p.search {
		return WebFetchResult{}, errors.New("dynamic web provider does not support fetch")
	}
	value, err := p.invoke(ctx, request)
	if err != nil {
		return WebFetchResult{}, err
	}
	var result WebFetchResult
	if err := dynamicCordisDecodeJSON(value, &result); err != nil {
		return WebFetchResult{}, &WebError{Code: "WEB_PROVIDER_ERROR", Message: "dynamic web fetch returned an invalid result"}
	}
	return result, nil
}

func (p *dynamicCordisJSWebProvider) invoke(ctx context.Context, request any) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	type outcome struct {
		value any
		err   error
	}
	result := make(chan outcome, 1)
	if !p.engine.dynamicCordis.loop.post(func() {
		p.run.mu.Lock()
		active := !p.run.disposed && (p.run.active || p.run.activating)
		p.run.mu.Unlock()
		if !active {
			result <- outcome{err: errors.New("dynamic web provider is no longer active")}
			return
		}
		signal, stop := dynamicAuthorizationSignal(p.run, ctx)
		value, callErr := p.operation(p.object, p.run.runtime.ToValue(cloneJSON(request)), signal)
		if callErr != nil {
			stop()
			result <- outcome{err: dynamicCordisWebError(callErr)}
			return
		}
		finish := func(value goja.Value, awaitErr error) {
			stop()
			if awaitErr != nil {
				result <- outcome{err: dynamicCordisWebError(awaitErr)}
				return
			}
			canonical, err := dynamicCordisJSONValue(value)
			if err != nil {
				result <- outcome{err: err}
				return
			}
			result <- outcome{value: canonical}
		}
		dynamicCordisAwaitOnLoop(p.run, value, finish)
	}) {
		return nil, errors.New("dynamic Cordis runtime is closed")
	}
	select {
	case resolved := <-result:
		return resolved.value, resolved.err
	case <-ctx.Done():
		return nil, &WebError{Code: "WEB_ABORTED", Message: "web request aborted"}
	}
}

func dynamicCordisDecodeJSON(value any, target any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func dynamicCordisWebError(err error) error {
	if webErr, ok := err.(*WebError); ok {
		return webErr
	}
	message, _ := dynamicJSErrorDetails(err)
	code := dynamicJSErrorCode(err)
	if message == "" {
		message = err.Error()
	}
	if code == "" {
		code = "WEB_PROVIDER_ERROR"
	}
	return &WebError{Code: code, Message: message}
}

func dynamicJSErrorCode(err error) string {
	var jsErr *dynamicCordisJSError
	if errors.As(err, &jsErr) {
		return jsErr.code
	}
	var exception *goja.Exception
	if errors.As(err, &exception) {
		return dynamicJSValueCode(exception.Value())
	}
	return ""
}

func dynamicCordisWebErrorValue(vm *goja.Runtime, err error) goja.Value {
	value := vm.NewGoError(err)
	var webErr *WebError
	if errors.As(err, &webErr) && webErr.Code != "" {
		_ = value.Set("code", webErr.Code)
	}
	return value
}

func dynamicCordisDecode(value goja.Value, target any) error {
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return errors.New("expected an object")
	}
	data, err := json.Marshal(value.Export())
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func dynamicCordisProxyValue(target, owner *dynamicCordisRun, value goja.Value) goja.Value {
	vm := target.runtime
	if value == nil || goja.IsUndefined(value) {
		return goja.Undefined()
	}
	if goja.IsNull(value) {
		return goja.Null()
	}
	if function, ok := goja.AssertFunction(value); ok {
		return vm.ToValue(func(call goja.FunctionCall) goja.Value {
			return dynamicCordisProxyCall(target, owner, function, goja.Undefined(), call.Arguments)
		})
	}
	object, ok := value.(*goja.Object)
	if !ok {
		return vm.ToValue(value.Export())
	}
	proxy := vm.NewObject()
	for _, key := range dynamicCordisProxyKeys(object) {
		member := object.Get(key)
		if function, ok := goja.AssertFunction(member); ok {
			fn := function
			_ = proxy.Set(key, func(call goja.FunctionCall) goja.Value {
				return dynamicCordisProxyCall(target, owner, fn, object, call.Arguments)
			})
			continue
		}
		_ = proxy.Set(key, dynamicCordisProxyValue(target, owner, member))
	}
	return proxy
}

func dynamicCordisProxyKeys(object *goja.Object) []string {
	seen := map[string]struct{}{}
	keys := make([]string, 0, len(object.Keys()))
	for _, key := range object.Keys() {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for prototype := object.Prototype(); prototype != nil && prototype.Prototype() != nil; prototype = prototype.Prototype() {
		for _, key := range prototype.GetOwnPropertyNames() {
			if key == "constructor" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	return keys
}

func dynamicCordisProxyCall(target, owner *dynamicCordisRun, function goja.Callable, this goja.Value, arguments []goja.Value) goja.Value {
	vm := target.runtime
	if owner == nil || owner.runtime == nil {
		panic(vm.ToValue("dynamic service is unavailable"))
	}
	converted := make([]goja.Value, len(arguments))
	for index, argument := range arguments {
		data, err := json.Marshal(argument.Export())
		if err != nil {
			panic(vm.ToValue("dynamic service arguments must be JSON values"))
		}
		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		converted[index] = owner.runtime.ToValue(value)
	}
	owner.mu.Lock()
	active := !owner.disposed && (owner.active || owner.activating)
	owner.mu.Unlock()
	if !active {
		panic(vm.ToValue("dynamic service provider is not active"))
	}
	result, err := function(this, converted...)
	if err != nil {
		panic(vm.ToValue(dynamicJSMessage(err)))
	}
	promise, ok := result.Export().(*goja.Promise)
	if ok && promise.State() == goja.PromiseStatePending {
		bridged, resolve, reject := vm.NewPromise()
		dynamicCordisAwaitOnLoop(owner, result, func(value goja.Value, err error) {
			if err != nil {
				_ = reject(vm.NewGoError(err))
				return
			}
			if value == nil || goja.IsUndefined(value) {
				_ = resolve(goja.Undefined())
				return
			}
			canonical, err := dynamicCordisJSONValue(value)
			if err != nil {
				_ = reject(vm.NewGoError(err))
				return
			}
			_ = resolve(vm.ToValue(canonical))
		})
		return vm.ToValue(bridged)
	}
	if result == nil || goja.IsUndefined(result) {
		return goja.Undefined()
	}
	canonical, err := dynamicCordisJSONValue(result)
	if err != nil {
		panic(vm.ToValue(err.Error()))
	}
	return vm.ToValue(canonical)
}

func (e *Engine) removeDynamicCordisService(owner *dynamicCordisRun, name string) {
	e.dynamicCordis.Lock()
	service, ok := e.dynamicCordis.services[name]
	if ok && service.owner == owner {
		delete(e.dynamicCordis.services, name)
		delete(owner.provided, name)
	}
	e.dynamicCordis.Unlock()
	if ok && service.owner == owner {
		e.deactivateDynamicCordisDependents(name, owner)
	}
}

func (e *Engine) activateDynamicCordisRun(run *dynamicCordisRun, owner string) ([]string, error) {
	type result struct {
		waiting []string
		err     error
	}
	ready := make(chan result, 1)
	if !e.dynamicCordis.loop.post(func() {
		e.activateDynamicCordisRunNow(run, owner, func(waiting []string, err error) {
			ready <- result{waiting: waiting, err: err}
		})
	}) {
		return nil, errors.New("dynamic Cordis runtime is closed")
	}
	resolved := <-ready
	return resolved.waiting, resolved.err
}

func (e *Engine) activateDynamicCordisRunNow(run *dynamicCordisRun, owner string, finish func([]string, error)) {
	missing := e.dynamicCordisMissingServices(run)
	if len(missing) > 0 {
		e.setDynamicCordisRunWaiting(run, missing)
		finish(missing, nil)
		return
	}
	run.mu.Lock()
	if run.disposed || run.active || run.activating {
		run.mu.Unlock()
		finish(nil, nil)
		return
	}
	run.activating = true
	run.mu.Unlock()
	if err := e.configureDynamicCordisContext(run, owner); err != nil {
		run.mu.Lock()
		run.activating = false
		run.mu.Unlock()
		finish(nil, err)
		return
	}
	complete := func(err error) {
		if err != nil {
			run.mu.Lock()
			shouldCleanup := run.activating
			run.activating = false
			run.mu.Unlock()
			if shouldCleanup {
				e.cleanupDynamicCordisRunNow(run, false)
			}
			finish(nil, err)
			return
		}
		run.mu.Lock()
		if run.disposed {
			run.activating = false
			run.mu.Unlock()
			finish(nil, errors.New("dynamic package is no longer active"))
			return
		}
		run.active = true
		run.activating = false
		run.mu.Unlock()
		e.setDynamicCordisRunWaiting(run, nil)
		finish(nil, nil)
	}
	completeApply := func(err error) {
		if err != nil {
			complete(err)
			return
		}
		dynamicCordisAwaitEffectSetups(run, complete)
	}
	if run.apply == nil {
		completeApply(nil)
		return
	}
	value, err := dynamicCordisCallBounded(run.runtime, run.vmTimeout, func() (goja.Value, error) {
		return run.apply(run.plugin, run.ctx)
	})
	if err != nil {
		completeApply(errors.New(dynamicJSMessage(err)))
		return
	}
	if goja.IsUndefined(value) {
		completeApply(nil)
		return
	}
	dynamicCordisAwaitOnLoop(run, value, func(_ goja.Value, err error) { completeApply(err) })
}

func (e *Engine) activateAvailableDynamicCordisRuns() {
	for {
		e.dynamicCordis.RLock()
		type candidate struct {
			run   *dynamicCordisRun
			owner string
		}
		candidates := make([]candidate, 0)
		for _, plugin := range e.dynamicCordis.plugins {
			if plugin.run != nil {
				candidates = append(candidates, candidate{run: plugin.run, owner: plugin.run.ownerSessionID()})
			}
		}
		e.dynamicCordis.RUnlock()
		activated := false
		for _, item := range candidates {
			item.run.mu.Lock()
			eligible := !item.run.disposed && !item.run.active && !item.run.activating
			item.run.mu.Unlock()
			if !eligible || len(e.dynamicCordisMissingServices(item.run)) > 0 {
				continue
			}
			if _, err := e.activateDynamicCordisRun(item.run, item.owner); err == nil {
				activated = true
			}
		}
		if !activated {
			return
		}
	}
}

func (e *Engine) setDynamicCordisRunWaiting(run *dynamicCordisRun, missing []string) {
	e.dynamicCordis.Lock()
	defer e.dynamicCordis.Unlock()
	for _, plugin := range e.dynamicCordis.plugins {
		if plugin.run != run || dynamicCordisAttemptRunID(plugin.latest) != run.runID {
			continue
		}
		status := "running"
		if len(missing) > 0 {
			status = "waiting"
		}
		dynamicCordisSetAttemptHalf(plugin.latest, "host", status, missing, "")
		if plugin.packages[run.packageID].client == "" {
			plugin.latest["status"] = status
		}
		return
	}
}

func (e *Engine) cleanupDynamicCordisRun(run *dynamicCordisRun, final bool) {
	if run == nil {
		return
	}
	e.dynamicCordis.loop.call(func() { e.cleanupDynamicCordisRunNow(run, final) })
}

func (e *Engine) cleanupDynamicCordisRunNow(run *dynamicCordisRun, final bool) {
	run.mu.Lock()
	run.active = false
	run.activating = false
	run.cleaning = true
	disposers := append([]func(){}, run.disposers...)
	run.disposers = nil
	if final {
		disposers = append(disposers, run.finalDisposers...)
		run.finalDisposers = nil
	}
	provided := make([]string, 0, len(run.provided))
	for name := range run.provided {
		provided = append(provided, name)
	}
	run.mu.Unlock()
	e.jobs.releaseControllers(run.sessionID, run)
	for index := len(disposers) - 1; index >= 0; index-- {
		disposers[index]()
		for len(run.cleanupWaits) > 0 {
			wait := run.cleanupWaits[0]
			if !e.dynamicCordis.loop.pumpUntil(func() bool { return wait.done }) {
				break
			}
			run.cleanupWaits[0] = nil
			run.cleanupWaits = run.cleanupWaits[1:]
		}
	}
	run.mu.Lock()
	run.cleaning = false
	if final {
		run.disposed = true
	}
	run.mu.Unlock()
	pendingErr := errors.New("dynamic package lifecycle changed while JavaScript was pending")
	if final {
		pendingErr = errors.New("dynamic package is no longer active")
	}
	for wait := range run.pending {
		wait.cancel(pendingErr)
	}
	run.toolMu.Lock()
	names := make([]string, 0, len(run.toolNames))
	for name := range run.toolNames {
		names = append(names, name)
	}
	run.toolNames = map[string]struct{}{}
	run.toolMu.Unlock()
	for _, name := range names {
		_, _ = e.unregisterToolFrom(run, name)
	}
	for _, name := range provided {
		e.removeDynamicCordisService(run, name)
	}
}

func (e *Engine) deactivateDynamicCordisDependents(service string, provider *dynamicCordisRun) {
	e.dynamicCordis.RLock()
	runs := make([]*dynamicCordisRun, 0)
	for _, plugin := range e.dynamicCordis.plugins {
		run := plugin.run
		if run == nil || run == provider {
			continue
		}
		for _, dependency := range run.inject {
			if dependency == service {
				runs = append(runs, run)
				break
			}
		}
	}
	e.dynamicCordis.RUnlock()
	for _, run := range runs {
		e.cleanupDynamicCordisRunNow(run, false)
		e.setDynamicCordisRunWaiting(run, e.dynamicCordisMissingServices(run))
	}
}
