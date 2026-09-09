package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/dop251/goja"
)

func dynamicCordisNativeService(name string) bool {
	switch name {
	case "agentDefaultModel", "authorization", "credentials", "goals", "jobs", "messageFeedback", "permissionPresets", "sandboxPolicy", "sessionProjections", "sessionReferenceResolver", "sessions", "spillStore":
		return true
	default:
		return false
	}
}

func (e *Engine) dynamicCordisNativeServiceValue(run *dynamicCordisRun, name string) goja.Value {
	switch name {
	case "agentDefaultModel":
		return e.dynamicCordisAgentDefaultModelFacade(run)
	case "authorization":
		return e.dynamicCordisAuthorizationFacade(run)
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
	case "sessionProjections":
		return e.dynamicCordisSessionProjectionsFacade(run)
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

type dynamicAuthorizationAbortListener struct {
	value goja.Value
	call  goja.Callable
}

func dynamicAuthorizationErrorValue(vm *goja.Runtime, err error) goja.Value {
	value := vm.NewGoError(err)
	var authorizationErr *AuthorizationError
	var declinedErr *AuthorizationDeclinedError
	var invariantErr *InvariantError
	switch {
	case errors.As(err, &authorizationErr):
		_ = value.Set("name", "AuthorizationError")
		_ = value.Set("code", authorizationErr.Code)
	case errors.As(err, &declinedErr):
		_ = value.Set("name", "AuthorizationDeclinedError")
		_ = value.Set("code", AuthorizationDeclined)
	case errors.As(err, &invariantErr):
		_ = value.Set("name", "InvariantError")
		_ = value.Set("code", invariantErr.Code)
	}
	return value
}

func dynamicAuthorizationJavaScriptError(err error) error {
	var exception *goja.Exception
	if !errors.As(err, &exception) {
		return err
	}
	message, stack := dynamicJSValueDetails(exception.Value())
	jsErr := &dynamicCordisJSError{message: message, stack: stack, code: dynamicJSValueCode(exception.Value())}
	if jsErr.code == AuthorizationDeclined {
		return &AuthorizationDeclinedError{Message: message, Cause: jsErr}
	}
	return jsErr
}

func dynamicAuthorizationSignal(run *dynamicCordisRun, ctx context.Context) (*goja.Object, func()) {
	vm := run.runtime
	signal := vm.NewObject()
	listeners := []dynamicAuthorizationAbortListener{}
	aborted := false
	abort := func() {
		if aborted {
			return
		}
		aborted = true
		cause := context.Cause(ctx)
		if cause == nil {
			cause = context.Canceled
		}
		_ = signal.Set("aborted", true)
		_ = signal.Set("reason", vm.NewGoError(cause))
		for _, listener := range append([]dynamicAuthorizationAbortListener(nil), listeners...) {
			if _, err := listener.call(goja.Undefined()); err != nil {
				log.Printf("deepseek-harness: authorization abort listener failed: %v", err)
			}
		}
		listeners = nil
	}
	_ = signal.Set("aborted", false)
	_ = signal.Set("reason", goja.Undefined())
	_ = signal.Set("addEventListener", func(call goja.FunctionCall) goja.Value {
		if call.Argument(0).String() != "abort" {
			return goja.Undefined()
		}
		if listener, ok := goja.AssertFunction(call.Argument(1)); ok {
			listeners = append(listeners, dynamicAuthorizationAbortListener{value: call.Argument(1), call: listener})
		}
		return goja.Undefined()
	})
	_ = signal.Set("removeEventListener", func(call goja.FunctionCall) goja.Value {
		if call.Argument(0).String() != "abort" {
			return goja.Undefined()
		}
		value := call.Argument(1)
		kept := listeners[:0]
		for _, listener := range listeners {
			if !listener.value.StrictEquals(value) {
				kept = append(kept, listener)
			}
		}
		listeners = kept
		return goja.Undefined()
	})
	_ = signal.Set("throwIfAborted", func(goja.FunctionCall) goja.Value {
		if aborted {
			panic(signal.Get("reason"))
		}
		return goja.Undefined()
	})
	stop := context.AfterFunc(ctx, func() { _ = run.ctxFacade.engine.dynamicCordis.loop.post(abort) })
	if ctx.Err() != nil {
		abort()
	}
	return signal, func() { stop() }
}

func dynamicContextFromSignal(run *dynamicCordisRun, value goja.Value, subject string) (context.Context, context.CancelCauseFunc, func(), error) {
	ctx, cancel := context.WithCancelCause(context.Background())
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return ctx, cancel, func() {}, nil
	}
	signal, ok := value.(*goja.Object)
	if !ok {
		cancel(context.Canceled)
		return nil, nil, nil, errors.New(subject + " signal must be an AbortSignal")
	}
	if signal.Get("aborted").ToBoolean() {
		cancel(errors.New(subject + " request aborted"))
		return ctx, cancel, func() {}, nil
	}
	add, addOK := goja.AssertFunction(signal.Get("addEventListener"))
	remove, removeOK := goja.AssertFunction(signal.Get("removeEventListener"))
	if !addOK {
		return ctx, cancel, func() {}, nil
	}
	onAbort := run.runtime.ToValue(func(goja.FunctionCall) goja.Value {
		cancel(errors.New(subject + " request aborted"))
		return goja.Undefined()
	})
	if _, err := add(signal, run.runtime.ToValue("abort"), onAbort, run.runtime.ToValue(map[string]any{"once": true})); err != nil {
		cancel(context.Canceled)
		return nil, nil, nil, err
	}
	removeAbort := func() {
		if removeOK {
			_, _ = remove(signal, run.runtime.ToValue("abort"), onAbort)
		}
	}
	return ctx, cancel, removeAbort, nil
}

func dynamicAuthorizationContextFromSignal(run *dynamicCordisRun, value goja.Value) (context.Context, context.CancelCauseFunc, func(), error) {
	return dynamicContextFromSignal(run, value, "authorization")
}

func dynamicSessionReferenceErrorValue(vm *goja.Runtime, err error) goja.Value {
	value := vm.NewGoError(err)
	var reference *SessionReferenceError
	if errors.As(err, &reference) {
		_ = value.Set("name", "SessionReferenceError")
		_ = value.Set("code", reference.Code)
	}
	return value
}

func dynamicAuthorizationFlow(run *dynamicCordisRun, value goja.Value) (AuthorizationFlow, goja.Callable, error) {
	object, ok := value.(*goja.Object)
	if !ok {
		return AuthorizationFlow{}, nil, errors.New("ctx.authorization.registerFlow requires a flow object")
	}
	key, err := ParseCredentialKey(strings.TrimSpace(object.Get("key").String()))
	if err != nil {
		return AuthorizationFlow{}, nil, err
	}
	var methods []AuthorizationMethod
	if err := dynamicCordisDecode(object.Get("methods"), &methods); err != nil {
		return AuthorizationFlow{}, nil, fmt.Errorf("authorization flow methods: %w", err)
	}
	runFlow, ok := goja.AssertFunction(object.Get("run"))
	if !ok {
		return AuthorizationFlow{}, nil, errors.New("authorization flow run must be a function")
	}
	return AuthorizationFlow{
		Key: key, Label: strings.TrimSpace(object.Get("label").String()), Methods: methods,
	}, runFlow, nil
}

func dynamicAuthorizationPrompt(run *dynamicCordisRun, value goja.Value) (AuthorizationPrompt, context.CancelCauseFunc, func(), error) {
	object, ok := value.(*goja.Object)
	if !ok {
		return AuthorizationPrompt{}, nil, nil, errors.New("authorization prompt must be an object")
	}
	prompt := AuthorizationPrompt{
		Kind:    AuthorizationPromptKind(strings.TrimSpace(object.Get("kind").String())),
		Message: strings.TrimSpace(object.Get("message").String()),
	}
	if placeholder := object.Get("placeholder"); placeholder != nil && !goja.IsUndefined(placeholder) && !goja.IsNull(placeholder) {
		prompt.Placeholder = placeholder.String()
	}
	if options := object.Get("options"); options != nil && !goja.IsUndefined(options) && !goja.IsNull(options) {
		if err := dynamicCordisDecode(options, &prompt.Options); err != nil {
			return AuthorizationPrompt{}, nil, nil, fmt.Errorf("authorization prompt options: %w", err)
		}
	}
	if prompt.Message == "" {
		return AuthorizationPrompt{}, nil, nil, errors.New("authorization prompt message must be non-empty")
	}
	switch prompt.Kind {
	case AuthorizationPromptText, AuthorizationPromptSecret:
	case AuthorizationPromptSelect:
		if len(prompt.Options) == 0 {
			return AuthorizationPrompt{}, nil, nil, errors.New("authorization select prompt needs options")
		}
	default:
		return AuthorizationPrompt{}, nil, nil, fmt.Errorf("unknown authorization prompt kind %q", prompt.Kind)
	}
	signal := object.Get("signal")
	if signal == nil || goja.IsUndefined(signal) || goja.IsNull(signal) {
		return prompt, func(error) {}, func() {}, nil
	}
	ctx, cancel, removeAbort, err := dynamicAuthorizationContextFromSignal(run, signal)
	if err != nil {
		return AuthorizationPrompt{}, nil, nil, err
	}
	prompt.Context = ctx
	return prompt, cancel, removeAbort, nil
}

func dynamicAuthorizationPromptValue(run *dynamicCordisRun, ctx context.Context, prompt AuthorizationPrompt) (goja.Value, func()) {
	value := run.runtime.ToValue(cloneJSON(prompt)).ToObject(run.runtime)
	if prompt.Context == nil {
		return value, func() {}
	}
	signal, stop := dynamicAuthorizationSignal(run, ctx)
	_ = value.Set("signal", signal)
	return value, stop
}

func dynamicAuthorizationRunFlow(e *Engine, run *dynamicCordisRun, runFlow goja.Callable, session AuthorizationSession) error {
	result := make(chan error, 1)
	if !e.dynamicCordis.loop.post(func() {
		run.mu.Lock()
		active := !run.disposed && (run.active || run.activating)
		run.mu.Unlock()
		if !active {
			result <- errors.New("dynamic package is no longer active")
			return
		}
		vm := run.runtime
		signal, stopSignal := dynamicAuthorizationSignal(run, session.Context)
		jsSession := vm.NewObject()
		_ = jsSession.Set("method", session.Method)
		_ = jsSession.Set("signal", signal)
		_ = jsSession.Set("notify", func(call goja.FunctionCall) goja.Value {
			var notice AuthorizationNotice
			if err := dynamicCordisDecode(call.Argument(0), &notice); err != nil {
				panic(vm.ToValue("authorization notice: " + err.Error()))
			}
			session.Notify(notice)
			return goja.Undefined()
		})
		_ = jsSession.Set("prompt", func(call goja.FunctionCall) goja.Value {
			prompt, cancelPrompt, removeAbort, err := dynamicAuthorizationPrompt(run, call.Argument(0))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			promise, resolve, reject := vm.NewPromise()
			go func() {
				answer, promptErr := session.Prompt(prompt)
				_ = e.dynamicCordis.loop.post(func() {
					removeAbort()
					cancelPrompt(context.Canceled)
					if promptErr != nil {
						_ = reject(dynamicAuthorizationErrorValue(vm, promptErr))
						return
					}
					_ = resolve(vm.ToValue(answer))
				})
			}()
			return vm.ToValue(promise)
		})
		value, err := runFlow(goja.Undefined(), jsSession)
		if err != nil {
			stopSignal()
			result <- errors.New(dynamicJSMessage(err))
			return
		}
		dynamicCordisAwaitOnLoop(run, value, func(_ goja.Value, err error) {
			stopSignal()
			result <- err
		})
	}) {
		return errors.New("dynamic Cordis runtime is closed")
	}
	return <-result
}

func dynamicAuthorizationInteraction(e *Engine, run *dynamicCordisRun, value goja.Value) (AuthorizationInteraction, error) {
	object, ok := value.(*goja.Object)
	if !ok {
		return AuthorizationInteraction{}, errors.New("authorization interaction must be an object")
	}
	notify, notifyOK := goja.AssertFunction(object.Get("notify"))
	prompt, promptOK := goja.AssertFunction(object.Get("prompt"))
	if !notifyOK || !promptOK {
		return AuthorizationInteraction{}, errors.New("authorization interaction requires notify and prompt functions")
	}
	return AuthorizationInteraction{
		Notify: func(notice AuthorizationNotice) {
			_ = e.dynamicCordis.loop.post(func() {
				if _, err := notify(object, run.runtime.ToValue(cloneJSON(notice))); err != nil {
					log.Printf("deepseek-harness: authorization interaction failed to render a notice: %v", err)
				}
			})
		},
		Prompt: func(ctx context.Context, question AuthorizationPrompt) (string, error) {
			type promptResult struct {
				answer string
				err    error
			}
			settled := make(chan promptResult, 1)
			if !e.dynamicCordis.loop.post(func() {
				questionValue, stopSignal := dynamicAuthorizationPromptValue(run, ctx, question)
				value, err := prompt(object, questionValue)
				if err != nil {
					stopSignal()
					settled <- promptResult{err: dynamicAuthorizationJavaScriptError(err)}
					return
				}
				dynamicCordisAwaitOnLoop(run, value, func(value goja.Value, err error) {
					stopSignal()
					if err != nil {
						var jsErr *dynamicCordisJSError
						if errors.As(err, &jsErr) && jsErr.code == AuthorizationDeclined {
							err = &AuthorizationDeclinedError{Message: jsErr.message, Cause: err}
						}
						settled <- promptResult{err: err}
						return
					}
					answer, ok := value.Export().(string)
					if !ok {
						settled <- promptResult{err: errors.New("authorization prompt must resolve to a string")}
						return
					}
					settled <- promptResult{answer: answer}
				})
			}) {
				return "", errors.New("dynamic Cordis runtime is closed")
			}
			select {
			case result := <-settled:
				return result.answer, result.err
			case <-ctx.Done():
				return "", context.Cause(ctx)
			}
		},
	}, nil
}

func (e *Engine) dynamicCordisAuthorizationFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("registerFlow", func(call goja.FunctionCall) goja.Value {
		flow, runFlow, err := dynamicAuthorizationFlow(run, call.Argument(0))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		flow.Run = func(session AuthorizationSession) error {
			return dynamicAuthorizationRunFlow(e, run, runFlow, session)
		}
		dispose, err := e.Authorization().RegisterFlow(flow)
		if err != nil {
			panic(dynamicAuthorizationErrorValue(vm, err))
		}
		run.disposers = append(run.disposers, dispose)
		return vm.ToValue(dispose)
	})
	_ = service.Set("list", func(goja.FunctionCall) goja.Value {
		return vm.ToValue(cloneJSON(e.Authorization().List()))
	})
	_ = service.Set("describe", func(call goja.FunctionCall) goja.Value {
		key, err := ParseCredentialKey(strings.TrimSpace(call.Argument(0).String()))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		entry := e.Authorization().Describe(key)
		if entry == nil {
			return goja.Undefined()
		}
		return vm.ToValue(cloneJSON(entry))
	})
	_ = service.Set("cancel", func(call goja.FunctionCall) goja.Value {
		key, err := ParseCredentialKey(strings.TrimSpace(call.Argument(0).String()))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		e.Authorization().Cancel(key)
		return goja.Undefined()
	})
	_ = service.Set("begin", func(call goja.FunctionCall) goja.Value {
		request, ok := call.Argument(0).(*goja.Object)
		if !ok {
			panic(vm.ToValue("ctx.authorization.begin requires a request object"))
		}
		key, err := ParseCredentialKey(strings.TrimSpace(request.Get("key").String()))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		method := ""
		if value := request.Get("method"); value != nil && !goja.IsUndefined(value) && !goja.IsNull(value) {
			method = strings.TrimSpace(value.String())
		}
		interaction, err := dynamicAuthorizationInteraction(e, run, request.Get("interaction"))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		ctx, cancel, removeAbort, err := dynamicAuthorizationContextFromSignal(run, request.Get("signal"))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		promise, resolve, reject := vm.NewPromise()
		go func() {
			outcome, beginErr := e.Authorization().Begin(ctx, AuthorizationRequest{Key: key, Method: method, Interaction: interaction})
			_ = e.dynamicCordis.loop.post(func() {
				removeAbort()
				cancel(context.Canceled)
				if beginErr != nil {
					_ = reject(dynamicAuthorizationErrorValue(vm, beginErr))
					return
				}
				_ = resolve(vm.ToValue(cloneJSON(outcome)))
			})
		}()
		return vm.ToValue(promise)
	})
	return service
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
	_ = service.Set("readRecord", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			key, err := ParseCredentialKey(strings.TrimSpace(call.Argument(0).String()))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			record, err := e.Credentials().ReadRecord(key)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			if record == nil {
				return goja.Undefined()
			}
			return vm.ToValue(cloneJSON(record))
		})
	})
	_ = service.Set("describeRecord", func(call goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			key, err := ParseCredentialKey(strings.TrimSpace(call.Argument(0).String()))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			info, err := e.Credentials().DescribeRecord(key)
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return vm.ToValue(cloneJSON(info))
		})
	})
	_ = service.Set("listRecords", func(goja.FunctionCall) goja.Value {
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			return vm.ToValue(cloneJSON(e.Credentials().ListRecords()))
		})
	})
	_ = service.Set("modifyRecord", func(call goja.FunctionCall) goja.Value {
		key, err := ParseCredentialKey(strings.TrimSpace(call.Argument(0).String()))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		mutate, ok := goja.AssertFunction(call.Argument(1))
		if !ok {
			panic(vm.ToValue("ctx.credentials.modifyRecord mutate must be a function"))
		}
		promise, resolve, reject := vm.NewPromise()
		go func() {
			record, modifyErr := e.Credentials().ModifyRecord(context.Background(), key, func(_ context.Context, current *CredentialRecord) (*CredentialRecord, error) {
				type mutationResult struct {
					record *CredentialRecord
					err    error
				}
				settled := make(chan mutationResult, 1)
				if !e.dynamicCordis.loop.post(func() {
					currentValue := goja.Undefined()
					if current != nil {
						currentValue = vm.ToValue(cloneJSON(current))
					}
					value, callErr := mutate(goja.Undefined(), currentValue)
					if callErr != nil {
						settled <- mutationResult{err: callErr}
						return
					}
					dynamicCordisAwaitOnLoop(run, value, func(value goja.Value, awaitErr error) {
						if awaitErr != nil {
							settled <- mutationResult{err: awaitErr}
							return
						}
						if value == nil || goja.IsUndefined(value) {
							settled <- mutationResult{}
							return
						}
						var next CredentialRecord
						if err := dynamicCordisDecode(value, &next); err != nil {
							settled <- mutationResult{err: fmt.Errorf("ctx.credentials.modifyRecord result: %w", err)}
							return
						}
						settled <- mutationResult{record: &next}
					})
				}) {
					return nil, fmt.Errorf("dynamic Cordis runtime is closed")
				}
				result := <-settled
				return result.record, result.err
			})
			_ = e.dynamicCordis.loop.post(func() {
				if modifyErr != nil {
					if err := reject(vm.NewGoError(modifyErr)); err != nil {
						e.reportDynamicCordisHostFailure(run, "credentials modifyRecord", err)
					}
					return
				}
				value := goja.Undefined()
				if record != nil {
					value = vm.ToValue(cloneJSON(record))
				}
				if err := resolve(value); err != nil {
					e.reportDynamicCordisHostFailure(run, "credentials modifyRecord", err)
				}
			})
		}()
		return vm.ToValue(promise)
	})
	_ = service.Set("deleteRecord", func(call goja.FunctionCall) goja.Value {
		key, err := ParseCredentialKey(strings.TrimSpace(call.Argument(0).String()))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		promise, resolve, reject := vm.NewPromise()
		go func() {
			deleteErr := e.Credentials().DeleteRecord(context.Background(), key)
			_ = e.dynamicCordis.loop.post(func() {
				if deleteErr != nil {
					if err := reject(vm.NewGoError(deleteErr)); err != nil {
						e.reportDynamicCordisHostFailure(run, "credentials deleteRecord", err)
					}
					return
				}
				if err := resolve(goja.Undefined()); err != nil {
					e.reportDynamicCordisHostFailure(run, "credentials deleteRecord", err)
				}
			})
		}()
		return vm.ToValue(promise)
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
				value, rpcErr := e.remoteMessageFeedbackFrom("messageFeedback/"+method, map[string]json.RawMessage{"request": request}, run)
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
		targetID := dynamicCordisSessionID(vm, call.Argument(0))
		query := ""
		if value := call.Argument(1); value != nil && !goja.IsUndefined(value) && !goja.IsNull(value) {
			query = value.String()
		}
		config, err := e.cfg.SessionReference.normalized()
		if err != nil {
			panic(dynamicSessionReferenceErrorValue(vm, err))
		}
		limit := config.CandidateLimit
		if value := call.Argument(2); value != nil && !goja.IsUndefined(value) && !goja.IsNull(value) {
			number := value.ToFloat()
			if math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || math.Abs(number) > 9_007_199_254_740_991 {
				err := sessionReferenceError(SessionReferenceInvalidReference, "candidate limit must be a positive safe integer", nil)
				panic(dynamicSessionReferenceErrorValue(vm, err))
			}
			limit = int(number)
		}
		ctx, cancel, removeAbort, err := dynamicContextFromSignal(run, call.Argument(3), "session reference")
		if err != nil {
			panic(vm.NewGoError(err))
		}
		promise, resolve, reject := vm.NewPromise()
		go func() {
			rows, listErr := e.ListSessionReferenceCandidates(ctx, targetID, query, limit, config)
			_ = e.dynamicCordis.loop.post(func() {
				removeAbort()
				cancel(context.Canceled)
				if listErr != nil {
					_ = reject(dynamicSessionReferenceErrorValue(vm, listErr))
					return
				}
				_ = resolve(vm.ToValue(cloneJSON(rows)))
			})
		}()
		return vm.ToValue(promise)
	})
	_ = service.Set("prepare", func(call goja.FunctionCall) goja.Value {
		targetID := dynamicCordisSessionID(vm, call.Argument(0))
		var content []ContentBlock
		if err := dynamicCordisDecode(call.Argument(1), &content); err != nil {
			panic(vm.ToValue("ctx.sessionReferenceResolver.prepare content: " + err.Error()))
		}
		var references []SessionReferenceInput
		if err := dynamicCordisDecode(call.Argument(2), &references); err != nil {
			referenceErr := sessionReferenceError(SessionReferenceInvalidReference, err.Error(), err)
			panic(dynamicSessionReferenceErrorValue(vm, referenceErr))
		}
		ctx, cancel, removeAbort, err := dynamicContextFromSignal(run, call.Argument(3), "session reference")
		if err != nil {
			panic(vm.NewGoError(err))
		}
		promise, resolve, reject := vm.NewPromise()
		go func() {
			prepared, prepareErr := e.PrepareSessionReferences(ctx, targetID, content, references, e.cfg.SessionReference)
			_ = e.dynamicCordis.loop.post(func() {
				removeAbort()
				cancel(context.Canceled)
				if prepareErr != nil {
					_ = reject(dynamicSessionReferenceErrorValue(vm, prepareErr))
					return
				}
				_ = resolve(vm.ToValue(cloneJSON(prepared)))
			})
		}()
		return vm.ToValue(promise)
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
