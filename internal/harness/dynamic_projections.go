package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/dop251/goja"
)

func dynamicProjectionClone(value any) (any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var cloned any
	if err := json.Unmarshal(data, &cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

func dynamicProjectionSyncValue(vm *goja.Runtime, value goja.Value, operation string) any {
	if value == nil {
		panic(vm.ToValue(operation + " must return plain JSON state"))
	}
	if _, ok := value.Export().(*goja.Promise); ok {
		panic(vm.ToValue(operation + " must be synchronous"))
	}
	cloned, err := dynamicProjectionClone(value.Export())
	if err != nil {
		panic(vm.ToValue(operation + " must return plain JSON state"))
	}
	return cloned
}

func dynamicProjectionDefinition(run *dynamicCordisRun, value goja.Value) (ProjectionDefinition, error) {
	object, ok := value.(*goja.Object)
	if !ok {
		return ProjectionDefinition{}, errors.New("ctx.sessionProjections.register requires a definition object")
	}
	key := strings.TrimSpace(object.Get("key").String())
	if key == "" {
		return ProjectionDefinition{}, errors.New("session projection key is required")
	}
	versionValue := object.Get("stateVersion")
	versionNumber := versionValue.ToFloat()
	if math.IsNaN(versionNumber) || math.IsInf(versionNumber, 0) || versionNumber < 0 || math.Trunc(versionNumber) != versionNumber || versionNumber > float64(int(^uint(0)>>1)) {
		return ProjectionDefinition{}, fmt.Errorf("session projection %q stateVersion must be a non-negative integer", key)
	}
	init, initOK := goja.AssertFunction(object.Get("init"))
	apply, applyOK := goja.AssertFunction(object.Get("apply"))
	if !initOK || !applyOK {
		return ProjectionDefinition{}, fmt.Errorf("session projection %q requires init and apply", key)
	}
	definition := ProjectionDefinition{
		Key: key, StateVersion: int(versionNumber), dynamicRuntime: true,
		Init: func() any {
			value, err := dynamicCordisCallBounded(run.runtime, run.vmTimeout, func() (goja.Value, error) {
				return init(goja.Undefined())
			})
			if err != nil {
				panic(dynamicJSMessage(err))
			}
			return dynamicProjectionSyncValue(run.runtime, value, "session projection "+key+" init")
		},
		Apply: func(state any, event Event) ProjectionResult {
			inputState, err := dynamicProjectionClone(state)
			if err != nil {
				panic(err)
			}
			eventValue, err := dynamicProjectionClone(dynamicSessionEventValue(event))
			if err != nil {
				panic(err)
			}
			input := run.runtime.ToValue(inputState)
			value, err := dynamicCordisCallBounded(run.runtime, run.vmTimeout, func() (goja.Value, error) {
				return apply(goja.Undefined(), input, run.runtime.ToValue(eventValue))
			})
			if err != nil {
				panic(dynamicJSMessage(err))
			}
			return ProjectionResult{
				State:   dynamicProjectionSyncValue(run.runtime, value, "session projection "+key+" apply"),
				Changed: !value.StrictEquals(input),
			}
		},
	}
	wireValue := object.Get("wire")
	if wireValue == nil || goja.IsUndefined(wireValue) || goja.IsNull(wireValue) {
		return definition, nil
	}
	wire, ok := wireValue.(*goja.Object)
	if !ok {
		return ProjectionDefinition{}, fmt.Errorf("session projection %q wire must be an object", key)
	}
	view, ok := goja.AssertFunction(wire.Get("view"))
	if !ok {
		return ProjectionDefinition{}, fmt.Errorf("session projection %q wire requires view", key)
	}
	definition.View = func(state any) any {
		input, err := dynamicProjectionClone(state)
		if err != nil {
			panic(err)
		}
		value, err := dynamicCordisCallBounded(run.runtime, run.vmTimeout, func() (goja.Value, error) {
			return view(goja.Undefined(), run.runtime.ToValue(input))
		})
		if err != nil {
			panic(dynamicJSMessage(err))
		}
		return dynamicProjectionSyncValue(run.runtime, value, "session projection "+key+" view")
	}
	return definition, nil
}

func (e *Engine) dynamicCordisSessionProjectionsFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("register", func(call goja.FunctionCall) goja.Value {
		definition, err := dynamicProjectionDefinition(run, call.Argument(0))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		dispose, err := e.sessionProjections.registerRuntime(definition)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		run.disposers = append(run.disposers, dispose)
		return vm.ToValue(dispose)
	})
	_ = service.Set("stateOf", func(call goja.FunctionCall) goja.Value {
		sessionID := dynamicCordisSessionID(vm, call.Argument(0))
		key := strings.TrimSpace(call.Argument(1).String())
		if sessionID == "" || key == "" {
			panic(vm.ToValue("ctx.sessionProjections.stateOf requires a session and key"))
		}
		session, err := e.getSession(sessionID)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		state, found, err := e.sessionProjections.stateOfRuntime(session, key)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		if !found {
			return goja.Undefined()
		}
		value, err := dynamicProjectionClone(state)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(value)
	})
	_ = service.Set("snapshot", func(call goja.FunctionCall) goja.Value {
		sessionID := dynamicCordisSessionID(vm, call.Argument(0))
		session, err := e.getSession(sessionID)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		snapshot, err := e.sessionProjections.snapshotRuntime(session)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		value, err := dynamicProjectionClone(snapshot)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(value)
	})
	return service
}
