package harness

// The TypeScript runner uses Cordis fibers and node:vm. The Go port keeps the
// same wire/lifecycle contract with goja and explicit disposable Host services.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/dop251/goja"
	"github.com/evanw/esbuild/pkg/api"
)

type DynamicCordisDefineRequest struct {
	SessionID string                      `json:"sessionId"`
	Plugin    DynamicCordisPluginSelector `json:"plugin"`
	Name      string                      `json:"name"`
	Purpose   string                      `json:"purpose"`
	Code      DynamicCordisCode           `json:"code"`
	// Global is reserved for trusted host composition. Model-created dynamic
	// packages remain session-scoped because the cordis_define tool never sets it.
	Global bool `json:"-"`
}

type DynamicCordisPluginSelector struct {
	Kind     string `json:"kind"`
	IDPrefix string `json:"idPrefix,omitempty"`
	PluginID string `json:"pluginId,omitempty"`
}

type DynamicCordisCode struct {
	Host   string `json:"host,omitempty"`
	Client string `json:"client,omitempty"`
}

type DynamicCordisDefineReceipt struct {
	PluginID      string `json:"pluginId"`
	PackageID     string `json:"packageId"`
	Name          string `json:"name"`
	Purpose       string `json:"purpose"`
	HasHostHalf   bool   `json:"hasHostHalf"`
	HasClientHalf bool   `json:"hasClientHalf"`
}

type DynamicCordisRunResolution struct {
	OK          bool     `json:"ok"`
	PluginRunID string   `json:"pluginRunId,omitempty"`
	WaitingFor  []string `json:"waitingFor,omitempty"`
	Reason      string   `json:"reason,omitempty"`
	StartedHere *bool    `json:"startedHere,omitempty"`
	Message     string   `json:"message,omitempty"`
	Stack       string   `json:"stack,omitempty"`
}

type DynamicCordisRunResponse struct {
	OK               bool     `json:"ok"`
	Status           string   `json:"status,omitempty"`
	Reason           string   `json:"reason,omitempty"`
	Message          string   `json:"message,omitempty"`
	Stack            string   `json:"stack,omitempty"`
	PluginID         string   `json:"pluginId,omitempty"`
	PackageID        string   `json:"packageId,omitempty"`
	PluginRunID      string   `json:"pluginRunId,omitempty"`
	Mode             string   `json:"mode,omitempty"`
	WaitingFor       []string `json:"waitingFor"`
	ClientWaitingFor []string `json:"clientWaitingFor,omitempty"`
	CurrentPackageID string   `json:"currentPackageId,omitempty"`
	NextPackageID    string   `json:"nextPackageId,omitempty"`
}

type DynamicCordisHostHalfResult struct {
	OK          bool     `json:"ok"`
	Message     string   `json:"message,omitempty"`
	Stack       string   `json:"stack,omitempty"`
	PluginID    string   `json:"pluginId,omitempty"`
	PackageID   string   `json:"packageId,omitempty"`
	PluginRunID string   `json:"pluginRunId,omitempty"`
	WaitingFor  []string `json:"waitingFor"`
	StartedHere bool     `json:"startedHere"`
}

type DynamicCordisClientSource struct {
	Code        string `json:"code"`
	Name        string `json:"name"`
	PluginID    string `json:"pluginId"`
	PackageID   string `json:"packageId"`
	PluginRunID string `json:"pluginRunId"`
}

type DynamicCordisInvokeResult struct {
	OK      bool   `json:"ok"`
	Value   any    `json:"value"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	Stack   string `json:"stack,omitempty"`
}

type DynamicCordisStopResponse struct {
	OK      bool   `json:"ok"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type DynamicCordisUndefineReceipt struct {
	OK         bool   `json:"ok"`
	Reason     string `json:"reason,omitempty"`
	Message    string `json:"message,omitempty"`
	WasRunning bool   `json:"wasRunning"`
}

type dynamicCordisPackage struct {
	id, name, purpose string
	host, client      string
}

type dynamicCordisRun struct {
	mu                sync.Mutex
	reportMu          sync.Mutex
	eventMu           sync.RWMutex
	toolMu            sync.Mutex
	pluginID          string
	runID             string
	packageID         string
	sessionID         string
	global            bool
	vmTimeout         time.Duration
	startedForRequest string
	runtime           *goja.Runtime
	agentInitiator    *dynamicCordisAgentAsyncContextTracker
	plugin            *goja.Object
	apply             goja.Callable
	ctx               *goja.Object
	ctxFacade         *dynamicCordisContextFacade
	inject            []string
	handlers          map[string]goja.Callable
	handlerVersions   map[string]uint64
	nextHandler       uint64
	toolDefinitions   map[*goja.Object]*dynamicCordisTool
	toolNames         map[string]struct{}
	provided          map[string]goja.Value
	disposers         []func()
	finalDisposers    []func()
	pending           map[*dynamicCordisAwait]struct{}
	effectSetups      int
	effectSetupErr    error
	effectWaiters     []func(error)
	cleanupWaits      []*dynamicCordisCleanupWait
	renderFailure     *DynamicCordisRenderFailure
	reportedErrors    map[string]struct{}
	listeners         map[string][]*dynamicCordisListener
	preparedSessions  map[string]*dynamicCordisPreparedSession
	active            bool
	activating        bool
	cleaning          bool
	disposed          bool
}

type dynamicCordisCleanupWait struct {
	done bool
}

type dynamicCordisEffectState struct {
	cleanups []goja.Callable
	disposed bool
	result   goja.Value
}

type dynamicCordisAwait struct {
	run    *dynamicCordisRun
	done   bool
	finish func(goja.Value, error)
}

type dynamicCordisService struct {
	owner *dynamicCordisRun
	value goja.Value
}

type dynamicCordisTool struct {
	name             string
	description      string
	parameters       map[string]any
	outputSchema     map[string]any
	execute          goja.Callable
	render           goja.Callable
	presentationMeta goja.Callable
}

type dynamicCordisPlugin struct {
	id, sessionID string
	global        bool
	packages      map[string]*dynamicCordisPackage
	order         []string
	approved      map[string]bool
	approveFuture bool
	current       string
	next          string
	run           *dynamicCordisRun
	latest        map[string]any
}

func (run *dynamicCordisRun) ownerSessionID() string {
	if run.global {
		return ""
	}
	return run.sessionID
}

func (run *dynamicCordisRun) appliesToSession(sessionID string) bool {
	return run.global || run.sessionID == sessionID
}

type dynamicCordisPendingRun struct {
	id, agentID, pluginID, packageID, runID, mode string
	requiresApproval                              bool
}

var dynamicPluginPrefix = regexp.MustCompile(`^[a-z]{3,6}$`)
var dynamicPluginMention = regexp.MustCompile(`@[a-z]{3,6}-[0-9]+`)
var dynamicTypeScriptAssertion = regexp.MustCompile(`\bas\b`)
var dynamicSyntaxPosition = regexp.MustCompile(`Line ([0-9]+):([0-9]+)`)

func dynamicCordisAttempt(runID, packageID, mode string, pkg *dynamicCordisPackage) map[string]any {
	hostStatus, clientStatus := "absent", "absent"
	if pkg.host != "" {
		hostStatus = "pending"
	}
	if pkg.client != "" {
		clientStatus = "pending"
	}
	return map[string]any{
		"pluginRunId": runID,
		"packageId":   packageID,
		"mode":        mode,
		"status":      "starting-host",
		"host":        map[string]any{"status": hostStatus, "waitingFor": []string{}},
		"client":      map[string]any{"status": clientStatus, "waitingFor": []string{}},
	}
}

func dynamicCordisSetAttemptHalf(attempt map[string]any, half, status string, waiting []string, message string) {
	state := map[string]any{"status": status, "waitingFor": append([]string{}, waiting...)}
	if message != "" {
		state["error"] = message
	}
	attempt[half] = state
}

func dynamicCordisAttemptRunID(attempt map[string]any) string {
	runID, _ := attempt["pluginRunId"].(string)
	return runID
}

func dynamicCordisAttemptString(attempt map[string]any, key string) string {
	value, _ := attempt[key].(string)
	return value
}

func dynamicCordisAttemptHalf(attempt map[string]any, half string) map[string]any {
	value, _ := attempt[half].(map[string]any)
	return value
}

func dynamicCordisAttemptWaiting(attempt map[string]any, half string) []string {
	value := dynamicCordisAttemptHalf(attempt, half)
	waiting, _ := value["waitingFor"].([]string)
	return append([]string{}, waiting...)
}

func dynamicCordisCommitActivation(plugin *dynamicCordisPlugin, run *dynamicCordisRun, clientWaiting []string) (string, []string) {
	mode := "run"
	hostWaiting := []string{}
	if dynamicCordisAttemptRunID(plugin.latest) == run.runID {
		if value := dynamicCordisAttemptString(plugin.latest, "mode"); value != "" {
			mode = value
		}
		hostWaiting = dynamicCordisAttemptWaiting(plugin.latest, "host")
		clientStatus := "running"
		if len(clientWaiting) > 0 {
			clientStatus = "waiting"
		}
		dynamicCordisSetAttemptHalf(plugin.latest, "client", clientStatus, clientWaiting, "")
		plugin.latest["status"] = "running"
		if dynamicCordisAttemptString(dynamicCordisAttemptHalf(plugin.latest, "host"), "status") == "waiting" || clientStatus == "waiting" {
			plugin.latest["status"] = "waiting"
		}
		delete(plugin.latest, "approvalRequestId")
		delete(plugin.latest, "requiresApproval")
		delete(plugin.latest, "error")
	}
	plugin.current = run.packageID
	plugin.next = ""
	run.startedForRequest = ""
	return mode, hostWaiting
}

func (e *Engine) DynamicCordisDefine(request DynamicCordisDefineRequest) (DynamicCordisDefineReceipt, error) {
	if strings.TrimSpace(request.SessionID) == "" && !request.Global {
		return DynamicCordisDefineReceipt{}, errors.New("cordis_define requires a sessionId")
	}
	if !request.Global {
		if _, err := e.getSession(request.SessionID); err != nil {
			return DynamicCordisDefineReceipt{}, err
		}
	}
	name, purpose := strings.TrimSpace(request.Name), strings.TrimSpace(request.Purpose)
	if name == "" || purpose == "" {
		return DynamicCordisDefineReceipt{}, errors.New("cordis_define requires non-empty name and purpose")
	}
	if request.Code.Host == "" && request.Code.Client == "" {
		return DynamicCordisDefineReceipt{}, errors.New("cordis_define requires code.host, code.client, or both")
	}
	if request.Code.Host != "" {
		if _, err := compileDynamicBody(request.Code.Host); err != nil {
			return DynamicCordisDefineReceipt{}, dynamicCordisParseError("code.host", request.Code.Host, err)
		}
	}
	if request.Code.Client != "" {
		if _, err := compileDynamicBody(request.Code.Client); err != nil {
			return DynamicCordisDefineReceipt{}, dynamicCordisParseError("code.client", request.Code.Client, err)
		}
	}
	e.dynamicCordis.Lock()
	defer e.dynamicCordis.Unlock()
	var plugin *dynamicCordisPlugin
	switch request.Plugin.Kind {
	case "new", "":
		prefix := strings.TrimSpace(request.Plugin.IDPrefix)
		if !dynamicPluginPrefix.MatchString(prefix) {
			return DynamicCordisDefineReceipt{}, errors.New("cordis_define plugin.idPrefix must contain 3-6 lowercase English letters")
		}
		for {
			id := fmt.Sprintf("%s-%d", prefix, e.dynamicCordis.nextPlugin)
			e.dynamicCordis.nextPlugin++
			if _, exists := e.dynamicCordis.plugins[id]; !exists {
				plugin = &dynamicCordisPlugin{id: id, sessionID: request.SessionID, global: request.Global, packages: map[string]*dynamicCordisPackage{}, approved: map[string]bool{}, latest: map[string]any{}}
				e.dynamicCordis.plugins[id] = plugin
				e.dynamicCordis.pluginOrder = append(e.dynamicCordis.pluginOrder, id)
				break
			}
		}
	case "existing":
		plugin = e.dynamicCordis.plugins[request.Plugin.PluginID]
		if plugin == nil || plugin.sessionID != request.SessionID || plugin.global != request.Global {
			return DynamicCordisDefineReceipt{}, fmt.Errorf("dynamic plugin %q does not exist in this session", request.Plugin.PluginID)
		}
	default:
		return DynamicCordisDefineReceipt{}, errors.New("cordis_define plugin.kind must be new or existing")
	}
	packageID := fmt.Sprintf("pkg-%d", e.dynamicCordis.nextPackage)
	e.dynamicCordis.nextPackage++
	pkg := &dynamicCordisPackage{id: packageID, name: name, purpose: purpose, host: request.Code.Host, client: request.Code.Client}
	plugin.packages[packageID] = pkg
	plugin.order = append(plugin.order, packageID)
	return DynamicCordisDefineReceipt{PluginID: plugin.id, PackageID: packageID, Name: name, Purpose: purpose, HasHostHalf: pkg.host != "", HasClientHalf: pkg.client != ""}, nil
}

func compileDynamicBody(body string) (*goja.Program, error) {
	source := "(async function(){\n" + body + "\n})"
	program, err := goja.Compile("dynamic-cordis.js", source, false)
	if err == nil {
		return program, nil
	}
	transformed := api.Transform(source, api.TransformOptions{
		Loader: api.LoaderJS, Target: api.ES2017, Sourcefile: "dynamic-cordis.js",
		LegalComments: api.LegalCommentsNone, LogLevel: api.LogLevelSilent,
	})
	if len(transformed.Errors) > 0 {
		return nil, err
	}
	return goja.Compile("dynamic-cordis.js", string(transformed.Code), false)
}

func dynamicCordisParseError(half, body string, err error) error {
	context := err.Error()
	if match := dynamicSyntaxPosition.FindStringSubmatch(context); len(match) == 3 {
		line, lineErr := strconv.Atoi(match[1])
		column, columnErr := strconv.Atoi(match[2])
		lines := strings.Split(body, "\n")
		line--
		if lineErr == nil && columnErr == nil && line >= 1 && line <= len(lines) {
			offending := lines[line-1]
			column = max(column, 1)
			context = fmt.Sprintf("%s\n%s\n%s^", err, offending, strings.Repeat(" ", column-1))
		}
	}
	message := fmt.Sprintf("dynamic package `%s` failed to parse:\n%s\n", half, context)
	offending := ""
	parts := strings.Split(context, "\n")
	if len(parts) > 1 {
		offending = parts[1]
	}
	if dynamicTypeScriptAssertion.MatchString(offending) {
		return errors.New(message + "The sandbox runs plain JavaScript, not TypeScript. Remove type annotations:\n" +
			"  x { type: 'text' as const, text: x }\n" +
			"  ok { type: 'text', text: x }")
	}
	return errors.New(message + "Note: it runs as the BODY of an async function (line numbers are offset by the 1-line wrapper). " +
		"Check bracket balance: ending the returned plugin object with `});` closes a call that was never opened; " +
		"a plain `return { ... }` ends with `}` (an optional `;`), never `)`.")
}

func dynamicCordisResolveValue(value goja.Value) (goja.Value, error) {
	if value == nil || goja.IsUndefined(value) {
		return nil, errors.New("dynamic JavaScript returned undefined")
	}
	promise, ok := value.Export().(*goja.Promise)
	if !ok {
		return value, nil
	}
	switch promise.State() {
	case goja.PromiseStateFulfilled:
		return promise.Result(), nil
	case goja.PromiseStateRejected:
		return nil, errors.New(dynamicJSMessage(fmt.Errorf("%s", promise.Result().String())))
	default:
		return nil, errors.New("dynamic JavaScript promise did not settle")
	}
}

func dynamicCordisAwaitOnLoop(run *dynamicCordisRun, value goja.Value, finish func(goja.Value, error)) {
	if value == nil || goja.IsUndefined(value) {
		finish(nil, errors.New("dynamic JavaScript returned undefined"))
		return
	}
	promise, ok := value.Export().(*goja.Promise)
	if !ok {
		finish(value, nil)
		return
	}
	wait := &dynamicCordisAwait{run: run, finish: finish}
	complete := func(value goja.Value, err error) {
		if wait.done {
			return
		}
		wait.done = true
		delete(run.pending, wait)
		finish(value, err)
	}
	switch promise.State() {
	case goja.PromiseStateFulfilled:
		complete(promise.Result(), nil)
		return
	case goja.PromiseStateRejected:
		message, stack := dynamicJSValueDetails(promise.Result())
		complete(nil, &dynamicCordisJSError{message: message, stack: stack, code: dynamicJSValueCode(promise.Result())})
		return
	}
	run.pending[wait] = struct{}{}
	object := run.runtime.ToValue(promise).ToObject(run.runtime)
	then, ok := goja.AssertFunction(object.Get("then"))
	if !ok {
		complete(nil, errors.New("dynamic JavaScript promise has no then method"))
		return
	}
	fulfilled := run.runtime.ToValue(func(call goja.FunctionCall) goja.Value {
		complete(call.Argument(0), nil)
		return goja.Undefined()
	})
	rejected := run.runtime.ToValue(func(call goja.FunctionCall) goja.Value {
		message, stack := dynamicJSValueDetails(call.Argument(0))
		complete(nil, &dynamicCordisJSError{message: message, stack: stack, code: dynamicJSValueCode(call.Argument(0))})
		return goja.Undefined()
	})
	if _, err := then(object, fulfilled, rejected); err != nil {
		complete(nil, err)
	}
}

func dynamicCordisFinishEffectSetup(run *dynamicCordisRun, err error) {
	if err != nil && run.effectSetupErr == nil {
		run.effectSetupErr = err
	}
	run.effectSetups--
	if run.effectSetups != 0 {
		return
	}
	waiters := append([]func(error){}, run.effectWaiters...)
	run.effectWaiters = nil
	setupErr := run.effectSetupErr
	for _, waiter := range waiters {
		waiter(setupErr)
	}
}

func dynamicCordisAwaitEffectSetups(run *dynamicCordisRun, finish func(error)) {
	if run.effectSetups == 0 {
		finish(run.effectSetupErr)
		return
	}
	run.effectWaiters = append(run.effectWaiters, finish)
}

type dynamicCordisEffectIteratorState struct {
	object *goja.Object
	next   goja.Callable
}

func dynamicCordisEffectIterator(vm *goja.Runtime, value goja.Value, async bool) (dynamicCordisEffectIteratorState, bool, error) {
	object, ok := value.(*goja.Object)
	if !ok {
		return dynamicCordisEffectIteratorState{}, false, nil
	}
	member := "iterator"
	if async {
		member = "asyncIterator"
	}
	symbol, ok := vm.Get("Symbol").ToObject(vm).Get(member).(*goja.Symbol)
	if !ok {
		return dynamicCordisEffectIteratorState{}, false, nil
	}
	methodValue := object.GetSymbol(symbol)
	if methodValue == nil || goja.IsUndefined(methodValue) {
		return dynamicCordisEffectIteratorState{}, false, nil
	}
	method, ok := goja.AssertFunction(methodValue)
	if !ok {
		return dynamicCordisEffectIteratorState{}, false, errors.New("Invalid effect")
	}
	iteratorValue, err := method(object)
	if err != nil {
		return dynamicCordisEffectIteratorState{}, false, errors.New(dynamicJSMessage(err))
	}
	iterator, ok := iteratorValue.(*goja.Object)
	if !ok {
		return dynamicCordisEffectIteratorState{}, false, errors.New("Invalid effect iterator")
	}
	next, ok := goja.AssertFunction(iterator.Get("next"))
	if !ok {
		return dynamicCordisEffectIteratorState{}, false, errors.New("Invalid effect iterator")
	}
	return dynamicCordisEffectIteratorState{object: iterator, next: next}, true, nil
}

func dynamicCordisCollectEffectCleanup(run *dynamicCordisRun, state *dynamicCordisEffectState, value goja.Value) error {
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return nil
	}
	cleanup, ok := goja.AssertFunction(value)
	if !ok {
		return errors.New("Invalid effect")
	}
	if !state.disposed {
		state.cleanups = append(state.cleanups, cleanup)
		return nil
	}
	result, err := cleanup(goja.Undefined())
	if err != nil {
		return errors.New(dynamicJSMessage(err))
	}
	dynamicCordisTrackCleanup(run, result)
	return nil
}

func dynamicCordisConsumeEffectIterator(run *dynamicCordisRun, state *dynamicCordisEffectState, iterator dynamicCordisEffectIteratorState) error {
	for {
		resultValue, err := iterator.next(iterator.object)
		if err != nil {
			return errors.New(dynamicJSMessage(err))
		}
		result, ok := resultValue.(*goja.Object)
		if !ok {
			return errors.New("Invalid effect iterator result")
		}
		if err := dynamicCordisCollectEffectCleanup(run, state, result.Get("value")); err != nil {
			return err
		}
		if result.Get("done").ToBoolean() {
			return nil
		}
	}
}

func dynamicCordisConsumeAsyncEffectIterator(run *dynamicCordisRun, state *dynamicCordisEffectState, iterator dynamicCordisEffectIteratorState, finish func(error)) {
	var next func()
	next = func() {
		resultValue, err := iterator.next(iterator.object)
		if err != nil {
			finish(errors.New(dynamicJSMessage(err)))
			return
		}
		dynamicCordisAwaitOnLoop(run, resultValue, func(resolved goja.Value, awaitErr error) {
			if awaitErr != nil {
				finish(awaitErr)
				return
			}
			result, ok := resolved.(*goja.Object)
			if !ok {
				finish(errors.New("Invalid effect iterator result"))
				return
			}
			if err := dynamicCordisCollectEffectCleanup(run, state, result.Get("value")); err != nil {
				finish(err)
				return
			}
			if result.Get("done").ToBoolean() {
				finish(nil)
				return
			}
			next()
		})
	}
	next()
}

func dynamicCordisDisposeEffect(run *dynamicCordisRun, state *dynamicCordisEffectState) goja.Value {
	if state.disposed {
		if state.result != nil {
			return state.result
		}
		return goja.Undefined()
	}
	state.disposed = true
	if len(state.cleanups) == 0 {
		return goja.Undefined()
	}
	cleanups := append([]goja.Callable(nil), state.cleanups...)
	state.cleanups = nil
	vm := run.runtime
	promise, resolve, reject := vm.NewPromise()
	state.result = vm.ToValue(promise)
	index := len(cleanups) - 1
	var next func()
	next = func() {
		if index < 0 {
			_ = resolve(goja.Undefined())
			return
		}
		cleanup := cleanups[index]
		index--
		result, err := cleanup(goja.Undefined())
		if err != nil {
			_ = reject(vm.NewGoError(errors.New(dynamicJSMessage(err))))
			return
		}
		dynamicCordisAwaitOnLoop(run, result, func(_ goja.Value, cleanupErr error) {
			if cleanupErr != nil {
				_ = reject(vm.NewGoError(cleanupErr))
				return
			}
			next()
		})
	}
	next()
	return state.result
}

func dynamicCordisTrackCleanup(run *dynamicCordisRun, value goja.Value) {
	if value == nil || goja.IsUndefined(value) {
		return
	}
	if _, ok := value.Export().(*goja.Promise); !ok {
		return
	}
	wait := &dynamicCordisCleanupWait{}
	run.cleanupWaits = append(run.cleanupWaits, wait)
	dynamicCordisAwaitOnLoop(run, value, func(goja.Value, error) { wait.done = true })
}

func (wait *dynamicCordisAwait) cancel(err error) {
	if wait.done {
		return
	}
	wait.done = true
	delete(wait.run.pending, wait)
	wait.finish(nil, err)
}

func dynamicJSValueMessage(value goja.Value) string {
	message, _ := dynamicJSValueDetails(value)
	return message
}

type dynamicCordisJSError struct {
	message string
	stack   string
	code    string
}

func (e *dynamicCordisJSError) Error() string { return e.message }

func dynamicJSValueDetails(value goja.Value) (string, string) {
	if value == nil {
		return "JavaScript promise rejected", ""
	}
	if object, ok := value.(*goja.Object); ok {
		stack := ""
		if raw := object.Get("stack"); raw != nil && !goja.IsUndefined(raw) {
			stack = raw.String()
		}
		if message := object.Get("message"); message != nil && !goja.IsUndefined(message) {
			return message.String(), stack
		}
		return object.String(), stack
	}
	return value.String(), ""
}

func dynamicJSValueCode(value goja.Value) string {
	object, ok := value.(*goja.Object)
	if !ok {
		return ""
	}
	code := object.Get("code")
	if code == nil || goja.IsUndefined(code) || goja.IsNull(code) {
		return ""
	}
	return code.String()
}

func dynamicJSErrorDetails(err error) (string, string) {
	var jsError *dynamicCordisJSError
	if errors.As(err, &jsError) {
		return jsError.message, jsError.stack
	}
	var exception *goja.Exception
	if errors.As(err, &exception) {
		message, stack := dynamicJSValueDetails(exception.Value())
		if stack == "" {
			stack = exception.String()
		}
		return message, stack
	}
	return dynamicJSMessage(err), ""
}

func dynamicCordisJSONValue(value goja.Value) (any, error) {
	value, err := dynamicCordisResolveValue(value)
	if err != nil {
		return nil, err
	}
	if goja.IsNull(value) {
		return nil, nil
	}
	data, err := json.Marshal(value.Export())
	if err != nil {
		return nil, errors.New("dynamic JavaScript returned a non-JSON value")
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func dynamicCordisToolParameters(value goja.Value) (map[string]any, error) {
	raw, err := dynamicCordisJSONValue(value)
	if err != nil {
		return nil, fmt.Errorf("harness.defineTool parameters: %w", err)
	}
	parameters, ok := raw.(map[string]any)
	if !ok {
		return nil, errors.New("harness.defineTool parameters must be an object")
	}
	if parameters["type"] == "object" {
		return parameters, nil
	}
	properties := make(map[string]any, len(parameters))
	required := make([]string, 0)
	for name, rawProperty := range parameters {
		property, ok := rawProperty.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("harness.defineTool parameters.%s must be an object", name)
		}
		if requiredValue, exists := property["required"]; exists {
			isRequired, ok := requiredValue.(bool)
			if !ok {
				return nil, fmt.Errorf("harness.defineTool parameters.%s.required must be a boolean", name)
			}
			if isRequired {
				required = append(required, name)
			}
			delete(property, "required")
		}
		if property["type"] == "json" {
			delete(property, "type")
		}
		properties[name] = property
	}
	sort.Strings(required)
	result := map[string]any{"type": "object", "properties": properties, "additionalProperties": true}
	if len(required) > 0 {
		result["required"] = required
	}
	return result, nil
}

func dynamicCordisToolDefinition(vm *goja.Runtime, run *dynamicCordisRun, value goja.Value) (*dynamicCordisTool, *goja.Object, error) {
	object, ok := value.(*goja.Object)
	if !ok {
		return nil, nil, errors.New("harness.defineTool options must be an object")
	}
	name := strings.TrimSpace(object.Get("name").String())
	description := strings.TrimSpace(object.Get("description").String())
	if name == "" || description == "" {
		return nil, nil, errors.New("harness.defineTool requires non-empty name and description")
	}
	parameters, err := dynamicCordisToolParameters(object.Get("parameters"))
	if err != nil {
		return nil, nil, err
	}
	execute, ok := goja.AssertFunction(object.Get("execute"))
	if !ok {
		return nil, nil, errors.New("harness.defineTool execute must be a function")
	}
	output, ok := object.Get("output").(*goja.Object)
	if !ok {
		return nil, nil, errors.New("harness.defineTool output must declare schema and render")
	}
	rawOutputSchema, err := dynamicCordisJSONValue(output.Get("schema"))
	if err != nil {
		return nil, nil, fmt.Errorf("harness.defineTool output.schema: %w", err)
	}
	outputSchema, ok := rawOutputSchema.(map[string]any)
	if !ok {
		return nil, nil, errors.New("harness.defineTool output.schema must be an object")
	}
	render, ok := goja.AssertFunction(output.Get("render"))
	if !ok {
		return nil, nil, errors.New("harness.defineTool output.render must be a function")
	}
	var presentationMeta goja.Callable
	if raw := output.Get("presentationMeta"); raw != nil && !goja.IsUndefined(raw) {
		presentationMeta, ok = goja.AssertFunction(raw)
		if !ok {
			return nil, nil, errors.New("harness.defineTool output.presentationMeta must be a function")
		}
	}
	definition := &dynamicCordisTool{name: name, description: description, parameters: parameters, outputSchema: outputSchema, execute: execute, render: render, presentationMeta: presentationMeta}
	run.toolDefinitions[object] = definition
	return definition, object, nil
}

func (e *Engine) executeDynamicCordisTool(ctx context.Context, run *dynamicCordisRun, owner string, definition *dynamicCordisTool, call ToolCall) (ToolResult, error) {
	if owner != "" && call.SessionID != owner {
		return ToolResult{}, errors.New("dynamic tool is unavailable outside its owning session")
	}
	if err := ctx.Err(); err != nil {
		return ToolResult{}, err
	}
	var args any
	if len(call.Arguments) == 0 {
		call.Arguments = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(call.Arguments, &args); err != nil {
		return ToolResult{}, err
	}
	type outcome struct {
		result ToolResult
		err    error
	}
	ready := make(chan outcome, 1)
	if !e.dynamicCordis.loop.post(func() {
		run.mu.Lock()
		active := !run.disposed && (run.active || run.activating)
		run.mu.Unlock()
		if !active {
			ready <- outcome{err: errors.New("dynamic tool is no longer active")}
			return
		}
		arguments := run.runtime.ToValue(args)
		exec := run.runtime.ToValue(map[string]any{"callId": call.ID, "sessionId": call.SessionID, "workspace": call.Workspace})
		value, err := definition.execute(goja.Undefined(), arguments, exec)
		if err != nil {
			ready <- outcome{err: errors.New(dynamicJSMessage(err))}
			return
		}
		dynamicCordisAwaitOnLoop(run, value, func(value goja.Value, err error) {
			if err != nil {
				ready <- outcome{err: err}
				return
			}
			canonical, err := dynamicCordisJSONValue(value)
			if err != nil {
				ready <- outcome{err: err}
				return
			}
			if err := validateJSONAgainstSchema(canonical, definition.outputSchema); err != nil {
				ready <- outcome{err: fmt.Errorf("dynamic tool %q output: %w", definition.name, err)}
				return
			}
			rendered, err := definition.render(goja.Undefined(), arguments, run.runtime.ToValue(canonical))
			if err != nil {
				ready <- outcome{err: errors.New(dynamicJSMessage(err))}
				return
			}
			renderedValue, err := dynamicCordisJSONValue(rendered)
			if err != nil {
				ready <- outcome{err: err}
				return
			}
			data, err := json.Marshal(renderedValue)
			var blocks []ContentBlock
			if err != nil {
				ready <- outcome{err: err}
				return
			}
			if err := json.Unmarshal(data, &blocks); err != nil || blocks == nil {
				ready <- outcome{err: errors.New("harness.defineTool output.render must return an array of content blocks")}
				return
			}
			for _, block := range blocks {
				if strings.TrimSpace(block.Type) == "" {
					ready <- outcome{err: errors.New("harness.defineTool output.render returned a content block without type")}
					return
				}
			}
			result := ToolResult{Content: blocks, Value: canonical}
			if definition.presentationMeta != nil {
				meta, callErr := definition.presentationMeta(goja.Undefined(), arguments, run.runtime.ToValue(canonical))
				if callErr != nil {
					ready <- outcome{err: errors.New(dynamicJSMessage(callErr))}
					return
				}
				result.Meta, err = dynamicCordisJSONValue(meta)
				if err != nil {
					ready <- outcome{err: err}
					return
				}
			}
			ready <- outcome{result: result}
		})
	}) {
		return ToolResult{}, errors.New("dynamic Cordis runtime is closed")
	}
	select {
	case resolved := <-ready:
		return resolved.result, resolved.err
	case <-ctx.Done():
		return ToolResult{}, ctx.Err()
	}
}

func (e *Engine) registerDynamicCordisTool(run *dynamicCordisRun, owner string, definition *dynamicCordisTool) error {
	run.toolMu.Lock()
	if run.disposed {
		run.toolMu.Unlock()
		return errors.New("dynamic package is no longer active")
	}
	if _, exists := run.toolNames[definition.name]; exists {
		run.toolMu.Unlock()
		return fmt.Errorf("tool already registered: %s", definition.name)
	}
	run.toolMu.Unlock()
	tool := Tool{Schema: ToolSchema{
		Name:        definition.name,
		Description: definition.description,
		Parameters:  definition.parameters,
		Output:      definition.outputSchema,
	}}
	tool.Execute = func(ctx context.Context, call ToolCall) (ToolResult, error) {
		return e.executeDynamicCordisTool(ctx, run, owner, definition, call)
	}
	if err := e.registerToolFrom(run, tool, owner); err != nil {
		return err
	}
	run.toolMu.Lock()
	run.toolNames[definition.name] = struct{}{}
	run.toolMu.Unlock()
	return nil
}

func (e *Engine) unregisterDynamicCordisTool(run *dynamicCordisRun, name string) {
	run.toolMu.Lock()
	if _, exists := run.toolNames[name]; !exists {
		run.toolMu.Unlock()
		return
	}
	delete(run.toolNames, name)
	run.toolMu.Unlock()
	if _, err := e.unregisterToolFrom(run, name); err != nil {
		panic(run.runtime.ToValue(err.Error()))
	}
}

func (e *Engine) disposeDynamicCordisRun(run *dynamicCordisRun) {
	e.cleanupDynamicCordisRun(run, true)
}

// disposeDynamicCordisRuns tears down every active run while the shared loop
// remains available. Engine shutdown uses this phase after the final session
// projection checkpoint: run disposers can unregister dynamic projections and
// detach prepared sessions, both of which still need the runtime.
func (e *Engine) disposeDynamicCordisRuns() {
	e.dynamicCordis.Lock()
	runs := make([]*dynamicCordisRun, 0, len(e.dynamicCordis.plugins))
	for _, plugin := range e.dynamicCordis.plugins {
		if plugin.run != nil {
			runs = append(runs, plugin.run)
			plugin.run = nil
		}
	}
	e.dynamicCordis.Unlock()
	for _, run := range runs {
		e.disposeDynamicCordisRun(run)
	}
}

func (e *Engine) closeDynamicCordisLoop() {
	e.dynamicCordis.loop.close()
}

func (e *Engine) closeDynamicCordis() {
	e.disposeDynamicCordisRuns()
	e.closeDynamicCordisLoop()
}

func (e *Engine) DynamicCordisRun(ctx context.Context, sessionID, pluginID, packageID, mode string) (DynamicCordisRunResponse, error) {
	if err := ctx.Err(); err != nil {
		return DynamicCordisRunResponse{}, err
	}
	e.dynamicCordis.Lock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID {
		e.dynamicCordis.Unlock()
		return DynamicCordisRunResponse{Reason: "plugin-missing", Message: fmt.Sprintf("dynamic plugin %q is unavailable", pluginID)}, nil
	}
	pkg := plugin.packages[packageID]
	if pkg == nil {
		e.dynamicCordis.Unlock()
		return DynamicCordisRunResponse{Reason: "package-missing", Message: fmt.Sprintf("dynamic package %q is unavailable", packageID)}, nil
	}
	if mode != "run" && mode != "update" {
		e.dynamicCordis.Unlock()
		return DynamicCordisRunResponse{Reason: "invalid-mode", Message: "dynamic Cordis mode must be run or update"}, nil
	}
	if mode == "update" && (plugin.current == "" || plugin.current == packageID) {
		e.dynamicCordis.Unlock()
		return DynamicCordisRunResponse{Reason: "invalid-mode", Message: "mode update requires a different existing current package"}, nil
	}
	if mode == "run" && plugin.current != "" && plugin.current != packageID {
		e.dynamicCordis.Unlock()
		return DynamicCordisRunResponse{Reason: "invalid-mode", Message: "mode run can only start or restart the current package"}, nil
	}
	transitioning := e.dynamicCordis.starting[pluginID] != nil
	if !transitioning {
		for _, pending := range e.dynamicCordis.pendingRuns {
			if pending.pluginID == pluginID {
				transitioning = true
				break
			}
		}
	}
	if transitioning {
		e.dynamicCordis.Unlock()
		return DynamicCordisRunResponse{Reason: "transition-in-flight", Message: fmt.Sprintf("dynamic plugin %q already has a transition in flight", pluginID)}, nil
	}
	runID := fmt.Sprintf("run-%d", e.dynamicCordis.nextRun)
	e.dynamicCordis.nextRun++
	plugin.next = packageID
	plugin.latest = dynamicCordisAttempt(runID, packageID, mode, pkg)
	if pkg.client != "" {
		requiresApproval := !plugin.approved[packageID] && !plugin.approveFuture
		requestID := fmt.Sprintf("approval-%d", e.dynamicCordis.nextApproval)
		e.dynamicCordis.nextApproval++
		if requiresApproval {
			plugin.latest["status"] = "awaiting-approval"
		}
		plugin.latest["approvalRequestId"] = requestID
		plugin.latest["requiresApproval"] = requiresApproval
		pending := &dynamicCordisPendingRun{id: requestID, agentID: sessionID, pluginID: pluginID, packageID: packageID, runID: runID, mode: mode, requiresApproval: requiresApproval}
		e.dynamicCordis.pendingRuns[requestID] = pending
		e.dynamicCordis.Unlock()
		e.emitCordisEvent("cordis/request-run", map[string]any{"requestId": requestID, "agentId": sessionID, "pluginId": pluginID, "packageId": packageID, "mode": mode, "name": pkg.name, "purpose": pkg.purpose, "requiresApproval": requiresApproval})
		status := "starting"
		if requiresApproval {
			status = "awaiting-approval"
		}
		return DynamicCordisRunResponse{OK: true, Status: status, PluginID: pluginID, PackageID: packageID, PluginRunID: runID, Mode: mode, WaitingFor: []string{}, CurrentPackageID: plugin.current, NextPackageID: packageID}, nil
	}
	e.dynamicCordis.Unlock()
	started, _, err := e.dynamicCordisStartHost(ctx, sessionID, pluginID, packageID, runID, "")
	if err != nil {
		e.dynamicCordisFailAttempt(pluginID, runID, "host-load", err.Error())
		return DynamicCordisRunResponse{Reason: "host-half-failed", Message: err.Error()}, nil
	}
	return DynamicCordisRunResponse{OK: true, Status: "running", PluginID: pluginID, PackageID: packageID, PluginRunID: runID, Mode: mode, WaitingFor: started, CurrentPackageID: packageID}, nil
}

func (e *Engine) loadDynamicCordisHost(run *dynamicCordisRun, program *goja.Program, owner string) error {
	vm := goja.New()
	run.runtime = vm
	run.agentInitiator = &dynamicCordisAgentAsyncContextTracker{}
	vm.SetAsyncContextTracker(run.agentInitiator)
	run.ctxFacade = &dynamicCordisContextFacade{engine: e, run: run, values: map[string]goja.Value{}}
	run.ctx = vm.NewDynamicObject(run.ctxFacade)
	if err := installDynamicCordisGlobals(run, run.pluginID); err != nil {
		return err
	}
	harness := map[string]any{
		"handle": func(call goja.FunctionCall) goja.Value {
			name, nameOK := call.Argument(0).Export().(string)
			fn, ok := goja.AssertFunction(call.Argument(1))
			if !nameOK || name == "" {
				panic(vm.ToValue("harness.handle(method, fn) needs a non-empty string method name"))
			}
			if !ok {
				panic(vm.ToValue(fmt.Sprintf("harness.handle(%q) needs a handler function as its second argument", name)))
			}
			run.nextHandler++
			version := run.nextHandler
			run.handlers[name] = fn
			run.handlerVersions[name] = version
			var once sync.Once
			dispose := func() {
				once.Do(func() {
					if run.handlerVersions[name] == version {
						delete(run.handlers, name)
						delete(run.handlerVersions, name)
					}
				})
			}
			run.finalDisposers = append(run.finalDisposers, dispose)
			return vm.ToValue(dispose)
		},
		"defineTool": func(call goja.FunctionCall) goja.Value {
			_, object, err := dynamicCordisToolDefinition(vm, run, call.Argument(0))
			if err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return object
		},
		"registerTool": func(call goja.FunctionCall) goja.Value {
			if call.Argument(0) != run.ctx {
				panic(vm.ToValue("harness.registerTool requires the package ctx"))
			}
			object, ok := call.Argument(1).(*goja.Object)
			definition := run.toolDefinitions[object]
			if !ok || definition == nil {
				panic(vm.ToValue("dynamic tool registration must use a tool returned by harness.defineTool(...)"))
			}
			if err := e.registerDynamicCordisTool(run, owner, definition); err != nil {
				panic(vm.ToValue(err.Error()))
			}
			return vm.ToValue(func() { e.unregisterDynamicCordisTool(run, definition.name) })
		},
	}
	if err := vm.Set("harness", harness); err != nil {
		return err
	}
	value, err := vm.RunProgram(program)
	if err != nil {
		return errors.New(dynamicJSMessage(err))
	}
	factory, ok := goja.AssertFunction(value)
	if !ok {
		return errors.New("dynamic Host code did not compile to a function")
	}
	pluginValue, err := dynamicCordisCallBounded(vm, run.vmTimeout, func() (goja.Value, error) {
		return factory(goja.Undefined())
	})
	if err != nil {
		return errors.New(dynamicJSMessage(err))
	}
	pluginValue, err = dynamicCordisResolveValue(pluginValue)
	if err != nil {
		return err
	}
	object, ok := pluginValue.(*goja.Object)
	if !ok {
		return errors.New("dynamic Host code must return a Plugin object")
	}
	inject, err := dynamicCordisInjectList(object)
	if err != nil {
		return err
	}
	run.plugin = object
	run.inject = inject
	if fn, ok := goja.AssertFunction(object.Get("apply")); ok {
		run.apply = fn
	} else if len(run.handlers) == 0 {
		return errors.New("dynamic Host Plugin must provide an apply(ctx) function")
	}
	return nil
}

// dynamicCordisStartHost evaluates and applies one Host half. It is safe to
// call more than once for the same run; an already active run is returned as
// an attachment rather than evaluated again.
func (e *Engine) dynamicCordisStartHost(ctx context.Context, sessionID, pluginID, packageID, runID, requestID string) ([]string, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	e.dynamicCordis.Lock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID {
		e.dynamicCordis.Unlock()
		return nil, false, errors.New("dynamic plugin is unavailable")
	}
	pkg := plugin.packages[packageID]
	if pkg == nil {
		e.dynamicCordis.Unlock()
		return nil, false, errors.New("dynamic package is unavailable")
	}
	if plugin.run != nil && plugin.run.runID == runID && plugin.run.packageID == packageID {
		e.dynamicCordis.Unlock()
		return []string{}, false, nil
	}
	if starting := e.dynamicCordis.starting[pluginID]; starting != nil {
		e.dynamicCordis.Unlock()
		select {
		case <-starting.done:
			return append([]string{}, starting.waiting...), starting.startedHere, starting.err
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	starting := &dynamicCordisStarting{done: make(chan struct{})}
	e.dynamicCordis.starting[pluginID] = starting
	old := plugin.run
	if old != nil {
		plugin.run = nil
	}
	e.dynamicCordis.Unlock()
	if old != nil {
		e.disposeDynamicCordisRun(old)
		e.emitCordisEvent("cordis/dynamic-retract", map[string]any{"pluginId": pluginID, "packageId": old.packageID, "pluginRunId": old.runID})
	}
	finish := func(waiting []string, startedHere bool, err error) ([]string, bool, error) {
		e.dynamicCordis.Lock()
		if e.dynamicCordis.starting[pluginID] == starting {
			delete(e.dynamicCordis.starting, pluginID)
			starting.waiting = append([]string{}, waiting...)
			starting.startedHere = startedHere
			starting.err = err
			close(starting.done)
		}
		e.dynamicCordis.Unlock()
		return waiting, startedHere, err
	}

	var run *dynamicCordisRun
	committed := false
	defer func() {
		if !committed {
			e.disposeDynamicCordisRun(run)
		}
	}()
	if pkg.host != "" {
		program, err := compileDynamicBody(pkg.host)
		if err != nil {
			return finish(nil, false, err)
		}
		run = &dynamicCordisRun{
			pluginID: pluginID, runID: runID, packageID: packageID, sessionID: sessionID, global: plugin.global, startedForRequest: requestID,
			vmTimeout: e.cfg.DynamicCordisVMTimeout,
			handlers:  map[string]goja.Callable{}, handlerVersions: map[string]uint64{},
			toolDefinitions: map[*goja.Object]*dynamicCordisTool{}, toolNames: map[string]struct{}{},
			provided: map[string]goja.Value{}, pending: map[*dynamicCordisAwait]struct{}{}, reportedErrors: map[string]struct{}{}, listeners: map[string][]*dynamicCordisListener{}, preparedSessions: map[string]*dynamicCordisPreparedSession{},
		}
		var loadErr error
		if !e.dynamicCordis.loop.call(func() { loadErr = e.loadDynamicCordisHost(run, program, run.ownerSessionID()) }) {
			loadErr = errors.New("dynamic Cordis runtime is closed")
		}
		if loadErr != nil {
			return finish(nil, false, loadErr)
		}
	} else {
		run = &dynamicCordisRun{
			pluginID: pluginID, runID: runID, packageID: packageID, sessionID: sessionID, global: plugin.global, startedForRequest: requestID,
			vmTimeout: e.cfg.DynamicCordisVMTimeout,
			handlers:  map[string]goja.Callable{}, handlerVersions: map[string]uint64{}, toolDefinitions: map[*goja.Object]*dynamicCordisTool{},
			toolNames: map[string]struct{}{}, provided: map[string]goja.Value{}, pending: map[*dynamicCordisAwait]struct{}{}, reportedErrors: map[string]struct{}{}, listeners: map[string][]*dynamicCordisListener{}, preparedSessions: map[string]*dynamicCordisPreparedSession{}, active: true,
		}
	}
	e.dynamicCordis.Lock()
	plugin = e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID {
		e.dynamicCordis.Unlock()
		return finish(nil, false, errors.New("dynamic plugin is unavailable"))
	}
	plugin.run = run
	committed = true
	e.dynamicCordis.Unlock()
	waiting := []string{}
	if pkg.host != "" {
		var err error
		waiting, err = e.activateDynamicCordisRun(run, run.ownerSessionID())
		if err != nil {
			e.dynamicCordis.Lock()
			if current := e.dynamicCordis.plugins[pluginID]; current != nil && current.run == run {
				current.run = nil
			}
			e.dynamicCordis.Unlock()
			e.cleanupDynamicCordisRun(run, true)
			return finish(nil, false, err)
		}
		e.activateAvailableDynamicCordisRuns()
	}
	e.dynamicCordis.Lock()
	plugin = e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.run != run {
		e.dynamicCordis.Unlock()
		return finish(nil, false, errors.New("dynamic plugin is unavailable"))
	}
	if dynamicCordisAttemptRunID(plugin.latest) == runID {
		hostStatus := "absent"
		if pkg.host != "" {
			hostStatus = "running"
			if len(waiting) > 0 {
				hostStatus = "waiting"
			}
		}
		dynamicCordisSetAttemptHalf(plugin.latest, "host", hostStatus, waiting, "")
		if pkg.client == "" {
			plugin.latest["status"] = hostStatus
			plugin.current = packageID
			plugin.next = ""
		} else {
			plugin.latest["status"] = "client-pending"
			dynamicCordisSetAttemptHalf(plugin.latest, "client", "pending", []string{}, "")
		}
	}
	e.dynamicCordis.Unlock()
	e.emitCordisEvent("cordis/dynamic-package", map[string]any{"pluginId": pluginID, "packageId": packageID, "pluginRunId": runID, "name": pkg.name})
	return finish(waiting, true, nil)
}

func dynamicJSMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (e *Engine) DynamicCordisRunHostHalf(ctx context.Context, sessionID, pluginID, packageID, mode, requestID string, approveFuture bool) (DynamicCordisHostHalfResult, error) {
	e.dynamicCordis.Lock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID {
		e.dynamicCordis.Unlock()
		return DynamicCordisHostHalfResult{Message: "dynamic plugin is unavailable"}, nil
	}
	if plugin.packages[packageID] == nil {
		e.dynamicCordis.Unlock()
		return DynamicCordisHostHalfResult{Message: "dynamic package is unavailable"}, nil
	}
	runID := ""
	if requestID != "" {
		pending := e.dynamicCordis.pendingRuns[requestID]
		if pending == nil || pending.agentID != sessionID || pending.pluginID != pluginID || pending.packageID != packageID || pending.mode != mode {
			e.dynamicCordis.Unlock()
			return DynamicCordisHostHalfResult{Message: "run request does not authorize this package"}, nil
		}
		runID = pending.runID
		if pending.requiresApproval {
			if approveFuture {
				plugin.approveFuture = true
			}
			plugin.approved[packageID] = true
		}
	} else {
		for _, pending := range e.dynamicCordis.pendingRuns {
			if pending.pluginID == pluginID {
				e.dynamicCordis.Unlock()
				return DynamicCordisHostHalfResult{Message: "dynamic plugin has a pending run request"}, nil
			}
		}
		if mode == "update" && (plugin.current == "" || plugin.current == packageID) {
			e.dynamicCordis.Unlock()
			return DynamicCordisHostHalfResult{Message: "mode update requires a different existing current package"}, nil
		}
		if mode == "run" && plugin.current != "" && plugin.current != packageID {
			e.dynamicCordis.Unlock()
			return DynamicCordisHostHalfResult{Message: "mode run can only start or restart the current package"}, nil
		}
		if plugin.run != nil && plugin.run.packageID == packageID && dynamicCordisAttemptRunID(plugin.latest) == plugin.run.runID {
			runID = plugin.run.runID
		} else if plugin.run == nil && plugin.next == packageID &&
			dynamicCordisAttemptRunID(plugin.latest) != "" &&
			dynamicCordisAttemptString(plugin.latest, "packageId") == packageID &&
			dynamicCordisAttemptString(plugin.latest, "mode") == mode &&
			dynamicCordisAttemptString(plugin.latest, "status") == "starting-host" {
			runID = dynamicCordisAttemptRunID(plugin.latest)
		} else {
			runID = fmt.Sprintf("run-%d", e.dynamicCordis.nextRun)
			e.dynamicCordis.nextRun++
			plugin.next = packageID
			plugin.latest = dynamicCordisAttempt(runID, packageID, mode, plugin.packages[packageID])
			if plugin.packages[packageID].client != "" {
				plugin.approved[packageID] = true
			}
		}
	}
	e.dynamicCordis.Unlock()
	waiting, startedHere, err := e.dynamicCordisStartHost(ctx, sessionID, pluginID, packageID, runID, requestID)
	if err != nil {
		e.dynamicCordisFailAttempt(pluginID, runID, "host-load", err.Error())
		return DynamicCordisHostHalfResult{Message: err.Error()}, nil
	}
	return DynamicCordisHostHalfResult{OK: true, PluginID: pluginID, PackageID: packageID, PluginRunID: runID, WaitingFor: waiting, StartedHere: startedHere}, nil
}

func (e *Engine) DynamicCordisGetClientCode(sessionID, pluginID, runID string) (DynamicCordisClientSource, error) {
	e.dynamicCordis.RLock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID || plugin.run == nil || plugin.run.runID != runID {
		e.dynamicCordis.RUnlock()
		return DynamicCordisClientSource{}, errors.New("dynamic plugin is not running this activation")
	}
	pkg := plugin.packages[plugin.run.packageID]
	if pkg == nil || pkg.client == "" {
		e.dynamicCordis.RUnlock()
		return DynamicCordisClientSource{}, errors.New("dynamic package has no Client half")
	}
	value := DynamicCordisClientSource{Code: pkg.client, Name: pkg.name, PluginID: pluginID, PackageID: pkg.id, PluginRunID: runID}
	e.dynamicCordis.RUnlock()
	return value, nil
}

func (e *Engine) DynamicCordisResolveRequestRun(requestID string, resolution DynamicCordisRunResolution) (map[string]any, error) {
	e.dynamicCordis.Lock()
	pending := e.dynamicCordis.pendingRuns[requestID]
	if pending == nil {
		e.dynamicCordis.Unlock()
		return map[string]any{"accepted": false}, nil
	}
	plugin := e.dynamicCordis.plugins[pending.pluginID]
	if resolution.OK {
		if plugin == nil || plugin.run == nil || plugin.run.runID != resolution.PluginRunID {
			e.dynamicCordis.Unlock()
			return map[string]any{"accepted": false}, nil
		}
	} else if resolution.PluginRunID != "" && (plugin == nil || plugin.run == nil || plugin.run.runID != resolution.PluginRunID) {
		e.dynamicCordis.Unlock()
		return map[string]any{"accepted": false}, nil
	}
	delete(e.dynamicCordis.pendingRuns, requestID)
	var retract *dynamicCordisRun
	if !resolution.OK {
		if plugin != nil && len(plugin.latest) > 0 && (resolution.PluginRunID == "" || dynamicCordisAttemptRunID(plugin.latest) == resolution.PluginRunID) {
			if resolution.Reason == "rejected" {
				plugin.latest["status"] = "rejected"
				dynamicCordisSetAttemptHalf(plugin.latest, "client", "stopped", []string{}, "")
			} else {
				if plugin.run != nil && plugin.run.runID == resolution.PluginRunID && plugin.run.startedForRequest == requestID && (resolution.StartedHere == nil || *resolution.StartedHere) {
					retract = plugin.run
					plugin.run = nil
				}
				plugin.latest["status"] = "failed"
				half := "client"
				if resolution.Reason == "host-half-failed" {
					half = "host"
				}
				dynamicCordisSetAttemptHalf(plugin.latest, half, "failed", []string{}, resolution.Message)
			}
		}
		e.dynamicCordis.Unlock()
		if retract != nil {
			e.disposeDynamicCordisRun(retract)
			e.emitCordisEvent("cordis/dynamic-retract", map[string]any{"pluginId": pending.pluginID, "packageId": retract.packageID, "pluginRunId": retract.runID})
		}
		outcome := "failed"
		if resolution.Reason == "rejected" {
			outcome = "rejected"
		}
		e.emitCordisEvent("cordis/request-run-resolved", map[string]any{"requestId": requestID, "outcome": outcome})
		return map[string]any{"accepted": true}, nil
	}
	dynamicCordisCommitActivation(plugin, plugin.run, resolution.WaitingFor)
	e.dynamicCordis.Unlock()
	outcome := "completed"
	if pending.requiresApproval {
		outcome = "approved"
	}
	e.emitCordisEvent("cordis/request-run-resolved", map[string]any{"requestId": requestID, "outcome": outcome})
	return map[string]any{"accepted": true}, nil
}

func (e *Engine) DynamicCordisSettleUserRun(sessionID, pluginID string, resolution DynamicCordisRunResolution) (DynamicCordisRunResponse, error) {
	e.dynamicCordis.Lock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID {
		e.dynamicCordis.Unlock()
		return DynamicCordisRunResponse{Reason: "plugin-missing", Message: "dynamic plugin is unavailable"}, nil
	}
	if !resolution.OK {
		if resolution.Reason == "rejected" {
			if len(plugin.latest) > 0 {
				plugin.latest["status"] = "rejected"
				dynamicCordisSetAttemptHalf(plugin.latest, "client", "stopped", []string{}, "")
			}
			e.dynamicCordis.Unlock()
			message := resolution.Message
			if message == "" {
				message = "the run request was declined"
			}
			return DynamicCordisRunResponse{Reason: "rejected", Message: message}, nil
		}
		run := plugin.run
		retract := false
		if run != nil && run.runID == resolution.PluginRunID && (resolution.StartedHere == nil || *resolution.StartedHere) {
			plugin.run = nil
			retract = true
		}
		if len(plugin.latest) > 0 && (resolution.PluginRunID == "" || dynamicCordisAttemptRunID(plugin.latest) == resolution.PluginRunID) {
			plugin.latest["status"] = "failed"
			half := "client"
			if resolution.Reason == "host-half-failed" {
				half = "host"
			}
			dynamicCordisSetAttemptHalf(plugin.latest, half, "failed", []string{}, resolution.Message)
		}
		e.dynamicCordis.Unlock()
		if retract {
			e.disposeDynamicCordisRun(run)
			e.emitCordisEvent("cordis/dynamic-retract", map[string]any{"pluginId": pluginID, "packageId": run.packageID, "pluginRunId": run.runID})
		}
		reason := resolution.Reason
		if reason == "" {
			reason = "client-half-failed"
		}
		message := resolution.Message
		if message == "" {
			message = reason
		}
		return DynamicCordisRunResponse{Reason: reason, Message: message, Stack: resolution.Stack}, nil
	}
	if plugin.run == nil || plugin.run.runID != resolution.PluginRunID {
		e.dynamicCordis.Unlock()
		return DynamicCordisRunResponse{Reason: "client-half-failed", Message: fmt.Sprintf("activation %q is no longer active", resolution.PluginRunID)}, nil
	}
	runID, packageID := plugin.run.runID, plugin.run.packageID
	mode, hostWaiting := dynamicCordisCommitActivation(plugin, plugin.run, resolution.WaitingFor)
	e.dynamicCordis.Unlock()
	return DynamicCordisRunResponse{OK: true, Status: "running", PluginID: pluginID, PackageID: packageID, PluginRunID: runID, WaitingFor: hostWaiting, ClientWaitingFor: append([]string{}, resolution.WaitingFor...), CurrentPackageID: packageID, Mode: mode}, nil
}

func (e *Engine) dynamicCordisFailAttempt(pluginID, runID, phase, message string) {
	e.dynamicCordis.Lock()
	defer e.dynamicCordis.Unlock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || dynamicCordisAttemptRunID(plugin.latest) != runID {
		return
	}
	plugin.latest["status"] = "failed"
	half := "host"
	if phase != "host-load" {
		half = "client"
	}
	dynamicCordisSetAttemptHalf(plugin.latest, half, "failed", []string{}, message)
}

func (e *Engine) DynamicCordisStop(sessionID, pluginID string) (DynamicCordisStopResponse, error) {
	var cancelled []string
	var run *dynamicCordisRun
	e.dynamicCordis.Lock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID {
		e.dynamicCordis.Unlock()
		return DynamicCordisStopResponse{Reason: "plugin-missing", Message: "dynamic plugin is unavailable"}, nil
	}
	for id, pending := range e.dynamicCordis.pendingRuns {
		if pending.pluginID == pluginID {
			delete(e.dynamicCordis.pendingRuns, id)
			cancelled = append(cancelled, id)
		}
	}
	if plugin.run != nil {
		run = plugin.run
		plugin.run = nil
	}
	if run == nil && len(cancelled) == 0 {
		e.dynamicCordis.Unlock()
		return DynamicCordisStopResponse{Reason: "not-running", Message: "dynamic plugin is not running"}, nil
	}
	if len(plugin.latest) > 0 {
		plugin.latest["status"] = "stopped"
		delete(plugin.latest, "approvalRequestId")
		delete(plugin.latest, "requiresApproval")
		for _, half := range []string{"host", "client"} {
			state := dynamicCordisAttemptHalf(plugin.latest, half)
			if dynamicCordisAttemptString(state, "status") != "absent" {
				dynamicCordisSetAttemptHalf(plugin.latest, half, "stopped", []string{}, "")
			}
		}
	}
	e.dynamicCordis.Unlock()
	for _, id := range cancelled {
		e.emitCordisEvent("cordis/request-run-resolved", map[string]any{"requestId": id, "outcome": "cancelled"})
	}
	if run != nil {
		e.disposeDynamicCordisRun(run)
		e.emitCordisEvent("cordis/dynamic-retract", map[string]any{"pluginId": pluginID, "packageId": run.packageID, "pluginRunId": run.runID})
	}
	return DynamicCordisStopResponse{OK: true}, nil
}

func (e *Engine) DynamicCordisUndefine(sessionID, pluginID string) (DynamicCordisUndefineReceipt, error) {
	wasRunning := false
	var cancelled []string
	e.dynamicCordis.Lock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID {
		e.dynamicCordis.Unlock()
		return DynamicCordisUndefineReceipt{Reason: "plugin-missing", Message: "dynamic plugin is unavailable"}, nil
	}
	wasRunning = plugin.run != nil
	var run *dynamicCordisRun
	if plugin.run != nil {
		run = plugin.run
	}
	for id, pending := range e.dynamicCordis.pendingRuns {
		if pending.pluginID == pluginID {
			delete(e.dynamicCordis.pendingRuns, id)
			cancelled = append(cancelled, id)
		}
	}
	delete(e.dynamicCordis.plugins, pluginID)
	e.dynamicCordis.Unlock()
	for _, id := range cancelled {
		e.emitCordisEvent("cordis/request-run-resolved", map[string]any{"requestId": id, "outcome": "cancelled"})
	}
	if run != nil {
		e.disposeDynamicCordisRun(run)
		e.emitCordisEvent("cordis/dynamic-retract", map[string]any{"pluginId": pluginID, "packageId": run.packageID, "pluginRunId": run.runID})
	}
	return DynamicCordisUndefineReceipt{OK: true, WasRunning: wasRunning}, nil
}

func (e *Engine) DynamicCordisInvoke(ctx context.Context, pluginID, runID, method string, args any) DynamicCordisInvokeResult {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return DynamicCordisInvokeResult{Code: "handler-error", Message: err.Error()}
	}
	ready := make(chan DynamicCordisInvokeResult, 1)
	if !e.dynamicCordis.loop.post(func() {
		e.dynamicCordis.RLock()
		plugin := e.dynamicCordis.plugins[pluginID]
		if plugin == nil || plugin.run == nil {
			e.dynamicCordis.RUnlock()
			ready <- DynamicCordisInvokeResult{Code: "plugin-not-running", Message: fmt.Sprintf("dynamic plugin %q is not running", pluginID)}
			return
		}
		run := plugin.run
		e.dynamicCordis.RUnlock()
		if run.runID != runID {
			ready <- DynamicCordisInvokeResult{Code: "stale-run", Message: "activation is no longer active"}
			return
		}
		run.mu.Lock()
		active := !run.disposed && (run.active || run.activating)
		run.mu.Unlock()
		if !active {
			ready <- DynamicCordisInvokeResult{Code: "plugin-not-running", Message: fmt.Sprintf("dynamic plugin %q is not running", pluginID)}
			return
		}
		fn := run.handlers[method]
		if fn == nil {
			ready <- DynamicCordisInvokeResult{Code: "method-not-found", Message: fmt.Sprintf("dynamic plugin %q registered no Host method %q", pluginID, method)}
			return
		}
		failure := func(err error) DynamicCordisInvokeResult {
			message, stack := dynamicJSErrorDetails(err)
			e.reportDynamicCordisHandlerFailure(run, method, message, stack)
			return DynamicCordisInvokeResult{Code: "handler-error", Message: message, Stack: stack}
		}
		value, err := fn(goja.Undefined(), run.runtime.ToValue(args))
		if err != nil {
			ready <- failure(err)
			return
		}
		dynamicCordisAwaitOnLoop(run, value, func(value goja.Value, err error) {
			if err != nil {
				ready <- failure(err)
				return
			}
			out, err := dynamicCordisJSONValue(value)
			if err != nil {
				ready <- failure(fmt.Errorf("harness.handle(%q) result: %w", method, err))
				return
			}
			ready <- DynamicCordisInvokeResult{OK: true, Value: out}
		})
	}) {
		return DynamicCordisInvokeResult{Code: "plugin-not-running", Message: "dynamic Cordis runtime is closed"}
	}
	select {
	case result := <-ready:
		return result
	case <-ctx.Done():
		return DynamicCordisInvokeResult{Code: "handler-error", Message: ctx.Err().Error()}
	}
}

func (e *Engine) reportDynamicCordisHandlerFailure(run *dynamicCordisRun, method, message, stack string) {
	key := "Host\x00handler\x00" + method + "\x00" + message
	if !run.claimDynamicCordisRuntimeError(key) {
		return
	}
	e.steerDynamicCordisFrom(run, run.sessionID, fmt.Sprintf(
		"Cordis Host handler %s/%s (%s) failed when the Client called host.call(%q).\n%s\nThe Plugin remains running. Inspect this Package, correct the Host code on the same Plugin, and activate the new Package autonomously with cordis_run mode:\"update\". If the handler needs a Service, either declare that Service in the returned Plugin inject list or read it with ctx.get(name) and handle undefined.",
		run.pluginID, run.packageID, run.runID, method, formatDynamicCordisError(message, stack),
	))
}

func (e *Engine) DynamicCordisReportRenderFailure(sessionID, pluginID, runID string, failure DynamicCordisRenderFailure) {
	e.dynamicCordis.Lock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID || plugin.run == nil || plugin.run.runID != runID {
		e.dynamicCordis.Unlock()
		return
	}
	shouldSteer := plugin.run.renderFailure == nil
	copy := failure
	plugin.run.renderFailure = &copy
	if dynamicCordisAttemptRunID(plugin.latest) == runID {
		plugin.latest["status"] = "failed"
		plugin.latest["error"] = map[string]any{
			"phase": "client-render", "slot": failure.Slot, "message": failure.Message,
			"stack": failure.Stack, "abdicated": failure.Abdicated,
			"pluginId": pluginID, "packageId": plugin.run.packageID, "pluginRunId": runID,
		}
		waiting := dynamicCordisAttemptWaiting(plugin.latest, "client")
		dynamicCordisSetAttemptHalf(plugin.latest, "client", "failed", waiting, failure.Message)
	}
	packageID := plugin.run.packageID
	e.dynamicCordis.Unlock()
	if shouldSteer {
		e.steerDynamicCordis(sessionID, fmt.Sprintf(
			"Cordis Client UI %s/%s (%s) failed while rendering Slot %q after activation.\n%s\nentryAbdicated: %t\nInspect the failed Package, fix the Client code by defining a new Package on the same Plugin, and activate that Package autonomously with cordis_run mode:\"update\".",
			pluginID, packageID, runID, failure.Slot, formatDynamicCordisError(failure.Message, failure.Stack), failure.Abdicated,
		))
	}
}

func (e *Engine) DynamicCordisReportClientGuardFailure(sessionID, pluginID, runID string, failure DynamicCordisErrorDetails) {
	e.dynamicCordis.RLock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID || plugin.run == nil || plugin.run.runID != runID {
		e.dynamicCordis.RUnlock()
		return
	}
	run := plugin.run
	packageID := run.packageID
	key := "Client\x00guard\x00" + failure.Message
	claimed := run.claimDynamicCordisRuntimeError(key)
	e.dynamicCordis.RUnlock()
	if !claimed {
		return
	}
	e.steerDynamicCordis(sessionID, fmt.Sprintf(
		"Cordis Client guard rejected runtime code in %s/%s (%s) after activation.\n%s\nThe Plugin remains running. Inspect this Package, define a corrected Package on the same Plugin, and activate it autonomously with cordis_run mode:\"update\".",
		pluginID, packageID, runID, formatDynamicCordisError(failure.Message, failure.Stack),
	))
}

func (run *dynamicCordisRun) claimDynamicCordisRuntimeError(key string) bool {
	run.reportMu.Lock()
	defer run.reportMu.Unlock()
	if _, exists := run.reportedErrors[key]; exists {
		return false
	}
	run.reportedErrors[key] = struct{}{}
	return true
}

func formatDynamicCordisError(message, stack string) string {
	text := "message: " + message
	if stack != "" {
		text += "\nstack:\n" + stack
	}
	return text
}

func (e *Engine) steerDynamicCordis(sessionID, text string) {
	e.steerDynamicCordisFrom(nil, sessionID, text)
}

func (e *Engine) steerDynamicCordisFrom(origin *dynamicCordisRun, sessionID, text string) {
	_, _, _ = e.enqueuePromptFrom(context.Background(), sessionID, PromptRequest{
		SessionID: sessionID,
		Mode:      "steer",
		Literal:   true,
		Content:   []PromptContentPart{{Type: "text", Text: text}},
		Source:    map[string]any{"kind": "plugin", "plugin": "cordis-host-runner"},
	}, false, origin)
}

func (e *Engine) DynamicCordisInventory(sessionID string) []DynamicCordisInventoryRow {
	e.dynamicCordis.RLock()
	defer e.dynamicCordis.RUnlock()
	rows := make([]DynamicCordisInventoryRow, 0)
	for _, pluginID := range e.dynamicCordis.pluginOrder {
		plugin := e.dynamicCordis.plugins[pluginID]
		if plugin == nil {
			continue
		}
		if sessionID != "" && plugin.sessionID != sessionID {
			continue
		}
		row := DynamicCordisInventoryRow{PluginID: plugin.id, AgentID: plugin.sessionID, Packages: make([]DynamicCordisInventoryPackage, 0, len(plugin.order)), CurrentPackageID: plugin.current, NextPackageID: plugin.next}
		for _, id := range plugin.order {
			pkg := plugin.packages[id]
			row.Packages = append(row.Packages, DynamicCordisInventoryPackage{PackageID: pkg.id, Name: pkg.name, Purpose: pkg.purpose, HasHostHalf: pkg.host != "", HasClientHalf: pkg.client != ""})
		}
		if plugin.run != nil {
			row.ActiveRun = &DynamicCordisActiveRun{PluginRunID: plugin.run.runID, PackageID: plugin.run.packageID}
			if plugin.run.renderFailure != nil {
				failure := *plugin.run.renderFailure
				row.ActiveRun.RenderFailure = &failure
			}
		}
		if plugin.latest != nil {
			row.LatestRun = cloneJSON(plugin.latest)
		}
		rows = append(rows, row)
	}
	return rows
}

func (e *Engine) DynamicCordisInspectSelf(sessionID, pluginID, packageID string) (map[string]any, error) {
	e.dynamicCordis.RLock()
	defer e.dynamicCordis.RUnlock()
	if pluginID == "" {
		plugins := make([]map[string]any, 0)
		for _, id := range e.dynamicCordis.pluginOrder {
			plugin := e.dynamicCordis.plugins[id]
			if plugin == nil || plugin.sessionID != sessionID {
				continue
			}
			plugins = append(plugins, dynamicCordisSelfSummaryLocked(plugin))
		}
		return map[string]any{"mode": "plugins", "plugins": plugins}, nil
	}
	p := e.dynamicCordis.plugins[pluginID]
	if p == nil || p.sessionID != sessionID {
		return nil, errors.New("dynamic plugin is unavailable")
	}
	if packageID == "" {
		row := dynamicCordisSelfSummaryLocked(p)
		packages := make([]map[string]any, 0, len(p.order))
		for _, id := range p.order {
			pkg := p.packages[id]
			packages = append(packages, map[string]any{
				"packageId":     id,
				"name":          pkg.name,
				"purpose":       pkg.purpose,
				"hasHostHalf":   pkg.host != "",
				"hasClientHalf": pkg.client != "",
				"isCurrent":     id == p.current,
				"isNext":        id == p.next,
			})
		}
		row["mode"] = "plugin"
		row["packages"] = packages
		return row, nil
	}
	pkg := p.packages[packageID]
	if pkg == nil {
		return nil, errors.New("dynamic package is unavailable")
	}
	code := map[string]any{}
	if pkg.host != "" {
		code["host"] = pkg.host
	}
	if pkg.client != "" {
		code["client"] = pkg.client
	}
	active := p.run
	if active != nil && active.packageID != packageID {
		active = nil
	}
	latest := p.latest
	if dynamicCordisAttemptString(latest, "packageId") != packageID {
		latest = nil
	}
	hostWaiting := dynamicCordisAttemptWaiting(latest, "host")
	provided := []string{}
	handlers := []string{}
	if active != nil {
		hostWaiting = dynamicCordisMissingServicesLocked(e, active)
		for name := range active.provided {
			provided = append(provided, name)
		}
		sort.Strings(provided)
		for name := range active.handlers {
			handlers = append(handlers, name)
		}
		sort.Strings(handlers)
	}
	hostStatus := "absent"
	if pkg.host != "" {
		hostStatus = dynamicCordisAttemptString(dynamicCordisAttemptHalf(latest, "host"), "status")
		if hostStatus == "" {
			hostStatus = "stopped"
			if active != nil {
				hostStatus = "running"
				if len(hostWaiting) > 0 {
					hostStatus = "waiting"
				}
			}
		}
	}
	clientStatus := "absent"
	if pkg.client != "" {
		clientStatus = dynamicCordisAttemptString(dynamicCordisAttemptHalf(latest, "client"), "status")
		if clientStatus == "" {
			clientStatus = "stopped"
		}
	}
	hostRuntime := map[string]any{"status": hostStatus, "provides": provided, "waitingFor": hostWaiting, "handlers": handlers}
	clientRuntime := map[string]any{"status": clientStatus, "waitingFor": dynamicCordisAttemptWaiting(latest, "client")}
	if errText := dynamicCordisAttemptString(dynamicCordisAttemptHalf(latest, "host"), "error"); errText != "" {
		hostRuntime["error"] = errText
	}
	if errText := dynamicCordisAttemptString(dynamicCordisAttemptHalf(latest, "client"), "error"); errText != "" {
		clientRuntime["error"] = errText
	}
	if active != nil && active.renderFailure != nil {
		clientRuntime["renderFailure"] = *active.renderFailure
	}
	return map[string]any{
		"mode":      "package",
		"plugin":    dynamicCordisSelfSummaryLocked(p),
		"packageId": packageID,
		"name":      pkg.name,
		"purpose":   pkg.purpose,
		"code":      code,
		"runtime": map[string]any{
			"state":  dynamicCordisSelfStateLocked(p),
			"host":   hostRuntime,
			"client": clientRuntime,
		},
	}, nil
}

func dynamicCordisPreferredPackageLocked(p *dynamicCordisPlugin) *dynamicCordisPackage {
	packageID := p.next
	if packageID == "" {
		packageID = p.current
	}
	if packageID == "" && len(p.order) > 0 {
		packageID = p.order[len(p.order)-1]
	}
	return p.packages[packageID]
}

func dynamicCordisSelfStateLocked(p *dynamicCordisPlugin) string {
	switch status := dynamicCordisAttemptString(p.latest, "status"); status {
	case "awaiting-approval":
		return "awaiting-approval"
	case "client-pending", "starting-host":
		return "client-pending"
	case "failed", "rejected", "cancelled":
		return "failed"
	case "waiting", "running":
		return status
	}
	if p.run != nil {
		return "running"
	}
	if p.current == "" {
		return "defined"
	}
	return "stopped"
}

func dynamicCordisSelfSummaryLocked(p *dynamicCordisPlugin) map[string]any {
	pkg := dynamicCordisPreferredPackageLocked(p)
	name := ""
	if pkg != nil {
		name = pkg.name
	}
	row := map[string]any{
		"pluginId":     p.id,
		"name":         name,
		"packageCount": len(p.order),
		"state":        dynamicCordisSelfStateLocked(p),
	}
	if p.current != "" {
		row["currentPackageId"] = p.current
	}
	if p.next != "" {
		row["nextPackageId"] = p.next
	}
	if p.run != nil {
		row["activeRun"] = map[string]any{"pluginRunId": p.run.runID, "packageId": p.run.packageID}
	}
	if dynamicCordisAttemptString(p.latest, "status") == "awaiting-approval" {
		row["pendingApproval"] = map[string]any{
			"pluginRunId": dynamicCordisAttemptRunID(p.latest),
			"packageId":   dynamicCordisAttemptString(p.latest, "packageId"),
			"mode":        dynamicCordisAttemptString(p.latest, "mode"),
		}
	}
	return row
}

func dynamicCordisMissingServicesLocked(e *Engine, run *dynamicCordisRun) []string {
	missing := make([]string, 0)
	for _, name := range run.inject {
		if _, ok := e.dynamicCordis.services[name]; !ok && !e.dynamicCordisBuiltinService(name) {
			missing = append(missing, name)
		}
	}
	return missing
}

func (e *Engine) dynamicCordisReferenceMessages(sessionID string, messages []ChatMessage, runtime agentRuntime) []ChatMessage {
	if runtime.toolNames != nil && !runtime.toolNames["cordis_inspect_self"] {
		return messages
	}
	ids := make([]string, 0)
	seen := map[string]struct{}{}
	for _, message := range messages {
		if message.Role != "user" || stringValue(message.Source["kind"]) != "user" {
			continue
		}
		text := message.Content
		for _, indexes := range dynamicPluginMention.FindAllStringIndex(text, -1) {
			if indexes[0] > 0 {
				before, _ := utf8.DecodeLastRuneInString(text[:indexes[0]])
				if !unicode.IsSpace(before) {
					continue
				}
			}
			if indexes[1] < len(text) {
				after, _ := utf8.DecodeRuneInString(text[indexes[1]:])
				if !unicode.IsSpace(after) {
					continue
				}
			}
			id := text[indexes[0]+1 : indexes[1]]
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return messages
	}
	out := append([]ChatMessage(nil), messages...)
	for _, id := range ids {
		out = append(out, ChatMessage{
			Role:    "user",
			Content: e.renderDynamicCordisReference(sessionID, id),
			Source:  map[string]any{"kind": "plugin", "plugin": "tool-cordis", "form": "instructions"},
		})
	}
	return out
}

func (e *Engine) renderDynamicCordisReference(sessionID, pluginID string) string {
	e.dynamicCordis.RLock()
	plugin := e.dynamicCordis.plugins[pluginID]
	if plugin == nil || plugin.sessionID != sessionID {
		e.dynamicCordis.RUnlock()
		return strings.Join([]string{
			"<cordis_dynamic_plugin_context>",
			fmt.Sprintf("The user explicitly referenced @%s, but this Plugin is unavailable in the current Session.", pluginID),
			"It may have been removed, belong to another Session, or have been lost when the DSH process restarted.",
			"Do not claim that it was updated or silently create a replacement Plugin. Tell the user that the reference is currently unavailable.",
			"</cordis_dynamic_plugin_context>",
		}, "\n")
	}
	pkg := dynamicCordisPreferredPackageLocked(plugin)
	if pkg == nil {
		e.dynamicCordis.RUnlock()
		return "<cordis_dynamic_plugin_context>\nThe referenced Plugin has no Package.\n</cordis_dynamic_plugin_context>"
	}
	reference := map[string]any{
		"pluginId": plugin.id, "packageId": pkg.id, "name": pkg.name, "purpose": pkg.purpose,
	}
	if plugin.current != "" {
		reference["currentPackageId"] = plugin.current
	}
	if plugin.next != "" {
		reference["nextPackageId"] = plugin.next
	}
	if plugin.run != nil {
		reference["activeRun"] = map[string]any{"pluginRunId": plugin.run.runID, "packageId": plugin.run.packageID}
	}
	if len(plugin.latest) > 0 {
		reference["latestRun"] = cloneJSON(plugin.latest)
	}
	mode := "run"
	if plugin.current != "" {
		mode = "update"
	}
	e.dynamicCordis.RUnlock()
	encoded, _ := json.MarshalIndent(reference, "", "  ")
	return strings.Join([]string{
		"<cordis_dynamic_plugin_context>", string(encoded), "",
		fmt.Sprintf("The user explicitly referenced @%s. Use Package %s as the base for this modification.", pluginID, pkg.id),
		fmt.Sprintf("Before modifying it, call cordis_inspect_self with pluginId=\"%s\" and packageId=\"%s\" to read the exact metadata and source.", pluginID, pkg.id),
		fmt.Sprintf("Use cordis_define with plugin.kind=\"existing\" and the original pluginId=\"%s\" to append an immutable Package.", pluginID),
		fmt.Sprintf("Do not create a new Plugin for this request. After cordis_define succeeds, call cordis_run mode=\"%s\" with the returned packageId.", mode),
		"</cordis_dynamic_plugin_context>",
	}, "\n")
}

func (e *Engine) dynamicCordisInventoryLocked(sessionID string) []map[string]any {
	rows := make([]map[string]any, 0)
	for _, pluginID := range e.dynamicCordis.pluginOrder {
		p := e.dynamicCordis.plugins[pluginID]
		if p == nil {
			continue
		}
		if sessionID != "" && p.sessionID != sessionID {
			continue
		}
		rows = append(rows, e.dynamicCordisPluginViewLocked(p, false))
	}
	return rows
}

func (e *Engine) dynamicCordisPluginViewLocked(p *dynamicCordisPlugin, includePackages bool) map[string]any {
	row := map[string]any{"pluginId": p.id, "agentId": p.sessionID, "packageCount": len(p.order), "state": "defined"}
	if p.current != "" {
		row["currentPackageId"] = p.current
		row["state"] = "stopped"
	}
	if p.next != "" {
		row["nextPackageId"] = p.next
	}
	if p.run != nil {
		row["state"] = "running"
		active := map[string]any{"pluginRunId": p.run.runID, "packageId": p.run.packageID}
		if p.run.renderFailure != nil {
			active["renderFailure"] = *p.run.renderFailure
		}
		row["activeRun"] = active
	}
	if p.latest != nil {
		row["latestRun"] = cloneJSON(p.latest)
		if status, _ := p.latest["status"].(string); status != "" {
			row["state"] = status
		}
	}
	if includePackages {
		packages := make([]map[string]any, 0, len(p.order))
		for _, id := range p.order {
			pkg := p.packages[id]
			packages = append(packages, map[string]any{"packageId": pkg.id, "name": pkg.name, "purpose": pkg.purpose, "hasHostHalf": pkg.host != "", "hasClientHalf": pkg.client != ""})
		}
		row["packages"] = packages
	}
	return row
}
