package harness

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dop251/goja"
)

func (e *Engine) dynamicIndexInjectionRowsNow(rows []IndexInjection) ([]IndexInjection, error) {
	e.dynamicCordis.RLock()
	listeners := make([]*dynamicCordisListener, 0)
	seen := map[*dynamicCordisRun]struct{}{}
	for _, plugin := range e.dynamicCordis.plugins {
		run := plugin.run
		if run == nil {
			continue
		}
		if _, ok := seen[run]; ok {
			continue
		}
		seen[run] = struct{}{}
		run.eventMu.RLock()
		listeners = append(listeners, run.listeners["webserver/index-inject"]...)
		run.eventMu.RUnlock()
	}
	e.dynamicCordis.RUnlock()
	sort.SliceStable(listeners, func(i, j int) bool { return dynamicCordisListenerLess(listeners[i], listeners[j]) })
	for _, listener := range listeners {
		run := listener.run
		if listener.once {
			run.eventMu.Lock()
			for index, current := range run.listeners["webserver/index-inject"] {
				if current != listener {
					continue
				}
				remaining := run.listeners["webserver/index-inject"]
				remaining = append(remaining[:index], remaining[index+1:]...)
				if len(remaining) == 0 {
					delete(run.listeners, "webserver/index-inject")
				} else {
					run.listeners["webserver/index-inject"] = remaining
				}
				break
			}
			run.eventMu.Unlock()
		}
		run.mu.Lock()
		active := !run.disposed && (run.active || run.activating)
		run.mu.Unlock()
		if !active {
			continue
		}
		table := run.runtime.ToValue(cloneJSON(rows))
		value, err := listener.fn(goja.Undefined(), table)
		if err != nil {
			e.reportDynamicCordisHostFailure(run, "event webserver/index-inject", err)
			return nil, errors.New(dynamicJSMessage(err))
		}
		if promise, ok := value.Export().(*goja.Promise); ok {
			return nil, fmt.Errorf("webserver/index-inject listener returned asynchronous work with state %v", promise.State())
		}
		if err := dynamicCordisDecode(table, &rows); err != nil {
			return nil, fmt.Errorf("webserver/index-inject rows: %w", err)
		}
		rows, err = normalizeIndexInjections(rows)
		if err != nil {
			return nil, fmt.Errorf("webserver/index-inject rows: %w", err)
		}
	}
	return rows, nil
}

func (e *Engine) dynamicIndexInjectionRows(caller *dynamicCordisRun, rows []IndexInjection) ([]IndexInjection, error) {
	if caller != nil {
		return e.dynamicIndexInjectionRowsNow(rows)
	}
	var result []IndexInjection
	var err error
	if !e.dynamicCordis.loop.call(func() { result, err = e.dynamicIndexInjectionRowsNow(rows) }) {
		return nil, errors.New("dynamic Cordis runtime is closed")
	}
	return result, err
}

func (e *Engine) dynamicIndexCall(owner, caller *dynamicCordisRun, inactive string, invoke func() (string, error)) (string, error) {
	call := func() (string, error) {
		owner.mu.Lock()
		active := !owner.disposed && (owner.active || owner.activating)
		owner.mu.Unlock()
		if !active {
			return inactive, nil
		}
		return invoke()
	}
	if caller != nil {
		return call()
	}
	var value string
	var err error
	if !e.dynamicCordis.loop.call(func() { value, err = call() }) {
		return "", errors.New("dynamic Cordis runtime is closed")
	}
	return value, err
}

func (e *Engine) dynamicCordisWebServerFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.DefineAccessorProperty("host", vm.ToValue(func() string { return e.cfg.Host }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = service.DefineAccessorProperty("port", vm.ToValue(func() int { return e.dynamicWebServerPort() }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = service.Set("register", func(call goja.FunctionCall) goja.Value {
		object, ok := call.Argument(0).(*goja.Object)
		if !ok {
			panic(vm.ToValue("ctx.webServer.register requires a route object"))
		}
		kind := strings.TrimSpace(object.Get("kind").String())
		path, err := dynamicWebPath(object.Get("path"))
		if err != nil || kind != "exact" && kind != "prefix" {
			panic(vm.ToValue("ctx.webServer.register requires kind exact/prefix and an absolute path without a trailing slash"))
		}
		handler, ok := goja.AssertFunction(object.Get("handler"))
		if !ok {
			panic(vm.ToValue("ctx.webServer.register requires a handler function"))
		}
		registration := newDynamicWebRouteRegistration(run, kind, path, handler)
		if err := e.registerDynamicWebRoute(registration); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(e.dynamicWebRouteDisposer(run, registration, false))
	})
	_ = service.Set("registerUpgrade", func(call goja.FunctionCall) goja.Value {
		object, ok := call.Argument(0).(*goja.Object)
		if !ok {
			panic(vm.ToValue("ctx.webServer.registerUpgrade requires a route object"))
		}
		path, err := dynamicWebPath(object.Get("path"))
		if err != nil {
			panic(vm.ToValue("ctx.webServer.registerUpgrade requires an absolute path without a trailing slash"))
		}
		handler, ok := goja.AssertFunction(object.Get("handler"))
		if !ok {
			panic(vm.ToValue("ctx.webServer.registerUpgrade requires a handler function"))
		}
		registration := newDynamicWebUpgradeRegistration(run, path, handler)
		if err := e.registerDynamicWebUpgrade(registration); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(e.dynamicWebUpgradeDisposer(run, registration))
	})
	_ = service.Set("registerFallback", func(call goja.FunctionCall) goja.Value {
		handler, ok := goja.AssertFunction(call.Argument(0))
		if !ok {
			panic(vm.ToValue("ctx.webServer.registerFallback requires a handler function"))
		}
		registration := newDynamicWebRouteRegistration(run, "fallback", "", handler)
		if err := e.registerDynamicWebFallback(registration); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(e.dynamicWebRouteDisposer(run, registration, true))
	})
	_ = service.Set("tapIndex", func(call goja.FunctionCall) goja.Value {
		transform, ok := goja.AssertFunction(call.Argument(0))
		if !ok {
			panic(vm.ToValue("ctx.webServer.tapIndex requires a transform function"))
		}
		dispose := e.registerIndexTap(func(caller *dynamicCordisRun, html string) (string, error) {
			return e.dynamicIndexCall(run, caller, html, func() (string, error) {
				value, err := transform(goja.Undefined(), vm.ToValue(html))
				if err != nil {
					return "", errors.New(dynamicJSMessage(err))
				}
				if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
					return "", errors.New("ctx.webServer.tapIndex transform must return HTML")
				}
				return value.String(), nil
			})
		})
		var once sync.Once
		cleanup := func() { once.Do(dispose) }
		run.disposers = append(run.disposers, cleanup)
		return vm.ToValue(cleanup)
	})
	_ = service.Set("applyIndexTaps", func(call goja.FunctionCall) goja.Value {
		html, err := e.applyIndexTapsFrom(run, call.Argument(0).String())
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(html)
	})
	_ = service.Set("collectIndexInjections", func(goja.FunctionCall) goja.Value {
		rows, err := e.collectIndexInjectionsFrom(run)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(cloneJSON(rows))
	})
	_ = service.Set("renderIndex", func(call goja.FunctionCall) goja.Value {
		html, err := e.renderIndexFrom(run, call.Argument(0).String())
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(html)
	})
	return service
}

type dynamicWebRouteRegistration struct {
	run     *dynamicCordisRun
	kind    string
	path    string
	handler goja.Callable
	mu      sync.Mutex
	active  map[*dynamicWebHTTPExchange]struct{}
	closed  bool
}

type dynamicWebUpgradeRegistration struct {
	run     *dynamicCordisRun
	path    string
	handler goja.Callable
	mu      sync.Mutex
	sockets map[*dynamicWebSocket]struct{}
	closed  bool
}

type dynamicWebHTTPExchange struct {
	done     chan struct{}
	doneOnce sync.Once
	response *dynamicWebHTTPResponse
}

type dynamicWebHTTPResponse struct {
	engine      *Engine
	run         *dynamicCordisRun
	exchange    *dynamicWebHTTPExchange
	writer      http.ResponseWriter
	mu          sync.Mutex
	statusCode  int
	headersSent bool
	ended       bool
	listeners   map[string][]goja.Callable
}

type dynamicWebSocket struct {
	engine    *Engine
	run       *dynamicCordisRun
	owner     *dynamicWebUpgradeRegistration
	conn      net.Conn
	mu        sync.Mutex
	closed    bool
	reading   bool
	listeners map[string][]goja.Callable
}

func dynamicWebPath(value goja.Value) (string, error) {
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return "", errors.New("missing path")
	}
	path := strings.TrimSpace(value.String())
	if path == "" || path[0] != '/' || strings.ContainsAny(path, "?#") || path != "/" && strings.HasSuffix(path, "/") {
		return "", errors.New("invalid path")
	}
	return path, nil
}

func newDynamicWebRouteRegistration(run *dynamicCordisRun, kind, path string, handler goja.Callable) *dynamicWebRouteRegistration {
	return &dynamicWebRouteRegistration{run: run, kind: kind, path: path, handler: handler, active: map[*dynamicWebHTTPExchange]struct{}{}}
}

func newDynamicWebUpgradeRegistration(run *dynamicCordisRun, path string, handler goja.Callable) *dynamicWebUpgradeRegistration {
	return &dynamicWebUpgradeRegistration{run: run, path: path, handler: handler, sockets: map[*dynamicWebSocket]struct{}{}}
}

func dynamicWebRouteKey(kind, path string) string { return kind + "\x00" + path }

func (e *Engine) registerDynamicWebRoute(registration *dynamicWebRouteRegistration) error {
	key := dynamicWebRouteKey(registration.kind, registration.path)
	e.dynamicCordis.Lock()
	defer e.dynamicCordis.Unlock()
	if _, exists := e.dynamicCordis.webRoutes[key]; exists {
		return fmt.Errorf("webserver: duplicate %s route %q", registration.kind, registration.path)
	}
	e.dynamicCordis.webRoutes[key] = registration
	return nil
}

func (e *Engine) registerDynamicWebFallback(registration *dynamicWebRouteRegistration) error {
	e.dynamicCordis.Lock()
	defer e.dynamicCordis.Unlock()
	if e.dynamicCordis.webFallback != nil {
		return errors.New("webserver: fallback already registered")
	}
	e.dynamicCordis.webFallback = registration
	return nil
}

func (e *Engine) registerDynamicWebUpgrade(registration *dynamicWebUpgradeRegistration) error {
	e.dynamicCordis.Lock()
	defer e.dynamicCordis.Unlock()
	if _, exists := e.dynamicCordis.webUpgrades[registration.path]; exists {
		return fmt.Errorf("webserver: duplicate upgrade route %q", registration.path)
	}
	e.dynamicCordis.webUpgrades[registration.path] = registration
	return nil
}

func (e *Engine) dynamicWebRouteDisposer(run *dynamicCordisRun, registration *dynamicWebRouteRegistration, fallback bool) func() {
	var once sync.Once
	dispose := func() {
		once.Do(func() {
			e.dynamicCordis.Lock()
			if fallback {
				if e.dynamicCordis.webFallback == registration {
					e.dynamicCordis.webFallback = nil
				}
			} else {
				key := dynamicWebRouteKey(registration.kind, registration.path)
				if e.dynamicCordis.webRoutes[key] == registration {
					delete(e.dynamicCordis.webRoutes, key)
				}
			}
			e.dynamicCordis.Unlock()
			registration.close()
		})
	}
	run.disposers = append(run.disposers, dispose)
	return dispose
}

func (e *Engine) dynamicWebUpgradeDisposer(run *dynamicCordisRun, registration *dynamicWebUpgradeRegistration) func() {
	var once sync.Once
	dispose := func() {
		once.Do(func() {
			e.dynamicCordis.Lock()
			if e.dynamicCordis.webUpgrades[registration.path] == registration {
				delete(e.dynamicCordis.webUpgrades, registration.path)
			}
			e.dynamicCordis.Unlock()
			registration.close()
		})
	}
	run.disposers = append(run.disposers, dispose)
	return dispose
}

func (registration *dynamicWebRouteRegistration) attach(exchange *dynamicWebHTTPExchange) bool {
	registration.mu.Lock()
	defer registration.mu.Unlock()
	if registration.closed {
		return false
	}
	registration.active[exchange] = struct{}{}
	return true
}

func (registration *dynamicWebRouteRegistration) detach(exchange *dynamicWebHTTPExchange) {
	registration.mu.Lock()
	delete(registration.active, exchange)
	registration.mu.Unlock()
}

func (registration *dynamicWebRouteRegistration) close() {
	registration.mu.Lock()
	registration.closed = true
	active := make([]*dynamicWebHTTPExchange, 0, len(registration.active))
	for exchange := range registration.active {
		active = append(active, exchange)
	}
	registration.mu.Unlock()
	for _, exchange := range active {
		exchange.finish()
	}
}

func (registration *dynamicWebUpgradeRegistration) attach(socket *dynamicWebSocket) bool {
	registration.mu.Lock()
	defer registration.mu.Unlock()
	if registration.closed {
		return false
	}
	registration.sockets[socket] = struct{}{}
	return true
}

func (registration *dynamicWebUpgradeRegistration) detach(socket *dynamicWebSocket) {
	registration.mu.Lock()
	delete(registration.sockets, socket)
	registration.mu.Unlock()
}

func (registration *dynamicWebUpgradeRegistration) close() {
	registration.mu.Lock()
	registration.closed = true
	sockets := make([]*dynamicWebSocket, 0, len(registration.sockets))
	for socket := range registration.sockets {
		sockets = append(sockets, socket)
	}
	registration.mu.Unlock()
	for _, socket := range sockets {
		socket.close("")
	}
}

func (exchange *dynamicWebHTTPExchange) finish() {
	exchange.doneOnce.Do(func() { close(exchange.done) })
}

func (e *Engine) dynamicWebServerPort() int {
	e.dynamicCordis.RLock()
	port := e.dynamicCordis.webPort
	e.dynamicCordis.RUnlock()
	if port == 0 {
		return e.cfg.Port
	}
	return port
}

// SetWebServerListener publishes an already-bound listener to dynamic Host plugins.
func (e *Engine) SetWebServerListener(listener net.Listener) {
	if listener == nil {
		return
	}
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return
	}
	e.dynamicCordis.Lock()
	e.dynamicCordis.webPort = port
	e.dynamicCordis.Unlock()
}

func (e *Engine) matchDynamicWebRoute(path string) *dynamicWebRouteRegistration {
	e.dynamicCordis.RLock()
	defer e.dynamicCordis.RUnlock()
	if exact := e.dynamicCordis.webRoutes[dynamicWebRouteKey("exact", path)]; exact != nil {
		return exact
	}
	var best *dynamicWebRouteRegistration
	for key, route := range e.dynamicCordis.webRoutes {
		if !strings.HasPrefix(key, "prefix\x00") || path != route.path && !strings.HasPrefix(path, route.path+"/") {
			continue
		}
		if best == nil || len(route.path) > len(best.path) {
			best = route
		}
	}
	return best
}

func (e *Engine) dynamicWebNamedHandler(w http.ResponseWriter, r *http.Request) bool {
	registration := e.matchDynamicWebRoute(r.URL.Path)
	if registration == nil {
		return false
	}
	return e.invokeDynamicWebRoute(registration, w, r)
}

func (e *Engine) dynamicWebFallbackHandler(w http.ResponseWriter, r *http.Request) bool {
	e.dynamicCordis.RLock()
	registration := e.dynamicCordis.webFallback
	e.dynamicCordis.RUnlock()
	if registration == nil {
		return false
	}
	return e.invokeDynamicWebRoute(registration, w, r)
}

func (e *Engine) invokeDynamicWebRoute(registration *dynamicWebRouteRegistration, w http.ResponseWriter, r *http.Request) bool {
	limit := e.cfg.MaxBodyBytes
	if limit <= 0 {
		limit = 16 << 20
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return true
	}
	if int64(len(body)) > limit {
		http.Error(w, "request entity too large", http.StatusRequestEntityTooLarge)
		return true
	}
	exchange := &dynamicWebHTTPExchange{done: make(chan struct{})}
	response := &dynamicWebHTTPResponse{engine: e, run: registration.run, exchange: exchange, writer: w, statusCode: http.StatusOK, listeners: map[string][]goja.Callable{}}
	exchange.response = response
	if !registration.attach(exchange) {
		return false
	}
	defer registration.detach(exchange)
	handlerDone := make(chan error, 1)
	if !e.dynamicCordis.loop.post(func() {
		registration.run.mu.Lock()
		active := !registration.run.disposed && (registration.run.active || registration.run.activating)
		registration.run.mu.Unlock()
		if !active {
			handlerDone <- errors.New("dynamic web route is no longer active")
			return
		}
		request := e.dynamicWebRequestValue(registration.run, r, body)
		responseValue := response.value()
		value, callErr := registration.handler(goja.Undefined(), request, responseValue)
		if callErr != nil {
			handlerDone <- errors.New(dynamicJSMessage(callErr))
			return
		}
		if _, ok := value.Export().(*goja.Promise); !ok {
			handlerDone <- nil
			return
		}
		dynamicCordisAwaitOnLoop(registration.run, value, func(_ goja.Value, err error) { handlerDone <- err })
	}) {
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return true
	}
	for {
		select {
		case handlerErr := <-handlerDone:
			handlerDone = nil
			if handlerErr != nil {
				e.reportDynamicCordisHostFailure(registration.run, "webServer route "+registration.path, handlerErr)
				if !response.sent() {
					http.Error(w, "bad request", http.StatusBadRequest)
				}
				exchange.finish()
			}
		case <-exchange.done:
			response.emitAsync("close")
			return true
		case <-r.Context().Done():
			exchange.finish()
			response.emitAsync("close")
			return true
		}
	}
}

func (e *Engine) dynamicWebRequestValue(run *dynamicCordisRun, request *http.Request, body []byte) *goja.Object {
	vm := run.runtime
	value := vm.NewObject()
	headers := map[string]any{}
	rawHeaders := make([]string, 0, len(request.Header)*2)
	for name, values := range request.Header {
		key := strings.ToLower(name)
		if len(values) == 1 {
			headers[key] = values[0]
		} else {
			headers[key] = append([]string(nil), values...)
		}
		for _, item := range values {
			rawHeaders = append(rawHeaders, name, item)
		}
	}
	if request.Host != "" {
		headers["host"] = request.Host
	}
	_ = value.Set("method", request.Method)
	_ = value.Set("url", request.URL.RequestURI())
	_ = value.Set("headers", headers)
	_ = value.Set("rawHeaders", rawHeaders)
	_ = value.Set("httpVersion", strconv.Itoa(request.ProtoMajor)+"."+strconv.Itoa(request.ProtoMinor))
	_ = value.Set("body", dynamicWebByteValue(vm, body))
	_ = value.Set("bodyText", string(body))
	_ = value.Set("readableEnded", true)
	_ = value.Set("destroy", func(goja.FunctionCall) goja.Value { _ = request.Body.Close(); return goja.Undefined() })
	_ = value.Set("setEncoding", func(goja.FunctionCall) goja.Value { return value })
	on := func(call goja.FunctionCall) goja.Value {
		event := call.Argument(0).String()
		listener, ok := goja.AssertFunction(call.Argument(1))
		if !ok {
			return value
		}
		switch event {
		case "data":
			if len(body) > 0 {
				_, _ = listener(value, dynamicWebByteValue(vm, body))
			}
		case "end", "close":
			_, _ = listener(value)
		}
		return value
	}
	_ = value.Set("on", on)
	_ = value.Set("once", on)
	return value
}

func dynamicWebByteValue(vm *goja.Runtime, data []byte) goja.Value {
	array, err := vm.New(vm.Get("Uint8Array"), vm.ToValue(vm.NewArrayBuffer(append([]byte(nil), data...))))
	if err != nil {
		return vm.ToValue(append([]byte(nil), data...))
	}
	_ = array.Set("toString", func(goja.FunctionCall) goja.Value { return vm.ToValue(string(data)) })
	return array
}

func dynamicWebBytes(value goja.Value) []byte {
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return nil
	}
	switch exported := value.Export().(type) {
	case string:
		return []byte(exported)
	case []byte:
		return append([]byte(nil), exported...)
	case goja.ArrayBuffer:
		return append([]byte(nil), exported.Bytes()...)
	default:
		return []byte(value.String())
	}
}

func (response *dynamicWebHTTPResponse) value() *goja.Object {
	vm := response.run.runtime
	value := vm.NewObject()
	_ = value.DefineAccessorProperty("statusCode", vm.ToValue(func() int { response.mu.Lock(); defer response.mu.Unlock(); return response.statusCode }), vm.ToValue(func(call goja.FunctionCall) goja.Value {
		response.mu.Lock()
		if !response.headersSent {
			response.statusCode = int(call.Argument(0).ToInteger())
		}
		response.mu.Unlock()
		return goja.Undefined()
	}), goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = value.DefineAccessorProperty("headersSent", vm.ToValue(func() bool { response.mu.Lock(); defer response.mu.Unlock(); return response.headersSent }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = value.DefineAccessorProperty("writableEnded", vm.ToValue(func() bool { response.mu.Lock(); defer response.mu.Unlock(); return response.ended }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = value.Set("setHeader", func(call goja.FunctionCall) goja.Value {
		response.writer.Header().Set(call.Argument(0).String(), call.Argument(1).String())
		return value
	})
	_ = value.Set("getHeader", func(call goja.FunctionCall) goja.Value {
		if header := response.writer.Header().Get(call.Argument(0).String()); header != "" {
			return vm.ToValue(header)
		}
		return goja.Undefined()
	})
	_ = value.Set("getHeaders", func(goja.FunctionCall) goja.Value { return vm.ToValue(response.writer.Header()) })
	_ = value.Set("hasHeader", func(call goja.FunctionCall) goja.Value {
		return vm.ToValue(response.writer.Header().Get(call.Argument(0).String()) != "")
	})
	_ = value.Set("removeHeader", func(call goja.FunctionCall) goja.Value {
		response.writer.Header().Del(call.Argument(0).String())
		return goja.Undefined()
	})
	_ = value.Set("writeHead", func(call goja.FunctionCall) goja.Value {
		status := int(call.Argument(0).ToInteger())
		headers := call.Argument(1)
		if _, ok := headers.Export().(string); ok {
			headers = call.Argument(2)
		}
		response.applyHeaders(headers)
		response.writeHeader(status)
		return value
	})
	_ = value.Set("flushHeaders", func(goja.FunctionCall) goja.Value {
		response.writeHeader(response.status())
		response.flush()
		return goja.Undefined()
	})
	_ = value.Set("write", func(call goja.FunctionCall) goja.Value {
		return vm.ToValue(response.write(dynamicWebBytes(call.Argument(0))))
	})
	_ = value.Set("end", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) > 0 && !goja.IsUndefined(call.Argument(0)) {
			response.write(dynamicWebBytes(call.Argument(0)))
		} else {
			response.writeHeader(response.status())
		}
		response.finish()
		return value
	})
	_ = value.Set("destroy", func(goja.FunctionCall) goja.Value { response.finish(); return value })
	_ = value.Set("on", func(call goja.FunctionCall) goja.Value {
		if listener, ok := goja.AssertFunction(call.Argument(1)); ok {
			response.mu.Lock()
			response.listeners[call.Argument(0).String()] = append(response.listeners[call.Argument(0).String()], listener)
			response.mu.Unlock()
		}
		return value
	})
	_ = value.Set("once", value.Get("on"))
	return value
}

func (response *dynamicWebHTTPResponse) applyHeaders(value goja.Value) {
	object, ok := value.(*goja.Object)
	if !ok {
		return
	}
	for _, name := range object.Keys() {
		response.writer.Header().Set(name, object.Get(name).String())
	}
}

func (response *dynamicWebHTTPResponse) status() int {
	response.mu.Lock()
	defer response.mu.Unlock()
	return response.statusCode
}

func (response *dynamicWebHTTPResponse) sent() bool {
	response.mu.Lock()
	defer response.mu.Unlock()
	return response.headersSent
}

func (response *dynamicWebHTTPResponse) writeHeader(status int) {
	response.mu.Lock()
	if response.headersSent || response.ended {
		response.mu.Unlock()
		return
	}
	response.statusCode = status
	response.headersSent = true
	response.mu.Unlock()
	response.writer.WriteHeader(status)
}

func (response *dynamicWebHTTPResponse) write(data []byte) bool {
	response.writeHeader(response.status())
	response.mu.Lock()
	ended := response.ended
	response.mu.Unlock()
	if ended {
		return false
	}
	if len(data) > 0 {
		if _, err := response.writer.Write(data); err != nil {
			response.finish()
			return false
		}
	}
	response.flush()
	return true
}

func (response *dynamicWebHTTPResponse) flush() {
	if flusher, ok := response.writer.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (response *dynamicWebHTTPResponse) finish() {
	response.mu.Lock()
	if response.ended {
		response.mu.Unlock()
		return
	}
	response.ended = true
	listeners := append([]goja.Callable(nil), response.listeners["finish"]...)
	response.mu.Unlock()
	for _, listener := range listeners {
		_, _ = listener(goja.Undefined())
	}
	response.exchange.finish()
}

func (response *dynamicWebHTTPResponse) emitAsync(event string) {
	response.engine.dynamicCordis.loop.post(func() {
		response.run.mu.Lock()
		active := !response.run.disposed && (response.run.active || response.run.activating)
		response.run.mu.Unlock()
		if !active {
			return
		}
		response.mu.Lock()
		listeners := append([]goja.Callable(nil), response.listeners[event]...)
		response.mu.Unlock()
		for _, listener := range listeners {
			if _, err := listener(goja.Undefined()); err != nil {
				response.engine.reportDynamicCordisHostFailure(response.run, "webServer response "+event, err)
			}
		}
	})
}

func isDynamicWebUpgrade(request *http.Request) bool {
	return request.Header.Get("Upgrade") != "" && strings.Contains(strings.ToLower(request.Header.Get("Connection")), "upgrade")
}

func (e *Engine) dynamicWebUpgradeHandler(w http.ResponseWriter, r *http.Request) bool {
	if !isDynamicWebUpgrade(r) {
		return false
	}
	e.dynamicCordis.RLock()
	registration := e.dynamicCordis.webUpgrades[r.URL.Path]
	e.dynamicCordis.RUnlock()
	if registration == nil {
		return false
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "upgrade unavailable", http.StatusInternalServerError)
		return true
	}
	conn, buffer, err := hijacker.Hijack()
	if err != nil {
		return true
	}
	head := dynamicWebBufferedHead(buffer)
	socket := &dynamicWebSocket{engine: e, run: registration.run, owner: registration, conn: conn, listeners: map[string][]goja.Callable{}}
	if !registration.attach(socket) {
		_ = conn.Close()
		return true
	}
	if !e.dynamicCordis.loop.post(func() {
		registration.run.mu.Lock()
		active := !registration.run.disposed && (registration.run.active || registration.run.activating)
		registration.run.mu.Unlock()
		if !active {
			socket.close("")
			return
		}
		value, callErr := registration.handler(goja.Undefined(), e.dynamicWebRequestValue(registration.run, r, nil), socket.value(), dynamicWebByteValue(registration.run.runtime, head))
		if callErr != nil {
			e.reportDynamicCordisHostFailure(registration.run, "webServer upgrade "+registration.path, callErr)
			socket.close("")
			return
		}
		if _, ok := value.Export().(*goja.Promise); ok {
			dynamicCordisAwaitOnLoop(registration.run, value, func(_ goja.Value, err error) {
				if err != nil {
					e.reportDynamicCordisHostFailure(registration.run, "webServer upgrade "+registration.path, err)
					socket.close("")
				}
			})
		}
	}) {
		socket.close("")
	}
	return true
}

func dynamicWebBufferedHead(buffer *bufio.ReadWriter) []byte {
	if buffer == nil || buffer.Reader == nil || buffer.Reader.Buffered() == 0 {
		return nil
	}
	head, _ := buffer.Reader.Peek(buffer.Reader.Buffered())
	return append([]byte(nil), head...)
}

func (socket *dynamicWebSocket) value() *goja.Object {
	vm := socket.run.runtime
	value := vm.NewObject()
	_ = value.DefineAccessorProperty("destroyed", vm.ToValue(func() bool { socket.mu.Lock(); defer socket.mu.Unlock(); return socket.closed }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = value.Set("write", func(call goja.FunctionCall) goja.Value {
		_, err := socket.conn.Write(dynamicWebBytes(call.Argument(0)))
		if err != nil {
			socket.close(err.Error())
		}
		return vm.ToValue(err == nil)
	})
	_ = value.Set("end", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) > 0 && !goja.IsUndefined(call.Argument(0)) {
			_, _ = socket.conn.Write(dynamicWebBytes(call.Argument(0)))
		}
		socket.close("")
		return value
	})
	_ = value.Set("destroy", func(call goja.FunctionCall) goja.Value { socket.close(""); return value })
	_ = value.Set("setNoDelay", func(call goja.FunctionCall) goja.Value {
		if conn, ok := socket.conn.(*net.TCPConn); ok {
			_ = conn.SetNoDelay(len(call.Arguments) == 0 || call.Argument(0).ToBoolean())
		}
		return value
	})
	_ = value.Set("on", func(call goja.FunctionCall) goja.Value {
		event := call.Argument(0).String()
		listener, ok := goja.AssertFunction(call.Argument(1))
		if !ok {
			return value
		}
		socket.mu.Lock()
		socket.listeners[event] = append(socket.listeners[event], listener)
		startRead := event == "data" && !socket.reading && !socket.closed
		if startRead {
			socket.reading = true
		}
		closed := socket.closed
		socket.mu.Unlock()
		if startRead {
			go socket.read()
		}
		if closed && event == "close" {
			_, _ = listener(goja.Undefined())
		}
		return value
	})
	_ = value.Set("once", value.Get("on"))
	return value
}

func (socket *dynamicWebSocket) read() {
	buffer := make([]byte, 32<<10)
	for {
		count, err := socket.conn.Read(buffer)
		if count > 0 {
			data := append([]byte(nil), buffer[:count]...)
			socket.emit("data", data)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				socket.emit("error", err.Error())
			}
			socket.close("")
			return
		}
	}
}

func (socket *dynamicWebSocket) emit(event string, payload any) {
	socket.engine.dynamicCordis.loop.post(func() {
		socket.run.mu.Lock()
		active := !socket.run.disposed && (socket.run.active || socket.run.activating)
		socket.run.mu.Unlock()
		if !active {
			return
		}
		socket.mu.Lock()
		listeners := append([]goja.Callable(nil), socket.listeners[event]...)
		socket.mu.Unlock()
		for _, listener := range listeners {
			argument := socket.run.runtime.ToValue(payload)
			if data, ok := payload.([]byte); ok {
				argument = dynamicWebByteValue(socket.run.runtime, data)
			}
			if _, err := listener(goja.Undefined(), argument); err != nil {
				socket.engine.reportDynamicCordisHostFailure(socket.run, "webServer socket "+event, err)
			}
		}
	})
}

func (socket *dynamicWebSocket) close(message string) {
	socket.mu.Lock()
	if socket.closed {
		socket.mu.Unlock()
		return
	}
	socket.closed = true
	socket.mu.Unlock()
	_ = socket.conn.Close()
	socket.owner.detach(socket)
	if message != "" {
		socket.emit("error", message)
	}
	socket.emit("close", nil)
}
