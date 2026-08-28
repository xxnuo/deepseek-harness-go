package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// CordisInspectMethodManifest is the JSON-only contract advertised by one
// dynamic Cordis inspect provider.
type CordisInspectMethodManifest struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	InputSchema  any    `json:"inputSchema"`
	OutputSchema any    `json:"outputSchema"`
}

// CordisInspectProviderManifest is one JSON-only Host or Client provider contract.
type CordisInspectProviderManifest struct {
	ID          string                        `json:"id"`
	Description string                        `json:"description"`
	Methods     []CordisInspectMethodManifest `json:"methods"`
}

// CordisInspectProviderView adds the runtime plane to a provider manifest.
type CordisInspectProviderView struct {
	CordisInspectProviderManifest
	Platform string `json:"platform"`
}

// CordisInspectQueryResolution is a result returned by a browser provider.
type CordisInspectQueryResolution struct {
	OK      bool   `json:"ok"`
	Data    any    `json:"data,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// CordisInspectResolveAck reports whether a response claimed a pending query.
type CordisInspectResolveAck struct {
	Accepted bool `json:"accepted"`
}

// DynamicCordisInventoryRow is the source-free inventory row exposed by the
// dynamic Cordis runner.
type DynamicCordisInventoryRow struct {
	PluginID         string                          `json:"pluginId"`
	AgentID          string                          `json:"agentId"`
	Packages         []DynamicCordisInventoryPackage `json:"packages"`
	CurrentPackageID string                          `json:"currentPackageId,omitempty"`
	NextPackageID    string                          `json:"nextPackageId,omitempty"`
	ActiveRun        *DynamicCordisActiveRun         `json:"activeRun,omitempty"`
	LatestRun        any                             `json:"latestRun,omitempty"`
}

type DynamicCordisInventoryPackage struct {
	PackageID     string `json:"packageId"`
	Name          string `json:"name"`
	Purpose       string `json:"purpose"`
	HasHostHalf   bool   `json:"hasHostHalf"`
	HasClientHalf bool   `json:"hasClientHalf"`
}

type DynamicCordisActiveRun struct {
	PluginRunID   string                      `json:"pluginRunId"`
	PackageID     string                      `json:"packageId"`
	RenderFailure *DynamicCordisRenderFailure `json:"renderFailure,omitempty"`
}

type DynamicCordisRenderFailure struct {
	Slot      string `json:"slot"`
	Message   string `json:"message"`
	Stack     string `json:"stack,omitempty"`
	Abdicated bool   `json:"abdicated"`
}

type DynamicCordisErrorDetails struct {
	Message string `json:"message"`
	Stack   string `json:"stack,omitempty"`
}

type dynamicCordisInspectPending struct {
	owner  string
	method CordisInspectMethodManifest
	result chan CordisInspectQueryResolution
}

type dynamicCordisStarting struct {
	done        chan struct{}
	waiting     []string
	startedHere bool
	err         error
}

type dynamicCordisState struct {
	sync.RWMutex
	loop           *dynamicCordisLoop
	clientManifest []CordisInspectProviderManifest
	pending        map[string]*dynamicCordisInspectPending
	plugins        map[string]*dynamicCordisPlugin
	pluginOrder    []string
	services       map[string]dynamicCordisService
	nextInspect    uint64
	nextPlugin     uint64
	nextPackage    uint64
	nextRun        uint64
	nextApproval   uint64
	nextListener   uint64
	indexProviders []*indexInjectionProviderRegistration
	indexTaps      []*indexTapRegistration
	webRoutes      map[string]*dynamicWebRouteRegistration
	webUpgrades    map[string]*dynamicWebUpgradeRegistration
	webFallback    *dynamicWebRouteRegistration
	webPort        int
	pendingRuns    map[string]*dynamicCordisPendingRun
	starting       map[string]*dynamicCordisStarting
	// agent entries model Cordis runtime ownership independently from durable
	// session parentage. The Go engine still uses Session as the live object.
	agents            map[string]*dynamicCordisAgentEntry
	agentOrder        []string
	agentReservations map[string]struct{}
	factoryRun        *dynamicCordisRun
	factoryValue      any
}

type dynamicCordisAgentEntry struct {
	id              string
	owner           string
	announced       bool
	announcing      bool
	detachRequested bool
	detached        bool
	object          any
	run             *dynamicCordisRun
}

type dynamicCordisLoop struct {
	mu     sync.Mutex
	cond   *sync.Cond
	jobs   []func()
	closed bool
	done   chan struct{}
}

func newDynamicCordisLoop() *dynamicCordisLoop {
	loop := &dynamicCordisLoop{done: make(chan struct{})}
	loop.cond = sync.NewCond(&loop.mu)
	go loop.run()
	return loop
}

func (loop *dynamicCordisLoop) run() {
	defer close(loop.done)
	for {
		loop.mu.Lock()
		for len(loop.jobs) == 0 && !loop.closed {
			loop.cond.Wait()
		}
		if len(loop.jobs) == 0 {
			loop.mu.Unlock()
			return
		}
		job := loop.jobs[0]
		loop.jobs[0] = nil
		loop.jobs = loop.jobs[1:]
		loop.mu.Unlock()
		job()
	}
}

func (loop *dynamicCordisLoop) post(job func()) bool {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	if loop.closed {
		return false
	}
	loop.jobs = append(loop.jobs, job)
	loop.cond.Signal()
	return true
}

func (loop *dynamicCordisLoop) call(job func()) bool {
	done := make(chan struct{})
	if !loop.post(func() {
		defer close(done)
		job()
	}) {
		return false
	}
	<-done
	return true
}

// pumpUntil keeps the single JavaScript runtime responsive while lifecycle
// cleanup awaits a Promise whose resolver posts back onto this loop.
func (loop *dynamicCordisLoop) pumpUntil(done func() bool) bool {
	for !done() {
		loop.mu.Lock()
		for len(loop.jobs) == 0 && !loop.closed && !done() {
			loop.cond.Wait()
		}
		if done() {
			loop.mu.Unlock()
			return true
		}
		if len(loop.jobs) == 0 {
			loop.mu.Unlock()
			return false
		}
		job := loop.jobs[0]
		loop.jobs[0] = nil
		loop.jobs = loop.jobs[1:]
		loop.mu.Unlock()
		job()
	}
	return true
}

func (loop *dynamicCordisLoop) close() {
	loop.mu.Lock()
	loop.closed = true
	loop.cond.Broadcast()
	loop.mu.Unlock()
	<-loop.done
}

func newDynamicCordisState() *dynamicCordisState {
	return &dynamicCordisState{
		loop:              newDynamicCordisLoop(),
		pending:           map[string]*dynamicCordisInspectPending{},
		plugins:           map[string]*dynamicCordisPlugin{},
		nextPlugin:        1,
		nextPackage:       1,
		nextRun:           1,
		nextApproval:      1,
		services:          map[string]dynamicCordisService{},
		webRoutes:         map[string]*dynamicWebRouteRegistration{},
		webUpgrades:       map[string]*dynamicWebUpgradeRegistration{},
		pendingRuns:       map[string]*dynamicCordisPendingRun{},
		starting:          map[string]*dynamicCordisStarting{},
		agents:            map[string]*dynamicCordisAgentEntry{},
		agentReservations: map[string]struct{}{},
	}
}

func (e *Engine) claimDynamicAgentReservation(id string) (func(), bool) {
	e.dynamicCordis.Lock()
	defer e.dynamicCordis.Unlock()
	if _, exists := e.dynamicCordis.agentReservations[id]; exists {
		return nil, false
	}
	if entry := e.dynamicCordis.agents[id]; entry != nil && !entry.detached {
		return nil, false
	}
	e.dynamicCordis.agentReservations[id] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			e.dynamicCordis.Lock()
			delete(e.dynamicCordis.agentReservations, id)
			e.dynamicCordis.Unlock()
		})
	}, true
}

func (e *Engine) emitCordisEvent(event string, payload any) {
	e.emitRemoteEvent(event, payload)
}

// SyncInspectManifest replaces the browser-side inspect provider directory.
// It mirrors the upstream all-at-once update: invalid input leaves the prior
// directory untouched.
func (e *Engine) SyncInspectManifest(providers []CordisInspectProviderManifest) error {
	validated, err := validateInspectManifest(providers)
	if err != nil {
		return err
	}
	e.dynamicCordis.Lock()
	e.dynamicCordis.clientManifest = validated
	e.dynamicCordis.Unlock()
	return nil
}

// ListInspectProviders returns a copy safe for callers to mutate.
func (e *Engine) ListInspectProviders() []CordisInspectProviderView {
	e.dynamicCordis.RLock()
	defer e.dynamicCordis.RUnlock()
	rows := make([]CordisInspectProviderView, 0, len(hostInspectProviderManifests)+len(e.dynamicCordis.clientManifest))
	for _, provider := range hostInspectProviderManifests {
		rows = append(rows, CordisInspectProviderView{
			CordisInspectProviderManifest: cloneInspectProvider(provider),
			Platform:                      "host",
		})
	}
	for _, provider := range e.dynamicCordis.clientManifest {
		rows = append(rows, CordisInspectProviderView{
			CordisInspectProviderManifest: cloneInspectProvider(provider),
			Platform:                      "client",
		})
	}
	return rows
}

func (e *Engine) ResolveInspectQuery(agentID, requestID string, resolution CordisInspectQueryResolution) CordisInspectResolveAck {
	if agentID == "" || requestID == "" || !resolution.OK {
		return CordisInspectResolveAck{}
	}
	e.dynamicCordis.Lock()
	pending, ok := e.dynamicCordis.pending[requestID]
	if ok && pending.owner == agentID && validInspectValue(resolution.Data, pending.method.OutputSchema) {
		delete(e.dynamicCordis.pending, requestID)
	} else {
		ok = false
	}
	e.dynamicCordis.Unlock()
	if !ok {
		return CordisInspectResolveAck{}
	}
	pending.result <- CordisInspectQueryResolution{OK: true, Data: cloneJSON(resolution.Data)}
	e.emitCordisEvent("cordis/inspect-query-resolved", map[string]any{"requestId": requestID})
	return CordisInspectResolveAck{Accepted: true}
}

// QueryInspectProvider runs one browser-owned read-only inspect method and
// waits until a page answers or the caller cancels the request.
func (e *Engine) QueryInspectProvider(ctx context.Context, agentID, providerID, methodName string, input any) (any, error) {
	if _, err := e.getSession(agentID); err != nil {
		return nil, err
	}
	e.dynamicCordis.Lock()
	var provider *CordisInspectProviderManifest
	for index := range e.dynamicCordis.clientManifest {
		if e.dynamicCordis.clientManifest[index].ID == providerID {
			provider = &e.dynamicCordis.clientManifest[index]
			break
		}
	}
	if provider == nil {
		e.dynamicCordis.Unlock()
		return nil, fmt.Errorf("Client Cordis inspect provider %q is not registered", providerID)
	}
	var method *CordisInspectMethodManifest
	for index := range provider.Methods {
		if provider.Methods[index].Name == methodName {
			method = &provider.Methods[index]
			break
		}
	}
	if method == nil {
		e.dynamicCordis.Unlock()
		return nil, fmt.Errorf("Cordis inspect provider %q has no method %q", providerID, methodName)
	}
	hasInput := input != nil
	validatedInput := input
	if validatedInput == nil {
		validatedInput = map[string]any{}
	}
	if !validInspectValue(validatedInput, method.InputSchema) {
		e.dynamicCordis.Unlock()
		return nil, fmt.Errorf("Client Cordis inspect %s.%s rejected input", providerID, methodName)
	}
	e.dynamicCordis.nextInspect++
	requestID := fmt.Sprintf("inspect-%d", e.dynamicCordis.nextInspect)
	pending := &dynamicCordisInspectPending{owner: agentID, method: *method, result: make(chan CordisInspectQueryResolution, 1)}
	e.dynamicCordis.pending[requestID] = pending
	e.dynamicCordis.Unlock()

	request := map[string]any{
		"requestId": requestID, "agentId": agentID, "provider": providerID, "method": methodName,
	}
	if hasInput {
		request["input"] = cloneJSON(input)
	}
	e.emitCordisEvent("cordis/inspect-query", request)
	select {
	case resolution := <-pending.result:
		return resolution.Data, nil
	case <-ctx.Done():
		e.dynamicCordis.Lock()
		current := e.dynamicCordis.pending[requestID]
		if current == pending {
			delete(e.dynamicCordis.pending, requestID)
		}
		e.dynamicCordis.Unlock()
		if current == pending {
			e.emitCordisEvent("cordis/inspect-query-resolved", map[string]any{"requestId": requestID})
		}
		return nil, ctx.Err()
	}
}

func validInspectValue(value, rawSchema any) bool {
	if !jsonValue(value) {
		return false
	}
	schema, ok := rawSchema.(map[string]any)
	return !ok || validateJSONAgainstSchema(value, schema) == nil
}

func validateInspectManifest(providers []CordisInspectProviderManifest) ([]CordisInspectProviderManifest, error) {
	seen := make(map[string]struct{}, len(providers))
	validated := make([]CordisInspectProviderManifest, 0, len(providers))
	for _, provider := range providers {
		id := strings.TrimSpace(provider.ID)
		if id == "" {
			return nil, errors.New("Cordis inspect provider id must not be empty")
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("Cordis inspect manifest repeats provider %q", id)
		}
		seen[id] = struct{}{}
		description := strings.TrimSpace(provider.Description)
		if description == "" {
			return nil, fmt.Errorf("Cordis inspect provider %q needs a description", id)
		}
		methodNames := make(map[string]struct{}, len(provider.Methods))
		methods := make([]CordisInspectMethodManifest, 0, len(provider.Methods))
		for _, method := range provider.Methods {
			name := strings.TrimSpace(method.Name)
			if name == "" {
				return nil, fmt.Errorf("Cordis inspect provider %q has an empty method name", id)
			}
			if _, exists := methodNames[name]; exists {
				return nil, fmt.Errorf("Cordis inspect provider %q repeats method %q", id, name)
			}
			methodNames[name] = struct{}{}
			methodDescription := strings.TrimSpace(method.Description)
			if methodDescription == "" {
				return nil, fmt.Errorf("Cordis inspect method %s.%s needs a description", id, name)
			}
			if method.InputSchema == nil || method.OutputSchema == nil {
				return nil, fmt.Errorf("Cordis inspect method %s.%s needs inputSchema and outputSchema", id, name)
			}
			if !jsonValue(method.InputSchema) || !jsonValue(method.OutputSchema) {
				return nil, fmt.Errorf("Cordis inspect method %s.%s has a non-JSON schema", id, name)
			}
			methods = append(methods, CordisInspectMethodManifest{
				Name: name, Description: methodDescription,
				InputSchema: cloneJSON(method.InputSchema), OutputSchema: cloneJSON(method.OutputSchema),
			})
		}
		validated = append(validated, CordisInspectProviderManifest{ID: id, Description: description, Methods: methods})
	}
	return validated, nil
}

func jsonValue(value any) bool {
	_, err := json.Marshal(value)
	return err == nil
}

func cloneJSON(value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var out any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if dec.Decode(&out) != nil {
		return nil
	}
	return out
}

func cloneInspectProvider(provider CordisInspectProviderManifest) CordisInspectProviderManifest {
	methods := make([]CordisInspectMethodManifest, 0, len(provider.Methods))
	for _, method := range provider.Methods {
		methods = append(methods, CordisInspectMethodManifest{
			Name: method.Name, Description: method.Description,
			InputSchema: cloneJSON(method.InputSchema), OutputSchema: cloneJSON(method.OutputSchema),
		})
	}
	return CordisInspectProviderManifest{ID: provider.ID, Description: provider.Description, Methods: methods}
}

func dynamicRPCArgs(payload map[string]any) (map[string]any, error) {
	if raw, exists := payload["args"]; exists {
		args, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("dynamic Cordis RPC args must be an object")
		}
		return args, nil
	}
	return payload, nil
}

func decodeDynamicProviders(payload map[string]any) ([]CordisInspectProviderManifest, bool, error) {
	args, err := dynamicRPCArgs(payload)
	if err != nil {
		return nil, false, err
	}
	raw, exists := args["providers"]
	if !exists {
		return nil, false, nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, true, err
	}
	var providers []CordisInspectProviderManifest
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&providers); err != nil {
		return nil, true, err
	}
	return providers, true, nil
}

func decodeDynamicResolution(payload map[string]any) (string, string, CordisInspectQueryResolution, error) {
	args, err := dynamicRPCArgs(payload)
	if err != nil {
		return "", "", CordisInspectQueryResolution{}, err
	}
	data, err := json.Marshal(args)
	if err != nil {
		return "", "", CordisInspectQueryResolution{}, err
	}
	var wire struct {
		AgentID    string                       `json:"agentId"`
		RequestID  string                       `json:"requestId"`
		Resolution CordisInspectQueryResolution `json:"resolution"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&wire); err != nil {
		return "", "", CordisInspectQueryResolution{}, err
	}
	return wire.AgentID, wire.RequestID, wire.Resolution, nil
}
