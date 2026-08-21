package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// ProjectionResult is the result of folding one committed event.
// Changed is explicit because Go values do not have JavaScript Object.is
// identity semantics.
type ProjectionResult struct {
	State   any
	Changed bool
}

// ProjectionDefinition is one synchronous, JSON-state session projection.
// A nil View makes the unit host-only.
type ProjectionDefinition struct {
	Key          string
	StateVersion int
	Init         func() any
	Apply        func(state any, event Event) ProjectionResult
	View         func(state any) any

	dynamicRuntime bool
}

// ProjectionChange is one client-visible unit transition caused by an event.
type ProjectionChange struct {
	Key   string
	Value any
	Seq   int
}

// ProjectionChangeListener observes client-visible projection transitions.
type ProjectionChangeListener func(session *Session, change ProjectionChange)

type projectionCell struct {
	state       any
	observedSeq int
}

type projectionRegistration struct {
	definition ProjectionDefinition
	cells      map[*Session]*projectionCell
	refs       int
}

// SessionProjectionRegistry owns projection definitions and their per-session
// fold cells. Engine creates one registry and exposes it for reusable library
// integrations through SessionProjections.
type SessionProjectionRegistry struct {
	mu            sync.Mutex
	registrations map[string]*projectionRegistration
	order         []string
	dynamicUnits  int
	listeners     map[uint64]ProjectionChangeListener
	nextListener  uint64
	runtime       func(func()) error
}

// NewSessionProjectionRegistry creates an empty projection registry.
func NewSessionProjectionRegistry() *SessionProjectionRegistry {
	return &SessionProjectionRegistry{
		registrations: map[string]*projectionRegistration{},
		listeners:     map[uint64]ProjectionChangeListener{},
	}
}

func (r *SessionProjectionRegistry) setRuntimeExecutor(execute func(func()) error) {
	r.mu.Lock()
	r.runtime = execute
	r.mu.Unlock()
}

func (r *SessionProjectionRegistry) execute(runtimeReady bool, operation func() error) error {
	if runtimeReady {
		return operation()
	}
	r.mu.Lock()
	execute := r.runtime
	r.mu.Unlock()
	if execute == nil {
		return operation()
	}
	var operationErr error
	if err := execute(func() { operationErr = operation() }); err != nil {
		return err
	}
	return operationErr
}

// Register installs one projection definition and returns an idempotent
// disposer. Same-key, same-version registrations share the existing unit;
// the final disposer removes the key and all of its cells.
func (r *SessionProjectionRegistry) Register(definition ProjectionDefinition) (func(), error) {
	return r.register(definition, false)
}

func (r *SessionProjectionRegistry) registerRuntime(definition ProjectionDefinition) (func(), error) {
	return r.register(definition, true)
}

func (r *SessionProjectionRegistry) register(definition ProjectionDefinition, runtimeReady bool) (func(), error) {
	definition.Key = strings.TrimSpace(definition.Key)
	var dispose func()
	var registerErr error
	err := r.execute(runtimeReady, func() error {
		dispose, registerErr = r.registerNow(definition, runtimeReady)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dispose, registerErr
}

func (r *SessionProjectionRegistry) registerNow(definition ProjectionDefinition, runtimeReady bool) (func(), error) {
	if err := validateProjectionDefinition(definition); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if existing := r.registrations[definition.Key]; existing != nil {
		if existing.definition.StateVersion != definition.StateVersion {
			r.mu.Unlock()
			return nil, fmt.Errorf(
				"session projection key %q is already registered at stateVersion %d; refusing stateVersion %d",
				definition.Key, existing.definition.StateVersion, definition.StateVersion,
			)
		}
		existing.refs++
	} else {
		r.registrations[definition.Key] = &projectionRegistration{
			definition: definition,
			cells:      map[*Session]*projectionCell{},
			refs:       1,
		}
		r.order = append(r.order, definition.Key)
		if definition.dynamicRuntime {
			r.dynamicUnits++
		}
	}
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			_ = r.execute(runtimeReady, func() error {
				r.mu.Lock()
				registration := r.registrations[definition.Key]
				if registration != nil {
					registration.refs--
					if registration.refs == 0 {
						if registration.definition.dynamicRuntime {
							r.dynamicUnits--
						}
						delete(r.registrations, definition.Key)
						for index, key := range r.order {
							if key == definition.Key {
								r.order = append(r.order[:index], r.order[index+1:]...)
								break
							}
						}
					}
				}
				r.mu.Unlock()
				return nil
			})
		})
	}, nil
}

func validateProjectionDefinition(definition ProjectionDefinition) error {
	if definition.Key == "" {
		return errors.New("session projection key is required")
	}
	if definition.StateVersion < 0 {
		return fmt.Errorf("session projection %q stateVersion must be non-negative", definition.Key)
	}
	if definition.Init == nil || definition.Apply == nil {
		return fmt.Errorf("session projection %q requires init and apply", definition.Key)
	}
	state, err := callProjectionInit(definition)
	if err != nil {
		return err
	}
	if err := validateProjectionJSON(definition.Key+" state", state); err != nil {
		return err
	}
	if definition.View != nil {
		value, err := callProjectionView(definition, state)
		if err != nil {
			return err
		}
		if err := validateProjectionJSON(definition.Key+" view", value); err != nil {
			return err
		}
	}
	return nil
}

func validateProjectionJSON(name string, value any) error {
	if _, err := json.Marshal(value); err != nil {
		return fmt.Errorf("session projection %s must be JSON-serializable: %w", name, err)
	}
	return nil
}

func callProjectionInit(definition ProjectionDefinition) (state any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("session projection %q init failed: %v", definition.Key, recovered)
		}
	}()
	return definition.Init(), nil
}

func callProjectionApply(definition ProjectionDefinition, state any, event Event) (result ProjectionResult, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("session projection %q apply failed at seq %d: %v", definition.Key, event.Seq, recovered)
		}
	}()
	result = definition.Apply(state, event)
	if err := validateProjectionJSON(definition.Key+" state", result.State); err != nil {
		return ProjectionResult{}, err
	}
	return result, nil
}

func callProjectionView(definition ProjectionDefinition, state any) (value any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("session projection %q view failed: %v", definition.Key, recovered)
		}
	}()
	value = definition.View(state)
	if err := validateProjectionJSON(definition.Key+" view", value); err != nil {
		return nil, err
	}
	return value, nil
}

// Signature identifies the active projection composition for wire-value cache
// invalidation. Definitions must bump StateVersion when fold semantics change.
func (r *SessionProjectionRegistry) Signature() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.signatureLocked()
}

func (r *SessionProjectionRegistry) signatureLocked() string {
	items := make([]string, 0, len(r.registrations))
	for key, registration := range r.registrations {
		wire := "host"
		if registration.definition.View != nil {
			wire = "wire"
		}
		items = append(items, fmt.Sprintf("%s:%d:%s", key, registration.definition.StateVersion, wire))
	}
	sort.Strings(items)
	return strings.Join(items, "|")
}

// Snapshot returns one consistent client-visible projection cut.
func (r *SessionProjectionRegistry) Snapshot(session *Session) (ProjectionSnapshot, error) {
	return r.snapshot(session, false)
}

func (r *SessionProjectionRegistry) snapshotRuntime(session *Session) (ProjectionSnapshot, error) {
	return r.snapshot(session, true)
}

func (r *SessionProjectionRegistry) snapshot(session *Session, runtimeReady bool) (ProjectionSnapshot, error) {
	if session == nil {
		return ProjectionSnapshot{}, errors.New("session projection snapshot requires a session")
	}
	var snapshot ProjectionSnapshot
	var snapshotErr error
	err := r.execute(runtimeReady, func() error {
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		session.mu.Unlock()
		snapshot, snapshotErr = r.snapshotNow(session, events)
		return nil
	})
	if err != nil {
		return ProjectionSnapshot{}, err
	}
	return snapshot, snapshotErr
}

func (r *SessionProjectionRegistry) snapshotForCache(session *Session) (ProjectionSnapshot, string, error) {
	if session == nil {
		return ProjectionSnapshot{}, "", errors.New("session projection snapshot requires a session")
	}
	var snapshot ProjectionSnapshot
	var composition string
	var snapshotErr error
	err := r.execute(false, func() error {
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		session.mu.Unlock()
		snapshot, snapshotErr = r.snapshotNow(session, events)
		composition = r.Signature()
		return nil
	})
	if err != nil {
		return ProjectionSnapshot{}, "", err
	}
	return snapshot, composition, snapshotErr
}

func (r *SessionProjectionRegistry) snapshotNow(session *Session, events []Event) (ProjectionSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	values := map[string]any{}
	var failures []error
	for _, key := range r.order {
		registration := r.registrations[key]
		if registration == nil || registration.definition.View == nil {
			continue
		}
		cell, err := r.cellForLocked(registration, session, events)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		value, err := callProjectionView(registration.definition, cell.state)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		values[key] = value
	}
	return ProjectionSnapshot{AsOfSeq: len(events) - 1, Values: values}, errors.Join(failures...)
}

// StateOf reads one registered unit's internal state. Callers must not mutate
// the returned value.
func (r *SessionProjectionRegistry) StateOf(session *Session, key string) (any, bool, error) {
	return r.stateOf(session, key, false)
}

func (r *SessionProjectionRegistry) stateOfRuntime(session *Session, key string) (any, bool, error) {
	return r.stateOf(session, key, true)
}

func (r *SessionProjectionRegistry) stateOf(session *Session, key string, runtimeReady bool) (any, bool, error) {
	if session == nil {
		return nil, false, errors.New("session projection state requires a session")
	}
	var state any
	var found bool
	var stateErr error
	err := r.execute(runtimeReady, func() error {
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		session.mu.Unlock()
		state, found, stateErr = r.stateOfNow(session, events, key)
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return state, found, stateErr
}

func (r *SessionProjectionRegistry) stateOfNow(session *Session, events []Event, key string) (any, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	registration := r.registrations[key]
	if registration == nil {
		return nil, false, nil
	}
	cell, err := r.cellForLocked(registration, session, events)
	if err != nil {
		return nil, false, err
	}
	return cell.state, true, nil
}

func (r *SessionProjectionRegistry) cellForLocked(registration *projectionRegistration, session *Session, events []Event) (*projectionCell, error) {
	cell := registration.cells[session]
	if cell == nil {
		state, err := callProjectionInit(registration.definition)
		if err != nil {
			return nil, err
		}
		cell = &projectionCell{state: state, observedSeq: -1}
		registration.cells[session] = cell
	}
	for _, event := range events {
		if event.Seq <= cell.observedSeq {
			continue
		}
		result, err := callProjectionApply(registration.definition, cell.state, event)
		cell.observedSeq = event.Seq
		if err != nil {
			return nil, err
		}
		cell.state = result.State
	}
	return cell, nil
}

// Drive folds one committed event through every registered unit.
func (r *SessionProjectionRegistry) Drive(session *Session, event Event) ([]ProjectionChange, error) {
	return r.drive(session, event, false)
}

func (r *SessionProjectionRegistry) driveRuntime(session *Session, event Event) ([]ProjectionChange, error) {
	return r.drive(session, event, true)
}

func (r *SessionProjectionRegistry) drive(session *Session, event Event, runtimeReady bool) ([]ProjectionChange, error) {
	if session == nil {
		return nil, errors.New("session projection drive requires a session")
	}
	var changes []ProjectionChange
	var driveErr error
	err := r.execute(runtimeReady, func() error {
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		session.mu.Unlock()
		changes, driveErr = r.driveNow(session, events, event)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return changes, driveErr
}

func (r *SessionProjectionRegistry) driveNow(session *Session, events []Event, event Event) ([]ProjectionChange, error) {
	r.mu.Lock()
	changes := make([]ProjectionChange, 0)
	var failures []error
	for _, key := range r.order {
		registration := r.registrations[key]
		if registration == nil {
			continue
		}
		cell := registration.cells[session]
		if cell == nil {
			state, err := callProjectionInit(registration.definition)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			cell = &projectionCell{state: state, observedSeq: -1}
			registration.cells[session] = cell
			for _, previous := range events {
				if previous.Seq >= event.Seq {
					break
				}
				result, err := callProjectionApply(registration.definition, cell.state, previous)
				cell.observedSeq = previous.Seq
				if err != nil {
					failures = append(failures, err)
					continue
				}
				cell.state = result.State
			}
		}
		if event.Seq <= cell.observedSeq {
			continue
		}
		result, err := callProjectionApply(registration.definition, cell.state, event)
		cell.observedSeq = event.Seq
		if err != nil {
			failures = append(failures, err)
			continue
		}
		cell.state = result.State
		if !result.Changed || registration.definition.View == nil {
			continue
		}
		value, err := callProjectionView(registration.definition, result.State)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		changes = append(changes, ProjectionChange{Key: key, Value: value, Seq: event.Seq})
	}
	listeners := make([]ProjectionChangeListener, 0, len(r.listeners))
	for _, listener := range r.listeners {
		listeners = append(listeners, listener)
	}
	r.mu.Unlock()
	for _, change := range changes {
		for _, listener := range listeners {
			listener(session, change)
		}
	}
	return changes, errors.Join(failures...)
}

// OnChanged subscribes to client-visible projection transitions.
func (r *SessionProjectionRegistry) OnChanged(listener ProjectionChangeListener) func() {
	if listener == nil {
		return func() {}
	}
	r.mu.Lock()
	r.nextListener++
	id := r.nextListener
	r.listeners[id] = listener
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.listeners, id)
			r.mu.Unlock()
		})
	}
}

func newBuiltinSessionProjectionRegistry() (*SessionProjectionRegistry, error) {
	registry := NewSessionProjectionRegistry()
	for _, definition := range builtinProjectionDefinitions() {
		if _, err := registry.Register(definition); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

type permissionProjectionState struct {
	Preset   *string `json:"preset"`
	Sandbox  *string `json:"sandbox"`
	Approval *string `json:"approval"`
}

type planProjectionRunning struct {
	CommandID string `json:"commandId"`
	Wanted    bool   `json:"wanted"`
}

type planProjectionState struct {
	Active  bool                   `json:"active"`
	Wanted  *bool                  `json:"wanted"`
	Running *planProjectionRunning `json:"running"`
}

type projectionUsageSampleState struct {
	Turn    int                  `json:"turn"`
	Step    int                  `json:"step"`
	Buckets tokenUsageProjection `json:"buckets"`
}

type tokenUsageProjectionState struct {
	Totals tokenUsageProjection        `json:"totals"`
	Last   *projectionUsageSampleState `json:"last"`
}

type projectionClaimState struct {
	Start  int `json:"start"`
	End    int `json:"end"`
	Tokens int `json:"tokens"`
}

type contextPressureProjectionState struct {
	ContextWindow        *int                  `json:"contextWindow,omitempty"`
	PressureTokens       *int                  `json:"pressureTokens,omitempty"`
	SurfaceTokens        int                   `json:"surfaceTokens"`
	SampledSurfaceTokens *int                  `json:"sampledSurfaceTokens,omitempty"`
	Claim                *projectionClaimState `json:"claim,omitempty"`
}

type contextBreakdownProjectionState struct {
	SystemTokens  int                   `json:"systemTokens"`
	ToolsTokens   int                   `json:"toolsTokens"`
	MessageTokens int                   `json:"messageTokens"`
	Claim         *projectionClaimState `json:"claim,omitempty"`
}

type sessionStatsOpenStep struct {
	Turn       int    `json:"turn"`
	Step       int    `json:"step"`
	Start      int64  `json:"startTime"`
	FirstToken *int64 `json:"firstTokenTime"`
}

type sessionStatsProjectionState struct {
	Turns        int                   `json:"turns"`
	Steps        int                   `json:"steps"`
	LLMMS        int64                 `json:"llmMs"`
	ToolMS       int64                 `json:"toolMs"`
	TTFTMS       int64                 `json:"ttftMs"`
	TTFTSteps    int                   `json:"ttftSteps"`
	DecodeMS     int64                 `json:"decodeMs"`
	DecodeTokens int                   `json:"decodeTokens"`
	LastTurn     int                   `json:"lastTurn"`
	Open         *sessionStatsOpenStep `json:"openStep"`
	PendingCalls map[string]int64      `json:"pendingCalls"`
}

type sessionListProjectionState struct {
	Blank        bool  `json:"blank"`
	LastPromptAt int64 `json:"lastPromptAt"`
}

type subagentActiveInterval struct {
	Since   int64 `json:"since"`
	Through int64 `json:"through"`
}

type subagentTimingProjectionState struct {
	SettledMS        int64                   `json:"settledMs"`
	Active           *subagentActiveInterval `json:"active,omitempty"`
	PendingTurnStart *int64                  `json:"pendingTurnStart,omitempty"`
	DescriptorSeen   bool                    `json:"descriptorSeen"`
}

func projectionUnchanged(state any) ProjectionResult {
	return ProjectionResult{State: state}
}

func projectionChanged(state any) ProjectionResult {
	return ProjectionResult{State: state, Changed: true}
}

func projectionValueResult(previous, next any) ProjectionResult {
	return ProjectionResult{State: next, Changed: !reflect.DeepEqual(previous, next)}
}

func projectionFoldResult(apply func(any, Event) any) func(any, Event) ProjectionResult {
	return func(state any, event Event) ProjectionResult {
		return projectionValueResult(state, apply(state, event))
	}
}

func builtinProjectionDefinitions() []ProjectionDefinition {
	return []ProjectionDefinition{
		{
			Key: "title", StateVersion: 1,
			Init: func() any { return nil },
			Apply: func(state any, event Event) ProjectionResult {
				if value, ok := titleProjectionChange(event); ok {
					return projectionValueResult(state, value)
				}
				return projectionUnchanged(state)
			},
			View: func(state any) any { return state },
		},
		{
			Key: "todos", StateVersion: 2,
			Init: func() any { return nil },
			Apply: func(state any, event Event) ProjectionResult {
				value, ok := todosProjectionChange(event)
				if !ok {
					return projectionUnchanged(state)
				}
				if event.Type == "turn/start" && state == nil {
					return projectionUnchanged(state)
				}
				return projectionChanged(value)
			},
			View: func(state any) any { return state },
		},
		{
			Key: "permissions", StateVersion: 1,
			Init:  func() any { return permissionProjectionState{} },
			Apply: projectionFoldResult(applyPermissionProjection),
			View: func(state any) any {
				value := state.(permissionProjectionState)
				events := make([]Event, 0, 3)
				if value.Preset != nil {
					events = append(events, Event{Type: "permission/preset", Data: map[string]any{"preset": *value.Preset}})
				}
				if value.Sandbox != nil {
					events = append(events, Event{Type: "sandbox/mode", Data: map[string]any{"mode": *value.Sandbox}})
				}
				if value.Approval != nil {
					events = append(events, Event{Type: "approval/policy", Data: map[string]any{"policy": *value.Approval}})
				}
				return currentPermissions(events)
			},
		},
		{
			Key: "plan", StateVersion: 2,
			Init:  func() any { return planProjectionState{} },
			Apply: projectionFoldResult(applyPlanProjection),
			View: func(state any) any {
				value := state.(planProjectionState)
				wanted := value.Wanted
				if value.Running != nil {
					running := value.Running.Wanted
					wanted = &running
				}
				return map[string]any{"active": value.Active, "pending": wanted != nil && *wanted != value.Active}
			},
		},
		{
			Key: "goal", StateVersion: 4,
			Init: func() any { return nil },
			Apply: func(state any, event Event) ProjectionResult {
				if value, ok := goalProjectionChange(event); ok {
					return projectionValueResult(state, value)
				}
				return projectionUnchanged(state)
			},
			View: func(state any) any { return state },
		},
		{
			Key: "tokenUsage", StateVersion: 1,
			Init:  func() any { return tokenUsageProjectionState{} },
			Apply: projectionFoldResult(applyTokenUsageProjection),
			View:  func(state any) any { return state.(tokenUsageProjectionState).Totals.value() },
		},
		{
			Key: "contextPressure", StateVersion: 4,
			Init:  func() any { return contextPressureProjectionState{} },
			Apply: projectionFoldResult(applyContextPressureProjection),
			View:  viewContextPressureProjection,
		},
		{
			Key: "contextBreakdown", StateVersion: 2,
			Init:  func() any { return contextBreakdownProjectionState{} },
			Apply: projectionFoldResult(applyContextBreakdownProjection),
			View: func(state any) any {
				value := state.(contextBreakdownProjectionState)
				return map[string]any{"systemTokens": value.SystemTokens, "toolsTokens": value.ToolsTokens, "messageTokens": value.MessageTokens}
			},
		},
		{
			Key: "sessionStats", StateVersion: 1,
			Init:  func() any { return sessionStatsProjectionState{LastTurn: -1, PendingCalls: map[string]int64{}} },
			Apply: projectionFoldResult(applySessionStatsProjection),
			View:  viewSessionStatsProjection,
		},
		{
			Key: "sessionListMetadata", StateVersion: 1,
			Init:  func() any { return sessionListProjectionState{Blank: true} },
			Apply: projectionFoldResult(applySessionListProjection),
			View: func(state any) any {
				value := state.(sessionListProjectionState)
				var lastPrompt any
				if value.LastPromptAt != 0 {
					lastPrompt = value.LastPromptAt
				}
				return map[string]any{"blank": value.Blank, "lastPromptAt": lastPrompt}
			},
		},
		{
			Key: "subagent", StateVersion: 2,
			Init: func() any { return nil },
			Apply: func(state any, event Event) ProjectionResult {
				if event.Type != "subagent/descriptor" {
					return projectionUnchanged(state)
				}
				return projectionChanged(applySubagentIdentityProjection(state, event))
			},
			View: func(state any) any { return state },
		},
		{
			Key: "subagentTiming", StateVersion: 2,
			Init:  func() any { return subagentTimingProjectionState{} },
			Apply: projectionFoldResult(applySubagentTimingProjection),
			View: func(state any) any {
				value := state.(subagentTimingProjectionState)
				view := map[string]any{"settledMs": value.SettledMS}
				if value.Active != nil {
					view["active"] = map[string]any{"since": value.Active.Since, "through": value.Active.Through}
				}
				return view
			},
		},
		{
			Key: "imageLimits", StateVersion: 1,
			Init:  func() any { return nil },
			Apply: func(state any, _ Event) ProjectionResult { return projectionUnchanged(state) },
			View:  func(any) any { return imageLimitsProjection() },
		},
	}
}

func applyPermissionProjection(state any, event Event) any {
	current := state.(permissionProjectionState)
	data, _ := event.Data.(map[string]any)
	var target **string
	switch event.Type {
	case "permission/preset":
		target = &current.Preset
	case "sandbox/mode":
		target = &current.Sandbox
	case "approval/policy":
		target = &current.Approval
	default:
		return state
	}
	var key string
	switch event.Type {
	case "permission/preset":
		key = "preset"
	case "sandbox/mode":
		key = "mode"
	case "approval/policy":
		key = "policy"
	}
	value, ok := data[key].(string)
	if !ok {
		return state
	}
	if *target != nil && **target == value {
		return state
	}
	copy := value
	*target = &copy
	return current
}

func applyPlanProjection(state any, event Event) any {
	current := state.(planProjectionState)
	data, _ := event.Data.(map[string]any)
	switch event.Type {
	case "command/run":
		if data["name"] != "plan" {
			return state
		}
		args, ok := data["args"].(string)
		if !ok {
			return state
		}
		wanted := strings.TrimSpace(args) != "off"
		commandID, _ := data["commandId"].(string)
		if commandID == "" {
			if current.Wanted != nil && *current.Wanted == wanted {
				return state
			}
			current.Wanted = &wanted
			return current
		}
		next := &planProjectionRunning{CommandID: commandID, Wanted: wanted}
		if reflect.DeepEqual(current.Running, next) {
			return state
		}
		current.Running = next
		return current
	case "command/done":
		commandID, _ := data["commandId"].(string)
		if current.Running == nil || commandID == "" || commandID != current.Running.CommandID {
			return state
		}
		if data["kind"] == "success" && current.Running.Wanted != current.Active {
			wanted := current.Running.Wanted
			current.Wanted = &wanted
		} else {
			current.Wanted = nil
		}
		current.Running = nil
		return current
	case "plan/mode":
		active, ok := data["active"].(bool)
		if !ok || active == current.Active && current.Wanted == nil {
			return state
		}
		current.Active = active
		current.Wanted = nil
		return current
	default:
		return state
	}
}

func applyTokenUsageProjection(state any, event Event) any {
	current := state.(tokenUsageProjectionState)
	sample, ok := eventUsageSample(event)
	if !ok {
		return state
	}
	nextSample := &projectionUsageSampleState{Turn: sample.turn, Step: sample.step, Buckets: sample.buckets}
	if reflect.DeepEqual(current.Last, nextSample) {
		return state
	}
	var previous tokenUsageProjection
	if current.Last != nil && current.Last.Turn == sample.turn && current.Last.Step == sample.step {
		previous = current.Last.Buckets
	}
	current.Totals.UncachedInputTokens += sample.buckets.UncachedInputTokens - previous.UncachedInputTokens
	current.Totals.OutputTokens += sample.buckets.OutputTokens - previous.OutputTokens
	current.Totals.CacheReadTokens += sample.buckets.CacheReadTokens - previous.CacheReadTokens
	current.Totals.CacheWriteTokens += sample.buckets.CacheWriteTokens - previous.CacheWriteTokens
	current.Last = nextSample
	return current
}

func claimForProjectionStep(claim *projectionClaimState) *projectionSurfaceClaim {
	if claim == nil {
		return nil
	}
	return &projectionSurfaceClaim{start: claim.Start, end: claim.End, tokens: claim.Tokens}
}

func claimFromProjectionStep(claim *projectionSurfaceClaim) *projectionClaimState {
	if claim == nil {
		return nil
	}
	return &projectionClaimState{Start: claim.start, End: claim.end, Tokens: claim.tokens}
}

func applyContextPressureProjection(state any, event Event) any {
	current := state.(contextPressureProjectionState)
	next := current
	if event.Type == "request/context" {
		data, _ := event.Data.(map[string]any)
		if value, ok := projectionNonnegativeInt(data["contextWindow"]); ok && value > 0 {
			if next.ContextWindow == nil || *next.ContextWindow != value {
				copy := value
				next.ContextWindow = &copy
			}
		} else {
			next.ContextWindow = nil
		}
	}
	if sample, ok := eventUsageSample(event); ok {
		pressure := sample.buckets.UncachedInputTokens + sample.buckets.CacheReadTokens + sample.buckets.CacheWriteTokens
		sampled := next.SurfaceTokens
		next.PressureTokens = &pressure
		next.SampledSurfaceTokens = &sampled
	}
	delta, claim := projectionSurfaceStep(event, claimForProjectionStep(current.Claim))
	next.SurfaceTokens += delta
	if next.SurfaceTokens < 0 {
		next.SurfaceTokens = 0
	}
	next.Claim = claimFromProjectionStep(claim)
	if reflect.DeepEqual(next, current) {
		return state
	}
	return next
}

func viewContextPressureProjection(state any) any {
	value := state.(contextPressureProjectionState)
	view := map[string]any{}
	if value.ContextWindow != nil {
		view["contextWindow"] = *value.ContextWindow
	}
	if value.PressureTokens != nil {
		view["pressureTokens"] = *value.PressureTokens
		if value.SampledSurfaceTokens != nil {
			projected := *value.PressureTokens + value.SurfaceTokens - *value.SampledSurfaceTokens
			if projected < 0 {
				projected = 0
			}
			view["projectedTokens"] = projected
		}
	}
	return view
}

func applyContextBreakdownProjection(state any, event Event) any {
	current := state.(contextBreakdownProjectionState)
	next := current
	if event.Type == "request/header" {
		data, _ := event.Data.(map[string]any)
		header, _ := data["header"].(map[string]any)
		system, _ := header["system"].(string)
		if _, present := header["system"]; present {
			next.SystemTokens = projectionDensityPrice(system) + 4
		} else {
			next.SystemTokens = 0
		}
		next.ToolsTokens = 0
		if projectionContentLength(header["tools"]) > 0 {
			encoded, _ := json.Marshal(header["tools"])
			next.ToolsTokens = projectionDensityPrice(string(encoded)) + 4
		} else if tools, ok := header["tools"].([]ToolSchema); ok && len(tools) > 0 {
			encoded, _ := json.Marshal(tools)
			next.ToolsTokens = projectionDensityPrice(string(encoded)) + 4
		}
	}
	delta, claim := projectionSurfaceStep(event, claimForProjectionStep(current.Claim))
	next.MessageTokens += delta
	if next.MessageTokens < 0 {
		next.MessageTokens = 0
	}
	next.Claim = claimFromProjectionStep(claim)
	if reflect.DeepEqual(next, current) {
		return state
	}
	return next
}

func applySessionStatsProjection(state any, event Event) any {
	current := state.(sessionStatsProjectionState)
	data, _ := event.Data.(map[string]any)
	switch event.Type {
	case "step/start":
		turn, turnOK := eventSeqNumber(data["turn"])
		step, stepOK := eventSeqNumber(data["step"])
		if !turnOK || !stepOK {
			return state
		}
		current.Open = &sessionStatsOpenStep{Turn: turn, Step: step, Start: event.Time}
		return current
	case "assistant/chunk":
		if current.Open == nil || current.Open.FirstToken != nil || !projectionChunkHasToken(event) {
			return state
		}
		turn, turnOK := eventSeqNumber(data["turn"])
		step, stepOK := eventSeqNumber(data["step"])
		if !turnOK || !stepOK || turn != current.Open.Turn || step != current.Open.Step {
			return state
		}
		time := event.Time
		open := *current.Open
		open.FirstToken = &time
		current.Open = &open
		return current
	case "assistant/message":
		if current.Open == nil {
			return state
		}
		turn, turnOK := eventSeqNumber(data["turn"])
		step, stepOK := eventSeqNumber(data["step"])
		if !turnOK || !stepOK || turn != current.Open.Turn || step != current.Open.Step {
			return state
		}
		if elapsed := event.Time - current.Open.Start; elapsed > 0 {
			current.LLMMS += elapsed
		}
		if current.Open.FirstToken != nil {
			if elapsed := *current.Open.FirstToken - current.Open.Start; elapsed > 0 {
				current.TTFTMS += elapsed
			}
			current.TTFTSteps++
			if usage, ok := usageBuckets(data["usage"]); ok {
				if elapsed := event.Time - *current.Open.FirstToken; elapsed > 0 {
					current.DecodeMS += elapsed
				}
				current.DecodeTokens += usage.OutputTokens
			}
		}
		current.Open = nil
		return current
	case "tool/call":
		callID := projectionCallID(event)
		if callID == "" {
			return state
		}
		pending := make(map[string]int64, len(current.PendingCalls)+1)
		for id, started := range current.PendingCalls {
			pending[id] = started
		}
		pending[callID] = event.Time
		current.PendingCalls = pending
		return current
	case "tool/result":
		callID := projectionCallID(event)
		started, ok := current.PendingCalls[callID]
		if !ok {
			return state
		}
		pending := make(map[string]int64, len(current.PendingCalls)-1)
		for id, value := range current.PendingCalls {
			if id != callID {
				pending[id] = value
			}
		}
		current.PendingCalls = pending
		if elapsed := event.Time - started; elapsed > 0 {
			current.ToolMS += elapsed
		}
		return current
	case "step/end":
		turn, ok := eventSeqNumber(data["turn"])
		if !ok {
			return state
		}
		if turn != current.LastTurn {
			current.Turns++
			current.LastTurn = turn
		}
		current.Steps++
		current.Open = nil
		return current
	case "turn/end":
		if len(current.PendingCalls) == 0 {
			return state
		}
		current.PendingCalls = map[string]int64{}
		return current
	default:
		return state
	}
}

func viewSessionStatsProjection(state any) any {
	value := state.(sessionStatsProjectionState)
	return map[string]any{
		"turns": value.Turns, "steps": value.Steps, "llmMs": value.LLMMS, "toolMs": value.ToolMS,
		"ttftMs": value.TTFTMS, "ttftSteps": value.TTFTSteps, "decodeMs": value.DecodeMS, "decodeTokens": value.DecodeTokens,
	}
}

func applySessionListProjection(state any, event Event) any {
	current := state.(sessionListProjectionState)
	next := current
	if event.Type == "turn/start" {
		next.Blank = false
	}
	if event.Type == "user/message" && eventSourceKind(event.Data) == "user" {
		next.LastPromptAt = event.Time
	}
	if next == current {
		return state
	}
	return next
}

func applySubagentIdentityProjection(state any, event Event) any {
	if event.Type != "subagent/descriptor" {
		return state
	}
	data, _ := event.Data.(map[string]any)
	mode, label, ok := validSubagentDescriptor(data)
	if !ok {
		return nil
	}
	identity := map[string]any{"mode": mode, "seq": event.Seq}
	if mode == "continuable" || label != "" {
		identity["label"] = label
	}
	return identity
}

func applySubagentTimingProjection(state any, event Event) any {
	current := state.(subagentTimingProjectionState)
	switch event.Type {
	case "turn/start":
		if current.DescriptorSeen {
			current.Active = &subagentActiveInterval{Since: event.Time, Through: event.Time}
			return current
		}
		if current.PendingTurnStart != nil && *current.PendingTurnStart == event.Time {
			return state
		}
		time := event.Time
		current.PendingTurnStart = &time
		return current
	case "subagent/descriptor":
		var since *int64
		if current.Active != nil {
			value := current.Active.Since
			since = &value
		} else if current.PendingTurnStart != nil {
			value := *current.PendingTurnStart
			since = &value
		}
		next := subagentTimingProjectionState{DescriptorSeen: true}
		if since != nil {
			next.Active = &subagentActiveInterval{Since: *since, Through: event.Time}
		}
		return next
	case "turn/end":
		if !current.DescriptorSeen {
			if current.PendingTurnStart == nil {
				return state
			}
			current.PendingTurnStart = nil
			return current
		}
		if current.Active == nil {
			return state
		}
		if elapsed := event.Time - current.Active.Since; elapsed > 0 {
			current.SettledMS += elapsed
		}
		current.Active = nil
		return current
	default:
		if current.Active == nil || current.Active.Through == event.Time {
			return state
		}
		active := *current.Active
		active.Through = event.Time
		current.Active = &active
		return current
	}
}
