package harness

import (
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

	"github.com/dop251/goja"
)

const InvariantErrorCode = "INVARIANT"

const (
	invariantPackageRegistry        = "@deepseek-ai/dsh-invariants"
	invariantPackageSession         = "@deepseek-ai/dsh-session"
	invariantPackageSessionTitle    = "@deepseek-ai/dsh-session-title"
	invariantPackageTimeContext     = "@deepseek-ai/dsh-time-context"
	invariantPackagePermission      = "@deepseek-ai/dsh-permission-presets"
	invariantPackageGoal            = "@deepseek-ai/dsh-goal"
	invariantPackageSchedule        = "@deepseek-ai/dsh-schedule"
	invariantPackageAgentTeam       = "@deepseek-ai/dsh-experimental-agent-team"
	invariantPackageAgentLoop       = "@deepseek-ai/dsh-agent-loop"
	invariantPackageSessionRef      = "@deepseek-ai/dsh-session-reference"
	invariantPackageTmuxContext     = "@deepseek-ai/dsh-tmux-context"
	invariantPackageProjectionCache = "@deepseek-ai/dsh-session-projection-cache"
	invariantPackageToolWeb         = "@deepseek-ai/dsh-tool-web"
	invariantPackageWebFetchHTTP    = "@deepseek-ai/dsh-web-fetch-http"
)

var timeContextReadingPattern = regexp.MustCompile(
	`^Time sampled while preparing turn (\d+), step (\d+): ` +
		`(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})\[[^\]]+\])\n` +
		`(Browser time zone for this request: .+)\n` +
		`Elapsed since the preceding (model-visible message|step context): ` +
		`(?:unavailable|(?:(?:\d+d )?(?:\d+h )?(?:\d+m )?\d+s))\.$`,
)

// RuntimeInvariantConfig selects package-owned diagnostic checks. Patterns use
// JavaScript RegExp source semantics to match the TypeScript implementation.
type RuntimeInvariantConfig struct {
	Disabled         bool
	PackageAllowlist []string
	PackageBlocklist []string
}

func cloneRuntimeInvariantConfig(config *RuntimeInvariantConfig) *RuntimeInvariantConfig {
	if config == nil {
		return nil
	}
	clone := *config
	clone.PackageAllowlist = append([]string(nil), config.PackageAllowlist...)
	clone.PackageBlocklist = append([]string(nil), config.PackageBlocklist...)
	return &clone
}

// InvariantError attributes a runtime relationship failure to its owning
// upstream package.
type InvariantError struct {
	Code        string
	PackageName string
	Detail      string
}

func (e *InvariantError) Error() string {
	return fmt.Sprintf("invariant violated by %q: %s", e.PackageName, e.Detail)
}

func invariantFailure(packageName string) InvariantFailure {
	return func(message string) error {
		return &InvariantError{Code: InvariantErrorCode, PackageName: packageName, Detail: message}
	}
}

// InvariantFailure constructs a package-attributed invariant error.
type InvariantFailure func(message string) error

// SessionInvariant checks one complete proposed durable event stream.
type SessionInvariant func(SessionHeader, []Event) error

// AgentRequestInvariant checks a loop-built request immediately before provider dispatch.
type AgentRequestInvariant func(*Engine, *Session, ChatRequest) error

// InvariantDisposer removes one package registration after its owned cleanup completes.
type InvariantDisposer func() error

// InvariantInstaller contributes checks and cleanup under one package owner.
type InvariantInstaller func(*InvariantScope, InvariantFailure) error

// InvariantScope collects one registration's checks before atomic publication.
type InvariantScope struct {
	sessionChecks []SessionInvariant
	requestChecks []AgentRequestInvariant
	cleanups      []InvariantDisposer
}

func (s *InvariantScope) CheckSessions(check SessionInvariant) {
	if check == nil {
		panic("invariants: nil session check")
	}
	s.sessionChecks = append(s.sessionChecks, check)
}

func (s *InvariantScope) CheckAgentRequests(check AgentRequestInvariant) {
	if check == nil {
		panic("invariants: nil agent request check")
	}
	s.requestChecks = append(s.requestChecks, check)
}

func (s *InvariantScope) Defer(dispose InvariantDisposer) {
	if dispose == nil {
		panic("invariants: nil disposer")
	}
	s.cleanups = append(s.cleanups, dispose)
}

type javascriptPattern struct {
	source  string
	program *goja.Program
}

func compileInvariantPatterns(field string, values []string) ([]javascriptPattern, error) {
	seen := make(map[string]bool, len(values))
	patterns := make([]javascriptPattern, 0, len(values))
	for _, value := range values {
		if value == "" || strings.TrimSpace(value) != value {
			return nil, fmt.Errorf("invariants: %s entries must be non-blank and have no surrounding whitespace", field)
		}
		if seen[value] {
			return nil, fmt.Errorf("invariants: %s contains duplicate regex %q", field, value)
		}
		seen[value] = true
		encoded, _ := json.Marshal(value)
		program, err := goja.Compile("invariants-regexp", "new RegExp("+string(encoded)+").test(__packageName)", true)
		if err == nil {
			vm := goja.New()
			_ = vm.Set("__packageName", "")
			_, err = vm.RunProgram(program)
		}
		if err != nil {
			return nil, fmt.Errorf("invariants: %s contains invalid regex %q: %w", field, value, err)
		}
		patterns = append(patterns, javascriptPattern{source: value, program: program})
	}
	return patterns, nil
}

func (p javascriptPattern) matches(packageName string) bool {
	vm := goja.New()
	_ = vm.Set("__packageName", packageName)
	value, err := vm.RunProgram(p.program)
	return err == nil && value.ToBoolean()
}

type ownedSessionInvariant struct {
	id    uint64
	owner string
	check SessionInvariant
}

type ownedRequestInvariant struct {
	id    uint64
	owner string
	check AgentRequestInvariant
}

type invariantRegistration struct {
	packageName string
	sessionIDs  []uint64
	requestIDs  []uint64
	cleanups    []InvariantDisposer
	dispose     InvariantDisposer
}

// InvariantRegistry owns package selection, unique registration, checks, and lifecycle.
type InvariantRegistry struct {
	mu            sync.RWMutex
	disabled      bool
	allowlist     []javascriptPattern
	blocklist     []javascriptPattern
	registrations map[string]*invariantRegistration
	sessionChecks map[uint64]ownedSessionInvariant
	requestChecks map[uint64]ownedRequestInvariant
	nextID        uint64
	closed        bool
}

func NewInvariantRegistry(config RuntimeInvariantConfig) (*InvariantRegistry, error) {
	allowlist, err := compileInvariantPatterns("package_allowlist", config.PackageAllowlist)
	if err != nil {
		return nil, err
	}
	blocklist, err := compileInvariantPatterns("package_blocklist", config.PackageBlocklist)
	if err != nil {
		return nil, err
	}
	return &InvariantRegistry{
		disabled: config.Disabled, allowlist: allowlist, blocklist: blocklist,
		registrations: make(map[string]*invariantRegistration),
		sessionChecks: make(map[uint64]ownedSessionInvariant),
		requestChecks: make(map[uint64]ownedRequestInvariant),
	}, nil
}

func validInvariantPackageName(packageName string) bool {
	return packageName != "" && strings.TrimSpace(packageName) == packageName &&
		strings.IndexFunc(packageName, unicode.IsSpace) < 0
}

func (r *InvariantRegistry) selected(packageName string) bool {
	if r.disabled {
		return false
	}
	if len(r.allowlist) > 0 {
		matched := false
		for _, pattern := range r.allowlist {
			if pattern.matches(packageName) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, pattern := range r.blocklist {
		if pattern.matches(packageName) {
			return false
		}
	}
	return true
}

func (r *InvariantRegistry) Register(packageName string, installer InvariantInstaller) (InvariantDisposer, error) {
	if !validInvariantPackageName(packageName) {
		return nil, errors.New("invariants: packageName must be non-blank and contain no whitespace")
	}
	if installer == nil {
		return nil, errors.New("invariants: installer is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("invariants: registry is inactive")
	}
	if _, exists := r.registrations[packageName]; exists {
		return nil, fmt.Errorf("invariants: package %q is already registered", packageName)
	}
	registration := &invariantRegistration{packageName: packageName}
	r.registrations[packageName] = registration
	if r.selected(packageName) {
		scope := &InvariantScope{}
		if err := installer(scope, invariantFailure(packageName)); err != nil {
			delete(r.registrations, packageName)
			return nil, errors.Join(err, runInvariantCleanups(scope.cleanups))
		}
		registration.cleanups = append([]InvariantDisposer(nil), scope.cleanups...)
		for _, check := range scope.sessionChecks {
			r.nextID++
			registration.sessionIDs = append(registration.sessionIDs, r.nextID)
			r.sessionChecks[r.nextID] = ownedSessionInvariant{id: r.nextID, owner: packageName, check: check}
		}
		for _, check := range scope.requestChecks {
			r.nextID++
			registration.requestIDs = append(registration.requestIDs, r.nextID)
			r.requestChecks[r.nextID] = ownedRequestInvariant{id: r.nextID, owner: packageName, check: check}
		}
	}
	var once sync.Once
	var disposeErr error
	registration.dispose = func() error {
		once.Do(func() { disposeErr = r.disposeRegistration(registration) })
		return disposeErr
	}
	return registration.dispose, nil
}

func runInvariantCleanups(cleanups []InvariantDisposer) error {
	var result error
	for index := len(cleanups) - 1; index >= 0; index-- {
		result = errors.Join(result, cleanups[index]())
	}
	return result
}

func (r *InvariantRegistry) disposeRegistration(registration *invariantRegistration) error {
	cleanupErr := runInvariantCleanups(registration.cleanups)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.registrations[registration.packageName] != registration {
		return cleanupErr
	}
	for _, id := range registration.sessionIDs {
		delete(r.sessionChecks, id)
	}
	for _, id := range registration.requestIDs {
		delete(r.requestChecks, id)
	}
	delete(r.registrations, registration.packageName)
	return cleanupErr
}

func attributeInvariantError(owner string, err error) error {
	if err == nil {
		return nil
	}
	var invariantErr *InvariantError
	if errors.As(err, &invariantErr) {
		return err
	}
	return invariantFailure(owner)(err.Error())
}

func (r *InvariantRegistry) sessionCheckSnapshot() []ownedSessionInvariant {
	r.mu.RLock()
	checks := make([]ownedSessionInvariant, 0, len(r.sessionChecks))
	for _, check := range r.sessionChecks {
		checks = append(checks, check)
	}
	r.mu.RUnlock()
	sort.Slice(checks, func(i, j int) bool { return checks[i].id < checks[j].id })
	return checks
}

func (r *InvariantRegistry) requestCheckSnapshot() []ownedRequestInvariant {
	r.mu.RLock()
	checks := make([]ownedRequestInvariant, 0, len(r.requestChecks))
	for _, check := range r.requestChecks {
		checks = append(checks, check)
	}
	r.mu.RUnlock()
	sort.Slice(checks, func(i, j int) bool { return checks[i].id < checks[j].id })
	return checks
}

func (r *InvariantRegistry) ValidateSession(header SessionHeader, events []Event) error {
	// ponytail: diagnostics re-fold the proposed log; add incremental traces only if profiling shows this opt-in path is material.
	for _, owned := range r.sessionCheckSnapshot() {
		if err := owned.check(header, events); err != nil {
			return attributeInvariantError(owned.owner, err)
		}
	}
	return nil
}

func (r *InvariantRegistry) validateSessionAppend(header SessionHeader, history []Event, event Event) error {
	checks := r.sessionCheckSnapshot()
	if len(checks) == 0 {
		return nil
	}
	events := make([]Event, len(history)+1)
	copy(events, history)
	events[len(history)] = event
	for _, owned := range checks {
		if err := owned.check(header, events); err != nil {
			return attributeInvariantError(owned.owner, err)
		}
	}
	return nil
}

func (r *InvariantRegistry) ValidateAgentRequest(engine *Engine, session *Session, request ChatRequest) error {
	for _, owned := range r.requestCheckSnapshot() {
		if err := owned.check(engine, session, request); err != nil {
			return attributeInvariantError(owned.owner, err)
		}
	}
	return nil
}

func (r *InvariantRegistry) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	registrations := make([]*invariantRegistration, 0, len(r.registrations))
	for _, registration := range r.registrations {
		registrations = append(registrations, registration)
	}
	r.mu.Unlock()
	sort.Slice(registrations, func(i, j int) bool { return registrations[i].packageName < registrations[j].packageName })
	var result error
	for index := len(registrations) - 1; index >= 0; index-- {
		result = errors.Join(result, registrations[index].dispose())
	}
	return result
}

func (e *Engine) InvariantRegistry() *InvariantRegistry { return e.invariants }

func (e *Engine) installRuntimeInvariants() error {
	registrations := []struct {
		packageName string
		installer   InvariantInstaller
	}{
		{invariantPackageRegistry, func(*InvariantScope, InvariantFailure) error { return nil }},
		{invariantPackageSession, installSessionInvariant},
		{invariantPackageSessionTitle, installSessionTitleInvariant},
		{invariantPackageTimeContext, installTimeContextInvariant},
		{invariantPackagePermission, installPermissionInvariant},
		{invariantPackageGoal, installGoalInvariant},
		{invariantPackageAgentLoop, installAgentLoopInvariant},
		{invariantPackageSessionRef, func(*InvariantScope, InvariantFailure) error { return nil }},
		{invariantPackageTmuxContext, func(*InvariantScope, InvariantFailure) error { return nil }},
		{invariantPackageProjectionCache, func(*InvariantScope, InvariantFailure) error { return nil }},
		{invariantPackageToolWeb, func(*InvariantScope, InvariantFailure) error { return nil }},
		{invariantPackageWebFetchHTTP, func(*InvariantScope, InvariantFailure) error { return nil }},
	}
	if e.cfg.ScheduleEnabled {
		registrations = append(registrations, struct {
			packageName string
			installer   InvariantInstaller
		}{invariantPackageSchedule, installScheduleInvariant})
	}
	if e.cfg.AgentTeams != nil {
		registrations = append(registrations, struct {
			packageName string
			installer   InvariantInstaller
		}{invariantPackageAgentTeam, installTeamInvariant})
	}
	for _, registration := range registrations {
		if _, err := e.invariants.Register(registration.packageName, registration.installer); err != nil {
			return err
		}
	}
	return nil
}

type sessionInvariantTrace struct {
	lastSeq      int
	openTurn     int
	openStep     int
	nextTurn     int
	nextStep     int
	pendingCalls map[string]bool
}

func newSessionInvariantTrace() sessionInvariantTrace {
	return sessionInvariantTrace{lastSeq: -1, openTurn: -1, openStep: -1, nextTurn: 1, nextStep: 1, pendingCalls: map[string]bool{}}
}

func invariantEventPosition(event Event) (int, int, error) {
	turn, turnOK := eventFieldInt(event.Data, "turn")
	step, stepOK := eventFieldInt(event.Data, "step")
	if !turnOK || !stepOK {
		return 0, 0, fmt.Errorf("%s must name integer turn and step", event.Type)
	}
	return turn, step, nil
}

func requireInvariantOpenStep(trace sessionInvariantTrace, event Event) error {
	turn, step, err := invariantEventPosition(event)
	if err != nil {
		return err
	}
	if trace.openTurn != turn || trace.openStep != step {
		return fmt.Errorf("%s names turn %d/step %d but open is turn %d/step %d", event.Type, turn, step, trace.openTurn, trace.openStep)
	}
	return nil
}

func validateSessionRelations(events []Event) error {
	trace := newSessionInvariantTrace()
	for _, event := range events {
		if int(event.Seq) <= trace.lastSeq {
			return fmt.Errorf("seq must strictly increase: saw %d after %d", event.Seq, trace.lastSeq)
		}
		trace.lastSeq = int(event.Seq)
		switch event.Type {
		case "turn/start":
			turn, ok := eventFieldInt(event.Data, "turn")
			if !ok {
				return errors.New("turn/start must name an integer turn")
			}
			if trace.openTurn >= 0 {
				return fmt.Errorf("turn/start %d while turn %d is still open", turn, trace.openTurn)
			}
			if turn != trace.nextTurn {
				return fmt.Errorf("turn/start expected turn %d, got %d", trace.nextTurn, turn)
			}
			trace.openTurn, trace.nextStep = turn, 1
		case "turn/end":
			turn, ok := eventFieldInt(event.Data, "turn")
			if !ok || trace.openTurn != turn {
				return fmt.Errorf("turn/end %d does not match open turn %d", turn, trace.openTurn)
			}
			if trace.openStep >= 0 {
				return fmt.Errorf("turn/end %d while step %d is still open", turn, trace.openStep)
			}
			trace.openTurn = -1
			trace.nextTurn++
		case "step/start":
			turn, step, err := invariantEventPosition(event)
			if err != nil {
				return err
			}
			if trace.openTurn != turn {
				return fmt.Errorf("step/start in turn %d but open turn is %d", turn, trace.openTurn)
			}
			if trace.openStep >= 0 {
				return fmt.Errorf("step/start %d while step %d is still open", step, trace.openStep)
			}
			if step != trace.nextStep {
				return fmt.Errorf("step/start expected step %d in turn %d, got %d", trace.nextStep, turn, step)
			}
			trace.openStep = step
		case "step/end":
			if err := requireInvariantOpenStep(trace, event); err != nil {
				return err
			}
			trace.openStep = -1
			trace.nextStep++
			clear(trace.pendingCalls)
		case "assistant/chunk", "assistant/message", "tool/call":
			if err := requireInvariantOpenStep(trace, event); err != nil {
				return err
			}
			if event.Type == "tool/call" {
				data, _ := event.Data.(map[string]any)
				if callID := stringValue(data["callId"]); callID != "" {
					trace.pendingCalls[callID] = true
				}
			}
		case "tool/result":
			if event.SurfaceOp != "append" {
				if trace.openTurn < 0 {
					return errors.New("tool/result surface replacement appended outside any open turn")
				}
				continue
			}
			if err := requireInvariantOpenStep(trace, event); err != nil {
				return err
			}
			data, _ := event.Data.(map[string]any)
			message := nestedMessage(event.Data)
			source, _ := message["source"].(map[string]any)
			callID := stringValue(source["callId"])
			errorData, _ := data["error"].(map[string]any)
			blocks := contentBlocks(message["content"])
			syntheticNotStarted := len(blocks) > 0 && blocks[0].IsError && stringValue(errorData["code"]) == "TOOL_NOT_STARTED"
			if !trace.pendingCalls[callID] && !syntheticNotStarted {
				return fmt.Errorf("tool/result for %s with no prior tool/call in this step", callID)
			}
			delete(trace.pendingCalls, callID)
		case "todo/write", "request/header", "request/context":
			if trace.openTurn < 0 {
				return fmt.Errorf("%s appended outside any open turn (core execution events must be turn-enclosed)", event.Type)
			}
		}
	}
	return nil
}

func installSessionInvariant(scope *InvariantScope, fail InvariantFailure) error {
	scope.CheckSessions(func(_ SessionHeader, events []Event) error {
		if err := validateSessionRelations(events); err != nil {
			return fail(err.Error())
		}
		return nil
	})
	return nil
}

func intSliceLength(value any) (int, bool) {
	switch values := value.(type) {
	case []int:
		return len(values), true
	case []any:
		for _, value := range values {
			if _, ok := eventSeqNumber(value); !ok {
				return 0, false
			}
		}
		return len(values), true
	default:
		return 0, false
	}
}

func installSessionTitleInvariant(scope *InvariantScope, fail InvariantFailure) error {
	scope.CheckSessions(func(_ SessionHeader, events []Event) error {
		for _, event := range events {
			if event.Type != "session/title" {
				continue
			}
			data, _ := event.Data.(map[string]any)
			source, _ := data["source"].(map[string]any)
			kind := stringValue(source["kind"])
			count, ok := intSliceLength(data["messageSeqs"])
			if !ok || (count == 0) != (kind == "user") {
				requirement := "cite at least one message seq"
				if kind == "user" {
					requirement = "cite no message seqs"
				}
				return fail(fmt.Sprintf("session/title event %d with source %q must %s; got %d", event.Seq, kind, requirement, count))
			}
		}
		return nil
	})
	return nil
}

func timeContextPosition(history []Event) (int, int, error) {
	openTurn, openStep := -1, -1
	requestStarted := false
	for _, event := range history {
		switch event.Type {
		case "turn/start":
			openTurn, _ = eventFieldInt(event.Data, "turn")
			openStep, requestStarted = -1, false
		case "step/start":
			openStep, _ = eventFieldInt(event.Data, "step")
			requestStarted = false
		case "request/header":
			requestStarted = true
		case "step/end":
			openStep, requestStarted = -1, false
		case "turn/end":
			openTurn, openStep, requestStarted = -1, -1, false
		}
	}
	if openTurn < 0 {
		return 0, 0, errors.New("time-context reading must be appended inside an open turn")
	}
	if openStep < 0 {
		return 0, 0, errors.New("time-context reading must follow step/start")
	}
	if requestStarted {
		return 0, 0, errors.New("time-context reading must precede request/header")
	}
	return openTurn, openStep, nil
}

func exactTimeContextText(data map[string]any) (string, bool) {
	encoded, err := json.Marshal(data["content"])
	if err != nil {
		return "", false
	}
	var blocks []map[string]any
	if json.Unmarshal(encoded, &blocks) != nil || len(blocks) != 1 || len(blocks[0]) != 2 || blocks[0]["type"] != "text" {
		return "", false
	}
	text, ok := blocks[0]["text"].(string)
	return text, ok
}

func expectedBrowserTimeZoneText(history []Event, turn int) (string, []string, error) {
	zones, err := browserTimeZones(history, turn)
	if err != nil {
		return "", nil, err
	}
	switch len(zones) {
	case 0:
		return "Browser time zone for this request: unavailable. Ask the user to clarify otherwise-unqualified dates and times.", zones, nil
	case 1:
		return "Browser time zone for this request: " + zones[0] + ". Interpret otherwise-unqualified dates and times in this zone.", zones, nil
	default:
		encoded, _ := json.Marshal(zones)
		return "Browser time zone for this request: mixed " + string(encoded) + ". Ask the user to clarify otherwise-unqualified dates and times.", zones, nil
	}
}

func validateTimeContextReading(history []Event, event Event) error {
	data, _ := event.Data.(map[string]any)
	text, ok := exactTimeContextText(data)
	if !ok {
		return errors.New("time-context messages must contain exactly one text block")
	}
	match := timeContextReadingPattern.FindStringSubmatch(text)
	if match == nil {
		return errors.New("time-context message does not match the durable reading format")
	}
	turn, turnErr := strconv.Atoi(match[1])
	step, stepErr := strconv.Atoi(match[2])
	if turnErr != nil || stepErr != nil || turn < 1 || step < 1 || int64(turn) > maxJSONSafeInteger || int64(step) > maxJSONSafeInteger {
		return errors.New("time-context turn and step must be positive safe integers")
	}
	expectedTurn, expectedStep, err := timeContextPosition(history)
	if err != nil {
		return err
	}
	if turn != expectedTurn || step != expectedStep {
		return fmt.Errorf("time-context reading names turn %d/step %d, expected turn %d/step %d", turn, step, expectedTurn, expectedStep)
	}
	source, _ := data["source"].(map[string]any)
	sectionsEncoded, _ := json.Marshal(source["sections"])
	var sections []map[string]any
	if len(source) != 4 || source["kind"] != "plugin" || source["plugin"] != "time-context" || source["form"] != "snapshot" ||
		json.Unmarshal(sectionsEncoded, &sections) != nil || len(sections) != 1 || len(sections[0]) != 2 ||
		sections[0]["name"] != "time-context" || sections[0]["text"] != text {
		return errors.New("time-context source must carry only the exact snapshot text, not request authority")
	}
	expectedBrowser, zones, err := expectedBrowserTimeZoneText(history, turn)
	if err != nil {
		return fmt.Errorf("time-context browser zone is invalid: %w", err)
	}
	if match[4] != expectedBrowser {
		return errors.New("time-context browser-zone text does not match current-turn user messages")
	}
	if (step == 1) != (match[5] == "model-visible message") {
		return fmt.Errorf("time-context step %d uses the wrong elapsed-time baseline %q", step, match[5])
	}
	rendered := match[3]
	bracket := strings.LastIndexByte(rendered, '[')
	if bracket < 0 {
		return errors.New("time-context reading omitted its rendered timestamp")
	}
	instant, err := time.Parse(time.RFC3339, rendered[:bracket])
	if err != nil || event.Time < -maxJSONSafeInteger || event.Time > maxJSONSafeInteger || event.Time < instant.UnixMilli() {
		return errors.New("time-context rendered timestamp must parse and not postdate its durable event")
	}
	if len(zones) == 1 {
		location, err := time.LoadLocation(zones[0])
		if err != nil {
			return fmt.Errorf("time-context browser zone cannot format its durable timestamp: %w", err)
		}
		if formatContextTimestamp(instant, location, zones[0]) != rendered {
			return errors.New("time-context rendered timestamp does not match the unique browser zone")
		}
	}
	return nil
}

func installTimeContextInvariant(scope *InvariantScope, fail InvariantFailure) error {
	scope.CheckSessions(func(_ SessionHeader, events []Event) error {
		for index, event := range events {
			if !isPluginMessage(event, "time-context") {
				continue
			}
			if err := validateTimeContextReading(events[:index], event); err != nil {
				return fail(err.Error())
			}
		}
		return nil
	})
	return nil
}

func installPermissionInvariant(scope *InvariantScope, fail InvariantFailure) error {
	scope.CheckSessions(func(_ SessionHeader, events []Event) error {
		for _, event := range events {
			if event.Type != "permission/preset" {
				continue
			}
			data, _ := event.Data.(map[string]any)
			preset := stringValue(data["preset"])
			if _, ok := commandPermissionPresets[preset]; !ok {
				return fail(fmt.Sprintf("permission/preset names unknown preset %q", preset))
			}
		}
		return nil
	})
	return nil
}

func installGoalInvariant(scope *InvariantScope, fail InvariantFailure) error {
	scope.CheckSessions(func(_ SessionHeader, events []Event) error {
		if _, _, err := foldGoalState(events); err != nil {
			seq := -1
			if len(events) > 0 {
				seq = int(events[len(events)-1].Seq)
			}
			return fail(fmt.Sprintf("session event %d violates the durable goal stream: %s", seq, err))
		}
		return nil
	})
	return nil
}

func installTeamInvariant(scope *InvariantScope, fail InvariantFailure) error {
	scope.CheckSessions(func(header SessionHeader, events []Event) error {
		if _, err := foldTeam(header.ID, events); err != nil {
			seq := -1
			if len(events) > 0 {
				seq = int(events[len(events)-1].Seq)
			}
			return fail(fmt.Sprintf("session event %d violates the Agent Teams stream: %s", seq, err))
		}
		return nil
	})
	return nil
}

func latestRequestHeader(events []Event) map[string]any {
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type != "request/header" {
			continue
		}
		data, _ := events[index].Data.(map[string]any)
		header, _ := data["header"].(map[string]any)
		return header
	}
	return nil
}

func latestRequestTurn(events []Event) int {
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type == "step/start" {
			turn, _ := eventFieldInt(events[index].Data, "turn")
			return turn
		}
	}
	return -1
}

func installAgentLoopInvariant(scope *InvariantScope, fail InvariantFailure) error {
	scope.CheckAgentRequests(func(engine *Engine, session *Session, request ChatRequest) error {
		if request.SessionID == "" {
			return fail("a loop-built request must carry a session id")
		}
		if session == nil || session.Header.ID != request.SessionID {
			return fail(fmt.Sprintf("a loop-built request must carry a live session id, got %q", request.SessionID))
		}
		live, liveErr := engine.getSession(request.SessionID)
		if liveErr != nil || live != session {
			return fail(fmt.Sprintf("a loop-built request must carry a live session id, got %q", request.SessionID))
		}
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		provider := session.Model.Provider
		session.mu.Unlock()
		turn := latestRequestTurn(events)
		stepStarted := false
		for _, event := range events {
			if event.Type == "step/start" {
				stepStarted = true
			}
		}
		if !stepStarted {
			return fail("a loop-built request with no step/start in its session log")
		}
		header := latestRequestHeader(events)
		if header == nil {
			return fail("a loop-built request with no request/header event in its session log")
		}
		expectedMessages := engine.hydrateChatMessagesWithLimit(transcriptMessages(events, turn), engine.requestImageLimit(provider))
		if !jsonEqual(request.Messages, expectedMessages) {
			return fail(fmt.Sprintf("llm request for session %q diverges from the dispatch-time durable derivation (log-reconstruction desync)", session.Header.ID))
		}
		config, _ := header["config"].(map[string]any)
		model := stringValue(config["model"])
		system := stringValue(header["system"])
		temperature, hasTemperature := finiteFloatSetting(config["temperature"])
		maxTokens := positiveIntSetting(config["maxTokens"], 0)
		stop, hasStop := config["stop"]
		tools := header["tools"]
		if tools == nil {
			tools = []ToolSchema{}
		}
		requestTools := any(request.Tools)
		if request.Tools == nil {
			requestTools = []ToolSchema{}
		}
		requestTemperatureMatches := request.Temperature == nil && !hasTemperature ||
			request.Temperature != nil && hasTemperature && *request.Temperature == temperature
		requestStop := any(request.Stop)
		if request.Stop == nil {
			requestStop = nil
		}
		if request.Model != model || request.System != system || !requestTemperatureMatches || request.MaxTokens != maxTokens ||
			!jsonEqual(requestStop, func() any {
				if hasStop {
					return stop
				}
				return nil
			}()) ||
			!jsonEqual(requestTools, tools) {
			return fail(fmt.Sprintf("llm request for session %q diverges from the folded request header", session.Header.ID))
		}
		return nil
	})
	return nil
}
