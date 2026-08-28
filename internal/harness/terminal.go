package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	TerminalWaitStdinRead    = "stdin_read"
	TerminalWaitInferredIdle = "inferred_idle"
	TerminalWaitTimeout      = "timeout"
	TerminalWaitSessionExit  = "session_exit"

	TerminalSignalInterrupt = "SIGINT"
	TerminalSignalTerminate = "SIGTERM"
	TerminalSignalKill      = "SIGKILL"
	TerminalSignalStop      = "SIGTSTP"
	TerminalSignalHangup    = "SIGHUP"

	TerminalShellDialectBash = "bash"
	TerminalShellDialectPwsh = "pwsh"

	defaultTerminalMaxResultBytes = 256 * 1024
	minimumTerminalResultBytes    = 64

	terminalPowerShellEncodingPreamble = "[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false); $OutputEncoding = [System.Text.UTF8Encoding]::new($false); "
	terminalPowerShellPrompt           = "function prompt { [Console]::Write([char]27 + ']133;D;' + [int]$LASTEXITCODE + [char]7); '" + terminalPrompt + "' }"
)

// TerminalConfig configures the built-in interactive shell PTY backend.
type TerminalConfig struct {
	Disabled              bool          `json:"disabled,omitempty" yaml:"disabled,omitempty"`
	BackendType           string        `json:"backendType,omitempty" yaml:"backendType,omitempty"`
	ShellDialect          string        `json:"shellDialect,omitempty" yaml:"shellDialect,omitempty"`
	ShellPath             string        `json:"shellPath,omitempty" yaml:"shellPath,omitempty"`
	ShellArgs             []string      `json:"shellArgs,omitempty" yaml:"shellArgs,omitempty"`
	Rows                  int           `json:"rows,omitempty" yaml:"rows,omitempty"`
	Cols                  int           `json:"cols,omitempty" yaml:"cols,omitempty"`
	ScrollbackLines       int           `json:"scrollbackLines,omitempty" yaml:"scrollbackLines,omitempty"`
	ScrollbackMaxBytes    int           `json:"scrollbackMaxBytes,omitempty" yaml:"scrollbackMaxBytes,omitempty"`
	MaxReadBytes          int           `json:"maxReadBytes,omitempty" yaml:"maxReadBytes,omitempty"`
	PollInterval          time.Duration `json:"-" yaml:"-"`
	ExactProbeAfter       time.Duration `json:"-" yaml:"-"`
	IdleSilence           time.Duration `json:"-" yaml:"-"`
	HandoffGrace          time.Duration `json:"-" yaml:"-"`
	Timeout               time.Duration `json:"-" yaml:"-"`
	DisposeGrace          time.Duration `json:"-" yaml:"-"`
	PollIntervalMillis    int           `json:"pollIntervalMs,omitempty" yaml:"pollIntervalMs,omitempty"`
	ExactProbeAfterMillis int           `json:"exactProbeAfterMs,omitempty" yaml:"exactProbeAfterMs,omitempty"`
	IdleSilenceMillis     int           `json:"idleSilenceMs,omitempty" yaml:"idleSilenceMs,omitempty"`
	HandoffGraceMillis    int           `json:"handoffGraceMs,omitempty" yaml:"handoffGraceMs,omitempty"`
	TimeoutMillis         int           `json:"timeoutMs,omitempty" yaml:"timeoutMs,omitempty"`
	DisposeGraceMillis    int           `json:"disposeGraceMs,omitempty" yaml:"disposeGraceMs,omitempty"`
}

// TerminalToolConfig controls the six model-facing terminal tools.
type TerminalToolConfig struct {
	Disabled              bool  `json:"disabled,omitempty" yaml:"disabled,omitempty"`
	EnableRunInBackground *bool `json:"enableRunInBackground,omitempty" yaml:"enableRunInBackground,omitempty"`
	MaxResultBytes        int   `json:"maxResultBytes,omitempty" yaml:"maxResultBytes,omitempty"`
}

func defaultTerminalConfig() TerminalConfig {
	shellDialect, shellPath, shellArgs := platformTerminalShellDefaults()
	return TerminalConfig{
		BackendType: "shell", ShellDialect: shellDialect, ShellPath: shellPath, ShellArgs: shellArgs,
		Rows: 40, Cols: 160, ScrollbackLines: 10_000, ScrollbackMaxBytes: 4 * 1024 * 1024,
		MaxReadBytes: 256 * 1024, PollInterval: 50 * time.Millisecond,
		ExactProbeAfter: 150 * time.Millisecond, IdleSilence: 3 * time.Second,
		HandoffGrace: 500 * time.Millisecond, Timeout: 30 * time.Second, DisposeGrace: 3 * time.Second,
	}
}

func normalizeTerminalConfig(config TerminalConfig) TerminalConfig {
	defaults := defaultTerminalConfig()
	if config.BackendType == "" {
		config.BackendType = defaults.BackendType
	}
	if config.ShellDialect == "" {
		config.ShellDialect = defaults.ShellDialect
	}
	shellPath, shellArgs := terminalShellDefaults(config.ShellDialect)
	if config.ShellPath == "" {
		config.ShellPath = shellPath
	}
	if len(config.ShellArgs) == 0 {
		config.ShellArgs = append([]string(nil), shellArgs...)
	} else {
		config.ShellArgs = append([]string(nil), config.ShellArgs...)
	}
	if config.Rows == 0 {
		config.Rows = defaults.Rows
	}
	if config.Cols == 0 {
		config.Cols = defaults.Cols
	}
	if config.ScrollbackLines == 0 {
		config.ScrollbackLines = defaults.ScrollbackLines
	}
	if config.ScrollbackMaxBytes == 0 {
		config.ScrollbackMaxBytes = defaults.ScrollbackMaxBytes
	}
	if config.MaxReadBytes == 0 {
		config.MaxReadBytes = defaults.MaxReadBytes
	}
	config.PollInterval = terminalDuration(config.PollInterval, config.PollIntervalMillis, defaults.PollInterval)
	config.ExactProbeAfter = terminalDuration(config.ExactProbeAfter, config.ExactProbeAfterMillis, defaults.ExactProbeAfter)
	config.IdleSilence = terminalDuration(config.IdleSilence, config.IdleSilenceMillis, defaults.IdleSilence)
	config.HandoffGrace = terminalDuration(config.HandoffGrace, config.HandoffGraceMillis, defaults.HandoffGrace)
	config.Timeout = terminalDuration(config.Timeout, config.TimeoutMillis, defaults.Timeout)
	config.DisposeGrace = terminalDuration(config.DisposeGrace, config.DisposeGraceMillis, defaults.DisposeGrace)
	return config
}

func terminalDuration(value time.Duration, milliseconds int, fallback time.Duration) time.Duration {
	if value != 0 {
		return value
	}
	if milliseconds != 0 {
		return time.Duration(milliseconds) * time.Millisecond
	}
	return fallback
}

func validateTerminalConfig(config TerminalConfig) error {
	if strings.TrimSpace(config.BackendType) == "" {
		return errors.New("terminal-bash: backendType must be non-empty")
	}
	if config.ShellDialect != TerminalShellDialectBash && config.ShellDialect != TerminalShellDialectPwsh {
		return fmt.Errorf("terminal-bash: unsupported shellDialect %q", config.ShellDialect)
	}
	if strings.TrimSpace(config.ShellPath) == "" {
		return errors.New("terminal-bash: shellPath must be non-empty")
	}
	for name, value := range map[string]int{
		"rows": config.Rows, "cols": config.Cols, "scrollbackLines": config.ScrollbackLines,
		"scrollbackMaxBytes": config.ScrollbackMaxBytes, "maxReadBytes": config.MaxReadBytes,
	} {
		if value < 1 {
			return fmt.Errorf("terminal-bash: %s must be a positive integer", name)
		}
	}
	for name, value := range map[string]time.Duration{
		"pollIntervalMs": config.PollInterval, "exactProbeAfterMs": config.ExactProbeAfter,
		"idleSilenceMs": config.IdleSilence, "handoffGraceMs": config.HandoffGrace,
		"timeoutMs": config.Timeout, "disposeGraceMs": config.DisposeGrace,
	} {
		if value < time.Millisecond {
			return fmt.Errorf("terminal-bash: %s must be positive", name)
		}
	}
	if config.MaxReadBytes > config.ScrollbackMaxBytes {
		return errors.New("terminal-bash: maxReadBytes must not exceed scrollbackMaxBytes")
	}
	if config.HandoffGrace < config.PollInterval {
		return errors.New("terminal-bash: handoffGraceMs must be at least pollIntervalMs")
	}
	return nil
}

func normalizeTerminalToolConfig(config TerminalToolConfig) TerminalToolConfig {
	if config.MaxResultBytes == 0 {
		config.MaxResultBytes = defaultTerminalMaxResultBytes
	}
	return config
}

func validateTerminalToolConfig(config TerminalToolConfig) error {
	if config.MaxResultBytes < minimumTerminalResultBytes {
		return fmt.Errorf("tool-terminal: maxResultBytes must be at least %d", minimumTerminalResultBytes)
	}
	return nil
}

func terminalBackgroundEnabled(config TerminalToolConfig) bool {
	return config.EnableRunInBackground == nil || *config.EnableRunInBackground
}

// NewBashTerminalBackend creates the built-in persistent interactive shell backend.
func NewBashTerminalBackend(config TerminalConfig) (TerminalBackend, error) {
	config = normalizeTerminalConfig(config)
	if err := validateTerminalConfig(config); err != nil {
		return nil, err
	}
	return newPlatformBashTerminalBackend(config), nil
}

// TerminalSessionStatus is the top-level PTY process status.
type TerminalSessionStatus struct {
	Kind     string  `json:"kind"`
	ExitCode *int    `json:"exitCode,omitempty"`
	Signal   *string `json:"signal,omitempty"`
}

func (status TerminalSessionStatus) MarshalJSON() ([]byte, error) {
	if status.Kind == "running" {
		return []byte(`{"kind":"running"}`), nil
	}
	type exitedStatus struct {
		Kind     string  `json:"kind"`
		ExitCode *int    `json:"exitCode"`
		Signal   *string `json:"signal"`
	}
	return json.Marshal(exitedStatus{Kind: "exited", ExitCode: status.ExitCode, Signal: status.Signal})
}

func runningTerminalStatus() TerminalSessionStatus { return TerminalSessionStatus{Kind: "running"} }

func cloneTerminalStatus(status TerminalSessionStatus) TerminalSessionStatus {
	clone := status
	if status.ExitCode != nil {
		value := *status.ExitCode
		clone.ExitCode = &value
	}
	if status.Signal != nil {
		value := *status.Signal
		clone.Signal = &value
	}
	return clone
}

type TerminalSpawnRequest struct {
	Type string `json:"type"`
	Name string `json:"name,omitempty"`
	CWD  string `json:"cwd,omitempty"`
}

type TerminalBackendSpawnSpec struct {
	TerminalSpawnRequest
	SessionID   string
	OwnerID     string
	Workspace   string
	SandboxMode string
}

func terminalShellEnvironment(config TerminalConfig, spec TerminalBackendSpawnSpec) map[string]string {
	env := map[string]string{
		"TERM": "dumb", "PAGER": "cat", "GIT_PAGER": "cat", "DSH_SHELL": "1",
		"DSH_SESSION_ID": spec.OwnerID, "DSH_PTY_SESSION_ID": spec.SessionID,
	}
	if config.ShellDialect == TerminalShellDialectPwsh {
		env["NO_COLOR"] = "1"
		return env
	}
	env["PS1"] = terminalPrompt
	env["PROMPT_COMMAND"] = `printf "\033]133;D;%s\007" "$?"; PS1='` + terminalPrompt + `'`
	env["BASH_SILENCE_DEPRECATION_WARNING"] = "1"
	return env
}

type TerminalSendRequest struct {
	Text   string `json:"text"`
	Submit bool   `json:"submit"`
}

type TerminalSendRead struct {
	Delta     string `json:"delta"`
	Truncated bool   `json:"truncated"`
}

type TerminalSendResult struct {
	Viewport      string                `json:"viewport"`
	WaitReason    string                `json:"waitReason"`
	SessionStatus TerminalSessionStatus `json:"sessionStatus"`
	Truncated     bool                  `json:"truncated"`
}

type TerminalReadRequest struct {
	Offset int `json:"offset,omitempty"`
	Count  int `json:"count,omitempty"`
}

type TerminalReadResult struct {
	Text       string `json:"text"`
	TotalLines int    `json:"totalLines"`
	LineBegin  int    `json:"lineBegin"`
	LineEnd    int    `json:"lineEnd"`
	Truncated  bool   `json:"truncated"`
}

type TerminalSignalResult struct {
	Delivered  bool `json:"delivered"`
	TargetPGID int  `json:"targetPgid"`
}

type TerminalSessionSnapshot struct {
	SessionID string                `json:"sessionId"`
	Name      string                `json:"name,omitempty"`
	Type      string                `json:"type"`
	PID       int                   `json:"pid,omitempty"`
	Status    TerminalSessionStatus `json:"status"`
}

type TerminalSpawnResult struct {
	TerminalSessionSnapshot
	MOTD string `json:"motd"`
}

// TerminalSendOperation is one exclusive live send on a PTY session.
type TerminalSendOperation interface {
	Done() <-chan struct{}
	Result() (TerminalSendResult, error)
	ReadOutput() TerminalSendRead
	Cancel() bool
}

// TerminalBackendSession owns one unpublished or published PTY process.
type TerminalBackendSession interface {
	MOTD() string
	PID() int
	StartSend(context.Context, TerminalSendRequest) (TerminalSendOperation, error)
	Read(TerminalReadRequest) (TerminalReadResult, error)
	Signal(string) (TerminalSignalResult, error)
	Status() TerminalSessionStatus
	Close(reason string) error
}

// TerminalBackend creates sessions for one stable backend type.
type TerminalBackend interface {
	Type() string
	Spawn(context.Context, TerminalBackendSpawnSpec) (TerminalBackendSession, error)
}

func initializeTerminalShell(ctx context.Context, session TerminalBackendSession, config TerminalConfig) (string, error) {
	startupCtx := ctx
	cancel := func() {}
	if config.ShellDialect == TerminalShellDialectPwsh {
		startupCtx, cancel = context.WithTimeout(ctx, config.Timeout)
	}
	defer cancel()
	first := true
	for {
		if err := startupCtx.Err(); err != nil {
			return "", err
		}
		request := TerminalSendRequest{}
		if first && config.ShellDialect == TerminalShellDialectPwsh {
			request.Text = terminalPowerShellEncodingPreamble + terminalPowerShellPrompt
			request.Submit = true
		}
		operation, err := session.StartSend(startupCtx, request)
		if err != nil {
			return "", err
		}
		select {
		case <-operation.Done():
		case <-startupCtx.Done():
			return "", startupCtx.Err()
		}
		result, err := operation.Result()
		if err != nil {
			return "", err
		}
		if result.WaitReason == TerminalWaitSessionExit {
			return "", errors.New("PTY shell exited during startup")
		}
		if result.WaitReason == TerminalWaitTimeout {
			return "", errors.New("PTY shell did not reach readiness before startup timeout")
		}
		if config.ShellDialect != TerminalShellDialectPwsh {
			return result.Viewport, nil
		}
		scrollback, err := session.Read(TerminalReadRequest{Count: 20})
		if err != nil {
			return "", err
		}
		if strings.Contains(result.Viewport, terminalPrompt) || strings.Contains(scrollback.Text, terminalPrompt) {
			return result.Viewport, nil
		}
		first = false
	}
}

type TerminalBackendCleanupError struct {
	SpawnError   error
	CleanupError error
}

func (e *TerminalBackendCleanupError) Error() string {
	return "PTY backend startup and cleanup both failed"
}

func (e *TerminalBackendCleanupError) Unwrap() []error {
	return []error{e.SpawnError, e.CleanupError}
}

type TerminalError struct {
	Code    string
	Message string
}

func (e *TerminalError) Error() string { return e.Code + ": " + e.Message }

type terminalRecord struct {
	id       string
	owner    string
	name     string
	typeID   string
	session  TerminalBackendSession
	active   *ownedTerminalOperation
	closing  chan struct{}
	closeErr error
}

type terminalRegistry struct {
	mu             sync.Mutex
	backends       map[string]TerminalBackend
	backendTokens  map[string]*struct{}
	backendOrder   []string
	sessions       map[string]*terminalRecord
	order          []string
	names          map[string]map[string]string
	pendingByOwner map[string]map[*terminalPendingSpawn]struct{}
	nextID         int
	closed         bool
	ctx            context.Context
	cancel         context.CancelCauseFunc
	pending        sync.WaitGroup
}

type terminalPendingSpawn struct {
	cancel     context.CancelCauseFunc
	done       chan struct{}
	cleanupErr error
}

func newTerminalRegistry() *terminalRegistry {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &terminalRegistry{
		backends: map[string]TerminalBackend{}, backendTokens: map[string]*struct{}{},
		sessions: map[string]*terminalRecord{}, names: map[string]map[string]string{},
		pendingByOwner: map[string]map[*terminalPendingSpawn]struct{}{}, ctx: ctx, cancel: cancel,
	}
}

func (r *terminalRegistry) register(backend TerminalBackend) (func(), error) {
	if backend == nil {
		return nil, errors.New("pty backend type must be non-empty")
	}
	typeID := strings.TrimSpace(backend.Type())
	if typeID == "" {
		return nil, errors.New("pty backend type must be non-empty")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, &TerminalError{Code: "SERVICE_DISPOSING", Message: "PTY service is disposing"}
	}
	if r.backends[typeID] != nil {
		r.mu.Unlock()
		return nil, &TerminalError{Code: "DUPLICATE_BACKEND", Message: fmt.Sprintf("a PTY backend named %q is already registered", typeID)}
	}
	token := &struct{}{}
	r.backends[typeID] = backend
	r.backendTokens[typeID] = token
	r.backendOrder = append(r.backendOrder, typeID)
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			if r.backendTokens[typeID] == token {
				delete(r.backends, typeID)
				delete(r.backendTokens, typeID)
				for index, name := range r.backendOrder {
					if name == typeID {
						r.backendOrder = append(r.backendOrder[:index], r.backendOrder[index+1:]...)
						break
					}
				}
			}
			r.mu.Unlock()
		})
	}, nil
}

func (r *terminalRegistry) listBackends() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	rows := make([]string, 0, len(r.backendOrder))
	for _, name := range r.backendOrder {
		if r.backends[name] != nil {
			rows = append(rows, name)
		}
	}
	return rows
}

func (r *terminalRegistry) open(ctx context.Context, spec TerminalBackendSpawnSpec) (TerminalSpawnResult, error) {
	if err := ctx.Err(); err != nil {
		return TerminalSpawnResult{}, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return TerminalSpawnResult{}, &TerminalError{Code: "SERVICE_DISPOSING", Message: "PTY service is disposing"}
	}
	backend := r.backends[spec.Type]
	if backend == nil {
		r.mu.Unlock()
		return TerminalSpawnResult{}, &TerminalError{Code: "NO_BACKEND", Message: fmt.Sprintf("no PTY backend registered for %q", spec.Type)}
	}
	if spec.Name != "" {
		ownerNames := r.names[spec.OwnerID]
		if ownerNames == nil {
			ownerNames = map[string]string{}
			r.names[spec.OwnerID] = ownerNames
		}
		if _, exists := ownerNames[spec.Name]; exists {
			r.mu.Unlock()
			return TerminalSpawnResult{}, &TerminalError{Code: "DUPLICATE_NAME", Message: fmt.Sprintf("PTY session name %q is already in use", spec.Name)}
		}
		ownerNames[spec.Name] = ""
	}
	spawnCtx, cancel := context.WithCancelCause(ctx)
	pending := &terminalPendingSpawn{cancel: cancel, done: make(chan struct{})}
	ownedPending := r.pendingByOwner[spec.OwnerID]
	if ownedPending == nil {
		ownedPending = map[*terminalPendingSpawn]struct{}{}
		r.pendingByOwner[spec.OwnerID] = ownedPending
	}
	ownedPending[pending] = struct{}{}
	r.nextID++
	spec.SessionID = fmt.Sprintf("pty-%d", r.nextID)
	r.pending.Add(1)
	r.mu.Unlock()

	stop := context.AfterFunc(r.ctx, func() { cancel(context.Cause(r.ctx)) })
	published := false
	defer func() {
		stop()
		if !published {
			cancel(nil)
		}
		r.mu.Lock()
		if pending.cleanupErr == nil {
			r.removePendingLocked(spec.OwnerID, pending)
		}
		r.mu.Unlock()
		close(pending.done)
		r.pending.Done()
	}()
	session, spawnErr := backend.Spawn(spawnCtx, spec)

	rollback := func(cause error) (TerminalSpawnResult, error) {
		r.releaseName(spec.OwnerID, spec.Name)
		if cancellation := context.Cause(spawnCtx); cancellation != nil {
			cause = cancellation
		}
		if session == nil {
			return TerminalSpawnResult{}, cause
		}
		if closeErr := session.Close("PTY spawn rolled back"); closeErr != nil {
			pending.cleanupErr = closeErr
			if context.Cause(spawnCtx) != nil {
				return TerminalSpawnResult{}, cause
			}
			return TerminalSpawnResult{}, errors.Join(cause, closeErr)
		}
		return TerminalSpawnResult{}, cause
	}
	if spawnErr != nil {
		var cleanup *TerminalBackendCleanupError
		if errors.As(spawnErr, &cleanup) {
			pending.cleanupErr = cleanup.CleanupError
		}
		cause := spawnErr
		if cancellation := context.Cause(spawnCtx); cancellation != nil {
			cause = cancellation
		}
		return rollback(cause)
	}
	if session == nil {
		return rollback(errors.New("PTY backend returned a nil session"))
	}
	if cause := context.Cause(spawnCtx); cause != nil {
		return rollback(cause)
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return rollback(&TerminalError{Code: "SERVICE_DISPOSING", Message: "PTY service is disposing"})
	}
	record := &terminalRecord{id: spec.SessionID, owner: spec.OwnerID, name: spec.Name, typeID: spec.Type, session: session}
	r.sessions[record.id] = record
	r.order = append(r.order, record.id)
	if spec.Name != "" {
		r.names[spec.OwnerID][spec.Name] = record.id
	}
	snapshot := snapshotTerminal(record)
	r.mu.Unlock()
	published = true
	return TerminalSpawnResult{TerminalSessionSnapshot: snapshot, MOTD: session.MOTD()}, nil
}

func (r *terminalRegistry) removePendingLocked(owner string, pending *terminalPendingSpawn) {
	owned := r.pendingByOwner[owner]
	delete(owned, pending)
	if len(owned) == 0 {
		delete(r.pendingByOwner, owner)
	}
}

func (r *terminalRegistry) releaseName(owner, name string) {
	if name == "" {
		return
	}
	r.mu.Lock()
	if ownerNames := r.names[owner]; ownerNames != nil {
		delete(ownerNames, name)
		if len(ownerNames) == 0 {
			delete(r.names, owner)
		}
	}
	r.mu.Unlock()
}

func (r *terminalRegistry) expectOwnedLocked(owner, id string) (*terminalRecord, error) {
	record := r.sessions[id]
	if record == nil {
		return nil, &TerminalError{Code: "NO_SESSION", Message: fmt.Sprintf("unknown PTY session %s", id)}
	}
	if record.owner != owner {
		return nil, &TerminalError{Code: "FOREIGN_SESSION", Message: fmt.Sprintf("PTY session %s belongs to another agent", id)}
	}
	return record, nil
}

type ownedTerminalOperation struct {
	inner  TerminalSendOperation
	done   chan struct{}
	mu     sync.Mutex
	result TerminalSendResult
	err    error
}

func (operation *ownedTerminalOperation) Done() <-chan struct{} { return operation.done }

func (operation *ownedTerminalOperation) Result() (TerminalSendResult, error) {
	select {
	case <-operation.done:
		operation.mu.Lock()
		defer operation.mu.Unlock()
		return operation.result, operation.err
	default:
		return TerminalSendResult{}, errors.New("PTY send is still active")
	}
}

func (operation *ownedTerminalOperation) ReadOutput() TerminalSendRead {
	return operation.inner.ReadOutput()
}

func (operation *ownedTerminalOperation) Cancel() bool { return operation.inner.Cancel() }

func (r *terminalRegistry) startSend(ctx context.Context, owner, id string, request TerminalSendRequest) (TerminalSendOperation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	record, err := r.expectOwnedLocked(owner, id)
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	if record.closing != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("PTY session %s is closing", id)
	}
	if record.active != nil {
		r.mu.Unlock()
		return nil, &TerminalError{Code: "SEND_ACTIVE", Message: fmt.Sprintf("PTY session %s already has an active send", id)}
	}
	inner, err := record.session.StartSend(ctx, request)
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	operation := &ownedTerminalOperation{inner: inner, done: make(chan struct{})}
	record.active = operation
	r.mu.Unlock()
	go func() {
		<-inner.Done()
		result, resultErr := inner.Result()
		operation.mu.Lock()
		operation.result, operation.err = result, resultErr
		operation.mu.Unlock()
		r.mu.Lock()
		if record.active == operation {
			record.active = nil
		}
		r.mu.Unlock()
		close(operation.done)
	}()
	return operation, nil
}

func (r *terminalRegistry) read(owner, id string, request TerminalReadRequest) (TerminalReadResult, error) {
	r.mu.Lock()
	record, err := r.expectOwnedLocked(owner, id)
	r.mu.Unlock()
	if err != nil {
		return TerminalReadResult{}, err
	}
	return record.session.Read(request)
}

func (r *terminalRegistry) signal(owner, id, signal string) (TerminalSignalResult, error) {
	r.mu.Lock()
	record, err := r.expectOwnedLocked(owner, id)
	r.mu.Unlock()
	if err != nil {
		return TerminalSignalResult{}, err
	}
	return record.session.Signal(signal)
}

func (r *terminalRegistry) closeOne(owner, id, reason string) (bool, error) {
	r.mu.Lock()
	record, err := r.expectOwnedLocked(owner, id)
	if err != nil {
		r.mu.Unlock()
		return false, err
	}
	if record.closing != nil {
		done := record.closing
		r.mu.Unlock()
		<-done
		return false, record.closeErr
	}
	record.closing = make(chan struct{})
	done := record.closing
	r.mu.Unlock()

	closeErr := record.session.Close(reason)
	r.mu.Lock()
	record.closeErr = closeErr
	if closeErr == nil {
		delete(r.sessions, id)
		if record.name != "" {
			delete(r.names[owner], record.name)
			if len(r.names[owner]) == 0 {
				delete(r.names, owner)
			}
		}
	} else {
		record.closing = nil
	}
	close(done)
	r.mu.Unlock()
	return closeErr == nil, closeErr
}

func snapshotTerminal(record *terminalRecord) TerminalSessionSnapshot {
	return TerminalSessionSnapshot{
		SessionID: record.id, Name: record.name, Type: record.typeID,
		PID: record.session.PID(), Status: cloneTerminalStatus(record.session.Status()),
	}
}

func (r *terminalRegistry) list(owner string) []TerminalSessionSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	rows := make([]TerminalSessionSnapshot, 0)
	for _, id := range r.order {
		record := r.sessions[id]
		if record != nil && record.owner == owner {
			rows = append(rows, snapshotTerminal(record))
		}
	}
	return rows
}

func (r *terminalRegistry) hasOwnerActivity(owner string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pendingByOwner[owner]) > 0 {
		return true
	}
	for _, record := range r.sessions {
		if record.owner == owner {
			return true
		}
	}
	return false
}

func (r *terminalRegistry) closeOwner(owner string) error {
	r.mu.Lock()
	pending := make([]*terminalPendingSpawn, 0, len(r.pendingByOwner[owner]))
	for spawn := range r.pendingByOwner[owner] {
		pending = append(pending, spawn)
	}
	r.mu.Unlock()
	cause := &TerminalError{Code: "OWNER_NOT_LIVE", Message: "PTY owner is no longer live"}
	for _, spawn := range pending {
		spawn.cancel(cause)
	}
	var failures []error
	for _, spawn := range pending {
		<-spawn.done
		if spawn.cleanupErr != nil {
			failures = append(failures, spawn.cleanupErr)
		}
	}
	r.mu.Lock()
	for _, spawn := range pending {
		r.removePendingLocked(owner, spawn)
	}
	r.mu.Unlock()

	r.mu.Lock()
	ids := make([]string, 0)
	for _, id := range r.order {
		if record := r.sessions[id]; record != nil && record.owner == owner {
			ids = append(ids, id)
		}
	}
	r.mu.Unlock()
	for _, id := range ids {
		if _, err := r.closeOne(owner, id, "PTY owner disposed"); err != nil {
			if !isTerminalErrorCode(err, "NO_SESSION") {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

func (r *terminalRegistry) close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.cancel(&TerminalError{Code: "SERVICE_DISPOSING", Message: "PTY service is disposing"})
	r.mu.Unlock()
	r.pending.Wait()

	r.mu.Lock()
	var failures []error
	for owner, pending := range r.pendingByOwner {
		for spawn := range pending {
			if spawn.cleanupErr != nil {
				failures = append(failures, spawn.cleanupErr)
			}
			r.removePendingLocked(owner, spawn)
		}
	}
	records := make([]*terminalRecord, 0, len(r.sessions))
	for _, id := range r.order {
		if record := r.sessions[id]; record != nil {
			records = append(records, record)
		}
	}
	r.mu.Unlock()
	for _, record := range records {
		if _, err := r.closeOne(record.owner, record.id, "PTY service disposed"); err != nil && !isTerminalErrorCode(err, "NO_SESSION") {
			failures = append(failures, fmt.Errorf("terminal %s: %w", record.id, err))
		}
	}
	r.mu.Lock()
	r.sessions = map[string]*terminalRecord{}
	r.names = map[string]map[string]string{}
	r.backends = map[string]TerminalBackend{}
	r.backendTokens = map[string]*struct{}{}
	r.backendOrder = nil
	r.order = nil
	r.mu.Unlock()
	return errors.Join(failures...)
}

func (e *Engine) RegisterTerminalBackend(backend TerminalBackend) (func(), error) {
	return e.terminals.register(backend)
}

func (e *Engine) ListTerminalBackends() []string { return e.terminals.listBackends() }

func (e *Engine) terminalOwner(owner string) (string, string, error) {
	if owner == "" {
		return "", "", &TerminalError{Code: "OWNER_NOT_LIVE", Message: "PTY owner is required"}
	}
	session, err := e.getSession(owner)
	if err != nil {
		return "", "", &TerminalError{Code: "OWNER_NOT_LIVE", Message: fmt.Sprintf("agent %q is not a live PTY owner", owner)}
	}
	session.mu.Lock()
	workspace, attached := session.Header.CWD, session.attached
	session.mu.Unlock()
	if !attached {
		return "", "", &TerminalError{Code: "OWNER_NOT_LIVE", Message: fmt.Sprintf("agent %q is not a live PTY owner", owner)}
	}
	mode, err := e.sandboxModeForCall(ToolCall{SessionID: owner})
	return workspace, mode, err
}

func (e *Engine) OpenTerminal(ctx context.Context, owner string, request TerminalSpawnRequest) (TerminalSpawnResult, error) {
	if strings.TrimSpace(request.Type) == "" {
		return TerminalSpawnResult{}, errors.New("type must be a non-empty string")
	}
	if request.Name != "" && strings.TrimSpace(request.Name) == "" {
		return TerminalSpawnResult{}, errors.New("PTY session name must be non-empty")
	}
	workspace, mode, err := e.terminalOwner(owner)
	if err != nil {
		return TerminalSpawnResult{}, err
	}
	cwd := request.CWD
	if cwd == "" {
		cwd = workspace
	} else if !filepath.IsAbs(cwd) {
		cwd = filepath.Join(workspace, cwd)
	}
	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return TerminalSpawnResult{}, err
	}
	info, err := os.Stat(cwd)
	if err != nil || !info.IsDir() {
		return TerminalSpawnResult{}, fmt.Errorf("terminal-bash: invalid cwd %q", cwd)
	}
	request.Type = strings.TrimSpace(request.Type)
	request.CWD = cwd
	return e.terminals.open(ctx, TerminalBackendSpawnSpec{
		TerminalSpawnRequest: request, OwnerID: owner, Workspace: workspace, SandboxMode: mode,
	})
}

func (e *Engine) StartTerminalSend(ctx context.Context, owner, sessionID string, request TerminalSendRequest) (TerminalSendOperation, error) {
	if _, _, err := e.terminalOwner(owner); err != nil {
		return nil, err
	}
	if sessionID == "" {
		return nil, errors.New("sessionId must be a non-empty string")
	}
	return e.terminals.startSend(ctx, owner, sessionID, request)
}

func (e *Engine) SendTerminal(ctx context.Context, owner, sessionID string, request TerminalSendRequest) (TerminalSendResult, error) {
	operation, err := e.StartTerminalSend(ctx, owner, sessionID, request)
	if err != nil {
		return TerminalSendResult{}, err
	}
	select {
	case <-operation.Done():
	case <-ctx.Done():
		operation.Cancel()
		<-operation.Done()
	}
	result, resultErr := operation.Result()
	if err := ctx.Err(); err != nil {
		return TerminalSendResult{}, err
	}
	return result, resultErr
}

func (e *Engine) ReadTerminal(owner, sessionID string, request TerminalReadRequest) (TerminalReadResult, error) {
	if _, _, err := e.terminalOwner(owner); err != nil {
		return TerminalReadResult{}, err
	}
	if sessionID == "" {
		return TerminalReadResult{}, errors.New("sessionId must be a non-empty string")
	}
	return e.terminals.read(owner, sessionID, request)
}

func (e *Engine) SignalTerminal(owner, sessionID, signal string) (TerminalSignalResult, error) {
	if _, _, err := e.terminalOwner(owner); err != nil {
		return TerminalSignalResult{}, err
	}
	if sessionID == "" {
		return TerminalSignalResult{}, errors.New("sessionId must be a non-empty string")
	}
	if !validTerminalSignal(signal) {
		return TerminalSignalResult{}, fmt.Errorf("unsupported terminal signal %q", signal)
	}
	return e.terminals.signal(owner, sessionID, signal)
}

func (e *Engine) CloseTerminal(owner, sessionID string) (bool, error) {
	if _, _, err := e.terminalOwner(owner); err != nil {
		return false, err
	}
	if sessionID == "" {
		return false, errors.New("sessionId must be a non-empty string")
	}
	return e.terminals.closeOne(owner, sessionID, "model request")
}

func (e *Engine) ListTerminals(owner string) ([]TerminalSessionSnapshot, error) {
	if _, _, err := e.terminalOwner(owner); err != nil {
		return nil, err
	}
	return e.terminals.list(owner), nil
}

func (e *Engine) HasTerminalActivity(owner string) bool {
	return e.terminals.hasOwnerActivity(owner) || e.shells.hasOwnerActivity(owner)
}

func validTerminalSignal(signal string) bool {
	switch signal {
	case TerminalSignalInterrupt, TerminalSignalTerminate, TerminalSignalKill, TerminalSignalStop, TerminalSignalHangup:
		return true
	default:
		return false
	}
}

func isTerminalErrorCode(err error, code string) bool {
	var terminalErr *TerminalError
	return errors.As(err, &terminalErr) && terminalErr.Code == code
}
