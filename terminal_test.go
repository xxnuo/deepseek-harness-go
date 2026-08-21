package harness

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type testTerminalBackendState struct {
	mu       sync.Mutex
	typeID   string
	specs    []TerminalBackendSpawnSpec
	sessions []*testTerminalSession
	spawn    func(context.Context, TerminalBackendSpawnSpec) (TerminalBackendSession, error)
}

// A slice-backed implementation exercises registration of incomparable dynamic types.
type testTerminalBackend []any

func (backend testTerminalBackend) state() *testTerminalBackendState {
	return backend[0].(*testTerminalBackendState)
}

func (backend testTerminalBackend) Type() string { return backend.state().typeID }

func (backend testTerminalBackend) Spawn(ctx context.Context, spec TerminalBackendSpawnSpec) (TerminalBackendSession, error) {
	state := backend.state()
	state.mu.Lock()
	state.specs = append(state.specs, spec)
	spawn := state.spawn
	state.mu.Unlock()
	if spawn != nil {
		return spawn(ctx, spec)
	}
	session := newTestTerminalSession()
	state.mu.Lock()
	state.sessions = append(state.sessions, session)
	state.mu.Unlock()
	return session, nil
}

type testTerminalSession struct {
	mu           sync.Mutex
	started      chan *testTerminalOperation
	closeReasons []string
	status       TerminalSessionStatus
}

func newTestTerminalSession() *testTerminalSession {
	return &testTerminalSession{started: make(chan *testTerminalOperation, 8), status: runningTerminalStatus()}
}

func (session *testTerminalSession) MOTD() string { return "stub ready" }
func (session *testTerminalSession) PID() int     { return 123 }

func (session *testTerminalSession) StartSend(_ context.Context, _ TerminalSendRequest) (TerminalSendOperation, error) {
	operation := newTestTerminalOperation()
	session.started <- operation
	return operation, nil
}

func (session *testTerminalSession) Read(request TerminalReadRequest) (TerminalReadResult, error) {
	return TerminalReadResult{Text: "history", TotalLines: 1, LineBegin: request.Offset, LineEnd: request.Offset + 1}, nil
}

func (session *testTerminalSession) Signal(string) (TerminalSignalResult, error) {
	return TerminalSignalResult{Delivered: true, TargetPGID: 321}, nil
}

func (session *testTerminalSession) Status() TerminalSessionStatus {
	session.mu.Lock()
	defer session.mu.Unlock()
	return cloneTerminalStatus(session.status)
}

func (session *testTerminalSession) Close(reason string) error {
	session.mu.Lock()
	session.closeReasons = append(session.closeReasons, reason)
	status := TerminalSessionStatus{Kind: "exited"}
	exitCode := 0
	status.ExitCode = &exitCode
	session.status = status
	session.mu.Unlock()
	for {
		select {
		case operation := <-session.started:
			operation.finish(TerminalSendResult{WaitReason: TerminalWaitSessionExit, SessionStatus: status}, nil)
		default:
			return nil
		}
	}
}

type testTerminalOperation struct {
	mu      sync.Mutex
	done    chan struct{}
	result  TerminalSendResult
	err     error
	output  string
	settled bool
}

func newTestTerminalOperation() *testTerminalOperation {
	return &testTerminalOperation{done: make(chan struct{})}
}

func (operation *testTerminalOperation) Done() <-chan struct{} { return operation.done }

func (operation *testTerminalOperation) Result() (TerminalSendResult, error) {
	select {
	case <-operation.done:
		operation.mu.Lock()
		defer operation.mu.Unlock()
		return operation.result, operation.err
	default:
		return TerminalSendResult{}, errors.New("still running")
	}
}

func (operation *testTerminalOperation) ReadOutput() TerminalSendRead {
	operation.mu.Lock()
	defer operation.mu.Unlock()
	result := TerminalSendRead{Delta: operation.output}
	operation.output = ""
	return result
}

func (operation *testTerminalOperation) Cancel() bool {
	operation.mu.Lock()
	if operation.settled {
		operation.mu.Unlock()
		return false
	}
	operation.mu.Unlock()
	operation.finish(TerminalSendResult{WaitReason: TerminalWaitStdinRead, SessionStatus: runningTerminalStatus()}, nil)
	return true
}

func (operation *testTerminalOperation) append(text string) {
	operation.mu.Lock()
	operation.output += text
	operation.mu.Unlock()
}

func (operation *testTerminalOperation) finish(result TerminalSendResult, err error) {
	operation.mu.Lock()
	if operation.settled {
		operation.mu.Unlock()
		return
	}
	operation.settled, operation.result, operation.err = true, result, err
	close(operation.done)
	operation.mu.Unlock()
}

func newTerminalTestEngine(t *testing.T, toolConfig TerminalToolConfig) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Persist = false
	cfg.Terminal.Disabled = true
	cfg.TerminalTool = toolConfig
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func terminalTestOwner(t *testing.T, e *Engine, id string) string {
	t.Helper()
	owner, err := e.CreateSession(context.Background(), e.Config().Workspace, id, "")
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func terminalErrorCode(err error) string {
	var terminalErr *TerminalError
	if errors.As(err, &terminalErr) {
		return terminalErr.Code
	}
	return ""
}

func TestTerminalRegistryOwnershipAndLifecycle(t *testing.T) {
	e := newTerminalTestEngine(t, TerminalToolConfig{})
	state := &testTerminalBackendState{typeID: "stub"}
	dispose, err := e.RegisterTerminalBackend(testTerminalBackend{state})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.ListTerminalBackends(); len(got) != 1 || got[0] != "stub" {
		t.Fatalf("backends = %#v", got)
	}
	if _, err := e.RegisterTerminalBackend(testTerminalBackend{&testTerminalBackendState{typeID: "stub"}}); terminalErrorCode(err) != "DUPLICATE_BACKEND" {
		t.Fatalf("duplicate backend error = %v", err)
	}

	owner := terminalTestOwner(t, e, "terminal-owner")
	foreign := terminalTestOwner(t, e, "terminal-foreign")
	created, err := e.OpenTerminal(context.Background(), owner, TerminalSpawnRequest{Type: "stub", Name: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if created.SessionID != "pty-1" || created.Name != "main" || created.MOTD != "stub ready" {
		t.Fatalf("created = %#v", created)
	}
	if _, err := e.OpenTerminal(context.Background(), owner, TerminalSpawnRequest{Type: "stub", Name: "main"}); terminalErrorCode(err) != "DUPLICATE_NAME" {
		t.Fatalf("duplicate name error = %v", err)
	}
	if _, err := e.OpenTerminal(context.Background(), foreign, TerminalSpawnRequest{Type: "stub", Name: "main"}); err != nil {
		t.Fatalf("owner-local name rejected: %v", err)
	}
	if sessions, err := e.ListTerminals(owner); err != nil || len(sessions) != 1 || sessions[0].SessionID != created.SessionID {
		t.Fatalf("owner sessions = %#v, err=%v", sessions, err)
	}
	if _, err := e.ReadTerminal(foreign, created.SessionID, TerminalReadRequest{}); terminalErrorCode(err) != "FOREIGN_SESSION" {
		t.Fatalf("foreign read error = %v", err)
	}

	operation, err := e.StartTerminalSend(context.Background(), owner, created.SessionID, TerminalSendRequest{Text: "work", Submit: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.StartTerminalSend(context.Background(), owner, created.SessionID, TerminalSendRequest{}); terminalErrorCode(err) != "SEND_ACTIVE" {
		t.Fatalf("concurrent send error = %v", err)
	}
	state.mu.Lock()
	session := state.sessions[0]
	state.mu.Unlock()
	started := waitTerminalOperation(t, session.started)
	started.finish(TerminalSendResult{Viewport: "done", WaitReason: TerminalWaitStdinRead, SessionStatus: runningTerminalStatus()}, nil)
	<-operation.Done()
	if result, err := operation.Result(); err != nil || result.Viewport != "done" {
		t.Fatalf("send result = %#v, err=%v", result, err)
	}

	closed, err := e.CloseTerminal(owner, created.SessionID)
	if err != nil || !closed {
		t.Fatalf("close = %v, err=%v", closed, err)
	}
	if sessions, err := e.ListTerminals(owner); err != nil || len(sessions) != 0 {
		t.Fatalf("sessions after close = %#v, err=%v", sessions, err)
	}
	dispose()
	if got := e.ListTerminalBackends(); len(got) != 0 {
		t.Fatalf("backends after dispose = %#v", got)
	}
}

func TestTerminalPendingOwnerCleanupAndPermissionFence(t *testing.T) {
	e := newTerminalTestEngine(t, TerminalToolConfig{})
	started := make(chan struct{})
	state := &testTerminalBackendState{typeID: "slow"}
	state.spawn = func(ctx context.Context, _ TerminalBackendSpawnSpec) (TerminalBackendSession, error) {
		close(started)
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	if _, err := e.RegisterTerminalBackend(testTerminalBackend{state}); err != nil {
		t.Fatal(err)
	}
	owner := terminalTestOwner(t, e, "pending-owner")
	openDone := make(chan error, 1)
	go func() {
		_, err := e.OpenTerminal(context.Background(), owner, TerminalSpawnRequest{Type: "slow", Name: "main"})
		openDone <- err
	}()
	<-started
	if !e.HasTerminalActivity(owner) {
		t.Fatal("pending spawn was not reported as terminal activity")
	}
	if _, err := e.OpenTerminal(context.Background(), owner, TerminalSpawnRequest{Type: "slow", Name: "main"}); terminalErrorCode(err) != "DUPLICATE_NAME" {
		t.Fatalf("pending duplicate name error = %v", err)
	}
	session, err := e.getSession(owner)
	if err != nil {
		t.Fatal(err)
	}
	result, err := e.runCommand(session, "/permission danger-full-access")
	if err != nil || result.Command == nil || result.Command.Kind != "error" || !strings.Contains(result.Command.Text, "open or being created") {
		t.Fatalf("permission fence result = %#v, err=%v", result, err)
	}
	session.mu.Lock()
	for _, event := range session.Events {
		if event.Type == "permission/preset" || event.Type == "sandbox/mode" || event.Type == "approval/policy" {
			session.mu.Unlock()
			t.Fatalf("permission fence wrote partial state: %#v", event)
		}
	}
	session.mu.Unlock()
	if err := detachSDKSession(e, owner); err != nil {
		t.Fatal(err)
	}
	if err := <-openDone; terminalErrorCode(err) != "OWNER_NOT_LIVE" {
		t.Fatalf("pending spawn error = %v", err)
	}
	if e.HasTerminalActivity(owner) {
		t.Fatal("terminal activity survived owner detach")
	}
}

func TestTerminalToolSchemasAndConfig(t *testing.T) {
	e := newTerminalTestEngine(t, TerminalToolConfig{})
	schemas := map[string]ToolSchema{}
	for _, schema := range e.ListTools() {
		schemas[schema.Name] = schema
	}
	for _, name := range []string{"terminal_open", "terminal_send", "terminal_read", "terminal_signal", "terminal_close", "terminal_list"} {
		if _, ok := schemas[name]; !ok {
			t.Fatalf("missing %s schema", name)
		}
	}
	properties := schemas["terminal_send"].Parameters["properties"].(map[string]any)
	if _, ok := properties["run_in_background"]; !ok {
		t.Fatal("terminal_send schema is missing run_in_background")
	}

	disabled := false
	e = newTerminalTestEngine(t, TerminalToolConfig{EnableRunInBackground: &disabled})
	for _, schema := range e.ListTools() {
		if schema.Name == "terminal_send" {
			properties = schema.Parameters["properties"].(map[string]any)
		}
	}
	if _, ok := properties["run_in_background"]; ok {
		t.Fatal("disabled background property remains visible")
	}

	if _, err := New(WithPersistence(false), WithWorkspace(t.TempDir()), WithTerminalTool(TerminalToolConfig{MaxResultBytes: 63})); err == nil || !strings.Contains(err.Error(), "at least 64") {
		t.Fatalf("invalid tool config error = %v", err)
	}
	if _, err := New(WithPersistence(false), WithWorkspace(t.TempDir()), WithTerminal(TerminalConfig{Rows: -1})); err == nil || !strings.Contains(err.Error(), "rows") {
		t.Fatalf("invalid backend config error = %v", err)
	}
}

func TestTerminalBackgroundSendUsesJobTools(t *testing.T) {
	e := newTerminalTestEngine(t, TerminalToolConfig{})
	state := &testTerminalBackendState{typeID: "stub"}
	if _, err := e.RegisterTerminalBackend(testTerminalBackend{state}); err != nil {
		t.Fatal(err)
	}
	owner := terminalTestOwner(t, e, "background-owner")
	foreign := terminalTestOwner(t, e, "background-foreign")
	opened, err := callBuiltin(t, e, owner, "terminal_open", map[string]any{"type": "stub"})
	if err != nil {
		t.Fatal(err)
	}
	created := opened.Value.(TerminalSpawnResult)
	started, err := callBuiltin(t, e, owner, "terminal_send", map[string]any{
		"sessionId": created.SessionID, "text": "work", "run_in_background": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobID := started.Value.(map[string]any)["jobId"].(string)
	state.mu.Lock()
	session := state.sessions[0]
	state.mu.Unlock()
	operation := waitTerminalOperation(t, session.started)
	operation.append("first")
	output, err := callBuiltin(t, e, owner, "job_output", map[string]any{"job_id": jobID})
	if err != nil || !strings.Contains(resultText(output), "first") || !strings.Contains(resultText(output), "[status: running]") {
		t.Fatalf("incremental output = %q, err=%v", resultText(output), err)
	}
	if _, err := callBuiltin(t, e, foreign, "job_output", map[string]any{"job_id": jobID}); err == nil || !strings.Contains(err.Error(), "another session") {
		t.Fatalf("foreign job output error = %v", err)
	}
	operation.append("second")
	operation.finish(TerminalSendResult{WaitReason: TerminalWaitStdinRead, SessionStatus: runningTerminalStatus()}, nil)
	output, err = callBuiltin(t, e, owner, "job_output", map[string]any{"job_id": jobID, "wait": true, "timeout_ms": 1000})
	if err != nil || !strings.Contains(resultText(output), "second") || !strings.Contains(resultText(output), "[status: completed, wait: stdin_read]") {
		t.Fatalf("final output = %q, err=%v", resultText(output), err)
	}

	started, err = callBuiltin(t, e, owner, "terminal_send", map[string]any{
		"sessionId": created.SessionID, "text": "stop", "run_in_background": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobID = started.Value.(map[string]any)["jobId"].(string)
	waitTerminalOperation(t, session.started)
	if _, err := callBuiltin(t, e, owner, "job_kill", map[string]any{"job_id": jobID}); err != nil {
		t.Fatal(err)
	}
	output, err = callBuiltin(t, e, owner, "job_output", map[string]any{"job_id": jobID, "wait": true, "timeout_ms": 1000})
	if err != nil || !strings.Contains(resultText(output), "[status: killed") {
		t.Fatalf("killed output = %q, err=%v", resultText(output), err)
	}
}

func TestTerminalCloseOwnerClosesPublishedSessions(t *testing.T) {
	e := newTerminalTestEngine(t, TerminalToolConfig{})
	state := &testTerminalBackendState{typeID: "stub"}
	if _, err := e.RegisterTerminalBackend(testTerminalBackend{state}); err != nil {
		t.Fatal(err)
	}
	owner := terminalTestOwner(t, e, "detached-owner")
	if _, err := e.OpenTerminal(context.Background(), owner, TerminalSpawnRequest{Type: "stub"}); err != nil {
		t.Fatal(err)
	}
	if err := detachSDKSession(e, owner); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	session := state.sessions[0]
	state.mu.Unlock()
	session.mu.Lock()
	reasons := append([]string(nil), session.closeReasons...)
	session.mu.Unlock()
	if len(reasons) != 1 || reasons[0] != "PTY owner disposed" {
		t.Fatalf("close reasons = %#v", reasons)
	}
}

type startupTerminalSession struct {
	requests []TerminalSendRequest
}

func (session *startupTerminalSession) MOTD() string { return "" }
func (session *startupTerminalSession) PID() int     { return 1 }
func (session *startupTerminalSession) StartSend(_ context.Context, request TerminalSendRequest) (TerminalSendOperation, error) {
	session.requests = append(session.requests, request)
	operation := newTestTerminalOperation()
	viewport := "PowerShell banner"
	if len(session.requests) > 1 {
		viewport = terminalPrompt
	}
	operation.finish(TerminalSendResult{Viewport: viewport, WaitReason: TerminalWaitInferredIdle, SessionStatus: runningTerminalStatus()}, nil)
	return operation, nil
}
func (session *startupTerminalSession) Read(TerminalReadRequest) (TerminalReadResult, error) {
	text := "PowerShell banner"
	if len(session.requests) > 1 {
		text += "\n" + terminalPrompt
	}
	return TerminalReadResult{Text: text}, nil
}
func (session *startupTerminalSession) Signal(string) (TerminalSignalResult, error) {
	return TerminalSignalResult{}, nil
}
func (session *startupTerminalSession) Status() TerminalSessionStatus { return runningTerminalStatus() }
func (session *startupTerminalSession) Close(string) error            { return nil }

func TestTerminalPwshDialectDefaultsAndStartup(t *testing.T) {
	config := normalizeTerminalConfig(TerminalConfig{ShellDialect: TerminalShellDialectPwsh})
	if config.ShellPath == "" || len(config.ShellArgs) != 2 || config.ShellArgs[0] != "-NoLogo" || config.ShellArgs[1] != "-NoProfile" {
		t.Fatalf("pwsh defaults = path %q args %#v", config.ShellPath, config.ShellArgs)
	}
	if err := validateTerminalConfig(config); err != nil {
		t.Fatal(err)
	}
	invalid := config
	invalid.ShellDialect = "fish"
	if err := validateTerminalConfig(invalid); err == nil || !strings.Contains(err.Error(), "shellDialect") {
		t.Fatalf("invalid dialect error = %v", err)
	}

	session := &startupTerminalSession{}
	motd, err := initializeTerminalShell(context.Background(), session, config)
	if err != nil {
		t.Fatal(err)
	}
	if motd != terminalPrompt || len(session.requests) != 2 {
		t.Fatalf("pwsh startup = motd %q requests %#v", motd, session.requests)
	}
	first := session.requests[0]
	if !first.Submit || !strings.Contains(first.Text, terminalPowerShellEncodingPreamble) || !strings.Contains(first.Text, "function prompt") {
		t.Fatalf("pwsh bootstrap = %#v", first)
	}
	if second := session.requests[1]; second.Text != "" || second.Submit {
		t.Fatalf("pwsh readiness poll = %#v", second)
	}
}

func waitTerminalOperation(t *testing.T, channel <-chan *testTerminalOperation) *testTerminalOperation {
	t.Helper()
	select {
	case operation := <-channel:
		return operation
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for terminal operation")
		return nil
	}
}
