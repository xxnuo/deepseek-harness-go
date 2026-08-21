//go:build windows

package harness

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	terminalPromptMarker = "133;D;"
	terminalPrompt       = "dsh> "
)

func platformTerminalShellDefaults() (string, string, []string) {
	path, args := terminalShellDefaults(TerminalShellDialectPwsh)
	return TerminalShellDialectPwsh, path, args
}

func terminalShellDefaults(dialect string) (string, []string) {
	if dialect == TerminalShellDialectBash {
		return "/bin/bash", []string{"--noprofile", "--norc", "-i"}
	}
	return "pwsh.exe", []string{"-NoLogo", "-NoProfile"}
}

type powershellTerminalBackend struct{ config TerminalConfig }

func newPlatformBashTerminalBackend(config TerminalConfig) TerminalBackend {
	return &powershellTerminalBackend{config: config}
}

func (backend *powershellTerminalBackend) Type() string { return backend.config.BackendType }

func (backend *powershellTerminalBackend) Spawn(ctx context.Context, spec TerminalBackendSpawnSpec) (TerminalBackendSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	program := backend.config.ShellPath
	if backend.config.ShellDialect == TerminalShellDialectPwsh && strings.EqualFold(program, "pwsh.exe") {
		resolved, err := resolvePowerShell()
		if err != nil {
			return nil, err
		}
		program = resolved
	}
	var sandbox *windowsSandbox
	var err error
	if spec.SandboxMode != sandboxDangerFull {
		sandbox, err = newWindowsSandbox(spec.SandboxMode, spec.Workspace)
		if err != nil {
			return nil, err
		}
	}
	env := scrubbedChildEnv(terminalShellEnvironment(backend.config, spec))
	if sandbox != nil {
		env = sandbox.environment(env)
	}
	pty, err := startWindowsConPTY(program, backend.config.ShellArgs, spec.CWD, env, backend.config.Rows, backend.config.Cols, sandbox)
	if err != nil {
		if sandbox != nil {
			_ = sandbox.close()
		}
		return nil, err
	}
	session := newWindowsPTYSession(pty, backend.config)
	if err := ctx.Err(); err != nil {
		if closeErr := session.Close("PTY startup cancelled"); closeErr != nil {
			return nil, &TerminalBackendCleanupError{SpawnError: err, CleanupError: closeErr}
		}
		return nil, err
	}
	if err := session.initialize(ctx); err != nil {
		if closeErr := session.Close("PTY startup failed"); closeErr != nil {
			return nil, &TerminalBackendCleanupError{SpawnError: err, CleanupError: closeErr}
		}
		return nil, err
	}
	return session, nil
}

type terminalTextBuffer struct {
	mu       sync.Mutex
	value    string
	dropped  bool
	maxBytes int
	maxLines int
}

func newTerminalTextBuffer(maxBytes, maxLines int) *terminalTextBuffer {
	return &terminalTextBuffer{maxBytes: maxBytes, maxLines: maxLines}
}

func (buffer *terminalTextBuffer) append(text string) {
	if text == "" {
		return
	}
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	buffer.value += text
	if buffer.maxLines > 0 {
		lines := strings.Split(buffer.value, "\n")
		if len(lines) > buffer.maxLines {
			buffer.value = strings.Join(lines[len(lines)-buffer.maxLines:], "\n")
			buffer.dropped = true
		}
	}
	if tail, truncated := terminalUTF8Tail(buffer.value, buffer.maxBytes); truncated {
		buffer.value, buffer.dropped = tail, true
	}
}

func (buffer *terminalTextBuffer) snapshot() (string, bool) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.value, buffer.dropped
}

func (buffer *terminalTextBuffer) consume() TerminalSendRead {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	result := TerminalSendRead{Delta: buffer.value, Truncated: buffer.dropped}
	buffer.value, buffer.dropped = "", false
	return result
}

func terminalUTF8Tail(text string, maxBytes int) (string, bool) {
	if maxBytes <= 0 || len(text) <= maxBytes {
		return text, false
	}
	runes := []rune(text)
	bytes := 0
	start := len(runes)
	for start > 0 {
		next := utf8.RuneLen(runes[start-1])
		if next < 0 {
			next = len(string(utf8.RuneError))
		}
		if bytes+next > maxBytes {
			break
		}
		bytes += next
		start--
	}
	return string(runes[start:]), true
}

type terminalUTF8Decoder struct{ pending []byte }

func (decoder *terminalUTF8Decoder) push(chunk []byte) string {
	data := append(append([]byte(nil), decoder.pending...), chunk...)
	decoder.pending = nil
	var output strings.Builder
	for len(data) > 0 {
		if !utf8.FullRune(data) {
			decoder.pending = append([]byte(nil), data...)
			break
		}
		r, size := utf8.DecodeRune(data)
		output.WriteRune(r)
		data = data[size:]
	}
	return output.String()
}

func (decoder *terminalUTF8Decoder) flush() string {
	text := strings.ToValidUTF8(string(decoder.pending), string(utf8.RuneError))
	decoder.pending = nil
	return text
}

type terminalSanitizer struct {
	pending                string
	discardMode            byte
	discardOSCEscape       bool
	trailingCarriageReturn bool
	trackingPromptTail     bool
	maxPendingBytes        int
}

type sanitizedTerminalChunk struct {
	text       string
	prompt     bool
	promptTail string
	hasTail    bool
}

func (sanitizer *terminalSanitizer) push(chunk string) sanitizedTerminalChunk {
	sanitizer.pending += sanitizer.discardPrefix(chunk)
	var text, promptTail strings.Builder
	prompt := false
	hasTail := sanitizer.trackingPromptTail
	index := 0
	appendText := func(value string) {
		text.WriteString(value)
		if sanitizer.trackingPromptTail {
			promptTail.WriteString(value)
		}
	}
	for index < len(sanitizer.pending) {
		escape := strings.IndexByte(sanitizer.pending[index:], '\x1b')
		if escape < 0 {
			appendText(sanitizer.pending[index:])
			index = len(sanitizer.pending)
			break
		}
		escape += index
		appendText(sanitizer.pending[index:escape])
		if escape+1 >= len(sanitizer.pending) {
			index = escape
			break
		}
		switch sanitizer.pending[escape+1] {
		case ']':
			bel := strings.IndexByte(sanitizer.pending[escape+2:], '\a')
			if bel >= 0 {
				bel += escape + 2
			}
			st := strings.Index(sanitizer.pending[escape+2:], "\x1b\\")
			if st >= 0 {
				st += escape + 2
			}
			end, terminator := -1, 0
			switch {
			case bel >= 0 && (st < 0 || bel < st):
				end, terminator = bel+1, 1
			case st >= 0:
				end, terminator = st+2, 2
			}
			if end < 0 {
				index = escape
				break
			}
			content := sanitizer.pending[escape+2 : end-terminator]
			if strings.HasPrefix(content, terminalPromptMarker) {
				prompt = true
				sanitizer.trackingPromptTail = true
				hasTail = true
				promptTail.Reset()
			}
			index = end
		case '[':
			end := escape + 2
			for end < len(sanitizer.pending) && !(sanitizer.pending[end] >= 0x40 && sanitizer.pending[end] <= 0x7e) {
				end++
			}
			if end >= len(sanitizer.pending) {
				index = escape
				break
			}
			index = end + 1
		default:
			index = escape + 2
		}
		if index == escape {
			break
		}
	}
	sanitizer.pending = sanitizer.pending[index:]
	if len(sanitizer.pending) > sanitizer.maxPendingBytes {
		if len(sanitizer.pending) > 1 && sanitizer.pending[1] == ']' {
			sanitizer.discardMode = ']'
		} else {
			sanitizer.discardMode = '['
		}
		sanitizer.pending = ""
	}
	return sanitizedTerminalChunk{text: sanitizer.normalize(text.String()), prompt: prompt, promptTail: promptTail.String(), hasTail: hasTail}
}

func (sanitizer *terminalSanitizer) discardPrefix(chunk string) string {
	if sanitizer.discardMode == 0 {
		return chunk
	}
	if sanitizer.discardMode == '[' {
		for index := 0; index < len(chunk); index++ {
			if chunk[index] >= 0x40 && chunk[index] <= 0x7e {
				sanitizer.discardMode = 0
				return chunk[index+1:]
			}
		}
		return ""
	}
	if sanitizer.discardOSCEscape {
		sanitizer.discardOSCEscape = false
		if strings.HasPrefix(chunk, "\\") {
			sanitizer.discardMode = 0
			return chunk[1:]
		}
	}
	for index := 0; index < len(chunk); index++ {
		if chunk[index] == '\a' {
			sanitizer.discardMode = 0
			return chunk[index+1:]
		}
		if chunk[index] == '\x1b' {
			if index+1 < len(chunk) && chunk[index+1] == '\\' {
				sanitizer.discardMode = 0
				return chunk[index+2:]
			}
			if index+1 == len(chunk) {
				sanitizer.discardOSCEscape = true
			}
		}
	}
	return ""
}

func (sanitizer *terminalSanitizer) normalize(text string) string {
	if sanitizer.trailingCarriageReturn {
		text = "\r" + text
		sanitizer.trailingCarriageReturn = false
	}
	if strings.HasSuffix(text, "\r") {
		text = strings.TrimSuffix(text, "\r")
		sanitizer.trailingCarriageReturn = true
	}
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	return strings.ReplaceAll(text, "\a", "")
}

func (sanitizer *terminalSanitizer) flush() string {
	text := sanitizer.pending
	if strings.HasPrefix(text, "\x1b") {
		text = ""
	}
	sanitizer.pending, sanitizer.discardMode, sanitizer.trackingPromptTail = "", 0, false
	text = sanitizer.normalize(text)
	if sanitizer.trailingCarriageReturn {
		sanitizer.trailingCarriageReturn = false
		text += "\n"
	}
	return text
}

type windowsTerminalSendOperation struct {
	mu              sync.Mutex
	done            chan struct{}
	output          *terminalTextBuffer
	startedAt       time.Time
	result          TerminalSendResult
	err             error
	settled         bool
	cancelRequested bool
	cancelOnce      sync.Once
	cancel          func()
}

func newWindowsTerminalSendOperation(maxBytes int, cancel func()) *windowsTerminalSendOperation {
	return &windowsTerminalSendOperation{done: make(chan struct{}), output: newTerminalTextBuffer(maxBytes, 0), startedAt: time.Now(), cancel: cancel}
}

func (operation *windowsTerminalSendOperation) Done() <-chan struct{} { return operation.done }

func (operation *windowsTerminalSendOperation) Result() (TerminalSendResult, error) {
	select {
	case <-operation.done:
		operation.mu.Lock()
		defer operation.mu.Unlock()
		return operation.result, operation.err
	default:
		return TerminalSendResult{}, errors.New("PTY send is still active")
	}
}

func (operation *windowsTerminalSendOperation) ReadOutput() TerminalSendRead {
	return operation.output.consume()
}

func (operation *windowsTerminalSendOperation) Cancel() bool {
	operation.mu.Lock()
	if operation.settled {
		operation.mu.Unlock()
		return false
	}
	operation.cancelRequested = true
	operation.mu.Unlock()
	operation.cancelOnce.Do(operation.cancel)
	return true
}

func (operation *windowsTerminalSendOperation) finish(result TerminalSendResult, err error) {
	operation.mu.Lock()
	if operation.settled {
		operation.mu.Unlock()
		return
	}
	operation.settled, operation.result, operation.err = true, result, err
	operation.mu.Unlock()
	close(operation.done)
}

type windowsPTYSession struct {
	mu           sync.Mutex
	pty          *windowsConPTY
	config       TerminalConfig
	pid          int
	motd         string
	status       TerminalSessionStatus
	scrollback   *terminalTextBuffer
	active       *windowsTerminalSendOperation
	closing      bool
	closeDone    chan struct{}
	closeErr     error
	readDone     chan struct{}
	decoder      terminalUTF8Decoder
	sanitizer    terminalSanitizer
	lastOutput   time.Time
	promptSeen   bool
	promptText   bool
	promptTail   string
	initializing bool
}

func newWindowsPTYSession(pty *windowsConPTY, config TerminalConfig) *windowsPTYSession {
	session := &windowsPTYSession{
		pty: pty, config: config, pid: pty.pid, status: runningTerminalStatus(),
		scrollback: newTerminalTextBuffer(config.ScrollbackMaxBytes, config.ScrollbackLines),
		readDone:   make(chan struct{}), lastOutput: time.Now(), sanitizer: terminalSanitizer{maxPendingBytes: config.MaxReadBytes},
	}
	go session.readLoop()
	go session.waitLoop()
	return session
}

func (session *windowsPTYSession) initialize(ctx context.Context) error {
	session.mu.Lock()
	session.initializing = true
	session.mu.Unlock()
	motd, err := initializeTerminalShell(ctx, session, session.config)
	session.mu.Lock()
	session.initializing = false
	if err == nil {
		session.motd = motd
	}
	session.mu.Unlock()
	return err
}

func (session *windowsPTYSession) MOTD() string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.motd
}

func (session *windowsPTYSession) PID() int { return session.pid }

func (session *windowsPTYSession) Status() TerminalSessionStatus {
	session.mu.Lock()
	defer session.mu.Unlock()
	return cloneTerminalStatus(session.status)
}

func (session *windowsPTYSession) resetReadinessLocked() {
	session.lastOutput = time.Now()
	session.promptSeen, session.promptText, session.promptTail = false, false, ""
}

func (session *windowsPTYSession) StartSend(ctx context.Context, request TerminalSendRequest) (TerminalSendOperation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session.mu.Lock()
	if session.closing {
		session.mu.Unlock()
		return nil, errors.New("PTY session is closing")
	}
	if session.status.Kind == "exited" {
		session.mu.Unlock()
		return nil, errors.New("PTY session has exited")
	}
	if session.active != nil {
		session.mu.Unlock()
		return nil, &TerminalError{Code: "SEND_ACTIVE", Message: "PTY session already has an active send"}
	}
	var operation *windowsTerminalSendOperation
	operation = newWindowsTerminalSendOperation(session.config.MaxReadBytes, func() { go session.interrupt(operation) })
	session.active = operation
	session.resetReadinessLocked()
	session.mu.Unlock()
	input := request.Text
	if request.Submit {
		input += "\r"
	}
	if input != "" {
		if err := session.pty.write([]byte(input)); err != nil {
			session.failActive(operation, err)
			return operation, nil
		}
	}
	go session.pollOperation(operation)
	go func() {
		select {
		case <-ctx.Done():
			operation.Cancel()
		case <-operation.Done():
		}
	}()
	return operation, nil
}

func (session *windowsPTYSession) pollOperation(operation *windowsTerminalSendOperation) {
	ticker := time.NewTicker(session.config.PollInterval)
	timer := time.NewTimer(session.config.Timeout)
	defer ticker.Stop()
	defer timer.Stop()
	for {
		select {
		case <-operation.Done():
			return
		case <-session.pty.done:
			session.settleActive(operation, TerminalWaitSessionExit)
		case <-timer.C:
			session.settleActive(operation, TerminalWaitTimeout)
		case <-ticker.C:
			session.pollReadiness(operation)
		}
	}
}

func (session *windowsPTYSession) pollReadiness(operation *windowsTerminalSendOperation) {
	session.mu.Lock()
	if session.active != operation || session.closing {
		session.mu.Unlock()
		return
	}
	status := cloneTerminalStatus(session.status)
	promptSeen, promptText := session.promptSeen, session.promptText
	lastOutput, initializing := session.lastOutput, session.initializing
	session.mu.Unlock()
	if status.Kind == "exited" {
		session.settleActive(operation, TerminalWaitSessionExit)
		return
	}
	idleFor := time.Since(lastOutput)
	if promptSeen && promptText && idleFor >= session.config.PollInterval {
		session.settleActive(operation, TerminalWaitStdinRead)
		return
	}
	scrollback, _ := session.scrollback.snapshot()
	if (!initializing || scrollback != "") && idleFor >= session.config.IdleSilence {
		session.settleActive(operation, TerminalWaitInferredIdle)
	}
}

func (session *windowsPTYSession) settleActive(operation *windowsTerminalSendOperation, reason string) {
	session.mu.Lock()
	if session.active != operation {
		session.mu.Unlock()
		return
	}
	session.active = nil
	status := cloneTerminalStatus(session.status)
	_, scrollbackTruncated := session.scrollback.snapshot()
	session.mu.Unlock()
	viewport, operationTruncated := operation.output.snapshot()
	operation.finish(TerminalSendResult{Viewport: viewport, WaitReason: reason, SessionStatus: status, Truncated: operationTruncated || scrollbackTruncated}, nil)
}

func (session *windowsPTYSession) failActive(operation *windowsTerminalSendOperation, err error) {
	session.mu.Lock()
	if session.active != operation {
		session.mu.Unlock()
		return
	}
	session.active = nil
	session.mu.Unlock()
	operation.finish(TerminalSendResult{}, err)
}

func (session *windowsPTYSession) interrupt(operation *windowsTerminalSendOperation) {
	session.mu.Lock()
	active := session.active == operation && !session.closing
	session.mu.Unlock()
	if active {
		if _, err := session.Signal(TerminalSignalInterrupt); err != nil {
			session.failActive(operation, err)
		}
	}
}

func (session *windowsPTYSession) readLoop() {
	defer close(session.readDone)
	defer session.pty.output.Close()
	buffer := make([]byte, 32*1024)
	for {
		count, err := session.pty.output.Read(buffer)
		if count > 0 {
			session.onDecoded(session.decoder.push(buffer[:count]))
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				session.mu.Lock()
				closing, active := session.closing, session.active
				session.mu.Unlock()
				if !closing && active != nil {
					session.failActive(active, err)
				}
			}
			break
		}
	}
	session.onDecoded(session.decoder.flush())
	session.appendOutput(session.sanitizer.flush())
}

func (session *windowsPTYSession) onDecoded(text string) {
	if text == "" {
		return
	}
	chunk := session.sanitizer.push(text)
	session.appendOutput(chunk.text)
	session.mu.Lock()
	if chunk.prompt {
		session.promptSeen, session.promptTail, session.lastOutput = true, "", time.Now()
	}
	if session.promptSeen && chunk.hasTail {
		remaining := len(terminalPrompt) + 1 - len(session.promptTail)
		if remaining < 0 {
			remaining = 0
		}
		if len(chunk.promptTail) > remaining {
			session.promptTail = terminalPrompt + "\x00"
		} else {
			session.promptTail += chunk.promptTail
		}
		session.promptText = session.promptTail == terminalPrompt
	}
	session.mu.Unlock()
}

func (session *windowsPTYSession) appendOutput(text string) {
	if text == "" {
		return
	}
	session.scrollback.append(text)
	session.mu.Lock()
	session.lastOutput = time.Now()
	active := session.active
	session.mu.Unlock()
	if active != nil {
		active.output.append(text)
	}
}

func (session *windowsPTYSession) waitLoop() {
	exitCode, waitErr := session.pty.result()
	select {
	case <-session.readDone:
	case <-time.After(session.config.DisposeGrace):
		_ = session.pty.output.Close()
		<-session.readDone
	}
	status := TerminalSessionStatus{Kind: "exited", ExitCode: &exitCode}
	session.mu.Lock()
	session.status = status
	active := session.active
	closing := session.closing
	session.mu.Unlock()
	if active != nil {
		if waitErr != nil && !closing {
			session.failActive(active, waitErr)
		} else {
			session.settleActive(active, TerminalWaitSessionExit)
		}
	}
}

func (session *windowsPTYSession) Read(request TerminalReadRequest) (TerminalReadResult, error) {
	if request.Offset < 0 {
		return TerminalReadResult{}, errors.New("PTY read offset must be a non-negative integer")
	}
	if request.Count < 0 {
		return TerminalReadResult{}, errors.New("PTY read count must be a positive integer")
	}
	count := request.Count
	if count == 0 {
		count = 500
	}
	text, retainedTruncated := session.scrollback.snapshot()
	if text == "" {
		return TerminalReadResult{LineBegin: request.Offset, LineEnd: request.Offset, Truncated: retainedTruncated}, nil
	}
	lines := strings.Split(text, "\n")
	total := len(lines)
	if request.Offset >= total {
		return TerminalReadResult{TotalLines: total, LineBegin: request.Offset, LineEnd: request.Offset, Truncated: retainedTruncated}, nil
	}
	end := total - request.Offset
	start := end - count
	if start < 0 {
		start = 0
	}
	page, bounded := terminalUTF8Tail(strings.Join(lines[start:end], "\n"), session.config.MaxReadBytes)
	returned := 0
	if page != "" {
		returned = len(strings.Split(page, "\n"))
	}
	return TerminalReadResult{Text: page, TotalLines: total, LineBegin: request.Offset, LineEnd: request.Offset + returned, Truncated: retainedTruncated || bounded}, nil
}

func (session *windowsPTYSession) Signal(name string) (TerminalSignalResult, error) {
	session.mu.Lock()
	closing := session.closing
	session.mu.Unlock()
	if closing {
		return TerminalSignalResult{}, errors.New("PTY session is closing")
	}
	if name == TerminalSignalKill {
		return TerminalSignalResult{}, errors.New("refusing to SIGKILL the terminal shell; close the terminal session instead")
	}
	pid, err := session.pty.signal(name)
	return TerminalSignalResult{Delivered: err == nil, TargetPGID: pid}, err
}

func (session *windowsPTYSession) Close(reason string) error {
	session.mu.Lock()
	if session.closeDone != nil {
		done := session.closeDone
		session.mu.Unlock()
		<-done
		return session.closeErr
	}
	session.closing = true
	session.closeDone = make(chan struct{})
	done := session.closeDone
	session.mu.Unlock()
	err := session.pty.terminate(1)
	select {
	case <-session.readDone:
	case <-time.After(session.config.DisposeGrace):
		_ = session.pty.output.Close()
		if err == nil {
			err = errors.New("PTY cleanup failed (" + reason + "): output did not close")
		}
	}
	session.mu.Lock()
	active := session.active
	session.closeErr = err
	session.mu.Unlock()
	if active != nil {
		session.settleActive(active, TerminalWaitSessionExit)
	}
	close(done)
	return err
}
