//go:build !windows

package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
)

const (
	terminalPromptMarker = "133;D;"
	terminalPrompt       = "dsh> "
)

func platformTerminalShellDefaults() (string, string, []string) {
	path, args := terminalShellDefaults(TerminalShellDialectBash)
	return TerminalShellDialectBash, path, args
}

func terminalShellDefaults(dialect string) (string, []string) {
	if dialect == TerminalShellDialectPwsh {
		return "pwsh", []string{"-NoLogo", "-NoProfile"}
	}
	return "/bin/bash", []string{"--noprofile", "--norc", "-i"}
}

type bashTerminalBackend struct{ config TerminalConfig }

func newPlatformBashTerminalBackend(config TerminalConfig) TerminalBackend {
	return &bashTerminalBackend{config: config}
}

func (backend *bashTerminalBackend) Type() string { return backend.config.BackendType }

func (backend *bashTerminalBackend) Spawn(ctx context.Context, spec TerminalBackendSpawnSpec) (TerminalBackendSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	program, args, err := terminalInvocation(backend.config, spec)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(program, args...)
	cmd.Dir = spec.CWD
	cmd.Env = scrubbedChildEnv(terminalShellEnvironment(backend.config, spec))
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: uint16(backend.config.Rows), Cols: uint16(backend.config.Cols)})
	if err != nil {
		return nil, err
	}
	session := newLocalPTYSession(cmd, ptmx, backend.config)
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

func terminalInvocation(config TerminalConfig, spec TerminalBackendSpawnSpec) (string, []string, error) {
	if spec.SandboxMode == sandboxDangerFull {
		return config.ShellPath, append([]string(nil), config.ShellArgs...), nil
	}
	if spec.SandboxMode != sandboxReadOnly && spec.SandboxMode != sandboxWorkspaceWrite {
		return "", nil, fmt.Errorf("terminal-bash: unsupported sandbox mode %q", spec.SandboxMode)
	}
	return sandboxInvocation(spec.SandboxMode, spec.Workspace, spec.CWD, config.ShellPath, config.ShellArgs)
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
	if len(decoder.pending) == 0 {
		return ""
	}
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
			for end < len(sanitizer.pending) {
				code := sanitizer.pending[end]
				if code >= 0x40 && code <= 0x7e {
					break
				}
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
	index := 0
	if sanitizer.discardOSCEscape {
		sanitizer.discardOSCEscape = false
		if strings.HasPrefix(chunk, "\\") {
			sanitizer.discardMode = 0
			return chunk[1:]
		}
	}
	for index < len(chunk) {
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
		index++
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

type localTerminalSendOperation struct {
	mu                   sync.Mutex
	done                 chan struct{}
	output               *terminalTextBuffer
	startedAt            time.Time
	result               TerminalSendResult
	err                  error
	settled              bool
	cancelRequested      bool
	cancelOnce           sync.Once
	cancel               func()
	initialForeground    int
	initialLeftStdinWait bool
}

func newLocalTerminalSendOperation(maxBytes int, cancel func()) *localTerminalSendOperation {
	return &localTerminalSendOperation{
		done: make(chan struct{}), output: newTerminalTextBuffer(maxBytes, 0),
		startedAt: time.Now(), cancel: cancel, initialLeftStdinWait: true,
	}
}

func (operation *localTerminalSendOperation) Done() <-chan struct{} { return operation.done }

func (operation *localTerminalSendOperation) Result() (TerminalSendResult, error) {
	select {
	case <-operation.done:
		operation.mu.Lock()
		defer operation.mu.Unlock()
		return operation.result, operation.err
	default:
		return TerminalSendResult{}, errors.New("PTY send is still active")
	}
}

func (operation *localTerminalSendOperation) ReadOutput() TerminalSendRead {
	return operation.output.consume()
}

func (operation *localTerminalSendOperation) Cancel() bool {
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

func (operation *localTerminalSendOperation) append(text string) { operation.output.append(text) }

func (operation *localTerminalSendOperation) setInitialForeground(foreground terminalForeground) {
	operation.initialForeground = foreground.pgid
	operation.initialLeftStdinWait = !foreground.stdinWaiting
}

func (operation *localTerminalSendOperation) acceptsStdinWait(foreground terminalForeground) bool {
	operation.mu.Lock()
	defer operation.mu.Unlock()
	if foreground.pgid != operation.initialForeground {
		return foreground.stdinWaiting
	}
	if !foreground.stdinWaiting {
		operation.initialLeftStdinWait = true
	}
	return foreground.stdinWaiting && operation.initialLeftStdinWait
}

func (operation *localTerminalSendOperation) finish(result TerminalSendResult, err error) {
	operation.mu.Lock()
	if operation.settled {
		operation.mu.Unlock()
		return
	}
	operation.settled, operation.result, operation.err = true, result, err
	operation.mu.Unlock()
	close(operation.done)
}

type localPTYSession struct {
	mu           sync.Mutex
	writeMu      sync.Mutex
	cmd          *exec.Cmd
	ptmx         *os.File
	config       TerminalConfig
	pid          int
	motd         string
	status       TerminalSessionStatus
	scrollback   *terminalTextBuffer
	active       *localTerminalSendOperation
	closing      bool
	closeDone    chan struct{}
	closeErr     error
	readDone     chan struct{}
	cmdDone      chan struct{}
	decoder      terminalUTF8Decoder
	sanitizer    terminalSanitizer
	lastOutput   time.Time
	promptSeen   bool
	promptText   bool
	promptTail   string
	shellPGID    int
	initializing bool
}

func newLocalPTYSession(cmd *exec.Cmd, ptmx *os.File, config TerminalConfig) *localPTYSession {
	session := &localPTYSession{
		cmd: cmd, ptmx: ptmx, config: config, pid: cmd.Process.Pid,
		status: runningTerminalStatus(), scrollback: newTerminalTextBuffer(config.ScrollbackMaxBytes, config.ScrollbackLines),
		readDone: make(chan struct{}), cmdDone: make(chan struct{}), lastOutput: time.Now(),
		sanitizer: terminalSanitizer{maxPendingBytes: config.MaxReadBytes},
	}
	go session.readLoop()
	go session.waitLoop()
	return session
}

func (session *localPTYSession) initialize(ctx context.Context) error {
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

func (session *localPTYSession) MOTD() string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.motd
}

func (session *localPTYSession) PID() int { return session.pid }

func (session *localPTYSession) Status() TerminalSessionStatus {
	session.mu.Lock()
	defer session.mu.Unlock()
	return cloneTerminalStatus(session.status)
}

func (session *localPTYSession) resetReadinessLocked() {
	session.lastOutput = time.Now()
	session.promptSeen, session.promptText, session.promptTail = false, false, ""
}

func (session *localPTYSession) StartSend(ctx context.Context, request TerminalSendRequest) (TerminalSendOperation, error) {
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
	var operation *localTerminalSendOperation
	operation = newLocalTerminalSendOperation(session.config.MaxReadBytes, func() { go session.interrupt(operation) })
	session.active = operation
	session.resetReadinessLocked()
	session.mu.Unlock()

	foreground, _ := session.inspectForeground()
	operation.setInitialForeground(foreground)
	input := request.Text
	if request.Submit {
		input += "\r"
	}
	if input != "" {
		session.mu.Lock()
		if session.active == operation {
			session.resetReadinessLocked()
		}
		session.mu.Unlock()
		session.writeMu.Lock()
		_, err := io.WriteString(session.ptmx, input)
		session.writeMu.Unlock()
		if err != nil {
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

func (session *localPTYSession) pollOperation(operation *localTerminalSendOperation) {
	ticker := time.NewTicker(session.config.PollInterval)
	timer := time.NewTimer(session.config.Timeout)
	defer ticker.Stop()
	defer timer.Stop()
	for {
		select {
		case <-operation.Done():
			return
		case <-session.cmdDone:
			session.settleActive(operation, TerminalWaitSessionExit)
		case <-timer.C:
			session.settleActive(operation, TerminalWaitTimeout)
		case <-ticker.C:
			session.pollReadiness(operation)
		}
	}
}

func (session *localPTYSession) pollReadiness(operation *localTerminalSendOperation) {
	session.mu.Lock()
	if session.active != operation || session.closing {
		session.mu.Unlock()
		return
	}
	status := cloneTerminalStatus(session.status)
	promptSeen, promptText, shellPGID := session.promptSeen, session.promptText, session.shellPGID
	lastOutput, initializing := session.lastOutput, session.initializing
	session.mu.Unlock()
	if status.Kind == "exited" {
		session.settleActive(operation, TerminalWaitSessionExit)
		return
	}
	foreground, _ := session.inspectForeground()
	idleFor := time.Since(lastOutput)
	if promptSeen && foreground.pgid > 0 && shellPGID == 0 {
		session.mu.Lock()
		if session.shellPGID == 0 {
			session.shellPGID = foreground.pgid
		}
		shellPGID = session.shellPGID
		session.mu.Unlock()
	}
	if promptSeen && promptText && idleFor >= session.config.PollInterval && foreground.pgid == shellPGID {
		session.settleActive(operation, TerminalWaitStdinRead)
		return
	}
	if time.Since(operation.startedAt) >= session.config.ExactProbeAfter && operation.acceptsStdinWait(foreground) {
		session.settleActive(operation, TerminalWaitStdinRead)
		return
	}
	scrollback, _ := session.scrollback.snapshot()
	startupHasOutput := !initializing || scrollback != ""
	handoff := time.Duration(0)
	if promptSeen {
		handoff = session.config.HandoffGrace
	}
	if startupHasOutput && idleFor >= session.config.IdleSilence+handoff {
		session.settleActive(operation, TerminalWaitInferredIdle)
	}
}

func (session *localPTYSession) settleActive(operation *localTerminalSendOperation, reason string) {
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
	operation.finish(TerminalSendResult{
		Viewport: viewport, WaitReason: reason, SessionStatus: status,
		Truncated: operationTruncated || scrollbackTruncated,
	}, nil)
}

func (session *localPTYSession) failActive(operation *localTerminalSendOperation, err error) {
	session.mu.Lock()
	if session.active != operation {
		session.mu.Unlock()
		return
	}
	session.active = nil
	session.mu.Unlock()
	operation.finish(TerminalSendResult{}, err)
}

func (session *localPTYSession) interrupt(operation *localTerminalSendOperation) {
	session.mu.Lock()
	active := session.active == operation && !session.closing
	session.mu.Unlock()
	if !active {
		return
	}
	if _, err := session.Signal(TerminalSignalInterrupt); err != nil {
		session.failActive(operation, err)
	}
}

func (session *localPTYSession) readLoop() {
	defer close(session.readDone)
	buffer := make([]byte, 32*1024)
	for {
		count, err := session.ptmx.Read(buffer)
		if count > 0 {
			session.onDecoded(session.decoder.push(buffer[:count]))
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.EIO) {
				session.mu.Lock()
				closing := session.closing
				active := session.active
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

func (session *localPTYSession) onDecoded(text string) {
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

func (session *localPTYSession) appendOutput(text string) {
	if text == "" {
		return
	}
	session.scrollback.append(text)
	session.mu.Lock()
	session.lastOutput = time.Now()
	active := session.active
	session.mu.Unlock()
	if active != nil {
		active.append(text)
	}
}

func (session *localPTYSession) waitLoop() {
	err := session.cmd.Wait()
	select {
	case <-session.readDone:
	case <-time.After(100 * time.Millisecond):
		_ = session.ptmx.Close()
		<-session.readDone
	}
	status := TerminalSessionStatus{Kind: "exited"}
	if wait, ok := session.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && wait.Signaled() {
		signal := jobSignalName(wait.Signal())
		status.Signal = &signal
	} else {
		exitCode := session.cmd.ProcessState.ExitCode()
		if err == nil || exitCode >= 0 {
			status.ExitCode = &exitCode
		}
	}
	session.mu.Lock()
	session.status = status
	active := session.active
	session.mu.Unlock()
	if active != nil {
		session.settleActive(active, TerminalWaitSessionExit)
	}
	close(session.cmdDone)
}

func (session *localPTYSession) Read(request TerminalReadRequest) (TerminalReadResult, error) {
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
	return TerminalReadResult{
		Text: page, TotalLines: total, LineBegin: request.Offset,
		LineEnd: request.Offset + returned, Truncated: retainedTruncated || bounded,
	}, nil
}

type terminalForeground struct {
	pgid         int
	stdinWaiting bool
}

func (session *localPTYSession) inspectForeground() (terminalForeground, error) {
	pgid, err := unix.IoctlGetInt(int(session.ptmx.Fd()), unix.TIOCGPGRP)
	if err != nil || pgid <= 0 {
		return terminalForeground{}, err
	}
	if runtime.GOOS == "linux" && !linuxProcessGroupInSession(pgid, session.pid) {
		return terminalForeground{}, fmt.Errorf("foreground process group %d is outside terminal session %d", pgid, session.pid)
	}
	return terminalForeground{pgid: pgid, stdinWaiting: runtime.GOOS == "linux" && linuxProcessGroupWaitsOnStdin(pgid)}, nil
}

func terminalSignalNumber(name string) (syscall.Signal, error) {
	switch name {
	case TerminalSignalInterrupt:
		return syscall.SIGINT, nil
	case TerminalSignalTerminate:
		return syscall.SIGTERM, nil
	case TerminalSignalKill:
		return syscall.SIGKILL, nil
	case TerminalSignalStop:
		return syscall.SIGTSTP, nil
	case TerminalSignalHangup:
		return syscall.SIGHUP, nil
	default:
		return 0, fmt.Errorf("unsupported terminal signal %q", name)
	}
}

func (session *localPTYSession) Signal(name string) (TerminalSignalResult, error) {
	session.mu.Lock()
	closing := session.closing
	session.mu.Unlock()
	if closing {
		return TerminalSignalResult{}, errors.New("PTY session is closing")
	}
	signal, err := terminalSignalNumber(name)
	if err != nil {
		return TerminalSignalResult{}, err
	}
	foreground, err := session.inspectForeground()
	if err != nil || foreground.pgid == 0 {
		return TerminalSignalResult{}, fmt.Errorf("cannot resolve foreground process group for terminal %d", session.pid)
	}
	if signal == syscall.SIGKILL && foreground.pgid == session.pid {
		return TerminalSignalResult{}, errors.New("refusing to SIGKILL the terminal shell; close the terminal session instead")
	}
	if err := syscall.Kill(-foreground.pgid, signal); err != nil {
		return TerminalSignalResult{}, err
	}
	return TerminalSignalResult{Delivered: true, TargetPGID: foreground.pgid}, nil
}

func (session *localPTYSession) Close(reason string) error {
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

	err := session.terminateProcessTree()
	_ = session.ptmx.Close()
	select {
	case <-session.cmdDone:
	case <-time.After(session.config.DisposeGrace):
		if err == nil {
			err = fmt.Errorf("PTY cleanup failed (%s): surviving pid %d", reason, session.pid)
		}
	}
	session.mu.Lock()
	active := session.active
	session.closeErr = err
	if err != nil {
		session.closing = false
		session.closeDone = nil
	}
	session.mu.Unlock()
	if active != nil {
		session.settleActive(active, TerminalWaitSessionExit)
	}
	close(done)
	return err
}

type linuxProcessIdentity struct {
	pid, parent, group, session int
	state                       byte
	started                     string
}

func readLinuxProcess(pid int) (linuxProcessIdentity, bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return linuxProcessIdentity{}, false
	}
	text := string(data)
	close := strings.LastIndexByte(text, ')')
	if close < 0 || close+2 >= len(text) {
		return linuxProcessIdentity{}, false
	}
	fields := strings.Fields(text[close+2:])
	if len(fields) < 20 || len(fields[0]) != 1 {
		return linuxProcessIdentity{}, false
	}
	parent, err1 := strconv.Atoi(fields[1])
	group, err2 := strconv.Atoi(fields[2])
	sessionID, err3 := strconv.Atoi(fields[3])
	if err1 != nil || err2 != nil || err3 != nil {
		return linuxProcessIdentity{}, false
	}
	return linuxProcessIdentity{pid: pid, parent: parent, group: group, session: sessionID, state: fields[0][0], started: fields[19]}, true
}

func scanLinuxProcesses() []linuxProcessIdentity {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	processes := make([]linuxProcessIdentity, 0)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if process, ok := readLinuxProcess(pid); ok {
			processes = append(processes, process)
		}
	}
	return processes
}

func linuxProcessGroupInSession(group, sessionID int) bool {
	for _, process := range scanLinuxProcesses() {
		if process.group == group && process.session == sessionID {
			return true
		}
	}
	return false
}

func linuxProcessGroupWaitsOnStdin(group int) bool {
	for _, process := range scanLinuxProcesses() {
		if process.group != group || strings.ContainsRune("ZXx", rune(process.state)) {
			continue
		}
		tasks, _ := os.ReadDir(fmt.Sprintf("/proc/%d/task", process.pid))
		for _, task := range tasks {
			data, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/syscall", process.pid, task.Name()))
			if err != nil {
				continue
			}
			fields := strings.Fields(string(data))
			if len(fields) < 2 || fields[0] == "running" {
				continue
			}
			number, numberErr := strconv.ParseInt(fields[0], 0, 64)
			fd, fdErr := strconv.ParseInt(fields[1], 0, 64)
			readSyscall := int64(0)
			if runtime.GOARCH == "arm64" {
				readSyscall = 63
			}
			if numberErr == nil && fdErr == nil && number == readSyscall && fd == 0 {
				return true
			}
		}
	}
	return false
}

func linuxOwnedProcesses(root int) []linuxProcessIdentity {
	processes := scanLinuxProcesses()
	owned := map[int]bool{root: true}
	changed := true
	for changed {
		changed = false
		for _, process := range processes {
			if owned[process.pid] || process.session == root || owned[process.parent] {
				if !owned[process.pid] {
					owned[process.pid], changed = true, true
				}
			}
		}
	}
	result := make([]linuxProcessIdentity, 0)
	for _, process := range processes {
		if process.pid != root && owned[process.pid] {
			result = append(result, process)
		}
	}
	return result
}

func linuxProcessAlive(identity linuxProcessIdentity) bool {
	current, ok := readLinuxProcess(identity.pid)
	return ok && current.started == identity.started && !strings.ContainsRune("ZXx", rune(current.state))
}

func signalLinuxProcesses(processes []linuxProcessIdentity, signal syscall.Signal) {
	for _, process := range processes {
		if linuxProcessAlive(process) {
			_ = syscall.Kill(process.pid, signal)
		}
	}
}

func waitLinuxProcesses(processes []linuxProcessIdentity, timeout time.Duration) []linuxProcessIdentity {
	deadline := time.Now().Add(timeout)
	for {
		survivors := processes[:0]
		for _, process := range processes {
			if linuxProcessAlive(process) {
				survivors = append(survivors, process)
			}
		}
		if len(survivors) == 0 || time.Now().After(deadline) {
			return survivors
		}
		processes = append([]linuxProcessIdentity(nil), survivors...)
		time.Sleep(25 * time.Millisecond)
	}
}

func (session *localPTYSession) terminateProcessTree() error {
	if runtime.GOOS != "linux" {
		_ = syscall.Kill(-session.pid, syscall.SIGTERM)
		select {
		case <-session.cmdDone:
			return nil
		case <-time.After(session.config.DisposeGrace):
		}
		_ = syscall.Kill(-session.pid, syscall.SIGKILL)
		return nil
	}
	descendants := linuxOwnedProcesses(session.pid)
	signalLinuxProcesses(descendants, syscall.SIGTERM)
	survivors := waitLinuxProcesses(descendants, session.config.DisposeGrace)
	survivors = append(survivors, linuxOwnedProcesses(session.pid)...)
	signalLinuxProcesses(survivors, syscall.SIGKILL)
	survivors = waitLinuxProcesses(survivors, session.config.DisposeGrace)
	if session.cmd.Process != nil {
		_ = session.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-session.cmdDone:
	case <-time.After(session.config.DisposeGrace):
		if session.cmd.Process != nil {
			_ = session.cmd.Process.Kill()
		}
	}
	after := linuxOwnedProcesses(session.pid)
	signalLinuxProcesses(after, syscall.SIGKILL)
	after = waitLinuxProcesses(after, session.config.DisposeGrace)
	if len(survivors) > 0 || len(after) > 0 {
		pids := make([]string, 0, len(survivors)+len(after))
		for _, process := range append(survivors, after...) {
			pids = append(pids, strconv.Itoa(process.pid))
		}
		return fmt.Errorf("terminal cleanup failed; surviving pids: %s", strings.Join(pids, ", "))
	}
	return nil
}
