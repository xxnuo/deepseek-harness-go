package harness

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const persistentShellTimeout = 5 * time.Minute

const (
	persistentShellDefaultDescription = "Run commands in a persistent shell. State, including the current directory and environment variables, persists across calls for this agent."
	persistentShellResetMessage       = "The persistent shell was reset; the next shell call starts from the workspace with a fresh current directory and environment."
)

type persistentShellRegistry struct {
	mu     sync.Mutex
	shells map[string]*persistentShell
	closed bool
}

type persistentShell struct {
	mu             sync.Mutex
	workspace      string
	mode           string
	timeout        time.Duration
	maxOutputChars int
	cmd            *exec.Cmd
	stdin          io.WriteCloser
	output         io.ReadCloser
	stdout         *bufio.Reader
	state          shellChildState
	closed         bool
}

type persistentShellResult struct {
	text     string
	exitCode int
	started  bool
	err      error
}

func newPersistentShellRegistry() *persistentShellRegistry {
	return &persistentShellRegistry{shells: map[string]*persistentShell{}}
}

func (r *persistentShellRegistry) run(ctx context.Context, owner, workspace, command string) (string, error) {
	return r.runWithMode(ctx, owner, workspace, sandboxWorkspaceWrite, command)
}

func (r *persistentShellRegistry) runWithMode(ctx context.Context, owner, workspace, mode, command string) (string, error) {
	return r.runWithModeOptions(ctx, owner, workspace, mode, command, persistentShellTimeout, editorOutputLimit)
}

func (r *persistentShellRegistry) runWithModeOptions(ctx context.Context, owner, workspace, mode, command string, timeout time.Duration, maxOutputChars int) (string, error) {
	if strings.TrimSpace(owner) == "" {
		return "", errors.New("persistent shell requires an owning session")
	}
	workspace, err := filepath.Abs(workspace)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(workspace)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("persistent shell: invalid workspace %q", workspace)
	}
	if timeout <= 0 {
		timeout = persistentShellTimeout
	}
	if maxOutputChars <= 0 {
		maxOutputChars = editorOutputLimit
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return "", errors.New("persistent shells are closed")
	}
	shell := r.shells[owner]
	if shell == nil {
		shell = &persistentShell{workspace: workspace, mode: mode, timeout: timeout, maxOutputChars: maxOutputChars}
		r.shells[owner] = shell
	} else if shell.workspace != workspace {
		r.mu.Unlock()
		return "", fmt.Errorf("persistent shell workspace changed from %q to %q", shell.workspace, workspace)
	}
	if mode != sandboxDangerFull && mode != sandboxWorkspaceWrite && mode != sandboxReadOnly {
		r.mu.Unlock()
		return "", fmt.Errorf("persistent shell: unsupported sandbox mode %q", mode)
	}
	if shell.mode != mode {
		// A persistent process cannot be tightened or widened in place. Drop its
		// state and start a fresh process under the newly resolved policy.
		shell.mu.Lock()
		shell.mode = mode
		shell.resetLocked()
		shell.mu.Unlock()
	}
	shell.mu.Lock()
	shell.timeout = timeout
	shell.maxOutputChars = maxOutputChars
	shell.mu.Unlock()
	r.mu.Unlock()
	return shell.run(ctx, command)
}

func (r *persistentShellRegistry) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	shells := make([]*persistentShell, 0, len(r.shells))
	for _, shell := range r.shells {
		shells = append(shells, shell)
	}
	r.shells = map[string]*persistentShell{}
	r.mu.Unlock()
	for _, shell := range shells {
		shell.close()
	}
}

func (r *persistentShellRegistry) hasOwnerActivity(owner string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.shells[owner] != nil
}

func (r *persistentShellRegistry) closeOwner(owner string) {
	r.mu.Lock()
	shell := r.shells[owner]
	delete(r.shells, owner)
	r.mu.Unlock()
	if shell != nil {
		shell.close()
	}
}

func (s *persistentShell) run(ctx context.Context, command string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s.closed {
		return "", errors.New("persistent shell is closed")
	}
	if strings.TrimSpace(command) == "" {
		return "", errors.New("command must be a non-empty string")
	}
	if strings.IndexByte(command, 0) >= 0 {
		return "", errors.New("command contains a NUL byte")
	}
	if err := s.ensureLocked(); err != nil {
		return "", err
	}
	nonce := strings.TrimPrefix(newID("shell"), "shell-")
	start := "__DSH_PERSISTENT_SHELL_START_" + nonce + "__"
	end := "__DSH_PERSISTENT_SHELL_END_" + nonce + ":"
	wrapper := persistentShellWrapper(command, start, end)
	if _, err := io.WriteString(s.stdin, wrapper); err != nil {
		s.resetLocked()
		return "", err
	}
	result := make(chan persistentShellResult, 1)
	go func(reader *bufio.Reader) {
		result <- readPersistentShellResult(reader, start, end)
	}(s.stdout)
	timeout := s.timeout
	if timeout <= 0 {
		timeout = persistentShellTimeout
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case value := <-result:
		if value.err != nil {
			state := s.finishAfterReadLocked()
			if !value.started {
				if value.text == "" {
					return "", fmt.Errorf("persistent shell failed to start: %w", value.err)
				}
				return "", fmt.Errorf("persistent shell failed to start: %s: %w", value.text, value.err)
			}
			text := appendShellStatus(clipEditorOutputLimit(value.text, s.maxOutputChars), persistentShellExitMarker(state))
			return appendShellStatus(text, persistentShellResetMessage), nil
		}
		text := clipEditorOutputLimit(value.text, s.maxOutputChars)
		if value.exitCode != 0 {
			text = appendShellStatus(text, fmt.Sprintf("[exit code: %d]", value.exitCode))
		}
		return text, nil
	case <-waitCtx.Done():
		upstreamErr := ctx.Err()
		s.terminateLocked()
		var value persistentShellResult
		select {
		case value = <-result:
		case <-time.After(2 * time.Second):
		}
		s.reapLocked()
		if upstreamErr != nil {
			return "", upstreamErr
		}
		partial := clipEditorOutputLimit(value.text, s.maxOutputChars)
		seconds := int((timeout + 500*time.Millisecond) / time.Second)
		return fmt.Sprintf("Your command timed out after %d seconds or experienced an OOM error. Below is partial output:\n%s\n%s", seconds, partial, persistentShellResetMessage), nil
	}
}

func (s *persistentShell) ensureLocked() error {
	if s.cmd != nil && s.cmd.ProcessState == nil {
		return nil
	}
	mode := s.mode
	if mode == "" {
		mode = sandboxWorkspaceWrite
	}
	program, args, err := persistentShellInvocation(mode, s.workspace)
	if err != nil {
		return err
	}
	cmd := exec.Command(program, args...)
	cmd.Dir = s.workspace
	cmd.Env = scrubbedChildEnv(map[string]string{"LC_ALL": "C", "LANG": "C", "NO_COLOR": "1", "TERM": "dumb", "PAGER": "cat", "GIT_PAGER": "cat", "PS1": "", "PS2": ""})
	configureChildProcess(cmd)
	state, err := prepareShellChild(cmd, mode, s.workspace)
	if err != nil {
		return err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		if state != nil {
			_ = state.close()
		}
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		if state != nil {
			_ = state.close()
		}
		return err
	}
	cmd.Stderr = cmd.Stdout
	if err := startShellChild(cmd, state); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return err
	}
	s.cmd, s.stdin, s.output, s.stdout, s.state = cmd, stdin, stdout, bufio.NewReader(stdout), state
	return nil
}

func (s *persistentShell) resetLocked() {
	s.terminateLocked()
	s.reapLocked()
}

func (s *persistentShell) terminateLocked() {
	if s.stdin != nil {
		_ = s.stdin.Close()
	}
	if s.cmd != nil && s.cmd.Process != nil && s.cmd.ProcessState == nil {
		_ = killChildProcess(s.cmd)
	}
}

func (s *persistentShell) reapLocked() *os.ProcessState {
	if s.cmd == nil {
		return nil
	}
	cmd := s.cmd
	_ = waitShellChild(cmd, s.state)
	state := cmd.ProcessState
	if s.output != nil {
		_ = s.output.Close()
	}
	s.cmd, s.stdin, s.output, s.stdout, s.state = nil, nil, nil, nil, nil
	return state
}

func (s *persistentShell) finishAfterReadLocked() *os.ProcessState {
	if s.stdin != nil {
		_ = s.stdin.Close()
	}
	if s.cmd == nil {
		return nil
	}
	done := make(chan struct{})
	childState := s.state
	go func(cmd *exec.Cmd, state shellChildState) {
		_ = waitShellChild(cmd, state)
		close(done)
	}(s.cmd, childState)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		if s.cmd.Process != nil {
			_ = killChildProcess(s.cmd)
		}
		<-done
	}
	state := s.cmd.ProcessState
	if s.output != nil {
		_ = s.output.Close()
	}
	s.cmd, s.stdin, s.output, s.stdout, s.state = nil, nil, nil, nil, nil
	return state
}

func (s *persistentShell) close() {
	s.mu.Lock()
	s.closed = true
	s.resetLocked()
	s.mu.Unlock()
}

func readPersistentShellResult(reader *bufio.Reader, start, end string) persistentShellResult {
	var captured strings.Builder
	var prefix strings.Builder
	started := false
	for {
		line, err := reader.ReadString('\n')
		if !started {
			if index := strings.Index(line, start); index >= 0 {
				started = true
				line = line[index+len(start):]
				line = strings.TrimPrefix(line, "\r")
				line = strings.TrimPrefix(line, "\n")
			} else {
				prefix.WriteString(line)
			}
		}
		if started {
			if index := strings.Index(line, end); index >= 0 {
				captured.WriteString(line[:index])
				status := strings.TrimSuffix(strings.TrimSuffix(line[index+len(end):], "\n"), "\r")
				exitCode, scanErr := strconv.Atoi(status)
				if scanErr != nil {
					return persistentShellResult{text: trimOneNewline(captured.String()), started: true, err: scanErr}
				}
				return persistentShellResult{text: trimOneNewline(captured.String()), exitCode: exitCode, started: true}
			}
			captured.WriteString(line)
		}
		if err != nil {
			text := captured.String()
			if !started {
				text = prefix.String()
			}
			return persistentShellResult{text: trimOneNewline(text), started: started, err: err}
		}
	}
}

func persistentShellExitMarker(state *os.ProcessState) string {
	if state == nil {
		return "[shell exited]"
	}
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok {
		return "[shell exited]"
	}
	if status.Signaled() {
		return fmt.Sprintf("[shell killed by signal: %s]", persistentSignalName(status.Signal()))
	}
	return fmt.Sprintf("[shell exited: code %d]", status.ExitStatus())
}

func persistentSignalName(signal syscall.Signal) string {
	switch signal {
	case syscall.SIGHUP:
		return "SIGHUP"
	case syscall.SIGINT:
		return "SIGINT"
	case syscall.SIGQUIT:
		return "SIGQUIT"
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGTERM:
		return "SIGTERM"
	default:
		return fmt.Sprintf("SIG%d", signal)
	}
}

func bashANSIQuote(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	value = strings.ReplaceAll(value, "\r", `\r`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return "$'" + value + "'"
}

func trimOneNewline(value string) string {
	value = strings.TrimSuffix(value, "\n")
	return strings.TrimSuffix(value, "\r")
}

func appendShellStatus(content, status string) string {
	if content == "" {
		return status
	}
	return content + "\n" + status
}
