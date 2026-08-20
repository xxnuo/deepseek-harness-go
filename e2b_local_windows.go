//go:build windows

package harness

import (
	"context"
	"errors"
	"sync"
)

func localCommandInvocation(command string) (string, []string, error) {
	program, err := resolvePowerShell()
	if err != nil {
		return "", nil, err
	}
	return program, powershellArgs(command), nil
}

type windowsLocalPTY struct {
	sandbox *localSandbox
	mu      sync.Mutex
	handles map[int]*windowsLocalPTYHandle
}

func newLocalPTY(sandbox *localSandbox) E2BPTY {
	return &windowsLocalPTY{sandbox: sandbox, handles: map[int]*windowsLocalPTYHandle{}}
}

func (pty *windowsLocalPTY) Create(ctx context.Context, opts E2BPTYOptions, onData func([]byte)) (E2BCommandHandle, error) {
	program, err := resolvePowerShell()
	if err != nil {
		return nil, err
	}
	rows, cols := opts.Rows, opts.Cols
	if rows == 0 {
		rows = 40
	}
	if cols == 0 {
		cols = 160
	}
	process, err := startWindowsConPTY(
		program, []string{"-NoLogo", "-NoProfile", "-NoExit"},
		pty.sandbox.path(defaultRemoteCWD(opts.CWD)), localEnv(opts.Env), rows, cols, nil,
	)
	if err != nil {
		return nil, err
	}
	handle := &windowsLocalPTYHandle{pty: process, outputDone: make(chan struct{})}
	pty.mu.Lock()
	pty.handles[process.pid] = handle
	pty.mu.Unlock()
	go func() {
		defer close(handle.outputDone)
		defer process.output.Close()
		buffer := make([]byte, 32*1024)
		for {
			count, readErr := process.output.Read(buffer)
			if count > 0 && onData != nil {
				onData(append([]byte(nil), buffer[:count]...))
			}
			if readErr != nil {
				break
			}
		}
	}()
	go func() {
		<-process.done
		pty.mu.Lock()
		delete(pty.handles, process.pid)
		pty.mu.Unlock()
	}()
	go func() {
		select {
		case <-ctx.Done():
			_ = process.terminate(1)
		case <-process.done:
		}
	}()
	return handle, nil
}

func (pty *windowsLocalPTY) SendInput(_ context.Context, pid int, data []byte) error {
	pty.mu.Lock()
	handle := pty.handles[pid]
	pty.mu.Unlock()
	if handle == nil {
		return errors.New("pty process not found")
	}
	return handle.SendStdin(data)
}

type windowsLocalPTYHandle struct {
	pty        *windowsConPTY
	outputDone chan struct{}
	waitOnce   sync.Once
	result     E2BCommandResult
	err        error
}

func (handle *windowsLocalPTYHandle) PID() int { return handle.pty.pid }

func (handle *windowsLocalPTYHandle) Wait() (E2BCommandResult, error) {
	handle.waitOnce.Do(func() {
		code, err := handle.pty.result()
		<-handle.outputDone
		handle.result = E2BCommandResult{ExitCode: code}
		if err != nil {
			handle.err = err
		} else if code != 0 {
			handle.err = &E2BCommandError{ExitCode: code}
		}
	})
	return handle.result, handle.err
}

func (handle *windowsLocalPTYHandle) SendStdin(data []byte) error { return handle.pty.write(data) }
func (handle *windowsLocalPTYHandle) CloseStdin() error {
	if handle.pty.input == nil {
		return nil
	}
	return handle.pty.input.Close()
}
func (handle *windowsLocalPTYHandle) Kill() error       { return handle.pty.terminate(1) }
func (handle *windowsLocalPTYHandle) Disconnect() error { return nil }
