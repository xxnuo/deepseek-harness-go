//go:build !windows

package harness

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/creack/pty"
)

func localCommandInvocation(command string) (string, []string, error) {
	return "/bin/bash", []string{"-lc", command}, nil
}

type localPTY struct {
	sandbox *localSandbox
	mu      sync.Mutex
	handles map[int]*localCommandHandle
}

func newLocalPTY(sandbox *localSandbox) E2BPTY {
	return &localPTY{sandbox: sandbox, handles: map[int]*localCommandHandle{}}
}

func (p *localPTY) Create(ctx context.Context, opts E2BPTYOptions, onData func([]byte)) (E2BCommandHandle, error) {
	cmd := exec.CommandContext(ctx, "/bin/bash", "-i")
	cmd.Dir = p.sandbox.path(defaultRemoteCWD(opts.CWD))
	cmd.Env = localEnv(opts.Env)
	ptmx, err := pty.Start(cmd)
	if err != nil {
		return nil, err
	}
	if opts.Rows > 0 && opts.Cols > 0 {
		_ = pty.Setsize(ptmx, &pty.Winsize{Rows: uint16(opts.Rows), Cols: uint16(opts.Cols)})
	}
	h := &localPTYHandle{localCommandHandle: &localCommandHandle{cmd: cmd, stdin: ptmx, result: make(chan commandResult, 1)}, pty: ptmx}
	h.stdoutDone = make(chan struct{})
	h.stderrDone = make(chan struct{})
	p.mu.Lock()
	p.handles[cmd.Process.Pid] = h.localCommandHandle
	p.mu.Unlock()
	go func() {
		b, _ := io.ReadAll(ptmx)
		if onData != nil {
			onData(b)
		}
		close(h.stdoutDone)
		close(h.stderrDone)
	}()
	go func() {
		err := cmd.Wait()
		h.result <- commandResult{result: E2BCommandResult{ExitCode: exitCode(err)}, err: err}
		_ = ptmx.Close()
		p.mu.Lock()
		delete(p.handles, cmd.Process.Pid)
		p.mu.Unlock()
	}()
	return h, nil
}

func (p *localPTY) SendInput(_ context.Context, pid int, data []byte) error {
	p.mu.Lock()
	h := p.handles[pid]
	p.mu.Unlock()
	if h == nil {
		return errors.New("pty process not found")
	}
	return h.SendStdin(data)
}

type localPTYHandle struct {
	*localCommandHandle
	pty *os.File
}
