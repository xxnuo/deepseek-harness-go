package harness

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

type E2BSubprocessSpec struct {
	Argv     []string
	CWD      string
	Env      map[string]string
	Grace    time.Duration
	OnStdout func([]byte)
	OnStderr func([]byte)
}

type E2BProcess struct {
	handle E2BCommandHandle
	done   chan struct{}
	result E2BCommandResult
	err    error
	once   sync.Once
}

type E2BSubprocessRuntime struct {
	Runtime *E2BRuntime
	Poll    time.Duration
	mu      sync.Mutex
	live    map[*E2BProcess]struct{}
}

func NewE2BSubprocessRuntime(runtime *E2BRuntime) *E2BSubprocessRuntime {
	return &E2BSubprocessRuntime{Runtime: runtime, Poll: 20 * time.Millisecond, live: map[*E2BProcess]struct{}{}}
}

func (s *E2BSubprocessRuntime) ResolveExecutable(ctx context.Context, command string, env map[string]string) (string, error) {
	if strings.TrimSpace(command) == "" {
		return "", errors.New("executable name must be non-empty")
	}
	if strings.Contains(command, "/") {
		if command[0] != '/' {
			return "", errors.New("relative executable paths are not supported")
		}
		return command, nil
	}
	sandbox, err := s.Runtime.GetSandbox(ctx)
	if err != nil {
		return "", err
	}
	prefix := ""
	if value, ok := env["PATH"]; ok {
		prefix = "PATH=" + e2bQuote(value) + " "
	}
	result, err := sandbox.Commands().Run(ctx, prefix+"command -v -- "+e2bQuote(command), E2BCommandOptions{CWD: s.Runtime.CWD, Env: e2bControlEnvs(nil)})
	if err != nil {
		return "", err
	}
	resolved := strings.TrimSpace(result.Stdout)
	if resolved == "" || strings.Contains(resolved, "\n") {
		return "", errors.New("executable did not resolve to one path")
	}
	if resolved[0] != '/' {
		resolved = strings.TrimSuffix(s.Runtime.CWD, "/") + "/" + resolved
	}
	return resolved, nil
}

func (s *E2BSubprocessRuntime) Spawn(ctx context.Context, spec E2BSubprocessSpec) (*E2BProcess, error) {
	if len(spec.Argv) == 0 || strings.TrimSpace(spec.Argv[0]) == "" {
		return nil, errors.New("argv must contain a program")
	}
	if spec.Grace <= 0 {
		spec.Grace = 3 * time.Second
	}
	sandbox, err := s.Runtime.GetSandbox(ctx)
	if err != nil {
		return nil, err
	}
	handle, err := sandbox.Commands().Start(ctx, spec.Argv, E2BCommandOptions{CWD: spec.CWD, Env: spec.Env, Stdin: true}, spec.OnStdout, spec.OnStderr)
	if err != nil {
		return nil, err
	}
	p := &E2BProcess{handle: handle, done: make(chan struct{})}
	s.mu.Lock()
	s.live[p] = struct{}{}
	s.mu.Unlock()
	go func() { p.result, p.err = handle.Wait(); close(p.done); s.mu.Lock(); delete(s.live, p); s.mu.Unlock() }()
	return p, nil
}

func (p *E2BProcess) PID() int                        { return p.handle.PID() }
func (p *E2BProcess) Wait() (E2BCommandResult, error) { <-p.done; return p.result, p.err }
func (p *E2BProcess) WriteStdin(data []byte) error    { return p.handle.SendStdin(data) }
func (p *E2BProcess) CloseStdin() error               { return p.handle.CloseStdin() }
func (p *E2BProcess) Terminate() error                { return p.handle.Kill() }
func (p *E2BProcess) Disconnect() error               { return p.handle.Disconnect() }

func (s *E2BSubprocessRuntime) Close(ctx context.Context) error {
	s.mu.Lock()
	live := make([]*E2BProcess, 0, len(s.live))
	for p := range s.live {
		live = append(live, p)
	}
	s.mu.Unlock()
	var failures []error
	for _, p := range live {
		if err := p.Terminate(); err != nil {
			failures = append(failures, err)
		}
		select {
		case <-p.done:
		case <-time.After(3 * time.Second):
			failures = append(failures, errors.New("subprocess cleanup timeout"))
		}
		if err := p.Disconnect(); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

type E2BTerminal struct {
	handle E2BCommandHandle
	output chan []byte
	done   chan struct{}
	result E2BCommandResult
	err    error
}

func (s *E2BSubprocessRuntime) SpawnTerminal(ctx context.Context, opts E2BPTYOptions) (*E2BTerminal, error) {
	sandbox, err := s.Runtime.GetSandbox(ctx)
	if err != nil {
		return nil, err
	}
	t := &E2BTerminal{output: make(chan []byte, 32), done: make(chan struct{})}
	h, err := sandbox.PTY().Create(ctx, opts, func(data []byte) {
		copyData := append([]byte(nil), data...)
		select {
		case t.output <- copyData:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	t.handle = h
	go func() { t.result, t.err = h.Wait(); close(t.output); close(t.done) }()
	return t, nil
}
func (t *E2BTerminal) PID() int                        { return t.handle.PID() }
func (t *E2BTerminal) Output() <-chan []byte           { return t.output }
func (t *E2BTerminal) Write(data []byte) error         { return t.handle.SendStdin(data) }
func (t *E2BTerminal) SignalTerminate() error          { return t.handle.Kill() }
func (t *E2BTerminal) Wait() (E2BCommandResult, error) { <-t.done; return t.result, t.err }
func (t *E2BTerminal) Close() error                    { return t.handle.Disconnect() }
