package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type E2BCommandError struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

func (e *E2BCommandError) Error() string {
	return fmt.Sprintf("command exited with code %d: %s", e.ExitCode, strings.TrimSpace(e.Stderr))
}

type LocalSandboxFactory struct{ Root string }

func NewLocalSandboxFactory(root string) E2BSandboxFactory {
	configuredRoot := root
	return func(ctx context.Context, config E2BConfig) (E2BSandbox, error) {
		actualRoot := configuredRoot
		cleanup := actualRoot == "" && config.LocalRoot == ""
		if actualRoot == "" {
			actualRoot = config.LocalRoot
		}
		if actualRoot == "" {
			var err error
			actualRoot, err = os.MkdirTemp("", "dsh-e2b-")
			if err != nil {
				return nil, err
			}
		}
		actualRoot, err := filepath.Abs(actualRoot)
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(actualRoot, 0o700); err != nil {
			return nil, err
		}
		sandbox := newLocalSandbox(actualRoot)
		sandbox.cleanup = cleanup
		return sandbox, nil
	}
}

func (f LocalSandboxFactory) New(ctx context.Context, config E2BConfig) (E2BSandbox, error) {
	return NewLocalSandboxFactory(f.Root)(ctx, config)
}

type localSandbox struct {
	root     string
	files    *localFiles
	commands *localCommands
	pty      E2BPTY
	dead     chan struct{}
	cleanup  bool
}

func newLocalSandbox(root string) *localSandbox {
	s := &localSandbox{root: root, dead: make(chan struct{})}
	s.files = &localFiles{sandbox: s, metadata: map[string]map[string]string{}}
	s.commands = &localCommands{sandbox: s}
	s.pty = newLocalPTY(s)
	return s
}

func (s *localSandbox) ID() string            { return s.root }
func (s *localSandbox) Files() E2BFiles       { return s.files }
func (s *localSandbox) Commands() E2BCommands { return s.commands }
func (s *localSandbox) PTY() E2BPTY           { return s.pty }
func (s *localSandbox) Kill(context.Context) error {
	select {
	case <-s.dead:
		return nil
	default:
		close(s.dead)
	}
	if s.cleanup {
		return os.RemoveAll(s.root)
	}
	return nil
}

func (s *localSandbox) path(remote string) string {
	remote = filepath.ToSlash(remote)
	if !strings.HasPrefix(remote, "/") {
		remote = "/" + remote
	}
	return filepath.Join(s.root, filepath.FromSlash(strings.TrimPrefix(filepath.Clean(remote), "/")))
}

type localFiles struct {
	sandbox  *localSandbox
	mu       sync.Mutex
	metadata map[string]map[string]string
}

func (f *localFiles) Read(_ context.Context, remote string) ([]byte, error) {
	return os.ReadFile(f.sandbox.path(remote))
}

func (f *localFiles) Write(_ context.Context, remote string, data []byte, metadata map[string]string) (E2BEntryInfo, error) {
	path := f.sandbox.path(remote)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return E2BEntryInfo{}, err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return E2BEntryInfo{}, err
	}
	f.mu.Lock()
	f.metadata[filepath.Clean(path)] = cloneE2BMap(metadata)
	f.mu.Unlock()
	return f.GetInfo(context.Background(), remote)
}

func (f *localFiles) List(_ context.Context, remote string, depth int) ([]E2BEntryInfo, error) {
	root := f.sandbox.path(remote)
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("not a directory")
	}
	entries := []E2BEntryInfo{}
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if depth > 0 && strings.Count(filepath.ToSlash(rel), "/") >= depth {
			return filepath.SkipDir
		}
		entry, err := f.info(path, info)
		if err != nil {
			return err
		}
		entries = append(entries, entry)
		return nil
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, err
}

func (f *localFiles) MakeDir(_ context.Context, remote string) error {
	return os.MkdirAll(f.sandbox.path(remote), 0o755)
}
func (f *localFiles) Rename(_ context.Context, oldRemote, newRemote string) (E2BEntryInfo, error) {
	oldPath, newPath := f.sandbox.path(oldRemote), f.sandbox.path(newRemote)
	if err := os.MkdirAll(filepath.Dir(newPath), 0o755); err != nil {
		return E2BEntryInfo{}, err
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		return E2BEntryInfo{}, err
	}
	return f.GetInfo(context.Background(), newRemote)
}
func (f *localFiles) Remove(_ context.Context, remote string) error {
	return os.RemoveAll(f.sandbox.path(remote))
}
func (f *localFiles) GetInfo(_ context.Context, remote string) (E2BEntryInfo, error) {
	path := f.sandbox.path(remote)
	info, err := os.Lstat(path)
	if err != nil {
		return E2BEntryInfo{}, err
	}
	return f.info(path, info)
}

func (f *localFiles) info(path string, info os.FileInfo) (E2BEntryInfo, error) {
	relative, _ := filepath.Rel(f.sandbox.root, path)
	remote := "/" + filepath.ToSlash(relative)
	if relative == "." {
		remote = "/"
	}
	typeID := E2BFile
	target := ""
	if info.IsDir() {
		typeID = E2BDir
	} else if info.Mode()&os.ModeSymlink != 0 {
		typeID = E2BSymlink
		target, _ = os.Readlink(path)
	}
	f.mu.Lock()
	metadata := cloneE2BMap(f.metadata[filepath.Clean(path)])
	f.mu.Unlock()
	return E2BEntryInfo{Name: info.Name(), Path: remote, Type: typeID, Size: info.Size(), Mode: info.Mode(), ModifiedTime: info.ModTime(), SymlinkTarget: target, Metadata: metadata}, nil
}

type localCommands struct{ sandbox *localSandbox }

func (c *localCommands) Run(ctx context.Context, command string, options E2BCommandOptions) (E2BCommandResult, error) {
	program, args, err := localCommandInvocation(command)
	if err != nil {
		return E2BCommandResult{}, err
	}
	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Dir = c.sandbox.path(defaultRemoteCWD(options.CWD))
	cmd.Env = localEnv(options.Env)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	result := E2BCommandResult{ExitCode: 0, Stdout: stdout.String(), Stderr: stderr.String()}
	if err != nil {
		result.ExitCode = exitCode(err)
		return result, &E2BCommandError{ExitCode: result.ExitCode, Stdout: result.Stdout, Stderr: result.Stderr}
	}
	return result, nil
}

func (c *localCommands) Start(ctx context.Context, argv []string, options E2BCommandOptions, onStdout, onStderr func([]byte)) (E2BCommandHandle, error) {
	if len(argv) == 0 {
		return nil, errors.New("empty argv")
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = c.sandbox.path(defaultRemoteCWD(options.CWD))
	cmd.Env = localEnv(options.Env)
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	errout, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	h := &localCommandHandle{cmd: cmd, stdin: in, result: make(chan commandResult, 1), stdoutDone: make(chan struct{}), stderrDone: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		b := copyBytes(out)
		if onStdout != nil {
			onStdout(b)
		}
		close(h.stdoutDone)
	}()
	go func() {
		b := copyBytes(errout)
		if onStderr != nil {
			onStderr(b)
		}
		close(h.stderrDone)
	}()
	go func() {
		err := cmd.Wait()
		result := E2BCommandResult{ExitCode: exitCode(err)}
		if err == nil {
			result.ExitCode = 0
		}
		h.result <- commandResult{result: result, err: err}
	}()
	return h, nil
}

func defaultRemoteCWD(value string) string {
	if value == "" {
		return "/home/user/workspace"
	}
	return value
}
func localEnv(overrides map[string]string) []string {
	env := os.Environ()
	if len(overrides) == 0 {
		return env
	}
	values := map[string]string{}
	for _, item := range env {
		if i := strings.IndexByte(item, '='); i > 0 {
			values[item[:i]] = item[i+1:]
		}
	}
	for k, v := range overrides {
		values[k] = v
	}
	out := make([]string, 0, len(values))
	for k, v := range values {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

type commandResult struct {
	result E2BCommandResult
	err    error
}
type localCommandHandle struct {
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	result     chan commandResult
	once       sync.Once
	waitOnce   sync.Once
	waited     commandResult
	stdoutDone chan struct{}
	stderrDone chan struct{}
}

func (h *localCommandHandle) PID() int {
	if h.cmd.Process == nil {
		return -1
	}
	return h.cmd.Process.Pid
}
func (h *localCommandHandle) Wait() (E2BCommandResult, error) {
	h.waitOnce.Do(func() {
		h.waited = <-h.result
		if h.stdoutDone != nil {
			<-h.stdoutDone
		}
		if h.stderrDone != nil {
			<-h.stderrDone
		}
	})
	r := h.waited
	if r.err == nil {
		return r.result, nil
	}
	r.result.ExitCode = exitCode(r.err)
	return r.result, &E2BCommandError{ExitCode: r.result.ExitCode}
}
func (h *localCommandHandle) SendStdin(data []byte) error { _, err := h.stdin.Write(data); return err }
func (h *localCommandHandle) CloseStdin() error           { return h.stdin.Close() }
func (h *localCommandHandle) Kill() error {
	h.once.Do(func() {
		if h.cmd.Process != nil {
			_ = h.cmd.Process.Kill()
		}
	})
	return nil
}
func (h *localCommandHandle) Disconnect() error { return nil }

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var e *exec.ExitError
	if errors.As(err, &e) {
		if status, ok := e.Sys().(interface{ ExitStatus() int }); ok {
			return status.ExitStatus()
		}
	}
	return 1
}
func cloneE2BMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for k, v := range values {
		out[k] = v
	}
	return out
}
