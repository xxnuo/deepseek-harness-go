package harness

// These interfaces keep E2B available as a Go library while allowing callers
// to replace the official cloud transport or use a local sandbox.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type E2BFileType string

const (
	E2BFile    E2BFileType = "file"
	E2BDir     E2BFileType = "directory"
	E2BSymlink E2BFileType = "symlink"
)

type E2BEntryInfo struct {
	Name          string
	Path          string
	Type          E2BFileType
	Size          int64
	Mode          os.FileMode
	ModifiedTime  time.Time
	SymlinkTarget string
	Metadata      map[string]string
}

type E2BCommandOptions struct {
	Context context.Context
	CWD     string
	Env     map[string]string
	Stdin   bool
}

type E2BCommandResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

type E2BCommandHandle interface {
	PID() int
	Wait() (E2BCommandResult, error)
	SendStdin([]byte) error
	CloseStdin() error
	Kill() error
	Disconnect() error
}

type E2BCommands interface {
	Run(context.Context, string, E2BCommandOptions) (E2BCommandResult, error)
	Start(context.Context, []string, E2BCommandOptions, func([]byte), func([]byte)) (E2BCommandHandle, error)
}

type E2BFiles interface {
	Read(context.Context, string) ([]byte, error)
	Write(context.Context, string, []byte, map[string]string) (E2BEntryInfo, error)
	List(context.Context, string, int) ([]E2BEntryInfo, error)
	MakeDir(context.Context, string) error
	Rename(context.Context, string, string) (E2BEntryInfo, error)
	Remove(context.Context, string) error
	GetInfo(context.Context, string) (E2BEntryInfo, error)
}

type E2BPTY interface {
	Create(context.Context, E2BPTYOptions, func([]byte)) (E2BCommandHandle, error)
	SendInput(context.Context, int, []byte) error
}

type E2BPTYOptions struct {
	Rows, Cols int
	CWD        string
	Env        map[string]string
}

type E2BSandbox interface {
	ID() string
	Files() E2BFiles
	Commands() E2BCommands
	PTY() E2BPTY
	Kill(context.Context) error
}

type E2BSandboxFactory func(context.Context, E2BConfig) (E2BSandbox, error)

type E2BConfig struct {
	APIKey     string
	APIURL     string
	SandboxURL string
	CWD        string
	Timeout    time.Duration
	Factory    E2BSandboxFactory
	LocalRoot  string
}

func (c E2BConfig) normalized() (E2BConfig, error) {
	if c.APIKey == "" {
		c.APIKey = os.Getenv("E2B_API_KEY")
	}
	if c.APIURL == "" {
		c.APIURL = os.Getenv("E2B_API_URL")
	}
	if c.APIURL == "" {
		c.APIURL = "https://api.e2b.app"
	}
	if c.SandboxURL == "" {
		c.SandboxURL = os.Getenv("E2B_SANDBOX_URL")
	}
	if c.CWD == "" {
		c.CWD = "/home/user/workspace"
	}
	if !strings.HasPrefix(c.CWD, "/") {
		return E2BConfig{}, fmt.Errorf("dsh-e2b: cwd must be an absolute Linux path: %s", c.CWD)
	}
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Minute
	}
	if c.Timeout <= 0 {
		return E2BConfig{}, errors.New("dsh-e2b: timeout must be positive")
	}
	if c.Factory == nil && c.LocalRoot == "" && c.APIKey == "" {
		return E2BConfig{}, errors.New("dsh-e2b: configure apiKey or set E2B_API_KEY")
	}
	return c, nil
}

type E2BRuntime struct {
	CWD         string
	RuntimeRoot string
	config      E2BConfig
	ready       chan struct{}
	readyErr    error
	sandbox     E2BSandbox
	disposed    bool
	mu          sync.Mutex
}

func NewE2BRuntime(ctx context.Context, config E2BConfig) (*E2BRuntime, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	r := &E2BRuntime{CWD: config.CWD, RuntimeRoot: filepath.ToSlash(filepath.Join(config.CWD, ".dsh-e2b")), config: config, ready: make(chan struct{})}
	go r.open(ctx)
	return r, nil
}

func (r *E2BRuntime) open(ctx context.Context) {
	var sandbox E2BSandbox
	if r.config.Factory != nil {
		sandbox, r.readyErr = r.config.Factory(ctx, r.config)
	} else if r.config.LocalRoot != "" {
		sandbox, r.readyErr = NewLocalSandboxFactory(r.config.LocalRoot)(ctx, r.config)
	} else {
		sandbox, r.readyErr = NewE2BCloudSandboxFactory()(ctx, r.config)
	}
	if r.readyErr == nil {
		r.sandbox = sandbox
		if err := sandbox.Files().MakeDir(ctx, r.CWD); err != nil {
			r.readyErr = err
		} else if err := sandbox.Files().MakeDir(ctx, r.RuntimeRoot); err != nil {
			r.readyErr = err
		}
		if r.readyErr != nil {
			_ = sandbox.Kill(context.Background())
		}
	}
	close(r.ready)
}

func (r *E2BRuntime) GetSandbox(ctx context.Context) (E2BSandbox, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.ready:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.disposed {
		return nil, errors.New("E2B sandbox service is disposing")
	}
	if r.readyErr != nil {
		return nil, r.readyErr
	}
	return r.sandbox, nil
}

func (r *E2BRuntime) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.disposed {
		r.mu.Unlock()
		return nil
	}
	r.disposed = true
	sandbox := r.sandbox
	r.mu.Unlock()
	if sandbox == nil {
		<-r.ready
		r.mu.Lock()
		sandbox, r.sandbox = r.sandbox, nil
		err := r.readyErr
		r.mu.Unlock()
		if err != nil || sandbox == nil {
			return err
		}
	}
	return sandbox.Kill(ctx)
}

func e2bQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }

func e2bControlEnvs(overrides map[string]string) map[string]string {
	out := make(map[string]string, len(overrides)+1)
	for k, v := range overrides {
		out[k] = v
	}
	out["HOME"] = fmt.Sprintf("/.dsh-e2b-control-%d", time.Now().UnixNano())
	return out
}

func copyBytes(r io.Reader) []byte {
	b, _ := io.ReadAll(r)
	return b
}
