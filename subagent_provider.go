package harness

import (
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
	"time"
	"unicode/utf8"
)

const (
	maxSubagentDiagnosticBytes = 4096
	diagnosticTruncationSuffix = "\n[diagnostic truncated]"
)

// SubagentStopReason is the provider-independent terminal state of one
// external subagent run.
type SubagentStopReason string

const (
	SubagentCompleted SubagentStopReason = "completed"
	SubagentMaxTokens SubagentStopReason = "max-tokens"
	SubagentRefusal   SubagentStopReason = "refusal"
	SubagentAborted   SubagentStopReason = "aborted"
	SubagentError     SubagentStopReason = "error"
)

// SubagentCapabilities declares which parent-enforced start options a
// provider can honor.
type SubagentCapabilities struct {
	OutputSchema bool `json:"outputSchema"`
	DepthLimit   bool `json:"depthLimit"`
	ToolFilter   bool `json:"toolFilter"`
	Persona      bool `json:"persona"`
}

// NoSubagentStartCapabilities returns the capability set shared by
// out-of-process providers.
func NoSubagentStartCapabilities() SubagentCapabilities { return SubagentCapabilities{} }

// SubagentStartRequest describes one one-shot external child run. CWD may be
// omitted when ParentSessionID names an Engine session with a workspace.
type SubagentStartRequest struct {
	ParentSessionID string
	CWD             string
	Prompt          []ContentBlock
	OutputSchema    map[string]any
	MaxDepth        *int
	AgentOptions    *SubagentAgentOptions
	ToolFilter      *SubagentToolFilter
	Persona         string
}

// SubagentAgentOptions selects child model defaults for providers that expose
// the corresponding start capability.
type SubagentAgentOptions struct {
	Provider  string `json:"provider,omitempty"`
	Model     string `json:"model,omitempty"`
	MaxTokens int    `json:"maxTokens,omitempty"`
}

// SubagentToolFilter limits the child tool catalog for capable providers.
type SubagentToolFilter struct {
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
}

// SubagentResult is the terminal output of one published run.
type SubagentResult struct {
	Output     []ContentBlock     `json:"output"`
	Diagnostic string             `json:"diagnostic,omitempty"`
	StopReason SubagentStopReason `json:"stopReason"`
}

// SubagentProvider starts one-shot children. Startup errors are returned from
// Start; after publication a run always settles to a SubagentResult.
type SubagentProvider interface {
	Name() string
	Capabilities() SubagentCapabilities
	InheritsParentContext() bool
	Start(context.Context, SubagentStartRequest) (*SubagentRun, error)
}

// SubagentProviderInfo is the stable library-facing provider descriptor.
type SubagentProviderInfo struct {
	Name                  string               `json:"name"`
	Capabilities          SubagentCapabilities `json:"capabilities"`
	InheritsParentContext bool                 `json:"inheritsParentContext"`
}

// SubagentRun is one published asynchronous external child.
type SubagentRun struct {
	ID string

	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	result SubagentResult

	dispose     func() error
	disposeOnce sync.Once
	disposeDone chan struct{}
	disposeErr  error
}

func newSubagentRun(id string, cancel context.CancelFunc, dispose func() error) *SubagentRun {
	return &SubagentRun{ID: id, cancel: cancel, done: make(chan struct{}), dispose: dispose, disposeDone: make(chan struct{})}
}

func (r *SubagentRun) settle(result SubagentResult) {
	r.mu.Lock()
	select {
	case <-r.done:
		r.mu.Unlock()
		return
	default:
	}
	r.result = cloneSubagentResult(result)
	close(r.done)
	r.mu.Unlock()
}

// Done closes when the run has a terminal provider result.
func (r *SubagentRun) Done() <-chan struct{} { return r.done }

// Wait returns the published result or the caller's wait-context error. A
// wait timeout does not cancel the child; call Cancel or Dispose explicitly.
func (r *SubagentRun) Wait(ctx context.Context) (SubagentResult, error) {
	select {
	case <-ctx.Done():
		return SubagentResult{}, ctx.Err()
	case <-r.done:
		r.mu.Lock()
		result := cloneSubagentResult(r.result)
		r.mu.Unlock()
		return result, nil
	}
}

// Cancel requests local run cancellation. Providers do not depend on a
// cooperative child for terminal settlement.
func (r *SubagentRun) Cancel() {
	if r != nil && r.cancel != nil {
		r.cancel()
	}
}

// Dispose cancels the run and tears its child process down to quiescence.
// It is idempotent and safe for concurrent callers.
func (r *SubagentRun) Dispose() error {
	if r == nil {
		return nil
	}
	r.disposeOnce.Do(func() {
		r.Cancel()
		if r.dispose != nil {
			r.disposeErr = r.dispose()
		}
		close(r.disposeDone)
	})
	<-r.disposeDone
	return r.disposeErr
}

// Close implements io.Closer as an alias for Dispose.
func (r *SubagentRun) Close() error { return r.Dispose() }

func cloneSubagentResult(result SubagentResult) SubagentResult {
	result.Output = append([]ContentBlock(nil), result.Output...)
	result.Diagnostic = limitSubagentDiagnostic(result.Diagnostic)
	return result
}

func limitSubagentDiagnostic(diagnostic string) string {
	diagnostic = strings.ToValidUTF8(diagnostic, "\uFFFD")
	if len(diagnostic) <= maxSubagentDiagnosticBytes {
		return diagnostic
	}
	limit := maxSubagentDiagnosticBytes - len(diagnosticTruncationSuffix)
	for limit > 0 && !utf8.RuneStart(diagnostic[limit]) {
		limit--
	}
	return diagnostic[:limit] + diagnosticTruncationSuffix
}

// RegisterSubagentProvider installs a provider for programmatic consumers.
// Duplicate names are rejected rather than silently replacing live behavior.
func (e *Engine) RegisterSubagentProvider(provider SubagentProvider) error {
	if provider == nil {
		return errors.New("subagent provider is required")
	}
	name := strings.TrimSpace(provider.Name())
	if name == "" {
		return errors.New("subagent provider name is required")
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errors.New("engine is closed")
	}
	if e.subagentProviders == nil {
		e.subagentProviders = map[string]SubagentProvider{}
	}
	if _, exists := e.subagentProviders[name]; exists {
		e.mu.Unlock()
		return fmt.Errorf("subagent provider already registered: %s", name)
	}
	e.subagentProviders[name] = provider
	e.mu.Unlock()
	return nil
}

// UnregisterSubagentProvider removes a provider for future starts. Published
// runs retain their provider-owned process and disposal path.
func (e *Engine) UnregisterSubagentProvider(name string) bool {
	e.mu.Lock()
	name = strings.TrimSpace(name)
	if _, ok := e.subagentProviders[name]; !ok {
		e.mu.Unlock()
		return false
	}
	delete(e.subagentProviders, name)
	e.mu.Unlock()
	e.emitDynamicCordisScopedContained("", "subagent/provider-removed", name)
	return true
}

// ListSubagentProviders returns deterministic descriptors for custom CLIs and
// frontends.
func (e *Engine) ListSubagentProviders() []SubagentProviderInfo {
	e.mu.RLock()
	rows := make([]SubagentProviderInfo, 0, len(e.subagentProviders))
	for name, provider := range e.subagentProviders {
		rows = append(rows, SubagentProviderInfo{
			Name: name, Capabilities: provider.Capabilities(),
			InheritsParentContext: provider.InheritsParentContext(),
		})
	}
	e.mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows
}

// StartSubagent starts a registered one-shot provider. The returned run is
// caller-owned and must be disposed.
func (e *Engine) StartSubagent(ctx context.Context, providerName string, request SubagentStartRequest) (*SubagentRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.RLock()
	closed := e.closed
	provider := e.subagentProviders[strings.TrimSpace(providerName)]
	e.mu.RUnlock()
	if closed {
		return nil, errors.New("engine is closed")
	}
	if provider == nil {
		return nil, fmt.Errorf("subagent provider not found: %s", providerName)
	}
	if err := validateSubagentCapabilities(provider, request); err != nil {
		return nil, err
	}
	if request.CWD == "" && request.ParentSessionID != "" {
		parent, err := e.getSession(request.ParentSessionID)
		if err != nil {
			return nil, err
		}
		parent.mu.Lock()
		request.CWD = parent.Header.CWD
		parent.mu.Unlock()
	}
	request.Prompt = append([]ContentBlock(nil), request.Prompt...)
	run, err := provider.Start(ctx, request)
	if err != nil {
		return nil, err
	}
	if run == nil || strings.TrimSpace(run.ID) == "" || run.done == nil {
		return nil, errors.New("subagent provider returned an invalid run")
	}
	identity := map[string]any{
		"runId": newRunID(), "provider": strings.TrimSpace(provider.Name()), "id": run.ID, "local": false,
	}
	e.emitDynamicCordisScopedContained(request.ParentSessionID, "subagent/start", identity)
	go func() {
		<-run.Done()
		result, _ := run.Wait(context.Background())
		terminal := map[string]any{
			"runId": identity["runId"], "provider": identity["provider"], "id": run.ID,
			"local": false, "stopReason": result.StopReason,
		}
		if len(result.Output) > 0 {
			terminal["lastAssistantMessage"] = result.Output
		}
		e.emitDynamicCordisScopedContained(request.ParentSessionID, "subagent/end", terminal)
	}()
	return run, nil
}

func validateSubagentCapabilities(provider SubagentProvider, request SubagentStartRequest) error {
	capabilities := provider.Capabilities()
	if request.OutputSchema != nil && !capabilities.OutputSchema {
		return fmt.Errorf("subagent provider %q does not support outputSchema", provider.Name())
	}
	if request.MaxDepth != nil && !capabilities.DepthLimit {
		return fmt.Errorf("subagent provider %q does not support maxDepth", provider.Name())
	}
	if request.ToolFilter != nil && !capabilities.ToolFilter {
		return fmt.Errorf("subagent provider %q does not support toolFilter", provider.Name())
	}
	if request.Persona != "" && !capabilities.Persona {
		return fmt.Errorf("subagent provider %q does not support persona", provider.Name())
	}
	return nil
}

func validateSubagentCWD(prefix, cwd string) (string, error) {
	if cwd == "" {
		return "", fmt.Errorf("%s: no working directory for the child", prefix)
	}
	if !filepath.IsAbs(cwd) {
		return "", fmt.Errorf("%s: working directory must be an absolute path: %s", prefix, cwd)
	}
	info, err := os.Stat(cwd)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("%s: working directory is not an accessible directory: %s", prefix, cwd)
	}
	return cwd, nil
}

func validateConfiguredSubagentCWD(prefix, cwd string) (string, error) {
	if cwd == "" {
		return "", nil
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", fmt.Errorf("%s: config cwd is invalid: %w", prefix, err)
	}
	return validateSubagentCWD(prefix, abs)
}

func positiveSubagentDuration(prefix, name string, value, fallback time.Duration) (time.Duration, error) {
	if value == 0 {
		value = fallback
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s: %s must be positive", prefix, name)
	}
	return value, nil
}

type subagentProcessResult struct {
	exitCode int
	err      error
}

type subagentProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser

	done chan struct{}
	mu   sync.Mutex
	res  subagentProcessResult
}

func startSubagentProcess(command string, args []string, cwd string, env map[string]string) (*subagentProcess, error) {
	cmd := exec.Command(command, args...)
	cmd.Dir = cwd
	cmd.Env = scrubbedChildEnv(env)
	cmd.Stderr = os.Stderr
	configureChildProcess(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	cmd.Stdout = stdoutWriter
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		return nil, err
	}
	process := &subagentProcess{cmd: cmd, stdin: stdin, stdout: stdout, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		exitCode := -1
		if cmd.ProcessState != nil {
			exitCode = cmd.ProcessState.ExitCode()
		}
		// Close only the explicit pipe write end. The consumer can then drain
		// buffered stream-json bytes before observing EOF on the read end.
		_ = stdoutWriter.Close()
		process.mu.Lock()
		process.res = subagentProcessResult{exitCode: exitCode, err: err}
		close(process.done)
		process.mu.Unlock()
	}()
	return process, nil
}

func (p *subagentProcess) result() subagentProcessResult {
	<-p.done
	p.mu.Lock()
	result := p.res
	p.mu.Unlock()
	return result
}

func (p *subagentProcess) waitWithin(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return true
	case <-timer.C:
		return false
	}
}

func (p *subagentProcess) closeInput() {
	if p != nil && p.stdin != nil {
		_ = p.stdin.Close()
	}
}

func (p *subagentProcess) dispose(eofGrace, terminateGrace time.Duration) error {
	if p == nil {
		return nil
	}
	p.closeInput()
	if eofGrace > 0 && p.waitWithin(eofGrace) {
		return nil
	}
	select {
	case <-p.done:
		return nil
	default:
	}
	_ = terminateChildProcess(p.cmd)
	if p.waitWithin(terminateGrace) {
		return nil
	}
	if err := killChildProcess(p.cmd); err != nil {
		return err
	}
	if !p.waitWithin(terminateGrace) {
		return fmt.Errorf("subagent process did not exit within %s after forced termination", terminateGrace)
	}
	return nil
}

func newRunID() string { return newID("subagent-run") }
