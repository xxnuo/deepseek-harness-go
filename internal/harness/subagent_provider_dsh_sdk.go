package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	defaultSDKShutdownTimeout = time.Second
	defaultSDKDisposeEOFGrace = 6 * time.Second
	defaultSDKDisposeGrace    = 3 * time.Second
)

// DSHSDKSubagentConfig configures a complete child Harness runtime driven by
// the stdio SDK protocol.
type DSHSDKSubagentConfig struct {
	ProviderName    string
	Command         string
	Args            []string
	CWD             string
	Provider        string
	Model           string
	MaxTokens       int
	Env             map[string]string
	ShutdownTimeout time.Duration
	DisposeEOFGrace time.Duration
	DisposeGrace    time.Duration
}

// DSHSDKSubagentProvider drives one fresh SDK runtime process per run.
type DSHSDKSubagentProvider struct {
	name            string
	command         string
	args            []string
	cwd             string
	provider        string
	model           string
	maxTokens       int
	env             map[string]string
	shutdownTimeout time.Duration
	disposeEOFGrace time.Duration
	disposeGrace    time.Duration
}

func NewDSHSDKSubagentProvider(config DSHSDKSubagentConfig) (*DSHSDKSubagentProvider, error) {
	name := strings.TrimSpace(config.ProviderName)
	if name == "" {
		name = "dsh-sdk"
	}
	command := strings.TrimSpace(config.Command)
	if command == "" {
		return nil, errors.New("subagent-dsh-sdk: command is required")
	}
	provider := strings.TrimSpace(config.Provider)
	if provider == "" {
		provider = "deepseek-official"
	}
	model := strings.TrimSpace(config.Model)
	if model == "" {
		model = "deepseek-v4-flash"
	}
	if config.MaxTokens < 0 || int64(config.MaxTokens) > maxJSONSafeInteger {
		return nil, errors.New("subagent-dsh-sdk: maxTokens must be a positive safe integer")
	}
	cwd, err := validateConfiguredSubagentCWD("subagent-dsh-sdk", config.CWD)
	if err != nil {
		return nil, err
	}
	shutdown, err := positiveSubagentDuration("subagent-dsh-sdk", "shutdownTimeout", config.ShutdownTimeout, defaultSDKShutdownTimeout)
	if err != nil {
		return nil, err
	}
	eofGrace, err := positiveSubagentDuration("subagent-dsh-sdk", "disposeEOFGrace", config.DisposeEOFGrace, defaultSDKDisposeEOFGrace)
	if err != nil {
		return nil, err
	}
	disposeGrace, err := positiveSubagentDuration("subagent-dsh-sdk", "disposeGrace", config.DisposeGrace, defaultSDKDisposeGrace)
	if err != nil {
		return nil, err
	}
	return &DSHSDKSubagentProvider{
		name: name, command: command, args: append([]string(nil), config.Args...), cwd: cwd,
		provider: provider, model: model, maxTokens: config.MaxTokens, env: cloneSubagentEnv(config.Env),
		shutdownTimeout: shutdown, disposeEOFGrace: eofGrace, disposeGrace: disposeGrace,
	}, nil
}

func (p *DSHSDKSubagentProvider) Name() string { return p.name }
func (p *DSHSDKSubagentProvider) Capabilities() SubagentCapabilities {
	return SubagentCapabilities{AgentOptions: true}
}
func (p *DSHSDKSubagentProvider) InheritsParentContext() bool { return false }
func (p *DSHSDKSubagentProvider) AgentRouteDefaults() SubagentAgentOptions {
	return SubagentAgentOptions{Provider: p.provider, Model: p.model}
}

func (p *DSHSDKSubagentProvider) Start(ctx context.Context, request SubagentStartRequest) (*SubagentRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.New("subagent request was aborted before the SDK child started")
	}
	cwd := p.cwd
	if cwd == "" {
		cwd = request.CWD
	}
	var err error
	if cwd, err = validateSubagentCWD("subagent-dsh-sdk", cwd); err != nil {
		return nil, err
	}
	process, err := startSubagentProcessWithStderr(p.command, p.args, cwd, p.env, true)
	if err != nil {
		return nil, fmt.Errorf("subagent-dsh-sdk: start child: %w", err)
	}
	process.startStderrCapture()
	state := newSDKSubagentState()
	rpc := newSubagentRPCClient(process.stdout, process.stdin)
	rpc.setHandlers(nil, state.notify)
	rpc.start()
	startupDispose := func() error {
		rpc.close()
		return process.dispose(p.disposeEOFGrace, p.disposeGrace)
	}
	route := SubagentAgentOptions{Provider: p.provider, Model: p.model, MaxTokens: p.maxTokens}
	if request.AgentOptions != nil {
		if request.AgentOptions.Provider != "" {
			route.Provider = request.AgentOptions.Provider
		}
		if request.AgentOptions.Model != "" {
			route.Model = request.AgentOptions.Model
		}
		if request.AgentOptions.ReasoningEffort != "" {
			route.ReasoningEffort = request.AgentOptions.ReasoningEffort
		}
		if request.AgentOptions.MaxTokens != 0 {
			if request.AgentOptions.MaxTokens < 0 || int64(request.AgentOptions.MaxTokens) > maxJSONSafeInteger {
				_ = startupDispose()
				return nil, errors.New("subagent-dsh-sdk: agentOptions.maxTokens must be a positive safe integer")
			}
			route.MaxTokens = request.AgentOptions.MaxTokens
		}
	}
	initialize := map[string]any{"cwd": cwd, "provider": route.Provider, "model": route.Model}
	if route.ReasoningEffort != "" {
		initialize["reasoningEffort"] = route.ReasoningEffort
	}
	if route.MaxTokens > 0 {
		initialize["maxTokens"] = route.MaxTokens
	}
	var initialized struct {
		ServerInfo struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"serverInfo"`
	}
	if err := rpc.request(ctx, "initialize", initialize, &initialized); err != nil {
		_ = startupDispose()
		if ctx.Err() != nil {
			return nil, errors.New("subagent request was aborted before the SDK child started")
		}
		return nil, fmt.Errorf("subagent-dsh-sdk: initialize: %w", sdkProcessError(err, process))
	}
	if ctx.Err() != nil {
		_ = startupDispose()
		return nil, errors.New("subagent request was aborted before the SDK child started")
	}
	if initialized.ServerInfo.Name == "" || initialized.ServerInfo.Version == "" {
		_ = startupDispose()
		return nil, errors.New("subagent-dsh-sdk: initialize returned no server identity")
	}

	childSessionID := newID("session")
	state.mu.Lock()
	state.sessionID = childSessionID
	state.mu.Unlock()
	runCtx, runCancel := context.WithCancel(ctx)
	var disposeOnce sync.Once
	var disposeErr error
	disposeDone := make(chan struct{})
	dispose := func() error {
		disposeOnce.Do(func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), p.shutdownTimeout)
			var ignored map[string]any
			_ = rpc.request(shutdownCtx, "shutdown", map[string]any{}, &ignored)
			cancel()
			rpc.close()
			disposeErr = process.dispose(p.disposeEOFGrace, p.disposeGrace)
			close(disposeDone)
		})
		<-disposeDone
		return disposeErr
	}
	run := newSubagentRun(newRunID(), runCancel, dispose)
	go func() {
		var response struct {
			MessageID string `json:"messageId"`
		}
		err := rpc.request(runCtx, "session/prompt", map[string]any{
			"sessionId": childSessionID, "contentBlocks": request.Prompt,
		}, &response)
		if runCtx.Err() != nil {
			run.settle(SubagentResult{Output: state.output(response.MessageID), StopReason: SubagentAborted})
			return
		}
		if err != nil || response.MessageID == "" {
			diagnostic := ""
			if err != nil {
				diagnostic = sdkProcessError(err, process).Error()
			}
			run.settle(SubagentResult{Output: state.output(""), Diagnostic: diagnostic, StopReason: SubagentError})
			return
		}
		result, err := state.wait(runCtx, rpc, response.MessageID)
		if runCtx.Err() != nil {
			run.settle(SubagentResult{Output: state.output(response.MessageID), StopReason: SubagentAborted})
			return
		}
		if err != nil {
			diagnostic := sdkProcessError(err, process).Error()
			run.settle(SubagentResult{Output: state.output(response.MessageID), Diagnostic: diagnostic, StopReason: SubagentError})
			return
		}
		run.settle(result)
	}()
	return run, nil
}

func sdkProcessError(err error, process *subagentProcess) error {
	if process == nil || (!errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe)) {
		return err
	}
	parts := []string{err.Error()}
	select {
	case <-process.done:
		outcome := process.result()
		if outcome.exitCode >= 0 {
			parts = append(parts, fmt.Sprintf("exit code: %d", outcome.exitCode))
		}
		if outcome.signal != "" {
			parts = append(parts, "signal: "+outcome.signal)
		}
	case <-time.After(100 * time.Millisecond):
	}
	if stderr := process.stderrDiagnostic(100 * time.Millisecond); stderr != "" {
		parts = append(parts, "stderr tail: "+stderr)
	}
	return errors.New(strings.Join(parts, "; "))
}

type sdkSubagentNotification struct {
	method string
	params map[string]any
}

type sdkSubagentState struct {
	mu            sync.Mutex
	sessionID     string
	notifications []sdkSubagentNotification
	changed       chan struct{}
}

func newSDKSubagentState() *sdkSubagentState {
	return &sdkSubagentState{changed: make(chan struct{})}
}

func (s *sdkSubagentState) notify(method string, raw json.RawMessage) error {
	var params map[string]any
	if err := decodeSubagentRPCParams(raw, &params); err != nil {
		return err
	}
	s.mu.Lock()
	if sessionID, _ := params["sessionId"].(string); sessionID == s.sessionID {
		s.notifications = append(s.notifications, sdkSubagentNotification{method: method, params: params})
		close(s.changed)
		s.changed = make(chan struct{})
	}
	s.mu.Unlock()
	return nil
}

func (s *sdkSubagentState) wait(ctx context.Context, rpc *subagentRPCClient, messageID string) (SubagentResult, error) {
	for {
		if result, ok := s.result(messageID); ok {
			return result, nil
		}
		s.mu.Lock()
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return SubagentResult{}, ctx.Err()
		case <-rpc.done:
			return SubagentResult{}, rpc.waitError()
		case <-changed:
		}
	}
}

func (s *sdkSubagentState) result(messageID string) (SubagentResult, bool) {
	s.mu.Lock()
	notifications := append([]sdkSubagentNotification(nil), s.notifications...)
	s.mu.Unlock()
	receipt := -1
	for index, notification := range notifications {
		if notification.method == "session.event" && sdkInboxReceipt(notification.params["event"], messageID) {
			receipt = index
			break
		}
	}
	if receipt < 0 {
		return SubagentResult{}, false
	}
	for _, notification := range notifications[receipt:] {
		if notification.method == "session.status" && notification.params["status"] == "idle" {
			return SubagentResult{Output: sdkOutput(notifications[receipt:]), StopReason: sdkStopReason(notifications[receipt:])}, true
		}
	}
	return SubagentResult{}, false
}

func (s *sdkSubagentState) output(messageID string) []ContentBlock {
	s.mu.Lock()
	notifications := append([]sdkSubagentNotification(nil), s.notifications...)
	s.mu.Unlock()
	if messageID == "" {
		return nil
	}
	receipt := -1
	for index, notification := range notifications {
		if notification.method == "session.event" && sdkInboxReceipt(notification.params["event"], messageID) {
			receipt = index
			break
		}
	}
	if receipt < 0 {
		return nil
	}
	return sdkOutput(notifications[receipt:])
}

func sdkInboxReceipt(value any, messageID string) bool {
	event, ok := value.(map[string]any)
	if !ok || event["type"] != "agent/inbox/spliced" {
		return false
	}
	data, _ := event["data"].(map[string]any)
	inserted, _ := data["inserted"].([]any)
	for _, raw := range inserted {
		message, _ := raw.(map[string]any)
		if message["id"] == messageID {
			return true
		}
	}
	return false
}

func sdkOutput(notifications []sdkSubagentNotification) []ContentBlock {
	var selected []ContentBlock
	var partial strings.Builder
	for _, notification := range notifications {
		if notification.method != "session.event" {
			continue
		}
		event, _ := notification.params["event"].(map[string]any)
		data, _ := event["data"].(map[string]any)
		switch event["type"] {
		case "assistant/message":
			message := nestedMessage(data)
			if message == nil {
				continue
			}
			blocks := contentBlocks(message["content"])
			if len(blocks) > 0 {
				selected = blocks
			}
		case "assistant/chunk":
			chunk, _ := data["chunk"].(map[string]any)
			if chunk["type"] == "text-delta" {
				partial.WriteString(stringValue(chunk["text"]))
			}
		}
	}
	if len(selected) > 0 {
		return append([]ContentBlock(nil), selected...)
	}
	if partial.Len() > 0 {
		return []ContentBlock{{Type: "text", Text: partial.String()}}
	}
	return nil
}

func sdkStopReason(notifications []sdkSubagentNotification) SubagentStopReason {
	reason := ""
	for _, notification := range notifications {
		if notification.method != "session.event" {
			continue
		}
		event, _ := notification.params["event"].(map[string]any)
		if event["type"] != "turn/end" {
			continue
		}
		data, _ := event["data"].(map[string]any)
		reasonValue, _ := data["reason"].(map[string]any)
		reason, _ = reasonValue["kind"].(string)
	}
	switch reason {
	case "completed":
		return SubagentCompleted
	case "max-tokens":
		return SubagentMaxTokens
	case "aborted":
		return SubagentAborted
	default:
		return SubagentError
	}
}
