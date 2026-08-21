package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

const defaultCodexDisposeGrace = 3 * time.Second

// CodexPermissionMode fixes the native non-interactive policy for one Codex
// provider instance.
type CodexPermissionMode string

const (
	CodexPermissionNever                                CodexPermissionMode = "never"
	CodexPermissionApproveForMe                         CodexPermissionMode = "approve-for-me"
	CodexPermissionDangerouslyBypassApprovalsAndSandbox CodexPermissionMode = "dangerously-bypass-approvals-and-sandbox"
)

// CodexSubagentConfig configures one named Codex app-server provider.
type CodexSubagentConfig struct {
	ProviderName   string
	PermissionMode CodexPermissionMode
	Executable     string
	Env            map[string]string
	DisposeGrace   time.Duration
}

// CodexSubagentProvider drives one fresh codex app-server process per run.
type CodexSubagentProvider struct {
	name           string
	permissionMode CodexPermissionMode
	executable     string
	env            map[string]string
	disposeGrace   time.Duration
}

func NewCodexSubagentProvider(config CodexSubagentConfig) (*CodexSubagentProvider, error) {
	name := strings.TrimSpace(config.ProviderName)
	if name == "" {
		name = "codex"
	}
	permissionMode, err := resolveCodexPermissionMode(config.PermissionMode)
	if err != nil {
		return nil, err
	}
	executable := strings.TrimSpace(config.Executable)
	if executable == "" {
		executable = "codex"
	}
	grace, err := positiveSubagentDuration("subagent-codex", "disposeGrace", config.DisposeGrace, defaultCodexDisposeGrace)
	if err != nil {
		return nil, err
	}
	return &CodexSubagentProvider{name: name, permissionMode: permissionMode, executable: executable, env: cloneSubagentEnv(config.Env), disposeGrace: grace}, nil
}

func resolveCodexPermissionMode(mode CodexPermissionMode) (CodexPermissionMode, error) {
	switch CodexPermissionMode(strings.TrimSpace(string(mode))) {
	case "", CodexPermissionNever:
		return CodexPermissionNever, nil
	case CodexPermissionApproveForMe:
		return CodexPermissionApproveForMe, nil
	case CodexPermissionDangerouslyBypassApprovalsAndSandbox:
		return CodexPermissionDangerouslyBypassApprovalsAndSandbox, nil
	default:
		return "", fmt.Errorf("subagent-codex: permissionMode must be never, approve-for-me, or dangerously-bypass-approvals-and-sandbox")
	}
}

func (p *CodexSubagentProvider) Name() string { return p.name }
func (p *CodexSubagentProvider) Capabilities() SubagentCapabilities {
	return NoSubagentStartCapabilities()
}
func (p *CodexSubagentProvider) InheritsParentContext() bool { return false }

func (p *CodexSubagentProvider) Start(ctx context.Context, request SubagentStartRequest) (*SubagentRun, error) {
	texts, err := codexTextTask(request.Prompt)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.New("subagent-codex: request was aborted before app-server startup")
	}
	cwd, err := validateSubagentCWD("subagent-codex", request.CWD)
	if err != nil {
		return nil, err
	}
	command, args := codexAppServerCommand(p.executable)
	process, err := startSubagentProcess(command, args, cwd, p.env)
	if err != nil {
		return nil, fmt.Errorf("subagent-codex: start app-server: %w", err)
	}
	rpc := newSubagentRPCClient(process.stdout, process.stdin)
	wire := newCodexSubagentWire(rpc, p.permissionMode)
	rpc.setHandlers(func(method string, raw json.RawMessage) (any, error) {
		result, handleErr := wire.handleRequest(method, raw)
		if handleErr != nil {
			wire.fail(handleErr)
		}
		return result, handleErr
	}, func(method string, raw json.RawMessage) error {
		handleErr := wire.handleNotification(method, raw)
		if handleErr != nil {
			wire.fail(handleErr)
		}
		return handleErr
	})
	rpc.start()
	dispose := func() error {
		rpc.close()
		return process.dispose(0, p.disposeGrace)
	}
	if err := wire.initialize(ctx); err != nil {
		_ = dispose()
		if ctx.Err() != nil {
			return nil, errors.New("subagent-codex: request was aborted before run publication")
		}
		return nil, err
	}
	if err := wire.startThread(ctx, cwd); err != nil {
		_ = dispose()
		if ctx.Err() != nil {
			return nil, errors.New("subagent-codex: request was aborted before run publication")
		}
		return nil, err
	}
	if ctx.Err() != nil {
		_ = dispose()
		return nil, errors.New("subagent-codex: request was aborted before run publication")
	}

	runCtx, runCancel := context.WithCancel(ctx)
	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			runCancel()
			wire.interrupt()
		})
	}
	run := newSubagentRun(newRunID(), cancel, dispose)
	go func() {
		result, runErr := wire.runTurn(runCtx, texts)
		if runCtx.Err() != nil {
			run.settle(SubagentResult{Output: wire.collectOutput(), StopReason: SubagentAborted})
			return
		}
		if runErr != nil {
			run.settle(SubagentResult{Output: wire.collectOutput(), StopReason: SubagentError})
			return
		}
		run.settle(result)
	}()
	return run, nil
}

func codexAppServerCommand(executable string) (string, []string) {
	if executable == "" {
		executable = "codex"
	}
	if runtime.GOOS == "windows" {
		return "cmd.exe", []string{"/d", "/s", "/c", executable, "app-server", "--stdio"}
	}
	return executable, []string{"app-server", "--stdio"}
}

func codexTextTask(prompt []ContentBlock) ([]string, error) {
	if len(prompt) == 0 {
		return nil, errors.New("subagent-codex: the one-shot task must contain only text blocks")
	}
	texts := make([]string, 0, len(prompt))
	visible := false
	for _, block := range prompt {
		if block.Type != "text" {
			return nil, errors.New("subagent-codex: the one-shot task must contain only text blocks")
		}
		texts = append(texts, block.Text)
		visible = visible || strings.TrimSpace(block.Text) != ""
	}
	if !visible {
		return nil, errors.New("subagent-codex: the one-shot task must not be empty")
	}
	return texts, nil
}

type codexNotification struct {
	method string
	params map[string]any
}

type codexSubagentWire struct {
	rpc            *subagentRPCClient
	permissionMode CodexPermissionMode

	mu                 sync.Mutex
	threadID           string
	turnID             string
	pendingTurnID      string
	turnCompleted      chan map[string]any
	early              []codexNotification
	lastFinalAnswer    string
	lastUnphasedAnswer string
	hasFinalAnswer     bool
	hasUnphasedAnswer  bool
	fatal              chan struct{}
	fatalErr           error
	fatalOnce          sync.Once
}

func newCodexSubagentWire(rpc *subagentRPCClient, permissionMode CodexPermissionMode) *codexSubagentWire {
	return &codexSubagentWire{rpc: rpc, permissionMode: permissionMode, fatal: make(chan struct{})}
}

func (w *codexSubagentWire) fail(err error) {
	w.fatalOnce.Do(func() {
		w.mu.Lock()
		w.fatalErr = err
		w.mu.Unlock()
		close(w.fatal)
	})
}

func (w *codexSubagentWire) initialize(ctx context.Context) error {
	var response map[string]any
	if err := w.rpc.request(ctx, "initialize", map[string]any{
		"clientInfo":   map[string]any{"name": "deepseek-harness", "title": "DeepSeek Harness", "version": "0.0.1"},
		"capabilities": map[string]any{"experimentalApi": false, "requestAttestation": false},
	}, &response); err != nil {
		return fmt.Errorf("subagent-codex: initialize: %w", err)
	}
	if response == nil {
		return errors.New("subagent-codex: app-server returned invalid initialize response")
	}
	return w.rpc.notify("initialized", map[string]any{})
}

func (w *codexSubagentWire) startThread(ctx context.Context, cwd string) error {
	var response map[string]any
	params := map[string]any{"cwd": cwd, "ephemeral": true}
	for key, value := range codexThreadPermissionParams(w.permissionMode) {
		params[key] = value
	}
	if err := w.rpc.request(ctx, "thread/start", params, &response); err != nil {
		return fmt.Errorf("subagent-codex: thread/start: %w", err)
	}
	thread, err := subagentObject(response["thread"], "thread/start thread")
	if err != nil {
		return err
	}
	id, err := subagentString(thread["id"], "thread/start thread id")
	if err != nil {
		return err
	}
	if ephemeral, _ := thread["ephemeral"].(bool); !ephemeral {
		return errors.New("subagent-codex: app-server did not create an ephemeral thread")
	}
	w.mu.Lock()
	w.threadID = id
	w.mu.Unlock()
	return nil
}

func codexThreadPermissionParams(mode CodexPermissionMode) map[string]any {
	switch mode {
	case CodexPermissionApproveForMe:
		return map[string]any{"approvalPolicy": "on-request", "approvalsReviewer": "auto_review", "sandbox": "workspace-write"}
	case CodexPermissionDangerouslyBypassApprovalsAndSandbox:
		return map[string]any{"approvalPolicy": "never", "sandbox": "danger-full-access"}
	default:
		return map[string]any{"approvalPolicy": "never"}
	}
}

func (w *codexSubagentWire) runTurn(ctx context.Context, texts []string) (SubagentResult, error) {
	w.mu.Lock()
	w.turnCompleted = make(chan map[string]any, 1)
	threadID := w.threadID
	w.mu.Unlock()
	input := make([]map[string]any, 0, len(texts))
	for _, text := range texts {
		input = append(input, map[string]any{"type": "text", "text": text, "text_elements": []any{}})
	}
	var response map[string]any
	if err := w.rpc.request(ctx, "turn/start", map[string]any{"threadId": threadID, "input": input}, &response); err != nil {
		return SubagentResult{}, fmt.Errorf("subagent-codex: turn/start: %w", err)
	}
	turn, err := subagentObject(response["turn"], "turn/start turn")
	if err != nil {
		return SubagentResult{}, err
	}
	id, err := subagentString(turn["id"], "turn/start turn id")
	if err != nil {
		return SubagentResult{}, err
	}
	if err := w.commitTurnID(id); err != nil {
		return SubagentResult{}, err
	}
	w.mu.Lock()
	completed := w.turnCompleted
	w.mu.Unlock()
	select {
	case <-ctx.Done():
		return SubagentResult{}, ctx.Err()
	case <-w.fatal:
		w.mu.Lock()
		err := w.fatalErr
		w.mu.Unlock()
		return SubagentResult{}, err
	case <-w.rpc.done:
		return SubagentResult{}, w.rpc.waitError()
	case params := <-completed:
		terminal, err := subagentObject(params["turn"], "turn/completed turn")
		if err != nil {
			return SubagentResult{}, err
		}
		if codexContextWindowExceeded(terminal) {
			return SubagentResult{Output: w.collectOutput(), StopReason: SubagentMaxTokens}, nil
		}
		if terminal["status"] != "completed" {
			return SubagentResult{}, fmt.Errorf("subagent-codex: Codex turn ended with status %v", terminal["status"])
		}
		output := w.collectOutput()
		if len(output) == 0 {
			return SubagentResult{}, errors.New("subagent-codex: Codex completed without a final answer")
		}
		return SubagentResult{Output: output, StopReason: SubagentCompleted}, nil
	}
}

func (w *codexSubagentWire) interrupt() {
	w.mu.Lock()
	threadID, turnID := w.threadID, w.turnID
	w.mu.Unlock()
	if threadID == "" || turnID == "" {
		return
	}
	go func() {
		var ignored map[string]any
		_ = w.rpc.request(context.Background(), "turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, &ignored)
	}()
}

func (w *codexSubagentWire) collectOutput() []ContentBlock {
	w.mu.Lock()
	defer w.mu.Unlock()
	text, ok := w.lastUnphasedAnswer, w.hasUnphasedAnswer
	if w.hasFinalAnswer {
		text, ok = w.lastFinalAnswer, true
	}
	if !ok || strings.TrimSpace(text) == "" {
		return nil
	}
	return []ContentBlock{{Type: "text", Text: text}}
}

func (w *codexSubagentWire) handleRequest(method string, raw json.RawMessage) (any, error) {
	var params map[string]any
	if err := decodeSubagentRPCParams(raw, &params); err != nil {
		return nil, err
	}
	switch method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		if err := w.validateRunIDs(params, false); err != nil {
			return nil, err
		}
		decision, err := codexUnattendedDecision(params)
		if err != nil {
			return nil, err
		}
		return map[string]any{"decision": decision}, nil
	case "item/permissions/requestApproval":
		if err := w.validateRunIDs(params, false); err != nil {
			return nil, err
		}
		return map[string]any{"permissions": map[string]any{}, "scope": "turn"}, nil
	case "item/tool/requestUserInput":
		if err := w.validateRunIDs(params, false); err != nil {
			return nil, err
		}
		return map[string]any{"answers": map[string]any{}}, nil
	case "mcpServer/elicitation/request":
		if err := w.validateRunIDs(params, true); err != nil {
			return nil, err
		}
		return map[string]any{"action": "decline", "content": nil, "_meta": nil}, nil
	default:
		return nil, fmt.Errorf("subagent-codex: unsupported app-server request %q", method)
	}
}

func (w *codexSubagentWire) handleNotification(method string, raw json.RawMessage) error {
	var params map[string]any
	if err := decodeSubagentRPCParams(raw, &params); err != nil {
		return err
	}
	return w.applyNotification(method, params, true)
}

func (w *codexSubagentWire) applyNotification(method string, params map[string]any, queueEarly bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch method {
	case "turn/started":
		threadID, err := subagentString(params["threadId"], "turn/started thread id")
		if err != nil || threadID != w.threadID {
			return err
		}
		turn, err := subagentObject(params["turn"], "turn/started turn")
		if err != nil {
			return err
		}
		if w.turnCompleted != nil && w.turnID == "" {
			return w.observePendingTurnIDLocked(stringValue(turn["id"]))
		}
		return nil
	case "item/completed":
		threadID, err := subagentString(params["threadId"], "item/completed thread id")
		if err != nil || threadID != w.threadID {
			return err
		}
		id, err := subagentString(params["turnId"], "item/completed turn id")
		if err != nil {
			return err
		}
		if w.turnID == "" {
			if w.turnCompleted != nil && queueEarly {
				if err := w.observePendingTurnIDLocked(id); err != nil {
					return err
				}
				w.early = append(w.early, codexNotification{method: method, params: params})
			}
			return nil
		}
		if id != w.turnID {
			return nil
		}
		item, err := subagentObject(params["item"], "item/completed item")
		if err != nil || item["type"] != "agentMessage" {
			return err
		}
		text, ok := item["text"].(string)
		if !ok {
			return errors.New("subagent-codex: app-server returned an invalid agent message")
		}
		switch item["phase"] {
		case "final_answer":
			w.lastFinalAnswer, w.hasFinalAnswer = text, true
		case nil:
			w.lastUnphasedAnswer, w.hasUnphasedAnswer = text, true
		case "commentary":
		default:
			return fmt.Errorf("subagent-codex: app-server returned an unknown agent message phase %v", item["phase"])
		}
		return nil
	case "turn/completed":
		threadID, err := subagentString(params["threadId"], "turn/completed thread id")
		if err != nil || threadID != w.threadID {
			return err
		}
		turn, err := subagentObject(params["turn"], "turn/completed turn")
		if err != nil {
			return err
		}
		id, err := subagentString(turn["id"], "turn/completed turn id")
		if err != nil || w.turnCompleted == nil {
			return err
		}
		if w.turnID == "" {
			if queueEarly {
				if err := w.observePendingTurnIDLocked(id); err != nil {
					return err
				}
				w.early = append(w.early, codexNotification{method: method, params: params})
			}
			return nil
		}
		if id != w.turnID {
			return nil
		}
		status, _ := turn["status"].(string)
		if status != "completed" && status != "interrupted" && status != "failed" {
			return fmt.Errorf("subagent-codex: app-server returned invalid terminal turn status %v", turn["status"])
		}
		select {
		case w.turnCompleted <- params:
		default:
		}
	}
	return nil
}

func (w *codexSubagentWire) observePendingTurnIDLocked(id string) error {
	if id == "" {
		return errors.New("subagent-codex: app-server returned invalid turn id")
	}
	if w.pendingTurnID != "" && w.pendingTurnID != id {
		return errors.New("subagent-codex: app-server referenced conflicting turns")
	}
	w.pendingTurnID = id
	return nil
}

func (w *codexSubagentWire) commitTurnID(id string) error {
	w.mu.Lock()
	if w.pendingTurnID != "" && w.pendingTurnID != id {
		w.mu.Unlock()
		return errors.New("subagent-codex: turn/start response did not match the active turn")
	}
	w.turnID = id
	early := append([]codexNotification(nil), w.early...)
	w.early = nil
	w.mu.Unlock()
	for _, notification := range early {
		if err := w.applyNotification(notification.method, notification.params, false); err != nil {
			return err
		}
	}
	return nil
}

func (w *codexSubagentWire) validateRunIDs(params map[string]any, nullableTurn bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if params["threadId"] != w.threadID {
		return errors.New("subagent-codex: app-server request referenced another thread")
	}
	if nullableTurn && params["turnId"] == nil {
		return nil
	}
	id, err := subagentString(params["turnId"], "server request turn id")
	if err != nil {
		return err
	}
	if w.turnID == "" {
		return w.observePendingTurnIDLocked(id)
	}
	if id != w.turnID {
		return errors.New("subagent-codex: app-server request referenced another turn")
	}
	return nil
}

func codexUnattendedDecision(params map[string]any) (string, error) {
	raw, exists := params["availableDecisions"]
	if !exists || raw == nil {
		return "decline", nil
	}
	values, ok := raw.([]any)
	if !ok {
		return "", errors.New("subagent-codex: app-server offered no unattended approval decision")
	}
	for _, value := range values {
		if value == "cancel" {
			return "cancel", nil
		}
	}
	for _, value := range values {
		if value == "decline" {
			return "decline", nil
		}
	}
	return "", errors.New("subagent-codex: app-server offered no unattended approval decision")
}

func codexContextWindowExceeded(turn map[string]any) bool {
	if turn["status"] != "failed" {
		return false
	}
	errorValue, ok := turn["error"].(map[string]any)
	return ok && errorValue["codexErrorInfo"] == "contextWindowExceeded"
}

func subagentObject(value any, label string) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("subagent-codex: app-server returned invalid %s", label)
	}
	return object, nil
}

func subagentString(value any, label string) (string, error) {
	text, ok := value.(string)
	if !ok || text == "" {
		return "", fmt.Errorf("subagent-codex: app-server returned invalid %s", label)
	}
	return text, nil
}
