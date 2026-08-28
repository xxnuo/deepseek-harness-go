package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const defaultClaudeCodeDisposeGrace = 3 * time.Second

// ClaudeCodePermissionMode fixes the native non-interactive policy for one
// Claude Code provider instance.
type ClaudeCodePermissionMode string

const (
	ClaudeCodePermissionDontAsk           ClaudeCodePermissionMode = "dontAsk"
	ClaudeCodePermissionAcceptEdits       ClaudeCodePermissionMode = "acceptEdits"
	ClaudeCodePermissionAuto              ClaudeCodePermissionMode = "auto"
	ClaudeCodePermissionPlan              ClaudeCodePermissionMode = "plan"
	ClaudeCodePermissionBypassPermissions ClaudeCodePermissionMode = "bypassPermissions"
)

// ClaudeCodeSubagentConfig configures one named Claude Code provider.
// Executable defaults to claude; an explicit path is useful for packaged
// applications that do not inherit a shell PATH.
type ClaudeCodeSubagentConfig struct {
	ProviderName   string
	PermissionMode ClaudeCodePermissionMode
	Executable     string
	Env            map[string]string
	DisposeGrace   time.Duration
}

// ClaudeCodeSubagentProvider drives the Claude Code stream-json CLI protocol.
type ClaudeCodeSubagentProvider struct {
	name           string
	permissionMode ClaudeCodePermissionMode
	executable     string
	env            map[string]string
	disposeGrace   time.Duration
}

func NewClaudeCodeSubagentProvider(config ClaudeCodeSubagentConfig) (*ClaudeCodeSubagentProvider, error) {
	name := strings.TrimSpace(config.ProviderName)
	if name == "" {
		name = "claude-code"
	}
	permissionMode, err := resolveClaudeCodePermissionMode(config.PermissionMode)
	if err != nil {
		return nil, err
	}
	executable := strings.TrimSpace(config.Executable)
	if executable == "" {
		executable = "claude"
	}
	grace, err := positiveSubagentDuration("subagent-claude-code", "disposeGrace", config.DisposeGrace, defaultClaudeCodeDisposeGrace)
	if err != nil {
		return nil, err
	}
	env := cloneSubagentEnv(config.Env)
	if env == nil {
		env = map[string]string{}
	}
	if _, ok := env["CLAUDE_CODE_ENTRYPOINT"]; !ok {
		env["CLAUDE_CODE_ENTRYPOINT"] = "sdk-ts"
	}
	if _, ok := env["CLAUDE_AGENT_SDK_VERSION"]; !ok {
		env["CLAUDE_AGENT_SDK_VERSION"] = "0.3.220"
	}
	return &ClaudeCodeSubagentProvider{name: name, permissionMode: permissionMode, executable: executable, env: env, disposeGrace: grace}, nil
}

func resolveClaudeCodePermissionMode(mode ClaudeCodePermissionMode) (ClaudeCodePermissionMode, error) {
	switch ClaudeCodePermissionMode(strings.TrimSpace(string(mode))) {
	case "", ClaudeCodePermissionDontAsk:
		return ClaudeCodePermissionDontAsk, nil
	case ClaudeCodePermissionAcceptEdits:
		return ClaudeCodePermissionAcceptEdits, nil
	case ClaudeCodePermissionAuto:
		return ClaudeCodePermissionAuto, nil
	case ClaudeCodePermissionPlan:
		return ClaudeCodePermissionPlan, nil
	case ClaudeCodePermissionBypassPermissions:
		return ClaudeCodePermissionBypassPermissions, nil
	default:
		return "", errors.New("subagent-claude-code: permissionMode must be dontAsk, acceptEdits, auto, plan, or bypassPermissions")
	}
}

func (p *ClaudeCodeSubagentProvider) Name() string { return p.name }
func (p *ClaudeCodeSubagentProvider) Capabilities() SubagentCapabilities {
	return NoSubagentStartCapabilities()
}
func (p *ClaudeCodeSubagentProvider) InheritsParentContext() bool { return false }

func (p *ClaudeCodeSubagentProvider) Start(ctx context.Context, request SubagentStartRequest) (*SubagentRun, error) {
	prompt, err := claudeCodeTextTask(request.Prompt)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.New("subagent-claude-code: request was aborted before CLI startup")
	}
	cwd, err := validateSubagentCWD("subagent-claude-code", request.CWD)
	if err != nil {
		return nil, err
	}
	args := []string{
		"--output-format", "stream-json", "--verbose", "--input-format", "stream-json",
		"--disallowedTools", "AskUserQuestion",
	}
	if p.permissionMode == ClaudeCodePermissionPlan {
		args = append(args, "ExitPlanMode")
	}
	args = append(args, "--permission-mode", string(p.permissionMode), "--no-session-persistence")
	if p.permissionMode == ClaudeCodePermissionBypassPermissions {
		args = append(args, "--dangerously-skip-permissions")
	}
	process, err := startSubagentProcess(p.executable, args, cwd, p.env)
	if err != nil {
		return nil, fmt.Errorf("subagent-claude-code: start CLI: %w", err)
	}
	input := map[string]any{
		"type": "user", "session_id": "", "parent_tool_use_id": nil,
		"message": map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": prompt}}},
	}
	data, err := json.Marshal(input)
	if err == nil {
		data = append(data, '\n')
		_, err = process.stdin.Write(data)
	}
	process.closeInput()
	if err != nil {
		_ = process.dispose(0, p.disposeGrace)
		return nil, fmt.Errorf("subagent-claude-code: send task: %w", err)
	}
	if ctx.Err() != nil {
		_ = process.dispose(0, p.disposeGrace)
		return nil, errors.New("subagent-claude-code: request was aborted before run publication")
	}

	runCtx, runCancel := context.WithCancel(ctx)
	var disposeOnce sync.Once
	var disposeErr error
	disposeDone := make(chan struct{})
	dispose := func() error {
		disposeOnce.Do(func() {
			disposeErr = process.dispose(0, p.disposeGrace)
			close(disposeDone)
		})
		<-disposeDone
		return disposeErr
	}
	cancel := func() {
		runCancel()
		go func() { _ = dispose() }()
	}
	run := newSubagentRun(newRunID(), cancel, dispose)
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-run.Done():
		}
	}()
	resultReady := make(chan SubagentResult, 1)
	go func() {
		resultReady <- consumeClaudeCodeProcess(process, p.permissionMode)
	}()
	go func() {
		select {
		case <-runCtx.Done():
			run.settle(SubagentResult{StopReason: SubagentAborted})
		case result := <-resultReady:
			if runCtx.Err() != nil {
				run.settle(SubagentResult{StopReason: SubagentAborted})
				return
			}
			run.settle(result)
		}
	}()
	return run, nil
}

func claudeCodeTextTask(prompt []ContentBlock) (string, error) {
	if len(prompt) == 0 {
		return "", errors.New("subagent-claude-code: the one-shot task must contain only text blocks")
	}
	var text strings.Builder
	visible := false
	for _, block := range prompt {
		if block.Type != "text" {
			return "", errors.New("subagent-claude-code: the one-shot task must contain only text blocks")
		}
		text.WriteString(block.Text)
		visible = visible || strings.TrimSpace(block.Text) != ""
	}
	if !visible {
		return "", errors.New("subagent-claude-code: the one-shot task must not be empty")
	}
	return text.String(), nil
}

func consumeClaudeCodeProcess(process *subagentProcess, permissionMode ClaudeCodePermissionMode) SubagentResult {
	scanner := bufio.NewScanner(process.stdout)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	var answer string
	hasAnswer := false
	permissionDiagnostic := ""
	for scanner.Scan() {
		var message map[string]any
		if json.Unmarshal(scanner.Bytes(), &message) != nil {
			return claudeCodeFailureResult("query-run", "unknown", permissionDiagnostic, subagentProcessResult{})
		}
		if message["type"] == "system" && message["subtype"] == "permission_denied" {
			permissionDiagnostic = fmt.Sprintf("Claude Code unattended decision (mode: %s; request: tool permission; decision: denied): the provider does not request human approval", permissionMode)
			continue
		}
		if message["type"] != "result" {
			continue
		}
		text, category, err := classifyClaudeCodeResult(message)
		if err != nil {
			return claudeCodeFailureResult("query-run", category, permissionDiagnostic, subagentProcessResult{})
		}
		answer, hasAnswer = text, true
	}
	if scanner.Err() != nil {
		return claudeCodeFailureResult("query-run", "unknown", permissionDiagnostic, subagentProcessResult{})
	}
	processResult := process.result()
	if processResult.exitCode != 0 || processResult.err != nil {
		return claudeCodeFailureResult("process", "process-exit", permissionDiagnostic, processResult)
	}
	if !hasAnswer {
		return claudeCodeFailureResult("query-run", "missing-result", permissionDiagnostic, processResult)
	}
	return SubagentResult{Output: []ContentBlock{{Type: "text", Text: answer}}, StopReason: SubagentCompleted}
}

func successfulClaudeCodeResult(message map[string]any) (string, error) {
	result, _, err := classifyClaudeCodeResult(message)
	return result, err
}

func classifyClaudeCodeResult(message map[string]any) (string, string, error) {
	subtype, _ := message["subtype"].(string)
	isError, _ := message["is_error"].(bool)
	result, _ := message["result"].(string)
	if subtype == "success" && !isError && strings.TrimSpace(result) != "" {
		return result, "", nil
	}
	if subtype == "success" {
		return "", "invalid-success", errors.New("subagent-claude-code: Claude Code returned an invalid success result")
	}
	category := "unknown"
	switch subtype {
	case "error_during_execution", "error_max_turns", "error_max_budget_usd", "error_max_structured_output_retries":
		category = subtype
	}
	return "", category, fmt.Errorf("subagent-claude-code: Claude Code failed: %s", category)
}

func claudeCodeFailureResult(stage, category, permission string, outcome subagentProcessResult) SubagentResult {
	fields := []string{"product: Claude Code", "stage: " + stage, "category: " + category}
	if outcome.exitCode >= 0 && stage == "process" {
		fields = append(fields, fmt.Sprintf("exit code: %d", outcome.exitCode))
	}
	if outcome.signal != "" {
		fields = append(fields, "signal: "+outcome.signal)
	}
	diagnostic := "Product subagent failure (" + strings.Join(fields, "; ") + ")"
	if permission != "" {
		diagnostic += "\n" + permission
	}
	return SubagentResult{Diagnostic: diagnostic, StopReason: SubagentError}
}
