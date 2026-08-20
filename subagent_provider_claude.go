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

// ClaudeCodeSubagentConfig configures the fixed Claude Code one-shot
// provider. Executable defaults to claude; an explicit path is useful for
// packaged applications that do not inherit a shell PATH.
type ClaudeCodeSubagentConfig struct {
	Executable   string
	Env          map[string]string
	DisposeGrace time.Duration
}

// ClaudeCodeSubagentProvider drives the Claude Code stream-json CLI protocol.
type ClaudeCodeSubagentProvider struct {
	executable   string
	env          map[string]string
	disposeGrace time.Duration
}

func NewClaudeCodeSubagentProvider(config ClaudeCodeSubagentConfig) (*ClaudeCodeSubagentProvider, error) {
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
	return &ClaudeCodeSubagentProvider{executable: executable, env: env, disposeGrace: grace}, nil
}

func (p *ClaudeCodeSubagentProvider) Name() string { return "claude-code" }
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
		"--disallowedTools", "AskUserQuestion", "--permission-mode", "default", "--no-session-persistence",
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
	resultReady := make(chan SubagentResult, 1)
	go func() {
		resultReady <- consumeClaudeCodeProcess(process)
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

func consumeClaudeCodeProcess(process *subagentProcess) SubagentResult {
	scanner := bufio.NewScanner(process.stdout)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	var answer string
	hasAnswer := false
	for scanner.Scan() {
		var message map[string]any
		if json.Unmarshal(scanner.Bytes(), &message) != nil {
			continue
		}
		if message["type"] != "result" {
			continue
		}
		text, err := successfulClaudeCodeResult(message)
		if err != nil {
			return SubagentResult{StopReason: SubagentError}
		}
		answer, hasAnswer = text, true
	}
	if scanner.Err() != nil {
		return SubagentResult{StopReason: SubagentError}
	}
	processResult := process.result()
	if processResult.exitCode != 0 || processResult.err != nil {
		return SubagentResult{StopReason: SubagentError}
	}
	if !hasAnswer {
		return SubagentResult{StopReason: SubagentError}
	}
	return SubagentResult{Output: []ContentBlock{{Type: "text", Text: answer}}, StopReason: SubagentCompleted}
}

func successfulClaudeCodeResult(message map[string]any) (string, error) {
	subtype, _ := message["subtype"].(string)
	isError, _ := message["is_error"].(bool)
	result, _ := message["result"].(string)
	if subtype == "success" && !isError && strings.TrimSpace(result) != "" {
		return result, nil
	}
	detail := subtype
	if subtype == "success" {
		detail = "success result was marked as an error or contained no answer"
	} else if raw, ok := message["errors"].([]any); ok {
		parts := make([]string, 0, len(raw))
		for _, item := range raw {
			if text, ok := item.(string); ok {
				parts = append(parts, text)
			}
		}
		if len(parts) > 0 {
			detail = strings.Join(parts, "; ")
		}
	}
	return "", fmt.Errorf("subagent-claude-code: Claude Code failed: %s", detail)
}
