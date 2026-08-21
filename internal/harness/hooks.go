package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dlclark/regexp2/v2"
)

const (
	DefaultHookTimeout               = 10 * time.Minute
	DefaultHookStderrSummaryMaxChars = 500
)

type HookDialect string

const (
	HookDialectClaudeCode HookDialect = "claude-code"
	HookDialectCodex      HookDialect = "codex"
)

type HookBridgeConfig struct {
	Dialect               HookDialect
	ConfigPath            string
	PluginRoot            string
	ProjectDir            string
	Model                 string
	DefaultTimeout        time.Duration
	StderrSummaryMaxChars int
}

type HookCommand struct {
	Command    string
	Timeout    time.Duration
	TimeoutSet bool
}

type HookOutput struct {
	ExitCode          *int
	Stderr            string
	Stdout            string
	Continue          *bool
	StopReason        string
	Decision          string
	Reason            string
	HookEventName     string
	AdditionalContext string
	SystemMessage     string
	UpdatedInput      map[string]any
}

type MergedHookOutcome struct {
	Decision          string
	Reason            string
	Stop              bool
	StopReason        string
	AdditionalContext []string
	SystemMessages    []string
}

type HookPointRequest struct {
	Point                string
	MatchQuery           string
	Payload              any
	CWD                  string
	PlainStdoutAsContext bool
}

type HookRunResult struct {
	Matcher   *string
	HandlerID string
	Output    HookOutput
	Duration  time.Duration
}

type HookPointResult struct {
	Outcome MergedHookOutcome
	Runs    []HookRunResult
}

type hookMatcherGroup struct {
	matcher *string
	hooks   []HookCommand
}

// HookBridge runs an unmodified Claude Code or Codex command-hook config.
// Engine lifecycle integration uses the same public runner exposed to embedders.
type HookBridge struct {
	config HookBridgeConfig
	groups map[string][]hookMatcherGroup
	nextID atomic.Uint64
}

func NewHookBridge(config HookBridgeConfig) (*HookBridge, error) {
	if config.Dialect != HookDialectClaudeCode && config.Dialect != HookDialectCodex {
		return nil, fmt.Errorf("hooks: unsupported dialect %q", config.Dialect)
	}
	if strings.TrimSpace(config.ConfigPath) == "" {
		return nil, errors.New("hooks: configPath is required")
	}
	if config.DefaultTimeout == 0 {
		config.DefaultTimeout = DefaultHookTimeout
	}
	if config.DefaultTimeout < 0 {
		return nil, errors.New("hooks: default timeout must be non-negative")
	}
	if config.StderrSummaryMaxChars == 0 {
		config.StderrSummaryMaxChars = DefaultHookStderrSummaryMaxChars
	}
	if config.StderrSummaryMaxChars < 1 {
		return nil, errors.New("hooks: stderr summary limit must be a positive integer")
	}
	raw, err := os.ReadFile(config.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("hooks: read %q: %w", config.ConfigPath, err)
	}
	groups, err := parseHookBridgeConfig(raw, config)
	if err != nil {
		return nil, err
	}
	return &HookBridge{config: config, groups: groups}, nil
}

func (b *HookBridge) Config() HookBridgeConfig { return b.config }

func (b *HookBridge) Run(ctx context.Context, request HookPointRequest) (HookPointResult, error) {
	return b.run(ctx, request, nil, nil)
}

func (b *HookBridge) run(
	ctx context.Context,
	request HookPointRequest,
	before func(HookRunResult) error,
	after func(HookRunResult) error,
) (HookPointResult, error) {
	if err := ctx.Err(); err != nil {
		return HookPointResult{}, err
	}
	stdin, err := json.Marshal(request.Payload)
	if err != nil {
		return HookPointResult{}, fmt.Errorf("hooks: encode %s payload: %w", request.Point, err)
	}
	if b.config.Dialect == HookDialectClaudeCode {
		stdin = append(stdin, '\n')
	}
	matched := b.groups[request.Point]
	runs := make([]HookRunResult, 0)
	outputs := make([]HookOutput, 0)
	for _, group := range matched {
		if !MatchesHookMatcher(group.matcher, request.MatchQuery, b.config.Dialect) {
			continue
		}
		for _, hook := range group.hooks {
			handlerID := fmt.Sprintf("%s:%s:%d", b.config.Dialect, request.Point, b.nextID.Add(1))
			run := HookRunResult{Matcher: group.matcher, HandlerID: handlerID}
			if before != nil {
				if err := before(run); err != nil {
					return HookPointResult{}, err
				}
			}
			env := map[string]string(nil)
			if b.config.Dialect == HookDialectClaudeCode {
				projectDir := b.config.ProjectDir
				if projectDir == "" {
					projectDir = request.CWD
				}
				if projectDir != "" {
					env = map[string]string{"CLAUDE_PROJECT_DIR": projectDir}
				}
			}
			output, duration := runCommandHook(ctx, hook, request.CWD, env, stdin, request.Point, b.config.DefaultTimeout)
			if request.PlainStdoutAsContext && output.ExitCode != nil && *output.ExitCode == 0 && output.AdditionalContext == "" && output.Stdout != "" && !strings.HasPrefix(output.Stdout, "{") {
				output.AdditionalContext = output.Stdout
			}
			run.Output, run.Duration = output, duration
			if after != nil {
				if err := after(run); err != nil {
					return HookPointResult{}, err
				}
			}
			runs = append(runs, run)
			outputs = append(outputs, output)
		}
	}
	return HookPointResult{Outcome: MergeHookOutputs(outputs), Runs: runs}, nil
}

func parseHookBridgeConfig(data []byte, config HookBridgeConfig) (map[string][]hookMatcherGroup, error) {
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("hooks: parse %q: %w", config.ConfigPath, err)
	}
	root, ok := raw.(map[string]any)
	if !ok {
		return map[string][]hookMatcherGroup{}, nil
	}
	if wrapped, ok := root["hooks"].(map[string]any); ok {
		root = wrapped
	}
	events := []string{"PreToolUse", "PostToolUse", "SessionStart", "UserPromptSubmit", "Stop"}
	if config.Dialect == HookDialectClaudeCode {
		events = []string{"SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "Stop", "SubagentStart", "SubagentStop"}
	}
	parsed := make(map[string][]hookMatcherGroup)
	for _, event := range events {
		rawGroups, ok := root[event].([]any)
		if !ok {
			continue
		}
		for _, rawGroup := range rawGroups {
			group, ok := rawGroup.(map[string]any)
			if !ok {
				continue
			}
			rawHooks, ok := group["hooks"].([]any)
			if !ok {
				continue
			}
			commands := make([]HookCommand, 0, len(rawHooks))
			for _, rawHook := range rawHooks {
				hook, ok := rawHook.(map[string]any)
				if !ok {
					continue
				}
				typ, _ := hook["type"].(string)
				if typ == "" {
					typ = "command"
				}
				if typ != "command" || (config.Dialect == HookDialectCodex && hook["async"] == true) {
					continue
				}
				command, ok := hook["command"].(string)
				if !ok {
					continue
				}
				if config.Dialect == HookDialectClaudeCode {
					if config.PluginRoot != "" {
						command = strings.ReplaceAll(command, "${CLAUDE_PLUGIN_ROOT}", config.PluginRoot)
					}
					if config.ProjectDir != "" {
						command = strings.ReplaceAll(command, "${CLAUDE_PROJECT_DIR}", config.ProjectDir)
					}
				}
				timeoutSeconds, hasTimeout := hookNumber(hook["timeout"])
				if !hasTimeout && config.Dialect == HookDialectCodex {
					timeoutSeconds, hasTimeout = hookNumber(hook["timeoutSec"])
				}
				commandHook := HookCommand{Command: command}
				if hasTimeout {
					commandHook.Timeout = time.Duration(timeoutSeconds * float64(time.Second))
					commandHook.TimeoutSet = true
				}
				commands = append(commands, commandHook)
			}
			if len(commands) == 0 {
				continue
			}
			var matcher *string
			if event != "UserPromptSubmit" && event != "Stop" {
				if value, ok := group["matcher"].(string); ok {
					matcher = &value
				}
			}
			if diagnostic := HookMatcherDiagnostic(matcher, config.Dialect); diagnostic != "" {
				return nil, fmt.Errorf("%s on event %q", diagnostic, event)
			}
			parsed[event] = append(parsed[event], hookMatcherGroup{matcher: matcher, hooks: commands})
		}
	}
	return parsed, nil
}

func hookNumber(value any) (float64, bool) {
	number, ok := value.(float64)
	return number, ok
}

func HookMatcherDiagnostic(matcher *string, dialect HookDialect) string {
	if matcher == nil || *matcher == "" || *matcher == "*" {
		return ""
	}
	if dialect == HookDialectClaudeCode && isClaudeLiteralMatcher(*matcher) {
		return ""
	}
	if _, err := regexp2.Compile(*matcher, regexp2.ECMAScript); err != nil {
		return fmt.Sprintf("invalid %s regex matcher %q", dialect, *matcher)
	}
	return ""
}

func MatchesHookMatcher(matcher *string, query string, dialect HookDialect) bool {
	if matcher == nil || *matcher == "" || *matcher == "*" {
		return true
	}
	if dialect == HookDialectClaudeCode && isClaudeLiteralMatcher(*matcher) {
		for _, alternative := range strings.Split(*matcher, "|") {
			if alternative == query {
				return true
			}
		}
		return false
	}
	compiled, err := regexp2.Compile(*matcher, regexp2.ECMAScript)
	if err != nil {
		return false
	}
	matched, err := compiled.MatchString(query)
	return err == nil && matched
}

func isClaudeLiteralMatcher(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character != '_' && character != '|' && (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func ParseHookOutput(exitCode *int, stdout, stderr string, expectedEvent ...string) HookOutput {
	trimmedErr := strings.TrimSpace(stderr)
	trimmedOut := strings.TrimSpace(stdout)
	output := HookOutput{ExitCode: exitCode, Stderr: trimmedErr, Stdout: trimmedOut}
	if exitCode != nil && *exitCode == 2 {
		output.Decision = "block"
		output.Reason = trimmedErr
	}
	if exitCode == nil || *exitCode != 0 || !strings.HasPrefix(trimmedOut, "{") {
		return output
	}
	var parsed map[string]any
	if json.Unmarshal([]byte(trimmedOut), &parsed) != nil || parsed == nil {
		return output
	}
	if value, ok := parsed["continue"].(bool); ok {
		output.Continue = &value
	}
	output.StopReason, _ = parsed["stopReason"].(string)
	output.SystemMessage, _ = parsed["systemMessage"].(string)
	if decision, _ := parsed["decision"].(string); decision == "approve" || decision == "block" {
		output.Decision = decision
	}
	if reason, ok := parsed["reason"].(string); ok {
		output.Reason = reason
	}
	hso, _ := parsed["hookSpecificOutput"].(map[string]any)
	if hso == nil {
		return output
	}
	output.HookEventName, _ = hso["hookEventName"].(string)
	if len(expectedEvent) > 0 && output.HookEventName != expectedEvent[0] {
		return output
	}
	if decision, _ := hso["permissionDecision"].(string); decision == "allow" || decision == "deny" || decision == "ask" {
		output.Decision = decision
	}
	if reason, ok := hso["permissionDecisionReason"].(string); ok {
		output.Reason = reason
	}
	output.AdditionalContext, _ = hso["additionalContext"].(string)
	if updated, ok := hso["updatedInput"].(map[string]any); ok {
		output.UpdatedInput = updated
	}
	return output
}

func MergeHookOutputs(outputs []HookOutput) MergedHookOutcome {
	merged := MergedHookOutcome{Decision: "none", AdditionalContext: []string{}, SystemMessages: []string{}}
	maxRank := 0
	reasons := map[int][]string{}
	for _, output := range outputs {
		rank := hookDecisionRank(output.Decision)
		if rank > maxRank {
			maxRank = rank
		}
		if (rank == 2 || rank == 3) && output.Reason != "" {
			reasons[rank] = append(reasons[rank], output.Reason)
		}
		if output.Continue != nil && !*output.Continue && !merged.Stop {
			merged.Stop = true
			merged.StopReason = output.StopReason
		}
		if output.AdditionalContext != "" {
			merged.AdditionalContext = append(merged.AdditionalContext, output.AdditionalContext)
		}
		if output.SystemMessage != "" {
			merged.SystemMessages = append(merged.SystemMessages, output.SystemMessage)
		}
	}
	switch maxRank {
	case 3:
		merged.Decision = "deny"
	case 2:
		merged.Decision = "ask"
	case 1:
		merged.Decision = "allow"
	}
	merged.Reason = strings.Join(reasons[maxRank], "\n\n")
	return merged
}

func hookDecisionRank(decision string) int {
	switch decision {
	case "deny", "block":
		return 3
	case "ask":
		return 2
	case "approve", "allow":
		return 1
	default:
		return 0
	}
}

func SummarizeHookStderr(stderr string, maxChars int) string {
	trimmed := strings.TrimSpace(stderr)
	if trimmed == "" {
		return ""
	}
	characters := []rune(trimmed)
	if len(characters) <= maxChars {
		return trimmed
	}
	return string(characters[:maxChars]) + "…"
}

func runCommandHook(ctx context.Context, hook HookCommand, cwd string, env map[string]string, stdin []byte, expectedEvent string, defaultTimeout time.Duration) (HookOutput, time.Duration) {
	started := time.Now()
	timeout := hook.Timeout
	if !hook.TimeoutSet && timeout == 0 {
		timeout = defaultTimeout
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, "bash", "-c", hook.Command)
	if cwd != "" {
		absolute, err := filepath.Abs(cwd)
		if err != nil {
			return ParseHookOutput(nil, "", err.Error()), time.Since(started)
		}
		cmd.Dir = absolute
	}
	cmd.Env = scrubbedChildEnv(env)
	cmd.Stdin = bytes.NewReader(stdin)
	configureChildProcess(cmd)
	var stdout, stderr limitedBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var exitCode *int
	if err == nil {
		value := 0
		exitCode = &value
	} else {
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() >= 0 {
			value := exit.ExitCode()
			exitCode = &value
		} else if runCtx.Err() == nil {
			if stderr.Len() == 0 {
				_, _ = stderr.Write([]byte(err.Error()))
			}
		}
	}
	return ParseHookOutput(exitCode, stdout.String(), stderr.String(), expectedEvent), time.Since(started)
}
