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
	defaultACPDisposeEOFGrace = 6 * time.Second
	defaultACPDisposeGrace    = 3 * time.Second
)

// ACPSubagentConfig configures a fresh Agent Client Protocol process per run.
type ACPSubagentConfig struct {
	ProviderName    string
	Command         string
	Args            []string
	CWD             string
	Permission      string
	Env             map[string]string
	DisposeEOFGrace time.Duration
	DisposeGrace    time.Duration
}

// ACPSubagentProvider drives an external ACP agent over NDJSON JSON-RPC.
type ACPSubagentProvider struct {
	name            string
	command         string
	args            []string
	cwd             string
	permission      string
	env             map[string]string
	disposeEOFGrace time.Duration
	disposeGrace    time.Duration
}

// NewACPSubagentProvider validates and constructs an ACP provider.
func NewACPSubagentProvider(config ACPSubagentConfig) (*ACPSubagentProvider, error) {
	name := strings.TrimSpace(config.ProviderName)
	if name == "" {
		name = "acp"
	}
	command := strings.TrimSpace(config.Command)
	if command == "" {
		return nil, errors.New("subagent-acp: command is required")
	}
	permission := strings.TrimSpace(config.Permission)
	if permission == "" {
		permission = "reject"
	}
	if permission != "allow" && permission != "reject" {
		return nil, errors.New("subagent-acp: permission must be allow or reject")
	}
	cwd, err := validateConfiguredSubagentCWD("subagent-acp", config.CWD)
	if err != nil {
		return nil, err
	}
	eofGrace, err := positiveSubagentDuration("subagent-acp", "disposeEOFGrace", config.DisposeEOFGrace, defaultACPDisposeEOFGrace)
	if err != nil {
		return nil, err
	}
	disposeGrace, err := positiveSubagentDuration("subagent-acp", "disposeGrace", config.DisposeGrace, defaultACPDisposeGrace)
	if err != nil {
		return nil, err
	}
	return &ACPSubagentProvider{
		name: name, command: command, args: append([]string(nil), config.Args...), cwd: cwd,
		permission: permission, env: cloneSubagentEnv(config.Env),
		disposeEOFGrace: eofGrace, disposeGrace: disposeGrace,
	}, nil
}

func (p *ACPSubagentProvider) Name() string { return p.name }
func (p *ACPSubagentProvider) Capabilities() SubagentCapabilities {
	return NoSubagentStartCapabilities()
}
func (p *ACPSubagentProvider) InheritsParentContext() bool { return false }

// Start launches, initializes, and creates the private ACP session before
// publishing the run.
func (p *ACPSubagentProvider) Start(ctx context.Context, request SubagentStartRequest) (*SubagentRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.New("subagent request was aborted before the ACP child started")
	}
	cwd := p.cwd
	if cwd == "" {
		cwd = request.CWD
	}
	var err error
	if cwd, err = validateSubagentCWD("subagent-acp", cwd); err != nil {
		return nil, err
	}
	process, err := startSubagentProcess(p.command, p.args, cwd, p.env)
	if err != nil {
		return nil, fmt.Errorf("subagent-acp: start child: %w", err)
	}

	var outputMu sync.Mutex
	var output strings.Builder
	rpc := newSubagentRPCClient(process.stdout, process.stdin)
	rpc.setHandlers(func(method string, raw json.RawMessage) (any, error) {
		if method != "session/request_permission" {
			return nil, fmt.Errorf("subagent-acp: unsupported client request %q", method)
		}
		var params struct {
			Options []struct {
				OptionID string `json:"optionId"`
				Kind     string `json:"kind"`
			} `json:"options"`
		}
		if err := decodeSubagentRPCParams(raw, &params); err != nil {
			return nil, err
		}
		if p.permission == "allow" {
			for _, option := range params.Options {
				if option.Kind == "allow_once" || option.Kind == "allow_always" {
					return map[string]any{"outcome": map[string]any{"outcome": "selected", "optionId": option.OptionID}}, nil
				}
			}
		}
		return map[string]any{"outcome": map[string]any{"outcome": "cancelled"}}, nil
	}, func(method string, raw json.RawMessage) error {
		if method != "session/update" {
			return nil
		}
		var params struct {
			Update struct {
				Kind    string `json:"sessionUpdate"`
				Content struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"update"`
		}
		if err := decodeSubagentRPCParams(raw, &params); err != nil {
			return err
		}
		if params.Update.Kind == "agent_message_chunk" && params.Update.Content.Type == "text" {
			outputMu.Lock()
			output.WriteString(params.Update.Content.Text)
			outputMu.Unlock()
		}
		return nil
	})
	rpc.start()

	cleanupStartup := func() error {
		rpc.close()
		return process.dispose(p.disposeEOFGrace, p.disposeGrace)
	}
	var initialized map[string]any
	if err := rpc.request(ctx, "initialize", map[string]any{
		"protocolVersion": ACPProtocolVersion, "clientCapabilities": map[string]any{},
	}, &initialized); err != nil {
		_ = cleanupStartup()
		if ctx.Err() != nil {
			return nil, errors.New("subagent request was aborted before the ACP child started")
		}
		return nil, fmt.Errorf("subagent-acp: initialize: %w", err)
	}
	var session struct {
		SessionID string `json:"sessionId"`
	}
	if err := rpc.request(ctx, "session/new", map[string]any{"cwd": cwd, "mcpServers": []any{}}, &session); err != nil {
		_ = cleanupStartup()
		if ctx.Err() != nil {
			return nil, errors.New("subagent request was aborted before the ACP child started")
		}
		return nil, fmt.Errorf("subagent-acp: session/new: %w", err)
	}
	if session.SessionID == "" {
		_ = cleanupStartup()
		return nil, errors.New("subagent-acp: ACP child published without a session id")
	}
	if ctx.Err() != nil {
		_ = cleanupStartup()
		return nil, errors.New("subagent request was aborted before the ACP child started")
	}

	runCtx, runCancel := context.WithCancel(ctx)
	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			runCancel()
			_ = rpc.notify("session/cancel", map[string]any{"sessionId": session.SessionID})
		})
	}
	run := newSubagentRun(newRunID(), cancel, func() error {
		rpc.close()
		return process.dispose(p.disposeEOFGrace, p.disposeGrace)
	})
	collect := func() []ContentBlock {
		outputMu.Lock()
		text := output.String()
		outputMu.Unlock()
		if text == "" {
			return nil
		}
		return []ContentBlock{{Type: "text", Text: text}}
	}
	go func() {
		var response struct {
			StopReason string `json:"stopReason"`
		}
		err := rpc.request(runCtx, "session/prompt", map[string]any{
			"sessionId": session.SessionID, "prompt": toACPPrompt(request.Prompt),
		}, &response)
		if runCtx.Err() != nil {
			run.settle(SubagentResult{Output: collect(), StopReason: SubagentAborted})
			return
		}
		if err != nil {
			run.settle(SubagentResult{Output: collect(), StopReason: SubagentError})
			return
		}
		run.settle(SubagentResult{Output: collect(), StopReason: ACPSubagentStopReason(response.StopReason)})
	}()
	return run, nil
}

// ACPSubagentStopReason maps ACP wire stop reasons to the shared vocabulary.
func ACPSubagentStopReason(reason string) SubagentStopReason {
	switch reason {
	case "end_turn":
		return SubagentCompleted
	case "max_tokens":
		return SubagentMaxTokens
	case "refusal":
		return SubagentRefusal
	case "cancelled":
		return SubagentAborted
	default:
		return SubagentError
	}
}

func toACPPrompt(prompt []ContentBlock) []map[string]any {
	blocks := make([]map[string]any, 0, len(prompt))
	for _, block := range prompt {
		if block.Type == "text" {
			blocks = append(blocks, map[string]any{"type": "text", "text": block.Text})
		}
	}
	return blocks
}

func cloneSubagentEnv(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

var _ io.Closer = (*SubagentRun)(nil)
