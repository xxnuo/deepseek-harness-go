package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// SubagentToolConfig exposes one registered provider as a model-facing
// one-shot delegation tool.
type SubagentToolConfig struct {
	Provider              string
	ToolName              string
	BackgroundMode        string
	EnableRunInBackground *bool
	AgentOptions          *SubagentAgentOptions
	Persona               string
	ToolFilter            *SubagentToolFilter
	MaxDepth              *int
}

func subagentToolOutputSchema() map[string]any {
	return map[string]any{"oneOf": []any{
		objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "background"}, "jobId": map[string]any{"type": "string"}}, "kind", "jobId"),
		objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "continuable"}, "subagentId": map[string]any{"type": "string"}}, "kind", "subagentId"),
		objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "foreground"}, "runId": map[string]any{"type": "string"}, "output": map[string]any{"type": "array", "items": map[string]any{}}}, "kind", "runId", "output"),
	}}
}

// RegisterSubagentTool registers a provider-specific one-shot delegation
// tool. Foreground runs are always disposed before the call returns.
func (e *Engine) RegisterSubagentTool(config SubagentToolConfig) error {
	config.Provider = strings.TrimSpace(config.Provider)
	config.ToolName = strings.TrimSpace(config.ToolName)
	if config.Provider == "" {
		return errors.New("tool-subagent: provider is required")
	}
	if config.ToolName == "" {
		config.ToolName = "subagent"
	}
	if config.BackgroundMode == "" {
		config.BackgroundMode = "one-shot"
	}
	if config.BackgroundMode != "one-shot" && config.BackgroundMode != "continuable" {
		return fmt.Errorf("tool-subagent: unsupported backgroundMode %q", config.BackgroundMode)
	}
	if config.MaxDepth != nil && *config.MaxDepth < 0 {
		return errors.New("tool-subagent: maxDepth must be a non-negative integer")
	}
	if config.AgentOptions != nil && config.AgentOptions.MaxTokens < 0 {
		return errors.New("tool-subagent: agentOptions.maxTokens must be positive when set")
	}
	if config.ToolFilter != nil && config.ToolFilter.Allow == nil && config.ToolFilter.Deny == nil {
		return errors.New("tool-subagent: toolFilter must name allow or deny tools")
	}
	if config.Provider == "spawn" || config.Provider == "fork" {
		return e.RegisterTool(inProcessSubagentTool(e, config, config.Provider == "fork"))
	}
	if config.BackgroundMode != "one-shot" {
		return fmt.Errorf("tool-subagent: provider %q does not support backgroundMode continuable", config.Provider)
	}
	e.mu.RLock()
	provider := e.subagentProviders[config.Provider]
	e.mu.RUnlock()
	if provider == nil {
		return fmt.Errorf("tool-subagent: provider %q is not registered", config.Provider)
	}
	request := SubagentStartRequest{
		MaxDepth: config.MaxDepth, AgentOptions: config.AgentOptions,
		Persona: config.Persona, ToolFilter: config.ToolFilter,
	}
	if err := validateSubagentCapabilities(provider, request); err != nil {
		return err
	}
	return e.RegisterTool(subagentProviderTool(e, provider, config))
}

func subagentProviderTool(e *Engine, provider SubagentProvider, config SubagentToolConfig) Tool {
	type input struct {
		Description     string `json:"description"`
		Prompt          string `json:"prompt"`
		RunInBackground bool   `json:"run_in_background"`
	}
	backgroundEnabled := config.EnableRunInBackground == nil || *config.EnableRunInBackground
	wording, promptDescription := subagentProviderWording(provider.InheritsParentContext())
	description := wording + " This call waits for the result by default."
	properties := map[string]any{
		"description": map[string]any{"type": "string", "description": "A short (3-5 word) description of the delegated task, for display."},
		"prompt":      map[string]any{"type": "string", "description": promptDescription},
	}
	if backgroundEnabled {
		description += " Set run_in_background to true to return a job id; collect with job_output and stop with job_kill."
		properties["run_in_background"] = map[string]any{"type": "boolean", "description": "Whether to run as a background job and return its id. Defaults to false; collect with job_output or stop with job_kill."}
	}
	return Tool{
		Schema: ToolSchema{Name: config.ToolName, Description: description, Parameters: objectSchema(properties, "description", "prompt"), Output: subagentToolOutputSchema()},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			in.Description, in.Prompt = strings.TrimSpace(in.Description), strings.TrimSpace(in.Prompt)
			if in.Description == "" || in.Prompt == "" {
				return ToolResult{}, errors.New("subagent description and prompt are required")
			}
			if in.RunInBackground && !backgroundEnabled {
				return ToolResult{}, errors.New("run_in_background is disabled for this tool")
			}
			if call.SessionID == "" {
				return ToolResult{}, errors.New("subagent tool requires a calling agent session")
			}
			request := SubagentStartRequest{
				ParentSessionID: call.SessionID, CWD: call.Workspace,
				Prompt:   []ContentBlock{{Type: "text", Text: in.Prompt}},
				MaxDepth: config.MaxDepth, AgentOptions: config.AgentOptions,
				Persona: config.Persona, ToolFilter: config.ToolFilter,
			}
			if in.RunInBackground {
				jobID, err := startBackgroundSubagent(e, call.SessionID, in.Description, config.Provider, request)
				if err != nil {
					return ToolResult{}, err
				}
				result := textToolResult("started background subagent job " + jobID)
				result.Value = map[string]any{"kind": "background", "jobId": jobID}
				return result, nil
			}
			run, err := e.StartSubagent(ctx, config.Provider, request)
			if err != nil {
				return ToolResult{}, err
			}
			result, err := settleForegroundSubagent(run)
			if err != nil {
				return ToolResult{}, err
			}
			toolResult := textToolResult(contentValueText(result.Output))
			toolResult.Value = map[string]any{"kind": "foreground", "runId": run.ID, "output": result.Output}
			return toolResult, nil
		},
	}
}

func subagentProviderWording(inheritsConversation bool) (string, string) {
	if inheritsConversation {
		return "Delegate a task to a subagent that inherits this conversation: a child agent seeded with all completed turns so far (it does not see the current in-flight turn). You receive its result, not its intermediate steps.", "The task for the subagent. It already sees this conversation's completed turns, so state only what is new."
	}
	return "Delegate a self-contained task to a subagent in its own context. You receive its result, not its intermediate steps.", "The complete, self-contained task for the subagent. It does not share this conversation's context, so include everything it needs."
}

func settleForegroundSubagent(run *SubagentRun) (SubagentResult, error) {
	result, executionErr := run.Wait(context.Background())
	if executionErr == nil {
		executionErr = subagentStopError(result)
	}
	disposeErr := run.Dispose()
	if executionErr != nil {
		if disposeErr != nil {
			return SubagentResult{}, fmt.Errorf("%v; dispose failed: %w", executionErr, disposeErr)
		}
		return SubagentResult{}, executionErr
	}
	if disposeErr != nil {
		return SubagentResult{}, disposeErr
	}
	return result, nil
}

func subagentStopError(result SubagentResult) error {
	var message string
	switch result.StopReason {
	case SubagentCompleted:
		return nil
	case SubagentAborted:
		message = "subagent run was cancelled"
	case SubagentError:
		message = "subagent run failed"
	case SubagentMaxTokens:
		message = "subagent run hit its token limit before finishing"
	case SubagentRefusal:
		message = "subagent declined the task"
	default:
		message = fmt.Sprintf("subagent run ended abnormally (%s)", result.StopReason)
	}
	if partial := contentValueText(result.Output); partial != "" {
		message += "\nPartial output before the run ended:\n" + partial
	}
	return errors.New(message)
}

type backgroundSubagentStart struct {
	mu       sync.Mutex
	cancel   context.CancelCauseFunc
	run      *SubagentRun
	finished bool
}

func startBackgroundSubagent(e *Engine, owner, label, provider string, request SubagentStartRequest) (string, error) {
	return e.jobs.startManaged(owner, "subagent", label, 0, func() (*managedJobHandle, error) {
		startCtx, cancel := context.WithCancelCause(context.Background())
		state := &backgroundSubagentStart{cancel: cancel}
		done := make(chan managedJobResult, 1)
		go func() {
			run, err := e.StartSubagent(startCtx, provider, request)
			if err != nil {
				status := jobFailed
				if startCtx.Err() != nil && subagentStartupWasCancelled(err) {
					status = jobKilled
				}
				state.mu.Lock()
				state.finished = true
				state.mu.Unlock()
				done <- managedJobResult{Status: status, Detail: err.Error()}
				close(done)
				return
			}
			state.mu.Lock()
			state.run = run
			cancelled := startCtx.Err() != nil
			state.mu.Unlock()
			if cancelled {
				run.Cancel()
			}
			result, waitErr := run.Wait(context.Background())
			outcome := backgroundSubagentOutcome(result, waitErr)
			if disposeErr := run.Dispose(); disposeErr != nil {
				if outcome.Detail != "" {
					outcome.Detail += "; "
				}
				outcome.Status = jobFailed
				outcome.Output = ""
				outcome.Detail += "dispose failed: " + disposeErr.Error()
			}
			state.mu.Lock()
			state.finished = true
			state.mu.Unlock()
			done <- outcome
			close(done)
		}()
		return &managedJobHandle{
			Done: done,
			Cancel: func(reason string) error {
				state.mu.Lock()
				defer state.mu.Unlock()
				if state.finished || startCtx.Err() != nil {
					return nil
				}
				if strings.TrimSpace(reason) == "" {
					reason = "background subagent task killed"
				}
				state.cancel(errors.New(reason))
				if state.run != nil {
					state.run.Cancel()
				}
				return nil
			},
		}, nil
	})
}

func subagentStartupWasCancelled(err error) bool {
	return errors.Is(err, context.Canceled) || strings.Contains(strings.ToLower(err.Error()), "aborted before")
}

func backgroundSubagentOutcome(result SubagentResult, err error) managedJobResult {
	if err != nil {
		return managedJobResult{Status: jobFailed, Detail: err.Error()}
	}
	switch result.StopReason {
	case SubagentCompleted:
		return managedJobResult{Status: jobCompleted, Output: contentValueText(result.Output)}
	case SubagentAborted:
		return managedJobResult{Status: jobKilled}
	default:
		return managedJobResult{Status: jobFailed, Detail: string(result.StopReason)}
	}
}
