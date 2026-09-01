package harness

import (
	"context"
	"errors"
	"fmt"
)

type inProcessSubagentProvider struct {
	engine *Engine
	name   string
	fork   bool
}

func (provider *inProcessSubagentProvider) Name() string { return provider.name }

func (*inProcessSubagentProvider) Capabilities() SubagentCapabilities {
	return SubagentCapabilities{OutputSchema: true, DepthLimit: true, ToolFilter: true, Persona: true, AgentOptions: true}
}

func (provider *inProcessSubagentProvider) InheritsParentContext() bool { return provider.fork }

func (provider *inProcessSubagentProvider) Start(ctx context.Context, request SubagentStartRequest) (*SubagentRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.New("subagent request was aborted before child publication")
	}
	if request.ParentSessionID == "" {
		return nil, errors.New("in-process subagent requires a parent session")
	}
	config := SubagentToolConfig{
		Provider: provider.name, AgentOptions: request.AgentOptions, Persona: request.Persona,
		ToolFilter: request.ToolFilter, MaxDepth: request.MaxDepth,
	}
	var structured *structuredOutputRuntime
	setup := func(child *Session) error {
		if request.OutputSchema == nil {
			return nil
		}
		var err error
		structured, err = provider.engine.attachStructuredOutputRuntime(child.Header.ID, request.OutputSchema)
		return err
	}
	childID, err := provider.engine.createModelSubagentWithSetup(ctx, request.ParentSessionID, request.Label, provider.fork, "one-shot", config, setup)
	if err != nil {
		return nil, err
	}
	child, _ := provider.engine.getSession(childID)
	boundary := provider.engine.modelSubagentEventCount(childID)
	runCtx, cancel := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	cancelRun := func() {
		cancel()
		_ = provider.engine.CancelAgent(childID, AgentCancelCause{Kind: "parent"}, CancelAgentOptions{KeepInbox: false})
	}
	run := newSubagentRun(childID, cancelRun, func() error {
		<-workerDone
		return provider.engine.disposeOneShotModelSubagent(childID)
	})
	run.local = true
	go func() {
		defer close(workerDone)
		_, runErr := provider.engine.Run(runCtx, childID, PromptRequest{
			SessionID: childID, Mode: "queue", Literal: true,
			Content:         []PromptContentPart{{Type: "text", Text: "delegated prompt"}},
			preparedContent: cloneContentBlocks(request.Prompt),
		})
		cancelled := runCtx.Err() != nil
		if cancelled {
			_ = provider.engine.CancelAgent(childID, AgentCancelCause{Kind: "parent"}, CancelAgentOptions{KeepInbox: false})
		}
		idleErr := provider.engine.WaitForIdle(context.Background(), childID)
		result := provider.engine.readInProcessSubagentResult(child, boundary, cancelled, structured)
		if runErr != nil && result.StopReason == SubagentCompleted {
			result.StopReason = SubagentError
		}
		if idleErr != nil {
			result.StopReason = SubagentError
			result.Diagnostic = errors.Join(runErr, idleErr).Error()
		} else if runErr != nil && result.StopReason == SubagentError {
			result.Diagnostic = runErr.Error()
		}
		run.settle(result)
	}()
	return run, nil
}

func (e *Engine) ensureInProcessSubagentProvider(name string) (SubagentProvider, error) {
	if provider := e.GetSubagentProvider(name); provider != nil {
		return provider, nil
	}
	if name != "spawn" && name != "fork" {
		return nil, fmt.Errorf("unsupported in-process subagent provider %q", name)
	}
	provider := &inProcessSubagentProvider{engine: e, name: name, fork: name == "fork"}
	if err := e.RegisterSubagentProvider(provider); err != nil {
		return nil, err
	}
	return provider, nil
}

func (e *Engine) readInProcessSubagentResult(child *Session, boundary int, cancelled bool, structured *structuredOutputRuntime) SubagentResult {
	child.mu.Lock()
	events := append([]Event(nil), child.Events...)
	child.mu.Unlock()
	if boundary < 0 || boundary > len(events) {
		boundary = len(events)
	}
	own := events[boundary:]
	end, _ := foldModelSubagentConsumedWork(own)
	stopReason := SubagentError
	if end != nil {
		data, _ := end.Data.(map[string]any)
		reason, _ := data["reason"].(map[string]any)
		kind, _ := reason["kind"].(string)
		switch kind {
		case "completed":
			stopReason = SubagentCompleted
		case "max-tokens":
			stopReason = SubagentMaxTokens
		case "aborted":
			stopReason = SubagentAborted
		case "blocked", "rejected":
			stopReason = SubagentRefusal
		case "error", "interrupted":
			stopReason = SubagentError
		}
	}
	if cancelled && stopReason != SubagentCompleted {
		stopReason = SubagentAborted
	}
	result := SubagentResult{Output: finalAssistantOutput(own), StopReason: stopReason}
	if structured != nil {
		if value, ok := structured.capturedValue(); ok {
			result.Structured = value
		} else if stopReason == SubagentCompleted {
			if cancelled {
				result.StopReason = SubagentAborted
			} else {
				result.StopReason = SubagentError
			}
		}
	}
	return result
}
