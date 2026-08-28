package harness

import (
	"context"
	"fmt"

	"github.com/dop251/goja"
)

type codeToolDispatchEntry struct {
	call      ToolCall
	arguments map[string]any
	vm        *goja.Runtime
	resolve   func(any) error
	reject    func(any) error

	parallel bool
	prepared *preparedToolCall
	result   ToolResult
	settled  bool
}

type codeToolDispatchOutcome struct {
	entry  *codeToolDispatchEntry
	result ToolResult
}

type codeToolDispatcher struct {
	engine  *Engine
	session *Session
	outer   *ToolRunContext
	ctx     context.Context
	tasks   chan<- jsTask
	parent  ToolCall

	submitCh  chan *codeToolDispatchEntry
	settledCh chan codeToolDispatchOutcome
	stopCh    chan struct{}
	doneCh    chan struct{}
}

func newCodeToolDispatcher(e *Engine, session *Session, outer *ToolRunContext, ctx context.Context, tasks chan<- jsTask, parent ToolCall) *codeToolDispatcher {
	dispatcher := &codeToolDispatcher{
		engine: e, session: session, outer: outer, ctx: ctx, tasks: tasks, parent: parent,
		submitCh:  make(chan *codeToolDispatchEntry, 1024),
		settledCh: make(chan codeToolDispatchOutcome, e.cfg.MaxParallelToolCalls),
		stopCh:    make(chan struct{}), doneCh: make(chan struct{}),
	}
	go dispatcher.run()
	return dispatcher
}

func (d *codeToolDispatcher) submit(entry *codeToolDispatchEntry) bool {
	select {
	case d.submitCh <- entry:
		return true
	case <-d.stopCh:
		return false
	case <-d.ctx.Done():
		return false
	}
}

func (d *codeToolDispatcher) closeAndDrain() {
	select {
	case <-d.stopCh:
	default:
		close(d.stopCh)
	}
	<-d.doneCh
}

func (d *codeToolDispatcher) run() {
	defer close(d.doneCh)
	cap := d.engine.cfg.MaxParallelToolCalls
	if cap < 1 {
		cap = 1
	}
	pending := make([]*codeToolDispatchEntry, 0)
	commit := make([]*codeToolDispatchEntry, 0)
	active := 0
	exclusiveActive := false
	closing := false

	for {
		if d.ctx.Err() != nil {
			closing = true
		}
		progressed := false
		for {
			select {
			case outcome := <-d.settledCh:
				outcome.entry.result = outcome.result
				outcome.entry.settled = true
				active--
				if !outcome.entry.parallel {
					exclusiveActive = false
				}
				progressed = true
			default:
				goto outcomesDrained
			}
		}

	outcomesDrained:
		for len(commit) > 0 && commit[0].settled {
			entry := commit[0]
			commit = commit[1:]
			d.commit(entry)
			progressed = true
		}

		if closing {
			for {
				select {
				case entry := <-d.submitCh:
					pending = append(pending, entry)
				default:
					goto submissionsDrained
				}
			}
		submissionsDrained:
			for _, entry := range pending {
				d.reject(entry, fmt.Sprintf("run_code run is over (run_code settled); %s tool call abandoned", entry.call.Name))
			}
			pending = nil
		}

		for !closing && len(pending) > 0 {
			if d.ctx.Err() != nil {
				closing = true
				break
			}
			entry := pending[0]
			parallel := d.engine.nestedToolIsParallel(d.session, entry.call)
			if parallel {
				if exclusiveActive || active >= cap {
					break
				}
			} else if active > 0 || exclusiveActive {
				break
			}
			pending = pending[1:]
			entry.parallel = parallel
			if !parallel {
				exclusiveActive = true
			}
			commit = append(commit, entry)
			d.start(entry)
			if entry.settled {
				if !parallel {
					exclusiveActive = false
				}
			} else {
				active++
			}
			progressed = true
			if entry.settled {
				break
			}
		}

		if closing && active == 0 && len(commit) == 0 {
			return
		}
		if progressed {
			continue
		}

		select {
		case entry := <-d.submitCh:
			if closing {
				d.reject(entry, fmt.Sprintf("run_code run is over (run_code settled); %s not dispatched", entry.call.Name))
			} else {
				pending = append(pending, entry)
			}
		case outcome := <-d.settledCh:
			outcome.entry.result = outcome.result
			outcome.entry.settled = true
			active--
			if !outcome.entry.parallel {
				exclusiveActive = false
			}
		case <-d.stopCh:
			closing = true
		case <-d.ctx.Done():
			closing = true
		}
	}
}

func (d *codeToolDispatcher) start(entry *codeToolDispatchEntry) {
	data := map[string]any{
		"rootCallId": d.parent.ID, "parentCallId": d.parent.ID,
		"subCallId": entry.call.ID, "name": entry.call.Name, "arguments": entry.arguments,
	}
	if _, err := d.engine.appendEvent(d.session, "tool/code-dispatch-start", data); err != nil {
		entry.result = toolRuntimeFailureResult(err)
		entry.settled = true
		return
	}
	entry.prepared = d.engine.prepareNestedToolCall(d.ctx, d.session, entry.call)
	if !entry.prepared.started {
		entry.result = entry.prepared.result
		entry.settled = true
		return
	}
	go func() {
		d.settledCh <- codeToolDispatchOutcome{entry: entry, result: d.engine.dispatchToolCall(d.ctx, entry.prepared)}
	}()
}

func (d *codeToolDispatcher) commit(entry *codeToolDispatchEntry) {
	result := entry.result
	var hookContexts []hookInjectedContext
	if entry.prepared != nil {
		entry.prepared.result = result
		result, hookContexts = d.engine.finishNestedToolCall(d.ctx, d.session, entry.prepared)
	}
	for _, value := range hookContextsAsToolContexts(hookContexts) {
		d.outer.DeferContext(value)
	}
	if !result.IsError && contentHasImage(result.Content) {
		d.outer.DeferContext(ToolContext{
			Content: cloneContentBlocks(result.Content),
			Source:  map[string]any{"kind": "plugin", "plugin": "tools-code-mode"},
		})
	}
	for _, value := range result.AdditionalContexts {
		d.outer.DeferContext(value)
	}
	if !result.IsError && result.ConcludesTurn {
		d.outer.ConcludeTurn()
	}
	settled := map[string]any{
		"rootCallId": d.parent.ID, "parentCallId": d.parent.ID,
		"subCallId": entry.call.ID, "name": entry.call.Name, "arguments": entry.arguments,
		"isError": result.IsError, "content": result.Content,
	}
	_, appendErr := d.engine.appendEvent(d.session, "tool/code-dispatch", settled)
	value := canonicalCodeToolValue(entry.call.Name, result)
	sendJSTask(d.ctx, d.tasks, func() error {
		if appendErr != nil {
			return entry.reject(toolCallJSError(entry.vm, entry.call.Name, appendErr.Error()))
		}
		if result.IsError {
			message := "tool call failed"
			if result.Error != nil && result.Error.Message != "" {
				message = result.Error.Message
			} else if len(result.Content) > 0 {
				message = result.Content[0].Text
			}
			return entry.reject(toolCallJSError(entry.vm, entry.call.Name, message))
		}
		return entry.resolve(value)
	})
}

func (d *codeToolDispatcher) reject(entry *codeToolDispatchEntry, message string) {
	sendJSTask(d.ctx, d.tasks, func() error {
		return entry.reject(toolCallJSError(entry.vm, entry.call.Name, message))
	})
}

func (e *Engine) nestedToolIsParallel(s *Session, call ToolCall) bool {
	tools, err := e.codeToolsForSession(s)
	if err != nil || call.Name == "run_code" {
		return false
	}
	tool, ok := tools[call.Name]
	return ok && toolConcurrencySafe(tool, call)
}

func hookContextsAsToolContexts(contexts []hookInjectedContext) []ToolContext {
	result := make([]ToolContext, 0, len(contexts))
	for _, value := range contexts {
		content := make([]ContentBlock, 0, len(value.texts))
		for _, text := range value.texts {
			if text != "" {
				content = append(content, ContentBlock{Type: "text", Text: text})
			}
		}
		if len(content) > 0 {
			result = append(result, ToolContext{Content: content, Source: map[string]any{"kind": "plugin", "plugin": hookPluginName(value.dialect)}})
		}
	}
	return result
}

func contentHasImage(content []ContentBlock) bool {
	for _, block := range content {
		if block.Type == "image" || block.Attachment != nil {
			return true
		}
	}
	return false
}
