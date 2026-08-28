package harness

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	structuredOutputToolName    = "structured_output"
	structuredOutputInstruction = "When you have your final answer, you MUST report it by calling the `structured_output` tool with arguments matching its parameter schema exactly. Do not finish with a plain text answer: only the tool call counts as your result."
)

type pendingStructuredOutput struct {
	parentCallID string
	value        any
}

type structuredOutputRuntime struct {
	mu       sync.Mutex
	staged   map[*ToolRunContext]any
	pending  *pendingStructuredOutput
	captured any
	hasValue bool
}

func (e *Engine) attachStructuredOutputRuntime(sessionID string, schema map[string]any) (*structuredOutputRuntime, error) {
	runtime := &structuredOutputRuntime{staged: map[*ToolRunContext]any{}}
	tool := Tool{
		Schema: ToolSchema{
			Name:        structuredOutputToolName,
			Description: "Report your final structured result. Call this exactly once, when your answer is complete; the arguments must match this tool's parameter schema exactly.",
			Parameters:  cloneJSON(schema).(map[string]any),
			Output: objectSchema(map[string]any{
				"recorded": map[string]any{"type": "boolean", "const": true},
			}, "recorded"),
		},
		ExecuteRuntime: func(exec *ToolRunContext) (ToolResult, error) {
			decoder := json.NewDecoder(bytes.NewReader(exec.Call.Arguments))
			decoder.UseNumber()
			var value any
			if err := decoder.Decode(&value); err != nil {
				return ToolResult{}, fmt.Errorf("INVALID_ARGS: structured_output arguments are invalid JSON: %w", err)
			}
			var extra any
			if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
				return ToolResult{}, errors.New("INVALID_ARGS: structured_output arguments must contain one JSON value")
			}
			if err := validateJSONAgainstSchema(value, schema); err != nil {
				return ToolResult{}, fmt.Errorf("INVALID_ARGS: structured_output arguments do not match the schema: %w", err)
			}
			runtime.mu.Lock()
			runtime.staged[exec] = cloneJSON(value)
			runtime.mu.Unlock()
			exec.ConcludeTurn()
			result := textToolResult("Structured output recorded.")
			result.Value = map[string]any{"recorded": true}
			return result, nil
		},
	}
	if err := e.registerSessionScopedTool(sessionID, tool); err != nil {
		return nil, err
	}
	e.mu.Lock()
	if e.structuredOutputs[sessionID] != nil {
		delete(e.scopedTools[sessionID], structuredOutputToolName)
		e.mu.Unlock()
		return nil, fmt.Errorf("structured output runtime already attached to session %s", sessionID)
	}
	e.structuredOutputs[sessionID] = runtime
	e.mu.Unlock()
	return runtime, nil
}

func (runtime *structuredOutputRuntime) capturedValue() (any, bool) {
	if runtime == nil {
		return nil, false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if !runtime.hasValue {
		return nil, false
	}
	return cloneJSON(runtime.captured), true
}

func (e *Engine) structuredOutputRuntimeForSession(sessionID string) *structuredOutputRuntime {
	e.mu.RLock()
	runtime := e.structuredOutputs[sessionID]
	e.mu.RUnlock()
	return runtime
}

func (e *Engine) structuredToolGuard(sessionID string) string {
	runtime := e.structuredOutputRuntimeForSession(sessionID)
	if runtime == nil {
		return ""
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if !runtime.hasValue && runtime.pending == nil {
		return ""
	}
	return "structured output already recorded: the run is complete, so the tool is not executed"
}

func (e *Engine) observeStructuredToolResult(s *Session, prepared *preparedToolCall, result ToolResult) {
	if prepared == nil || prepared.runtime == nil {
		return
	}
	s.mu.Lock()
	sessionID := s.Header.ID
	s.mu.Unlock()
	runtime := e.structuredOutputRuntimeForSession(sessionID)
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if prepared.call.Name == structuredOutputToolName {
		value, staged := runtime.staged[prepared.runtime]
		if !staged {
			return
		}
		delete(runtime.staged, prepared.runtime)
		if result.IsError || result.Error != nil {
			return
		}
		if prepared.call.ParentCallID == "" {
			if !runtime.hasValue {
				runtime.captured, runtime.hasValue = value, true
			}
			return
		}
		if !runtime.hasValue && runtime.pending == nil {
			runtime.pending = &pendingStructuredOutput{parentCallID: prepared.call.ParentCallID, value: value}
		}
		return
	}
	if runtime.pending == nil || runtime.pending.parentCallID != prepared.call.ID {
		return
	}
	pending := runtime.pending
	runtime.pending = nil
	if !result.IsError && result.Error == nil && !runtime.hasValue {
		runtime.captured, runtime.hasValue = pending.value, true
	}
}
