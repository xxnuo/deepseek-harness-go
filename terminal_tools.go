package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"unicode/utf8"
)

const terminalTruncatedMarker = "\n[output truncated]"

func terminalUTF8Head(text string, maxBytes int) string {
	if len(text) <= maxBytes {
		return text
	}
	if maxBytes <= 0 {
		return ""
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(text[:end]) {
		end--
	}
	return text[:end]
}

func terminalUTF8TailForResult(text string, maxBytes int) string {
	if len(text) <= maxBytes {
		return text
	}
	if maxBytes <= 0 {
		return ""
	}
	start := len(text) - maxBytes
	for start < len(text) && !utf8.ValidString(text[start:]) {
		start++
	}
	return text[start:]
}

func boundTerminalText(text string, maxBytes int) string {
	if len(text) <= maxBytes {
		return text
	}
	if len(terminalTruncatedMarker) >= maxBytes {
		return terminalUTF8TailForResult(terminalTruncatedMarker, maxBytes)
	}
	return terminalUTF8Head(text, maxBytes-len(terminalTruncatedMarker)) + terminalTruncatedMarker
}

func terminalBodyWithSuffix(content, metadata string, truncated bool, maxBytes int) string {
	suffix := metadata
	if truncated {
		suffix += terminalTruncatedMarker
	}
	complete := content + suffix
	if len(complete) <= maxBytes {
		return complete
	}
	suffix = metadata + terminalTruncatedMarker
	if len(suffix) >= maxBytes {
		return terminalUTF8TailForResult(suffix, maxBytes)
	}
	return terminalUTF8TailForResult(content, maxBytes-len(suffix)) + suffix
}

func renderTerminalSpawn(result TerminalSpawnResult, maxBytes int) string {
	label := result.SessionID
	if result.Name != "" {
		label += " (" + result.Name + ")"
	}
	prefix := fmt.Sprintf("started terminal session %s [type: %s]\n", label, result.Type)
	motd := result.MOTD
	if motd == "" {
		motd = "(no startup output)"
	}
	if len(prefix)+len(motd) <= maxBytes {
		return prefix + motd
	}
	fixed := prefix + terminalTruncatedMarker
	if len(fixed) >= maxBytes {
		return terminalUTF8Head(fixed, maxBytes)
	}
	return prefix + terminalUTF8TailForResult(motd, maxBytes-len(fixed)) + terminalTruncatedMarker
}

func renderTerminalStatus(status TerminalSessionStatus) string {
	if status.Kind == "running" {
		return "running"
	}
	exitCode, signal := "null", "null"
	if status.ExitCode != nil {
		exitCode = fmt.Sprintf("%d", *status.ExitCode)
	}
	if status.Signal != nil {
		signal = *status.Signal
	}
	return fmt.Sprintf("exited code=%s signal=%s", exitCode, signal)
}

func renderTerminalSend(result TerminalSendResult, maxBytes int) string {
	output := result.Viewport
	if output == "" {
		output = "(no new output)"
	}
	metadata := fmt.Sprintf("\n[wait: %s]\n[session: %s]", result.WaitReason, renderTerminalStatus(result.SessionStatus))
	return terminalBodyWithSuffix(output, metadata, result.Truncated, maxBytes)
}

func renderTerminalSendRead(read TerminalSendRead) string {
	if !read.Truncated {
		return read.Delta
	}
	separator := "\n"
	if read.Delta == "" || strings.HasSuffix(read.Delta, "\n") {
		separator = ""
	}
	return read.Delta + separator + "[output truncated]"
}

func renderTerminalRead(result TerminalReadResult, maxBytes int) string {
	output := result.Text
	if output == "" {
		output = "(no retained output)"
	}
	metadata := fmt.Sprintf("\n[lines: %d-%d of %d]", result.LineBegin, result.LineEnd, result.TotalLines)
	return terminalBodyWithSuffix(output, metadata, result.Truncated, maxBytes)
}

func renderTerminalList(sessions []TerminalSessionSnapshot, maxBytes int) string {
	if len(sessions) == 0 {
		return "(no terminal sessions)"
	}
	rows := make([]string, 0, len(sessions))
	for _, session := range sessions {
		name, pid := "", ""
		if session.Name != "" {
			name = " (" + session.Name + ")"
		}
		if session.PID != 0 {
			pid = fmt.Sprintf(" pid=%d", session.PID)
		}
		rows = append(rows, fmt.Sprintf("%s%s [%s] %s%s", session.SessionID, name, session.Type, renderTerminalStatus(session.Status), pid))
	}
	return terminalBodyWithSuffix(strings.Join(rows, "\n"), "", false, maxBytes)
}

func terminalToolResult(text string, value, meta any, maxBytes int) ToolResult {
	return ToolResult{Content: []ContentBlock{{Type: "text", Text: boundTerminalText(text, maxBytes)}}, Value: value, Meta: meta}
}

func terminalStatusSchema() map[string]any {
	return map[string]any{"oneOf": []any{
		objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "running"}}, "kind"),
		objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "exited"}, "exitCode": map[string]any{"oneOf": []any{map[string]any{"type": "integer"}, map[string]any{"type": "null"}}}, "signal": map[string]any{"oneOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "null"}}}}, "kind", "exitCode", "signal"),
	}}
}

func terminalSnapshotSchema(extra map[string]any, required ...string) map[string]any {
	properties := map[string]any{"sessionId": map[string]any{"type": "string"}, "name": map[string]any{"type": "string"}, "type": map[string]any{"type": "string"}, "pid": map[string]any{"type": "integer"}, "status": terminalStatusSchema()}
	for key, value := range extra {
		properties[key] = value
	}
	return objectSchema(properties, append([]string{"sessionId", "type", "status"}, required...)...)
}

func builtinTerminalTools(engine *Engine) []Tool {
	config := engine.cfg.TerminalTool
	backgroundEnabled := terminalBackgroundEnabled(config)
	maxBytes := config.MaxResultBytes

	type openInput struct {
		Type string `json:"type"`
		Name string `json:"name"`
		CWD  string `json:"cwd"`
	}
	openTool := Tool{
		Schema: ToolSchema{
			Name: "terminal_open", Description: "Create a persistent, owner-isolated terminal session from a registered backend type. Use this for shell or REPL state that must survive across tool calls.",
			Parameters: objectSchema(map[string]any{
				"type": map[string]any{"type": "string", "description": "Registered terminal backend type, usually shell."},
				"name": map[string]any{"type": "string", "description": "Optional owner-local display name."},
				"cwd":  map[string]any{"type": "string", "description": "Initial working directory. Defaults to the session workspace."},
			}, "type"),
			Output: terminalSnapshotSchema(map[string]any{"motd": map[string]any{"type": "string"}}, "motd"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var input openInput
			if err := decodeToolArguments(call, &input); err != nil {
				return ToolResult{}, err
			}
			result, err := engine.OpenTerminal(ctx, call.SessionID, TerminalSpawnRequest{Type: input.Type, Name: input.Name, CWD: input.CWD})
			if err != nil {
				return ToolResult{}, err
			}
			return terminalToolResult(renderTerminalSpawn(result, maxBytes), result, nil, maxBytes), nil
		},
	}

	type sendInput struct {
		SessionID       string `json:"sessionId"`
		Text            string `json:"text"`
		Submit          *bool  `json:"submit"`
		RunInBackground bool   `json:"run_in_background"`
	}
	sendProperties := map[string]any{
		"sessionId": map[string]any{"type": "string", "description": "Terminal session id returned by terminal_open or terminal_list."},
		"text":      map[string]any{"type": "string", "description": "UTF-8 text to write to the terminal."},
		"submit":    map[string]any{"type": "boolean", "description": "Submit Enter after text (default true)."},
	}
	sendDescription := "Send text to a persistent terminal. By default Enter is submitted and the call waits for a prompt, stdin wait, output silence, timeout, or session exit."
	if backgroundEnabled {
		sendProperties["run_in_background"] = map[string]any{"type": "boolean", "description": "Return a job id immediately; collect with job_output or stop with job_kill."}
		sendDescription += " Background mode returns a job id for job_output/job_kill."
	}
	sendTool := Tool{
		Schema: ToolSchema{Name: "terminal_send", Description: sendDescription, Parameters: objectSchema(sendProperties, "sessionId", "text"), Output: map[string]any{"oneOf": []any{
			objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "background"}, "jobId": map[string]any{"type": "string"}}, "kind", "jobId"),
			objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "foreground"}, "viewport": map[string]any{"type": "string"}, "waitReason": map[string]any{"type": "string"}, "sessionStatus": terminalStatusSchema(), "truncated": map[string]any{"type": "boolean"}}, "kind", "viewport", "waitReason", "sessionStatus", "truncated"),
		}}},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var input sendInput
			if err := decodeToolArguments(call, &input); err != nil {
				return ToolResult{}, err
			}
			if input.SessionID == "" {
				return ToolResult{}, errors.New("sessionId must be a non-empty string")
			}
			submit := true
			if input.Submit != nil {
				submit = *input.Submit
			}
			request := TerminalSendRequest{Text: input.Text, Submit: submit}
			if input.RunInBackground {
				if !backgroundEnabled {
					return ToolResult{}, errors.New("background terminal sends are disabled by tool-terminal configuration")
				}
				if err := ctx.Err(); err != nil {
					return ToolResult{}, err
				}
				label := input.Text
				if label == "" {
					label = "(input)"
				}
				jobID, err := engine.jobs.startManaged(call.SessionID, "pty-send", input.SessionID+": "+label, maxBytes, func() (*managedJobHandle, error) {
					operation, startErr := engine.StartTerminalSend(context.Background(), call.SessionID, input.SessionID, request)
					if startErr != nil {
						return nil, startErr
					}
					results := make(chan managedJobResult, 1)
					var cancelled atomic.Bool
					go func() {
						<-operation.Done()
						result, resultErr := operation.Result()
						if resultErr != nil {
							results <- managedJobResult{Status: jobFailed, Detail: resultErr.Error()}
						} else if cancelled.Load() {
							results <- managedJobResult{Status: jobKilled, Detail: terminalSendDetail(result)}
						} else {
							results <- managedJobResult{Status: jobCompleted, Detail: terminalSendDetail(result)}
						}
						close(results)
					}()
					return &managedJobHandle{
						Done: results,
						Cancel: func(string) error {
							cancelled.Store(true)
							operation.Cancel()
							return nil
						},
						ReadOutput: func() (string, bool) {
							return renderTerminalSendRead(operation.ReadOutput()), false
						},
					}, nil
				})
				if err != nil {
					return ToolResult{}, err
				}
				value := map[string]any{"kind": "background", "jobId": jobID}
				return terminalToolResult("started background job "+jobID, value, nil, maxBytes), nil
			}
			result, err := engine.SendTerminal(ctx, call.SessionID, input.SessionID, request)
			if err != nil {
				return ToolResult{}, err
			}
			value := map[string]any{
				"kind": "foreground", "viewport": result.Viewport, "waitReason": result.WaitReason,
				"sessionStatus": result.SessionStatus, "truncated": result.Truncated,
			}
			return terminalToolResult(renderTerminalSend(result, maxBytes), value, result, maxBytes), nil
		},
	}

	type sessionInput struct {
		SessionID string `json:"sessionId"`
	}
	type readInput struct {
		SessionID string `json:"sessionId"`
		Offset    *int   `json:"offset"`
		Count     *int   `json:"count"`
	}
	readTool := Tool{
		Schema: ToolSchema{
			Name: "terminal_read", Description: "Read a bounded page of retained output from a persistent terminal without sending input.",
			Parameters: objectSchema(map[string]any{
				"sessionId": map[string]any{"type": "string", "description": "Terminal session id."},
				"offset":    map[string]any{"type": "integer", "description": "Newest-relative line offset (default 0)."},
				"count":     map[string]any{"type": "integer", "description": "Requested line count (default 500)."},
			}, "sessionId"),
			Output: objectSchema(map[string]any{"text": map[string]any{"type": "string"}, "totalLines": map[string]any{"type": "integer"}, "lineBegin": map[string]any{"type": "integer"}, "lineEnd": map[string]any{"type": "integer"}, "truncated": map[string]any{"type": "boolean"}}, "text", "totalLines", "lineBegin", "lineEnd", "truncated"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var input readInput
			if err := decodeToolArguments(call, &input); err != nil {
				return ToolResult{}, err
			}
			request := TerminalReadRequest{}
			if input.Offset != nil {
				if *input.Offset < 0 {
					return ToolResult{}, errors.New("PTY read offset must be a non-negative integer")
				}
				request.Offset = *input.Offset
			}
			if input.Count != nil {
				if *input.Count <= 0 {
					return ToolResult{}, errors.New("PTY read count must be a positive integer")
				}
				request.Count = *input.Count
			}
			result, err := engine.ReadTerminal(call.SessionID, input.SessionID, request)
			if err != nil {
				return ToolResult{}, err
			}
			return terminalToolResult(renderTerminalRead(result, maxBytes), result, nil, maxBytes), nil
		},
	}

	type signalInput struct {
		SessionID string `json:"sessionId"`
		Signal    string `json:"signal"`
	}
	signalTool := Tool{
		Schema: ToolSchema{
			Name: "terminal_signal", Description: "Send an allowed signal to the current foreground process group of a persistent terminal.",
			Parameters: objectSchema(map[string]any{
				"sessionId": map[string]any{"type": "string", "description": "Terminal session id."},
				"signal": map[string]any{"type": "string", "enum": []string{
					TerminalSignalInterrupt, TerminalSignalTerminate, TerminalSignalKill, TerminalSignalStop, TerminalSignalHangup,
				}},
			}, "sessionId", "signal"),
			Output: objectSchema(map[string]any{"delivered": map[string]any{"type": "boolean", "const": true}, "targetPgid": map[string]any{"type": "integer"}}, "delivered", "targetPgid"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var input signalInput
			if err := decodeToolArguments(call, &input); err != nil {
				return ToolResult{}, err
			}
			result, err := engine.SignalTerminal(call.SessionID, input.SessionID, input.Signal)
			if err != nil {
				return ToolResult{}, err
			}
			text := fmt.Sprintf("delivered %s to foreground process group %d", input.Signal, result.TargetPGID)
			return terminalToolResult(text, result, nil, maxBytes), nil
		},
	}

	closeTool := Tool{
		Schema: ToolSchema{
			Name: "terminal_close", Description: "Close one persistent terminal and wait until its captured owned process tree is gone.",
			Parameters: objectSchema(map[string]any{"sessionId": map[string]any{"type": "string", "description": "Terminal session id."}}, "sessionId"),
			Output:     objectSchema(map[string]any{"sessionId": map[string]any{"type": "string"}, "outcome": map[string]any{"type": "string", "enum": []string{"closed", "already-closing"}}}, "sessionId", "outcome"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var input sessionInput
			if err := decodeToolArguments(call, &input); err != nil {
				return ToolResult{}, err
			}
			closed, err := engine.CloseTerminal(call.SessionID, input.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			outcome, text := "already-closing", "terminal session "+input.SessionID+" was already closing"
			if closed {
				outcome, text = "closed", "closed terminal session "+input.SessionID
			}
			value := map[string]any{"sessionId": input.SessionID, "outcome": outcome}
			return terminalToolResult(text, value, nil, maxBytes), nil
		},
	}

	listTool := Tool{
		Schema: ToolSchema{Name: "terminal_list", Description: "List persistent terminal sessions owned by the current agent.", Parameters: objectSchema(map[string]any{}), Output: map[string]any{"type": "array", "items": terminalSnapshotSchema(nil)}},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			sessions, err := engine.ListTerminals(call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			return terminalToolResult(renderTerminalList(sessions, maxBytes), sessions, nil, maxBytes), nil
		},
	}

	return []Tool{openTool, sendTool, readTool, signalTool, closeTool, listTool}
}

func terminalSendDetail(result TerminalSendResult) string {
	if result.SessionStatus.Kind == "running" {
		return "wait: " + result.WaitReason
	}
	if result.SessionStatus.ExitCode != nil {
		return fmt.Sprintf("session exited: %d", *result.SessionStatus.ExitCode)
	}
	if result.SessionStatus.Signal != nil {
		return "session exited: " + *result.SessionStatus.Signal
	}
	return "session exited: unknown"
}
