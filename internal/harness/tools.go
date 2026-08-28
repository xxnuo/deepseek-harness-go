package harness

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"os/exec"
	pathpkg "path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	toolOutputLimit      = 1 << 20
	readToolLineLimit    = 2000
	readToolMaxLineChars = 2000
	readToolMaxBytes     = 50 * 1024
	globToolMaxResults   = 100
	grepToolMaxMatches   = 250
	grepToolMaxLineBytes = 2000
)

func registerBuiltinTools(e *Engine) error {
	registered := make([]Tool, 0, 16)
	// The upstream base bundle mounts exactly one shell dialect on a platform.
	// Persistent shell plugins are Agent-preset overlays and do not own this
	// host-level executor registration.
	shellActive := false
	if runtime.GOOS == "windows" {
		shellActive = e.hostPluginActive("@deepseek-ai/dsh-tool-pwsh")
	} else {
		shellActive = e.hostPluginActive("@deepseek-ai/dsh-tool-bash")
	}
	if shellActive {
		registered = append(registered, builtinShellTool(e))
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tool-fs") {
		registered = append(registered, builtinReadTool(e), builtinReadImageTool(e), builtinWriteTool(e), builtinEditTool(e))
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tool-fs-search") {
		registered = append(registered, builtinGlobTool(e), builtinGrepTool(e))
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tool-str-replace-editor") {
		registered = append(registered, builtinStrReplaceEditorTool(e))
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tool-jobs") {
		registered = append(registered, builtinJobOutputTool(e), builtinJobListTool(e), builtinJobKillTool(e))
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tool-todo") {
		registered = append(registered, builtinTodoTool(e))
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tool-skill") {
		registered = append(registered, builtinSkillTool(e))
	}
	// The terminal registry and model-facing tools are mounted by the Agent
	// preset's isolated persistent-shell group. Host inventory must not gate
	// this registration: shipped minimal owns the terminal stack even though
	// dsh-tool-terminal is absent from the base Host composition.
	if !e.cfg.TerminalTool.Disabled && !e.hasRegisteredTool("terminal_open") {
		registered = append(registered, builtinTerminalTools(e)...)
	}
	for _, tool := range registered {
		if err := e.RegisterTool(tool); err != nil {
			return err
		}
	}
	if e.hostPluginActive("@deepseek-ai/dsh-tool-session-query") {
		return registerSessionQueryTools(e)
	}
	return nil
}

func objectSchema(properties map[string]any, required ...string) map[string]any {
	schema := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func decodeToolArguments(call ToolCall, dst any) error {
	raw := bytes.TrimSpace(call.Arguments)
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid arguments for %s: %w", call.Name, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("invalid arguments for %s: multiple JSON values", call.Name)
		}
		return fmt.Errorf("invalid arguments for %s: %w", call.Name, err)
	}
	return nil
}

func textToolResult(text string) ToolResult {
	return ToolResult{Content: []ContentBlock{{Type: "text", Text: text}}}
}

func toolExecutionError(err error) *ToolError {
	var outputErr *toolOutputError
	if errors.As(err, &outputErr) {
		return &ToolError{Name: "ToolOutputError", Code: "INVALID_TOOL_OUTPUT", Message: outputErr.Error()}
	}
	message := err.Error()
	code := "TOOL_EXECUTION"
	if prefix, _, ok := strings.Cut(message, ":"); ok && prefix != "" {
		valid := true
		for _, character := range prefix {
			if character != '_' && (character < 'A' || character > 'Z') && (character < '0' || character > '9') {
				valid = false
				break
			}
		}
		if valid {
			code = prefix
		}
	}
	return &ToolError{Name: "ToolExecutionError", Code: code, Message: message}
}

func builtinTodoTool(e *Engine) Tool {
	type input struct {
		Todos json.RawMessage `json:"todos"`
	}
	type itemInput struct {
		Content *string `json:"content"`
		Status  *string `json:"status"`
	}
	allowParallel := true
	if e != nil && e.cfg.TodoAllowParallelInProgress != nil {
		allowParallel = *e.cfg.TodoAllowParallelInProgress
	}
	description := "Record the complete task list for the current work. Each call replaces the previous list; multiple tasks may be in progress when work runs in parallel."
	if !allowParallel {
		description = "Record the complete task list for the current work. Each call replaces the previous list; keep at most one task in progress."
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "todo_write",
			Description: description,
			Parameters: objectSchema(map[string]any{
				"todos": map[string]any{
					"type": "array", "description": "The complete task list, replacing any previous list.",
					"items": objectSchema(map[string]any{
						"content": map[string]any{"type": "string"},
						"status":  map[string]any{"type": "string", "enum": []string{"pending", "in_progress", "completed"}},
					}, "content", "status"),
				},
			}, "todos"),
			Output: objectSchema(map[string]any{
				"todos":  map[string]any{"type": "array", "items": objectSchema(map[string]any{"content": map[string]any{"type": "string"}, "status": map[string]any{"type": "string"}}, "content", "status")},
				"counts": objectSchema(map[string]any{"pending": map[string]any{"type": "integer"}, "inProgress": map[string]any{"type": "integer"}, "completed": map[string]any{"type": "integer"}}, "pending", "inProgress", "completed"),
			}, "todos", "counts"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if len(in.Todos) == 0 || bytes.Equal(bytes.TrimSpace(in.Todos), []byte("null")) {
				return ToolResult{}, errors.New("todo_write: `todos` is required and must be an array")
			}
			var rawItems []json.RawMessage
			if err := json.Unmarshal(in.Todos, &rawItems); err != nil || rawItems == nil {
				return ToolResult{}, errors.New("todo_write: `todos` must be an array")
			}
			todos := make([]TodoItem, 0, len(rawItems))
			seen := make(map[string]struct{}, len(rawItems))
			counts := map[string]int{"pending": 0, "in_progress": 0, "completed": 0}
			for i, raw := range rawItems {
				if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
					return ToolResult{}, fmt.Errorf("todo_write: todo %d must be an object", i)
				}
				var item itemInput
				if err := decodeToolArguments(ToolCall{Name: call.Name, Arguments: raw}, &item); err != nil {
					return ToolResult{}, err
				}
				if item.Content == nil {
					return ToolResult{}, fmt.Errorf("todo_write: todo %d is missing `content`", i)
				}
				content := strings.TrimSpace(*item.Content)
				if content == "" {
					return ToolResult{}, errors.New("todo_write: `content` must be a non-empty string")
				}
				if _, exists := seen[content]; exists {
					return ToolResult{}, fmt.Errorf("todo_write: duplicate content %q", content)
				}
				seen[content] = struct{}{}
				if item.Status == nil {
					return ToolResult{}, fmt.Errorf("todo_write: todo %d is missing `status`", i)
				}
				if _, ok := counts[*item.Status]; !ok {
					return ToolResult{}, fmt.Errorf("todo_write: invalid status %q", *item.Status)
				}
				counts[*item.Status]++
				todos = append(todos, TodoItem{Content: content, Status: *item.Status})
			}
			if !allowParallel && counts["in_progress"] > 1 {
				return ToolResult{}, fmt.Errorf("todo_write: at most one task may be in_progress (got %d)", counts["in_progress"])
			}
			if call.SessionID == "" {
				return ToolResult{}, errors.New("todo_write requires an owning agent session")
			}
			s, err := e.getSession(call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			if _, err := e.appendEvent(s, "todo/write", map[string]any{"todos": todos}); err != nil {
				return ToolResult{}, err
			}
			result := textToolResult(fmt.Sprintf(
				"Updated todo list: %d pending, %d in progress, %d completed.",
				counts["pending"], counts["in_progress"], counts["completed"],
			))
			result.Value = map[string]any{"todos": todos, "counts": map[string]any{"pending": counts["pending"], "inProgress": counts["in_progress"], "completed": counts["completed"]}}
			return result, nil
		},
	}
}

func builtinSkillTool(e *Engine) Tool {
	type input struct {
		Name *string `json:"name"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "skill",
			Description: "Load the full instructions for an available skill by its exact catalog name.",
			Parameters: objectSchema(map[string]any{
				"name": map[string]any{"type": "string", "description": "The exact skill name from the available skills list."},
			}, "name"),
			Output: objectSchema(map[string]any{
				"name": map[string]any{"type": "string"}, "provider": map[string]any{"type": "string"},
				"resourceBase": objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "directory"}, "path": map[string]any{"type": "string"}}, "kind", "path"),
				"content":      map[string]any{"type": "string"},
			}, "name", "provider", "resourceBase", "content"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if in.Name == nil {
				return ToolResult{}, errors.New("skill: `name` is required")
			}
			name := *in.Name
			if !validSkillName(name) {
				return ToolResult{}, fmt.Errorf("invalid skill name %q", name)
			}
			records, rpcErr := e.skillRecordsForSession(call.SessionID)
			if rpcErr != nil {
				return ToolResult{}, rpcErr
			}
			for _, record := range records {
				if record.name != name {
					continue
				}
				if !record.modelInvocable {
					return ToolResult{}, fmt.Errorf("skill %q is not available for model invocation", name)
				}
				base := escapeSkillText(filepath.Dir(record.path))
				result := textToolResult(strings.Join([]string{
					fmt.Sprintf("<skill_content name=\"%s\">", name),
					"<skill_resources>",
					"Base directory for this skill: " + base,
					"Resolve relative paths mentioned by this skill against the base directory before using them. Load referenced resources only as needed.",
					"</skill_resources>",
					"",
					"<skill_instructions>",
					record.content,
					"</skill_instructions>",
					"</skill_content>",
				}, "\n"))
				result.Value = map[string]any{"name": record.name, "provider": record.provider, "resourceBase": map[string]any{"kind": "directory", "path": filepath.Dir(record.path)}, "content": record.content}
				return result, nil
			}
			return ToolResult{}, fmt.Errorf("skill %q is unknown or no longer available", name)
		},
	}
}

func escapeSkillText(value string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(value)
}

func builtinReadTool(e *Engine) Tool {
	type input struct {
		FilePath string `json:"file_path"`
		Offset   *int   `json:"offset"`
		Limit    *int   `json:"limit"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "read", Description: "Read a UTF-8 text file with line numbers.",
			Parameters: objectSchema(map[string]any{
				"file_path": map[string]any{"type": "string", "description": "Path to read, resolved relative to the session workspace."},
				"offset":    map[string]any{"type": "integer", "minimum": 1},
				"limit":     map[string]any{"type": "integer", "minimum": 1, "maximum": readToolLineLimit},
			}, "file_path"),
			Output: objectSchema(map[string]any{
				"path":   map[string]any{"type": "string"},
				"offset": map[string]any{"type": "integer"},
				"lines": map[string]any{"type": "array", "items": objectSchema(map[string]any{
					"number": map[string]any{"type": "integer"},
					"text":   map[string]any{"type": "string"},
				}, "number", "text")},
				"totalLines": map[string]any{"type": "integer"},
			}, "path", "offset", "lines", "totalLines"),
		},
		IsConcurrencySafe: alwaysConcurrencySafe,
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			caps := readToolCaps(e, call.SessionID)
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			target, err := resolveFSTarget(call.Workspace, in.FilePath)
			if err != nil {
				return ToolResult{}, err
			}
			if err := ctx.Err(); err != nil {
				return ToolResult{}, err
			}
			offset, limit := 1, caps.limit
			if in.Offset != nil {
				offset = *in.Offset
			}
			if in.Limit != nil {
				limit = *in.Limit
			}
			if offset < 1 {
				return ToolResult{}, errors.New("read: offset must be a positive integer")
			}
			if limit < 1 {
				return ToolResult{}, errors.New("read: limit must be a positive integer")
			}
			if limit > caps.limit {
				return ToolResult{}, fmt.Errorf("read: limit must be less than or equal to %d", caps.limit)
			}
			if fileInfo, statErr := os.Stat(target.targetKey); statErr == nil && fileInfo.Size() >= int64(caps.streamMinSize) {
				valueLines, totalLines, streamedVersion, truncatedByBytes, streamErr := readStreamVersionedFile(target.targetKey, offset, limit, caps.maxLineChars, caps.maxBytes)
				if streamErr != nil {
					if errors.Is(streamErr, fs.ErrNotExist) {
						e.fsState.observe(call.SessionID, target, fsObservation{})
						return ToolResult{}, fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("cannot read %q: not found", target.displayPath))
					}
					return ToolResult{}, streamErr
				}
				if offset > totalLines && !(totalLines == 0 && offset == 1) {
					return ToolResult{}, fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("offset %d is out of range for %q (%d lines)", offset, target.displayPath, totalLines))
				}
				var out strings.Builder
				for _, line := range valueLines {
					fmt.Fprintf(&out, "%d: %s\n", line["number"], line["text"])
				}
				endLine := offset - 1
				if len(valueLines) > 0 {
					endLine = valueLines[len(valueLines)-1]["number"].(int)
				}
				if truncatedByBytes {
					fmt.Fprintf(&out, "\n(Output capped. Showing lines %d-%d. Use offset=%d to continue.)", offset, endLine, endLine+1)
				} else if endLine < totalLines {
					fmt.Fprintf(&out, "\n(Showing lines %d-%d of %d. Use offset=%d to continue.)", offset, endLine, totalLines, endLine+1)
				} else {
					fmt.Fprintf(&out, "\n(End of file - total %d lines)", totalLines)
				}
				e.fsState.observe(call.SessionID, target, fsObservation{present: true, version: streamedVersion})
				return ToolResult{Content: []ContentBlock{{Type: "text", Text: out.String()}}, Value: map[string]any{"path": target.displayPath, "offset": offset, "lines": valueLines, "totalLines": totalLines}}, nil
			}
			data, version, _, err := readVersionedFile(target.targetKey)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					e.fsState.observe(call.SessionID, target, fsObservation{})
					return ToolResult{}, fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("cannot read %q: not found", target.displayPath))
				}
				return ToolResult{}, err
			}
			if bytes.IndexByte(data, 0) >= 0 {
				return ToolResult{}, errors.New("read: binary file is not supported")
			}
			lines := splitReadToolLines(string(data))
			if offset > len(lines) && !(len(lines) == 0 && offset == 1) {
				return ToolResult{}, fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("offset %d is out of range for %q (%d lines)", offset, target.displayPath, len(lines)))
			}
			valueLines := make([]map[string]any, 0, min(limit, max(0, len(lines)-offset+1)))
			outputBytes := 0
			truncatedByBytes := false
			for index := offset - 1; index < len(lines) && len(valueLines) < limit; index++ {
				text := truncateReadToolLine(lines[index], caps.maxLineChars)
				lineBytes := len([]byte(text))
				if len(valueLines) > 0 {
					lineBytes++
				}
				if outputBytes+lineBytes > caps.maxBytes {
					truncatedByBytes = true
					break
				}
				outputBytes += lineBytes
				valueLines = append(valueLines, map[string]any{"number": index + 1, "text": text})
			}
			var out strings.Builder
			for _, line := range valueLines {
				fmt.Fprintf(&out, "%d: %s\n", line["number"], line["text"])
			}
			endLine := offset - 1
			if len(valueLines) > 0 {
				endLine = valueLines[len(valueLines)-1]["number"].(int)
			}
			if truncatedByBytes {
				fmt.Fprintf(&out, "\n(Output capped. Showing lines %d-%d. Use offset=%d to continue.)", offset, endLine, endLine+1)
			} else if endLine < len(lines) {
				fmt.Fprintf(&out, "\n(Showing lines %d-%d of %d. Use offset=%d to continue.)", offset, endLine, len(lines), endLine+1)
			} else {
				fmt.Fprintf(&out, "\n(End of file - total %d lines)", len(lines))
			}
			e.fsState.observe(call.SessionID, target, fsObservation{present: true, version: version})
			return ToolResult{
				Content: []ContentBlock{{Type: "text", Text: out.String()}},
				Value:   map[string]any{"path": target.displayPath, "offset": offset, "lines": valueLines, "totalLines": len(lines)},
			}, nil
		},
	}
}

// readStreamVersionedFile keeps the read window bounded for large files while
// still hashing every byte so optimistic filesystem observations retain the
// same version contract as the regular path.
func readStreamVersionedFile(path string, offset, limit, maxLineChars, maxBytes int) ([]map[string]any, int, fsFileVersion, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, fsFileVersion{}, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, 0, fsFileVersion{}, false, err
	}
	if !info.Mode().IsRegular() {
		return nil, 0, fsFileVersion{}, false, fsPolicyError("FS_NOT_REGULAR_FILE", fmt.Sprintf("cannot read %q: not a regular file", path))
	}
	hash := sha256.New()
	reader := bufio.NewReaderSize(file, 64*1024)
	lines := make([]map[string]any, 0, limit)
	total, outputBytes := 0, 0
	truncated := false
	for {
		part, readErr := reader.ReadString('\n')
		if len(part) > 0 {
			_, _ = hash.Write([]byte(part))
			if bytes.IndexByte([]byte(part), 0) >= 0 {
				return nil, 0, fsFileVersion{}, false, errors.New("read: binary file is not supported")
			}
			hasNewline := strings.HasSuffix(part, "\n")
			line := strings.TrimSuffix(part, "\n")
			line = strings.TrimSuffix(line, "\r")
			if total >= offset-1 && len(lines) < limit {
				text := truncateReadToolLine(line, maxLineChars)
				lineBytes := len([]byte(text))
				if len(lines) > 0 {
					lineBytes++
				}
				if outputBytes+lineBytes > maxBytes {
					truncated = true
				} else {
					outputBytes += lineBytes
					lines = append(lines, map[string]any{"number": total + 1, "text": text})
				}
			}
			total++
			if !hasNewline && readErr == io.EOF {
				break
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, 0, fsFileVersion{}, false, readErr
		}
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return lines, total, fsFileVersion{info: info, digest: digest}, truncated, nil
}

func splitReadToolLines(text string) []string {
	if text == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	for index := range lines {
		lines[index] = strings.TrimSuffix(lines[index], "\r")
	}
	return lines
}

func truncateReadToolLine(line string, maxChars int) string {
	characters := []rune(line)
	if len(characters) <= maxChars {
		return line
	}
	return string(characters[:maxChars]) + fmt.Sprintf("... (line truncated to %d chars)", maxChars)
}

func builtinWriteTool(e *Engine) Tool {
	type input struct {
		FilePath          string  `json:"file_path"`
		Content           string  `json:"content"`
		SandboxPermission *string `json:"sandbox_permissions"`
		Justification     *string `json:"justification"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "write", Description: "Create or replace a UTF-8 file under the current sandbox policy.",
			Parameters: objectSchema(map[string]any{
				"file_path":           map[string]any{"type": "string", "description": "Path to write, resolved relative to the session workspace."},
				"content":             map[string]any{"type": "string"},
				"sandbox_permissions": map[string]any{"type": "string", "enum": []string{sandboxWorkspaceWrite, sandboxDangerFull}, "description": "A wider one-shot sandbox mode; requires justification and user approval."},
				"justification":       map[string]any{"type": "string", "description": "Why this exact operation needs wider access."},
			}, "file_path", "content"),
			Output: objectSchema(map[string]any{
				"path":      map[string]any{"type": "string"},
				"operation": map[string]any{"type": "string", "enum": []any{"create", "update"}},
				"before":    map[string]any{"oneOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "null"}}},
				"after":     map[string]any{"type": "string"},
			}, "path", "operation", "before", "after"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			mode, err := e.resolveSandboxMode(ctx, call, in.SandboxPermission, in.Justification, "operation")
			if err != nil {
				return ToolResult{}, err
			}
			path, err := e.sandboxMutationPathWithMode(call, in.FilePath, mode)
			if err != nil {
				return ToolResult{}, sandboxErrorWithEscalationHint(err, "operation")
			}
			target, err := resolveFSTarget(call.Workspace, in.FilePath)
			if err != nil {
				return ToolResult{}, err
			}
			target.targetKey = path
			if err := ctx.Err(); err != nil {
				return ToolResult{}, err
			}
			intent := e.fsState.writeIntent(call.SessionID, target)
			version, existed, before, err := e.guardedFSWrite(target, []byte(in.Content), intent)
			if err != nil {
				return ToolResult{}, err
			}
			e.fsState.observe(call.SessionID, target, fsObservation{present: true, version: version})
			var beforeValue any
			if existed {
				beforeValue = before
			}
			return ToolResult{
				Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf("wrote %s (%d bytes)", in.FilePath, len(in.Content))}},
				Value: map[string]any{
					"path": target.displayPath, "operation": map[bool]string{true: "update", false: "create"}[existed],
					"before": beforeValue, "after": in.Content,
				},
			}, nil
		},
	}
}

func builtinEditTool(e *Engine) Tool {
	type input struct {
		FilePath          string  `json:"file_path"`
		OldString         string  `json:"old_string"`
		NewString         string  `json:"new_string"`
		ReplaceAll        bool    `json:"replace_all"`
		SandboxPermission *string `json:"sandbox_permissions"`
		Justification     *string `json:"justification"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "edit", Description: "Replace literal text in an existing UTF-8 file under the current sandbox policy.",
			Parameters: objectSchema(map[string]any{
				"file_path":           map[string]any{"type": "string", "description": "Path to edit, resolved relative to the session workspace."},
				"old_string":          map[string]any{"type": "string"},
				"new_string":          map[string]any{"type": "string"},
				"replace_all":         map[string]any{"type": "boolean"},
				"sandbox_permissions": map[string]any{"type": "string", "enum": []string{sandboxWorkspaceWrite, sandboxDangerFull}, "description": "A wider one-shot sandbox mode; requires justification and user approval."},
				"justification":       map[string]any{"type": "string", "description": "Why this exact operation needs wider access."},
			}, "file_path", "old_string", "new_string"),
			Output: objectSchema(map[string]any{
				"path":   map[string]any{"type": "string"},
				"before": map[string]any{"type": "string"},
				"after":  map[string]any{"type": "string"},
			}, "path", "before", "after"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if in.OldString == "" {
				return ToolResult{}, errors.New("edit: old_string must not be empty")
			}
			mode, err := e.resolveSandboxMode(ctx, call, in.SandboxPermission, in.Justification, "operation")
			if err != nil {
				return ToolResult{}, err
			}
			path, err := e.sandboxMutationPathWithMode(call, in.FilePath, mode)
			if err != nil {
				return ToolResult{}, sandboxErrorWithEscalationHint(err, "operation")
			}
			target, err := resolveFSTarget(call.Workspace, in.FilePath)
			if err != nil {
				return ToolResult{}, err
			}
			target.targetKey = path
			if err := ctx.Err(); err != nil {
				return ToolResult{}, err
			}
			expected, err := e.fsState.editIntent(call.SessionID, target)
			if err != nil {
				return ToolResult{}, err
			}
			before, after := "", ""
			version, err := e.guardedFSEdit(target, expected, func(input string) (string, error) {
				before = input
				count := strings.Count(input, in.OldString)
				if count == 0 {
					return "", fsPolicyError("FS_EDIT_NOT_FOUND", "edit: old_string was not found")
				}
				if !in.ReplaceAll && count != 1 {
					return "", fsPolicyError("FS_AMBIGUOUS_EDIT", fmt.Sprintf("edit: old_string occurs %d times; make it unique or set replace_all", count))
				}
				n := 1
				if in.ReplaceAll {
					n = -1
				}
				after = strings.Replace(input, in.OldString, in.NewString, n)
				return after, nil
			})
			if err != nil {
				return ToolResult{}, err
			}
			e.fsState.observe(call.SessionID, target, fsObservation{present: true, version: version})
			return ToolResult{
				Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf("edited %s", in.FilePath)}},
				Value:   map[string]any{"path": target.displayPath, "before": before, "after": after},
			}, nil
		},
	}
}

func builtinGlobTool(e *Engine) Tool {
	type input struct {
		Pattern string  `json:"pattern"`
		Path    *string `json:"path"`
	}
	tool := Tool{
		Schema: ToolSchema{
			Name: "glob", Description: "Find files below a path by glob pattern.",
			Parameters: objectSchema(map[string]any{
				"pattern": map[string]any{"type": "string", "description": "Go path.Match pattern; ** is supported recursively."},
				"path":    map[string]any{"type": "string", "description": "Optional directory, resolved relative to the session workspace."},
			}, "pattern"),
			Output: objectSchema(map[string]any{
				"root":  map[string]any{"type": "string"},
				"paths": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			}, "root", "paths"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			caps := searchToolCaps(e, call.SessionID)
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if strings.TrimSpace(in.Pattern) == "" {
				return ToolResult{}, errors.New("glob: pattern must be a non-empty string")
			}
			if in.Path != nil && strings.TrimSpace(*in.Path) == "" {
				return ToolResult{}, errors.New("glob: path must be a non-empty string when given")
			}
			args := []string{"--no-config", "--files", "--glob=" + in.Pattern, "--sort=modified", "--no-ignore", "--hidden"}
			for _, name := range []string{".git", ".svn", ".hg", ".bzr", ".jj", ".sl"} {
				args = append(args, "--glob=!**/"+name, "--glob=!**/"+name+"/**")
			}
			if in.Path != nil {
				args = append(args, "--", *in.Path)
			}
			run, err := runRipgrep(ctx, call.Workspace, "glob", args, caps)
			if err != nil {
				return ToolResult{}, err
			}
			displayMatches := make([]string, 0)
			for _, line := range strings.Split(strings.TrimSuffix(run.stdout, "\n"), "\n") {
				if strings.TrimSpace(line) == "" {
					continue
				}
				displayMatches = append(displayMatches, displaySearchPath(call.Workspace, line))
			}
			displayRoot := "."
			if in.Path != nil {
				displayRoot = displaySearchPath(call.Workspace, *in.Path)
			}
			page := retainGlobPage(displayMatches, caps, displayRoot)
			return ToolResult{
				Content: []ContentBlock{{Type: "text", Text: renderGlobToolResultWithCaps(displayMatches, caps, displayRoot)}},
				Value:   map[string]any{"root": displayRoot, "paths": displayMatches},
				Meta:    capSearchMeta(map[string]any{"shape": "paths", "paths": stringsToAny(page), "truncated": len(displayMatches) > caps.globMaxResults, "total": len(displayMatches)}, caps.searchMetaMaxBytes),
			}, nil
		},
	}
	return tool
}

func globMatch(pattern, path string) bool {
	pattern = strings.TrimPrefix(filepath.ToSlash(pattern), "./")
	path = strings.TrimPrefix(filepath.ToSlash(path), "./")
	if !strings.Contains(pattern, "/") {
		matched, err := pathpkg.Match(pattern, pathpkg.Base(path))
		return err == nil && matched
	}
	return matchGlobSegments(strings.Split(pattern, "/"), strings.Split(path, "/"))
}

func matchGlobSegments(pattern, candidate []string) bool {
	if len(pattern) == 0 {
		return len(candidate) == 0
	}
	if pattern[0] == "**" {
		return matchGlobSegments(pattern[1:], candidate) || len(candidate) > 0 && matchGlobSegments(pattern, candidate[1:])
	}
	if len(candidate) == 0 {
		return false
	}
	matched, err := pathpkg.Match(pattern[0], candidate[0])
	return err == nil && matched && matchGlobSegments(pattern[1:], candidate[1:])
}

func renderGlobToolResult(paths []string) string {
	if len(paths) == 0 {
		return "No files found"
	}
	if len(paths) <= globToolMaxResults {
		return strings.Join(paths, "\n")
	}
	return strings.Join(paths[:globToolMaxResults], "\n") + fmt.Sprintf(
		"\n\n(Showing %d of %d paths. The complete result could not be saved; narrow pattern or path to see more.)",
		globToolMaxResults, len(paths),
	)
}

type readCaps struct{ limit, maxLineChars, maxBytes, streamMinSize int }
type searchCaps struct {
	sampleOverCap                                    bool
	globMaxResults, grepMaxMatches, grepMaxLineBytes int
	timeout                                          time.Duration
	rawOutputMaxBytes, searchMetaMaxBytes            int
	grace                                            time.Duration
	stderrMaxBytes                                   int
}

type searchRun struct {
	stdout, stderr string
	exitCode       int
	noMatches      bool
	timedOut       bool
}

func runRipgrep(ctx context.Context, workspace, toolName string, args []string, caps searchCaps) (searchRun, error) {
	if err := ctx.Err(); err != nil {
		return searchRun{}, fmt.Errorf("SEARCH_ABORTED: %s was aborted", toolName)
	}
	program := strings.TrimSpace(os.Getenv("DSH_RIPGREP_PATH"))
	if program == "" {
		program, _ = exec.LookPath("rg")
	}
	if program == "" {
		return searchRun{}, errors.New("SEARCH_FAILED: ripgrep executable is unavailable")
	}
	timeout := caps.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.Command(program, args...)
	cmd.Dir, cmd.Env = workspace, scrubbedChildEnv(map[string]string{"LC_ALL": "C", "LANG": "C", "NO_COLOR": "1"})
	configureChildProcess(cmd)
	var stdout, stderr limitedSearchBuffer
	stdout.maxBytes, stderr.maxBytes = caps.rawOutputMaxBytes, caps.stderrMaxBytes
	stderr.tail = true
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Start()
	if err != nil {
		if ctx.Err() != nil || runCtx.Err() != nil {
			return searchRun{}, fmt.Errorf("SEARCH_ABORTED: %s was aborted", toolName)
		}
		return searchRun{}, fmt.Errorf("SEARCH_FAILED: %s could not start ripgrep: %w", toolName, err)
	}
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	select {
	case err = <-waitErr:
	case <-runCtx.Done():
		_ = terminateChildProcess(cmd)
		grace := caps.grace
		if grace <= 0 {
			grace = 3 * time.Second
		}
		select {
		case err = <-waitErr:
		case <-time.After(grace):
			_ = killChildProcess(cmd)
			err = <-waitErr
		}
		if ctx.Err() != nil || runCtx.Err() == context.DeadlineExceeded {
			return searchRun{stdout: stdout.String(), stderr: stderr.String(), timedOut: runCtx.Err() == context.DeadlineExceeded}, fmt.Errorf("SEARCH_ABORTED: %s was aborted", toolName)
		}
	}
	if ctx.Err() != nil {
		return searchRun{stdout: stdout.String(), stderr: stderr.String()}, fmt.Errorf("SEARCH_ABORTED: %s was aborted", toolName)
	}
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return searchRun{}, fmt.Errorf("SEARCH_FAILED: %s search failed: %w", toolName, err)
		}
	}
	if stdout.truncated {
		return searchRun{}, fmt.Errorf("SEARCH_RAW_OUTPUT_OVERFLOW: %s produced more than %d bytes of raw output", toolName, caps.rawOutputMaxBytes)
	}
	if exitCode != 0 && exitCode != 1 {
		message := strings.TrimSpace(stderr.String())
		if stderr.truncated {
			message += " [stderr truncated]"
		}
		if strings.Contains(strings.ToLower(message), "regex parse error") || strings.Contains(strings.ToLower(message), "error parsing glob") {
			return searchRun{}, fmt.Errorf("SEARCH_INVALID_PATTERN: %s pattern rejected by ripgrep: %s", toolName, message)
		}
		return searchRun{}, fmt.Errorf("SEARCH_FAILED: %s search failed (exit %d): %s", toolName, exitCode, message)
	}
	return searchRun{stdout: stdout.String(), stderr: stderr.String(), exitCode: exitCode, noMatches: exitCode == 1}, nil
}

type limitedSearchBuffer struct {
	buffer    bytes.Buffer
	truncated bool
	maxBytes  int
	tail      bool
}

func (b *limitedSearchBuffer) String() string { return b.buffer.String() }
func (b *limitedSearchBuffer) Len() int       { return b.buffer.Len() }

func (b *limitedSearchBuffer) Write(p []byte) (int, error) {
	limit := b.maxBytes
	if limit <= 0 {
		limit = toolOutputLimit
	}
	if len(p) > limit-b.Len() {
		if b.tail {
			combined := append(append([]byte(nil), b.buffer.Bytes()...), p...)
			if len(combined) > limit {
				combined = combined[len(combined)-limit:]
			}
			b.buffer.Reset()
			_, _ = b.buffer.Write(combined)
		} else {
			remaining := limit - b.Len()
			if remaining > 0 {
				_, _ = b.buffer.Write(p[:remaining])
			}
		}
		b.truncated = true
		return len(p), nil
	}
	return b.buffer.Write(p)
}

func readToolCaps(e *Engine, sessionID string) readCaps {
	caps := readCaps{readToolLineLimit, readToolMaxLineChars, readToolMaxBytes, 10 * 1024 * 1024}
	if e != nil && sessionID != "" {
		if s, err := e.getSession(sessionID); err == nil {
			if runtimeConfig, err := e.runtimeForSession(s); err == nil {
				if runtimeConfig.readLimit > 0 {
					caps.limit = runtimeConfig.readLimit
				}
				if runtimeConfig.readMaxLineLength > 0 {
					caps.maxLineChars = runtimeConfig.readMaxLineLength
				}
				if runtimeConfig.readMaxBytes > 0 {
					caps.maxBytes = runtimeConfig.readMaxBytes
				}
				if runtimeConfig.readStreamMinSize > 0 {
					caps.streamMinSize = runtimeConfig.readStreamMinSize
				}
			}
		}
	}
	return caps
}

func searchToolCaps(e *Engine, sessionID string) searchCaps {
	caps := searchCaps{globMaxResults: globToolMaxResults, grepMaxMatches: grepToolMaxMatches, grepMaxLineBytes: grepToolMaxLineBytes, timeout: 30 * time.Second, rawOutputMaxBytes: 20_000_000, searchMetaMaxBytes: 65_536, grace: 3 * time.Second, stderrMaxBytes: 64 * 1024}
	if e != nil && sessionID != "" {
		if s, err := e.getSession(sessionID); err == nil {
			if runtimeConfig, err := e.runtimeForSession(s); err == nil {
				caps.sampleOverCap = runtimeConfig.globSampleOverCapResults
				if runtimeConfig.globMaxResults > 0 {
					caps.globMaxResults = runtimeConfig.globMaxResults
				}
				if runtimeConfig.grepMaxMatches > 0 {
					caps.grepMaxMatches = runtimeConfig.grepMaxMatches
				}
				if runtimeConfig.grepMaxLineBytes > 0 {
					caps.grepMaxLineBytes = runtimeConfig.grepMaxLineBytes
				}
				if runtimeConfig.searchTimeout > 0 {
					caps.timeout = runtimeConfig.searchTimeout
				}
				if runtimeConfig.rawOutputMaxBytes > 0 {
					caps.rawOutputMaxBytes = runtimeConfig.rawOutputMaxBytes
				}
				if runtimeConfig.searchMetaMaxBytes > 0 {
					caps.searchMetaMaxBytes = runtimeConfig.searchMetaMaxBytes
				}
				if runtimeConfig.searchGrace > 0 {
					caps.grace = runtimeConfig.searchGrace
				}
				if runtimeConfig.searchStderrMaxBytes > 0 {
					caps.stderrMaxBytes = runtimeConfig.searchStderrMaxBytes
				}
			}
		}
	}
	return caps
}

func capSearchMeta(meta map[string]any, maxBytes int) map[string]any {
	if maxBytes <= 0 {
		return meta
	}
	encoded, _ := json.Marshal(meta)
	if len(encoded) <= maxBytes {
		return meta
	}
	copyMeta := cloneJSON(meta).(map[string]any)
	if files, ok := copyMeta["files"].([]any); ok {
		for len(files) > 1 {
			copyMeta["files"] = files[:len(files)-1]
			copyMeta["truncated"] = true
			encoded, _ = json.Marshal(copyMeta)
			if len(encoded) <= maxBytes {
				return copyMeta
			}
			files = files[:len(files)-1]
		}
	}
	if paths, ok := copyMeta["paths"].([]any); ok {
		for len(paths) > 1 {
			copyMeta["paths"] = paths[:len(paths)-1]
			copyMeta["truncated"] = true
			encoded, _ = json.Marshal(copyMeta)
			if len(encoded) <= maxBytes {
				return copyMeta
			}
			paths = paths[:len(paths)-1]
		}
	}
	return copyMeta
}

func stringsToAny(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

func truncateSearchPreview(value string, maxBytes int) string {
	if maxBytes <= 0 || len([]byte(value)) <= maxBytes {
		return value
	}
	for len([]byte(value)) > maxBytes {
		value = string([]rune(value)[:len([]rune(value))-1])
	}
	return value
}

type globPathSample struct {
	items        []string
	shown, total int
}

func sampleGlobPaths(paths []string, maxItems int, root string) globPathSample {
	type group struct {
		key   string
		items []string
	}
	groups := make([]group, 0)
	indices := map[string]int{}
	for _, path := range paths {
		relative := path
		trimmedRoot := strings.TrimRight(root, "/")
		if root == "." {
			relative = strings.TrimPrefix(relative, "./")
		} else if trimmedRoot != "" {
			if relative == trimmedRoot {
				relative = ""
			} else if strings.HasPrefix(relative, trimmedRoot+"/") {
				relative = strings.TrimPrefix(relative, trimmedRoot+"/")
			}
		}
		relative = strings.TrimLeft(relative, "/")
		key := strings.SplitN(relative, "/", 2)[0]
		index, exists := indices[key]
		if !exists {
			indices[key] = len(groups)
			groups = append(groups, group{key: key, items: []string{path}})
			continue
		}
		groups[index].items = append(groups[index].items, path)
	}
	taken := make([][]string, len(groups))
	active := make([]int, len(groups))
	for index := range groups {
		active[index] = index
	}
	positions := make([]int, len(groups))
	count := 0
	for len(active) > 0 && count < maxItems {
		next := make([]int, 0, len(active))
		for _, groupIndex := range active {
			if count >= maxItems {
				break
			}
			position := positions[groupIndex]
			taken[groupIndex] = append(taken[groupIndex], groups[groupIndex].items[position])
			positions[groupIndex]++
			count++
			if positions[groupIndex] < len(groups[groupIndex].items) {
				next = append(next, groupIndex)
			}
		}
		active = next
	}
	items := make([]string, 0, count)
	shown := 0
	for _, bucket := range taken {
		if len(bucket) == 0 {
			continue
		}
		shown++
		items = append(items, bucket...)
	}
	return globPathSample{items: items, shown: shown, total: len(groups)}
}

func retainGlobPage(paths []string, caps searchCaps, root string) []string {
	if len(paths) <= caps.globMaxResults {
		return append([]string(nil), paths...)
	}
	if !caps.sampleOverCap {
		return append([]string(nil), paths[:caps.globMaxResults]...)
	}
	return sampleGlobPaths(paths, caps.globMaxResults, root).items
}

func renderGlobToolResultWithCaps(paths []string, caps searchCaps, root string, spillPath ...string) string {
	if len(paths) == 0 {
		return "No files found"
	}
	if len(paths) <= caps.globMaxResults {
		return strings.Join(paths, "\n")
	}
	recovery := "The complete result could not be saved; narrow pattern or path to see more."
	if len(spillPath) > 0 && spillPath[0] != "" {
		recovery = "Full sorted result stored at: " + spillPath[0] + ". Use read with offset/limit, or grep this path to search within it."
	}
	if caps.sampleOverCap {
		sample := sampleGlobPaths(paths, caps.globMaxResults, root)
		basis := "."
		if sample.total != len(paths) {
			basis = fmt.Sprintf(", sampled across %d of the %d top-level entries this pattern matched instead of taken in modification-time order.", sample.shown, sample.total)
			if sample.shown < sample.total {
				basis += " Narrow path to inspect a specific subtree."
			}
		}
		return strings.Join(sample.items, "\n") + fmt.Sprintf("\n\n(Showing %d of %d paths%s %s)", len(sample.items), len(paths), basis, recovery)
	}
	return strings.Join(paths[:caps.globMaxResults], "\n") + fmt.Sprintf("\n\n(Showing %d of %d paths. %s)", caps.globMaxResults, len(paths), recovery)
}

func builtinGrepTool(e *Engine) Tool {
	type input struct {
		Pattern string  `json:"pattern"`
		Path    *string `json:"path"`
		Include *string `json:"include"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "grep", Description: "Search UTF-8 files below a path with a regular expression.",
			Parameters: objectSchema(map[string]any{
				"pattern": map[string]any{"type": "string"},
				"path":    map[string]any{"type": "string", "description": "Optional file or directory, resolved relative to the session workspace."},
				"include": map[string]any{"type": "string", "description": "Optional filename glob."},
			}, "pattern"),
			Output: objectSchema(map[string]any{
				"matches": map[string]any{"type": "array", "items": objectSchema(map[string]any{
					"path":       map[string]any{"type": "string"},
					"lineNumber": map[string]any{"type": "integer"},
					"line":       map[string]any{"type": "string"},
				}, "path", "lineNumber", "line")},
			}, "matches"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			caps := searchToolCaps(e, call.SessionID)
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if in.Pattern == "" {
				return ToolResult{}, errors.New("grep: pattern must be a non-empty string")
			}
			if in.Path != nil && strings.TrimSpace(*in.Path) == "" {
				return ToolResult{}, errors.New("grep: path must be a non-empty string when given")
			}
			if in.Include != nil {
				if strings.TrimSpace(*in.Include) == "" || strings.HasPrefix(*in.Include, "!") {
					return ToolResult{}, errors.New("grep: include must be a positive non-empty glob filter")
				}
				depth := 0
				for _, r := range *in.Include {
					switch r {
					case '{':
						depth++
					case '}':
						if depth > 0 {
							depth--
						}
					case ',':
						if depth == 0 {
							return ToolResult{}, errors.New("grep: include must be one glob, not a comma-separated list")
						}
					}
				}
			}
			args := []string{"--no-config", "--json", "--regexp=" + in.Pattern}
			if in.Include != nil {
				args = append(args, "--glob="+*in.Include)
			}
			if in.Path != nil {
				args = append(args, "--", *in.Path)
			}
			run, err := runRipgrep(ctx, call.Workspace, "grep", args, caps)
			if err != nil {
				return ToolResult{}, err
			}
			matches := []map[string]any{}
			for _, raw := range strings.Split(strings.TrimSuffix(run.stdout, "\n"), "\n") {
				if raw == "" {
					continue
				}
				var record struct {
					Type string `json:"type"`
					Data *struct {
						Path *struct {
							Text *string `json:"text"`
						} `json:"path"`
						LineNumber *int `json:"line_number"`
						Lines      *struct {
							Text  *string `json:"text"`
							Bytes *string `json:"bytes"`
						} `json:"lines"`
					} `json:"data"`
				}
				if err := json.Unmarshal([]byte(raw), &record); err != nil {
					return ToolResult{}, fmt.Errorf("SEARCH_FAILED: grep received malformed ripgrep output: %w", err)
				}
				if record.Type != "match" {
					continue
				}
				if record.Data == nil || record.Data.Path == nil || record.Data.Path.Text == nil || record.Data.LineNumber == nil || record.Data.Lines == nil {
					return ToolResult{}, errors.New("SEARCH_FAILED: grep received malformed ripgrep output: match record is missing required fields")
				}
				line := ""
				if record.Data.Lines.Text != nil {
					line = strings.TrimSuffix(*record.Data.Lines.Text, "\n")
				} else if record.Data.Lines.Bytes != nil {
					line = "(line is not valid UTF-8)"
				} else {
					return ToolResult{}, errors.New("SEARCH_FAILED: grep received malformed ripgrep output: match record has no line text or bytes")
				}
				matches = append(matches, map[string]any{"path": displaySearchPath(call.Workspace, *record.Data.Path.Text), "lineNumber": *record.Data.LineNumber, "line": line})
			}
			retained := matches
			if len(retained) > caps.grepMaxMatches {
				retained = retained[:caps.grepMaxMatches]
			}
			metaFiles := map[string][]any{}
			fileOrder := []string{}
			for _, match := range retained {
				path, _ := match["path"].(string)
				if _, ok := metaFiles[path]; !ok {
					fileOrder = append(fileOrder, path)
				}
				line, _ := match["line"].(string)
				line = truncateSearchPreview(line, caps.grepMaxLineBytes)
				metaFiles[path] = append(metaFiles[path], map[string]any{"lineNumber": match["lineNumber"], "line": line})
			}
			groups := make([]any, 0, len(fileOrder))
			for _, path := range fileOrder {
				groups = append(groups, map[string]any{"path": path, "matches": metaFiles[path]})
			}
			return ToolResult{
				Content: []ContentBlock{{Type: "text", Text: renderGrepToolResultWithCaps(matches, caps)}},
				Value:   map[string]any{"matches": matches},
				Meta:    capSearchMeta(map[string]any{"shape": "matches", "files": groups, "truncated": len(matches) > caps.grepMaxMatches, "total": len(matches)}, caps.searchMetaMaxBytes),
			}, nil
		},
	}
}

func renderGrepToolResultWithCaps(matches []map[string]any, caps searchCaps) string {
	return renderGrepToolResultWithCapsAndSpill(matches, caps, "")
}

func renderGrepToolResultWithCapsAndSpill(matches []map[string]any, caps searchCaps, spillPath string) string {
	if len(matches) == 0 {
		return "No matches found"
	}
	kept := min(len(matches), caps.grepMaxMatches)
	var out strings.Builder
	if kept < len(matches) {
		fmt.Fprintf(&out, "Found %d of %d matches\n\n", kept, len(matches))
	} else if len(matches) == 1 {
		out.WriteString("Found 1 match\n\n")
	} else {
		fmt.Fprintf(&out, "Found %d matches\n\n", len(matches))
	}
	lastPath := ""
	for index, match := range matches[:kept] {
		path, _ := match["path"].(string)
		lineNumber, _ := match["lineNumber"].(int)
		line, _ := match["line"].(string)
		if path != lastPath {
			if index > 0 {
				out.WriteString("\n\n")
			}
			out.WriteString(path)
			out.WriteByte('\n')
			lastPath = path
		}
		fmt.Fprintf(&out, "Line %d: %s", lineNumber, previewGrepToolLineWithCap(line, caps.grepMaxLineBytes))
		if index+1 < kept && matches[index+1]["path"] == path {
			out.WriteByte('\n')
		}
	}
	if kept < len(matches) {
		recovery := "The complete result could not be saved; narrow pattern, path, or include to see more."
		if spillPath != "" {
			recovery = "Full grep result stored at: " + spillPath + ". Use read with offset/limit, or grep this path to search within it."
		}
		out.WriteString("\n\n(" + recovery + ")")
	}
	return out.String()
}

func fullGrepSearchResult(matches []map[string]any, caps searchCaps) string {
	if len(matches) == 0 {
		return "No matches found"
	}
	all := caps
	all.grepMaxMatches = len(matches)
	return renderGrepToolResultWithCaps(matches, all)
}

func (e *Engine) applySearchSpillPolicy(s *Session, call ToolCall, result ToolResult) ToolResult {
	result, _ = e.applySearchSpillPolicyResult(s, call, result)
	return result
}

func (e *Engine) applySearchSpillPolicyResult(s *Session, call ToolCall, result ToolResult) (ToolResult, bool) {
	if e.cfg.Spill.Disabled || result.IsError || result.Error != nil || call.ParentCallID != "" {
		return result, false
	}
	caps := searchToolCaps(e, call.SessionID)
	switch call.Name {
	case "glob":
		value, ok := result.Value.(map[string]any)
		if !ok {
			return result, false
		}
		paths, ok := searchResultPaths(value["paths"])
		if !ok || len(paths) <= caps.globMaxResults {
			return result, false
		}
		path, err := e.saveSpillText(s, "glob-results.txt", strings.Join(paths, "\n"))
		if err != nil {
			return result, false
		}
		root, _ := value["root"].(string)
		result.Content = []ContentBlock{{Type: "text", Text: renderGlobToolResultWithCaps(paths, caps, root, path)}}
	case "grep":
		value, ok := result.Value.(map[string]any)
		if !ok {
			return result, false
		}
		matches, ok := searchResultMatches(value["matches"])
		if !ok || len(matches) <= caps.grepMaxMatches {
			return result, false
		}
		path, err := e.saveSpillText(s, "grep-results.txt", fullGrepSearchResult(matches, caps))
		if err != nil {
			return result, false
		}
		result.Content = []ContentBlock{{Type: "text", Text: renderGrepToolResultWithCapsAndSpill(matches, caps, path)}}
	}
	return result, true
}

func searchResultPaths(value any) ([]string, bool) {
	switch values := value.(type) {
	case []string:
		return append([]string(nil), values...), true
	case []any:
		paths := make([]string, len(values))
		for index, value := range values {
			path, ok := value.(string)
			if !ok {
				return nil, false
			}
			paths[index] = path
		}
		return paths, true
	default:
		return nil, false
	}
}

func searchResultMatches(value any) ([]map[string]any, bool) {
	switch values := value.(type) {
	case []map[string]any:
		return append([]map[string]any(nil), values...), true
	case []any:
		matches := make([]map[string]any, len(values))
		for index, value := range values {
			match, ok := value.(map[string]any)
			if !ok {
				return nil, false
			}
			if number, ok := eventSeqNumber(match["lineNumber"]); ok {
				match = cloneJSON(match).(map[string]any)
				match["lineNumber"] = number
			}
			matches[index] = match
		}
		return matches, true
	default:
		return nil, false
	}
}

func previewGrepToolLineWithCap(line string, maxBytes int) string {
	if len([]byte(line)) <= maxBytes {
		return line
	}
	return truncateUTF8(line, maxBytes) + " (line truncated)"
}

func renderGrepToolResult(matches []map[string]any) string {
	if len(matches) == 0 {
		return "No matches found"
	}
	kept := min(len(matches), grepToolMaxMatches)
	var out strings.Builder
	if kept < len(matches) {
		fmt.Fprintf(&out, "Found %d of %d matches\n\n", kept, len(matches))
	} else if len(matches) == 1 {
		out.WriteString("Found 1 match\n\n")
	} else {
		fmt.Fprintf(&out, "Found %d matches\n\n", len(matches))
	}
	lastPath := ""
	for index, match := range matches[:kept] {
		path, _ := match["path"].(string)
		lineNumber, _ := match["lineNumber"].(int)
		line, _ := match["line"].(string)
		if path != lastPath {
			if index > 0 {
				out.WriteString("\n\n")
			}
			out.WriteString(path)
			out.WriteByte('\n')
			lastPath = path
		}
		fmt.Fprintf(&out, "Line %d: %s", lineNumber, previewGrepToolLine(line))
		if index+1 < kept && matches[index+1]["path"] == path {
			out.WriteByte('\n')
		}
	}
	if kept < len(matches) {
		out.WriteString("\n\n(The complete result could not be saved; narrow pattern, path, or include to see more.)")
	}
	return out.String()
}

func previewGrepToolLine(line string) string {
	if len([]byte(line)) <= grepToolMaxLineBytes {
		return line
	}
	return truncateUTF8(line, grepToolMaxLineBytes) + " (line truncated)"
}

func builtinShellTool(e *Engine) Tool {
	type input struct {
		Command           string   `json:"command"`
		Description       string   `json:"description"`
		TimeoutMS         *float64 `json:"timeoutMs"`
		Workdir           string   `json:"workdir"`
		RunInBackground   bool     `json:"run_in_background"`
		SandboxPermission *string  `json:"sandbox_permissions"`
		Justification     *string  `json:"justification"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: shellToolName, Description: shellToolDescription,
			Parameters: objectSchema(map[string]any{
				"command":             map[string]any{"type": "string"},
				"description":         map[string]any{"type": "string"},
				"timeoutMs":           map[string]any{"type": "number", "description": "Timeout in milliseconds. The executor applies its configured default and cap."},
				"workdir":             map[string]any{"type": "string", "description": "Workspace-relative directory."},
				"run_in_background":   map[string]any{"type": "boolean", "description": "Run in the background and return a job id immediately (collect with job_output, stop with job_kill). No timeout applies."},
				"sandbox_permissions": map[string]any{"type": "string", "enum": []string{sandboxWorkspaceWrite, sandboxDangerFull}, "description": "A wider one-shot sandbox mode; requires justification and user approval."},
				"justification":       map[string]any{"type": "string", "description": "Why this exact command needs wider access."},
			}, "command", "description"),
			Output: map[string]any{"oneOf": []any{
				objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "background"}, "jobId": map[string]any{"type": "string"}}, "kind", "jobId"),
				objectSchema(map[string]any{
					"kind":     map[string]any{"type": "string", "const": "foreground"},
					"exitCode": map[string]any{"oneOf": []any{map[string]any{"type": "integer"}, map[string]any{"type": "null"}}},
					"signal":   map[string]any{"oneOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "null"}}},
					"timedOut": map[string]any{"type": "boolean"}, "aborted": map[string]any{"type": "boolean"}, "timeoutMs": map[string]any{"type": "number"},
					"stdout":  objectSchema(map[string]any{"text": map[string]any{"type": "string"}, "truncated": map[string]any{"type": "boolean"}}, "text", "truncated"),
					"stderr":  objectSchema(map[string]any{"text": map[string]any{"type": "string"}, "truncated": map[string]any{"type": "boolean"}}, "text", "truncated"),
					"sandbox": objectSchema(map[string]any{"mode": map[string]any{"type": "string"}, "denied": map[string]any{"type": "boolean"}}, "mode", "denied"),
				}, "kind", "exitCode", "signal", "timedOut", "aborted", "timeoutMs", "stdout", "stderr"),
			}},
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if strings.TrimSpace(in.Command) == "" {
				return ToolResult{}, fmt.Errorf("%s: command is required", shellToolName)
			}
			persistent := false
			persistentTimeout := persistentShellTimeout
			persistentMaxOutputChars := editorOutputLimit
			runtimeConfig := defaultAgentRuntime(e.cfg)
			if call.SessionID != "" {
				if session, sessionErr := e.getSession(call.SessionID); sessionErr == nil {
					var runtimeErr error
					runtimeConfig, runtimeErr = e.runtimeForSession(session)
					if runtimeErr != nil {
						return ToolResult{}, runtimeErr
					}
					persistent = runtimeConfig.persistentBash && shellToolPersistent
					if runtimeConfig.persistentBashTimeout > 0 {
						persistentTimeout = runtimeConfig.persistentBashTimeout
					}
					if runtimeConfig.persistentBashMaxOutputChars > 0 {
						persistentMaxOutputChars = runtimeConfig.persistentBashMaxOutputChars
					}
				}
			}
			if !persistent && strings.TrimSpace(in.Description) == "" {
				return ToolResult{}, fmt.Errorf("%s: invalid description: expected a non-empty string", shellToolName)
			}
			if !persistent && !runtimeConfig.bashEnableRunInBackground && in.RunInBackground {
				return ToolResult{}, fmt.Errorf("run_in_background is disabled for this deployment (enableRunInBackground: false)")
			}
			if !persistent && in.TimeoutMS != nil && (math.IsNaN(*in.TimeoutMS) || math.IsInf(*in.TimeoutMS, 0) || *in.TimeoutMS <= 0) {
				return ToolResult{}, fmt.Errorf("%s: invalid timeoutMs: expected a positive number", shellToolName)
			}
			mode, err := e.resolveSandboxMode(ctx, call, in.SandboxPermission, in.Justification, "command")
			if err != nil {
				return ToolResult{}, err
			}
			if persistent {
				output, err := e.shells.runWithModeOptions(ctx, call.SessionID, call.Workspace, mode, in.Command, persistentTimeout, persistentMaxOutputChars)
				if err != nil {
					return ToolResult{}, err
				}
				if mode != sandboxDangerFull && sandboxOutputDenied(output) {
					output = appendShellStatus(output, sandboxDenialMarker(mode))
					output = appendShellStatus(output, sandboxEscalationHint("command"))
				}
				result := textToolResult(output)
				result.Value = output
				return result, nil
			}
			workspace, err := filepath.Abs(call.Workspace)
			if err != nil {
				return ToolResult{}, err
			}
			workdir, err := shellWorkdir(workspace, in.Workdir)
			if err != nil {
				return ToolResult{}, err
			}
			info, err := os.Stat(workdir)
			if err != nil || !info.IsDir() {
				return ToolResult{}, fmt.Errorf("%s: invalid workdir %q", shellToolName, in.Workdir)
			}
			program, args, err := shellInvocation(in.Command, mode, workspace, workdir)
			if err != nil {
				return ToolResult{}, err
			}
			dshEnvironment, err := e.collectShellEnvironment(call)
			if err != nil {
				return ToolResult{}, err
			}
			environment := shellEnvironment()
			for key, value := range dshEnvironment {
				environment[key] = value
			}
			if in.RunInBackground {
				if err := ctx.Err(); err != nil {
					return ToolResult{}, err
				}
				cmd := exec.Command(program, args...)
				cmd.Dir = workdir
				cmd.Env = scrubbedChildEnv(environment)
				configureChildProcess(cmd)
				childState, err := prepareShellChild(cmd, mode, workspace)
				if err != nil {
					return ToolResult{}, err
				}
				id, err := e.jobs.start(call.SessionID, shellToolName, in.Command, mode, cmd, childState)
				if err != nil {
					return ToolResult{}, err
				}
				result := textToolResult("started background job " + id)
				result.Value = map[string]any{"kind": "background", "jobId": id}
				return result, nil
			}
			timeout := 60 * time.Second
			if in.TimeoutMS != nil {
				milliseconds := *in.TimeoutMS
				if milliseconds > 120000 {
					milliseconds = 120000
				}
				timeout = time.Duration(milliseconds * float64(time.Millisecond))
			}
			runCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			cmd := exec.CommandContext(runCtx, program, args...)
			cmd.Dir = workdir
			cmd.Env = scrubbedChildEnv(environment)
			configureChildProcess(cmd)
			childState, err := prepareShellChild(cmd, mode, workspace)
			if err != nil {
				return ToolResult{}, err
			}
			var stdout, stderr limitedBuffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err = startShellChild(cmd, childState)
			if err == nil {
				err = waitShellChild(cmd, childState)
			}
			exitCode := 0
			if err != nil {
				var exit *exec.ExitError
				if errors.As(err, &exit) {
					exitCode = exit.ExitCode()
				} else if runCtx.Err() == nil {
					if mode != sandboxDangerFull && errors.Is(err, exec.ErrNotFound) {
						return ToolResult{}, errors.New("SANDBOX_UNAVAILABLE: the selected sandbox runner is not installed or executable")
					}
					return ToolResult{}, err
				}
			}
			if runCtx.Err() == context.DeadlineExceeded {
				exitCode = -1
			}
			var out strings.Builder
			fmt.Fprintf(&out, "[exit code: %d]\n", exitCode)
			if stdout.Len() > 0 {
				out.WriteString(stdout.String())
				if !strings.HasSuffix(stdout.String(), "\n") {
					out.WriteByte('\n')
				}
			}
			if stderr.Len() > 0 {
				out.WriteString("[stderr]\n")
				out.WriteString(stderr.String())
			}
			if mode != sandboxDangerFull && exitCode != 0 && sandboxOutputDenied(stdout.String()+"\n"+stderr.String()) {
				out.WriteString("\n" + sandboxDenialMarker(mode) + "\n" + sandboxEscalationHint("command") + "\n")
			}
			if stdout.truncated || stderr.truncated {
				out.WriteString("\n[output truncated]\n")
			}
			result := textToolResult(out.String())
			value := map[string]any{
				"kind": "foreground", "exitCode": exitCode, "signal": nil,
				"timedOut": runCtx.Err() == context.DeadlineExceeded, "aborted": ctx.Err() != nil && runCtx.Err() != context.DeadlineExceeded,
				"timeoutMs": timeout.Milliseconds(),
				"stdout":    map[string]any{"text": stdout.String(), "truncated": stdout.truncated},
				"stderr":    map[string]any{"text": stderr.String(), "truncated": stderr.truncated},
			}
			if mode != sandboxDangerFull {
				value["sandbox"] = map[string]any{"mode": mode, "denied": exitCode != 0 && sandboxOutputDenied(stdout.String()+"\n"+stderr.String())}
			}
			result.Value = value
			return result, nil
		},
	}
}

type limitedBuffer struct {
	bytes.Buffer
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	written := len(p)
	remaining := toolOutputLimit - b.Len()
	if remaining <= 0 {
		b.truncated = true
		return written, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
		b.truncated = true
	}
	_, err := b.Buffer.Write(p)
	return written, err
}

func openToolReadRoot(workspace, target string) (*os.Root, string, error) {
	target, err := resolveToolReadPath(workspace, target)
	if err != nil {
		return nil, "", err
	}
	volumeRoot := filepath.VolumeName(target) + string(filepath.Separator)
	name, err := filepath.Rel(volumeRoot, target)
	if err != nil {
		return nil, "", err
	}
	root, err := os.OpenRoot(volumeRoot)
	if err != nil {
		return nil, "", err
	}
	return root, name, nil
}

func resolveToolReadPath(workspace, target string) (string, error) {
	workspace, err := filepath.Abs(workspace)
	if err != nil {
		return "", err
	}
	target = strings.TrimSpace(target)
	if target == "" || target == "." {
		target = workspace
	} else if !filepath.IsAbs(target) {
		target = filepath.Join(workspace, target)
	}
	return filepath.Abs(filepath.Clean(target))
}

func isVCSMetadataDirectory(name string) bool {
	switch name {
	case ".git", ".svn", ".hg", ".bzr", ".jj", ".sl":
		return true
	default:
		return false
	}
}

func displaySearchPath(workspace, path string) string {
	if !filepath.IsAbs(path) {
		return filepath.ToSlash(path)
	}
	workspace, workspaceErr := filepath.Abs(workspace)
	path, pathErr := filepath.Abs(path)
	if workspaceErr != nil || pathErr != nil {
		return filepath.ToSlash(path)
	}
	rel, err := filepath.Rel(workspace, path)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return filepath.ToSlash(rel)
	}
	return filepath.ToSlash(path)
}

func shellWorkdir(workspace, target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return filepath.Abs(workspace)
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(workspace, target)
	}
	return filepath.Abs(filepath.Clean(target))
}
