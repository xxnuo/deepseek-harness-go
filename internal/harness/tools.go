package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	pathpkg "path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
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
	registered := []Tool{
		builtinShellTool(e), builtinReadTool(e), builtinReadImageTool(e), builtinWriteTool(e), builtinEditTool(e), builtinGlobTool(), builtinGrepTool(),
		builtinStrReplaceEditorTool(e),
		builtinJobOutputTool(e), builtinJobListTool(e), builtinJobKillTool(e),
		builtinTodoTool(e), builtinSkillTool(e),
	}
	if !e.cfg.TerminalTool.Disabled {
		registered = append(registered, builtinTerminalTools(e)...)
	}
	for _, tool := range registered {
		if err := e.RegisterTool(tool); err != nil {
			return err
		}
	}
	return registerSessionQueryTools(e)
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
	return Tool{
		Schema: ToolSchema{
			Name:        "todo_write",
			Description: "Record the complete task list for the current work. Each call replaces the previous list; multiple tasks may be in progress when work runs in parallel.",
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
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
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
			offset, limit := 1, readToolLineLimit
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
			if limit > readToolLineLimit {
				return ToolResult{}, fmt.Errorf("read: limit must be less than or equal to %d", readToolLineLimit)
			}
			lines := splitReadToolLines(string(data))
			if offset > len(lines) && !(len(lines) == 0 && offset == 1) {
				return ToolResult{}, fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("offset %d is out of range for %q (%d lines)", offset, target.displayPath, len(lines)))
			}
			valueLines := make([]map[string]any, 0, min(limit, max(0, len(lines)-offset+1)))
			outputBytes := 0
			truncatedByBytes := false
			for index := offset - 1; index < len(lines) && len(valueLines) < limit; index++ {
				text := truncateReadToolLine(lines[index], readToolMaxLineChars)
				lineBytes := len([]byte(text))
				if len(valueLines) > 0 {
					lineBytes++
				}
				if outputBytes+lineBytes > readToolMaxBytes {
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

func builtinGlobTool() Tool {
	type input struct {
		Pattern string `json:"pattern"`
		Path    string `json:"path"`
	}
	return Tool{
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
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			searchRoot, err := resolveToolReadPath(call.Workspace, in.Path)
			if err != nil {
				return ToolResult{}, err
			}
			root, base, err := openToolReadRoot(call.Workspace, in.Path)
			if err != nil {
				return ToolResult{}, err
			}
			defer root.Close()
			pattern := filepath.ToSlash(strings.TrimSpace(in.Pattern))
			if pattern == "" {
				return ToolResult{}, errors.New("glob: pattern is required")
			}
			type match struct {
				path    string
				modTime time.Time
			}
			matches := []match{}
			err = fs.WalkDir(root.FS(), base, func(path string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if entry.IsDir() {
					if isVCSMetadataDirectory(entry.Name()) {
						return fs.SkipDir
					}
					return nil
				}
				rel, err := filepath.Rel(base, path)
				if err != nil {
					return err
				}
				if !globMatch(pattern, filepath.ToSlash(rel)) {
					return nil
				}
				info, err := entry.Info()
				if err != nil {
					return err
				}
				absolutePath := searchRoot
				if rel != "." {
					absolutePath = filepath.Join(searchRoot, rel)
				}
				matches = append(matches, match{path: displaySearchPath(call.Workspace, absolutePath), modTime: info.ModTime()})
				return nil
			})
			if err != nil {
				return ToolResult{}, err
			}
			sort.Slice(matches, func(left, right int) bool {
				if !matches[left].modTime.Equal(matches[right].modTime) {
					return matches[left].modTime.After(matches[right].modTime)
				}
				return matches[left].path < matches[right].path
			})
			displayRoot := displaySearchPath(call.Workspace, searchRoot)
			displayMatches := make([]string, len(matches))
			for index, match := range matches {
				displayMatches[index] = match.path
			}
			return ToolResult{
				Content: []ContentBlock{{Type: "text", Text: renderGlobToolResult(displayMatches)}},
				Value:   map[string]any{"root": displayRoot, "paths": displayMatches},
			}, nil
		},
	}
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

func builtinGrepTool() Tool {
	type input struct {
		Pattern string `json:"pattern"`
		Path    string `json:"path"`
		Include string `json:"include"`
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
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			expression, err := regexp.Compile(in.Pattern)
			if err != nil {
				return ToolResult{}, fmt.Errorf("grep: %w", err)
			}
			searchRoot, err := resolveToolReadPath(call.Workspace, in.Path)
			if err != nil {
				return ToolResult{}, err
			}
			root, base, err := openToolReadRoot(call.Workspace, in.Path)
			if err != nil {
				return ToolResult{}, err
			}
			defer root.Close()
			matches := []map[string]any{}
			err = fs.WalkDir(root.FS(), base, func(path string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if entry.IsDir() {
					if isVCSMetadataDirectory(entry.Name()) {
						return fs.SkipDir
					}
					return nil
				}
				rel, err := filepath.Rel(base, path)
				if err != nil {
					return err
				}
				if in.Include != "" {
					includePath := filepath.ToSlash(rel)
					if rel == "." {
						includePath = entry.Name()
					}
					if !globMatch(filepath.ToSlash(in.Include), includePath) {
						return nil
					}
				}
				data, err := fs.ReadFile(root.FS(), path)
				if err != nil || bytes.IndexByte(data, 0) >= 0 {
					return nil
				}
				for lineNo, line := range strings.Split(string(data), "\n") {
					if err := ctx.Err(); err != nil {
						return err
					}
					line = strings.TrimSuffix(line, "\r")
					if expression.MatchString(line) {
						absolutePath := searchRoot
						if rel != "." {
							absolutePath = filepath.Join(searchRoot, rel)
						}
						displayPath := displaySearchPath(call.Workspace, absolutePath)
						if !utf8.ValidString(line) {
							line = "(line is not valid UTF-8)"
						}
						matches = append(matches, map[string]any{"path": displayPath, "lineNumber": lineNo + 1, "line": line})
					}
				}
				return nil
			})
			if err != nil {
				return ToolResult{}, err
			}
			return ToolResult{
				Content: []ContentBlock{{Type: "text", Text: renderGrepToolResult(matches)}},
				Value:   map[string]any{"matches": matches},
			}, nil
		},
	}
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
		Command           string  `json:"command"`
		Description       string  `json:"description"`
		TimeoutMS         int     `json:"timeoutMs"`
		Workdir           string  `json:"workdir"`
		RunInBackground   bool    `json:"run_in_background"`
		SandboxPermission *string `json:"sandbox_permissions"`
		Justification     *string `json:"justification"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: shellToolName, Description: shellToolDescription,
			Parameters: objectSchema(map[string]any{
				"command":             map[string]any{"type": "string"},
				"description":         map[string]any{"type": "string"},
				"timeoutMs":           map[string]any{"type": "integer", "minimum": 1, "maximum": 120000},
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
			mode, err := e.resolveSandboxMode(ctx, call, in.SandboxPermission, in.Justification, "command")
			if err != nil {
				return ToolResult{}, err
			}
			if call.SessionID != "" {
				if session, sessionErr := e.getSession(call.SessionID); sessionErr == nil {
					runtimeConfig, runtimeErr := e.runtimeForSession(session)
					if runtimeErr != nil {
						return ToolResult{}, runtimeErr
					}
					if runtimeConfig.persistentBash && shellToolPersistent {
						output, err := e.shells.runWithMode(ctx, call.SessionID, call.Workspace, mode, in.Command)
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
				}
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
			if in.RunInBackground {
				if err := ctx.Err(); err != nil {
					return ToolResult{}, err
				}
				cmd := exec.Command(program, args...)
				cmd.Dir = workdir
				cmd.Env = scrubbedChildEnv(shellEnvironment())
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
			timeout := time.Duration(in.TimeoutMS) * time.Millisecond
			if timeout <= 0 || timeout > 2*time.Minute {
				timeout = 60 * time.Second
			}
			runCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			cmd := exec.CommandContext(runCtx, program, args...)
			cmd.Dir = workdir
			cmd.Env = scrubbedChildEnv(shellEnvironment())
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
