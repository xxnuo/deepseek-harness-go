package harness

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const editorOutputLimit = 16000

func builtinStrReplaceEditorTool(e *Engine) Tool {
	type input struct {
		Command    string  `json:"command"`
		Path       string  `json:"path"`
		FileText   *string `json:"file_text"`
		InsertLine *int    `json:"insert_line"`
		NewString  *string `json:"new_str"`
		OldString  *string `json:"old_str"`
		ViewRange  []int   `json:"view_range"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "str_replace_editor",
			Description: "Custom editing tool for viewing, creating and editing files. State is persistent across calls. Paths must be absolute.",
			Parameters: objectSchema(map[string]any{
				"command":     map[string]any{"type": "string", "enum": []string{"view", "create", "str_replace", "insert"}},
				"path":        map[string]any{"type": "string", "description": "Absolute path to file or directory."},
				"file_text":   map[string]any{"type": "string"},
				"insert_line": map[string]any{"type": "integer"},
				"new_str":     map[string]any{"type": "string"},
				"old_str":     map[string]any{"type": "string"},
				"view_range":  map[string]any{"type": "array", "items": map[string]any{"type": "integer"}},
			}, "command", "path"),
			Output: map[string]any{"type": "string"},
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if err := ctx.Err(); err != nil {
				return ToolResult{}, err
			}
			if strings.TrimSpace(in.Path) == "" {
				return ToolResult{}, errors.New("path must be a non-empty string")
			}
			if !filepath.IsAbs(in.Path) {
				return ToolResult{}, fmt.Errorf("the path %s is not an absolute path, it should start with `/`", in.Path)
			}
			display := filepath.Clean(in.Path)
			target, err := resolveFSTarget(call.Workspace, in.Path)
			if err != nil {
				return ToolResult{}, err
			}
			switch in.Command {
			case "view":
				info, err := os.Stat(target.targetKey)
				if err != nil {
					if errors.Is(err, fs.ErrNotExist) {
						e.fsState.observe(call.SessionID, target, fsObservation{})
						return ToolResult{}, fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("the path %s does not exist", display))
					}
					return ToolResult{}, err
				}
				if info.IsDir() {
					if in.ViewRange != nil {
						return ToolResult{}, errors.New("view_range is valid only when path is a file")
					}
					root, name, err := openToolReadRoot(call.Workspace, in.Path)
					if err != nil {
						return ToolResult{}, err
					}
					defer root.Close()
					text := editorDirectoryView(root.FS(), name, display)
					result := textToolResult(text)
					result.Value = text
					return result, nil
				}
				data, version, _, err := readVersionedFile(target.targetKey)
				if err != nil {
					return ToolResult{}, err
				}
				e.fsState.observe(call.SessionID, target, fsObservation{present: true, version: version})
				text, err := editorFileView(display, string(data), in.ViewRange)
				if err != nil {
					return ToolResult{}, err
				}
				text = clipEditorOutput(text)
				result := textToolResult(text)
				result.Value = text
				return result, nil
			case "create":
				mode, err := e.sandboxModeForCall(call)
				if err != nil {
					return ToolResult{}, err
				}
				path, err := e.sandboxMutationPathWithMode(call, in.Path, mode)
				if err != nil {
					return ToolResult{}, err
				}
				target.targetKey = path
				if in.FileText == nil {
					return ToolResult{}, errors.New("parameter `file_text` is required for command: create")
				}
				if _, err := os.Stat(path); err == nil {
					return ToolResult{}, fmt.Errorf("file already exists at: %s", display)
				} else if !errors.Is(err, fs.ErrNotExist) {
					return ToolResult{}, err
				}
				intent := e.fsState.writeIntent(call.SessionID, target)
				version, _, _, err := e.guardedFSWrite(target, []byte(*in.FileText), intent)
				if err != nil {
					return ToolResult{}, err
				}
				e.fsState.observe(call.SessionID, target, fsObservation{present: true, version: version})
				text := "New file created successfully at: " + display
				result := textToolResult(text)
				result.Value = text
				return result, nil
			case "str_replace":
				mode, err := e.sandboxModeForCall(call)
				if err != nil {
					return ToolResult{}, err
				}
				path, err := e.sandboxMutationPathWithMode(call, in.Path, mode)
				if err != nil {
					return ToolResult{}, err
				}
				target.targetKey = path
				if in.OldString == nil {
					return ToolResult{}, errors.New("parameter `old_str` is required for command: str_replace")
				}
				if *in.OldString == "" {
					return ToolResult{}, errors.New("parameter `old_str` is empty for command: str_replace")
				}
				expected, err := e.fsState.editIntent(call.SessionID, target)
				if err != nil {
					return ToolResult{}, err
				}
				replacement := ""
				if in.NewString != nil {
					replacement = *in.NewString
				}
				version, err := e.guardedFSEdit(target, expected, func(before string) (string, error) {
					offsets := editorMatchOffsets(before, *in.OldString)
					if len(offsets) == 0 {
						return "", fsPolicyError("FS_EDIT_NOT_FOUND", fmt.Sprintf("old_str `%s` did not appear verbatim in %s", *in.OldString, display))
					}
					if len(offsets) > 1 {
						return "", fsPolicyError("FS_AMBIGUOUS_EDIT", fmt.Sprintf("multiple occurrences of old_str `%s` in lines %v", *in.OldString, editorLineNumbers(before, offsets)))
					}
					return strings.Replace(before, *in.OldString, replacement, 1), nil
				})
				if err != nil {
					return ToolResult{}, err
				}
				e.fsState.observe(call.SessionID, target, fsObservation{present: true, version: version})
				text := fmt.Sprintf("The file %s has been edited successfully.", display)
				result := textToolResult(text)
				result.Value = text
				return result, nil
			case "insert":
				mode, err := e.sandboxModeForCall(call)
				if err != nil {
					return ToolResult{}, err
				}
				path, err := e.sandboxMutationPathWithMode(call, in.Path, mode)
				if err != nil {
					return ToolResult{}, err
				}
				target.targetKey = path
				if in.InsertLine == nil {
					return ToolResult{}, errors.New("parameter `insert_line` is required for command: insert")
				}
				if in.NewString == nil {
					return ToolResult{}, errors.New("parameter `new_str` is required for command: insert")
				}
				expected, err := e.fsState.editIntent(call.SessionID, target)
				if err != nil {
					return ToolResult{}, err
				}
				version, err := e.guardedFSEdit(target, expected, func(before string) (string, error) {
					lines := strings.Split(before, "\n")
					if *in.InsertLine < 0 || *in.InsertLine > len(lines) {
						return "", fmt.Errorf("invalid insert_line %d for file with %d lines", *in.InsertLine, len(lines))
					}
					lines = append(lines, "")
					copy(lines[*in.InsertLine+1:], lines[*in.InsertLine:])
					lines[*in.InsertLine] = *in.NewString
					return strings.Join(lines, "\n"), nil
				})
				if err != nil {
					return ToolResult{}, err
				}
				e.fsState.observe(call.SessionID, target, fsObservation{present: true, version: version})
				text := fmt.Sprintf("The file %s has been edited successfully.", display)
				result := textToolResult(text)
				result.Value = text
				return result, nil
			default:
				return ToolResult{}, fmt.Errorf("unknown command %q", in.Command)
			}
		},
	}
}

func editorReadRegular(fsys fs.FS, name, display string) ([]byte, error) {
	info, err := fs.Stat(fsys, name)
	if err != nil {
		return nil, fmt.Errorf("the path %s does not exist", display)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("the path %s is not a regular file", display)
	}
	return fs.ReadFile(fsys, name)
}

func editorFileView(path, content string, viewRange []int) (string, error) {
	all := strings.Split(content, "\n")
	start, end := 1, len(all)
	suffix := ""
	if viewRange != nil {
		if len(viewRange) != 2 {
			return "", errors.New("invalid view_range: expected two integers")
		}
		start, end = viewRange[0], viewRange[1]
		if start < 1 || start > len(all) || end > len(all) || end != -1 && end < start {
			return "", fmt.Errorf("invalid view_range: %v", viewRange)
		}
		if end == -1 {
			end = len(all)
		}
		suffix = fmt.Sprintf(" with view_range=[%d, %d]", viewRange[0], viewRange[1])
	}
	var out strings.Builder
	fmt.Fprintf(&out, "Here's the content of %s with line numbers (which has a total of %d lines)%s:\n", path, len(all), suffix)
	for index := start - 1; index < end; index++ {
		fmt.Fprintf(&out, "%6d  %s\n", index+1, all[index])
	}
	return out.String(), nil
}

func editorDirectoryView(fsys fs.FS, name, display string) string {
	rows := []string{"d\t" + display}
	_ = fs.WalkDir(fsys, name, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if path == name {
			return nil
		}
		rel, relErr := filepath.Rel(name, path)
		if relErr != nil {
			return nil
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) > 2 {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		base := entry.Name()
		if strings.HasPrefix(base, ".") || base == "node_modules" || base == "__pycache__" {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		kind := "f"
		if entry.IsDir() {
			kind = "d"
		} else if entry.Type()&fs.ModeType != 0 {
			kind = "?"
		}
		rows = append(rows, kind+"\t"+filepath.Join(display, rel))
		return nil
	})
	sort.Strings(rows)
	return clipEditorOutput(fmt.Sprintf("Here're the files and directories up to 2 levels deep in %s, excluding hidden items, node_modules, and Python cache directories:\n%s\n\n", display, strings.Join(rows, "\n")))
}

func editorMatchOffsets(content, search string) []int {
	var offsets []int
	for offset := 0; ; {
		index := strings.Index(content[offset:], search)
		if index < 0 {
			return offsets
		}
		offset += index
		offsets = append(offsets, offset)
		offset += len(search)
	}
}

func editorLineNumbers(content string, offsets []int) []int {
	lines := make([]int, len(offsets))
	for i, offset := range offsets {
		lines[i] = 1 + strings.Count(content[:offset], "\n")
	}
	return lines
}

func clipEditorOutput(text string) string {
	if len(text) <= editorOutputLimit {
		return text
	}
	return text[:editorOutputLimit] + "<response clipped><NOTE>To save on context only part of this file has been shown to you.</NOTE>"
}
