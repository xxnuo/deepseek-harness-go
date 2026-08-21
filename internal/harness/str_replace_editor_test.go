package harness

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStrReplaceEditorEndToEnd(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "editor", "minimal")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.Config().Workspace, "sample.txt")

	created := executeRegisteredTool(t, e, "str_replace_editor", id, map[string]any{"command": "create", "path": path, "file_text": "one\ntwo\nthree\n"})
	if text := modelToolResultText(created); text != "New file created successfully at: "+path {
		t.Fatalf("create = %q", text)
	}
	viewed := executeRegisteredTool(t, e, "str_replace_editor", id, map[string]any{"command": "view", "path": path, "view_range": []int{2, -1}})
	if text := modelToolResultText(viewed); !strings.Contains(text, "     2  two") || !strings.Contains(text, "     4  ") {
		t.Fatalf("view = %q", text)
	}
	executeRegisteredTool(t, e, "str_replace_editor", id, map[string]any{"command": "str_replace", "path": path, "old_str": "two", "new_str": "TWO"})
	executeRegisteredTool(t, e, "str_replace_editor", id, map[string]any{"command": "insert", "path": path, "insert_line": 1, "new_str": "between"})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "one\nbetween\nTWO\nthree\n" {
		t.Fatalf("edited file = %q", data)
	}
}

func TestStrReplaceEditorRejectsAmbiguousEditsAndReadsAbsolutePaths(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "editor-errors", "minimal")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.Config().Workspace, "ambiguous.txt")
	if err := os.WriteFile(path, []byte("same\nother\nsame"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool := e.tools["str_replace_editor"]
	e.mu.RUnlock()
	if _, err := tool.Execute(context.Background(), ToolCall{Name: "str_replace_editor", SessionID: id, Workspace: e.Config().Workspace, Arguments: []byte(`{"command":"view","path":"` + path + `"}`)}); err != nil {
		t.Fatal(err)
	}
	_, err = tool.Execute(context.Background(), ToolCall{Name: "str_replace_editor", SessionID: id, Workspace: e.Config().Workspace, Arguments: []byte(`{"command":"str_replace","path":"` + path + `","old_str":"same","new_str":"x"}`)})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "multiple occurrences") {
		t.Fatalf("ambiguous replacement error = %v", err)
	}
	result, err := tool.Execute(context.Background(), ToolCall{Name: "str_replace_editor", SessionID: id, Workspace: e.Config().Workspace, Arguments: []byte(`{"command":"view","path":"/etc/hosts"}`)})
	if err != nil || !strings.Contains(modelToolResultText(result), "/etc/hosts") {
		t.Fatalf("absolute read = %#v, %v", result, err)
	}
}

func TestStrReplaceEditorCASDetectsExternalChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cas.txt")
	if err := os.WriteFile(path, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, version, _, err := readVersionedFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("after!"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, current, _, err := readVersionedFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if sameFSVersion(current, version) {
		t.Fatal("editor CAS did not detect an external file change")
	}
}
