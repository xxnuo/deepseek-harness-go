package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func executeFSTool(t *testing.T, e *Engine, name string, arguments any) (ToolResult, error) {
	t.Helper()
	raw, err := json.Marshal(arguments)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool := e.tools[name]
	e.mu.RUnlock()
	return tool.Execute(context.Background(), ToolCall{
		ID: "fs-test", Name: name, Arguments: raw, Workspace: e.Config().Workspace,
	})
}

func TestReadToolUpstreamWindowCaps(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace := e.Config().Workspace

	var source strings.Builder
	source.WriteString(strings.Repeat("x", readToolMaxLineChars+1))
	source.WriteByte('\n')
	for line := 2; line <= 2100; line++ {
		fmt.Fprintf(&source, "line-%04d\n", line)
	}
	if err := os.WriteFile(filepath.Join(workspace, "lines.txt"), []byte(source.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := executeFSTool(t, e, "read", map[string]any{"file_path": "lines.txt"})
	if err != nil {
		t.Fatal(err)
	}
	value := result.Value.(map[string]any)
	lines := value["lines"].([]map[string]any)
	if len(lines) != readToolLineLimit || value["totalLines"] != 2100 {
		t.Fatalf("read window = %d/%v", len(lines), value["totalLines"])
	}
	first := lines[0]["text"].(string)
	if !strings.HasSuffix(first, "... (line truncated to 2000 chars)") || len([]rune(strings.TrimSuffix(first, "... (line truncated to 2000 chars)"))) != readToolMaxLineChars {
		t.Fatalf("truncated first line = %q", first[len(first)-min(len(first), 80):])
	}
	if text := contentValueText(result.Content); !strings.Contains(text, "Showing lines 1-2000 of 2100") {
		t.Fatalf("read limit footer = %q", text[len(text)-min(len(text), 120):])
	}

	largeLines := strings.Repeat(strings.Repeat("y", 1000)+"\n", 100)
	if err := os.WriteFile(filepath.Join(workspace, "bytes.txt"), []byte(largeLines), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err = executeFSTool(t, e, "read", map[string]any{"file_path": "bytes.txt"})
	if err != nil {
		t.Fatal(err)
	}
	value = result.Value.(map[string]any)
	lines = value["lines"].([]map[string]any)
	if len(lines) != 51 || value["totalLines"] != 100 {
		t.Fatalf("byte-capped window = %d/%v", len(lines), value["totalLines"])
	}
	if text := contentValueText(result.Content); !strings.Contains(text, "Output capped") || !strings.Contains(text, "offset=52") {
		t.Fatalf("read byte-cap footer = %q", text[len(text)-min(len(text), 120):])
	}

	if _, err := executeFSTool(t, e, "read", map[string]any{"file_path": "bytes.txt", "offset": 101}); err == nil || !strings.Contains(err.Error(), "FS_NOT_FOUND") {
		t.Fatalf("offset past EOF error = %v", err)
	}
	if _, err := executeFSTool(t, e, "read", map[string]any{"file_path": "bytes.txt", "limit": readToolLineLimit + 1}); err == nil {
		t.Fatal("read accepted a limit above the upstream cap")
	}
}

func TestGlobToolKeepsCompleteMtimeSortedValue(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace := e.Config().Workspace
	baseTime := time.Unix(1_700_000_000, 0)
	if err := os.MkdirAll(filepath.Join(workspace, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 105; index++ {
		path := filepath.Join(workspace, "nested", fmt.Sprintf("f%03d.txt", index))
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		stamp := baseTime.Add(time.Duration(index) * time.Second)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	for name, offset := range map[string]int{
		".hidden.txt":               1000,
		"node_modules/included.txt": 999,
		".git/excluded.txt":         2000,
	} {
		path := filepath.Join(workspace, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		stamp := baseTime.Add(time.Duration(offset) * time.Second)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}

	result, err := executeFSTool(t, e, "glob", map[string]any{"pattern": "*.txt"})
	if err != nil {
		t.Fatal(err)
	}
	value := result.Value.(map[string]any)
	paths := value["paths"].([]string)
	if value["root"] != "." || len(paths) != 107 {
		t.Fatalf("glob value root=%v paths=%d", value["root"], len(paths))
	}
	if paths[0] != ".hidden.txt" || paths[1] != "node_modules/included.txt" || paths[2] != "nested/f104.txt" {
		t.Fatalf("glob mtime order prefix = %#v", paths[:3])
	}
	for _, path := range paths {
		if strings.Contains(path, ".git/") {
			t.Fatalf("glob exposed VCS metadata path %q", path)
		}
	}
	text := contentValueText(result.Content)
	if !strings.Contains(text, "Showing 100 of 107 paths") || strings.Contains(text, paths[100]) {
		t.Fatalf("glob capped text footer=%q omitted=%q", text[len(text)-min(len(text), 160):], paths[100])
	}
}

func TestGrepToolKeepsCompleteValueAndBoundsPreview(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace := e.Config().Workspace
	longLine := "needle " + strings.Repeat("界", 1000)
	lines := []string{longLine}
	for index := 1; index < 260; index++ {
		lines = append(lines, fmt.Sprintf("needle match-%03d", index))
	}
	if err := os.WriteFile(filepath.Join(workspace, "matches.txt"), []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := executeFSTool(t, e, "grep", map[string]any{"pattern": "needle", "include": "*.txt"})
	if err != nil {
		t.Fatal(err)
	}
	value := result.Value.(map[string]any)
	matches := value["matches"].([]map[string]any)
	if len(matches) != 260 || matches[0]["line"] != longLine || matches[259]["line"] != "needle match-259" {
		t.Fatalf("grep canonical matches = %d first=%v last=%v", len(matches), matches[0]["line"], matches[len(matches)-1]["line"])
	}
	text := contentValueText(result.Content)
	if !strings.HasPrefix(text, "Found 250 of 260 matches\n\n") || !strings.Contains(text, "(line truncated)") || !strings.Contains(text, "complete result could not be saved") {
		t.Fatalf("grep capped output prefix/suffix missing: %q", text[:min(len(text), 240)])
	}
	if strings.Contains(text, "match-250") {
		t.Fatal("grep model-facing text included a match past the 250-match cap")
	}
	previewStart := strings.Index(text, "Line 1: ") + len("Line 1: ")
	previewEnd := strings.Index(text[previewStart:], " (line truncated)")
	if previewStart < len("Line 1: ") || previewEnd < 0 {
		t.Fatalf("grep long-line preview not found in %q", text[:min(len(text), 240)])
	}
	preview := text[previewStart : previewStart+previewEnd]
	if !utf8.ValidString(preview) || len([]byte(preview)) > grepToolMaxLineBytes {
		t.Fatalf("grep preview bytes=%d valid=%v", len([]byte(preview)), utf8.ValidString(preview))
	}
}
