package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestFileReferenceGrammar(t *testing.T) {
	tests := []struct {
		line   string
		want   ActiveFileReferenceToken
		active bool
	}{
		{"@src/ma", ActiveFileReferenceToken{Prefix: "@src/ma", Query: "src/ma"}, true},
		{"open @\"docs/my file", ActiveFileReferenceToken{Prefix: `@"docs/my file`, Query: "docs/my file", Quoted: true}, true},
		{"mail@example.com", ActiveFileReferenceToken{}, false},
		{"closed @\"file name\"", ActiveFileReferenceToken{}, false},
	}
	for _, test := range tests {
		got, active := ParseActiveFileReferenceToken(test.line, len(test.line))
		if active != test.active || got != test.want {
			t.Fatalf("token(%q) = %#v, %v; want %#v, %v", test.line, got, active, test.want, test.active)
		}
	}
	formatted, ok := FormatFileReferenceMention(FileReferenceCandidate{Path: "docs/my file", Kind: "file"}, false)
	if !ok || formatted != `@"docs/my file"` {
		t.Fatalf("file mention = %q, %v", formatted, ok)
	}
	formatted, ok = FormatFileReferenceMention(FileReferenceCandidate{Path: "src", Kind: "directory"}, true)
	if !ok || formatted != `@"src/` {
		t.Fatalf("directory mention = %q, %v", formatted, ok)
	}
	if _, ok := FormatFileReferenceMention(FileReferenceCandidate{Path: "bad\nname", Kind: "file"}, false); ok {
		t.Fatal("control character was accepted")
	}
}

func TestSearchWorkspaceFileReferencesRanksAndBoundsPaths(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"src/nested", "node_modules/pkg", ".hidden"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"README.md", "src/main.go", "src/nested/model.go", "node_modules/pkg/index.js", ".hidden/secret"} {
		if err := os.WriteFile(filepath.Join(root, path), []byte(path), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}

	rows, err := SearchWorkspaceFileReferences(context.Background(), root, "", FileReferenceConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []FileReferenceCandidate{{Path: "src", Kind: "directory"}, {Path: "README.md", Kind: "file"}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("root candidates = %#v, want %#v", rows, want)
	}
	rows, err = SearchWorkspaceFileReferences(context.Background(), root, "mai", FileReferenceConfig{})
	if err != nil || len(rows) == 0 || rows[0].Path != "src/main.go" {
		t.Fatalf("fuzzy candidates = %#v, %v", rows, err)
	}
	rows, err = SearchWorkspaceFileReferences(context.Background(), root, "src/", FileReferenceConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []FileReferenceCandidate{{Path: "src/nested", Kind: "directory"}, {Path: "src/main.go", Kind: "file"}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("directory candidates = %#v, want %#v", rows, want)
	}
	rows, err = SearchWorkspaceFileReferences(context.Background(), root, "../", FileReferenceConfig{})
	if err != nil || len(rows) != 0 {
		t.Fatalf("escaping candidates = %#v, %v", rows, err)
	}
	rows, err = SearchWorkspaceFileReferences(context.Background(), root, "linked/", FileReferenceConfig{})
	if err != nil || len(rows) != 0 {
		t.Fatalf("symlink candidates = %#v, %v", rows, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := SearchWorkspaceFileReferences(ctx, root, "", FileReferenceConfig{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}
	if _, err := SearchWorkspaceFileReferences(context.Background(), root, "", FileReferenceConfig{MaxResults: -1}); err == nil {
		t.Fatal("invalid maxResults was accepted")
	}
}

func TestWorkspaceFileSearchCachesAndInvalidatesBareQueries(t *testing.T) {
	root := t.TempDir()
	search, err := NewWorkspaceFileSearch(root, FileReferenceConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(search.Close)
	rows, err := search.List(t.Context(), "later")
	if err != nil || len(rows) != 0 {
		t.Fatalf("initial rows = %#v, %v", rows, err)
	}
	if err := os.WriteFile(filepath.Join(root, "later.txt"), []byte("later"), 0o600); err != nil {
		t.Fatal(err)
	}
	rows, err = search.List(t.Context(), "later")
	if err != nil || len(rows) != 0 {
		t.Fatalf("cached rows = %#v, %v", rows, err)
	}
	search.Invalidate()
	rows, err = search.List(t.Context(), "later")
	if err != nil || len(rows) != 1 || rows[0].Path != "later.txt" {
		t.Fatalf("refreshed rows = %#v, %v", rows, err)
	}
	if err := os.WriteFile(filepath.Join(root, "live.txt"), []byte("live"), 0o600); err != nil {
		t.Fatal(err)
	}
	rows, err = search.List(t.Context(), "")
	if err != nil || !strings.Contains(fmt.Sprint(rows), "live.txt") {
		t.Fatalf("live directory rows = %#v, %v", rows, err)
	}
}

func TestFileReferenceSearchReleasedWithSession(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e, err := New(WithPersistence(false), WithWorkspace(root), WithProvider("echo"), WithModel("echo"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(t.Context(), root, "file-reference-release", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.ListFileReferenceCandidates(t.Context(), id, "main"); err != nil {
		t.Fatal(err)
	}
	e.fileReferenceMu.Lock()
	search := e.fileReferenceSearches[id]
	e.fileReferenceMu.Unlock()
	if search == nil {
		t.Fatal("file reference search was not cached")
	}
	if err := detachSDKSession(e, id); err != nil {
		t.Fatal(err)
	}
	e.fileReferenceMu.Lock()
	_, cached := e.fileReferenceSearches[id]
	e.fileReferenceMu.Unlock()
	if cached {
		t.Fatal("file reference search survived session disposal")
	}
	if candidates, err := search.List(t.Context(), "main"); err != nil || len(candidates) != 0 {
		t.Fatalf("disposed search = %#v, %v", candidates, err)
	}
}

func TestFileReferencePromptIsInstalledWithReadTool(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "file-reference-prompt", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession(id)
	runtimeConfig, err := e.runtimeForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := e.systemPromptForSession(session, session.Model, runtimeConfig)
	if err != nil || !strings.Contains(prompt, FileReferencePrompt) {
		t.Fatalf("prompt = %q, %v", prompt, err)
	}
}
