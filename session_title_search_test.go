package harness

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSessionTitleNormalizationMatchesUpstreamLimits(t *testing.T) {
	got := NormalizeSessionTitle("\x1b]0;hidden\a  Hello\t brave\nnew\u202e world  ", 80)
	if got != "Hello brave new world" {
		t.Fatalf("normalized title = %q", got)
	}
	if got := FallbackSessionTitle("one two three four five six", 5, 40); got != "one two three four five" {
		t.Fatalf("fallback title = %q", got)
	}
	if got := FallbackSessionTitle("你好世界", 5, 7); got != "你好" {
		t.Fatalf("CJK fallback = %q", got)
	}
	if got := FallbackSessionTitle("😀😀", 5, 5); len([]byte(got)) != 4 {
		t.Fatalf("astral fallback = %q (%d bytes)", got, len([]byte(got)))
	}
}

func TestFirstPromptCreatesBoundedFallbackTitle(t *testing.T) {
	e := newIntegrationEngine(t)
	id := createToolSession(t, e)
	_, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{
		Type: "text", Text: "one two three four five six seven",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	s.mu.Lock()
	title := s.Title
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	if title != "one two three four five" {
		t.Fatalf("fallback title = %q", title)
	}
	if folded := sessionTitleFromEvents(events); folded != title {
		t.Fatalf("folded title = %q, want %q", folded, title)
	}
}

func TestSessionTitleRenamePersistsAcrossReload(t *testing.T) {
	home := t.TempDir()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = filepath.Join(home, "data")
	cfg.Workspace = filepath.Join(home, "project-alpha")
	cfg.Provider, cfg.Model = "echo", "echo"
	if err := os.MkdirAll(cfg.Workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	id, err := e.CreateSession(context.Background(), cfg.Workspace, "title-session", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(mustSession(t, e, id), "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.RenameSession(id, "  \x1b[31mBuild\x1b[0m   Harness  "); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	rows := e.ListSessions()
	if len(rows) != 1 {
		t.Fatalf("reloaded rows = %#v", rows)
	}
	values := rows[0].Projections.(map[string]any)["values"].(map[string]any)
	if values["title"] != "Build Harness" {
		t.Fatalf("reloaded title = %#v", values["title"])
	}
}

func TestSessionSearchUsesOnlyCurrentMessageSurface(t *testing.T) {
	e := newIntegrationEngine(t)
	id := createToolSession(t, e)
	s := mustSession(t, e, id)
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	first, err := e.appendEvent(s, "user/message", map[string]any{
		"content": []ContentBlock{{Type: "text", Text: "shadow-only needle"}},
		"source":  map[string]any{"kind": "user"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEventWithMetadata(s, "user/message", map[string]any{
		"content": []ContentBlock{{Type: "text", Text: "current replacement"}},
		"source":  map[string]any{"kind": "plugin", "plugin": "test"},
	}, map[string]any{"op": "replace", "start": first.Seq, "end": first.Seq}, []int{first.Seq}, false); err != nil {
		t.Fatal(err)
	}
	if items, more := e.SearchSessions("needle"); more || len(items) != 0 {
		t.Fatalf("shadowed search = %#v, more=%v", items, more)
	}
	if items, more := e.SearchSessions("replacement"); more || len(items) != 1 || items[0]["sessionId"] != id {
		t.Fatalf("current search = %#v, more=%v", items, more)
	}
}
