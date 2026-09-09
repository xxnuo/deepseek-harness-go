package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWorkspaceFilePageWindows(t *testing.T) {
	for _, test := range []struct {
		name, input        string
		offset, limit, cap int
		text               string
		lines              int
		eof, fail          bool
	}{
		{"empty", "", 1, 1, 10, "", 0, true, false},
		{"empty line", "\n", 1, 1, 10, "", 1, true, false},
		{"terminated page", "a\nb\n", 1, 1, 10, "a", 1, false, false},
		{"last page", "a\nb\n", 2, 1, 10, "b", 1, true, false},
		{"unterminated", "a\nb", 1, 3, 10, "a\nb", 2, true, false},
		{"empty lines", "\n\n", 1, 3, 10, "\n", 2, true, false},
		{"beyond end", "a\n", 9, 2, 10, "", 0, true, false},
		{"skip oversized line", strings.Repeat("x", 10000) + "\nsmall", 2, 1, 5, "small", 1, true, false},
		{"byte cap", "你好", 1, 1, 5, "", 0, false, true},
		{"newline counts", "a\nb", 1, 2, 2, "", 0, false, true},
		{"binary", "a\x00b", 1, 2, 10, "", 0, false, true},
		{"invalid UTF8", "a\xffb", 1, 2, 10, "", 0, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			text, lines, eof, err := readWorkspaceFilePage(t.Context(), strings.NewReader(test.input), test.offset, test.limit, test.cap)
			if text != test.text || lines != test.lines || eof != test.eof || (err != nil) != test.fail {
				t.Fatalf("page = %q, %d, %v, %v", text, lines, eof, err)
			}
		})
	}
}

func TestWorkspaceFilesRemoteConfinesAndReads(t *testing.T) {
	engine := newIntegrationEngine(t)
	root := engine.Config().Workspace
	id, err := engine.CreateSession(t.Context(), root, "workspace-files", "")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "page.txt")
	if err := os.WriteFile(file, []byte("first\nsecond\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	invoke := func(method, path string, window map[string]any) (map[string]any, *RPCError) {
		t.Helper()
		args := map[string]any{"agentId": id, "path": path}
		if window != nil {
			args["range"] = window
		}
		raw, _ := json.Marshal(map[string]any{"args": args})
		value, _, rpcErr := engine.dispatchRemote(t.Context(), "workspaceFiles/"+method, raw)
		result, _ := value.(map[string]any)
		return result, rpcErr
	}
	page, rpcErr := invoke("read", "page.txt", map[string]any{"offset": 2, "limit": 1})
	if rpcErr != nil || page["text"] != "second" || page["eof"] != true || page["absolutePath"] != file {
		t.Fatalf("read = %#v, %v", page, rpcErr)
	}
	data, rpcErr := invoke("readBytes", "page.txt", map[string]any{"offset": 0, "length": 5})
	if rpcErr != nil || data["data"] != "Zmlyc3Q=" || data["eof"] != false {
		t.Fatalf("bytes = %#v, %v", data, rpcErr)
	}
	stat, rpcErr := invoke("stat", "page.txt", nil)
	if rpcErr != nil || stat["version"] != page["version"] || stat["bytes"] != int64(13) {
		t.Fatalf("stat = %#v, %v", stat, rpcErr)
	}
	listing, rpcErr := invoke("list", ".", nil)
	if rpcErr != nil || listing["path"] != "" || listing["truncated"] != false {
		t.Fatalf("list = %#v, %v", listing, rpcErr)
	}
	for _, test := range []struct{ method, path, code string }{
		{"read", outside, "workspace-file/outside-workspace"},
		{"read", ".", "workspace-file/not-regular-file"},
		{"list", "page.txt", "workspace-file/not-directory"},
		{"read", "missing.txt", "workspace-file/not-found"},
		{"read", "", "gateway/bad-request"},
	} {
		var window map[string]any
		if test.method == "read" {
			window = map[string]any{}
		}
		if _, rpcErr := invoke(test.method, test.path, window); rpcErr == nil || rpcErr.Code != test.code {
			t.Fatalf("%s %q error = %v, want %s", test.method, test.path, rpcErr, test.code)
		}
	}
	if err := os.Symlink(file, filepath.Join(root, "symlink")); err == nil {
		if _, rpcErr := invoke("stat", "symlink", nil); rpcErr == nil || rpcErr.Code != "workspace-file/not-regular-file" {
			t.Fatalf("symlink stat error = %v", rpcErr)
		}
	}
}

func TestWorkspaceFilesChangesObserveToolsAndCancel(t *testing.T) {
	engine := newIntegrationEngine(t)
	id, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "workspace-file-changes", "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	frames := make(chan map[string]any, 10)
	done := make(chan *RPCError, 1)
	payload, _ := json.Marshal(map[string]any{"args": map[string]any{"agentId": id}})
	go func() {
		done <- engine.runRemoteStream(ctx, "workspaceFiles/changes", payload, func(frame any) error {
			frames <- frame.(map[string]any)
			return nil
		})
	}()
	select {
	case frame := <-frames:
		if frame["kind"] != "ready" {
			t.Fatalf("first frame = %#v", frame)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("changes stream did not become ready")
	}
	if _, err := executeFSTool(t, engine, "write", map[string]any{"file_path": "watched.txt", "content": "hello"}); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-frames:
		change, _ := frame["change"].(map[string]any)
		if frame["kind"] != "change" || change["absolutePath"] != filepath.Join(engine.Config().Workspace, "watched.txt") || change["version"] == nil {
			t.Fatalf("change = %#v", frame)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("tool write observation missing")
	}
	cancel()
	select {
	case rpcErr := <-done:
		if rpcErr != nil {
			t.Fatal(rpcErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("changes stream did not cancel")
	}
	engine.fsState.mu.Lock()
	defer engine.fsState.mu.Unlock()
	if len(engine.fsState.followers) != 0 {
		t.Fatal("changes stream leaked a follower")
	}
}

func TestWorkspaceFilesChangesEndOnEngineClose(t *testing.T) {
	engine := newIntegrationEngine(t)
	id, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "workspace-file-close", "")
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	done := make(chan *RPCError, 1)
	payload, _ := json.Marshal(map[string]any{"args": map[string]any{"agentId": id}})
	go func() {
		done <- engine.runRemoteStream(t.Context(), "workspaceFiles/changes", payload, func(frame any) error {
			if frame.(map[string]any)["kind"] == "ready" {
				close(ready)
			}
			return nil
		})
	}()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("no ready frame")
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case rpcErr := <-done:
		if rpcErr != nil {
			t.Fatal(rpcErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("engine close leaked stream")
	}
}
