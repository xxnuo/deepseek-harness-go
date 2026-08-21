package harness

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newLocalE2BTestRuntime(t *testing.T) *E2BRuntime {
	t.Helper()
	root := t.TempDir()
	runtime, err := NewE2BRuntime(context.Background(), E2BConfig{LocalRoot: root, CWD: "/home/user/workspace"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	return runtime
}

func TestE2BLocalFilesystemAndProcess(t *testing.T) {
	runtime := newLocalE2BTestRuntime(t)
	fs := NewE2BFileSystem(runtime)
	target, err := fs.Resolve(context.Background(), "hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	created, err := fs.WriteText(context.Background(), target, "one\ntwo\n", &E2BFSWriteIntent{Kind: "createIfAbsent"})
	if err != nil {
		t.Fatal(err)
	}
	if created.Operation != "create" || created.Version == "" {
		t.Fatalf("unexpected write: %#v", created)
	}
	edited, err := fs.EditText(context.Background(), target, "one", "ONE", created.Version, false)
	if err != nil {
		t.Fatal(err)
	}
	if edited.After != "ONE\ntwo\n" {
		t.Fatalf("unexpected edit: %q", edited.After)
	}
	text, err := fs.ReadText(context.Background(), target)
	if err != nil || text != "ONE\ntwo\n" {
		t.Fatalf("read=%q err=%v", text, err)
	}

	var stdout strings.Builder
	proc, err := NewE2BSubprocessRuntime(runtime).Spawn(context.Background(), E2BSubprocessSpec{
		Argv: []string{"/bin/sh", "-c", "printf process-ok"}, CWD: "/home/user/workspace",
		OnStdout: func(data []byte) { stdout.Write(data) }, Grace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := proc.Wait()
	if err != nil || result.ExitCode != 0 || stdout.String() != "process-ok" {
		t.Fatalf("result=%#v err=%v stdout=%q", result, err, stdout.String())
	}
}

func TestE2BLocalTerminalRunsRealProcess(t *testing.T) {
	runtime := newLocalE2BTestRuntime(t)
	subprocess := NewE2BSubprocessRuntime(runtime)
	terminal, err := subprocess.SpawnTerminal(context.Background(), E2BPTYOptions{Rows: 24, Cols: 80, CWD: "/home/user/workspace", Env: map[string]string{"TERM": "dumb"}})
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Close()
	if terminal.PID() <= 0 {
		t.Fatalf("invalid terminal pid %d", terminal.PID())
	}
	if err := terminal.Write([]byte("printf terminal-ok\nexit\n")); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	var output strings.Builder
	for {
		select {
		case chunk, ok := <-terminal.Output():
			if !ok {
				goto done
			}
			output.Write(chunk)
		case <-deadline:
			t.Fatal("terminal did not exit")
		}
	}
done:
	result, err := terminal.Wait()
	if err != nil || result.ExitCode != 0 || !strings.Contains(output.String(), "terminal-ok") {
		t.Fatalf("result=%#v err=%v output=%q", result, err, output.String())
	}
}

func TestE2BLocalSandboxMapsRemotePaths(t *testing.T) {
	root := t.TempDir()
	sandbox, err := NewLocalSandboxFactory(root)(context.Background(), E2BConfig{CWD: "/home/user/workspace"})
	if err != nil {
		t.Fatal(err)
	}
	if err := sandbox.Files().MakeDir(context.Background(), "/home/user/workspace"); err != nil {
		t.Fatal(err)
	}
	if _, err := sandbox.Files().Write(context.Background(), "/home/user/workspace/a.txt", []byte("a"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "home/user/workspace/a.txt")); err != nil {
		t.Fatal(err)
	}
}
