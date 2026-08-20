package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func requirePersistentShell(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("persistent bash currently requires Linux bubblewrap")
	}
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bwrap is not installed")
	}
}

func runPersistentTool(t *testing.T, tool Tool, sessionID, workspace, command string) string {
	t.Helper()
	arguments, err := json.Marshal(map[string]string{"command": command})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := tool.Execute(ctx, ToolCall{Name: "bash", Arguments: arguments, SessionID: sessionID, Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	return contentValueText(result.Content)
}

func TestPersistentShellMinimalPresetStateAndBoundary(t *testing.T) {
	requirePersistentShell(t)
	e := newIntegrationEngine(t)
	workspace := e.Config().Workspace
	sessionID, err := e.CreateSession(context.Background(), workspace, "persistent-minimal", "minimal")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	schemas, err := e.toolsForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	var bash ToolSchema
	for _, schema := range schemas {
		if schema.Name == "bash" {
			bash = schema
			break
		}
	}
	properties, _ := bash.Parameters["properties"].(map[string]any)
	if len(properties) != 1 || properties["command"] == nil || !reflect.DeepEqual(bash.Parameters["required"], []string{"command"}) {
		t.Fatalf("minimal bash parameters = %#v", bash.Parameters)
	}
	if !strings.Contains(bash.Description, `contents of the "command" parameter does NOT need to be XML-escaped`) {
		t.Fatalf("minimal bash description = %q", bash.Description)
	}

	e.mu.RLock()
	tool := e.tools["bash"]
	e.mu.RUnlock()
	stateDir := filepath.Join(workspace, "persistent-state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if output := runPersistentTool(t, tool, sessionID, workspace, "cd "+bashANSIQuote(stateDir)+" && export DSH_PERSISTED=value"); output != "" {
		t.Fatalf("state setup output = %q", output)
	}
	output := runPersistentTool(t, tool, sessionID, workspace, `printf 'stdout:%s:%s' "$DSH_PERSISTED" "$PWD"; printf ':stderr' >&2; false`)
	want := "stdout:value:" + stateDir + ":stderr\n[exit code: 1]"
	if output != want {
		t.Fatalf("persistent output = %q, want %q", output, want)
	}
	output = runPersistentTool(t, tool, sessionID, workspace, `head -c 17000 /dev/zero | tr '\0' x; false`)
	if !strings.Contains(output, "<response clipped>") || !strings.HasSuffix(output, "[exit code: 1]") {
		t.Fatalf("clipped nonzero output suffix = %q", output[max(0, len(output)-128):])
	}

	outside := filepath.Join("/var/tmp", newID("persistent-shell-outside"))
	_ = os.Remove(outside)
	output = runPersistentTool(t, tool, sessionID, workspace, "printf blocked > "+bashANSIQuote(outside))
	if !strings.Contains(output, "[exit code: 1]") {
		t.Fatalf("workspace escape output = %q", output)
	}
	if _, err := os.Stat(outside); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside file exists: %v", err)
	}

	output = runPersistentTool(t, tool, sessionID, workspace, "exit 9")
	if !strings.Contains(output, "[shell exited: code 9]") || !strings.Contains(output, persistentShellResetMessage) {
		t.Fatalf("shell exit output = %q", output)
	}
	output = runPersistentTool(t, tool, sessionID, workspace, `printf '%s:%s' "${DSH_PERSISTED-unset}" "$PWD"`)
	if output != "unset:"+workspace {
		t.Fatalf("reset shell output = %q", output)
	}
}

func TestPersistentShellTimeoutCancelAndQueue(t *testing.T) {
	requirePersistentShell(t)
	workspace := t.TempDir()
	shell := &persistentShell{workspace: workspace, timeout: 40 * time.Millisecond}
	t.Cleanup(shell.close)

	leaked := filepath.Join(workspace, "leaked")
	output, err := shell.run(context.Background(), `(sleep 0.3; printf leaked > `+bashANSIQuote(leaked)+`) & printf partial; wait`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, "timed out after 0 seconds") || !strings.Contains(output, "partial") || !strings.Contains(output, persistentShellResetMessage) {
		t.Fatalf("timeout output = %q", output)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := os.Stat(leaked); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("timed-out process survived: %v", err)
	}

	shell.timeout = 5 * time.Second
	started := filepath.Join(workspace, "started")
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, runErr := shell.run(ctx, "printf started > "+bashANSIQuote(started)+"; sleep 30")
		first <- runErr
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, statErr := os.Stat(started); statErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancellable command did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	second := make(chan struct {
		text string
		err  error
	}, 1)
	go func() {
		text, runErr := shell.run(context.Background(), "printf after")
		second <- struct {
			text string
			err  error
		}{text: text, err: runErr}
	}()
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}
	select {
	case result := <-second:
		if result.err != nil || result.text != "after" {
			t.Fatalf("queued result = %q, %v", result.text, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued command did not resume")
	}
}

func TestPersistentShellRegistryWorkspaceAndClose(t *testing.T) {
	requirePersistentShell(t)
	registry := newPersistentShellRegistry()
	first := t.TempDir()
	second := t.TempDir()
	if _, err := registry.run(context.Background(), "owner", first, "pwd"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.run(context.Background(), "owner", second, "pwd"); err == nil || !strings.Contains(err.Error(), "workspace changed") {
		t.Fatalf("workspace change error = %v", err)
	}
	registry.close()
	if _, err := registry.run(context.Background(), "owner", first, "pwd"); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("closed registry error = %v", err)
	}
}

func TestEngineCloseCancelsPersistentShellFirst(t *testing.T) {
	requirePersistentShell(t)
	e := newIntegrationEngine(t)
	workspace := e.Config().Workspace
	sessionID, err := e.CreateSession(context.Background(), workspace, "persistent-close", "minimal")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	session.mu.Lock()
	session.Cancel = cancel
	session.mu.Unlock()
	started := filepath.Join(workspace, "close-started")
	running := make(chan error, 1)
	go func() {
		_, runErr := e.shells.run(ctx, sessionID, workspace, "printf started > "+bashANSIQuote(started)+"; sleep 30")
		running <- runErr
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, statErr := os.Stat(started); statErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("persistent command did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Engine.Close blocked behind persistent command")
	}
	if err := <-running; !errors.Is(err, context.Canceled) {
		t.Fatalf("persistent command error = %v", err)
	}
}

func TestReadPersistentShellResultFraming(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("startup\nSTART\r\nstdout\nstderrEND:7\r\ntrailing"))
	result := readPersistentShellResult(reader, "START", "END:")
	if result.err != nil || !result.started || result.exitCode != 7 || result.text != "stdout\nstderr" {
		t.Fatalf("framed result = %#v", result)
	}
	remaining, err := reader.ReadString('\n')
	if err == nil || remaining != "trailing" {
		t.Fatalf("remaining output = %q, %v", remaining, err)
	}
}
