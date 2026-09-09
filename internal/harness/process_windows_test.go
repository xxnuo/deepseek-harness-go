//go:build windows

package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestWindowsPowerShellContract(t *testing.T) {
	if shellToolName != "pwsh" || !shellToolPersistent {
		t.Fatalf("Windows shell identity = %q, persistent=%v", shellToolName, shellToolPersistent)
	}
	want := []string{"-NoLogo", "-NoProfile", "-NonInteractive", "-Command", powershellEncodingPreamble + "Get-Location"}
	if got := powershellArgs("Get-Location"); !reflect.DeepEqual(got, want) {
		t.Fatalf("PowerShell args = %#v", got)
	}
	wrapped := persistentShellWrapper("Write-Output 'quoted'", "start", "end:")
	if !strings.Contains(wrapped, `[ScriptBlock]::Create('Write-Output ''quoted''')`) || !strings.Contains(wrapped, "[Console]::Out.WriteLine('end:' + [string]$__dsh_code)") {
		t.Fatalf("persistent PowerShell wrapper = %q", wrapped)
	}
}

func TestWindowsSandboxPureContracts(t *testing.T) {
	if got := windowsCapabilitySID(`C:\work`, false); got != "S-1-4-187239658-163713272" {
		t.Fatalf("workspace SID = %q", got)
	}
	if got := windowsCapabilitySID(`C:\temp\dsh-1`, true); got != "S-1-4-821961279-309433518-1" {
		t.Fatalf("temp SID = %q", got)
	}
	env := replaceWindowsEnvironment([]string{"Path=C:\\bin", "temp=old", "KEEP=value"}, map[string]string{"TEMP": "private", "TMP": "private"})
	joined := strings.Join(env, "\n")
	if strings.Contains(strings.ToLower(joined), "temp=old") || !strings.Contains(joined, "TEMP=private") || !strings.Contains(joined, "TMP=private") || !strings.Contains(joined, "KEEP=value") {
		t.Fatalf("replaced environment = %#v", env)
	}
}

func TestWindowsACLSandboxWorkspaceBoundary(t *testing.T) {
	program, err := resolvePowerShell()
	if err != nil {
		t.Skip(err)
	}
	workspace, outside := t.TempDir(), t.TempDir()
	insideFile := filepath.Join(workspace, "inside.txt")
	outsideFile := filepath.Join(outside, "outside.txt")
	command := fmt.Sprintf(
		"$ErrorActionPreference='Stop'; Set-Content -LiteralPath %s -Value inside; Set-Content -LiteralPath %s -Value outside",
		powershellQuote(insideFile), powershellQuote(outsideFile),
	)
	cmd := exec.Command(program, powershellArgs(command)...)
	cmd.Dir = workspace
	cmd.Env = scrubbedChildEnv(shellEnvironment())
	configureChildProcess(cmd)
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.CreationFlags&windows.CREATE_NO_WINDOW == 0 {
		t.Fatal("Windows subprocess is not configured to hide its console window")
	}
	state, err := prepareShellChild(cmd, sandboxWorkspaceWrite, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := startShellChild(cmd, state); err != nil {
		t.Fatal(err)
	}
	if err := waitShellChild(cmd, state); err == nil {
		t.Fatal("workspace-write command unexpectedly wrote outside the workspace")
	}
	if _, err := os.Stat(insideFile); err != nil {
		t.Fatalf("inside write failed: %v", err)
	}
	if _, err := os.Stat(outsideFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside write exists: %v", err)
	}

	readOnlyFile := filepath.Join(workspace, "read-only.txt")
	cmd = exec.Command(program, powershellArgs("$ErrorActionPreference='Stop'; Set-Content -LiteralPath "+powershellQuote(readOnlyFile)+" -Value denied")...)
	cmd.Dir = workspace
	cmd.Env = scrubbedChildEnv(shellEnvironment())
	configureChildProcess(cmd)
	state, err = prepareShellChild(cmd, sandboxReadOnly, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := startShellChild(cmd, state); err != nil {
		t.Fatal(err)
	}
	if err := waitShellChild(cmd, state); err == nil {
		t.Fatal("read-only command unexpectedly wrote into the workspace")
	}
	if _, err := os.Stat(readOnlyFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only file exists: %v", err)
	}
}

func TestWindowsPersistentPowerShellState(t *testing.T) {
	if _, err := resolvePowerShell(); err != nil {
		t.Skip(err)
	}
	workspace := t.TempDir()
	nested := filepath.Join(workspace, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	shell := &persistentShell{workspace: workspace, mode: sandboxDangerFull, timeout: 10 * time.Second}
	t.Cleanup(shell.close)
	if output, err := shell.run(context.Background(), "$env:DSH_PERSISTED='value'; Set-Location -LiteralPath "+powershellQuote(nested)); err != nil || output != "" {
		t.Fatalf("state setup = %q, %v", output, err)
	}
	output, err := shell.run(context.Background(), `Write-Output ($env:DSH_PERSISTED + '|' + (Get-Location).Path)`)
	if err != nil || strings.TrimSpace(output) != "value|"+nested {
		t.Fatalf("persistent state = %q, %v", output, err)
	}
}

func TestWindowsConPTYRoundTrip(t *testing.T) {
	if _, err := resolvePowerShell(); err != nil {
		t.Skip(err)
	}
	config := normalizeTerminalConfig(TerminalConfig{Timeout: 10 * time.Second, IdleSilence: time.Second})
	backend, err := NewBashTerminalBackend(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	workspace := t.TempDir()
	session, err := backend.Spawn(ctx, TerminalBackendSpawnSpec{
		TerminalSpawnRequest: TerminalSpawnRequest{CWD: workspace},
		SessionID:            "pty-1", OwnerID: "owner", Workspace: workspace, SandboxMode: sandboxDangerFull,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close("test cleanup") })
	operation, err := session.StartSend(ctx, TerminalSendRequest{Text: "Write-Output 'conpty-ok'", Submit: true})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-operation.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result, err := operation.Result()
	if err != nil || !strings.Contains(result.Viewport, "conpty-ok") {
		t.Fatalf("ConPTY result = %#v, %v", result, err)
	}
}

func TestWindowsConPTYTerminateAfterExit(t *testing.T) {
	program, err := resolvePowerShell()
	if err != nil {
		t.Skip(err)
	}
	pty, err := startWindowsConPTY(
		program, powershellArgs("exit 0"), t.TempDir(), scrubbedChildEnv(nil), 40, 160, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	outputDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, pty.output)
		close(outputDone)
	}()
	if code, err := pty.result(); err != nil || code != 0 {
		t.Fatalf("ConPTY exit = %d, %v", code, err)
	}
	if err := pty.terminate(1); err != nil {
		t.Fatalf("terminate after exit: %v", err)
	}
	if _, err := pty.signal(TerminalSignalTerminate); err != nil {
		t.Fatalf("signal after exit: %v", err)
	}
	_ = pty.output.Close()
	<-outputDone
}

func TestWindowsTerminalSignalContract(t *testing.T) {
	pty := &windowsConPTY{pid: 123}
	for _, signal := range []string{TerminalSignalStop, TerminalSignalHangup} {
		if _, err := pty.signal(signal); err == nil || !strings.Contains(err.Error(), "unsupported on Windows") {
			t.Fatalf("%s error = %v", signal, err)
		}
	}
	session := &windowsPTYSession{pty: pty, status: runningTerminalStatus()}
	if _, err := session.Signal(TerminalSignalKill); err == nil || !strings.Contains(err.Error(), "refusing to SIGKILL") {
		t.Fatalf("SIGKILL error = %v", err)
	}
}
