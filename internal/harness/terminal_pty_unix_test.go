//go:build linux

package harness

import (
	"context"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func newLinuxPTYTestEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	shell, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is required for PTY integration tests")
	}
	t.Setenv("DSH_PERMISSION_MODE", sandboxDangerFull)
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Persist = false
	cfg.Terminal = TerminalConfig{
		BackendType: "shell", ShellPath: shell, ShellArgs: []string{"--noprofile", "--norc", "-i"},
		Rows: 24, Cols: 100, ScrollbackLines: 1_000, ScrollbackMaxBytes: 256 * 1024,
		MaxReadBytes: 64 * 1024, PollInterval: 10 * time.Millisecond,
		ExactProbeAfter: 20 * time.Millisecond, IdleSilence: 100 * time.Millisecond,
		HandoffGrace: 50 * time.Millisecond, Timeout: 3 * time.Second, DisposeGrace: 250 * time.Millisecond,
	}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	owner, err := engine.CreateSession(context.Background(), cfg.Workspace, newID("pty-linux"), "")
	if err != nil {
		t.Fatal(err)
	}
	return engine, owner
}

func openLinuxPTY(t *testing.T, engine *Engine, owner string) TerminalSpawnResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opened, err := engine.OpenTerminal(ctx, owner, TerminalSpawnRequest{Type: "shell"})
	if err != nil {
		t.Fatal(err)
	}
	if opened.PID <= 0 || opened.Status.Kind != "running" {
		t.Fatalf("opened terminal = %#v", opened)
	}
	return opened
}

func sendLinuxPTY(t *testing.T, engine *Engine, owner, sessionID, text string) TerminalSendResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	result, err := engine.SendTerminal(ctx, owner, sessionID, TerminalSendRequest{Text: text, Submit: true})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func awaitLinuxPTYOperation(t *testing.T, operation TerminalSendOperation) TerminalSendResult {
	t.Helper()
	select {
	case <-operation.Done():
	case <-time.After(4 * time.Second):
		operation.Cancel()
		t.Fatal("timed out waiting for PTY send")
	}
	result, err := operation.Result()
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func waitForLinuxPTYForeground(t *testing.T, root int) linuxProcessIdentity {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, process := range linuxOwnedProcesses(root) {
			if process.group > 0 && process.group != root && linuxProcessAlive(process) {
				return process
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("terminal %d did not start a foreground process group", root)
	return linuxProcessIdentity{}
}

func requireLinuxProcessGone(t *testing.T, process linuxProcessIdentity) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !linuxProcessAlive(process) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d survived PTY cleanup", process.pid)
}

func TestLinuxPTYStateAndInteractiveInput(t *testing.T) {
	engine, owner := newLinuxPTYTestEngine(t)
	opened := openLinuxPTY(t, engine, owner)

	first := sendLinuxPTY(t, engine, owner, opened.SessionID, `export DSH_PTY_STATE=kept; printf '__first__%s\n' "$DSH_PTY_STATE"`)
	if first.WaitReason != TerminalWaitStdinRead || first.SessionStatus.Kind != "running" || !strings.Contains(first.Viewport, "__first__kept") {
		t.Fatalf("first send = %#v", first)
	}
	second := sendLinuxPTY(t, engine, owner, opened.SessionID, `printf '__second__%s\n' "$DSH_PTY_STATE"`)
	if second.WaitReason != TerminalWaitStdinRead || !strings.Contains(second.Viewport, "__second__kept") {
		t.Fatalf("state did not survive send: %#v", second)
	}

	waiting := sendLinuxPTY(t, engine, owner, opened.SessionID, `read -r DSH_PTY_INPUT; printf '__input__%s\n' "$DSH_PTY_INPUT"`)
	if waiting.WaitReason != TerminalWaitStdinRead || strings.Contains(waiting.Viewport, "__input__value") {
		t.Fatalf("interactive read did not wait for stdin: %#v", waiting)
	}
	input := sendLinuxPTY(t, engine, owner, opened.SessionID, "value")
	if input.WaitReason != TerminalWaitStdinRead || !strings.Contains(input.Viewport, "__input__value") {
		t.Fatalf("interactive input result = %#v", input)
	}

	read, err := engine.ReadTerminal(owner, opened.SessionID, TerminalReadRequest{Count: 50})
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"__first__kept", "__second__kept", "__input__value"} {
		if !strings.Contains(read.Text, marker) {
			t.Fatalf("scrollback missing %q: %#v", marker, read)
		}
	}
}

func TestLinuxPTYSIGINTInterruptsForegroundAndKeepsShell(t *testing.T) {
	engine, owner := newLinuxPTYTestEngine(t)
	opened := openLinuxPTY(t, engine, owner)
	operation, err := engine.StartTerminalSend(context.Background(), owner, opened.SessionID, TerminalSendRequest{
		Text: `DSH_PTY_AFTER_INT=unreached; sleep 30; printf '__%s__\n' "$DSH_PTY_AFTER_INT"`, Submit: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	foreground := waitForLinuxPTYForeground(t, opened.PID)
	signal, err := engine.SignalTerminal(owner, opened.SessionID, TerminalSignalInterrupt)
	if err != nil {
		t.Fatal(err)
	}
	if !signal.Delivered || signal.TargetPGID != foreground.group {
		t.Fatalf("SIGINT result = %#v", signal)
	}
	result := awaitLinuxPTYOperation(t, operation)
	if result.WaitReason != TerminalWaitStdinRead || result.SessionStatus.Kind != "running" || strings.Contains(result.Viewport, "__unreached__") {
		t.Fatalf("interrupted send = %#v", result)
	}
	after := sendLinuxPTY(t, engine, owner, opened.SessionID, `printf '__alive_%s__\n' ok`)
	if !strings.Contains(after.Viewport, "__alive_ok__") || after.SessionStatus.Kind != "running" {
		t.Fatalf("shell did not survive SIGINT: %#v", after)
	}
}

var linuxPTYChildPattern = regexp.MustCompile(`__child__(\d+)`)

func startLinuxPTYBackgroundChild(t *testing.T, engine *Engine, owner string, opened TerminalSpawnResult) linuxProcessIdentity {
	t.Helper()
	result := sendLinuxPTY(t, engine, owner, opened.SessionID, `bash -c 'trap "" HUP TERM; exec sleep 30' & printf '__child__%s\n' "$!"`)
	match := linuxPTYChildPattern.FindStringSubmatch(result.Viewport)
	if len(match) != 2 {
		t.Fatalf("background child pid missing from %#v", result)
	}
	pid, err := strconv.Atoi(match[1])
	if err != nil {
		t.Fatal(err)
	}
	process, ok := readLinuxProcess(pid)
	if !ok || !linuxProcessAlive(process) {
		t.Fatalf("background child %d was not alive", pid)
	}
	return process
}

func TestLinuxPTYCloseTerminalKillsBackgroundChild(t *testing.T) {
	engine, owner := newLinuxPTYTestEngine(t)
	opened := openLinuxPTY(t, engine, owner)
	root, ok := readLinuxProcess(opened.PID)
	if !ok {
		t.Fatalf("terminal process %d was not alive", opened.PID)
	}
	child := startLinuxPTYBackgroundChild(t, engine, owner, opened)
	closed, err := engine.CloseTerminal(owner, opened.SessionID)
	if err != nil || !closed {
		t.Fatalf("close terminal = %v, %v", closed, err)
	}
	requireLinuxProcessGone(t, child)
	requireLinuxProcessGone(t, root)
}

func TestLinuxPTYEngineCloseKillsBackgroundChild(t *testing.T) {
	engine, owner := newLinuxPTYTestEngine(t)
	opened := openLinuxPTY(t, engine, owner)
	root, ok := readLinuxProcess(opened.PID)
	if !ok {
		t.Fatalf("terminal process %d was not alive", opened.PID)
	}
	child := startLinuxPTYBackgroundChild(t, engine, owner, opened)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	requireLinuxProcessGone(t, child)
	requireLinuxProcessGone(t, root)
}
