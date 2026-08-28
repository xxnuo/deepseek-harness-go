package harness

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func createSandboxTestSession(t *testing.T, e *Engine, id string, preset ...string) (*Session, string) {
	t.Helper()
	selected := ""
	if len(preset) > 0 {
		selected = preset[0]
	}
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, id, selected)
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	return session, sessionID
}

func requireSandboxDenial(t *testing.T, err error, mode string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "FS_SANDBOX_DENIED") || !strings.Contains(err.Error(), "under "+mode+" mode") {
		t.Fatalf("sandbox denial for %s = %v", mode, err)
	}
}

func TestSandboxErrorsPreserveStructuredToolCode(t *testing.T) {
	err := sandboxPolicyDenied(sandboxWorkspaceWrite, "file mutation")
	if got := toolExecutionError(err); got.Code != "FS_SANDBOX_DENIED" || got.Message != err.Error() {
		t.Fatalf("tool error = %#v", got)
	}
}

func TestFilesystemToolsHonorSessionSandboxMode(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", sandboxWorkspaceWrite)
	e := newIntegrationEngine(t)
	session, sessionID := createSandboxTestSession(t, e, "fs-sandbox")
	workspace := e.Config().Workspace

	inside := filepath.Join(workspace, "inside.txt")
	if _, err := callBuiltin(t, e, sessionID, "write", map[string]any{"file_path": inside, "content": "inside"}); err != nil {
		t.Fatal(err)
	}
	temp := filepath.Join(os.TempDir(), newID("dsh-fs-temp"))
	t.Cleanup(func() { _ = os.Remove(temp) })
	if _, err := callBuiltin(t, e, sessionID, "write", map[string]any{"file_path": temp, "content": "temp"}); err != nil {
		t.Fatalf("workspace-write temp write: %v", err)
	}

	outside := filepath.Join("/var/tmp", newID("dsh-fs-outside"))
	t.Cleanup(func() { _ = os.Remove(outside) })
	_, err := callBuiltin(t, e, sessionID, "write", map[string]any{"file_path": outside, "content": "blocked"})
	requireSandboxDenial(t, err, sandboxWorkspaceWrite)
	if _, statErr := os.Stat(outside); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("workspace-write created outside file: %v", statErr)
	}

	symlink := filepath.Join(workspace, "outside-link")
	if err := os.Symlink("/var/tmp", symlink); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(symlink, newID("dsh-fs-linked"))
	_, err = callBuiltin(t, e, sessionID, "write", map[string]any{"file_path": linked, "content": "blocked"})
	requireSandboxDenial(t, err, sandboxWorkspaceWrite)

	if result, readErr := callBuiltin(t, e, sessionID, "read", map[string]any{"file_path": "/etc/hosts"}); readErr != nil || !strings.Contains(resultText(result), "1: ") {
		t.Fatalf("absolute read = %q, %v", resultText(result), readErr)
	}

	if result, commandErr := e.runCommand(session, "/permission read-only"); commandErr != nil || result.Command == nil || result.Command.Kind != "success" {
		t.Fatalf("read-only command = %#v, %v", result, commandErr)
	}
	_, err = callBuiltin(t, e, sessionID, "edit", map[string]any{"file_path": inside, "old_string": "inside", "new_string": "blocked"})
	requireSandboxDenial(t, err, sandboxReadOnly)
	_, err = callBuiltin(t, e, sessionID, "str_replace_editor", map[string]any{"command": "create", "path": filepath.Join(workspace, "blocked.txt"), "file_text": "blocked"})
	requireSandboxDenial(t, err, sandboxReadOnly)

	if _, commandErr := e.runCommand(session, "/permission danger-full-access"); commandErr != nil {
		t.Fatal(commandErr)
	}
	if _, err := callBuiltin(t, e, sessionID, "write", map[string]any{"file_path": outside, "content": "full"}); err != nil {
		t.Fatalf("danger-full-access write: %v", err)
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "full" {
		t.Fatalf("danger-full-access file = %q, %v", data, err)
	}
}

func TestPersistentBashRestartsAcrossSandboxModeChanges(t *testing.T) {
	requirePersistentShell(t)
	t.Setenv("DSH_PERMISSION_MODE", sandboxWorkspaceWrite)
	e := newIntegrationEngine(t)
	session, sessionID := createSandboxTestSession(t, e, "persistent-sandbox", "minimal")

	if output := runPersistentTool(t, e.tools["bash"], sessionID, e.Config().Workspace, "export DSH_MODE_STATE=workspace"); output != "" {
		t.Fatalf("workspace setup = %q", output)
	}
	blockedSwitch, err := e.runCommand(session, "/permission danger-full-access")
	if err != nil || blockedSwitch.Command == nil || blockedSwitch.Command.Kind != "error" || !strings.Contains(blockedSwitch.Command.Text, "open or being created") {
		t.Fatalf("open persistent shell permission switch = %#v, %v", blockedSwitch, err)
	}
	e.shells.closeOwner(sessionID)
	if switched, err := e.runCommand(session, "/permission danger-full-access"); err != nil || switched.Command == nil || switched.Command.Kind != "success" {
		t.Fatalf("permission switch after shell close = %#v, %v", switched, err)
	}
	outside := filepath.Join("/var/tmp", newID("dsh-persistent-full"))
	t.Cleanup(func() { _ = os.Remove(outside) })
	output := runPersistentTool(t, e.tools["bash"], sessionID, e.Config().Workspace, "printf '%s' \"${DSH_MODE_STATE-unset}\"; printf full > "+bashANSIQuote(outside))
	if output != "unset" {
		t.Fatalf("danger-full-access did not restart persistent shell: %q", output)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("danger-full-access persistent write: %v", err)
	}
	blockedSwitch, err = e.runCommand(session, "/permission read-only")
	if err != nil || blockedSwitch.Command == nil || blockedSwitch.Command.Kind != "error" || !strings.Contains(blockedSwitch.Command.Text, "open or being created") {
		t.Fatalf("open persistent shell read-only switch = %#v, %v", blockedSwitch, err)
	}
	e.shells.closeOwner(sessionID)
	if switched, err := e.runCommand(session, "/permission read-only"); err != nil || switched.Command == nil || switched.Command.Kind != "success" {
		t.Fatalf("read-only switch after shell close = %#v, %v", switched, err)
	}
	result, err := callBuiltin(t, e, sessionID, "bash", map[string]any{"command": "printf blocked > blocked.txt", "description": "verify read-only persistent denial"})
	if err != nil || !strings.Contains(resultText(result), sandboxDenialMarker(sandboxReadOnly)) {
		t.Fatalf("read-only persistent result = %q, %v", resultText(result), err)
	}
}

func TestOneShotAndBackgroundBashHonorSandboxMode(t *testing.T) {
	requirePersistentShell(t)
	t.Setenv("DSH_PERMISSION_MODE", sandboxWorkspaceWrite)
	e := newIntegrationEngine(t)
	session, sessionID := createSandboxTestSession(t, e, "bash-sandbox")
	outside := filepath.Join("/var/tmp", newID("dsh-bash-outside"))
	t.Cleanup(func() { _ = os.Remove(outside) })

	result, err := callBuiltin(t, e, sessionID, "bash", map[string]any{
		"command": "printf blocked > " + bashANSIQuote(outside), "description": "verify workspace boundary",
	})
	if err != nil || !strings.Contains(resultText(result), sandboxDenialMarker(sandboxWorkspaceWrite)) {
		t.Fatalf("workspace-write bash = %q, %v", resultText(result), err)
	}
	if _, statErr := os.Stat(outside); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("workspace-write bash created outside file: %v", statErr)
	}
	result, err = callBuiltin(t, e, sessionID, "bash", map[string]any{"command": "pwd", "description": "verify absolute workdir", "workdir": "/etc"})
	if err != nil || !strings.Contains(resultText(result), "/etc") {
		t.Fatalf("absolute workdir = %q, %v", resultText(result), err)
	}
	hostNet, err := os.Readlink("/proc/self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	result, err = callBuiltin(t, e, sessionID, "bash", map[string]any{"command": "readlink /proc/self/ns/net", "description": "verify network namespace"})
	if err != nil || !strings.Contains(resultText(result), hostNet) {
		t.Fatalf("network namespace = %q, want %q, err=%v", resultText(result), hostNet, err)
	}

	if _, err := e.runCommand(session, "/permission read-only"); err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(e.Config().Workspace, "read-only-blocked.txt")
	result, err = callBuiltin(t, e, sessionID, "bash", map[string]any{
		"command": "printf blocked > " + bashANSIQuote(blocked), "description": "verify read-only boundary",
	})
	if err != nil || !strings.Contains(resultText(result), sandboxDenialMarker(sandboxReadOnly)) {
		t.Fatalf("read-only bash = %q, %v", resultText(result), err)
	}
	jobID := startBackgroundBash(t, e, sessionID, "printf blocked > "+bashANSIQuote(blocked))
	job, err := callBuiltin(t, e, sessionID, "job_output", map[string]any{"job_id": jobID, "wait": true, "timeout_ms": 3000})
	if err != nil || !strings.Contains(resultText(job), sandboxDenialMarker(sandboxReadOnly)) {
		t.Fatalf("read-only background bash = %q, %v", resultText(job), err)
	}

	if _, err := e.runCommand(session, "/permission danger-full-access"); err != nil {
		t.Fatal(err)
	}
	result, err = callBuiltin(t, e, sessionID, "bash", map[string]any{
		"command": "printf full > " + bashANSIQuote(outside), "description": "verify full access",
	})
	if err != nil || !strings.Contains(resultText(result), "[exit code: 0]") {
		t.Fatalf("danger-full-access bash = %q, %v", resultText(result), err)
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "full" {
		t.Fatalf("danger-full-access bash file = %q, %v", data, err)
	}
}

func TestSandboxEscalationIsApprovedOnceAndAudited(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", sandboxWorkspaceWrite)
	e := newIntegrationEngine(t)
	session, sessionID := createSandboxTestSession(t, e, "sandbox-escalation")
	if _, err := e.appendEvent(session, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join("/var/tmp", newID("dsh-approved-once"))
	t.Cleanup(func() { _ = os.Remove(target) })
	raw, err := json.Marshal(map[string]any{
		"file_path": target, "content": "approved", "sandbox_permissions": sandboxDangerFull,
		"justification": "Write the requested external fixture.",
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, executeErr := e.tools["write"].Execute(context.Background(), ToolCall{
			ID: "call-approved", Name: "write", SessionID: sessionID, Workspace: e.Config().Workspace, Arguments: raw,
		})
		result <- executeErr
	}()

	deadline := time.Now().Add(2 * time.Second)
	var pending *pendingInteraction
	for pending == nil {
		for _, candidate := range e.pendingInteractions() {
			if candidate.method == "approval/requested" {
				pending = candidate
				break
			}
		}
		if pending == nil {
			if time.Now().After(deadline) {
				t.Fatal("approval interaction was not published")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	payload, ok := pending.payload.(map[string]any)
	if !ok || payload["toolName"] != "write" || payload["callId"] != "call-approved" {
		t.Fatalf("approval payload = %#v", pending.payload)
	}
	approvalID, _ := payload["approvalId"].(string)
	if approvalID == "" || !e.ResolveInteraction(pending.id, map[string]any{
		"ok":    true,
		"value": map[string]any{"sessionId": sessionID, "approvalId": approvalID, "outcome": "allowed-once"},
	}) {
		t.Fatal("approval interaction was not resolved")
	}
	if err := <-result; err != nil {
		t.Fatalf("approved write: %v", err)
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "approved" {
		t.Fatalf("approved file = %q, %v", data, err)
	}

	_, err = callBuiltin(t, e, sessionID, "write", map[string]any{"file_path": target, "content": "not standing"})
	requireSandboxDenial(t, err, sandboxWorkspaceWrite)
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	var asked, decided bool
	for _, event := range events {
		data, _ := event.Data.(map[string]any)
		if event.Type == "approval/asked" && data["id"] == approvalID {
			asked = data["toolName"] == "write" && data["callId"] == "call-approved"
		}
		if event.Type == "approval/decided" && data["id"] == approvalID {
			decided = data["outcome"] == "allowed-once"
		}
	}
	if !asked || !decided {
		t.Fatalf("approval audit pair missing: %#v", events)
	}
}
