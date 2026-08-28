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

func callBuiltin(t *testing.T, e *Engine, sessionID, name string, args any) (ToolResult, error) {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool := e.tools[name]
	e.mu.RUnlock()
	return tool.Execute(context.Background(), ToolCall{Name: name, Arguments: raw, Workspace: e.cfg.Workspace, SessionID: sessionID})
}

func resultText(result ToolResult) string {
	if len(result.Content) == 0 {
		return ""
	}
	return result.Content[0].Text
}

func createJobTestSession(t *testing.T, e *Engine, id string) string {
	t.Helper()
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, id, "")
	if err != nil {
		t.Fatal(err)
	}
	return sessionID
}

func startBackgroundBash(t *testing.T, e *Engine, owner, command string) string {
	t.Helper()
	result, err := callBuiltin(t, e, owner, "bash", map[string]any{
		"command": command, "description": "run background test", "run_in_background": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	const prefix = "started background job "
	text := resultText(result)
	if !strings.HasPrefix(text, prefix) {
		t.Fatalf("start result = %q", text)
	}
	return strings.TrimPrefix(text, prefix)
}

func TestBackgroundBashIncrementalOutputAndWait(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner := createJobTestSession(t, e, "session-a")
	id := startBackgroundBash(t, e, owner, "printf first; sleep 0.4; printf second")

	deadline := time.Now().Add(2 * time.Second)
	var first string
	for time.Now().Before(deadline) {
		result, err := callBuiltin(t, e, owner, "job_output", map[string]any{"job_id": id})
		if err != nil {
			t.Fatal(err)
		}
		first = resultText(result)
		if strings.Contains(first, "first") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(first, "first") || strings.Contains(first, "second") {
		t.Fatalf("first incremental read = %q", first)
	}

	final, err := callBuiltin(t, e, owner, "job_output", map[string]any{"job_id": id, "wait": true, "timeout_ms": 3000})
	if err != nil {
		t.Fatal(err)
	}
	text := resultText(final)
	if !strings.Contains(text, "second") || !strings.Contains(text, "[status: completed, exit code: 0]") {
		t.Fatalf("final read = %q", text)
	}
	empty, err := callBuiltin(t, e, owner, "job_output", map[string]any{"job_id": id})
	if err != nil {
		t.Fatal(err)
	}
	if text := resultText(empty); !strings.Contains(text, "(no new output)") || !strings.Contains(text, "[status: completed, exit code: 0]") {
		t.Fatalf("consumed read = %q", text)
	}
}

func TestBackgroundJobCompletionQueuesOwnerNotice(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false), WithProvider("echo"), WithModel("echo"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner := createJobTestSession(t, e, "session-notice")
	_ = startBackgroundBash(t, e, owner, "printf notice-ok")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s, getErr := e.getSession(owner)
		if getErr == nil {
			s.mu.Lock()
			for _, event := range s.Events {
				if event.Type != "agent/inbox/spliced" {
					continue
				}
				data, _ := event.Data.(map[string]any)
				inserted, _ := data["inserted"].([]any)
				if len(inserted) == 0 {
					continue
				}
				message, _ := inserted[0].(map[string]any)
				source, _ := message["source"].(map[string]any)
				if source["plugin"] == "tool-jobs" {
					s.mu.Unlock()
					return
				}
			}
			s.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("background completion notice was not queued")
}

func TestBackgroundJobsAreOwnerIsolatedAndKillable(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner := createJobTestSession(t, e, "session-a")
	other := createJobTestSession(t, e, "session-b")
	id := startBackgroundBash(t, e, owner, "sleep 60")

	list, err := callBuiltin(t, e, other, "job_list", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if text := resultText(list); text != "(no background jobs)" {
		t.Fatalf("other owner list = %q", text)
	}
	if _, err := callBuiltin(t, e, other, "job_output", map[string]any{"job_id": id}); err == nil || !strings.Contains(err.Error(), "belongs to another session") {
		t.Fatalf("other owner read error = %v", err)
	}

	killed, err := callBuiltin(t, e, owner, "job_kill", map[string]any{"job_id": id, "reason": "test complete"})
	if err != nil {
		t.Fatal(err)
	}
	if text := resultText(killed); text != "requested cancellation of job "+id {
		t.Fatalf("kill result = %q", text)
	}
	final, err := callBuiltin(t, e, owner, "job_output", map[string]any{"job_id": id, "wait": true, "timeout_ms": 3000})
	if err != nil {
		t.Fatal(err)
	}
	if text := resultText(final); !strings.Contains(text, "[status: killed, signal: SIGTERM]") {
		t.Fatalf("killed read = %q", text)
	}
}

func TestJobOutputWaitTimeoutLeavesJobRunning(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner := createJobTestSession(t, e, "session-a")
	id := startBackgroundBash(t, e, owner, "sleep 60")

	started := time.Now()
	result, err := callBuiltin(t, e, owner, "job_output", map[string]any{"job_id": id, "wait": true, "timeout_ms": 25})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond || elapsed > time.Second {
		t.Fatalf("wait elapsed = %s", elapsed)
	}
	if text := resultText(result); !strings.Contains(text, "[status: running]") {
		t.Fatalf("timed out read = %q", text)
	}
	if _, err := callBuiltin(t, e, owner, "job_kill", map[string]any{"job_id": id}); err != nil {
		t.Fatal(err)
	}
}

func TestBackgroundBashReportsCommandOutcomes(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner := createJobTestSession(t, e, "session-a")

	nonzero := startBackgroundBash(t, e, owner, "printf problem >&2; exit 7")
	result, err := callBuiltin(t, e, owner, "job_output", map[string]any{"job_id": nonzero, "wait": true, "timeout_ms": 3000})
	if err != nil {
		t.Fatal(err)
	}
	text := resultText(result)
	if !strings.Contains(text, "[stderr]\nproblem") || !strings.Contains(text, "[status: completed, exit code: 7]") {
		t.Fatalf("nonzero read = %q", text)
	}

	selfSignal := startBackgroundBash(t, e, owner, "kill -TERM $$")
	result, err = callBuiltin(t, e, owner, "job_output", map[string]any{"job_id": selfSignal, "wait": true, "timeout_ms": 3000})
	if err != nil {
		t.Fatal(err)
	}
	if text := resultText(result); !strings.Contains(text, "[status: killed, signal: SIGTERM]") {
		t.Fatalf("self-signal read = %q", text)
	}
}

func TestForegroundBashTimeoutKillsProcessGroup(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", sandboxDangerFull)
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	owner := createJobTestSession(t, e, "session-timeout")
	leaked := filepath.Join(e.Config().Workspace, "leaked")
	result, err := callBuiltin(t, e, owner, "bash", map[string]any{
		"command":     `(sleep 0.3; printf leaked > ` + bashANSIQuote(leaked) + `) & wait`,
		"description": "verify timeout cleanup", "timeoutMs": 40,
	})
	if err != nil {
		t.Fatal(err)
	}
	if text := resultText(result); !strings.Contains(text, "[exit code: -1]") {
		t.Fatalf("timeout result = %q", text)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := os.Stat(leaked); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("timed-out process survived: %v", err)
	}
}

func TestJobToolSchemas(t *testing.T) {
	e, err := New(WithWorkspace(t.TempDir()), WithPersistence(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	schemas := map[string]ToolSchema{}
	for _, schema := range e.ListTools() {
		schemas[schema.Name] = schema
	}
	properties := schemas["bash"].Parameters["properties"].(map[string]any)
	if _, ok := properties["run_in_background"]; !ok {
		t.Fatal("bash schema is missing run_in_background")
	}
	for _, name := range []string{"job_output", "job_list", "job_kill"} {
		if _, ok := schemas[name]; !ok {
			t.Fatalf("missing %s schema", name)
		}
	}
}

func TestManagedJobCloseForceFailsThrowingCancel(t *testing.T) {
	registry := newJobRegistry()
	producerDone := make(chan managedJobResult)
	cancelled := make(chan string, 1)
	id, err := registry.startManaged("", " ", "\t", 0, func() (*managedJobHandle, error) {
		return &managedJobHandle{
			Done: producerDone,
			Cancel: func(reason string) error {
				cancelled <- reason
				return errors.New("cancel boom")
			},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() {
		registry.close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("registry close waited for a producer whose cancel threw")
	}
	select {
	case reason := <-cancelled:
		if reason != "background jobs are closed" {
			t.Fatalf("cancel reason = %q", reason)
		}
	default:
		t.Fatal("managed cancel was not called")
	}
	rows := registry.list("")
	if len(rows) != 1 || rows[0].ID != id || rows[0].Kind != " " || rows[0].Label != "\t" || rows[0].Status != jobFailed || !rows[0].Reported || !strings.Contains(rows[0].Detail, "cancel threw during teardown") {
		t.Fatalf("forced teardown snapshot = %#v", rows)
	}
	close(producerDone)
}

func TestJobWaitDurationClampsOverflow(t *testing.T) {
	if got := jobWaitDuration(1e300); got != time.Duration(1<<63-1) {
		t.Fatalf("overflow wait duration = %s", got)
	}
	if got := jobWaitDuration(1.5); got != 1500*time.Microsecond {
		t.Fatalf("fractional wait duration = %s", got)
	}
}
