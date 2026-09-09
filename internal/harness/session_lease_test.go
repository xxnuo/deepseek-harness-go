package harness

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestSessionWriteLeaseExcludesIndependentStores(t *testing.T) {
	root := t.TempDir()
	first, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := NewJSONLSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	meta := testSessionHeader("leased")
	writer, err := first.Create(t.Context(), meta, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Dir(first.pathFor(meta))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("create left filesystem footprint: %v", err)
	}
	if err := writer.Append(t.Context(), testClosedTurn()); err != nil {
		t.Fatal(err)
	}
	var owned *SessionAlreadyOwnedError
	if contender, err := second.Open(t.Context(), meta.ID, SessionAccessWrite); !errors.As(err, &owned) {
		if contender != nil {
			_ = contender.Close()
		}
		t.Fatalf("second store write-open = %v", err)
	}
	reader, err := second.Open(t.Context(), meta.ID, SessionAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	if events, err := reader.Read(t.Context()); err != nil || len(events) != 2 {
		t.Fatalf("reader while leased = %#v, %v", events, err)
	}
	_ = reader.Close()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	successor, err := second.Open(t.Context(), meta.ID, SessionAccessWrite)
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Close()
	if err := successor.Append(t.Context(), []Event{{Type: "session/title", Seq: 2, Time: 3, Data: map[string]any{"title": "continued"}}}); err != nil {
		t.Fatal(err)
	}
}

func TestSessionMaterializationFailureNeverRemovesExistingLog(t *testing.T) {
	root := t.TempDir()
	first, _ := NewJSONLSessionStore(root)
	defer first.Close()
	second, _ := NewJSONLSessionStore(root)
	defer second.Close()
	meta := testSessionHeader("racing-create")
	owner, err := first.Create(t.Context(), meta, 0)
	if err != nil {
		t.Fatal(err)
	}
	contender, err := second.Create(t.Context(), meta, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Close()
	if err := owner.Append(t.Context(), testClosedTurn()); err != nil {
		t.Fatal(err)
	}
	path := first.pathFor(meta)
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := contender.Flush(t.Context()); err == nil {
		t.Fatal("contending create acquired live lease")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := contender.Flush(t.Context()); !errors.Is(err, os.ErrExist) {
		t.Fatalf("collision = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("existing log changed or disappeared: %v", err)
	}
}

func TestSessionLeaseSubprocess(t *testing.T) {
	root := os.Getenv("DSH_TEST_SESSION_LEASE")
	if root == "" {
		return
	}
	lease, err := acquireSessionWriteLease(root, "child")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	fmt.Println("lease-ready")
	_, _ = io.Copy(io.Discard, os.Stdin)
}

func TestSessionLeaseSurvivesUntilProcessDeath(t *testing.T) {
	root := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestSessionLeaseSubprocess$")
	command.Env = append(os.Environ(), "DSH_TEST_SESSION_LEASE="+root)
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(output).ReadString('\n'); ready <- line }()
	select {
	case line := <-ready:
		if line != "lease-ready\n" {
			t.Fatalf("child output = %q", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("child did not acquire lease")
	}
	lease, err := acquireSessionWriteLease(root, "parent")
	var owned *SessionAlreadyOwnedError
	if !errors.As(err, &owned) {
		if lease != nil {
			_ = lease.Close()
		}
		t.Fatalf("parent acquired child's lock: %v", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	lease, err = acquireSessionWriteLease(root, "successor")
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}
