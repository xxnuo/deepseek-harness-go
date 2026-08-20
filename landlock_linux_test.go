//go:build linux

package harness

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestLandlockFallbackEnforcesWorkspaceBoundary(t *testing.T) {
	helper := filepath.Join(t.TempDir(), "dsh-landlock-run")
	build := exec.Command("go", "build", "-o", helper, "./cmd/dsh-landlock-run")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build landlock helper: %v\n%s", err, output)
	}
	probe := exec.Command(helper, "--probe")
	if output, err := probe.CombinedOutput(); err != nil {
		t.Skipf("Landlock is unavailable on this kernel: %v: %s", err, output)
	}

	workspace, err := os.MkdirTemp("/var/tmp", "dsh-landlock-workspace-")
	if err != nil {
		t.Skipf("cannot create /var/tmp workspace: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workspace) })
	outside := filepath.Join("/var/tmp", "dsh-landlock-outside-"+strings.TrimPrefix(newID("test"), "test-"))
	t.Cleanup(func() { _ = os.Remove(outside) })

	bin := t.TempDir()
	for name, source := range map[string]string{"bash": "bash", "true": "true"} {
		path, err := exec.LookPath(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(path, filepath.Join(bin, name)); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin)
	t.Setenv("DSH_LANDLOCK_RUNNER", helper)
	program, args, err := shellInvocation(
		`printf allowed > "$1"; printf denied > "$2"`,
		sandboxWorkspaceWrite,
		workspace,
		workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if program != helper {
		t.Fatalf("selected sandbox runner = %q, want %q", program, helper)
	}
	args = append(args, "bash", filepath.Join(workspace, "allowed"), outside)
	command := exec.Command(program, args...)
	command.Dir = workspace
	command.Env = append(os.Environ(), "LC_ALL=C", "LANG=C")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	err = command.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() == 0 {
		t.Fatalf("outside write result = %v, stderr=%q", err, stderr.String())
	}
	if data, err := os.ReadFile(filepath.Join(workspace, "allowed")); err != nil || string(data) != "allowed" {
		t.Fatalf("workspace write = %q, %v", data, err)
	}
	if _, err := os.Stat(outside); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside path escaped Landlock: %v", err)
	}
	if !strings.Contains(strings.ToLower(stderr.String()), "permission denied") {
		t.Fatalf("Landlock denial stderr = %q", stderr.String())
	}
}

func TestLandlockReadOnlyDeniesWorkspaceWrite(t *testing.T) {
	helper := filepath.Join(t.TempDir(), "dsh-landlock-run")
	build := exec.Command("go", "build", "-o", helper, "./cmd/dsh-landlock-run")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build landlock helper: %v\n%s", err, output)
	}
	if output, err := exec.Command(helper, "--probe").CombinedOutput(); err != nil {
		t.Skipf("Landlock is unavailable on this kernel: %v: %s", err, output)
	}
	workspace := t.TempDir()
	target := filepath.Join(workspace, "blocked")
	command := exec.Command(helper, "--ro", "/", "--rw", "/dev/null", "--", "bash", "-c", `printf blocked > "$1"`, "bash", target)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	err := command.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() == 0 {
		t.Fatalf("read-only write result = %v, stderr=%q", err, stderr.String())
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only path was created: %v", err)
	}
}
