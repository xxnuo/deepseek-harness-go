//go:build linux

package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type linuxSandboxRunner struct {
	program string
	prefix  []string
	kind    string
	err     error
}

var linuxSandboxRunnerCache sync.Map

func sandboxInvocation(mode, workspace, workdir, program string, args []string) (string, []string, error) {
	if mode != sandboxReadOnly && mode != sandboxWorkspaceWrite {
		return "", nil, fmt.Errorf("unsupported sandbox mode %q", mode)
	}
	runner := selectLinuxSandboxRunner()
	if runner.err != nil {
		return "", nil, runner.err
	}
	switch runner.kind {
	case "bwrap":
		profile := bwrapProfileArgs(mode, workspace)
		profile = append(profile, "--chdir", workdir, "--", program)
		return runner.program, append(profile, args...), nil
	case "landlock":
		profile := append([]string(nil), runner.prefix...)
		profile = append(profile, "--ro", "/", "--rw", "/dev/null")
		if mode == sandboxWorkspaceWrite {
			profile = append(profile, "--rw", "/tmp", "--rw", workspace)
		}
		profile = append(profile, "--", program)
		return runner.program, append(profile, args...), nil
	default:
		return "", nil, errors.New("SANDBOX_UNAVAILABLE: no Linux sandbox runner is available")
	}
}

func bwrapProfileArgs(mode, workspace string) []string {
	profile := []string{"--ro-bind", "/", "/", "--dev", "/dev", "--unshare-pid", "--proc", "/proc", "--die-with-parent"}
	if mode == sandboxWorkspaceWrite {
		profile = append(profile, "--tmpfs", "/tmp", "--bind", workspace, workspace)
	}
	return profile
}

func selectLinuxSandboxRunner() linuxSandboxRunner {
	key := os.Getenv("PATH") + "\x00" + os.Getenv("DSH_LANDLOCK_RUNNER") + "\x00" + os.Getenv("DSH_GO_LANDLOCK_SELF")
	if cached, ok := linuxSandboxRunnerCache.Load(key); ok {
		return cached.(linuxSandboxRunner)
	}
	selected := probeLinuxSandboxRunner()
	actual, _ := linuxSandboxRunnerCache.LoadOrStore(key, selected)
	return actual.(linuxSandboxRunner)
}

func probeLinuxSandboxRunner() linuxSandboxRunner {
	if path, err := exec.LookPath("bwrap"); err == nil && probeSandboxCommand(path, append(bwrapProfileArgs(sandboxReadOnly, "/"), "--", "true")) {
		return linuxSandboxRunner{program: path, kind: "bwrap"}
	}
	for _, candidate := range landlockRunnerCandidates() {
		if probeSandboxCommand(candidate.program, append(append([]string(nil), candidate.prefix...), "--probe")) {
			candidate.kind = "landlock"
			return candidate
		}
	}
	return linuxSandboxRunner{err: errors.New("SANDBOX_UNAVAILABLE: neither bubblewrap nor Landlock is usable on this host")}
}

func landlockRunnerCandidates() []linuxSandboxRunner {
	var candidates []linuxSandboxRunner
	seen := map[string]bool{}
	add := func(program string, prefix ...string) {
		if program == "" {
			return
		}
		key := program + "\x00" + strings.Join(prefix, "\x00")
		if !seen[key] {
			seen[key] = true
			candidates = append(candidates, linuxSandboxRunner{program: program, prefix: append([]string(nil), prefix...)})
		}
	}
	add(strings.TrimSpace(os.Getenv("DSH_LANDLOCK_RUNNER")))
	if path, err := exec.LookPath("dsh-landlock-run"); err == nil {
		add(path)
	}
	if executable := strings.TrimSpace(os.Getenv("DSH_GO_LANDLOCK_SELF")); executable != "" {
		current, err := os.Executable()
		if err == nil {
			executable, _ = filepath.Abs(executable)
			current, _ = filepath.Abs(current)
			if executable == current {
				add(executable, "__landlock-run")
			}
		}
	}
	return candidates
}

func probeSandboxCommand(program string, args []string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, program, args...)
	command.Stdout, command.Stderr = nil, nil
	return command.Run() == nil
}
