//go:build !windows

package harness

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

const (
	shellToolName        = "bash"
	shellToolDescription = "Run a bash command under the current sandbox policy. Set run_in_background for long-running commands, then use job_output or job_kill with the returned job id."
	shellToolPersistent  = true
)

func shellInvocation(command, mode, workspace, workdir string) (string, []string, error) {
	if mode == sandboxDangerFull {
		return "bash", []string{"-c", command}, nil
	}
	return sandboxInvocation(mode, workspace, workdir, "bash", []string{"-c", command})
}

func persistentShellInvocation(mode, workspace string) (string, []string, error) {
	if mode == sandboxDangerFull {
		return "bash", []string{"--noprofile", "--norc"}, nil
	}
	return sandboxInvocation(mode, workspace, workspace, "bash", []string{"--noprofile", "--norc"})
}

func persistentShellWrapper(command, start, end string) string {
	return fmt.Sprintf("printf '%%s\\n' %s; eval -- %s; __dsh_status=$?; printf '%%s%%s\\n' %s \"$__dsh_status\"\n", bashANSIQuote(start), bashANSIQuote(command), bashANSIQuote(end))
}

func prepareShellChild(*exec.Cmd, string, string) (shellChildState, error) { return nil, nil }

func shellEnvironment() map[string]string {
	return map[string]string{"LC_ALL": "C", "LANG": "C", "NO_COLOR": "1", "TERM": "dumb", "PAGER": "cat", "GIT_PAGER": "cat"}
}

func configureChildProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if cmd.Cancel != nil {
		cmd.Cancel = func() error { return killChildProcess(cmd) }
	}
}

func terminateChildProcess(cmd *exec.Cmd) error {
	return signalProcessGroup(cmd, syscall.SIGTERM)
}

func killChildProcess(cmd *exec.Cmd) error {
	return signalProcessGroup(cmd, syscall.SIGKILL)
}

func killChildProcessPID(pid int) error {
	return normalizeProcessError(syscall.Kill(-pid, syscall.SIGKILL))
}

func signalProcessGroup(cmd *exec.Cmd, signal syscall.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return os.ErrProcessDone
	}
	return normalizeProcessError(syscall.Kill(-cmd.Process.Pid, signal))
}

func normalizeProcessError(err error) error {
	if errors.Is(err, syscall.ESRCH) || errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
