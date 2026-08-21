//go:build windows

package harness

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/windows"
)

const (
	shellToolName           = "pwsh"
	shellToolDescription    = "Execute a PowerShell command in a fresh process under the current sandbox policy. Set run_in_background for long-running commands, then use job_output or job_kill with the returned job id."
	shellCommandDescription = "The PowerShell command to run. Relative path is preferred in the command."
	shellToolPersistent     = true
)

const powershellEncodingPreamble = "[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false); $OutputEncoding = [System.Text.UTF8Encoding]::new($false); "

func shellInvocation(command, mode, _, _ string) (string, []string, error) {
	if mode != sandboxDangerFull && mode != sandboxWorkspaceWrite && mode != sandboxReadOnly {
		return "", nil, fmt.Errorf("unsupported sandbox mode %q", mode)
	}
	program, err := resolvePowerShell()
	if err != nil {
		return "", nil, err
	}
	return program, powershellArgs(command), nil
}

func persistentShellInvocation(mode, _ string) (string, []string, error) {
	if mode != sandboxDangerFull && mode != sandboxWorkspaceWrite && mode != sandboxReadOnly {
		return "", nil, fmt.Errorf("unsupported sandbox mode %q", mode)
	}
	program, err := resolvePowerShell()
	if err != nil {
		return "", nil, err
	}
	return program, []string{"-NoLogo", "-NoProfile", "-NonInteractive", "-NoExit", "-Command", "-"}, nil
}

func persistentShellWrapper(command, start, end string) string {
	return fmt.Sprintf("$__dsh_command=[ScriptBlock]::Create(%s); [Console]::Out.WriteLine(%s); $global:LASTEXITCODE=0; try { . $__dsh_command; $__dsh_ok=$?; $__dsh_code=$global:LASTEXITCODE; if ($__dsh_ok) { $__dsh_code=0 } elseif ($__dsh_code -eq 0) { $__dsh_code=1 } } catch { [Console]::Error.WriteLine($_); $__dsh_code=1 }; [Console]::Out.WriteLine(%s + [string]$__dsh_code)\r\n", powershellQuote(command), powershellQuote(start), powershellQuote(end))
}

func powershellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func powershellArgs(command string) []string {
	return []string{"-NoLogo", "-NoProfile", "-NonInteractive", "-Command", powershellEncodingPreamble + command}
}

func resolvePowerShell() (string, error) {
	programFiles := windowsEnvironment("ProgramFiles", `C:\Program Files`)
	systemRoot := windowsEnvironment("SystemRoot", `C:\Windows`)
	for _, candidate := range []string{
		filepath.Join(programFiles, "PowerShell", "7", "pwsh.exe"),
		"pwsh.exe",
		filepath.Join(systemRoot, "System32", "WindowsPowerShell", "v1.0", "powershell.exe"),
		"powershell.exe",
	} {
		if filepath.IsAbs(candidate) {
			if info, err := os.Lstat(candidate); err == nil && !info.IsDir() {
				return candidate, nil
			}
			continue
		}
		if path, err := exec.LookPath(candidate); err == nil {
			return path, nil
		}
	}
	return "", errors.New("SHELL_UNAVAILABLE: neither pwsh nor Windows PowerShell is installed")
}

func windowsEnvironment(name, fallback string) string {
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok && strings.EqualFold(key, name) && value != "" {
			return strings.Trim(value, `"`)
		}
	}
	return fallback
}

func shellEnvironment() map[string]string {
	return map[string]string{"NO_COLOR": "1", "PAGER": "cat", "GIT_PAGER": "cat"}
}

func configureChildProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
	if cmd.Cancel != nil {
		cmd.Cancel = func() error { return killChildProcess(cmd) }
	}
}

type windowsShellChildState struct {
	sandbox *windowsSandbox
	job     windows.Handle
	once    sync.Once
	err     error
}

func prepareShellChild(cmd *exec.Cmd, mode, workspace string) (shellChildState, error) {
	if mode == sandboxDangerFull {
		return nil, nil
	}
	sandbox, err := newWindowsSandbox(mode, workspace)
	if err != nil {
		return nil, err
	}
	job, err := newWindowsKillOnCloseJob()
	if err != nil {
		_ = sandbox.close()
		return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: CreateJobObject: %w", err)
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Token = syscall.Token(sandbox.token)
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_SUSPENDED | windows.CREATE_NEW_PROCESS_GROUP
	cmd.Env = sandbox.environment(cmd.Env)
	return &windowsShellChildState{sandbox: sandbox, job: job}, nil
}

func (state *windowsShellChildState) started(cmd *exec.Cmd) error {
	if err := assignWindowsProcessToJob(cmd.Process, state.job); err != nil {
		return fmt.Errorf("AssignProcessToJobObject: %w", err)
	}
	if err := resumeWindowsProcess(cmd.Process.Pid); err != nil {
		_ = windows.TerminateJobObject(state.job, 1)
		return fmt.Errorf("ResumeThread: %w", err)
	}
	return nil
}

func (state *windowsShellChildState) close() error {
	state.once.Do(func() {
		state.err = errors.Join(windows.CloseHandle(state.job), state.sandbox.close())
	})
	return state.err
}

func terminateChildProcess(cmd *exec.Cmd) error {
	return killChildProcess(cmd)
}

func killChildProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return os.ErrProcessDone
	}
	return killChildProcessPID(cmd.Process.Pid)
}

func killChildProcessPID(pid int) error {
	taskkill := "taskkill.exe"
	if root := os.Getenv("SystemRoot"); root != "" {
		taskkill = filepath.Join(root, "System32", taskkill)
	}
	command := exec.Command(taskkill, "/PID", strconv.Itoa(pid), "/T", "/F")
	command.Stdout, command.Stderr = nil, nil
	if err := command.Run(); err == nil {
		return nil
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	err = process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
