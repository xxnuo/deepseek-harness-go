//go:build windows

package harness

import (
	"os/exec"
	"syscall"
)

func configureDetachedOpenInAppProcess(command *exec.Cmd, windowsHide bool) {
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP, HideWindow: windowsHide}
}
