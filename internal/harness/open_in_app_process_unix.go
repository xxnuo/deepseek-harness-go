//go:build !windows

package harness

import (
	"os/exec"
	"syscall"
)

func configureDetachedOpenInAppProcess(command *exec.Cmd, _ bool) {
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
