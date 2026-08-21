//go:build windows

package harness

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

type windowsConPTY struct {
	input   *os.File
	output  *os.File
	console windows.Handle
	process windows.Handle
	job     windows.Handle
	sandbox *windowsSandbox
	pid     int

	writeMu sync.Mutex
	mu      sync.Mutex
	done    chan struct{}
	code    int
	err     error
	closed  bool
}

func startWindowsConPTY(program string, args []string, cwd string, env []string, rows, cols int, sandbox *windowsSandbox) (*windowsConPTY, error) {
	if rows < 1 || rows > 32767 || cols < 1 || cols > 32767 {
		return nil, errors.New("ConPTY rows and cols must be between 1 and 32767")
	}
	resolved, err := exec.LookPath(program)
	if err != nil {
		return nil, err
	}
	var inputRead, inputWrite, outputRead, outputWrite windows.Handle
	closeHandles := func() {
		for _, handle := range []windows.Handle{inputRead, inputWrite, outputRead, outputWrite} {
			if handle != 0 {
				_ = windows.CloseHandle(handle)
			}
		}
	}
	if err := windows.CreatePipe(&inputRead, &inputWrite, nil, 0); err != nil {
		return nil, err
	}
	if err := windows.CreatePipe(&outputRead, &outputWrite, nil, 0); err != nil {
		closeHandles()
		return nil, err
	}
	var console windows.Handle
	if err := windows.CreatePseudoConsole(windows.Coord{X: int16(cols), Y: int16(rows)}, inputRead, outputWrite, 0, &console); err != nil {
		closeHandles()
		return nil, err
	}
	_ = windows.CloseHandle(inputRead)
	_ = windows.CloseHandle(outputWrite)
	inputRead, outputWrite = 0, 0

	attributes, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		windows.ClosePseudoConsole(console)
		closeHandles()
		return nil, err
	}
	defer attributes.Delete()
	if err := attributes.Update(windows.PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, unsafe.Pointer(&console), unsafe.Sizeof(console)); err != nil {
		windows.ClosePseudoConsole(console)
		closeHandles()
		return nil, err
	}
	startup := windows.StartupInfoEx{ProcThreadAttributeList: attributes.List()}
	startup.Cb = uint32(unsafe.Sizeof(startup))
	commandLine, err := windows.UTF16FromString(windows.ComposeCommandLine(append([]string{resolved}, args...)))
	if err != nil {
		windows.ClosePseudoConsole(console)
		closeHandles()
		return nil, err
	}
	directory, err := windows.UTF16PtrFromString(cwd)
	if err != nil {
		windows.ClosePseudoConsole(console)
		closeHandles()
		return nil, err
	}
	environment, err := windowsEnvironmentBlock(env)
	if err != nil {
		windows.ClosePseudoConsole(console)
		closeHandles()
		return nil, err
	}
	job, err := newWindowsKillOnCloseJob()
	if err != nil {
		windows.ClosePseudoConsole(console)
		closeHandles()
		return nil, err
	}
	processInfo := windows.ProcessInformation{}
	flags := uint32(windows.EXTENDED_STARTUPINFO_PRESENT | windows.CREATE_UNICODE_ENVIRONMENT | windows.CREATE_SUSPENDED)
	if sandbox != nil && sandbox.token != 0 {
		err = windows.CreateProcessAsUser(
			sandbox.token, nil, &commandLine[0], nil, nil, false, flags,
			&environment[0], directory, &startup.StartupInfo, &processInfo,
		)
	} else {
		err = windows.CreateProcess(
			nil, &commandLine[0], nil, nil, false, flags,
			&environment[0], directory, &startup.StartupInfo, &processInfo,
		)
	}
	if err != nil {
		_ = windows.CloseHandle(job)
		windows.ClosePseudoConsole(console)
		closeHandles()
		return nil, err
	}
	cleanupProcess := func() {
		_ = windows.TerminateProcess(processInfo.Process, 1)
		_ = windows.CloseHandle(processInfo.Thread)
		_ = windows.CloseHandle(processInfo.Process)
		_ = windows.CloseHandle(job)
		windows.ClosePseudoConsole(console)
		closeHandles()
	}
	if err := windows.AssignProcessToJobObject(job, processInfo.Process); err != nil {
		cleanupProcess()
		return nil, err
	}
	if resumed, resumeErr := windows.ResumeThread(processInfo.Thread); resumed == ^uint32(0) {
		cleanupProcess()
		if resumeErr == nil {
			resumeErr = windows.GetLastError()
		}
		return nil, resumeErr
	}
	_ = windows.CloseHandle(processInfo.Thread)
	processInfo.Thread = 0

	pty := &windowsConPTY{
		input: os.NewFile(uintptr(inputWrite), "conpty-input"), output: os.NewFile(uintptr(outputRead), "conpty-output"),
		console: console, process: processInfo.Process, job: job, sandbox: sandbox,
		pid: int(processInfo.ProcessId), done: make(chan struct{}), code: -1,
	}
	inputWrite, outputRead = 0, 0
	go pty.wait()
	return pty, nil
}

func windowsEnvironmentBlock(env []string) ([]uint16, error) {
	env = append([]string(nil), env...)
	sort.SliceStable(env, func(left, right int) bool {
		return strings.ToUpper(env[left]) < strings.ToUpper(env[right])
	})
	block := make([]uint16, 0, len(env)*16)
	for _, entry := range env {
		if strings.IndexByte(entry, 0) >= 0 {
			return nil, errors.New("Windows environment contains a NUL byte")
		}
		block = append(block, utf16.Encode([]rune(entry))...)
		block = append(block, 0)
	}
	block = append(block, 0)
	if len(block) == 1 {
		block = append(block, 0)
	}
	return block, nil
}

func (pty *windowsConPTY) wait() {
	_, waitErr := windows.WaitForSingleObject(pty.process, windows.INFINITE)
	exitCode := uint32(1)
	if waitErr == nil {
		waitErr = windows.GetExitCodeProcess(pty.process, &exitCode)
	}
	if pty.input != nil {
		_ = pty.input.Close()
	}
	windows.ClosePseudoConsole(pty.console)
	pty.mu.Lock()
	processErr := windows.CloseHandle(pty.process)
	jobErr := windows.CloseHandle(pty.job)
	pty.process, pty.job = 0, 0
	sandboxErr := pty.sandbox.close()
	pty.code = int(exitCode)
	pty.err = errors.Join(waitErr, processErr, jobErr, sandboxErr)
	pty.closed = true
	close(pty.done)
	pty.mu.Unlock()
}

func (pty *windowsConPTY) result() (int, error) {
	<-pty.done
	pty.mu.Lock()
	defer pty.mu.Unlock()
	return pty.code, pty.err
}

func (pty *windowsConPTY) write(data []byte) error {
	pty.writeMu.Lock()
	defer pty.writeMu.Unlock()
	select {
	case <-pty.done:
		return errors.New("terminal process has exited")
	default:
	}
	_, err := pty.input.Write(data)
	return err
}

func (pty *windowsConPTY) terminate(exitCode uint32) error {
	terminateErr := pty.terminateJob(exitCode)
	_, waitErr := pty.result()
	return errors.Join(terminateErr, waitErr)
}

func (pty *windowsConPTY) terminateJob(exitCode uint32) error {
	pty.mu.Lock()
	defer pty.mu.Unlock()
	if pty.closed || pty.job == 0 {
		return nil
	}
	return windows.TerminateJobObject(pty.job, exitCode)
}

func (pty *windowsConPTY) signal(name string) (int, error) {
	switch name {
	case TerminalSignalInterrupt:
		return pty.pid, pty.write([]byte{3})
	case TerminalSignalTerminate:
		return pty.pid, pty.terminateJob(1)
	case TerminalSignalKill:
		return pty.pid, pty.terminateJob(137)
	case TerminalSignalStop, TerminalSignalHangup:
		return 0, fmt.Errorf("signal %s is unsupported on Windows; only SIGINT, SIGTERM, and SIGKILL are available", name)
	default:
		return 0, fmt.Errorf("unsupported terminal signal %q", name)
	}
}
