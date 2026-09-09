//go:build windows

package harness

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

var sessionCreateSemaphore = windows.NewLazySystemDLL("kernel32.dll").NewProc("CreateSemaphoreW")
var sessionReleaseSemaphore = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReleaseSemaphore")

type sessionWindowsLease struct{ handle windows.Handle }

func acquireSessionWriteLease(directory, id string) (io.Closer, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	path, err := filepath.Abs(filepath.Join(directory, "session.lock"))
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(strings.ToLower(path)))
	name, err := windows.UTF16PtrFromString(fmt.Sprintf("Local\\dsh-session-lock-%x", digest))
	if err != nil {
		return nil, err
	}
	handle, _, callErr := sessionCreateSemaphore.Call(0, 1, 1, uintptr(unsafe.Pointer(name)))
	if handle == 0 {
		return nil, callErr
	}
	status, waitErr := windows.WaitForSingleObject(windows.Handle(handle), 0)
	if waitErr == nil && status == windows.WAIT_OBJECT_0 {
		return &sessionWindowsLease{handle: windows.Handle(handle)}, nil
	}
	_ = windows.CloseHandle(windows.Handle(handle))
	if status == uint32(windows.WAIT_TIMEOUT) {
		return nil, &SessionAlreadyOwnedError{SessionID: id}
	}
	if waitErr != nil {
		return nil, waitErr
	}
	return nil, fmt.Errorf("session write lease wait returned %d", status)
}

func (lease *sessionWindowsLease) Close() error {
	ok, _, releaseErr := sessionReleaseSemaphore.Call(uintptr(lease.handle), 1, 0)
	closeErr := windows.CloseHandle(lease.handle)
	if ok == 0 {
		return errors.Join(releaseErr, closeErr)
	}
	return closeErr
}
