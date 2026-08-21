//go:build windows

package harness

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	windowsSandboxGrantMask    = (windows.FILE_GENERIC_WRITE | windows.DELETE | 0x40) &^ windows.READ_CONTROL
	windowsDisableMaxPrivilege = 0x1
	windowsLuaToken            = 0x4
	windowsWriteRestricted     = 0x8
	windowsFileAllAccess       = 0x1f01ff
)

var createRestrictedTokenProc = windows.NewLazySystemDLL("advapi32.dll").NewProc("CreateRestrictedToken")

type windowsSandbox struct {
	token   windows.Token
	tempDir string
	tempSID *windows.SID
	once    sync.Once
	err     error
}

func newWindowsSandbox(mode, workspace string) (*windowsSandbox, error) {
	if mode != sandboxReadOnly && mode != sandboxWorkspaceWrite {
		return nil, fmt.Errorf("unsupported sandbox mode %q", mode)
	}
	workspace, err := canonicalWindowsDirectory(workspace)
	if err != nil {
		return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: Windows ACL workspace: %w", err)
	}
	sandbox := &windowsSandbox{}
	var workspaceSID *windows.SID
	if mode == sandboxWorkspaceWrite {
		workspaceSID, err = windows.StringToSid(windowsCapabilitySID(workspace, false))
		if err != nil {
			return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: Windows workspace SID: %w", err)
		}
		if err := grantWindowsWrite(workspace, workspaceSID); err != nil {
			return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: Windows workspace ACL: %w", err)
		}
		tempRoot, rootErr := canonicalWindowsDirectory(os.TempDir())
		if rootErr != nil {
			return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: Windows temp root: %w", rootErr)
		}
		if windowsPathContains(workspace, tempRoot) {
			return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: Windows ACL temp root must be outside the workspace: workspace=%s; temp=%s", workspace, tempRoot)
		}
		sandbox.tempDir, err = os.MkdirTemp(tempRoot, "dsh-")
		if err != nil {
			return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: create private Windows temp: %w", err)
		}
		sandbox.tempSID, err = windows.StringToSid(windowsCapabilitySID(sandbox.tempDir, true))
		if err == nil {
			err = grantWindowsWrite(sandbox.tempDir, sandbox.tempSID)
		}
		if err != nil {
			_ = sandbox.close()
			return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: Windows temp ACL: %w", err)
		}
	}

	current, err := openWindowsProcessToken()
	if err != nil {
		_ = sandbox.close()
		return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: OpenProcessToken: %w", err)
	}
	defer current.Close()
	groups, err := current.GetTokenGroups()
	if err != nil {
		_ = sandbox.close()
		return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: TokenGroups: %w", err)
	}
	var logonSID *windows.SID
	for _, group := range groups.AllGroups() {
		if group.Sid != nil && group.Attributes&windows.SE_GROUP_LOGON_ID == windows.SE_GROUP_LOGON_ID {
			logonSID, err = group.Sid.Copy()
			break
		}
	}
	if err != nil || logonSID == nil {
		_ = sandbox.close()
		if err == nil {
			err = errors.New("current token has no logon SID")
		}
		return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: Windows logon SID: %w", err)
	}
	worldSID, err := windows.CreateWellKnownSid(windows.WinWorldSid)
	if err != nil {
		_ = sandbox.close()
		return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: Everyone SID: %w", err)
	}
	restricting := []*windows.SID{logonSID, worldSID}
	if mode == sandboxWorkspaceWrite {
		restricting = append(restricting, workspaceSID, sandbox.tempSID)
	}
	sandbox.token, err = createWindowsRestrictedToken(current, restricting)
	if err == nil {
		defaultSID := worldSID
		if sandbox.tempSID != nil {
			defaultSID = sandbox.tempSID
		} else if workspaceSID != nil {
			defaultSID = workspaceSID
		}
		err = grantWindowsTokenDefaultDACL(sandbox.token, defaultSID)
	}
	if err != nil {
		_ = sandbox.close()
		return nil, fmt.Errorf("SANDBOX_UNAVAILABLE: Windows restricted token: %w", err)
	}
	return sandbox, nil
}

func canonicalWindowsDirectory(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if resolved, resolveErr := filepath.EvalSymlinks(path); resolveErr == nil {
		path = resolved
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s is not a directory", path)
	}
	return filepath.Clean(path), nil
}

func windowsPathContains(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func windowsCapabilitySID(path string, temp bool) string {
	hash := sha256.New()
	if temp {
		_, _ = hash.Write([]byte("temp\x00"))
	}
	_, _ = hash.Write([]byte(path))
	digest := hash.Sum(nil)
	const limit = uint32(1<<30 - 1)
	first := binary.LittleEndian.Uint32(digest[:4])%limit + 1
	second := binary.LittleEndian.Uint32(digest[4:8])%limit + 1
	if temp {
		return fmt.Sprintf("S-1-4-%d-%d-1", first, second)
	}
	return fmt.Sprintf("S-1-4-%d-%d", first, second)
}

func openWindowsProcessToken() (windows.Token, error) {
	var token windows.Token
	err := windows.OpenProcessToken(
		windows.CurrentProcess(),
		windows.TOKEN_QUERY|windows.TOKEN_DUPLICATE|windows.TOKEN_ADJUST_DEFAULT|windows.TOKEN_ASSIGN_PRIMARY,
		&token,
	)
	return token, err
}

func createWindowsRestrictedToken(current windows.Token, sids []*windows.SID) (windows.Token, error) {
	if len(sids) == 0 {
		return 0, errors.New("restricting SID list is empty")
	}
	entries := make([]windows.SIDAndAttributes, len(sids))
	for index, sid := range sids {
		if sid == nil || !sid.IsValid() {
			return 0, fmt.Errorf("restricting SID %d is invalid", index)
		}
		entries[index].Sid = sid
	}
	var restricted windows.Token
	created, _, callErr := createRestrictedTokenProc.Call(
		uintptr(current),
		windowsDisableMaxPrivilege|windowsLuaToken|windowsWriteRestricted,
		0, 0,
		0, 0,
		uintptr(len(entries)), uintptr(unsafe.Pointer(&entries[0])),
		uintptr(unsafe.Pointer(&restricted)),
	)
	runtime.KeepAlive(entries)
	runtime.KeepAlive(sids)
	if created == 0 {
		if callErr == syscall.Errno(0) {
			callErr = windows.GetLastError()
		}
		return 0, callErr
	}
	if restricted == 0 {
		return 0, errors.New("CreateRestrictedToken returned a null token")
	}
	return restricted, nil
}

func windowsExplicitAccess(sid *windows.SID, mode windows.ACCESS_MODE, mask windows.ACCESS_MASK) (windows.EXPLICIT_ACCESS, *runtime.Pinner) {
	pinner := &runtime.Pinner{}
	pinner.Pin(sid)
	return windows.EXPLICIT_ACCESS{
		AccessPermissions: mask,
		AccessMode:        mode,
		Inheritance:       windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT,
		Trustee: windows.TRUSTEE{
			TrusteeForm:  windows.TRUSTEE_IS_SID,
			TrusteeType:  windows.TRUSTEE_IS_UNKNOWN,
			TrusteeValue: windows.TrusteeValueFromSID(sid),
		},
	}, pinner
}

func grantWindowsWrite(path string, sid *windows.SID) error {
	return withWindowsACLPathLock(path, func() error {
		descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			return err
		}
		var oldACL *windows.ACL
		if descriptor != nil {
			oldACL, _, err = descriptor.DACL()
			if errors.Is(err, windows.ERROR_OBJECT_NOT_FOUND) {
				err = nil
			}
			if err != nil {
				return err
			}
		}
		if oldACL != nil && windowsACLHasGrant(oldACL, sid) {
			return nil
		}
		entry, pinner := windowsExplicitAccess(sid, windows.GRANT_ACCESS, windows.ACCESS_MASK(windowsSandboxGrantMask))
		defer pinner.Unpin()
		acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{entry}, oldACL)
		runtime.KeepAlive(descriptor)
		if err != nil {
			return err
		}
		err = windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION, nil, nil, acl, nil)
		runtime.KeepAlive(acl)
		return err
	})
}

func revokeWindowsWrite(path string, sid *windows.SID) error {
	return withWindowsACLPathLock(path, func() error {
		descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			return err
		}
		if descriptor == nil {
			return nil
		}
		oldACL, _, err := descriptor.DACL()
		if errors.Is(err, windows.ERROR_OBJECT_NOT_FOUND) {
			return nil
		}
		if err != nil {
			return err
		}
		if oldACL == nil {
			return nil
		}
		entry, pinner := windowsExplicitAccess(sid, windows.REVOKE_ACCESS, 0)
		defer pinner.Unpin()
		acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{entry}, oldACL)
		runtime.KeepAlive(descriptor)
		if err != nil {
			return err
		}
		err = windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION, nil, nil, acl, nil)
		runtime.KeepAlive(acl)
		return err
	})
}

func windowsACLHasGrant(acl *windows.ACL, sid *windows.SID) bool {
	for index := uint32(0); index < uint32(acl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if windows.GetAce(acl, index, &ace) != nil || ace == nil {
			return false
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE ||
			ace.Header.AceFlags != windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT ||
			ace.Mask != windows.ACCESS_MASK(windowsSandboxGrantMask) {
			continue
		}
		aceSID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if aceSID.IsValid() && aceSID.Equals(sid) {
			return true
		}
	}
	return false
}

func withWindowsACLPathLock(path string, action func() error) (err error) {
	digest := sha256.Sum256([]byte(strings.ToLower(filepath.Clean(path))))
	lockDir := filepath.Join(os.TempDir(), "dsh-acl-locks")
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		return err
	}
	name, err := windows.UTF16PtrFromString(filepath.Join(lockDir, fmt.Sprintf("%x.lock", digest[:8])))
	if err != nil {
		return err
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.OPEN_ALWAYS, 0, 0)
	if err != nil {
		return err
	}
	overlapped := &windows.Overlapped{}
	if err := windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, overlapped); err != nil {
		_ = windows.CloseHandle(handle)
		return err
	}
	actionErr := action()
	unlockErr := windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
	closeErr := windows.CloseHandle(handle)
	if actionErr != nil {
		return actionErr
	}
	return errors.Join(unlockErr, closeErr)
}

func grantWindowsTokenDefaultDACL(token windows.Token, sid *windows.SID) error {
	var size uint32
	err := windows.GetTokenInformation(token, windows.TokenDefaultDacl, nil, 0, &size)
	if err != nil && !errors.Is(err, windows.ERROR_INSUFFICIENT_BUFFER) {
		return err
	}
	if size < uint32(unsafe.Sizeof(uintptr(0))) {
		return fmt.Errorf("implausible token default DACL size %d", size)
	}
	buffer := make([]byte, size)
	if err := windows.GetTokenInformation(token, windows.TokenDefaultDacl, &buffer[0], size, &size); err != nil {
		return err
	}
	current := *(**windows.ACL)(unsafe.Pointer(&buffer[0]))
	if current == nil {
		return errors.New("restricted token has no default DACL")
	}
	entry, pinner := windowsExplicitAccess(sid, windows.GRANT_ACCESS, windows.ACCESS_MASK(windowsFileAllAccess))
	defer pinner.Unpin()
	acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{entry}, current)
	runtime.KeepAlive(buffer)
	if err != nil {
		return err
	}
	info := struct{ DACL *windows.ACL }{DACL: acl}
	err = windows.SetTokenInformation(token, windows.TokenDefaultDacl, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
	runtime.KeepAlive(acl)
	return err
}

func (sandbox *windowsSandbox) environment(env []string) []string {
	if sandbox == nil || sandbox.tempDir == "" {
		return env
	}
	return replaceWindowsEnvironment(env, map[string]string{"TMP": sandbox.tempDir, "TEMP": sandbox.tempDir})
}

func replaceWindowsEnvironment(env []string, values map[string]string) []string {
	result := make([]string, 0, len(env)+len(values))
	for _, entry := range env {
		name, _, ok := strings.Cut(entry, "=")
		if !ok {
			result = append(result, entry)
			continue
		}
		drop := false
		for key := range values {
			if strings.EqualFold(name, key) {
				drop = true
				break
			}
		}
		if !drop {
			result = append(result, entry)
		}
	}
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	return result
}

func (sandbox *windowsSandbox) close() error {
	if sandbox == nil {
		return nil
	}
	sandbox.once.Do(func() {
		var failures []error
		if sandbox.token != 0 {
			failures = append(failures, sandbox.token.Close())
			sandbox.token = 0
		}
		if sandbox.tempDir != "" && sandbox.tempSID != nil {
			failures = append(failures, revokeWindowsWrite(sandbox.tempDir, sandbox.tempSID))
		}
		if sandbox.tempDir != "" {
			failures = append(failures, os.RemoveAll(sandbox.tempDir))
		}
		sandbox.err = errors.Join(failures...)
	})
	return sandbox.err
}

func newWindowsKillOnCloseJob() (windows.Handle, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, err
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	result, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	)
	if result == 0 {
		_ = windows.CloseHandle(job)
		if err == nil {
			err = windows.GetLastError()
		}
		return 0, err
	}
	return job, nil
}

func assignWindowsProcessToJob(process *os.Process, job windows.Handle) error {
	if process == nil {
		return os.ErrProcessDone
	}
	var assignErr error
	err := process.WithHandle(func(handle uintptr) {
		assignErr = windows.AssignProcessToJobObject(job, windows.Handle(handle))
	})
	return errors.Join(err, assignErr)
}

func resumeWindowsProcess(pid int) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snapshot)
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID != uint32(pid) {
			continue
		}
		thread, openErr := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
		if openErr != nil {
			return openErr
		}
		resumed, resumeErr := windows.ResumeThread(thread)
		closeErr := windows.CloseHandle(thread)
		if resumed == ^uint32(0) && resumeErr == nil {
			resumeErr = windows.GetLastError()
		}
		return errors.Join(resumeErr, closeErr)
	}
	if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return fmt.Errorf("suspended process %d has no thread", pid)
	}
	return err
}
