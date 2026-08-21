//go:build !windows

package harness

import (
	"fmt"
	"os"
	"syscall"
)

func validateSessionSQLiteParentAccess(path string, info os.FileInfo) error {
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(owner.Uid) != uint64(os.Getuid()) || info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("session database parent %q must be owned by the current user and not group/world-writable", path)
	}
	return nil
}

func validateSessionSQLiteFileAccess(path string, info os.FileInfo) error {
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(owner.Uid) != uint64(os.Getuid()) || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("session database %q must be owned by the current user and accessible only by that user", path)
	}
	return nil
}
