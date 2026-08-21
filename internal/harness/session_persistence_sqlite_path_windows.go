//go:build windows

package harness

import "os"

func validateSessionSQLiteParentAccess(string, os.FileInfo) error { return nil }

func validateSessionSQLiteFileAccess(string, os.FileInfo) error { return nil }
