//go:build !windows

package harness

import "golang.org/x/sys/unix"

func validateSubagentCWDSearch(cwd string) error {
	return unix.Access(cwd, unix.X_OK)
}
