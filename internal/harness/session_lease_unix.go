//go:build !windows

package harness

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func acquireSessionWriteLease(directory, id string) (io.Closer, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(directory, "session.lock")
	for attempt := 0; attempt < 3; attempt++ {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, err
		}
		if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			_ = file.Close()
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				return nil, &SessionAlreadyOwnedError{SessionID: id}
			}
			return nil, err
		}
		held, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		current, err := os.Stat(path)
		if err == nil && os.SameFile(held, current) {
			return file, nil
		}
		_ = file.Close()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return nil, &SessionAlreadyOwnedError{SessionID: id}
}
