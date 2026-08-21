//go:build !linux

package harness

import (
	"fmt"
	"io"
)

func RunLandlockLauncher(_ []string, _ io.Writer, stderr io.Writer) int {
	fmt.Fprintln(stderr, "landlock-run: Landlock is only available on Linux")
	return 125
}
