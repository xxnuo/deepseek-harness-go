package harness

import (
	_ "embed"
	"strings"
)

//go:embed upstream.lock
var upstreamLock string

// Version returns the DeepSeek Harness version pinned by upstream.lock.
func Version() string {
	for line := range strings.SplitSeq(upstreamLock, "\n") {
		if value, ok := strings.CutPrefix(line, "tag=dsh-v"); ok {
			return strings.TrimSpace(value)
		}
	}
	return "0.0.0"
}
