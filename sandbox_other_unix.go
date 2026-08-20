//go:build !windows && !linux && !darwin

package harness

import "errors"

func sandboxInvocation(_, _, _, _ string, _ []string) (string, []string, error) {
	return "", nil, errors.New("SANDBOX_UNAVAILABLE: this Unix platform has no supported sandbox runner")
}
