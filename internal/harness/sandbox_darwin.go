//go:build darwin

package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func sandboxInvocation(mode, workspace, _ string, program string, args []string) (string, []string, error) {
	if mode != sandboxReadOnly && mode != sandboxWorkspaceWrite {
		return "", nil, fmt.Errorf("unsupported sandbox mode %q", mode)
	}
	forms := []string{"(version 1)", "(allow default)", "(deny file-write*)", `(allow file-write* (literal "/dev/null"))`}
	if mode == sandboxWorkspaceWrite {
		roots := []string{workspace, "/tmp", os.TempDir()}
		seen := map[string]bool{}
		var grants []string
		for _, root := range roots {
			if resolved, err := filepath.EvalSymlinks(root); err == nil {
				root = resolved
			}
			root, _ = filepath.Abs(root)
			if root != "" && !seen[root] {
				seen[root] = true
				grants = append(grants, "(subpath "+seatbeltString(root)+")")
			}
		}
		if len(grants) > 0 {
			forms = append(forms, "(allow file-write* "+strings.Join(grants, " ")+")")
		}
	}
	profile := []string{"-p", strings.Join(forms, " "), "--", program}
	return "sandbox-exec", append(profile, args...), nil
}

func seatbeltString(value string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value) + `"`
}
