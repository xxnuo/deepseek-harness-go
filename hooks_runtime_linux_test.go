//go:build linux

package harness

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestHookRuntimeCloseAbortsAndReapsDetachedProcess(t *testing.T) {
	dir := t.TempDir()
	pidPath, startedPath := filepath.Join(dir, "pid"), filepath.Join(dir, "started")
	slow := writeHookRuntimeScript(t, dir, "slow.sh", "echo $$ > "+strconv.Quote(pidPath)+"\ntouch "+strconv.Quote(startedPath)+"\nsleep 30\n")
	engine, _, _ := newHookRuntimeEngine(t, HookDialectCodex, map[string]any{
		"SessionStart": []any{map[string]any{"hooks": []any{map[string]any{"command": slow}}}},
	})
	if _, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "", ""); err != nil {
		t.Fatal(err)
	}
	var process linuxProcessIdentity
	waitHookRuntime(t, func() bool {
		if _, err := os.Stat(startedPath); err != nil {
			return false
		}
		data, err := os.ReadFile(pidPath)
		if err != nil {
			return false
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			return false
		}
		process, _ = readLinuxProcess(pid)
		return process.pid != 0
	})
	started := time.Now()
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("Close() took %s", time.Since(started))
	}
	requireLinuxProcessGone(t, process)
}
