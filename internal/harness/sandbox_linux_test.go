//go:build linux

package harness

import (
	"slices"
	"testing"
)

func TestBwrapProfileIsolatesPIDNamespace(t *testing.T) {
	args := bwrapProfileArgs(sandboxWorkspaceWrite, "/workspace")
	pid := slices.Index(args, "--unshare-pid")
	proc := slices.Index(args, "--proc")
	if pid < 0 || proc < 0 || pid > proc {
		t.Fatalf("bwrap profile = %#v", args)
	}
	if !slices.Contains(args, "--bind") || !slices.Contains(args, "/workspace") {
		t.Fatalf("workspace-write profile = %#v", args)
	}
}
