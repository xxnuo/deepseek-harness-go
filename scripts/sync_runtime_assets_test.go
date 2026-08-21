package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestVerifyOfficialClientBuildUsesUpstreamValidator(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}
	root := t.TempDir()
	bin := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "pnpm.log")
	helper := filepath.Join(bin, "pnpm")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\nprintf 'cwd=%s\\n' \"$PWD\" > \"$VERIFY_LOG\"\nprintf 'arg=%s\\n' \"$@\" >> \"$VERIFY_LOG\"\nif [ -n \"$VERIFY_FAILURE\" ]; then echo \"$VERIFY_FAILURE\" >&2; exit 1; fi\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VERIFY_LOG", logPath)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	if err := verifyOfficialClientBuild(root); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	log := string(data)
	for _, expected := range []string{
		"cwd=" + root,
		"arg=exec",
		"arg=tsx",
		"officialClientBuildEnvironment",
		"readClientBuildRecord",
	} {
		if !strings.Contains(log, expected) {
			t.Fatalf("validator invocation missing %q:\n%s", expected, log)
		}
	}

	t.Setenv("VERIFY_FAILURE", "stale client artifacts")
	if err := verifyOfficialClientBuild(root); err == nil || !strings.Contains(err.Error(), "stale client artifacts") {
		t.Fatalf("validator failure was not preserved: %v", err)
	}
}

func TestCollectRuntimeAssetsIncludesPythonProtocolMirror(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{
		"apps/web/dist/index.html",
		"apps/cli/config/agent-presets/standard/agent.cordis.yml",
		"packages/code-runtime/code-runtime-python/py/protocol.py",
		"packages/skill/skill-badge/assets/badge.txt",
		"packages/subagent/subagent-codex/cordis.patch.yml",
	} {
		absolute := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, []byte(path), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	assets, err := collectRuntimeAssets(root)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		filepath.FromSlash("packages/code-runtime/code-runtime-python/py/protocol.py"): true,
		filepath.FromSlash("packages/subagent/subagent-codex/cordis.patch.yml"):        true,
	}
	for _, asset := range assets {
		delete(want, asset.relative)
	}
	if len(want) > 0 {
		missing := make([]string, 0, len(want))
		for path := range want {
			missing = append(missing, path)
		}
		t.Fatalf("runtime assets omit %s", strings.Join(missing, ", "))
	}
}
