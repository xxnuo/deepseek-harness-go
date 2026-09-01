package main

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLoadLayeredEnvMatchesUpstreamPrecedenceAndRestores(t *testing.T) {
	home := t.TempDir()
	project := t.TempDir()
	t.Setenv("DSH_HOME", home)
	for _, name := range []string{"APP_BOOT_GO_SHARED", "APP_BOOT_GO_PROJECT", "APP_BOOT_GO_USER"} {
		_ = os.Unsetenv(name)
		name := name
		t.Cleanup(func() { _ = os.Unsetenv(name) })
	}
	if err := os.WriteFile(filepath.Join(home, ".env"), []byte("APP_BOOT_GO_SHARED=user\nAPP_BOOT_GO_USER=user-only\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(project, ".env"), []byte("APP_BOOT_GO_SHARED=project\nAPP_BOOT_GO_PROJECT='project only'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(project); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })

	snapshot, restore, err := loadLayeredEnv("dsh", &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("APP_BOOT_GO_SHARED") != "project" || os.Getenv("APP_BOOT_GO_PROJECT") != "project only" || os.Getenv("APP_BOOT_GO_USER") != "user-only" {
		t.Fatalf("layered environment = shared:%q project:%q user:%q", os.Getenv("APP_BOOT_GO_SHARED"), os.Getenv("APP_BOOT_GO_PROJECT"), os.Getenv("APP_BOOT_GO_USER"))
	}
	if snapshot.Project["APP_BOOT_GO_SHARED"] != "project" || snapshot.User["APP_BOOT_GO_SHARED"] != "user" {
		t.Fatalf("launch environment snapshot = %#v", snapshot)
	}
	restore()
	if _, exists := os.LookupEnv("APP_BOOT_GO_SHARED"); exists {
		t.Fatal("restore left project environment materialized")
	}
}

func TestLoadLayeredEnvRejectsBootstrapBeforeApplying(t *testing.T) {
	home := t.TempDir()
	project := t.TempDir()
	t.Setenv("DSH_HOME", home)
	_ = os.Unsetenv("APP_BOOT_GO_WOULD_APPLY")
	t.Cleanup(func() { _ = os.Unsetenv("APP_BOOT_GO_WOULD_APPLY") })
	if err := os.WriteFile(filepath.Join(project, ".env"), []byte("APP_BOOT_GO_WOULD_APPLY=yes\nhttps_proxy=http://invalid.example\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	old, _ := os.Getwd()
	if err := os.Chdir(project); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	if _, _, err := loadLayeredEnv("dsh", &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "only the launching environment may set") {
		t.Fatalf("bootstrap rejection = %v", err)
	}
	if _, exists := os.LookupEnv("APP_BOOT_GO_WOULD_APPLY"); exists {
		t.Fatal("a rejected layer was partially applied")
	}
}

func TestVersionDoesNotLoadProjectEnv(t *testing.T) {
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, ".env"), []byte("PATH=/project-only-path\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	old, _ := os.Getwd()
	if err := os.Chdir(project); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	var stdout, stderr bytes.Buffer
	if err := runWithIO([]string{"--version"}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(stdout.String()) != "0.1.2-alpha.1" || stderr.Len() != 0 {
		t.Fatalf("version output = %q, stderr = %q", stdout.String(), stderr.String())
	}
}

func TestProfileJSEnvironmentIncludesProcessPlatform(t *testing.T) {
	value, err := evaluateProfileJS("process.platform")
	if err != nil {
		t.Fatal(err)
	}
	if value != runtime.GOOS {
		t.Fatalf("process.platform = %#v, want %q", value, runtime.GOOS)
	}
}
