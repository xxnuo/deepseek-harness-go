package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestSkillBadgeProfileEnablementMatchesBundle(t *testing.T) {
	home := t.TempDir()
	t.Setenv("DSH_HOME", home)
	loader, err := newProfileLoader(true)
	if err != nil {
		t.Fatal(err)
	}
	composed, err := loader.compose("headless", nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if engineConfig(loader, composed).BundledBadgeSkill {
		t.Fatal("disabled base skill-badge was enabled")
	}
	patch := filepath.Join(home, "enable-badge.yml")
	if err := os.WriteFile(patch, []byte("- id: skill-badge\n  disabled: false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	composed, err = loader.compose("headless", []string{patch}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !engineConfig(loader, composed).BundledBadgeSkill {
		t.Fatal("enabled skill-badge was not wired into the Go engine")
	}
}
