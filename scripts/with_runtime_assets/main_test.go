package main

import (
	"os"
	"slices"
	"testing"
)

func TestTargetGoEnvironment(t *testing.T) {
	t.Setenv("DSH_TARGET_GOOS", "windows")
	t.Setenv("DSH_TARGET_GOARCH", "arm64")

	got := targetGoEnvironment([]string{"PATH=/bin", "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"})
	for _, want := range []string{"PATH=/bin", "CGO_ENABLED=0", "GOOS=windows", "GOARCH=arm64"} {
		if !slices.Contains(got, want) {
			t.Fatalf("target environment %q does not contain %q", got, want)
		}
	}
	if slices.Contains(got, "GOOS=linux") || slices.Contains(got, "GOARCH=amd64") {
		t.Fatalf("target environment retained host target: %q", got)
	}
}

func TestTargetGoEnvironmentUnchangedWithoutTarget(t *testing.T) {
	_ = os.Unsetenv("DSH_TARGET_GOOS")
	_ = os.Unsetenv("DSH_TARGET_GOARCH")
	environment := []string{"GOOS=linux", "GOARCH=amd64"}
	if got := targetGoEnvironment(environment); !slices.Equal(got, environment) {
		t.Fatalf("target environment = %q, want %q", got, environment)
	}
}
