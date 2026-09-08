package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestScrubbedChildEnvDropsAmbientCredentialsAndAllowsExplicitValues(t *testing.T) {
	t.Setenv("DSH_ENV_TEST", "stale")
	t.Setenv("HARNESS_TEST_API_KEY", "secret")
	t.Setenv("HARNESS_TEST_PASSWORD", "secret")
	t.Setenv("HARNESS_TEST_PLAIN", "kept")

	env := strings.Join(scrubbedChildEnv(map[string]string{
		"DSH_EXPLICIT_TEST":   "trusted",
		"EXPLICIT_TEST_TOKEN": "explicit",
	}), "\n")
	for _, absent := range []string{"DSH_ENV_TEST=", "HARNESS_TEST_API_KEY=", "HARNESS_TEST_PASSWORD="} {
		if strings.Contains(env, absent) {
			t.Fatalf("child environment leaked %q:\n%s", absent, env)
		}
	}
	for _, present := range []string{"HARNESS_TEST_PLAIN=kept", "DSH_EXPLICIT_TEST=trusted", "EXPLICIT_TEST_TOKEN=explicit"} {
		if !strings.Contains(env, present) {
			t.Fatalf("child environment missed %q:\n%s", present, env)
		}
	}
}

func TestBuiltinShellRejectsEmptyDescription(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	args, _ := json.Marshal(map[string]any{"command": "true", "description": "   "})
	_, err = e.tools["bash"].Execute(context.Background(), ToolCall{Name: "bash", Workspace: cfg.Workspace, Arguments: args})
	if err == nil || !strings.Contains(err.Error(), "invalid description") {
		t.Fatalf("empty description error = %v", err)
	}
}

func TestShellEnvironmentForSessionUsesTrustedIdentity(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id, err := e.CreateSession(context.Background(), cfg.Workspace, "env", "standard")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("DSH_SESSION_ID", "ambient-stale")
	env := e.shellEnvironmentForSession(id)
	if env["DSH_HOME"] != cfg.DataDir || env["DSH_SHELL"] != "1" || env["DSH_SESSION_ID"] != id {
		t.Fatalf("trusted shell environment = %#v", env)
	}
	clean := strings.Join(scrubbedChildEnv(env), "\n")
	if strings.Contains(clean, "DSH_SESSION_ID=ambient-stale") {
		t.Fatal("ambient session identity leaked")
	}
}
