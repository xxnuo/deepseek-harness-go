package harness

import (
	"strings"
	"testing"
)

func TestScrubbedChildEnvDropsAmbientCredentialsAndAllowsExplicitValues(t *testing.T) {
	t.Setenv("DSH_ENV_TEST", "stale")
	t.Setenv("HARNESS_TEST_API_KEY", "secret")
	t.Setenv("HARNESS_TEST_PASSWORD", "secret")
	t.Setenv("HARNESS_TEST_PLAIN", "kept")

	env := strings.Join(scrubbedChildEnv(map[string]string{
		"DSH_SESSION_JSONL":   "/tmp/session.jsonl",
		"EXPLICIT_TEST_TOKEN": "explicit",
	}), "\n")
	for _, absent := range []string{"DSH_ENV_TEST=", "HARNESS_TEST_API_KEY=", "HARNESS_TEST_PASSWORD="} {
		if strings.Contains(env, absent) {
			t.Fatalf("child environment leaked %q:\n%s", absent, env)
		}
	}
	for _, present := range []string{"HARNESS_TEST_PLAIN=kept", "DSH_SESSION_JSONL=/tmp/session.jsonl", "EXPLICIT_TEST_TOKEN=explicit"} {
		if !strings.Contains(env, present) {
			t.Fatalf("child environment missed %q:\n%s", present, env)
		}
	}
}
