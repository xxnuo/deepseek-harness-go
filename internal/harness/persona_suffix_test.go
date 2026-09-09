package harness

import (
	"strings"
	"testing"
)

func TestPersonaSuffixFollowsReusableInstructions(t *testing.T) {
	engine := newIntegrationEngine(t)
	id, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "persona-suffix", "")
	if err != nil {
		t.Fatal(err)
	}
	session := mustSession(t, engine, id)
	config := defaultAgentRuntime(engine.Config())
	config.persona = "Reusable prefix for {{model}}."
	config.personaSuffix = "Environment {{cwd}}."
	sections, err := engine.resolvedSystemPromptSections(session, ModelSelection{Model: "test-model"}, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sections) < 3 || sections[1].Name != "deployment:persona-prefix" || sections[1].Text != "Reusable prefix for test-model." {
		t.Fatalf("prefix sections = %#v", sections)
	}
	last := sections[len(sections)-1]
	if last.Name != "deployment:persona-suffix" || !strings.Contains(last.Text, engine.Config().Workspace) {
		t.Fatalf("last section = %#v", last)
	}
	config.completePersona = true
	sections, err = engine.resolvedSystemPromptSections(session, ModelSelection{Model: "test-model"}, config, nil)
	if err != nil || len(sections) != 1 || sections[0].Name != "deployment:persona-prefix" {
		t.Fatalf("complete persona = %#v, %v", sections, err)
	}
}
