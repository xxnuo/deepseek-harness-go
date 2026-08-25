package harness

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfigUsesSupportedEnvironmentSwitches(t *testing.T) {
	home := t.TempDir()
	agents := t.TempDir()
	t.Setenv("DSH_HOME", home)
	t.Setenv("DSH_AGENTS_HOME", agents)
	t.Setenv("DSH_WEB_SEARCH_PROVIDER", "exa")
	t.Setenv("DSH_WEB_FETCH_PROVIDER", "custom-fetch")
	t.Setenv("DSH_TOOLS_MODE", "both")
	cfg := DefaultConfig()
	if cfg.DataDir != home || cfg.AgentsHome != agents || cfg.WebSearchProvider != "exa" || cfg.WebFetchProvider != "custom-fetch" || cfg.ToolPresentation != "both" {
		t.Fatalf("environment config = %#v", cfg)
	}
	if cfg.Version != Version() || cfg.Version != "0.1.1-rc.2" {
		t.Fatalf("version = %q", cfg.Version)
	}
}

func TestToolPresentationControlsModelVisibleCatalog(t *testing.T) {
	for _, test := range []struct {
		mode        string
		wantRunCode bool
		wantBash    bool
	}{
		{mode: "native", wantBash: true},
		{mode: "code", wantRunCode: true},
		{mode: "both", wantRunCode: true, wantBash: true},
	} {
		t.Run(test.mode, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.DataDir = t.TempDir()
			cfg.Workspace = cfg.DataDir
			cfg.ToolPresentation = test.mode
			cfg.Persist = false
			cfg.SessionTitleLLM.Enabled = false
			e, err := New(WithConfig(cfg))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			id, err := e.CreateSession(t.Context(), cfg.Workspace, "", "")
			if err != nil {
				t.Fatal(err)
			}
			s, _ := e.getSession(id)
			tools, err := e.toolsForSession(s)
			if err != nil {
				t.Fatal(err)
			}
			visible := map[string]bool{}
			for _, tool := range tools {
				visible[tool.Name] = true
			}
			if visible["run_code"] != test.wantRunCode || visible["bash"] != test.wantBash {
				t.Fatalf("%s tools: run_code=%v bash=%v", test.mode, visible["run_code"], visible["bash"])
			}
		})
	}

	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.ToolPresentation = "invalid"
	cfg.Persist = false
	if _, err := New(WithConfig(cfg)); err == nil {
		t.Fatal("invalid tool presentation was accepted")
	}
}

func TestSkillRootsUseDshAgentsAndProjectHomes(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	cwd := filepath.Join(root, "nested")
	dshHome := t.TempDir()
	agentsHome := t.TempDir()
	for _, dir := range []string{
		filepath.Join(cwd),
		filepath.Join(root, ".dsh", "skills"),
		filepath.Join(root, ".agents", "skills"),
		filepath.Join(dshHome, "skills"),
		filepath.Join(agentsHome, "skills"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	e := &Engine{cfg: Config{DataDir: dshHome, AgentsHome: agentsHome}}
	want := map[string]bool{
		filepath.Join(dshHome, "skills"):         true,
		filepath.Join(agentsHome, "skills"):      true,
		filepath.Join(root, ".dsh", "skills"):    true,
		filepath.Join(root, ".agents", "skills"): true,
	}
	for _, path := range e.skillRoots(cwd, "") {
		delete(want, path)
	}
	if len(want) != 0 {
		t.Fatalf("missing skill roots: %#v", want)
	}
}
