package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBundledBadgeSkillUsesUpstreamAssetsAndAllowsOverride(t *testing.T) {
	assets, err := defaultAssetPaths()
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.PluginDir = assets.PluginDir
	cfg.BundledBadgeSkill = true
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.Persist = false
	cfg.SessionProjectionCache = nil
	cfg.SessionTitleLLM.Enabled = false
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id, err := e.CreateSession(context.Background(), cfg.Workspace, "badge-skill", "")
	if err != nil {
		t.Fatal(err)
	}

	definition, err := e.LoadSkill(id, "dsh-badge")
	if err != nil {
		t.Fatal(err)
	}
	if definition.Provider != "dsh-badge" || !strings.Contains(definition.Content, "Preserve the badge's 121") {
		t.Fatalf("bundled definition = %#v", definition)
	}
	image, err := os.ReadFile(filepath.Join(definition.ResourceBase, "dsh-badge.png"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(image)
	if got := hex.EncodeToString(sum[:]); got != "f2c4f5ec9cbe847c0c763545c4d839efa8485bc74203733d0a0e8259f233c653" {
		t.Fatalf("badge sha256 = %s", got)
	}

	override := filepath.Join(t.TempDir(), "dsh-badge", "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(override), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(override, []byte("---\nname: dsh-badge\ndescription: Local override\n---\n\nUse the local badge.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.cfg.SkillDir = filepath.Dir(filepath.Dir(override))
	definition, err = e.LoadSkill(id, "dsh-badge")
	if err != nil {
		t.Fatal(err)
	}
	if definition.Provider != "filesystem" || definition.Content != "Use the local badge." {
		t.Fatalf("override definition = %#v", definition)
	}
}
