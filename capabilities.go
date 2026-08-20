package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// The upstream deployment discovers these two registries from filesystem
// roots. Keeping discovery filesystem-based makes a Go embedding behave like
// the shipped CLI without introducing another configuration format.
type presetRecord struct {
	id          string
	trust       string
	dir         string
	name        string
	description string
	content     string
	broken      string
}

func (e *Engine) presetRoots() [][2]string {
	roots := make([][2]string, 0, 3)
	add := func(path, trust string) {
		if path == "" {
			return
		}
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
		for _, root := range roots {
			if root[0] == path {
				return
			}
		}
		if st, err := os.Stat(path); err == nil && st.IsDir() {
			roots = append(roots, [2]string{path, trust})
		}
	}
	add(e.cfg.PresetDir, "system")
	if home, err := os.UserHomeDir(); err == nil {
		add(filepath.Join(home, ".dsh", ".agent-presets"), "user")
		add(filepath.Join(home, ".deepseek-harness", ".agent-presets"), "user")
	}
	add(filepath.Join(e.cfg.DataDir, ".agent-presets"), "user")
	// Locate the fixed checkout when the library is used from this repository.
	if _, file, _, ok := runtime.Caller(0); ok {
		root := filepath.Dir(file)
		add(filepath.Join(root, "deepseek-harness", "apps", "cli", "config", "agent-presets"), "system")
	}
	return roots
}

func parsePresetMetadata(dir string) (name, description string, err error) {
	data, readErr := os.ReadFile(filepath.Join(dir, "preset.yml"))
	if errors.Is(readErr, os.ErrNotExist) {
		return "", "", nil
	}
	if readErr != nil {
		return "", "", readErr
	}
	var doc struct {
		Name        string `yaml:"name"`
		Description string `yaml:"description"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return "", "", err
	}
	return strings.TrimSpace(doc.Name), strings.TrimSpace(doc.Description), nil
}

func scanPresets(e *Engine) []presetRecord {
	seen := map[string]bool{}
	rows := []presetRecord{}
	for _, root := range e.presetRoots() {
		entries, err := os.ReadDir(root[0])
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() || !validPresetID(entry.Name()) || seen[entry.Name()] {
				continue
			}
			dir := filepath.Join(root[0], entry.Name())
			composition := filepath.Join(dir, "agent.cordis.yml")
			data, readErr := os.ReadFile(composition)
			if errors.Is(readErr, os.ErrNotExist) {
				continue
			}
			row := presetRecord{id: entry.Name(), trust: root[1], dir: dir}
			if readErr != nil {
				row.broken = readErr.Error()
			} else {
				row.content = string(data)
				if strings.TrimSpace(row.content) == "" {
					row.broken = "composition is empty"
				}
			}
			row.name, row.description, _ = parsePresetMetadata(dir)
			rows = append(rows, row)
			seen[row.id] = true
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	return rows
}

func validPresetID(id string) bool {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, `/\\`) {
		return false
	}
	for _, r := range id {
		if !(r == '-' || r == '_' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func (e *Engine) presetRows() []map[string]any {
	rows := scanPresets(e)
	out := make([]map[string]any, 0, len(rows))
	defaultID := ""
	e.mu.RLock()
	if value, ok := e.settings["agent-presets"]; ok {
		defaultID, _ = value["default"].(string)
	}
	e.mu.RUnlock()
	if defaultID == "" && len(rows) > 0 {
		for _, row := range rows {
			if row.id == "standard" {
				defaultID = row.id
				break
			}
		}
		if defaultID == "" {
			defaultID = rows[0].id
		}
	}
	for _, row := range rows {
		item := map[string]any{"id": row.id, "trust": row.trust, "isDefault": row.id == defaultID}
		if row.name != "" {
			item["name"] = row.name
		}
		if row.description != "" {
			item["description"] = row.description
		}
		if row.broken != "" {
			item["broken"] = row.broken
		}
		out = append(out, item)
	}
	return out
}

func (e *Engine) findPreset(id string) (presetRecord, bool) {
	for _, row := range scanPresets(e) {
		if row.id == id {
			return row, true
		}
	}
	return presetRecord{}, false
}

func (e *Engine) presetEntries() []map[string]any { return e.presetRows() }

func (e *Engine) resolvePreset(id string) (string, *RPCError) {
	rows := scanPresets(e)
	if len(rows) == 0 {
		if id != "" {
			return "", rpcError("agent-preset-not-found", "preset roster is empty", map[string]any{"agentPreset": id})
		}
		return "", nil
	}
	if id == "" {
		for _, row := range e.presetRows() {
			if isDefault, _ := row["isDefault"].(bool); isDefault {
				id, _ = row["id"].(string)
				break
			}
		}
	}
	row, ok := e.findPreset(id)
	if !ok {
		return "", rpcError("agent-preset-not-found", "preset not found", map[string]any{"agentPreset": id})
	}
	if row.broken != "" {
		return "", rpcError("agent-preset-invalid", row.broken, map[string]any{"agentPreset": id})
	}
	return id, nil
}

func (e *Engine) readPresetContent(id string) (any, *RPCError) {
	row, ok := e.findPreset(id)
	if !ok {
		return nil, rpcError("agent-preset-not-found", "preset not found", map[string]any{"agentPreset": id})
	}
	if row.broken != "" {
		return nil, rpcError("agent-preset-invalid", row.broken, map[string]any{"agentPreset": id})
	}
	out := map[string]any{"agentPreset": id, "trust": row.trust, "content": row.content}
	if row.name != "" {
		out["name"] = row.name
	}
	if row.description != "" {
		out["description"] = row.description
	}
	return out, nil
}

func (e *Engine) copyPreset(from, id, name string) (map[string]any, *RPCError) {
	if !validPresetID(id) || id == "standard" || id == "minimal" {
		return nil, rpcError("agent-preset-invalid", "invalid or reserved preset id", map[string]any{"agentPreset": id})
	}
	source, ok := e.findPreset(from)
	if !ok {
		return nil, rpcError("agent-preset-not-found", "source preset not found", map[string]any{"agentPreset": from})
	}
	if source.broken != "" {
		return nil, rpcError("agent-preset-invalid", source.broken, map[string]any{"agentPreset": from})
	}
	root := filepath.Join(e.cfg.DataDir, ".agent-presets")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, rpcError("agent-preset-invalid", err.Error(), nil)
	}
	dir := filepath.Join(root, id)
	if _, err := os.Stat(dir); err == nil {
		return nil, rpcError("agent-preset-invalid", "preset already exists", map[string]any{"agentPreset": id})
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, rpcError("agent-preset-invalid", err.Error(), nil)
	}
	if err := os.WriteFile(filepath.Join(dir, "agent.cordis.yml"), []byte(source.content), 0o600); err != nil {
		_ = os.RemoveAll(dir)
		return nil, rpcError("agent-preset-invalid", err.Error(), nil)
	}
	if name = strings.TrimSpace(name); name != "" || source.name != "" || source.description != "" {
		meta := fmt.Sprintf("name: %s\ndescription: %s\n", yamlQuote(name), yamlQuote(source.description))
		if name == "" {
			meta = fmt.Sprintf("description: %s\n", yamlQuote(source.description))
		}
		if err := os.WriteFile(filepath.Join(dir, "preset.yml"), []byte(meta), 0o600); err != nil {
			_ = os.RemoveAll(dir)
			return nil, rpcError("agent-preset-invalid", err.Error(), nil)
		}
	}
	return map[string]any{"agentPreset": id}, nil
}

func yamlQuote(value string) string {
	data, _ := yaml.Marshal(value)
	return strings.TrimSpace(string(data))
}

func (e *Engine) removePreset(id string) *RPCError {
	row, ok := e.findPreset(id)
	if !ok {
		return rpcError("agent-preset-not-found", "preset not found", map[string]any{"agentPreset": id})
	}
	if row.trust != "user" {
		return rpcError("agent-preset-read-only", "shipped presets are read-only", map[string]any{"agentPreset": id})
	}
	if err := os.RemoveAll(row.dir); err != nil {
		return rpcError("agent-preset-invalid", err.Error(), map[string]any{"agentPreset": id})
	}
	return nil
}

type skillFrontmatter struct {
	Name                   string `yaml:"name"`
	Description            string `yaml:"description"`
	WhenToUse              string `yaml:"whenToUse"`
	WhenToUseAlt           string `yaml:"when-to-use"`
	DisableModelInvocation *bool  `yaml:"disable-model-invocation"`
	ModelInvocable         *bool  `yaml:"model-invocable"`
	UserInvocable          *bool  `yaml:"user-invocable"`
}

type skillRecord struct {
	name, description, whenToUse string
	path, content                string
	provider                     string
	modelInvocable               bool
}

func validSkillName(name string) bool {
	if name == "" || name[0] == '-' || name[len(name)-1] == '-' {
		return false
	}
	previousDash := false
	for _, r := range name {
		if r == '-' {
			if previousDash {
				return false
			}
			previousDash = true
			continue
		}
		previousDash = false
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func parseSkill(path string) (skillRecord, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return skillRecord{}, false
	}
	text := string(data)
	if !strings.HasPrefix(strings.TrimSpace(text), "---") {
		return skillRecord{}, false
	}
	trimmed := strings.TrimSpace(text)
	parts := strings.SplitN(trimmed, "---", 3)
	if len(parts) < 3 {
		return skillRecord{}, false
	}
	var front skillFrontmatter
	if yaml.Unmarshal([]byte(parts[1]), &front) != nil {
		return skillRecord{}, false
	}
	name := strings.TrimSpace(front.Name)
	if !validSkillName(name) {
		return skillRecord{}, false
	}
	description := strings.TrimSpace(front.Description)
	if description == "" {
		return skillRecord{}, false
	}
	when := strings.TrimSpace(front.WhenToUse)
	if when == "" {
		when = strings.TrimSpace(front.WhenToUseAlt)
	}
	model := true
	if front.DisableModelInvocation != nil {
		model = !*front.DisableModelInvocation
	} else if front.ModelInvocable != nil {
		model = *front.ModelInvocable
	}
	return skillRecord{
		name: name, description: description, whenToUse: when,
		path: path, content: strings.TrimSpace(parts[2]), provider: "filesystem", modelInvocable: model,
	}, true
}

const bundledBadgeSkillDescription = "Add the official \u201cpowered by dsh\u201d badge to documents, pull requests, merge requests, and other content produced with DeepSeek Harness. Use whenever creating a pull request or merge request. Also use when the user asks for a dsh badge, powered-by-dsh attribution, or a reusable dsh badge asset or snippet."

func (e *Engine) bundledBadgeSkill() (skillRecord, bool) {
	for _, root := range e.frontendPluginRoots() {
		for _, path := range []string{
			filepath.Join(root, "skill", "skill-badge", "assets", "dsh-badge.md"),
			filepath.Join(root, "@deepseek-ai", "dsh-skill-badge", "assets", "dsh-badge.md"),
		} {
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			return skillRecord{
				name: "dsh-badge", description: bundledBadgeSkillDescription,
				path: path, content: strings.TrimSpace(string(data)), provider: "dsh-badge", modelInvocable: true,
			}, true
		}
	}
	return skillRecord{}, false
}

func (e *Engine) skillRoots(cwd, preset string) []string {
	roots := []string{}
	add := func(path string) {
		if path == "" {
			return
		}
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
		for _, existing := range roots {
			if existing == path {
				return
			}
		}
		if st, err := os.Stat(path); err == nil && st.IsDir() {
			roots = append(roots, path)
		}
	}
	add(e.cfg.SkillDir)
	add(filepath.Join(e.cfg.DataDir, "skills"))
	add(filepath.Join(e.cfg.AgentsHome, "skills"))
	if root := findInstructionRoot(cwd); root != "" {
		add(filepath.Join(root, ".dsh", "skills"))
		add(filepath.Join(root, ".agents", "skills"))
	}
	if preset != "" {
		if row, ok := e.findPreset(preset); ok {
			add(filepath.Join(row.dir, "skills"))
		}
	}
	return roots
}

func (e *Engine) skillRecordsForSession(id string) ([]skillRecord, *RPCError) {
	s, err := e.getSession(id)
	if err != nil {
		return nil, errorToRPC(err)
	}
	s.mu.Lock()
	cwd, preset := s.Header.CWD, sessionAgentPreset(s.Header, s.Events)
	s.mu.Unlock()
	seen := map[string]bool{}
	rows := []skillRecord{}
	for _, root := range e.skillRoots(cwd, preset) {
		entries, readErr := os.ReadDir(root)
		if readErr != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			row, ok := parseSkill(filepath.Join(root, entry.Name(), "SKILL.md"))
			if !ok || seen[row.name] {
				continue
			}
			seen[row.name] = true
			rows = append(rows, row)
		}
	}
	if e.cfg.BundledBadgeSkill && !seen["dsh-badge"] {
		if row, ok := e.bundledBadgeSkill(); ok {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	return rows, nil
}

func (e *Engine) skillsForSession(id string) ([]map[string]any, *RPCError) {
	records, err := e.skillRecordsForSession(id)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(records))
	for _, record := range records {
		row := map[string]any{
			"name": record.name, "description": record.description,
			"modelInvocable": record.modelInvocable,
		}
		if record.whenToUse != "" {
			row["whenToUse"] = record.whenToUse
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *Engine) createSubagent(ctx context.Context, parentID, id, preset string) (string, error) {
	parent, err := e.getSession(parentID)
	if err != nil {
		return "", err
	}
	parent.mu.Lock()
	cwd := parent.Header.CWD
	depth := parent.Header.DelegationDepth
	model := parent.Model
	parent.mu.Unlock()
	child, err := e.createSession(ctx, SessionHeader{
		ID: id, CWD: cwd, ParentSession: parentID, Origin: "subagent",
		DelegationDepth: depth + 1, AgentPreset: preset, Mode: "continuable",
	}, shouldPinPermissionSnapshot(preset))
	if err != nil {
		return "", err
	}
	s, _ := e.getSession(child)
	s.mu.Lock()
	s.Model = model
	s.mu.Unlock()
	e.mu.Lock()
	_ = e.saveStateLocked()
	e.mu.Unlock()
	return child, nil
}
