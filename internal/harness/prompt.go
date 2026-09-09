package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dop251/goja"
	"gopkg.in/yaml.v3"
)

const (
	harnessIdentity             = "You are an AI agent powered by DeepSeek Harness."
	deliverableFilePrompt       = "When you successfully create or modify files, mention the primary outputs in your final response. To make those and any other changed-file references clickable in Web, format them as Markdown inline code using the exact file-tool path, or a basename when unique among the files changed in that turn."
	defaultCodingPersona        = "You are a coding agent powered by the {{model}} model."
	defaultInstructionMaxBytes  = 65536
	defaultInstructionSourceCap = 1 << 20
	subagentDelegationContext   = "You are a delegated subagent: your permission scope was fixed when you were started and cannot be widened from inside this session - operations that require approval are rejected automatically. When the task needs access beyond that scope, do not retry the denied operation; state the limitation in your reply so the delegating agent can handle it."
)

type agentRuntime struct {
	persona                          string
	personaSuffix                    string
	completePersona                  bool
	includeInstructions              bool
	instructionMaxBytes              int
	includeRuntimeContext            bool
	toolNames                        map[string]bool
	persistentBash                   bool
	persistentBashDesc               string
	persistentBashTimeout            time.Duration
	persistentBashMaxOutputChars     int
	editorDescription                string
	editorMaxOutputChars             int
	toolPresentation                 string
	webTools                         *WebToolConfig
	planSection                      string
	goalDefaultMaxRounds             int
	goalBlockThreshold               int
	goalRoundDriver                  bool
	teamTools                        bool
	legacySubagentControl            bool
	legacyListAgents                 bool
	customSkillDirs                  []string
	skillFilesystemEnabled           bool
	skillIncludeDefaultRoots         bool
	skillDshHome                     string
	skillAgentsHome                  string
	skillBundledSkillDir             string
	readLimit                        int
	readMaxLineLength                int
	readMaxBytes                     int
	readStreamMinSize                int
	globSampleOverCapResults         bool
	globMaxResults                   int
	grepMaxMatches                   int
	grepMaxLineBytes                 int
	searchTimeout                    time.Duration
	searchGrace                      time.Duration
	searchStderrMaxBytes             int
	rawOutputMaxBytes                int
	searchMetaMaxBytes               int
	bashEnableRunInBackground        bool
	skillCatalogDescriptionMaxLength int
	toolResultPruneThresholdChars    int
	toolResultPruneHeadChars         int
	toolResultPruneTailChars         int
	toolResultPrunerEnabled          bool
	ralphSubagentProvider            string
	ralphMaxRounds                   int
	ralphMaxHandoffChars             int
	ralphMaxResultChars              int
	workflowToolName                 string
	workflowMaxResultChars           int
	workflowProvider                 string
	workflowMaxConcurrentAgents      int
	workflowMaxAgents                int
	workflowMaxItems                 int
	workflowSyncTimeout              time.Duration
	workflowDisposeGrace             time.Duration
	compactionEnabled                bool
	compactionAuto                   bool
	compactionConfig                 compactionRuntimeConfig
	compactCommandEnabled            bool
}

type presetRuntimeGeneration struct {
	presetID        string
	path            string
	mtimeMs         int64
	size            int64
	runtime         agentRuntime
	pluginInventory []PluginInventoryEntry
}

func defaultAgentRuntime(config Config) agentRuntime {
	return agentRuntime{
		persona: config.Persona, personaSuffix: config.PersonaSuffix, includeInstructions: true,
		instructionMaxBytes: config.InstructionMaxBytes, includeRuntimeContext: true,
		toolPresentation: config.ToolPresentation, webTools: cloneWebToolConfig(config.WebTools), planSection: defaultPlanModeSection,
		goalDefaultMaxRounds: defaultMaxGoalRounds, goalBlockThreshold: defaultGoalBlockThreshold,
		goalRoundDriver: true, teamTools: config.AgentTeams != nil,
		readLimit: readToolLineLimit, readMaxLineLength: readToolMaxLineChars, readMaxBytes: readToolMaxBytes,
		readStreamMinSize: 10 * 1024 * 1024, globMaxResults: globToolMaxResults,
		grepMaxMatches: grepToolMaxMatches, grepMaxLineBytes: grepToolMaxLineBytes,
		searchTimeout:                    30 * time.Second,
		searchGrace:                      3 * time.Second,
		searchStderrMaxBytes:             64 * 1024,
		rawOutputMaxBytes:                20_000_000,
		searchMetaMaxBytes:               65_536,
		skillCatalogDescriptionMaxLength: 500,
		bashEnableRunInBackground:        true,
		persistentBashTimeout:            persistentShellTimeout,
		persistentBashMaxOutputChars:     editorOutputLimit,
		skillIncludeDefaultRoots:         true,
		skillFilesystemEnabled:           true,
		skillDshHome:                     config.DataDir,
		skillAgentsHome:                  config.AgentsHome,
		skillBundledSkillDir:             config.SkillDir,
		editorMaxOutputChars:             editorOutputLimit,
		toolResultPruneThresholdChars:    config.ToolResultPruner.ThresholdChars,
		toolResultPruneHeadChars:         config.ToolResultPruner.HeadChars,
		toolResultPruneTailChars:         config.ToolResultPruner.TailChars,
		toolResultPrunerEnabled:          true,
		ralphSubagentProvider:            "spawn",
		ralphMaxRounds:                   256,
		ralphMaxHandoffChars:             16_384,
		ralphMaxResultChars:              16_384,
		workflowToolName:                 "workflow",
		workflowMaxResultChars:           50_000,
		workflowProvider:                 "spawn",
		workflowMaxConcurrentAgents:      0,
		workflowMaxAgents:                1000,
		workflowMaxItems:                 4096,
		workflowSyncTimeout:              5 * time.Second,
		workflowDisposeGrace:             5 * time.Second,
		compactionEnabled:                !config.Compaction.Disabled,
		compactionAuto:                   !config.Compaction.AutoDisabled,
		compactionConfig:                 newCompactionRuntimeConfig(config.Compaction),
		compactCommandEnabled:            !config.Compaction.Disabled,
	}
}

func (e *Engine) runtimeForSession(s *Session) (agentRuntime, error) {
	s.mu.Lock()
	preset := sessionAgentPreset(s.Header, s.Events)
	personaOverride := s.personaOverride
	parentID := s.Header.ParentSession
	generation := s.presetRuntime
	s.mu.Unlock()
	if preset == "" {
		runtimeConfig := defaultAgentRuntime(e.cfg)
		if personaOverride != "" {
			runtimeConfig.persona = personaOverride
		}
		return runtimeConfig, nil
	}
	if generation == nil {
		var err error
		generation, err = e.presetRuntimeForSession(s, preset, parentID)
		if err != nil {
			return agentRuntime{}, err
		}
	}
	runtimeConfig := generation.runtime
	if personaOverride != "" {
		runtimeConfig.persona = personaOverride
		runtimeConfig.completePersona = false
	}
	return runtimeConfig, nil
}

func (e *Engine) presetRuntimeForSession(s *Session, preset, parentID string) (*presetRuntimeGeneration, error) {
	generation, err := e.presetRuntimeForCreation(preset, parentID, s)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.presetRuntime == nil {
		s.presetRuntime = generation
	} else {
		generation = s.presetRuntime
	}
	s.mu.Unlock()
	return generation, nil
}

func (e *Engine) presetRuntimeForCreation(preset, parentID string, child *Session) (*presetRuntimeGeneration, error) {
	if parentID != "" {
		if parent, err := e.getSession(parentID); err == nil && parent != child {
			parent.mu.Lock()
			parentPreset := sessionAgentPreset(parent.Header, parent.Events)
			parentGeneration := parent.presetRuntime
			parent.mu.Unlock()
			if parentPreset == preset {
				if parentGeneration == nil {
					if _, err := e.runtimeForSession(parent); err != nil {
						return nil, err
					}
					parent.mu.Lock()
					parentGeneration = parent.presetRuntime
					parent.mu.Unlock()
				}
				if parentGeneration != nil {
					return parentGeneration, nil
				}
			}
		}
	}
	return e.ensurePresetRuntime(preset)
}

func (e *Engine) ensurePresetRuntime(preset string) (*presetRuntimeGeneration, error) {
	e.presetRuntimeMu.Lock()
	defer e.presetRuntimeMu.Unlock()
	if e.presetRuntimes == nil {
		e.presetRuntimes = make(map[string]*presetRuntimeGeneration)
	}
	row, ok := e.findPreset(preset)
	if !ok {
		return nil, fmt.Errorf("agent-preset-not-found: %s", preset)
	}
	if row.broken != "" {
		return nil, fmt.Errorf("agent-preset-invalid: %s", row.broken)
	}
	path := filepath.Join(row.dir, "agent.cordis.yml")
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("agent-preset-invalid: composition file is unreadable: %w", err)
	}
	mtimeMs, size := info.ModTime().UnixMilli(), info.Size()
	if current := e.presetRuntimes[preset]; current != nil && current.mtimeMs == mtimeMs && current.size == size {
		return current, nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("agent-preset-invalid: %w", err)
	}
	row.content = string(content)
	runtimeConfig, err := e.compilePresetRuntime(row)
	if err != nil {
		return nil, err
	}
	pluginInventory, err := compilePresetPluginInventory(row.content, filepath.Dir(path), e.deepSeekPluginBarePackageBase())
	if err != nil {
		return nil, err
	}
	generation := &presetRuntimeGeneration{
		presetID: preset, path: path, mtimeMs: mtimeMs, size: size,
		runtime: runtimeConfig, pluginInventory: pluginInventory,
	}
	e.presetRuntimes[preset] = generation
	return generation, nil
}

func compilePresetPluginInventory(content, moduleBase, barePackageBase string) ([]PluginInventoryEntry, error) {
	rows, err := compilePresetCompositionRows(content, true, true)
	if err != nil {
		return nil, err
	}
	entries := make([]PluginInventoryEntry, 0, len(rows))
	for _, row := range rows {
		enabled, _ := row.enabled.(bool)
		entries = append(entries, PluginInventoryEntry{
			EntryID: row.entryID, ModuleName: row.moduleName, ModuleBase: moduleBase,
			BarePackageBase: barePackageBase, Enabled: enabled, FiberPhase: row.fiberPhase,
			condition: row.condition, conditional: row.enabled == "conditional",
		})
	}
	return entries, nil
}

// presetCompositionRow is the wire-neutral form shared by mounted and file
// inventory projections. Empty entryID becomes JSON null at the boundary.
type presetCompositionRow struct {
	entryID    string
	moduleName string
	enabled    any
	condition  string
	fiberPhase *string
}

// compilePresetCompositionRows parses the Loader entry-list dialect once.
// prefixIDs matches mounted Loader ids; file projections pass false because a
// cold composition reports the ids declared by the file itself.
func compilePresetCompositionRows(content string, prefixIDs, live bool) ([]presetCompositionRow, error) {
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(content), &document); err != nil {
		return nil, fmt.Errorf("agent-preset-invalid: %w", err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.SequenceNode {
		return nil, errors.New("agent-preset-invalid: composition must be a YAML array")
	}
	entries := make([]presetCompositionRow, 0)
	var walk func(*yaml.Node, int, string)
	walk = func(node *yaml.Node, inheritedDisabled int, parentID string) {
		if node == nil || node.Kind != yaml.MappingNode {
			return
		}
		id := yamlScalar(yamlMapValue(node, "id"))
		entryID := id
		if prefixIDs && parentID != "" && id != "" {
			entryID = parentID + ":" + id
		}
		name := yamlScalar(yamlMapValue(node, "name"))
		group := yamlNodeBool(yamlMapValue(node, "group"), false)
		disabled, condition := presetDisabledState(yamlMapValue(node, "disabled"))
		disabled = combinePresetDisabled(inheritedDisabled, disabled)
		if name != "" && !group {
			var phase *string
			if live && disabled == 0 {
				active := "active"
				phase = &active
			}
			enabled := any(true)
			if disabled == 1 {
				enabled = false
			} else if disabled == 2 {
				enabled = "conditional"
			}
			entries = append(entries, presetCompositionRow{entryID: entryID, moduleName: name, enabled: enabled, condition: condition, fiberPhase: phase})
		}
		if group {
			children := yamlMapValue(node, "config")
			if children != nil && children.Kind == yaml.SequenceNode {
				for _, child := range children.Content {
					walk(child, disabled, entryID)
				}
			}
		}
	}
	for _, node := range document.Content[0].Content {
		walk(node, 0, "")
	}
	return entries, nil
}

// disabled state: 0 enabled, 1 disabled, 2 conditional (unresolved !!js).
func presetDisabledState(node *yaml.Node) (int, string) {
	if node == nil {
		return 0, ""
	}
	if node.Tag == "!!js" || node.Tag == "tag:yaml.org,2002:js" {
		expression := strings.TrimSpace(node.Value)
		switch expression {
		case "process.platform === 'win32'", `process.platform === "win32"`:
			if runtime.GOOS == "windows" {
				return 1, expression
			}
			return 0, expression
		case "process.platform !== 'win32'", `process.platform !== "win32"`:
			if runtime.GOOS != "windows" {
				return 1, expression
			}
			return 0, expression
		default:
			return 2, expression
		}
	}
	if yamlNodeBool(node, false) {
		return 1, ""
	}
	return 0, ""
}

func combinePresetDisabled(outer, own int) int {
	if outer == 1 || own == 1 {
		return 1
	}
	if outer == 2 || own == 2 {
		return 2
	}
	return 0
}

func (e *Engine) compilePresetRuntime(row presetRecord) (agentRuntime, error) {
	pruneDefaults := defaultToolResultPruneConfig()
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(row.content), &document); err != nil {
		return agentRuntime{}, fmt.Errorf("agent-preset-invalid: %w", err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.SequenceNode {
		return agentRuntime{}, errors.New("agent-preset-invalid: composition must be a YAML array")
	}
	runtimeConfig := agentRuntime{
		persona: e.cfg.Persona, personaSuffix: e.cfg.PersonaSuffix, includeRuntimeContext: true,
		instructionMaxBytes: e.cfg.InstructionMaxBytes,
		toolNames:           map[string]bool{}, toolPresentation: e.cfg.ToolPresentation,
		goalDefaultMaxRounds: defaultMaxGoalRounds, goalBlockThreshold: defaultGoalBlockThreshold,
		readLimit: readToolLineLimit, readMaxLineLength: readToolMaxLineChars, readMaxBytes: readToolMaxBytes,
		readStreamMinSize: 10 * 1024 * 1024, globMaxResults: globToolMaxResults,
		grepMaxMatches: grepToolMaxMatches, grepMaxLineBytes: grepToolMaxLineBytes,
		searchTimeout:                    30 * time.Second,
		searchGrace:                      3 * time.Second,
		searchStderrMaxBytes:             64 * 1024,
		rawOutputMaxBytes:                20_000_000,
		searchMetaMaxBytes:               65_536,
		skillCatalogDescriptionMaxLength: 500,
		bashEnableRunInBackground:        true,
		skillIncludeDefaultRoots:         true,
		skillFilesystemEnabled:           false,
		skillDshHome:                     e.cfg.DataDir,
		skillAgentsHome:                  e.cfg.AgentsHome,
		skillBundledSkillDir:             e.cfg.SkillDir,
		toolResultPruneThresholdChars:    pruneDefaults.ThresholdChars,
		toolResultPruneHeadChars:         pruneDefaults.HeadChars,
		toolResultPruneTailChars:         pruneDefaults.TailChars,
		toolResultPrunerEnabled:          false,
		ralphSubagentProvider:            "spawn",
		ralphMaxRounds:                   256,
		ralphMaxHandoffChars:             16_384,
		ralphMaxResultChars:              16_384,
		workflowToolName:                 "workflow",
		workflowMaxResultChars:           50_000,
		workflowProvider:                 "spawn",
		workflowMaxConcurrentAgents:      0,
		workflowMaxAgents:                1000,
		workflowMaxItems:                 4096,
		workflowSyncTimeout:              5 * time.Second,
		workflowDisposeGrace:             5 * time.Second,
		compactionEnabled:                false,
		compactionAuto:                   false,
		compactionConfig:                 newCompactionRuntimeConfig(defaultCompactionConfig()),
		compactCommandEnabled:            false,
	}
	if err := walkPresetRows(document.Content[0], func(entry *yaml.Node) error {
		name := yamlScalar(yamlMapValue(entry, "name"))
		config := yamlMapValue(entry, "config")
		switch name {
		case "@deepseek-ai/dsh-tool-fs":
			if err := rejectUnknownPresetConfig(config, "tool-fs", "readLimit", "readMaxLineLength", "readMaxBytes", "readStreamMinSize"); err != nil {
				return err
			}
			for key, target := range map[string]*int{
				"readLimit": &runtimeConfig.readLimit, "readMaxLineLength": &runtimeConfig.readMaxLineLength,
				"readMaxBytes": &runtimeConfig.readMaxBytes, "readStreamMinSize": &runtimeConfig.readStreamMinSize,
			} {
				if node := yamlMapValue(config, key); node != nil {
					value := yamlNodeInt(node)
					if value <= 0 {
						return fmt.Errorf("agent-preset-invalid: tool-fs %s must be a positive integer", key)
					}
					*target = value
				}
			}
		case "@deepseek-ai/dsh-tool-fs-search":
			if err := rejectUnknownPresetConfig(config, "tool-fs-search", "sampleOverCapGlobResults", "globMaxResults", "grepMaxMatches", "grepMaxLineBytes", "searchMetaMaxBytes", "rawOutputMaxBytes", "graceMs", "stderrMaxBytes", "timeoutMs"); err != nil {
				return err
			}
			if node := yamlMapValue(config, "sampleOverCapGlobResults"); node != nil {
				if node.Tag != "!!bool" {
					return errors.New("agent-preset-invalid: tool-fs-search sampleOverCapGlobResults must be boolean")
				}
				runtimeConfig.globSampleOverCapResults = yamlNodeBool(node, false)
			} else {
				return errors.New("agent-preset-invalid: tool-fs-search sampleOverCapGlobResults is required")
			}
			for key, target := range map[string]*int{
				"globMaxResults": &runtimeConfig.globMaxResults, "grepMaxMatches": &runtimeConfig.grepMaxMatches,
				"grepMaxLineBytes": &runtimeConfig.grepMaxLineBytes, "searchMetaMaxBytes": &runtimeConfig.searchMetaMaxBytes,
				"rawOutputMaxBytes": &runtimeConfig.rawOutputMaxBytes, "stderrMaxBytes": &runtimeConfig.searchStderrMaxBytes,
			} {
				if node := yamlMapValue(config, key); node != nil {
					value, ok := presetSafeInteger(node, false)
					if !ok {
						return fmt.Errorf("agent-preset-invalid: tool-fs-search %s must be a positive integer", key)
					}
					*target = value
				}
			}
			if node := yamlMapValue(config, "timeoutMs"); node != nil {
				value, ok := presetSafeInteger(node, false)
				if !ok {
					return fmt.Errorf("agent-preset-invalid: tool-fs-search timeoutMs must be a positive integer")
				}
				runtimeConfig.searchTimeout = time.Duration(value) * time.Millisecond
			}
			if node := yamlMapValue(config, "graceMs"); node != nil {
				value, ok := presetSafeInteger(node, false)
				if !ok {
					return fmt.Errorf("agent-preset-invalid: tool-fs-search graceMs must be a positive integer")
				}
				if value > 2_147_483_647 {
					return errors.New("agent-preset-invalid: tool-fs-search graceMs must be no greater than 2147483647")
				}
				runtimeConfig.searchGrace = time.Duration(value) * time.Millisecond
			}
		case "@deepseek-ai/dsh-tool-web":
			webTools, err := presetWebToolConfig(config)
			if err != nil {
				return err
			}
			runtimeConfig.webTools = webTools
		case "@deepseek-ai/dsh-tool-bash", "@deepseek-ai/dsh-tool-pwsh":
			if err := rejectUnknownPresetConfig(config, "tool-shell", "enableRunInBackground"); err != nil {
				return err
			}
			if node := yamlMapValue(config, "enableRunInBackground"); node != nil {
				if node.Tag != "!!bool" {
					return errors.New("agent-preset-invalid: tool shell enableRunInBackground must be boolean")
				}
				runtimeConfig.bashEnableRunInBackground = yamlNodeBool(node, true)
			}
		case "@deepseek-ai/dsh-tool-skill":
			if err := rejectUnknownPresetConfig(config, "tool-skill", "catalogDescriptionMaxLength"); err != nil {
				return err
			}
			if node := yamlMapValue(config, "catalogDescriptionMaxLength"); node != nil {
				value := yamlNodeInt(node)
				if value < 3 {
					return errors.New("agent-preset-invalid: tool-skill catalogDescriptionMaxLength must be at least 3")
				}
				runtimeConfig.skillCatalogDescriptionMaxLength = value
			}
		case "@deepseek-ai/dsh-skill-filesystem":
			if err := rejectUnknownPresetConfig(config, "skill-filesystem", "providerName", "includeDefaultRoots", "dshHome", "agentsHome", "customSkillDirs", "watch", "watchUsePolling", "watchStabilityThresholdMs", "watchPollIntervalMs", "watchMaxProjects", "watchFollowSymlinks", "bundledSkillDir"); err != nil {
				return err
			}
			runtimeConfig.skillFilesystemEnabled = true
			if node := yamlMapValue(config, "includeDefaultRoots"); node != nil {
				if node.Tag != "!!bool" {
					return errors.New("agent-preset-invalid: skill-filesystem includeDefaultRoots must be boolean")
				}
				runtimeConfig.skillIncludeDefaultRoots = yamlNodeBool(node, true)
			}
			for key, target := range map[string]*string{
				"dshHome": &runtimeConfig.skillDshHome, "agentsHome": &runtimeConfig.skillAgentsHome, "bundledSkillDir": &runtimeConfig.skillBundledSkillDir,
			} {
				if node := yamlMapValue(config, key); node != nil {
					value, err := presetConfigPath(node, row.dir)
					if err != nil {
						return fmt.Errorf("agent-preset-invalid: skill-filesystem %s: %w", key, err)
					}
					*target = value
				}
			}
			dirs, err := presetCustomSkillDirs(config, row.dir)
			if err != nil {
				return err
			}
			runtimeConfig.customSkillDirs = append(runtimeConfig.customSkillDirs, dirs...)
		case "@deepseek-ai/dsh-persona":
			if err := rejectUnknownPresetConfig(config, "persona", "prefix", "suffix", "text", "complete", "includeRuntimeContext"); err != nil {
				return err
			}
			prefix := yamlMapValue(config, "prefix")
			if prefix == nil {
				prefix = yamlMapValue(config, "text")
			}
			if prefix == nil {
				return errors.New("agent-preset-invalid: persona prefix is required")
			}
			runtimeConfig.persona = yamlScalar(prefix)
			runtimeConfig.personaSuffix = yamlScalar(yamlMapValue(config, "suffix"))
			runtimeConfig.completePersona = yamlNodeBool(yamlMapValue(config, "complete"), false)
			runtimeConfig.includeRuntimeContext = yamlNodeBool(yamlMapValue(config, "includeRuntimeContext"), true)
		case "@deepseek-ai/dsh-agent-instructions":
			if err := rejectUnknownPresetConfig(config, "agent-instructions", "dshHome", "projectRootMarkers", "maxBytes", "maxSourceBytes", "instructionFileCandidates", "localInstructionFileCandidates"); err != nil {
				return err
			}
			if yamlMapValue(config, "maxBytes") == nil {
				return errors.New("agent-preset-invalid: agent-instructions maxBytes is required")
			}
			runtimeConfig.includeInstructions = true
			if max := yamlNodeInt(yamlMapValue(config, "maxBytes")); max >= 0 {
				runtimeConfig.instructionMaxBytes = max
			}
		case "@deepseek-ai/dsh-tool-bash-persistent", "@deepseek-ai/dsh-tool-pwsh-persistent":
			if err := rejectUnknownPresetConfig(config, "tool-persistent-shell", "timeoutMs", "maxOutputChars", "description"); err != nil {
				return err
			}
			runtimeConfig.persistentBash = true
			runtimeConfig.persistentBashDesc = yamlScalar(yamlMapValue(config, "description"))
			if timeout := yamlNodeInt(yamlMapValue(config, "timeoutMs")); timeout >= 0 {
				if timeout <= 0 {
					return fmt.Errorf("agent-preset-invalid: persistent shell timeoutMs must be a positive integer")
				}
				runtimeConfig.persistentBashTimeout = time.Duration(timeout) * time.Millisecond
			}
			if maxOutput := yamlNodeInt(yamlMapValue(config, "maxOutputChars")); maxOutput >= 0 {
				if maxOutput <= 0 {
					return fmt.Errorf("agent-preset-invalid: persistent shell maxOutputChars must be a positive integer")
				}
				runtimeConfig.persistentBashMaxOutputChars = maxOutput
			}
		case "@deepseek-ai/dsh-tool-str-replace-editor":
			if err := rejectUnknownPresetConfig(config, "tool-str-replace-editor", "maxOutputChars", "description"); err != nil {
				return err
			}
			runtimeConfig.editorDescription = yamlScalar(yamlMapValue(config, "description"))
			if maxOutput := yamlNodeInt(yamlMapValue(config, "maxOutputChars")); maxOutput >= 0 {
				if maxOutput <= 0 {
					return fmt.Errorf("agent-preset-invalid: str_replace_editor maxOutputChars must be a positive integer")
				}
				runtimeConfig.editorMaxOutputChars = maxOutput
			}
		case "@deepseek-ai/dsh-agent-tool-presentation":
			if err := rejectUnknownPresetConfig(config, "agent-tool-presentation", "mode"); err != nil {
				return err
			}
			mode := yamlScalar(yamlMapValue(config, "mode"))
			if mode != "native" && mode != "ptc" && mode != "both" {
				return errors.New("agent-preset-invalid: agent-tool-presentation mode must be native, ptc, or both")
			}
			runtimeConfig.toolPresentation = mode
		case "@deepseek-ai/dsh-plan-mode":
			if config == nil || config.Kind != yaml.MappingNode {
				return errors.New("agent-preset-invalid: PlanModeConfig is { section }")
			}
			for index := 0; index+1 < len(config.Content); index += 2 {
				if config.Content[index].Value != "section" {
					return fmt.Errorf("agent-preset-invalid: PlanModeConfig has unknown key %q - config is { section }", config.Content[index].Value)
				}
			}
			runtimeConfig.planSection = yamlScalar(yamlMapValue(config, "section"))
			if strings.TrimSpace(runtimeConfig.planSection) == "" {
				return errors.New("agent-preset-invalid: PlanModeConfig needs a non-empty `section`")
			}
		case "@deepseek-ai/dsh-goal":
			if err := rejectUnknownPresetConfig(config, "goal", "defaultMaxGoalRounds"); err != nil {
				return err
			}
			if value := yamlNodeInt(yamlMapValue(config, "defaultMaxGoalRounds")); value >= 0 {
				if value < 1 {
					return errors.New("agent-preset-invalid: defaultMaxGoalRounds must be a positive integer")
				}
				runtimeConfig.goalDefaultMaxRounds = value
			}
		case "@deepseek-ai/dsh-tool-goal":
			if err := rejectUnknownPresetConfig(config, "tool-goal", "blockedAfterConsecutiveRounds"); err != nil {
				return err
			}
			if value := yamlNodeInt(yamlMapValue(config, "blockedAfterConsecutiveRounds")); value >= 0 {
				if value < 1 {
					return errors.New("agent-preset-invalid: blockedAfterConsecutiveRounds must be a positive integer")
				}
				runtimeConfig.goalBlockThreshold = value
			}
		case "@deepseek-ai/dsh-goal-round-driver":
			runtimeConfig.goalRoundDriver = true
		case "@deepseek-ai/dsh-compaction-tool-result-pruner":
			runtimeConfig.toolResultPrunerEnabled = true
			if err := rejectUnknownPresetConfig(config, "compaction-tool-result-pruner", "thresholdChars", "headChars", "tailChars"); err != nil {
				return err
			}
			for key, target := range map[string]*int{
				"thresholdChars": &runtimeConfig.toolResultPruneThresholdChars,
				"headChars":      &runtimeConfig.toolResultPruneHeadChars,
				"tailChars":      &runtimeConfig.toolResultPruneTailChars,
			} {
				if node := yamlMapValue(config, key); node != nil {
					value, ok := presetInteger(node, key != "thresholdChars", int64(^uint(0)>>1))
					if !ok {
						return fmt.Errorf("agent-preset-invalid: compaction-tool-result-pruner %s must be %s", key, map[bool]string{true: "positive", false: "non-negative"}[key == "thresholdChars"]+" integer")
					}
					*target = value
				}
			}
			if runtimeConfig.toolResultPruneHeadChars+utf8.RuneCountInString(toolResultPruneMarker)+runtimeConfig.toolResultPruneTailChars > runtimeConfig.toolResultPruneThresholdChars {
				return errors.New("agent-preset-invalid: compaction-tool-result-pruner headChars + marker + tailChars must fit thresholdChars")
			}
		case "@deepseek-ai/dsh-compaction-basic":
			if err := compilePresetCompactionConfig(config, &runtimeConfig.compactionConfig); err != nil {
				return err
			}
			runtimeConfig.compactionEnabled = true
			runtimeConfig.compactionAuto = !runtimeConfig.compactionConfig.AutoDisabled
		case "@deepseek-ai/dsh-command-compact":
			if err := rejectUnknownPresetConfig(config, "command-compact"); err != nil {
				return err
			}
			runtimeConfig.compactCommandEnabled = true
		case "@deepseek-ai/dsh-tool-ralph":
			if err := rejectUnknownPresetConfig(config, "tool-ralph", "subagentProvider", "maxRounds", "maxHandoffChars", "maxResultChars"); err != nil {
				return err
			}
			if node := yamlMapValue(config, "subagentProvider"); node != nil {
				if node.Kind != yaml.ScalarNode || strings.TrimSpace(node.Value) == "" || node.Value != strings.TrimSpace(node.Value) {
					return errors.New("agent-preset-invalid: tool-ralph subagentProvider must be a non-empty normalized string")
				}
				runtimeConfig.ralphSubagentProvider = node.Value
			}
			for key, target := range map[string]*int{
				"maxRounds":       &runtimeConfig.ralphMaxRounds,
				"maxHandoffChars": &runtimeConfig.ralphMaxHandoffChars,
				"maxResultChars":  &runtimeConfig.ralphMaxResultChars,
			} {
				if node := yamlMapValue(config, key); node != nil {
					value := yamlNodeInt(node)
					if value < 1 || int64(value) > maxJSONSafeInteger {
						return fmt.Errorf("agent-preset-invalid: tool-ralph %s must be a positive safe integer", key)
					}
					*target = value
				}
			}
		case "@deepseek-ai/dsh-tool-workflow":
			if err := rejectUnknownPresetConfig(config, "tool-workflow", "toolName", "maxResultChars"); err != nil {
				return err
			}
			if node := yamlMapValue(config, "toolName"); node != nil {
				if node.Kind != yaml.ScalarNode || strings.TrimSpace(node.Value) == "" || node.Value != strings.TrimSpace(node.Value) {
					return errors.New("agent-preset-invalid: tool-workflow toolName must be a non-empty normalized string")
				}
				runtimeConfig.workflowToolName = node.Value
			}
			if node := yamlMapValue(config, "maxResultChars"); node != nil {
				value := yamlNodeInt(node)
				if value < 1 || int64(value) > maxJSONSafeInteger {
					return errors.New("agent-preset-invalid: tool-workflow maxResultChars must be a positive safe integer")
				}
				runtimeConfig.workflowMaxResultChars = value
			}
		case "@deepseek-ai/dsh-workflow-worker-thread":
			if err := rejectUnknownPresetConfig(config, "workflow-worker-thread", "provider", "maxConcurrentAgents", "maxTotalAgents", "maxItemsPerCall", "syncTimeoutMs", "disposeGraceMs"); err != nil {
				return err
			}
			if node := yamlMapValue(config, "provider"); node != nil {
				if node.Kind != yaml.ScalarNode || strings.TrimSpace(node.Value) == "" || node.Value != strings.TrimSpace(node.Value) {
					return errors.New("agent-preset-invalid: workflow-worker-thread provider must be a non-empty normalized string")
				}
				runtimeConfig.workflowProvider = node.Value
			}
			for key, target := range map[string]*int{
				"maxTotalAgents":  &runtimeConfig.workflowMaxAgents,
				"maxItemsPerCall": &runtimeConfig.workflowMaxItems,
			} {
				if node := yamlMapValue(config, key); node != nil {
					value := yamlNodeInt(node)
					if value < 1 || int64(value) > maxJSONSafeInteger {
						return fmt.Errorf("agent-preset-invalid: workflow-worker-thread %s must be a positive safe integer", key)
					}
					*target = value
				}
			}
			if node := yamlMapValue(config, "maxConcurrentAgents"); node != nil {
				value := yamlNodeInt(node)
				if value < 0 || int64(value) > maxJSONSafeInteger {
					return errors.New("agent-preset-invalid: workflow-worker-thread maxConcurrentAgents must be a non-negative safe integer")
				}
				runtimeConfig.workflowMaxConcurrentAgents = value
			}
			if node := yamlMapValue(config, "syncTimeoutMs"); node != nil {
				value := yamlNodeInt(node)
				if value < 1 || int64(value) > maxJSONSafeInteger {
					return errors.New("agent-preset-invalid: workflow-worker-thread syncTimeoutMs must be a positive safe integer")
				}
				runtimeConfig.workflowSyncTimeout = time.Duration(value) * time.Millisecond
			}
			if node := yamlMapValue(config, "disposeGraceMs"); node != nil {
				value := yamlNodeInt(node)
				if value < 0 || int64(value) > maxJSONSafeInteger {
					return errors.New("agent-preset-invalid: workflow-worker-thread disposeGraceMs must be a non-negative safe integer")
				}
				runtimeConfig.workflowDisposeGrace = time.Duration(value) * time.Millisecond
			}
		case "@deepseek-ai/dsh-experimental-tool-agent-team":
			runtimeConfig.teamTools = true
		case "@deepseek-ai/dsh-tool-subagent-control":
			runtimeConfig.legacySubagentControl = true
		case "@deepseek-ai/dsh-tool-subagent-control/list-agents":
			runtimeConfig.legacyListAgents = true
		}
		for _, toolName := range presetToolNames(name, config) {
			runtimeConfig.toolNames[toolName] = true
		}
		return nil
	}); err != nil {
		return agentRuntime{}, err
	}
	if runtimeConfig.compactCommandEnabled && !runtimeConfig.compactionEnabled {
		return agentRuntime{}, errors.New("agent-preset-invalid: command-compact requires a mounted compaction service")
	}
	return runtimeConfig, nil
}

func presetCustomSkillDirs(config *yaml.Node, baseDir string) ([]string, error) {
	node := yamlMapValue(config, "customSkillDirs")
	if node == nil {
		return nil, nil
	}
	if node.Kind != yaml.SequenceNode {
		return nil, errors.New("agent-preset-invalid: skill-filesystem customSkillDirs must be an array")
	}
	result := make([]string, 0, len(node.Content))
	for _, item := range node.Content {
		if item.Kind != yaml.ScalarNode {
			return nil, errors.New("agent-preset-invalid: skill-filesystem customSkillDirs entries must be strings")
		}
		value := strings.TrimSpace(item.Value)
		if item.Tag == "!!js" || item.Tag == "tag:yaml.org,2002:js" {
			resolved, err := evaluatePresetConfigJS(value, baseDir)
			if err != nil {
				return nil, fmt.Errorf("agent-preset-invalid: customSkillDirs: %w", err)
			}
			value, _ = resolved.(string)
		}
		if value == "" {
			continue
		}
		if abs, err := filepath.Abs(value); err == nil {
			value = abs
		}
		if !containsString(result, value) {
			result = append(result, value)
		}
	}
	return result, nil
}

func presetConfigPath(node *yaml.Node, baseDir string) (string, error) {
	if node == nil || node.Kind != yaml.ScalarNode {
		return "", errors.New("must be a string")
	}
	value := strings.TrimSpace(node.Value)
	if node.Tag == "!!js" || node.Tag == "tag:yaml.org,2002:js" {
		resolved, err := evaluatePresetConfigJS(value, baseDir)
		if err != nil {
			return "", err
		}
		var ok bool
		value, ok = resolved.(string)
		if !ok {
			return "", errors.New("expression must return a string")
		}
		value = strings.TrimSpace(value)
	}
	if value == "" {
		return "", nil
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	return abs, nil
}

func evaluatePresetConfigJS(expression, baseDir string) (any, error) {
	// The shipped cordis preset uses node:url + new URL(..., baseUrl). Goja does
	// not provide Node's URL implementation, so resolve this loader idiom at the
	// same boundary before evaluating other independent expressions.
	trimmed := strings.TrimSpace(expression)
	if strings.Contains(trimmed, "fileURLToPath") && strings.Contains(trimmed, "new URL") && strings.Contains(trimmed, "baseUrl") {
		if start := strings.Index(trimmed, "new URL("); start >= 0 {
			rest := trimmed[start+len("new URL("):]
			if len(rest) > 0 && (rest[0] == '\'' || rest[0] == '"') {
				quote := rest[0]
				if end := strings.IndexByte(rest[1:], quote); end >= 0 {
					path := rest[1 : end+1]
					return filepath.Join(baseDir, filepath.FromSlash(path)), nil
				}
			}
		}
	}
	vm := goja.New()
	process := vm.NewObject()
	env := vm.NewObject()
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			_ = env.Set(key, value)
		}
	}
	_ = process.Set("env", env)
	_ = process.Set("platform", runtime.GOOS)
	_ = process.Set("cwd", func() string { cwd, _ := os.Getwd(); return cwd })
	_ = process.Set("getBuiltinModule", func(name string) any {
		module := vm.NewObject()
		switch name {
		case "node:path", "path":
			_ = module.Set("join", filepath.Join)
		case "node:os", "os":
			_ = module.Set("homedir", func() string { home, _ := os.UserHomeDir(); return home })
		}
		return module
	})
	_ = vm.Set("process", process)
	_ = vm.Set("baseUrl", baseDir)
	_ = vm.Set("dshHomePath", func(parts ...string) string {
		home := strings.TrimSpace(os.Getenv("DSH_HOME"))
		if home == "" {
			home, _ = os.UserHomeDir()
			home = filepath.Join(home, ".dsh")
		}
		return filepath.Join(append([]string{home}, parts...)...)
	})
	value, err := vm.RunString("(" + expression + "\n)")
	if err != nil {
		return nil, err
	}
	if goja.IsUndefined(value) || goja.IsNull(value) {
		return nil, nil
	}
	return value.Export(), nil
}

func presetToolNames(plugin string, config *yaml.Node) []string {
	switch plugin {
	case "@deepseek-ai/dsh-tool-bash", "@deepseek-ai/dsh-tool-bash-persistent":
		return []string{"bash"}
	case "@deepseek-ai/dsh-tool-pwsh", "@deepseek-ai/dsh-tool-pwsh-persistent":
		return []string{"pwsh"}
	case "@deepseek-ai/dsh-tool-fs":
		return []string{"read", "read_image", "write", "edit"}
	case "@deepseek-ai/dsh-tool-fs-search":
		return []string{"glob", "grep"}
	case "@deepseek-ai/dsh-tool-str-replace-editor":
		return []string{"str_replace_editor"}
	case "@deepseek-ai/dsh-tool-jobs":
		return []string{"job_output", "job_list", "job_kill"}
	case "@deepseek-ai/dsh-tool-skill":
		return []string{"skill"}
	case "@deepseek-ai/dsh-tool-goal":
		return []string{"get_goal", "create_goal", "update_goal"}
	case "@deepseek-ai/dsh-plan-mode":
		return []string{"exit_plan_mode"}
	case "@deepseek-ai/dsh-tool-subagent-control":
		return []string{"send_message", "interrupt_agent"}
	case "@deepseek-ai/dsh-tool-subagent-control/list-agents":
		return []string{"list_agents"}
	case "@deepseek-ai/dsh-experimental-tool-agent-team":
		return []string{
			"spawn_teammate", "send_message", "list_agents", "wait_agent", "interrupt_agent",
			"team_task_create", "team_task_list", "team_task_get", "team_task_update",
		}
	case "@deepseek-ai/dsh-tool-subagent":
		name := yamlScalar(yamlMapValue(config, "toolName"))
		if name == "" {
			name = "subagent"
		}
		return []string{name}
	case "@deepseek-ai/dsh-tool-ask-user":
		return []string{"ask_user_question"}
	case "@deepseek-ai/dsh-tool-todo":
		return []string{"todo_write"}
	case "@deepseek-ai/dsh-tool-web":
		tools := []string{}
		if yamlNodeBool(yamlMapValue(config, "search"), true) {
			tools = append(tools, "web_search")
		}
		if yamlNodeBool(yamlMapValue(config, "fetch"), true) {
			tools = append(tools, "web_fetch")
		}
		return tools
	case "@deepseek-ai/dsh-tool-workflow":
		name := yamlScalar(yamlMapValue(config, "toolName"))
		if name == "" {
			name = "workflow"
		}
		return []string{name}
	case "@deepseek-ai/dsh-tool-ralph":
		return []string{"ralph"}
	case "@deepseek-ai/dsh-tool-cordis":
		return append([]string(nil), dynamicCordisToolNames...)
	case "@deepseek-ai/dsh-tool-session-query":
		return append([]string(nil), sessionQueryToolNames...)
	case "@deepseek-ai/dsh-schedule":
		return []string{"schedule_create", "schedule_list", "schedule_delete"}
	case "@deepseek-ai/dsh-tool-lsp":
		return []string{"lsp"}
	case "@deepseek-ai/dsh-tool-terminal":
		return []string{"terminal_open", "terminal_send", "terminal_read", "terminal_signal", "terminal_close", "terminal_list"}
	}
	return nil
}

func presetWebToolConfig(node *yaml.Node) (*WebToolConfig, error) {
	if err := rejectUnknownPresetConfig(node, "tool-web", "search", "fetch", "searchMaxResults", "searchMaxQueries", "fetchTimeoutMs", "searchTimeoutMs", "fetchMaxOutputChars"); err != nil {
		return nil, err
	}
	config := DefaultWebToolConfig()
	for key, target := range map[string]*bool{"search": &config.SearchEnabled, "fetch": &config.FetchEnabled} {
		if value := yamlMapValue(node, key); value != nil {
			if value.Tag != "!!bool" {
				return nil, fmt.Errorf("agent-preset-invalid: tool-web %s must be boolean", key)
			}
			*target = yamlNodeBool(value, false)
		}
	}
	for key, target := range map[string]*int{
		"searchMaxResults":    &config.SearchMaxResults,
		"searchMaxQueries":    &config.SearchMaxQueries,
		"fetchMaxOutputChars": &config.FetchMaxOutputChars,
	} {
		if value := yamlMapValue(node, key); value != nil {
			parsed, ok := presetSafeInteger(value, false)
			if !ok {
				return nil, fmt.Errorf("agent-preset-invalid: tool-web %s must be a positive integer", key)
			}
			*target = parsed
		}
	}
	for key, target := range map[string]*time.Duration{
		"fetchTimeoutMs":  &config.FetchTimeout,
		"searchTimeoutMs": &config.SearchTimeout,
	} {
		if value := yamlMapValue(node, key); value != nil {
			parsed, ok := presetSafeInteger(value, false)
			if !ok {
				return nil, fmt.Errorf("agent-preset-invalid: tool-web %s must be a positive integer", key)
			}
			*target = time.Duration(parsed) * time.Millisecond
		}
	}
	return &config, nil
}

func walkPresetRows(sequence *yaml.Node, visit func(*yaml.Node) error) error {
	if sequence == nil || sequence.Kind != yaml.SequenceNode {
		return nil
	}
	for _, entry := range sequence.Content {
		if entry.Kind != yaml.MappingNode || presetRowDisabled(yamlMapValue(entry, "disabled")) {
			continue
		}
		if err := visit(entry); err != nil {
			return err
		}
		if yamlNodeBool(yamlMapValue(entry, "group"), false) {
			if err := walkPresetRows(yamlMapValue(entry, "config"), visit); err != nil {
				return err
			}
		}
	}
	return nil
}

func presetRowDisabled(node *yaml.Node) bool {
	if node == nil {
		return false
	}
	if node.Tag == "!!js" || node.Tag == "tag:yaml.org,2002:js" {
		expression := strings.TrimSpace(node.Value)
		switch expression {
		case "process.platform === 'win32'", `process.platform === "win32"`:
			return runtime.GOOS == "windows"
		case "process.platform !== 'win32'", `process.platform !== "win32"`:
			return runtime.GOOS != "windows"
		default:
			return false
		}
	}
	return yamlNodeBool(node, false)
}

func yamlMapValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	return nil
}

func rejectUnknownPresetConfig(config *yaml.Node, plugin string, allowed ...string) error {
	if config == nil {
		return nil
	}
	if config.Kind != yaml.MappingNode {
		return fmt.Errorf("agent-preset-invalid: %s config must be an object", plugin)
	}
	known := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		known[key] = true
	}
	for index := 0; index+1 < len(config.Content); index += 2 {
		key := config.Content[index].Value
		if !known[key] {
			return fmt.Errorf("agent-preset-invalid: %s unknown key %q", plugin, key)
		}
	}
	return nil
}

func compilePresetCompactionConfig(node *yaml.Node, target *compactionRuntimeConfig) error {
	const plugin = "compaction-basic"
	keys := []string{"thresholdRatio", "retainRatio", "retainTokens", "summarizationProvider", "summarizationModel", "maxTokens", "compactionRetries", "maxOverflowRetries", "modelPolicies", "auto"}
	if err := rejectUnknownPresetConfig(node, plugin, keys...); err != nil {
		return err
	}
	resolved, err := resolvePresetCompactionPolicy(node, newCompactionRuntimeConfig(defaultCompactionConfig()), "BasicCompactionConfig", true)
	if err != nil {
		return err
	}
	if auto := yamlMapValue(node, "auto"); auto != nil {
		if auto.Tag != "!!bool" {
			return errors.New("agent-preset-invalid: BasicCompactionConfig: auto must be a boolean")
		}
		resolved.AutoDisabled = !yamlNodeBool(auto, true)
	}
	policies := yamlMapValue(node, "modelPolicies")
	if policies != nil {
		if policies.Kind != yaml.SequenceNode {
			return errors.New("agent-preset-invalid: BasicCompactionConfig: modelPolicies must be an array")
		}
		seen := map[string]bool{}
		for index, policyNode := range policies.Content {
			name := fmt.Sprintf("BasicCompactionConfig: modelPolicies[%d]", index)
			if policyNode.Kind != yaml.MappingNode {
				return fmt.Errorf("agent-preset-invalid: %s must be an object", name)
			}
			if err := rejectUnknownPresetConfig(policyNode, name, "provider", "model", "thresholdRatio", "retainRatio", "retainTokens", "summarizationProvider", "summarizationModel", "maxTokens", "compactionRetries", "maxOverflowRetries"); err != nil {
				return err
			}
			providerNode, modelNode := yamlMapValue(policyNode, "provider"), yamlMapValue(policyNode, "model")
			provider, model := yamlScalar(providerNode), yamlScalar(modelNode)
			if !presetStringNode(providerNode) || !presetStringNode(modelNode) || provider == "" || model == "" {
				return fmt.Errorf("agent-preset-invalid: %s provider and model must be non-empty strings", name)
			}
			key := provider + "\x00" + model
			if seen[key] {
				return fmt.Errorf("agent-preset-invalid: BasicCompactionConfig: duplicate model policy for %s/%s", provider, model)
			}
			seen[key] = true
			policy, policyErr := resolvePresetCompactionPolicy(policyNode, resolved, name, false)
			if policyErr != nil {
				return policyErr
			}
			resolved.modelPolicies = append(resolved.modelPolicies, compactionModelPolicy{
				Provider: provider, Model: model, ThresholdRatio: policy.ThresholdRatio,
				RetainRatio: policy.RetainRatio, RetainTokens: policy.RetainTokens, useRetainTokens: policy.useRetainTokens,
				SummarizationProvider: policy.SummarizationProvider, SummarizationModel: policy.SummarizationModel,
				MaxTokens: policy.MaxTokens, CompactionRetries: policy.CompactionRetries, MaxOverflowRetries: policy.MaxOverflowRetries,
			})
		}
	}
	*target = resolved
	return nil
}

func resolvePresetCompactionPolicy(node *yaml.Node, fallback compactionRuntimeConfig, name string, topLevel bool) (compactionRuntimeConfig, error) {
	resolved := cloneCompactionRuntimeConfig(fallback)
	resolved.modelPolicies = nil
	if node == nil {
		return resolved, nil
	}
	if node.Kind != yaml.MappingNode {
		return compactionRuntimeConfig{}, fmt.Errorf("agent-preset-invalid: %s config must be an object", name)
	}
	for _, field := range []string{"thresholdRatio", "retainRatio"} {
		if value := yamlMapValue(node, field); value != nil {
			var ratio float64
			if value.Decode(&ratio) != nil || ratio <= 0 || ratio > 1 {
				return compactionRuntimeConfig{}, fmt.Errorf("agent-preset-invalid: %s.%s must be a number in (0, 1]", name, field)
			}
			if field == "thresholdRatio" {
				resolved.ThresholdRatio = ratio
			} else {
				resolved.RetainRatio, resolved.RetainTokens, resolved.useRetainTokens = ratio, 0, false
			}
		}
	}
	retainRatio, retainTokens := yamlMapValue(node, "retainRatio"), yamlMapValue(node, "retainTokens")
	if retainRatio != nil && retainTokens != nil {
		return compactionRuntimeConfig{}, fmt.Errorf("agent-preset-invalid: %s: retainRatio and retainTokens are mutually exclusive", name)
	}
	if retainTokens != nil {
		value, ok := presetSafeInteger(retainTokens, true)
		if !ok {
			return compactionRuntimeConfig{}, fmt.Errorf("agent-preset-invalid: %s.retainTokens must be a non-negative safe integer", name)
		}
		resolved.RetainTokens, resolved.RetainRatio, resolved.useRetainTokens = value, 0, true
	}
	providerNode, modelNode := yamlMapValue(node, "summarizationProvider"), yamlMapValue(node, "summarizationModel")
	if providerNode != nil || modelNode != nil {
		if providerNode == nil || modelNode == nil || !presetStringNode(providerNode) || !presetStringNode(modelNode) || (providerNode.Value == "") != (modelNode.Value == "") {
			return compactionRuntimeConfig{}, fmt.Errorf("agent-preset-invalid: %s: summarizationProvider and summarizationModel must be set together as an empty or non-empty pair", name)
		}
		resolved.SummarizationProvider, resolved.SummarizationModel = providerNode.Value, modelNode.Value
	}
	for _, field := range []string{"maxTokens", "compactionRetries", "maxOverflowRetries"} {
		if value := yamlMapValue(node, field); value != nil {
			integer, ok := presetSafeInteger(value, field != "maxTokens")
			if !ok || field == "maxTokens" && integer < 1 {
				kind := "non-negative"
				if field == "maxTokens" {
					kind = "positive"
				}
				return compactionRuntimeConfig{}, fmt.Errorf("agent-preset-invalid: %s.%s must be a %s safe integer", name, field, kind)
			}
			switch field {
			case "maxTokens":
				resolved.MaxTokens = integer
			case "compactionRetries":
				resolved.CompactionRetries = integer
			case "maxOverflowRetries":
				resolved.MaxOverflowRetries = integer
			}
		}
	}
	if !resolved.useRetainTokens && resolved.RetainRatio >= resolved.ThresholdRatio {
		return compactionRuntimeConfig{}, fmt.Errorf("agent-preset-invalid: %s: retainRatio (%v) must be less than the resolved thresholdRatio (%v)", name, resolved.RetainRatio, resolved.ThresholdRatio)
	}
	if !topLevel {
		resolved.Disabled, resolved.AutoDisabled = false, false
	}
	return resolved, nil
}

func presetSafeInteger(node *yaml.Node, allowZero bool) (int, bool) {
	return presetInteger(node, allowZero, maxJSONSafeInteger)
}

func presetInteger(node *yaml.Node, allowZero bool, maximum int64) (int, bool) {
	if node == nil || node.Kind != yaml.ScalarNode {
		return 0, false
	}
	var value int64
	switch node.Tag {
	case "!!int", "tag:yaml.org,2002:int":
		if node.Decode(&value) != nil {
			return 0, false
		}
	case "!!float", "tag:yaml.org,2002:float":
		var number float64
		if node.Decode(&number) != nil || math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || number > float64(maximum) {
			return 0, false
		}
		value = int64(number)
		if float64(value) != number {
			return 0, false
		}
	default:
		return 0, false
	}
	if value > maximum || value < 0 || !allowZero && value == 0 || int64(int(value)) != value {
		return 0, false
	}
	return int(value), true
}

func presetStringNode(node *yaml.Node) bool {
	return node != nil && node.Kind == yaml.ScalarNode && (node.Tag == "!!str" || node.Tag == "tag:yaml.org,2002:str")
}

func yamlScalar(node *yaml.Node) string {
	if node == nil || node.Kind != yaml.ScalarNode {
		return ""
	}
	return node.Value
}

func yamlNodeBool(node *yaml.Node, fallback bool) bool {
	if node == nil {
		return fallback
	}
	var value bool
	if node.Decode(&value) != nil {
		return fallback
	}
	return value
}

func yamlNodeInt(node *yaml.Node) int {
	if node == nil {
		return -1
	}
	var value int
	if node.Decode(&value) != nil {
		return -1
	}
	return value
}

func (e *Engine) systemPromptForSession(s *Session, selection ModelSelection, agent agentRuntime) (string, error) {
	sections, err := e.resolvedSystemPromptSections(s, selection, agent, nil)
	if err != nil {
		return "", err
	}
	text := make([]string, len(sections))
	for index, section := range sections {
		text[index] = section.Text
	}
	return strings.Join(text, "\n\n"), nil
}

func (e *Engine) promptAssemblyForSession(ctx context.Context, s *Session, selection ModelSelection, agent agentRuntime) (resolvedPromptAssembly, error) {
	s.mu.Lock()
	cwd, sessionID := s.Header.CWD, s.Header.ID
	s.mu.Unlock()
	assemblyContext := map[string]any{"provider": selection.Provider, "model": selection.Model, "cwd": cwd}
	assembly, complete, suppressed, err := e.resolvedPromptAssemblyForSession(s, selection, agent, nil, assemblyContext)
	if err != nil {
		return resolvedPromptAssembly{}, err
	}
	transformed, err := e.dynamicPromptWaterfallForSession(ctx, sessionID, projectResolvedPromptAssembly(assembly), assemblyContext, complete, suppressed)
	if err != nil {
		return resolvedPromptAssembly{}, err
	}
	return decodeResolvedPromptAssembly(transformed)
}

func renderResolvedPromptAssembly(assembly resolvedPromptAssembly) (string, []resolvedPromptSection, error) {
	sections, err := renderResolvedPromptSections(assembly.Sections, assembly.Variables)
	if err != nil {
		return "", nil, err
	}
	contexts, err := renderResolvedPromptContexts(assembly.Contexts, assembly.Variables)
	if err != nil {
		return "", nil, err
	}
	text := make([]string, len(sections))
	for index, section := range sections {
		text[index] = section.Text
	}
	return strings.Join(text, "\n\n"), contexts, nil
}

func (e *Engine) resolvedSystemPromptSections(s *Session, selection ModelSelection, agent agentRuntime, caller *dynamicCordisRun) ([]resolvedPromptSection, error) {
	sections, variables, err := e.resolvedSystemPromptAssembly(s, selection, agent, caller, nil)
	if err != nil {
		return nil, err
	}
	for _, section := range sections {
		if section.Complete {
			sections = []resolvedPromptSection{section}
			break
		}
	}
	return renderResolvedPromptSections(sections, variables)
}

func (e *Engine) resolvedSystemPromptAssembly(s *Session, selection ModelSelection, agent agentRuntime, caller *dynamicCordisRun, assemblyContext map[string]any) ([]resolvedPromptSection, map[string]any, error) {
	s.mu.Lock()
	cwd, sessionID := s.Header.CWD, s.Header.ID
	s.mu.Unlock()
	if assemblyContext == nil {
		assemblyContext = map[string]any{"provider": selection.Provider, "model": selection.Model, "cwd": cwd}
	}
	variables := map[string]any{"provider": selection.Provider, "model": selection.Model, "cwd": cwd}
	providedVariables, err := e.dynamicPromptVariables(sessionID, caller, assemblyContext)
	if err != nil {
		return nil, nil, err
	}
	for name, value := range providedVariables {
		variables[name] = value
	}
	sections := []resolvedPromptSection{
		{Name: "harness:identity", Order: -100, Text: harnessIdentity},
		{Name: "deployment:persona-prefix", Order: 0, Text: agent.persona, Complete: agent.completePersona},
		{Name: "deployment:persona-suffix", Order: 10200, Text: agent.personaSuffix},
	}
	add := func(name string, order float64, text string) {
		sections = append(sections, resolvedPromptSection{Name: name, Order: order, Text: text})
	}
	if e.clientPluginComposed("@deepseek-ai/dsh-client-ui-deliverables") {
		add("ui:deliverable-file-references", 190, deliverableFilePrompt)
	}
	if e.structuredOutputRuntimeForSession(sessionID) != nil {
		add("tool:"+structuredOutputToolName, 190, structuredOutputInstruction)
	}
	if (e.cfg.ClientPlugins == nil || e.clientPluginActive("@deepseek-ai/dsh-file-reference-local")) &&
		(agent.toolNames == nil || agent.toolNames["read"]) {
		add("context:file-reference", 99, FileReferencePrompt)
	}
	if e.agentTeams != nil && agent.teamTools {
		if policy := teamPolicyPrompt(e.agentTeams, sessionID); policy != "" {
			add("team:policy", 60, policy)
		}
	}
	if agent.planSection != "" && planModeActiveForSession(s) {
		add("plan:policy", 50, agent.planSection)
	}
	if agent.toolNames == nil || agent.toolNames["get_goal"] {
		add("tool:goal", 114, goalToolGuidance(agent.goalBlockThreshold))
	}
	if agent.workflowToolName != "" && agent.toolNames[agent.workflowToolName] {
		add("tool:"+agent.workflowToolName, 100, "Use the "+agent.workflowToolName+" tool ONLY when the user explicitly asks for a workflow or for large multi-agent orchestration. For one or two delegations, prefer plain subagent calls.")
	}
	if agent.toolNames["ralph"] {
		add("tool:ralph", 101, "Use the ralph tool ONLY when the direct human explicitly asks for a Ralph loop or fresh-agent iterative execution. Completion and blockers are worker reports, not independent evaluation.")
	}
	webSearchVisible := e.hasRegisteredTool("web_search") && (agent.toolNames == nil || agent.toolNames["web_search"]) && (agent.webTools == nil || agent.webTools.SearchEnabled)
	webFetchVisible := e.hasRegisteredTool("web_fetch") && (agent.toolNames == nil || agent.toolNames["web_fetch"]) && (agent.webTools == nil || agent.webTools.FetchEnabled)
	if webSearchVisible {
		maxQueries := DefaultWebToolConfig().SearchMaxQueries
		if agent.webTools != nil {
			maxQueries = agent.webTools.SearchMaxQueries
		}
		guidance := fmt.Sprintf("Use the web_search tool to discover current information on the web. The required queries array accepts 1–%d non-empty search queries; use a one-item array for a single search. It returns an optional answer plus a list of source URLs. Use the returned source snippets when available, and cite the relevant URLs as markdown links.", maxQueries)
		if webFetchVisible {
			guidance = fmt.Sprintf("Use the web_search tool to discover current information on the web. The required queries array accepts 1–%d non-empty search queries; use a one-item array for a single search. It returns an optional answer plus a list of source URLs. Follow up with web_fetch when you need the full content of a specific result, and cite the relevant URLs as markdown links.", maxQueries)
		}
		add("tool:web_search", 110, guidance)
	}
	if webFetchVisible {
		add("tool:web_fetch", 111, "Use the web_fetch tool to retrieve the content of a specific HTTP(S) URL (for example a result from web_search). It returns the page content decoded to text. Cite the URL as a markdown link when you use its content.")
	}
	if agent.toolNames == nil || agent.toolNames["session_search"] {
		add("tool:session-query", 113, "Use session_search to find relevant work from prior sessions, or session_event_search to search earlier events in one session. Search results are cursor-free and workspace-scoped. Follow a useful hit with session_trace, session_event_trace, or session_event_read when you need lineage, relationships, or exact data.")
	}
	if agent.toolNames["lsp"] {
		add("tool:lsp", 120, "Use search/read for ordinary navigation. Use lsp when textual matches are ambiguous or before a change requires precise definitions, implementations, or references. Positions are one-based line and character (UTF-16) at the cursor; an off-symbol position may return no results. findReferences always includes the declaration.")
	}
	if agent.toolNames == nil || agent.toolNames["terminal_open"] {
		add("tool:terminal", 130, "Use a terminal session only when work needs persistent terminal state or interactive stdin; prefer shell/read/write/edit for bounded one-shot operations. Track every terminal session id and close sessions that no longer matter. An inferred_idle or timeout result does not prove the foreground command exited.")
	}
	if agent.toolPresentation == "ptc" {
		add("tools:ptc-only", 800, "`run_code` is the only tool you can call directly. A tool call naming any other tool fails. Reach every tool the SDK declares below from inside the program.")
	}
	if agent.toolPresentation == "ptc" || agent.toolPresentation == "both" {
		add("tools:sdk", 5000, e.codeModePrompt(s, agent))
	}
	dynamic, err := e.dynamicPromptSections(sessionID, caller, assemblyContext)
	if err != nil {
		return nil, nil, err
	}
	shadowed := map[string]bool{}
	for _, section := range dynamic {
		shadowed[section.Name] = true
	}
	kept := sections[:0]
	for _, section := range sections {
		if !shadowed[section.Name] {
			kept = append(kept, section)
		}
	}
	sections = append(kept, dynamic...)
	sort.SliceStable(sections, func(i, j int) bool { return sections[i].Order < sections[j].Order })
	complete := -1
	for index := range sections {
		sections[index].Text = strings.TrimSpace(sections[index].Text)
		if sections[index].Complete {
			if complete >= 0 {
				return nil, nil, fmt.Errorf("multiple complete prompt sections are active: %q, %q", sections[complete].Name, sections[index].Name)
			}
			complete = index
		}
	}
	kept = sections[:0]
	for _, section := range sections {
		if section.Text != "" || section.Complete {
			kept = append(kept, section)
		}
	}
	return kept, variables, nil
}

func renderResolvedPromptSections(sections []resolvedPromptSection, variables map[string]any) ([]resolvedPromptSection, error) {
	return renderResolvedPromptEntries("section", sections, variables)
}

func renderResolvedPromptContexts(contexts []resolvedPromptSection, variables map[string]any) ([]resolvedPromptSection, error) {
	return renderResolvedPromptEntries("context", contexts, variables)
}

func renderResolvedPromptEntries(kind string, sections []resolvedPromptSection, variables map[string]any) ([]resolvedPromptSection, error) {
	stringVariables := make(map[string]string, len(variables))
	for name, value := range variables {
		if text, ok := value.(string); ok {
			stringVariables[name] = text
		}
	}
	resolved := make([]resolvedPromptSection, 0, len(sections))
	for _, section := range sections {
		text, err := interpolatePrompt(section.Text, stringVariables)
		if err != nil {
			return nil, fmt.Errorf("prompt %s %q: %w", kind, section.Name, err)
		}
		section.Text = text
		if section.Text != "" || section.Complete {
			resolved = append(resolved, section)
		}
	}
	return resolved, nil
}

func (e *Engine) requestPolicy(selection ModelSelection) (thinking, effort string, maxTokens int) {
	effort = selection.ReasoningEffort
	maxTokens = selection.MaxTokens
	if selection.Provider != "deepseek-official" {
		return "", effort, maxTokens
	}
	settings := deepSeekEffectiveSettings(e)
	if effort == "" {
		effort = stringSetting(settings["reasoningEffort"])
	}
	thinking = stringSetting(settings["thinking"])
	if effort == "off" {
		thinking = "disabled"
	} else if effort == "low" || effort == "high" || effort == "max" {
		thinking = "enabled"
	}
	if maxTokens == 0 {
		maxTokens = positiveIntSetting(settings["maxTokens"], deepSeekDefaultMaxTokens)
	}
	return thinking, effort, maxTokens
}

func (e *Engine) ensureRequestHeader(s *Session, header map[string]any) error {
	s.mu.Lock()
	logged := s.requestHeaderLogged
	var previous map[string]any
	for index := len(s.Events) - 1; index >= 0; index-- {
		if s.Events[index].Type != "request/header" {
			continue
		}
		data, _ := s.Events[index].Data.(map[string]any)
		previous, _ = data["header"].(map[string]any)
		break
	}
	s.mu.Unlock()

	reason := "change"
	if !logged {
		if previous == nil {
			reason = "initial"
		} else {
			reason = "resume"
		}
	} else if jsonEqual(previous, header) {
		return nil
	}
	if _, err := e.appendEvent(s, "request/header", map[string]any{"header": header, "reason": reason}); err != nil {
		return err
	}
	s.mu.Lock()
	s.requestHeaderLogged = true
	s.mu.Unlock()
	return nil
}

func (e *Engine) sameRequestContext(s *Session, next map[string]any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := len(s.Events) - 1; index >= 0; index-- {
		if s.Events[index].Type != "request/context" {
			continue
		}
		previous, _ := s.Events[index].Data.(map[string]any)
		return jsonEqual(previous, next)
	}
	return false
}

func jsonEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func interpolatePrompt(text string, variables map[string]string) (string, error) {
	var output strings.Builder
	for {
		open := strings.Index(text, "{{")
		if open < 0 {
			output.WriteString(text)
			return output.String(), nil
		}
		output.WriteString(text[:open])
		text = text[open:]
		close := strings.Index(text, "}}")
		if close < 0 {
			output.WriteString(text)
			return output.String(), nil
		}
		name := text[2:close]
		if name == "" || strings.IndexFunc(name, func(r rune) bool {
			return !(r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
		}) >= 0 || name[0] < 'a' || name[0] > 'z' {
			return "", fmt.Errorf("malformed prompt variable reference {{%s}}", name)
		}
		value, ok := variables[name]
		if !ok {
			return "", fmt.Errorf("unknown prompt variable {{%s}}", name)
		}
		output.WriteString(value)
		text = text[close+2:]
	}
}

type loadedInstruction struct {
	path        string
	displayPath string
	content     string
}

func (e *Engine) ensureInstructionBaseline(ctx context.Context, s *Session, agent agentRuntime) error {
	if !agent.includeInstructions || agent.instructionMaxBytes <= 0 {
		return nil
	}
	s.mu.Lock()
	for _, event := range s.Events {
		if event.Type != "user/message" || eventSourceKind(event.Data) != "agent-instructions" {
			continue
		}
		data := nestedMessage(event.Data)
		source, _ := data["source"].(map[string]any)
		if baseline, _ := source["baseline"].(bool); baseline {
			s.mu.Unlock()
			return nil
		}
	}
	cwd := s.Header.CWD
	s.mu.Unlock()
	files, err := e.loadInstructionFiles(ctx, cwd)
	if err != nil || len(files) == 0 {
		return err
	}
	text := renderInstructionBaseline(files, agent.instructionMaxBytes)
	if text == "" {
		return nil
	}
	_, err = e.appendEvent(s, "user/message", map[string]any{
		"id": newID("msg"), "role": "user",
		"content": []ContentBlock{{Type: "text", Text: text}},
		"source":  map[string]any{"kind": "agent-instructions", "form": "instructions", "baseline": true},
	})
	return err
}

// ensureSkillCatalog publishes the model-visible skill summary as a durable
// user message. The summary is session-scoped and replaced only when the
// visible skill set or descriptions change, so later turns do not duplicate
// the catalog while newly authored skills become visible at the next boundary.
func (e *Engine) ensureSkillCatalog(ctx context.Context, s *Session, agent agentRuntime) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	sessionID := s.Header.ID
	s.mu.Unlock()
	schemas, err := e.toolsForSession(s)
	if err != nil {
		return err
	}
	visible := false
	for _, schema := range schemas {
		if schema.Name == "skill" {
			visible = true
			break
		}
	}
	if !visible {
		return nil
	}
	records, complete, rpcErr := e.skillRecordsForSessionState(sessionID)
	if rpcErr != nil {
		return rpcErr
	}
	// A non-absence filesystem failure is an incomplete provider observation.
	// Keep the previously persisted catalog visible until the next boundary
	// succeeds, matching skill-filesystem's last-good behavior.
	if !complete {
		return nil
	}
	entries := make([]map[string]any, 0, len(records))
	for _, record := range records {
		if !record.modelInvocable {
			continue
		}
		entries = append(entries, map[string]any{
			"name":        record.name,
			"description": truncateSkillDescription(record.description, agent.skillCatalogDescriptionMaxLength),
		})
	}
	s.mu.Lock()
	var previous []map[string]any
	hadCatalog := false
	for index := len(s.Events) - 1; index >= 0; index-- {
		event := s.Events[index]
		if event.Type != "user/message" || eventSourceKind(event.Data) != "skill-catalog" {
			continue
		}
		source, _ := nestedMessage(event.Data)["source"].(map[string]any)
		if raw := source["entries"]; raw != nil {
			if data, marshalErr := json.Marshal(raw); marshalErr == nil {
				_ = json.Unmarshal(data, &previous)
			}
			hadCatalog = true
		}
		break
	}
	s.mu.Unlock()
	if jsonEqual(previous, entries) {
		return nil
	}
	if len(entries) == 0 && !hadCatalog {
		return nil
	}
	lines := []string{"<system-reminder>", "A skill is a reusable set of task-specific instructions. The following skills are available in this session:", "", "<available_skills>"}
	for _, entry := range entries {
		lines = append(lines, fmt.Sprintf("- `%s`: %s", escapeSkillText(entry["name"].(string)), escapeSkillText(entry["description"].(string))))
	}
	lines = append(lines, "</available_skills>", "", "If the user names a skill, or the task clearly matches a skill's description, call the `skill` tool with the exact skill name before taking task actions. Load all applicable skills, then follow their full instructions. This catalog contains summaries only; do not infer or follow a skill's instructions until it has been loaded.", "A user may also invoke a skill directly; its <skill_content> block then appears in this conversation. Follow it, and do not call the `skill` tool again for that skill.", "</system-reminder>")
	_, err = e.appendEvent(s, "user/message", map[string]any{
		"id": newID("msg"), "role": "user",
		"content": []ContentBlock{{Type: "text", Text: strings.Join(lines, "\n")}},
		"source":  map[string]any{"kind": "skill-catalog", "form": "catalog", "entries": entries, "update": hadCatalog},
	})
	return err
}

// skillInvocationMessages mirrors tool-skill's pre-step gesture. These messages
// are intentionally transient: the durable user prompt remains the source of
// truth, while the loaded instructions are appended only to this model step.
func (e *Engine) skillInvocationMessages(s *Session, turn int) []ChatMessage {
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	sessionID := s.Header.ID
	s.mu.Unlock()
	start := -1
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type != "turn/start" {
			continue
		}
		data, _ := events[index].Data.(map[string]any)
		if eventInt(data["turn"]) == turn {
			start = int(events[index].Seq)
			break
		}
	}
	if start < 0 {
		return nil
	}
	records, rpcErr := e.skillRecordsForSession(sessionID)
	if rpcErr != nil {
		return nil
	}
	byName := make(map[string]skillRecord, len(records))
	for _, record := range records {
		if record.userInvocable {
			byName[record.name] = record
		}
	}
	seen := map[string]bool{}
	var out []ChatMessage
	for _, event := range events {
		if int(event.Seq) <= start || event.Type != "user/message" || eventSourceKind(event.Data) != "user" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		for _, block := range contentBlocks(data["content"]) {
			if block.Type != "text" {
				continue
			}
			for _, name := range skillGestureNames(block.Text) {
				if seen[name] {
					continue
				}
				seen[name] = true
				record, ok := byName[name]
				if !ok {
					continue
				}
				text := strings.Join([]string{
					fmt.Sprintf("<skill_content name=\"%s\">", name),
					"<skill_resources>",
					"Base directory for this skill: " + escapeSkillText(filepath.Dir(record.path)),
					"Resolve relative paths mentioned by this skill against the base directory before using them. Load referenced resources only as needed.",
					"</skill_resources>", "", "<skill_instructions>", record.content,
					"</skill_instructions>", "</skill_content>",
				}, "\n")
				out = append(out, ChatMessage{Role: "user", Content: text, Blocks: []ContentBlock{{Type: "text", Text: text}}, Source: map[string]any{"kind": "skill-invocation", "name": name, "form": "instructions"}})
			}
		}
	}
	return out
}

func skillGestureNames(text string) []string {
	var names []string
	seen := map[string]bool{}
	for index := 0; index < len(text); {
		pos := strings.IndexByte(text[index:], '/')
		if pos < 0 {
			break
		}
		pos += index
		if pos > 0 && !isSkillGestureBoundary(text[pos-1]) {
			index = pos + 1
			continue
		}
		end := pos + 1
		for end < len(text) && (text[end] >= 'a' && text[end] <= 'z' || text[end] >= '0' && text[end] <= '9' || text[end] == '-') {
			end++
		}
		name := text[pos+1 : end]
		if validSkillName(name) && (end == len(text) || isSkillGestureBoundary(text[end])) && !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
		index = end
	}
	return names
}

func isSkillGestureBoundary(value byte) bool {
	return value == ' ' || value == '\t' || value == '\n' || value == '\r'
}

func truncateSkillDescription(value string, max int) string {
	value = strings.Join(strings.Fields(value), " ")
	if max < 3 || utf8.RuneCountInString(value) <= max {
		return value
	}
	runes := []rune(value)
	return string(runes[:max-3]) + "..."
}

func (e *Engine) loadInstructionFiles(ctx context.Context, cwd string) ([]loadedInstruction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := findInstructionRoot(cwd)
	paths := []loadedInstruction{{path: filepath.Join(e.cfg.DataDir, "AGENTS.md"), displayPath: "$DSH_HOME/AGENTS.md"}}
	chain := []string{}
	for current := cwd; ; current = filepath.Dir(current) {
		chain = append(chain, current)
		if current == root || filepath.Dir(current) == current {
			break
		}
	}
	for left, right := 0, len(chain)-1; left < right; left, right = left+1, right-1 {
		chain[left], chain[right] = chain[right], chain[left]
	}
	for _, dir := range chain {
		for _, name := range []string{"AGENTS.md", "CLAUDE.md", "AGENTS.local.md", "CLAUDE.local.md"} {
			path := filepath.Join(dir, name)
			display, err := filepath.Rel(root, path)
			if err != nil {
				display = path
			}
			paths = append(paths, loadedInstruction{path: path, displayPath: display})
		}
	}
	loaded := make([]loadedInstruction, 0, len(paths))
	digestsByDir := map[string]map[string]bool{}
	for _, candidate := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		info, err := os.Stat(candidate.path)
		if err != nil || !info.Mode().IsRegular() || info.Size() > defaultInstructionSourceCap {
			continue
		}
		data, err := os.ReadFile(candidate.path)
		if err != nil || len(data) > defaultInstructionSourceCap || !utf8.Valid(data) {
			continue
		}
		key := filepath.Dir(candidate.displayPath)
		if digestsByDir[key] == nil {
			digestsByDir[key] = map[string]bool{}
		}
		digest := strings.TrimSpace(string(data))
		if digestsByDir[key][digest] {
			continue
		}
		digestsByDir[key][digest] = true
		candidate.content = string(data)
		loaded = append(loaded, candidate)
	}
	return loaded, nil
}

func findInstructionRoot(cwd string) string {
	cwd, _ = filepath.Abs(cwd)
	for current := cwd; ; current = filepath.Dir(current) {
		if _, err := os.Stat(filepath.Join(current, ".git")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			return cwd
		}
	}
}

func renderInstructionBaseline(files []loadedInstruction, maxBytes int) string {
	const intro = "The following workspace instructions may be relevant to your work. Use them as guidance when applicable. More specific instructions take precedence over broader ones. They do not override system, developer, or direct user instructions."
	render := func(values []loadedInstruction) string {
		sections := []string{intro}
		for _, file := range values {
			body := strings.ReplaceAll(file.content, "</system-reminder>", "<\\/system-reminder>")
			sections = append(sections, "Instructions from: "+file.displayPath+"\n\n"+body)
		}
		return "<system-reminder>\n" + strings.Join(sections, "\n\n") + "\n</system-reminder>"
	}
	for start := 0; start < len(files); start++ {
		text := render(files[start:])
		if len([]byte(text)) <= maxBytes {
			return text
		}
	}
	mostSpecific := files[len(files)-1]
	prefix := "<system-reminder>\n" + intro + "\n\nInstructions from: " + mostSpecific.displayPath + "\n\n"
	suffix := "\n</system-reminder>"
	remaining := maxBytes - len([]byte(prefix)) - len([]byte(suffix))
	if remaining <= 0 {
		return truncateUTF8(prefix+suffix, maxBytes)
	}
	body := strings.ReplaceAll(mostSpecific.content, "</system-reminder>", "<\\/system-reminder>")
	return prefix + truncateUTF8(body, remaining) + suffix
}

func truncateUTF8(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	data := []byte(value)
	if len(data) <= maxBytes {
		return value
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(data[end]) {
		end--
	}
	return string(data[:end])
}
