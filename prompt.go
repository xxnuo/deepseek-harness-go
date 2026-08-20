package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const (
	harnessIdentity             = "You are an AI agent powered by DeepSeek Harness."
	deliverableFilePrompt       = "When you successfully create or modify files, mention the primary outputs in your final response. To make those and any other changed-file references clickable in Web, format them as Markdown inline code using the exact file-tool path, or a basename when unique among the files changed in that turn."
	defaultCodingPersona        = "You are a coding agent powered by the {{model}} model. Your working directory is {{cwd}}."
	defaultInstructionMaxBytes  = 65536
	defaultInstructionSourceCap = 1 << 20
)

type agentRuntime struct {
	persona               string
	completePersona       bool
	includeInstructions   bool
	instructionMaxBytes   int
	includeRuntimeContext bool
	toolNames             map[string]bool
	persistentBash        bool
	persistentBashDesc    string
	toolPresentation      string
}

func defaultAgentRuntime(config Config) agentRuntime {
	return agentRuntime{
		persona: config.Persona, includeInstructions: true,
		instructionMaxBytes: config.InstructionMaxBytes, includeRuntimeContext: true,
		toolPresentation: config.ToolPresentation,
	}
}

func (e *Engine) runtimeForSession(s *Session) (agentRuntime, error) {
	s.mu.Lock()
	preset := sessionAgentPreset(s.Header, s.Events)
	personaOverride := s.personaOverride
	s.mu.Unlock()
	if preset == "" {
		runtimeConfig := defaultAgentRuntime(e.cfg)
		if personaOverride != "" {
			runtimeConfig.persona = personaOverride
		}
		return runtimeConfig, nil
	}
	row, ok := e.findPreset(preset)
	if !ok {
		return agentRuntime{}, fmt.Errorf("agent-preset-not-found: %s", preset)
	}
	if row.broken != "" {
		return agentRuntime{}, fmt.Errorf("agent-preset-invalid: %s", row.broken)
	}
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(row.content), &document); err != nil {
		return agentRuntime{}, fmt.Errorf("agent-preset-invalid: %w", err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.SequenceNode {
		return agentRuntime{}, errors.New("agent-preset-invalid: composition must be a YAML array")
	}
	runtimeConfig := agentRuntime{
		persona: e.cfg.Persona, includeRuntimeContext: true,
		instructionMaxBytes: e.cfg.InstructionMaxBytes,
		toolNames:           map[string]bool{}, toolPresentation: e.cfg.ToolPresentation,
	}
	if err := walkPresetRows(document.Content[0], func(entry *yaml.Node) error {
		name := yamlScalar(yamlMapValue(entry, "name"))
		config := yamlMapValue(entry, "config")
		switch name {
		case "@deepseek-ai/dsh-persona":
			runtimeConfig.persona = yamlScalar(yamlMapValue(config, "text"))
			runtimeConfig.completePersona = yamlNodeBool(yamlMapValue(config, "complete"), false)
			runtimeConfig.includeRuntimeContext = yamlNodeBool(yamlMapValue(config, "includeRuntimeContext"), true)
		case "@deepseek-ai/dsh-agent-instructions":
			runtimeConfig.includeInstructions = true
			if max := yamlNodeInt(yamlMapValue(config, "maxBytes")); max >= 0 {
				runtimeConfig.instructionMaxBytes = max
			}
		case "@deepseek-ai/dsh-tool-bash-persistent":
			runtimeConfig.persistentBash = true
			runtimeConfig.persistentBashDesc = yamlScalar(yamlMapValue(config, "description"))
		case "@deepseek-ai/dsh-agent-tool-presentation":
			runtimeConfig.toolPresentation = yamlScalar(yamlMapValue(config, "mode"))
		}
		for _, toolName := range presetToolNames(name, config) {
			runtimeConfig.toolNames[toolName] = true
		}
		return nil
	}); err != nil {
		return agentRuntime{}, err
	}
	if personaOverride != "" {
		runtimeConfig.persona = personaOverride
		runtimeConfig.completePersona = false
	}
	return runtimeConfig, nil
}

func presetToolNames(plugin string, config *yaml.Node) []string {
	switch plugin {
	case "@deepseek-ai/dsh-tool-bash", "@deepseek-ai/dsh-tool-bash-persistent":
		return []string{"bash"}
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
	case "@deepseek-ai/dsh-tool-subagent-control":
		return []string{"send_message", "interrupt_agent"}
	case "@deepseek-ai/dsh-tool-subagent-control/list-agents":
		return []string{"list_agents"}
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
		tools := []string{"web_search"}
		if yamlNodeBool(yamlMapValue(config, "fetch"), true) {
			tools = append(tools, "web_fetch")
		}
		return tools
	case "@deepseek-ai/dsh-tool-workflow":
		return []string{"workflow"}
	case "@deepseek-ai/dsh-tool-ralph":
		return []string{"ralph"}
	case "@deepseek-ai/dsh-tool-cordis":
		return append([]string(nil), dynamicCordisToolNames...)
	case "@deepseek-ai/dsh-tool-session-query":
		return append([]string(nil), sessionQueryToolNames...)
	case "@deepseek-ai/dsh-tool-lsp":
		return []string{"lsp"}
	case "@deepseek-ai/dsh-tool-terminal":
		return []string{"terminal_open", "terminal_send", "terminal_read", "terminal_signal", "terminal_close", "terminal_list"}
	}
	return nil
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
		{Name: "deployment:persona", Order: 0, Text: agent.persona, Complete: agent.completePersona},
	}
	add := func(name string, order float64, text string) {
		sections = append(sections, resolvedPromptSection{Name: name, Order: order, Text: text})
	}
	if e.clientPluginComposed("@deepseek-ai/dsh-client-ui-deliverables") {
		add("ui:deliverable-file-references", 190, deliverableFilePrompt)
	}
	if agent.toolNames["workflow"] {
		add("tool:workflow", 100, "Use the workflow tool ONLY when the user explicitly asks for a workflow or for large multi-agent orchestration. For one or two delegations, prefer plain subagent calls.")
	}
	if agent.toolNames["ralph"] {
		add("tool:ralph", 101, "Use the ralph tool ONLY when the direct human explicitly asks for a Ralph loop or fresh-agent iterative execution. Completion and blockers are worker reports, not independent evaluation.")
	}
	if agent.toolNames["web_search"] {
		guidance := "Use the web_search tool to discover current information on the web. It returns an optional answer plus a list of source URLs. Use the returned source snippets when available, and cite the relevant URLs as markdown links."
		if agent.toolNames["web_fetch"] {
			guidance = "Use the web_search tool to discover current information on the web. It returns an optional answer plus a list of source URLs. Follow up with web_fetch when you need the full content of a specific result, and cite the relevant URLs as markdown links."
		}
		add("tool:web-search", 110, guidance)
	}
	if agent.toolNames["web_fetch"] {
		add("tool:web-fetch", 111, "Use the web_fetch tool to retrieve the content of a specific HTTP(S) URL. It returns the page content decoded to text. Cite the URL as a markdown link when you use its content.")
	}
	if agent.toolNames["lsp"] {
		add("tool:lsp", 120, "Use search/read for ordinary navigation. Use lsp when textual matches are ambiguous or before a change requires precise definitions, implementations, or references. Positions are one-based line and character (UTF-16) at the cursor; an off-symbol position may return no results. findReferences always includes the declaration.")
	}
	if agent.toolNames == nil || agent.toolNames["terminal_open"] {
		add("tool:terminal", 130, "Use a terminal session only when work needs persistent terminal state or interactive stdin; prefer shell/read/write/edit for bounded one-shot operations. Track every terminal session id and close sessions that no longer matter. An inferred_idle or timeout result does not prove the foreground command exited.")
	}
	if agent.toolPresentation == "code" || agent.toolPresentation == "both" {
		add("tools:code-mode", 200, e.codeModePrompt(s, agent))
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
