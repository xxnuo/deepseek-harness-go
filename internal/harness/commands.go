package harness

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var slashCommandPattern = regexp.MustCompile(`^/([a-z][a-z0-9_-]*)(?:$|[\t\n\r ])`)

type commandDescriptor struct {
	Name        string                  `json:"name"`
	Description string                  `json:"description"`
	Input       *commandInputDescriptor `json:"input,omitempty"`
}

type commandInputDescriptor struct {
	Hint   string `json:"hint"`
	Images bool   `json:"images,omitempty"`
}

type commandInvocation struct {
	CommandID   string
	Session     *Session
	RawInput    string
	Attachments []ContentBlock
}

type commandDefinition struct {
	commandDescriptor
	RecordInput bool
	Handler     func(context.Context, commandInvocation) (CommandResult, error)
}

type commandExecution struct {
	CommandID string         `json:"commandId"`
	Result    *CommandResult `json:"result"`
}

func parseSlashCommand(line string) (name, rawInput string, ok bool) {
	match := slashCommandPattern.FindStringSubmatchIndex(line)
	if match == nil {
		return "", "", false
	}
	return line[match[2]:match[3]], line[match[3]:], true
}

func commandCatalog() []commandDescriptor {
	return []commandDescriptor{
		{Name: "clear", Description: "Clear the current Session title"},
		{Name: "compact", Description: "Compact older conversation history"},
		{Name: "export", Description: "Download this Session log as a ZIP archive"},
		{Name: "feedback", Description: "record feedback about this session", Input: &commandInputDescriptor{Hint: "<text>"}},
		{Name: "goal", Description: "set or view the goal for a long-running task", Input: &commandInputDescriptor{Hint: "[<objective>|clear|edit <objective>|pause|resume]", Images: true}},
		{Name: "permission", Description: "Switch the permission preset (sandbox mode + approval policy)", Input: &commandInputDescriptor{Hint: "<preset>"}},
		{Name: "plan", Description: "Enter or leave plan mode", Input: &commandInputDescriptor{Hint: "[off|message]", Images: true}},
	}
}

func (e *Engine) commandCatalogForSession(s *Session) ([]commandDescriptor, error) {
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		return nil, err
	}
	catalog := commandCatalog()
	if runtimeConfig.compactCommandEnabled {
		return catalog, nil
	}
	filtered := make([]commandDescriptor, 0, len(catalog)-1)
	for _, command := range catalog {
		if command.Name != "compact" {
			filtered = append(filtered, command)
		}
	}
	return filtered, nil
}

func (e *Engine) commandDefinition(name string) (commandDefinition, bool) {
	definitions := []commandDefinition{
		{commandDescriptor: commandCatalog()[0], RecordInput: true, Handler: e.commandClear},
		{commandDescriptor: commandCatalog()[1], RecordInput: true, Handler: e.commandCompact},
		{commandDescriptor: commandCatalog()[2], RecordInput: true, Handler: e.commandExport},
		{commandDescriptor: commandCatalog()[3], Handler: e.commandFeedback},
		{commandDescriptor: commandCatalog()[4], RecordInput: true, Handler: e.commandGoal},
		{commandDescriptor: commandCatalog()[5], RecordInput: true, Handler: e.commandPermission},
		{commandDescriptor: commandCatalog()[6], RecordInput: true, Handler: e.commandPlan},
	}
	for _, definition := range definitions {
		if definition.Name == name {
			return definition, true
		}
	}
	return commandDefinition{}, false
}

func (e *Engine) executeCommand(ctx context.Context, s *Session, line string, images []EncodedImageAttachment) (*commandExecution, bool, error) {
	name, rawInput, ok := parseSlashCommand(line)
	if !ok {
		return nil, false, nil
	}
	definition, ok := e.commandDefinition(name)
	if !ok {
		return nil, false, nil
	}
	if name == "compact" {
		runtimeConfig, runtimeErr := e.runtimeForSession(s)
		if runtimeErr != nil {
			return nil, true, runtimeErr
		}
		if !runtimeConfig.compactCommandEnabled {
			return nil, false, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	s.mu.Lock()
	draining := s.draining
	s.mu.Unlock()
	if draining {
		return nil, true, fmt.Errorf("session-draining: session %q is being released", s.Header.ID)
	}
	commandID := newID("cmd")
	run := map[string]any{"commandId": commandID, "name": name, "source": map[string]any{"kind": "user"}}
	if definition.RecordInput {
		run["args"] = rawInput
	}
	if _, err := e.appendEvent(s, "command/run", run); err != nil {
		return nil, true, err
	}
	attachments := []ContentBlock(nil)
	if len(images) > 0 {
		if definition.Input == nil || !definition.Input.Images {
			result := CommandResult{Kind: "error", Text: fmt.Sprintf("/%s does not accept image attachments", name)}
			if _, err := e.appendEvent(s, "command/done", map[string]any{"commandId": commandID, "kind": result.Kind, "text": result.Text}); err != nil {
				return nil, true, err
			}
			return &commandExecution{CommandID: commandID, Result: &result}, true, nil
		}
		parts := make([]PromptContentPart, len(images))
		for index, image := range images {
			parts[index] = PromptContentPart{Type: "image", MediaType: image.MediaType, Data: image.Data, Name: image.Name}
		}
		var admissionErr error
		attachments, admissionErr = e.durablePromptContentContext(ctx, parts)
		if admissionErr != nil {
			result := CommandResult{Kind: "error", Text: admissionErr.Error()}
			if _, err := e.appendEvent(s, "command/done", map[string]any{"commandId": commandID, "kind": result.Kind, "text": result.Text}); err != nil {
				return nil, true, err
			}
			return &commandExecution{CommandID: commandID, Result: &result}, true, nil
		}
		if err := ctx.Err(); err != nil {
			_, _ = e.appendEvent(s, "command/done", map[string]any{"commandId": commandID, "kind": "error", "text": err.Error()})
			return nil, true, err
		}
	}
	result, handlerErr := definition.Handler(ctx, commandInvocation{
		CommandID: commandID, Session: s, RawInput: rawInput, Attachments: attachments,
	})
	if handlerErr != nil {
		_, _ = e.appendEvent(s, "command/done", map[string]any{"commandId": commandID, "kind": "error", "text": handlerErr.Error()})
		return nil, true, handlerErr
	}
	if result.Kind != "success" && result.Kind != "error" {
		handlerErr = fmt.Errorf("command %q returned invalid result kind %q", name, result.Kind)
		_, _ = e.appendEvent(s, "command/done", map[string]any{"commandId": commandID, "kind": "error", "text": handlerErr.Error()})
		return nil, true, handlerErr
	}
	done := map[string]any{"commandId": commandID, "kind": result.Kind}
	if result.Text != "" {
		done["text"] = result.Text
	}
	if result.SourceEventSeqSet || result.SourceEventSeq > 0 {
		done["sourceEventSeq"] = result.SourceEventSeq
	}
	if _, err := e.appendEvent(s, "command/done", done); err != nil {
		return nil, true, err
	}
	return &commandExecution{CommandID: commandID, Result: &result}, true, nil
}

func (e *Engine) runCommand(s *Session, raw string) (PromptResult, error) {
	execution, admitted, err := e.executeCommand(context.Background(), s, raw, nil)
	if err != nil {
		return PromptResult{}, err
	}
	if !admitted {
		return PromptResult{}, fmt.Errorf("unknown-command: %s", raw)
	}
	return PromptResult{Accepted: true, Command: execution.Result}, nil
}

func (e *Engine) commandClear(_ context.Context, invocation commandInvocation) (CommandResult, error) {
	if strings.TrimSpace(invocation.RawInput) != "" {
		return CommandResult{Kind: "error", Text: "Usage: /clear (no arguments)"}, nil
	}
	invocation.Session.mu.Lock()
	invocation.Session.Title = ""
	invocation.Session.mu.Unlock()
	return CommandResult{Kind: "success", Text: "cleared"}, nil
}

func (e *Engine) commandExport(_ context.Context, invocation commandInvocation) (CommandResult, error) {
	if strings.TrimSpace(invocation.RawInput) != "" {
		return CommandResult{Kind: "error", Text: "The Web /export command does not accept a path."}, nil
	}
	return CommandResult{Kind: "success", Text: "Session log download requested."}, nil
}

const compactionInstruction = `You are now acting as a compaction engine for this AI coding assistant. Condense the conversation ABOVE into a structured checkpoint that lets another model resume the work with no loss of essential context.

Output concise Markdown with these headings, in order: Primary Request and Intent, Key Technical Concepts, Files and Code, Errors and Fixes, Pending Jobs, Current Work, Next Step, Critical Context. Preserve exact paths, commands, errors, identifiers, values, and user instructions. Output only the checkpoint text.`

const compactCheckpointPreamble = "This is an automatically generated checkpoint condensing an earlier span of the conversation to free up context. Treat the captured context as established background and build on it without restating it. Continue the task directly from the messages that follow, without acknowledging this checkpoint."

func (e *Engine) commandCompact(ctx context.Context, invocation commandInvocation) (CommandResult, error) {
	if strings.TrimSpace(invocation.RawInput) != "" {
		return CommandResult{Kind: "error", Text: "Usage: /compact (no arguments)"}, nil
	}
	s := invocation.Session
	s.mu.Lock()
	if s.Running {
		s.mu.Unlock()
		return CommandResult{Kind: "error", Text: "Compaction is unavailable because this process has an active compaction, or the agent is not idle."}, nil
	}
	selection := s.Model
	s.mu.Unlock()
	system := ""
	var tools []ToolSchema
	if routed, routedSystem, routedTools, ok := routedCompactionRequest(s); ok {
		selection = routed
		system, tools = routedSystem, routedTools
	}
	runtimeConfig, runtimeErr := e.runtimeForSession(s)
	if runtimeErr != nil {
		return CommandResult{}, runtimeErr
	}
	if !runtimeConfig.compactionEnabled {
		return CommandResult{Kind: "error", Text: "Compaction is unavailable because this agent preset does not mount a compaction service."}, nil
	}
	result, err := e.compactSession(ctx, s, compactRequest{selection: selection, system: system, tools: tools, sourceCommandID: invocation.CommandID})
	if errors.Is(err, errNoCompactableHistory) {
		return CommandResult{Kind: "success", Text: "No compactable history yet."}, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return CommandResult{Kind: "error", Text: "Compaction cancelled."}, nil
		}
		return CommandResult{Kind: "error", Text: "Compaction could not produce a useful summary. The conversation is unchanged; the attempt is recorded in the session log."}, nil
	}
	return CommandResult{
		Kind: "success", Text: fmt.Sprintf("Compacted %d history items (~%d tokens).", len(result.shadowedSeqs), result.shadowedTokenCount),
		SourceEventSeq: result.summarySeq, SourceEventSeqSet: true,
	}, nil
}

func compactionOpen(events []Event) bool {
	for index := len(events) - 1; index >= 0; index-- {
		switch events[index].Type {
		case "compaction/end", "session/end-seed":
			return false
		case "compaction/start":
			return true
		}
	}
	return false
}

func surfaceSpanPresent(surface []Event, seqs []int) bool {
	if len(seqs) == 0 || len(surface) < len(seqs) {
		return false
	}
	for start := 0; start+len(seqs) <= len(surface); start++ {
		matched := true
		for offset, seq := range seqs {
			if int(surface[start+offset].Seq) != seq {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func toolMessagesBalanced(messages []ChatMessage) bool {
	open := map[string]bool{}
	for _, message := range messages {
		for _, call := range message.ToolCalls {
			open[call.ID] = true
		}
		if message.Role == "tool" {
			if !open[message.ToolCallID] {
				return false
			}
			delete(open, message.ToolCallID)
		}
	}
	return len(open) == 0
}

func estimateMessagesTokens(messages []ChatMessage) int {
	characters := 0
	for _, message := range messages {
		characters += len(message.Content) + len(message.Reasoning)
		for _, call := range message.ToolCalls {
			characters += len(call.Name) + len(call.Arguments)
		}
	}
	if characters == 0 {
		return 0
	}
	return (characters + 3) / 4
}

func (e *Engine) commandFeedback(_ context.Context, invocation commandInvocation) (CommandResult, error) {
	text := strings.TrimSpace(invocation.RawInput)
	if text == "" {
		return CommandResult{Kind: "error", Text: "Feedback text is required. Usage: /feedback <text>"}, nil
	}
	if _, err := e.appendEvent(invocation.Session, "feedback/record", map[string]any{"text": text}); err != nil {
		return CommandResult{}, err
	}
	disclosure := "Session sharing is not configured."
	if sharing, ok := e.SessionTelemetrySharing(); ok {
		switch sharing {
		case SessionTelemetrySharingFull:
			disclosure = "Session sharing is enabled."
		case SessionTelemetrySharingFeedbackOnly:
			disclosure = "Session sharing is feedback-gated; recording feedback releases the session prefix for sharing."
		case SessionTelemetrySharingDisabled:
			disclosure = "Session sharing is disabled."
		default:
			return CommandResult{}, fmt.Errorf("command-feedback: unsupported sharing status %q", sharing)
		}
	}
	userID := anonymousUserID(e.cfg.DataDir)
	return CommandResult{Kind: "success", Text: fmt.Sprintf("Feedback recorded for session %s\nAnonymous user: %s. %s", invocation.Session.Header.ID, userID, disclosure)}, nil
}

const goalCommandUsage = "Usage: /goal [<objective>|clear|edit <objective>|pause|resume]"

func (e *Engine) commandGoal(_ context.Context, invocation commandInvocation) (CommandResult, error) {
	input := strings.TrimSpace(invocation.RawInput)
	control := strings.ToLower(input)
	imageObjective := input != "" && control != "clear" && control != "pause" && control != "resume" &&
		control != "edit"
	if len(invocation.Attachments) > 0 && !imageObjective {
		return CommandResult{
			Kind: "error",
			Text: "Image attachments only accompany a goal objective: /goal <objective> or /goal edit <objective>.",
		}, nil
	}
	current, err := e.GetGoal(invocation.Session.Header.ID)
	if err != nil {
		return CommandResult{}, err
	}
	if input == "" {
		if current == nil {
			return CommandResult{Kind: "success", Text: "No goal is currently set.\n" + goalCommandUsage}, nil
		}
		return renderGoalCommand("Goal", current), nil
	}
	operation, objective := "create", input
	switch {
	case control == "clear":
		if current == nil {
			return CommandResult{Kind: "success", Text: "No goal to clear."}, nil
		}
		_, err = e.GoalMutation(invocation.Session.Header.ID, "clear", "", current.Revision, 0)
		if err != nil {
			return goalCommandStateError(), nil
		}
		return CommandResult{Kind: "success", Text: "Goal cleared."}, nil
	case control == "pause", control == "resume":
		if current == nil {
			return CommandResult{Kind: "error", Text: fmt.Sprintf("No goal is currently set; /goal %s requires one. %s", control, goalCommandUsage)}, nil
		}
		operation = control
		objective = ""
	case control == "edit":
		return CommandResult{Kind: "error", Text: "Goal editing requires a replacement objective.\n" + goalCommandUsage}, nil
	case strings.HasPrefix(control, "edit ") || strings.HasPrefix(control, "edit\t") || strings.HasPrefix(control, "edit\n") || strings.HasPrefix(control, "edit\r"):
		objective = strings.TrimSpace(input[4:])
		if objective == "" {
			return CommandResult{Kind: "error", Text: "Goal editing requires a replacement objective.\n" + goalCommandUsage}, nil
		}
		if current == nil {
			return CommandResult{Kind: "error", Text: "No goal is currently set; /goal edit requires one. " + goalCommandUsage}, nil
		}
		if current.Phase == "complete" {
			operation = "create"
		} else {
			operation = "edit"
		}
	default:
		if current != nil && current.Phase != "complete" {
			return CommandResult{Kind: "error", Text: fmt.Sprintf("A goal is already %s. Use /goal edit <objective> to change it or /goal clear before replacing it.", current.Phase)}, nil
		}
	}
	revision := 0
	if current != nil {
		revision = current.Revision
	}
	if _, err := e.GoalMutation(invocation.Session.Header.ID, operation, objective, revision, 0); err != nil {
		return goalCommandStateError(), nil
	}
	if len(invocation.Attachments) > 0 && (operation == "create" || operation == "edit") {
		content := append([]ContentBlock(nil), invocation.Attachments...)
		content = append(content, ContentBlock{Type: "text", Text: "Reference images for the goal objective."})
		if err := e.queueCommandContent(invocation.Session, content); err != nil {
			return CommandResult{}, err
		}
	}
	next, err := e.GetGoal(invocation.Session.Header.ID)
	if err != nil || next == nil {
		if err == nil {
			err = errors.New("goal mutation did not produce a goal")
		}
		return CommandResult{}, err
	}
	title := map[string]string{"create": "Goal created", "edit": "Goal updated", "pause": "Goal paused", "resume": "Goal resumed"}[operation]
	return renderGoalCommand(title, next), nil
}

func goalCommandStateError() CommandResult {
	return CommandResult{Kind: "error", Text: "The goal command is not valid for the current state. Run /goal to view available commands."}
}

func renderGoalCommand(title string, goal *GoalView) CommandResult {
	commands := "/goal edit <objective>, /goal resume, /goal clear"
	if goal.Phase == "active" && goal.Activation == "armed" {
		commands = "/goal edit <objective>, /goal pause, /goal clear"
	} else if goal.Phase == "complete" {
		commands = "/goal <objective>, /goal clear"
	}
	lines := []string{title, "Status: " + goal.Phase}
	if goal.Phase == "blocked" && goal.BlockedReason != nil {
		lines = append(lines, fmt.Sprintf("Blocker: %s: %s", goal.BlockedReason.Code, goal.BlockedReason.Message))
	}
	lines = append(lines,
		"Objective: "+goal.Objective,
		fmt.Sprintf("Rounds: %d/%d", goal.RoundsStarted, goal.MaxGoalRounds),
		"Activation: "+goal.Activation,
		"",
		"Commands: "+commands,
	)
	return CommandResult{Kind: "success", Text: strings.Join(lines, "\n")}
}

type commandPermissionPreset struct{ sandbox, approval string }

var commandPermissionPresets = map[string]commandPermissionPreset{
	"read-only":          {sandbox: "read-only", approval: "ask"},
	"workspace-write":    {sandbox: "workspace-write", approval: "ask"},
	"danger-full-access": {sandbox: "danger-full-access", approval: "never"},
}

func permissionPresetNames() []string {
	return []string{"read-only", "workspace-write", "danger-full-access"}
}

func currentPermissionPreset(events []Event) string {
	current := "workspace-write"
	if configured := strings.TrimSpace(os.Getenv("DSH_PERMISSION_MODE")); commandPermissionPresets[configured] != (commandPermissionPreset{}) {
		current = configured
	}
	for _, event := range events {
		if event.Type != "permission/preset" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		if value, ok := data["preset"].(string); ok {
			current = value
		}
	}
	return current
}

func (e *Engine) commandPermission(_ context.Context, invocation commandInvocation) (CommandResult, error) {
	invocation.Session.mu.Lock()
	events := append([]Event(nil), invocation.Session.Events...)
	invocation.Session.mu.Unlock()
	available := strings.Join(permissionPresetNames(), ", ")
	name := strings.TrimSpace(invocation.RawInput)
	if name == "" {
		return CommandResult{Kind: "success", Text: fmt.Sprintf("current preset %s (available: %s)", currentPermissionPreset(events), available)}, nil
	}
	spec, ok := commandPermissionPresets[name]
	if !ok {
		return CommandResult{Kind: "error", Text: fmt.Sprintf("unknown preset %q (available: %s)", name, available)}, nil
	}
	currentSandbox := effectiveEventString(events, "sandbox/mode", "mode", "workspace-write")
	if currentSandbox != spec.sandbox && e.HasTerminalActivity(invocation.Session.Header.ID) {
		return CommandResult{Kind: "error", Text: fmt.Sprintf(
			"cannot change sandbox mode from %q to %q while persistent terminal sessions are open or being created; wait for creation to settle and close them first",
			currentSandbox, spec.sandbox,
		)}, nil
	}
	if currentPermissionPreset(events) != name {
		if _, err := e.appendEvent(invocation.Session, "permission/preset", map[string]any{"preset": name}); err != nil {
			return CommandResult{}, err
		}
	}
	if currentSandbox != spec.sandbox {
		if _, err := e.appendEvent(invocation.Session, "sandbox/mode", map[string]any{"mode": spec.sandbox}); err != nil {
			return CommandResult{}, err
		}
	}
	if effectiveEventString(events, "approval/policy", "policy", "ask") != spec.approval {
		if _, err := e.appendEvent(invocation.Session, "approval/policy", map[string]any{"policy": spec.approval}); err != nil {
			return CommandResult{}, err
		}
	}
	return CommandResult{Kind: "success", Text: "preset " + name}, nil
}

func effectiveEventString(events []Event, eventType, field, fallback string) string {
	value := fallback
	for _, event := range events {
		if event.Type != eventType {
			continue
		}
		data, _ := event.Data.(map[string]any)
		if next, ok := data[field].(string); ok {
			value = next
		}
	}
	return value
}

func planModeActive(events []Event) bool {
	active := false
	for _, event := range events {
		if event.Type != "plan/mode" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		if next, ok := data["active"].(bool); ok {
			active = next
		}
	}
	return active
}

func (e *Engine) commandPlan(_ context.Context, invocation commandInvocation) (CommandResult, error) {
	message := strings.TrimSpace(invocation.RawInput)
	if message == "off" && len(invocation.Attachments) > 0 {
		return CommandResult{Kind: "error", Text: "Image attachments cannot accompany /plan off."}, nil
	}
	wanted := message != "off"
	outcome, err := e.setPlanMode(invocation.Session, wanted, true)
	if err != nil {
		return CommandResult{}, err
	}
	if wanted && (message != "" || len(invocation.Attachments) > 0) {
		content := append([]ContentBlock(nil), invocation.Attachments...)
		if message != "" {
			content = append(content, ContentBlock{Type: "text", Text: message})
		}
		if err := e.queueCommandContent(invocation.Session, content); err != nil {
			return CommandResult{}, err
		}
	}
	if !wanted {
		switch outcome {
		case planModeCommitted:
			return CommandResult{Kind: "success", Text: "Plan mode off."}, nil
		case planModeQueued:
			return CommandResult{Kind: "success", Text: "Leaving plan mode (applies from the next step)."}, nil
		case planModeCancelled:
			return CommandResult{Kind: "success", Text: "Plan mode entry cancelled."}, nil
		case planModeNoop:
			invocation.Session.mu.Lock()
			active := planModeActive(invocation.Session.Events)
			invocation.Session.mu.Unlock()
			if active {
				return CommandResult{Kind: "success", Text: "Leaving plan mode (applies from the next step)."}, nil
			}
			return CommandResult{Kind: "success", Text: "Plan mode is already inactive."}, nil
		}
	}
	if outcome == planModeCommitted {
		return CommandResult{Kind: "success", Text: "Plan mode on. Use /plan off to leave."}, nil
	}
	return CommandResult{Kind: "success", Text: "Entering plan mode (applies from the next step). Use /plan off to leave."}, nil
}

func (e *Engine) queueCommandMessage(s *Session, text string) error {
	return e.queueCommandContent(s, []ContentBlock{{Type: "text", Text: text}})
}

func (e *Engine) queueCommandContent(s *Session, content []ContentBlock) error {
	content = cloneSessionReferenceContent(content)
	job := &queuedPrompt{
		id: newID("msg"), text: strings.TrimSpace(blockText(content)), content: content,
		source: map[string]any{"kind": "user"},
	}
	s.mu.Lock()
	target := "next-turn"
	queue := &s.pending
	if s.Running {
		target = "next-step"
		queue = &s.steering
	}
	event, err := appendEventLocked(s, "agent/inbox/spliced", map[string]any{
		"target": target, "start": len(*queue), "inserted": []any{job.message()},
	}, nil, nil, false)
	if err == nil {
		*queue = append(*queue, job)
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	e.publishEvent(s.Header.ID, event)
	e.emitQueue(s)
	return nil
}

var anonymousIDs = struct {
	sync.Mutex
	values map[string]string
}{values: map[string]string{}}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func anonymousUserID(home string) string {
	path := filepath.Join(home, ".anonymous-user-id")
	anonymousIDs.Lock()
	defer anonymousIDs.Unlock()
	if cached := anonymousIDs.values[path]; cached != "" {
		return cached
	}
	if data, err := os.ReadFile(path); err == nil {
		if value := strings.ToLower(strings.TrimSpace(string(data))); uuidPattern.MatchString(value) {
			anonymousIDs.values[path] = value
			return value
		}
	}
	value := randomUUID()
	_ = os.MkdirAll(filepath.Dir(path), 0o700)
	if file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600); err == nil {
		_, _ = fmt.Fprintln(file, value)
		_ = file.Close()
	} else if data, readErr := os.ReadFile(path); readErr == nil && uuidPattern.MatchString(strings.ToLower(strings.TrimSpace(string(data)))) {
		value = strings.ToLower(strings.TrimSpace(string(data)))
	} else {
		_ = os.WriteFile(path, []byte(value+"\n"), 0o600)
	}
	anonymousIDs.values[path] = value
	return value
}

func randomUUID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return strings.TrimPrefix(newID(""), "-")
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16])
}
