package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// SubagentModelSelectionSettings is the host-owned opt-in preference used by
// model-selectable subagent tools. A session samples this value once.
type SubagentModelSelectionSettings struct {
	Enabled       bool                `json:"enabled"`
	AllowedModels []AllowedModelRoute `json:"allowedModels"`
}

// AllowedModelRoute is one exact provider/model route authorized for a child.
type AllowedModelRoute struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

const subagentModelSelectionSettingsNamespace = "subagent-model-selection"
const subagentModelSelectionPolicyEvent = "subagent/model-selection-policy"

func cloneAllowedModelRoutes(routes []AllowedModelRoute) []AllowedModelRoute {
	if routes == nil {
		return nil
	}
	out := make([]AllowedModelRoute, len(routes))
	copy(out, routes)
	return out
}

func validateAllowedModelRoutes(routes []AllowedModelRoute) error {
	seen := make(map[string]struct{}, len(routes))
	for _, route := range routes {
		if strings.TrimSpace(route.Provider) == "" || strings.TrimSpace(route.Model) == "" {
			return errors.New("subagent model selection requires non-empty provider and model ids")
		}
		key := route.Provider + "\x00" + route.Model
		if _, ok := seen[key]; ok {
			return fmt.Errorf("subagent model selection repeats route %q/%q", route.Provider, route.Model)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func cloneSubagentModelSelectionSettings(value SubagentModelSelectionSettings) SubagentModelSelectionSettings {
	value.AllowedModels = cloneAllowedModelRoutes(value.AllowedModels)
	return value
}

func decodeSubagentModelSelectionSettings(value map[string]any) (SubagentModelSelectionSettings, error) {
	settings := SubagentModelSelectionSettings{AllowedModels: []AllowedModelRoute{}}
	encoded, err := json.Marshal(value)
	if err != nil {
		return settings, err
	}
	if err := json.Unmarshal(encoded, &settings); err != nil {
		return settings, err
	}
	if err := validateAllowedModelRoutes(settings.AllowedModels); err != nil {
		return settings, err
	}
	if settings.Enabled && len(settings.AllowedModels) == 0 {
		return settings, errors.New("enabled subagent model selection requires at least one allowed model")
	}
	return settings, nil
}

func subagentModelSelectionSettingsSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"enabled": map[string]any{"type": "boolean", "default": false},
			"allowedModels": map[string]any{
				"type": "array", "default": []any{},
				"items": map[string]any{"type": "object", "properties": map[string]any{
					"provider": map[string]any{"type": "string", "minLength": 1},
					"model":    map[string]any{"type": "string", "minLength": 1},
				}, "required": []string{"provider", "model"}, "additionalProperties": false},
			},
		},
		"additionalProperties": false,
	}
}

// subagentModelSelectionSettingsLocked reads the resolved setting while the
// caller owns e.mu. Missing or malformed legacy values fall back to disabled;
// settings writes are validated before they reach this path.
func (e *Engine) subagentModelSelectionSettingsLocked() SubagentModelSelectionSettings {
	value, _ := e.resolvedSettingsValueLocked(subagentModelSelectionSettingsNamespace)
	settings, err := decodeSubagentModelSelectionSettings(value)
	if err != nil {
		return SubagentModelSelectionSettings{AllowedModels: []AllowedModelRoute{}}
	}
	return settings
}

// subagentModelSelectionSnapshotCapture identifies a snapshot introduced by
// one model-selectable tool installation. The prior sampled bit is retained so
// a failed disabled installation can make the session eligible for a later
// retry without disturbing a session that had already been sampled.
type subagentModelSelectionSnapshotCapture struct {
	session       *Session
	snapshot      *SubagentModelSelectionSettings
	sampledBefore bool
}

// captureSubagentModelSelectionSnapshotLocked freezes the host setting for a
// fresh root session and reports whether this call introduced a snapshot. The
// caller must own both e.mu and s.mu.
func (e *Engine) captureSubagentModelSelectionSnapshotLocked(s *Session) *subagentModelSelectionSnapshotCapture {
	if s == nil {
		return nil
	}
	if s.subagentModelSelectionSnapshot != nil ||
		s.Header.Origin == "subagent" || s.Header.ParentSession != "" || !s.subagentModelSelectionEligible {
		return nil
	}
	sampledBefore := s.subagentModelSelectionSampled
	settings := e.subagentModelSelectionSettingsLocked()
	settings = cloneSubagentModelSelectionSettings(settings)
	s.subagentModelSelectionSnapshot = &settings
	return &subagentModelSelectionSnapshotCapture{
		session:       s,
		snapshot:      s.subagentModelSelectionSnapshot,
		sampledBefore: sampledBefore,
	}
}

func (e *Engine) hasSubagentModelSelectionToolLocked() bool {
	for _, tool := range e.tools {
		if subagentModelSelectionToolEnabled(tool) {
			return true
		}
	}
	return false
}

// captureSubagentModelSelectionSnapshotsLocked freezes the current setting for
// every already-published fresh root session when a capable tool is installed.
// It returns only snapshots introduced by this installation so a failed
// tools/change emission can roll back its own state without touching an older
// tool's decision. The caller must own e.mu.
func (e *Engine) captureSubagentModelSelectionSnapshotsLocked() []subagentModelSelectionSnapshotCapture {
	captures := []subagentModelSelectionSnapshotCapture{}
	if !e.hasSubagentModelSelectionToolLocked() {
		return captures
	}
	for _, session := range e.sessions {
		session.mu.Lock()
		if capture := e.captureSubagentModelSelectionSnapshotLocked(session); capture != nil {
			captures = append(captures, *capture)
		}
		session.mu.Unlock()
	}
	return captures
}

type subagentModelSelectionPolicyCommit struct {
	sessionID string
	event     Event
}

// recordSubagentModelSelectionSnapshotCapturesLocked commits enabled
// late-install decisions while the new tool is still hidden behind e.mu.
// Disabled decisions remain snapshot-only. The caller must own e.mu; committed
// events are published only after releasing it.
func (e *Engine) recordSubagentModelSelectionSnapshotCapturesLocked(captures []subagentModelSelectionSnapshotCapture) ([]subagentModelSelectionPolicyCommit, error) {
	commits := make([]subagentModelSelectionPolicyCommit, 0, len(captures))
	for _, capture := range captures {
		session, snapshot := capture.session, capture.snapshot
		if session == nil || snapshot == nil || !snapshot.Enabled || len(snapshot.AllowedModels) == 0 {
			continue
		}
		session.mu.Lock()
		if session.subagentModelSelectionSnapshot != snapshot {
			session.mu.Unlock()
			continue
		}
		_, present, err := subagentModelSelectionPolicyFromEvents(session.Events)
		if err != nil {
			session.mu.Unlock()
			return commits, err
		}
		if present {
			session.mu.Unlock()
			continue
		}
		id := session.Header.ID
		event, err := appendEventLocked(session, subagentModelSelectionPolicyEvent, map[string]any{
			"allowedModels": cloneAllowedModelRoutes(snapshot.AllowedModels),
		}, nil, nil, false)
		session.mu.Unlock()
		if err != nil {
			return commits, err
		}
		commits = append(commits, subagentModelSelectionPolicyCommit{sessionID: id, event: event})
	}
	return commits, nil
}

func (e *Engine) publishSubagentModelSelectionPolicyCommits(origin *dynamicCordisRun, commits []subagentModelSelectionPolicyCommit) {
	for _, commit := range commits {
		e.publishEventFrom(origin, commit.sessionID, commit.event)
	}
}

// recordSubagentModelSelectionPolicy commits an enabled snapshot for a single
// session. It is idempotent and leaves the sampled state untouched when the
// store rejects the append, allowing a later caller to retry.
func (e *Engine) recordSubagentModelSelectionPolicy(s *Session) (bool, error) {
	if s == nil {
		return false, nil
	}
	s.mu.Lock()
	event, recorded, err := e.recordSubagentModelSelectionPolicyLocked(s)
	id := s.Header.ID
	s.mu.Unlock()
	if err != nil || !recorded {
		return recorded, err
	}
	e.publishEvent(id, event)
	return true, nil
}

// recordSubagentModelSelectionPolicyLocked is the publication-boundary variant
// used while the engine registry lock is held. It deliberately does not emit
// the event; callers publish after releasing e.mu to avoid lock inversion.
// The caller must hold s.mu.
func (e *Engine) recordSubagentModelSelectionPolicyLocked(s *Session) (Event, bool, error) {
	if s == nil {
		return Event{}, false, nil
	}
	routes, present, err := subagentModelSelectionPolicyFromEvents(s.Events)
	if err != nil || present {
		return Event{}, present, err
	}
	if s.subagentModelSelectionSnapshot == nil || !s.subagentModelSelectionSnapshot.Enabled || len(s.subagentModelSelectionSnapshot.AllowedModels) == 0 {
		return Event{}, false, nil
	}
	routes = cloneAllowedModelRoutes(s.subagentModelSelectionSnapshot.AllowedModels)
	if err := validateAllowedModelRoutes(routes); err != nil {
		return Event{}, false, err
	}
	event, err := appendEventLocked(s, subagentModelSelectionPolicyEvent, map[string]any{"allowedModels": routes}, nil, nil, false)
	if err != nil {
		return Event{}, false, err
	}
	return event, true, nil
}

// rollbackSubagentModelSelectionSnapshotCapturesLocked removes snapshots
// introduced by a failed tool installation when they have not become durable.
// A tools/change failure preserves enabled captures because their policy was
// already committed; an earlier policy-commit failure releases them so a retry
// can read settings again. Existing selectable tools and durable policies take
// precedence in either case. The caller must own e.mu.
func (e *Engine) rollbackSubagentModelSelectionSnapshotCapturesLocked(captures []subagentModelSelectionSnapshotCapture, preserveEnabled bool) {
	if len(captures) == 0 {
		return
	}
	otherSelectableTool := e.hasSubagentModelSelectionToolLocked()
	for _, capture := range captures {
		session := capture.session
		if session == nil {
			continue
		}
		session.mu.Lock()
		// A concurrent successful installation may have replaced the snapshot;
		// never clear state that this failed registration did not create.
		if session.subagentModelSelectionSnapshot != capture.snapshot {
			session.mu.Unlock()
			continue
		}
		_, hasPolicy, policyErr := subagentModelSelectionPolicyFromEvents(session.Events)
		if (!preserveEnabled || !capture.snapshot.Enabled) && policyErr == nil && !hasPolicy && !otherSelectableTool {
			session.subagentModelSelectionSnapshot = nil
			session.subagentModelSelectionSampled = capture.sampledBefore
		}
		session.mu.Unlock()
	}
}

// removeSubagentModelSelectionListToolsLocked removes only discovery helpers
// installed by this feature. Durable policy events are intentionally retained
// on their sessions so a later selectable-tool installation can reuse them.
// The caller must hold e.mu.
func (e *Engine) removeSubagentModelSelectionListToolsLocked() {
	for _, tools := range e.scopedTools {
		if tool, ok := tools["list_subagent_models"]; ok && tool.subagentModelSelectionList {
			delete(tools, "list_subagent_models")
		}
	}
}

func subagentModelSelectionPolicyFromEvents(events []Event) ([]AllowedModelRoute, bool, error) {
	for _, event := range events {
		if event.Type != subagentModelSelectionPolicyEvent {
			continue
		}
		data, ok := event.Data.(map[string]any)
		if !ok {
			return nil, true, errors.New("subagent/model-selection-policy data must be an object")
		}
		raw, ok := data["allowedModels"]
		if !ok {
			return nil, true, errors.New("subagent/model-selection-policy requires allowedModels")
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			return nil, true, err
		}
		var routes []AllowedModelRoute
		if err := json.Unmarshal(encoded, &routes); err != nil {
			return nil, true, err
		}
		if len(routes) == 0 {
			return nil, true, errors.New("subagent/model-selection-policy requires at least one route")
		}
		if err := validateAllowedModelRoutes(routes); err != nil {
			return nil, true, err
		}
		return routes, true, nil
	}
	return nil, false, nil
}

func subagentModelSelectionPolicyForSession(s *Session) ([]AllowedModelRoute, bool, error) {
	if s == nil {
		return nil, false, nil
	}
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	return subagentModelSelectionPolicyFromEvents(events)
}

// sampleSubagentModelSelection records a top-level decision once per live
// session. The event is log-only and therefore does not enter model history.
func (e *Engine) sampleSubagentModelSelection(s *Session) ([]AllowedModelRoute, bool, error) {
	if s == nil {
		return nil, false, nil
	}
	s.mu.Lock()
	routes, present, err := subagentModelSelectionPolicyFromEvents(s.Events)
	if err != nil || present {
		s.subagentModelSelectionSampled = true
		s.mu.Unlock()
		return routes, present, err
	}
	if s.subagentModelSelectionSampled {
		s.mu.Unlock()
		return nil, false, nil
	}
	origin, eligible := s.Header.Origin, s.subagentModelSelectionEligible
	settings := SubagentModelSelectionSettings{}
	if s.subagentModelSelectionSnapshot != nil {
		settings = cloneSubagentModelSelectionSettings(*s.subagentModelSelectionSnapshot)
	}
	needsFallbackSnapshot := eligible && origin != "subagent" && s.subagentModelSelectionSnapshot == nil
	s.mu.Unlock()
	if needsFallbackSnapshot {
		// Direct library callers may sample a hand-built session before a tool
		// registration boundary exists. Preserve that compatibility path while
		// keeping published sessions on the immutable snapshot above.
		e.mu.RLock()
		fallback := e.subagentModelSelectionSettingsLocked()
		e.mu.RUnlock()
		fallback = cloneSubagentModelSelectionSettings(fallback)
		s.mu.Lock()
		if s.subagentModelSelectionSnapshot == nil && !s.subagentModelSelectionSampled {
			s.subagentModelSelectionSnapshot = &fallback
		}
		if s.subagentModelSelectionSnapshot != nil {
			settings = cloneSubagentModelSelectionSettings(*s.subagentModelSelectionSnapshot)
		}
		s.mu.Unlock()
	}
	s.mu.Lock()
	// A concurrent caller may have committed the durable policy while this
	// caller was reading the fallback setting. Re-read under the session lock so
	// every first visitor observes the same route policy rather than a spurious
	// disabled result.
	if routes, present, err := subagentModelSelectionPolicyFromEvents(s.Events); err != nil || present {
		s.subagentModelSelectionSampled = true
		s.mu.Unlock()
		return routes, present, err
	}
	if s.subagentModelSelectionSampled {
		s.mu.Unlock()
		return nil, false, nil
	}
	if origin == "subagent" || !eligible {
		s.subagentModelSelectionSampled = true
		s.mu.Unlock()
		return nil, false, nil
	}
	if s.subagentModelSelectionSnapshot != nil {
		settings = cloneSubagentModelSelectionSettings(*s.subagentModelSelectionSnapshot)
	}
	if s.subagentModelSelectionSnapshot == nil || !settings.Enabled || len(settings.AllowedModels) == 0 {
		s.subagentModelSelectionSampled = true
		s.mu.Unlock()
		return nil, false, nil
	}
	if err := validateAllowedModelRoutes(settings.AllowedModels); err != nil {
		s.mu.Unlock()
		return nil, false, err
	}
	data := map[string]any{"allowedModels": cloneAllowedModelRoutes(settings.AllowedModels)}
	event, err := appendEventLocked(s, subagentModelSelectionPolicyEvent, data, nil, nil, false)
	if err == nil {
		// Sampling is committed only after the durable append succeeds. A
		// transient store failure must leave this session retryable.
		s.subagentModelSelectionSampled = true
	}
	id := s.Header.ID
	s.mu.Unlock()
	if err != nil {
		return nil, false, err
	}
	e.publishEvent(id, event)
	return cloneAllowedModelRoutes(settings.AllowedModels), true, nil
}

func (e *Engine) ensureSubagentModelSelectionTools(s *Session) (bool, error) {
	if s == nil {
		return false, nil
	}
	s.mu.Lock()
	sessionID := s.Header.ID
	s.mu.Unlock()
	e.mu.RLock()
	capable := make([]Tool, 0)
	for _, tool := range e.tools {
		if subagentModelSelectionToolEnabled(tool) {
			capable = append(capable, tool)
		}
	}
	_, listRegistered := e.scopedTools[sessionID]["list_subagent_models"]
	e.mu.RUnlock()
	if len(capable) == 0 {
		// A selectable definition may have been removed after this session first
		// visited its tool directory. Hide the feature-owned discovery helper but
		// retain any durable policy event for a future reinstallation.
		e.mu.Lock()
		if tools := e.scopedTools[sessionID]; tools != nil {
			if tool, ok := tools["list_subagent_models"]; ok && tool.subagentModelSelectionList {
				delete(tools, "list_subagent_models")
			}
		}
		e.mu.Unlock()
		return false, nil
	}
	e.mu.Lock()
	s.mu.Lock()
	e.captureSubagentModelSelectionSnapshotLocked(s)
	s.mu.Unlock()
	e.mu.Unlock()
	// Publication/install paths normally record this before the definition is
	// visible. Keep the helper for direct library sessions and dynamic callers
	// that bypass those paths.
	if _, err := e.recordSubagentModelSelectionPolicy(s); err != nil {
		return false, err
	}
	routes, enabled, err := e.sampleSubagentModelSelection(s)
	if err != nil {
		return false, err
	}
	if !enabled || len(routes) == 0 {
		return false, nil
	}
	if !listRegistered {
		normalized, err := normalizeRegisteredTool(listSubagentModelsTool(e))
		if err != nil {
			return false, err
		}
		// Re-check while holding the registry lock. Two concurrent first visits
		// may both have observed absence above; only one inserts the helper and
		// both callers then share the same definition.
		e.mu.Lock()
		tools := e.scopedTools[sessionID]
		if tools == nil {
			tools = map[string]Tool{}
			e.scopedTools[sessionID] = tools
		}
		if _, exists := tools[normalized.Schema.Name]; !exists {
			tools[normalized.Schema.Name] = normalized
		}
		e.mu.Unlock()
	}
	return true, nil
}

func (e *Engine) inheritSubagentModelSelection(child *Session, parentEvents []Event) error {
	routes, present, err := subagentModelSelectionPolicyFromEvents(parentEvents)
	if err != nil || child == nil {
		return err
	}
	if !present && child.Header.ParentSession != "" {
		// A parent can be delegated from before its model-selectable tool is
		// first executed. In that window the immutable publication snapshot is
		// the same decision the child must inherit; do not consult live Host
		// settings here.
		if parent, parentErr := e.getSession(child.Header.ParentSession); parentErr == nil {
			parent.mu.Lock()
			if snapshot := parent.subagentModelSelectionSnapshot; snapshot != nil && snapshot.Enabled {
				routes = cloneAllowedModelRoutes(snapshot.AllowedModels)
				present = len(routes) > 0
			}
			parent.mu.Unlock()
		}
	}
	if !present {
		return nil
	}
	child.mu.Lock()
	existing, existingPresent, existingErr := subagentModelSelectionPolicyFromEvents(child.Events)
	child.mu.Unlock()
	if existingErr != nil {
		return existingErr
	}
	if existingPresent {
		if len(existing) == 0 {
			return errors.New("subagent/model-selection-policy requires at least one route")
		}
		return nil
	}
	_, err = e.appendEvent(child, subagentModelSelectionPolicyEvent, map[string]any{"allowedModels": cloneAllowedModelRoutes(routes)})
	return err
}

func subagentModelSelectionToolEnabled(tool Tool) bool {
	return tool.subagentModelSelectionCapable
}

func subagentModelSelectionSchemaEnabled(tool Tool, enabled bool) Tool {
	tool.Schema = cloneToolSchema(tool.Schema)
	properties, _ := tool.Schema.Parameters["properties"].(map[string]any)
	if !enabled {
		delete(properties, "provider")
		delete(properties, "model")
		delete(properties, "reasoning_effort")
		return tool
	}
	return tool
}

func allowedSubagentRoute(routes []AllowedModelRoute, provider, model string) bool {
	for _, route := range routes {
		if route.Provider == provider && route.Model == model {
			return true
		}
	}
	return false
}

func parentModelSelectionForDelegation(s *Session) ModelSelection {
	if s == nil {
		return ModelSelection{}
	}
	s.mu.Lock()
	selection := cloneModelSelection(s.Model)
	if latest, ok := latestLoggedModel(s.Events); ok {
		selection = latest
	}
	s.mu.Unlock()
	return selection
}

func hasConfiguredSubagentLlm(options *SubagentAgentOptions) bool {
	return options != nil && (options.Provider != "" || options.Model != "" || options.ReasoningEffort != "" || options.MaxTokens != 0)
}

type subagentModelRequest struct {
	Provider        *string `json:"provider"`
	Model           *string `json:"model"`
	ReasoningEffort *string `json:"reasoning_effort"`
}

func hasSubagentModelRequest(request subagentModelRequest) bool {
	return request.Provider != nil || request.Model != nil || request.ReasoningEffort != nil
}

func requestedSubagentAgentOptions(parent ModelSelection, configured *SubagentAgentOptions, request subagentModelRequest, enabled bool) (*SubagentAgentOptions, error) {
	if !hasSubagentModelRequest(request) {
		if configured == nil {
			return nil, nil
		}
		copy := *configured
		return &copy, nil
	}
	if !enabled {
		return nil, errors.New("child model selection is disabled for this tool instance")
	}
	nonEmpty := func(value *string, field string) error {
		if value != nil && *value == "" {
			return fmt.Errorf("child LLM `%s` must be non-empty", field)
		}
		return nil
	}
	if err := nonEmpty(request.Provider, "provider"); err != nil {
		return nil, err
	}
	if err := nonEmpty(request.Model, "model"); err != nil {
		return nil, err
	}
	if err := nonEmpty(request.ReasoningEffort, "reasoning_effort"); err != nil {
		return nil, err
	}
	if (request.Provider == nil) != (request.Model == nil) {
		return nil, errors.New("child LLM `provider` and `model` must be supplied together")
	}
	var out SubagentAgentOptions
	if configured != nil {
		out = *configured
	}
	baselineProvider, baselineModel := out.Provider, out.Model
	if baselineProvider == "" {
		baselineProvider = parent.Provider
	}
	if baselineModel == "" {
		baselineModel = parent.Model
	}
	routeChanged := request.Provider != nil && (*request.Provider != baselineProvider || *request.Model != baselineModel)
	if routeChanged && request.ReasoningEffort == nil {
		out.ReasoningEffort = ""
	}
	if request.Provider != nil {
		out.Provider, out.Model = *request.Provider, *request.Model
	}
	if request.ReasoningEffort != nil {
		out.ReasoningEffort = *request.ReasoningEffort
	}
	return &out, nil
}

func (e *Engine) preflightSubagentLlm(ctx context.Context, parent ModelSelection, options *SubagentAgentOptions, inheritParentReasoningEffort bool) error {
	if options == nil || (options.Provider == "" && options.Model == "" && options.ReasoningEffort == "" && options.MaxTokens == 0) {
		return nil
	}
	if options.MaxTokens < 0 || int64(options.MaxTokens) > maxJSONSafeInteger {
		return errors.New("child LLM maxTokens must be a positive safe integer")
	}
	providerID, modelID := options.Provider, options.Model
	if providerID == "" {
		providerID = parent.Provider
	}
	if modelID == "" {
		modelID = parent.Model
	}
	if providerID == "" || modelID == "" {
		return errors.New("cannot select child LLM values without an effective provider and model")
	}
	info, err := resolveExactModelInfo(ctx, e, ModelSelection{Provider: providerID, Model: modelID})
	if err != nil {
		return err
	}
	if info.MaxTokens < 0 || int64(info.MaxTokens) > maxJSONSafeInteger {
		return fmt.Errorf("model %q returned invalid default maxTokens", modelID)
	}
	reasoningEffort := options.ReasoningEffort
	if reasoningEffort == "" && inheritParentReasoningEffort && providerID == parent.Provider && modelID == parent.Model {
		reasoningEffort = parent.ReasoningEffort
	}
	if reasoningEffort != "" {
		if info.Reasoning == nil {
			return fmt.Errorf("model %q does not support reasoning effort %q", modelID, reasoningEffort)
		}
		for _, effort := range info.Reasoning.Efforts {
			if effort.ID == reasoningEffort {
				return nil
			}
		}
		return fmt.Errorf("model %q does not support reasoning effort %q", modelID, reasoningEffort)
	}
	return nil
}

func listSubagentModelsTool(e *Engine) Tool {
	type input struct {
		Provider *string `json:"provider"`
		Model    *string `json:"model"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "list_subagent_models",
			Description: "Discover allowed LLM routes for subagents.",
			Parameters: objectSchema(map[string]any{
				"provider": map[string]any{"type": "string"},
				"model":    map[string]any{"type": "string"},
			}),
			Output: map[string]any{"type": "string"},
		},
		subagentModelSelectionList: true,
		IsConcurrencySafe:          alwaysConcurrencySafe,
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			s, err := e.getSession(call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			routes, present, err := subagentModelSelectionPolicyForSession(s)
			if err != nil {
				return ToolResult{}, err
			}
			if !present {
				return ToolResult{}, errors.New("child model selection is disabled for this Session")
			}
			if in.Model != nil && in.Provider == nil {
				return ToolResult{}, errors.New("`model` requires `provider`")
			}
			e.mu.RLock()
			providers := make(map[string]Provider, len(e.providers))
			providerOrder := make([]string, 0, len(e.providerOrder))
			for _, id := range e.providerOrder {
				if provider := e.providers[id]; provider != nil {
					providers[id] = provider
					providerOrder = append(providerOrder, id)
				}
			}
			e.mu.RUnlock()
			if in.Provider == nil {
				ids := make([]string, 0, len(providerOrder))
				for _, id := range providerOrder {
					for _, route := range routes {
						if route.Provider == id {
							ids = append(ids, id)
							break
						}
					}
				}
				if len(ids) == 0 {
					return textToolResult("(no LLM providers)"), nil
				}
				lines := make([]string, 0, len(ids))
				for _, id := range ids {
					lines = append(lines, id+" — "+providers[id].Name())
				}
				return textToolResult(strings.Join(lines, "\n")), nil
			}
			providerID := *in.Provider
			if providerID == "" {
				return ToolResult{}, errors.New("`provider` must be non-empty")
			}
			allowed := make([]AllowedModelRoute, 0)
			for _, route := range routes {
				if route.Provider == providerID {
					allowed = append(allowed, route)
				}
			}
			if len(allowed) == 0 {
				return ToolResult{}, fmt.Errorf("LLM provider %q is not allowed for this Session", providerID)
			}
			provider := providers[providerID]
			if provider == nil {
				available := make([]string, 0, len(providerOrder))
				for _, id := range providerOrder {
					for _, route := range routes {
						if route.Provider == id {
							available = append(available, id)
							break
						}
					}
				}
				alternatives := strings.Join(available, ", ")
				if alternatives == "" {
					alternatives = "(none)"
				}
				return ToolResult{}, fmt.Errorf("LLM provider %q is not registered; available providers: %s", providerID, alternatives)
			}
			if in.Model == nil {
				models, err := provider.Models(ctx)
				if err != nil {
					return ToolResult{}, err
				}
				lines := make([]string, 0)
				for _, model := range models {
					if allowedSubagentRoute(allowed, providerID, model.ID) {
						line := providerID + "/" + model.ID + " — " + model.Name
						if model.Description != "" {
							line += ": " + model.Description
						}
						lines = append(lines, line)
					}
				}
				if len(lines) == 0 {
					return textToolResult("(no advertised models for " + providerID + ")"), nil
				}
				return textToolResult(strings.Join(lines, "\n")), nil
			}
			modelID := *in.Model
			if modelID == "" {
				return ToolResult{}, errors.New("`model` must be non-empty")
			}
			if !allowedSubagentRoute(allowed, providerID, modelID) {
				return ToolResult{}, fmt.Errorf("child LLM route %q/%q is not allowed for this Session", providerID, modelID)
			}
			info, err := resolveExactModelInfo(ctx, e, ModelSelection{Provider: providerID, Model: modelID})
			if err != nil {
				return ToolResult{}, err
			}
			line := providerID + "/" + info.ID + " — " + info.Name
			if info.Description != "" {
				line += ": " + info.Description
			}
			line += "\nReasoning efforts:\n"
			if info.Reasoning == nil || len(info.Reasoning.Efforts) == 0 {
				line += "(no advertised reasoning efforts)"
			} else {
				for i, effort := range info.Reasoning.Efforts {
					if i > 0 {
						line += "\n"
					}
					line += effort.ID
					if info.Reasoning.DefaultEffort == effort.ID {
						line += " (default)"
					}
					line += " — " + effort.Name
					if effort.Description != "" {
						line += ": " + effort.Description
					}
				}
			}
			return textToolResult(line), nil
		},
	}
}
