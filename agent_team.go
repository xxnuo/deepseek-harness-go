package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultTeamMaxMembers      = 8
	defaultTeamMaxTasks        = 256
	defaultTeamMaxPending      = 64
	defaultTeamMaxMessageBytes = 65_536
	defaultTeamDisposalTimeout = 5 * time.Second
	minimumTeamWait            = 10 * time.Second
	maximumTeamWait            = time.Hour
	teamEventVersion           = 1
	teamLeadName               = "lead"
	teamMemberProvisioning     = "provisioning"
	teamMemberActive           = "active"
	teamMemberFailed           = "failed"
	teamMessageQuiet           = "quiet"
	teamMessageWakeup          = "wakeup"
	teamTaskPending            = "pending"
	teamTaskInProgress         = "in_progress"
	teamTaskCompleted          = "completed"
	teamTaskDeleted            = "deleted"
)

var teamMemberNamePattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

// AgentTeamConfig enables the experimental durable Agent Teams runtime.
type AgentTeamConfig struct {
	MaxMembers                  int
	MaxTasks                    int
	MaxPendingMessagesPerMember int
	MaxMessageBytes             int
	DisposalTimeout             time.Duration
	FreshProvider               string
	ForkProvider                string
}

// DefaultAgentTeamConfig returns the upstream rc.8 deployment defaults.
func DefaultAgentTeamConfig() AgentTeamConfig {
	return AgentTeamConfig{
		MaxMembers: defaultTeamMaxMembers, MaxTasks: defaultTeamMaxTasks,
		MaxPendingMessagesPerMember: defaultTeamMaxPending, MaxMessageBytes: defaultTeamMaxMessageBytes,
		DisposalTimeout: defaultTeamDisposalTimeout, FreshProvider: "spawn", ForkProvider: "fork",
	}
}

func normalizeAgentTeamConfig(config AgentTeamConfig) (AgentTeamConfig, error) {
	defaults := DefaultAgentTeamConfig()
	if config.MaxMembers == 0 {
		config.MaxMembers = defaults.MaxMembers
	}
	if config.MaxTasks == 0 {
		config.MaxTasks = defaults.MaxTasks
	}
	if config.MaxPendingMessagesPerMember == 0 {
		config.MaxPendingMessagesPerMember = defaults.MaxPendingMessagesPerMember
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if config.DisposalTimeout == 0 {
		config.DisposalTimeout = defaults.DisposalTimeout
	}
	if config.FreshProvider == "" {
		config.FreshProvider = defaults.FreshProvider
	}
	if config.ForkProvider == "" {
		config.ForkProvider = defaults.ForkProvider
	}
	if config.MaxMembers < 1 || config.MaxTasks < 1 || config.MaxPendingMessagesPerMember < 1 || config.MaxMessageBytes < 1 || config.DisposalTimeout < 1 {
		return AgentTeamConfig{}, &TeamError{Code: "TEAM_INVALID_CONFIG", Message: "Agent Teams limits must be positive"}
	}
	config.FreshProvider = strings.TrimSpace(config.FreshProvider)
	config.ForkProvider = strings.TrimSpace(config.ForkProvider)
	if config.FreshProvider == "" || config.ForkProvider == "" {
		return AgentTeamConfig{}, &TeamError{Code: "TEAM_INVALID_CONFIG", Message: "Agent Teams providers must be non-empty"}
	}
	return config, nil
}

// TeamError carries the stable error code used by the TypeScript service.
type TeamError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Cause   error  `json:"-"`
}

func (e *TeamError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

func (e *TeamError) Unwrap() error { return e.Cause }

func teamError(code, message string) error { return &TeamError{Code: code, Message: message} }

type TeamMemberSnapshot struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Provider    string `json:"provider"`
	Context     string `json:"context"`
	Phase       string `json:"phase"`
	Error       string `json:"error,omitempty"`
}

type TeamMemberView struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Role        string   `json:"role"`
	Status      string   `json:"status"`
	Description string   `json:"description,omitempty"`
	Provider    string   `json:"provider,omitempty"`
	Context     string   `json:"context,omitempty"`
	Model       string   `json:"model,omitempty"`
	Diagnostics []string `json:"diagnostics"`
}

type TeamTaskSnapshot struct {
	ID          string   `json:"id"`
	Revision    int      `json:"revision"`
	Subject     string   `json:"subject"`
	Description string   `json:"description"`
	Status      string   `json:"status"`
	OwnerID     string   `json:"ownerId,omitempty"`
	BlockedBy   []string `json:"blockedBy"`
	WriteScopes []string `json:"writeScopes"`
}

type TeamTaskView struct {
	ID                 string   `json:"id"`
	Revision           int      `json:"revision"`
	Subject            string   `json:"subject"`
	Description        string   `json:"description"`
	Status             string   `json:"status"`
	OwnerName          string   `json:"ownerName,omitempty"`
	BlockedBy          []string `json:"blockedBy"`
	WriteScopes        []string `json:"writeScopes"`
	Ready              bool     `json:"ready"`
	WriteScopeWarnings []string `json:"writeScopeWarnings"`
}

type TeamMessageSnapshot struct {
	ID         string         `json:"id"`
	SenderID   string         `json:"senderId"`
	SenderName string         `json:"senderName"`
	TargetID   string         `json:"targetId"`
	Delivery   string         `json:"delivery"`
	Content    []ContentBlock `json:"content"`
}

type SpawnTeammateRequest struct {
	Name        string
	Description string
	Prompt      []ContentBlock
	Context     string
	Provider    string
}

type SpawnTeammateResult struct {
	Member TeamMemberView `json:"member"`
}

type SendTeamMessageRequest struct {
	Target   string
	Content  []ContentBlock
	Delivery string
}

type SendTeamMessageResult struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
}

type CreateTeamTaskRequest struct {
	Subject     string
	Description string
	BlockedBy   []string
	WriteScopes []string
}

type UpdateTeamTaskRequest struct {
	TaskID           string
	ExpectedRevision int
	Action           string
	Subject          *string
	Description      *string
	BlockedBy        []string
	BlockedBySet     bool
	WriteScopes      []string
	WriteScopesSet   bool
	Owner            *string
}

type TeamWaitResult struct {
	TimedOut bool `json:"timedOut"`
}

type TeamFold struct {
	ID             string                `json:"id"`
	Members        []TeamMemberSnapshot  `json:"members"`
	Tasks          []TeamTaskSnapshot    `json:"tasks"`
	Messages       []TeamMessageSnapshot `json:"messages"`
	Delivered      []string              `json:"delivered"`
	NextTaskNumber int                   `json:"nextTaskNumber"`
}

type teamFoldState struct {
	id             string
	members        map[string]TeamMemberSnapshot
	memberNames    map[string]string
	memberOrder    []string
	tasks          map[string]TeamTaskSnapshot
	taskOrder      []string
	messages       map[string]TeamMessageSnapshot
	messageOrder   []string
	delivered      map[string]bool
	nextTaskNumber int
}

type teamEventSelector struct {
	Version int    `json:"version"`
	TeamID  string `json:"teamId"`
}

type teamMemberEvent struct {
	Version int                `json:"version"`
	TeamID  string             `json:"teamId"`
	Member  TeamMemberSnapshot `json:"member"`
}

type teamTaskEvent struct {
	Version int              `json:"version"`
	TeamID  string           `json:"teamId"`
	Task    TeamTaskSnapshot `json:"task"`
}

type teamMessageQueuedEvent struct {
	Version int                 `json:"version"`
	TeamID  string              `json:"teamId"`
	Message TeamMessageSnapshot `json:"message"`
}

type teamMessageDeliveredEvent struct {
	Version   int    `json:"version"`
	TeamID    string `json:"teamId"`
	MessageID string `json:"messageId"`
	TargetID  string `json:"targetId"`
}

func emptyTeamFold(rootID string) *teamFoldState {
	return &teamFoldState{
		id: rootID, members: map[string]TeamMemberSnapshot{}, memberNames: map[string]string{},
		tasks: map[string]TeamTaskSnapshot{}, messages: map[string]TeamMessageSnapshot{},
		delivered: map[string]bool{}, nextTaskNumber: 1,
	}
}

func decodeTeamPayload(value any, target any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, target); err != nil {
		return err
	}
	return nil
}

func isTeamEventType(typ string) bool {
	switch typ {
	case "team/member", "team/task", "team/message/queued", "team/message/delivered":
		return true
	default:
		return false
	}
}

func foldTeam(rootID string, events []Event) (*teamFoldState, error) {
	state := emptyTeamFold(rootID)
	for _, event := range events {
		if !isTeamEventType(event.Type) {
			continue
		}
		var selector teamEventSelector
		if err := decodeTeamPayload(event.Data, &selector); err != nil || selector.TeamID == "" {
			return nil, fmt.Errorf("persisted Agent Teams %s payload is invalid", event.Type)
		}
		if selector.TeamID != rootID {
			continue
		}
		if selector.Version != teamEventVersion {
			return nil, fmt.Errorf("unsupported Agent Teams event version %d", selector.Version)
		}
		switch event.Type {
		case "team/member":
			var value teamMemberEvent
			if err := decodeTeamPayload(event.Data, &value); err != nil {
				return nil, fmt.Errorf("persisted Agent Teams team/member payload is invalid: %w", err)
			}
			member := value.Member
			if member.ID == "" || member.Name == "" || member.Provider == "" || (member.Context != "fresh" && member.Context != "fork") ||
				(member.Phase != teamMemberProvisioning && member.Phase != teamMemberActive && member.Phase != teamMemberFailed) {
				return nil, errors.New("persisted Agent Teams team/member payload is invalid")
			}
			prior, exists := state.members[member.ID]
			if named := state.memberNames[member.Name]; named != "" && named != member.ID {
				return nil, fmt.Errorf("teammate name %q is reused by another member", member.Name)
			}
			if !exists {
				if member.Phase != teamMemberProvisioning {
					return nil, fmt.Errorf("teammate %q must begin provisioning", member.Name)
				}
				state.memberNames[member.Name] = member.ID
				state.memberOrder = append(state.memberOrder, member.ID)
			} else {
				if prior.Name != member.Name || prior.Provider != member.Provider || prior.Context != member.Context {
					return nil, fmt.Errorf("teammate %q changed immutable identity fields", member.ID)
				}
				if prior.Phase != teamMemberProvisioning || member.Phase == teamMemberProvisioning {
					return nil, fmt.Errorf("teammate %q has an invalid %s -> %s transition", member.Name, prior.Phase, member.Phase)
				}
			}
			state.members[member.ID] = cloneTeamMember(member)
		case "team/task":
			var value teamTaskEvent
			if err := decodeTeamPayload(event.Data, &value); err != nil {
				return nil, fmt.Errorf("persisted Agent Teams team/task payload is invalid: %w", err)
			}
			task := cloneTeamTask(value.Task)
			if task.ID == "" || task.Revision < 1 || !validTeamTaskStatus(task.Status) {
				return nil, errors.New("persisted Agent Teams team/task payload is invalid")
			}
			prior, exists := state.tasks[task.ID]
			if !exists && task.Revision != 1 {
				return nil, fmt.Errorf("team task %q must begin at revision 1", task.ID)
			}
			if exists && task.Revision != prior.Revision+1 {
				return nil, fmt.Errorf("team task %q revision is not contiguous", task.ID)
			}
			if err := validateTeamTaskGraph(state.tasks, task); err != nil {
				return nil, err
			}
			if !exists {
				state.taskOrder = append(state.taskOrder, task.ID)
			}
			if number, ok := numericTeamTaskID(task.ID); ok && number >= state.nextTaskNumber {
				if int64(number) == maxJSONSafeInteger {
					state.nextTaskNumber = number
				} else {
					state.nextTaskNumber = number + 1
				}
			}
			state.tasks[task.ID] = task
		case "team/message/queued":
			var value teamMessageQueuedEvent
			if err := decodeTeamPayload(event.Data, &value); err != nil {
				return nil, fmt.Errorf("persisted Agent Teams team/message/queued payload is invalid: %w", err)
			}
			message := cloneTeamMessage(value.Message)
			if message.ID == "" || message.SenderID == "" || message.SenderName == "" || message.TargetID == "" ||
				(message.Delivery != teamMessageQuiet && message.Delivery != teamMessageWakeup) {
				return nil, errors.New("persisted Agent Teams team/message/queued payload is invalid")
			}
			if _, exists := state.messages[message.ID]; exists {
				return nil, fmt.Errorf("team message %q was queued twice", message.ID)
			}
			state.messages[message.ID] = message
			state.messageOrder = append(state.messageOrder, message.ID)
		case "team/message/delivered":
			var value teamMessageDeliveredEvent
			if err := decodeTeamPayload(event.Data, &value); err != nil || value.MessageID == "" || value.TargetID == "" {
				return nil, errors.New("persisted Agent Teams team/message/delivered payload is invalid")
			}
			queued, exists := state.messages[value.MessageID]
			if !exists {
				return nil, fmt.Errorf("team message %q was delivered before queueing", value.MessageID)
			}
			if queued.TargetID != value.TargetID {
				return nil, fmt.Errorf("team message %q target changed", value.MessageID)
			}
			if state.delivered[value.MessageID] {
				return nil, fmt.Errorf("team message %q was delivered twice", value.MessageID)
			}
			state.delivered[value.MessageID] = true
		}
	}
	return state, nil
}

// FoldTeam replays the Team-owned portion of one Lead Session log.
func FoldTeam(rootID string, events []Event) (TeamFold, error) {
	state, err := foldTeam(rootID, events)
	if err != nil {
		return TeamFold{}, err
	}
	result := TeamFold{ID: rootID, NextTaskNumber: state.nextTaskNumber}
	for _, id := range state.memberOrder {
		result.Members = append(result.Members, cloneTeamMember(state.members[id]))
	}
	for _, id := range state.taskOrder {
		result.Tasks = append(result.Tasks, cloneTeamTask(state.tasks[id]))
	}
	for _, id := range state.messageOrder {
		result.Messages = append(result.Messages, cloneTeamMessage(state.messages[id]))
		if state.delivered[id] {
			result.Delivered = append(result.Delivered, id)
		}
	}
	return result, nil
}

func cloneTeamMember(value TeamMemberSnapshot) TeamMemberSnapshot { return value }

func cloneTeamTask(value TeamTaskSnapshot) TeamTaskSnapshot {
	value.BlockedBy = append([]string(nil), value.BlockedBy...)
	value.WriteScopes = append([]string(nil), value.WriteScopes...)
	return value
}

func cloneTeamMessage(value TeamMessageSnapshot) TeamMessageSnapshot {
	value.Content = cloneContentBlocks(value.Content)
	return value
}

func validTeamTaskStatus(status string) bool {
	return status == teamTaskPending || status == teamTaskInProgress || status == teamTaskCompleted || status == teamTaskDeleted
}

func numericTeamTaskID(id string) (int, bool) {
	if !strings.HasPrefix(id, "task-") {
		return 0, false
	}
	var number int64
	if _, err := fmt.Sscanf(id, "task-%d", &number); err != nil || number < 0 || number > maxJSONSafeInteger || id != fmt.Sprintf("task-%d", number) {
		return 0, false
	}
	return int(number), true
}

type teamMembership struct {
	root *Session
	id   string
	role string
	name string
}

type teamRootRuntime struct {
	op      sync.Mutex
	waitMu  sync.Mutex
	changed chan struct{}
}

// teamCreation is an admitted SpawnTeammate operation. It remains tracked
// until the durable provisioning edge has reached a terminal state so service
// disposal can cancel and wait for it before discovering live children.
type teamCreation struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
}

// TeamService exposes Agent Teams as a reusable Go library service.
type TeamService struct {
	engine *Engine
	config AgentTeamConfig

	mu     sync.Mutex
	roots  map[string]*teamRootRuntime
	closed bool

	creations map[*teamCreation]struct{}
	closeDone chan struct{}
	closeErr  error
}

func newTeamService(engine *Engine, config AgentTeamConfig) (*TeamService, error) {
	normalized, err := normalizeAgentTeamConfig(config)
	if err != nil {
		return nil, err
	}
	return &TeamService{
		engine: engine, config: normalized, roots: map[string]*teamRootRuntime{},
		creations: map[*teamCreation]struct{}{},
	}, nil
}

func (t *TeamService) runtime(rootID string) *teamRootRuntime {
	t.mu.Lock()
	defer t.mu.Unlock()
	if runtime := t.roots[rootID]; runtime != nil {
		return runtime
	}
	runtime := &teamRootRuntime{changed: make(chan struct{})}
	t.roots[rootID] = runtime
	return runtime
}

func (t *TeamService) notify(rootID string) {
	runtime := t.runtime(rootID)
	runtime.waitMu.Lock()
	close(runtime.changed)
	runtime.changed = make(chan struct{})
	runtime.waitMu.Unlock()
}

func (t *TeamService) checkOpen() error {
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return teamError("TEAM_DISPOSED", "Agent Teams service is disposing")
	}
	return nil
}

func (t *TeamService) beginCreation(ctx context.Context) (context.Context, *teamCreation, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, nil, teamError("TEAM_DISPOSED", "Agent Teams service is disposing")
	}
	operationCtx, cancel := context.WithCancelCause(ctx)
	creation := &teamCreation{cancel: cancel, done: make(chan struct{})}
	t.creations[creation] = struct{}{}
	return operationCtx, creation, nil
}

func (t *TeamService) finishCreation(creation *teamCreation) bool {
	if creation == nil {
		return false
	}
	t.mu.Lock()
	closed := t.closed
	if _, ok := t.creations[creation]; ok {
		delete(t.creations, creation)
		close(creation.done)
	}
	t.mu.Unlock()
	creation.cancel(nil)
	return closed
}

func (t *TeamService) creationContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			var teamErr *TeamError
			if errors.As(cause, &teamErr) && teamErr.Code == "TEAM_DISPOSED" {
				return teamErr
			}
		}
		return err
	}
	return t.checkOpen()
}

func (t *TeamService) state(root *Session) (*teamFoldState, error) {
	root.mu.Lock()
	id := root.Header.ID
	events := append([]Event(nil), root.Events...)
	root.mu.Unlock()
	return foldTeam(id, events)
}

func (t *TeamService) membership(callerID string) (teamMembership, error) {
	caller, err := t.engine.getSession(callerID)
	if err != nil {
		return teamMembership{}, teamError("TEAM_NOT_MEMBER", fmt.Sprintf("agent %q is not a member of an active Agent Team", callerID))
	}
	caller.mu.Lock()
	header := caller.Header
	caller.mu.Unlock()
	if header.Origin != "subagent" {
		return teamMembership{root: caller, id: header.ID, role: "lead", name: teamLeadName}, nil
	}
	root, err := t.engine.getSession(header.ParentSession)
	if err != nil {
		return teamMembership{}, teamError("TEAM_NOT_MEMBER", fmt.Sprintf("agent %q is not a member of an active Agent Team", callerID))
	}
	state, err := t.state(root)
	if err != nil {
		return teamMembership{}, err
	}
	member, ok := state.members[callerID]
	if !ok || (member.Phase != teamMemberActive && member.Phase != teamMemberProvisioning) {
		return teamMembership{}, teamError("TEAM_NOT_MEMBER", fmt.Sprintf("agent %q is not a member of an active Agent Team", callerID))
	}
	return teamMembership{root: root, id: root.Header.ID, role: "teammate", name: member.Name}, nil
}

func (t *TeamService) append(root *Session, typ string, data any) error {
	if _, err := t.engine.appendEventWithMetadata(root, typ, data, nil, nil, false); err != nil {
		return err
	}
	root.mu.Lock()
	rootID := root.Header.ID
	root.mu.Unlock()
	t.notify(rootID)
	return nil
}

func requiredTeamText(value, field string, max int) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", teamError("TEAM_INVALID_ARGUMENT", field+" must be non-empty")
	}
	if len([]rune(value)) > max {
		return "", teamError("TEAM_INVALID_ARGUMENT", fmt.Sprintf("%s exceeds %d characters", field, max))
	}
	return value, nil
}

func teamMemberName(value string) (string, error) {
	if !teamMemberNamePattern.MatchString(value) || len(value) > 64 || value == teamLeadName {
		return "", teamError("TEAM_INVALID_MEMBER_NAME", "teammate name must be lower-kebab-case, at most 64 characters, and not \"lead\"")
	}
	return value, nil
}

func (t *TeamService) resolveActiveMember(root *Session, state *teamFoldState, raw string) (string, string, error) {
	name := strings.TrimSpace(raw)
	root.mu.Lock()
	rootID := root.Header.ID
	root.mu.Unlock()
	if name == teamLeadName {
		return rootID, name, nil
	}
	id := state.memberNames[name]
	member, ok := state.members[id]
	if !ok || member.Phase != teamMemberActive {
		return "", "", teamError("TEAM_MEMBER_NOT_FOUND", fmt.Sprintf("active teammate %q not found", name))
	}
	return member.ID, member.Name, nil
}

func sessionTeamStatus(session *Session) string {
	if session == nil {
		return "inactive"
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.Running {
		return "running"
	}
	if session.attached {
		return "idle"
	}
	return "inactive"
}

func (t *TeamService) memberView(root *Session, state *teamFoldState, member TeamMemberSnapshot) TeamMemberView {
	live, _ := t.engine.getSession(member.ID)
	model := ""
	if live != nil {
		live.mu.Lock()
		model = live.Model.Model
		live.mu.Unlock()
	}
	if model == "" {
		root.mu.Lock()
		model = root.Model.Model
		root.mu.Unlock()
	}
	status := member.Phase
	if member.Phase == teamMemberActive {
		status = sessionTeamStatus(live)
	}
	diagnostics := []string{}
	if member.Error != "" {
		diagnostics = append(diagnostics, member.Error)
	}
	return TeamMemberView{
		ID: member.ID, Name: member.Name, Role: "teammate", Status: status,
		Description: member.Description, Provider: member.Provider, Context: member.Context,
		Model: model, Diagnostics: diagnostics,
	}
}

// ListMembers returns the Lead and durable teammates in creation order.
func (t *TeamService) ListMembers(callerID string) ([]TeamMemberView, error) {
	membership, err := t.membership(callerID)
	if err != nil {
		return nil, err
	}
	state, err := t.state(membership.root)
	if err != nil {
		return nil, err
	}
	membership.root.mu.Lock()
	rootID, rootModel := membership.root.Header.ID, membership.root.Model.Model
	membership.root.mu.Unlock()
	result := []TeamMemberView{{
		ID: rootID, Name: teamLeadName, Role: "lead", Status: sessionTeamStatus(membership.root),
		Model: rootModel, Diagnostics: []string{},
	}}
	for _, id := range state.memberOrder {
		result = append(result, t.memberView(membership.root, state, state.members[id]))
	}
	return result, nil
}

// SpawnTeammate creates a named durable continuable child.
func (t *TeamService) SpawnTeammate(ctx context.Context, callerID string, request SpawnTeammateRequest) (result SpawnTeammateResult, resultErr error) {
	if err := t.checkOpen(); err != nil {
		return SpawnTeammateResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SpawnTeammateResult{}, err
	}
	membership, err := t.membership(callerID)
	if err != nil {
		return SpawnTeammateResult{}, err
	}
	if membership.role != "lead" {
		return SpawnTeammateResult{}, teamError("TEAM_LEAD_REQUIRED", "only the Team Lead can create teammates")
	}
	name, err := teamMemberName(request.Name)
	if err != nil {
		return SpawnTeammateResult{}, err
	}
	description, err := requiredTeamText(request.Description, "description", 200)
	if err != nil {
		return SpawnTeammateResult{}, err
	}
	provider, err := requiredTeamText(request.Provider, "provider", 200)
	if err != nil {
		return SpawnTeammateResult{}, err
	}
	if request.Context != "fresh" && request.Context != "fork" {
		return SpawnTeammateResult{}, teamError("TEAM_INVALID_ARGUMENT", "context must be fresh or fork")
	}
	if len(request.Prompt) == 0 {
		return SpawnTeammateResult{}, teamError("TEAM_INVALID_ARGUMENT", "prompt must be non-empty")
	}
	rootID := membership.id
	runtime := t.runtime(rootID)
	childID := ""
	operationCtx, creation, err := t.beginCreation(ctx)
	if err != nil {
		return SpawnTeammateResult{}, err
	}
	defer func() {
		if t.finishCreation(creation) && resultErr == nil {
			if childID != "" {
				_ = t.engine.CancelSession(childID)
			}
			result = SpawnTeammateResult{}
			resultErr = teamError("TEAM_DISPOSED", "Agent Teams service is disposing")
		}
	}()
	childID = newID("ses")
	member := TeamMemberSnapshot{
		ID: childID, Name: name, Description: description, Provider: provider,
		Context: request.Context, Phase: teamMemberProvisioning,
	}
	runtime.op.Lock()
	stateErr := t.creationContextError(operationCtx)
	state := (*teamFoldState)(nil)
	if stateErr == nil {
		state, stateErr = t.state(membership.root)
	}
	if stateErr == nil {
		if state.memberNames[name] != "" {
			stateErr = teamError("TEAM_MEMBER_NAME_TAKEN", fmt.Sprintf("teammate name %q was already used in this Team", name))
		} else if len(state.members) >= t.config.MaxMembers {
			stateErr = teamError("TEAM_MEMBER_LIMIT", fmt.Sprintf("Team member limit %d reached", t.config.MaxMembers))
		}
	}
	if stateErr == nil {
		stateErr = t.append(membership.root, "team/member", teamMemberEvent{Version: teamEventVersion, TeamID: rootID, Member: member})
	}
	runtime.op.Unlock()
	if stateErr != nil {
		return SpawnTeammateResult{}, stateErr
	}

	config := SubagentToolConfig{Provider: provider, BackgroundMode: "continuable"}
	fork := request.Context == "fork"
	_, createErr := t.engine.createModelSubagentWithID(operationCtx, callerID, childID, description, fork, "continuable", config)
	if operationErr := t.creationContextError(operationCtx); operationErr != nil {
		createErr = operationErr
	}
	if createErr == nil {
		child, getErr := t.engine.getSession(childID)
		if getErr != nil {
			createErr = getErr
		} else if operationErr := t.creationContextError(operationCtx); operationErr != nil {
			createErr = operationErr
		} else {
			_, createErr = t.engine.enqueueTeamPrompt(child, cloneContentBlocks(request.Prompt), map[string]any{
				"kind": "coordinator", "form": "relay", "senderSessionId": callerID,
			}, "next-turn", true)
		}
	}
	if createErr == nil {
		createErr = t.creationContextError(operationCtx)
	}
	terminal := member
	terminal.Phase = teamMemberActive
	if createErr != nil {
		terminal.Phase, terminal.Error = teamMemberFailed, createErr.Error()
		_ = t.engine.CancelSession(childID)
	}
	runtime.op.Lock()
	terminalErr := t.append(membership.root, "team/member", teamMemberEvent{Version: teamEventVersion, TeamID: rootID, Member: terminal})
	runtime.op.Unlock()
	if createErr != nil {
		if terminalErr != nil {
			return SpawnTeammateResult{}, errors.Join(createErr, terminalErr)
		}
		return SpawnTeammateResult{}, createErr
	}
	if terminalErr != nil {
		return SpawnTeammateResult{}, terminalErr
	}
	state, err = t.state(membership.root)
	if err != nil {
		return SpawnTeammateResult{}, err
	}
	return SpawnTeammateResult{Member: t.memberView(membership.root, state, terminal)}, nil
}

func teamMessageContent(message TeamMessageSnapshot) []ContentBlock {
	return append([]ContentBlock{{Type: "text", Text: fmt.Sprintf("Team message %s from %s:", message.ID, message.SenderName)}}, cloneContentBlocks(message.Content)...)
}

func teamMessageSource(rootID string, message TeamMessageSnapshot) map[string]any {
	return map[string]any{
		"kind": "team-message", "teamId": rootID, "messageId": message.ID,
		"senderId": message.SenderID, "senderName": message.SenderName,
	}
}

func sessionHasTeamMessage(session *Session, messageID string) bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, item := range append(append([]*queuedPrompt(nil), session.pending...), session.steering...) {
		if item != nil && item.source["kind"] == "team-message" && item.source["messageId"] == messageID {
			return true
		}
	}
	for _, event := range session.Events {
		if event.Type != "user/message" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		source, _ := data["source"].(map[string]any)
		if source["kind"] == "team-message" && source["messageId"] == messageID {
			return true
		}
	}
	return false
}

func (t *TeamService) attachForWakeup(ctx context.Context, target *Session) error {
	target.mu.Lock()
	header := target.Header
	attached := target.attached
	target.mu.Unlock()
	if attached {
		return nil
	}
	_, err := t.engine.createSession(ctx, header, false)
	return err
}

func (t *TeamService) dispatchLocked(ctx context.Context, root *Session, message TeamMessageSnapshot) (bool, error) {
	target, err := t.engine.getSession(message.TargetID)
	if err != nil {
		return false, nil
	}
	if sessionHasTeamMessage(target, message.ID) {
		return true, nil
	}
	root.mu.Lock()
	rootID := root.Header.ID
	root.mu.Unlock()
	if message.Delivery == teamMessageQuiet {
		target.mu.Lock()
		attached := target.attached
		target.mu.Unlock()
		if !attached {
			return false, nil
		}
		_, err = t.engine.enqueueTeamPrompt(target, teamMessageContent(message), teamMessageSource(rootID, message), "next-step", false)
		return err == nil, err
	}
	if err := t.attachForWakeup(ctx, target); err != nil {
		return false, err
	}
	_, err = t.engine.enqueueTeamPrompt(target, teamMessageContent(message), teamMessageSource(rootID, message), "next-turn", true)
	return err == nil, err
}

// SendMessage durably queues a peer message and attempts immediate delivery.
func (t *TeamService) SendMessage(ctx context.Context, callerID string, request SendTeamMessageRequest) (SendTeamMessageResult, error) {
	if err := t.checkOpen(); err != nil {
		return SendTeamMessageResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SendTeamMessageResult{}, err
	}
	if request.Delivery != teamMessageQuiet && request.Delivery != teamMessageWakeup {
		return SendTeamMessageResult{}, teamError("TEAM_INVALID_ARGUMENT", "delivery must be quiet or wakeup")
	}
	membership, err := t.membership(callerID)
	if err != nil {
		return SendTeamMessageResult{}, err
	}
	runtime := t.runtime(membership.id)
	runtime.op.Lock()
	defer runtime.op.Unlock()
	state, err := t.state(membership.root)
	if err != nil {
		return SendTeamMessageResult{}, err
	}
	targetID, targetName, err := t.resolveActiveMember(membership.root, state, request.Target)
	if err != nil {
		return SendTeamMessageResult{}, err
	}
	if targetID == callerID {
		return SendTeamMessageResult{}, teamError("TEAM_SELF_MESSAGE", "a Team member cannot message itself")
	}
	pending := 0
	for _, message := range state.messages {
		if message.TargetID == targetID && !state.delivered[message.ID] {
			pending++
		}
	}
	if pending >= t.config.MaxPendingMessagesPerMember {
		return SendTeamMessageResult{}, teamError("TEAM_MAILBOX_FULL", fmt.Sprintf("teammate %q has %d pending messages", targetName, pending))
	}
	message := TeamMessageSnapshot{
		ID: newID("team-message"), SenderID: callerID, SenderName: membership.name,
		TargetID: targetID, Delivery: request.Delivery, Content: cloneContentBlocks(request.Content),
	}
	encoded, _ := json.Marshal(teamMessageContent(message))
	if len(encoded) > t.config.MaxMessageBytes {
		return SendTeamMessageResult{}, teamError("TEAM_MESSAGE_TOO_LARGE", fmt.Sprintf("team message exceeds %d bytes", t.config.MaxMessageBytes))
	}
	if err := t.append(membership.root, "team/message/queued", teamMessageQueuedEvent{Version: teamEventVersion, TeamID: membership.id, Message: message}); err != nil {
		return SendTeamMessageResult{}, err
	}
	accepted, dispatchErr := t.dispatchLocked(ctx, membership.root, message)
	if dispatchErr == nil && accepted {
		dispatchErr = t.append(membership.root, "team/message/delivered", teamMessageDeliveredEvent{
			Version: teamEventVersion, TeamID: membership.id, MessageID: message.ID, TargetID: targetID,
		})
	}
	if dispatchErr != nil {
		return SendTeamMessageResult{MessageID: message.ID, Status: "queued"}, nil
	}
	status := "queued"
	if accepted {
		status = "accepted"
	}
	return SendTeamMessageResult{MessageID: message.ID, Status: status}, nil
}

func normalizeTeamWriteScope(value string) (string, error) {
	normalized := strings.ReplaceAll(value, "\\", "/")
	normalized = strings.TrimPrefix(normalized, "./")
	normalized = strings.TrimRight(normalized, "/")
	segments := strings.Split(normalized, "/")
	invalidDrive := len(normalized) >= 2 && normalized[1] == ':' && ((normalized[0] >= 'a' && normalized[0] <= 'z') || (normalized[0] >= 'A' && normalized[0] <= 'Z'))
	invalid := normalized == "" || strings.HasPrefix(normalized, "/") || invalidDrive
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			invalid = true
		}
	}
	if invalid {
		return "", teamError("TEAM_INVALID_WRITE_SCOPE", fmt.Sprintf("invalid workspace-relative write scope %q", value))
	}
	return normalized, nil
}

func normalizeTeamWriteScopes(values []string) ([]string, error) {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		normalized, err := normalizeTeamWriteScope(value)
		if err != nil {
			return nil, err
		}
		if !seen[normalized] {
			seen[normalized] = true
			result = append(result, normalized)
		}
	}
	return result, nil
}

func normalizeTeamDependencies(values []string, state *teamFoldState, self string) ([]string, error) {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, id := range values {
		if id == self {
			return nil, teamError("TEAM_TASK_DEPENDENCY_CYCLE", "a team task cannot block itself")
		}
		if seen[id] {
			return nil, teamError("TEAM_INVALID_ARGUMENT", fmt.Sprintf("duplicate blocker %q", id))
		}
		task, exists := state.tasks[id]
		if !exists || task.Status == teamTaskDeleted {
			return nil, teamError("TEAM_TASK_NOT_FOUND", fmt.Sprintf("blocker task %q not found", id))
		}
		seen[id] = true
		result = append(result, id)
	}
	return result, nil
}

func validateTeamTaskGraph(current map[string]TeamTaskSnapshot, candidate TeamTaskSnapshot) error {
	tasks := make(map[string]TeamTaskSnapshot, len(current)+1)
	for id, task := range current {
		tasks[id] = task
	}
	tasks[candidate.ID] = candidate
	for _, task := range tasks {
		if task.Status == teamTaskDeleted {
			continue
		}
		seen := map[string]bool{}
		for _, blockerID := range task.BlockedBy {
			if blockerID == task.ID {
				return teamError("TEAM_TASK_DEPENDENCY_CYCLE", fmt.Sprintf("team task %q cannot block itself", task.ID))
			}
			if seen[blockerID] {
				return teamError("TEAM_INVALID_ARGUMENT", fmt.Sprintf("team task %q repeats blocker %q", task.ID, blockerID))
			}
			blocker, exists := tasks[blockerID]
			if !exists || blocker.Status == teamTaskDeleted {
				return teamError("TEAM_TASK_NOT_FOUND", fmt.Sprintf("blocker task %q for %q is missing or deleted", blockerID, task.ID))
			}
			seen[blockerID] = true
		}
	}
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if visiting[id] {
			return teamError("TEAM_TASK_DEPENDENCY_CYCLE", fmt.Sprintf("task dependency cycle includes %q", id))
		}
		if visited[id] {
			return nil
		}
		task, exists := tasks[id]
		if !exists || task.Status == teamTaskDeleted {
			return nil
		}
		visiting[id] = true
		for _, blocker := range task.BlockedBy {
			if err := visit(blocker); err != nil {
				return err
			}
		}
		delete(visiting, id)
		visited[id] = true
		return nil
	}
	for id := range tasks {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func teamTaskReady(state *teamFoldState, task TeamTaskSnapshot) bool {
	for _, id := range task.BlockedBy {
		if state.tasks[id].Status != teamTaskCompleted {
			return false
		}
	}
	return true
}

func teamScopesOverlap(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}

func teamTaskView(root *Session, state *teamFoldState, task TeamTaskSnapshot) TeamTaskView {
	root.mu.Lock()
	rootID := root.Header.ID
	root.mu.Unlock()
	ownerName := ""
	if task.OwnerID == rootID {
		ownerName = teamLeadName
	} else if task.OwnerID != "" {
		ownerName = state.members[task.OwnerID].Name
	}
	warnings := []string{}
	for _, id := range state.taskOrder {
		other := state.tasks[id]
		if other.ID == task.ID || other.Status != teamTaskInProgress {
			continue
		}
		overlap := false
		for _, left := range task.WriteScopes {
			for _, right := range other.WriteScopes {
				if teamScopesOverlap(left, right) {
					overlap = true
				}
			}
		}
		if overlap {
			warnings = append(warnings, "write scopes overlap with "+other.ID)
		}
	}
	return TeamTaskView{
		ID: task.ID, Revision: task.Revision, Subject: task.Subject, Description: task.Description,
		Status: task.Status, OwnerName: ownerName, BlockedBy: append([]string(nil), task.BlockedBy...),
		WriteScopes: append([]string(nil), task.WriteScopes...),
		Ready:       task.Status == teamTaskPending && teamTaskReady(state, task), WriteScopeWarnings: warnings,
	}
}

// CreateTask creates an unowned pending task in the Lead log.
func (t *TeamService) CreateTask(callerID string, request CreateTeamTaskRequest) (TeamTaskView, error) {
	membership, err := t.membership(callerID)
	if err != nil {
		return TeamTaskView{}, err
	}
	runtime := t.runtime(membership.id)
	runtime.op.Lock()
	defer runtime.op.Unlock()
	state, err := t.state(membership.root)
	if err != nil {
		return TeamTaskView{}, err
	}
	active := 0
	for _, task := range state.tasks {
		if task.Status != teamTaskDeleted {
			active++
		}
	}
	if active >= t.config.MaxTasks {
		return TeamTaskView{}, teamError("TEAM_TASK_LIMIT", fmt.Sprintf("Team task limit %d reached", t.config.MaxTasks))
	}
	subject, err := requiredTeamText(request.Subject, "subject", 200)
	if err != nil {
		return TeamTaskView{}, err
	}
	description, err := requiredTeamText(request.Description, "description", 16_384)
	if err != nil {
		return TeamTaskView{}, err
	}
	dependencies, err := normalizeTeamDependencies(request.BlockedBy, state, "")
	if err != nil {
		return TeamTaskView{}, err
	}
	scopes, err := normalizeTeamWriteScopes(request.WriteScopes)
	if err != nil {
		return TeamTaskView{}, err
	}
	id := fmt.Sprintf("task-%d", state.nextTaskNumber)
	if _, exists := state.tasks[id]; exists {
		return TeamTaskView{}, teamError("TEAM_TASK_LIMIT", "Team task id space exhausted")
	}
	task := TeamTaskSnapshot{ID: id, Revision: 1, Subject: subject, Description: description, Status: teamTaskPending, BlockedBy: dependencies, WriteScopes: scopes}
	if err := validateTeamTaskGraph(state.tasks, task); err != nil {
		return TeamTaskView{}, err
	}
	if err := t.append(membership.root, "team/task", teamTaskEvent{Version: teamEventVersion, TeamID: membership.id, Task: task}); err != nil {
		return TeamTaskView{}, err
	}
	return teamTaskView(membership.root, state, task), nil
}

// GetTask returns the latest task snapshot, including a deleted tombstone.
func (t *TeamService) GetTask(callerID, taskID string) (TeamTaskView, error) {
	membership, err := t.membership(callerID)
	if err != nil {
		return TeamTaskView{}, err
	}
	state, err := t.state(membership.root)
	if err != nil {
		return TeamTaskView{}, err
	}
	task, exists := state.tasks[taskID]
	if !exists {
		return TeamTaskView{}, teamError("TEAM_TASK_NOT_FOUND", fmt.Sprintf("team task %q not found", taskID))
	}
	return teamTaskView(membership.root, state, task), nil
}

// ListTasks returns current non-deleted tasks in creation order.
func (t *TeamService) ListTasks(callerID string) ([]TeamTaskView, error) {
	membership, err := t.membership(callerID)
	if err != nil {
		return nil, err
	}
	state, err := t.state(membership.root)
	if err != nil {
		return nil, err
	}
	result := []TeamTaskView{}
	for _, id := range state.taskOrder {
		task := state.tasks[id]
		if task.Status != teamTaskDeleted {
			result = append(result, teamTaskView(membership.root, state, task))
		}
	}
	return result, nil
}

// UpdateTask applies one compare-and-set task transition.
func (t *TeamService) UpdateTask(callerID string, request UpdateTeamTaskRequest) (TeamTaskView, error) {
	membership, err := t.membership(callerID)
	if err != nil {
		return TeamTaskView{}, err
	}
	runtime := t.runtime(membership.id)
	runtime.op.Lock()
	defer runtime.op.Unlock()
	state, err := t.state(membership.root)
	if err != nil {
		return TeamTaskView{}, err
	}
	current, exists := state.tasks[request.TaskID]
	if !exists {
		return TeamTaskView{}, teamError("TEAM_TASK_NOT_FOUND", fmt.Sprintf("team task %q not found", request.TaskID))
	}
	if current.Revision != request.ExpectedRevision {
		return TeamTaskView{}, teamError("TEAM_TASK_STALE_REVISION", fmt.Sprintf("stale team task %q revision %d; current revision is %d", current.ID, request.ExpectedRevision, current.Revision))
	}
	if current.Status == teamTaskDeleted {
		return TeamTaskView{}, teamError("TEAM_TASK_DELETED", fmt.Sprintf("team task %q is deleted", current.ID))
	}
	lead := membership.role == "lead"
	owner := current.OwnerID == callerID
	authorizeOwner := func() error {
		if !lead && !owner {
			return teamError("TEAM_TASK_UNAUTHORIZED", "task mutation requires its owner or Team Lead")
		}
		return nil
	}
	next := cloneTeamTask(current)
	switch request.Action {
	case "claim":
		if current.OwnerID != "" && current.OwnerID != callerID {
			return TeamTaskView{}, teamError("TEAM_TASK_ALREADY_CLAIMED", fmt.Sprintf("team task %q is owned by another member", current.ID))
		}
		if current.Status != teamTaskPending || !teamTaskReady(state, current) {
			return TeamTaskView{}, teamError("TEAM_TASK_BLOCKED", fmt.Sprintf("team task %q is not ready to claim", current.ID))
		}
		next.Status, next.OwnerID = teamTaskInProgress, callerID
	case "release":
		if err := authorizeOwner(); err != nil {
			return TeamTaskView{}, err
		}
		if current.Status != teamTaskInProgress {
			return TeamTaskView{}, teamError("TEAM_TASK_INVALID_TRANSITION", "only an in-progress task can be released")
		}
		next.Status, next.OwnerID = teamTaskPending, ""
	case "edit":
		if err := authorizeOwner(); err != nil {
			return TeamTaskView{}, err
		}
		if request.Subject == nil && request.Description == nil && !request.WriteScopesSet {
			return TeamTaskView{}, teamError("TEAM_INVALID_ARGUMENT", "task edit requires subject, description, or write_scopes")
		}
		if request.Subject != nil {
			next.Subject, err = requiredTeamText(*request.Subject, "subject", 200)
		}
		if err == nil && request.Description != nil {
			next.Description, err = requiredTeamText(*request.Description, "description", 16_384)
		}
		if err == nil && request.WriteScopesSet {
			next.WriteScopes, err = normalizeTeamWriteScopes(request.WriteScopes)
		}
		if err != nil {
			return TeamTaskView{}, err
		}
	case "set_dependencies":
		if err := authorizeOwner(); err != nil {
			return TeamTaskView{}, err
		}
		if !request.BlockedBySet {
			return TeamTaskView{}, teamError("TEAM_INVALID_ARGUMENT", "set_dependencies requires blocked_by")
		}
		next.BlockedBy, err = normalizeTeamDependencies(request.BlockedBy, state, current.ID)
		if err != nil {
			return TeamTaskView{}, err
		}
	case "complete":
		if err := authorizeOwner(); err != nil {
			return TeamTaskView{}, err
		}
		if current.Status != teamTaskInProgress {
			return TeamTaskView{}, teamError("TEAM_TASK_INVALID_TRANSITION", "only an in-progress task can complete")
		}
		next.Status = teamTaskCompleted
	case "reopen":
		if err := authorizeOwner(); err != nil {
			return TeamTaskView{}, err
		}
		if current.Status != teamTaskCompleted {
			return TeamTaskView{}, teamError("TEAM_TASK_INVALID_TRANSITION", "only a completed task can reopen")
		}
		next.Status, next.OwnerID = teamTaskPending, ""
	case "reassign":
		if !lead {
			return TeamTaskView{}, teamError("TEAM_LEAD_REQUIRED", "only the Team Lead can reassign tasks")
		}
		if current.Status != teamTaskPending && current.Status != teamTaskInProgress {
			return TeamTaskView{}, teamError("TEAM_TASK_INVALID_TRANSITION", "only a pending or in-progress task can be reassigned")
		}
		if request.Owner == nil || strings.TrimSpace(*request.Owner) == "" {
			next.Status, next.OwnerID = teamTaskPending, ""
			break
		}
		if !teamTaskReady(state, current) {
			return TeamTaskView{}, teamError("TEAM_TASK_BLOCKED", fmt.Sprintf("team task %q is blocked", current.ID))
		}
		assignee, _, resolveErr := t.resolveActiveMember(membership.root, state, *request.Owner)
		if resolveErr != nil {
			return TeamTaskView{}, resolveErr
		}
		next.Status, next.OwnerID = teamTaskInProgress, assignee
	case "delete":
		if err := authorizeOwner(); err != nil {
			return TeamTaskView{}, err
		}
		for _, task := range state.tasks {
			if task.Status == teamTaskDeleted || task.ID == current.ID {
				continue
			}
			for _, blocker := range task.BlockedBy {
				if blocker == current.ID {
					return TeamTaskView{}, teamError("TEAM_TASK_HAS_DEPENDENTS", fmt.Sprintf("team task %q still blocks %q", current.ID, task.ID))
				}
			}
		}
		next.Status = teamTaskDeleted
	default:
		return TeamTaskView{}, teamError("TEAM_INVALID_ARGUMENT", fmt.Sprintf("unsupported task action %s", request.Action))
	}
	next.Revision++
	if err := validateTeamTaskGraph(state.tasks, next); err != nil {
		return TeamTaskView{}, err
	}
	if err := t.append(membership.root, "team/task", teamTaskEvent{Version: teamEventVersion, TeamID: membership.id, Task: next}); err != nil {
		return TeamTaskView{}, err
	}
	return teamTaskView(membership.root, state, next), nil
}

// WaitForChange waits for a later Team event or status notification.
func (t *TeamService) WaitForChange(ctx context.Context, callerID string, timeout time.Duration) (TeamWaitResult, error) {
	if timeout < minimumTeamWait || timeout > maximumTeamWait || timeout%time.Millisecond != 0 {
		return TeamWaitResult{}, teamError("TEAM_INVALID_TIMEOUT", "timeout must be from 10s through 1h in whole milliseconds")
	}
	membership, err := t.membership(callerID)
	if err != nil {
		return TeamWaitResult{}, err
	}
	runtime := t.runtime(membership.id)
	runtime.waitMu.Lock()
	changed := runtime.changed
	runtime.waitMu.Unlock()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return TeamWaitResult{}, ctx.Err()
	case <-changed:
		return TeamWaitResult{TimedOut: false}, nil
	case <-timer.C:
		return TeamWaitResult{TimedOut: true}, nil
	}
}

// Interrupt cancels one live teammate turn while preserving its inbox.
func (t *TeamService) Interrupt(callerID, targetName string) (string, error) {
	membership, err := t.membership(callerID)
	if err != nil {
		return "", err
	}
	if membership.role != "lead" {
		return "", teamError("TEAM_LEAD_REQUIRED", "only the Team Lead can interrupt teammates")
	}
	state, err := t.state(membership.root)
	if err != nil {
		return "", err
	}
	targetID, _, err := t.resolveActiveMember(membership.root, state, targetName)
	if err != nil {
		return "", err
	}
	if targetID == membership.id {
		return "", teamError("TEAM_INVALID_TARGET", "the Team Lead cannot interrupt itself")
	}
	target, getErr := t.engine.getSession(targetID)
	previous := "inactive"
	if getErr == nil {
		previous = sessionTeamStatus(target)
		if previous == "running" {
			if err := t.engine.CancelSession(targetID); err != nil {
				return "", err
			}
		}
	}
	t.notify(membership.id)
	return previous, nil
}

func (t *TeamService) recoverRoot(ctx context.Context, root *Session) {
	root.mu.Lock()
	rootID := root.Header.ID
	root.mu.Unlock()
	runtime := t.runtime(rootID)
	runtime.op.Lock()
	defer runtime.op.Unlock()
	state, err := t.state(root)
	if err != nil {
		return
	}
	for _, id := range state.messageOrder {
		if state.delivered[id] {
			continue
		}
		message := state.messages[id]
		accepted, dispatchErr := t.dispatchLocked(ctx, root, message)
		if dispatchErr != nil || !accepted {
			continue
		}
		if err := t.append(root, "team/message/delivered", teamMessageDeliveredEvent{
			Version: teamEventVersion, TeamID: rootID, MessageID: message.ID, TargetID: message.TargetID,
		}); err != nil {
			return
		}
	}
}

func (t *TeamService) recoverAll() {
	t.engine.mu.RLock()
	sessions := make([]*Session, 0, len(t.engine.sessions))
	for _, session := range t.engine.sessions {
		sessions = append(sessions, session)
	}
	t.engine.mu.RUnlock()
	for _, session := range sessions {
		session.mu.Lock()
		root := session.Header.Origin != "subagent"
		session.mu.Unlock()
		if root {
			t.recoverRoot(context.Background(), session)
		}
	}
}

func (t *TeamService) recoverSession(id string) {
	membership, err := t.membership(id)
	if err != nil {
		return
	}
	t.recoverRoot(context.Background(), membership.root)
}

func (t *TeamService) notifyStatus(sessionID string) {
	membership, err := t.membership(sessionID)
	if err == nil {
		t.notify(membership.id)
	}
}

func (t *TeamService) activePeer(callerID string) bool {
	members, err := t.ListMembers(callerID)
	if err != nil {
		return false
	}
	for _, member := range members {
		if member.ID != callerID && (member.Status == "running" || member.Status == teamMemberProvisioning) {
			return true
		}
	}
	return false
}

func (t *TeamService) close() error {
	t.mu.Lock()
	if t.closed {
		done := t.closeDone
		t.mu.Unlock()
		if done != nil {
			<-done
		}
		t.mu.Lock()
		err := t.closeErr
		t.mu.Unlock()
		return err
	}
	t.closed = true
	t.closeDone = make(chan struct{})
	roots := make([]*teamRootRuntime, 0, len(t.roots))
	for _, runtime := range t.roots {
		roots = append(roots, runtime)
	}
	creations := make([]*teamCreation, 0, len(t.creations))
	for creation := range t.creations {
		creations = append(creations, creation)
	}
	t.mu.Unlock()
	for _, runtime := range roots {
		runtime.waitMu.Lock()
		close(runtime.changed)
		runtime.changed = make(chan struct{})
		runtime.waitMu.Unlock()
	}
	for _, creation := range creations {
		creation.cancel(teamError("TEAM_DISPOSED", "Agent Teams service is disposing"))
	}
	var failures []error
	if err := waitTeamCreations(creations, t.config.DisposalTimeout); err != nil {
		failures = append(failures, err)
	}
	for rootID, childIDs := range t.rosterChildrenByRoot() {
		ctx, cancel := context.WithTimeout(context.Background(), t.config.DisposalTimeout)
		err := t.engine.DrainSubagentChildren(ctx, rootID, childIDs)
		cancel()
		if err != nil {
			failures = append(failures, err)
		}
	}
	err := errors.Join(failures...)
	t.mu.Lock()
	t.closeErr = err
	close(t.closeDone)
	t.mu.Unlock()
	return err
}

func waitTeamCreations(creations []*teamCreation, timeout time.Duration) error {
	if len(creations) == 0 {
		return nil
	}
	done := make(chan struct{})
	go func() {
		for _, creation := range creations {
			<-creation.done
		}
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return teamError("TEAM_DISPOSAL_TIMEOUT", fmt.Sprintf("Agent Teams runtime disposal exceeded %s", timeout))
	}
}

func (t *TeamService) rosterChildrenByRoot() map[string][]string {
	t.engine.mu.RLock()
	sessions := make([]*Session, 0, len(t.engine.sessions))
	for _, session := range t.engine.sessions {
		sessions = append(sessions, session)
	}
	t.engine.mu.RUnlock()
	result := map[string][]string{}
	for _, root := range sessions {
		root.mu.Lock()
		rootID, rootAttached, rootOrigin := root.Header.ID, root.attached, root.Header.Origin
		root.mu.Unlock()
		if !rootAttached || rootOrigin == "subagent" {
			continue
		}
		state, err := t.state(root)
		if err != nil {
			continue
		}
		for _, memberID := range state.memberOrder {
			child, err := t.engine.getSession(memberID)
			if err != nil {
				continue
			}
			child.mu.Lock()
			match := child.attached && child.Header.Origin == "subagent" && child.Header.ParentSession == rootID
			child.mu.Unlock()
			if match {
				result[rootID] = append(result[rootID], memberID)
			}
		}
	}
	return result
}

func sortedTeamTasks(tasks []TeamTaskView) {
	sort.SliceStable(tasks, func(i, j int) bool {
		left, leftOK := numericTeamTaskID(tasks[i].ID)
		right, rightOK := numericTeamTaskID(tasks[j].ID)
		if leftOK && rightOK {
			return left < right
		}
		return tasks[i].ID < tasks[j].ID
	})
}
