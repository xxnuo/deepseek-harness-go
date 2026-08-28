package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func newAgentTeamEngine(t *testing.T, dataDir string, sqlite bool) *Engine {
	return newAgentTeamEngineWithInvariants(t, dataDir, sqlite, false)
}

func newAgentTeamEngineWithInvariants(t *testing.T, dataDir string, sqlite, runtimeInvariants bool) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dataDir, dataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.AgentTeams = &AgentTeamConfig{}
	if runtimeInvariants {
		cfg.RuntimeInvariants = &RuntimeInvariantConfig{}
	}
	cfg.Persist = true
	if sqlite {
		store, err := NewSQLiteSessionStore(filepath.Join(dataDir, "sessions.sqlite"), SQLiteJournalWAL)
		if err != nil {
			t.Fatal(err)
		}
		cfg.SessionStore = store
	}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

// blockingTeamCreateStore models a child creation whose durable commit wins a
// disposal race after observing the service cancellation.
type blockingTeamCreateStore struct {
	SessionStore
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

type blockingTeamFlushStore struct {
	SessionStore
	mu      sync.Mutex
	blockID string
	except  string
	blocked bool
	entered chan struct{}
	release chan struct{}
}

type failingTeamFlushStore struct {
	SessionStore
	mu      sync.Mutex
	failID  string
	failure error
}

func (s *blockingTeamFlushStore) Flush(ctx context.Context, id string) error {
	s.mu.Lock()
	selected := s.blockID != "" && id == s.blockID || s.except != "" && id != s.except
	block := selected && !s.blocked
	if block {
		s.blocked = true
		close(s.entered)
	}
	s.mu.Unlock()
	if block {
		<-s.release
	}
	return s.SessionStore.(SessionPersistenceFlusher).Flush(ctx, id)
}

func (s *blockingTeamFlushStore) Delete(ctx context.Context, id string) error {
	return s.SessionStore.(sessionStoreCreateRollback).Delete(ctx, id)
}

func (s *failingTeamFlushStore) Flush(ctx context.Context, id string) error {
	s.mu.Lock()
	failure := error(nil)
	if id == s.failID && s.failure != nil {
		failure = s.failure
		s.failure = nil
	}
	s.mu.Unlock()
	if failure != nil {
		return failure
	}
	return s.SessionStore.(SessionPersistenceFlusher).Flush(ctx, id)
}

func (s *failingTeamFlushStore) Delete(ctx context.Context, id string) error {
	return s.SessionStore.(sessionStoreCreateRollback).Delete(ctx, id)
}

func (s *blockingTeamCreateStore) Create(ctx context.Context, meta SessionHeader) error {
	if meta.ParentSession == "" {
		return s.SessionStore.Create(ctx, meta)
	}
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	select {
	case s.canceled <- struct{}{}:
	default:
	}
	<-s.release
	return s.SessionStore.Create(context.Background(), meta)
}

func (s *blockingTeamCreateStore) Flush(ctx context.Context, id string) error {
	return s.SessionStore.(SessionPersistenceFlusher).Flush(ctx, id)
}

func (s *blockingTeamCreateStore) Delete(ctx context.Context, id string) error {
	return s.SessionStore.(sessionStoreCreateRollback).Delete(ctx, id)
}

func teamErrorCode(err error) string {
	var target *TeamError
	if errors.As(err, &target) {
		return target.Code
	}
	return ""
}

func TestFoldTeamStrictLifecycleAndForeignForkEvents(t *testing.T) {
	events := []Event{
		{Type: "team/member", Data: map[string]any{
			"version": 99, "teamId": "other", "member": map[string]any{"ignored": true},
		}},
		{Type: "team/member", Data: map[string]any{
			"version": 1, "teamId": "root", "member": map[string]any{
				"id": "child", "name": "worker", "description": "work", "provider": "spawn", "context": "fresh", "phase": "provisioning",
			},
		}},
		{Type: "team/member", Data: map[string]any{
			"version": 1, "teamId": "root", "member": map[string]any{
				"id": "child", "name": "worker", "description": "work", "provider": "spawn", "context": "fresh", "phase": "active",
			},
		}},
	}
	fold, err := FoldTeam("root", events)
	if err != nil {
		t.Fatal(err)
	}
	if len(fold.Members) != 1 || fold.Members[0].Phase != teamMemberActive {
		t.Fatalf("fold = %#v", fold)
	}
	bad := []Event{{Type: "team/member", Data: map[string]any{
		"version": 1, "teamId": "root", "member": map[string]any{
			"id": "child", "name": "worker", "description": "work", "provider": "spawn", "context": "fresh", "phase": "active",
		},
	}}}
	if _, err := FoldTeam("root", bad); err == nil || !strings.Contains(err.Error(), "must begin provisioning") {
		t.Fatalf("active-first fold error = %v", err)
	}
	foreignMalformed := []Event{{Type: "team/task", Data: map[string]any{
		"version": 1, "teamId": "other", "task": map[string]any{
			"id": "task-1", "revision": 1, "subject": 42, "description": "work", "status": "pending",
			"blockedBy": []any{}, "writeScopes": []any{},
		},
	}}}
	if _, err := FoldTeam("root", foreignMalformed); err == nil || !strings.Contains(err.Error(), "payload is invalid") {
		t.Fatalf("foreign current-version payload error = %v", err)
	}
}

func TestFoldTeamStrictContentBlocksPreservePluginVariants(t *testing.T) {
	message := map[string]any{
		"id": "message-1", "senderId": "root", "senderName": "lead", "targetId": "child", "delivery": "quiet",
		"content": []any{map[string]any{"type": "plugin/custom", "payload": map[string]any{"value": 1.0}}},
	}
	fold, err := FoldTeam("root", []Event{{Type: "team/message/queued", Data: map[string]any{
		"version": 1, "teamId": "root", "message": message,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(fold.Messages) != 1 || len(fold.Messages[0].Content) != 1 {
		t.Fatalf("fold = %#v", fold)
	}
	encoded, err := json.Marshal(fold.Messages[0].Content[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"payload":{"value":1},"type":"plugin/custom"}` {
		t.Fatalf("plugin block = %s", encoded)
	}
	malformed := []any{
		map[string]any{"type": "text"},
		map[string]any{"type": "text", "text": "hello", "unexpected": true},
		map[string]any{"type": "image", "attachment": map[string]any{
			"attachmentId": "image", "mediaType": "image/png", "bytes": 1, "width": 1, "height": 1,
			"originalDimensions": map[string]any{"width": 1, "height": 1},
		}},
	}
	for _, block := range malformed {
		candidate := cloneJSON(message).(map[string]any)
		candidate["content"] = []any{block}
		if _, err := FoldTeam("root", []Event{{Type: "team/message/queued", Data: map[string]any{
			"version": 1, "teamId": "root", "message": candidate,
		}}}); err == nil || !strings.Contains(err.Error(), "payload is invalid") {
			t.Fatalf("malformed core block %#v error = %v", block, err)
		}
	}
}

func TestFoldTeamNumericTaskIDsUseJavaScriptNumberSemantics(t *testing.T) {
	fold, err := FoldTeam("root", []Event{{Type: "team/task", Data: map[string]any{
		"version": 1, "teamId": "root", "task": map[string]any{
			"id": "task-0007", "revision": 1, "subject": "work", "description": "work", "status": "pending",
			"blockedBy": []any{}, "writeScopes": []any{},
		},
	}}})
	if err != nil || fold.NextTaskNumber != 8 {
		t.Fatalf("numeric allocation = %#v, %v", fold, err)
	}
	if _, err := FoldTeam("root", []Event{{Type: "team/task", Data: map[string]any{
		"version": 1, "teamId": "root", "task": map[string]any{
			"id": "task-9007199254740992", "revision": 1, "subject": "work", "description": "work", "status": "pending",
			"blockedBy": []any{}, "writeScopes": []any{},
		},
	}}}); err == nil || !strings.Contains(err.Error(), "payload is invalid") {
		t.Fatalf("unsafe numeric task id error = %v", err)
	}
}

func TestAgentTeamConfigAndTextUseUpstreamNumericSemantics(t *testing.T) {
	if _, err := normalizeAgentTeamConfig(AgentTeamConfig{MaxMembers: int(maxJSONSafeInteger) + 1}); teamErrorCode(err) != "TEAM_INVALID_CONFIG" {
		t.Fatalf("unsafe limit error = %v", err)
	}
	if _, err := normalizeAgentTeamConfig(AgentTeamConfig{DisposalTimeout: time.Nanosecond}); teamErrorCode(err) != "TEAM_INVALID_CONFIG" {
		t.Fatalf("fractional millisecond disposal timeout error = %v", err)
	}
	if _, err := requiredTeamText(strings.Repeat("\U0001F600", 101), "description", 200); teamErrorCode(err) != "TEAM_INVALID_ARGUMENT" {
		t.Fatalf("UTF-16 length error = %v", err)
	}
}

func TestAgentTeamInvariantRejectsInvalidCandidateBeforeAppend(t *testing.T) {
	engine := newAgentTeamEngineWithInvariants(t, t.TempDir(), false, true)
	defer engine.Close()
	id, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "team-invariant", "")
	if err != nil {
		t.Fatal(err)
	}
	session := mustSession(t, engine, id)
	session.mu.Lock()
	before := len(session.Events)
	session.mu.Unlock()
	_, err = engine.appendEventWithMetadata(session, "team/member", map[string]any{
		"version": 1, "teamId": id, "member": map[string]any{
			"id": "child", "name": "worker", "description": "work", "provider": "spawn", "context": "fresh", "phase": "active",
		},
	}, nil, nil, false)
	var invariant *InvariantError
	if !errors.As(err, &invariant) || invariant.PackageName != invariantPackageAgentTeam {
		t.Fatalf("invariant error = %v", err)
	}
	session.mu.Lock()
	after := len(session.Events)
	session.mu.Unlock()
	if after != before {
		t.Fatalf("invalid candidate was published: before=%d after=%d", before, after)
	}
}

func TestTeamServiceRunsTeammateMailboxAndTaskBoard(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	defer engine.Close()
	root, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "team-root", "")
	if err != nil {
		t.Fatal(err)
	}
	spawned, err := engine.AgentTeams().SpawnTeammate(context.Background(), root, SpawnTeammateRequest{
		Name: "worker-one", Description: "Own focused implementation", Prompt: []ContentBlock{{Type: "text", Text: "initial work"}},
		Context: "fresh", Provider: "spawn",
	})
	if err != nil {
		t.Fatal(err)
	}
	child := spawned.Member.ID
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := engine.WaitForIdle(waitCtx, child); err != nil {
		t.Fatal(err)
	}
	waitForModelSubagentDetached(t, engine, child)
	members, err := engine.AgentTeams().ListMembers(root)
	if err != nil || len(members) != 2 || members[1].Name != "worker-one" || members[1].Status != "inactive" {
		t.Fatalf("members = %#v, %v", members, err)
	}
	childHistory, _, err := engine.History(child, -1, 100)
	if err != nil {
		t.Fatal(err)
	}
	initialHistory := ""
	for _, entry := range childHistory {
		initialHistory += contentValueText(entry.Event.Data)
	}
	if !strings.Contains(initialHistory, "initial work") {
		t.Fatalf("initial teammate history = %#v", childHistory)
	}

	quiet, err := engine.AgentTeams().SendMessage(context.Background(), root, SendTeamMessageRequest{
		Target: "worker-one", Delivery: teamMessageQuiet, Content: []ContentBlock{{Type: "text", Text: "quiet context"}},
	})
	if err != nil || quiet.Status != "queued" {
		t.Fatalf("quiet = %#v, %v", quiet, err)
	}
	if status := sessionTeamStatus(mustSession(t, engine, child)); status != "inactive" {
		t.Fatalf("quiet message woke child: %s", status)
	}
	followup, err := engine.AgentTeams().SendMessage(context.Background(), root, SendTeamMessageRequest{
		Target: "worker-one", Delivery: teamMessageWakeup, Content: []ContentBlock{{Type: "text", Text: "follow up"}},
	})
	if err != nil || followup.Status != "accepted" {
		t.Fatalf("followup = %#v, %v", followup, err)
	}
	waitCtx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := engine.WaitForIdle(waitCtx2, child); err != nil {
		t.Fatal(err)
	}
	history, _, _ := engine.History(child, -1, 200)
	encoded := ""
	for _, entry := range history {
		encoded += contentValueText(entry.Event.Data)
	}
	if !strings.Contains(encoded, "quiet context") || !strings.Contains(encoded, "follow up") {
		t.Fatalf("mailbox history = %q", encoded)
	}
	if _, err := engine.AgentTeams().SendMessage(context.Background(), root, SendTeamMessageRequest{
		Target: "lead", Delivery: teamMessageQuiet, Content: []ContentBlock{{Type: "text", Text: "self"}},
	}); teamErrorCode(err) != "TEAM_SELF_MESSAGE" {
		t.Fatalf("self-send error = %v", err)
	}

	first, err := engine.AgentTeams().CreateTask(root, CreateTeamTaskRequest{
		Subject: "First", Description: "Do first", WriteScopes: []string{"pkg/core/"},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.AgentTeams().CreateTask(root, CreateTeamTaskRequest{
		Subject: "Second", Description: "Do second", BlockedBy: []string{first.ID}, WriteScopes: []string{"pkg/core/sub"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.AgentTeams().UpdateTask(root, UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: second.Revision, Action: "claim"}); teamErrorCode(err) != "TEAM_TASK_BLOCKED" {
		t.Fatalf("blocked claim error = %v", err)
	}
	first, err = engine.AgentTeams().UpdateTask(root, UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "claim"})
	if err != nil || first.OwnerName != "lead" || first.Status != teamTaskInProgress {
		t.Fatalf("claimed first = %#v, %v", first, err)
	}
	first, err = engine.AgentTeams().UpdateTask(root, UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "complete"})
	if err != nil || first.Status != teamTaskCompleted {
		t.Fatalf("completed first = %#v, %v", first, err)
	}
	second, err = engine.AgentTeams().UpdateTask(root, UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: second.Revision, Action: "claim"})
	if err != nil || second.OwnerName != teamLeadName {
		t.Fatalf("claimed second = %#v, %v", second, err)
	}
	third, err := engine.AgentTeams().CreateTask(root, CreateTeamTaskRequest{
		Subject: "Third", Description: "Overlap", WriteScopes: []string{"pkg/core"},
	})
	if err != nil {
		t.Fatal(err)
	}
	third, err = engine.AgentTeams().UpdateTask(root, UpdateTeamTaskRequest{TaskID: third.ID, ExpectedRevision: third.Revision, Action: "claim"})
	if err != nil || !reflect.DeepEqual(third.WriteScopeWarnings, []string{"write scopes overlap with " + second.ID}) {
		t.Fatalf("overlap warnings = %#v, %v", third, err)
	}

	waitDone := make(chan TeamWaitResult, 1)
	waitErr := make(chan error, 1)
	go func() {
		value, err := engine.AgentTeams().WaitForChange(context.Background(), root, minimumTeamWait)
		waitDone <- value
		waitErr <- err
	}()
	time.Sleep(10 * time.Millisecond)
	if _, err := engine.AgentTeams().CreateTask(root, CreateTeamTaskRequest{Subject: "Wake waiter", Description: "Change"}); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-waitDone:
		if err := <-waitErr; err != nil || result.TimedOut {
			t.Fatalf("wait = %#v, %v", result, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Team waiter did not observe task change")
	}
}

func TestAgentTeamTaskBoardLimitsTombstonesAndIDExhaustion(t *testing.T) {
	newWithLimit := func(maxTasks int) *Engine {
		t.Helper()
		dataDir := t.TempDir()
		cfg := DefaultConfig()
		cfg.DataDir, cfg.Workspace = dataDir, dataDir
		cfg.Provider, cfg.Model = "echo", "echo"
		cfg.SessionTitleLLM.Enabled = false
		cfg.AgentTeams, cfg.Persist = &AgentTeamConfig{MaxTasks: maxTasks}, true
		engine, err := New(WithConfig(cfg))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = engine.Close() })
		return engine
	}
	limited := newWithLimit(1)
	rootID, err := limited.CreateSession(context.Background(), limited.Config().Workspace, "task-limit-root", "")
	if err != nil {
		t.Fatal(err)
	}
	first, err := limited.AgentTeams().CreateTask(rootID, CreateTeamTaskRequest{Subject: "first", Description: "first task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := limited.AgentTeams().CreateTask(rootID, CreateTeamTaskRequest{Subject: "overflow", Description: "overflow"}); teamErrorCode(err) != "TEAM_TASK_LIMIT" {
		t.Fatalf("task limit error = %v", err)
	}
	deleted, err := limited.AgentTeams().UpdateTask(rootID, UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "delete"})
	if err != nil || deleted.Status != teamTaskDeleted {
		t.Fatalf("deleted task = %#v, %v", deleted, err)
	}
	second, err := limited.AgentTeams().CreateTask(rootID, CreateTeamTaskRequest{Subject: "second", Description: "second task"})
	if err != nil || second.ID != "task-2" {
		t.Fatalf("second task = %#v, %v", second, err)
	}
	tombstone, err := limited.AgentTeams().GetTask(rootID, first.ID)
	if err != nil || tombstone.Status != teamTaskDeleted {
		t.Fatalf("tombstone = %#v, %v", tombstone, err)
	}
	listed, err := limited.AgentTeams().ListTasks(rootID)
	if err != nil || len(listed) != 1 || listed[0].ID != second.ID {
		t.Fatalf("listed tasks = %#v, %v", listed, err)
	}

	exhausted := newWithLimit(2)
	exhaustedRootID, err := exhausted.CreateSession(context.Background(), exhausted.Config().Workspace, "task-exhausted-root", "")
	if err != nil {
		t.Fatal(err)
	}
	exhaustedRoot := mustSession(t, exhausted, exhaustedRootID)
	maxID := fmt.Sprintf("task-%d", maxJSONSafeInteger)
	if err := exhausted.AgentTeams().append(exhaustedRoot, "team/task", teamTaskEvent{
		Version: teamEventVersion, TeamID: exhaustedRootID,
		Task: TeamTaskSnapshot{ID: maxID, Revision: 1, Subject: "last", Description: "last safe id", Status: teamTaskPending, BlockedBy: []string{}, WriteScopes: []string{}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := exhausted.AgentTeams().CreateTask(exhaustedRootID, CreateTeamTaskRequest{Subject: "cannot allocate", Description: "exhausted"}); teamErrorCode(err) != "TEAM_TASK_LIMIT" {
		t.Fatalf("task id exhaustion error = %v", err)
	}
}

func TestAgentTeamTaskBoardTransitionAndGraphMatrix(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	defer engine.Close()
	rootID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "task-matrix-root", "")
	if err != nil {
		t.Fatal(err)
	}
	first, err := engine.AgentTeams().CreateTask(rootID, CreateTeamTaskRequest{
		Subject: " first ", Description: " first task ", WriteScopes: []string{"src", "./src/", "src"},
	})
	if err != nil || first.Subject != "first" || first.Description != "first task" || !reflect.DeepEqual(first.WriteScopes, []string{"src"}) {
		t.Fatalf("normalized first task = %#v, %v", first, err)
	}
	second, err := engine.AgentTeams().CreateTask(rootID, CreateTeamTaskRequest{
		Subject: "second", Description: "second task", BlockedBy: []string{first.ID}, WriteScopes: []string{"src/feature"},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertCode := func(want string, request UpdateTeamTaskRequest) {
		t.Helper()
		if _, err := engine.AgentTeams().UpdateTask(rootID, request); teamErrorCode(err) != want {
			t.Fatalf("task update %#v error = %v, want %s", request, err, want)
		}
	}
	assertCode("TEAM_TASK_BLOCKED", UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: second.Revision, Action: "claim"})
	assertCode("TEAM_TASK_DEPENDENCY_CYCLE", UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "set_dependencies", BlockedBySet: true, BlockedBy: []string{second.ID}})
	assertCode("TEAM_TASK_DEPENDENCY_CYCLE", UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "set_dependencies", BlockedBySet: true, BlockedBy: []string{first.ID}})
	assertCode("TEAM_INVALID_ARGUMENT", UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "set_dependencies", BlockedBySet: true, BlockedBy: []string{second.ID, second.ID}})
	assertCode("TEAM_INVALID_ARGUMENT", UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "edit"})
	assertCode("TEAM_INVALID_ARGUMENT", UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "set_dependencies"})
	assertCode("TEAM_TASK_INVALID_TRANSITION", UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "release"})
	assertCode("TEAM_TASK_INVALID_TRANSITION", UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "complete"})
	assertCode("TEAM_TASK_INVALID_TRANSITION", UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "reopen"})
	assertCode("TEAM_TASK_HAS_DEPENDENTS", UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "delete"})
	if _, err := engine.AgentTeams().CreateTask(rootID, CreateTeamTaskRequest{Subject: "missing", Description: "missing", BlockedBy: []string{"missing"}}); teamErrorCode(err) != "TEAM_TASK_NOT_FOUND" {
		t.Fatalf("missing blocker error = %v", err)
	}
	for _, scope := range []string{"", ".", "..", "/root", `C:\root`, "C:root", "a//b", "a/../b"} {
		if _, err := engine.AgentTeams().CreateTask(rootID, CreateTeamTaskRequest{Subject: "scope", Description: "scope", WriteScopes: []string{scope}}); teamErrorCode(err) != "TEAM_INVALID_WRITE_SCOPE" {
			t.Fatalf("invalid scope %q error = %v", scope, err)
		}
	}
	claimed, err := engine.AgentTeams().UpdateTask(rootID, UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "claim"})
	if err != nil || claimed.OwnerName != "lead" || claimed.Status != teamTaskInProgress {
		t.Fatalf("claimed first = %#v, %v", claimed, err)
	}
	assertCode("TEAM_TASK_STALE_REVISION", UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "complete"})
	view, err := engine.AgentTeams().GetTask(rootID, second.ID)
	if err != nil || view.Ready || !reflect.DeepEqual(view.WriteScopeWarnings, []string{"write scopes overlap with " + first.ID}) {
		t.Fatalf("blocked task view = %#v, %v", view, err)
	}
	completed, err := engine.AgentTeams().UpdateTask(rootID, UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: claimed.Revision, Action: "complete"})
	if err != nil || completed.Status != teamTaskCompleted {
		t.Fatalf("completed first = %#v, %v", completed, err)
	}
	secondClaim, err := engine.AgentTeams().UpdateTask(rootID, UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: second.Revision, Action: "claim"})
	if err != nil || secondClaim.Status != teamTaskInProgress {
		t.Fatalf("claimed second = %#v, %v", secondClaim, err)
	}
	released, err := engine.AgentTeams().UpdateTask(rootID, UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: secondClaim.Revision, Action: "release"})
	if err != nil || released.Status != teamTaskPending || released.OwnerName != "" || !released.Ready {
		t.Fatalf("released second = %#v, %v", released, err)
	}
	subject := "edited second"
	edited, err := engine.AgentTeams().UpdateTask(rootID, UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: released.Revision, Action: "edit", Subject: &subject})
	if err != nil || edited.Subject != subject || edited.Description != "second task" {
		t.Fatalf("edited second = %#v, %v", edited, err)
	}
	leadOwner := "lead"
	reassigned, err := engine.AgentTeams().UpdateTask(rootID, UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: edited.Revision, Action: "reassign", Owner: &leadOwner})
	if err != nil || reassigned.OwnerName != "lead" || reassigned.Status != teamTaskInProgress {
		t.Fatalf("reassigned second = %#v, %v", reassigned, err)
	}
	secondComplete, err := engine.AgentTeams().UpdateTask(rootID, UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: reassigned.Revision, Action: "complete"})
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := engine.AgentTeams().UpdateTask(rootID, UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: secondComplete.Revision, Action: "reopen"})
	if err != nil || reopened.Status != teamTaskPending || reopened.OwnerName != "" {
		t.Fatalf("reopened second = %#v, %v", reopened, err)
	}
	deleted, err := engine.AgentTeams().UpdateTask(rootID, UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: reopened.Revision, Action: "delete"})
	if err != nil || deleted.Status != teamTaskDeleted {
		t.Fatalf("deleted second = %#v, %v", deleted, err)
	}
	assertCode("TEAM_TASK_DELETED", UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: deleted.Revision, Action: "edit", Subject: &subject})
	if _, err := engine.AgentTeams().GetTask(rootID, "missing"); teamErrorCode(err) != "TEAM_TASK_NOT_FOUND" {
		t.Fatalf("missing task get error = %v", err)
	}
}

func TestAgentTeamMessageLimitUsesJavaScriptJSONStringifyBytes(t *testing.T) {
	content := []ContentBlock{{Type: "text", Text: strings.Repeat("<>&", 20)}}
	fake := TeamMessageSnapshot{
		ID: "team-message-00000000000000000000000000000000", SenderID: "root", SenderName: "lead", TargetID: "target",
		Delivery: teamMessageQuiet, Content: content,
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(teamMessageContent(fake)); err != nil {
		t.Fatal(err)
	}
	limit := len(bytes.TrimSuffix(encoded.Bytes(), []byte("\n")))
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dataDir, dataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.AgentTeams, cfg.Persist = &AgentTeamConfig{MaxMessageBytes: limit}, true
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	rootID, err := engine.CreateSession(context.Background(), dataDir, "message-bytes-root", "")
	if err != nil {
		t.Fatal(err)
	}
	spawned, err := engine.AgentTeams().SpawnTeammate(context.Background(), rootID, SpawnTeammateRequest{
		Name: "target", Description: "message byte target", Prompt: []ContentBlock{{Type: "text", Text: "finish"}}, Context: "fresh", Provider: "spawn",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := engine.WaitForIdle(waitCtx, spawned.Member.ID); err != nil {
		t.Fatal(err)
	}
	result, err := engine.AgentTeams().SendMessage(context.Background(), rootID, SendTeamMessageRequest{
		Target: "target", Delivery: teamMessageQuiet, Content: content,
	})
	if err != nil || (result.Status != "accepted" && result.Status != "queued") {
		t.Fatalf("JavaScript-sized message = %#v, %v", result, err)
	}
}

func TestAgentTeamLiveTeammateAuthorityAndInterrupt(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	defer engine.Close()
	gate := make(chan struct{})
	provider := &promptGateProvider{gates: map[string]chan struct{}{"live team work": gate}, started: make(chan string, 4)}
	engine.RegisterProvider(provider)
	rootID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "live-team-root", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(rootID, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	spawned, err := engine.AgentTeams().SpawnTeammate(context.Background(), rootID, SpawnTeammateRequest{
		Name: "live-worker", Description: "Live worker", Prompt: []ContentBlock{{Type: "text", Text: "live team work"}},
		Context: "fresh", Provider: "spawn",
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(2 * time.Second):
		t.Fatal("teammate did not start its live task")
	}
	childID := spawned.Member.ID
	childSession := mustSession(t, engine, childID)
	for name := range agentTeamToolNames {
		visible, visibleErr := engine.toolVisibleForSession(childSession, name)
		if visibleErr != nil || !visible {
			t.Fatalf("live teammate Team tool %q visible=%v err=%v", name, visible, visibleErr)
		}
	}
	childPolicy := teamPolicyPrompt(engine.AgentTeams(), childID)
	if !strings.Contains(childPolicy, "Your Team role is teammate; your Team name is live-worker") {
		t.Fatalf("live teammate policy = %q", childPolicy)
	}
	task, err := engine.AgentTeams().CreateTask(childID, CreateTeamTaskRequest{Subject: "Owned", Description: "Owned by teammate"})
	if err != nil {
		t.Fatal(err)
	}
	task, err = engine.AgentTeams().UpdateTask(childID, UpdateTeamTaskRequest{
		TaskID: task.ID, ExpectedRevision: task.Revision, Action: "claim",
	})
	if err != nil || task.OwnerName != "live-worker" || task.Status != teamTaskInProgress {
		t.Fatalf("live teammate claim = %#v, %v", task, err)
	}
	message, err := engine.AgentTeams().SendMessage(context.Background(), childID, SendTeamMessageRequest{
		Target: "lead", Delivery: teamMessageQuiet, Content: []ContentBlock{{Type: "text", Text: "live report"}},
	})
	if err != nil || message.Status != "accepted" {
		t.Fatalf("live teammate message = %#v, %v", message, err)
	}
	if _, err := engine.AgentTeams().Interrupt(childID, "live-worker"); teamErrorCode(err) != "TEAM_LEAD_REQUIRED" {
		t.Fatalf("teammate interrupt authority = %v", err)
	}
	previous, err := engine.AgentTeams().Interrupt(rootID, "live-worker")
	if err != nil || previous != "running" {
		t.Fatalf("Lead interrupt = %q, %v", previous, err)
	}
	close(gate)
}

func TestAgentTeamMembershipUsesOwnedDescriptorAndParentTeamState(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	defer engine.Close()
	rootID, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "identity-root", "")
	if err != nil {
		t.Fatal(err)
	}
	ordinaryID := "ordinary-child-root"
	if _, err := engine.createSession(t.Context(), SessionHeader{
		ID: ordinaryID, CWD: engine.Config().Workspace, ParentSession: rootID,
	}, false); err != nil {
		t.Fatal(err)
	}
	ordinary, err := engine.AgentTeams().membership(ordinaryID)
	if err != nil || ordinary.role != "lead" || ordinary.id != ordinaryID || ordinary.root.Header.ID != ordinaryID {
		t.Fatalf("ordinary child membership = %#v, %v", ordinary, err)
	}

	orphanProviderID := "orphan-provider-child"
	if _, err := engine.createSession(t.Context(), SessionHeader{
		ID: orphanProviderID, CWD: engine.Config().Workspace, ParentSession: "absent-parent",
	}, false); err != nil {
		t.Fatal(err)
	}
	orphanProvider := mustSession(t, engine, orphanProviderID)
	if _, err := engine.appendEventWithMetadata(orphanProvider, "subagent/descriptor", map[string]any{
		"version": SubagentDescriptorVersion, "mode": "continuable", "provider": "", "label": "provider child",
	}, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.AgentTeams().membership(orphanProviderID); teamErrorCode(err) != "TEAM_NOT_MEMBER" {
		t.Fatalf("orphan provider membership error = %v", err)
	}

	unsupportedID := "unsupported-descriptor-root"
	if _, err := engine.createSession(t.Context(), SessionHeader{
		ID: unsupportedID, CWD: engine.Config().Workspace, ParentSession: "absent-parent",
	}, false); err != nil {
		t.Fatal(err)
	}
	unsupported := mustSession(t, engine, unsupportedID)
	if _, err := engine.appendEventWithMetadata(unsupported, "subagent/descriptor", map[string]any{
		"version": SubagentDescriptorVersion + 1,
	}, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if membership, err := engine.AgentTeams().membership(unsupportedID); err != nil || membership.role != "lead" {
		t.Fatalf("unsupported descriptor membership = %#v, %v", membership, err)
	}

	root := mustSession(t, engine, rootID)
	if _, err := engine.appendEventWithMetadata(root, "team/member", map[string]any{
		"version": teamEventVersion, "teamId": rootID, "member": "malformed",
	}, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.AgentTeams().membership(ordinaryID); teamErrorCode(err) != "TEAM_NOT_MEMBER" {
		t.Fatalf("malformed parent membership error = %v", err)
	}
}

func TestAgentTeamProvisioningRecoveryUsesFirstDescriptor(t *testing.T) {
	rootID := "descriptor-recovery-root"
	member := TeamMemberSnapshot{ID: "descriptor-child", Provider: "spawn"}
	descriptor := func(provider string) Event {
		return Event{Type: "subagent/descriptor", Data: map[string]any{
			"version": SubagentDescriptorVersion, "mode": "continuable", "provider": provider, "label": "worker",
		}}
	}
	accepted := Event{Type: "user/message", Data: map[string]any{
		"id": "initial", "role": "user", "content": []any{}, "source": map[string]any{"kind": "user"},
	}}
	inspection := SessionInspection{
		Meta:   SessionHeader{ParentSession: rootID},
		Events: []Event{descriptor("spawn"), descriptor("other"), accepted},
	}
	matched, err := teamProvisionedChildMatches(rootID, member, inspection)
	if err != nil || !matched {
		t.Fatalf("first descriptor match = %v, %v", matched, err)
	}

	inspection.Events = []Event{{Type: "subagent/descriptor", Data: map[string]any{
		"version": SubagentDescriptorVersion, "mode": "continuable", "provider": 7, "label": "worker",
	}}, descriptor("spawn"), accepted}
	if matched, err := teamProvisionedChildMatches(rootID, member, inspection); err == nil || matched {
		t.Fatalf("malformed first descriptor = %v, %v", matched, err)
	}

	inspection.Events = []Event{{Type: "subagent/descriptor", Data: map[string]any{
		"version": SubagentDescriptorVersion + 1,
	}}, descriptor("spawn"), accepted}
	if matched, err := teamProvisionedChildMatches(rootID, member, inspection); err != nil || matched {
		t.Fatalf("unsupported first descriptor = %v, %v", matched, err)
	}
}

func TestAgentTeamMailboxAdmissionAndTargetOrdering(t *testing.T) {
	t.Run("capacity observes durable blocked delivery", func(t *testing.T) {
		dataDir := t.TempDir()
		base, err := NewJSONLSessionStore(filepath.Join(dataDir, "sessions"))
		if err != nil {
			t.Fatal(err)
		}
		store := &blockingTeamFlushStore{
			SessionStore: base, entered: make(chan struct{}), release: make(chan struct{}),
		}
		cfg := DefaultConfig()
		cfg.DataDir, cfg.Workspace = dataDir, dataDir
		cfg.Provider, cfg.Model, cfg.Persist, cfg.SessionStore = "echo", "echo", true, store
		cfg.SessionTitleLLM.Enabled = false
		cfg.AgentTeams = &AgentTeamConfig{MaxPendingMessagesPerMember: 1}
		engine, err := New(WithConfig(cfg))
		if err != nil {
			t.Fatal(err)
		}
		defer engine.Close()
		root, err := engine.CreateSession(t.Context(), dataDir, "capacity-root", "")
		if err != nil {
			t.Fatal(err)
		}
		spawned, err := engine.AgentTeams().SpawnTeammate(t.Context(), root, SpawnTeammateRequest{
			Name: "capacity-worker", Description: "capacity worker", Prompt: []ContentBlock{{Type: "text", Text: "initial"}},
			Context: "fresh", Provider: "spawn",
		})
		if err != nil {
			t.Fatal(err)
		}
		waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		if err := engine.WaitForIdle(waitCtx, spawned.Member.ID); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		waitForModelSubagentDetached(t, engine, spawned.Member.ID)
		store.mu.Lock()
		store.blockID = spawned.Member.ID
		store.mu.Unlock()
		firstDone := make(chan error, 1)
		go func() {
			_, err := engine.AgentTeams().SendMessage(context.Background(), root, SendTeamMessageRequest{
				Target: "capacity-worker", Delivery: teamMessageWakeup, Content: []ContentBlock{{Type: "text", Text: "first"}},
			})
			firstDone <- err
		}()
		select {
		case <-store.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("first delivery did not reach target durability")
		}
		if _, err := engine.AgentTeams().SendMessage(t.Context(), root, SendTeamMessageRequest{
			Target: "capacity-worker", Delivery: teamMessageWakeup, Content: []ContentBlock{{Type: "text", Text: "second"}},
		}); teamErrorCode(err) != "TEAM_MAILBOX_FULL" {
			t.Fatalf("blocked-delivery capacity error = %v", err)
		}
		close(store.release)
		if err := <-firstDone; err != nil {
			t.Fatal(err)
		}
	})

	t.Run("different targets dispatch independently", func(t *testing.T) {
		dataDir := t.TempDir()
		base, err := NewJSONLSessionStore(filepath.Join(dataDir, "sessions"))
		if err != nil {
			t.Fatal(err)
		}
		store := &blockingTeamFlushStore{
			SessionStore: base, entered: make(chan struct{}), release: make(chan struct{}),
		}
		cfg := DefaultConfig()
		cfg.DataDir, cfg.Workspace = dataDir, dataDir
		cfg.Provider, cfg.Model, cfg.Persist, cfg.SessionStore = "echo", "echo", true, store
		cfg.SessionTitleLLM.Enabled = false
		cfg.AgentTeams = &AgentTeamConfig{}
		engine, err := New(WithConfig(cfg))
		if err != nil {
			t.Fatal(err)
		}
		defer engine.Close()
		root, err := engine.CreateSession(t.Context(), dataDir, "parallel-target-root", "")
		if err != nil {
			t.Fatal(err)
		}
		spawn := func(name string) SpawnTeammateResult {
			value, spawnErr := engine.AgentTeams().SpawnTeammate(t.Context(), root, SpawnTeammateRequest{
				Name: name, Description: name + " responsibility", Prompt: []ContentBlock{{Type: "text", Text: "initial"}},
				Context: "fresh", Provider: "spawn",
			})
			if spawnErr != nil {
				t.Fatal(spawnErr)
			}
			waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if waitErr := engine.WaitForIdle(waitCtx, value.Member.ID); waitErr != nil {
				t.Fatal(waitErr)
			}
			waitForModelSubagentDetached(t, engine, value.Member.ID)
			return value
		}
		alpha, beta := spawn("alpha-target"), spawn("beta-target")
		store.mu.Lock()
		store.blockID = alpha.Member.ID
		store.mu.Unlock()
		alphaDone := make(chan error, 1)
		go func() {
			_, sendErr := engine.AgentTeams().SendMessage(context.Background(), root, SendTeamMessageRequest{
				Target: "alpha-target", Delivery: teamMessageWakeup, Content: []ContentBlock{{Type: "text", Text: "alpha work"}},
			})
			alphaDone <- sendErr
		}()
		select {
		case <-store.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("alpha delivery did not block")
		}
		betaDone := make(chan error, 1)
		go func() {
			value, sendErr := engine.AgentTeams().SendMessage(context.Background(), root, SendTeamMessageRequest{
				Target: "beta-target", Delivery: teamMessageWakeup, Content: []ContentBlock{{Type: "text", Text: "beta work"}},
			})
			if sendErr == nil && value.Status != "accepted" {
				sendErr = fmt.Errorf("beta status = %s", value.Status)
			}
			betaDone <- sendErr
		}()
		select {
		case err := <-betaDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("beta delivery was blocked by a different target")
		}
		close(store.release)
		if err := <-alphaDone; err != nil {
			t.Fatal(err)
		}
		_ = beta
	})
}

func TestAgentTeamMailboxTargetFlushFailureRecoversWithoutDuplicate(t *testing.T) {
	dataDir := t.TempDir()
	base, err := NewJSONLSessionStore(filepath.Join(dataDir, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	store := &failingTeamFlushStore{SessionStore: base}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dataDir, dataDir
	cfg.Provider, cfg.Model, cfg.Persist, cfg.SessionStore = "echo", "echo", true, store
	cfg.SessionTitleLLM.Enabled = false
	cfg.AgentTeams = &AgentTeamConfig{}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	root, err := engine.CreateSession(t.Context(), dataDir, "flush-recovery-root", "")
	if err != nil {
		t.Fatal(err)
	}
	spawned, err := engine.AgentTeams().SpawnTeammate(t.Context(), root, SpawnTeammateRequest{
		Name: "flush-worker", Description: "flush worker", Prompt: []ContentBlock{{Type: "text", Text: "initial"}},
		Context: "fresh", Provider: "spawn",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	if err := engine.WaitForIdle(waitCtx, spawned.Member.ID); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	waitForModelSubagentDetached(t, engine, spawned.Member.ID)
	sentinel := errors.New("target flush failed")
	store.mu.Lock()
	store.failID, store.failure = spawned.Member.ID, sentinel
	store.mu.Unlock()
	queued, err := engine.AgentTeams().SendMessage(t.Context(), root, SendTeamMessageRequest{
		Target: "flush-worker", Delivery: teamMessageWakeup, Content: []ContentBlock{{Type: "text", Text: "recover exactly once"}},
	})
	if err != nil || queued.Status != "queued" {
		t.Fatalf("failed target flush result = %#v, %v", queued, err)
	}
	engine.agentTeams.recoverSession(root)
	deadline := time.Now().Add(2 * time.Second)
	for {
		state, stateErr := engine.agentTeams.state(mustSession(t, engine, root))
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		if state.delivered[queued.MessageID] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovered mailbox state = %#v", state)
		}
		time.Sleep(time.Millisecond)
	}
	target := mustSession(t, engine, spawned.Member.ID)
	target.mu.Lock()
	receiptIDs := map[string]bool{}
	for _, event := range target.Events {
		data, _ := event.Data.(map[string]any)
		if event.Type == "user/message" {
			source, _ := data["source"].(map[string]any)
			id, _ := data["id"].(string)
			if source["kind"] == "team-message" && source["messageId"] == queued.MessageID && id != "" {
				receiptIDs[id] = true
			}
			continue
		}
		if event.Type != "agent/inbox/spliced" {
			continue
		}
		inserted, _ := data["inserted"].([]any)
		for _, raw := range inserted {
			message, _ := raw.(map[string]any)
			source, _ := message["source"].(map[string]any)
			id, _ := message["id"].(string)
			if source["kind"] == "team-message" && source["messageId"] == queued.MessageID && id != "" {
				receiptIDs[id] = true
			}
		}
	}
	target.mu.Unlock()
	if len(receiptIDs) != 1 {
		t.Fatalf("target receipt ids = %#v, want one logical inbox item", receiptIDs)
	}
}

func TestAgentTeamToolsAndPolicyAreScopedToExactMembers(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	defer engine.Close()
	rootID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "team-scope-root", "")
	if err != nil {
		t.Fatal(err)
	}
	ordinaryID, err := engine.CreateSubagent(context.Background(), rootID, "team-scope-ordinary", "")
	if err != nil {
		t.Fatal(err)
	}
	root := mustSession(t, engine, rootID)
	ordinary := mustSession(t, engine, ordinaryID)
	for name := range agentTeamToolNames {
		rootVisible, rootErr := engine.toolVisibleForSession(root, name)
		if rootErr != nil || !rootVisible {
			t.Fatalf("Lead Team tool %q visible=%v err=%v", name, rootVisible, rootErr)
		}
		ordinaryVisible, ordinaryErr := engine.toolVisibleForSession(ordinary, name)
		if ordinaryErr != nil || ordinaryVisible {
			t.Fatalf("ordinary subagent Team tool %q visible=%v err=%v", name, ordinaryVisible, ordinaryErr)
		}
	}
	policy := teamPolicyPrompt(engine.AgentTeams(), rootID)
	for _, marker := range []string{
		"FS_STALE_VERSION",
		"Bash, formatters, code generators, and scripts are not fully protected",
		"A successful send is already durable even when its result says queued; do not resend it.",
		"Task readiness never starts an owner.",
		"returns noProgress immediately",
		"Your Team role is lead; your Team name is lead",
	} {
		if !strings.Contains(policy, marker) {
			t.Fatalf("Lead Team policy missing %q: %q", marker, policy)
		}
	}
	if policy := teamPolicyPrompt(engine.AgentTeams(), ordinaryID); policy != "" {
		t.Fatalf("ordinary subagent Team policy = %q", policy)
	}
}

func TestAgentTeamToolsShadowLegacyControlsOnlyForMembers(t *testing.T) {
	presetRoot := t.TempDir()
	presetDir := filepath.Join(presetRoot, "mixed-controls")
	if err := os.Mkdir(presetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	composition := `
- id: team-tools
  name: '@deepseek-ai/dsh-experimental-tool-agent-team'
- id: legacy-control
  name: '@deepseek-ai/dsh-tool-subagent-control'
- id: legacy-list
  name: '@deepseek-ai/dsh-tool-subagent-control/list-agents'
`
	if err := os.WriteFile(filepath.Join(presetDir, "agent.cordis.yml"), []byte(composition), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.PresetDir = t.TempDir(), t.TempDir(), presetRoot
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.AgentTeams, cfg.Persist = &AgentTeamConfig{}, true
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	rootID, err := engine.CreateSession(context.Background(), cfg.Workspace, "mixed-team-root", "mixed-controls")
	if err != nil {
		t.Fatal(err)
	}
	ordinaryID, err := engine.CreateSubagent(context.Background(), rootID, "mixed-ordinary", "mixed-controls")
	if err != nil {
		t.Fatal(err)
	}
	root, ordinary := mustSession(t, engine, rootID), mustSession(t, engine, ordinaryID)
	rootSchemas, err := engine.toolsForSession(root)
	if err != nil {
		t.Fatal(err)
	}
	ordinarySchemas, err := engine.toolsForSession(ordinary)
	if err != nil {
		t.Fatal(err)
	}
	parameters := func(schemas []ToolSchema, name string) map[string]any {
		t.Helper()
		for _, schema := range schemas {
			if schema.Name == name {
				properties, _ := schema.Parameters["properties"].(map[string]any)
				return properties
			}
		}
		t.Fatalf("missing tool schema %q", name)
		return nil
	}
	leadSend := parameters(rootSchemas, "send_message")
	if leadSend["target"] == nil || leadSend["subagent_id"] != nil {
		t.Fatalf("Lead send_message schema = %#v", leadSend)
	}
	ordinarySend := parameters(ordinarySchemas, "send_message")
	if ordinarySend["subagent_id"] == nil || ordinarySend["target"] != nil {
		t.Fatalf("ordinary send_message schema = %#v", ordinarySend)
	}
	if visible, visibleErr := engine.toolVisibleForSession(ordinary, "spawn_teammate"); visibleErr != nil || visible {
		t.Fatalf("ordinary spawn_teammate visible=%v err=%v", visible, visibleErr)
	}
	listTool := engine.tools["list_agents"]
	leadList, err := executeTool(context.Background(), listTool, ToolCall{Name: "list_agents", SessionID: rootID, Arguments: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	leadRows, ok := leadList.Value.([]any)
	if !ok || len(leadRows) != 1 {
		t.Fatalf("Lead list_agents value = %#v", leadList.Value)
	}
	leadRow, _ := leadRows[0].(map[string]any)
	if leadRow["role"] != "lead" {
		t.Fatalf("Lead list_agents value = %#v", leadList.Value)
	}
	ordinaryList, err := executeTool(context.Background(), listTool, ToolCall{Name: "list_agents", SessionID: ordinaryID, Arguments: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	legacyRows, ok := ordinaryList.Value.([]any)
	if !ok || len(legacyRows) != 0 {
		t.Fatalf("ordinary list_agents value = %#v", ordinaryList.Value)
	}
}

func TestAgentTeamToolsAreCompleteAndExecutable(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	defer engine.Close()
	root, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "team-tools", "")
	if err != nil {
		t.Fatal(err)
	}
	registered := map[string]bool{}
	for _, schema := range engine.ListTools() {
		registered[schema.Name] = true
	}
	for _, name := range []string{
		"spawn_teammate", "send_message", "followup_task", "list_agents", "wait_agent", "interrupt_agent",
		"team_task_create", "team_task_list", "team_task_get", "team_task_update",
	} {
		if !registered[name] {
			t.Fatalf("missing Team tool %q", name)
		}
	}
	waitOutput := engine.tools["wait_agent"].Schema.Output
	properties, _ := waitOutput["properties"].(map[string]any)
	noProgressSchema, _ := properties["noProgress"].(map[string]any)
	noProgressProperties, _ := noProgressSchema["properties"].(map[string]any)
	reason, _ := noProgressProperties["reason"].(map[string]any)
	if reason["const"] != "no-active-peer" {
		t.Fatalf("wait_agent noProgress reason schema = %#v", reason)
	}
	spawned := executeRegisteredTool(t, engine, "spawn_teammate", root, map[string]any{
		"name": "tool-worker", "description": "Tool-created worker", "prompt": "tool initial task",
	})
	value, ok := spawned.Value.(SpawnTeammateResult)
	if !ok || value.Member.ID == "" {
		t.Fatalf("spawn tool value = %#v", spawned.Value)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := engine.WaitForIdle(waitCtx, value.Member.ID); err != nil {
		t.Fatal(err)
	}
	noProgress := executeRegisteredTool(t, engine, "wait_agent", root, map[string]any{"timeout_ms": 10_000})
	result, ok := noProgress.Value.(map[string]any)
	if !ok || result["timedOut"] != false || result["noProgress"] == nil {
		t.Fatalf("wait_agent noProgress = %#v", noProgress.Value)
	}
	created := executeRegisteredTool(t, engine, "team_task_create", root, map[string]any{
		"subject": "Tool task", "description": "Created through the model tool",
	})
	if task, ok := created.Value.(TeamTaskView); !ok || task.ID != "task-1" {
		t.Fatalf("team_task_create value = %#v", created.Value)
	}
	listTool := engine.tools["team_task_list"]
	if _, err := listTool.Execute(context.Background(), ToolCall{
		Name: "team_task_list", SessionID: root, Arguments: json.RawMessage(`{"cursor":9007199254740992}`),
	}); err == nil || !strings.Contains(err.Error(), "cursor must be a non-negative safe integer") {
		t.Fatalf("unsafe cursor error = %v", err)
	}
	waitTool := engine.tools["wait_agent"]
	if _, err := waitTool.Execute(context.Background(), ToolCall{
		Name: "wait_agent", SessionID: root, Arguments: json.RawMessage(`{"timeout_ms":9999}`),
	}); err == nil || !strings.Contains(err.Error(), "timeoutMs must be an integer from 10000 through 3600000") {
		t.Fatalf("invalid timeout error = %v", err)
	}
}

func TestAgentTeamMailboxRecoversAcrossJSONLAndSQLite(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		name := "jsonl"
		if sqlite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			dataDir := t.TempDir()
			first := newAgentTeamEngine(t, dataDir, sqlite)
			root, err := first.CreateSession(context.Background(), dataDir, "persisted-team", "")
			if err != nil {
				t.Fatal(err)
			}
			spawned, err := first.AgentTeams().SpawnTeammate(context.Background(), root, SpawnTeammateRequest{
				Name: "persisted-worker", Description: "Persistent worker", Prompt: []ContentBlock{{Type: "text", Text: "initial"}},
				Context: "fresh", Provider: "spawn",
			})
			if err != nil {
				t.Fatal(err)
			}
			child := spawned.Member.ID
			waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := first.WaitForIdle(waitCtx, child); err != nil {
				cancel()
				t.Fatal(err)
			}
			cancel()
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}

			second := newAgentTeamEngine(t, dataDir, sqlite)
			if _, err := second.CreateSession(context.Background(), dataDir, root, ""); err != nil {
				t.Fatal(err)
			}
			queued, err := second.AgentTeams().SendMessage(context.Background(), root, SendTeamMessageRequest{
				Target: "persisted-worker", Delivery: teamMessageQuiet, Content: []ContentBlock{{Type: "text", Text: "recover me"}},
			})
			if err != nil || queued.Status != "queued" {
				t.Fatalf("queued = %#v, %v", queued, err)
			}
			if _, err := second.CreateSession(context.Background(), dataDir, child, ""); err != nil {
				t.Fatal(err)
			}
			rootSession := mustSession(t, second, root)
			rootSession.mu.Lock()
			rootEvents := append([]Event(nil), rootSession.Events...)
			rootSession.mu.Unlock()
			fold, err := FoldTeam(root, rootEvents)
			if err != nil || !reflect.DeepEqual(fold.Delivered, []string{queued.MessageID}) {
				t.Fatalf("recovered fold = %#v, %v", fold, err)
			}
			if !sessionHasTeamMessage(mustSession(t, second, child), queued.MessageID) {
				t.Fatal("recovered message was not durably staged in target Session")
			}
			if err := second.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAgentTeamAcknowledgesTargetSideReceipt(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	defer engine.Close()
	rootID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "receipt-root", "")
	if err != nil {
		t.Fatal(err)
	}
	root := mustSession(t, engine, rootID)
	message := TeamMessageSnapshot{
		ID: "receipt-message", SenderID: "sender", SenderName: "sender", TargetID: rootID,
		Delivery: teamMessageWakeup, Content: []ContentBlock{{Type: "text", Text: "already recorded"}},
	}
	if _, err := engine.appendEventWithMetadata(root, "team/message/queued", teamMessageQueuedEvent{
		Version: teamEventVersion, TeamID: rootID, Message: message,
	}, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := engine.sessionStore.(SessionPersistenceFlusher).Flush(context.Background(), rootID); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEventWithMetadata(root, "user/message", map[string]any{
		"id": "receipt-user-message", "role": "user", "content": message.Content,
		"source": teamMessageSource(rootID, message),
	}, "append", nil, false); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		root.mu.Lock()
		events := append([]Event(nil), root.Events...)
		root.mu.Unlock()
		fold, foldErr := FoldTeam(rootID, events)
		if foldErr != nil {
			t.Fatal(foldErr)
		}
		if reflect.DeepEqual(fold.Delivered, []string{message.ID}) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("target-side receipt was not acknowledged: %#v", fold)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAgentTeamDispatchRechecksLiveReceiptAfterFlush(t *testing.T) {
	dataDir := t.TempDir()
	base, err := NewJSONLSessionStore(filepath.Join(dataDir, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingTeamFlushStore{
		SessionStore: base, entered: make(chan struct{}), release: make(chan struct{}),
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dataDir, dataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.AgentTeams, cfg.Persist, cfg.SessionStore = &AgentTeamConfig{}, true, store
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	rootID, err := engine.CreateSession(context.Background(), dataDir, "receipt-race-root", "")
	if err != nil {
		t.Fatal(err)
	}
	root := mustSession(t, engine, rootID)
	message := TeamMessageSnapshot{
		ID: "receipt-race-message", SenderID: "peer", SenderName: "peer", TargetID: rootID,
		Delivery: teamMessageQuiet, Content: []ContentBlock{{Type: "text", Text: "remove during flush"}},
	}
	if err := engine.agentTeams.append(root, "team/message/queued", teamMessageQueuedEvent{
		Version: teamEventVersion, TeamID: rootID, Message: message,
	}); err != nil {
		t.Fatal(err)
	}
	job := &queuedPrompt{
		id: "receipt-race-inbox", text: "remove during flush", content: teamMessageContent(message),
		source: teamMessageSource(rootID, message),
	}
	root.mu.Lock()
	if _, err := appendEventLocked(root, "agent/inbox/spliced", map[string]any{
		"target": "next-turn", "start": len(root.pending), "inserted": []any{job.message()},
	}, nil, nil, false); err != nil {
		root.mu.Unlock()
		t.Fatal(err)
	}
	root.pending = append(root.pending, job)
	root.mu.Unlock()
	store.mu.Lock()
	store.blockID = rootID
	store.mu.Unlock()
	type dispatchResult struct {
		accepted bool
		err      error
	}
	done := make(chan dispatchResult, 1)
	go func() {
		accepted, err := engine.agentTeams.dispatchLocked(context.Background(), root, message, false)
		done <- dispatchResult{accepted: accepted, err: err}
	}()
	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("target receipt flush did not block")
	}
	root.mu.Lock()
	root.pending = nil
	root.mu.Unlock()
	close(store.release)
	result := <-done
	if result.err != nil || result.accepted {
		t.Fatalf("dispatch after disappearing receipt = accepted %v, err %v", result.accepted, result.err)
	}
	state, err := engine.agentTeams.state(root)
	if err != nil {
		t.Fatal(err)
	}
	if state.delivered[message.ID] {
		t.Fatal("disappearing target receipt was acknowledged")
	}
}

func TestAgentTeamWaitPreservesCauseAndClosesAdmission(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	rootID, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "wait-lifecycle-root", "")
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("typed wait cancellation")
	canceled, cancel := context.WithCancelCause(context.Background())
	cancel(sentinel)
	if _, err := engine.AgentTeams().WaitForChange(canceled, rootID, minimumTeamWait); !errors.Is(err, sentinel) {
		t.Fatalf("wait cancellation = %v", err)
	}
	waiting := make(chan struct {
		value TeamWaitResult
		err   error
	}, 1)
	go func() {
		value, err := engine.AgentTeams().WaitForChange(context.Background(), rootID, minimumTeamWait)
		waiting <- struct {
			value TeamWaitResult
			err   error
		}{value: value, err: err}
	}()
	time.Sleep(10 * time.Millisecond)
	if err := engine.AgentTeams().close(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-waiting:
		if result.err != nil || result.value.TimedOut {
			t.Fatalf("wait released by close = %#v, %v", result.value, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Team close did not release an admitted waiter")
	}
	started := time.Now()
	value, err := engine.AgentTeams().WaitForChange(context.Background(), rootID, maximumTeamWait)
	if err != nil || value.TimedOut || time.Since(started) > 100*time.Millisecond {
		t.Fatalf("closed wait admission = %#v, %v, elapsed %s", value, err, time.Since(started))
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentTeamCloseAwaitsAdmittedReceiptAcknowledgement(t *testing.T) {
	dataDir := t.TempDir()
	base, err := NewJSONLSessionStore(filepath.Join(dataDir, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingTeamFlushStore{
		SessionStore: base, entered: make(chan struct{}), release: make(chan struct{}),
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dataDir, dataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.AgentTeams = &AgentTeamConfig{DisposalTimeout: time.Second}
	cfg.Persist, cfg.SessionStore = true, store
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	rootID, err := engine.CreateSession(context.Background(), dataDir, "dispose-receipt-root", "")
	if err != nil {
		t.Fatal(err)
	}
	root := mustSession(t, engine, rootID)
	message := TeamMessageSnapshot{
		ID: "dispose-receipt-message", SenderID: "sender", SenderName: "sender", TargetID: rootID,
		Delivery: teamMessageWakeup, Content: []ContentBlock{{Type: "text", Text: "await acknowledgement"}},
	}
	if _, err := engine.appendEventWithMetadata(root, "team/message/queued", teamMessageQueuedEvent{
		Version: teamEventVersion, TeamID: rootID, Message: message,
	}, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(context.Background(), rootID); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.blockID = rootID
	store.mu.Unlock()
	if _, err := engine.appendEventWithMetadata(root, "user/message", map[string]any{
		"id": "dispose-receipt-user", "role": "user", "content": message.Content,
		"source": teamMessageSource(rootID, message),
	}, "append", nil, false); err != nil {
		t.Fatal(err)
	}
	<-store.entered
	closed := make(chan error, 1)
	go func() { closed <- engine.agentTeams.close() }()
	select {
	case err := <-closed:
		t.Fatalf("Team close returned before admitted acknowledgement settled: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(store.release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	root.mu.Lock()
	events := append([]Event(nil), root.Events...)
	root.mu.Unlock()
	fold, err := FoldTeam(rootID, events)
	if err != nil || !reflect.DeepEqual(fold.Delivered, []string{message.ID}) {
		t.Fatalf("fold after close = %#v, %v", fold, err)
	}
}

func TestAgentTeamCloseAggregatesAdmittedOperationFailures(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	service := engine.AgentTeams()
	creationCtx, creation, err := service.beginCreation(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	dispatchCtx, dispatch, err := service.beginDispatch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	creationFailure := errors.New("creation cleanup failed")
	dispatchFailure := errors.New("dispatch settlement failed")
	go func() {
		<-creationCtx.Done()
		resultErr := error(creationFailure)
		service.finishCreation(creation, &resultErr, "")
	}()
	go func() {
		<-dispatchCtx.Done()
		service.finishDispatch(dispatch, dispatchFailure)
	}()
	closeErr := service.close()
	if !errors.Is(closeErr, creationFailure) || !errors.Is(closeErr, dispatchFailure) {
		t.Fatalf("Team close failures = %v", closeErr)
	}

	disposed := teamError("TEAM_DISPOSED", "Agent Teams service is disposing")
	wrappedDisposed := fmt.Errorf("wrapped cancellation: %w", disposed)
	if err := teamCreationFailures([]*teamCreation{{err: wrappedDisposed}}); err != nil {
		t.Fatalf("wrapped disposal cancellation was retained: %v", err)
	}
	joined := errors.Join(disposed, creationFailure)
	if err := teamCreationFailures([]*teamCreation{{err: joined}}); !errors.Is(err, creationFailure) {
		t.Fatalf("mixed cancellation and cleanup failure = %v", err)
	}
	timeoutErr := waitTeamCreations([]*teamCreation{{done: make(chan struct{})}}, 25*time.Millisecond)
	if teamErrorCode(timeoutErr) != "TEAM_DISPOSAL_TIMEOUT" || !strings.Contains(timeoutErr.Error(), "exceeded 25ms") {
		t.Fatalf("creation disposal timeout = %v", timeoutErr)
	}
	_ = engine.Close()
}

func TestAgentTeamRosterDiscoverySurfacesMalformedParentState(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	defer engine.Close()
	rootID, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "discovery-root", "")
	if err != nil {
		t.Fatal(err)
	}
	childID := "discovery-child"
	root := mustSession(t, engine, rootID)
	member := TeamMemberSnapshot{
		ID: childID, Name: "discovery-worker", Description: "discovery worker", Provider: "spawn",
		Context: "fresh", Phase: teamMemberProvisioning,
	}
	for _, phase := range []string{teamMemberProvisioning, teamMemberActive} {
		member.Phase = phase
		if _, err := engine.appendEventWithMetadata(root, "team/member", teamMemberEvent{
			Version: teamEventVersion, TeamID: rootID, Member: member,
		}, nil, nil, false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := engine.createSession(t.Context(), SessionHeader{
		ID: childID, CWD: engine.Config().Workspace, ParentSession: rootID,
	}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEventWithMetadata(root, "team/task", map[string]any{
		"version": teamEventVersion, "teamId": rootID, "task": "malformed",
	}, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.AgentTeams().rosterChildrenByRoot(); err == nil {
		t.Fatal("malformed parent Team state was hidden during roster discovery")
	}
}

func TestAgentTeamCloseReleasesRosterChildrenOnly(t *testing.T) {
	engine := newAgentTeamEngine(t, t.TempDir(), false)
	root, err := engine.CreateSession(context.Background(), engine.Config().Workspace, "close-team-root", "")
	if err != nil {
		t.Fatal(err)
	}
	rostered, err := engine.AgentTeams().SpawnTeammate(context.Background(), root, SpawnTeammateRequest{
		Name: "rostered", Description: "Roster-owned child", Prompt: []ContentBlock{{Type: "text", Text: "initial"}}, Context: "fresh", Provider: "spawn",
	})
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := engine.CreateSubagent(context.Background(), root, "close-ordinary", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.agentTeams.close(); err != nil {
		t.Fatal(err)
	}
	rosterSession := mustSession(t, engine, rostered.Member.ID)
	rosterSession.mu.Lock()
	rosteredAttached := rosterSession.attached
	rosterSession.mu.Unlock()
	if rosteredAttached {
		t.Fatal("roster child remains attached after Team close")
	}
	ordinarySession := mustSession(t, engine, ordinary)
	ordinarySession.mu.Lock()
	ordinaryAttached := ordinarySession.attached
	ordinarySession.mu.Unlock()
	if !ordinaryAttached {
		t.Fatal("ordinary child was released by Team close")
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentTeamCloseCancelsAndDrainsInFlightSpawn(t *testing.T) {
	dataDir := t.TempDir()
	store, err := NewJSONLSessionStore(filepath.Join(dataDir, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	blockingStore := &blockingTeamCreateStore{
		SessionStore: store,
		entered:      make(chan struct{}, 1),
		canceled:     make(chan struct{}, 1),
		release:      make(chan struct{}, 1),
	}
	defer func() {
		select {
		case blockingStore.release <- struct{}{}:
		default:
		}
	}()
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dataDir, dataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.AgentTeams = &AgentTeamConfig{DisposalTimeout: time.Second}
	cfg.Persist, cfg.SessionStore = true, blockingStore
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	root, err := engine.CreateSession(context.Background(), dataDir, "inflight-team-root", "")
	if err != nil {
		t.Fatal(err)
	}
	spawned := make(chan error, 1)
	go func() {
		_, err := engine.AgentTeams().SpawnTeammate(context.Background(), root, SpawnTeammateRequest{
			Name: "inflight-worker", Description: "Blocked durable creation", Prompt: []ContentBlock{{Type: "text", Text: "initial"}},
			Context: "fresh", Provider: "spawn",
		})
		spawned <- err
	}()
	select {
	case <-blockingStore.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("teammate creation did not reach the controllable durable commit")
	}
	closed := make(chan error, 1)
	go func() { closed <- engine.agentTeams.close() }()
	select {
	case <-blockingStore.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("Team close did not cancel the admitted creation")
	}
	select {
	case err := <-closed:
		t.Fatalf("Team close returned before the admitted creation settled: %v", err)
	default:
	}
	if _, err := engine.AgentTeams().SpawnTeammate(context.Background(), root, SpawnTeammateRequest{
		Name: "late-worker", Description: "Must not be admitted", Prompt: []ContentBlock{{Type: "text", Text: "late"}},
		Context: "fresh", Provider: "spawn",
	}); teamErrorCode(err) != "TEAM_DISPOSED" {
		t.Fatalf("late SpawnTeammate error = %v", err)
	}
	blockingStore.release <- struct{}{}
	select {
	case err := <-spawned:
		if teamErrorCode(err) != "TEAM_DISPOSED" {
			t.Fatalf("in-flight SpawnTeammate error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight SpawnTeammate did not settle after release")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Team close = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Team close did not wait for and drain the admitted creation")
	}
	rootSession := mustSession(t, engine, root)
	rootSession.mu.Lock()
	events := append([]Event(nil), rootSession.Events...)
	rootSession.mu.Unlock()
	fold, err := FoldTeam(root, events)
	if err != nil || len(fold.Members) != 1 || fold.Members[0].Phase != teamMemberFailed {
		t.Fatalf("in-flight roster fold = %#v, %v", fold, err)
	}
	if child, getErr := engine.getSession(fold.Members[0].ID); getErr == nil {
		child.mu.Lock()
		attached := child.attached
		child.mu.Unlock()
		if attached {
			t.Fatal("Team close did not drain the child materialized by the admitted creation")
		}
	}
}

func TestAgentTeamCreationContainsRecoverySettlementConflict(t *testing.T) {
	dataDir := t.TempDir()
	base, err := NewJSONLSessionStore(filepath.Join(dataDir, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingTeamFlushStore{
		SessionStore: base, entered: make(chan struct{}), release: make(chan struct{}),
	}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dataDir, dataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.AgentTeams = &AgentTeamConfig{}
	cfg.Persist, cfg.SessionStore = true, store
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	rootID, err := engine.CreateSession(context.Background(), dataDir, "settlement-root", "")
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.except = rootID
	store.mu.Unlock()
	spawned := make(chan error, 1)
	go func() {
		_, err := engine.AgentTeams().SpawnTeammate(context.Background(), rootID, SpawnTeammateRequest{
			Name: "settlement-worker", Description: "Settlement worker", Prompt: []ContentBlock{{Type: "text", Text: "work"}},
			Context: "fresh", Provider: "spawn",
		})
		spawned <- err
	}()
	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("creation did not reach child durability checkpoint")
	}
	root := mustSession(t, engine, rootID)
	root.mu.Lock()
	events := append([]Event(nil), root.Events...)
	root.mu.Unlock()
	fold, err := FoldTeam(rootID, events)
	if err != nil || len(fold.Members) != 1 || fold.Members[0].Phase != teamMemberProvisioning {
		t.Fatalf("provisioning fold = %#v, %v", fold, err)
	}
	failed := fold.Members[0]
	failed.Phase, failed.Error = teamMemberFailed, "recovery settled first"
	if err := engine.agentTeams.append(root, "team/member", teamMemberEvent{
		Version: teamEventVersion, TeamID: rootID, Member: failed,
	}); err != nil {
		t.Fatal(err)
	}
	close(store.release)
	select {
	case err := <-spawned:
		if teamErrorCode(err) != "TEAM_PROVISIONING_CONFLICT" {
			t.Fatalf("spawn conflict = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("spawn did not settle after recovery conflict")
	}
	waitForModelSubagentDetached(t, engine, failed.ID)
}
