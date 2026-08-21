package harness

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func newAgentTeamEngine(t *testing.T, dataDir string, sqlite bool) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dataDir, dataDir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.AgentTeams = &AgentTeamConfig{}
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
	members, err := engine.AgentTeams().ListMembers(root)
	if err != nil || len(members) != 2 || members[1].Name != "worker-one" || members[1].Status != "idle" {
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
	if err != nil || quiet.Status != "accepted" {
		t.Fatalf("quiet = %#v, %v", quiet, err)
	}
	if status := sessionTeamStatus(mustSession(t, engine, child)); status != "idle" {
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
	if _, err := engine.AgentTeams().SendMessage(context.Background(), child, SendTeamMessageRequest{
		Target: "worker-one", Delivery: teamMessageQuiet, Content: []ContentBlock{{Type: "text", Text: "self"}},
	}); teamErrorCode(err) != "TEAM_SELF_MESSAGE" {
		t.Fatalf("self-send error = %v", err)
	}

	first, err := engine.AgentTeams().CreateTask(root, CreateTeamTaskRequest{
		Subject: "First", Description: "Do first", WriteScopes: []string{"pkg/core/"},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.AgentTeams().CreateTask(child, CreateTeamTaskRequest{
		Subject: "Second", Description: "Do second", BlockedBy: []string{first.ID}, WriteScopes: []string{"pkg/core/sub"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.AgentTeams().UpdateTask(root, UpdateTeamTaskRequest{TaskID: second.ID, ExpectedRevision: second.Revision, Action: "claim"}); teamErrorCode(err) != "TEAM_TASK_BLOCKED" {
		t.Fatalf("blocked claim error = %v", err)
	}
	first, err = engine.AgentTeams().UpdateTask(child, UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "claim"})
	if err != nil || first.OwnerName != "worker-one" || first.Status != teamTaskInProgress {
		t.Fatalf("claimed first = %#v, %v", first, err)
	}
	first, err = engine.AgentTeams().UpdateTask(child, UpdateTeamTaskRequest{TaskID: first.ID, ExpectedRevision: first.Revision, Action: "complete"})
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
		value, err := engine.AgentTeams().WaitForChange(context.Background(), child, minimumTeamWait)
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
	child := mustSession(t, engine, fold.Members[0].ID)
	child.mu.Lock()
	attached := child.attached
	child.mu.Unlock()
	if attached {
		t.Fatal("Team close did not drain the child materialized by the admitted creation")
	}
}
