package harness

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func newPersistentDomainEngine(t *testing.T) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = t.TempDir()
	cfg.Workspace = cfg.DataDir
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func blockStatePersistence(t *testing.T, e *Engine) {
	t.Helper()
	blocked := filepath.Join(t.TempDir(), "state-block")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.cfg.DataDir = blocked
}

func TestCreateWorkspaceCanonicalizesSymlinkPath(t *testing.T) {
	e := newPersistentDomainEngine(t)
	root := t.TempDir()
	target := filepath.Join(root, "project")
	link := filepath.Join(root, "project-link")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	first, created, err := e.CreateWorkspace(link)
	if err != nil || !created {
		t.Fatalf("CreateWorkspace(link) = %#v, %v, created=%v", first, err, created)
	}
	canonical, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	if first.Path != canonical {
		t.Fatalf("workspace path = %q, want canonical %q", first.Path, canonical)
	}
	second, created, err := e.CreateWorkspace(target)
	if err != nil || created || second.WorkspaceID != first.WorkspaceID {
		t.Fatalf("CreateWorkspace(target) = %#v, %v, created=%v; want same workspace", second, err, created)
	}
}

func TestRenameWorkspaceTitlesAreCaseSensitive(t *testing.T) {
	e := newPersistentDomainEngine(t)
	first, _, err := e.CreateWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := e.CreateWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.RenameWorkspace(first.WorkspaceID, "Work"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.RenameWorkspace(second.WorkspaceID, "work"); err != nil {
		t.Fatal(err)
	}
}

func TestSessionSummaryUsesTurnBlanknessAndHumanPromptRecency(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "summary-semantics", "")
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "session/title", map[string]any{"title": "standalone"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "user/message", map[string]any{
		"content": []ContentBlock{{Type: "text", Text: "plugin context"}},
		"source":  map[string]any{"kind": "plugin"},
	}); err != nil {
		t.Fatal(err)
	}
	row := e.ListSessions()[0]
	if !row.Blank {
		t.Fatal("standalone user-shaped event opened the conversation")
	}
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	prompt, err := e.appendEvent(s, "user/message", map[string]any{
		"content": []ContentBlock{{Type: "text", Text: "prompt"}},
		"source":  map[string]any{"kind": "user"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "assistant/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "answer"}}}); err != nil {
		t.Fatal(err)
	}
	// Make the non-human tail obviously newer than the prompt.
	s.mu.Lock()
	s.Events[len(s.Events)-1].Time = prompt.Time + 10_000
	s.mu.Unlock()
	row = e.ListSessions()[0]
	if row.Blank || row.UpdatedAt != prompt.Time {
		t.Fatalf("summary = blank:%v updated:%d, want blank:false updated:%d", row.Blank, row.UpdatedAt, prompt.Time)
	}
}

func TestHostDescribeCountsAttachedIdleSessions(t *testing.T) {
	e := newIntegrationEngine(t)
	if _, err := e.CreateSession(context.Background(), e.Config().Workspace, "attached-idle", ""); err != nil {
		t.Fatal(err)
	}
	value, rpcErr := e.dispatch(context.Background(), "host.describe", nil)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	describe := value.(map[string]any)
	if describe["attachedSessions"] != 1 {
		t.Fatalf("attachedSessions = %v, want 1", describe["attachedSessions"])
	}
	s, _ := e.getSession("attached-idle")
	s.mu.Lock()
	s.attached = false
	s.mu.Unlock()
	value, rpcErr = e.dispatch(context.Background(), "host.describe", nil)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if value.(map[string]any)["attachedSessions"] != 0 {
		t.Fatalf("cold attachedSessions = %v, want 0", value.(map[string]any)["attachedSessions"])
	}
}

func TestWorkspaceMutationsRollbackWhenStatePersistenceFails(t *testing.T) {
	e := newPersistentDomainEngine(t)
	firstDir, secondDir := t.TempDir(), t.TempDir()
	first, _, err := e.CreateWorkspace(firstDir)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := e.CreateWorkspace(secondDir)
	if err != nil {
		t.Fatal(err)
	}
	sessionOne, err := e.CreateSession(context.Background(), e.Config().Workspace, "domain-s1", "")
	if err != nil {
		t.Fatal(err)
	}
	sessionTwo, err := e.CreateSession(context.Background(), e.Config().Workspace, "domain-s2", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AttachSession(first.WorkspaceID, sessionOne); err != nil {
		t.Fatal(err)
	}
	if err := e.AttachSession(first.WorkspaceID, sessionTwo); err != nil {
		t.Fatal(err)
	}
	beforeOrder := append([]string(nil), e.workspaceOrder...)
	beforeIDs := append([]string(nil), e.workspaces[first.WorkspaceID].SessionIDs...)
	oldTitle := e.workspaces[first.WorkspaceID].Title
	blockStatePersistence(t, e)

	if _, err := e.RenameWorkspace(first.WorkspaceID, "renamed"); err == nil || !strings.Contains(err.Error(), "persist") {
		t.Fatalf("RenameWorkspace error = %v, want persistence failure", err)
	}
	if got := e.workspaces[first.WorkspaceID].Title; got != oldTitle {
		t.Fatalf("rename was not rolled back: %q", got)
	}
	if err := e.DeleteWorkspace(second.WorkspaceID); err == nil || !strings.Contains(err.Error(), "persist") {
		t.Fatalf("DeleteWorkspace error = %v, want persistence failure", err)
	}
	if e.workspaces[second.WorkspaceID] == nil {
		t.Fatal("delete was not rolled back")
	}
	if _, err := e.ArchiveSession(sessionOne); err == nil || !strings.Contains(err.Error(), "persist") {
		t.Fatalf("ArchiveSession error = %v, want persistence failure", err)
	}
	if e.archived[sessionOne] {
		t.Fatal("archive was not rolled back")
	}
	if _, rpcErr := e.reorderWorkspace(map[string]any{
		"workspaceId":       second.WorkspaceID,
		"beforeWorkspaceId": first.WorkspaceID,
	}); rpcErr == nil || rpcErr.Code != "gateway/internal" {
		t.Fatalf("reorderWorkspace error = %#v, want persistence failure", rpcErr)
	}
	if got := e.workspaceOrder; len(got) != len(beforeOrder) || got[0] != beforeOrder[0] || got[1] != beforeOrder[1] {
		t.Fatalf("workspace order = %#v, want %#v", got, beforeOrder)
	}
	if _, rpcErr := e.reorderSession(map[string]any{
		"workspaceId":     first.WorkspaceID,
		"sessionId":       sessionOne,
		"beforeSessionId": sessionTwo,
	}); rpcErr == nil || rpcErr.Code != "gateway/internal" {
		t.Fatalf("reorderSession error = %#v, want persistence failure", rpcErr)
	}
	if got := e.workspaces[first.WorkspaceID].SessionIDs; len(got) != len(beforeIDs) || got[0] != beforeIDs[0] || got[1] != beforeIDs[1] {
		t.Fatalf("session order = %#v, want %#v", got, beforeIDs)
	}
}

func TestWorkspaceMutationsEmitHostFramesAndAttachPrepends(t *testing.T) {
	e := newIntegrationEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := e.SubscribeHost(ctx)
	next := func() map[string]any {
		t.Helper()
		select {
		case frame := <-frames:
			return frame
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for host frame")
			return nil
		}
	}

	workspace, created, err := e.CreateWorkspace(t.TempDir())
	if err != nil || !created {
		t.Fatalf("CreateWorkspace() = %#v, %v, %v", workspace, created, err)
	}
	if frame := next(); frame["type"] != "host/workspace-changed" {
		t.Fatalf("create frame = %#v", frame)
	}
	renamed, err := e.RenameWorkspace(workspace.WorkspaceID, "renamed")
	if err != nil {
		t.Fatal(err)
	}
	if frame := next(); frame["type"] != "host/workspace-changed" || frame["workspace"].(Workspace).Title != renamed.Title {
		t.Fatalf("rename frame = %#v", frame)
	}

	first, err := e.CreateSession(context.Background(), workspace.Path, "workspace-frame-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if frame := next(); frame["type"] != "host/session-added" {
		t.Fatalf("session frame = %#v", frame)
	}
	if err := e.AttachSession(workspace.WorkspaceID, first); err != nil {
		t.Fatal(err)
	}
	if frame := next(); frame["workspace"].(Workspace).SessionIDs[0] != first {
		t.Fatalf("first attach frame = %#v", frame)
	}
	second, err := e.CreateSession(context.Background(), workspace.Path, "workspace-frame-2", "")
	if err != nil {
		t.Fatal(err)
	}
	_ = next()
	if err := e.AttachSession(workspace.WorkspaceID, second); err != nil {
		t.Fatal(err)
	}
	if frame := next(); !reflect.DeepEqual(frame["workspace"].(Workspace).SessionIDs, []string{second, first}) {
		t.Fatalf("second attach frame = %#v", frame)
	}
	if _, rpcErr := e.reorderSession(map[string]any{"workspaceId": workspace.WorkspaceID, "sessionId": first, "beforeSessionId": second}); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if frame := next(); !reflect.DeepEqual(frame["workspace"].(Workspace).SessionIDs, []string{first, second}) {
		t.Fatalf("reorder frame = %#v", frame)
	}
	if err := e.DeleteWorkspace(workspace.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	if frame := next(); frame["type"] != "host/workspace-removed" || frame["workspaceId"] != workspace.WorkspaceID {
		t.Fatalf("delete frame = %#v", frame)
	}
}

func TestGoalMutationRequiresExactRevisionAndRollsBackPersistence(t *testing.T) {
	e := newPersistentDomainEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "goal-s1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.GoalMutation(sessionID, "pause", "", 0, 0); err == nil || err.Error() != "goal-not-found" {
		t.Fatalf("pause without goal error = %v, want goal-not-found", err)
	}
	created, err := e.GoalMutation(sessionID, "create", "ship it", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	ref := created["ref"].(map[string]any)
	if _, err := e.GoalMutation(sessionID, "pause", "", 0, 0); err == nil || err.Error() != "goal-conflict" {
		t.Fatalf("pause with revision 0 error = %v, want goal-conflict", err)
	}
	blockStatePersistence(t, e)
	if _, err := e.GoalMutation(sessionID, "pause", "", int(ref["revision"].(int)), 0); err == nil || !strings.Contains(err.Error(), "persist") {
		t.Fatalf("pause persistence error = %v, want persistence failure", err)
	}
	if got := e.goals[sessionID].Phase; got != "active" {
		t.Fatalf("goal phase after failed pause = %q, want active", got)
	}
	if _, err := e.GoalMutation(sessionID, "clear", "", int(ref["revision"].(int)), 0); err == nil || !strings.Contains(err.Error(), "persist") {
		t.Fatalf("clear persistence error = %v, want persistence failure", err)
	}
	if _, ok := e.goals[sessionID]; !ok {
		t.Fatal("goal clear was not rolled back")
	}
}
