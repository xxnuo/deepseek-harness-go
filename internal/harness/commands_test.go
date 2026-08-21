package harness

import (
	"context"
	"strings"
	"testing"
)

type shortSummaryProvider struct{}

func (shortSummaryProvider) ID() string   { return "echo" }
func (shortSummaryProvider) Name() string { return "summary" }
func (shortSummaryProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "summary", Name: "summary"}}, nil
}
func (shortSummaryProvider) Complete(ctx context.Context, _ ChatRequest, onDelta func(Delta) error) (Completion, error) {
	if err := ctx.Err(); err != nil {
		return Completion{}, err
	}
	if err := onDelta(Delta{Text: "short checkpoint", Finish: "stop"}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: "short checkpoint", Finish: "stop"}, nil
}

func commandTestSession(t *testing.T) (*Engine, *Session) {
	t.Helper()
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	return e, s
}

func TestCommandCatalogAndLifecycle(t *testing.T) {
	e, s := commandTestSession(t)
	if got := len(commandCatalog()); got != 7 {
		t.Fatalf("command catalog length = %d", got)
	}
	if _, err := e.runCommand(s, "/unknown"); err == nil {
		t.Fatal("unknown command unexpectedly succeeded")
	}
	result, err := e.runCommand(s, "/feedback useful feedback")
	if err != nil || result.Command == nil || result.Command.Kind != "success" {
		t.Fatalf("feedback result = %#v, err=%v", result, err)
	}
	result, err = e.runCommand(s, "/permission danger-full-access")
	if err != nil || result.Command == nil || result.Command.Text != "preset danger-full-access" {
		t.Fatalf("permission result = %#v, err=%v", result, err)
	}
	result, err = e.runCommand(s, "/plan")
	if err != nil || result.Command == nil || !strings.Contains(result.Command.Text, "Plan mode on") {
		t.Fatalf("plan result = %#v, err=%v", result, err)
	}
	result, err = e.runCommand(s, "/plan off")
	if err != nil || result.Command == nil || result.Command.Text != "Plan mode off." {
		t.Fatalf("plan off result = %#v, err=%v", result, err)
	}
	result, err = e.runCommand(s, "/export")
	if err != nil || result.Command == nil || result.Command.Text != "Session log download requested." {
		t.Fatalf("export result = %#v, err=%v", result, err)
	}
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	var runs, dones int
	for _, event := range events {
		switch event.Type {
		case "command/run":
			runs++
		case "command/done":
			dones++
		}
	}
	if runs != 5 || dones != 5 {
		t.Fatalf("lifecycle counts = %d/%d, events=%#v", runs, dones, events)
	}
}

func TestGoalCommandLifecycle(t *testing.T) {
	e, s := commandTestSession(t)
	result, err := e.runCommand(s, "/goal ship the release")
	if err != nil || result.Command == nil || !strings.Contains(result.Command.Text, "Goal created") {
		t.Fatalf("create = %#v, err=%v", result, err)
	}
	for _, line := range []string{"/goal pause", "/goal resume", "/goal edit ship it", "/goal complete"} {
		if line == "/goal complete" {
			goal, _ := e.GetGoal(s.Header.ID)
			if goal == nil {
				t.Fatal("goal disappeared before complete")
			}
			if _, err := e.GoalMutation(s.Header.ID, "complete", "", goal.Revision, 0); err != nil {
				t.Fatal(err)
			}
			continue
		}
		result, err = e.runCommand(s, line)
		if err != nil || result.Command == nil || result.Command.Kind != "success" {
			t.Fatalf("%s = %#v, err=%v", line, result, err)
		}
	}
	result, err = e.runCommand(s, "/goal clear")
	if err != nil || result.Command == nil || result.Command.Text != "Goal cleared." {
		t.Fatalf("clear = %#v, err=%v", result, err)
	}
}

func TestCompactCommandReplacesDurableSurface(t *testing.T) {
	e, s := commandTestSession(t)
	e.RegisterProvider(shortSummaryProvider{})
	long := strings.Repeat("context ", 200)
	if _, err := e.appendEvent(s, "user/message", map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: long}},
		"source": map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "assistant/message", map[string]any{
		"turn": 1, "step": 1, "message": map[string]any{
			"id": newID("msg"), "role": "assistant", "content": []ContentBlock{{Type: "text", Text: long}},
		},
	}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "user/message", map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: "keep this tail"}},
		"source": map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}
	result, err := e.runCommand(s, "/compact")
	if err != nil || result.Command == nil || result.Command.Kind != "success" {
		t.Fatalf("compact = %#v, err=%v", result, err)
	}
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	var ended bool
	for _, event := range events {
		if event.Type == "compaction/end" {
			ended = true
		}
	}
	if !ended {
		t.Fatalf("compact events have no end = %#v", events)
	}
	surface, err := foldSurfaceEvents(events, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(surface) != 2 || !strings.Contains(contentValueText(nestedMessage(surface[0].Data)["content"]), "short checkpoint") {
		t.Fatalf("compacted surface = %#v", surface)
	}
}
