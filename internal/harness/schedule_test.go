package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func scheduleResultValue(t *testing.T, result ToolResult) any {
	t.Helper()
	if result.Error != nil || result.IsError || len(result.Content) != 1 {
		t.Fatalf("schedule tool result = %#v", result)
	}
	var value any
	if err := json.Unmarshal([]byte(result.Content[0].Text), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestScheduleRulesReplayAndTimeZones(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC).UnixMilli()
	after, err := scheduleAfterRecord("schedule-1", " check logs ", 30, now)
	if err != nil || after.Prompt != "check logs" || after.ScheduledAt != "2026-08-05T12:00:30.000Z" {
		t.Fatalf("after = %#v, %v", after, err)
	}
	offset, err := scheduleAtRecord("schedule-2", "join", json.RawMessage(`"2026-08-06T09:00:00+08:00"`), now)
	if err != nil || offset.ScheduledAt != "2026-08-06T01:00:00.000Z" {
		t.Fatalf("offset = %#v, %v", offset, err)
	}
	local, err := scheduleAtRecord("schedule-3", "local", json.RawMessage(`{"date":"2026-08-07","time":"09:30:00","time_zone":"Asia/Shanghai"}`), now)
	if err != nil || local.ScheduledAt != "2026-08-07T01:30:00.000Z" {
		t.Fatalf("local = %#v, %v", local, err)
	}
	every, err := scheduleEveryRecord("schedule-4", "metrics", 300, now)
	if err != nil {
		t.Fatal(err)
	}
	accepted := time.Date(2026, 8, 5, 12, 16, 0, 0, time.UTC).UnixMilli()
	occurrence, next, err := resolveEverySchedule(every, accepted)
	if err != nil || occurrence != "2026-08-05T12:15:00.000Z" || next != "2026-08-05T12:20:00.000Z" {
		t.Fatalf("occurrence = %q, next = %q, err = %v", occurrence, next, err)
	}
	events := []Event{
		{Type: "schedule/change", Data: map[string]any{"version": 1, "operation": "create", "schedule": after.value()}},
		{Type: "schedule/change", Data: map[string]any{"version": 1, "operation": "create", "schedule": every.value()}},
		{Type: "schedule/change", Data: map[string]any{"version": 1, "operation": "dispatch", "id": after.ID}},
		{Type: "schedule/change", Data: map[string]any{"version": 1, "operation": "dispatch", "id": every.ID, "acceptedAt": "2026-08-05T12:16:00.000Z"}},
	}
	folded, err := foldScheduleEvents(events, 0)
	if err != nil || len(folded.active) != 1 || folded.active[0].ScheduledAt != next {
		t.Fatalf("folded = %#v, %v", folded, err)
	}
	if inherited, err := foldScheduleEvents(events, len(events)); err != nil || len(inherited.active) != 0 {
		t.Fatalf("fork suffix = %#v, %v", inherited, err)
	}
}

func TestScheduleToolsCRUDAndNeverReuseIDs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.ScheduleEnabled = true
	cfg.Provider, cfg.Model = "echo", "echo"
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id, err := e.CreateSession(context.Background(), t.TempDir(), "schedule-tools", "")
	if err != nil {
		t.Fatal(err)
	}
	created := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_create", id, map[string]any{"prompt": "later", "after_seconds": 3600})).(map[string]any)
	if created["id"] != "schedule-1" || created["state"] != "scheduled" {
		t.Fatalf("created = %#v", created)
	}
	listed := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_list", id, map[string]any{})).([]any)
	if len(listed) != 1 {
		t.Fatalf("listed = %#v", listed)
	}
	deleted := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_delete", id, map[string]any{"id": "schedule-1"})).(map[string]any)
	if deleted["deleted"] != true {
		t.Fatalf("deleted = %#v", deleted)
	}
	again := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_create", id, map[string]any{"prompt": "next", "after_seconds": 3600})).(map[string]any)
	if again["id"] != "schedule-2" {
		t.Fatalf("second create = %#v", again)
	}
	if _, err := e.Run(context.Background(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "seed"}}}); err != nil {
		t.Fatal(err)
	}
	child, err := e.ForkSession(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	childList := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_list", child, map[string]any{})).([]any)
	if len(childList) != 0 {
		t.Fatalf("fork inherited schedules = %#v", childList)
	}
}

func TestScheduleRuntimeDispatchesRealAgentTurn(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.ScheduleEnabled = true
	cfg.Provider, cfg.Model = "echo", "echo"
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id, err := e.CreateSession(context.Background(), t.TempDir(), "schedule-runtime", "")
	if err != nil {
		t.Fatal(err)
	}
	created := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_create", id, map[string]any{"prompt": `quote " safely`, "after_seconds": 1})).(map[string]any)
	if created["id"] != "schedule-1" {
		t.Fatalf("created = %#v", created)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		session, _ := e.getSession(id)
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		running := session.Running
		session.mu.Unlock()
		dispatched, pluginPrompt, completed := false, false, false
		for _, event := range events {
			if event.Type == "schedule/change" {
				data, _ := event.Data.(map[string]any)
				dispatched = dispatched || data["operation"] == "dispatch"
			}
			if event.Type == "user/message" && eventSourceKind(event.Data) == "plugin" && strings.Contains(contentValueText(event.Data), "[SCHEDULE REMINDER]") {
				pluginPrompt = true
			}
			if event.Type == "turn/end" {
				data, _ := event.Data.(map[string]any)
				reason, _ := data["reason"].(map[string]any)
				completed = completed || reason["kind"] == "completed"
			}
		}
		if dispatched && pluginPrompt && completed && !running {
			listed := scheduleResultValue(t, executeRegisteredTool(t, e, "schedule_list", id, map[string]any{})).([]any)
			if len(listed) != 0 {
				t.Fatalf("completed one-shot remained active = %#v", listed)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("scheduled agent turn did not complete")
}
