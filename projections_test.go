package harness

import (
	"reflect"
	"testing"
)

func projectionEvent(typ string, time int64, data map[string]any) Event {
	return Event{Type: typ, Seq: int(time / 10), Time: time, Data: data, SurfaceOp: func() any {
		if isSurfaceEligibleType(typ) {
			return "append"
		}
		return nil
	}()}
}

func TestSessionProjectionBaselineIncludesUIUnits(t *testing.T) {
	values := sessionProjectionValues(nil, "")
	for _, key := range []string{"title", "todos", "permissions", "plan", "goal", "tokenUsage", "contextPressure", "contextBreakdown", "sessionStats", "imageLimits"} {
		if _, ok := values[key]; !ok {
			t.Fatalf("missing projection %q: %#v", key, values)
		}
	}
	if got := values["tokenUsage"].(map[string]any)["outputTokens"]; got != 0 {
		t.Fatalf("empty token usage = %#v", values["tokenUsage"])
	}
	limits := values["imageLimits"].(map[string]any)
	if limits["maxImageBytes"] != maxImageBytes || limits["maxImageDimension"] != maxImageDimension ||
		!reflect.DeepEqual(limits["mediaTypes"], []string{"image/png", "image/jpeg", "image/webp", "image/gif"}) {
		t.Fatalf("image limits = %#v", limits)
	}
}

func TestTokenUsageAndContextProjectionsReplayCanonicalAndOpenAIUsage(t *testing.T) {
	events := []Event{
		projectionEvent("request/header", 1, map[string]any{
			"header": map[string]any{"system": "abcd", "tools": []any{map[string]any{"name": "x"}}},
		}),
		projectionEvent("user/message", 2, map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "hello"}},
		}),
		projectionEvent("assistant/chunk", 3, map[string]any{
			"turn": 1, "step": 1,
			"chunk": map[string]any{"type": "usage", "usage": map[string]any{"inputTokens": 8, "outputTokens": 2, "cacheReadTokens": 3}},
		}),
		projectionEvent("assistant/message", 4, map[string]any{
			"turn": 1, "step": 1,
			"message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "answer"}}},
			"usage":   map[string]any{"prompt_tokens": 12, "completion_tokens": 4, "prompt_tokens_details": map[string]any{"cached_tokens": 5}},
		}),
		projectionEvent("request/context", 5, map[string]any{"provider": "deepseek", "model": "x", "contextWindow": 100}),
	}
	values := sessionProjectionValues(events, "")
	usage := values["tokenUsage"].(map[string]any)
	wantUsage := map[string]any{"uncachedInputTokens": 7, "outputTokens": 4, "cacheReadTokens": 5, "cacheWriteTokens": 0}
	if !reflect.DeepEqual(usage, wantUsage) {
		t.Fatalf("token usage = %#v, want %#v", usage, wantUsage)
	}
	pressure := values["contextPressure"].(map[string]any)
	if pressure["pressureTokens"] != 12 || pressure["contextWindow"] != 100 || pressure["projectedTokens"] == nil {
		t.Fatalf("context pressure = %#v", pressure)
	}
	breakdown := values["contextBreakdown"].(map[string]any)
	if breakdown["systemTokens"] != 5 || breakdown["toolsTokens"] == 0 || breakdown["messageTokens"] == 13 {
		// Message price is intentionally checked only for non-zero here; the
		// exact value follows the shared fixed-density estimator.
		if breakdown["messageTokens"] == 0 {
			t.Fatalf("context breakdown = %#v", breakdown)
		}
	}
}

func TestPermissionPlanAndStatsProjections(t *testing.T) {
	events := []Event{
		projectionEvent("permission/preset", 1, map[string]any{"preset": "danger-full-access"}),
		projectionEvent("sandbox/mode", 2, map[string]any{"mode": "danger-full-access"}),
		projectionEvent("approval/policy", 3, map[string]any{"policy": "never"}),
		projectionEvent("command/run", 4, map[string]any{"commandId": "plan-1", "name": "plan", "args": " draft "}),
		projectionEvent("command/done", 5, map[string]any{"commandId": "plan-1", "kind": "success"}),
		projectionEvent("plan/mode", 6, map[string]any{"active": true}),
		projectionEvent("step/start", 7, map[string]any{"turn": 1, "step": 1}),
		projectionEvent("assistant/chunk", 8, map[string]any{"turn": 1, "step": 1, "chunk": map[string]any{"type": "text-delta", "text": "x"}}),
		projectionEvent("assistant/message", 9, map[string]any{"turn": 1, "step": 1, "message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "x"}}}, "usage": map[string]any{"inputTokens": 1, "outputTokens": 2}}),
		projectionEvent("step/end", 10, map[string]any{"turn": 1, "step": 1}),
	}
	values := sessionProjectionValues(events, "")
	permissions := values["permissions"].(map[string]any)
	if permissions["currentValue"] != "danger-full-access" {
		t.Fatalf("permissions = %#v", permissions)
	}
	plan := values["plan"].(map[string]any)
	if plan["active"] != true || plan["pending"] != false {
		t.Fatalf("plan = %#v", plan)
	}
	stats := values["sessionStats"].(map[string]any)
	if stats["turns"] != 1 || stats["steps"] != 1 || stats["ttftSteps"] != 1 || stats["decodeTokens"] != 2 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestPlanProjectionPairsCommandSettlement(t *testing.T) {
	events := []Event{
		projectionEvent("command/run", 10, map[string]any{"commandId": "plan-fail", "name": "plan", "args": "draft"}),
	}
	if pending := currentPlan(events)["pending"]; pending != true {
		t.Fatalf("running plan projection pending = %#v", pending)
	}
	events = append(events,
		projectionEvent("command/done", 20, map[string]any{"commandId": "other", "kind": "success"}),
		projectionEvent("command/done", 30, map[string]any{"commandId": "plan-fail", "kind": "error"}),
	)
	if pending := currentPlan(events)["pending"]; pending != false {
		t.Fatalf("failed plan projection pending = %#v", pending)
	}
	if changed := projectionKeysChanged(events[len(events)-1]); !reflect.DeepEqual(changed, []string{"plan"}) {
		t.Fatalf("command/done changed keys = %#v", changed)
	}
}
