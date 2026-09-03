package harness

import (
	"reflect"
	"strings"
	"testing"
)

func turnOutlineEvent(typ string, seq int, data map[string]any) Event {
	return Event{Type: typ, Seq: SessionSeq(seq), Data: data}
}

func turnOutlineTextMessage(text string, source string) map[string]any {
	return map[string]any{
		"message": map[string]any{
			"content": []ContentBlock{{Type: "text", Text: text}},
			"source":  map[string]any{"kind": source},
		},
	}
}

func TestTurnOutlineProjectionFold(t *testing.T) {
	state := turnOutlineProjectionState{}
	state = applyTurnOutlineProjection(state, turnOutlineEvent("turn/start", 4, map[string]any{"turn": 1})).(turnOutlineProjectionState)
	state = applyTurnOutlineProjection(state, turnOutlineEvent("user/message", 5, turnOutlineTextMessage("  hello\nworld  ", "user"))).(turnOutlineProjectionState)
	unchanged := applyTurnOutlineProjection(state, turnOutlineEvent("user/message", 6, turnOutlineTextMessage("second prompt", "user")))
	if !reflect.DeepEqual(unchanged, state) {
		t.Fatalf("second prompt replaced first: %#v", unchanged)
	}
	state = applyTurnOutlineProjection(state, turnOutlineEvent("assistant/message", 7, map[string]any{
		"message": map[string]any{"content": []ContentBlock{{Type: "text", Text: "draft answer"}}},
	})).(turnOutlineProjectionState)
	if state.Turns[0].Response != "" || state.Draft != "draft answer" {
		t.Fatalf("open-turn state = %#v", state)
	}
	state = applyTurnOutlineProjection(state, turnOutlineEvent("turn/end", 8, nil)).(turnOutlineProjectionState)
	if state.Draft != "" || state.Turns[0].Response != "draft answer" {
		t.Fatalf("settled state = %#v", state)
	}
	if got := viewTurnOutlineProjection(state); !reflect.DeepEqual(got, []map[string]any{{"turn": 1, "seq": 4, "prompt": "hello world", "response": "draft answer"}}) {
		t.Fatalf("view = %#v", got)
	}
}

func TestTurnOutlineProjectionIgnoresIneligibleEventsAndBoundsPreview(t *testing.T) {
	state := turnOutlineProjectionState{}
	state = applyTurnOutlineProjection(state, turnOutlineEvent("user/message", 0, turnOutlineTextMessage("before", "user"))).(turnOutlineProjectionState)
	state = applyTurnOutlineProjection(state, turnOutlineEvent("turn/start", 1, map[string]any{"turn": 2})).(turnOutlineProjectionState)
	state = applyTurnOutlineProjection(state, turnOutlineEvent("user/message", 2, turnOutlineTextMessage("plugin prompt", "plugin"))).(turnOutlineProjectionState)
	long := strings.Repeat("x", 5000)
	state = applyTurnOutlineProjection(state, turnOutlineEvent("user/message", 3, turnOutlineTextMessage(long, "user"))).(turnOutlineProjectionState)
	if len(state.Turns) != 1 || len([]rune(state.Turns[0].Prompt)) == 0 || !strings.HasSuffix(state.Turns[0].Prompt, "…") {
		t.Fatalf("bounded prompt = %#v", state.Turns)
	}
	if utf16Units(state.Turns[0].Prompt) != turnOutlinePromptPreviewLimit {
		t.Fatalf("prompt units = %d", utf16Units(state.Turns[0].Prompt))
	}
	state = applyTurnOutlineProjection(state, turnOutlineEvent("turn/start", 4, map[string]any{"turn": 2})).(turnOutlineProjectionState)
	if len(state.Turns) != 1 {
		t.Fatalf("regressive boundary changed state = %#v", state)
	}
}

func TestTurnOutlineProjectionKeepsEmptyTurnsSliceForDraftAndOrphanEnd(t *testing.T) {
	state := turnOutlineProjectionState{}
	state = applyTurnOutlineProjection(state, turnOutlineEvent("assistant/message", 1, map[string]any{
		"message": map[string]any{"content": []ContentBlock{{Type: "text", Text: "orphan draft"}}},
	})).(turnOutlineProjectionState)
	if state.Turns == nil || len(state.Turns) != 0 || state.Draft != "orphan draft" {
		t.Fatalf("orphan draft state = %#v", state)
	}
	state = applyTurnOutlineProjection(state, turnOutlineEvent("turn/end", 2, nil)).(turnOutlineProjectionState)
	if state.Turns == nil || len(state.Turns) != 0 || state.Draft != "" {
		t.Fatalf("orphan end state = %#v", state)
	}
}

func TestTurnOutlinePreviewTrimsWhitespaceBeforeEllipsis(t *testing.T) {
	value := strings.Repeat("x", turnOutlinePromptPreviewLimit-2) + " " + "yy"
	got := turnOutlinePreview([]ContentBlock{{Type: "text", Text: value}}, turnOutlinePromptPreviewLimit)
	want := strings.Repeat("x", turnOutlinePromptPreviewLimit-2) + "…"
	if got != want {
		t.Fatalf("preview = %q, want %q", got, want)
	}
}
