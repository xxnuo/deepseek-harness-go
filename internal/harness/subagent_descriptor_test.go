package harness

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func descriptorEvent(data any) Event { return Event{Type: "subagent/descriptor", Data: data} }

func TestFoldSubagentDescriptorUsesFirstDescriptor(t *testing.T) {
	first := map[string]any{
		"version": SubagentDescriptorVersion, "mode": "continuable", "provider": "spawn", "label": "first",
		"agentProvider": "deepseek", "agentModel": "chat", "persona": "reviewer",
		"toolFilter": map[string]any{"allow": []any{"read"}, "deny": []string{"bash"}},
	}
	later := map[string]any{"version": SubagentDescriptorVersion, "mode": "one-shot", "provider": "other", "label": "later"}
	descriptor, err := FoldSubagentDescriptor([]Event{descriptorEvent(first), descriptorEvent(later)})
	if err != nil {
		t.Fatal(err)
	}
	if descriptor == nil || descriptor.Mode != "continuable" || descriptor.Provider != "spawn" || descriptor.Label == nil || *descriptor.Label != "first" || descriptor.Persona == nil || *descriptor.Persona != "reviewer" {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	if descriptor.ToolFilter == nil || !reflect.DeepEqual(descriptor.ToolFilter.Allow, []string{"read"}) || !reflect.DeepEqual(descriptor.ToolFilter.Deny, []string{"bash"}) {
		t.Fatalf("tool filter = %#v", descriptor.ToolFilter)
	}

	unsupported := map[string]any{"version": SubagentDescriptorVersion + 1, "mode": "continuable", "provider": "spawn", "label": "old"}
	if descriptor, err := FoldSubagentDescriptor([]Event{descriptorEvent(unsupported), descriptorEvent(first)}); err != nil || descriptor != nil {
		t.Fatalf("unsupported first descriptor = %#v, %v", descriptor, err)
	}
	malformed := map[string]any{"version": SubagentDescriptorVersion, "mode": "continuable", "provider": "spawn"}
	if descriptor, err := FoldSubagentDescriptor([]Event{descriptorEvent(malformed), descriptorEvent(first)}); err == nil || descriptor != nil || !strings.Contains(err.Error(), "label") {
		t.Fatalf("malformed first descriptor = %#v, %v", descriptor, err)
	}
}

func TestFoldSubagentDescriptorRejectsCompleteMalformedMatrix(t *testing.T) {
	cases := []struct {
		data any
		want string
	}{
		{"invalid", "payload must be an object"},
		{map[string]any{"provider": "spawn"}, "version must be a number"},
		{map[string]any{"version": 2, "provider": "spawn"}, "mode must be"},
		{map[string]any{"version": 2, "mode": "one-shot", "provider": "spawn", "persona": "bad"}, "unknown field"},
		{map[string]any{"version": 2, "mode": "continuable", "provider": 7, "label": "l"}, "provider must be a string"},
		{map[string]any{"version": 2, "mode": "continuable", "provider": "spawn", "label": 7}, "label must be a string"},
		{map[string]any{"version": 2, "mode": "continuable", "provider": "spawn", "label": "l", "toolFilter": []any{}}, "toolFilter must be an object"},
		{map[string]any{"version": 2, "mode": "continuable", "provider": "spawn", "label": "l", "toolFilter": map[string]any{}}, "must declare allow"},
		{map[string]any{"version": 2, "mode": "continuable", "provider": "spawn", "label": "l", "toolFilter": map[string]any{"deny": []any{7}}}, "deny must be an array of strings"},
	}
	for _, test := range cases {
		if descriptor, err := FoldSubagentDescriptor([]Event{descriptorEvent(test.data)}); err == nil || descriptor != nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("data %#v = %#v, %v; want %q", test.data, descriptor, err, test.want)
		}
	}
}

func TestFinalAssistantOutputMatchesCanonicalSelection(t *testing.T) {
	events := []Event{
		{Type: "assistant/chunk", Data: map[string]any{"chunk": map[string]any{"type": "text-delta", "text": "partial "}}},
		{Type: "assistant/message", Data: map[string]any{"message": map[string]any{"content": []ContentBlock{{Type: "reasoning", Text: "complete reasoning"}, {Type: "tool-call", ID: "call-1", Name: "read", Arguments: `{}`}}}}},
		{Type: "assistant/chunk", Data: map[string]any{"chunk": map[string]any{"type": "text-delta", "text": "later"}}},
		{Type: "assistant/message", Data: map[string]any{"message": map[string]any{"content": []ContentBlock{}}}},
	}
	want := []ContentBlock{{Type: "reasoning", Text: "complete reasoning"}, {Type: "tool-call", ID: "call-1", Name: "read", Arguments: `{}`}}
	if got := finalAssistantOutput(events); !reflect.DeepEqual(got, want) {
		t.Fatalf("message output = %#v", got)
	}
	fallback := []Event{
		{Type: "assistant/chunk", Data: map[string]any{"chunk": map[string]any{"type": "reasoning-delta", "text": "thinking"}}},
		{Type: "assistant/chunk", Data: map[string]any{"chunk": map[string]any{"type": "text-delta", "text": "partial "}}},
		{Type: "assistant/chunk", Data: map[string]any{"chunk": map[string]any{"type": "text-delta", "text": "answer"}}},
		{Type: "assistant/message", Data: map[string]any{"message": map[string]any{"content": []ContentBlock{}}}},
	}
	if got := finalAssistantOutput(fallback); !reflect.DeepEqual(got, []ContentBlock{{Type: "text", Text: "partial answer"}}) {
		t.Fatalf("fallback output = %#v", got)
	}
	custom := []Event{{Type: "assistant/message", Data: map[string]any{"message": map[string]any{"content": []any{
		map[string]any{"type": "provider-artifact", "uri": "artifact://one", "count": float64(2)},
	}}}}}
	if got := finalAssistantOutput(custom); !reflect.DeepEqual(got, []ContentBlock{{Type: "provider-artifact", Extra: map[string]any{"uri": "artifact://one", "count": json.Number("2")}}}) {
		t.Fatalf("custom output = %#v", got)
	}
}
