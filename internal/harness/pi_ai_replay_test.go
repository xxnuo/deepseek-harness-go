package harness

import (
	"encoding/json"
	"testing"
)

func TestPiAIReplayStateSurvivesJSONAndValidatesAgainstDurableSource(t *testing.T) {
	empty := ""
	selection := ModelSelection{Provider: "anthropic", Model: "claude-alias"}
	content := []ContentBlock{{Type: "reasoning", Text: "thought", Signature: "signed"}, {Type: "text", Text: "done"}}
	state := piAIReplayState(selection, Completion{
		ReplayAPI: "anthropic-messages", ResponseModel: "claude-native", ResponseID: "msg-1", ProviderThinkingLevel: &empty,
	}, content, "stop")
	source := map[string]any{"kind": "model", "provider": selection.Provider, "model": selection.Model, "replayState": state}
	encoded, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var restored map[string]any
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	message := ChatMessage{Source: restored, ReasoningSignature: "signed"}
	applyPiAIReplayState(&message, content)
	if !message.ReplayStatePresent || !message.ReplayValid || message.NativeModel != "claude-native" || message.ProviderThinkingLevel == nil || *message.ProviderThinkingLevel != "" {
		t.Fatalf("restored replay = %#v", message)
	}

	restored["model"] = "different"
	invalid := ChatMessage{Source: restored, ReasoningSignature: "signed"}
	applyPiAIReplayState(&invalid, content)
	if invalid.ReplayValid || invalid.ReasoningSignature != "" {
		t.Fatalf("invalid replay was trusted: %#v", invalid)
	}
}
