package harness

import (
	"context"
	"strings"
	"testing"
)

type modelSwitchProvider struct{ id string }

func (provider modelSwitchProvider) ID() string   { return provider.id }
func (provider modelSwitchProvider) Name() string { return provider.id }
func (provider modelSwitchProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: provider.id, Name: provider.id}}, nil
}
func (provider modelSwitchProvider) Complete(_ context.Context, _ ChatRequest, onDelta func(Delta) error) (Completion, error) {
	if err := onDelta(Delta{Text: provider.id, Finish: "stop"}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: provider.id, Finish: "stop"}, nil
}

func TestModelSwitchAddsOneDurableNotice(t *testing.T) {
	e := newIntegrationEngine(t)
	e.RegisterProvider(modelSwitchProvider{id: "next"})
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "switch", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "first"}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: "next", Model: "next"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "second"}}}); err != nil {
		t.Fatal(err)
	}
	session, _ := e.getSession(id)
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	count := 0
	for _, event := range events {
		if event.Type != "user/message" || eventSourceKind(event.Data) != "plugin" {
			continue
		}
		message := nestedMessage(event.Data)
		source, _ := message["source"].(map[string]any)
		if source["plugin"] != "model-selection" {
			continue
		}
		count++
		if text := sessionReferenceText(message["content"]); !strings.Contains(text, "echo/echo") || !strings.Contains(text, "next/next") {
			t.Fatalf("notice = %q", text)
		}
	}
	if count != 1 {
		t.Fatalf("model switch notices = %d", count)
	}
}
