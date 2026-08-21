package harness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const readImagePNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"

type readImageProvider struct {
	id         string
	model      string
	modalities []string
}

func (p *readImageProvider) ID() string   { return p.id }
func (p *readImageProvider) Name() string { return p.id }
func (p *readImageProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: p.model, Name: p.model, InputModalities: p.modalities}}, nil
}
func (p *readImageProvider) Complete(context.Context, ChatRequest, func(Delta) error) (Completion, error) {
	return Completion{Text: "unused", Finish: "stop"}, nil
}

func readImageTool(t *testing.T, e *Engine) Tool {
	t.Helper()
	e.mu.RLock()
	tool := e.tools["read_image"]
	e.mu.RUnlock()
	if tool.Execute == nil {
		t.Fatal("read_image is not registered")
	}
	return tool
}

func TestReadImagePersistsAttachmentAndHydratesToolHistory(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &readImageProvider{id: "vision", model: "vision-1", modalities: []string{"text", "image"}}
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "image-tool", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.model}); err != nil {
		t.Fatal(err)
	}
	data, err := base64.StdEncoding.DecodeString(readImagePNG)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.Config().Workspace, "fixture.PNG")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	result := executeRegisteredTool(t, e, "read_image", id, map[string]any{"file_path": "fixture.PNG"})
	value, ok := result.Value.(ImageReadValue)
	if !ok {
		t.Fatalf("read_image value = %#v", result.Value)
	}
	if value.Path != path || value.Image.MediaType != "image/png" || value.Image.Name != "fixture.PNG" {
		t.Fatalf("read_image value = %#v", value)
	}
	if len(result.Content) != 2 || result.Content[0].Type != "text" || result.Content[1].Type != "image" || result.Content[1].Attachment == nil {
		t.Fatalf("read_image content = %#v", result.Content)
	}
	if !strings.Contains(result.Content[0].Text, "<type>image</type>") {
		t.Fatalf("read_image envelope = %q", result.Content[0].Text)
	}
	stored, err := e.readImage(value.Image)
	if err != nil || !bytes.Equal(stored, data) {
		t.Fatalf("stored attachment err=%v equal=%v", err, bytes.Equal(stored, data))
	}

	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "tool/result", map[string]any{
		"turn": 1, "step": 1, "message": toolResultMessage("image-call", result.Content, false),
	}); err != nil {
		t.Fatal(err)
	}
	messages := e.durableMessages(s, 1)
	if len(messages) != 1 || messages[0].Role != "tool" || len(messages[0].Images) != 1 {
		t.Fatalf("hydrated messages = %#v", messages)
	}
	hydrated, err := base64.StdEncoding.DecodeString(messages[0].Images[0].Data)
	if err != nil || !bytes.Equal(hydrated, data) {
		t.Fatalf("hydrated image err=%v equal=%v", err, bytes.Equal(hydrated, data))
	}
}

func TestReadImageRejectsTextOnlyRouteAndNonImagePath(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &readImageProvider{id: "text", model: "text-1", modalities: []string{"text"}}
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "text-tool", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.model}); err != nil {
		t.Fatal(err)
	}
	call := func(path string) error {
		arguments, marshalErr := json.Marshal(map[string]any{"file_path": path})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		_, executeErr := readImageTool(t, e).Execute(context.Background(), ToolCall{
			Name: "read_image", Arguments: arguments, SessionID: id, Workspace: e.Config().Workspace,
		})
		return executeErr
	}
	if err := call("missing.png"); err == nil || !strings.Contains(err.Error(), "does not declare image input") {
		t.Fatalf("text route error = %v", err)
	}
	if err := call("note.txt"); err == nil || !strings.Contains(err.Error(), "only accepts PNG/JPEG/WebP/GIF") {
		t.Fatalf("extension error = %v", err)
	}
}

func TestToolFSIncludesReadImage(t *testing.T) {
	if got := presetToolNames("@deepseek-ai/dsh-tool-fs", nil); !containsString(got, "read_image") {
		t.Fatalf("tool-fs tools = %#v", got)
	}
}
