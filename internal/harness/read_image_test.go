package harness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const readImagePNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"

type readImageProvider struct {
	id         string
	model      string
	modalities []string
	catalog    []ModelInfo
	resolveErr error
}

func (p *readImageProvider) ID() string   { return p.id }
func (p *readImageProvider) Name() string { return p.id }
func (p *readImageProvider) Models(context.Context) ([]ModelInfo, error) {
	if p.catalog != nil {
		return append([]ModelInfo(nil), p.catalog...), nil
	}
	return []ModelInfo{{ID: p.model, Name: p.model, InputModalities: p.modalities}}, nil
}
func (p *readImageProvider) ResolveModelInfo(ctx context.Context, model string) (ModelInfo, error) {
	if err := ctx.Err(); err != nil {
		return ModelInfo{}, context.Cause(ctx)
	}
	if p.resolveErr != nil {
		return ModelInfo{}, p.resolveErr
	}
	if model != p.model {
		return ModelInfo{}, errors.New("model unavailable")
	}
	return ModelInfo{ID: p.model, Name: p.model, InputModalities: append([]string(nil), p.modalities...)}, nil
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

func TestSniffImageMediaTypeMatchesSupportedSignatures(t *testing.T) {
	tests := []struct {
		data []byte
		want string
	}{
		{data: []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}, want: "image/png"},
		{data: []byte{0xff, 0xd8, 0xff, 0xe0}, want: "image/jpeg"},
		{data: []byte("GIF87a"), want: "image/gif"},
		{data: []byte("GIF89a"), want: "image/gif"},
		{data: []byte("RIFF\x00\x00\x00\x00WEBP"), want: "image/webp"},
		{data: []byte("RIFF\x00\x00\x00\x00WAVE")},
		{data: []byte{0x89, 0x50, 0x4e}},
	}
	for _, test := range tests {
		if got := sniffImageMediaType(test.data); got != test.want {
			t.Fatalf("sniffImageMediaType(%x) = %q, want %q", test.data, got, test.want)
		}
	}
	if imagePathExtension(".hidden") != "" || imagePathExtension("foo.") != "." {
		t.Fatalf("dotfile/trailing-dot extensions = %q/%q, want empty/dot", imagePathExtension(".hidden"), imagePathExtension("foo."))
	}
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

	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	arguments, err := json.Marshal(map[string]any{"file_path": "fixture.PNG"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.executeToolCalls(t.Context(), s, 1, 1, []ToolCall{{
		ID: "image-call", Name: "read_image", Arguments: arguments,
	}}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	var resultContent []ContentBlock
	var meta map[string]any
	for _, event := range s.Events {
		if event.Type != "tool/result" {
			continue
		}
		value, _ := event.Data.(map[string]any)
		message := nestedMessage(value)
		blocks := contentBlocks(message["content"])
		if len(blocks) != 1 || blocks[0].ToolCallID != "image-call" {
			continue
		}
		resultContent = blocks[0].Content
		meta, _ = value["meta"].(map[string]any)
	}
	s.mu.Unlock()
	if !reflect.DeepEqual(meta, map[string]any{"path": path}) {
		t.Fatalf("read_image presentation metadata = %#v", meta)
	}
	if len(resultContent) != 2 || resultContent[0].Type != "text" || resultContent[1].Type != "image" || resultContent[1].Attachment == nil {
		t.Fatalf("read_image content = %#v", resultContent)
	}
	if !strings.Contains(resultContent[0].Text, "<type>image</type>") {
		t.Fatalf("read_image envelope = %q", resultContent[0].Text)
	}
	image := *resultContent[1].Attachment
	if image.MediaType != "image/png" || image.Name != "fixture.PNG" {
		t.Fatalf("read_image attachment = %#v", image)
	}
	stored, err := e.readImage(image)
	if err != nil || !bytes.Equal(stored, data) {
		t.Fatalf("stored attachment err=%v equal=%v", err, bytes.Equal(stored, data))
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

func TestReadImageSniffsExtensionlessAndContentAddressedPaths(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &readImageProvider{id: "vision-extensionless", model: "vision-1", modalities: []string{"text", "image"}}
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "image-extensionless", "")
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
	path := filepath.Join(e.Config().Workspace, "avatar")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	first := executeRegisteredTool(t, e, "read_image", id, map[string]any{"file_path": "avatar"})
	value, ok := first.Value.(ImageReadValue)
	if !ok || value.Image.MediaType != "image/png" || value.Image.Name != "avatar" {
		t.Fatalf("extensionless read = %#v", first.Value)
	}
	objectPath, err := e.imagePath(value.Image)
	if err != nil {
		t.Fatal(err)
	}
	second := executeRegisteredTool(t, e, "read_image", id, map[string]any{"file_path": objectPath})
	reread, ok := second.Value.(ImageReadValue)
	if !ok || reread.Image.AttachmentID != value.Image.AttachmentID || reread.Image.MediaType != "image/png" {
		t.Fatalf("content-addressed read = %#v, want attachment %q", second.Value, value.Image.AttachmentID)
	}
}

func TestReadImageCapsExtensionlessFileBeforeFormatDetection(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &readImageProvider{id: "vision-bounded", model: "vision-1", modalities: []string{"text", "image"}}
	e.RegisterProvider(provider)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "image-bounded", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.model}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.Config().Workspace, "oversized")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(int64(min(maxImageBytes, maxMessageImageBytes)) + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	arguments, err := json.Marshal(map[string]any{"file_path": "oversized"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = readImageTool(t, e).Execute(t.Context(), ToolCall{
		Name: "read_image", Arguments: arguments, SessionID: id, Workspace: e.Config().Workspace,
	})
	if err == nil || !strings.Contains(err.Error(), "FS_TOO_LARGE") || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized extensionless error = %v", err)
	}
}

func TestReadImageMapsTruncatedSignatureToMalformedImage(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &readImageProvider{id: "vision-truncated", model: "vision-1", modalities: []string{"text", "image"}}
	e.RegisterProvider(provider)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "image-truncated", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.model}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.Config().Workspace, "truncated")
	if err := os.WriteFile(path, []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}, 0o600); err != nil {
		t.Fatal(err)
	}
	arguments, err := json.Marshal(map[string]any{"file_path": "truncated"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = readImageTool(t, e).Execute(t.Context(), ToolCall{
		Name: "read_image", Arguments: arguments, SessionID: id, Workspace: e.Config().Workspace,
	})
	if err == nil || !strings.Contains(err.Error(), "bytes do not decode as a supported PNG/JPEG/WebP/GIF image") || strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("truncated image error = %v", err)
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
	if err := call("note.txt"); err == nil || !strings.Contains(err.Error(), "extension does not declare a supported image format") {
		t.Fatalf("extension error = %v", err)
	}
}

func TestReadImageUsesLatestRequestHeaderAndExactRouteResolver(t *testing.T) {
	e := newIntegrationEngine(t)
	textProvider := &readImageProvider{id: "text-route", model: "text-1", modalities: []string{"text"}}
	visionProvider := &readImageProvider{id: "vision-route", model: "hidden-vision", modalities: []string{"text", "image"}, catalog: []ModelInfo{}}
	e.RegisterProvider(textProvider)
	e.RegisterProvider(visionProvider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "header-route", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: textProvider.ID(), Model: textProvider.model}); err != nil {
		t.Fatal(err)
	}
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "request/header", map[string]any{"header": map[string]any{"config": map[string]any{
		"provider": visionProvider.ID(), "model": visionProvider.model,
	}}, "reason": "change"}); err != nil {
		t.Fatal(err)
	}
	data, err := base64.StdEncoding.DecodeString(readImagePNG)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(e.Config().Workspace, "header.png"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	result := executeRegisteredTool(t, e, "read_image", id, map[string]any{"file_path": "header.png"})
	if result.IsError {
		t.Fatalf("read_image exact route result = %#v", result)
	}
}

func TestReadImageRouteResolutionPreservesFailureAndCancellation(t *testing.T) {
	e := newIntegrationEngine(t)
	provider := &readImageProvider{id: "failing-vision", model: "vision", modalities: []string{"text", "image"}, resolveErr: errors.New("catalog down")}
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "route-failure", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.model}); err != nil {
		t.Fatal(err)
	}
	if err := imageModelSupportsInput(context.Background(), e, ToolCall{SessionID: id}, "image.png"); err == nil ||
		!strings.Contains(err.Error(), "resolve model route: catalog down") || strings.Contains(err.Error(), "does not declare") {
		t.Fatalf("route resolution error = %v", err)
	}
	reason := errors.New("cancel route resolution")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(reason)
	if err := imageModelSupportsInput(ctx, e, ToolCall{SessionID: id}, "image.png"); !errors.Is(err, reason) {
		t.Fatalf("route cancellation = %v", err)
	}
}

func TestReadImageSchemaAndDescriptionExposeRC2ImageSemantics(t *testing.T) {
	e := newIntegrationEngine(t)
	e.mu.RLock()
	tool := e.tools["read_image"]
	e.mu.RUnlock()
	if !strings.Contains(tool.Schema.Description, "downscales") || !strings.Contains(tool.Schema.Description, "concurrently") {
		t.Fatalf("read_image description = %q", tool.Schema.Description)
	}
	imageSchema, ok := tool.Schema.Output["properties"].(map[string]any)["image"].(map[string]any)
	if !ok {
		t.Fatalf("read_image output image schema = %#v", tool.Schema.Output)
	}
	properties, ok := imageSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("read_image image properties = %#v", imageSchema)
	}
	original, ok := properties["originalDimensions"].(map[string]any)
	if !ok {
		t.Fatalf("read_image originalDimensions schema = %#v", properties)
	}
	if !reflect.DeepEqual(original["required"], []any{"width", "height"}) {
		t.Fatalf("originalDimensions required = %#v", original["required"])
	}
}

func TestFormatImageReadOutputIncludesCoordinateMapping(t *testing.T) {
	base := ImageAttachmentRef{MediaType: "image/jpeg", Bytes: 9, Width: 2, Height: 1}
	if got := formatImageReadOutput("/img/photo.jpg", base); strings.Contains(got, "downscaled") {
		t.Fatalf("unscaled output = %q", got)
	}
	base.OriginalDimensions = &ImageDimensions{Width: 4, Height: 2}
	if got := formatImageReadOutput("/img/photo.jpg", base); !strings.Contains(got, "downscaled from 4x2 px; multiply coordinates by 2.00 to locate features in the original file") {
		t.Fatalf("uniform scaling output = %q", got)
	}
	base.OriginalDimensions = &ImageDimensions{Width: 5, Height: 2}
	if got := formatImageReadOutput("/img/photo.jpg", base); !strings.Contains(got, "multiply x coordinates by 2.50 and y coordinates by 2.00") {
		t.Fatalf("axis scaling output = %q", got)
	}
}

func TestReadImageAttachmentErrorGuidesRecoverableFailures(t *testing.T) {
	tests := []struct {
		name string
		err  string
		want string
	}{
		{name: "dimension", err: "attachment-error: image exceeds the configured per-side pixel limit", want: "at least one image side exceeds"},
		{name: "pixels", err: "attachment-error: image exceeds the decoded pixel limit", want: "decoded-size limit"},
		{name: "bytes", err: "attachment-error: image cannot be encoded within the normalized byte limit", want: "deployment's byte limits"},
		{name: "16-bit", err: "attachment-error: The 16-bit PNG could not be converted to the normalized 8-bit sRGB form.", want: "convert it to an 8-bit PNG/JPEG/WebP"},
		{name: "mismatch", err: "attachment-error: declared image media type does not match the data", want: ".jpg extension declares image/jpeg"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := readImageAttachmentError("photo.jpg", "image/jpeg", errors.New(test.err)).Error()
			if !strings.Contains(got, test.want) {
				t.Fatalf("error = %q, want substring %q", got, test.want)
			}
		})
	}
}

func TestToolFSIncludesReadImage(t *testing.T) {
	if got := presetToolNames("@deepseek-ai/dsh-tool-fs", nil); !containsString(got, "read_image") {
		t.Fatalf("tool-fs tools = %#v", got)
	}
}
