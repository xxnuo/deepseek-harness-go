package harness

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ImageReadValue is the lossless value returned by read_image. The image
// bytes are represented by the content-addressed reference in Image.
type ImageReadValue struct {
	Path  string             `json:"path"`
	Image ImageAttachmentRef `json:"image"`
}

var readImageMediaTypes = map[string]string{
	".png":  "image/png",
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".webp": "image/webp",
	".gif":  "image/gif",
}

func imageMediaTypeForPath(path string) string {
	return readImageMediaTypes[strings.ToLower(filepath.Ext(path))]
}

func formatImageReadOutput(path string, image ImageAttachmentRef) string {
	return fmt.Sprintf("<path>%s</path>\n<type>image</type>\n<content>\n%s image, %dx%d px, %d bytes\n</content>", path, image.MediaType, image.Width, image.Height, image.Bytes)
}

func imageModelSupportsInput(ctx context.Context, e *Engine, call ToolCall) error {
	if strings.TrimSpace(call.SessionID) == "" {
		return errors.New("cannot read image: the current model route could not be resolved")
	}
	s, err := e.getSession(call.SessionID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	selection := s.Model
	s.mu.Unlock()
	e.mu.RLock()
	provider := e.providers[selection.Provider]
	e.mu.RUnlock()
	if provider == nil || selection.Model == "" {
		return errors.New("cannot read image: the current model route could not be resolved")
	}
	models, err := provider.Models(ctx)
	if err != nil {
		return fmt.Errorf("cannot read image: resolve model route: %w", err)
	}
	for _, model := range models {
		if model.ID != selection.Model {
			continue
		}
		if containsString(model.InputModalities, "image") {
			return nil
		}
		return fmt.Errorf("cannot read image: model %q does not declare image input; switch to an image-capable model to read images", selection.Model)
	}
	return fmt.Errorf("cannot read image: model %q is unavailable", selection.Model)
}

func builtinReadImageTool(e *Engine) Tool {
	type input struct {
		FilePath string `json:"file_path"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "read_image",
			Description: "Read a PNG/JPEG/WebP/GIF file and return the image itself. Requires the current model to accept image input.",
			Parameters: objectSchema(map[string]any{
				"file_path": map[string]any{"type": "string", "description": "Path to the image file, resolved relative to the session workspace."},
			}, "file_path"),
			Output: objectSchema(map[string]any{"path": map[string]any{"type": "string"}, "image": objectSchema(map[string]any{
				"attachmentId": map[string]any{"type": "string"}, "mediaType": map[string]any{"type": "string", "enum": []string{"image/png", "image/jpeg", "image/webp", "image/gif"}},
				"bytes": map[string]any{"type": "integer"}, "width": map[string]any{"type": "integer"}, "height": map[string]any{"type": "integer"}, "name": map[string]any{"type": "string"},
			}, "attachmentId", "mediaType", "bytes", "width", "height")}, "path", "image"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if strings.TrimSpace(in.FilePath) == "" {
				return ToolResult{}, errors.New("file_path must be a non-empty string")
			}
			mediaType := imageMediaTypeForPath(in.FilePath)
			if mediaType == "" {
				return ToolResult{}, fmt.Errorf("cannot read %q: read_image only accepts PNG/JPEG/WebP/GIF paths", in.FilePath)
			}
			if err := imageModelSupportsInput(ctx, e, call); err != nil {
				return ToolResult{}, err
			}
			if err := ctx.Err(); err != nil {
				return ToolResult{}, err
			}
			target, err := resolveFSTarget(call.Workspace, in.FilePath)
			if err != nil {
				return ToolResult{}, err
			}
			data, version, _, err := readVersionedFile(target.targetKey)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					e.fsState.observe(call.SessionID, target, fsObservation{})
					return ToolResult{}, fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("cannot read %q: not found", target.displayPath))
				}
				return ToolResult{}, err
			}
			if len(data) > min(maxImageBytes, maxMessageImageBytes) {
				return ToolResult{}, errors.New("attachment-error: image exceeds the configured byte limit")
			}
			prepared, err := prepareImage(mediaType, base64.StdEncoding.EncodeToString(data), filepath.Base(target.displayPath))
			if err != nil {
				return ToolResult{}, err
			}
			if err := e.storePreparedImage(prepared); err != nil {
				return ToolResult{}, err
			}
			e.fsState.observe(call.SessionID, target, fsObservation{present: true, version: version})
			value := ImageReadValue{Path: target.displayPath, Image: prepared.ref}
			return ToolResult{
				Content: []ContentBlock{
					{Type: "text", Text: formatImageReadOutput(value.Path, value.Image)},
					{Type: "image", Attachment: &value.Image},
				},
				Value: value,
			}, nil
		},
	}
}
