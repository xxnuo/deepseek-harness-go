package harness

import (
	"context"
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

var errImageRouteUnresolved = errors.New("image model route could not be resolved")

func imageMediaTypeForPath(path string) string {
	return readImageMediaTypes[strings.ToLower(filepath.Ext(path))]
}

func formatImageReadOutput(path string, image ImageAttachmentRef) string {
	scaled := ""
	if image.OriginalDimensions != nil && image.Width > 0 && image.Height > 0 {
		x := float64(image.OriginalDimensions.Width) / float64(image.Width)
		y := float64(image.OriginalDimensions.Height) / float64(image.Height)
		if fmt.Sprintf("%.2f", x) == fmt.Sprintf("%.2f", y) {
			scaled = fmt.Sprintf(" (downscaled from %dx%d px; multiply coordinates by %.2f to locate features in the original file)", image.OriginalDimensions.Width, image.OriginalDimensions.Height, x)
		} else {
			scaled = fmt.Sprintf(" (downscaled from %dx%d px; multiply x coordinates by %.2f and y coordinates by %.2f to locate features in the original file)", image.OriginalDimensions.Width, image.OriginalDimensions.Height, x, y)
		}
	}
	return fmt.Sprintf("<path>%s</path>\n<type>image</type>\n<content>\n%s image, %dx%d px, %d bytes%s\n</content>", path, image.MediaType, image.Width, image.Height, image.Bytes, scaled)
}

// readImageAttachmentError turns recoverable image-admission failures into
// model-facing repair guidance. Storage and infrastructure errors retain their
// original message so callers can distinguish them from bad image input.
func readImageAttachmentError(path, mediaType string, err error) error {
	message := err.Error()
	switch {
	case strings.Contains(message, "image exceeds the configured per-side pixel limit"):
		return fmt.Errorf("cannot read %q: at least one image side exceeds the %dpx limit; downscale the image and read the smaller copy", path, maxImageDimension)
	case strings.Contains(message, "image exceeds the decoded pixel limit"):
		return fmt.Errorf("cannot read %q: the image exceeds the %d-pixel decoded-size limit; downscale the image and read the smaller copy", path, maxImagePixels)
	case strings.Contains(message, "image cannot be encoded within the normalized byte limit"):
		return fmt.Errorf("cannot read %q: the image cannot be stored within the deployment's byte limits; downscale the image and read the smaller copy", path)
	case strings.Contains(strings.ToLower(message), "16-bit png"):
		return fmt.Errorf("cannot read %q: the 16-bit PNG could not be converted to the normalized 8-bit sRGB form; convert it to an 8-bit PNG/JPEG/WebP and retry", path)
	case strings.Contains(message, "declared image media type does not match the data"):
		extension := strings.ToLower(filepath.Ext(path))
		return fmt.Errorf("cannot read %q: the %s extension declares %s, but the bytes use a different image format; rename the file to match its actual format if it is PNG/JPEG/WebP/GIF, or convert it to one of those formats", path, extension, mediaType)
	default:
		return err
	}
}

func imageModelSelection(e *Engine, call ToolCall) (ModelSelection, error) {
	if strings.TrimSpace(call.SessionID) == "" {
		return ModelSelection{}, errImageRouteUnresolved
	}
	s, err := e.getSession(call.SessionID)
	if err != nil {
		return ModelSelection{}, err
	}
	s.mu.Lock()
	selection := cloneModelSelection(s.Model)
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	if routed, ok := latestLoggedModel(events); ok {
		selection = routed
	}
	if strings.TrimSpace(selection.Provider) == "" || strings.TrimSpace(selection.Model) == "" {
		return ModelSelection{}, errImageRouteUnresolved
	}
	return selection, nil
}

func resolveExactModelInfo(ctx context.Context, e *Engine, selection ModelSelection) (ModelInfo, error) {
	if err := ctx.Err(); err != nil {
		return ModelInfo{}, context.Cause(ctx)
	}
	e.mu.RLock()
	provider := e.providers[selection.Provider]
	e.mu.RUnlock()
	if provider == nil {
		return ModelInfo{}, errImageRouteUnresolved
	}
	if resolver, ok := provider.(ExactModelInfoResolver); ok {
		model, resolveErr := resolver.ResolveModelInfo(ctx, selection.Model)
		if resolveErr != nil {
			if ctx.Err() != nil {
				return ModelInfo{}, context.Cause(ctx)
			}
			return ModelInfo{}, resolveErr
		}
		return model, nil
	}
	models, err := provider.Models(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ModelInfo{}, context.Cause(ctx)
		}
		return ModelInfo{}, err
	}
	for _, model := range models {
		if model.ID == selection.Model {
			return model, nil
		}
	}
	return ModelInfo{}, fmt.Errorf("model %q is unavailable", selection.Model)
}

func imageModelSupportsInput(ctx context.Context, e *Engine, call ToolCall, requestedPath string) error {
	selection, err := imageModelSelection(e, call)
	if err != nil {
		if errors.Is(err, errImageRouteUnresolved) {
			return fmt.Errorf("cannot read %q as an image: the current model route could not be resolved", requestedPath)
		}
		return err
	}
	model, err := resolveExactModelInfo(ctx, e, selection)
	if err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if errors.Is(err, errImageRouteUnresolved) {
			return fmt.Errorf("cannot read %q as an image: the current model route could not be resolved", requestedPath)
		}
		return fmt.Errorf("cannot read %q as an image: resolve model route: %w", requestedPath, err)
	}
	if containsString(model.InputModalities, "image") {
		return nil
	}
	return fmt.Errorf("cannot read %q as an image: model %q does not declare image input; switch to an image-capable model to read images", requestedPath, selection.Model)
}

func builtinReadImageTool(e *Engine) Tool {
	type input struct {
		FilePath string `json:"file_path"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "read_image",
			Description: "Read a PNG/JPEG/WebP/GIF file and return the image itself. Harness validates and downscales large supported images before the next model request, so use this tool directly instead of installing image libraries or creating thumbnails merely to inspect an image. Independent files may be read concurrently in small batches. Requires the current model to accept image input.",
			Parameters: objectSchema(map[string]any{
				"file_path": map[string]any{"type": "string", "description": "Path to the image file, resolved relative to the session workspace."},
			}, "file_path"),
			Output: objectSchema(map[string]any{"path": map[string]any{"type": "string"}, "image": objectSchema(map[string]any{
				"attachmentId": map[string]any{"type": "string"}, "mediaType": map[string]any{"type": "string", "enum": []string{"image/png", "image/jpeg", "image/webp", "image/gif"}},
				"bytes": map[string]any{"type": "integer"}, "width": map[string]any{"type": "integer"}, "height": map[string]any{"type": "integer"}, "name": map[string]any{"type": "string"}, "originalDimensions": objectSchema(map[string]any{
					"width": map[string]any{"type": "integer"}, "height": map[string]any{"type": "integer"},
				}, "width", "height"),
			}, "attachmentId", "mediaType", "bytes", "width", "height")}, "path", "image"),
		},
		IsConcurrencySafe: alwaysConcurrencySafe,
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
			if err := imageModelSupportsInput(ctx, e, call, in.FilePath); err != nil {
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
				return ToolResult{}, fmt.Errorf("cannot read %q: the image cannot be stored within the deployment's byte limits; downscale the image and read the smaller copy", target.displayPath)
			}
			prepared, err := e.prepareImageContext(ctx, decodedImageInput{mediaType: mediaType, data: data, name: filepath.Base(target.displayPath)})
			if err != nil {
				return ToolResult{}, readImageAttachmentError(target.displayPath, mediaType, err)
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
