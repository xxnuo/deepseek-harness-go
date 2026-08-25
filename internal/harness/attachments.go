package harness

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	"image/png"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"
)

// ImageAttachmentRef is the durable, value-free image reference used in
// session events. Bytes are stored below Config.DataDir and never embedded in
// the session log.
type ImageAttachmentRef struct {
	AttachmentID       string           `json:"attachmentId"`
	MediaType          string           `json:"mediaType"`
	Bytes              int              `json:"bytes"`
	Width              int              `json:"width"`
	Height             int              `json:"height"`
	Name               string           `json:"name,omitempty"`
	OriginalDimensions *ImageDimensions `json:"originalDimensions,omitempty"`
}

type ImageDimensions struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

// ImageRequestPolicy is the route-owned pixel and encoded-byte budget used to
// derive a deterministic provider request version from a durable attachment.
type ImageRequestPolicy struct {
	MaxPixels int `json:"maxPixels"`
	MaxBytes  int `json:"maxBytes"`
}

// RequestImageAttachment is a transient, verified request image. It is never
// written to session history; only the durable ImageAttachmentRef is logged.
type RequestImageAttachment struct {
	VariantID  string             `json:"variantId"`
	Attachment ImageAttachmentRef `json:"attachment"`
	Data       []byte             `json:"-"`
	MediaType  string             `json:"mediaType"`
	Bytes      int                `json:"bytes"`
	Width      int                `json:"width"`
	Height     int                `json:"height"`
	Depth      string             `json:"depth"`
	Space      string             `json:"space"`
	HasAlpha   bool               `json:"hasAlpha"`
}

const (
	maxImageBytes               = 20 << 20
	maxImagesPerMessage         = 20
	maxMessageImageBytes        = 200 << 20
	maxImagePixels              = 64_000_000
	maxImageDimension           = 8192
	normalizedImageMaxDimension = 2048
	normalizedImageMaxBytes     = 4 << 20
)

// EncodedImageAttachment is the public wire shape used by slash commands.
// Admission validates and stores the bytes before a handler receives refs.
type EncodedImageAttachment struct {
	MediaType string `json:"mediaType"`
	Data      string `json:"data"`
	Name      string `json:"name,omitempty"`
}

var attachmentIDPattern = regexp.MustCompile(`^sha256:([a-f0-9]{64})$`)

type preparedImage struct {
	ref  ImageAttachmentRef
	data []byte
}

func imageExtension(mediaType string) string {
	switch mediaType {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "image/webp":
		return ".webp"
	case "image/gif":
		return ".gif"
	default:
		return ""
	}
}

func imageDimensions(mediaType string, data []byte) (int, int, error) {
	if imageExtension(mediaType) == "" {
		return 0, 0, errors.New("unsupported image media type")
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || config.Width <= 0 || config.Height <= 0 {
		if err == nil {
			err = errors.New("invalid image dimensions")
		}
		return 0, 0, err
	}
	want := strings.TrimPrefix(mediaType, "image/")
	if want == "jpeg" && format == "jpg" {
		format = "jpeg"
	}
	if format != want {
		return 0, 0, errors.New("declared image media type does not match the data")
	}
	if config.Width > maxImagePixels/config.Height {
		return 0, 0, errors.New("image exceeds the decoded pixel limit")
	}
	if config.Width > maxImageDimension || config.Height > maxImageDimension {
		return 0, 0, errors.New("image exceeds the configured per-side pixel limit")
	}
	if _, _, err := image.Decode(bytes.NewReader(data)); err != nil {
		return 0, 0, errors.New("image data is malformed")
	}
	return config.Width, config.Height, nil
}

func imageHasAlpha(img image.Image) bool {
	if opaque, ok := img.(interface{ Opaque() bool }); ok && opaque.Opaque() {
		return false
	}
	bounds := img.Bounds()
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			_, _, _, alpha := img.At(x, y).RGBA()
			if alpha != 0xffff {
				return true
			}
		}
	}
	return false
}

func encodeNormalizedImage(img image.Image, alpha bool, format string) ([]byte, string, error) {
	var output bytes.Buffer
	if alpha || format == "png" {
		if err := png.Encode(&output, img); err != nil {
			return nil, "", err
		}
		return output.Bytes(), "image/png", nil
	}
	if err := jpeg.Encode(&output, img, &jpeg.Options{Quality: 85}); err != nil {
		return nil, "", err
	}
	return output.Bytes(), "image/jpeg", nil
}

func normalizeImageData(mediaType string, data []byte, width, height int) ([]byte, string, int, int, *ImageDimensions, error) {
	if mediaType != "image/gif" && width <= normalizedImageMaxDimension && height <= normalizedImageMaxDimension && len(data) <= normalizedImageMaxBytes {
		return data, mediaType, width, height, nil, nil
	}
	decoded, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, "", 0, 0, nil, fmt.Errorf("image normalization failed: %w", err)
	}
	alpha := imageHasAlpha(decoded)
	targetWidth, targetHeight := width, height
	if width > normalizedImageMaxDimension || height > normalizedImageMaxDimension {
		scale := float64(normalizedImageMaxDimension) / float64(max(width, height))
		targetWidth = max(1, int(float64(width)*scale+0.5))
		targetHeight = max(1, int(float64(height)*scale+0.5))
	}
	original := (*ImageDimensions)(nil)
	if targetWidth != width || targetHeight != height || mediaType == "image/gif" {
		original = &ImageDimensions{Width: width, Height: height}
	}
	for {
		var target image.Image = decoded
		if targetWidth != width || targetHeight != height {
			dst := image.NewRGBA(image.Rect(0, 0, targetWidth, targetHeight))
			draw.CatmullRom.Scale(dst, dst.Bounds(), decoded, decoded.Bounds(), draw.Over, nil)
			target = dst
		}
		encoded, normalizedType, encodeErr := encodeNormalizedImage(target, alpha, "")
		if encodeErr != nil {
			return nil, "", 0, 0, nil, encodeErr
		}
		if len(encoded) <= normalizedImageMaxBytes {
			return encoded, normalizedType, targetWidth, targetHeight, original, nil
		}
		if targetWidth == 1 && targetHeight == 1 {
			return nil, "", 0, 0, nil, errors.New("image cannot be encoded within the normalized byte limit")
		}
		scale := 0.9
		targetWidth = max(1, int(float64(targetWidth)*scale))
		targetHeight = max(1, int(float64(targetHeight)*scale))
		if original == nil {
			original = &ImageDimensions{Width: width, Height: height}
		}
	}
}

func sanitizeImageName(value string) string {
	if slash := max(strings.LastIndex(value, "/"), strings.LastIndex(value, `\`)); slash >= 0 {
		value = value[slash+1:]
	}
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, value)
	value = strings.TrimSpace(value)
	if runes := []rune(value); len(runes) > 255 {
		value = string(runes[:255])
	}
	return value
}

func decodeImageBase64(encoded string) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || encoded == "" || base64.StdEncoding.EncodeToString(data) != encoded {
		return nil, errors.New("attachment-error: image data is not canonical base64")
	}
	return data, nil
}

func prepareImage(mediaType, encoded, name string) (preparedImage, error) {
	if imageExtension(mediaType) == "" {
		return preparedImage{}, errors.New("attachment-error: unsupported image media type")
	}
	data, err := decodeImageBase64(encoded)
	if err != nil {
		return preparedImage{}, err
	}
	if len(data) == 0 {
		return preparedImage{}, errors.New("attachment-error: image is empty")
	}
	if len(data) > maxImageBytes {
		return preparedImage{}, errors.New("attachment-error: image exceeds the configured byte limit")
	}
	w, h, err := imageDimensions(mediaType, data)
	if err != nil {
		return preparedImage{}, fmt.Errorf("attachment-error: %w", err)
	}
	normalized, normalizedType, normalizedWidth, normalizedHeight, originalDimensions, err := normalizeImageData(mediaType, data, w, h)
	if err != nil {
		return preparedImage{}, fmt.Errorf("attachment-error: %w", err)
	}
	hash := sha256.Sum256(normalized)
	ref := ImageAttachmentRef{AttachmentID: "sha256:" + hex.EncodeToString(hash[:]), MediaType: normalizedType, Bytes: len(normalized), Width: normalizedWidth, Height: normalizedHeight, OriginalDimensions: originalDimensions}
	if clean := sanitizeImageName(name); clean != "" {
		ref.Name = clean
	}
	return preparedImage{ref: ref, data: normalized}, nil
}

func (e *Engine) imagePath(ref ImageAttachmentRef) (string, error) {
	match := attachmentIDPattern.FindStringSubmatch(ref.AttachmentID)
	if len(match) != 2 || imageExtension(ref.MediaType) == "" {
		return "", errors.New("attachment-error: invalid image reference")
	}
	return filepath.Join(e.cfg.DataDir, "attachments", "v1", "objects", match[1][:2], match[1]), nil
}

func syncDirectory(path string) {
	if runtime.GOOS == "windows" {
		return
	}
	f, err := os.Open(path)
	if err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
}

func (e *Engine) storePreparedImage(image preparedImage) error {
	path, err := e.imagePath(image.ref)
	if err != nil {
		return err
	}
	root := filepath.Join(e.cfg.DataDir, "attachments", "v1")
	bucket, staging := filepath.Dir(path), filepath.Join(root, "tmp")
	if err := os.MkdirAll(bucket, 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(staging, ".image-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(image.data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmpName, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := os.ReadFile(path)
		if readErr != nil || !bytes.Equal(existing, image.data) {
			return errors.New("attachment-error: stored attachment failed integrity verification")
		}
	}
	syncDirectory(bucket)
	syncDirectory(filepath.Dir(bucket))
	return nil
}

// StoreImage validates and durably stores one base64 image. It is public so a
// custom frontend can upload images without knowing the on-disk layout.
func (e *Engine) StoreImage(mediaType, encoded, name string) (ImageAttachmentRef, error) {
	image, err := prepareImage(mediaType, encoded, name)
	if err != nil {
		return ImageAttachmentRef{}, err
	}
	if err := e.storePreparedImage(image); err != nil {
		return ImageAttachmentRef{}, err
	}
	return image.ref, nil
}

func (e *Engine) durablePromptContent(parts []PromptContentPart) ([]ContentBlock, error) {
	prepared := make([]*preparedImage, len(parts))
	imageCount, totalBytes := 0, 0
	for i, part := range parts {
		switch part.Type {
		case "text":
		case "image":
			image, err := prepareImage(part.MediaType, part.Data, part.Name)
			if err != nil {
				return nil, err
			}
			prepared[i] = &image
			imageCount++
			totalBytes += len(image.data)
		default:
			return nil, fmt.Errorf("bad-request: unsupported prompt content type %q", part.Type)
		}
	}
	if imageCount > maxImagesPerMessage {
		return nil, errors.New("attachment-error: image batch exceeds the configured image-count limit")
	}
	if totalBytes > maxMessageImageBytes {
		return nil, errors.New("attachment-error: image batch exceeds the configured aggregate image-byte limit")
	}
	for _, image := range prepared {
		if image != nil {
			if err := e.storePreparedImage(*image); err != nil {
				return nil, err
			}
		}
	}
	blocks := make([]ContentBlock, 0, len(parts))
	for i, part := range parts {
		if prepared[i] == nil {
			blocks = append(blocks, ContentBlock{Type: "text", Text: part.Text})
		} else {
			ref := prepared[i].ref
			blocks = append(blocks, ContentBlock{Type: "image", Attachment: &ref})
		}
	}
	return blocks, nil
}

func (e *Engine) readImage(ref ImageAttachmentRef) ([]byte, error) {
	path, err := e.imagePath(ref)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("attachment-error: image is unavailable")
	}
	hash := sha256.Sum256(data)
	if ref.AttachmentID != "sha256:"+hex.EncodeToString(hash[:]) {
		return nil, errors.New("attachment-error: stored attachment failed integrity verification")
	}
	w, h, metadataErr := imageDimensions(ref.MediaType, data)
	if metadataErr != nil || len(data) != ref.Bytes || w != ref.Width || h != ref.Height {
		return nil, errors.New("attachment-error: image metadata does not match stored bytes")
	}
	return data, nil
}

const requestImageTransformVersion = "request-image-v1"

// requestImageDimensions computes inward-rounded aspect-preserving dimensions
// without enlarging small images.
func requestImageDimensions(width, height, maxPixels int) (int, int, error) {
	if width <= 0 || height <= 0 || maxPixels <= 0 {
		return 0, 0, errors.New("attachment-error: image request policy must contain positive integers")
	}
	if width <= maxPixels/height {
		return width, height, nil
	}
	scale := math.Sqrt(float64(maxPixels) / float64(width*height))
	if width >= height {
		w := max(1, int(float64(width)*scale))
		h := max(1, int(float64(w)*float64(height)/float64(width)+0.5))
		for w > 1 && w*h > maxPixels {
			w--
			h = max(1, int(float64(w)*float64(height)/float64(width)+0.5))
		}
		return w, h, nil
	}
	h := max(1, int(float64(height)*scale))
	w := max(1, int(float64(h)*float64(width)/float64(height)+0.5))
	for h > 1 && w*h > maxPixels {
		h--
		w = max(1, int(float64(h)*float64(width)/float64(height)+0.5))
	}
	return w, h, nil
}

func requestImageVariantID(ref ImageAttachmentRef, policy ImageRequestPolicy) string {
	descriptor := fmt.Sprintf("%s\x00%s\x00%d\x00%d", requestImageTransformVersion, ref.AttachmentID, policy.MaxPixels, policy.MaxBytes)
	hash := sha256.Sum256([]byte(descriptor))
	return "sha256:" + hex.EncodeToString(hash[:])
}

func requestImageCachePath(root, variantID string) string {
	match := attachmentIDPattern.FindStringSubmatch(variantID)
	if len(match) != 2 {
		return ""
	}
	return filepath.Join(root, "request-images", match[1][:2], match[1])
}

// readImageRequest generates or reuses a deterministic request image version.
// The same exact bytes can subsequently be sent inline or uploaded to a Files
// API, which keeps both transport paths stable for one model route.
func (e *Engine) readImageRequest(ref ImageAttachmentRef, policy ImageRequestPolicy) (RequestImageAttachment, error) {
	if policy.MaxPixels <= 0 || policy.MaxBytes <= 0 {
		return RequestImageAttachment{}, errors.New("attachment-error: image request policy must contain positive integers")
	}
	data, err := e.readImage(ref)
	if err != nil {
		return RequestImageAttachment{}, err
	}
	variantID := requestImageVariantID(ref, policy)
	root := filepath.Join(e.cfg.DataDir, "attachments", "v1")
	cache := requestImageCachePath(root, variantID)
	if cache != "" {
		if cached, readErr := os.ReadFile(cache); readErr == nil && len(cached) <= policy.MaxBytes {
			if w, h, metadataErr := imageDimensions("image/png", cached); metadataErr == nil {
				return RequestImageAttachment{VariantID: variantID, Attachment: ref, Data: cached, MediaType: "image/png", Bytes: len(cached), Width: w, Height: h, Depth: "uchar", Space: "srgb", HasAlpha: true}, nil
			}
			if w, h, metadataErr := imageDimensions("image/jpeg", cached); metadataErr == nil {
				return RequestImageAttachment{VariantID: variantID, Attachment: ref, Data: cached, MediaType: "image/jpeg", Bytes: len(cached), Width: w, Height: h, Depth: "uchar", Space: "srgb"}, nil
			}
		}
	}
	w, h, err := imageDimensions(ref.MediaType, data)
	if err != nil {
		return RequestImageAttachment{}, err
	}
	targetWidth, targetHeight, err := requestImageDimensions(w, h, policy.MaxPixels)
	if err != nil {
		return RequestImageAttachment{}, err
	}
	decoded, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return RequestImageAttachment{}, fmt.Errorf("attachment-error: decode request image: %w", err)
	}
	alpha := imageHasAlpha(decoded)
	for {
		var target image.Image = decoded
		if targetWidth != w || targetHeight != h {
			dst := image.NewRGBA(image.Rect(0, 0, targetWidth, targetHeight))
			draw.CatmullRom.Scale(dst, dst.Bounds(), decoded, decoded.Bounds(), draw.Over, nil)
			target = dst
		}
		encoded, mediaType, encodeErr := encodeNormalizedImage(target, alpha, "")
		if encodeErr != nil {
			return RequestImageAttachment{}, fmt.Errorf("attachment-error: encode request image: %w", encodeErr)
		}
		if len(encoded) <= policy.MaxBytes {
			if cache != "" {
				if err := os.MkdirAll(filepath.Dir(cache), 0o700); err == nil {
					tmp, tempErr := os.CreateTemp(filepath.Dir(cache), ".request-image-*")
					if tempErr == nil {
						if _, writeErr := tmp.Write(encoded); writeErr == nil {
							_ = tmp.Close()
							_ = os.Rename(tmp.Name(), cache)
						} else {
							_ = tmp.Close()
						}
						_ = os.Remove(tmp.Name())
					}
				}
			}
			return RequestImageAttachment{VariantID: variantID, Attachment: ref, Data: encoded, MediaType: mediaType, Bytes: len(encoded), Width: targetWidth, Height: targetHeight, Depth: "uchar", Space: "srgb", HasAlpha: alpha}, nil
		}
		if targetWidth == 1 && targetHeight == 1 {
			return RequestImageAttachment{}, errors.New("attachment-error: image cannot be encoded within the model-request byte budget")
		}
		targetWidth = max(1, int(float64(targetWidth)*0.9))
		targetHeight = max(1, int(float64(targetHeight)*0.9))
	}
}

// ReadImageRequest exposes request-image derivation to custom frontends and
// provider adapters while keeping durable attachment bytes immutable.
func (e *Engine) ReadImageRequest(ref ImageAttachmentRef, policy ImageRequestPolicy) (RequestImageAttachment, error) {
	return e.readImageRequest(ref, policy)
}
