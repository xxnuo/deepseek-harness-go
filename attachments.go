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
	_ "image/jpeg"
	_ "image/png"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	_ "golang.org/x/image/webp"
)

// ImageAttachmentRef is the durable, value-free image reference used in
// session events. Bytes are stored below Config.DataDir and never embedded in
// the session log.
type ImageAttachmentRef struct {
	AttachmentID string `json:"attachmentId"`
	MediaType    string `json:"mediaType"`
	Bytes        int    `json:"bytes"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	Name         string `json:"name,omitempty"`
}

const (
	maxImageBytes        = 5 << 20
	maxImagesPerMessage  = 20
	maxMessageImageBytes = 100 << 20
	maxImagePixels       = 40_000_000
)

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
	if _, _, err := image.Decode(bytes.NewReader(data)); err != nil {
		return 0, 0, errors.New("image data is malformed")
	}
	return config.Width, config.Height, nil
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
	hash := sha256.Sum256(data)
	ref := ImageAttachmentRef{AttachmentID: "sha256:" + hex.EncodeToString(hash[:]), MediaType: mediaType, Bytes: len(data), Width: w, Height: h}
	if clean := sanitizeImageName(name); clean != "" {
		ref.Name = clean
	}
	return preparedImage{ref: ref, data: data}, nil
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
