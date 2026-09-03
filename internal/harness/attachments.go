package harness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"

	webpencode "github.com/HugoSmits86/nativewebp"
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

type decodedImageInput struct {
	mediaType string
	data      []byte
	name      string
}

type imageAdmissionFailure struct {
	err    error
	reason string
}

func (e *imageAdmissionFailure) Error() string { return e.err.Error() }
func (e *imageAdmissionFailure) Unwrap() error { return e.err }

func markImageAdmissionFailure(err error) error {
	return markImageAdmissionFailureReason(err, "")
}

func markImageAdmissionFailureReason(err error, reason string) error {
	if err == nil {
		return nil
	}
	var marked *imageAdmissionFailure
	if errors.As(err, &marked) {
		return err
	}
	return &imageAdmissionFailure{err: err, reason: reason}
}

func validateDecodedImageBatch(inputs []*decodedImageInput, maxCount, maxBytes int) error {
	count, totalBytes := 0, 0
	for _, input := range inputs {
		if input == nil {
			continue
		}
		count++
		totalBytes += len(input.data)
	}
	if count > maxCount {
		return markImageAdmissionFailureReason(errors.New("attachment-error: image batch exceeds the configured image-count limit"), "TOO_MANY_IMAGES")
	}
	if totalBytes > maxBytes {
		return markImageAdmissionFailureReason(errors.New("attachment-error: image batch exceeds the configured aggregate image-byte limit"), "IMAGE_BATCH_TOO_LARGE")
	}
	for _, input := range inputs {
		if input != nil && imageExtension(input.mediaType) == "" {
			return markImageAdmissionFailure(errors.New("attachment-error: unsupported image media type"))
		}
	}
	return nil
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
		return 0, 0, errors.New("image data is malformed")
	}
	want := strings.TrimPrefix(mediaType, "image/")
	if want == "jpeg" && format == "jpg" {
		format = "jpeg"
	}
	if format != want {
		return 0, 0, errors.New("declared image media type does not match the data")
	}
	width, height := config.Width, config.Height
	if orientation := containerFacts(mediaType, data).orientation; orientation >= 5 {
		width, height = height, width
	}
	if width > maxImagePixels/height {
		return 0, 0, errors.New("image exceeds the decoded pixel limit")
	}
	if width > maxImageDimension || height > maxImageDimension {
		return 0, 0, errors.New("image exceeds the configured per-side pixel limit")
	}
	if mediaType == "image/gif" {
		decoded, err := gif.DecodeAll(bytes.NewReader(data))
		if err != nil || len(decoded.Image) == 0 {
			return 0, 0, errors.New("image data is malformed")
		}
	} else if _, _, err := image.Decode(bytes.NewReader(data)); err != nil {
		return 0, 0, errors.New("image data is malformed")
	}
	return width, height, nil
}

func probeImageDimensions(mediaType string, data []byte) (int, int, error) {
	if imageExtension(mediaType) == "" {
		return 0, 0, errors.New("unsupported image media type")
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || config.Width <= 0 || config.Height <= 0 {
		return 0, 0, errors.New("image data is malformed")
	}
	want := strings.TrimPrefix(mediaType, "image/")
	if want == "jpeg" && format == "jpg" {
		format = "jpeg"
	}
	if format != want {
		return 0, 0, errors.New("declared image media type does not match the data")
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

type imageEncodingCandidate struct {
	data      []byte
	mediaType string
}

func encodePNGImage(img image.Image) (imageEncodingCandidate, error) {
	var output bytes.Buffer
	encoder := png.Encoder{CompressionLevel: png.BestCompression}
	if err := encoder.Encode(&output, img); err != nil {
		return imageEncodingCandidate{}, err
	}
	return imageEncodingCandidate{data: output.Bytes(), mediaType: "image/png"}, nil
}

func encodeJPEGImage(img image.Image, quality int) (imageEncodingCandidate, error) {
	var output bytes.Buffer
	if err := jpeg.Encode(&output, img, &jpeg.Options{Quality: quality}); err != nil {
		return imageEncodingCandidate{}, err
	}
	return imageEncodingCandidate{data: output.Bytes(), mediaType: "image/jpeg"}, nil
}

func encodeWebPImage(img image.Image) (imageEncodingCandidate, error) {
	var output bytes.Buffer
	if err := webpencode.Encode(&output, img, &webpencode.Options{CompressionLevel: webpencode.BestCompression}); err != nil {
		return imageEncodingCandidate{}, err
	}
	return imageEncodingCandidate{data: output.Bytes(), mediaType: "image/webp"}, nil
}

func encodingAttempts(img image.Image, alpha, lowColour bool, jpegQualities []int) []func() (imageEncodingCandidate, error) {
	if lowColour {
		return []func() (imageEncodingCandidate, error){
			func() (imageEncodingCandidate, error) { return encodePNGImage(img) },
			func() (imageEncodingCandidate, error) { return encodeWebPImage(img) },
		}
	}
	if alpha {
		return []func() (imageEncodingCandidate, error){func() (imageEncodingCandidate, error) { return encodeWebPImage(img) }}
	}
	attempts := make([]func() (imageEncodingCandidate, error), 0, len(jpegQualities))
	for _, quality := range jpegQualities {
		quality := quality
		attempts = append(attempts, func() (imageEncodingCandidate, error) { return encodeJPEGImage(img, quality) })
	}
	return attempts
}

func encodeFirstWithinLimit(attempts []func() (imageEncodingCandidate, error), maxBytes int) (imageEncodingCandidate, bool, error) {
	var smallest imageEncodingCandidate
	for index, attempt := range attempts {
		candidate, err := attempt()
		if err != nil {
			return imageEncodingCandidate{}, false, err
		}
		if len(candidate.data) <= maxBytes {
			return candidate, true, nil
		}
		if index == 0 || len(candidate.data) < len(smallest.data) {
			smallest = candidate
		}
	}
	return smallest, false, nil
}

func pngSampleDepth(data []byte) int {
	if len(data) > 24 && bytes.Equal(data[:8], []byte("\x89PNG\r\n\x1a\n")) {
		return int(data[24])
	}
	return 8
}

func encodedColourSpace(data []byte) string {
	config, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return "unknown"
	}
	switch config.ColorModel {
	case color.CMYKModel:
		return "cmyk"
	case color.GrayModel, color.Gray16Model:
		return "b-w"
	default:
		return "srgb"
	}
}

func encodedHasAlpha(mediaType string, data []byte, decoded image.Image) bool {
	switch mediaType {
	case "image/png":
		return len(data) > 25 && (data[25] == 4 || data[25] == 6)
	case "image/webp":
		if len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP" {
			for offset := 12; offset+8 <= len(data); {
				kind := string(data[offset : offset+4])
				length := int(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
				end := offset + 8 + length
				if length < 0 || end > len(data) {
					break
				}
				if kind == "ALPH" || (kind == "VP8X" && length > 0 && data[offset+8]&0x10 != 0) {
					return true
				}
				offset = end + length%2
			}
		}
	case "image/gif":
		for offset := 0; offset+7 < len(data); offset++ {
			if data[offset] == 0x21 && data[offset+1] == 0xf9 && data[offset+2] == 0x04 && data[offset+3]&0x01 != 0 {
				return true
			}
		}
	}
	return imageHasAlpha(decoded)
}

type imageContainerFacts struct {
	orientation     int
	carriesMetadata bool
	animated        bool
}

func tiffOrientation(data []byte) int {
	if len(data) >= 6 && bytes.Equal(data[:6], []byte("Exif\x00\x00")) {
		data = data[6:]
	}
	if len(data) < 8 {
		return 1
	}
	var order binary.ByteOrder
	switch string(data[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return 1
	}
	if order.Uint16(data[2:4]) != 42 {
		return 1
	}
	offset := int(order.Uint32(data[4:8]))
	if offset < 0 || offset+2 > len(data) {
		return 1
	}
	count := int(order.Uint16(data[offset : offset+2]))
	for index := 0; index < count; index++ {
		entry := offset + 2 + index*12
		if entry+12 > len(data) {
			break
		}
		if order.Uint16(data[entry:entry+2]) != 0x0112 || order.Uint16(data[entry+2:entry+4]) != 3 || order.Uint32(data[entry+4:entry+8]) != 1 {
			continue
		}
		orientation := int(order.Uint16(data[entry+8 : entry+10]))
		if orientation >= 1 && orientation <= 8 {
			return orientation
		}
	}
	return 1
}

func jpegContainerFacts(data []byte) imageContainerFacts {
	facts := imageContainerFacts{orientation: 1}
	for offset := 2; offset+4 <= len(data) && data[offset] == 0xff; {
		marker := data[offset+1]
		if marker == 0xd9 || marker == 0xda {
			break
		}
		if marker == 0x00 || marker == 0xff || (marker >= 0xd0 && marker <= 0xd7) {
			offset += 2
			continue
		}
		length := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
		if length < 2 || offset+2+length > len(data) {
			break
		}
		payload := data[offset+4 : offset+2+length]
		switch marker {
		case 0xe1:
			facts.carriesMetadata = true
			if orientation := tiffOrientation(payload); orientation != 1 {
				facts.orientation = orientation
			}
		case 0xe2, 0xed, 0xfe:
			facts.carriesMetadata = true
		}
		offset += 2 + length
	}
	return facts
}

func pngContainerFacts(data []byte) imageContainerFacts {
	facts := imageContainerFacts{orientation: 1}
	if len(data) < 8 || !bytes.Equal(data[:8], []byte("\x89PNG\r\n\x1a\n")) {
		return facts
	}
	for offset := 8; offset+12 <= len(data); {
		length := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		if length < 0 || offset+12+length > len(data) {
			break
		}
		kind := string(data[offset+4 : offset+8])
		payload := data[offset+8 : offset+8+length]
		switch kind {
		case "eXIf":
			facts.carriesMetadata = true
			facts.orientation = tiffOrientation(payload)
		case "iCCP", "tEXt", "zTXt", "iTXt":
			facts.carriesMetadata = true
		case "acTL":
			facts.animated = true
		}
		offset += 12 + length
		if kind == "IEND" {
			break
		}
	}
	return facts
}

func webpContainerFacts(data []byte) imageContainerFacts {
	facts := imageContainerFacts{orientation: 1}
	if len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WEBP" {
		return facts
	}
	for offset := 12; offset+8 <= len(data); {
		kind := string(data[offset : offset+4])
		length := int(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
		end := offset + 8 + length
		if length < 0 || end > len(data) {
			break
		}
		payload := data[offset+8 : end]
		switch kind {
		case "EXIF":
			facts.carriesMetadata = true
			facts.orientation = tiffOrientation(payload)
		case "ICCP", "XMP ":
			facts.carriesMetadata = true
		case "ANIM", "ANMF":
			facts.animated = true
		case "VP8X":
			if len(payload) > 0 && payload[0]&0x02 != 0 {
				facts.animated = true
			}
		}
		offset = end + length%2
	}
	return facts
}

func containerFacts(mediaType string, data []byte) imageContainerFacts {
	switch mediaType {
	case "image/jpeg":
		return jpegContainerFacts(data)
	case "image/png":
		return pngContainerFacts(data)
	case "image/webp":
		return webpContainerFacts(data)
	case "image/gif":
		return imageContainerFacts{orientation: 1, animated: true}
	default:
		return imageContainerFacts{orientation: 1}
	}
}

func orientImage(source image.Image, orientation int) image.Image {
	if orientation < 2 || orientation > 8 {
		return source
	}
	bounds := source.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	dstWidth, dstHeight := width, height
	if orientation >= 5 {
		dstWidth, dstHeight = height, width
	}
	destination := image.NewNRGBA(image.Rect(0, 0, dstWidth, dstHeight))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			dx, dy := x, y
			switch orientation {
			case 2:
				dx = width - 1 - x
			case 3:
				dx, dy = width-1-x, height-1-y
			case 4:
				dy = height - 1 - y
			case 5:
				dx, dy = y, x
			case 6:
				dx, dy = height-1-y, x
			case 7:
				dx, dy = height-1-y, width-1-x
			case 8:
				dx, dy = y, width-1-x
			}
			destination.Set(dx, dy, source.At(bounds.Min.X+x, bounds.Min.Y+y))
		}
	}
	return destination
}

func normalizeImageData(mediaType string, data []byte, width, height int) ([]byte, string, int, int, *ImageDimensions, error) {
	facts := containerFacts(mediaType, data)
	colourSpace := encodedColourSpace(data)
	forcePNG := mediaType == "image/gif" || (mediaType == "image/png" && (pngSampleDepth(data) != 8 || facts.carriesMetadata))
	forceNormalize := forcePNG || facts.animated || facts.carriesMetadata || facts.orientation != 1 || colourSpace != "srgb"
	if !forceNormalize && width <= normalizedImageMaxDimension && height <= normalizedImageMaxDimension && len(data) <= normalizedImageMaxBytes {
		return data, mediaType, width, height, nil, nil
	}
	decoded, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, "", 0, 0, nil, fmt.Errorf("image normalization failed: %w", err)
	}
	decoded = orientImage(decoded, facts.orientation)
	alpha := encodedHasAlpha(mediaType, data, decoded)
	lowColour := lowColourImage(decoded)
	targetWidth, targetHeight := width, height
	if width > normalizedImageMaxDimension || height > normalizedImageMaxDimension {
		scale := float64(normalizedImageMaxDimension) / float64(max(width, height))
		targetWidth = max(1, int(float64(width)*scale+0.5))
		targetHeight = max(1, int(float64(height)*scale+0.5))
	}
	original := (*ImageDimensions)(nil)
	if targetWidth != width || targetHeight != height {
		original = &ImageDimensions{Width: width, Height: height}
	}
	for {
		var target image.Image = decoded
		if targetWidth != width || targetHeight != height || forcePNG || facts.orientation != 1 || colourSpace != "srgb" {
			dst := image.NewRGBA(image.Rect(0, 0, targetWidth, targetHeight))
			draw.CatmullRom.Scale(dst, dst.Bounds(), decoded, decoded.Bounds(), draw.Over, nil)
			target = dst
		}
		attempts := encodingAttempts(target, alpha, lowColour, []int{85, 80, 75})
		encoded, fits, encodeErr := encodeFirstWithinLimit(attempts, normalizedImageMaxBytes)
		if encodeErr != nil {
			return nil, "", 0, 0, nil, encodeErr
		}
		if fits {
			verifiedType, verifiedWidth, verifiedHeight, verifiedAlpha, depth, space, verifyErr := detectedRequestImage(encoded.data)
			if verifyErr != nil || verifiedType != encoded.mediaType || verifiedWidth != targetWidth || verifiedHeight != targetHeight || depth != "uchar" || space != "srgb" || !requestAlphaCompatible(alpha, verifiedType, verifiedAlpha) {
				return nil, "", 0, 0, nil, errors.New("image normalization did not produce a single-frame 8-bit sRGB image with matching metadata")
			}
			return encoded.data, encoded.mediaType, targetWidth, targetHeight, original, nil
		}
		if targetWidth == 1 && targetHeight == 1 {
			return nil, "", 0, 0, nil, errors.New("image cannot be encoded within the normalized byte limit")
		}
		scale := math.Min(0.9, math.Sqrt(float64(normalizedImageMaxBytes)/float64(len(encoded.data)))*0.95)
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
		return nil, markImageAdmissionFailureReason(errors.New("attachment-error: image data is not canonical base64"), "INVALID_IMAGE_BASE64")
	}
	return data, nil
}

func decodeImageInput(mediaType, encoded, name string) (decodedImageInput, error) {
	data, err := decodeImageBase64(encoded)
	if err != nil {
		return decodedImageInput{}, err
	}
	return decodedImageInput{mediaType: mediaType, data: data, name: name}, nil
}

func prepareDecodedImage(input decodedImageInput) (preparedImage, error) {
	mediaType, data, name := input.mediaType, input.data, input.name
	if imageExtension(mediaType) == "" {
		return preparedImage{}, markImageAdmissionFailure(errors.New("attachment-error: unsupported image media type"))
	}
	if len(data) == 0 {
		return preparedImage{}, markImageAdmissionFailure(errors.New("attachment-error: image is empty"))
	}
	if len(data) > maxImageBytes {
		return preparedImage{}, markImageAdmissionFailure(errors.New("attachment-error: image exceeds the configured byte limit"))
	}
	w, h, err := imageDimensions(mediaType, data)
	if err != nil {
		return preparedImage{}, markImageAdmissionFailure(fmt.Errorf("attachment-error: %w", err))
	}
	normalized, normalizedType, normalizedWidth, normalizedHeight, originalDimensions, err := normalizeImageData(mediaType, data, w, h)
	if err != nil {
		if strings.Contains(err.Error(), "image cannot be encoded within the normalized byte limit") {
			return preparedImage{}, markImageAdmissionFailure(fmt.Errorf("attachment-error: %w", err))
		}
		return preparedImage{}, fmt.Errorf("attachment-error: %w", err)
	}
	hash := sha256.Sum256(normalized)
	ref := ImageAttachmentRef{AttachmentID: "sha256:" + hex.EncodeToString(hash[:]), MediaType: normalizedType, Bytes: len(normalized), Width: normalizedWidth, Height: normalizedHeight, OriginalDimensions: originalDimensions}
	if clean := sanitizeImageName(name); clean != "" {
		ref.Name = clean
	}
	return preparedImage{ref: ref, data: normalized}, nil
}

func prepareImage(mediaType, encoded, name string) (preparedImage, error) {
	input, err := decodeImageInput(mediaType, encoded, name)
	if err != nil {
		return preparedImage{}, err
	}
	return prepareDecodedImage(input)
}

func (e *Engine) imagePath(ref ImageAttachmentRef) (string, error) {
	match := attachmentIDPattern.FindStringSubmatch(ref.AttachmentID)
	if len(match) != 2 || imageExtension(ref.MediaType) == "" {
		return "", errors.New("attachment-error: invalid image reference")
	}
	return filepath.Join(e.cfg.DataDir, "attachments", "v1", "objects", match[1][:2], match[1]), nil
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (e *Engine) storePreparedImage(image preparedImage) (resultErr error) {
	defer func() {
		if resultErr != nil && !strings.HasPrefix(resultErr.Error(), "attachment-error:") {
			resultErr = fmt.Errorf("attachment-error: unable to persist image attachment: %w", resultErr)
		}
	}()
	hash := sha256.Sum256(image.data)
	if image.ref.AttachmentID != "sha256:"+hex.EncodeToString(hash[:]) || image.ref.Bytes != len(image.data) {
		return errors.New("attachment-error: prepared attachment bytes do not match their reference")
	}
	path, err := e.imagePath(image.ref)
	if err != nil {
		return err
	}
	root := filepath.Join(e.cfg.DataDir, "attachments", "v1")
	bucket, staging := filepath.Dir(path), filepath.Join(root, "tmp")
	if err := os.MkdirAll(bucket, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(bucket, 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(staging, 0o700); err != nil {
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
	if err := syncDirectory(bucket); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(bucket)); err != nil {
		return err
	}
	return nil
}

func (e *Engine) prepareImageContext(ctx context.Context, input decodedImageInput) (preparedImage, error) {
	select {
	case e.imageCompression <- struct{}{}:
		defer func() { <-e.imageCompression }()
	case <-ctx.Done():
		return preparedImage{}, context.Cause(ctx)
	}
	if err := ctx.Err(); err != nil {
		return preparedImage{}, context.Cause(ctx)
	}
	return prepareDecodedImage(input)
}

func (e *Engine) prepareDecodedImageBatch(ctx context.Context, decoded []*decodedImageInput) ([]*preparedImage, error) {
	prepared := make([]*preparedImage, len(decoded))
	prepareErrors := make([]error, len(decoded))
	var prepareWG sync.WaitGroup
	for index, input := range decoded {
		if input == nil {
			continue
		}
		prepareWG.Add(1)
		go func(index int, input decodedImageInput) {
			defer prepareWG.Done()
			image, err := e.prepareImageContext(ctx, input)
			if err != nil {
				prepareErrors[index] = err
				return
			}
			prepared[index] = &image
		}(index, *input)
	}
	prepareWG.Wait()
	for _, err := range prepareErrors {
		if err != nil {
			return nil, err
		}
	}
	return prepared, nil
}

// StoreImage validates and durably stores one base64 image. It is public so a
// custom frontend can upload images without knowing the on-disk layout.
func (e *Engine) StoreImage(mediaType, encoded, name string) (ImageAttachmentRef, error) {
	input, err := decodeImageInput(mediaType, encoded, name)
	if err != nil {
		return ImageAttachmentRef{}, err
	}
	image, err := e.prepareImageContext(context.Background(), input)
	if err != nil {
		return ImageAttachmentRef{}, err
	}
	if err := e.storePreparedImage(image); err != nil {
		return ImageAttachmentRef{}, err
	}
	return image.ref, nil
}

func (e *Engine) durablePromptContentContext(ctx context.Context, parts []PromptContentPart) ([]ContentBlock, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	decoded := make([]*decodedImageInput, len(parts))
	for i, part := range parts {
		switch part.Type {
		case "text":
		case "image":
			if part.decoded != nil {
				input := decodedImageInput{mediaType: part.MediaType, data: append([]byte(nil), part.decoded...), name: part.Name}
				decoded[i] = &input
			} else {
				input, err := decodeImageInput(part.MediaType, part.Data, part.Name)
				if err != nil {
					return nil, err
				}
				decoded[i] = &input
			}
		default:
			return nil, fmt.Errorf("bad-request: unsupported prompt content type %q", part.Type)
		}
	}
	if err := validateDecodedImageBatch(decoded, maxImagesPerMessage, maxMessageImageBytes); err != nil {
		return nil, err
	}
	prepared, err := e.prepareDecodedImageBatch(ctx, decoded)
	if err != nil {
		return nil, err
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

func (e *Engine) durablePromptContent(parts []PromptContentPart) ([]ContentBlock, error) {
	return e.durablePromptContentContext(context.Background(), parts)
}

func (e *Engine) readImageContext(ctx context.Context, ref ImageAttachmentRef) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}
	path, err := e.imagePath(ref)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, errors.New("attachment-error: image is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}
	hash := sha256.Sum256(data)
	if ref.AttachmentID != "sha256:"+hex.EncodeToString(hash[:]) {
		return nil, errors.New("attachment-error: stored attachment failed integrity verification")
	}
	w, h, metadataErr := probeImageDimensions(ref.MediaType, data)
	if metadataErr != nil || len(data) != ref.Bytes || w != ref.Width || h != ref.Height {
		return nil, errors.New("attachment-error: image metadata does not match stored bytes")
	}
	return data, nil
}

func (e *Engine) readImage(ref ImageAttachmentRef) ([]byte, error) {
	return e.readImageContext(context.Background(), ref)
}

const requestImageTransformVersion = "request-image-v4-go1"

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
	descriptor := fmt.Sprintf(`{"transformVersion":"%s","attachmentId":"%s","routePixelBudget":%d,"encodedByteBudget":%d,"encoding":{"png":{"compression":"best"},"webp":{"mode":"lossless","compressionLevel":6},"jpegQualities":[85,80],"order":["low-colour:png-webp","alpha:webp","opaque:jpeg"],"colourspace":"srgb"}}`, requestImageTransformVersion, ref.AttachmentID, policy.MaxPixels, policy.MaxBytes)
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

type sharedRequestImage struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelCauseFunc
	done    chan struct{}
	waiters int
	settled bool
	value   RequestImageAttachment
	err     error
}

func newSharedRequestImage(run func(context.Context) (RequestImageAttachment, error)) *sharedRequestImage {
	ctx, cancel := context.WithCancelCause(context.Background())
	shared := &sharedRequestImage{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go func() {
		shared.value, shared.err = run(ctx)
		shared.mu.Lock()
		shared.settled = true
		shared.mu.Unlock()
		close(shared.done)
	}()
	return shared
}

func (shared *sharedRequestImage) wait(ctx context.Context) (RequestImageAttachment, error) {
	if err := ctx.Err(); err != nil {
		return RequestImageAttachment{}, context.Cause(ctx)
	}
	shared.mu.Lock()
	shared.waiters++
	shared.mu.Unlock()
	release := func(cancelled bool) {
		shared.mu.Lock()
		shared.waiters--
		if cancelled && shared.waiters == 0 && !shared.settled {
			shared.cancel(context.Cause(ctx))
		}
		shared.mu.Unlock()
	}
	select {
	case <-shared.done:
		release(false)
		return shared.value, shared.err
	case <-ctx.Done():
		release(true)
		return RequestImageAttachment{}, context.Cause(ctx)
	}
}

func detectedRequestImage(data []byte) (string, int, int, bool, string, string, error) {
	decoded, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return "", 0, 0, false, "", "", err
	}
	mediaType := "image/" + format
	if format == "jpg" {
		mediaType = "image/jpeg"
	}
	if imageExtension(mediaType) == "" || mediaType == "image/gif" {
		return "", 0, 0, false, "", "", errors.New("unsupported request image format")
	}
	bounds := decoded.Bounds()
	depth := "uchar"
	if mediaType == "image/png" && pngSampleDepth(data) != 8 {
		depth = "ushort"
	}
	return mediaType, bounds.Dx(), bounds.Dy(), encodedHasAlpha(mediaType, data, decoded), depth, encodedColourSpace(data), nil
}

func requestAlphaCompatible(sourceAlpha bool, mediaType string, outputAlpha bool) bool {
	return outputAlpha == sourceAlpha || (sourceAlpha && !outputAlpha && mediaType == "image/webp")
}

func readCachedRequestImage(ctx context.Context, path string, ref ImageAttachmentRef, policy ImageRequestPolicy, sourceAlpha bool) (RequestImageAttachment, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if ctx.Err() != nil {
			return RequestImageAttachment{}, false, context.Cause(ctx)
		}
		return RequestImageAttachment{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return RequestImageAttachment{}, false, context.Cause(ctx)
	}
	mediaType, width, height, alpha, depth, space, err := detectedRequestImage(data)
	if err != nil {
		return RequestImageAttachment{}, false, nil
	}
	maxWidth, maxHeight, err := requestImageDimensions(ref.Width, ref.Height, policy.MaxPixels)
	if err != nil {
		return RequestImageAttachment{}, false, err
	}
	if len(data) > policy.MaxBytes || depth != "uchar" || space != "srgb" || width > maxWidth || height > maxHeight || !requestAlphaCompatible(sourceAlpha, mediaType, alpha) {
		return RequestImageAttachment{}, false, nil
	}
	return RequestImageAttachment{Attachment: ref, Data: data, MediaType: mediaType, Bytes: len(data), Width: width, Height: height, Depth: "uchar", Space: "srgb", HasAlpha: alpha}, true, nil
}

func lowColourImage(img image.Image) bool {
	bounds := img.Bounds()
	stepX := max(1, bounds.Dx()/128)
	stepY := max(1, bounds.Dy()/128)
	colours := make(map[uint32]struct{}, 257)
	for y := bounds.Min.Y; y < bounds.Max.Y; y += stepY {
		for x := bounds.Min.X; x < bounds.Max.X; x += stepX {
			r, g, b, a := img.At(x, y).RGBA()
			key := uint32(r>>11)<<15 | uint32(g>>11)<<10 | uint32(b>>11)<<5 | uint32(a>>11)
			colours[key] = struct{}{}
			if len(colours) > 256 {
				return false
			}
		}
	}
	return true
}

func writeRequestImageCache(ctx context.Context, path string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".request-image-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	return os.Rename(tmpName, path)
}

func (e *Engine) deriveRequestImage(ctx context.Context, ref ImageAttachmentRef, policy ImageRequestPolicy, variantID string) (RequestImageAttachment, error) {
	select {
	case e.imageCompression <- struct{}{}:
		defer func() { <-e.imageCompression }()
	case <-ctx.Done():
		return RequestImageAttachment{}, context.Cause(ctx)
	}
	data, err := e.readImageContext(ctx, ref)
	if err != nil {
		return RequestImageAttachment{}, err
	}
	sourceType, sourceWidth, sourceHeight, sourceAlpha, sourceDepth, sourceSpace, err := detectedRequestImage(data)
	if err != nil || sourceType != ref.MediaType || sourceWidth != ref.Width || sourceHeight != ref.Height || sourceDepth != "uchar" || sourceSpace != "srgb" {
		return RequestImageAttachment{}, errors.New("attachment-error: stored attachment metadata does not match stored bytes")
	}
	root := filepath.Join(e.cfg.DataDir, "attachments", "v1")
	cache := requestImageCachePath(root, variantID)
	if cache != "" {
		if cached, ok, cacheErr := readCachedRequestImage(ctx, cache, ref, policy, sourceAlpha); cacheErr != nil {
			return RequestImageAttachment{}, cacheErr
		} else if ok {
			cached.VariantID = variantID
			return cached, nil
		}
	}
	if ref.Width <= policy.MaxPixels/ref.Height && len(data) <= policy.MaxBytes {
		return RequestImageAttachment{VariantID: variantID, Attachment: ref, Data: data, MediaType: ref.MediaType, Bytes: len(data), Width: ref.Width, Height: ref.Height, Depth: "uchar", Space: "srgb", HasAlpha: sourceAlpha}, nil
	}
	targetWidth, targetHeight, err := requestImageDimensions(ref.Width, ref.Height, policy.MaxPixels)
	if err != nil {
		return RequestImageAttachment{}, err
	}
	decoded, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return RequestImageAttachment{}, fmt.Errorf("attachment-error: decode request image: %w", err)
	}
	lowColour := lowColourImage(decoded)
	for {
		if err := ctx.Err(); err != nil {
			return RequestImageAttachment{}, context.Cause(ctx)
		}
		var target image.Image = decoded
		if targetWidth != ref.Width || targetHeight != ref.Height {
			dst := image.NewRGBA(image.Rect(0, 0, targetWidth, targetHeight))
			draw.CatmullRom.Scale(dst, dst.Bounds(), decoded, decoded.Bounds(), draw.Over, nil)
			target = dst
		}
		encoded, fits, encodeErr := encodeFirstWithinLimit(encodingAttempts(target, sourceAlpha, lowColour, []int{85, 80}), policy.MaxBytes)
		if encodeErr != nil {
			return RequestImageAttachment{}, fmt.Errorf("attachment-error: encode request image: %w", encodeErr)
		}
		if fits {
			verifiedType, width, height, alpha, depth, space, verifyErr := detectedRequestImage(encoded.data)
			if verifyErr != nil || verifiedType != encoded.mediaType || width != targetWidth || height != targetHeight || depth != "uchar" || space != "srgb" || !requestAlphaCompatible(sourceAlpha, verifiedType, alpha) {
				return RequestImageAttachment{}, errors.New("attachment-error: encoded model-request image does not match its verified 8-bit sRGB metadata")
			}
			if cache != "" {
				if err := writeRequestImageCache(ctx, cache, encoded.data); err != nil {
					return RequestImageAttachment{}, err
				}
			}
			return RequestImageAttachment{VariantID: variantID, Attachment: ref, Data: encoded.data, MediaType: encoded.mediaType, Bytes: len(encoded.data), Width: width, Height: height, Depth: "uchar", Space: "srgb", HasAlpha: alpha}, nil
		}
		if targetWidth == 1 && targetHeight == 1 {
			return RequestImageAttachment{}, errors.New("attachment-error: image cannot be encoded within the model-request byte budget")
		}
		scale := math.Min(0.9, math.Sqrt(float64(policy.MaxBytes)/float64(len(encoded.data)))*0.95)
		targetWidth = max(1, int(float64(targetWidth)*scale))
		targetHeight = max(1, int(float64(targetHeight)*scale))
	}
}

// readImageRequest generates or reuses a deterministic request image version.
// Concurrent callers for the same variant share the transform, while each
// caller retains independent cancellation.
func (e *Engine) readImageRequest(ctx context.Context, ref ImageAttachmentRef, policy ImageRequestPolicy) (RequestImageAttachment, error) {
	if policy.MaxPixels <= 0 || policy.MaxBytes <= 0 {
		return RequestImageAttachment{}, errors.New("attachment-error: image request policy must contain positive integers")
	}
	if err := ctx.Err(); err != nil {
		return RequestImageAttachment{}, context.Cause(ctx)
	}
	variantID := requestImageVariantID(ref, policy)
	e.attachmentRequestMu.Lock()
	shared := e.attachmentRequestInflight[variantID]
	if shared != nil && shared.ctx.Err() != nil {
		delete(e.attachmentRequestInflight, variantID)
		shared = nil
	}
	if shared == nil {
		shared = newSharedRequestImage(func(sharedCtx context.Context) (RequestImageAttachment, error) {
			return e.deriveRequestImage(sharedCtx, ref, policy, variantID)
		})
		e.attachmentRequestInflight[variantID] = shared
		go func() {
			<-shared.done
			e.attachmentRequestMu.Lock()
			if e.attachmentRequestInflight[variantID] == shared {
				delete(e.attachmentRequestInflight, variantID)
			}
			e.attachmentRequestMu.Unlock()
		}()
	}
	e.attachmentRequestMu.Unlock()
	return shared.wait(ctx)
}

// ReadImageRequest exposes request-image derivation to custom frontends and
// provider adapters while keeping durable attachment bytes immutable.
func (e *Engine) ReadImageRequest(ref ImageAttachmentRef, policy ImageRequestPolicy) (RequestImageAttachment, error) {
	return e.ReadImageRequestContext(context.Background(), ref, policy)
}

// ReadImageRequestContext is the cancellable request-image derivation entry.
func (e *Engine) ReadImageRequestContext(ctx context.Context, ref ImageAttachmentRef, policy ImageRequestPolicy) (RequestImageAttachment, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return e.readImageRequest(ctx, ref, policy)
}

func (e *Engine) closeAttachmentRequests() {
	e.attachmentRequestMu.Lock()
	requests := make([]*sharedRequestImage, 0, len(e.attachmentRequestInflight))
	for _, request := range e.attachmentRequestInflight {
		requests = append(requests, request)
	}
	e.attachmentRequestMu.Unlock()
	reason := errors.New("attachment service disposed")
	for _, request := range requests {
		request.cancel(reason)
	}
	for _, request := range requests {
		<-request.done
	}
}
