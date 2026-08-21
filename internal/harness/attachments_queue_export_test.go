package harness

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// A tiny lossless WebP generated from a 1x1 red pixel. Go's standard image
// package has no WebP encoder, so keep this wire fixture inline.
const oneByOneWebP = "UklGRjwAAABXRUJQVlA4IDAAAADQAQCdASoBAAEAAgA0JaACdLoB+AADsAD+8MQL/yC5YXXI1/8gP+QH/ID/+PIAAAA="

func attachmentFixtureBytes(t *testing.T) map[string][]byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 2, 1))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	img.Set(1, 0, color.RGBA{G: 255, A: 255})
	var pngData, jpegData, gifData bytes.Buffer
	if err := png.Encode(&pngData, img); err != nil {
		t.Fatal(err)
	}
	if err := jpeg.Encode(&jpegData, img, &jpeg.Options{Quality: 90}); err != nil {
		t.Fatal(err)
	}
	if err := gif.Encode(&gifData, img, nil); err != nil {
		t.Fatal(err)
	}
	webpData, err := base64.StdEncoding.DecodeString(oneByOneWebP)
	if err != nil {
		t.Fatal(err)
	}
	return map[string][]byte{
		"image/png":  pngData.Bytes(),
		"image/jpeg": jpegData.Bytes(),
		"image/gif":  gifData.Bytes(),
		"image/webp": webpData,
	}
}

func attachmentEngine(t *testing.T, persist bool) *Engine {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir, cfg.Workspace = dir, dir
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.Persist = persist
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func b64(data []byte) string { return base64.StdEncoding.EncodeToString(data) }

func TestAttachmentAdmissionFormatsAndIntegrity(t *testing.T) {
	e := attachmentEngine(t, false)
	fixtures := attachmentFixtureBytes(t)
	for mediaType, data := range fixtures {
		mediaType, data := mediaType, data
		t.Run(mediaType, func(t *testing.T) {
			ref, err := e.StoreImage(mediaType, b64(data), `C:\Users\alice\private\photo.`+strings.TrimPrefix(mediaType, "image/"))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(ref.AttachmentID, "sha256:") || len(ref.AttachmentID) != len("sha256:")+64 {
				t.Fatalf("attachment id = %q", ref.AttachmentID)
			}
			if ref.Name != "photo."+strings.TrimPrefix(mediaType, "image/") {
				t.Fatalf("sanitized name = %q", ref.Name)
			}
			if ref.Bytes != len(data) || ref.Width <= 0 || ref.Height <= 0 {
				t.Fatalf("metadata = %#v", ref)
			}
			got, err := e.readImage(ref)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("readImage() err=%v equal=%v", err, bytes.Equal(got, data))
			}
		})
	}

	pngData := fixtures["image/png"]
	if _, err := e.StoreImage("image/jpeg", b64(pngData), "wrong.jpg"); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mime mismatch error = %v", err)
	}
	canonical := b64(pngData)
	for _, encoded := range []string{canonical + "\n", strings.TrimRight(canonical, "=")} {
		if _, err := e.StoreImage("image/png", encoded, "x.png"); err == nil || !strings.Contains(err.Error(), "canonical base64") {
			t.Fatalf("non-canonical %q error = %v", encoded[len(encoded)-min(8, len(encoded)):], err)
		}
	}

	ref, err := e.StoreImage("image/png", canonical, "x.png")
	if err != nil {
		t.Fatal(err)
	}
	path, err := e.imagePath(ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := e.readImage(ref); err == nil || !strings.Contains(err.Error(), "integrity") {
		t.Fatalf("tampered read error = %v", err)
	}
}

func TestAttachmentRC8ByteAndDimensionLimits(t *testing.T) {
	if maxImageBytes != int(3.5*1024*1024) || maxImageDimension != 2000 {
		t.Fatalf("limits = %d bytes, %d px", maxImageBytes, maxImageDimension)
	}
	e := attachmentEngine(t, false)
	oversizedBytes := append([]byte(nil), attachmentFixtureBytes(t)["image/png"]...)
	oversizedBytes = append(oversizedBytes, make([]byte, maxImageBytes+1-len(oversizedBytes))...)
	if _, err := e.StoreImage("image/png", b64(oversizedBytes), "large.png"); err == nil ||
		!strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("oversized byte error = %v", err)
	}
	wide := image.NewRGBA(image.Rect(0, 0, maxImageDimension+1, 1))
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, wide); err != nil {
		t.Fatal(err)
	}
	if _, err := e.StoreImage("image/png", b64(encoded.Bytes()), "wide.png"); err == nil ||
		!strings.Contains(err.Error(), "per-side") {
		t.Fatalf("oversized dimension error = %v", err)
	}
}

type imageCaptureProvider struct {
	mu       sync.Mutex
	requests []ChatRequest
}

func (p *imageCaptureProvider) ID() string   { return "image-capture" }
func (p *imageCaptureProvider) Name() string { return "Image Capture" }
func (p *imageCaptureProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "image-capture", Name: "Image Capture", InputModalities: []string{"text", "image"}}}, nil
}
func (p *imageCaptureProvider) Complete(_ context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	p.mu.Unlock()
	if err := onDelta(Delta{Text: "image received", Finish: "stop"}); err != nil {
		return Completion{}, err
	}
	return Completion{Text: "image received", Finish: "stop"}, nil
}
func (p *imageCaptureProvider) lastRequest() ChatRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.requests[len(p.requests)-1]
}

func TestImagePromptStoresOnlyRefAndHydratesProvider(t *testing.T) {
	e := attachmentEngine(t, false)
	provider := &imageCaptureProvider{}
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "image-session", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: "image-capture"}); err != nil {
		t.Fatal(err)
	}
	pngData := attachmentFixtureBytes(t)["image/png"]
	if _, err := e.Prompt(context.Background(), id, PromptRequest{SessionID: id, Mode: "queue", Content: []PromptContentPart{{Type: "image", MediaType: "image/png", Data: b64(pngData), Name: "/home/user/secret.png"}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := e.WaitForIdle(ctx, id); err != nil {
		t.Fatal(err)
	}
	req := provider.lastRequest()
	if len(req.Messages) == 0 || len(req.Messages[0].Images) != 1 {
		t.Fatalf("provider messages = %#v", req.Messages)
	}
	decoded, err := base64.StdEncoding.DecodeString(req.Messages[0].Images[0].Data)
	if err != nil || !bytes.Equal(decoded, pngData) || req.Messages[0].Images[0].MediaType != "image/png" {
		t.Fatalf("provider image = %#v decode err=%v equal=%v", req.Messages[0].Images[0], err, bytes.Equal(decoded, pngData))
	}

	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	var ref ImageAttachmentRef
	var userJSON []byte
	for _, event := range s.Events {
		if event.Type != "user/message" {
			continue
		}
		data, _ := json.Marshal(event.Data)
		userJSON = data
		if content := nestedMessage(event.Data)["content"]; content != nil {
			blocks := contentBlocks(content)
			if len(blocks) != 1 || blocks[0].Type != "image" || blocks[0].Attachment == nil {
				t.Fatalf("user content = %#v", blocks)
			}
			ref = *blocks[0].Attachment
		}
		break
	}
	s.mu.Unlock()
	if len(userJSON) == 0 || bytes.Contains(userJSON, []byte(b64(pngData))) {
		t.Fatalf("user event leaked image bytes: %s", userJSON)
	}
	if ref.AttachmentID == "" {
		t.Fatal("user event did not contain attachment ref")
	}

	value, rpcErr := e.sessionAttachment(map[string]any{"sessionId": id, "attachmentId": ref.AttachmentID})
	if rpcErr != nil || value == nil {
		t.Fatalf("authorized session.attachment value=%#v err=%v", value, rpcErr)
	}
	attachmentValue, ok := value.(map[string]any)
	if !ok || attachmentValue["attachment"] == nil {
		t.Fatalf("authorized attachment value = %#v", value)
	}
	encoded, _ := attachmentValue["data"].(string)
	returned, decodeErr := base64.StdEncoding.DecodeString(encoded)
	if decodeErr != nil || !bytes.Equal(returned, pngData) {
		t.Fatalf("authorized attachment bytes err=%v equal=%v", decodeErr, bytes.Equal(returned, pngData))
	}
	otherID, err := e.CreateSession(context.Background(), e.Config().Workspace, "other-image-session", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, rpcErr := e.sessionAttachment(map[string]any{"sessionId": otherID, "attachmentId": ref.AttachmentID}); rpcErr == nil || rpcErr.Code != "attachment-error" {
		t.Fatalf("unauthorized session.attachment error = %#v", rpcErr)
	}
}

func TestOpenAIImageWireUsesDataURLParts(t *testing.T) {
	wire := openAIWireMessages([]ChatMessage{{
		Role:    "user",
		Content: "describe",
		Images:  []ChatImage{{MediaType: "image/png", Data: "AQID"}},
	}})
	if len(wire) != 1 {
		t.Fatalf("wire messages = %#v", wire)
	}
	parts, ok := wire[0].Content.([]map[string]any)
	if !ok || len(parts) != 2 {
		t.Fatalf("wire content = %#v (%T)", wire[0].Content, wire[0].Content)
	}
	if parts[0]["type"] != "text" || parts[0]["text"] != "describe" {
		t.Fatalf("text part = %#v", parts[0])
	}
	imagePart, ok := parts[1]["image_url"].(map[string]any)
	if !ok || imagePart["url"] != "data:image/png;base64,AQID" {
		t.Fatalf("image part = %#v", parts[1])
	}
}

func TestSessionExportZIPDeduplicatesMedia(t *testing.T) {
	e := attachmentEngine(t, false)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "export-session", "")
	if err != nil {
		t.Fatal(err)
	}
	pngData := attachmentFixtureBytes(t)["image/png"]
	ref, err := e.StoreImage("image/png", b64(pngData), "export.png")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(mustSession(t, e, id), "user/message", map[string]any{"content": []ContentBlock{{Type: "image", Attachment: &ref}}}); err != nil {
		t.Fatal(err)
	}
	// The second occurrence is nested in a tool result; both must map to one ZIP entry.
	if _, err := e.appendEvent(mustSession(t, e, id), "tool/result", map[string]any{"message": map[string]any{"content": []ContentBlock{{Type: "tool-result", Content: []ContentBlock{{Type: "image", Attachment: &ref}}}}}}); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := e.ExportSessionZIP(id, false, &archive); err != nil {
		t.Fatal(err)
	}
	zr, err := zip.NewReader(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	if err != nil {
		t.Fatal(err)
	}
	wantMedia := "media/" + ref.AttachmentID + ".png"
	seen := map[string]int{}
	var media []byte
	for _, file := range zr.File {
		seen[file.Name]++
		if file.Name == wantMedia {
			reader, err := file.Open()
			if err != nil {
				t.Fatal(err)
			}
			media, err = io.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if seen["session.jsonl"] != 1 || seen[wantMedia] != 1 || len(seen) != 2 {
		t.Fatalf("zip entries = %#v, want session.jsonl + %s", seen, wantMedia)
	}
	if !bytes.Equal(media, pngData) {
		t.Fatalf("exported media differs: %d vs %d bytes", len(media), len(pngData))
	}
}

func mustSession(t *testing.T, e *Engine, id string) *Session {
	t.Helper()
	s, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func queueItems(frame streamFrame) []map[string]any {
	payload, _ := frame.payload.(map[string]any)
	items, _ := payload["items"].([]map[string]any)
	return items
}

func waitQueueFrame(t *testing.T, frames <-chan streamFrame, wantID string) streamFrame {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case frame, ok := <-frames:
			if !ok {
				t.Fatal("stream closed before queue frame")
			}
			if frame.method != "session/queue" {
				continue
			}
			items := queueItems(frame)
			if wantID == "" || queueContainsID(items, wantID) {
				return frame
			}
		case <-deadline:
			t.Fatalf("timed out waiting for queue frame %q", wantID)
		}
	}
}

func queueContainsID(items []map[string]any, id string) bool {
	for _, item := range items {
		if item["id"] == id {
			return true
		}
	}
	return false
}

func TestQueueSnapshotsEditRemoveSteerAndMuxConcurrency(t *testing.T) {
	e := attachmentEngine(t, false)
	provider := newQueuedTestProvider()
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "queue-snapshot", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Prompt(context.Background(), id, PromptRequest{SessionID: id, Content: []PromptContentPart{{Type: "text", Text: "running"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("blocking turn did not start")
	}
	if _, err := e.Prompt(context.Background(), id, PromptRequest{SessionID: id, Content: []PromptContentPart{{Type: "text", Text: "second"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Prompt(context.Background(), id, PromptRequest{SessionID: id, Content: []PromptContentPart{{Type: "text", Text: "third"}}}); err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	s.mu.Lock()
	if len(s.pending) != 2 {
		s.mu.Unlock()
		t.Fatalf("pending = %d, want 2", len(s.pending))
	}
	secondID, thirdID := s.pending[0].id, s.pending[1].id
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := e.streamFrames(ctx, "mux")
	baseline := waitQueueFrame(t, frames, secondID)
	if len(queueItems(baseline)) != 2 {
		t.Fatalf("baseline queue = %#v", queueItems(baseline))
	}

	edit := map[string]any{"sessionId": id, "itemId": secondID, "action": map[string]any{"kind": "edit", "content": []any{map[string]any{"type": "text", "text": "edited"}}}}
	if _, rpcErr := e.updateQueue(edit); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	edited := waitQueueFrame(t, frames, secondID)
	if got := queueItems(edited)[0]["message"].(map[string]any)["content"].([]ContentBlock)[0].Text; got != "edited" {
		t.Fatalf("edited queue text = %q", got)
	}

	if _, rpcErr := e.updateQueue(map[string]any{"sessionId": id, "itemId": thirdID, "action": map[string]any{"kind": "steer"}}); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	steered := waitQueueFrame(t, frames, thirdID)
	var foundSteering bool
	for _, item := range queueItems(steered) {
		if item["id"] == thirdID && item["placement"] == "steering" {
			foundSteering = true
		}
	}
	if !foundSteering {
		t.Fatalf("steered queue = %#v", queueItems(steered))
	}

	if _, rpcErr := e.updateQueue(map[string]any{"sessionId": id, "itemId": secondID, "action": map[string]any{"kind": "remove"}}); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	removed := waitQueueFrame(t, frames, "")
	if queueContainsID(queueItems(removed), secondID) {
		t.Fatalf("removed item remains: %#v", queueItems(removed))
	}

	// Exercise subscribe/emit/unsubscribe while the same queue is live. The
	// channel ownership stays with Engine; callers only cancel their contexts.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localCtx, localCancel := context.WithCancel(ctx)
			defer localCancel()
			ch := e.SubscribeMux(localCtx)
			for range ch {
			}
		}()
	}
	for i := 0; i < 100; i++ {
		e.emitQueue(s)
	}
	cancel()
	wg.Wait()
	close(provider.firstGate)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := e.WaitForIdle(waitCtx, id); err != nil {
		t.Fatal(err)
	}
}

func TestQueueBaselineOverWebSocket(t *testing.T) {
	e := attachmentEngine(t, false)
	provider := newQueuedTestProvider()
	e.RegisterProvider(provider)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "queue-transport", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Prompt(context.Background(), id, PromptRequest{SessionID: id, Content: []PromptContentPart{{Type: "text", Text: "active"}}}); err != nil {
		t.Fatal(err)
	}
	<-provider.started
	if _, err := e.Prompt(context.Background(), id, PromptRequest{SessionID: id, Content: []PromptContentPart{{Type: "text", Text: "queued"}}}); err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	s.mu.Lock()
	queuedID := s.pending[0].id
	s.mu.Unlock()

	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	if _, err := fmt.Fprintf(conn, "GET /api/events.mux HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n\r\n", server.Listener.Addr().String(), key); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	response, err := http.ReadResponse(br, nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("WS upgrade status=%v err=%v", response.Status, err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	found := false
	for i := 0; i < 32; i++ {
		envelope, err := readServerWSEnvelope(br)
		if err != nil {
			t.Fatal(err)
		}
		if envelope["method"] != "session/queue" {
			continue
		}
		payload, _ := envelope["payload"].(map[string]any)
		items, _ := payload["items"].([]any)
		for _, raw := range items {
			item, _ := raw.(map[string]any)
			if item["id"] == queuedID {
				found = true
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Fatalf("WS queue baseline did not contain %s", queuedID)
	}
	close(provider.firstGate)
}
