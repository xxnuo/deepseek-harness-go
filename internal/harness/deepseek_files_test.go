package harness

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeepSeekFilesClientAndStoreReuseUploadIndex(t *testing.T) {
	var mu sync.Mutex
	uploads := 0
	deletes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/files":
			if got := r.Header.Get("Authorization"); got != "Bearer secret-key" {
				t.Fatalf("authorization = %q", got)
			}
			reader, err := r.MultipartReader()
			if err != nil {
				t.Fatal(err)
			}
			var imageBytes int
			for {
				part, nextErr := reader.NextPart()
				if nextErr == io.EOF {
					break
				}
				if nextErr != nil {
					t.Fatal(nextErr)
				}
				data, readErr := io.ReadAll(part)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if part.FormName() == "file" {
					if got := part.Header.Get("Content-Type"); got != "image/png" {
						t.Fatalf("uploaded MIME = %q", got)
					}
					imageBytes = len(data)
				}
			}
			mu.Lock()
			uploads++
			id := "file-" + string(rune('0'+uploads))
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": id, "object": "file", "bytes": imageBytes, "created_at": time.Now().Unix(),
				"filename": "dsh-test.png", "purpose": "user_data", "expires_at": time.Now().Add(7 * 24 * time.Hour).Unix(),
			})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/files/"):
			mu.Lock()
			deletes++
			mu.Unlock()
			id := strings.TrimPrefix(r.URL.Path, "/files/")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "object": "file", "deleted": true})
		case r.Method == http.MethodGet && r.URL.Path == "/files":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": []any{}, "has_more": false})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	index := NewDeepSeekUploadIndex(filepath.Join(t.TempDir(), "llm-deepseek", "files-v3.json"))
	store := NewDeepSeekFileStore(index, server.Client())
	version := RequestImageAttachment{
		VariantID:  "sha256:" + strings.Repeat("b", 64),
		Attachment: ImageAttachmentRef{AttachmentID: "sha256:" + strings.Repeat("a", 64), MediaType: "image/png", Bytes: 3, Width: 1, Height: 1},
		Data:       []byte{1, 2, 3}, MediaType: "image/png", Bytes: 3, Width: 1, Height: 1, Depth: "uchar", Space: "srgb",
	}
	connection := DeepSeekFileConnection{BaseURL: server.URL, APIKey: "secret-key"}
	policy := DeepSeekFilePolicy{ExpiresAfterSeconds: DeepSeekFileExpirySeconds, RefreshMargin: time.Hour, QuotaCleanupBatch: 100}
	first, err := store.EnsureUploaded(t.Context(), version, connection, policy)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.EnsureUploaded(t.Context(), version, connection, policy)
	if err != nil {
		t.Fatal(err)
	}
	if first.Record.FileID != second.Record.FileID || !first.Uploaded || second.Uploaded {
		t.Fatalf("upload reuse = %#v / %#v", first, second)
	}
	mu.Lock()
	gotUploads, gotDeletes := uploads, deletes
	mu.Unlock()
	if gotUploads != 1 || gotDeletes != 0 {
		t.Fatalf("file calls = uploads %d deletes %d", gotUploads, gotDeletes)
	}
	data, err := os.ReadFile(index.Path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-key") || strings.Contains(string(data), server.URL) {
		t.Fatalf("upload index leaked connection secret: %s", data)
	}
	if err := store.Invalidate(version, first.Record.FileID, connection); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := index.Get(deepSeekFileScope(connection.BaseURL, connection.APIKey), version.VariantID, time.Now().UnixMilli(), time.Hour.Milliseconds()); err != nil || ok {
		t.Fatalf("invalidated mapping = ok %v err %v", ok, err)
	}
}

func TestDeepSeekUploadIndexKeepsScopeAndVariantMappingsSeparate(t *testing.T) {
	index := NewDeepSeekUploadIndex(filepath.Join(t.TempDir(), "files-v3.json"))
	now := time.Now().UnixMilli()
	base := DeepSeekUploadRecord{Scope: strings.Repeat("a", 64), AttachmentID: "sha256:" + strings.Repeat("a", 64), VariantID: "sha256:" + strings.Repeat("b", 64), FileID: "file-a", Bytes: 1, CreatedAt: now, ExpiresAt: now + 2*time.Hour.Milliseconds()}
	if _, accepted, err := index.Commit(base, now, time.Hour.Milliseconds()); err != nil || !accepted {
		t.Fatalf("first commit = accepted %v err %v", accepted, err)
	}
	other := base
	other.Scope = strings.Repeat("c", 64)
	other.FileID = "file-b"
	if _, accepted, err := index.Commit(other, now, time.Hour.Milliseconds()); err != nil || !accepted {
		t.Fatalf("second scope commit = accepted %v err %v", accepted, err)
	}
	if got, ok, err := index.Get(strings.Repeat("a", 64), base.VariantID, now, time.Hour.Milliseconds()); err != nil || !ok || got.FileID != "file-a" {
		t.Fatalf("scope-a mapping = %#v ok %v err %v", got, ok, err)
	}
	if got, ok, err := index.Get(strings.Repeat("c", 64), base.VariantID, now, time.Hour.Milliseconds()); err != nil || !ok || got.FileID != "file-b" {
		t.Fatalf("scope-b mapping = %#v ok %v err %v", got, ok, err)
	}
}

func TestDeepSeekFileStoreRefreshesExpiredMapping(t *testing.T) {
	var uploads int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/files" {
			http.NotFound(w, r)
			return
		}
		uploads++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "file-refreshed", "object": "file", "bytes": 3, "created_at": 2,
			"filename": "dsh-refresh.png", "purpose": "user_data", "expires_at": 7_200,
		})
	}))
	defer server.Close()
	now := int64(1_000_000)
	index := NewDeepSeekUploadIndex(filepath.Join(t.TempDir(), "files-v3.json"))
	version := RequestImageAttachment{
		VariantID:  "sha256:" + strings.Repeat("b", 64),
		Attachment: ImageAttachmentRef{AttachmentID: "sha256:" + strings.Repeat("a", 64), MediaType: "image/png", Bytes: 3, Width: 1, Height: 1},
		Data:       []byte{1, 2, 3}, MediaType: "image/png", Bytes: 3, Width: 1, Height: 1,
	}
	connection := DeepSeekFileConnection{BaseURL: server.URL, APIKey: "key"}
	scope := deepSeekFileScope(connection.BaseURL, connection.APIKey)
	if _, accepted, err := index.Commit(DeepSeekUploadRecord{Scope: scope, AttachmentID: version.Attachment.AttachmentID, VariantID: version.VariantID, FileID: "file-expired", Bytes: 3, CreatedAt: now - 10_000, ExpiresAt: now - 1}, now, time.Hour.Milliseconds()); err != nil || !accepted {
		t.Fatalf("expired mapping commit = accepted %v err %v", accepted, err)
	}
	store := NewDeepSeekFileStore(index, server.Client())
	store.now = func() time.Time { return time.UnixMilli(now) }
	got, err := store.EnsureUploaded(t.Context(), version, connection, DeepSeekFilePolicy{ExpiresAfterSeconds: 3_600, RefreshMargin: time.Hour, QuotaCleanupBatch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got.Record.FileID != "file-refreshed" || !got.Uploaded || uploads != 1 {
		t.Fatalf("refresh result = %#v uploads=%d", got, uploads)
	}
}

func TestDeepSeekFileStoreQuotaCleanupRetriesUpload(t *testing.T) {
	var uploads, deletes int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/files":
			uploads++
			if uploads == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":{"message":"stored files quota exceeded"}}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "file-after-quota", "object": "file", "bytes": 3, "created_at": 2, "filename": "dsh-new.png", "purpose": "user_data", "expires_at": 7_200})
		case r.Method == http.MethodGet && r.URL.Path == "/files":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": []any{map[string]any{"id": "file-old", "object": "file", "bytes": 3, "created_at": 1, "filename": "dsh-old.png", "purpose": "user_data", "expires_at": 7_200}}, "has_more": false})
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/file-old"):
			deletes++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "file-old", "object": "file", "deleted": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	version := RequestImageAttachment{VariantID: "sha256:" + strings.Repeat("c", 64), Attachment: ImageAttachmentRef{AttachmentID: "sha256:" + strings.Repeat("d", 64), MediaType: "image/png", Bytes: 3, Width: 1, Height: 1}, Data: []byte{1, 2, 3}, MediaType: "image/png", Bytes: 3, Width: 1, Height: 1}
	store := NewDeepSeekFileStore(NewDeepSeekUploadIndex(filepath.Join(t.TempDir(), "files-v3.json")), server.Client())
	got, err := store.EnsureUploaded(t.Context(), version, DeepSeekFileConnection{BaseURL: server.URL, APIKey: "key"}, DeepSeekFilePolicy{ExpiresAfterSeconds: 3_600, RefreshMargin: time.Hour, QuotaCleanupBatch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got.Record.FileID != "file-after-quota" || uploads != 2 || deletes != 1 {
		t.Fatalf("quota recovery = %#v uploads=%d deletes=%d", got, uploads, deletes)
	}
}

func TestDeepSeekFileStorePropagatesQuotaCleanupFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/files":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"stored files quota exceeded"}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/files":
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"error":{"message":"quota listing unavailable"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	version := RequestImageAttachment{VariantID: "sha256:" + strings.Repeat("e", 64), Attachment: ImageAttachmentRef{AttachmentID: "sha256:" + strings.Repeat("f", 64), MediaType: "image/png", Bytes: 3, Width: 1, Height: 1}, Data: []byte{1, 2, 3}, MediaType: "image/png", Bytes: 3, Width: 1, Height: 1}
	store := NewDeepSeekFileStore(NewDeepSeekUploadIndex(filepath.Join(t.TempDir(), "files-v3.json")), server.Client())
	_, err := store.EnsureUploaded(t.Context(), version, DeepSeekFileConnection{BaseURL: server.URL, APIKey: "key"}, DeepSeekFilePolicy{ExpiresAfterSeconds: 3_600, RefreshMargin: time.Hour, QuotaCleanupBatch: 1})
	if err == nil || !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("quota cleanup error = %v", err)
	}
}

func TestDeepSeekFileStoreCancelledSharedUploadDoesNotCaptureNextCaller(t *testing.T) {
	var uploads atomic.Int32
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/files" {
			http.NotFound(w, r)
			return
		}
		if uploads.Add(1) == 1 {
			close(started)
			select {
			case <-r.Context().Done():
			case <-time.After(500 * time.Millisecond):
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "file-second", "object": "file", "bytes": 3, "created_at": time.Now().Unix(), "filename": "dsh-second.png", "purpose": "user_data", "expires_at": time.Now().Add(2 * time.Hour).Unix()})
	}))
	defer server.Close()
	version := RequestImageAttachment{VariantID: "sha256:" + strings.Repeat("1", 64), Attachment: ImageAttachmentRef{AttachmentID: "sha256:" + strings.Repeat("2", 64), MediaType: "image/png", Bytes: 3, Width: 1, Height: 1}, Data: []byte{1, 2, 3}, MediaType: "image/png", Bytes: 3, Width: 1, Height: 1}
	store := NewDeepSeekFileStore(NewDeepSeekUploadIndex(filepath.Join(t.TempDir(), "files-v3.json")), server.Client())
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstDone := make(chan error, 1)
	go func() {
		_, err := store.EnsureUploaded(firstCtx, version, DeepSeekFileConnection{BaseURL: server.URL, APIKey: "key"}, DeepSeekFilePolicy{ExpiresAfterSeconds: 3_600, RefreshMargin: time.Hour})
		firstDone <- err
	}()
	<-started
	cancelFirst()
	if err := <-firstDone; err == nil {
		t.Fatal("cancelled shared upload unexpectedly succeeded")
	}
	second, err := store.EnsureUploaded(t.Context(), version, DeepSeekFileConnection{BaseURL: server.URL, APIKey: "key"}, DeepSeekFilePolicy{ExpiresAfterSeconds: 3_600, RefreshMargin: time.Hour})
	if err != nil || second.Record.FileID != "file-second" || uploads.Load() != 2 {
		t.Fatalf("next caller after cancellation = %#v uploads=%d err=%v", second, uploads.Load(), err)
	}
}

func TestDeepSeekWirePrefersFileIDAndFallsBackToInline(t *testing.T) {
	compat := openAICompletionsCompat{deepSeekFileIDs: true}
	file := openAIWireMessages([]ChatMessage{{Role: "user", Parts: []ChatContentPart{{Type: "image", MediaType: "image/png", FileID: "file-1"}}}}, compat)
	parts, ok := file[0].Content.([]map[string]any)
	if !ok || len(parts) != 1 || parts[0]["type"] != "file" || parts[0]["file_id"] != "file-1" {
		t.Fatalf("file wire = %#v", file)
	}
	inline := openAIWireMessages([]ChatMessage{{Role: "user", Parts: []ChatContentPart{{Type: "image", MediaType: "image/png", Data: "AQI="}}}}, compat)
	inlineParts, ok := inline[0].Content.([]map[string]any)
	if !ok || inlineParts[0]["type"] != "image_url" {
		t.Fatalf("inline wire = %#v", inline)
	}
}
