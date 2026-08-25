package harness

// DeepSeek Files API support is deliberately kept below the provider adapter:
// durable attachment objects remain provider-independent, while this package
// owns endpoint/key scoping, upload reuse, expiry refresh, and recovery.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DeepSeekMinFileExpirySeconds = 3600
	DeepSeekMaxFileExpirySeconds = 2_592_000
	DeepSeekMaxFileUploadBytes   = 128 << 20
	DeepSeekMaxStoredFileCount   = 10_000
	DeepSeekMaxStoredFileBytes   = 25 * 1024 * 1024 * 1024
	DeepSeekMaxChatImageBytes    = 32 * 1024 * 1024
	DeepSeekMaxRequestImages     = 600
	DeepSeekRequestImagePixels   = 640_000
	DeepSeekRequestImageBytes    = 1 << 20
	DeepSeekFileExpirySeconds    = 7 * 24 * 60 * 60
	DeepSeekFileRefreshSeconds   = 60 * 60
	DeepSeekQuotaCleanupBatch    = 100
	deepSeekMaxSafeInteger       = int64(1<<53 - 1)
)

var deepSeekQuotaPattern = regexp.MustCompile(`(?i)(quota|storage|stored files|file count|too many files)`)

const deepSeekHarnessUserAgent = "deepseek-harness/0.1.1-rc.2 (+https://github.com/deepseek-ai/deepseek-harness)"

type DeepSeekFileObject struct {
	ID           string `json:"id"`
	Bytes        int    `json:"bytes"`
	CreatedAt    int64  `json:"created_at"`
	Filename     string `json:"filename"`
	Purpose      string `json:"purpose"`
	ExpiresAt    int64  `json:"expires_at,omitempty"`
	ExpiresAtSet bool   `json:"-"`
}

type DeepSeekFilePage struct {
	Data    []DeepSeekFileObject
	FirstID string
	LastID  string
	HasMore bool
}

type DeepSeekFilesError struct {
	Status  int
	Detail  string
	Message string
}

func (e *DeepSeekFilesError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("DeepSeek Files API error (HTTP %d)", e.Status)
}

func isDeepSeekFilesQuotaError(err error) bool {
	var filesErr *DeepSeekFilesError
	return errors.As(err, &filesErr) && deepSeekQuotaPattern.MatchString(filesErr.Detail)
}

func parseDeepSeekFileObject(raw json.RawMessage, operation string) (DeepSeekFileObject, error) {
	var value struct {
		ID        string          `json:"id"`
		Object    string          `json:"object"`
		Bytes     int64           `json:"bytes"`
		CreatedAt int64           `json:"created_at"`
		Filename  string          `json:"filename"`
		Purpose   string          `json:"purpose"`
		ExpiresAt json.RawMessage `json:"expires_at"`
	}
	if err := json.Unmarshal(raw, &value); err != nil || value.ID == "" || value.Object != "file" || value.Bytes < 0 || value.CreatedAt < 0 || value.Filename == "" || value.Purpose != "user_data" {
		return DeepSeekFileObject{}, fmt.Errorf("DeepSeek Files API returned an invalid %s response", operation)
	}
	if value.Bytes > deepSeekMaxSafeInteger || value.CreatedAt > deepSeekMaxSafeInteger {
		return DeepSeekFileObject{}, fmt.Errorf("DeepSeek Files API returned an invalid %s response", operation)
	}
	result := DeepSeekFileObject{ID: value.ID, Bytes: int(value.Bytes), CreatedAt: value.CreatedAt, Filename: value.Filename, Purpose: value.Purpose}
	if len(value.ExpiresAt) > 0 {
		var expiresAt int64
		if string(value.ExpiresAt) == "null" || json.Unmarshal(value.ExpiresAt, &expiresAt) != nil || expiresAt < 0 || expiresAt > deepSeekMaxSafeInteger {
			return DeepSeekFileObject{}, fmt.Errorf("DeepSeek Files API returned an invalid %s response", operation)
		}
		result.ExpiresAt = expiresAt
		result.ExpiresAtSet = true
	}
	return result, nil
}

type DeepSeekFilesClient struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

func NewDeepSeekFilesClient(baseURL, apiKey string, client *http.Client) *DeepSeekFilesClient {
	if client == nil {
		client = &http.Client{}
	}
	return &DeepSeekFilesClient{BaseURL: strings.TrimRight(baseURL, "/"), APIKey: apiKey, HTTPClient: client}
}

func (c *DeepSeekFilesClient) request(ctx context.Context, method, path string, body io.Reader, contentType string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	// Match the upstream client: the bearer header is always present, while
	// managed provider admission rejects missing credentials before transport.
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("User-Agent", deepSeekHarnessUserAgent)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("DeepSeek Files API request to %s failed: %w", c.BaseURL, err)
	}
	if resp.StatusCode/100 == 2 {
		return resp, nil
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	detail := strings.TrimSpace(string(data))
	var payload struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &payload) == nil {
		if payload.Error.Message != "" {
			detail = strings.TrimSpace(strings.Join([]string{payload.Error.Code, payload.Error.Type, payload.Error.Message}, " "))
		}
	}
	return nil, &DeepSeekFilesError{Status: resp.StatusCode, Detail: detail, Message: fmt.Sprintf("DeepSeek Files API error (HTTP %d): %s", resp.StatusCode, detail)}
}

func (c *DeepSeekFilesClient) Upload(ctx context.Context, data []byte, mediaType, filename string, expiresAfterSeconds int) (DeepSeekFileObject, error) {
	if len(data) > DeepSeekMaxFileUploadBytes {
		return DeepSeekFileObject{}, errors.New("DeepSeek Files API upload exceeds 128 MiB")
	}
	if expiresAfterSeconds < DeepSeekMinFileExpirySeconds || expiresAfterSeconds > DeepSeekMaxFileExpirySeconds {
		return DeepSeekFileObject{}, errors.New("DeepSeek file expiry must be between 3600 and 2592000 seconds")
	}
	if mediaType != "image/png" && mediaType != "image/jpeg" && mediaType != "image/webp" && mediaType != "image/gif" {
		return DeepSeekFileObject{}, errors.New("DeepSeek Files API upload requires a supported image media type")
	}
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	_ = form.WriteField("purpose", "user_data")
	_ = form.WriteField("expires_after[anchor]", "created_at")
	_ = form.WriteField("expires_after[seconds]", strconv.Itoa(expiresAfterSeconds))
	contentDisposition := mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": filename})
	part, err := form.CreatePart(textproto.MIMEHeader{
		"Content-Disposition": []string{contentDisposition},
		"Content-Type":        []string{mediaType},
	})
	if err != nil {
		return DeepSeekFileObject{}, err
	}
	if _, err := part.Write(data); err != nil {
		return DeepSeekFileObject{}, err
	}
	if err := form.Close(); err != nil {
		return DeepSeekFileObject{}, err
	}
	resp, err := c.request(ctx, http.MethodPost, "/files", &body, form.FormDataContentType())
	if err != nil {
		return DeepSeekFileObject{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return DeepSeekFileObject{}, err
	}
	file, err := parseDeepSeekFileObject(raw, "upload")
	if err != nil || !file.ExpiresAtSet {
		if err == nil {
			err = errors.New("DeepSeek Files API upload response omitted expires_at")
		}
		return DeepSeekFileObject{}, err
	}
	return file, nil
}

func (c *DeepSeekFilesClient) List(ctx context.Context, after string, limit int, order string) (DeepSeekFilePage, error) {
	query := url.Values{"purpose": {"user_data"}}
	if after != "" {
		query.Set("after", after)
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if order != "" {
		query.Set("order", order)
	}
	resp, err := c.request(ctx, http.MethodGet, "/files?"+query.Encode(), nil, "")
	if err != nil {
		return DeepSeekFilePage{}, err
	}
	defer resp.Body.Close()
	var wire struct {
		Object  string          `json:"object"`
		Data    json.RawMessage `json:"data"`
		FirstID json.RawMessage `json:"first_id"`
		LastID  json.RawMessage `json:"last_id"`
		HasMore *bool           `json:"has_more"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&wire); err != nil || wire.Object != "list" || len(wire.Data) == 0 || string(wire.Data) == "null" || wire.HasMore == nil {
		return DeepSeekFilePage{}, errors.New("DeepSeek Files API returned an invalid list response")
	}
	var rawData []json.RawMessage
	if json.Unmarshal(wire.Data, &rawData) != nil || rawData == nil {
		return DeepSeekFilePage{}, errors.New("DeepSeek Files API returned an invalid list response")
	}
	parseCursor := func(raw json.RawMessage) (string, error) {
		if len(raw) == 0 {
			return "", nil
		}
		var cursor string
		if string(raw) == "null" || json.Unmarshal(raw, &cursor) != nil {
			return "", errors.New("DeepSeek Files API returned an invalid list response")
		}
		return cursor, nil
	}
	firstID, err := parseCursor(wire.FirstID)
	if err != nil {
		return DeepSeekFilePage{}, err
	}
	lastID, err := parseCursor(wire.LastID)
	if err != nil {
		return DeepSeekFilePage{}, err
	}
	page := DeepSeekFilePage{FirstID: firstID, LastID: lastID, HasMore: *wire.HasMore, Data: make([]DeepSeekFileObject, 0, len(rawData))}
	for _, raw := range rawData {
		file, err := parseDeepSeekFileObject(raw, "list")
		if err != nil {
			return DeepSeekFilePage{}, err
		}
		page.Data = append(page.Data, file)
	}
	return page, nil
}

func (c *DeepSeekFilesClient) Retrieve(ctx context.Context, id string) (DeepSeekFileObject, error) {
	resp, err := c.request(ctx, http.MethodGet, "/files/"+url.PathEscape(id), nil, "")
	if err != nil {
		return DeepSeekFileObject{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return DeepSeekFileObject{}, err
	}
	return parseDeepSeekFileObject(raw, "retrieve")
}

func (c *DeepSeekFilesClient) Delete(ctx context.Context, id string) error {
	resp, err := c.request(ctx, http.MethodDelete, "/files/"+url.PathEscape(id), nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var wire struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Deleted bool   `json:"deleted"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&wire); err != nil || wire.Object != "file" || wire.ID != id || !wire.Deleted {
		return errors.New("DeepSeek Files API returned an invalid delete response")
	}
	return nil
}

type DeepSeekUploadRecord struct {
	Scope        string `json:"scope"`
	AttachmentID string `json:"attachmentId"`
	VariantID    string `json:"variantId"`
	FileID       string `json:"fileId"`
	Bytes        int    `json:"bytes"`
	CreatedAt    int64  `json:"createdAt"`
	ExpiresAt    int64  `json:"expiresAt"`
}

type DeepSeekUploadIndex struct {
	Path string
}

type deepSeekUploadIndexFile struct {
	FormatVersion int                    `json:"formatVersion"`
	Records       []DeepSeekUploadRecord `json:"records"`
}

func NewDeepSeekUploadIndex(path string) *DeepSeekUploadIndex {
	return &DeepSeekUploadIndex{Path: path}
}

func deepSeekFileScope(baseURL, apiKey string) string {
	hash := sha256.Sum256([]byte(strings.TrimRight(baseURL, "/") + "\x00" + apiKey))
	return hex.EncodeToString(hash[:])
}

func (i *DeepSeekUploadIndex) load() (deepSeekUploadIndexFile, error) {
	data, err := os.ReadFile(i.Path)
	if errors.Is(err, os.ErrNotExist) {
		return deepSeekUploadIndexFile{FormatVersion: 3, Records: []DeepSeekUploadRecord{}}, nil
	}
	if err != nil {
		return deepSeekUploadIndexFile{}, err
	}
	var envelope struct {
		FormatVersion int             `json:"formatVersion"`
		Records       json.RawMessage `json:"records"`
	}
	if json.Unmarshal(data, &envelope) != nil || envelope.FormatVersion != 3 || len(envelope.Records) == 0 || string(envelope.Records) == "null" {
		return deepSeekUploadIndexFile{FormatVersion: 3, Records: []DeepSeekUploadRecord{}}, nil
	}
	var records []DeepSeekUploadRecord
	if json.Unmarshal(envelope.Records, &records) != nil || records == nil {
		return deepSeekUploadIndexFile{FormatVersion: 3, Records: []DeepSeekUploadRecord{}}, nil
	}
	value := deepSeekUploadIndexFile{FormatVersion: envelope.FormatVersion, Records: records}
	seen := map[string]bool{}
	for _, record := range value.Records {
		key := record.Scope + "\x00" + record.VariantID
		if !deepSeekValidUploadRecord(record) || seen[key] {
			// The upstream cache treats any malformed or duplicate record as a
			// corrupt index, rather than salvaging an ambiguous subset.
			return deepSeekUploadIndexFile{FormatVersion: 3, Records: []DeepSeekUploadRecord{}}, nil
		}
		seen[key] = true
	}
	return value, nil
}

var deepSeekHex64Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func deepSeekValidUploadRecord(record DeepSeekUploadRecord) bool {
	return deepSeekHex64Pattern.MatchString(record.Scope) &&
		strings.HasPrefix(record.AttachmentID, "sha256:") &&
		deepSeekHex64Pattern.MatchString(strings.TrimPrefix(record.AttachmentID, "sha256:")) &&
		strings.HasPrefix(record.VariantID, "sha256:") &&
		deepSeekHex64Pattern.MatchString(strings.TrimPrefix(record.VariantID, "sha256:")) &&
		record.FileID != "" && record.Bytes >= 0 && int64(record.Bytes) <= deepSeekMaxSafeInteger && record.CreatedAt >= 0 && record.CreatedAt <= deepSeekMaxSafeInteger && record.ExpiresAt >= 0 && record.ExpiresAt <= deepSeekMaxSafeInteger
}

func (i *DeepSeekUploadIndex) save(value deepSeekUploadIndexFile) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeOwnerOnlyFile(i.Path, data)
}

func deepSeekRecordReusable(record DeepSeekUploadRecord, now, margin int64) bool {
	return record.ExpiresAt-now > margin
}

func (i *DeepSeekUploadIndex) Get(scope, variantID string, now, margin int64) (DeepSeekUploadRecord, bool, error) {
	value, err := i.load()
	if err != nil {
		return DeepSeekUploadRecord{}, false, err
	}
	for _, record := range value.Records {
		if record.Scope == scope && record.VariantID == variantID && deepSeekRecordReusable(record, now, margin) {
			return record, true, nil
		}
	}
	return DeepSeekUploadRecord{}, false, nil
}

func (i *DeepSeekUploadIndex) Commit(candidate DeepSeekUploadRecord, now, margin int64) (DeepSeekUploadRecord, bool, error) {
	var winner DeepSeekUploadRecord
	accepted := false
	err := withOwnerFileLock(i.Path, func() error {
		value, err := i.load()
		if err != nil {
			return err
		}
		for _, record := range value.Records {
			if record.Scope == candidate.Scope && record.VariantID == candidate.VariantID && deepSeekRecordReusable(record, now, margin) {
				winner = record
				return nil
			}
		}
		filtered := value.Records[:0]
		for _, record := range value.Records {
			if deepSeekRecordReusable(record, now, margin) && !(record.Scope == candidate.Scope && record.VariantID == candidate.VariantID) {
				filtered = append(filtered, record)
			}
		}
		filtered = append(filtered, candidate)
		value.Records = filtered
		if err := i.save(value); err != nil {
			return err
		}
		winner, accepted = candidate, true
		return nil
	})
	return winner, accepted, err
}

func (i *DeepSeekUploadIndex) Remove(scope, variantID, fileID string) error {
	return withOwnerFileLock(i.Path, func() error {
		value, err := i.load()
		if err != nil {
			return err
		}
		filtered := value.Records[:0]
		for _, record := range value.Records {
			if record.Scope == scope && record.VariantID == variantID && record.FileID == fileID {
				continue
			}
			filtered = append(filtered, record)
		}
		if len(filtered) != len(value.Records) {
			value.Records = filtered
			return i.save(value)
		}
		return nil
	})
}

func (i *DeepSeekUploadIndex) Clear(scope string) error {
	return withOwnerFileLock(i.Path, func() error {
		value, err := i.load()
		if err != nil {
			return err
		}
		filtered := value.Records[:0]
		for _, record := range value.Records {
			if record.Scope != scope {
				filtered = append(filtered, record)
			}
		}
		if len(filtered) != len(value.Records) {
			value.Records = filtered
			return i.save(value)
		}
		return nil
	})
}

type DeepSeekFilePolicy struct {
	ExpiresAfterSeconds int
	RefreshMargin       time.Duration
	QuotaCleanupBatch   int
	APITimeout          time.Duration
}

type DeepSeekFileConnection struct {
	BaseURL string
	APIKey  string
}

type DeepSeekFileReference struct {
	Record   DeepSeekUploadRecord
	Uploaded bool
}

type deepSeekUploadCall struct {
	done    chan struct{}
	result  DeepSeekFileReference
	err     error
	ctx     context.Context
	cancel  context.CancelFunc
	waiters int
	settled bool
}

type DeepSeekFileStore struct {
	Index      *DeepSeekUploadIndex
	HTTPClient *http.Client
	now        func() time.Time
	mu         sync.Mutex
	inflight   map[string]*deepSeekUploadCall
}

func NewDeepSeekFileStore(index *DeepSeekUploadIndex, client *http.Client) *DeepSeekFileStore {
	if index == nil {
		index = NewDeepSeekUploadIndex(filepath.Join(".", "llm-deepseek", "files-v3.json"))
	}
	if client == nil {
		client = &http.Client{}
	}
	return &DeepSeekFileStore{Index: index, HTTPClient: client, now: time.Now, inflight: map[string]*deepSeekUploadCall{}}
}

func deepSeekImageExtension(mediaType string) string {
	switch mediaType {
	case "image/png":
		return "png"
	case "image/jpeg":
		return "jpeg"
	case "image/webp":
		return "webp"
	default:
		return "gif"
	}
}

func deepSeekUploadFilename(version RequestImageAttachment) string {
	attachment := strings.TrimPrefix(version.Attachment.AttachmentID, "sha256:")
	variant := strings.TrimPrefix(version.VariantID, "sha256:")
	if len(attachment) > 16 {
		attachment = attachment[:16]
	}
	if len(variant) > 8 {
		variant = variant[:8]
	}
	return fmt.Sprintf("dsh-%s-%s.%s", attachment, variant, deepSeekImageExtension(version.MediaType))
}

func (s *DeepSeekFileStore) EnsureUploaded(ctx context.Context, version RequestImageAttachment, connection DeepSeekFileConnection, policy DeepSeekFilePolicy) (DeepSeekFileReference, error) {
	if err := ctx.Err(); err != nil {
		return DeepSeekFileReference{}, err
	}
	if version.Bytes > DeepSeekMaxChatImageBytes {
		return DeepSeekFileReference{}, errors.New("DeepSeek chat image exceeds the 32 MiB per-image limit")
	}
	scope := deepSeekFileScope(connection.BaseURL, connection.APIKey)
	key := scope + "\x00" + version.VariantID
	s.mu.Lock()
	call := s.inflight[key]
	if call != nil && call.ctx.Err() != nil {
		// A timed-out shared controller may still be unwinding. Do not attach
		// a new waiter to its already-cancelled operation.
		delete(s.inflight, key)
		call = nil
	}
	if call == nil {
		sharedCtx, cancel := context.WithCancel(context.Background())
		call = &deepSeekUploadCall{done: make(chan struct{}), ctx: sharedCtx, cancel: cancel, waiters: 0}
		s.inflight[key] = call
		go s.runUpload(call, key, version, connection, policy)
	}
	call.waiters++
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		s.releaseCall(key, call, ctx.Err())
		return DeepSeekFileReference{}, ctx.Err()
	case <-call.done:
		s.releaseCall(key, call, nil)
		return call.result, call.err
	}
}

func (s *DeepSeekFileStore) releaseCall(key string, call *deepSeekUploadCall, _ error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if call.waiters > 0 {
		call.waiters--
	}
	if call.waiters == 0 && !call.settled {
		if s.inflight[key] == call {
			// Do not let a cancelled controller capture a later caller. The
			// worker may still be unwinding, but a new request must create a
			// fresh shared upload operation.
			delete(s.inflight, key)
		}
		call.cancel()
	}
	if call.settled && s.inflight[key] == call {
		delete(s.inflight, key)
	}
}

func (s *DeepSeekFileStore) runUpload(call *deepSeekUploadCall, key string, version RequestImageAttachment, connection DeepSeekFileConnection, policy DeepSeekFilePolicy) {
	call.result, call.err = s.ensureUploadedOnce(call.ctx, version, connection, policy)
	s.mu.Lock()
	call.settled = true
	if s.inflight[key] == call {
		delete(s.inflight, key)
	}
	s.mu.Unlock()
	close(call.done)
}

func (s *DeepSeekFileStore) ensureUploadedOnce(ctx context.Context, version RequestImageAttachment, connection DeepSeekFileConnection, policy DeepSeekFilePolicy) (DeepSeekFileReference, error) {
	if policy.APITimeout <= 0 {
		policy.APITimeout = time.Minute
	}
	operationCtx, cancel := context.WithTimeout(ctx, policy.APITimeout)
	defer cancel()
	ctx = operationCtx
	margin := policy.RefreshMargin
	now := s.now().UnixMilli()
	scope := deepSeekFileScope(connection.BaseURL, connection.APIKey)
	if cached, ok, err := s.Index.Get(scope, version.VariantID, now, margin.Milliseconds()); err != nil {
		return DeepSeekFileReference{}, err
	} else if ok {
		return DeepSeekFileReference{Record: cached}, nil
	}
	client := NewDeepSeekFilesClient(connection.BaseURL, connection.APIKey, s.HTTPClient)
	upload := func() (DeepSeekUploadRecord, error) {
		remote, err := client.Upload(ctx, version.Data, version.MediaType, deepSeekUploadFilename(version), policy.ExpiresAfterSeconds)
		if err != nil {
			return DeepSeekUploadRecord{}, err
		}
		if remote.Bytes != len(version.Data) {
			return DeepSeekUploadRecord{}, errors.New("DeepSeek Files API upload response does not match the submitted image")
		}
		return DeepSeekUploadRecord{Scope: scope, AttachmentID: version.Attachment.AttachmentID, VariantID: version.VariantID, FileID: remote.ID, Bytes: remote.Bytes, CreatedAt: remote.CreatedAt * 1000, ExpiresAt: remote.ExpiresAt * 1000}, nil
	}
	candidate, err := upload()
	if err != nil && isDeepSeekFilesQuotaError(err) {
		deleted, reclaimErr := s.ReclaimOldestOwned(ctx, connection, policy.QuotaCleanupBatch)
		if reclaimErr != nil {
			return DeepSeekFileReference{}, reclaimErr
		}
		if deleted > 0 {
			candidate, err = upload()
		}
	}
	if err != nil {
		return DeepSeekFileReference{}, err
	}
	winner, accepted, err := s.Index.Commit(candidate, s.now().UnixMilli(), margin.Milliseconds())
	if err != nil {
		return DeepSeekFileReference{}, err
	}
	if !accepted {
		_ = client.Delete(ctx, candidate.FileID)
	}
	return DeepSeekFileReference{Record: winner, Uploaded: accepted}, nil
}

func (s *DeepSeekFileStore) Invalidate(version RequestImageAttachment, fileID string, connection DeepSeekFileConnection) error {
	return s.Index.Remove(deepSeekFileScope(connection.BaseURL, connection.APIKey), version.VariantID, fileID)
}

// Release deletes one indexed remote file and removes only its exact local mapping.
func (s *DeepSeekFileStore) Release(ctx context.Context, version RequestImageAttachment, connection DeepSeekFileConnection, policy DeepSeekFilePolicy) (bool, error) {
	scope := deepSeekFileScope(connection.BaseURL, connection.APIKey)
	margin := policy.RefreshMargin.Milliseconds()
	record, ok, err := s.Index.Get(scope, version.VariantID, s.now().UnixMilli(), margin)
	if err != nil || !ok {
		return false, err
	}
	if err := NewDeepSeekFilesClient(connection.BaseURL, connection.APIKey, s.HTTPClient).Delete(ctx, record.FileID); err != nil {
		return false, err
	}
	if err := s.Index.Remove(scope, version.VariantID, record.FileID); err != nil {
		return false, err
	}
	return true, nil
}

func (s *DeepSeekFileStore) ReclaimOldestOwned(ctx context.Context, connection DeepSeekFileConnection, count int) (int, error) {
	if count <= 0 {
		return 0, nil
	}
	client := NewDeepSeekFilesClient(connection.BaseURL, connection.APIKey, s.HTTPClient)
	owned := make([]DeepSeekFileObject, 0, count)
	after := ""
	for len(owned) < count {
		page, err := client.List(ctx, after, 1000, "asc")
		if err != nil {
			return 0, err
		}
		for _, file := range page.Data {
			if strings.HasPrefix(file.Filename, "dsh-") {
				owned = append(owned, file)
				if len(owned) == count {
					break
				}
			}
		}
		if !page.HasMore || page.LastID == "" || page.LastID == after {
			break
		}
		after = page.LastID
	}
	for _, file := range owned {
		if err := client.Delete(ctx, file.ID); err != nil {
			return 0, err
		}
	}
	return len(owned), nil
}

func (s *DeepSeekFileStore) ReleaseAll(ctx context.Context, connection DeepSeekFileConnection) (int, error) {
	total := 0
	for {
		deleted, err := s.ReclaimOldestOwned(ctx, connection, 1000)
		if err != nil {
			return total, err
		}
		total += deleted
		if deleted < 1000 {
			break
		}
	}
	if err := s.Index.Clear(deepSeekFileScope(connection.BaseURL, connection.APIKey)); err != nil {
		return total, err
	}
	return total, nil
}

func deepSeekSortRecords(records []DeepSeekUploadRecord) {
	sort.Slice(records, func(i, j int) bool { return records[i].CreatedAt < records[j].CreatedAt })
}
