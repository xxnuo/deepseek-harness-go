package harness

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"
)

type FileAttachmentRef struct {
	AttachmentID string `json:"attachmentId"`
	Name         string `json:"name"`
	Bytes        int    `json:"bytes"`
}

type stagedFileUpload struct {
	ref       FileAttachmentRef
	requestID string
}

var windowsDeviceFileName = regexp.MustCompile(`(?i)^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?$`)

func fileAttachmentName(value string) string {
	value = strings.ReplaceAll(value, "\\", "/")
	value = filepath.Base(value)
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || strings.ContainsRune(`<>:"|?*`, r) {
			return '_'
		}
		return r
	}, value)
	value = strings.TrimRight(strings.TrimSpace(value), ". ")
	if windowsDeviceFileName.MatchString(value) {
		value = "_" + value
	}
	for len(value) > 255 {
		_, size := utf8.DecodeLastRuneInString(value)
		value = value[:len(value)-size]
	}
	value = strings.TrimRight(value, ". ")
	if value == "" || value == "." || value == ".." {
		return "file"
	}
	return value
}

func (e *Engine) fileAttachmentPath(ref FileAttachmentRef) (string, error) {
	match := attachmentIDPattern.FindStringSubmatch(ref.AttachmentID)
	if match == nil || ref.Name != fileAttachmentName(ref.Name) || ref.Bytes < 0 {
		return "", errors.New("attachment-error: invalid file reference")
	}
	return filepath.Join(e.cfg.DataDir, "attachments", "v1", "files", match[1][:2], match[1], ref.Name), nil
}

func (e *Engine) StoreFile(encoded, name string) (FileAttachmentRef, error) {
	data, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(data) != encoded {
		return FileAttachmentRef{}, errors.New("attachment-error: file upload is not canonical base64")
	}
	digest := sha256.Sum256(data)
	ref := FileAttachmentRef{AttachmentID: "sha256:" + hex.EncodeToString(digest[:]), Name: fileAttachmentName(name), Bytes: len(data)}
	path, _ := e.fileAttachmentPath(ref)
	if err := publishImmutableFile(path, data); err != nil {
		return FileAttachmentRef{}, fmt.Errorf("attachment-error: unable to persist file attachment: %w", err)
	}
	return ref, nil
}

func publishImmutableFile(path string, data []byte) error {
	if existing, err := os.ReadFile(path); err == nil {
		if string(existing) == string(data) {
			return nil
		}
		return errors.New("content-addressed file collision")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".file-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Link(tempPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := os.ReadFile(path)
			if readErr == nil && string(existing) == string(data) {
				return nil
			}
		}
		return err
	}
	return nil
}

func (e *Engine) readFileAttachment(ctx context.Context, ref FileAttachmentRef) ([]byte, error) {
	path, err := e.fileAttachmentPath(ref)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("attachment-error: file attachment object is missing")
	}
	defer file.Close()
	hash := sha256.New()
	data, err := io.ReadAll(io.TeeReader(file, hash))
	if err != nil {
		return nil, errors.New("attachment-error: unable to read file attachment")
	}
	if err := ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}
	if len(data) != ref.Bytes || ref.AttachmentID != "sha256:"+hex.EncodeToString(hash.Sum(nil)) {
		return nil, errors.New("attachment-error: stored file attachment failed integrity verification")
	}
	return data, nil
}

func (e *Engine) remoteFileUpload(ctx context.Context, endpoint string, args map[string]json.RawMessage) (any, *RPCError) {
	id, rpcErr := remoteString(endpoint, args, "agentId")
	if rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr = e.workspaceFileRoot(endpoint, map[string]json.RawMessage{"agentId": args["agentId"]}); rpcErr != nil {
		return nil, rpcErr
	}
	session, sessionErr := e.getSession(id)
	if sessionErr != nil {
		return nil, rpcError("session/not-found", "session is not attached", map[string]any{"sessionId": id})
	}
	session.mu.Lock()
	origin := session.Header.Origin
	session.mu.Unlock()
	if origin == "subagent" {
		return nil, rpcError("subagent/attachment-invalid", "subagent conversations do not accept file uploads", map[string]any{"reason": "SUBAGENT_FILE_UNSUPPORTED"})
	}
	request, err := remoteObject(args["request"], []string{"data", "name"}, []string{"data"})
	if err != nil {
		return nil, remoteBoundaryError(endpoint, "request")
	}
	data, rpcErr := remoteString(endpoint, request, "data")
	if rpcErr != nil {
		return nil, remoteBoundaryError(endpoint, "request")
	}
	name := ""
	if _, ok := request["name"]; ok {
		name, rpcErr = remoteString(endpoint, request, "name")
		if rpcErr != nil {
			return nil, remoteBoundaryError(endpoint, "request")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, rpcError("gateway/internal", err.Error(), map[string]any{})
	}
	ref, storeErr := e.StoreFile(data, name)
	if storeErr != nil {
		return nil, rpcError("session/attachment-invalid", storeErr.Error(), map[string]any{"reason": "INVALID_FILE_BASE64"})
	}
	receipt := newID("file")
	e.fileUploadMu.Lock()
	if e.fileUploads == nil {
		e.fileUploads = map[string]map[string]stagedFileUpload{}
	}
	if e.fileUploads[id] == nil {
		e.fileUploads[id] = map[string]stagedFileUpload{}
	}
	e.fileUploads[id][receipt] = stagedFileUpload{ref: ref}
	e.fileUploadMu.Unlock()
	return map[string]any{"receiptId": receipt, "file": ref}, nil
}

func (e *Engine) resolveFileReceipt(sessionID, receiptID string) (FileAttachmentRef, bool) {
	e.fileUploadMu.Lock()
	defer e.fileUploadMu.Unlock()
	upload, ok := e.fileUploads[sessionID][receiptID]
	return upload.ref, ok
}

type fileReceiptBinding struct {
	engine    *Engine
	sessionID string
	previous  map[string]string
	committed bool
}

func (e *Engine) bindFileReceipts(sessionID string, receiptIDs []string, requestID string) (*fileReceiptBinding, error) {
	e.fileUploadMu.Lock()
	defer e.fileUploadMu.Unlock()
	staged := e.fileUploads[sessionID]
	binding := &fileReceiptBinding{engine: e, sessionID: sessionID, previous: map[string]string{}}
	for _, receiptID := range receiptIDs {
		upload, ok := staged[receiptID]
		if !ok {
			return nil, errors.New("attachment-error: File was not uploaded for this session")
		}
		binding.previous[receiptID] = upload.requestID
		upload.requestID = requestID
		staged[receiptID] = upload
	}
	return binding, nil
}

func (binding *fileReceiptBinding) commit() { binding.committed = true }

func (binding *fileReceiptBinding) close() {
	if binding == nil || binding.committed {
		return
	}
	binding.engine.fileUploadMu.Lock()
	defer binding.engine.fileUploadMu.Unlock()
	staged := binding.engine.fileUploads[binding.sessionID]
	for receiptID, previous := range binding.previous {
		upload, ok := staged[receiptID]
		if !ok {
			continue
		}
		upload.requestID = previous
		staged[receiptID] = upload
	}
}

func (e *Engine) retireFileReceipts(sessionID, requestID string) {
	e.fileUploadMu.Lock()
	defer e.fileUploadMu.Unlock()
	staged := e.fileUploads[sessionID]
	for receiptID, upload := range staged {
		if upload.requestID == requestID {
			delete(staged, receiptID)
		}
	}
	if len(staged) == 0 {
		delete(e.fileUploads, sessionID)
	}
}

func (e *Engine) resolvePromptFiles(sessionID string, parts []PromptContentPart) ([]PromptContentPart, []ContentBlock, []string, error) {
	inline := make([]PromptContentPart, 0, len(parts))
	resolved := make([]ContentBlock, 0, len(parts))
	receiptSet := map[string]bool{}
	receiptIDs := make([]string, 0)
	for _, part := range parts {
		if part.Type != "file" {
			inline = append(inline, part)
			continue
		}
		ref, ok := e.resolveFileReceipt(sessionID, part.ReceiptID)
		if !ok {
			return nil, nil, nil, errors.New("attachment-error: File was not uploaded for this session")
		}
		copy := ref
		resolved = append(resolved, ContentBlock{Type: "file", FileAttachment: &copy})
		if !receiptSet[part.ReceiptID] {
			receiptSet[part.ReceiptID] = true
			receiptIDs = append(receiptIDs, part.ReceiptID)
		}
	}
	return inline, resolved, receiptIDs, nil
}

func fileRequestText(ref FileAttachmentRef, path string) string {
	digest := strings.TrimPrefix(ref.AttachmentID, "sha256:")
	if len(digest) > 8 {
		digest = digest[:8]
	}
	identity := fmt.Sprintf("File %q (%d bytes, sha256:%s)", ref.Name, ref.Bytes, digest)
	if path == "" {
		return fmt.Sprintf("[%s was uploaded, but the current execution environment cannot access a readable path. Report that limitation if its contents are needed; do not claim to have read it.]", identity)
	}
	return fmt.Sprintf("[%s: verbatim read-only copy saved at %q. Read that path with your file tools when its contents are needed; copy it to a writable location before modifying it. When delegating file work, include this saved path in the delegation prompt; only subagents sharing this execution environment can read it.]", identity, path)
}
