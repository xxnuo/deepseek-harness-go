package harness

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

const workspaceFileMaxBytes = 2 * 1024 * 1024
const workspaceFileMaxLines = 5000
const workspaceFileMaxEntries = 2000

type WorkspaceFilesConfig struct {
	MaxBytes   int `json:"maxBytes"`
	MaxLines   int `json:"maxLines"`
	MaxEntries int `json:"maxEntries"`
}

func (config WorkspaceFilesConfig) defaults() WorkspaceFilesConfig {
	if config.MaxBytes == 0 {
		config.MaxBytes = workspaceFileMaxBytes
	}
	if config.MaxLines == 0 {
		config.MaxLines = workspaceFileMaxLines
	}
	if config.MaxEntries == 0 {
		config.MaxEntries = workspaceFileMaxEntries
	}
	return config
}

func workspaceFileKind(info fs.FileInfo) string {
	if info.Mode().IsRegular() {
		return "file"
	}
	if info.IsDir() {
		return "directory"
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return "symlink"
	}
	return "other"
}

func workspaceFileVersion(info fs.FileInfo) string {
	return fmt.Sprintf("%x:%x:%x", info.ModTime().UnixNano(), info.Size(), info.Mode())
}

func workspaceRelativePath(root, target string) (string, bool) {
	relative, err := filepath.Rel(root, target)
	return relative, err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}

func (e *Engine) workspaceFileRoot(endpoint string, args map[string]json.RawMessage) (string, *RPCError) {
	id, rpcErr := remoteString(endpoint, args, "agentId")
	if rpcErr != nil {
		return "", rpcErr
	}
	session, err := e.getSession(id)
	if err != nil {
		return "", rpcError("session/not-found", "session is not attached", map[string]any{"sessionId": id})
	}
	session.mu.Lock()
	root, attached := session.Header.CWD, session.attached && !session.draining
	session.mu.Unlock()
	if !attached {
		return "", rpcError("session/not-found", "session is not attached", map[string]any{"sessionId": id})
	}
	return root, nil
}

func workspaceFileFailure(path, code, message string) *RPCError {
	return rpcError("workspace-file/"+code, message, map[string]any{"path": path})
}

func workspaceFileRange(endpoint string, raw json.RawMessage, binary bool, config WorkspaceFilesConfig) (int, int, *RPCError) {
	key, offset, limit := "limit", 1, config.MaxLines
	if binary {
		key, offset, limit = "length", 0, config.MaxBytes
	}
	args, err := remoteObject(raw, []string{"offset", key}, nil)
	if err != nil {
		return 0, 0, remoteBoundaryError(endpoint, "range")
	}
	if _, exists := args["offset"]; exists {
		value, rpcErr := remoteInt(endpoint, args, "offset")
		if rpcErr != nil || value < offset {
			return 0, 0, rpcError("gateway/bad-request", "invalid offset", map[string]any{})
		}
		offset = value
	}
	if _, exists := args[key]; exists {
		value, rpcErr := remoteInt(endpoint, args, key)
		if rpcErr != nil || value < 1 {
			return 0, 0, rpcError("gateway/bad-request", "invalid "+key, map[string]any{})
		}
		limit = value
	}
	if int64(offset)+int64(limit) > 9007199254740991 {
		return 0, 0, rpcError("gateway/bad-request", "range must stay a safe integer", map[string]any{})
	}
	if !binary && limit > config.MaxLines {
		return 0, 0, rpcError("gateway/bad-request", "limit exceeds maximum lines", map[string]any{})
	}
	return offset, limit, nil
}

func (e *Engine) remoteWorkspaceFile(ctx context.Context, endpoint string, args map[string]json.RawMessage) (any, *RPCError) {
	config := e.Config().WorkspaceFiles.defaults()
	requested, rpcErr := remoteString(endpoint, args, "path")
	if rpcErr != nil {
		return nil, rpcErr
	}
	if requested == "" {
		return nil, rpcError("gateway/bad-request", "path is required", map[string]any{})
	}
	offset, limit := 0, 0
	if endpoint == "workspaceFiles/read" || endpoint == "workspaceFiles/readBytes" {
		offset, limit, rpcErr = workspaceFileRange(endpoint, args["range"], endpoint == "workspaceFiles/readBytes", config)
		if rpcErr != nil {
			return nil, rpcErr
		}
		if endpoint == "workspaceFiles/readBytes" && limit > config.MaxBytes {
			return nil, rpcError("workspace-file/too-large", "byte range exceeds limit", map[string]any{"path": requested, "limit": config.MaxBytes})
		}
	}
	workspace, rpcErr := e.workspaceFileRoot(endpoint, args)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if err := ctx.Err(); err != nil {
		return nil, rpcError("gateway/internal", err.Error(), map[string]any{})
	}
	rootPath, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return nil, workspaceFileFailure(requested, "not-found", err.Error())
	}
	target := requested
	if !filepath.IsAbs(target) {
		target = filepath.Join(rootPath, target)
	}
	entry, err := os.Lstat(target)
	if err != nil {
		return nil, workspaceFileFailure(requested, "not-found", err.Error())
	}
	wanted, code := "file", "not-regular-file"
	if endpoint == "workspaceFiles/list" {
		wanted, code = "directory", "not-directory"
	}
	if kind := workspaceFileKind(entry); kind != wanted {
		return nil, rpcError("workspace-file/"+code, "unexpected entry type", map[string]any{"path": requested, "kind": kind})
	}
	absolute, err := filepath.EvalSymlinks(target)
	if err != nil {
		return nil, workspaceFileFailure(requested, "not-found", err.Error())
	}
	relative, contained := workspaceRelativePath(rootPath, absolute)
	if !contained {
		return nil, workspaceFileFailure(requested, "outside-workspace", "path is outside the workspace")
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, workspaceFileFailure(requested, "not-found", err.Error())
	}
	defer root.Close()
	file, err := root.Open(relative)
	if err != nil {
		return nil, workspaceFileFailure(requested, "not-found", err.Error())
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, workspaceFileFailure(requested, "not-found", err.Error())
	}
	if kind := workspaceFileKind(info); kind != wanted {
		return nil, rpcError("workspace-file/"+code, "entry changed type", map[string]any{"path": requested, "kind": kind})
	}
	if endpoint == "workspaceFiles/list" {
		children, err := file.ReadDir(-1)
		if err != nil {
			return nil, workspaceFileFailure(requested, "not-directory", err.Error())
		}
		return workspaceDirectoryListing(relative, absolute, children, config.MaxEntries), nil
	}
	result := map[string]any{"absolutePath": absolute, "version": workspaceFileVersion(info), "bytes": info.Size()}
	if endpoint == "workspaceFiles/stat" {
		return result, nil
	}
	result["offset"] = offset
	if endpoint == "workspaceFiles/readBytes" {
		data := make([]byte, limit)
		count, err := file.ReadAt(data, int64(offset))
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, workspaceFileFailure(requested, "not-found", err.Error())
		}
		result["data"], result["eof"] = base64.StdEncoding.EncodeToString(data[:count]), int64(offset)+int64(count) >= info.Size()
		return result, nil
	}
	text, lines, eof, err := readWorkspaceFilePage(ctx, file, offset, limit, config.MaxBytes)
	if err != nil {
		if errors.Is(err, errWorkspacePageTooLarge) {
			return nil, rpcError("workspace-file/too-large", err.Error(), map[string]any{"path": requested, "limit": config.MaxBytes})
		}
		return nil, workspaceFileFailure(requested, "not-text", err.Error())
	}
	result["text"], result["lines"], result["eof"] = text, lines, eof
	return result, nil
}

var errWorkspacePageTooLarge = errors.New("text page exceeds byte limit")

func readWorkspaceFilePage(ctx context.Context, input io.Reader, offset, limit, maxBytes int) (string, int, bool, error) {
	reader := bufio.NewReader(input)
	var text strings.Builder
	line, lines, pending := 1, 0, false
	for {
		if err := ctx.Err(); err != nil {
			return "", 0, false, err
		}
		if line-offset >= limit {
			_, err := reader.Peek(1)
			return text.String(), lines, errors.Is(err, io.EOF), errUnlessEOF(err)
		}
		character, size, err := reader.ReadRune()
		if errors.Is(err, io.EOF) {
			if pending {
				lines++
			}
			return text.String(), lines, true, nil
		}
		if err != nil {
			return "", 0, false, err
		}
		if character == utf8.RuneError && size == 1 {
			return "", 0, false, errors.New("file is not UTF-8 text")
		}
		if line >= offset {
			if character == 0 {
				return "", 0, false, errors.New("page contains NUL bytes")
			}
			if !pending && lines > 0 {
				text.WriteByte('\n')
			}
			pending = true
			if character != '\n' {
				text.WriteRune(character)
			}
			if text.Len() > maxBytes {
				return "", 0, false, errWorkspacePageTooLarge
			}
		}
		if character == '\n' {
			if line >= offset {
				lines++
			}
			pending = false
			line++
		}
	}
}

func errUnlessEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func workspaceDirectoryListing(relative, absolute string, children []os.DirEntry, maxEntries int) map[string]any {
	sort.Slice(children, func(left, right int) bool { return children[left].Name() < children[right].Name() })
	truncated := len(children) > maxEntries
	if truncated {
		children = children[:maxEntries]
	}
	entries := make([]map[string]any, 0, len(children))
	for _, child := range children {
		entry := map[string]any{"name": child.Name(), "type": "other"}
		if info, err := os.Stat(filepath.Join(absolute, child.Name())); err == nil {
			entry["type"] = workspaceFileKind(info)
			if info.Mode().IsRegular() {
				entry["size"] = info.Size()
			}
		}
		entries = append(entries, entry)
	}
	if relative == "." {
		relative = ""
	}
	return map[string]any{"path": filepath.ToSlash(relative), "entries": entries, "truncated": truncated}
}
