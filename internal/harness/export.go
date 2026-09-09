package harness

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
)

func safeExportSegment(value string) string {
	value = filepath.Base(strings.TrimSpace(value))
	value = strings.NewReplacer("/", "_", "\\", "_", "..", "_").Replace(value)
	if value == "" || value == "." {
		return "session"
	}
	return value
}

func (e *Engine) sessionJSONL(id string) ([]byte, error) {
	var out bytes.Buffer
	if err := e.ExportSession(id, &out); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// ExportSessionZIP writes the upstream-compatible archive. Descendants are
// ordered by session id for deterministic exports and are selected through the
// durable parentSession header.
func (e *Engine) ExportSessionZIP(id string, includeDescendants bool, w io.Writer) error {
	if _, err := e.getSession(id); err != nil {
		return err
	}
	entries := []struct {
		id   string
		path string
	}{{id: id, path: "session.v2.jsonl"}}
	if includeDescendants {
		// Do not hold the Engine lock while reading Session headers. Persistence
		// takes the same locks in Engine -> Session order.
		e.mu.RLock()
		sessions := make([]struct {
			id string
			s  *Session
		}, 0, len(e.sessions))
		for childID, s := range e.sessions {
			sessions = append(sessions, struct {
				id string
				s  *Session
			}{id: childID, s: s})
		}
		e.mu.RUnlock()
		children := make(map[string][]string)
		for _, item := range sessions {
			childID, s := item.id, item.s
			s.mu.Lock()
			parent := s.Header.ParentSession
			s.mu.Unlock()
			if parent != "" {
				children[parent] = append(children[parent], childID)
			}
		}
		var walk func(string)
		seen := map[string]bool{id: true}
		walk = func(parent string) {
			ids := append([]string(nil), children[parent]...)
			sort.Strings(ids)
			for _, child := range ids {
				if seen[child] {
					continue
				}
				seen[child] = true
				entries = append(entries, struct {
					id   string
					path string
				}{id: child, path: "subagents/" + safeExportSegment(child) + "/session.v2.jsonl"})
				walk(child)
			}
		}
		walk(id)
	}
	media := map[string]ImageAttachmentRef{}
	files := map[string]FileAttachmentRef{}
	for _, entry := range entries {
		data, err := e.sessionJSONL(entry.id)
		if err != nil {
			return err
		}
		collectExportAttachmentRefs(data, media, files)
	}
	zw := zip.NewWriter(w)
	for _, entry := range entries {
		data, err := e.sessionJSONL(entry.id)
		if err != nil {
			_ = zw.Close()
			return err
		}
		file, err := zw.CreateHeader(&zip.FileHeader{Name: entry.path, Method: zip.Deflate})
		if err != nil {
			_ = zw.Close()
			return err
		}
		if _, err := file.Write(data); err != nil {
			_ = zw.Close()
			return err
		}
	}
	mediaIDs := make([]string, 0, len(media))
	for id := range media {
		mediaIDs = append(mediaIDs, id)
	}
	sort.Strings(mediaIDs)
	for _, id := range mediaIDs {
		ref := media[id]
		data, err := e.readImage(ref)
		if err != nil {
			_ = zw.Close()
			return err
		}
		file, err := zw.CreateHeader(&zip.FileHeader{Name: "media/" + id + imageExtension(ref.MediaType), Method: zip.Deflate})
		if err != nil {
			_ = zw.Close()
			return err
		}
		if _, err := file.Write(data); err != nil {
			_ = zw.Close()
			return err
		}
	}
	fileIDs := make([]string, 0, len(files))
	for id := range files {
		fileIDs = append(fileIDs, id)
	}
	sort.Strings(fileIDs)
	for _, id := range fileIDs {
		ref := files[id]
		data, err := e.readFileAttachment(context.Background(), ref)
		if err != nil {
			_ = zw.Close()
			return err
		}
		digest := strings.TrimPrefix(ref.AttachmentID, "sha256:")
		file, err := zw.CreateHeader(&zip.FileHeader{Name: "files/" + digest[:2] + "/" + digest + "/" + fileAttachmentName(ref.Name), Method: zip.Deflate})
		if err != nil {
			_ = zw.Close()
			return err
		}
		if _, err := file.Write(data); err != nil {
			_ = zw.Close()
			return err
		}
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("close session archive: %w", err)
	}
	return nil
}

func collectExportAttachmentRefs(data []byte, images map[string]ImageAttachmentRef, files map[string]FileAttachmentRef) {
	var lines = strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var value any
		if json.Unmarshal([]byte(line), &value) != nil {
			continue
		}
		collectExportAttachmentValue(value, images, files)
	}
}

func collectExportAttachmentValue(value any, images map[string]ImageAttachmentRef, files map[string]FileAttachmentRef) {
	switch value := value.(type) {
	case map[string]any:
		if blockType, _ := value["type"].(string); blockType == "image" {
			if raw, ok := value["attachment"].(map[string]any); ok {
				var ref ImageAttachmentRef
				if data, err := json.Marshal(raw); err == nil && json.Unmarshal(data, &ref) == nil && ref.AttachmentID != "" {
					images[ref.AttachmentID] = ref
				}
			}
		}
		if blockType, _ := value["type"].(string); blockType == "file" {
			if raw, ok := value["attachment"].(map[string]any); ok {
				var ref FileAttachmentRef
				if data, err := json.Marshal(raw); err == nil && json.Unmarshal(data, &ref) == nil && ref.AttachmentID != "" {
					files[ref.AttachmentID+"\x00"+ref.Name] = ref
				}
			}
		}
		for _, child := range value {
			collectExportAttachmentValue(child, images, files)
		}
	case []any:
		for _, child := range value {
			collectExportAttachmentValue(child, images, files)
		}
	}
}
