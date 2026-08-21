package harness

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

func (e *Engine) applySpillPolicy(s *Session, call ToolCall, result ToolResult) ToolResult {
	config := e.cfg.Spill
	if config.Disabled || call.Name == "read" || call.ParentCallID != "" {
		return result
	}
	var text strings.Builder
	for _, block := range result.Content {
		if block.Type != "text" {
			return result
		}
		text.WriteString(block.Text)
	}
	body := text.String()
	total := len([]byte(body))
	if total <= config.MaxInlineBytes {
		return result
	}
	path, err := e.saveSpillText(s, call.Name+".txt", body)
	if err != nil {
		return result
	}
	worstNotice := spillNotice(total, total, path)
	previewBudget := config.MaxInlineBytes - len([]byte(worstNotice)) - 2
	if previewBudget < 0 {
		return result
	}
	preview, omitted := headTailUTF8(body, previewBudget)
	notice := spillNotice(omitted, total, path)
	replacement := notice
	if preview != "" {
		replacement = preview + "\n\n" + notice
	}
	if len([]byte(replacement)) > config.MaxInlineBytes {
		return result
	}
	result.Content = []ContentBlock{{Type: "text", Text: replacement}}
	return result
}

func spillNotice(omitted, total int, path string) string {
	if omitted < 0 {
		omitted = total
	}
	return fmt.Sprintf("(Omitted %d bytes. Full formatted result stored at: %s. Use read with offset/limit, or grep this path to search within it.)", omitted, path)
}

func headTailUTF8(text string, budget int) (string, int) {
	if budget <= 0 {
		return "", len([]byte(text))
	}
	data := []byte(text)
	if len(data) <= budget {
		return text, 0
	}
	headBytes := (budget + 1) / 2
	tailBytes := budget / 2
	head := truncateUTF8(text, headBytes)
	tailStart := len(data) - tailBytes
	for tailStart < len(data) && !utf8.RuneStart(data[tailStart]) {
		tailStart++
	}
	tail := string(data[tailStart:])
	kept := len([]byte(head)) + len([]byte(tail))
	return head + tail, len(data) - kept
}

func (e *Engine) saveSpillText(s *Session, suggestedName, content string) (string, error) {
	rootPath := e.cfg.Spill.Root
	if rootPath == "" {
		rootPath = filepath.Join(s.Header.CWD, ".spill")
	}
	rootPath, err := filepath.Abs(rootPath)
	if err != nil {
		return "", err
	}
	if info, statErr := os.Lstat(rootPath); statErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("spill root is not a directory: %s", rootPath)
		}
	} else if !os.IsNotExist(statErr) {
		return "", statErr
	} else if err := os.MkdirAll(rootPath, 0o700); err != nil {
		return "", err
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return "", err
	}
	defer root.Close()
	hash := sha256.Sum256([]byte(s.Header.ID))
	dir := "session-" + hex.EncodeToString(hash[:6])
	if err := root.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	random := make([]byte, 6)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	name := hex.EncodeToString(random) + "-" + encodePathSegment(suggestedName)
	relative := filepath.Join(dir, name)
	file, err := root.OpenFile(relative, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	_, writeErr := file.WriteString(content)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if closeErr := file.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		_ = root.Remove(relative)
		return "", writeErr
	}
	return filepath.Join(rootPath, relative), nil
}

func encodePathSegment(value string) string {
	if value == "" {
		return "~"
	}
	var out strings.Builder
	for _, character := range value {
		if character != '~' && character < utf8.RuneSelf && (character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("._-", character)) {
			out.WriteRune(character)
			continue
		}
		fmt.Fprintf(&out, "~%04X", character)
	}
	if out.String() == "." || out.String() == ".." {
		return strings.ReplaceAll(out.String(), ".", "~002E")
	}
	return out.String()
}
