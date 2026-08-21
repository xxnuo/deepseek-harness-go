package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const gentleRepeatToolReminder = "You are repeating the exact same tool call with identical arguments. Carefully analyze the previous result before calling again: if the task is not complete, try a different approach or different arguments instead of repeating the call."

func (e *Engine) appendRepeatToolReminder(s *Session, call ToolCall) error {
	config := e.cfg.RepeatToolReminder
	if config.Disabled || !repeatToolTracked(call.Name, config.Include, config.Exclude) {
		return nil
	}
	canonical := canonicalToolArguments(call.Arguments)
	keyData, _ := json.Marshal([]string{call.Name, canonical})
	key := string(keyData)
	s.mu.Lock()
	if s.repeatToolKey == key {
		s.repeatToolCount++
	} else {
		s.repeatToolKey = key
		s.repeatToolCount = 1
	}
	count := s.repeatToolCount
	s.mu.Unlock()
	matched := false
	for _, threshold := range config.Thresholds {
		if count == threshold {
			matched = true
			break
		}
	}
	if !matched {
		return nil
	}
	text := gentleRepeatToolReminder
	if count != config.Thresholds[0] {
		preview := []rune(canonical)
		omitted := 0
		if len(preview) > config.ArgumentsPreviewChars {
			omitted = len(preview) - config.ArgumentsPreviewChars
			preview = preview[:config.ArgumentsPreviewChars]
		}
		arguments := string(preview)
		if omitted > 0 {
			arguments += fmt.Sprintf("... (+%d more chars)", omitted)
		}
		text = fmt.Sprintf("Repeated tool call detected:\n- tool: %s\n- consecutive_calls: %d\n- arguments: %s\nThe repeated calls are not making progress. Do not call this tool with these exact arguments again. Inspect the latest result and choose a different action, different arguments, or finish the task if enough evidence has been gathered.", call.Name, count, arguments)
	}
	_, err := e.appendEvent(s, "user/message", map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: text}},
		"source": map[string]any{"kind": "plugin", "plugin": "repeat-tool-reminder", "form": "notice", "summary": fmt.Sprintf("%s x %d", call.Name, count)},
	})
	return err
}

func repeatToolTracked(name string, include, exclude []string) bool {
	if len(include) > 0 {
		matched := false
		for _, pattern := range include {
			if wildcardMatch(pattern, name) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, pattern := range exclude {
		if wildcardMatch(pattern, name) {
			return false
		}
	}
	return true
}

func wildcardMatch(pattern, value string) bool {
	patternRunes, valueRunes := []rune(pattern), []rune(value)
	row := make([]bool, len(valueRunes)+1)
	row[0] = true
	for _, character := range patternRunes {
		next := make([]bool, len(valueRunes)+1)
		if character == '*' {
			next[0] = row[0]
			for index := 1; index <= len(valueRunes); index++ {
				next[index] = next[index-1] || row[index]
			}
		} else {
			for index := 1; index <= len(valueRunes); index++ {
				next[index] = row[index-1] && character == valueRunes[index-1]
			}
		}
		row = next
	}
	return row[len(valueRunes)]
}

func canonicalToolArguments(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return "{}"
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return strings.TrimSpace(string(raw))
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return strings.TrimSpace(string(raw))
	}
	return string(encoded)
}

func resetRepeatToolChain(s *Session, typ string, data any) {
	if typ != "user/message" {
		return
	}
	message := nestedMessage(data)
	source, _ := message["source"].(map[string]any)
	if source["kind"] == "user" {
		s.repeatToolKey = ""
		s.repeatToolCount = 0
	}
}
