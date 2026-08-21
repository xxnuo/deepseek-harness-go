package harness

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

var errToolCallTimeout = errors.New("tool call timeout")

func transcriptMessages(events []Event, turn int) []ChatMessage {
	surface, _ := foldSurfaceEvents(events, false)
	messages := make([]ChatMessage, 0)
	for _, event := range surface {
		switch event.Type {
		case "user/message":
			blocks := contentBlocks(nestedMessage(event.Data)["content"])
			if text := contentValueText(blocks); text != "" || len(blocks) > 0 {
				messages = append(messages, ChatMessage{Role: "user", Content: text, Blocks: blocks})
			}
		case "assistant/message":
			if eventTurnValue, ok := eventTurn(event.Data); ok && eventTurnValue > turn {
				continue
			}
			messageValue := nestedMessage(event.Data)
			if messageValue == nil {
				continue
			}
			message := ChatMessage{Role: "assistant"}
			for _, block := range contentBlocks(messageValue["content"]) {
				switch block.Type {
				case "text":
					message.Content += block.Text
				case "reasoning":
					message.Reasoning += block.Text
					if block.Signature != "" {
						message.ReasoningSignature = block.Signature
					}
				case "tool-call":
					message.ToolCalls = append(message.ToolCalls, ToolCall{ID: block.ID, Name: block.Name, Arguments: json.RawMessage(block.Arguments)})
				}
			}
			if message.Content != "" || message.Reasoning != "" || len(message.ToolCalls) > 0 {
				messages = append(messages, message)
			}
		case "tool/result":
			if eventTurnValue, ok := eventTurn(event.Data); ok && eventTurnValue > turn {
				continue
			}
			messageValue := nestedMessage(event.Data)
			if messageValue == nil {
				continue
			}
			for _, block := range contentBlocks(messageValue["content"]) {
				if block.Type == "tool-result" {
					messages = append(messages, ChatMessage{Role: "tool", Content: contentValueText(block.Content), Blocks: block.Content, ToolCallID: block.ToolCallID})
				}
			}
		}
	}
	return messages
}

func nestedMessage(value any) map[string]any {
	data, _ := value.(map[string]any)
	if data == nil {
		return nil
	}
	if nested, _ := data["message"].(map[string]any); nested != nil {
		return nested
	}
	return data
}

func contentBlocks(value any) []ContentBlock {
	switch blocks := value.(type) {
	case []ContentBlock:
		return append([]ContentBlock(nil), blocks...)
	case []any:
		out := make([]ContentBlock, 0, len(blocks))
		for _, raw := range blocks {
			data, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			block := ContentBlock{Type: stringValue(data["type"]), Text: stringValue(data["text"]), Signature: stringValue(data["signature"]), ID: stringValue(data["id"]), Name: stringValue(data["name"]), Arguments: stringValue(data["arguments"]), ToolCallID: stringValue(data["toolCallId"])}
			if raw, ok := data["attachment"].(map[string]any); ok {
				var attachment ImageAttachmentRef
				if encoded, err := json.Marshal(raw); err == nil && json.Unmarshal(encoded, &attachment) == nil {
					block.Attachment = &attachment
				}
			}
			block.IsError, _ = data["isError"].(bool)
			block.Content = contentBlocks(data["content"])
			out = append(out, block)
		}
		return out
	default:
		return nil
	}
}

func (e *Engine) hydrateChatMessages(messages []ChatMessage) []ChatMessage {
	return e.hydrateChatMessagesWithLimit(messages, 0)
}

func base64PayloadBytes(bytes int) int {
	if bytes <= 0 {
		return 0
	}
	return ((bytes + 2) / 3) * 4
}

func replaceOldestBlockImages(blocks []ContentBlock, remaining *int) []ContentBlock {
	cloned := cloneContentBlocks(blocks)
	for index := range cloned {
		if *remaining == 0 {
			break
		}
		if cloned[index].Type == "image" && cloned[index].Attachment != nil {
			cloned[index] = ContentBlock{Type: "text", Text: OffloadedImageText}
			*remaining = *remaining - 1
			continue
		}
		cloned[index].Content = replaceOldestBlockImages(cloned[index].Content, remaining)
	}
	return cloned
}

func offloadDurableMessageImages(messages []ChatMessage, maxBytes int) []ChatMessage {
	if maxBytes <= 0 {
		return messages
	}
	lengths := make([]int, 0)
	var collect func([]ContentBlock)
	collect = func(blocks []ContentBlock) {
		for _, block := range blocks {
			if block.Type == "image" && block.Attachment != nil {
				lengths = append(lengths, base64PayloadBytes(block.Attachment.Bytes))
			}
			collect(block.Content)
		}
	}
	for _, message := range messages {
		collect(message.Blocks)
	}
	total, count := 0, 0
	for _, bytes := range lengths {
		total += bytes
	}
	for _, bytes := range lengths {
		if total <= maxBytes {
			break
		}
		total -= bytes
		count++
	}
	if count == 0 {
		return messages
	}
	out := append([]ChatMessage(nil), messages...)
	remaining := count
	for index := range out {
		before := remaining
		blocks := replaceOldestBlockImages(out[index].Blocks, &remaining)
		if remaining != before {
			out[index].Blocks = blocks
			out[index].Content = contentValueText(blocks)
			out[index].HadImages = true
		}
	}
	return out
}

func (e *Engine) hydrateChatMessagesWithLimit(messages []ChatMessage, maxBytes int) []ChatMessage {
	messages = offloadDurableMessageImages(messages, maxBytes)
	out := make([]ChatMessage, len(messages))
	copy(out, messages)
	for i := range out {
		if len(out[i].Blocks) == 0 {
			continue
		}
		var hydrate func([]ContentBlock)
		hydrate = func(blocks []ContentBlock) {
			for _, block := range blocks {
				switch {
				case block.Type == "text":
					out[i].Parts = append(out[i].Parts, ChatContentPart{Type: "text", Text: block.Text})
				case block.Type == "image" && block.Attachment != nil:
					data, err := e.readImage(*block.Attachment)
					if err != nil {
						continue
					}
					encoded := base64.StdEncoding.EncodeToString(data)
					out[i].Images = append(out[i].Images, ChatImage{MediaType: block.Attachment.MediaType, Data: encoded})
					out[i].Parts = append(out[i].Parts, ChatContentPart{Type: "image", MediaType: block.Attachment.MediaType, Data: encoded})
				default:
					hydrate(block.Content)
				}
			}
		}
		hydrate(out[i].Blocks)
	}
	return out
}

func (e *Engine) requestImageLimit(provider string) int {
	if provider == "" {
		provider = e.cfg.Provider
	}
	if provider == "deepseek-official" {
		return positiveIntSetting(deepSeekEffectiveSettings(e)["maxRequestImageBytes"], deepSeekDefaultImageBytes)
	}
	e.mu.RLock()
	configured := e.piAIProviders[provider]
	e.mu.RUnlock()
	if configured != nil {
		return configured.profile.maxRequestImageBytes
	}
	return 0
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func (e *Engine) executeToolCall(ctx context.Context, s *Session, turn, step int, call ToolCall) error {
	if len(call.Arguments) == 0 {
		call.Arguments = json.RawMessage(`{}`)
	}
	call.Workspace = s.Header.CWD
	call.SessionID = s.Header.ID
	callEvent, err := e.appendEvent(s, "tool/call", map[string]any{"turn": turn, "step": step, "callId": call.ID, "name": call.Name, "arguments": string(call.Arguments)})
	if err != nil {
		return err
	}
	if !json.Valid(call.Arguments) {
		return e.appendToolFailure(s, turn, step, call, callEvent.Seq, "INVALID_ARGUMENTS", "tool arguments are not valid JSON")
	}
	visible, visibilityErr := e.toolVisibleForSession(s, call.Name)
	if visibilityErr != nil {
		return e.appendToolFailure(s, turn, step, call, callEvent.Seq, "TOOL_CATALOG", visibilityErr.Error())
	}
	e.mu.RLock()
	tool, ok := e.tools[call.Name]
	e.mu.RUnlock()
	if !ok || !visible {
		return e.appendToolFailure(s, turn, step, call, callEvent.Seq, "UNKNOWN_TOOL", fmt.Sprintf("unknown tool: %s", call.Name))
	}
	pre, err := e.runHookPoint(ctx, s, hookPointInput{
		point: "PreToolUse", match: call.Name, turn: turn, call: &call,
	})
	if err != nil {
		return err
	}
	if pre.Decision == "deny" {
		reason := pre.Reason
		if reason == "" {
			reason = "blocked by PreToolUse hook"
		}
		return e.appendToolFailure(s, turn, step, call, callEvent.Seq, "HOOK_DENIED", reason)
	}
	if pre.Decision == "ask" {
		reason := pre.Reason
		if reason == "" {
			reason = "approval requested by PreToolUse hook"
		}
		allowed, approvalErr := e.requestHookApproval(ctx, s, call, reason)
		if approvalErr != nil {
			return approvalErr
		}
		if !allowed {
			return e.appendToolFailure(s, turn, step, call, callEvent.Seq, "HOOK_APPROVAL_DENIED", reason)
		}
	}
	result, err := executeTool(ctx, tool, call)
	if err != nil {
		if result.Error == nil {
			result.Error = toolExecutionError(err)
		}
		result.IsError = true
	}
	if result.Error != nil {
		result.IsError = true
	}
	if result.Content == nil && result.Error != nil {
		result.Content = []ContentBlock{{Type: "text", Text: result.Error.Message}}
	}
	post, err := e.runHookPoint(ctx, s, hookPointInput{
		point: "PostToolUse", match: call.Name, turn: turn, call: &call, result: &result,
	})
	if err != nil {
		return err
	}
	if post.Decision == "deny" {
		reason := post.Reason
		if reason == "" {
			reason = "blocked by PostToolUse hook"
		}
		result.Content = []ContentBlock{{Type: "text", Text: reason}}
		result.IsError = true
		result.Error = &ToolError{Name: "ToolError", Code: "HOOK_DENIED", Message: reason}
	}
	result = e.applySpillPolicy(s, call, result)
	data := map[string]any{"turn": turn, "step": step, "message": toolResultMessage(call.ID, result.Content, result.IsError)}
	if result.Error != nil {
		data["error"] = result.Error
	}
	if result.Meta != nil {
		data["meta"] = result.Meta
	}
	if _, err = e.appendEvent(s, "tool/result", data, callEvent.Seq); err != nil {
		return err
	}
	if err := e.appendHookContexts(s, post.contexts); err != nil {
		return err
	}
	return e.appendRepeatToolReminder(s, call)
}

func executeTool(ctx context.Context, tool Tool, call ToolCall) (ToolResult, error) {
	var result ToolResult
	var err error
	if tool.Timeout == 0 {
		result, err = tool.Execute(ctx, call)
		return validateToolResult(tool, result, err)
	}
	runCtx, cancel := context.WithTimeoutCause(ctx, tool.Timeout, errToolCallTimeout)
	defer cancel()
	result, err = tool.Execute(runCtx, call)
	if errors.Is(context.Cause(runCtx), errToolCallTimeout) {
		milliseconds := tool.Timeout.Milliseconds()
		message := fmt.Sprintf("tool call timed out after %dms", milliseconds)
		return ToolResult{
			Content: []ContentBlock{{Type: "text", Text: "Error: " + message}},
			IsError: true,
			Error:   &ToolError{Name: "ToolTimeoutError", Code: "TOOL_TIMEOUT", Message: message},
		}, nil
	}
	return validateToolResult(tool, result, err)
}

type toolOutputError struct {
	name, message string
}

func (e *toolOutputError) Error() string {
	return fmt.Sprintf("tool %q returned invalid output: %s", e.name, e.message)
}

func validateToolResult(tool Tool, result ToolResult, executeErr error) (ToolResult, error) {
	if executeErr != nil || result.IsError || result.Error != nil || tool.Schema.Output == nil {
		return result, executeErr
	}
	value, err := canonicalToolValue(result.Value)
	if err != nil {
		return result, &toolOutputError{name: tool.Schema.Name, message: "value is not lossless JSON: " + err.Error()}
	}
	if err := validateJSONAgainstSchema(value, tool.Schema.Output); err != nil {
		return result, &toolOutputError{name: tool.Schema.Name, message: err.Error()}
	}
	return result, nil
}

func (e *Engine) appendToolFailure(s *Session, turn, step int, call ToolCall, sourceSeq int, code, message string) error {
	_, err := e.appendEvent(s, "tool/result", map[string]any{
		"turn": turn, "step": step,
		"message": toolResultMessage(call.ID, []ContentBlock{{Type: "text", Text: message}}, true),
		"error":   map[string]any{"name": "ToolError", "code": code, "message": message},
	}, sourceSeq)
	return err
}

func toolResultMessage(callID string, content []ContentBlock, isError bool) map[string]any {
	return map[string]any{
		"id": newID("msg"), "role": "user",
		"content": []ContentBlock{{Type: "tool-result", ToolCallID: callID, Content: content, IsError: isError}},
		"source":  map[string]any{"kind": "tool", "callId": callID},
	}
}
