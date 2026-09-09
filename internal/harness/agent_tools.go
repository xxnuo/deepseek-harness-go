package harness

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var errToolCallTimeout = errors.New("tool call timeout")

func transcriptMessages(events []Event, turn int) []ChatMessage {
	surface, _ := foldSurfaceEvents(events, false)
	messages := make([]ChatMessage, 0)
	for _, event := range surface {
		switch event.Type {
		case "user/message":
			messageValue := nestedMessage(event.Data)
			blocks := contentBlocks(messageValue["content"])
			if text := contentValueText(blocks); text != "" || len(blocks) > 0 {
				source, _ := messageValue["source"].(map[string]any)
				messages = append(messages, ChatMessage{Role: "user", Content: text, Blocks: blocks, Source: cloneStringMap(source)})
			}
		case "assistant/message":
			if eventTurnValue, ok := eventTurn(event.Data); ok && eventTurnValue > turn {
				continue
			}
			messageValue := nestedMessage(event.Data)
			if messageValue == nil {
				continue
			}
			blocks := contentBlocks(messageValue["content"])
			source, _ := messageValue["source"].(map[string]any)
			message := ChatMessage{Role: "assistant", Blocks: blocks, Source: cloneStringMap(source)}
			for _, block := range blocks {
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
			applyPiAIReplayState(&message, blocks)
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
			typeName := stringValue(data["type"])
			if !coreContentBlockTypes[typeName] {
				extra := cloneJSON(data).(map[string]any)
				delete(extra, "type")
				out = append(out, ContentBlock{Type: typeName, Extra: extra})
				continue
			}
			block := ContentBlock{Type: typeName, Text: stringValue(data["text"]), Signature: stringValue(data["signature"]), ID: stringValue(data["id"]), Name: stringValue(data["name"]), Arguments: stringValue(data["arguments"]), ToolCallID: stringValue(data["toolCallId"])}
			if raw, ok := data["attachment"].(map[string]any); ok {
				encoded, err := json.Marshal(raw)
				if typeName == "file" {
					var attachment FileAttachmentRef
					if err == nil && json.Unmarshal(encoded, &attachment) == nil {
						block.FileAttachment = &attachment
					}
				} else {
					var attachment ImageAttachmentRef
					if err == nil && json.Unmarshal(encoded, &attachment) == nil {
						block.Attachment = &attachment
					}
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
					out[i].Images = append(out[i].Images, ChatImage{MediaType: block.Attachment.MediaType, Data: encoded, AttachmentID: block.Attachment.AttachmentID})
					out[i].Parts = append(out[i].Parts, ChatContentPart{Type: "image", MediaType: block.Attachment.MediaType, Data: encoded, AttachmentID: block.Attachment.AttachmentID})
				case block.Type == "file" && block.FileAttachment != nil:
					path, err := e.fileAttachmentPath(*block.FileAttachment)
					if err != nil {
						path = ""
					}
					text := fileRequestText(*block.FileAttachment, path)
					out[i].Parts = append(out[i].Parts, ChatContentPart{Type: "text", Text: text})
				default:
					hydrate(block.Content)
				}
			}
		}
		hydrate(out[i].Blocks)
		if len(out[i].Parts) > 0 {
			var text strings.Builder
			for _, part := range out[i].Parts {
				if part.Type == "text" {
					text.WriteString(part.Text)
				}
			}
			out[i].Content = text.String()
		}
	}
	return out
}

func (e *Engine) requestImageLimit(provider string) int {
	if provider == "" {
		provider = e.cfg.Provider
	}
	if provider == "deepseek-official" {
		return positiveIntSetting(deepSeekEffectiveSettings(e)["maxInlineRequestImageBytes"], deepSeekDefaultMaxInlineRequestImageBytes)
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

func executeTool(ctx context.Context, tool Tool, call ToolCall) (ToolResult, error) {
	return executeToolRuntime(ctx, tool, call, &ToolRunContext{Context: ctx, Call: call})
}

func executeToolRuntime(ctx context.Context, tool Tool, call ToolCall, runtime *ToolRunContext) (ToolResult, error) {
	var result ToolResult
	var err error
	if tool.Timeout == 0 || (tool.TimeoutPolicyEnabled != nil && !*tool.TimeoutPolicyEnabled) {
		result, err = invokeToolExecutor(ctx, tool, call, runtime)
		result = applyToolRuntimeOutcome(runtime, result)
		return validateToolResult(tool, call, result, err)
	}
	runCtx, cancel := context.WithTimeoutCause(ctx, tool.Timeout, errToolCallTimeout)
	defer cancel()
	result, err = invokeToolExecutor(runCtx, tool, call, runtime)
	result = applyToolRuntimeOutcome(runtime, result)
	if errors.Is(context.Cause(runCtx), errToolCallTimeout) {
		milliseconds := tool.Timeout.Milliseconds()
		message := fmt.Sprintf("tool call timed out after %dms", milliseconds)
		return ToolResult{
			Content:            []ContentBlock{{Type: "text", Text: "Error: " + message}},
			IsError:            true,
			Error:              &ToolError{Name: "ToolTimeoutError", Code: "TOOL_TIMEOUT", Message: message},
			AdditionalContexts: result.AdditionalContexts,
		}, nil
	}
	return validateToolResult(tool, call, result, err)
}

func applyToolRuntimeOutcome(runtime *ToolRunContext, result ToolResult) ToolResult {
	if runtime == nil {
		return result
	}
	contexts, concludes := runtime.outcome()
	result.AdditionalContexts = append(contexts, result.AdditionalContexts...)
	if !result.IsError && result.Error == nil {
		result.ConcludesTurn = result.ConcludesTurn || concludes
	}
	return result
}

func invokeToolExecutor(ctx context.Context, tool Tool, call ToolCall, runtime *ToolRunContext) (ToolResult, error) {
	if tool.ExecuteRuntime == nil {
		return tool.Execute(ctx, call)
	}
	if runtime == nil {
		runtime = &ToolRunContext{Call: call}
	}
	runtime.Context = ctx
	runtime.Call = call
	return tool.ExecuteRuntime(runtime)
}

type toolOutputError struct {
	name, message string
}

func (e *toolOutputError) Error() string {
	return fmt.Sprintf("tool %q returned invalid output: %s", e.name, e.message)
}

func validateToolResult(tool Tool, call ToolCall, result ToolResult, executeErr error) (ToolResult, error) {
	result.Content = cloneContentBlocks(result.Content)
	result.AdditionalContexts = cloneToolContexts(result.AdditionalContexts)
	if result.Error != nil {
		errorCopy := *result.Error
		result.Error = &errorCopy
	}
	if result.Meta != nil {
		meta, err := canonicalToolValue(result.Meta)
		if err != nil {
			return result, &toolOutputError{name: tool.Schema.Name, message: "presentation metadata is not lossless JSON: " + err.Error()}
		}
		result.Meta = meta
	}
	if executeErr != nil || result.IsError || result.Error != nil {
		result.ConcludesTurn = false
		return result, executeErr
	}
	if tool.Schema.Output != nil {
		value, err := canonicalToolValue(result.Value)
		if err != nil {
			return result, &toolOutputError{name: tool.Schema.Name, message: "value is not lossless JSON: " + err.Error()}
		}
		if err := validateJSONAgainstSchema(value, tool.Schema.Output); err != nil {
			return result, &toolOutputError{name: tool.Schema.Name, message: err.Error()}
		}
		result.Value = value
	}
	if tool.RenderOutput != nil {
		content, err := callToolOutputRenderer(tool, call, result.Value)
		if err != nil {
			return result, err
		}
		result.Content = cloneContentBlocks(content)
	}
	if tool.PresentationMeta != nil && call.ParentCallID == "" {
		meta, err := callToolPresentationMeta(tool, call, result.Value)
		if err != nil {
			return result, err
		}
		if meta != nil {
			canonical, err := canonicalToolValue(meta)
			if err != nil {
				return result, &toolOutputError{name: tool.Schema.Name, message: "presentationMeta result is not lossless JSON: " + err.Error()}
			}
			result.Meta = canonical
		}
	}
	return result, nil
}

func callToolOutputRenderer(tool Tool, call ToolCall, value any) (content []ContentBlock, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &toolOutputError{name: tool.Schema.Name, message: fmt.Sprintf("render panicked: %v", recovered)}
		}
	}()
	content, err = tool.RenderOutput(call, value)
	if err != nil {
		return nil, &toolOutputError{name: tool.Schema.Name, message: "render failed: " + err.Error()}
	}
	return content, nil
}

func callToolPresentationMeta(tool Tool, call ToolCall, value any) (meta any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &toolOutputError{name: tool.Schema.Name, message: fmt.Sprintf("presentationMeta panicked: %v", recovered)}
		}
	}()
	meta, err = tool.PresentationMeta(call, value)
	if err != nil {
		return nil, &toolOutputError{name: tool.Schema.Name, message: "presentationMeta failed: " + err.Error()}
	}
	return meta, nil
}

func callToolContentFinalizer(tool Tool, call ToolCall, result ToolResult) (content []ContentBlock, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &toolOutputError{name: tool.Schema.Name, message: fmt.Sprintf("finalizeContent panicked: %v", recovered)}
		}
	}()
	content, err = tool.FinalizeContent(call, result)
	if err != nil {
		return nil, &toolOutputError{name: tool.Schema.Name, message: "finalizeContent failed: " + err.Error()}
	}
	return content, nil
}

func toolResultMessage(callID string, content []ContentBlock, isError bool) map[string]any {
	return map[string]any{
		"id": newID("msg"), "role": "user",
		"content": []ContentBlock{{Type: "tool-result", ToolCallID: callID, Content: content, IsError: isError}},
		"source":  map[string]any{"kind": "tool", "callId": callID},
	}
}
