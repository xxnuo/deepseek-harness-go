package harness

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

func (e *Engine) sessionAttachment(p map[string]any) (any, *RPCError) {
	id, _ := p["sessionId"].(string)
	attachmentID, _ := p["attachmentId"].(string)
	s, err := e.getSession(id)
	if err != nil {
		return nil, errorToRPC(err)
	}
	var ref ImageAttachmentRef
	found := false
	s.mu.Lock()
	for _, event := range s.Events {
		collectImageRefs(event.Data, attachmentID, &ref, &found)
		if found {
			break
		}
	}
	s.mu.Unlock()
	if !found {
		return nil, rpcError("attachment-error", "Image is not referenced by this session.", map[string]any{"reason": "ATTACHMENT_NOT_REFERENCED"})
	}
	data, err := e.readImage(ref)
	if err != nil {
		return nil, errorToRPC(err)
	}
	return map[string]any{"attachment": ref, "data": base64.StdEncoding.EncodeToString(data)}, nil
}

func collectImageRefs(value any, id string, ref *ImageAttachmentRef, found *bool) {
	if *found {
		return
	}
	switch value := value.(type) {
	case map[string]any:
		if candidate, ok := value["attachment"].(map[string]any); ok {
			var parsed ImageAttachmentRef
			if data, err := json.Marshal(candidate); err == nil && json.Unmarshal(data, &parsed) == nil && parsed.AttachmentID == id {
				*ref, *found = parsed, true
				return
			}
		}
		for _, child := range value {
			collectImageRefs(child, id, ref, found)
		}
	case []any:
		for _, child := range value {
			collectImageRefs(child, id, ref, found)
		}
	case []ContentBlock:
		for _, child := range value {
			collectImageRefs(child, id, ref, found)
		}
	case ContentBlock:
		if value.Type == "image" && value.Attachment != nil && value.Attachment.AttachmentID == id {
			*ref, *found = *value.Attachment, true
			return
		}
		collectImageRefs(value.Content, id, ref, found)
	case *ContentBlock:
		if value != nil {
			collectImageRefs(*value, id, ref, found)
		}
	}
}

func (e *Engine) updateQueue(p map[string]any) (any, *RPCError) {
	id, _ := p["sessionId"].(string)
	itemID, _ := p["itemId"].(string)
	action, _ := p["action"].(map[string]any)
	kind, _ := action["kind"].(string)
	s, err := e.getSession(id)
	if err != nil {
		return nil, errorToRPC(err)
	}
	s.mu.Lock()
	for i, item := range s.pending {
		if item.id != itemID {
			continue
		}
		var events []Event
		switch kind {
		case "remove":
			event, appendErr := appendEventLocked(s, "agent/inbox/spliced", map[string]any{
				"target": "next-turn", "start": i, "removedCount": 1, "inserted": []any{}, "outcome": "canceled",
			}, nil, nil, false)
			if appendErr != nil {
				s.mu.Unlock()
				return nil, errorToRPC(appendErr)
			}
			events = append(events, event)
			s.pending = append(s.pending[:i], s.pending[i+1:]...)
		case "edit":
			content, _ := action["content"].([]any)
			var parts []PromptContentPart
			for _, raw := range content {
				block, _ := raw.(map[string]any)
				typ, _ := block["type"].(string)
				if typ != "text" {
					s.mu.Unlock()
					return nil, rpcError("attachment-error", "queue edits accept text content only", map[string]any{"reason": "QUEUE_EDIT_NON_TEXT"})
				}
				text, _ := block["text"].(string)
				parts = append(parts, PromptContentPart{Type: "text", Text: text})
			}
			text := strings.TrimSpace(contentText(parts))
			if text == "" {
				s.mu.Unlock()
				return nil, rpcError("bad-request", "edited prompt content is empty", map[string]any{})
			}
			replacement := *item
			replacement.text = text
			replacement.content = []ContentBlock{{Type: "text", Text: text}}
			event, appendErr := appendEventLocked(s, "agent/inbox/spliced", map[string]any{
				"target": "next-turn", "start": i, "removedCount": 1, "inserted": []any{replacement.message()}, "outcome": "canceled",
			}, nil, nil, false)
			if appendErr != nil {
				s.mu.Unlock()
				return nil, errorToRPC(appendErr)
			}
			events = append(events, event)
			*item = replacement
		case "steer":
			if !s.Running {
				s.mu.Unlock()
				return nil, rpcError("steer-unavailable", "current turn no longer accepts steering", map[string]any{"itemId": itemID})
			}
			removed, appendErr := appendEventLocked(s, "agent/inbox/spliced", map[string]any{
				"target": "next-turn", "start": i, "removedCount": 1, "inserted": []any{}, "outcome": "canceled",
			}, nil, nil, false)
			if appendErr != nil {
				s.mu.Unlock()
				return nil, errorToRPC(appendErr)
			}
			events = append(events, removed)
			s.pending = append(s.pending[:i], s.pending[i+1:]...)
			inserted, appendErr := appendEventLocked(s, "agent/inbox/spliced", map[string]any{
				"target": "next-step", "start": len(s.steering), "inserted": []any{item.message()},
			}, nil, nil, false)
			if appendErr != nil {
				s.mu.Unlock()
				for _, event := range events {
					e.publishEvent(id, event)
				}
				e.emitQueue(s)
				return nil, errorToRPC(appendErr)
			}
			events = append(events, inserted)
			s.steering = append(s.steering, item)
		default:
			s.mu.Unlock()
			return nil, rpcError("bad-request", "unknown queue action", map[string]any{})
		}
		s.mu.Unlock()
		for _, event := range events {
			e.publishEvent(id, event)
		}
		e.emitQueue(s)
		return map[string]any{"accepted": true}, nil
	}
	for i, item := range s.steering {
		if item.id != itemID {
			continue
		}
		if kind == "remove" {
			event, appendErr := appendEventLocked(s, "agent/inbox/spliced", map[string]any{
				"target": "next-step", "start": i, "removedCount": 1, "inserted": []any{}, "outcome": "canceled",
			}, nil, nil, false)
			if appendErr != nil {
				s.mu.Unlock()
				return nil, errorToRPC(appendErr)
			}
			s.steering = append(s.steering[:i], s.steering[i+1:]...)
			s.mu.Unlock()
			e.publishEvent(id, event)
			e.emitQueue(s)
			return map[string]any{"accepted": true}, nil
		}
		s.mu.Unlock()
		return nil, rpcError("steer-unavailable", "steering item is already assigned to the current turn", map[string]any{"itemId": itemID})
	}
	s.mu.Unlock()
	return nil, rpcError("queue-item-not-found", "queued item is no longer pending", map[string]any{"itemId": itemID})
}

func (e *Engine) queueFrame(s *Session) map[string]any {
	s.mu.Lock()
	id := s.Header.ID
	items := make([]map[string]any, 0, len(s.pending)+len(s.steering))
	for _, item := range s.pending {
		items = append(items, map[string]any{"id": item.id, "placement": "queued", "message": item.message()})
	}
	for _, item := range s.steering {
		items = append(items, map[string]any{"id": item.id, "placement": "steering", "message": item.message()})
	}
	s.mu.Unlock()
	return map[string]any{"type": "session/queue", "sessionId": id, "items": items}
}

func (e *Engine) emitQueue(s *Session) { e.emitMux(e.queueFrame(s)) }
