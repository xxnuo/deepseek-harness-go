package harness

import (
	"context"
)

func (e *Engine) childFor(parentID, childID string) (*Session, *RPCError) {
	if parentID == "" || childID == "" {
		return nil, rpcError("bad-request", "parentSessionId and childSessionId are required", nil)
	}
	child, err := e.getSession(childID)
	if err != nil {
		return nil, rpcError("subagent-not-found", "subagent not found", map[string]any{"parentSessionId": parentID, "childSessionId": childID})
	}
	child.mu.Lock()
	parent := child.Header.ParentSession
	origin := child.Header.Origin
	child.mu.Unlock()
	if parent != parentID || origin != "subagent" {
		return nil, rpcError("subagent-unauthorized", "subagent does not belong to this parent", map[string]any{"childSessionId": childID})
	}
	return child, nil
}

func (e *Engine) subagentList(p map[string]any) (any, *RPCError) {
	parentID, _ := p["parentSessionId"].(string)
	if _, err := e.getSession(parentID); err != nil {
		return nil, rpcError("subagent-parent-unavailable", "parent session is unavailable", map[string]any{"parentSessionId": parentID})
	}
	// Snapshot the registry before taking any Session locks. Other state paths
	// (notably persistence) use Engine -> Session ordering; keeping the Engine
	// lock out of the per-session reads prevents lock inversion as those paths
	// run concurrently.
	e.mu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, candidate := range e.sessions {
		sessions = append(sessions, candidate)
	}
	e.mu.RUnlock()
	children := make([]*Session, 0)
	for _, candidate := range sessions {
		candidate.mu.Lock()
		match := candidate.Header.ParentSession == parentID && candidate.Header.Origin == "subagent"
		candidate.mu.Unlock()
		if match {
			children = append(children, candidate)
		}
	}
	entries := make([]map[string]any, 0, len(children))
	for _, child := range children {
		child.mu.Lock()
		id, running, mode, label := child.Header.ID, child.Running, child.Header.Mode, child.Title
		if mode == "" {
			mode = "continuable"
		}
		child.mu.Unlock()
		hasChildren := false
		for _, candidate := range sessions {
			candidate.mu.Lock()
			if candidate.Header.ParentSession == id && candidate.Header.Origin == "subagent" {
				hasChildren = true
			}
			candidate.mu.Unlock()
			if hasChildren {
				break
			}
		}
		entry := map[string]any{"kind": "child", "id": id, "activity": "inactive", "mode": mode, "hasChildren": hasChildren}
		if running {
			entry["activity"] = "running"
		}
		if mode == "continuable" {
			entry["label"] = label
		} else if label != "" {
			entry["label"] = label
		}
		entries = append(entries, entry)
	}
	return map[string]any{"entries": entries, "parentAvailable": true}, nil
}

// CreateSubagent creates a durable continuable child owned by parentID.
// It is the library counterpart of the subagent creation primitive used by
// the runtime; callers can then use subagent.list/history/prompt/interrupt or
// the regular Engine APIs subject to the child-session ownership fence.
func (e *Engine) CreateSubagent(ctx context.Context, parentID, id, preset string) (string, error) {
	return e.createSubagent(ctx, parentID, id, preset)
}

func (e *Engine) subagentHistory(p map[string]any) (any, *RPCError) {
	parentID, _ := p["parentSessionId"].(string)
	childID, _ := p["childSessionId"].(string)
	child, err := e.childFor(parentID, childID)
	if err != nil {
		return nil, err
	}
	child.mu.Lock()
	mode := child.Header.Mode
	child.mu.Unlock()
	if requested, _ := p["mode"].(string); requested != "" && requested != mode {
		return nil, rpcError("subagent-unauthorized", "subagent mode does not match", map[string]any{"childSessionId": childID})
	}
	before := -1
	if value, ok := p["beforeSeq"].(float64); ok {
		before = int(value)
	}
	max := 50
	if value, ok := p["maxMessages"].(float64); ok {
		max = int(value)
	}
	items, more, historyErr := e.History(childID, before, max)
	if historyErr != nil {
		return nil, errorToRPC(historyErr)
	}
	return map[string]any{"events": items, "hasMore": more}, nil
}

func (e *Engine) subagentPrompt(ctx context.Context, p map[string]any, rpcIDs ...string) (any, *RPCError) {
	parentID, _ := p["parentSessionId"].(string)
	childID, _ := p["childSessionId"].(string)
	child, err := e.childFor(parentID, childID)
	if err != nil {
		return nil, err
	}
	child.mu.Lock()
	mode := child.Header.Mode
	child.mu.Unlock()
	if mode != "continuable" {
		return nil, rpcError("subagent-not-resumable", "subagent cannot be resumed", map[string]any{"childSessionId": childID})
	}
	var content []PromptContentPart
	if raw, ok := p["content"].([]any); ok {
		for _, item := range raw {
			block, ok := item.(map[string]any)
			if !ok {
				return nil, rpcError("bad-request", "content block must be an object", nil)
			}
			typ, _ := block["type"].(string)
			part := PromptContentPart{Type: typ}
			part.Text, _ = block["text"].(string)
			part.MediaType, _ = block["mediaType"].(string)
			part.Data, _ = block["data"].(string)
			part.Name, _ = block["name"].(string)
			content = append(content, part)
		}
	}
	if len(content) == 0 {
		return nil, rpcError("bad-request", "content is required", nil)
	}
	clientTimeZone := ""
	if raw, present := p["clientTimeZone"]; present {
		value, ok := raw.(string)
		if !ok {
			return nil, rpcError("bad-request", "clientTimeZone must be a string", nil)
		}
		canonical, ok := canonicalClientTimeZone(value)
		if !ok {
			return nil, errorToRPC(&ClientTimeZoneError{Value: value})
		}
		clientTimeZone = canonical
	}
	job, _, enqueueErr := e.enqueuePrompt(ctx, childID, PromptRequest{
		SessionID: childID, Mode: "queue", Content: content, ClientTimeZone: clientTimeZone, RPCID: firstRPCID(rpcIDs),
		Source: map[string]any{"kind": "coordinator", "form": "relay", "senderSessionId": parentID},
	}, false)
	if enqueueErr != nil {
		return nil, errorToRPC(enqueueErr)
	}
	return map[string]any{"messageId": job.id}, nil
}

func (e *Engine) subagentInterrupt(p map[string]any) (any, *RPCError) {
	parentID, _ := p["parentSessionId"].(string)
	childID, _ := p["childSessionId"].(string)
	if _, err := e.childFor(parentID, childID); err != nil {
		return nil, err
	}
	if err := e.CancelSession(childID); err != nil {
		return nil, errorToRPC(err)
	}
	return map[string]any{"accepted": true}, nil
}

func isSubagentSession(s *Session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Header.Origin == "subagent"
}

func ordinarySessionError(s *Session) *RPCError {
	s.mu.Lock()
	id := s.Header.ID
	s.mu.Unlock()
	return rpcError("agent-busy", "use subagent delivery for this child session", map[string]any{"sessionId": id})
}

func (e *Engine) requireOrdinary(id string) (*Session, *RPCError) {
	s, err := e.getSession(id)
	if err != nil {
		return nil, errorToRPC(err)
	}
	if isSubagentSession(s) {
		return nil, ordinarySessionError(s)
	}
	return s, nil
}
