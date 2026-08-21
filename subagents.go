package harness

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

const (
	SubagentReportQuiet    = "quiet"
	SubagentReportNextStep = "next-step"
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

// DrainSubagentChildren releases selected resident direct children and their
// resident descendants without touching their siblings.
func (e *Engine) DrainSubagentChildren(ctx context.Context, parentID string, childIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	parent, err := e.getSession(parentID)
	if err != nil {
		return errors.New("subagent-unauthorized: selected child teardown requires a live parent session")
	}
	parent.mu.Lock()
	parentAvailable := parent.attached && !parent.draining
	parent.mu.Unlock()
	if !parentAvailable {
		return errors.New("subagent-unauthorized: selected child teardown requires a live parent session")
	}

	selected := make([]*Session, 0, len(childIDs))
	seen := map[string]bool{}
	for _, childID := range childIDs {
		if childID == "" || seen[childID] {
			continue
		}
		seen[childID] = true
		child, childErr := e.getSession(childID)
		if childErr != nil {
			continue
		}
		child.mu.Lock()
		attached, origin, mode, directParent := child.attached, child.Header.Origin, child.Header.Mode, child.Header.ParentSession
		child.mu.Unlock()
		if !attached {
			continue
		}
		if origin != "subagent" || mode != "continuable" || directParent != parentID {
			return fmt.Errorf("subagent-unauthorized: subagent %q is not a direct child of session %q", childID, parentID)
		}
		selected = append(selected, child)
	}

	branches := make([]*Session, 0, len(selected))
	defer func() {
		for _, child := range branches {
			child.mu.Lock()
			child.draining = false
			child.mu.Unlock()
		}
	}()
	for _, child := range selected {
		if err := e.beginSubagentDrain(child); err != nil {
			return err
		}
		branches = append(branches, child)
	}
	for index := 0; index < len(branches); index++ {
		parent := branches[index]
		parent.mu.Lock()
		id := parent.Header.ID
		parent.mu.Unlock()
		for _, child := range e.residentDirectSubagentChildren(id) {
			child.mu.Lock()
			childID := child.Header.ID
			child.mu.Unlock()
			if seen[childID] {
				continue
			}
			seen[childID] = true
			if err := e.beginSubagentDrain(child); err != nil {
				return err
			}
			branches = append(branches, child)
		}
	}
	for _, child := range branches {
		child.mu.Lock()
		id := child.Header.ID
		child.mu.Unlock()
		if err := e.WaitForIdle(ctx, id); err != nil {
			return err
		}
	}
	var failures []error
	for index := len(branches) - 1; index >= 0; index-- {
		branches[index].mu.Lock()
		id := branches[index].Header.ID
		branches[index].mu.Unlock()
		if err := detachSDKSession(e, id); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (e *Engine) residentDirectSubagentChildren(parentID string) []*Session {
	e.mu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.mu.RUnlock()
	children := make([]*Session, 0)
	for _, session := range sessions {
		session.mu.Lock()
		match := session.attached && session.Header.Origin == "subagent" && session.Header.Mode == "continuable" && session.Header.ParentSession == parentID
		session.mu.Unlock()
		if match {
			children = append(children, session)
		}
	}
	sort.Slice(children, func(i, j int) bool {
		children[i].mu.Lock()
		left := children[i].Header.ID
		children[i].mu.Unlock()
		children[j].mu.Lock()
		right := children[j].Header.ID
		children[j].mu.Unlock()
		return left < right
	})
	return children
}

func (e *Engine) beginSubagentDrain(session *Session) error {
	session.mu.Lock()
	if !session.attached || session.draining {
		session.mu.Unlock()
		return nil
	}
	session.draining = true
	var events []Event
	if len(session.pending) > 0 {
		event, err := appendEventLocked(session, "agent/inbox/spliced", map[string]any{
			"target": "next-turn", "start": 0, "removedCount": len(session.pending), "inserted": []any{},
		}, nil, nil, false)
		if err != nil {
			session.draining = false
			session.mu.Unlock()
			return err
		}
		events = append(events, event)
	}
	if len(session.steering) > 0 {
		event, err := appendEventLocked(session, "agent/inbox/spliced", map[string]any{
			"target": "next-step", "start": 0, "removedCount": len(session.steering), "inserted": []any{},
		}, nil, nil, false)
		if err != nil {
			session.draining = false
			session.mu.Unlock()
			return err
		}
		events = append(events, event)
	}
	pending := append(append([]*queuedPrompt(nil), session.pending...), session.steering...)
	session.pending, session.steering = nil, nil
	id, cancel, maintenanceCancel := session.Header.ID, session.Cancel, session.maintenanceCancel
	session.mu.Unlock()
	for _, event := range events {
		e.publishEvent(id, event)
	}
	if len(events) > 0 {
		e.emitQueue(session)
	}
	if cancel != nil {
		cancel()
	}
	if maintenanceCancel != nil {
		maintenanceCancel()
	}
	for _, item := range pending {
		if item.done != nil {
			select {
			case item.done <- promptOutcome{err: errors.New("subagent session is being released")}:
			default:
			}
		}
	}
	return nil
}

// ReportFromSubagent delivers selected content from one resident continuable
// child to its direct parent. next-step wakes an idle parent; quiet does not.
func (e *Engine) ReportFromSubagent(ctx context.Context, childID string, content []ContentBlock, delivery string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if delivery == "" {
		delivery = e.cfg.SubagentReportDelivery
	}
	if delivery != SubagentReportQuiet && delivery != SubagentReportNextStep {
		return "", fmt.Errorf("subagent report delivery must be quiet or next-step, got %q", delivery)
	}
	child, err := e.getSession(childID)
	if err != nil {
		return "", err
	}
	child.mu.Lock()
	header, attached, draining := child.Header, child.attached, child.draining
	child.mu.Unlock()
	if !attached || draining || header.Origin != "subagent" || header.Mode != "continuable" || header.ParentSession == "" {
		return "", errors.New("subagent-unauthorized: reporting requires a resident continuable child")
	}
	parent, err := e.getSession(header.ParentSession)
	if err != nil {
		return "", errors.New("subagent-parent-unavailable: direct parent is not live")
	}
	parent.mu.Lock()
	parentAttached := parent.attached
	parent.mu.Unlock()
	if !parentAttached {
		return "", errors.New("subagent-parent-unavailable: direct parent is not live")
	}
	framed := append([]ContentBlock{{Type: "text", Text: "Background subagent " + childID + " reported:"}}, cloneContentBlocks(content)...)
	return e.enqueueTeamPrompt(parent, framed, map[string]any{
		"kind": "subagent-report", "form": "relay", "senderSessionId": childID,
	}, "next-step", delivery == SubagentReportNextStep)
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
