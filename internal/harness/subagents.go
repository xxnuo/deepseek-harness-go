package harness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
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

func (e *Engine) subagentList(ctx context.Context, p map[string]any) (any, *RPCError) {
	parentID, _ := p["parentSessionId"].(string)
	entries, parentAvailable, err := e.listSubagentEntries(ctx, parentID, false)
	if err != nil {
		return nil, subagentListingError(err)
	}
	values := make([]map[string]any, 0, len(entries))
	for _, row := range entries {
		if row.Kind == "diagnostic" {
			values = append(values, map[string]any{"kind": "diagnostic", "id": row.ID, "reason": row.Reason})
			continue
		}
		entry := map[string]any{
			"kind": "child", "id": row.ID, "activity": row.Activity,
			"mode": row.Mode, "hasChildren": row.HasChildren,
		}
		if row.Mode == "continuable" || row.Label != "" {
			entry["label"] = row.Label
		}
		values = append(values, entry)
	}
	return map[string]any{"entries": values, "parentAvailable": parentAvailable}, nil
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

	type activationDrain struct {
		activation *modelSubagentActivation
		owner      bool
		done       <-chan struct{}
	}
	branches := make([]*Session, 0, len(selected))
	activations := make([]activationDrain, 0, len(selected))
	var failures []error
	defer func() {
		for _, child := range branches {
			child.mu.Lock()
			child.draining = false
			child.mu.Unlock()
		}
	}()
	schedule := func(child *Session) {
		child.mu.Lock()
		childID := child.Header.ID
		child.mu.Unlock()
		e.modelSubagentMu.Lock()
		activation := e.modelSubagentActivations[childID]
		e.modelSubagentMu.Unlock()
		if activation != nil && activation.session == child {
			owner, done := e.beginModelSubagentDisposal(activation)
			activations = append(activations, activationDrain{activation: activation, owner: owner, done: done})
			return
		}
		if err := e.beginSubagentDrain(child); err != nil {
			failures = append(failures, err)
			return
		}
		branches = append(branches, child)
	}
	for _, child := range selected {
		schedule(child)
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
			schedule(child)
		}
	}
	activationResults := make(chan error, len(activations))
	for _, item := range activations {
		item := item
		go func() {
			if item.owner {
				activationResults <- e.finishModelSubagentActivation(item.activation)
				return
			}
			<-item.done
			e.modelSubagentMu.Lock()
			err := item.activation.finishErr
			e.modelSubagentMu.Unlock()
			activationResults <- err
		}()
	}
	for _, child := range branches {
		child.mu.Lock()
		id := child.Header.ID
		child.mu.Unlock()
		if err := e.WaitForIdle(ctx, id); err != nil {
			failures = append(failures, err)
			break
		}
	}
	for index := len(branches) - 1; index >= 0; index-- {
		branches[index].mu.Lock()
		id := branches[index].Header.ID
		branches[index].mu.Unlock()
		if err := detachSDKSession(e, id); err != nil {
			failures = append(failures, err)
		}
	}
	for range activations {
		select {
		case err := <-activationResults:
			if err != nil {
				failures = append(failures, err)
			}
		case <-ctx.Done():
			failures = append(failures, ctx.Err())
			return errors.Join(failures...)
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
			"target": "next-turn", "start": 0, "removedCount": len(session.pending), "inserted": []any{}, "outcome": "canceled",
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
			"target": "next-step", "start": 0, "removedCount": len(session.steering), "inserted": []any{}, "outcome": "canceled",
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
	id, activity, cancel, maintenanceCancel := session.Header.ID, session.activity, session.Cancel, session.maintenanceCancel
	session.activity = nil
	session.Cancel = nil
	session.mu.Unlock()
	for _, event := range events {
		e.publishEvent(id, event)
	}
	if len(events) > 0 {
		e.emitQueue(session)
	}
	if activity != nil {
		activity.cancel(&agentCancelError{cause: AgentCancelCause{Kind: "disposed"}})
	} else if cancel != nil {
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

// SendAdjacentAgentMessage steers a model-authored message across one exact
// continuable parent-child edge. Direct children may be cold-resumed; a child
// sender must be the resident activation and its direct parent must be live.
func (e *Engine) SendAdjacentAgentMessage(ctx context.Context, senderID, targetID string, content []ContentBlock) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if senderID == "" || targetID == "" || senderID == targetID {
		return "", errors.New("subagent-unauthorized: send_message requires two distinct adjacent agents")
	}
	sender, err := e.getSession(senderID)
	if err != nil {
		return "", errors.New("subagent-unauthorized: send_message requires an exact live sender")
	}
	sender.mu.Lock()
	header, attached, draining := sender.Header, sender.attached, sender.draining
	sender.mu.Unlock()
	if !attached || draining {
		return "", errors.New("subagent-unauthorized: send_message requires an exact live sender")
	}
	framed := append([]ContentBlock{{Type: "text", Text: "Agent " + senderID + " sent a message: "}}, cloneContentBlocks(content)...)
	source := map[string]any{"kind": "agent-message", "form": "relay", "senderSessionId": senderID}

	if _, rpcErr := e.childFor(senderID, targetID); rpcErr == nil {
		return e.deliverContinuableModelSubagent(ctx, senderID, targetID, framed, source, "next-step")
	}
	if header.Origin != "subagent" || header.Mode != "continuable" || header.ParentSession != targetID {
		return "", errors.New("subagent-unauthorized: target is not the sender's direct parent or direct continuable child")
	}
	e.modelSubagentMu.Lock()
	activation := e.modelSubagentActivations[senderID]
	authorized := activation != nil && activation.session == sender && !activation.disposing
	targetActivation := e.modelSubagentActivations[targetID]
	targetDisposing := targetActivation != nil && targetActivation.disposing
	e.modelSubagentMu.Unlock()
	if !authorized {
		return "", errors.New("subagent-unauthorized: parent delivery requires the exact resident continuable child")
	}
	parent, parentErr := e.getSession(targetID)
	if parentErr != nil || targetDisposing {
		return "", errors.New("subagent-parent-unavailable: direct parent is not live")
	}
	parent.mu.Lock()
	parentAvailable := parent.attached && !parent.draining
	parent.mu.Unlock()
	if !parentAvailable {
		return "", errors.New("subagent-parent-unavailable: direct parent is not live")
	}
	return e.enqueueTeamPrompt(parent, framed, source, "next-step", true)
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
	return e.subagentPromptWithSource(ctx, p, "coordinator", rpcIDs...)
}

// subagentPromptWithSource is shared by the trusted model-facing primitive and
// the browser Remote adapter. The adapter supplies the durable source kind;
// the model-facing path remains coordinator/relay for compatibility.
func (e *Engine) subagentPromptWithSource(ctx context.Context, p map[string]any, sourceKind string, rpcIDs ...string) (any, *RPCError) {
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
			return nil, rpcError("subagent/invalid-time-zone", (&ClientTimeZoneError{Value: value}).Error(), map[string]any{"value": value})
		}
		clientTimeZone = canonical
	}
	prepared, prepareErr := e.durablePromptContentContext(ctx, content)
	if prepareErr != nil {
		return nil, subagentPromptError(prepareErr, parentID, childID)
	}
	source := map[string]any{"kind": sourceKind}
	if sourceKind == "coordinator" {
		source["form"] = "relay"
		source["senderSessionId"] = parentID
	}
	if clientTimeZone != "" {
		source["clientTimeZone"] = clientTimeZone
	}
	if rpcID := firstRPCID(rpcIDs); rpcID != "" {
		source["rpcId"] = rpcID
	}
	messageID, enqueueErr := e.promptContinuableModelSubagent(ctx, parentID, childID, prepared, source)
	if enqueueErr != nil {
		return nil, subagentPromptError(enqueueErr, parentID, childID)
	}
	return map[string]any{"messageId": messageID}, nil
}

func subagentPromptError(err error, parentID, childID string) *RPCError {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return rpcError("cancelled", "subagent prompt was cancelled", nil)
	}
	var imageErr *imageAdmissionFailure
	if errors.As(err, &imageErr) {
		reason := imageErr.reason
		if reason == "" {
			reason = imageAdmissionReason(err)
		}
		return rpcError("subagent-attachment-invalid", imageErr.Error(), map[string]any{"reason": reason})
	}
	var providerErr *ProviderError
	if errors.As(err, &providerErr) && providerErr.Code == "UNSUPPORTED_CONTENT" && strings.Contains(strings.ToLower(providerErr.Error()), "image") {
		return rpcError("subagent-attachment-invalid", providerErr.Error(), map[string]any{"reason": "MODEL_DOES_NOT_SUPPORT_IMAGES"})
	}
	mapped := errorToRPC(err)
	switch mapped.Code {
	case "subagent/not-resumable", "subagent/unauthorized", "subagent/not-found", "subagent/catalog-diagnostic", "subagent/parent-unavailable", "subagent/delivery-unavailable":
		mapped = withSubagentAddressDetails(mapped, parentID, childID)
	case "subagent/attachment-invalid":
		if details, ok := mapped.Details.(map[string]any); !ok || details["reason"] == nil {
			mapped = rpcWithDetails(mapped, map[string]any{"reason": imageAdmissionReason(err)})
		}
	}
	return mapped
}

func imageAdmissionReason(err error) string {
	var marked *imageAdmissionFailure
	if errors.As(err, &marked) && marked.reason != "" {
		return marked.reason
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "canonical base64"):
		return "INVALID_IMAGE_BASE64"
	case strings.Contains(message, "image-count limit"):
		return "TOO_MANY_IMAGES"
	case strings.Contains(message, "does not support image"), strings.Contains(message, "accept image"):
		return "MODEL_DOES_NOT_SUPPORT_IMAGES"
	default:
		return "IMAGE_INVALID"
	}
}

func (e *Engine) subagentInterrupt(p map[string]any) (any, *RPCError) {
	parentID, _ := p["parentSessionId"].(string)
	childID, _ := p["childSessionId"].(string)
	if rawMode, present := p["mode"]; present {
		mode, ok := rawMode.(string)
		if !ok || mode != "continuable" {
			return nil, rpcError("bad-request", "invalid payload for subagent.interrupt", map[string]any{})
		}
	}
	e.modelSubagentMu.Lock()
	activation := e.modelSubagentActivations[childID]
	e.modelSubagentMu.Unlock()
	if activation == nil || activation.disposing {
		return map[string]any{"accepted": true}, nil
	}
	if activation.parentID != parentID {
		return nil, rpcError("subagent-unauthorized", "subagent does not belong to this parent", map[string]any{"childSessionId": childID})
	}
	if err := e.CancelAgent(childID, AgentCancelCause{Kind: "user"}, CancelAgentOptions{KeepInbox: true, ParkInbox: true}); err != nil {
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
	return rpcError("agent-busy", "use subagent delivery for this child session", map[string]any{
		"reason": "use subagent delivery for this child session",
	})
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

func (e *Engine) requireQueueMutable(id string) (*Session, *RPCError) {
	s, err := e.getSession(id)
	if err != nil {
		return nil, errorToRPC(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Header.Origin != "subagent" {
		return s, nil
	}
	for _, event := range s.Events {
		if event.Type != "subagent/descriptor" {
			continue
		}
		descriptor, supported, parseErr := parseSubagentDescriptor(event.Data)
		if parseErr == nil && supported && descriptor.Mode == "continuable" && int(event.Seq) >= int(s.InheritedEventCount) {
			return s, nil
		}
		break
	}
	return nil, ordinarySessionError(s)
}
