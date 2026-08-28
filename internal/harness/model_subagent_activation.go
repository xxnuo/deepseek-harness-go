package harness

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// modelSubagentActivation is one resident epoch of a durable continuable child.
// The Session remains durable after this epoch is detached and can be resumed.
type modelSubagentActivation struct {
	childID   string
	parentID  string
	provider  string
	runID     string
	session   *Session
	startSeq  int
	announced bool
	disposing bool
	finishErr error
	owned     map[string]struct{}
	changed   chan struct{}
	done      chan struct{}
}

func (e *Engine) lockModelSubagent(id string) func() {
	value, _ := e.modelSubagentLocks.LoadOrStore(id, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (e *Engine) requireContinuablePersistence() error {
	if e.sessionStore == nil {
		return errors.New("continuable subagents require session persistence")
	}
	return nil
}

func signalModelSubagentActivationLocked(activation *modelSubagentActivation) {
	close(activation.changed)
	activation.changed = make(chan struct{})
}

func (e *Engine) registerModelSubagentActivation(child *Session, parentID string) (*modelSubagentActivation, error) {
	child.mu.Lock()
	childID, startSeq, attached := child.Header.ID, len(child.Events), child.attached
	seedLength := child.Header.SeedLength
	events := append([]Event(nil), child.Events...)
	child.mu.Unlock()
	if !attached {
		return nil, errors.New("subagent activation requires a resident child")
	}

	var descriptor *SubagentDescriptorData
	var descriptorErr error
	if seedLength >= 0 && seedLength <= len(events) {
		descriptor, descriptorErr = FoldSubagentDescriptor(events[seedLength:])
	}
	if descriptorErr != nil || descriptor == nil || descriptor.Mode != "continuable" {
		return nil, errors.New("subagent activation requires a valid continuable descriptor")
	}
	provider := descriptor.Provider

	e.modelSubagentMu.Lock()
	if current := e.modelSubagentActivations[childID]; current != nil {
		e.modelSubagentMu.Unlock()
		return nil, fmt.Errorf("subagent %q already has a resident activation", childID)
	}
	activation := &modelSubagentActivation{
		childID: childID, parentID: parentID, provider: provider, runID: newRunID(), session: child, startSeq: startSeq,
		owned: map[string]struct{}{}, changed: make(chan struct{}), done: make(chan struct{}),
	}
	if parent := e.modelSubagentActivations[parentID]; parent != nil {
		if parent.disposing {
			e.modelSubagentMu.Unlock()
			return nil, fmt.Errorf("subagent parent %q is being disposed", parentID)
		}
		parent.owned[childID] = struct{}{}
		signalModelSubagentActivationLocked(parent)
	}
	e.modelSubagentActivations[childID] = activation
	e.modelSubagentMu.Unlock()
	e.emitDynamicCordisScopedContained(parentID, "subagent/start", map[string]any{
		"runId": activation.runID, "provider": provider, "id": childID, "local": true,
	})
	return activation, nil
}

func (e *Engine) releaseModelSubagentOwnership(activation *modelSubagentActivation) {
	e.modelSubagentMu.Lock()
	if parent := e.modelSubagentActivations[activation.parentID]; parent != nil {
		if _, present := parent.owned[activation.childID]; present {
			delete(parent.owned, activation.childID)
			signalModelSubagentActivationLocked(parent)
		}
	}
	e.modelSubagentMu.Unlock()
}

func (e *Engine) markModelSubagentAnnounced(activation *modelSubagentActivation) {
	e.modelSubagentMu.Lock()
	if e.modelSubagentActivations[activation.childID] == activation {
		activation.announced = true
		signalModelSubagentActivationLocked(activation)
	}
	e.modelSubagentMu.Unlock()
}

func (e *Engine) startContinuableModelSubagent(ctx context.Context, parentID, label, prompt string, fork bool, config SubagentToolConfig) (string, string, error) {
	return e.startContinuableModelSubagentWithID(ctx, parentID, newID("ses"), label, []ContentBlock{{Type: "text", Text: prompt}}, fork, config)
}

func (e *Engine) startContinuableModelSubagentWithID(ctx context.Context, parentID, childID, label string, content []ContentBlock, fork bool, config SubagentToolConfig) (string, string, error) {
	if err := e.requireContinuablePersistence(); err != nil {
		return "", "", err
	}
	unlock := e.lockModelSubagent(childID)
	defer unlock()

	childID, err := e.createModelSubagentWithIDLocked(ctx, parentID, childID, label, fork, "continuable", config, nil)
	if err != nil {
		return "", "", err
	}
	child, _ := e.getSession(childID)
	if err := e.prepareModelSubagentActivationSetup(child); err != nil {
		e.rollbackCreatedModelSubagent(childID, child)
		return "", "", err
	}
	activation, err := e.registerModelSubagentActivation(child, parentID)
	if err != nil {
		e.rollbackCreatedModelSubagent(childID, child)
		return "", "", err
	}
	if err := ctx.Err(); err != nil {
		e.removeModelSubagentActivation(activation)
		e.rollbackCreatedModelSubagent(childID, child)
		return "", "", err
	}
	messageID, err := e.enqueueTeamPrompt(child, cloneContentBlocks(content), map[string]any{"kind": "user"}, "next-turn", true)
	if err != nil {
		e.removeModelSubagentActivation(activation)
		e.rollbackCreatedModelSubagent(childID, child)
		return "", "", err
	}
	e.markModelSubagentAnnounced(activation)
	e.watchModelSubagentActivation(activation)
	return childID, messageID, nil
}

func (e *Engine) removeModelSubagentActivation(activation *modelSubagentActivation) {
	e.modelSubagentMu.Lock()
	if e.modelSubagentActivations[activation.childID] == activation {
		delete(e.modelSubagentActivations, activation.childID)
		close(activation.done)
	}
	e.modelSubagentMu.Unlock()
	e.releaseModelSubagentOwnership(activation)
	e.emitModelSubagentActivationEnd(activation, "error", nil)
}

func (e *Engine) promptContinuableModelSubagent(ctx context.Context, parentID, childID string, content []ContentBlock, source map[string]any) (string, error) {
	if err := e.requireContinuablePersistence(); err != nil {
		return "", err
	}
	for {
		unlock := e.lockModelSubagent(childID)
		child, err := e.childFor(parentID, childID)
		if err != nil {
			unlock()
			return "", err
		}

		e.modelSubagentMu.Lock()
		activation := e.modelSubagentActivations[childID]
		if activation != nil && activation.disposing {
			done := activation.done
			e.modelSubagentMu.Unlock()
			unlock()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-done:
				continue
			}
		}
		e.modelSubagentMu.Unlock()

		attachedNow := false
		newActivation := false
		if activation == nil {
			child.mu.Lock()
			header, attached := child.Header, child.attached
			events := append([]Event(nil), child.Events...)
			child.mu.Unlock()
			if header.SeedLength < 0 || header.SeedLength > len(events) {
				unlock()
				return "", errors.New("subagent-not-resumable: child has invalid continuation lineage")
			}
			descriptor, descriptorErr := FoldSubagentDescriptor(events[header.SeedLength:])
			if descriptorErr != nil || descriptor == nil || descriptor.Mode != "continuable" {
				unlock()
				return "", errors.New("subagent-not-resumable: child has no supported continuation state")
			}
			if !attached {
				if _, err := e.createSession(ctx, header, false); err != nil {
					unlock()
					return "", fmt.Errorf("subagent-not-resumable: %w", err)
				}
				attachedNow = true
			}
			if attachedNow {
				if err := e.restoreContinuableSubagentComposition(child, descriptor); err != nil {
					_ = detachSDKSession(e, childID)
					unlock()
					return "", fmt.Errorf("subagent-not-resumable: %w", err)
				}
			}
			if err := e.prepareModelSubagentActivationSetup(child); err != nil {
				if attachedNow {
					_ = detachSDKSession(e, childID)
				}
				unlock()
				return "", fmt.Errorf("subagent-not-resumable: %w", err)
			}
			var activationErr error
			activation, activationErr = e.registerModelSubagentActivation(child, parentID)
			if activationErr != nil {
				if attachedNow {
					_ = detachSDKSession(e, childID)
				} else {
					_ = e.subagentActivationSetups.releaseChild(child)
				}
				unlock()
				return "", activationErr
			}
			newActivation = true
		}

		if enqueueErr := ctx.Err(); enqueueErr != nil {
			if newActivation {
				e.removeModelSubagentActivation(activation)
				if attachedNow {
					_ = detachSDKSession(e, childID)
				} else {
					_ = e.subagentActivationSetups.releaseChild(child)
				}
			}
			unlock()
			return "", enqueueErr
		}
		messageID, enqueueErr := e.enqueueTeamPrompt(child, cloneContentBlocks(content), cloneJSON(source).(map[string]any), "next-turn", true)
		if enqueueErr != nil {
			if newActivation {
				e.removeModelSubagentActivation(activation)
				if attachedNow {
					_ = detachSDKSession(e, childID)
				} else {
					_ = e.subagentActivationSetups.releaseChild(child)
				}
			}
			unlock()
			return "", enqueueErr
		}
		e.markModelSubagentAnnounced(activation)
		e.modelSubagentMu.Lock()
		signalModelSubagentActivationLocked(activation)
		e.modelSubagentMu.Unlock()
		if newActivation {
			e.watchModelSubagentActivation(activation)
		}
		unlock()
		return messageID, nil
	}
}

func (e *Engine) prepareModelSubagentActivationSetup(child *Session) error {
	transaction, err := e.subagentActivationSetups.apply(child)
	if err != nil {
		return err
	}
	if err := transaction.commit(); err != nil {
		_ = e.subagentActivationSetups.releaseChild(child)
		return err
	}
	return nil
}

func (e *Engine) watchModelSubagentActivation(activation *modelSubagentActivation) {
	go func() {
		for {
			if err := e.WaitForIdle(context.Background(), activation.childID); err != nil {
				return
			}
			unlock := e.lockModelSubagent(activation.childID)
			e.modelSubagentMu.Lock()
			current := e.modelSubagentActivations[activation.childID]
			if current != activation || activation.disposing {
				e.modelSubagentMu.Unlock()
				unlock()
				return
			}
			activation.session.mu.Lock()
			running := activation.session.Running || activation.session.maintenance
			queued := activation.session.claimed != nil || len(activation.session.pending) > 0 || len(activation.session.steering) > 0
			activation.session.mu.Unlock()
			if !running && !queued && len(activation.owned) == 0 && activation.announced {
				activation.disposing = true
				e.modelSubagentMu.Unlock()
				unlock()
				_ = e.finishModelSubagentActivation(activation)
				return
			}
			changed := activation.changed
			e.modelSubagentMu.Unlock()
			unlock()
			if running {
				continue
			}
			<-changed
		}
	}()
}

func (e *Engine) beginModelSubagentDisposal(activation *modelSubagentActivation) (bool, <-chan struct{}) {
	unlock := e.lockModelSubagent(activation.childID)
	defer unlock()
	e.modelSubagentMu.Lock()
	defer e.modelSubagentMu.Unlock()
	if e.modelSubagentActivations[activation.childID] != activation {
		return false, activation.done
	}
	if activation.disposing {
		return false, activation.done
	}
	activation.disposing = true
	signalModelSubagentActivationLocked(activation)
	return true, activation.done
}

func (e *Engine) finishModelSubagentActivation(activation *modelSubagentActivation) error {
	_ = e.CancelAgent(activation.childID, AgentCancelCause{Kind: "parent"}, CancelAgentOptions{KeepInbox: false})

	e.modelSubagentMu.Lock()
	children := make([]*modelSubagentActivation, 0, len(activation.owned))
	for childID := range activation.owned {
		if child := e.modelSubagentActivations[childID]; child != nil {
			children = append(children, child)
		}
	}
	e.modelSubagentMu.Unlock()
	failures := e.finishModelSubagentBranches(children)
	if err := e.WaitForIdle(context.Background(), activation.childID); err != nil {
		failures = append(failures, err)
	}
	if flusher, ok := e.sessionStore.(SessionPersistenceFlusher); ok {
		_ = flusher.Flush(context.Background(), activation.childID)
	}
	reason, output := e.modelSubagentActivationTerminal(activation)
	if err := detachSDKSession(e, activation.childID); err != nil {
		failures = append(failures, err)
	}
	if len(failures) > 0 {
		reason, output = "error", nil
	}

	e.modelSubagentMu.Lock()
	if e.modelSubagentActivations[activation.childID] == activation {
		delete(e.modelSubagentActivations, activation.childID)
	}
	announced := activation.announced
	e.modelSubagentMu.Unlock()
	if announced {
		e.notifyModelSubagentSettlement(activation.parentID, activation.childID, output, reason)
	}
	e.releaseModelSubagentOwnership(activation)
	e.emitModelSubagentActivationEnd(activation, reason, output)
	e.modelSubagentMu.Lock()
	select {
	case <-activation.done:
	default:
		activation.finishErr = errors.Join(failures...)
		close(activation.done)
	}
	e.modelSubagentMu.Unlock()
	return activation.finishErr
}

func (e *Engine) emitModelSubagentActivationEnd(activation *modelSubagentActivation, reason string, output []ContentBlock) {
	terminal := map[string]any{
		"runId": activation.runID, "provider": activation.provider, "id": activation.childID,
		"local": true, "stopReason": reason,
	}
	if len(output) > 0 {
		terminal["lastAssistantMessage"] = cloneContentBlocks(output)
	}
	e.emitDynamicCordisScopedContained(activation.parentID, "subagent/end", terminal)
}

func (e *Engine) modelSubagentActivationTerminal(activation *modelSubagentActivation) (string, []ContentBlock) {
	activation.session.mu.Lock()
	events := append([]Event(nil), activation.session.Events...)
	activation.session.mu.Unlock()
	own := events[activation.startSeq:]
	end, droppedUnrun := foldModelSubagentConsumedWork(own)
	reason := "completed"
	if end != nil {
		data, _ := end.Data.(map[string]any)
		value, _ := data["reason"].(map[string]any)
		kind, _ := value["kind"].(string)
		switch kind {
		case "completed":
			if droppedUnrun {
				reason = "aborted"
			}
		case "aborted", "interrupted":
			reason = "aborted"
		case "blocked", "rejected":
			reason = "refusal"
		case "error", "max-tokens":
			reason = kind
		default:
			reason = "error"
		}
	} else if droppedUnrun {
		reason = "aborted"
	}
	return reason, finalAssistantOutput(own)
}

func foldModelSubagentConsumedWork(events []Event) (*Event, bool) {
	stepped := map[int]bool{}
	claimed := map[int]bool{}
	open := 0
	pendingClaim := false
	var end *Event
	droppedUnrun := false
	for _, event := range events {
		switch event.Type {
		case "turn/start":
			open, _ = eventTurn(event.Data)
			if open != 0 && pendingClaim {
				claimed[open] = true
				pendingClaim = false
			}
		case "step/start":
			if turn, ok := eventTurn(event.Data); ok {
				stepped[turn] = true
			}
		case "agent/inbox/spliced":
			data, _ := event.Data.(map[string]any)
			if _, present := data["removedCount"]; !present {
				continue
			}
			if data["outcome"] == "canceled" {
				if inserted, ok := data["inserted"]; ok {
					value := reflect.ValueOf(inserted)
					if value.IsValid() && (value.Kind() == reflect.Array || value.Kind() == reflect.Slice) && value.Len() == 0 {
						droppedUnrun = true
					}
				}
			} else if open != 0 {
				claimed[open] = true
			} else {
				// Go synchronously claims the first queued prompt before launching
				// its worker, so the durable splice precedes the owning turn/start.
				pendingClaim = true
			}
		case "turn/end":
			turn, ok := eventTurn(event.Data)
			open = 0
			if !ok {
				continue
			}
			data, _ := event.Data.(map[string]any)
			reason, _ := data["reason"].(map[string]any)
			kind, _ := reason["kind"].(string)
			accountsForClaim := kind != "completed"
			if stepped[turn] || (claimed[turn] && accountsForClaim) {
				copy := event
				end = &copy
				droppedUnrun = false
			}
			delete(stepped, turn)
			delete(claimed, turn)
		}
	}
	return end, droppedUnrun
}

func (e *Engine) restoreContinuableSubagentComposition(child *Session, descriptor *SubagentDescriptorData) error {
	restriction, err := e.resolveSessionToolRestriction(descriptor.ToolFilter)
	if err != nil {
		return err
	}
	selection := ModelSelection{Provider: e.cfg.Provider, Model: e.cfg.Model}
	if descriptor.AgentProvider != nil {
		selection.Provider = *descriptor.AgentProvider
	}
	if descriptor.AgentModel != nil {
		selection.Model = *descriptor.AgentModel
	}
	persona := ""
	if descriptor.Persona != nil {
		persona = *descriptor.Persona
	}
	child.mu.Lock()
	child.Model = selection
	child.personaOverride = persona
	child.toolRestriction = restriction
	child.mu.Unlock()
	return nil
}

func (e *Engine) disposeOneShotModelSubagent(childID string) error {
	_ = e.CancelAgent(childID, AgentCancelCause{Kind: "disposed"}, CancelAgentOptions{KeepInbox: false})
	if err := e.WaitForIdle(context.Background(), childID); err != nil {
		return err
	}
	if flusher, ok := e.sessionStore.(SessionPersistenceFlusher); ok {
		_ = flusher.Flush(context.Background(), childID)
	}
	return detachSDKSession(e, childID)
}

func (e *Engine) closeModelSubagentActivations() error {
	e.modelSubagentMu.Lock()
	owned := map[string]struct{}{}
	for _, activation := range e.modelSubagentActivations {
		for childID := range activation.owned {
			owned[childID] = struct{}{}
		}
	}
	roots := make([]*modelSubagentActivation, 0, len(e.modelSubagentActivations))
	for childID, activation := range e.modelSubagentActivations {
		if _, isOwned := owned[childID]; !isOwned {
			roots = append(roots, activation)
		}
	}
	e.modelSubagentMu.Unlock()
	return errors.Join(e.finishModelSubagentBranches(roots)...)
}

func (e *Engine) finishModelSubagentBranches(activations []*modelSubagentActivation) []error {
	type branch struct {
		activation *modelSubagentActivation
		owner      bool
		done       <-chan struct{}
	}
	branches := make([]branch, 0, len(activations))
	for _, activation := range activations {
		owner, done := e.beginModelSubagentDisposal(activation)
		branches = append(branches, branch{activation: activation, owner: owner, done: done})
	}
	for _, item := range branches {
		if !item.owner {
			continue
		}
		go func(item branch) {
			_ = e.finishModelSubagentActivation(item.activation)
		}(item)
	}
	var failures []error
	for _, item := range branches {
		<-item.done
		e.modelSubagentMu.Lock()
		err := item.activation.finishErr
		e.modelSubagentMu.Unlock()
		if err != nil {
			failures = append(failures, err)
		}
	}
	return failures
}

func (e *Engine) drainModelSubagentDescendants(ctx context.Context, parentIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	roots := map[string]struct{}{}
	for _, id := range parentIDs {
		if id != "" {
			roots[id] = struct{}{}
		}
	}
	if len(roots) == 0 {
		return nil
	}
	e.modelSubagentMu.Lock()
	targets := map[string]*modelSubagentActivation{}
	for id, activation := range e.modelSubagentActivations {
		seen := map[string]bool{id: true}
		for parentID := activation.parentID; parentID != "" && !seen[parentID]; {
			if _, selected := roots[parentID]; selected {
				targets[id] = activation
				break
			}
			seen[parentID] = true
			parent := e.modelSubagentActivations[parentID]
			if parent == nil {
				break
			}
			parentID = parent.parentID
		}
	}
	e.modelSubagentMu.Unlock()
	if len(targets) == 0 {
		return nil
	}

	type drain struct {
		activation *modelSubagentActivation
		owner      bool
		done       <-chan struct{}
	}
	drains := make(map[string]drain, len(targets))
	for id, activation := range targets {
		owner, done := e.beginModelSubagentDisposal(activation)
		drains[id] = drain{activation: activation, owner: owner, done: done}
	}
	for _, item := range drains {
		if !item.owner {
			continue
		}
		go func(item drain) {
			_ = e.finishModelSubagentActivation(item.activation)
		}(item)
	}

	drainRoots := make([]drain, 0, len(drains))
	for _, item := range drains {
		if _, parentSelected := targets[item.activation.parentID]; !parentSelected {
			drainRoots = append(drainRoots, item)
		}
	}
	if len(drainRoots) == 0 {
		for _, item := range drains {
			drainRoots = append(drainRoots, item)
		}
	}
	var failures []error
	for _, item := range drainRoots {
		select {
		case <-ctx.Done():
			failures = append(failures, ctx.Err())
			return errors.Join(failures...)
		case <-item.done:
			e.modelSubagentMu.Lock()
			err := item.activation.finishErr
			e.modelSubagentMu.Unlock()
			if err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

func (e *Engine) stopModelSubagentActivation(childID string) error {
	e.modelSubagentMu.Lock()
	activation := e.modelSubagentActivations[childID]
	e.modelSubagentMu.Unlock()
	if activation == nil {
		return detachSDKSession(e, childID)
	}
	owner, done := e.beginModelSubagentDisposal(activation)
	if owner {
		return e.finishModelSubagentActivation(activation)
	}
	<-done
	e.modelSubagentMu.Lock()
	err := activation.finishErr
	e.modelSubagentMu.Unlock()
	return err
}
