package harness

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
)

// dynamicCordisAgentService reports the two core agent-plane services. The
// router is kept outside this file so the facade remains independently
// testable and can be reused by future service registries.
func dynamicCordisAgentService(name string) bool {
	return name == "agents" || name == "agentLoop"
}

func (e *Engine) dynamicCordisAgentServiceValue(run *dynamicCordisRun, name string) goja.Value {
	switch name {
	case "agents":
		return e.dynamicCordisAgentsFacade(run)
	case "agentLoop":
		return e.dynamicCordisAgentLoopFacade(run)
	default:
		return goja.Undefined()
	}
}

type dynamicCordisAgentInitiatorScope struct {
	id string
}

type dynamicCordisAgentAsyncContextTracker struct {
	scope *dynamicCordisAgentInitiatorScope
}

func (t *dynamicCordisAgentAsyncContextTracker) Grab() any {
	return t.scope
}

func (t *dynamicCordisAgentAsyncContextTracker) Resumed(value any) {
	t.scope, _ = value.(*dynamicCordisAgentInitiatorScope)
}

func (t *dynamicCordisAgentAsyncContextTracker) Exited() {
	t.scope = nil
}

func (t *dynamicCordisAgentAsyncContextTracker) enter(id string) func() {
	previous := t.scope
	t.scope = &dynamicCordisAgentInitiatorScope{id: id}
	return func() { t.scope = previous }
}

func (t *dynamicCordisAgentAsyncContextTracker) current() string {
	if t == nil || t.scope == nil {
		return ""
	}
	return t.scope.id
}

func dynamicCordisAgentID(vm *goja.Runtime, value goja.Value) string {
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return ""
	}
	if raw, ok := value.Export().(string); ok {
		return strings.TrimSpace(raw)
	}
	object, ok := value.(*goja.Object)
	if !ok {
		panic(vm.ToValue("expected an Agent, Session, or session id"))
	}
	for _, key := range []string{"id", "sessionId"} {
		member := object.Get(key)
		if member != nil && !goja.IsUndefined(member) && !goja.IsNull(member) {
			if id := strings.TrimSpace(member.String()); id != "" {
				return id
			}
		}
	}
	if header, ok := object.Get("header").(*goja.Object); ok {
		if id := header.Get("id"); id != nil && !goja.IsUndefined(id) && !goja.IsNull(id) {
			return strings.TrimSpace(id.String())
		}
	}
	panic(vm.ToValue("expected an Agent or Session with a non-empty id"))
}

func dynamicCordisAgentRejectUnknownFields(vm *goja.Runtime, value goja.Value, label string, allowed ...string) {
	object, ok := value.(*goja.Object)
	if !ok {
		return
	}
	known := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		known[name] = struct{}{}
	}
	for _, name := range object.Keys() {
		if _, ok := known[name]; !ok {
			panic(vm.ToValue(fmt.Sprintf("%s has unsupported field %q", label, name)))
		}
	}
}

func dynamicCordisAgentValidateOptions(vm *goja.Runtime, value goja.Value, resume bool) {
	if resume {
		dynamicCordisAgentRejectUnknownFields(vm, value, "agents resume options", "resumeSessionId", "agentOptions", "setup", "signal")
	} else {
		dynamicCordisAgentRejectUnknownFields(vm, value, "agents create options", "sessionId", "meta", "seed", "agentOptions", "setup", "signal")
	}
	object, ok := value.(*goja.Object)
	if !ok {
		return
	}
	if meta := object.Get("meta"); meta != nil && !goja.IsUndefined(meta) && !goja.IsNull(meta) {
		dynamicCordisAgentRejectUnknownFields(vm, meta, "agents create options meta", "cwd", "parentSession", "seedLength", "origin", "delegationDepth", "agentPreset")
	}
	if options := object.Get("agentOptions"); options != nil && !goja.IsUndefined(options) && !goja.IsNull(options) {
		dynamicCordisAgentRejectUnknownFields(vm, options, "agents create options agentOptions", "provider", "model", "maxTokens")
	}
}

func (e *Engine) dynamicCordisAgentSession(id string) (*Session, error) {
	if strings.TrimSpace(id) == "" {
		return nil, errors.New("agent id is required")
	}
	session, err := e.getSession(id)
	if err != nil {
		return nil, err
	}
	session.mu.Lock()
	attached := session.attached
	session.mu.Unlock()
	if !attached {
		return nil, fmt.Errorf("agent %q is not live", id)
	}
	return session, nil
}

// dynamicCordisAgentOwner returns the runtime initiator, when the call is
// running inside an explicit agent boundary. A plugin session is not itself a
// runtime owner; durable parentSession is intentionally not consulted here.
func dynamicCordisAgentOwner(run *dynamicCordisRun) string {
	return run.agentInitiator.current()
}

func (e *Engine) dynamicCordisAgentEntry(id string) *dynamicCordisAgentEntry {
	e.dynamicCordis.RLock()
	entry := e.dynamicCordis.agents[id]
	e.dynamicCordis.RUnlock()
	return entry
}

func (e *Engine) dynamicCordisAgentEvent(run *dynamicCordisRun, id string) map[string]any {
	session, err := e.dynamicCordisAgentSession(id)
	if err != nil {
		return map[string]any{"id": id}
	}
	session.mu.Lock()
	selection := cloneModelSelection(session.Model)
	status := "idle"
	if session.Running {
		status = "running"
	}
	session.mu.Unlock()
	return map[string]any{
		"id": id, "options": selection, "session": dynamicSessionView(session), "status": status,
	}
}

func (e *Engine) dynamicCordisAgentEnter(run *dynamicCordisRun, id, sessionID, owner string, announced bool) (func(), error) {
	if sessionID != "" && id != sessionID {
		return nil, fmt.Errorf("agent id %q does not match session id %q", id, sessionID)
	}
	if _, err := e.dynamicCordisAgentSession(id); err != nil {
		return nil, err
	}
	e.dynamicCordis.Lock()
	if existing := e.dynamicCordis.agents[id]; existing != nil && !existing.detached {
		e.dynamicCordis.Unlock()
		return nil, fmt.Errorf("agent %q is already registered", id)
	}
	entry := &dynamicCordisAgentEntry{id: id, owner: owner, announced: announced, run: run}
	e.dynamicCordis.agents[id] = entry
	e.dynamicCordis.agentOrder = append(e.dynamicCordis.agentOrder, id)
	e.dynamicCordis.Unlock()
	var once sync.Once
	detach := func() {
		e.dynamicCordis.Lock()
		if entry.detached {
			e.dynamicCordis.Unlock()
			return
		}
		if entry.announcing {
			entry.detachRequested = true
			e.dynamicCordis.Unlock()
			return
		}
		entry.detached = true
		if e.dynamicCordis.agents[id] == entry {
			delete(e.dynamicCordis.agents, id)
		}
		e.dynamicCordis.Unlock()
		if entry.announced {
			_ = e.emitDynamicCordisEventFrom(run, "agent/disposed", map[string]any{"agent": e.dynamicCordisAgentEvent(run, id)})
		}
	}
	return func() { once.Do(detach) }, nil
}

func (e *Engine) dynamicCordisAgentAnnounce(run *dynamicCordisRun, id string) error {
	e.dynamicCordis.Lock()
	entry := e.dynamicCordis.agents[id]
	if entry == nil || entry.detached {
		e.dynamicCordis.Unlock()
		return fmt.Errorf("agent %q is not live in this registry", id)
	}
	if entry.announced || entry.announcing {
		e.dynamicCordis.Unlock()
		return fmt.Errorf("agent %q was already announced", id)
	}
	entry.announcing = true
	entry.announced = true
	e.dynamicCordis.Unlock()
	err := e.emitDynamicCordisEventFrom(run, "agent/created", map[string]any{"agent": e.dynamicCordisAgentEvent(run, id)})
	e.dynamicCordis.Lock()
	entry.announcing = false
	detach := entry.detachRequested
	entry.detachRequested = false
	e.dynamicCordis.Unlock()
	if err != nil {
		// A synchronous creation listener vetoes publication.
		e.dynamicCordis.Lock()
		if !entry.detached {
			entry.detached = true
			if e.dynamicCordis.agents[id] == entry {
				delete(e.dynamicCordis.agents, id)
			}
		}
		e.dynamicCordis.Unlock()
		_ = e.emitDynamicCordisEventFrom(run, "agent/disposed", map[string]any{"agent": e.dynamicCordisAgentEvent(run, id)})
		return err
	}
	if detach {
		e.dynamicCordis.Lock()
		if !entry.detached {
			entry.detached = true
			if e.dynamicCordis.agents[id] == entry {
				delete(e.dynamicCordis.agents, id)
			}
		}
		e.dynamicCordis.Unlock()
		_ = e.emitDynamicCordisEventFrom(run, "agent/disposed", map[string]any{"agent": e.dynamicCordisAgentEvent(run, id)})
	}
	return nil
}

func (e *Engine) dynamicCordisAgentList(roots bool) []string {
	e.mu.RLock()
	sessions := make(map[string]*Session, len(e.sessions))
	for id, session := range e.sessions {
		sessions[id] = session
	}
	e.mu.RUnlock()
	e.dynamicCordis.RLock()
	order := append([]string(nil), e.dynamicCordis.agentOrder...)
	entries := make(map[string]*dynamicCordisAgentEntry, len(e.dynamicCordis.agents))
	for id, entry := range e.dynamicCordis.agents {
		entries[id] = entry
	}
	e.dynamicCordis.RUnlock()
	seen := make(map[string]bool, len(order))
	ids := make([]string, 0, len(sessions))
	for _, id := range order {
		// An id that was explicitly entered and later detached must not be
		// reintroduced by the legacy session fallback below. Keep the ordered
		// registry history as the tombstone; a later lifecycle with the same id
		// replaces the live entry and is handled normally here.
		if seen[id] {
			continue
		}
		seen[id] = true
		if sessions[id] == nil {
			continue
		}
		entry := entries[id]
		if entry == nil || entry.detached {
			continue
		}
		if roots && entry != nil && entry.owner != "" {
			continue
		}
		ids = append(ids, id)
	}
	// Existing host-created sessions have no registry entry. Keep them visible
	// as runtime roots for compatibility with the host facade.
	legacy := make([]string, 0)
	for id, session := range sessions {
		session.mu.Lock()
		live := session.attached
		session.mu.Unlock()
		if !live || seen[id] {
			continue
		}
		if roots && entries[id] != nil && entries[id].owner != "" {
			continue
		}
		seen[id] = true
		legacy = append(legacy, id)
	}
	sort.Strings(legacy)
	ids = append(legacy, ids...)
	return ids
}

func (e *Engine) dynamicCordisAgentFactoryCall(run *dynamicCordisRun, options goja.Value, resume bool) (goja.Value, bool) {
	e.dynamicCordis.RLock()
	owner := e.dynamicCordis.factoryRun
	factory, _ := e.dynamicCordis.factoryValue.(*goja.Object)
	e.dynamicCordis.RUnlock()
	if owner == nil || factory == nil {
		return nil, false
	}
	method := "createAgent"
	if resume {
		method = "resume"
	}
	fn, ok := goja.AssertFunction(factory.Get(method))
	if !ok && !resume {
		fn, ok = goja.AssertFunction(factory.Get("create"))
	}
	if !ok {
		panic(run.runtime.ToValue(fmt.Sprintf("registered agent factory has no %s method", method)))
	}
	ownerCtx, ownerOptions := goja.Value(owner.ctx), options
	if owner != run {
		ownerCtx = dynamicCordisProxyValue(owner, run, run.ctx)
		ownerOptions = dynamicCordisProxyValue(owner, run, options)
	}
	result, err := fn(factory, ownerCtx, ownerOptions)
	if err != nil {
		panic(run.runtime.ToValue(dynamicJSMessage(err)))
	}
	if owner == run {
		return result, true
	}
	promise, pending := result.Export().(*goja.Promise)
	if !pending || promise.State() != goja.PromiseStatePending {
		if pending && promise.State() == goja.PromiseStateRejected {
			panic(run.runtime.ToValue(dynamicJSValueMessage(promise.Result())))
		}
		if pending {
			result = promise.Result()
		}
		return dynamicCordisProxyValue(run, owner, result), true
	}
	bridged, resolve, reject := run.runtime.NewPromise()
	dynamicCordisAwaitOnLoop(owner, result, func(value goja.Value, err error) {
		if err != nil {
			_ = reject(run.runtime.NewGoError(err))
			return
		}
		_ = resolve(dynamicCordisProxyValue(run, owner, value))
	})
	return run.runtime.ToValue(bridged), true
}

func (e *Engine) dynamicCordisAgentValue(run *dynamicCordisRun, session *Session) *goja.Object {
	vm := run.runtime
	session.mu.Lock()
	id := session.Header.ID
	selection := cloneModelSelection(session.Model)
	session.mu.Unlock()
	agent := vm.NewObject()
	_ = agent.Set("id", id)
	_ = agent.Set("options", selection)
	_ = agent.Set("session", e.dynamicCordisSessionValue(run, session))
	_ = agent.DefineAccessorProperty("status", vm.ToValue(func() string {
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.Running {
			return "running"
		}
		return "idle"
	}), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = agent.Set("inbox", e.dynamicCordisAgentInboxValue(run, session))
	_ = agent.Set("send", func(call goja.FunctionCall) goja.Value {
		message := dynamicCordisAgentMessageInput(vm, call.Argument(0))
		target := call.Argument(1).String()
		if err := e.dynamicCordisAgentSend(run, session, message, target, call.Argument(2).ToBoolean()); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return goja.Undefined()
	})
	_ = agent.Set("followup", func(call goja.FunctionCall) goja.Value {
		message := dynamicCordisAgentMessageInput(vm, call.Argument(0))
		if err := e.dynamicCordisAgentSend(run, session, message, "next-turn", true); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return goja.Undefined()
	})
	_ = agent.Set("steer", func(call goja.FunctionCall) goja.Value {
		message := dynamicCordisAgentMessageInput(vm, call.Argument(0))
		if err := e.dynamicCordisAgentSend(run, session, message, "next-step", true); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return goja.Undefined()
	})
	_ = agent.Set("inject", func(call goja.FunctionCall) goja.Value {
		message := dynamicCordisAgentMessageInput(vm, call.Argument(0))
		if err := e.dynamicCordisAgentSend(run, session, message, "next-step", false); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return goja.Undefined()
	})
	_ = agent.Set("runMaintenance", func(call goja.FunctionCall) goja.Value {
		task, ok := goja.AssertFunction(call.Argument(0))
		if !ok {
			panic(vm.ToValue("agent.runMaintenance requires a function"))
		}
		return e.dynamicCordisAgentMaintenance(run, session, task)
	})
	_ = agent.Set("cancel", func(call goja.FunctionCall) goja.Value {
		keepInbox := false
		if options := call.Argument(1); options != nil && !goja.IsUndefined(options) && !goja.IsNull(options) {
			if object, ok := options.(*goja.Object); ok {
				keepInbox = object.Get("keepInbox").ToBoolean()
			}
		} else if options := call.Argument(0); options != nil && !goja.IsUndefined(options) && !goja.IsNull(options) {
			// Compatibility with the older Go facade which accepted options as
			// the first and only argument.
			if object, ok := options.(*goja.Object); ok {
				kind := object.Get("kind")
				if kind == nil || goja.IsUndefined(kind) || goja.IsNull(kind) {
					keepInbox = object.Get("keepInbox").ToBoolean()
				}
			}
		}
		if err := e.dynamicCordisCancelAgent(id, keepInbox); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return goja.Undefined()
	})
	_ = agent.Set("whenIdle", func(goja.FunctionCall) goja.Value {
		return e.dynamicCordisAgentAsync(run, func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
			defer cancel()
			return e.WaitForIdle(ctx, id)
		})
	})
	return agent
}

func (e *Engine) dynamicCordisAgentSnapshot(run *dynamicCordisRun, id string) (*goja.Object, error) {
	session, err := e.dynamicCordisAgentSession(id)
	if err != nil {
		return nil, err
	}
	return e.dynamicCordisAgentValue(run, session), nil
}

func (e *Engine) dynamicCordisCancelAgent(id string, keepInbox bool) error {
	if keepInbox {
		return e.CancelSession(id)
	}
	session, err := e.getSession(id)
	if err != nil {
		return err
	}
	session.mu.Lock()
	var events []Event
	if len(session.pending) > 0 {
		event, appendErr := appendEventLocked(session, "agent/inbox/spliced", map[string]any{
			"target": "next-turn", "start": 0, "removedCount": len(session.pending), "inserted": []any{}, "outcome": "canceled",
		}, nil, nil, false)
		if appendErr != nil {
			session.mu.Unlock()
			return appendErr
		}
		events = append(events, event)
	}
	if len(session.steering) > 0 {
		event, appendErr := appendEventLocked(session, "agent/inbox/spliced", map[string]any{
			"target": "next-step", "start": 0, "removedCount": len(session.steering), "inserted": []any{}, "outcome": "canceled",
		}, nil, nil, false)
		if appendErr != nil {
			session.mu.Unlock()
			return appendErr
		}
		events = append(events, event)
	}
	pending := append(append([]*queuedPrompt(nil), session.pending...), session.steering...)
	session.pending = nil
	session.steering = nil
	cancel := session.Cancel
	maintenanceCancel := session.maintenanceCancel
	session.mu.Unlock()
	for _, event := range events {
		e.publishEvent(id, event)
	}
	if len(events) > 0 {
		e.emitQueue(session)
	}
	for _, item := range pending {
		if item.done != nil {
			item.done <- promptOutcome{err: context.Canceled}
		}
	}
	if cancel != nil {
		cancel()
	}
	if maintenanceCancel != nil {
		maintenanceCancel()
	}
	return nil
}

type dynamicCordisAgentMessage struct {
	ID      string         `json:"id"`
	Role    string         `json:"role"`
	Content []ContentBlock `json:"content"`
	Source  map[string]any `json:"source"`
}

func dynamicCordisAgentMessageInput(vm *goja.Runtime, value goja.Value) *queuedPrompt {
	var message dynamicCordisAgentMessage
	if err := dynamicCordisDecode(value, &message); err != nil {
		panic(vm.ToValue("agent message: " + err.Error()))
	}
	if strings.TrimSpace(message.ID) == "" || message.Role != "user" || len(message.Content) == 0 {
		panic(vm.ToValue("agent message requires a user id, role, and content"))
	}
	if kind, _ := message.Source["kind"].(string); strings.TrimSpace(kind) == "" {
		panic(vm.ToValue("agent message source.kind is required"))
	}
	for _, block := range message.Content {
		switch block.Type {
		case "text":
		case "image":
			if block.Attachment == nil || block.Attachment.AttachmentID == "" {
				panic(vm.ToValue("agent image message requires an attachment"))
			}
		default:
			panic(vm.ToValue(fmt.Sprintf("agent message has unsupported content type %q", block.Type)))
		}
	}
	return &queuedPrompt{id: message.ID, text: strings.TrimSpace(blockText(message.Content)), content: message.Content, source: message.Source}
}

func (e *Engine) dynamicCordisAgentSend(run *dynamicCordisRun, session *Session, message *queuedPrompt, target string, wakeup bool) error {
	session.mu.Lock()
	if !session.attached {
		session.mu.Unlock()
		return errors.New("agent is not live")
	}
	session.mu.Unlock()
	if _, _, err := e.dynamicCordisAgentInboxMutate(run, session, target, math.MaxInt, 0, []*queuedPrompt{message}, false, "", wakeup); err != nil {
		return err
	}
	session.mu.Lock()
	startWorker := wakeup && !session.Running && !session.maintenance
	if startWorker {
		session.Running = true
	}
	id := session.Header.ID
	session.mu.Unlock()
	if startWorker {
		e.emitHost(map[string]any{"type": "host/session-status", "sessionId": id, "running": true})
		e.launchSessionWorker(session)
	}
	return nil
}

// dynamicCordisAgentInboxMutate commits an inbox splice while session.mu is
// held, preserving the durable event as the source of truth for queue replay.
// locateID makes replace/remove atomic with the lookup across both queues.
func (e *Engine) dynamicCordisAgentInboxMutate(run *dynamicCordisRun, session *Session, target string, start, deleteCount int, inserted []*queuedPrompt, discardRemoved bool, locateID string, wakeup bool) ([]*queuedPrompt, bool, error) {
	if target != "next-turn" && target != "next-step" {
		return nil, false, errors.New("agent inbox target must be next-turn or next-step")
	}
	session.mu.Lock()
	if !session.attached {
		session.mu.Unlock()
		return nil, false, errors.New("agent is not live")
	}
	if locateID != "" {
		located := false
		for _, candidate := range []struct {
			target string
			queue  []*queuedPrompt
		}{{"next-turn", session.pending}, {"next-step", session.steering}} {
			for index, message := range candidate.queue {
				if message.id == locateID {
					target, start, deleteCount, located = candidate.target, index, 1, true
					break
				}
			}
			if located {
				break
			}
		}
		if !located {
			session.mu.Unlock()
			return nil, false, nil
		}
	}
	queue := &session.pending
	other := session.steering
	if target == "next-step" {
		queue, other = &session.steering, session.pending
	}
	actualStart := dynamicCordisAgentSpliceStart(len(*queue), start)
	actualDeleteCount := dynamicCordisAgentSpliceDeleteCount(len(*queue), actualStart, deleteCount)
	if actualDeleteCount == 0 && len(inserted) == 0 {
		session.mu.Unlock()
		return nil, true, nil
	}
	candidate := make([]*queuedPrompt, 0, len(*queue)-actualDeleteCount+len(inserted))
	candidate = append(candidate, (*queue)[:actualStart]...)
	candidate = append(candidate, inserted...)
	candidate = append(candidate, (*queue)[actualStart+actualDeleteCount:]...)
	ids := make(map[string]struct{}, len(candidate)+len(other))
	for _, message := range append(candidate, other...) {
		if message == nil || message.id == "" {
			session.mu.Unlock()
			return nil, false, errors.New("agent inbox message id is required")
		}
		if _, exists := ids[message.id]; exists {
			session.mu.Unlock()
			return nil, false, fmt.Errorf("message %q is already pending", message.id)
		}
		ids[message.id] = struct{}{}
	}
	removed := append([]*queuedPrompt(nil), (*queue)[actualStart:actualStart+actualDeleteCount]...)
	data := map[string]any{"target": target, "start": actualStart, "inserted": messagesToAny(inserted)}
	if actualDeleteCount > 0 {
		data["removedCount"] = actualDeleteCount
		if discardRemoved {
			data["outcome"] = "canceled"
		}
	}
	event, err := appendEventLocked(session, "agent/inbox/spliced", data, nil, nil, false)
	if err != nil {
		session.mu.Unlock()
		return nil, false, err
	}
	*queue = candidate
	if wakeup && session.maintenance {
		session.maintenanceWake = true
	}
	id := session.Header.ID
	session.mu.Unlock()
	e.publishEventFrom(run, id, event)
	e.emitQueue(session)
	agent := e.dynamicCordisAgentEvent(run, id)
	for _, message := range removed {
		if discardRemoved {
			_ = e.emitDynamicCordisEventFrom(run, "agent/inbox/discarded", map[string]any{"agent": agent, "message": message.message()})
		}
	}
	for _, message := range inserted {
		_ = e.emitDynamicCordisEventFrom(run, "agent/inbox/inserted", map[string]any{"agent": agent, "message": message.message()})
	}
	return removed, true, nil
}

func dynamicCordisAgentSpliceStart(length, start int) int {
	if start < 0 {
		if start < -length {
			return 0
		}
		return length + start
	}
	if start > length {
		return length
	}
	return start
}

func dynamicCordisAgentSpliceDeleteCount(length, start, deleteCount int) int {
	if deleteCount <= 0 {
		return 0
	}
	if deleteCount > length-start {
		return length - start
	}
	return deleteCount
}

func messagesToAny(messages []*queuedPrompt) []any {
	result := make([]any, len(messages))
	for index, message := range messages {
		result[index] = message.message()
	}
	return result
}

func dynamicCordisAgentSpliceIndex(value goja.Value) int {
	number := value.ToFloat()
	if math.IsNaN(number) {
		return 0
	}
	if number >= float64(math.MaxInt) {
		return math.MaxInt
	}
	if number <= float64(math.MinInt) {
		return math.MinInt
	}
	return int(math.Trunc(number))
}

func dynamicCordisAgentMessagesInput(vm *goja.Runtime, value goja.Value) []*queuedPrompt {
	array, ok := value.(*goja.Object)
	if !ok || array.ClassName() != "Array" {
		panic(vm.ToValue("agent inbox inserted messages must be an array"))
	}
	length := int(array.Get("length").ToInteger())
	messages := make([]*queuedPrompt, length)
	for index := range messages {
		messages[index] = dynamicCordisAgentMessageInput(vm, array.Get(fmt.Sprintf("%d", index)))
	}
	return messages
}

func (e *Engine) dynamicCordisAgentInboxValue(run *dynamicCordisRun, session *Session) *goja.Object {
	vm := run.runtime
	inbox := vm.NewObject()
	items := func(nextTurn bool) []any {
		session.mu.Lock()
		queue := session.steering
		if nextTurn {
			queue = session.pending
		}
		value := make([]any, len(queue))
		for index, item := range queue {
			value[index] = item.message()
		}
		session.mu.Unlock()
		return value
	}
	_ = inbox.DefineAccessorProperty("nextTurn", vm.ToValue(func() any { return items(true) }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = inbox.DefineAccessorProperty("nextStep", vm.ToValue(func() any { return items(false) }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = inbox.DefineAccessorProperty("hasPending", vm.ToValue(func() bool {
		session.mu.Lock()
		defer session.mu.Unlock()
		return len(session.pending) > 0 || len(session.steering) > 0
	}), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	mutate := func(target string, start, deleteCount int, inserted []*queuedPrompt, discard bool, locateID string) ([]*queuedPrompt, bool) {
		removed, found, err := e.dynamicCordisAgentInboxMutate(run, session, target, start, deleteCount, inserted, discard, locateID, false)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return removed, found
	}
	_ = inbox.Set("clear", func(goja.FunctionCall) goja.Value {
		mutate("next-step", 0, math.MaxInt, nil, true, "")
		mutate("next-turn", 0, math.MaxInt, nil, true, "")
		return goja.Undefined()
	})
	_ = inbox.Set("append", func(call goja.FunctionCall) goja.Value {
		message := dynamicCordisAgentMessageInput(vm, call.Argument(1))
		mutate(call.Argument(0).String(), math.MaxInt, 0, []*queuedPrompt{message}, true, "")
		return goja.Undefined()
	})
	_ = inbox.Set("prepend", func(call goja.FunctionCall) goja.Value {
		message := dynamicCordisAgentMessageInput(vm, call.Argument(1))
		mutate(call.Argument(0).String(), 0, 0, []*queuedPrompt{message}, true, "")
		return goja.Undefined()
	})
	_ = inbox.Set("replace", func(call goja.FunctionCall) goja.Value {
		message := dynamicCordisAgentMessageInput(vm, call.Argument(1))
		_, found := mutate("next-turn", 0, 0, []*queuedPrompt{message}, true, call.Argument(0).String())
		return vm.ToValue(found)
	})
	_ = inbox.Set("remove", func(call goja.FunctionCall) goja.Value {
		_, found := mutate("next-turn", 0, 0, nil, true, call.Argument(0).String())
		return vm.ToValue(found)
	})
	_ = inbox.Set("splice", func(call goja.FunctionCall) goja.Value {
		removed, _ := mutate(call.Argument(0).String(), dynamicCordisAgentSpliceIndex(call.Argument(1)), dynamicCordisAgentSpliceIndex(call.Argument(2)), dynamicCordisAgentMessagesInput(vm, call.Argument(3)), true, "")
		return vm.ToValue(messagesToAny(removed))
	})
	return inbox
}

func (e *Engine) dynamicCordisAgentMaintenance(run *dynamicCordisRun, session *Session, task goja.Callable) goja.Value {
	vm := run.runtime
	promise, resolve, reject := vm.NewPromise()
	signal := vm.NewObject()
	type abortListener struct {
		value goja.Value
		call  goja.Callable
	}
	listeners := []abortListener{}
	aborted := false
	abort := func() {
		if aborted {
			return
		}
		aborted = true
		_ = signal.Set("aborted", true)
		_ = signal.Set("reason", vm.NewGoError(errors.New("agent maintenance canceled")))
		for _, listener := range append([]abortListener(nil), listeners...) {
			if _, err := listener.call(goja.Undefined()); err != nil {
				e.reportDynamicCordisHostFailure(run, "agent maintenance abort listener", err)
			}
		}
	}
	_ = signal.Set("aborted", false)
	_ = signal.Set("addEventListener", func(call goja.FunctionCall) goja.Value {
		if call.Argument(0).String() == "abort" {
			if listener, ok := goja.AssertFunction(call.Argument(1)); ok {
				listeners = append(listeners, abortListener{value: call.Argument(1), call: listener})
			}
		}
		return goja.Undefined()
	})
	_ = signal.Set("removeEventListener", func(call goja.FunctionCall) goja.Value {
		if call.Argument(0).String() == "abort" {
			listener := call.Argument(1)
			kept := listeners[:0]
			for _, registered := range listeners {
				if !registered.value.StrictEquals(listener) {
					kept = append(kept, registered)
				}
			}
			listeners = kept
		}
		return goja.Undefined()
	})

	session.mu.Lock()
	if !session.attached || session.Running || session.maintenance {
		session.mu.Unlock()
		panic(vm.ToValue(fmt.Sprintf("agent %q already has active work", session.Header.ID)))
	}
	session.maintenance = true
	session.maintenanceWake = false
	session.maintenanceCancel = func() { _ = e.dynamicCordis.loop.post(abort) }
	session.mu.Unlock()

	finish := func(value goja.Value, err error) {
		session.mu.Lock()
		session.maintenance = false
		session.maintenanceCancel = nil
		wake := session.maintenanceWake
		session.maintenanceWake = false
		startWorker := session.attached && !session.Running && wake && (len(session.pending) > 0 || len(session.steering) > 0)
		if startWorker {
			session.Running = true
		}
		id := session.Header.ID
		session.mu.Unlock()
		if startWorker {
			e.emitHost(map[string]any{"type": "host/session-status", "sessionId": id, "running": true})
			e.launchSessionWorker(session)
		}
		if err != nil {
			_ = reject(dynamicCordisAgentErrorValue(vm, err))
			return
		}
		_ = resolve(value)
	}
	value, err := task(goja.Undefined(), signal)
	if err != nil {
		finish(nil, err)
		return vm.ToValue(promise)
	}
	dynamicCordisAwaitOnLoop(run, value, finish)
	return vm.ToValue(promise)
}

func (e *Engine) dynamicCordisAgentAsync(run *dynamicCordisRun, operation func() error) goja.Value {
	vm := run.runtime
	promise, resolve, reject := vm.NewPromise()
	go func() {
		err := operation()
		if !e.dynamicCordis.loop.post(func() {
			if err != nil {
				_ = reject(vm.NewGoError(err))
				return
			}
			_ = resolve(goja.Undefined())
		}) {
			_ = reject(vm.NewGoError(errors.New("dynamic Cordis runtime is closed")))
		}
	}()
	return vm.ToValue(promise)
}

func (e *Engine) dynamicCordisAgentHandle(run *dynamicCordisRun, agent *goja.Object, dispose func() error) *goja.Object {
	vm := run.runtime
	handle := vm.NewObject()
	_ = handle.Set("agent", agent)
	_ = handle.Set("dispose", func(goja.FunctionCall) goja.Value {
		return e.dynamicCordisAgentAsync(run, dispose)
	})
	return handle
}

func (e *Engine) dynamicCordisOwnAgent(run *dynamicCordisRun, id string, session *Session) func(bool) error {
	var once sync.Once
	var disposeErr error
	var detachRegistry func()
	dispose := func(remove bool) error {
		once.Do(func() {
			current, err := e.getSession(id)
			if err != nil || current != session {
				return
			}
			if err := detachSDKSessionWithDynamicOrigin(e, run, id); err != nil {
				disposeErr = err
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
			defer cancel()
			_ = e.WaitForIdle(ctx, id)
			if detachRegistry != nil {
				detachRegistry()
			}
		})
		if remove {
			session.mu.Lock()
			attached := session.attached
			session.mu.Unlock()
			if attached {
				return disposeErr
			}
			e.mu.Lock()
			if e.sessions[id] == session {
				delete(e.sessions, id)
			}
			e.mu.Unlock()
		}
		return disposeErr
	}
	run.disposers = append(run.disposers, func() { _ = dispose(true) })
	owner := dynamicCordisAgentOwner(run)
	detach, err := e.dynamicCordisAgentEnter(run, id, id, owner, false)
	if err != nil {
		disposeErr = err
	} else {
		detachRegistry = detach
	}
	return dispose
}

func (e *Engine) dynamicCordisLiveAgentIDs() []string {
	e.mu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.mu.RUnlock()
	ids := make([]string, 0, len(sessions))
	for _, session := range sessions {
		session.mu.Lock()
		if session.attached {
			ids = append(ids, session.Header.ID)
		}
		session.mu.Unlock()
	}
	sort.Strings(ids)
	return ids
}

func (e *Engine) dynamicCordisAgentsFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	_ = service.Set("get", func(call goja.FunctionCall) goja.Value {
		id := dynamicCordisAgentID(vm, call.Argument(0))
		e.dynamicCordis.RLock()
		entry := e.dynamicCordis.agents[id]
		retired := false
		if entry == nil {
			for _, candidate := range e.dynamicCordis.agentOrder {
				if candidate == id {
					retired = true
					break
				}
			}
		}
		e.dynamicCordis.RUnlock()
		if entry != nil && entry.detached || retired {
			return goja.Undefined()
		}
		agent, err := e.dynamicCordisAgentSnapshot(run, id)
		if err != nil {
			return goja.Undefined()
		}
		return agent
	})
	_ = service.Set("list", func(goja.FunctionCall) goja.Value {
		rows := make([]any, 0)
		for _, id := range e.dynamicCordisAgentList(false) {
			if agent, err := e.dynamicCordisAgentSnapshot(run, id); err == nil {
				rows = append(rows, agent)
			}
		}
		return vm.NewArray(rows...)
	})
	_ = service.Set("roots", func(goja.FunctionCall) goja.Value {
		rows := make([]any, 0)
		for _, id := range e.dynamicCordisAgentList(true) {
			if agent, err := e.dynamicCordisAgentSnapshot(run, id); err == nil {
				rows = append(rows, agent)
			}
		}
		return vm.NewArray(rows...)
	})
	_ = service.Set("currentInitiator", func(goja.FunctionCall) goja.Value {
		id := run.agentInitiator.current()
		if id == "" {
			return goja.Undefined()
		}
		agent, err := e.dynamicCordisAgentSnapshot(run, id)
		if err != nil {
			return goja.Undefined()
		}
		return agent
	})
	_ = service.Set("requireInitiator", func(goja.FunctionCall) goja.Value {
		value := service.Get("currentInitiator")
		fn, _ := goja.AssertFunction(value)
		current, err := fn(service)
		if err != nil || current == nil || goja.IsUndefined(current) {
			panic(vm.ToValue("no initiating agent is active"))
		}
		return current
	})
	_ = service.Set("withInitiator", func(call goja.FunctionCall) goja.Value {
		id := dynamicCordisAgentID(vm, call.Argument(0))
		if _, err := e.dynamicCordisAgentSession(id); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		fn, ok := goja.AssertFunction(call.Argument(1))
		if !ok {
			panic(vm.ToValue("agents.withInitiator requires a function"))
		}
		restore := run.agentInitiator.enter(id)
		value, err := fn(goja.Undefined())
		restore()
		if err != nil {
			panic(err)
		}
		return value
	})
	_ = service.Set("withoutInitiator", func(call goja.FunctionCall) goja.Value {
		fn, ok := goja.AssertFunction(call.Argument(0))
		if !ok {
			panic(vm.ToValue("agents.withoutInitiator requires a function"))
		}
		restore := run.agentInitiator.enter("")
		value, err := fn(goja.Undefined())
		restore()
		if err != nil {
			panic(err)
		}
		return value
	})
	_ = service.Set("create", func(call goja.FunctionCall) goja.Value {
		owner := dynamicCordisAgentOwner(run)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if value, ok := e.dynamicCordisAgentFactoryCall(run, call.Argument(0), false); ok {
				return value
			}
			return e.dynamicCordisCreateAgent(run, call.Argument(0), false, owner)
		})
	})
	_ = service.Set("resume", func(call goja.FunctionCall) goja.Value {
		owner := dynamicCordisAgentOwner(run)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			if value, ok := e.dynamicCordisAgentFactoryCall(run, call.Argument(0), true); ok {
				return value
			}
			return e.dynamicCordisCreateAgent(run, call.Argument(0), true, owner)
		})
	})
	_ = service.Set("setFactory", func(call goja.FunctionCall) goja.Value {
		factory := call.Argument(0)
		object, ok := factory.(*goja.Object)
		if !ok || object == nil {
			panic(vm.ToValue("agents.setFactory requires a factory object"))
		}
		if _, ok := goja.AssertFunction(object.Get("createAgent")); !ok {
			if _, ok := goja.AssertFunction(object.Get("create")); !ok {
				panic(vm.ToValue("agents.setFactory requires createAgent or create"))
			}
		}
		e.dynamicCordis.Lock()
		if e.dynamicCordis.factoryValue != nil {
			e.dynamicCordis.Unlock()
			panic(vm.ToValue("an agent factory is already registered"))
		}
		e.dynamicCordis.factoryRun = run
		e.dynamicCordis.factoryValue = factory
		e.dynamicCordis.Unlock()
		var once sync.Once
		dispose := func() {
			once.Do(func() {
				e.dynamicCordis.Lock()
				if e.dynamicCordis.factoryRun == run {
					e.dynamicCordis.factoryRun = nil
					e.dynamicCordis.factoryValue = nil
				}
				e.dynamicCordis.Unlock()
			})
		}
		run.disposers = append(run.disposers, dispose)
		return vm.ToValue(func(goja.FunctionCall) goja.Value {
			dispose()
			return goja.Undefined()
		})
	})
	_ = service.Set("isOwnedBy", func(call goja.FunctionCall) goja.Value {
		id := dynamicCordisAgentID(vm, call.Argument(0))
		owner := dynamicCordisAgentID(vm, call.Argument(1))
		e.dynamicCordis.RLock()
		entry := e.dynamicCordis.agents[id]
		owned := entry != nil && !entry.detached && entry.owner == owner
		e.dynamicCordis.RUnlock()
		return vm.ToValue(owned)
	})
	_ = service.Set("register", func(call goja.FunctionCall) goja.Value {
		id := dynamicCordisAgentID(vm, call.Argument(0))
		owner := ""
		if len(call.Arguments) > 1 && !goja.IsUndefined(call.Argument(1)) && !goja.IsNull(call.Argument(1)) {
			owner = dynamicCordisAgentID(vm, call.Argument(1))
		}
		sessionID := id
		if object, ok := call.Argument(0).(*goja.Object); ok {
			if raw := object.Get("session"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
				sessionID = dynamicCordisAgentID(vm, raw)
			}
		}
		detach, err := e.dynamicCordisAgentEnter(run, id, sessionID, owner, false)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		if err := e.dynamicCordisAgentAnnounce(run, id); err != nil {
			detach()
			panic(vm.ToValue(err.Error()))
		}
		run.disposers = append(run.disposers, detach)
		return vm.ToValue(detach)
	})
	_ = service.Set("enter", func(call goja.FunctionCall) goja.Value {
		id := dynamicCordisAgentID(vm, call.Argument(0))
		owner := ""
		if len(call.Arguments) > 1 && !goja.IsUndefined(call.Argument(1)) && !goja.IsNull(call.Argument(1)) {
			owner = dynamicCordisAgentID(vm, call.Argument(1))
		}
		sessionID := id
		if object, ok := call.Argument(0).(*goja.Object); ok {
			if raw := object.Get("session"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
				sessionID = dynamicCordisAgentID(vm, raw)
			}
		}
		detach, err := e.dynamicCordisAgentEnter(run, id, sessionID, owner, false)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		run.disposers = append(run.disposers, detach)
		return vm.ToValue(detach)
	})
	_ = service.Set("announce", func(call goja.FunctionCall) goja.Value {
		id := dynamicCordisAgentID(vm, call.Argument(0))
		if err := e.dynamicCordisAgentAnnounce(run, id); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return goja.Undefined()
	})
	return service
}

type dynamicCordisAgentCreationInput struct {
	id      string
	meta    goja.Value
	seed    goja.Value
	signal  *goja.Object
	setup   goja.Callable
	options struct {
		Provider  string `json:"provider"`
		Model     string `json:"model"`
		MaxTokens *int   `json:"maxTokens"`
	}
}

func dynamicCordisAgentCreationOptions(vm *goja.Runtime, value goja.Value, resume bool) dynamicCordisAgentCreationInput {
	dynamicCordisAgentValidateOptions(vm, value, resume)
	object, ok := value.(*goja.Object)
	if !ok {
		panic(vm.ToValue("agents create options: expected an object"))
	}
	input := dynamicCordisAgentCreationInput{}
	idField := "sessionId"
	if resume {
		idField = "resumeSessionId"
	}
	if raw := object.Get(idField); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
		input.id = strings.TrimSpace(raw.String())
	}
	if input.id == "" {
		panic(vm.ToValue(fmt.Sprintf("agents %s requires %s", map[bool]string{true: "resume", false: "create"}[resume], idField)))
	}
	if raw := object.Get("agentOptions"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
		if err := dynamicCordisDecode(raw, &input.options); err != nil {
			panic(vm.ToValue("agents create options agentOptions: " + err.Error()))
		}
	}
	if input.options.MaxTokens != nil && *input.options.MaxTokens <= 0 {
		panic(vm.ToValue("agents create options agentOptions.maxTokens must be positive"))
	}
	if raw := object.Get("setup"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
		input.setup, ok = goja.AssertFunction(raw)
		if !ok {
			panic(vm.ToValue("agents create options setup must be a function"))
		}
	}
	if raw := object.Get("signal"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
		input.signal, ok = raw.(*goja.Object)
		if !ok {
			panic(vm.ToValue("agents create options signal must be an AbortSignal"))
		}
	}
	if !resume {
		input.meta = object.Get("meta")
		input.seed = object.Get("seed")
	}
	return input
}

func dynamicCordisAgentAbortValue(vm *goja.Runtime, id string, signal *goja.Object) goja.Value {
	if signal != nil {
		reason := signal.Get("reason")
		if object, ok := reason.(*goja.Object); ok && object.ClassName() == "Error" {
			return reason
		}
		errorValue := vm.NewGoError(fmt.Errorf("agent %q creation aborted", id))
		if reason != nil && !goja.IsUndefined(reason) {
			_ = errorValue.Set("cause", reason)
		}
		return errorValue
	}
	return vm.NewGoError(fmt.Errorf("agent %q creation aborted", id))
}

func dynamicCordisAgentErrorValue(vm *goja.Runtime, err error) goja.Value {
	var exception *goja.Exception
	if errors.As(err, &exception) {
		return exception.Value()
	}
	return vm.NewGoError(err)
}

func (e *Engine) dynamicCordisPrepareAgentSession(run *dynamicCordisRun, input dynamicCordisAgentCreationInput, resume bool) (*dynamicCordisPreparedSession, error) {
	vm := run.runtime
	options := vm.NewObject()
	var releaseReservation func()
	if resume {
		var claimed bool
		releaseReservation, claimed = e.claimDynamicAgentReservation(input.id)
		if !claimed {
			return nil, fmt.Errorf("agent %q is already live or being resumed", input.id)
		}
		var inspection SessionInspection
		existing, err := e.getSession(input.id)
		if err == nil {
			existing.mu.Lock()
			if existing.attached {
				existing.mu.Unlock()
				releaseReservation()
				return nil, fmt.Errorf("agent %q is already live", input.id)
			}
			inspection.Meta = existing.Header
			inspection.Events = append([]Event(nil), existing.Events...)
			existing.mu.Unlock()
		} else {
			e.mu.RLock()
			store := e.sessionStore
			e.mu.RUnlock()
			if store == nil {
				releaseReservation()
				return nil, errors.New("cannot resume: session persistence is not configured")
			}
			inspection, err = store.Load(context.Background(), input.id)
			if err != nil {
				releaseReservation()
				return nil, err
			}
		}
		_ = options.Set("meta", inspection.Meta)
		_ = options.Set("seed", inspection.Events)
		_ = options.Set("seedSource", "persistence")
	} else {
		if input.meta != nil && !goja.IsUndefined(input.meta) {
			_ = options.Set("meta", input.meta)
		}
		if input.seed != nil && !goja.IsUndefined(input.seed) {
			_ = options.Set("seed", input.seed)
		}
	}
	prepared, err := e.dynamicCordisPrepareSession(run, input.id, options)
	if err != nil {
		if releaseReservation != nil {
			releaseReservation()
		}
		return nil, err
	}
	prepared.reservationRelease = releaseReservation
	if releaseReservation != nil {
		run.disposers = append(run.disposers, func() {
			if prepared.reservationRelease != nil {
				release := prepared.reservationRelease
				prepared.reservationRelease = nil
				release()
			}
		})
	}
	return prepared, nil
}

func (e *Engine) dynamicCordisApplyAgentOptions(session *Session, input dynamicCordisAgentCreationInput) error {
	session.mu.Lock()
	selection := cloneModelSelection(session.Model)
	if input.options.Provider != "" {
		selection.Provider = input.options.Provider
	}
	if input.options.Model != "" {
		selection.Model = input.options.Model
	}
	if input.options.MaxTokens != nil {
		selection.MaxTokens = *input.options.MaxTokens
	}
	session.Model = selection
	session.mu.Unlock()
	if selection.Provider == "" || selection.Model == "" {
		return errors.New("bad-request: provider and model are required")
	}
	e.mu.RLock()
	_, available := e.providers[selection.Provider]
	e.mu.RUnlock()
	if !available {
		return fmt.Errorf("model-unavailable: %s/%s", selection.Provider, selection.Model)
	}
	return nil
}

func (e *Engine) dynamicCordisCreateAgent(run *dynamicCordisRun, value goja.Value, resume bool, owner string) goja.Value {
	vm := run.runtime
	input := dynamicCordisAgentCreationOptions(vm, value, resume)
	if entry := e.dynamicCordisAgentEntry(input.id); entry != nil && !entry.detached {
		panic(vm.ToValue(fmt.Sprintf("agent %q is already registered", input.id)))
	}
	prepared, err := e.dynamicCordisPrepareAgentSession(run, input, resume)
	if err != nil {
		panic(vm.ToValue(err.Error()))
	}
	session := prepared.session
	if err := e.dynamicCordisApplyAgentOptions(session, input); err != nil {
		e.discardDynamicCordisPreparedSession(run, input.id)
		panic(vm.ToValue(err.Error()))
	}

	agent := e.dynamicCordisAgentValue(run, session)
	agentCtx := vm.NewObject()
	if err := agentCtx.SetPrototype(run.ctx); err != nil {
		e.discardDynamicCordisPreparedSession(run, input.id)
		panic(vm.ToValue(err.Error()))
	}
	_ = agentCtx.Set("agent", agent)
	_ = agent.Set("ctx", agentCtx)
	if resume {
		_ = agent.Set("resumed", true)
	}

	promise, resolve, reject := vm.NewPromise()
	setupStart, setupEnd := len(run.disposers), -1
	cleanupNext := setupStart
	cleanupSetup := func() {
		end := len(run.disposers)
		if setupEnd >= 0 && setupEnd < end {
			end = setupEnd
		}
		if end <= cleanupNext {
			return
		}
		for index := end - 1; index >= cleanupNext; index-- {
			run.disposers[index]()
		}
		cleanupNext = end
	}
	var registryDetach func()
	entered, finished, rolledBack := false, false, false
	rollback := func() {
		if rolledBack {
			cleanupSetup()
			return
		}
		rolledBack = true
		if registryDetach != nil {
			registryDetach()
		}
		if entered {
			e.dynamicCordisDetachPrepared(run, prepared)
		} else {
			e.discardDynamicCordisPreparedSession(run, input.id)
		}
		cleanupSetup()
	}
	removeAbort := func() {}
	fail := func(reason goja.Value) {
		if finished {
			return
		}
		finished = true
		removeAbort()
		rollback()
		_ = reject(reason)
	}
	aborted := func() bool {
		return input.signal != nil && input.signal.Get("aborted").ToBoolean()
	}
	if input.signal != nil {
		if add, ok := goja.AssertFunction(input.signal.Get("addEventListener")); ok {
			onAbort := vm.ToValue(func(goja.FunctionCall) goja.Value {
				fail(dynamicCordisAgentAbortValue(vm, input.id, input.signal))
				return goja.Undefined()
			})
			if _, err := add(input.signal, vm.ToValue("abort"), onAbort, vm.ToValue(map[string]any{"once": true})); err != nil {
				fail(dynamicCordisAgentErrorValue(vm, err))
				return vm.ToValue(promise)
			}
			if remove, ok := goja.AssertFunction(input.signal.Get("removeEventListener")); ok {
				removeAbort = func() { _, _ = remove(input.signal, vm.ToValue("abort"), onAbort) }
			}
		}
	}
	if aborted() {
		fail(dynamicCordisAgentAbortValue(vm, input.id, input.signal))
		return vm.ToValue(promise)
	}

	publish := func(setupValue goja.Value, setupErr error) {
		if finished {
			cleanupSetup()
			return
		}
		setupEnd = len(run.disposers)
		if setupErr != nil {
			fail(dynamicCordisAgentErrorValue(vm, setupErr))
			return
		}
		if aborted() {
			fail(dynamicCordisAgentAbortValue(vm, input.id, input.signal))
			return
		}
		if object, ok := setupValue.(*goja.Object); ok && object != nil {
			if commitValue := object.Get("commit"); commitValue != nil && !goja.IsUndefined(commitValue) && !goja.IsNull(commitValue) {
				commit, ok := goja.AssertFunction(commitValue)
				if !ok {
					fail(vm.NewGoError(errors.New("agents setup commit must be a function")))
					return
				}
				if _, err := commit(object); err != nil {
					fail(dynamicCordisAgentErrorValue(vm, err))
					return
				}
			}
		}
		if aborted() {
			fail(dynamicCordisAgentAbortValue(vm, input.id, input.signal))
			return
		}
		if err := e.dynamicCordisEnterPrepared(run, prepared); err != nil {
			fail(vm.NewGoError(err))
			return
		}
		entered = true
		registryDetach, err = e.dynamicCordisAgentEnter(run, input.id, input.id, owner, false)
		if err != nil {
			fail(vm.NewGoError(err))
			return
		}
		if prepared.reservationRelease != nil {
			release := prepared.reservationRelease
			prepared.reservationRelease = nil
			release()
		}

		var disposeOnce sync.Once
		var disposeErr error
		dispose := func(remove bool) error {
			disposeOnce.Do(func() {
				current, getErr := e.getSession(input.id)
				if getErr != nil || current != session || rolledBack {
					return
				}
				if err := detachSDKSessionWithDynamicOrigin(e, run, input.id); err != nil {
					disposeErr = err
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
				defer cancel()
				_ = e.WaitForIdle(ctx, input.id)
				registryDetach()
				e.discardDynamicCordisPreparedSession(run, input.id)
				cleanupSetup()
			})
			if remove {
				session.mu.Lock()
				attached := session.attached
				session.mu.Unlock()
				if !attached {
					e.mu.Lock()
					if e.sessions[input.id] == session {
						delete(e.sessions, input.id)
					}
					e.mu.Unlock()
				}
			}
			return disposeErr
		}
		run.disposers = append(run.disposers, func() { _ = dispose(true) })

		prepared.announced, prepared.announcing = true, true
		if err := e.emitDynamicCordisEventFrom(run, "session/created", dynamicSessionView(session)); err != nil {
			prepared.announcing = false
			fail(dynamicCordisAgentErrorValue(vm, err))
			return
		}
		prepared.announcing = false
		if prepared.detachRequested {
			e.dynamicCordisDetachPrepared(run, prepared)
		}
		if finished || aborted() {
			if !finished {
				fail(dynamicCordisAgentAbortValue(vm, input.id, input.signal))
			}
			return
		}
		if err := e.dynamicCordisAgentAnnounce(run, input.id); err != nil {
			fail(dynamicCordisAgentErrorValue(vm, err))
			return
		}
		if finished || aborted() {
			if !finished {
				fail(dynamicCordisAgentAbortValue(vm, input.id, input.signal))
			}
			return
		}
		source := "startup"
		if resume {
			source = "resume"
		}
		if err := e.emitDynamicCordisEventFrom(run, "agent/session-start", map[string]any{
			"agent": e.dynamicCordisAgentEvent(run, input.id), "source": source,
		}); err != nil {
			fail(dynamicCordisAgentErrorValue(vm, err))
			return
		}
		if finished || aborted() {
			if !finished {
				fail(dynamicCordisAgentAbortValue(vm, input.id, input.signal))
			}
			return
		}
		finished = true
		removeAbort()
		_ = resolve(e.dynamicCordisAgentHandle(run, agent, func() error { return dispose(false) }))
	}

	if input.setup == nil {
		publish(goja.Undefined(), nil)
		return vm.ToValue(promise)
	}
	setupValue, setupErr := input.setup(goja.Undefined(), agentCtx)
	if setupErr != nil {
		publish(nil, setupErr)
		return vm.ToValue(promise)
	}
	if _, ok := setupValue.Export().(*goja.Promise); ok {
		dynamicCordisAwaitOnLoop(run, setupValue, publish)
	} else {
		publish(setupValue, nil)
	}
	return vm.ToValue(promise)
}

func (e *Engine) dynamicCordisAgentLoopFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	config := map[string]any{"provider": e.cfg.Provider, "model": e.cfg.Model}
	_ = service.Set("config", config)
	_ = service.Set("create", func(call goja.FunctionCall) goja.Value {
		id := strings.TrimSpace(call.Argument(0).String())
		if id == "" {
			panic(vm.ToValue("agentLoop.create requires a non-empty session id"))
		}
		options := call.Argument(1)
		meta := call.Argument(2)
		input := map[string]any{"sessionId": id}
		if options != nil && !goja.IsUndefined(options) {
			input["agentOptions"] = options.Export()
		}
		if meta != nil && !goja.IsUndefined(meta) {
			input["meta"] = meta.Export()
		}
		handle := e.dynamicCordisCreateAgent(run, vm.ToValue(input), false, dynamicCordisAgentOwner(run))
		if promise, ok := handle.Export().(*goja.Promise); ok {
			switch promise.State() {
			case goja.PromiseStateFulfilled:
				handle = promise.Result()
			case goja.PromiseStateRejected:
				panic(promise.Result())
			default:
				panic(vm.ToValue("agentLoop.create setup must settle synchronously"))
			}
		}
		object, ok := handle.(*goja.Object)
		if !ok {
			panic(vm.ToValue("agentLoop.create returned an invalid AgentHandle"))
		}
		return object.Get("agent")
	})
	_ = service.Set("resume", func(call goja.FunctionCall) goja.Value {
		options := call.Argument(0)
		if len(call.Arguments) > 1 {
			options = call.Argument(1)
		}
		owner := dynamicCordisAgentOwner(run)
		return e.dynamicCordisAsyncValue(run, func() goja.Value {
			return e.dynamicCordisCreateAgent(run, options, true, owner)
		})
	})
	return service
}
