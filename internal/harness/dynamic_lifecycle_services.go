package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
)

func dynamicSessionHeaderValue(header SessionHeader) map[string]any {
	value := map[string]any{
		"version": header.Version, "id": header.ID, "createdAt": header.CreatedAt,
		"isSeeded": header.IsSeeded,
	}
	if header.CWD != "" {
		value["cwd"] = header.CWD
	}
	if header.ParentSession != "" {
		value["parentSession"] = header.ParentSession
	}
	if header.Origin != "" {
		value["origin"] = header.Origin
	}
	if header.DelegationDepth != 0 {
		value["delegationDepth"] = header.DelegationDepth
	}
	if header.AgentPreset != "" {
		value["agentPreset"] = header.AgentPreset
	}
	if header.Mode != "" {
		value["mode"] = header.Mode
	}
	return value
}

func dynamicSessionEventValue(event Event) map[string]any {
	value := map[string]any{"type": event.Type, "seq": int(event.Seq), "time": event.Time, "data": cloneJSON(event.Data)}
	if event.SourceEventSeqs != nil {
		value["sourceEventSeqs"] = append([]int(nil), event.SourceEventSeqs...)
	}
	if event.SurfaceOp != nil {
		value["surfaceOp"] = cloneJSON(event.SurfaceOp)
	}
	if event.Ignorable {
		value["ignorable"] = true
	}
	return value
}

func dynamicCordisSessionValue(vm *goja.Runtime, session *Session) *goja.Object {
	value := vm.NewObject()
	_ = value.DefineAccessorProperty("id", vm.ToValue(func() string { session.mu.Lock(); defer session.mu.Unlock(); return session.Header.ID }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = value.DefineAccessorProperty("header", vm.ToValue(func() any {
		session.mu.Lock()
		defer session.mu.Unlock()
		return dynamicSessionHeaderValue(session.Header)
	}), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = value.DefineAccessorProperty("firstLiveSeq", vm.ToValue(func() int { session.mu.Lock(); defer session.mu.Unlock(); return session.firstLiveSeq }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = value.DefineAccessorProperty("inheritedEventCount", vm.ToValue(func() int { session.mu.Lock(); defer session.mu.Unlock(); return int(session.InheritedEventCount) }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = value.DefineAccessorProperty("seq", vm.ToValue(func() int { session.mu.Lock(); defer session.mu.Unlock(); return len(session.Events) }), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	_ = value.DefineAccessorProperty("events", vm.ToValue(func() any {
		session.mu.Lock()
		events := make([]any, len(session.Events))
		for i, event := range session.Events {
			events[i] = dynamicSessionEventValue(event)
		}
		session.mu.Unlock()
		return events
	}), nil, goja.FLAG_FALSE, goja.FLAG_TRUE)
	return value
}

func (e *Engine) dynamicCordisSessionValue(run *dynamicCordisRun, session *Session) *goja.Object {
	value := dynamicCordisSessionValue(run.runtime, session)
	_ = value.Set("append", func(call goja.FunctionCall) goja.Value {
		typ := strings.TrimSpace(call.Argument(0).String())
		if typ == "" {
			panic(run.runtime.ToValue("session.append requires a non-empty type"))
		}
		data, err := dynamicCordisJSONValue(call.Argument(1))
		if err != nil {
			panic(run.runtime.ToValue("session.append data: " + err.Error()))
		}
		var surface any
		var sources []int
		ignorable := false
		options := call.Argument(2)
		if options != nil && !goja.IsUndefined(options) && !goja.IsNull(options) {
			object, ok := options.(*goja.Object)
			if !ok {
				panic(run.runtime.ToValue("session.append options must be an object"))
			}
			if raw := object.Get("surfaceOp"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
				surface, err = dynamicCordisJSONValue(raw)
				if err != nil {
					panic(run.runtime.ToValue("session.append surfaceOp: " + err.Error()))
				}
			}
			if raw := object.Get("sourceEventSeqs"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
				if err := dynamicCordisDecode(raw, &sources); err != nil {
					panic(run.runtime.ToValue("session.append sourceEventSeqs: " + err.Error()))
				}
			}
			if raw := object.Get("ignorable"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
				ignorable = raw.ToBoolean()
			}
		}
		if isSurfaceEligibleType(typ) && surface == nil {
			panic(run.runtime.ToValue(fmt.Sprintf("event %q requires surfaceOp", typ)))
		}
		if !isSurfaceEligibleType(typ) && (surface != nil || sources != nil) {
			panic(run.runtime.ToValue(fmt.Sprintf("event %q cannot carry surface metadata", typ)))
		}
		event, err := e.dynamicCordisAppendSessionEvent(run, session, typ, data, surface, sources, ignorable)
		if err != nil {
			panic(run.runtime.ToValue(err.Error()))
		}
		return run.runtime.ToValue(dynamicSessionEventValue(event))
	})
	return value
}

func (e *Engine) dynamicCordisAppendSessionEvent(run *dynamicCordisRun, session *Session, typ string, data any, surface any, sources []int, ignorable bool) (Event, error) {
	session.mu.Lock()
	if err := validateDynamicSessionEvent(typ, len(session.Events), surface, sources); err != nil {
		session.mu.Unlock()
		return Event{}, err
	}
	attached := session.attached
	id := session.Header.ID
	event, err := appendEventLocked(session, typ, data, surface, sources, ignorable)
	session.mu.Unlock()
	if err != nil {
		return Event{}, err
	}
	if attached {
		e.publishEventFrom(run, id, event)
	}
	e.observeSessionTitleEvent(session, event)
	return event, nil
}

func validateDynamicSessionEvent(typ string, seq int, surface any, sources []int) error {
	if isSurfaceEligibleType(typ) {
		if surface == nil {
			return fmt.Errorf("event %q requires surfaceOp", typ)
		}
		raw, err := json.Marshal(surface)
		if err != nil {
			return fmt.Errorf("event %q has invalid surfaceOp: %w", typ, err)
		}
		if err := validateSurfaceOp(raw); err != nil {
			return fmt.Errorf("event %q: %w", typ, err)
		}
	} else if surface != nil || sources != nil {
		return fmt.Errorf("event %q cannot carry surface metadata", typ)
	}
	if sources != nil {
		if err := validateSourceEventSeqs(Event{Type: typ, Seq: SessionSeq(seq), SourceEventSeqs: sources}); err != nil {
			return err
		}
	}
	return nil
}

type dynamicCordisPreparedSession struct {
	session            *Session
	restored           bool
	handle             SessionHandle
	storedEventCount   int
	reservationRelease func()
	entered            bool
	announced          bool
	announcing         bool
	detachRequested    bool
	detachOnce         sync.Once
}

func (e *Engine) discardDynamicCordisPreparedSession(run *dynamicCordisRun, id string) {
	prepared := run.preparedSessions[id]
	if prepared == nil {
		return
	}
	if prepared.reservationRelease != nil {
		release := prepared.reservationRelease
		prepared.reservationRelease = nil
		release()
	}
	if prepared.handle != nil {
		_ = prepared.handle.Close()
		prepared.handle = nil
	}
	delete(run.preparedSessions, id)
}

func (e *Engine) dynamicCordisPrepareSession(run *dynamicCordisRun, id string, options goja.Value) (*dynamicCordisPreparedSession, error) {
	var input struct {
		Meta                *SessionHeader    `json:"meta"`
		InheritedEventCount *SessionLogOffset `json:"inheritedEventCount"`
		Seed                []any             `json:"seed"`
		SeedSource          string            `json:"seedSource"`
	}
	seedProvided, createdAtProvided, cwdProvided := false, false, false
	if options != nil && !goja.IsUndefined(options) && !goja.IsNull(options) {
		object, ok := options.(*goja.Object)
		if !ok {
			return nil, errors.New("sessions.prepare options: expected an object")
		}
		if raw := object.Get("seed"); raw != nil && !goja.IsUndefined(raw) {
			seedProvided = true
			if goja.IsNull(raw) {
				return nil, errors.New("sessions.prepare options: seed must be an array")
			}
		}
		if raw := object.Get("meta"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
			metaObject, ok := raw.(*goja.Object)
			if !ok {
				return nil, errors.New("sessions.prepare options: meta must be an object")
			}
			for _, key := range []string{"cwd", "parentSession", "createdAt", "isSeeded", "origin", "delegationDepth", "agentPreset"} {
				if field := metaObject.Get(key); field != nil && !goja.IsUndefined(field) && goja.IsNull(field) {
					return nil, fmt.Errorf("sessions.prepare options: meta.%s is invalid", key)
				}
			}
			if field := metaObject.Get("createdAt"); field != nil && !goja.IsUndefined(field) && !goja.IsNull(field) {
				createdAtProvided = true
			}
			if field := metaObject.Get("cwd"); field != nil && !goja.IsUndefined(field) && !goja.IsNull(field) {
				cwdProvided = true
			}
		}
		if err := dynamicCordisDecode(options, &input); err != nil {
			return nil, fmt.Errorf("sessions.prepare options: %w", err)
		}
	}
	if input.SeedSource != "" && input.SeedSource != "persistence" {
		return nil, fmt.Errorf("sessions.prepare options: unsupported seedSource %q", input.SeedSource)
	}
	if input.SeedSource == "persistence" && !seedProvided {
		return nil, errors.New("sessions.prepare options: seedSource persistence requires seed")
	}
	if input.SeedSource == "persistence" && input.Meta == nil {
		return nil, errors.New("sessions.prepare options: seedSource persistence requires meta")
	}
	if id == "" {
		id = newID("ses")
	}
	if _, exists := run.preparedSessions[id]; exists {
		return nil, fmt.Errorf("session %q already exists", id)
	}
	if existing, err := e.getSession(id); err == nil {
		existing.mu.Lock()
		attached := existing.attached
		existing.mu.Unlock()
		if input.SeedSource != "persistence" || attached {
			return nil, fmt.Errorf("session %q already exists", id)
		}
	}
	meta := SessionHeader{}
	if input.Meta != nil {
		meta = *input.Meta
	}
	if input.SeedSource == "persistence" {
		if meta.ID != id {
			return nil, fmt.Errorf("restored session header id %q does not match session id %q", meta.ID, id)
		}
		if meta.Version != SessionFormatVersion {
			return nil, fmt.Errorf("unsupported session format version %d", meta.Version)
		}
	}
	meta.Version, meta.ID = SessionFormatVersion, id
	if cwdProvided && !filepath.IsAbs(meta.CWD) {
		return nil, fmt.Errorf("session header cwd must be an absolute path, got %q", meta.CWD)
	}
	if !createdAtProvided {
		meta.CreatedAt = time.Now().UnixMilli()
	}
	inheritedEventCount := SessionLogOffset(0)
	if input.InheritedEventCount != nil {
		inheritedEventCount = *input.InheritedEventCount
	}
	if err := validateSessionLogOffset(inheritedEventCount); err != nil {
		return nil, err
	}
	if meta.IsSeeded && !seedProvided {
		return nil, errors.New("seeded session requires an explicit seed")
	}
	if meta.IsSeeded && input.InheritedEventCount == nil {
		return nil, errors.New("seeded session requires an inherited event count")
	}
	if !meta.IsSeeded && inheritedEventCount != 0 {
		return nil, errors.New("unseeded session inherited event count must be 0")
	}
	meta.SeedLength = int(inheritedEventCount)
	if err := validateSessionHeader(meta); err != nil {
		return nil, err
	}
	var parentEvents []Event
	if meta.Origin == "subagent" && meta.ParentSession != "" {
		if parent, parentErr := e.getSession(meta.ParentSession); parentErr == nil {
			parent.mu.Lock()
			parentEvents = append([]Event(nil), parent.Events...)
			parent.mu.Unlock()
		}
	}
	session := &Session{
		Header: meta, InheritedEventCount: inheritedEventCount,
		Model: ModelSelection{Provider: e.cfg.Provider, Model: e.cfg.Model},
		store: nil, invariants: e.invariants,
		// A dynamically prepared session becomes eligible only when it is a
		// fresh root publication. Seeded and persistence-backed sessions must
		// never resample host settings.
		subagentModelSelectionEligible: !seedProvided && input.SeedSource != "persistence" && meta.Origin != "subagent" && meta.ParentSession == "",
	}
	session.mu.Lock()
	for index, raw := range input.Seed {
		seed, err := decodeSessionSeedEvent(raw, index)
		if err != nil {
			session.mu.Unlock()
			return nil, err
		}
		if err := validateDynamicSessionEvent(seed.Type, len(session.Events), seed.SurfaceOp, seed.SourceEventSeqs); err != nil {
			session.mu.Unlock()
			return nil, fmt.Errorf("invalid seed event at index %d: %w", index, err)
		}
		if _, err := appendSeedEventLocked(session, seed); err != nil {
			session.mu.Unlock()
			return nil, err
		}
	}
	if int(inheritedEventCount) > len(session.Events) {
		session.mu.Unlock()
		return nil, errors.New("session inherited event count exceeds its event log")
	}
	session.firstLiveSeq = len(session.Events)
	if seedProvided && (len(session.Events) == 0 || session.Events[len(session.Events)-1].Type != "session/end-seed") {
		if _, err := appendEventLocked(session, "session/end-seed", map[string]any{}, nil, nil, false); err != nil {
			session.mu.Unlock()
			return nil, err
		}
	}
	if _, err := foldSurfaceEvents(session.Events, true); err != nil {
		session.mu.Unlock()
		return nil, fmt.Errorf("invalid session seed: %w", err)
	}
	pending, steering, err := restorePromptQueues(session.Events)
	if err != nil {
		session.mu.Unlock()
		return nil, fmt.Errorf("invalid session seed: %w", err)
	}
	session.pending, session.steering = pending, steering
	session.mu.Unlock()
	// Dynamic agent/session creation can identify a delegated child directly in
	// its metadata. Copy the parent's durable route policy before entering the
	// store or exposing the prepared session, so setup and publication observe
	// one immutable decision.
	if meta.Origin == "subagent" && meta.ParentSession != "" {
		if err := e.inheritSubagentModelSelection(session, parentEvents); err != nil {
			return nil, err
		}
	}
	prepared := &dynamicCordisPreparedSession{session: session, restored: input.SeedSource == "persistence"}
	run.preparedSessions[id] = prepared
	return prepared, nil
}

func (e *Engine) dynamicCordisEnterPrepared(run *dynamicCordisRun, prepared *dynamicCordisPreparedSession) error {
	if prepared == nil || prepared.session == nil {
		return errors.New("session is not prepared")
	}
	if prepared.entered {
		return errors.New("session is already attached to a store")
	}
	session := prepared.session
	session.mu.Lock()
	header := session.Header
	priorStore := session.store
	priorAttached := session.attached
	priorAttachmentGeneration := session.attachmentGeneration
	priorModelSelectionSnapshot := session.subagentModelSelectionSnapshot
	priorModelSelectionSampled := session.subagentModelSelectionSampled
	priorEventCount := len(session.Events)
	session.mu.Unlock()
	e.mu.RLock()
	if existing := e.sessions[header.ID]; existing != nil && existing != session {
		existing.mu.Lock()
		attached := existing.attached
		existing.mu.Unlock()
		if !prepared.restored || attached {
			e.mu.RUnlock()
			return fmt.Errorf("session %q already exists", header.ID)
		}
	}
	store := e.sessionStore
	e.mu.RUnlock()
	handle := prepared.handle
	storedEventCount := prepared.storedEventCount
	if store != nil && handle == nil {
		if prepared.restored {
			var inspection SessionInspection
			var err error
			handle, inspection, err = openStoredSessionForWrite(context.Background(), store, header.ID)
			if err != nil {
				return err
			}
			storedEventCount = len(inspection.Events)
		} else {
			var err error
			handle, err = store.Create(context.Background(), header, session.InheritedEventCount)
			if err != nil {
				return err
			}
		}
	}
	e.mu.Lock()
	var modelSelectionEvent Event
	modelSelectionRecorded := false
	if !prepared.restored && e.hasSubagentModelSelectionToolLocked() {
		session.mu.Lock()
		e.captureSubagentModelSelectionSnapshotLocked(session)
		var recordErr error
		modelSelectionEvent, modelSelectionRecorded, recordErr = e.recordSubagentModelSelectionPolicyLocked(session)
		session.mu.Unlock()
		if recordErr != nil {
			session.mu.Lock()
			session.store = priorStore
			session.attached = priorAttached
			session.attachmentGeneration = priorAttachmentGeneration
			session.subagentModelSelectionSnapshot = priorModelSelectionSnapshot
			session.subagentModelSelectionSampled = priorModelSelectionSampled
			session.Events = session.Events[:priorEventCount]
			session.mu.Unlock()
			prepared.entered = false
			e.mu.Unlock()
			if handle != nil {
				_ = handle.Close()
			}
			return recordErr
		}
	}
	session.mu.Lock()
	events := cloneSessionEvents(session.Events)
	if storedEventCount < 0 || storedEventCount > len(events) {
		session.mu.Unlock()
		e.mu.Unlock()
		if handle != nil {
			_ = handle.Close()
		}
		return fmt.Errorf("session %q stored event count %d exceeds log length %d", header.ID, storedEventCount, len(events))
	}
	if handle != nil && storedEventCount < len(events) {
		if err := handle.Append(context.Background(), events[storedEventCount:]); err != nil {
			session.store = priorStore
			session.attached = priorAttached
			session.attachmentGeneration = priorAttachmentGeneration
			session.subagentModelSelectionSnapshot = priorModelSelectionSnapshot
			session.subagentModelSelectionSampled = priorModelSelectionSampled
			session.Events = session.Events[:priorEventCount]
			session.mu.Unlock()
			e.mu.Unlock()
			_ = handle.Close()
			return err
		}
		storedEventCount = len(events)
	}
	session.store = handle
	session.storedEventCount = storedEventCount
	session.published = true
	attachSessionLocked(session)
	session.mu.Unlock()
	prepared.handle = nil
	prepared.storedEventCount = storedEventCount
	e.sessions[header.ID] = session
	e.mu.Unlock()
	if modelSelectionRecorded {
		e.publishEvent(header.ID, modelSelectionEvent)
	}
	prepared.entered = true
	return nil
}

func (e *Engine) dynamicCordisDetachPrepared(run *dynamicCordisRun, prepared *dynamicCordisPreparedSession) {
	if prepared == nil || prepared.session == nil {
		return
	}
	if prepared.announcing {
		prepared.detachRequested = true
		return
	}
	prepared.detachOnce.Do(func() {
		session := prepared.session
		session.mu.Lock()
		id := session.Header.ID
		detachSessionLocked(session)
		handle := session.store
		session.store = nil
		activity := session.activity
		cancel := session.Cancel
		session.Cancel = nil
		session.activity = nil
		session.mu.Unlock()
		if activity != nil {
			activity.cancel(&agentCancelError{cause: AgentCancelCause{Kind: "disposed"}})
		} else if cancel != nil {
			cancel()
		}
		if handle != nil {
			_ = handle.Close()
		}
		e.releaseFileReferenceSearch(id)
		_ = e.jobs.disposeOwner(id, "owner disposed")
		prepared.entered = false
		e.discardDynamicCordisPreparedSession(run, id)
		e.mu.Lock()
		if e.sessions[id] == session {
			delete(e.sessions, id)
		}
		e.mu.Unlock()
		if prepared.announced {
			_ = e.emitDynamicCordisEventFrom(run, "session/disposed", dynamicSessionView(session))
		}
	})
}

func (e *Engine) dynamicCordisOwnPreparedSession(run *dynamicCordisRun, prepared *dynamicCordisPreparedSession) {
	if prepared == nil || prepared.session == nil {
		return
	}
	run.disposers = append(run.disposers, func() {
		e.dynamicCordisDetachPrepared(run, prepared)
	})
}

func (e *Engine) dynamicCordisForkSession(run *dynamicCordisRun, sourceID string, boundary *int, childID string) (*Session, error) {
	source, err := e.getSession(sourceID)
	if err != nil {
		return nil, err
	}
	source.mu.Lock()
	if !source.attached {
		source.mu.Unlock()
		return nil, fmt.Errorf("session %q is not live", sourceID)
	}
	meta := source.Header
	events := append([]Event(nil), source.Events...)
	source.mu.Unlock()
	if boundary != nil {
		if *boundary < 0 || *boundary >= len(events) {
			return nil, fmt.Errorf("fork boundary %d does not exist in session %q", *boundary, sourceID)
		}
		for index, event := range events {
			if int(event.Seq) != index {
				return nil, fmt.Errorf("fork boundary %d does not match a contiguous event seq in session %q", *boundary, sourceID)
			}
		}
		events = events[:*boundary+1]
	}
	openTurn := false
	for _, event := range events {
		switch event.Type {
		case "turn/start":
			openTurn = true
		case "turn/end":
			openTurn = false
		}
	}
	if openTurn {
		if boundary == nil {
			return nil, fmt.Errorf("fork boundary in session %q ends inside an open turn", sourceID)
		}
		return nil, fmt.Errorf("fork boundary %d in session %q ends inside an open turn", *boundary, sourceID)
	}
	if _, err := foldSurfaceEvents(events, true); err != nil {
		return nil, fmt.Errorf("invalid fork seed for session %q: %w", sourceID, err)
	}
	if childID == "" {
		childID = newID("ses")
	}
	seedRows := make([]any, len(events))
	for index, event := range events {
		seedRows[index] = dynamicSessionEventValue(event)
	}
	metaMap := map[string]any{
		"cwd": meta.CWD, "parentSession": sourceID, "isSeeded": true,
	}
	prepared, err := e.dynamicCordisPrepareSession(run, childID, run.runtime.ToValue(map[string]any{
		"meta": metaMap, "seed": seedRows, "inheritedEventCount": len(events),
	}))
	if err != nil {
		return nil, err
	}
	if err := e.dynamicCordisEnterPrepared(run, prepared); err != nil {
		e.discardDynamicCordisPreparedSession(run, childID)
		return nil, err
	}
	e.dynamicCordisOwnPreparedSession(run, prepared)
	prepared.announced = true
	prepared.session.mu.Lock()
	prepared.session.published = true
	prepared.session.mu.Unlock()
	if err := e.emitDynamicCordisEventFrom(run, "session/created", dynamicSessionView(prepared.session)); err != nil {
		e.dynamicCordisDetachPrepared(run, prepared)
		return nil, err
	}
	return prepared.session, nil
}

func (e *Engine) dynamicCordisSessionFlush(run *dynamicCordisRun, session *Session) goja.Value {
	vm := run.runtime
	promise, resolve, reject := vm.NewPromise()
	session.mu.Lock()
	live := session.attached
	session.mu.Unlock()
	if !live {
		_ = reject(vm.NewGoError(fmt.Errorf("session %q is not live in the store", session.Header.ID)))
		return vm.ToValue(promise)
	}
	e.dynamicCordis.RLock()
	listeners := make([]*dynamicCordisListener, 0)
	seen := map[*dynamicCordisRun]struct{}{}
	for _, plugin := range e.dynamicCordis.plugins {
		listenerRun := plugin.run
		if listenerRun == nil || listenerRun.sessionID != session.Header.ID {
			continue
		}
		if _, ok := seen[listenerRun]; ok {
			continue
		}
		seen[listenerRun] = struct{}{}
		listenerRun.eventMu.RLock()
		listeners = append(listeners, listenerRun.listeners["session/flush"]...)
		listenerRun.eventMu.RUnlock()
	}
	e.dynamicCordis.RUnlock()
	sort.Slice(listeners, func(i, j int) bool { return dynamicCordisListenerLess(listeners[i], listeners[j]) })
	if len(listeners) == 0 {
		_ = resolve(vm.ToValue(false))
		return vm.ToValue(promise)
	}
	remaining, settled := 0, 0
	invoked := false
	var firstErr error
	finish := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
		settled++
		if !invoked || settled != remaining {
			return
		}
		if firstErr != nil {
			_ = reject(vm.NewGoError(firstErr))
		} else {
			_ = resolve(vm.ToValue(true))
		}
	}
	for _, listener := range listeners {
		listenerRun := listener.run
		listenerRun.mu.Lock()
		active := !listenerRun.disposed && (listenerRun.active || listenerRun.activating)
		listenerRun.mu.Unlock()
		if !active {
			continue
		}
		remaining++
		value, err := listener.fn(goja.Undefined(), listenerRun.runtime.ToValue(dynamicSessionView(session)))
		if err != nil {
			finish(err)
			continue
		}
		if _, ok := value.Export().(*goja.Promise); !ok {
			finish(nil)
			continue
		}
		dynamicCordisAwaitOnLoop(listenerRun, value, func(_ goja.Value, err error) { finish(err) })
	}
	invoked = true
	if settled == remaining && remaining > 0 {
		if firstErr != nil {
			_ = reject(vm.NewGoError(firstErr))
		} else {
			_ = resolve(vm.ToValue(true))
		}
	}
	if remaining == 0 {
		_ = resolve(vm.ToValue(false))
	}
	return vm.ToValue(promise)
}

func (e *Engine) dynamicCordisSessionsFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	makeSession := func(id string) goja.Value {
		session, err := e.getSession(id)
		if err != nil {
			return goja.Undefined()
		}
		session.mu.Lock()
		live := session.attached
		session.mu.Unlock()
		if !live {
			return goja.Undefined()
		}
		return e.dynamicCordisSessionValue(run, session)
	}
	create := func(call goja.FunctionCall) goja.Value {
		id := ""
		options := call.Argument(1)
		if _, ok := call.Argument(0).(*goja.Object); ok && (options == nil || goja.IsUndefined(options)) {
			options = call.Argument(0)
		} else if call.Argument(0) != nil && !goja.IsUndefined(call.Argument(0)) && !goja.IsNull(call.Argument(0)) {
			id = strings.TrimSpace(call.Argument(0).String())
		}
		if id != "" {
			if _, err := e.getSession(id); err == nil {
				panic(vm.ToValue(fmt.Sprintf("session %q already exists", id)))
			}
		}
		return e.dynamicCordisCreateSession(run, id, options)
	}
	_ = service.Set("create", create)
	_ = service.Set("prepare", func(call goja.FunctionCall) goja.Value {
		id := ""
		options := call.Argument(1)
		if call.Argument(0) != nil && !goja.IsUndefined(call.Argument(0)) && !goja.IsNull(call.Argument(0)) {
			id = strings.TrimSpace(call.Argument(0).String())
		}
		if _, ok := call.Argument(0).(*goja.Object); ok && (options == nil || goja.IsUndefined(options)) {
			options = call.Argument(0)
			id = ""
		}
		prepared, err := e.dynamicCordisPrepareSession(run, id, options)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return e.dynamicCordisSessionValue(run, prepared.session)
	})
	_ = service.Set("enter", func(call goja.FunctionCall) goja.Value {
		id := dynamicCordisSessionID(vm, call.Argument(0))
		prepared := run.preparedSessions[id]
		if err := e.dynamicCordisEnterPrepared(run, prepared); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		e.dynamicCordisOwnPreparedSession(run, prepared)
		return vm.ToValue(func() { e.dynamicCordisDetachPrepared(run, prepared) })
	})
	_ = service.Set("announce", func(call goja.FunctionCall) goja.Value {
		id := dynamicCordisSessionID(vm, call.Argument(0))
		prepared := run.preparedSessions[id]
		if prepared == nil || !prepared.entered {
			panic(vm.ToValue("session is not live in the store"))
		}
		if prepared.announced {
			panic(vm.ToValue(fmt.Sprintf("session %q was already announced", id)))
		}
		prepared.announced, prepared.announcing = true, true
		prepared.session.mu.Lock()
		prepared.session.published = true
		prepared.session.mu.Unlock()
		if err := e.emitDynamicCordisEventFrom(run, "session/created", dynamicSessionView(prepared.session)); err != nil {
			prepared.announcing = false
			e.dynamicCordisDetachPrepared(run, prepared)
			panic(vm.ToValue(err.Error()))
		}
		prepared.announcing = false
		if prepared.detachRequested {
			e.dynamicCordisDetachPrepared(run, prepared)
		}
		return goja.Undefined()
	})
	_ = service.Set("get", func(call goja.FunctionCall) goja.Value {
		return makeSession(dynamicCordisSessionID(vm, call.Argument(0)))
	})
	_ = service.Set("list", func(goja.FunctionCall) goja.Value {
		e.mu.RLock()
		sessions := make([]*Session, 0, len(e.sessions))
		for _, session := range e.sessions {
			sessions = append(sessions, session)
		}
		e.mu.RUnlock()
		sort.SliceStable(sessions, func(i, j int) bool {
			sessions[i].mu.Lock()
			ai, idi := sessions[i].Header.CreatedAt, sessions[i].Header.ID
			sessions[i].mu.Unlock()
			sessions[j].mu.Lock()
			aj, idj := sessions[j].Header.CreatedAt, sessions[j].Header.ID
			sessions[j].mu.Unlock()
			if ai == aj {
				return idi < idj
			}
			return ai < aj
		})
		rows := make([]any, 0, len(sessions))
		for _, session := range sessions {
			session.mu.Lock()
			live := session.attached
			session.mu.Unlock()
			if live {
				rows = append(rows, e.dynamicCordisSessionValue(run, session))
			}
		}
		return vm.NewArray(rows...)
	})
	_ = service.Set("flush", func(call goja.FunctionCall) goja.Value {
		id := dynamicCordisSessionID(vm, call.Argument(0))
		session, err := e.getSession(id)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return e.dynamicCordisSessionFlush(run, session)
	})
	_ = service.Set("fork", func(call goja.FunctionCall) goja.Value {
		id := dynamicCordisSessionID(vm, call.Argument(0))
		boundary := call.Argument(1)
		childID := ""
		if call.Argument(2) != nil && !goja.IsUndefined(call.Argument(2)) && !goja.IsNull(call.Argument(2)) {
			childID = strings.TrimSpace(call.Argument(2).String())
		}
		if childID != "" {
			if _, err := e.getSession(childID); err == nil {
				panic(vm.ToValue(fmt.Sprintf("session %q already exists", childID)))
			}
		}
		var at *int
		if boundary != nil && !goja.IsUndefined(boundary) && !goja.IsNull(boundary) {
			number := boundary.ToFloat()
			if math.IsNaN(number) || math.IsInf(number, 0) || number < 0 || number > float64(maxJSONSafeInteger) || math.Trunc(number) != number {
				panic(vm.ToValue("fork boundary must be a non-negative safe integer"))
			}
			n := int(number)
			at = &n
		}
		child, err := e.dynamicCordisForkSession(run, id, at, childID)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return e.dynamicCordisSessionValue(run, child)
	})
	return service
}

func (e *Engine) dynamicCordisCreateSession(run *dynamicCordisRun, id string, options goja.Value) goja.Value {
	vm := run.runtime
	prepared, err := e.dynamicCordisPrepareSession(run, id, options)
	if err != nil {
		panic(vm.ToValue("sessions.create: " + err.Error()))
	}
	if err := e.dynamicCordisEnterPrepared(run, prepared); err != nil {
		e.discardDynamicCordisPreparedSession(run, prepared.session.Header.ID)
		panic(vm.ToValue(err.Error()))
	}
	e.dynamicCordisOwnPreparedSession(run, prepared)
	prepared.announced, prepared.announcing = true, true
	prepared.session.mu.Lock()
	prepared.session.published = true
	prepared.session.mu.Unlock()
	if err := e.emitDynamicCordisEventFrom(run, "session/created", dynamicSessionView(prepared.session)); err != nil {
		prepared.announcing = false
		e.dynamicCordisDetachPrepared(run, prepared)
		panic(vm.ToValue(err.Error()))
	}
	prepared.announcing = false
	if prepared.detachRequested {
		e.dynamicCordisDetachPrepared(run, prepared)
	}
	return e.dynamicCordisSessionValue(run, prepared.session)
}

func dynamicGoalViewValue(goal *GoalView) any {
	if goal == nil {
		return nil
	}
	return cloneJSON(goal)
}

func (e *Engine) dynamicCordisGoalTarget(run *dynamicCordisRun, vm *goja.Runtime, value goja.Value) (*Session, string) {
	id := dynamicCordisSessionID(vm, value)
	if id == "" {
		if run.global {
			panic(vm.ToValue("goal operation requires an explicit agent in global scope"))
		}
		id = run.sessionID
	}
	if !run.global && id != run.sessionID {
		panic(vm.ToValue("goal operation is limited to the current session"))
	}
	session, err := e.getSession(id)
	if err != nil {
		panic(vm.ToValue(err.Error()))
	}
	return session, id
}

func (e *Engine) dynamicCordisGoalsFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	ref := func(value goja.Value) (string, int) {
		var input struct {
			ID       string `json:"id"`
			Revision int    `json:"revision"`
		}
		if err := dynamicCordisDecode(value, &input); err != nil || input.ID == "" || input.Revision < 1 {
			panic(vm.ToValue("goal ref requires id and positive revision"))
		}
		return input.ID, input.Revision
	}
	mutation := func(call goja.FunctionCall, op string) goja.Value {
		session, id := e.dynamicCordisGoalTarget(run, vm, call.Argument(0))
		goalID, revision := "", 0
		if op != "create" {
			goalID, revision = ref(call.Argument(1))
		}
		objective, maxRounds := "", 0
		reason := (*GoalBlockReason)(nil)
		request := call.Argument(1)
		if op != "create" {
			request = call.Argument(2)
		}
		if request != nil && !goja.IsUndefined(request) && !goja.IsNull(request) {
			if op == "block" {
				var input GoalBlockReason
				if err := dynamicCordisDecode(request, &input); err != nil {
					panic(vm.ToValue(err.Error()))
				}
				reason = &input
			} else {
				var input struct {
					Objective     string `json:"objective"`
					MaxGoalRounds int    `json:"maxGoalRounds"`
				}
				if err := dynamicCordisDecode(request, &input); err != nil {
					panic(vm.ToValue(err.Error()))
				}
				objective, maxRounds = input.Objective, input.MaxGoalRounds
			}
		}
		_, err := e.goalMutationWithOrigin(run, id, goalID, op, objective, reason, revision, maxRounds)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		if op == "create" || op == "resume" {
			e.startSessionWorker(session)
		}
		if op == "clear" {
			return vm.ToValue(map[string]any{"id": goalID, "revision": revision + 1})
		}
		goal, err := e.GetGoal(id)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(dynamicGoalViewValue(goal))
	}
	_ = service.Set("get", func(call goja.FunctionCall) goja.Value {
		_, id := e.dynamicCordisGoalTarget(run, vm, call.Argument(0))
		goal, err := e.GetGoal(id)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		if goal == nil {
			return goja.Undefined()
		}
		return vm.ToValue(dynamicGoalViewValue(goal))
	})
	_ = service.Set("disarm", func(call goja.FunctionCall) goja.Value {
		session, id := e.dynamicCordisGoalTarget(run, vm, call.Argument(0))
		e.disarmGoal(id)
		_ = session
		goal, err := e.GetGoal(id)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(dynamicGoalViewValue(goal))
	})
	for _, op := range []string{"create", "edit", "pause", "resume", "complete", "block", "clear"} {
		op := op
		_ = service.Set(op, func(call goja.FunctionCall) goja.Value { return mutation(call, op) })
	}
	_ = service.Set("remoteExportCreate", func(call goja.FunctionCall) goja.Value {
		value := mutation(goja.FunctionCall{Arguments: []goja.Value{call.Argument(0), call.Argument(1)}}, "create")
		object := value.ToObject(vm)
		return vm.ToValue(map[string]any{"ref": map[string]any{"id": object.Get("id").String(), "revision": int(object.Get("revision").ToInteger())}})
	})
	return service
}

func dynamicJobSnapshotValue(snapshot jobSnapshot) map[string]any {
	value := map[string]any{"id": snapshot.ID, "kind": snapshot.Kind, "label": snapshot.Label, "status": string(snapshot.Status), "startedAt": snapshot.StartedAt, "reported": snapshot.Reported}
	if snapshot.Owner != "" {
		value["ownerSession"] = snapshot.Owner
	}
	if snapshot.Detail != "" {
		value["detail"] = snapshot.Detail
	}
	if snapshot.FinishedAt != 0 {
		value["finishedAt"] = snapshot.FinishedAt
	}
	if snapshot.ResultLimit > 0 {
		value["outputLimitBytes"] = snapshot.ResultLimit
	}
	return value
}

func (e *Engine) dynamicCordisJobsFacade(run *dynamicCordisRun) *goja.Object {
	vm := run.runtime
	service := vm.NewObject()
	controllerServes := func(owner string) bool {
		return e.jobs.hasController(owner)
	}
	ownerID := func(value goja.Value) string {
		if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
			return ""
		}
		id := dynamicCordisSessionID(vm, value)
		if !run.global && id != run.sessionID {
			panic(vm.ToValue("job operation is limited to the current session"))
		}
		return id
	}
	_ = service.Set("list", func(call goja.FunctionCall) goja.Value {
		owner := ownerID(call.Argument(0))
		rows := e.jobs.list(owner)
		values := make([]any, len(rows))
		for i, row := range rows {
			values[i] = dynamicJobSnapshotValue(row)
		}
		return vm.NewArray(values...)
	})
	_ = service.Set("get", func(call goja.FunctionCall) goja.Value {
		row, err := func() (jobSnapshot, error) {
			e.jobs.mu.Lock()
			defer e.jobs.mu.Unlock()
			job, err := e.jobs.lookupLocked(ownerID(call.Argument(1)), strings.TrimSpace(call.Argument(0).String()))
			if err != nil {
				return jobSnapshot{}, err
			}
			return snapshotJob(job), nil
		}()
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(dynamicJobSnapshotValue(row))
	})
	_ = service.Set("read", func(call goja.FunctionCall) goja.Value {
		text, row, err := e.jobs.read(ownerID(call.Argument(1)), strings.TrimSpace(call.Argument(0).String()))
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(map[string]any{"text": text, "snapshot": dynamicJobSnapshotValue(row)})
	})
	_ = service.Set("kill", func(call goja.FunctionCall) goja.Value {
		reason := ""
		if call.Argument(2) != nil && !goja.IsUndefined(call.Argument(2)) {
			reason = call.Argument(2).String()
		}
		row, finished, err := e.jobs.killInline(ownerID(call.Argument(1)), strings.TrimSpace(call.Argument(0).String()), reason)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return vm.ToValue(map[string]any{"status": map[bool]string{true: "already-finished", false: "requested"}[finished], "job": dynamicJobSnapshotValue(row)})
	})
	_ = service.Set("wait", func(call goja.FunctionCall) goja.Value {
		id := strings.TrimSpace(call.Argument(0).String())
		owner := ownerID(call.Argument(2))
		timeout := call.Argument(1).ToFloat()
		timeoutType := call.Argument(1).ExportType()
		if timeoutType == nil || (timeoutType.Kind() != reflect.Int64 && timeoutType.Kind() != reflect.Float64) || timeout <= 0 || math.IsNaN(timeout) || math.IsInf(timeout, 0) {
			panic(vm.ToValue(fmt.Sprintf("invalid wait timeout: expected a positive number of milliseconds, got %v", timeout)))
		}
		signal := call.Argument(3)
		ctx, cancel := context.WithCancel(context.Background())
		var removeAbort func()
		if signal != nil && !goja.IsUndefined(signal) && !goja.IsNull(signal) {
			object, ok := signal.(*goja.Object)
			if !ok {
				cancel()
				panic(vm.ToValue("jobs.wait signal must be an AbortSignal"))
			}
			if object.Get("aborted").ToBoolean() {
				cancel()
				promise, _, reject := vm.NewPromise()
				_ = reject(vm.NewGoError(errors.New("wait aborted")))
				return vm.ToValue(promise)
			}
			add, addOK := goja.AssertFunction(object.Get("addEventListener"))
			remove, removeOK := goja.AssertFunction(object.Get("removeEventListener"))
			if addOK && removeOK {
				onAbort := vm.ToValue(func(goja.FunctionCall) goja.Value { cancel(); return goja.Undefined() })
				if _, err := add(object, vm.ToValue("abort"), onAbort, vm.ToValue(map[string]any{"once": true})); err != nil {
					cancel()
					panic(vm.ToValue(err.Error()))
				}
				removeAbort = func() { _, _ = remove(object, vm.ToValue("abort"), onAbort) }
			}
		}
		promise, resolve, reject := vm.NewPromise()
		go func() {
			row, err := e.jobs.waitFor(ctx, owner, id, jobWaitDuration(timeout))
			e.dynamicCordis.loop.post(func() {
				cancel()
				if removeAbort != nil {
					removeAbort()
				}
				if err != nil {
					_ = reject(vm.NewGoError(err))
				} else {
					_ = resolve(vm.ToValue(dynamicJobSnapshotValue(row)))
				}
			})
		}()
		return vm.ToValue(promise)
	})
	_ = service.Set("start", func(call goja.FunctionCall) goja.Value {
		object, ok := call.Argument(0).(*goja.Object)
		if !ok {
			panic(vm.ToValue("jobs.start requires a spec object"))
		}
		kind, label := object.Get("kind").String(), object.Get("label").String()
		if kind == "" || label == "" {
			panic(vm.ToValue("jobs.start requires kind and label"))
		}
		owner := ""
		if raw := object.Get("owner"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
			owner = ownerID(raw)
		}
		if !controllerServes(owner) {
			panic(vm.ToValue("no job controller serves this agent"))
		}
		limit := 0
		if raw := object.Get("outputLimitBytes"); raw != nil && !goja.IsUndefined(raw) && !goja.IsNull(raw) {
			value := raw.ToFloat()
			valueType := raw.ExportType()
			if valueType == nil || (valueType.Kind() != reflect.Int64 && valueType.Kind() != reflect.Float64) || value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value || value > 9007199254740991 || value > float64(^uint(0)>>1) {
				panic(vm.ToValue(fmt.Sprintf("invalid outputLimitBytes: expected a positive safe integer, got %v", value)))
			}
			limit = int(value)
		}
		runFn, ok := goja.AssertFunction(object.Get("run"))
		if !ok {
			panic(vm.ToValue("jobs.start requires run()"))
		}
		var startErr error
		id, startErr := e.jobs.startManaged(owner, kind, label, limit, func() (*managedJobHandle, error) {
			hooksValue, err := runFn(goja.Undefined())
			if err != nil {
				return nil, err
			}
			hooks, ok := hooksValue.(*goja.Object)
			if !ok {
				return nil, errors.New("jobs.start run() must return hooks")
			}
			doneValue := hooks.Get("done")
			cancelFn, cancelOK := goja.AssertFunction(hooks.Get("cancel"))
			if !cancelOK {
				cancelFn = func(goja.Value, ...goja.Value) (goja.Value, error) { return goja.Undefined(), nil }
			}
			readFn, _ := goja.AssertFunction(hooks.Get("readOutput"))
			resultCh := make(chan managedJobResult, 1)
			dynamicCordisAwaitOnLoop(run, doneValue, func(value goja.Value, err error) {
				if err != nil {
					resultCh <- managedJobResult{Status: jobFailed, Detail: err.Error()}
					return
				}
				result, ok := value.(*goja.Object)
				if !ok {
					resultCh <- managedJobResult{Status: jobFailed, Detail: "managed job returned an invalid outcome"}
					return
				}
				statusValue := result.Get("status")
				if statusValue == nil || goja.IsUndefined(statusValue) {
					resultCh <- managedJobResult{Status: jobFailed, Detail: "managed job returned an invalid status"}
					return
				}
				statusType := statusValue.ExportType()
				if statusType == nil || statusType.Kind() != reflect.String {
					resultCh <- managedJobResult{Status: jobFailed, Detail: "managed job returned an invalid status"}
					return
				}
				status := jobStatus(statusValue.String())
				if status != jobCompleted && status != jobKilled && status != jobFailed {
					resultCh <- managedJobResult{Status: jobFailed, Detail: "managed job returned an invalid status"}
					return
				}
				field := func(name string) (string, bool) {
					raw := result.Get(name)
					if raw == nil || goja.IsUndefined(raw) {
						return "", true
					}
					rawType := raw.ExportType()
					if rawType == nil || rawType.Kind() != reflect.String {
						return "", false
					}
					return raw.String(), true
				}
				detail, detailOK := field("detail")
				output, outputOK := field("output")
				if !detailOK || !outputOK {
					resultCh <- managedJobResult{Status: jobFailed, Detail: "managed job returned invalid detail or output"}
					return
				}
				resultCh <- managedJobResult{Status: status, Detail: detail, Output: output}
			})
			cancelInline := func(reason string) error {
				_, err := cancelFn(goja.Undefined(), vm.ToValue(reason))
				return err
			}
			return &managedJobHandle{Done: resultCh, Cancel: func(reason string) error {
				var err error
				if !e.dynamicCordis.loop.call(func() { err = cancelInline(reason) }) {
					return errors.New("dynamic Cordis runtime is closed")
				}
				return err
			}, CancelInline: cancelInline, ReadOutput: func() (string, bool, error) {
				if readFn == nil {
					return "", false, nil
				}
				value, err := readFn(goja.Undefined())
				if err != nil {
					return "", false, err
				}
				return value.String(), false, nil
			}}, nil
		})
		if startErr != nil {
			panic(vm.ToValue(startErr.Error()))
		}
		return vm.ToValue(id)
	})
	_ = service.Set("onJobDone", func(call goja.FunctionCall) goja.Value {
		fn, ok := goja.AssertFunction(call.Argument(0))
		if !ok {
			panic(vm.ToValue("jobs.onJobDone requires a function"))
		}
		dispose := e.jobs.onDone(run.ownerSessionID(), func(row jobSnapshot) {
			e.dynamicCordis.loop.post(func() {
				var owner goja.Value = goja.Undefined()
				if row.Owner != "" {
					if snapshot, err := e.dynamicCordisAgentSnapshot(run, row.Owner); err == nil {
						owner = snapshot
					} else {
						owner = vm.ToValue(map[string]any{"id": row.Owner})
					}
				}
				_, _ = fn(goja.Undefined(), vm.ToValue(dynamicJobSnapshotValue(row)), owner)
			})
		})
		run.disposers = append(run.disposers, dispose)
		return vm.ToValue(dispose)
	})
	_ = service.Set("onJobsChanged", func(call goja.FunctionCall) goja.Value {
		fn, ok := goja.AssertFunction(call.Argument(0))
		if !ok {
			panic(vm.ToValue("jobs.onJobsChanged requires a function"))
		}
		dispose := e.jobs.onChanged(run.ownerSessionID(), func(owner string) {
			e.dynamicCordis.loop.post(func() {
				if owner == "" {
					_, _ = fn(goja.Undefined(), goja.Undefined())
					return
				}
				if snapshot, err := e.dynamicCordisAgentSnapshot(run, owner); err == nil {
					_, _ = fn(goja.Undefined(), snapshot)
				} else {
					_, _ = fn(goja.Undefined(), vm.ToValue(map[string]any{"id": owner}))
				}
			})
		})
		run.disposers = append(run.disposers, dispose)
		return vm.ToValue(dispose)
	})
	_ = service.Set("attachController", func(call goja.FunctionCall) goja.Value {
		name := strings.TrimSpace(call.Argument(0).String())
		if name == "" {
			panic(vm.ToValue("jobs.attachController requires a name"))
		}
		dispose, err := e.jobs.attachController(run.ownerSessionID(), run)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		run.disposers = append(run.disposers, dispose)
		return vm.ToValue(dispose)
	})
	return service
}
