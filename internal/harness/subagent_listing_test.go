package harness

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

type subagentListingStoreStub struct {
	SessionStore
	mu          sync.Mutex
	snapshots   []SessionPersistenceSnapshot
	inspections map[string]SessionInspection
	inspectErrs map[string]error
	list        func(context.Context) ([]SessionPersistenceSnapshot, error)
	inspect     func(context.Context, string) (SessionInspection, error)
	inspectIDs  []string
}

func (s *subagentListingStoreStub) ListSnapshots(ctx context.Context) ([]SessionPersistenceSnapshot, error) {
	if s.list != nil {
		return s.list(ctx)
	}
	return append([]SessionPersistenceSnapshot(nil), s.snapshots...), nil
}

func (s *subagentListingStoreStub) Inspect(ctx context.Context, id string) (SessionInspection, error) {
	s.mu.Lock()
	s.inspectIDs = append(s.inspectIDs, id)
	s.mu.Unlock()
	if s.inspect != nil {
		return s.inspect(ctx, id)
	}
	if err := s.inspectErrs[id]; err != nil {
		return SessionInspection{}, err
	}
	inspection, ok := s.inspections[id]
	if !ok {
		return SessionInspection{}, errors.New("session-not-found: " + id)
	}
	return inspection, nil
}

func (s *subagentListingStoreStub) Close() error { return nil }

func listingDescriptor(mode, label string) map[string]any {
	value := map[string]any{"version": SubagentDescriptorVersion, "mode": mode, "provider": "spawn"}
	if mode == "continuable" || label != "" {
		value["label"] = label
	}
	return value
}

func listingEvents(descriptor map[string]any) []Event {
	return []Event{{Type: "subagent/descriptor", Seq: 0, Time: 1, Data: descriptor}}
}

func installListingSession(e *Engine, header SessionHeader, events []Event, attached, running bool) {
	session := &Session{
		Header: header, Events: append([]Event(nil), events...), attached: attached, Running: running,
		firstLiveSeq: len(events), invariants: e.invariants,
	}
	e.mu.Lock()
	e.sessions[header.ID] = session
	e.mu.Unlock()
}

func TestListModelAgentsMergesColdSessionsAndContainsDiagnostics(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "listing-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	headers := []SessionHeader{
		{Version: SessionFormatVersion, ID: "cold-valid", CreatedAt: 1, ParentSession: parent, Origin: "subagent"},
		{Version: SessionFormatVersion, ID: "cold-corrupt", CreatedAt: 2, ParentSession: parent, Origin: "subagent"},
		{Version: SessionFormatVersion, ID: "cold-unavailable", CreatedAt: 3, ParentSession: parent, Origin: "subagent"},
	}
	store := &subagentListingStoreStub{
		inspections: map[string]SessionInspection{
			"cold-valid":   {Meta: headers[0], Events: listingEvents(listingDescriptor("continuable", "cold child"))},
			"cold-corrupt": {Meta: headers[1], Events: listingEvents(map[string]any{"version": SubagentDescriptorVersion, "mode": "continuable", "provider": 7})},
		},
		inspectErrs: map[string]error{"cold-unavailable": errors.New("backend read failed")},
	}
	for _, header := range headers {
		store.snapshots = append(store.snapshots, SessionPersistenceSnapshot{Header: header})
	}
	e.sessionStore = store

	entries, err := e.listModelAgents(t.Context(), parent, false)
	if err != nil {
		t.Fatal(err)
	}
	want := []agentListEntry{
		{Kind: "child", ID: "cold-valid", Label: "cold child", Status: "ready"},
		{Kind: "diagnostic", ID: "cold-corrupt", Reason: "corrupt"},
		{Kind: "diagnostic", ID: "cold-unavailable", Reason: "unavailable"},
	}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("entries = %#v, want %#v", entries, want)
	}
}

func TestListModelAgentsTraversesOrdinaryAndOneShotIntermediates(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "tree-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	installListingSession(e, SessionHeader{Version: SessionFormatVersion, ID: "ordinary-mid", CreatedAt: 1, ParentSession: parent}, nil, true, false)
	installListingSession(e, SessionHeader{Version: SessionFormatVersion, ID: "under-ordinary", CreatedAt: 2, ParentSession: "ordinary-mid", Origin: "subagent"}, listingEvents(listingDescriptor("continuable", "under ordinary")), true, false)
	installListingSession(e, SessionHeader{Version: SessionFormatVersion, ID: "one-shot-mid", CreatedAt: 3, ParentSession: parent, Origin: "subagent"}, listingEvents(listingDescriptor("one-shot", "one shot")), true, false)
	installListingSession(e, SessionHeader{Version: SessionFormatVersion, ID: "under-one-shot", CreatedAt: 4, ParentSession: "one-shot-mid", Origin: "subagent"}, listingEvents(listingDescriptor("continuable", "under one shot")), true, true)

	entries, err := e.listModelAgents(t.Context(), parent, true)
	if err != nil {
		t.Fatal(err)
	}
	want := []agentListEntry{
		{Kind: "child", ID: "under-ordinary", Label: "under ordinary", Status: "idle", Parent: "ordinary-mid", Depth: 2},
		{Kind: "child", ID: "under-one-shot", Label: "under one shot", Status: "running", Parent: "one-shot-mid", Depth: 2},
	}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("entries = %#v, want %#v", entries, want)
	}
}

func TestListModelAgentsColdLifecycleMismatchAndCancellation(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "cancel-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{Version: SessionFormatVersion, ID: "reborn-child", CreatedAt: 1, ParentSession: parent, Origin: "subagent"}
	store := &subagentListingStoreStub{
		snapshots: []SessionPersistenceSnapshot{{Header: header}},
		inspections: map[string]SessionInspection{
			"reborn-child": {Meta: SessionHeader{Version: SessionFormatVersion, ID: "reborn-child", CreatedAt: 1, ParentSession: "someone-else", Origin: "subagent"}, Events: listingEvents(listingDescriptor("continuable", "wrong lifecycle"))},
		},
	}
	e.sessionStore = store
	entries, err := e.listModelAgents(t.Context(), parent, false)
	if err != nil {
		t.Fatal(err)
	}
	if want := []agentListEntry{{Kind: "diagnostic", ID: "reborn-child", Reason: "corrupt"}}; !reflect.DeepEqual(entries, want) {
		t.Fatalf("lifecycle entries = %#v, want %#v", entries, want)
	}

	entered := make(chan struct{})
	store.inspect = func(ctx context.Context, _ string) (SessionInspection, error) {
		close(entered)
		<-ctx.Done()
		return SessionInspection{}, errors.New("backend aborted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := e.listModelAgents(ctx, parent, false)
		done <- err
	}()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}
}

func TestListAgentsSchemaAndDiagnosticRendering(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "schema-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{Version: SessionFormatVersion, ID: "bad-child", CreatedAt: 1, ParentSession: parent, Origin: "subagent"}
	e.sessionStore = &subagentListingStoreStub{
		snapshots:   []SessionPersistenceSnapshot{{Header: header}},
		inspections: map[string]SessionInspection{"bad-child": {Meta: header, Events: listingEvents(map[string]any{"version": 999, "mode": "continuable", "provider": "spawn", "label": "future"})}},
	}
	tool := builtinListAgentsTool(e)
	items, _ := tool.Schema.Output["items"].(map[string]any)
	branches, _ := items["oneOf"].([]any)
	if len(branches) != 2 {
		t.Fatalf("list_agents output schema = %#v", tool.Schema.Output)
	}
	result, err := tool.Execute(t.Context(), ToolCall{Name: "list_agents", SessionID: parent})
	if err != nil {
		t.Fatal(err)
	}
	if text := modelToolResultText(result); text != "bad-child [diagnostic: corrupt]" {
		t.Fatalf("rendered result = %q", text)
	}
	if want := []map[string]any{{"kind": "diagnostic", "id": "bad-child", "reason": "corrupt"}}; !reflect.DeepEqual(result.Value, want) {
		t.Fatalf("diagnostic value = %#v, want %#v", result.Value, want)
	}
}

func TestListModelAgentsUsesOnlyOwnSuffixCachedIdentity(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "cache-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{
		Version: SessionFormatVersion, ID: "cached-child", CreatedAt: 1,
		ParentSession: parent, SeedLength: 3, Origin: "subagent",
	}
	store := &subagentListingStoreStub{
		snapshots: []SessionPersistenceSnapshot{{Header: header}},
		inspections: map[string]SessionInspection{
			"cached-child": {Meta: header, Events: []Event{
				{Type: "subagent/descriptor", Seq: 0, Time: 1, Data: listingDescriptor("continuable", "ancestor")},
				{Type: "subagent/descriptor", Seq: 3, Time: 2, Data: listingDescriptor("continuable", "own label")},
			}},
		},
	}
	e.sessionStore = store
	composition := e.sessionProjections.Signature()
	e.projectionCache = &sessionProjectionCache{medium: &projectionCacheMedium{records: map[string]projectionCacheRecord{
		"cached-child": {
			Identity: projectionIdentity(header), Seq: 0, Version: projectionValuesVersion,
			Composition: composition, Values: map[string]any{"subagent": map[string]any{"mode": "continuable", "label": "ancestor", "seq": 0}},
		},
	}}}
	defer func() { e.projectionCache = nil }()
	entries, err := e.listModelAgents(t.Context(), parent, false)
	if err != nil {
		t.Fatal(err)
	}
	if want := []agentListEntry{{Kind: "child", ID: "cached-child", Label: "own label", Status: "ready"}}; !reflect.DeepEqual(entries, want) {
		t.Fatalf("ancestor-gated entries = %#v, want %#v", entries, want)
	}
	store.mu.Lock()
	inspectCount := len(store.inspectIDs)
	store.mu.Unlock()
	if inspectCount != 1 {
		t.Fatalf("ancestor cache inspect count = %d", inspectCount)
	}

	e.projectionCache.medium.records["cached-child"] = projectionCacheRecord{
		Identity: projectionIdentity(header), Seq: 3, Version: projectionValuesVersion,
		Composition: composition, Values: map[string]any{"subagent": map[string]any{"mode": "continuable", "label": "cached own", "seq": 3}},
	}
	store.inspect = func(context.Context, string) (SessionInspection, error) {
		return SessionInspection{}, errors.New("must not inspect own-suffix cache")
	}
	entries, err = e.listModelAgents(t.Context(), parent, false)
	if err != nil {
		t.Fatal(err)
	}
	if want := []agentListEntry{{Kind: "child", ID: "cached-child", Label: "cached own", Status: "ready"}}; !reflect.DeepEqual(entries, want) {
		t.Fatalf("own-cache entries = %#v, want %#v", entries, want)
	}
}

func TestListModelAgentsContainsForeignProjectionFailurePerChild(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "projection-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.sessionProjections.Register(ProjectionDefinition{
		Key: "hostile-listing", StateVersion: 1,
		Init: func() any { return false },
		Apply: func(state any, event Event) ProjectionResult {
			data, _ := event.Data.(map[string]any)
			if data["label"] == "poison" {
				panic("hostile projection")
			}
			return projectionUnchanged(state)
		},
		View: func(state any) any { return state },
	}); err != nil {
		t.Fatal(err)
	}
	headers := []SessionHeader{
		{Version: SessionFormatVersion, ID: "healthy-child", CreatedAt: 1, ParentSession: parent, Origin: "subagent"},
		{Version: SessionFormatVersion, ID: "poisoned-child", CreatedAt: 2, ParentSession: parent, Origin: "subagent"},
	}
	e.sessionStore = &subagentListingStoreStub{
		snapshots: []SessionPersistenceSnapshot{{Header: headers[0]}, {Header: headers[1]}},
		inspections: map[string]SessionInspection{
			"healthy-child":  {Meta: headers[0], Events: listingEvents(listingDescriptor("continuable", "healthy"))},
			"poisoned-child": {Meta: headers[1], Events: listingEvents(listingDescriptor("continuable", "poison"))},
		},
	}
	entries, err := e.listModelAgents(t.Context(), parent, false)
	if err != nil {
		t.Fatal(err)
	}
	want := []agentListEntry{
		{Kind: "child", ID: "healthy-child", Label: "healthy", Status: "ready"},
		{Kind: "diagnostic", ID: "poisoned-child", Reason: "corrupt"},
	}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("projection-contained entries = %#v, want %#v", entries, want)
	}
}

func TestSubagentListingCorpusUsesOnlyLiveOrPersistedSessions(t *testing.T) {
	e, err := New(WithPersistence(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "listing-live-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{
		Version: SessionFormatVersion, ID: "detached-child", CreatedAt: 1,
		ParentSession: parent, Origin: "subagent",
	}
	installListingSession(e, header, listingEvents(listingDescriptor("continuable", "detached")), false, false)
	entries, available, err := e.listSubagentEntries(t.Context(), parent, false)
	if err != nil {
		t.Fatal(err)
	}
	if !available || len(entries) != 0 {
		t.Fatalf("live-only listing = %#v, parent available = %v", entries, available)
	}

	persistedHeader := header
	persistedHeader.CreatedAt = 2
	store := &subagentListingStoreStub{
		snapshots: []SessionPersistenceSnapshot{{Header: persistedHeader}},
		inspections: map[string]SessionInspection{
			header.ID: {Meta: persistedHeader, Events: listingEvents(listingDescriptor("continuable", "persisted"))},
		},
	}
	e.sessionStore = store
	entries, _, err = e.listSubagentEntries(t.Context(), parent, false)
	if err != nil {
		t.Fatal(err)
	}
	want := []subagentListingEntry{{
		Kind: "child", ID: header.ID, Mode: "continuable", Label: "persisted",
		Activity: "inactive", ParentID: parent, Depth: 1,
	}}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("persisted listing = %#v, want %#v", entries, want)
	}
}

func TestSubagentListingLiveSnapshotOutranksPersistenceWithoutInspection(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "listing-live-wins-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	header := SessionHeader{
		Version: SessionFormatVersion, ID: "live-wins-child", CreatedAt: 1,
		ParentSession: parent, Origin: "subagent",
	}
	installListingSession(e, header, listingEvents(listingDescriptor("continuable", "live")), true, false)
	store := &subagentListingStoreStub{
		snapshots: []SessionPersistenceSnapshot{{Header: header}},
		inspect: func(context.Context, string) (SessionInspection, error) {
			return SessionInspection{}, errors.New("live child must not be inspected")
		},
	}
	e.sessionStore = store
	entries, _, err := e.listSubagentEntries(t.Context(), parent, false)
	if err != nil {
		t.Fatal(err)
	}
	want := []subagentListingEntry{{
		Kind: "child", ID: header.ID, Mode: "continuable", Label: "live",
		Activity: "running", ParentID: parent, Depth: 1, attached: true,
	}}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("live-preferred listing = %#v, want %#v", entries, want)
	}
	store.mu.Lock()
	inspectCount := len(store.inspectIDs)
	store.mu.Unlock()
	if inspectCount != 0 {
		t.Fatalf("live child inspect count = %d", inspectCount)
	}
}

func TestSubagentListRPCPropagatesRequestCancellation(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "listing-rpc-cancel-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	e.sessionStore = &subagentListingStoreStub{list: func(ctx context.Context) ([]SessionPersistenceSnapshot, error) {
		close(entered)
		<-ctx.Done()
		return nil, errors.New("backend listing aborted")
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *RPCError, 1)
	go func() {
		_, rpcErr := e.dispatch(ctx, "subagent.list", []byte(`{"parentSessionId":"`+parent+`"}`))
		done <- rpcErr
	}()
	<-entered
	cancel()
	rpcErr := <-done
	if rpcErr == nil || rpcErr.Code != "cancelled" {
		t.Fatalf("subagent.list cancellation = %#v", rpcErr)
	}
}

func TestSubagentListingBoundsColdReadsAndPreservesCandidateOrder(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "listing-concurrency-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"cold-a", "cold-b", "cold-c", "cold-d", "cold-e", "cold-f"}
	headers := make(map[string]SessionHeader, len(ids))
	snapshots := make([]SessionPersistenceSnapshot, 0, len(ids))
	for index := len(ids) - 1; index >= 0; index-- {
		header := SessionHeader{
			Version: SessionFormatVersion, ID: ids[index], CreatedAt: int64(index),
			ParentSession: parent, Origin: "subagent",
		}
		headers[header.ID] = header
		snapshots = append(snapshots, SessionPersistenceSnapshot{Header: header})
	}
	started := make(chan string, len(ids))
	release := make(chan struct{})
	var activeMu sync.Mutex
	active, maxActive := 0, 0
	store := &subagentListingStoreStub{snapshots: snapshots}
	store.inspect = func(_ context.Context, id string) (SessionInspection, error) {
		activeMu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		activeMu.Unlock()
		started <- id
		<-release
		activeMu.Lock()
		active--
		activeMu.Unlock()
		return SessionInspection{
			Meta: headers[id], Events: listingEvents(listingDescriptor("continuable", id)),
		}, nil
	}
	e.sessionStore = store
	type listingResult struct {
		entries []subagentListingEntry
		err     error
	}
	done := make(chan listingResult, 1)
	go func() {
		entries, _, err := e.listSubagentEntries(t.Context(), parent, false)
		done <- listingResult{entries: entries, err: err}
	}()
	for index := 0; index < subagentColdReadConcurrency; index++ {
		<-started
	}
	select {
	case id := <-started:
		close(release)
		t.Fatalf("more than %d cold reads started before release: %s", subagentColdReadConcurrency, id)
	default:
	}
	close(release)
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	activeMu.Lock()
	observedMax := maxActive
	activeMu.Unlock()
	if observedMax != subagentColdReadConcurrency {
		t.Fatalf("max concurrent cold reads = %d, want %d", observedMax, subagentColdReadConcurrency)
	}
	gotIDs := make([]string, 0, len(result.entries))
	for _, entry := range result.entries {
		gotIDs = append(gotIDs, entry.ID)
	}
	if !reflect.DeepEqual(gotIDs, ids) {
		t.Fatalf("listing order = %#v, want %#v", gotIDs, ids)
	}
}

func TestListAgentsRequiresCallingAgent(t *testing.T) {
	e := newIntegrationEngine(t)
	_, err := builtinListAgentsTool(e).Execute(t.Context(), ToolCall{Name: "list_agents"})
	if err == nil || err.Error() != "list_agents requires a calling agent session" {
		t.Fatalf("list_agents caller error = %v", err)
	}
}
