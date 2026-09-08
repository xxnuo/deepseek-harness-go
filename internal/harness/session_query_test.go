package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type sessionQueryStoreStub struct {
	SessionStore
	snapshots   []SessionPersistenceSnapshot
	inspections map[string]SessionInspection
	inspectIDs  []string
	list        func(context.Context) ([]SessionPersistenceSnapshot, error)
}

type sessionQueryHandleStub struct {
	testSessionHandleDefaults
	inspection SessionInspection
}

func (h *sessionQueryHandleStub) ID() string { return h.inspection.Meta.ID }

func (h *sessionQueryHandleStub) Header() SessionHeader { return h.inspection.Meta }

func (h *sessionQueryHandleStub) InheritedEventCount() SessionLogOffset {
	return h.inspection.InheritedEventCount
}

func (*sessionQueryHandleStub) Access() SessionAccess { return SessionAccessRead }

func (h *sessionQueryHandleStub) Read(ctx context.Context, _ ...SessionLogOffset) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cloneSessionEvents(h.inspection.Events), nil
}

func (h *sessionQueryHandleStub) Append(context.Context, []Event) error {
	return &SessionReadOnlyError{SessionID: h.ID(), Operation: "append"}
}

func (s *sessionQueryStoreStub) List(ctx context.Context) ([]SessionPersistenceSnapshot, error) {
	if s.list != nil {
		return s.list(ctx)
	}
	return append([]SessionPersistenceSnapshot(nil), s.snapshots...), nil
}

func (s *sessionQueryStoreStub) Open(ctx context.Context, id string, access SessionAccess) (SessionHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if access != SessionAccessRead {
		return nil, errors.New("session query stub only supports read handles")
	}
	s.inspectIDs = append(s.inspectIDs, id)
	inspection, ok := s.inspections[id]
	if !ok {
		return nil, &SessionPersistenceNotFoundError{SessionID: id}
	}
	return &sessionQueryHandleStub{inspection: inspection}, nil
}

func (s *sessionQueryStoreStub) Close() error { return nil }

func TestSessionQueryToolsAreRegisteredWithIndependentSchemas(t *testing.T) {
	e := newIntegrationEngine(t)
	for index, name := range sessionQueryToolNames {
		tool := registeredTool(t, e, name)
		if tool.Schema.Name != name || tool.Execute == nil {
			t.Fatalf("tool %q = %#v", name, tool)
		}
		if tool.Schema.Parameters["type"] != "object" {
			t.Fatalf("tool %q parameters = %#v", name, tool.Schema.Parameters)
		}
		call := ToolCall{Name: name}
		if index < 2 {
			if tool.Timeout != defaultSessionQuerySearchTimeout || toolConcurrencySafe(tool, call) {
				t.Fatalf("search tool %q timeout/concurrency = %s/%v", name, tool.Timeout, toolConcurrencySafe(tool, call))
			}
		} else if tool.Timeout != 0 || !toolConcurrencySafe(tool, call) {
			t.Fatalf("exact tool %q timeout/concurrency = %s/%v", name, tool.Timeout, toolConcurrencySafe(tool, call))
		}
	}
	meta, err := registeredTool(t, e, "session_event_read").PresentationMeta(ToolCall{
		Name: "session_event_read", Arguments: json.RawMessage(`{"session_id":"other","seq":4}`),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := meta.(map[string]any)
	if view["card"] != "generic" || view["kind"] != "read" || view["title"] != "Read event 4" {
		t.Fatalf("event read presentation = %#v", view)
	}
	raw := view["rawInput"].(map[string]any)
	if raw["session_id"] != "other" || raw["seq"] != float64(4) {
		t.Fatalf("event read raw input = %#v", raw)
	}
}

func TestSessionQueryPromptGuidanceIsMountedWithTools(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "query-prompt", "")
	if err != nil {
		t.Fatal(err)
	}
	session := mustSession(t, e, id)
	prompt, err := e.systemPromptForSession(session, session.Model, agentRuntime{toolNames: map[string]bool{"session_search": true}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(prompt, "Search results are cursor-free and workspace-scoped") || !strings.Contains(prompt, "session_event_read") {
		t.Fatalf("session query prompt = %q", prompt)
	}
}

func TestSessionQueryTraceFormatsDeepLineageIteratively(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace := e.Config().Workspace
	target, err := e.CreateSession(t.Context(), workspace, "deep-lineage-target", "")
	if err != nil {
		t.Fatal(err)
	}
	parent := target
	const depth = 3000
	for index := 1; index <= depth; index++ {
		id := fmt.Sprintf("deep-lineage-%d", index)
		if _, err := e.createSession(t.Context(), SessionHeader{ID: id, CWD: workspace, ParentSession: parent}, false); err != nil {
			t.Fatal(err)
		}
		parent = id
	}
	trace, err := e.sessionQueryTraceContext(t.Context(), target, target)
	if err != nil {
		t.Fatal(err)
	}
	output := formatSessionTraceResult(trace)
	if !strings.Contains(output, "Descendants:\n- deep-lineage-1 -") {
		t.Fatalf("first descendant missing from output")
	}
	last := strings.Repeat("  ", depth-1) + fmt.Sprintf("- deep-lineage-%d -", depth)
	if !strings.Contains(output, last) {
		t.Fatalf("deepest descendant missing from output")
	}
}

func TestSessionQueryToolsSearchTraceAndRead(t *testing.T) {
	e := newIntegrationEngine(t)
	parent, err := e.CreateSession(context.Background(), e.Config().Workspace, "query-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSubagent(context.Background(), parent, "query-child", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, parent)
	e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "find the durable needle"}}, "source": map[string]any{"kind": "user"}})
	e.appendEvent(s, "assistant/message", map[string]any{"message": map[string]any{"content": []ContentBlock{{Type: "text", Text: "done"}}}})
	rows, err := e.sessionQuerySearch(parent, "needle", 10)
	if err != nil || len(rows) != 1 || !strings.Contains(rows[0]["snippet"].(string), "needle") {
		t.Fatalf("search = %#v, %v", rows, err)
	}
	trace, err := e.sessionQueryTrace(parent, child)
	if err != nil || len(trace["ancestors"].([]map[string]any)) != 1 {
		t.Fatalf("trace = %#v, %v", trace, err)
	}
	read, err := e.sessionQueryEventRead(parent, parent, 0, 1, 1)
	if err != nil || read["event"] == nil {
		t.Fatalf("read = %#v, %v", read, err)
	}
}

func TestSessionQueryRejectsOtherWorkspace(t *testing.T) {
	e := newIntegrationEngine(t)
	a, _ := e.CreateSession(context.Background(), e.Config().Workspace, "query-a", "")
	b, _ := e.CreateSession(context.Background(), t.TempDir(), "query-b", "")
	if _, err := e.sessionQueryEventSearch(a, b, "x", 1); err == nil || !strings.Contains(err.Error(), "SESSION_QUERY_TOOL_UNAUTHORIZED") {
		t.Fatalf("unexpected auth result: %v", err)
	}
}

func TestSessionQueryEventTraceTracksSurfaceReplacementChain(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "trace-chain", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	first, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "original"}}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.appendEventWithMetadata(s, "assistant/message", map[string]any{"message": map[string]any{"content": []ContentBlock{{Type: "text", Text: "replacement one"}}}}, map[string]any{"op": "replace", "start": first.Seq, "end": first.Seq}, []int{int(first.Seq)}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.appendEventWithMetadata(s, "assistant/message", map[string]any{"message": map[string]any{"content": []ContentBlock{{Type: "text", Text: "replacement two"}}}}, map[string]any{"op": "replace", "start": second.Seq, "end": second.Seq}, []int{int(second.Seq)}, false)
	if err != nil {
		t.Fatal(err)
	}
	trace, err := e.sessionQueryEventTrace(id, id, int(first.Seq))
	if err != nil {
		t.Fatal(err)
	}
	if got := trace["replacementChain"].([]int); len(got) != 2 || got[0] != int(second.Seq) || got[1] != int(second.Seq)+1 {
		t.Fatalf("replacement chain = %#v", got)
	}
	if got := trace["target"].(map[string]any)["surface"]; got != string(sessionSurfaceShadowed) {
		t.Fatalf("target surface = %#v", got)
	}
}

func TestSessionQueryEventSearchExcludesCurrentActiveStep(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "active-step", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	if _, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "before step needle"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "step/start", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "active step needle"}}}); err != nil {
		t.Fatal(err)
	}
	tool := registeredTool(t, e, "session_event_search")
	result, err := tool.Execute(context.Background(), ToolCall{Name: "session_event_search", SessionID: id, Arguments: json.RawMessage(`{"query":"needle"}`)})
	if err != nil {
		t.Fatal(err)
	}
	text := toolResultText(result)
	if !strings.Contains(text, "before step needle") || strings.Contains(text, "active step needle") {
		t.Fatalf("active-step search output = %s", text)
	}
}

func TestSessionQueryEventReadUsesBeforeAndAfterArguments(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "read-window", "")
	if err != nil {
		t.Fatal(err)
	}
	s := mustSession(t, e, id)
	s.mu.Lock()
	s.Title = "Read window title"
	s.mu.Unlock()
	for _, text := range []string{"zero", "one", "two"} {
		if _, err := e.appendEvent(s, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: text}}}); err != nil {
			t.Fatal(err)
		}
	}
	tool := registeredTool(t, e, "session_event_read")
	result, err := tool.Execute(context.Background(), ToolCall{Name: "session_event_read", SessionID: id, Arguments: json.RawMessage(`{"seq":1,"before":1,"after":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	value, ok := result.Value.(string)
	if !ok || value != toolResultText(result) {
		t.Fatalf("read value = %#v", result.Value)
	}
	if !strings.Contains(value, "Before\n- seq 0") || !strings.Contains(value, "After\n- seq 2") {
		t.Fatalf("read window = %q", value)
	}
	if !strings.Contains(value, "Session "+id+" - Read window title") {
		t.Fatalf("read title = %q", value)
	}
	read, err := e.sessionQueryEventRead(id, id, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	target := read["target"].(Event)
	data := target.Data.(map[string]any)
	content := data["content"].([]any)
	content[0].(map[string]any)["text"] = "mutated snapshot"
	s.mu.Lock()
	original := s.Events[1].Data.(map[string]any)["content"].([]ContentBlock)[0].Text
	s.mu.Unlock()
	if original != "one" {
		t.Fatalf("read snapshot mutated session data: %q", original)
	}
}

func TestSessionQueryWorkspaceAuthorizationUsesExactHeaderCWD(t *testing.T) {
	e := newIntegrationEngine(t)
	caller, err := e.CreateSession(t.Context(), e.Config().Workspace, "exact-cwd-caller", "")
	if err != nil {
		t.Fatal(err)
	}
	target, err := e.CreateSession(t.Context(), e.Config().Workspace, "exact-cwd-target", "")
	if err != nil {
		t.Fatal(err)
	}
	targetSession := mustSession(t, e, target)
	targetSession.mu.Lock()
	targetSession.Header.CWD = targetSession.Header.CWD + "/."
	targetSession.mu.Unlock()
	if _, err := e.sessionQueryEventRead(caller, target, 0, 0, 0); err == nil || !strings.Contains(err.Error(), "SESSION_QUERY_TOOL_UNAUTHORIZED") {
		t.Fatalf("equivalent but non-identical cwd authorized: %v", err)
	}
}

func TestSessionQueryParentFilterPreauthorizesAndRedactsLineage(t *testing.T) {
	e := newIntegrationEngine(t)
	caller, err := e.CreateSession(t.Context(), e.Config().Workspace, "parent-filter-caller", "")
	if err != nil {
		t.Fatal(err)
	}
	hiddenParent, err := e.CreateSession(t.Context(), t.TempDir(), "hidden-parent-secret", "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSession(t.Context(), e.Config().Workspace, "visible-child", "")
	if err != nil {
		t.Fatal(err)
	}
	childSession := mustSession(t, e, child)
	childSession.mu.Lock()
	childSession.Header.ParentSession = hiddenParent
	childSession.mu.Unlock()
	if _, err := e.appendEvent(childSession, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "needle"}}}); err != nil {
		t.Fatal(err)
	}
	filtered, err := e.sessionQuerySearchWithOptionsContext(t.Context(), caller, "needle", sessionQuerySearchOptions{
		max: 10, filters: sessionQuerySessionFilters{parents: map[string]bool{hiddenParent: true}}, excludeCaller: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.items) != 0 {
		t.Fatalf("hidden parent guess returned rows: %#v", filtered.items)
	}
	visible, err := e.sessionQuerySearchWithOptionsContext(t.Context(), caller, "needle", sessionQuerySearchOptions{max: 10, excludeCaller: true})
	if err != nil || len(visible.items) != 1 {
		t.Fatalf("unfiltered search = %#v, %v", visible, err)
	}
	output := formatSessionSearchResults(visible.items, visible.capped)
	if !strings.Contains(output, "Parent: [outside workspace]") || strings.Contains(output, hiddenParent) {
		t.Fatalf("lineage redaction output = %q", output)
	}
}

func TestSessionQueryExactTimestampBounds(t *testing.T) {
	from := "2026-07-24T00:00:00.12300001Z"
	to := "2026-07-24T08:00:00.1239999+08:00"
	filters, err := buildSessionQueryFilters(sessionQuerySearchInput{CreatedAtFrom: &from, CreatedAtTo: &to})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 24, 0, 0, 0, 123_000_000, time.UTC).UnixMilli()
	if filters.createdFrom == nil || filters.createdTo == nil || *filters.createdFrom != base+1 || *filters.createdTo != base {
		t.Fatalf("exact integer-domain bounds = %#v/%#v, base=%d", filters.createdFrom, filters.createdTo, base)
	}
	reversedFrom := "2026-07-24T00:00:00.12300002Z"
	reversedTo := "2026-07-24T00:00:00.12300001Z"
	if _, err := buildSessionQueryFilters(sessionQuerySearchInput{CreatedAtFrom: &reversedFrom, CreatedAtTo: &reversedTo}); err == nil || !strings.Contains(err.Error(), "SESSION_QUERY_INVALID_FILTER") {
		t.Fatalf("reversed sub-millisecond range = %v", err)
	}
	omittedSeconds := "2024-02-29T00:00Z"
	if _, err := buildSessionQueryFilters(sessionQuerySearchInput{CreatedAtFrom: &omittedSeconds}); err != nil {
		t.Fatalf("timestamp without seconds rejected: %v", err)
	}
}

func TestSessionQueryInputValidationMatchesToolContract(t *testing.T) {
	if _, err := buildSessionQueryFilters(sessionQuerySearchInput{EventTypes: []string{}}); err == nil || !strings.Contains(err.Error(), "SESSION_QUERY_INVALID_FILTER") {
		t.Fatalf("empty event_types = %v", err)
	}
	if ^uint(0)>>63 != 0 {
		tooLarge64 := int64(maxSessionQuerySafeInteger + 1)
		tooLarge := int(tooLarge64)
		if _, err := buildEventSearchInputFilters(&tooLarge, nil, nil, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "SESSION_QUERY_INVALID_FILTER") {
			t.Fatalf("unsafe sequence = %v", err)
		}
	}
	e := newIntegrationEngine(t)
	caller, err := e.CreateSession(t.Context(), e.Config().Workspace, "query-input-caller", "")
	if err != nil {
		t.Fatal(err)
	}
	read := registeredTool(t, e, "session_event_read")
	_, err = read.Execute(t.Context(), ToolCall{
		Name: "session_event_read", SessionID: caller,
		Arguments: json.RawMessage(`{"session_id":"","seq":0}`),
	})
	if err == nil || !strings.HasPrefix(err.Error(), "SESSION_QUERY_TOOL_UNAUTHORIZED:") {
		t.Fatalf("explicit empty target = %v", err)
	}
	_, err = e.sessionQueryEventReadContext(t.Context(), caller, caller, 0, maxSessionQueryWindow+1, 0)
	safe := sessionQueryOperationError(err)
	if safe == nil || !strings.HasPrefix(safe.Error(), "SESSION_QUERY_INVALID_WINDOW:") {
		t.Fatalf("oversized read window = %v", safe)
	}
}

func TestSessionQueryHiddenAncestorCycleIsSanitized(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace := e.Config().Workspace
	caller, err := e.CreateSession(t.Context(), workspace, "cycle-caller", "")
	if err != nil {
		t.Fatal(err)
	}
	hiddenA := "hidden-cycle-a-secret"
	hiddenB := "hidden-cycle-b-secret"
	outside := t.TempDir()
	if _, err := e.createSession(t.Context(), SessionHeader{ID: hiddenA, CWD: outside, ParentSession: hiddenB}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := e.createSession(t.Context(), SessionHeader{ID: hiddenB, CWD: outside, ParentSession: hiddenA}, false); err != nil {
		t.Fatal(err)
	}
	target := "visible-cycle-target"
	if _, err := e.createSession(t.Context(), SessionHeader{ID: target, CWD: workspace, ParentSession: hiddenA}, false); err != nil {
		t.Fatal(err)
	}
	_, err = e.sessionQueryTraceContext(t.Context(), caller, target)
	safe := sessionQueryOperationError(err)
	if safe == nil || safe.Error() != "SESSION_QUERY_INVALID_LINEAGE: session lineage is invalid" {
		t.Fatalf("sanitized cycle error = %v", safe)
	}
	if strings.Contains(safe.Error(), hiddenA) || strings.Contains(safe.Error(), hiddenB) {
		t.Fatalf("cycle error leaked hidden ids: %v", safe)
	}
}

func TestSessionQueryObservedTargetMustMatchCapturedCallerWorkspace(t *testing.T) {
	callerID := "caller"
	if !sessionQueryObservedTargetAuthorized(callerID, "/work", callerID, SessionHeader{ID: callerID, CWD: "/work"}) {
		t.Fatal("unchanged self observation was rejected")
	}
	for _, header := range []SessionHeader{
		{ID: callerID, CWD: "/outside"},
		{ID: "other", CWD: "/work"},
	} {
		if sessionQueryObservedTargetAuthorized(callerID, "/work", callerID, header) {
			t.Fatalf("moved or substituted observation authorized: %#v", header)
		}
	}
	if sessionQueryObservedTargetAuthorized(callerID, "", "other", SessionHeader{ID: "other"}) {
		t.Fatal("null-cwd caller authorized another session")
	}
}

func TestSessionQueryCorpusUsesLivePrecedenceAndCurrentPersistedSnapshots(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace := e.Config().Workspace
	caller, err := e.CreateSession(t.Context(), workspace, "corpus-caller", "")
	if err != nil {
		t.Fatal(err)
	}
	target, err := e.CreateSession(t.Context(), workspace, "corpus-target", "")
	if err != nil {
		t.Fatal(err)
	}
	targetSession := mustSession(t, e, target)
	targetSession.mu.Lock()
	header := targetSession.Header
	targetSession.Events = []Event{{
		Type: "user/message", Seq: 0, Time: 1,
		Data: map[string]any{"content": []ContentBlock{{Type: "text", Text: "stale live needle"}}}, SurfaceOp: "append",
	}}
	targetSession.attached = false
	targetSession.mu.Unlock()
	persistedEvent := Event{
		Type: "user/message", Seq: 0, Time: 2,
		Data: map[string]any{"content": []ContentBlock{{Type: "text", Text: "fresh persisted needle"}}}, SurfaceOp: "append",
	}
	persistedOnlyHeader := header
	persistedOnlyHeader.ID = "persisted-only"
	store := &sessionQueryStoreStub{
		snapshots: []SessionPersistenceSnapshot{{Header: header}, {Header: persistedOnlyHeader}},
		inspections: map[string]SessionInspection{
			target:                 {Meta: header, Events: []Event{persistedEvent}},
			persistedOnlyHeader.ID: {Meta: persistedOnlyHeader, Events: []Event{persistedEvent}},
		},
	}
	e.sessionStore = store
	collection, err := e.sessionQuerySearchWithOptionsContext(t.Context(), caller, "persisted needle", sessionQuerySearchOptions{max: 10, excludeCaller: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(collection.items) != 2 {
		t.Fatalf("persisted search rows = %#v", collection.items)
	}
	for _, row := range collection.items {
		if got := mapString(row, "availability"); got != "persisted" {
			t.Fatalf("cold availability = %q", got)
		}
	}
	if len(store.inspectIDs) != 2 {
		t.Fatalf("persisted inspections = %#v", store.inspectIDs)
	}

	targetSession.mu.Lock()
	targetSession.attached = true
	targetSession.mu.Unlock()
	store.inspectIDs = nil
	collection, err = e.sessionQuerySearchWithOptionsContext(t.Context(), caller, "stale live needle", sessionQuerySearchOptions{max: 10, excludeCaller: true})
	if err != nil || len(collection.items) != 1 || mapString(collection.items[0], "sessionId") != target {
		t.Fatalf("live-preferred search = %#v, %v", collection, err)
	}
	if got := mapString(collection.items[0], "availability"); got != "live, persisted" {
		t.Fatalf("live availability = %q", got)
	}
	if len(store.inspectIDs) != 1 || store.inspectIDs[0] != persistedOnlyHeader.ID {
		t.Fatalf("live target unexpectedly inspected: %#v", store.inspectIDs)
	}
}

func TestSessionQueryCorpusRejectsSourceHeaderConflicts(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "conflict-target-secret", "")
	if err != nil {
		t.Fatal(err)
	}
	header := mustSession(t, e, id).Header
	durable := header
	durable.CWD = durable.CWD + "/moved"
	e.sessionStore = &sessionQueryStoreStub{snapshots: []SessionPersistenceSnapshot{{Header: durable}}}
	_, err = e.sessionQuerySnapshotsContext(t.Context())
	safe := sessionQueryOperationError(err)
	if safe == nil || safe.Error() != "SESSION_QUERY_TOOL_FAILED: session query operation failed" {
		t.Fatalf("source conflict = %v", safe)
	}
	if strings.Contains(safe.Error(), id) {
		t.Fatalf("source conflict leaked id: %v", safe)
	}
}

func TestSessionQueryParentAuthorizationIncludesPersistedOnlySessions(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace := e.Config().Workspace
	caller, err := e.CreateSession(t.Context(), workspace, "persisted-parent-caller", "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.CreateSession(t.Context(), workspace, "persisted-parent-child", "")
	if err != nil {
		t.Fatal(err)
	}
	parentHeader := SessionHeader{Version: 0, ID: "persisted-parent", CreatedAt: 1, CWD: workspace}
	childSession := mustSession(t, e, child)
	childSession.mu.Lock()
	childSession.Header.ParentSession = parentHeader.ID
	childSession.mu.Unlock()
	if _, err := e.appendEvent(childSession, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "persisted parent needle"}}}); err != nil {
		t.Fatal(err)
	}
	e.sessionStore = &sessionQueryStoreStub{
		snapshots: []SessionPersistenceSnapshot{{Header: parentHeader}},
		inspections: map[string]SessionInspection{
			parentHeader.ID: {Meta: parentHeader, Events: []Event{}},
		},
	}
	collection, err := e.sessionQuerySearchWithOptionsContext(t.Context(), caller, "needle", sessionQuerySearchOptions{
		max: 10, excludeCaller: true,
		filters: sessionQuerySessionFilters{parents: map[string]bool{parentHeader.ID: true}},
	})
	if err != nil || len(collection.items) != 1 {
		t.Fatalf("persisted parent filter = %#v, %v", collection, err)
	}
	output := formatSessionSearchResults(collection.items, collection.capped)
	if !strings.Contains(output, "Parent: "+parentHeader.ID) || strings.Contains(output, "[outside workspace]") {
		t.Fatalf("persisted parent output = %q", output)
	}
}

func TestSessionQueryContextCancellationAndSafeTargetErrors(t *testing.T) {
	e := newIntegrationEngine(t)
	caller, err := e.CreateSession(t.Context(), e.Config().Workspace, "query-cancel-caller", "")
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := e.sessionQuerySearchWithOptionsContext(cancelled, caller, "needle", sessionQuerySearchOptions{max: 10}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled search = %v", err)
	}
	secret := "missing-target-secret"
	_, err = e.sessionQueryEventReadContext(t.Context(), caller, secret, 0, 0, 0)
	safe := sessionQueryOperationError(err)
	if safe == nil || !strings.HasPrefix(safe.Error(), "SESSION_QUERY_TOOL_UNAUTHORIZED:") || strings.Contains(safe.Error(), secret) {
		t.Fatalf("safe target error = %v", safe)
	}
}

func TestSessionQueryPreservesCancellationAfterIgnoringPersistenceCleanup(t *testing.T) {
	e := newIntegrationEngine(t)
	started := make(chan struct{})
	release := make(chan struct{})
	e.sessionStore = &sessionQueryStoreStub{list: func(context.Context) ([]SessionPersistenceSnapshot, error) {
		close(started)
		<-release
		return nil, nil
	}}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := e.sessionQuerySnapshotsContext(ctx)
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		t.Fatalf("query returned before persistence cleanup: %v", err)
	default:
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("post-cleanup cancellation = %v", err)
	}
}

func TestSessionQueryCapAndLogOnlyNeighborRendering(t *testing.T) {
	e := newIntegrationEngine(t)
	caller, err := e.CreateSession(t.Context(), e.Config().Workspace, "query-cap-caller", "")
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		id, createErr := e.CreateSession(t.Context(), e.Config().Workspace, fmt.Sprintf("query-cap-%d", index), "")
		if createErr != nil {
			t.Fatal(createErr)
		}
		if _, appendErr := e.appendEvent(mustSession(t, e, id), "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "needle"}}}); appendErr != nil {
			t.Fatal(appendErr)
		}
	}
	collection, err := e.sessionQuerySearchWithOptionsContext(t.Context(), caller, "needle", sessionQuerySearchOptions{max: 1, excludeCaller: true})
	if err != nil || len(collection.items) != 1 || !collection.capped {
		t.Fatalf("capped collection = %#v, %v", collection, err)
	}
	if output := formatSessionSearchResults(collection.items, collection.capped); !strings.Contains(output, "Result cap reached") {
		t.Fatalf("cap output = %q", output)
	}
	session := mustSession(t, e, caller)
	if _, err := e.appendEvent(session, "step/start", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.appendEvent(session, "step/end", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	read, err := e.sessionQueryEventRead(caller, caller, 0, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if output := formatEventReadResult(read); !strings.Contains(output, "(no semantic text)") {
		t.Fatalf("log-only neighbor output = %q", output)
	}
}

func TestSessionQuerySearchRankingMatchesSQLiteOrder(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "ranking", "")
	if err != nil {
		t.Fatal(err)
	}
	session := mustSession(t, e, id)
	for index, item := range []struct {
		text string
		time int64
	}{
		{text: "needle in a much longer document that should lose the length tie", time: 40},
		{text: "needle short", time: 10},
		{text: "needle needle longest", time: 20},
		{text: "needle needle", time: 30},
	} {
		if _, err := e.appendEvent(session, "user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: item.text}}}); err != nil {
			t.Fatal(err)
		}
		session.mu.Lock()
		session.Events[index].Time = item.time
		session.mu.Unlock()
	}
	collection, err := e.sessionQueryEventSearchWithOptionsContext(t.Context(), id, id, "needle", sessionQueryEventSearchOptions{max: 10})
	if err != nil {
		t.Fatal(err)
	}
	want := []int{3, 2, 1, 0}
	if len(collection.items) != len(want) {
		t.Fatalf("ranked rows = %#v", collection.items)
	}
	for index, seq := range want {
		if got := mapInt(collection.items[index], "seq"); got != seq {
			t.Fatalf("rank %d seq = %d, want %d", index, got, seq)
		}
	}
}

func TestSessionQuerySnippetUsesSQLiteLengthBound(t *testing.T) {
	text := strings.Repeat("a", 200) + " needle " + strings.Repeat("b", 200)
	snippet := querySnippet(text, "needle")
	if got := len([]rune(snippet)); got != 240 {
		t.Fatalf("snippet length = %d", got)
	}
	if !strings.Contains(snippet, "needle") || !strings.HasPrefix(snippet, "…") || !strings.HasSuffix(snippet, "…") {
		t.Fatalf("snippet = %q", snippet)
	}
}

func TestSessionQueryUsesLiteralUnicodeTokenPhrases(t *testing.T) {
	for _, test := range []struct {
		text  string
		query string
		want  int
	}{
		{text: "An AI helper", query: "AI", want: 1},
		{text: "BRAID", query: "AI", want: 0},
		{text: "alpha beta", query: "alpha beta", want: 1},
		{text: "alpha middle beta", query: "alpha beta", want: 0},
		{text: "needle OR absent", query: "needle OR absent", want: 1},
		{text: `say "needle" exactly`, query: `say "needle"`, want: 1},
		{text: "long long long—café, next value", query: "CAFE", want: 1},
		{text: "anything", query: "*", want: 0},
	} {
		count, _ := sessionQueryPhraseMatches(test.text, test.query)
		if count != test.want {
			t.Fatalf("matches(%q, %q) = %d, want %d", test.text, test.query, count, test.want)
		}
	}
}
