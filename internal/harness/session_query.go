package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// sessionQueryToolNames is kept in one place so preset discovery and the
// built-in registration path cannot drift apart.
var sessionQueryToolNames = []string{
	"session_search",
	"session_event_search",
	"session_trace",
	"session_event_trace",
	"session_event_read",
}

const (
	defaultSessionQueryMaxResults = 100
	maxSessionQueryResults        = 500
	maxSessionQueryWindow         = 1000
)

type sessionQueryEventSurface string

const (
	sessionSurfaceCurrent  sessionQueryEventSurface = "current"
	sessionSurfaceShadowed sessionQueryEventSurface = "shadowed"
	sessionSurfaceLogOnly  sessionQueryEventSurface = "log-only"
)

type sessionQueryEventRecord struct {
	SessionID string
	Seq       int
	Type      string
	Time      int64
	Surface   sessionQueryEventSurface
}

type sessionQuerySurfaceAnalysis struct {
	records           map[int]sessionQueryEventRecord
	replacedBy        map[int]int
	replacedEventSeqs map[int][]int
	current           map[int]bool
}

type sessionQuerySessionSnapshot struct {
	header    SessionHeader
	title     string
	events    []Event
	archived  bool
	persisted bool
}

type sessionQueryEventFilters struct {
	seqFrom  *int
	seqTo    *int
	timeFrom *int64
	timeTo   *int64
	types    map[string]bool
	surfaces map[sessionQueryEventSurface]bool
}

type sessionQuerySessionFilters struct {
	ids          map[string]bool
	createdFrom  *int64
	createdTo    *int64
	parents      map[string]bool
	includeRoots bool
	availability map[string]bool
	event        sessionQueryEventFilters
}

type sessionQuerySearchOptions struct {
	max           int
	filters       sessionQuerySessionFilters
	excludeCaller bool
}

type sessionQueryEventSearchOptions struct {
	max               int
	filters           sessionQueryEventFilters
	excludeActiveStep bool
}

type sessionQuerySearchInput struct {
	Query            string   `json:"query"`
	SessionIDs       []string `json:"session_ids"`
	CreatedAtFrom    *string  `json:"created_at_from"`
	CreatedAtTo      *string  `json:"created_at_to"`
	ParentSessionIDs []string `json:"parent_session_ids"`
	IncludeRoot      bool     `json:"include_root_sessions"`
	Availability     []string `json:"availability"`
	EventSeqFrom     *int     `json:"event_seq_from"`
	EventSeqTo       *int     `json:"event_seq_to"`
	EventTimeFrom    *string  `json:"event_time_from"`
	EventTimeTo      *string  `json:"event_time_to"`
	EventTypes       []string `json:"event_types"`
	EventSurfaces    []string `json:"event_surfaces"`
}

func sessionQueryText(value any) (ToolResult, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return ToolResult{}, err
	}
	return ToolResult{Content: []ContentBlock{{Type: "text", Text: string(b)}}, Value: string(b)}, nil
}

func sessionQueryFormattedResult(_ any, text string) ToolResult {
	return ToolResult{Content: []ContentBlock{{Type: "text", Text: text}}, Value: text}
}

func sessionCWD(s *Session) string {
	s.mu.Lock()
	cwd := s.Header.CWD
	s.mu.Unlock()
	return sessionQueryWorkspaceKey(cwd)
}

func sessionQueryWorkspaceKey(cwd string) string {
	cwd = strings.TrimSpace(cwd)
	if cwd == "" {
		return ""
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return filepath.Clean(cwd)
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	return filepath.Clean(abs)
}

func (e *Engine) authorizeSessionQuery(callerID, targetID string) (*Session, error) {
	if strings.TrimSpace(callerID) == "" {
		return nil, errors.New("SESSION_QUERY_TOOL_MISSING_AGENT: session query tools require an agent-bound caller")
	}
	caller, err := e.getSession(callerID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(targetID) == "" {
		targetID = callerID
	}
	target, err := e.getSession(targetID)
	if err != nil {
		return nil, err
	}
	if callerID == targetID {
		return target, nil
	}
	callerCWD := sessionCWD(caller)
	if callerCWD == "" || callerCWD != sessionCWD(target) {
		return nil, errors.New("SESSION_QUERY_TOOL_UNAUTHORIZED: SESSION_QUERY_UNAUTHORIZED: target session is outside the caller workspace")
	}
	return target, nil
}

func (e *Engine) sessionQuerySnapshots() []sessionQuerySessionSnapshot {
	persisted := map[string]bool{}
	if e.sessionStore != nil {
		if snapshots, err := e.sessionStore.ListSnapshots(context.Background()); err == nil {
			for _, snapshot := range snapshots {
				persisted[snapshot.Header.ID] = true
			}
		}
	}
	e.mu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	archived := make(map[string]bool, len(e.archived))
	for id, value := range e.archived {
		archived[id] = value
	}
	for _, session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.mu.RUnlock()

	result := make([]sessionQuerySessionSnapshot, 0, len(sessions))
	for _, session := range sessions {
		session.mu.Lock()
		snapshot := sessionQuerySessionSnapshot{
			header:   session.Header,
			title:    session.Title,
			events:   append([]Event(nil), session.Events...),
			archived: archived[session.Header.ID],
		}
		session.mu.Unlock()
		snapshot.persisted = persisted[snapshot.header.ID]
		result = append(result, snapshot)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].header.CreatedAt != result[j].header.CreatedAt {
			return result[i].header.CreatedAt > result[j].header.CreatedAt
		}
		return result[i].header.ID < result[j].header.ID
	})
	return result
}

func normalizeSessionQueryMax(max int) int {
	if max <= 0 {
		return defaultSessionQueryMaxResults
	}
	if max > maxSessionQueryResults {
		return maxSessionQueryResults
	}
	return max
}

func normalizeSessionQueryString(value string) (string, error) {
	value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	if value == "" {
		return "", errors.New("SESSION_QUERY_INVALID_QUERY: query must contain non-whitespace text")
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", errors.New("SESSION_QUERY_INVALID_QUERY: query must not contain NUL")
	}
	return strings.ToLower(value), nil
}

func normalizeSessionQueryText(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func queryTextMatches(text, query string) bool {
	return strings.Contains(normalizeSessionQueryText(text), query)
}

func clipQueryText(text string) string {
	text = strings.TrimSpace(strings.Join(strings.Fields(text), " "))
	runes := []rune(text)
	if len(runes) > 500 {
		return string(runes[:500]) + "..."
	}
	return text
}

func querySnippet(text, query string) string {
	text = strings.TrimSpace(strings.Join(strings.Fields(text), " "))
	if text == "" {
		return ""
	}
	if len([]rune(text)) <= 500 {
		return text
	}
	// Search on the normalized string so a multi-word query remains useful
	// even when the event stores line breaks between semantic fields.
	match := strings.Index(strings.ToLower(text), query)
	if match < 0 {
		return clipQueryText(text)
	}
	start := match - 180
	if start < 0 {
		start = 0
	}
	end := match + len(query) + 260
	if end > len(text) {
		end = len(text)
	}
	for start < end && !utf8.RuneStart(text[start]) {
		start++
	}
	for end > start && !utf8.RuneStart(text[end-1]) {
		end--
	}
	return clipQueryText(text[start:end])
}

func parseSessionQueryTime(name, value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("SESSION_QUERY_INVALID_RANGE: %s must be a timestamp", name)
	}
	timestamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return 0, fmt.Errorf("SESSION_QUERY_INVALID_RANGE: %s must be an ISO 8601 timestamp with timezone", name)
	}
	return timestamp.UnixMilli(), nil
}

func validateQueryRange(name string, from, to *int) error {
	if from != nil && *from < 0 {
		return fmt.Errorf("SESSION_QUERY_INVALID_RANGE: %s_from must be non-negative", name)
	}
	if to != nil && *to < 0 {
		return fmt.Errorf("SESSION_QUERY_INVALID_RANGE: %s_to must be non-negative", name)
	}
	if from != nil && to != nil && *from > *to {
		return fmt.Errorf("SESSION_QUERY_INVALID_RANGE: %s_from must be less than or equal to %s_to", name, name)
	}
	return nil
}

func validateQueryTimeRange(name string, from, to *int64) error {
	if from != nil && to != nil && *from > *to {
		return fmt.Errorf("SESSION_QUERY_INVALID_RANGE: %s_from must be less than or equal to %s_to", name, name)
	}
	return nil
}

func cloneStringSet(values []string, name string) (map[string]bool, error) {
	if values == nil {
		return nil, nil
	}
	set := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("SESSION_QUERY_INVALID_RANGE: %s must not contain empty values", name)
		}
		set[value] = true
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("SESSION_QUERY_INVALID_RANGE: %s must not be empty", name)
	}
	return set, nil
}

func buildSessionQueryEventFilters(seqFrom, seqTo *int, timeFrom, timeTo *string, eventTypes []string, surfaces []string) (sessionQueryEventFilters, error) {
	filters := sessionQueryEventFilters{}
	if err := validateQueryRange("seq", seqFrom, seqTo); err != nil {
		return filters, err
	}
	filters.seqFrom, filters.seqTo = seqFrom, seqTo
	if timeFrom != nil {
		value, err := parseSessionQueryTime("time_from", *timeFrom)
		if err != nil {
			return filters, err
		}
		filters.timeFrom = &value
	}
	if timeTo != nil {
		value, err := parseSessionQueryTime("time_to", *timeTo)
		if err != nil {
			return filters, err
		}
		filters.timeTo = &value
	}
	if err := validateQueryTimeRange("time", filters.timeFrom, filters.timeTo); err != nil {
		return filters, err
	}
	filters.types, _ = cloneStringSet(eventTypes, "event_types")
	if len(eventTypes) > 0 && filters.types == nil {
		return filters, fmt.Errorf("SESSION_QUERY_INVALID_RANGE: event_types must not be empty")
	}
	if surfaces != nil {
		filters.surfaces = make(map[sessionQueryEventSurface]bool, len(surfaces))
		for _, value := range surfaces {
			surface := sessionQueryEventSurface(strings.TrimSpace(value))
			switch surface {
			case sessionSurfaceCurrent, sessionSurfaceShadowed, sessionSurfaceLogOnly:
				filters.surfaces[surface] = true
			default:
				return filters, fmt.Errorf("SESSION_QUERY_INVALID_RANGE: unsupported event surface %q", value)
			}
		}
		if len(filters.surfaces) == 0 {
			return filters, errors.New("SESSION_QUERY_INVALID_RANGE: surfaces must not be empty")
		}
	}
	return filters, nil
}

func (filters sessionQueryEventFilters) matches(record sessionQueryEventRecord, text string) bool {
	if filters.seqFrom != nil && record.Seq < *filters.seqFrom {
		return false
	}
	if filters.seqTo != nil && record.Seq > *filters.seqTo {
		return false
	}
	if filters.timeFrom != nil && record.Time < *filters.timeFrom {
		return false
	}
	if filters.timeTo != nil && record.Time > *filters.timeTo {
		return false
	}
	if len(filters.types) > 0 && !filters.types[record.Type] {
		return false
	}
	if len(filters.surfaces) > 0 && !filters.surfaces[record.Surface] {
		return false
	}
	return text != ""
}

func (e *Engine) sessionQueryAnalyzeEvents(sessionID string, events []Event) (sessionQuerySurfaceAnalysis, error) {
	analysis := sessionQuerySurfaceAnalysis{
		records:           make(map[int]sessionQueryEventRecord, len(events)),
		replacedBy:        map[int]int{},
		replacedEventSeqs: map[int][]int{},
		current:           map[int]bool{},
	}
	for index, event := range events {
		if event.Seq != index {
			return analysis, fmt.Errorf("SESSION_QUERY_INVALID_SURFACE: session %q has non-contiguous event seq %d at index %d", sessionID, event.Seq, index)
		}
		if event.SourceEventSeqs != nil {
			seen := make(map[int]bool, len(event.SourceEventSeqs))
			for _, source := range event.SourceEventSeqs {
				if source < 0 || source >= event.Seq {
					return analysis, fmt.Errorf("SESSION_QUERY_INVALID_SURFACE: event %d cites invalid source seq %d", event.Seq, source)
				}
				if seen[source] {
					return analysis, fmt.Errorf("SESSION_QUERY_INVALID_SURFACE: event %d cites duplicate source seq %d", event.Seq, source)
				}
				seen[source] = true
			}
		}
	}
	if _, err := foldSurfaceEvents(events, true); err != nil {
		return analysis, fmt.Errorf("SESSION_QUERY_INVALID_SURFACE: %w", err)
	}
	// Track the fold again while retaining replacement provenance. The shared
	// foldSurfaceEvents validator above keeps this logic aligned with storage.
	surface := make([]int, 0, len(events))
	for _, event := range events {
		if !isSurfaceEligibleType(event.Type) {
			continue
		}
		if isAppendSurfaceEvent(event) {
			surface = append(surface, event.Seq)
			continue
		}
		start, end, ok := surfaceReplaceBounds(event.SurfaceOp)
		if !ok {
			return analysis, fmt.Errorf("SESSION_QUERY_INVALID_SURFACE: invalid surface operation at seq %d", event.Seq)
		}
		startIndex, endIndex := -1, -1
		for index, seq := range surface {
			if seq == start {
				startIndex = index
			}
			if seq == end {
				endIndex = index
			}
		}
		if startIndex < 0 || endIndex < startIndex {
			return analysis, fmt.Errorf("SESSION_QUERY_INVALID_SURFACE: replacement at seq %d references an invalid range", event.Seq)
		}
		removed := append([]int(nil), surface[startIndex:endIndex+1]...)
		analysis.replacedEventSeqs[event.Seq] = removed
		for _, seq := range removed {
			analysis.replacedBy[seq] = event.Seq
		}
		next := make([]int, 0, len(surface)-len(removed)+1)
		next = append(next, surface[:startIndex]...)
		next = append(next, event.Seq)
		next = append(next, surface[endIndex+1:]...)
		surface = next
	}
	for _, seq := range surface {
		analysis.current[seq] = true
	}
	for _, event := range events {
		surfaceKind := sessionSurfaceLogOnly
		if analysis.current[event.Seq] {
			surfaceKind = sessionSurfaceCurrent
		} else if _, ok := analysis.replacedBy[event.Seq]; ok {
			surfaceKind = sessionSurfaceShadowed
		}
		analysis.records[event.Seq] = sessionQueryEventRecord{
			SessionID: sessionID, Seq: event.Seq, Type: event.Type, Time: event.Time, Surface: surfaceKind,
		}
	}
	return analysis, nil
}

func eventDataMap(value any) map[string]any {
	data, _ := value.(map[string]any)
	return data
}

func semanticContentText(value any) string {
	parts := make([]string, 0)
	var visit func(any)
	visit = func(value any) {
		switch item := value.(type) {
		case ContentBlock:
			switch item.Type {
			case "text":
				parts = append(parts, item.Text)
			case "tool-call":
				parts = append(parts, item.Name, item.Arguments)
			case "tool-result":
				for _, child := range item.Content {
					visit(child)
				}
			}
		case []ContentBlock:
			for _, child := range item {
				visit(child)
			}
		case []any:
			for _, child := range item {
				visit(child)
			}
		case map[string]any:
			if nested, ok := item["message"].(map[string]any); ok {
				visit(nested["content"])
				return
			}
			if content, ok := item["content"]; ok {
				visit(content)
			}
			if typ, _ := item["type"].(string); typ == "text" {
				if text, _ := item["text"].(string); text != "" {
					parts = append(parts, text)
				}
			} else if typ == "tool-call" {
				name, _ := item["name"].(string)
				args, _ := item["arguments"].(string)
				parts = append(parts, name, args)
			}
		}
	}
	visit(value)
	return joinSessionQueryText(parts...)
}

func joinSessionQueryText(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			filtered = append(filtered, part)
		}
	}
	return strings.Join(filtered, "\n")
}

func sessionQueryEventText(event Event) string {
	data := eventDataMap(event.Data)
	if data == nil {
		return ""
	}
	switch event.Type {
	case "user/message":
		return semanticContentText(data["content"])
	case "assistant/message":
		if message, ok := data["message"].(map[string]any); ok {
			return semanticContentText(message["content"])
		}
		return semanticContentText(data["content"])
	case "tool/call":
		name, _ := data["name"].(string)
		args, _ := data["arguments"].(string)
		return joinSessionQueryText(name, args)
	case "tool/result":
		parts := []string{}
		if message, ok := data["message"].(map[string]any); ok {
			parts = append(parts, semanticContentText(message["content"]))
		}
		if errData, ok := data["error"].(map[string]any); ok {
			name, _ := errData["name"].(string)
			code, _ := errData["code"].(string)
			parts = append(parts, name, code)
		}
		return joinSessionQueryText(parts...)
	case "todo/write":
		parts := []string{}
		switch todos := data["todos"].(type) {
		case []any:
			for _, raw := range todos {
				item, _ := raw.(map[string]any)
				status, _ := item["status"].(string)
				content, _ := item["content"].(string)
				parts = append(parts, status, content)
			}
		case []map[string]any:
			for _, item := range todos {
				status, _ := item["status"].(string)
				content, _ := item["content"].(string)
				parts = append(parts, status, content)
			}
		}
		return joinSessionQueryText(parts...)
	case "turn/end":
		reason, _ := data["reason"].(map[string]any)
		kind, _ := reason["kind"].(string)
		switch kind {
		case "error":
			errData, _ := reason["error"].(map[string]any)
			message, _ := errData["message"].(string)
			return joinSessionQueryText("error", message)
		case "aborted", "max-tokens", "interrupted":
			return kind
		}
	}
	return ""
}

func sessionQueryRecordMap(record sessionQueryEventRecord) map[string]any {
	return map[string]any{
		"sessionId": record.SessionID,
		"seq":       record.Seq,
		"type":      record.Type,
		"time":      record.Time,
		"surface":   string(record.Surface),
	}
}

func sessionQueryAvailability(snapshot sessionQuerySessionSnapshot) string {
	values := []string{}
	// A session in the engine is the live source, including a cold session
	// loaded from disk. Persisted is reported independently when a log exists.
	values = append(values, "live")
	if snapshot.persisted {
		values = append(values, "persisted")
	}
	return strings.Join(values, ", ")
}

func sessionQuerySessionMap(snapshot sessionQuerySessionSnapshot) map[string]any {
	parent := any(nil)
	if snapshot.header.ParentSession != "" {
		parent = snapshot.header.ParentSession
	}
	return map[string]any{
		"sessionId":     snapshot.header.ID,
		"id":            snapshot.header.ID,
		"title":         sessionQueryTitle(snapshot.title),
		"createdAt":     snapshot.header.CreatedAt,
		"cwd":           snapshot.header.CWD,
		"parentSession": parent,
		"origin":        snapshot.header.Origin,
		"availability":  sessionQueryAvailability(snapshot),
	}
}

func sessionQueryTitle(title string) string {
	if strings.TrimSpace(title) == "" {
		return "untitled"
	}
	return title
}

func (e *Engine) sessionQuerySearch(callerID, query string, max int) ([]map[string]any, error) {
	return e.sessionQuerySearchWithOptions(callerID, query, sessionQuerySearchOptions{
		max: normalizeSessionQueryMax(max),
	})
}

func (e *Engine) sessionQuerySearchWithOptions(callerID, query string, options sessionQuerySearchOptions) ([]map[string]any, error) {
	query, err := normalizeSessionQueryString(query)
	if err != nil {
		return nil, err
	}
	caller, err := e.getSession(callerID)
	if err != nil {
		if strings.TrimSpace(callerID) == "" {
			return nil, errors.New("SESSION_QUERY_TOOL_MISSING_AGENT: session query tools require an agent-bound caller")
		}
		return nil, err
	}
	callerCWD := sessionCWD(caller)
	if callerCWD == "" {
		return nil, errors.New("SESSION_QUERY_TOOL_UNAUTHORIZED: caller session has no workspace")
	}
	if options.max <= 0 {
		options.max = defaultSessionQueryMaxResults
	}
	rows := make([]map[string]any, 0, options.max)
	for _, snapshot := range e.sessionQuerySnapshots() {
		if snapshot.archived || (options.excludeCaller && snapshot.header.ID == callerID) {
			continue
		}
		if sessionQueryWorkspaceKey(snapshot.header.CWD) != callerCWD || !sessionQuerySessionFiltersMatch(options.filters, snapshot) {
			continue
		}
		analysis, err := e.sessionQueryAnalyzeEvents(snapshot.header.ID, snapshot.events)
		if err != nil {
			return nil, err
		}
		var best *sessionQueryEventRecord
		var bestText string
		bestScore := int(^uint(0) >> 1)
		for _, event := range snapshot.events {
			record := analysis.records[event.Seq]
			if !options.filters.event.matches(record, sessionQueryEventText(event)) {
				continue
			}
			text := sessionQueryEventText(event)
			if !queryTextMatches(text, query) {
				continue
			}
			normalized := normalizeSessionQueryText(text)
			score := strings.Index(normalized, query)
			if score < 0 {
				score = len(normalized)
			}
			if best == nil || score < bestScore || score == bestScore && event.Seq < best.Seq {
				copyRecord := record
				best, bestText, bestScore = &copyRecord, text, score
			}
		}
		if best == nil {
			continue
		}
		bestMatch := sessionQueryRecordMap(*best)
		bestMatch["snippet"] = querySnippet(bestText, query)
		row := sessionQuerySessionMap(snapshot)
		row["bestMatch"] = bestMatch
		row["snippet"] = bestMatch["snippet"]
		rows = append(rows, row)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		left := rows[i]["bestMatch"].(map[string]any)
		right := rows[j]["bestMatch"].(map[string]any)
		if left["time"].(int64) != right["time"].(int64) {
			return left["time"].(int64) > right["time"].(int64)
		}
		return rows[i]["sessionId"].(string) < rows[j]["sessionId"].(string)
	})
	if len(rows) > options.max {
		rows = rows[:options.max]
	}
	return rows, nil
}

func sessionQuerySessionFiltersMatch(filters sessionQuerySessionFilters, snapshot sessionQuerySessionSnapshot) bool {
	id := snapshot.header.ID
	if len(filters.ids) > 0 && !filters.ids[id] {
		return false
	}
	if filters.createdFrom != nil && snapshot.header.CreatedAt < *filters.createdFrom {
		return false
	}
	if filters.createdTo != nil && snapshot.header.CreatedAt > *filters.createdTo {
		return false
	}
	parent := snapshot.header.ParentSession
	if len(filters.parents) > 0 {
		if parent == "" {
			if !filters.includeRoots {
				return false
			}
		} else if !filters.parents[parent] {
			return false
		}
	} else if filters.includeRoots && parent != "" {
		return false
	}
	if len(filters.availability) > 0 {
		live := filters.availability["live"]
		persisted := filters.availability["persisted"]
		if live && !persisted {
			// All in-memory sessions are live.
		} else if persisted && !live && !snapshot.persisted {
			return false
		} else if !live && !persisted {
			return false
		}
	}
	return true
}

func (e *Engine) sessionQueryEventSearch(callerID, targetID, query string, max int) ([]map[string]any, error) {
	return e.sessionQueryEventSearchWithOptions(callerID, targetID, query, sessionQueryEventSearchOptions{
		max: normalizeSessionQueryMax(max),
	})
}

func (e *Engine) sessionQueryEventSearchWithOptions(callerID, targetID, query string, options sessionQueryEventSearchOptions) ([]map[string]any, error) {
	query, err := normalizeSessionQueryString(query)
	if err != nil {
		return nil, err
	}
	session, err := e.authorizeSessionQuery(callerID, targetID)
	if err != nil {
		return nil, err
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	if options.max <= 0 {
		options.max = defaultSessionQueryMaxResults
	}
	analysis, err := e.sessionQueryAnalyzeEvents(targetIDOrCaller(callerID, targetID), events)
	if err != nil {
		return nil, err
	}
	if options.excludeActiveStep && strings.TrimSpace(targetID) == "" || options.excludeActiveStep && targetID == callerID {
		boundary := -1
		for _, event := range events {
			if event.Type == "step/start" {
				boundary = event.Seq
			}
		}
		if boundary < 0 {
			return nil, errors.New("SESSION_QUERY_TOOL_NO_CURRENT_STEP: current-session search requires an active step boundary")
		}
		if options.filters.seqTo == nil || *options.filters.seqTo >= boundary {
			value := boundary - 1
			options.filters.seqTo = &value
		}
	}
	rows := make([]map[string]any, 0, options.max)
	for _, event := range events {
		record := analysis.records[event.Seq]
		text := sessionQueryEventText(event)
		if !options.filters.matches(record, text) || !queryTextMatches(text, query) {
			continue
		}
		row := sessionQueryRecordMap(record)
		row["snippet"] = querySnippet(text, query)
		rows = append(rows, row)
		if len(rows) >= options.max {
			break
		}
	}
	return rows, nil
}

func targetIDOrCaller(callerID, targetID string) string {
	if strings.TrimSpace(targetID) == "" {
		return callerID
	}
	return targetID
}

func (e *Engine) sessionQueryTrace(callerID, targetID string) (map[string]any, error) {
	target, err := e.authorizeSessionQuery(callerID, targetID)
	if err != nil {
		return nil, err
	}
	targetID = targetIDOrCaller(callerID, targetID)
	_ = target
	snapshots := e.sessionQuerySnapshots()
	byID := make(map[string]sessionQuerySessionSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		if !snapshot.archived {
			byID[snapshot.header.ID] = snapshot
		}
	}
	targetSnapshot, ok := byID[targetID]
	if !ok {
		return nil, fmt.Errorf("SESSION_QUERY_SESSION_NOT_FOUND: session %q not found", targetID)
	}
	callerCWD := sessionCWD(target)
	ancestors := make([]map[string]any, 0)
	seen := map[string]bool{targetID: true}
	parentID := targetSnapshot.header.ParentSession
	complete := true
	var unresolved string
	for parentID != "" {
		if seen[parentID] {
			return nil, fmt.Errorf("SESSION_QUERY_INVALID_LINEAGE: session lineage contains a cycle at %q", parentID)
		}
		seen[parentID] = true
		parent, exists := byID[parentID]
		if !exists || sessionQueryWorkspaceKey(parent.header.CWD) != callerCWD {
			complete = false
			unresolved = parentID
			break
		}
		ancestors = append(ancestors, sessionQuerySessionMap(parent))
		parentID = parent.header.ParentSession
	}
	children := make(map[string][]sessionQuerySessionSnapshot)
	for _, snapshot := range snapshots {
		if snapshot.archived || snapshot.header.ParentSession == "" {
			continue
		}
		children[snapshot.header.ParentSession] = append(children[snapshot.header.ParentSession], snapshot)
	}
	for parent := range children {
		sort.SliceStable(children[parent], func(i, j int) bool {
			if children[parent][i].header.CreatedAt != children[parent][j].header.CreatedAt {
				return children[parent][i].header.CreatedAt < children[parent][j].header.CreatedAt
			}
			return children[parent][i].header.ID < children[parent][j].header.ID
		})
	}
	var buildDescendants func(string, map[string]bool) []map[string]any
	buildDescendants = func(id string, lineage map[string]bool) []map[string]any {
		result := []map[string]any{}
		for _, child := range children[id] {
			if sessionQueryWorkspaceKey(child.header.CWD) != callerCWD {
				result = append(result, map[string]any{"outsideWorkspace": true})
				continue
			}
			if lineage[child.header.ID] {
				result = append(result, map[string]any{"session": sessionQuerySessionMap(child), "invalidLineage": true})
				continue
			}
			nextLineage := make(map[string]bool, len(lineage)+1)
			for key, value := range lineage {
				nextLineage[key] = value
			}
			nextLineage[child.header.ID] = true
			result = append(result, map[string]any{
				"session":     sessionQuerySessionMap(child),
				"descendants": buildDescendants(child.header.ID, nextLineage),
			})
		}
		return result
	}
	result := map[string]any{
		"sessionId":   targetID,
		"target":      sessionQuerySessionMap(targetSnapshot),
		"session":     sessionQuerySessionMap(targetSnapshot),
		"ancestors":   ancestors,
		"descendants": buildDescendants(targetID, map[string]bool{targetID: true}),
		"complete":    complete,
	}
	if complete {
		root := targetSnapshot
		if len(ancestors) > 0 {
			rootID, _ := ancestors[len(ancestors)-1]["sessionId"].(string)
			root = byID[rootID]
		}
		result["root"] = sessionQuerySessionMap(root)
	} else {
		result["unresolvedParentId"] = unresolved
		result["outsideWorkspaceBoundary"] = true
	}
	return result, nil
}

func (e *Engine) sessionQueryEventTrace(callerID, targetID string, seq int) (map[string]any, error) {
	if seq < 0 {
		return nil, errors.New("SESSION_QUERY_INVALID_RANGE: seq must be non-negative")
	}
	session, err := e.authorizeSessionQuery(callerID, targetID)
	if err != nil {
		return nil, err
	}
	targetID = targetIDOrCaller(callerID, targetID)
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	if seq >= len(events) || events[seq].Seq != seq {
		return nil, fmt.Errorf("SESSION_QUERY_EVENT_NOT_FOUND: session %q has no event at seq %d", targetID, seq)
	}
	analysis, err := e.sessionQueryAnalyzeEvents(targetID, events)
	if err != nil {
		return nil, err
	}
	target := events[seq]
	replacementChain := []int{}
	for replacement, ok := analysis.replacedBy[seq]; ok; replacement, ok = analysis.replacedBy[replacement] {
		replacementChain = append(replacementChain, replacement)
		if len(replacementChain) > len(events) {
			return nil, errors.New("SESSION_QUERY_INVALID_LINEAGE: replacement chain contains a cycle")
		}
	}
	derived := []int{}
	for _, event := range events {
		if event.Seq <= seq {
			continue
		}
		for _, source := range event.SourceEventSeqs {
			if source == seq {
				derived = append(derived, event.Seq)
				break
			}
		}
	}
	sources := append([]int(nil), target.SourceEventSeqs...)
	replaced := append([]int(nil), analysis.replacedEventSeqs[seq]...)
	targetRecord := sessionQueryRecordMap(analysis.records[seq])
	result := map[string]any{
		"sessionId":         targetID,
		"target":            targetRecord,
		"event":             target,
		"replacedBy":        nil,
		"replacementChain":  replacementChain,
		"replacedEventSeqs": replaced,
		"sourceEventSeqs":   sources,
		"derivedEventSeqs":  derived,
		"sources":           sources,
		"derived":           derived,
	}
	if len(replacementChain) > 0 {
		result["replacedBy"] = replacementChain[0]
	}
	return result, nil
}

func (e *Engine) sessionQueryEventRead(callerID, targetID string, seq, before, after int) (map[string]any, error) {
	if seq < 0 || before < 0 || after < 0 {
		return nil, errors.New("SESSION_QUERY_INVALID_RANGE: seq, before, and after must be non-negative")
	}
	if before > maxSessionQueryWindow || after > maxSessionQueryWindow {
		return nil, fmt.Errorf("SESSION_QUERY_INVALID_RANGE: before and after must be no greater than %d", maxSessionQueryWindow)
	}
	session, err := e.authorizeSessionQuery(callerID, targetID)
	if err != nil {
		return nil, err
	}
	targetID = targetIDOrCaller(callerID, targetID)
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	if seq >= len(events) || events[seq].Seq != seq {
		return nil, fmt.Errorf("SESSION_QUERY_EVENT_NOT_FOUND: session %q has no event at seq %d", targetID, seq)
	}
	start := seq - before
	if start < 0 {
		start = 0
	}
	end := seq + after
	if end >= len(events) {
		end = len(events) - 1
	}
	body := append([]Event(nil), events[start:end+1]...)
	neighbors := make([]map[string]any, 0, len(body)-1)
	for _, event := range body {
		if event.Seq == seq {
			continue
		}
		neighbor := map[string]any{"seq": event.Seq, "type": event.Type, "time": event.Time}
		if text := sessionQueryEventText(event); text != "" {
			neighbor["snippet"] = clipQueryText(text)
		}
		neighbors = append(neighbors, neighbor)
	}
	return map[string]any{
		"sessionId": targetID,
		"target":    events[seq],
		"event":     events[seq],
		"events":    body,
		"neighbors": neighbors,
		"startSeq":  body[0].Seq,
		"endSeq":    body[len(body)-1].Seq,
	}, nil
}

func mapString(value map[string]any, key string) string {
	result, _ := value[key].(string)
	return result
}

func mapInt(value map[string]any, key string) int {
	result, _ := value[key].(int)
	return result
}

func formatSessionQueryTime(value any) string {
	var milliseconds int64
	switch number := value.(type) {
	case int:
		milliseconds = int64(number)
	case int64:
		milliseconds = number
	case float64:
		milliseconds = int64(number)
	default:
		return "unknown"
	}
	return time.UnixMilli(milliseconds).UTC().Format(time.RFC3339Nano)
}

func formatSessionSearchResults(rows []map[string]any) string {
	if len(rows) == 0 {
		return "No prior session matches found."
	}
	lines := []string{fmt.Sprintf("Session search results (%d):", len(rows))}
	for index, row := range rows {
		best, _ := row["bestMatch"].(map[string]any)
		parent := mapString(row, "parentSession")
		if parent == "" {
			parent = "root"
		}
		lines = append(lines, "", fmt.Sprintf("%d. Session %s - %s", index+1, mapString(row, "sessionId"), sessionQueryTitle(mapString(row, "title"))))
		lines = append(lines,
			fmt.Sprintf("   Created: %s", formatSessionQueryTime(row["createdAt"])),
			fmt.Sprintf("   Parent: %s", parent),
			fmt.Sprintf("   Availability: %s", mapString(row, "availability")),
			fmt.Sprintf("   Best match: seq %d | %s | %s | %s", mapInt(best, "seq"), mapString(best, "type"), mapString(best, "surface"), formatSessionQueryTime(best["time"])),
			fmt.Sprintf("   Snippet: %s", mapString(best, "snippet")),
		)
	}
	return strings.Join(lines, "\n")
}

func formatEventSearchResults(rows []map[string]any, sessionID, title string) string {
	lines := []string{fmt.Sprintf("Session %s - %s", sessionID, sessionQueryTitle(title))}
	if len(rows) == 0 {
		return strings.Join(append(lines, "", "No prior event matches found."), "\n")
	}
	lines = append(lines, "", fmt.Sprintf("Event search results (%d):", len(rows)))
	for index, row := range rows {
		lines = append(lines,
			fmt.Sprintf("%d. seq %d | %s | %s | %s", index+1, mapInt(row, "seq"), mapString(row, "type"), mapString(row, "surface"), formatSessionQueryTime(row["time"])),
			fmt.Sprintf("   Snippet: %s", mapString(row, "snippet")),
		)
	}
	return strings.Join(lines, "\n")
}

func formatSessionTraceResult(trace map[string]any) string {
	target, _ := trace["target"].(map[string]any)
	lines := []string{
		fmt.Sprintf("Session %s - %s", mapString(trace, "sessionId"), sessionQueryTitle(mapString(target, "title"))),
		fmt.Sprintf("Created: %s", formatSessionQueryTime(target["createdAt"])),
		fmt.Sprintf("Availability: %s", mapString(target, "availability")),
		"",
		"Ancestors (nearest first):",
	}
	ancestors, _ := trace["ancestors"].([]map[string]any)
	if len(ancestors) == 0 {
		if !trace["complete"].(bool) {
			lines = append(lines, "- [outside workspace boundary]")
		} else {
			lines = append(lines, "- none (target is a root session)")
		}
	}
	for _, ancestor := range ancestors {
		lines = append(lines, fmt.Sprintf("- %s - %s | %s | %s", mapString(ancestor, "sessionId"), sessionQueryTitle(mapString(ancestor, "title")), formatSessionQueryTime(ancestor["createdAt"]), mapString(ancestor, "availability")))
	}
	if len(ancestors) > 0 && !trace["complete"].(bool) {
		lines = append(lines, "- [outside workspace boundary]")
	}
	lines = append(lines, "", "Descendants:")
	descendants, _ := trace["descendants"].([]map[string]any)
	if len(descendants) == 0 {
		lines = append(lines, "- none")
	} else {
		formatDescendantLines(&lines, descendants, 0)
	}
	return strings.Join(lines, "\n")
}

func formatDescendantLines(lines *[]string, nodes []map[string]any, depth int) {
	indent := strings.Repeat("  ", depth)
	for _, node := range nodes {
		if outside, _ := node["outsideWorkspace"].(bool); outside {
			*lines = append(*lines, indent+"- [outside workspace subtree]")
			continue
		}
		session, _ := node["session"].(map[string]any)
		*lines = append(*lines, fmt.Sprintf("%s- %s - %s | %s | %s", indent, mapString(session, "sessionId"), sessionQueryTitle(mapString(session, "title")), formatSessionQueryTime(session["createdAt"]), mapString(session, "availability")))
		children, _ := node["descendants"].([]map[string]any)
		if len(children) > 0 {
			formatDescendantLines(lines, children, depth+1)
		}
	}
}

func formatEventTraceResult(trace map[string]any) string {
	target, _ := trace["target"].(map[string]any)
	seqList := func(value any) string {
		values, _ := value.([]int)
		if len(values) == 0 {
			return "none"
		}
		parts := make([]string, len(values))
		for index, value := range values {
			parts[index] = fmt.Sprintf("%d", value)
		}
		return strings.Join(parts, ", ")
	}
	replacedBy := "none"
	if value, ok := trace["replacedBy"].(int); ok {
		replacedBy = fmt.Sprintf("%d", value)
	}
	return strings.Join([]string{
		fmt.Sprintf("Session %s - event trace", mapString(trace, "sessionId")),
		fmt.Sprintf("Target: seq %d | %s | %s | %s", mapInt(target, "seq"), mapString(target, "type"), mapString(target, "surface"), formatSessionQueryTime(target["time"])),
		"Replaced by: " + replacedBy,
		"Replacement chain: " + seqList(trace["replacementChain"]),
		"Events replaced by target: " + seqList(trace["replacedEventSeqs"]),
		"Events cited directly as sources: " + seqList(trace["sourceEventSeqs"]),
		"Direct derived events: " + seqList(trace["derivedEventSeqs"]),
	}, "\n")
}

func formatEventReadResult(value map[string]any) string {
	target, _ := value["target"].(Event)
	encoded, _ := json.MarshalIndent(target, "", "  ")
	lines := []string{
		fmt.Sprintf("Session %s - event read", mapString(value, "sessionId")),
		fmt.Sprintf("Target event seq %d:", target.Seq),
		"```json",
		string(encoded),
		"```",
	}
	neighbors, _ := value["neighbors"].([]map[string]any)
	for _, section := range []struct {
		name string
		from func(int) bool
	}{
		{"Before", func(seq int) bool { return seq < target.Seq }},
		{"After", func(seq int) bool { return seq > target.Seq }},
	} {
		sectionLines := []string{}
		for _, neighbor := range neighbors {
			seq := mapInt(neighbor, "seq")
			if !section.from(seq) {
				continue
			}
			line := fmt.Sprintf("- seq %d | %s | %s", seq, mapString(neighbor, "type"), formatSessionQueryTime(neighbor["time"]))
			if snippet := mapString(neighbor, "snippet"); snippet != "" {
				line += "\n  " + strings.ReplaceAll(snippet, "\n", "\n  ")
			}
			sectionLines = append(sectionLines, line)
		}
		if len(sectionLines) > 0 {
			lines = append(lines, "", section.name)
			lines = append(lines, sectionLines...)
		}
	}
	return strings.Join(lines, "\n")
}

func registerSessionQueryTools(e *Engine) error {
	type eventSearchInput struct {
		SessionID  string   `json:"session_id"`
		Query      string   `json:"query"`
		SeqFrom    *int     `json:"seq_from"`
		SeqTo      *int     `json:"seq_to"`
		TimeFrom   *string  `json:"time_from"`
		TimeTo     *string  `json:"time_to"`
		EventTypes []string `json:"event_types"`
		Surfaces   []string `json:"surfaces"`
	}
	type targetInput struct {
		SessionID string `json:"session_id"`
	}
	type eventTargetInput struct {
		SessionID string `json:"session_id"`
		Seq       int    `json:"seq"`
	}
	type eventReadInput struct {
		SessionID string `json:"session_id"`
		Seq       int    `json:"seq"`
		Before    *int   `json:"before"`
		After     *int   `json:"after"`
	}

	tools := []Tool{
		{
			Schema: ToolSchema{Name: "session_search", Description: "Search prior sessions in the caller workspace and return the strongest matching event from each session.", Parameters: objectSchema(map[string]any{
				"query": map[string]any{"type": "string"}, "session_ids": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"created_at_from": map[string]any{"type": "string"}, "created_at_to": map[string]any{"type": "string"},
				"parent_session_ids": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, "include_root_sessions": map[string]any{"type": "boolean"},
				"availability":   map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": []string{"live", "persisted"}}},
				"event_seq_from": map[string]any{"type": "integer"}, "event_seq_to": map[string]any{"type": "integer"},
				"event_time_from": map[string]any{"type": "string"}, "event_time_to": map[string]any{"type": "string"},
				"event_types": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, "event_surfaces": map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": []string{"current", "shadowed", "log-only"}}},
			}, "query"), Output: map[string]any{"type": "string"}},
			Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
				if err := ctx.Err(); err != nil {
					return ToolResult{}, err
				}
				var in sessionQuerySearchInput
				if err := decodeToolArguments(call, &in); err != nil {
					return ToolResult{}, err
				}
				filters, err := buildSessionQueryFilters(in)
				if err != nil {
					return ToolResult{}, err
				}
				rows, err := e.sessionQuerySearchWithOptions(call.SessionID, in.Query, sessionQuerySearchOptions{max: defaultSessionQueryMaxResults, filters: filters, excludeCaller: true})
				if err != nil {
					return ToolResult{}, err
				}
				return sessionQueryFormattedResult(rows, formatSessionSearchResults(rows)), nil
			},
		},
		{
			Schema: ToolSchema{Name: "session_event_search", Description: "Search prior events in one authorized session; the current session excludes the active step performing this call.", Parameters: objectSchema(map[string]any{
				"session_id": map[string]any{"type": "string"}, "query": map[string]any{"type": "string"}, "seq_from": map[string]any{"type": "integer"}, "seq_to": map[string]any{"type": "integer"},
				"time_from": map[string]any{"type": "string"}, "time_to": map[string]any{"type": "string"}, "event_types": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, "surfaces": map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": []string{"current", "shadowed", "log-only"}}},
			}, "query"), Output: map[string]any{"type": "string"}},
			Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
				if err := ctx.Err(); err != nil {
					return ToolResult{}, err
				}
				var in eventSearchInput
				if err := decodeToolArguments(call, &in); err != nil {
					return ToolResult{}, err
				}
				filters, err := buildEventSearchInputFilters(in.SeqFrom, in.SeqTo, in.TimeFrom, in.TimeTo, in.EventTypes, in.Surfaces)
				if err != nil {
					return ToolResult{}, err
				}
				rows, err := e.sessionQueryEventSearchWithOptions(call.SessionID, in.SessionID, in.Query, sessionQueryEventSearchOptions{max: defaultSessionQueryMaxResults, filters: filters, excludeActiveStep: true})
				if err != nil {
					return ToolResult{}, err
				}
				target, _ := e.authorizeSessionQuery(call.SessionID, in.SessionID)
				title := ""
				if target != nil {
					target.mu.Lock()
					title = target.Title
					target.mu.Unlock()
				}
				return sessionQueryFormattedResult(rows, formatEventSearchResults(rows, targetIDOrCaller(call.SessionID, in.SessionID), title)), nil
			},
		},
		{
			Schema: ToolSchema{Name: "session_trace", Description: "Read the authorized session lineage, including complete visible ancestors and descendants.", Parameters: objectSchema(map[string]any{"session_id": map[string]any{"type": "string"}}), Output: map[string]any{"type": "string"}},
			Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
				if err := ctx.Err(); err != nil {
					return ToolResult{}, err
				}
				var in targetInput
				if err := decodeToolArguments(call, &in); err != nil {
					return ToolResult{}, err
				}
				value, err := e.sessionQueryTrace(call.SessionID, in.SessionID)
				if err != nil {
					return ToolResult{}, err
				}
				return sessionQueryFormattedResult(value, formatSessionTraceResult(value)), nil
			},
		},
		{
			Schema: ToolSchema{Name: "session_event_trace", Description: "Read direct replacement, source, and derived-event relationships for one authorized event.", Parameters: objectSchema(map[string]any{"session_id": map[string]any{"type": "string"}, "seq": map[string]any{"type": "integer"}}, "seq"), Output: map[string]any{"type": "string"}},
			Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
				if err := ctx.Err(); err != nil {
					return ToolResult{}, err
				}
				var in eventTargetInput
				if err := decodeToolArguments(call, &in); err != nil {
					return ToolResult{}, err
				}
				value, err := e.sessionQueryEventTrace(call.SessionID, in.SessionID, in.Seq)
				if err != nil {
					return ToolResult{}, err
				}
				return sessionQueryFormattedResult(value, formatEventTraceResult(value)), nil
			},
		},
		{
			Schema: ToolSchema{Name: "session_event_read", Description: "Read one full event and optional neighboring raw-event summaries from an authorized session.", Parameters: objectSchema(map[string]any{"session_id": map[string]any{"type": "string"}, "seq": map[string]any{"type": "integer"}, "before": map[string]any{"type": "integer"}, "after": map[string]any{"type": "integer"}}, "seq"), Output: map[string]any{"type": "string"}},
			Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
				if err := ctx.Err(); err != nil {
					return ToolResult{}, err
				}
				var in eventReadInput
				if err := decodeToolArguments(call, &in); err != nil {
					return ToolResult{}, err
				}
				before, after := 0, 0
				if in.Before != nil {
					before = *in.Before
				}
				if in.After != nil {
					after = *in.After
				}
				value, err := e.sessionQueryEventRead(call.SessionID, in.SessionID, in.Seq, before, after)
				if err != nil {
					return ToolResult{}, err
				}
				return sessionQueryFormattedResult(value, formatEventReadResult(value)), nil
			},
		},
	}
	for _, tool := range tools {
		if err := e.RegisterTool(tool); err != nil {
			return err
		}
	}
	return nil
}

func buildEventSearchInputFilters(seqFrom, seqTo *int, timeFrom, timeTo *string, eventTypes, surfaces []string) (sessionQueryEventFilters, error) {
	return buildSessionQueryEventFilters(seqFrom, seqTo, timeFrom, timeTo, eventTypes, surfaces)
}

func buildSessionQueryFilters(in sessionQuerySearchInput) (sessionQuerySessionFilters, error) {
	filters := sessionQuerySessionFilters{}
	var err error
	filters.ids, err = cloneStringSet(in.SessionIDs, "session_ids")
	if err != nil {
		return filters, err
	}
	filters.parents, err = cloneStringSet(in.ParentSessionIDs, "parent_session_ids")
	if err != nil {
		return filters, err
	}
	filters.includeRoots = in.IncludeRoot
	filters.availability, err = cloneStringSet(in.Availability, "availability")
	if err != nil {
		return filters, err
	}
	for value := range filters.availability {
		if value != "live" && value != "persisted" {
			return filters, fmt.Errorf("SESSION_QUERY_INVALID_RANGE: unsupported availability %q", value)
		}
	}
	if in.CreatedAtFrom != nil {
		value, parseErr := parseSessionQueryTime("created_at_from", *in.CreatedAtFrom)
		if parseErr != nil {
			return filters, parseErr
		}
		filters.createdFrom = &value
	}
	if in.CreatedAtTo != nil {
		value, parseErr := parseSessionQueryTime("created_at_to", *in.CreatedAtTo)
		if parseErr != nil {
			return filters, parseErr
		}
		filters.createdTo = &value
	}
	if err := validateQueryTimeRange("created_at", filters.createdFrom, filters.createdTo); err != nil {
		return filters, err
	}
	filters.event, err = buildSessionQueryEventFilters(in.EventSeqFrom, in.EventSeqTo, in.EventTimeFrom, in.EventTimeTo, in.EventTypes, in.EventSurfaces)
	return filters, err
}
