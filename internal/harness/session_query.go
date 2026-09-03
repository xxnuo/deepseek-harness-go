package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
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
	defaultSessionQueryMaxResults    = 100
	defaultSessionQuerySearchTimeout = 30 * time.Second
	maxSessionQuerySafeInteger       = 9_007_199_254_740_991
	maxSessionQueryResults           = 500
	maxSessionQueryWindow            = 50
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
	live      bool
	persisted bool
}

type sessionQueryCollection struct {
	items     []map[string]any
	capped    bool
	sessionID string
	title     string
}

type sessionQueryMatchRank struct {
	matchCount     int
	documentLength int
	time           int64
	seq            int
}

type sessionQueryToken struct {
	value string
	start int
}

type sessionQueryRankedRow struct {
	row  map[string]any
	rank sessionQueryMatchRank
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

func cloneSessionQueryEvent(event Event) Event {
	cloned := event
	cloned.Data = cloneJSON(event.Data)
	cloned.SurfaceOp = cloneJSON(event.SurfaceOp)
	cloned.SourceEventSeqs = append([]int(nil), event.SourceEventSeqs...)
	return cloned
}

func cloneSessionQueryEvents(events []Event) []Event {
	cloned := make([]Event, len(events))
	for index, event := range events {
		cloned[index] = cloneSessionQueryEvent(event)
	}
	return cloned
}

func sessionQueryOperationError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	code, _, ok := strings.Cut(err.Error(), ":")
	if !ok {
		return errors.New("SESSION_QUERY_TOOL_FAILED: session query operation failed")
	}
	messages := map[string]string{
		"SESSION_QUERY_TOOL_MISSING_AGENT":   "session query tools require an agent-bound caller",
		"SESSION_QUERY_TOOL_UNAUTHORIZED":    "session target is outside the caller workspace",
		"SESSION_QUERY_TOOL_NO_CURRENT_STEP": "current-session search requires an active step boundary",
		"SESSION_QUERY_ABORTED":              "session query was cancelled",
		"SESSION_QUERY_CORRUPT_SESSION":      "session event history is corrupt",
		"SESSION_QUERY_INDEX_FAILED":         "session search index is unavailable",
		"SESSION_QUERY_INVALID_CURSOR":       "session search continuation is invalid",
		"SESSION_QUERY_INVALID_QUERY":        "session query was rejected",
		"SESSION_QUERY_INVALID_FILTER":       "session query filters were rejected",
		"SESSION_QUERY_INVALID_RANGE":        "session query filters were rejected",
		"SESSION_QUERY_INVALID_LIMIT":        "session query result limit was rejected",
		"SESSION_QUERY_INVALID_LINEAGE":      "session lineage is invalid",
		"SESSION_QUERY_INVALID_SURFACE":      "session event history is invalid",
		"SESSION_QUERY_INVALID_WINDOW":       "session event window is invalid",
		"SESSION_QUERY_EVENT_NOT_FOUND":      "session event was not found",
		"SESSION_QUERY_PERSISTENCE_FAILED":   "session history storage is unavailable",
		"SESSION_QUERY_SEARCH_DISABLED":      "session search is disabled in this deployment",
		"SESSION_QUERY_SESSION_NOT_FOUND":    "session was not found",
		"SESSION_QUERY_STALE_CURSOR":         "session history changed while paging; retry the complete search call",
	}
	message, safe := messages[code]
	if !safe {
		return errors.New("SESSION_QUERY_TOOL_FAILED: session query operation failed")
	}
	if code == "SESSION_QUERY_INVALID_RANGE" {
		code = "SESSION_QUERY_INVALID_FILTER"
	}
	return fmt.Errorf("%s: %s", code, message)
}

func sessionCWD(s *Session) string {
	s.mu.Lock()
	cwd := s.Header.CWD
	s.mu.Unlock()
	return cwd
}

func sessionQueryWorkspaceKey(cwd string) string {
	return cwd
}

func sessionQueryHeadersCompatible(left, right SessionHeader) bool {
	return left.Version == right.Version &&
		left.ID == right.ID &&
		left.CreatedAt == right.CreatedAt &&
		left.CWD == right.CWD &&
		left.ParentSession == right.ParentSession &&
		left.IsSeeded == right.IsSeeded &&
		left.DelegationDepth == right.DelegationDepth
}

func sessionQueryObservedTargetAuthorized(callerID, callerCWD, targetID string, header SessionHeader) bool {
	if header.ID != targetID || header.CWD != callerCWD {
		return false
	}
	return targetID == callerID || callerCWD != ""
}

func (e *Engine) authorizeSessionQuery(callerID, targetID string) (*Session, error) {
	if strings.TrimSpace(callerID) == "" {
		return nil, errors.New("SESSION_QUERY_TOOL_MISSING_AGENT: session query tools require an agent-bound caller")
	}
	caller, err := e.getSession(callerID)
	if err != nil {
		return nil, err
	}
	if targetID == "" {
		targetID = callerID
	}
	target, err := e.getSession(targetID)
	if err != nil {
		return nil, errors.New("SESSION_QUERY_TOOL_UNAUTHORIZED: target session is outside the caller workspace")
	}
	if callerID == targetID {
		return target, nil
	}
	callerCWD := sessionCWD(caller)
	if callerCWD == "" || callerCWD != sessionCWD(target) {
		return nil, errors.New("SESSION_QUERY_TOOL_UNAUTHORIZED: target session is outside the caller workspace")
	}
	return target, nil
}

func (e *Engine) sessionQuerySnapshots() []sessionQuerySessionSnapshot {
	snapshots, _ := e.sessionQuerySnapshotsContext(context.Background())
	return snapshots
}

func (e *Engine) sessionQueryAuthorizeIDsContext(ctx context.Context, callerCWD string, ids map[string]bool) (map[string]bool, error) {
	if len(ids) == 0 {
		return map[string]bool{}, nil
	}
	e.mu.RLock()
	sessions := make(map[string]*Session, len(e.sessions))
	for id, session := range e.sessions {
		sessions[id] = session
	}
	e.mu.RUnlock()
	headers := make(map[string]SessionHeader, len(sessions))
	authorized := make(map[string]bool, len(ids))
	for id, session := range sessions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		session.mu.Lock()
		header := session.Header
		session.mu.Unlock()
		headers[id] = header
		if ids[id] && header.CWD == callerCWD {
			authorized[id] = true
		}
	}
	if e.sessionStore == nil {
		return authorized, nil
	}
	persisted, err := e.sessionStore.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, snapshot := range persisted {
		if live, ok := headers[snapshot.Header.ID]; ok {
			if !sessionQueryHeadersCompatible(live, snapshot.Header) {
				return nil, fmt.Errorf("SESSION_QUERY_SOURCE_CONFLICT: session source headers conflict for session %q", live.ID)
			}
			continue
		}
		if ids[snapshot.Header.ID] && snapshot.Header.CWD == callerCWD {
			authorized[snapshot.Header.ID] = true
		}
	}
	return authorized, nil
}

func (e *Engine) sessionQuerySnapshotsContext(ctx context.Context) ([]sessionQuerySessionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	persisted := map[string]SessionPersistenceSnapshot{}
	if e.sessionStore != nil {
		snapshots, err := e.sessionStore.ListSnapshots(ctx)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, snapshot := range snapshots {
			persisted[snapshot.Header.ID] = snapshot
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		session.mu.Lock()
		snapshot := sessionQuerySessionSnapshot{
			header:   session.Header,
			title:    session.Title,
			events:   cloneSessionQueryEvents(session.Events),
			archived: archived[session.Header.ID],
			live:     session.attached,
		}
		session.mu.Unlock()
		if durable, ok := persisted[snapshot.header.ID]; ok {
			if !sessionQueryHeadersCompatible(snapshot.header, durable.Header) {
				return nil, fmt.Errorf("SESSION_QUERY_SOURCE_CONFLICT: session source headers conflict for session %q", snapshot.header.ID)
			}
			snapshot.persisted = true
			delete(persisted, snapshot.header.ID)
			if !snapshot.live {
				inspection, err := e.sessionStore.Inspect(ctx, snapshot.header.ID)
				if err != nil {
					return nil, err
				}
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if !sessionQueryHeadersCompatible(inspection.Meta, durable.Header) {
					return nil, fmt.Errorf("SESSION_QUERY_SOURCE_CONFLICT: session source headers conflict for session %q", snapshot.header.ID)
				}
				snapshot.header = inspection.Meta
				snapshot.title = sessionTitleFromEvents(inspection.Events)
				snapshot.events = cloneSessionQueryEvents(inspection.Events)
			}
		}
		result = append(result, snapshot)
	}
	for id, durable := range persisted {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		inspection, err := e.sessionStore.Inspect(ctx, id)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !sessionQueryHeadersCompatible(inspection.Meta, durable.Header) {
			return nil, fmt.Errorf("SESSION_QUERY_SOURCE_CONFLICT: session source headers conflict for session %q", id)
		}
		result = append(result, sessionQuerySessionSnapshot{
			header: inspection.Meta, title: sessionTitleFromEvents(inspection.Events),
			events: cloneSessionQueryEvents(inspection.Events), archived: archived[id], persisted: true,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].header.CreatedAt != result[j].header.CreatedAt {
			return result[i].header.CreatedAt > result[j].header.CreatedAt
		}
		return result[i].header.ID < result[j].header.ID
	})
	return result, nil
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

func clipQueryText(text string) string {
	text = strings.TrimSpace(strings.Join(strings.Fields(text), " "))
	runes := []rune(text)
	if len(runes) > 240 {
		return string(runes[:239]) + "…"
	}
	return text
}

func querySnippet(text, query string) string {
	text = strings.TrimSpace(strings.Join(strings.Fields(text), " "))
	if text == "" {
		return ""
	}
	characters := []rune(text)
	if len(characters) <= 240 {
		return text
	}
	_, match := sessionQueryPhraseMatches(text, query)
	if match < 0 {
		return clipQueryText(text)
	}
	start := match - 80
	if start < 0 {
		start = 0
	}
	prefix := ""
	if start > 0 {
		prefix = "…"
	}
	contentLength := 240 - len([]rune(prefix)) - 1
	end := start + contentLength
	if end >= len(characters) {
		end = len(characters)
		prefixLength := len([]rune(prefix))
		start = end - (240 - prefixLength)
		if start < 0 {
			start = 0
		}
		return prefix + string(characters[start:end])
	}
	if match >= end {
		start = match - contentLength + 1
		end = start + contentLength
	}
	return prefix + string(characters[start:end]) + "…"
}

func sessionQueryTextRank(text, query string, record sessionQueryEventRecord) (sessionQueryMatchRank, bool) {
	matchCount, _ := sessionQueryPhraseMatches(text, query)
	if matchCount == 0 {
		return sessionQueryMatchRank{}, false
	}
	return sessionQueryMatchRank{
		matchCount: matchCount, documentLength: utf8.RuneCountInString(text),
		time: record.Time, seq: record.Seq,
	}, true
}

func sessionQueryPhraseMatches(text, query string) (int, int) {
	textTokens := sessionQueryTokens(text)
	queryTokens := sessionQueryTokens(query)
	if len(queryTokens) == 0 || len(queryTokens) > len(textTokens) {
		return 0, -1
	}
	count, first := 0, -1
	for start := 0; start+len(queryTokens) <= len(textTokens); start++ {
		matches := true
		for offset, queryToken := range queryTokens {
			if textTokens[start+offset].value != queryToken.value {
				matches = false
				break
			}
		}
		if matches {
			count++
			if first < 0 {
				first = textTokens[start].start
			}
		}
	}
	return count, first
}

func sessionQueryTokens(value string) []sessionQueryToken {
	characters := []rune(value)
	tokens := make([]sessionQueryToken, 0)
	start := -1
	flush := func(end int) {
		if start < 0 {
			return
		}
		var normalized strings.Builder
		for _, character := range norm.NFD.String(string(characters[start:end])) {
			if unicode.Is(unicode.Mn, character) || unicode.Is(unicode.Me, character) {
				continue
			}
			normalized.WriteRune(unicode.ToLower(character))
		}
		if normalized.Len() > 0 {
			tokens = append(tokens, sessionQueryToken{value: normalized.String(), start: start})
		}
		start = -1
	}
	for index, character := range characters {
		if unicode.IsLetter(character) || unicode.IsNumber(character) || start >= 0 && unicode.IsMark(character) {
			if start < 0 {
				start = index
			}
			continue
		}
		flush(index)
	}
	flush(len(characters))
	return tokens
}

func sessionQueryRankBetter(left, right sessionQueryMatchRank) bool {
	if left.matchCount != right.matchCount {
		return left.matchCount > right.matchCount
	}
	if left.documentLength != right.documentLength {
		return left.documentLength < right.documentLength
	}
	if left.time != right.time {
		return left.time > right.time
	}
	return left.seq > right.seq
}

var sessionQueryISOTime = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?(Z|[+-]\d{2}:\d{2})$`)

type sessionQueryExactTime struct {
	millisecond int64
	remainder   string
}

func parseSessionQueryTime(name, value string) (sessionQueryExactTime, error) {
	value = strings.TrimSpace(value)
	match := sessionQueryISOTime.FindStringSubmatch(value)
	if match == nil {
		return sessionQueryExactTime{}, fmt.Errorf("SESSION_QUERY_INVALID_FILTER: %s must be an ISO 8601 timestamp with Z or a numeric offset", name)
	}
	part := func(index int) int {
		value, _ := strconv.Atoi(match[index])
		return value
	}
	year, month, day := part(1), part(2), part(3)
	hour, minute := part(4), part(5)
	second := 0
	if match[6] != "" {
		second = part(6)
	}
	if month < 1 || month > 12 || day < 1 || day > daysInSessionQueryMonth(year, month) || hour > 23 || minute > 59 || second > 59 {
		return sessionQueryExactTime{}, fmt.Errorf("SESSION_QUERY_INVALID_FILTER: %s must be a valid ISO 8601 timestamp", name)
	}
	fraction := match[7]
	millisecondDigits := fraction
	if len(millisecondDigits) > 3 {
		millisecondDigits = millisecondDigits[:3]
	}
	for len(millisecondDigits) < 3 {
		millisecondDigits += "0"
	}
	normalized := fmt.Sprintf("%s-%s-%sT%s:%s:%02d.%s%s", match[1], match[2], match[3], match[4], match[5], second, millisecondDigits, match[8])
	timestamp, err := time.Parse("2006-01-02T15:04:05.000Z07:00", normalized)
	if err != nil {
		return sessionQueryExactTime{}, fmt.Errorf("SESSION_QUERY_INVALID_FILTER: %s must be a valid ISO 8601 timestamp", name)
	}
	remainder := ""
	if len(fraction) > 3 {
		remainder = strings.TrimRight(fraction[3:], "0")
	}
	return sessionQueryExactTime{millisecond: timestamp.UnixMilli(), remainder: remainder}, nil
}

func daysInSessionQueryMonth(year, month int) int {
	if month == 2 {
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	}
	if month == 4 || month == 6 || month == 9 || month == 11 {
		return 30
	}
	return 31
}

func compareSessionQueryTimes(left, right sessionQueryExactTime) int {
	if left.millisecond < right.millisecond {
		return -1
	}
	if left.millisecond > right.millisecond {
		return 1
	}
	length := len(left.remainder)
	if len(right.remainder) > length {
		length = len(right.remainder)
	}
	for index := 0; index < length; index++ {
		leftDigit, rightDigit := byte('0'), byte('0')
		if index < len(left.remainder) {
			leftDigit = left.remainder[index]
		}
		if index < len(right.remainder) {
			rightDigit = right.remainder[index]
		}
		if leftDigit < rightDigit {
			return -1
		}
		if leftDigit > rightDigit {
			return 1
		}
	}
	return 0
}

func sessionQueryLowerBound(value sessionQueryExactTime) int64 {
	if value.remainder == "" {
		return value.millisecond
	}
	return value.millisecond + 1
}

func sessionQueryUpperBound(value sessionQueryExactTime) int64 {
	return value.millisecond
}

func validateQueryRange(name string, from, to *int) error {
	if from != nil && (*from < 0 || int64(*from) > maxSessionQuerySafeInteger) {
		return fmt.Errorf("SESSION_QUERY_INVALID_FILTER: %s_from must be a non-negative safe integer", name)
	}
	if to != nil && (*to < 0 || int64(*to) > maxSessionQuerySafeInteger) {
		return fmt.Errorf("SESSION_QUERY_INVALID_FILTER: %s_to must be a non-negative safe integer", name)
	}
	if from != nil && to != nil && *from > *to {
		return fmt.Errorf("SESSION_QUERY_INVALID_FILTER: %s_from must be less than or equal to %s_to", name, name)
	}
	return nil
}

func validateSessionQuerySafeInteger(name string, value int) error {
	if value < 0 || int64(value) > maxSessionQuerySafeInteger {
		return fmt.Errorf("SESSION_QUERY_INVALID_FILTER: %s must be a non-negative safe integer", name)
	}
	return nil
}

func cloneStringSet(values []string, name string) (map[string]bool, error) {
	if values == nil {
		return nil, nil
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("SESSION_QUERY_INVALID_FILTER: %s must contain at least one value when supplied", name)
	}
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set, nil
}

func buildSessionQueryEventFilters(seqFrom, seqTo *int, timeFrom, timeTo *string, eventTypes []string, surfaces []string) (sessionQueryEventFilters, error) {
	filters := sessionQueryEventFilters{}
	if err := validateQueryRange("seq", seqFrom, seqTo); err != nil {
		return filters, err
	}
	filters.seqFrom, filters.seqTo = seqFrom, seqTo
	var exactFrom, exactTo *sessionQueryExactTime
	if timeFrom != nil {
		value, err := parseSessionQueryTime("time_from", *timeFrom)
		if err != nil {
			return filters, err
		}
		exactFrom = &value
		bound := sessionQueryLowerBound(value)
		filters.timeFrom = &bound
	}
	if timeTo != nil {
		value, err := parseSessionQueryTime("time_to", *timeTo)
		if err != nil {
			return filters, err
		}
		exactTo = &value
		bound := sessionQueryUpperBound(value)
		filters.timeTo = &bound
	}
	if exactFrom != nil && exactTo != nil && compareSessionQueryTimes(*exactFrom, *exactTo) > 0 {
		return filters, errors.New("SESSION_QUERY_INVALID_FILTER: session time range from must be less than or equal to to")
	}
	types, err := cloneStringSet(eventTypes, "event_types")
	if err != nil {
		return filters, err
	}
	filters.types = types
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
			return filters, errors.New("SESSION_QUERY_INVALID_FILTER: surfaces must contain at least one value when supplied")
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
		if int(event.Seq) != index {
			return analysis, fmt.Errorf("SESSION_QUERY_INVALID_SURFACE: session %q has non-contiguous event seq %d at index %d", sessionID, event.Seq, index)
		}
		if event.SourceEventSeqs != nil {
			seen := make(map[int]bool, len(event.SourceEventSeqs))
			for _, source := range event.SourceEventSeqs {
				if source < 0 || source >= int(event.Seq) {
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
			surface = append(surface, int(event.Seq))
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
		analysis.replacedEventSeqs[int(event.Seq)] = removed
		for _, seq := range removed {
			analysis.replacedBy[seq] = int(event.Seq)
		}
		next := make([]int, 0, len(surface)-len(removed)+1)
		next = append(next, surface[:startIndex]...)
		next = append(next, int(event.Seq))
		next = append(next, surface[endIndex+1:]...)
		surface = next
	}
	for _, seq := range surface {
		analysis.current[seq] = true
	}
	for _, event := range events {
		surfaceKind := sessionSurfaceLogOnly
		if analysis.current[int(event.Seq)] {
			surfaceKind = sessionSurfaceCurrent
		} else if _, ok := analysis.replacedBy[int(event.Seq)]; ok {
			surfaceKind = sessionSurfaceShadowed
		}
		analysis.records[int(event.Seq)] = sessionQueryEventRecord{
			SessionID: sessionID, Seq: int(event.Seq), Type: event.Type, Time: event.Time, Surface: surfaceKind,
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
	if snapshot.live {
		values = append(values, "live")
	}
	if snapshot.persisted {
		values = append(values, "persisted")
	}
	if len(values) == 0 {
		return "unavailable"
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
	collection, err := e.sessionQuerySearchWithOptionsContext(context.Background(), callerID, query, options)
	return collection.items, err
}

func (e *Engine) sessionQuerySearchWithOptionsContext(ctx context.Context, callerID, query string, options sessionQuerySearchOptions) (sessionQueryCollection, error) {
	if err := ctx.Err(); err != nil {
		return sessionQueryCollection{}, err
	}
	query, err := normalizeSessionQueryString(query)
	if err != nil {
		return sessionQueryCollection{}, err
	}
	caller, err := e.getSession(callerID)
	if err != nil {
		if strings.TrimSpace(callerID) == "" {
			return sessionQueryCollection{}, errors.New("SESSION_QUERY_TOOL_MISSING_AGENT: session query tools require an agent-bound caller")
		}
		return sessionQueryCollection{}, err
	}
	callerCWD := sessionCWD(caller)
	if callerCWD == "" {
		return sessionQueryCollection{}, errors.New("SESSION_QUERY_TOOL_UNAUTHORIZED: caller session has no workspace")
	}
	if len(options.filters.parents) > 0 {
		authorized, err := e.sessionQueryAuthorizeIDsContext(ctx, callerCWD, options.filters.parents)
		if err != nil {
			return sessionQueryCollection{}, err
		}
		options.filters.parents = authorized
		if len(authorized) == 0 && !options.filters.includeRoots {
			return sessionQueryCollection{items: []map[string]any{}}, nil
		}
	}
	if options.max <= 0 {
		options.max = defaultSessionQueryMaxResults
	}
	rankedRows := make([]sessionQueryRankedRow, 0, options.max)
	snapshots, err := e.sessionQuerySnapshotsContext(ctx)
	if err != nil {
		return sessionQueryCollection{}, err
	}
	for _, snapshot := range snapshots {
		if err := ctx.Err(); err != nil {
			return sessionQueryCollection{}, err
		}
		if snapshot.archived || (options.excludeCaller && snapshot.header.ID == callerID) {
			continue
		}
		if sessionQueryWorkspaceKey(snapshot.header.CWD) != callerCWD || !sessionQuerySessionFiltersMatch(options.filters, snapshot) {
			continue
		}
		analysis, err := e.sessionQueryAnalyzeEvents(snapshot.header.ID, snapshot.events)
		if err != nil {
			return sessionQueryCollection{}, err
		}
		var best *sessionQueryEventRecord
		var bestText string
		var bestRank sessionQueryMatchRank
		for _, event := range snapshot.events {
			record := analysis.records[int(event.Seq)]
			text := sessionQueryEventText(event)
			if !options.filters.event.matches(record, text) {
				continue
			}
			rank, matches := sessionQueryTextRank(text, query, record)
			if !matches {
				continue
			}
			if best == nil || sessionQueryRankBetter(rank, bestRank) {
				copyRecord := record
				best, bestText, bestRank = &copyRecord, text, rank
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
		rankedRows = append(rankedRows, sessionQueryRankedRow{row: row, rank: bestRank})
	}
	parentIDs := map[string]bool{}
	for _, ranked := range rankedRows {
		if parent := mapString(ranked.row, "parentSession"); parent != "" {
			parentIDs[parent] = true
		}
	}
	authorizedParents, err := e.sessionQueryAuthorizeIDsContext(ctx, callerCWD, parentIDs)
	if err != nil {
		return sessionQueryCollection{}, err
	}
	for _, ranked := range rankedRows {
		if parent := mapString(ranked.row, "parentSession"); parent != "" {
			ranked.row["parentAuthorized"] = authorizedParents[parent]
		}
	}
	sort.SliceStable(rankedRows, func(i, j int) bool {
		if rankedRows[i].rank != rankedRows[j].rank {
			return sessionQueryRankBetter(rankedRows[i].rank, rankedRows[j].rank)
		}
		return mapString(rankedRows[i].row, "sessionId") < mapString(rankedRows[j].row, "sessionId")
	})
	capped := len(rankedRows) > options.max
	if capped {
		rankedRows = rankedRows[:options.max]
	}
	rows := make([]map[string]any, len(rankedRows))
	for index, ranked := range rankedRows {
		rows[index] = ranked.row
	}
	return sessionQueryCollection{items: rows, capped: capped}, nil
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
		if !(filters.availability["live"] && snapshot.live) &&
			!(filters.availability["persisted"] && snapshot.persisted) {
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
	collection, err := e.sessionQueryEventSearchWithOptionsContext(context.Background(), callerID, targetID, query, options)
	return collection.items, err
}

func (e *Engine) sessionQueryEventSearchWithOptionsContext(ctx context.Context, callerID, targetID, query string, options sessionQueryEventSearchOptions) (sessionQueryCollection, error) {
	if err := ctx.Err(); err != nil {
		return sessionQueryCollection{}, err
	}
	query, err := normalizeSessionQueryString(query)
	if err != nil {
		return sessionQueryCollection{}, err
	}
	caller, err := e.getSession(callerID)
	if err != nil {
		return sessionQueryCollection{}, err
	}
	callerCWD := sessionCWD(caller)
	session, err := e.authorizeSessionQuery(callerID, targetID)
	if err != nil {
		return sessionQueryCollection{}, err
	}
	targetID = targetIDOrCaller(callerID, targetID)
	session.mu.Lock()
	header := session.Header
	title := session.Title
	events := cloneSessionQueryEvents(session.Events)
	session.mu.Unlock()
	if !sessionQueryObservedTargetAuthorized(callerID, callerCWD, targetID, header) {
		return sessionQueryCollection{}, errors.New("SESSION_QUERY_TOOL_UNAUTHORIZED: target session is outside the caller workspace")
	}
	if options.max <= 0 {
		options.max = defaultSessionQueryMaxResults
	}
	analysis, err := e.sessionQueryAnalyzeEvents(targetID, events)
	if err != nil {
		return sessionQueryCollection{}, err
	}
	if options.excludeActiveStep && targetID == callerID {
		boundary := -1
		for _, event := range events {
			if event.Type == "step/start" {
				boundary = int(event.Seq)
			}
		}
		if boundary < 0 {
			return sessionQueryCollection{}, errors.New("SESSION_QUERY_TOOL_NO_CURRENT_STEP: current-session search requires an active step boundary")
		}
		if options.filters.seqTo == nil || *options.filters.seqTo >= boundary {
			value := boundary - 1
			options.filters.seqTo = &value
		}
		if options.filters.seqFrom != nil && options.filters.seqTo != nil && *options.filters.seqFrom > *options.filters.seqTo {
			return sessionQueryCollection{items: []map[string]any{}, sessionID: targetID, title: title}, nil
		}
	}
	rankedRows := make([]sessionQueryRankedRow, 0, len(events))
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return sessionQueryCollection{}, err
		}
		record := analysis.records[int(event.Seq)]
		text := sessionQueryEventText(event)
		if !options.filters.matches(record, text) {
			continue
		}
		rank, matches := sessionQueryTextRank(text, query, record)
		if !matches {
			continue
		}
		row := sessionQueryRecordMap(record)
		row["snippet"] = querySnippet(text, query)
		rankedRows = append(rankedRows, sessionQueryRankedRow{row: row, rank: rank})
	}
	sort.SliceStable(rankedRows, func(i, j int) bool {
		return sessionQueryRankBetter(rankedRows[i].rank, rankedRows[j].rank)
	})
	capped := len(rankedRows) > options.max
	if capped {
		rankedRows = rankedRows[:options.max]
	}
	rows := make([]map[string]any, len(rankedRows))
	for index, ranked := range rankedRows {
		rows[index] = ranked.row
	}
	return sessionQueryCollection{items: rows, capped: capped, sessionID: targetID, title: title}, nil
}

func targetIDOrCaller(callerID, targetID string) string {
	if targetID == "" {
		return callerID
	}
	return targetID
}

func sessionQueryToolTarget(_ string, target *string) (string, error) {
	if target == nil {
		return "", nil
	}
	if *target == "" {
		return "", errors.New("SESSION_QUERY_TOOL_UNAUTHORIZED: target session is outside the caller workspace")
	}
	return *target, nil
}

func sessionQueryPresentationMeta(call ToolCall, _ any) (any, error) {
	arguments := map[string]any{}
	if len(strings.TrimSpace(string(call.Arguments))) > 0 {
		if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
			return nil, err
		}
		if arguments == nil {
			arguments = map[string]any{}
		}
	}
	sessionID, hasSessionID := arguments["session_id"].(string)
	switch call.Name {
	case "session_search":
		return map[string]any{"card": "generic", "kind": "search", "title": "Search prior sessions", "rawInput": arguments["query"]}, nil
	case "session_event_search":
		return map[string]any{"card": "generic", "kind": "search", "title": "Search session events", "rawInput": arguments["query"]}, nil
	case "session_trace":
		if !hasSessionID {
			return map[string]any{"card": "generic", "kind": "read", "title": "Trace current session"}, nil
		}
		return map[string]any{"card": "generic", "kind": "read", "title": "Trace session " + sessionID, "rawInput": sessionID}, nil
	case "session_event_trace", "session_event_read":
		action := "Trace event"
		if call.Name == "session_event_read" {
			action = "Read event"
		}
		rawInput := map[string]any{"seq": arguments["seq"]}
		if hasSessionID {
			rawInput["session_id"] = sessionID
		}
		return map[string]any{"card": "generic", "kind": "read", "title": fmt.Sprintf("%s %v", action, arguments["seq"]), "rawInput": rawInput}, nil
	default:
		return nil, nil
	}
}

func (e *Engine) sessionQueryTrace(callerID, targetID string) (map[string]any, error) {
	return e.sessionQueryTraceContext(context.Background(), callerID, targetID)
}

func (e *Engine) sessionQueryTraceContext(ctx context.Context, callerID, targetID string) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	caller, err := e.getSession(callerID)
	if err != nil {
		return nil, err
	}
	callerCWD := sessionCWD(caller)
	_, err = e.authorizeSessionQuery(callerID, targetID)
	if err != nil {
		return nil, err
	}
	targetID = targetIDOrCaller(callerID, targetID)
	snapshots, err := e.sessionQuerySnapshotsContext(ctx)
	if err != nil {
		return nil, err
	}
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
	if !sessionQueryObservedTargetAuthorized(callerID, callerCWD, targetID, targetSnapshot.header) {
		return nil, errors.New("SESSION_QUERY_TOOL_UNAUTHORIZED: target session is outside the caller workspace")
	}
	lineageSeen := map[string]bool{targetID: true}
	for parentID := targetSnapshot.header.ParentSession; parentID != ""; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if lineageSeen[parentID] {
			return nil, fmt.Errorf("SESSION_QUERY_INVALID_LINEAGE: session lineage contains a cycle at %q", parentID)
		}
		lineageSeen[parentID] = true
		parent, exists := byID[parentID]
		if !exists {
			break
		}
		parentID = parent.header.ParentSession
	}
	ancestors := make([]map[string]any, 0)
	seen := map[string]bool{targetID: true}
	parentID := targetSnapshot.header.ParentSession
	complete := true
	var unresolved string
	for parentID != "" {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if seen[parentID] {
			return nil, fmt.Errorf("SESSION_QUERY_INVALID_LINEAGE: session lineage contains a cycle at %q", parentID)
		}
		seen[parentID] = true
		parent, exists := byID[parentID]
		if !exists || callerCWD == "" || parent.header.CWD != callerCWD {
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
	descendants, err := buildSessionQueryDescendants(ctx, targetID, callerCWD, children)
	if err != nil {
		return nil, err
	}
	result := map[string]any{
		"sessionId":   targetID,
		"target":      sessionQuerySessionMap(targetSnapshot),
		"session":     sessionQuerySessionMap(targetSnapshot),
		"ancestors":   ancestors,
		"descendants": descendants,
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

func buildSessionQueryDescendants(ctx context.Context, targetID, callerCWD string, children map[string][]sessionQuerySessionSnapshot) ([]map[string]any, error) {
	type frame struct {
		target   *[]map[string]any
		owner    map[string]any
		children []sessionQuerySessionSnapshot
		index    int
		entered  string
	}
	result := []map[string]any{}
	lineage := map[string]bool{targetID: true}
	stack := []frame{{target: &result, children: children[targetID]}}
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current := &stack[len(stack)-1]
		if current.index >= len(current.children) {
			if current.owner != nil {
				current.owner["descendants"] = *current.target
			}
			if current.entered != "" {
				delete(lineage, current.entered)
			}
			stack = stack[:len(stack)-1]
			continue
		}
		child := current.children[current.index]
		current.index++
		if callerCWD == "" || child.header.CWD != callerCWD {
			*current.target = append(*current.target, map[string]any{"outsideWorkspace": true})
			continue
		}
		if lineage[child.header.ID] {
			*current.target = append(*current.target, map[string]any{"session": sessionQuerySessionMap(child), "invalidLineage": true})
			continue
		}
		lineage[child.header.ID] = true
		nodeChildren := []map[string]any{}
		node := map[string]any{"session": sessionQuerySessionMap(child), "descendants": nodeChildren}
		*current.target = append(*current.target, node)
		stack = append(stack, frame{target: &nodeChildren, owner: node, children: children[child.header.ID], entered: child.header.ID})
	}
	return result, nil
}

func (e *Engine) sessionQueryEventTrace(callerID, targetID string, seq int) (map[string]any, error) {
	return e.sessionQueryEventTraceContext(context.Background(), callerID, targetID, seq)
}

func (e *Engine) sessionQueryEventTraceContext(ctx context.Context, callerID, targetID string, seq int) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateSessionQuerySafeInteger("seq", seq); err != nil {
		return nil, err
	}
	caller, err := e.getSession(callerID)
	if err != nil {
		return nil, err
	}
	callerCWD := sessionCWD(caller)
	session, err := e.authorizeSessionQuery(callerID, targetID)
	if err != nil {
		return nil, err
	}
	targetID = targetIDOrCaller(callerID, targetID)
	session.mu.Lock()
	header := session.Header
	title := session.Title
	events := cloneSessionQueryEvents(session.Events)
	session.mu.Unlock()
	if !sessionQueryObservedTargetAuthorized(callerID, callerCWD, targetID, header) {
		return nil, errors.New("SESSION_QUERY_TOOL_UNAUTHORIZED: target session is outside the caller workspace")
	}
	if seq >= len(events) || int(events[seq].Seq) != seq {
		return nil, fmt.Errorf("SESSION_QUERY_EVENT_NOT_FOUND: session %q has no event at seq %d", targetID, seq)
	}
	analysis, err := e.sessionQueryAnalyzeEvents(targetID, events)
	if err != nil {
		return nil, err
	}
	target := events[seq]
	replacementChain := []int{}
	for replacement, ok := analysis.replacedBy[seq]; ok; replacement, ok = analysis.replacedBy[replacement] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		replacementChain = append(replacementChain, replacement)
		if len(replacementChain) > len(events) {
			return nil, errors.New("SESSION_QUERY_INVALID_LINEAGE: replacement chain contains a cycle")
		}
	}
	derived := []int{}
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if int(event.Seq) <= seq {
			continue
		}
		for _, source := range event.SourceEventSeqs {
			if source == seq {
				derived = append(derived, int(event.Seq))
				break
			}
		}
	}
	sources := append([]int(nil), target.SourceEventSeqs...)
	replaced := append([]int(nil), analysis.replacedEventSeqs[seq]...)
	targetRecord := sessionQueryRecordMap(analysis.records[seq])
	result := map[string]any{
		"sessionId":         targetID,
		"title":             sessionQueryTitle(title),
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
	return e.sessionQueryEventReadContext(context.Background(), callerID, targetID, seq, before, after)
}

func (e *Engine) sessionQueryEventReadContext(ctx context.Context, callerID, targetID string, seq, before, after int) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateSessionQuerySafeInteger("seq", seq); err != nil {
		return nil, err
	}
	if err := validateSessionQuerySafeInteger("before", before); err != nil {
		return nil, err
	}
	if err := validateSessionQuerySafeInteger("after", after); err != nil {
		return nil, err
	}
	if before > maxSessionQueryWindow || after > maxSessionQueryWindow {
		return nil, fmt.Errorf("SESSION_QUERY_INVALID_WINDOW: before and after must be no greater than %d", maxSessionQueryWindow)
	}
	caller, err := e.getSession(callerID)
	if err != nil {
		return nil, err
	}
	callerCWD := sessionCWD(caller)
	session, err := e.authorizeSessionQuery(callerID, targetID)
	if err != nil {
		return nil, err
	}
	targetID = targetIDOrCaller(callerID, targetID)
	session.mu.Lock()
	header := session.Header
	title := session.Title
	events := cloneSessionQueryEvents(session.Events)
	session.mu.Unlock()
	if !sessionQueryObservedTargetAuthorized(callerID, callerCWD, targetID, header) {
		return nil, errors.New("SESSION_QUERY_TOOL_UNAUTHORIZED: target session is outside the caller workspace")
	}
	if seq >= len(events) || int(events[seq].Seq) != seq {
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if int(event.Seq) == seq {
			continue
		}
		neighbor := map[string]any{"seq": int(event.Seq), "type": event.Type, "time": event.Time}
		if text := sessionQueryEventText(event); text != "" {
			neighbor["snippet"] = clipQueryText(text)
		}
		neighbors = append(neighbors, neighbor)
	}
	return map[string]any{
		"sessionId": targetID,
		"title":     sessionQueryTitle(title),
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
	result, _ := eventSeqNumber(value[key])
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

func formatSessionSearchResults(rows []map[string]any, capped bool) string {
	if len(rows) == 0 {
		return "No prior session matches found."
	}
	lines := []string{fmt.Sprintf("Session search results (%d):", len(rows))}
	for index, row := range rows {
		best, _ := row["bestMatch"].(map[string]any)
		parent := mapString(row, "parentSession")
		if parent == "" {
			parent = "root"
		} else if authorized, _ := row["parentAuthorized"].(bool); !authorized {
			parent = "[outside workspace]"
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
	if capped {
		lines = append(lines, "", "Result cap reached. Narrow the query or add filters to find additional matches.")
	}
	return strings.Join(lines, "\n")
}

func formatEventSearchResults(rows []map[string]any, sessionID, title string, capped bool) string {
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
	if capped {
		lines = append(lines, "", "Result cap reached. Narrow the query or add filters to find additional matches.")
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
	type frame struct {
		nodes []map[string]any
		index int
		depth int
	}
	stack := []frame{{nodes: nodes, depth: depth}}
	for len(stack) > 0 {
		current := &stack[len(stack)-1]
		if current.index >= len(current.nodes) {
			stack = stack[:len(stack)-1]
			continue
		}
		node := current.nodes[current.index]
		current.index++
		indent := strings.Repeat("  ", current.depth)
		if outside, _ := node["outsideWorkspace"].(bool); outside {
			*lines = append(*lines, indent+"- [outside workspace subtree]")
			continue
		}
		session, _ := node["session"].(map[string]any)
		*lines = append(*lines, fmt.Sprintf("%s- %s - %s | %s | %s", indent, mapString(session, "sessionId"), sessionQueryTitle(mapString(session, "title")), formatSessionQueryTime(session["createdAt"]), mapString(session, "availability")))
		children, _ := node["descendants"].([]map[string]any)
		if len(children) > 0 {
			stack = append(stack, frame{nodes: children, depth: current.depth + 1})
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
		fmt.Sprintf("Session %s - %s", mapString(trace, "sessionId"), sessionQueryTitle(mapString(trace, "title"))),
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
		fmt.Sprintf("Session %s - %s", mapString(value, "sessionId"), sessionQueryTitle(mapString(value, "title"))),
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
		{"Before", func(seq int) bool { return seq < int(target.Seq) }},
		{"After", func(seq int) bool { return seq > int(target.Seq) }},
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
			} else {
				line += " | (no semantic text)"
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
		SessionID  *string  `json:"session_id"`
		Query      string   `json:"query"`
		SeqFrom    *int     `json:"seq_from"`
		SeqTo      *int     `json:"seq_to"`
		TimeFrom   *string  `json:"time_from"`
		TimeTo     *string  `json:"time_to"`
		EventTypes []string `json:"event_types"`
		Surfaces   []string `json:"surfaces"`
	}
	type targetInput struct {
		SessionID *string `json:"session_id"`
	}
	type eventTargetInput struct {
		SessionID *string `json:"session_id"`
		Seq       int     `json:"seq"`
	}
	type eventReadInput struct {
		SessionID *string `json:"session_id"`
		Seq       int     `json:"seq"`
		Before    *int    `json:"before"`
		After     *int    `json:"after"`
	}

	tools := []Tool{
		{
			Timeout: defaultSessionQuerySearchTimeout,
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
					return ToolResult{}, sessionQueryOperationError(err)
				}
				collection, err := e.sessionQuerySearchWithOptionsContext(ctx, call.SessionID, in.Query, sessionQuerySearchOptions{max: defaultSessionQueryMaxResults, filters: filters, excludeCaller: true})
				if err != nil {
					return ToolResult{}, sessionQueryOperationError(err)
				}
				return sessionQueryFormattedResult(collection.items, formatSessionSearchResults(collection.items, collection.capped)), nil
			},
		},
		{
			Timeout: defaultSessionQuerySearchTimeout,
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
				targetID, err := sessionQueryToolTarget(call.SessionID, in.SessionID)
				if err != nil {
					return ToolResult{}, sessionQueryOperationError(err)
				}
				filters, err := buildEventSearchInputFilters(in.SeqFrom, in.SeqTo, in.TimeFrom, in.TimeTo, in.EventTypes, in.Surfaces)
				if err != nil {
					return ToolResult{}, sessionQueryOperationError(err)
				}
				collection, err := e.sessionQueryEventSearchWithOptionsContext(ctx, call.SessionID, targetID, in.Query, sessionQueryEventSearchOptions{max: defaultSessionQueryMaxResults, filters: filters, excludeActiveStep: true})
				if err != nil {
					return ToolResult{}, sessionQueryOperationError(err)
				}
				return sessionQueryFormattedResult(collection.items, formatEventSearchResults(collection.items, collection.sessionID, collection.title, collection.capped)), nil
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
				targetID, err := sessionQueryToolTarget(call.SessionID, in.SessionID)
				if err != nil {
					return ToolResult{}, sessionQueryOperationError(err)
				}
				value, err := e.sessionQueryTraceContext(ctx, call.SessionID, targetID)
				if err != nil {
					return ToolResult{}, sessionQueryOperationError(err)
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
				targetID, err := sessionQueryToolTarget(call.SessionID, in.SessionID)
				if err != nil {
					return ToolResult{}, sessionQueryOperationError(err)
				}
				value, err := e.sessionQueryEventTraceContext(ctx, call.SessionID, targetID, in.Seq)
				if err != nil {
					return ToolResult{}, sessionQueryOperationError(err)
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
				targetID, err := sessionQueryToolTarget(call.SessionID, in.SessionID)
				if err != nil {
					return ToolResult{}, sessionQueryOperationError(err)
				}
				before, after := 0, 0
				if in.Before != nil {
					before = *in.Before
				}
				if in.After != nil {
					after = *in.After
				}
				value, err := e.sessionQueryEventReadContext(ctx, call.SessionID, targetID, in.Seq, before, after)
				if err != nil {
					return ToolResult{}, sessionQueryOperationError(err)
				}
				return sessionQueryFormattedResult(value, formatEventReadResult(value)), nil
			},
		},
	}
	for index, tool := range tools {
		tool.PresentationMeta = sessionQueryPresentationMeta
		if index >= 2 {
			tool.IsConcurrencySafe = alwaysConcurrencySafe
		}
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
	var exactFrom, exactTo *sessionQueryExactTime
	if in.CreatedAtFrom != nil {
		value, parseErr := parseSessionQueryTime("created_at_from", *in.CreatedAtFrom)
		if parseErr != nil {
			return filters, parseErr
		}
		exactFrom = &value
		bound := sessionQueryLowerBound(value)
		filters.createdFrom = &bound
	}
	if in.CreatedAtTo != nil {
		value, parseErr := parseSessionQueryTime("created_at_to", *in.CreatedAtTo)
		if parseErr != nil {
			return filters, parseErr
		}
		exactTo = &value
		bound := sessionQueryUpperBound(value)
		filters.createdTo = &bound
	}
	if exactFrom != nil && exactTo != nil && compareSessionQueryTimes(*exactFrom, *exactTo) > 0 {
		return filters, errors.New("SESSION_QUERY_INVALID_FILTER: session created_at range from must be less than or equal to to")
	}
	filters.event, err = buildSessionQueryEventFilters(in.EventSeqFrom, in.EventSeqTo, in.EventTimeFrom, in.EventTimeTo, in.EventTypes, in.EventSurfaces)
	return filters, err
}
