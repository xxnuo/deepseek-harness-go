package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	defaultTitleFallbackWords = 5
	defaultTitleFallbackBytes = 40
	defaultTitleMaxBytes      = 80
)

// truncateTitleUTF8 returns the longest prefix that fits maxBytes without
// splitting a UTF-8 code point.
func truncateTitleUTF8(input string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len([]byte(input)) <= maxBytes {
		return input
	}
	used := 0
	var out strings.Builder
	for _, r := range input {
		n := utf8.RuneLen(r)
		if n < 0 {
			n = 1
		}
		if used+n > maxBytes {
			break
		}
		out.WriteRune(r)
		used += n
	}
	return out.String()
}

func stripTitleEscapes(input string) string {
	runes := []rune(input)
	var out strings.Builder
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		// OSC: ESC ] ... BEL/ST (also accept the C1 OSC introducer).
		if r == '\x1b' && i+1 < len(runes) && runes[i+1] == ']' || r == '\u009d' {
			if r == '\x1b' {
				i++
			}
			for i+1 < len(runes) {
				i++
				if runes[i] == '\a' {
					break
				}
				if runes[i] == '\x1b' && i+1 < len(runes) && runes[i+1] == '\\' {
					i++
					break
				}
			}
			continue
		}
		// CSI: ESC [ ... final byte, or the C1 CSI introducer.
		if (r == '\x1b' && i+1 < len(runes) && runes[i+1] == '[') || r == '\u009b' {
			if r == '\x1b' {
				i++
			}
			for i+1 < len(runes) {
				i++
				if runes[i] >= 0x40 && runes[i] <= 0x7e {
					break
				}
			}
			continue
		}
		// Remaining two-byte ESC controls.
		if r == '\x1b' {
			if i+1 < len(runes) {
				i++
			}
			continue
		}
		// C0/C1 controls except whitespace, plus directional/invisible marks.
		if (r < 0x20 && r != '\t' && r != '\n' && r != '\r') ||
			(r >= 0x7f && r <= 0x9f) ||
			r == '\u200b' || r == '\u200e' || r == '\u200f' ||
			(r >= '\u202a' && r <= '\u202e') || (r >= '\u2060' && r <= '\u206f') || r == '\ufeff' {
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

// NormalizeSessionTitle removes terminal/invisible controls, folds whitespace
// to one line, and applies the upstream UTF-8 byte limit.
func NormalizeSessionTitle(input string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	clean := strings.Join(strings.Fields(stripTitleEscapes(input)), " ")
	return strings.TrimRight(truncateTitleUTF8(clean, maxBytes), " \t\r\n")
}

// FallbackSessionTitle derives the deterministic title used before an
// optional model-backed title provider is available.
func FallbackSessionTitle(input string, maxWords, maxBytes int) string {
	if maxWords <= 0 || maxBytes <= 0 {
		return ""
	}
	words := strings.Fields(stripTitleEscapes(input))
	if len(words) > maxWords {
		words = words[:maxWords]
	}
	return strings.TrimRight(truncateTitleUTF8(strings.Join(words, " "), maxBytes), " \t\r\n")
}

func sessionTitleFromEvents(events []Event) string {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type != "session/title" {
			continue
		}
		data, _ := events[i].Data.(map[string]any)
		if title, ok := data["title"].(string); ok {
			return NormalizeSessionTitle(title, defaultTitleMaxBytes)
		}
	}
	return ""
}

type SessionTitleAutomaticMode string

const (
	SessionTitleFirstPrompt SessionTitleAutomaticMode = "first-prompt"
	SessionTitleAllPrompts  SessionTitleAutomaticMode = "all-prompts"
)

type SessionTitleConfig struct {
	FallbackMaxWords int
	FallbackMaxBytes int
	MaxTitleBytes    int
}

type SessionTitleLLMConfig struct {
	Enabled             bool
	Automatic           SessionTitleAutomaticMode
	TargetWords         int
	TargetCJKCharacters int
	MaxInputBytes       int
	MaxOutputTokens     int
	Timeout             time.Duration
	Provider            string
	Model               string
}

type SessionTitleModelProvenance struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

type SessionTitleSource struct {
	Kind     string                       `json:"kind"`
	Provider string                       `json:"provider,omitempty"`
	Model    *SessionTitleModelProvenance `json:"model,omitempty"`
}

type SessionTitleSnapshot struct {
	Title       string             `json:"title"`
	MessageSeqs []int              `json:"messageSeqs"`
	Source      SessionTitleSource `json:"source"`
	EventSeq    int                `json:"eventSeq"`
	UpdatedAt   int64              `json:"updatedAt"`
}

type SessionTitleUserMessage struct {
	Seq  int    `json:"seq"`
	Text string `json:"text"`
}

type SessionTitleProviderRequest struct {
	Session  *Session
	Messages []SessionTitleUserMessage
	Route    *SessionTitleModelProvenance
}

type SessionTitleProviderResult struct {
	Title       string
	MessageSeqs []int
	Model       *SessionTitleModelProvenance
}

type SessionTitleProvider interface {
	ID() string
	Automatic() SessionTitleAutomaticMode
	Generate(context.Context, SessionTitleProviderRequest) (SessionTitleProviderResult, error)
}

func defaultSessionTitleConfig() SessionTitleConfig {
	return SessionTitleConfig{
		FallbackMaxWords: defaultTitleFallbackWords,
		FallbackMaxBytes: defaultTitleFallbackBytes,
		MaxTitleBytes:    defaultTitleMaxBytes,
	}
}

func normalizeSessionTitleConfig(config SessionTitleConfig) SessionTitleConfig {
	defaults := defaultSessionTitleConfig()
	if config.FallbackMaxWords == 0 {
		config.FallbackMaxWords = defaults.FallbackMaxWords
	}
	if config.FallbackMaxBytes == 0 {
		config.FallbackMaxBytes = defaults.FallbackMaxBytes
	}
	if config.MaxTitleBytes == 0 {
		config.MaxTitleBytes = defaults.MaxTitleBytes
	}
	return config
}

func validateSessionTitleConfig(config SessionTitleConfig) error {
	if config.FallbackMaxWords <= 0 || config.FallbackMaxBytes <= 0 || config.MaxTitleBytes <= 0 {
		return errors.New("session-title: limits must be positive integers")
	}
	if config.FallbackMaxBytes > config.MaxTitleBytes {
		return errors.New("session-title: fallback max bytes must not exceed title max bytes")
	}
	return nil
}

func defaultSessionTitleLLMConfig() SessionTitleLLMConfig {
	return SessionTitleLLMConfig{
		Enabled:             true,
		Automatic:           SessionTitleFirstPrompt,
		TargetWords:         5,
		TargetCJKCharacters: 10,
		MaxInputBytes:       4096,
		MaxOutputTokens:     64,
		Timeout:             time.Minute,
	}
}

func normalizeSessionTitleLLMConfig(config SessionTitleLLMConfig) SessionTitleLLMConfig {
	if !config.Enabled {
		return config
	}
	defaults := defaultSessionTitleLLMConfig()
	if config.Automatic == "" {
		config.Automatic = defaults.Automatic
	}
	if config.TargetWords == 0 {
		config.TargetWords = defaults.TargetWords
	}
	if config.TargetCJKCharacters == 0 {
		config.TargetCJKCharacters = defaults.TargetCJKCharacters
	}
	if config.MaxInputBytes == 0 {
		config.MaxInputBytes = defaults.MaxInputBytes
	}
	if config.MaxOutputTokens == 0 {
		config.MaxOutputTokens = defaults.MaxOutputTokens
	}
	if config.Timeout == 0 {
		config.Timeout = defaults.Timeout
	}
	return config
}

func validateSessionTitleLLMConfig(config SessionTitleLLMConfig) error {
	if !config.Enabled {
		return nil
	}
	if config.Automatic != SessionTitleFirstPrompt && config.Automatic != SessionTitleAllPrompts {
		return errors.New("session-title-llm: automatic mode is invalid")
	}
	if config.TargetWords <= 0 || config.TargetCJKCharacters <= 0 || config.MaxInputBytes <= 0 || config.MaxOutputTokens <= 0 || config.Timeout <= 0 {
		return errors.New("session-title-llm: limits and timeout must be positive")
	}
	if (config.Provider == "") != (config.Model == "") {
		return errors.New("session-title-llm: provider and model must be supplied together")
	}
	return nil
}

func FoldSessionTitle(events []Event) (SessionTitleSnapshot, bool) {
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.Type != "session/title" {
			continue
		}
		var data struct {
			Title       string             `json:"title"`
			MessageSeqs []int              `json:"messageSeqs"`
			Source      SessionTitleSource `json:"source"`
		}
		encoded, err := json.Marshal(event.Data)
		if err != nil || json.Unmarshal(encoded, &data) != nil || data.Title == "" {
			continue
		}
		return SessionTitleSnapshot{
			Title: data.Title, MessageSeqs: append([]int(nil), data.MessageSeqs...), Source: cloneSessionTitleSource(data.Source),
			EventSeq: event.Seq, UpdatedAt: event.Time,
		}, true
	}
	return SessionTitleSnapshot{}, false
}

func cloneSessionTitleSource(source SessionTitleSource) SessionTitleSource {
	copy := source
	if source.Model != nil {
		model := *source.Model
		copy.Model = &model
	}
	return copy
}

func collectSessionTitleMessages(events []Event, throughSeq *int) []SessionTitleUserMessage {
	messages := make([]SessionTitleUserMessage, 0)
	for _, event := range events {
		if throughSeq != nil && event.Seq > *throughSeq {
			break
		}
		if event.Type != "user/message" {
			continue
		}
		var data struct {
			Content []ContentBlock `json:"content"`
			Source  struct {
				Kind string `json:"kind"`
			} `json:"source"`
			Message *struct {
				Content []ContentBlock `json:"content"`
				Source  struct {
					Kind string `json:"kind"`
				} `json:"source"`
			} `json:"message"`
		}
		encoded, err := json.Marshal(event.Data)
		if err != nil || json.Unmarshal(encoded, &data) != nil {
			continue
		}
		content, kind := data.Content, data.Source.Kind
		if data.Message != nil {
			content, kind = data.Message.Content, data.Message.Source.Kind
		}
		if kind != "user" {
			continue
		}
		parts := make([]string, 0, len(content))
		for _, block := range content {
			if block.Type == "text" {
				parts = append(parts, block.Text)
			}
		}
		text := strings.Join(parts, "\n")
		if NormalizeSessionTitle(text, int(^uint(0)>>1)) == "" {
			continue
		}
		messages = append(messages, SessionTitleUserMessage{Seq: event.Seq, Text: text})
	}
	return messages
}

type sessionTitleProviderRegistration struct {
	provider SessionTitleProvider
	closing  bool
	active   sync.WaitGroup
}

type sessionTitlePendingWork struct {
	registration *sessionTitleProviderRegistration
	revision     uint64
	throughSeq   int
}

type sessionTitleActiveWork struct {
	sessionTitlePendingWork
	ctx    context.Context
	cancel context.CancelFunc
	route  *SessionTitleModelProvenance
}

type sessionTitleWorkState struct {
	revision uint64
	pending  *sessionTitlePendingWork
	active   *sessionTitleActiveWork
}

func (e *Engine) RegisterSessionTitleProvider(provider SessionTitleProvider) (func() error, error) {
	if provider == nil || provider.ID() == "" {
		return nil, errors.New("session-title provider id must be a non-empty string")
	}
	if provider.Automatic() != SessionTitleFirstPrompt && provider.Automatic() != SessionTitleAllPrompts {
		return nil, errors.New("session-title provider automatic mode is invalid")
	}
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, errors.New("session-title service disposed")
	}
	e.titleMu.Lock()
	if e.titleProvider != nil {
		id := e.titleProvider.provider.ID()
		e.titleMu.Unlock()
		e.mu.RUnlock()
		return nil, fmt.Errorf("session-title provider %q is already registered", id)
	}
	registration := &sessionTitleProviderRegistration{provider: provider}
	e.titleProvider = registration
	e.titleMu.Unlock()
	e.mu.RUnlock()

	var once sync.Once
	return func() error {
		once.Do(func() {
			e.titleMu.Lock()
			registration.closing = true
			for _, state := range e.titleWork {
				if state.pending != nil && state.pending.registration == registration {
					state.pending = nil
				}
				if state.active != nil && state.active.registration == registration {
					state.active.cancel()
				}
			}
			e.titleMu.Unlock()
			registration.active.Wait()
			e.titleMu.Lock()
			if e.titleProvider == registration {
				e.titleProvider = nil
			}
			e.titleMu.Unlock()
		})
		return nil
	}, nil
}

// replaceSessionTitleProvider applies the configuration-owned provider
// lifetime without cancelling the shared title context. This is the reload
// counterpart of RegisterSessionTitleProvider: active requests from the old
// provider are cancelled and drained before the new provider is published.
func (e *Engine) replaceSessionTitleProvider(config SessionTitleLLMConfig) error {
	e.titleMu.Lock()
	previous := e.titleProvider
	if previous != nil {
		previous.closing = true
		for _, state := range e.titleWork {
			if state.pending != nil && state.pending.registration == previous {
				state.pending = nil
			}
			if state.active != nil && state.active.registration == previous {
				state.active.cancel()
			}
		}
	}
	e.titleMu.Unlock()
	if previous != nil {
		previous.active.Wait()
		e.titleMu.Lock()
		if e.titleProvider == previous {
			e.titleProvider = nil
		}
		e.titleMu.Unlock()
	}
	if !config.Enabled {
		return nil
	}
	_, err := e.RegisterSessionTitleProvider(&llmSessionTitleProvider{engine: e, config: config})
	return err
}

func (e *Engine) SessionTitle(id string) (SessionTitleSnapshot, bool, error) {
	session, err := e.getSession(id)
	if err != nil {
		return SessionTitleSnapshot{}, false, err
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	snapshot, ok := FoldSessionTitle(events)
	return snapshot, ok, nil
}

func (e *Engine) RefreshSessionTitle(ctx context.Context, id string) (SessionTitleSnapshot, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SessionTitleSnapshot{}, false, err
	}
	session, err := e.getSession(id)
	if err != nil {
		return SessionTitleSnapshot{}, false, err
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	messages := collectSessionTitleMessages(events, nil)
	current, hasCurrent := FoldSessionTitle(events)

	e.titleMu.Lock()
	registration := e.titleProvider
	if registration == nil || registration.closing || len(messages) == 0 {
		e.supersedeSessionTitleLocked(id)
		e.titleMu.Unlock()
		if len(messages) > 0 && hasCurrent && current.Source.Kind == "user" {
			first := messages[0]
			title := FallbackSessionTitle(first.Text, e.cfg.SessionTitle.FallbackMaxWords, e.cfg.SessionTitle.FallbackMaxBytes)
			if title != "" {
				if _, err := e.appendSessionTitle(session, title, []int{first.Seq}, SessionTitleSource{Kind: "fallback"}); err != nil {
					return SessionTitleSnapshot{}, false, err
				}
			}
		} else if _, _, err := e.ensureSessionTitleFallback(session); err != nil {
			return SessionTitleSnapshot{}, false, err
		}
		return e.SessionTitle(id)
	}
	state := e.sessionTitleStateLocked(id)
	revision := e.supersedeSessionTitleStateLocked(state)
	latest := messages[len(messages)-1]
	work := e.activateSessionTitleLocked(ctx, registration, state, revision, latest.Seq, latestSessionTitleRoute(events))
	e.titleMu.Unlock()
	return e.runSessionTitleProvider(session, work)
}

func (e *Engine) observeSessionTitleEvent(session *Session, event Event) {
	switch event.Type {
	case "user/message":
		e.observeSessionTitleUserMessage(session, event)
	case "request/header":
		if route, ok := sessionTitleRouteFromHeader(event); ok {
			e.startPendingSessionTitle(session, route)
		}
	}
}

func (e *Engine) observeSessionTitleUserMessage(session *Session, event Event) {
	if len(collectSessionTitleMessages([]Event{event}, nil)) == 0 {
		return
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	parent := session.Header.ParentSession
	session.mu.Unlock()
	current, hasCurrent := FoldSessionTitle(events)
	if hasCurrent && current.Source.Kind == "user" {
		return
	}
	messages := collectSessionTitleMessages(events, &event.Seq)

	e.titleMu.Lock()
	registration := e.titleProvider
	if registration != nil && !registration.closing {
		shouldSchedule := registration.provider.Automatic() == SessionTitleAllPrompts ||
			(parent == "" && len(messages) == 1 && !hasCurrent)
		if shouldSchedule {
			state := e.sessionTitleStateLocked(session.Header.ID)
			revision := e.supersedeSessionTitleStateLocked(state)
			state.pending = &sessionTitlePendingWork{registration: registration, revision: revision, throughSeq: event.Seq}
		}
	}
	e.titleMu.Unlock()
	_, _, _ = e.ensureSessionTitleFallback(session)
}

func (e *Engine) startPendingSessionTitle(session *Session, route SessionTitleModelProvenance) {
	if route.Provider == "" || route.Model == "" {
		return
	}
	e.titleMu.Lock()
	state := e.titleWork[session.Header.ID]
	if state == nil || state.pending == nil {
		e.titleMu.Unlock()
		return
	}
	pending := state.pending
	if pending.registration != e.titleProvider || pending.registration.closing || pending.revision != state.revision {
		state.pending = nil
		e.titleMu.Unlock()
		return
	}
	state.pending = nil
	work := e.activateSessionTitleLocked(e.titleCtx, pending.registration, state, pending.revision, pending.throughSeq, &route)
	e.titleMu.Unlock()
	go func() {
		_, _, _ = e.runSessionTitleProvider(session, work)
	}()
}

func (e *Engine) activateSessionTitleLocked(ctx context.Context, registration *sessionTitleProviderRegistration, state *sessionTitleWorkState, revision uint64, throughSeq int, route *SessionTitleModelProvenance) *sessionTitleActiveWork {
	workCtx, cancel := context.WithCancel(ctx)
	work := &sessionTitleActiveWork{
		sessionTitlePendingWork: sessionTitlePendingWork{registration: registration, revision: revision, throughSeq: throughSeq},
		ctx:                     workCtx, cancel: cancel, route: route,
	}
	state.active = work
	registration.active.Add(1)
	return work
}

func (e *Engine) runSessionTitleProvider(session *Session, work *sessionTitleActiveWork) (SessionTitleSnapshot, bool, error) {
	defer work.registration.active.Done()
	defer func() {
		e.titleMu.Lock()
		if state := e.titleWork[session.Header.ID]; state != nil && state.active == work {
			state.active = nil
		}
		e.titleMu.Unlock()
	}()
	if err := work.ctx.Err(); err != nil {
		return SessionTitleSnapshot{}, false, err
	}
	if _, _, err := e.ensureSessionTitleFallback(session); err != nil {
		return SessionTitleSnapshot{}, false, err
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	messages := collectSessionTitleMessages(events, &work.throughSeq)
	request := SessionTitleProviderRequest{Session: session, Messages: append([]SessionTitleUserMessage(nil), messages...)}
	if work.route != nil {
		route := *work.route
		request.Route = &route
	}
	result, err := work.registration.provider.Generate(work.ctx, request)
	if err != nil {
		return SessionTitleSnapshot{}, false, err
	}
	if err := work.ctx.Err(); err != nil {
		return SessionTitleSnapshot{}, false, err
	}
	result, err = e.validateSessionTitleProviderResult(result, messages)
	if err != nil {
		return SessionTitleSnapshot{}, false, err
	}

	e.titleMu.Lock()
	state := e.titleWork[session.Header.ID]
	if work.registration != e.titleProvider || work.registration.closing || state == nil || state.active != work || state.revision != work.revision {
		e.titleMu.Unlock()
		return SessionTitleSnapshot{}, false, context.Canceled
	}
	source := SessionTitleSource{Kind: "provider", Provider: work.registration.provider.ID(), Model: result.Model}
	_, err = e.appendSessionTitle(session, result.Title, result.MessageSeqs, source)
	e.titleMu.Unlock()
	if err != nil {
		return SessionTitleSnapshot{}, false, err
	}
	return e.SessionTitle(session.Header.ID)
}

func (e *Engine) validateSessionTitleProviderResult(result SessionTitleProviderResult, messages []SessionTitleUserMessage) (SessionTitleProviderResult, error) {
	result.Title = NormalizeSessionTitle(result.Title, e.cfg.SessionTitle.MaxTitleBytes)
	if result.Title == "" {
		return SessionTitleProviderResult{}, errors.New("session-title provider returned an empty title")
	}
	if len(result.MessageSeqs) == 0 {
		return SessionTitleProviderResult{}, errors.New("session-title provider must identify at least one source message seq")
	}
	order := make(map[int]int, len(messages))
	for index, message := range messages {
		order[message.Seq] = index
	}
	previous := -1
	for _, seq := range result.MessageSeqs {
		index, ok := order[seq]
		if !ok || seq < 0 || int64(seq) > maxJSONSafeInteger || index <= previous {
			return SessionTitleProviderResult{}, errors.New("session-title provider message seqs must be unique ordered seqs from the request")
		}
		previous = index
	}
	result.MessageSeqs = append([]int(nil), result.MessageSeqs...)
	if result.Model != nil {
		if result.Model.Provider == "" || result.Model.Model == "" {
			return SessionTitleProviderResult{}, errors.New("session-title provider model must contain provider and model")
		}
		model := *result.Model
		result.Model = &model
	}
	return result, nil
}

func (e *Engine) ensureSessionTitleFallback(session *Session) (SessionTitleSnapshot, bool, error) {
	session.mu.Lock()
	if current, ok := FoldSessionTitle(session.Events); ok {
		session.mu.Unlock()
		return current, true, nil
	}
	messages := collectSessionTitleMessages(session.Events, nil)
	if len(messages) == 0 {
		session.mu.Unlock()
		return SessionTitleSnapshot{}, false, nil
	}
	first := messages[0]
	title := FallbackSessionTitle(first.Text, e.cfg.SessionTitle.FallbackMaxWords, e.cfg.SessionTitle.FallbackMaxBytes)
	if title == "" {
		session.mu.Unlock()
		return SessionTitleSnapshot{}, false, nil
	}
	event, err := appendEventLocked(session, "session/title", map[string]any{
		"title": title, "messageSeqs": []int{first.Seq}, "source": map[string]any{"kind": "fallback"},
	}, nil, nil, false)
	if err == nil {
		session.Title = title
	}
	id := session.Header.ID
	session.mu.Unlock()
	if err != nil {
		return SessionTitleSnapshot{}, false, err
	}
	e.publishEvent(id, event)
	return SessionTitleSnapshot{Title: title, MessageSeqs: []int{first.Seq}, Source: SessionTitleSource{Kind: "fallback"}, EventSeq: event.Seq, UpdatedAt: event.Time}, true, nil
}

func (e *Engine) appendSessionTitle(session *Session, title string, messageSeqs []int, source SessionTitleSource) (Event, error) {
	sourceValue := map[string]any{"kind": source.Kind}
	if source.Provider != "" {
		sourceValue["provider"] = source.Provider
	}
	if source.Model != nil {
		sourceValue["model"] = map[string]any{"provider": source.Model.Provider, "model": source.Model.Model}
	}
	session.mu.Lock()
	event, err := appendEventLocked(session, "session/title", map[string]any{
		"title": title, "messageSeqs": append([]int(nil), messageSeqs...), "source": sourceValue,
	}, nil, nil, false)
	if err == nil {
		session.Title = title
	}
	id := session.Header.ID
	session.mu.Unlock()
	if err == nil {
		e.publishEvent(id, event)
	}
	return event, err
}

func (e *Engine) sessionTitleStateLocked(id string) *sessionTitleWorkState {
	state := e.titleWork[id]
	if state == nil {
		state = &sessionTitleWorkState{}
		e.titleWork[id] = state
	}
	return state
}

func (e *Engine) supersedeSessionTitleLocked(id string) uint64 {
	return e.supersedeSessionTitleStateLocked(e.sessionTitleStateLocked(id))
}

func (e *Engine) supersedeSessionTitleStateLocked(state *sessionTitleWorkState) uint64 {
	if state.active != nil {
		state.active.cancel()
	}
	state.pending = nil
	state.revision++
	return state.revision
}

func sessionTitleRouteFromHeader(event Event) (SessionTitleModelProvenance, bool) {
	if event.Type != "request/header" {
		return SessionTitleModelProvenance{}, false
	}
	var data struct {
		Header struct {
			Config SessionTitleModelProvenance `json:"config"`
		} `json:"header"`
	}
	encoded, err := json.Marshal(event.Data)
	if err != nil || json.Unmarshal(encoded, &data) != nil || data.Header.Config.Provider == "" || data.Header.Config.Model == "" {
		return SessionTitleModelProvenance{}, false
	}
	return data.Header.Config, true
}

func latestSessionTitleRoute(events []Event) *SessionTitleModelProvenance {
	for index := len(events) - 1; index >= 0; index-- {
		if route, ok := sessionTitleRouteFromHeader(events[index]); ok {
			return &route
		}
	}
	return nil
}

func (e *Engine) closeSessionTitles() {
	if e.titleCancel != nil {
		e.titleCancel()
	}
	e.titleMu.Lock()
	registration := e.titleProvider
	if registration != nil {
		registration.closing = true
	}
	for _, state := range e.titleWork {
		state.pending = nil
		if state.active != nil {
			state.active.cancel()
		}
	}
	e.titleMu.Unlock()
	if registration != nil {
		registration.active.Wait()
	}
	e.titleMu.Lock()
	e.titleProvider = nil
	e.titleWork = map[string]*sessionTitleWorkState{}
	e.titleMu.Unlock()
}

type llmSessionTitleProvider struct {
	engine *Engine
	config SessionTitleLLMConfig
}

func (provider *llmSessionTitleProvider) ID() string { return "session-title-first-prompt-llm" }
func (provider *llmSessionTitleProvider) Automatic() SessionTitleAutomaticMode {
	return provider.config.Automatic
}

func (provider *llmSessionTitleProvider) Generate(ctx context.Context, request SessionTitleProviderRequest) (SessionTitleProviderResult, error) {
	messages := request.Messages
	if provider.config.Automatic == SessionTitleFirstPrompt && len(messages) > 0 {
		messages = messages[:1]
	}
	if len(messages) == 0 {
		return SessionTitleProviderResult{}, errors.New("session-title-llm: at least one source message is required")
	}
	route := request.Route
	if provider.config.Provider != "" {
		route = &SessionTitleModelProvenance{Provider: provider.config.Provider, Model: provider.config.Model}
	}
	if route == nil || route.Provider == "" || route.Model == "" {
		return SessionTitleProviderResult{}, errors.New("session-title-llm: no logged request route is available")
	}
	encodedMessages, err := json.Marshal(messages)
	if err != nil {
		return SessionTitleProviderResult{}, err
	}
	framed := "Generate the session title from this JSON array of human messages:\n" + string(encodedMessages)
	if len([]byte(framed)) > provider.config.MaxInputBytes {
		return SessionTitleProviderResult{}, fmt.Errorf("session-title-llm: input is %d bytes, exceeding max input bytes %d", len([]byte(framed)), provider.config.MaxInputBytes)
	}
	system := strings.Join([]string{
		"Create a concise title for an AI coding-assistant session from the supplied human messages.",
		"Return only the title on one line, **in plain text of natural language**, with no quotes, prefix, explanation, Markdown, XML, or terminal control codes. No code is allowed.",
		"Use the language of the messages.",
		fmt.Sprintf("Aim for about %d words in non-CJK languages or %d CJK characters.", provider.config.TargetWords, provider.config.TargetCJKCharacters),
	}, "\n")
	messageSeqs := make([]int, len(messages))
	for index, message := range messages {
		messageSeqs[index] = message.Seq
	}
	if _, err := provider.engine.appendEvent(request.Session, "session/title-llm-request", map[string]any{
		"titleProvider": provider.ID(),
		"messageSeqs":   messageSeqs,
		"route":         map[string]any{"provider": route.Provider, "model": route.Model},
		"system":        system,
		"messages": []any{map[string]any{
			"role": "user", "content": []ContentBlock{{Type: "text", Text: framed}},
			"source": map[string]any{"kind": "plugin", "plugin": "dsh-session-title-llm"},
		}},
		"maxTokens": provider.config.MaxOutputTokens,
	}); err != nil {
		return SessionTitleProviderResult{}, err
	}
	provider.engine.mu.RLock()
	modelProvider := provider.engine.providers[route.Provider]
	provider.engine.mu.RUnlock()
	if modelProvider == nil {
		return SessionTitleProviderResult{}, fmt.Errorf("model-unavailable: %s/%s", route.Provider, route.Model)
	}
	callCtx, cancel := context.WithTimeout(ctx, provider.config.Timeout)
	defer cancel()
	var text, finish string
	completion, err := modelProvider.Complete(callCtx, ChatRequest{
		SessionID: request.Session.Header.ID, Model: route.Model, System: system, Messages: []ChatMessage{{Role: "user", Content: framed}},
		Purpose: "session-title", Thinking: "disabled", MaxTokens: provider.config.MaxOutputTokens,
	}, func(delta Delta) error {
		if len(delta.ToolCalls) > 0 {
			return errors.New("session-title-llm: title model unexpectedly requested a tool")
		}
		text += delta.Text
		if delta.Finish != "" {
			finish = delta.Finish
		}
		return nil
	})
	if err != nil {
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return SessionTitleProviderResult{}, fmt.Errorf("SESSION_TITLE_TIMEOUT: %w", callCtx.Err())
		}
		return SessionTitleProviderResult{}, err
	}
	if err := callCtx.Err(); err != nil {
		return SessionTitleProviderResult{}, err
	}
	if len(completion.ToolCalls) > 0 {
		return SessionTitleProviderResult{}, errors.New("session-title-llm: title model unexpectedly requested a tool")
	}
	if completion.Text != "" {
		text = completion.Text
	}
	if completion.Finish != "" {
		finish = completion.Finish
	}
	if finish == "" {
		finish = "stop"
	}
	if finish == "length" {
		return SessionTitleProviderResult{}, errors.New("session-title-llm: title output reached max output tokens")
	}
	if finish != "stop" {
		return SessionTitleProviderResult{}, fmt.Errorf("session-title-llm: unsupported finish reason %q", finish)
	}
	title := NormalizeSessionTitle(text, int(^uint(0)>>1))
	if title == "" {
		return SessionTitleProviderResult{}, errors.New("session-title-llm: title model produced no text")
	}
	model := *route
	return SessionTitleProviderResult{Title: title, MessageSeqs: messageSeqs, Model: &model}, nil
}
