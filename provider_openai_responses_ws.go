package harness

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

const (
	openAIResponsesWebSocketBeta        = "responses_websockets=2026-02-06"
	openAIResponsesWebSocketIdleTTL     = 5 * time.Minute
	openAIResponsesWebSocketMaxAge      = 55 * time.Minute
	openAIResponsesWebSocketDialTimeout = 15 * time.Second
)

type openAIResponsesWebSocketContinuation struct {
	lastRequestBody   map[string]any
	lastResponseID    string
	lastResponseItems []any
}

type openAIResponsesWebSocketEntry struct {
	conn         *websocket.Conn
	busy         bool
	createdAt    time.Time
	idleTimer    *time.Timer
	continuation *openAIResponsesWebSocketContinuation
}

type openAIResponsesWebSocketPool struct {
	mu       sync.Mutex
	sessions map[string]*openAIResponsesWebSocketEntry
}

type openAIResponsesWebSocketLease struct {
	pool      *openAIResponsesWebSocketPool
	key       string
	conn      *websocket.Conn
	entry     *openAIResponsesWebSocketEntry
	ephemeral bool
}

func newOpenAIResponsesWebSocketPool() *openAIResponsesWebSocketPool {
	return &openAIResponsesWebSocketPool{sessions: map[string]*openAIResponsesWebSocketEntry{}}
}

func (p *openAIResponsesWebSocketPool) acquire(ctx context.Context, endpoint, origin string, headers http.Header, key string, timeout time.Duration, timeoutSet bool) (*openAIResponsesWebSocketLease, error) {
	if key != "" {
		p.mu.Lock()
		entry := p.sessions[key]
		if entry != nil && entry.idleTimer != nil {
			entry.idleTimer.Stop()
			entry.idleTimer = nil
		}
		if entry != nil && !entry.busy && time.Since(entry.createdAt) < openAIResponsesWebSocketMaxAge {
			entry.busy = true
			p.mu.Unlock()
			return &openAIResponsesWebSocketLease{pool: p, key: key, conn: entry.conn, entry: entry}, nil
		}
		if entry != nil && !entry.busy {
			delete(p.sessions, key)
		}
		busy := entry != nil && entry.busy
		p.mu.Unlock()
		if entry != nil && !busy {
			_ = entry.conn.Close()
		}
		if busy {
			conn, err := dialOpenAIResponsesWebSocket(ctx, endpoint, origin, headers, timeout, timeoutSet)
			if err != nil {
				return nil, err
			}
			return &openAIResponsesWebSocketLease{conn: conn, ephemeral: true}, nil
		}
	}

	conn, err := dialOpenAIResponsesWebSocket(ctx, endpoint, origin, headers, timeout, timeoutSet)
	if err != nil {
		return nil, err
	}
	if key == "" {
		return &openAIResponsesWebSocketLease{conn: conn, ephemeral: true}, nil
	}

	entry := &openAIResponsesWebSocketEntry{conn: conn, busy: true, createdAt: time.Now()}
	p.mu.Lock()
	if current := p.sessions[key]; current == nil {
		p.sessions[key] = entry
		p.mu.Unlock()
		return &openAIResponsesWebSocketLease{pool: p, key: key, conn: conn, entry: entry}, nil
	}
	p.mu.Unlock()
	return &openAIResponsesWebSocketLease{conn: conn, ephemeral: true}, nil
}

func (l *openAIResponsesWebSocketLease) release(keep bool) {
	_ = l.conn.SetDeadline(time.Time{})
	if l.ephemeral || l.entry == nil || l.pool == nil {
		_ = l.conn.Close()
		return
	}

	closeConnection := !keep
	l.pool.mu.Lock()
	if l.pool.sessions[l.key] != l.entry {
		closeConnection = true
	} else if !keep {
		delete(l.pool.sessions, l.key)
	} else {
		l.entry.busy = false
		entry := l.entry
		entry.idleTimer = time.AfterFunc(openAIResponsesWebSocketIdleTTL, func() {
			shouldClose := false
			l.pool.mu.Lock()
			if l.pool.sessions[l.key] == entry && !entry.busy {
				delete(l.pool.sessions, l.key)
				shouldClose = true
			}
			l.pool.mu.Unlock()
			if shouldClose {
				_ = entry.conn.Close()
			}
		})
	}
	l.pool.mu.Unlock()
	if closeConnection {
		_ = l.conn.Close()
	}
}

func (p *openAIResponsesWebSocketPool) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	entries := make([]*openAIResponsesWebSocketEntry, 0, len(p.sessions))
	for _, entry := range p.sessions {
		if entry.idleTimer != nil {
			entry.idleTimer.Stop()
		}
		entries = append(entries, entry)
	}
	p.sessions = map[string]*openAIResponsesWebSocketEntry{}
	p.mu.Unlock()
	for _, entry := range entries {
		_ = entry.conn.Close()
	}
}

func dialOpenAIResponsesWebSocket(ctx context.Context, endpoint, origin string, headers http.Header, timeout time.Duration, timeoutSet bool) (*websocket.Conn, error) {
	config, err := websocket.NewConfig(endpoint, origin)
	if err != nil {
		return nil, err
	}
	config.Header = headers.Clone()
	if !timeoutSet {
		timeout = openAIResponsesWebSocketDialTimeout
	}
	dialCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	conn, err := config.DialContext(dialCtx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &ProviderError{Code: "TRANSPORT", Message: "OpenAI Responses WebSocket connect failed: " + err.Error(), Err: err}
	}
	return conn, nil
}

func openAIResponsesWebSocketEndpoint(baseURL string) (string, string, error) {
	return responsesWebSocketEndpoint(baseURL + "/responses")
}

func responsesWebSocketEndpoint(rawEndpoint string) (string, string, error) {
	endpoint, err := url.Parse(rawEndpoint)
	if err != nil {
		return "", "", err
	}
	origin := *endpoint
	origin.Path, origin.RawPath, origin.RawQuery, origin.Fragment = "", "", "", ""
	switch endpoint.Scheme {
	case "http":
		endpoint.Scheme = "ws"
	case "https":
		endpoint.Scheme = "wss"
	default:
		return "", "", fmt.Errorf("OpenAI Responses WebSocket requires an HTTP base URL")
	}
	return endpoint.String(), origin.String(), nil
}

func openAIResponsesWebSocketCacheKey(sessionID, endpoint string, headers http.Header) string {
	if sessionID == "" {
		return ""
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(endpoint))
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		_, _ = hash.Write([]byte{'\n'})
		_, _ = hash.Write([]byte(name))
		values := append([]string(nil), headers.Values(name)...)
		sort.Strings(values)
		for _, value := range values {
			_, _ = hash.Write([]byte{'\x00'})
			_, _ = hash.Write([]byte(value))
		}
	}
	return sessionID + "\x00" + fmt.Sprintf("%x", hash.Sum(nil))
}

func openAIResponsesRequestBodyWithoutInput(body map[string]any) map[string]any {
	copy := make(map[string]any, len(body))
	for key, value := range body {
		if key != "input" && key != "previous_response_id" {
			copy[key] = value
		}
	}
	return copy
}

func openAIResponsesCachedRequestBody(entry *openAIResponsesWebSocketEntry, body map[string]any) map[string]any {
	continuation := entry.continuation
	if continuation == nil || continuation.lastResponseID == "" || !reflect.DeepEqual(openAIResponsesRequestBodyWithoutInput(body), openAIResponsesRequestBodyWithoutInput(continuation.lastRequestBody)) {
		entry.continuation = nil
		return body
	}
	current, _ := body["input"].([]any)
	previous, _ := continuation.lastRequestBody["input"].([]any)
	baseline := append(append([]any(nil), previous...), continuation.lastResponseItems...)
	if len(current) < len(baseline) || !reflect.DeepEqual(current[:len(baseline)], baseline) {
		entry.continuation = nil
		return body
	}
	request := make(map[string]any, len(body)+1)
	for key, value := range body {
		request[key] = value
	}
	request["previous_response_id"] = continuation.lastResponseID
	request["input"] = append([]any(nil), current[len(baseline):]...)
	return request
}

func (s *openAIResponsesState) responseItems() []any {
	indices := make([]int, 0, len(s.outputItems))
	for index := range s.outputItems {
		indices = append(indices, index)
	}
	sort.Ints(indices)
	items := make([]any, 0, len(indices))
	for _, index := range indices {
		items = append(items, s.outputItems[index])
	}
	return items
}

func recoverableOpenAIResponsesWebSocketError(payload []byte) bool {
	var event struct {
		Type  string `json:"type"`
		Code  string `json:"code"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
		Response struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		} `json:"response"`
	}
	if json.Unmarshal(payload, &event) != nil || event.Type != "error" && event.Type != "response.failed" {
		return false
	}
	code := event.Code
	if code == "" {
		code = event.Error.Code
	}
	if code == "" {
		code = event.Response.Error.Code
	}
	return code == "previous_response_not_found" || code == "websocket_connection_limit_reached"
}

func (p *OpenAIResponsesProvider) completeWebSocket(ctx context.Context, req ChatRequest, fullBody map[string]any, onDelta func(Delta) error) (completion Completion, started bool, err error) {
	headers := make(http.Header)
	headerRequest := &http.Request{Header: headers}
	p.applyRequestHeaders(headerRequest, req)
	if p.apiKey != "" {
		headers.Set("Authorization", "Bearer "+p.apiKey)
	}
	headers.Set("OpenAI-Beta", openAIResponsesWebSocketBeta)
	return p.completeWebSocketPrepared(ctx, req, fullBody, p.baseURL+"/responses", headers, onDelta)
}

func (p *OpenAIResponsesProvider) completeWebSocketPrepared(ctx context.Context, req ChatRequest, fullBody map[string]any, endpoint string, headers http.Header, onDelta func(Delta) error) (completion Completion, started bool, err error) {
	endpoint, origin, err := responsesWebSocketEndpoint(endpoint)
	if err != nil {
		return Completion{}, false, err
	}

	cacheKey := ""
	if resolvedPiAICacheRetention(p.cacheRetention) != "none" {
		cacheKey = openAIResponsesWebSocketCacheKey(req.SessionID, endpoint, headers)
	}
	lease, err := p.websockets.acquire(ctx, endpoint, origin, headers, cacheKey, p.websocketConnectTimeout, p.websocketConnectTimeoutSet)
	if err != nil {
		return Completion{}, false, err
	}
	keep := false
	defer func() { lease.release(keep) }()

	useCachedContext := p.transport == "auto" || p.transport == "websocket-cached"
	body := fullBody
	if useCachedContext && lease.entry != nil {
		body = openAIResponsesCachedRequestBody(lease.entry, fullBody)
	} else if lease.entry != nil {
		lease.entry.continuation = nil
	}
	frame := make(map[string]any, len(body)+1)
	for key, value := range body {
		frame[key] = value
	}
	frame["type"] = "response.create"
	payload, err := json.Marshal(frame)
	if err != nil {
		return Completion{}, false, err
	}

	requestDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = lease.conn.Close()
		case <-requestDone:
		}
	}()
	defer close(requestDone)
	if deadline, ok := ctx.Deadline(); ok {
		_ = lease.conn.SetWriteDeadline(deadline)
	}
	if err := websocket.Message.Send(lease.conn, string(payload)); err != nil {
		if ctx.Err() != nil {
			return Completion{}, false, ctx.Err()
		}
		return Completion{}, false, &ProviderError{Code: "TRANSPORT", Message: "OpenAI Responses WebSocket write failed: " + err.Error(), Err: err}
	}

	state := newOpenAIResponsesState(onDelta)
	for !state.terminal {
		deadline := time.Time{}
		if p.streamIdleTimeout > 0 {
			deadline = time.Now().Add(p.streamIdleTimeout)
		}
		if contextDeadline, ok := ctx.Deadline(); ok && (deadline.IsZero() || contextDeadline.Before(deadline)) {
			deadline = contextDeadline
		}
		_ = lease.conn.SetReadDeadline(deadline)
		var message string
		if receiveErr := websocket.Message.Receive(lease.conn, &message); receiveErr != nil {
			if ctx.Err() != nil {
				return Completion{}, started, ctx.Err()
			}
			return Completion{}, started, &ProviderError{Code: "TRANSPORT", Message: "OpenAI Responses WebSocket read failed: " + receiveErr.Error(), Err: receiveErr}
		}
		if message == "" {
			continue
		}
		if !json.Valid([]byte(message)) {
			return Completion{}, started, fmt.Errorf("malformed Responses WebSocket payload")
		}
		if recoverableOpenAIResponsesWebSocketError([]byte(message)) {
			if lease.entry != nil {
				lease.entry.continuation = nil
			}
			return Completion{}, false, &ProviderError{Code: "TRANSPORT", Message: "OpenAI Responses WebSocket request could not start"}
		}
		started = true
		if err := state.handleJSON([]byte(message), "WebSocket"); err != nil {
			return Completion{}, started, err
		}
	}

	completion, err = state.completion()
	if err != nil {
		return Completion{}, started, err
	}
	if useCachedContext && lease.entry != nil && state.responseID != "" {
		lease.entry.continuation = &openAIResponsesWebSocketContinuation{
			lastRequestBody: fullBody, lastResponseID: state.responseID, lastResponseItems: state.responseItems(),
		}
	}
	keep = ctx.Err() == nil
	return completion, started, nil
}
