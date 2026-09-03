package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	remoteStreamHeartbeatInterval = 2 * time.Second
	remoteStreamMaxMissedPongs    = 2
)

type remoteStreamClientMessage struct {
	typ      string
	streamID string
	endpoint string
	payload  json.RawMessage
}

type remoteStreamState struct {
	cancel context.CancelFunc
}

type remoteStreamMuxConnection struct {
	engine *Engine
	ws     *wsConn
	ctx    context.Context
	cancel context.CancelFunc

	mu             sync.Mutex
	streams        map[string]*remoteStreamState
	missedPongs    int
	graceScheduled bool
	closeOnce      sync.Once
	wg             sync.WaitGroup
}

func (e *Engine) handleRemoteStreamMux(w http.ResponseWriter, r *http.Request) {
	ws, ok := acceptWebSocket(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	connection := &remoteStreamMuxConnection{
		engine: e, ws: ws, ctx: ctx, cancel: cancel,
		streams: map[string]*remoteStreamState{},
	}
	connection.run(remoteStreamHeartbeatInterval)
}

func (c *remoteStreamMuxConnection) run(heartbeatInterval time.Duration) {
	defer func() {
		c.shutdown(0, "")
		c.wg.Wait()
	}()
	go func() {
		<-c.ctx.Done()
		_ = c.ws.Close()
	}()
	if heartbeatInterval > 0 {
		go c.heartbeat(heartbeatInterval)
	}
	for {
		opcode, payload, err := readWSFrame(c.ws.reader)
		if err != nil {
			var protocol *wsProtocolError
			if errors.As(err, &protocol) {
				c.shutdown(protocol.code, protocol.reason)
			}
			return
		}
		switch opcode {
		case wsPing:
			if err := c.ws.writeFrame(wsPong, payload); err != nil {
				return
			}
		case wsPong:
			c.recordPong()
		case wsClose:
			_ = c.ws.writeFrame(wsClose, payload)
			return
		case wsBinary:
			c.shutdown(1003, "text messages required")
			return
		case wsText:
			if !utf8.Valid(payload) {
				c.shutdown(1008, "invalid Remote stream request")
				return
			}
			message, parseErr := parseRemoteStreamClientMessage(payload)
			if parseErr != nil || !c.receive(message) {
				c.shutdown(1008, "invalid Remote stream request")
				return
			}
		default:
			c.shutdown(1008, "invalid Remote stream request")
			return
		}
	}
}

func (c *remoteStreamMuxConnection) receive(message remoteStreamClientMessage) bool {
	if message.typ == "cancel" {
		c.mu.Lock()
		stream := c.streams[message.streamID]
		c.mu.Unlock()
		if stream != nil {
			stream.cancel()
		}
		return true
	}
	streamCtx, cancel := context.WithCancel(c.ctx)
	stream := &remoteStreamState{cancel: cancel}
	c.mu.Lock()
	if c.streams[message.streamID] != nil {
		c.mu.Unlock()
		cancel()
		return false
	}
	c.streams[message.streamID] = stream
	c.mu.Unlock()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()
		defer func() {
			c.mu.Lock()
			if c.streams[message.streamID] == stream {
				delete(c.streams, message.streamID)
			}
			c.mu.Unlock()
		}()
		rpcErr := c.engine.runRemoteStream(streamCtx, message.endpoint, message.payload, func(value any) error {
			return c.send(map[string]any{"type": "item", "streamId": message.streamID, "value": value})
		})
		if streamCtx.Err() != nil {
			return
		}
		if rpcErr != nil {
			if err := c.send(map[string]any{
				"type": "error", "streamId": message.streamID, "error": normalizeRPCError(rpcErr),
			}); err != nil {
				c.shutdown(1011, "Remote stream failure could not be delivered")
			}
			return
		}
		if err := c.send(map[string]any{"type": "end", "streamId": message.streamID}); err != nil {
			c.shutdown(0, "")
		}
	}()
	return true
}

func (c *remoteStreamMuxConnection) send(value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.ws.writeFrame(wsText, data)
}

func (c *remoteStreamMuxConnection) shutdown(code uint16, reason string) {
	c.closeOnce.Do(func() {
		if code != 0 {
			_ = c.ws.writeFrame(wsClose, closePayload(code, reason))
		}
		c.cancel()
		c.mu.Lock()
		streams := make([]*remoteStreamState, 0, len(c.streams))
		for _, stream := range c.streams {
			streams = append(streams, stream)
		}
		c.mu.Unlock()
		for _, stream := range streams {
			stream.cancel()
		}
		_ = c.ws.Close()
	})
}

func (c *remoteStreamMuxConnection) heartbeat(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if !c.heartbeatTick() {
				continue
			}
			if err := c.ws.writeFrame(wsPing, nil); err != nil {
				c.shutdown(0, "")
				return
			}
		}
	}
}

func (c *remoteStreamMuxConnection) recordPong() {
	c.mu.Lock()
	c.missedPongs = 0
	c.mu.Unlock()
}

func (c *remoteStreamMuxConnection) heartbeatTick() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.missedPongs < remoteStreamMaxMissedPongs {
		c.missedPongs++
		return true
	}
	if c.graceScheduled {
		return false
	}
	c.graceScheduled = true
	time.AfterFunc(time.Millisecond, func() {
		c.mu.Lock()
		closeNow := c.missedPongs >= remoteStreamMaxMissedPongs
		c.graceScheduled = false
		c.mu.Unlock()
		if closeNow {
			c.shutdown(0, "")
		}
	})
	return false
}

func parseRemoteStreamClientMessage(raw json.RawMessage) (remoteStreamClientMessage, error) {
	object, err := remoteObject(raw, nil, nil)
	if err != nil {
		return remoteStreamClientMessage{}, err
	}
	typ, ok := nonEmptyRemoteString(object["type"])
	if !ok {
		return remoteStreamClientMessage{}, errors.New("message type must be a non-empty string")
	}
	switch typ {
	case "cancel":
		object, err = remoteObject(raw, []string{"type", "streamId"}, []string{"type", "streamId"})
		if err != nil {
			return remoteStreamClientMessage{}, err
		}
		streamID, ok := nonEmptyRemoteString(object["streamId"])
		if !ok {
			return remoteStreamClientMessage{}, errors.New("streamId must be a non-empty string")
		}
		return remoteStreamClientMessage{typ: typ, streamID: streamID}, nil
	case "open":
		object, err = remoteObject(raw, []string{"type", "streamId", "endpoint", "payload"}, []string{"type", "streamId", "endpoint", "payload"})
		if err != nil {
			return remoteStreamClientMessage{}, err
		}
		streamID, streamOK := nonEmptyRemoteString(object["streamId"])
		endpoint, endpointOK := nonEmptyRemoteString(object["endpoint"])
		if !streamOK || !endpointOK {
			return remoteStreamClientMessage{}, errors.New("streamId and endpoint must be non-empty strings")
		}
		return remoteStreamClientMessage{
			typ: typ, streamID: streamID, endpoint: endpoint,
			payload: append(json.RawMessage(nil), object["payload"]...),
		}, nil
	default:
		return remoteStreamClientMessage{}, errors.New("unknown Remote stream message")
	}
}

func (e *Engine) runRemoteStream(
	ctx context.Context,
	endpoint string,
	payload json.RawMessage,
	send func(any) error,
) *RPCError {
	var rpcErr *RPCError
	switch endpoint {
	case "$events":
		rpcErr = e.streamRemoteEvents(ctx, payload, send)
	case "session/follow":
		rpcErr = e.streamSessionFollow(ctx, payload, send)
	case "session/control":
		rpcErr = e.streamSessionControl(ctx, payload, send)
	case "workspace/follow":
		rpcErr = e.streamWorkspaceFollow(ctx, payload, send)
	default:
		rpcErr = rpcError("invocation-unavailable", "no active stream Remote method exports this endpoint", map[string]any{"endpoint": endpoint})
	}
	if ctx.Err() != nil {
		return nil
	}
	return rpcErr
}

func validateEmptyRemoteEventPayload(payload json.RawMessage) *RPCError {
	object, err := remoteObject(payload, []string{"args"}, []string{"args"})
	if err == nil {
		_, err = remoteObject(object["args"], []string{}, nil)
	}
	if err != nil {
		return rpcError("input-invalid", "forwarded Remote event stream requires an empty args object", map[string]any{"endpoint": "$events"})
	}
	return nil
}

func (e *Engine) streamRemoteEvents(ctx context.Context, payload json.RawMessage, send func(any) error) *RPCError {
	if rpcErr := validateEmptyRemoteEventPayload(payload); rpcErr != nil {
		return rpcErr
	}
	host := e.SubscribeHost(ctx)
	mux := e.SubscribeMux(ctx)
	clientID := newID("client")
	e.pendingMu.Lock()
	for {
		if _, exists := e.remoteEventClients[clientID]; !exists {
			break
		}
		clientID = newID("client")
	}
	e.remoteEventClients[clientID] = map[string]struct{}{}
	e.pendingMu.Unlock()
	defer func() {
		e.pendingMu.Lock()
		delete(e.remoteEventClients, clientID)
		e.pendingMu.Unlock()
	}()
	home, _ := os.UserHomeDir()
	if err := send(map[string]any{"type": "ready", "clientId": clientID, "host": map[string]any{"home": home}}); err != nil {
		return rpcError("internal", err.Error(), nil)
	}
	seen := map[string]bool{}
	for _, pending := range e.pendingInteractions() {
		sent, err := e.deliverRemoteInteraction(clientID, pending, send)
		if err != nil {
			return rpcError("internal", err.Error(), nil)
		}
		if sent {
			seen[pending.id] = true
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case frame, ok := <-host:
			if !ok {
				return nil
			}
			switch frame["type"] {
			case "host/remote-event":
				event, _ := frame["event"].(string)
				if event == "" {
					continue
				}
				args, _ := frame["args"].([]any)
				if args == nil {
					args = []any{}
				}
				if err := send(map[string]any{"type": "emit", "event": event, "args": args}); err != nil {
					return rpcError("internal", err.Error(), nil)
				}
			case "host/pending-request":
				id, _ := frame["rpcId"].(string)
				if id == "" || seen[id] {
					continue
				}
				if pending := e.pendingInteraction(id); pending != nil {
					sent, err := e.deliverRemoteInteraction(clientID, pending, send)
					if err != nil {
						return rpcError("internal", err.Error(), nil)
					}
					if sent {
						seen[id] = true
					}
				}
			case "host/remote-event-delivery-removed":
				owner, _ := frame["clientId"].(string)
				id, _ := frame["eventId"].(string)
				if owner == clientID {
					delete(seen, id)
				}
			case "host/pending-cancelled":
				id, _ := frame["rpcId"].(string)
				if !seen[id] {
					continue
				}
				delete(seen, id)
				if err := send(map[string]any{"type": "cancel", "eventId": id}); err != nil {
					return rpcError("internal", err.Error(), nil)
				}
			}
		case frame, ok := <-mux:
			if !ok {
				return nil
			}
			switch frame["type"] {
			case "host/remote-event":
				event, _ := frame["event"].(string)
				if event == "" {
					continue
				}
				args, _ := frame["args"].([]any)
				if args == nil {
					args = []any{}
				}
				if err := send(map[string]any{"type": "emit", "event": event, "args": args}); err != nil {
					return rpcError("internal", err.Error(), nil)
				}
			case "host/pending-request":
				id, _ := frame["rpcId"].(string)
				if id == "" || seen[id] {
					continue
				}
				if pending := e.pendingInteraction(id); pending != nil {
					sent, err := e.deliverRemoteInteraction(clientID, pending, send)
					if err != nil {
						return rpcError("internal", err.Error(), nil)
					}
					if sent {
						seen[id] = true
					}
				}
			}
		}
	}
}

func (e *Engine) deliverRemoteInteraction(clientID string, pending *pendingInteraction, send func(any) error) (bool, error) {
	frame := remoteInteractionFrame(pending)
	if frame == nil {
		return false, nil
	}
	e.pendingMu.Lock()
	deliveries := e.remoteEventClients[clientID]
	if deliveries == nil {
		e.pendingMu.Unlock()
		return false, nil
	}
	deliveries[pending.id] = struct{}{}
	e.pendingMu.Unlock()
	return true, send(frame)
}

func (e *Engine) consumeRemoteEventDelivery(clientID, eventID string) (active, delivered, remaining bool) {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	deliveries, active := e.remoteEventClients[clientID]
	if !active {
		return false, false, false
	}
	if _, delivered = deliveries[eventID]; !delivered {
		return true, false, false
	}
	delete(deliveries, eventID)
	for _, candidate := range e.remoteEventClients {
		if _, remaining = candidate[eventID]; remaining {
			break
		}
	}
	return true, true, remaining
}

func (e *Engine) clearRemoteEventDeliveriesLocked(eventID string) {
	for _, deliveries := range e.remoteEventClients {
		delete(deliveries, eventID)
	}
}

func (e *Engine) pendingInteraction(id string) *pendingInteraction {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	pending := e.pending[id]
	if pending == nil {
		return nil
	}
	copy := *pending
	return &copy
}

func remoteInteractionFrame(pending *pendingInteraction) map[string]any {
	if pending == nil || pending.id == "" || pending.sessionID == "" {
		return nil
	}
	payload, _ := pending.payload.(map[string]any)
	request := map[string]any{}
	event := ""
	switch pending.method {
	case "approval/requested":
		event = "approval/request"
		for _, key := range []string{"toolName", "callId", "reason"} {
			if value, exists := payload[key]; exists {
				request[key] = value
			}
		}
	case "question/requested":
		event = "user-questions/request"
		if questions, exists := payload["questions"]; exists {
			request["questions"] = questions
		}
	default:
		return nil
	}
	return map[string]any{
		"type": "waterfall", "event": event, "eventId": pending.id,
		"agentId": pending.sessionID, "request": request,
	}
}

func (e *Engine) settleRemoteEventResult(
	endpoint string,
	args map[string]json.RawMessage,
) (any, bool, *RPCError) {
	clientID, rpcErr := remoteString(endpoint, args, "clientId")
	if rpcErr != nil {
		return nil, false, rpcErr
	}
	if clientID == "" {
		return nil, false, remoteInputError(endpoint, "clientId", errors.New("must be a non-empty string"))
	}
	eventID, rpcErr := remoteString(endpoint, args, "eventId")
	if rpcErr != nil {
		return nil, false, rpcErr
	}
	if eventID == "" {
		return nil, false, remoteInputError(endpoint, "eventId", errors.New("must be a non-empty string"))
	}
	outcome, err := remoteObject(args["outcome"], nil, nil)
	if err != nil {
		return nil, false, remoteInputError(endpoint, "outcome", err)
	}
	kind, ok := nonEmptyRemoteString(outcome["kind"])
	if !ok {
		return nil, false, remoteInputError(endpoint, "outcome.kind", errors.New("must be a non-empty string"))
	}
	var result interactionResult
	switch kind {
	case "next":
		if _, err := remoteObject(args["outcome"], []string{"kind"}, []string{"kind"}); err != nil {
			return nil, false, remoteInputError(endpoint, "outcome", err)
		}
		result.err = rpcError("invocation-unavailable", "no Remote event listener accepted the interaction", map[string]any{"eventId": eventID})
	case "result":
		if _, err := remoteObject(args["outcome"], []string{"kind", "value"}, []string{"kind"}); err != nil {
			return nil, false, remoteInputError(endpoint, "outcome", err)
		}
		result.ok = true
		if raw, exists := outcome["value"]; exists {
			if err := json.Unmarshal(raw, &result.value); err != nil {
				return nil, false, remoteInputError(endpoint, "outcome.value", err)
			}
		}
	case "rejected":
		if _, err := remoteObject(args["outcome"], []string{"kind", "error"}, []string{"kind", "error"}); err != nil {
			return nil, false, remoteInputError(endpoint, "outcome", err)
		}
		rejection, err := remoteObject(outcome["error"], []string{"name", "message", "code", "details"}, []string{"name", "message"})
		if err != nil {
			return nil, false, remoteInputError(endpoint, "outcome.error", err)
		}
		message, messageOK := nonEmptyRemoteString(rejection["message"])
		_, nameOK := nonEmptyRemoteString(rejection["name"])
		if !messageOK || !nameOK {
			return nil, false, remoteInputError(endpoint, "outcome.error", errors.New("name and message must be non-empty strings"))
		}
		code := "internal"
		if raw, exists := rejection["code"]; exists {
			value, ok := remoteJSON[string](raw)
			if !ok {
				return nil, false, remoteInputError(endpoint, "outcome.error.code", errors.New("must be a string"))
			}
			code = value
		}
		var details any
		if raw, exists := rejection["details"]; exists {
			if err := json.Unmarshal(raw, &details); err != nil {
				return nil, false, remoteInputError(endpoint, "outcome.error.details", err)
			}
		}
		result.err = rpcError(code, message, details)
	default:
		return nil, false, remoteInputError(endpoint, "outcome.kind", fmt.Errorf("unknown value %q", kind))
	}
	active, delivered, remaining := e.consumeRemoteEventDelivery(clientID, eventID)
	if !active {
		return nil, false, rpcError("bad-request", "Remote event result identifies no active event stream", map[string]any{"clientId": clientID})
	}
	if !delivered {
		return nil, false, nil
	}
	e.emitHostRaw(map[string]any{"type": "host/remote-event-delivery-removed", "clientId": clientID, "eventId": eventID})
	if kind == "next" && remaining {
		return nil, false, nil
	}
	e.settleInteraction(eventID, result)
	return nil, false, nil
}

type remoteSessionAddress struct {
	kind     string
	session  string
	parent   string
	child    string
	mode     string
	maxItems int
}

func decodeRemoteSessionFollow(payload json.RawMessage) (remoteSessionAddress, *RPCError) {
	args, rpcErr := remoteArgs("session/follow", payload, remoteDescriptors["session/follow"])
	if rpcErr != nil {
		return remoteSessionAddress{}, rpcErr
	}
	request, err := remoteObject(args["request"], []string{"address", "maxMessages"}, []string{"address"})
	if err != nil {
		return remoteSessionAddress{}, remoteInputError("session/follow", "request", err)
	}
	address, err := remoteObject(request["address"], nil, nil)
	if err != nil {
		return remoteSessionAddress{}, remoteInputError("session/follow", "request.address", err)
	}
	kind, ok := nonEmptyRemoteString(address["kind"])
	if !ok {
		return remoteSessionAddress{}, remoteInputError("session/follow", "request.address.kind", errors.New("must be a non-empty string"))
	}
	result := remoteSessionAddress{kind: kind, maxItems: 50}
	switch kind {
	case "session":
		address, err = remoteObject(request["address"], []string{"kind", "sessionId"}, []string{"kind", "sessionId"})
		if err == nil {
			result.session, ok = nonEmptyRemoteString(address["sessionId"])
		}
	case "subagent":
		address, err = remoteObject(request["address"], []string{"kind", "parentSessionId", "childSessionId", "mode"}, []string{"kind", "parentSessionId", "childSessionId", "mode"})
		if err == nil {
			var parentOK, childOK, modeOK bool
			result.parent, parentOK = nonEmptyRemoteString(address["parentSessionId"])
			result.child, childOK = nonEmptyRemoteString(address["childSessionId"])
			result.mode, modeOK = nonEmptyRemoteString(address["mode"])
			ok = parentOK && childOK && modeOK && (result.mode == "one-shot" || result.mode == "continuable")
		}
	default:
		return remoteSessionAddress{}, remoteInputError("session/follow", "request.address.kind", fmt.Errorf("unknown value %q", kind))
	}
	if err != nil || !ok {
		if err == nil {
			err = errors.New("contains an invalid address field")
		}
		return remoteSessionAddress{}, remoteInputError("session/follow", "request.address", err)
	}
	if raw, exists := request["maxMessages"]; exists {
		var value float64
		if json.Unmarshal(raw, &value) != nil || math.Trunc(value) != value || value <= 0 || value > math.MaxInt {
			return remoteSessionAddress{}, remoteInputError("session/follow", "request.maxMessages", errors.New("must be a positive safe integer"))
		}
		result.maxItems = int(value)
	}
	return result, nil
}

func (e *Engine) remoteAddressSession(address remoteSessionAddress) (*Session, *RPCError) {
	if address.kind == "session" {
		session, rpcErr := e.requireOrdinary(address.session)
		if rpcErr != nil {
			if rpcErr.Code == "session/not-found" {
				rpcErr.Details = map[string]any{"sessionId": address.session}
			}
			return nil, rpcErr
		}
		return session, nil
	}
	child, rpcErr := e.childFor(address.parent, address.child)
	if rpcErr != nil {
		return nil, rpcErr
	}
	child.mu.Lock()
	header := child.Header
	child.mu.Unlock()
	snapshot, err := e.sessionProjections.Snapshot(child)
	if err != nil {
		return nil, rpcError("internal", err.Error(), nil)
	}
	identity, valid := subagentIdentityFromProjection(snapshot.Values["subagent"])
	if !valid {
		return nil, rpcError("subagent/catalog-diagnostic", "subagent descriptor is corrupt", map[string]any{
			"parentSessionId": address.parent, "childSessionId": address.child, "reason": "corrupt",
		})
	}
	if identity == nil || identity.seq < header.SeedLength {
		return nil, rpcError("subagent/catalog-diagnostic", "subagent descriptor is unavailable", map[string]any{
			"parentSessionId": address.parent, "childSessionId": address.child, "reason": "unsupported",
		})
	}
	if identity.mode != address.mode {
		return nil, rpcError("subagent/unauthorized", "subagent mode does not match the supplied address", map[string]any{"childSessionId": address.child})
	}
	return child, nil
}

func (e *Engine) streamSessionFollow(ctx context.Context, payload json.RawMessage, send func(any) error) *RPCError {
	address, rpcErr := decodeRemoteSessionFollow(payload)
	if rpcErr != nil {
		return rpcErr
	}
	session, rpcErr := e.remoteAddressSession(address)
	if rpcErr != nil {
		return rpcErr
	}
	id := address.session
	if address.kind == "subagent" {
		id = address.child
	}
	events := e.Subscribe(ctx, id)
	projection, err := e.sessionProjections.Snapshot(session)
	if err != nil {
		return rpcError("internal", err.Error(), nil)
	}
	session.mu.Lock()
	header := session.Header
	cursor := projection.AsOfSeq
	log := append([]Event(nil), session.Events...)
	if cursor+1 < len(log) {
		log = log[:cursor+1]
	}
	session.mu.Unlock()
	records, hasMore := remoteHistoryRecords(log, address.maxItems)
	if err := send(map[string]any{
		"type": "snapshot", "header": header, "cursor": cursor,
		"records": records, "hasMore": hasMore,
		"projections": map[string]any{"asOfSeq": projection.AsOfSeq, "values": projection.Values},
	}); err != nil {
		return rpcError("internal", err.Error(), nil)
	}
	nextSeq := cursor + 1
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-events:
			if !ok {
				return nil
			}
			if int(event.Seq) < nextSeq {
				continue
			}
			if int(event.Seq) != nextSeq {
				return rpcError("internal", fmt.Sprintf("session event stream skipped seq %d", nextSeq), nil)
			}
			nextSeq++
			if err := send(map[string]any{"type": "event", "event": event}); err != nil {
				return rpcError("internal", err.Error(), nil)
			}
		}
	}
}

func remoteHistoryRecords(events []Event, maxMessages int) ([]map[string]any, bool) {
	cut := 0
	count := 0
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if (event.Type != "user/message" && event.Type != "assistant/message") || !isAppendSurfaceEvent(event) {
			continue
		}
		count++
		groupStart := int(event.Seq)
		for _, source := range event.SourceEventSeqs {
			if source < groupStart {
				groupStart = source
			}
		}
		if count >= maxMessages {
			cut = groupStart
			break
		}
	}
	records := make([]map[string]any, 0, len(events)-cut)
	for _, event := range events {
		if int(event.Seq) >= cut {
			records = append(records, map[string]any{"type": "event", "event": event})
		}
	}
	return records, cut > 0
}

func (e *Engine) streamSessionControl(ctx context.Context, payload json.RawMessage, send func(any) error) *RPCError {
	if _, rpcErr := remoteArgs("session/control", payload, remoteDescriptors["session/control"]); rpcErr != nil {
		return rpcErr
	}
	mux := e.SubscribeMux(ctx)
	jobChanges := make(chan string, 64)
	disposeJobs := e.jobs.onChanged("", func(owner string) {
		select {
		case jobChanges <- owner:
		default:
		}
	})
	defer disposeJobs()
	baseline, rpcErr := e.remoteSessionControlBaseline()
	if rpcErr != nil {
		return rpcErr
	}
	if err := send(map[string]any{"type": "baseline", "value": baseline}); err != nil {
		return rpcError("internal", err.Error(), nil)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case frame, ok := <-mux:
			if !ok {
				return nil
			}
			typ, _ := frame["type"].(string)
			switch typ {
			case "session/queue":
				id, _ := frame["sessionId"].(string)
				if session, err := e.getSession(id); err == nil {
					if err := send(map[string]any{"type": "queue", "sessionId": id, "items": remoteQueueItems(session)}); err != nil {
						return rpcError("internal", err.Error(), nil)
					}
				}
			case "session/projection":
				if err := send(map[string]any{
					"type": "projection", "sessionId": frame["sessionId"], "key": frame["key"],
					"value": frame["value"], "seq": frame["seq"],
				}); err != nil {
					return rpcError("internal", err.Error(), nil)
				}
			}
		case owner := <-jobChanges:
			ids := []string{owner}
			if owner == "" {
				ids = e.sessionIDs()
			}
			for _, id := range ids {
				if id == "" {
					continue
				}
				if err := send(map[string]any{"type": "jobs", "sessionId": id, "jobs": e.remoteJobs(id)}); err != nil {
					return rpcError("internal", err.Error(), nil)
				}
			}
		}
	}
}

func (e *Engine) remoteSessionControlBaseline() (map[string]any, *RPCError) {
	ids := e.sessionIDs()
	queues := map[string]any{}
	jobs := map[string]any{}
	projections := map[string]any{}
	for _, id := range ids {
		session, err := e.getSession(id)
		if err != nil {
			continue
		}
		queues[id] = remoteQueueItems(session)
		jobs[id] = e.remoteJobs(id)
		snapshot, snapshotErr := e.sessionProjections.Snapshot(session)
		if snapshotErr != nil {
			return nil, rpcError("internal", snapshotErr.Error(), nil)
		}
		projections[id] = map[string]any{"asOfSeq": snapshot.AsOfSeq, "values": snapshot.Values}
	}
	return map[string]any{"queues": queues, "jobs": jobs, "projections": projections}, nil
}

func (e *Engine) sessionIDs() []string {
	e.mu.RLock()
	ids := make([]string, 0, len(e.sessions))
	for id := range e.sessions {
		ids = append(ids, id)
	}
	e.mu.RUnlock()
	sort.Strings(ids)
	return ids
}

func remoteQueueItems(session *Session) []map[string]any {
	session.mu.Lock()
	defer session.mu.Unlock()
	items := make([]map[string]any, 0, len(session.pending)+len(session.steering))
	appendItem := func(prompt *queuedPrompt, placement string) {
		item := map[string]any{
			"id": prompt.id, "placement": placement,
			"message": map[string]any{"id": prompt.id, "content": prompt.content},
		}
		if rpcID, ok := prompt.source["rpcId"].(string); ok && rpcID != "" {
			item["rpcId"] = rpcID
		}
		items = append(items, item)
	}
	for _, prompt := range session.pending {
		appendItem(prompt, "queued")
	}
	for _, prompt := range session.steering {
		placement := "context"
		if prompt.source["kind"] == "user" {
			placement = "steering"
		}
		appendItem(prompt, placement)
	}
	return items
}

func (e *Engine) remoteJobs(owner string) []map[string]any {
	snapshots := e.jobs.list(owner)
	jobs := make([]map[string]any, 0, len(snapshots))
	for _, snapshot := range snapshots {
		job := map[string]any{
			"id": snapshot.ID, "kind": snapshot.Kind, "label": snapshot.Label,
			"status": snapshot.Status, "startedAt": snapshot.StartedAt,
		}
		if snapshot.Detail != "" {
			job["detail"] = snapshot.Detail
		}
		if snapshot.FinishedAt != 0 {
			job["finishedAt"] = snapshot.FinishedAt
		}
		jobs = append(jobs, job)
	}
	return jobs
}

func (e *Engine) streamWorkspaceFollow(ctx context.Context, payload json.RawMessage, send func(any) error) *RPCError {
	if _, rpcErr := remoteArgs("workspace/follow", payload, remoteDescriptors["workspace/follow"]); rpcErr != nil {
		return rpcErr
	}
	host := e.SubscribeHost(ctx)
	items, archived := e.ListWorkspaces()
	known := make(map[string]bool, len(items))
	for _, workspace := range items {
		known[workspace.WorkspaceID] = true
	}
	if err := send(map[string]any{
		"type": "baseline", "value": map[string]any{"items": items, "archivedSessionIds": archived},
	}); err != nil {
		return rpcError("internal", err.Error(), nil)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case frame, ok := <-host:
			if !ok {
				return nil
			}
			switch frame["type"] {
			case "host/workspace-changed":
				workspace := frame["workspace"]
				id := remoteWorkspaceID(workspace)
				if id == "" {
					continue
				}
				added := !known[id]
				known[id] = true
				if err := send(map[string]any{"type": "upsert", "workspace": workspace}); err != nil {
					return rpcError("internal", err.Error(), nil)
				}
				if added {
					if err := send(map[string]any{"type": "order", "workspaceIds": e.workspaceIDs()}); err != nil {
						return rpcError("internal", err.Error(), nil)
					}
				}
			case "host/workspace-removed":
				id, _ := frame["workspaceId"].(string)
				if id == "" || !known[id] {
					continue
				}
				delete(known, id)
				if err := send(map[string]any{"type": "remove", "workspaceId": id}); err != nil {
					return rpcError("internal", err.Error(), nil)
				}
				if err := send(map[string]any{"type": "order", "workspaceIds": e.workspaceIDs()}); err != nil {
					return rpcError("internal", err.Error(), nil)
				}
			case "host/workspace-order-changed":
				if err := send(map[string]any{"type": "order", "workspaceIds": frame["workspaceIds"]}); err != nil {
					return rpcError("internal", err.Error(), nil)
				}
			case "host/archived-sessions-changed":
				if err := send(map[string]any{"type": "archived", "archivedSessionIds": frame["archivedSessionIds"]}); err != nil {
					return rpcError("internal", err.Error(), nil)
				}
			}
		}
	}
}

func remoteWorkspaceID(value any) string {
	switch value := value.(type) {
	case Workspace:
		return value.WorkspaceID
	case *Workspace:
		if value != nil {
			return value.WorkspaceID
		}
	case map[string]any:
		id, _ := value["workspaceId"].(string)
		return id
	}
	return ""
}

func (e *Engine) workspaceIDs() []string {
	items, _ := e.ListWorkspaces()
	ids := make([]string, 0, len(items))
	for _, workspace := range items {
		ids = append(ids, workspace.WorkspaceID)
	}
	return ids
}
