package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func sendRemoteFrame(t *testing.T, conn net.Conn, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(maskedWSFrame(wsText, payload)); err != nil {
		t.Fatal(err)
	}
}

func readServerWSFrame(reader io.Reader) (byte, []byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(reader, head[:]); err != nil {
		return 0, nil, err
	}
	opcode := head[0] & 0x0f
	if head[0]&0x80 == 0 || head[1]&0x80 != 0 {
		return 0, nil, errors.New("invalid server websocket frame")
	}
	n := uint64(head[1] & 0x7f)
	if n == 126 {
		var size [2]byte
		if _, err := io.ReadFull(reader, size[:]); err != nil {
			return 0, nil, err
		}
		n = uint64(binary.BigEndian.Uint16(size[:]))
	} else if n == 127 {
		var size [8]byte
		if _, err := io.ReadFull(reader, size[:]); err != nil {
			return 0, nil, err
		}
		n = binary.BigEndian.Uint64(size[:])
	}
	payload := make([]byte, int(n))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	return opcode, payload, nil
}

func readRemoteFrame(t *testing.T, reader *bufio.Reader) map[string]any {
	t.Helper()
	opcode, payload, err := readServerWSFrame(reader)
	if err != nil {
		t.Fatal(err)
	}
	if opcode != wsText {
		t.Fatalf("remote frame opcode = %d, want text", opcode)
	}
	var frame map[string]any
	if err := json.Unmarshal(payload, &frame); err != nil {
		t.Fatal(err)
	}
	return frame
}

func waitRemoteFrame(t *testing.T, reader *bufio.Reader, accept func(map[string]any) bool) map[string]any {
	t.Helper()
	for range 64 {
		frame := readRemoteFrame(t, reader)
		if accept(frame) {
			return frame
		}
	}
	t.Fatal("matching Remote stream frame was not received")
	return nil
}

func openRemoteMux(t *testing.T, engine *Engine) (*httptest.Server, net.Conn, *bufio.Reader) {
	t.Helper()
	server := httptest.NewServer(engine.Handler())
	t.Cleanup(server.Close)
	conn, reader, response := openTestWebSocket(t, server, "/api/remote.mux", "")
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("remote mux upgrade status = %s", response.Status)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	return server, conn, reader
}

func postRemoteEventResult(t *testing.T, server *httptest.Server, clientID, eventID string, outcome map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"type": "client-request", "rpcId": "event-result", "method": "$events/result",
		"payload": map[string]any{"args": map[string]any{
			"clientId": clientID, "eventId": eventID, "outcome": outcome,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Post(server.URL+"/api/$events/result", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var wire serverResponse
	if err := json.NewDecoder(response.Body).Decode(&wire); err != nil {
		t.Fatal(err)
	}
	result, _ := wire.Result.(map[string]any)
	if result["ok"] != true {
		t.Fatalf("event result response = %#v", wire.Result)
	}
}

func TestRemoteStreamMuxServesRequiredStreams(t *testing.T) {
	engine := newIntegrationEngine(t)
	sessionID, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "remote-stream", "")
	if err != nil {
		t.Fatal(err)
	}
	_, conn, reader := openRemoteMux(t, engine)
	sendRemoteFrame(t, conn, map[string]any{
		"type": "open", "streamId": "events", "endpoint": "$events", "payload": map[string]any{"args": map[string]any{}},
	})
	sendRemoteFrame(t, conn, map[string]any{
		"type": "open", "streamId": "follow", "endpoint": "session/follow",
		"payload": map[string]any{"args": map[string]any{"request": map[string]any{
			"address": map[string]any{"kind": "session", "sessionId": sessionID},
		}}},
	})
	for _, streamID := range []string{"control", "workspaces"} {
		endpoint := "session/control"
		if streamID == "workspaces" {
			endpoint = "workspace/follow"
		}
		sendRemoteFrame(t, conn, map[string]any{
			"type": "open", "streamId": streamID, "endpoint": endpoint, "payload": map[string]any{"args": map[string]any{}},
		})
	}
	wantOpening := map[string]string{
		"events": "ready", "follow": "snapshot", "control": "baseline", "workspaces": "baseline",
	}
	for len(wantOpening) > 0 {
		frame := readRemoteFrame(t, reader)
		streamID, _ := frame["streamId"].(string)
		opening, wanted := wantOpening[streamID]
		if !wanted || frame["type"] != "item" {
			continue
		}
		value, _ := frame["value"].(map[string]any)
		if value["type"] != opening {
			t.Fatalf("%s opening = %#v, want %s", streamID, value, opening)
		}
		delete(wantOpening, streamID)
	}

	engine.emitHost(map[string]any{"type": "host/session-status", "sessionId": sessionID, "running": true})
	status := waitRemoteFrame(t, reader, func(frame map[string]any) bool {
		value, _ := frame["value"].(map[string]any)
		return frame["streamId"] == "events" && value["type"] == "emit" && value["event"] == "api-session/status"
	})
	value := status["value"].(map[string]any)
	args := value["args"].([]any)
	if args[0] != sessionID || args[1] != true {
		t.Fatalf("status args = %#v", args)
	}

	if _, _, err := engine.RenameSession(sessionID, "Remote stream renamed"); err != nil {
		t.Fatal(err)
	}
	waitRemoteFrame(t, reader, func(frame map[string]any) bool {
		value, _ := frame["value"].(map[string]any)
		return frame["streamId"] == "follow" && value["type"] == "event"
	})

	workspace, _, err := engine.CreateWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	waitRemoteFrame(t, reader, func(frame map[string]any) bool {
		value, _ := frame["value"].(map[string]any)
		row, _ := value["workspace"].(map[string]any)
		return frame["streamId"] == "workspaces" && value["type"] == "upsert" && row["workspaceId"] == workspace.WorkspaceID
	})
	waitRemoteFrame(t, reader, func(frame map[string]any) bool {
		value, _ := frame["value"].(map[string]any)
		return frame["streamId"] == "workspaces" && value["type"] == "order"
	})

	for _, streamID := range []string{"events", "follow", "control", "workspaces"} {
		sendRemoteFrame(t, conn, map[string]any{"type": "cancel", "streamId": streamID})
	}
	waitForRemoteCondition(t, func() bool {
		engine.pendingMu.Lock()
		defer engine.pendingMu.Unlock()
		return len(engine.remoteEventClients) == 0
	})
	sendRemoteFrame(t, conn, map[string]any{
		"type": "open", "streamId": "unknown", "endpoint": "missing/follow", "payload": map[string]any{"args": map[string]any{}},
	})
	remoteError := waitRemoteFrame(t, reader, func(frame map[string]any) bool {
		return frame["streamId"] == "unknown" && frame["type"] == "error"
	})
	errorValue := remoteError["error"].(map[string]any)
	if errorValue["code"] != "gateway/invocation-unavailable" {
		t.Fatalf("unknown stream error = %#v", errorValue)
	}
}

func TestRemoteEventWaterfallRoundTrip(t *testing.T) {
	engine := newIntegrationEngine(t)
	sessionID, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "remote-waterfall", "")
	if err != nil {
		t.Fatal(err)
	}
	server, firstConn, firstReader := openRemoteMux(t, engine)
	_, secondConn, secondReader := openRemoteMux(t, engine)
	open := map[string]any{
		"type": "open", "streamId": "events", "endpoint": "$events", "payload": map[string]any{"args": map[string]any{}},
	}
	sendRemoteFrame(t, firstConn, open)
	sendRemoteFrame(t, secondConn, open)
	clientID := func(reader *bufio.Reader) string {
		ready := waitRemoteFrame(t, reader, func(frame map[string]any) bool {
			value, _ := frame["value"].(map[string]any)
			return frame["streamId"] == "events" && value["type"] == "ready"
		})
		return ready["value"].(map[string]any)["clientId"].(string)
	}
	firstClientID := clientID(firstReader)
	secondClientID := clientID(secondReader)
	type outcome struct {
		value any
		err   error
	}
	request := func(reason string) (chan outcome, map[string]any, map[string]any) {
		done := make(chan outcome, 1)
		go func() {
			value, err := engine.RequestInteraction(t.Context(), sessionID, "approval/requested", map[string]any{
				"type": "approval/requested", "sessionId": sessionID,
				"approvalId": "approval-one", "toolName": "bash", "reason": reason,
			})
			done <- outcome{value: value, err: err}
		}()
		read := func(reader *bufio.Reader) map[string]any {
			frame := waitRemoteFrame(t, reader, func(frame map[string]any) bool {
				value, _ := frame["value"].(map[string]any)
				return frame["streamId"] == "events" && value["type"] == "waterfall"
			})
			return frame["value"].(map[string]any)
		}
		return done, read(firstReader), read(secondReader)
	}

	done, first, second := request("first-result")
	if first["eventId"] != second["eventId"] {
		t.Fatalf("fan-out event ids = %q and %q", first["eventId"], second["eventId"])
	}
	if first["event"] != "approval/request" || first["agentId"] != sessionID {
		t.Fatalf("waterfall = %#v", first)
	}
	requestValue := first["request"].(map[string]any)
	if requestValue["toolName"] != "bash" || requestValue["reason"] != "first-result" || requestValue["type"] != nil {
		t.Fatalf("waterfall request = %#v", requestValue)
	}
	eventID := first["eventId"].(string)
	postRemoteEventResult(t, server, secondClientID, eventID, map[string]any{"kind": "result", "value": "allowed-once"})
	select {
	case settled := <-done:
		if settled.err != nil || settled.value != "allowed-once" {
			t.Fatalf("interaction result = %#v err=%v", settled.value, settled.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("interaction did not settle")
	}
	waitRemoteFrame(t, firstReader, func(frame map[string]any) bool {
		value, _ := frame["value"].(map[string]any)
		return frame["streamId"] == "events" && value["type"] == "cancel" && value["eventId"] == eventID
	})

	done, first, second = request("all-next")
	eventID = first["eventId"].(string)
	if eventID != second["eventId"] {
		t.Fatalf("fan-out event ids = %q and %q", eventID, second["eventId"])
	}
	postRemoteEventResult(t, server, firstClientID, eventID, map[string]any{"kind": "next"})
	postRemoteEventResult(t, server, firstClientID, eventID, map[string]any{"kind": "result", "value": "late"})
	if engine.pendingInteraction(eventID) == nil {
		t.Fatal("one next or a repeated result settled the interaction")
	}
	postRemoteEventResult(t, server, secondClientID, eventID, map[string]any{"kind": "next"})
	select {
	case settled := <-done:
		if settled.err == nil || settled.err.Error() != "gateway/invocation-unavailable: no Remote event listener accepted the interaction" {
			t.Fatalf("all-next interaction result = %#v err=%v", settled.value, settled.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("all-next interaction did not settle")
	}
}

func TestRemoteEventSourcePublishesPendingInteraction(t *testing.T) {
	engine := newIntegrationEngine(t)
	sessionID, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "remote-event-source", "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	frames := make(chan map[string]any, 4)
	done := make(chan *RPCError, 1)
	go func() {
		done <- engine.streamRemoteEvents(ctx, json.RawMessage(`{"args":{}}`), func(value any) error {
			frame, ok := value.(map[string]any)
			if !ok {
				return errors.New("unexpected Remote event frame")
			}
			frames <- frame
			return nil
		})
	}()
	select {
	case frame := <-frames:
		if frame["type"] != "ready" {
			t.Fatalf("opening frame = %#v", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("Remote event source did not become ready")
	}
	interaction := make(chan error, 1)
	go func() {
		_, err := engine.RequestInteraction(ctx, sessionID, "approval/requested", map[string]any{"toolName": "bash"})
		interaction <- err
	}()
	select {
	case frame := <-frames:
		if frame["type"] != "waterfall" || frame["event"] != "approval/request" {
			t.Fatalf("interaction frame = %#v", frame)
		}
		engine.ResolveInteraction(frame["eventId"].(string), map[string]any{"ok": true, "value": "allowed-once"})
	case <-time.After(time.Second):
		t.Fatal("Remote event source did not publish the interaction")
	}
	select {
	case err := <-interaction:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("interaction did not settle")
	}
	cancel()
	select {
	case rpcErr := <-done:
		if rpcErr != nil {
			t.Fatal(rpcErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Remote event source did not stop")
	}
}

func TestRemoteStreamMuxRejectsInvalidCarrierMessages(t *testing.T) {
	tests := []struct {
		name string
		send func(*testing.T, net.Conn)
		code uint16
	}{
		{
			name: "malformed",
			send: func(t *testing.T, conn net.Conn) {
				t.Helper()
				_, _ = conn.Write(maskedWSFrame(wsText, []byte(`{`)))
			},
			code: 1008,
		},
		{
			name: "binary",
			send: func(t *testing.T, conn net.Conn) {
				t.Helper()
				_, _ = conn.Write(maskedWSFrame(wsBinary, []byte("binary")))
			},
			code: 1003,
		},
		{
			name: "duplicate",
			send: func(t *testing.T, conn net.Conn) {
				t.Helper()
				open := map[string]any{
					"type": "open", "streamId": "duplicate", "endpoint": "workspace/follow", "payload": map[string]any{"args": map[string]any{}},
				}
				sendRemoteFrame(t, conn, open)
				sendRemoteFrame(t, conn, open)
			},
			code: 1008,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := newIntegrationEngine(t)
			_, conn, reader := openRemoteMux(t, engine)
			test.send(t, conn)
			for {
				opcode, payload, err := readServerWSFrame(reader)
				if err != nil {
					t.Fatal(err)
				}
				if opcode != wsClose {
					continue
				}
				if len(payload) < 2 || binary.BigEndian.Uint16(payload[:2]) != test.code {
					t.Fatalf("close payload = %v, want code %d", payload, test.code)
				}
				return
			}
		})
	}
}

func TestRemoteStreamHeartbeatGraceAndTermination(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	ctx, cancel := context.WithCancel(t.Context())
	connection := &remoteStreamMuxConnection{
		ws: &wsConn{Conn: server, reader: server}, ctx: ctx, cancel: cancel,
		streams: map[string]*remoteStreamState{},
	}
	if !connection.heartbeatTick() || !connection.heartbeatTick() || connection.heartbeatTick() {
		t.Fatal("heartbeat did not allow exactly two missed Pong intervals")
	}
	connection.recordPong()
	time.Sleep(5 * time.Millisecond)
	select {
	case <-ctx.Done():
		t.Fatal("grace check terminated a connection after Pong reset")
	default:
	}
	connection.mu.Lock()
	connection.missedPongs = remoteStreamMaxMissedPongs
	connection.mu.Unlock()
	if connection.heartbeatTick() {
		t.Fatal("expired heartbeat unexpectedly requested another Ping")
	}
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expired heartbeat did not terminate the connection")
	}
}

func TestRemoteStreamSocketCloseCancelsLogicalStreams(t *testing.T) {
	engine := newIntegrationEngine(t)
	_, conn, reader := openRemoteMux(t, engine)
	sendRemoteFrame(t, conn, map[string]any{
		"type": "open", "streamId": "events", "endpoint": "$events", "payload": map[string]any{"args": map[string]any{}},
	})
	waitRemoteFrame(t, reader, func(frame map[string]any) bool {
		value, _ := frame["value"].(map[string]any)
		return frame["streamId"] == "events" && value["type"] == "ready"
	})
	engine.pendingMu.Lock()
	clients := len(engine.remoteEventClients)
	engine.pendingMu.Unlock()
	if clients != 1 {
		t.Fatalf("active Remote event clients = %d, want 1", clients)
	}
	_ = conn.Close()
	waitForRemoteCondition(t, func() bool {
		engine.pendingMu.Lock()
		defer engine.pendingMu.Unlock()
		return len(engine.remoteEventClients) == 0
	})
}

func waitForRemoteCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Remote stream condition did not become true")
}
