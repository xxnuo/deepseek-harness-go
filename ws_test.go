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

func maskedWSFrame(opcode byte, payload []byte) []byte {
	mask := [4]byte{1, 2, 3, 4}
	out := []byte{0x80 | opcode}
	switch {
	case len(payload) < 126:
		out = append(out, 0x80|byte(len(payload)))
	case len(payload) <= 65535:
		out = append(out, 0x80|126, byte(len(payload)>>8), byte(len(payload)))
	default:
		out = append(out, 0x80|127, 0, 0, 0, 0, byte(len(payload)>>24), byte(len(payload)>>16), byte(len(payload)>>8), byte(len(payload)))
	}
	out = append(out, mask[:]...)
	for i, b := range payload {
		out = append(out, b^mask[i%4])
	}
	return out
}

func TestReadWSFrameUnmasksAndValidates(t *testing.T) {
	opcode, payload, err := readWSFrame(bytes.NewReader(maskedWSFrame(wsText, []byte("hello"))))
	if err != nil || opcode != wsText || string(payload) != "hello" {
		t.Fatalf("read frame = opcode %d payload %q err %v", opcode, payload, err)
	}
	_, _, err = readWSFrame(bytes.NewReader([]byte{0x81, 0x00}))
	var protocol *wsProtocolError
	if !errors.As(err, &protocol) || protocol.code != 1002 {
		t.Fatalf("unmasked frame error = %v, want protocol 1002", err)
	}
}

func TestWriteWSFrameExtendedLength(t *testing.T) {
	payload := bytes.Repeat([]byte{'x'}, 126)
	var out bytes.Buffer
	if err := writeWSFrame(&out, wsText, payload); err != nil {
		t.Fatal(err)
	}
	data := out.Bytes()
	if len(data) != 130 || data[0] != 0x81 || data[1] != 126 || binary.BigEndian.Uint16(data[2:4]) != 126 {
		t.Fatalf("frame header = %v", data[:4])
	}
}

func TestDrainWebSocketRespondsToPing(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drainWebSocket(&wsConn{Conn: server, reader: server}, cancel)
	if _, err := client.Write(maskedWSFrame(wsPing, []byte("x"))); err != nil {
		t.Fatal(err)
	}
	var header [2]byte
	if _, err := client.Read(header[:]); err != nil {
		t.Fatal(err)
	}
	if header[0] != 0x8a || header[1] != 1 {
		t.Fatalf("pong header = %v", header)
	}
	var payload [1]byte
	if _, err := client.Read(payload[:]); err != nil || payload[0] != 'x' {
		t.Fatalf("pong payload = %q err %v", payload, err)
	}
	_ = server.Close()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("drain did not stop")
	}
}

func TestMuxStreamIncludesSessionCreatedAfterConnect(t *testing.T) {
	e := newIntegrationEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := e.streamFrames(ctx, "mux")
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "late", "")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case frame := <-frames:
			if frame.method == "session/subscribed" {
				payload, ok := frame.payload.(map[string]any)
				if ok && payload["sessionId"] == id {
					return
				}
			}
		case <-deadline:
			t.Fatalf("did not receive subscription for %s", id)
		}
	}
}

func TestWebSocketMuxIncludesSessionCreatedAfterUpgrade(t *testing.T) {
	e := newIntegrationEngine(t)
	if _, err := e.CreateSession(context.Background(), e.Config().Workspace, "baseline", ""); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(e.Handler())
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("GET /api/events.mux HTTP/1.1\r\nHost: " + server.Listener.Addr().String() + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %s", response.Status)
	}
	if _, err := readServerWSEnvelope(reader); err != nil {
		t.Fatalf("read baseline frame: %v", err)
	}
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "late", "")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		envelope, err := readServerWSEnvelope(reader)
		if err != nil {
			t.Fatalf("read late frame: %v", err)
		}
		if envelope["method"] != "session/subscribed" {
			continue
		}
		payload, _ := envelope["payload"].(map[string]any)
		if payload["sessionId"] == id {
			return
		}
	}
}

func readServerWSEnvelope(r *bufio.Reader) (map[string]any, error) {
	var head [2]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err
	}
	n := int(head[1] & 0x7f)
	if head[0] != 0x81 || head[1]&0x80 != 0 {
		return nil, errors.New("invalid server websocket frame")
	}
	if n == 126 {
		var b [2]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, err
		}
		n = int(binary.BigEndian.Uint16(b[:]))
	} else if n == 127 {
		var b [8]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, err
		}
		n = int(binary.BigEndian.Uint64(b[:]))
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	var envelope map[string]any
	return envelope, json.Unmarshal(payload, &envelope)
}
