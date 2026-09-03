package harness

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"unicode/utf8"
)

const maxWebSocketPayload = 16 << 20

const (
	wsContinuation = 0x0
	wsText         = 0x1
	wsBinary       = 0x2
	wsClose        = 0x8
	wsPing         = 0x9
	wsPong         = 0xa
)

type wsProtocolError struct {
	code   uint16
	reason string
}

func (e *wsProtocolError) Error() string { return e.reason }

type wsConn struct {
	net.Conn
	reader  io.Reader
	writeMu sync.Mutex
}

func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return writeWSFrame(c.Conn, opcode, payload)
}

func (e *Engine) handleWebSocket(w http.ResponseWriter, r *http.Request, kind string) {
	ws, ok := acceptWebSocket(w, r)
	if !ok {
		return
	}
	defer ws.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	frames := e.streamFrames(ctx, kind)
	go drainWebSocket(ws, cancel)
	e.pumpEvents(ws, frames)
}

func acceptWebSocket(w http.ResponseWriter, r *http.Request) (*wsConn, bool) {
	if r.Method != http.MethodGet || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "websocket upgrade required", http.StatusUpgradeRequired)
		return nil, false
	}
	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	decoded, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(decoded) != 16 {
		http.Error(w, "invalid websocket key", http.StatusBadRequest)
		return nil, false
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")
		http.Error(w, "unsupported websocket version", http.StatusUpgradeRequired)
		return nil, false
	}
	h, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket unavailable", http.StatusNotImplemented)
		return nil, false
	}
	conn, brw, err := h.Hijack()
	if err != nil {
		return nil, false
	}
	accept := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	_, _ = fmt.Fprintf(brw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(accept[:]))
	if err := brw.Flush(); err != nil {
		_ = conn.Close()
		return nil, false
	}
	return &wsConn{Conn: conn, reader: brw.Reader}, true
}

func drainWebSocket(conn *wsConn, cancel context.CancelFunc) {
	defer cancel()
	for {
		opcode, payload, err := readWSFrame(conn.reader)
		if err != nil {
			var protocol *wsProtocolError
			if errors.As(err, &protocol) {
				_ = conn.writeFrame(wsClose, closePayload(protocol.code, protocol.reason))
			}
			return
		}
		switch opcode {
		case wsPing:
			if err := conn.writeFrame(wsPong, payload); err != nil {
				return
			}
		case wsPong:
			// Keepalive response; no application action is needed.
		case wsClose:
			_ = conn.writeFrame(wsClose, payload)
			return
		case wsText, wsBinary, wsContinuation:
			// The event downlink is server-to-client only. Match upstream's 1008.
			_ = conn.writeFrame(wsClose, closePayload(1008, "downlink only"))
			return
		default:
			_ = conn.writeFrame(wsClose, closePayload(1002, "unknown opcode"))
			return
		}
	}
}

func readWSFrame(r io.Reader) (byte, []byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	if head[0]&0x70 != 0 {
		return 0, nil, &wsProtocolError{code: 1002, reason: "reserved websocket bits"}
	}
	fin := head[0]&0x80 != 0
	opcode := head[0] & 0x0f
	if opcode != wsContinuation && opcode != wsText && opcode != wsBinary && opcode != wsClose && opcode != wsPing && opcode != wsPong {
		return 0, nil, &wsProtocolError{code: 1002, reason: "invalid websocket frame"}
	}
	if opcode&0x08 != 0 && !fin {
		return 0, nil, &wsProtocolError{code: 1002, reason: "fragmented websocket control frame"}
	}
	masked := head[1]&0x80 != 0
	if !masked {
		return 0, nil, &wsProtocolError{code: 1002, reason: "client websocket frame is not masked"}
	}
	n := uint64(head[1] & 0x7f)
	if n == 126 {
		var b [2]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, nil, err
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
		if n < 126 {
			return 0, nil, &wsProtocolError{code: 1002, reason: "non-canonical websocket length"}
		}
	} else if n == 127 {
		var b [8]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, nil, err
		}
		n = binary.BigEndian.Uint64(b[:])
		if n>>63 != 0 {
			return 0, nil, &wsProtocolError{code: 1002, reason: "invalid websocket length"}
		}
		if n <= 65535 {
			return 0, nil, &wsProtocolError{code: 1002, reason: "non-canonical websocket length"}
		}
	}
	if (opcode == wsClose || opcode == wsPing || opcode == wsPong) && (!fin || n > 125) {
		return 0, nil, &wsProtocolError{code: 1002, reason: "invalid websocket control frame"}
	}
	if n > maxWebSocketPayload {
		return 0, nil, &wsProtocolError{code: 1009, reason: "websocket message too large"}
	}
	var mask [4]byte
	if _, err := io.ReadFull(r, mask[:]); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, int(n))
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	if opcode == wsClose && len(payload) == 1 {
		return 0, nil, &wsProtocolError{code: 1002, reason: "invalid websocket close payload"}
	}
	if opcode == wsClose && len(payload) >= 2 {
		code := binary.BigEndian.Uint16(payload[:2])
		if !validCloseCode(code) || !utf8.Valid(payload[2:]) {
			return 0, nil, &wsProtocolError{code: 1002, reason: "invalid websocket close payload"}
		}
	}
	return opcode, payload, nil
}

func validCloseCode(code uint16) bool {
	return code >= 3000 && code <= 4999 || code >= 1000 && code <= 1014 && code != 1004 && code != 1005 && code != 1006
}

func closePayload(code uint16, reason string) []byte {
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload, code)
	copy(payload[2:], reason)
	return payload
}

func writeWS(conn *wsConn, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.writeFrame(wsText, data)
}

func envelope(id, typ string, payload any) map[string]any {
	return map[string]any{"type": "server-request", "rpcId": id, "method": typ, "payload": payload}
}

func writeWSFrame(conn io.Writer, opcode byte, payload []byte) error {
	if opcode&0x08 != 0 && len(payload) > 125 {
		return errors.New("websocket control payload too large")
	}
	head := []byte{0x80 | opcode}
	n := uint64(len(payload))
	switch {
	case n < 126:
		head = append(head, byte(n))
	case n <= 65535:
		head = append(head, 126, byte(n>>8), byte(n))
	default:
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], n)
		head = append(head, 127)
		head = append(head, size[:]...)
	}
	if err := writeAll(conn, head); err != nil {
		return err
	}
	return writeAll(conn, payload)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (e *Engine) pumpEvents(conn *wsConn, frames <-chan streamFrame) {
	for frame := range frames {
		rpcID := frame.rpcID
		if rpcID == "" {
			rpcID = newID("rpc")
		}
		if err := writeWS(conn, envelope(rpcID, frame.method, frame.payload)); err != nil {
			return
		}
	}
}

// writeJSON is kept separate from the WebSocket codec so HTTP and custom transports share the same envelope.
func writeEnvelope(w http.ResponseWriter, value any, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func readRequest(r *http.Request, max int64) (clientRequest, error) {
	var req clientRequest
	body := io.LimitReader(r.Body, max)
	dec := json.NewDecoder(bufio.NewReader(body))
	if err := dec.Decode(&req); err != nil {
		return req, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return req, errors.New("request must contain one JSON value")
	}
	return req, nil
}
