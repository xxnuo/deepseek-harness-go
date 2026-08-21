package harness

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDynamicCordisWebServerRoutesFallbackPortAndLifecycle(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "dynamic-web-routes", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "wrt", `
return {
  inject: ['webServer'],
  apply(ctx) {
    const exact = ctx.webServer.register({
      kind: 'exact', path: '/probe',
      async handler(req, res) {
        await Promise.resolve()
        res.writeHead(201, { 'content-type': 'text/plain', 'x-dynamic': 'exact' })
        res.end(req.method + ' ' + req.url + ' ' + req.bodyText)
      }
    })
    ctx.webServer.register({ kind: 'prefix', path: '/tree', handler(_req, res) { res.end('TREE') } })
    ctx.webServer.register({ kind: 'prefix', path: '/tree/deep', handler(_req, res) { res.end('DEEP') } })
    ctx.webServer.register({
      kind: 'exact', path: '/stream',
      handler(_req, res) { res.writeHead(200, { 'content-type': 'text/plain' }); res.write('OPEN') }
    })
    ctx.webServer.registerFallback((req, res) => { res.writeHead(202); res.end('FALLBACK ' + req.url) })
    harness.handle('meta', () => ({ host: ctx.webServer.host, port: ctx.webServer.port }))
    harness.handle('disposeExact', () => { exact(); return true })
    harness.handle('duplicate', () => {
      try {
        ctx.webServer.register({ kind: 'prefix', path: '/tree', handler() {} })
        return ''
      } catch (error) {
        return String(error)
      }
    })
  }
}`)
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	e.SetWebServerListener(server.Listener)

	meta := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "meta", nil)
	if !meta.OK {
		t.Fatalf("meta = %#v", meta)
	}
	port, _ := strconv.Atoi(strings.TrimPrefix(server.URL, "http://127.0.0.1:"))
	metaValue := meta.Value.(map[string]any)
	if metaValue["host"] != "127.0.0.1" || int(metaValue["port"].(float64)) != port {
		t.Fatalf("webServer address = %#v, want port %d", metaValue, port)
	}

	request, err := http.NewRequest(http.MethodPost, server.URL+"/probe?x=1", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusCreated || response.Header.Get("X-Dynamic") != "exact" || string(body) != "POST /probe?x=1 payload" {
		t.Fatalf("exact response = %d %q %#v", response.StatusCode, body, response.Header)
	}
	for path, want := range map[string]string{"/tree": "TREE", "/tree/leaf": "TREE", "/tree/deep/leaf": "DEEP"} {
		got := dynamicWebGET(t, server, path)
		if got.status != http.StatusOK || got.body != want {
			t.Fatalf("GET %s = %#v", path, got)
		}
	}
	if got := dynamicWebGET(t, server, "/unmatched?q=1"); got.status != http.StatusAccepted || got.body != "FALLBACK /unmatched?q=1" {
		t.Fatalf("fallback = %#v", got)
	}
	if got := dynamicWebGET(t, server, "/healthz"); got.status != http.StatusOK || !strings.Contains(got.body, `"ok":true`) {
		t.Fatalf("core route lost precedence = %#v", got)
	}
	duplicate := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "duplicate", nil)
	if !duplicate.OK || !strings.Contains(duplicate.Value.(string), `duplicate prefix route "/tree"`) {
		t.Fatalf("duplicate registration = %#v", duplicate)
	}
	if disposed := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "disposeExact", nil); !disposed.OK {
		t.Fatalf("dispose exact = %#v", disposed)
	}
	if got := dynamicWebGET(t, server, "/probe"); got.status != http.StatusAccepted || got.body != "FALLBACK /probe" {
		t.Fatalf("disposed exact = %#v", got)
	}

	stream, err := server.Client().Get(server.URL + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, 4)
	if _, err := io.ReadFull(stream.Body, prefix); err != nil || string(prefix) != "OPEN" {
		session, _ := e.getSession(sessionID)
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		steering := append([]*queuedPrompt(nil), session.steering...)
		session.mu.Unlock()
		t.Fatalf("stream prefix = %q, %v; status=%d events=%#v steering=%#v", prefix, err, stream.StatusCode, events, steering)
	}
	if stopped, err := e.DynamicCordisStop(sessionID, pluginID); err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	done := make(chan error, 1)
	go func() { _, readErr := io.ReadAll(stream.Body); done <- readErr }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream remained open after route owner stopped")
	}
	_ = stream.Body.Close()
	if got := dynamicWebGET(t, server, "/unmatched"); got.status != http.StatusNotFound {
		t.Fatalf("fallback remained after stop = %#v", got)
	}
}

func TestDynamicCordisWebServerUpgradeRouteAndLifecycle(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "dynamic-web-upgrade", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "wup", `
return {
  inject: ['webServer'],
  apply(ctx) {
    ctx.webServer.registerUpgrade({
      path: '/events',
      handler(req, socket, head) {
        socket.write('HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: dsh-test\r\n\r\n')
        socket.write('READY ' + req.url + ' ' + head.toString() + '\n')
        socket.on('data', data => socket.write('ECHO ' + data.toString()))
      }
    })
    harness.handle('duplicate', () => {
      try {
        ctx.webServer.registerUpgrade({ path: '/events', handler() {} })
        return ''
      } catch (error) {
        return String(error)
      }
    })
  }
}`)
	server := httptest.NewUnstartedServer(e.Handler())
	server.Start()
	t.Cleanup(server.Close)
	e.SetWebServerListener(server.Listener)
	address := server.Listener.Addr().String()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	_, err = io.WriteString(conn, "GET /events?stream=mux HTTP/1.1\r\nHost: "+address+"\r\nConnection: Upgrade\r\nUpgrade: dsh-test\r\n\r\nHEAD")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(status, "101 Switching Protocols") {
		session, _ := e.getSession(sessionID)
		session.mu.Lock()
		events := append([]Event(nil), session.Events...)
		steering := append([]*queuedPrompt(nil), session.steering...)
		session.mu.Unlock()
		t.Fatalf("upgrade status = %q, %v; events=%#v steering=%#v", status, err, events, steering)
	}
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatal(readErr)
		}
		if line == "\r\n" {
			break
		}
	}
	ready, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(ready, "READY /events?stream=mux ") {
		t.Fatalf("upgrade ready = %q, %v", ready, err)
	}
	if _, err := io.WriteString(conn, "PING"); err != nil {
		t.Fatal(err)
	}
	echo := make([]byte, len("ECHO PING"))
	if _, err := io.ReadFull(reader, echo); err != nil || string(echo) != "ECHO PING" {
		t.Fatalf("upgrade echo = %q, %v", echo, err)
	}
	duplicate := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "duplicate", nil)
	if !duplicate.OK || !strings.Contains(duplicate.Value.(string), `duplicate upgrade route "/events"`) {
		t.Fatalf("duplicate upgrade = %#v", duplicate)
	}
	if stopped, err := e.DynamicCordisStop(sessionID, pluginID); err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := reader.ReadByte(); err == nil {
		t.Fatal("upgraded connection remained open after route owner stopped")
	}
}

type dynamicWebResponse struct {
	status int
	body   string
}

func dynamicWebGET(t *testing.T, server *httptest.Server, path string) dynamicWebResponse {
	t.Helper()
	response, err := server.Client().Get(server.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return dynamicWebResponse{status: response.StatusCode, body: string(body)}
}
