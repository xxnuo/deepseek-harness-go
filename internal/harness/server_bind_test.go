package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestWebServerConfigMatchesUpstreamSchema(t *testing.T) {
	for _, test := range []struct {
		name      string
		host      string
		port      int
		wantError bool
	}{
		{name: "loopback", host: "127.0.0.1", port: 0},
		{name: "all interfaces", host: "0.0.0.0", port: 65535},
		{name: "unsupported host", host: "localhost", port: 3080, wantError: true},
		{name: "negative port", host: "127.0.0.1", port: -1, wantError: true},
		{name: "oversized port", host: "127.0.0.1", port: 65536, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.DataDir = t.TempDir()
			cfg.Host, cfg.Port = test.host, test.port
			e, err := New(WithConfig(cfg))
			if test.wantError {
				if err == nil {
					_ = e.Close()
					t.Fatal("New() unexpectedly succeeded")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			_ = e.Close()
		})
	}
}

func TestWildcardBindTrustsLocalIPv4Addresses(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Host = "0.0.0.0"
	cfg.TrustedHosts = []string{"lab.internal", "lab.internal"}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })

	trusted := e.Config().TrustedHosts
	if len(trusted) == 0 || trusted[len(trusted)-1] != "lab.internal" {
		t.Fatalf("trusted hosts = %q", trusted)
	}
	for _, address := range LANIPv4Addresses() {
		if !slices.Contains(trusted, address) {
			t.Fatalf("trusted hosts %q omit LAN address %q", trusted, address)
		}
		req := httptest.NewRequest("GET", "http://"+address+"/api/host.describe", nil)
		if !e.allowed(req) {
			t.Fatalf("LAN address %q is not accepted by browser trust fence", address)
		}
	}
}

func TestTrustedAuthorityConfigMatchesUpstream(t *testing.T) {
	valid := []string{"harness.internal", "harness.internal:3080", "HARNESS.internal:80", "10.0.0.9", "[::1]:3080"}
	for _, authority := range valid {
		cfg := DefaultConfig()
		cfg.DataDir, cfg.TrustedHosts = t.TempDir(), []string{authority}
		e, err := New(WithConfig(cfg))
		if err != nil {
			t.Fatalf("trusted authority %q: %v", authority, err)
		}
		_ = e.Close()
	}
	invalid := []string{
		"harness.internal/path", "harness.internal/", "user@harness.internal", "harness.internal?x",
		"harness.internal#f", `harness.internal\path`, "bad entry", "", "harness.internal:3080 ",
		" harness.internal", "harness.internal:30\t80", "harness.internal:", "[::1]:",
		"harness.internal:0080", "0x7f.0.0.1", "[0:0:0:0:0:0:0:1]",
	}
	for _, authority := range invalid {
		cfg := DefaultConfig()
		cfg.DataDir, cfg.TrustedHosts = t.TempDir(), []string{authority}
		if e, err := New(WithConfig(cfg)); err == nil {
			_ = e.Close()
			t.Fatalf("trusted authority %q unexpectedly succeeded", authority)
		}
	}
}

func TestAPITrustFenceMatchesUpstreamAuthorityAndBrowserMarkers(t *testing.T) {
	req := func(host, origin, site string) *http.Request {
		request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/api/host.describe", nil)
		request.Host = host
		if origin != "" {
			request.Header.Set("Origin", origin)
		}
		if site != "" {
			request.Header.Set("Sec-Fetch-Site", site)
		}
		return request
	}
	for _, request := range []*http.Request{
		req("localhost:3080", "http://localhost:3080", "same-origin"),
		req("127.8.9.10:80", "http://127.8.9.10", ""),
		req("[::1]:3080", "http://[::1]:3080", ""),
		req("Harness.INTERNAL:3080", "http://harness.internal:3080", ""),
	} {
		trusted := []string(nil)
		if strings.Contains(strings.ToLower(request.Host), "harness.internal") {
			trusted = []string{"harness.internal:3080"}
		}
		if !isTrustedAPIRequest(request, trusted) {
			t.Fatalf("trusted request rejected: host=%q origin=%q site=%q", request.Host, request.Header.Get("Origin"), request.Header.Get("Sec-Fetch-Site"))
		}
	}

	for _, test := range []struct {
		host, origin, site string
		trusted            []string
	}{
		{host: "harness.internal:3080", origin: "http://harness.internal:3080", trusted: []string{"harness.internal:9999"}},
		{host: "harness.internal:3080", origin: "http://evil.example", trusted: []string{"harness.internal"}},
		{host: "127.0.0.1:3080", site: "cross-site"},
		{host: "127.0.0.1:3080", origin: "null"},
		{host: "evil.example:3080", origin: "http://evil.example:3080"},
		{host: "127.0.0.999"},
	} {
		if isTrustedAPIRequest(req(test.host, test.origin, test.site), test.trusted) {
			t.Fatalf("untrusted request accepted: %#v", test)
		}
	}
	if !isTrustedAPIRequest(req("harness.internal:9999", "http://harness.internal:9999", ""), []string{"harness.internal"}) {
		t.Fatal("port-less trusted authority did not accept another port")
	}
}

func TestTrustedHostCannotReachLoopbackOnlyRPC(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.TrustedHosts = []string{"lab.internal"}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	post := func(method string) int {
		body, _ := json.Marshal(map[string]any{"type": "client-request", "rpcId": "trust", "method": method, "payload": map[string]any{}})
		request, err := http.NewRequest(http.MethodPost, server.URL+"/api/"+method, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Host = "lab.internal"
		request.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return response.StatusCode
	}
	if status := post("host.describe"); status != http.StatusOK {
		t.Fatalf("ordinary trusted-host RPC status = %d", status)
	}
	if status := post("settings.describe"); status != http.StatusForbidden {
		t.Fatalf("loopback-only trusted-host RPC status = %d", status)
	}
}

func TestEventStreamsRequireWebSocketUpgrade(t *testing.T) {
	e := newIntegrationEngine(t)
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	for _, path := range []string{"/api/events.mux", "/api/events.host", "/api/remote.mux"} {
		response, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusUpgradeRequired || response.Header.Get("Upgrade") != "websocket" {
			t.Fatalf("GET %s = %d Upgrade=%q", path, response.StatusCode, response.Header.Get("Upgrade"))
		}
	}
}

func TestServerListenAndServeAllowsConfiguredWildcardBind(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Host, cfg.Port = "0.0.0.0", 0
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	server := NewServer(e)
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ListenAndServe() = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListenAndServe() did not stop")
	}
}
