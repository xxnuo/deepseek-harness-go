package harness

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	Engine   *Engine
	HTTP     *http.Server
	Listener net.Listener
}

func NewServer(e *Engine) *Server { return &Server{Engine: e} }

func (s *Server) Handler() http.Handler { return s.Engine.Handler() }

func (s *Server) ListenAndServe(ctx context.Context) error {
	cfg := s.Engine.Config()
	ln, err := net.Listen("tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		return err
	}
	s.Listener = ln
	s.Engine.SetWebServerListener(ln)
	s.HTTP = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 0, IdleTimeout: 60 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- s.HTTP.Serve(ln) }()
	select {
	case <-ctx.Done():
		_ = s.HTTP.Shutdown(context.Background())
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func validateWebServerConfig(cfg Config) error {
	if cfg.Host != "127.0.0.1" && cfg.Host != "0.0.0.0" {
		return fmt.Errorf("webserver: host must be 127.0.0.1 or 0.0.0.0, got %q", cfg.Host)
	}
	if cfg.Port < 0 || cfg.Port > 65535 {
		return fmt.Errorf("webserver: port must be between 0 and 65535, got %d", cfg.Port)
	}
	for _, authority := range cfg.TrustedHosts {
		if err := validateTrustedAuthority(authority); err != nil {
			return err
		}
	}
	return nil
}

func trustedHostsForBind(host string, explicit []string) []string {
	result := make([]string, 0, len(explicit)+4)
	if host == "0.0.0.0" {
		result = append(result, LANIPv4Addresses()...)
	}
	return append(result, explicit...)
}

type apiAuthority struct {
	host         string
	hostname     string
	port         string
	explicitPort bool
}

func parseAPIAuthority(value string) (apiAuthority, bool) {
	parsed, err := url.Parse("http://" + strings.TrimSpace(value))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return apiAuthority{}, false
	}
	return normalizeAPIAuthority(parsed)
}

func parseAPIOrigin(value string) (apiAuthority, bool) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return apiAuthority{}, false
	}
	return normalizeAPIAuthority(parsed)
}

func normalizeAPIAuthority(parsed *url.URL) (apiAuthority, bool) {
	hostname, ok := normalizeAPIHostname(parsed.Hostname())
	if !ok {
		return apiAuthority{}, false
	}
	port := parsed.Port()
	if port != "" {
		value, err := strconv.ParseUint(port, 10, 16)
		if err != nil || value > 65535 {
			return apiAuthority{}, false
		}
		port = strconv.FormatUint(value, 10)
	}
	host := renderAPIHostname(hostname)
	if port != "" && port != defaultAPIPort(parsed.Scheme) {
		host += ":" + port
	}
	return apiAuthority{host: host, hostname: hostname, port: port, explicitPort: parsed.Port() != ""}, true
}

func normalizeAPIHostname(hostname string) (string, bool) {
	hostname = strings.ToLower(hostname)
	hostname = strings.TrimPrefix(strings.TrimSuffix(hostname, "]"), "[")
	if hostname == "" {
		return "", false
	}
	for _, r := range hostname {
		if r <= 0x20 || r >= 0x7f || strings.ContainsRune("#/?<>@[\\]^|", r) {
			return "", false
		}
	}
	if address, err := netip.ParseAddr(hostname); err == nil {
		if address.Zone() != "" {
			return "", false
		}
		return address.String(), true
	}
	if address, numeric, valid := parseWHATWGIPv4(hostname); numeric {
		return address, valid
	}
	return hostname, true
}

func parseWHATWGIPv4(hostname string) (address string, numeric, valid bool) {
	value := strings.TrimSuffix(hostname, ".")
	parts := strings.Split(value, ".")
	if _, ok := parseIPv4Number(parts[len(parts)-1]); !ok {
		return "", false, false
	}
	values := make([]uint64, len(parts))
	for index, part := range parts {
		parsed, ok := parseIPv4Number(part)
		if !ok {
			return "", true, false
		}
		values[index] = parsed
	}
	if len(values) == 0 || len(values) > 4 {
		return "", true, false
	}
	for _, part := range values[:len(values)-1] {
		if part > 255 {
			return "", true, false
		}
	}
	lastLimit := uint64(1) << (8 * (5 - len(values)))
	if values[len(values)-1] >= lastLimit {
		return "", true, false
	}
	result := values[len(values)-1]
	for index, part := range values[:len(values)-1] {
		result += part << (8 * (3 - index))
	}
	return netip.AddrFrom4([4]byte{byte(result >> 24), byte(result >> 16), byte(result >> 8), byte(result)}).String(), true, true
}

func parseIPv4Number(value string) (uint64, bool) {
	base, digits := 10, value
	switch {
	case len(value) > 2 && (strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X")):
		base, digits = 16, value[2:]
	case len(value) > 1 && value[0] == '0':
		base, digits = 8, value[1:]
	}
	if digits == "" && value != "0" {
		return 0, false
	}
	parsed, err := strconv.ParseUint(digits, base, 64)
	return parsed, err == nil
}

func renderAPIHostname(hostname string) string {
	if strings.Contains(hostname, ":") {
		return "[" + hostname + "]"
	}
	return hostname
}

func defaultAPIPort(scheme string) string {
	switch strings.ToLower(scheme) {
	case "http", "ws":
		return "80"
	case "https", "wss":
		return "443"
	case "ftp":
		return "21"
	default:
		return ""
	}
}

func validateTrustedAuthority(value string) error {
	authority, ok := parseAPIAuthority(value)
	if ok {
		// WHATWG keeps an explicit default port only under the other HTTP scheme.
		if authority.port == "" {
			if https, valid := url.Parse("https://" + value); valid == nil {
				authority.port = https.Port()
				authority.explicitPort = authority.port != ""
			}
		}
		canonical := renderAPIHostname(authority.hostname)
		if authority.explicitPort {
			canonical += ":" + authority.port
		}
		if strings.EqualFold(value, canonical) {
			return nil
		}
	}
	return fmt.Errorf("client-connection: trustedHosts entry %q is not a bare host[:port] authority", value)
}

func isLoopbackAPIHostname(hostname string) bool {
	if hostname == "localhost" || hostname == "::1" {
		return true
	}
	address, err := netip.ParseAddr(hostname)
	return err == nil && address.Is4() && address.As4()[0] == 127
}

func isTrustedAPIAuthority(authority apiAuthority, trusted []string) bool {
	for _, value := range trusted {
		candidate, ok := parseAPIAuthority(value)
		if !ok {
			continue
		}
		if candidate.explicitPort && candidate.host == authority.host || !candidate.explicitPort && candidate.hostname == authority.hostname {
			return true
		}
	}
	return false
}

func isTrustedAPIRequest(r *http.Request, trusted []string) bool {
	authority, ok := parseAPIAuthority(r.Host)
	if !ok || !isLoopbackAPIHostname(authority.hostname) && !isTrustedAPIAuthority(authority, trusted) {
		return false
	}
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		return false
	}
	origins := r.Header.Values("Origin")
	if len(origins) == 0 {
		return true
	}
	if len(origins) != 1 {
		return false
	}
	origin, ok := parseAPIOrigin(origins[0])
	return ok && origin.host == authority.host
}

// LANIPv4Addresses returns the non-loopback IPv4 literals used by the browser trust fence.
func LANIPv4Addresses() []string {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	result := make([]string, 0, len(addresses))
	seen := make(map[string]bool, len(addresses))
	for _, address := range addresses {
		ip, _, err := net.ParseCIDR(address.String())
		if err != nil || ip.IsLoopback() || ip.To4() == nil {
			continue
		}
		value := ip.String()
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func (s *Server) Close() error {
	if s.HTTP != nil {
		return s.HTTP.Close()
	}
	return s.Engine.Close()
}

func (e *Engine) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w, map[string]any{"ok": true, "version": e.cfg.Version}, http.StatusOK)
	})
	mux.HandleFunc("/api/events.mux", func(w http.ResponseWriter, r *http.Request) {
		if !e.allowed(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if r.Method == http.MethodGet && !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			w.Header().Set("Connection", "Upgrade")
			w.Header().Set("Upgrade", "websocket")
			http.Error(w, "upgrade required", http.StatusUpgradeRequired)
			return
		}
		e.handleWebSocket(w, r, "mux")
	})
	mux.HandleFunc("/api/events.host", func(w http.ResponseWriter, r *http.Request) {
		if !e.allowed(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if r.Method == http.MethodGet && !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			w.Header().Set("Connection", "Upgrade")
			w.Header().Set("Upgrade", "websocket")
			http.Error(w, "upgrade required", http.StatusUpgradeRequired)
			return
		}
		e.handleWebSocket(w, r, "host")
	})
	mux.HandleFunc("/api/", e.apiHandler)
	mux.HandleFunc("/api/session.export", e.sessionExportHandler)
	mux.HandleFunc("/plugins/events", e.servePluginEvents)
	mux.HandleFunc("/plugins/", e.pluginHandler)
	mux.HandleFunc("/", e.staticHandler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e.dynamicWebUpgradeHandler(w, r) || e.dynamicWebNamedHandler(w, r) {
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (e *Engine) allowed(r *http.Request) bool {
	return isTrustedAPIRequest(r, e.cfg.TrustedHosts)
}

var loopbackOnlyRPCMethods = map[string]bool{
	"agentPreset.read":         true,
	"agentPreset.copy":         true,
	"agentPreset.openDocument": true,
	"agentPreset.remove":       true,
	"host.pickDirectory":       true,
	"host.openPath":            true,
	"settings.describe":        true,
	"settings.openDocument":    true,
	"settings.update":          true,
	"settings.replace":         true,
	"settings.mutate":          true,
	"credentials.describe":     true,
	"credentials.set":          true,
	"credentials.unset":        true,
	"llm.discoverModels":       true,
}

func (e *Engine) apiHandler(w http.ResponseWriter, r *http.Request) {
	if !e.allowed(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if r.URL.Path == "/api/session.export" {
		e.sessionExportHandler(w, r)
		return
	}
	if r.URL.Path == "/api/respond" {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var response struct {
			Type   string         `json:"type"`
			RPCID  string         `json:"rpcId"`
			Result map[string]any `json:"result"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, e.cfg.MaxBodyBytes)).Decode(&response); err != nil || response.Type != "client-response" || response.RPCID == "" || response.Result == nil {
			writeEnvelope(w, map[string]any{"accepted": false, "reason": "bad-response"}, http.StatusOK)
			return
		}
		if e.ResolveInteraction(response.RPCID, response.Result) {
			writeEnvelope(w, map[string]any{"accepted": true}, http.StatusOK)
			return
		}
		writeEnvelope(w, map[string]any{"accepted": false, "reason": "not-pending"}, http.StatusOK)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	mediaType := strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0])
	if mediaType != "application/json" {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}
	method := strings.TrimPrefix(r.URL.Path, "/api/")
	remote := strings.Contains(method, "/")
	if method == "" || remote && !isRemoteEndpoint(method) {
		http.NotFound(w, r)
		return
	}
	if loopbackOnlyRPCMethods[method] && !isTrustedAPIRequest(r, nil) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	req, err := readRequest(r, e.cfg.MaxBodyBytes)
	if err != nil {
		writeEnvelope(w, errResult("invalid-request", rpcError("bad-request", "invalid JSON request", nil)), http.StatusBadRequest)
		return
	}
	if req.RPCID == "" {
		req.RPCID = "invalid-request"
	}
	if req.Method != "" && req.Method != method {
		writeEnvelope(w, errResult(req.RPCID, rpcError("bad-request", "method does not match URL", nil)), http.StatusOK)
		return
	}
	if req.Type != "" && req.Type != "client-request" {
		writeEnvelope(w, errResult(req.RPCID, rpcError("bad-request", "request type must be client-request", nil)), http.StatusOK)
		return
	}
	if remote {
		value, present, businessErr := e.dispatchRemote(r.Context(), method, req.Payload)
		if businessErr != nil {
			writeEnvelope(w, errResult(req.RPCID, businessErr), http.StatusOK)
			return
		}
		writeEnvelope(w, okRemoteResult(req.RPCID, value, present), http.StatusOK)
		return
	}
	value, businessErr := e.dispatch(r.Context(), method, req.Payload, req.RPCID)
	if businessErr != nil {
		writeEnvelope(w, errResult(req.RPCID, businessErr), http.StatusOK)
		return
	}
	writeEnvelope(w, okResult(req.RPCID, value), http.StatusOK)
}

func (e *Engine) sessionExportHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("sessionId")
	if id == "" {
		http.Error(w, "missing or invalid sessionId query parameter", http.StatusBadRequest)
		return
	}
	include := r.URL.Query().Get("includeDescendants")
	if include != "" && include != "true" && include != "false" {
		http.Error(w, "includeDescendants must be true or false", http.StatusBadRequest)
		return
	}
	if _, err := e.getSession(id); err != nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="dsh-session-`+safeExportSegment(id)+`.zip"`)
	if r.Method == http.MethodHead {
		return
	}
	if err := e.ExportSessionZIP(id, include == "true", w); err != nil {
		http.Error(w, "session export failed", http.StatusInternalServerError)
	}
}

func (e *Engine) staticHandler(w http.ResponseWriter, r *http.Request) {
	if e.dynamicWebFallbackHandler(w, r) {
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	root := e.frontendRoot()
	if root == "" {
		http.NotFound(w, r)
		return
	}
	root, _ = filepath.Abs(root)
	path := filepath.Join(root, filepath.Clean(filepath.FromSlash("."+r.URL.Path)))
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	index := filepath.Join(root, "index.html")
	if path != root && path != index {
		st, statErr := os.Stat(path)
		if statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				http.NotFound(w, r)
			} else {
				w.WriteHeader(http.StatusBadRequest)
			}
			return
		}
		if st.IsDir() {
			http.NotFound(w, r)
			return
		}
		if contentType := mime.TypeByExtension(filepath.Ext(path)); contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		http.ServeFile(w, r, path)
		return
	}
	data, readErr := os.ReadFile(index)
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			http.NotFound(w, r)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
		return
	}
	html, renderErr := e.RenderIndex(string(data))
	if renderErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if r.Method != http.MethodHead {
		_, _ = io.WriteString(w, html)
	}
}

func (e *Engine) pluginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/plugins/")
	isMap := strings.HasSuffix(path, "/client.js.map")
	suffix := "/client.js"
	if isMap {
		suffix = "/client.js.map"
	}
	if !strings.HasSuffix(path, suffix) {
		http.NotFound(w, r)
		return
	}
	id := strings.TrimSuffix(path, suffix)
	bundle, ok := findPluginPath(e, id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if isMap {
		bundle += ".map"
	}
	if _, err := os.Stat(bundle); err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	if isMap {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	}
	http.ServeFile(w, r, bundle)
}
func (e *Engine) frontendRoot() string {
	if e.cfg.FrontendDir != "" {
		if _, err := os.Stat(e.cfg.FrontendDir); err == nil {
			return e.cfg.FrontendDir
		}
	}
	for _, candidate := range []string{"deepseek-harness/apps/web/dist", "deepseek-harness/apps/web", "apps/web/dist"} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return ""
}

// ExportSession returns the canonical JSONL transcript for integrations that do not need ZIP support.
func (e *Engine) ExportSession(id string, w io.Writer) error {
	s, err := e.getSession(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header := map[string]any{"type": "session", "version": s.Header.Version, "id": s.Header.ID, "createdAt": s.Header.CreatedAt, "cwd": s.Header.CWD}
	if s.Header.ParentSession != "" {
		header["parentSession"] = s.Header.ParentSession
	}
	if s.Header.SeedLength != 0 {
		header["seedLength"] = s.Header.SeedLength
	}
	if s.Header.Origin != "" {
		header["origin"] = s.Header.Origin
	}
	header["delegationDepth"] = s.Header.DelegationDepth
	if s.Header.AgentPreset != "" {
		header["agentPreset"] = s.Header.AgentPreset
	}
	if s.Header.Mode != "" {
		header["mode"] = s.Header.Mode
	}
	if err := writeJSONLine(w, header); err != nil {
		return err
	}
	for _, ev := range s.Events {
		if err := writeJSONLine(w, ev); err != nil {
			return err
		}
	}
	return nil
}

// RunWithListener is the embeddable entry point used by custom frontends and tests.
func RunWithListener(ctx context.Context, e *Engine, listener net.Listener) error {
	srv := NewServer(e)
	srv.Listener = listener
	e.SetWebServerListener(listener)
	srv.HTTP = &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.HTTP.Serve(listener) }()
	select {
	case <-ctx.Done():
		_ = srv.HTTP.Shutdown(context.Background())
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// TLSConfig is intentionally small; callers can use it to put the library behind an existing listener.
func TLSConfig(certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}
