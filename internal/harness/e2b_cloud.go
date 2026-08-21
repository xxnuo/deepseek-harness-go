package harness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http/httpguts"
)

const (
	e2bEnvdPort          = 49983
	e2bConnectMaxMessage = 64 << 20
	e2bRequestTimeout    = 60 * time.Second
)

var e2bAPIKeyPattern = regexp.MustCompile(`^e2b_[0-9a-f]+$`)

func NewE2BCloudSandboxFactory() E2BSandboxFactory {
	return newE2BCloudSandbox
}

func newE2BCloudSandbox(ctx context.Context, config E2BConfig) (E2BSandbox, error) {
	if !e2bAPIKeyPattern.MatchString(config.APIKey) {
		return nil, errors.New(`dsh-e2b: invalid API key format: expected "e2b_" followed by hex characters`)
	}
	apiURL, err := e2bValidateBaseURL(config.APIURL, "API URL")
	if err != nil {
		return nil, err
	}
	if config.SandboxURL != "" {
		if config.SandboxURL, err = e2bValidateBaseURL(config.SandboxURL, "sandbox URL"); err != nil {
			return nil, err
		}
	}

	request := struct {
		TemplateID          string `json:"templateID"`
		Timeout             int64  `json:"timeout"`
		Secure              bool   `json:"secure"`
		AllowInternetAccess bool   `json:"allow_internet_access"`
		AutoPause           bool   `json:"autoPause"`
		AutoResume          struct {
			Enabled bool `json:"enabled"`
		} `json:"autoResume"`
	}{
		TemplateID:          "base",
		Timeout:             int64((config.Timeout + time.Second - 1) / time.Second),
		Secure:              true,
		AllowInternetAccess: true,
	}
	var created struct {
		SandboxID          string `json:"sandboxID"`
		Domain             string `json:"domain"`
		EnvdVersion        string `json:"envdVersion"`
		EnvdAccessToken    string `json:"envdAccessToken"`
		TrafficAccessToken string `json:"trafficAccessToken"`
	}
	if err := e2bAPIJSON(ctx, http.MethodPost, apiURL+"/sandboxes", config.APIKey, request, &created); err != nil {
		return nil, err
	}
	if created.SandboxID == "" || created.EnvdVersion == "" {
		return nil, errors.New("dsh-e2b: create sandbox returned incomplete metadata")
	}

	domain := created.Domain
	if domain == "" {
		domain = "e2b.app"
	}
	sandboxURL := config.SandboxURL
	if sandboxURL == "" {
		switch domain {
		case "e2b.app", "e2b.dev", "e2b.pro", "e2b-staging.dev":
			sandboxURL = "https://sandbox." + domain
		default:
			sandboxURL = fmt.Sprintf("https://%d-%s.%s", e2bEnvdPort, created.SandboxID, domain)
		}
	}

	sandbox := &e2bCloudSandbox{
		id:          created.SandboxID,
		version:     created.EnvdVersion,
		apiKey:      config.APIKey,
		apiURL:      apiURL,
		sandboxURL:  sandboxURL,
		accessToken: created.EnvdAccessToken,
		client:      http.DefaultClient,
	}
	sandbox.files = &e2bCloudFiles{sandbox: sandbox}
	sandbox.commands = &e2bCloudCommands{sandbox: sandbox}
	sandbox.pty = &e2bCloudPTY{sandbox: sandbox}
	if !e2bVersionAtLeast(sandbox.version, "0.1.0") {
		_ = sandbox.Kill(context.Background())
		return nil, fmt.Errorf("dsh-e2b: sandbox envd %s is too old", sandbox.version)
	}
	return sandbox, nil
}

func e2bValidateBaseURL(value, name string) (string, error) {
	u, err := url.Parse(strings.TrimRight(value, "/"))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", fmt.Errorf("dsh-e2b: invalid %s %q", name, value)
	}
	return u.String(), nil
}

func e2bAPIJSON(ctx context.Context, method, endpoint, apiKey string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-KEY", apiKey)
	req.Header.Set("Accept", "application/json")
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return e2bReadHTTPError(res)
	}
	if output == nil || res.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, res.Body)
		return nil
	}
	if err := json.NewDecoder(res.Body).Decode(output); err != nil {
		return fmt.Errorf("dsh-e2b: decode API response: %w", err)
	}
	return nil
}

type e2bHTTPError struct {
	Status  int
	Code    string
	Message string
}

func (e *e2bHTTPError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("dsh-e2b: HTTP %d %s: %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("dsh-e2b: HTTP %d: %s", e.Status, e.Message)
}

func e2bReadHTTPError(res *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(data, &payload)
	if payload.Message == "" {
		payload.Message = strings.TrimSpace(string(data))
	}
	if payload.Message == "" {
		payload.Message = res.Status
	}
	return &e2bHTTPError{Status: res.StatusCode, Code: payload.Code, Message: payload.Message}
}

type e2bConnectError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *e2bConnectError) Error() string {
	if e.Message == "" {
		return "dsh-e2b: Connect " + e.Code
	}
	return "dsh-e2b: Connect " + e.Code + ": " + e.Message
}

type e2bCloudSandbox struct {
	id          string
	version     string
	apiKey      string
	apiURL      string
	sandboxURL  string
	accessToken string
	client      *http.Client
	files       *e2bCloudFiles
	commands    *e2bCloudCommands
	pty         *e2bCloudPTY
}

func (s *e2bCloudSandbox) ID() string            { return s.id }
func (s *e2bCloudSandbox) Files() E2BFiles       { return s.files }
func (s *e2bCloudSandbox) Commands() E2BCommands { return s.commands }
func (s *e2bCloudSandbox) PTY() E2BPTY           { return s.pty }

func (s *e2bCloudSandbox) Kill(ctx context.Context) error {
	err := e2bAPIJSON(ctx, http.MethodDelete, s.apiURL+"/sandboxes/"+url.PathEscape(s.id), s.apiKey, nil, nil)
	var status *e2bHTTPError
	if errors.As(err, &status) && status.Status == http.StatusNotFound {
		return nil
	}
	return err
}

func (s *e2bCloudSandbox) envdRequest(ctx context.Context, method, route string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, s.sandboxURL+route, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("E2b-Sandbox-Id", s.id)
	req.Header.Set("E2b-Sandbox-Port", strconv.Itoa(e2bEnvdPort))
	if s.accessToken != "" {
		req.Header.Set("X-Access-Token", s.accessToken)
	}
	return req, nil
}

func (s *e2bCloudSandbox) defaultUserHeader(req *http.Request) {
	if !e2bVersionAtLeast(s.version, "0.4.0") {
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("user:")))
	}
}

func (s *e2bCloudSandbox) rpcUnary(ctx context.Context, service, method string, input, output any, defaultUser bool) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := s.envdRequest(ctx, http.MethodPost, "/"+service+"/"+method, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	if defaultUser {
		s.defaultUserHeader(req)
	}
	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return e2bReadConnectError(res)
	}
	if output == nil {
		_, _ = io.Copy(io.Discard, res.Body)
		return nil
	}
	if err := json.NewDecoder(res.Body).Decode(output); err != nil {
		return fmt.Errorf("dsh-e2b: decode Connect response: %w", err)
	}
	return nil
}

func e2bReadConnectError(res *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	err := &e2bConnectError{}
	if json.Unmarshal(data, err) != nil || (err.Code == "" && err.Message == "") {
		return &e2bHTTPError{Status: res.StatusCode, Message: strings.TrimSpace(string(data))}
	}
	return err
}

func (s *e2bCloudSandbox) processStream(ctx context.Context, input e2bProcessStart, defaultUser bool, connectTimeout time.Duration, onStdout, onStderr, onPTY func([]byte), pty bool) (E2BCommandHandle, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	handshakeTimer := time.AfterFunc(e2bRequestTimeout, cancel)
	req, err := s.envdRequest(streamCtx, http.MethodPost, "/process.Process/Start", bytes.NewReader(e2bEnvelope(0, data)))
	if err != nil {
		handshakeTimer.Stop()
		cancel()
		return nil, err
	}
	req.Header.Set("Content-Type", "application/connect+json")
	req.Header.Set("Connect-Protocol-Version", "1")
	req.Header.Set("Keepalive-Ping-Interval", "50")
	if connectTimeout > 0 {
		req.Header.Set("Connect-Timeout-Ms", strconv.FormatInt(connectTimeout.Milliseconds(), 10))
	}
	if defaultUser {
		s.defaultUserHeader(req)
	}
	res, err := s.client.Do(req)
	if err != nil {
		handshakeTimer.Stop()
		cancel()
		return nil, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		defer res.Body.Close()
		handshakeTimer.Stop()
		cancel()
		return nil, e2bReadConnectError(res)
	}
	flag, payload, err := e2bReadEnvelope(res.Body)
	if err != nil {
		handshakeTimer.Stop()
		res.Body.Close()
		cancel()
		return nil, err
	}
	if flag != 0 {
		handshakeTimer.Stop()
		res.Body.Close()
		cancel()
		return nil, e2bParseEndStream(flag, payload)
	}
	var event e2bProcessResponse
	if err := json.Unmarshal(payload, &event); err != nil {
		handshakeTimer.Stop()
		res.Body.Close()
		cancel()
		return nil, fmt.Errorf("dsh-e2b: decode process start: %w", err)
	}
	if event.Event.Start == nil || event.Event.Start.PID <= 0 {
		handshakeTimer.Stop()
		res.Body.Close()
		cancel()
		return nil, errors.New("dsh-e2b: expected process start event")
	}
	handshakeTimer.Stop()
	h := &e2bCloudCommandHandle{
		pid:      event.Event.Start.PID,
		sandbox:  s,
		pty:      pty,
		body:     res.Body,
		ctx:      streamCtx,
		cancel:   cancel,
		done:     make(chan struct{}),
		onStdout: onStdout,
		onStderr: onStderr,
		onPTY:    onPTY,
	}
	go h.consume()
	return h, nil
}

func e2bEnvelope(flag byte, data []byte) []byte {
	out := make([]byte, len(data)+5)
	out[0] = flag
	binary.BigEndian.PutUint32(out[1:5], uint32(len(data)))
	copy(out[5:], data)
	return out
}

func e2bReadEnvelope(r io.Reader) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	size := binary.BigEndian.Uint32(header[1:])
	if size > e2bConnectMaxMessage {
		return 0, nil, fmt.Errorf("dsh-e2b: Connect message exceeds %d bytes", e2bConnectMaxMessage)
	}
	data := make([]byte, size)
	_, err := io.ReadFull(r, data)
	return header[0], data, err
}

func e2bParseEndStream(flag byte, data []byte) error {
	if flag&1 != 0 {
		return errors.New("dsh-e2b: compressed Connect streams are unsupported")
	}
	if flag&2 == 0 {
		return fmt.Errorf("dsh-e2b: unexpected Connect envelope flags %d", flag)
	}
	var end struct {
		Error *e2bConnectError `json:"error"`
	}
	if err := json.Unmarshal(data, &end); err != nil {
		return fmt.Errorf("dsh-e2b: decode Connect end stream: %w", err)
	}
	if end.Error != nil {
		return end.Error
	}
	return nil
}

type e2bJSONInt64 int64

func (n *e2bJSONInt64) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return err
	}
	*n = e2bJSONInt64(value)
	return nil
}

type e2bCloudEntry struct {
	Name          string            `json:"name"`
	Type          json.RawMessage   `json:"type"`
	Path          string            `json:"path"`
	Size          e2bJSONInt64      `json:"size"`
	Mode          uint32            `json:"mode"`
	ModifiedTime  *time.Time        `json:"modifiedTime"`
	SymlinkTarget *string           `json:"symlinkTarget"`
	Metadata      map[string]string `json:"metadata"`
}

func (e e2bCloudEntry) public() (E2BEntryInfo, error) {
	typeID := E2BFileType("")
	var enumName string
	if len(e.Type) > 0 && e.Type[0] == '"' {
		if err := json.Unmarshal(e.Type, &enumName); err != nil {
			return E2BEntryInfo{}, err
		}
	} else if len(e.Type) > 0 {
		var enumValue int
		if err := json.Unmarshal(e.Type, &enumValue); err != nil {
			return E2BEntryInfo{}, err
		}
		enumName = strconv.Itoa(enumValue)
	}
	switch enumName {
	case "FILE_TYPE_FILE", "1", "file":
		typeID = E2BFile
	case "FILE_TYPE_DIRECTORY", "2", "dir", "directory":
		typeID = E2BDir
	default:
		return E2BEntryInfo{}, fmt.Errorf("dsh-e2b: unsupported file type %s", string(e.Type))
	}
	target := ""
	if e.SymlinkTarget != nil {
		typeID, target = E2BSymlink, *e.SymlinkTarget
	}
	modified := time.Time{}
	if e.ModifiedTime != nil {
		modified = *e.ModifiedTime
	}
	return E2BEntryInfo{
		Name: e.Name, Path: e.Path, Type: typeID, Size: int64(e.Size), Mode: os.FileMode(e.Mode),
		ModifiedTime: modified, SymlinkTarget: target, Metadata: cloneE2BMap(e.Metadata),
	}, nil
}

type e2bCloudFiles struct{ sandbox *e2bCloudSandbox }

func (f *e2bCloudFiles) Read(ctx context.Context, path string) ([]byte, error) {
	query := url.Values{"path": {path}}
	if !e2bVersionAtLeast(f.sandbox.version, "0.4.0") {
		query.Set("username", "user")
	}
	req, err := f.sandbox.envdRequest(ctx, http.MethodGet, "/files?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	res, err := f.sandbox.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, e2bReadHTTPError(res)
	}
	return io.ReadAll(res.Body)
}

func (f *e2bCloudFiles) Write(ctx context.Context, path string, data []byte, metadata map[string]string) (E2BEntryInfo, error) {
	if len(metadata) > 0 && !e2bVersionAtLeast(f.sandbox.version, "0.6.2") {
		return E2BEntryInfo{}, fmt.Errorf("dsh-e2b: file metadata requires envd 0.6.2 or later, got %s", f.sandbox.version)
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", path)
	if err != nil {
		return E2BEntryInfo{}, err
	}
	if _, err := part.Write(data); err != nil {
		return E2BEntryInfo{}, err
	}
	if err := writer.Close(); err != nil {
		return E2BEntryInfo{}, err
	}
	req, err := f.sandbox.envdRequest(ctx, http.MethodPost, "/files?"+url.Values{"path": {path}}.Encode(), &body)
	if err != nil {
		return E2BEntryInfo{}, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	for key, value := range metadata {
		if key == "" || !httpguts.ValidHeaderFieldName("X-Metadata-"+key) {
			return E2BEntryInfo{}, fmt.Errorf("dsh-e2b: invalid metadata key %q", key)
		}
		if !e2bPrintableASCII(value) || !httpguts.ValidHeaderFieldValue(value) {
			return E2BEntryInfo{}, fmt.Errorf("dsh-e2b: invalid metadata value for %q", key)
		}
		req.Header.Set("X-Metadata-"+key, value)
	}
	res, err := f.sandbox.client.Do(req)
	if err != nil {
		return E2BEntryInfo{}, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		defer res.Body.Close()
		return E2BEntryInfo{}, e2bReadHTTPError(res)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	res.Body.Close()
	return f.GetInfo(ctx, path)
}

func e2bPrintableASCII(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

func (f *e2bCloudFiles) List(ctx context.Context, path string, depth int) ([]E2BEntryInfo, error) {
	var response struct {
		Entries []e2bCloudEntry `json:"entries"`
	}
	if err := f.sandbox.rpcUnary(ctx, "filesystem.Filesystem", "ListDir", struct {
		Path  string `json:"path"`
		Depth int    `json:"depth"`
	}{path, depth}, &response, true); err != nil {
		return nil, err
	}
	entries := make([]E2BEntryInfo, 0, len(response.Entries))
	for _, entry := range response.Entries {
		mapped, err := entry.public()
		if err != nil {
			return nil, err
		}
		entries = append(entries, mapped)
	}
	return entries, nil
}

func (f *e2bCloudFiles) MakeDir(ctx context.Context, path string) error {
	err := f.sandbox.rpcUnary(ctx, "filesystem.Filesystem", "MakeDir", struct {
		Path string `json:"path"`
	}{path}, nil, true)
	var connectErr *e2bConnectError
	if errors.As(err, &connectErr) && connectErr.Code == "already_exists" {
		return nil
	}
	return err
}

func (f *e2bCloudFiles) Rename(ctx context.Context, oldPath, newPath string) (E2BEntryInfo, error) {
	var response struct {
		Entry *e2bCloudEntry `json:"entry"`
	}
	if err := f.sandbox.rpcUnary(ctx, "filesystem.Filesystem", "Move", struct {
		Source      string `json:"source"`
		Destination string `json:"destination"`
	}{oldPath, newPath}, &response, true); err != nil {
		return E2BEntryInfo{}, err
	}
	if response.Entry == nil {
		return E2BEntryInfo{}, errors.New("dsh-e2b: move returned no entry")
	}
	return response.Entry.public()
}

func (f *e2bCloudFiles) Remove(ctx context.Context, path string) error {
	return f.sandbox.rpcUnary(ctx, "filesystem.Filesystem", "Remove", struct {
		Path string `json:"path"`
	}{path}, nil, true)
}

func (f *e2bCloudFiles) GetInfo(ctx context.Context, path string) (E2BEntryInfo, error) {
	var response struct {
		Entry *e2bCloudEntry `json:"entry"`
	}
	if err := f.sandbox.rpcUnary(ctx, "filesystem.Filesystem", "Stat", struct {
		Path string `json:"path"`
	}{path}, &response, true); err != nil {
		return E2BEntryInfo{}, err
	}
	if response.Entry == nil {
		return E2BEntryInfo{}, errors.New("dsh-e2b: stat returned no entry")
	}
	return response.Entry.public()
}

type e2bProcessStart struct {
	Process e2bProcessConfig `json:"process"`
	PTY     *e2bPTYConfig    `json:"pty,omitempty"`
	Stdin   bool             `json:"stdin,omitempty"`
}

type e2bProcessConfig struct {
	Cmd  string            `json:"cmd"`
	Args []string          `json:"args,omitempty"`
	Envs map[string]string `json:"envs,omitempty"`
	CWD  string            `json:"cwd,omitempty"`
}

type e2bPTYConfig struct {
	Size struct {
		Cols int `json:"cols"`
		Rows int `json:"rows"`
	} `json:"size"`
}

type e2bProcessResponse struct {
	Event struct {
		Start *struct {
			PID int `json:"pid"`
		} `json:"start,omitempty"`
		Data *struct {
			Stdout *string `json:"stdout,omitempty"`
			Stderr *string `json:"stderr,omitempty"`
			PTY    *string `json:"pty,omitempty"`
		} `json:"data,omitempty"`
		End *struct {
			ExitCode int    `json:"exitCode"`
			Error    string `json:"error,omitempty"`
		} `json:"end,omitempty"`
	} `json:"event"`
}

type e2bCloudCommands struct{ sandbox *e2bCloudSandbox }

func (c *e2bCloudCommands) Run(ctx context.Context, command string, options E2BCommandOptions) (E2BCommandResult, error) {
	handle, err := c.sandbox.processStream(ctx, e2bProcessStart{Process: e2bProcessConfig{
		Cmd: "/bin/bash", Args: []string{"-l", "-c", command}, Envs: options.Env, CWD: options.CWD,
	}, Stdin: options.Stdin}, true, time.Minute, nil, nil, nil, false)
	if err != nil {
		return E2BCommandResult{}, err
	}
	return handle.Wait()
}

func (c *e2bCloudCommands) Start(ctx context.Context, argv []string, options E2BCommandOptions, onStdout, onStderr func([]byte)) (E2BCommandHandle, error) {
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return nil, errors.New("dsh-e2b: empty argv")
	}
	words := make([]string, len(argv))
	for i, arg := range argv {
		if strings.IndexByte(arg, 0) >= 0 {
			return nil, errors.New("dsh-e2b: argv contains NUL")
		}
		words[i] = e2bQuote(arg)
	}
	return c.sandbox.processStream(ctx, e2bProcessStart{Process: e2bProcessConfig{
		Cmd: "/bin/bash", Args: []string{"-l", "-c", "exec " + strings.Join(words, " ")}, Envs: options.Env, CWD: options.CWD,
	}, Stdin: options.Stdin}, true, 0, onStdout, onStderr, nil, false)
}

type e2bCloudPTY struct{ sandbox *e2bCloudSandbox }

func (p *e2bCloudPTY) Create(ctx context.Context, options E2BPTYOptions, onData func([]byte)) (E2BCommandHandle, error) {
	envs := cloneE2BMap(options.Env)
	if envs == nil {
		envs = map[string]string{}
	}
	if envs["TERM"] == "" {
		envs["TERM"] = "xterm-256color"
	}
	if envs["LANG"] == "" {
		envs["LANG"] = "C.UTF-8"
	}
	if envs["LC_ALL"] == "" {
		envs["LC_ALL"] = "C.UTF-8"
	}
	pty := &e2bPTYConfig{}
	pty.Size.Cols, pty.Size.Rows = options.Cols, options.Rows
	return p.sandbox.processStream(ctx, e2bProcessStart{Process: e2bProcessConfig{
		Cmd: "/bin/bash", Args: []string{"-i", "-l"}, Envs: envs, CWD: options.CWD,
	}, PTY: pty}, true, 0, nil, nil, onData, true)
}

func (p *e2bCloudPTY) SendInput(ctx context.Context, pid int, data []byte) error {
	return p.sandbox.sendInput(ctx, pid, data, true)
}

type e2bCloudCommandHandle struct {
	pid      int
	sandbox  *e2bCloudSandbox
	pty      bool
	body     io.ReadCloser
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	onStdout func([]byte)
	onStderr func([]byte)
	onPTY    func([]byte)
	result   E2BCommandResult
	err      error
	close    sync.Once
}

func (h *e2bCloudCommandHandle) PID() int { return h.pid }

func (h *e2bCloudCommandHandle) Wait() (E2BCommandResult, error) {
	<-h.done
	return h.result, h.err
}

func (h *e2bCloudCommandHandle) SendStdin(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), e2bRequestTimeout)
	defer cancel()
	return h.sandbox.sendInput(ctx, h.pid, data, h.pty)
}

func (h *e2bCloudCommandHandle) CloseStdin() error {
	if h.pty {
		return errors.New("dsh-e2b: closing PTY stdin is unsupported; send Ctrl-D instead")
	}
	if !e2bVersionAtLeast(h.sandbox.version, "0.5.2") {
		return fmt.Errorf("dsh-e2b: close stdin requires envd 0.5.2 or later, got %s", h.sandbox.version)
	}
	ctx, cancel := context.WithTimeout(context.Background(), e2bRequestTimeout)
	defer cancel()
	return h.sandbox.rpcUnary(ctx, "process.Process", "CloseStdin", map[string]any{
		"process": map[string]any{"pid": h.pid},
	}, nil, false)
}

func (h *e2bCloudCommandHandle) Kill() error {
	ctx, cancel := context.WithTimeout(context.Background(), e2bRequestTimeout)
	defer cancel()
	err := h.sandbox.rpcUnary(ctx, "process.Process", "SendSignal", map[string]any{
		"process": map[string]any{"pid": h.pid}, "signal": "SIGNAL_SIGKILL",
	}, nil, false)
	var connectErr *e2bConnectError
	if errors.As(err, &connectErr) && connectErr.Code == "not_found" {
		return nil
	}
	return err
}

func (h *e2bCloudCommandHandle) Disconnect() error {
	h.close.Do(func() {
		h.cancel()
		_ = h.body.Close()
	})
	return nil
}

func (h *e2bCloudCommandHandle) consume() {
	defer func() {
		h.close.Do(func() {
			h.cancel()
			_ = h.body.Close()
		})
		close(h.done)
	}()
	var stdout, stderr bytes.Buffer
	hasResult := false
	for {
		flag, payload, err := e2bReadEnvelope(h.body)
		if err != nil {
			if h.ctx.Err() != nil {
				h.err = h.ctx.Err()
			} else {
				h.err = fmt.Errorf("dsh-e2b: read process stream: %w", err)
			}
			return
		}
		if flag != 0 {
			if err := e2bParseEndStream(flag, payload); err != nil {
				h.err = err
				return
			}
			if !hasResult {
				h.err = errors.New("dsh-e2b: process exited without a result")
			} else if h.result.ExitCode != 0 {
				h.err = &E2BCommandError{ExitCode: h.result.ExitCode, Stdout: h.result.Stdout, Stderr: h.result.Stderr}
			}
			return
		}
		var event e2bProcessResponse
		if err := json.Unmarshal(payload, &event); err != nil {
			h.err = fmt.Errorf("dsh-e2b: decode process event: %w", err)
			return
		}
		if event.Event.Data != nil {
			if event.Event.Data.Stdout != nil {
				chunk, err := e2bDecodeBytes(*event.Event.Data.Stdout)
				if err != nil {
					h.err = err
					return
				}
				stdout.Write(chunk)
				if h.onStdout != nil {
					h.onStdout(chunk)
				}
			}
			if event.Event.Data.Stderr != nil {
				chunk, err := e2bDecodeBytes(*event.Event.Data.Stderr)
				if err != nil {
					h.err = err
					return
				}
				stderr.Write(chunk)
				if h.onStderr != nil {
					h.onStderr(chunk)
				}
			}
			if event.Event.Data.PTY != nil {
				chunk, err := e2bDecodeBytes(*event.Event.Data.PTY)
				if err != nil {
					h.err = err
					return
				}
				if h.onPTY != nil {
					h.onPTY(chunk)
				}
			}
		}
		if event.Event.End != nil {
			h.result = E2BCommandResult{
				ExitCode: event.Event.End.ExitCode, Stdout: stdout.String(), Stderr: stderr.String(),
			}
			if event.Event.End.Error != "" {
				h.result.Stderr += event.Event.End.Error
			}
			hasResult = true
		}
	}
}

func e2bDecodeBytes(value string) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(value)
	if err == nil {
		return data, nil
	}
	data, rawErr := base64.RawStdEncoding.DecodeString(value)
	if rawErr != nil {
		return nil, fmt.Errorf("dsh-e2b: decode process output: %w", err)
	}
	return data, nil
}

func (s *e2bCloudSandbox) sendInput(ctx context.Context, pid int, data []byte, pty bool) error {
	field := "stdin"
	if pty {
		field = "pty"
	}
	return s.rpcUnary(ctx, "process.Process", "SendInput", map[string]any{
		"process": map[string]any{"pid": pid},
		"input":   map[string]any{field: base64.StdEncoding.EncodeToString(data)},
	}, nil, false)
}

func e2bVersionAtLeast(version, minimum string) bool {
	parse := func(value string) [3]int {
		var out [3]int
		value = strings.SplitN(value, "-", 2)[0]
		for i, part := range strings.Split(value, ".") {
			if i == len(out) {
				break
			}
			out[i], _ = strconv.Atoi(part)
		}
		return out
	}
	a, b := parse(version), parse(minimum)
	for i := range a {
		if a[i] != b[i] {
			return a[i] > b[i]
		}
	}
	return true
}
