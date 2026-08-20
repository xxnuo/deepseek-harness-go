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
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const e2bTestAPIKey = "e2b_0000000000000000000000000000000000000000"

type e2bProtocolFile struct {
	data     []byte
	metadata map[string]string
	dir      bool
}

type e2bProtocolFixture struct {
	t             *testing.T
	server        *httptest.Server
	mu            sync.Mutex
	files         map[string]e2bProtocolFile
	commandKilled chan struct{}
	ptyInput      chan struct{}
	killOnce      sync.Once
	ptyOnce       sync.Once
	killed        bool
	stdin         [][]byte
	stdinClosed   bool
	ptyInputs     [][]byte
	failMakeDir   string
}

func newE2BProtocolFixture(t *testing.T) *e2bProtocolFixture {
	f := &e2bProtocolFixture{
		t: t, files: map[string]e2bProtocolFile{}, commandKilled: make(chan struct{}), ptyInput: make(chan struct{}),
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.server.Close)
	return f
}

func (f *e2bProtocolFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/api/sandboxes" {
		f.serveAPI(w, r)
		return
	}
	if r.Header.Get("X-API-KEY") != "" {
		f.t.Errorf("API key leaked to envd request %s", r.URL.Path)
	}
	if got := r.Header.Get("E2b-Sandbox-Id"); got != "sandbox-1" {
		f.t.Errorf("sandbox id header = %q", got)
	}
	if got := r.Header.Get("E2b-Sandbox-Port"); got != "49983" {
		f.t.Errorf("sandbox port header = %q", got)
	}
	if got := r.Header.Get("X-Access-Token"); got != "envd-token" {
		f.t.Errorf("access token header = %q", got)
	}
	switch {
	case r.URL.Path == "/envd/files":
		f.serveFiles(w, r)
	case strings.HasPrefix(r.URL.Path, "/envd/filesystem.Filesystem/"):
		f.serveFilesystemRPC(w, r)
	case strings.HasPrefix(r.URL.Path, "/envd/process.Process/"):
		f.serveProcessRPC(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (f *e2bProtocolFixture) serveAPI(w http.ResponseWriter, r *http.Request) {
	if got := r.Header.Get("X-API-KEY"); got != e2bTestAPIKey {
		f.t.Errorf("API key = %q", got)
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/sandboxes":
		var body struct {
			TemplateID          string `json:"templateID"`
			Timeout             int    `json:"timeout"`
			Secure              bool   `json:"secure"`
			AllowInternetAccess bool   `json:"allow_internet_access"`
			AutoPause           bool   `json:"autoPause"`
			AutoResume          struct {
				Enabled bool `json:"enabled"`
			} `json:"autoResume"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			f.t.Errorf("decode create: %v", err)
		}
		if body.TemplateID != "base" || body.Timeout != 300 || !body.Secure || !body.AllowInternetAccess || body.AutoPause || body.AutoResume.Enabled {
			f.t.Errorf("unexpected create body: %#v", body)
		}
		writeE2BJSON(w, http.StatusCreated, map[string]any{
			"sandboxID": "sandbox-1", "domain": "example.invalid", "envdVersion": "0.6.3", "envdAccessToken": "envd-token",
		})
	case r.Method == http.MethodDelete && r.URL.Path == "/api/sandboxes/sandbox-1":
		f.mu.Lock()
		f.killed = true
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (f *e2bProtocolFixture) serveFiles(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Query().Get("path")
	switch r.Method {
	case http.MethodGet:
		f.mu.Lock()
		file, ok := f.files[target]
		f.mu.Unlock()
		if !ok || file.dir {
			writeE2BJSON(w, http.StatusNotFound, map[string]string{"message": "not found"})
			return
		}
		_, _ = w.Write(file.data)
	case http.MethodPost:
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data;") {
			f.t.Errorf("upload content type = %q", r.Header.Get("Content-Type"))
		}
		reader, err := r.MultipartReader()
		if err != nil {
			f.t.Errorf("multipart reader: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		part, err := reader.NextPart()
		if err != nil {
			f.t.Errorf("multipart part: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if part.FormName() != "file" || part.FileName() != path.Base(target) || !strings.Contains(part.Header.Get("Content-Disposition"), `filename="`+target+`"`) {
			f.t.Errorf("multipart part name=%q filename=%q disposition=%q target=%q", part.FormName(), part.FileName(), part.Header.Get("Content-Disposition"), target)
		}
		data, _ := io.ReadAll(part)
		metadata := map[string]string{}
		for key, values := range r.Header {
			if strings.HasPrefix(strings.ToLower(key), "x-metadata-") {
				metadata[strings.TrimPrefix(strings.ToLower(key), "x-metadata-")] = values[0]
			}
		}
		f.mu.Lock()
		f.files[target] = e2bProtocolFile{data: data, metadata: metadata}
		f.mu.Unlock()
		writeE2BJSON(w, http.StatusOK, []map[string]any{{"name": path.Base(target), "path": target, "type": "file", "metadata": metadata}})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *e2bProtocolFixture) serveFilesystemRPC(w http.ResponseWriter, r *http.Request) {
	f.requireUnaryRPC(r)
	method := path.Base(r.URL.Path)
	var request map[string]any
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		f.t.Errorf("decode %s request: %v", method, err)
		writeE2BConnectError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	stringField := func(name string) string {
		value, _ := request[name].(string)
		return value
	}
	switch method {
	case "MakeDir":
		target := stringField("path")
		f.mu.Lock()
		failMakeDir := f.failMakeDir
		f.mu.Unlock()
		if target == failMakeDir {
			writeE2BConnectError(w, http.StatusInternalServerError, "internal", "mkdir failed")
			return
		}
		f.mu.Lock()
		f.files[target] = e2bProtocolFile{dir: true}
		f.mu.Unlock()
		writeE2BJSON(w, http.StatusOK, map[string]any{})
	case "Stat":
		target := stringField("path")
		f.mu.Lock()
		file, ok := f.files[target]
		f.mu.Unlock()
		if !ok {
			writeE2BConnectError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		writeE2BJSON(w, http.StatusOK, map[string]any{"entry": e2bProtocolEntry(target, file)})
	case "ListDir":
		directory := stringField("path")
		if depth, ok := request["depth"].(float64); !ok || depth != 1 {
			f.t.Errorf("list depth = %#v", request["depth"])
		}
		f.mu.Lock()
		entries := make([]map[string]any, 0)
		for name, file := range f.files {
			if name != directory && path.Dir(name) == directory {
				entries = append(entries, e2bProtocolEntry(name, file))
			}
		}
		f.mu.Unlock()
		sort.Slice(entries, func(i, j int) bool { return entries[i]["path"].(string) < entries[j]["path"].(string) })
		writeE2BJSON(w, http.StatusOK, map[string]any{"entries": entries})
	case "Move":
		source, destination := stringField("source"), stringField("destination")
		f.mu.Lock()
		file, ok := f.files[source]
		if ok {
			delete(f.files, source)
			f.files[destination] = file
		}
		f.mu.Unlock()
		if !ok {
			writeE2BConnectError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		writeE2BJSON(w, http.StatusOK, map[string]any{"entry": e2bProtocolEntry(destination, file)})
	case "Remove":
		f.mu.Lock()
		delete(f.files, stringField("path"))
		f.mu.Unlock()
		writeE2BJSON(w, http.StatusOK, map[string]any{})
	default:
		http.NotFound(w, r)
	}
}

func e2bProtocolEntry(name string, file e2bProtocolFile) map[string]any {
	typeName, mode := "FILE_TYPE_FILE", 384
	if file.dir {
		typeName, mode = "FILE_TYPE_DIRECTORY", 448
	}
	return map[string]any{
		"name": path.Base(name), "type": typeName, "path": name, "size": fmt.Sprint(len(file.data)), "mode": mode,
		"permissions": "rw-------", "owner": "user", "group": "user", "modifiedTime": "2026-08-20T00:00:00Z", "metadata": file.metadata,
	}
}

func (f *e2bProtocolFixture) serveProcessRPC(w http.ResponseWriter, r *http.Request) {
	method := path.Base(r.URL.Path)
	if method == "Start" {
		f.serveProcessStart(w, r)
		return
	}
	f.requireUnaryRPC(r)
	var request struct {
		Process struct {
			PID int `json:"pid"`
		} `json:"process"`
		Input  map[string]string `json:"input"`
		Signal string            `json:"signal"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		f.t.Errorf("decode process %s: %v", method, err)
	}
	switch method {
	case "SendInput":
		for kind, encoded := range request.Input {
			data, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				f.t.Errorf("decode %s input: %v", kind, err)
			}
			f.mu.Lock()
			if kind == "pty" {
				f.ptyInputs = append(f.ptyInputs, data)
			} else {
				f.stdin = append(f.stdin, data)
			}
			f.mu.Unlock()
			if kind == "pty" {
				f.ptyOnce.Do(func() { close(f.ptyInput) })
			}
		}
		writeE2BJSON(w, http.StatusOK, map[string]any{})
	case "CloseStdin":
		f.mu.Lock()
		f.stdinClosed = true
		f.mu.Unlock()
		writeE2BJSON(w, http.StatusOK, map[string]any{})
	case "SendSignal":
		if request.Signal != "SIGNAL_SIGKILL" {
			f.t.Errorf("signal = %q", request.Signal)
		}
		f.killOnce.Do(func() { close(f.commandKilled) })
		writeE2BJSON(w, http.StatusOK, map[string]any{})
	default:
		http.NotFound(w, r)
	}
}

func (f *e2bProtocolFixture) serveProcessStart(w http.ResponseWriter, r *http.Request) {
	if got := r.Header.Get("Content-Type"); got != "application/connect+json" {
		f.t.Errorf("stream content type = %q", got)
	}
	if got := r.Header.Get("Connect-Protocol-Version"); got != "1" {
		f.t.Errorf("stream protocol version = %q", got)
	}
	if got := r.Header.Get("Keepalive-Ping-Interval"); got != "50" {
		f.t.Errorf("keepalive header = %q", got)
	}
	payload, err := readE2BTestEnvelope(r.Body)
	if err != nil {
		f.t.Errorf("read start envelope: %v", err)
		return
	}
	var request e2bProcessStart
	if err := json.Unmarshal(payload, &request); err != nil {
		f.t.Errorf("decode start: %v", err)
		return
	}
	pid := 101
	mode := "run"
	command := strings.Join(request.Process.Args, " ")
	if request.PTY != nil {
		pid, mode = 301, "pty"
		if request.Process.Cmd != "/bin/bash" || strings.Join(request.Process.Args, "\x00") != "-i\x00-l" || request.PTY.Size.Cols != 80 || request.PTY.Size.Rows != 24 {
			f.t.Errorf("unexpected PTY request: %#v", request)
		}
	} else if strings.Contains(command, "disconnect-probe") {
		pid, mode = 203, "disconnect"
	} else if strings.Contains(command, "exec ") {
		pid, mode = 202, "start"
		if !request.Stdin || request.Process.CWD != "/home/user/workspace" {
			f.t.Errorf("unexpected command start: %#v", request)
		}
	} else if request.Process.Cmd != "/bin/bash" || command != "-l -c printf run-ok" {
		f.t.Errorf("unexpected command run: %#v", request)
	}
	if got := r.Header.Get("Connect-Timeout-Ms"); (mode == "run" && got != "60000") || (mode != "run" && got != "") {
		f.t.Errorf("connect timeout for %s = %q", mode, got)
	}
	w.Header().Set("Content-Type", "application/connect+json")
	w.WriteHeader(http.StatusOK)
	writeE2BEnvelope(w, 0, map[string]any{"event": map[string]any{"start": map[string]any{"pid": pid}}})
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	switch mode {
	case "run":
		writeE2BProcessEnd(w, "stdout", []byte("run-ok"), 0)
	case "start":
		select {
		case <-f.commandKilled:
			writeE2BProcessEnd(w, "stdout", []byte("start-ok"), 137)
		case <-r.Context().Done():
		}
	case "disconnect":
		<-r.Context().Done()
	case "pty":
		select {
		case <-f.ptyInput:
			writeE2BProcessEnd(w, "pty", []byte("pty-ok"), 0)
		case <-r.Context().Done():
		}
	}
}

func (f *e2bProtocolFixture) requireUnaryRPC(r *http.Request) {
	if got := r.Header.Get("Content-Type"); got != "application/json" {
		f.t.Errorf("unary content type = %q for %s", got, r.URL.Path)
	}
	if got := r.Header.Get("Connect-Protocol-Version"); got != "1" {
		f.t.Errorf("unary protocol version = %q for %s", got, r.URL.Path)
	}
}

func writeE2BProcessEnd(w http.ResponseWriter, stream string, data []byte, exitCode int) {
	writeE2BEnvelope(w, 0, map[string]any{"event": map[string]any{"data": map[string]any{stream: base64.StdEncoding.EncodeToString(data)}}})
	writeE2BEnvelope(w, 0, map[string]any{"event": map[string]any{"end": map[string]any{"exitCode": exitCode, "exited": true}}})
	writeE2BEnvelope(w, 2, map[string]any{})
}

func writeE2BEnvelope(w io.Writer, flag byte, payload any) {
	data, _ := json.Marshal(payload)
	_, _ = w.Write(e2bEnvelope(flag, data))
}

func readE2BTestEnvelope(r io.Reader) ([]byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	if header[0] != 0 {
		return nil, fmt.Errorf("flags %d", header[0])
	}
	data := make([]byte, binary.BigEndian.Uint32(header[1:]))
	_, err := io.ReadFull(r, data)
	return data, err
}

func writeE2BConnectError(w http.ResponseWriter, status int, code, message string) {
	writeE2BJSON(w, status, map[string]string{"code": code, "message": message})
}

func writeE2BJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func TestE2BCloudProtocol(t *testing.T) {
	fixture := newE2BProtocolFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runtime, err := NewE2BRuntime(ctx, E2BConfig{
		APIKey: e2bTestAPIKey, APIURL: fixture.server.URL + "/api", SandboxURL: fixture.server.URL + "/envd",
		CWD: "/home/user/workspace", Timeout: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	sandbox, err := runtime.GetSandbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sandbox.ID() != "sandbox-1" {
		t.Fatalf("sandbox id = %q", sandbox.ID())
	}

	entry, err := sandbox.Files().Write(ctx, "/home/user/workspace/a.txt", []byte("hello"), map[string]string{"dsh-version": "v1"})
	if err != nil {
		t.Fatal(err)
	}
	if entry.Size != 5 || entry.Metadata["dsh-version"] != "v1" {
		t.Fatalf("write entry = %#v", entry)
	}
	data, err := sandbox.Files().Read(ctx, entry.Path)
	if err != nil || string(data) != "hello" {
		t.Fatalf("read = %q, %v", data, err)
	}
	listed, err := sandbox.Files().List(ctx, "/home/user/workspace", 1)
	if err != nil || len(listed) != 2 {
		t.Fatalf("list = %#v, %v", listed, err)
	}
	if err := sandbox.Files().MakeDir(ctx, "/home/user/workspace/sub"); err != nil {
		t.Fatal(err)
	}
	renamed, err := sandbox.Files().Rename(ctx, entry.Path, "/home/user/workspace/b.txt")
	if err != nil || renamed.Path != "/home/user/workspace/b.txt" {
		t.Fatalf("rename = %#v, %v", renamed, err)
	}
	info, err := sandbox.Files().GetInfo(ctx, renamed.Path)
	if err != nil || info.Size != 5 {
		t.Fatalf("info = %#v, %v", info, err)
	}
	if err := sandbox.Files().Remove(ctx, renamed.Path); err != nil {
		t.Fatal(err)
	}

	result, err := sandbox.Commands().Run(ctx, "printf run-ok", E2BCommandOptions{CWD: "/home/user/workspace"})
	if err != nil || result.ExitCode != 0 || result.Stdout != "run-ok" {
		t.Fatalf("run = %#v, %v", result, err)
	}
	var stdout bytes.Buffer
	handle, err := sandbox.Commands().Start(ctx, []string{"/bin/cat", "two words"}, E2BCommandOptions{
		CWD: "/home/user/workspace", Stdin: true,
	}, func(data []byte) { stdout.Write(data) }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.SendStdin([]byte("input")); err != nil {
		t.Fatal(err)
	}
	if err := handle.CloseStdin(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Kill(); err != nil {
		t.Fatal(err)
	}
	result, err = handle.Wait()
	if result.ExitCode != 137 || stdout.String() != "start-ok" {
		t.Fatalf("start = %#v, %v, stdout=%q", result, err, stdout.String())
	}
	var exitErr *E2BCommandError
	if !errors.As(err, &exitErr) {
		t.Fatalf("start error = %v", err)
	}
	fixture.mu.Lock()
	stdinOK := len(fixture.stdin) == 1 && string(fixture.stdin[0]) == "input" && fixture.stdinClosed
	fixture.mu.Unlock()
	if !stdinOK {
		t.Fatal("stdin or closeStdin was not transported")
	}

	disconnected, err := sandbox.Commands().Start(ctx, []string{"/bin/sh", "-c", "disconnect-probe"}, E2BCommandOptions{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := disconnected.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if _, err := disconnected.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("disconnect wait error = %v", err)
	}

	var ptyOutput bytes.Buffer
	terminal, err := sandbox.PTY().Create(ctx, E2BPTYOptions{Rows: 24, Cols: 80, CWD: "/home/user/workspace"}, func(data []byte) {
		ptyOutput.Write(data)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := terminal.SendStdin([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := sandbox.PTY().SendInput(ctx, terminal.PID(), []byte("second")); err != nil {
		t.Fatal(err)
	}
	result, err = terminal.Wait()
	if err != nil || result.ExitCode != 0 || ptyOutput.String() != "pty-ok" {
		t.Fatalf("pty = %#v, %v, output=%q", result, err, ptyOutput.String())
	}
	fixture.mu.Lock()
	ptyInputs := len(fixture.ptyInputs)
	fixture.mu.Unlock()
	if ptyInputs != 2 {
		t.Fatalf("PTY inputs = %d", ptyInputs)
	}

	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	killed := fixture.killed
	fixture.mu.Unlock()
	if !killed {
		t.Fatal("sandbox was not killed")
	}
}

func TestE2BCloudSetupFailureKillsSandbox(t *testing.T) {
	fixture := newE2BProtocolFixture(t)
	fixture.mu.Lock()
	fixture.failMakeDir = "/home/user/workspace"
	fixture.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime, err := NewE2BRuntime(ctx, E2BConfig{
		APIKey: e2bTestAPIKey, APIURL: fixture.server.URL + "/api", SandboxURL: fixture.server.URL + "/envd",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.GetSandbox(ctx); err == nil || !strings.Contains(err.Error(), "mkdir failed") {
		t.Fatalf("setup error = %v", err)
	}
	fixture.mu.Lock()
	killed := fixture.killed
	fixture.mu.Unlock()
	if !killed {
		t.Fatal("failed sandbox setup was not killed")
	}
}

func TestE2BCloudSelectionPreservesOverrides(t *testing.T) {
	ctx := context.Background()
	local, err := NewE2BRuntime(ctx, E2BConfig{
		APIKey: e2bTestAPIKey, APIURL: "://unused", LocalRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.GetSandbox(ctx); err != nil {
		t.Fatal(err)
	}
	_ = local.Close(ctx)

	called := false
	factoryRoot := t.TempDir()
	custom, err := NewE2BRuntime(ctx, E2BConfig{
		APIKey: e2bTestAPIKey, APIURL: "://unused", LocalRoot: t.TempDir(),
		Factory: func(ctx context.Context, config E2BConfig) (E2BSandbox, error) {
			called = true
			return NewLocalSandboxFactory(factoryRoot)(ctx, config)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := custom.GetSandbox(ctx); err != nil {
		t.Fatal(err)
	}
	_ = custom.Close(ctx)
	if !called {
		t.Fatal("custom factory was not selected")
	}
}

func TestE2BCloudLive(t *testing.T) {
	apiKey := os.Getenv("E2B_API_KEY")
	if apiKey == "" {
		t.Skip("E2B_API_KEY is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runtime, err := NewE2BRuntime(ctx, E2BConfig{APIKey: apiKey, CWD: "/home/user/workspace", Timeout: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = runtime.Close(closeCtx)
	})
	sandbox, err := runtime.GetSandbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	directory := fmt.Sprintf("/home/user/workspace/.dsh-e2b-live-%d", time.Now().UnixNano())
	if err := sandbox.Files().MakeDir(ctx, directory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sandbox.Files().Remove(context.Background(), directory) })
	file := directory + "/a.txt"
	if _, err := sandbox.Files().Write(ctx, file, []byte("live"), map[string]string{"dsh-live": "1"}); err != nil {
		t.Fatal(err)
	}
	if data, err := sandbox.Files().Read(ctx, file); err != nil || string(data) != "live" {
		t.Fatalf("live read = %q, %v", data, err)
	}
	renamed := directory + "/b.txt"
	if _, err := sandbox.Files().Rename(ctx, file, renamed); err != nil {
		t.Fatal(err)
	}
	if entries, err := sandbox.Files().List(ctx, directory, 1); err != nil || len(entries) != 1 {
		t.Fatalf("live list = %#v, %v", entries, err)
	}
	if _, err := sandbox.Files().GetInfo(ctx, renamed); err != nil {
		t.Fatal(err)
	}
	if err := sandbox.Files().Remove(ctx, renamed); err != nil {
		t.Fatal(err)
	}
	if result, err := sandbox.Commands().Run(ctx, "printf live-run-ok", E2BCommandOptions{CWD: directory}); err != nil || result.Stdout != "live-run-ok" {
		t.Fatalf("live run = %#v, %v", result, err)
	}
	handle, err := sandbox.Commands().Start(ctx, []string{"/bin/sh", "-c", "IFS= read -r line; printf '%s' \"$line\""}, E2BCommandOptions{CWD: directory, Stdin: true}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.SendStdin([]byte("live-stdin-ok\n")); err != nil {
		t.Fatal(err)
	}
	if err := handle.CloseStdin(); err != nil {
		t.Fatal(err)
	}
	if result, err := handle.Wait(); err != nil || result.Stdout != "live-stdin-ok" {
		t.Fatalf("live start = %#v, %v", result, err)
	}
	var mu sync.Mutex
	var output bytes.Buffer
	terminal, err := sandbox.PTY().Create(ctx, E2BPTYOptions{Rows: 24, Cols: 80, CWD: directory}, func(data []byte) {
		mu.Lock()
		output.Write(data)
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := terminal.SendStdin([]byte("printf live-pty-ok\\n\nexit\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := terminal.Wait(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	ptyText := output.String()
	mu.Unlock()
	if !strings.Contains(ptyText, "live-pty-ok") {
		t.Fatalf("live PTY output = %q", ptyText)
	}
}
