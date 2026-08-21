package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	LSPGoToDefinition     = "goToDefinition"
	LSPFindReferences     = "findReferences"
	LSPGoToImplementation = "goToImplementation"
	LSPHover              = "hover"

	defaultLSPMaxMessageBytes  = 16_000_000
	defaultLSPMaxStderrBytes   = 1_000_000
	defaultLSPMaxDocumentBytes = 4_000_000
	defaultLSPShutdownTimeout  = 5 * time.Second
	defaultLSPKillGrace        = 2 * time.Second
	defaultLSPToolTimeout      = 60 * time.Second
	defaultLSPMaxLocations     = 100
	defaultLSPMaxResultChars   = 16_000
)

var lspOperations = map[string]string{
	LSPGoToDefinition:     "textDocument/definition",
	LSPFindReferences:     "textDocument/references",
	LSPGoToImplementation: "textDocument/implementation",
	LSPHover:              "textDocument/hover",
}

type LSPPosition struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

type LSPRange struct {
	Start LSPPosition `json:"start"`
	End   LSPPosition `json:"end"`
}

type LSPLocation struct {
	URI   string   `json:"uri"`
	Range LSPRange `json:"range"`
}

type LSPHoverResult struct {
	Contents string    `json:"contents"`
	Range    *LSPRange `json:"range,omitempty"`
}

type LSPQuery struct {
	Operation     string      `json:"operation"`
	FilePath      string      `json:"filePath"`
	Position      LSPPosition `json:"position"`
	WorkspaceRoot string      `json:"workspaceRoot"`
}

type LSPProviderQuery struct {
	LSPQuery
	LanguageID string `json:"languageId"`
}

type LSPQueryResult struct {
	Kind                 string          `json:"kind"`
	Locations            []LSPLocation   `json:"locations,omitempty"`
	ResolvedWorkspaceURI string          `json:"resolvedWorkspaceUri,omitempty"`
	Hover                *LSPHoverResult `json:"hover,omitempty"`
}

type LSPError struct {
	Code    string
	Message string
	Err     error
}

func (e *LSPError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

func (e *LSPError) Unwrap() error { return e.Err }

type LSPProvider interface {
	ID() string
	Extensions() map[string]string
	Query(context.Context, LSPProviderQuery) (LSPQueryResult, error)
}

type LSPStdioConfig struct {
	Command               string            `json:"command" yaml:"command"`
	Args                  []string          `json:"args,omitempty" yaml:"args,omitempty"`
	Env                   map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	ExtensionToLanguage   map[string]string `json:"extensionToLanguage" yaml:"extensionToLanguage"`
	InitializationOptions any               `json:"initializationOptions,omitempty" yaml:"initializationOptions,omitempty"`
	Configuration         any               `json:"configuration,omitempty" yaml:"configuration,omitempty"`
	MaxMessageBytes       int               `json:"maxMessageBytes,omitempty" yaml:"maxMessageBytes,omitempty"`
	MaxStderrBytes        int               `json:"maxStderrBytes,omitempty" yaml:"maxStderrBytes,omitempty"`
	MaxDocumentBytes      int               `json:"maxDocumentBytes,omitempty" yaml:"maxDocumentBytes,omitempty"`
	ShutdownTimeout       time.Duration     `json:"-" yaml:"-"`
	KillGrace             time.Duration     `json:"-" yaml:"-"`
	ShutdownTimeoutMillis int               `json:"shutdownTimeoutMs,omitempty" yaml:"shutdownTimeoutMs,omitempty"`
	KillGraceMillis       int               `json:"killGraceMs,omitempty" yaml:"killGraceMs,omitempty"`
}

type LSPToolConfig struct {
	Enabled        bool          `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	MaxLocations   int           `json:"maxLocations,omitempty" yaml:"maxLocations,omitempty"`
	MaxResultChars int           `json:"maxResultChars,omitempty" yaml:"maxResultChars,omitempty"`
	Timeout        time.Duration `json:"-" yaml:"-"`
	TimeoutMillis  int           `json:"timeoutMs,omitempty" yaml:"timeoutMs,omitempty"`
}

type lspRoute struct {
	provider   LSPProvider
	languageID string
}

type lspRegistry struct {
	mu        sync.RWMutex
	providers map[string]LSPProvider
	routes    map[string]lspRoute
	closed    bool
}

func newLSPRegistry() *lspRegistry {
	return &lspRegistry{providers: map[string]LSPProvider{}, routes: map[string]lspRoute{}}
}

func finalLSPExtension(path string) string {
	if slash := max(strings.LastIndexByte(path, '/'), strings.LastIndexByte(path, '\\')); slash >= 0 {
		path = path[slash+1:]
	}
	dot := strings.LastIndexByte(path, '.')
	if dot <= 0 {
		return ""
	}
	return strings.ToLower(path[dot:])
}

func normalizeLSPExtension(ext string) (string, error) {
	ext = strings.ToLower(strings.TrimSpace(ext))
	if ext == "" {
		return "", errors.New("extension must be non-empty")
	}
	if ext[0] != '.' {
		ext = "." + ext
	}
	if len(ext) < 2 || strings.ContainsAny(ext[1:], ".\\/") {
		return "", fmt.Errorf("invalid extension %q", ext)
	}
	return ext, nil
}

func (r *lspRegistry) register(provider LSPProvider) (func(), error) {
	if provider == nil {
		return nil, errors.New("LSP_INVALID_PROVIDER: provider is required")
	}
	id := strings.TrimSpace(provider.ID())
	if id == "" {
		return nil, errors.New("LSP_INVALID_PROVIDER: provider id must be non-empty")
	}
	extensions := provider.Extensions()
	if len(extensions) == 0 {
		return nil, fmt.Errorf("LSP_INVALID_PROVIDER: provider %q registers no file extensions", id)
	}
	pending := make(map[string]lspRoute, len(extensions))
	for raw, language := range extensions {
		ext, err := normalizeLSPExtension(raw)
		if err != nil {
			return nil, fmt.Errorf("LSP_INVALID_PROVIDER: provider %q: %w", id, err)
		}
		language = strings.TrimSpace(language)
		if language == "" {
			return nil, fmt.Errorf("LSP_INVALID_PROVIDER: provider %q maps %q to an empty language id", id, ext)
		}
		if _, exists := pending[ext]; exists {
			return nil, fmt.Errorf("LSP_INVALID_PROVIDER: provider %q maps %q more than once", id, ext)
		}
		pending[ext] = lspRoute{provider: provider, languageID: language}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("LSP_DISPOSED: LSP registry is closed")
	}
	if _, exists := r.providers[id]; exists {
		return nil, fmt.Errorf("LSP_CONFLICT: provider %q is already registered", id)
	}
	for ext := range pending {
		if _, exists := r.routes[ext]; exists {
			return nil, fmt.Errorf("LSP_CONFLICT: extension %q is already handled", ext)
		}
	}
	r.providers[id] = provider
	for ext, route := range pending {
		r.routes[ext] = route
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.providers[id] != provider {
				return
			}
			delete(r.providers, id)
			for ext, route := range r.routes {
				if route.provider == provider {
					delete(r.routes, ext)
				}
			}
		})
	}, nil
}

func (r *lspRegistry) query(ctx context.Context, request LSPQuery) (LSPQueryResult, error) {
	if _, ok := lspOperations[request.Operation]; !ok {
		return LSPQueryResult{}, fmt.Errorf("LSP_INVALID_REQUEST: unsupported operation %q", request.Operation)
	}
	if strings.TrimSpace(request.FilePath) == "" {
		return LSPQueryResult{}, errors.New("LSP_INVALID_REQUEST: filePath must be non-empty")
	}
	if strings.TrimSpace(request.WorkspaceRoot) == "" {
		return LSPQueryResult{}, errors.New("LSP_WORKSPACE_REQUIRED: workspaceRoot must be non-empty")
	}
	if request.Position.Line < 0 || request.Position.Character < 0 {
		return LSPQueryResult{}, errors.New("LSP_INVALID_REQUEST: position coordinates must be non-negative")
	}
	ext := finalLSPExtension(request.FilePath)
	r.mu.RLock()
	route, exists := r.routes[ext]
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return LSPQueryResult{}, errors.New("LSP_DISPOSED: LSP registry is closed")
	}
	if !exists {
		return LSPQueryResult{}, fmt.Errorf("LSP_UNAVAILABLE: no LSP provider handles %q", request.FilePath)
	}
	return route.provider.Query(ctx, LSPProviderQuery{LSPQuery: request, LanguageID: route.languageID})
}

func (r *lspRegistry) close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	providers := make([]LSPProvider, 0, len(r.providers))
	for _, provider := range r.providers {
		providers = append(providers, provider)
	}
	r.providers = map[string]LSPProvider{}
	r.routes = map[string]lspRoute{}
	r.mu.Unlock()
	sort.Slice(providers, func(i, j int) bool { return providers[i].ID() < providers[j].ID() })
	var failures []error
	for _, provider := range providers {
		if closer, ok := provider.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

func (e *Engine) RegisterLSPProvider(provider LSPProvider) (func(), error) {
	return e.lsp.register(provider)
}

func (e *Engine) QueryLSP(ctx context.Context, request LSPQuery) (LSPQueryResult, error) {
	return e.lsp.query(ctx, request)
}

type stdioLSPProvider struct {
	id         string
	config     LSPStdioConfig
	executable string

	mu        sync.Mutex
	instances map[string]*lspInstance
	closed    bool
}

func NewStdioLSPProvider(id string, config LSPStdioConfig) (LSPProvider, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("lsp-stdio: provider id must be non-empty")
	}
	config.Command = strings.TrimSpace(config.Command)
	if config.Command == "" {
		return nil, fmt.Errorf("lsp-stdio: servers.%s.command is required", id)
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultLSPMaxMessageBytes
	}
	if config.MaxStderrBytes == 0 {
		config.MaxStderrBytes = defaultLSPMaxStderrBytes
	}
	if config.MaxDocumentBytes == 0 {
		config.MaxDocumentBytes = defaultLSPMaxDocumentBytes
	}
	if config.ShutdownTimeout == 0 && config.ShutdownTimeoutMillis > 0 {
		config.ShutdownTimeout = time.Duration(config.ShutdownTimeoutMillis) * time.Millisecond
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = defaultLSPShutdownTimeout
	}
	if config.KillGrace == 0 && config.KillGraceMillis > 0 {
		config.KillGrace = time.Duration(config.KillGraceMillis) * time.Millisecond
	}
	if config.KillGrace == 0 {
		config.KillGrace = defaultLSPKillGrace
	}
	if config.MaxMessageBytes < 1 || config.MaxStderrBytes < 1 || config.MaxDocumentBytes < 1 || config.ShutdownTimeout < time.Millisecond || config.KillGrace < time.Millisecond {
		return nil, fmt.Errorf("lsp-stdio: servers.%s byte limits and timeouts must be positive", id)
	}
	executable, err := exec.LookPath(config.Command)
	if err != nil {
		return nil, fmt.Errorf("lsp-stdio: resolve servers.%s.command %q: %w", id, config.Command, err)
	}
	return &stdioLSPProvider{id: id, config: config, executable: executable, instances: map[string]*lspInstance{}}, nil
}

func (p *stdioLSPProvider) ID() string { return p.id }

func (p *stdioLSPProvider) Extensions() map[string]string {
	out := make(map[string]string, len(p.config.ExtensionToLanguage))
	for ext, language := range p.config.ExtensionToLanguage {
		out[ext] = language
	}
	return out
}

func (p *stdioLSPProvider) Query(ctx context.Context, request LSPProviderQuery) (LSPQueryResult, error) {
	workspace, source, err := p.readSource(ctx, request.WorkspaceRoot, request.FilePath)
	if err != nil {
		return LSPQueryResult{}, err
	}
	for attempt := 0; attempt < 2; attempt++ {
		instance, err := p.instance(workspace)
		if err != nil {
			return LSPQueryResult{}, err
		}
		result, err := instance.query(ctx, request, source)
		if err == nil {
			return result, nil
		}
		if ctx.Err() != nil || !isLSPTransportError(err) {
			return LSPQueryResult{}, err
		}
		_ = instance.close()
		p.mu.Lock()
		if p.instances[workspace.path] == instance {
			delete(p.instances, workspace.path)
		}
		p.mu.Unlock()
	}
	return LSPQueryResult{}, errors.New("LSP_TRANSPORT: language server failed twice")
}

type lspWorkspace struct {
	path string
	uri  string
}

type lspSource struct {
	uri  string
	text string
}

func (p *stdioLSPProvider) readSource(ctx context.Context, workspaceRoot, filePath string) (lspWorkspace, lspSource, error) {
	if err := ctx.Err(); err != nil {
		return lspWorkspace{}, lspSource{}, err
	}
	workspacePath, err := filepath.Abs(workspaceRoot)
	if err != nil {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_WORKSPACE_INVALID: %w", err)
	}
	workspacePath, err = filepath.EvalSymlinks(workspacePath)
	if err != nil {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_WORKSPACE_INVALID: workspace root %q cannot be resolved: %w", workspaceRoot, err)
	}
	info, err := os.Stat(workspacePath)
	if err != nil || !info.IsDir() {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_WORKSPACE_INVALID: workspace root %q is not a directory", workspaceRoot)
	}
	target := filePath
	if !filepath.IsAbs(target) {
		target = filepath.Join(workspacePath, target)
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_SOURCE_INVALID: %w", err)
	}
	target, err = filepath.EvalSymlinks(target)
	if err != nil {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_SOURCE_INVALID: source %q cannot be resolved: %w", filePath, err)
	}
	relative, err := filepath.Rel(workspacePath, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_SOURCE_OUTSIDE_WORKSPACE: source %q resolves outside the workspace", filePath)
	}
	file, err := os.Open(target)
	if err != nil {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_SOURCE_INVALID: source %q could not be read: %w", filePath, err)
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_SOURCE_INVALID: source %q is not a regular file", filePath)
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(p.config.MaxDocumentBytes)+1))
	if err != nil {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_SOURCE_INVALID: source %q could not be read: %w", filePath, err)
	}
	if len(data) > p.config.MaxDocumentBytes {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_SOURCE_TOO_LARGE: source %q exceeds the %d-byte limit", filePath, p.config.MaxDocumentBytes)
	}
	if !utf8.Valid(data) {
		return lspWorkspace{}, lspSource{}, fmt.Errorf("LSP_SOURCE_INVALID: source %q is not valid UTF-8", filePath)
	}
	if err := ctx.Err(); err != nil {
		return lspWorkspace{}, lspSource{}, err
	}
	return lspWorkspace{path: workspacePath, uri: lspFileURI(workspacePath)}, lspSource{uri: lspFileURI(target), text: string(data)}, nil
}

func lspFileURI(path string) string {
	path = filepath.ToSlash(path)
	if volume := filepath.VolumeName(path); volume != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return (&url.URL{Scheme: "file", Path: path}).String()
}

func (p *stdioLSPProvider) instance(workspace lspWorkspace) (*lspInstance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("LSP_DISPOSED: lsp-stdio provider is closed")
	}
	if current := p.instances[workspace.path]; current != nil && !current.dead() {
		return current, nil
	}
	instance, err := newLSPInstance(p.executable, p.config, workspace)
	if err != nil {
		return nil, err
	}
	p.instances[workspace.path] = instance
	return instance, nil
}

func (p *stdioLSPProvider) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	instances := make([]*lspInstance, 0, len(p.instances))
	for _, instance := range p.instances {
		instances = append(instances, instance)
	}
	p.instances = map[string]*lspInstance{}
	p.mu.Unlock()
	var failures []error
	for _, instance := range instances {
		if err := instance.close(); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

type lspTransportError struct{ err error }

func (e *lspTransportError) Error() string { return "LSP_TRANSPORT: " + e.err.Error() }
func (e *lspTransportError) Unwrap() error { return e.err }

func isLSPTransportError(err error) bool {
	var target *lspTransportError
	return errors.As(err, &target)
}

type lspInstance struct {
	mu           sync.Mutex
	connection   *lspConnection
	config       LSPStdioConfig
	workspace    lspWorkspace
	capabilities map[string]any
	initialized  bool
	closed       atomic.Bool
}

func newLSPInstance(executable string, config LSPStdioConfig, workspace lspWorkspace) (*lspInstance, error) {
	connection, err := newLSPConnection(executable, config, workspace.path)
	if err != nil {
		return nil, err
	}
	return &lspInstance{connection: connection, config: config, workspace: workspace}, nil
}

func (i *lspInstance) dead() bool {
	return i.closed.Load() || i.connection.dead()
}

func (i *lspInstance) query(ctx context.Context, request LSPProviderQuery, source lspSource) (LSPQueryResult, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed.Load() {
		return LSPQueryResult{}, errors.New("LSP_DISPOSED: LSP instance is closed")
	}
	if err := i.initialize(ctx); err != nil {
		i.closed.Store(true)
		i.connection.terminate()
		return LSPQueryResult{}, err
	}
	if !lspSupportsOperation(i.capabilities, request.Operation) {
		return LSPQueryResult{}, fmt.Errorf("LSP_UNSUPPORTED_OPERATION: server does not support %s", request.Operation)
	}
	if !lspSupportsTransientOpen(i.capabilities["textDocumentSync"]) {
		return LSPQueryResult{}, errors.New("LSP_UNSUPPORTED_OPERATION: server does not support transient textDocument/didOpen")
	}
	if err := i.connection.notify(map[string]any{"jsonrpc": "2.0", "method": "textDocument/didOpen", "params": map[string]any{
		"textDocument": map[string]any{"uri": source.uri, "languageId": request.LanguageID, "version": 1, "text": source.text},
	}}); err != nil {
		return LSPQueryResult{}, err
	}
	opened := true
	defer func() {
		if opened && !i.connection.dead() {
			_ = i.connection.notify(map[string]any{"jsonrpc": "2.0", "method": "textDocument/didClose", "params": map[string]any{"textDocument": map[string]any{"uri": source.uri}}})
		}
	}()
	params := map[string]any{
		"textDocument": map[string]any{"uri": source.uri},
		"position":     request.Position,
	}
	if request.Operation == LSPFindReferences {
		params["context"] = map[string]any{"includeDeclaration": true}
	}
	payload, err := i.connection.request(ctx, lspOperations[request.Operation], params)
	if err != nil {
		return LSPQueryResult{}, err
	}
	if request.Operation == LSPHover {
		hover, err := normalizeLSPHover(payload)
		if err != nil {
			return LSPQueryResult{}, err
		}
		return LSPQueryResult{Kind: "hover", Hover: hover}, nil
	}
	locations, err := normalizeLSPLocations(payload)
	if err != nil {
		return LSPQueryResult{}, err
	}
	return LSPQueryResult{Kind: "locations", Locations: locations, ResolvedWorkspaceURI: i.workspace.uri}, nil
}

func (i *lspInstance) initialize(ctx context.Context) error {
	if i.initialized {
		return nil
	}
	result, err := i.connection.request(ctx, "initialize", map[string]any{
		"processId":        nil,
		"rootUri":          i.workspace.uri,
		"workspaceFolders": []any{map[string]any{"uri": i.workspace.uri, "name": "workspace"}},
		"capabilities": map[string]any{
			"general":   map[string]any{"positionEncodings": []string{"utf-16"}},
			"workspace": map[string]any{"workspaceFolders": true, "configuration": true},
			"textDocument": map[string]any{
				"synchronization": map[string]any{"dynamicRegistration": false},
				"hover":           map[string]any{"contentFormat": []string{"markdown", "plaintext"}},
				"definition":      map[string]any{"linkSupport": true},
				"implementation":  map[string]any{"linkSupport": true},
				"references":      map[string]any{},
			},
		},
		"initializationOptions": i.config.InitializationOptions,
	})
	if err != nil {
		return err
	}
	root, ok := result.(map[string]any)
	if !ok {
		return errors.New("LSP_MALFORMED_RESPONSE: initialize result was not an object")
	}
	capabilities, ok := root["capabilities"].(map[string]any)
	if !ok {
		return errors.New("LSP_MALFORMED_RESPONSE: initialize result had no capabilities object")
	}
	if encoding, _ := capabilities["positionEncoding"].(string); encoding != "" && encoding != "utf-16" {
		return fmt.Errorf("LSP_UNSUPPORTED_ENCODING: server negotiated %q; this host requires utf-16", encoding)
	}
	i.capabilities = capabilities
	if err := i.connection.notify(map[string]any{"jsonrpc": "2.0", "method": "initialized", "params": map[string]any{}}); err != nil {
		return err
	}
	i.initialized = true
	return nil
}

func (i *lspInstance) close() error {
	if !i.mu.TryLock() {
		i.connection.terminate()
		<-i.connection.done
		i.closed.Store(true)
		return nil
	}
	defer i.mu.Unlock()
	if i.closed.Load() {
		return nil
	}
	i.closed.Store(true)
	if i.initialized && !i.connection.dead() {
		ctx, cancel := context.WithTimeout(context.Background(), i.config.ShutdownTimeout)
		_, err := i.connection.request(ctx, "shutdown", nil)
		if err == nil {
			_ = i.connection.notify(map[string]any{"jsonrpc": "2.0", "method": "exit", "params": nil})
			select {
			case <-i.connection.done:
				cancel()
				return nil
			case <-ctx.Done():
			}
		}
		cancel()
	}
	i.connection.terminate()
	<-i.connection.done
	return nil
}

func lspSupportsOperation(capabilities map[string]any, operation string) bool {
	name := map[string]string{
		LSPGoToDefinition: "definitionProvider", LSPFindReferences: "referencesProvider",
		LSPGoToImplementation: "implementationProvider", LSPHover: "hoverProvider",
	}[operation]
	value, exists := capabilities[name]
	if !exists || value == nil {
		return false
	}
	if enabled, ok := value.(bool); ok {
		return enabled
	}
	_, ok := value.(map[string]any)
	return ok
}

func lspSupportsTransientOpen(value any) bool {
	switch value := value.(type) {
	case json.Number:
		number, err := strconv.Atoi(value.String())
		return err == nil && (number == 1 || number == 2)
	case float64:
		return value == 1 || value == 2
	case map[string]any:
		enabled, _ := value["openClose"].(bool)
		return enabled
	default:
		return false
	}
}

type lspConnection struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	reader    *bufio.Reader
	stderr    *lspByteTail
	config    any
	maxBytes  int
	killGrace time.Duration
	done      chan struct{}

	writeMu  sync.Mutex
	nextID   int64
	waitErr  error
	killOnce sync.Once
}

func newLSPConnection(executable string, config LSPStdioConfig, cwd string) (*lspConnection, error) {
	cmd := exec.Command(executable, config.Args...)
	cmd.Dir = cwd
	cmd.Env = scrubbedChildEnv(config.Env)
	configureChildProcess(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, &lspTransportError{err: err}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, &lspTransportError{err: err}
	}
	tail := &lspByteTail{max: config.MaxStderrBytes}
	cmd.Stderr = tail
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, &lspTransportError{err: err}
	}
	connection := &lspConnection{
		cmd: cmd, stdin: stdin, reader: bufio.NewReaderSize(stdout, 64<<10), stderr: tail,
		config: config.Configuration, maxBytes: config.MaxMessageBytes, killGrace: config.KillGrace, done: make(chan struct{}),
	}
	go func() {
		connection.waitErr = cmd.Wait()
		close(connection.done)
	}()
	return connection, nil
}

func (c *lspConnection) dead() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *lspConnection) request(ctx context.Context, method string, params any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.nextID++
	id := c.nextID
	if err := c.notify(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		return nil, err
	}
	type response struct {
		value any
		err   error
	}
	result := make(chan response, 1)
	go func() {
		value, err := c.readResponse(id)
		result <- response{value: value, err: err}
	}()
	select {
	case response := <-result:
		return response.value, response.err
	case <-ctx.Done():
		_ = c.notify(map[string]any{"jsonrpc": "2.0", "method": "$/cancelRequest", "params": map[string]any{"id": id}})
		timer := time.NewTimer(c.killGrace)
		defer timer.Stop()
		select {
		case <-result:
			return nil, ctx.Err()
		case <-timer.C:
			c.terminate()
			<-result
			return nil, ctx.Err()
		}
	}
}

func (c *lspConnection) notify(message any) error {
	if c.dead() {
		return &lspTransportError{err: c.exitError()}
	}
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}
	frame := make([]byte, 0, len(body)+64)
	frame = append(frame, "Content-Length: "...)
	frame = strconv.AppendInt(frame, int64(len(body)), 10)
	frame = append(frame, '\r', '\n', '\r', '\n')
	frame = append(frame, body...)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := io.Copy(c.stdin, bytes.NewReader(frame)); err != nil {
		return &lspTransportError{err: err}
	}
	return nil
}

func (c *lspConnection) readResponse(id int64) (any, error) {
	for {
		message, err := c.readMessage()
		if err != nil {
			return nil, &lspTransportError{err: err}
		}
		method, _ := message["method"].(string)
		frameID, hasID := message["id"]
		if method != "" && hasID {
			c.answerServerRequest(frameID, method, message["params"])
			continue
		}
		if method != "" || !sameLSPID(frameID, id) {
			continue
		}
		if rawError, ok := message["error"].(map[string]any); ok {
			text, _ := rawError["message"].(string)
			if text == "" {
				text = "LSP error response"
			}
			return nil, errors.New(text)
		}
		return message["result"], nil
	}
}

func sameLSPID(value any, id int64) bool {
	switch value := value.(type) {
	case json.Number:
		return value.String() == strconv.FormatInt(id, 10)
	case float64:
		return value == float64(id)
	case int64:
		return value == id
	default:
		return false
	}
}

func (c *lspConnection) readMessage() (map[string]any, error) {
	contentLength := -1
	headerBytes := 0
	for {
		line, err := c.reader.ReadString('\n')
		headerBytes += len(line)
		if headerBytes > 64<<10 {
			return nil, errors.New("LSP header exceeded 65536 bytes")
		}
		if err != nil {
			return nil, err
		}
		if line == "\r\n" {
			break
		}
		name, value, ok := strings.Cut(strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), ":")
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			continue
		}
		length, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || length < 0 {
			return nil, fmt.Errorf("invalid Content-Length header %q", strings.TrimSpace(line))
		}
		contentLength = length
	}
	if contentLength < 0 {
		return nil, errors.New("LSP header block missing Content-Length")
	}
	if contentLength > c.maxBytes {
		return nil, fmt.Errorf("LSP message length %d exceeds the %d-byte limit", contentLength, c.maxBytes)
	}
	body := make([]byte, contentLength)
	if _, err := io.ReadFull(c.reader, body); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var message map[string]any
	if err := decoder.Decode(&message); err != nil {
		return nil, fmt.Errorf("LSP message body was not valid JSON: %w", err)
	}
	return message, nil
}

func (c *lspConnection) answerServerRequest(id any, method string, params any) {
	var result any
	var responseError map[string]any
	switch method {
	case "workspace/configuration":
		items := []any{}
		if object, ok := params.(map[string]any); ok {
			items, _ = object["items"].([]any)
		}
		result = make([]any, len(items))
		for index := range items {
			result.([]any)[index] = c.config
		}
	case "window/workDoneProgress/create", "client/registerCapability", "client/unregisterCapability":
		result = nil
	case "workspace/applyEdit":
		responseError = map[string]any{"code": -32601, "message": "workspace/applyEdit is not permitted by this host"}
	default:
		responseError = map[string]any{"code": -32601, "message": "unsupported server request: " + method}
	}
	response := map[string]any{"jsonrpc": "2.0", "id": id}
	if responseError != nil {
		response["error"] = responseError
	} else {
		response["result"] = result
	}
	_ = c.notify(response)
}

func (c *lspConnection) terminate() {
	c.killOnce.Do(func() {
		_ = terminateChildProcess(c.cmd)
		go func() {
			timer := time.NewTimer(c.killGrace)
			defer timer.Stop()
			select {
			case <-c.done:
			case <-timer.C:
				_ = killChildProcess(c.cmd)
			}
		}()
	})
}

func (c *lspConnection) exitError() error {
	tail := strings.TrimSpace(c.stderr.String())
	if tail != "" {
		return fmt.Errorf("language server exited; stderr: %s", tail)
	}
	if c.waitErr != nil {
		return fmt.Errorf("language server exited: %w", c.waitErr)
	}
	return errors.New("language server exited")
}

type lspByteTail struct {
	mu   sync.Mutex
	max  int
	data []byte
}

func (w *lspByteTail) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.data = append(w.data, data...)
	if len(w.data) > w.max {
		w.data = append([]byte(nil), w.data[len(w.data)-w.max:]...)
	}
	return len(data), nil
}

func (w *lspByteTail) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return string(w.data)
}

func normalizeLSPLocations(payload any) ([]LSPLocation, error) {
	if payload == nil {
		return []LSPLocation{}, nil
	}
	values, ok := payload.([]any)
	if !ok {
		values = []any{payload}
	}
	locations := make([]LSPLocation, 0, len(values))
	for _, value := range values {
		object, ok := value.(map[string]any)
		if !ok {
			return nil, errors.New("LSP_MALFORMED_RESPONSE: navigation result contained a non-object entry")
		}
		uri, _ := object["uri"].(string)
		rawRange := object["range"]
		if uri == "" {
			uri, _ = object["targetUri"].(string)
			rawRange = object["targetSelectionRange"]
		}
		rangeValue, err := decodeLSPRange(rawRange)
		if uri == "" || err != nil {
			return nil, errors.New("LSP_MALFORMED_RESPONSE: navigation result contained neither a Location nor a LocationLink")
		}
		locations = append(locations, LSPLocation{URI: uri, Range: rangeValue})
	}
	return locations, nil
}

func decodeLSPRange(value any) (LSPRange, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return LSPRange{}, errors.New("range is not an object")
	}
	start, err := decodeLSPPosition(object["start"])
	if err != nil {
		return LSPRange{}, err
	}
	end, err := decodeLSPPosition(object["end"])
	if err != nil {
		return LSPRange{}, err
	}
	return LSPRange{Start: start, End: end}, nil
}

func decodeLSPPosition(value any) (LSPPosition, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return LSPPosition{}, errors.New("position is not an object")
	}
	line, ok := lspCoordinate(object["line"])
	if !ok {
		return LSPPosition{}, errors.New("position line is invalid")
	}
	character, ok := lspCoordinate(object["character"])
	if !ok {
		return LSPPosition{}, errors.New("position character is invalid")
	}
	return LSPPosition{Line: line, Character: character}, nil
}

func lspCoordinate(value any) (int, bool) {
	switch value := value.(type) {
	case json.Number:
		number, err := strconv.Atoi(value.String())
		return number, err == nil && number >= 0
	case float64:
		number := int(value)
		return number, value == float64(number) && number >= 0
	case int:
		return value, value >= 0
	default:
		return 0, false
	}
}

func normalizeLSPHover(payload any) (*LSPHoverResult, error) {
	if payload == nil {
		return nil, nil
	}
	object, ok := payload.(map[string]any)
	if !ok {
		return nil, errors.New("LSP_MALFORMED_RESPONSE: hover result was not an object")
	}
	contents, err := renderLSPHoverContents(object["contents"])
	if err != nil {
		return nil, err
	}
	if contents == "" {
		return nil, nil
	}
	hover := &LSPHoverResult{Contents: contents}
	if rawRange, exists := object["range"]; exists {
		rangeValue, err := decodeLSPRange(rawRange)
		if err != nil {
			return nil, errors.New("LSP_MALFORMED_RESPONSE: hover result contained a malformed range")
		}
		hover.Range = &rangeValue
	}
	return hover, nil
}

func renderLSPHoverContents(value any) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case []any:
		parts := make([]string, len(value))
		for index, item := range value {
			part, err := renderLSPMarkedString(item)
			if err != nil {
				return "", err
			}
			parts[index] = part
		}
		return strings.Join(parts, "\n\n"), nil
	case map[string]any:
		if kind, _ := value["kind"].(string); kind == "markdown" || kind == "plaintext" {
			text, ok := value["value"].(string)
			if !ok {
				return "", errors.New("LSP_MALFORMED_RESPONSE: hover MarkupContent value was not a string")
			}
			return text, nil
		}
		return renderLSPMarkedString(value)
	default:
		return "", errors.New("LSP_MALFORMED_RESPONSE: hover result had malformed contents")
	}
}

func renderLSPMarkedString(value any) (string, error) {
	if text, ok := value.(string); ok {
		return text, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return "", errors.New("LSP_MALFORMED_RESPONSE: hover contents contained a malformed MarkedString")
	}
	language, languageOK := object["language"].(string)
	text, textOK := object["value"].(string)
	if !languageOK || !textOK {
		return "", errors.New("LSP_MALFORMED_RESPONSE: hover contents contained a malformed MarkedString")
	}
	return "```" + language + "\n" + text + "\n```", nil
}

func (e *Engine) EnableLSPTool(config LSPToolConfig) error {
	config.Enabled = true
	if config.MaxLocations == 0 {
		config.MaxLocations = defaultLSPMaxLocations
	}
	if config.MaxResultChars == 0 {
		config.MaxResultChars = defaultLSPMaxResultChars
	}
	if config.Timeout == 0 && config.TimeoutMillis > 0 {
		config.Timeout = time.Duration(config.TimeoutMillis) * time.Millisecond
	}
	if config.Timeout == 0 {
		config.Timeout = defaultLSPToolTimeout
	}
	if config.MaxLocations < 1 || config.MaxResultChars < 1 || config.Timeout < time.Millisecond {
		return errors.New("tool-lsp: limits and timeout must be positive")
	}
	return e.RegisterTool(lspTool(e, config))
}

func lspTool(e *Engine, config LSPToolConfig) Tool {
	type input struct {
		Operation string `json:"operation"`
		FilePath  string `json:"file_path"`
		Line      int    `json:"line"`
		Character int    `json:"character"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "lsp",
			Description: "Query a language server for precise code navigation. operation is one of goToDefinition, findReferences, goToImplementation, hover. line and character are one-based UTF-16 cursor coordinates. findReferences includes the declaration.",
			Parameters: objectSchema(map[string]any{
				"operation": map[string]any{"type": "string", "enum": []string{LSPGoToDefinition, LSPFindReferences, LSPGoToImplementation, LSPHover}},
				"file_path": map[string]any{"type": "string"},
				"line":      map[string]any{"type": "integer", "minimum": 1},
				"character": map[string]any{"type": "integer", "minimum": 1},
			}, "operation", "file_path", "line", "character"),
			Output: map[string]any{"oneOf": []any{
				objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "locations"}, "locations": map[string]any{"type": "array", "items": objectSchema(map[string]any{"uri": map[string]any{"type": "string"}, "range": lspRangeOutputSchema()}, "uri", "range")}, "resolvedWorkspaceUri": map[string]any{"type": "string"}}, "kind", "locations", "resolvedWorkspaceUri"),
				objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "hover"}, "hover": map[string]any{"oneOf": []any{map[string]any{"type": "null"}, objectSchema(map[string]any{"contents": map[string]any{"type": "string"}, "range": lspRangeOutputSchema()}, "contents")}}}, "kind", "hover"),
			}},
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if _, ok := lspOperations[in.Operation]; !ok {
				return ToolResult{}, fmt.Errorf("LSP_INVALID_REQUEST: operation must be one of %s", strings.Join([]string{LSPGoToDefinition, LSPFindReferences, LSPGoToImplementation, LSPHover}, ", "))
			}
			if strings.TrimSpace(in.FilePath) == "" || in.Line < 1 || in.Character < 1 {
				return ToolResult{}, errors.New("LSP_INVALID_REQUEST: file_path must be non-empty and line/character must be positive one-based integers")
			}
			if strings.TrimSpace(call.Workspace) == "" {
				return ToolResult{}, errors.New("LSP_WORKSPACE_REQUIRED: the lsp tool requires a session workspace cwd")
			}
			queryCtx, cancel := context.WithTimeout(ctx, config.Timeout)
			defer cancel()
			result, err := e.QueryLSP(queryCtx, LSPQuery{
				Operation: in.Operation, FilePath: in.FilePath,
				Position: LSPPosition{Line: in.Line - 1, Character: in.Character - 1}, WorkspaceRoot: call.Workspace,
			})
			if err != nil {
				return ToolResult{}, err
			}
			value := lspToolValue(result)
			return ToolResult{Content: []ContentBlock{{Type: "text", Text: renderLSPResult(result, config)}}, Value: value}, nil
		},
	}
}

func lspRangeOutputSchema() map[string]any {
	position := objectSchema(map[string]any{"line": map[string]any{"type": "integer"}, "character": map[string]any{"type": "integer"}}, "line", "character")
	return objectSchema(map[string]any{"start": position, "end": position}, "start", "end")
}

func lspToolValue(result LSPQueryResult) any {
	if result.Kind == "hover" {
		return map[string]any{"kind": "hover", "hover": result.Hover}
	}
	return map[string]any{"kind": "locations", "locations": result.Locations, "resolvedWorkspaceUri": result.ResolvedWorkspaceURI}
}

func renderLSPResult(result LSPQueryResult, config LSPToolConfig) string {
	if result.Kind == "hover" {
		text := "No hover information."
		if result.Hover != nil {
			text = result.Hover.Contents
		}
		return boundLSPResult(text, config.MaxResultChars, "hover")
	}
	if len(result.Locations) == 0 {
		return boundLSPResult("No results.", config.MaxResultChars, "locations")
	}
	shown := result.Locations
	if len(shown) > config.MaxLocations {
		shown = shown[:config.MaxLocations]
	}
	lines := make([]string, 0, len(shown)+1)
	for _, location := range shown {
		lines = append(lines, fmt.Sprintf("%s:%d:%d", renderLSPURI(location.URI, result.ResolvedWorkspaceURI), location.Range.Start.Line+1, location.Range.Start.Character+1))
	}
	if omitted := len(result.Locations) - len(shown); omitted > 0 {
		lines = append(lines, fmt.Sprintf("... %d more locations omitted (limit %d).", omitted, config.MaxLocations))
	}
	return boundLSPResult(strings.Join(lines, "\n"), config.MaxResultChars, "locations")
}

func renderLSPURI(uri, workspaceURI string) string {
	target, targetErr := url.Parse(uri)
	workspace, workspaceErr := url.Parse(workspaceURI)
	if targetErr != nil || workspaceErr != nil || target.Scheme != "file" || workspace.Scheme != "file" {
		return uri
	}
	targetPath, targetErr := url.PathUnescape(target.Path)
	workspacePath, workspaceErr := url.PathUnescape(workspace.Path)
	if targetErr != nil || workspaceErr != nil {
		return uri
	}
	targetPath = filepath.FromSlash(targetPath)
	workspacePath = filepath.FromSlash(workspacePath)
	relative, err := filepath.Rel(workspacePath, targetPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return filepath.ToSlash(targetPath)
	}
	if relative == "." {
		return "."
	}
	return filepath.ToSlash(relative)
}

func boundLSPResult(text string, maxChars int, label string) string {
	if utf16Length(text) <= maxChars {
		return text
	}
	notice := fmt.Sprintf("\n... %s truncated (limit %d characters).", label, maxChars)
	if utf16Length(notice) >= maxChars {
		return truncateUTF16(notice, maxChars)
	}
	return truncateUTF16(text, maxChars-utf16Length(notice)) + notice
}

func utf16Length(text string) int { return len(utf16.Encode([]rune(text))) }

func truncateUTF16(text string, limit int) string {
	if limit <= 0 {
		return ""
	}
	units := utf16.Encode([]rune(text))
	if len(units) <= limit {
		return text
	}
	if limit > 0 && units[limit-1] >= 0xd800 && units[limit-1] <= 0xdbff {
		limit--
	}
	return string(utf16.Decode(units[:limit]))
}
