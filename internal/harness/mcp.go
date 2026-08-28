package harness

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	MCPTransportStdio           = "stdio"
	MCPTransportStreamableHTTP  = "streamable-http"
	defaultMCPToolCallTimeout   = 60 * time.Second
	defaultMCPReconnectDelay    = 500 * time.Millisecond
	defaultMCPReconnectMax      = 30 * time.Second
	defaultMCPReconnectAttempts = 10
)

var mcpServerNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

// MCPReconnectConfig controls automatic recovery after an MCP connection is lost.
type MCPReconnectConfig struct {
	Enabled      *bool
	InitialDelay time.Duration
	MaxDelay     time.Duration
	MaxAttempts  int
}

// MCPConfig connects one external MCP server and exposes its tools on an Engine.
type MCPConfig struct {
	Transport          string
	ServerName         string
	Command            string
	Args               []string
	Env                map[string]string
	CWD                string
	URL                string
	Headers            map[string]string
	ToolCallTimeout    time.Duration
	FailOnStartupError bool
	Reconnect          MCPReconnectConfig
}

type resolvedMCPReconnect struct {
	enabled      bool
	initialDelay time.Duration
	maxDelay     time.Duration
	maxAttempts  int
}

// MCPConnection owns one supervised MCP client and its registered tools.
type MCPConnection struct {
	engine *Engine
	config MCPConfig
	policy resolvedMCPReconnect
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	ready  chan struct{}

	mu         sync.Mutex
	session    *mcp.ClientSession
	initialErr error
	tools      map[string]struct{}
	closeOnce  sync.Once
	syncMu     sync.Mutex
}

var mcpConnections = struct {
	sync.Mutex
	byEngine map[*Engine]map[string]*MCPConnection
}{byEngine: map[*Engine]map[string]*MCPConnection{}}

// WithMCPServers configures MCP servers that New connects before returning.
func WithMCPServers(configs ...MCPConfig) Option {
	return func(cfg *Config) { cfg.MCPServers = append([]MCPConfig(nil), configs...) }
}

func normalizeMCPConfig(config MCPConfig) (MCPConfig, resolvedMCPReconnect, error) {
	config.ServerName = strings.TrimSpace(config.ServerName)
	if !mcpServerNamePattern.MatchString(config.ServerName) {
		return config, resolvedMCPReconnect{}, fmt.Errorf("mcp-client: serverName %q must match [A-Za-z0-9_-]{1,32}", config.ServerName)
	}
	if config.ToolCallTimeout == 0 {
		config.ToolCallTimeout = defaultMCPToolCallTimeout
	}
	if config.ToolCallTimeout < 0 {
		return config, resolvedMCPReconnect{}, errors.New("mcp-client: toolCallTimeout must be positive")
	}
	policy := resolvedMCPReconnect{enabled: true, initialDelay: defaultMCPReconnectDelay, maxDelay: defaultMCPReconnectMax, maxAttempts: defaultMCPReconnectAttempts}
	if config.Reconnect.Enabled != nil {
		policy.enabled = *config.Reconnect.Enabled
	}
	if config.Reconnect.InitialDelay != 0 {
		policy.initialDelay = config.Reconnect.InitialDelay
	}
	if config.Reconnect.MaxDelay != 0 {
		policy.maxDelay = config.Reconnect.MaxDelay
	}
	if config.Reconnect.MaxAttempts != 0 {
		policy.maxAttempts = config.Reconnect.MaxAttempts
	}
	if policy.initialDelay <= 0 || policy.maxDelay <= 0 || policy.initialDelay > policy.maxDelay {
		return config, resolvedMCPReconnect{}, errors.New("mcp-client: reconnect delays must be positive and initialDelay must not exceed maxDelay")
	}
	if policy.maxAttempts < 1 {
		return config, resolvedMCPReconnect{}, errors.New("mcp-client: reconnect maxAttempts must be positive")
	}
	switch config.Transport {
	case MCPTransportStdio:
		if strings.TrimSpace(config.Command) == "" {
			return config, resolvedMCPReconnect{}, errors.New("mcp-client: stdio command is required")
		}
	case MCPTransportStreamableHTTP:
		parsed, err := url.Parse(config.URL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return config, resolvedMCPReconnect{}, fmt.Errorf("mcp-client: streamable-http URL must be http(s): %q", config.URL)
		}
	default:
		return config, resolvedMCPReconnect{}, fmt.Errorf("mcp-client: unsupported transport %q", config.Transport)
	}
	return config, policy, nil
}

// ConnectMCP connects one MCP server, waits for its first discovery attempt,
// and keeps the connection supervised until Close or Engine.Close.
func (e *Engine) ConnectMCP(ctx context.Context, raw MCPConfig) (*MCPConnection, error) {
	e.mu.RLock()
	closed := e.closed
	e.mu.RUnlock()
	if closed {
		return nil, errors.New("mcp-client: engine is closed")
	}
	config, policy, err := normalizeMCPConfig(raw)
	if err != nil {
		return nil, err
	}
	root, cancel := context.WithCancel(context.WithoutCancel(ctx))
	connection := &MCPConnection{
		engine: e, config: config, policy: policy, ctx: root, cancel: cancel,
		done: make(chan struct{}), ready: make(chan struct{}), tools: map[string]struct{}{},
	}
	mcpConnections.Lock()
	servers := mcpConnections.byEngine[e]
	if servers == nil {
		servers = map[string]*MCPConnection{}
		mcpConnections.byEngine[e] = servers
	}
	if servers[config.ServerName] != nil {
		mcpConnections.Unlock()
		cancel()
		return nil, fmt.Errorf("mcp-client: serverName %q is already in use", config.ServerName)
	}
	servers[config.ServerName] = connection
	mcpConnections.Unlock()

	go connection.run()
	select {
	case <-connection.ready:
		if connection.initialError() != nil && config.FailOnStartupError {
			err := connection.initialError()
			_ = connection.Close()
			return nil, fmt.Errorf("mcp-client(%s): initial connection or tool synchronization failed: %w", config.ServerName, err)
		}
		return connection, nil
	case <-ctx.Done():
		_ = connection.Close()
		return nil, ctx.Err()
	}
}

// InitialError reports the first connection or discovery failure, if any.
func (c *MCPConnection) InitialError() error { return c.initialError() }

func (c *MCPConnection) initialError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.initialErr
}

func (c *MCPConnection) setInitialResult(err error) {
	c.mu.Lock()
	c.initialErr = err
	c.mu.Unlock()
	close(c.ready)
}

func (c *MCPConnection) run() {
	defer close(c.done)
	first := true
	failedAttempts := 0
	for {
		session, err := c.connectGeneration()
		if first {
			c.setInitialResult(err)
			first = false
		}
		if err == nil {
			connectedAt := time.Now()
			err = session.Wait()
			c.clearSession(session)
			if c.ctx.Err() != nil {
				return
			}
			if time.Since(connectedAt) >= c.policy.maxDelay {
				failedAttempts = 0
			}
		}
		if c.ctx.Err() != nil || !c.policy.enabled {
			return
		}
		failedAttempts++
		if failedAttempts > c.policy.maxAttempts {
			c.unregisterTools()
			return
		}
		delay := c.policy.initialDelay
		for range failedAttempts - 1 {
			if delay >= c.policy.maxDelay/2 {
				delay = c.policy.maxDelay
				break
			}
			delay *= 2
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-c.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (c *MCPConnection) connectGeneration() (*mcp.ClientSession, error) {
	client := mcp.NewClient(&mcp.Implementation{Name: "deepseek-harness", Version: Version()}, &mcp.ClientOptions{
		Capabilities: &mcp.ClientCapabilities{},
		ToolListChangedHandler: func(context.Context, *mcp.ToolListChangedRequest) {
			go c.resyncCurrent()
		},
	})
	transport, err := c.transport()
	if err != nil {
		return nil, err
	}
	session, err := client.Connect(c.ctx, transport, nil)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.ctx.Err() != nil {
		c.mu.Unlock()
		_ = session.Close()
		return nil, c.ctx.Err()
	}
	c.session = session
	c.mu.Unlock()
	if err := c.syncTools(session); err != nil {
		c.clearSession(session)
		_ = session.Close()
		return nil, err
	}
	return session, nil
}

func (c *MCPConnection) transport() (mcp.Transport, error) {
	if c.config.Transport == MCPTransportStdio {
		cmd := exec.Command(c.config.Command, c.config.Args...)
		cmd.Env = scrubbedChildEnv(c.config.Env)
		cmd.Dir = c.config.CWD
		cmd.Stderr = os.Stderr
		return &mcp.CommandTransport{Command: cmd}, nil
	}
	client := &http.Client{Transport: &mcpHeaderTransport{base: http.DefaultTransport, headers: c.config.Headers}}
	return &mcp.StreamableClientTransport{Endpoint: c.config.URL, HTTPClient: client}, nil
}

type mcpHeaderTransport struct {
	base    http.RoundTripper
	headers map[string]string
}

func (t *mcpHeaderTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	for key, value := range t.headers {
		clone.Header.Set(key, value)
	}
	return t.base.RoundTrip(clone)
}

func (c *MCPConnection) resyncCurrent() {
	c.mu.Lock()
	session := c.session
	c.mu.Unlock()
	if session != nil {
		_ = c.syncTools(session)
	}
}

func (c *MCPConnection) syncTools(session *mcp.ClientSession) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	definitions := map[string]Tool{}
	seenRaw := map[string]bool{}
	params := &mcp.ListToolsParams{}
	for {
		result, err := session.ListTools(c.ctx, params)
		if err != nil {
			return err
		}
		for _, remote := range result.Tools {
			if remote == nil || seenRaw[remote.Name] {
				return fmt.Errorf("mcp-client(%s): server listed tool %q more than once", c.config.ServerName, remote.Name)
			}
			seenRaw[remote.Name] = true
			name := MCPPublicToolName(c.config.ServerName, remote.Name)
			parameters, err := mcpInputSchema(remote.InputSchema)
			if err != nil {
				return fmt.Errorf("mcp-client(%s): tool %q input schema: %w", c.config.ServerName, remote.Name, err)
			}
			definitions[name] = c.bridgeTool(session, name, remote.Name, remote.Description, parameters, mcpCanonicalOutputSchema(remote.OutputSchema))
		}
		if result.NextCursor == "" {
			break
		}
		params = &mcp.ListToolsParams{Cursor: result.NextCursor}
	}

	c.mu.Lock()
	if c.session != session || c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil
	}
	previous := c.tools
	c.tools = map[string]struct{}{}
	c.mu.Unlock()
	for name := range previous {
		c.engine.UnregisterTool(name)
	}
	registered := map[string]struct{}{}
	for name, tool := range definitions {
		if err := c.engine.RegisterTool(tool); err != nil {
			for added := range registered {
				c.engine.UnregisterTool(added)
			}
			return err
		}
		registered[name] = struct{}{}
	}
	c.mu.Lock()
	if c.session == session && c.ctx.Err() == nil {
		c.tools = registered
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	for name := range registered {
		c.engine.UnregisterTool(name)
	}
	return nil
}

func mcpInputSchema(value any) (map[string]any, error) {
	if value == nil {
		return map[string]any{"type": "object"}, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil || schema == nil {
		return nil, errors.New("expected a JSON object")
	}
	return schema, nil
}

func mcpCanonicalOutputSchema(value any) map[string]any {
	structured := map[string]any{}
	required := []string{"content"}
	if schema, ok := mcpSupportedOutputSchema(value); ok {
		structured = schema
		required = append(required, "structuredContent")
	}
	return objectSchema(map[string]any{
		"content":           map[string]any{"type": "array", "items": map[string]any{}},
		"structuredContent": structured,
	}, required...)
}

func mcpSupportedOutputSchema(value any) (map[string]any, bool) {
	if value == nil {
		return nil, false
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	var schema map[string]any
	if json.Unmarshal(data, &schema) != nil || schema == nil || validateWorkflowSchema(schema, false) != nil {
		return nil, false
	}
	return schema, true
}

// MCPPublicToolName returns the stable model-facing name for one MCP tool.
func MCPPublicToolName(serverName, rawName string) string {
	joined := "mcp__" + serverName + "__" + rawName
	units := utf16.Encode([]rune(joined))
	var normalized strings.Builder
	normalized.Grow(len(units))
	for _, unit := range units {
		if unit < 128 && (unit == '_' || unit == '-' || unit >= '0' && unit <= '9' || unit >= 'A' && unit <= 'Z' || unit >= 'a' && unit <= 'z') {
			normalized.WriteByte(byte(unit))
		} else {
			normalized.WriteByte('_')
		}
	}
	name := normalized.String()
	if name == joined && len(name) <= 64 {
		return name
	}
	hash := sha256.Sum256([]byte(serverName + "\x00" + rawName))
	suffix := hex.EncodeToString(hash[:])[:12]
	return name[:min(len(name), 51)] + "_" + suffix
}

func (c *MCPConnection) bridgeTool(session *mcp.ClientSession, publicName, rawName, description string, parameters, output map[string]any) Tool {
	return Tool{
		Schema: ToolSchema{Name: publicName, Description: description, Parameters: parameters, Output: output},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var arguments any = map[string]any{}
			if raw := strings.TrimSpace(string(call.Arguments)); raw != "" {
				if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
					return ToolResult{}, fmt.Errorf("invalid arguments for %s: %w", publicName, err)
				}
				if _, ok := arguments.(map[string]any); !ok {
					arguments = map[string]any{}
				}
			}
			callCtx, cancel := context.WithTimeout(ctx, c.config.ToolCallTimeout)
			defer cancel()
			result, err := session.CallTool(callCtx, &mcp.CallToolParams{Name: rawName, Arguments: arguments})
			if err != nil {
				return ToolResult{}, err
			}
			value, content, err := canonicalMCPResult(result)
			if err != nil {
				return ToolResult{}, err
			}
			projected, err := projectMCPContent(ctx, c.engine, call, rawName, content)
			if err != nil {
				return ToolResult{}, err
			}
			if result.IsError {
				return ToolResult{}, errors.New(mcpContentText(projected))
			}
			return ToolResult{Content: projected, Value: value}, nil
		},
	}
}

func canonicalMCPResult(result *mcp.CallToolResult) (map[string]any, []any, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return nil, nil, err
	}
	var wire map[string]any
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, nil, err
	}
	content, _ := wire["content"].([]any)
	value := map[string]any{"content": content}
	if structured, ok := wire["structuredContent"]; ok {
		value["structuredContent"] = structured
	}
	return value, content, nil
}

func projectMCPContent(ctx context.Context, engine *Engine, call ToolCall, toolName string, content []any) ([]ContentBlock, error) {
	images, imageDiagnostics, err := prepareMCPImages(ctx, engine, call, content)
	if err != nil {
		return nil, err
	}
	blocks := make([]ContentBlock, 0, len(content))
	text := make([]string, 0)
	flush := func() {
		if len(text) > 0 {
			blocks = append(blocks, ContentBlock{Type: "text", Text: strings.Join(text, "\n")})
			text = text[:0]
		}
	}
	for index, value := range content {
		block, ok := value.(map[string]any)
		if !ok {
			text = append(text, "[unsupported MCP content block: expected an object]")
			continue
		}
		typ, _ := block["type"].(string)
		switch typ {
		case "text":
			if value, ok := block["text"].(string); ok {
				text = append(text, value)
			}
		case "image":
			flush()
			if imageDiagnostic := imageDiagnostics[index]; imageDiagnostic != "" {
				media, _ := block["mimeType"].(string)
				if media == "" {
					media = "unknown media type"
				}
				blocks = append(blocks, ContentBlock{Type: "text", Text: fmt.Sprintf("[image unavailable: %s; %s; raw image data remains available to programmatic callers]", media, imageDiagnostic)})
			} else if image := images[index]; image != nil {
				blocks = append(blocks, ContentBlock{Type: "image", Attachment: image})
			}
		case "resource_link":
			name, nameOK := block["name"].(string)
			uri, uriOK := block["uri"].(string)
			if !nameOK || !uriOK {
				text = append(text, "[resource link unavailable: the MCP block is missing its name or URI]")
			} else {
				text = append(text, fmt.Sprintf("Resource link: %s (%s)", name, uri))
			}
		case "audio":
			media, _ := block["mimeType"].(string)
			if media == "" {
				media = "unknown media type"
			}
			text = append(text, fmt.Sprintf("[audio result unsupported: %s; raw audio data remains available to programmatic callers]", media))
		case "resource":
			text = append(text, "[embedded resource unsupported; raw resource data remains available to programmatic callers]")
		default:
			text = append(text, fmt.Sprintf("[unsupported MCP content type: %s]", typ))
		}
	}
	flush()
	if len(blocks) == 0 {
		return []ContentBlock{{Type: "text", Text: fmt.Sprintf("(%s returned no model-visible content)", toolName)}}, nil
	}
	return blocks, nil
}

func prepareMCPImages(ctx context.Context, engine *Engine, call ToolCall, content []any) (map[int]*ImageAttachmentRef, map[int]string, error) {
	decoded := make([]*decodedImageInput, len(content))
	imageIndexes := make([]int, 0)
	diagnostics := make(map[int]string)
	for index, value := range content {
		block, ok := value.(map[string]any)
		if !ok || block["type"] != "image" {
			continue
		}
		imageIndexes = append(imageIndexes, index)
		media, _ := block["mimeType"].(string)
		data, _ := block["data"].(string)
		if imageExtension(media) == "" {
			diagnostics[index] = "the declared media type is not PNG, JPEG, WebP, or GIF"
			continue
		}
		decodedBytes, err := base64.StdEncoding.DecodeString(data)
		if err != nil || base64.StdEncoding.EncodeToString(decodedBytes) != data {
			diagnostics[index] = "the image data is not canonical base64"
			continue
		}
		input := decodedImageInput{mediaType: media, data: decodedBytes}
		decoded[index] = &input
	}
	if len(imageIndexes) == 0 {
		return nil, diagnostics, nil
	}
	if len(diagnostics) > 0 {
		for _, index := range imageIndexes {
			if diagnostics[index] == "" {
				diagnostics[index] = "another image in the same result was invalid"
			}
		}
		return nil, diagnostics, nil
	}
	selection, routeErr := imageModelSelection(engine, call)
	if routeErr != nil {
		reason := "the current model route could not be resolved"
		for _, index := range imageIndexes {
			diagnostics[index] = reason
		}
		return nil, diagnostics, nil
	}
	model, routeErr := resolveExactModelInfo(ctx, engine, selection)
	if routeErr != nil {
		if ctx.Err() != nil {
			return nil, nil, context.Cause(ctx)
		}
		reason := "the current model route could not be verified"
		if errors.Is(routeErr, errImageRouteUnresolved) {
			reason = "the current model route could not be resolved"
		}
		for _, index := range imageIndexes {
			diagnostics[index] = reason
		}
		return nil, diagnostics, nil
	}
	if !containsString(model.InputModalities, "image") {
		for _, index := range imageIndexes {
			diagnostics[index] = fmt.Sprintf("model %q does not declare image input", selection.Model)
		}
		return nil, diagnostics, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, context.Cause(ctx)
	}
	if err := validateDecodedImageBatch(decoded, maxImagesPerMessage, maxMessageImageBytes); err != nil {
		for _, index := range imageIndexes {
			diagnostics[index] = "image admission rejected the result: " + err.Error()
		}
		return nil, diagnostics, nil
	}
	prepared, err := engine.prepareDecodedImageBatch(ctx, decoded)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, context.Cause(ctx)
		}
		reason := "durable image storage rejected the result"
		if isImageAdmissionFailure(err) {
			reason = "image admission rejected the result: " + err.Error()
		}
		for _, index := range imageIndexes {
			diagnostics[index] = reason
		}
		return nil, diagnostics, nil
	}
	refs := map[int]*ImageAttachmentRef{}
	for _, index := range imageIndexes {
		image := prepared[index]
		if image == nil {
			continue
		}
		if err := engine.storePreparedImage(*image); err != nil {
			for _, imageIndex := range imageIndexes {
				diagnostics[imageIndex] = "durable image storage rejected the result"
			}
			return nil, diagnostics, nil
		}
		ref := image.ref
		refs[index] = &ref
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, context.Cause(ctx)
	}
	return refs, diagnostics, nil
}

func isImageAdmissionFailure(err error) bool {
	var marked *imageAdmissionFailure
	if errors.As(err, &marked) {
		return true
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"image batch exceeds", "unsupported image media type", "image is empty", "image exceeds",
		"image data is malformed", "declared image media type", "image cannot be encoded",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func mcpContentText(content []ContentBlock) string {
	parts := make([]string, 0, len(content))
	for _, block := range content {
		if block.Text != "" {
			parts = append(parts, block.Text)
		}
	}
	if len(parts) == 0 {
		return "MCP tool returned an error"
	}
	return strings.Join(parts, "\n")
}

func (c *MCPConnection) clearSession(session *mcp.ClientSession) {
	c.mu.Lock()
	if c.session == session {
		c.session = nil
	}
	c.mu.Unlock()
}

func (c *MCPConnection) unregisterTools() {
	c.mu.Lock()
	tools := c.tools
	c.tools = map[string]struct{}{}
	c.mu.Unlock()
	for name := range tools {
		c.engine.UnregisterTool(name)
	}
}

// Close stops reconnection, disconnects the current server, and unregisters its tools.
func (c *MCPConnection) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.cancel()
		c.mu.Lock()
		session := c.session
		c.session = nil
		c.mu.Unlock()
		if session != nil {
			closeErr = session.Close()
		}
		<-c.done
		c.unregisterTools()
		mcpConnections.Lock()
		if servers := mcpConnections.byEngine[c.engine]; servers != nil {
			delete(servers, c.config.ServerName)
			if len(servers) == 0 {
				delete(mcpConnections.byEngine, c.engine)
			}
		}
		mcpConnections.Unlock()
	})
	return closeErr
}

func closeMCPConnections(engine *Engine) {
	mcpConnections.Lock()
	servers := mcpConnections.byEngine[engine]
	connections := make([]*MCPConnection, 0, len(servers))
	for _, connection := range servers {
		connections = append(connections, connection)
	}
	mcpConnections.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func mcpConnectionsSnapshot(engine *Engine) map[string]*MCPConnection {
	mcpConnections.Lock()
	defer mcpConnections.Unlock()
	result := map[string]*MCPConnection{}
	for name, connection := range mcpConnections.byEngine[engine] {
		result[name] = connection
	}
	return result
}

// reconcileMCPServers applies profile-owned MCP rows by stable serverName.
// Existing equal connections keep their sessions and discovered tools; a
// changed or removed row is closed before its replacement is connected.
func (e *Engine) reconcileMCPServers(ctx context.Context, desired []MCPConfig) error {
	want := make(map[string]MCPConfig, len(desired))
	for _, raw := range desired {
		config, _, err := normalizeMCPConfig(raw)
		if err != nil {
			return err
		}
		if _, exists := want[config.ServerName]; exists {
			return fmt.Errorf("mcp-client: serverName %q is configured more than once", config.ServerName)
		}
		want[config.ServerName] = config
	}
	current := mcpConnectionsSnapshot(e)
	closed := make(map[string]MCPConfig)
	for name, connection := range current {
		config, _, _ := normalizeMCPConfig(connection.config)
		candidate, keep := want[name]
		if keep && reflect.DeepEqual(config, candidate) {
			delete(want, name)
			continue
		}
		closed[name] = config
		if err := connection.Close(); err != nil {
			return err
		}
	}
	added := make([]*MCPConnection, 0, len(want))
	names := make([]string, 0, len(want))
	for name := range want {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		config := want[name]
		connection, err := e.ConnectMCP(ctx, config)
		if err != nil {
			rollbackErrs := []error{err}
			for _, candidate := range added {
				if closeErr := candidate.Close(); closeErr != nil {
					rollbackErrs = append(rollbackErrs, closeErr)
				}
			}
			closedNames := make([]string, 0, len(closed))
			for previousName := range closed {
				closedNames = append(closedNames, previousName)
			}
			sort.Strings(closedNames)
			for _, previousName := range closedNames {
				if _, restoreErr := e.ConnectMCP(context.Background(), closed[previousName]); restoreErr != nil {
					rollbackErrs = append(rollbackErrs, fmt.Errorf("restore MCP server %q: %w", previousName, restoreErr))
				}
			}
			return errors.Join(rollbackErrs...)
		}
		added = append(added, connection)
	}
	return nil
}
