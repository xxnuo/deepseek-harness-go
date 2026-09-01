package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// DeepSeekLlmAPIExtensionRequest is the immutable request view exposed to a
// contributor while the official DeepSeek wire body is being assembled.
// Providers must treat Body as read-only and should return JSON values only.
type DeepSeekLlmAPIExtensionRequest struct {
	Body      map[string]any
	SessionID string
	Purpose   string
}

// DeepSeekLlmAPIExtensionProvider prepares one top-level request field.
type DeepSeekLlmAPIExtensionProvider func(context.Context, DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error)

// DeepSeekLlmAPIExtensionContribution is a detached field and an optional
// post-HTTP-2xx commit. Present distinguishes a JSON null from no contribution.
// Accept is never called when transport or HTTP fails.
type DeepSeekLlmAPIExtensionContribution struct {
	Present bool
	Value   any
	Accept  func() error
}

type deepSeekExtensionRegistration struct {
	field    string
	provider DeepSeekLlmAPIExtensionProvider
	seq      uint64
}

// PreparedDeepSeekLlmAPIExtensions contains detached fields and an idempotent
// joint acceptance transaction for one request.
type PreparedDeepSeekLlmAPIExtensions struct {
	Fields map[string]any
	accept func() error
	once   sync.Once
	err    error
}

// Accept commits every captured contribution exactly once. All callbacks are
// attempted before a combined error is returned, matching Promise.allSettled.
func (p *PreparedDeepSeekLlmAPIExtensions) Accept() error {
	if p == nil || p.accept == nil {
		return nil
	}
	p.once.Do(func() { p.err = p.accept() })
	return p.err
}

// deepSeekLlmAPIExtensionRegistry owns field names and registration lifetime.
type deepSeekLlmAPIExtensionRegistry struct {
	mu            sync.RWMutex
	registrations map[string]*deepSeekExtensionRegistration
	nextSeq       uint64
	closed        bool
}

func newDeepSeekLlmAPIExtensionRegistry() *deepSeekLlmAPIExtensionRegistry {
	return &deepSeekLlmAPIExtensionRegistry{registrations: make(map[string]*deepSeekExtensionRegistration)}
}

func validDeepSeekExtensionField(field string) bool {
	return field != "" && strings.TrimSpace(field) == field
}

// register adds one sole owner for a top-level field. The disposer is safe to
// call repeatedly and only removes the registration it created.
func (r *deepSeekLlmAPIExtensionRegistry) register(field string, provider DeepSeekLlmAPIExtensionProvider) (func() error, error) {
	if r == nil {
		return nil, errors.New("deepseek-llm-api-extensions: registry is nil")
	}
	if !validDeepSeekExtensionField(field) {
		return nil, errors.New("deepseek-llm-api-extensions: field must be non-blank and trimmed")
	}
	if provider == nil {
		return nil, errors.New("deepseek-llm-api-extensions: provider is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("deepseek-llm-api-extensions: registry is inactive")
	}
	if _, exists := r.registrations[field]; exists {
		return nil, fmt.Errorf("deepseek-llm-api-extensions: field %q is already registered", field)
	}
	r.nextSeq++
	registration := &deepSeekExtensionRegistration{field: field, provider: provider, seq: r.nextSeq}
	r.registrations[field] = registration
	var once sync.Once
	var disposeErr error
	dispose := func() error {
		once.Do(func() {
			r.mu.Lock()
			if r.registrations[field] == registration {
				delete(r.registrations, field)
			}
			r.mu.Unlock()
		})
		return disposeErr
	}
	return dispose, nil
}

func (r *deepSeekLlmAPIExtensionRegistry) snapshot() ([]*deepSeekExtensionRegistration, error) {
	if r == nil {
		return nil, errors.New("deepseek-llm-api-extensions: registry is nil")
	}
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errors.New("deepseek-llm-api-extensions: registry is inactive")
	}
	rows := make([]*deepSeekExtensionRegistration, 0, len(r.registrations))
	for _, registration := range r.registrations {
		rows = append(rows, registration)
	}
	r.mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].seq < rows[j].seq })
	return rows, nil
}

func cloneDeepSeekJSON(value any) (any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// prepare runs all providers concurrently, then clones every value through a
// JSON round trip so a provider cannot mutate the outgoing request afterward.
func (r *deepSeekLlmAPIExtensionRegistry) prepare(ctx context.Context, request DeepSeekLlmAPIExtensionRequest) (*PreparedDeepSeekLlmAPIExtensions, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	base, err := cloneDeepSeekJSON(request.Body)
	if err != nil {
		return nil, fmt.Errorf("deepseek-llm-api-extensions: request body is not JSON-serializable: %w", err)
	}
	body, ok := base.(map[string]any)
	if !ok {
		return nil, errors.New("deepseek-llm-api-extensions: request body must be a JSON object")
	}
	request.Body = body
	registrations, err := r.snapshot()
	if err != nil {
		return nil, err
	}
	type result struct {
		index        int
		registration *deepSeekExtensionRegistration
		value        DeepSeekLlmAPIExtensionContribution
		err          error
	}
	results := make([]result, len(registrations))
	resultCh := make(chan result, len(registrations))
	for index, registration := range registrations {
		go func(index int, registration *deepSeekExtensionRegistration) {
			item := result{index: index, registration: registration}
			defer func() {
				if recovered := recover(); recovered != nil {
					item.err = fmt.Errorf("deepseek-llm-api-extensions: provider for field %q panicked: %v", registration.field, recovered)
				}
				resultCh <- item
			}()
			if err := ctx.Err(); err != nil {
				item.err = err
				return
			}
			// Keep providers from observing or mutating one another's request
			// view. The TypeScript adapter gives every provider a frozen clone;
			// independent Go maps provide the equivalent aliasing boundary.
			bodyValue, cloneErr := cloneDeepSeekJSON(request.Body)
			if cloneErr != nil {
				item.err = cloneErr
				return
			}
			providerRequest := request
			providerRequest.Body, _ = bodyValue.(map[string]any)
			item.value, item.err = registration.provider(ctx, providerRequest)
		}(index, registration)
	}
	for range registrations {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case item := <-resultCh:
			results[item.index] = item
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
	}
	for _, item := range results {
		if item.err != nil {
			return nil, item.err
		}
	}
	fields := make(map[string]any)
	callbacks := make([]func() error, 0, len(results))
	for _, item := range results {
		if !item.value.Present && item.value.Value == nil {
			continue
		}
		value, err := cloneDeepSeekJSON(item.value.Value)
		if err != nil {
			return nil, fmt.Errorf("deepseek-llm-api-extensions: field %q is not JSON-serializable: %w", item.registration.field, err)
		}
		fields[item.registration.field] = value
		if item.value.Accept != nil {
			callbacks = append(callbacks, item.value.Accept)
		}
	}
	prepared := &PreparedDeepSeekLlmAPIExtensions{Fields: fields}
	prepared.accept = func() error {
		if len(callbacks) == 0 {
			return nil
		}
		errs := make([]error, len(callbacks))
		var callbacksWG sync.WaitGroup
		for index, callback := range callbacks {
			callbacksWG.Add(1)
			go func(index int, callback func() error) {
				defer callbacksWG.Done()
				defer func() {
					if recovered := recover(); recovered != nil {
						errs[index] = fmt.Errorf("panic in acceptance callback: %v", recovered)
					}
				}()
				errs[index] = callback()
			}(index, callback)
		}
		callbacksWG.Wait()
		joined := make([]error, 0, len(errs))
		for _, err := range errs {
			if err != nil {
				joined = append(joined, err)
			}
		}
		return errors.Join(joined...)
	}
	return prepared, nil
}

func (r *deepSeekLlmAPIExtensionRegistry) close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.closed = true
	r.registrations = make(map[string]*deepSeekExtensionRegistration)
	r.mu.Unlock()
	return nil
}

// RegisterDeepSeekLlmAPIExtension exposes the constrained Go equivalent of
// the upstream registry. The returned disposer releases the field owner.
func (e *Engine) RegisterDeepSeekLlmAPIExtension(field string, provider DeepSeekLlmAPIExtensionProvider) (func() error, error) {
	if e == nil {
		return nil, errors.New("engine is nil")
	}
	e.deepSeekExtensionsMu.RLock()
	registry := e.deepSeekExtensions
	e.deepSeekExtensionsMu.RUnlock()
	if registry == nil {
		return nil, errors.New("deepseek-llm-api-extensions: registry is unavailable")
	}
	return registry.register(field, provider)
}

func (e *Engine) prepareDeepSeekLlmAPIExtensions(ctx context.Context, body map[string]any, request ChatRequest) (*PreparedDeepSeekLlmAPIExtensions, error) {
	e.deepSeekExtensionsMu.RLock()
	registry := e.deepSeekExtensions
	e.deepSeekExtensionsMu.RUnlock()
	if registry == nil {
		return &PreparedDeepSeekLlmAPIExtensions{Fields: map[string]any{}}, nil
	}
	return registry.prepare(ctx, DeepSeekLlmAPIExtensionRequest{Body: body, SessionID: request.SessionID, Purpose: request.Purpose})
}

// DeepSeekSessionLogExtension is the versioned incremental log payload.
type DeepSeekSessionLogExtension struct {
	Version    int           `json:"version"`
	Session    SessionHeader `json:"session"`
	AfterSeq   int           `json:"afterSeq"`
	ThroughSeq int           `json:"throughSeq"`
	Events     []Event       `json:"events"`
}

// DeepSeekPluginPackageIdentity identifies one active owning package.
type DeepSeekPluginPackageIdentity struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// DeepSeekPluginPackageInventoryExtension is the versioned package payload.
type DeepSeekPluginPackageInventoryExtension struct {
	Version  int                             `json:"version"`
	Packages []DeepSeekPluginPackageIdentity `json:"packages"`
}

type deepSeekPackageIdentityCacheResult struct {
	identity DeepSeekPluginPackageIdentity
	present  bool
}

func acceptedDeepSeekSessionLogThrough(header SessionHeader, events []Event) (int, error) {
	through := -1
	for _, event := range events {
		if event.Type != "session-log-deepseek/delivery-accepted" {
			continue
		}
		data, ok := event.Data.(map[string]any)
		if !ok {
			return 0, fmt.Errorf("session-log-deepseek: malformed acceptance watermark at seq %d", event.Seq)
		}
		sessionID, ok := data["sessionId"].(string)
		throughValue, throughOK := integerJSONValue(data["throughSeq"])
		if !ok || sessionID == "" || !throughOK || throughValue < 0 || throughValue >= event.Seq {
			return 0, fmt.Errorf("session-log-deepseek: malformed acceptance watermark at seq %d", event.Seq)
		}
		// Seeded acceptance markers belong to the inherited parent log and
		// must never advance the child upload cursor.
		if header.ParentSession != "" && event.Seq < header.SeedLength {
			continue
		}
		if sessionID == header.ID && throughValue > through {
			through = throughValue
		}
	}
	return through, nil
}

func integerJSONValue(value any) (int, bool) {
	maxInt := int64(^uint(0) >> 1)
	maxValue := maxJSONSafeInteger
	if maxInt < maxValue {
		maxValue = maxInt
	}
	var integer int64
	switch value := value.(type) {
	case int:
		integer = int64(value)
	case int8:
		integer = int64(value)
	case int16:
		integer = int64(value)
	case int32:
		integer = int64(value)
	case int64:
		integer = value
	case uint:
		if uint64(value) > uint64(maxValue) {
			return 0, false
		}
		return int(value), true
	case uint8:
		return int(value), true
	case uint16:
		return int(value), true
	case uint32:
		if uint64(value) > uint64(maxValue) {
			return 0, false
		}
		return int(value), true
	case uint64:
		if value > uint64(maxValue) {
			return 0, false
		}
		return int(value), true
	case float32:
		integer = int64(value)
		if float32(integer) != value {
			return 0, false
		}
	case float64:
		integer = int64(value)
		if float64(integer) != value {
			return 0, false
		}
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return 0, false
		}
		integer = parsed
	default:
		return 0, false
	}
	if integer < 0 || integer > maxValue {
		return 0, false
	}
	return int(integer), true
}

func cloneDeepSeekEvents(events []Event) []Event {
	if len(events) == 0 {
		return nil
	}
	cloned := make([]Event, len(events))
	for index, event := range events {
		cloned[index] = event
		cloned[index].Data = cloneJSON(event.Data)
		cloned[index].SurfaceOp = cloneJSON(event.SurfaceOp)
		if event.SourceEventSeqs != nil {
			cloned[index].SourceEventSeqs = append([]int(nil), event.SourceEventSeqs...)
		}
	}
	return cloned
}

func (e *Engine) prepareDeepSeekSessionLog(request ChatRequest) (DeepSeekLlmAPIExtensionContribution, error) {
	e.mu.RLock()
	enabled := boolConfigValue(e.cfg.DeepSeekSessionLogEnabled, false)
	session := e.sessions[request.SessionID]
	e.mu.RUnlock()
	if !enabled || request.SessionID == "" || session == nil {
		return DeepSeekLlmAPIExtensionContribution{}, nil
	}
	session.mu.Lock()
	events := cloneDeepSeekEvents(session.Events)
	header := session.Header
	session.mu.Unlock()
	if len(events) == 0 {
		return DeepSeekLlmAPIExtensionContribution{}, nil
	}
	afterSeq, err := acceptedDeepSeekSessionLogThrough(header, events)
	if err != nil {
		return DeepSeekLlmAPIExtensionContribution{}, err
	}
	throughSeq := len(events) - 1
	suffix := cloneDeepSeekEvents(events[afterSeq+1:])
	value := DeepSeekSessionLogExtension{Version: 1, Session: header, AfterSeq: afterSeq, ThroughSeq: throughSeq, Events: suffix}
	return DeepSeekLlmAPIExtensionContribution{Present: true, Value: value, Accept: func() error {
		_, appendErr := e.appendEvent(session, "session-log-deepseek/delivery-accepted", map[string]any{
			"sessionId": header.ID, "throughSeq": throughSeq,
		})
		return appendErr
	}}, nil
}

func deepSeekPackageName(specifier string) string {
	if strings.HasPrefix(specifier, ".") || filepath.IsAbs(specifier) || strings.Contains(specifier, ":") {
		return ""
	}
	parts := strings.Split(specifier, "/")
	if len(parts) == 0 || parts[0] == "" {
		return ""
	}
	if strings.HasPrefix(specifier, "@") {
		if len(parts) < 2 {
			return ""
		}
		return parts[0] + "/" + parts[1]
	}
	return parts[0]
}

func normalizeDeepSeekPackageAnchor(anchor string) string {
	if anchor == "" {
		return ""
	}
	if strings.HasPrefix(anchor, "file://") {
		if parsed, err := url.Parse(anchor); err == nil && (parsed.Host == "" || parsed.Host == "localhost") {
			if path, err := url.PathUnescape(parsed.Path); err == nil {
				anchor = path
			}
		}
	}
	if absolute, err := filepath.Abs(anchor); err == nil {
		anchor = absolute
	}
	return filepath.Clean(anchor)
}

func appendDeepSeekAnchor(anchors []string, seen map[string]struct{}, anchor string) []string {
	anchor = normalizeDeepSeekPackageAnchor(anchor)
	if anchor == "" {
		return anchors
	}
	if _, exists := seen[anchor]; exists {
		return anchors
	}
	seen[anchor] = struct{}{}
	return append(anchors, anchor)
}

func (e *Engine) deepSeekPluginBarePackageBase() string {
	e.mu.RLock()
	cfg := e.cfg
	e.mu.RUnlock()
	if cfg.PluginDir != "" {
		return cfg.PluginDir
	}
	if len(cfg.PluginDirs) > 0 {
		return cfg.PluginDirs[0]
	}
	return cfg.Workspace
}

func readDeepSeekPackageIdentity(manifest string, allowAnonymous bool) (DeepSeekPluginPackageIdentity, bool, error) {
	data, err := os.ReadFile(manifest)
	if err != nil {
		return DeepSeekPluginPackageIdentity{}, false, err
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(data, &document); err != nil || document == nil {
		if err == nil {
			err = errors.New("manifest is not a JSON object")
		}
		return DeepSeekPluginPackageIdentity{}, false, fmt.Errorf("plugin-package-inventory-deepseek: %s has invalid package metadata: %w", manifest, err)
	}
	nameRaw, hasName := document["name"]
	if allowAnonymous && !hasName {
		return DeepSeekPluginPackageIdentity{}, false, nil
	}
	var name, version string
	versionRaw, hasVersion := document["version"]
	if !hasName || json.Unmarshal(nameRaw, &name) != nil || name == "" ||
		!hasVersion || json.Unmarshal(versionRaw, &version) != nil || version == "" {
		return DeepSeekPluginPackageIdentity{}, false, fmt.Errorf("plugin-package-inventory-deepseek: %s must declare non-empty name and version", manifest)
	}
	return DeepSeekPluginPackageIdentity{Name: name, Version: version}, true, nil
}

func nearestDeepSeekManifest(path string) string {
	if path == "" {
		return ""
	}
	path, _ = filepath.Abs(path)
	path = filepath.Dir(path)
	for {
		candidate := filepath.Join(path, "package.json")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(path)
		if parent == path {
			return ""
		}
		path = parent
	}
}

func resolveDeepSeekBareManifest(packageName string, anchors []string) string {
	for _, anchor := range anchors {
		base := normalizeDeepSeekPackageAnchor(anchor)
		if base == "" {
			continue
		}
		if info, err := os.Stat(base); err == nil && !info.IsDir() {
			base = filepath.Dir(base)
		}
		// PluginDirs are commonly supplied as an existing node_modules
		// directory. Node's resolver treats that directory as a package root;
		// do the direct lookup before walking upward to avoid appending a second
		// node_modules segment.
		if filepath.Base(base) == "node_modules" {
			manifest := filepath.Join(base, filepath.FromSlash(packageName), "package.json")
			if info, err := os.Stat(manifest); err == nil && !info.IsDir() {
				return manifest
			}
		}
		for current := base; ; current = filepath.Dir(current) {
			manifest := filepath.Join(current, "node_modules", filepath.FromSlash(packageName), "package.json")
			if info, err := os.Stat(manifest); err == nil && !info.IsDir() {
				return manifest
			}
			parent := filepath.Dir(current)
			if parent == current {
				break
			}
		}
	}
	return ""
}

func indexDeepSeekPackageTreeManifests(roots []string) map[string]string {
	manifests := make(map[string]string)
	seenRoots := make(map[string]bool)
	for _, root := range roots {
		if root == "" {
			continue
		}
		root, _ = filepath.Abs(root)
		if seenRoots[root] {
			continue
		}
		seenRoots[root] = true
		level := []string{root}
		for depth := 0; depth <= 2 && len(level) > 0; depth++ {
			next := make([]string, 0)
			for _, dir := range level {
				manifest := filepath.Join(dir, "package.json")
				if data, err := os.ReadFile(manifest); err == nil {
					var document struct {
						Name string `json:"name"`
					}
					if json.Unmarshal(data, &document) == nil && document.Name != "" {
						if _, exists := manifests[document.Name]; !exists {
							manifests[document.Name] = manifest
						}
					}
				}
				if depth == 2 {
					continue
				}
				children, err := os.ReadDir(dir)
				if err != nil {
					continue
				}
				for _, child := range children {
					if !child.IsDir() || child.Name() == ".git" || child.Name() == "dist" || child.Name() == "node_modules" {
						continue
					}
					next = append(next, filepath.Join(dir, child.Name()))
				}
			}
			level = next
		}
	}
	return manifests
}

func (e *Engine) packageIdentityForPluginEntry(entry PluginInventoryEntry, packageTreeManifests map[string]string) (DeepSeekPluginPackageIdentity, bool, error) {
	if entry.PackageName != "" || entry.PackageVersion != "" {
		if strings.TrimSpace(entry.PackageName) == "" || strings.TrimSpace(entry.PackageVersion) == "" {
			return DeepSeekPluginPackageIdentity{}, false, fmt.Errorf("plugin-package-inventory-deepseek: entry %q has incomplete package identity", entry.EntryID)
		}
		return DeepSeekPluginPackageIdentity{Name: entry.PackageName, Version: entry.PackageVersion}, true, nil
	}
	name := entry.ModuleName
	if name == "" || strings.HasPrefix(name, "cordis:") {
		return DeepSeekPluginPackageIdentity{}, false, nil
	}
	e.mu.RLock()
	cfg := e.cfg
	e.mu.RUnlock()
	moduleBase := entry.ModuleBase
	if moduleBase == "" {
		moduleBase = cfg.Workspace
	}
	bareBase := entry.BarePackageBase
	anchors := make([]string, 0, 5+len(cfg.PluginDirs))
	seenAnchors := make(map[string]struct{}, 5+len(cfg.PluginDirs))
	if bareBase != "" {
		anchors = appendDeepSeekAnchor(anchors, seenAnchors, bareBase)
	}
	anchors = appendDeepSeekAnchor(anchors, seenAnchors, moduleBase)
	anchors = appendDeepSeekAnchor(anchors, seenAnchors, cfg.PluginDir)
	anchors = appendDeepSeekAnchor(anchors, seenAnchors, cfg.Workspace)
	for _, configured := range cfg.PluginDirs {
		anchors = appendDeepSeekAnchor(anchors, seenAnchors, configured)
	}
	cacheKey := strings.Join([]string{name, normalizeDeepSeekPackageAnchor(moduleBase), normalizeDeepSeekPackageAnchor(bareBase)}, "\x00")
	if cached, ok := e.deepSeekPackageIdentityCached(cacheKey); ok {
		return cached.identity, cached.present, nil
	}
	resolve := func() (DeepSeekPluginPackageIdentity, bool, error) {
		if packageName := deepSeekPackageName(name); packageName != "" {
			manifest := resolveDeepSeekBareManifest(packageName, anchors)
			if manifest == "" {
				manifest = packageTreeManifests[packageName]
			}
			if manifest == "" {
				return DeepSeekPluginPackageIdentity{}, false, fmt.Errorf("plugin-package-inventory-deepseek: cannot resolve active package %q", packageName)
			}
			return readDeepSeekPackageIdentity(manifest, false)
		}
		path := name
		if strings.Contains(path, ":") {
			parsed, err := url.Parse(path)
			if err != nil {
				return DeepSeekPluginPackageIdentity{}, false, fmt.Errorf("plugin-package-inventory-deepseek: invalid file URL %q", path)
			}
			if parsed.Scheme != "file" {
				return DeepSeekPluginPackageIdentity{}, false, nil
			}
			if parsed.Host != "" && parsed.Host != "localhost" {
				return DeepSeekPluginPackageIdentity{}, false, fmt.Errorf("plugin-package-inventory-deepseek: invalid file URL %q", path)
			}
			path = parsed.Path
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(moduleBase, filepath.FromSlash(path))
		}
		manifest := nearestDeepSeekManifest(path)
		if manifest == "" {
			return DeepSeekPluginPackageIdentity{}, false, nil
		}
		return readDeepSeekPackageIdentity(manifest, true)
	}
	identity, present, err := e.cacheDeepSeekPackageIdentity(cacheKey, resolve)
	return identity, present, err
}

func (e *Engine) deepSeekPackageIdentityCached(key string) (deepSeekPackageIdentityCacheResult, bool) {
	e.deepSeekPackageIdentityMu.Lock()
	defer e.deepSeekPackageIdentityMu.Unlock()
	if e.deepSeekPackageIdentityCache == nil {
		return deepSeekPackageIdentityCacheResult{}, false
	}
	result, ok := e.deepSeekPackageIdentityCache[key]
	return result, ok
}

func (e *Engine) cacheDeepSeekPackageIdentity(key string, resolve func() (DeepSeekPluginPackageIdentity, bool, error)) (DeepSeekPluginPackageIdentity, bool, error) {
	e.deepSeekPackageIdentityMu.Lock()
	defer e.deepSeekPackageIdentityMu.Unlock()
	if e.deepSeekPackageIdentityCache == nil {
		e.deepSeekPackageIdentityCache = make(map[string]deepSeekPackageIdentityCacheResult)
	}
	if result, ok := e.deepSeekPackageIdentityCache[key]; ok {
		return result.identity, result.present, nil
	}
	identity, present, err := resolve()
	if err != nil {
		return DeepSeekPluginPackageIdentity{}, false, err
	}
	e.deepSeekPackageIdentityCache[key] = deepSeekPackageIdentityCacheResult{identity: identity, present: present}
	return identity, present, nil
}

func (e *Engine) presetPluginInventoryEntries(session *Session) []PluginInventoryEntry {
	if session == nil {
		return nil
	}
	session.mu.Lock()
	preset := sessionAgentPreset(session.Header, session.Events)
	parentID := session.Header.ParentSession
	generation := session.presetRuntime
	session.mu.Unlock()
	if preset == "" {
		return nil
	}
	if generation == nil {
		var err error
		generation, err = e.presetRuntimeForSession(session, canonicalPresetID(preset), parentID)
		if err != nil {
			return nil
		}
	}
	entries := clonePluginInventory(generation.pluginInventory)
	for index := range entries {
		if entries[index].ModuleBase == "" {
			entries[index].ModuleBase = filepath.Dir(generation.path)
		}
		if entries[index].BarePackageBase == "" {
			entries[index].BarePackageBase = e.deepSeekPluginBarePackageBase()
		}
	}
	return entries
}

func (e *Engine) prepareDeepSeekPluginInventory(request ChatRequest) (DeepSeekLlmAPIExtensionContribution, error) {
	e.mu.RLock()
	enabled := boolConfigValue(e.cfg.DeepSeekPluginInventoryEnabled, true)
	packageRoots := append([]string(nil), e.cfg.PluginDirs...)
	packageRoots = append(packageRoots, e.cfg.PluginDir)
	e.mu.RUnlock()
	if !enabled {
		return DeepSeekLlmAPIExtensionContribution{}, nil
	}
	entries := e.pluginInventorySnapshot()
	if request.SessionID != "" {
		e.mu.RLock()
		session := e.sessions[request.SessionID]
		e.mu.RUnlock()
		entries = append(entries, e.presetPluginInventoryEntries(session)...)
	}
	packageTreeManifests := indexDeepSeekPackageTreeManifests(packageRoots)
	unique := make(map[string]DeepSeekPluginPackageIdentity)
	for _, entry := range entries {
		if entry.Group || !entry.Enabled || entry.FiberPhase == nil || *entry.FiberPhase != "active" {
			continue
		}
		identity, present, err := e.packageIdentityForPluginEntry(entry, packageTreeManifests)
		if err != nil {
			return DeepSeekLlmAPIExtensionContribution{}, err
		}
		if !present {
			continue
		}
		unique[identity.Name+"\x00"+identity.Version] = identity
	}
	packages := make([]DeepSeekPluginPackageIdentity, 0, len(unique))
	for _, identity := range unique {
		packages = append(packages, identity)
	}
	sort.Slice(packages, func(i, j int) bool {
		if packages[i].Name != packages[j].Name {
			return packages[i].Name < packages[j].Name
		}
		return packages[i].Version < packages[j].Version
	})
	return DeepSeekLlmAPIExtensionContribution{Present: true, Value: DeepSeekPluginPackageInventoryExtension{Version: 1, Packages: packages}}, nil
}

func (e *Engine) registerBuiltinDeepSeekExtensions() error {
	if e.deepSeekExtensions == nil {
		return errors.New("deepseek-llm-api-extensions: registry is unavailable")
	}
	if _, err := e.deepSeekExtensions.register("dsh_session_log", func(_ context.Context, request DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
		return e.prepareDeepSeekSessionLog(ChatRequest{SessionID: request.SessionID, Purpose: request.Purpose})
	}); err != nil {
		return err
	}
	if _, err := e.deepSeekExtensions.register("dsh_plugin_packages", func(_ context.Context, request DeepSeekLlmAPIExtensionRequest) (DeepSeekLlmAPIExtensionContribution, error) {
		return e.prepareDeepSeekPluginInventory(ChatRequest{SessionID: request.SessionID, Purpose: request.Purpose})
	}); err != nil {
		return err
	}
	return nil
}
