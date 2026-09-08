package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	maxSubagentDiagnosticBytes = 4096
	diagnosticTruncationSuffix = "\n[diagnostic truncated]"
)

// SubagentStopReason is the provider-independent terminal state of one
// external subagent run.
type SubagentStopReason string

const (
	SubagentCompleted SubagentStopReason = "completed"
	SubagentMaxTokens SubagentStopReason = "max-tokens"
	SubagentRefusal   SubagentStopReason = "refusal"
	SubagentAborted   SubagentStopReason = "aborted"
	SubagentError     SubagentStopReason = "error"
)

// SubagentCapabilities declares which parent-enforced start options a
// provider can honor.
type SubagentCapabilities struct {
	OutputSchema bool `json:"outputSchema"`
	DepthLimit   bool `json:"depthLimit"`
	ToolFilter   bool `json:"toolFilter"`
	Persona      bool `json:"persona"`
	AgentOptions bool `json:"agentOptions"`
}

// SubagentServiceError carries the stable error codes exposed by the upstream
// subagent service. The name differs from the TypeScript class because
// SubagentError is retained as the public stop-reason constant in Go.
type SubagentServiceError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Cause   error  `json:"-"`
}

func (e *SubagentServiceError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *SubagentServiceError) Unwrap() error { return e.Cause }

func subagentServiceError(code, message string, cause error) error {
	return &SubagentServiceError{Code: code, Message: message, Cause: cause}
}

// IsSubagentServiceError reports whether err carries the requested stable code.
func IsSubagentServiceError(err error, code string) bool {
	var target *SubagentServiceError
	return errors.As(err, &target) && target.Code == code
}

// NoSubagentStartCapabilities returns the capability set shared by
// out-of-process providers.
func NoSubagentStartCapabilities() SubagentCapabilities { return SubagentCapabilities{} }

// SubagentStartRequest describes one one-shot external child run. CWD may be
// omitted when ParentSessionID names an Engine session with a workspace.
type SubagentStartRequest struct {
	ParentSessionID string
	CWD             string
	Label           string
	Prompt          []ContentBlock
	OutputSchema    map[string]any
	MaxDepth        *int
	AgentOptions    *SubagentAgentOptions
	ToolFilter      *SubagentToolFilter
	Persona         string
	Descriptor      SubagentDescriptorData
}

// SubagentAgentOptions selects child model defaults for providers that expose
// the corresponding start capability.
type SubagentAgentOptions struct {
	Provider        string `json:"provider,omitempty"`
	Model           string `json:"model,omitempty"`
	ReasoningEffort string `json:"reasoningEffort,omitempty"`
	MaxTokens       int    `json:"maxTokens,omitempty"`
}

// SubagentToolFilter limits the child tool catalog for capable providers.
type SubagentToolFilter struct {
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
}

// SubagentDescriptorVersion is the durable subagent/descriptor payload version.
const SubagentDescriptorVersion = 2

// SubagentResult is the terminal output of one published run.
type SubagentResult struct {
	Output     []ContentBlock     `json:"output"`
	Structured any                `json:"structured,omitempty"`
	Diagnostic string             `json:"diagnostic,omitempty"`
	StopReason SubagentStopReason `json:"stopReason"`
}

// SubagentProvider starts one-shot children. Startup errors are returned from
// Start; after publication a run always settles to a SubagentResult.
type SubagentProvider interface {
	Name() string
	Capabilities() SubagentCapabilities
	InheritsParentContext() bool
	Start(context.Context, SubagentStartRequest) (*SubagentRun, error)
}

// SubagentAgentRouteDefaultsProvider exposes provider-owned route defaults for
// callers that must validate a partial per-run AgentOptions override.
type SubagentAgentRouteDefaultsProvider interface {
	AgentRouteDefaults() SubagentAgentOptions
}

// SubagentProviderInfo is the stable library-facing provider descriptor.
type SubagentProviderInfo struct {
	Name                  string               `json:"name"`
	Capabilities          SubagentCapabilities `json:"capabilities"`
	InheritsParentContext bool                 `json:"inheritsParentContext"`
}

// subagentProviderBinding is the provider instance and registration generation
// observed during admission. A name-only lookup after an asynchronous
// preflight could accidentally hand the request to a replacement provider.
type subagentProviderBinding struct {
	name     string
	provider SubagentProvider
	token    uint64
}

// SubagentRun is one published asynchronous external child.
type SubagentRun struct {
	ID string

	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	result SubagentResult

	dispose     func() error
	disposeOnce sync.Once
	disposeDone chan struct{}
	disposeErr  error
	local       bool
}

func newSubagentRun(id string, cancel context.CancelFunc, dispose func() error) *SubagentRun {
	return &SubagentRun{ID: id, cancel: cancel, done: make(chan struct{}), dispose: dispose, disposeDone: make(chan struct{})}
}

func (r *SubagentRun) settle(result SubagentResult) {
	r.mu.Lock()
	select {
	case <-r.done:
		r.mu.Unlock()
		return
	default:
	}
	r.result = cloneSubagentResult(result)
	close(r.done)
	r.mu.Unlock()
}

// Done closes when the run has a terminal provider result.
func (r *SubagentRun) Done() <-chan struct{} { return r.done }

// Wait returns the published result or the caller's wait-context error. A
// wait timeout does not cancel the child; call Cancel or Dispose explicitly.
func (r *SubagentRun) Wait(ctx context.Context) (SubagentResult, error) {
	select {
	case <-ctx.Done():
		return SubagentResult{}, ctx.Err()
	case <-r.done:
		r.mu.Lock()
		result := cloneSubagentResult(r.result)
		r.mu.Unlock()
		return result, nil
	}
}

// Cancel requests local run cancellation. Providers do not depend on a
// cooperative child for terminal settlement.
func (r *SubagentRun) Cancel() {
	if r != nil && r.cancel != nil {
		r.cancel()
	}
}

// Dispose cancels the run and tears its child process down to quiescence.
// It is idempotent and safe for concurrent callers.
func (r *SubagentRun) Dispose() error {
	if r == nil {
		return nil
	}
	r.disposeOnce.Do(func() {
		r.Cancel()
		if r.dispose != nil {
			r.disposeErr = r.dispose()
		}
		close(r.disposeDone)
	})
	<-r.disposeDone
	return r.disposeErr
}

// Close implements io.Closer as an alias for Dispose.
func (r *SubagentRun) Close() error { return r.Dispose() }

func cloneSubagentResult(result SubagentResult) SubagentResult {
	result.Output = append([]ContentBlock(nil), result.Output...)
	if result.Structured != nil {
		result.Structured = cloneJSON(result.Structured)
	}
	result.Diagnostic = limitSubagentDiagnostic(result.Diagnostic)
	return result
}

func limitSubagentDiagnostic(diagnostic string) string {
	diagnostic = strings.ToValidUTF8(diagnostic, "\uFFFD")
	if len(diagnostic) <= maxSubagentDiagnosticBytes {
		return diagnostic
	}
	limit := maxSubagentDiagnosticBytes - len(diagnosticTruncationSuffix)
	for limit > 0 && !utf8.RuneStart(diagnostic[limit]) {
		limit--
	}
	return diagnostic[:limit] + diagnosticTruncationSuffix
}

// RegisterSubagentProvider installs a provider for programmatic consumers.
// Duplicate names are rejected rather than silently replacing live behavior.
func (e *Engine) RegisterSubagentProvider(provider SubagentProvider) error {
	if provider == nil {
		return errors.New("subagent provider is required")
	}
	name := strings.TrimSpace(provider.Name())
	if name == "" {
		return errors.New("subagent provider name is required")
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errors.New("engine is closed")
	}
	if e.subagentProviders == nil {
		e.subagentProviders = map[string]SubagentProvider{}
	}
	if e.subagentProviderTokens == nil {
		e.subagentProviderTokens = map[string]uint64{}
	}
	if _, exists := e.subagentProviders[name]; exists {
		e.mu.Unlock()
		return subagentServiceError("DUPLICATE_PROVIDER", fmt.Sprintf("a subagent provider named %q is already registered", name), nil)
	}
	e.nextSubagentProviderToken++
	token := e.nextSubagentProviderToken
	e.subagentProviders[name] = provider
	e.subagentProviderTokens[name] = token
	e.subagentProviderOrder = append(e.subagentProviderOrder, name)
	e.mu.Unlock()
	info := subagentProviderInfo(name, provider)
	if err := e.emitDynamicCordisEvent("subagent/provider-added", info); err != nil {
		rolledBack := false
		e.mu.Lock()
		if e.subagentProviderTokens[name] == token {
			delete(e.subagentProviders, name)
			delete(e.subagentProviderTokens, name)
			e.subagentProviderOrder = removeSubagentProviderName(e.subagentProviderOrder, name)
			rolledBack = true
		}
		e.mu.Unlock()
		if rolledBack {
			e.emitDynamicCordisScopedContained("", "subagent/provider-removed", name)
		}
		return err
	}
	return nil
}

// UnregisterSubagentProvider removes a provider for future starts. Published
// runs retain their provider-owned process and disposal path.
func (e *Engine) UnregisterSubagentProvider(name string) bool {
	e.mu.Lock()
	name = strings.TrimSpace(name)
	if _, ok := e.subagentProviders[name]; !ok {
		e.mu.Unlock()
		return false
	}
	delete(e.subagentProviders, name)
	delete(e.subagentProviderTokens, name)
	e.subagentProviderOrder = removeSubagentProviderName(e.subagentProviderOrder, name)
	e.mu.Unlock()
	e.emitDynamicCordisScopedContained("", "subagent/provider-removed", name)
	return true
}

// GetSubagentProvider returns the provider currently registered under name.
func (e *Engine) GetSubagentProvider(name string) SubagentProvider {
	e.mu.RLock()
	provider := e.subagentProviders[strings.TrimSpace(name)]
	e.mu.RUnlock()
	return provider
}

// ListSubagentProviders returns descriptors in registration order for custom
// CLIs and frontends.
func (e *Engine) ListSubagentProviders() []SubagentProviderInfo {
	e.mu.RLock()
	rows := make([]SubagentProviderInfo, 0, len(e.subagentProviderOrder))
	for _, name := range e.subagentProviderOrder {
		if provider := e.subagentProviders[name]; provider != nil {
			rows = append(rows, subagentProviderInfo(name, provider))
		}
	}
	e.mu.RUnlock()
	return rows
}

func subagentProviderInfo(name string, provider SubagentProvider) SubagentProviderInfo {
	return SubagentProviderInfo{
		Name: name, Capabilities: provider.Capabilities(),
		InheritsParentContext: provider.InheritsParentContext(),
	}
}

func removeSubagentProviderName(order []string, name string) []string {
	for index, current := range order {
		if current == name {
			return append(order[:index], order[index+1:]...)
		}
	}
	return order
}

func (e *Engine) snapshotSubagentProvider(providerName string) (subagentProviderBinding, error) {
	name := strings.TrimSpace(providerName)
	e.mu.RLock()
	closed := e.closed
	provider := e.subagentProviders[name]
	token := e.subagentProviderTokens[name]
	e.mu.RUnlock()
	if closed {
		return subagentProviderBinding{}, errors.New("engine is closed")
	}
	if provider == nil {
		return subagentProviderBinding{}, subagentServiceError("NO_PROVIDER", fmt.Sprintf("no subagent provider registered for %q", providerName), nil)
	}
	return subagentProviderBinding{name: name, provider: provider, token: token}, nil
}

// startSubagentWithBinding starts the exact provider admitted by a caller.
// The provider is checked while holding the registry read lock, then invoked
// through the captured interface value after the lock is released. A later
// unregister/re-register therefore cannot redirect this request to a new
// provider, while an earlier replacement fails closed.
func (e *Engine) startSubagentWithBinding(ctx context.Context, binding subagentProviderBinding, request SubagentStartRequest) (*SubagentRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := e.validateSubagentProviderBinding(binding); err != nil {
		return nil, err
	}
	return e.startSubagentOnProvider(ctx, binding.name, binding.provider, request)
}

func (e *Engine) validateSubagentProviderBinding(binding subagentProviderBinding) error {
	e.mu.RLock()
	closed := e.closed
	provider := e.subagentProviders[binding.name]
	token := e.subagentProviderTokens[binding.name]
	e.mu.RUnlock()
	if closed {
		return errors.New("engine is closed")
	}
	if provider == nil || token != binding.token {
		return fmt.Errorf("subagent provider %q changed while resolving the child LLM route; retry the delegation", binding.name)
	}
	return nil
}

// StartSubagent starts a registered one-shot provider. The returned run is
// caller-owned and must be disposed.
func (e *Engine) StartSubagent(ctx context.Context, providerName string, request SubagentStartRequest) (*SubagentRun, error) {
	binding, err := e.snapshotSubagentProvider(providerName)
	if err != nil {
		return nil, err
	}
	return e.startSubagentWithBinding(ctx, binding, request)
}

func (e *Engine) startSubagentOnProvider(ctx context.Context, providerName string, provider SubagentProvider, request SubagentStartRequest) (*SubagentRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateSubagentCapabilities(provider, request); err != nil {
		return nil, err
	}
	if err := validateSubagentMaxDepth(request.MaxDepth); err != nil {
		return nil, err
	}
	if request.OutputSchema != nil {
		if err := validateObjectJSONSchema(request.OutputSchema); err != nil {
			return nil, err
		}
	}
	descriptor := SubagentDescriptorData{Mode: "one-shot", Provider: strings.TrimSpace(providerName)}
	if request.Label != "" {
		descriptor.Label = descriptorString(request.Label)
	}
	descriptor, err := SnapshotSubagentDescriptor(descriptor)
	if err != nil {
		return nil, err
	}
	request.Descriptor = descriptor
	if request.CWD == "" && request.ParentSessionID != "" {
		parent, err := e.getSession(request.ParentSessionID)
		if err != nil {
			return nil, err
		}
		parent.mu.Lock()
		request.CWD = parent.Header.CWD
		parent.mu.Unlock()
	}
	request.Prompt = append([]ContentBlock(nil), request.Prompt...)
	run, err := provider.Start(ctx, request)
	if err != nil {
		return nil, err
	}
	if run == nil || strings.TrimSpace(run.ID) == "" || run.done == nil {
		return nil, errors.New("subagent provider returned an invalid run")
	}
	identity := map[string]any{
		"runId": newRunID(), "provider": strings.TrimSpace(provider.Name()), "id": run.ID, "local": run.local,
	}
	e.emitDynamicCordisScopedContained(request.ParentSessionID, "subagent/start", identity)
	go func() {
		<-run.Done()
		result, _ := run.Wait(context.Background())
		terminal := map[string]any{
			"runId": identity["runId"], "provider": identity["provider"], "id": run.ID,
			"local": run.local, "stopReason": result.StopReason,
		}
		if len(result.Output) > 0 {
			terminal["lastAssistantMessage"] = result.Output
		}
		e.notifySDKSubagentEnd(request.ParentSessionID, terminal)
		e.emitDynamicCordisScopedContained(request.ParentSessionID, "subagent/end", terminal)
	}()
	return run, nil
}

func validateSubagentCapabilities(provider SubagentProvider, request SubagentStartRequest) error {
	capabilities := provider.Capabilities()
	if request.OutputSchema != nil && !capabilities.OutputSchema {
		return subagentServiceError("UNSUPPORTED_CAPABILITY", fmt.Sprintf("subagent provider %q does not support the %q capability", provider.Name(), "outputSchema"), nil)
	}
	if request.MaxDepth != nil && !capabilities.DepthLimit {
		return subagentServiceError("UNSUPPORTED_CAPABILITY", fmt.Sprintf("subagent provider %q does not support the %q capability", provider.Name(), "depthLimit"), nil)
	}
	if request.ToolFilter != nil && !capabilities.ToolFilter {
		return subagentServiceError("UNSUPPORTED_CAPABILITY", fmt.Sprintf("subagent provider %q does not support the %q capability", provider.Name(), "toolFilter"), nil)
	}
	if request.Persona != "" && !capabilities.Persona {
		return subagentServiceError("UNSUPPORTED_CAPABILITY", fmt.Sprintf("subagent provider %q does not support the %q capability", provider.Name(), "persona"), nil)
	}
	if request.AgentOptions != nil && !capabilities.AgentOptions {
		return subagentServiceError("UNSUPPORTED_CAPABILITY", fmt.Sprintf("subagent provider %q does not support the %q capability", provider.Name(), "agentOptions"), nil)
	}
	return nil
}

func validateSubagentMaxDepth(maxDepth *int) error {
	if maxDepth != nil && (*maxDepth < 0 || int64(*maxDepth) > maxJSONSafeInteger) {
		return errors.New("subagent maxDepth must be a non-negative safe integer")
	}
	return nil
}

var subagentSchemaKeywords = map[string]bool{
	"type": true, "oneOf": true, "properties": true, "required": true,
	"additionalProperties": true, "items": true, "enum": true, "const": true,
	"description": true, "title": true, "default": true, "examples": true,
}

func validateObjectJSONSchema(schema map[string]any) error {
	violations := make([]string, 0)
	validateSubagentSchemaNode(schema, "schema", map[uintptr]bool{}, &violations)
	if schema["type"] != "object" {
		violations = append(violations, `schema.type must be "object" (structured output is object-rooted)`)
	}
	if len(violations) > 0 {
		return subagentServiceError("UNSUPPORTED_SCHEMA", "unsupported JSON schema: "+strings.Join(violations, "; "), nil)
	}
	return nil
}

func validateSubagentSchemaNode(node any, path string, seen map[uintptr]bool, violations *[]string) {
	record, ok := node.(map[string]any)
	if !ok || record == nil {
		*violations = append(*violations, path+" must be a schema object")
		return
	}
	pointer := reflect.ValueOf(record).Pointer()
	if seen[pointer] {
		*violations = append(*violations, path+" is circular")
		return
	}
	seen[pointer] = true
	defer delete(seen, pointer)

	for key, value := range record {
		if !subagentSchemaKeywords[key] {
			*violations = append(*violations, fmt.Sprintf("%s.%s is not a supported keyword (subset: type/oneOf/properties/required/additionalProperties/items/enum/const + annotations)", path, key))
			continue
		}
		if (key == "default" || key == "examples") && !isLosslessSubagentJSON(value, map[uintptr]bool{}) {
			*violations = append(*violations, fmt.Sprintf("%s.%s annotation must be lossless JSON data", path, key))
		}
	}
	if value, exists := record["description"]; exists {
		if _, ok := value.(string); !ok {
			*violations = append(*violations, path+".description must be a string")
		}
	}
	if value, exists := record["title"]; exists {
		if _, ok := value.(string); !ok {
			*violations = append(*violations, path+".title must be a string")
		}
	}

	typeValue, hasType := record["type"]
	oneOf, hasOneOf := record["oneOf"]
	if hasType && hasOneOf {
		*violations = append(*violations, path+" cannot declare both type and oneOf")
		return
	}
	constraintKeys := []string{"properties", "required", "additionalProperties", "items", "enum", "const"}
	if !hasType && !hasOneOf {
		for _, key := range constraintKeys {
			if _, exists := record[key]; exists {
				*violations = append(*violations, fmt.Sprintf("%s.%s requires type or oneOf", path, key))
			}
		}
		return
	}
	if hasOneOf {
		branches, ok := subagentAnySlice(oneOf)
		if !ok || len(branches) < 2 {
			*violations = append(*violations, path+".oneOf must be an array of at least two schemas")
		} else {
			for index, branch := range branches {
				validateSubagentSchemaNode(branch, fmt.Sprintf("%s.oneOf[%d]", path, index), seen, violations)
			}
		}
		for _, key := range constraintKeys {
			if _, exists := record[key]; exists {
				*violations = append(*violations, fmt.Sprintf("%s.%s is not supported beside oneOf", path, key))
			}
		}
		return
	}

	typeName, ok := typeValue.(string)
	if !ok || !map[string]bool{"object": true, "array": true, "string": true, "number": true, "integer": true, "boolean": true, "null": true}[typeName] {
		*violations = append(*violations, path+".type must be one of object/array/string/number/integer/boolean/null")
		return
	}
	allowedFor := map[string]map[string]bool{
		"properties": {"object": true}, "required": {"object": true}, "additionalProperties": {"object": true},
		"items": {"array": true}, "enum": {"string": true, "number": true, "integer": true, "boolean": true, "null": true},
		"const": {"string": true, "number": true, "integer": true, "boolean": true, "null": true},
	}
	for key, types := range allowedFor {
		if _, exists := record[key]; exists && !types[typeName] {
			*violations = append(*violations, fmt.Sprintf("%s.%s is not supported on type %q", path, key, typeName))
		}
	}
	switch typeName {
	case "object":
		properties, hasProperties := record["properties"]
		propertyMap, propertiesOK := properties.(map[string]any)
		if hasProperties {
			if !propertiesOK || propertyMap == nil {
				*violations = append(*violations, path+".properties must be an object of schemas")
			} else {
				for name, child := range propertyMap {
					validateSubagentSchemaNode(child, path+".properties."+name, seen, violations)
				}
			}
		}
		if required, exists := record["required"]; exists {
			items, ok := subagentStringSlice(required)
			if !ok {
				*violations = append(*violations, path+".required must be an array of strings")
			} else {
				for _, name := range items {
					if !propertiesOK {
						*violations = append(*violations, fmt.Sprintf("%s.required names %q which is not in properties", path, name))
					} else if _, exists := propertyMap[name]; !exists {
						*violations = append(*violations, fmt.Sprintf("%s.required names %q which is not in properties", path, name))
					}
				}
			}
		}
		if additional, exists := record["additionalProperties"]; exists {
			if _, ok := additional.(bool); !ok {
				*violations = append(*violations, path+".additionalProperties must be a boolean")
			}
		}
	case "array":
		if items, exists := record["items"]; exists {
			validateSubagentSchemaNode(items, path+".items", seen, violations)
		}
	default:
		validateSubagentScalarConstraints(record, path, typeName, violations)
	}
}

func validateSubagentScalarConstraints(record map[string]any, path, typeName string, violations *[]string) {
	enum, hasEnum := record["enum"]
	values, enumOK := subagentAnySlice(enum)
	if hasEnum {
		enumOK = enumOK && len(values) > 0
		if enumOK {
			for _, value := range values {
				if !subagentScalarMatches(typeName, value) {
					enumOK = false
					break
				}
			}
		}
		if !enumOK {
			*violations = append(*violations, fmt.Sprintf("%s.enum must be a non-empty array of %s values", path, typeName))
		}
	}
	if value, hasConst := record["const"]; hasConst {
		if !subagentScalarMatches(typeName, value) {
			*violations = append(*violations, fmt.Sprintf("%s.const must be a %s value", path, typeName))
		} else if enumOK && !subagentScalarContains(values, value) {
			*violations = append(*violations, fmt.Sprintf("%s.const must be one of %s.enum when both are declared", path, path))
		}
	}
}

func subagentAnySlice(value any) ([]any, bool) {
	if values, ok := value.([]any); ok {
		return values, true
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Kind() != reflect.Slice {
		return nil, false
	}
	values := make([]any, rv.Len())
	for index := range values {
		values[index] = rv.Index(index).Interface()
	}
	return values, true
}

func subagentStringSlice(value any) ([]string, bool) {
	values, ok := subagentAnySlice(value)
	if !ok {
		return nil, false
	}
	result := make([]string, len(values))
	for index, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil, false
		}
		result[index] = text
	}
	return result, true
}

func subagentScalarMatches(typeName string, value any) bool {
	switch typeName {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	case "number", "integer":
		number, ok := subagentJSONNumber(value)
		return ok && (typeName != "integer" || math.Trunc(number) == number)
	default:
		return false
	}
}

func subagentJSONNumber(value any) (float64, bool) {
	var number float64
	switch value := value.(type) {
	case int:
		number = float64(value)
	case int8:
		number = float64(value)
	case int16:
		number = float64(value)
	case int32:
		number = float64(value)
	case int64:
		number = float64(value)
	case uint:
		number = float64(value)
	case uint8:
		number = float64(value)
	case uint16:
		number = float64(value)
	case uint32:
		number = float64(value)
	case uint64:
		number = float64(value)
	case float32:
		number = float64(value)
	case float64:
		number = value
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			return 0, false
		}
		number = parsed
	default:
		return 0, false
	}
	return number, !math.IsNaN(number) && !math.IsInf(number, 0) && !(number == 0 && math.Signbit(number))
}

func subagentScalarContains(values []any, target any) bool {
	for _, value := range values {
		if reflect.DeepEqual(value, target) {
			return true
		}
		left, leftOK := subagentJSONNumber(value)
		right, rightOK := subagentJSONNumber(target)
		if leftOK && rightOK && left == right {
			return true
		}
	}
	return false
}

func isLosslessSubagentJSON(value any, seen map[uintptr]bool) bool {
	if value == nil {
		return true
	}
	switch value := value.(type) {
	case string, bool:
		return true
	case json.Number, float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		_, ok := subagentJSONNumber(value)
		return ok
	case map[string]any:
		pointer := reflect.ValueOf(value).Pointer()
		if seen[pointer] {
			return false
		}
		seen[pointer] = true
		defer delete(seen, pointer)
		for _, child := range value {
			if !isLosslessSubagentJSON(child, seen) {
				return false
			}
		}
		return true
	default:
		items, ok := subagentAnySlice(value)
		if !ok {
			return false
		}
		rv := reflect.ValueOf(value)
		pointer := rv.Pointer()
		if seen[pointer] {
			return false
		}
		seen[pointer] = true
		defer delete(seen, pointer)
		for _, child := range items {
			if !isLosslessSubagentJSON(child, seen) {
				return false
			}
		}
		return true
	}
}

func validateSubagentCWD(prefix, cwd string) (string, error) {
	if cwd == "" {
		return "", fmt.Errorf("%s: no working directory for the child", prefix)
	}
	if !filepath.IsAbs(cwd) {
		return "", fmt.Errorf("%s: working directory must be an absolute path: %s", prefix, cwd)
	}
	info, err := os.Stat(cwd)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("%s: working directory is not an accessible directory: %s", prefix, cwd)
	}
	if err := validateSubagentCWDSearch(cwd); err != nil {
		return "", fmt.Errorf("%s: working directory is not an accessible directory: %s", prefix, cwd)
	}
	return cwd, nil
}

func validateConfiguredSubagentCWD(prefix, cwd string) (string, error) {
	if cwd == "" {
		return "", nil
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", fmt.Errorf("%s: config cwd is invalid: %w", prefix, err)
	}
	return validateSubagentCWD(prefix, abs)
}

func positiveSubagentDuration(prefix, name string, value, fallback time.Duration) (time.Duration, error) {
	if value == 0 {
		value = fallback
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s: %s must be positive", prefix, name)
	}
	return value, nil
}

type subagentProcessResult struct {
	exitCode int
	signal   string
	err      error
}

type subagentProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser

	done       chan struct{}
	mu         sync.Mutex
	res        subagentProcessResult
	stderrMu   sync.Mutex
	stderrTail []byte
	stderrDone chan struct{}
}

const maxSubagentStderrBytes = 16 << 10

func startSubagentProcess(command string, args []string, cwd string, env map[string]string) (*subagentProcess, error) {
	return startSubagentProcessWithStderr(command, args, cwd, env, false)
}

func startSubagentProcessWithStderr(command string, args []string, cwd string, env map[string]string, captureStderr bool) (*subagentProcess, error) {
	cmd := exec.Command(command, args...)
	cmd.Dir = cwd
	cmd.Env = scrubbedChildEnv(env)
	configureChildProcess(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	var stderr io.ReadCloser
	var stderrWriter *os.File
	if captureStderr {
		stderr, stderrWriter, err = os.Pipe()
		if err != nil {
			_ = stdin.Close()
			_ = stdout.Close()
			_ = stdoutWriter.Close()
			return nil, err
		}
		cmd.Stderr = stderrWriter
	} else {
		cmd.Stderr = os.Stderr
	}
	cmd.Stdout = stdoutWriter
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		if stderr != nil {
			_ = stderr.Close()
		}
		if stderrWriter != nil {
			_ = stderrWriter.Close()
		}
		return nil, err
	}
	process := &subagentProcess{cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		exitCode := -1
		if cmd.ProcessState != nil {
			exitCode = cmd.ProcessState.ExitCode()
		}
		// Close only the explicit pipe write end. The consumer can then drain
		// buffered stream-json bytes before observing EOF on the read end.
		_ = stdoutWriter.Close()
		if stderrWriter != nil {
			_ = stderrWriter.Close()
		}
		process.mu.Lock()
		process.res = subagentProcessResult{exitCode: exitCode, signal: childProcessExitSignal(cmd.ProcessState), err: err}
		close(process.done)
		process.mu.Unlock()
	}()
	return process, nil
}

func (p *subagentProcess) startStderrCapture() {
	if p == nil || p.stderr == nil {
		return
	}
	p.stderrDone = make(chan struct{})
	go p.captureStderr()
}

func (p *subagentProcess) captureStderr() {
	defer close(p.stderrDone)
	buffer := make([]byte, 4096)
	for {
		n, err := p.stderr.Read(buffer)
		if n > 0 {
			p.stderrMu.Lock()
			p.stderrTail = append(p.stderrTail, buffer[:n]...)
			if len(p.stderrTail) > maxSubagentStderrBytes {
				p.stderrTail = append([]byte(nil), p.stderrTail[len(p.stderrTail)-maxSubagentStderrBytes:]...)
			}
			p.stderrMu.Unlock()
		}
		if err != nil {
			break
		}
	}
}

func (p *subagentProcess) stderrDiagnostic(settleGrace time.Duration) string {
	if p == nil {
		return ""
	}
	if p.stderrDone != nil {
		timer := time.NewTimer(settleGrace)
		select {
		case <-p.stderrDone:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
	p.stderrMu.Lock()
	data := append([]byte(nil), p.stderrTail...)
	p.stderrMu.Unlock()
	return strings.TrimSpace(string(data))
}

func (p *subagentProcess) result() subagentProcessResult {
	<-p.done
	p.mu.Lock()
	result := p.res
	p.mu.Unlock()
	return result
}

func (p *subagentProcess) waitWithin(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return true
	case <-timer.C:
		return false
	}
}

func (p *subagentProcess) closeInput() {
	if p != nil && p.stdin != nil {
		_ = p.stdin.Close()
	}
}

func (p *subagentProcess) dispose(eofGrace, terminateGrace time.Duration) error {
	if p == nil {
		return nil
	}
	p.closeInput()
	if eofGrace > 0 && p.waitWithin(eofGrace) {
		return nil
	}
	select {
	case <-p.done:
		return nil
	default:
	}
	_ = terminateChildProcess(p.cmd)
	if p.waitWithin(terminateGrace) {
		return nil
	}
	if err := killChildProcess(p.cmd); err != nil {
		return err
	}
	if !p.waitWithin(terminateGrace) {
		return fmt.Errorf("subagent process did not exit within %s after forced termination", terminateGrace)
	}
	return nil
}

func newRunID() string { return newID("subagent-run") }
