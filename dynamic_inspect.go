package harness

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

//go:embed dynamic_inspect_catalog.json
var dynamicInspectCatalogJSON []byte

type cordisInspectCatalog struct {
	Services []cordisInspectService `json:"services"`
	Events   []cordisInspectEvent   `json:"events"`
	Types    []cordisInspectType    `json:"types"`
}

type cordisInspectParameter struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type cordisInspectMethod struct {
	Signature   string                   `json:"signature"`
	Description string                   `json:"description"`
	Parameters  []cordisInspectParameter `json:"parameters"`
	Returns     string                   `json:"returns,omitempty"`
	Throws      []string                 `json:"throws,omitempty"`
}

type cordisInspectService struct {
	Key         string                `json:"key"`
	Summary     string                `json:"summary"`
	Description string                `json:"description"`
	Methods     []cordisInspectMethod `json:"methods"`
}

type cordisInspectEvent struct {
	Name        string                   `json:"name"`
	Mode        string                   `json:"mode"`
	Signature   string                   `json:"signature"`
	Summary     string                   `json:"summary"`
	Description string                   `json:"description"`
	Parameters  []cordisInspectParameter `json:"parameters"`
}

type cordisInspectType struct {
	Name        string `json:"name"`
	Declaration string `json:"declaration"`
}

var dynamicInspectCatalog = func() cordisInspectCatalog {
	var catalog cordisInspectCatalog
	if err := json.Unmarshal(dynamicInspectCatalogJSON, &catalog); err != nil {
		panic(fmt.Sprintf("decode embedded Cordis inspect catalog: %v", err))
	}
	return catalog
}()

var hostInspectProviderManifests = []CordisInspectProviderManifest{
	hostInspectProviderManifest(
		"Service",
		"Progressive Host Service discovery: compact capability/signature directory, then one exact coding contract.",
		"listService",
		exactInspectInput("service", "Exact Service key. Omit it for the compact Service and method-signature directory."),
		map[string]any{"description": "Compact Service directory, or one exact Service contract with only its referenced type declarations."},
	),
	hostInspectProviderManifest(
		"Event",
		"Progressive Host Event discovery: compact listener directory, then one exact event contract.",
		"listEvents",
		exactInspectInput("event", "Exact Event name. Omit it for the compact Event and listener-signature directory."),
		map[string]any{"description": "Compact Event directory, or one exact Event contract with only its referenced type declarations."},
	),
	hostInspectProviderManifest(
		"Builtin",
		"Plain-JavaScript symbols available to a dynamic Host half.",
		"listBuiltins",
		emptyInspectInput(),
		map[string]any{"description": "JSON data owned by this inspect provider."},
	),
	hostInspectProviderManifest(
		"Tool",
		"Tools visible to the requesting Agent, including scoped and dynamic registrations.",
		"listTools",
		emptyInspectInput(),
		map[string]any{"description": "JSON data owned by this inspect provider."},
	),
}

func hostInspectProviderManifest(id, description, method string, inputSchema, outputSchema any) CordisInspectProviderManifest {
	methodDescription := description
	if id == "Tool" {
		methodDescription = "Return every Tool schema currently callable by this Agent."
	}
	return CordisInspectProviderManifest{ID: id, Description: description, Methods: []CordisInspectMethodManifest{{
		Name: method, Description: methodDescription, InputSchema: inputSchema, OutputSchema: outputSchema,
	}}}
}

func emptyInspectInput() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}
}

func exactInspectInput(field, description string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{field: map[string]any{"type": "string", "description": description}},
		"additionalProperties": false,
	}
}

// QueryHostInspectProvider executes one first-party Host provider locally.
func (e *Engine) QueryHostInspectProvider(ctx context.Context, agentID, providerID, methodName string, input any) (any, error) {
	session, err := e.getSession(agentID)
	if err != nil {
		return nil, err
	}
	provider, method := findHostInspectMethod(providerID, methodName)
	if provider == nil {
		return nil, fmt.Errorf("Host Cordis inspect provider %q is not registered", providerID)
	}
	if method == nil {
		return nil, fmt.Errorf("Cordis inspect provider %q has no method %q", providerID, methodName)
	}
	validatedInput := input
	if validatedInput == nil {
		validatedInput = map[string]any{}
	}
	if !validInspectValue(validatedInput, method.InputSchema) {
		return nil, fmt.Errorf("Host Cordis inspect %s.%s rejected input", providerID, methodName)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var data any
	switch providerID {
	case "Service":
		data, err = queryHostServiceInspect(exactInspectField(validatedInput, "service"))
	case "Event":
		data, err = queryHostEventInspect(exactInspectField(validatedInput, "event"))
	case "Builtin":
		data = map[string]any{"builtins": hostCordisBuiltinInspection, "referencedTypes": []any{}}
	case "Tool":
		var tools []ToolSchema
		tools, err = e.toolsForSession(session)
		data = map[string]any{"tools": tools}
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !jsonValue(data) {
		return nil, fmt.Errorf("Host Cordis inspect %s.%s returned a non-JSON value", providerID, methodName)
	}
	snapshot := cloneJSON(data)
	if !validInspectValue(snapshot, method.OutputSchema) {
		return nil, fmt.Errorf("Host Cordis inspect %s.%s returned invalid output", providerID, methodName)
	}
	return snapshot, nil
}

func findHostInspectMethod(providerID, methodName string) (*CordisInspectProviderManifest, *CordisInspectMethodManifest) {
	for index := range hostInspectProviderManifests {
		provider := &hostInspectProviderManifests[index]
		if provider.ID != providerID {
			continue
		}
		for methodIndex := range provider.Methods {
			if provider.Methods[methodIndex].Name == methodName {
				return provider, &provider.Methods[methodIndex]
			}
		}
		return provider, nil
	}
	return nil, nil
}

func exactInspectField(input any, field string) *string {
	record, _ := input.(map[string]any)
	value, exists := record[field].(string)
	if !exists {
		return nil
	}
	return &value
}

func queryHostServiceInspect(key *string) (any, error) {
	if key == nil {
		services := make([]any, 0, len(dynamicInspectCatalog.Services))
		for _, service := range dynamicInspectCatalog.Services {
			methods := make([]any, 0, len(service.Methods))
			for _, method := range service.Methods {
				methods = append(methods, map[string]any{"signature": method.Signature})
			}
			services = append(services, map[string]any{"key": service.Key, "description": service.Summary, "methods": methods})
		}
		return map[string]any{"mode": "catalog", "services": services}, nil
	}
	for _, service := range dynamicInspectCatalog.Services {
		if service.Key != *key {
			continue
		}
		return map[string]any{
			"mode": "service",
			"service": map[string]any{
				"key": service.Key, "description": service.Description,
				"access": map[string]any{
					"optional":       map[string]any{"expression": fmt.Sprintf("ctx.get(%q)", service.Key), "requiresUndefinedCheck": true},
					"hardDependency": map[string]any{"inject": []string{service.Key}, "expression": cordisContextProperty(service.Key)},
				},
				"methods": service.Methods,
			},
			"referencedTypes": referencedCordisTypes(serviceMethodSignatures(service.Methods)),
		}, nil
	}
	return nil, fmt.Errorf("no catalogued Service named %q", *key)
}

func queryHostEventInspect(name *string) (any, error) {
	if name == nil {
		events := make([]any, 0, len(dynamicInspectCatalog.Events))
		for _, event := range dynamicInspectCatalog.Events {
			if isCordisInternalEvent(event.Name) {
				continue
			}
			events = append(events, map[string]any{
				"name": event.Name, "description": event.Summary, "mode": event.Mode, "signature": event.Signature,
			})
		}
		return map[string]any{"mode": "catalog", "events": events}, nil
	}
	for _, event := range dynamicInspectCatalog.Events {
		if event.Name != *name || isCordisInternalEvent(event.Name) {
			continue
		}
		return map[string]any{
			"mode": "event",
			"event": map[string]any{
				"name": event.Name, "description": event.Description, "mode": event.Mode,
				"signature": event.Signature, "parameters": event.Parameters,
			},
			"referencedTypes": referencedCordisTypes([]string{event.Signature}),
		}, nil
	}
	return nil, fmt.Errorf("no catalogued Event named %q", *name)
}

func serviceMethodSignatures(methods []cordisInspectMethod) []string {
	signatures := make([]string, len(methods))
	for index, method := range methods {
		signatures[index] = method.Signature
	}
	return signatures
}

func referencedCordisTypes(seeds []string) []cordisInspectType {
	included := make(map[string]bool)
	frontier := append([]string(nil), seeds...)
	for len(frontier) > 0 {
		next := []string{}
		for _, entry := range dynamicInspectCatalog.Types {
			if included[entry.Name] {
				continue
			}
			pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(entry.Name) + `\b`)
			matched := false
			for _, text := range frontier {
				if pattern.MatchString(text) {
					matched = true
					break
				}
			}
			if matched {
				included[entry.Name] = true
				next = append(next, entry.Declaration)
			}
		}
		frontier = next
	}
	result := make([]cordisInspectType, 0, len(included))
	for _, entry := range dynamicInspectCatalog.Types {
		if included[entry.Name] {
			result = append(result, entry)
		}
	}
	return result
}

func cordisContextProperty(key string) string {
	if regexp.MustCompile(`^[A-Za-z_$][\w$]*$`).MatchString(key) {
		return "ctx." + key
	}
	encoded, _ := json.Marshal(key)
	return "ctx[" + string(encoded) + "]"
}

func isCordisInternalEvent(name string) bool {
	return strings.HasPrefix(name, "cordis/")
}

var hostCordisBuiltinInspection = []map[string]any{
	{
		"name":        "ctx",
		"description": "Restricted Cordis Context. Prefer ctx.get(name) with an undefined check; use inject for hard dependencies.",
		"signatures": []string{
			"ctx.get(name: string): unknown | undefined",
			"ctx.on(name: string, listener: Function): () => void",
			"ctx.provide(name: string, value: unknown): () => void",
			"ctx.effect(callback: Function, label?: string): () => void",
		},
	},
	{
		"name":        "harness",
		"description": "Host helpers for Package-private Client RPC and model-visible dynamic Tools.",
		"signatures": []string{
			"harness.handle(method: string, handler: (args: JsonValue) => JsonValue | Promise<JsonValue>): () => void",
			"harness.defineTool(definition: ToolDefinition): ToolDefinition",
			"harness.registerTool(ctx: Context, tool: ToolDefinition): () => void",
		},
	},
	{"name": "console", "description": "Package-tagged Host logging.", "signatures": []string{"console.log(...values): void", "console.error(...values): void"}},
	{"name": "btoa", "description": "Encode UTF-8 text as base64.", "signatures": []string{"btoa(value: string): string"}},
	{"name": "atob", "description": "Decode base64 as UTF-8 text.", "signatures": []string{"atob(value: string): string"}},
	{"name": "TextEncoder", "description": "Standard UTF-8 encoder constructor.", "signatures": []string{"new TextEncoder()"}},
	{"name": "TextDecoder", "description": "Standard text decoder constructor.", "signatures": []string{"new TextDecoder(label?: string)"}},
}
