package harness

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

const piAICatalogPackageVersion = "0.82.1"
const piAICatalogManifestHash = "1a3c7cf59ada71c94abe4540976960524ee933034491c75d6418e2abc1b42535"

//go:embed pi_ai_catalog.json
var piAICatalogJSON []byte

type piAIModelCompat struct {
	ThinkingFormat                   string         `json:"thinkingFormat,omitempty"`
	CacheControlFormat               string         `json:"cacheControlFormat,omitempty"`
	MaxTokensField                   string         `json:"maxTokensField,omitempty"`
	SessionAffinityFormat            string         `json:"sessionAffinityFormat,omitempty"`
	DeferredToolsMode                string         `json:"deferredToolsMode,omitempty"`
	ChatTemplateKwargs               map[string]any `json:"chatTemplateKwargs,omitempty"`
	SupportsReasoningEffort          *bool          `json:"supportsReasoningEffort,omitempty"`
	SupportsLongCacheRetention       *bool          `json:"supportsLongCacheRetention,omitempty"`
	SupportsExplicitPromptCacheMode  *bool          `json:"supportsExplicitPromptCacheMode,omitempty"`
	SupportsStore                    *bool          `json:"supportsStore,omitempty"`
	SupportsUsageInStreaming         *bool          `json:"supportsUsageInStreaming,omitempty"`
	RequiresToolResultName           *bool          `json:"requiresToolResultName,omitempty"`
	RequiresAssistantAfterToolResult *bool          `json:"requiresAssistantAfterToolResult,omitempty"`
	RequiresThinkingAsText           *bool          `json:"requiresThinkingAsText,omitempty"`
	ForceAdaptiveThinking            *bool          `json:"forceAdaptiveThinking,omitempty"`
	SendSessionAffinityHeaders       *bool          `json:"sendSessionAffinityHeaders,omitempty"`
	SupportsCacheControlOnTools      *bool          `json:"supportsCacheControlOnTools,omitempty"`
	SupportsDeveloperRole            *bool          `json:"supportsDeveloperRole,omitempty"`
	RequiresReasoningContent         *bool          `json:"requiresReasoningContentOnAssistantMessages,omitempty"`
	SupportsEagerToolInputStreaming  *bool          `json:"supportsEagerToolInputStreaming,omitempty"`
	SupportsStrictMode               *bool          `json:"supportsStrictMode,omitempty"`
	SupportsOpenAIGrammarTools       *bool          `json:"supportsOpenAIGrammarTools,omitempty"`
	SupportsToolSearch               *bool          `json:"supportsToolSearch,omitempty"`
	SupportsTemperature              *bool          `json:"supportsTemperature,omitempty"`
	SupportsStrictTools              *bool          `json:"supportsStrictTools,omitempty"`
	AllowEmptySignature              *bool          `json:"allowEmptySignature,omitempty"`
	ZaiToolStream                    *bool          `json:"zaiToolStream,omitempty"`
}

type piAIModel struct {
	ID               string             `json:"id"`
	Name             string             `json:"name"`
	API              string             `json:"api"`
	BaseURL          string             `json:"baseUrl"`
	Headers          map[string]string  `json:"headers,omitempty"`
	Reasoning        bool               `json:"reasoning"`
	Input            []string           `json:"input"`
	ContextWindow    int                `json:"contextWindow"`
	MaxTokens        int                `json:"maxTokens"`
	ThinkingLevelMap map[string]*string `json:"thinkingLevelMap,omitempty"`
	Compat           piAIModelCompat    `json:"compat,omitempty"`
}

func (m piAIModel) info() ModelInfo {
	return ModelInfo{ID: m.ID, Name: m.Name, InputModalities: append([]string(nil), m.Input...), ContextWindow: m.ContextWindow, MaxTokens: m.MaxTokens}
}

type piAICatalogRoute struct {
	ID          string      `json:"id"`
	DisplayName string      `json:"name"`
	BaseURL     string      `json:"baseUrl"`
	Models      []piAIModel `json:"models"`
}

type piAICatalogDocument struct {
	PackageVersion        string             `json:"packageVersion"`
	ManifestStructureHash string             `json:"manifestStructureHash"`
	SupportedProtocols    []string           `json:"supportedProtocols"`
	UnsupportedProtocols  []string           `json:"unsupportedProtocols"`
	Providers             []piAICatalogRoute `json:"providers"`
}

var piAICatalog, piAIUnsupportedCatalogProtocols = loadPiAICatalog()

func loadPiAICatalog() (map[string]piAICatalogRoute, []string) {
	var document piAICatalogDocument
	if err := json.Unmarshal(piAICatalogJSON, &document); err != nil {
		panic(fmt.Sprintf("decode embedded pi-ai catalog: %v", err))
	}
	if document.PackageVersion != piAICatalogPackageVersion || document.ManifestStructureHash != piAICatalogManifestHash {
		panic("embedded pi-ai catalog version/hash does not match the Go drift gate")
	}
	protocols := make(map[string]bool, len(document.SupportedProtocols))
	for _, protocol := range document.SupportedProtocols {
		protocols[protocol] = true
	}
	if len(protocols) != len(piAICatalogProtocols) {
		panic("embedded pi-ai catalog protocol set does not match the Go implementation")
	}
	for protocol := range piAICatalogProtocols {
		if !protocols[protocol] {
			panic("embedded pi-ai catalog is missing supported protocol " + protocol)
		}
	}
	routes := make(map[string]piAICatalogRoute, len(document.Providers))
	for _, route := range document.Providers {
		if route.ID == "" || routes[route.ID].ID != "" {
			panic("embedded pi-ai catalog contains an invalid provider route")
		}
		for _, model := range route.Models {
			if model.ID == "" || model.Name == "" || model.BaseURL == "" && model.API != "azure-openai-responses" || !piAICatalogProtocols[model.API] {
				panic("embedded pi-ai catalog contains an invalid model")
			}
		}
		routes[route.ID] = route
	}
	return routes, append([]string(nil), document.UnsupportedProtocols...)
}

func clonePiAIModel(model piAIModel) piAIModel {
	model.Input = append([]string(nil), model.Input...)
	model.Headers = clonePIAIHeaders(model.Headers)
	if model.ThinkingLevelMap != nil {
		levels := make(map[string]*string, len(model.ThinkingLevelMap))
		for level, value := range model.ThinkingLevelMap {
			if value == nil {
				levels[level] = nil
				continue
			}
			copy := *value
			levels[level] = &copy
		}
		model.ThinkingLevelMap = levels
	}
	return model
}

func clonePiAIModels(models []piAIModel) []piAIModel {
	out := make([]piAIModel, len(models))
	for index, model := range models {
		out[index] = clonePiAIModel(model)
	}
	return out
}

func piAIModelInfos(models []piAIModel) []ModelInfo {
	out := make([]ModelInfo, len(models))
	for index, model := range models {
		out[index] = model.info()
	}
	return out
}
