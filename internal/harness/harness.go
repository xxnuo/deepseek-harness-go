package harness

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const SessionFormatVersion = 0

// LaunchEnvironmentSnapshot preserves the source of launch-time values even
// when a CLI temporarily materializes dotenv entries in os.Environ.
type LaunchEnvironmentSnapshot struct {
	Process map[string]string
	Project map[string]string
	User    map[string]string
}

type Config struct {
	DataDir                string
	Workspace              string
	FrontendDir            string
	PluginDir              string
	PluginDirs             []string
	ClientPlugins          []string
	ClientHMRPollInterval  time.Duration
	PresetDir              string
	SkillDir               string
	BundledBadgeSkill      bool
	AgentsHome             string
	Host                   string
	Port                   int
	TrustedHosts           []string
	Version                string
	Provider               string
	Model                  string
	BaseURL                string
	APIKey                 string
	LaunchEnvironment      *LaunchEnvironmentSnapshot
	WebSearchProvider      string
	WebFetchProvider       string
	WebTools               *WebToolConfig
	HTTPWebFetch           HTTPWebFetchConfig
	ToolPresentation       string
	Persona                string
	InstructionMaxBytes    int
	ExaSearch              ExaSearchProviderOptions
	PerplexitySearch       PerplexitySearchProviderOptions
	SessionTitle           SessionTitleConfig
	SessionTitleLLM        SessionTitleLLMConfig
	SessionTelemetry       *SessionTelemetryConfig
	Compaction             CompactionConfig
	ToolResultPruner       ToolResultPruneConfig
	Spill                  SpillConfig
	RepeatToolReminder     RepeatToolReminderConfig
	FileReference          FileReferenceConfig
	SessionReference       SessionReferenceConfig
	SessionProjectionCache *SessionProjectionCacheConfig
	Storage                *StorageRuntimeConfig
	DynamicCordisVMTimeout time.Duration
	RuntimeInvariants      *RuntimeInvariantConfig
	TimeContext            *TimeContextConfig
	TmuxContext            *TmuxContextConfig
	ScheduleEnabled        bool
	LSPServers             map[string]LSPStdioConfig
	LSPTool                LSPToolConfig
	MCPServers             []MCPConfig
	Hooks                  []HookBridgeConfig
	SubagentProviders      []SubagentProvider
	SubagentTools          []SubagentToolConfig
	SubagentReportDelivery string
	AgentTeams             *AgentTeamConfig
	Terminal               TerminalConfig
	TerminalTool           TerminalToolConfig
	E2B                    *E2BConfig
	// RetryPolicy is the default for providers that do not expose a route-
	// specific policy. A provider may override it by implementing
	// RetryPolicyProvider.
	RetryPolicy  RetryPolicy
	Persist      bool
	SessionStore SessionStore
	MaxBodyBytes int64
}

func DefaultConfig() Config {
	cwd, _ := os.Getwd()
	home, _ := os.UserHomeDir()
	data := strings.TrimSpace(os.Getenv("DSH_HOME"))
	if data == "" {
		data = filepath.Join(home, ".dsh")
	}
	agentsHome := strings.TrimSpace(os.Getenv("DSH_AGENTS_HOME"))
	if agentsHome == "" {
		agentsHome = filepath.Join(home, ".agents")
	}
	provider := os.Getenv("DSH_PROVIDER")
	if provider == "" {
		provider = "deepseek-official"
	}
	model := os.Getenv("DSH_MODEL")
	if model == "" {
		model = "deepseek-v4-flash"
	}
	baseURL := os.Getenv("DEEPSEEK_BASE_URL")
	if baseURL == "" {
		baseURL = "https://api.deepseek.com"
	}
	webSearchProvider := strings.TrimSpace(os.Getenv("DSH_WEB_SEARCH_PROVIDER"))
	if webSearchProvider == "" {
		webSearchProvider = "deepseek-official"
	}
	webFetchProvider := strings.TrimSpace(os.Getenv("DSH_WEB_FETCH_PROVIDER"))
	webTools := defaultWebToolConfig()
	webTools.FetchEnabled = webFetchProvider != ""
	toolPresentation := strings.TrimSpace(os.Getenv("DSH_TOOLS_MODE"))
	if toolPresentation == "" {
		toolPresentation = "native"
	}
	return Config{
		DataDir: data, Workspace: cwd, AgentsHome: agentsHome, Host: "127.0.0.1", Port: 3080,
		ClientHMRPollInterval: 500 * time.Millisecond,
		Version:               Version(), Provider: provider, Model: model,
		BaseURL: baseURL, APIKey: os.Getenv("DEEPSEEK_API_KEY"), Persist: true,
		WebSearchProvider: webSearchProvider, WebFetchProvider: webFetchProvider,
		WebTools: webTools, HTTPWebFetch: defaultHTTPWebFetchConfig(),
		ToolPresentation: toolPresentation, Persona: defaultCodingPersona,
		InstructionMaxBytes: defaultInstructionMaxBytes,
		SessionTitle:        defaultSessionTitleConfig(), SessionTitleLLM: defaultSessionTitleLLMConfig(),
		Compaction: defaultCompactionConfig(), ToolResultPruner: defaultToolResultPruneConfig(),
		Spill: defaultSpillConfig(), RepeatToolReminder: defaultRepeatToolReminderConfig(),
		FileReference:          defaultFileReferenceConfig(),
		SubagentReportDelivery: "next-step",
		DynamicCordisVMTimeout: 5 * time.Second,
		RetryPolicy:            defaultRetryPolicy(),
		MaxBodyBytes:           140 << 20,
	}
}

func normalizeConfig(c Config) Config {
	d := DefaultConfig()
	if c.DataDir == "" {
		c.DataDir = d.DataDir
	}
	if c.Workspace == "" {
		c.Workspace = d.Workspace
	}
	if c.AgentsHome == "" {
		c.AgentsHome = d.AgentsHome
	}
	if c.Host == "" {
		c.Host = d.Host
	}
	if c.Version == "" {
		c.Version = d.Version
	}
	if c.Provider == "" {
		c.Provider = d.Provider
	}
	if c.Model == "" {
		c.Model = d.Model
	}
	if c.BaseURL == "" {
		c.BaseURL = d.BaseURL
	}
	if c.APIKey == "" && c.LaunchEnvironment == nil {
		c.APIKey = d.APIKey
	}
	c.LaunchEnvironment = cloneLaunchEnvironment(c.LaunchEnvironment)
	if c.WebSearchProvider == "" {
		c.WebSearchProvider = d.WebSearchProvider
	}
	if c.WebFetchProvider == "" {
		c.WebFetchProvider = d.WebFetchProvider
	}
	c.WebTools = normalizeWebToolConfig(c.WebTools)
	c.HTTPWebFetch = normalizeHTTPWebFetchConfig(c.HTTPWebFetch)
	if c.ToolPresentation == "" {
		c.ToolPresentation = d.ToolPresentation
	}
	if c.Persona == "" {
		c.Persona = d.Persona
	}
	if c.InstructionMaxBytes == 0 {
		c.InstructionMaxBytes = d.InstructionMaxBytes
	}
	if c.ClientHMRPollInterval <= 0 {
		c.ClientHMRPollInterval = d.ClientHMRPollInterval
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = d.MaxBodyBytes
	}
	c.SessionTitle = normalizeSessionTitleConfig(c.SessionTitle)
	c.SessionTitleLLM = normalizeSessionTitleLLMConfig(c.SessionTitleLLM)
	c.SessionTelemetry = normalizeSessionTelemetryConfig(c.SessionTelemetry)
	c.Compaction = normalizeCompactionConfig(c.Compaction)
	c.ToolResultPruner = normalizeToolResultPruneConfig(c.ToolResultPruner)
	c.Spill = normalizeSpillConfig(c.Spill)
	c.RepeatToolReminder = normalizeRepeatToolReminderConfig(c.RepeatToolReminder)
	c.FileReference = normalizeFileReferenceConfig(c.FileReference)
	c.Storage = cloneStorageRuntimeConfig(c.Storage)
	if c.SubagentReportDelivery == "" {
		c.SubagentReportDelivery = d.SubagentReportDelivery
	}
	if c.AgentTeams != nil {
		clone := *c.AgentTeams
		c.AgentTeams = &clone
	}
	if c.DynamicCordisVMTimeout <= 0 {
		c.DynamicCordisVMTimeout = d.DynamicCordisVMTimeout
	}
	c.RuntimeInvariants = cloneRuntimeInvariantConfig(c.RuntimeInvariants)
	c.Terminal = normalizeTerminalConfig(c.Terminal)
	c.TerminalTool = normalizeTerminalToolConfig(c.TerminalTool)
	c.RetryPolicy = normalizeRetryPolicy(c.RetryPolicy)
	c.TrustedHosts = trustedHostsForBind(c.Host, c.TrustedHosts)
	return c
}

type Option func(*Config)

func WithConfig(c Config) Option    { return func(dst *Config) { *dst = c } }
func WithDataDir(v string) Option   { return func(c *Config) { c.DataDir = v } }
func WithWorkspace(v string) Option { return func(c *Config) { c.Workspace = v } }
func WithProvider(v string) Option  { return func(c *Config) { c.Provider = v } }
func WithModel(v string) Option     { return func(c *Config) { c.Model = v } }
func WithAPIKey(v string) Option    { return func(c *Config) { c.APIKey = v } }
func WithLaunchEnvironment(v *LaunchEnvironmentSnapshot) Option {
	return func(c *Config) { c.LaunchEnvironment = cloneLaunchEnvironment(v) }
}
func WithBaseURL(v string) Option { return func(c *Config) { c.BaseURL = v } }
func WithWebSearchProvider(v string) Option {
	return func(c *Config) { c.WebSearchProvider = v }
}
func WithWebFetchProvider(v string) Option {
	return func(c *Config) {
		c.WebFetchProvider = v
		c.WebTools = normalizeWebToolConfig(c.WebTools)
		c.WebTools.FetchEnabled = strings.TrimSpace(v) != ""
	}
}
func WithDynamicCordisVMTimeout(v time.Duration) Option {
	return func(c *Config) { c.DynamicCordisVMTimeout = v }
}
func WithFrontendDir(v string) Option { return func(c *Config) { c.FrontendDir = v } }
func WithPluginDir(v string) Option   { return func(c *Config) { c.PluginDir = v } }
func WithBundledBadgeSkill(v bool) Option {
	return func(c *Config) { c.BundledBadgeSkill = v }
}
func WithPluginDirs(v ...string) Option {
	return func(c *Config) { c.PluginDirs = append([]string(nil), v...) }
}
func WithClientPlugins(v ...string) Option {
	return func(c *Config) { c.ClientPlugins = append([]string{}, v...) }
}
func WithClientHMRPollInterval(v time.Duration) Option {
	return func(c *Config) { c.ClientHMRPollInterval = v }
}
func WithPersistence(v bool) Option { return func(c *Config) { c.Persist = v } }
func WithSessionStore(v SessionStore) Option {
	return func(c *Config) {
		c.SessionStore = v
		c.Persist = v != nil
	}
}
func WithSessionTitleLLM(v bool) Option {
	return func(c *Config) { c.SessionTitleLLM.Enabled = v }
}
func WithSessionTelemetry(v *SessionTelemetryConfig) Option {
	return func(c *Config) { c.SessionTelemetry = v }
}
func WithRetryPolicy(v RetryPolicy) Option {
	return func(c *Config) { c.RetryPolicy = v }
}
func WithCompaction(v CompactionConfig) Option {
	return func(c *Config) { c.Compaction = v }
}
func WithToolResultPruner(v ToolResultPruneConfig) Option {
	return func(c *Config) { c.ToolResultPruner = v }
}
func WithSpill(v SpillConfig) Option {
	return func(c *Config) { c.Spill = v }
}
func WithRepeatToolReminder(v RepeatToolReminderConfig) Option {
	return func(c *Config) { c.RepeatToolReminder = v }
}
func WithSessionReference(v SessionReferenceConfig) Option {
	return func(c *Config) { c.SessionReference = v }
}
func WithFileReference(v FileReferenceConfig) Option {
	return func(c *Config) { c.FileReference = v }
}
func WithStorageRuntime(v StorageRuntimeConfig) Option {
	return func(c *Config) { c.Storage = cloneStorageRuntimeConfig(&v) }
}
func WithRuntimeInvariants(v RuntimeInvariantConfig) Option {
	return func(c *Config) { c.RuntimeInvariants = cloneRuntimeInvariantConfig(&v) }
}
func WithTimeContext(v TimeContextConfig) Option {
	return func(c *Config) { c.TimeContext = &v }
}
func WithTmuxContext(v TmuxContextConfig) Option {
	return func(c *Config) { c.TmuxContext = &v }
}
func WithSchedule(v bool) Option { return func(c *Config) { c.ScheduleEnabled = v } }
func WithLSPServers(v map[string]LSPStdioConfig) Option {
	return func(c *Config) { c.LSPServers = v }
}
func WithLSPTool(v LSPToolConfig) Option { return func(c *Config) { c.LSPTool = v } }
func WithHooks(v ...HookBridgeConfig) Option {
	return func(c *Config) { c.Hooks = append([]HookBridgeConfig(nil), v...) }
}
func WithSubagentProviders(v ...SubagentProvider) Option {
	return func(c *Config) { c.SubagentProviders = append([]SubagentProvider(nil), v...) }
}
func WithSubagentTools(v ...SubagentToolConfig) Option {
	return func(c *Config) { c.SubagentTools = append([]SubagentToolConfig(nil), v...) }
}
func WithSubagentReportDelivery(v string) Option {
	return func(c *Config) { c.SubagentReportDelivery = v }
}
func WithAgentTeams(v AgentTeamConfig) Option {
	return func(c *Config) { c.AgentTeams = &v }
}
func WithTerminal(v TerminalConfig) Option {
	return func(c *Config) { c.Terminal = v }
}
func WithTerminalTool(v TerminalToolConfig) Option {
	return func(c *Config) { c.TerminalTool = v }
}
func WithE2B(v E2BConfig) Option { return func(c *Config) { c.E2B = &v } }

type ContentBlock struct {
	Type       string              `json:"type"`
	Text       string              `json:"text,omitempty"`
	Signature  string              `json:"signature,omitempty"`
	Attachment *ImageAttachmentRef `json:"attachment,omitempty"`
	ID         string              `json:"id,omitempty"`
	Name       string              `json:"name,omitempty"`
	Arguments  string              `json:"arguments,omitempty"`
	ToolCallID string              `json:"toolCallId,omitempty"`
	Content    []ContentBlock      `json:"content,omitempty"`
	IsError    bool                `json:"isError,omitempty"`
}

type PromptContentPart struct {
	Type      string `json:"type"`
	Text      string `json:"text,omitempty"`
	MediaType string `json:"mediaType,omitempty"`
	Data      string `json:"data,omitempty"`
	Name      string `json:"name,omitempty"`
}

type PromptRequest struct {
	SessionID      string                  `json:"sessionId,omitempty"`
	Mode           string                  `json:"mode,omitempty"`
	Content        []PromptContentPart     `json:"content"`
	ClientTimeZone string                  `json:"clientTimeZone,omitempty"`
	References     []SessionReferenceInput `json:"references,omitempty"`
	Literal        bool                    `json:"-"`
	RPCID          string                  `json:"-"`
	Source         map[string]any          `json:"-"`
}

type ModelSelection struct {
	Provider        string   `json:"provider"`
	Model           string   `json:"model"`
	ReasoningEffort string   `json:"reasoningEffort,omitempty"`
	Temperature     *float64 `json:"temperature,omitempty"`
	MaxTokens       int      `json:"maxTokens,omitempty"`
}

type ModelInfo struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Description     string   `json:"description,omitempty"`
	InputModalities []string `json:"inputModalities,omitempty"`
	ContextWindow   int      `json:"contextWindow,omitempty"`
	MaxTokens       int      `json:"maxTokens,omitempty"`
}

type ChatMessage struct {
	Role               string            `json:"role"`
	Content            string            `json:"content"`
	Parts              []ChatContentPart `json:"parts,omitempty"`
	Blocks             []ContentBlock    `json:"blocks,omitempty"`
	Images             []ChatImage       `json:"images,omitempty"`
	HadImages          bool              `json:"-"`
	Reasoning          string            `json:"reasoning_content,omitempty"`
	ReasoningSignature string            `json:"reasoning_signature,omitempty"`
	ToolCalls          []ToolCall        `json:"tool_calls,omitempty"`
	ToolCallID         string            `json:"tool_call_id,omitempty"`
}

type ChatImage struct {
	MediaType    string `json:"mediaType"`
	Data         string `json:"data"`
	FileID       string `json:"-"`
	AttachmentID string `json:"-"`
}

// ChatContentPart preserves text/image ordering for provider requests.
type ChatContentPart struct {
	Type         string `json:"type"`
	Text         string `json:"text,omitempty"`
	MediaType    string `json:"mediaType,omitempty"`
	Data         string `json:"data,omitempty"`
	FileID       string `json:"-"`
	AttachmentID string `json:"-"`
}

type ChatRequest struct {
	SessionID            string
	Model                string
	System               string
	Messages             []ChatMessage
	Tools                []ToolSchema
	Thinking             string
	ReasoningEffort      string
	Temperature          *float64
	MaxTokens            int
	Stop                 []string
	deepSeekFileVersions map[string]RequestImageAttachment
	deepSeekForceInline  bool
}

type Delta struct {
	Text               string
	Reasoning          string
	ReasoningSignature string
	ToolCalls          []ToolCallDelta
	Usage              map[string]any
	Finish             string
}

type Completion struct {
	Text               string
	Reasoning          string
	ReasoningSignature string
	ToolCalls          []ToolCall
	Usage              map[string]any
	Finish             string
}

type ToolConstrainedSampling struct {
	Type   string `json:"type"`
	Strict string `json:"strict,omitempty"`
}

// ToolSchema is the provider-neutral JSON schema advertised to a model.
type ToolSchema struct {
	Name                string                   `json:"name"`
	Description         string                   `json:"description,omitempty"`
	Parameters          map[string]any           `json:"parameters"`
	ConstrainedSampling *ToolConstrainedSampling `json:"constrainedSampling,omitempty"`
	// Output is the canonical result schema used by Code Mode and Go SDK clients.
	// Provider-native function calling only receives Name, Description, and Parameters.
	Output map[string]any `json:"-"`
}

// ToolCall preserves the model's raw argument JSON for replay and retries.
type ToolCall struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Arguments    json.RawMessage `json:"arguments"`
	Workspace    string          `json:"-"`
	SessionID    string          `json:"-"`
	ParentCallID string          `json:"-"`
}

type ToolCallDelta struct {
	Index          int
	ID             string
	Name           string
	ArgumentsDelta string
}

type ToolError struct {
	Name    string `json:"name,omitempty"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type ToolResult struct {
	Content []ContentBlock `json:"content,omitempty"`
	IsError bool           `json:"isError,omitempty"`
	Error   *ToolError     `json:"error,omitempty"`
	Value   any            `json:"-"`
	Meta    any            `json:"-"`
}

type TodoItem struct {
	Content string `json:"content"`
	Status  string `json:"status"`
}

// ToolExecutor is the minimal public extension point for custom frontends and
// CLIs. Arguments remain raw JSON so a tool can apply its own schema policy.
type ToolExecutor func(context.Context, ToolCall) (ToolResult, error)

type Tool struct {
	Schema  ToolSchema
	Timeout time.Duration
	Execute ToolExecutor
}

type Provider interface {
	ID() string
	Name() string
	Models(context.Context) ([]ModelInfo, error)
	Complete(context.Context, ChatRequest, func(Delta) error) (Completion, error)
}

type SessionHeader struct {
	Version         int    `json:"version"`
	ID              string `json:"id"`
	CreatedAt       int64  `json:"createdAt"`
	CWD             string `json:"cwd,omitempty"`
	ParentSession   string `json:"parentSession,omitempty"`
	SeedLength      int    `json:"seedLength,omitempty"`
	Origin          string `json:"origin,omitempty"`
	DelegationDepth int    `json:"delegationDepth,omitempty"`
	AgentPreset     string `json:"agentPreset,omitempty"`
	Mode            string `json:"mode,omitempty"`
}

type Event struct {
	Type            string `json:"type"`
	Seq             int    `json:"seq"`
	Time            int64  `json:"time"`
	Data            any    `json:"data"`
	SourceEventSeqs []int  `json:"sourceEventSeqs,omitempty"`
	SurfaceOp       any    `json:"surfaceOp,omitempty"`
	Ignorable       bool   `json:"ignorable,omitempty"`
}

func (e Event) MarshalJSON() ([]byte, error) {
	type wireEvent struct {
		Type            string `json:"type"`
		Seq             int    `json:"seq"`
		Time            int64  `json:"time"`
		Data            any    `json:"data"`
		SourceEventSeqs *[]int `json:"sourceEventSeqs,omitempty"`
		SurfaceOp       any    `json:"surfaceOp,omitempty"`
		Ignorable       bool   `json:"ignorable,omitempty"`
	}
	var sources *[]int
	if e.SourceEventSeqs != nil {
		sources = &e.SourceEventSeqs
	}
	return json.Marshal(wireEvent{
		Type: e.Type, Seq: e.Seq, Time: e.Time, Data: e.Data,
		SourceEventSeqs: sources, SurfaceOp: e.SurfaceOp, Ignorable: e.Ignorable,
	})
}

type SessionSummary struct {
	SessionID       string `json:"sessionId"`
	UpdatedAt       int64  `json:"updatedAt"`
	Running         bool   `json:"running"`
	Blank           bool   `json:"blank"`
	ParentSessionID string `json:"parentSessionId,omitempty"`
	Origin          string `json:"origin,omitempty"`
	CWD             string `json:"cwd,omitempty"`
	AgentPreset     string `json:"agentPreset,omitempty"`
	Projections     any    `json:"projections,omitempty"`
}

type HistoryEntry struct {
	Event Event `json:"event"`
	View  any   `json:"view,omitempty"`
}

type Session struct {
	Header SessionHeader
	Model  ModelSelection
	Title  string
	Events []Event
	// attached distinguishes a live in-process agent from a cold persisted log.
	// It is intentionally private: callers use the host/session APIs rather than
	// mutating lifecycle state behind the Engine registry.
	attached          bool
	draining          bool
	firstLiveSeq      int
	Running           bool
	Cancel            context.CancelFunc
	maintenance       bool
	maintenanceWake   bool
	maintenanceCancel context.CancelFunc
	turnCounter       int
	pending           []*queuedPrompt
	steering          []*queuedPrompt
	// requestHeaderLogged belongs to the live agent attachment. A cold session
	// logs one resume snapshot before its first provider request.
	requestHeaderLogged bool
	repeatToolKey       string
	repeatToolCount     int
	personaOverride     string
	toolRestriction     *sessionToolRestriction
	scheduleMu          sync.Mutex
	mu                  sync.Mutex
	store               SessionStore
	invariants          *InvariantRegistry
}

type Workspace struct {
	WorkspaceID string   `json:"workspaceId"`
	Path        string   `json:"path"`
	Title       string   `json:"title"`
	SessionIDs  []string `json:"sessionIds"`
	CreatedAt   string   `json:"createdAt"`
	UpdatedAt   string   `json:"updatedAt"`
}

type goalState struct {
	ID            string
	Revision      int
	Objective     string
	Phase         string
	MaxRounds     int
	RoundsStarted int
	CreatedAt     int64
	UpdatedAt     int64
	Activation    string
	BlockedReason *GoalBlockReason
}

type SessionConflictError struct {
	SessionID    string
	RequestedCWD string
	ExistingCWD  string
}

func (e *SessionConflictError) Error() string {
	return fmt.Sprintf("session-conflict: session %q already uses cwd %q", e.SessionID, e.ExistingCWD)
}

type AgentPresetConflictError struct {
	SessionID       string
	RequestedPreset string
	ExistingPreset  string
}

func (e *AgentPresetConflictError) Error() string {
	if e.ExistingPreset == "" {
		return fmt.Sprintf("agent-preset-conflict: session %q records no agent preset", e.SessionID)
	}
	return fmt.Sprintf("agent-preset-conflict: session %q already uses agent preset %q", e.SessionID, e.ExistingPreset)
}

type Engine struct {
	cfg                   Config
	mu                    sync.RWMutex
	sessions              map[string]*Session
	providers             map[string]Provider
	subagentProviders     map[string]SubagentProvider
	piAIProviders         map[string]*managedPiAIProvider
	retryPolicies         map[string]RetryPolicy
	webSearchProviders    map[string]WebSearchProvider
	webFetchProviders     map[string]WebFetchProvider
	tools                 map[string]Tool
	toolOwners            map[string]string
	workspaces            map[string]*Workspace
	workspaceOrder        []string
	archived              map[string]bool
	goals                 map[string]goalState
	settings              map[string]map[string]any
	settingsRev           map[string]int
	credentials           map[string]string
	credentialRecords     map[CredentialKey]CredentialRecord
	credentialRecordOrder []CredentialKey
	credentialService     *CredentialService
	authorizationService  *AuthorizationService
	subs                  map[string]map[chan Event]struct{}
	hostSubs              map[chan map[string]any]struct{}
	muxSubs               map[chan map[string]any]struct{}
	pendingMu             sync.Mutex
	pending               map[string]*pendingInteraction
	scheduleRuntimeMu     sync.Mutex
	scheduleRuntimes      map[string]*scheduleRuntime
	dynamicCordis         *dynamicCordisState
	fsState               *fsObservationState
	fileReferenceMu       sync.Mutex
	fileReferenceSearches map[string]*WorkspaceFileSearch
	jobs                  *jobRegistry
	shells                *persistentShellRegistry
	terminals             *terminalRegistry
	e2b                   *E2BRuntime
	lsp                   *lspRegistry
	telemetry             *sessionTelemetryCoordinator
	agentTeams            *TeamService
	sessionStore          SessionStore
	sessionProjections    *SessionProjectionRegistry
	projectionCache       *sessionProjectionCache
	storage               *StorageHub
	storageBackends       []StorageBackend
	storageDisposers      []func()
	storageDomain         *DomainFacility
	// DeepSeek Files uploads are shared by every provider snapshot in this
	// process. The provider endpoint/key remain request-scoped; only the
	// owner-private index and in-flight upload coordination are shared.
	deepSeekFileStore *DeepSeekFileStore
	invariants        *InvariantRegistry
	hooks             []*HookBridge
	hookCtx           context.Context
	hookCancel        context.CancelFunc
	hookWG            sync.WaitGroup
	workerMu          sync.Mutex
	workerWG          sync.WaitGroup
	workersClosing    bool
	titleMu           sync.Mutex
	titleProvider     *sessionTitleProviderRegistration
	titleWork         map[string]*sessionTitleWorkState
	titleCtx          context.Context
	titleCancel       context.CancelFunc
	closed            bool
}

func New(opts ...Option) (*Engine, error) {
	cfg := normalizeConfig(DefaultConfig())
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg = normalizeConfig(cfg)
	if cfg.FrontendDir == "" || cfg.PluginDir == "" || cfg.PresetDir == "" {
		assets, err := defaultAssetPaths()
		if err != nil {
			return nil, fmt.Errorf("prepare embedded runtime assets: %w", err)
		}
		if cfg.FrontendDir == "" {
			cfg.FrontendDir = assets.FrontendDir
		}
		if cfg.PluginDir == "" {
			cfg.PluginDir = assets.PluginDir
		}
		if cfg.PresetDir == "" {
			cfg.PresetDir = assets.PresetDir
		}
	}
	if err := validateSessionTitleConfig(cfg.SessionTitle); err != nil {
		return nil, err
	}
	if err := validateSessionTitleLLMConfig(cfg.SessionTitleLLM); err != nil {
		return nil, err
	}
	if err := validateSessionTelemetryConfig(cfg.SessionTelemetry); err != nil {
		return nil, err
	}
	if err := validateRuntimePolicies(cfg); err != nil {
		return nil, err
	}
	if err := validateWebRuntimeConfig(cfg); err != nil {
		return nil, err
	}
	if _, err := cfg.SessionReference.normalized(); err != nil {
		return nil, err
	}
	if _, err := cfg.FileReference.normalized(); err != nil {
		return nil, err
	}
	if err := validateSessionProjectionCacheConfig(cfg.SessionProjectionCache); err != nil {
		return nil, err
	}
	if cfg.SessionProjectionCache != nil && !cfg.Persist {
		return nil, errors.New("session-projection-cache requires session persistence")
	}
	if err := validateContextConfig(cfg); err != nil {
		return nil, err
	}
	if err := validateTerminalConfig(cfg.Terminal); err != nil {
		return nil, err
	}
	if err := validateTerminalToolConfig(cfg.TerminalTool); err != nil {
		return nil, err
	}
	if cfg.ToolPresentation != "native" && cfg.ToolPresentation != "code" && cfg.ToolPresentation != "both" {
		return nil, fmt.Errorf("tools: mode must be native, code, or both, got %q", cfg.ToolPresentation)
	}
	if cfg.SubagentReportDelivery != "quiet" && cfg.SubagentReportDelivery != "next-step" {
		return nil, fmt.Errorf("tool-subagent-report: reportDelivery must be quiet or next-step, got %q", cfg.SubagentReportDelivery)
	}
	if cfg.InstructionMaxBytes < 0 {
		return nil, errors.New("agent-instructions: maxBytes must be non-negative")
	}
	if err := validateWebServerConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Workspace != "" {
		if abs, err := filepath.Abs(cfg.Workspace); err == nil {
			cfg.Workspace = abs
		}
	}
	if cfg.AgentTeams != nil {
		normalized, err := normalizeAgentTeamConfig(*cfg.AgentTeams)
		if err != nil {
			return nil, err
		}
		cfg.AgentTeams = &normalized
	}
	invariantConfig := RuntimeInvariantConfig{}
	if cfg.RuntimeInvariants != nil {
		invariantConfig = *cfg.RuntimeInvariants
	}
	invariants, err := NewInvariantRegistry(invariantConfig)
	if err != nil {
		return nil, err
	}
	sessionProjections, err := newBuiltinSessionProjectionRegistry()
	if err != nil {
		_ = invariants.Close()
		return nil, err
	}
	titleCtx, titleCancel := context.WithCancel(context.Background())
	e := &Engine{cfg: cfg, sessions: map[string]*Session{}, providers: map[string]Provider{}, subagentProviders: map[string]SubagentProvider{}, piAIProviders: map[string]*managedPiAIProvider{}, retryPolicies: map[string]RetryPolicy{}, webSearchProviders: map[string]WebSearchProvider{}, webFetchProviders: map[string]WebFetchProvider{}, tools: map[string]Tool{}, toolOwners: map[string]string{}, workspaces: map[string]*Workspace{}, workspaceOrder: []string{}, archived: map[string]bool{}, goals: map[string]goalState{}, settings: map[string]map[string]any{}, settingsRev: map[string]int{}, credentials: map[string]string{}, credentialRecords: map[CredentialKey]CredentialRecord{}, credentialRecordOrder: []CredentialKey{}, subs: map[string]map[chan Event]struct{}{}, hostSubs: map[chan map[string]any]struct{}{}, muxSubs: map[chan map[string]any]struct{}{}, pending: map[string]*pendingInteraction{}, dynamicCordis: newDynamicCordisState(), fsState: newFSObservationState(), fileReferenceSearches: map[string]*WorkspaceFileSearch{}, jobs: newJobRegistry(), shells: newPersistentShellRegistry(), terminals: newTerminalRegistry(), lsp: newLSPRegistry(), titleWork: map[string]*sessionTitleWorkState{}, titleCtx: titleCtx, titleCancel: titleCancel, invariants: invariants, sessionProjections: sessionProjections, storage: NewStorageHub()}
	sessionProjections.setRuntimeExecutor(func(job func()) error {
		if !e.dynamicCordis.loop.call(job) {
			return errors.New("dynamic Cordis runtime is closed")
		}
		return nil
	})
	sessionProjections.OnChanged(func(session *Session, change ProjectionChange) {
		e.emitMux(map[string]any{
			"type": "session/projection", "sessionId": session.Header.ID,
			"key": change.Key, "value": change.Value, "seq": change.Seq,
		})
	})
	e.credentialService = newCredentialService(e)
	e.authorizationService = newAuthorizationService(e.credentialService)
	e.authorizationService.Subscribe(func(key CredentialKey, settlement AuthorizationSettlement) error {
		return e.dispatchDynamicCordisEvent(nil, "", true, "authorization/settled", string(key), string(settlement))
	})
	if err := e.registerPiAIAuthFlows(); err != nil {
		return nil, err
	}
	if cfg.AgentTeams != nil {
		e.agentTeams, err = newTeamService(e, *cfg.AgentTeams)
		if err != nil {
			return nil, err
		}
	}
	ready := false
	defer func() {
		if !ready {
			_ = e.Close()
		}
	}()
	e.scheduleRuntimes = map[string]*scheduleRuntime{}
	if cfg.RuntimeInvariants != nil {
		if err := e.installRuntimeInvariants(); err != nil {
			_ = invariants.Close()
			return nil, err
		}
	}
	if cfg.E2B != nil {
		runtime, err := NewE2BRuntime(context.Background(), *cfg.E2B)
		if err != nil {
			return nil, err
		}
		e.e2b = runtime
	}
	if !cfg.Terminal.Disabled {
		backend, err := NewBashTerminalBackend(cfg.Terminal)
		if err != nil {
			return nil, err
		}
		if _, err := e.RegisterTerminalBackend(backend); err != nil {
			return nil, err
		}
	}
	e.RegisterProvider(NewEchoProvider("echo", "Echo"))
	for _, provider := range cfg.SubagentProviders {
		if err := e.RegisterSubagentProvider(provider); err != nil {
			return nil, err
		}
	}
	// Keep the shipped DeepSeek route mounted even before its credential exists.
	// The provider resolves its key and endpoint per request, so onboarding can
	// write credentials and become usable without restarting the host.
	e.RegisterProvider(&managedDeepSeekProvider{engine: e})
	if err := e.RegisterWebSearchProvider(&deepSeekWebSearchProvider{engine: e}); err != nil {
		return nil, err
	}
	if err := e.RegisterWebSearchProvider(NewExaSearchProvider(cfg.ExaSearch)); err != nil {
		return nil, err
	}
	if err := e.RegisterWebSearchProvider(NewPerplexitySearchProvider(cfg.PerplexitySearch)); err != nil {
		return nil, err
	}
	if cfg.WebFetchProvider == "http" {
		if err := e.RegisterWebFetchProvider(NewHTTPWebFetchProvider(cfg.HTTPWebFetch)); err != nil {
			return nil, err
		}
	}
	if cfg.APIKey != "" && cfg.Provider != "deepseek-official" {
		p := NewOpenAIProvider(cfg.Provider, cfg.BaseURL, cfg.APIKey, cfg.Model)
		e.RegisterProvider(p)
	}
	if cfg.SessionTitleLLM.Enabled {
		if _, err := e.RegisterSessionTitleProvider(&llmSessionTitleProvider{engine: e, config: cfg.SessionTitleLLM}); err != nil {
			return nil, err
		}
	}
	if err := registerBuiltinTools(e); err != nil {
		return nil, err
	}
	if err := registerModelTools(e); err != nil {
		return nil, err
	}
	for _, tool := range cfg.SubagentTools {
		if tool.Provider == "spawn" || tool.Provider == "fork" {
			continue
		}
		if err := e.RegisterSubagentTool(tool); err != nil {
			return nil, err
		}
	}
	if err := registerWebTools(e); err != nil {
		return nil, err
	}
	if err := registerWorkflowTools(e); err != nil {
		return nil, err
	}
	if cfg.ScheduleEnabled {
		if err := registerScheduleTools(e); err != nil {
			return nil, err
		}
	}
	serverIDs := make([]string, 0, len(cfg.LSPServers))
	for id := range cfg.LSPServers {
		serverIDs = append(serverIDs, id)
	}
	sort.Strings(serverIDs)
	for _, id := range serverIDs {
		provider, err := NewStdioLSPProvider(id, cfg.LSPServers[id])
		if err != nil {
			_ = e.Close()
			return nil, err
		}
		if _, err := e.RegisterLSPProvider(provider); err != nil {
			if closer, ok := provider.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
			_ = e.Close()
			return nil, err
		}
	}
	if cfg.LSPTool.Enabled {
		if err := e.EnableLSPTool(cfg.LSPTool); err != nil {
			_ = e.Close()
			return nil, err
		}
	}
	telemetry, err := newSessionTelemetryCoordinator(cfg.SessionTelemetry, cfg.DataDir, cfg.Version)
	if err != nil {
		_ = e.Close()
		return nil, err
	}
	e.telemetry = telemetry
	if err := e.initStorageRuntime(cfg.Storage); err != nil {
		_ = e.Close()
		return nil, err
	}
	if cfg.Persist {
		store := cfg.SessionStore
		if store == nil {
			store, err = NewJSONLSessionStore(filepath.Join(cfg.DataDir, "sessions"))
			if err != nil {
				_ = e.Close()
				return nil, err
			}
		}
		e.sessionStore = store
		e.cfg.SessionStore = store
		if cfg.SessionProjectionCache != nil {
			e.projectionCache, err = newSessionProjectionCache(cfg.DataDir, *cfg.SessionProjectionCache, e.sessionProjections)
			if err != nil {
				_ = e.Close()
				return nil, err
			}
		}
		if err := e.load(); err != nil {
			_ = e.Close()
			return nil, err
		}
		for _, session := range e.sessions {
			e.telemetry.trackSession(session)
		}
		resolved, err := e.resolvePiAIProvidersLocked(e.settings[piAISettingsNamespace])
		if err != nil {
			_ = e.Close()
			return nil, fmt.Errorf("invalid %s settings: %w", piAISettingsNamespace, err)
		}
		e.replacePiAIProvidersLocked(resolved)
		e.credentialService.startWatcher()
	}
	if e.agentTeams != nil {
		e.agentTeams.recoverAll()
	}
	for _, server := range cfg.MCPServers {
		if _, err := e.ConnectMCP(context.Background(), server); err != nil {
			_ = e.Close()
			return nil, err
		}
	}
	e.initHooks()
	ready = true
	return e, nil
}

func (e *Engine) Config() Config { return e.cfg }

// SessionProjections exposes the engine's projection registry for Go library
// integrations and custom transports.
func (e *Engine) SessionProjections() *SessionProjectionRegistry { return e.sessionProjections }

// Credentials returns the reusable credential service exposed by the Go library.
func (e *Engine) Credentials() *CredentialService { return e.credentialService }

// Authorization returns the reusable human-assisted credential flow service.
func (e *Engine) Authorization() *AuthorizationService { return e.authorizationService }

// AgentTeams returns the optional reusable Agent Teams service.
func (e *Engine) AgentTeams() *TeamService { return e.agentTeams }

// E2B returns the optional shared sandbox runtime for custom Go frontends.
func (e *Engine) E2B() *E2BRuntime { return e.e2b }

func (e *Engine) SessionTelemetrySharing() (SessionTelemetrySharingStatus, bool) {
	if e.telemetry == nil {
		return "", false
	}
	return e.telemetry.sharing, true
}

func (e *Engine) EmitSessionTelemetry(ctx context.Context, record SessionTelemetryRecord) error {
	return e.telemetry.emitDirect(ctx, record)
}

func (e *Engine) RegisterProvider(p Provider) {
	if p != nil {
		e.mu.Lock()
		if current := e.piAIProviders[p.ID()]; current != nil && p != current {
			delete(e.piAIProviders, p.ID())
		}
		e.providers[p.ID()] = p
		e.mu.Unlock()
		e.emitRemoteEvent("llm/adapters-updated")
	}
}

// SetProviderRetryPolicy installs a route policy for providers that do not
// expose RetryPolicy themselves. Provider-owned policies take precedence.
func (e *Engine) SetProviderRetryPolicy(providerID string, policy RetryPolicy) error {
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return errors.New("provider id is required")
	}
	e.mu.Lock()
	if e.retryPolicies == nil {
		e.retryPolicies = map[string]RetryPolicy{}
	}
	e.retryPolicies[providerID] = normalizeRetryPolicy(policy)
	e.mu.Unlock()
	return nil
}

// ClearProviderRetryPolicy removes a route override and restores the global
// default for providers without an owned policy.
func (e *Engine) ClearProviderRetryPolicy(providerID string) {
	e.mu.Lock()
	delete(e.retryPolicies, strings.TrimSpace(providerID))
	e.mu.Unlock()
}

// RegisterTool adds one model-visible tool. Duplicate names are rejected so a
// plugin cannot silently replace a security-sensitive implementation.
func (e *Engine) RegisterTool(tool Tool) error {
	return e.registerTool(tool, "")
}

func (e *Engine) registerTool(tool Tool, ownerSessionID string) error {
	return e.registerToolFrom(nil, tool, ownerSessionID)
}

func (e *Engine) registerToolFrom(origin *dynamicCordisRun, tool Tool, ownerSessionID string) error {
	tool.Schema.Name = strings.TrimSpace(tool.Schema.Name)
	if tool.Schema.Name == "" {
		return errors.New("tool name is required")
	}
	if tool.Execute == nil {
		return fmt.Errorf("tool %q has no executor", tool.Schema.Name)
	}
	if tool.Timeout < 0 {
		return fmt.Errorf("tool %q timeout must be non-negative", tool.Schema.Name)
	}
	if tool.Schema.Parameters == nil {
		tool.Schema.Parameters = map[string]any{"type": "object"}
	}
	data, err := json.Marshal(tool.Schema.Parameters)
	if err != nil {
		return fmt.Errorf("tool %q parameters: %w", tool.Schema.Name, err)
	}
	if err := json.Unmarshal(data, &tool.Schema.Parameters); err != nil {
		return fmt.Errorf("tool %q parameters: %w", tool.Schema.Name, err)
	}
	if tool.Schema.Output != nil {
		output, ok := cloneJSON(tool.Schema.Output).(map[string]any)
		if !ok {
			return fmt.Errorf("tool %q output schema must be a JSON object", tool.Schema.Name)
		}
		if err := validateWorkflowSchema(output, false); err != nil {
			return fmt.Errorf("tool %q output schema: %w", tool.Schema.Name, err)
		}
		tool.Schema.Output = output
	}
	e.mu.Lock()
	if _, exists := e.tools[tool.Schema.Name]; exists {
		e.mu.Unlock()
		return fmt.Errorf("tool already registered: %s", tool.Schema.Name)
	}
	e.tools[tool.Schema.Name] = tool
	if ownerSessionID != "" {
		e.toolOwners[tool.Schema.Name] = ownerSessionID
	}
	e.mu.Unlock()
	if err := e.emitDynamicCordisEventFrom(origin, "tools/change"); err != nil {
		e.mu.Lock()
		delete(e.tools, tool.Schema.Name)
		delete(e.toolOwners, tool.Schema.Name)
		e.mu.Unlock()
		return err
	}
	return nil
}

// UnregisterTool removes a tool and reports whether it was present.
func (e *Engine) UnregisterTool(name string) bool {
	removed, _ := e.unregisterToolFrom(nil, name)
	return removed
}

func (e *Engine) unregisterToolFrom(origin *dynamicCordisRun, name string) (bool, error) {
	e.mu.Lock()
	if _, ok := e.tools[name]; !ok {
		e.mu.Unlock()
		return false, nil
	}
	delete(e.tools, name)
	delete(e.toolOwners, name)
	e.mu.Unlock()
	return true, e.emitDynamicCordisEventFrom(origin, "tools/change")
}

// ListTools returns deterministic schemas for custom UIs and SDK clients.
func (e *Engine) ListTools() []ToolSchema {
	return e.listToolSchemas(nil)
}

func (e *Engine) listToolSchemas(allowed func(string) bool) []ToolSchema {
	e.mu.RLock()
	rows := make([]ToolSchema, 0, len(e.tools))
	for name, tool := range e.tools {
		if allowed != nil && !allowed(name) {
			continue
		}
		schema := tool.Schema
		data, _ := json.Marshal(schema.Parameters)
		// Decode into a fresh map. Assigning through the shallow-copied
		// schema.Parameters would mutate the registered tool while callers are
		// concurrently reading its schema.
		var parameters map[string]any
		if json.Unmarshal(data, &parameters) == nil {
			schema.Parameters = parameters
		}
		if schema.Output != nil {
			if output, ok := cloneJSON(schema.Output).(map[string]any); ok {
				schema.Output = output
			}
		}
		rows = append(rows, schema)
	}
	e.mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows
}

var shippedToolNames = map[string]bool{
	"bash": true, "pwsh": true, "read": true, "write": true, "edit": true, "glob": true, "grep": true,
	"read_image":         true,
	"str_replace_editor": true,
	"job_output":         true, "job_list": true, "job_kill": true,
	"skill": true, "get_goal": true, "create_goal": true, "update_goal": true,
	"send_message": true, "interrupt_agent": true, "list_agents": true, "report": true,
	"spawn_teammate": true, "followup_task": true, "wait_agent": true,
	"team_task_create": true, "team_task_list": true, "team_task_get": true, "team_task_update": true,
	"subagent": true, "subagent_fork": true, "ask_user_question": true,
	"subagent_codex": true, "subagent_claude_code": true,
	"todo_write": true, "web_search": true, "web_fetch": true,
	"workflow": true, "ralph": true, "run_code": true,
	"session_search": true, "session_event_search": true, "session_trace": true,
	"session_event_trace": true, "session_event_read": true,
	"cordis_inspect_list": true, "cordis_inspect_query": true, "cordis_inspect_self": true,
	"cordis_define": true, "cordis_run": true, "cordis_stop": true, "cordis_undefine": true,
	"schedule_create": true, "schedule_list": true, "schedule_delete": true,
	"lsp":           true,
	"terminal_open": true, "terminal_send": true, "terminal_read": true,
	"terminal_signal": true, "terminal_close": true, "terminal_list": true,
}

func (e *Engine) toolsForSession(s *Session) ([]ToolSchema, error) {
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	sessionID, restriction := s.Header.ID, s.toolRestriction
	reportVisible := s.Header.Origin == "subagent" && s.Header.Mode == "continuable" && s.attached
	s.mu.Unlock()
	allowed := func(name, owner string) bool {
		if name == "report" {
			return owner == "" && reportVisible && restriction.allows(name)
		}
		if owner != "" {
			return owner == sessionID
		}
		return restriction.allows(name)
	}
	if runtimeConfig.toolPresentation == "code" {
		return e.listToolSchemas(func(name string) bool {
			return name == "run_code" && (e.toolOwners[name] == "" || e.toolOwners[name] == sessionID)
		}), nil
	}
	if runtimeConfig.toolNames == nil {
		return e.listToolSchemas(func(name string) bool {
			return allowed(name, e.toolOwners[name]) &&
				(name != "run_code" || runtimeConfig.toolPresentation == "both")
		}), nil
	}
	rows := e.listToolSchemas(func(name string) bool {
		if !allowed(name, e.toolOwners[name]) {
			return false
		}
		if name == "run_code" {
			return runtimeConfig.toolPresentation == "both"
		}
		return !shippedToolNames[name] || runtimeConfig.toolNames[name]
	})
	if runtimeConfig.persistentBash {
		for index := range rows {
			if rows[index].Name != shellToolName {
				continue
			}
			rows[index].Description = runtimeConfig.persistentBashDesc
			if rows[index].Description == "" {
				rows[index].Description = persistentShellDefaultDescription
			}
			rows[index].Parameters = objectSchema(map[string]any{"command": map[string]any{"type": "string", "description": shellCommandDescription}}, "command")
			rows[index].Output = map[string]any{"type": "string"}
		}
	}
	return rows, nil
}

func (e *Engine) toolVisibleForSession(s *Session, name string) (bool, error) {
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	sessionID, restriction := s.Header.ID, s.toolRestriction
	reportVisible := s.Header.Origin == "subagent" && s.Header.Mode == "continuable" && s.attached
	s.mu.Unlock()
	if runtimeConfig.toolPresentation == "code" {
		if name != "run_code" {
			return false, nil
		}
		e.mu.RLock()
		owner := e.toolOwners[name]
		e.mu.RUnlock()
		return owner == "" || owner == sessionID, nil
	}
	e.mu.RLock()
	owner := e.toolOwners[name]
	e.mu.RUnlock()
	if owner != "" && owner != sessionID {
		return false, nil
	}
	if name == "report" {
		return owner == "" && reportVisible && restriction.allows(name) &&
			(runtimeConfig.toolNames == nil || runtimeConfig.toolNames[name]), nil
	}
	if owner == "" && !restriction.allows(name) && name != "run_code" {
		return false, nil
	}
	if name == "run_code" {
		return runtimeConfig.toolPresentation == "both", nil
	}
	if !shippedToolNames[name] {
		return true, nil
	}
	return runtimeConfig.toolNames == nil || runtimeConfig.toolNames[name], nil
}

func newID(prefix string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(b)
}

func (e *Engine) load() error {
	headers, err := e.sessionStore.List(context.Background())
	if err != nil {
		return err
	}
	for _, header := range headers {
		inspection, err := e.sessionStore.Load(context.Background(), header.ID)
		if err != nil {
			continue
		}
		pending, steering, err := restorePromptQueues(inspection.Events)
		if err != nil {
			continue
		}
		s := &Session{
			Header: inspection.Meta, Title: sessionTitleFromEvents(inspection.Events),
			Events: append([]Event(nil), inspection.Events...), firstLiveSeq: len(inspection.Events),
			pending: pending, steering: steering, store: e.sessionStore, invariants: e.invariants,
		}
		if err := e.invariants.ValidateSession(s.Header, s.Events); err != nil {
			return fmt.Errorf("session %q: %w", s.Header.ID, err)
		}
		if title, ok := FoldSessionTitle(s.Events); ok {
			s.Title = NormalizeSessionTitle(title.Title, e.cfg.SessionTitle.MaxTitleBytes)
		}
		s.Model = ModelSelection{Provider: e.cfg.Provider, Model: e.cfg.Model}
		if selection, ok := latestLoggedModel(s.Events); ok {
			s.Model = selection
		}
		goal, hasGoal, goalErr := foldGoalState(s.Events)
		if goalErr != nil {
			continue
		}
		e.sessions[s.Header.ID] = s
		if hasGoal {
			goal.Activation = "disarmed"
			e.goals[s.Header.ID] = goal
		}
	}
	settingsLoaded, err := loadSettingsYAML(e)
	if err != nil {
		return err
	}
	if !settingsLoaded {
		if data, err := os.ReadFile(filepath.Join(e.cfg.DataDir, "settings.json")); err == nil {
			var stored struct {
				Values map[string]map[string]any `json:"values"`
				Revs   map[string]int            `json:"revisions"`
			}
			if json.Unmarshal(data, &stored) == nil {
				for ns, value := range stored.Values {
					e.settings[ns] = value
				}
				for ns, rev := range stored.Revs {
					e.settingsRev[ns] = rev
				}
			}
		}
	}
	if err := e.loadState(); err != nil {
		return err
	}
	return e.loadCredentials()
}

func (e *Engine) prepareSettingsLocked() error {
	if !e.cfg.Persist {
		return nil
	}
	return prepareSettingsYAMLLocked(e)
}

func (e *Engine) credentialsPath() string {
	return filepath.Join(e.cfg.DataDir, "credentials.json")
}

func validCredentialRef(ref string) bool {
	if ref == "" {
		return false
	}
	for i := 0; i < len(ref); i++ {
		c := ref[i]
		letter := c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z'
		digit := c >= '0' && c <= '9'
		if i == 0 {
			if !letter && c != '_' {
				return false
			}
		} else if !letter && !digit && c != '_' {
			return false
		}
	}
	return true
}

func (e *Engine) loadCredentials() error {
	if loaded, err := loadCredentialYAMLDocument(e); loaded || err != nil {
		if err != nil {
			return err
		}
		return nil
	}
	path := e.credentialsPath()
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("credentials file %s must not be readable beyond its owner", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	values := map[string]string{}
	if err := json.Unmarshal(data, &values); err != nil {
		return fmt.Errorf("invalid credentials file: %w", err)
	}
	for ref, value := range values {
		if !validCredentialRef(ref) || value == "" {
			return errors.New("invalid credentials file")
		}
	}
	e.credentials = values
	e.credentialRecords = map[CredentialKey]CredentialRecord{}
	e.credentialRecordOrder = nil
	return nil
}

func cloneLaunchEnvironment(source *LaunchEnvironmentSnapshot) *LaunchEnvironmentSnapshot {
	if source == nil {
		return nil
	}
	clone := func(values map[string]string) map[string]string {
		if values == nil {
			return nil
		}
		out := make(map[string]string, len(values))
		for name, value := range values {
			out[name] = value
		}
		return out
	}
	return &LaunchEnvironmentSnapshot{Process: clone(source.Process), Project: clone(source.Project), User: clone(source.User)}
}

func launchEnvironmentValue(values map[string]string, name string) (string, bool) {
	if runtime.GOOS != "windows" {
		value, ok := values[name]
		return value, ok
	}
	for candidate, value := range values {
		if strings.EqualFold(candidate, name) {
			return value, true
		}
	}
	return "", false
}

func (e *Engine) inheritedCredential(ref string) (string, bool) {
	if e.cfg.LaunchEnvironment != nil {
		if value, ok := launchEnvironmentValue(e.cfg.LaunchEnvironment.Process, ref); ok && value != "" {
			return value, true
		}
	} else if value, ok := os.LookupEnv(ref); ok && value != "" {
		return value, true
	}
	if ref == "DEEPSEEK_API_KEY" && e.cfg.APIKey != "" {
		return e.cfg.APIKey, true
	}
	return "", false
}

func (e *Engine) dotenvCredential(ref string) (string, string, bool) {
	if e.cfg.LaunchEnvironment == nil {
		return "", "", false
	}
	if value, ok := launchEnvironmentValue(e.cfg.LaunchEnvironment.Project, ref); ok && value != "" {
		return value, "project-env", true
	}
	if value, ok := launchEnvironmentValue(e.cfg.LaunchEnvironment.User, ref); ok && value != "" {
		return value, "user-env", true
	}
	return "", "", false
}

func (e *Engine) resolveCredential(ref string) (string, string, bool) {
	if value, ok := e.inheritedCredential(ref); ok {
		return value, "env", true
	}
	e.mu.RLock()
	value, ok := e.credentials[ref]
	e.mu.RUnlock()
	if ok {
		return value, "file", true
	}
	return e.dotenvCredential(ref)
}

func (e *Engine) credentialInfo(ref string) (configured bool, source string, writable bool) {
	if _, ok := e.inheritedCredential(ref); ok {
		return true, "env", false
	}
	e.mu.RLock()
	_, configured = e.credentials[ref]
	e.mu.RUnlock()
	if configured {
		return true, "file", true
	}
	if _, source, ok := e.dotenvCredential(ref); ok {
		return true, source, true
	}
	return false, "", true
}

func (e *Engine) setCredential(ref, value string) *RPCError {
	return e.setCredentialFrom(nil, ref, value)
}

func (e *Engine) setCredentialFrom(origin *dynamicCordisRun, ref, value string) *RPCError {
	if !validCredentialRef(ref) || value == "" {
		return rpcError("bad-request", "invalid credential", nil)
	}
	if err := e.credentialService.set(context.Background(), origin, CredentialRef(ref), value); err != nil {
		return rpcError("credential-rejected", err.Error(), map[string]any{"ref": ref})
	}
	return nil
}

func (e *Engine) unsetCredential(ref string) *RPCError {
	return e.unsetCredentialFrom(nil, ref)
}

func (e *Engine) unsetCredentialFrom(origin *dynamicCordisRun, ref string) *RPCError {
	if !validCredentialRef(ref) {
		return rpcError("bad-request", "invalid credential", nil)
	}
	if err := e.credentialService.unset(context.Background(), origin, CredentialRef(ref)); err != nil {
		return rpcError("credential-rejected", err.Error(), map[string]any{"ref": ref})
	}
	return nil
}

func readSession(r io.Reader) (*Session, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	var header SessionHeader
	var first map[string]any
	var events []Event
	if !scanner.Scan() {
		return nil, errors.New("empty session log")
	}
	if err := json.Unmarshal(scanner.Bytes(), &first); err != nil {
		return nil, err
	}
	b, _ := json.Marshal(first)
	if err := json.Unmarshal(b, &header); err != nil {
		return nil, err
	}
	if first["type"] != "session" || header.ID == "" {
		return nil, errors.New("invalid session header")
	}
	if header.Version != SessionFormatVersion {
		return nil, fmt.Errorf("unsupported session format version %d", header.Version)
	}
	line := 1
	for scanner.Scan() {
		line++
		record, err := decodeSessionStorageRecord(scanner.Bytes())
		if err != nil {
			return nil, fmt.Errorf("session log line %d: %w", line, err)
		}
		for _, event := range record {
			if event.Seq != len(events) {
				return nil, fmt.Errorf("session log line %d: expected seq %d, got %d", line, len(events), event.Seq)
			}
			events = append(events, event)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if _, err := foldSurfaceEvents(events, true); err != nil {
		return nil, err
	}
	pending, steering, err := restorePromptQueues(events)
	if err != nil {
		return nil, err
	}
	return &Session{Header: header, Title: sessionTitleFromEvents(events), Events: events, firstLiveSeq: len(events), pending: pending, steering: steering}, nil
}

func (e *Engine) CreateSession(ctx context.Context, cwd, id, preset string) (string, error) {
	// The composed Web/RPC path resolves a preset before reaching this API.
	// Keep an unconfigured library session's core log free of optional policy
	// events, while an explicit preset or permission environment still pins it.
	created, err := e.createSessionWithPresetAdoption(ctx, SessionHeader{ID: id, CWD: cwd, AgentPreset: preset}, shouldPinPermissionSnapshot(preset), false)
	if err == nil && e.agentTeams != nil {
		e.agentTeams.recoverSession(created)
	}
	return created, err
}

func shouldPinPermissionSnapshot(preset string) bool {
	return strings.TrimSpace(preset) != "" || strings.TrimSpace(os.Getenv("DSH_PERMISSION_MODE")) != ""
}

func (e *Engine) createSession(ctx context.Context, meta SessionHeader, pinPermission bool) (string, error) {
	return e.createSessionWithPresetAdoption(ctx, meta, pinPermission, false)
}

func (e *Engine) createSessionWithPresetAdoption(ctx context.Context, meta SessionHeader, pinPermission, adoptExistingPreset bool) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	cwd, id, preset := meta.CWD, meta.ID, meta.AgentPreset
	if cwd == "" {
		cwd = e.cfg.Workspace
	}
	if abs, err := filepath.Abs(cwd); err == nil {
		cwd = abs
	}
	if id == "" {
		id = newID("ses")
	}
	meta.CWD, meta.ID, meta.AgentPreset = cwd, id, preset
	e.mu.Lock()
	defer e.mu.Unlock()
	if existing := e.sessions[id]; existing != nil {
		existing.mu.Lock()
		if existing.draining {
			existing.mu.Unlock()
			return "", fmt.Errorf("session-draining: session %q is being released", id)
		}
		existingCWD := existing.Header.CWD
		existingPreset := sessionAgentPreset(existing.Header, existing.Events)
		if existing.Title == "" {
			existing.Title = sessionTitleFromEvents(existing.Events)
		}
		if existingCWD != cwd {
			existing.mu.Unlock()
			return "", &SessionConflictError{SessionID: id, RequestedCWD: cwd, ExistingCWD: existingCWD}
		}
		if !adoptExistingPreset && preset != "" && preset != existingPreset {
			existing.mu.Unlock()
			return "", &AgentPresetConflictError{SessionID: id, RequestedPreset: preset, ExistingPreset: existingPreset}
		}
		if meta.ParentSession != "" && meta.ParentSession != existing.Header.ParentSession ||
			meta.Origin != "" && meta.Origin != existing.Header.Origin ||
			meta.DelegationDepth != 0 && meta.DelegationDepth != existing.Header.DelegationDepth ||
			meta.SeedLength != 0 && meta.SeedLength != existing.Header.SeedLength ||
			meta.Mode != "" && meta.Mode != existing.Header.Mode {
			existing.mu.Unlock()
			return "", fmt.Errorf("session-conflict: session %q metadata does not match", id)
		}
		wasAttached := existing.attached
		existing.attached = true
		startWorker := !existing.Running && (len(existing.pending) > 0 || len(existing.steering) > 0)
		if startWorker {
			existing.Running = true
		}
		existing.mu.Unlock()
		if !wasAttached {
			if goal, ok := e.goals[id]; ok && goal.Phase == "active" {
				goal.Activation = "disarmed"
				e.goals[id] = goal
			}
		}
		if startWorker {
			e.mu.Unlock()
			e.emitQueue(existing)
			e.emitHost(map[string]any{"type": "host/session-status", "sessionId": id, "running": true})
			e.notifyAgentTeamStatus(id)
			e.launchSessionWorker(existing)
			e.mu.Lock()
		}
		if !wasAttached && e.cfg.ScheduleEnabled {
			e.mu.Unlock()
			e.startScheduleRuntime(existing)
			e.mu.Lock()
		}
		if !wasAttached {
			e.mu.Unlock()
			e.startSessionHooks(existing, "resume")
			e.emitDynamicCordisScopedContained(id, "session/created", dynamicSessionView(existing))
			e.mu.Lock()
		}
		return id, nil
	}
	now := time.Now().UnixMilli()
	meta.Version, meta.CreatedAt = SessionFormatVersion, now
	s := &Session{Header: meta, Model: ModelSelection{Provider: e.cfg.Provider, Model: e.cfg.Model}, attached: true, store: e.sessionStore, invariants: e.invariants}
	if s.store != nil {
		if err := s.store.Create(ctx, meta); err != nil {
			return "", err
		}
	}
	if pinPermission {
		name := e.permissionDefaultPresetLocked()
		spec := commandPermissionPresets[name]
		s.mu.Lock()
		for _, item := range []struct {
			typ  string
			data map[string]any
		}{
			{"permission/preset", map[string]any{"preset": name}},
			{"sandbox/mode", map[string]any{"mode": spec.sandbox}},
			{"approval/policy", map[string]any{"policy": spec.approval}},
		} {
			if _, err := appendEventLocked(s, item.typ, item.data, nil, nil, false); err != nil {
				s.mu.Unlock()
				rollbackSessionStoreCreate(s.store, id)
				return "", err
			}
		}
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.firstLiveSeq = len(s.Events)
	s.mu.Unlock()
	e.sessions[id] = s
	e.mu.Unlock()
	e.emitHost(map[string]any{"type": "host/session-added", "sessionId": id, "blank": true, "cwd": cwd, "agentPreset": preset})
	e.emitDynamicCordisScopedContained(id, "session/created", dynamicSessionView(s))
	e.startSessionHooks(s, "startup")
	if e.cfg.ScheduleEnabled {
		e.startScheduleRuntime(s)
	}
	e.mu.Lock()
	return id, nil
}

func writeJSONLine(w io.Writer, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "%s\n", b)
	return err
}

func (e *Engine) getSession(id string) (*Session, error) {
	e.mu.RLock()
	s := e.sessions[id]
	e.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("session-not-found: %s", id)
	}
	return s, nil
}

func (e *Engine) appendEvent(s *Session, typ string, data any, sourceEventSeqs ...int) (Event, error) {
	var surfaceOp any
	if isSurfaceEligibleType(typ) {
		surfaceOp = "append"
	}
	return e.appendEventWithMetadata(s, typ, data, surfaceOp, sourceEventSeqs, false)
}

func (e *Engine) appendEventWithMetadata(s *Session, typ string, data, surfaceOp any, sourceEventSeqs []int, ignorable bool) (Event, error) {
	return e.appendEventWithMetadataObserved(s, typ, data, surfaceOp, sourceEventSeqs, ignorable, true)
}

func (e *Engine) appendSeedEvent(s *Session, event Event) (Event, error) {
	return e.appendSeedEventFrom(nil, s, event)
}

func (e *Engine) appendSeedEventFrom(origin *dynamicCordisRun, s *Session, event Event) (Event, error) {
	s.mu.Lock()
	id := s.Header.ID
	appended, err := appendSeedEventLocked(s, event)
	if err == nil {
		s.firstLiveSeq = len(s.Events)
	}
	s.mu.Unlock()
	if err != nil {
		return Event{}, err
	}
	if origin == nil {
		e.publishEventFrom(nil, id, appended)
	} else {
		e.observeSessionTitleEvent(s, appended)
	}
	return appended, nil
}

func appendSeedEventLocked(s *Session, event Event) (Event, error) {
	if event.Type == "" {
		return Event{}, errors.New("seed event type is required")
	}
	if event.Seq != len(s.Events) {
		return Event{}, fmt.Errorf("seed event seq %d does not match expected %d", event.Seq, len(s.Events))
	}
	if event.Time < -maxJSONSafeInteger || event.Time > maxJSONSafeInteger {
		return Event{}, fmt.Errorf("seed event %d has an unsafe time", event.Seq)
	}
	event.Data = cloneJSON(event.Data)
	if event.SourceEventSeqs != nil {
		event.SourceEventSeqs = append([]int(nil), event.SourceEventSeqs...)
	}
	if s.invariants != nil {
		if err := s.invariants.validateSessionAppend(s.Header, s.Events, event); err != nil {
			return Event{}, err
		}
	}
	if s.store != nil {
		if err := s.store.Append(context.Background(), s.Header.ID, []Event{event}); err != nil {
			return Event{}, err
		}
	}
	resetRepeatToolChain(s, event.Type, event.Data)
	s.Events = append(s.Events, event)
	return event, nil
}

func (e *Engine) appendEventWithMetadataObserved(s *Session, typ string, data, surfaceOp any, sourceEventSeqs []int, ignorable, observe bool) (Event, error) {
	s.mu.Lock()
	id := s.Header.ID
	event, err := appendEventLocked(s, typ, data, surfaceOp, sourceEventSeqs, ignorable)
	if err == nil && !observe {
		s.firstLiveSeq = len(s.Events)
	}
	s.mu.Unlock()
	if err != nil {
		return Event{}, err
	}
	e.publishEvent(id, event)
	if observe {
		e.observeSessionTitleEvent(s, event)
	}
	return event, nil
}

// appendEventLocked commits one event while the caller owns s.mu. Queue
// mutations use it so the durable inbox splice and its in-memory projection
// cannot be interleaved by another prompt.
func appendEventLocked(s *Session, typ string, data, surfaceOp any, sourceEventSeqs []int, ignorable bool) (Event, error) {
	var sources []int
	if sourceEventSeqs != nil {
		sources = append(make([]int, 0, len(sourceEventSeqs)), sourceEventSeqs...)
	}
	event := Event{Type: typ, Seq: len(s.Events), Time: time.Now().UnixMilli(), Data: data, SurfaceOp: surfaceOp, SourceEventSeqs: sources, Ignorable: ignorable}
	if s.invariants != nil {
		if err := s.invariants.validateSessionAppend(s.Header, s.Events, event); err != nil {
			return Event{}, err
		}
	}
	if s.store != nil {
		if err := s.store.Append(context.Background(), s.Header.ID, []Event{event}); err != nil {
			return Event{}, err
		}
	}
	resetRepeatToolChain(s, typ, data)
	s.Events = append(s.Events, event)
	return event, nil
}

func (e *Engine) publishEvent(id string, event Event) {
	e.publishEventFrom(nil, id, event)
}

func (e *Engine) publishEventFrom(origin *dynamicCordisRun, id string, event Event) {
	if event.Type == "tool/result" {
		e.invalidateFileReferenceSearch(id)
	}
	s, sessionErr := e.getSession(id)
	if sessionErr == nil {
		s.mu.Lock()
		firstLiveSeq := s.firstLiveSeq
		s.mu.Unlock()
		if event.Seq < firstLiveSeq {
			e.telemetry.trackEvent(s, event)
		} else {
			e.telemetry.captureEvent(s, event)
		}
		var projectionErr error
		if origin == nil {
			_, projectionErr = e.sessionProjections.Drive(s, event)
		} else {
			_, projectionErr = e.sessionProjections.driveRuntime(s, event)
		}
		if projectionErr != nil {
			log.Printf("deepseek-harness: drive session projections for %q at seq %d: %v", id, event.Seq, projectionErr)
		}
	}
	e.emitEvent(id, event)
	if sessionErr == nil {
		_ = e.dispatchDynamicCordisEvent(origin, id, true, "session/event", dynamicSessionView(s), event)
	}
	if sessionErr == nil && e.projectionCache != nil {
		e.projectionCache.observe(s, event)
	}
}

func (e *Engine) emitEvent(id string, event Event) {
	e.mu.RLock()
	subs := e.subs[id]
	for ch := range subs {
		select {
		case ch <- event:
		default:
		}
	}
	e.mu.RUnlock()
}

func (e *Engine) emitHost(frame map[string]any) {
	e.mu.RLock()
	for ch := range e.hostSubs {
		select {
		case ch <- frame:
		default:
		}
	}
	e.mu.RUnlock()
}

func (e *Engine) emitMux(frame map[string]any) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for ch := range e.muxSubs {
		select {
		case ch <- frame:
		default:
		}
	}
}

func (e *Engine) Subscribe(ctx context.Context, id string) <-chan Event {
	ch := make(chan Event, 32)
	e.mu.Lock()
	if e.subs[id] == nil {
		e.subs[id] = map[chan Event]struct{}{}
	}
	e.subs[id][ch] = struct{}{}
	e.mu.Unlock()
	go func() {
		<-ctx.Done()
		e.mu.Lock()
		if _, ok := e.subs[id][ch]; ok {
			delete(e.subs[id], ch)
			close(ch)
		}
		e.mu.Unlock()
	}()
	return ch
}

func (e *Engine) SubscribeHost(ctx context.Context) <-chan map[string]any {
	ch := make(chan map[string]any, 32)
	e.mu.Lock()
	e.hostSubs[ch] = struct{}{}
	e.mu.Unlock()
	go func() {
		<-ctx.Done()
		e.mu.Lock()
		if _, ok := e.hostSubs[ch]; ok {
			delete(e.hostSubs, ch)
			close(ch)
		}
		e.mu.Unlock()
	}()
	return ch
}

func (e *Engine) SubscribeMux(ctx context.Context) <-chan map[string]any {
	ch := make(chan map[string]any, 64)
	e.mu.Lock()
	e.muxSubs[ch] = struct{}{}
	e.mu.Unlock()
	go func() {
		<-ctx.Done()
		e.mu.Lock()
		if _, ok := e.muxSubs[ch]; ok {
			delete(e.muxSubs, ch)
			close(ch)
		}
		e.mu.Unlock()
	}()
	return ch
}

// sessionListMetadata mirrors the upstream projection: standalone events do
// not start a conversation, and recency follows the latest human prompt rather
// than model/tool lifecycle noise.
func sessionListMetadata(events []Event) (blank bool, lastPromptAt int64) {
	blank = true
	for _, event := range events {
		if event.Type == "turn/start" {
			blank = false
		}
		if event.Type == "user/message" && eventSourceKind(event.Data) == "user" && event.Time > lastPromptAt {
			lastPromptAt = event.Time
		}
	}
	return blank, lastPromptAt
}

// sessionAgentPreset resolves the composition that produced the session's
// history. The header is the creation-time value; later blank-session switches
// are durable log events and take precedence.
func sessionAgentPreset(header SessionHeader, events []Event) string {
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type != "agent-preset/selected" {
			continue
		}
		data, _ := events[index].Data.(map[string]any)
		if preset, ok := data["agentPreset"].(string); ok {
			return preset
		}
	}
	return header.AgentPreset
}

func eventSourceKind(value any) string {
	data, _ := value.(map[string]any)
	if data == nil {
		return ""
	}
	if nested, _ := data["message"].(map[string]any); nested != nil {
		data = nested
	}
	source, _ := data["source"].(map[string]any)
	kind, _ := source["kind"].(string)
	return kind
}

func latestLoggedModel(events []Event) (ModelSelection, bool) {
	var selection ModelSelection
	found := false
	for _, event := range events {
		if event.Type != "request/header" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		header, _ := data["header"].(map[string]any)
		config, _ := header["config"].(map[string]any)
		provider, _ := config["provider"].(string)
		model, _ := config["model"].(string)
		if provider == "" || model == "" {
			continue
		}
		selection = ModelSelection{Provider: provider, Model: model}
		selection.ReasoningEffort, _ = config["reasoningEffort"].(string)
		if temperature, ok := finiteFloatSetting(config["temperature"]); ok {
			selection.Temperature = &temperature
		}
		selection.MaxTokens = positiveIntSetting(config["maxTokens"], 0)
		found = true
	}
	return selection, found
}

func finiteFloatSetting(value any) (float64, bool) {
	var number float64
	switch value := value.(type) {
	case float64:
		number = value
	case float32:
		number = float64(value)
	case int:
		number = float64(value)
	case int64:
		number = float64(value)
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			return 0, false
		}
		number = parsed
	default:
		return 0, false
	}
	return number, !math.IsNaN(number) && !math.IsInf(number, 0)
}

func cloneModelSelection(selection ModelSelection) ModelSelection {
	clone := selection
	if selection.Temperature != nil {
		temperature := *selection.Temperature
		clone.Temperature = &temperature
	}
	return clone
}

func (e *Engine) ListSessions() []SessionSummary {
	e.mu.RLock()
	list := make([]*Session, 0, len(e.sessions))
	archived := make(map[string]bool, len(e.archived))
	for _, s := range e.sessions {
		list = append(list, s)
	}
	for id, value := range e.archived {
		archived[id] = value
	}
	e.mu.RUnlock()
	rows := make([]SessionSummary, 0, len(list))
	for _, s := range list {
		s.mu.Lock()
		id := s.Header.ID
		parentID := s.Header.ParentSession
		origin := s.Header.Origin
		cwd := s.Header.CWD
		preset := sessionAgentPreset(s.Header, s.Events)
		blank, lastPromptAt := sessionListMetadata(s.Events)
		updated := s.Header.CreatedAt
		if lastPromptAt > updated {
			updated = lastPromptAt
		}
		header := s.Header
		attached := s.attached
		running := s.Running
		lastSeq := len(s.Events) - 1
		s.mu.Unlock()
		var snapshot ProjectionSnapshot
		cached := false
		if !attached && e.projectionCache != nil {
			snapshot, cached = e.projectionCache.medium.snapshot(header, lastSeq, false, e.sessionProjections.Signature())
		}
		if !cached {
			var snapshotErr error
			snapshot, snapshotErr = e.sessionProjections.Snapshot(s)
			if snapshotErr != nil {
				log.Printf("deepseek-harness: snapshot session projections for list row %q: %v", id, snapshotErr)
				snapshot = ProjectionSnapshot{AsOfSeq: lastSeq, Values: map[string]any{}}
			}
		}
		if archived[id] {
			continue
		}
		row := SessionSummary{SessionID: id, UpdatedAt: updated, Running: running, Blank: blank, ParentSessionID: parentID, Origin: origin, CWD: cwd, AgentPreset: preset}
		row.Projections = map[string]any{"asOfSeq": snapshot.AsOfSeq, "values": snapshot.Values}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].UpdatedAt > rows[j].UpdatedAt })
	return rows
}

func (e *Engine) History(id string, beforeSeq, maxMessages int) ([]HistoryEntry, bool, error) {
	s, err := e.getSession(id)
	if err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	if maxMessages <= 0 {
		maxMessages = 50
	}
	window := events
	if beforeSeq >= 0 {
		window = make([]Event, 0, len(events))
		for _, event := range events {
			if event.Seq < beforeSeq {
				window = append(window, event)
			}
		}
	}
	cut := 0
	count := 0
	for i := len(window) - 1; i >= 0; i-- {
		event := window[i]
		if (event.Type != "user/message" && event.Type != "assistant/message") || !isAppendSurfaceEvent(event) {
			continue
		}
		count++
		groupStart := event.Seq
		for _, sourceSeq := range event.SourceEventSeqs {
			if sourceSeq < groupStart {
				groupStart = sourceSeq
			}
		}
		if count >= maxMessages {
			cut = groupStart
			break
		}
	}
	page := make([]Event, 0, len(window))
	for _, event := range window {
		if event.Seq >= cut {
			page = append(page, event)
		}
	}
	out := make([]HistoryEntry, 0, len(page))
	for _, event := range page {
		out = append(out, HistoryEntry{Event: event})
	}
	return out, cut > 0, nil
}

func isAppendSurfaceEvent(event Event) bool {
	op, ok := event.SurfaceOp.(string)
	return ok && op == "append"
}

func (e *Engine) RenameSession(id, title string) (string, int, error) {
	s, err := e.getSession(id)
	if err != nil {
		return "", -1, err
	}
	title = NormalizeSessionTitle(title, e.cfg.SessionTitle.MaxTitleBytes)
	if title == "" {
		return "", -1, errors.New("title-invalid: session title must contain visible characters")
	}
	e.titleMu.Lock()
	e.supersedeSessionTitleLocked(id)
	ev, err := e.appendSessionTitle(s, title, nil, SessionTitleSource{Kind: "user"})
	e.titleMu.Unlock()
	if err != nil {
		return "", -1, err
	}
	return title, ev.Seq, nil
}

func (e *Engine) SelectModel(id string, selection ModelSelection) error {
	s, err := e.getSession(id)
	if err != nil {
		return err
	}
	if selection.Provider == "" || selection.Model == "" {
		return errors.New("bad-request: provider and model are required")
	}
	if selection.Temperature != nil && (math.IsNaN(*selection.Temperature) || math.IsInf(*selection.Temperature, 0)) {
		return errors.New("bad-request: temperature must be finite")
	}
	e.mu.RLock()
	_, ok := e.providers[selection.Provider]
	e.mu.RUnlock()
	if !ok {
		return fmt.Errorf("model-unavailable: %s/%s", selection.Provider, selection.Model)
	}
	s.mu.Lock()
	s.Model = cloneModelSelection(selection)
	s.mu.Unlock()
	return nil
}

func (e *Engine) CancelSession(id string) error {
	s, err := e.getSession(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	cancel := s.Cancel
	maintenanceCancel := s.maintenanceCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if maintenanceCancel != nil {
		maintenanceCancel()
	}
	return nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	hostSubs := make([]chan map[string]any, 0, len(e.hostSubs))
	for ch := range e.hostSubs {
		hostSubs = append(hostSubs, ch)
	}
	e.hostSubs = map[chan map[string]any]struct{}{}
	muxSubs := make([]chan map[string]any, 0, len(e.muxSubs))
	for ch := range e.muxSubs {
		muxSubs = append(muxSubs, ch)
	}
	e.muxSubs = map[chan map[string]any]struct{}{}
	eventSubs := make([]chan Event, 0)
	for _, subs := range e.subs {
		for ch := range subs {
			eventSubs = append(eventSubs, ch)
		}
	}
	e.subs = map[string]map[chan Event]struct{}{}
	websocketPools := make([]*openAIResponsesWebSocketPool, 0, len(e.piAIProviders))
	for _, provider := range e.piAIProviders {
		websocketPools = append(websocketPools, provider.websockets)
	}
	e.mu.Unlock()
	var teamErr error
	if e.agentTeams != nil {
		teamErr = e.agentTeams.close()
	}
	e.closeFileReferenceSearches()
	e.authorizationService.close()
	e.credentialService.close()
	e.closeDynamicCordis()
	e.sessionProjections.setRuntimeExecutor(nil)
	closeMCPConnections(e)
	e.closeSessionTitles()
	e.closeScheduleRuntimes()
	for _, s := range sessions {
		s.mu.Lock()
		if s.Cancel != nil {
			s.Cancel()
		}
		s.mu.Unlock()
	}
	e.workerMu.Lock()
	e.workersClosing = true
	e.workerMu.Unlock()
	e.workerWG.Wait()
	var projectionCacheErr error
	if e.projectionCache != nil {
		projectionCacheErr = e.projectionCache.close(sessions)
	}
	e.closeHooks()
	e.telemetry.close(sessions)
	for _, pool := range websocketPools {
		pool.Close()
	}
	terminalErr := e.terminals.close()
	e.jobs.close()
	e.shells.close()
	lspErr := e.lsp.close()
	var sessionStoreErr error
	if e.sessionStore != nil {
		sessionStoreErr = e.sessionStore.Close()
	}
	var e2bErr error
	if e.e2b != nil {
		e2bErr = e.e2b.Close(context.Background())
	}
	var invariantErr error
	if e.invariants != nil {
		invariantErr = e.invariants.Close()
	}
	storageErr := e.closeStorageRuntime()
	for _, ch := range hostSubs {
		close(ch)
	}
	for _, ch := range muxSubs {
		close(ch)
	}
	for _, ch := range eventSubs {
		close(ch)
	}
	return errors.Join(teamErr, terminalErr, lspErr, projectionCacheErr, sessionStoreErr, e2bErr, invariantErr, storageErr)
}
