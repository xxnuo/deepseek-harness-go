package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	harness "github.com/xxnuo/deepseek-harness-go"
)

type invocation struct {
	mode        string
	profile     string
	patches     []string
	args        []string
	defaultOnly bool
}

type exitError struct{ code int }

func (e exitError) Error() string { return "" }

const processShutdownTimeout = 5 * time.Second

type processSignals struct {
	context.Context
	cancel  context.CancelFunc
	signals chan os.Signal
	done    chan struct{}
	once    sync.Once
	code    atomic.Int32
}

func newProcessSignals() *processSignals {
	ctx, cancel := context.WithCancel(context.Background())
	s := &processSignals{Context: ctx, cancel: cancel, signals: make(chan os.Signal, 2), done: make(chan struct{})}
	s.code.Store(-1)
	signal.Notify(s.signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-s.signals:
			code := signalExitCode(sig)
			s.code.Store(int32(code))
			cancel()
			timer := time.NewTimer(processShutdownTimeout)
			defer timer.Stop()
			select {
			case sig = <-s.signals:
				os.Exit(signalExitCode(sig))
			case <-timer.C:
				os.Exit(code)
			case <-s.done:
			}
		case <-s.done:
		}
	}()
	return s
}

func signalExitCode(sig os.Signal) int {
	if sig == os.Interrupt {
		return 130
	}
	return 0
}

func (s *processSignals) stop() {
	s.once.Do(func() {
		signal.Stop(s.signals)
		close(s.done)
		s.cancel()
	})
}

func (s *processSignals) translate(err error) error {
	if !errors.Is(err, context.Canceled) || s.code.Load() < 0 {
		return err
	}
	if s.code.Load() == 0 {
		return nil
	}
	return exitError{code: int(s.code.Load())}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "__landlock-run" {
		os.Exit(harness.RunLandlockLauncher(os.Args[2:], os.Stdout, os.Stderr))
	}
	if executable, err := os.Executable(); err == nil {
		_ = os.Setenv("DSH_GO_LANDLOCK_SELF", executable)
	}
	if err := run(os.Args[1:]); err != nil {
		var exit exitError
		if errors.As(err, &exit) {
			os.Exit(exit.code)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	return runWithIO(args, os.Stdin, os.Stdout, os.Stderr)
}

func runWithIO(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	inv, err := parseArgs(args)
	if err != nil {
		return err
	}
	switch inv.mode {
	case "help":
		printHelp(stdout)
		return nil
	case "version":
		fmt.Fprintln(stdout, harness.Version())
		return nil
	case "plugin":
		loader, err := newProfileLoader(false)
		if err != nil {
			return err
		}
		return loader.runPlugin(inv.profile, inv.args, stdin, stdout, stderr)
	case "dump":
		loader, err := newProfileLoader(true)
		if err != nil {
			return err
		}
		return loader.dump(inv.profile, inv.defaultOnly, inv.patches, stdout, stderr)
	case "profile":
		launchEnvironment, restoreEnv, err := loadLayeredEnv("dsh", stderr)
		if err != nil {
			return err
		}
		defer restoreEnv()
		loader, err := newProfileLoader(true)
		if err != nil {
			return err
		}
		loader.launchEnvironment = launchEnvironment
		loadConfig := func() (*composition, harness.Config, error) {
			composed, err := loader.compose(inv.profile, inv.patches, stderr)
			if err != nil {
				return nil, harness.Config{}, err
			}
			if err := composed.validate(); err != nil {
				return nil, harness.Config{}, err
			}
			cfg := engineConfig(loader, composed)
			attachSessionTelemetryWarnings(&cfg, stderr)
			return composed, cfg, nil
		}
		composed, cfg, err := loadConfig()
		if err != nil {
			return err
		}
		switch composed.surface() {
		case "web":
			var watch *webProfileWatch
			if composed.patchReload == "live" {
				watch = &webProfileWatch{
					paths: newProfileWatchPaths(
						filepath.Join(composed.profileDir, profilePatchFile),
						filepath.Join(loader.home, profilePatchFile),
					),
					load: func() (*composition, harness.Config, error) {
						next, cfg, err := loadConfig()
						if err != nil {
							return nil, harness.Config{}, err
						}
						if next.surface() != "web" {
							return nil, harness.Config{}, fmt.Errorf("dsh: profile %q no longer composes a Go web surface", inv.profile)
						}
						return next, cfg, nil
					},
				}
			}
			return runWeb(inv.args, composed, cfg, stdout, stderr, watch)
		case "headless":
			return runHeadless(inv.args, composed, cfg, stdout, stderr)
		case "acp":
			return runACP(inv.args, composed, cfg, stdin, stdout, stderr)
		case "sdk":
			return runSDK(inv.args, composed, cfg, stdin, stdout, stderr)
		case "custom":
			var watch *customProfileWatch
			if composed.patchReload == "live" {
				watch = &customProfileWatch{
					paths: newProfileWatchPaths(
						filepath.Join(composed.profileDir, profilePatchFile),
						filepath.Join(loader.home, profilePatchFile),
					),
					load: func() (*composition, harness.Config, error) {
						next, cfg, err := loadConfig()
						if err != nil {
							return nil, harness.Config{}, err
						}
						if next.surface() != "custom" {
							return nil, harness.Config{}, fmt.Errorf("dsh: profile %q no longer composes a custom app surface", inv.profile)
						}
						return next, cfg, nil
					},
				}
			}
			return runCustomProfile(inv.args, composed, cfg, stdout, stderr, watch)
		default:
			return fmt.Errorf("dsh: profile %q does not compose a Go web, headless, ACP, SDK, or custom app surface", inv.profile)
		}
	default:
		return fmt.Errorf("dsh: unsupported invocation mode %q", inv.mode)
	}
}

func runACP(args []string, composed *composition, cfg harness.Config, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) != 0 {
		return fmt.Errorf("error: ACP profile takes no app arguments, got %s", quoteArgs(args))
	}
	e, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		return err
	}
	defer e.Close()
	if _, err := mountProfileRuntimePlugins(e, composed, args); err != nil {
		return err
	}
	return e.ServeACP(context.Background(), stdin, stdout)
}

func runSDK(args []string, composed *composition, cfg harness.Config, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		profile := "sdk"
		if composed != nil {
			if configured, ok := composed.configString("sdk-app-startup", "profile"); ok {
				profile = configured
			}
		}
		printSDKHelp(stdout, profile)
		return nil
	}
	if len(args) != 0 {
		return fmt.Errorf("error: SDK profile takes no app arguments, got %s", quoteArgs(args))
	}
	signals := newProcessSignals()
	defer signals.stop()
	e, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		return err
	}
	defer e.Close()
	if _, err := mountProfileRuntimePlugins(e, composed, args); err != nil {
		return err
	}
	return signals.translate(e.ServeJSONRPCWithOptions(signals, stdin, stdout, composed.sdkServer))
}

func attachSessionTelemetryWarnings(cfg *harness.Config, stderr io.Writer) {
	if cfg.SessionTelemetry == nil || cfg.SessionTelemetry.OnError != nil {
		return
	}
	var mu sync.Mutex
	cfg.SessionTelemetry.OnError = func(err error) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(stderr, "dsh: %v\n", err)
	}
}

func parseArgs(args []string) (invocation, error) {
	if len(args) == 0 {
		return invocation{}, errors.New("error: --profile <name> is required")
	}
	if args[0] == "--" {
		args = args[1:]
		if len(args) == 0 {
			return invocation{}, errors.New("error: --profile <name> is required")
		}
	}
	switch args[0] {
	case "-h", "--help":
		return invocation{mode: "help"}, nil
	case "-V", "--version":
		return invocation{mode: "version"}, nil
	case "web":
		return parseBootArgs("web", args[1:])
	case "plugin":
		return parsePluginArgs(args[1:])
	}

	var inv invocation
	inv.mode = "profile"
	profileSet := false
	for index := 0; index < len(args); {
		argument := args[index]
		switch {
		case argument == "--":
			inv.args = append([]string(nil), args[index+1:]...)
			index = len(args)
		case argument == "-V" || argument == "--version":
			return invocation{mode: "version"}, nil
		case argument == "--profile":
			if index+1 >= len(args) || args[index+1] == "" {
				return invocation{}, errors.New("error: --profile needs a name")
			}
			inv.profile = args[index+1]
			profileSet = true
			index += 2
		case strings.HasPrefix(argument, "--profile="):
			inv.profile = strings.TrimPrefix(argument, "--profile=")
			if inv.profile == "" {
				return invocation{}, errors.New("error: --profile needs a name")
			}
			profileSet = true
			index++
		case argument == "--patch":
			if index+1 >= len(args) || args[index+1] == "" {
				return invocation{}, errors.New("error: --patch needs a path")
			}
			inv.patches = append(inv.patches, args[index+1])
			index += 2
		case strings.HasPrefix(argument, "--patch="):
			patch := strings.TrimPrefix(argument, "--patch=")
			if patch == "" {
				return invocation{}, errors.New("error: --patch needs a path")
			}
			inv.patches = append(inv.patches, patch)
			index++
		case argument == "--dump-config":
			if inv.mode == "dump" && inv.defaultOnly {
				return invocation{}, errors.New("error: --dump-config and --dump-default-config are mutually exclusive")
			}
			inv.mode = "dump"
			index++
		case argument == "--dump-default-config":
			if inv.mode == "dump" && !inv.defaultOnly {
				return invocation{}, errors.New("error: --dump-config and --dump-default-config are mutually exclusive")
			}
			inv.mode = "dump"
			inv.defaultOnly = true
			index++
		default:
			inv.args = append([]string(nil), args[index:]...)
			index = len(args)
		}
	}
	if !profileSet {
		for _, argument := range inv.args {
			if argument == "-h" || argument == "--help" {
				return invocation{mode: "help"}, nil
			}
		}
		return invocation{}, errors.New("error: --profile <name> is required")
	}
	if len(inv.args) > 0 && (inv.args[0] == "web" || inv.args[0] == "plugin") {
		return invocation{}, fmt.Errorf("error: %s takes none of parent --profile, --patch, --dump-config, or --dump-default-config", inv.args[0])
	}
	return resolveBoot(inv)
}

func parseBootArgs(profile string, args []string) (invocation, error) {
	inv := invocation{mode: "profile", profile: profile}
	for index := 0; index < len(args); {
		argument := args[index]
		switch {
		case argument == "--":
			inv.args = append([]string(nil), args[index+1:]...)
			index = len(args)
		case argument == "--patch":
			if index+1 >= len(args) || args[index+1] == "" {
				return invocation{}, errors.New("error: --patch needs a path")
			}
			inv.patches = append(inv.patches, args[index+1])
			index += 2
		case strings.HasPrefix(argument, "--patch="):
			patch := strings.TrimPrefix(argument, "--patch=")
			if patch == "" {
				return invocation{}, errors.New("error: --patch needs a path")
			}
			inv.patches = append(inv.patches, patch)
			index++
		case argument == "--dump-config":
			if inv.mode == "dump" && inv.defaultOnly {
				return invocation{}, errors.New("error: --dump-config and --dump-default-config are mutually exclusive")
			}
			inv.mode = "dump"
			index++
		case argument == "--dump-default-config":
			if inv.mode == "dump" && !inv.defaultOnly {
				return invocation{}, errors.New("error: --dump-config and --dump-default-config are mutually exclusive")
			}
			inv.mode = "dump"
			inv.defaultOnly = true
			index++
		default:
			inv.args = append([]string(nil), args[index:]...)
			index = len(args)
		}
	}
	return resolveBoot(inv)
}

func resolveBoot(inv invocation) (invocation, error) {
	if inv.mode != "dump" {
		return inv, nil
	}
	if len(inv.args) > 0 {
		return invocation{}, fmt.Errorf("error: config dumps take no app arguments, got %s", quoteArgs(inv.args))
	}
	if inv.defaultOnly && len(inv.patches) > 0 {
		return invocation{}, errors.New("error: --dump-default-config prints the bundle layers and takes no --patch")
	}
	return inv, nil
}

func parsePluginArgs(args []string) (invocation, error) {
	inv := invocation{mode: "plugin"}
	for index := 0; index < len(args); {
		argument := args[index]
		switch {
		case argument == "--":
			inv.args = append([]string(nil), args[index+1:]...)
			index = len(args)
		case argument == "--profile":
			if index+1 >= len(args) || args[index+1] == "" {
				return invocation{}, errors.New("error: --profile needs a name")
			}
			inv.profile = args[index+1]
			index += 2
		case strings.HasPrefix(argument, "--profile="):
			inv.profile = strings.TrimPrefix(argument, "--profile=")
			if inv.profile == "" {
				return invocation{}, errors.New("error: --profile needs a name")
			}
			index++
		default:
			inv.args = append(inv.args, argument)
			index++
		}
	}
	if inv.profile == "" {
		return invocation{}, errors.New("error: required option '--profile <name>' not specified")
	}
	if len(inv.args) == 0 {
		return invocation{}, errors.New("error: plugin needs pnpm arguments to forward (e.g. add <package>)")
	}
	return inv, nil
}

func quoteArgs(args []string) string {
	quoted := make([]string, len(args))
	for index, argument := range args {
		quoted[index] = strconv.Quote(argument)
	}
	return strings.Join(quoted, " ")
}

func engineConfig(loader *profileLoader, composed *composition) harness.Config {
	cfg := harness.DefaultConfig()
	// Profile composition supplies the same execution-time selection fields as
	// the upstream WebRuntime. Start from the process environment explicitly so
	// an empty env value means "unset" rather than inheriting DefaultConfig's
	// library default; a profile's explicit web config below still wins.
	cfg.WebSearchProvider = strings.TrimSpace(os.Getenv("DSH_WEB_SEARCH_PROVIDER"))
	cfg.WebFetchProvider = strings.TrimSpace(os.Getenv("DSH_WEB_FETCH_PROVIDER"))
	cfg.LaunchEnvironment = loader.launchEnvironment
	if cfg.LaunchEnvironment != nil {
		cfg.APIKey = ""
	}
	cfg.DataDir = loader.home
	cfg.FrontendDir = loader.frontendDir()
	cfg.PluginDir = loader.packagesDir()
	if composed.profileDir != "" {
		cfg.PluginDirs = []string{filepath.Join(composed.profileDir, "node_modules")}
	}
	cfg.ClientPlugins = composed.activePluginNames()
	cfg.PluginInventory = composed.pluginInventoryEntries()
	if composed.clientHMRPollInterval > 0 {
		cfg.ClientHMRPollInterval = composed.clientHMRPollInterval
	}
	cfg.MCPServers = append([]harness.MCPConfig(nil), composed.mcpConfigs...)
	cfg.Jobs = composed.jobs
	cfg.Hooks = append([]harness.HookBridgeConfig(nil), composed.hooks...)
	cfg.LSPServers = composed.lspServers
	cfg.LSPTool = composed.lspTool
	cfg.Terminal = composed.terminalConfig
	cfg.TerminalTool = composed.terminalToolConfig
	cfg.SessionTelemetry = composed.sessionTelemetry
	cfg.SubagentProviders = append([]harness.SubagentProvider(nil), composed.subagentProviders...)
	cfg.SubagentTools = append([]harness.SubagentToolConfig(nil), composed.subagentTools...)
	if composed.subagentReportDelivery != "" {
		cfg.SubagentReportDelivery = composed.subagentReportDelivery
	}
	if composed.e2b != nil {
		config := *composed.e2b
		cfg.E2B = &config
	}
	if composed.exaSearch != nil {
		cfg.ExaSearch = *composed.exaSearch
	}
	if composed.deepSeekWebSearch != nil {
		cfg.DeepSeekWebSearch = *composed.deepSeekWebSearch
	}
	if composed.perplexitySearch != nil {
		cfg.PerplexitySearch = *composed.perplexitySearch
	}
	webTools := harness.DefaultWebToolConfig()
	webTools.SearchEnabled = false
	if composed.webTools != nil {
		webTools = *composed.webTools
	}
	cfg.WebTools = &webTools
	timeoutPolicyEnabled := composed.pluginEnabled("@deepseek-ai/dsh-tool-call-timeout-policy")
	cfg.ToolTimeoutPolicyEnabled = &timeoutPolicyEnabled
	if composed.todoAllowParallel != nil {
		value := *composed.todoAllowParallel
		cfg.TodoAllowParallelInProgress = &value
	}
	if composed.httpFetchConfig != nil {
		cfg.HTTPWebFetch = *composed.httpFetchConfig
	}
	for _, name := range cfg.ClientPlugins {
		if name == "@deepseek-ai/dsh-schedule" {
			cfg.ScheduleEnabled = true
			break
		}
	}
	if composed.surface() == "web" {
		cfg.ClientPlugins = append(cfg.ClientPlugins, "@deepseek-ai/dsh-client-ui-directory-picker-native")
	}
	cfg.PresetDir = loader.presetsDir()
	cfg.BundledBadgeSkill = composed.enabled("skill-badge")
	cfg.Persist = composed.persist || composed.enabled("session-persistence-jsonl")
	cfg.SessionStore = composed.sessionStore
	cfg.Storage = composed.storage
	if composed.fileReference != nil {
		cfg.FileReference = *composed.fileReference
	}
	if composed.agentTeams != nil {
		config := *composed.agentTeams
		cfg.AgentTeams = &config
	}
	if composed.sessionTitleLLM != nil {
		cfg.SessionTitleLLM = *composed.sessionTitleLLM
	} else {
		cfg.SessionTitleLLM.Enabled = composed.enabled("session-title-llm")
	}
	cfg.Compaction.Disabled = !composed.enabled("compaction-basic")
	cfg.Spill.Disabled = !composed.enabled("spill-policy")
	cfg.ToolResultPruner.Disabled = !composed.enabled("tool-result-pruner")
	cfg.RepeatToolReminder.Disabled = !composed.enabled("repeat-tool-reminder")
	if os.Getenv("DSH_PROVIDER") == "" {
		if provider, ok := composed.configString("agent-default-model", "provider"); ok {
			cfg.Provider = provider
		}
	}
	if os.Getenv("DSH_MODEL") == "" {
		if model, ok := composed.configString("agent-default-model", "model"); ok {
			cfg.Model = model
		}
	}
	if host, ok := composed.configString("webserver", "host"); ok {
		cfg.Host = host
	}
	if port, ok := composed.configInt("webserver", "port"); ok {
		cfg.Port = port
	}
	if mode, ok := composed.configString("tools", "mode"); ok {
		cfg.ToolPresentation = mode
	}
	if persona, ok := composed.configString("system-prompt", "persona"); ok {
		cfg.Persona = persona
	}
	if max, ok := composed.configInt("agent-instructions", "maxBytes"); ok {
		cfg.InstructionMaxBytes = max
	}
	if value, ok := composed.configInt("session-title", "fallbackMaxWords"); ok {
		cfg.SessionTitle.FallbackMaxWords = value
	}
	if value, ok := composed.configInt("session-title", "fallbackMaxBytes"); ok {
		cfg.SessionTitle.FallbackMaxBytes = value
	}
	if value, ok := composed.configInt("session-title", "maxTitleBytes"); ok {
		cfg.SessionTitle.MaxTitleBytes = value
	}
	if value, ok := composed.configInt("session-title-llm", "targetWords"); ok {
		cfg.SessionTitleLLM.TargetWords = value
	}
	if value, ok := composed.configInt("session-title-llm", "targetCjkCharacters"); ok {
		cfg.SessionTitleLLM.TargetCJKCharacters = value
	}
	if value, ok := composed.configInt("session-title-llm", "maxInputBytes"); ok {
		cfg.SessionTitleLLM.MaxInputBytes = value
	}
	if value, ok := composed.configInt("session-title-llm", "maxOutputTokens"); ok {
		cfg.SessionTitleLLM.MaxOutputTokens = value
	}
	if value, ok := composed.configInt("session-title-llm", "timeoutMs"); ok {
		cfg.SessionTitleLLM.Timeout = time.Duration(value) * time.Millisecond
	}
	if value, ok := composed.configInt("spill-policy", "maxInlineBytes"); ok {
		cfg.Spill.MaxInlineBytes = value
	}
	if value, ok := composed.configInt("tool-result-pruner", "thresholdChars"); ok {
		cfg.ToolResultPruner.ThresholdChars = value
	}
	if value, ok := composed.configInt("tool-result-pruner", "headChars"); ok {
		cfg.ToolResultPruner.HeadChars = value
	}
	if value, ok := composed.configInt("tool-result-pruner", "tailChars"); ok {
		cfg.ToolResultPruner.TailChars = value
	}
	if values, ok := composed.configInts("repeat-tool-reminder", "thresholds"); ok {
		cfg.RepeatToolReminder.Thresholds = values
	}
	if values, ok := composed.configStrings("repeat-tool-reminder", "include"); ok {
		cfg.RepeatToolReminder.Include = values
	}
	if values, ok := composed.configStrings("repeat-tool-reminder", "exclude"); ok {
		cfg.RepeatToolReminder.Exclude = values
	}
	if value, ok := composed.configInt("repeat-tool-reminder", "argumentsPreviewChars"); ok {
		cfg.RepeatToolReminder.ArgumentsPreviewChars = value
	}
	if composed.enabled("session-reference") {
		if value, ok := composed.configInt("session-reference", "maxReferences"); ok {
			cfg.SessionReference.MaxReferences = value
		}
		if value, ok := composed.configInt("session-reference", "candidateLimit"); ok {
			cfg.SessionReference.CandidateLimit = value
		}
		if value, ok := composed.configInt("session-reference", "maxReferenceBytes"); ok {
			cfg.SessionReference.MaxReferenceBytes = value
		}
	}
	if composed.enabled("session-projection-cache") {
		cache := &harness.SessionProjectionCacheConfig{}
		if value, ok := composed.configInt("session-projection-cache", "writeEveryEvents"); ok {
			cache.WriteEveryEvents = value
		}
		if value, ok := composed.configInt("session-projection-cache", "writeIntervalMs"); ok {
			cache.WriteInterval = time.Duration(value) * time.Millisecond
		}
		cfg.SessionProjectionCache = cache
	}
	if composed.enabled("time-context") {
		contextConfig := &harness.TimeContextConfig{}
		if value, ok := composed.configString("time-context", "timeZone"); ok {
			contextConfig.TimeZone = value
		}
		if value, ok := composed.configInt("time-context", "refreshIntervalMs"); ok {
			contextConfig.RefreshInterval = time.Duration(value) * time.Millisecond
		}
		cfg.TimeContext = contextConfig
	}
	if composed.enabled("tmux-context") {
		contextConfig := &harness.TmuxContextConfig{}
		if value, ok := composed.configInt("tmux-context", "refreshIntervalMs"); ok {
			contextConfig.RefreshInterval = time.Duration(value) * time.Millisecond
		}
		cfg.TmuxContext = contextConfig
	}
	if composed.webSearchSet {
		cfg.WebSearchProvider = composed.webSearchProvider
	} else if value, ok := composed.configString("web", "searchProvider"); ok {
		cfg.WebSearchProvider = value
	}
	if composed.webFetchSet {
		cfg.WebFetchProvider = composed.webFetchProvider
	} else if value, ok := composed.configString("web", "fetchProvider"); ok {
		cfg.WebFetchProvider = value
	}
	return cfg
}

func parsePort(value string) (int, error) {
	if value == "" || strings.IndexFunc(value, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return 0, fmt.Errorf("error: --port must be a number, got %q", value)
	}
	port, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("error: --port must be a number, got %q", value)
	}
	return port, nil
}

func runHeadless(args []string, composed *composition, cfg harness.Config, stdout, stderr io.Writer) error {
	positionals := make([]string, 0, len(args))
	options := true
	for _, argument := range args {
		if options && argument == "--" {
			options = false
			continue
		}
		if options && (argument == "-h" || argument == "--help") {
			printHeadlessHelp(stdout)
			return nil
		}
		if options && strings.HasPrefix(argument, "-") {
			return fmt.Errorf("error: unknown option %q", argument)
		}
		positionals = append(positionals, argument)
	}
	task := strings.Join(positionals, " ")
	if strings.TrimSpace(task) == "" {
		return errors.New("error: a task is required, for example: dsh --profile headless \"run the tests\"")
	}
	e, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		return err
	}
	profileRuntime, err := mountProfileRuntimePlugins(e, composed, args)
	if err != nil {
		_ = e.Close()
		return err
	}
	if profileRuntime != nil {
		if code, err := profileRuntime.flush(stdout, stderr); err != nil {
			_ = e.Close()
			return err
		} else if code != nil {
			_ = e.Close()
			return profileExit(*code)
		}
	}
	signals := newProcessSignals()
	defer signals.stop()
	defer e.Close()
	runContext, cancelRun := context.WithCancel(signals)
	var profilePump sync.WaitGroup
	profileResults := make(chan profileRuntimeResult, 1)
	if profileRuntime != nil {
		profilePump.Add(1)
		go func() {
			defer profilePump.Done()
			if result, ok := profileRuntime.pump(runContext, stdout, stderr); ok {
				profileResults <- result
				cancelRun()
			}
		}()
	}
	defer func() {
		cancelRun()
		profilePump.Wait()
	}()
	stopProfile := func() {
		cancelRun()
		profilePump.Wait()
	}
	profileOutcome := func() (profileRuntimeResult, bool) {
		select {
		case result := <-profileResults:
			return result, true
		default:
			return profileRuntimeResult{}, false
		}
	}
	resultError := func(result profileRuntimeResult) error {
		if result.code != nil {
			return profileExit(*result.code)
		}
		return result.err
	}
	id, err := e.CreateSession(runContext, cfg.Workspace, "", "")
	if err != nil {
		stopProfile()
		if result, ok := profileOutcome(); ok {
			return resultError(result)
		}
		return err
	}
	text, err := e.Run(runContext, id, harness.PromptRequest{
		Mode:    "queue",
		Content: []harness.PromptContentPart{{Type: "text", Text: task}},
		Literal: true,
	})
	stopProfile()
	if result, ok := profileOutcome(); ok {
		return resultError(result)
	}
	if err != nil && errors.Is(err, context.Canceled) && signals.code.Load() >= 0 {
		return signals.translate(err)
	}
	fmt.Fprintln(stdout, text)
	kind, code, message := headlessOutcome(e, id)
	if kind == "" && err != nil {
		return err
	}
	if kind == "error" {
		fmt.Fprintf(stderr, "dsh: %s: %s\n", code, message)
	}
	if kind != "completed" {
		return exitError{code: 1}
	}
	return nil
}

func headlessOutcome(e *harness.Engine, id string) (kind, code, message string) {
	history, _, err := e.History(id, -1, 50)
	if err != nil {
		return "", "", ""
	}
	for index := len(history) - 1; index >= 0; index-- {
		if history[index].Event.Type != "turn/end" {
			continue
		}
		data, _ := history[index].Event.Data.(map[string]any)
		reason, _ := data["reason"].(map[string]any)
		kind, _ = reason["kind"].(string)
		failure, _ := reason["error"].(map[string]any)
		code, _ = failure["code"].(string)
		message, _ = failure["message"].(string)
		return kind, code, message
	}
	return "", "", ""
}

func printHelp(w io.Writer) {
	fmt.Fprint(w, `dsh: boot a DeepSeek Harness profile.

Usage:
  dsh --profile <name> [--patch FILE] [APP ARGS...]
  dsh web [--patch FILE] [APP ARGS...]
  dsh plugin --profile <name> <pnpm args...>

Launcher options:
  --profile <name>         profile under $DSH_HOME/profiles
  --patch <path>           overlay patch list (repeatable)
  --dump-config            print bundles plus user and overlay layers
  --dump-default-config    print bundle layers only
  -V, --version            output the version number
  -h, --help               show this help

Examples:
  dsh --profile web
  dsh --profile headless "run the tests"
  dsh web --port 8080
  dsh plugin --profile tui add <package>
`)
}

func printWebHelp(w io.Writer) {
	fmt.Fprint(w, `Usage: dsh --profile web [options]

Serve the DeepSeek Harness browser UI.

Options:
  --host <host>                     bind host
  --no-open                         do not open the Web UI in the default browser
  --port <port>                     listen port; pass 0 to pick a free one
  --trusted-host <authority...>     extra accepted browser authority
  -h, --help                        show this help
`)
}

func printHeadlessHelp(w io.Writer) {
	fmt.Fprint(w, `Usage: dsh --profile headless [task...]

Answer one task, print the final assistant message, and exit.

Arguments:
  task                              task text; multiple words are joined

Options:
  -h, --help                        show this help
`)
}

func printSDKHelp(w io.Writer, profile string) {
	fmt.Fprintf(w, `Usage: dsh --profile %s [options]

Serve DeepSeek Harness SDK clients over stdio JSON-RPC.

Options:
  -h, --help                        show this help

Example:
  dsh --profile %s                 serve one SDK runtime until its client disconnects
`, profile, profile)
}
