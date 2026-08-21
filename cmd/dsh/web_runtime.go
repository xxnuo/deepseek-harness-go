package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	harness "github.com/xxnuo/deepseek-harness-go"
)

const profileWatchInterval = 100 * time.Millisecond

type webProfileWatch struct {
	paths *profileWatchPaths
	load  func() (*composition, harness.Config, error)
}

type profileWatchPaths struct {
	mu      sync.RWMutex
	base    []string
	runtime []string
}

func newProfileWatchPaths(paths ...string) *profileWatchPaths {
	return &profileWatchPaths{base: append([]string(nil), paths...)}
}

func (paths *profileWatchPaths) setRuntime(runtime *profileRuntimeMount) {
	paths.mu.Lock()
	paths.runtime = nil
	if runtime != nil {
		paths.runtime = append(paths.runtime, runtime.watchPaths...)
	}
	paths.mu.Unlock()
}

func (paths *profileWatchPaths) snapshot() []string {
	paths.mu.RLock()
	result := make([]string, 0, len(paths.base)+len(paths.runtime))
	result = append(result, paths.base...)
	result = append(result, paths.runtime...)
	paths.mu.RUnlock()
	return result
}

type webOptions struct {
	help         bool
	host         string
	hostSet      bool
	openBrowser  bool
	port         int
	portSet      bool
	trustedHosts []string
}

func parseWebOptions(args []string) (webOptions, error) {
	options := webOptions{openBrowser: true}
	for index := 0; index < len(args); {
		argument := args[index]
		switch {
		case argument == "-h" || argument == "--help":
			options.help = true
			return options, nil
		case argument == "--host":
			if index+1 >= len(args) {
				return webOptions{}, errors.New("error: option '--host <host>' argument missing")
			}
			options.host, options.hostSet = args[index+1], true
			index += 2
		case strings.HasPrefix(argument, "--host="):
			options.host, options.hostSet = strings.TrimPrefix(argument, "--host="), true
			index++
		case argument == "--no-open":
			options.openBrowser = false
			index++
		case argument == "--port":
			if index+1 >= len(args) {
				return webOptions{}, errors.New("error: option '--port <port>' argument missing")
			}
			port, err := parsePort(args[index+1])
			if err != nil {
				return webOptions{}, err
			}
			options.port, options.portSet = port, true
			index += 2
		case strings.HasPrefix(argument, "--port="):
			port, err := parsePort(strings.TrimPrefix(argument, "--port="))
			if err != nil {
				return webOptions{}, err
			}
			options.port, options.portSet = port, true
			index++
		case argument == "--trusted-host":
			index++
			start := index
			for index < len(args) && !strings.HasPrefix(args[index], "-") {
				options.trustedHosts = append(options.trustedHosts, args[index])
				index++
			}
			if index == start {
				return webOptions{}, errors.New("error: option '--trusted-host <authority...>' argument missing")
			}
		case strings.HasPrefix(argument, "--trusted-host="):
			authority := strings.TrimPrefix(argument, "--trusted-host=")
			if authority == "" {
				return webOptions{}, errors.New("error: option '--trusted-host <authority...>' argument missing")
			}
			options.trustedHosts = append(options.trustedHosts, authority)
			index++
		default:
			return webOptions{}, fmt.Errorf("error: unknown option %q", argument)
		}
	}
	return options, nil
}

func (options webOptions) apply(cfg harness.Config) (harness.Config, error) {
	if options.hostSet {
		if options.host == "0.0.0.0" {
			return harness.Config{}, errors.New("error: --host 0.0.0.0 is intentionally not supported yet for safety: it would expose remote code execution to the network; use 127.0.0.1 instead")
		}
		cfg.Host = options.host
	}
	if options.portSet {
		cfg.Port = options.port
	}
	cfg.TrustedHosts = append(cfg.TrustedHosts, options.trustedHosts...)
	return cfg, nil
}

type webGeneration struct {
	engine         *harness.Engine
	handler        http.Handler
	profileRuntime *profileRuntimeMount
	ctx            context.Context
	cancel         context.CancelFunc
	active         sync.WaitGroup
	profile        sync.WaitGroup
}

func newWebGeneration(engine *harness.Engine, profileRuntime *profileRuntimeMount) *webGeneration {
	ctx, cancel := context.WithCancel(context.Background())
	return &webGeneration{engine: engine, handler: engine.Handler(), profileRuntime: profileRuntime, ctx: ctx, cancel: cancel}
}

type webProfileResult struct {
	generation *webGeneration
	result     profileRuntimeResult
}

type reloadableWebServer struct {
	mu             sync.RWMutex
	server         *http.Server
	generation     *webGeneration
	listener       net.Listener
	configuredAddr string
	serveErrors    chan error
	profileResults chan webProfileResult
	retired        sync.WaitGroup
	closeOnce      sync.Once
	closeErr       error
	stdout         io.Writer
	stderr         io.Writer
}

func newReloadableWebServer(engine *harness.Engine, profileRuntime *profileRuntimeMount, cfg harness.Config, stdout, stderr io.Writer) (*reloadableWebServer, error) {
	configuredAddr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	listener, err := net.Listen("tcp", configuredAddr)
	if err != nil {
		return nil, err
	}
	engine.SetWebServerListener(listener)
	runtime := &reloadableWebServer{
		listener: listener, configuredAddr: configuredAddr,
		serveErrors: make(chan error, 1), profileResults: make(chan webProfileResult, 1), stdout: stdout, stderr: stderr,
	}
	runtime.generation = newWebGeneration(engine, profileRuntime)
	runtime.server = &http.Server{Handler: runtime, ReadHeaderTimeout: 5 * time.Second}
	runtime.serve(listener)
	runtime.printAddress(listener, cfg.Host)
	runtime.startProfile(runtime.generation)
	return runtime, nil
}

func (runtime *reloadableWebServer) startProfile(generation *webGeneration) {
	if generation == nil || generation.profileRuntime == nil {
		return
	}
	generation.profile.Add(1)
	go func() {
		defer generation.profile.Done()
		result, ok := generation.profileRuntime.pump(generation.ctx, runtime.stdout, runtime.stderr)
		if ok {
			select {
			case runtime.profileResults <- webProfileResult{generation: generation, result: result}:
			case <-generation.ctx.Done():
			}
		}
	}()
}

func (runtime *reloadableWebServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	runtime.mu.RLock()
	generation := runtime.generation
	if generation != nil {
		generation.active.Add(1)
	}
	runtime.mu.RUnlock()
	if generation == nil {
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return
	}
	defer generation.active.Done()
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(generation.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()
	generation.handler.ServeHTTP(w, r.WithContext(ctx))
}

func (runtime *reloadableWebServer) serve(listener net.Listener) {
	go func() {
		if err := runtime.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			select {
			case runtime.serveErrors <- err:
			default:
			}
		}
	}()
}

func (runtime *reloadableWebServer) replace(engine *harness.Engine, profileRuntime *profileRuntimeMount, cfg harness.Config) error {
	configuredAddr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	runtime.mu.RLock()
	addressChanged := configuredAddr != runtime.configuredAddr
	runtime.mu.RUnlock()
	var nextListener net.Listener
	var err error
	if addressChanged {
		nextListener, err = net.Listen("tcp", configuredAddr)
		if err != nil {
			_ = engine.Close()
			return err
		}
	}
	runtime.mu.RLock()
	currentListener := runtime.listener
	runtime.mu.RUnlock()
	next := newWebGeneration(engine, profileRuntime)
	if nextListener != nil {
		engine.SetWebServerListener(nextListener)
	} else {
		engine.SetWebServerListener(currentListener)
	}
	runtime.mu.Lock()
	previous := runtime.generation
	previousListener := runtime.listener
	runtime.generation = next
	if nextListener != nil {
		runtime.listener = nextListener
		runtime.configuredAddr = configuredAddr
	}
	runtime.mu.Unlock()
	runtime.startProfile(next)
	if nextListener != nil {
		runtime.serve(nextListener)
		_ = previousListener.Close()
		runtime.printAddress(nextListener, cfg.Host)
	}
	runtime.retire(previous)
	return nil
}

func (runtime *reloadableWebServer) retire(generation *webGeneration) {
	if generation == nil {
		return
	}
	generation.cancel()
	runtime.retired.Add(1)
	go func() {
		defer runtime.retired.Done()
		generation.active.Wait()
		generation.profile.Wait()
		_ = generation.engine.Close()
	}()
}

func (runtime *reloadableWebServer) printAddress(listener net.Listener, host string) {
	port := listener.Addr().(*net.TCPAddr).Port
	fmt.Fprintf(runtime.stdout, "dsh web: http://127.0.0.1:%d", port)
	if host == "0.0.0.0" {
		if addresses := harness.LANIPv4Addresses(); len(addresses) > 0 {
			fmt.Fprintf(runtime.stdout, " (LAN: http://%s:%d)", addresses[0], port)
		}
	}
	fmt.Fprintln(runtime.stdout)
}

func (runtime *reloadableWebServer) close() error {
	runtime.closeOnce.Do(func() {
		runtime.mu.Lock()
		generation := runtime.generation
		runtime.generation = nil
		runtime.mu.Unlock()
		if generation != nil {
			generation.cancel()
		}
		ctx, cancel := context.WithTimeout(context.Background(), processShutdownTimeout)
		defer cancel()
		shutdownErr := runtime.server.Shutdown(ctx)
		if generation != nil {
			generation.active.Wait()
			generation.profile.Wait()
		}
		engineErr := error(nil)
		if generation != nil {
			engineErr = generation.engine.Close()
		}
		runtime.retired.Wait()
		runtime.closeErr = errors.Join(shutdownErr, engineErr)
	})
	return runtime.closeErr
}

func runWeb(args []string, composed *composition, cfg harness.Config, stdout, stderr io.Writer, watch *webProfileWatch) error {
	options, err := parseWebOptions(args)
	if err != nil {
		return err
	}
	if options.help {
		printWebHelp(stdout)
		return nil
	}
	cfg, err = options.apply(cfg)
	if err != nil {
		return err
	}
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		return err
	}
	profileRuntime, err := mountProfileRuntimePlugins(engine, composed, args)
	if err != nil {
		_ = engine.Close()
		return err
	}
	if profileRuntime != nil {
		if code, err := profileRuntime.flush(stdout, stderr); err != nil {
			_ = engine.Close()
			return err
		} else if code != nil {
			_ = engine.Close()
			return profileExit(*code)
		}
	}
	if watch != nil {
		watch.paths.setRuntime(profileRuntime)
	}
	runtime, err := newReloadableWebServer(engine, profileRuntime, cfg, stdout, stderr)
	if err != nil {
		_ = engine.Close()
		return err
	}
	if options.openBrowser && !webLaunchedThroughSSH(cfg.LaunchEnvironment) {
		runtime.openBrowser()
	}
	signals := newProcessSignals()
	watchContext, stopWatcher := context.WithCancel(signals)
	var watcher sync.WaitGroup
	if watch != nil {
		watcher.Add(1)
		go func() {
			defer watcher.Done()
			watchProfilePatches(watchContext, watch.paths, func() error {
				nextComposition, nextConfig, err := watch.load()
				if err != nil {
					return err
				}
				nextConfig, err = options.apply(nextConfig)
				if err != nil {
					return err
				}
				next, err := harness.New(harness.WithConfig(nextConfig))
				if err != nil {
					return err
				}
				profileRuntime, err := mountProfileRuntimePlugins(next, nextComposition, args)
				if err != nil {
					_ = next.Close()
					return err
				}
				if profileRuntime != nil {
					if code, err := profileRuntime.flush(stdout, stderr); err != nil {
						_ = next.Close()
						return err
					} else if code != nil {
						_ = next.Close()
						return profileExit(*code)
					}
				}
				if err := runtime.replace(next, profileRuntime, nextConfig); err != nil {
					return err
				}
				watch.paths.setRuntime(profileRuntime)
				return nil
			}, stderr)
		}()
	}
	var runErr error
	done := false
	for !done {
		select {
		case <-signals.Done():
			runErr, done = signals.Err(), true
		case err := <-runtime.serveErrors:
			runErr, done = err, true
		case outcome := <-runtime.profileResults:
			runtime.mu.RLock()
			current := runtime.generation == outcome.generation
			runtime.mu.RUnlock()
			if !current {
				continue
			}
			runErr, done = outcome.result.err, true
			if outcome.result.code != nil {
				runErr = profileExit(*outcome.result.code)
			}
		}
	}
	stopWatcher()
	watcher.Wait()
	closeErr := runtime.close()
	signals.stop()
	if runErr != nil {
		return signals.translate(runErr)
	}
	return closeErr
}

type watchedFileState struct {
	exists bool
	sum    [sha256.Size]byte
	err    string
}

func readWatchedFileState(path string) watchedFileState {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return watchedFileState{}
	}
	if err != nil {
		return watchedFileState{err: err.Error()}
	}
	return watchedFileState{exists: true, sum: sha256.Sum256(data)}
}

func watchProfilePatches(ctx context.Context, paths *profileWatchPaths, reload func() error, stderr io.Writer) {
	watchProfilePatchesReady(ctx, paths, reload, stderr, nil)
}

func watchProfilePatchesReady(ctx context.Context, paths *profileWatchPaths, reload func() error, stderr io.Writer, ready chan<- struct{}) {
	states := make(map[string]watchedFileState)
	for _, path := range paths.snapshot() {
		states[path] = readWatchedFileState(path)
	}
	if ready != nil {
		close(ready)
	}
	ticker := time.NewTicker(profileWatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			changed := false
			nextStates := make(map[string]watchedFileState)
			for _, path := range paths.snapshot() {
				next := readWatchedFileState(path)
				nextStates[path] = next
				if previous, ok := states[path]; ok && next != previous {
					changed = true
				}
			}
			states = nextStates
			if changed {
				if err := reload(); err != nil {
					fmt.Fprintf(stderr, "dsh: config reload failed: %v\n", err)
				}
			}
		}
	}
}
