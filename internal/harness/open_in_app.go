package harness

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	openInAppProbeTimeout = 5 * time.Second
	openInAppIconTimeout  = 5 * time.Second
	openInAppLaunchWatch  = time.Second
)

type openInAppIconCacheEntry struct {
	ready chan struct{}
	icon  *openInAppIcon
}

type openInAppRuntime struct {
	mu          sync.Mutex
	internals   openInAppInternals
	initialized bool
	resolutions map[string]*openInAppResolvedLaunch
	icons       map[string]*openInAppIconCacheEntry
}

func (runtime *openInAppRuntime) availability() map[string]*openInAppResolvedLaunch {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if !runtime.initialized {
		runtime.internals = completedOpenInAppInternals(runtime.internals)
		runtime.resolutions = resolveOpenInAppApps(openInAppProbeTimeout, runtime.internals)
		runtime.icons = map[string]*openInAppIconCacheEntry{}
		runtime.initialized = true
	}
	return runtime.resolutions
}

func (runtime *openInAppRuntime) availableIDs() []string {
	runtime.availability()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	apps := make([]string, 0, len(runtime.resolutions))
	for _, app := range openInAppCatalog {
		if runtime.resolutions[app.id] != nil {
			apps = append(apps, app.id)
		}
	}
	return apps
}

func (runtime *openInAppRuntime) resolution(id string) *openInAppResolvedLaunch {
	runtime.availability()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.resolutions[id]
}

func (runtime *openInAppRuntime) icon(app openInAppApp, resolved *openInAppResolvedLaunch) *openInAppIcon {
	runtime.availability()
	runtime.mu.Lock()
	if cached, ok := runtime.icons[app.id]; ok {
		runtime.mu.Unlock()
		<-cached.ready
		return cached.icon
	}
	cached := &openInAppIconCacheEntry{ready: make(chan struct{})}
	runtime.icons[app.id] = cached
	internals := runtime.internals
	runtime.mu.Unlock()
	icon := extractOpenInAppIcon(app, resolved, openInAppIconTimeout, internals)
	runtime.mu.Lock()
	cached.icon = icon
	close(cached.ready)
	runtime.mu.Unlock()
	return icon
}

func (runtime *openInAppRuntime) refresh(app openInAppApp) *openInAppResolvedLaunch {
	runtime.availability()
	runtime.mu.Lock()
	internals := runtime.internals
	runtime.mu.Unlock()
	registry := openInAppRegistryView{appPaths: map[string]string{}}
	if internals.platform == "windows" {
		registry = readOpenInAppRegistryView(openInAppProbeTimeout, internals)
	}
	fresh := resolveOpenInAppLaunch(app, openInAppProbeTimeout, internals, &registry)
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	delete(runtime.icons, app.id)
	if fresh == nil {
		delete(runtime.resolutions, app.id)
		return nil
	}
	runtime.resolutions[app.id] = fresh
	return fresh
}

func (e *Engine) openInAppRuntimeState() *openInAppRuntime {
	e.openInAppMu.Lock()
	defer e.openInAppMu.Unlock()
	if e.openInApp == nil {
		e.openInApp = &openInAppRuntime{}
	}
	return e.openInApp
}

func openInAppJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (e *Engine) openInAppApps(w http.ResponseWriter, request *http.Request) {
	if !e.allowed(request) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if request.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	apps := e.openInAppRuntimeState().availableIDs()
	openInAppJSON(w, http.StatusOK, map[string]any{"apps": apps})
}

func openInAppCatalogEntry(id string) (openInAppApp, bool) {
	for _, app := range openInAppCatalog {
		if app.id == id {
			return app, true
		}
	}
	return openInAppApp{}, false
}

func (e *Engine) openInAppIcon(w http.ResponseWriter, request *http.Request) {
	if !e.allowed(request) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if request.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(request.URL.Path, "/open-in-app/icon/")
	noIcon := func() {
		openInAppJSON(w, http.StatusNotFound, map[string]any{"code": "not-found", "message": "no icon for " + id})
	}
	app, ok := openInAppCatalogEntry(id)
	if !ok {
		noIcon()
		return
	}
	runtime := e.openInAppRuntimeState()
	resolved := runtime.resolution(id)
	if resolved == nil {
		noIcon()
		return
	}
	icon := runtime.icon(app, resolved)
	if icon == nil {
		noIcon()
		return
	}
	w.Header().Set("Content-Type", icon.contentType)
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(icon.bytes)
}

func (e *Engine) openInAppOpen(w http.ResponseWriter, request *http.Request) {
	if !e.allowed(request) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if request.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if media := strings.ToLower(strings.TrimSpace(strings.Split(request.Header.Get("Content-Type"), ";")[0])); media != "application/json" {
		openInAppJSON(w, http.StatusUnsupportedMediaType, map[string]any{"code": "unsupported-media-type", "message": "content-type must be application/json"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, 64<<10+1))
	if err != nil {
		openInAppJSON(w, http.StatusBadRequest, map[string]any{"code": "bad-request", "message": "request body unreadable"})
		return
	}
	if len(body) > 64<<10 {
		openInAppJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"code": "payload-too-large", "message": "request body is too large"})
		return
	}
	var decoded struct {
		App  *string `json:"app"`
		Path *string `json:"path"`
	}
	if json.Unmarshal(body, &decoded) != nil || decoded.App == nil || decoded.Path == nil {
		openInAppJSON(w, http.StatusBadRequest, map[string]any{"code": "bad-request", "message": "request body must be JSON with string \"app\" and \"path\""})
		return
	}
	payload := struct{ App, Path string }{App: *decoded.App, Path: *decoded.Path}
	app, known := openInAppCatalogEntry(payload.App)
	runtime := e.openInAppRuntimeState()
	resolved := runtime.resolution(payload.App)
	if !known || resolved == nil {
		openInAppJSON(w, http.StatusBadRequest, map[string]any{"code": "bad-request", "message": "unknown or unavailable app: " + payload.App})
		return
	}
	if payload.Path == "" || !filepath.IsAbs(payload.Path) {
		openInAppJSON(w, http.StatusBadRequest, map[string]any{"code": "bad-request", "message": "path must be an absolute directory path"})
		return
	}
	info, err := os.Stat(payload.Path)
	if err != nil || !info.IsDir() {
		openInAppJSON(w, http.StatusNotFound, map[string]any{"code": "not-found", "message": "directory does not exist: " + payload.Path})
		return
	}
	outcome := launchResolvedOpenInApp(request.Context(), resolved, payload.Path, openInAppLaunchWatch, runtime.internals)
	if outcome == openInAppMissing {
		fresh := runtime.refresh(app)
		if fresh == nil {
			outcome = openInAppFailed
		} else {
			outcome = launchResolvedOpenInApp(request.Context(), fresh, payload.Path, openInAppLaunchWatch, runtime.internals)
		}
	}
	if outcome != openInAppLaunched {
		openInAppJSON(w, http.StatusBadGateway, map[string]any{"code": "launch-failed", "message": "failed to launch " + app.id})
		return
	}
	openInAppJSON(w, http.StatusOK, map[string]any{"ok": true})
}
