package harness

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOpenInAppCatalogMatchesTargetOrder(t *testing.T) {
	ids := make([]string, 0, len(openInAppCatalog))
	for _, entry := range openInAppCatalog {
		ids = append(ids, entry.id)
	}
	want := []string{
		"finder", "explorer", "filemanager", "cursor", "vscode", "vscodeinsiders", "windsurf", "zed", "sublimetext", "xcode",
		"androidstudio", "intellij", "pycharm", "webstorm", "phpstorm", "goland", "rider", "rustrover", "fork", "sourcetree",
		"github", "tower", "gitkraken", "smartgit", "sublimemerge", "ghostty", "warp", "iterm", "kitty", "terminal",
		"windowsterminal", "gitbash", "gnometerminal", "konsole",
	}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("app catalog = %#v", ids)
	}
}

func TestOpenInAppResolvesMacBundlesAndXcode(t *testing.T) {
	root := t.TempDir()
	cursor := filepath.Join(root, "Cursor.app")
	if err := os.MkdirAll(cursor, 0o755); err != nil {
		t.Fatal(err)
	}
	internals := completedOpenInAppInternals(openInAppInternals{
		platform: "darwin", applicationRoots: []string{root}, home: root, env: map[string]string{},
		resolveExecutable: func(string) (string, error) { return "", os.ErrNotExist },
		run: func(_ context.Context, command string, _ []string, _ map[string]string) ([]byte, error) {
			if command == "xcode-select" {
				return []byte(filepath.Join(root, "Xcode-beta.app", "Contents", "Developer") + "\n"), nil
			}
			return nil, errors.New("unexpected command")
		},
	})
	if err := os.MkdirAll(filepath.Join(root, "Xcode-beta.app", "Contents", "Developer"), 0o755); err != nil {
		t.Fatal(err)
	}
	registry := openInAppRegistryView{appPaths: map[string]string{}}
	app, _ := openInAppCatalogEntry("cursor")
	resolved := resolveOpenInAppLaunch(app, time.Second, internals, &registry)
	if resolved == nil || resolved.launch.command != "open" || !reflect.DeepEqual(resolved.launch.args, []string{"-a", cursor}) || resolved.icon == nil || resolved.icon.path != cursor {
		t.Fatalf("cursor resolution = %s", openInAppResolutionString(resolved))
	}
	app, _ = openInAppCatalogEntry("xcode")
	resolved = resolveOpenInAppLaunch(app, time.Second, internals, &registry)
	if resolved == nil || resolved.launch.command != "xed" || resolved.fallbackLaunch == nil || resolved.fallbackLaunch.command != "open" {
		t.Fatalf("xcode resolution = %#v", resolved)
	}
}

func TestOpenInAppWindowsRegistryAndVersionedLocators(t *testing.T) {
	root := t.TempDir()
	code := filepath.Join(root, "Code.exe")
	git := filepath.Join(root, "Git")
	github := filepath.Join(root, "GitHubDesktop", "app-3.3.6")
	for _, directory := range []string{git, filepath.Join(github, "resources", "app"), filepath.Join(root, "JetBrains", "PyCharm 2024.1.10", "bin"), filepath.Join(root, "JetBrains", "PyCharm 2024.1.9", "bin")} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []string{code, filepath.Join(git, "git-bash.exe"), filepath.Join(github, "GitHubDesktop.exe"), filepath.Join(github, "resources", "app", "cli.js"), filepath.Join(root, "JetBrains", "PyCharm 2024.1.10", "bin", "pycharm64.exe"), filepath.Join(root, "JetBrains", "PyCharm 2024.1.9", "bin", "pycharm64.exe")} {
		if err := os.WriteFile(file, []byte("fixture"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	dump := strings.Join([]string{
		`HKEY_CURRENT_USER\Software\Microsoft\Windows\CurrentVersion\App Paths\Code.exe`,
		`    (默认)    REG_EXPAND_SZ    %ROOT%/Code.exe`,
		`HKEY_LOCAL_MACHINE\Software\Microsoft\Windows\CurrentVersion\Uninstall\Git`,
		`    DisplayName    REG_SZ    Git version 2.44.0`,
		`    InstallLocation    REG_SZ    %ROOT%/Git`,
	}, "\r\n")
	parsed := parseOpenInAppRegistryDump(dump)
	if parsed[`HKEY_CURRENT_USER\Software\Microsoft\Windows\CurrentVersion\App Paths\Code.exe`]["(Default)"] != `%ROOT%/Code.exe` {
		t.Fatalf("registry parse = %#v", parsed)
	}
	internals := completedOpenInAppInternals(openInAppInternals{platform: "windows", home: root, env: map[string]string{"ROOT": root, "ProgramFiles": root, "LOCALAPPDATA": root}, resolveExecutable: func(string) (string, error) { return "", os.ErrNotExist }})
	view := openInAppRegistryView{
		appPaths:       map[string]string{"code.exe": code},
		installRecords: []openInAppInstallRecord{{displayName: "Git version 2.44.0", installLocation: git}},
	}
	cases := []struct {
		id      string
		command string
	}{
		{id: "vscode", command: code},
		{id: "gitbash", command: filepath.Join(git, "git-bash.exe")},
		{id: "pycharm", command: filepath.Join(root, "JetBrains", "PyCharm 2024.1.10", "bin", "pycharm64.exe")},
		{id: "github", command: filepath.Join(github, "GitHubDesktop.exe")},
	}
	for _, test := range cases {
		app, _ := openInAppCatalogEntry(test.id)
		resolved := resolveOpenInAppLaunch(app, time.Second, internals, &view)
		if resolved == nil || resolved.launch.command != test.command || resolved.icon == nil || resolved.icon.path != test.command {
			t.Fatalf("%s resolution = %#v", test.id, resolved)
		}
		if test.id == "github" && (!resolved.launch.windowsHide || resolved.launch.env["ELECTRON_RUN_AS_NODE"] != "1") {
			t.Fatalf("github launch = %#v", resolved.launch)
		}
	}
}

func TestOpenInAppLinuxDesktopFallbackAndIcons(t *testing.T) {
	home := t.TempDir()
	data := filepath.Join(home, "data")
	applications := filepath.Join(data, "applications")
	launcher := filepath.Join(home, "bin", "kitty")
	icon := filepath.Join(data, "icons", "hicolor", "256x256", "apps", "kitty.svg")
	for _, directory := range []string{applications, filepath.Dir(launcher), filepath.Dir(icon)} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(launcher, []byte("bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(icon, []byte("<svg/>"), 0o644); err != nil {
		t.Fatal(err)
	}
	entry := "[Desktop Action ignored]\nExec=wrong\n[Desktop Entry]\nTryExec=" + launcher + "\nExec=kitty %U\nIcon=kitty\n"
	if err := os.WriteFile(filepath.Join(applications, "kitty.desktop"), []byte(entry), 0o644); err != nil {
		t.Fatal(err)
	}
	internals := completedOpenInAppInternals(openInAppInternals{
		platform: "linux", home: home, env: map[string]string{"XDG_DATA_HOME": data, "XDG_DATA_DIRS": "", "DISPLAY": ":1"},
		resolveExecutable: func(string) (string, error) { return "", os.ErrNotExist },
	})
	parsed := parseOpenInAppDesktopEntry(entry)
	if parsed.tryExec != launcher || parsed.exec != "kitty %U" || parsed.icon != "kitty" || openInAppExecCommand(`"/opt/App Name/bin" --flag`) != "/opt/App Name/bin" {
		t.Fatalf("desktop parse = %#v", parsed)
	}
	app, _ := openInAppCatalogEntry("kitty")
	registry := openInAppRegistryView{appPaths: map[string]string{}}
	resolved := resolveOpenInAppLaunch(app, time.Second, internals, &registry)
	if resolved == nil || resolved.launch.command != launcher || !reflect.DeepEqual(resolved.launch.args, []string{"--directory"}) {
		t.Fatalf("kitty resolution = %#v", resolved)
	}
	extracted := extractOpenInAppIcon(app, resolved, time.Second, internals)
	if extracted == nil || extracted.contentType != "image/svg+xml" || string(extracted.bytes) != "<svg/>" {
		t.Fatalf("kitty icon = %#v", extracted)
	}
}

func TestOpenInAppMacAndWindowsIconExtraction(t *testing.T) {
	t.Run("mac bundle", func(t *testing.T) {
		bundle := filepath.Join(t.TempDir(), "Cursor.app")
		resources := filepath.Join(bundle, "Contents", "Resources")
		if err := os.MkdirAll(resources, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(resources, "CursorIcon.icns"), []byte("icns"), 0o644); err != nil {
			t.Fatal(err)
		}
		internals := completedOpenInAppInternals(openInAppInternals{
			platform: "darwin", home: t.TempDir(), env: map[string]string{}, resolveExecutable: func(string) (string, error) { return "", os.ErrNotExist },
			run: func(_ context.Context, command string, args []string, _ map[string]string) ([]byte, error) {
				switch command {
				case "plutil":
					return []byte(`{"CFBundleIconFile":"CursorIcon"}`), nil
				case "sips":
					return nil, os.WriteFile(args[len(args)-1], []byte("png-mac"), 0o644)
				default:
					return nil, errors.New("unexpected command")
				}
			},
		})
		app, _ := openInAppCatalogEntry("cursor")
		resolved := &openInAppResolvedLaunch{icon: &openInAppIconSource{kind: "app-bundle", path: bundle}}
		icon := extractOpenInAppIcon(app, resolved, time.Second, internals)
		if icon == nil || icon.contentType != "image/png" || string(icon.bytes) != "png-mac" {
			t.Fatalf("mac icon = %#v", icon)
		}
	})

	t.Run("windows executable", func(t *testing.T) {
		executable := filepath.Join(t.TempDir(), "Code.exe")
		if err := os.WriteFile(executable, []byte("exe"), 0o644); err != nil {
			t.Fatal(err)
		}
		internals := completedOpenInAppInternals(openInAppInternals{
			platform: "windows", home: t.TempDir(), env: map[string]string{}, resolveExecutable: func(string) (string, error) { return "", os.ErrNotExist },
			run: func(_ context.Context, command string, args []string, _ map[string]string) ([]byte, error) {
				if command != "powershell.exe" || len(args) < 2 {
					return nil, errors.New("unexpected command")
				}
				return nil, os.WriteFile(args[len(args)-1], []byte("png-windows"), 0o644)
			},
		})
		app, _ := openInAppCatalogEntry("vscode")
		resolved := &openInAppResolvedLaunch{icon: &openInAppIconSource{kind: "executable", path: executable}}
		icon := extractOpenInAppIcon(app, resolved, time.Second, internals)
		if icon == nil || icon.contentType != "image/png" || string(icon.bytes) != "png-windows" {
			t.Fatalf("Windows icon = %#v", icon)
		}
	})
}

func TestOpenInAppLaunchSubstitutionFallbackAndShellOpen(t *testing.T) {
	var mu sync.Mutex
	var calls [][]string
	launcher := func(_ context.Context, command string, args []string, _ time.Duration, _ map[string]string, _ bool) error {
		mu.Lock()
		calls = append(calls, append([]string{command}, args...))
		mu.Unlock()
		if command == "primary" {
			return errors.New("early failure")
		}
		return nil
	}
	internals := openInAppInternals{platform: "linux", env: map[string]string{"DISPLAY": ":1"}, home: t.TempDir(), launch: launcher, resolveExecutable: func(name string) (string, error) { return name, nil }}
	fallback := openInAppLaunch{kind: "argv", command: "fallback", args: []string{"--cd=" + openInAppPathToken}}
	resolved := &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "argv", command: "primary"}, fallbackLaunch: &fallback}
	if outcome := launchResolvedOpenInApp(context.Background(), resolved, "/w/dir", time.Second, internals); outcome != openInAppLaunched {
		t.Fatalf("fallback outcome = %s", outcome)
	}
	if want := [][]string{{"primary", "/w/dir"}, {"fallback", "--cd=/w/dir"}}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("launches = %#v", calls)
	}
	var shell []string
	internals.run = func(_ context.Context, command string, args []string, _ map[string]string) ([]byte, error) {
		shell = append([]string{command}, args...)
		return nil, nil
	}
	outcome := launchResolvedOpenInApp(context.Background(), &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "shell-open"}}, "/w/dir", time.Second, internals)
	if outcome != openInAppLaunched || !reflect.DeepEqual(shell, []string{"xdg-open", "/w/dir"}) {
		t.Fatalf("shell open = %s %#v", outcome, shell)
	}
}

func TestOpenInAppRoutesCacheIconsAndRefreshMissingLaunch(t *testing.T) {
	e := newIntegrationEngine(t)
	workspace := t.TempDir()
	data := filepath.Join(t.TempDir(), "data")
	applications := filepath.Join(data, "applications")
	iconPath := filepath.Join(data, "icons", "hicolor", "128x128", "apps", "code.png")
	if err := os.MkdirAll(applications, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(iconPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(applications, "code.desktop"), []byte("[Desktop Entry]\nExec=code\nIcon=code\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(iconPath, []byte("png"), 0o644); err != nil {
		t.Fatal(err)
	}
	resolveCount := 0
	launchCount := 0
	internals := openInAppInternals{
		platform: "linux", home: t.TempDir(), env: map[string]string{"DISPLAY": ":1", "XDG_DATA_HOME": data},
		resolveExecutable: func(name string) (string, error) {
			if name != "code" {
				return "", os.ErrNotExist
			}
			resolveCount++
			if resolveCount > 1 {
				return "", os.ErrNotExist
			}
			return "/fixture/code", nil
		},
		launch: func(_ context.Context, _ string, _ []string, _ time.Duration, _ map[string]string, _ bool) error {
			launchCount++
			return os.ErrNotExist
		},
	}
	e.openInApp = &openInAppRuntime{internals: internals}
	handler := e.Handler()
	request := httptest.NewRequest(http.MethodGet, "http://localhost/open-in-app/apps", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"vscode"`) || resolveCount != 1 {
		t.Fatalf("apps = %d %s resolves=%d", response.Code, response.Body.String(), resolveCount)
	}
	for range 2 {
		request = httptest.NewRequest(http.MethodGet, "http://localhost/open-in-app/icon/vscode", nil)
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "image/png" || response.Body.String() != "png" {
			t.Fatalf("icon = %d %s", response.Code, response.Body.String())
		}
	}
	request = httptest.NewRequest(http.MethodPost, "http://localhost/open-in-app/open", strings.NewReader(`{"app":"vscode","path":"`+workspace+`"}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadGateway || launchCount != 1 || resolveCount != 2 {
		t.Fatalf("stale open = %d %s launches=%d resolves=%d", response.Code, response.Body.String(), launchCount, resolveCount)
	}
	request = httptest.NewRequest(http.MethodGet, "http://localhost/open-in-app/apps", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if strings.Contains(response.Body.String(), `"vscode"`) {
		t.Fatalf("refreshed apps = %s", response.Body.String())
	}
}

func TestOpenInAppRoutesValidateTrustMethodsAndBodies(t *testing.T) {
	e := newIntegrationEngine(t)
	e.openInApp = &openInAppRuntime{internals: openInAppInternals{platform: "plan9", env: map[string]string{}, home: t.TempDir(), resolveExecutable: func(string) (string, error) { return "", os.ErrNotExist }}}
	handler := e.Handler()
	request := httptest.NewRequest(http.MethodGet, "http://localhost/open-in-app/apps", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "{\"apps\":[]}\n" {
		t.Fatalf("apps = %d %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "http://localhost/open-in-app/open", strings.NewReader(`{"app":"missing","path":"relative"}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("bad open = %d %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "http://localhost/open-in-app/open", strings.NewReader(`{}`))
	request.Header.Set("Origin", "http://evil.example")
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("untrusted open = %d", response.Code)
	}
}
