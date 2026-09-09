package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

type openInAppIconSource struct {
	kind string
	path string
}

type openInAppResolvedLaunch struct {
	launch         openInAppLaunch
	fallbackLaunch *openInAppLaunch
	icon           *openInAppIconSource
}

type openInAppLaunchOutcome string

const (
	openInAppLaunched openInAppLaunchOutcome = "launched"
	openInAppMissing  openInAppLaunchOutcome = "missing"
	openInAppFailed   openInAppLaunchOutcome = "failed"
)

type openInAppCommandRunner func(context.Context, string, []string, map[string]string) ([]byte, error)
type openInAppLauncher func(context.Context, string, []string, time.Duration, map[string]string, bool) error
type openInAppExecutableResolver func(string) (string, error)

type openInAppInternals struct {
	platform          string
	applicationRoots  []string
	env               map[string]string
	home              string
	run               openInAppCommandRunner
	launch            openInAppLauncher
	resolveExecutable openInAppExecutableResolver
}

func defaultOpenInAppCommandRunner(ctx context.Context, command string, args []string, overlay map[string]string) ([]byte, error) {
	process := exec.CommandContext(ctx, command, args...)
	process.Env = scrubbedChildEnv(overlay)
	return process.Output()
}

func completedOpenInAppInternals(value openInAppInternals) openInAppInternals {
	if value.platform == "" {
		value.platform = runtime.GOOS
	}
	if value.home == "" {
		value.home, _ = os.UserHomeDir()
	}
	if value.applicationRoots == nil {
		value.applicationRoots = []string{"/Applications", filepath.Join(value.home, "Applications")}
	}
	if value.env == nil {
		value.env = map[string]string{}
		for _, item := range os.Environ() {
			if key, raw, ok := strings.Cut(item, "="); ok {
				value.env[key] = raw
			}
		}
	}
	if value.run == nil {
		value.run = defaultOpenInAppCommandRunner
	}
	if value.launch == nil {
		value.launch = launchDetachedOpenInApp
	}
	if value.resolveExecutable == nil {
		value.resolveExecutable = exec.LookPath
	}
	return value
}

func openInAppCommandOutput(command string, args []string, timeout time.Duration, internals openInAppInternals) string {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	output, err := internals.run(ctx, command, args, nil)
	if err != nil {
		return ""
	}
	return string(output)
}

func openInAppIsFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func openInAppIsDirectory(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

var openInAppCandidateVariable = regexp.MustCompile(`\$\{([^}]+)\}`)

func expandOpenInAppCandidate(template string, internals openInAppInternals) (string, bool) {
	valid := true
	expanded := openInAppCandidateVariable.ReplaceAllStringFunc(template, func(token string) string {
		key := strings.TrimSuffix(strings.TrimPrefix(token, "${"), "}")
		value, ok := internals.env[key]
		if !ok {
			valid = false
			return token
		}
		return value
	})
	if !valid {
		return "", false
	}
	if strings.HasPrefix(expanded, "~/") {
		expanded = filepath.Join(internals.home, filepath.FromSlash(strings.TrimPrefix(expanded, "~/")))
	}
	return expanded, true
}

var openInAppRegistryVariable = regexp.MustCompile(`%([^%]+)%`)

func expandOpenInAppRegistryValue(value string, internals openInAppInternals) (string, bool) {
	valid := true
	expanded := openInAppRegistryVariable.ReplaceAllStringFunc(value, func(token string) string {
		key := strings.Trim(token, "%")
		found, ok := internals.env[key]
		if !ok {
			valid = false
			return token
		}
		return found
	})
	return expanded, valid
}

type openInAppInstallRecord struct {
	displayName     string
	installLocation string
	displayIcon     string
}

type openInAppRegistryView struct {
	appPaths       map[string]string
	installRecords []openInAppInstallRecord
}

var openInAppRegistryLine = regexp.MustCompile(`^\s+(.*?)\s+(REG_SZ|REG_EXPAND_SZ)\s+(.*)$`)

func parseOpenInAppRegistryDump(dump string) map[string]map[string]string {
	keys := map[string]map[string]string{}
	var current map[string]string
	for _, line := range strings.Split(strings.ReplaceAll(dump, "\r\n", "\n"), "\n") {
		if strings.HasPrefix(line, "HK") {
			current = map[string]string{}
			keys[strings.TrimSpace(line)] = current
			continue
		}
		match := openInAppRegistryLine.FindStringSubmatch(line)
		if current == nil || len(match) != 4 {
			continue
		}
		name := match[1]
		if strings.HasPrefix(name, "(") && strings.HasSuffix(name, ")") {
			name = "(Default)"
		}
		current[name] = strings.TrimSpace(match[3])
	}
	return keys
}

var openInAppAppPathsRoots = []string{
	`HKCU\Software\Microsoft\Windows\CurrentVersion\App Paths`,
	`HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths`,
}

var openInAppUninstallRoots = []string{
	`HKCU\Software\Microsoft\Windows\CurrentVersion\Uninstall`,
	`HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall`,
	`HKLM\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall`,
}

func readOpenInAppRegistryView(timeout time.Duration, internals openInAppInternals) openInAppRegistryView {
	view := openInAppRegistryView{appPaths: map[string]string{}}
	for _, root := range openInAppAppPathsRoots {
		dump := openInAppCommandOutput("reg.exe", []string{"query", root, "/s"}, timeout, internals)
		for key, values := range parseOpenInAppRegistryDump(dump) {
			separator := strings.LastIndex(key, `\`)
			exe := strings.ToLower(key[separator+1:])
			target, ok := values["(Default)"]
			if !ok || !strings.HasSuffix(exe, ".exe") {
				continue
			}
			if _, exists := view.appPaths[exe]; exists {
				continue
			}
			target = strings.Trim(target, `"`)
			if expanded, valid := expandOpenInAppRegistryValue(target, internals); valid {
				view.appPaths[exe] = expanded
			}
		}
	}
	for _, root := range openInAppUninstallRoots {
		dump := openInAppCommandOutput("reg.exe", []string{"query", root, "/s"}, timeout, internals)
		for _, values := range parseOpenInAppRegistryDump(dump) {
			if displayName := values["DisplayName"]; displayName != "" {
				view.installRecords = append(view.installRecords, openInAppInstallRecord{
					displayName: displayName, installLocation: values["InstallLocation"], displayIcon: values["DisplayIcon"],
				})
			}
		}
	}
	return view
}

type openInAppDesktopEntry struct {
	exec    string
	tryExec string
	icon    string
}

func parseOpenInAppDesktopEntry(text string) openInAppDesktopEntry {
	var result openInAppDesktopEntry
	inEntry := false
	for _, line := range strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			inEntry = line == "[Desktop Entry]"
			continue
		}
		if !inEntry {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "Exec":
			result.exec = strings.TrimSpace(value)
		case "TryExec":
			result.tryExec = strings.TrimSpace(value)
		case "Icon":
			result.icon = strings.TrimSpace(value)
		}
	}
	return result
}

func openInAppXDGDataDirectories(internals openInAppInternals) []string {
	home := internals.env["XDG_DATA_HOME"]
	if home == "" {
		home = filepath.Join(internals.home, ".local", "share")
	}
	dirs := internals.env["XDG_DATA_DIRS"]
	if dirs == "" {
		dirs = "/usr/local/share:/usr/share"
	}
	result := []string{home}
	for _, dir := range strings.Split(dirs, ":") {
		if dir != "" {
			result = append(result, dir)
		}
	}
	return result
}

func findOpenInAppDesktopEntry(desktopID string, internals openInAppInternals) (openInAppDesktopEntry, bool) {
	for _, dataDir := range openInAppXDGDataDirectories(internals) {
		data, err := os.ReadFile(filepath.Join(dataDir, "applications", desktopID+".desktop"))
		if err == nil {
			return parseOpenInAppDesktopEntry(string(data)), true
		}
	}
	return openInAppDesktopEntry{}, false
}

func openInAppExecCommand(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, `"`) {
		if end := strings.Index(value[1:], `"`); end >= 0 {
			return value[1 : end+1]
		}
	}
	if fields := strings.Fields(value); len(fields) > 0 {
		return fields[0]
	}
	return ""
}

func openInAppDesktopLauncher(entry openInAppDesktopEntry, internals openInAppInternals) string {
	candidate := entry.tryExec
	if candidate == "" {
		candidate = openInAppExecCommand(entry.exec)
	}
	if candidate == "" {
		return ""
	}
	if filepath.IsAbs(candidate) {
		if openInAppIsFile(candidate) {
			return candidate
		}
		return ""
	}
	found, err := internals.resolveExecutable(candidate)
	if err != nil {
		return ""
	}
	return found
}

func openInAppDesktopAvailable(platform string, env map[string]string) bool {
	if platform != "linux" {
		return true
	}
	return env["DISPLAY"] != "" || env["WAYLAND_DISPLAY"] != ""
}

func openInAppExecutableIcon(path string, internals openInAppInternals) *openInAppIconSource {
	if internals.platform == "windows" {
		return &openInAppIconSource{kind: "executable", path: path}
	}
	return nil
}

func openInAppNaturalLess(left, right string) bool {
	leftParts := regexp.MustCompile(`\d+|\D+`).FindAllString(left, -1)
	rightParts := regexp.MustCompile(`\d+|\D+`).FindAllString(right, -1)
	for index := 0; index < len(leftParts) && index < len(rightParts); index++ {
		leftNumber, leftErr := strconv.Atoi(leftParts[index])
		rightNumber, rightErr := strconv.Atoi(rightParts[index])
		if leftErr == nil && rightErr == nil && leftNumber != rightNumber {
			return leftNumber > rightNumber
		}
		if leftParts[index] != rightParts[index] {
			return leftParts[index] > rightParts[index]
		}
	}
	return len(leftParts) > len(rightParts)
}

func openInAppRecordLauncher(record openInAppInstallRecord, relative string, internals openInAppInternals) string {
	if relative != "" && record.installLocation != "" {
		location := strings.Trim(record.installLocation, `"`)
		if expanded, valid := expandOpenInAppRegistryValue(location, internals); valid {
			candidate := filepath.Join(expanded, filepath.FromSlash(relative))
			if openInAppIsFile(candidate) {
				return candidate
			}
		}
	}
	if record.displayIcon != "" {
		bare := regexp.MustCompile(`,-?\d+$`).ReplaceAllString(record.displayIcon, "")
		bare = strings.Trim(strings.TrimSpace(bare), `"`)
		if expanded, valid := expandOpenInAppRegistryValue(bare, internals); valid && strings.HasSuffix(strings.ToLower(expanded), ".exe") && openInAppIsFile(expanded) {
			return expanded
		}
	}
	return ""
}

func resolveOpenInAppLocator(locator openInAppLocator, timeout time.Duration, registry *openInAppRegistryView, internals openInAppInternals) *openInAppResolvedLaunch {
	switch locator.kind {
	case "fixed":
		resolved := &openInAppResolvedLaunch{launch: locator.launch}
		if iconPath, valid := expandOpenInAppCandidate(locator.iconPath, internals); valid {
			kind := "app-bundle"
			if internals.platform == "windows" {
				kind = "executable"
			}
			resolved.icon = &openInAppIconSource{kind: kind, path: iconPath}
		}
		return resolved
	case "app":
		for _, root := range internals.applicationRoots {
			for _, name := range locator.fsNames {
				bundle := filepath.Join(root, name)
				if openInAppIsDirectory(bundle) {
					return &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "argv", command: "open", args: []string{"-a", bundle}}, icon: &openInAppIconSource{kind: "app-bundle", path: bundle}}
				}
			}
		}
	case "xcode":
		developer := strings.TrimSpace(openInAppCommandOutput("xcode-select", []string{"-p"}, timeout, internals))
		bundle := filepath.Dir(filepath.Dir(developer))
		if developer != "" && strings.HasSuffix(bundle, ".app") && openInAppIsDirectory(bundle) {
			fallback := openInAppLaunch{kind: "argv", command: "open", args: []string{"-a", bundle}}
			return &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "argv", command: "xed"}, fallbackLaunch: &fallback, icon: &openInAppIconSource{kind: "app-bundle", path: bundle}}
		}
	case "cli":
		if locator.requiresDesktop && !openInAppDesktopAvailable(internals.platform, internals.env) {
			return nil
		}
		if found, err := internals.resolveExecutable(locator.name); err == nil {
			return &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "argv", command: found, args: append([]string(nil), locator.args...)}, icon: openInAppExecutableIcon(found, internals)}
		}
	case "file":
		for _, candidate := range locator.candidates {
			if path, valid := expandOpenInAppCandidate(candidate, internals); valid && openInAppIsFile(path) {
				return &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "argv", command: path, args: append([]string(nil), locator.args...)}, icon: openInAppExecutableIcon(path, internals)}
			}
		}
	case "scan":
		root, valid := expandOpenInAppCandidate(locator.root, internals)
		if !valid {
			return nil
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), locator.namePrefix) {
				names = append(names, entry.Name())
			}
		}
		sort.Slice(names, func(i, j int) bool { return openInAppNaturalLess(names[i], names[j]) })
		for _, name := range names {
			launcher := filepath.Join(root, name, filepath.FromSlash(locator.relativeLauncher))
			if openInAppIsFile(launcher) {
				return &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "argv", command: launcher, args: append([]string(nil), locator.args...)}, icon: openInAppExecutableIcon(launcher, internals)}
			}
		}
	case "app-paths":
		target := registry.appPaths[strings.ToLower(locator.exe)]
		if openInAppIsFile(target) {
			return &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "argv", command: target, args: append([]string(nil), locator.args...)}, icon: &openInAppIconSource{kind: "executable", path: target}}
		}
	case "install-record":
		for _, record := range registry.installRecords {
			if strings.HasPrefix(record.displayName, locator.displayNamePrefix) {
				if launcher := openInAppRecordLauncher(record, locator.relativeLauncher, internals); launcher != "" {
					return &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "argv", command: launcher, args: append([]string(nil), locator.args...)}, icon: &openInAppIconSource{kind: "executable", path: launcher}}
				}
			}
		}
	case "github-desktop":
		root, valid := expandOpenInAppCandidate(locator.root, internals)
		if !valid {
			return nil
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "app-") {
				names = append(names, entry.Name())
			}
		}
		sort.Slice(names, func(i, j int) bool { return openInAppNaturalLess(names[i], names[j]) })
		for _, name := range names {
			directory := filepath.Join(root, name)
			executable := filepath.Join(directory, "GitHubDesktop.exe")
			cli := filepath.Join(directory, "resources", "app", "cli.js")
			if openInAppIsFile(executable) && openInAppIsFile(cli) {
				return &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "argv", command: executable, args: []string{cli, "open"}, env: map[string]string{"ELECTRON_RUN_AS_NODE": "1"}, windowsHide: true}, icon: &openInAppIconSource{kind: "executable", path: executable}}
			}
		}
	case "desktop":
		entry, ok := findOpenInAppDesktopEntry(locator.desktopID, internals)
		if ok {
			if launcher := openInAppDesktopLauncher(entry, internals); launcher != "" {
				return &openInAppResolvedLaunch{launch: openInAppLaunch{kind: "argv", command: launcher, args: append([]string(nil), locator.args...)}}
			}
		}
	default:
		panic("unknown open-in-app locator: " + locator.kind)
	}
	return nil
}

func resolveOpenInAppLaunch(app openInAppApp, timeout time.Duration, internals openInAppInternals, registry *openInAppRegistryView) *openInAppResolvedLaunch {
	spec, ok := app.platforms[internals.platform]
	if !ok {
		return nil
	}
	for _, locator := range spec.locators {
		if resolved := resolveOpenInAppLocator(locator, timeout, registry, internals); resolved != nil {
			return resolved
		}
	}
	return nil
}

func resolveOpenInAppApps(timeout time.Duration, value openInAppInternals) map[string]*openInAppResolvedLaunch {
	internals := completedOpenInAppInternals(value)
	registry := openInAppRegistryView{appPaths: map[string]string{}}
	if internals.platform == "windows" {
		registry = readOpenInAppRegistryView(timeout, internals)
	}
	result := map[string]*openInAppResolvedLaunch{}
	for _, app := range openInAppCatalog {
		if resolved := resolveOpenInAppLaunch(app, timeout, internals, &registry); resolved != nil {
			result[app.id] = resolved
		}
	}
	return result
}

func openInAppLaunchArgs(args []string, path string) []string {
	result := append([]string(nil), args...)
	hasPath := false
	for index := range result {
		if strings.Contains(result[index], openInAppPathToken) {
			hasPath = true
			result[index] = strings.ReplaceAll(result[index], openInAppPathToken, path)
		}
	}
	if !hasPath {
		result = append(result, path)
	}
	return result
}

func openInAppMissingExecutable(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, exec.ErrNotFound)
}

func runOpenInAppLaunch(ctx context.Context, launch openInAppLaunch, path string, watch time.Duration, internals openInAppInternals) openInAppLaunchOutcome {
	if launch.kind == "shell-open" {
		var command string
		var args []string
		switch internals.platform {
		case "darwin":
			command, args = "open", []string{path}
		case "windows":
			command, args = "powershell.exe", []string{"-NoProfile", "-Command", "Invoke-Item -LiteralPath '" + strings.ReplaceAll(path, "'", "''") + "'"}
		case "linux":
			command, args = "xdg-open", []string{path}
		default:
			return openInAppFailed
		}
		done := make(chan error, 1)
		go func() {
			_, err := internals.run(context.Background(), command, args, nil)
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				return openInAppLaunched
			}
			if openInAppMissingExecutable(err) {
				return openInAppMissing
			}
			return openInAppFailed
		case <-ctx.Done():
			return openInAppFailed
		case <-time.After(watch):
			return openInAppLaunched
		}
	}
	err := internals.launch(ctx, launch.command, openInAppLaunchArgs(launch.args, path), watch, launch.env, launch.windowsHide)
	if err == nil {
		return openInAppLaunched
	}
	if openInAppMissingExecutable(err) {
		return openInAppMissing
	}
	return openInAppFailed
}

func launchResolvedOpenInApp(ctx context.Context, resolved *openInAppResolvedLaunch, path string, watch time.Duration, value openInAppInternals) openInAppLaunchOutcome {
	internals := completedOpenInAppInternals(value)
	primary := runOpenInAppLaunch(ctx, resolved.launch, path, watch, internals)
	if primary == openInAppLaunched || resolved.fallbackLaunch == nil {
		return primary
	}
	fallback := runOpenInAppLaunch(ctx, *resolved.fallbackLaunch, path, watch, internals)
	if primary == openInAppMissing || fallback == openInAppMissing {
		return openInAppMissing
	}
	return fallback
}

func launchDetachedOpenInApp(ctx context.Context, command string, args []string, watch time.Duration, overlay map[string]string, windowsHide bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	process := exec.Command(command, args...)
	process.Env = scrubbedChildEnv(overlay)
	process.Stdin = nil
	process.Stdout = nil
	process.Stderr = nil
	configureDetachedOpenInAppProcess(process, windowsHide)
	if err := process.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(watch):
		return nil
	}
}

func openInAppResolutionString(value *openInAppResolvedLaunch) string {
	if value == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s %q", value.launch.command, value.launch.args)
}
