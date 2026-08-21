package harness

import (
	"bytes"
	"context"
	"encoding/json"
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

var ianaTimeZonePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+.-]*(/[A-Za-z0-9_+.-]+)+$`)

type ClientTimeZoneError struct{ Value string }

func (e *ClientTimeZoneError) Error() string {
	return "clientTimeZone must be UTC or a valid IANA Area/Location name"
}

type TimeContextConfig struct {
	TimeZone        string
	RefreshInterval time.Duration
	location        *time.Location
	resolvedZone    string
}

type TmuxContextConfig struct {
	RefreshInterval time.Duration
}

func validateContextConfig(config Config) error {
	if config.TimeContext != nil {
		if config.TimeContext.RefreshInterval < 0 {
			return fmt.Errorf("time-context: refreshIntervalMs must be a non-negative safe integer, got %d", config.TimeContext.RefreshInterval.Milliseconds())
		}
		location, zone, err := resolveTimeContextLocation(config.TimeContext.TimeZone)
		if err != nil {
			return err
		}
		config.TimeContext.location = location
		config.TimeContext.resolvedZone = zone
	}
	if config.TmuxContext != nil && config.TmuxContext.RefreshInterval < 0 {
		return fmt.Errorf("tmux-context: refreshIntervalMs must be a non-negative safe integer, got %d", config.TmuxContext.RefreshInterval.Milliseconds())
	}
	return nil
}

func canonicalClientTimeZone(value string) (string, bool) {
	if value == "UTC" {
		return value, true
	}
	if value == "" || strings.TrimSpace(value) != value || !ianaTimeZonePattern.MatchString(value) {
		return "", false
	}
	if _, err := time.LoadLocation(value); err != nil {
		return "", false
	}
	canonical := canonicalZoneAlias(value)
	if canonical != "UTC" && !ianaTimeZonePattern.MatchString(canonical) {
		return "", false
	}
	return canonical, true
}

func canonicalZoneAlias(value string) string {
	if canonical, ok := map[string]string{
		"Etc/GMT": "UTC", "Etc/UTC": "UTC",
		"US/Pacific": "America/Los_Angeles",
	}[value]; ok {
		return canonical
	}
	for _, root := range []string{"/usr/share/zoneinfo", "/usr/share/lib/zoneinfo"} {
		path, err := filepath.EvalSymlinks(filepath.Join(root, filepath.FromSlash(value)))
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(root, path)
		if err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return filepath.ToSlash(rel)
		}
	}
	return value
}

func resolveTimeContextLocation(name string) (*time.Location, string, error) {
	if name == "" {
		name = systemTimeZoneName()
	}
	if name == "" || name == "Local" {
		return time.Local, time.Local.String(), nil
	}
	canonical := canonicalZoneAlias(name)
	location, err := time.LoadLocation(canonical)
	if err != nil {
		return nil, "", fmt.Errorf("time-context: invalid IANA timeZone %q", name)
	}
	return location, canonical, nil
}

func systemTimeZoneName() string {
	if value := strings.TrimSpace(os.Getenv("TZ")); value != "" && !strings.HasPrefix(value, ":") {
		return value
	}
	if data, err := os.ReadFile("/etc/timezone"); err == nil {
		if value := strings.TrimSpace(string(data)); value != "" {
			return value
		}
	}
	if path, err := filepath.EvalSymlinks("/etc/localtime"); err == nil {
		for _, marker := range []string{"/zoneinfo/", "/zones/"} {
			if index := strings.Index(filepath.ToSlash(path), marker); index >= 0 {
				return filepath.ToSlash(path)[index+len(marker):]
			}
		}
	}
	return time.Local.String()
}

func (e *Engine) appendStepContexts(ctx context.Context, session *Session, turn, step int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	messages := make([]map[string]any, 0, 2)
	if e.cfg.TmuxContext != nil && step == 1 {
		if message, ok := e.tmuxContext(ctx, session, turn); ok {
			messages = append(messages, message)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.cfg.TimeContext != nil {
		message, ok, err := e.timeContext(session, turn, step)
		if err != nil {
			return err
		}
		if ok {
			messages = append(messages, message)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, message := range messages {
		if _, err := e.appendEvent(session, "user/message", message); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) appendDynamicPromptContext(session *Session, sections []resolvedPromptSection) error {
	const source = "@deepseek-ai/dsh-system-prompt"
	const cleared = "Current runtime context: none. Earlier runtime-context snapshots no longer apply."
	body := make([]string, len(sections))
	projected := make([]map[string]any, len(sections))
	for index, section := range sections {
		body[index] = section.Text
		projected[index] = map[string]any{"name": section.Name, "text": section.Text}
	}
	current := strings.Join(body, "\n\n")
	if current != "" {
		current = "Current runtime context. This snapshot supersedes earlier runtime-context snapshots.\n\n" + current
	} else {
		current = cleared
	}

	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	isOwned := func(event Event) bool {
		if event.Type != "user/message" || eventSourceKind(event.Data) != "plugin" {
			return false
		}
		message := nestedMessage(event.Data)
		origin, _ := message["source"].(map[string]any)
		plugin, _ := origin["plugin"].(string)
		return plugin == source
	}
	ever := false
	for _, event := range events {
		if isOwned(event) {
			ever = true
			break
		}
	}
	if !ever && len(sections) == 0 {
		return nil
	}
	surface, err := foldSurfaceEvents(events, false)
	if err != nil {
		return err
	}
	for index := len(surface) - 1; index >= 0; index-- {
		if !isOwned(surface[index]) {
			continue
		}
		if contentValueText(surface[index].Data) == current {
			return nil
		}
		break
	}
	origin := map[string]any{"kind": "plugin", "plugin": source}
	if len(sections) > 0 {
		origin["form"] = "snapshot"
		origin["sections"] = projected
	}
	_, err = e.appendEvent(session, "user/message", map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: current}}, "source": origin,
	})
	return err
}

func (e *Engine) timeContext(session *Session, turn, step int) (map[string]any, bool, error) {
	now := time.Now()
	config := *e.cfg.TimeContext
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	if config.RefreshInterval > 0 {
		if last, ok := latestPluginEventTime(events, "time-context"); ok && !now.Before(last) && now.Sub(last) < config.RefreshInterval {
			return nil, false, nil
		}
	}
	location, fallbackZone := config.location, config.resolvedZone
	if location == nil || fallbackZone == "" {
		var err error
		location, fallbackZone, err = resolveTimeContextLocation(config.TimeZone)
		if err != nil {
			return nil, false, err
		}
	}
	zones, err := browserTimeZones(events, turn)
	if err != nil {
		return nil, false, err
	}
	selectedZone := fallbackZone
	browserText := "Browser time zone for this request: unavailable. Ask the user to clarify otherwise-unqualified dates and times."
	if len(zones) == 1 {
		selectedZone = zones[0]
		location, err = time.LoadLocation(selectedZone)
		if err != nil {
			return nil, false, err
		}
		browserText = "Browser time zone for this request: " + selectedZone + ". Interpret otherwise-unqualified dates and times in this zone."
	} else if len(zones) > 1 {
		encoded, _ := json.Marshal(zones)
		browserText = "Browser time zone for this request: mixed " + string(encoded) + ". Ask the user to clarify otherwise-unqualified dates and times."
	}
	previous, ok := precedingContextTime(events, turn, step)
	elapsed := "unavailable"
	if ok {
		elapsed = formatContextDuration(now.Sub(previous))
	}
	baseline := "model-visible message"
	if step > 1 {
		baseline = "step context"
	}
	text := fmt.Sprintf("Time sampled while preparing turn %d, step %d: %s\n%s\nElapsed since the preceding %s: %s.",
		turn, step, formatContextTimestamp(now, location, selectedZone), browserText, baseline, elapsed)
	return pluginContextMessage("time-context", text), true, nil
}

func browserTimeZones(events []Event, turn int) ([]string, error) {
	start := turnStartIndex(events, turn)
	seen := map[string]bool{}
	for _, event := range events[start+1:] {
		if event.Type != "user/message" {
			continue
		}
		message := nestedMessage(event.Data)
		source, _ := message["source"].(map[string]any)
		if source["kind"] != "user" {
			continue
		}
		if rpcID, _ := source["rpcId"].(string); rpcID == "" {
			continue
		}
		zone, present := source["clientTimeZone"].(string)
		if !present {
			continue
		}
		canonical, ok := canonicalClientTimeZone(zone)
		if !ok || canonical != zone {
			return nil, fmt.Errorf("browser time zone must be canonical UTC or IANA Area/Location: %q", zone)
		}
		seen[zone] = true
	}
	zones := make([]string, 0, len(seen))
	for zone := range seen {
		zones = append(zones, zone)
	}
	sort.Strings(zones)
	return zones, nil
}

func precedingContextTime(events []Event, turn, step int) (time.Time, bool) {
	start := turnStartIndex(events, turn)
	if step > 1 {
		for index := len(events) - 1; index > start; index-- {
			if isPluginMessage(events[index], "time-context") {
				return time.UnixMilli(events[index].Time), true
			}
		}
		return time.Time{}, false
	}
	for index := len(events) - 1; index >= 0; index-- {
		switch events[index].Type {
		case "user/message", "assistant/message", "tool/result":
			return time.UnixMilli(events[index].Time), true
		}
	}
	return time.Time{}, false
}

func latestPluginEventTime(events []Event, plugin string) (time.Time, bool) {
	for index := len(events) - 1; index >= 0; index-- {
		if isPluginMessage(events[index], plugin) {
			return time.UnixMilli(events[index].Time), true
		}
	}
	return time.Time{}, false
}

func isPluginMessage(event Event, plugin string) bool {
	if event.Type != "user/message" {
		return false
	}
	message := nestedMessage(event.Data)
	source, _ := message["source"].(map[string]any)
	return source["kind"] == "plugin" && source["plugin"] == plugin
}

func turnStartIndex(events []Event, turn int) int {
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type == "turn/start" {
			data, _ := events[index].Data.(map[string]any)
			if eventInt(data["turn"]) == turn {
				return index
			}
		}
	}
	return -1
}

func formatContextDuration(duration time.Duration) string {
	seconds := int64(duration / time.Second)
	if seconds < 0 {
		seconds = 0
	}
	days := seconds / 86400
	seconds %= 86400
	hours := seconds / 3600
	seconds %= 3600
	minutes := seconds / 60
	seconds %= 60
	parts := make([]string, 0, 4)
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%dm", minutes))
	}
	parts = append(parts, fmt.Sprintf("%ds", seconds))
	return strings.Join(parts, " ")
}

func formatContextTimestamp(now time.Time, location *time.Location, zone string) string {
	return now.In(location).Format("2006-01-02T15:04:05-07:00") + "[" + zone + "]"
}

func pluginContextMessage(plugin, text string) map[string]any {
	return map[string]any{
		"id": newID("msg"), "role": "user", "content": []ContentBlock{{Type: "text", Text: text}},
		"source": map[string]any{"kind": "plugin", "plugin": plugin, "form": "snapshot", "sections": []map[string]any{{"name": plugin, "text": text}}},
	}
}

type tmuxLocation struct {
	sessionName, windowIndex, windowName, paneIndex, paneID string
	windowActive, paneActive, windowLayout                  string
}

func (e *Engine) tmuxContext(ctx context.Context, session *Session, turn int) (map[string]any, bool) {
	config := *e.cfg.TmuxContext
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	previousState, previousTime, previous := latestTmuxContext(events)
	now := time.Now()
	if previous && config.RefreshInterval > 0 && !now.Before(previousTime) && now.Sub(previousTime) < config.RefreshInterval {
		return nil, false
	}
	location, ok := queryTmuxLocation(ctx, os.Getpid())
	if !ok {
		return nil, false
	}
	state := renderTmuxState(location)
	if previous && state == previousState {
		return nil, false
	}
	text := fmt.Sprintf("tmux location (turn %d):\n%s", turn, state)
	return pluginContextMessage("tmux-context", text), true
}

func queryTmuxLocation(ctx context.Context, pid int) (tmuxLocation, bool) {
	pane := os.Getenv("TMUX_PANE")
	if pane == "" || runtime.GOOS == "windows" {
		return tmuxLocation{}, false
	}
	output, err := exec.CommandContext(ctx, "ps", "-o", "tty=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return tmuxLocation{}, false
	}
	tty := strings.TrimSpace(string(output))
	if tty == "" || tty == "?" {
		return tmuxLocation{}, false
	}
	paneTTY, err := exec.CommandContext(ctx, "tmux", "display-message", "-t", pane, "-p", "#{pane_tty}").Output()
	if err != nil || strings.TrimSpace(string(paneTTY)) != "/dev/"+strings.TrimPrefix(tty, "/dev/") {
		return tmuxLocation{}, false
	}
	fields := []string{
		"#{session_name}", "#{window_index}", "#{window_name}", "#{pane_index}",
		"#{pane_id}", "#{window_active}", "#{pane_active}", "#{window_layout}",
	}
	output, err = exec.CommandContext(ctx, "tmux", "display-message", "-t", pane, "-p", strings.Join(fields, `\t`)).Output()
	if err != nil {
		return tmuxLocation{}, false
	}
	line, _, _ := strings.Cut(string(output), "\n")
	parts := strings.Split(line, `\t`)
	if len(parts) != len(fields) || parts[4] == "" {
		return tmuxLocation{}, false
	}
	return tmuxLocation{
		sessionName: parts[0], windowIndex: parts[1], windowName: parts[2], paneIndex: parts[3], paneID: parts[4],
		windowActive: parts[5], paneActive: parts[6], windowLayout: parts[7],
	}, true
}

func renderTmuxState(location tmuxLocation) string {
	windowName, _ := encodeJSONString(location.windowName)
	return fmt.Sprintf("session %s, window %s %s, pane %s %s\nwindow active=%s, pane active=%s, layout %s",
		location.sessionName, location.windowIndex, windowName, location.paneIndex, location.paneID,
		location.windowActive, location.paneActive, location.windowLayout)
}

func latestTmuxContext(events []Event) (string, time.Time, bool) {
	for index := len(events) - 1; index >= 0; index-- {
		if !isPluginMessage(events[index], "tmux-context") {
			continue
		}
		message := nestedMessage(events[index].Data)
		text := contentValueText(message["content"])
		if text == "" {
			return "", time.Time{}, false
		}
		_, state, ok := strings.Cut(text, "\n")
		return state, time.UnixMilli(events[index].Time), ok
	}
	return "", time.Time{}, false
}

func encodeJSONString(value string) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(output.Bytes(), []byte("\n")), nil
}
