package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSettingsRegistryMergeRevisionAndRootMutation(t *testing.T) {
	e := newIntegrationEngine(t)

	described := e.settingsDescribe()["namespaces"].([]any)
	seenWelcome := false
	for _, raw := range described {
		if raw.(map[string]any)["ns"] == "ui-onboarding" {
			seenWelcome = true
		}
	}
	if !seenWelcome {
		t.Fatal("settings.describe did not register ui-onboarding")
	}

	if _, err := e.settingsUpdate("ui-onboarding", map[string]any{
		"nested": map[string]any{"keep": "yes", "replace": "old"},
	}, nil, false); err != nil {
		t.Fatal(err)
	}
	view, err := e.settingsUpdate("ui-onboarding", map[string]any{
		"nested": map[string]any{"replace": "new", "added": true},
	}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	row := view.(map[string]any)
	nested := row["value"].(map[string]any)["nested"].(map[string]any)
	if nested["keep"] != "yes" || nested["replace"] != "new" || nested["added"] != true {
		t.Fatalf("settings update was not recursive: %#v", nested)
	}
	if row["revision"] != 2 {
		t.Fatalf("revision = %v, want 2", row["revision"])
	}

	_, conflict := e.settingsUpdate("ui-onboarding", map[string]any{"x": true}, intPtr(0), false)
	if conflict == nil || conflict.Code != "settings-conflict" {
		t.Fatalf("conflict = %#v", conflict)
	}
	details := conflict.Details.(map[string]any)
	if details["ns"] != "ui-onboarding" || details["expected"] != 0 || details["actual"] != 2 {
		t.Fatalf("conflict details = %#v", details)
	}

	if _, err := e.settingsMutate("ui-onboarding", []any{map[string]any{"op": "set", "path": []any{}, "value": map[string]any{"root": true}}}, nil); err != nil {
		t.Fatal(err)
	}
	view, err = e.settingsMutate("ui-onboarding", []any{map[string]any{"op": "unset", "path": []any{}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := view.(map[string]any)["value"].(map[string]any); len(got) != 0 {
		t.Fatalf("root unset value = %#v, want empty object", got)
	}

	_, rejected := e.settingsUpdate("not-registered", map[string]any{}, nil, false)
	if rejected == nil || rejected.Code != "settings-rejected" {
		t.Fatalf("unknown namespace error = %#v", rejected)
	}
}

func TestClientHostSettingsSchemasDefaultsAndValidation(t *testing.T) {
	e := newIntegrationEngine(t)
	views := map[string]map[string]any{}
	for _, raw := range e.settingsDescribe()["namespaces"].([]any) {
		view := raw.(map[string]any)
		views[view["ns"].(string)] = view
	}
	for ns, want := range map[string]map[string]any{
		"locale":          {},
		"ui-conversation": {"busyEnter": "queue"},
		"ui-onboarding":   {},
		"ui-theme":        {"preference": "system"},
	} {
		view := views[ns]
		if view == nil || !reflect.DeepEqual(view["value"], want) {
			t.Fatalf("%s settings view = %#v", ns, view)
		}
		schema := view["schema"].(map[string]any)
		refs := schema["refs"].(map[string]any)
		if refs[fmt.Sprint(schema["uid"])].(map[string]any)["type"] != "object" {
			t.Fatalf("%s schema = %#v", ns, schema)
		}
	}
	for ns, patch := range map[string]map[string]any{
		"locale":          {"preference": "fr"},
		"ui-conversation": {"busyEnter": "later"},
		"ui-onboarding":   {"welcomeNoticeVersion": true},
		"ui-theme":        {"preference": "sepia"},
	} {
		if _, rpcErr := e.settingsUpdate(ns, patch, nil, false); rpcErr == nil || rpcErr.Code != "settings-rejected" {
			t.Fatalf("%s invalid write = %#v", ns, rpcErr)
		}
	}
	for ns, patch := range map[string]map[string]any{
		"locale":          {"preference": "en"},
		"ui-conversation": {"busyEnter": "steer"},
		"ui-onboarding":   {"welcomeNoticeVersion": "2026-08-20"},
		"ui-theme":        {"preference": "dark"},
	} {
		if _, rpcErr := e.settingsUpdate(ns, patch, nil, false); rpcErr != nil {
			t.Fatalf("%s valid write = %#v", ns, rpcErr)
		}
	}
}

func TestPermissionSettingsDriveOnlyFutureSessions(t *testing.T) {
	t.Setenv("DSH_PERMISSION_MODE", "workspace-write")
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Workspace = dir
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })

	permissionView := func() map[string]any {
		t.Helper()
		for _, raw := range e.settingsDescribe()["namespaces"].([]any) {
			view := raw.(map[string]any)
			if view["ns"] == "permission" {
				return view
			}
		}
		t.Fatal("permission settings namespace is missing")
		return nil
	}
	view := permissionView()
	if got := view["value"].(map[string]any)["defaultPreset"]; got != "workspace-write" {
		t.Fatalf("permission default = %v", got)
	}
	schema := view["schema"].(map[string]any)
	refs := schema["refs"].(map[string]any)
	root := refs[fmt.Sprint(schema["uid"])].(map[string]any)
	union := refs[fmt.Sprint(root["dict"].(map[string]any)["defaultPreset"])].(map[string]any)
	if got := len(union["list"].([]any)); got != 3 {
		t.Fatalf("permission schema choices = %d", got)
	}

	existingID, err := e.CreateSession(t.Context(), dir, "permission-before", "")
	if err != nil {
		t.Fatal(err)
	}
	existing, _ := e.getSession(existingID)
	if got := permissionEventValues(existing); !reflect.DeepEqual(got, []string{"workspace-write", "workspace-write", "ask"}) {
		t.Fatalf("existing permission events = %#v", got)
	}

	updated, rpcErr := e.settingsMutate("permission", []any{map[string]any{
		"op": "set", "path": []any{"defaultPreset"}, "value": "read-only",
	}}, nil)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if got := updated.(map[string]any)["value"].(map[string]any)["defaultPreset"]; got != "read-only" {
		t.Fatalf("updated permission default = %v", got)
	}
	if got := permissionEventValues(existing); !reflect.DeepEqual(got, []string{"workspace-write", "workspace-write", "ask"}) {
		t.Fatalf("existing session changed = %#v", got)
	}
	createdID, err := e.CreateSession(t.Context(), dir, "permission-after", "")
	if err != nil {
		t.Fatal(err)
	}
	created, _ := e.getSession(createdID)
	if got := permissionEventValues(created); !reflect.DeepEqual(got, []string{"read-only", "read-only", "ask"}) {
		t.Fatalf("new permission events = %#v", got)
	}
	document, err := os.ReadFile(filepath.Join(dir, settingsYAMLName))
	if err != nil || !strings.Contains(string(document), "defaultPreset: read-only") {
		t.Fatalf("permission settings document = %q, %v", document, err)
	}

	if _, rejected := e.settingsMutate("permission", []any{map[string]any{
		"op": "set", "path": []any{"defaultPreset"}, "value": "unknown",
	}}, nil); rejected == nil || rejected.Code != "settings-rejected" {
		t.Fatalf("invalid permission default = %#v", rejected)
	}
}

func permissionEventValues(s *Session) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := make([]string, 0, 3)
	for _, event := range s.Events {
		data, _ := event.Data.(map[string]any)
		switch event.Type {
		case "permission/preset":
			values = append(values, data["preset"].(string))
		case "sandbox/mode":
			values = append(values, data["mode"].(string))
		case "approval/policy":
			values = append(values, data["policy"].(string))
		}
	}
	return values
}

func TestCredentialsPersistWithOwnerOnlyPermissionsAndEvents(t *testing.T) {
	const ref = "DSH_TEST_CREDENTIAL"
	t.Setenv(ref, "")
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = dir
	cfg.Workspace = dir
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(e.Handler())
	t.Cleanup(func() {
		server.Close()
		_ = e.Close()
	})

	status, envelope := postRPC(t, server.Client(), server.URL, "", "credentials.set", map[string]any{"ref": ref, "value": "secret-value"})
	if status != http.StatusOK || rpcResult(t, envelope)["ok"] != true {
		t.Fatalf("credentials.set = %d %#v", status, envelope)
	}
	path := filepath.Join(dir, credentialsYAMLName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("credentials mode = %o, want 600", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "secret-value") {
		t.Fatalf("credentials file = %q, err=%v", data, err)
	}

	status, envelope = postRPC(t, server.Client(), server.URL, "", "credentials.describe", map[string]any{"refs": []string{ref}})
	if status != http.StatusOK {
		t.Fatalf("credentials.describe status = %d", status)
	}
	result := rpcResult(t, envelope)
	if result["ok"] != true {
		t.Fatalf("credentials.describe result = %#v", result)
	}
	credentials := result["value"].(map[string]any)["credentials"].(map[string]any)
	view := credentials[ref].(map[string]any)
	if view["configured"] != true || view["source"] != "file" || view["writable"] != true {
		t.Fatalf("credential view = %#v", view)
	}

	_ = e.Close()
	e2, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e2.Close() })
	configured, source, writable := e2.credentialInfo(ref)
	if !configured || source != "file" || !writable {
		t.Fatalf("reloaded credential = %v/%s/%v", configured, source, writable)
	}
	if err := e2.unsetCredential(ref); err != nil {
		t.Fatal(err)
	}
	configured, _, _ = e2.credentialInfo(ref)
	if configured {
		t.Fatal("credential remained configured after unset")
	}
}

func TestCredentialLaunchEnvironmentPrecedence(t *testing.T) {
	const ref = "DSH_CREDENTIAL_LAYER_TEST"
	t.Setenv(ref, "ambient-value-that-must-not-leak-through-the-snapshot")
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.APIKey = ""
	cfg.LaunchEnvironment = &LaunchEnvironmentSnapshot{
		Process: map[string]string{},
		Project: map[string]string{ref: "project"},
		User:    map[string]string{ref: "user"},
	}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if value, source, ok := e.resolveCredential(ref); !ok || value != "project" || source != "project-env" {
		t.Fatalf("project fallback = %q, %q, %v", value, source, ok)
	}
	if rpcErr := e.setCredential(ref, "managed"); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if value, source, ok := e.resolveCredential(ref); !ok || value != "managed" || source != "file" {
		t.Fatalf("managed credential = %q, %q, %v", value, source, ok)
	}
	configured, source, writable := e.credentialInfo(ref)
	if !configured || source != "file" || !writable {
		t.Fatalf("managed credential info = %v, %q, %v", configured, source, writable)
	}

	e.cfg.LaunchEnvironment.Process[ref] = "process"
	if value, source, ok := e.resolveCredential(ref); !ok || value != "process" || source != "env" {
		t.Fatalf("process credential = %q, %q, %v", value, source, ok)
	}
	if rpcErr := e.setCredential(ref, "blocked"); rpcErr == nil || rpcErr.Code != "credential-rejected" {
		t.Fatalf("process-shadowed write = %#v", rpcErr)
	}
}

func TestUpstreamYAMLSettingsAndCredentialsReload(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "")
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = dir
	cfg.Workspace = dir
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.APIKey = ""
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if _, rpcErr := e.settingsMutate("ui-onboarding", []any{map[string]any{
		"op": "set", "path": []any{"welcomeNoticeVersion"}, "value": "2026-08-13.1",
	}}, nil); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if rpcErr := e.setCredential("DEEPSEEK_API_KEY", "yaml-secret"); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	settingsData, err := os.ReadFile(filepath.Join(dir, settingsYAMLName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(settingsData), "ui-onboarding:") || !strings.Contains(string(settingsData), "welcomeNoticeVersion: 2026-08-13.1") {
		t.Fatalf("settings YAML = %q", settingsData)
	}
	credentialData, err := os.ReadFile(filepath.Join(dir, credentialsYAMLName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(credentialData), "DEEPSEEK_API_KEY: yaml-secret") {
		t.Fatalf("credentials YAML = %q", credentialData)
	}
	if _, err := os.Stat(filepath.Join(dir, "settings.json")); !os.IsNotExist(err) {
		t.Fatalf("legacy settings.json should not be written: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "credentials.json")); !os.IsNotExist(err) {
		t.Fatalf("legacy credentials.json should not be written: %v", err)
	}

	e2, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	view := e2.settingsViewLocked("ui-onboarding")
	if view["value"].(map[string]any)["welcomeNoticeVersion"] != "2026-08-13.1" {
		t.Fatalf("reloaded settings = %#v", view)
	}
	configured, source, writable := e2.credentialInfo("DEEPSEEK_API_KEY")
	if !configured || source != "file" || !writable {
		t.Fatalf("reloaded credential = %v/%s/%v", configured, source, writable)
	}
}

func TestLegacyJSONLoadsButYAMLWins(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "")
	dir := t.TempDir()
	legacySettings := `{"values":{"ui-onboarding":{"welcomeNoticeVersion":"legacy"}},"revisions":{"ui-onboarding":7}}`
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), []byte(legacySettings), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "credentials.json"), []byte(`{"DEEPSEEK_API_KEY":"legacy-key"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = dir
	cfg.Workspace = dir
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.APIKey = ""
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if e.settings["ui-onboarding"]["welcomeNoticeVersion"] != "legacy" || e.credentials["DEEPSEEK_API_KEY"] != "legacy-key" {
		t.Fatalf("legacy migration was not read: settings=%#v credentials=%#v", e.settings, e.credentials)
	}
	_ = e.Close()

	if err := os.WriteFile(filepath.Join(dir, settingsYAMLName), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, credentialsYAMLName), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e2, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if len(e2.settings) != 0 || len(e2.credentials) != 0 {
		t.Fatalf("canonical empty YAML did not override legacy JSON: settings=%#v credentials=%#v", e2.settings, e2.credentials)
	}
}

func TestDeepSeekOnboardingCredentialEnablesRealRequestWithoutRestart(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "")
	t.Setenv("DEEPSEEK_BASE_URL", "")
	type requestView struct {
		Authorization string
		Body          map[string]any
	}
	requests := make(chan requestView, 1)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode provider request: %v", err)
		}
		requests <- requestView{Authorization: r.Header.Get("Authorization"), Body: body}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"live response\"},\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer provider.Close()

	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = dir
	cfg.Workspace = dir
	cfg.Provider = "deepseek-official"
	cfg.Model = "deepseek-v4-flash"
	cfg.BaseURL = provider.URL
	cfg.APIKey = ""
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	rows := e.providerViews()
	found := false
	for _, row := range rows {
		if row["provider"] == "deepseek-official" {
			found = row["displayName"] == "DeepSeek" && row["settingsNs"] == "llm-deepseek" && row["active"] == true
		}
	}
	if !found {
		t.Fatalf("DeepSeek provider row = %#v", rows)
	}
	view := e.settingsViewLocked("llm-deepseek")
	value := view["value"].(map[string]any)
	if value["apiKeyEnv"] != "DEEPSEEK_API_KEY" || value["baseURL"] != provider.URL {
		t.Fatalf("DeepSeek settings view = %#v", view)
	}
	if rpcErr := e.setCredential("DEEPSEEK_API_KEY", "fresh-key"); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	sessionID, err := e.CreateSession(context.Background(), dir, "", "")
	if err != nil {
		t.Fatal(err)
	}
	text, err := e.Run(context.Background(), sessionID, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "hello"}}})
	if err != nil || text != "live response" {
		t.Fatalf("Run() = %q, %v", text, err)
	}
	select {
	case request := <-requests:
		if request.Authorization != "Bearer fresh-key" || request.Body["model"] != "deepseek-v4-flash" || request.Body["stream"] != true {
			t.Fatalf("provider request = %#v", request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("provider did not receive request")
	}
}

func TestSettingsAndCredentialRemoteEvents(t *testing.T) {
	e := newIntegrationEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host := e.SubscribeHost(ctx)
	if _, err := e.settingsMutate("ui-onboarding", []any{map[string]any{"op": "set", "path": []any{"welcomeNoticeVersion"}, "value": "2026-08-13.1"}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := e.setCredential("DSH_EVENT_CREDENTIAL", "value"); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"settings/document-updated": false, "credentials/updated": false}
	deadline := time.After(2 * time.Second)
	for !want["settings/document-updated"] || !want["credentials/updated"] {
		select {
		case frame := <-host:
			if frame["type"] != "host/remote-event" {
				continue
			}
			if event, _ := frame["event"].(string); event != "" {
				want[event] = true
			}
		case <-deadline:
			t.Fatalf("remote events = %#v", want)
		}
	}
}

func TestCredentialsRejectInvalidWirePayload(t *testing.T) {
	e := newIntegrationEngine(t)
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	status, envelope := postRPC(t, server.Client(), server.URL, "", "credentials.describe", map[string]any{"refs": []string{"not valid"}})
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	result := rpcResult(t, envelope)
	if result["ok"] != false || result["error"].(map[string]any)["code"] != "bad-request" {
		t.Fatalf("invalid credentials response = %#v", result)
	}
}

func newSharedPersistenceEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = dir
	cfg.Workspace = dir
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func holdOwnerWriterLock(t *testing.T, path string) func() {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	lockPath := path + ".lock"
	lock, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	t.Cleanup(release)
	return release
}

func requireWriterBlocked(t *testing.T, done <-chan *RPCError) {
	t.Helper()
	select {
	case result := <-done:
		t.Fatalf("writer completed while its lock was held: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSettingsWritersMergeAcrossIndependentEngines(t *testing.T) {
	dir := t.TempDir()
	first := newSharedPersistenceEngine(t, dir)
	second := newSharedPersistenceEngine(t, dir)
	release := holdOwnerWriterLock(t, settingsYAMLPathFor(first))
	start := make(chan struct{})
	firstReady := make(chan struct{})
	secondReady := make(chan struct{})
	firstDone := make(chan *RPCError, 1)
	secondDone := make(chan *RPCError, 1)
	go func() {
		close(firstReady)
		<-start
		_, rpcErr := first.settingsUpdate("ui-onboarding", map[string]any{"welcomeNoticeVersion": "first"}, nil, false)
		firstDone <- rpcErr
	}()
	go func() {
		close(secondReady)
		<-start
		_, rpcErr := second.settingsUpdate("ui-theme", map[string]any{"preference": "dark"}, nil, false)
		secondDone <- rpcErr
	}()
	<-firstReady
	<-secondReady
	close(start)
	requireWriterBlocked(t, firstDone)
	requireWriterBlocked(t, secondDone)
	release()
	if rpcErr := <-firstDone; rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if rpcErr := <-secondDone; rpcErr != nil {
		t.Fatal(rpcErr)
	}
	reloaded := newSharedPersistenceEngine(t, dir)
	if got := reloaded.settings["ui-onboarding"]["welcomeNoticeVersion"]; got != "first" {
		t.Fatalf("ui-onboarding after independent writes = %#v", reloaded.settings)
	}
	if got := reloaded.settings["ui-theme"]["preference"]; got != "dark" {
		t.Fatalf("ui-theme after independent writes = %#v", reloaded.settings)
	}
}

func TestCredentialWritersMergeAcrossIndependentEngines(t *testing.T) {
	const firstRef = "DSH_WRITER_LOCK_FIRST"
	const secondRef = "DSH_WRITER_LOCK_SECOND"
	t.Setenv(firstRef, "")
	t.Setenv(secondRef, "")
	dir := t.TempDir()
	first := newSharedPersistenceEngine(t, dir)
	second := newSharedPersistenceEngine(t, dir)
	release := holdOwnerWriterLock(t, credentialsYAMLPathFor(first))
	start := make(chan struct{})
	firstReady := make(chan struct{})
	secondReady := make(chan struct{})
	firstDone := make(chan *RPCError, 1)
	secondDone := make(chan *RPCError, 1)
	go func() {
		close(firstReady)
		<-start
		firstDone <- first.setCredential(firstRef, "first-value")
	}()
	go func() {
		close(secondReady)
		<-start
		secondDone <- second.setCredential(secondRef, "second-value")
	}()
	<-firstReady
	<-secondReady
	close(start)
	requireWriterBlocked(t, firstDone)
	requireWriterBlocked(t, secondDone)
	release()
	if rpcErr := <-firstDone; rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if rpcErr := <-secondDone; rpcErr != nil {
		t.Fatal(rpcErr)
	}
	reloaded := newSharedPersistenceEngine(t, dir)
	if got := reloaded.credentials[firstRef]; got != "first-value" {
		t.Fatalf("first credential after independent writes = %#v", reloaded.credentials)
	}
	if got := reloaded.credentials[secondRef]; got != "second-value" {
		t.Fatalf("second credential after independent writes = %#v", reloaded.credentials)
	}
}

func TestOwnerWriterLockTimeoutAndCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private.yaml")
	release := holdOwnerWriterLock(t, path)
	err := withOwnerFileLock(path, func() error {
		t.Fatal("timed-out writer acquired lock")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "timed out waiting for the writer lock") {
		t.Fatalf("writer lock timeout = %v", err)
	}
	release()
	want := fmt.Errorf("operation failed")
	if err := withOwnerFileLock(path, func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("writer operation error = %v", err)
	}
	if _, err := os.Stat(path + ".lock"); !os.IsNotExist(err) {
		t.Fatalf("writer lock remained after operation failure: %v", err)
	}
	called := false
	if err := withOwnerFileLock(path, func() error {
		called = true
		return nil
	}); err != nil || !called {
		t.Fatalf("writer lock was not reusable: called=%v err=%v", called, err)
	}
}

func TestCrossProcessSettingsAndCredentialsWriters(t *testing.T) {
	if role := os.Getenv("DSH_TEST_WRITER_ROLE"); role != "" {
		crossProcessWriter(t, role)
		return
	}
	dir := t.TempDir()
	start := make(chan struct{})
	type process struct {
		role string
		done chan error
	}
	processes := []process{
		{role: "first", done: make(chan error, 1)},
		{role: "second", done: make(chan error, 1)},
	}
	for index := range processes {
		proc := &processes[index]
		go func() {
			<-start
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrossProcessSettingsAndCredentialsWriters$", "-test.count=1")
			cmd.Env = append(os.Environ(), "DSH_TEST_WRITER_ROLE="+proc.role, "DSH_TEST_WRITER_DIR="+dir)
			proc.done <- cmd.Run()
		}()
	}
	close(start)
	for _, proc := range processes {
		if err := <-proc.done; err != nil {
			t.Fatalf("%s writer process: %v", proc.role, err)
		}
	}
	reloaded := newSharedPersistenceEngine(t, dir)
	if got := reloaded.settings["ui-onboarding"]["welcomeNoticeVersion"]; got != "first" {
		t.Fatalf("settings after cross-process writes = %#v", reloaded.settings)
	}
	if got := reloaded.settings["ui-theme"]["preference"]; got != "dark" {
		t.Fatalf("settings after cross-process writes = %#v", reloaded.settings)
	}
	if got := reloaded.credentials["DSH_PROCESS_LOCK_FIRST"]; got != "first-value" {
		t.Fatalf("credentials after cross-process writes = %#v", reloaded.credentials)
	}
	if got := reloaded.credentials["DSH_PROCESS_LOCK_SECOND"]; got != "second-value" {
		t.Fatalf("credentials after cross-process writes = %#v", reloaded.credentials)
	}
}

func TestUnsetCredentialReloadsCurrentDiskValue(t *testing.T) {
	const ref = "DSH_UNSET_CURRENT_DISK_VALUE"
	t.Setenv(ref, "")
	dir := t.TempDir()
	stale := newSharedPersistenceEngine(t, dir)
	writer := newSharedPersistenceEngine(t, dir)
	if rpcErr := writer.setCredential(ref, "current-value"); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if _, found := stale.credentials[ref]; found {
		t.Fatalf("stale engine unexpectedly observed new credential: %#v", stale.credentials)
	}
	if rpcErr := stale.unsetCredential(ref); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	reloaded := newSharedPersistenceEngine(t, dir)
	if _, found := reloaded.credentials[ref]; found {
		t.Fatalf("unset left current disk credential: %#v", reloaded.credentials)
	}
}

func crossProcessWriter(t *testing.T, role string) {
	t.Helper()
	dir := os.Getenv("DSH_TEST_WRITER_DIR")
	if dir == "" {
		t.Fatal("missing writer data directory")
	}
	var ns string
	var patch map[string]any
	var ref, value string
	switch role {
	case "first":
		ns, patch, ref, value = "ui-onboarding", map[string]any{"welcomeNoticeVersion": "first"}, "DSH_PROCESS_LOCK_FIRST", "first-value"
	case "second":
		ns, patch, ref, value = "ui-theme", map[string]any{"preference": "dark"}, "DSH_PROCESS_LOCK_SECOND", "second-value"
	default:
		t.Fatalf("unknown writer role %q", role)
	}
	e := newSharedPersistenceEngine(t, dir)
	if _, rpcErr := e.settingsUpdate(ns, patch, nil, false); rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if rpcErr := e.setCredential(ref, value); rpcErr != nil {
		t.Fatal(rpcErr)
	}
}

func intPtr(value int) *int { return &value }
