package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPTCCompatibilityColdLoadsHistoricalCodeWithoutRewritingJSONL(t *testing.T) {
	root := t.TempDir()
	store, err := NewJSONLSessionStore(filepath.Join(root, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	headerMeta := SessionHeader{
		Version: SessionFormatVersion, ID: "historical-code-header", CreatedAt: 1,
		CWD: root, AgentPreset: "code",
	}
	headerPath := store.pathFor(headerMeta)
	header, err := marshalSessionHeader(headerMeta, SessionLogOffset(headerMeta.SeedLength))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(headerPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(headerPath, append(header, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	eventMeta := SessionHeader{
		Version: SessionFormatVersion, ID: "historical-code-event", CreatedAt: 2,
		CWD: root, AgentPreset: "standard",
	}
	eventWriter, err := store.Create(context.Background(), eventMeta, SessionLogOffset(eventMeta.SeedLength))
	if err != nil {
		t.Fatal(err)
	}
	if err := eventWriter.Append(context.Background(), []Event{{
		Type: "agent-preset/selected", Seq: 0, Time: 3,
		Data: map[string]any{"agentPreset": "code"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := eventWriter.Close(); err != nil {
		t.Fatal(err)
	}
	eventPath := store.pathFor(eventMeta)
	before := map[string][]byte{}
	for name, path := range map[string]string{"header": headerPath, "event": eventPath} {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		before[name] = data
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = root, root
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = true
	reopened, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	checks := []struct {
		name, id, header, raw string
	}{
		{name: "header", id: headerMeta.ID, header: "code", raw: "code"},
		{name: "event", id: eventMeta.ID, header: "standard", raw: "code"},
	}
	for _, check := range checks {
		session, sessionErr := reopened.getSession(check.id)
		if sessionErr != nil {
			t.Fatal(sessionErr)
		}
		session.mu.Lock()
		headerPreset := session.Header.AgentPreset
		effectivePreset := sessionAgentPreset(session.Header, session.Events)
		rawPreset := sessionAgentPresetRaw(session.Header, session.Events)
		session.mu.Unlock()
		if headerPreset != check.header || rawPreset != check.raw || effectivePreset != "ptc" {
			t.Fatalf("%s historical presets header=%q raw=%q effective=%q", check.name, headerPreset, rawPreset, effectivePreset)
		}
	}
	for name, path := range map[string]string{"header": headerPath, "event": eventPath} {
		after, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if string(after) != string(before[name]) {
			t.Fatalf("%s cold load rewrote historical JSONL:\nbefore=%q\nafter=%q", name, before[name], after)
		}
		if !strings.Contains(string(after), `"agentPreset":"code"`) {
			t.Fatalf("%s historical spelling was not retained: %s", name, after)
		}
	}
}

func TestPTCCompatibilityWritesCanonicalPresetForNewSessions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = t.TempDir(), t.TempDir()
	cfg.Provider, cfg.Model = "echo", "echo"
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id, err := e.CreateSession(context.Background(), cfg.Workspace, "new-code-alias", "code")
	if err != nil {
		t.Fatal(err)
	}
	session, err := e.getSession(id)
	if err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	if session.Header.AgentPreset != "ptc" || sessionAgentPreset(session.Header, session.Events) != "ptc" {
		session.mu.Unlock()
		t.Fatalf("new session retained legacy preset: header=%q effective=%q", session.Header.AgentPreset, sessionAgentPreset(session.Header, session.Events))
	}
	session.mu.Unlock()
	store, ok := e.sessionStore.(*JSONLSessionStore)
	if !ok {
		t.Fatalf("session store = %T", e.sessionStore)
	}
	data, err := os.ReadFile(store.pathFor(session.Header))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"agentPreset":"ptc"`) || strings.Contains(string(data), `"agentPreset":"code"`) {
		t.Fatalf("new session did not persist canonical preset: %s", data)
	}

	selectionID, err := e.CreateSession(context.Background(), cfg.Workspace, "new-selection-alias", "standard")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{"sessionId": selectionID, "agentPreset": "code"})
	if err != nil {
		t.Fatal(err)
	}
	value, rpcErr := e.dispatch(context.Background(), "agentPreset.select", payload)
	if rpcErr != nil || value.(map[string]any)["agentPreset"] != "ptc" {
		t.Fatalf("legacy selection alias = %#v, %#v", value, rpcErr)
	}
	selected, err := e.getSession(selectionID)
	if err != nil {
		t.Fatal(err)
	}
	selected.mu.Lock()
	last := selected.Events[len(selected.Events)-1]
	selected.mu.Unlock()
	if last.Type != "agent-preset/selected" || last.Data.(map[string]any)["agentPreset"] != "ptc" {
		t.Fatalf("selection event retained legacy preset: %#v", last)
	}
}
