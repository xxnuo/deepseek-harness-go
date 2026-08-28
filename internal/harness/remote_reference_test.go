package harness

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReferenceDiscoveryRemotesUseRealWorkspaceAndSessions(t *testing.T) {
	e := newIntegrationEngine(t)
	if err := os.MkdirAll(filepath.Join(e.Config().Workspace, "src"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(e.Config().Workspace, "src", "main.go"), []byte("package main"), 0o600); err != nil {
		t.Fatal(err)
	}
	targetID, err := e.CreateSession(t.Context(), e.Config().Workspace, "reference-remote-target", "")
	if err != nil {
		t.Fatal(err)
	}
	sourceID, err := e.CreateSession(t.Context(), e.Config().Workspace, "reference-remote-source", "")
	if err != nil {
		t.Fatal(err)
	}
	source, _ := e.getSession(sourceID)
	if _, err := e.appendEvent(source, "session/title", map[string]any{
		"title": "Source title", "messageSeqs": []any{}, "source": map[string]any{"kind": "fallback"},
	}); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	status, envelope := postRPC(t, server.Client(), server.URL, "", "fileReferences/list", map[string]any{
		"args": map[string]any{"agentId": targetID, "query": "main"},
	})
	files, ok := remoteValue(t, envelope).([]any)
	if status != http.StatusOK || !ok || len(files) != 1 || files[0].(map[string]any)["path"] != "src/main.go" {
		t.Fatalf("fileReferences/list = %d %#v", status, envelope)
	}

	status, envelope = postRPC(t, server.Client(), server.URL, "", "sessionReferenceResolver/candidates", map[string]any{
		"args": map[string]any{"agentId": targetID, "query": "source"},
	})
	sessions, ok := remoteValue(t, envelope).([]any)
	if status != http.StatusOK || !ok || len(sessions) != 1 {
		t.Fatalf("sessionReferenceResolver/candidates = %d %#v", status, envelope)
	}
	candidate := sessions[0].(map[string]any)
	if candidate["sessionId"] != sourceID || candidate["label"] != "Source title" ||
		!strings.HasPrefix(candidate["mention"].(string), "@[Source title](dsh-session:") {
		t.Fatalf("session candidate = %#v", candidate)
	}
}

func TestCommandsRemoteAdmitsImagesForGoalAndPlan(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "command-images", "")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)
	image := map[string]any{
		"mediaType": "image/png",
		"data":      b64(attachmentFixtureBytes(t)["image/png"]),
		"name":      "diagram.png",
	}

	_, envelope := postRPC(t, server.Client(), server.URL, "", "commands/execute", map[string]any{
		"args": map[string]any{"agentId": id, "line": "/clear", "images": []any{image}},
	})
	refused := remoteValue(t, envelope).(map[string]any)["result"].(map[string]any)
	if refused["kind"] != "error" || refused["text"] != "/clear does not accept image attachments" {
		t.Fatalf("non-image command result = %#v", refused)
	}

	_, envelope = postRPC(t, server.Client(), server.URL, "", "commands/execute", map[string]any{
		"args": map[string]any{"agentId": id, "line": "/goal", "images": []any{image}},
	})
	bareGoal := remoteValue(t, envelope).(map[string]any)["result"].(map[string]any)
	if bareGoal["kind"] != "error" || !strings.Contains(bareGoal["text"].(string), "only accompany a goal objective") {
		t.Fatalf("bare goal result = %#v", bareGoal)
	}

	_, envelope = postRPC(t, server.Client(), server.URL, "", "commands/execute", map[string]any{
		"args": map[string]any{"agentId": id, "line": "/plan", "images": []any{image}},
	})
	plan := remoteValue(t, envelope).(map[string]any)["result"].(map[string]any)
	if plan["kind"] != "success" {
		t.Fatalf("image-only plan result = %#v", plan)
	}
	session, _ := e.getSession(id)
	session.mu.Lock()
	pending := append([]*queuedPrompt(nil), session.pending...)
	session.mu.Unlock()
	if len(pending) != 1 || len(pending[0].content) != 1 || pending[0].content[0].Type != "image" ||
		pending[0].content[0].Attachment == nil {
		t.Fatalf("image-only plan queue = %#v", pending)
	}

	_, envelope = postRPC(t, server.Client(), server.URL, "", "commands/execute", map[string]any{
		"args": map[string]any{"agentId": id, "line": "/plan off", "images": []any{image}},
	})
	planOff := remoteValue(t, envelope).(map[string]any)["result"].(map[string]any)
	if planOff["kind"] != "error" || planOff["text"] != "Image attachments cannot accompany /plan off." {
		t.Fatalf("image plan off result = %#v", planOff)
	}
}
