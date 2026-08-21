package harness_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	harness "github.com/xxnuo/deepseek-harness-go"
	core "github.com/xxnuo/deepseek-harness-go/internal/harness"
)

func TestPublicFacadeInitialVariablesMatchImplementation(t *testing.T) {
	if harness.ErrEngineClosed != core.ErrEngineClosed ||
		!reflect.DeepEqual(harness.DefaultFileReferenceExcludedDirectories, core.DefaultFileReferenceExcludedDirectories) ||
		!reflect.DeepEqual(harness.PythonWireFrameFields, core.PythonWireFrameFields) {
		t.Fatal("public facade variables do not match the implementation defaults")
	}
}

func TestPublicLibraryRoundTrip(t *testing.T) {
	e, err := harness.New(
		harness.WithDataDir(t.TempDir()),
		harness.WithWorkspace(t.TempDir()),
		harness.WithProvider("echo"),
		harness.WithModel("echo"),
		harness.WithPersistence(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "library-session", "")
	if err != nil {
		t.Fatal(err)
	}
	output, err := e.Run(context.Background(), id, harness.PromptRequest{
		Mode:    "queue",
		Literal: true,
		Content: []harness.PromptContentPart{{Type: "text", Text: "library round trip"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if output != "library round trip" {
		t.Fatalf("output = %q", output)
	}
}

func TestPublicLibraryServesCustomFrontend(t *testing.T) {
	frontend := t.TempDir()
	if err := os.WriteFile(filepath.Join(frontend, "index.html"), []byte("<!doctype html><title>custom harness</title>"), 0o600); err != nil {
		t.Fatal(err)
	}
	e, err := harness.New(
		harness.WithDataDir(t.TempDir()),
		harness.WithWorkspace(t.TempDir()),
		harness.WithFrontendDir(frontend),
		harness.WithProvider("echo"),
		harness.WithModel("echo"),
		harness.WithPersistence(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	server := httptest.NewServer(e.Handler())
	defer server.Close()
	response, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), "custom harness") {
		t.Fatalf("custom frontend response = %d, %q, %v", response.StatusCode, body, readErr)
	}
}
