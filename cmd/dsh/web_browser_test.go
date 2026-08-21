package main

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	harness "github.com/xxnuo/deepseek-harness-go"
)

func TestWebBrowserHandoffUsesProcessLaunchContext(t *testing.T) {
	if webLaunchedThroughSSH(&harness.LaunchEnvironmentSnapshot{
		Process: map[string]string{"SSH_CONNECTION": ""},
		Project: map[string]string{"SSH_TTY": "/dev/pts/9"},
	}) {
		t.Fatal("project .env SSH marker must not suppress the browser handoff")
	}
	if !webLaunchedThroughSSH(&harness.LaunchEnvironmentSnapshot{
		Process: map[string]string{"SSH_CONNECTION": "10.0.0.2 50000 10.0.0.9 22"},
	}) {
		t.Fatal("inherited SSH launch was not detected")
	}
}

func TestBrowserEnvironmentOmitsHarnessCredentials(t *testing.T) {
	t.Setenv("DSH_BROWSER_TEST_TOKEN", "secret")
	t.Setenv("DEEPSEEK_API_KEY", "secret")
	t.Setenv("DISPLAY", ":99")
	values := map[string]string{}
	for _, entry := range scrubbedBrowserEnvironment() {
		name, value, ok := strings.Cut(entry, "=")
		if ok {
			values[name] = value
		}
	}
	if values["DSH_BROWSER_TEST_TOKEN"] != "" || values["DEEPSEEK_API_KEY"] != "" {
		t.Fatal("browser launcher inherited Harness credentials")
	}
	if values["DISPLAY"] != ":99" {
		t.Fatalf("browser launcher DISPLAY = %q", values["DISPLAY"])
	}
}

func TestOpenWebBrowserRunsRealScrubbedLauncher(t *testing.T) {
	switch runtime.GOOS {
	case "linux", "freebsd", "openbsd", "netbsd", "dragonfly":
	default:
		t.Skip("BROWSER launcher override is Unix-only")
	}
	directory := t.TempDir()
	record := filepath.Join(directory, "record")
	launcher := filepath.Join(directory, "browser")
	source := "#!/bin/sh\nprintf '%s\\n%s\\n%s\\n' \"$1\" \"${DEEPSEEK_API_KEY-unset}\" \"${DSH_HOME-unset}\" > \"$OPEN_RECORD\"\n"
	if err := os.WriteFile(launcher, []byte(source), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BROWSER", launcher)
	t.Setenv("OPEN_RECORD", record)
	t.Setenv("DEEPSEEK_API_KEY", "must-not-reach-browser")
	t.Setenv("DSH_HOME", "/must-not-reach-browser")
	const url = "http://127.0.0.1:4567"
	if err := openWebBrowser(url); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), url+"\nunset\nunset\n"; got != want {
		t.Fatalf("browser launcher record = %q, want %q", got, want)
	}
}

func TestReloadableWebServerBrowserHandoffIsNonFatal(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var stdout, stderr synchronizedBuffer
	runtime := &reloadableWebServer{listener: listener, stdout: &stdout, stderr: &stderr}
	original := openWebBrowser
	t.Cleanup(func() { openWebBrowser = original })
	opened := make(chan string, 1)
	openWebBrowser = func(url string) error {
		opened <- url
		return errors.New("desktop unavailable")
	}
	runtime.openBrowser()
	url := <-opened
	if !strings.HasPrefix(url, "http://127.0.0.1:") {
		t.Fatalf("browser URL = %q", url)
	}
	waitFor(t, func() bool { return strings.Contains(stderr.String(), "desktop unavailable; visit "+url+" manually") })
	if !strings.Contains(stdout.String(), "opening the default browser; pass --no-open to disable") {
		t.Fatalf("browser handoff output = %q", stdout.String())
	}
}
