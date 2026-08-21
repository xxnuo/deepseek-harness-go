package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"

	harness "github.com/xxnuo/deepseek-harness-go"
)

var openWebBrowser = func(url string) error {
	program, args, err := webBrowserCommand(url)
	if err != nil {
		return err
	}
	command := exec.Command(program, args...)
	command.Env = scrubbedBrowserEnvironment()
	output, err := command.CombinedOutput()
	if err == nil {
		return nil
	}
	reason := strings.TrimSpace(string(output))
	if line, _, ok := strings.Cut(reason, "\n"); ok {
		reason = strings.TrimSpace(line)
	}
	if reason == "" {
		reason = err.Error()
	}
	return errors.New(reason)
}

func webBrowserCommand(url string) (string, []string, error) {
	switch runtime.GOOS {
	case "darwin":
		return "open", []string{url}, nil
	case "windows":
		return "rundll32.exe", []string{"url.dll,FileProtocolHandler", url}, nil
	case "linux", "freebsd", "openbsd", "netbsd", "dragonfly":
		if browser := strings.TrimSpace(os.Getenv("BROWSER")); browser != "" {
			return browser, []string{url}, nil
		}
		return "xdg-open", []string{url}, nil
	default:
		return "", nil, fmt.Errorf("no default-browser launcher for %s", runtime.GOOS)
	}
}

func scrubbedBrowserEnvironment() []string {
	values := map[string]string{}
	for _, entry := range os.Environ() {
		name, value, ok := strings.Cut(entry, "=")
		upper := strings.ToUpper(name)
		if !ok || strings.Contains(upper, "KEY") || strings.Contains(upper, "PASSWORD") || strings.Contains(upper, "SECRET") || strings.Contains(upper, "TOKEN") || strings.HasPrefix(upper, "DSH_") {
			continue
		}
		values[name] = value
	}
	keys := make([]string, 0, len(values))
	for name := range values {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, name := range keys {
		result = append(result, name+"="+values[name])
	}
	return result
}

func webLaunchedThroughSSH(environment *harness.LaunchEnvironmentSnapshot) bool {
	if environment != nil {
		for _, name := range []string{"SSH_CONNECTION", "SSH_TTY"} {
			if strings.TrimSpace(environment.Process[name]) != "" {
				return true
			}
		}
		return false
	}
	return strings.TrimSpace(os.Getenv("SSH_CONNECTION")) != "" || strings.TrimSpace(os.Getenv("SSH_TTY")) != ""
}

func (runtime *reloadableWebServer) openBrowser() {
	port := runtime.listener.Addr().(*net.TCPAddr).Port
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	fmt.Fprintln(runtime.stdout, "dsh web: opening the default browser; pass --no-open to disable")
	go func() {
		if err := openWebBrowser(url); err != nil {
			fmt.Fprintf(runtime.stderr, "web-app: could not open the default browser because %v; visit %s manually\n", err, url)
		}
	}()
}
