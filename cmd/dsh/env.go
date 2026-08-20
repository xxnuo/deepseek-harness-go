package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	harness "github.com/xxnuo/deepseek-harness-go"
)

var bootstrapEnvNames = map[string]bool{
	"PATH": true, "HOME": true, "USERPROFILE": true, "SHELL": true,
	"NODE_OPTIONS": true, "NODE_PATH": true, "NODE_EXTRA_CA_CERTS": true,
	"LD_PRELOAD": true, "LD_LIBRARY_PATH": true, "LD_AUDIT": true,
	"BASH_ENV": true, "ENV": true, "SHELLOPTS": true, "BASHOPTS": true,
	"PERL5OPT": true, "PERL5LIB": true, "PYTHONSTARTUP": true, "PYTHONPATH": true,
	"RUBYOPT": true, "RUBYLIB": true, "JAVA_TOOL_OPTIONS": true,
	"_JAVA_OPTIONS": true, "JDK_JAVA_OPTIONS": true, "PYTHONHOME": true,
	"GIT_SSH": true, "GIT_SSH_COMMAND": true, "GIT_EXTERNAL_DIFF": true,
	"GIT_PAGER": true, "GIT_EDITOR": true, "GIT_ASKPASS": true, "SSH_ASKPASS": true,
	"GIT_CONFIG_GLOBAL": true, "GIT_CONFIG_SYSTEM": true, "GIT_CONFIG_COUNT": true,
	"EDITOR": true, "VISUAL": true, "PAGER": true,
	"DEEPSEEK_BASE_URL": true, "DEEPSEEK_SEARCH_BASE_URL": true,
	"SSL_CERT_FILE": true, "SSL_CERT_DIR": true, "HTTP_PROXY": true,
	"HTTPS_PROXY": true, "ALL_PROXY": true, "NO_PROXY": true,
	"REQUESTS_CA_BUNDLE": true, "CURL_CA_BUNDLE": true,
	"NODE_TLS_REJECT_UNAUTHORIZED": true,
}

var bootstrapEnvPrefixes = []string{"DSH_", "XDG_", "DYLD_", "BASH_FUNC_"}

type envLayer struct {
	path   string
	values map[string]string
}

func loadLayeredEnv(binName string, stderr io.Writer) (*harness.LaunchEnvironmentSnapshot, func(), error) {
	process := environmentValues(os.Environ())
	cwd, err := os.Getwd()
	if err != nil {
		return nil, nil, err
	}
	home, err := resolveDshHome()
	if err != nil {
		return nil, nil, err
	}
	project, err := readEnvLayer(binName, cwd, stderr)
	if err != nil {
		return nil, nil, err
	}
	var user *envLayer
	if filepath.Clean(home) != filepath.Clean(cwd) {
		user, err = readEnvLayer(binName, home, stderr)
		if err != nil {
			return nil, nil, err
		}
	}

	applied := make([]string, 0)
	for _, layer := range []*envLayer{project, user} {
		if layer == nil {
			continue
		}
		for name, value := range layer.values {
			if _, exists := os.LookupEnv(name); exists {
				continue
			}
			if err := os.Setenv(name, value); err != nil {
				for _, previous := range applied {
					_ = os.Unsetenv(previous)
				}
				return nil, nil, err
			}
			applied = append(applied, name)
		}
	}
	snapshot := &harness.LaunchEnvironmentSnapshot{Process: process}
	if project != nil {
		snapshot.Project = project.values
	}
	if user != nil {
		snapshot.User = user.values
	}
	return snapshot, func() {
		for _, name := range applied {
			_ = os.Unsetenv(name)
		}
	}, nil
}

func environmentValues(entries []string) map[string]string {
	values := make(map[string]string, len(entries))
	for _, entry := range entries {
		name, value, ok := strings.Cut(entry, "=")
		if ok {
			values[name] = value
		}
	}
	return values
}

func readEnvLayer(binName, dir string, stderr io.Writer) (*envLayer, error) {
	path := filepath.Join(dir, ".env")
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		fmt.Fprintf(stderr, "%s: failed to load .env: %v\n", binName, err)
		return nil, nil
	}
	values, err := parseEnv(data)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to parse %s: %w", binName, path, err)
	}
	for name := range values {
		upper := strings.ToUpper(name)
		blocked := bootstrapEnvNames[upper]
		for _, prefix := range bootstrapEnvPrefixes {
			blocked = blocked || strings.HasPrefix(upper, prefix)
		}
		if blocked {
			return nil, fmt.Errorf("%s: %s sets %q, which only the launching environment may set (it decides how this process starts, where its code and instructions load from, or how it reaches the network); export %s instead of putting it in a .env file", binName, path, name, name)
		}
	}
	return &envLayer{path: path, values: values}, nil
}

func parseEnv(data []byte) (map[string]string, error) {
	values := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		name, raw, ok := strings.Cut(line, "=")
		name = strings.TrimSpace(name)
		if !ok || !validEnvName(name) {
			return nil, fmt.Errorf("line %d: invalid environment assignment", lineNumber)
		}
		value, err := parseEnvValue(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		values[name] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func validEnvName(name string) bool {
	for index, char := range name {
		if char == '_' || unicode.IsLetter(char) || index > 0 && unicode.IsDigit(char) {
			continue
		}
		return false
	}
	return name != ""
}

func parseEnvValue(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if raw[0] != '\'' && raw[0] != '"' {
		if before, _, ok := strings.Cut(raw, "#"); ok {
			raw = before
		}
		return strings.TrimSpace(raw), nil
	}
	quote := raw[0]
	if len(raw) < 2 || raw[len(raw)-1] != quote {
		return "", errors.New("unterminated quoted value")
	}
	body := raw[1 : len(raw)-1]
	if quote == '\'' {
		return body, nil
	}
	var value strings.Builder
	escaped := false
	for _, char := range body {
		if !escaped {
			if char == '\\' {
				escaped = true
				continue
			}
			value.WriteRune(char)
			continue
		}
		escaped = false
		switch char {
		case 'n':
			value.WriteByte('\n')
		case 'r':
			value.WriteByte('\r')
		case 't':
			value.WriteByte('\t')
		default:
			value.WriteRune(char)
		}
	}
	if escaped {
		value.WriteByte('\\')
	}
	return value.String(), nil
}
