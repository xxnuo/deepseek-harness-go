package main

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"testing"
)

type cliSyntaxContract struct {
	commands  []string
	arguments []string
	options   []string
}

var (
	cliCommandPattern  = regexp.MustCompile(`\.command\('([^']+)'`)
	cliArgumentPattern = regexp.MustCompile(`\.argument\('([^']+)'`)
	cliOptionPattern   = regexp.MustCompile(`\.(?:requiredOption|option)\('([^']+)'`)
	cliVersionPattern  = regexp.MustCompile(`\.version\([^,\n]+,\s*'([^']+)'`)
)

func TestUpstreamCLIContract(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	if _, err := os.Stat(filepath.Join(root, "deepseek-harness", ".git")); os.IsNotExist(err) {
		t.Skip("upstream checkout is absent; run make prepare to enable upstream contract tests")
	} else if err != nil {
		t.Fatal(err)
	}
	tests := map[string]cliSyntaxContract{
		"apps/cli/src/args.ts": {
			commands:  []string{"plugin", "web"},
			arguments: []string{"[args...]"},
			options:   []string{"--dump-config", "--dump-default-config", "--patch <path>", "--profile <name>", "-V, --version"},
		},
		"packages/bundle/headless/src/startup.ts": {
			arguments: []string{"[task...]"},
		},
		"packages/bundle/web-app/src/startup.ts": {
			options: []string{"--host <host>", "--port <port>", "--trusted-host <authority...>"},
		},
	}
	for path, want := range tests {
		data, err := os.ReadFile(filepath.Join(root, "deepseek-harness", filepath.FromSlash(path)))
		if err != nil {
			t.Fatal(err)
		}
		got := cliSyntaxContract{
			commands:  uniqueMatches(cliCommandPattern, data),
			arguments: uniqueMatches(cliArgumentPattern, data),
			options:   uniqueMatches(cliOptionPattern, data),
		}
		got.options = append(got.options, uniqueMatches(cliVersionPattern, data)...)
		sort.Strings(got.options)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("upstream CLI contract changed in %s\nGo:       %#v\nupstream: %#v", path, want, got)
		}
	}
}

func uniqueMatches(pattern *regexp.Regexp, data []byte) []string {
	seen := map[string]bool{}
	for _, match := range pattern.FindAllSubmatch(data, -1) {
		seen[string(match[1])] = true
	}
	if len(seen) == 0 {
		return nil
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}
