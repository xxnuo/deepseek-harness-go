package harness

import (
	"os"
	"sort"
	"strings"
)

func scrubbedChildEnv(extra map[string]string) []string {
	values := make(map[string]string, len(os.Environ())+len(extra))
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || sensitiveEnvName(key) || strings.HasPrefix(strings.ToUpper(key), "DSH_") {
			continue
		}
		values[key] = value
	}
	for key, value := range extra {
		values[key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, key := range keys {
		env = append(env, key+"="+values[key])
	}
	return env
}

func sensitiveEnvName(name string) bool {
	name = strings.ToUpper(name)
	return strings.Contains(name, "KEY") || strings.Contains(name, "PASSWORD") ||
		strings.Contains(name, "SECRET") || strings.Contains(name, "TOKEN")
}
