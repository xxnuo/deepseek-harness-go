package harness

import (
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
)

var loopbackNoProxy = []string{"localhost", "127.0.0.1", "::1", "[::1]"}

type httpProxyPolicy struct {
	httpProxy  string
	httpsProxy string
	noProxy    string
}

type proxyCandidate struct {
	value    string
	present  bool
	accepted bool
}

func proxyEnvironmentValue(lower string) (string, string, bool) {
	for _, name := range []string{lower, strings.ToUpper(lower)} {
		if value, exists := os.LookupEnv(name); exists && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value), name, true
		}
	}
	return "", "", false
}

func parseProxyCandidate(lower string) proxyCandidate {
	value, _, present := proxyEnvironmentValue(lower)
	if !present {
		return proxyCandidate{}
	}
	parsed, err := url.Parse(value)
	accepted := err == nil && parsed.Host != "" && (parsed.Scheme == "http" || parsed.Scheme == "https")
	return proxyCandidate{value: value, present: true, accepted: accepted}
}

func resolveHTTPProxyPolicy() httpProxyPolicy {
	all := parseProxyCandidate("all_proxy")
	httpCandidate := parseProxyCandidate("http_proxy")
	httpsCandidate := parseProxyCandidate("https_proxy")
	policy := httpProxyPolicy{}
	if httpCandidate.accepted {
		policy.httpProxy = httpCandidate.value
	} else if !httpCandidate.present && all.accepted {
		policy.httpProxy = all.value
	}
	if httpsCandidate.accepted {
		policy.httpsProxy = httpsCandidate.value
	} else if !httpsCandidate.present {
		if all.accepted {
			policy.httpsProxy = all.value
		} else {
			policy.httpsProxy = policy.httpProxy
		}
	}
	if policy.httpProxy != "" || policy.httpsProxy != "" {
		value, _, _ := proxyEnvironmentValue("no_proxy")
		policy.noProxy = mergeLoopbackNoProxy(value)
	}
	return policy
}

func mergeLoopbackNoProxy(value string) string {
	entries := strings.FieldsFunc(value, func(char rune) bool {
		return char == ',' || char == ' ' || char == '\t' || char == '\n' || char == '\r'
	})
	for _, entry := range entries {
		if entry == "*" {
			return "*"
		}
	}
	present := make(map[string]bool, len(entries))
	for _, entry := range entries {
		present[strings.ToLower(entry)] = true
	}
	for _, entry := range loopbackNoProxy {
		if !present[strings.ToLower(entry)] {
			entries = append(entries, entry)
		}
	}
	return strings.Join(entries, ",")
}

func isLoopbackProxyHost(hostname string) bool {
	host := strings.ToLower(strings.TrimSuffix(strings.Trim(hostname, "[]"), "."))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || host == "0.0.0.0" || host == "::" {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && (address.IsLoopback() || address.IsUnspecified())
}

func proxyBypassed(noProxy string, target *url.URL) bool {
	host := strings.ToLower(strings.TrimSuffix(strings.Trim(target.Hostname(), "[]"), "."))
	port := target.Port()
	if port == "" {
		if target.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	for _, raw := range strings.FieldsFunc(noProxy, func(char rune) bool {
		return char == ',' || char == ' ' || char == '\t' || char == '\n' || char == '\r'
	}) {
		entry := strings.ToLower(strings.TrimSpace(raw))
		if entry == "" {
			continue
		}
		if entry == "*" {
			return true
		}
		candidateHost, candidatePort := splitProxyBypassEntry(entry)
		if candidateHost == "" || candidatePort != "" && candidatePort != port {
			continue
		}
		candidateHost = strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(candidateHost, "*."), "."), ".")
		if candidateHost != "" && (host == candidateHost || strings.HasSuffix(host, "."+candidateHost)) {
			return true
		}
	}
	return false
}

func splitProxyBypassEntry(entry string) (string, string) {
	if strings.HasPrefix(entry, "[") {
		if close := strings.IndexByte(entry, ']'); close >= 0 {
			rest := entry[close+1:]
			if strings.HasPrefix(rest, ":") {
				return entry[1:close], rest[1:]
			}
			return entry[1:close], ""
		}
	}
	if strings.Count(entry, ":") == 1 {
		host, port, _ := strings.Cut(entry, ":")
		return host, port
	}
	return entry, ""
}

func proxyForRequest(policy httpProxyPolicy, request *http.Request) (*url.URL, error) {
	if request == nil || request.URL == nil || isLoopbackProxyHost(request.URL.Hostname()) || proxyBypassed(policy.noProxy, request.URL) {
		return nil, nil
	}
	value := ""
	switch request.URL.Scheme {
	case "http":
		value = policy.httpProxy
	case "https":
		value = policy.httpsProxy
	}
	if value == "" {
		return nil, nil
	}
	return url.Parse(value)
}

func proxyHTTPTransport() *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		base = &http.Transport{}
	}
	transport := base.Clone()
	policy := resolveHTTPProxyPolicy()
	transport.Proxy = func(request *http.Request) (*url.URL, error) {
		return proxyForRequest(policy, request)
	}
	return transport
}

func newHTTPClient() *http.Client {
	return &http.Client{Transport: proxyHTTPTransport()}
}

func proxyEnvironmentForChild() map[string]*string {
	policy := resolveHTTPProxyPolicy()
	if policy.httpProxy == "" && policy.httpsProxy == "" {
		return nil
	}
	overlay := map[string]*string{}
	allSupported := true
	for _, spec := range []struct {
		lower    string
		resolved string
	}{{"http_proxy", policy.httpProxy}, {"https_proxy", policy.httpsProxy}} {
		_, _, named := proxyEnvironmentValue(spec.lower)
		if named {
			for _, name := range []string{spec.lower, strings.ToUpper(spec.lower)} {
				if value, exists := os.LookupEnv(name); exists {
					copied := value
					overlay[name] = &copied
					if candidate := parseProxyValue(value); !candidate {
						allSupported = false
					}
				} else {
					overlay[name] = nil
				}
			}
			continue
		}
		for _, name := range []string{spec.lower, strings.ToUpper(spec.lower)} {
			if spec.resolved == "" {
				overlay[name] = nil
			} else {
				copied := spec.resolved
				overlay[name] = &copied
			}
		}
	}
	noProxy := policy.noProxy
	for _, name := range []string{"no_proxy", "NO_PROXY"} {
		overlay[name] = &noProxy
	}
	if allSupported {
		value := "1"
		overlay["NODE_USE_ENV_PROXY"] = &value
	}
	return overlay
}

func parseProxyValue(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	return err == nil && parsed.Host != "" && (parsed.Scheme == "http" || parsed.Scheme == "https")
}
