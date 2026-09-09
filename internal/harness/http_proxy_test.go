package harness

import (
	"net/http"
	"net/url"
	"testing"
)

func TestHTTPProxyPolicyParity(t *testing.T) {
	for _, name := range []string{"http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"} {
		t.Setenv(name, "")
	}
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:7897")
	t.Setenv("NO_PROXY", "example.com")
	policy := resolveHTTPProxyPolicy()
	if policy.httpProxy != "http://127.0.0.1:7897" || policy.httpsProxy != policy.httpProxy {
		t.Fatalf("policy = %#v", policy)
	}
	if policy.noProxy != "example.com,localhost,127.0.0.1,::1,[::1]" {
		t.Fatalf("noProxy = %q", policy.noProxy)
	}
	for _, raw := range []string{"http://127.0.0.53:8080", "https://app.localhost", "http://sub.example.com"} {
		target, _ := url.Parse(raw)
		proxy, err := proxyForRequest(policy, &http.Request{URL: target})
		if err != nil || proxy != nil {
			t.Fatalf("proxy for %s = %v, %v", raw, proxy, err)
		}
	}
}

func TestHTTPProxyRejectedSchemeDoesNotFallBack(t *testing.T) {
	for _, name := range []string{"http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"} {
		t.Setenv(name, "")
	}
	t.Setenv("HTTP_PROXY", "http://proxy.example:8080")
	t.Setenv("HTTPS_PROXY", "socks5://proxy.example:1080")
	policy := resolveHTTPProxyPolicy()
	if policy.httpProxy == "" || policy.httpsProxy != "" {
		t.Fatalf("policy = %#v", policy)
	}
}

func TestHTTPProxyAllProxyAndChildEnvironment(t *testing.T) {
	for _, name := range []string{"http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"} {
		t.Setenv(name, "")
	}
	t.Setenv("ALL_PROXY", "http://proxy.example:8080")
	overlay := proxyEnvironmentForChild()
	for _, name := range []string{"http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY"} {
		if overlay[name] == nil || *overlay[name] != "http://proxy.example:8080" {
			t.Fatalf("%s overlay = %#v", name, overlay[name])
		}
	}
	if overlay["NODE_USE_ENV_PROXY"] == nil || *overlay["NODE_USE_ENV_PROXY"] != "1" {
		t.Fatalf("NODE_USE_ENV_PROXY = %#v", overlay["NODE_USE_ENV_PROXY"])
	}
}
