package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	harness "github.com/xxnuo/deepseek-harness-go"
)

func TestSDKProfileServerOptions(t *testing.T) {
	t.Run("sdk-default", func(t *testing.T) {
		t.Setenv("DSH_HOME", t.TempDir())
		unsetTestEnv(t, "DSH_MAX_TOKENS_AS_SUCCESS")
		composed := composeSDKProfile(t, "sdk")
		if !composed.sdkServer.MaxTokensAsSuccess {
			t.Fatal("sdk profile did not default maxTokensAsSuccess to true")
		}
	})

	t.Run("sdk-environment-false", func(t *testing.T) {
		t.Setenv("DSH_HOME", t.TempDir())
		t.Setenv("DSH_MAX_TOKENS_AS_SUCCESS", "false")
		composed := composeSDKProfile(t, "sdk")
		if composed.sdkServer.MaxTokensAsSuccess {
			t.Fatal("sdk profile ignored DSH_MAX_TOKENS_AS_SUCCESS=false")
		}
	})

	t.Run("sdk-minimal-literal-false", func(t *testing.T) {
		t.Setenv("DSH_HOME", t.TempDir())
		t.Setenv("DSH_MAX_TOKENS_AS_SUCCESS", "true")
		composed := composeSDKProfile(t, "sdk-minimal")
		if composed.sdkServer.MaxTokensAsSuccess {
			t.Fatal("sdk-minimal profile did not retain its literal false mapping")
		}
	})

	t.Run("missing-config-defaults-false", func(t *testing.T) {
		composed := testComposition(t, `
- id: sdk-jsonrpc-server
  name: '@deepseek-ai/dsh-sdk-jsonrpc-server'
`)
		if err := composed.validate(); err != nil {
			t.Fatal(err)
		}
		if composed.sdkServer.MaxTokensAsSuccess {
			t.Fatal("missing maxTokensAsSuccess did not preserve the server default")
		}
	})
}

func TestSDKProfileRejectsInvalidServerOptions(t *testing.T) {
	for _, test := range []struct {
		name, value, want string
	}{
		{name: "non-boolean", value: "yes", want: "must be a boolean"},
		{name: "invalid-json-environment", value: `!!js "JSON.parse(process.env.DSH_BAD_SDK_OPTION)"`, want: "!!js evaluation failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("DSH_BAD_SDK_OPTION", "not-json")
			composed := testComposition(t, `
- id: sdk-jsonrpc-server
  name: '@deepseek-ai/dsh-sdk-jsonrpc-server'
  config:
    maxTokensAsSuccess: `+test.value+`
`)
			err := composed.validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validate() = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRunSDKHelpDoesNotStartRuntime(t *testing.T) {
	var stdout, stderr bytes.Buffer
	composed := testComposition(t, `
- id: sdk-app-startup
  name: '@deepseek-ai/dsh-sdk-app'
  config: {profile: sdk-minimal}
`)
	if err := runSDK([]string{"--help"}, composed, harness.Config{}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if text := stdout.String(); !strings.Contains(text, "Usage: dsh --profile sdk-minimal") || !strings.Contains(text, "stdio JSON-RPC") {
		t.Fatalf("SDK help = %q", text)
	}
	if stderr.Len() != 0 {
		t.Fatalf("SDK help stderr = %q", stderr.String())
	}
}

func composeSDKProfile(t *testing.T, name string) *composition {
	t.Helper()
	loader, err := newProfileLoader(true)
	if err != nil {
		t.Fatal(err)
	}
	composed, err := loader.compose(name, nil, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	return composed
}

func unsetTestEnv(t *testing.T, name string) {
	t.Helper()
	value, present := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if present {
			_ = os.Setenv(name, value)
		} else {
			_ = os.Unsetenv(name)
		}
	})
}
