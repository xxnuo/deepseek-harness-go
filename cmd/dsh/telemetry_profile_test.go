package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	harness "github.com/xxnuo/deepseek-harness-go"
)

func TestSessionTelemetryProfileMapsExporterAndProcessor(t *testing.T) {
	composed := mcpTestComposition(t, `
- id: telemetry
  name: '@deepseek-ai/dsh-session-telemetry-otel'
  config:
    mode: FULL
    shutdownTimeoutMillis: 3210
    exporter:
      url: http://127.0.0.1:4318/v1/logs
      headers: {authorization: Bearer-test}
      compression: gzip
      timeoutMillis: 1200
      keepAlive: false
      concurrencyLimit: 3
      userAgent: custom-agent
    processor:
      scheduledDelayMillis: 101
      maxQueueSize: 202
      maxExportBatchSize: 203
      exportTimeoutMillis: 404
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	telemetry := cfg.SessionTelemetry
	if telemetry == nil || telemetry.Mode != harness.SessionTelemetryModeFull || telemetry.ShutdownTimeout != 3210*time.Millisecond {
		t.Fatalf("telemetry = %#v", telemetry)
	}
	if telemetry.Exporter.URL != "http://127.0.0.1:4318/v1/logs" || telemetry.Exporter.Headers["authorization"] != "Bearer-test" || telemetry.Exporter.Compression != "gzip" || telemetry.Exporter.Timeout != 1200*time.Millisecond {
		t.Fatalf("exporter = %#v", telemetry.Exporter)
	}
	if telemetry.Exporter.KeepAlive == nil || *telemetry.Exporter.KeepAlive || telemetry.Exporter.ConcurrencyLimit != 3 || telemetry.Exporter.UserAgent != "custom-agent" {
		t.Fatalf("exporter options = %#v", telemetry.Exporter)
	}
	if telemetry.Processor.ScheduledDelay != 101*time.Millisecond || telemetry.Processor.MaxQueueSize != 202 || telemetry.Processor.MaxExportBatchSize != 203 || telemetry.Processor.ExportTimeout != 404*time.Millisecond {
		t.Fatalf("processor = %#v", telemetry.Processor)
	}
}

func TestSessionTelemetryProfileDisabledEnvironmentUnmountsService(t *testing.T) {
	t.Setenv("DSH_TELEMETRY_DISABLED", "0")
	composed := mcpTestComposition(t, `
- id: telemetry
  name: '@deepseek-ai/dsh-session-telemetry-otel'
  config: {mode: FULL}
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	if cfg := engineConfig(&profileLoader{}, composed); cfg.SessionTelemetry != nil {
		t.Fatalf("telemetry = %#v", cfg.SessionTelemetry)
	}
}

func TestSessionTelemetryCLIReportsDisabledFeedback(t *testing.T) {
	composed := mcpTestComposition(t, `
- id: telemetry
  name: '@deepseek-ai/dsh-session-telemetry-otel'
  config: {mode: DISABLED}
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	cfg.Persist, cfg.Terminal.Disabled = false, true
	var stderr bytes.Buffer
	attachSessionTelemetryWarnings(&cfg, &stderr)
	engine, err := harness.New(harness.WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	id, err := engine.CreateSession(context.Background(), t.TempDir(), "", "")
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Prompt(context.Background(), id, harness.PromptRequest{Content: []harness.PromptContentPart{{Type: "text", Text: "/feedback useful"}}})
	if err != nil || result.Command == nil || result.Command.Kind != "success" {
		t.Fatalf("feedback result = %#v, err = %v", result, err)
	}
	if !strings.Contains(stderr.String(), "session telemetry is DISABLED; nothing will be shared and this feedback remains local") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestSessionTelemetryProfileRejectsInvalidConfig(t *testing.T) {
	for name, source := range map[string]string{
		"mode": `
- id: telemetry
  name: '@deepseek-ai/dsh-session-telemetry-otel'
  config: {mode: INVALID}
`,
		"batch size": `
- id: telemetry
  name: '@deepseek-ai/dsh-session-telemetry-otel'
  config: {mode: FULL, exporter: {url: http://collector/v1/logs}, processor: {maxExportBatchSize: 0}}
`,
		"duplicate": `
- id: telemetry-one
  name: '@deepseek-ai/dsh-session-telemetry-otel'
  config: {mode: DISABLED}
- id: telemetry-two
  name: '@deepseek-ai/dsh-session-telemetry-otel'
  config: {mode: DISABLED}
`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := mcpTestComposition(t, source).validate(); err == nil {
				t.Fatal("invalid telemetry config was accepted")
			}
		})
	}

	composed := mcpTestComposition(t, `
- id: telemetry
  name: '@deepseek-ai/dsh-session-telemetry-otel'
  config: {mode: FULL}
`)
	if err := composed.validate(); err != nil {
		t.Fatal(err)
	}
	cfg := engineConfig(&profileLoader{home: t.TempDir()}, composed)
	cfg.Persist, cfg.Terminal.Disabled = false, true
	if _, err := harness.New(harness.WithConfig(cfg)); err == nil || !strings.Contains(err.Error(), "exporter.url is required") {
		t.Fatalf("missing endpoint error = %v", err)
	}
}
