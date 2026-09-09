package harness

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingTelemetrySink struct {
	mu       sync.Mutex
	records  []SessionTelemetryRecord
	shutdown int
}

type telemetryBlockingProvider struct{ started chan struct{} }

func (p *telemetryBlockingProvider) ID() string   { return "telemetry-blocking" }
func (p *telemetryBlockingProvider) Name() string { return "Telemetry Blocking" }
func (p *telemetryBlockingProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: p.ID(), Name: p.Name()}}, nil
}
func (p *telemetryBlockingProvider) Complete(ctx context.Context, _ ChatRequest, _ func(Delta) error) (Completion, error) {
	select {
	case <-p.started:
	default:
		close(p.started)
	}
	<-ctx.Done()
	return Completion{}, ctx.Err()
}

func (s *recordingTelemetrySink) Emit(_ context.Context, record SessionTelemetryRecord) error {
	s.mu.Lock()
	s.records = append(s.records, record)
	s.mu.Unlock()
	return nil
}

func (s *recordingTelemetrySink) Shutdown(context.Context) error {
	s.mu.Lock()
	s.shutdown++
	s.mu.Unlock()
	return nil
}

func (s *recordingTelemetrySink) snapshot() ([]SessionTelemetryRecord, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SessionTelemetryRecord(nil), s.records...), s.shutdown
}

func newTelemetryTestEngine(t *testing.T, mode SessionTelemetryMode, sink SessionTelemetrySink, onError func(error)) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.Terminal.Disabled = true
	cfg.SessionTelemetry = &SessionTelemetryConfig{Mode: mode, Sink: sink, OnError: onError}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func TestSessionTelemetryFullCaptureProjectionAndShutdown(t *testing.T) {
	sink := &recordingTelemetrySink{}
	engine := newTelemetryTestEngine(t, SessionTelemetryModeFull, sink, nil)
	id, err := engine.CreateSession(context.Background(), t.TempDir(), "telemetry-full", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	if _, err := engine.appendEvent(session, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	firstChunk := map[string]any{"turn": 1, "step": 1, "chunk": map[string]any{"type": "text-delta", "index": 0, "text": "first"}}
	if _, err := engine.appendEvent(session, "assistant/chunk", firstChunk); err != nil {
		t.Fatal(err)
	}
	firstChunk["turn"] = 99
	if _, err := engine.appendEvent(session, "assistant/chunk", map[string]any{"turn": 1, "step": 1, "chunk": map[string]any{"type": "text-delta", "index": 0, "text": "second"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEvent(session, "tool/result", map[string]any{
		"turn": 1, "step": 1,
		"message": toolResultMessage("call", []ContentBlock{{Type: "text", Text: "failed"}}, true),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEvent(session, "turn/end", map[string]any{"turn": 1, "reason": map[string]any{"kind": "error"}}); err != nil {
		t.Fatal(err)
	}
	engine.telemetry.relayAgentError(session, 1, 1, context.DeadlineExceeded)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	records, shutdowns := sink.snapshot()
	if shutdowns != 1 {
		t.Fatalf("shutdown calls = %d", shutdowns)
	}
	var eventTypes []string
	chunkCount, errorEvents, ops := 0, 0, 0
	for _, record := range records {
		if record.Channel == SessionTelemetryChannelOps {
			ops++
			continue
		}
		eventType, _ := record.Attributes["event.type"].(string)
		eventTypes = append(eventTypes, eventType)
		if eventType == "assistant/chunk" {
			chunkCount++
			body := record.Body.(map[string]any)
			if body["turn"] != json.Number("1") {
				t.Fatalf("chunk body was not isolated: %#v", body)
			}
		}
		if record.Severity == SessionTelemetrySeverityError {
			errorEvents++
		}
	}
	if strings.Join(eventTypes, ",") != "turn/start,assistant/chunk,tool/result,turn/end" {
		t.Fatalf("ledger types = %v", eventTypes)
	}
	if chunkCount != 1 || errorEvents != 2 || ops != 2 {
		t.Fatalf("chunk=%d errorEvents=%d ops=%d records=%#v", chunkCount, errorEvents, ops, records)
	}
}

func TestSessionTelemetryFeedbackOnlyAndDisabled(t *testing.T) {
	t.Run("feedback only", func(t *testing.T) {
		sink := &recordingTelemetrySink{}
		engine := newTelemetryTestEngine(t, SessionTelemetryModeFeedbackOnly, sink, nil)
		id, err := engine.CreateSession(context.Background(), t.TempDir(), "feedback-only", "")
		if err != nil {
			t.Fatal(err)
		}
		session, _ := engine.getSession(id)
		_, _ = engine.appendEvent(session, "turn/start", map[string]any{"turn": 1})
		if records, _ := sink.snapshot(); len(records) != 0 {
			t.Fatalf("live records = %#v", records)
		}
		_, _ = engine.appendEvent(session, "feedback/record", map[string]any{"text": "first"})
		_, _ = engine.appendEvent(session, "turn/end", map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}})
		_, _ = engine.appendEvent(session, "feedback/record", map[string]any{"text": "second"})
		if err := engine.EmitSessionTelemetry(context.Background(), SessionTelemetryRecord{
			Channel: SessionTelemetryChannelLedger, Time: time.Now().UnixMilli(), Severity: SessionTelemetrySeverityInfo,
			Attributes: map[string]any{"session.id": id}, Body: map[string]any{"ignored": true},
		}); err != nil {
			t.Fatal(err)
		}
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
		records, shutdowns := sink.snapshot()
		var types []string
		for _, record := range records {
			types = append(types, record.Attributes["event.type"].(string))
		}
		if strings.Join(types, ",") != "turn/start,feedback/record,turn/end,feedback/record" || shutdowns != 1 {
			t.Fatalf("types=%v shutdowns=%d", types, shutdowns)
		}
	})

	t.Run("disabled", func(t *testing.T) {
		sink := &recordingTelemetrySink{}
		var warnings []string
		engine := newTelemetryTestEngine(t, SessionTelemetryModeDisabled, sink, func(err error) {
			warnings = append(warnings, err.Error())
		})
		id, err := engine.CreateSession(context.Background(), t.TempDir(), "disabled", "")
		if err != nil {
			t.Fatal(err)
		}
		session, _ := engine.getSession(id)
		_, _ = engine.appendEvent(session, "feedback/record", map[string]any{"text": "local"})
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
		records, shutdowns := sink.snapshot()
		if len(records) != 0 || shutdowns != 0 || len(warnings) != 1 || !strings.Contains(warnings[0], "not uploaded through OpenTelemetry") {
			t.Fatalf("records=%#v shutdowns=%d warnings=%v", records, shutdowns, warnings)
		}
	})
}

func TestSessionTelemetryOTLPHTTPJSONWire(t *testing.T) {
	type capture struct {
		header http.Header
		body   map[string]any
	}
	var mu sync.Mutex
	var captures []capture
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var reader io.Reader = request.Body
		if request.Header.Get("Content-Encoding") == "gzip" {
			compressed, err := gzip.NewReader(request.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer compressed.Close()
			reader = compressed
		}
		var body map[string]any
		if err := json.NewDecoder(reader).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		captures = append(captures, capture{header: request.Header.Clone(), body: body})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{}")
	}))
	defer collector.Close()

	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.Terminal.Disabled = true
	cfg.Version = "test-version"
	cfg.SessionTelemetry = &SessionTelemetryConfig{
		Mode: SessionTelemetryModeFull,
		Exporter: SessionTelemetryOTLPExporterConfig{
			URL: collector.URL, Headers: map[string]string{"authorization": "Bearer test-token"}, Compression: "gzip",
		},
		Processor: SessionTelemetryProcessorConfig{
			ScheduledDelay: time.Hour, MaxQueueSize: 64, MaxExportBatchSize: 64, ExportTimeout: time.Second,
		},
		ShutdownTimeout: 2 * time.Second,
	}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	id, err := engine.CreateSession(context.Background(), cfg.Workspace, "wire", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	_, _ = engine.appendEvent(session, "turn/start", map[string]any{"turn": 1})
	_, _ = engine.appendEvent(session, "turn/end", map[string]any{"turn": 1, "reason": map[string]any{"kind": "error"}})
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	got := append([]capture(nil), captures...)
	mu.Unlock()
	if len(got) == 0 {
		t.Fatal("collector received no exports")
	}
	if got[0].header.Get("Content-Type") != "application/json" || got[0].header.Get("Content-Encoding") != "gzip" || got[0].header.Get("Authorization") != "Bearer test-token" {
		t.Fatalf("headers = %#v", got[0].header)
	}
	resourceLogs := jsonArray(t, got[0].body["resourceLogs"])
	resourceLog := jsonObject(t, resourceLogs[0])
	resource := jsonObject(t, resourceLog["resource"])
	resourceAttrs := otlpAttributeMap(t, jsonArray(t, resource["attributes"]))
	if resourceAttrs["service.name"] != "deepseek-harness" || resourceAttrs["service.version"] != "test-version" || resourceAttrs["user.id"] == "" {
		t.Fatalf("resource attributes = %#v", resourceAttrs)
	}
	var eventTypes []string
	var severities []float64
	var scopes []string
	for _, rawScope := range jsonArray(t, resourceLog["scopeLogs"]) {
		scopeLog := jsonObject(t, rawScope)
		scope := jsonObject(t, scopeLog["scope"])
		scopes = append(scopes, scope["name"].(string))
		for _, rawRecord := range jsonArray(t, scopeLog["logRecords"]) {
			record := jsonObject(t, rawRecord)
			attributes := otlpAttributeMap(t, jsonArray(t, record["attributes"]))
			if eventType := attributes["event.type"]; eventType != "" {
				eventTypes = append(eventTypes, eventType)
				severity, ok := record["severityNumber"].(float64)
				if !ok {
					t.Fatalf("severityNumber is not numeric: %#v", record["severityNumber"])
				}
				severities = append(severities, severity)
			}
		}
	}
	if strings.Join(scopes, ",") != sessionTelemetryLedgerScope+","+sessionTelemetryOpsScope && strings.Join(scopes, ",") != sessionTelemetryOpsScope+","+sessionTelemetryLedgerScope {
		t.Fatalf("scopes = %v", scopes)
	}
	if strings.Join(eventTypes, ",") != "turn/start,turn/end" || len(severities) != 2 || severities[0] != 9 || severities[1] != 17 {
		t.Fatalf("eventTypes=%v severities=%v", eventTypes, severities)
	}
}

func TestSessionTelemetryCloseCapturesFinalTurnBeforeShutdown(t *testing.T) {
	sink := &recordingTelemetrySink{}
	engine := newTelemetryTestEngine(t, SessionTelemetryModeFull, sink, nil)
	provider := &telemetryBlockingProvider{started: make(chan struct{})}
	engine.RegisterProvider(provider)
	id, err := engine.CreateSession(context.Background(), t.TempDir(), "close-order", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Prompt(context.Background(), id, PromptRequest{
		Mode: "queue", Content: []PromptContentPart{{Type: "text", Text: "wait"}}, Literal: true,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("provider did not start")
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	records, _ := sink.snapshot()
	turnEnd, shutdown := -1, -1
	for index, record := range records {
		if record.Channel == SessionTelemetryChannelLedger && record.Attributes["event.type"] == "turn/end" {
			turnEnd = index
		}
		if record.Channel == SessionTelemetryChannelOps && record.Attributes["telemetry.op"] == "shutdown" {
			shutdown = index
		}
	}
	if turnEnd < 0 || shutdown < 0 || turnEnd > shutdown {
		t.Fatalf("turnEnd=%d shutdown=%d records=%#v", turnEnd, shutdown, records)
	}
}

func jsonArray(t *testing.T, value any) []any {
	t.Helper()
	result, ok := value.([]any)
	if !ok || len(result) == 0 {
		t.Fatalf("expected non-empty JSON array, got %#v", value)
	}
	return result
}

func jsonObject(t *testing.T, value any) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected JSON object, got %#v", value)
	}
	return result
}

func otlpAttributeMap(t *testing.T, values []any) map[string]string {
	t.Helper()
	result := map[string]string{}
	for _, raw := range values {
		attribute := jsonObject(t, raw)
		value := jsonObject(t, attribute["value"])
		if stringValue, ok := value["stringValue"].(string); ok {
			result[attribute["key"].(string)] = stringValue
		}
	}
	return result
}
