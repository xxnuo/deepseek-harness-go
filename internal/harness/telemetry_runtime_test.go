package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type failingTelemetrySink struct {
	mu         sync.Mutex
	records    []SessionTelemetryRecord
	failNext   error
	panicNext  bool
	shutdownFn func(context.Context) error
}

type sliceTelemetrySink []byte

func (sliceTelemetrySink) Emit(context.Context, SessionTelemetryRecord) error { return nil }
func (sliceTelemetrySink) Shutdown(context.Context) error                     { return nil }

func TestEquivalentTelemetryConfigHandlesNonComparableSink(t *testing.T) {
	left := &SessionTelemetryConfig{Sink: sliceTelemetrySink{1}}
	right := &SessionTelemetryConfig{Sink: sliceTelemetrySink{1}}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("telemetry config comparison panicked: %v", recovered)
		}
	}()
	if equivalentSessionTelemetryConfig(left, right) {
		t.Fatal("non-comparable sinks must not be treated as identical")
	}
}

func TestApplyRuntimeConfigReplacesTelemetryCoordinator(t *testing.T) {
	oldSink := &recordingTelemetrySink{}
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.Terminal.Disabled = true
	cfg.SessionTelemetry = &SessionTelemetryConfig{Mode: SessionTelemetryModeFull, Sink: oldSink}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	newSink := &recordingTelemetrySink{}
	next := cfg
	next.SessionTelemetry = &SessionTelemetryConfig{Mode: SessionTelemetryModeFull, Sink: newSink}
	if err := e.ApplyRuntimeConfig(next); err != nil {
		t.Fatal(err)
	}
	_, oldShutdown := oldSink.snapshot()
	if oldShutdown != 1 {
		t.Fatalf("old telemetry coordinator shutdown count = %d, want 1", oldShutdown)
	}
	if e.telemetry == nil || e.telemetry.sink != newSink {
		t.Fatal("new telemetry coordinator was not published")
	}
	disabled := next
	disabled.SessionTelemetry = nil
	if err := e.ApplyRuntimeConfig(disabled); err != nil {
		t.Fatal(err)
	}
	if e.telemetry != nil {
		t.Fatal("disabled telemetry coordinator survived runtime update")
	}
}

func (s *failingTelemetrySink) Emit(_ context.Context, record SessionTelemetryRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.panicNext {
		s.panicNext = false
		panic("telemetry sink panic")
	}
	if s.failNext != nil {
		err := s.failNext
		s.failNext = nil
		return err
	}
	s.records = append(s.records, record)
	return nil
}

func (s *failingTelemetrySink) Shutdown(ctx context.Context) error {
	if s.shutdownFn != nil {
		return s.shutdownFn(ctx)
	}
	return nil
}

func (s *failingTelemetrySink) snapshot() []SessionTelemetryRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SessionTelemetryRecord(nil), s.records...)
}

func TestSessionTelemetryFailuresStayOutsideSessionFlow(t *testing.T) {
	sink := &failingTelemetrySink{failNext: errors.New("collector unavailable")}
	var mu sync.Mutex
	var warnings []error
	redactions := 0
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.Terminal.Disabled = true
	cfg.SessionTelemetry = &SessionTelemetryConfig{
		Mode: SessionTelemetryModeFull,
		Sink: sink,
		Redact: func(record SessionTelemetryRecord) (SessionTelemetryRecord, error) {
			redactions++
			if redactions == 1 {
				return SessionTelemetryRecord{}, errors.New("redaction rejected record")
			}
			record.Severity = SessionTelemetrySeverityWarn
			return record, nil
		},
		OnError: func(err error) {
			mu.Lock()
			warnings = append(warnings, err)
			mu.Unlock()
		},
	}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	id, err := engine.CreateSession(t.Context(), cfg.Workspace, "containment", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	if _, err := engine.appendEvent(session, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEvent(session, "step/start", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	sink.panicNext = true
	sink.mu.Unlock()
	if _, err := engine.appendEvent(session, "step/end", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEvent(session, "turn/end", map[string]any{"turn": 1, "reason": map[string]any{"kind": "completed"}}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	gotWarnings := append([]error(nil), warnings...)
	mu.Unlock()
	records := sink.snapshot()
	if len(gotWarnings) != 3 {
		t.Fatalf("warnings = %v", gotWarnings)
	}
	if len(records) != 2 || records[0].Attributes["event.type"] != "turn/end" || records[0].Severity != SessionTelemetrySeverityWarn || records[1].Attributes["telemetry.op"] != "shutdown" {
		t.Fatalf("records = %#v", records)
	}
}

type telemetryRuntimeErrorProvider struct{}

func (telemetryRuntimeErrorProvider) ID() string   { return "telemetry-runtime-error" }
func (telemetryRuntimeErrorProvider) Name() string { return "telemetry-runtime-error" }
func (telemetryRuntimeErrorProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "telemetry-runtime-error", Name: "telemetry-runtime-error"}}, nil
}
func (telemetryRuntimeErrorProvider) Complete(context.Context, ChatRequest, func(Delta) error) (Completion, error) {
	return Completion{Finish: "content_filter"}, nil
}

func TestSessionTelemetryAgentErrorUsesCurrentTurnAndStep(t *testing.T) {
	sink := &recordingTelemetrySink{}
	engine := newTelemetryTestEngine(t, SessionTelemetryModeFull, sink, nil)
	provider := telemetryRuntimeErrorProvider{}
	engine.RegisterProvider(provider)
	id, err := engine.CreateSession(t.Context(), engine.Config().Workspace, "agent-error", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(t.Context(), id, PromptRequest{Content: []PromptContentPart{{Type: "text", Text: "fail"}}}); err == nil {
		t.Fatal("failing turn returned nil error")
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	records, _ := sink.snapshot()
	for _, record := range records {
		if record.Attributes["telemetry.op"] != "agent-error" {
			continue
		}
		if record.Attributes["turn"] != 1 || record.Attributes["step"] != 1 || record.Attributes["agent.id"] != id {
			t.Fatalf("agent error record = %#v", record)
		}
		return
	}
	t.Fatalf("agent error record not found: %#v", records)
}

func TestSessionTelemetryFeedbackAcknowledgementDoesNotDiscloseBackend(t *testing.T) {
	tests := []struct {
		name       string
		configured bool
		mode       SessionTelemetryMode
	}{
		{"not configured", false, ""},
		{"full", true, SessionTelemetryModeFull},
		{"feedback-only", true, SessionTelemetryModeFeedbackOnly},
		{"disabled", true, SessionTelemetryModeDisabled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
			cfg.Terminal.Disabled = true
			if test.configured {
				cfg.SessionTelemetry = &SessionTelemetryConfig{Mode: test.mode, Sink: &recordingTelemetrySink{}}
			}
			engine, err := New(WithConfig(cfg))
			if err != nil {
				t.Fatal(err)
			}
			id, err := engine.CreateSession(t.Context(), cfg.Workspace, "", "")
			if err != nil {
				t.Fatal(err)
			}
			session, _ := engine.getSession(id)
			result, err := engine.runCommand(session, "/feedback useful")
			want := fmt.Sprintf("Feedback recorded for session %s\nAnonymous user: ", id)
			if err != nil || result.Command == nil || !strings.HasPrefix(result.Command.Text, want) || strings.Contains(result.Command.Text, "Session sharing") {
				t.Fatalf("feedback = %#v, err=%v", result, err)
			}
			if err := engine.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSessionTelemetryShutdownTimeoutIsContained(t *testing.T) {
	sink := &failingTelemetrySink{shutdownFn: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	warnings := make(chan error, 2)
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace, cfg.Persist = t.TempDir(), t.TempDir(), false
	cfg.Terminal.Disabled = true
	cfg.SessionTelemetry = &SessionTelemetryConfig{
		Mode: SessionTelemetryModeFeedbackOnly, Sink: sink, ShutdownTimeout: 20 * time.Millisecond,
		OnError: func(err error) { warnings <- err },
	}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("shutdown elapsed %s", elapsed)
	}
	select {
	case warning := <-warnings:
		if !strings.Contains(warning.Error(), "shutdown") {
			t.Fatalf("warning = %v", warning)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown timeout warning not reported")
	}
}
