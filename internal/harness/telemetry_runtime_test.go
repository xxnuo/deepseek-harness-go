package harness

import (
	"context"
	"errors"
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

func TestSessionTelemetryFeedbackDisclosure(t *testing.T) {
	tests := []struct {
		name       string
		configured bool
		mode       SessionTelemetryMode
		want       string
	}{
		{"not configured", false, "", "Session sharing is not configured."},
		{"full", true, SessionTelemetryModeFull, "Session sharing is enabled."},
		{"feedback-only", true, SessionTelemetryModeFeedbackOnly, "Session sharing is feedback-gated; recording feedback releases the session prefix for sharing."},
		{"disabled", true, SessionTelemetryModeDisabled, "Session sharing is disabled."},
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
			if err != nil || result.Command == nil || !strings.Contains(result.Command.Text, test.want) {
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
