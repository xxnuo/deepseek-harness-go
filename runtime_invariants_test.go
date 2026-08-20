package harness

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type invariantBadFinishProvider struct{}

func (invariantBadFinishProvider) ID() string   { return "invariant-bad-finish" }
func (invariantBadFinishProvider) Name() string { return "Invariant Bad Finish" }
func (invariantBadFinishProvider) Models(context.Context) ([]ModelInfo, error) {
	return []ModelInfo{{ID: "invariant-bad-finish", Name: "Invariant Bad Finish"}}, nil
}
func (invariantBadFinishProvider) Complete(context.Context, ChatRequest, func(Delta) error) (Completion, error) {
	return Completion{Text: "partial", Finish: "invalid-finish"}, nil
}

func requireInvariantError(t *testing.T, err error, packageName string) *InvariantError {
	t.Helper()
	var invariantErr *InvariantError
	if !errors.As(err, &invariantErr) {
		t.Fatalf("error = %v, want InvariantError", err)
	}
	if invariantErr.Code != InvariantErrorCode || invariantErr.PackageName != packageName {
		t.Fatalf("invariant error = %#v", invariantErr)
	}
	return invariantErr
}

func TestInvariantRegistrySelectionValidationAndOwnership(t *testing.T) {
	registry, err := NewInvariantRegistry(RuntimeInvariantConfig{
		PackageAllowlist: []string{`(?=session)session`},
		PackageBlocklist: []string{`title`},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()

	var sessionChecks atomic.Int32
	dispose, err := registry.Register(invariantPackageSession, func(scope *InvariantScope, _ InvariantFailure) error {
		scope.CheckSessions(func(SessionHeader, []Event) error {
			sessionChecks.Add(1)
			return nil
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(invariantPackageSession, func(*InvariantScope, InvariantFailure) error { return nil }); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate registration error = %v", err)
	}
	if _, err := registry.Register(invariantPackageSessionTitle, func(*InvariantScope, InvariantFailure) error {
		t.Fatal("blocklisted installer ran")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.ValidateSession(SessionHeader{}, nil); err != nil {
		t.Fatal(err)
	}
	if sessionChecks.Load() != 1 {
		t.Fatalf("session checks = %d", sessionChecks.Load())
	}
	if err := dispose(); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(invariantPackageSession, func(*InvariantScope, InvariantFailure) error { return nil }); err != nil {
		t.Fatalf("re-register after dispose: %v", err)
	}

	for _, test := range []RuntimeInvariantConfig{
		{PackageAllowlist: []string{""}},
		{PackageAllowlist: []string{" session"}},
		{PackageBlocklist: []string{"session", "session"}},
		{PackageAllowlist: []string{"["}},
	} {
		if _, err := NewInvariantRegistry(test); err == nil {
			t.Fatalf("config %#v unexpectedly accepted", test)
		}
	}
	if _, err := registry.Register("pack age", func(*InvariantScope, InvariantFailure) error { return nil }); err == nil {
		t.Fatal("whitespace package name accepted")
	}
}

func TestInvariantRegistryDisabledStillReservesAndDisposerWaitsForCleanup(t *testing.T) {
	disabled, err := NewInvariantRegistry(RuntimeInvariantConfig{Disabled: true})
	if err != nil {
		t.Fatal(err)
	}
	var installed atomic.Bool
	dispose, err := disabled.Register(invariantPackageSession, func(*InvariantScope, InvariantFailure) error {
		installed.Store(true)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if installed.Load() {
		t.Fatal("disabled installer ran")
	}
	if _, err := disabled.Register(invariantPackageSession, func(*InvariantScope, InvariantFailure) error { return nil }); err == nil {
		t.Fatal("disabled registration did not reserve ownership")
	}
	if err := dispose(); err != nil {
		t.Fatal(err)
	}

	registry, err := NewInvariantRegistry(RuntimeInvariantConfig{})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	dispose, err = registry.Register(invariantPackageSession, func(scope *InvariantScope, _ InvariantFailure) error {
		scope.Defer(func() error {
			close(started)
			<-release
			return nil
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- dispose() }()
	<-started
	if _, err := registry.Register(invariantPackageSession, func(*InvariantScope, InvariantFailure) error { return nil }); err == nil {
		t.Fatal("ownership released before cleanup completed")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(invariantPackageSession, func(*InvariantScope, InvariantFailure) error { return nil }); err != nil {
		t.Fatalf("re-register after cleanup: %v", err)
	}
}

func TestInvariantRegistryAttributesFailuresAndRollsBackInstaller(t *testing.T) {
	registry, err := NewInvariantRegistry(RuntimeInvariantConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cleaned := false
	if _, err := registry.Register(invariantPackageSession, func(scope *InvariantScope, _ InvariantFailure) error {
		scope.Defer(func() error { cleaned = true; return nil })
		return errors.New("installer failed")
	}); err == nil || !cleaned {
		t.Fatalf("installer error = %v, cleaned = %v", err, cleaned)
	}
	if _, err := registry.Register(invariantPackageSession, func(scope *InvariantScope, fail InvariantFailure) error {
		scope.CheckSessions(func(SessionHeader, []Event) error { return fail("seq must strictly increase") })
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err = registry.ValidateSession(SessionHeader{}, nil)
	invariantErr := requireInvariantError(t, err, invariantPackageSession)
	if invariantErr.Error() != `invariant violated by "@deepseek-ai/dsh-session": seq must strictly increase` {
		t.Fatalf("message = %q", invariantErr.Error())
	}
}

func newInvariantEngine(t *testing.T, options ...Option) *Engine {
	t.Helper()
	base := []Option{WithPersistence(false), WithRuntimeInvariants(RuntimeInvariantConfig{})}
	engine, err := New(append(base, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func TestRuntimeSessionInvariantRejectsBeforeCommit(t *testing.T) {
	engine := newInvariantEngine(t)
	id, err := engine.CreateSession(context.Background(), t.TempDir(), "invariant-session", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	if _, err := engine.appendEvent(session, "turn/start", map[string]any{"turn": 2}); err == nil {
		t.Fatal("out-of-order turn accepted")
	} else {
		requireInvariantError(t, err, invariantPackageSession)
	}
	if len(session.Events) != 0 {
		t.Fatalf("failed event committed: %#v", session.Events)
	}
	for _, item := range []struct {
		typeName string
		data     map[string]any
	}{
		{"turn/start", map[string]any{"turn": 1}},
		{"step/start", map[string]any{"turn": 1, "step": 1}},
		{"assistant/message", map[string]any{"turn": 1, "step": 1}},
		{"step/end", map[string]any{"turn": 1, "step": 1}},
		{"turn/end", map[string]any{"turn": 1}},
	} {
		if _, err := engine.appendEvent(session, item.typeName, item.data); err != nil {
			t.Fatalf("append %s: %v", item.typeName, err)
		}
	}
}

func TestRuntimeOwnedEventInvariants(t *testing.T) {
	engine := newInvariantEngine(t)
	id, err := engine.CreateSession(context.Background(), t.TempDir(), "owned-invariants", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)

	_, err = engine.appendEvent(session, "session/title", map[string]any{
		"title": "bad", "messageSeqs": []int{1}, "source": map[string]any{"kind": "user"},
	})
	requireInvariantError(t, err, invariantPackageSessionTitle)

	_, err = engine.appendEvent(session, "permission/preset", map[string]any{"preset": "unknown"})
	requireInvariantError(t, err, invariantPackagePermission)

	_, err = engine.appendEvent(session, "goal/change", map[string]any{"kind": "goal/change", "version": 1, "operation": "unknown"})
	requireInvariantError(t, err, invariantPackageGoal)

	session.mu.Lock()
	_, err = appendUserMessagesLocked(session, []map[string]any{{
		"content": []ContentBlock{{Type: "text", Text: "bad goal"}},
		"source":  map[string]any{"kind": "goal", "goalId": "", "revision": 1, "round": 1},
	}})
	session.mu.Unlock()
	requireInvariantError(t, err, invariantPackageGoal)
	if len(session.Events) != 0 {
		t.Fatalf("failed owned events committed: %#v", session.Events)
	}
}

func TestRuntimeInvariantClosesFailedAgentStep(t *testing.T) {
	engine := newInvariantEngine(t)
	provider := invariantBadFinishProvider{}
	engine.RegisterProvider(provider)
	id, err := engine.CreateSession(context.Background(), t.TempDir(), "failed-step-invariant", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.SelectModel(id, ModelSelection{Provider: provider.ID(), Model: provider.ID()}); err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	if _, err := engine.appendEvent(session, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEvent(session, "user/message", map[string]any{
		"content": []ContentBlock{{Type: "text", Text: "hello"}}, "source": map[string]any{"kind": "user"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.runTurnSync(context.Background(), session, 1); err == nil {
		t.Fatal("invalid provider finish accepted")
	}
	if err := engine.invariants.ValidateSession(session.Header, session.Events); err != nil {
		t.Fatalf("failed turn left invalid log: %v", err)
	}
	if len(session.Events) < 2 || session.Events[len(session.Events)-2].Type != "step/end" || session.Events[len(session.Events)-1].Type != "turn/end" {
		t.Fatalf("failed turn tail = %#v", session.Events)
	}
}

func TestRuntimeInvariantAcceptsProgrammaticSubagentTitle(t *testing.T) {
	engine := newInvariantEngine(t)
	id, err := engine.CreateSession(context.Background(), t.TempDir(), "subagent-title-invariant", "")
	if err != nil {
		t.Fatal(err)
	}
	engine.setSessionTitle(id, "worker")
	session, _ := engine.getSession(id)
	if len(session.Events) != 1 || session.Events[0].Type != "session/title" {
		t.Fatalf("title events = %#v", session.Events)
	}
	data, _ := session.Events[0].Data.(map[string]any)
	source, _ := data["source"].(map[string]any)
	if source["kind"] != "user" {
		t.Fatalf("title source = %#v", source)
	}
}

func TestRuntimeTimeContextInvariantAcceptsCanonicalSnapshotAndRejectsDrift(t *testing.T) {
	engine := newInvariantEngine(t, WithTimeContext(TimeContextConfig{TimeZone: "UTC"}))
	id, err := engine.CreateSession(context.Background(), t.TempDir(), "time-invariant", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	if _, err := engine.appendEvent(session, "turn/start", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEvent(session, "user/message", map[string]any{
		"content": []ContentBlock{{Type: "text", Text: "now"}},
		"source":  map[string]any{"kind": "user", "rpcId": "rpc-1", "clientTimeZone": "UTC"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.appendEvent(session, "step/start", map[string]any{"turn": 1, "step": 1}); err != nil {
		t.Fatal(err)
	}
	message, ok, err := engine.timeContext(session, 1, 1)
	if err != nil || !ok {
		t.Fatalf("time context = %#v, %v, %v", message, ok, err)
	}
	if _, err := engine.appendEvent(session, "user/message", message); err != nil {
		t.Fatalf("canonical time context: %v", err)
	}

	bad := pluginContextMessage("time-context", "not a durable reading")
	if _, err := engine.appendEvent(session, "user/message", bad); err == nil {
		t.Fatal("malformed time context accepted")
	} else {
		requireInvariantError(t, err, invariantPackageTimeContext)
	}
}

func TestRuntimeAgentLoopInvariantMatchesDurableRequest(t *testing.T) {
	engine := newInvariantEngine(t)
	id, err := engine.CreateSession(context.Background(), t.TempDir(), "request-invariant", "")
	if err != nil {
		t.Fatal(err)
	}
	session, _ := engine.getSession(id)
	for _, item := range []struct {
		typeName string
		data     map[string]any
	}{
		{"turn/start", map[string]any{"turn": 1}},
		{"user/message", map[string]any{"content": []ContentBlock{{Type: "text", Text: "hello"}}, "source": map[string]any{"kind": "user"}}},
		{"step/start", map[string]any{"turn": 1, "step": 1}},
		{"request/header", map[string]any{"header": map[string]any{"config": map[string]any{
			"provider": "echo", "model": "echo", "temperature": 0.2, "stop": []string{"END"},
		}}}},
	} {
		if _, err := engine.appendEvent(session, item.typeName, item.data); err != nil {
			t.Fatalf("append %s: %v", item.typeName, err)
		}
	}
	temperature := 0.2
	request := ChatRequest{
		SessionID: id, Model: "echo", Messages: engine.durableMessages(session, 1), Tools: []ToolSchema{},
		Temperature: &temperature, Stop: []string{"END"},
	}
	if err := engine.invariants.ValidateAgentRequest(engine, session, request); err != nil {
		t.Fatalf("canonical request: %v", err)
	}
	request.Messages = append(request.Messages, ChatMessage{Role: "user", Content: "drift"})
	err = engine.invariants.ValidateAgentRequest(engine, session, request)
	requireInvariantError(t, err, invariantPackageAgentLoop)
	request.Messages = engine.durableMessages(session, 1)
	request.Stop = []string{"OTHER"}
	err = engine.invariants.ValidateAgentRequest(engine, session, request)
	requireInvariantError(t, err, invariantPackageAgentLoop)
}

func TestRuntimeInvariantConfigIsCloned(t *testing.T) {
	allowlist := []string{"session"}
	config := RuntimeInvariantConfig{PackageAllowlist: allowlist}
	engine := newInvariantEngine(t, WithRuntimeInvariants(config))
	allowlist[0] = "goal"
	config.PackageAllowlist[0] = "permission"
	if got := engine.Config().RuntimeInvariants.PackageAllowlist[0]; got != "session" {
		t.Fatalf("stored allowlist = %q", got)
	}

	start := time.Now()
	if err := engine.invariants.ValidateSession(SessionHeader{}, nil); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("empty validation unexpectedly slow")
	}
}
