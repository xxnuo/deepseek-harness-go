package harness

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	authorizationTestKey  = CredentialKey("authorization-test/openai-codex")
	authorizationOtherKey = CredentialKey("authorization-test/anthropic")
)

func authorizationTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := New(
		WithPersistence(false),
		WithWorkspace(t.TempDir()),
		WithProvider("echo"),
		WithModel("echo"),
		WithSessionTitleLLM(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func authorizationSurface(answer string) (AuthorizationInteraction, *[]AuthorizationNotice, *[]AuthorizationPrompt) {
	notices := []AuthorizationNotice{}
	prompts := []AuthorizationPrompt{}
	return AuthorizationInteraction{
		Notify: func(notice AuthorizationNotice) { notices = append(notices, notice) },
		Prompt: func(_ context.Context, prompt AuthorizationPrompt) (string, error) {
			prompts = append(prompts, prompt)
			return answer, nil
		},
	}, &notices, &prompts
}

func authorizationCommittingFlow(
	e *Engine,
	key CredentialKey,
	run func(AuthorizationSession) error,
) AuthorizationFlow {
	return AuthorizationFlow{
		Key:   key,
		Label: "ChatGPT (Codex)",
		Methods: []AuthorizationMethod{
			{ID: "oauth", Label: "Sign in with ChatGPT"},
			{ID: "api-key", Label: "Paste a key"},
		},
		Run: func(session AuthorizationSession) error {
			if run != nil {
				if err := run(session); err != nil {
					return err
				}
			}
			_, err := e.Credentials().ModifyRecord(session.Context, key, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
				return &CredentialRecord{Kind: CredentialRecordGrant, Payload: map[string]any{"token": "granted"}}, nil
			})
			return err
		},
	}
}

func requireAuthorizationCode(t *testing.T, err error, code string) {
	t.Helper()
	if !IsAuthorizationError(err, code) {
		t.Fatalf("error = %v, want authorization code %s", err, code)
	}
}

func TestAuthorizationRegistryAndEngineLifecycle(t *testing.T) {
	e := authorizationTestEngine(t)
	baseline := e.Authorization().List()
	flow := authorizationCommittingFlow(e, authorizationTestKey, nil)
	dispose, err := e.Authorization().RegisterFlow(flow)
	if err != nil {
		t.Fatal(err)
	}
	want := AuthorizationEntry{
		Key: authorizationTestKey, Label: flow.Label, Methods: flow.Methods, InFlight: false,
	}
	got := e.Authorization().List()
	if len(got) != len(baseline)+1 {
		t.Fatalf("List length = %d, want %d: %#v", len(got), len(baseline)+1, got)
	}
	found := false
	for _, entry := range got {
		if entry.Key == authorizationTestKey {
			found = reflect.DeepEqual(entry, want)
			break
		}
	}
	if !found {
		t.Fatalf("List missing test flow %#v: %#v", want, got)
	}
	if got := e.Authorization().Describe(authorizationTestKey); got == nil || got.Label != flow.Label {
		t.Fatalf("Describe = %#v", got)
	}
	if got := e.Authorization().Describe(authorizationOtherKey); got != nil {
		t.Fatalf("Describe(other) = %#v", got)
	}
	if _, err := e.Authorization().RegisterFlow(flow); err == nil {
		t.Fatal("duplicate flow registration succeeded")
	} else {
		requireAuthorizationCode(t, err, AuthorizationDuplicateFlow)
	}
	dispose()
	dispose()
	if got := e.Authorization().List(); !reflect.DeepEqual(got, baseline) {
		t.Fatalf("List after dispose = %#v, want baseline %#v", got, baseline)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Authorization().RegisterFlow(flow); err == nil {
		t.Fatal("registration after close succeeded")
	} else {
		requireAuthorizationCode(t, err, AuthorizationClosed)
	}
}

func TestAuthorizationBeginCarriesInteractionCommitsAndSettlesAfterRelease(t *testing.T) {
	e := authorizationTestEngine(t)
	var methods []string
	var answers []string
	flow := authorizationCommittingFlow(e, authorizationTestKey, func(session AuthorizationSession) error {
		methods = append(methods, session.Method)
		session.Notify(AuthorizationNotice{Message: "Continue in your browser", URL: "https://auth.example/start"})
		answer, err := session.Prompt(AuthorizationPrompt{Kind: AuthorizationPromptText, Message: "Paste the code"})
		answers = append(answers, answer)
		return err
	})
	if _, err := e.Authorization().RegisterFlow(flow); err != nil {
		t.Fatal(err)
	}
	var settlements []AuthorizationSettlement
	dispose := e.Authorization().Subscribe(func(key CredentialKey, settlement AuthorizationSettlement) error {
		if key != authorizationTestKey {
			t.Fatalf("settled key = %q", key)
		}
		if entry := e.Authorization().Describe(key); entry == nil || entry.InFlight {
			t.Fatalf("settled before key release: %#v", entry)
		}
		settlements = append(settlements, settlement)
		return nil
	})
	defer dispose()

	interaction, notices, prompts := authorizationSurface("code-123")
	got, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
	if err != nil || got.Status != AuthorizationAuthorized {
		t.Fatalf("first Begin = %#v, %v", got, err)
	}
	got, err = e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Method: "api-key", Interaction: interaction})
	if err != nil || got.Status != AuthorizationAuthorized {
		t.Fatalf("second Begin = %#v, %v", got, err)
	}
	if !reflect.DeepEqual(methods, []string{"oauth", "api-key"}) || !reflect.DeepEqual(answers, []string{"code-123", "code-123"}) {
		t.Fatalf("methods = %#v, answers = %#v", methods, answers)
	}
	if len(*notices) != 2 || (*notices)[0].URL != "https://auth.example/start" || len(*prompts) != 2 {
		t.Fatalf("notices = %#v, prompts = %#v", *notices, *prompts)
	}
	if !reflect.DeepEqual(settlements, []AuthorizationSettlement{AuthorizationSettledAuthorized, AuthorizationSettledAuthorized}) {
		t.Fatalf("settlements = %#v", settlements)
	}
	record, err := e.Credentials().ReadRecord(authorizationTestKey)
	if err != nil || record == nil || record.Kind != CredentialRecordGrant {
		t.Fatalf("record = %#v, %v", record, err)
	}
}

func TestAuthorizationValidationPrecedesPreCancellation(t *testing.T) {
	e := authorizationTestEngine(t)
	interaction, _, _ := authorizationSurface("")
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := e.Authorization().Begin(cancelled, AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction}); err == nil {
		t.Fatal("cancelled unknown key succeeded")
	} else {
		requireAuthorizationCode(t, err, AuthorizationNoFlow)
	}
	var ran atomic.Bool
	flow := authorizationCommittingFlow(e, authorizationTestKey, func(AuthorizationSession) error {
		ran.Store(true)
		select {}
	})
	if _, err := e.Authorization().RegisterFlow(flow); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Authorization().Begin(cancelled, AuthorizationRequest{Key: authorizationTestKey, Method: "device", Interaction: interaction}); err == nil {
		t.Fatal("cancelled unknown method succeeded")
	} else {
		requireAuthorizationCode(t, err, AuthorizationUnknownMethod)
	}
	settled := atomic.Int32{}
	e.Authorization().Subscribe(func(CredentialKey, AuthorizationSettlement) error { settled.Add(1); return nil })
	got, err := e.Authorization().Begin(cancelled, AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
	if err != nil || got.Status != AuthorizationCancelled || ran.Load() || settled.Load() != 0 {
		t.Fatalf("pre-cancelled Begin = %#v, %v; ran=%v settled=%d", got, err, ran.Load(), settled.Load())
	}
}

func TestAuthorizationSingleFlightCancelAndDisposalReleaseUnresponsiveFlows(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		e := authorizationTestEngine(t)
		started := make(chan struct{})
		orphan := make(chan struct{})
		var first atomic.Bool
		first.Store(true)
		flow := authorizationCommittingFlow(e, authorizationTestKey, func(AuthorizationSession) error {
			if first.CompareAndSwap(true, false) {
				close(started)
				<-orphan
			}
			return nil
		})
		if _, err := e.Authorization().RegisterFlow(flow); err != nil {
			t.Fatal(err)
		}
		interaction, _, _ := authorizationSurface("")
		result := make(chan struct {
			out AuthorizationOutcome
			err error
		}, 1)
		go func() {
			out, err := e.Authorization().Begin(context.Background(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
			result <- struct {
				out AuthorizationOutcome
				err error
			}{out, err}
		}()
		<-started
		if entry := e.Authorization().Describe(authorizationTestKey); entry == nil || !entry.InFlight {
			t.Fatalf("entry = %#v", entry)
		}
		if _, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction}); err == nil {
			t.Fatal("second attempt succeeded while first was running")
		} else {
			requireAuthorizationCode(t, err, AuthorizationAlreadyRunning)
		}
		e.Authorization().Cancel(authorizationOtherKey)
		e.Authorization().Cancel(authorizationTestKey)
		select {
		case got := <-result:
			if got.err != nil || got.out.Status != AuthorizationCancelled {
				t.Fatalf("cancelled Begin = %#v, %v", got.out, got.err)
			}
		case <-time.After(time.Second):
			t.Fatal("cancel waited for an unresponsive flow")
		}
		if entry := e.Authorization().Describe(authorizationTestKey); entry == nil || entry.InFlight {
			t.Fatalf("key was not released: %#v", entry)
		}
		out, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
		if err != nil || out.Status != AuthorizationAuthorized {
			t.Fatalf("attempt after cancellation = %#v, %v", out, err)
		}
		close(orphan)
	})

	t.Run("flow disposal", func(t *testing.T) {
		e := authorizationTestEngine(t)
		started := make(chan struct{})
		flow := authorizationCommittingFlow(e, authorizationTestKey, func(session AuthorizationSession) error {
			close(started)
			<-session.Context.Done()
			return errors.New("withdrawn")
		})
		dispose, err := e.Authorization().RegisterFlow(flow)
		if err != nil {
			t.Fatal(err)
		}
		interaction, _, _ := authorizationSurface("")
		result := make(chan AuthorizationOutcome, 1)
		go func() {
			out, _ := e.Authorization().Begin(context.Background(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
			result <- out
		}()
		<-started
		dispose()
		select {
		case got := <-result:
			if got.Status != AuthorizationCancelled {
				t.Fatalf("disposed outcome = %#v", got)
			}
		case <-time.After(time.Second):
			t.Fatal("disposing a flow did not withdraw its attempt")
		}
		if got := e.Authorization().Describe(authorizationTestKey); got != nil {
			t.Fatalf("disposed flow = %#v", got)
		}
	})
}

func TestAuthorizationCommitConfirmationAndFailures(t *testing.T) {
	t.Run("flow failure", func(t *testing.T) {
		e := authorizationTestEngine(t)
		flow := authorizationCommittingFlow(e, authorizationTestKey, func(AuthorizationSession) error {
			return errors.New("the token endpoint said no")
		})
		if _, err := e.Authorization().RegisterFlow(flow); err != nil {
			t.Fatal(err)
		}
		var settlement AuthorizationSettlement
		e.Authorization().Subscribe(func(_ CredentialKey, got AuthorizationSettlement) error { settlement = got; return nil })
		interaction, _, _ := authorizationSurface("")
		_, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
		if err == nil || !strings.Contains(err.Error(), "token endpoint") || settlement != AuthorizationSettledFailed {
			t.Fatalf("Begin error = %v, settlement = %q", err, settlement)
		}
	})

	t.Run("no current commit", func(t *testing.T) {
		e := authorizationTestEngine(t)
		if _, err := e.Credentials().ModifyRecord(t.Context(), authorizationTestKey, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
			return &CredentialRecord{Kind: CredentialRecordGrant, Payload: map[string]any{"token": "stale"}}, nil
		}); err != nil {
			t.Fatal(err)
		}
		flow := AuthorizationFlow{
			Key: authorizationTestKey, Label: "Forgetful",
			Methods: []AuthorizationMethod{{ID: "oauth", Label: "Sign in"}},
			Run: func(session AuthorizationSession) error {
				_, err := e.Credentials().ModifyRecord(session.Context, authorizationOtherKey, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
					return &CredentialRecord{Kind: CredentialRecordGrant, Payload: map[string]any{}}, nil
				})
				return err
			},
		}
		if _, err := e.Authorization().RegisterFlow(flow); err != nil {
			t.Fatal(err)
		}
		interaction, _, _ := authorizationSurface("")
		_, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
		requireAuthorizationCode(t, err, AuthorizationNotCommitted)
		record, readErr := e.Credentials().ReadRecord(authorizationTestKey)
		if readErr != nil || record == nil || record.Payload.(map[string]any)["token"] != "stale" {
			t.Fatalf("stale record = %#v, %v", record, readErr)
		}
	})

	t.Run("delete is not commit", func(t *testing.T) {
		e := authorizationTestEngine(t)
		if _, err := e.Credentials().ModifyRecord(t.Context(), authorizationTestKey, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
			return &CredentialRecord{Kind: CredentialRecordGrant, Payload: map[string]any{}}, nil
		}); err != nil {
			t.Fatal(err)
		}
		flow := AuthorizationFlow{
			Key: authorizationTestKey, Label: "Destructive",
			Methods: []AuthorizationMethod{{ID: "oauth", Label: "Sign in"}},
			Run: func(session AuthorizationSession) error {
				return e.Credentials().DeleteRecord(session.Context, authorizationTestKey)
			},
		}
		if _, err := e.Authorization().RegisterFlow(flow); err != nil {
			t.Fatal(err)
		}
		interaction, _, _ := authorizationSurface("")
		_, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
		requireAuthorizationCode(t, err, AuthorizationNotCommitted)
		if !strings.Contains(err.Error(), "deleted") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestAuthorizationDeclinesPromptFailuresAndNoticePanics(t *testing.T) {
	t.Run("decline survives rewrap", func(t *testing.T) {
		e := authorizationTestEngine(t)
		flow := authorizationCommittingFlow(e, authorizationTestKey, func(session AuthorizationSession) error {
			_, err := session.Prompt(AuthorizationPrompt{Kind: AuthorizationPromptText, Message: "Paste the code"})
			if err != nil {
				return errors.New("sign-in aborted")
			}
			return nil
		})
		if _, err := e.Authorization().RegisterFlow(flow); err != nil {
			t.Fatal(err)
		}
		interaction := AuthorizationInteraction{Prompt: func(context.Context, AuthorizationPrompt) (string, error) {
			return "", &AuthorizationDeclinedError{}
		}}
		var settlement AuthorizationSettlement
		e.Authorization().Subscribe(func(_ CredentialKey, got AuthorizationSettlement) error { settlement = got; return nil })
		out, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
		if err != nil || out.Status != AuthorizationCancelled || settlement != AuthorizationSettledCancelled {
			t.Fatalf("Begin = %#v, %v; settlement=%q", out, err, settlement)
		}
	})

	t.Run("surface failure", func(t *testing.T) {
		e := authorizationTestEngine(t)
		flow := authorizationCommittingFlow(e, authorizationTestKey, func(session AuthorizationSession) error {
			_, err := session.Prompt(AuthorizationPrompt{Kind: AuthorizationPromptText, Message: "Paste the code"})
			return err
		})
		if _, err := e.Authorization().RegisterFlow(flow); err != nil {
			t.Fatal(err)
		}
		interaction := AuthorizationInteraction{Prompt: func(context.Context, AuthorizationPrompt) (string, error) {
			return "", errors.New("the transport dropped")
		}}
		_, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
		if err == nil || !strings.Contains(err.Error(), "transport dropped") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("notice panic", func(t *testing.T) {
		e := authorizationTestEngine(t)
		flow := authorizationCommittingFlow(e, authorizationTestKey, func(session AuthorizationSession) error {
			session.Notify(AuthorizationNotice{Message: "Continue"})
			return nil
		})
		if _, err := e.Authorization().RegisterFlow(flow); err != nil {
			t.Fatal(err)
		}
		interaction := AuthorizationInteraction{Notify: func(AuthorizationNotice) { panic("connection closed") }}
		out, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
		if err != nil || out.Status != AuthorizationAuthorized {
			t.Fatalf("Begin = %#v, %v", out, err)
		}
	})
}

func TestAuthorizationSettlementListenerContainment(t *testing.T) {
	t.Run("ordinary failures", func(t *testing.T) {
		e := authorizationTestEngine(t)
		if _, err := e.Authorization().RegisterFlow(authorizationCommittingFlow(e, authorizationTestKey, nil)); err != nil {
			t.Fatal(err)
		}
		e.Authorization().Subscribe(func(CredentialKey, AuthorizationSettlement) error { panic("watcher panic") })
		var second atomic.Bool
		e.Authorization().Subscribe(func(CredentialKey, AuthorizationSettlement) error {
			second.Store(true)
			return errors.New("watcher failure")
		})
		interaction, _, _ := authorizationSurface("")
		out, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
		if err != nil || out.Status != AuthorizationAuthorized || !second.Load() {
			t.Fatalf("Begin = %#v, %v; second=%v", out, err, second.Load())
		}
	})

	t.Run("invariant failure", func(t *testing.T) {
		e := authorizationTestEngine(t)
		if _, err := e.Authorization().RegisterFlow(authorizationCommittingFlow(e, authorizationTestKey, nil)); err != nil {
			t.Fatal(err)
		}
		e.Authorization().Subscribe(func(CredentialKey, AuthorizationSettlement) error {
			return &InvariantError{Code: InvariantErrorCode, PackageName: "authorization-test", Detail: "forged relation"}
		})
		var second atomic.Bool
		e.Authorization().Subscribe(func(CredentialKey, AuthorizationSettlement) error { second.Store(true); return nil })
		interaction, _, _ := authorizationSurface("")
		_, err := e.Authorization().Begin(t.Context(), AuthorizationRequest{Key: authorizationTestKey, Interaction: interaction})
		var invariant *InvariantError
		if !errors.As(err, &invariant) || !second.Load() {
			t.Fatalf("error = %v, second=%v", err, second.Load())
		}
		record, readErr := e.Credentials().ReadRecord(authorizationTestKey)
		if readErr != nil || record == nil {
			t.Fatalf("committed record = %#v, %v", record, readErr)
		}
	})
}

func TestAuthorizationConcurrentDifferentKeys(t *testing.T) {
	e := authorizationTestEngine(t)
	barrier := make(chan struct{})
	started := sync.WaitGroup{}
	started.Add(2)
	for _, key := range []CredentialKey{authorizationTestKey, authorizationOtherKey} {
		flow := authorizationCommittingFlow(e, key, func(AuthorizationSession) error {
			started.Done()
			<-barrier
			return nil
		})
		if _, err := e.Authorization().RegisterFlow(flow); err != nil {
			t.Fatal(err)
		}
	}
	interaction, _, _ := authorizationSurface("")
	errs := make(chan error, 2)
	for _, key := range []CredentialKey{authorizationTestKey, authorizationOtherKey} {
		go func(key CredentialKey) {
			_, err := e.Authorization().Begin(context.Background(), AuthorizationRequest{Key: key, Interaction: interaction})
			errs <- err
		}(key)
	}
	started.Wait()
	close(barrier)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
