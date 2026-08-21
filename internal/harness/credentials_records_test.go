package harness

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func credentialTestEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir, cfg.Workspace = dir, dir
	cfg.Provider, cfg.Model, cfg.APIKey = "echo", "echo", ""
	cfg.SessionTitleLLM.Enabled = false
	cfg.Persist = true
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func waitCredentialCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("credential condition did not become true")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestCredentialRecordsRoundTripListAndDelete(t *testing.T) {
	dir := t.TempDir()
	e := credentialTestEngine(t, dir)
	key := CredentialKey("llm-pi-ai/openai-codex")
	record := CredentialRecord{Kind: CredentialRecordGrant, Payload: map[string]any{"access": "token", "expires": 123.0}}

	stored, err := e.Credentials().ModifyRecord(t.Context(), key, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
		return &record, nil
	})
	if err != nil || !reflect.DeepEqual(stored, &record) {
		t.Fatalf("ModifyRecord = %#v, %v", stored, err)
	}
	if entries := e.Credentials().ListRecords(); !reflect.DeepEqual(entries, []CredentialRecordEntry{{Key: key, Kind: CredentialRecordGrant}}) {
		t.Fatalf("ListRecords = %#v", entries)
	}
	info, err := os.Stat(filepath.Join(dir, credentialsYAMLName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("credential document mode = %v, %v", info, err)
	}

	reloaded := credentialTestEngine(t, dir)
	got, err := reloaded.Credentials().ReadRecord(key)
	if err != nil || !reflect.DeepEqual(got, &record) {
		t.Fatalf("reloaded record = %#v, %v", got, err)
	}
	if err := reloaded.Credentials().DeleteRecord(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if err := reloaded.Credentials().DeleteRecord(t.Context(), key); err != nil {
		t.Fatalf("second delete = %v", err)
	}
	if got, err := reloaded.Credentials().ReadRecord(key); err != nil || got != nil {
		t.Fatalf("deleted record = %#v, %v", got, err)
	}
}

func TestCredentialWatcherPublishesExternalEditsAndKeepsLastGood(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, credentialsYAMLName)
	initial := "version: 1\nrefs:\n  DSH_WATCH_REF: first\nrecords:\n  llm-pi-ai/deepseek:\n    kind: api-key\n    key: old\n"
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}
	e := credentialTestEngine(t, dir)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events := e.Credentials().Subscribe(ctx)

	updated := "version: 1\nrefs:\n  DSH_WATCH_REF: second\nrecords:\n  llm-pi-ai/deepseek:\n    kind: api-key\n    key: new\n"
	if err := os.WriteFile(path, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}
	waitCredentialCondition(t, func() bool {
		resolved, _ := e.Credentials().Resolve(CredentialRef("DSH_WATCH_REF"))
		record, _ := e.Credentials().ReadRecord(CredentialKey("llm-pi-ai/deepseek"))
		return resolved != nil && resolved.Value == "second" && record != nil && record.Key == "new"
	})
	seen := map[CredentialEventKind]bool{}
	for len(seen) < 2 {
		select {
		case event := <-events:
			seen[event.Kind] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("watch events = %#v", seen)
		}
	}

	if err := os.WriteFile(path, []byte("version: 1\nrefs:\n  BAD-KEY: broken\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	resolved, _ := e.Credentials().Resolve(CredentialRef("DSH_WATCH_REF"))
	if resolved == nil || resolved.Value != "second" {
		t.Fatalf("invalid reload replaced last good snapshot: %#v", resolved)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	waitCredentialCondition(t, func() bool {
		resolved, _ := e.Credentials().Resolve(CredentialRef("DSH_WATCH_REF"))
		record, _ := e.Credentials().ReadRecord(CredentialKey("llm-pi-ai/deepseek"))
		return resolved == nil && record == nil
	})
}

func TestCredentialRecordMutationHoldsCrossEngineFileLock(t *testing.T) {
	dir := t.TempDir()
	first := credentialTestEngine(t, dir)
	second := credentialTestEngine(t, dir)
	key := CredentialKey("llm-pi-ai/slow")
	entered := make(chan struct{})
	release := make(chan struct{})
	mutationDone := make(chan error, 1)
	go func() {
		_, err := first.Credentials().ModifyRecord(context.Background(), key, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
			close(entered)
			<-release
			return &CredentialRecord{Kind: CredentialRecordAPIKey, Key: "stored"}, nil
		})
		mutationDone <- err
	}()
	<-entered
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- second.Credentials().Set(context.Background(), CredentialRef("DSH_LOCK_WAIT"), "waited")
	}()
	select {
	case err := <-writeDone:
		t.Fatalf("competing writer escaped record lock: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	if err := <-mutationDone; err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}
