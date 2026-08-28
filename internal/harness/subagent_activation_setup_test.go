package harness

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestSubagentActivationSetupRegistryOrderCommitAndOwnership(t *testing.T) {
	registry := newSubagentActivationSetupRegistry()
	var order []string
	firstRemove, err := registry.register(func(*Session) (func() error, error) {
		order = append(order, "first")
		return func() error { order = append(order, "undo-first"); return nil }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.register(func(*Session) (func() error, error) {
		order = append(order, "second")
		return func() error { order = append(order, "undo-second"); return nil }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	child := &Session{}
	transaction, err := registry.apply(child)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.commit(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"first", "second"}) {
		t.Fatalf("install order = %#v", order)
	}
	if err := firstRemove(); err != nil {
		t.Fatal(err)
	}
	if err := firstRemove(); err != nil {
		t.Fatal(err)
	}
	if err := registry.releaseChild(child); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"first", "second", "undo-first", "undo-second"}) {
		t.Fatalf("release order = %#v", order)
	}
}

func TestSubagentActivationSetupRegistryRevocationAndRollback(t *testing.T) {
	registry := newSubagentActivationSetupRegistry()
	var disposals atomic.Int32
	var remove func() error
	remove, _ = registry.register(func(*Session) (func() error, error) {
		if err := remove(); err != nil {
			return nil, err
		}
		return func() error { disposals.Add(1); return nil }, nil
	})
	transaction, err := registry.apply(&Session{})
	if err != nil {
		t.Fatal(err)
	}
	if disposals.Load() != 1 {
		t.Fatalf("self-revoked disposal count = %d", disposals.Load())
	}
	if err := transaction.commit(); !IsSubagentServiceError(err, "ACTIVATION_SETUP_REVOKED") {
		t.Fatalf("revoked commit = %v", err)
	}

	registry = newSubagentActivationSetupRegistry()
	var rolledBack []string
	_, _ = registry.register(func(*Session) (func() error, error) {
		return func() error { rolledBack = append(rolledBack, "first"); return nil }, nil
	})
	_, _ = registry.register(func(*Session) (func() error, error) {
		return nil, errors.New("installer failed")
	})
	if _, err := registry.apply(&Session{}); err == nil || err.Error() != "installer failed" {
		t.Fatalf("installer error = %v", err)
	}
	if !reflect.DeepEqual(rolledBack, []string{"first"}) {
		t.Fatalf("setup rollback = %#v", rolledBack)
	}
}

func TestSubagentActivationSetupRegistryAttemptsEveryDisposer(t *testing.T) {
	registry := newSubagentActivationSetupRegistry()
	var released []string
	sequence := 0
	remove, _ := registry.register(func(*Session) (func() error, error) {
		sequence++
		id := sequence
		return func() error {
			released = append(released, string(rune('0'+id)))
			if id == 1 {
				return errors.New("first failed")
			}
			return nil
		}, nil
	})
	for range 3 {
		transaction, err := registry.apply(&Session{})
		if err != nil {
			t.Fatal(err)
		}
		if err := transaction.commit(); err != nil {
			t.Fatal(err)
		}
	}
	if err := remove(); !IsSubagentServiceError(err, "ACTIVATION_SETUP_RELEASE_FAILED") {
		t.Fatalf("removal error = %v", err)
	}
	if !reflect.DeepEqual(released, []string{"1", "2", "3"}) {
		t.Fatalf("released installations = %#v", released)
	}
}

func TestContinuableSubagentSetupReinstallsForColdResume(t *testing.T) {
	e := newPersistentModelSubagentEngine(t)
	var installed atomic.Int32
	var disposed atomic.Int32
	remove, err := e.RegisterContinuableSubagentSetup(func(*Session) (func() error, error) {
		installed.Add(1)
		return func() error { disposed.Add(1); return nil }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = remove() })
	parent, err := e.CreateSession(t.Context(), e.Config().Workspace, "setup-parent", "")
	if err != nil {
		t.Fatal(err)
	}
	result := executeRegisteredTool(t, e, "subagent", parent, map[string]any{
		"description": "setup child", "prompt": "first epoch",
	})
	childID, _ := result.Value.(map[string]any)["subagentId"].(string)
	waitForModelSubagentDetached(t, e, childID)
	if installed.Load() != 1 || disposed.Load() != 1 {
		t.Fatalf("first epoch setup = installed:%d disposed:%d", installed.Load(), disposed.Load())
	}
	if _, err := e.promptContinuableModelSubagent(t.Context(), parent, childID,
		[]ContentBlock{{Type: "text", Text: "second epoch"}},
		map[string]any{"kind": "coordinator", "form": "relay", "senderSessionId": parent}); err != nil {
		t.Fatal(err)
	}
	waitForModelSubagentDetached(t, e, childID)
	if installed.Load() != 2 || disposed.Load() != 2 {
		t.Fatalf("cold epoch setup = installed:%d disposed:%d", installed.Load(), disposed.Load())
	}
}
