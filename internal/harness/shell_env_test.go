package harness

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dop251/goja"
)

func TestShellEnvironmentRegistryValidationCollectionAndDisposal(t *testing.T) {
	registry := newShellEnvironmentRegistry()
	vm := goja.New()
	run := &dynamicCordisRun{runtime: vm, sessionID: "session-a", active: true}
	value, err := vm.RunString(`execution => ({ DSH_TEST_SESSION: execution.agent.session.header.id })`)
	if err != nil {
		t.Fatal(err)
	}
	resolve, ok := goja.AssertFunction(value)
	if !ok {
		t.Fatal("resolver is not callable")
	}
	dispose, err := registry.register(run, shellEnvironmentContributor{
		name: "test-contributor", variables: map[string]string{"DSH_TEST_SESSION": "Current test session."}, resolve: resolve,
	})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{
		cfg: Config{DataDir: t.TempDir()}, sessions: map[string]*Session{
			"session-a": {Header: SessionHeader{ID: "session-a"}},
		}, shellEnv: registry,
	}
	arguments, _ := json.Marshal(map[string]any{"command": "true"})
	values, err := e.collectShellEnvironmentOnLoop(ToolCall{ID: "call-a", Name: "bash", SessionID: "session-a", Arguments: arguments})
	if err != nil {
		t.Fatal(err)
	}
	if values["DSH_HOME"] == "" || values["DSH_SHELL"] != "1" || values["DSH_SESSION_ID"] != "session-a" || values["DSH_TEST_SESSION"] != "session-a" {
		t.Fatalf("collected values = %#v", values)
	}
	rows := registry.list()
	if len(rows) != 2 || rows[0].Key != "DSH_SESSION_JSONL" || rows[1].Key != "DSH_TEST_SESSION" {
		t.Fatalf("registry list = %#v", rows)
	}
	dispose()
	values, err = e.collectShellEnvironmentOnLoop(ToolCall{SessionID: "session-a"})
	if err != nil || values["DSH_TEST_SESSION"] != "" {
		t.Fatalf("disposed contributor values = %#v, %v", values, err)
	}

	if _, err := registry.register(run, shellEnvironmentContributor{name: "reserved", variables: map[string]string{"DSH_HOME": "bad"}, resolve: resolve}); err == nil || !strings.Contains(err.Error(), "reserved key") {
		t.Fatalf("reserved key error = %v", err)
	}
}

func TestShellEnvironmentRegistryRejectsUndeclaredResolution(t *testing.T) {
	registry := newShellEnvironmentRegistry()
	vm := goja.New()
	run := &dynamicCordisRun{runtime: vm, active: true}
	resolve, _ := goja.AssertFunction(vm.ToValue(func(goja.FunctionCall) goja.Value {
		return vm.ToValue(map[string]any{"DSH_UNDECLARED": "bad"})
	}))
	if _, err := registry.register(run, shellEnvironmentContributor{name: "drifted", variables: map[string]string{"DSH_DECLARED": "Declared."}, resolve: resolve}); err != nil {
		t.Fatal(err)
	}
	e := &Engine{cfg: Config{DataDir: t.TempDir()}, sessions: map[string]*Session{}, shellEnv: registry}
	if _, err := e.collectShellEnvironmentOnLoop(ToolCall{}); err == nil || !strings.Contains(err.Error(), "undeclared key") {
		t.Fatalf("undeclared resolution error = %v", err)
	}
}
