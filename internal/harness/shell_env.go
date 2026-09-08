package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/dop251/goja"
)

var shellEnvironmentKey = regexp.MustCompile(`^DSH_[A-Z][A-Z0-9_]*$`)

type shellEnvironmentVariableInfo struct {
	Contributor string `json:"contributor"`
	Description string `json:"description"`
	Key         string `json:"key"`
}

type shellEnvironmentContributor struct {
	name      string
	variables map[string]string
	resolve   goja.Callable
	owner     *dynamicCordisRun
	sessionID string
}

type shellEnvironmentRegistry struct {
	mu           sync.RWMutex
	contributors map[string]*shellEnvironmentContributor
	keyOwners    map[string]string
}

func newShellEnvironmentRegistry() *shellEnvironmentRegistry {
	return &shellEnvironmentRegistry{
		contributors: map[string]*shellEnvironmentContributor{},
		keyOwners:    map[string]string{},
	}
}

func (r *shellEnvironmentRegistry) register(owner *dynamicCordisRun, contributor shellEnvironmentContributor) (func(), error) {
	contributor.name = strings.TrimSpace(contributor.name)
	if contributor.name == "" {
		return nil, errors.New("bash env contributor name must be non-empty")
	}
	if contributor.resolve == nil {
		return nil, fmt.Errorf("bash env contributor %q requires resolve", contributor.name)
	}
	for key, description := range contributor.variables {
		if !shellEnvironmentKey.MatchString(key) {
			return nil, fmt.Errorf("bash env contributor %q declared invalid key %q", contributor.name, key)
		}
		if key == "DSH_HOME" || key == "DSH_SHELL" || key == "DSH_SESSION_ID" {
			return nil, fmt.Errorf("bash env contributor %q cannot own reserved key %q", contributor.name, key)
		}
		if strings.TrimSpace(description) == "" {
			return nil, fmt.Errorf("bash env contributor %q must describe %q", contributor.name, key)
		}
	}
	contributor.owner = owner
	if owner != nil {
		contributor.sessionID = owner.ownerSessionID()
	}
	r.mu.Lock()
	if _, exists := r.contributors[contributor.name]; exists {
		r.mu.Unlock()
		return nil, fmt.Errorf("bash env contributor %q is already registered", contributor.name)
	}
	for key := range contributor.variables {
		if existing := r.keyOwners[key]; existing != "" {
			r.mu.Unlock()
			return nil, fmt.Errorf("bash env key %q is already owned by contributor %q; contributor %q cannot also own it", key, existing, contributor.name)
		}
	}
	registered := &contributor
	r.contributors[contributor.name] = registered
	for key := range contributor.variables {
		r.keyOwners[key] = contributor.name
	}
	r.mu.Unlock()
	var once sync.Once
	dispose := func() {
		once.Do(func() {
			r.mu.Lock()
			if r.contributors[contributor.name] == registered {
				delete(r.contributors, contributor.name)
				for key := range contributor.variables {
					delete(r.keyOwners, key)
				}
			}
			r.mu.Unlock()
		})
	}
	return dispose, nil
}

func (r *shellEnvironmentRegistry) list() []shellEnvironmentVariableInfo {
	r.mu.RLock()
	rows := []shellEnvironmentVariableInfo{}
	for _, contributor := range r.contributors {
		for key, description := range contributor.variables {
			rows = append(rows, shellEnvironmentVariableInfo{Contributor: contributor.name, Description: description, Key: key})
		}
	}
	r.mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return rows
}

func (e *Engine) collectShellEnvironment(call ToolCall) (map[string]string, error) {
	var values map[string]string
	var collectErr error
	if !e.dynamicCordis.loop.call(func() { values, collectErr = e.collectShellEnvironmentOnLoop(call) }) {
		return nil, errors.New("dynamic Cordis runtime is closed")
	}
	return values, collectErr
}

func (e *Engine) collectShellEnvironmentOnLoop(call ToolCall) (map[string]string, error) {
	values := e.trustedDSHEnvironmentForSession(call.SessionID)
	e.shellEnv.mu.RLock()
	contributors := make([]*shellEnvironmentContributor, 0, len(e.shellEnv.contributors))
	for _, contributor := range e.shellEnv.contributors {
		if contributor.sessionID == "" || contributor.sessionID == call.SessionID {
			contributors = append(contributors, contributor)
		}
	}
	e.shellEnv.mu.RUnlock()
	sort.Slice(contributors, func(i, j int) bool { return contributors[i].name < contributors[j].name })
	execution := e.shellEnvironmentExecution(call)
	for _, contributor := range contributors {
		owner := contributor.owner
		if owner == nil || owner.runtime == nil {
			continue
		}
		owner.mu.Lock()
		active := !owner.disposed && (owner.active || owner.activating)
		owner.mu.Unlock()
		if !active {
			continue
		}
		resolved, err := contributor.resolve(goja.Undefined(), owner.runtime.ToValue(execution))
		if err != nil {
			return nil, fmt.Errorf("bash env contributor %q: %s", contributor.name, dynamicJSMessage(err))
		}
		exported := resolved.Export()
		if exported == nil {
			continue
		}
		object, ok := exported.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("bash env contributor %q returned a non-object value", contributor.name)
		}
		for key, raw := range object {
			if _, declared := contributor.variables[key]; !declared {
				return nil, fmt.Errorf("bash env contributor %q returned undeclared key %q", contributor.name, key)
			}
			value, ok := raw.(string)
			if !ok {
				return nil, fmt.Errorf("bash env contributor %q returned a non-string value for %q", contributor.name, key)
			}
			values[key] = value
		}
	}
	return values, nil
}

func (e *Engine) shellEnvironmentExecution(call ToolCall) map[string]any {
	arguments := any(map[string]any{})
	if len(call.Arguments) > 0 {
		_ = json.Unmarshal(call.Arguments, &arguments)
	}
	execution := map[string]any{
		"signal": map[string]any{"aborted": false}, "callId": call.ID, "rootCallId": call.ID,
		"name": call.Name, "arguments": arguments,
	}
	if session, err := e.getSession(call.SessionID); err == nil {
		header := any(map[string]any{})
		if data, err := json.Marshal(session.Header); err == nil {
			_ = json.Unmarshal(data, &header)
		}
		execution["agent"] = map[string]any{"session": map[string]any{"header": header}}
	}
	return execution
}

func (e *Engine) trustedDSHEnvironmentForSession(sessionID string) map[string]string {
	dshHome, err := filepath.Abs(e.cfg.DataDir)
	if err != nil {
		dshHome = e.cfg.DataDir
	}
	env := map[string]string{"DSH_HOME": dshHome, "DSH_SHELL": "1"}
	if strings.TrimSpace(sessionID) == "" {
		return env
	}
	env["DSH_SESSION_ID"] = sessionID
	return env
}
