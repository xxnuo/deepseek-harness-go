package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func registeredTool(t *testing.T, e *Engine, name string) Tool {
	t.Helper()
	e.mu.RLock()
	tool, ok := e.tools[name]
	e.mu.RUnlock()
	if !ok {
		t.Fatalf("tool %q is not registered", name)
	}
	return tool
}

func createToolSession(t *testing.T, e *Engine) string {
	t.Helper()
	id, err := e.CreateSession(context.Background(), e.Config().Workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func toolResultText(result ToolResult) string {
	var text strings.Builder
	for _, block := range result.Content {
		if block.Type == "text" {
			text.WriteString(block.Text)
		}
	}
	return text.String()
}

func projectionValues(t *testing.T, e *Engine, sessionID string) map[string]any {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"sessionId": sessionID})
	result, rpcErr := e.dispatch(context.Background(), "session.history", raw)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	history := result.(map[string]any)
	projection := history["projections"].(map[string]any)
	return projection["values"].(map[string]any)
}

func waitProjection(t *testing.T, frames <-chan map[string]any, key string) map[string]any {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case frame := <-frames:
			if frame["type"] == "session/projection" && frame["key"] == key {
				return frame
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for %q projection", key)
		}
	}
}

func TestBuiltinTodoAndSkillSchemas(t *testing.T) {
	e := newIntegrationEngine(t)
	todo := registeredTool(t, e, "todo_write").Schema.Parameters
	if todo["additionalProperties"] != false {
		t.Fatalf("todo_write schema = %#v", todo)
	}
	properties := todo["properties"].(map[string]any)
	if len(properties) != 1 {
		t.Fatalf("todo_write properties = %#v", properties)
	}
	todos := properties["todos"].(map[string]any)
	item := todos["items"].(map[string]any)
	itemProperties := item["properties"].(map[string]any)
	status := itemProperties["status"].(map[string]any)
	if !reflect.DeepEqual(status["enum"], []any{"pending", "in_progress", "completed"}) {
		t.Fatalf("todo status enum = %#v", status["enum"])
	}

	skill := registeredTool(t, e, "skill").Schema.Parameters
	skillProperties := skill["properties"].(map[string]any)
	if len(skillProperties) != 1 || skillProperties["name"] == nil || skill["additionalProperties"] != false {
		t.Fatalf("skill schema = %#v", skill)
	}
}

func TestTodoWriteUpdatesAndClearsProjection(t *testing.T) {
	e := newIntegrationEngine(t)
	id := createToolSession(t, e)
	tool := registeredTool(t, e, "todo_write")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := e.SubscribeMux(ctx)

	result, err := tool.Execute(context.Background(), ToolCall{
		Name: "todo_write", SessionID: id,
		Arguments: json.RawMessage(`{"todos":[{"content":"  plan  ","status":"in_progress"},{"content":"build","status":"in_progress"},{"content":"ship","status":"pending"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := toolResultText(result); got != "Updated todo list: 1 pending, 2 in progress, 0 completed." {
		t.Fatalf("todo result = %q", got)
	}
	wantFirst := []TodoItem{
		{Content: "plan", Status: "in_progress"},
		{Content: "build", Status: "in_progress"},
		{Content: "ship", Status: "pending"},
	}
	frame := waitProjection(t, frames, "todos")
	if frame["sessionId"] != id || !reflect.DeepEqual(frame["value"], wantFirst) {
		t.Fatalf("todo projection frame = %#v", frame)
	}
	if got := projectionValues(t, e, id)["todos"]; !reflect.DeepEqual(got, wantFirst) {
		t.Fatalf("history todos = %#v", got)
	}

	wantSecond := []TodoItem{{Content: "ship", Status: "completed"}}
	if _, err := tool.Execute(context.Background(), ToolCall{
		Name: "todo_write", SessionID: id,
		Arguments: json.RawMessage(`{"todos":[{"content":"ship","status":"completed"}]}`),
	}); err != nil {
		t.Fatal(err)
	}
	_ = waitProjection(t, frames, "todos")
	s := mustSession(t, e, id)
	if _, err := e.appendEvent(s, "turn/end", map[string]any{"turn": 1}); err != nil {
		t.Fatal(err)
	}
	if got := projectionValues(t, e, id)["todos"]; !reflect.DeepEqual(got, wantSecond) {
		t.Fatalf("turn/end todos = %#v", got)
	}
	rows := e.ListSessions()
	if len(rows) != 1 {
		t.Fatalf("session rows = %#v", rows)
	}
	listed := rows[0].Projections.(map[string]any)["values"].(map[string]any)
	if !reflect.DeepEqual(listed["todos"], wantSecond) {
		t.Fatalf("listed todos = %#v", listed["todos"])
	}
	if _, err := e.appendEvent(s, "turn/start", map[string]any{"turn": 2}); err != nil {
		t.Fatal(err)
	}
	cleared := waitProjection(t, frames, "todos")
	if cleared["value"] != nil || projectionValues(t, e, id)["todos"] != nil {
		t.Fatalf("cleared projection = %#v", cleared)
	}
}

func TestTodoWriteRejectsInvalidArguments(t *testing.T) {
	e := newIntegrationEngine(t)
	id := createToolSession(t, e)
	tool := registeredTool(t, e, "todo_write")
	cases := map[string]string{
		"missing":           `{}`,
		"null":              `{"todos":null}`,
		"not-array":         `{"todos":"nope"}`,
		"unknown-top":       `{"todos":[],"extra":true}`,
		"null-item":         `{"todos":[null]}`,
		"unknown-item":      `{"todos":[{"content":"a","status":"pending","children":[]}]}`,
		"missing-content":   `{"todos":[{"status":"pending"}]}`,
		"empty-content":     `{"todos":[{"content":"   ","status":"pending"}]}`,
		"missing-status":    `{"todos":[{"content":"a"}]}`,
		"invalid-status":    `{"todos":[{"content":"a","status":"doing"}]}`,
		"duplicate-content": `{"todos":[{"content":" a ","status":"pending"},{"content":"a","status":"completed"}]}`,
		"multiple-values":   `{"todos":[]} {}`,
	}
	for name, arguments := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := tool.Execute(context.Background(), ToolCall{Name: "todo_write", SessionID: id, Arguments: json.RawMessage(arguments)}); err == nil {
				t.Fatalf("accepted %s", arguments)
			}
		})
	}
	if _, err := tool.Execute(context.Background(), ToolCall{Name: "todo_write", Arguments: json.RawMessage(`{"todos":[]}`)}); err == nil || !strings.Contains(err.Error(), "owning agent session") {
		t.Fatalf("missing session error = %v", err)
	}
	if _, err := tool.Execute(context.Background(), ToolCall{Name: "todo_write", SessionID: "missing", Arguments: json.RawMessage(`{"todos":[]}`)}); err == nil || !strings.Contains(err.Error(), "session-not-found") {
		t.Fatalf("unknown session error = %v", err)
	}
	s := mustSession(t, e, id)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.Events {
		if event.Type == "todo/write" {
			t.Fatalf("invalid call appended %#v", event)
		}
	}
}

func TestTodoWriteHonorsSingleActivePolicy(t *testing.T) {
	e := newIntegrationEngine(t)
	id := createToolSession(t, e)
	allowed := false
	cfg := e.Config()
	cfg.TodoAllowParallelInProgress = &allowed
	if err := e.ApplyRuntimeConfig(cfg); err != nil {
		t.Fatal(err)
	}
	tool := registeredTool(t, e, "todo_write")
	_, err := tool.Execute(context.Background(), ToolCall{
		Name: "todo_write", SessionID: id,
		Arguments: json.RawMessage(`{"todos":[{"content":"one","status":"in_progress"},{"content":"two","status":"in_progress"}]}`),
	})
	if err == nil || !strings.Contains(err.Error(), "at most one task may be in_progress (got 2)") {
		t.Fatalf("single-active policy error = %v", err)
	}
}

func writeTestSkill(t *testing.T, root, dir, document string) string {
	t.Helper()
	path := filepath.Join(root, dir, "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSkillLoadsRealInstructionsAndEnforcesPolicy(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := filepath.Join(home, "configured-skills")
	goodPath := writeTestSkill(t, root, "project-skill", "---\nname: project-skill\ndescription: Project help\n---\n\nFollow the real instructions.\n")
	writeTestSkill(t, root, "hidden-skill", "---\nname: hidden-skill\ndescription: Hidden help\ndisable-model-invocation: true\n---\n\nSECRET INSTRUCTIONS\n")
	writeTestSkill(t, root, "bad-name", "---\nname: Bad_Name\ndescription: Invalid\n---\n\nInvalid body.\n")
	writeTestSkill(t, root, "missing-description", "---\nname: missing-description\n---\n\nInvalid body.\n")

	cfg := DefaultConfig()
	cfg.SessionTitleLLM.Enabled = false
	cfg.DataDir = filepath.Join(home, "data")
	cfg.Workspace = filepath.Join(home, "workspace")
	cfg.SkillDir = root
	cfg.Provider = "echo"
	cfg.Model = "echo"
	cfg.Persist = false
	if err := os.MkdirAll(cfg.Workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	e, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	id := createToolSession(t, e)
	tool := registeredTool(t, e, "skill")

	result, err := tool.Execute(context.Background(), ToolCall{
		Name: "skill", SessionID: id, Arguments: json.RawMessage(`{"name":"project-skill"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		`<skill_content name="project-skill">`,
		"<skill_resources>",
		"Base directory for this skill: " + filepath.Dir(goodPath),
		"Resolve relative paths mentioned by this skill against the base directory before using them. Load referenced resources only as needed.",
		"</skill_resources>",
		"",
		"<skill_instructions>",
		"Follow the real instructions.",
		"</skill_instructions>",
		"</skill_content>",
	}, "\n")
	if got := toolResultText(result); got != want {
		t.Fatalf("skill result = %q\nwant %q", got, want)
	}

	errorsByArgs := map[string]string{
		`{}`:                                    "required",
		`{"name":null}`:                         "required",
		`{"name":"Bad_Name"}`:                   "invalid skill name",
		`{"name":" project-skill"}`:             "invalid skill name",
		`{"name":"missing-skill"}`:              "unknown or no longer available",
		`{"name":"hidden-skill"}`:               "not available for model invocation",
		`{"name":"project-skill","extra":true}`: "unknown field",
		`{"name":"project-skill"} {}`:           "multiple JSON values",
	}
	for arguments, fragment := range errorsByArgs {
		_, err := tool.Execute(context.Background(), ToolCall{Name: "skill", SessionID: id, Arguments: json.RawMessage(arguments)})
		if err == nil || !strings.Contains(err.Error(), fragment) {
			t.Fatalf("skill(%s) error = %v, want %q", arguments, err, fragment)
		}
		if strings.Contains(arguments, "hidden-skill") && strings.Contains(err.Error(), "SECRET") {
			t.Fatal("disabled skill content was disclosed")
		}
	}

	raw, _ := json.Marshal(map[string]any{"sessionId": id})
	listed, rpcErr := e.dispatch(context.Background(), "skill.list", raw)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	rows := listed.(map[string]any)["skills"].([]map[string]any)
	byName := map[string]map[string]any{}
	for _, row := range rows {
		byName[row["name"].(string)] = row
	}
	if byName["project-skill"] == nil || byName["hidden-skill"]["modelInvocable"] != false {
		t.Fatalf("skill list = %#v", rows)
	}
	if byName["Bad_Name"] != nil || byName["missing-description"] != nil {
		t.Fatalf("invalid skills listed = %#v", rows)
	}
}
