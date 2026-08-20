package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestGlobalDynamicCordisProfilePluginReachesEverySession(t *testing.T) {
	e := newIntegrationEngine(t)
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		Plugin:  DynamicCordisPluginSelector{Kind: "new", IDPrefix: "prof"},
		Name:    "global profile fixture",
		Purpose: "verify host profile ownership",
		Global:  true,
		Code: DynamicCordisCode{Host: `
const created = []
harness.handle('created', () => created)
return {
  inject: ['tools', 'systemPrompt'],
  apply(ctx) {
    ctx.on('session/created', session => created.push(session.id))
    ctx.systemPrompt.section({ name: 'profile:fixture', order: 1, text: 'GLOBAL PROFILE MARKER' })
    ctx.tools.register(harness.defineTool({
      name: 'profile_probe',
      description: 'Return the current session id.',
      parameters: {},
      output: {
        schema: { type: 'string' },
        render(_args, value) { return [{ type: 'text', text: value }] },
      },
      execute(_args, call) { return call.sessionId },
    }))
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.DynamicCordisRun(t.Context(), "", receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}

	for _, id := range []string{"profile-session-a", "profile-session-b"} {
		if _, err := e.CreateSession(t.Context(), e.Config().Workspace, id, ""); err != nil {
			t.Fatal(err)
		}
		session, err := e.getSession(id)
		if err != nil {
			t.Fatal(err)
		}
		tools, err := e.toolsForSession(session)
		if err != nil {
			t.Fatal(err)
		}
		if !toolSchemaNamed(tools, "profile_probe") {
			t.Fatalf("profile_probe is not visible in %s: %#v", id, tools)
		}
		agent, err := e.runtimeForSession(session)
		if err != nil {
			t.Fatal(err)
		}
		prompt, err := e.systemPromptForSession(session, session.Model, agent)
		if err != nil || !strings.Contains(prompt, "GLOBAL PROFILE MARKER") {
			t.Fatalf("system prompt for %s = %q, %v", id, prompt, err)
		}
	}

	e.mu.RLock()
	tool := e.tools["profile_probe"]
	e.mu.RUnlock()
	arguments, _ := json.Marshal(map[string]any{})
	result, err := tool.Execute(context.Background(), ToolCall{ID: "profile-call", Name: "profile_probe", SessionID: "profile-session-b", Arguments: arguments})
	if err != nil || result.Value != "profile-session-b" {
		t.Fatalf("global profile tool = %#v, %v", result, err)
	}
	created := e.DynamicCordisInvoke(t.Context(), receipt.PluginID, run.PluginRunID, "created", nil)
	if !created.OK || !jsonEqual(created.Value, []any{"profile-session-a", "profile-session-b"}) {
		t.Fatalf("session/created events = %#v", created)
	}
}

func TestGlobalDynamicCordisProfileHostServicesUseExplicitAgentScope(t *testing.T) {
	e := newIntegrationEngine(t)
	ids := []string{"profile-host-a", "profile-host-b"}
	for _, id := range ids {
		if _, err := e.CreateSession(t.Context(), e.Config().Workspace, id, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := e.goalMutation(id, "", "create", "goal for "+id, "", 0, 4); err != nil {
			t.Fatal(err)
		}
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		Plugin:  DynamicCordisPluginSelector{Kind: "new", IDPrefix: "host"},
		Name:    "global host services fixture",
		Purpose: "verify explicit agent scope from a profile plugin",
		Global:  true,
		Code: DynamicCordisCode{Host: `
return {
  inject: ['agents', 'goals', 'jobs', 'systemPrompt'],
  apply(ctx) {
    ctx.jobs.attachController('global-profile')
    ctx.systemPrompt.section({ name: 'profile:host-services', order: 1, text: 'GLOBAL HOST SERVICE MARKER' })
    harness.handle('exercise', async ids => {
      const rows = []
      for (const id of ids) {
        const agent = ctx.agents.get(id)
        const goal = ctx.goals.get(agent)
        const assembly = await ctx.systemPrompt.assemble({ scope: agent, agent, label: id })
        const jobId = ctx.jobs.start({
          kind: 'profile', label: id, owner: agent,
          run() { return { done: Promise.resolve({ status: 'completed', detail: id }), cancel() {} } },
        })
        const job = await ctx.jobs.wait(jobId, 1000, agent)
        rows.push({
          id,
          goal: goal.objective,
          prompt: assembly.sections.some(section => section.text === 'GLOBAL HOST SERVICE MARKER'),
          job: job.status,
        })
      }
      return rows
    })
  },
}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.DynamicCordisRun(t.Context(), "", receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	result := e.DynamicCordisInvoke(t.Context(), receipt.PluginID, run.PluginRunID, "exercise", ids)
	if !result.OK {
		t.Fatalf("exercise = %#v", result)
	}
	rows := result.Value.([]any)
	if len(rows) != len(ids) {
		t.Fatalf("rows = %#v", rows)
	}
	for index, raw := range rows {
		row := raw.(map[string]any)
		if row["id"] != ids[index] || row["goal"] != "goal for "+ids[index] || row["prompt"] != true || row["job"] != "completed" {
			t.Fatalf("rows[%d] = %#v", index, row)
		}
	}
	if stopped, err := e.DynamicCordisStop("", receipt.PluginID); err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	if e.jobs.hasController(ids[0]) {
		t.Fatal("global profile job controller remained attached after stop")
	}
}

func toolSchemaNamed(tools []ToolSchema, name string) bool {
	for _, tool := range tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}
