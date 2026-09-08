package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const noActiveTeamPeerMessage = "No other Team member is running or provisioning. wait_agent cannot make progress or wake inactive teammates. Re-list with list_agents and team_task_list, then use send_message to wake each required inactive teammate before waiting again."

const agentTeamPolicy = `Agent Teams is available in this session, but create teammates only when the user explicitly asks to use Agent Teams or teammates.

The Team Lead and all teammates share the same working directory and filesystem. Edits are immediately visible to every member. Split write work into disjoint scopes, record expected write scopes on shared tasks, and use task dependencies when work must be ordered. Write-scope overlap is advisory, not a lock.

Prefer read/edit/write for file changes. If a file operation returns FS_STALE_VERSION, read the current file, rebase your intended change onto the new content, and retry. Bash, formatters, code generators, and scripts are not fully protected by the filesystem version guard; coordinate them explicitly and have the Lead review the final diff and run tests.

send_message steers a running target at its nearest step boundary, starts an idle target, and cold-resumes an inactive teammate. A delivered peer item starts with its stable message id and sender name. A successful send is already durable even when its result says queued; do not resend it. Shared-task workflow is list, get, claim with the current revision, perform the work, then complete. Task readiness never starts an owner. Before wait_agent, use list_agents and make sure another required member is running or provisioning; use send_message first when the required member is inactive. wait_agent observes only changes after that call starts, never wakes a member, and returns noProgress immediately when no other member can produce a change. Re-list after wakeup or timeout. The Lead must wait for required teammates before giving the final answer.`

var agentTeamToolNames = map[string]bool{
	"spawn_teammate": true, "send_message": true, "list_agents": true,
	"wait_agent": true, "interrupt_agent": true, "team_task_create": true, "team_task_list": true,
	"team_task_get": true, "team_task_update": true,
}

func isAgentTeamToolName(name string) bool { return agentTeamToolNames[name] }

func legacyToolShadowedByAgentTeam(e *Engine, name string) (Tool, bool) {
	switch name {
	case "send_message":
		return builtinSendMessageTool(e), true
	case "interrupt_agent":
		return builtinInterruptAgentTool(e), true
	case "list_agents":
		return builtinListAgentsTool(e), true
	default:
		return Tool{}, false
	}
}

func muxAgentTeamTool(e *Engine, team, legacy Tool) Tool {
	teamOutput, legacyOutput := cloneJSON(team.Schema.Output), cloneJSON(legacy.Schema.Output)
	teamExecute, legacyExecute := team.Execute, legacy.Execute
	team.Schema.Output = map[string]any{"anyOf": []any{teamOutput, legacyOutput}}
	team.Execute = func(ctx context.Context, call ToolCall) (ToolResult, error) {
		session, err := e.getSession(call.SessionID)
		if err != nil {
			return ToolResult{}, err
		}
		runtimeConfig, err := e.runtimeForSession(session)
		if err != nil {
			return ToolResult{}, err
		}
		switch e.agentTeamToolMode(session, runtimeConfig, call.Name) {
		case "team":
			return teamExecute(ctx, call)
		case "legacy":
			return legacyExecute(ctx, call)
		default:
			return ToolResult{}, fmt.Errorf("unknown tool: %s", call.Name)
		}
	}
	return team
}

func teamMemberViewSchema() map[string]any {
	return objectSchema(map[string]any{
		"id": map[string]any{"type": "string"}, "name": map[string]any{"type": "string"},
		"role":        map[string]any{"type": "string", "enum": []string{"lead", "teammate"}},
		"status":      map[string]any{"type": "string", "enum": []string{"running", "idle", "inactive", "provisioning", "failed"}},
		"description": map[string]any{"type": "string"}, "provider": map[string]any{"type": "string"},
		"context":     map[string]any{"type": "string", "enum": []string{"fresh", "fork"}},
		"model":       map[string]any{"type": "string"},
		"diagnostics": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
	}, "id", "name", "role", "status", "diagnostics")
}

func teamTaskViewSchema() map[string]any {
	return objectSchema(map[string]any{
		"id": map[string]any{"type": "string"}, "revision": map[string]any{"type": "integer"},
		"subject": map[string]any{"type": "string"}, "description": map[string]any{"type": "string"},
		"status":             map[string]any{"type": "string", "enum": []string{"pending", "in_progress", "completed", "deleted"}},
		"ownerName":          map[string]any{"type": "string"},
		"blockedBy":          map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		"writeScopes":        map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		"ready":              map[string]any{"type": "boolean"},
		"writeScopeWarnings": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
	}, "id", "revision", "subject", "description", "status", "blockedBy", "writeScopes", "ready", "writeScopeWarnings")
}

func builtinAgentTeamTools(e *Engine) []Tool {
	return []Tool{
		builtinSpawnTeammateTool(e),
		builtinTeamMessageTool(e),
		builtinTeamListAgentsTool(e),
		builtinTeamWaitAgentTool(e),
		builtinTeamInterruptAgentTool(e),
		builtinTeamTaskCreateTool(e),
		builtinTeamTaskListTool(e),
		builtinTeamTaskGetTool(e),
		builtinTeamTaskUpdateTool(e),
	}
}

func requireTeamCaller(call ToolCall) error {
	if call.SessionID == "" {
		return errors.New("Agent Teams tool requires a calling agent session")
	}
	return nil
}

func builtinSpawnTeammateTool(e *Engine) Tool {
	type input struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Prompt      string `json:"prompt"`
		Context     string `json:"context"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "spawn_teammate", Description: "Create one named, durable teammate. Only the Team Lead may call this tool.",
			Parameters: objectSchema(map[string]any{
				"name":        map[string]any{"type": "string", "description": "Unique lower-kebab-case teammate name."},
				"description": map[string]any{"type": "string", "description": "Short description of the delegated responsibility."},
				"prompt":      map[string]any{"type": "string", "description": "Complete initial task for the teammate."},
				"context": map[string]any{
					"type": "string", "enum": []string{"fresh", "fork"},
					"description": "fresh starts without Lead history; fork inherits completed Lead turns. Defaults to fresh.",
				},
			}, "name", "description", "prompt"),
			Output: objectSchema(map[string]any{"member": teamMemberViewSchema()}, "member"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			if err := requireTeamCaller(call); err != nil {
				return ToolResult{}, err
			}
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if in.Context == "" {
				in.Context = "fresh"
			}
			provider := e.agentTeams.config.FreshProvider
			if in.Context == "fork" {
				provider = e.agentTeams.config.ForkProvider
			}
			value, err := e.agentTeams.SpawnTeammate(ctx, call.SessionID, SpawnTeammateRequest{
				Name: in.Name, Description: in.Description, Prompt: []ContentBlock{{Type: "text", Text: in.Prompt}},
				Context: in.Context, Provider: provider,
			})
			if err != nil {
				return ToolResult{}, err
			}
			return jsonToolResult(value)
		},
	}
}

func builtinTeamMessageTool(e *Engine) Tool {
	type input struct {
		Target  string `json:"target"`
		Message string `json:"message"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "send_message", Description: "Send one durable message to another Team member. A running target receives it at the nearest step boundary; an idle target starts a turn; an inactive teammate cold-resumes.",
			Parameters: objectSchema(map[string]any{
				"target":  map[string]any{"type": "string", "description": "Team member name, or lead."},
				"message": map[string]any{"type": "string", "description": "Self-contained message for the target."},
			}, "target", "message"),
			Output: objectSchema(map[string]any{
				"messageId": map[string]any{"type": "string"}, "status": map[string]any{"type": "string", "enum": []string{"accepted", "queued"}},
			}, "messageId", "status"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			if err := requireTeamCaller(call); err != nil {
				return ToolResult{}, err
			}
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			value, err := e.agentTeams.SendMessage(ctx, call.SessionID, SendTeamMessageRequest{
				Target: in.Target, Content: []ContentBlock{{Type: "text", Text: in.Message}},
			})
			if err != nil {
				return ToolResult{}, err
			}
			return jsonToolResult(value)
		},
	}
}

func builtinTeamListAgentsTool(e *Engine) Tool {
	return Tool{
		Schema: ToolSchema{
			Name: "list_agents", Description: "List the Lead and every durable teammate with current runtime status.",
			Parameters: objectSchema(map[string]any{}),
			Output:     map[string]any{"type": "array", "items": teamMemberViewSchema()},
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			if err := requireTeamCaller(call); err != nil {
				return ToolResult{}, err
			}
			members, err := e.agentTeams.ListMembers(call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			return jsonToolResult(members)
		},
	}
}

func builtinTeamWaitAgentTool(e *Engine) Tool {
	type input struct {
		TimeoutMS *int `json:"timeout_ms"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "wait_agent", Description: "Wait for the next teammate status, mailbox, or shared-task change after this call starts. This never wakes inactive members and returns noProgress immediately when no other member is running or provisioning. Re-list after wakeup or timeout instead of polling.",
			Parameters: objectSchema(map[string]any{"timeout_ms": map[string]any{
				"type": "integer", "description": "Wait duration in milliseconds, from 10000 through 3600000. Defaults to 30000.",
			}}),
			Output: objectSchema(map[string]any{
				"timedOut": map[string]any{"type": "boolean"},
				"noProgress": objectSchema(map[string]any{
					"reason": map[string]any{"type": "string", "const": "no-active-peer"}, "message": map[string]any{"type": "string"},
				}, "reason", "message"),
			}, "timedOut"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			if err := requireTeamCaller(call); err != nil {
				return ToolResult{}, err
			}
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			timeoutMS := 30_000
			if in.TimeoutMS != nil {
				timeoutMS = *in.TimeoutMS
			}
			value, noProgress, err := e.agentTeams.waitForModelChange(ctx, call.SessionID, time.Duration(timeoutMS)*time.Millisecond)
			if err != nil {
				return ToolResult{}, err
			}
			if noProgress {
				return jsonToolResult(map[string]any{
					"timedOut":   false,
					"noProgress": map[string]any{"reason": "no-active-peer", "message": noActiveTeamPeerMessage},
				})
			}
			return jsonToolResult(value)
		},
	}
}

func builtinTeamInterruptAgentTool(e *Engine) Tool {
	type input struct {
		Target string `json:"target"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "interrupt_agent", Description: "Interrupt one teammate's current turn while preserving its pending inbox. Team Lead only.",
			Parameters: objectSchema(map[string]any{"target": map[string]any{"type": "string", "description": "Teammate name."}}, "target"),
			Output:     objectSchema(map[string]any{"previousStatus": map[string]any{"type": "string", "enum": []string{"running", "idle", "inactive"}}}, "previousStatus"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			if err := requireTeamCaller(call); err != nil {
				return ToolResult{}, err
			}
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			status, err := e.agentTeams.Interrupt(call.SessionID, in.Target)
			if err != nil {
				return ToolResult{}, err
			}
			return jsonToolResult(map[string]any{"previousStatus": status})
		},
	}
}

func builtinTeamTaskCreateTool(e *Engine) Tool {
	type input struct {
		Subject     string   `json:"subject"`
		Description string   `json:"description"`
		BlockedBy   []string `json:"blocked_by"`
		WriteScopes []string `json:"write_scopes"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "team_task_create", Description: "Create one unowned pending task on the shared Team task board.",
			Parameters: objectSchema(map[string]any{
				"subject":     map[string]any{"type": "string", "description": "Concise task title."},
				"description": map[string]any{"type": "string", "description": "Complete task details and acceptance criteria."},
				"blocked_by": map[string]any{
					"type": "array", "items": map[string]any{"type": "string"}, "description": "Task ids that must complete first.",
				},
				"write_scopes": map[string]any{
					"type": "array", "items": map[string]any{"type": "string"},
					"description": "Advisory workspace-relative file or directory prefixes this task expects to modify.",
				},
			}, "subject", "description"), Output: teamTaskViewSchema(),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			if err := requireTeamCaller(call); err != nil {
				return ToolResult{}, err
			}
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			value, err := e.agentTeams.CreateTask(call.SessionID, CreateTeamTaskRequest{
				Subject: in.Subject, Description: in.Description, BlockedBy: in.BlockedBy, WriteScopes: in.WriteScopes,
			})
			if err != nil {
				return ToolResult{}, err
			}
			return jsonToolResult(value)
		},
	}
}

func builtinTeamTaskListTool(e *Engine) Tool {
	type input struct {
		Status *string `json:"status"`
		Owner  *string `json:"owner"`
		Ready  *bool   `json:"ready"`
		Cursor *int    `json:"cursor"`
		Limit  *int    `json:"limit"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "team_task_list", Description: "List shared tasks, including readiness, owner, revision, blockers, and write-scope warnings.",
			Parameters: objectSchema(map[string]any{
				"status": map[string]any{
					"type": "string", "enum": []string{"pending", "in_progress", "completed"}, "description": "Optional exact status filter.",
				},
				"owner": map[string]any{
					"type": "string", "description": "Optional member-name filter; use unowned for tasks without an owner.",
				},
				"ready":  map[string]any{"type": "boolean", "description": "Optional readiness filter."},
				"cursor": map[string]any{"type": "integer", "description": "Zero-based result offset. Defaults to 0."},
				"limit":  map[string]any{"type": "integer", "description": "Number of rows, 1 through 100. Defaults to 50."},
			}),
			Output: objectSchema(map[string]any{
				"tasks": map[string]any{"type": "array", "items": teamTaskViewSchema()}, "nextCursor": map[string]any{"type": "integer"},
			}, "tasks"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			if err := requireTeamCaller(call); err != nil {
				return ToolResult{}, err
			}
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			cursor, limit := 0, 50
			if in.Cursor != nil {
				cursor = *in.Cursor
			}
			if in.Limit != nil {
				limit = *in.Limit
			}
			if cursor < 0 || int64(cursor) > maxJSONSafeInteger {
				return ToolResult{}, errors.New("cursor must be a non-negative safe integer")
			}
			if limit < 1 || int64(limit) > maxJSONSafeInteger || limit > 100 {
				return ToolResult{}, errors.New("limit must be an integer from 1 through 100")
			}
			tasks, err := e.agentTeams.ListTasks(call.SessionID)
			if err != nil {
				return ToolResult{}, err
			}
			filtered := make([]TeamTaskView, 0, len(tasks))
			for _, task := range tasks {
				if in.Status != nil && task.Status != *in.Status || in.Ready != nil && task.Ready != *in.Ready {
					continue
				}
				if in.Owner != nil {
					if *in.Owner == "unowned" && task.OwnerName != "" || *in.Owner != "unowned" && task.OwnerName != *in.Owner {
						continue
					}
				}
				filtered = append(filtered, task)
			}
			if cursor > len(filtered) {
				cursor = len(filtered)
			}
			end := cursor + limit
			if end > len(filtered) {
				end = len(filtered)
			}
			value := map[string]any{"tasks": filtered[cursor:end]}
			if end < len(filtered) {
				value["nextCursor"] = end
			}
			return jsonToolResult(value)
		},
	}
}

func builtinTeamTaskGetTool(e *Engine) Tool {
	type input struct {
		TaskID string `json:"task_id"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "team_task_get", Description: "Read the complete latest value of one shared task before changing or executing it.",
			Parameters: objectSchema(map[string]any{"task_id": map[string]any{"type": "string", "description": "Shared task id."}}, "task_id"), Output: teamTaskViewSchema(),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			if err := requireTeamCaller(call); err != nil {
				return ToolResult{}, err
			}
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			value, err := e.agentTeams.GetTask(call.SessionID, in.TaskID)
			if err != nil {
				return ToolResult{}, err
			}
			return jsonToolResult(value)
		},
	}
}

func builtinTeamTaskUpdateTool(e *Engine) Tool {
	type input struct {
		TaskID           string    `json:"task_id"`
		ExpectedRevision int       `json:"expected_revision"`
		Action           string    `json:"action"`
		Subject          *string   `json:"subject"`
		Description      *string   `json:"description"`
		BlockedBy        *[]string `json:"blocked_by"`
		WriteScopes      *[]string `json:"write_scopes"`
		Owner            *string   `json:"owner"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "team_task_update", Description: "Compare-and-set a shared task action using the latest revision from team_task_get or team_task_list.",
			Parameters: objectSchema(map[string]any{
				"task_id":           map[string]any{"type": "string", "description": "Shared task id."},
				"expected_revision": map[string]any{"type": "integer", "description": "Current task revision used as the CAS precondition."},
				"action": map[string]any{
					"type": "string", "enum": []string{"claim", "release", "edit", "set_dependencies", "complete", "reopen", "reassign", "delete"},
					"description": "Task transition to apply.",
				},
				"subject":     map[string]any{"type": "string", "description": "Replacement title for edit."},
				"description": map[string]any{"type": "string", "description": "Replacement details for edit."},
				"blocked_by": map[string]any{
					"type": "array", "items": map[string]any{"type": "string"}, "description": "Complete blocker list for set_dependencies.",
				},
				"write_scopes": map[string]any{
					"type": "array", "items": map[string]any{"type": "string"}, "description": "Replacement advisory write scopes for edit.",
				},
				"owner": map[string]any{"type": "string", "description": "Member name for Lead-only reassign; omit to unassign."},
			}, "task_id", "expected_revision", "action"), Output: teamTaskViewSchema(),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			if err := requireTeamCaller(call); err != nil {
				return ToolResult{}, err
			}
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			request := UpdateTeamTaskRequest{
				TaskID: in.TaskID, ExpectedRevision: in.ExpectedRevision, Action: strings.TrimSpace(in.Action),
				Subject: in.Subject, Description: in.Description, Owner: in.Owner,
			}
			if in.BlockedBy != nil {
				request.BlockedBy, request.BlockedBySet = append([]string(nil), (*in.BlockedBy)...), true
			}
			if in.WriteScopes != nil {
				request.WriteScopes, request.WriteScopesSet = append([]string(nil), (*in.WriteScopes)...), true
			}
			value, err := e.agentTeams.UpdateTask(call.SessionID, request)
			if err != nil {
				return ToolResult{}, err
			}
			return jsonToolResult(value)
		},
	}
}

func teamPolicyPrompt(service *TeamService, sessionID string) string {
	membership, err := service.membership(sessionID)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s\n\nYour Team role is %s; your Team name is %s; Team id is %s.", agentTeamPolicy, membership.role, membership.name, membership.id)
}
