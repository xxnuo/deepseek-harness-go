package harness

import (
	"context"
	"testing"
	"time"
)

func TestToolTimeoutPolicyFollowsRuntimeConfig(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.RegisterTool(Tool{
		Schema:  ToolSchema{Name: "timeout-policy-probe", Description: "probe", Parameters: map[string]any{"type": "object"}},
		Timeout: 5 * time.Millisecond,
		Execute: func(ctx context.Context, _ ToolCall) (ToolResult, error) {
			select {
			case <-time.After(25 * time.Millisecond):
				return ToolResult{Content: []ContentBlock{{Type: "text", Text: "completed"}}}, nil
			case <-ctx.Done():
				return ToolResult{Content: []ContentBlock{{Type: "text", Text: "aborted"}}}, ctx.Err()
			}
		},
	}); err != nil {
		t.Fatal(err)
	}

	e.mu.RLock()
	tool := e.tools["timeout-policy-probe"]
	e.mu.RUnlock()
	result, err := executeTool(context.Background(), tool, ToolCall{Name: tool.Schema.Name})
	if err != nil || !result.IsError || result.Error == nil || result.Error.Code != "TOOL_TIMEOUT" {
		t.Fatalf("enabled timeout policy result = %#v, err=%v", result, err)
	}

	disabled := false
	cfg := e.Config()
	cfg.ToolTimeoutPolicyEnabled = &disabled
	if err := e.ApplyRuntimeConfig(cfg); err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	tool = e.tools["timeout-policy-probe"]
	e.mu.RUnlock()
	result, err = executeTool(context.Background(), tool, ToolCall{Name: tool.Schema.Name})
	if err != nil || result.IsError || result.Error != nil || len(result.Content) != 1 || result.Content[0].Text != "completed" {
		t.Fatalf("disabled timeout policy result = %#v, err=%v", result, err)
	}
}
