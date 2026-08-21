package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var dynamicCordisToolNames = []string{
	"cordis_inspect_list",
	"cordis_inspect_query",
	"cordis_inspect_self",
	"cordis_define",
	"cordis_run",
	"cordis_stop",
	"cordis_undefine",
}

func registerDynamicCordisTools(e *Engine) error {
	for _, tool := range []Tool{
		builtinCordisInspectListTool(e),
		builtinCordisInspectQueryTool(e),
		builtinCordisInspectSelfTool(e),
		builtinCordisDefineTool(e),
		builtinCordisRunTool(e),
		builtinCordisStopTool(e),
		builtinCordisUndefineTool(e),
	} {
		if err := e.RegisterTool(tool); err != nil {
			return err
		}
	}
	return nil
}

func cordisToolResult(value any) (ToolResult, error) {
	result, err := jsonToolResult(value)
	if err == nil {
		result.Value = value
	}
	return result, err
}

func builtinCordisInspectListTool(e *Engine) Tool {
	return Tool{
		Schema: ToolSchema{
			Name:        "cordis_inspect_list",
			Description: "List the currently available read-only Cordis Inspect providers and their exact methods and schemas.",
			Parameters:  objectSchema(map[string]any{}),
			Output:      map[string]any{},
		},
		Execute: func(context.Context, ToolCall) (ToolResult, error) {
			return cordisToolResult(map[string]any{"providers": e.ListInspectProviders()})
		},
	}
}

func builtinCordisInspectQueryTool(e *Engine) Tool {
	type input struct {
		Platform string          `json:"platform"`
		Provider string          `json:"provider"`
		Method   string          `json:"method"`
		Input    json.RawMessage `json:"input,omitempty"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "cordis_inspect_query",
			Description: "Run one read-only method selected from cordis_inspect_list. Host queries execute locally; Client queries wait for a browser page to answer.",
			Parameters: objectSchema(map[string]any{
				"platform": map[string]any{"type": "string", "enum": []string{"host", "client"}},
				"provider": map[string]any{"type": "string"},
				"method":   map[string]any{"type": "string"},
				"input":    map[string]any{},
			}, "platform", "provider", "method"),
			Output: map[string]any{},
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			value, err := decodeCordisToolJSON(in.Input)
			if err != nil {
				return ToolResult{}, fmt.Errorf("cordis_inspect_query input: %w", err)
			}
			var data any
			switch in.Platform {
			case "host":
				data, err = e.QueryHostInspectProvider(ctx, call.SessionID, in.Provider, in.Method, value)
			case "client":
				data, err = e.QueryInspectProvider(ctx, call.SessionID, in.Provider, in.Method, value)
			default:
				return ToolResult{}, fmt.Errorf("Cordis inspect platform %q has no registered provider runtime", in.Platform)
			}
			if err != nil {
				return ToolResult{}, err
			}
			return cordisToolResult(map[string]any{
				"platform": in.Platform, "provider": in.Provider, "method": in.Method, "data": data,
			})
		},
	}
}

func decodeCordisToolJSON(raw json.RawMessage) (any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func builtinCordisInspectSelfTool(e *Engine) Tool {
	type input struct {
		PluginID  string `json:"pluginId,omitempty"`
		PackageID string `json:"packageId,omitempty"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "cordis_inspect_self",
			Description: "Inspect dynamic Cordis plugins owned by this session. Source is returned only for an exact pluginId and packageId.",
			Parameters: objectSchema(map[string]any{
				"pluginId":  map[string]any{"type": "string"},
				"packageId": map[string]any{"type": "string"},
			}),
			Output: map[string]any{},
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if in.PackageID != "" && in.PluginID == "" {
				return ToolResult{}, errors.New("cordis_inspect_self packageId requires pluginId")
			}
			value, err := e.DynamicCordisInspectSelf(call.SessionID, in.PluginID, in.PackageID)
			if err != nil {
				return ToolResult{}, err
			}
			return cordisToolResult(value)
		},
	}
}

func builtinCordisDefineTool(e *Engine) Tool {
	type input struct {
		Plugin  DynamicCordisPluginSelector `json:"plugin"`
		Name    string                      `json:"name"`
		Purpose string                      `json:"purpose"`
		Code    DynamicCordisCode           `json:"code"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "cordis_define",
			Description: "Define one immutable plain-JavaScript Host and/or Client package without running it.",
			Parameters: objectSchema(map[string]any{
				"plugin": map[string]any{
					"oneOf": []any{
						objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "new"}, "idPrefix": map[string]any{"type": "string"}}, "kind", "idPrefix"),
						objectSchema(map[string]any{"kind": map[string]any{"type": "string", "const": "existing"}, "pluginId": map[string]any{"type": "string"}}, "kind", "pluginId"),
					},
				},
				"name":    map[string]any{"type": "string"},
				"purpose": map[string]any{"type": "string"},
				"code": objectSchema(map[string]any{
					"host": map[string]any{"type": "string"}, "client": map[string]any{"type": "string"},
				}),
			}, "plugin", "name", "purpose", "code"),
			Output: objectSchema(map[string]any{
				"pluginId": map[string]any{"type": "string"}, "packageId": map[string]any{"type": "string"}, "name": map[string]any{"type": "string"}, "purpose": map[string]any{"type": "string"},
				"hasHostHalf": map[string]any{"type": "boolean"}, "hasClientHalf": map[string]any{"type": "boolean"},
			}, "pluginId", "packageId", "name", "purpose", "hasHostHalf", "hasClientHalf"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
				SessionID: call.SessionID, Plugin: in.Plugin, Name: in.Name, Purpose: in.Purpose, Code: in.Code,
			})
			if err != nil {
				return ToolResult{}, err
			}
			return cordisToolResult(receipt)
		},
	}
}

func builtinCordisRunTool(e *Engine) Tool {
	type input struct {
		PluginID  string `json:"pluginId"`
		PackageID string `json:"packageId"`
		Mode      string `json:"mode"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "cordis_run",
			Description: "Activate one exact dynamic Cordis package with run or update version semantics.",
			Parameters: objectSchema(map[string]any{
				"pluginId": map[string]any{"type": "string"}, "packageId": map[string]any{"type": "string"},
				"mode": map[string]any{"type": "string", "enum": []string{"run", "update"}},
			}, "pluginId", "packageId", "mode"),
			Output: map[string]any{},
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			receipt, err := e.DynamicCordisRun(ctx, call.SessionID, in.PluginID, in.PackageID, in.Mode)
			if err != nil {
				return ToolResult{}, err
			}
			if !receipt.OK {
				return ToolResult{}, errors.New(receipt.Message)
			}
			return cordisToolResult(receipt)
		},
	}
}

func builtinCordisStopTool(e *Engine) Tool {
	type input struct {
		PluginID string `json:"pluginId"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "cordis_stop",
			Description: "Stop a dynamic Cordis plugin while retaining its immutable package definitions.",
			Parameters:  objectSchema(map[string]any{"pluginId": map[string]any{"type": "string"}}, "pluginId"),
			Output:      objectSchema(map[string]any{"pluginId": map[string]any{"type": "string"}}, "pluginId"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			receipt, err := e.DynamicCordisStop(call.SessionID, in.PluginID)
			if err != nil {
				return ToolResult{}, err
			}
			if !receipt.OK && receipt.Reason != "not-running" {
				return ToolResult{}, errors.New(receipt.Message)
			}
			return cordisToolResult(map[string]any{"pluginId": in.PluginID})
		},
	}
}

func builtinCordisUndefineTool(e *Engine) Tool {
	type input struct {
		PluginID string `json:"pluginId"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "cordis_undefine",
			Description: "Permanently remove one dynamic Cordis plugin and all of its packages.",
			Parameters:  objectSchema(map[string]any{"pluginId": map[string]any{"type": "string"}}, "pluginId"),
			Output:      objectSchema(map[string]any{"pluginId": map[string]any{"type": "string"}, "wasRunning": map[string]any{"type": "boolean"}}, "pluginId", "wasRunning"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			receipt, err := e.DynamicCordisUndefine(call.SessionID, in.PluginID)
			if err != nil {
				return ToolResult{}, err
			}
			if !receipt.OK {
				return ToolResult{}, errors.New(receipt.Message)
			}
			return cordisToolResult(map[string]any{"pluginId": in.PluginID, "wasRunning": receipt.WasRunning})
		},
	}
}
