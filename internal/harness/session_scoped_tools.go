package harness

import (
	"fmt"
	"sort"
)

func (e *Engine) registerSessionScopedTool(sessionID string, tool Tool) error {
	if sessionID == "" {
		return fmt.Errorf("session-scoped tool requires a session")
	}
	normalized, err := normalizeRegisteredTool(tool)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	tools := e.scopedTools[sessionID]
	if tools == nil {
		tools = map[string]Tool{}
		e.scopedTools[sessionID] = tools
	}
	if _, exists := tools[normalized.Schema.Name]; exists {
		return fmt.Errorf("tool already registered in session %s: %s", sessionID, normalized.Schema.Name)
	}
	tools[normalized.Schema.Name] = normalized
	return nil
}

func (e *Engine) releaseSessionScopedTools(sessionID string) {
	e.mu.Lock()
	delete(e.scopedTools, sessionID)
	delete(e.structuredOutputs, sessionID)
	e.mu.Unlock()
}

func (e *Engine) toolForSession(s *Session, name string) (Tool, bool) {
	s.mu.Lock()
	sessionID := s.Header.ID
	s.mu.Unlock()
	e.mu.RLock()
	if tool, ok := e.scopedTools[sessionID][name]; ok {
		e.mu.RUnlock()
		return tool, true
	}
	tool, ok := e.tools[name]
	e.mu.RUnlock()
	if !ok {
		if runtimeConfig, err := e.runtimeForSession(s); err == nil && name == runtimeConfig.workflowToolName && name != "workflow" {
			e.mu.RLock()
			tool, ok = e.tools["workflow"]
			e.mu.RUnlock()
			if ok {
				tool.Schema.Name = name
			}
		}
	}
	if ok {
		if runtimeConfig, err := e.runtimeForSession(s); err == nil {
			if runtimeConfig.webTools != nil && (name == "web_search" || name == "web_fetch") {
				return sessionWebTool(e, name, runtimeConfig.webTools)
			}
			if (name == "glob" || name == "grep") && runtimeConfig.searchTimeout > 0 {
				tool.Timeout = runtimeConfig.searchTimeout
			}
		}
	}
	return tool, ok
}

func cloneToolSchema(schema ToolSchema) ToolSchema {
	if parameters, ok := cloneJSON(schema.Parameters).(map[string]any); ok {
		schema.Parameters = parameters
	}
	if output, ok := cloneJSON(schema.Output).(map[string]any); ok {
		schema.Output = output
	}
	return schema
}

func sortedToolSchemas(tools map[string]Tool) []ToolSchema {
	rows := make([]ToolSchema, 0, len(tools))
	for _, tool := range tools {
		rows = append(rows, cloneToolSchema(tool.Schema))
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows
}
