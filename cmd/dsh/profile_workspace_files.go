package main

import (
	"fmt"

	harness "github.com/xxnuo/deepseek-harness-go"
	"gopkg.in/yaml.v3"
)

func (composition *composition) resolveWorkspaceFilesConfig() error {
	composition.workspaceFiles = nil
	return composition.walkActiveEntries(func(id, name string, entry *yaml.Node) error {
		if name != "@deepseek-ai/dsh-api-workspace-files" {
			return nil
		}
		if composition.workspaceFiles != nil {
			return fmt.Errorf("workspace-files(%s): duplicate configuration", id)
		}
		config := harness.WorkspaceFilesConfig{MaxBytes: 2 * 1024 * 1024, MaxLines: 5000, MaxEntries: 2000}
		if err := decodeProfileEntryConfig(entry, &config); err != nil {
			return fmt.Errorf("workspace-files(%s): %w", id, err)
		}
		if config.MaxBytes < 1 || config.MaxLines < 1 || config.MaxEntries < 1 {
			return fmt.Errorf("workspace-files(%s): limits must be positive integers", id)
		}
		composition.workspaceFiles = &config
		return nil
	})
}
