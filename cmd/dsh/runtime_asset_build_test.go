package main

import (
	"os"
	"os/exec"
	"path/filepath"
)

func runtimeAssetGoCommand(repository string, args ...string) *exec.Cmd {
	upstream := os.Getenv("DEEPSEEK_HARNESS_UPSTREAM")
	if upstream == "" {
		upstream = filepath.Join(repository, "deepseek-harness")
	}
	commandArgs := []string{"run", "./scripts/with_runtime_assets", "--upstream", upstream, "--"}
	commandArgs = append(commandArgs, args...)
	command := exec.Command("go", commandArgs...)
	command.Dir = repository
	return command
}
