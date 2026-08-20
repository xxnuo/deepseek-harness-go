package harness

import (
	"errors"
	"os/exec"
)

type shellChildState interface {
	started(*exec.Cmd) error
	close() error
}

func startShellChild(cmd *exec.Cmd, state shellChildState) error {
	if err := cmd.Start(); err != nil {
		if state != nil {
			_ = state.close()
		}
		return err
	}
	if state == nil {
		return nil
	}
	if err := state.started(cmd); err != nil {
		_ = killChildProcess(cmd)
		_ = cmd.Wait()
		return errors.Join(err, state.close())
	}
	return nil
}

func waitShellChild(cmd *exec.Cmd, state shellChildState) error {
	err := cmd.Wait()
	if state == nil {
		return err
	}
	cleanupErr := state.close()
	if err == nil {
		return cleanupErr
	}
	if cleanupErr == nil {
		return err
	}
	return errors.Join(err, cleanupErr)
}
