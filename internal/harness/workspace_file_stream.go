package harness

import (
	"context"
	"encoding/json"
	"path/filepath"
)

type workspaceFileFollower struct {
	queue []map[string]any
	wake  chan struct{}
	done  chan struct{}
}

func (e *Engine) streamWorkspaceFiles(ctx context.Context, payload json.RawMessage, send func(any) error) *RPCError {
	const endpoint = "workspaceFiles/changes"
	args, rpcErr := remoteArgs(endpoint, payload, remoteDescriptors[endpoint])
	if rpcErr != nil {
		return rpcErr
	}
	workspace, rpcErr := e.workspaceFileRoot(endpoint, args)
	if rpcErr != nil {
		return rpcErr
	}
	state := e.fsState
	follower := &workspaceFileFollower{wake: make(chan struct{}, 1), done: make(chan struct{})}
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return nil
	}
	state.followers[follower] = struct{}{}
	state.mu.Unlock()
	defer func() {
		state.mu.Lock()
		delete(state.followers, follower)
		state.mu.Unlock()
	}()
	root, err := filepath.EvalSymlinks(workspace)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return rpcError("gateway/internal", err.Error(), map[string]any{})
	}
	select {
	case <-follower.done:
		return nil
	default:
	}
	if err := send(map[string]any{"kind": "ready"}); err != nil {
		return nil
	}
	for ctx.Err() == nil {
		state.mu.Lock()
		batch := follower.queue
		follower.queue = nil
		state.mu.Unlock()
		for _, change := range batch {
			select {
			case <-follower.done:
				return nil
			default:
			}
			if ctx.Err() != nil {
				return nil
			}
			if _, contained := workspaceRelativePath(root, change["absolutePath"].(string)); !contained {
				continue
			}
			if err := send(map[string]any{"kind": "change", "change": change}); err != nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
		case <-follower.done:
			return nil
		case <-follower.wake:
		}
	}
	return nil
}

func (s *fsObservationState) closeFollowers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for follower := range s.followers {
		close(follower.done)
	}
}
