package harness

import (
	"context"
	"errors"
	"fmt"
)

type interactionResult struct {
	ok    bool
	value any
	err   *RPCError
}

type pendingInteraction struct {
	id        string
	sessionID string
	method    string
	payload   any
	done      chan interactionResult
}

// RequestInteraction publishes an answerable server-request on the mux
// stream and waits for POST /api/respond (or ResolveInteraction from an
// embedding application). The request id is stable across stream reconnects.
func (e *Engine) RequestInteraction(ctx context.Context, sessionID, method string, payload any) (any, error) {
	if sessionID == "" || method == "" {
		return nil, errors.New("interaction requires sessionId and method")
	}
	if _, err := e.getSession(sessionID); err != nil {
		return nil, err
	}
	pending := &pendingInteraction{id: newID("rpc"), sessionID: sessionID, method: method, payload: payload, done: make(chan interactionResult, 1)}
	e.pendingMu.Lock()
	if e.pending == nil {
		e.pending = map[string]*pendingInteraction{}
	}
	e.pending[pending.id] = pending
	e.pendingMu.Unlock()
	e.emitMux(map[string]any{"type": "host/pending-request", "sessionId": sessionID, "rpcId": pending.id, "method": method, "payload": payload})
	select {
	case result := <-pending.done:
		if !result.ok {
			if result.err != nil {
				return nil, fmt.Errorf("%s: %s", result.err.Code, result.err.Message)
			}
			return nil, errors.New("interaction rejected")
		}
		return result.value, nil
	case <-ctx.Done():
		e.pendingMu.Lock()
		if current := e.pending[pending.id]; current == pending {
			delete(e.pending, pending.id)
		}
		e.pendingMu.Unlock()
		return nil, ctx.Err()
	}
}

// ResolveInteraction is the library equivalent of /api/respond.
func (e *Engine) ResolveInteraction(rpcID string, result map[string]any) bool {
	e.pendingMu.Lock()
	pending := e.pending[rpcID]
	if pending != nil {
		delete(e.pending, rpcID)
	}
	e.pendingMu.Unlock()
	if pending == nil {
		return false
	}
	ok, _ := result["ok"].(bool)
	if ok {
		pending.done <- interactionResult{ok: true, value: result["value"]}
	} else {
		errValue, _ := result["error"].(map[string]any)
		err := &RPCError{Code: "cancelled", Message: "interaction cancelled", Details: map[string]any{}}
		if errValue != nil {
			err.Code, _ = errValue["code"].(string)
			err.Message, _ = errValue["message"].(string)
			err.Details = errValue["details"]
		}
		pending.done <- interactionResult{err: err}
	}
	return true
}

func (e *Engine) pendingInteractions() []*pendingInteraction {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	items := make([]*pendingInteraction, 0, len(e.pending))
	for _, item := range e.pending {
		copy := *item
		items = append(items, &copy)
	}
	return items
}
