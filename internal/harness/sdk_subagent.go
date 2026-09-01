package harness

import (
	"context"
	"sync"
)

// sdkSubagentEnd is emitted only by the concrete provider/model-subagent
// terminal paths. Keeping this channel separate from dynamic Cordis prevents
// arbitrary plugin events from being mistaken for provider lifecycle facts.
type sdkSubagentEnd struct {
	ParentSessionID string
	Payload         map[string]any
}

// subscribeSDKSubagentEnd registers a short-lived SDK lifecycle observer.
// The caller's context owns the registration; disposal is idempotent.
func (e *Engine) subscribeSDKSubagentEnd(ctx context.Context, listener func(sdkSubagentEnd)) func() {
	if listener == nil {
		return func() {}
	}
	e.sdkSubagentMu.Lock()
	e.nextSDKSubagentToken++
	token := e.nextSDKSubagentToken
	if e.sdkSubagentListeners == nil {
		e.sdkSubagentListeners = map[uint64]func(sdkSubagentEnd){}
	}
	e.sdkSubagentListeners[token] = listener
	e.sdkSubagentMu.Unlock()

	var disposeOnce sync.Once
	dispose := func() {
		disposeOnce.Do(func() {
			e.sdkSubagentMu.Lock()
			delete(e.sdkSubagentListeners, token)
			e.sdkSubagentMu.Unlock()
		})
	}
	if ctx != nil {
		go func() {
			<-ctx.Done()
			dispose()
		}()
	}
	return dispose
}

func (e *Engine) notifySDKSubagentEnd(parentID string, payload map[string]any) {
	if payload == nil {
		return
	}
	e.sdkSubagentMu.Lock()
	listeners := make([]func(sdkSubagentEnd), 0, len(e.sdkSubagentListeners))
	for _, listener := range e.sdkSubagentListeners {
		listeners = append(listeners, listener)
	}
	e.sdkSubagentMu.Unlock()
	if len(listeners) == 0 {
		return
	}
	value, ok := cloneJSON(payload).(map[string]any)
	if !ok {
		return
	}
	event := sdkSubagentEnd{ParentSessionID: parentID, Payload: value}
	for _, listener := range listeners {
		listener(event)
	}
}
