package harness

// forwardedRemoteEvents is the browser-facing event boundary mirrored from
// packages/api/remotes/src/remote-events.ts in the pinned upstream checkout.
// Keeping it here makes new host event producers auditable in one place.
var forwardedRemoteEvents = map[string]struct{}{
	"agent-preset/selected":         {},
	"commands/change":               {},
	"credentials/updated":           {},
	"cordis/request-run":            {},
	"cordis/request-run-resolved":   {},
	"cordis/dynamic-package":        {},
	"cordis/dynamic-retract":        {},
	"cordis/inspect-query":          {},
	"cordis/inspect-query-resolved": {},
	"llm/adapters-updated":          {},
	"settings/document-updated":     {},
}

func (e *Engine) emitRemoteEvent(event string, args ...any) {
	e.emitRemoteEventFrom(nil, event, args...)
}

func (e *Engine) emitRemoteEventFrom(origin *dynamicCordisRun, event string, args ...any) {
	if _, ok := forwardedRemoteEvents[event]; !ok {
		return
	}
	_ = e.emitDynamicCordisEventFrom(origin, event, args...)
	e.emitHost(map[string]any{
		"type":  "host/remote-event",
		"event": event,
		"args":  args,
	})
}
