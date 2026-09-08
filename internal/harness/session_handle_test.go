package harness

import "context"

type testSessionHandleDefaults struct{}

func (testSessionHandleDefaults) ID() string { return "test-session" }

func (testSessionHandleDefaults) Header() SessionHeader { return SessionHeader{ID: "test-session"} }

func (testSessionHandleDefaults) InheritedEventCount() SessionLogOffset { return 0 }

func (testSessionHandleDefaults) Access() SessionAccess { return SessionAccessWrite }

func (testSessionHandleDefaults) Read(context.Context, ...SessionLogOffset) ([]Event, error) {
	return []Event{}, nil
}

func (testSessionHandleDefaults) Flush(context.Context) error { return nil }

func (testSessionHandleDefaults) Close() error { return nil }
