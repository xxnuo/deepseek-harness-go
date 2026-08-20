package harness

import (
	"runtime"
	"testing"
	"time"
)

func TestNewClosesDynamicCordisLoopAfterLateFailure(t *testing.T) {
	baseline := runtime.NumGoroutine()
	cfg := DefaultConfig()
	cfg.Persist = false
	cfg.SubagentTools = []SubagentToolConfig{{Provider: "missing", ToolName: "missing"}}
	for range 50 {
		if engine, err := New(WithConfig(cfg)); err == nil {
			_ = engine.Close()
			t.Fatal("invalid subagent provider was accepted")
		}
	}
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > baseline+10 && time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
	if current := runtime.NumGoroutine(); current > baseline+10 {
		t.Fatalf("late New failures leaked goroutines: baseline=%d current=%d", baseline, current)
	}
}
