package harness

import "testing"

func TestAssistantStreamCompactsAndExpandsLosslessly(t *testing.T) {
	stream := []any{}
	stream = appendAssistantStreamRecord(stream, 100, map[string]any{"type": "text-delta", "index": 0, "text": "hel"})
	stream = appendAssistantStreamRecord(stream, 103, map[string]any{"type": "text-delta", "index": 0, "text": "lo"})
	stream = appendAssistantStreamRecord(stream, 105, map[string]any{"type": "usage", "usage": map[string]any{"inputTokens": 2, "outputTokens": 1}})
	if len(stream) != 2 {
		t.Fatalf("records = %#v", stream)
	}
	first := stream[0].(map[string]any)
	if first["type"] != "text-chunks" || len(stringSlice(first["texts"])) != 2 || len(intSlice(first["dt"])) != 1 || intSlice(first["dt"])[0] != 3 {
		t.Fatalf("packed text run = %#v", first)
	}
	chunks := assistantStreamChunks(stream)
	if len(chunks) != 3 || chunks[0].time != 100 || chunks[0].chunk["text"] != "hel" || chunks[1].time != 103 || chunks[1].chunk["text"] != "lo" || chunks[2].chunk["type"] != "usage" {
		t.Fatalf("expanded chunks = %#v", chunks)
	}
}

func TestEmbeddedAssistantStreamFeedsUsageStatsAndFallbackOutput(t *testing.T) {
	stream := []any{}
	stream = appendAssistantStreamRecord(stream, 110, map[string]any{"type": "text-delta", "index": 0, "text": "partial"})
	stream = appendAssistantStreamRecord(stream, 120, map[string]any{"type": "usage", "usage": map[string]any{"inputTokens": 4, "outputTokens": 2}})
	events := []Event{
		{Type: "step/start", Seq: 0, Time: 100, Data: map[string]any{"turn": 1, "step": 1}},
		{Type: "assistant/message", Seq: 1, Time: 121, Data: map[string]any{
			"turn": 1, "step": 1, "stream": stream,
			"message": map[string]any{"id": "assistant", "role": "assistant", "content": []any{}},
		}},
		{Type: "step/end", Seq: 2, Time: 130, Data: map[string]any{"turn": 1, "step": 1}},
	}
	usage := currentTokenUsage(events)
	if usage["uncachedInputTokens"] != 4 || usage["outputTokens"] != 2 {
		t.Fatalf("usage = %#v", usage)
	}
	stats := currentSessionStats(events)
	if stats["ttftMs"] != int64(10) || stats["ttftSteps"] != 1 {
		t.Fatalf("stats = %#v", stats)
	}
	output := finalAssistantOutput(events)
	if len(output) != 1 || output[0].Type != "text" || output[0].Text != "partial" {
		t.Fatalf("fallback output = %#v", output)
	}
}
