package harness

import (
	"encoding/json"
	"fmt"
	"sort"
)

type tokenSurfaceNode struct {
	seq, tokens int
}

type tokenMeasurementAnchor struct {
	header        map[string]any
	surfaceTokens int
	baseline      int
}

type tokenMeasurement struct {
	totalTokens   int
	surfaceTokens int
}

func measureSessionTokens(s *Session) (tokenMeasurement, error) {
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	var header map[string]any
	var surface []tokenSurfaceNode
	surfaceTokens := 0
	var stepStart *struct{ turn, step, surfaceTokens int }
	var anchor *tokenMeasurementAnchor
	for _, event := range events {
		switch event.Type {
		case "request/header":
			data, _ := event.Data.(map[string]any)
			next, _ := data["header"].(map[string]any)
			header = cloneOptionalMap(next)
		case "step/start":
			if stepStart != nil {
				return tokenMeasurement{}, fmt.Errorf("token meter: step/start at seq %d arrived before turn %d/step %d ended", event.Seq, stepStart.turn, stepStart.step)
			}
			data, _ := event.Data.(map[string]any)
			stepStart = &struct{ turn, step, surfaceTokens int }{eventInt(data["turn"]), eventInt(data["step"]), surfaceTokens}
		case "step/end":
			data, _ := event.Data.(map[string]any)
			if stepStart == nil || stepStart.turn != eventInt(data["turn"]) || stepStart.step != eventInt(data["step"]) {
				return tokenMeasurement{}, fmt.Errorf("token meter: step/end at seq %d has no matching step/start event", event.Seq)
			}
			stepStart = nil
		}
		eventTokens, nextSurface, delta, surfaceErr := foldTokenSurface(surface, event)
		if surfaceErr != nil {
			return tokenMeasurement{}, surfaceErr
		}
		if event.Type == "assistant/message" {
			data, _ := event.Data.(map[string]any)
			turn, step := eventInt(data["turn"]), eventInt(data["step"])
			if stepStart == nil || stepStart.turn != turn || stepStart.step != step {
				return tokenMeasurement{}, fmt.Errorf("token meter: assistant/message at seq %d has no matching step/start event", event.Seq)
			}
			providerAssistantTokens, err := providerAssistantTokens(events, event, eventTokens)
			if err != nil {
				return tokenMeasurement{}, err
			}
			anchorSurface := stepStart.surfaceTokens + providerAssistantTokens
			estimated := estimateRequestHeader(header) + anchorSurface
			baseline := estimated
			if usage, ok := usageBuckets(data["usage"]); ok {
				providerTokens := usage.UncachedInputTokens + usage.CacheReadTokens + usage.CacheWriteTokens + usage.OutputTokens
				if providerTokens >= estimated {
					baseline = providerTokens
				}
			}
			anchor = &tokenMeasurementAnchor{header: cloneOptionalMap(header), surfaceTokens: anchorSurface, baseline: baseline}
		}
		if nextSurface != nil {
			surface = nextSurface
			surfaceTokens += delta
		}
	}
	total := estimateRequestHeader(header) + surfaceTokens
	if anchor != nil && jsonEqual(anchor.header, header) {
		total = anchor.baseline + surfaceTokens - anchor.surfaceTokens
	}
	if total < 0 {
		total = 0
	}
	return tokenMeasurement{totalTokens: total, surfaceTokens: surfaceTokens}, nil
}

func cloneOptionalMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	return cloneStringMap(value)
}

func foldTokenSurface(nodes []tokenSurfaceNode, event Event) (int, []tokenSurfaceNode, int, error) {
	if !isSurfaceEligibleType(event.Type) {
		return 0, nil, 0, nil
	}
	tokens := estimateProjectionEvent(event)
	if isAppendSurfaceEvent(event) {
		next := append(append([]tokenSurfaceNode(nil), nodes...), tokenSurfaceNode{seq: int(event.Seq), tokens: tokens})
		return tokens, next, tokens, nil
	}
	start, end, ok := surfaceReplaceBounds(event.SurfaceOp)
	if !ok {
		return 0, nil, 0, fmt.Errorf("token surface: replace at seq %d has invalid surfaceOp", event.Seq)
	}
	startIndex, endIndex := -1, -1
	for index, node := range nodes {
		if node.seq == start {
			startIndex = index
		}
		if node.seq == end {
			endIndex = index
		}
	}
	if startIndex < 0 || endIndex < startIndex {
		return 0, nil, 0, fmt.Errorf("token surface: replace at seq %d has invalid current range %d-%d", event.Seq, start, end)
	}
	removed := 0
	for _, node := range nodes[startIndex : endIndex+1] {
		removed += node.tokens
	}
	next := append([]tokenSurfaceNode(nil), nodes...)
	next = append(next[:startIndex], append([]tokenSurfaceNode{{seq: int(event.Seq), tokens: tokens}}, next[endIndex+1:]...)...)
	return tokens, next, tokens - removed, nil
}

func estimateRequestHeader(header map[string]any) int {
	if header == nil {
		return 0
	}
	total := 0
	if system, present := header["system"]; present {
		text, _ := system.(string)
		total += projectionDensityPrice(text) + 4
	}
	if tools, present := header["tools"]; present && projectionContentLength(tools) > 0 {
		encoded, _ := json.Marshal(tools)
		total += projectionDensityPrice(string(encoded)) + 4
	}
	return total
}

func providerAssistantTokens(events []Event, assistant Event, durableTokens int) (int, error) {
	if assistant.SourceEventSeqs == nil {
		return durableTokens, nil
	}
	data, _ := assistant.Data.(map[string]any)
	turn, step := eventInt(data["turn"]), eventInt(data["step"])
	seen := map[int]bool{}
	text, reasoning := "", ""
	type toolChunk struct{ name, arguments string }
	tools := map[int]*toolChunk{}
	indices := make([]int, 0)
	for _, seq := range assistant.SourceEventSeqs {
		if seq < 0 || seq >= int(assistant.Seq) || seq >= len(events) {
			return 0, fmt.Errorf("token meter: assistant/message at seq %d source seq %d is not earlier", assistant.Seq, seq)
		}
		if seen[seq] {
			return 0, fmt.Errorf("token meter: assistant/message at seq %d repeats source seq %d", assistant.Seq, seq)
		}
		seen[seq] = true
		event := events[seq]
		if event.Type != "assistant/chunk" {
			return 0, fmt.Errorf("token meter: assistant/message at seq %d source seq %d is not assistant/chunk", assistant.Seq, seq)
		}
		chunkData, _ := event.Data.(map[string]any)
		if eventInt(chunkData["turn"]) != turn || eventInt(chunkData["step"]) != step {
			return 0, fmt.Errorf("token meter: assistant/message at seq %d source seq %d belongs to another step", assistant.Seq, seq)
		}
		chunk, _ := chunkData["chunk"].(map[string]any)
		switch chunk["type"] {
		case "text-delta":
			value, _ := chunk["text"].(string)
			text += value
		case "reasoning-delta":
			value, _ := chunk["text"].(string)
			reasoning += value
		case "tool-call-delta":
			index := eventInt(chunk["index"])
			current := tools[index]
			if current == nil {
				current = &toolChunk{}
				tools[index] = current
				indices = append(indices, index)
			}
			if name, ok := chunk["name"].(string); ok && name != "" {
				current.name = name
			}
			arguments, _ := chunk["argumentsDelta"].(string)
			current.arguments += arguments
		}
	}
	blocks := make([]ContentBlock, 0, 2+len(tools))
	if reasoning != "" {
		blocks = append(blocks, ContentBlock{Type: "reasoning", Text: reasoning})
	}
	if text != "" {
		blocks = append(blocks, ContentBlock{Type: "text", Text: text})
	}
	sort.Ints(indices)
	for _, index := range indices {
		blocks = append(blocks, ContentBlock{Type: "tool-call", Name: tools[index].name, Arguments: tools[index].arguments})
	}
	if len(blocks) == 0 {
		return 0, nil
	}
	return estimateProjectionContent(blocks) + 4, nil
}
