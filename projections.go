package harness

import (
	"encoding/json"
	"strings"
)

type tokenUsageProjection struct {
	UncachedInputTokens int
	OutputTokens        int
	CacheReadTokens     int
	CacheWriteTokens    int
}

func (u tokenUsageProjection) value() map[string]any {
	return map[string]any{
		"uncachedInputTokens": u.UncachedInputTokens,
		"outputTokens":        u.OutputTokens,
		"cacheReadTokens":     u.CacheReadTokens,
		"cacheWriteTokens":    u.CacheWriteTokens,
	}
}

type usageSample struct {
	turn, step int
	buckets    tokenUsageProjection
}

func projectionNonnegativeInt(value any) (int, bool) {
	if number, ok := eventSeqNumber(value); ok && number >= 0 {
		return number, true
	}
	if number, ok := value.(json.Number); ok {
		integer, err := number.Int64()
		if err == nil && integer >= 0 && int64(int(integer)) == integer {
			return int(integer), true
		}
	}
	return 0, false
}

func usageBuckets(value any) (tokenUsageProjection, bool) {
	usage, ok := value.(map[string]any)
	if !ok {
		return tokenUsageProjection{}, false
	}
	input, inputOK := projectionNonnegativeInt(usage["inputTokens"])
	output, outputOK := projectionNonnegativeInt(usage["outputTokens"])
	cacheRead, cacheReadOK := projectionNonnegativeInt(usage["cacheReadTokens"])
	cacheWrite, cacheWriteOK := projectionNonnegativeInt(usage["cacheWriteTokens"])
	if !cacheReadOK {
		cacheRead, cacheReadOK = projectionNonnegativeInt(usage["prompt_cache_hit_tokens"])
	}
	if !cacheReadOK {
		if details, ok := usage["prompt_tokens_details"].(map[string]any); ok {
			cacheRead, cacheReadOK = projectionNonnegativeInt(details["cached_tokens"])
		}
	}
	if !cacheWriteOK {
		cacheWrite, cacheWriteOK = projectionNonnegativeInt(usage["cache_creation_input_tokens"])
	}
	if !inputOK || !outputOK {
		prompt, promptOK := projectionNonnegativeInt(usage["prompt_tokens"])
		output, outputOK = projectionNonnegativeInt(usage["completion_tokens"])
		if !promptOK || !outputOK || cacheRead > prompt {
			return tokenUsageProjection{}, false
		}
		input, inputOK = prompt-cacheRead, true
	}
	if !cacheReadOK {
		cacheRead = 0
	}
	if !cacheWriteOK {
		cacheWrite = 0
	}
	return tokenUsageProjection{input, output, cacheRead, cacheWrite}, inputOK && outputOK
}

func eventUsageSample(event Event) (usageSample, bool) {
	data, ok := event.Data.(map[string]any)
	if !ok {
		return usageSample{}, false
	}
	turn, turnOK := eventSeqNumber(data["turn"])
	step, stepOK := eventSeqNumber(data["step"])
	if !turnOK || !stepOK {
		return usageSample{}, false
	}
	value := data["usage"]
	if event.Type == "assistant/chunk" {
		chunk, ok := data["chunk"].(map[string]any)
		if !ok || chunk["type"] != "usage" {
			return usageSample{}, false
		}
		value = chunk["usage"]
	} else if event.Type != "assistant/message" {
		return usageSample{}, false
	}
	buckets, ok := usageBuckets(value)
	return usageSample{turn: turn, step: step, buckets: buckets}, ok
}

func currentTokenUsage(events []Event) map[string]any {
	var totals tokenUsageProjection
	var last *usageSample
	for _, event := range events {
		sample, ok := eventUsageSample(event)
		if !ok {
			continue
		}
		var previous tokenUsageProjection
		if last != nil && last.turn == sample.turn && last.step == sample.step {
			previous = last.buckets
		}
		totals.UncachedInputTokens += sample.buckets.UncachedInputTokens - previous.UncachedInputTokens
		totals.OutputTokens += sample.buckets.OutputTokens - previous.OutputTokens
		totals.CacheReadTokens += sample.buckets.CacheReadTokens - previous.CacheReadTokens
		totals.CacheWriteTokens += sample.buckets.CacheWriteTokens - previous.CacheWriteTokens
		copy := sample
		last = &copy
	}
	return totals.value()
}

type permissionPreset struct {
	sandbox, approval, description string
}

var permissionPresets = []struct {
	name string
	spec permissionPreset
}{
	{"read-only", permissionPreset{"read-only", "ask", "Read files without allowing file modifications; wider retries require approval."}},
	{"workspace-write", permissionPreset{"workspace-write", "ask", "Write inside the workspace and permitted temporary directories; wider retries require approval."}},
	{"danger-full-access", permissionPreset{"danger-full-access", "never", "Full file access without approval prompts."}},
}

func currentPermissions(events []Event) map[string]any {
	preset, sandbox, approval := "", "workspace-write", "ask"
	for _, event := range events {
		data, _ := event.Data.(map[string]any)
		switch event.Type {
		case "permission/preset":
			if value, ok := data["preset"].(string); ok {
				preset = value
			}
		case "sandbox/mode":
			if value, ok := data["mode"].(string); ok {
				sandbox = value
			}
		case "approval/policy":
			if value, ok := data["policy"].(string); ok {
				approval = value
			}
		}
	}
	matches := func(spec permissionPreset) bool { return spec.sandbox == sandbox && spec.approval == approval }
	current := "custom"
	if preset != "" {
		for _, candidate := range permissionPresets {
			if candidate.name == preset && matches(candidate.spec) {
				current = preset
				break
			}
		}
	}
	if current == "custom" {
		for _, candidate := range permissionPresets {
			if matches(candidate.spec) {
				current = candidate.name
				break
			}
		}
	}
	options := make([]map[string]any, 0, len(permissionPresets)+1)
	for _, candidate := range permissionPresets {
		options = append(options, map[string]any{"value": candidate.name, "name": candidate.name, "description": candidate.spec.description})
	}
	if current == "custom" {
		options = append(options, map[string]any{
			"value": "custom", "name": "Custom",
			"description": "Current sandbox and approval settings do not match a preset.",
		})
	}
	return map[string]any{"options": options, "currentValue": current}
}

func currentPlan(events []Event) map[string]any {
	active := false
	var wanted *bool
	for _, event := range events {
		data, _ := event.Data.(map[string]any)
		switch event.Type {
		case "command/run":
			if data["name"] != "plan" {
				continue
			}
			args, ok := data["args"].(string)
			if !ok {
				continue
			}
			value := strings.TrimSpace(args) != "off"
			wanted = &value
		case "plan/mode":
			if value, ok := data["active"].(bool); ok {
				active = value
				wanted = nil
			}
		}
	}
	pending := wanted != nil && *wanted != active
	return map[string]any{"active": active, "pending": pending}
}

func projectionTextLength(value string) int {
	length := 0
	for _, r := range value {
		length++
		if r > 0xffff {
			length++
		}
	}
	return length
}

func projectionDensityPrice(value string) int { return (projectionTextLength(value) + 3) / 4 }

func estimateProjectionBlock(value any) int {
	const blockOverhead = 4
	var typ, text, name, arguments string
	var content any
	switch block := value.(type) {
	case ContentBlock:
		typ, text, name, arguments, content = block.Type, block.Text, block.Name, block.Arguments, block.Content
	case map[string]any:
		typ, _ = block["type"].(string)
		text, _ = block["text"].(string)
		name, _ = block["name"].(string)
		arguments, _ = block["arguments"].(string)
		content = block["content"]
	default:
		encoded, _ := json.Marshal(value)
		return projectionDensityPrice(string(encoded)) + blockOverhead
	}
	switch typ {
	case "text", "reasoning":
		return projectionDensityPrice(text) + blockOverhead
	case "tool-call":
		return projectionDensityPrice(name) + projectionDensityPrice(arguments) + blockOverhead
	case "tool-result":
		return estimateProjectionContent(content) + blockOverhead
	default:
		encoded, _ := json.Marshal(value)
		return projectionDensityPrice(string(encoded)) + blockOverhead
	}
}

func estimateProjectionContent(value any) int {
	tokens := 0
	switch blocks := value.(type) {
	case []ContentBlock:
		for _, block := range blocks {
			tokens += estimateProjectionBlock(block)
		}
	case []any:
		for _, block := range blocks {
			tokens += estimateProjectionBlock(block)
		}
	}
	return tokens
}

func projectionContentLength(value any) int {
	switch blocks := value.(type) {
	case []ContentBlock:
		return len(blocks)
	case []any:
		return len(blocks)
	default:
		return 0
	}
}

func projectionEventContent(event Event) (any, bool) {
	data, ok := event.Data.(map[string]any)
	if !ok {
		return nil, false
	}
	switch event.Type {
	case "user/message":
		content := data["content"]
		return content, true
	case "assistant/message", "tool/result":
		message, ok := data["message"].(map[string]any)
		if !ok || event.Type == "assistant/message" && projectionContentLength(message["content"]) == 0 {
			return nil, false
		}
		return message["content"], true
	default:
		return nil, false
	}
}

func estimateProjectionEvent(event Event) int {
	content, ok := projectionEventContent(event)
	if !ok {
		return 0
	}
	return estimateProjectionContent(content) + 4
}

type projectionSurfaceClaim struct {
	start, end, tokens int
}

func projectionShadowClaim(event Event) *projectionSurfaceClaim {
	if event.Type != "compaction/summary" && event.Type != "compaction/prune" {
		return nil
	}
	data, _ := event.Data.(map[string]any)
	rangeValue, _ := data["shadowedRange"].(map[string]any)
	start, startOK := eventSeqNumber(rangeValue["start"])
	end, endOK := eventSeqNumber(rangeValue["end"])
	tokens, tokensOK := projectionNonnegativeInt(data["shadowedTokenCount"])
	if !startOK || !endOK || !tokensOK {
		return nil
	}
	return &projectionSurfaceClaim{start: start, end: end, tokens: tokens}
}

func projectionSurfaceStep(event Event, claim *projectionSurfaceClaim) (int, *projectionSurfaceClaim) {
	if event.Type == "compaction/summary" || event.Type == "compaction/prune" {
		return 0, projectionShadowClaim(event)
	}
	if !isSurfaceEligibleType(event.Type) {
		return 0, nil
	}
	tokens := estimateProjectionEvent(event)
	if isAppendSurfaceEvent(event) {
		return tokens, nil
	}
	start, end, replacement := surfaceReplaceBounds(event.SurfaceOp)
	if !replacement || claim == nil || claim.start != start || claim.end != end {
		return 0, nil
	}
	return tokens - claim.tokens, nil
}

func currentContextPressure(events []Event) map[string]any {
	var contextWindow, pressureTokens, sampledSurfaceTokens *int
	surfaceTokens := 0
	var claim *projectionSurfaceClaim
	for _, event := range events {
		if event.Type == "request/context" {
			data, _ := event.Data.(map[string]any)
			if value, ok := projectionNonnegativeInt(data["contextWindow"]); ok && value > 0 {
				copy := value
				contextWindow = &copy
			} else {
				contextWindow = nil
			}
		}
		if sample, ok := eventUsageSample(event); ok {
			value := sample.buckets.UncachedInputTokens + sample.buckets.CacheReadTokens + sample.buckets.CacheWriteTokens
			pressure, sampled := value, surfaceTokens
			pressureTokens, sampledSurfaceTokens = &pressure, &sampled
		}
		delta, nextClaim := projectionSurfaceStep(event, claim)
		surfaceTokens += delta
		if surfaceTokens < 0 {
			surfaceTokens = 0
		}
		claim = nextClaim
	}
	value := map[string]any{}
	if contextWindow != nil {
		value["contextWindow"] = *contextWindow
	}
	if pressureTokens != nil {
		value["pressureTokens"] = *pressureTokens
		if sampledSurfaceTokens != nil {
			projected := *pressureTokens + surfaceTokens - *sampledSurfaceTokens
			if projected < 0 {
				projected = 0
			}
			value["projectedTokens"] = projected
		}
	}
	return value
}

func currentContextBreakdown(events []Event) map[string]any {
	systemTokens, toolsTokens, messageTokens := 0, 0, 0
	var claim *projectionSurfaceClaim
	for _, event := range events {
		if event.Type == "request/header" {
			data, _ := event.Data.(map[string]any)
			header, _ := data["header"].(map[string]any)
			system, _ := header["system"].(string)
			if _, present := header["system"]; present {
				systemTokens = projectionDensityPrice(system) + 4
			} else {
				systemTokens = 0
			}
			toolsTokens = 0
			if projectionContentLength(header["tools"]) > 0 {
				encoded, _ := json.Marshal(header["tools"])
				toolsTokens = projectionDensityPrice(string(encoded)) + 4
			} else if tools, ok := header["tools"].([]ToolSchema); ok && len(tools) > 0 {
				encoded, _ := json.Marshal(tools)
				toolsTokens = projectionDensityPrice(string(encoded)) + 4
			}
		}
		delta, nextClaim := projectionSurfaceStep(event, claim)
		messageTokens += delta
		if messageTokens < 0 {
			messageTokens = 0
		}
		claim = nextClaim
	}
	return map[string]any{"systemTokens": systemTokens, "toolsTokens": toolsTokens, "messageTokens": messageTokens}
}

func projectionChunkHasToken(event Event) bool {
	if event.Type != "assistant/chunk" {
		return false
	}
	data, _ := event.Data.(map[string]any)
	chunk, _ := data["chunk"].(map[string]any)
	switch chunk["type"] {
	case "text-delta", "reasoning-delta":
		text, _ := chunk["text"].(string)
		return text != ""
	case "tool-call-delta":
		arguments, _ := chunk["argumentsDelta"].(string)
		_, hasName := chunk["name"]
		return arguments != "" || hasName
	default:
		return false
	}
}

func projectionCallID(event Event) string {
	data, _ := event.Data.(map[string]any)
	if event.Type == "tool/call" {
		value, _ := data["callId"].(string)
		return value
	}
	message, _ := data["message"].(map[string]any)
	source, _ := message["source"].(map[string]any)
	value, _ := source["callId"].(string)
	return value
}

func currentSessionStats(events []Event) map[string]any {
	type openStep struct {
		turn, step int
		start      int64
		firstToken *int64
	}
	turns, steps, ttftSteps, decodeTokens := 0, 0, 0, 0
	var llmMS, toolMS, ttftMS, decodeMS int64
	lastTurn := -1
	var open *openStep
	pendingCalls := map[string]int64{}
	for _, event := range events {
		data, _ := event.Data.(map[string]any)
		switch event.Type {
		case "step/start":
			turn, turnOK := eventSeqNumber(data["turn"])
			step, stepOK := eventSeqNumber(data["step"])
			if turnOK && stepOK {
				open = &openStep{turn: turn, step: step, start: event.Time}
			}
		case "assistant/chunk":
			if open == nil || open.firstToken != nil || !projectionChunkHasToken(event) {
				continue
			}
			turn, turnOK := eventSeqNumber(data["turn"])
			step, stepOK := eventSeqNumber(data["step"])
			if turnOK && stepOK && turn == open.turn && step == open.step {
				time := event.Time
				open.firstToken = &time
			}
		case "assistant/message":
			if open == nil {
				continue
			}
			turn, turnOK := eventSeqNumber(data["turn"])
			step, stepOK := eventSeqNumber(data["step"])
			if !turnOK || !stepOK || turn != open.turn || step != open.step {
				continue
			}
			if elapsed := event.Time - open.start; elapsed > 0 {
				llmMS += elapsed
			}
			if open.firstToken != nil {
				if elapsed := *open.firstToken - open.start; elapsed > 0 {
					ttftMS += elapsed
				}
				ttftSteps++
				if usage, ok := usageBuckets(data["usage"]); ok {
					if elapsed := event.Time - *open.firstToken; elapsed > 0 {
						decodeMS += elapsed
					}
					decodeTokens += usage.OutputTokens
				}
			}
			open = nil
		case "tool/call":
			if callID := projectionCallID(event); callID != "" {
				pendingCalls[callID] = event.Time
			}
		case "tool/result":
			callID := projectionCallID(event)
			start, ok := pendingCalls[callID]
			if !ok {
				continue
			}
			delete(pendingCalls, callID)
			if elapsed := event.Time - start; elapsed > 0 {
				toolMS += elapsed
			}
		case "step/end":
			turn, ok := eventSeqNumber(data["turn"])
			if !ok {
				continue
			}
			if turn != lastTurn {
				turns++
				lastTurn = turn
			}
			steps++
			open = nil
		case "turn/end":
			clear(pendingCalls)
		}
	}
	return map[string]any{
		"turns": turns, "steps": steps, "llmMs": llmMS, "toolMs": toolMS,
		"ttftMs": ttftMS, "ttftSteps": ttftSteps, "decodeMs": decodeMS, "decodeTokens": decodeTokens,
	}
}

func imageLimitsProjection() map[string]any {
	return map[string]any{
		"maxImageBytes": maxImageBytes, "maxImagesPerMessage": maxImagesPerMessage,
		"maxMessageImageBytes": maxMessageImageBytes, "maxImagePixels": maxImagePixels,
		"mediaTypes": []string{"image/png", "image/jpeg", "image/webp", "image/gif"},
	}
}

func todosProjectionChange(event Event) (any, bool) {
	switch event.Type {
	case "turn/start":
		return nil, true
	case "todo/write":
		data, _ := event.Data.(map[string]any)
		items, ok := decodeTodoItems(data["todos"])
		if !ok {
			return nil, false
		}
		return items, true
	default:
		return nil, false
	}
}

func titleProjectionChange(event Event) (any, bool) {
	if event.Type != "session/title" {
		return nil, false
	}
	data, _ := event.Data.(map[string]any)
	title, ok := data["title"].(string)
	if !ok || title == "" {
		return nil, false
	}
	return title, true
}

func goalProjectionChange(event Event) (any, bool) {
	if event.Type != "goal/change" {
		return nil, false
	}
	change, ok := event.Data.(map[string]any)
	version, validVersion := eventSeqNumber(change["version"])
	if !ok || change["kind"] != "goal/change" || !validVersion || version != 1 {
		return nil, false
	}
	if change["operation"] == "clear" {
		return nil, true
	}
	goal, ok := change["goal"].(map[string]any)
	if !ok {
		return nil, false
	}
	return map[string]any{
		"goal": goal, "roundsStarted": change["roundsStarted"],
		"createdAt": change["createdAt"], "updatedAt": change["updatedAt"],
	}, true
}

func currentGoalProjection(events []Event) any {
	var current any
	for _, event := range events {
		if value, ok := goalProjectionChange(event); ok {
			current = value
		}
		if event.Type != "user/message" || current == nil {
			continue
		}
		data, _ := event.Data.(map[string]any)
		source, _ := data["source"].(map[string]any)
		goalID, revision, round, ok := goalSource(source)
		if !ok {
			continue
		}
		projection, _ := current.(map[string]any)
		goal, _ := projection["goal"].(map[string]any)
		currentRounds, currentOK := eventSeqNumber(projection["roundsStarted"])
		goalRevision, revisionOK := eventSeqNumber(goal["revision"])
		if currentOK && revisionOK && goal["id"] == goalID && goalRevision == revision && round == currentRounds+1 {
			projection["roundsStarted"] = round
		}
	}
	return current
}

func currentTodos(events []Event) any {
	var current any
	for _, event := range events {
		if value, ok := todosProjectionChange(event); ok {
			current = value
		}
	}
	return current
}

func decodeTodoItems(value any) ([]TodoItem, bool) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	var items []TodoItem
	if err := json.Unmarshal(data, &items); err != nil || items == nil && string(data) != "[]" {
		return nil, false
	}
	return items, true
}

func sessionProjectionValues(events []Event, title string) map[string]any {
	if title == "" {
		for index := len(events) - 1; index >= 0; index-- {
			if events[index].Type != "session/title" {
				continue
			}
			data, _ := events[index].Data.(map[string]any)
			if value, ok := data["title"].(string); ok && value != "" {
				title = value
			}
			break
		}
	}
	var titleValue any
	if title != "" {
		titleValue = title
	}
	return map[string]any{
		"title":            titleValue,
		"todos":            currentTodos(events),
		"permissions":      currentPermissions(events),
		"plan":             currentPlan(events),
		"goal":             currentGoalProjection(events),
		"tokenUsage":       currentTokenUsage(events),
		"contextPressure":  currentContextPressure(events),
		"contextBreakdown": currentContextBreakdown(events),
		"sessionStats":     currentSessionStats(events),
		"imageLimits":      imageLimitsProjection(),
	}
}

// projectionKeysChanged mirrors the upstream state-driven projection feed.
// Baselines use sessionProjectionValues; frames only carry units whose fold
// state observes the committed event.
func projectionKeysChanged(event Event) []string {
	changed := map[string]bool{}
	if _, ok := titleProjectionChange(event); ok {
		changed["title"] = true
	}
	if _, ok := todosProjectionChange(event); ok {
		changed["todos"] = true
	}
	if _, ok := goalProjectionChange(event); ok {
		changed["goal"] = true
	}
	if event.Type == "permission/preset" || event.Type == "sandbox/mode" || event.Type == "approval/policy" {
		changed["permissions"] = true
	}
	if event.Type == "plan/mode" {
		changed["plan"] = true
	}
	if event.Type == "command/run" {
		data, _ := event.Data.(map[string]any)
		if data["name"] == "plan" {
			if _, ok := data["args"].(string); ok {
				changed["plan"] = true
			}
		}
	}
	if _, ok := eventUsageSample(event); ok {
		changed["tokenUsage"] = true
		changed["contextPressure"] = true
	}
	if event.Type == "request/context" {
		changed["contextPressure"] = true
	}
	if event.Type == "request/header" || event.Type == "compaction/summary" || event.Type == "compaction/prune" {
		changed["contextBreakdown"] = true
		changed["contextPressure"] = true
	}
	if isSurfaceEligibleType(event.Type) {
		changed["contextBreakdown"] = true
		changed["contextPressure"] = true
	}
	if event.Type == "step/start" || event.Type == "tool/call" || event.Type == "tool/result" || event.Type == "step/end" || event.Type == "turn/end" || event.Type == "assistant/message" || projectionChunkHasToken(event) {
		changed["sessionStats"] = true
	}
	keys := []string{"title", "todos", "permissions", "plan", "goal", "tokenUsage", "contextPressure", "contextBreakdown", "sessionStats"}
	result := make([]string, 0, len(changed))
	for _, key := range keys {
		if changed[key] {
			result = append(result, key)
		}
	}
	return result
}
