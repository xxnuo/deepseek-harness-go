package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
)

const maxJSONSafeInteger int64 = 1<<53 - 1

var knownSessionEventTypes = map[string]struct{}{
	"agent-preset/selected":                  {},
	"agent/inbox/spliced":                    {},
	"approval/asked":                         {},
	"approval/decided":                       {},
	"approval/policy":                        {},
	"assistant/attempt":                      {},
	"assistant/chunk":                        {},
	"assistant/message":                      {},
	"command/done":                           {},
	"command/run":                            {},
	"compaction/end":                         {},
	"compaction/prune":                       {},
	"compaction/start":                       {},
	"compaction/summary":                     {},
	"feedback/record":                        {},
	"feedback/message-put":                   {},
	"feedback/message-delete":                {},
	"goal/change":                            {},
	"hook/invoked":                           {},
	"hook/result":                            {},
	"llm/retry":                              {},
	"llm/retry-started":                      {},
	"permission/preset":                      {},
	"plan/mode":                              {},
	"request/context":                        {},
	"request/header":                         {},
	"sandbox/mode":                           {},
	"schedule/change":                        {},
	"session-log-deepseek/delivery-accepted": {},
	"session/end-seed":                       {},
	"session/title":                          {},
	"session/title-llm-request":              {},
	"step/end":                               {},
	"step/start":                             {},
	"subagent/descriptor":                    {},
	"subagent/model-selection-policy":        {},
	"team/member":                            {},
	"team/message/delivered":                 {},
	"team/message/queued":                    {},
	"team/task":                              {},
	"todo/write":                             {},
	"tool-workflow/agent-end":                {},
	"tool-workflow/agent-start":              {},
	"tool-workflow/run-end":                  {},
	"tool-workflow/run-start":                {},
	"tool/call":                              {},
	"tool/code-dispatch":                     {},
	"tool/code-dispatch-start":               {},
	"tool/result":                            {},
	"turn/end":                               {},
	"turn/start":                             {},
	"user/message":                           {},
	"web/deepseek-search-llm-request":        {},
}

func isSurfaceEligibleType(typ string) bool {
	return typ == "user/message" || typ == "assistant/message" || typ == "tool/result"
}

func decodeSessionStorageRecord(line []byte) ([]Event, error) {
	return decodeSessionStorageRecordVersion(line, 1)
}

func decodeSessionStorageRecordVersion(line []byte, version int) ([]Event, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(line, &raw); err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, fmt.Errorf("record is not a JSON object")
	}
	var typ string
	if err := unmarshalRequired(raw["type"], &typ); err != nil || typ == "" {
		return nil, fmt.Errorf("record type is missing or invalid")
	}
	if typ == "text-chunks" || typ == "reasoning-chunks" || typ == "tool-call-chunks" {
		if version >= 2 {
			return nil, fmt.Errorf("format v2 cannot contain packed top-level assistant chunks")
		}
		return decodePackedChunks(typ, raw)
	}
	if version >= 2 && typ == "assistant/chunk" {
		return nil, fmt.Errorf("format v2 cannot contain top-level assistant/chunk")
	}
	for key := range raw {
		switch key {
		case "type", "seq", "time", "data", "sourceEventSeqs", "surfaceOp", "ignorable":
		default:
			return nil, fmt.Errorf("event %q has an invalid envelope", typ)
		}
	}
	for _, key := range []string{"type", "seq", "time", "data"} {
		if _, ok := raw[key]; !ok {
			return nil, fmt.Errorf("event %q is missing %s", typ, key)
		}
	}
	if marker, ok := raw["ignorable"]; ok && !bytes.Equal(bytes.TrimSpace(marker), []byte("true")) {
		return nil, fmt.Errorf("event %q has invalid ignorable marker", typ)
	}
	var event Event
	event.Type = typ
	if err := json.Unmarshal(raw["data"], &event.Data); err != nil {
		return nil, err
	}
	if err := unmarshalRequired(raw["seq"], &event.Seq); err != nil {
		return nil, fmt.Errorf("event %q has invalid seq", typ)
	}
	if err := unmarshalRequired(raw["time"], &event.Time); err != nil {
		return nil, fmt.Errorf("event %q has invalid time", typ)
	}
	if event.Seq < 0 || int64(event.Seq) > maxJSONSafeInteger || event.Time < -maxJSONSafeInteger || event.Time > maxJSONSafeInteger {
		return nil, fmt.Errorf("event %q has invalid seq or time", typ)
	}
	if _, ok := raw["ignorable"]; ok {
		event.Ignorable = true
	}
	if _, ok := knownSessionEventTypes[typ]; !ok && !event.Ignorable {
		return nil, fmt.Errorf("session event type %q (seq %d) is unknown and not marked ignorable", typ, event.Seq)
	}
	_, hasSurfaceOp := raw["surfaceOp"]
	_, hasSources := raw["sourceEventSeqs"]
	if isSurfaceEligibleType(typ) {
		if !hasSurfaceOp {
			return nil, fmt.Errorf("event %q requires a surfaceOp", typ)
		}
	} else if hasSurfaceOp || hasSources {
		return nil, fmt.Errorf("event %q is not surface-eligible and cannot carry surface metadata", typ)
	}
	if op, ok := raw["surfaceOp"]; ok {
		if err := validateSurfaceOp(op); err != nil {
			return nil, fmt.Errorf("event %q: %w", typ, err)
		}
		if err := json.Unmarshal(op, &event.SurfaceOp); err != nil {
			return nil, fmt.Errorf("event %q has invalid surfaceOp", typ)
		}
	}
	if sources, ok := raw["sourceEventSeqs"]; ok {
		if len(bytes.TrimSpace(sources)) == 0 || bytes.TrimSpace(sources)[0] != '[' {
			return nil, fmt.Errorf("event %q has invalid sourceEventSeqs", typ)
		}
		var err error
		if version >= 2 {
			event.SourceEventSeqs, err = decodeSessionSeqRanges(sources, int(event.Seq))
		} else {
			err = json.Unmarshal(sources, &event.SourceEventSeqs)
		}
		if err != nil || event.SourceEventSeqs == nil {
			return nil, fmt.Errorf("event %q has invalid sourceEventSeqs", typ)
		}
		if err := validateSourceEventSeqs(event); err != nil {
			return nil, err
		}
	}
	return []Event{event}, nil
}

func decodeSessionSeqRanges(raw json.RawMessage, max int) ([]int, error) {
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil || entries == nil {
		return nil, fmt.Errorf("sourceEventSeqs must be an array")
	}
	result := make([]int, 0, len(entries))
	hasRange := false
	for _, entry := range entries {
		var scalar int
		if json.Unmarshal(entry, &scalar) == nil {
			result = append(result, scalar)
			continue
		}
		var pair []int
		if json.Unmarshal(entry, &pair) != nil || len(pair) != 2 || pair[0] < 0 || pair[0] > pair[1] || pair[1] >= max || pair[1]-pair[0]+1 > max-len(result) {
			return nil, fmt.Errorf("invalid sourceEventSeqs range")
		}
		for value := pair[0]; value <= pair[1]; value++ {
			result = append(result, value)
		}
		hasRange = true
	}
	seen := make(map[int]struct{}, len(result))
	for index, value := range result {
		if value < 0 || value >= max {
			return nil, fmt.Errorf("sourceEventSeqs must reference earlier events")
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("sourceEventSeqs contains duplicates")
		}
		seen[value] = struct{}{}
		if hasRange && index > 0 && value <= result[index-1] {
			return nil, fmt.Errorf("sourceEventSeqs ranges must be strictly increasing")
		}
	}
	return result, nil
}

func encodeSessionSeqRanges(values []int) []any {
	if len(values) == 0 {
		return []any{}
	}
	for index := 1; index < len(values); index++ {
		if values[index] <= values[index-1] {
			result := make([]any, len(values))
			for item, value := range values {
				result[item] = value
			}
			return result
		}
	}
	result := make([]any, 0, len(values))
	for index := 0; index < len(values); {
		start := values[index]
		end := start
		for index+1 < len(values) && values[index+1] == end+1 {
			index++
			end++
		}
		if end-start >= 2 {
			result = append(result, []int{start, end})
		} else {
			result = append(result, start)
			if end-start == 1 {
				result = append(result, end)
			}
		}
		index++
	}
	return result
}

func decodeSessionSeedEvent(value any, index int) (Event, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return Event{}, fmt.Errorf("seed event at index %d is not losslessly JSON-serializable", index)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &raw); err != nil || raw == nil {
		return Event{}, fmt.Errorf("seed event at index %d has an invalid event envelope", index)
	}
	for key := range raw {
		switch key {
		case "type", "seq", "time", "data", "sourceEventSeqs", "surfaceOp", "ignorable":
		default:
			return Event{}, fmt.Errorf("seed event at index %d has an invalid event envelope", index)
		}
	}
	for _, key := range []string{"type", "seq", "time", "data"} {
		if _, ok := raw[key]; !ok {
			return Event{}, fmt.Errorf("seed event at index %d has an invalid event envelope", index)
		}
	}
	if marker, ok := raw["ignorable"]; ok && !bytes.Equal(bytes.TrimSpace(marker), []byte("true")) {
		return Event{}, fmt.Errorf("seed event at index %d has an invalid event envelope", index)
	}
	var event Event
	if unmarshalRequired(raw["type"], &event.Type) != nil || event.Type == "" ||
		unmarshalRequired(raw["seq"], &event.Seq) != nil || event.Seq < 0 || int64(event.Seq) > maxJSONSafeInteger ||
		unmarshalRequired(raw["time"], &event.Time) != nil || event.Time < -maxJSONSafeInteger || event.Time > maxJSONSafeInteger ||
		json.Unmarshal(raw["data"], &event.Data) != nil {
		return Event{}, fmt.Errorf("seed event at index %d has an invalid event envelope", index)
	}
	if int(event.Seq) != index {
		return Event{}, fmt.Errorf("seed event at index %d has seq %d (expected %d); seed must be contiguous from 0", index, event.Seq, index)
	}
	if _, ok := raw["surfaceOp"]; ok {
		if err := json.Unmarshal(raw["surfaceOp"], &event.SurfaceOp); err != nil {
			return Event{}, fmt.Errorf("seed event at index %d has an invalid event envelope", index)
		}
	}
	if _, ok := raw["sourceEventSeqs"]; ok {
		if err := json.Unmarshal(raw["sourceEventSeqs"], &event.SourceEventSeqs); err != nil || event.SourceEventSeqs == nil {
			return Event{}, fmt.Errorf("seed event at index %d has an invalid event envelope", index)
		}
	}
	if _, ok := raw["ignorable"]; ok {
		event.Ignorable = true
	}
	_, hasSurface := raw["surfaceOp"]
	_, hasSources := raw["sourceEventSeqs"]
	if isSurfaceEligibleType(event.Type) {
		if !hasSurface {
			return Event{}, fmt.Errorf("seed event at index %d requires a surfaceOp", index)
		}
		if err := validateSurfaceOp(raw["surfaceOp"]); err != nil {
			return Event{}, fmt.Errorf("seed event at index %d: %w", index, err)
		}
	} else if hasSurface || hasSources {
		return Event{}, fmt.Errorf("seed event at index %d cannot carry surface metadata", index)
	}
	if hasSources {
		if err := validateSourceEventSeqs(event); err != nil {
			return Event{}, fmt.Errorf("seed event at index %d: %w", index, err)
		}
	}
	if err := validateDynamicSeedEventShape(event, index); err != nil {
		return Event{}, err
	}
	return event, nil
}

func validateDynamicSeedEventShape(event Event, index int) error {
	location := fmt.Sprintf("seed event at index %d", index)
	if event.Type == "request/header-delta" {
		return fmt.Errorf("%s uses unsupported legacy request/header-delta format", location)
	}
	data, _ := event.Data.(map[string]any)
	if event.Type == "request/header" {
		if stringValue(data["reason"]) == "fallback" {
			return fmt.Errorf("%s uses unsupported legacy request/header reason \"fallback\"", location)
		}
		header, _ := data["header"].(map[string]any)
		config, _ := header["config"].(map[string]any)
		if stringValue(config["provider"]) == "" || stringValue(config["model"]) == "" {
			return fmt.Errorf("seed request/header at index %d lacks provider/model", index)
		}
		if effort, ok := config["reasoningEffort"]; ok {
			text, valid := effort.(string)
			if !valid || text == "" {
				return fmt.Errorf("seed request/header at index %d has an invalid reasoningEffort", index)
			}
		}
		if defaults, ok := header["adapterDefaults"]; ok {
			markers, ok := defaults.(map[string]any)
			if !ok {
				return fmt.Errorf("seed request/header at index %d has invalid adapterDefaults", index)
			}
			for key, marker := range markers {
				if (key != "reasoningEffort" && key != "maxTokens") || marker != true || marker == nil {
					return fmt.Errorf("seed request/header at index %d has invalid adapterDefaults", index)
				}
				if key == "reasoningEffort" {
					if _, exists := config["reasoningEffort"]; !exists {
						return fmt.Errorf("seed request/header at index %d has invalid adapterDefaults", index)
					}
				}
				if key == "maxTokens" {
					if _, exists := config["maxTokens"]; !exists {
						return fmt.Errorf("seed request/header at index %d has invalid adapterDefaults", index)
					}
				}
			}
		}
	}
	if event.Type != "user/message" && event.Type != "assistant/message" && event.Type != "tool/result" {
		return nil
	}
	message := data
	if event.Type != "user/message" {
		message, _ = data["message"].(map[string]any)
	}
	if message == nil || stringValue(message["id"]) == "" {
		return fmt.Errorf("seed %s at index %d lacks an identified message", event.Type, index)
	}
	wantRole := "user"
	if event.Type == "assistant/message" {
		wantRole = "assistant"
	}
	if stringValue(message["role"]) != wantRole {
		return fmt.Errorf("seed %s at index %d message must have role %q", event.Type, index, wantRole)
	}
	source, _ := message["source"].(map[string]any)
	if source == nil || stringValue(source["kind"]) == "" {
		return fmt.Errorf("seed %s at index %d message has invalid source", event.Type, index)
	}
	if _, ok := message["content"].([]any); !ok {
		if _, ok := message["content"].([]ContentBlock); !ok {
			return fmt.Errorf("seed %s at index %d message has invalid content", event.Type, index)
		}
	}
	if event.Type == "assistant/message" {
		if source["kind"] != "model" || stringValue(source["provider"]) == "" || stringValue(source["model"]) == "" {
			return fmt.Errorf("seed assistant/message at index %d message must have model source", index)
		}
	}
	if event.Type == "tool/result" {
		callID := stringValue(source["callId"])
		if source["kind"] != "tool" || callID == "" {
			return fmt.Errorf("seed tool/result at index %d message must have tool source", index)
		}
		blocks := contentBlocks(message["content"])
		if len(blocks) != 1 || blocks[0].Type != "tool-result" || blocks[0].ToolCallID != callID || blocks[0].Content == nil {
			return fmt.Errorf("seed tool/result at index %d message must contain one tool-result block", index)
		}
	}
	return nil
}

func unmarshalRequired(raw json.RawMessage, target any) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return fmt.Errorf("missing JSON value")
	}
	return json.Unmarshal(trimmed, target)
}

func validateSurfaceOp(raw json.RawMessage) error {
	var marker string
	if json.Unmarshal(raw, &marker) == nil && marker == "append" {
		return nil
	}
	var op map[string]json.RawMessage
	if err := json.Unmarshal(raw, &op); err != nil || !hasExactRawKeys(op, "op", "start", "end") {
		return fmt.Errorf("invalid surfaceOp")
	}
	var kind string
	var start, end int
	if unmarshalRequired(op["op"], &kind) != nil || kind != "replace" ||
		unmarshalRequired(op["start"], &start) != nil || unmarshalRequired(op["end"], &end) != nil ||
		start < 0 || end < 0 || int64(start) > maxJSONSafeInteger || int64(end) > maxJSONSafeInteger {
		return fmt.Errorf("invalid replace surfaceOp")
	}
	return nil
}

func validateSourceEventSeqs(event Event) error {
	if len(event.SourceEventSeqs) == 0 && event.Type != "assistant/message" {
		return fmt.Errorf("sourceEventSeqs on event %q must not be empty", event.Type)
	}
	seen := make(map[int]struct{}, len(event.SourceEventSeqs))
	for _, source := range event.SourceEventSeqs {
		if source < 0 || source >= int(event.Seq) {
			return fmt.Errorf("sourceEventSeqs on event %q must reference earlier events", event.Type)
		}
		if _, ok := seen[source]; ok {
			return fmt.Errorf("sourceEventSeqs on event %q contains duplicates", event.Type)
		}
		seen[source] = struct{}{}
	}
	return nil
}

func decodePackedChunks(tag string, raw map[string]json.RawMessage) ([]Event, error) {
	if !hasExactRawKeys(raw, "type", "seq0", "time0", "data") {
		return nil, fmt.Errorf("malformed %s storage row", tag)
	}
	var seq0 int
	var time0 int64
	var data map[string]json.RawMessage
	if unmarshalRequired(raw["seq0"], &seq0) != nil || seq0 < 0 || int64(seq0) > maxJSONSafeInteger ||
		unmarshalRequired(raw["time0"], &time0) != nil || time0 < -maxJSONSafeInteger || time0 > maxJSONSafeInteger ||
		unmarshalRequired(raw["data"], &data) != nil || data == nil {
		return nil, fmt.Errorf("malformed %s storage row", tag)
	}
	payloadKey := "texts"
	wantKeys := []string{"turn", "step", "index", "dt", "texts"}
	if tag == "tool-call-chunks" {
		payloadKey = "args"
		wantKeys = []string{"turn", "step", "index", "id", "dt", "args"}
		if _, ok := data["name"]; ok {
			wantKeys = append(wantKeys, "name")
		}
	}
	if !hasExactRawKeys(data, wantKeys...) {
		return nil, fmt.Errorf("malformed %s storage row", tag)
	}
	var turn, step, index float64
	var gaps []int64
	var payload []string
	if unmarshalRequired(data["turn"], &turn) != nil || unmarshalRequired(data["step"], &step) != nil ||
		unmarshalRequired(data["index"], &index) != nil || unmarshalRequired(data["dt"], &gaps) != nil ||
		unmarshalRequired(data[payloadKey], &payload) != nil || len(payload) == 0 || len(gaps) != len(payload)-1 {
		return nil, fmt.Errorf("malformed %s storage row", tag)
	}
	var id, name string
	if tag == "tool-call-chunks" {
		if unmarshalRequired(data["id"], &id) != nil {
			return nil, fmt.Errorf("malformed %s storage row", tag)
		}
		if rawName, ok := data["name"]; ok && unmarshalRequired(rawName, &name) != nil {
			return nil, fmt.Errorf("malformed %s storage row", tag)
		}
	}
	events := make([]Event, 0, len(payload))
	currentTime := time0
	for i, value := range payload {
		if i > 0 {
			gap := gaps[i-1]
			if gap < -maxJSONSafeInteger || gap > maxJSONSafeInteger {
				return nil, fmt.Errorf("malformed %s storage row", tag)
			}
			currentTime += gap
			if currentTime < -maxJSONSafeInteger || currentTime > maxJSONSafeInteger {
				return nil, fmt.Errorf("malformed %s storage row", tag)
			}
		}
		seq := seq0 + i
		if seq < seq0 || int64(seq) > maxJSONSafeInteger {
			return nil, fmt.Errorf("malformed %s storage row", tag)
		}
		chunk := map[string]any{"index": index}
		switch tag {
		case "text-chunks":
			chunk["type"], chunk["text"] = "text-delta", value
		case "reasoning-chunks":
			chunk["type"], chunk["text"] = "reasoning-delta", value
		case "tool-call-chunks":
			chunk["type"], chunk["id"], chunk["argumentsDelta"] = "tool-call-delta", id, value
			if _, ok := data["name"]; ok {
				chunk["name"] = name
			}
		}
		events = append(events, Event{Type: "assistant/chunk", Seq: SessionSeq(seq), Time: currentTime, Data: map[string]any{
			"turn": turn, "step": step, "chunk": chunk,
		}})
	}
	return events, nil
}

func hasExactRawKeys(value map[string]json.RawMessage, keys ...string) bool {
	if len(value) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := value[key]; !ok {
			return false
		}
	}
	return true
}

func foldSurfaceEvents(events []Event, strict bool) ([]Event, error) {
	surface := make([]Event, 0)
	for _, event := range events {
		if !isSurfaceEligibleType(event.Type) {
			continue
		}
		if isAppendSurfaceEvent(event) {
			surface = append(surface, event)
			continue
		}
		start, end, replacement := surfaceReplaceBounds(event.SurfaceOp)
		if !replacement {
			if strict {
				return nil, fmt.Errorf("surface event %q at seq %d requires a valid surfaceOp", event.Type, event.Seq)
			}
			surface = append(surface, event)
			continue
		}
		startIndex, endIndex := -1, -1
		for i := range surface {
			if int(surface[i].Seq) == start {
				startIndex = i
			}
			if int(surface[i].Seq) == end {
				endIndex = i
			}
		}
		if startIndex < 0 || endIndex < startIndex {
			if strict {
				return nil, fmt.Errorf("surface replace at seq %d references an invalid range", event.Seq)
			}
			surface = append(surface, event)
			continue
		}
		if strict {
			sources := make(map[int]struct{}, len(event.SourceEventSeqs))
			for _, source := range event.SourceEventSeqs {
				sources[source] = struct{}{}
			}
			for _, shadowed := range surface[startIndex : endIndex+1] {
				if _, ok := sources[int(shadowed.Seq)]; !ok {
					return nil, fmt.Errorf("surface replace at seq %d does not cite shadowed seq %d", event.Seq, shadowed.Seq)
				}
			}
			if event.Type == "tool/result" {
				if endIndex != startIndex {
					return nil, fmt.Errorf("tool/result surface replacement must rewrite exactly one current node")
				}
				original := surface[startIndex]
				if original.Type != "tool/result" {
					return nil, fmt.Errorf("tool/result surface replacement must target a current tool/result")
				}
				if !toolResultReplacementContentOnly(original, event) {
					return nil, fmt.Errorf("tool/result surface replacement may change only content")
				}
			}
		}
		next := make([]Event, 0, len(surface)-(endIndex-startIndex))
		next = append(next, surface[:startIndex]...)
		next = append(next, event)
		next = append(next, surface[endIndex+1:]...)
		surface = next
	}
	return surface, nil
}

func toolResultReplacementContentOnly(original, replacement Event) bool {
	originalData, ok := cloneJSONMap(original.Data)
	if !ok {
		return false
	}
	replacementData, ok := cloneJSONMap(replacement.Data)
	if !ok {
		return false
	}
	if !stripToolResultContent(originalData) || !stripToolResultContent(replacementData) {
		return false
	}
	return reflect.DeepEqual(originalData, replacementData)
}

func cloneJSONMap(value any) (map[string]any, bool) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	var cloned map[string]any
	if json.Unmarshal(data, &cloned) != nil || cloned == nil {
		return nil, false
	}
	return cloned, true
}

func stripToolResultContent(data map[string]any) bool {
	message, ok := data["message"].(map[string]any)
	if !ok {
		return false
	}
	blocks, ok := message["content"].([]any)
	if !ok || len(blocks) != 1 {
		return false
	}
	block, ok := blocks[0].(map[string]any)
	if !ok {
		return false
	}
	block["content"] = nil
	return true
}

func surfaceReplaceBounds(value any) (int, int, bool) {
	op, ok := value.(map[string]any)
	if !ok || len(op) != 3 || op["op"] != "replace" {
		return 0, 0, false
	}
	start, startOK := eventSeqNumber(op["start"])
	end, endOK := eventSeqNumber(op["end"])
	return start, end, startOK && endOK && start >= 0 && end >= 0
}

func eventSeqNumber(value any) (int, bool) {
	switch number := value.(type) {
	case int:
		return number, int64(number) >= -maxJSONSafeInteger && int64(number) <= maxJSONSafeInteger
	case SessionSeq:
		return int(number), int64(number) >= -maxJSONSafeInteger && int64(number) <= maxJSONSafeInteger
	case SessionLogOffset:
		return int(number), int64(number) >= -maxJSONSafeInteger && int64(number) <= maxJSONSafeInteger
	case int64:
		return int(number), number >= -maxJSONSafeInteger && number <= maxJSONSafeInteger && int64(int(number)) == number
	case float64:
		integer := int(number)
		return integer, number >= -float64(maxJSONSafeInteger) && number <= float64(maxJSONSafeInteger) && float64(integer) == number
	case json.Number:
		integer, err := number.Int64()
		return int(integer), err == nil && integer >= -maxJSONSafeInteger && integer <= maxJSONSafeInteger && int64(int(integer)) == integer
	default:
		return 0, false
	}
}
