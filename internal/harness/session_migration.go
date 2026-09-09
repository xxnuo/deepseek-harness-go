package harness

import (
	"fmt"
	"reflect"
)

type legacyAssistantAttempt struct {
	turn     int
	step     int
	chunks   []Event
	buffered []Event
	terminal bool
}

type sessionMigrationState struct {
	header     SessionHeader
	sourceCut  int
	mapping    map[int]int
	events     []Event
	targetCut  int
	cutSet     bool
	pending    *legacyAssistantAttempt
	lastTime   int64
	messageIDs map[int]string
	compaction string
}

func inheritedSeedMarker(events []Event) (int, bool) {
	marker := 0
	found := false
	for _, event := range events {
		if event.Type != "session/end-seed" {
			continue
		}
		data, _ := event.Data.(map[string]any)
		if data["inherited"] == true {
			marker, found = int(event.Seq), true
		}
	}
	return marker, found
}

func migrateLegacySessionEvents(header SessionHeader, inherited SessionLogOffset, version int, source []Event) ([]Event, SessionLogOffset, error) {
	state := &sessionMigrationState{
		header: header, sourceCut: int(inherited), mapping: map[int]int{}, targetCut: 0,
		cutSet: !header.IsSeeded, messageIDs: map[int]string{}, lastTime: header.CreatedAt,
	}
	for _, original := range source {
		event := cloneSessionEvent(original)
		if version == 0 {
			var err error
			event, err = normalizeLegacyV0Event(event, header.ID, state)
			if err != nil {
				return nil, 0, err
			}
		}
		state.lastTime = event.Time
		if event.Type == "assistant/chunk" {
			if err := state.acceptChunk(event); err != nil {
				return nil, 0, err
			}
			continue
		}
		if event.Type == "assistant/message" {
			if err := state.acceptMessage(event); err != nil {
				return nil, 0, err
			}
			continue
		}
		if closesLegacyAssistantAttempt(event.Type) {
			if err := state.finishAttempt(); err != nil {
				return nil, 0, err
			}
			if err := state.emitSource(event); err != nil {
				return nil, 0, err
			}
			continue
		}
		if state.pending != nil {
			state.pending.buffered = append(state.pending.buffered, event)
			continue
		}
		if err := state.emitSource(event); err != nil {
			return nil, 0, err
		}
	}
	if err := state.finishAttempt(); err != nil {
		return nil, 0, err
	}
	if header.IsSeeded && !state.cutSet {
		state.targetCut = len(state.events)
		state.cutSet = true
		state.emitGenerated(Event{Type: "session/end-seed", Time: state.lastTime, Data: map[string]any{"inherited": true}})
	}
	return state.events, SessionLogOffset(state.targetCut), nil
}

func normalizeLegacyV0Event(event Event, sessionID string, state *sessionMigrationState) (Event, error) {
	switch event.Type {
	case "compact/start":
		event.Type = "compaction/start"
	case "compact/summary":
		event.Type = "compaction/summary"
	case "compact/end":
		event.Type = "compaction/end"
	case "compact/prune":
		event.Type = "compaction/prune"
	case "request/header-delta", "mode/set":
		return Event{}, fmt.Errorf("session %q contains unsupported legacy %s event at seq %d", sessionID, event.Type, event.Seq)
	}
	data, _ := event.Data.(map[string]any)
	if event.Type == "request/header" {
		if data["reason"] == "fallback" {
			return Event{}, fmt.Errorf("session %q contains unsupported request/header reason fallback at seq %d", sessionID, event.Seq)
		}
		if requestHeader, ok := data["header"].(map[string]any); ok {
			delete(requestHeader, "messagePrefix")
		}
	}
	if event.Type == "steering/message" {
		event.Type = "user/message"
		if message, ok := data["message"].(map[string]any); ok {
			event.Data = message
			data = message
		} else {
			delete(data, "turn")
			data["id"] = fmt.Sprintf("legacy-message:%s:%d", sessionID, event.Seq)
			data["role"] = "user"
		}
	}
	if event.Type == "turn/start" {
		delete(data, "trigger")
	}
	if event.Type == "turn/end" {
		if reason, ok := data["reason"].(map[string]any); ok {
			switch reason["kind"] {
			case "disposed":
				data["reason"] = map[string]any{"kind": "aborted", "reason": map[string]any{"kind": "disposed"}}
			case "aborted":
				if _, exists := reason["reason"]; !exists {
					data["reason"] = map[string]any{"kind": "aborted", "reason": map[string]any{"kind": "legacy"}}
				}
			case "error":
				if _, exists := reason["error"]; !exists {
					if failure, ok := reason["failure"].(map[string]any); ok {
						data["reason"] = map[string]any{"kind": "error", "error": failure}
					} else {
						code, _ := reason["code"].(string)
						if code == "" {
							code = "UNKNOWN"
						}
						data["reason"] = map[string]any{"kind": "error", "error": map[string]any{"message": stringValue(reason["message"]), "code": code}}
					}
				}
			}
		}
	}
	if event.Type == "llm/retry" {
		if _, exists := data["retryId"]; !exists {
			data["retryId"] = fmt.Sprintf("legacy-retry:%s:%d", sessionID, event.Seq)
		}
	}
	if event.Type == "compaction/start" {
		if value, _ := data["compactionId"].(string); value != "" {
			state.compaction = value
		} else {
			state.compaction = fmt.Sprintf("legacy-compaction:%s:%d", sessionID, event.Seq)
			data["compactionId"] = state.compaction
		}
	} else if state.compaction != "" && (event.Type == "compaction/summary" || event.Type == "compaction/end") {
		if _, exists := data["compactionId"]; !exists {
			data["compactionId"] = state.compaction
		}
		if event.Type == "compaction/end" {
			state.compaction = ""
		}
	}
	if event.Type == "user/message" {
		if _, exists := data["id"]; !exists {
			data["id"] = fmt.Sprintf("legacy-message:%s:%d", sessionID, event.Seq)
			data["role"] = "user"
		}
		if id, _ := data["id"].(string); id != "" {
			state.messageIDs[int(event.Seq)] = id
		}
	}
	if event.Type == "assistant/message" {
		if _, exists := data["message"]; !exists {
			content := data["content"]
			provenance, _ := data["provenance"].(map[string]any)
			delete(data, "content")
			delete(data, "provenance")
			source := cloneStringMap(provenance)
			source["kind"] = "model"
			data["message"] = map[string]any{"id": fmt.Sprintf("legacy-message:%s:%d", sessionID, event.Seq), "role": "assistant", "content": content, "source": source}
		}
		if message, ok := data["message"].(map[string]any); ok {
			if id, _ := message["id"].(string); id != "" {
				state.messageIDs[int(event.Seq)] = id
			}
		}
	}
	if event.Type == "tool/result" {
		if _, exists := data["message"]; !exists {
			callID, _ := data["callId"].(string)
			content := data["content"]
			isError, _ := data["isError"].(bool)
			delete(data, "callId")
			delete(data, "content")
			delete(data, "isError")
			id := fmt.Sprintf("legacy-message:%s:%d", sessionID, event.Seq)
			if start, _, ok := surfaceReplaceBounds(event.SurfaceOp); ok {
				if inheritedID := state.messageIDs[start]; inheritedID != "" {
					id = inheritedID
				}
			}
			data["message"] = map[string]any{"id": id, "role": "user", "content": []any{map[string]any{"type": "tool-result", "toolCallId": callID, "content": content, "isError": isError}}, "source": map[string]any{"kind": "tool", "callId": callID}}
		}
	}
	return event, nil
}

func (state *sessionMigrationState) acceptChunk(event Event) error {
	data, ok := event.Data.(map[string]any)
	if !ok {
		return fmt.Errorf("assistant/chunk %d data must be an object", event.Seq)
	}
	turn, step := eventInt(data["turn"]), eventInt(data["step"])
	if turn < 0 || step < 0 {
		return fmt.Errorf("assistant/chunk %d has invalid turn or step", event.Seq)
	}
	if state.pending != nil && (state.pending.terminal || state.pending.turn != turn || state.pending.step != step) {
		if err := state.finishAttempt(); err != nil {
			return err
		}
	}
	if state.pending == nil {
		state.pending = &legacyAssistantAttempt{turn: turn, step: step}
	}
	if state.header.IsSeeded && len(state.pending.chunks) > 0 && int(state.pending.chunks[0].Seq) < state.sourceCut && int(event.Seq) >= state.sourceCut {
		return fmt.Errorf("inherited Session cut %d splits one Assistant attempt", state.sourceCut)
	}
	state.pending.chunks = append(state.pending.chunks, event)
	if chunk, _ := data["chunk"].(map[string]any); chunk["type"] == "finish" {
		state.pending.terminal = true
	}
	return nil
}

func (state *sessionMigrationState) acceptMessage(event Event) error {
	data, ok := event.Data.(map[string]any)
	if !ok {
		return fmt.Errorf("assistant/message %d data must be an object", event.Seq)
	}
	turn, step := eventInt(data["turn"]), eventInt(data["step"])
	stream := []any{}
	if state.pending != nil {
		pending := state.pending
		if pending.turn != turn || pending.step != step {
			if err := state.finishAttempt(); err != nil {
				return err
			}
		} else {
			want := make([]int, len(pending.chunks))
			for index := range pending.chunks {
				want[index] = int(pending.chunks[index].Seq)
			}
			if event.SourceEventSeqs != nil && !reflect.DeepEqual(event.SourceEventSeqs, want) {
				return fmt.Errorf("assistant/message %d chunk provenance is not one complete ordered attempt", event.Seq)
			}
			stream = legacyAttemptStream(pending.chunks)
			for _, buffered := range pending.buffered {
				if err := state.emitSource(buffered); err != nil {
					return err
				}
			}
			state.pending = nil
		}
	}
	data["stream"] = stream
	event.SourceEventSeqs = nil
	return state.emitSource(event)
}

func (state *sessionMigrationState) finishAttempt() error {
	pending := state.pending
	if pending == nil {
		return nil
	}
	if len(pending.chunks) > 0 {
		last := pending.chunks[len(pending.chunks)-1]
		state.ensureCut(int(last.Seq), last.Time, "assistant/attempt")
		state.emitGenerated(Event{Type: "assistant/attempt", Time: last.Time, Data: map[string]any{"turn": pending.turn, "step": pending.step, "stream": legacyAttemptStream(pending.chunks)}})
	}
	for _, buffered := range pending.buffered {
		if err := state.emitSource(buffered); err != nil {
			return err
		}
	}
	state.pending = nil
	return nil
}

func legacyAttemptStream(chunks []Event) []any {
	stream := make([]any, 0, len(chunks))
	for _, event := range chunks {
		data, _ := event.Data.(map[string]any)
		stream = append(stream, map[string]any{"type": "chunk", "time": event.Time, "chunk": cloneJSON(data["chunk"])})
	}
	return stream
}

func closesLegacyAssistantAttempt(eventType string) bool {
	return eventType == "turn/end" || eventType == "step/end" || eventType == "llm/retry" || eventType == "llm/retry-started"
}

func (state *sessionMigrationState) ensureCut(origin int, eventTime int64, eventType string) {
	if !state.header.IsSeeded || state.cutSet || origin < state.sourceCut {
		return
	}
	state.targetCut = len(state.events)
	state.cutSet = true
	if origin == state.sourceCut && eventType == "session/end-seed" {
		return
	}
	state.emitGenerated(Event{Type: "session/end-seed", Time: eventTime, Data: map[string]any{"inherited": true}})
}

func (state *sessionMigrationState) emitSource(source Event) error {
	state.ensureCut(int(source.Seq), source.Time, source.Type)
	if source.Type == "session/end-seed" && state.header.IsSeeded && int(source.Seq) == state.sourceCut {
		data, _ := source.Data.(map[string]any)
		data["inherited"] = true
	}
	target := cloneSessionEvent(source)
	target.Seq = SessionSeq(len(state.events))
	if target.SourceEventSeqs != nil {
		mapped := make([]int, len(target.SourceEventSeqs))
		for index, value := range target.SourceEventSeqs {
			next, ok := state.mapping[value]
			if !ok {
				return fmt.Errorf("event %d source %d was removed during migration", source.Seq, value)
			}
			mapped[index] = next
		}
		target.SourceEventSeqs = mapped
	}
	if start, end, ok := surfaceReplaceBounds(target.SurfaceOp); ok {
		mappedStart, startOK := state.mapping[start]
		mappedEnd, endOK := state.mapping[end]
		if !startOK || !endOK {
			return fmt.Errorf("event %d replacement range was removed during migration", source.Seq)
		}
		target.SurfaceOp = map[string]any{"op": "replace", "start": mappedStart, "end": mappedEnd}
	}
	state.mapping[int(source.Seq)] = len(state.events)
	state.events = append(state.events, target)
	return nil
}

func (state *sessionMigrationState) emitGenerated(event Event) {
	event.Seq = SessionSeq(len(state.events))
	state.events = append(state.events, event)
}
