package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

type timedAssistantChunk struct {
	time  int64
	chunk map[string]any
}

type assistantStreamActive struct {
	attemptID    string
	startedAfter int
	turn         int
	step         int
	nextIndex    int
	stream       []any
}

type assistantAttempt struct {
	engine    *Engine
	session   *Session
	attemptID string
	turn      int
	step      int
	ended     bool
}

func (e *Engine) beginAssistantAttempt(session *Session, turn, step int) *assistantAttempt {
	session.mu.Lock()
	session.assistantAttemptCounter++
	attemptID := fmt.Sprintf("%s:%d", session.Header.ID, session.assistantAttemptCounter)
	session.assistantStreamRevision++
	revision := session.assistantStreamRevision
	startedAfter := len(session.Events) - 1
	session.assistantStreamActive = &assistantStreamActive{attemptID: attemptID, startedAfter: startedAfter, turn: turn, step: step, stream: []any{}}
	session.mu.Unlock()
	e.emitAssistantStream(session.Header.ID, map[string]any{
		"type": "start", "attemptId": attemptID, "revision": revision,
		"startedAfterSeq": startedAfter, "turn": turn, "step": step,
	})
	return &assistantAttempt{engine: e, session: session, attemptID: attemptID, turn: turn, step: step}
}

func (attempt *assistantAttempt) pushDelta(delta Delta) error {
	if delta.Text != "" {
		if err := attempt.pushChunk(map[string]any{"type": "text-delta", "index": 0, "text": delta.Text}); err != nil {
			return err
		}
	}
	if delta.Reasoning != "" {
		if err := attempt.pushChunk(map[string]any{"type": "reasoning-delta", "index": 0, "text": delta.Reasoning}); err != nil {
			return err
		}
	}
	for _, call := range delta.ToolCalls {
		if err := attempt.pushChunk(map[string]any{"type": "tool-call-delta", "index": call.Index, "id": call.ID, "name": call.Name, "argumentsDelta": call.ArgumentsDelta}); err != nil {
			return err
		}
	}
	if len(delta.Usage) > 0 {
		return attempt.pushChunk(map[string]any{"type": "usage", "usage": cloneStringMap(delta.Usage)})
	}
	return nil
}

func (attempt *assistantAttempt) pushChunk(chunk map[string]any) error {
	if attempt == nil || attempt.ended {
		return errorsNewAssistantAttemptEnded()
	}
	now := time.Now().UnixMilli()
	session := attempt.session
	session.mu.Lock()
	active := session.assistantStreamActive
	if active == nil || active.attemptID != attempt.attemptID {
		session.mu.Unlock()
		return errorsNewAssistantAttemptEnded()
	}
	index := active.nextIndex
	active.nextIndex++
	active.stream = appendAssistantStreamRecord(active.stream, now, chunk)
	session.assistantStreamRevision++
	revision := session.assistantStreamRevision
	session.mu.Unlock()
	attempt.engine.emitAssistantStream(session.Header.ID, map[string]any{
		"type": "chunk", "attemptId": attempt.attemptID, "revision": revision,
		"index": index, "time": now, "chunk": chunk,
	})
	return nil
}

func appendAssistantStreamRecord(stream []any, timestamp int64, chunk map[string]any) []any {
	typeName, _ := chunk["type"].(string)
	index, indexOK := eventSeqNumber(chunk["index"])
	recordType, memberKey := "", ""
	switch typeName {
	case "text-delta":
		recordType, memberKey = "text-chunks", "texts"
	case "reasoning-delta":
		recordType, memberKey = "reasoning-chunks", "texts"
	case "tool-call-delta":
		recordType, memberKey = "tool-call-chunks", "args"
	}
	if recordType == "" || !indexOK {
		return append(stream, map[string]any{"type": "chunk", "time": timestamp, "chunk": cloneJSON(chunk)})
	}
	fragmentKey := "text"
	if typeName == "tool-call-delta" {
		fragmentKey = "argumentsDelta"
	}
	fragment, fragmentOK := chunk[fragmentKey].(string)
	if !fragmentOK {
		return append(stream, map[string]any{"type": "chunk", "time": timestamp, "chunk": cloneJSON(chunk)})
	}
	if typeName == "tool-call-delta" {
		id, idOK := chunk["id"].(string)
		name, hasName := chunk["name"].(string)
		if !idOK || id == "" || hasName && name == "" {
			return append(stream, map[string]any{"type": "chunk", "time": timestamp, "chunk": cloneJSON(chunk)})
		}
	}
	if len(stream) > 0 {
		previous, _ := stream[len(stream)-1].(map[string]any)
		previousType, _ := previous["type"].(string)
		previousIndex, _ := eventSeqNumber(previous["index"])
		previousTime, timeOK := lastAssistantRunTime(previous)
		gap := timestamp - previousTime
		compatible := previousType == recordType && previousIndex == index && timeOK && gap >= -maxJSONSafeInteger && gap <= maxJSONSafeInteger
		if compatible && typeName == "tool-call-delta" {
			compatible = stringValue(previous["id"]) == stringValue(chunk["id"])
			previousName, previousHasName := previous["name"]
			chunkName, chunkHasName := chunk["name"]
			compatible = compatible && previousHasName == chunkHasName && previousName == chunkName
		}
		if compatible {
			previous["dt"] = append(intSlice(previous["dt"]), int(gap))
			previous[memberKey] = append(stringSlice(previous[memberKey]), fragment)
			return stream
		}
	}
	record := map[string]any{"type": recordType, "time0": timestamp, "index": index, "dt": []int{}, memberKey: []string{fragment}}
	if typeName == "tool-call-delta" {
		record["id"] = chunk["id"]
		if name, exists := chunk["name"]; exists {
			record["name"] = name
		}
	}
	return append(stream, record)
}

func assistantStreamChunks(value any) []timedAssistantChunk {
	stream, _ := value.([]any)
	result := make([]timedAssistantChunk, 0)
	for _, raw := range stream {
		record, _ := raw.(map[string]any)
		typeName, _ := record["type"].(string)
		if typeName == "chunk" {
			timestamp, _ := eventInt64(record["time"])
			chunk, _ := record["chunk"].(map[string]any)
			if chunk != nil {
				result = append(result, timedAssistantChunk{time: timestamp, chunk: chunk})
			}
			continue
		}
		timestamp, timeOK := eventInt64(record["time0"])
		index, indexOK := eventSeqNumber(record["index"])
		if !timeOK || !indexOK {
			continue
		}
		members := stringSlice(record["texts"])
		chunkType := "text-delta"
		if typeName == "reasoning-chunks" {
			chunkType = "reasoning-delta"
		} else if typeName == "tool-call-chunks" {
			chunkType, members = "tool-call-delta", stringSlice(record["args"])
		} else if typeName != "text-chunks" {
			continue
		}
		deltas := intSlice(record["dt"])
		for memberIndex, fragment := range members {
			if memberIndex > 0 && memberIndex-1 < len(deltas) {
				timestamp += int64(deltas[memberIndex-1])
			}
			chunk := map[string]any{"type": chunkType, "index": index}
			if chunkType == "tool-call-delta" {
				chunk["id"], chunk["argumentsDelta"] = record["id"], fragment
				if name, exists := record["name"]; exists {
					chunk["name"] = name
				}
			} else {
				chunk["text"] = fragment
			}
			result = append(result, timedAssistantChunk{time: timestamp, chunk: chunk})
		}
	}
	return result
}

func lastAssistantRunTime(record map[string]any) (int64, bool) {
	timestamp, ok := eventInt64(record["time0"])
	if !ok {
		return 0, false
	}
	for _, delta := range intSlice(record["dt"]) {
		timestamp += int64(delta)
	}
	return timestamp, true
}

func stringSlice(value any) []string {
	if values, ok := value.([]string); ok {
		return values
	}
	values, _ := value.([]any)
	result := make([]string, 0, len(values))
	for _, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil
		}
		result = append(result, text)
	}
	return result
}

func intSlice(value any) []int {
	if values, ok := value.([]int); ok {
		return values
	}
	values, _ := value.([]any)
	result := make([]int, 0, len(values))
	for _, value := range values {
		integer, ok := eventSeqNumber(value)
		if !ok {
			return nil
		}
		result = append(result, integer)
	}
	return result
}

func eventInt64(value any) (int64, bool) {
	switch number := value.(type) {
	case int64:
		return number, number >= -maxJSONSafeInteger && number <= maxJSONSafeInteger
	case int:
		integer := int64(number)
		return integer, integer >= -maxJSONSafeInteger && integer <= maxJSONSafeInteger
	case float64:
		if !math.IsNaN(number) && !math.IsInf(number, 0) && number >= -float64(maxJSONSafeInteger) && number <= float64(maxJSONSafeInteger) && math.Trunc(number) == number {
			return int64(number), true
		}
	case json.Number:
		integer, err := number.Int64()
		return integer, err == nil && integer >= -maxJSONSafeInteger && integer <= maxJSONSafeInteger
	}
	return 0, false
}

func errorsNewAssistantAttemptEnded() error {
	return fmt.Errorf("assistant stream attempt is no longer active")
}

func (attempt *assistantAttempt) stream() []any {
	if attempt == nil {
		return []any{}
	}
	attempt.session.mu.Lock()
	defer attempt.session.mu.Unlock()
	active := attempt.session.assistantStreamActive
	if active == nil || active.attemptID != attempt.attemptID {
		return []any{}
	}
	return cloneJSON(active.stream).([]any)
}

func (attempt *assistantAttempt) settle(event Event) {
	if attempt == nil || attempt.ended {
		return
	}
	attempt.ended = true
	session := attempt.session
	session.mu.Lock()
	active := session.assistantStreamActive
	index := 0
	if active != nil && active.attemptID == attempt.attemptID {
		index = active.nextIndex
		session.assistantStreamActive = nil
	}
	session.assistantStreamRevision++
	revision := session.assistantStreamRevision
	session.mu.Unlock()
	attempt.engine.emitAssistantStream(session.Header.ID, map[string]any{
		"type": "end", "attemptId": attempt.attemptID, "revision": revision, "index": index,
		"outcome": map[string]any{"kind": "committed", "eventType": event.Type, "seq": event.Seq},
	})
}

func (attempt *assistantAttempt) abandon() {
	if attempt == nil || attempt.ended {
		return
	}
	attempt.ended = true
	session := attempt.session
	session.mu.Lock()
	active := session.assistantStreamActive
	index := 0
	if active != nil && active.attemptID == attempt.attemptID {
		index = active.nextIndex
		session.assistantStreamActive = nil
	}
	session.assistantStreamRevision++
	revision := session.assistantStreamRevision
	session.mu.Unlock()
	attempt.engine.emitAssistantStream(session.Header.ID, map[string]any{
		"type": "end", "attemptId": attempt.attemptID, "revision": revision, "index": index,
		"outcome": map[string]any{"kind": "abandoned"},
	})
}

func (e *Engine) assistantStreamSnapshot(session *Session) map[string]any {
	session.mu.Lock()
	defer session.mu.Unlock()
	result := map[string]any{"revision": session.assistantStreamRevision}
	if active := session.assistantStreamActive; active != nil {
		result["activeAttempt"] = map[string]any{
			"attemptId": active.attemptID, "startedAfterSeq": active.startedAfter,
			"turn": active.turn, "step": active.step, "nextIndex": active.nextIndex,
			"stream": cloneJSON(active.stream),
		}
	}
	return result
}

func (e *Engine) emitAssistantStream(id string, frame map[string]any) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for channel := range e.assistantStreamSubs[id] {
		select {
		case channel <- cloneStringMap(frame):
		default:
		}
	}
}

func (e *Engine) subscribeAssistantStream(ctx context.Context, id string) <-chan map[string]any {
	channel := make(chan map[string]any, 64)
	e.mu.Lock()
	if e.assistantStreamSubs[id] == nil {
		e.assistantStreamSubs[id] = map[chan map[string]any]struct{}{}
	}
	e.assistantStreamSubs[id][channel] = struct{}{}
	e.mu.Unlock()
	go func() {
		<-ctx.Done()
		e.mu.Lock()
		if _, exists := e.assistantStreamSubs[id][channel]; exists {
			delete(e.assistantStreamSubs[id], channel)
			close(channel)
		}
		e.mu.Unlock()
	}()
	return channel
}
