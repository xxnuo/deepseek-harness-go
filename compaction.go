package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

var errNoCompactableHistory = errors.New("no compactable history")

type compactRequest struct {
	turn            *int
	sourceCommandID string
	selection       ModelSelection
	system          string
	tools           []ToolSchema
	retainTokens    int
	wholeSurface    bool
}

type compactResult struct {
	shadowedSeqs       []int
	shadowedTokenCount int
	summarySeq         int
}

func (e *Engine) compactSession(ctx context.Context, s *Session, request compactRequest) (result compactResult, err error) {
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if e.cfg.Compaction.Disabled {
		return result, errors.New("compaction disabled")
	}
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	if compactionOpen(events) {
		return result, errors.New("compaction already in progress")
	}
	surface, err := foldSurfaceEvents(events, true)
	if err != nil {
		return result, err
	}
	selected := selectCompactableSurface(surface, request.retainTokens)
	if len(selected) == 0 {
		return result, errNoCompactableHistory
	}
	shadowedSeqs := make([]int, len(selected))
	shadowedTokens := 0
	for index, event := range selected {
		shadowedSeqs[index] = event.Seq
		shadowedTokens += estimateProjectionEvent(event)
	}
	messages := e.hydrateChatMessages(transcriptMessages(selected, int(^uint(0)>>1)))
	if len(messages) == 0 || !toolMessagesBalanced(messages) {
		return result, errNoCompactableHistory
	}
	selection := request.selection
	if selection.Provider == "" {
		selection.Provider = e.cfg.Provider
	}
	if selection.Model == "" {
		selection.Model = e.cfg.Model
	}
	e.mu.RLock()
	provider := e.providers[selection.Provider]
	e.mu.RUnlock()
	if provider == nil {
		return result, fmt.Errorf("compaction model unavailable: %s/%s", selection.Provider, selection.Model)
	}
	compactionID := newID("compact")
	owner := any(nil)
	if request.turn != nil {
		owner = *request.turn
	}
	lifecycle := map[string]any{"compactionId": compactionID, "turn": owner}
	if request.sourceCommandID != "" {
		lifecycle["sourceCommandId"] = request.sourceCommandID
	}
	start, err := e.appendEvent(s, "compaction/start", lifecycle)
	if err != nil {
		return result, err
	}
	closed := false
	defer func() {
		if err == nil || closed {
			return
		}
		failure := cloneStringMap(lifecycle)
		failure["error"] = err.Error()
		_, _ = e.appendEvent(s, "compaction/end", failure)
	}()
	requestMessages := append(append([]ChatMessage(nil), messages...), ChatMessage{Role: "user", Content: compactionInstruction})
	completion, completeErr := provider.Complete(ctx, ChatRequest{
		SessionID: s.Header.ID, Model: selection.Model, System: request.system, Messages: requestMessages, Tools: request.tools,
		MaxTokens: e.cfg.Compaction.MaxTokens,
	}, func(Delta) error { return nil })
	if completeErr != nil {
		return result, completeErr
	}
	if completion.Finish == "length" {
		return result, errors.New("summarization truncated at the token cap")
	}
	summaryText := strings.TrimSpace(completion.Text)
	if summaryText == "" {
		return result, errors.New("summarization produced no text summary content")
	}
	framedTokens := estimateProjectionContent([]ContentBlock{
		{Type: "text", Text: compactCheckpointPreamble + "\n\n<compacted-summary>"},
		{Type: "text", Text: summaryText},
		{Type: "text", Text: "</compacted-summary>"},
	}) + 4
	if framedTokens >= shadowedTokens {
		return result, fmt.Errorf("summary is not smaller than the shadowed content (%d >= %d estimated tokens)", framedTokens, shadowedTokens)
	}
	s.mu.Lock()
	currentEvents := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	currentSurface, foldErr := foldSurfaceEvents(currentEvents, true)
	if foldErr != nil {
		return result, foldErr
	}
	if request.wholeSurface {
		if !slices.Equal(surfaceSeqs(surface), surfaceSeqs(currentSurface)) {
			return result, errors.New("session surface changed during compaction")
		}
	} else if !surfaceSpanPresent(currentSurface, shadowedSeqs) {
		return result, errors.New("selected history changed during compaction")
	}
	summary := []ContentBlock{{Type: "text", Text: summaryText}}
	summaryData := cloneStringMap(lifecycle)
	summaryData["summary"] = summary
	summaryData["rawOutput"] = summary
	summaryData["llmStreamCall"] = true
	summaryData["shadowedRange"] = map[string]any{"start": shadowedSeqs[0], "end": shadowedSeqs[len(shadowedSeqs)-1]}
	summaryData["shadowedSeqs"] = shadowedSeqs
	summaryData["shadowedTokenCount"] = shadowedTokens
	summaryData["provider"] = selection.Provider
	summaryData["model"] = selection.Model
	summaryData["maxTokens"] = e.cfg.Compaction.MaxTokens
	if len(completion.Usage) > 0 {
		summaryData["usage"] = completion.Usage
	}
	summaryEvent, err := e.appendEvent(s, "compaction/summary", summaryData)
	if err != nil {
		return result, err
	}
	checkpoint := map[string]any{
		"id": newID("msg"), "role": "user",
		"content": []ContentBlock{
			{Type: "text", Text: compactCheckpointPreamble + "\n\n<compacted-summary>"},
			{Type: "text", Text: summaryText},
			{Type: "text", Text: "</compacted-summary>"},
		},
		"source": map[string]any{"kind": "plugin", "plugin": "compact", "compactionId": compactionID},
	}
	if request.sourceCommandID != "" {
		checkpoint["source"].(map[string]any)["sourceCommandId"] = request.sourceCommandID
	}
	sources := append([]int{start.Seq, summaryEvent.Seq}, shadowedSeqs...)
	if _, err := e.appendEventWithMetadata(s, "user/message", checkpoint, map[string]any{
		"op": "replace", "start": shadowedSeqs[0], "end": shadowedSeqs[len(shadowedSeqs)-1],
	}, sources, false); err != nil {
		return result, err
	}
	if _, err := e.appendEvent(s, "compaction/end", lifecycle); err != nil {
		return result, err
	}
	closed = true
	return compactResult{shadowedSeqs: shadowedSeqs, shadowedTokenCount: shadowedTokens, summarySeq: summaryEvent.Seq}, nil
}

func selectCompactableSurface(surface []Event, retainTokens int) []Event {
	if len(surface) < 2 {
		return nil
	}
	keepFrom := len(surface)
	retained := 0
	for index := len(surface) - 1; index >= 0; index-- {
		retained += estimateProjectionEvent(surface[index])
		keepFrom = index
		if retained >= retainTokens {
			break
		}
	}
	if keepFrom == 0 {
		return nil
	}
	for keepFrom > 0 {
		head := transcriptMessages(surface[:keepFrom], int(^uint(0)>>1))
		tail := transcriptMessages(surface[keepFrom:], int(^uint(0)>>1))
		if len(head) > 0 && toolMessagesBalanced(head) && toolMessagesBalanced(tail) {
			return append([]Event(nil), surface[:keepFrom]...)
		}
		keepFrom--
	}
	return nil
}

func surfaceSeqs(surface []Event) []int {
	seqs := make([]int, len(surface))
	for index, event := range surface {
		seqs[index] = event.Seq
	}
	return seqs
}

func cloneStringMap(value map[string]any) map[string]any {
	copy := make(map[string]any, len(value)+1)
	for key, item := range value {
		copy[key] = item
	}
	return copy
}

func estimatedRequestTokens(system string, tools []ToolSchema, messages []ChatMessage) int {
	total := estimateMessagesTokens(messages)
	if system != "" {
		total += projectionDensityPrice(system) + 4
	}
	if len(tools) > 0 {
		encoded, _ := json.Marshal(tools)
		total += projectionDensityPrice(string(encoded)) + 4
	}
	return total
}

func (e *Engine) durableMessages(s *Session, turn int) []ChatMessage {
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	return e.hydrateChatMessages(transcriptMessages(events, turn))
}

func (e *Engine) contextWindowFor(selection ModelSelection) int {
	provider := selection.Provider
	if provider == "" {
		provider = e.cfg.Provider
	}
	if provider != "deepseek-official" {
		return 0
	}
	modelID := selection.Model
	if modelID == "" {
		modelID = e.cfg.Model
	}
	for _, model := range deepSeekCatalog(deepSeekEffectiveSettings(e)) {
		if model.ID == modelID {
			return model.ContextWindow
		}
	}
	return deepSeekDefaultContext
}

func (e *Engine) compactForPressure(ctx context.Context, s *Session, turn int, selection ModelSelection, system string, tools []ToolSchema) (bool, error) {
	config := e.cfg.Compaction
	if config.Disabled || config.AutoDisabled {
		return false, nil
	}
	contextWindow := e.contextWindowFor(selection)
	if contextWindow == 0 {
		return false, nil
	}
	threshold := int(float64(contextWindow) * config.ThresholdRatio)
	retainTokens := config.RetainTokens
	if retainTokens == 0 {
		retainTokens = int(float64(contextWindow) * config.RetainRatio)
	}
	if retainTokens >= threshold {
		return false, fmt.Errorf("compaction retain tokens %d must be below threshold %d", retainTokens, threshold)
	}
	messages := e.durableMessages(s, turn)
	if estimatedRequestTokens(system, tools, messages) < threshold {
		return false, nil
	}
	pruned, err := e.pruneToolResults(s)
	changed := pruned > 0
	if err != nil {
		return changed, err
	}
	messages = e.durableMessages(s, turn)
	if estimatedRequestTokens(system, tools, messages) < threshold {
		return changed, nil
	}
	for attempt := 0; attempt <= config.CompactionRetries; attempt++ {
		_, err = e.compactSession(ctx, s, compactRequest{
			turn: &turn, selection: selection, system: system, tools: tools,
			retainTokens: retainTokens, wholeSurface: true,
		})
		if errors.Is(err, errNoCompactableHistory) {
			break
		}
		if err != nil {
			return changed, err
		}
		changed = true
		messages = e.durableMessages(s, turn)
		if estimatedRequestTokens(system, tools, messages) < threshold {
			return true, nil
		}
	}
	return changed, fmt.Errorf("compaction still above threshold (%d estimated tokens >= %d)", estimatedRequestTokens(system, tools, e.durableMessages(s, turn)), threshold)
}

func (e *Engine) compactForOverflow(ctx context.Context, s *Session, turn int, selection ModelSelection, system string, tools []ToolSchema) (bool, error) {
	config := e.cfg.Compaction
	if config.Disabled || config.AutoDisabled {
		return false, nil
	}
	pruned, err := e.pruneToolResults(s)
	changed := pruned > 0
	if err != nil {
		return changed, err
	}
	_, err = e.compactSession(ctx, s, compactRequest{
		turn: &turn, selection: selection, system: system, tools: tools,
		wholeSurface: true,
	})
	if errors.Is(err, errNoCompactableHistory) {
		return changed, nil
	}
	return changed || err == nil, err
}

func (e *Engine) pruneToolResults(s *Session) (int, error) {
	config := e.cfg.ToolResultPruner
	if config.Disabled {
		return 0, nil
	}
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	surface, err := foldSurfaceEvents(events, true)
	if err != nil {
		return 0, err
	}
	pruned := 0
	for _, event := range surface {
		if event.Type != "tool/result" {
			continue
		}
		data, _ := cloneJSON(event.Data).(map[string]any)
		message := nestedMessage(data)
		blocks := contentBlocks(message["content"])
		if len(blocks) != 1 || blocks[0].Type != "tool-result" {
			continue
		}
		content, changed := pruneToolResultContent(blocks[0].Content, config)
		if !changed {
			continue
		}
		blocks[0].Content = content
		message["content"] = blocks
		shadowedTokens := estimateProjectionEvent(event)
		if _, err := e.appendEvent(s, "compaction/prune", map[string]any{
			"shadowedRange": map[string]any{"start": event.Seq, "end": event.Seq},
			"shadowedSeqs":  []int{event.Seq}, "shadowedTokenCount": shadowedTokens,
		}); err != nil {
			return pruned, err
		}
		if _, err := e.appendEventWithMetadata(s, "tool/result", data, map[string]any{
			"op": "replace", "start": event.Seq, "end": event.Seq,
		}, []int{event.Seq}, false); err != nil {
			return pruned, err
		}
		pruned++
	}
	return pruned, nil
}

func pruneToolResultContent(blocks []ContentBlock, config ToolResultPruneConfig) ([]ContentBlock, bool) {
	total := 0
	for _, block := range blocks {
		if block.Type == "text" {
			total += len([]rune(block.Text))
		}
	}
	if total <= config.ThresholdChars {
		return nil, false
	}
	removedStart := config.HeadChars
	removedEnd := total - config.TailChars
	result := make([]ContentBlock, 0, len(blocks)+1)
	consumed := 0
	markerInserted := false
	for _, block := range blocks {
		if block.Type != "text" {
			result = append(result, block)
			continue
		}
		points := []rune(block.Text)
		blockStart, blockEnd := consumed, consumed+len(points)
		headEnd := min(len(points), max(0, removedStart-blockStart))
		tailStart := min(len(points), max(0, removedEnd-blockStart))
		text := string(points[:headEnd])
		if blockStart < removedEnd && blockEnd > removedStart && !markerInserted {
			text += toolResultPruneMarker
			markerInserted = true
		}
		text += string(points[tailStart:])
		if text != "" {
			block.Text = text
			result = append(result, block)
		}
		consumed = blockEnd
	}
	return result, markerInserted
}
