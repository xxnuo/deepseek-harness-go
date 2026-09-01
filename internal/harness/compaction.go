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
	config          CompactionConfig
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
	runtimeConfig, runtimeErr := e.runtimeForSession(s)
	if runtimeErr != nil {
		return result, runtimeErr
	}
	if !runtimeConfig.compactionEnabled {
		return result, errors.New("compaction service unavailable")
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
	selection := request.selection
	if selection.Provider == "" {
		selection.Provider = e.cfg.Provider
	}
	if selection.Model == "" {
		selection.Model = e.cfg.Model
	}
	config := request.config
	if config.ThresholdRatio == 0 {
		config = compactionPolicyFor(runtimeConfig.compactionConfig, selection)
	}
	summarySelection := selection
	if config.SummarizationProvider != "" {
		summarySelection.Provider = config.SummarizationProvider
		summarySelection.Model = config.SummarizationModel
	}
	messages := e.hydrateChatMessagesWithLimit(transcriptMessages(selected, int(^uint(0)>>1)), e.requestImageLimit(selection.Provider))
	if len(messages) == 0 || !toolMessagesBalanced(messages) {
		return result, errNoCompactableHistory
	}
	e.mu.RLock()
	provider := e.providers[summarySelection.Provider]
	e.mu.RUnlock()
	if provider == nil {
		return result, fmt.Errorf("compaction model unavailable: %s/%s", summarySelection.Provider, summarySelection.Model)
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
		SessionID: s.Header.ID, Model: summarySelection.Model, System: request.system, Messages: requestMessages, Tools: request.tools,
		Purpose: "compaction", MaxTokens: config.MaxTokens,
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
	summaryData["provider"] = summarySelection.Provider
	summaryData["model"] = summarySelection.Model
	summaryData["maxTokens"] = config.MaxTokens
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
	provider := s.Model.Provider
	s.mu.Unlock()
	return e.hydrateChatMessagesWithLimit(transcriptMessages(events, turn), e.requestImageLimit(provider))
}

func routedCompactionRequest(s *Session) (ModelSelection, string, []ToolSchema, bool) {
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type != "request/header" {
			continue
		}
		var data struct {
			Header struct {
				Config ModelSelection `json:"config"`
				System string         `json:"system"`
				Tools  []ToolSchema   `json:"tools"`
			} `json:"header"`
		}
		encoded, err := json.Marshal(events[index].Data)
		if err == nil && json.Unmarshal(encoded, &data) == nil && data.Header.Config.Provider != "" && data.Header.Config.Model != "" {
			return data.Header.Config, data.Header.System, data.Header.Tools, true
		}
	}
	return ModelSelection{}, "", nil, false
}

func (e *Engine) contextWindowFor(ctx context.Context, selection ModelSelection) (int, error) {
	if selection.Provider == "" {
		selection.Provider = e.cfg.Provider
	}
	if selection.Model == "" {
		selection.Model = e.cfg.Model
	}
	model, err := resolveExactModelInfo(ctx, e, selection)
	if err != nil {
		return 0, err
	}
	return model.ContextWindow, nil
}

func (e *Engine) compactForPressure(ctx context.Context, s *Session, turn int, selection ModelSelection, system string, tools []ToolSchema) (bool, error) {
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		return false, err
	}
	if !runtimeConfig.compactionEnabled || !runtimeConfig.compactionAuto {
		return false, nil
	}
	routed, routedSystem, routedTools, ok := routedCompactionRequest(s)
	if !ok {
		return false, nil
	}
	selection = routed
	system, tools = routedSystem, routedTools
	config := compactionPolicyFor(runtimeConfig.compactionConfig, selection)
	contextWindow, err := e.contextWindowFor(ctx, selection)
	if err != nil {
		return false, err
	}
	if contextWindow == 0 {
		return false, fmt.Errorf("compaction-basic: no context capacity for %s/%s; configure contextWindow on that adapter model", selection.Provider, selection.Model)
	}
	threshold := int(float64(contextWindow) * config.ThresholdRatio)
	retainTokens := config.RetainTokens
	if retainTokens == 0 {
		retainTokens = int(float64(contextWindow) * config.RetainRatio)
	}
	if retainTokens >= threshold {
		return false, fmt.Errorf("compaction retain tokens %d must be below threshold %d", retainTokens, threshold)
	}
	measurement, err := measureSessionTokens(s)
	if err != nil {
		return false, err
	}
	if measurement.totalTokens < threshold {
		return false, nil
	}
	pruned, err := e.pruneToolResults(s)
	changed := pruned > 0
	if err != nil {
		return changed, err
	}
	measurement, err = measureSessionTokens(s)
	if err != nil {
		return changed, err
	}
	if measurement.totalTokens < threshold {
		return changed, nil
	}
	for attempt := 0; attempt <= config.CompactionRetries; attempt++ {
		_, err = e.compactSession(ctx, s, compactRequest{
			turn: &turn, selection: selection, system: system, tools: tools,
			retainTokens: retainTokens, wholeSurface: true, config: config,
		})
		if errors.Is(err, errNoCompactableHistory) {
			break
		}
		if err != nil {
			return changed, err
		}
		changed = true
		measurement, err = measureSessionTokens(s)
		if err != nil {
			return changed, err
		}
		if measurement.totalTokens < threshold {
			return true, nil
		}
	}
	return changed, fmt.Errorf("compaction still above threshold (%d estimated tokens >= %d)", measurement.totalTokens, threshold)
}

func (e *Engine) compactForOverflow(ctx context.Context, s *Session, turn int, selection ModelSelection, system string, tools []ToolSchema) (bool, error) {
	runtimeConfig, runtimeErr := e.runtimeForSession(s)
	if runtimeErr != nil {
		return false, runtimeErr
	}
	if !runtimeConfig.compactionEnabled || !runtimeConfig.compactionAuto {
		return false, nil
	}
	routed, routedSystem, routedTools, ok := routedCompactionRequest(s)
	if !ok {
		return false, nil
	}
	selection = routed
	system, tools = routedSystem, routedTools
	config := compactionPolicyFor(runtimeConfig.compactionConfig, selection)
	pruned, err := e.pruneToolResults(s)
	changed := pruned > 0
	if err != nil {
		return changed, err
	}
	_, err = e.compactSession(ctx, s, compactRequest{
		turn: &turn, selection: selection, system: system, tools: tools,
		wholeSurface: true, config: config,
	})
	if errors.Is(err, errNoCompactableHistory) {
		return changed, nil
	}
	return changed || err == nil, err
}

func (e *Engine) pruneToolResults(s *Session) (int, error) {
	config := e.cfg.ToolResultPruner
	runtimeConfig, err := e.runtimeForSession(s)
	if err != nil {
		return 0, err
	}
	if !runtimeConfig.toolResultPrunerEnabled {
		return 0, nil
	}
	if runtimeConfig.toolResultPruneThresholdChars > 0 {
		config.ThresholdChars = runtimeConfig.toolResultPruneThresholdChars
	}
	if runtimeConfig.toolResultPruneHeadChars >= 0 {
		config.HeadChars = runtimeConfig.toolResultPruneHeadChars
	}
	if runtimeConfig.toolResultPruneTailChars >= 0 {
		config.TailChars = runtimeConfig.toolResultPruneTailChars
	}
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
		data, ok := cloneJSON(event.Data).(map[string]any)
		if !ok {
			continue
		}
		message := nestedMessage(data)
		outerBlocks, ok := rawContentBlockMaps(message["content"])
		if !ok || len(outerBlocks) != 1 || stringValue(outerBlocks[0]["type"]) != "tool-result" {
			continue
		}
		content, changed, err := pruneRawToolResultContent(outerBlocks[0]["content"], config)
		if err != nil {
			return pruned, err
		}
		if !changed {
			continue
		}
		// Keep the original JSON maps intact and replace only the nested
		// `content` field. The TypeScript plugin spreads every block and the
		// complete event data, so provider-specific fields on core blocks must
		// survive this rewrite too.
		outerBlocks[0]["content"] = content
		outerValues := make([]any, len(outerBlocks))
		for index, block := range outerBlocks {
			outerValues[index] = block
		}
		message["content"] = outerValues
		shadowedTokens := estimateProjectionEvent(event)
		if err := e.appendToolResultPruneReplacement(s, event.Seq, shadowedTokens, data); err != nil {
			return pruned, err
		}
		pruned++
	}
	return pruned, nil
}

// appendToolResultPruneReplacement keeps the shadow-price event and its
// replacement adjacent even when other goroutines append to the session. If
// the replacement is rejected after the price event commits, the committed
// price remains durable, matching the upstream session.append sequence.
func (e *Engine) appendToolResultPruneReplacement(s *Session, originalSeq, shadowedTokens int, data map[string]any) error {
	pruneData := map[string]any{
		"shadowedRange": map[string]any{"start": originalSeq, "end": originalSeq},
		"shadowedSeqs":  []int{originalSeq}, "shadowedTokenCount": shadowedTokens,
	}
	replacementOp := map[string]any{"op": "replace", "start": originalSeq, "end": originalSeq}

	s.mu.Lock()
	id := s.Header.ID
	price, err := appendEventLocked(s, "compaction/prune", pruneData, nil, nil, false)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	replacement, replacementErr := appendEventLocked(s, "tool/result", data, replacementOp, []int{originalSeq}, false)
	s.mu.Unlock()

	e.publishEvent(id, price)
	e.observeSessionTitleEvent(s, price)
	if replacementErr != nil {
		return replacementErr
	}
	e.publishEvent(id, replacement)
	e.observeSessionTitleEvent(s, replacement)
	return nil
}

// rawContentBlockMaps returns the mutable JSON maps produced by cloneJSON.
// Keeping these maps instead of round-tripping through ContentBlock preserves
// extension fields on otherwise core block types.
func rawContentBlockMaps(value any) ([]map[string]any, bool) {
	blocks, ok := value.([]any)
	if !ok {
		return nil, false
	}
	out := make([]map[string]any, len(blocks))
	for index, value := range blocks {
		block, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		out[index] = block
	}
	return out, true
}

// pruneRawToolResultContent mirrors pruneToolResultContent while preserving
// arbitrary fields on every retained content block.
func pruneRawToolResultContent(value any, config ToolResultPruneConfig) ([]any, bool, error) {
	blocks, ok := value.([]any)
	if !ok {
		return nil, false, nil
	}
	total := 0
	for _, value := range blocks {
		block, ok := value.(map[string]any)
		if !ok || stringValue(block["type"]) != "text" {
			continue
		}
		text, _ := block["text"].(string)
		total += len([]rune(text))
	}
	if total <= config.ThresholdChars {
		return nil, false, nil
	}
	removedStart := config.HeadChars
	removedEnd := total - config.TailChars
	result := make([]any, 0, len(blocks)+1)
	consumed := 0
	markerInserted := false
	for _, value := range blocks {
		block, ok := value.(map[string]any)
		if !ok || stringValue(block["type"]) != "text" {
			result = append(result, value)
			continue
		}
		text, _ := block["text"].(string)
		points := []rune(text)
		blockStart, blockEnd := consumed, consumed+len(points)
		headEnd := min(len(points), max(0, removedStart-blockStart))
		tailStart := min(len(points), max(0, removedEnd-blockStart))
		replacement := string(points[:headEnd])
		if blockStart < removedEnd && blockEnd > removedStart && !markerInserted {
			replacement += toolResultPruneMarker
			markerInserted = true
		}
		replacement += string(points[tailStart:])
		if replacement != "" {
			retained := cloneJSON(block).(map[string]any)
			retained["text"] = replacement
			result = append(result, retained)
		}
		consumed = blockEnd
	}
	if !markerInserted {
		return nil, false, errors.New("tool-result prune: failed to locate the removed text span")
	}
	charsAfter := 0
	for _, value := range result {
		block, ok := value.(map[string]any)
		if !ok || stringValue(block["type"]) != "text" {
			continue
		}
		text, _ := block["text"].(string)
		charsAfter += len([]rune(text))
	}
	if charsAfter > config.ThresholdChars || charsAfter >= total {
		return nil, false, errors.New("tool-result prune: replacement must be smaller and within threshold")
	}
	return result, true, nil
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
