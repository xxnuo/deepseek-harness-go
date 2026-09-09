package harness

import "encoding/json"

func piAIReplayVersion(value any) int {
	switch number := value.(type) {
	case int:
		return number
	case int64:
		return int(number)
	case float64:
		return int(number)
	case json.Number:
		parsed, _ := number.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func piAIReplayBlockType(block ContentBlock) string {
	switch block.Type {
	case "text":
		return "text"
	case "reasoning":
		return "reasoning"
	case "tool-call":
		return "tool-call"
	default:
		return ""
	}
}

func applyPiAIReplayState(message *ChatMessage, blocks []ContentBlock) {
	raw, present := message.Source["replayState"]
	message.ReplayStatePresent = present
	if !present {
		return
	}
	envelope, ok := raw.(map[string]any)
	if !ok {
		message.ReasoningSignature = ""
		return
	}
	response, ok := envelope["response"].(map[string]any)
	if !ok || stringSetting(response["kind"]) != "pi-ai" || piAIReplayVersion(response["version"]) != 2 ||
		stringSetting(response["provider"]) != stringSetting(message.Source["provider"]) ||
		stringSetting(response["model"]) != stringSetting(message.Source["model"]) {
		message.ReasoningSignature = ""
		return
	}
	replayBlocks, ok := envelope["blocks"].([]any)
	if !ok || len(replayBlocks) != len(blocks) {
		message.ReasoningSignature = ""
		return
	}
	for index, block := range blocks {
		replay, ok := replayBlocks[index].(map[string]any)
		if !ok || stringSetting(replay["type"]) != piAIReplayBlockType(block) {
			message.ReasoningSignature = ""
			return
		}
	}
	api := stringSetting(response["api"])
	provider := stringSetting(response["provider"])
	model := stringSetting(response["model"])
	if api == "" || provider == "" || model == "" {
		message.ReasoningSignature = ""
		return
	}
	if api == "anthropic-messages" && stringSetting(response["responseModel"]) != "" {
		model = stringSetting(response["responseModel"])
	}
	message.ReplayValid = true
	message.NativeAPI = api
	message.NativeProvider = provider
	message.NativeModel = model
	if rawLevel, exists := response["providerThinkingLevel"]; exists {
		level, ok := rawLevel.(string)
		if !ok {
			message.ReplayValid = false
			message.ReasoningSignature = ""
			return
		}
		message.ProviderThinkingLevel = &level
	}
}

func piAIReplayState(selection ModelSelection, completion Completion, content []ContentBlock, finish string) map[string]any {
	if completion.ReplayAPI == "" {
		return nil
	}
	response := map[string]any{
		"kind": "pi-ai", "version": 2, "api": completion.ReplayAPI,
		"provider": selection.Provider, "model": selection.Model, "stopReason": piAIReplayStopReason(finish),
	}
	if completion.ResponseModel != "" && completion.ResponseModel != selection.Model {
		response["responseModel"] = completion.ResponseModel
	}
	if completion.ResponseID != "" {
		response["responseId"] = completion.ResponseID
	}
	if completion.ProviderThinkingLevel != nil {
		response["providerThinkingLevel"] = *completion.ProviderThinkingLevel
	}
	blocks := make([]any, 0, len(content))
	for _, block := range content {
		entry := map[string]any{"type": piAIReplayBlockType(block)}
		if block.Type == "reasoning" && block.Signature != "" {
			entry["thinkingSignature"] = block.Signature
		}
		blocks = append(blocks, entry)
	}
	return map[string]any{"response": response, "blocks": blocks}
}

func piAIReplayStopReason(finish string) string {
	switch finish {
	case "tool_calls":
		return "toolUse"
	case "length":
		return "length"
	default:
		return "stop"
	}
}
