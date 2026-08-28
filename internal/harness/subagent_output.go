package harness

import "strings"

func finalAssistantOutput(events []Event) []ContentBlock {
	var selected []ContentBlock
	var partial strings.Builder
	for _, event := range events {
		switch event.Type {
		case "assistant/message":
			blocks := contentBlocks(nestedMessage(event.Data)["content"])
			if len(blocks) > 0 {
				selected = cloneContentBlocks(blocks)
			}
		case "assistant/chunk":
			data, _ := event.Data.(map[string]any)
			chunk, _ := data["chunk"].(map[string]any)
			if chunk["type"] == "text-delta" {
				partial.WriteString(stringValue(chunk["text"]))
			}
		}
	}
	if selected != nil {
		return selected
	}
	if partial.Len() > 0 {
		return []ContentBlock{{Type: "text", Text: partial.String()}}
	}
	return nil
}
