package harness

import "encoding/json"

var coreContentBlockTypes = map[string]bool{
	"text": true, "reasoning": true, "image": true, "tool-call": true, "tool-result": true,
}

type contentBlockJSON ContentBlock

func (b ContentBlock) MarshalJSON() ([]byte, error) {
	if b.Type == "file" && b.FileAttachment != nil {
		return json.Marshal(struct {
			Type       string            `json:"type"`
			Attachment FileAttachmentRef `json:"attachment"`
		}{Type: b.Type, Attachment: *b.FileAttachment})
	}
	if coreContentBlockTypes[b.Type] || b.Extra == nil {
		return json.Marshal(contentBlockJSON(b))
	}
	value := cloneJSON(b.Extra).(map[string]any)
	value["type"] = b.Type
	return json.Marshal(value)
}

func (b *ContentBlock) UnmarshalJSON(data []byte) error {
	var selector struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &selector); err != nil {
		return err
	}
	if selector.Type == "file" {
		var decoded struct {
			Type       string            `json:"type"`
			Attachment FileAttachmentRef `json:"attachment"`
		}
		if err := json.Unmarshal(data, &decoded); err != nil {
			return err
		}
		*b = ContentBlock{Type: decoded.Type, FileAttachment: &decoded.Attachment}
		return nil
	}
	if coreContentBlockTypes[selector.Type] {
		var decoded contentBlockJSON
		if err := json.Unmarshal(data, &decoded); err != nil {
			return err
		}
		*b = ContentBlock(decoded)
		return nil
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	delete(value, "type")
	*b = ContentBlock{Type: selector.Type, Extra: value}
	return nil
}
