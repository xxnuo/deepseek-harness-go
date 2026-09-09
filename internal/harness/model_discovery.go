package harness

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
)

func parseDiscoveredModels(body []byte) ([]ModelInfo, error) {
	var listing map[string]json.RawMessage
	if err := json.Unmarshal(body, &listing); err != nil {
		return nil, err
	}
	var rows []json.RawMessage
	keys := []string{}
	if data := bytes.TrimSpace(listing["data"]); len(data) != 0 && data[0] == '[' {
		if err := json.Unmarshal(data, &rows); err != nil {
			return nil, err
		}
	} else {
		models := bytes.TrimSpace(listing["models"])
		if len(models) == 0 || models[0] != '{' {
			return nil, errors.New("the endpoint's model listing has neither a data array nor a models object; enter this provider's models by hand")
		}
		decoder := json.NewDecoder(bytes.NewReader(models))
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			var row json.RawMessage
			if err := decoder.Decode(&row); err != nil {
				return nil, err
			}
			keys = append(keys, key.(string))
			rows = append(rows, row)
		}
	}
	models := make([]ModelInfo, 0, len(rows))
	for index, raw := range rows {
		var row map[string]any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		if err := decoder.Decode(&row); err != nil || row == nil {
			continue
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			continue
		}
		key := ""
		if len(keys) != 0 {
			key = keys[index]
		}
		id := discoveryLabel(key, row["id"])
		if id == "" {
			continue
		}
		limit, _ := row["limit"].(map[string]any)
		top, _ := row["top_provider"].(map[string]any)
		models = append(models, ModelInfo{
			ID: id, Name: discoveryLabel(row["name"], row["display_name"], row["displayName"], id),
			ContextWindow: discoveryCapacity(row["contextWindow"], row["context_window"], row["context_length"], row["max_input_tokens"], limit["context"]),
			MaxTokens:     discoveryCapacity(row["maxOutputTokens"], row["max_output_tokens"], row["maxTokens"], row["max_tokens"], limit["output"], top["max_completion_tokens"]),
		})
	}
	return models, nil
}

func discoveryLabel(values ...any) string {
	for _, value := range values {
		if text, ok := value.(string); ok && text != "" {
			return text
		}
	}
	return ""
}

func discoveryCapacity(values ...any) int {
	for _, value := range values {
		if number, ok := value.(float64); ok && number > 0 && number < float64(math.MaxInt) && math.Trunc(number) == number {
			return int(number)
		}
	}
	return 0
}
