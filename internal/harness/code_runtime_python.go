package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// PythonProtocolFD is the child-side descriptor used by the versionless
// JSON-lines protocol owned by dsh-code-runtime-python.
const PythonProtocolFD = 3

type PythonErrorClass struct {
	Name               string `json:"name"`
	MemberNameProperty string `json:"memberNameProperty"`
}

type PythonNamespace struct {
	Global     string            `json:"global"`
	Names      []string          `json:"names"`
	ErrorClass *PythonErrorClass `json:"errorClass,omitempty"`
}

type PythonBootMessage struct {
	Type              string            `json:"type"`
	CPUSeconds        int               `json:"cpuSeconds"`
	AddressSpaceBytes int64             `json:"addressSpaceBytes"`
	MaxLogBytes       int               `json:"maxLogBytes"`
	MaxValueBytes     int               `json:"maxValueBytes"`
	Namespaces        []PythonNamespace `json:"namespaces"`
}

type PythonRunMessage struct {
	Type    string `json:"type"`
	Program string `json:"program"`
}

type PythonReplyOK struct {
	Type  string  `json:"type"`
	ID    float64 `json:"id"`
	OK    bool    `json:"ok"`
	Value any     `json:"value"`
}

type PythonReplyError struct {
	Type    string  `json:"type"`
	ID      float64 `json:"id"`
	OK      bool    `json:"ok"`
	Message string  `json:"message"`
}

type PythonDoneError struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

// PythonChildFrame is a rebuilt, validated child-to-host frame. HasValue
// distinguishes an absent done value from an explicit JSON null.
type PythonChildFrame struct {
	Type      string
	ID        float64
	Global    string
	Name      string
	Args      any
	Text      string
	Truncated bool
	Value     any
	HasValue  bool
	Error     *PythonDoneError
}

type PythonWireFieldSet struct {
	Required []string `json:"required"`
	Optional []string `json:"optional"`
}

// PythonUTF16String carries JavaScript string code units that a Go string
// cannot represent losslessly, including lone surrogates.
type PythonUTF16String []uint16

type PythonJSONMember struct {
	Key      string
	UTF16Key PythonUTF16String
	Value    any
}

// PythonJSONObject preserves property insertion order. Array-index keys are
// emitted first in numeric order, matching JavaScript Object.keys semantics.
type PythonJSONObject []PythonJSONMember

// PythonWireFrameFields mirrors the public TypedDict roster in protocol.py.
var PythonWireFrameFields = map[string]PythonWireFieldSet{
	"BootMessage":    {Required: []string{"addressSpaceBytes", "cpuSeconds", "maxLogBytes", "maxValueBytes", "namespaces", "type"}, Optional: []string{}},
	"Namespace":      {Required: []string{"global", "names"}, Optional: []string{"errorClass"}},
	"RunMessage":     {Required: []string{"program", "type"}, Optional: []string{}},
	"BootAckMessage": {Required: []string{"type"}, Optional: []string{}},
	"CallMessage":    {Required: []string{"args", "global", "id", "name", "type"}, Optional: []string{}},
	"LogMessage":     {Required: []string{"text", "type"}, Optional: []string{"truncated"}},
	"DoneErrorField": {Required: []string{"kind", "message"}, Optional: []string{}},
	"DoneMessage":    {Required: []string{"type"}, Optional: []string{"error", "value"}},
	"ErrorClass":     {Required: []string{"memberNameProperty", "name"}, Optional: []string{}},
	"ReplyOk":        {Required: []string{"id", "ok", "type", "value"}, Optional: []string{}},
	"ReplyErr":       {Required: []string{"id", "message", "ok", "type"}, Optional: []string{}},
}

func PythonLogTruncationMarker(maxBytes int) string {
	return fmt.Sprintf("[dsh-code-runtime-python] log capture truncated at %d bytes", maxBytes)
}

type pythonJSONTask struct {
	text   string
	value  any
	isText bool
}

// EncodePythonJSONPlain serializes a JSON-plain value iteratively, preserving
// exact integral float64 digits beyond JavaScript's safe-integer range.
func EncodePythonJSONPlain(value any) (string, error) {
	var output strings.Builder
	tasks := []pythonJSONTask{{value: value}}
	for len(tasks) > 0 {
		task := tasks[len(tasks)-1]
		tasks = tasks[:len(tasks)-1]
		if task.isText {
			output.WriteString(task.text)
			continue
		}
		switch current := task.value.(type) {
		case nil, bool, float64, float32, json.Number,
			int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			text, _, err := pythonJSONScalar(current)
			if err != nil {
				return "", err
			}
			output.WriteString(text)
		case string:
			text, err := pythonJSONString(current)
			if err != nil {
				return "", err
			}
			output.WriteString(text)
		case PythonUTF16String:
			output.WriteString(pythonUTF16JSONString(current))
		case []any:
			output.WriteByte('[')
			tasks = append(tasks, pythonJSONTask{text: "]", isText: true})
			for index := len(current) - 1; index >= 0; index-- {
				if index < len(current)-1 {
					tasks = append(tasks, pythonJSONTask{text: ",", isText: true})
				}
				tasks = append(tasks, pythonJSONTask{value: current[index]})
			}
		case PythonJSONObject:
			members, err := orderedPythonJSONMembers(current)
			if err != nil {
				return "", err
			}
			output.WriteByte('{')
			tasks = append(tasks, pythonJSONTask{text: "}", isText: true})
			for index := len(members) - 1; index >= 0; index-- {
				member := members[index]
				if index < len(members)-1 {
					tasks = append(tasks, pythonJSONTask{text: ",", isText: true})
				}
				key, _, _, err := pythonJSONMemberKey(member)
				if err != nil {
					return "", err
				}
				tasks = append(tasks, pythonJSONTask{value: member.Value})
				tasks = append(tasks, pythonJSONTask{text: key + ":", isText: true})
			}
		case map[string]any:
			output.WriteByte('{')
			tasks = append(tasks, pythonJSONTask{text: "}", isText: true})
			keys := make([]string, 0, len(current))
			for key := range current {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for index := len(keys) - 1; index >= 0; index-- {
				key := keys[index]
				if index < len(keys)-1 {
					tasks = append(tasks, pythonJSONTask{text: ",", isText: true})
				}
				encodedKey, err := pythonJSONString(key)
				if err != nil {
					return "", err
				}
				tasks = append(tasks, pythonJSONTask{value: current[key]})
				tasks = append(tasks, pythonJSONTask{text: encodedKey + ":", isText: true})
			}
		default:
			return "", fmt.Errorf("dsh-code-runtime-python: value contains unsupported JSON type %T", current)
		}
	}
	return output.String(), nil
}

type pythonJSONIndexedMember struct {
	member PythonJSONMember
	index  uint32
}

func orderedPythonJSONMembers(input PythonJSONObject) (PythonJSONObject, error) {
	indexed := make([]pythonJSONIndexedMember, 0, len(input))
	ordinary := make(PythonJSONObject, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, member := range input {
		encodedKey, index, isIndex, err := pythonJSONMemberKey(member)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[encodedKey]; exists {
			return nil, fmt.Errorf("dsh-code-runtime-python: duplicate object key %s", encodedKey)
		}
		seen[encodedKey] = struct{}{}
		if isIndex {
			indexed = append(indexed, pythonJSONIndexedMember{member: member, index: index})
		} else {
			ordinary = append(ordinary, member)
		}
	}
	sort.Slice(indexed, func(i, j int) bool { return indexed[i].index < indexed[j].index })
	result := make(PythonJSONObject, 0, len(input))
	for _, row := range indexed {
		result = append(result, row.member)
	}
	return append(result, ordinary...), nil
}

func pythonJSONMemberKey(member PythonJSONMember) (string, uint32, bool, error) {
	if member.UTF16Key != nil {
		if member.Key != "" {
			return "", 0, false, errors.New("dsh-code-runtime-python: object member cannot set both Key and UTF16Key")
		}
		encoded := pythonUTF16JSONString(member.UTF16Key)
		index, ok := pythonUTF16ArrayIndex(member.UTF16Key)
		return encoded, index, ok, nil
	}
	encoded, err := pythonJSONString(member.Key)
	if err != nil {
		return "", 0, false, err
	}
	index, ok := pythonJSONArrayIndex(member.Key)
	return encoded, index, ok, nil
}

func pythonJSONMemberKeyBytesUpTo(member PythonJSONMember, maxBytes int) (int, bool, error) {
	if member.UTF16Key != nil {
		if member.Key != "" {
			return 0, false, errors.New("dsh-code-runtime-python: object member cannot set both Key and UTF16Key")
		}
		bytes, ok := pythonUTF16JSONStringBytesUpTo(member.UTF16Key, maxBytes)
		return bytes, ok, nil
	}
	return pythonJSONStringBytesUpTo(member.Key, maxBytes)
}

func pythonJSONArrayIndex(key string) (uint32, bool) {
	if key == "0" {
		return 0, true
	}
	if key == "" || key[0] == '0' {
		return 0, false
	}
	value, err := strconv.ParseUint(key, 10, 32)
	if err != nil || value == math.MaxUint32 || strconv.FormatUint(value, 10) != key {
		return 0, false
	}
	return uint32(value), true
}

func pythonUTF16ArrayIndex(key PythonUTF16String) (uint32, bool) {
	if len(key) == 1 && key[0] == '0' {
		return 0, true
	}
	if len(key) == 0 || len(key) > 10 || key[0] == '0' {
		return 0, false
	}
	var value uint64
	for _, unit := range key {
		if unit < '0' || unit > '9' {
			return 0, false
		}
		value = value*10 + uint64(unit-'0')
	}
	if value >= math.MaxUint32 {
		return 0, false
	}
	return uint32(value), true
}

type PythonDoneValueCheck struct {
	OK     bool
	Bytes  int
	Reason string
}

// CheckPythonDoneValue meters encoded bytes and rejects non-lossless numbers.
// An over-budget value always reports over-budget, even if it is also lossy.
func CheckPythonDoneValue(value any, maxBytes int) (PythonDoneValueCheck, error) {
	bytes := 0
	nonLossless := false
	stack := []any{value}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch typed := current.(type) {
		case string:
			encoded, ok, err := pythonJSONStringBytesUpTo(typed, maxBytes-bytes)
			if err != nil {
				return PythonDoneValueCheck{}, err
			}
			if !ok {
				return PythonDoneValueCheck{Reason: "over-budget"}, nil
			}
			bytes += encoded
		case PythonUTF16String:
			encoded, ok := pythonUTF16JSONStringBytesUpTo(typed, maxBytes-bytes)
			if !ok {
				return PythonDoneValueCheck{Reason: "over-budget"}, nil
			}
			bytes += encoded
		case []any:
			bytes += 2
			if len(typed) > 1 {
				bytes += len(typed) - 1
			}
			if bytes+len(typed) > maxBytes {
				return PythonDoneValueCheck{Reason: "over-budget"}, nil
			}
			stack = append(stack, typed...)
		case PythonJSONObject:
			count := len(typed)
			bytes += 2
			if count > 1 {
				bytes += count - 1
			}
			if bytes+count*4 > maxBytes {
				return PythonDoneValueCheck{Reason: "over-budget"}, nil
			}
			seen := make(map[string]struct{}, count)
			for _, member := range typed {
				encoded, ok, err := pythonJSONMemberKeyBytesUpTo(member, maxBytes-bytes)
				if err != nil {
					return PythonDoneValueCheck{}, err
				}
				if !ok {
					return PythonDoneValueCheck{Reason: "over-budget"}, nil
				}
				bytes += encoded + 1
				identity, _, _, err := pythonJSONMemberKey(member)
				if err != nil {
					return PythonDoneValueCheck{}, err
				}
				if _, exists := seen[identity]; exists {
					return PythonDoneValueCheck{}, fmt.Errorf("dsh-code-runtime-python: duplicate object key %s", identity)
				}
				seen[identity] = struct{}{}
				stack = append(stack, member.Value)
			}
		case map[string]any:
			count := len(typed)
			bytes += 2
			if count > 1 {
				bytes += count - 1
			}
			if bytes+count*4 > maxBytes {
				return PythonDoneValueCheck{Reason: "over-budget"}, nil
			}
			for key, item := range typed {
				encoded, ok, err := pythonJSONStringBytesUpTo(key, maxBytes-bytes)
				if err != nil {
					return PythonDoneValueCheck{}, err
				}
				if !ok {
					return PythonDoneValueCheck{Reason: "over-budget"}, nil
				}
				bytes += encoded + 1
				stack = append(stack, item)
			}
		case nil, bool, float64, float32, json.Number,
			int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			text, lossy, err := pythonJSONScalar(typed)
			if err != nil {
				return PythonDoneValueCheck{}, err
			}
			bytes += len(text)
			nonLossless = nonLossless || lossy
		default:
			return PythonDoneValueCheck{}, fmt.Errorf("dsh-code-runtime-python: value contains unsupported JSON type %T", typed)
		}
		if bytes > maxBytes {
			return PythonDoneValueCheck{Reason: "over-budget"}, nil
		}
	}
	if nonLossless {
		return PythonDoneValueCheck{Reason: "non-lossless"}, nil
	}
	return PythonDoneValueCheck{OK: true, Bytes: bytes}, nil
}

// HasUnsafeJSONIntegerToken reports an integer token that a JavaScript
// float64 parse would round. Number-like text inside JSON strings is skipped.
func HasUnsafeJSONIntegerToken(line string) bool {
	for index := 0; index < len(line); index++ {
		char := line[index]
		if char == '"' {
			for index++; index < len(line); index++ {
				if line[index] == '\\' {
					index++
				} else if index < len(line) && line[index] == '"' {
					break
				}
			}
			continue
		}
		if char != '-' && (char < '0' || char > '9') {
			continue
		}
		end := index + 1
		for end < len(line) {
			c := line[end]
			if c >= '0' && c <= '9' || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-' {
				end++
				continue
			}
			break
		}
		token := line[index:end]
		if isPlainIntegerToken(token) && integerTokenRounds(token) {
			return true
		}
		index = end - 1
	}
	return false
}

func isPlainIntegerToken(token string) bool {
	if token == "" {
		return false
	}
	if token[0] == '-' {
		token = token[1:]
	}
	if token == "" {
		return false
	}
	for index := range len(token) {
		if token[index] < '0' || token[index] > '9' {
			return false
		}
	}
	return true
}

func integerTokenRounds(token string) bool {
	parsed, err := strconv.ParseFloat(token, 64)
	if err != nil || math.IsInf(parsed, 0) {
		return true
	}
	if math.Abs(parsed) <= float64(maxSafeInteger) {
		return false
	}
	exact, ok := new(big.Int).SetString(token, 10)
	if !ok {
		return true
	}
	roundTrip, accuracy := new(big.Float).SetFloat64(parsed).Int(nil)
	return accuracy != big.Exact || exact.Cmp(roundTrip) != 0
}

type pythonJSONCursorKind uint8

const (
	pythonJSONArrayCursor pythonJSONCursorKind = iota
	pythonJSONObjectCursor
	pythonJSONMapCursor
)

type pythonJSONCursor struct {
	kind    pythonJSONCursorKind
	index   int
	array   []any
	members PythonJSONObject
	object  *reflect.MapIter
}

func (cursor *pythonJSONCursor) next() (any, bool) {
	switch cursor.kind {
	case pythonJSONArrayCursor:
		if cursor.index >= len(cursor.array) {
			return nil, false
		}
		value := cursor.array[cursor.index]
		cursor.index++
		return value, true
	case pythonJSONObjectCursor:
		if cursor.index >= len(cursor.members) {
			return nil, false
		}
		value := cursor.members[cursor.index].Value
		cursor.index++
		return value, true
	case pythonJSONMapCursor:
		if !cursor.object.Next() {
			return nil, false
		}
		return cursor.object.Value().Interface(), true
	default:
		return nil, false
	}
}

// HasNonLosslessJSONNumber scans for non-finite values or negative zero while
// retaining one cursor per nesting level rather than one stack item per value.
func HasNonLosslessJSONNumber(value any) bool {
	cursors := []pythonJSONCursor{{kind: pythonJSONArrayCursor, array: []any{value}}}
	for len(cursors) > 0 {
		cursor := &cursors[len(cursors)-1]
		current, ok := cursor.next()
		if !ok {
			cursors = cursors[:len(cursors)-1]
			continue
		}
		switch typed := current.(type) {
		case float64:
			if !isLosslessPythonFloat(typed) {
				return true
			}
		case float32:
			if !isLosslessPythonFloat(float64(typed)) {
				return true
			}
		case json.Number:
			parsed, err := strconv.ParseFloat(typed.String(), 64)
			if err != nil || !isLosslessPythonFloat(parsed) {
				return true
			}
		case []any:
			cursors = append(cursors, pythonJSONCursor{kind: pythonJSONArrayCursor, array: typed})
		case PythonJSONObject:
			cursors = append(cursors, pythonJSONCursor{kind: pythonJSONObjectCursor, members: typed})
		case map[string]any:
			cursors = append(cursors, pythonJSONCursor{kind: pythonJSONMapCursor, object: reflect.ValueOf(typed).MapRange()})
		}
	}
	return false
}

func isLosslessPythonFloat(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && !(value == 0 && math.Signbit(value))
}

// ValidatePythonChildFrame shape-validates and rebuilds hostile fd-3 traffic.
func ValidatePythonChildFrame(raw any) (PythonChildFrame, bool) {
	message, ok := raw.(map[string]any)
	if !ok {
		return PythonChildFrame{}, false
	}
	typeName, _ := message["type"].(string)
	switch typeName {
	case "boot-ack":
		return PythonChildFrame{Type: typeName}, true
	case "log":
		text, ok := message["text"].(string)
		if !ok {
			return PythonChildFrame{}, false
		}
		truncated, _ := message["truncated"].(bool)
		return PythonChildFrame{Type: typeName, Text: text, Truncated: truncated}, true
	case "call":
		id, ok := pythonFrameNumber(message["id"])
		global, globalOK := message["global"].(string)
		name, nameOK := message["name"].(string)
		args, argsOK := message["args"]
		if !ok || !globalOK || !nameOK || !argsOK || HasNonLosslessJSONNumber(args) {
			return PythonChildFrame{}, false
		}
		return PythonChildFrame{Type: typeName, ID: id, Global: global, Name: name, Args: args}, true
	case "done":
		value, hasValue := message["value"]
		errorValue, hasError := message["error"]
		frame := PythonChildFrame{Type: typeName, Value: value, HasValue: hasValue}
		if !hasError {
			return frame, true
		}
		errorObject, ok := errorValue.(map[string]any)
		if !ok {
			return PythonChildFrame{}, false
		}
		kind, kindOK := errorObject["kind"].(string)
		message, messageOK := errorObject["message"].(string)
		if !kindOK || !messageOK || kind != "exception" && kind != "invalid-output" && kind != "output-limit" {
			return PythonChildFrame{}, false
		}
		frame.Error = &PythonDoneError{Kind: kind, Message: message}
		return frame, true
	default:
		return PythonChildFrame{}, false
	}
}

func pythonFrameNumber(value any) (float64, bool) {
	var number float64
	switch typed := value.(type) {
	case float64:
		number = typed
	case float32:
		number = float64(typed)
	case json.Number:
		parsed, err := strconv.ParseFloat(typed.String(), 64)
		if err != nil {
			return 0, false
		}
		number = parsed
	case int:
		number = float64(typed)
	case int64:
		number = float64(typed)
	default:
		return 0, false
	}
	return number, isLosslessPythonFloat(number)
}

func pythonJSONScalar(value any) (string, bool, error) {
	switch typed := value.(type) {
	case nil:
		return "null", false, nil
	case bool:
		return strconv.FormatBool(typed), false, nil
	case float64:
		return pythonJSONFloat(typed)
	case float32:
		return pythonJSONFloat(float64(typed))
	case json.Number:
		if !validJSONNumber(typed.String()) {
			return "", false, fmt.Errorf("dsh-code-runtime-python: invalid JSON number %q", typed)
		}
		parsed, err := strconv.ParseFloat(typed.String(), 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return "", false, fmt.Errorf("dsh-code-runtime-python: invalid JSON number %q: %w", typed, err)
		}
		return pythonJSONFloat(parsed)
	case int:
		return strconv.Itoa(typed), false, nil
	case int8:
		return strconv.FormatInt(int64(typed), 10), false, nil
	case int16:
		return strconv.FormatInt(int64(typed), 10), false, nil
	case int32:
		return strconv.FormatInt(int64(typed), 10), false, nil
	case int64:
		return strconv.FormatInt(typed, 10), false, nil
	case uint:
		return strconv.FormatUint(uint64(typed), 10), false, nil
	case uint8:
		return strconv.FormatUint(uint64(typed), 10), false, nil
	case uint16:
		return strconv.FormatUint(uint64(typed), 10), false, nil
	case uint32:
		return strconv.FormatUint(uint64(typed), 10), false, nil
	case uint64:
		return strconv.FormatUint(typed, 10), false, nil
	default:
		return "", false, errors.New("dsh-code-runtime-python: scalar is not JSON-plain")
	}
}

func pythonJSONFloat(value float64) (string, bool, error) {
	nonLossless := !isLosslessPythonFloat(value)
	switch {
	case math.IsNaN(value):
		return "NaN", true, nil
	case math.IsInf(value, 1):
		return "Infinity", true, nil
	case math.IsInf(value, -1):
		return "-Infinity", true, nil
	case value == 0 && math.Signbit(value):
		return "0", true, nil
	case math.Trunc(value) == value && math.Abs(value) > float64(maxSafeInteger):
		return strconv.FormatFloat(value, 'f', 0, 64), nonLossless, nil
	default:
		encoded, err := json.Marshal(value)
		return string(encoded), nonLossless, err
	}
}

func validJSONNumber(value string) bool {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return false
	}
	_, ok := decoded.(json.Number)
	return ok
}

func pythonJSONString(value string) (string, error) {
	if !utf8.ValidString(value) {
		return "", errors.New("dsh-code-runtime-python: string is not valid UTF-8; use PythonUTF16String for lone surrogates")
	}
	var output strings.Builder
	output.Grow(len(value) + 2)
	output.WriteByte('"')
	for _, character := range value {
		switch character {
		case '"', '\\':
			output.WriteByte('\\')
			output.WriteRune(character)
		case '\b':
			output.WriteString(`\b`)
		case '\t':
			output.WriteString(`\t`)
		case '\n':
			output.WriteString(`\n`)
		case '\f':
			output.WriteString(`\f`)
		case '\r':
			output.WriteString(`\r`)
		default:
			if character < 0x20 {
				fmt.Fprintf(&output, `\u%04x`, character)
			} else {
				output.WriteRune(character)
			}
		}
	}
	output.WriteByte('"')
	return output.String(), nil
}

func pythonJSONStringBytesUpTo(value string, maxBytes int) (int, bool, error) {
	if !utf8.ValidString(value) {
		return 0, false, errors.New("dsh-code-runtime-python: string is not valid UTF-8; use PythonUTF16String for lone surrogates")
	}
	bytes := 2
	if bytes > maxBytes {
		return 0, false, nil
	}
	for _, character := range value {
		switch character {
		case '"', '\\', '\b', '\t', '\n', '\f', '\r':
			bytes += 2
		default:
			if character < 0x20 {
				bytes += 6
			} else {
				bytes += utf8.RuneLen(character)
			}
		}
		if bytes > maxBytes {
			return 0, false, nil
		}
	}
	return bytes, true, nil
}

func pythonUTF16JSONString(value PythonUTF16String) string {
	var output strings.Builder
	output.WriteByte('"')
	for index := 0; index < len(value); index++ {
		unit := value[index]
		switch unit {
		case '"', '\\':
			output.WriteByte('\\')
			output.WriteByte(byte(unit))
		case '\b':
			output.WriteString(`\b`)
		case '\t':
			output.WriteString(`\t`)
		case '\n':
			output.WriteString(`\n`)
		case '\f':
			output.WriteString(`\f`)
		case '\r':
			output.WriteString(`\r`)
		default:
			switch {
			case unit < 0x20:
				fmt.Fprintf(&output, `\u%04x`, unit)
			case unit >= 0xd800 && unit <= 0xdbff && index+1 < len(value) && value[index+1] >= 0xdc00 && value[index+1] <= 0xdfff:
				output.WriteRune(utf16.DecodeRune(rune(unit), rune(value[index+1])))
				index++
			case unit >= 0xd800 && unit <= 0xdfff:
				fmt.Fprintf(&output, `\u%04x`, unit)
			default:
				output.WriteRune(rune(unit))
			}
		}
	}
	output.WriteByte('"')
	return output.String()
}

func pythonUTF16JSONStringBytesUpTo(value PythonUTF16String, maxBytes int) (int, bool) {
	bytes := 2
	if bytes > maxBytes {
		return 0, false
	}
	for index := 0; index < len(value); index++ {
		unit := value[index]
		switch {
		case unit == '"' || unit == '\\' || unit == '\b' || unit == '\t' || unit == '\n' || unit == '\f' || unit == '\r':
			bytes += 2
		case unit < 0x20:
			bytes += 6
		case unit < 0x80:
			bytes++
		case unit < 0x800:
			bytes += 2
		case unit >= 0xd800 && unit <= 0xdbff && index+1 < len(value) && value[index+1] >= 0xdc00 && value[index+1] <= 0xdfff:
			bytes += 4
			index++
		case unit >= 0xd800 && unit <= 0xdfff:
			bytes += 6
		default:
			bytes += 3
		}
		if bytes > maxBytes {
			return 0, false
		}
	}
	return bytes, true
}
