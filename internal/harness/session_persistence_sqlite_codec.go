package harness

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

const (
	minSQLitePackedRowMembers = 3
	maxSQLitePackedRowMembers = 1_024
	maxSQLitePackedDataBytes  = 1_048_576
	sqliteZstdThresholdBytes  = 4_096
	sqlitePackedSentinel      = 0
)

var (
	sqliteZstdEncoder = func() *zstd.Encoder {
		encoder, err := zstd.NewWriter(nil,
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)),
		)
		if err != nil {
			panic(err)
		}
		return encoder
	}()
	sqliteZstdDecoder = func() *zstd.Decoder {
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			panic(err)
		}
		return decoder
	}()
	sqlitePackedZstdDecoder = func() *zstd.Decoder {
		decoder, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(maxSQLitePackedDataBytes),
		)
		if err != nil {
			panic(err)
		}
		return decoder
	}()
)

type sqliteEventRow struct {
	seq       int
	typ       string
	time      int64
	data      any
	sources   any
	surface   sql.NullString
	ignorable sql.NullInt64
}

type sqliteEventBinding struct {
	seq       int
	typ       string
	time      int64
	data      any
	sources   any
	surface   any
	ignorable any
}

type sqliteStorageRecord struct {
	event  Event
	packed *sqlitePackedRecord
}

type sqlitePackedRecord struct {
	typ  string
	seq  int
	time int64
	data []byte
}

type sqliteChunkMember struct {
	event   Event
	kind    string
	turn    any
	step    any
	index   any
	turnN   float64
	stepN   float64
	indexN  float64
	text    string
	id      string
	name    string
	hasName bool
}

type sqlitePackedTextData struct {
	Turn  any      `json:"turn"`
	Step  any      `json:"step"`
	Index any      `json:"index"`
	DT    []int64  `json:"dt"`
	Texts []string `json:"texts"`
}

type sqlitePackedToolData struct {
	Turn  any      `json:"turn"`
	Step  any      `json:"step"`
	Index any      `json:"index"`
	DT    []int64  `json:"dt"`
	ID    string   `json:"id"`
	Name  *string  `json:"name,omitempty"`
	Args  []string `json:"args"`
}

func sqliteEventBindingsFor(events []Event) ([]sqliteEventBinding, error) {
	records, err := packSQLiteChunkRuns(events)
	if err != nil {
		return nil, err
	}
	bindings := make([]sqliteEventBinding, 0, len(records))
	for _, record := range records {
		if record.packed != nil {
			bindings = append(bindings, sqliteEventBinding{
				seq: record.packed.seq, typ: record.packed.typ, time: record.packed.time,
				data: encodeSQLiteData(record.packed.data), ignorable: sqlitePackedSentinel,
			})
			continue
		}
		event := record.event
		data, err := marshalSQLiteJSON(event.Data)
		if err != nil {
			return nil, fmt.Errorf("session event %q is not JSON-serializable: %w", event.Type, err)
		}
		var sources, surface, ignorable any
		if event.SourceEventSeqs != nil {
			sources, err = encodeSQLiteSourceSeqs(event.SourceEventSeqs)
			if err != nil {
				return nil, err
			}
		}
		if event.SurfaceOp != nil {
			encoded, err := marshalSQLiteJSON(event.SurfaceOp)
			if err != nil {
				return nil, err
			}
			surface = string(encoded)
		}
		if event.Ignorable {
			ignorable = 1
		}
		bindings = append(bindings, sqliteEventBinding{
			seq: event.Seq, typ: event.Type, time: event.Time, data: encodeSQLiteData(data),
			sources: sources, surface: surface, ignorable: ignorable,
		})
	}
	return bindings, nil
}

func packSQLiteChunkRuns(events []Event) ([]sqliteStorageRecord, error) {
	out := make([]sqliteStorageRecord, 0, len(events))
	var run []sqliteChunkMember
	flush := func() error {
		if len(run) == 0 {
			return nil
		}
		var err error
		out, err = emitSQLiteChunkRun(out, run)
		run = nil
		return err
	}
	for _, event := range events {
		member, ok := classifySQLiteChunk(event)
		if !ok {
			if err := flush(); err != nil {
				return nil, err
			}
			out = append(out, sqliteStorageRecord{event: event})
			continue
		}
		if len(run) > 0 && (!continuesSQLiteChunk(run[len(run)-1], member)) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		run = append(run, member)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

func emitSQLiteChunkRun(out []sqliteStorageRecord, run []sqliteChunkMember) ([]sqliteStorageRecord, error) {
	offset := 0
	for len(run)-offset >= minSQLitePackedRowMembers {
		high := min(len(run)-offset, maxSQLitePackedRowMembers)
		largest, err := buildSQLitePackedRecord(run[offset : offset+high])
		if err != nil {
			return nil, err
		}
		if len(largest.data) <= maxSQLitePackedDataBytes {
			out = append(out, sqliteStorageRecord{packed: &largest})
			offset += high
			continue
		}
		low, accepted := minSQLitePackedRowMembers, 0
		var acceptedRecord sqlitePackedRecord
		for high--; low <= high; {
			middle := (low + high) / 2
			candidate, err := buildSQLitePackedRecord(run[offset : offset+middle])
			if err != nil {
				return nil, err
			}
			if len(candidate.data) <= maxSQLitePackedDataBytes {
				accepted, acceptedRecord, low = middle, candidate, middle+1
			} else {
				high = middle - 1
			}
		}
		if accepted == 0 {
			out = append(out, sqliteStorageRecord{event: run[offset].event})
			offset++
			continue
		}
		out = append(out, sqliteStorageRecord{packed: &acceptedRecord})
		offset += accepted
	}
	for ; offset < len(run); offset++ {
		out = append(out, sqliteStorageRecord{event: run[offset].event})
	}
	return out, nil
}

func buildSQLitePackedRecord(run []sqliteChunkMember) (sqlitePackedRecord, error) {
	first := run[0]
	deltas := make([]int64, len(run)-1)
	for index := 1; index < len(run); index++ {
		deltas[index-1] = run[index].event.Time - run[index-1].event.Time
	}
	var data any
	if first.kind == "tool-call-delta" {
		args := make([]string, len(run))
		for index := range run {
			args[index] = run[index].text
		}
		var name *string
		if first.hasName {
			value := first.name
			name = &value
		}
		data = sqlitePackedToolData{
			Turn: first.turn, Step: first.step, Index: first.index, DT: deltas,
			ID: first.id, Name: name, Args: args,
		}
	} else {
		texts := make([]string, len(run))
		for index := range run {
			texts[index] = run[index].text
		}
		data = sqlitePackedTextData{
			Turn: first.turn, Step: first.step, Index: first.index, DT: deltas, Texts: texts,
		}
	}
	encoded, err := marshalSQLiteJSON(data)
	if err != nil {
		return sqlitePackedRecord{}, err
	}
	typ := "text-chunks"
	if first.kind == "reasoning-delta" {
		typ = "reasoning-chunks"
	} else if first.kind == "tool-call-delta" {
		typ = "tool-call-chunks"
	}
	return sqlitePackedRecord{typ: typ, seq: first.event.Seq, time: first.event.Time, data: encoded}, nil
}

func classifySQLiteChunk(event Event) (sqliteChunkMember, bool) {
	if event.Type != "assistant/chunk" || event.Ignorable || event.SourceEventSeqs != nil || event.SurfaceOp != nil ||
		event.Seq < 0 || int64(event.Seq) > maxJSONSafeInteger ||
		event.Time < -maxJSONSafeInteger || event.Time > maxJSONSafeInteger {
		return sqliteChunkMember{}, false
	}
	data, ok := event.Data.(map[string]any)
	if !ok || !hasExactSQLiteKeys(data, "turn", "step", "chunk") {
		return sqliteChunkMember{}, false
	}
	turnN, ok := sqliteJSONNumber(data["turn"])
	if !ok {
		return sqliteChunkMember{}, false
	}
	stepN, ok := sqliteJSONNumber(data["step"])
	if !ok {
		return sqliteChunkMember{}, false
	}
	chunk, ok := data["chunk"].(map[string]any)
	if !ok {
		return sqliteChunkMember{}, false
	}
	kind, ok := chunk["type"].(string)
	if !ok {
		return sqliteChunkMember{}, false
	}
	indexN, ok := sqliteJSONNumber(chunk["index"])
	if !ok {
		return sqliteChunkMember{}, false
	}
	member := sqliteChunkMember{
		event: event, kind: kind, turn: data["turn"], step: data["step"], index: chunk["index"],
		turnN: turnN, stepN: stepN, indexN: indexN,
	}
	switch kind {
	case "text-delta", "reasoning-delta":
		if !hasExactSQLiteKeys(chunk, "type", "index", "text") {
			return sqliteChunkMember{}, false
		}
		member.text, ok = chunk["text"].(string)
		return member, ok
	case "tool-call-delta":
		member.hasName = len(chunk) == 5
		if !hasExactSQLiteKeys(chunk, "type", "index", "id", "argumentsDelta") &&
			!(member.hasName && hasExactSQLiteKeys(chunk, "type", "index", "id", "name", "argumentsDelta")) {
			return sqliteChunkMember{}, false
		}
		member.id, ok = chunk["id"].(string)
		if !ok {
			return sqliteChunkMember{}, false
		}
		member.text, ok = chunk["argumentsDelta"].(string)
		if !ok {
			return sqliteChunkMember{}, false
		}
		if member.hasName {
			member.name, ok = chunk["name"].(string)
		}
		return member, ok
	default:
		return sqliteChunkMember{}, false
	}
}

func continuesSQLiteChunk(previous, next sqliteChunkMember) bool {
	delta := next.event.Time - previous.event.Time
	if next.kind != previous.kind || next.event.Seq != previous.event.Seq+1 ||
		delta < -maxJSONSafeInteger || delta > maxJSONSafeInteger ||
		next.turnN != previous.turnN || next.stepN != previous.stepN || next.indexN != previous.indexN {
		return false
	}
	return next.kind != "tool-call-delta" ||
		next.id == previous.id && next.hasName == previous.hasName && next.name == previous.name
}

func hasExactSQLiteKeys(value map[string]any, keys ...string) bool {
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

func sqliteJSONNumber(value any) (float64, bool) {
	var number float64
	switch value := value.(type) {
	case float64:
		number = value
	case float32:
		number = float64(value)
	case int:
		number = float64(value)
	case int8:
		number = float64(value)
	case int16:
		number = float64(value)
	case int32:
		number = float64(value)
	case int64:
		number = float64(value)
	case uint:
		number = float64(value)
	case uint8:
		number = float64(value)
	case uint16:
		number = float64(value)
	case uint32:
		number = float64(value)
	case uint64:
		number = float64(value)
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			return 0, false
		}
		number = parsed
	default:
		return 0, false
	}
	return number, !math.IsNaN(number) && !math.IsInf(number, 0)
}

func encodeSQLiteData(serialized []byte) any {
	if len(serialized) < sqliteZstdThresholdBytes {
		return string(serialized)
	}
	compressed := sqliteZstdEncoder.EncodeAll(serialized, nil)
	if len(compressed) < len(serialized) {
		return compressed
	}
	return string(serialized)
}

func marshalSQLiteJSON(value any) ([]byte, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'}), nil
}

func decodeSQLiteData(value any, maxBytes int) ([]byte, error) {
	var decoded []byte
	switch value := value.(type) {
	case string:
		if maxBytes > 0 && len(value) > maxBytes {
			return nil, fmt.Errorf("packed data exceeds %d UTF-8 bytes", maxBytes)
		}
		decoded = []byte(value)
	case []byte:
		decoder := sqliteZstdDecoder
		if maxBytes > 0 {
			decoder = sqlitePackedZstdDecoder
		}
		var err error
		decoded, err = decoder.DecodeAll(value, nil)
		if err != nil {
			if maxBytes > 0 && errors.Is(err, zstd.ErrDecoderSizeExceeded) {
				return nil, fmt.Errorf("packed data exceeds %d UTF-8 bytes: %w", maxBytes, err)
			}
			return nil, err
		}
	default:
		return nil, fmt.Errorf("stored event data must be text or blob")
	}
	if maxBytes > 0 && len(decoded) > maxBytes {
		return nil, fmt.Errorf("packed data exceeds %d UTF-8 bytes", maxBytes)
	}
	if !utf8.Valid(decoded) {
		return nil, fmt.Errorf("stored event data is not valid UTF-8")
	}
	return decoded, nil
}

func encodeSQLiteSourceSeqs(values []int) ([]byte, error) {
	bytes := make([]byte, 0, len(values))
	var previous int64
	for index, value := range values {
		seq := int64(value)
		if value < 0 || seq > maxJSONSafeInteger {
			return nil, fmt.Errorf("sourceEventSeqs must contain non-negative safe integers")
		}
		var encoded uint64
		if index == 0 {
			encoded = uint64(seq)
		} else if seq >= previous {
			encoded = uint64(seq-previous) * 2
		} else {
			encoded = uint64(previous-seq)*2 - 1
		}
		bytes = appendSQLiteVarint(bytes, encoded)
		previous = seq
	}
	return bytes, nil
}

func appendSQLiteVarint(bytes []byte, value uint64) []byte {
	for value >= 0x80 {
		bytes = append(bytes, byte(value)|0x80)
		value >>= 7
	}
	return append(bytes, byte(value))
}

func decodeSQLiteSourceSeqs(bytes []byte) ([]int, error) {
	values := make([]int, 0)
	var previous int64
	for offset := 0; offset < len(bytes); {
		first := len(values) == 0
		limit := uint64(maxJSONSafeInteger * 2)
		if first {
			limit = uint64(maxJSONSafeInteger)
		}
		encoded, next, err := readSQLiteVarint(bytes, offset, limit)
		if err != nil {
			return nil, err
		}
		offset = next
		var value int64
		if first {
			value = int64(encoded)
		} else if encoded&1 == 0 {
			value = previous + int64(encoded/2)
		} else {
			value = previous - int64((encoded+1)/2)
		}
		if value < 0 || value > maxJSONSafeInteger || uint64(value) > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("malformed source_event_seqs storage value: decoded seq is out of range")
		}
		values = append(values, int(value))
		previous = value
	}
	return values, nil
}

func readSQLiteVarint(bytes []byte, offset int, limit uint64) (uint64, int, error) {
	var value uint64
	var shift uint
	for offset < len(bytes) {
		current := bytes[offset]
		offset++
		value |= uint64(current&0x7f) << shift
		if current&0x80 == 0 {
			if shift > 0 && current&0x7f == 0 {
				return 0, 0, fmt.Errorf("malformed source_event_seqs storage value: non-canonical varint")
			}
			if value > limit {
				return 0, 0, fmt.Errorf("malformed source_event_seqs storage value: varint is out of range")
			}
			return value, offset, nil
		}
		shift += 7
		if shift > 56 {
			return 0, 0, fmt.Errorf("malformed source_event_seqs storage value: varint is out of range")
		}
	}
	return 0, 0, fmt.Errorf("malformed source_event_seqs storage value: truncated varint")
}

func scanSQLiteSessionEvents(rows []sqliteEventRow, base int) ([]Event, *int, error) {
	lastTurnEndRow := -1
	for index := len(rows) - 1; index >= 0; index-- {
		events, err := sqliteRowEvents(rows[index])
		if err != nil {
			continue
		}
		for _, event := range events {
			if event.Type == "turn/end" {
				lastTurnEndRow = index
				break
			}
		}
		if lastTurnEndRow >= 0 {
			break
		}
	}
	preserved := make([]Event, 0, len(rows))
	expected := base
	for index, row := range rows {
		events, err := sqliteRowEvents(row)
		contiguous := err == nil
		if contiguous {
			for _, event := range events {
				if event.Seq != expected {
					contiguous = false
					break
				}
				expected++
			}
		}
		if !contiguous {
			if index <= lastTurnEndRow {
				return nil, nil, fmt.Errorf("corrupt session log: invalid committed physical row at seq %d", row.seq)
			}
			torn := row.seq
			if base == 0 {
				if _, err := foldSurfaceEvents(preserved, true); err != nil {
					return nil, nil, err
				}
			}
			return preserved, &torn, nil
		}
		preserved = append(preserved, events...)
	}
	if base == 0 {
		if _, err := foldSurfaceEvents(preserved, true); err != nil {
			return nil, nil, err
		}
	}
	return preserved, nil, nil
}

func sqliteRowEvents(row sqliteEventRow) ([]Event, error) {
	if row.ignorable.Valid && row.ignorable.Int64 != 0 && row.ignorable.Int64 != 1 {
		return nil, fmt.Errorf("stored event ignorable must be 0, 1, or null")
	}
	if row.ignorable.Valid && row.ignorable.Int64 == sqlitePackedSentinel {
		if !isSQLitePackedTag(row.typ) {
			return nil, fmt.Errorf("malformed %s storage row: packed discriminator requires a chunk tag", row.typ)
		}
		if row.sources != nil || row.surface.Valid {
			return nil, fmt.Errorf("malformed %s storage row: packed surface fields must be null", row.typ)
		}
		data, err := decodeSQLiteData(row.data, maxSQLitePackedDataBytes)
		if err != nil {
			return nil, err
		}
		record, err := json.Marshal(struct {
			Type string          `json:"type"`
			Seq  int             `json:"seq0"`
			Time int64           `json:"time0"`
			Data json.RawMessage `json:"data"`
		}{Type: row.typ, Seq: row.seq, Time: row.time, Data: data})
		if err != nil {
			return nil, err
		}
		events, err := decodeSessionStorageRecord(record)
		if err != nil {
			return nil, err
		}
		if len(events) < minSQLitePackedRowMembers || len(events) > maxSQLitePackedRowMembers {
			return nil, fmt.Errorf("malformed %s storage row: member count must be between %d and %d", row.typ, minSQLitePackedRowMembers, maxSQLitePackedRowMembers)
		}
		return events, nil
	}
	data, err := decodeSQLiteData(row.data, 0)
	if err != nil {
		return nil, err
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	event := Event{Type: row.typ, Seq: row.seq, Time: row.time, Data: value}
	if row.sources != nil {
		encoded, ok := row.sources.([]byte)
		if !ok {
			return nil, fmt.Errorf("stored event source_event_seqs must be blob or null")
		}
		event.SourceEventSeqs, err = decodeSQLiteSourceSeqs(encoded)
		if err != nil {
			return nil, err
		}
	}
	if row.surface.Valid {
		if err := json.Unmarshal([]byte(row.surface.String), &event.SurfaceOp); err != nil {
			return nil, err
		}
	}
	event.Ignorable = row.ignorable.Valid && row.ignorable.Int64 == 1
	if isSQLitePackedTag(event.Type) {
		if !event.Ignorable {
			return nil, fmt.Errorf("session event type %q (seq %d) is unknown and not marked ignorable", event.Type, event.Seq)
		}
		return []Event{event}, nil
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	decoded, err := decodeSessionStorageRecord(encoded)
	if err != nil {
		return nil, err
	}
	if len(decoded) != 1 {
		return nil, fmt.Errorf("decoded into %d events", len(decoded))
	}
	return decoded, nil
}

func isSQLitePackedTag(value string) bool {
	return value == "text-chunks" || value == "reasoning-chunks" || value == "tool-call-chunks"
}
