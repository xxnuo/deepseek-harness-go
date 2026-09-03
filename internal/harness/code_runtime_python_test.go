package harness

import (
	"encoding/json"
	"math"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestPythonProtocolCodecAndHostileFrameValidation(t *testing.T) {
	if got := PythonLogTruncationMarker(65536); got != "[dsh-code-runtime-python] log capture truncated at 65536 bytes" {
		t.Fatalf("marker = %q", got)
	}
	frame, ok := ValidatePythonChildFrame(map[string]any{"type": "boot-ack", "extra": true})
	if !ok || frame != (PythonChildFrame{Type: "boot-ack"}) {
		t.Fatalf("boot ack = %#v, %v", frame, ok)
	}
	frame, ok = ValidatePythonChildFrame(map[string]any{"type": "log", "text": "hi", "truncated": "yes"})
	if !ok || frame.Type != "log" || frame.Text != "hi" || frame.Truncated {
		t.Fatalf("log = %#v, %v", frame, ok)
	}
	frame, ok = ValidatePythonChildFrame(map[string]any{
		"type": "call", "id": float64(1), "global": "tools", "name": "echo", "args": map[string]any{"x": float64(1)}, "extra": true,
	})
	if !ok || frame.Type != "call" || frame.ID != 1 || frame.Global != "tools" || frame.Name != "echo" {
		t.Fatalf("call = %#v, %v", frame, ok)
	}
	for _, raw := range []any{
		nil,
		map[string]any{"type": "call", "id": "1", "global": "tools", "name": "echo", "args": nil},
		map[string]any{"type": "call", "id": math.Copysign(0, -1), "global": "tools", "name": "echo", "args": nil},
		map[string]any{"type": "call", "id": float64(1), "global": "tools", "name": "echo"},
		map[string]any{"type": "call", "id": float64(1), "global": "tools", "name": "echo", "args": math.Inf(1)},
		map[string]any{"type": "done", "error": map[string]any{"kind": "timeout", "message": "bad"}},
	} {
		if frame, ok := ValidatePythonChildFrame(raw); ok {
			t.Fatalf("hostile frame accepted: %#v -> %#v", raw, frame)
		}
	}
	frame, ok = ValidatePythonChildFrame(map[string]any{
		"type": "done", "value": nil, "error": map[string]any{"kind": "exception", "message": "boom"}, "extra": true,
	})
	if !ok || !frame.HasValue || frame.Value != nil || frame.Error == nil || frame.Error.Kind != "exception" {
		t.Fatalf("done = %#v, %v", frame, ok)
	}
}

func TestPythonProtocolLosslessJSONChecks(t *testing.T) {
	cases := []any{nil, true, false, float64(0), -1.5, `a"b\`, []any{}, map[string]any{}, []any{float64(1), "x", nil}, map[string]any{"a": []any{float64(1), float64(2)}, "b": map[string]any{"c": "d"}}}
	for _, value := range cases {
		encoded, err := EncodePythonJSONPlain(value)
		if err != nil {
			t.Fatal(err)
		}
		check, err := CheckPythonDoneValue(value, len(encoded))
		if err != nil || !check.OK || check.Bytes != len(encoded) {
			t.Fatalf("check(%s) = %#v, %v", encoded, check, err)
		}
		check, err = CheckPythonDoneValue(value, len(encoded)-1)
		if err != nil || check.Reason != "over-budget" {
			t.Fatalf("limited check(%s) = %#v, %v", encoded, check, err)
		}
	}
	if encoded, err := EncodePythonJSONPlain([]any{float64(1 << 60)}); err != nil || encoded != `[1152921504606846976]` {
		t.Fatalf("large integer encoding = %q, %v", encoded, err)
	}
	for _, value := range []any{math.Inf(1), math.Inf(-1), math.NaN(), math.Copysign(0, -1), map[string]any{"a": []any{float64(1), math.Copysign(0, -1)}}} {
		check, err := CheckPythonDoneValue(value, 1024)
		if err != nil || check.Reason != "non-lossless" || !HasNonLosslessJSONNumber(value) {
			t.Fatalf("lossy check = %#v, %v", check, err)
		}
	}
	for _, value := range []any{[]any{"x", math.Inf(1)}, []any{math.Inf(1), "x"}} {
		check, err := CheckPythonDoneValue(value, 3)
		if err != nil || check.Reason != "over-budget" {
			t.Fatalf("over-budget precedence = %#v, %v", check, err)
		}
	}
	if check, _ := CheckPythonDoneValue("\x00", 8); !check.OK || check.Bytes != 8 {
		t.Fatalf("escaped NUL = %#v", check)
	}
	if check, _ := CheckPythonDoneValue(strings.Repeat("\x00", 200), 1024); check.Reason != "over-budget" {
		t.Fatalf("control-heavy string = %#v", check)
	}

	deep := any(float64(0))
	for range 100_000 {
		deep = []any{deep}
	}
	check, err := CheckPythonDoneValue(deep, 1_000_000)
	if err != nil || !check.OK || check.Bytes != 200_001 {
		t.Fatalf("deep check = %#v, %v", check, err)
	}
	encoded, err := EncodePythonJSONPlain(deep)
	if err != nil || len(encoded) != 200_001 || encoded[100_000] != '0' {
		t.Fatalf("deep encoding length = %d, %v", len(encoded), err)
	}
}

func TestPythonProtocolNumberWalkUsesDepthBoundedCursors(t *testing.T) {
	wideArray := make([]any, 2_000_000)
	wideArray[len(wideArray)-1] = math.Copysign(0, -1)
	if !HasNonLosslessJSONNumber(wideArray) {
		t.Fatal("wide array negative zero was not found")
	}

	wideObject := make(PythonJSONObject, 200_000)
	for index := range wideObject {
		wideObject[index] = PythonJSONMember{Key: "k" + strconv.Itoa(index), Value: float64(0)}
	}
	wideObject[len(wideObject)-1].Value = math.Inf(1)
	if !HasNonLosslessJSONNumber(wideObject) {
		t.Fatal("wide object infinity was not found")
	}

	deep := any(float64(0))
	for range 100_000 {
		deep = []any{deep}
	}
	if !HasNonLosslessJSONNumber([]any{deep, math.Copysign(0, -1)}) {
		t.Fatal("parent cursor did not resume after deep child")
	}
}

func TestPythonProtocolOrderedObjectsAndJSONNumbers(t *testing.T) {
	object := PythonJSONObject{
		{Key: "b", Value: 1},
		{Key: "10", Value: 10},
		{Key: "2", Value: 2},
		{Key: "a", Value: 3},
		{Key: "01", Value: 4},
		{Key: "4294967295", Value: 5},
	}
	const wantObject = `{"2":2,"10":10,"b":1,"a":3,"01":4,"4294967295":5}`
	encoded, err := EncodePythonJSONPlain(object)
	if err != nil || encoded != wantObject {
		t.Fatalf("ordered object = %q, %v", encoded, err)
	}
	check, err := CheckPythonDoneValue(object, len(wantObject))
	if err != nil || !check.OK || check.Bytes != len(wantObject) {
		t.Fatalf("ordered object check = %#v, %v", check, err)
	}

	duplicate := PythonJSONObject{{Key: "x", Value: 1}, {Key: "x", Value: 2}}
	if _, err := EncodePythonJSONPlain(duplicate); err == nil {
		t.Fatal("duplicate object key encoded")
	}
	if _, err := CheckPythonDoneValue(duplicate, 1024); err == nil {
		t.Fatal("duplicate object key passed budget check")
	}
	if check, err := CheckPythonDoneValue(duplicate, 1); err != nil || check.Reason != "over-budget" {
		t.Fatalf("duplicate object cheap bound = %#v, %v", check, err)
	}

	cases := []struct {
		token       string
		want        string
		nonLossless bool
	}{
		{token: "1.0", want: "1"},
		{token: "1e20", want: "100000000000000000000"},
		{token: "9007199254740993", want: "9007199254740992"},
		{token: "1e400", want: "Infinity", nonLossless: true},
		{token: "-0", want: "0", nonLossless: true},
	}
	for _, test := range cases {
		value := json.Number(test.token)
		encoded, err := EncodePythonJSONPlain(value)
		if err != nil || encoded != test.want {
			t.Fatalf("encode json.Number(%q) = %q, %v", test.token, encoded, err)
		}
		check, err := CheckPythonDoneValue(value, len(test.want))
		if err != nil {
			t.Fatalf("check json.Number(%q): %v", test.token, err)
		}
		if test.nonLossless {
			if check.Reason != "non-lossless" || !HasNonLosslessJSONNumber(value) {
				t.Fatalf("lossy json.Number(%q) = %#v", test.token, check)
			}
		} else if !check.OK || check.Bytes != len(test.want) || HasNonLosslessJSONNumber(value) {
			t.Fatalf("lossless json.Number(%q) = %#v", test.token, check)
		}
		check, err = CheckPythonDoneValue(value, len(test.want)-1)
		if err != nil || check.Reason != "over-budget" {
			t.Fatalf("limited json.Number(%q) = %#v, %v", test.token, check, err)
		}
	}
}

func TestPythonProtocolUTF16Strings(t *testing.T) {
	cases := []struct {
		name  string
		value PythonUTF16String
		want  string
	}{
		{name: "lone surrogate", value: PythonUTF16String{0xd800}, want: `"\ud800"`},
		{name: "lone surrogate then ascii", value: PythonUTF16String{0xd800, 'a'}, want: `"\ud800a"`},
		{name: "surrogate pair", value: PythonUTF16String{0xd83d, 0xde00}, want: `"😀"`},
	}
	for _, test := range cases {
		encoded, err := EncodePythonJSONPlain(test.value)
		if err != nil || encoded != test.want {
			t.Fatalf("%s encoding = %q, %v", test.name, encoded, err)
		}
		check, err := CheckPythonDoneValue(test.value, len(test.want))
		if err != nil || !check.OK || check.Bytes != len(test.want) {
			t.Fatalf("%s check = %#v, %v", test.name, check, err)
		}
		check, err = CheckPythonDoneValue(test.value, len(test.want)-1)
		if err != nil || check.Reason != "over-budget" {
			t.Fatalf("limited %s check = %#v, %v", test.name, check, err)
		}
	}

	invalid := string([]byte{0xff})
	if _, err := EncodePythonJSONPlain(invalid); err == nil {
		t.Fatal("invalid UTF-8 string encoded")
	}
	if _, err := CheckPythonDoneValue(invalid, 1024); err == nil {
		t.Fatal("invalid UTF-8 string passed budget check")
	}

	loneKey := PythonJSONObject{{UTF16Key: PythonUTF16String{0xd800}, Value: 1}}
	const wantLoneKey = `{"\ud800":1}`
	encoded, err := EncodePythonJSONPlain(loneKey)
	if err != nil || encoded != wantLoneKey {
		t.Fatalf("lone surrogate key = %q, %v", encoded, err)
	}
	check, err := CheckPythonDoneValue(loneKey, len(wantLoneKey))
	if err != nil || !check.OK || check.Bytes != len(wantLoneKey) {
		t.Fatalf("lone surrogate key check = %#v, %v", check, err)
	}
	check, err = CheckPythonDoneValue(loneKey, len(wantLoneKey)-1)
	if err != nil || check.Reason != "over-budget" {
		t.Fatalf("limited lone surrogate key check = %#v, %v", check, err)
	}

	ordered := PythonJSONObject{
		{Key: "b", Value: 1},
		{UTF16Key: PythonUTF16String{'1', '0'}, Value: 10},
		{UTF16Key: PythonUTF16String{'2'}, Value: 2},
	}
	if encoded, err := EncodePythonJSONPlain(ordered); err != nil || encoded != `{"2":2,"10":10,"b":1}` {
		t.Fatalf("UTF-16 array-index keys = %q, %v", encoded, err)
	}

	duplicateKey := PythonJSONObject{
		{Key: "😀", Value: 1},
		{UTF16Key: PythonUTF16String{0xd83d, 0xde00}, Value: 2},
	}
	if _, err := EncodePythonJSONPlain(duplicateKey); err == nil {
		t.Fatal("equivalent UTF-8 and UTF-16 keys encoded")
	}
	if _, err := CheckPythonDoneValue(duplicateKey, 1024); err == nil {
		t.Fatal("equivalent UTF-8 and UTF-16 keys passed budget check")
	}
}

func TestPythonProtocolUnsafeIntegerTokenScan(t *testing.T) {
	unsafe := []string{`{"v":9007199254740993}`, `{"v":-9007199254740993}`, `{"v":` + strings.Repeat("9", 400) + `}`}
	for _, line := range unsafe {
		if !HasUnsafeJSONIntegerToken(line) {
			t.Fatalf("unsafe token accepted: %s", line)
		}
	}
	safe := []string{`{"v":9007199254740992}`, `{"v":18446744073709551616}`, `{"v":9007199254740991}`, `{"v":"9007199254740993"}`, `{"v":9007199254740993.0}`, `{"v":9e99}`}
	for _, line := range safe {
		if HasUnsafeJSONIntegerToken(line) {
			t.Fatalf("safe token rejected: %s", line)
		}
	}
}

func TestPythonProtocolMirrorWithRealPython(t *testing.T) {
	if _, err := exec.LookPath("uv"); err != nil {
		t.Skip("uv is unavailable")
	}
	assets, err := defaultAssetPaths()
	if err != nil {
		t.Fatal(err)
	}
	pyDir := filepath.Join(assets.UpstreamDir, "packages", "experimental", "code-runtime-python", "py")
	pathJSON, _ := json.Marshal(pyDir)
	ordered, err := EncodePythonJSONPlain(PythonJSONObject{
		{Key: "b", Value: 1}, {Key: "10", Value: 10}, {Key: "2", Value: 2},
		{Key: "a", Value: 3}, {Key: "01", Value: 4}, {Key: "4294967295", Value: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	lone, err := EncodePythonJSONPlain(PythonUTF16String{0xd800})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := EncodePythonJSONPlain(PythonUTF16String{0xd83d, 0xde00})
	if err != nil {
		t.Fatal(err)
	}
	loneKey, err := EncodePythonJSONPlain(PythonJSONObject{{UTF16Key: PythonUTF16String{0xd800}, Value: 1}})
	if err != nil {
		t.Fatal(err)
	}
	orderedJSON, _ := json.Marshal(ordered)
	loneJSON, _ := json.Marshal(lone)
	pairJSON, _ := json.Marshal(pair)
	loneKeyJSON, _ := json.Marshal(loneKey)
	probe := strings.Join([]string{
		"import json, sys",
		"sys.path.insert(0, " + string(pathJSON) + ")",
		"import protocol as p",
		`def keys(td): return {"required": sorted(td.__required_keys__), "optional": sorted(td.__optional_keys__)}`,
		`frames = {n: keys(v) for n, v in vars(p).items() if not n.startswith("_") and hasattr(v, "__required_keys__")}`,
		"ordered = json.loads(" + string(orderedJSON) + ")",
		"lone = json.loads(" + string(loneJSON) + ")",
		"pair = json.loads(" + string(pairJSON) + ")",
		"lone_key = json.loads(" + string(loneKeyJSON) + ")",
		`print(json.dumps({"fd": p.PROTOCOL_FD, "markers": [p.log_truncation_marker(v) for v in [1, 65536, 1048576]], "frames": frames, "orderedKeys": list(ordered), "loneOrd": ord(lone), "pairOrd": ord(pair), "loneKeyOrd": ord(next(iter(lone_key)))}))`,
	}, "\n")
	output, err := exec.Command("uv", "run", "python", "-I", "-B", "-c", probe).CombinedOutput()
	if err != nil {
		t.Fatalf("python mirror: %v\n%s", err, output)
	}
	var seen struct {
		FD      int                           `json:"fd"`
		Markers []string                      `json:"markers"`
		Frames  map[string]PythonWireFieldSet `json:"frames"`
		Ordered []string                      `json:"orderedKeys"`
		LoneOrd int                           `json:"loneOrd"`
		PairOrd int                           `json:"pairOrd"`
		LoneKey int                           `json:"loneKeyOrd"`
	}
	if err := json.Unmarshal(output, &seen); err != nil {
		t.Fatalf("decode mirror: %v\n%s", err, output)
	}
	if seen.FD != PythonProtocolFD {
		t.Fatalf("protocol fd = %d", seen.FD)
	}
	wantMarkers := []string{PythonLogTruncationMarker(1), PythonLogTruncationMarker(65536), PythonLogTruncationMarker(1048576)}
	wantOrder := []string{"2", "10", "b", "a", "01", "4294967295"}
	if !reflect.DeepEqual(seen.Markers, wantMarkers) ||
		!reflect.DeepEqual(seen.Frames, PythonWireFrameFields) ||
		!reflect.DeepEqual(seen.Ordered, wantOrder) || seen.LoneOrd != 0xd800 || seen.PairOrd != 0x1f600 || seen.LoneKey != 0xd800 {
		t.Fatalf("python mirror mismatch: %#v", seen)
	}
}
