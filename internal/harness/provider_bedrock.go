package harness

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const bedrockEmptyText = "<empty>"

type bedrockProvider struct {
	id, baseURL, apiKey, model string
	client                     *http.Client
	headers                    map[string]string
	cacheRetention             string
	modelSpec                  piAIModel
	thinkingBudgets            map[string]int
	streamIdleTimeout          time.Duration
	env                        map[string]string
}

func newBedrockProvider(id, baseURL, apiKey, model string) *bedrockProvider {
	return &bedrockProvider{
		id: id, baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, model: model,
		client: &http.Client{},
	}
}

func (p *bedrockProvider) Complete(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (Completion, error) {
	model := firstNonBlank(req.Model, p.model)
	region := bedrockRegion(model, p.baseURL, p.env)
	endpoint, err := bedrockEndpoint(p.baseURL, region, model, p.env)
	if err != nil {
		return Completion{}, err
	}
	supportsStrict := p.modelSpec.Compat.SupportsStrictMode != nil && *p.modelSpec.Compat.SupportsStrictMode
	if err := validateToolSampling(req.Tools, supportsStrict); err != nil {
		return Completion{}, err
	}
	body := p.requestBody(req)
	data, err := json.Marshal(body)
	if err != nil {
		return Completion{}, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return Completion{}, err
	}
	hreq.Header.Set("Accept", "application/vnd.amazon.eventstream")
	hreq.Header.Set("Content-Type", "application/json")
	for name, value := range p.headers {
		lower := strings.ToLower(name)
		if lower == "authorization" || lower == "host" || strings.HasPrefix(lower, "x-amz-") {
			continue
		}
		hreq.Header.Set(name, value)
	}
	if piAIRequestEnv(p.env, "AWS_BEDROCK_SKIP_AUTH") != "1" {
		bearer := firstNonBlank(p.apiKey, piAIRequestEnv(p.env, "AWS_BEARER_TOKEN_BEDROCK"))
		if bearer != "" {
			hreq.Header.Set("Authorization", "Bearer "+bearer)
		} else {
			credential, credErr := resolveAWSCredential(p.env)
			if credErr != nil {
				return Completion{}, credErr
			}
			if err := signAWSRequest(hreq, data, credential, region, "bedrock", time.Now().UTC()); err != nil {
				return Completion{}, err
			}
		}
	}
	resp, err := p.client.Do(hreq)
	if err != nil {
		if ctx.Err() != nil {
			return Completion{}, ctx.Err()
		}
		return Completion{}, &ProviderError{Code: "TRANSPORT", Message: err.Error(), Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return Completion{}, providerHTTPFailure(resp, string(payload))
	}
	state := bedrockStreamState{onDelta: onDelta, blocks: map[int]*bedrockStreamBlock{}, usage: map[string]any{}}
	if err := consumeBedrockEventStream(ctx, resp.Body, p.streamIdleTimeout, state.handleEvent); err != nil {
		return Completion{}, err
	}
	return state.completion()
}

func (p *bedrockProvider) requestBody(req ChatRequest) map[string]any {
	retention := resolvedPiAICacheRetention(p.cacheRetention)
	body := map[string]any{"messages": bedrockMessages(req.Messages, p.modelSpec, retention)}
	if req.System != "" {
		system := []any{map[string]any{"text": req.System}}
		if retention != "none" && bedrockSupportsPromptCache(p.modelSpec) {
			cache := map[string]any{"type": "default"}
			if retention == "long" {
				cache["ttl"] = "1h"
			}
			system = append(system, map[string]any{"cachePoint": cache})
		}
		body["system"] = system
	}
	maxTokens := req.MaxTokens
	if maxTokens == 0 && bedrockClaude(p.modelSpec) {
		maxTokens = p.modelSpec.MaxTokens
	}
	inference := map[string]any{}
	if maxTokens > 0 {
		inference["maxTokens"] = maxTokens
	}
	if req.Temperature != nil {
		inference["temperature"] = *req.Temperature
	}
	if len(inference) > 0 {
		body["inferenceConfig"] = inference
	}
	if len(req.Tools) > 0 {
		supportsStrict := p.modelSpec.Compat.SupportsStrictMode != nil && *p.modelSpec.Compat.SupportsStrictMode
		tools := make([]any, 0, len(req.Tools))
		for _, tool := range req.Tools {
			spec := map[string]any{
				"name": tool.Name, "description": tool.Description,
				"inputSchema": map[string]any{"json": tool.Parameters},
			}
			if _, requested, _ := resolveJSONSchemaStrictSampling(tool, supportsStrict); requested {
				spec["strict"] = true
			}
			tools = append(tools, map[string]any{"toolSpec": spec})
		}
		body["toolConfig"] = map[string]any{"tools": tools}
	}
	if fields := p.reasoningFields(req.ReasoningEffort); fields != nil {
		body["additionalModelRequestFields"] = fields
	}
	return body
}

func (p *bedrockProvider) reasoningFields(effort string) map[string]any {
	if !p.modelSpec.Reasoning || effort == "" || effort == "off" || !bedrockClaude(p.modelSpec) {
		return nil
	}
	if bedrockAdaptiveThinking(p.modelSpec) {
		wire, ok := piAIReasoningWire(p.modelSpec, effort)
		if !ok {
			wire = map[string]string{"minimal": "low", "low": "low", "medium": "medium", "high": "high", "xhigh": "xhigh", "max": "high"}[effort]
		}
		return map[string]any{
			"thinking":      map[string]any{"type": "adaptive", "display": "summarized"},
			"output_config": map[string]any{"effort": wire},
		}
	}
	level := effort
	if level == "xhigh" || level == "max" {
		level = "high"
	}
	budget := p.thinkingBudgets[level]
	if budget == 0 {
		budget = map[string]int{"minimal": 1024, "low": 2048, "medium": 8192, "high": 16384}[level]
	}
	return map[string]any{
		"thinking":       map[string]any{"type": "enabled", "budget_tokens": budget, "display": "summarized"},
		"anthropic_beta": []string{"interleaved-thinking-2025-05-14"},
	}
}

func bedrockMessages(messages []ChatMessage, model piAIModel, retention string) []any {
	out := make([]any, 0, len(messages))
	for index := 0; index < len(messages); index++ {
		message := messages[index]
		switch message.Role {
		case "user", "system":
			content := bedrockTextAndImages(message)
			out = append(out, map[string]any{"role": "user", "content": content})
		case "assistant":
			content := make([]any, 0, len(message.ToolCalls)+2)
			if text := strings.TrimSpace(message.Content); text != "" {
				content = append(content, map[string]any{"text": message.Content})
			}
			if reasoning := strings.TrimSpace(message.Reasoning); reasoning != "" {
				if bedrockClaude(model) {
					if strings.TrimSpace(message.ReasoningSignature) == "" {
						content = append(content, map[string]any{"text": message.Reasoning})
					} else {
						content = append(content, map[string]any{"reasoningContent": map[string]any{"reasoningText": map[string]any{"text": message.Reasoning, "signature": message.ReasoningSignature}}})
					}
				} else {
					content = append(content, map[string]any{"reasoningContent": map[string]any{"reasoningText": map[string]any{"text": message.Reasoning}}})
				}
			}
			for _, call := range message.ToolCalls {
				arguments := map[string]any{}
				if len(call.Arguments) > 0 {
					_ = json.Unmarshal(call.Arguments, &arguments)
				}
				content = append(content, map[string]any{"toolUse": map[string]any{
					"toolUseId": bedrockToolCallID(call.ID), "name": call.Name, "input": arguments,
				}})
			}
			if len(content) > 0 {
				out = append(out, map[string]any{"role": "assistant", "content": content})
			}
		case "tool":
			results := make([]any, 0, 1)
			for ; index < len(messages) && messages[index].Role == "tool"; index++ {
				item := messages[index]
				content := bedrockTextAndImages(item)
				results = append(results, map[string]any{"toolResult": map[string]any{
					"toolUseId": bedrockToolCallID(item.ToolCallID), "content": content, "status": "success",
				}})
			}
			index--
			out = append(out, map[string]any{"role": "user", "content": results})
		}
	}
	if retention != "none" && bedrockSupportsPromptCache(model) && len(out) > 0 {
		last, _ := out[len(out)-1].(map[string]any)
		if last["role"] == "user" {
			content, _ := last["content"].([]any)
			cache := map[string]any{"type": "default"}
			if retention == "long" {
				cache["ttl"] = "1h"
			}
			last["content"] = append(content, map[string]any{"cachePoint": cache})
		}
	}
	return out
}

func bedrockTextAndImages(message ChatMessage) []any {
	content := make([]any, 0, len(chatContentParts(message)))
	for _, part := range chatContentParts(message) {
		if part.Type == "text" && strings.TrimSpace(part.Text) != "" {
			content = append(content, map[string]any{"text": part.Text})
		} else if part.Type == "image" {
			format := strings.TrimPrefix(strings.ToLower(part.MediaType), "image/")
			if format == "jpg" {
				format = "jpeg"
			}
			content = append(content, map[string]any{"image": map[string]any{"format": format, "source": map[string]any{"bytes": part.Data}}})
		}
	}
	if len(content) == 0 {
		content = append(content, map[string]any{"text": bedrockEmptyText})
	}
	return content
}

func bedrockToolCallID(id string) string {
	var out strings.Builder
	for _, r := range id {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			out.WriteRune(r)
		} else {
			out.WriteByte('_')
		}
		if out.Len() == 64 {
			break
		}
	}
	return firstNonBlank(out.String(), "tool_call")
}

func bedrockClaude(model piAIModel) bool {
	value := strings.ToLower(model.ID + " " + model.Name)
	return strings.Contains(value, "claude")
}

func bedrockAdaptiveThinking(model piAIModel) bool {
	value := strings.NewReplacer("_", "-", ".", "-", ":", "-", " ", "-").Replace(strings.ToLower(model.ID + " " + model.Name))
	for _, marker := range []string{"opus-4-6", "opus-4-7", "opus-4-8", "opus-5", "sonnet-4-6", "sonnet-5", "fable-5"} {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}

func bedrockSupportsPromptCache(model piAIModel) bool {
	value := strings.NewReplacer("_", "-", ".", "-", ":", "-", " ", "-").Replace(strings.ToLower(model.ID + " " + model.Name))
	return strings.Contains(value, "claude") && (strings.Contains(value, "-4-") || strings.Contains(value, "-5") || strings.Contains(value, "claude-3-7-sonnet") || strings.Contains(value, "claude-3-5-haiku"))
}

func bedrockRegion(model, baseURL string, env map[string]string) string {
	if strings.HasPrefix(model, "arn:") {
		parts := strings.Split(model, ":")
		if len(parts) > 3 && parts[2] == "bedrock" && parts[3] != "" {
			return parts[3]
		}
	}
	if region := firstNonBlank(piAIRequestEnv(env, "AWS_REGION"), piAIRequestEnv(env, "AWS_DEFAULT_REGION")); region != "" {
		return region
	}
	if parsed, err := url.Parse(baseURL); err == nil {
		parts := strings.Split(parsed.Hostname(), ".")
		if len(parts) >= 4 && strings.HasPrefix(parts[0], "bedrock-runtime") {
			return parts[1]
		}
	}
	return "us-east-1"
}

func bedrockEndpoint(baseURL, region, model string, env map[string]string) (string, error) {
	base := firstNonBlank(baseURL, "https://bedrock-runtime."+region+".amazonaws.com")
	if parsed, err := url.Parse(base); err == nil && strings.HasPrefix(parsed.Hostname(), "bedrock-runtime.us-east-1.") && region != "us-east-1" && firstNonBlank(piAIRequestEnv(env, "AWS_REGION"), piAIRequestEnv(env, "AWS_DEFAULT_REGION")) != "" {
		parsed.Host = strings.Replace(parsed.Host, "bedrock-runtime.us-east-1.", "bedrock-runtime."+region+".", 1)
		base = parsed.String()
	}
	parsed, err := url.Parse(strings.TrimRight(base, "/") + "/model/" + url.PathEscape(model) + "/converse-stream")
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", &ProviderError{Code: "PROVIDER", Message: "invalid Bedrock endpoint " + base, Err: err}
	}
	return parsed.String(), nil
}

type awsCredential struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

func resolveAWSCredential(env map[string]string) (awsCredential, error) {
	credential := awsCredential{
		AccessKeyID:     firstNonBlank(piAIRequestEnv(env, "AWS_ACCESS_KEY_ID"), piAIRequestEnv(env, "AWS_ACCESS_KEY")),
		SecretAccessKey: firstNonBlank(piAIRequestEnv(env, "AWS_SECRET_ACCESS_KEY"), piAIRequestEnv(env, "AWS_SECRET_KEY")),
		SessionToken:    piAIRequestEnv(env, "AWS_SESSION_TOKEN"),
	}
	if credential.AccessKeyID != "" && credential.SecretAccessKey != "" {
		return credential, nil
	}
	path := piAIRequestEnv(env, "AWS_SHARED_CREDENTIALS_FILE")
	if path == "" {
		home, _ := os.UserHomeDir()
		path = filepath.Join(home, ".aws", "credentials")
	}
	profile := firstNonBlank(piAIRequestEnv(env, "AWS_PROFILE"), "default")
	file, err := os.Open(path)
	if err != nil {
		return awsCredential{}, &ProviderError{Code: "MISSING_CREDENTIAL", Message: "Amazon Bedrock requires AWS_BEARER_TOKEN_BEDROCK or AWS credentials", Err: err}
	}
	defer file.Close()
	section := ""
	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "["), "]"))
			continue
		}
		if section != profile || line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			values[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	if err := scanner.Err(); err != nil {
		return awsCredential{}, err
	}
	credential = awsCredential{AccessKeyID: values["aws_access_key_id"], SecretAccessKey: values["aws_secret_access_key"], SessionToken: values["aws_session_token"]}
	if credential.AccessKeyID == "" || credential.SecretAccessKey == "" {
		return awsCredential{}, &ProviderError{Code: "MISSING_CREDENTIAL", Message: "AWS profile " + profile + " has no usable access keys"}
	}
	return credential, nil
}

func signAWSRequest(req *http.Request, body []byte, credential awsCredential, region, service string, now time.Time) error {
	if credential.AccessKeyID == "" || credential.SecretAccessKey == "" {
		return errors.New("AWS access key and secret are required")
	}
	amzDate := now.UTC().Format("20060102T150405Z")
	shortDate := now.UTC().Format("20060102")
	payloadHash := sha256Hex(body)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if credential.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", credential.SessionToken)
	}
	canonicalHeaders, signedHeaders := awsCanonicalHeaders(req)
	canonicalRequest := strings.Join([]string{
		req.Method, req.URL.EscapedPath(), req.URL.Query().Encode(), canonicalHeaders, signedHeaders, payloadHash,
	}, "\n")
	scope := shortDate + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonicalRequest))
	kDate := hmacSHA256([]byte("AWS4"+credential.SecretAccessKey), shortDate)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+credential.AccessKeyID+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
	return nil
}

func awsCanonicalHeaders(req *http.Request) (string, string) {
	values := map[string]string{"host": req.URL.Host}
	for name, rows := range req.Header {
		lower := strings.ToLower(name)
		if lower == "authorization" {
			continue
		}
		joined := strings.Join(rows, ",")
		values[lower] = strings.Join(strings.Fields(joined), " ")
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	var canonical strings.Builder
	for _, name := range names {
		canonical.WriteString(name)
		canonical.WriteByte(':')
		canonical.WriteString(values[name])
		canonical.WriteByte('\n')
	}
	return canonical.String(), strings.Join(names, ";")
}

func sha256Hex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func hmacSHA256(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

type bedrockEvent struct {
	Type    string
	Headers map[string]string
	Payload []byte
}

func consumeBedrockEventStream(ctx context.Context, body io.Reader, idleTimeout time.Duration, handle func(bedrockEvent) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		event bedrockEvent
		err   error
	}
	results := make(chan result, 1)
	go func() {
		for {
			event, err := readBedrockEvent(body)
			select {
			case results <- result{event: event, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var timer *time.Timer
	var timeout <-chan time.Time
	if idleTimeout > 0 {
		timer = time.NewTimer(idleTimeout)
		timeout = timer.C
		defer timer.Stop()
	}
	reset := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(idleTimeout)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return &ProviderError{Code: "TIMEOUT", Message: fmt.Sprintf("provider stream idle timeout after %s", idleTimeout)}
		case result := <-results:
			if result.err != nil {
				if errors.Is(result.err, io.EOF) {
					return nil
				}
				return result.err
			}
			reset()
			if err := handle(result.event); err != nil {
				return err
			}
		}
	}
}

func readBedrockEvent(reader io.Reader) (bedrockEvent, error) {
	prelude := make([]byte, 12)
	if _, err := io.ReadFull(reader, prelude); err != nil {
		return bedrockEvent{}, err
	}
	total := int(binary.BigEndian.Uint32(prelude[:4]))
	headerLength := int(binary.BigEndian.Uint32(prelude[4:8]))
	if total < 16 || headerLength < 0 || headerLength > total-16 {
		return bedrockEvent{}, errors.New("invalid AWS EventStream frame lengths")
	}
	if crc32.ChecksumIEEE(prelude[:8]) != binary.BigEndian.Uint32(prelude[8:12]) {
		return bedrockEvent{}, errors.New("invalid AWS EventStream prelude CRC")
	}
	rest := make([]byte, total-12)
	if _, err := io.ReadFull(reader, rest); err != nil {
		return bedrockEvent{}, err
	}
	frame := append(append([]byte(nil), prelude...), rest...)
	if crc32.ChecksumIEEE(frame[:len(frame)-4]) != binary.BigEndian.Uint32(frame[len(frame)-4:]) {
		return bedrockEvent{}, errors.New("invalid AWS EventStream message CRC")
	}
	headers, err := decodeEventStreamHeaders(rest[:headerLength])
	if err != nil {
		return bedrockEvent{}, err
	}
	payload := append([]byte(nil), rest[headerLength:len(rest)-4]...)
	messageType := headers[":message-type"]
	if messageType == "exception" || messageType == "error" {
		return bedrockEvent{}, &ProviderError{Code: "PROVIDER", Message: firstNonBlank(headers[":exception-type"], headers[":error-code"], "Bedrock stream exception") + ": " + string(payload)}
	}
	return bedrockEvent{Type: headers[":event-type"], Headers: headers, Payload: payload}, nil
}

func decodeEventStreamHeaders(data []byte) (map[string]string, error) {
	headers := map[string]string{}
	for len(data) > 0 {
		nameLength := int(data[0])
		data = data[1:]
		if len(data) < nameLength+1 {
			return nil, errors.New("invalid AWS EventStream header")
		}
		name := string(data[:nameLength])
		kind := data[nameLength]
		data = data[nameLength+1:]
		switch kind {
		case 0:
			headers[name] = "true"
		case 1:
			headers[name] = "false"
		case 7:
			if len(data) < 2 {
				return nil, errors.New("invalid AWS EventStream string header")
			}
			length := int(binary.BigEndian.Uint16(data[:2]))
			data = data[2:]
			if len(data) < length {
				return nil, errors.New("truncated AWS EventStream string header")
			}
			headers[name] = string(data[:length])
			data = data[length:]
		default:
			return nil, fmt.Errorf("unsupported AWS EventStream header type %d", kind)
		}
	}
	return headers, nil
}

type bedrockStreamBlock struct {
	kind      string
	text      string
	id        string
	name      string
	arguments string
	signature string
}

type bedrockStreamState struct {
	onDelta            func(Delta) error
	blocks             map[int]*bedrockStreamBlock
	text               string
	reasoning          string
	reasoningSignature string
	calls              []ToolCall
	finish             string
	usage              map[string]any
	terminal           bool
}

func (s *bedrockStreamState) handleEvent(event bedrockEvent) error {
	var payload map[string]any
	if len(event.Payload) > 0 {
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("malformed Bedrock %s event: %w", event.Type, err)
		}
	}
	switch event.Type {
	case "messageStart":
		if role := stringSetting(payload["role"]); role != "" && role != "assistant" {
			return &ProviderError{Code: "PROVIDER", Message: "Bedrock started a non-assistant message"}
		}
	case "contentBlockStart":
		index := jsonInt(payload["contentBlockIndex"])
		start, _ := payload["start"].(map[string]any)
		if tool, ok := start["toolUse"].(map[string]any); ok {
			s.blocks[index] = &bedrockStreamBlock{kind: "tool", id: stringSetting(tool["toolUseId"]), name: stringSetting(tool["name"])}
		}
	case "contentBlockDelta":
		return s.handleDelta(payload)
	case "contentBlockStop":
		index := jsonInt(payload["contentBlockIndex"])
		if block := s.blocks[index]; block != nil && block.kind == "tool" {
			arguments := json.RawMessage(block.arguments)
			if !json.Valid(arguments) {
				arguments = json.RawMessage("{}")
			}
			s.calls = append(s.calls, ToolCall{ID: block.id, Name: block.name, Arguments: arguments})
		}
	case "messageStop":
		s.terminal = true
		switch stringSetting(payload["stopReason"]) {
		case "end_turn", "stop_sequence":
			s.finish = "stop"
		case "max_tokens", "model_context_window_exceeded":
			s.finish = "length"
		case "tool_use":
			s.finish = "tool_calls"
		default:
			return &ProviderError{Code: "PROVIDER", Message: "Bedrock stopped: " + stringSetting(payload["stopReason"])}
		}
	case "metadata":
		if usage, ok := payload["usage"].(map[string]any); ok {
			s.usage = map[string]any{
				"input_tokens": jsonInt(usage["inputTokens"]), "output_tokens": jsonInt(usage["outputTokens"]),
				"cache_read_tokens": jsonInt(usage["cacheReadInputTokens"]), "cache_write_tokens": jsonInt(usage["cacheWriteInputTokens"]),
				"total_tokens": jsonInt(usage["totalTokens"]),
			}
			if err := s.onDelta(Delta{Usage: cloneStringMap(s.usage)}); err != nil {
				return err
			}
		}
	case "internalServerException", "modelStreamErrorException", "validationException", "throttlingException", "serviceUnavailableException":
		return &ProviderError{Code: "PROVIDER", Message: event.Type + ": " + string(event.Payload)}
	}
	return nil
}

func (s *bedrockStreamState) handleDelta(payload map[string]any) error {
	index := jsonInt(payload["contentBlockIndex"])
	delta, _ := payload["delta"].(map[string]any)
	if text := stringSetting(delta["text"]); text != "" {
		block := s.blocks[index]
		if block == nil {
			block = &bedrockStreamBlock{kind: "text"}
			s.blocks[index] = block
		}
		block.text += text
		s.text += text
		return s.onDelta(Delta{Text: text})
	}
	if tool, ok := delta["toolUse"].(map[string]any); ok {
		block := s.blocks[index]
		if block == nil {
			block = &bedrockStreamBlock{kind: "tool"}
			s.blocks[index] = block
		}
		arguments := stringSetting(tool["input"])
		block.arguments += arguments
		return s.onDelta(Delta{ToolCalls: []ToolCallDelta{{Index: index, ID: block.id, Name: block.name, ArgumentsDelta: arguments}}})
	}
	if reasoning, ok := delta["reasoningContent"].(map[string]any); ok {
		text := stringSetting(reasoning["text"])
		signature := stringSetting(reasoning["signature"])
		if text == "" {
			if nested, ok := reasoning["reasoningText"].(map[string]any); ok {
				text = stringSetting(nested["text"])
				signature = stringSetting(nested["signature"])
			}
		}
		if text != "" {
			s.reasoning += text
			if err := s.onDelta(Delta{Reasoning: text}); err != nil {
				return err
			}
		}
		if signature != "" {
			s.reasoningSignature += signature
			return s.onDelta(Delta{ReasoningSignature: signature})
		}
	}
	return nil
}

func (s *bedrockStreamState) completion() (Completion, error) {
	if !s.terminal {
		return Completion{}, &ProviderError{Code: "STREAM_CLOSED", Message: "Bedrock stream ended before messageStop"}
	}
	if len(s.calls) > 0 {
		s.finish = "tool_calls"
	}
	if s.finish == "" {
		s.finish = "stop"
	}
	if err := s.onDelta(Delta{Finish: s.finish}); err != nil {
		return Completion{}, err
	}
	if s.text == "" && s.reasoning == "" && len(s.calls) == 0 {
		return Completion{}, &ProviderError{Code: "EMPTY_RESPONSE", Message: "model returned a completed response with no content"}
	}
	return Completion{Text: s.text, Reasoning: s.reasoning, ReasoningSignature: s.reasoningSignature, ToolCalls: s.calls, Usage: s.usage, Finish: s.finish}, nil
}
