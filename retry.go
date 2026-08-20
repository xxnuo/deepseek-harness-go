package harness

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net"
	"sort"
	"strings"
	"time"
)

// RetryPolicy is the request-recovery policy for one provider route.
// A zero policy means the normal upstream defaults. Set Mode explicitly when
// intentionally selecting a zero-retry normal policy.
type RetryPolicy struct {
	Mode           string
	MaxRetries     int
	RetryableCodes []string
	InitialDelay   time.Duration
	MaxDelay       time.Duration
	JitterRatio    float64
}

// RetryPolicyProvider is an optional provider extension. Providers that do
// not implement it use Config.RetryPolicy.
type RetryPolicyProvider interface {
	RetryPolicy() RetryPolicy
}

// ProviderRetryPolicy is the descriptive alias used by SDK integrations.
type ProviderRetryPolicy = RetryPolicyProvider

const (
	RetryNormal = "normal"
	RetryAlways = "always"
)

// ErrEngineClosed is returned when a request races Engine.Close.
var ErrEngineClosed = errors.New("engine closed")

// LlmFailure is the serializable failure vocabulary used by retry events.
type LlmFailure struct {
	Message            string        `json:"message"`
	Code               string        `json:"code"`
	Status             int           `json:"status,omitempty"`
	ProviderRetryAfter time.Duration `json:"-"`
	RequestID          string        `json:"requestId,omitempty"`
}

// LlmRequestError carries stable provider facts without forcing callers to
// parse a diagnostic string.
type LlmRequestError struct {
	Failure LlmFailure
	Err     error
}

func NewLlmRequestError(failure LlmFailure, cause error) *LlmRequestError {
	if failure.Message == "" && cause != nil {
		failure.Message = cause.Error()
	}
	return &LlmRequestError{Failure: failure, Err: cause}
}

func (e *LlmRequestError) Error() string {
	if e == nil {
		return ""
	}
	if e.Failure.Message != "" {
		return e.Failure.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Failure.Code
}

func (e *LlmRequestError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ProviderError is retained as a compact adapter-facing constructor. New
// providers may use LlmRequestError instead.
type ProviderError struct {
	Code       string
	Message    string
	Status     int
	RetryAfter time.Duration
	RequestID  string
	Err        error
}

func (e *ProviderError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Code
}

func (e *ProviderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func defaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Mode:           RetryNormal,
		MaxRetries:     2,
		RetryableCodes: []string{"EMPTY_RESPONSE", "RATE_LIMIT", "SERVER", "TIMEOUT", "TRANSPORT"},
		InitialDelay:   500 * time.Millisecond,
		MaxDelay:       10 * time.Second,
		JitterRatio:    0.1,
	}
}

func retryPolicyIsZero(policy RetryPolicy) bool {
	return policy.Mode == "" && policy.MaxRetries == 0 && len(policy.RetryableCodes) == 0 &&
		policy.InitialDelay == 0 && policy.MaxDelay == 0 && policy.JitterRatio == 0
}

func normalizeRetryPolicy(policy RetryPolicy) RetryPolicy {
	if retryPolicyIsZero(policy) {
		policy = defaultRetryPolicy()
	} else {
		if policy.Mode == "" {
			policy.Mode = RetryNormal
		}
		if policy.Mode != RetryNormal && policy.Mode != RetryAlways {
			policy.Mode = RetryNormal
		}
		// MaxRetries == 0 is meaningful when Mode is explicit (disable retry).
		if policy.Mode == RetryAlways {
			policy.MaxRetries = 0
		}
		if policy.RetryableCodes == nil {
			policy.RetryableCodes = append([]string(nil), defaultRetryPolicy().RetryableCodes...)
		}
		if policy.InitialDelay <= 0 {
			policy.InitialDelay = defaultRetryPolicy().InitialDelay
		}
		if policy.MaxDelay <= 0 {
			policy.MaxDelay = defaultRetryPolicy().MaxDelay
		}
	}
	if policy.MaxRetries < 0 {
		policy.MaxRetries = 0
	}
	if policy.InitialDelay > policy.MaxDelay {
		policy.InitialDelay = policy.MaxDelay
	}
	if policy.JitterRatio < 0 {
		policy.JitterRatio = 0
	}
	if policy.JitterRatio > 1 {
		policy.JitterRatio = 1
	}
	policy.RetryableCodes = append([]string(nil), policy.RetryableCodes...)
	for index := range policy.RetryableCodes {
		policy.RetryableCodes[index] = strings.ToUpper(strings.TrimSpace(policy.RetryableCodes[index]))
	}
	return policy
}

func retryFailure(err error) LlmFailure {
	if err == nil {
		return LlmFailure{}
	}
	var requestErr *LlmRequestError
	if errors.As(err, &requestErr) {
		failure := requestErr.Failure
		if failure.Message == "" {
			failure.Message = err.Error()
		}
		if failure.Code == "" && requestErr.Err != nil {
			inferred := retryFailure(requestErr.Err)
			if inferred.Code != "" {
				failure.Code = inferred.Code
			}
		}
		return failure
	}
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		failure := LlmFailure{
			Message:            providerErr.Error(),
			Code:               strings.ToUpper(strings.TrimSpace(providerErr.Code)),
			Status:             providerErr.Status,
			ProviderRetryAfter: providerErr.RetryAfter,
			RequestID:          providerErr.RequestID,
		}
		if failure.Code == "" && providerErr.Err != nil {
			inferred := retryFailure(providerErr.Err)
			failure.Code = inferred.Code
		}
		return failure
	}

	failure := LlmFailure{Message: err.Error()}
	if errors.Is(err, context.DeadlineExceeded) {
		failure.Code = "TIMEOUT"
	}
	text := strings.ToUpper(failure.Message)
	for _, code := range []string{"EMPTY_RESPONSE", "RATE_LIMIT", "TIMEOUT", "TRANSPORT", "STREAM_CLOSED", "SERVER", "AUTH"} {
		if failure.Code != "" {
			break
		}
		if strings.Contains(text, code) {
			failure.Code = code
			break
		}
	}
	if failure.Code == "" && errors.Is(err, io.EOF) {
		failure.Code = "STREAM_CLOSED"
	}
	if failure.Code == "" {
		var netErr net.Error
		if errors.As(err, &netErr) {
			failure.Code = "TRANSPORT"
		} else {
			failure.Code = "PROVIDER"
		}
	}
	return failure
}

func retryCode(err error) string { return retryFailure(err).Code }

func retryAfter(err error) time.Duration { return retryFailure(err).ProviderRetryAfter }

func retryEligible(policy RetryPolicy, code string, retry int) bool {
	if policy.Mode == RetryAlways {
		return true
	}
	if retry <= 0 || retry > policy.MaxRetries || code == "" {
		return false
	}
	for _, candidate := range policy.RetryableCodes {
		if strings.EqualFold(candidate, code) {
			return true
		}
	}
	return false
}

// retryDelay returns the local/provider-selected delay. The compatibility
// wrapper keeps the original helper signature used by package-level tests.
func retryDelay(policy RetryPolicy, retry int, providerDelay time.Duration) time.Duration {
	delay, _ := retryDelayDecision(policy, retry, providerDelay)
	return delay
}

// retryDelayDecision also reports whether a normal policy should delegate an
// over-cap provider Retry-After instead of scheduling a local retry.
func retryDelayDecision(policy RetryPolicy, retry int, providerDelay time.Duration) (time.Duration, bool) {
	if providerDelay > 0 {
		if providerDelay <= policy.MaxDelay {
			return providerDelay, false
		}
		if policy.Mode == RetryNormal {
			return 0, true
		}
	}
	delay := policy.InitialDelay
	for index := 1; index < retry && delay < policy.MaxDelay; index++ {
		if delay > policy.MaxDelay/2 {
			delay = policy.MaxDelay
			break
		}
		delay *= 2
	}
	if delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}
	if policy.JitterRatio > 0 {
		factor := 1 - policy.JitterRatio + 2*policy.JitterRatio*rand.Float64()
		delay = time.Duration(float64(delay) * factor)
		if delay > policy.MaxDelay {
			delay = policy.MaxDelay
		}
	}
	return delay, false
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryPolicyKey(policy RetryPolicy) string {
	policy = normalizeRetryPolicy(policy)
	codes := append([]string(nil), policy.RetryableCodes...)
	sort.Strings(codes)
	if policy.Mode == RetryAlways {
		data, _ := json.Marshal([]any{policy.Mode, durationMillis(policy.InitialDelay), durationMillis(policy.MaxDelay), policy.JitterRatio})
		return string(data)
	}
	data, _ := json.Marshal([]any{policy.Mode, policy.MaxRetries, codes, durationMillis(policy.InitialDelay), durationMillis(policy.MaxDelay), policy.JitterRatio})
	return string(data)
}

func durationMillis(value time.Duration) int64 { return int64(value / time.Millisecond) }

func retryFailurePayload(failure LlmFailure) map[string]any {
	if failure.Message == "" {
		failure.Message = "provider request failed"
	}
	if failure.Code == "" {
		failure.Code = "PROVIDER"
	}
	payload := map[string]any{"message": failure.Message, "code": failure.Code}
	if failure.Status > 0 {
		payload["status"] = failure.Status
	}
	if failure.ProviderRetryAfter > 0 {
		payload["providerRetryAfterMs"] = durationMillis(failure.ProviderRetryAfter)
	}
	if failure.RequestID != "" {
		payload["requestId"] = failure.RequestID
	}
	return payload
}

// successfulAttemptChunkSeqs excludes chunks emitted before the latest
// retry-started boundary. Failed streams remain inspectable in the log, but
// only the successful attempt is linked to the assistant message.
func successfulAttemptChunkSeqs(session *Session, turn, step, stepStartSeq int) []int {
	if session == nil {
		return nil
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	boundary := stepStartSeq
	for _, event := range events {
		if event.Seq <= stepStartSeq {
			continue
		}
		data, ok := event.Data.(map[string]any)
		if !ok {
			continue
		}
		if event.Type == "llm/retry-started" && eventInt(data["turn"]) == turn && eventInt(data["step"]) == step ||
			event.Type == "compaction/end" && eventInt(data["turn"]) == turn {
			boundary = event.Seq
		}
	}
	seqs := make([]int, 0)
	for _, event := range events {
		if event.Seq <= boundary || event.Type != "assistant/chunk" {
			continue
		}
		data, ok := event.Data.(map[string]any)
		if !ok || eventInt(data["turn"]) != turn || eventInt(data["step"]) != step {
			continue
		}
		seqs = append(seqs, event.Seq)
	}
	return seqs
}

func eventInt(value any) int {
	switch number := value.(type) {
	case int:
		return number
	case int8:
		return int(number)
	case int16:
		return int(number)
	case int32:
		return int(number)
	case int64:
		return int(number)
	case uint:
		return int(number)
	case uint8:
		return int(number)
	case uint16:
		return int(number)
	case uint32:
		return int(number)
	case uint64:
		return int(number)
	case float64:
		return int(number)
	default:
		return -1
	}
}

func (e *Engine) retryPolicyFor(provider Provider) RetryPolicy {
	if provider != nil {
		if configured, ok := provider.(RetryPolicyProvider); ok {
			return normalizeRetryPolicy(configured.RetryPolicy())
		}
		e.mu.RLock()
		configured, ok := e.retryPolicies[provider.ID()]
		e.mu.RUnlock()
		if ok {
			return normalizeRetryPolicy(configured)
		}
	}
	return normalizeRetryPolicy(e.cfg.RetryPolicy)
}

// completeWithRetry keeps all attempts inside one step. The callback is
// invoked for every streamed delta, including failed attempts; callers must
// only use the returned completion to build a durable assistant message.
func (e *Engine) completeWithRetry(ctx context.Context, provider Provider, request ChatRequest, session *Session, turn, step int) (Completion, []Delta, error) {
	return e.completeWithRetrySink(ctx, provider, request, session, turn, step, nil)
}

func cloneContentBlocks(blocks []ContentBlock) []ContentBlock {
	cloned := append([]ContentBlock(nil), blocks...)
	for index := range cloned {
		cloned[index].Content = cloneContentBlocks(cloned[index].Content)
		if cloned[index].Attachment != nil {
			attachment := *cloned[index].Attachment
			cloned[index].Attachment = &attachment
		}
	}
	return cloned
}

func cloneChatRequest(request ChatRequest) ChatRequest {
	clone := request
	if request.Temperature != nil {
		temperature := *request.Temperature
		clone.Temperature = &temperature
	}
	clone.Stop = append([]string(nil), request.Stop...)
	clone.Messages = append([]ChatMessage(nil), request.Messages...)
	for index := range clone.Messages {
		clone.Messages[index].Blocks = cloneContentBlocks(clone.Messages[index].Blocks)
		clone.Messages[index].Images = append([]ChatImage(nil), clone.Messages[index].Images...)
		clone.Messages[index].ToolCalls = append([]ToolCall(nil), clone.Messages[index].ToolCalls...)
		for callIndex := range clone.Messages[index].ToolCalls {
			clone.Messages[index].ToolCalls[callIndex].Arguments = append(json.RawMessage(nil), clone.Messages[index].ToolCalls[callIndex].Arguments...)
		}
	}
	clone.Tools = append([]ToolSchema(nil), request.Tools...)
	for index := range clone.Tools {
		if parameters, ok := cloneJSON(clone.Tools[index].Parameters).(map[string]any); ok {
			clone.Tools[index].Parameters = parameters
		}
	}
	return clone
}

func (e *Engine) completeWithRetrySink(ctx context.Context, provider Provider, request ChatRequest, session *Session, turn, step int, onDelta func(Delta) error) (Completion, []Delta, error) {
	if provider == nil {
		return Completion{}, nil, errors.New("model-unavailable")
	}
	if e.invariants != nil && session != nil {
		if err := e.invariants.ValidateAgentRequest(e, session, request); err != nil {
			return Completion{}, nil, err
		}
	}
	policy := e.retryPolicyFor(provider)
	policyKey := retryPolicyKey(policy)
	retry := 0
	retryID := newID("retry")
	for {
		if err := ctx.Err(); err != nil {
			return Completion{}, nil, err
		}
		e.mu.RLock()
		closed := e.closed
		e.mu.RUnlock()
		if closed {
			return Completion{}, nil, ErrEngineClosed
		}
		deltas := make([]Delta, 0, 8)
		completion, err := provider.Complete(ctx, cloneChatRequest(request), func(delta Delta) error {
			deltas = append(deltas, delta)
			if onDelta != nil {
				return onDelta(delta)
			}
			return nil
		})
		if err == nil && completion.Text == "" && completion.Reasoning == "" && len(completion.ToolCalls) == 0 {
			err = &ProviderError{Code: "EMPTY_RESPONSE", Message: "model returned a completed response with no content"}
		}
		if err == nil {
			return completion, deltas, nil
		}
		// A caller cancellation/deadline is authoritative even when the
		// provider wrapped it in a retry-looking transport error.
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return Completion{}, nil, err
		}
		e.mu.RLock()
		closed = e.closed
		e.mu.RUnlock()
		if closed {
			return Completion{}, nil, ErrEngineClosed
		}
		failure := retryFailure(err)
		next := retry + 1
		if !retryEligible(policy, failure.Code, next) {
			return Completion{}, nil, err
		}
		delay, delegated := retryDelayDecision(policy, next, failure.ProviderRetryAfter)
		if delegated {
			return Completion{}, nil, err
		}
		retry = next
		if session != nil {
			data := map[string]any{
				"retryId": retryID, "turn": turn, "step": step,
				"provider": provider.ID(), "mode": policy.Mode,
				"policyKey": policyKey, "retry": retry,
				"delayMs": durationMillis(delay), "failure": retryFailurePayload(failure),
			}
			if policy.Mode == RetryNormal {
				data["maxRetries"] = policy.MaxRetries
			}
			if _, appendErr := e.appendEvent(session, "llm/retry", data); appendErr != nil {
				return Completion{}, nil, appendErr
			}
		}
		if err := waitRetry(ctx, delay); err != nil {
			return Completion{}, nil, err
		}
		if session != nil {
			if _, appendErr := e.appendEvent(session, "llm/retry-started", map[string]any{
				"retryId": retryID, "turn": turn, "step": step, "retry": retry,
			}); appendErr != nil {
				return Completion{}, nil, appendErr
			}
		}
	}
}

func retryError(code, message string) error {
	return &ProviderError{Code: code, Message: message}
}
