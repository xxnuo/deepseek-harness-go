package harness

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
)

type AuthorizationMethod struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type AuthorizationNotice struct {
	Message string `json:"message"`
	URL     string `json:"url,omitempty"`
	Code    string `json:"code,omitempty"`
}

type AuthorizationPromptOption struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

type AuthorizationPromptKind string

const (
	AuthorizationPromptText   AuthorizationPromptKind = "text"
	AuthorizationPromptSecret AuthorizationPromptKind = "secret"
	AuthorizationPromptSelect AuthorizationPromptKind = "select"
)

type AuthorizationPrompt struct {
	Kind        AuthorizationPromptKind     `json:"kind"`
	Message     string                      `json:"message"`
	Placeholder string                      `json:"placeholder,omitempty"`
	Options     []AuthorizationPromptOption `json:"options,omitempty"`
	Context     context.Context             `json:"-"`
}

type AuthorizationInteraction struct {
	Notify func(AuthorizationNotice)
	Prompt func(context.Context, AuthorizationPrompt) (string, error)
}

type AuthorizationSession struct {
	Method  string
	Context context.Context
	Notify  func(AuthorizationNotice)
	Prompt  func(AuthorizationPrompt) (string, error)
}

type AuthorizationFlow struct {
	Key     CredentialKey
	Label   string
	Methods []AuthorizationMethod
	Run     func(AuthorizationSession) error
}

type AuthorizationRequest struct {
	Key         CredentialKey
	Method      string
	Interaction AuthorizationInteraction
}

type AuthorizationStatus string

const (
	AuthorizationAuthorized AuthorizationStatus = "authorized"
	AuthorizationCancelled  AuthorizationStatus = "cancelled"
)

type AuthorizationSettlement string

const (
	AuthorizationSettledAuthorized AuthorizationSettlement = "authorized"
	AuthorizationSettledCancelled  AuthorizationSettlement = "cancelled"
	AuthorizationSettledFailed     AuthorizationSettlement = "failed"
)

type AuthorizationOutcome struct {
	Status AuthorizationStatus `json:"status"`
}

type AuthorizationEntry struct {
	Key      CredentialKey         `json:"key"`
	Label    string                `json:"label"`
	Methods  []AuthorizationMethod `json:"methods"`
	InFlight bool                  `json:"inFlight"`
}

const (
	AuthorizationDuplicateFlow  = "DUPLICATE_FLOW"
	AuthorizationNoFlow         = "NO_FLOW"
	AuthorizationUnknownMethod  = "UNKNOWN_METHOD"
	AuthorizationAlreadyRunning = "ALREADY_IN_FLIGHT"
	AuthorizationNotCommitted   = "NOT_COMMITTED"
	AuthorizationInvalidFlow    = "INVALID_FLOW"
	AuthorizationClosed         = "CLOSED"
	AuthorizationDeclined       = "DECLINED"
)

type AuthorizationError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Cause   error  `json:"-"`
}

func (err *AuthorizationError) Error() string {
	if err == nil {
		return ""
	}
	return err.Message
}

func (err *AuthorizationError) Unwrap() error { return err.Cause }

type AuthorizationDeclinedError struct {
	Message string
	Cause   error
}

func (err *AuthorizationDeclinedError) Error() string {
	if err == nil || strings.TrimSpace(err.Message) == "" {
		return "the authorization prompt was declined"
	}
	return err.Message
}

func (err *AuthorizationDeclinedError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

func IsAuthorizationError(err error, code string) bool {
	var authorizationErr *AuthorizationError
	if errors.As(err, &authorizationErr) && authorizationErr.Code == code {
		return true
	}
	var declined *AuthorizationDeclinedError
	return code == AuthorizationDeclined && errors.As(err, &declined)
}

type AuthorizationSettlementListener func(CredentialKey, AuthorizationSettlement) error

type authorizationRegistration struct {
	flow AuthorizationFlow
	id   uint64
}

type authorizationAttempt struct {
	cancel context.CancelCauseFunc
}

type AuthorizationService struct {
	credentials *CredentialService

	mu        sync.RWMutex
	flows     map[CredentialKey]*authorizationRegistration
	order     []CredentialKey
	running   map[CredentialKey]*authorizationAttempt
	listeners map[uint64]AuthorizationSettlementListener
	nextID    uint64
	closed    bool
}

func newAuthorizationService(credentials *CredentialService) *AuthorizationService {
	return &AuthorizationService{
		credentials: credentials,
		flows:       map[CredentialKey]*authorizationRegistration{},
		running:     map[CredentialKey]*authorizationAttempt{},
		listeners:   map[uint64]AuthorizationSettlementListener{},
	}
}

func (service *AuthorizationService) RegisterFlow(flow AuthorizationFlow) (func(), error) {
	if _, err := ParseCredentialKey(string(flow.Key)); err != nil {
		return nil, &AuthorizationError{Code: AuthorizationInvalidFlow, Message: err.Error(), Cause: err}
	}
	if strings.TrimSpace(flow.Label) == "" || len(flow.Methods) == 0 || flow.Run == nil {
		return nil, &AuthorizationError{Code: AuthorizationInvalidFlow, Message: "an authorization flow requires a label, at least one method, and a runner"}
	}
	copy := flow
	copy.Methods = append([]AuthorizationMethod(nil), flow.Methods...)
	for _, method := range copy.Methods {
		if strings.TrimSpace(method.ID) == "" || strings.TrimSpace(method.Label) == "" {
			return nil, &AuthorizationError{Code: AuthorizationInvalidFlow, Message: "authorization methods require non-empty ids and labels"}
		}
	}

	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return nil, &AuthorizationError{Code: AuthorizationClosed, Message: "authorization service is closed"}
	}
	if service.flows[flow.Key] != nil {
		service.mu.Unlock()
		return nil, &AuthorizationError{
			Code:    AuthorizationDuplicateFlow,
			Message: fmt.Sprintf("an authorization flow for %q is already registered", flow.Key),
		}
	}
	service.nextID++
	registration := &authorizationRegistration{flow: copy, id: service.nextID}
	service.flows[flow.Key] = registration
	service.order = append(service.order, flow.Key)
	service.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			service.mu.Lock()
			if service.flows[flow.Key] != registration {
				service.mu.Unlock()
				return
			}
			delete(service.flows, flow.Key)
			service.removeFlowOrderLocked(flow.Key)
			attempt := service.running[flow.Key]
			service.mu.Unlock()
			if attempt != nil {
				attempt.cancel(context.Canceled)
			}
		})
	}, nil
}

func (service *AuthorizationService) removeFlowOrderLocked(key CredentialKey) {
	for index, candidate := range service.order {
		if candidate == key {
			service.order = append(service.order[:index], service.order[index+1:]...)
			return
		}
	}
}

func (service *AuthorizationService) List() []AuthorizationEntry {
	service.mu.RLock()
	entries := make([]AuthorizationEntry, 0, len(service.order))
	for _, key := range service.order {
		if registration := service.flows[key]; registration != nil {
			entries = append(entries, service.entryLocked(registration))
		}
	}
	service.mu.RUnlock()
	return entries
}

func (service *AuthorizationService) Describe(key CredentialKey) *AuthorizationEntry {
	service.mu.RLock()
	registration := service.flows[key]
	if registration == nil {
		service.mu.RUnlock()
		return nil
	}
	entry := service.entryLocked(registration)
	service.mu.RUnlock()
	return &entry
}

func (service *AuthorizationService) entryLocked(registration *authorizationRegistration) AuthorizationEntry {
	return AuthorizationEntry{
		Key:      registration.flow.Key,
		Label:    registration.flow.Label,
		Methods:  append([]AuthorizationMethod(nil), registration.flow.Methods...),
		InFlight: service.running[registration.flow.Key] != nil,
	}
}

func (service *AuthorizationService) Cancel(key CredentialKey) {
	service.mu.RLock()
	attempt := service.running[key]
	service.mu.RUnlock()
	if attempt != nil {
		attempt.cancel(context.Canceled)
	}
}

func (service *AuthorizationService) Subscribe(listener AuthorizationSettlementListener) func() {
	if listener == nil {
		return func() {}
	}
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return func() {}
	}
	service.nextID++
	id := service.nextID
	service.listeners[id] = listener
	service.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			service.mu.Lock()
			delete(service.listeners, id)
			service.mu.Unlock()
		})
	}
}

func (service *AuthorizationService) Begin(ctx context.Context, request AuthorizationRequest) (AuthorizationOutcome, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return AuthorizationOutcome{}, &AuthorizationError{Code: AuthorizationClosed, Message: "authorization service is closed"}
	}
	registration := service.flows[request.Key]
	if registration == nil {
		service.mu.Unlock()
		return AuthorizationOutcome{}, &AuthorizationError{
			Code:    AuthorizationNoFlow,
			Message: fmt.Sprintf("no authorization flow is registered for %q", request.Key),
		}
	}
	method := request.Method
	if method == "" {
		method = registration.flow.Methods[0].ID
	}
	if !authorizationOffersMethod(registration.flow.Methods, method) {
		service.mu.Unlock()
		return AuthorizationOutcome{}, &AuthorizationError{
			Code:    AuthorizationUnknownMethod,
			Message: fmt.Sprintf("authorization flow for %q offers no method %q", request.Key, method),
		}
	}
	if service.running[request.Key] != nil {
		service.mu.Unlock()
		return AuthorizationOutcome{}, &AuthorizationError{
			Code:    AuthorizationAlreadyRunning,
			Message: fmt.Sprintf("an authorization attempt for %q is already running", request.Key),
		}
	}
	if ctx.Err() != nil {
		service.mu.Unlock()
		return AuthorizationOutcome{Status: AuthorizationCancelled}, nil
	}
	attemptCtx, cancel := context.WithCancelCause(context.Background())
	attempt := &authorizationAttempt{cancel: cancel}
	service.running[request.Key] = attempt
	service.mu.Unlock()

	stopForwarding := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	settlement := AuthorizationSettledFailed
	outcome, err := service.attempt(attemptCtx, registration.flow, method, request.Interaction)
	if err == nil {
		if outcome.Status == AuthorizationAuthorized {
			settlement = AuthorizationSettledAuthorized
		} else {
			settlement = AuthorizationSettledCancelled
		}
	}
	stopForwarding()
	cancel(context.Canceled)

	service.mu.Lock()
	if service.running[request.Key] == attempt {
		delete(service.running, request.Key)
	}
	service.mu.Unlock()
	if settleErr := service.settle(request.Key, settlement); settleErr != nil {
		return AuthorizationOutcome{}, settleErr
	}
	return outcome, err
}

func authorizationOffersMethod(methods []AuthorizationMethod, id string) bool {
	for _, method := range methods {
		if method.ID == id {
			return true
		}
	}
	return false
}

type authorizationRunResult struct {
	err error
}

func (service *AuthorizationService) attempt(
	ctx context.Context,
	flow AuthorizationFlow,
	method string,
	interaction AuthorizationInteraction,
) (AuthorizationOutcome, error) {
	initialGeneration := service.credentials.recordGeneration(flow.Key)
	var declined atomic.Bool
	session := AuthorizationSession{
		Method:  method,
		Context: ctx,
		Notify: func(notice AuthorizationNotice) {
			if interaction.Notify == nil {
				return
			}
			if panicValue := callAuthorizationNotify(interaction.Notify, notice); panicValue != nil {
				log.Printf("deepseek-harness: authorization interaction failed to render a notice: %v", panicValue)
			}
		},
		Prompt: func(prompt AuthorizationPrompt) (string, error) {
			if interaction.Prompt == nil {
				declined.Store(true)
				return "", &AuthorizationDeclinedError{}
			}
			promptCtx := ctx
			if prompt.Context != nil {
				var cancel context.CancelFunc
				promptCtx, cancel = authorizationPromptContext(ctx, prompt.Context)
				defer cancel()
			}
			answer, err := interaction.Prompt(promptCtx, prompt)
			var decline *AuthorizationDeclinedError
			if errors.As(err, &decline) {
				declined.Store(true)
			}
			return answer, err
		},
	}
	result := make(chan authorizationRunResult, 1)
	go func() {
		result <- authorizationRunResult{err: callAuthorizationFlow(flow.Run, session)}
	}()

	select {
	case <-ctx.Done():
		return AuthorizationOutcome{Status: AuthorizationCancelled}, nil
	case completed := <-result:
		if completed.err != nil {
			if ctx.Err() != nil || declined.Load() {
				return AuthorizationOutcome{Status: AuthorizationCancelled}, nil
			}
			return AuthorizationOutcome{}, completed.err
		}
	}
	if ctx.Err() != nil {
		return AuthorizationOutcome{Status: AuthorizationCancelled}, nil
	}
	if service.credentials.recordGeneration(flow.Key) <= initialGeneration {
		return AuthorizationOutcome{}, &AuthorizationError{
			Code:    AuthorizationNotCommitted,
			Message: fmt.Sprintf("authorization flow for %q resolved without committing a credential record in this attempt", flow.Key),
		}
	}
	info, err := service.credentials.DescribeRecord(flow.Key)
	if err != nil {
		return AuthorizationOutcome{}, err
	}
	if !info.Configured {
		return AuthorizationOutcome{}, &AuthorizationError{
			Code:    AuthorizationNotCommitted,
			Message: fmt.Sprintf("authorization flow for %q deleted its credential record instead of committing one", flow.Key),
		}
	}
	return AuthorizationOutcome{Status: AuthorizationAuthorized}, nil
}

func callAuthorizationFlow(run func(AuthorizationSession) error, session AuthorizationSession) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("authorization flow panicked: %v\n%s", recovered, debug.Stack())
		}
	}()
	return run(session)
}

func callAuthorizationNotify(notify func(AuthorizationNotice), notice AuthorizationNotice) (recovered any) {
	defer func() { recovered = recover() }()
	notify(notice)
	return nil
}

func authorizationPromptContext(attempt, prompt context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(attempt)
	stop := context.AfterFunc(prompt, func() { cancel(context.Cause(prompt)) })
	return ctx, func() {
		stop()
		cancel(context.Canceled)
	}
}

func (service *AuthorizationService) settle(key CredentialKey, settlement AuthorizationSettlement) error {
	service.mu.RLock()
	listeners := make([]AuthorizationSettlementListener, 0, len(service.listeners))
	for id := uint64(1); id <= service.nextID; id++ {
		if listener := service.listeners[id]; listener != nil {
			listeners = append(listeners, listener)
		}
	}
	service.mu.RUnlock()
	var invariantErr error
	for _, listener := range listeners {
		err := callAuthorizationSettlementListener(listener, key, settlement)
		if err == nil {
			continue
		}
		var typed *InvariantError
		if errors.As(err, &typed) && typed.Code == InvariantErrorCode {
			if invariantErr == nil {
				invariantErr = err
			}
			continue
		}
		log.Printf("deepseek-harness: authorization/settled listener for %q failed: %v", key, err)
	}
	return invariantErr
}

func callAuthorizationSettlementListener(
	listener AuthorizationSettlementListener,
	key CredentialKey,
	settlement AuthorizationSettlement,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredErr, ok := recovered.(error); ok {
				err = recoveredErr
			} else {
				err = fmt.Errorf("authorization/settled listener panicked: %v", recovered)
			}
		}
	}()
	return listener(key, settlement)
}

func (service *AuthorizationService) close() {
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return
	}
	service.closed = true
	attempts := make([]*authorizationAttempt, 0, len(service.running))
	for _, attempt := range service.running {
		attempts = append(attempts, attempt)
	}
	service.flows = map[CredentialKey]*authorizationRegistration{}
	service.order = nil
	service.listeners = map[uint64]AuthorizationSettlementListener{}
	service.mu.Unlock()
	for _, attempt := range attempts {
		attempt.cancel(context.Canceled)
	}
}
