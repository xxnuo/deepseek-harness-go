package harness

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	collectorl "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type SessionTelemetryMode string

const (
	SessionTelemetryModeFull         SessionTelemetryMode = "FULL"
	SessionTelemetryModeFeedbackOnly SessionTelemetryMode = "FEEDBACK_ONLY"
	SessionTelemetryModeDisabled     SessionTelemetryMode = "DISABLED"
)

type SessionTelemetrySharingStatus string

const (
	SessionTelemetrySharingFull         SessionTelemetrySharingStatus = "full"
	SessionTelemetrySharingFeedbackOnly SessionTelemetrySharingStatus = "feedback-only"
	SessionTelemetrySharingDisabled     SessionTelemetrySharingStatus = "disabled"
)

type SessionTelemetrySeverity string

const (
	SessionTelemetrySeverityInfo  SessionTelemetrySeverity = "info"
	SessionTelemetrySeverityWarn  SessionTelemetrySeverity = "warn"
	SessionTelemetrySeverityError SessionTelemetrySeverity = "error"
)

type SessionTelemetryChannel string

const (
	SessionTelemetryChannelLedger SessionTelemetryChannel = "ledger"
	SessionTelemetryChannelOps    SessionTelemetryChannel = "ops"
)

type SessionTelemetryRecord struct {
	Channel    SessionTelemetryChannel
	Time       int64
	Severity   SessionTelemetrySeverity
	Attributes map[string]any
	Body       any
}

// SessionTelemetrySink is the reusable delivery seam for custom Go hosts.
// Implementations may enqueue records, but must not retain mutable references
// from a record without copying them.
type SessionTelemetrySink interface {
	Emit(context.Context, SessionTelemetryRecord) error
	Shutdown(context.Context) error
}

type SessionTelemetryOTLPExporterConfig struct {
	URL              string
	Headers          map[string]string
	Compression      string
	Timeout          time.Duration
	KeepAlive        *bool
	ConcurrencyLimit int
	UserAgent        string
	HTTPClient       *http.Client
}

type SessionTelemetryProcessorConfig struct {
	ScheduledDelay     time.Duration
	MaxQueueSize       int
	MaxExportBatchSize int
	ExportTimeout      time.Duration
}

type SessionTelemetryConfig struct {
	Mode            SessionTelemetryMode
	Exporter        SessionTelemetryOTLPExporterConfig
	Processor       SessionTelemetryProcessorConfig
	ShutdownTimeout time.Duration
	Sink            SessionTelemetrySink
	Redact          func(SessionTelemetryRecord) (SessionTelemetryRecord, error)
	OnError         func(error)
}

func equivalentSessionTelemetryConfig(a, b *SessionTelemetryConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Mode != b.Mode || a.ShutdownTimeout != b.ShutdownTimeout || !sameTelemetrySink(a.Sink, b.Sink) {
		return false
	}
	if a.Exporter.URL != b.Exporter.URL || a.Exporter.Compression != b.Exporter.Compression || a.Exporter.Timeout != b.Exporter.Timeout || a.Exporter.ConcurrencyLimit != b.Exporter.ConcurrencyLimit || a.Exporter.UserAgent != b.Exporter.UserAgent || a.Exporter.HTTPClient != b.Exporter.HTTPClient {
		return false
	}
	if (a.Exporter.KeepAlive == nil) != (b.Exporter.KeepAlive == nil) || a.Exporter.KeepAlive != nil && *a.Exporter.KeepAlive != *b.Exporter.KeepAlive {
		return false
	}
	if len(a.Exporter.Headers) != len(b.Exporter.Headers) {
		return false
	}
	for key, value := range a.Exporter.Headers {
		if b.Exporter.Headers[key] != value {
			return false
		}
	}
	return a.Processor == b.Processor
}

// Interface equality panics when the dynamic concrete value is not
// comparable (for example, a named slice implementing SessionTelemetrySink).
// Such sinks cannot be proven identical across a reload, so conservatively
// treat them as changed and let the replacement lifecycle handle them.
func sameTelemetrySink(a, b SessionTelemetrySink) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) || !reflect.TypeOf(a).Comparable() {
		return false
	}
	return a == b
}

const defaultSessionTelemetryShutdownTimeout = 3 * time.Second

const (
	sessionTelemetryLedgerScope = "@deepseek-ai/dsh-session-telemetry-otel"
	sessionTelemetryOpsScope    = "@deepseek-ai/dsh-session-telemetry-otel/ops"
)

var (
	errSessionTelemetryDisabledFeedback = errors.New("OpenTelemetry session upload is DISABLED; this feedback is not uploaded through OpenTelemetry")
)

func normalizeSessionTelemetryConfig(config *SessionTelemetryConfig) *SessionTelemetryConfig {
	if config == nil {
		return nil
	}
	normalized := *config
	if normalized.Mode == "" {
		normalized.Mode = SessionTelemetryModeFeedbackOnly
	}
	if normalized.ShutdownTimeout == 0 {
		normalized.ShutdownTimeout = defaultSessionTelemetryShutdownTimeout
	}
	normalized.Exporter.Headers = cloneTelemetryHeaders(normalized.Exporter.Headers)
	return &normalized
}

func validateSessionTelemetryConfig(config *SessionTelemetryConfig) error {
	if config == nil {
		return nil
	}
	switch config.Mode {
	case SessionTelemetryModeFull, SessionTelemetryModeFeedbackOnly, SessionTelemetryModeDisabled:
	default:
		return fmt.Errorf("session-telemetry-otel: unsupported mode %q", config.Mode)
	}
	if config.Mode == SessionTelemetryModeDisabled {
		return nil
	}
	if config.ShutdownTimeout <= 0 {
		return fmt.Errorf("session-telemetry-otel: shutdownTimeoutMillis must be positive, got %s", config.ShutdownTimeout)
	}
	if config.Sink != nil {
		return nil
	}
	endpoint := strings.TrimSpace(config.Exporter.URL)
	if endpoint == "" {
		return errors.New("session-telemetry-otel: exporter.url is required (the full OTLP logs endpoint)")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("session-telemetry-otel: exporter.url is not a valid URL: %q", endpoint)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("session-telemetry-otel: exporter.url must be http(s), got %s", parsed.Scheme)
	}
	switch config.Exporter.Compression {
	case "", "none", "gzip":
	default:
		return fmt.Errorf("session-telemetry-otel: exporter.compression must be none or gzip, got %q", config.Exporter.Compression)
	}
	if config.Exporter.Timeout < 0 {
		return fmt.Errorf("session-telemetry-otel: exporter.timeoutMillis must be non-negative, got %s", config.Exporter.Timeout)
	}
	if config.Exporter.ConcurrencyLimit < 0 {
		return fmt.Errorf("session-telemetry-otel: exporter.concurrencyLimit must be non-negative, got %d", config.Exporter.ConcurrencyLimit)
	}
	if config.Processor.MaxExportBatchSize < 0 {
		return fmt.Errorf("session-telemetry-otel: processor.maxExportBatchSize must be a positive integer, got %d", config.Processor.MaxExportBatchSize)
	}
	if config.Processor.MaxQueueSize < 0 || config.Processor.ScheduledDelay < 0 || config.Processor.ExportTimeout < 0 {
		return errors.New("session-telemetry-otel: processor sizes and durations must be non-negative")
	}
	return nil
}

func sharingStatusForSessionTelemetry(mode SessionTelemetryMode) SessionTelemetrySharingStatus {
	switch mode {
	case SessionTelemetryModeFull:
		return SessionTelemetrySharingFull
	case SessionTelemetryModeFeedbackOnly:
		return SessionTelemetrySharingFeedbackOnly
	default:
		return SessionTelemetrySharingDisabled
	}
}

type sessionTelemetryCoordinator struct {
	mode            SessionTelemetryMode
	sharing         SessionTelemetrySharingStatus
	sink            SessionTelemetrySink
	redact          func(SessionTelemetryRecord) (SessionTelemetryRecord, error)
	onError         func(error)
	shutdownTimeout time.Duration

	mu        sync.Mutex
	cursor    map[string]int
	chunkSeen map[string]map[string]struct{}
	closed    bool
}

func newSessionTelemetryCoordinator(config *SessionTelemetryConfig, dataDir, version string) (*sessionTelemetryCoordinator, error) {
	if config == nil {
		return nil, nil
	}
	coordinator := &sessionTelemetryCoordinator{
		mode:            config.Mode,
		sharing:         sharingStatusForSessionTelemetry(config.Mode),
		redact:          config.Redact,
		onError:         config.OnError,
		shutdownTimeout: config.ShutdownTimeout,
		cursor:          map[string]int{},
		chunkSeen:       map[string]map[string]struct{}{},
	}
	if config.Mode == SessionTelemetryModeDisabled {
		return coordinator, nil
	}
	if config.Sink != nil {
		coordinator.sink = config.Sink
		return coordinator, nil
	}
	sink, err := newOTLPSessionTelemetrySink(*config, dataDir, version)
	if err != nil {
		return nil, err
	}
	coordinator.sink = sink
	return coordinator, nil
}

func (c *sessionTelemetryCoordinator) captureEvent(session *Session, event Event) {
	if c == nil {
		return
	}
	if c.mode == SessionTelemetryModeDisabled {
		if isFeedbackTelemetryEvent(session, event) {
			c.report(errSessionTelemetryDisabledFeedback)
		}
		return
	}
	if c.mode == SessionTelemetryModeFeedbackOnly {
		if isFeedbackTelemetryEvent(session, event) {
			c.captureSession(session, int(event.Seq))
		}
		return
	}
	header := sessionTelemetryHeader(session)
	c.mu.Lock()
	err := c.captureEventLocked(header, event)
	c.mu.Unlock()
	c.report(err)
}

func (c *sessionTelemetryCoordinator) captureSession(session *Session, throughSeq int) {
	header, _, events := sessionTelemetrySnapshot(session)
	c.mu.Lock()
	cursor, ok := c.cursor[header.ID]
	if !ok {
		cursor = -1
	}
	var errs []error
	for _, event := range events {
		if int(event.Seq) > throughSeq {
			break
		}
		if int(event.Seq) <= cursor {
			c.trackChunkLocked(header.ID, event)
			continue
		}
		if err := c.captureEventLocked(header, event); err != nil {
			errs = append(errs, err)
		}
	}
	c.mu.Unlock()
	for _, err := range errs {
		c.report(err)
	}
}

func isFeedbackTelemetryEvent(session *Session, event Event) bool {
	if int(event.Seq) < int(session.InheritedEventCount) {
		return false
	}
	if event.Type == "feedback/record" {
		return true
	}
	if event.Type != "feedback/message-put" && event.Type != "feedback/message-delete" {
		return false
	}
	data, _ := event.Data.(map[string]any)
	sessionID, _ := data["sessionId"].(string)
	return sessionID == session.Header.ID
}

func (c *sessionTelemetryCoordinator) trackSession(session *Session) {
	if c == nil {
		return
	}
	header, _, events := sessionTelemetrySnapshot(session)
	c.mu.Lock()
	for _, event := range events {
		c.trackChunkLocked(header.ID, event)
	}
	c.mu.Unlock()
}

func (c *sessionTelemetryCoordinator) trackEvent(session *Session, event Event) {
	if c == nil {
		return
	}
	header := sessionTelemetryHeader(session)
	c.mu.Lock()
	c.trackChunkLocked(header.ID, event)
	c.mu.Unlock()
}

func (c *sessionTelemetryCoordinator) captureEventLocked(header SessionHeader, event Event) (err error) {
	if c.closed || c.sink == nil {
		return nil
	}
	if event.Type == "assistant/chunk" {
		key, ok := sessionTelemetryChunkKey(event)
		if ok {
			seen := c.chunkSeen[header.ID]
			if seen == nil {
				seen = map[string]struct{}{}
				c.chunkSeen[header.ID] = seen
			}
			if _, exists := seen[key]; exists {
				return nil
			}
			seen[key] = struct{}{}
		}
	}
	body, err := cloneTelemetryValue(event.Data)
	if err != nil {
		return fmt.Errorf("telemetry: clone event %s/%d: %w", event.Type, event.Seq, err)
	}
	record := SessionTelemetryRecord{
		Channel:    SessionTelemetryChannelLedger,
		Time:       event.Time,
		Severity:   sessionTelemetryEventSeverity(event.Type, body),
		Attributes: sessionTelemetryEventAttributes(header, event),
		Body:       body,
	}
	record, err = c.applyRedaction(record)
	if err != nil {
		return err
	}
	if err := safeSessionTelemetryEmit(c.sink, context.Background(), record); err != nil {
		return fmt.Errorf("telemetry: emit event %s/%d: %w", event.Type, event.Seq, err)
	}
	c.cursor[header.ID] = int(event.Seq)
	return nil
}

func (c *sessionTelemetryCoordinator) trackChunkLocked(sessionID string, event Event) {
	key, ok := sessionTelemetryChunkKey(event)
	if !ok {
		return
	}
	seen := c.chunkSeen[sessionID]
	if seen == nil {
		seen = map[string]struct{}{}
		c.chunkSeen[sessionID] = seen
	}
	seen[key] = struct{}{}
}

func (c *sessionTelemetryCoordinator) emitDirect(ctx context.Context, record SessionTelemetryRecord) error {
	if c == nil || c.mode != SessionTelemetryModeFull || c.sink == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSessionTelemetryRecord(record); err != nil {
		return err
	}
	return safeSessionTelemetryEmit(c.sink, ctx, record)
}

func (c *sessionTelemetryCoordinator) relayAgentError(session *Session, turn, step int, failure error) {
	if c == nil || c.mode != SessionTelemetryModeFull || failure == nil {
		return
	}
	header := sessionTelemetryHeader(session)
	detail := sessionTelemetryErrorDetail(failure)
	record := SessionTelemetryRecord{
		Channel:  SessionTelemetryChannelOps,
		Time:     time.Now().UnixMilli(),
		Severity: SessionTelemetrySeverityError,
		Attributes: map[string]any{
			"telemetry.op": "agent-error",
			"session.id":   header.ID,
			"agent.id":     header.ID,
			"error.name":   detail["name"],
			"turn":         turn,
			"step":         step,
		},
		Body: detail,
	}
	c.mu.Lock()
	record, err := c.applyRedaction(record)
	if err == nil && !c.closed && c.sink != nil {
		err = safeSessionTelemetryEmit(c.sink, context.Background(), record)
	}
	c.mu.Unlock()
	if err != nil {
		c.report(fmt.Errorf("telemetry: emit agent error: %w", err))
	}
}

func (c *sessionTelemetryCoordinator) close(sessions []*Session) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	var errs []error
	if c.mode == SessionTelemetryModeFull && c.sink != nil {
		for _, session := range sessions {
			header := sessionTelemetryHeader(session)
			record := SessionTelemetryRecord{
				Channel:    SessionTelemetryChannelOps,
				Time:       time.Now().UnixMilli(),
				Severity:   SessionTelemetrySeverityInfo,
				Attributes: map[string]any{"telemetry.op": "shutdown", "session.id": header.ID},
				Body:       map[string]any{"op": "shutdown"},
			}
			record, err := c.applyRedaction(record)
			if err == nil {
				err = safeSessionTelemetryEmit(c.sink, context.Background(), record)
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("telemetry: emit shutdown for %s: %w", header.ID, err))
			}
		}
	}
	c.closed = true
	sink := c.sink
	timeout := c.shutdownTimeout
	c.mu.Unlock()
	for _, err := range errs {
		c.report(err)
	}
	if sink == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- safeSessionTelemetryShutdown(sink, ctx)
	}()
	select {
	case err := <-done:
		if err != nil {
			c.report(fmt.Errorf("telemetry: backend shutdown failed: %w", err))
		}
	case <-ctx.Done():
		c.report(fmt.Errorf("session-telemetry-otel: provider shutdown exceeded %s", timeout))
	}
}

func (c *sessionTelemetryCoordinator) applyRedaction(record SessionTelemetryRecord) (result SessionTelemetryRecord, err error) {
	if c.redact == nil {
		return record, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("telemetry: redaction panic: %v", recovered)
		}
	}()
	return c.redact(record)
}

func (c *sessionTelemetryCoordinator) report(err error) {
	if err == nil || c == nil || c.onError == nil {
		return
	}
	defer func() { _ = recover() }()
	c.onError(err)
}

func safeSessionTelemetryEmit(sink SessionTelemetrySink, ctx context.Context, record SessionTelemetryRecord) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("sink panic: %v", recovered)
		}
	}()
	return sink.Emit(ctx, record)
}

func safeSessionTelemetryShutdown(sink SessionTelemetrySink, ctx context.Context) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("sink shutdown panic: %v", recovered)
		}
	}()
	return sink.Shutdown(ctx)
}

func sessionTelemetryHeader(session *Session) SessionHeader {
	session.mu.Lock()
	header := session.Header
	session.mu.Unlock()
	return header
}

func sessionTelemetrySnapshot(session *Session) (SessionHeader, int, []Event) {
	session.mu.Lock()
	header := session.Header
	firstLiveSeq := session.firstLiveSeq
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	return header, firstLiveSeq, events
}

func sessionTelemetryChunkKey(event Event) (string, bool) {
	if event.Type != "assistant/chunk" {
		return "", false
	}
	value, err := cloneTelemetryValue(event.Data)
	if err != nil {
		return "", false
	}
	data, ok := value.(map[string]any)
	if !ok {
		return "", false
	}
	turn, turnOK := telemetryInteger(data["turn"])
	step, stepOK := telemetryInteger(data["step"])
	if !turnOK || !stepOK {
		return "", false
	}
	return fmt.Sprintf("%d:%d", turn, step), true
}

func sessionTelemetryEventAttributes(header SessionHeader, event Event) map[string]any {
	attributes := map[string]any{
		"session.id": header.ID,
		"event.type": event.Type,
		"event.seq":  event.Seq,
	}
	if header.CWD != "" {
		attributes["session.cwd"] = header.CWD
	}
	if header.ParentSession != "" {
		attributes["session.parent_id"] = header.ParentSession
	}
	if header.SeedLength != 0 {
		attributes["session.seed_length"] = header.SeedLength
	}
	return attributes
}

func sessionTelemetryEventSeverity(eventType string, body any) SessionTelemetrySeverity {
	data, _ := body.(map[string]any)
	switch eventType {
	case "tool/result":
		message, _ := data["message"].(map[string]any)
		content, _ := message["content"].([]any)
		if len(content) > 0 {
			first, _ := content[0].(map[string]any)
			if isError, _ := first["isError"].(bool); isError {
				return SessionTelemetrySeverityError
			}
		}
	case "turn/end":
		reason, _ := data["reason"].(map[string]any)
		if reason["kind"] == "error" {
			return SessionTelemetrySeverityError
		}
	}
	return SessionTelemetrySeverityInfo
}

func sessionTelemetryErrorDetail(failure error) map[string]any {
	name := "Error"
	typeOf := reflect.TypeOf(failure)
	if typeOf != nil {
		if typeOf.Kind() == reflect.Pointer {
			typeOf = typeOf.Elem()
		}
		if candidate := typeOf.Name(); candidate != "" && candidate != "errorString" {
			name = candidate
		}
	}
	return map[string]any{"name": name, "message": failure.Error()}
}

func cloneTelemetryValue(value any) (any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var cloned any
	if err := decoder.Decode(&cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

func telemetryInteger(value any) (int64, bool) {
	switch value := value.(type) {
	case json.Number:
		integer, err := value.Int64()
		return integer, err == nil
	case int:
		return int64(value), true
	case int64:
		return value, true
	case float64:
		integer := int64(value)
		return integer, float64(integer) == value
	default:
		return 0, false
	}
}

func validateSessionTelemetryRecord(record SessionTelemetryRecord) error {
	if record.Channel != SessionTelemetryChannelLedger && record.Channel != SessionTelemetryChannelOps {
		return fmt.Errorf("telemetry: unsupported channel %q", record.Channel)
	}
	switch record.Severity {
	case SessionTelemetrySeverityInfo, SessionTelemetrySeverityWarn, SessionTelemetrySeverityError:
	default:
		return fmt.Errorf("telemetry: unsupported severity %q", record.Severity)
	}
	_, err := cloneTelemetryValue(record.Body)
	return err
}

type otlpSessionTelemetrySink struct {
	provider *sdklog.LoggerProvider
	ledger   otellog.Logger
	ops      otellog.Logger
}

func newOTLPSessionTelemetrySink(config SessionTelemetryConfig, dataDir, version string) (*otlpSessionTelemetrySink, error) {
	exporterOptions := []otlploghttp.Option{otlploghttp.WithEndpointURL(config.Exporter.URL)}
	if len(config.Exporter.Headers) > 0 {
		exporterOptions = append(exporterOptions, otlploghttp.WithHeaders(cloneTelemetryHeaders(config.Exporter.Headers)))
	}
	if config.Exporter.Compression == "gzip" {
		exporterOptions = append(exporterOptions, otlploghttp.WithCompression(otlploghttp.GzipCompression))
	}
	exporterOptions = append(exporterOptions, otlploghttp.WithHTTPClient(sessionTelemetryHTTPClient(config.Exporter)))
	exporter, err := otlploghttp.New(context.Background(), exporterOptions...)
	if err != nil {
		return nil, fmt.Errorf("session-telemetry-otel: create exporter: %w", err)
	}
	processorOptions := make([]sdklog.BatchProcessorOption, 0, 4)
	if config.Processor.ScheduledDelay > 0 {
		processorOptions = append(processorOptions, sdklog.WithExportInterval(config.Processor.ScheduledDelay))
	}
	if config.Processor.MaxQueueSize > 0 {
		processorOptions = append(processorOptions, sdklog.WithMaxQueueSize(config.Processor.MaxQueueSize))
	}
	if config.Processor.MaxExportBatchSize > 0 {
		processorOptions = append(processorOptions, sdklog.WithExportMaxBatchSize(config.Processor.MaxExportBatchSize))
	}
	if config.Processor.ExportTimeout > 0 {
		processorOptions = append(processorOptions, sdklog.WithExportTimeout(config.Processor.ExportTimeout))
	}
	processor := sdklog.NewBatchProcessor(exporter, processorOptions...)
	res := resource.NewSchemaless(
		attribute.String("service.name", "deepseek-harness"),
		attribute.String("service.version", version),
		attribute.String("user.id", anonymousUserID(dataDir)),
	)
	provider := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(processor))
	return &otlpSessionTelemetrySink{
		provider: provider,
		ledger:   provider.Logger(sessionTelemetryLedgerScope, otellog.WithInstrumentationVersion(version)),
		ops:      provider.Logger(sessionTelemetryOpsScope, otellog.WithInstrumentationVersion(version)),
	}, nil
}

func (s *otlpSessionTelemetrySink) Emit(ctx context.Context, record SessionTelemetryRecord) error {
	if err := validateSessionTelemetryRecord(record); err != nil {
		return err
	}
	body, err := telemetryAttributeValue(record.Body)
	if err != nil {
		return err
	}
	var emitted otellog.Record
	timestamp := time.UnixMilli(record.Time)
	emitted.SetTimestamp(timestamp)
	emitted.SetObservedTimestamp(timestamp)
	emitted.SetBody(body)
	switch record.Severity {
	case SessionTelemetrySeverityWarn:
		emitted.SetSeverity(otellog.SeverityWarn)
		emitted.SetSeverityText("WARN")
	case SessionTelemetrySeverityError:
		emitted.SetSeverity(otellog.SeverityError)
		emitted.SetSeverityText("ERROR")
	default:
		emitted.SetSeverity(otellog.SeverityInfo)
		emitted.SetSeverityText("INFO")
	}
	keys := make([]string, 0, len(record.Attributes))
	for key := range record.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	attributes := make([]attribute.KeyValue, 0, len(keys))
	for _, key := range keys {
		value, err := telemetryAttributeValue(record.Attributes[key])
		if err != nil {
			return fmt.Errorf("telemetry attribute %q: %w", key, err)
		}
		attributes = append(attributes, attribute.KeyValue{Key: attribute.Key(key), Value: value})
	}
	emitted.AddAttributes(attributes...)
	if ctx == nil {
		ctx = context.Background()
	}
	if record.Channel == SessionTelemetryChannelOps {
		s.ops.Emit(ctx, emitted)
	} else {
		s.ledger.Emit(ctx, emitted)
	}
	return nil
}

func (s *otlpSessionTelemetrySink) Shutdown(ctx context.Context) error {
	return s.provider.Shutdown(ctx)
}

func telemetryAttributeValue(value any) (attribute.Value, error) {
	switch value := value.(type) {
	case nil:
		return attribute.Value{}, nil
	case bool:
		return attribute.BoolValue(value), nil
	case string:
		return attribute.StringValue(value), nil
	case json.Number:
		if integer, err := value.Int64(); err == nil {
			return attribute.Int64Value(integer), nil
		}
		decimal, err := value.Float64()
		if err != nil {
			return attribute.Value{}, err
		}
		return attribute.Float64Value(decimal), nil
	case int:
		return attribute.IntValue(value), nil
	case int8:
		return attribute.Int64Value(int64(value)), nil
	case int16:
		return attribute.Int64Value(int64(value)), nil
	case int32:
		return attribute.Int64Value(int64(value)), nil
	case int64:
		return attribute.Int64Value(value), nil
	case uint:
		return attribute.Int64Value(int64(value)), nil
	case uint8:
		return attribute.Int64Value(int64(value)), nil
	case uint16:
		return attribute.Int64Value(int64(value)), nil
	case uint32:
		return attribute.Int64Value(int64(value)), nil
	case uint64:
		if value > uint64(^uint64(0)>>1) {
			return attribute.Value{}, fmt.Errorf("integer %d exceeds int64", value)
		}
		return attribute.Int64Value(int64(value)), nil
	case float32:
		return attribute.Float64Value(float64(value)), nil
	case float64:
		return attribute.Float64Value(value), nil
	case []any:
		values := make([]attribute.Value, len(value))
		for index, item := range value {
			converted, err := telemetryAttributeValue(item)
			if err != nil {
				return attribute.Value{}, err
			}
			values[index] = converted
		}
		return attribute.SliceValue(values...), nil
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		values := make([]attribute.KeyValue, 0, len(keys))
		for _, key := range keys {
			converted, err := telemetryAttributeValue(value[key])
			if err != nil {
				return attribute.Value{}, err
			}
			values = append(values, attribute.KeyValue{Key: attribute.Key(key), Value: converted})
		}
		return attribute.MapValue(values...), nil
	default:
		normalized, err := cloneTelemetryValue(value)
		if err != nil {
			return attribute.Value{}, err
		}
		return telemetryAttributeValue(normalized)
	}
}

type sessionTelemetryUserAgentTransport struct {
	base      http.RoundTripper
	userAgent string
}

type sessionTelemetryJSONTransport struct {
	base http.RoundTripper
}

func (t sessionTelemetryJSONTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	wire, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	_ = request.Body.Close()
	compressed := request.Header.Get("Content-Encoding") == "gzip"
	if compressed {
		reader, err := gzip.NewReader(bytes.NewReader(wire))
		if err != nil {
			return nil, err
		}
		wire, err = io.ReadAll(reader)
		closeErr := reader.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	var payload collectorl.ExportLogsServiceRequest
	if err := proto.Unmarshal(wire, &payload); err != nil {
		return nil, fmt.Errorf("session-telemetry-otel: decode exporter request: %w", err)
	}
	wire, err = (protojson.MarshalOptions{UseEnumNumbers: true}).Marshal(&payload)
	if err != nil {
		return nil, fmt.Errorf("session-telemetry-otel: encode JSON request: %w", err)
	}
	if compressed {
		var encoded bytes.Buffer
		writer := gzip.NewWriter(&encoded)
		if _, err := writer.Write(wire); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
		wire = encoded.Bytes()
	}
	cloned := request.Clone(request.Context())
	cloned.Header = request.Header.Clone()
	cloned.Header.Set("Content-Type", "application/json")
	cloned.Body = io.NopCloser(bytes.NewReader(wire))
	cloned.ContentLength = int64(len(wire))
	cloned.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(wire)), nil
	}
	return t.base.RoundTrip(cloned)
}

func (t sessionTelemetryUserAgentTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	cloned := request.Clone(request.Context())
	cloned.Header = request.Header.Clone()
	if current := cloned.Header.Get("User-Agent"); current != "" {
		cloned.Header.Set("User-Agent", t.userAgent+" "+current)
	} else {
		cloned.Header.Set("User-Agent", t.userAgent)
	}
	return t.base.RoundTrip(cloned)
}

func sessionTelemetryHTTPClient(config SessionTelemetryOTLPExporterConfig) *http.Client {
	client := newHTTPClient()
	if config.HTTPClient != nil {
		*client = *config.HTTPClient
	}
	transport := client.Transport
	if transport == nil {
		transport = proxyHTTPTransport()
	}
	if base, ok := transport.(*http.Transport); ok && (config.KeepAlive != nil || config.ConcurrencyLimit > 0) {
		cloned := base.Clone()
		if config.KeepAlive != nil {
			cloned.DisableKeepAlives = !*config.KeepAlive
		}
		if config.ConcurrencyLimit > 0 {
			cloned.MaxConnsPerHost = config.ConcurrencyLimit
			cloned.MaxIdleConnsPerHost = config.ConcurrencyLimit
		}
		transport = cloned
	}
	if config.UserAgent != "" {
		transport = sessionTelemetryUserAgentTransport{base: transport, userAgent: config.UserAgent}
	}
	client.Transport = sessionTelemetryJSONTransport{base: transport}
	if config.Timeout > 0 {
		client.Timeout = config.Timeout
	}
	return client
}

func cloneTelemetryHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		return nil
	}
	cloned := make(map[string]string, len(headers))
	for key, value := range headers {
		cloned[key] = value
	}
	return cloned
}
