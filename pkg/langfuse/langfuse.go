// Package langfuse reports gateway requests to Langfuse as OpenTelemetry traces
// through the OTLP/HTTP JSON protocol.
//
// Reporting is best effort: events are queued, batched, and dropped when the
// queue is full, so relay traffic is never blocked by observability. Input and
// output content is never captured unless explicitly enabled.
package langfuse

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
)

const (
	defaultHost      = "https://cloud.langfuse.com"
	otlpTracesPath   = "/api/public/otel/v1/traces"
	ingestionVersion = "4"
	// responseBodyLimit bounds how much of an OTLP error response is logged.
	responseBodyLimit = 8192
)

const (
	spanKindInternal = 1
	statusCodeUnset  = 0
	statusCodeError  = 2
)

var defaultReporter atomic.Pointer[Reporter]

// Config controls Langfuse reporting and content capture behavior.
type Config struct {
	Enabled         bool
	Host            string
	PublicKey       string
	SecretKey       string
	SampleRate      float64
	Timeout         time.Duration
	QueueSize       int
	BatchSize       int
	FlushInterval   time.Duration
	CaptureContent  bool
	ContentMaxBytes int
	Insecure        bool
	Environment     string
	Release         string
	// HTTPClient overrides the client used for OTLP export. Tests use it to
	// trust an httptest TLS certificate; production leaves it nil.
	HTTPClient *http.Client
}

// Event is the normalized gateway event sent to Langfuse.
type Event struct {
	RequestID    string
	TraceID      string
	SessionID    string
	UserID       string
	Model        string
	InputTokens  int
	OutputTokens int
	TotalTokens  int
	Quota        int
	DurationMS   int
	IsStream     bool
	Status       string
	Error        string
	Metadata     map[string]any
	Input        any
	Output       any
}

// Stats contains cumulative reporter delivery counters.
type Stats struct {
	Dropped uint64
	Sent    uint64
	Failed  uint64
}

// Reporter asynchronously batches and sends gateway events to Langfuse.
type Reporter struct {
	config Config
	client *http.Client
	queue  chan Event
	flush  chan chan struct{}
	stop   chan struct{}
	done   chan struct{}

	workerCtx context.Context
	cancel    context.CancelFunc

	started bool
	closed  atomic.Bool
	dropped atomic.Uint64
	sent    atomic.Uint64
	failed  atomic.Uint64
}

// SetDefaultReporter sets the process-wide reporter used by Publish.
func SetDefaultReporter(reporter *Reporter) {
	defaultReporter.Store(reporter)
}

// DefaultReporter returns the process-wide Langfuse reporter.
func DefaultReporter() *Reporter {
	return defaultReporter.Load()
}

// Publish publishes an event to the process-wide reporter.
func Publish(event Event) {
	if reporter := DefaultReporter(); reporter != nil {
		reporter.Publish(event)
	}
}

// CloseDefault flushes and closes the process-wide reporter.
func CloseDefault(ctx context.Context) error {
	reporter := DefaultReporter()
	if reporter == nil {
		return nil
	}
	err := reporter.Close(ctx)
	defaultReporter.Store(nil)
	return err
}

// LoadConfig reads Langfuse settings from environment variables.
func LoadConfig() Config {
	config := Config{
		Enabled:         envBool("LANGFUSE_ENABLED", false),
		Host:            envString("LANGFUSE_HOST", defaultHost),
		PublicKey:       strings.TrimSpace(os.Getenv("LANGFUSE_PUBLIC_KEY")),
		SecretKey:       strings.TrimSpace(os.Getenv("LANGFUSE_SECRET_KEY")),
		SampleRate:      1,
		Timeout:         time.Duration(envInt("LANGFUSE_TIMEOUT_MS", 3000)) * time.Millisecond,
		QueueSize:       envInt("LANGFUSE_QUEUE_SIZE", 256),
		BatchSize:       envInt("LANGFUSE_BATCH_SIZE", 20),
		FlushInterval:   time.Duration(envInt("LANGFUSE_FLUSH_INTERVAL_MS", 1000)) * time.Millisecond,
		CaptureContent:  envBool("LANGFUSE_CAPTURE_CONTENT", false),
		ContentMaxBytes: envInt("LANGFUSE_CONTENT_MAX_BYTES", 16384),
		Insecure:        envBool("LANGFUSE_INSECURE", false),
		Environment:     strings.TrimSpace(os.Getenv("LANGFUSE_ENVIRONMENT")),
		Release:         strings.TrimSpace(os.Getenv("LANGFUSE_RELEASE")),
	}
	// An unparseable sample rate disables sampling instead of silently
	// falling back to reporting everything.
	if raw := strings.TrimSpace(os.Getenv("LANGFUSE_SAMPLE_RATE")); raw != "" {
		config.SampleRate = 0
		if rate, err := strconv.ParseFloat(raw, 64); err == nil {
			config.SampleRate = rate
		}
	}
	return normalizeConfig(config)
}

// NewReporter creates a Langfuse reporter. It starts a worker only when the
// feature is enabled, both API keys are present, and the sample rate is above
// zero.
func NewReporter(config Config) *Reporter {
	config = normalizeConfig(config)
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: config.Timeout}
	} else {
		cloned := *client
		client = &cloned
		if client.Timeout == 0 {
			client.Timeout = config.Timeout
		}
	}
	previousCheckRedirect := client.CheckRedirect
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if !config.Insecure && (req.URL == nil || req.URL.Scheme != "https") {
			return fmt.Errorf("refusing non-HTTPS Langfuse redirect")
		}
		if previousCheckRedirect != nil {
			return previousCheckRedirect(req, via)
		}
		return nil
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	reporter := &Reporter{
		config:    config,
		client:    client,
		queue:     make(chan Event, config.QueueSize),
		flush:     make(chan chan struct{}),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		workerCtx: workerCtx,
		cancel:    cancel,
	}
	reporter.started = config.Enabled && config.PublicKey != "" && config.SecretKey != "" && config.SampleRate > 0
	if reporter.started {
		go reporter.run()
	}
	return reporter
}

// Publish queues an event without blocking the caller when the queue is full.
func (r *Reporter) Publish(event Event) {
	if r == nil || !r.started || r.closed.Load() {
		return
	}
	if !sampled(r.config.SampleRate) {
		return
	}
	select {
	case r.queue <- event:
	default:
		dropped := r.dropped.Add(1)
		if dropped == 1 || dropped%100 == 0 {
			common.SysLog(common.LogText("langfuse events dropped: queue is full, total=%d", dropped))
		}
	}
}

// Stats returns the reporter's cumulative queue and delivery counters.
func (r *Reporter) Stats() Stats {
	if r == nil {
		return Stats{}
	}
	return Stats{Dropped: r.dropped.Load(), Sent: r.sent.Load(), Failed: r.failed.Load()}
}

// Flush synchronously drains the reporter queue within the caller's deadline.
func (r *Reporter) Flush(ctx context.Context) error {
	if r == nil || !r.started {
		return nil
	}
	ack := make(chan struct{})
	select {
	case r.flush <- ack:
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ack:
		return nil
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes pending events and stops the reporter within the caller's
// deadline. It is safe to call more than once.
func (r *Reporter) Close(ctx context.Context) error {
	if r == nil || !r.started {
		return nil
	}
	if r.closed.Swap(true) {
		select {
		case <-r.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	flushErr := r.Flush(ctx)
	close(r.stop)
	select {
	case <-r.done:
		return flushErr
	case <-ctx.Done():
		r.cancel()
		return ctx.Err()
	}
}

func (r *Reporter) run() {
	defer close(r.done)
	if r.config.Insecure && strings.HasPrefix(strings.ToLower(r.config.Host), "http://") {
		common.SysLog(common.LogText("langfuse telemetry uses plain HTTP because LANGFUSE_INSECURE is enabled: %s", r.config.Host))
	}
	ticker := time.NewTicker(r.config.FlushInterval)
	defer ticker.Stop()
	batch := make([]Event, 0, r.config.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		rejected, err := r.send(r.workerCtx, batch)
		if err != nil {
			r.failed.Add(uint64(len(batch)))
			common.SysError(common.LogText("langfuse batch send failed: %s", err.Error()))
		} else {
			r.sent.Add(uint64(len(batch) - rejected))
			r.failed.Add(uint64(rejected))
		}
		batch = batch[:0]
	}
	for {
		select {
		case event := <-r.queue:
			batch = append(batch, event)
			if len(batch) >= r.config.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case ack := <-r.flush:
			for draining := true; draining; {
				select {
				case event := <-r.queue:
					batch = append(batch, event)
					if len(batch) >= r.config.BatchSize {
						flush()
					}
				default:
					draining = false
				}
			}
			flush()
			close(ack)
		case <-r.stop:
			flush()
			return
		}
	}
}

// send delivers one batch and returns the number of spans the collector
// rejected. A non-nil error means the request failed and every event in the
// batch counts as failed.
func (r *Reporter) send(ctx context.Context, events []Event) (int, error) {
	host, err := url.Parse(r.config.Host)
	if err != nil {
		return len(events), fmt.Errorf("invalid Langfuse host %q: %w", r.config.Host, err)
	}
	if host.Host == "" {
		return len(events), fmt.Errorf("invalid Langfuse host: %s", r.config.Host)
	}
	if host.Scheme != "https" && !(r.config.Insecure && host.Scheme == "http") {
		return len(events), fmt.Errorf("langfuse host must use HTTPS: %s", r.config.Host)
	}

	data, err := common.Marshal(buildTracesPayload(events, r.config))
	if err != nil {
		return len(events), err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.config.Host+otlpTracesPath, bytes.NewReader(data))
	if err != nil {
		return len(events), err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-langfuse-ingestion-version", ingestionVersion)
	req.SetBasicAuth(r.config.PublicKey, r.config.SecretKey)
	resp, err := r.client.Do(req)
	if err != nil {
		return len(events), err
	}
	defer func() { _ = resp.Body.Close() }()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, responseBodyLimit))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		common.SysError(common.LogText("langfuse OTLP export returned HTTP %d: %s", resp.StatusCode, common.LocalLogPreview(string(body))))
		return len(events), fmt.Errorf("langfuse OTLP export returned HTTP %d", resp.StatusCode)
	}
	if readErr != nil || len(body) == 0 {
		return 0, nil
	}
	var result otlpResponse
	if err := common.Unmarshal(body, &result); err != nil {
		common.SysError(common.LogText("langfuse OTLP response could not be decoded: %s", common.LocalLogPreview(string(body))))
		return 0, nil
	}
	if result.PartialSuccess == nil || result.PartialSuccess.RejectedSpans <= 0 {
		return 0, nil
	}
	rejected := int(result.PartialSuccess.RejectedSpans)
	if rejected > len(events) {
		rejected = len(events)
	}
	common.SysError(common.LogText("langfuse rejected %d spans: %s", rejected, common.LocalLogPreview(result.PartialSuccess.ErrorMessage)))
	return rejected, nil
}

type tracesPayload struct {
	ResourceSpans []resourceSpans `json:"resourceSpans"`
}

type resourceSpans struct {
	Resource   resource     `json:"resource"`
	ScopeSpans []scopeSpans `json:"scopeSpans"`
}

type resource struct {
	Attributes []attribute `json:"attributes"`
}

type scopeSpans struct {
	Scope scope  `json:"scope"`
	Spans []span `json:"spans"`
}

type scope struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

type span struct {
	TraceID           string      `json:"traceId"`
	SpanID            string      `json:"spanId"`
	Name              string      `json:"name"`
	Kind              int         `json:"kind"`
	StartTimeUnixNano string      `json:"startTimeUnixNano"`
	EndTimeUnixNano   string      `json:"endTimeUnixNano"`
	Attributes        []attribute `json:"attributes,omitempty"`
	Status            spanStatus  `json:"status"`
}

type spanStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

type attribute struct {
	Key   string         `json:"key"`
	Value attributeValue `json:"value"`
}

// attributeValue follows the OTLP JSON mapping: int64 values are decimal
// strings and unset values are omitted.
type attributeValue struct {
	StringValue *string `json:"stringValue,omitempty"`
	IntValue    *string `json:"intValue,omitempty"`
	BoolValue   *bool   `json:"boolValue,omitempty"`
}

type usageDetails struct {
	Input  int `json:"input"`
	Output int `json:"output"`
	Total  int `json:"total"`
}

type truncatedContent struct {
	Truncated     bool   `json:"truncated"`
	OriginalBytes int    `json:"original_bytes"`
	Preview       string `json:"preview"`
}

type otlpResponse struct {
	PartialSuccess *otlpPartialSuccess `json:"partialSuccess"`
}

type otlpPartialSuccess struct {
	RejectedSpans int64  `json:"rejectedSpans"`
	ErrorMessage  string `json:"errorMessage"`
}

func buildTracesPayload(events []Event, config Config) tracesPayload {
	now := time.Now()
	spans := make([]span, 0, len(events))
	for _, event := range events {
		spans = append(spans, spanForEvent(event, config, now))
	}
	return tracesPayload{ResourceSpans: []resourceSpans{{
		Resource: resource{Attributes: []attribute{
			stringAttribute("service.name", "new-api"),
			stringAttribute("service.version", common.Version),
		}},
		ScopeSpans: []scopeSpans{{
			Scope: scope{Name: "new-api.langfuse", Version: common.Version},
			Spans: spans,
		}},
	}}}
}

func spanForEvent(event Event, config Config, now time.Time) span {
	durationMS := event.DurationMS
	if durationMS < 0 {
		durationMS = 0
	}
	name := event.Model
	if name == "" {
		name = "gateway.request"
	}
	status := spanStatus{Code: statusCodeUnset}
	if event.Status == "error" {
		status.Code = statusCodeError
		status.Message = event.Error
	}
	return span{
		TraceID:           traceIDFor(event),
		SpanID:            randomHex(8),
		Name:              name,
		Kind:              spanKindInternal,
		StartTimeUnixNano: strconv.FormatInt(now.Add(-time.Duration(durationMS)*time.Millisecond).UnixNano(), 10),
		EndTimeUnixNano:   strconv.FormatInt(now.UnixNano(), 10),
		Attributes:        spanAttributes(event, config),
		Status:            status,
	}
}

func spanAttributes(event Event, config Config) []attribute {
	attributes := []attribute{
		stringAttribute("langfuse.observation.type", "generation"),
	}
	traceName := event.Model
	if traceName == "" {
		traceName = "new-api request"
	}
	attributes = append(attributes, stringAttribute("langfuse.trace.name", traceName))
	if event.UserID != "" {
		attributes = append(attributes, stringAttribute("langfuse.user.id", event.UserID))
	}
	if event.SessionID != "" {
		attributes = append(attributes, stringAttribute("langfuse.session.id", event.SessionID))
	}
	if config.Release != "" {
		attributes = append(attributes, stringAttribute("langfuse.release", config.Release))
	}
	if config.Environment != "" {
		attributes = append(attributes, stringAttribute("langfuse.environment", config.Environment))
	}
	if event.Model != "" {
		attributes = append(attributes, stringAttribute("langfuse.observation.model.name", event.Model))
	}
	if event.InputTokens != 0 || event.OutputTokens != 0 || event.TotalTokens != 0 {
		if usage, err := common.Marshal(usageDetails{Input: event.InputTokens, Output: event.OutputTokens, Total: event.TotalTokens}); err == nil {
			attributes = append(attributes, stringAttribute("langfuse.observation.usage_details", string(usage)))
		}
	}
	if event.Quota != 0 {
		attributes = append(attributes, intAttribute("langfuse.trace.metadata.quota", int64(event.Quota)))
	}
	attributes = append(attributes, boolAttribute("langfuse.trace.metadata.is_stream", event.IsStream))
	level := "DEFAULT"
	if event.Status == "error" {
		level = "ERROR"
	}
	attributes = append(attributes, stringAttribute("langfuse.observation.level", level))
	if event.Error != "" {
		attributes = append(attributes, stringAttribute("langfuse.observation.status_message", event.Error))
	}
	if len(event.Metadata) > 0 {
		keys := make([]string, 0, len(event.Metadata))
		for key := range event.Metadata {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			if key == "" {
				continue
			}
			if value, ok := metadataAttribute("langfuse.trace.metadata."+key, event.Metadata[key]); ok {
				attributes = append(attributes, value)
			}
		}
	}
	if event.RequestID != "" {
		if _, exists := event.Metadata["request_id"]; !exists {
			attributes = append(attributes, stringAttribute("langfuse.trace.metadata.request_id", event.RequestID))
		}
	}
	if config.CaptureContent {
		if input, ok := contentJSON(event.Input, config.ContentMaxBytes); ok {
			attributes = append(attributes, stringAttribute("langfuse.observation.input", input))
		}
		if output, ok := contentJSON(event.Output, config.ContentMaxBytes); ok {
			attributes = append(attributes, stringAttribute("langfuse.observation.output", output))
		}
	}
	return attributes
}

func stringAttribute(key string, value string) attribute {
	return attribute{Key: key, Value: attributeValue{StringValue: &value}}
}

func intAttribute(key string, value int64) attribute {
	decimal := strconv.FormatInt(value, 10)
	return attribute{Key: key, Value: attributeValue{IntValue: &decimal}}
}

func boolAttribute(key string, value bool) attribute {
	return attribute{Key: key, Value: attributeValue{BoolValue: &value}}
}

// metadataAttribute encodes one metadata entry as an OTLP attribute value.
// Unsupported types and nil values are skipped.
func metadataAttribute(key string, value any) (attribute, bool) {
	switch typed := value.(type) {
	case string:
		return stringAttribute(key, typed), true
	case bool:
		return boolAttribute(key, typed), true
	case int:
		return intAttribute(key, int64(typed)), true
	case int32:
		return intAttribute(key, int64(typed)), true
	case int64:
		return intAttribute(key, typed), true
	case uint:
		return unsignedAttribute(key, uint64(typed)), true
	case uint32:
		return unsignedAttribute(key, uint64(typed)), true
	case uint64:
		return unsignedAttribute(key, typed), true
	case []string:
		return stringAttribute(key, strings.Join(typed, ",")), true
	default:
		return attribute{}, false
	}
}

func unsignedAttribute(key string, value uint64) attribute {
	decimal := strconv.FormatUint(value, 10)
	return attribute{Key: key, Value: attributeValue{IntValue: &decimal}}
}

// contentJSON serializes captured content for an OTLP string attribute,
// replacing oversized content with a truncation marker.
func contentJSON(value any, maxBytes int) (string, bool) {
	if value == nil {
		return "", false
	}
	data, err := common.Marshal(value)
	if err != nil {
		return "", false
	}
	if len(data) <= maxBytes {
		return string(data), true
	}
	marker, err := common.Marshal(truncatedContent{
		Truncated:     true,
		OriginalBytes: len(data),
		Preview:       strings.ToValidUTF8(string(data[:maxBytes]), "\uFFFD"),
	})
	if err != nil {
		return "", false
	}
	return string(marker), true
}

// traceIDFor derives a stable 32-character trace id from the event's trace or
// request id so retries and the matching consume log share one trace.
func traceIDFor(event Event) string {
	seed := event.TraceID
	if seed == "" {
		seed = event.RequestID
	}
	if seed == "" {
		return randomHex(16)
	}
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:16])
}

func randomHex(size int) string {
	buffer := make([]byte, size)
	if _, err := rand.Read(buffer); err != nil {
		return strings.Repeat("0", size*2)
	}
	return hex.EncodeToString(buffer)
}

func sampled(rate float64) bool {
	if rate >= 1 {
		return true
	}
	if rate <= 0 {
		return false
	}
	var buffer [8]byte
	if _, err := rand.Read(buffer[:]); err != nil {
		return false
	}
	return binary.BigEndian.Uint64(buffer[:]) < uint64(rate*float64(^uint64(0)))
}

func normalizeConfig(config Config) Config {
	config.Host = strings.TrimSpace(config.Host)
	if config.Host == "" {
		config.Host = defaultHost
	}
	config.Host = strings.TrimRight(config.Host, "/")
	if math.IsNaN(config.SampleRate) || config.SampleRate < 0 {
		config.SampleRate = 0
	} else if config.SampleRate > 1 {
		config.SampleRate = 1
	}
	if config.Timeout <= 0 {
		config.Timeout = 3 * time.Second
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 256
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 20
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = time.Second
	}
	if config.ContentMaxBytes <= 0 {
		config.ContentMaxBytes = 16384
	}
	return config
}

func envString(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(key)))
	if err != nil {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil {
		return fallback
	}
	return value
}
