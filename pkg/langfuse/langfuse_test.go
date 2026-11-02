package langfuse

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigDefaults(t *testing.T) {
	for _, key := range []string{
		"LANGFUSE_ENABLED",
		"LANGFUSE_HOST",
		"LANGFUSE_PUBLIC_KEY",
		"LANGFUSE_SECRET_KEY",
		"LANGFUSE_SAMPLE_RATE",
		"LANGFUSE_TIMEOUT_MS",
		"LANGFUSE_QUEUE_SIZE",
		"LANGFUSE_BATCH_SIZE",
		"LANGFUSE_FLUSH_INTERVAL_MS",
		"LANGFUSE_CAPTURE_CONTENT",
		"LANGFUSE_CONTENT_MAX_BYTES",
		"LANGFUSE_INSECURE",
		"LANGFUSE_ENVIRONMENT",
		"LANGFUSE_RELEASE",
	} {
		t.Setenv(key, "")
	}

	config := LoadConfig()

	assert.False(t, config.Enabled)
	assert.Equal(t, defaultHost, config.Host)
	assert.Empty(t, config.PublicKey)
	assert.Empty(t, config.SecretKey)
	assert.Equal(t, 1.0, config.SampleRate)
	assert.Equal(t, 3*time.Second, config.Timeout)
	assert.Equal(t, 256, config.QueueSize)
	assert.Equal(t, 20, config.BatchSize)
	assert.Equal(t, time.Second, config.FlushInterval)
	assert.False(t, config.CaptureContent)
	assert.Equal(t, 16384, config.ContentMaxBytes)
	assert.False(t, config.Insecure)
	assert.Empty(t, config.Environment)
	assert.Empty(t, config.Release)
}

func TestLoadConfigReadsEnv(t *testing.T) {
	t.Setenv("LANGFUSE_ENABLED", "true")
	t.Setenv("LANGFUSE_HOST", "https://langfuse.internal/")
	t.Setenv("LANGFUSE_PUBLIC_KEY", "public")
	t.Setenv("LANGFUSE_SECRET_KEY", "secret")
	t.Setenv("LANGFUSE_SAMPLE_RATE", "0.25")
	t.Setenv("LANGFUSE_TIMEOUT_MS", "500")
	t.Setenv("LANGFUSE_QUEUE_SIZE", "32")
	t.Setenv("LANGFUSE_BATCH_SIZE", "8")
	t.Setenv("LANGFUSE_FLUSH_INTERVAL_MS", "250")
	t.Setenv("LANGFUSE_CAPTURE_CONTENT", "true")
	t.Setenv("LANGFUSE_CONTENT_MAX_BYTES", "1024")
	t.Setenv("LANGFUSE_INSECURE", "true")
	t.Setenv("LANGFUSE_ENVIRONMENT", "staging")
	t.Setenv("LANGFUSE_RELEASE", "v1.2.3")

	config := LoadConfig()

	assert.True(t, config.Enabled)
	assert.Equal(t, "https://langfuse.internal", config.Host)
	assert.Equal(t, "public", config.PublicKey)
	assert.Equal(t, "secret", config.SecretKey)
	assert.Equal(t, 0.25, config.SampleRate)
	assert.Equal(t, 500*time.Millisecond, config.Timeout)
	assert.Equal(t, 32, config.QueueSize)
	assert.Equal(t, 8, config.BatchSize)
	assert.Equal(t, 250*time.Millisecond, config.FlushInterval)
	assert.True(t, config.CaptureContent)
	assert.Equal(t, 1024, config.ContentMaxBytes)
	assert.True(t, config.Insecure)
	assert.Equal(t, "staging", config.Environment)
	assert.Equal(t, "v1.2.3", config.Release)
}

func TestLoadConfigSampleRate(t *testing.T) {
	cases := []struct {
		raw  string
		want float64
	}{
		{"1", 1},
		{"0", 0},
		{"0.5", 0.5},
		{"2", 1},
		{"-1", 0},
		{"not-a-number", 0},
	}
	for _, testCase := range cases {
		t.Run(testCase.raw, func(t *testing.T) {
			t.Setenv("LANGFUSE_SAMPLE_RATE", testCase.raw)
			assert.Equal(t, testCase.want, LoadConfig().SampleRate)
		})
	}
}

func TestReporterDoesNotReportWhenDisabledOrUnconfigured(t *testing.T) {
	cases := []struct {
		name   string
		config Config
	}{
		{"disabled", Config{Enabled: false, PublicKey: "public", SecretKey: "secret", SampleRate: 1}},
		{"missing public key", Config{Enabled: true, SecretKey: "secret", SampleRate: 1}},
		{"missing secret key", Config{Enabled: true, PublicKey: "public", SampleRate: 1}},
		{"zero sample rate", Config{Enabled: true, PublicKey: "public", SecretKey: "secret", SampleRate: 0}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			reporter := NewReporter(testCase.config)
			assert.False(t, reporter.started)

			reporter.Publish(Event{RequestID: "unreported"})
			assert.Empty(t, reporter.queue)
			assert.Empty(t, reporter.Stats())
			assert.NoError(t, reporter.Flush(context.Background()))
			assert.NoError(t, reporter.Close(context.Background()))
		})
	}
}

func TestPublishDropsWhenQueueIsFull(t *testing.T) {
	reporter := &Reporter{
		config:  Config{SampleRate: 1},
		queue:   make(chan Event, 1),
		started: true,
	}

	reporter.Publish(Event{RequestID: "first"})
	reporter.Publish(Event{RequestID: "second"})

	assert.Len(t, reporter.queue, 1)
	assert.Equal(t, Stats{Dropped: 1}, reporter.Stats())
}

func TestFlushSendsOTLPSpan(t *testing.T) {
	server, requests := newOTLPCollector(t, http.StatusOK, `{"partialSuccess":{}}`)
	reporter := NewReporter(Config{
		Enabled:       true,
		Host:          server.URL,
		PublicKey:     "public",
		SecretKey:     "secret",
		SampleRate:    1,
		QueueSize:     4,
		BatchSize:     10,
		FlushInterval: time.Hour,
		HTTPClient:    server.Client(),
	})
	closeReporter(t, reporter)

	reporter.Publish(Event{
		RequestID:    "req-1",
		TraceID:      "trace-1",
		SessionID:    "sess-1",
		UserID:       "42",
		Model:        "gpt-test",
		InputTokens:  10,
		OutputTokens: 5,
		TotalTokens:  15,
		Quota:        20,
		DurationMS:   125,
		Status:       "error",
		Error:        "upstream failed",
		Metadata: map[string]any{
			"team":        "team-a",
			"retry_count": 3,
			"cached":      false,
			"channels":    []string{"101", "102"},
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, reporter.Flush(ctx))
	got := requireResponse(t, requests)
	require.NoError(t, got.decodeErr)

	assert.Equal(t, "/api/public/otel/v1/traces", got.path)
	require.True(t, got.basicOK)
	assert.Equal(t, "public", got.username)
	assert.Equal(t, "secret", got.password)
	assert.Equal(t, "4", got.version)
	assert.Equal(t, "application/json", got.contentType)

	require.Len(t, got.payload.ResourceSpans, 1)
	resourceSpans := got.payload.ResourceSpans[0]
	resourceAttributes := attributeMap(resourceSpans.Resource.Attributes)
	assert.Equal(t, "new-api", attributeString(t, resourceAttributes, "service.name"))
	assert.Equal(t, common.Version, attributeString(t, resourceAttributes, "service.version"))
	require.Len(t, resourceSpans.ScopeSpans, 1)
	assert.Equal(t, "new-api.langfuse", resourceSpans.ScopeSpans[0].Scope.Name)
	require.Len(t, resourceSpans.ScopeSpans[0].Spans, 1)

	span := resourceSpans.ScopeSpans[0].Spans[0]
	assert.Len(t, span.TraceID, 32)
	assert.Len(t, span.SpanID, 16)
	_, hexErr := hex.DecodeString(span.TraceID)
	assert.NoError(t, hexErr)
	assert.Equal(t, 1, span.Kind)
	assert.Equal(t, "gpt-test", span.Name)
	assert.Equal(t, 2, span.Status.Code)
	assert.Equal(t, "upstream failed", span.Status.Message)
	startNano, err := strconv.ParseInt(span.StartTimeUnixNano, 10, 64)
	require.NoError(t, err)
	endNano, err := strconv.ParseInt(span.EndTimeUnixNano, 10, 64)
	require.NoError(t, err)
	assert.Equal(t, int64(125*time.Millisecond), endNano-startNano)

	attributes := attributeMap(span.Attributes)
	assert.Equal(t, "generation", attributeString(t, attributes, "langfuse.observation.type"))
	assert.Equal(t, "gpt-test", attributeString(t, attributes, "langfuse.trace.name"))
	assert.Equal(t, "42", attributeString(t, attributes, "langfuse.user.id"))
	assert.Equal(t, "sess-1", attributeString(t, attributes, "langfuse.session.id"))
	assert.Equal(t, "gpt-test", attributeString(t, attributes, "langfuse.observation.model.name"))
	assert.Equal(t, "ERROR", attributeString(t, attributes, "langfuse.observation.level"))
	assert.Equal(t, "upstream failed", attributeString(t, attributes, "langfuse.observation.status_message"))

	var usage usageDetails
	require.NoError(t, common.UnmarshalJsonStr(attributeString(t, attributes, "langfuse.observation.usage_details"), &usage))
	assert.Equal(t, usageDetails{Input: 10, Output: 5, Total: 15}, usage)

	assert.Equal(t, "team-a", attributeString(t, attributes, "langfuse.trace.metadata.team"))
	retryCount, ok := attributes["langfuse.trace.metadata.retry_count"]
	require.True(t, ok)
	require.NotNil(t, retryCount.IntValue)
	assert.Equal(t, "3", *retryCount.IntValue)
	cached, ok := attributes["langfuse.trace.metadata.cached"]
	require.True(t, ok)
	require.NotNil(t, cached.BoolValue)
	assert.False(t, *cached.BoolValue)
	assert.Equal(t, "101,102", attributeString(t, attributes, "langfuse.trace.metadata.channels"))
	assert.Equal(t, "req-1", attributeString(t, attributes, "langfuse.trace.metadata.request_id"))
	quota, ok := attributes["langfuse.trace.metadata.quota"]
	require.True(t, ok)
	require.NotNil(t, quota.IntValue)
	assert.Equal(t, "20", *quota.IntValue)

	assert.Equal(t, Stats{Sent: 1}, reporter.Stats())
}

func TestCaptureContent(t *testing.T) {
	t.Run("truncates oversized content", func(t *testing.T) {
		server, requests := newOTLPCollector(t, http.StatusOK, "")
		reporter := NewReporter(Config{
			Enabled:         true,
			Host:            server.URL,
			PublicKey:       "public",
			SecretKey:       "secret",
			SampleRate:      1,
			QueueSize:       2,
			BatchSize:       5,
			FlushInterval:   time.Hour,
			CaptureContent:  true,
			ContentMaxBytes: 8,
			HTTPClient:      server.Client(),
		})
		closeReporter(t, reporter)
		reporter.Publish(Event{RequestID: "content", Input: "01234567890123456789"})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, reporter.Flush(ctx))
		got := requireResponse(t, requests)
		require.NoError(t, got.decodeErr)

		require.Len(t, got.payload.ResourceSpans, 1)
		require.Len(t, got.payload.ResourceSpans[0].ScopeSpans, 1)
		require.Len(t, got.payload.ResourceSpans[0].ScopeSpans[0].Spans, 1)
		attributes := attributeMap(got.payload.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes)
		var marker truncatedContent
		require.NoError(t, common.UnmarshalJsonStr(attributeString(t, attributes, "langfuse.observation.input"), &marker))
		assert.True(t, marker.Truncated)
		assert.Equal(t, 22, marker.OriginalBytes)
		assert.Equal(t, "\"0123456", marker.Preview)
	})

	t.Run("reports raw JSON content as JSON", func(t *testing.T) {
		server, requests := newOTLPCollector(t, http.StatusOK, "")
		reporter := NewReporter(Config{
			Enabled:         true,
			Host:            server.URL,
			PublicKey:       "public",
			SecretKey:       "secret",
			SampleRate:      1,
			QueueSize:       2,
			BatchSize:       5,
			FlushInterval:   time.Hour,
			CaptureContent:  true,
			ContentMaxBytes: 4096,
			HTTPClient:      server.Client(),
		})
		closeReporter(t, reporter)
		reporter.Publish(Event{
			RequestID: "json",
			Input:     common.RawMessage(`{"messages":[{"role":"user","content":"hello"}]}`),
			Output:    common.RawMessage(`{"choices":[{"message":{"content":"world"}}]}`),
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, reporter.Flush(ctx))
		got := requireResponse(t, requests)
		require.NoError(t, got.decodeErr)

		attributes := attributeMap(got.payload.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes)
		assert.JSONEq(t, `{"messages":[{"role":"user","content":"hello"}]}`, attributeString(t, attributes, "langfuse.observation.input"))
		assert.JSONEq(t, `{"choices":[{"message":{"content":"world"}}]}`, attributeString(t, attributes, "langfuse.observation.output"))
	})

	t.Run("omits content when capture is off", func(t *testing.T) {
		server, requests := newOTLPCollector(t, http.StatusOK, "")
		reporter := NewReporter(Config{
			Enabled:       true,
			Host:          server.URL,
			PublicKey:     "public",
			SecretKey:     "secret",
			SampleRate:    1,
			QueueSize:     2,
			BatchSize:     5,
			FlushInterval: time.Hour,
			HTTPClient:    server.Client(),
		})
		closeReporter(t, reporter)
		reporter.Publish(Event{RequestID: "no-content", Input: "secret prompt", Output: "secret answer"})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, reporter.Flush(ctx))
		got := requireResponse(t, requests)
		require.NoError(t, got.decodeErr)

		require.Len(t, got.payload.ResourceSpans, 1)
		require.Len(t, got.payload.ResourceSpans[0].ScopeSpans, 1)
		require.Len(t, got.payload.ResourceSpans[0].ScopeSpans[0].Spans, 1)
		attributes := attributeMap(got.payload.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes)
		_, hasInput := attributes["langfuse.observation.input"]
		_, hasOutput := attributes["langfuse.observation.output"]
		assert.False(t, hasInput)
		assert.False(t, hasOutput)
	})
}

func TestCaptureGatewayContent(t *testing.T) {
	const requestBody = `{"model":"gpt-test","messages":[{"role":"user","content":"hello"}]}`
	const responseBody = `{"choices":[{"message":{"content":"world"}}]}`

	t.Run("captures request and response bodies", func(t *testing.T) {
		setCaptureReporter(t, 4096)
		c := newCaptureContext(t, requestBody)
		AttachContentCapture(c)
		c.Writer.Header().Set("Content-Type", "application/json")
		_, err := c.Writer.WriteString(responseBody)
		require.NoError(t, err)

		input, output := CapturedContent(c)

		require.IsType(t, common.RawMessage(nil), input)
		assert.JSONEq(t, requestBody, string(input.(common.RawMessage)))
		require.IsType(t, common.RawMessage(nil), output)
		assert.JSONEq(t, responseBody, string(output.(common.RawMessage)))
	})

	t.Run("folds SSE chunks into the final output", func(t *testing.T) {
		setCaptureReporter(t, 4096)
		c := newCaptureContext(t, requestBody)
		AttachContentCapture(c)
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		_, err := c.Writer.WriteString("data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n" +
			": PING\n\n" +
			"data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}]}\n\n" +
			"data: [DONE]\n\n")
		require.NoError(t, err)

		_, output := CapturedContent(c)

		raw, ok := output.(common.RawMessage)
		require.True(t, ok, "expected raw JSON, got %T", output)
		assert.JSONEq(t, `"hello"`, string(raw))
	})

	t.Run("truncates oversized request and response bodies", func(t *testing.T) {
		setCaptureReporter(t, 16)
		c := newCaptureContext(t, requestBody)
		AttachContentCapture(c)
		_, err := c.Writer.WriteString(responseBody)
		require.NoError(t, err)

		input, output := CapturedContent(c)

		var inputMarker truncatedContent
		require.NoError(t, common.Unmarshal(input.(common.RawMessage), &inputMarker))
		assert.True(t, inputMarker.Truncated)
		assert.Equal(t, len(requestBody), inputMarker.OriginalBytes)

		var outputMarker truncatedContent
		require.NoError(t, common.Unmarshal(output.(common.RawMessage), &outputMarker))
		assert.True(t, outputMarker.Truncated)
		assert.Equal(t, len(responseBody), outputMarker.OriginalBytes)
	})

	t.Run("does not capture when disabled", func(t *testing.T) {
		SetDefaultReporter(nil)
		c := newCaptureContext(t, requestBody)
		originalWriter := c.Writer
		AttachContentCapture(c)
		assert.True(t, originalWriter == c.Writer)

		_, err := c.Writer.WriteString(responseBody)
		require.NoError(t, err)
		input, output := CapturedContent(c)
		assert.Nil(t, input)
		assert.Nil(t, output)
	})
}

func TestStreamContentValue(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		total int
		limit int
		want  string
	}{
		{
			name: "openai chat text and tool call",
			body: `data: {"choices":[{"delta":{"content":"Hello"}}]}
data: {"choices":[{"delta":{"content":" world","tool_calls":[{"index":0,"function":{"name":"lookup","arguments":"{\"q\":"}}]}}]}
data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}}]}}]}
data: [DONE]`,
			want: `{"text":"Hello world","tool_calls":[{"name":"lookup","arguments":"{\"q\":\"x\"}"}]}`,
		},
		{
			name: "claude text reasoning and tool input",
			body: `data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}
data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"think"}}
data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","name":"search","input":{}}}
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"x\"}"}}`,
			want: `{"text":"Hi","reasoning":"think","tool_calls":[{"name":"search","arguments":"{\"q\":\"x\"}"}]}`,
		},
		{
			name: "responses text and function arguments",
			body: `data: {"type":"response.output_text.delta","delta":"Hel"}
data: {"type":"response.output_text.delta","delta":"lo"}
data: {"type":"response.function_call_arguments.delta","output_index":0,"name":"lookup","delta":"{\"q\":"}
data: {"type":"response.function_call_arguments.delta","output_index":0,"delta":"\"x\"}"}`,
			want: `{"text":"Hello","tool_calls":[{"name":"lookup","arguments":"{\"q\":\"x\"}"}]}`,
		},
		{
			name: "gemini parts",
			body: `data: {"candidates":[{"content":{"parts":[{"text":"你好"}]}}]}
data: {"candidates":[{"content":{"parts":[{"text":"，世界"}]}}]}`,
			want: `"你好，世界"`,
		},
		{
			name: "cumulative provider chunks do not duplicate",
			body: `data: {"choices":[{"delta":{"content":"Hel"}}]}
data: {"choices":[{"delta":{"content":"Hello"}}]}`,
			want: `"Hello"`,
		},
		{
			name: "non textual stream has no output",
			body: `data: {"candidates":[{"content":{"parts":[{"inlineData":{"mimeType":"image/png"}}]}}]}`,
			want: "",
		},
		{
			name:  "truncated raw stream is marked",
			body:  `data: {"choices":[{"delta":{"content":"Hello"}}]}`,
			total: 4097,
			limit: 4096,
			want:  `{"text":"Hello","truncated":true}`,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			total := testCase.total
			if total == 0 {
				total = len(testCase.body)
			}
			limit := testCase.limit
			if limit == 0 {
				limit = 4096
			}
			got := streamContentValue([]byte(testCase.body), int64(total), limit)
			if testCase.want == "" {
				assert.Nil(t, got)
				return
			}
			raw, ok := got.(common.RawMessage)
			require.True(t, ok, "expected raw JSON, got %T", got)
			assert.JSONEq(t, testCase.want, string(raw))
		})
	}
}

func setCaptureReporter(t *testing.T, maxBytes int) {
	t.Helper()
	previous := DefaultReporter()
	SetDefaultReporter(&Reporter{
		config:  Config{CaptureContent: true, ContentMaxBytes: maxBytes},
		started: true,
	})
	t.Cleanup(func() { SetDefaultReporter(previous) })
}

func newCaptureContext(t *testing.T, body string) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	storage, err := common.CreateBodyStorage([]byte(body))
	require.NoError(t, err)
	t.Cleanup(func() { _ = storage.Close() })
	c.Set(common.KeyBodyStorage, storage)
	return c
}

func TestSendHostSecurity(t *testing.T) {
	t.Run("rejects plain http host", func(t *testing.T) {
		reporter := NewReporter(Config{
			Enabled:    true,
			Host:       "http://127.0.0.1:1",
			PublicKey:  "public",
			SecretKey:  "secret",
			SampleRate: 1,
		})
		closeReporter(t, reporter)
		_, err := reporter.send(context.Background(), []Event{{RequestID: "plain"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "HTTPS")
	})

	t.Run("allows plain http host when insecure", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		reporter := NewReporter(Config{
			Enabled:       true,
			Host:          server.URL,
			PublicKey:     "public",
			SecretKey:     "secret",
			SampleRate:    1,
			QueueSize:     2,
			BatchSize:     5,
			FlushInterval: time.Hour,
			Insecure:      true,
			HTTPClient:    server.Client(),
		})
		closeReporter(t, reporter)
		reporter.Publish(Event{RequestID: "plain"})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, reporter.Flush(ctx))
		assert.Equal(t, int32(1), requests.Load())
	})
}

func TestPartialSuccessCountsRejectedSpans(t *testing.T) {
	server, _ := newOTLPCollector(t, http.StatusOK, `{"partialSuccess":{"rejectedSpans":1,"errorMessage":"invalid span"}}`)
	reporter := NewReporter(Config{
		Enabled:       true,
		Host:          server.URL,
		PublicKey:     "public",
		SecretKey:     "secret",
		SampleRate:    1,
		QueueSize:     4,
		BatchSize:     5,
		FlushInterval: time.Hour,
		HTTPClient:    server.Client(),
	})
	closeReporter(t, reporter)
	reporter.Publish(Event{RequestID: "one"})
	reporter.Publish(Event{RequestID: "two"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, reporter.Flush(ctx))

	assert.Equal(t, Stats{Sent: 1, Failed: 1}, reporter.Stats())
}

func TestCloseIsIdempotent(t *testing.T) {
	reporter := NewReporter(Config{
		Enabled:    true,
		Host:       "https://127.0.0.1:1",
		PublicKey:  "public",
		SecretKey:  "secret",
		SampleRate: 1,
		QueueSize:  1,
		BatchSize:  1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, reporter.Close(ctx))
	require.NoError(t, reporter.Close(ctx))

	reporter.Publish(Event{RequestID: "after-close"})
	assert.Empty(t, reporter.queue)
}

type otlpRequest struct {
	path        string
	username    string
	password    string
	basicOK     bool
	version     string
	contentType string
	payload     tracesPayload
	decodeErr   error
}

// newOTLPCollector starts a TLS collector that records every export request
// and answers with the given status and body. Results are delivered through a
// buffered channel so the handler never shares memory with the test goroutine.
func newOTLPCollector(t *testing.T, status int, responseBody string) (*httptest.Server, <-chan otlpRequest) {
	t.Helper()
	requests := make(chan otlpRequest, 4)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var captured otlpRequest
		captured.path = r.URL.Path
		captured.version = r.Header.Get("x-langfuse-ingestion-version")
		captured.contentType = r.Header.Get("Content-Type")
		captured.username, captured.password, captured.basicOK = r.BasicAuth()
		captured.decodeErr = common.DecodeJson(r.Body, &captured.payload)
		requests <- captured
		w.WriteHeader(status)
		_, _ = w.Write([]byte(responseBody))
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func requireResponse(t *testing.T, requests <-chan otlpRequest) otlpRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	default:
		t.Fatal("no OTLP export request was received")
		return otlpRequest{}
	}
}

func closeReporter(t *testing.T, reporter *Reporter) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = reporter.Close(ctx)
	})
}

func attributeMap(attributes []attribute) map[string]attributeValue {
	result := make(map[string]attributeValue, len(attributes))
	for _, item := range attributes {
		result[item.Key] = item.Value
	}
	return result
}

func attributeString(t *testing.T, attributes map[string]attributeValue, key string) string {
	t.Helper()
	value, ok := attributes[key]
	require.True(t, ok, "missing attribute %s", key)
	require.NotNil(t, value.StringValue, "attribute %s is not a string", key)
	return *value.StringValue
}
