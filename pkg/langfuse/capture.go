package langfuse

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/QuantumNous/new-api/common"

	"github.com/gin-gonic/gin"
)

const capturedResponseKey = "langfuse_captured_response"

// AttachContentCapture wraps the response writer so the body written to the
// client can be reported as the Langfuse observation output. It is a no-op
// unless LANGFUSE_CAPTURE_CONTENT is enabled, and it must run before the relay
// handler writes any response bytes.
func AttachContentCapture(c *gin.Context) {
	if c == nil || c.Writer == nil {
		return
	}
	limit, enabled := contentCaptureConfig()
	if !enabled {
		return
	}
	if _, exists := c.Get(capturedResponseKey); exists {
		return
	}
	capture := &responseCapture{ResponseWriter: c.Writer, limit: limit}
	c.Set(capturedResponseKey, capture)
	c.Writer = capture
}

// CapturedContent returns the request and response bodies recorded for the
// current gin request. It returns nil values when content capture is disabled
// or the corresponding body is unavailable.
func CapturedContent(c *gin.Context) (any, any) {
	limit, enabled := contentCaptureConfig()
	if !enabled || c == nil {
		return nil, nil
	}
	var input any
	if data, size, ok := requestBodyPreview(c, limit); ok {
		input = contentValue(data, size, limit)
	}
	var output any
	if capture := capturedResponse(c); capture != nil {
		if data, total := capture.snapshot(); total > 0 {
			output = capture.contentValue(data, total, limit)
		}
	}
	return input, output
}

func contentCaptureConfig() (int, bool) {
	reporter := DefaultReporter()
	if reporter == nil || !reporter.started || !reporter.config.CaptureContent {
		return 0, false
	}
	limit := reporter.config.ContentMaxBytes
	if limit <= 0 {
		limit = 16384
	}
	return limit, true
}

// responseCapture copies at most limit bytes of the response body while still
// forwarding every write to the wrapped writer. Captured bytes are bounded so a
// large stream cannot grow the request's memory footprint without limit.
type responseCapture struct {
	gin.ResponseWriter

	mu      sync.Mutex
	limit   int
	total   int64
	capture bytes.Buffer
}

func (w *responseCapture) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	if n > 0 {
		w.record(data[:n])
	}
	return n, err
}

func (w *responseCapture) WriteString(data string) (int, error) {
	n, err := w.ResponseWriter.WriteString(data)
	if n > 0 {
		w.record([]byte(data[:n]))
	}
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying connection.
func (w *responseCapture) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *responseCapture) record(data []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.total += int64(len(data))
	if remaining := w.limit - w.capture.Len(); remaining > 0 {
		if len(data) <= remaining {
			w.capture.Write(data)
		} else {
			w.capture.Write(data[:remaining])
		}
	}
}

func (w *responseCapture) snapshot() ([]byte, int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.capture.Bytes()...), w.total
}

func (w *responseCapture) contentValue(data []byte, total int64, limit int) any {
	contentType := strings.ToLower(w.Header().Get("Content-Type"))
	if strings.Contains(contentType, "text/event-stream") {
		return streamContentValue(data, total, limit)
	}
	return contentValue(data, int(total), limit)
}

func capturedResponse(c *gin.Context) *responseCapture {
	value, exists := c.Get(capturedResponseKey)
	if !exists {
		return nil
	}
	capture, _ := value.(*responseCapture)
	return capture
}

// requestBodyPreview reads a bounded prefix from the cached request body. It
// never consumes c.Request.Body, so reporting cannot change relay behavior.
func requestBodyPreview(c *gin.Context, limit int) ([]byte, int, bool) {
	if storage, exists := c.Get(common.KeyBodyStorage); exists && storage != nil {
		if body, ok := storage.(common.BodyStorage); ok {
			reader, err := body.NewReader()
			if err != nil {
				return nil, 0, false
			}
			defer func() { _ = reader.Close() }()
			data, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
			if err != nil {
				return nil, 0, false
			}
			return data, int(body.Size()), true
		}
	}
	if cached, exists := c.Get(common.KeyRequestBody); exists {
		if data, ok := cached.([]byte); ok && len(data) > 0 {
			return data, len(data), true
		}
	}
	return nil, 0, false
}

// contentValue turns raw content into a value that contentJSON can report
// without a second truncation pass: valid JSON stays a JSON object/array and
// oversized content becomes an explicit truncation marker.
func contentValue(data []byte, originalSize int, limit int) any {
	if len(data) > limit {
		data = data[:limit]
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil
	}
	if originalSize <= limit && isJSON(trimmed) {
		return common.RawMessage(trimmed)
	}
	if originalSize <= limit {
		return string(trimmed)
	}
	return truncatedValue(trimmed, originalSize, limit)
}

// streamContentValue folds SSE chunks into the finished assistant output.
// Storing every chunk wastes space and is unreadable in Langfuse, so only the
// assembled text, reasoning, and tool calls are reported.
func streamContentValue(data []byte, total int64, limit int) any {
	accumulator := &streamContentAccumulator{}
	for line := range strings.SplitSeq(string(data), "\n") {
		payload, ok := strings.CutPrefix(strings.TrimSpace(line), "data:")
		if !ok {
			continue
		}
		payload = strings.TrimSpace(payload)
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var event any
		if err := common.UnmarshalJsonStr(payload, &event); err != nil {
			continue
		}
		accumulator.observe(event)
	}
	value := accumulator.value()
	if value == nil {
		return nil
	}
	if total > int64(limit) {
		value = withStreamTruncation(value)
	}
	encoded, err := common.Marshal(value)
	if err != nil {
		return nil
	}
	if len(encoded) <= limit {
		return common.RawMessage(encoded)
	}
	return truncatedValue(encoded, len(encoded), limit)
}

func withStreamTruncation(value any) any {
	if text, ok := value.(string); ok {
		return map[string]any{"text": text, "truncated": true}
	}
	if object, ok := value.(map[string]any); ok {
		object["truncated"] = true
		return object
	}
	return value
}

func truncatedValue(data []byte, originalSize int, limit int) any {
	preview := strings.ToValidUTF8(string(data), "\uFFFD")
	for len(preview) > 0 {
		marker, err := common.Marshal(truncatedContent{
			Truncated:     true,
			OriginalBytes: originalSize,
			Preview:       preview,
		})
		if err != nil {
			return nil
		}
		if len(marker) <= limit {
			return common.RawMessage(marker)
		}
		preview = strings.ToValidUTF8(preview[:len(preview)*3/4], "\uFFFD")
	}
	marker, err := common.Marshal(truncatedContent{Truncated: true, OriginalBytes: originalSize})
	if err != nil {
		return nil
	}
	return common.RawMessage(marker)
}

func isJSON(data []byte) bool {
	var value any
	return common.Unmarshal(data, &value) == nil
}
