package langfuse

import (
	"strconv"
	"strings"

	"github.com/QuantumNous/new-api/common"
)

// streamContentAccumulator rebuilds the final assistant output from SSE
// chunks. Langfuse only needs the finished content, so the per-chunk events are
// folded into text, reasoning, and tool calls instead of being stored verbatim.
type streamContentAccumulator struct {
	text          strings.Builder
	reasoning     strings.Builder
	finalText     strings.Builder
	finalReason   strings.Builder
	tools         []streamToolCall
	toolPositions map[int]int
	toolNames     map[string]int
}

type streamToolCall struct {
	Name      string
	Arguments string
}

func (a *streamContentAccumulator) observe(event any) {
	object, ok := event.(map[string]any)
	if !ok {
		return
	}
	a.observeOpenAI(object)
	a.observeClaude(object)
	a.observeResponses(object)
	a.observeGemini(object)
}

func (a *streamContentAccumulator) observeOpenAI(object map[string]any) {
	choices, ok := object["choices"].([]any)
	if !ok {
		return
	}
	for _, rawChoice := range choices {
		choice, ok := rawChoice.(map[string]any)
		if !ok {
			continue
		}
		if delta, ok := choice["delta"].(map[string]any); ok {
			a.addText(contentText(delta["content"]))
			a.addReasoning(openAIReasoning(delta))
			a.observeOpenAIToolDeltas(delta["tool_calls"])
		}
		a.addText(contentText(choice["text"]))
		if message, ok := choice["message"].(map[string]any); ok {
			a.addFinalText(contentText(message["content"]))
			a.addFinalReasoning(openAIReasoning(message))
		}
	}
}

func (a *streamContentAccumulator) observeOpenAIToolDeltas(raw any) {
	calls, ok := raw.([]any)
	if !ok {
		return
	}
	for _, rawCall := range calls {
		call, ok := rawCall.(map[string]any)
		if !ok {
			continue
		}
		position := a.toolPosition(intValue(call["index"]))
		if function, ok := call["function"].(map[string]any); ok {
			a.setToolName(position, stringValue(function["name"]))
			a.appendToolArguments(position, stringValue(function["arguments"]))
		}
	}
}

func (a *streamContentAccumulator) observeClaude(object map[string]any) {
	switch stringValue(object["type"]) {
	case "content_block_start":
		block, _ := object["content_block"].(map[string]any)
		switch stringValue(block["type"]) {
		case "text":
			a.addText(stringValue(block["text"]))
		case "thinking":
			a.addReasoning(stringValue(block["thinking"]))
		case "tool_use":
			position := a.toolPosition(intValue(object["index"]))
			a.setToolName(position, stringValue(block["name"]))
			// Claude normally sends an empty input object here and streams the
			// real arguments as input_json_delta fragments.
			if input := block["input"]; input != nil {
				if arguments := jsonString(input); arguments != "" && arguments != "{}" {
					a.setToolArguments(position, arguments)
				}
			}
		}
	case "content_block_delta":
		delta, _ := object["delta"].(map[string]any)
		switch stringValue(delta["type"]) {
		case "text_delta":
			a.addText(stringValue(delta["text"]))
		case "thinking_delta":
			a.addReasoning(stringValue(delta["thinking"]))
		case "input_json_delta":
			position := a.toolPosition(intValue(object["index"]))
			a.appendToolArguments(position, stringValue(delta["partial_json"]))
		}
	}
}

func (a *streamContentAccumulator) observeResponses(object map[string]any) {
	switch stringValue(object["type"]) {
	case "response.output_text.delta", "response.refusal.delta":
		a.addText(stringValue(object["delta"]))
	case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
		a.addReasoning(stringValue(object["delta"]))
	case "response.function_call_arguments.delta":
		position := a.toolPosition(intValue(object["output_index"]))
		a.setToolName(position, stringValue(object["name"]))
		a.appendToolArguments(position, stringValue(object["delta"]))
	case "response.output_item.added", "response.output_item.done":
		item, _ := object["item"].(map[string]any)
		switch stringValue(item["type"]) {
		case "function_call":
			position := a.toolPosition(intValue(object["output_index"]))
			a.setToolName(position, stringValue(item["name"]))
			if arguments := stringValue(item["arguments"]); arguments != "" {
				a.setToolArguments(position, arguments)
			}
		case "message":
			a.addFinalText(responsesItemText(item))
		case "reasoning":
			a.addFinalReasoning(responsesItemReasoning(item))
		}
	case "response.completed", "response.done":
		response, _ := object["response"].(map[string]any)
		a.observeResponsesTerminal(response)
	}
}

func (a *streamContentAccumulator) observeResponsesTerminal(response map[string]any) {
	output, ok := response["output"].([]any)
	if !ok {
		return
	}
	for index, rawItem := range output {
		item, ok := rawItem.(map[string]any)
		if !ok {
			continue
		}
		switch stringValue(item["type"]) {
		case "message":
			a.addFinalText(responsesItemText(item))
		case "reasoning":
			a.addFinalReasoning(responsesItemReasoning(item))
		case "function_call":
			position := a.toolPosition(index)
			a.setToolName(position, stringValue(item["name"]))
			a.setToolArguments(position, stringValue(item["arguments"]))
		}
	}
}

func (a *streamContentAccumulator) observeGemini(object map[string]any) {
	candidates, ok := object["candidates"].([]any)
	if !ok {
		return
	}
	for _, rawCandidate := range candidates {
		candidate, ok := rawCandidate.(map[string]any)
		if !ok {
			continue
		}
		content, ok := candidate["content"].(map[string]any)
		if !ok {
			continue
		}
		parts, ok := content["parts"].([]any)
		if !ok {
			continue
		}
		for _, rawPart := range parts {
			part, ok := rawPart.(map[string]any)
			if !ok {
				continue
			}
			a.addText(stringValue(part["text"]))
			if call, ok := part["functionCall"].(map[string]any); ok {
				a.observeGeminiFunctionCall(call)
			}
		}
	}
}

func (a *streamContentAccumulator) observeGeminiFunctionCall(call map[string]any) {
	name := stringValue(call["name"])
	position := a.geminiToolPosition(name)
	a.setToolName(position, name)
	if arguments, exists := call["args"]; exists {
		a.setToolArguments(position, jsonString(arguments))
	}
}

func (a *streamContentAccumulator) value() any {
	text := a.text.String()
	if text == "" {
		text = a.finalText.String()
	}
	reasoning := a.reasoning.String()
	if reasoning == "" {
		reasoning = a.finalReason.String()
	}
	tools := a.toolValues()
	if text == "" && reasoning == "" && len(tools) == 0 {
		return nil
	}
	if reasoning == "" && len(tools) == 0 {
		return text
	}
	value := map[string]any{}
	if text != "" {
		value["text"] = text
	}
	if reasoning != "" {
		value["reasoning"] = reasoning
	}
	if len(tools) > 0 {
		value["tool_calls"] = tools
	}
	return value
}

func (a *streamContentAccumulator) toolValues() []map[string]any {
	values := make([]map[string]any, 0, len(a.tools))
	for _, tool := range a.tools {
		value := map[string]any{}
		if tool.Name != "" {
			value["name"] = tool.Name
		}
		if tool.Arguments != "" {
			value["arguments"] = tool.Arguments
		}
		if len(value) > 0 {
			values = append(values, value)
		}
	}
	return values
}

func (a *streamContentAccumulator) toolPosition(index int) int {
	if a.toolPositions == nil {
		a.toolPositions = map[int]int{}
	}
	if position, exists := a.toolPositions[index]; exists {
		return position
	}
	a.tools = append(a.tools, streamToolCall{})
	a.toolPositions[index] = len(a.tools) - 1
	return len(a.tools) - 1
}

func (a *streamContentAccumulator) geminiToolPosition(name string) int {
	if a.toolNames == nil {
		a.toolNames = map[string]int{}
	}
	if position, exists := a.toolNames[name]; exists {
		return position
	}
	a.tools = append(a.tools, streamToolCall{})
	position := len(a.tools) - 1
	if name != "" {
		a.toolNames[name] = position
	}
	return position
}

func (a *streamContentAccumulator) setToolName(position int, name string) {
	if name == "" || position < 0 || position >= len(a.tools) {
		return
	}
	a.tools[position].Name = name
}

func (a *streamContentAccumulator) appendToolArguments(position int, fragment string) {
	if fragment == "" || position < 0 || position >= len(a.tools) {
		return
	}
	a.tools[position].Arguments = mergeStreamText(a.tools[position].Arguments, fragment)
}

func (a *streamContentAccumulator) setToolArguments(position int, arguments string) {
	if arguments == "" || position < 0 || position >= len(a.tools) {
		return
	}
	a.tools[position].Arguments = arguments
}

func (a *streamContentAccumulator) addText(text string) {
	writeStreamText(&a.text, text)
}

func (a *streamContentAccumulator) addReasoning(text string) {
	writeStreamText(&a.reasoning, text)
}

func (a *streamContentAccumulator) addFinalText(text string) {
	writeStreamText(&a.finalText, text)
}

func (a *streamContentAccumulator) addFinalReasoning(text string) {
	writeStreamText(&a.finalReason, text)
}

func writeStreamText(builder *strings.Builder, fragment string) {
	if fragment == "" {
		return
	}
	current := builder.String()
	if current != "" && strings.HasPrefix(fragment, current) {
		builder.Reset()
		builder.WriteString(fragment)
		return
	}
	builder.WriteString(fragment)
}

// mergeStreamText appends incremental chunks while tolerating providers that
// resend the whole value so far in each event.
func mergeStreamText(current string, fragment string) string {
	if current == "" {
		return fragment
	}
	if strings.HasPrefix(fragment, current) {
		return fragment
	}
	return current + fragment
}

func openAIReasoning(source map[string]any) string {
	if reasoning := contentText(source["reasoning_content"]); reasoning != "" {
		return reasoning
	}
	return contentText(source["reasoning"])
}

func contentText(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []any:
		var builder strings.Builder
		for _, rawPart := range typed {
			switch part := rawPart.(type) {
			case string:
				builder.WriteString(part)
			case map[string]any:
				builder.WriteString(stringValue(part["text"]))
			}
		}
		return builder.String()
	case map[string]any:
		return stringValue(typed["text"])
	default:
		return ""
	}
}

func responsesItemText(item map[string]any) string {
	content, ok := item["content"].([]any)
	if !ok {
		return ""
	}
	var builder strings.Builder
	for _, rawPart := range content {
		part, ok := rawPart.(map[string]any)
		if !ok {
			continue
		}
		switch stringValue(part["type"]) {
		case "output_text", "text":
			builder.WriteString(stringValue(part["text"]))
		case "refusal":
			builder.WriteString(stringValue(part["refusal"]))
		}
	}
	return builder.String()
}

func responsesItemReasoning(item map[string]any) string {
	var builder strings.Builder
	if summary, ok := item["summary"].([]any); ok {
		for _, rawPart := range summary {
			if part, ok := rawPart.(map[string]any); ok {
				builder.WriteString(stringValue(part["text"]))
			}
		}
	}
	if content, ok := item["content"].([]any); ok {
		for _, rawPart := range content {
			if part, ok := rawPart.(map[string]any); ok {
				builder.WriteString(stringValue(part["text"]))
			}
		}
	}
	return builder.String()
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case string:
		parsed, err := strconv.Atoi(typed)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

func jsonString(value any) string {
	data, err := common.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}
