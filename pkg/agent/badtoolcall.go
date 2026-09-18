package agent

import (
	"regexp"
	"strings"
)

// maxBadToolRetries caps how many consecutive terminal completions whose
// final text is a malformed tool-call attempt we auto-retry within one user
// turn, so a persistently broken model cannot hang the turn forever.
const maxBadToolRetries = 2

// nudgeBadToolCall is sent to the model when its reply leaked a malformed
// tool-call attempt as visible text: the attempt is hidden from the chat and
// the model is asked to try again.
const nudgeBadToolCall = "Your last reply contained a malformed tool call: the tool invocation was emitted as raw text (XML/JSON) instead of being sent through the tool-calling protocol, so it was not executed and was hidden from the user. Please try again: if you intended to call a tool, issue it using the correct tool-calling format; if you did not intend to call a tool, continue with plain text."

// badToolMarkers are opening markers of the in-band tool-call dialects that
// oh-my-pi knows (qwen3, hermes, glm, anthropic, minimax, deepseek, harmony,
// kimi, gemma, legacy pi-xml), plus the bare <function=NAME> variant. If the
// final visible text of a turn contains one, the model almost certainly tried
// to call a tool in plain text instead of using the native protocol — and
// since nothing was executed, the agent loop terminated on that message.
var badToolMarkers = []string{
	"<tool_call",          // qwen3 <tool_call>…, qwen-xml <tool_calls>, hermes
	"<function_call",      // anthropic/minimax in-band <function_calls>
	"<function=",          // legacy pi-xml, e.g. <function=read>
	"<invoke ",            // anthropic/minimax in-band
	"<invoke>",            // anthropic/minimax in-band
	"<parameter name=",    // anthropic/minimax in-band
	"<minimax:tool_call",  // minimax in-band
	"<|DSML|tool_calls",   // deepseek DSML (ASCII)
	"<｜DSML｜tool_calls",   // deepseek DSML (fullwidth)
	"<|tool_call",         // kimi/gemma sigil blocks
	"<|start|>functions.", // harmony
	"<arg_key>",           // glm
	"<arg_value>",         // glm
	"<tool_name>",         // legacy pi
}

// bareJSONToolCallRe matches a tool call emitted as a bare JSON object in
// text, e.g. {"name":"bash","arguments":{…}}, without wrapper tags.
var bareJSONToolCallRe = regexp.MustCompile(`"name"\s*:.*"arguments"`)

// looksLikeBadToolCall reports whether text looks like a leaked or malformed
// tool-call attempt rather than a normal reply.
func looksLikeBadToolCall(text string) bool {
	for _, m := range badToolMarkers {
		if strings.Contains(text, m) {
			return true
		}
	}
	return bareJSONToolCallRe.MatchString(text)
}
