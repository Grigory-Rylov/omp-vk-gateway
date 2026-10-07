// Package vk — agent-mention normalization for the VK gateway.
package vk

import (
	"regexp"
	"strings"
)

// agentMentionRe matches a '#' followed by an identifier token. The candidate
// is only rewritten when it names a known agent (see defaultAgentNames /
// SetAgentNames), so free text like "#include" or "#1" is left untouched.
var agentMentionRe = regexp.MustCompile(`#([A-Za-z][A-Za-z0-9_-]*)`)

// defaultAgentNames is the built-in set of agent names recognized by
// normalizeAgentMentions when the gateway is not given an explicit list.
func defaultAgentNames() map[string]bool {
	names := []string{"lead", "developer", "reviewer", "qa", "scout", "task"}
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	return set
}

// SetAgentNames overrides the set of agent names that normalizeAgentMentions
// rewrites. Pass an empty slice to disable rewriting entirely.
func (h *BotHandler) SetAgentNames(names []string) {
	set := make(map[string]bool, len(names))
	for _, n := range names {
		n = strings.ToLower(strings.TrimSpace(n))
		if n != "" {
			set[n] = true
		}
	}
	h.agentNames = set
}

// normalizeAgentMentions rewrites '#name' to '@name' for each '#name' whose
// name (lowercased) is a known agent. VK mangles user-typed '@name' into an
// '[id|@name]' mention token, so callers write '#name' in the chat; the
// gateway forwards '@name' to the agent, which dispatches to the named
// subagent (the main session interprets '@<agent>' via its system prompt).
func (h *BotHandler) normalizeAgentMentions(message string) string {
	if len(h.agentNames) == 0 {
		return message
	}
	return agentMentionRe.ReplaceAllStringFunc(message, func(m string) string {
		name := strings.ToLower(m[1:])
		if h.agentNames[name] {
			return "@" + name
		}
		return m
	})
}

// mentionAllRe matches VK's mention-all trigger: an ASCII '*' glued to "all"
// or "все". Thinking lines carry shell globs and flags ("--include=*all",
// "pkill *все") that would otherwise ping every member of the reasoning chat.
var mentionAllRe = regexp.MustCompile(`(?i)\*(all|все)`)

// SanitizeMentionAll neutralizes the trigger by swapping the ASCII '*' for
// the fullwidth '＊', which VK does not parse and which reads identically.
func SanitizeMentionAll(text string) string {
	return mentionAllRe.ReplaceAllString(text, "＊$1")
}

// parseRunAgentDispatch reports whether message is a direct dispatch to a
// known agent: a leading "@<name>" or "#<name>" (the VK chat encodes a typed
// "@name" as an "[id|@name]" token, which extractCommand strips to "@name")
// whose name is a valid agent token. It returns the lowercased name and the
// trimmed remainder after the token; ok is false with no leading @/# token.
// Callers pass the message after normalizeAgentMentions; whether the name is
// a configured agent is enforced by the caller via the routing set.
func parseRunAgentDispatch(message string) (agent, rest string, ok bool) {
	t := strings.TrimSpace(message)
	if !strings.HasPrefix(t, "@") && !strings.HasPrefix(t, "#") {
		return "", "", false
	}
	i := 1
	for i < len(t) {
		c := t[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' {
			i++
		} else {
			break
		}
	}
	name := strings.ToLower(t[1:i])
	if name == "" {
		return "", "", false
	}
	return name, strings.TrimSpace(t[i:]), true
}
