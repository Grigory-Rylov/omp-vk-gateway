package vk

import (
	"testing"
)

func TestNormalizeAgentMentions(t *testing.T) {
	h := &BotHandler{agentNames: defaultAgentNames()}

	cases := []struct {
		in, want string
	}{
		{"#lead fix the bug", "@lead fix the bug"},
		{"#Lead fix the bug", "@lead fix the bug"}, // case-folded: agent names are case-insensitive
		{"#developer review this", "@developer review this"},
		{"#reviewer please", "@reviewer please"},
		{"#qa run tests", "@qa run tests"},
		{"#include <x.h>", "#include <x.h>"},     // not an agent
		{"#1 unchanged", "#1 unchanged"},         // not an identifier
		{"plain message", "plain message"},       // no '#'
		{"#lead", "@lead"},                       // bare token
		{"#lead #developer", "@lead @developer"}, // multiple
	}
	for _, c := range cases {
		if got := h.normalizeAgentMentions(c.in); got != c.want {
			t.Errorf("normalizeAgentMentions(%q) = %q, want %q", c.in, got, c.want)
		}
	}

	// Empty agent set disables rewriting entirely.
	h2 := &BotHandler{agentNames: map[string]bool{}}
	if got := h2.normalizeAgentMentions("#lead fix"); got != "#lead fix" {
		t.Errorf("disabled: got %q, want %q", got, "#lead fix")
	}

	// SetAgentNames overrides the set: 'lead' still works, 'scout' no longer
	// (it's a default but not in this custom list).
	h.SetAgentNames([]string{"lead"})
	if got := h.normalizeAgentMentions("#lead x"); got != "@lead x" {
		t.Errorf("custom set lead: got %q, want @lead x", got)
	}
	if got := h.normalizeAgentMentions("#scout x"); got != "#scout x" {
		t.Errorf("custom set scout should be untouched: got %q, want #scout x", got)
	}

	// SetAgentNames(nil) disables.
	h.SetAgentNames(nil)
	if got := h.normalizeAgentMentions("#lead x"); got != "#lead x" {
		t.Errorf("SetAgentNames(nil) should disable: got %q", got)
	}
}
func TestSanitizeMentionAll(t *testing.T) {
	cases := []struct{ in, want string }{
		{"*all", "＊all"},
		{"*все", "＊все"},
		{"--include=*all *.go", "--include=＊all *.go"},
		{"pkill *все процессы", "pkill ＊все процессы"},
		{"**all", "*＊all"}, // bold form carries the trigger too
		{"*ALL", "＊ALL"},   // case-folded, word case preserved
		{"*ВСЕ", "＊ВСЕ"},
		{"grep install .", "grep install ."},   // "all" without '*'
		{"**bold** text", "**bold** text"},     // no trigger
		{"*.txt *.md", "*.txt *.md"},           // glob, not a trigger
		{"3 * all of them", "3 * all of them"}, // '*' not glued to the word
	}
	for _, c := range cases {
		if got := SanitizeMentionAll(c.in); got != c.want {
			t.Errorf("SanitizeMentionAll(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
