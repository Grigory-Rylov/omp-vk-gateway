package agent

import (
	"strings"
	"testing"
	"time"
)

func TestAgentEndDeliversCleanText(t *testing.T) {
	s := newTestSession(t, 1)
	s.turnActive = true
	s.turnText.WriteString("Всё сделано.")
	s.bridge.handleEvent(s, nil, agentEndFrame(t, nil))

	res, ok := waitForTurn(t, s, time.Second)
	if !ok {
		t.Fatal("turn not delivered")
	}
	if res.aborted || res.text != "Всё сделано." {
		t.Fatalf("unexpected result: %+v", res)
	}
	if s.turnActive {
		t.Fatal("turn still active after delivery")
	}
	if s.badToolRetries != 0 {
		t.Fatalf("counter not zero: %d", s.badToolRetries)
	}
}

func TestAgentEndIgnoresDeferred(t *testing.T) {
	s := newTestSession(t, 1)
	s.turnActive = true
	s.bridge.handleEvent(s, nil, agentEndFrame(t, map[string]interface{}{"isTerminal": false}))

	if _, ok := waitForTurn(t, s, 50*time.Millisecond); ok {
		t.Fatal("deferred agent_end must not deliver")
	}
	if !s.turnActive {
		t.Fatal("turn must stay active on deferred agent_end")
	}
}

func TestAgentEndHidesBadToolCallAndRetries(t *testing.T) {
	s := newTestSession(t, 1)
	s.turnActive = true
	// proc is nil in the test: the retry goroutine must finalize the turn as
	// aborted rather than hang, and the leaked text must not be delivered.
	bad := "<tool_call>\n{\"name\":\"bash\",\"arguments\":{\"command\":\"pwd\"}}\n</tool_call>"
	s.turnText.WriteString(bad)
	s.bridge.handleEvent(s, nil, agentEndFrame(t, nil))

	res, ok := waitForTurn(t, s, time.Second)
	if !ok {
		t.Fatal("turn not delivered after retry failure")
	}
	if !res.aborted {
		t.Fatal("expected aborted result when the retry cannot be sent")
	}
	if res.text != "" {
		t.Fatalf("leaked tool-call text escaped to the user: %q", res.text)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.badToolRetries != 1 {
		t.Fatalf("badToolRetries = %d, want 1", s.badToolRetries)
	}
}

func TestAgentEndDeliversTextAfterRetryBudgetExhausted(t *testing.T) {
	s := newTestSession(t, 1)
	s.turnActive = true
	s.badToolRetries = maxBadToolRetries
	bad := "<function_calls>\n<invoke name=\"bash\"></invoke>\n</function_calls>"
	s.turnText.WriteString(bad)
	s.bridge.handleEvent(s, nil, agentEndFrame(t, nil))

	res, ok := waitForTurn(t, s, time.Second)
	if !ok {
		t.Fatal("turn not delivered after budget exhaustion")
	}
	if res.aborted {
		t.Fatal("budget-exhausted completion must not be marked aborted")
	}
	if !strings.Contains(res.text, "<function_calls>") {
		t.Fatalf("expected the raw text to be delivered as fallback, got %q", res.text)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.badToolRetries != 0 {
		t.Fatalf("counter should reset after delivery, got %d", s.badToolRetries)
	}
}

func TestAgentEndIgnoresInactiveTurn(t *testing.T) {
	s := newTestSession(t, 1)
	s.bridge.handleEvent(s, nil, agentEndFrame(t, nil))

	if _, ok := waitForTurn(t, s, 50*time.Millisecond); ok {
		t.Fatal("agent_end for an inactive turn must not deliver")
	}
}
