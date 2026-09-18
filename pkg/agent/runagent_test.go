// Package agent — tests for run_agent final-text delivery (the side channel
// that carries a directly-launched subagent's output to the VK peer).
package agent

import (
	"strconv"
	"testing"
)

// TestRunAgentTerminalDelivery checks that a terminal subagent_lifecycle
// frame whose parentToolCallId matches an in-flight run_agent delivers the
// subagent's output through the RunAgentResult callback (to the originating
// peer), while ordinary subagent frames (no matching id) do not. The slice
// lives behind a pointer: the callback may reallocate on append.
func TestRunAgentTerminalDelivery(t *testing.T) {
	s := newTestSession(t, 7)
	results := &[]string{}
	s.bridge.SetRunAgentResultCallback(func(peerID int64, text string) error {
		*results = append(*results, peerIDText(peerID, text))
		return nil
	})
	// The coprocess echoes the run_agent command id as parentToolCallId.
	s.runAgentIDs = map[string]bool{"vk-7-1": true}

	// started: tracked but not terminal — nothing delivered.
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_lifecycle", map[string]interface{}{
		"id": "sub-9", "agent": "developer", "index": 1, "status": "started",
		"description": "Ответь одним словом", "parentToolCallId": "vk-7-1",
	}))
	assertLines(t, *results, []string{})

	// completed: terminal frame carrying output — delivered to the peer.
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_lifecycle", map[string]interface{}{
		"id": "sub-9", "agent": "developer", "index": 1, "status": "completed",
		"output": "ок", "parentToolCallId": "vk-7-1",
	}))
	assertLines(t, *results, []string{"7|" + "ок"})

	// The id is consumed: a repeated frame with the same id must not redeliver.
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_lifecycle", map[string]interface{}{
		"id": "sub-9", "agent": "developer", "index": 1, "status": "completed",
		"output": "ок", "parentToolCallId": "vk-7-1",
	}))
	assertLines(t, *results, []string{"7|" + "ок"})
}

// TestRunAgentFailedDelivery checks that a failed run delivers the error form
// and an unrelated subagent (parentToolCallId not in the in-flight set) is
// never delivered.
func TestRunAgentFailedDelivery(t *testing.T) {
	s := newTestSession(t, 7)
	results := &[]string{}
	s.bridge.SetRunAgentResultCallback(func(peerID int64, text string) error {
		*results = append(*results, peerIDText(peerID, text))
		return nil
	})
	s.runAgentIDs = map[string]bool{"vk-7-1": true}

	// A run_agent that failed: error text, no output.
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_lifecycle", map[string]interface{}{
		"id": "sub-9", "agent": "developer", "index": 1, "status": "failed",
		"output": "tool exploded", "parentToolCallId": "vk-7-1",
	}))
	assertLines(t, *results, []string{"7|⚠️ developer ошибка\ntool exploded"})

	// An unrelated model-launched subagent: parentToolCallId not in the set —
	// its terminal frame must not reach the VK peer.
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_lifecycle", map[string]interface{}{
		"id": "sub-10", "agent": "qa", "index": 2, "status": "completed",
		"output": "all green", "parentToolCallId": "model-tool-call-xyz",
	}))
	assertLines(t, *results, []string{"7|⚠️ developer ошибка\ntool exploded"})
}

// TestRunAgentNoCallback checks that without a registered sink a matching
// terminal frame is dropped silently (no panic, no mirror-side effect).
func TestRunAgentNoCallback(t *testing.T) {
	s := newTestSession(t, 7)
	s.runAgentIDs = map[string]bool{"vk-7-1": true}

	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_lifecycle", map[string]interface{}{
		"id": "sub-9", "agent": "developer", "index": 1, "status": "completed",
		"output": "ок", "parentToolCallId": "vk-7-1",
	}))
	if len(s.runAgentIDs) != 0 {
		t.Fatalf("terminal frame must consume the in-flight id, got %v", s.runAgentIDs)
	}
}

func peerIDText(peerID int64, text string) string {
	return strconv.FormatInt(peerID, 10) + "|" + text
}
