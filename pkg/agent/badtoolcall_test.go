package agent

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// newTestSession builds an isolated peerSession with no live process: the
// bridge owns no subprocess, so turn handlers that would reach for s.proc see
// a nil process (retry paths finalize the turn as aborted instead of hanging).
func newTestSession(t *testing.T, peerID int64) *peerSession {
	t.Helper()
	b := &Bridge{peers: map[int64]*peerSession{}}
	s := &peerSession{
		bridge:        b,
		peerID:        peerID,
		turnDone:      make(chan turnResult, 8),
		resetCh:       make(chan struct{}, 8),
		subagentLine:  map[string]string{},
		subagentThink: map[string]*strings.Builder{},
		subagentName:  map[string]string{},
		inFlightRuns:  map[string]*runDescriptor{},
	}
	b.peers[peerID] = s
	return s
}

// agentEndFrame builds a JSON line for an agent_end event. extra keys (e.g.
// isTerminal, willContinue) are merged onto the base frame.
func agentEndFrame(t *testing.T, extra map[string]interface{}) string {
	t.Helper()
	frame := map[string]interface{}{"type": "agent_end"}
	for k, v := range extra {
		frame[k] = v
	}
	b, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal agent_end frame: %v", err)
	}
	return string(b)
}

// waitForTurn waits up to timeout for the session to deliver a turn result.
func waitForTurn(t *testing.T, s *peerSession, timeout time.Duration) (turnResult, bool) {
	t.Helper()
	select {
	case res := <-s.turnDone:
		return res, true
	case <-time.After(timeout):
		return turnResult{}, false
	}
}
