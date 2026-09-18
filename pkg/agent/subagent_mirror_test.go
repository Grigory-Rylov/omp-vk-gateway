package agent

import (
	"encoding/json"
	"slices"
	"testing"
)

// subagentFrame builds a JSON line for a subagent_* event.
func subagentFrame(t *testing.T, typ string, payload map[string]interface{}) string {
	t.Helper()
	b, err := json.Marshal(map[string]interface{}{"type": typ, "payload": payload})
	if err != nil {
		t.Fatalf("marshal %s frame: %v", typ, err)
	}
	return string(b)
}

// startMirrorCapture wires a thinking callback on the session's bridge that
// records every mirrored line, so tests can assert the exact reasoning-chat
// output produced by the subagent handlers. The slice lives behind a pointer:
// the callback may reallocate on append.
func (s *peerSession) startMirrorCapture(t *testing.T) *[]string {
	t.Helper()
	lines := &[]string{}
	s.bridge.SetThinkingCallback(func(_ int64, line string) error {
		*lines = append(*lines, line)
		return nil
	})
	return lines
}

func assertLines(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("mirror lines mismatch:\n got: %q\nwant: %q", got, want)
	}
}

// TestSubagentLifecycleAndProgress checks the reasoning-chat lines for
// subagent lifecycle + progress, including the dedup invariant: an unchanged
// progress gist must not be re-mirrored (upstream coalesces to ~6/s and would
// otherwise trip VK flood control).
func TestSubagentLifecycleAndProgress(t *testing.T) {
	s := newTestSession(t, 7)
	lines := s.startMirrorCapture(t)

	// started (with description)
	frame, _ := json.Marshal(map[string]interface{}{
		"type":    "subagent_lifecycle",
		"payload": map[string]interface{}{"id": "sub-1", "agent": "developer", "index": 1, "status": "started", "description": "Ответь одним словом"},
	})
	s.bridge.handleEvent(s, nil, string(frame))
	assertLines(t, *lines, []string{
		"\U0001F916 субагент developer [#1] запущен: Ответь одним словом",
	})

	// progress: first gist is new -> mirrored
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_progress", map[string]interface{}{
		"index": 1, "agent": "developer",
		"progress": map[string]interface{}{"id": "sub-1", "lastIntent": "Читаю код"},
	}))
	assertLines(t, *lines, []string{
		"\U0001F916 субагент developer [#1] запущен: Ответь одним словом",
		"⏳ developer: Читаю код",
	})

	// progress: same gist again -> deduped, no new line
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_progress", map[string]interface{}{
		"index": 1, "agent": "developer",
		"progress": map[string]interface{}{"id": "sub-1", "lastIntent": "Читаю код"},
	}))
	assertLines(t, *lines, []string{
		"\U0001F916 субагент developer [#1] запущен: Ответь одним словом",
		"⏳ developer: Читаю код",
	})

	// progress: changed gist (currentTool, lastIntent empty) -> mirrored
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_progress", map[string]interface{}{
		"index": 1, "agent": "developer",
		"progress": map[string]interface{}{"id": "sub-1", "currentTool": "bash"},
	}))
	assertLines(t, *lines, []string{
		"\U0001F916 субагент developer [#1] запущен: Ответь одним словом",
		"⏳ developer: Читаю код",
		"⏳ developer: выполняет bash",
	})

	// completed: terminal line; state cleared for this subagent
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_lifecycle", map[string]interface{}{
		"id": "sub-1", "agent": "developer", "index": 1, "status": "completed",
	}))
	assertLines(t, *lines, []string{
		"\U0001F916 субагент developer [#1] запущен: Ответь одним словом",
		"⏳ developer: Читаю код",
		"⏳ developer: выполняет bash",
		"\U0001F916 субагент developer [#1] завершён",
	})
}

// TestSubagentEventToolAndThinking checks that a subagent tool start renders
// as a prefixed tool line, and that reasoning deltas are buffered and flushed
// as a single line on the assistant message_end (per-token sends would trip
// VK flood control).
func TestSubagentEventToolAndThinking(t *testing.T) {
	s := newTestSession(t, 7)
	lines := s.startMirrorCapture(t)

	// Establish the subagent identity so tool/thinking lines carry its name.
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_lifecycle", map[string]interface{}{
		"id": "sub-1", "agent": "reviewer", "index": 2, "status": "started",
	}))

	// tool start
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_event", map[string]interface{}{
		"id":    "sub-1",
		"event": map[string]interface{}{"type": "tool_execution_start", "toolName": "read", "args": map[string]interface{}{"path": "/x"}},
	}))

	// reasoning: buffered across deltas, flushed once on message_end
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_event", map[string]interface{}{
		"id": "sub-1", "event": map[string]interface{}{"type": "message_start", "message": map[string]interface{}{"role": "assistant"}},
	}))
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_event", map[string]interface{}{
		"id": "sub-1", "event": map[string]interface{}{"type": "message_update", "assistantMessageEvent": map[string]interface{}{"type": "thinking_delta", "delta": "смотрю "}},
	}))
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_event", map[string]interface{}{
		"id": "sub-1", "event": map[string]interface{}{"type": "message_update", "assistantMessageEvent": map[string]interface{}{"type": "thinking_delta", "delta": "заготовку"}},
	}))
	// no flush before message_end: only the tool line so far
	assertLines(t, *lines, []string{
		"\U0001F916 субагент reviewer [#2] запущен",
		"\U0001F916 reviewer: 📖 чтение файла: {\"path\":\"/x\"}",
	})

	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_event", map[string]interface{}{
		"id": "sub-1", "event": map[string]interface{}{"type": "message_end", "message": map[string]interface{}{"role": "assistant"}},
	}))
	assertLines(t, *lines, []string{
		"\U0001F916 субагент reviewer [#2] запущен",
		"\U0001F916 reviewer: 📖 чтение файла: {\"path\":\"/x\"}",
		"\U0001F4AD reviewer: смотрю заготовку",
	})
}
