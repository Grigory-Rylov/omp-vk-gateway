// Package vk — tests for run_agent direct dispatch routing.
package vk

import (
	"context"
	"testing"
)

func TestParseRunAgentDispatch(t *testing.T) {
	cases := []struct {
		in        string
		wantAgent string
		wantRest  string
		wantOK    bool
	}{
		{`@developer fix the build`, "developer", "fix the build", true},
		{`#developer fix the build`, "developer", "fix the build", true},
		{`@qa`, "qa", "", true},
		{`@QA run checks`, "qa", "run checks", true},
		{`@Lead`, "lead", "", true},
		{`@unknown do it`, "unknown", "do it", true}, // token parses; caller gates on agentNames
		{`hello world`, "", "", false},
		{`@`, "", "", false},
		{`x @developer y`, "", "", false}, // only a leading token dispatches
		{`@dev-1_x go`, "dev-1_x", "go", true},
		{`  @developer   spaced  `, "developer", "spaced", true},
	}
	for _, c := range cases {
		agent, rest, ok := parseRunAgentDispatch(c.in)
		if ok != c.wantOK || agent != c.wantAgent || rest != c.wantRest {
			t.Errorf("parseRunAgentDispatch(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.in, agent, rest, ok, c.wantAgent, c.wantRest, c.wantOK)
		}
	}
}

// recordingBackend is an AgentBackend that records run_agent launches.
type recordingBackend struct {
	launched []string // "agent|task"
	err      error
}

func (b *recordingBackend) EnsureSession(peerID int64) {}
func (b *recordingBackend) ProcessMessage(ctx context.Context, message string, peerID int64) (string, error) {
	return "", nil
}
func (b *recordingBackend) NewSession(ctx context.Context, peerID int64) error { return nil }
func (b *recordingBackend) ResetSession(ctx context.Context, peerID int64, workdir string) error {
	return nil
}
func (b *recordingBackend) Status(ctx context.Context, peerID int64) (string, error) {
	return "", b.err
}
func (b *recordingBackend) Models(ctx context.Context, peerID int64) ([]ModelRef, string, error) {
	return nil, "", b.err
}
func (b *recordingBackend) SetModel(ctx context.Context, peerID int64, ref string) error {
	return b.err
}
func (b *recordingBackend) SteerText(ctx context.Context, message string, peerID int64) error {
	return b.err
}
func (b *recordingBackend) Abort(peerID int64)                                  {}
func (b *recordingBackend) AbortAll()                                           {}
func (b *recordingBackend) IsStreaming(peerID int64) bool                       { return false }
func (b *recordingBackend) WorkingDir(peerID int64) string                      { return "" }
func (b *recordingBackend) SetThinkingCallback(func(int64, string) error)       {}
func (b *recordingBackend) CloseAll()                                           {}
func (b *recordingBackend) SetRunAgentResultCallback(func(int64, string) error) {}

func (b *recordingBackend) RunAgent(ctx context.Context, agent, task string, peerID int64) error {
	if b.err != nil {
		return b.err
	}
	b.launched = append(b.launched, agent+"|"+task)
	return nil
}

func TestProcessMessageRunAgentRouting(t *testing.T) {
	backend := &recordingBackend{}
	handler := NewBotHandler(nil, backend, nil, 0, 0, "")
	handler.SetAgentNames([]string{"lead", "developer", "reviewer", "qa"})

	reply := handler.ProcessMessage("@developer fix the build", 42)
	if len(backend.launched) != 1 || backend.launched[0] != "developer|fix the build" {
		t.Fatalf("expected one launch of developer, got %v", backend.launched)
	}
	if reply == "" {
		t.Fatalf("expected a launch ack, got empty reply")
	}

	// Task-less mention: no launch, hint reply.
	backend.launched = nil
	reply = handler.ProcessMessage("@qa", 42)
	if len(backend.launched) != 0 {
		t.Fatalf("task-less mention must not launch, got %v", backend.launched)
	}
	if reply == "" {
		t.Fatalf("expected a hint reply for task-less mention")
	}

	// Unknown agent: not launched, falls through to the agent turn path.
	backend.launched = nil
	handler.ProcessMessage("@unknown do it", 42)
	if len(backend.launched) != 0 {
		t.Fatalf("unknown agent must not launch, got %v", backend.launched)
	}

	// Non-leading mention: not a dispatch.
	backend.launched = nil
	handler.ProcessMessage("hello @developer fix", 42)
	if len(backend.launched) != 0 {
		t.Fatalf("non-leading mention must not dispatch, got %v", backend.launched)
	}

	// #name is normalized to @name upstream, then dispatched.
	backend.launched = nil
	handler.ProcessMessage("#developer fix the build", 42)
	if len(backend.launched) != 1 || backend.launched[0] != "developer|fix the build" {
		t.Fatalf("expected #developer to dispatch as developer, got %v", backend.launched)
	}

	// Launch error surfaces to the reply.
	backend.err = context.DeadlineExceeded
	backend.launched = nil
	reply = handler.ProcessMessage("@developer boom", 42)
	if reply == "" {
		t.Fatalf("expected an error reply on launch failure")
	}
}
