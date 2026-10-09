// Tests for the /clear and /n reset semantics: /clear resets the session
// while keeping the peer's current workdir, /n without a path returns the
// peer to the gateway's own workdir, /n with a path switches to it.
package vk

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// resetProbeBackend is an AgentBackend double recording the workdir each
// ResetSession call received and answering WorkingDir with a canned value.
type resetProbeBackend struct {
	namedSessionsStub
	workdir string
	resets  []string
	err     error
}

func (b *resetProbeBackend) EnsureSession(peerID int64) {}
func (b *resetProbeBackend) ProcessMessage(ctx context.Context, message string, peerID int64) (string, error) {
	return "", nil
}
func (b *resetProbeBackend) ResetSession(ctx context.Context, peerID int64, workdir string) error {
	if b.err != nil {
		return b.err
	}
	b.resets = append(b.resets, workdir)
	return nil
}
func (b *resetProbeBackend) Status(ctx context.Context, peerID int64) (string, error) {
	return "", nil
}
func (b *resetProbeBackend) Models(ctx context.Context, peerID int64) ([]ModelRef, string, error) {
	return nil, "", nil
}
func (b *resetProbeBackend) SetModel(ctx context.Context, peerID int64, ref string) error {
	return nil
}
func (b *resetProbeBackend) SteerText(ctx context.Context, message string, peerID int64) error {
	return nil
}
func (b *resetProbeBackend) Abort(peerID int64)                                  {}
func (b *resetProbeBackend) AbortAll()                                           {}
func (b *resetProbeBackend) IsStreaming(peerID int64) bool                       { return false }
func (b *resetProbeBackend) WorkingDir(peerID int64) string                      { return b.workdir }
func (b *resetProbeBackend) SetThinkingCallback(func(int64, string) error)       {}
func (b *resetProbeBackend) CloseAll()                                           {}
func (b *resetProbeBackend) SetRunAgentResultCallback(func(int64, string) error) {}
func (b *resetProbeBackend) RunAgent(ctx context.Context, agent, task string, peerID int64) error {
	return nil
}

// newProbeHandler wires a probe backend into a real handler; defaultWD plays
// the role of the gateway's configured workdir.
func newProbeHandler(t *testing.T, be *resetProbeBackend, defaultWD string) *BotHandler {
	t.Helper()
	return NewBotHandler(nil, be, nil, 1, 2, defaultWD)
}

func assertSingleReset(t *testing.T, be *resetProbeBackend, want string) {
	t.Helper()
	if len(be.resets) != 1 {
		t.Fatalf("expected exactly one ResetSession call, got %d", len(be.resets))
	}
	if be.resets[0] != want {
		t.Fatalf("ResetSession workdir = %q, want %q", be.resets[0], want)
	}
}

// /clear resets the session through the same reset/respawn path /n uses,
// keeping the peer's current workdir: ResetSession is called with an empty
// workdir and the reply quotes the current directory.
func TestClearKeepsCurrentWorkdir(t *testing.T) {
	be := &resetProbeBackend{workdir: "/home/u/project-x"}
	h := newProbeHandler(t, be, "/opt/gateway")

	out := h.handleCommand("/clear", 1)
	assertSingleReset(t, be, "")
	if !strings.Contains(out, "/home/u/project-x") {
		t.Fatalf("reply must quote the kept workdir, got %q", out)
	}
	if strings.Contains(out, "❌") {
		t.Fatalf("unexpected error reply: %q", out)
	}
}

// A peer with no workdir of its own falls back to the gateway's default
// workdir for both the reset target display and the reply.
func TestClearFallsBackToDefaultWorkdir(t *testing.T) {
	be := &resetProbeBackend{workdir: ""}
	h := newProbeHandler(t, be, "/opt/gateway")

	out := h.handleCommand("/clear", 1)
	assertSingleReset(t, be, "")
	if !strings.Contains(out, "/opt/gateway") {
		t.Fatalf("reply must quote the default workdir, got %q", out)
	}
}

// No-argument /n returns the peer to the gateway's own workdir — the
// configured default — not to the peer's current one.
func TestNewSessionNoArgReturnsToGatewayWorkdir(t *testing.T) {
	tmp, err := os.MkdirTemp("", "gw-default-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmp) })
	abs, err := filepath.Abs(tmp)
	if err != nil {
		t.Fatal(err)
	}
	be := &resetProbeBackend{workdir: "/home/u/project-x"}
	h := newProbeHandler(t, be, abs)

	out := h.handleCommand("/n", 1)
	assertSingleReset(t, be, abs)
	if strings.Contains(out, "/home/u/project-x") {
		t.Fatalf("reply must not keep the peer workdir, got %q", out)
	}
}

// Without a configured default, both /n and /clear fall back to the
// gateway's CWD.
func TestResetFallbackToGatewayCwd(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	be := &resetProbeBackend{workdir: ""}
	h := newProbeHandler(t, be, "")
	h.handleCommand("/n", 1)
	assertSingleReset(t, be, cwd)

	be2 := &resetProbeBackend{workdir: ""}
	h2 := newProbeHandler(t, be2, "")
	out := h2.handleCommand("/clear", 1)
	assertSingleReset(t, be2, "")
	if !strings.Contains(out, cwd) {
		t.Fatalf("clear reply must quote the gateway CWD, got %q", out)
	}
}

// An explicit path wins over every fallback and is passed through
// absolutised.
func TestNewSessionWithPathSwitchesWorkdir(t *testing.T) {
	tmp := t.TempDir()
	abs, err := filepath.Abs(tmp)
	if err != nil {
		t.Fatal(err)
	}
	be := &resetProbeBackend{workdir: "/home/u/project-x"}
	h := newProbeHandler(t, be, "/opt/gateway")

	out := h.handleCommand("/n "+tmp, 1)
	assertSingleReset(t, be, abs)
	if !strings.Contains(out, abs) {
		t.Fatalf("reply must quote the new workdir, got %q", out)
	}
}
