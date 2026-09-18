package vk

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"omp-vk-gateway/pkg/gpu"
)

// statusTestBackend is a minimal AgentBackend for /status tests.
type statusTestBackend struct {
	status string
	err    error
}

func (b *statusTestBackend) EnsureSession(peerID int64) {}
func (b *statusTestBackend) ProcessMessage(ctx context.Context, message string, peerID int64) (string, error) {
	return "", nil
}
func (b *statusTestBackend) NewSession(ctx context.Context, peerID int64) error { return nil }
func (b *statusTestBackend) ResetSession(ctx context.Context, peerID int64, workdir string) error {
	return nil
}
func (b *statusTestBackend) Status(ctx context.Context, peerID int64) (string, error) {
	return b.status, b.err
}
func (b *statusTestBackend) Models(ctx context.Context, peerID int64) ([]ModelRef, string, error) {
	return nil, "", nil
}
func (b *statusTestBackend) SetModel(ctx context.Context, peerID int64, ref string) error { return nil }
func (b *statusTestBackend) SteerText(ctx context.Context, message string, peerID int64) error {
	return nil
}
func (b *statusTestBackend) Abort(peerID int64)                                  {}
func (b *statusTestBackend) AbortAll()                                           {}
func (b *statusTestBackend) IsStreaming(peerID int64) bool                       { return false }
func (b *statusTestBackend) WorkingDir(peerID int64) string                      { return "" }
func (b *statusTestBackend) SetThinkingCallback(func(int64, string) error)       {}
func (b *statusTestBackend) CloseAll()                                           {}
func (b *statusTestBackend) SetRunAgentResultCallback(func(int64, string) error) {}
func (b *statusTestBackend) RunAgent(ctx context.Context, agent, task string, peerID int64) error {
	return nil
}

func newStatusTestHandler(t *testing.T, backend AgentBackend) *BotHandler {
	t.Helper()
	return NewBotHandler(nil, backend, nil, 0, 0, "")
}

func TestStatusGPUBlockShape(t *testing.T) {
	handler := newStatusTestHandler(t, &statusTestBackend{status: "backend status"})

	block := handler.statusGPU()
	if block == "" {
		t.Skip("no GPU data available on this host")
	}

	if !strings.HasPrefix(block, "🎮 GPU ") {
		t.Errorf("GPU block should start with header, got %q", block)
	}
	if !strings.Contains(block, "Driver: ") || !strings.Contains(block, "CUDA: ") {
		t.Errorf("GPU block missing driver/cuda line: %q", block)
	}
	if !strings.Contains(block, "MiB (") {
		t.Errorf("GPU block missing memory usage line: %q", block)
	}
}

// TestStatusGPUHiddenWithoutNvidiaSMI forces nvidia-smi to be unavailable
// (PATH points at an empty directory) and verifies that /status then appends
// nothing extra, even on a host that actually has the GPU tooling.
func TestStatusGPUHiddenWithoutNvidiaSMI(t *testing.T) {
	t.Setenv("PATH", filepath.Join(t.TempDir(), "empty"))
	if gpu.Available() {
		t.Fatalf("test setup: nvidia-smi still resolvable via empty PATH")
	}

	backend := &statusTestBackend{status: "backend status"}
	handler := newStatusTestHandler(t, backend)

	if block := handler.statusGPU(); block != "" {
		t.Errorf("expected empty GPU block without nvidia-smi, got %q", block)
	}
	if status := handler.handleStatus(12345); status != backend.status {
		t.Errorf("handleStatus = %q, want unchanged backend status %q", status, backend.status)
	}
}

func TestStatusAppendsGPUBlockLast(t *testing.T) {
	backend := &statusTestBackend{
		status: "AI Agent (oh-my-pi) активен\nPeer ID: 12345\nСостояние: готов к работе",
	}
	handler := newStatusTestHandler(t, backend)

	status := handler.handleStatus(12345)
	index := strings.Index(status, "🎮 GPU")
	if index < 0 {
		t.Skip("no GPU data appended on this host")
	}

	if strings.Count(status, "🎮 GPU") != 1 {
		t.Errorf("status should contain exactly one GPU header:\n%s", status)
	}
	if rest := status[index:]; strings.Contains(rest, "Состояние:") || strings.Contains(rest, "Модель:") {
		t.Errorf("GPU block must be the last section of /status:\n%s", status)
	}
}
