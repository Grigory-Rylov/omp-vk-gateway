package agent

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestKillDuringSpawnTerminatesProcess pins the reset-wedge race: a reset
// issued while the peer's first spawn was still in flight killed nothing
// (p.cmd was nil), so the coprocess survived, supervise never respawned it,
// resetRequested stayed set forever and every later message answered
// «агент-процесс недоступен» until the next /clear.
func TestKillDuringSpawnTerminatesProcess(t *testing.T) {
	dir := t.TempDir()
	// A script rather than /bin/sleep: spawnArgs appends --cwd <dir>, which
	// sleep rejects; the script ignores the trailing args.
	script := filepath.Join(dir, "slow-agent.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	p := newProcess(1, nil)
	go p.kill()
	// Park kill() inside the spawn window: before the fix it returned
	// immediately on a nil cmd, so by the time spawn starts the kill is
	// already lost.
	time.Sleep(50 * time.Millisecond)

	if err := p.spawn(t.TempDir(), "", []string{script}, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.kill)

	select {
	case <-p.exitedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("kill issued during spawn was lost; coprocess still alive")
	}
}
