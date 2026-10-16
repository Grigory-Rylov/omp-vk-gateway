package agent

import (
	"syscall"
	"time"
)

// markReady completes the ready handshake (readyCh is closed exactly once).
func (p *process) markReady(err error) {
	p.readyOnce.Do(func() {
		p.readyErr = err
		if err == nil {
			p.readyFlag.Store(true)
		}
		close(p.readyCh)
	})
}

// failReady marks startup as failed.
func (p *process) failReady(err error) {
	p.markReady(err)
}

// kill terminates the process: SIGTERM, 5s grace, SIGKILL.
func (p *process) kill() {
	// A kill issued while the first spawn is still in flight would find
	// p.cmd nil and silently no-op — losing the reset forever, since
	// supervise only respawns on process exit. Wait for the spawn attempt
	// to settle: spawnedCh closes when spawn returns, exitedCh when the
	// failed process was marked done by the spawner.
	select {
	case <-p.spawnedCh:
	case <-p.exitedCh:
	}
	p.mu.Lock()
	cmd := p.cmd
	already := p.doneFlag
	p.mu.Unlock()
	if cmd == nil || cmd.Process == nil || already {
		return
	}
	if p.log != nil {
		p.log.DebugLogf("[rpc/peer%d] killing pid %d", p.peerID, cmd.Process.Pid)
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.exitedCh:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		select {
		case <-p.exitedCh:
		case <-time.After(2 * time.Second):
		}
	}
}
