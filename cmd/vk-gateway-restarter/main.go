// Command vk-gateway-restarter supervises the vk-gateway process: it keeps
// the gateway running (crash -> restart with backoff) and reacts to signal
// files written by the gateway's /restart and /update commands:
//
//	.agent-restart  -> stop and start the gateway
//	.agent-update   -> stop, git pull, go build, start
//
// The gateway itself owns the VK long-poll and all per-peer omp agent
// processes, so this restarter has no VK client of its own.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"flag"
)

type config struct {
	Debug      bool     `json:"debug"`
	Workdir    string   `json:"workdir"`
	GatewayCmd []string `json:"gateway_cmd"`
	UpdateCmd  []string `json:"update_cmd"`
}

// agentProc tracks one supervised child process (the gateway).
type agentProc struct {
	mu         sync.Mutex
	cmd        *exec.Cmd
	restarting bool
}

func (ap *agentProc) start(binary string, args []string, workdir string) error {
	ap.mu.Lock()
	defer ap.mu.Unlock()

	ap.killLocked()

	cmd := exec.Command(binary, args...)
	cmd.Dir = workdir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start gateway: %w", err)
	}
	ap.cmd = cmd
	fmt.Printf("[restarter] Gateway started (PID %d): %s %v\n", cmd.Process.Pid, binary, args)

	go func(c *exec.Cmd) {
		waitErr := c.Wait()
		ap.mu.Lock()
		defer ap.mu.Unlock()
		if ap.cmd != c {
			return
		}
		ap.cmd = nil
		if waitErr != nil {
			fmt.Printf("[restarter] Gateway exited (PID %d): %v\n", c.Process.Pid, waitErr)
		} else {
			fmt.Printf("[restarter] Gateway stopped (PID %d)\n", c.Process.Pid)
		}
	}(cmd)
	return nil
}

func (ap *agentProc) stop() {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	ap.killLocked()
}

func (ap *agentProc) killLocked() {
	if ap.cmd == nil || ap.cmd.Process == nil {
		return
	}
	pid := ap.cmd.Process.Pid
	_ = ap.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		ap.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = ap.cmd.Process.Kill()
		<-done
	}
	fmt.Printf("[restarter] Gateway stopped (PID %d)\n", pid)
	ap.cmd = nil
}

func (ap *agentProc) isRunning() bool {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	return ap.cmd != nil && ap.cmd.Process != nil && ap.cmd.ProcessState == nil
}

func (ap *agentProc) setRestarting(v bool) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	ap.restarting = v
}

func loadConfig(path string) (*config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &cfg, nil
}

func main() {
	flag.Parse()

	workdir, _ := os.Getwd()

	cfg := &config{}
	if c, err := loadConfig(filepath.Join(workdir, "config.json")); err == nil {
		cfg = c
	}
	if cfg.Workdir != "" {
		abs, err := filepath.Abs(cfg.Workdir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[restarter] bad workdir %q: %v\n", cfg.Workdir, err)
			os.Exit(1)
		}
		workdir = abs
	}

	binary := "omp-agent"
	args := []string{}
	if len(cfg.GatewayCmd) > 0 {
		binary = cfg.GatewayCmd[0]
		args = append(args, cfg.GatewayCmd[1:]...)
	} else {
		// default: the built gateway binary next to the restarter
		binary = filepath.Join(workdir, "omp-agent")
	}
	if _, err := os.Stat(binary); err != nil {
		fmt.Fprintf(os.Stderr, "[restarter] gateway binary not found: %s\n", binary)
		fmt.Fprintln(os.Stderr, "[restarter] build it with: sh ./build.sh")
		os.Exit(1)
	}

	var gateway agentProc
	debug := cfg.Debug

	monitor := func(f string) string { return filepath.Join(workdir, f) }

	fmt.Printf("[restarter] supervising %s (workdir %s, debug %v)\n", binary, workdir, debug)

	// Crash-loop protection: after an unexpected exit, wait before respawn.
	lastSpawn := time.Time{}

	restart := func(reason string) {
		gateway.setRestarting(true)
		defer gateway.setRestarting(false)
		gateway.stop()
		if err := gateway.start(binary, args, workdir); err != nil {
			fmt.Fprintf(os.Stderr, "[restarter] restart (%s) failed: %v\n", reason, err)
		}
		lastSpawn = time.Now()
	}

	monitorSignalFiles := func() {
		if _, err := os.Stat(monitor(".agent-restart")); err == nil {
			os.Remove(monitor(".agent-restart"))
			fmt.Println("[restarter] .agent-restart consumed")
			restart("signal")
			return
		}
		if _, err := os.Stat(monitor(".agent-update")); err != nil {
			return
		}
		os.Remove(monitor(".agent-update"))
		fmt.Println("[restarter] .agent-update consumed: git pull + build + restart")
		gateway.stop()

		if out, err := exec.Command("git", "pull").CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "[restarter] git pull failed: %v\n%s\n", err, out)
		} else {
			fmt.Printf("[restarter] git pull: %s\n", out)
		}

		buildCmd := cfg.UpdateCmd
		if len(buildCmd) == 0 {
			buildCmd = []string{"sh", "./build.sh"}
		}
		build := exec.Command(buildCmd[0], buildCmd[1:]...)
		build.Dir = workdir
		if out, err := build.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "[restarter] build failed: %v\n%s\n", err, out)
		} else {
			fmt.Printf("[restarter] build: %s\n", out)
		}
		restart("update")
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan struct{})

	go func() {
		<-sigChan
		fmt.Println("[restarter] shutting down...")
		gateway.stop()
		close(done)
	}()

	// Initial start.
	if err := gateway.start(binary, args, workdir); err != nil {
		fmt.Fprintf(os.Stderr, "[restarter] initial start failed: %v\n", err)
	}
	lastSpawn = time.Now()

	for {
		select {
		case <-done:
			fmt.Println("[restarter] shutdown complete")
			return
		case <-ticker.C:
			monitorSignalFiles()
			if gateway.isRunning() {
				continue
			}
			if gateway.getRestarting() {
				// A restart/update cycle is in progress; don't double-spawn.
				continue
			}
			// Crash respawn with backoff.
			if !lastSpawn.IsZero() && time.Since(lastSpawn) < 30*time.Second {
				fmt.Println("[restarter] gateway crashed; backing off 5s before respawn")
				select {
				case <-done:
					return
				case <-time.After(5 * time.Second):
				}
			}
			fmt.Println("[restarter] gateway not running; respawning")
			if err := gateway.start(binary, args, workdir); err != nil {
				fmt.Fprintf(os.Stderr, "[restarter] respawn failed: %v\n", err)
			}
			lastSpawn = time.Now()
		}
	}
}

// getRestarting reports whether a restart/update cycle is in progress.
func (ap *agentProc) getRestarting() bool {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	return ap.restarting
}
