// Package agent — tests for restart survival: per-peer state persistence
// (session id/file for --resume, in-flight run_agent descriptors) and the
// --resume CLI arg assembled on respawn.
package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSpawnArgsResume checks the respawn CLI: --resume <file> is present when a
// resume target is given, absent when empty, and --cwd stays last.
func TestSpawnArgsResume(t *testing.T) {
	cmd := []string{"/usr/local/bin/omp", "--mode", "rpc"}
	extra := []string{"--model", "p/m", "--thinking", "high"}

	args := spawnArgs(cmd, extra, "/home/u/.omp/agent/sessions/x/2026.jsonl", "/home/u/work")
	want := []string{"--mode", "rpc", "--model", "p/m", "--thinking", "high",
		"--resume", "/home/u/.omp/agent/sessions/x/2026.jsonl", "--cwd", "/home/u/work"}
	if strings.Join(args, " ") != strings.Join(want, " ") {
		t.Fatalf("resume args mismatch:\n got: %v\nwant: %v", args, want)
	}

	args = spawnArgs(cmd, extra, "", "/home/u/work")
	if strings.Contains(strings.Join(args, " "), "--resume") {
		t.Fatalf("empty resume must not add --resume, got %v", args)
	}
	if last := args[len(args)-1]; last != "/home/u/work" {
		t.Fatalf("--cwd must be last, got %v", args)
	}
}

// TestInFlightRunPersistence checks the run_agent descriptor lifecycle: recorded
// and persisted to the state file, then removed (and re-persisted) when the run's
// terminal frame arrives.
func TestInFlightRunPersistence(t *testing.T) {
	s := newTestSession(t, 5)
	s.bridge.SetStateDir(t.TempDir())

	s.mu.Lock()
	s.inFlightRuns["vk-5-1"] = &runDescriptor{Agent: "developer", Task: "make it"}
	s.bridge.persistStateLocked(s)
	s.mu.Unlock()

	st := readPeerState(t, s)
	run, ok := st.InFlightRuns["vk-5-1"]
	if !ok || run.Agent != "developer" || run.Task != "make it" {
		t.Fatalf("in-flight run not persisted correctly: %+v", st.InFlightRuns)
	}

	// A terminal frame matching the run consumes the descriptor from disk and
	// delivers the output to the peer.
	results := &[]string{}
	s.bridge.SetRunAgentResultCallback(func(peerID int64, text string) error {
		*results = append(*results, text)
		return nil
	})
	s.runAgentIDs = map[string]bool{"vk-5-1": true}
	s.bridge.handleEvent(s, nil, subagentFrame(t, "subagent_lifecycle", map[string]interface{}{
		"id": "sub-1", "agent": "developer", "index": 1, "status": "completed",
		"output": "done", "parentToolCallId": "vk-5-1",
	}))

	st = readPeerState(t, s)
	if len(st.InFlightRuns) != 0 {
		t.Fatalf("terminal frame must drop the run descriptor, got %+v", st.InFlightRuns)
	}
	if len(*results) != 1 || (*results)[0] != "done" {
		t.Fatalf("expected one delivery of \"done\", got %q", *results)
	}
}

// TestResumePeersReissues checks that on startup the bridge restores persisted
// peer state and re-issues in-flight runs (via RunAgent), consuming the state
// file. RunAgent is stubbed off: the test only asserts state restoration and
// descriptor consumption, not the coprocess round-trip.
func TestResumePeersReissues(t *testing.T) {
	s := newTestSession(t, 9)
	dir := t.TempDir()
	s.bridge.SetStateDir(dir)

	st := peerState{
		Workdir:      "/home/u/work",
		SessionID:    "sess-1",
		SessionFile:  "/home/u/.omp/agent/sessions/x/2026.jsonl",
		InFlightRuns: map[string]*runDescriptor{"r1": {Agent: "developer", Task: "long job"}},
	}
	data, _ := json.Marshal(st)
	if err := os.WriteFile(filepath.Join(dir, "agent-state-9.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	s.bridge.ResumePeers()

	s.mu.Lock()
	gotWd, gotID, gotFile := s.workdir, s.resumeSessionID, s.resumeSessionFile
	s.mu.Unlock()
	if gotWd != "/home/u/work" || gotID != "sess-1" || gotFile != st.SessionFile {
		t.Fatalf("resume did not restore peer state: wd=%q id=%q file=%q", gotWd, gotID, gotFile)
	}
	// The legacy per-peer file is migrated to the default state file, and
	// that file persists after re-issue so a further restart (double
	// reboot / restarter churn) still finds and re-issues the work.
	if _, err := os.Stat(filepath.Join(dir, "agent-state.json")); err != nil {
		t.Fatalf("default state file should persist after re-issue, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "agent-state-9.json")); !os.IsNotExist(err) {
		t.Fatalf("legacy state file must be migrated away, stat err=%v", err)
	}
}

// readPeerState loads the peer's active session state file from the
// bridge's state dir (package access; single-threaded tests need no
// locking): agent-state.json for the default session,
// agent-state-<alias>.json for a named one.
func readPeerState(t *testing.T, s *peerSession) peerState {
	t.Helper()
	b := s.bridge
	b.mu.Lock()
	dir := b.stateDir
	b.mu.Unlock()
	if dir == "" {
		t.Fatal("state dir not configured")
	}
	s.mu.Lock()
	name := s.stateNameLocked()
	s.mu.Unlock()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("state file %s not found: %v", name, err)
	}
	var st peerState
	if err := json.Unmarshal(data, &st); err != nil {
		t.Fatalf("state file unreadable: %v", err)
	}
	return st
}

// TestResumePeersRerunsInFlightTurn checks that a persisted in-flight normal
// turn (lastPrompt) survives a restart (state restoration) and is cleared on
// settle (deliverTurnLocked re-persists the empty value). The test bridge has
// no coprocess command configured, so the re-run goroutine is skipped and only
// state restoration + clearance are exercised.
func TestResumePeersRerunsInFlightTurn(t *testing.T) {
	s := newTestSession(t, 11)
	dir := t.TempDir()
	s.bridge.SetStateDir(dir)

	// Persist a state file with an in-flight main-bot turn.
	st := peerState{
		Workdir:     "/home/u/work",
		SessionID:   "sess-1",
		SessionFile: "/home/u/.omp/agent/sessions/x/2026.jsonl",
		LastPrompt:  "продолжи правку файла",
	}
	data, _ := json.Marshal(st)
	if err := os.WriteFile(filepath.Join(dir, "agent-state-11.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	s.bridge.ResumePeers()

	// State restored: the in-flight prompt is still on disk (the state file is
	// kept so a further restart re-issues again).
	got := readPeerState(t, s)
	if got.LastPrompt != "продолжи правку файла" {
		t.Fatalf("lastPrompt not restored, got %q", got.LastPrompt)
	}
	if got.SessionFile != st.SessionFile {
		t.Fatalf("sessionFile not restored, got %q", got.SessionFile)
	}

	// Settle the turn: deliverTurnLocked clears lastPrompt and re-persists.
	s.mu.Lock()
	s.turnActive = true
	s.lastPrompt = st.LastPrompt
	s.deliverTurnLocked(turnResult{text: "готово"})
	s.mu.Unlock()

	got = readPeerState(t, s)
	if got.LastPrompt != "" {
		t.Fatalf("lastPrompt must be cleared on settle, got %q", got.LastPrompt)
	}
}
