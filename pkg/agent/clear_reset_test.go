package agent

import (
	"os"
	"path/filepath"
	"testing"
)

// TestPrepareResetRespawnArmsQuarantine checks the /clear//n respawn
// preparation: the old session's resume target, re-issuable prompt and run
// queue are scrubbed (memory + disk); its on-disk storage (jsonl +
// companion dir) is wiped while neighbouring sessions of the same store
// survive; and an isolated --session-dir is armed so oh-my-pi's implicit
// "newest session for this workdir" lookup cannot re-attach the cleared
// session even if the wipe is incomplete.
func TestPrepareResetRespawnArmsQuarantine(t *testing.T) {
	s := newTestSession(t, 11)
	s.bridge.SetStateDir(t.TempDir())

	// Fixture: a per-cwd omp session store holding the pinned (cleared)
	// session with its companion dir, plus a neighbour session that must
	// survive the wipe.
	store := t.TempDir()
	oldFile := filepath.Join(store, "2026-01-01T00-00-00Z_sess-old.jsonl")
	oldDir := filepath.Join(store, "2026-01-01T00-00-00Z_sess-old")
	neighbour := filepath.Join(store, "2025-12-31T00-00-00Z_sess-neighbour.jsonl")
	for _, p := range []string{oldFile, filepath.Join(oldDir, "nested", "deep.log"), neighbour} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	s.mu.Lock()
	s.resumeSessionID = "sess-old"
	s.resumeSessionFile = oldFile
	s.lastPrompt = "вопрос до сброса"
	s.inFlightRuns["vk-11-1"] = &runDescriptor{Agent: "developer", Task: "job"}
	s.bridge.persistStateLocked(s)
	s.prepareResetRespawnLocked()
	qdir := s.freshSessionDir
	s.mu.Unlock()

	// Memory + disk scrubbed.
	st := readPeerState(t, s)
	if st.SessionID != "" || st.SessionFile != "" {
		t.Fatalf("resume target survived reset: id=%q file=%q", st.SessionID, st.SessionFile)
	}
	if st.LastPrompt != "" {
		t.Fatalf("pre-reset prompt survived: %q", st.LastPrompt)
	}
	if _, ok := st.InFlightRuns["vk-11-1"]; ok {
		t.Fatalf("in-flight run survived reset: %+v", st.InFlightRuns)
	}

	// Cleared session's storage gone, neighbour intact.
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Fatalf("cleared session file must be wiped, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(oldDir, "nested", "deep.log")); !os.IsNotExist(err) {
		t.Fatalf("cleared session companion dir must be wiped, stat err=%v", err)
	}
	if _, err := os.Stat(neighbour); err != nil {
		t.Fatalf("neighbour session must survive the wipe: %v", err)
	}

	// Quarantine armed and unique per reset.
	if qdir == "" {
		t.Fatal("no isolated session dir armed for the reset respawn")
	}
	if fi, err := os.Stat(qdir); err != nil || !fi.IsDir() {
		t.Fatalf("quarantine dir unusable: %s (%v)", qdir, err)
	}
	s.mu.Lock()
	s.prepareResetRespawnLocked()
	qdir2 := s.freshSessionDir
	s.mu.Unlock()
	if qdir2 == "" || qdir2 == qdir {
		t.Fatalf("second quarantine must be a fresh dir, got %q (first %q)", qdir2, qdir)
	}
}
