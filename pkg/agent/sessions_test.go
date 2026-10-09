// Package agent — tests for named sessions: /new creates an alias state
// file + active pointer without touching the old session; /switch swaps the
// live resume target/workdir/alias and persists; ResumePeers honours the
// active pointer (cold-start contract) and migrates legacy per-peer files;
// /sessions lists with the active session marked; alias validation and
// /del removal semantics (state file + session artifacts, active session
// switches to default first). No network, no coprocess: temp dirs only.
package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeStateFile drops a peerState fixture at path (a session state file).
func writeStateFile(t *testing.T, path string, st peerState) {
	t.Helper()
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// readStateFile loads a session state file fixture, failing on absence.
func readStateFile(t *testing.T, path string) peerState {
	t.Helper()
	st, found := loadStateFile(path)
	if !found {
		t.Fatalf("state file %s missing or unreadable", path)
	}
	return st
}

// assertPointer checks the peer's active session pointer.
func assertPointer(t *testing.T, dir string, peerID int64, wantAlias string) {
	t.Helper()
	got, ok := readActivePointer(activePointerFile(dir, peerID))
	if !ok || got != wantAlias {
		t.Fatalf("active pointer for peer %d = (%q, %v), want (%q, true)", peerID, got, ok, wantAlias)
	}
}

// writePointer drops an active-<peerID>.json fixture.
func writePointer(t *testing.T, dir string, peerID int64, alias string) {
	t.Helper()
	if err := os.WriteFile(activePointerFile(dir, peerID), []byte(`{"alias":"`+alias+`"}`), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestNewSessionNamedCreatesAliasFileAndPointer checks /new <alias>: the
// alias state file and the active pointer are created, the live session is
// cleared for a fresh start, and the session being left keeps its context
// under its own (default) file — its on-disk oh-my-pi session must NOT be
// deleted.
func TestNewSessionNamedCreatesAliasFileAndPointer(t *testing.T) {
	s := newTestSession(t, 21)
	dir := t.TempDir()
	s.bridge.SetStateDir(dir)

	// The default session holds a live oh-my-pi session on disk.
	store := t.TempDir()
	oldFile := filepath.Join(store, "2026-01-01T00-00-00Z_sess-old.jsonl")
	oldDir := filepath.Join(store, "2026-01-01T00-00-00Z_sess-old")
	for _, p := range []string{oldFile, filepath.Join(oldDir, "nested", "deep.log")} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	s.workdir = "/home/u/default-wd"
	s.resumeSessionID = "sess-old"
	s.resumeSessionFile = oldFile
	s.lastPrompt = "не доделано"
	s.inFlightRuns["vk-21-1"] = &runDescriptor{Agent: "developer", Task: "job"}
	s.bridge.persistStateLocked(s)
	s.mu.Unlock()

	if err := s.bridge.NewSessionNamed(context.Background(), 21, "proj", "/home/u/proj"); err != nil {
		t.Fatal(err)
	}

	// Alias state file created and clean (fresh session, new workdir).
	aliasSt := readStateFile(t, filepath.Join(dir, "agent-state-proj.json"))
	if aliasSt.Alias != "proj" || aliasSt.PeerID != 21 || aliasSt.Workdir != "/home/u/proj" {
		t.Fatalf("alias file identity/workdir wrong: %+v", aliasSt)
	}
	if aliasSt.SessionFile != "" || aliasSt.SessionID != "" || aliasSt.LastPrompt != "" || len(aliasSt.InFlightRuns) != 0 {
		t.Fatalf("new session must start clean, got %+v", aliasSt)
	}

	// The session being left keeps its context under the default file.
	defSt := readStateFile(t, filepath.Join(dir, "agent-state.json"))
	if defSt.SessionFile != oldFile || defSt.LastPrompt != "не доделано" {
		t.Fatalf("default session context lost on /new: %+v", defSt)
	}
	if _, err := os.Stat(oldFile); err != nil {
		t.Fatalf("old session file must NOT be deleted by /new: %v", err)
	}
	if _, err := os.Stat(filepath.Join(oldDir, "nested", "deep.log")); err != nil {
		t.Fatalf("old session companion must NOT be deleted by /new: %v", err)
	}

	assertPointer(t, dir, 21, "proj")

	// Live session: new alias/workdir, resume state cleared, fresh-spawn
	// reset armed for the ensuing respawn.
	s.mu.Lock()
	gotAlias, gotWd, gotFile, gotReset := s.alias, s.workdir, s.resumeSessionFile, s.resetRequested
	s.mu.Unlock()
	if gotAlias != "proj" || gotWd != "/home/u/proj" || gotFile != "" || !gotReset {
		t.Fatalf("live session state wrong after /new: alias=%q wd=%q file=%q reset=%v",
			gotAlias, gotWd, gotFile, gotReset)
	}

	// Taken alias → error.
	if err := s.bridge.NewSessionNamed(context.Background(), 21, "proj", ""); err == nil ||
		!strings.Contains(err.Error(), "уже занят") {
		t.Fatalf("taken alias must be rejected, got %v", err)
	}
}

// TestSwitchSessionSwapsAndPersists checks /switch: the peer adopts the
// target session's alias/workdir/resume target/in-flight prompt, the
// session being left is persisted under its own alias, the pointer follows,
// an unknown alias errors, and switching back to default re-points at
// agent-state.json without clobbering the named session.
func TestSwitchSessionSwapsAndPersists(t *testing.T) {
	s := newTestSession(t, 22)
	dir := t.TempDir()
	s.bridge.SetStateDir(dir)
	ctx := context.Background()

	defFile := filepath.Join(dir, "agent-state.json")
	projFile := filepath.Join(dir, "agent-state-proj.json")
	writeStateFile(t, defFile, peerState{Workdir: "/w/default", SessionID: "sess-def", SessionFile: "/store/def.jsonl"})
	writeStateFile(t, projFile, peerState{Alias: "proj", Workdir: "/w/proj", SessionID: "sess-proj",
		SessionFile: "/store/proj.jsonl", LastPrompt: "доделать"})

	// Peer starts on the default session with live in-flight state.
	s.mu.Lock()
	s.workdir = "/w/default"
	s.resumeSessionID = "sess-def"
	s.resumeSessionFile = "/store/def.jsonl"
	s.lastPrompt = "дефолтный промпт"
	s.bridge.persistStateLocked(s)
	s.mu.Unlock()

	if err := s.bridge.SwitchSession(ctx, 22, "proj"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	gotAlias, gotWd := s.alias, s.workdir
	gotID, gotFile, gotPrompt := s.resumeSessionID, s.resumeSessionFile, s.lastPrompt
	gotSwitch := s.switchRequested
	s.mu.Unlock()
	if gotAlias != "proj" || gotWd != "/w/proj" || gotID != "sess-proj" || gotFile != "/store/proj.jsonl" || gotPrompt != "доделать" {
		t.Fatalf("switch did not adopt the target session: alias=%q wd=%q id=%q file=%q prompt=%q",
			gotAlias, gotWd, gotID, gotFile, gotPrompt)
	}
	if gotSwitch {
		t.Fatal("no live process: switch must not arm a respawn flag")
	}
	assertPointer(t, dir, 22, "proj")

	// Leaving persisted the default session's live state under its own file.
	defSt := readStateFile(t, defFile)
	if defSt.SessionFile != "/store/def.jsonl" || defSt.LastPrompt != "дефолтный промпт" {
		t.Fatalf("default session state lost on switch: %+v", defSt)
	}

	// Unknown alias → error, pointer untouched.
	if err := s.bridge.SwitchSession(ctx, 22, "nope"); err == nil || !strings.Contains(err.Error(), "не найдена") {
		t.Fatalf("unknown alias error = %v, want \"не найдена\"", err)
	}
	assertPointer(t, dir, 22, "proj")

	// Switch back to default: pointer re-set to the default alias, live
	// state from agent-state.json, the named session file intact.
	if err := s.bridge.SwitchSession(ctx, 22, "default"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	gotAlias, gotFile = s.alias, s.resumeSessionFile
	s.mu.Unlock()
	if gotAlias != "" || gotFile != "/store/def.jsonl" {
		t.Fatalf("switch to default did not restore the default session: alias=%q file=%q", gotAlias, gotFile)
	}
	assertPointer(t, dir, 22, "")
	if st := readStateFile(t, projFile); st.SessionFile != "/store/proj.jsonl" {
		t.Fatalf("proj state clobbered by switching back: %+v", st)
	}
}

// TestListSessionsMarksActiveAndSorts checks /sessions rendering: the
// peer's active session is marked with ▶, entries are sorted by last use
// (newest first), the default session shows as "default", and legacy
// numeric leftovers are not listed.
func TestListSessionsMarksActiveAndSorts(t *testing.T) {
	b := &Bridge{peers: map[int64]*peerSession{}}
	dir := t.TempDir()
	b.SetStateDir(dir)

	writeStateFile(t, filepath.Join(dir, "agent-state.json"), peerState{Workdir: "/w/def", LastUsed: "2026-10-01T10:00:00Z"})
	writeStateFile(t, filepath.Join(dir, "agent-state-old.json"), peerState{Alias: "old", Workdir: "/w/old", LastUsed: "2026-10-02T10:00:00Z"})
	writeStateFile(t, filepath.Join(dir, "agent-state-new.json"), peerState{Alias: "new", Workdir: "/w/new", LastUsed: "2026-10-03T10:00:00Z"})
	writeStateFile(t, filepath.Join(dir, "agent-state-23.json"), peerState{Workdir: "/w/legacy"})
	writePointer(t, dir, 23, "old")

	list, err := b.ListSessions(23)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(list, "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 sessions (legacy leftover excluded), got %d: %q", len(lines), list)
	}
	if !strings.HasPrefix(lines[0], "  new — /w/new") {
		t.Fatalf("newest first, unmarked: %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "▶ old — /w/old") {
		t.Fatalf("active session must be marked ▶: %q", lines[1])
	}
	if !strings.HasPrefix(lines[2], "  default — /w/def") {
		t.Fatalf("default last, unmarked: %q", lines[2])
	}
}

// TestListSessionsEmpty checks the empty-state hint.
func TestListSessionsEmpty(t *testing.T) {
	b := &Bridge{peers: map[int64]*peerSession{}}
	b.SetStateDir(t.TempDir())
	list, err := b.ListSessions(77)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(list, "Сессий нет") {
		t.Fatalf("empty listing = %q, want the \"Сессий нет\" hint", list)
	}
}

// TestAliasValidation checks the alias gate on every named-session entry
// point, and that a valid alias saves the current session under the alias
// file + pointer.
func TestAliasValidation(t *testing.T) {
	b := &Bridge{peers: map[int64]*peerSession{}}
	dir := t.TempDir()
	b.SetStateDir(dir)

	for _, bad := range []string{"", " ", "a/b", "x y", "a\\b", "default", strings.Repeat("x", 65), "сессия"} {
		if err := b.SaveSession(24, bad); err == nil {
			t.Errorf("SaveSession(%q) must be rejected", bad)
		}
		if err := b.NewSessionNamed(context.Background(), 24, bad, ""); err == nil {
			t.Errorf("NewSessionNamed(%q) must be rejected", bad)
		}
		if err := b.DeleteSession(context.Background(), 24, bad); err == nil {
			t.Errorf("DeleteSession(%q) must be rejected", bad)
		}
	}

	if err := b.SaveSession(24, "proj-1.2_3"); err != nil {
		t.Fatal(err)
	}
	st := readStateFile(t, filepath.Join(dir, "agent-state-proj-1.2_3.json"))
	if st.Alias != "proj-1.2_3" || st.PeerID != 24 {
		t.Fatalf("saved session identity wrong: %+v", st)
	}
	assertPointer(t, dir, 24, "proj-1.2_3")
}

// TestResumePeersUsesActivePointer checks the cold-start contract: with an
// active pointer to alias "proj", the peer session is seeded from
// agent-state-proj.json (sessionFile, workdir, alias, in-flight state) —
// NOT from the stale default file sitting next to it.
func TestResumePeersUsesActivePointer(t *testing.T) {
	b := &Bridge{peers: map[int64]*peerSession{}}
	dir := t.TempDir()
	b.SetStateDir(dir)

	writeStateFile(t, filepath.Join(dir, "agent-state-proj.json"), peerState{
		Alias: "proj", Workdir: "/w/proj", SessionID: "sess-p", SessionFile: "/store/p.jsonl",
		InFlightRuns: map[string]*runDescriptor{"r1": {Agent: "developer", Task: "job"}},
		LastPrompt:   "доделать",
	})
	writeStateFile(t, filepath.Join(dir, "agent-state.json"), peerState{PeerID: 7, Workdir: "/w/def", SessionFile: "/store/def.jsonl"})
	writePointer(t, dir, 7, "proj")

	b.ResumePeers()

	s := b.session(7)
	s.mu.Lock()
	gotAlias, gotWd := s.alias, s.workdir
	gotID, gotFile, gotPrompt := s.resumeSessionID, s.resumeSessionFile, s.lastPrompt
	_, hasRun := s.inFlightRuns["r1"]
	s.mu.Unlock()
	if gotAlias != "proj" || gotWd != "/w/proj" || gotID != "sess-p" || gotFile != "/store/p.jsonl" || gotPrompt != "доделать" {
		t.Fatalf("pointer resume wrong: alias=%q wd=%q id=%q file=%q prompt=%q",
			gotAlias, gotWd, gotID, gotFile, gotPrompt)
	}
	if !hasRun {
		t.Fatal("in-flight run descriptor not seeded from the alias state")
	}
}

// TestResumePeersMigratesLegacyState checks the pre-named-sessions
// fallback: a peer without a pointer resumes from agent-state-<peerID>.json,
// which is renamed to the shared default file (content intact, identity
// recorded).
func TestResumePeersMigratesLegacyState(t *testing.T) {
	b := &Bridge{peers: map[int64]*peerSession{}}
	dir := t.TempDir()
	b.SetStateDir(dir)

	writeStateFile(t, filepath.Join(dir, "agent-state-31.json"),
		peerState{Workdir: "/w/legacy", SessionID: "sess-l", SessionFile: "/store/l.jsonl"})

	b.ResumePeers()

	if _, err := os.Stat(filepath.Join(dir, "agent-state-31.json")); !os.IsNotExist(err) {
		t.Fatalf("legacy state file must be renamed away, stat err=%v", err)
	}
	st := readStateFile(t, filepath.Join(dir, "agent-state.json"))
	if st.SessionFile != "/store/l.jsonl" || st.SessionID != "sess-l" || st.PeerID != 31 {
		t.Fatalf("migrated state content wrong: %+v", st)
	}
	s := b.session(31)
	s.mu.Lock()
	gotWd, gotFile := s.workdir, s.resumeSessionFile
	s.mu.Unlock()
	if gotWd != "/w/legacy" || gotFile != "/store/l.jsonl" {
		t.Fatalf("peer not seeded from migrated state: wd=%q file=%q", gotWd, gotFile)
	}
}

// TestDeleteSessionRemovesStateAndArtifacts checks /del of an inactive
// session: the state file, the session jsonl and its companion dir are
// removed while a neighbouring session in the same store survives.
func TestDeleteSessionRemovesStateAndArtifacts(t *testing.T) {
	s := newTestSession(t, 41)
	dir := t.TempDir()
	s.bridge.SetStateDir(dir)

	store := t.TempDir()
	sessFile := filepath.Join(store, "2026-01-01T00-00-00Z_sess-x.jsonl")
	sessDir := filepath.Join(store, "2026-01-01T00-00-00Z_sess-x")
	neighbour := filepath.Join(store, "2025-12-31T00-00-00Z_sess-n.jsonl")
	for _, p := range []string{sessFile, filepath.Join(sessDir, "nested", "deep.log"), neighbour} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeStateFile(t, filepath.Join(dir, "agent-state-x.json"),
		peerState{Alias: "x", Workdir: "/w/x", SessionFile: sessFile})

	if err := s.bridge.DeleteSession(context.Background(), 41, "x"); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dir, "agent-state-x.json")); !os.IsNotExist(err) {
		t.Fatalf("session state file must be gone, stat err=%v", err)
	}
	if _, err := os.Stat(sessFile); !os.IsNotExist(err) {
		t.Fatalf("session jsonl must be gone, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(sessDir, "nested", "deep.log")); !os.IsNotExist(err) {
		t.Fatalf("session companion dir must be gone, stat err=%v", err)
	}
	if _, err := os.Stat(neighbour); err != nil {
		t.Fatalf("neighbour session must survive the delete: %v", err)
	}
}

// TestDeleteSessionActiveSwitchesToDefault checks that deleting the peer's
// ACTIVE session first moves the peer to the default session (pointer
// cleared to the default alias, live state swapped) and only then removes
// the named session — the coprocess never resumes into a deleted file.
func TestDeleteSessionActiveSwitchesToDefault(t *testing.T) {
	s := newTestSession(t, 42)
	dir := t.TempDir()
	s.bridge.SetStateDir(dir)

	writeStateFile(t, filepath.Join(dir, "agent-state.json"),
		peerState{PeerID: 42, Workdir: "/w/def", SessionFile: "/store/def.jsonl"})
	writeStateFile(t, filepath.Join(dir, "agent-state-proj.json"),
		peerState{Alias: "proj", PeerID: 42, Workdir: "/w/proj", SessionFile: "/store/proj.jsonl"})
	writePointer(t, dir, 42, "proj")
	s.mu.Lock()
	s.alias = "proj"
	s.workdir = "/w/proj"
	s.resumeSessionFile = "/store/proj.jsonl"
	s.mu.Unlock()

	if err := s.bridge.DeleteSession(context.Background(), 42, "proj"); err != nil {
		t.Fatal(err)
	}

	assertPointer(t, dir, 42, "")
	s.mu.Lock()
	gotAlias, gotFile := s.alias, s.resumeSessionFile
	s.mu.Unlock()
	if gotAlias != "" || gotFile != "/store/def.jsonl" {
		t.Fatalf("peer not moved to the default session before delete: alias=%q file=%q", gotAlias, gotFile)
	}
	if _, err := os.Stat(filepath.Join(dir, "agent-state-proj.json")); !os.IsNotExist(err) {
		t.Fatalf("deleted session state must be gone, stat err=%v", err)
	}
	// The switch target (default file) survives.
	readStateFile(t, filepath.Join(dir, "agent-state.json"))
}

// TestDeleteSessionUnknownAndReserved checks /del error paths: an unknown
// alias reports "не найдена", the reserved "default" is rejected.
func TestDeleteSessionUnknownAndReserved(t *testing.T) {
	s := newTestSession(t, 43)
	s.bridge.SetStateDir(t.TempDir())

	if err := s.bridge.DeleteSession(context.Background(), 43, "ghost"); err == nil ||
		!strings.Contains(err.Error(), "не найдена") {
		t.Fatalf("unknown alias error = %v, want \"не найдена\"", err)
	}
	if err := s.bridge.DeleteSession(context.Background(), 43, "default"); err == nil {
		t.Fatal("deleting the reserved default session must be rejected")
	}
}
