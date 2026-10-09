package agent

// Named sessions: a session is a first-class entity keyed by a user-chosen
// alias and persisted to agent-state-<alias>.json (the unnamed default
// session lives in agent-state.json). A VK peer only holds a pointer to its
// active session (active-<peerID>.json, {"alias":"proj"}; absent or empty
// alias = the default session), so /new, /switch, /save, /del and /sessions
// move the pointer and respawn the coprocess on the target session.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// defaultStateFileName is the state file of the unnamed (default)
	// session; named sessions use stateFilePrefix+alias+stateFileSuffix.
	defaultStateFileName = "agent-state.json"
	stateFilePrefix      = "agent-state-"
	stateFileSuffix      = ".json"
	// activeFilePrefix names the per-peer pointer file:
	// active-<peerID>.json.
	activeFilePrefix = "active-"
	// defaultSessionName is the reserved display/switch name of the default
	// session (it cannot be taken as a user alias).
	defaultSessionName = "default"
)

// aliasRe accepts user-chosen session names: letters, digits, dot, dash,
// underscore, 1..64 chars. Path separators and spaces are impossible by
// construction, so an alias can never escape the state dir.
var aliasRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// validateAlias rejects empty/whitespace names, names with characters
// outside [A-Za-z0-9._-], over-long names, and the reserved "default".
func validateAlias(alias string) error {
	if !aliasRe.MatchString(alias) {
		return fmt.Errorf("недопустимое имя сессии %q: разрешены буквы, цифры, . _ - (до 64 символов, без пробелов и путей)", alias)
	}
	if alias == defaultSessionName {
		return fmt.Errorf("имя %s зарезервировано для обычной (безымянной) сессии", defaultSessionName)
	}
	return nil
}

// stateFileName maps a session alias ("" = default) to its state file name.
func stateFileName(alias string) string {
	if alias == "" {
		return defaultStateFileName
	}
	return stateFilePrefix + alias + stateFileSuffix
}

// sessionLabel renders an alias for user-facing text.
func sessionLabel(alias string) string {
	if alias == "" {
		return defaultSessionName
	}
	return alias
}

func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// activePointer is the content of active-<peerID>.json: the peer's active
// session alias ("" = the default session).
type activePointer struct {
	Alias string `json:"alias"`
}

func activePointerFile(dir string, peerID int64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%d%s", activeFilePrefix, peerID, stateFileSuffix))
}

// readActivePointer loads a peer's active alias; false when no (usable)
// pointer exists, which means the default session.
func readActivePointer(path string) (string, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	var p activePointer
	if json.Unmarshal(data, &p) != nil {
		return "", false
	}
	return p.Alias, true
}

// setActivePointer atomically (tmp+rename) records the peer's active session
// alias ("" = default). The pointer is the cold-start contract: ResumePeers
// respawns each peer on the session it names, so the write must never leave
// a truncated file behind.
func (b *Bridge) setActivePointer(dir string, peerID int64, alias string) error {
	f := activePointerFile(dir, peerID)
	data, err := json.Marshal(activePointer{Alias: alias})
	if err != nil {
		return err
	}
	if err := os.WriteFile(f+".tmp", data, 0o644); err != nil {
		return err
	}
	return os.Rename(f+".tmp", f)
}

// loadStateFile reads one session state file; false when missing or
// unreadable.
func loadStateFile(path string) (peerState, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return peerState{}, false
	}
	var st peerState
	if json.Unmarshal(data, &st) != nil {
		return peerState{}, false
	}
	return st, true
}

// migrateLegacyState renames a pre-named-sessions per-peer state file
// (agent-state-<peerID>.json) to the default file (agent-state.json),
// content intact. Returns true when the default file exists afterwards
// (renamed now, or already present).
func (b *Bridge) migrateLegacyState(peerID int64) bool {
	b.mu.Lock()
	dir := b.stateDir
	b.mu.Unlock()
	if dir == "" {
		return false
	}
	def := filepath.Join(dir, defaultStateFileName)
	if _, err := os.Stat(def); err == nil {
		return true
	}
	legacy := filepath.Join(dir, stateFileName(strconv.FormatInt(peerID, 10)))
	if _, err := os.Stat(legacy); err != nil {
		return false
	}
	if err := os.Rename(legacy, def); err != nil {
		if b.log != nil {
			b.log.WarnLogf("peer %d: legacy state migration to %s failed: %v", peerID, defaultStateFileName, err)
		}
		return false
	}
	b.debugf("peer %d: migrated legacy state to %s", peerID, defaultStateFileName)
	return true
}

// stateDirOrErr resolves the configured state dir or a user-facing error.
func (b *Bridge) stateDirOrErr() (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stateDir == "" {
		return "", errors.New("каталог состояния не настроен")
	}
	return b.stateDir, nil
}

// SaveSession registers (or re-registers) the peer's current session under
// alias: the live coprocess session (id + file) is captured first, the
// state is persisted to agent-state-<alias>.json and the alias becomes the
// peer's active session. The previous state file is left in place; the
// session simply continues under its new name.
func (b *Bridge) SaveSession(peerID int64, alias string) error {
	if err := validateAlias(alias); err != nil {
		return err
	}
	dir, err := b.stateDirOrErr()
	if err != nil {
		return err
	}
	s := b.session(peerID)
	// Capture the live oh-my-pi session so the alias points at the real
	// conversation, not a stale snapshot. No process (or one that never
	// becomes ready) is fine: the last persisted resume target is saved.
	if proc, err := s.waitReady(context.Background()); err == nil {
		b.refreshSessionState(s, proc)
	}
	s.mu.Lock()
	s.alias = alias
	b.persistStateLocked(s)
	s.mu.Unlock()
	if err := b.setActivePointer(dir, peerID, alias); err != nil {
		return err
	}
	s.debugf("session saved as %q", alias)
	return nil
}

// SwitchSession switches the peer to the named session: the session being
// left is persisted under its own alias first (leaving must not lose
// context), then the peer adopts the target's workdir, resume target and
// in-flight state, and the coprocess is killed and respawned with --resume
// on the target session. Unlike a reset, nothing is scrubbed or wiped: a
// later switch back finds the old session intact. The alias "default" (or
// an empty one) selects the default session and re-points the peer at
// agent-state.json.
func (b *Bridge) SwitchSession(ctx context.Context, peerID int64, alias string) error {
	if alias == "" || alias == defaultSessionName {
		alias = "" // the default session
	} else if err := validateAlias(alias); err != nil {
		return err
	}
	dir, err := b.stateDirOrErr()
	if err != nil {
		return err
	}
	st, found := loadStateFile(filepath.Join(dir, stateFileName(alias)))
	if !found && alias == "" {
		if b.migrateLegacyState(peerID) {
			st, found = loadStateFile(filepath.Join(dir, defaultStateFileName))
		}
	}
	if !found {
		return fmt.Errorf("сессия %s не найдена", sessionLabel(alias))
	}
	s := b.session(peerID)
	s.mu.Lock()
	// Persist the session we are leaving under its current alias so its
	// context survives a later switch back.
	b.persistStateLocked(s)
	s.alias = alias
	if st.Workdir != "" {
		s.workdir = st.Workdir
	}
	s.resumeSessionID = st.SessionID
	s.resumeSessionFile = st.SessionFile
	s.inFlightRuns = map[string]*runDescriptor{}
	for id, r := range st.InFlightRuns {
		s.inFlightRuns[id] = r
	}
	s.lastPrompt = st.LastPrompt
	s.resetRequested = false
	s.turnActive = false
	s.turnAborted = true
	proc := s.proc
	if proc != nil && !proc.done() {
		// A live process must die for the respawn to come up on the new
		// resume target; a dead/absent one already picks the new state up
		// on the next spawn, so no switch flag is needed.
		s.switchRequested = true
		go proc.kill()
	}
	s.notifyResetLocked()
	b.persistStateLocked(s)
	wd := s.workdir
	s.mu.Unlock()
	if err := b.setActivePointer(dir, peerID, alias); err != nil {
		return err
	}
	s.debugf("switched to session %q (workdir %q)", alias, wd)
	return nil
}

// NewSessionNamed creates a fresh session under alias and makes it active:
// the current session is persisted under its own alias first (its context
// survives a switch back), the peer's resume target / in-flight prompt /
// run queue are cleared WITHOUT touching the old session's on-disk files
// (they belong to the old alias), and the coprocess is killed and respawned
// clean (isolated --session-dir) in workdir (empty keeps the current one).
// The alias must not exist yet.
func (b *Bridge) NewSessionNamed(ctx context.Context, peerID int64, alias, workdir string) error {
	if err := validateAlias(alias); err != nil {
		return err
	}
	dir, err := b.stateDirOrErr()
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(dir, stateFileName(alias))); err == nil {
		return fmt.Errorf("алиас %s уже занят", alias)
	}
	s := b.session(peerID)
	s.mu.Lock()
	// Save the session being left under its current alias, then drop its
	// resume state from memory only. The ensuing prepareResetRespawnLocked
	// sees an empty stale target, so it wipes nothing and only arms the
	// isolated --session-dir for the fresh spawn.
	b.persistStateLocked(s)
	s.resumeSessionID = ""
	s.resumeSessionFile = ""
	s.lastPrompt = ""
	for id := range s.inFlightRuns {
		delete(s.inFlightRuns, id)
	}
	s.alias = alias
	if workdir != "" {
		s.workdir = workdir
	}
	b.persistStateLocked(s) // creates agent-state-<alias>.json
	s.resetRequested = true
	s.switchRequested = false
	s.turnActive = false
	s.turnAborted = true
	proc := s.proc
	if proc != nil && !proc.done() {
		go proc.kill()
	}
	s.notifyResetLocked()
	wd := s.workdir
	s.mu.Unlock()
	if err := b.setActivePointer(dir, peerID, alias); err != nil {
		return err
	}
	s.debugf("new session %q created (workdir %q)", alias, wd)
	return nil
}

// DeleteSession removes a named session: its state file AND the underlying
// oh-my-pi session artifacts (the session jsonl + its companion dir).
// Artifacts are removed FIRST: a switch respawn without a resume target
// resolves to the newest session of the workdir, so the files must be gone
// before the peer moves off the deleted alias — otherwise the coprocess
// would re-attach the very session being deleted. Deleting the peer's
// ACTIVE session then switches it to the default, so the coprocess never
// resumes into a deleted file. Missing artifacts are not an error; a real
// removal failure aborts before anything else moves.
func (b *Bridge) DeleteSession(ctx context.Context, peerID int64, alias string) error {
	if err := validateAlias(alias); err != nil {
		return err
	}
	dir, err := b.stateDirOrErr()
	if err != nil {
		return err
	}
	path := filepath.Join(dir, stateFileName(alias))
	st, found := loadStateFile(path)
	if !found {
		return fmt.Errorf("сессия %s не найдена", alias)
	}
	// Artifacts first (see doc): the switch respawn below must not resolve
	// to the session being deleted.
	if err := removeSessionArtifacts(st.SessionFile); err != nil {
		return fmt.Errorf("не удалось удалить файлы сессии %s: %v", alias, err)
	}
	if active, ok := readActivePointer(activePointerFile(dir, peerID)); ok && active == alias {
		// Never delete the session the peer runs on: move it to the
		// default session first. Seed an empty default file when the
		// peer never had one, so the switch has a target.
		def := filepath.Join(dir, defaultStateFileName)
		if _, err := os.Stat(def); os.IsNotExist(err) {
			s := b.session(peerID)
			s.mu.Lock()
			seed := peerState{Workdir: s.workdir, PeerID: peerID, LastUsed: nowRFC3339()}
			s.mu.Unlock()
			data, _ := json.Marshal(seed)
			if err := os.WriteFile(def+".tmp", data, 0o644); err != nil {
				return err
			}
			if err := os.Rename(def+".tmp", def); err != nil {
				return err
			}
		}
		if err := b.SwitchSession(ctx, peerID, defaultSessionName); err != nil {
			return fmt.Errorf("не удалось переключиться перед удалением %s: %v", alias, err)
		}
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	b.debugf("peer %d: session %q deleted", peerID, alias)
	return nil
}

// ListSessions renders the peer's saved sessions (the default one plus all
// named ones), most recently used first, marking the peer's active session
// with a leading ▶. Legacy numeric-named leftovers are not listed (they
// were, or should have been, migrated to the default file).
func (b *Bridge) ListSessions(peerID int64) (string, error) {
	dir, err := b.stateDirOrErr()
	if err != nil {
		return "Сессий нет. /new <имя> [путь] — создать.", nil
	}
	b.migrateLegacyState(peerID)
	active, hasPointer := readActivePointer(activePointerFile(dir, peerID))
	if !hasPointer {
		active = "" // no pointer = the default session
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	type sessionEntry struct {
		alias   string
		st      peerState
		lastUse time.Time
	}
	var items []sessionEntry
	add := func(alias, file string) {
		st, found := loadStateFile(filepath.Join(dir, file))
		if !found {
			return
		}
		t, _ := time.Parse(time.RFC3339, st.LastUsed)
		items = append(items, sessionEntry{alias: alias, st: st, lastUse: t})
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if name == defaultStateFileName {
			add("", name)
			continue
		}
		if !strings.HasPrefix(name, stateFilePrefix) || !strings.HasSuffix(name, stateFileSuffix) {
			continue
		}
		raw := strings.TrimSuffix(strings.TrimPrefix(name, stateFilePrefix), stateFileSuffix)
		if _, perr := strconv.ParseInt(raw, 10, 64); perr == nil {
			continue // legacy per-peer leftover, not a named session
		}
		add(raw, name)
	}
	if len(items) == 0 {
		return "Сессий нет. /new <имя> [путь] — создать.", nil
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].lastUse.After(items[j].lastUse) })
	lines := make([]string, 0, len(items))
	for _, it := range items {
		marker := "  "
		if it.alias == active {
			marker = "▶ "
		}
		wd := it.st.Workdir
		if wd == "" {
			wd = "—"
		}
		lines = append(lines, fmt.Sprintf("%s%s — %s (last used %s)", marker, sessionLabel(it.alias), wd, formatLastUsed(it.st.LastUsed)))
	}
	return strings.Join(lines, "\n"), nil
}

// formatLastUsed renders a persisted RFC3339 UTC timestamp in local time
// (minute precision); unknown values render as "—".
func formatLastUsed(raw string) string {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return "—"
	}
	return t.Local().Format("2006-01-02 15:04")
}
