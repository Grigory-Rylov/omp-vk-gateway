package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"omp-vk-gateway/pkg/vk"
)

type turnResult struct {
	text    string
	aborted bool
	err     error
}

// peerSession is the per-VK-peer bridge state: one supervised omp process,
// one in-flight turn, and pending session-reset requests.
type peerSession struct {
	bridge *Bridge
	peerID int64

	mu             sync.Mutex
	proc           *process
	workdir        string
	turnActive     bool
	turnAborted    bool
	turnText       strings.Builder
	turnThinking   strings.Builder
	turnDone       chan turnResult
	resetCh        chan struct{}
	resetRequested bool
	closed         bool
	// badToolRetries counts consecutive terminal completions of the current
	// turn whose final text looked like a leaked tool-call attempt; reset on
	// every fresh turn and on any clean completion.
	badToolRetries int
	// subagentLine dedupes mirrored subagent progress lines per subagent
	// (key = subagent id, else agent#index); subagentThink buffers the
	// reasoning deltas of a subagent assistant message (events level) so they
	// flush as one line on message_end; subagentName maps subagent id ->
	// agent name for display prefixes. All reset per turn.
	subagentLine  map[string]string
	subagentThink map[string]*strings.Builder
	subagentName  map[string]string
	// resumeSessionID/File is the oh-my-pi session the coprocess holds
	// (captured via get_state): a respawn passes --resume <sessionFile> so
	// the conversation context survives gateway restarts and host reboots.
	resumeSessionID   string
	resumeSessionFile string
	// inFlightRuns persists run_agent descriptors (wire id -> descriptor)
	// so interrupted work can be re-issued after a gateway restart; cleared
	// on each run's terminal frame.
	inFlightRuns map[string]*runDescriptor
	// runAgentIDs tracks in-flight run_agent command ids (the coprocess
	// echoes each as parentToolCallId on the subagent's terminal lifecycle
	// frame). It is deliberately NOT reset by resetSubagentStateLocked: a
	// subagent outlives the peer's turns, so a fresh turn must not drop a
	// still-running run_agent.
	runAgentIDs map[string]bool
	// lastPrompt is the plain-text prompt of the in-flight normal turn (set
	// when the turn starts, cleared when it settles or the session is reset).
	// Persisted so a restart/reboot re-sends it on the --resume session.
	lastPrompt string
}

// resetSubagentState clears all per-subagent mirror state. Caller holds s.mu.
func (s *peerSession) resetSubagentStateLocked() {
	s.subagentLine = map[string]string{}
	s.subagentThink = map[string]*strings.Builder{}
	s.subagentName = map[string]string{}
}

func (s *peerSession) debugf(format string, args ...interface{}) {
	if s.bridge.log != nil {
		s.bridge.log.DebugLogf("[peer%d] "+format, append([]interface{}{s.peerID}, args...)...)
	}
}

// startIfNeeded spawns the process if not already running.
func (s *peerSession) startIfNeeded() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc != nil && !s.proc.done() {
		return
	}
	s.startProcessLocked()
}

// startProcessLocked spawns a fresh process for the current workdir and
// starts its event pump. Caller must hold s.mu.
func (s *peerSession) startProcessLocked() {
	s.resetRequested = false
	wd := s.workdir
	if wd == "" {
		if wd, _ = os.Getwd(); wd == "" {
			wd = "."
		}
	}
	b := s.bridge
	proc := newProcess(s.peerID, b.log)
	s.proc = proc
	// Resume the previous oh-my-pi session when its file still exists on
	// disk; otherwise fall back to a fresh session and drop the stale
	// resume values (oh-my-pi exits cleanly on a missing resume target,
	// which would otherwise loop through the ready-timeout / respawn cycle).
	resume := s.resumeSessionFile
	if resume != "" {
		if _, err := os.Stat(resume); err != nil {
			s.resumeSessionID = ""
			s.resumeSessionFile = ""
			b.persistStateLocked(s)
			resume = ""
		}
	}
	go func() {
		if err := proc.spawn(wd, resume, b.agentCmd, b.extraArgs); err != nil {
			s.debugf("spawn failed: %v", err)
			if b.log != nil {
				b.log.ErrorLogf("peer %d: spawn failed: %v", s.peerID, err)
			}
			proc.markDone()
			proc.failReady(err)
			return
		}
		go b.pumpEvents(s, proc)
		if err := s.waitReadyCtx(proc, context.Background(), b.readyTimeout); err != nil {
			s.debugf("ready timeout: %v", err)
			if b.log != nil {
				b.log.ErrorLogf("peer %d: %v", s.peerID, err)
			}
			proc.failReady(err)
			proc.kill()
		}
	}()
}

// waitReadyCtx blocks until the process finished startup and protocol
// negotiation (or failed/timed out).
func (s *peerSession) waitReadyCtx(proc *process, ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	select {
	case <-ctx.Done():
		return fmt.Errorf("агент-процесс не запустился за %s", timeout)
	case <-proc.readyCh:
		if proc.readyErr != nil {
			return fmt.Errorf("%w: %v", ErrProcessFailed, proc.readyErr)
		}
		return nil
	}
}

// waitReady returns a ready process, or an error. The caller must verify
// the process is still alive (it may be replaced by a respawn).
func (s *peerSession) waitReady(ctx context.Context) (*process, error) {
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()
	if proc == nil {
		return nil, fmt.Errorf("агент-процесс не запущен")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-proc.readyCh:
		if proc.readyErr != nil {
			return nil, fmt.Errorf("%w: %v", ErrProcessFailed, proc.readyErr)
		}
		return proc, nil
	}
}

// supervise respawns the process when it dies and applies pending resets.
func (b *Bridge) supervise(peerID int64, s *peerSession) {
	for {
		s.mu.Lock()
		proc := s.proc
		closed := s.closed
		s.mu.Unlock()

		if closed || proc == nil {
			time.Sleep(time.Second)
			continue
		}
		<-proc.exitedCh

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		resetReq := s.resetRequested
		wd := s.workdir
		if resetReq {
			s.resetRequested = false
		}
		s.mu.Unlock()

		if resetReq {
			s.debugf("respawning after reset, workdir %q", wd)
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				return
			}
			// An explicit /clear or /newsession is a fresh start: drop the
			// persisted resume target and in-flight runs so the respawn opens
			// a clean session. Crash respawns (resetReq false) keep the
			// session for --resume.
			s.resumeSessionID = ""
			s.resumeSessionFile = ""
			s.lastPrompt = ""
			for id := range s.inFlightRuns {
				delete(s.inFlightRuns, id)
			}
			b.persistStateLocked(s)
			s.startProcessLocked()
			s.notifyResetLocked()
			s.mu.Unlock()
			continue
		}

		s.notifyReset()
		s.debugf("process exited unexpectedly; respawning")
		if b.log != nil {
			b.log.WarnLogf("peer %d: agent process exited, respawning", peerID)
		}
		time.Sleep(2 * time.Second)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.startProcessLocked()
		s.mu.Unlock()
	}
}

// notifyReset tells a blocked turn waiter the session was reset (non-blocking).
func (s *peerSession) notifyReset() {
	s.notifyResetLocked()
}

func (s *peerSession) notifyResetLocked() {
	select {
	case s.resetCh <- struct{}{}:
	default:
	}
}

// requestReset marks the session for reset: the current process is killed
// and respawned (in workdir, or the current one when empty), and any
// in-flight turn is failed with vk.ErrSessionReset. The new workdir is
// applied immediately so WorkingDir reflects it without waiting for the
// respawn.
func (s *peerSession) requestReset(workdir string) {
	s.mu.Lock()
	if workdir != "" {
		s.workdir = workdir
	}
	wd := s.workdir
	s.resetRequested = true
	s.turnActive = false
	s.turnAborted = true
	proc := s.proc
	if proc != nil && !proc.done() {
		go proc.kill()
	}
	s.notifyResetLocked()
	s.mu.Unlock()
	s.debugf("reset requested, workdir %q", wd)
}

// abortTurn cancels the in-flight turn (best-effort abort RPC) so it
// finalizes as aborted (no reply sent, ErrSessionReset to the waiter).
func (s *peerSession) abortTurn() {
	s.mu.Lock()
	if !s.turnActive {
		s.mu.Unlock()
		return
	}
	s.turnAborted = true
	proc := s.proc
	s.mu.Unlock()

	if proc == nil || proc.done() || !proc.ready() {
		s.mu.Lock()
		if s.turnActive {
			s.deliverTurnLocked(turnResult{aborted: true})
		}
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.bridge.sendCommand(s, proc, ctx, "abort", map[string]interface{}{}); err != nil {
		s.debugf("abort failed: %v", err)
	}
	// Safety net: if the agent never emits the terminal agent_end.
	go func() {
		time.Sleep(15 * time.Second)
		s.mu.Lock()
		if s.turnActive && s.turnAborted {
			s.deliverTurnLocked(turnResult{aborted: true})
		}
		s.mu.Unlock()
	}()
}

// deliverTurnLocked finalizes the turn exactly once. Caller holds s.mu.
func (s *peerSession) deliverTurnLocked(res turnResult) {
	if !s.turnActive {
		return
	}
	s.turnActive = false
	// The turn has settled: the in-flight prompt is no longer pending, so
	// clear it (a restart now must not re-issue a completed turn). This is
	// the single choke point every settle path (agent_end / abort / reset)
	// funnels through.
	if s.lastPrompt != "" {
		s.lastPrompt = ""
		s.bridge.persistStateLocked(s)
	}
	select {
	case s.turnDone <- res:
	default:
		s.debugf("turn waiter gone, dropping result (%d chars)", len(res.text))
	}
}

// close stops the session and its process.
func (s *peerSession) close() {
	s.mu.Lock()
	s.closed = true
	s.turnActive = false
	proc := s.proc
	s.mu.Unlock()
	if proc != nil {
		go proc.kill()
	}
	s.debugf("closed")
}

func (s *peerSession) procAlive() bool {
	s.mu.Lock()
	p := s.proc
	s.mu.Unlock()
	return p != nil && !p.done() && p.ready()
}

// pumpEvents consumes the process event stream until it exits. One pump per
// spawned process; it is started as soon as the process is up, so the ready
// frame itself arrives here.
func (b *Bridge) pumpEvents(s *peerSession, proc *process) {
	for {
		line, ok := proc.nextEvent()
		if !ok {
			return
		}
		b.handleEvent(s, proc, line)
	}
}

// nextEvent returns the next reassembled event line; false when the process
// has exited.
func (p *process) nextEvent() (string, bool) {
	select {
	case line := <-p.events:
		return string(line), true
	case <-p.exitedCh:
		return "", false
	}
}

func (b *Bridge) handleEvent(s *peerSession, proc *process, line string) {
	var frame map[string]interface{}
	if err := json.Unmarshal([]byte(line), &frame); err != nil {
		return
	}
	switch strOf(frame["type"]) {
	case "ready":
		go b.negotiate(s, proc, line)
	case "message_update":
		b.handleMessageUpdate(s, frame)
	case "message_start":
		b.handleMessageStart(s, frame)
	case "message_end":
		b.handleMessageEnd(s, frame)
	case "tool_execution_start":
		b.mirrorToolStart(s, frame)
	case "agent_end":
		b.handleAgentEnd(s, frame)
	case "extension_ui_request":
		b.handleExtensionUI(s, frame)
	case "subagent_lifecycle":
		b.handleSubagentLifecycle(s, frame)
	case "subagent_progress":
		b.handleSubagentProgress(s, frame)
	case "subagent_event":
		b.handleSubagentEvent(s, frame)
	}
}

// handleMessageUpdate tracks the turn's streaming text (a fallback if a
// message_end frame is missed). Thinking deltas are accumulated in
// turnThinking and flushed as ONE message per completed assistant message
// (sending per-token messages trips VK flood control). The authoritative
// final text is set by handleMessageEnd, which overwrites turnText with the
// last completed assistant message.
func (b *Bridge) handleMessageUpdate(s *peerSession, frame map[string]interface{}) {
	evFrame, _ := frame["assistantMessageEvent"].(map[string]interface{})
	if evFrame == nil {
		return
	}
	switch strOf(evFrame["type"]) {
	case "thinking_delta":
		delta := strOf(evFrame["delta"])
		if delta == "" {
			return
		}
		s.mu.Lock()
		if s.turnActive {
			s.turnThinking.WriteString(delta)
		}
		s.mu.Unlock()
	case "text_delta":
		delta := strOf(evFrame["delta"])
		if delta == "" {
			return
		}
		s.mu.Lock()
		if s.turnActive {
			s.turnText.WriteString(delta)
		}
		s.mu.Unlock()
	}
}

// handleMessageStart resets the turn's text buffer when a new assistant
// message begins streaming, so the fallback (text_delta accumulation) never
// mixes text from different assistant messages of one turn.
func (b *Bridge) handleMessageStart(s *peerSession, frame map[string]interface{}) {
	msg, _ := frame["message"].(map[string]interface{})
	if msg == nil || strOf(msg["role"]) != "assistant" {
		return
	}
	s.mu.Lock()
	if s.turnActive {
		s.turnText.Reset()
		s.turnThinking.Reset()
	}
	s.mu.Unlock()
}

// handleMessageEnd overwrites the turn's reply text with the full text of a
// just-completed assistant message. A turn can contain several assistant
// messages (separated by tool calls); only the last one is the final answer
// to the user, so each message_end replaces turnText rather than appending.
// Non-assistant messages (tool results, user echoes) are ignored.
func (b *Bridge) handleMessageEnd(s *peerSession, frame map[string]interface{}) {
	msg, _ := frame["message"].(map[string]interface{})
	if msg == nil {
		return
	}
	if strOf(msg["role"]) != "assistant" {
		return
	}
	text := assistantMessageText(msg)
	thinking := ""
	s.mu.Lock()
	if s.turnActive {
		s.turnText.Reset()
		s.turnText.WriteString(text)
		thinking = s.turnThinking.String()
		s.turnThinking.Reset()
	}
	s.mu.Unlock()
	if thinking != "" {
		b.mirrorLine(s, "💭 "+strings.TrimSpace(thinking))
	}
}

// assistantMessageText joins the text content items of an assistant message.
// Thinking and tool-call blocks are excluded (thinking is mirrored separately).
func assistantMessageText(msg map[string]interface{}) string {
	items, _ := msg["content"].([]interface{})
	if items == nil {
		return ""
	}
	var sb strings.Builder
	for _, it := range items {
		m, ok := it.(map[string]interface{})
		if !ok {
			continue
		}
		if strOf(m["type"]) == "text" {
			sb.WriteString(strOf(m["text"]))
		}
	}
	return sb.String()
}

// mirrorToolStart mirrors a tool start line to the thinking peer.
func (b *Bridge) mirrorToolStart(s *peerSession, frame map[string]interface{}) {
	line := toolStartLine(frame)
	if line == "" {
		return
	}
	b.mirrorLine(s, line)
}

func (b *Bridge) mirrorLine(s *peerSession, line string) {
	b.mu.Lock()
	thinker := b.thinker
	b.mu.Unlock()
	if thinker == nil {
		return
	}
	_ = thinker(s.peerID, line)
}

func toolStartLine(frame map[string]interface{}) string {
	name := strOf(frame["toolName"])
	if name == "" {
		return ""
	}
	args := ""
	if a, ok := frame["args"]; ok && a != nil {
		if jb, err := json.Marshal(a); err == nil {
			args = strings.TrimSpace(string(jb))
			if len(args) > 120 {
				args = args[:120] + "…"
			}
		}
	}
	label := toolLabel(name)
	if args != "" {
		return label + ": " + args
	}
	return label
}

func toolLabel(name string) string {
	switch name {
	case "bash":
		return "💻 bash"
	case "read":
		return "📖 чтение файла"
	case "write":
		return "✍️ запись файла"
	case "edit":
		return "✏️ правка файла"
	case "glob":
		return "🔍 поиск файлов"
	case "grep":
		return "🔎 поиск по тексту"
	case "web_search", "search":
		return "🌐 поиск"
	case "browser":
		return "🌐 браузер"
	case "eval":
		return "🧮 eval"
	case "task":
		return "🤖 субагент"
	default:
		return "🔧 " + name
	}
}

// handleAgentEnd terminates the in-flight turn. A frame is terminal when
// isTerminal is absent or true AND willContinue is not true; isTerminal:false
// means the agent deferred completion (e.g. queued work) and will continue.
func (b *Bridge) handleAgentEnd(s *peerSession, frame map[string]interface{}) {
	terminal := true
	if v, present := frame["isTerminal"]; present {
		if b, ok := v.(bool); ok && !b {
			terminal = false
		}
	}
	if v, present := frame["willContinue"]; present {
		if b, ok := v.(bool); ok && b {
			terminal = false
		}
	}
	if !terminal {
		s.debugf("agent_end deferred (will continue)")
		return
	}
	s.mu.Lock()
	if !s.turnActive {
		s.mu.Unlock()
		return
	}
	text := s.turnText.String()
	aborted := s.turnAborted
	thinking := s.turnThinking.String()
	s.turnText.Reset()
	s.turnThinking.Reset()

	if !aborted && looksLikeBadToolCall(text) && s.badToolRetries < maxBadToolRetries {
		// The model emitted a tool-call attempt as plain text: nothing was
		// executed, so the agent ended its turn on that message. Hide the
		// raw attempt from the chat, log it, and ask the model to retry —
		// the turn stays active so the continuation's terminal agent_end
		// re-enters this path (budget capped by maxBadToolRetries).
		s.badToolRetries++
		attempt := s.badToolRetries
		proc := s.proc
		s.mu.Unlock()
		if b.log != nil {
			b.log.WarnLogf("peer %d: hiding malformed tool-call attempt %d/%d: %.300s", s.peerID, attempt, maxBadToolRetries, text)
		}
		s.debugf("agent_end (terminal) = leaked tool call, nudge %d/%d", attempt, maxBadToolRetries)
		go b.retryBadToolCall(s, proc)
		return
	}
	s.badToolRetries = 0
	s.debugf("agent_end (terminal), %d chars, aborted=%v", len(text), aborted)
	s.resetSubagentStateLocked()
	s.deliverTurnLocked(turnResult{text: text, aborted: aborted})
	s.mu.Unlock()
	if thinking != "" {
		b.mirrorLine(s, "💭 "+strings.TrimSpace(thinking))
	}
	// Turn settled: re-capture the session id/file (it can rotate between
	// turns) and persist for a later --resume.
	b.refreshSessionState(s, s.proc)
}

// retryBadToolCall asks the model to retry a turn that terminated on a
// leaked tool-call attempt. If the prompt cannot be delivered (process dead
// or reset underneath), the turn finalizes as aborted so its waiter does
// not hang.
func (b *Bridge) retryBadToolCall(s *peerSession, proc *process) {
	if proc == nil || proc.done() {
		s.debugf("bad-tool retry skipped, process gone")
		s.mu.Lock()
		if s.turnActive {
			s.turnAborted = true
			s.deliverTurnLocked(turnResult{aborted: true})
		}
		s.mu.Unlock()
		return
	}
	if err := b.prompt(s, proc, nudgeBadToolCall); err != nil {
		s.debugf("bad-tool retry prompt failed: %v", err)
		s.mu.Lock()
		if s.turnActive {
			s.turnAborted = true
			s.deliverTurnLocked(turnResult{aborted: true})
		}
		s.mu.Unlock()
	}
}

// handleExtensionUI answers extension UI sub-protocol requests. Policy for
// this gateway: mirror notify/setStatus to the thinking peer, auto-respond
// to the rest so the agent never blocks on a UI it cannot render.
func (b *Bridge) handleExtensionUI(s *peerSession, frame map[string]interface{}) {
	id := strOf(frame["id"])
	method := strOf(frame["method"])

	switch method {
	case "notify", "setStatus", "setWidget", "setTitle":
		msg := strOf(frame["message"])
		if msg == "" {
			msg = strOf(frame["title"])
		}
		if msg != "" {
			b.mirrorLine(s, "ℹ️ "+msg)
		}
	case "cancel":
		// nothing to answer
	case "confirm":
		b.replyExtension(s, id, map[string]interface{}{"confirmed": true})
	case "select":
		var opts []string
		if o, ok := frame["options"].([]interface{}); ok {
			for _, it := range o {
				if str, ok := it.(string); ok {
					opts = append(opts, str)
				}
			}
		}
		value := ""
		if len(opts) > 0 {
			value = opts[0]
		}
		if msg := strOf(frame["message"]); msg != "" {
			b.mirrorLine(s, fmt.Sprintf("❓ %s (автоответ: %s)", msg, value))
		}
		b.replyExtension(s, id, map[string]interface{}{"value": value})
	case "input":
		if msg := strOf(frame["message"]); msg != "" {
			b.mirrorLine(s, fmt.Sprintf("❓ %s (автоответ: пусто)", msg))
		}
		b.replyExtension(s, id, map[string]interface{}{"value": ""})
	case "editor":
		b.mirrorLine(s, "📝 редактор недоступен в VK — отменено")
		b.replyExtension(s, id, map[string]interface{}{"cancelled": true})
	case "open_url":
		if u := strOf(frame["url"]); u != "" {
			b.mirrorLine(s, "🔗 "+u)
		}
	default:
		s.debugf("unhandled extension_ui method: %s", method)
	}
}

func (b *Bridge) replyExtension(s *peerSession, id string, value map[string]interface{}) {
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()
	if proc == nil || proc.done() {
		return
	}
	frame := map[string]interface{}{
		"type": "extension_ui_response",
		"id":   id,
	}
	for k, v := range value {
		frame[k] = v
	}
	if err := proc.write(frame); err != nil {
		s.debugf("extension_ui_response %s failed: %v", id, err)
	}
}

// sendCommand writes a command and waits for its correlated ack.
func (b *Bridge) sendCommand(s *peerSession, proc *process, ctx context.Context,
	cmdType string, payload map[string]interface{}) error {
	_, err := b.request(s, proc, ctx, cmdType, payload, 15*time.Second)
	return err
}

// negotiate completes the v2 protocol handshake after the ready frame.
func (b *Bridge) negotiate(s *peerSession, proc *process, readyLine string) {
	var ready struct {
		Supported []int `json:"supportedProtocolVersions"`
	}
	_ = json.Unmarshal([]byte(readyLine), &ready)
	if containsInt(ready.Supported, 2) {
		if _, err := b.request(s, proc, context.Background(), "negotiate_protocol",
			map[string]interface{}{"protocolVersion": 2}, 30*time.Second); err != nil {
			s.debugf("v2 negotiation failed: %v (continuing on v1)", err)
			if b.log != nil {
				b.log.WarnLogf("peer %d: v2 negotiation failed: %v", s.peerID, err)
			}
		} else {
			s.debugf("protocol v2 negotiated")
		}
	}
	// Capture the session id/file right after the ready handshake, so even a
	// gateway killed before the first turn can later resume this coprocess.
	b.refreshSessionState(s, proc)
	if level := b.subagentSubscriptionLevel(); level != "off" {
		if _, err := b.request(s, proc, context.Background(), "set_subagent_subscription",
			map[string]interface{}{"level": level}, 15*time.Second); err != nil {
			s.debugf("set_subagent_subscription: %v", err)
		}
	}
	proc.markReady(nil)
}

func containsInt(list []int, v int) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// subagentKey identifies a subagent across lifecycle/progress/event frames:
// the subagent id when present, else agent#index.
func subagentKey(id, agent string, index int) string {
	if id != "" {
		return id
	}
	if agent != "" {
		return fmt.Sprintf("%s#%d", agent, index)
	}
	return "subagent"
}

// truncateLine trims s to at most n runes, appending an ellipsis when cut.
func truncateLine(s string, n int) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "\u2026"
}

// handleSubagentLifecycle mirrors a subagent start/finish line to the
// reasoning peer.
func (b *Bridge) handleSubagentLifecycle(s *peerSession, frame map[string]interface{}) {
	pl, _ := frame["payload"].(map[string]interface{})
	if pl == nil {
		return
	}
	id := strOf(pl["id"])
	agent := strOf(pl["agent"])
	index := intOf(pl["index"])
	key := subagentKey(id, agent, index)
	status := strOf(pl["status"])
	s.mu.Lock()
	s.subagentName[key] = agent
	switch status {
	case "started":
		if sb, ok := s.subagentThink[key]; ok {
			sb.Reset()
		}
	case "completed", "failed", "aborted":
		delete(s.subagentLine, key)
		delete(s.subagentThink, key)
	}
	s.mu.Unlock()
	var line string
	switch status {
	case "started":
		if desc := strOf(pl["description"]); desc != "" {
			line = fmt.Sprintf("\U0001F916 \u0441\u0443\u0431\u0430\u0433\u0435\u043d\u0442 %s [#%d] \u0437\u0430\u043f\u0443\u0449\u0435\u043d: %s", agent, index, truncateLine(desc, 200))
		} else {
			line = fmt.Sprintf("\U0001F916 \u0441\u0443\u0431\u0430\u0433\u0435\u043d\u0442 %s [#%d] \u0437\u0430\u043f\u0443\u0449\u0435\u043d", agent, index)
		}
	case "completed":
		line = fmt.Sprintf("\U0001F916 \u0441\u0443\u0431\u0430\u0433\u0435\u043d\u0442 %s [#%d] \u0437\u0430\u0432\u0435\u0440\u0448\u0451\u043d", agent, index)
	case "failed":
		line = fmt.Sprintf("\U0001F916 \u0441\u0443\u0431\u0430\u0433\u0435\u043d\u0442 %s [#%d] \u043e\u0448\u0438\u0431\u043a\u0430", agent, index)
	case "aborted":
		line = fmt.Sprintf("\U0001F916 \u0441\u0443\u0431\u0430\u0433\u0435\u043d\u0442 %s [#%d] \u043f\u0440\u0435\u0440\u0432\u0430\u043d", agent, index)
	default:
		return
	}
	b.mirrorLine(s, line)

	// run_agent side-channel: a terminal frame whose parentToolCallId matches
	// an in-flight run_agent delivers the subagent's final text to the VK peer
	// that launched it (separate from the reasoning mirror above, which still
	// fires so the reasoning peer shows the subagent activity).
	ptcid := strOf(pl["parentToolCallId"])
	if ptcid != "" {
		s.mu.Lock()
		inFlight := s.runAgentIDs[ptcid]
		// Consume the id only on a terminal frame: the started frame of the
		// same run also carries parentToolCallId and must not claim it.
		if inFlight && status != "started" {
			delete(s.runAgentIDs, ptcid)
			if r := s.inFlightRuns[ptcid]; r != nil {
				delete(s.inFlightRuns, ptcid)
				b.persistStateLocked(s)
			}
		}
		s.mu.Unlock()
		if inFlight && status != "started" {
			text := strOf(pl["output"])
			if status == "failed" {
				text = "\u26a0\ufe0f " + agent + " \u043e\u0448\u0438\u0431\u043a\u0430" + "\n" + text
			} else if status == "aborted" {
				text = "\u26a0\ufe0f " + agent + " \u043f\u0440\u0435\u0440\u0432\u0430\u043d"
			}
			b.mu.Lock()
			cb := b.runAgentResult
			b.mu.Unlock()
			if cb != nil {
				if err := cb(s.peerID, text); err != nil {
					s.debugf("run_agent deliver failed: %v", err)
				}
			}
		}
	}
}

// handleSubagentProgress mirrors a subagent progress line, deduped so only a
// changed gist reaches the reasoning peer (the upstream progress feed is
// coalesced to ~6/s per subagent and would trip VK flood control otherwise).
func (b *Bridge) handleSubagentProgress(s *peerSession, frame map[string]interface{}) {
	pl, _ := frame["payload"].(map[string]interface{})
	if pl == nil {
		return
	}
	agent := strOf(pl["agent"])
	index := intOf(pl["index"])
	id := ""
	gist := "\u0432\u044b\u043f\u043e\u043b\u043d\u044f\u0435\u0442\u0441\u044f"
	if pg, ok := pl["progress"].(map[string]interface{}); ok {
		id = strOf(pg["id"])
		if a := strOf(pg["agent"]); a != "" {
			agent = a
		}
		if i := intOf(pg["index"]); i != 0 {
			index = i
		}
		if li := strOf(pg["lastIntent"]); li != "" {
			gist = li
		} else if ct := strOf(pg["currentTool"]); ct != "" {
			gist = "\u0432\u044b\u043f\u043e\u043b\u043d\u044f\u0435\u0442 " + ct
		}
	}
	key := subagentKey(id, agent, index)
	line := fmt.Sprintf("\u23F3 %s: %s", agent, truncateLine(gist, 200))
	s.mu.Lock()
	if s.subagentName[key] == "" {
		s.subagentName[key] = agent
	}
	if s.subagentLine[key] == line {
		s.mu.Unlock()
		return
	}
	s.subagentLine[key] = line
	s.mu.Unlock()
	b.mirrorLine(s, line)
}

// handleSubagentEvent mirrors subagent session events (reasoning + tool
// starts) to the reasoning peer. Reasoning deltas are buffered per subagent
// assistant message and flushed as one line on message_end (per-token sends
// trip VK flood control).
func (b *Bridge) handleSubagentEvent(s *peerSession, frame map[string]interface{}) {
	pl, _ := frame["payload"].(map[string]interface{})
	if pl == nil {
		return
	}
	id := strOf(pl["id"])
	ev, _ := pl["event"].(map[string]interface{})
	if ev == nil {
		return
	}
	switch strOf(ev["type"]) {
	case "tool_execution_start":
		line := toolStartLine(map[string]interface{}{"toolName": ev["toolName"], "args": ev["args"]})
		if line == "" {
			return
		}
		s.mu.Lock()
		name := s.subagentName[id]
		s.mu.Unlock()
		if name == "" {
			name = id
		}
		b.mirrorLine(s, fmt.Sprintf("\U0001F916 %s: %s", name, line))
	case "message_start":
		msg, _ := ev["message"].(map[string]interface{})
		if msg == nil || strOf(msg["role"]) != "assistant" {
			return
		}
		s.mu.Lock()
		if s.subagentThink[id] == nil {
			s.subagentThink[id] = &strings.Builder{}
		}
		s.subagentThink[id].Reset()
		s.mu.Unlock()
	case "message_update":
		aev, _ := ev["assistantMessageEvent"].(map[string]interface{})
		if aev == nil || strOf(aev["type"]) != "thinking_delta" {
			return
		}
		if d := strOf(aev["delta"]); d != "" {
			s.mu.Lock()
			if sb, ok := s.subagentThink[id]; ok {
				sb.WriteString(d)
			}
			s.mu.Unlock()
		}
	case "message_end":
		msg, _ := ev["message"].(map[string]interface{})
		if msg == nil || strOf(msg["role"]) != "assistant" {
			return
		}
		s.mu.Lock()
		name := s.subagentName[id]
		if name == "" {
			name = id
		}
		thinking := ""
		if sb, ok := s.subagentThink[id]; ok {
			thinking = sb.String()
			delete(s.subagentThink, id)
		}
		s.mu.Unlock()
		if t := strings.TrimSpace(thinking); t != "" {
			b.mirrorLine(s, fmt.Sprintf("\U0001F4AD %s: %s", name, truncateLine(t, 1000)))
		}
	}
}

func intOf(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

// Ensure the bridge satisfies the VK handler's backend interface.
var _ vk.AgentBackend = (*Bridge)(nil)
