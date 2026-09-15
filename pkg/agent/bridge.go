// Package agent implements the VK-side bridge to oh-my-pi agent processes.
//
// Architecture: one `omp --mode rpc` subprocess per VK peer (one agent
// session per process, working directory pinned at spawn via --cwd). The
// subprocess speaks newline-delimited JSON over stdio (protocol v2: oversized
// frames arrive as base64 rpc_chunk sequences that are reassembled here).
//
// Per peer:
//   - idle message  -> "prompt" command; the turn ends on agent_end with
//     isTerminal absent or true (a field set to false means the agent
//     deferred completion and will continue — keep waiting)
//   - streaming msg -> "steer" command
//   - /clear        -> "new_session" (process and cwd preserved)
//   - /newsession   -> process killed and respawned with the new --cwd
//   - /models /r    -> "get_available_models" / "set_model"
//
// Streaming events (text_delta, thinking_delta, tool_execution_start, ...)
// are demuxed by a per-process event pump; thinking/tool lines can be
// mirrored to a second VK peer, and the final assistant text is delivered
// back to the originating VK peer when the turn terminates.
package agent

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"omp-vk-gateway/pkg/logger"
	"omp-vk-gateway/pkg/vk"
)

// ErrProcessFailed reports that the agent subprocess failed to start.
var ErrProcessFailed = errors.New("agent process failed")

// Bridge owns one lazily-spawned omp RPC subprocess per VK peer.
type Bridge struct {
	mu           sync.Mutex
	peers        map[int64]*peerSession
	agentCmd     []string
	extraArgs    []string
	log          *logger.Logger
	thinker      func(peerID int64, line string) error
	readyTimeout time.Duration
}

// NewBridge creates the bridge. agentCmd is the full command line of the
// omp executable in rpc mode, e.g.
// ["/home/user/.bun/bin/omp", "--mode", "rpc"]. extraArgs are appended
// before --cwd (e.g. --model, --thinking, --approval-mode).
func NewBridge(agentCmd []string, extraArgs []string, log *logger.Logger) *Bridge {
	return &Bridge{
		peers:        make(map[int64]*peerSession),
		agentCmd:     agentCmd,
		extraArgs:    extraArgs,
		log:          log,
		readyTimeout: 60 * time.Second,
	}
}

// SetThinkingCallback registers the mirror sink (peerID, line) for
// thinking and tool-start lines.
func (b *Bridge) SetThinkingCallback(fn func(peerID int64, line string) error) {
	b.mu.Lock()
	b.thinker = fn
	b.mu.Unlock()
}

func (b *Bridge) debugf(format string, args ...interface{}) {
	if b.log != nil {
		b.log.DebugLogf(format, args...)
	}
}

func (b *Bridge) session(peerID int64) *peerSession {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.peers[peerID]
	if !ok {
		s = b.newPeerSession(peerID)
		b.peers[peerID] = s
	}
	return s
}

func (b *Bridge) newPeerSession(peerID int64) *peerSession {
	s := &peerSession{
		bridge:   b,
		peerID:   peerID,
		turnDone: make(chan turnResult, 8),
		resetCh:  make(chan struct{}, 8),
	}
	go b.supervise(peerID, s)
	return s
}

// EnsureSession lazily starts the peer's agent process (non-blocking).
func (b *Bridge) EnsureSession(peerID int64) {
	s := b.session(peerID)
	s.startIfNeeded()
}

// WorkingDir returns the peer's working directory (empty until spawned).
func (b *Bridge) WorkingDir(peerID int64) string {
	s := b.session(peerID)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workdir
}

// IsStreaming reports whether the peer has an in-flight agent turn.
func (b *Bridge) IsStreaming(peerID int64) bool {
	s := b.session(peerID)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turnActive
}

// Abort cancels the peer's active turn (no-op when idle).
func (b *Bridge) Abort(peerID int64) {
	s := b.session(peerID)
	s.abortTurn()
}

// AbortAll cancels every peer's active turn.
func (b *Bridge) AbortAll() {
	b.mu.Lock()
	peers := make([]*peerSession, 0, len(b.peers))
	for _, s := range b.peers {
		peers = append(peers, s)
	}
	b.mu.Unlock()
	for _, s := range peers {
		s.abortTurn()
	}
}

// CloseAll stops every agent subprocess.
func (b *Bridge) CloseAll() {
	b.mu.Lock()
	peers := make([]*peerSession, 0, len(b.peers))
	for _, s := range b.peers {
		peers = append(peers, s)
	}
	b.mu.Unlock()
	for _, s := range peers {
		s.close()
	}
}

// ProcessMessage runs one turn for the peer and blocks until it terminates.
// The returned string is the final assistant text (may be empty, e.g. after
// an abort or a local-only command). vk.ErrSessionReset is returned when the
// session was reset or the turn aborted underneath this call.
func (b *Bridge) ProcessMessage(ctx context.Context, message string, peerID int64) (string, error) {
	s := b.session(peerID)
	s.startIfNeeded()

	s.mu.Lock()
	if s.turnActive {
		// A turn is already in flight; steer into it. The reply for this
		// text is delivered with the running turn's final answer.
		s.mu.Unlock()
		if err := b.steer(s, message); err != nil {
			return "", err
		}
		return "", nil
	}
	s.turnActive = true
	s.turnAborted = false
	s.turnText.Reset()
	s.turnThinking.Reset()
	// Discard stale reset notifications queued before this turn started
	// (e.g. by the reset that triggered this process's respawn).
	for {
		select {
		case <-s.resetCh:
			continue
		default:
		}
		break
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.turnActive = false
		s.mu.Unlock()
	}()

	// Wait for a usable process: ready, still current, and not targeted by a
	// pending reset (a reset kills the current process and respawns a new
	// one; prompting the old one would lose the turn).
	var proc *process
	for range 20 {
		p, err := s.waitReady(ctx)
		if err != nil {
			return "", err
		}
		if !p.done() {
			s.mu.Lock()
			stale := s.resetRequested || s.proc != p
			s.mu.Unlock()
			if !stale {
				proc = p
				break
			}
		}
		select {
		case <-ctx.Done():
			return "", vk.ErrSessionReset
		case <-time.After(300 * time.Millisecond):
		}
	}
	if proc == nil {
		return "", fmt.Errorf("агент-процесс недоступен")
	}
	if err := b.prompt(s, proc, message); err != nil {
		return "", err
	}
	// A reset that landed while we selected the process / sent the prompt is
	// not a reset of this turn (its token was queued by the respawn): drop
	// it. A reset landing after this point is real and aborts the turn.
	s.mu.Lock()
	for {
		select {
		case <-s.resetCh:
			continue
		default:
		}
		break
	}
	aborted := s.turnAborted
	s.mu.Unlock()
	if aborted {
		return "", vk.ErrSessionReset
	}
	return s.waitTurnResult(ctx)
}

// SteerText injects a message into the peer's running turn.
func (b *Bridge) SteerText(ctx context.Context, message string, peerID int64) error {
	s := b.session(peerID)
	s.mu.Lock()
	active := s.turnActive
	s.mu.Unlock()
	if !active {
		return fmt.Errorf("нет активного ответа для перенаправления")
	}
	return b.steer(s, message)
}

func (b *Bridge) steer(s *peerSession, message string) error {
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()
	if proc == nil || proc.done() {
		return fmt.Errorf("агент-процесс недоступен")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return b.sendCommand(s, proc, ctx, "steer", map[string]interface{}{"message": message})
}

// prompt sends a "prompt" command and records the immediate ack.
func (b *Bridge) prompt(s *peerSession, proc *process, message string) error {
	resp, err := b.request(s, proc, context.Background(), "prompt", map[string]interface{}{"message": message}, 15*time.Second)
	if err != nil {
		return fmt.Errorf("prompt: %w", err)
	}
	// The prompt response encodes whether the agent will run:
	//   - no data: normal message (or a builtin that forwards a prompt) -
	//     the agent turn runs and agent events stream;
	//   - data.agentInvoked=true: skill/builtin that schedules an agent turn;
	//   - data.agentInvoked=false: local-only builtin - the agent will NOT
	//     emit agent_end, so finalize locally.
	var data struct {
		AgentInvoked bool `json:"agentInvoked"`
	}
	if resp.Data != nil {
		_ = json.Unmarshal(resp.Data, &data)
		if !data.AgentInvoked {
			// Local-only completion (e.g. a built-in slash command). The agent
			// will not emit agent_end for it; wait briefly for any command_output
			// or agent_end, then finalize with whatever text was produced.
			go b.finalizeLocalOnly(s)
		}
	}
	return nil
}

func (b *Bridge) finalizeLocalOnly(s *peerSession) {
	select {
	case res := <-s.turnDone:
		s.mu.Lock()
		if s.turnActive {
			s.deliverTurnLocked(res)
		}
		s.mu.Unlock()
	case <-time.After(10 * time.Second):
		s.mu.Lock()
		if s.turnActive {
			s.deliverTurnLocked(turnResult{text: s.turnText.String(), aborted: s.turnAborted})
		}
		s.mu.Unlock()
	}
}

// waitTurnResult blocks until the in-flight turn terminates.
func (s *peerSession) waitTurnResult(ctx context.Context) (string, error) {
	select {
	case <-ctx.Done():
		s.abortTurn()
		return "", vk.ErrSessionReset
	case <-s.resetCh:
		return "", vk.ErrSessionReset
	case res := <-s.turnDone:
		if res.aborted {
			return "", vk.ErrSessionReset
		}
		if res.err != nil {
			return "", res.err
		}
		return res.text, nil
	}
}

// NewSession resets the peer's session in place (working dir preserved).
func (b *Bridge) NewSession(ctx context.Context, peerID int64) error {
	s := b.session(peerID)
	s.mu.Lock()
	proc := s.proc
	active := s.turnActive
	s.mu.Unlock()

	if active {
		s.abortTurn()
	}
	if proc == nil || proc.done() || !proc.ready() {
		return nil // fresh process will start clean
	}
	resp, err := b.request(s, proc, ctx, "new_session", map[string]interface{}{}, 15*time.Second)
	if err != nil {
		return err
	}
	var data struct {
		Cancelled bool `json:"cancelled"`
	}
	if resp.Data != nil {
		_ = json.Unmarshal(resp.Data, &data)
	}
	if data.Cancelled {
		s.abortTurn()
	}
	return nil
}

// ResetSession stops the peer's agent and respawns it in workdir.
func (b *Bridge) ResetSession(ctx context.Context, peerID int64, workdir string) error {
	s := b.session(peerID)
	s.requestReset(workdir)
	return nil
}

// Status returns a human-readable multi-line status block.
func (b *Bridge) Status(ctx context.Context, peerID int64) (string, error) {
	s := b.session(peerID)
	s.mu.Lock()
	proc := s.proc
	wd := s.workdir
	active := s.turnActive
	s.mu.Unlock()

	var sb strings.Builder
	sb.WriteString("AI Agent (oh-my-pi) ")
	if s.procAlive() {
		sb.WriteString("активен")
	} else {
		sb.WriteString("не запущен")
	}
	sb.WriteString("\nPeer ID: ")
	sb.WriteString(fmt.Sprintf("%d", peerID))
	if wd != "" {
		sb.WriteString("\nРабочая директория: " + wd)
	}
	if active {
		sb.WriteString("\nСостояние: выполняется запрос")
	} else {
		sb.WriteString("\nСостояние: готов к работе")
	}

	if proc == nil || proc.done() || !proc.ready() {
		return sb.String(), nil
	}
	resp, err := b.request(s, proc, ctx, "get_state", nil, 10*time.Second)
	if err != nil {
		return sb.String(), nil
	}
	var st rpcSessionState
	if err := json.Unmarshal(resp.Data, &st); err != nil {
		return sb.String(), nil
	}
	if st.Model != nil {
		sb.WriteString(fmt.Sprintf("\nМодель: %s/%s", st.Model.Provider, st.Model.ID))
	}
	if st.ThinkingLevel != "" {
		sb.WriteString(fmt.Sprintf(" (thinking: %s)", st.ThinkingLevel))
	}
	if st.MessageCount > 0 {
		sb.WriteString(fmt.Sprintf("\nСообщений: %d", st.MessageCount))
	}
	if st.SessionID != "" {
		sb.WriteString(fmt.Sprintf("\nСессия: %s", st.SessionID))
	}
	if st.ContextUsage != nil {
		sb.WriteString(fmt.Sprintf("\nКонтекст: %d / %d токенов (%.0f%%)",
			st.ContextUsage.Tokens, st.ContextUsage.ContextWindow, st.ContextUsage.Percent))
	}
	if st.TokensPerSecond != nil {
		sb.WriteString(fmt.Sprintf("\nСкорость: %.1f ток/с", *st.TokensPerSecond))
	}
	return sb.String(), nil
}

// Models lists the peer's available models and the current "provider/id".
func (b *Bridge) Models(ctx context.Context, peerID int64) ([]vk.ModelRef, string, error) {
	s := b.session(peerID)
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()
	if proc == nil || proc.done() || !proc.ready() {
		return nil, "", fmt.Errorf("агент-процесс недоступен")
	}

	resp, err := b.request(s, proc, ctx, "get_available_models", nil, 15*time.Second)
	if err != nil {
		return nil, "", err
	}
	var out struct {
		Models []rpcModel `json:"models"`
	}
	if err := json.Unmarshal(resp.Data, &out); err != nil {
		return nil, "", err
	}
	refs := make([]vk.ModelRef, 0, len(out.Models))
	for _, m := range out.Models {
		if m.Provider == "" || m.ID == "" {
			continue
		}
		refs = append(refs, vk.ModelRef{Provider: m.Provider, ID: m.ID})
	}

	current := ""
	if stResp, err := b.request(s, proc, ctx, "get_state", nil, 10*time.Second); err == nil {
		var st rpcSessionState
		if json.Unmarshal(stResp.Data, &st) == nil && st.Model != nil && st.Model.Provider != "" {
			current = st.Model.Provider + "/" + st.Model.ID
		}
	}
	return refs, current, nil
}

// SetModel switches the peer's model by "provider/model" reference.
func (b *Bridge) SetModel(ctx context.Context, peerID int64, ref string) error {
	s := b.session(peerID)
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()
	if proc == nil || proc.done() || !proc.ready() {
		return fmt.Errorf("агент-процесс недоступен")
	}
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("ожидался формат provider/model, получено %q (список: /m)", ref)
	}
	_, err := b.request(s, proc, ctx, "set_model", map[string]interface{}{
		"provider": parts[0],
		"modelId":  parts[1],
	}, 15*time.Second)
	if err != nil {
		return fmt.Errorf("модель %q: %w", ref, err)
	}
	return nil
}

// request sends a command and waits for its correlated response.
func (b *Bridge) request(s *peerSession, proc *process, ctx context.Context,
	cmdType string, payload map[string]interface{}, timeout time.Duration) (*rpcResponse, error) {
	id := fmt.Sprintf("vk-%d-%s", s.peerID, proc.nextID())
	req := map[string]interface{}{
		"id":   id,
		"type": cmdType,
	}
	for k, v := range payload {
		req[k] = v
	}
	if err := proc.write(req); err != nil {
		return nil, err
	}
	ch := proc.addPending(id)

	effTimeout := timeout
	select {
	case <-ctx.Done():
		proc.dropPending(id)
		return nil, ctx.Err()
	case <-time.After(effTimeout):
		proc.dropPending(id)
		return nil, fmt.Errorf("%s: таймаут %s", cmdType, effTimeout)
	case r := <-ch:
		return &r, nil
	}
}

// process is one omp --mode rpc subprocess.
type process struct {
	peerID int64
	log    *logger.Logger

	mu       sync.Mutex
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	br       *bufio.Reader
	doneFlag bool

	readyOnce sync.Once
	readyCh   chan struct{}
	readyErr  error
	readyFlag atomic.Bool

	pendingMu sync.Mutex
	pending   map[string]chan rpcResponse

	eventMu sync.Mutex
	events  chan []byte // raw JSON lines for non-response frames (incl. chunks, handled by pump)

	exitedCh chan struct{}

	idCounter atomic.Uint64
}

func newProcess(peerID int64, log *logger.Logger) *process {
	return &process{
		peerID:   peerID,
		log:      log,
		readyCh:  make(chan struct{}),
		pending:  make(map[string]chan rpcResponse),
		events:   make(chan []byte, 1024),
		exitedCh: make(chan struct{}),
	}
}

func (p *process) nextID() string {
	return fmt.Sprintf("%d", p.idCounter.Add(1))
}

func (p *process) done() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.doneFlag
}

func (p *process) ready() bool { return p.readyFlag.Load() }

func (p *process) markDone() {
	p.mu.Lock()
	if p.doneFlag {
		p.mu.Unlock()
		return
	}
	p.doneFlag = true
	p.mu.Unlock()
	select {
	case <-p.exitedCh:
	default:
		close(p.exitedCh)
	}
	// fail all pending requests
	p.pendingMu.Lock()
	for id, ch := range p.pending {
		close(ch)
		delete(p.pending, id)
	}
	p.pendingMu.Unlock()
	if p.stdin != nil {
		p.stdin.Close()
	}
}

func (p *process) write(frame interface{}) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stdin == nil {
		return fmt.Errorf("agent stdin closed")
	}
	_, err = p.stdin.Write(data)
	return err
}

func (p *process) addPending(id string) <-chan rpcResponse {
	ch := make(chan rpcResponse, 1)
	p.pendingMu.Lock()
	p.pending[id] = ch
	p.pendingMu.Unlock()
	return ch
}

func (p *process) dropPending(id string) {
	p.pendingMu.Lock()
	delete(p.pending, id)
	p.pendingMu.Unlock()
}

// spawn starts the omp subprocess and blocks (in a background goroutine)
// until the ready frame + protocol v2 negotiation complete.
func (p *process) spawn(workdir string, agentCmd, extraArgs []string) error {
	args := append(append([]string{}, agentCmd[1:]...), extraArgs...)
	args = append(args, "--cwd", workdir)

	cmd := exec.Command(agentCmd[0], args...)
	cmd.Dir = workdir
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn omp: %w", err)
	}

	p.mu.Lock()
	p.cmd = cmd
	p.stdin = stdin
	p.br = bufio.NewReaderSize(stdout, 1024*1024)
	p.mu.Unlock()

	go p.readStderr(stderr)
	go p.readLoop()
	go p.waitExit(cmd)

	p.debugf("peer %d: omp spawned (pid %d, cwd %s)", p.peerID, cmd.Process.Pid, workdir)
	return nil
}

func (p *process) debugf(format string, args ...interface{}) {
	if p.log != nil {
		p.log.DebugLogf("[rpc/peer%d] "+format, append([]interface{}{p.peerID}, args...)...)
	}
}

func (p *process) readStderr(r io.Reader) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		p.debugf("stderr: %s", line)
		if p.log != nil {
			p.log.DebugLogf("[omp/peer%d] %s", p.peerID, line)
		}
	}
}

// readLoop consumes JSONL frames from stdout, reassembles v2 chunk frames,
// and routes responses to pending request channels and everything else to
// the session's event channel.
func (p *process) readLoop() {
	decoder := newChunkDecoder(p.debugf)
	for {
		line, err := p.br.ReadBytes('\n')
		if len(line) > 0 {
			p.handleLine(line, decoder)
		}
		if err != nil {
			p.debugf("stdout read stopped: %v", err)
			p.markDone()
			return
		}
	}
}

func (p *process) handleLine(line []byte, decoder *chunkDecoder) {
	payload := strings.TrimSpace(string(line))
	if payload == "" {
		return
	}
	var frame map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &frame); err != nil {
		p.debugf("malformed frame dropped: %.200s", payload)
		return
	}

	// v2 chunk reassembly
	if chunk, ok := frame["type"].(string); ok && chunk == "rpc_chunk" {
		reassembled, err := decoder.push(frame)
		if err != nil {
			p.debugf("chunk reassembly error: %v", err)
			return
		}
		if reassembled == "" {
			return // more chunks expected
		}
		var full map[string]interface{}
		if err := json.Unmarshal([]byte(reassembled), &full); err != nil {
			p.debugf("reassembled frame parse error: %v", err)
			return
		}
		frame = full
		payload = reassembled
	}

	typ, _ := frame["type"].(string)

	if respID, hasID := frame["id"].(string); hasID && typ == "response" {
		p.pendingMu.Lock()
		ch := p.pending[respID]
		delete(p.pending, respID)
		p.pendingMu.Unlock()
		if ch != nil {
			success, _ := frame["success"].(bool)
			r := rpcResponse{ID: respID, Command: strOf(frame["command"]), Success: success}
			if r.Command == "" {
				r.Command = "unknown"
			}
			if errStr, ok := frame["error"].(string); ok {
				r.Error = errStr
			}
			if data, ok := frame["data"]; ok && data != nil {
				if b, err := json.Marshal(data); err == nil {
					r.Data = b
				}
			}
			ch <- r
			if !r.Success {
				p.debugf("response error: %s: %s", r.Command, r.Error)
			}
		}
		return
	}

	// Event frame (ready, agent_start/end, message_update, ...)
	p.debugf("event: %s", typ)
	p.eventMu.Lock()
	evCh := p.events
	p.eventMu.Unlock()
	select {
	case evCh <- []byte(payload):
	default:
		p.debugf("event channel full, dropping %s frame", typ)
	}
}

func (p *process) waitExit(cmd *exec.Cmd) {
	err := cmd.Wait()
	p.debugf("process exited: %v", err)
	p.markDone()
}

// chunkDecoder reassembles protocol v2 rpc_chunk sequences into logical
// JSON frames (base64 payloads concatenated in index order).
type chunkDecoder struct {
	pending *pendingChunks
	logf    func(string, ...interface{})
}

type pendingChunks struct {
	chunkID    string
	count      int
	byteLength int
	nextIndex  int
	chunks     [][]byte
	received   int
}

func newChunkDecoder(logf func(string, ...interface{})) *chunkDecoder {
	return &chunkDecoder{logf: logf}
}

func (d *chunkDecoder) push(frame map[string]interface{}) (string, error) {
	chunkID, _ := frame["chunkId"].(string)
	index := int(toInt64Val(frame["index"]))
	count := int(toInt64Val(frame["count"]))
	byteLength := int(toInt64Val(frame["byteLength"]))
	data, _ := frame["data"].(string)

	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return "", fmt.Errorf("chunk %s: %w", chunkID, err)
	}

	if d.pending == nil {
		if index != 0 {
			return "", fmt.Errorf("chunk %s: sequence must start at index 0", chunkID)
		}
		d.pending = &pendingChunks{chunkID: chunkID, count: count, byteLength: byteLength}
	}
	pd := d.pending
	if pd.chunkID != chunkID || pd.count != count || pd.byteLength != byteLength || pd.nextIndex != index {
		return "", fmt.Errorf("chunk %s: sequence mismatch", chunkID)
	}
	pd.chunks = append(pd.chunks, decoded)
	pd.received += len(decoded)
	pd.nextIndex++
	if pd.nextIndex < pd.count {
		return "", nil
	}
	if pd.received != pd.byteLength {
		return "", fmt.Errorf("chunk %s: length mismatch (%d != %d)", chunkID, pd.received, pd.byteLength)
	}
	d.pending = nil
	var out strings.Builder
	for _, c := range pd.chunks {
		out.Write(c)
	}
	return out.String(), nil
}

func toInt64Val(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	}
	return 0
}

func strOf(v interface{}) string {
	s, _ := v.(string)
	return s
}

// rpcResponse is a correlated command response.
type rpcResponse struct {
	ID      string
	Command string
	Success bool
	Error   string
	Data    []byte
}

// rpcModel mirrors the RPC Model shape (subset we display).
type rpcModel struct {
	Provider  string `json:"provider"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	Thinking  bool   `json:"reasoning"`
	HasVision bool   `json:"hasVision"`
	HasTools  bool   `json:"hasTools"`
}

type rpcContextUsage struct {
	Tokens        int     `json:"tokens"`
	ContextWindow int     `json:"contextWindow"`
	Percent       float64 `json:"percent"`
}

// rpcSessionState mirrors get_state's data payload (subset we display).
type rpcSessionState struct {
	Model           *rpcModel        `json:"model"`
	ThinkingLevel   string           `json:"thinkingLevel"`
	IsStreaming     bool             `json:"isStreaming"`
	SessionID       string           `json:"sessionId"`
	SessionFile     string           `json:"sessionFile"`
	MessageCount    int              `json:"messageCount"`
	QueuedMessages  int              `json:"queuedMessageCount"`
	TokensPerSecond *float64         `json:"tokensPerSecond"`
	ContextUsage    *rpcContextUsage `json:"contextUsage"`
}
