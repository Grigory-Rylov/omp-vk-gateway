package vk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"omp-vk-gateway/pkg/logger"
)

// ErrSessionReset is returned by the agent backend when a turn is interrupted
// because the peer's session was reset (e.g. /newsession) mid-turn. The
// handler treats it as "no reply" instead of an error message.
var ErrSessionReset = errors.New("session reset")

// ModelRef identifies an available model by provider and model id.
type ModelRef struct {
	Provider string
	ID       string
}

// String returns the "provider/id" reference used by /r and the keyboard.
func (m ModelRef) String() string {
	return m.Provider + "/" + m.ID
}

// AgentBackend is the per-peer agent surface the handler drives.
// Implemented by the omp RPC bridge (pkg/agent).
type AgentBackend interface {
	// EnsureSession lazily starts the peer's agent session.
	EnsureSession(peerID int64)
	// ProcessMessage runs one prompt (or steer while streaming) and blocks
	// until the turn completes; it returns the final assistant text.
	// ctx cancellation aborts the turn; ErrSessionReset is returned when the
	// session was reset underneath the turn.
	ProcessMessage(ctx context.Context, message string, peerID int64) (string, error)
	// NewSession resets the in-flight session in place (working dir preserved).
	NewSession(ctx context.Context, peerID int64) error
	// ResetSession stops the agent and respawns it in the given workdir.
	ResetSession(ctx context.Context, peerID int64, workdir string) error
	// Status returns a human-readable multi-line status block.
	Status(ctx context.Context, peerID int64) (string, error)
	// Models lists available models and the current "provider/id" reference.
	Models(ctx context.Context, peerID int64) (models []ModelRef, current string, err error)
	// SetModel switches the peer's model by "provider/id" reference.
	SetModel(ctx context.Context, peerID int64, ref string) error

	// SteerText injects a message into the peer's running turn.
	SteerText(ctx context.Context, message string, peerID int64) error
	// Abort cancels the peer's active turn, if any.
	Abort(peerID int64)
	// AbortAll cancels every active turn.
	AbortAll()
	IsStreaming(peerID int64) bool
	// WorkingDir returns the peer's current working directory.
	WorkingDir(peerID int64) string
	// SetThinkingCallback registers a sink for mirrored thinking/tool lines.
	SetThinkingCallback(fn func(peerID int64, line string) error)
	// CloseAll stops every agent subprocess.
	CloseAll()
}

func expandTilde(path string) string {
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err == nil {
			if path == "~" {
				return home
			}
			return home + path[1:]
		}
	}
	return path
}

func truncateText(s string, max int, suffix string) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	if suffix == "" {
		return string(runes[:max])
	}
	return string(runes[:max]) + suffix
}

type BotHandler struct {
	vkClient       *BotClient
	backend        AgentBackend
	log            *logger.Logger
	mainPeerID     int64
	thinkingPeerID int64
	defaultWorkdir string
	attachmentsDir string

	cancelFuncs      map[int64]*cancelEntry
	resumeOwners     map[int64]bool
	cancelMu         sync.RWMutex
	peerProcessors   map[int64]*sync.Mutex
	peerProcessorsMu sync.RWMutex

	semaphore chan struct{}

	pendingKeyboards  map[int64]map[string]interface{}
	pendingKeyboardMu sync.RWMutex

	queueMu       sync.Mutex
	waitingCounts map[int64]int
	generations   map[int64]uint64

	restartSignalFile string
}

const maxConcurrentHandlers = 10

// NewBotHandler creates a handler. defaultWorkdir is the working directory
// given to peers on first contact (and by /clear).
func NewBotHandler(vkClient *BotClient, backend AgentBackend, log *logger.Logger,
	mainPeerID, thinkingPeerID int64, defaultWorkdir string) *BotHandler {
	return &BotHandler{
		vkClient:          vkClient,
		backend:           backend,
		log:               log,
		mainPeerID:        mainPeerID,
		thinkingPeerID:    thinkingPeerID,
		defaultWorkdir:    defaultWorkdir,
		attachmentsDir:    "./attachments",
		cancelFuncs:       make(map[int64]*cancelEntry),
		resumeOwners:      make(map[int64]bool),
		peerProcessors:    make(map[int64]*sync.Mutex),
		pendingKeyboards:  make(map[int64]map[string]interface{}),
		waitingCounts:     make(map[int64]int),
		generations:       make(map[int64]uint64),
		semaphore:         make(chan struct{}, maxConcurrentHandlers),
		restartSignalFile: ".agent-restart",
	}
}

func (h *BotHandler) SetAttachmentsDir(dir string) { h.attachmentsDir = dir }
func (h *BotHandler) SetRestartSignalFile(name string) {
	if name != "" {
		h.restartSignalFile = name
	}
}

// ProcessMessage runs one VK message: commands are handled inline, plain text
// is sent to the peer's agent backend. The returned string is the reply to
// send back to VK (empty = nothing to send).
func (h *BotHandler) ProcessMessage(message string, peerID int64) string {
	h.backend.EnsureSession(peerID)

	command := extractCommand(message)

	if strings.HasPrefix(command, "/") {
		result := h.handleCommand(command, peerID)
		if result != "" {
			return result
		}
		if restarterCommands[extractBaseCommand(command)] {
			return ""
		}
		return fmt.Sprintf("Неизвестная команда: %s. Напишите /help для списка команд.", command)
	}

	h.launchTurn(command, peerID)
	return ""
}

// launchTurn queues a plain-text turn: messages arriving while the peer is
// streaming are steered into the running turn; otherwise the peer mutex
// serializes a fresh prompt. Replies come from the backend's final-text
// callback wiring (see SendTurnResult), not from the return value.
func (h *BotHandler) launchTurn(message string, peerID int64) {
	if h.backend.IsStreaming(peerID) {
		if err := h.steerWhileStreaming(message, peerID); err != nil && h.log != nil {
			h.log.WarnLogf("Steer for peer %d failed: %v", peerID, err)
		}
		return
	}

	releaseQueueSlot, generationAtArrival := h.beginProcessingWait(peerID)
	mu := h.getPeerMutex(peerID)
	select {
	case h.semaphore <- struct{}{}:
	default:
		// Saturated: commands are dropped, plain text is admitted as a steer
		// when the peer turns out to be streaming after all.
		mu.Lock()
		releaseQueueSlot()
		if h.peerGeneration(peerID) != generationAtArrival {
			mu.Unlock()
			return
		}
		if h.backend.IsStreaming(peerID) {
			mu.Unlock()
			if err := h.steerWhileStreaming(message, peerID); err != nil && h.log != nil {
				h.log.WarnLogf("Steer for peer %d failed: %v", peerID, err)
			}
			return
		}
		mu.Unlock()
		h.log.WarnLogf("Dropping message from peer %d: max concurrent handlers (%d) reached", peerID, maxConcurrentHandlers)
		return
	}
	defer func() { <-h.semaphore }()
	mu.Lock()
	releaseQueueSlot()
	defer mu.Unlock()

	if h.peerGeneration(peerID) != generationAtArrival {
		if h.log != nil {
			h.log.InfoLogf("peer %d: session was reset while message waited, dropping", peerID)
		}
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entry := &cancelEntry{cancel: cancel}
	h.setCancelFunc(peerID, entry)
	defer h.clearCancelFunc(peerID, entry)

	response, err := h.backend.ProcessMessage(ctx, message, peerID)
	if errors.Is(err, ErrSessionReset) || errors.Is(err, context.Canceled) {
		if h.log != nil {
			h.log.InfoLogf("Turn for peer %d canceled", peerID)
		}
		return
	}
	if err != nil {
		if h.log != nil {
			h.log.ErrorLogf("Agent error for peer %d: %v", peerID, err)
		}
		h.sendReply(peerID, fmt.Sprintf("❌ Ошибка: %v", err))
		return
	}
	if response == "" {
		return
	}
	h.sendTurnResult(peerID, response)
}

// steerWhileStreaming forwards an in-turn message to the running agent.
func (h *BotHandler) steerWhileStreaming(message string, peerID int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return h.backend.SteerText(ctx, message, peerID)
}

// SendTurnResult delivers a completed turn's final text to VK, attaching any
// keyboard that was staged for the peer (e.g. after /models).
func (h *BotHandler) SendTurnResult(peerID int64, text string) {
	h.sendTurnResult(peerID, text)
}

func (h *BotHandler) sendTurnResult(peerID int64, text string) {
	target := peerID
	if h.mainPeerID > 0 {
		target = h.mainPeerID
	}
	kb := h.popPendingKeyboard(peerID)
	if kb != nil {
		if _, err := h.vkClient.SendMessageWithKeyboard(target, text, kb); err != nil && h.log != nil {
			h.log.ErrorLogf("Failed to send result with keyboard to peer %d: %v", target, err)
		}
		return
	}
	if _, err := h.vkClient.SendMessage(target, text); err != nil && h.log != nil {
		h.log.ErrorLogf("Failed to send result to peer %d: %v", target, err)
	}
}

func (h *BotHandler) sendReply(peerID int64, text string) {
	if text == "" {
		return
	}
	target := peerID
	if h.mainPeerID > 0 {
		target = h.mainPeerID
	}
	if _, err := h.vkClient.SendMessage(target, text); err != nil && h.log != nil {
		h.log.ErrorLogf("Failed to send reply to peer %d: %v", target, err)
	}
}

func (h *BotHandler) getPeerMutex(peerID int64) *sync.Mutex {
	h.peerProcessorsMu.RLock()
	mu, ok := h.peerProcessors[peerID]
	h.peerProcessorsMu.RUnlock()
	if ok {
		return mu
	}

	h.peerProcessorsMu.Lock()
	defer h.peerProcessorsMu.Unlock()
	if mu, ok = h.peerProcessors[peerID]; ok {
		return mu
	}
	mu = &sync.Mutex{}
	h.peerProcessors[peerID] = mu
	return mu
}

func extractCommand(message string) string {
	message = strings.TrimSpace(message)

	if len(message) > 0 && message[0] == '[' {
		closeIdx := strings.Index(message, "]")
		if closeIdx > 0 && closeIdx < len(message)-1 {
			rest := strings.TrimSpace(message[closeIdx+1:])
			return rest
		}
	}

	return message
}

var restarterCommands = map[string]bool{
	"/restart": true,
	"/update":  true,
	"/stop":    true,
}

func (h *BotHandler) handleCommand(input string, peerID int64) string {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return ""
	}
	cmd := parts[0]

	switch cmd {
	case "/clear":
		return h.handleClear(peerID)

	case "/newsession", "/n":
		return h.handleNewSession(input, peerID)

	case "/help":
		return "Доступные команды:\n" +
			"/clear — Очистить историю диалога (рабочая директория сохраняется)\n" +
			"/newsession [path] (/n) — Сбросить сессию и сменить рабочую директорию\n" +
			"/status — Статус агента: модель, сессия, контекст\n" +
			"/log — Отправить файлы из папки debug/\n" +
			"/m, /models — Список доступных моделей\n" +
			"/r [provider/model] — Переключить текущую модель\n" +
			"/abort — Остановить текущий ответ\n" +
			"/restart — Перезапустить агент-процесс(ы) oh-my-pi\n" +
			"/update — git pull + пересборка + перезапуск (нужен restarter)"

	case "/log", "/logs":
		return h.handleLogCommand(peerID)

	case "/status":
		return h.handleStatus(peerID)

	case "/m", "/models":
		return h.handleModelsList(peerID)

	case "/r":
		return h.handleModelSwitch(input, peerID)

	case "/abort":
		h.cancelActiveRequest(peerID)
		return "Останавливаю текущий ответ..."

	case "/restart":
		return h.handleRestart()

	case "/update":
		return h.handleUpdate()

	default:
		return ""
	}
}

func (h *BotHandler) handleClear(peerID int64) string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wd := h.backend.WorkingDir(peerID)
	if wd == "" {
		wd = h.defaultWorkdir
	}
	if err := h.backend.NewSession(ctx, peerID); err != nil {
		if h.log != nil {
			h.log.WarnLogf("/clear for peer %d: %v", peerID, err)
		}
		return fmt.Sprintf("❌ Не удалось очистить сессию: %v", err)
	}
	h.bumpPeerGeneration(peerID)
	return fmt.Sprintf("История очищена.\nРабочая директория: %s", wd)
}

func (h *BotHandler) handleNewSession(input string, peerID int64) string {
	newPath := ""
	parts := strings.SplitN(input, " ", 2)
	if len(parts) > 1 {
		newPath = strings.TrimSpace(parts[1])
	}
	if newPath == "" {
		newPath = h.backend.WorkingDir(peerID)
	}
	if newPath == "" {
		newPath = h.defaultWorkdir
	}
	if newPath == "" {
		var err error
		newPath, err = os.Getwd()
		if err != nil {
			return "Ошибка: не удалось определить рабочую директорию."
		}
	}

	newPath = expandTilde(newPath)

	info, err := os.Stat(newPath)
	if err != nil || !info.IsDir() {
		return fmt.Sprintf("Ошибка: директория '%s' не существует.", newPath)
	}
	absPath, err := filepath.Abs(newPath)
	if err != nil {
		return fmt.Sprintf("Ошибка: не удалось получить абсолютный путь: %v", err)
	}

	h.cancelActiveRequest(peerID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := h.backend.ResetSession(ctx, peerID, absPath); err != nil {
		if h.log != nil {
			h.log.ErrorLogf("ResetSession for peer %d: %v", peerID, err)
		}
		return fmt.Sprintf("❌ Не удалось сбросить сессию: %v", err)
	}

	h.bumpPeerGeneration(peerID)
	if h.log != nil {
		h.log.InfoLogf("Session reset for peer %d, working dir: %s", peerID, absPath)
	}
	return fmt.Sprintf("Сессия сброшена.\nРабочая директория: %s", absPath)
}

func (h *BotHandler) handleStatus(peerID int64) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h.backend.EnsureSession(peerID)
	status, err := h.backend.Status(ctx, peerID)
	if err != nil {
		return fmt.Sprintf("❌ Ошибка получения статуса: %v", err)
	}
	return status
}

func (h *BotHandler) handleModelsList(peerID int64) string {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	models, current, err := h.backend.Models(ctx, peerID)
	if err != nil {
		return fmt.Sprintf("❌ Ошибка получения списка моделей: %v", err)
	}
	if len(models) == 0 {
		return "Список моделей пуст. Проверьте конфигурацию и доступные API-ключи."
	}

	refs := make([]string, 0, len(models))
	for _, m := range models {
		refs = append(refs, m.String())
	}
	h.setPendingKeyboard(peerID, CreateModelsKeyboard(refs, current))

	var b strings.Builder
	b.WriteString("Доступные модели:\n")
	for _, m := range models {
		mark := " "
		if m.String() == current {
			mark = "✓"
		}
		b.WriteString(fmt.Sprintf("  %s %s\n", mark, m.String()))
	}
	if current != "" {
		b.WriteString(fmt.Sprintf("\nТекущая: %s", current))
	}
	return b.String()
}

func (h *BotHandler) handleModelSwitch(input string, peerID int64) string {
	parts := strings.SplitN(input, " ", 2)
	if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
		return "Укажите модель: /r provider/model\nСписок моделей: /m"
	}
	ref := strings.TrimSpace(parts[1])

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.backend.SetModel(ctx, peerID, ref); err != nil {
		return fmt.Sprintf("❌ %v", err)
	}
	return fmt.Sprintf("✓ Модель переключена на: %s", ref)
}

func (h *BotHandler) handleRestart() string {
	h.backend.AbortAll()
	if err := os.WriteFile(h.restartSignalFile, []byte(fmt.Sprint(time.Now().Unix())), 0644); err != nil {
		if h.log != nil {
			h.log.WarnLogf("Failed to write %s: %v", h.restartSignalFile, err)
		}
	}
	if h.log != nil {
		h.log.InfoLogf("Agent restart requested via /restart")
	}
	return "Перезапуск агента..."
}

func (h *BotHandler) handleUpdate() string {
	const sig = ".agent-update"
	if err := os.WriteFile(sig, []byte(fmt.Sprint(time.Now().Unix())), 0644); err != nil {
		if h.log != nil {
			h.log.WarnLogf("Failed to write %s: %v", sig, err)
		}
		return fmt.Sprintf("❌ Не удалось записать сигнал: %v", err)
	}
	return "Обновление: git pull + пересборка + перезапуск (обработает vk-gateway-restarter)"
}

func (h *BotHandler) setPendingKeyboard(peerID int64, kb map[string]interface{}) {
	h.pendingKeyboardMu.Lock()
	h.pendingKeyboards[peerID] = kb
	h.pendingKeyboardMu.Unlock()
}

func (h *BotHandler) popPendingKeyboard(peerID int64) map[string]interface{} {
	h.pendingKeyboardMu.Lock()
	kb := h.pendingKeyboards[peerID]
	delete(h.pendingKeyboards, peerID)
	h.pendingKeyboardMu.Unlock()
	return kb
}

func (h *BotHandler) payloadToCommand(payloadJSON string) string {
	var payload struct {
		Command string `json:"command"`
		Ref     string `json:"ref"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return ""
	}
	switch payload.Command {
	case "model_switch":
		return fmt.Sprintf("/r %s", payload.Ref)
	default:
		return ""
	}
}

func (h *BotHandler) logDirPath() string {
	logPath := "debug/gateway.log"
	if h.log != nil {
		if configured := h.log.LogFilePath(); configured != "" {
			logPath = configured
		}
	}
	absPath, err := filepath.Abs(logPath)
	if err != nil {
		return logPath
	}
	return filepath.Dir(absPath)
}

func (h *BotHandler) collectDebugFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read debug dir: %w", err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		files = append(files, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(files)
	return files, nil
}

func (h *BotHandler) handleLogCommand(peerID int64) string {
	dir := h.logDirPath()
	files, err := h.collectDebugFiles(dir)
	if err != nil || len(files) == 0 {
		if h.log != nil {
			h.log.WarnLogf("/log: debug dir is empty or missing: %s (%v)", dir, err)
		}
		return fmt.Sprintf("❌ Файлы логов не найдены в: %s", dir)
	}
	targetPeer := peerID
	if h.mainPeerID > 0 {
		targetPeer = h.mainPeerID
	}
	for _, file := range files {
		if _, err := h.vkClient.UploadAndSendDocument(file, targetPeer, "📋 Логи"); err != nil {
			if h.log != nil {
				h.log.ErrorLogf("/log: send %s failed: %v", filepath.Base(file), err)
			}
			return fmt.Sprintf("❌ Ошибка отправки лога %s: %v", filepath.Base(file), err)
		}
	}
	return fmt.Sprintf("📋 Отправлено файлов: %d (из %s)", len(files), dir)
}

type cancelEntry struct {
	cancel    context.CancelFunc
	cancelled bool
}

func (h *BotHandler) cancelActiveRequest(peerID int64) {
	h.cancelMu.Lock()
	defer h.cancelMu.Unlock()
	if entry, ok := h.cancelFuncs[peerID]; ok {
		entry.cancel()
		entry.cancelled = true
		delete(h.cancelFuncs, peerID)
	}
	h.backend.Abort(peerID)
}

func (h *BotHandler) setCancelFunc(peerID int64, entry *cancelEntry) {
	h.cancelMu.Lock()
	defer h.cancelMu.Unlock()
	if prev, ok := h.cancelFuncs[peerID]; ok && !prev.cancelled {
		prev.cancel()
	}
	h.cancelFuncs[peerID] = entry
}

func (h *BotHandler) clearCancelFunc(peerID int64, entry *cancelEntry) {
	h.cancelMu.Lock()
	defer h.cancelMu.Unlock()
	if cur, ok := h.cancelFuncs[peerID]; ok && cur == entry {
		delete(h.cancelFuncs, peerID)
	}
}

// extractBaseCommand returns the command word without its arguments
// (e.g. "/newsession /path" -> "/newsession").
func extractBaseCommand(input string) string {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func (h *BotHandler) beginProcessingWait(peerID int64) (release func(), generation uint64) {
	h.queueMu.Lock()
	h.waitingCounts[peerID]++
	waiting := h.waitingCounts[peerID]
	generation = h.generations[peerID]
	h.queueMu.Unlock()

	if waiting > 1 && h.log != nil {
		h.log.DebugLogf("peer %d: %d message(s) waiting for session", peerID, waiting)
	}
	return func() {
		h.queueMu.Lock()
		defer h.queueMu.Unlock()
		if h.waitingCounts[peerID] > 0 {
			h.waitingCounts[peerID]--
		}
	}, generation
}

func (h *BotHandler) bumpPeerGeneration(peerID int64) {
	h.queueMu.Lock()
	defer h.queueMu.Unlock()
	h.generations[peerID]++
}

func (h *BotHandler) peerGeneration(peerID int64) uint64 {
	h.queueMu.Lock()
	defer h.queueMu.Unlock()
	return h.generations[peerID]
}

// Start runs the VK long-poll loop until ctx is done.
func (h *BotHandler) Start(ctx context.Context) error {
	if h.log != nil {
		h.log.InfoLog("Starting VK Long Poll bot...")
	}
	for {
		select {
		case <-ctx.Done():
			if h.log != nil {
				h.log.InfoLog("Bot handler stopped")
			}
			return nil
		default:
			server, key, ts, err := h.vkClient.GetLongPollServer()
			if err != nil {
				if h.log != nil {
					h.log.WarnLogf("Failed to get long poll server: %v", err)
				}
				if !sleepCtx(ctx, 3*time.Second) {
					return nil
				}
				continue
			}
			if h.log != nil {
				h.log.InfoLog("Connected to VK Long Poll server")
			}
			if err := h.runLongPoll(ctx, server, key, ts); err != nil {
				if h.log != nil {
					h.log.WarnLogf("Long poll disconnected: %v", err)
				}
				if !sleepCtx(ctx, 3*time.Second) {
					return nil
				}
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) (awoken bool) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (h *BotHandler) runLongPoll(ctx context.Context, server, key string, ts int64) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			messages, newTs, err := h.vkClient.CheckUpdates(ctx, server, key, ts)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				if strings.Contains(err.Error(), "long poll failed") {
					return err
				}
				if !sleepCtx(ctx, time.Second) {
					return nil
				}
				continue
			}
			ts = newTs
			fullMsgMap := h.fetchFullMessages(messages)
			for _, msg := range messages {
				if h.thinkingPeerID > 0 && msg.PeerID == h.thinkingPeerID {
					continue
				}
				replyPeerID := msg.PeerID
				if h.mainPeerID > 0 {
					replyPeerID = h.mainPeerID
				}
				h.launchMessageHandler(msg, replyPeerID, fullMsgMap)
			}
		}
	}
}

func (h *BotHandler) fetchFullMessages(messages []VKMessage) map[int64]VKMessage {
	if len(messages) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(messages))
	for _, m := range messages {
		if m.EventID != "" || m.ID == 0 {
			continue
		}
		ids = append(ids, m.ID)
	}
	if len(ids) == 0 {
		return nil
	}
	full, err := h.vkClient.GetMessagesByID(ids)
	if err != nil {
		if h.log != nil {
			h.log.WarnLogf("Failed to fetch full messages: %v", err)
		}
		return nil
	}
	result := make(map[int64]VKMessage, len(full))
	for _, m := range full {
		result[m.ID] = m
	}
	return result
}

func (h *BotHandler) handleIncomingMessage(
	msg VKMessage,
	targetPeer int64,
	fullMsgMap map[int64]VKMessage,
) {
	isCallback := msg.EventID != ""

	if isCallback {
		logger.DebugToFile("[handler] callback received: peerID=%d, eventID=%s, payload=%s",
			msg.PeerID, msg.EventID, msg.Payload)
		if err := h.vkClient.SendMessageEventAnswer(msg.EventID, msg.FromID, msg.PeerID, ""); err != nil && h.log != nil {
			h.log.ErrorLogf("Failed to answer callback event: %v", err)
		}
	}

	atts := resolveAttachments(&msg, fullMsgMap)
	fullText := h.buildFullText(&msg, fullMsgMap, atts)
	if msg.Payload != "" {
		if cmd := h.payloadToCommand(msg.Payload); cmd != "" {
			logger.DebugToFile("[handler] callback payload: peerID=%d, cmd=%s", msg.PeerID, cmd)
			fullText = cmd
		}
	}

	logger.DebugToFile("[handler] goroutine: peerID=%d, targetPeer=%d, text=%s",
		msg.PeerID, targetPeer, truncateText(fullText, 100, "..."))

	reply := h.ProcessMessage(fullText, msg.PeerID)
	if reply != "" {
		h.sendTurnResult(msg.PeerID, reply)
	}
}

func (h *BotHandler) launchMessageHandler(
	msg VKMessage,
	replyPeerID int64,
	fullMsgMap map[int64]VKMessage,
) {
	go func() {
		select {
		case h.semaphore <- struct{}{}:
			defer func() { <-h.semaphore }()
			h.handleIncomingMessage(msg, replyPeerID, fullMsgMap)
		default:
			text := strings.TrimSpace(msg.Text)
			if msg.EventID == "" && text != "" && !strings.HasPrefix(text, "/") {
				if h.backend.IsStreaming(msg.PeerID) {
					logger.DebugToFile("[handler] semaphore saturated: admitting text from peer %d as steer", msg.PeerID)
					if err := h.steerWhileStreaming(text, msg.PeerID); err != nil && h.log != nil {
						h.log.WarnLogf("Steer admission for peer %d failed: %v", msg.PeerID, err)
					}
				} else if h.log != nil {
					h.log.WarnLogf("Dropping message from peer %d: max concurrent handlers (%d) reached", msg.PeerID, maxConcurrentHandlers)
				}
			}
		}
	}()
}

func resolveAttachments(msg *VKMessage, fullMsgMap map[int64]VKMessage) []VKAttachment {
	full, found := fullMsgMap[msg.ID]
	if found && len(full.Attachments) > 0 {
		return full.Attachments
	}
	if len(msg.Attachments) > 0 {
		if !found {
			logger.DebugToFile("[buildFullText] msg id=%d: full message not fetched, falling back to long-poll attachments", msg.ID)
		}
		return msg.Attachments
	}
	return nil
}

func (h *BotHandler) buildFullText(msg *VKMessage, fullMsgMap map[int64]VKMessage, atts []VKAttachment) string {
	if len(atts) == 0 {
		atts = resolveAttachments(msg, fullMsgMap)
	}
	if len(atts) == 0 {
		return msg.Text
	}

	logger.DebugToFile("[buildFullText] msg id=%d: %d attachment(s): %s", msg.ID, len(atts), describeAttachments(atts))
	downloaded, err := DownloadAttachments(toRawAttachments(atts), h.attachmentsDir, h.vkClient)
	if err != nil {
		logger.DebugToFile("[buildFullText] msg id=%d: download failed: %v (downloaded %d of %d)", msg.ID, err, len(downloaded), len(atts))
	}
	info := FormatAttachmentInfo(downloaded)
	if info == "" {
		logger.DebugToFile("[buildFullText] msg id=%d: no attachments could be downloaded", msg.ID)
		return msg.Text
	}
	return msg.Text + "\n\n" + info
}

func describeAttachments(atts []VKAttachment) string {
	parts := make([]string, 0, len(atts))
	for _, a := range atts {
		parts = append(parts, a.Type)
	}
	return strings.Join(parts, ",")
}

func toRawAttachments(attachments []VKAttachment) []map[string]interface{} {
	result := make([]map[string]interface{}, len(attachments))
	for i, a := range attachments {
		result[i] = a.ToRaw()
	}
	return result
}
