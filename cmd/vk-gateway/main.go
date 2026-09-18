// Command vk-gateway bridges VK messages to oh-my-pi agent processes.
//
// One `omp --mode rpc` subprocess is spawned per VK peer (lazily, on first
// contact). The long-poll loop, command handling, and thinking mirroring live
// in pkg/vk; the agent subprocess lifecycle lives in pkg/agent.
//
// Configuration: config.json next to the binary (or in the working
// directory, or $HOME/.config/omp-vk-gateway/config.json).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"omp-vk-gateway/pkg/agent"
	"omp-vk-gateway/pkg/buildinfo"
	"omp-vk-gateway/pkg/logger"
	"omp-vk-gateway/pkg/vk"
)

// config is the gateway configuration (config.json).
type config struct {
	TokenVK              string   `json:"token_vk"`
	PeerID               int64    `json:"peer_id"`
	ThinkingPeerID       int64    `json:"thinking_peer_id"`
	Workdir              string   `json:"workdir"`
	Model                string   `json:"model"`
	ThinkingLevel        string   `json:"thinking_level"`
	ApprovalMode         string   `json:"approval_mode"`
	Debug                bool     `json:"debug"`
	LogFile              string   `json:"log_file"`
	AgentCmd             []string `json:"agent_cmd"`
	ExtraArgs            []string `json:"extra_args"`
	AgentNames           []string `json:"agent_names"`
	SubagentSubscription string   `json:"subagent_subscription"`
}

// Version is the release label; the exact build time is stamped by
// build.sh via -ldflags into pkg/buildinfo.BuildTime.
var Version = "dev"

func loadConfig(candidates ...string) (*config, error) {
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var cfg config
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return &cfg, nil
	}
	return nil, fmt.Errorf("config.json not found (looked in: %v)", candidates)
}

func main() {
	debugFlag := flag.Bool("d", false, "Enable debug mode")
	flag.Parse()

	// Config search order: CWD, next to the binary, user config dir.
	cwd, _ := os.Getwd()
	candidates := []string{
		filepath.Join(cwd, "config.json"),
	}
	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), "config.json"))
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".config", "omp-vk-gateway", "config.json"))
	}
	cfg, err := loadConfig(candidates...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error loading config:", err)
		os.Exit(1)
	}
	if cfg.TokenVK == "" {
		fmt.Fprintln(os.Stderr, "Error: config.json must set token_vk")
		os.Exit(1)
	}

	debug := cfg.Debug || *debugFlag
	logFile := cfg.LogFile
	if logFile == "" {
		logFile = "debug/gateway.log"
	}
	log := logger.New("gateway", debug, logFile)
	log.InfoLogf("VK Bot Gateway v%s (build %s, oh-my-pi backend) starting...", Version, buildinfo.HumanReadable())

	if cfg.AgentCmd == nil || len(cfg.AgentCmd) == 0 {
		cfg.AgentCmd = []string{defaultOMPPath()}
	}
	if len(cfg.AgentCmd) == 1 && !containsArg(cfg.AgentCmd, "--mode") {
		cfg.AgentCmd = append(cfg.AgentCmd, "--mode", "rpc")
	}

	extraArgs := append([]string{}, cfg.ExtraArgs...)
	if cfg.Model != "" {
		extraArgs = append(extraArgs, "--model", cfg.Model)
	}
	if cfg.ThinkingLevel != "" {
		extraArgs = append(extraArgs, "--thinking", cfg.ThinkingLevel)
	}
	if cfg.ApprovalMode != "" {
		extraArgs = append(extraArgs, "--approval-mode", cfg.ApprovalMode)
	}

	workdir := cfg.Workdir
	if workdir == "" {
		workdir = cwd
	}
	log.InfoLogf("Default workdir: %s", workdir)
	log.InfoLogf("Agent command: %v %v", cfg.AgentCmd, extraArgs)

	vkClient := vk.NewBotClient(cfg.TokenVK)
	backend := agent.NewBridge(cfg.AgentCmd, extraArgs, log)
	backend.SetSubagentSubscription(cfg.SubagentSubscription)
	// Persist per-peer coprocess state (session id/file, in-flight run_agent
	// descriptors) so a gateway restart or host reboot resumes the same
	// oh-my-pi session (via --resume) and re-issues interrupted @agent work.
	backend.SetStateDir(filepath.Join(workdir, "debug"))

	handler := vk.NewBotHandler(vkClient, backend, log,
		cfg.PeerID, cfg.ThinkingPeerID, workdir)
	handler.SetAttachmentsDir(filepath.Join(workdir, "attachments"))
	if len(cfg.AgentNames) > 0 {
		handler.SetAgentNames(cfg.AgentNames)
	}

	backend.SetRunAgentResultCallback(func(peerID int64, text string) error {
		if text == "" {
			return nil
		}
		// A directly-launched subagent's answer is surfaced in the reasoning
		// chat (the peer its lifecycle is mirrored to); fall back to the
		// originating chat when no reasoning peer is configured.
		target := peerID
		if cfg.ThinkingPeerID > 0 {
			target = cfg.ThinkingPeerID
		}
		if _, err := vkClient.SendMessage(target, text); err != nil {
			log.WarnLogf("Failed to deliver run_agent result to peer %d: %v", target, err)
			return err
		}
		return nil
	})

	if cfg.ThinkingPeerID > 0 {
		backend.SetThinkingCallback(func(peerID int64, line string) error {
			if _, err := vkClient.SendThinking(cfg.ThinkingPeerID, line); err != nil {
				log.WarnLogf("Failed to mirror thinking to peer %d: %v", cfg.ThinkingPeerID, err)
			}
			return nil
		})
	}

	// A restart re-sends a peer's interrupted in-flight turn on its --resume
	// session; the resulting answer has no incoming VK message to reply to,
	// so it is delivered here to that peer's own chat (the turn is the user's
	// own main-chat turn, unlike a directly-launched subagent which surfaces
	// in the reasoning chat).
	backend.SetReRunResultCallback(func(peerID int64, text string) error {
		if text == "" {
			return nil
		}
		if _, err := vkClient.SendMessage(peerID, text); err != nil {
			log.WarnLogf("Failed to deliver re-run result to peer %d: %v", peerID, err)
			return err
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.InfoLog("Shutting down...")
		cancel()
	}()

	if cfg.PeerID > 0 {
		startMsg := fmt.Sprintf("🤖 VK Gateway v%s (build %s, oh-my-pi) запущен.\n"+
			"Рабочая директория: %s\n"+
			"Напишите /help для списка команд.", Version, buildinfo.HumanReadable(), workdir)
		if _, err := vkClient.SendMessageWithKeyboard(cfg.PeerID, startMsg, vk.CreateCommandKeyboard()); err != nil {
			log.WarnLogf("Failed to send startup message: %v", err)
		}
	}

	// Restart survival: respawn persisted peers with --resume <sessionFile>
	// and re-issue run_agent work that was in flight when the gateway died.
	backend.ResumePeers()

	log.InfoLog("Starting VK Bot Handler...")
	if err := handler.Start(ctx); err != nil {
		log.ErrorLogf("Handler stopped: %v", err)
	}

	// Graceful shutdown: stop all agent subprocesses.
	backend.CloseAll()
	log.InfoLog("Gateway stopped")
}

func defaultOMPPath() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".bun", "bin", "omp")
	}
	return "omp"
}

func containsArg(args []string, target string) bool {
	for _, a := range args {
		if a == target {
			return true
		}
	}
	return false
}
