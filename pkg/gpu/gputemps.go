package gpu

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const gputempsTimeout = 3 * time.Second

var gputempsPath = "~/projects/cpp/gpu-mem-temp/gputemps"

type extraTemps struct {
	Junction int `json:"junction"`
	VRAM     int `json:"vram"`
}

type gputempsEntry struct {
	Index int `json:"index"`
	extraTemps
}

type gputempsPayload struct {
	GPUs []gputempsEntry `json:"gpus"`
}

func mergeGputemps(info *NvidiaInfo) {
	if info == nil || info.GPUCount() == 0 {
		return
	}
	path, err := expandHome(gputempsPath)
	if err != nil {
		return
	}
	out, err := runGputemps(path)
	if err != nil {
		return
	}
	temps, err := parseGputempsJSON(out)
	if err != nil {
		return
	}
	applyExtraTemps(info, temps)
}

func expandHome(path string) (string, error) {
	if !strings.HasPrefix(path, "~") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolving home directory: %w", err)
	}
	if path == "~" {
		return home, nil
	}
	return home + path[1:], nil
}

func runGputemps(path string) (string, error) {
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("stat %s: %w", path, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), gputempsTimeout)
	defer cancel()

	out, err := exec.CommandContext(ctx, "sudo", "-n", path, "--once", "--json").Output()
	if err != nil {
		return "", fmt.Errorf("running %s: %w", path, err)
	}
	return string(out), nil
}

func parseGputempsJSON(out string) (map[int]extraTemps, error) {
	var payload gputempsPayload
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		return nil, fmt.Errorf("parsing gputemps json: %w", err)
	}

	temps := make(map[int]extraTemps, len(payload.GPUs))
	for _, entry := range payload.GPUs {
		temps[entry.Index] = entry.extraTemps
	}
	return temps, nil
}

func applyExtraTemps(info *NvidiaInfo, temps map[int]extraTemps) {
	for i := range info.GPUs {
		if found, ok := temps[info.GPUs[i].ID]; ok {
			info.GPUs[i].JunctionTemp = found.Junction
			info.GPUs[i].VRAMTemp = found.VRAM
		}
	}
}
