package gpu

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	binaryName       = "nvidia-smi"
	nvidiaSMITimeout = 5 * time.Second
	unknownValue     = "unknown"
)

var (
	smiVersionRe    = regexp.MustCompile(`NVIDIA-SMI\s+(\S+)`)
	driverVersionRe = regexp.MustCompile(`(?:Driver|KMD) Version:\s+(\S+)`)
	cudaVersionRe   = regexp.MustCompile(`CUDA (?:UMD )?Version:\s+(\S+)`)
	gpuHeaderRe     = regexp.MustCompile(`\|\s+(\d+)\s+(NVIDIA\s+\S+(?:\s+\S+)*?)\s+(On|Off)`)
	statsLineRe     = regexp.MustCompile(`(?:\d+%\s+)?(\d+)C\s+(P\d)\s+(\d+)W\s*/\s*(\d+)W\s*\|\s*(\d+)MiB\s*/\s*(\d+)MiB\s*\|\s*(\d+)%`)
)

type GPUInfo struct {
	ID           int
	Name         string
	Utilization  int
	Temperature  int
	PerfState    string
	PowerUsage   int
	PowerCap     int
	MemoryUsed   int
	MemoryTotal  int
	VRAMTemp     int
	JunctionTemp int
}

func (g GPUInfo) MemoryPercent() int {
	if g.MemoryTotal <= 0 {
		return 0
	}
	return g.MemoryUsed * 100 / g.MemoryTotal
}

type NvidiaInfo struct {
	SMIVersion    string
	DriverVersion string
	CUDAVersion   string
	GPUs          []GPUInfo
}

func (n *NvidiaInfo) GPUCount() int {
	if n == nil {
		return 0
	}
	return len(n.GPUs)
}

func Available() bool {
	_, err := exec.LookPath(binaryName)
	return err == nil
}

func Fetch(ctx context.Context) (*NvidiaInfo, error) {
	path, err := exec.LookPath(binaryName)
	if err != nil {
		return nil, fmt.Errorf("looking up %s: %w", binaryName, err)
	}

	ctx, cancel := context.WithTimeout(ctx, nvidiaSMITimeout)
	defer cancel()

	out, err := exec.CommandContext(ctx, path).Output()
	if err != nil {
		return nil, fmt.Errorf("running %s: %w", binaryName, err)
	}

	info, err := ParseNvidiaSMI(string(out))
	if err != nil {
		return nil, err
	}

	mergeGputemps(info)
	return info, nil
}

func ParseNvidiaSMI(out string) (*NvidiaInfo, error) {
	info := &NvidiaInfo{
		SMIVersion:    unknownValue,
		DriverVersion: unknownValue,
		CUDAVersion:   unknownValue,
	}

	for _, raw := range strings.Split(out, "\n") {
		line := strings.TrimSpace(raw)
		if applyVersions(info, line) {
			continue
		}
		if appendGPUHeader(info, line) {
			continue
		}
		applyStatsToLastGPU(info, line)
	}

	if err := validateNvidiaInfo(info); err != nil {
		return nil, err
	}
	return info, nil
}

func applyVersions(info *NvidiaInfo, line string) bool {
	found := false
	if match := smiVersionRe.FindStringSubmatch(line); match != nil {
		info.SMIVersion = match[1]
		found = true
	}
	if match := driverVersionRe.FindStringSubmatch(line); match != nil {
		info.DriverVersion = match[1]
		found = true
	}
	if match := cudaVersionRe.FindStringSubmatch(line); match != nil {
		info.CUDAVersion = match[1]
		found = true
	}
	return found
}

func appendGPUHeader(info *NvidiaInfo, line string) bool {
	match := gpuHeaderRe.FindStringSubmatch(line)
	if match == nil {
		return false
	}
	id, err := strconv.Atoi(match[1])
	if err != nil {
		return false
	}
	info.GPUs = append(info.GPUs, GPUInfo{ID: id, Name: strings.TrimSpace(match[2])})
	return true
}

func applyStatsToLastGPU(info *NvidiaInfo, line string) {
	if info.GPUCount() == 0 {
		return
	}

	last := &info.GPUs[len(info.GPUs)-1]
	if last.PerfState != "" {
		return
	}

	match := statsLineRe.FindStringSubmatch(line)
	if match == nil {
		return
	}

	numbers, ok := atoiAll(match[1], match[3], match[4], match[5], match[6], match[7])
	if !ok {
		return
	}

	last.Temperature, last.PowerUsage, last.PowerCap = numbers[0], numbers[1], numbers[2]
	last.MemoryUsed, last.MemoryTotal, last.Utilization = numbers[3], numbers[4], numbers[5]
	last.PerfState = match[2]
}

func atoiAll(groups ...string) ([]int, bool) {
	values := make([]int, 0, len(groups))
	for _, group := range groups {
		value, err := strconv.Atoi(group)
		if err != nil {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

func validateNvidiaInfo(info *NvidiaInfo) error {
	if info.GPUCount() == 0 {
		return fmt.Errorf("parsing %s output: no GPU entries found", binaryName)
	}
	for _, gpu := range info.GPUs {
		if gpu.PerfState == "" {
			return fmt.Errorf("parsing %s output: no stats line for GPU %d", binaryName, gpu.ID)
		}
	}
	return nil
}

func Format(info *NvidiaInfo) string {
	if info == nil || info.GPUCount() == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("🎮 GPU " + strconv.Itoa(info.GPUCount()) + "x\n")
	b.WriteString("Driver: " + info.DriverVersion + " | CUDA: " + info.CUDAVersion)
	for _, gpu := range info.GPUs {
		b.WriteString("\n" + formatGPUName(gpu) + "\n" + formatGPUStats(gpu))
		if line := formatExtraTemps(gpu); line != "" {
			b.WriteString("\n" + line)
		}
	}
	return b.String()
}

func formatGPUName(gpu GPUInfo) string {
	return "GPU " + strconv.Itoa(gpu.ID) + ": " + gpu.Name
}

func formatGPUStats(gpu GPUInfo) string {
	return "  " + strconv.Itoa(gpu.Utilization) + "%  " +
		strconv.Itoa(gpu.Temperature) + "C  " +
		gpu.PerfState + "  " +
		strconv.Itoa(gpu.PowerUsage) + "W/" + strconv.Itoa(gpu.PowerCap) + "W  " +
		strconv.Itoa(gpu.MemoryUsed) + "/" + strconv.Itoa(gpu.MemoryTotal) + "MiB (" +
		strconv.Itoa(gpu.MemoryPercent()) + "%)"
}

func formatExtraTemps(gpu GPUInfo) string {
	var temps []string
	if gpu.JunctionTemp != 0 {
		temps = append(temps, "Junction: "+strconv.Itoa(gpu.JunctionTemp)+"C")
	}
	if gpu.VRAMTemp != 0 {
		temps = append(temps, "VRAM: "+strconv.Itoa(gpu.VRAMTemp)+"C")
	}
	if len(temps) == 0 {
		return ""
	}
	return "  " + strings.Join(temps, "  ")
}
