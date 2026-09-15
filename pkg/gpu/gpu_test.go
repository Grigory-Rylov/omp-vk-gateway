package gpu

import (
	"strings"
	"testing"
)

const singleGPUSample = `Thu Sep 10 20:10:49 2026       
+-----------------------------------------------------------------------------------------+
| NVIDIA-SMI 610.57.04              KMD Version: 610.57.04     CUDA UMD Version: 13.3     |
+-----------------------------------------+------------------------+----------------------+
| GPU  Name                 Persistence-M | Bus-Id          Disp.A | Volatile Uncorr. ECC |
| Fan  Temp   Perf          Pwr:Usage/Cap |           Memory-Usage | GPU-Util  Compute M. |
|                                         |                        |               MIG M. |
|=========================================+========================+======================|
|   0  NVIDIA GeForce RTX 3090        Off |   00000000:01:00.0 Off |                  N/A |
| 63%   54C    P2            162W /  270W |   20304MiB /  24576MiB |     11%      Default |
|                                         |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+

+-----------------------------------------------------------------------------------------+
| Processes:                                                                              |
|  GPU   GI   CI              PID   Type   Process name                        GPU Memory |
|        ID   ID                                                               Usage      |
|=========================================================================================|
|    0   N/A  N/A            2905      C   ...p/build-cuda/bin/llama-server      20090MiB |
+-----------------------------------------------------------------------------------------+
`

const multiGPUSample = `Mon Jun  9 12:00:00 2025
+-----------------------------------------------------------------------------------------+
| NVIDIA-SMI 595.58.03              Driver Version: 595.58.03      CUDA Version: 13.2     |
+-----------------------------------------+------------------------+----------------------+
| GPU  Name                 Persistence-M | Bus-Id          Disp.A | Volatile Uncorr. ECC |
| Fan  Temp   Perf          Pwr:Usage/Cap |           Memory-Usage | GPU-Util  Compute M. |
|                                         |                        |               MIG M. |
|=========================================+========================+======================|
|   0  NVIDIA GeForce RTX 3090        On  |   00000000:01:00.0 Off |                  N/A |
| 50%   56C    P2            111W /  300W |   11248MiB /  24576MiB |      0%      Default |
|                                         |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+
|   1  NVIDIA GeForce RTX 4090        On  |   00000000:02:00.0  On |                  N/A |
| N/A   41C    P8             22W /  450W |   10240MiB /  24576MiB |      5%      Default |
|                                         |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+
`

func TestParseNvidiaSMISingleGPU(t *testing.T) {
	info, err := ParseNvidiaSMI(singleGPUSample)
	if err != nil {
		t.Fatalf("ParseNvidiaSMI returned error: %v", err)
	}

	if info.SMIVersion != "610.57.04" {
		t.Errorf("SMIVersion = %q, want %q", info.SMIVersion, "610.57.04")
	}
	if info.DriverVersion != "610.57.04" {
		t.Errorf("DriverVersion = %q, want %q", info.DriverVersion, "610.57.04")
	}
	if info.CUDAVersion != "13.3" {
		t.Errorf("CUDAVersion = %q, want %q", info.CUDAVersion, "13.3")
	}
	if info.GPUCount() != 1 {
		t.Fatalf("GPUCount = %d, want 1", info.GPUCount())
	}

	want := GPUInfo{
		ID: 0, Name: "NVIDIA GeForce RTX 3090", Utilization: 11, Temperature: 54,
		PerfState: "P2", PowerUsage: 162, PowerCap: 270,
		MemoryUsed: 20304, MemoryTotal: 24576,
	}
	if info.GPUs[0] != want {
		t.Errorf("GPUs[0] = %+v, want %+v", info.GPUs[0], want)
	}
}

func TestParseNvidiaSMIMultiGPU(t *testing.T) {
	info, err := ParseNvidiaSMI(multiGPUSample)
	if err != nil {
		t.Fatalf("ParseNvidiaSMI returned error: %v", err)
	}

	if info.DriverVersion != "595.58.03" {
		t.Errorf("DriverVersion = %q, want %q", info.DriverVersion, "595.58.03")
	}
	if info.CUDAVersion != "13.2" {
		t.Errorf("CUDAVersion = %q, want %q", info.CUDAVersion, "13.2")
	}
	if info.GPUCount() != 2 {
		t.Fatalf("GPUCount = %d, want 2", info.GPUCount())
	}

	names := []string{"NVIDIA GeForce RTX 3090", "NVIDIA GeForce RTX 4090"}
	states := []string{"P2", "P8"}
	temps := []int{56, 41}
	powers := []int{111, 22}
	caps := []int{300, 450}
	used := []int{11248, 10240}
	for i, gpu := range info.GPUs {
		if gpu.ID != i {
			t.Errorf("GPUs[%d].ID = %d, want %d", i, gpu.ID, i)
		}
		if gpu.Name != names[i] {
			t.Errorf("GPUs[%d].Name = %q, want %q", i, gpu.Name, names[i])
		}
		if gpu.PerfState != states[i] {
			t.Errorf("GPUs[%d].PerfState = %q, want %q", i, gpu.PerfState, states[i])
		}
		if gpu.Temperature != temps[i] {
			t.Errorf("GPUs[%d].Temperature = %d, want %d", i, gpu.Temperature, temps[i])
		}
		if gpu.PowerUsage != powers[i] {
			t.Errorf("GPUs[%d].PowerUsage = %d, want %d", i, gpu.PowerUsage, powers[i])
		}
		if gpu.PowerCap != caps[i] {
			t.Errorf("GPUs[%d].PowerCap = %d, want %d", i, gpu.PowerCap, caps[i])
		}
		if gpu.MemoryUsed != used[i] {
			t.Errorf("GPUs[%d].MemoryUsed = %d, want %d", i, gpu.MemoryUsed, used[i])
		}
		if gpu.MemoryTotal != 24576 {
			t.Errorf("GPUs[%d].MemoryTotal = %d, want 24576", i, gpu.MemoryTotal)
		}
	}
}

func TestParseNvidiaSMIInvalidOutput(t *testing.T) {
	cases := []struct {
		name   string
		output string
	}{
		{"empty", ""},
		{"whitespace", "   \n\n  \n"},
		{"garbage", "command not found: nvidia-smi\nbash: nothing here\n"},
		{"versions only", "| NVIDIA-SMI 550.54.14   Driver Version: 550.54.14   CUDA Version: 12.4 |\n"},
		{"header without stats", "|   0  NVIDIA GeForce RTX 3090        Off |\n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info, err := ParseNvidiaSMI(tc.output)
			if err == nil {
				t.Fatalf("expected error for %q, got %+v", tc.output, info)
			}
			if info != nil {
				t.Errorf("expected nil info on error, got %+v", info)
			}
		})
	}
}

func TestGPUInfoMemoryPercent(t *testing.T) {
	cases := []struct {
		name string
		gpu  GPUInfo
		want int
	}{
		{"typical", GPUInfo{MemoryUsed: 11248, MemoryTotal: 24576}, 45},
		{"full", GPUInfo{MemoryUsed: 24576, MemoryTotal: 24576}, 100},
		{"empty", GPUInfo{MemoryUsed: 0, MemoryTotal: 24576}, 0},
		{"zero total", GPUInfo{MemoryUsed: 100, MemoryTotal: 0}, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.gpu.MemoryPercent(); got != tc.want {
				t.Errorf("MemoryPercent() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestFormatSingleGPU(t *testing.T) {
	info := &NvidiaInfo{
		SMIVersion: "595.58.03", DriverVersion: "595.58.03", CUDAVersion: "13.2",
		GPUs: []GPUInfo{{
			ID: 0, Name: "NVIDIA GeForce RTX 3090", Utilization: 50, Temperature: 56,
			PerfState: "P2", PowerUsage: 111, PowerCap: 300,
			MemoryUsed: 11248, MemoryTotal: 24576,
		}},
	}

	want := "🎮 GPU 1x\n" +
		"Driver: 595.58.03 | CUDA: 13.2\n" +
		"GPU 0: NVIDIA GeForce RTX 3090\n" +
		"  50%  56C  P2  111W/300W  11248/24576MiB (45%)"

	got := Format(info)
	if got != want {
		t.Errorf("Format() =\n%q\nwant\n%q", got, want)
	}
}

func TestFormatWithExtraTemps(t *testing.T) {
	info := &NvidiaInfo{
		DriverVersion: "595.58.03", CUDAVersion: "13.2",
		GPUs: []GPUInfo{{
			ID: 0, Name: "NVIDIA GeForce RTX 3090", Utilization: 11, Temperature: 54,
			PerfState: "P2", PowerUsage: 162, PowerCap: 270,
			MemoryUsed: 20304, MemoryTotal: 24576,
			VRAMTemp: 64, JunctionTemp: 67,
		}},
	}

	want := "🎮 GPU 1x\n" +
		"Driver: 595.58.03 | CUDA: 13.2\n" +
		"GPU 0: NVIDIA GeForce RTX 3090\n" +
		"  11%  54C  P2  162W/270W  20304/24576MiB (82%)\n" +
		"  Junction: 67C  VRAM: 64C"

	got := Format(info)
	if got != want {
		t.Errorf("Format() =\n%q\nwant\n%q", got, want)
	}
}

func TestFormatJunctionOnlyOrVramOnly(t *testing.T) {
	cases := []struct {
		name     string
		gpu      GPUInfo
		wantLine string
	}{
		{"junction only", GPUInfo{JunctionTemp: 70}, "  Junction: 70C"},
		{"vram only", GPUInfo{VRAMTemp: 61}, "  VRAM: 61C"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.gpu.ID = 0
			tc.gpu.Name = "NVIDIA GeForce RTX 3090"
			tc.gpu.MemoryTotal = 24576
			info := &NvidiaInfo{DriverVersion: "1", CUDAVersion: "2", GPUs: []GPUInfo{tc.gpu}}
			got := Format(info)
			if !strings.HasSuffix(got, "\n"+tc.wantLine) {
				t.Errorf("Format() = %q, want suffix %q", got, tc.wantLine)
			}
		})
	}
}

func TestFormatMultipleGPUs(t *testing.T) {
	info := &NvidiaInfo{
		DriverVersion: "595.58.03", CUDAVersion: "13.2",
		GPUs: []GPUInfo{
			{ID: 0, Name: "NVIDIA GeForce RTX 3090", Utilization: 0, Temperature: 56, PerfState: "P2", PowerUsage: 111, PowerCap: 300, MemoryUsed: 11248, MemoryTotal: 24576},
			{ID: 1, Name: "NVIDIA GeForce RTX 4090", Utilization: 5, Temperature: 41, PerfState: "P8", PowerUsage: 22, PowerCap: 450, MemoryUsed: 10240, MemoryTotal: 24576},
		},
	}

	got := Format(info)
	if !strings.HasPrefix(got, "🎮 GPU 2x\n") {
		t.Errorf("Format() header = %q, want prefix %q", got, "🎮 GPU 2x")
	}
	if !strings.Contains(got, "GPU 0: NVIDIA GeForce RTX 3090") || !strings.Contains(got, "GPU 1: NVIDIA GeForce RTX 4090") {
		t.Errorf("Format() missing GPU lines:\n%s", got)
	}
	if strings.Count(got, "MiB (") != 2 {
		t.Errorf("Format() expected 2 stats lines:\n%s", got)
	}
}

func TestFormatEmptyOrNil(t *testing.T) {
	if got := Format(nil); got != "" {
		t.Errorf("Format(nil) = %q, want empty", got)
	}
	if got := Format(&NvidiaInfo{}); got != "" {
		t.Errorf("Format(empty) = %q, want empty", got)
	}
}

func TestParseNvidiaSMIRealOutputMatchesFormat(t *testing.T) {
	info, err := ParseNvidiaSMI(singleGPUSample)
	if err != nil {
		t.Fatalf("ParseNvidiaSMI returned error: %v", err)
	}
	got := Format(info)
	want := "🎮 GPU 1x\n" +
		"Driver: 610.57.04 | CUDA: 13.3\n" +
		"GPU 0: NVIDIA GeForce RTX 3090\n" +
		"  11%  54C  P2  162W/270W  20304/24576MiB (82%)"
	if got != want {
		t.Errorf("Format() =\n%q\nwant\n%q", got, want)
	}
}
