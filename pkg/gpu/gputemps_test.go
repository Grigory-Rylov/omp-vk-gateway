package gpu

import (
	"os"
	"os/exec"
	"testing"
)

func TestAvailableMatchesLookPath(t *testing.T) {
	_, err := exec.LookPath(binaryName)
	if got := Available(); got != (err == nil) {
		t.Errorf("Available() = %v, want %v", got, err == nil)
	}
}

func TestParseGputempsJSON(t *testing.T) {
	cases := []struct {
		name         string
		payload      string
		wantJunction map[int]int
		wantVRAM     map[int]int
		wantErr      bool
	}{
		{
			name:         "single gpu",
			payload:      `{"timestamp":1789060772,"gpus":[{"index":0,"core":53,"junction":67,"vram":64}]}`,
			wantJunction: map[int]int{0: 67},
			wantVRAM:     map[int]int{0: 64},
		},
		{
			name:         "two gpus",
			payload:      `{"gpus":[{"index":0,"junction":67,"vram":64},{"index":1,"junction":71,"vram":69}]}`,
			wantJunction: map[int]int{0: 67, 1: 71},
			wantVRAM:     map[int]int{0: 64, 1: 69},
		},
		{
			name:    "not json",
			payload: "sudo: a password is required\n",
			wantErr: true,
		},
		{
			name:         "empty gpus",
			payload:      `{"gpus":[]}`,
			wantJunction: map[int]int{},
			wantVRAM:     map[int]int{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			temps, err := parseGputempsJSON(tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %+v", tc.payload, temps)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseGputempsJSON returned error: %v", err)
			}
			if len(temps) != len(tc.wantJunction) {
				t.Fatalf("len(temps) = %d, want %d", len(temps), len(tc.wantJunction))
			}
			for id, want := range tc.wantJunction {
				if temps[id].Junction != want {
					t.Errorf("GPU %d junction = %d, want %d", id, temps[id].Junction, want)
				}
			}
			for id, want := range tc.wantVRAM {
				if temps[id].VRAM != want {
					t.Errorf("GPU %d vram = %d, want %d", id, temps[id].VRAM, want)
				}
			}
		})
	}
}

func TestApplyExtraTemps(t *testing.T) {
	info := &NvidiaInfo{GPUs: []GPUInfo{{ID: 0}, {ID: 1}}}
	temps := map[int]extraTemps{0: {Junction: 67, VRAM: 64}}

	applyExtraTemps(info, temps)

	if info.GPUs[0].JunctionTemp != 67 || info.GPUs[0].VRAMTemp != 64 {
		t.Errorf("GPU 0 temps = %d/%d, want 67/64", info.GPUs[0].JunctionTemp, info.GPUs[0].VRAMTemp)
	}
	if info.GPUs[1].JunctionTemp != 0 || info.GPUs[1].VRAMTemp != 0 {
		t.Errorf("GPU 1 temps = %d/%d, want 0/0", info.GPUs[1].JunctionTemp, info.GPUs[1].VRAMTemp)
	}
}

func TestExpandHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("no home directory: %v", err)
	}

	cases := []struct {
		name string
		path string
		want string
	}{
		{"tilde path", "~/projects/cpp/gpu-mem-temp/gputemps", home + "/projects/cpp/gpu-mem-temp/gputemps"},
		{"tilde only", "~", home},
		{"absolute path", "/usr/bin/nvidia-smi", "/usr/bin/nvidia-smi"},
		{"relative path", "tools/gputemps", "tools/gputemps"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := expandHome(tc.path)
			if err != nil {
				t.Fatalf("expandHome(%q) returned error: %v", tc.path, err)
			}
			if got != tc.want {
				t.Errorf("expandHome(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

func TestRunGputempsMissingBinary(t *testing.T) {
	if _, err := runGputemps("/nonexistent/path/gputemps"); err == nil {
		t.Error("expected error for missing gputemps binary")
	}
}

func TestMergeGputempsSurvivesMissingBinary(t *testing.T) {
	original := gputempsPath
	defer func() { gputempsPath = original }()

	gputempsPath = "/nonexistent/path/gputemps"
	info := &NvidiaInfo{GPUs: []GPUInfo{{ID: 0, Name: "NVIDIA GeForce RTX 3090", PerfState: "P0"}}}

	mergeGputemps(info)

	if info.GPUs[0].JunctionTemp != 0 || info.GPUs[0].VRAMTemp != 0 {
		t.Errorf("expected untouched temps, got %d/%d", info.GPUs[0].JunctionTemp, info.GPUs[0].VRAMTemp)
	}
}

func TestMergeGputempsWithRealTool(t *testing.T) {
	expanded, err := expandHome(gputempsPath)
	if err != nil {
		t.Fatalf("expandHome(%q) returned error: %v", gputempsPath, err)
	}
	if _, err := runGputemps(expanded); err != nil {
		t.Skipf("gputemps not usable on this host: %v", err)
	}

	info := &NvidiaInfo{GPUs: []GPUInfo{{ID: 0, Name: "NVIDIA GeForce RTX 3090", PerfState: "P0"}}}
	mergeGputemps(info)

	if info.GPUs[0].JunctionTemp <= 0 || info.GPUs[0].VRAMTemp <= 0 {
		t.Errorf("expected positive temps, got %d/%d", info.GPUs[0].JunctionTemp, info.GPUs[0].VRAMTemp)
	}
}

func TestFetchOnRealHost(t *testing.T) {
	if !Available() {
		t.Skip("nvidia-smi not present on this host")
	}

	info, err := Fetch(t.Context())
	if err != nil {
		t.Fatalf("Fetch on host with nvidia-smi failed: %v", err)
	}
	if info.GPUCount() == 0 {
		t.Fatal("Fetch returned no GPUs on host with nvidia-smi")
	}
	if Format(info) == "" {
		t.Fatal("Format returned empty block for real GPU data")
	}
	t.Logf("GPU block:\n%s", Format(info))
}
