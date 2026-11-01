//go:build integration

package broker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/broker/internal/processio"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestValidationSubprocessLimits(t *testing.T) {
	for _, tc := range []struct {
		name, script, diagnostic string
		timeout                  time.Duration
		memory                   int64
	}{
		{"timeout", "sleep 30 & wait", "timeout", 100 * time.Millisecond, 1 << 30},
		{"memory", "python3 -c 'import time; x=bytearray(100*1024*1024); time.sleep(30)' & wait", "memory limit", 5 * time.Second, 32 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := processio.RunValidationProcess(context.Background(), "/bin/sh", []string{"-c", tc.script}, t.TempDir(), tc.timeout, tc.memory)
			if err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("expected infrastructure %s: %v", tc.diagnostic, err)
			}
			if failure, ok := err.(*ValidationInfrastructureError); !ok || failure.ExitCode() != 2 {
				t.Fatalf("expected infrastructure exit 2: %T %v", err, err)
			}
		})
	}
}

func TestValidationTimeoutKillsDescendants(t *testing.T) {
	scratch := t.TempDir()
	pidFile := filepath.Join(scratch, "descendant")
	err := processio.RunValidationProcess(context.Background(), "/bin/sh", []string{"-c", `sleep 30 & echo $! > "$1"; wait`, "validation", pidFile}, scratch, 200*time.Millisecond, 1<<30)
	if err == nil {
		t.Fatal("timeout accepted")
	}
	data, readErr := os.ReadFile(pidFile)
	if readErr != nil {
		t.Fatal(readErr)
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	deadline := time.Now().Add(time.Second)
	for {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if os.IsNotExist(err) || (err == nil && strings.Contains(string(data), ") Z ")) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant %d survived validation timeout: %s", pid, data)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
