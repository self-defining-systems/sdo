package install

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The benchmark's pinned-tools lane check reads these rendered controller Jobs
// (tests/unit/benchmarks/sregym/fastloop/test_pinned_tools.py) to decide whether
// the broker can reuse attestations. Regenerate with SDO_UPDATE_GOLDEN=1.
func TestControllerJobAttestationGoldens(t *testing.T) {
	controller := "registry.test/sdo/images@sha256:" + strings.Repeat("a", 64)
	for _, tc := range []struct {
		name, controller, devMount string
	}{
		{"pinned.json", controller, ""},
		{"tagged.json", "sdo-controller:tag", ""},
		{"dev_mount.json", controller, "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SDO_DEV_MOUNT", tc.devMount)
			cfg := testConfig()
			cfg.ControllerImage = tc.controller
			got, err := json.MarshalIndent(controllerJob(cfg), "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			path := filepath.Join("testdata", "controller_job", tc.name)
			if os.Getenv("SDO_UPDATE_GOLDEN") == "1" {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v (generate with SDO_UPDATE_GOLDEN=1)", err)
			}
			if string(want) != string(got) {
				t.Fatalf("rendered controller Job changed; review and regenerate %s with SDO_UPDATE_GOLDEN=1", path)
			}
		})
	}
}
