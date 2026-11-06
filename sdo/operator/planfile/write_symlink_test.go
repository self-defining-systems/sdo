package planfile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/safefs"
)

// An agent controls its checkout, so a symlink at or above plan.yaml must make
// Write fail instead of writing outside the checkout.
func TestWriteRefusesSymlinksInTheCheckout(t *testing.T) {
	plan, err := Parse([]byte(goodPlan), catalogs(t))
	if err != nil {
		t.Fatal(err)
	}
	shapes := map[string]func(checkout, outside string){
		"plan.yaml itself": func(checkout, outside string) {
			os.MkdirAll(filepath.Join(checkout, ".sdo/deployment"), 0o755)
			os.Symlink(filepath.Join(outside, "victim"), filepath.Join(checkout, ".sdo/deployment/plan.yaml"))
		},
		".sdo": func(checkout, outside string) {
			os.Symlink(outside, filepath.Join(checkout, ".sdo"))
		},
		".sdo/deployment": func(checkout, outside string) {
			os.MkdirAll(filepath.Join(checkout, ".sdo"), 0o755)
			os.Symlink(outside, filepath.Join(checkout, ".sdo/deployment"))
		},
	}
	for name, plant := range shapes {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			checkout, outside := filepath.Join(base, "checkout"), filepath.Join(base, "outside")
			os.MkdirAll(checkout, 0o755)
			os.MkdirAll(outside, 0o755)
			victim := filepath.Join(outside, "victim")
			os.WriteFile(victim, []byte("precious"), 0o644)
			plant(checkout, outside)
			if err := Write(checkout, plan); !errors.Is(err, safefs.ErrSymlink) {
				t.Fatalf("Write error = %v, want a symlink refusal", err)
			}
			if data, _ := os.ReadFile(victim); string(data) != "precious" {
				t.Fatalf("victim overwritten: %q", data)
			}
			entries, _ := os.ReadDir(outside)
			if len(entries) != 1 {
				t.Fatalf("files created outside the checkout: %v", entries)
			}
		})
	}
}
