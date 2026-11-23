//go:build integration

package bench

import (
	"path/filepath"
	"strings"
	"testing"
)

func envValue(env []string, key string) string {
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, key+"="); ok {
			return v
		}
	}
	return ""
}

// A workload that pushes the images its lane will run needs the platform script that registers them in the host's
// pre-pull list: the lane's env names it, the vcluster script for a virtual lane (the list is the host cluster's) and
// the cluster script for a lane that is its own cluster. A lane of another provider has none.
func TestLaneEnvNamesThePrepullScriptOfItsProvider(t *testing.T) {
	platform := t.TempDir()
	for _, c := range []struct {
		name    string
		step    Step
		want    string
		present bool
	}{
		{"virtual", Step{Provider: ProviderEks, Virtual: true, Platform: platform}, "eks-vcluster.sh", true},
		{"cluster", Step{Provider: ProviderEks, Platform: platform}, "eks-lane.sh", true},
		{"kind", Step{Platform: platform}, "", false},
	} {
		r := &stepRunner{s: c.step}
		got := envValue(r.laneEnv(nil), "BENCH_PREPULL")
		if !c.present {
			if got != "" {
				t.Errorf("%s: BENCH_PREPULL = %q, want unset", c.name, got)
			}
			continue
		}
		if want := filepath.Join(platform, "deploy", c.want); got != want {
			t.Errorf("%s: BENCH_PREPULL = %q, want %q", c.name, got, want)
		}
	}
}
