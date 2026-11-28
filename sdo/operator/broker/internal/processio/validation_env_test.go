package processio

import (
	"slices"
	"strings"
	"testing"
)

func gomaxprocs(env []string) []string {
	var values []string
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, "GOMAXPROCS="); ok {
			values = append(values, value)
		}
	}
	return values
}

// The validation child's Go builds run as many compiles at once as it has
// procs, and the whole group must stay inside the memory budget, so the
// child's parallelism follows the budget, not the host's core count.
func TestValidationChildParallelismFollowsItsMemoryBudget(t *testing.T) {
	const gib = int64(1) << 30
	for _, tc := range []struct {
		name   string
		parent []string
		memory int64
		cpus   int
		want   string
	}{
		{"big host, default budget", []string{"PATH=/bin"}, 3 * gib, 32, "4"},
		{"small host keeps its cores", nil, 3 * gib, 2, "2"},
		{"tiny budget still builds", nil, gib / 4, 32, "1"},
		{"a lower caller setting stays", []string{"GOMAXPROCS=2"}, 3 * gib, 32, "2"},
		{"a higher caller setting is capped", []string{"GOMAXPROCS=64"}, 3 * gib, 32, "4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := validationChildEnvironment(tc.parent, "/scratch", tc.memory, tc.cpus)
			if got := gomaxprocs(env); !slices.Equal(got, []string{tc.want}) {
				t.Fatalf("GOMAXPROCS entries = %v, want [%s]", got, tc.want)
			}
			if !slices.Contains(env, "TMPDIR=/scratch") {
				t.Fatalf("child environment lost TMPDIR: %v", env)
			}
		})
	}
}
