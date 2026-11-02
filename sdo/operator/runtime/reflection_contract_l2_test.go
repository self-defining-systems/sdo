package runtime

import (
	"path/filepath"
	"testing"

	"sdo.dev/operator/sdk/program"
)

func t1OneGlob(t *testing.T, pattern string) string {
	t.Helper()
	paths, err := filepath.Glob(pattern)
	if err != nil || len(paths) != 1 {
		t.Fatalf("expected one %s: %v %v", pattern, paths, err)
	}
	return paths[0]
}

type t1ReplayInstance struct {
	ID       string           `json:"id"`
	Bindings program.Bindings `json:"bindings"`
	Verdict  program.Result   `json:"verdict"`
}

