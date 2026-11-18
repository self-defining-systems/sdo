package operatorcli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestWarmMemoryBuildRequiresAnAbsoluteRepository(t *testing.T) {
	for _, args := range [][]string{nil, {"--repository", "relative/path"}, {"--repository", "/abs", "extra"}} {
		var out bytes.Buffer
		err := RunWarmMemoryBuild(context.Background(), args, &out)
		if err == nil || !strings.Contains(err.Error(), "--repository") {
			t.Fatalf("%v: %v", args, err)
		}
		if out.Len() != 0 {
			t.Fatalf("%v wrote %q", args, out.String())
		}
	}
}
