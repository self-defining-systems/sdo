package operatorcli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// The warm build takes the controller's own arguments (it is the same compile under the same settings), so what the
// controller would refuse to start on it refuses too, before any compile.
func TestWarmBuildTakesTheControllersArguments(t *testing.T) {
	for _, args := range [][]string{nil, {"--app", "/workspace/application"}, {"controller"}} {
		var out bytes.Buffer
		err := RunWarmBuild(context.Background(), args, &out)
		if err == nil || !strings.Contains(err.Error(), "controller") && !strings.Contains(err.Error(), "native launch requires") {
			t.Fatalf("%v: %v", args, err)
		}
		if out.Len() != 0 {
			t.Fatalf("%v wrote %q", args, out.String())
		}
	}
}
