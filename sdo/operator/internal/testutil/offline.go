// Package testutil prepares dependencies for tests of the offline compiler.
package testutil

import (
	"context"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var moduleCache struct {
	sync.Once
	err    error
	output []byte
}

// OfflineGo mirrors the image build's cache preparation before disabling the
// proxy. A normal go test downloads only the selected graph; go mod tidy in a
// generated workspace also needs the transitive modules' older go.mod files.
func OfflineGo(t *testing.T, operatorRoot string) {
	t.Helper()
	moduleCache.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, module := range []string{"sdk", "core", "runtime", "."} {
			command := exec.CommandContext(ctx, "go", "mod", "download", "all")
			command.Dir = filepath.Join(operatorRoot, module)
			moduleCache.output, moduleCache.err = command.CombinedOutput()
			if moduleCache.err != nil {
				return
			}
		}
	})
	if moduleCache.err != nil {
		t.Fatalf("prepare offline Go module cache: %v: %s", moduleCache.err, moduleCache.output)
	}
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOSUMDB", "off")
}
