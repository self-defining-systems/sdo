package sim

import (
	"context"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// CompileCall is one invocation of the simulated independent compile gate.
type CompileCall struct {
	Stage   string
	Changed []string
}

// Compiler simulates the memory broker's independent compile gate
// (func(ctx, stage, changed) error) without a Go toolchain process. A call is
// recorded, honors cancellation, rejects changed Go files that do not parse,
// then asks Verdict. It does not type-check, link or run tests; callers whose
// subject is compilation keep a real compile.
type Compiler struct {
	// Verdict optionally injects a typed rejection for a call.
	Verdict func(CompileCall) error
	mu      sync.Mutex
	calls   []CompileCall
}

// Compile implements the broker compile hook.
func (c *Compiler) Compile(ctx context.Context, stage string, changed []string) error {
	call := CompileCall{Stage: stage, Changed: append([]string(nil), changed...)}
	c.mu.Lock()
	c.calls = append(c.calls, call)
	verdict := c.Verdict
	c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	fset := token.NewFileSet()
	for _, path := range changed {
		if !strings.HasSuffix(path, ".go") {
			continue
		}
		full := filepath.Join(stage, path)
		if _, err := os.Stat(full); os.IsNotExist(err) {
			continue
		}
		if _, err := parser.ParseFile(fset, full, nil, parser.SkipObjectResolution); err != nil {
			return fmt.Errorf("simulated compile: %s: %w", path, err)
		}
	}
	if verdict != nil {
		return verdict(call)
	}
	return nil
}

// Calls returns the recorded invocations in order.
func (c *Compiler) Calls() []CompileCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]CompileCall(nil), c.calls...)
}
