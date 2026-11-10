//go:build integration

package storestate_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/storestate"
)

// redisKeysSection runs the redis read against a stub redis-cli whose EVAL
// blocks (a write pause holds scripts that are not declared read-only) and
// whose EVAL_RO answers, or is unknown, as the case says. It returns the read's
// output.
func redisKeysSection(t *testing.T, eval, evalRO string) string {
	t.Helper()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	dir := t.TempDir()
	stub := `#!/bin/sh
shift 2
case "$1" in
EVAL) ` + eval + `;;
EVAL_RO) ` + evalRO + `;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "redis-cli"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	a, ok := storestate.AdapterFor(storestate.EngineRedis)
	if !ok {
		t.Fatal("no redis adapter")
	}
	argv := a.ReadCommand(storestate.Endpoint{Port: 6379})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, sh, argv[1:]...)
	cmd.Env = []string{"PATH=" + dir + ":/usr/bin:/bin"}
	cmd.WaitDelay = time.Second
	out, _ := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("the read blocked in a write pause: %s", out)
	}
	return string(out)
}

// A store under CLIENT PAUSE WRITE still answers read-only scripts, so the
// key-pattern read declares its script read-only.
func TestRedisKeyPatternsAreReadWithAReadOnlyScript(t *testing.T) {
	out := redisKeysSection(t, `sleep 30`, `printf 'complete\nuser:{n}\t3\n'`)
	if !strings.Contains(out, "== keys\ncomplete\nuser:{n}\t3\n") {
		t.Fatalf("keys section missing:\n%s", out)
	}
}

// Redis before 7 has no EVAL_RO: the read falls back to EVAL.
func TestRedisKeyPatternsFallBackWhereEvalROIsUnknown(t *testing.T) {
	out := redisKeysSection(t, `printf 'complete\nuser:{n}\t3\n'`, `echo "ERR unknown command 'EVAL_RO', with args beginning with:"`)
	if !strings.Contains(out, "== keys\ncomplete\nuser:{n}\t3\n") {
		t.Fatalf("no fallback to EVAL:\n%s", out)
	}
}
