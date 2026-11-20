package bench

import (
	"os"
	"path/filepath"
	"strings"
)

// Where a results root was found (ResolveRoot).
const (
	SourceFlag    = "--bench"
	SourceEnv     = "$FUZZOPS_BENCH_ROOT"
	SourceManager = "the running fuzzops-bench manager"
)

// PointerPath is the well-known file where `fuzzops-bench serve` records the
// root it manages: $XDG_STATE_HOME/fuzzops/bench-root, else
// ~/.local/state/fuzzops/bench-root. "" when the host has neither variable.
func PointerPath(getenv func(string) string) string {
	if d := getenv("XDG_STATE_HOME"); d != "" {
		return filepath.Join(d, "fuzzops", "bench-root")
	}
	if h := getenv("HOME"); h != "" {
		return filepath.Join(h, ".local", "state", "fuzzops", "bench-root")
	}
	return ""
}

// RecordRoot writes root (absolute) to the pointer file, atomically.
func RecordRoot(pointer, root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(pointer), 0o755); err != nil {
		return err
	}
	tmp := pointer + ".tmp"
	if err := os.WriteFile(tmp, []byte(root+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, pointer)
}

// ResolveRoot picks the results root to read, in order: the flag, the
// environment value, the root the manager recorded at pointer. flag and env
// are returned as given (the caller reports a bad one); the manager's is used
// only while it is still a bench root, so a stale pointer is no error. It
// returns "", "" when none is found.
func ResolveRoot(flag, env, pointer string) (root, source string) {
	if flag != "" {
		return flag, SourceFlag
	}
	if env != "" {
		return env, SourceEnv
	}
	if pointer == "" {
		return "", ""
	}
	b, err := os.ReadFile(pointer)
	if err != nil {
		return "", ""
	}
	p := strings.TrimSpace(string(b))
	if !filepath.IsAbs(p) {
		return "", ""
	}
	if _, err := os.Stat(filepath.Join(p, "hosts.yaml")); err != nil {
		return "", ""
	}
	return p, SourceManager
}
