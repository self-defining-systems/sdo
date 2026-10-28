package validate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"sdo.dev/operator/memory/layout"
)

var whitespaceLocation = regexp.MustCompile(`^(.+):(\d+): (.+)$`)

// checkWhitespace judges only SDO-authored memory text. Application source
// and manifests are data the agent did not author (for example an embedded
// script in an accepted ConfigMap), so their bytes stay exactly as given.
func checkWhitespace(ctx context.Context, r Request) ([]Diagnostic, error) {
	dir, err := os.MkdirTemp("", "sdo-diff-index-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	run := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", r.Root}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_INDEX_FILE="+filepath.Join(dir, "index"))
		return cmd.CombinedOutput()
	}
	for _, args := range [][]string{{"read-tree", r.Base}, {"add", "-A", "--", "."}} {
		if out, err := run(args...); err != nil {
			return nil, fmt.Errorf("prepare diff check: %w: %s", err, out)
		}
	}
	out, err := run("-c", "core.whitespace=blank-at-eol,blank-at-eof,space-before-tab", "diff", "--cached", "--check", r.Base, "--", layout.Dir)
	if err == nil {
		return nil, nil
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) || ctx.Err() != nil {
		return nil, fmt.Errorf("diff check: %w", err)
	}
	var ds []Diagnostic
	for _, line := range strings.Split(string(out), "\n") {
		if m := whitespaceLocation.FindStringSubmatch(line); m != nil {
			n, _ := strconv.Atoi(m[2])
			hint := "remove whitespace errors from added lines"
			if strings.Contains(m[3], "conflict marker") {
				hint = "resolve the conflict: merge both sides' intent and delete the markers"
			}
			ds = append(ds, Diagnostic{Rule: RuleWhitespace, Path: m[1], Line: n, Message: m[3], Hint: hint})
		}
	}
	if len(ds) == 0 {
		return nil, fmt.Errorf("diff check failed: %w: %s", err, out)
	}
	return ds, nil
}
