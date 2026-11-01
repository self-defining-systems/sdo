package validate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sdo.dev/operator/memory/internal/memoryio"
	"strconv"
	"strings"
)

var whitespaceLocation = regexp.MustCompile(`^(.+):(\d+): (.+)$`)

func checkWhitespace(ctx context.Context, r Request) ([]Diagnostic, error) {
	dir, err := os.MkdirTemp("", "sdo-diff-index-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	run := func(args ...string) ([]byte, error) {
		return memoryio.Git(ctx, r.Root, true, []string{"GIT_INDEX_FILE=" + filepath.Join(dir, "index")}, args...)
	}
	for _, args := range [][]string{{"read-tree", r.Base}, {"add", "-A", "--", "."}} {
		if out, err := run(args...); err != nil {
			return nil, fmt.Errorf("prepare diff check: %w: %s", err, out)
		}
	}
	out, err := run("-c", "core.whitespace=blank-at-eol,blank-at-eof,space-before-tab", "diff", "--cached", "--check", r.Base, "--")
	if err == nil {
		return nil, nil
	}
	if !memoryio.Exited(err) || ctx.Err() != nil {
		return nil, fmt.Errorf("diff check: %w", err)
	}
	var ds []Diagnostic
	for _, line := range strings.Split(string(out), "\n") {
		if m := whitespaceLocation.FindStringSubmatch(line); m != nil {
			n, _ := strconv.Atoi(m[2])
			ds = append(ds, Diagnostic{Rule: RuleWhitespace, Path: m[1], Line: n, Message: m[3], Hint: "remove whitespace errors from added lines"})
		}
	}
	if len(ds) == 0 {
		return nil, fmt.Errorf("diff check failed: %w: %s", err, out)
	}
	return ds, nil
}
