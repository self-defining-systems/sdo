//go:build integration

package deploy_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Under `set -o pipefail`, `producer | grep -q x` fails when grep finds x early: grep exits, the producer gets
// SIGPIPE (exit 141), and the pipeline's status is 141, so the match reads as "no match" (a lane cluster that exists
// is taken for missing). A shell script with pipefail writes `| grep x >/dev/null`, which reads all its input.
//ci:scoped walks only this module's own shell scripts
func TestNoEarlyExitGrepUnderPipefail(t *testing.T) {
	early := regexp.MustCompile(`\|\s*grep\s+-[a-zA-Z]*[qm]`)
	var bad []string
	err := filepath.WalkDir("..", func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "node_modules") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(p, ".sh") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil || !strings.Contains(string(b), "pipefail") {
			return err
		}
		for i, line := range strings.Split(string(b), "\n") {
			if early.MatchString(line) {
				bad = append(bad, fmt.Sprintf("%s:%d: %s", p, i+1, strings.TrimSpace(line)))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) > 0 {
		t.Fatalf("early-exit grep in a pipeline under pipefail (write `| grep PATTERN >/dev/null`):\n%s", strings.Join(bad, "\n"))
	}
}
