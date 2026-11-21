package bench

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// RenderedChecks holds the answers one validation session remembers: the pin check's per host and pin, the memory
// check's per directory and pinned SDO checkout. A session is safe for concurrent use and answers each probe once.
type RenderedChecks struct {
	Log  io.Writer
	pins *memoMap[string]
	mem  *memoMap[inputResult]
	desc *memoMap[*Described]
	foot *memoMap[*LaneLimit]
}

// NewRenderedChecks starts a validation session; log says what a check could not ask (nil is silent).
func NewRenderedChecks(log io.Writer) *RenderedChecks {
	return &RenderedChecks{Log: log, pins: newMemoMap[string](), mem: newMemoMap[inputResult](), desc: newMemoMap[*Described](), foot: newMemoMap[*LaneLimit]()}
}

// ValidateRendered is the one place that decides whether a rendered spec is acceptable. `plan add` and `--dry-run` call
// it for every node of every arm, and a submit (`Add`) calls it again: a rule is either here or listed in submitOnly
// (submitonly_test.go) with the reason only a submit can run it. A field an artifact edge provides (operator.seed)
// holds a placeholder path (plan.PlaceholderPrefix) at add time: rules about combinations of fields still fire; the
// file itself is not looked at.
func ValidateRendered(ctx context.Context, inv Inventory, s Spec, c *RenderedChecks) error {
	if err := s.Validate(); err != nil {
		return err
	}
	described, err := c.describe(ctx, inv, s)
	if err != nil {
		return err
	}
	s.Described = described
	fp, err := c.footprint(ctx, inv, s)
	if err != nil {
		return err
	}
	if fp != nil {
		s.Lane = fp
	}
	if err := checkInputRules(s); err != nil {
		return err
	}
	if len(inv.UsableHosts(s)) == 0 {
		return fmt.Errorf("no host of %v can take this spec: %s", s.Hosts, strings.Join(inv.UnusableReasons(s), "; "))
	}
	if problems := inv.SelectorProblems(s); len(problems) > 0 {
		return fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	if problems := inv.ClusterSourceProblems(ctx, s, addHostRunner(inv)); len(problems) > 0 {
		return fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	if problems := inv.ToolProblems(ctx, s, addHostRunner(inv)); len(problems) > 0 {
		return fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	if err := checkLaneCapacity(inv, s); err != nil {
		return err
	}
	if err := checkPoolCapacity(inv, s); err != nil {
		return err
	}
	if s.Operator.Hooks != "" && inv.HookCommand[s.Operator.Hooks] == "" {
		return fmt.Errorf("hosts.yaml has no hook_command for %s", s.Operator.Hooks)
	}
	if err := checkSpecInputs(s); err != nil {
		return err
	}
	if err := checkPins(ctx, inv, s, c.Log, c.pins); err != nil {
		return err
	}
	if local := inv.LocalHost(); local != "" {
		if fz := inv.FuzzopsPath(s.Fuzzops, local); fz != "" {
			if st, err := os.Stat(filepath.Join(fz, s.App)); err != nil || !st.IsDir() {
				return fmt.Errorf("app %s: not a pack directory in %s", s.App, fz)
			}
		}
	}
	return checkSpecSubjectInputs(ctx, inv, s, c)
}

func isPlaceholder(p string) bool { return strings.HasPrefix(p, plan.PlaceholderPrefix) }

// checkSpecInputs checks the files a spec names: the instances file exists and holds every fault ID, the baseline is a
// directory and a seed that is no placeholder exists. (A placeholder is an artifact of an earlier arm: its content
// can only be checked at submit.)
func checkSpecInputs(s Spec) error {
	if f := s.Faults.Instances; f != "" && !isPlaceholder(f) {
		if err := checkIDs(f, s.Faults.IDs); err != nil {
			return fmt.Errorf("faults.instances %s: %w", f, err)
		}
	}
	if b := s.Operator.Baseline; b != "" && !isPlaceholder(b) {
		if st, err := os.Stat(b); err != nil || !st.IsDir() {
			return fmt.Errorf("operator.baseline %s: not a directory", b)
		}
	}
	if sd := s.Operator.Seed; sd != "" && !isPlaceholder(sd) {
		if _, err := os.Stat(sd); err != nil {
			return fmt.Errorf("operator.seed %s: %w", sd, err)
		}
	}
	return nil
}
