package validate

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// CheckRepairAdoption permits the health policy to differ from the revision a
// verified repair was committed on only through broker-validated judge
// refreshes that follow that repair. The repair must be an ancestor of current,
// so the refreshes are bound to it by lineage, not by timing. Judge trailers
// are text anyone with write access to the branch can type, so every commit
// that carries them must also have touched only paths the ownership table
// grants the health judge. Anything else that rewrote policy, or an objective
// change, returns memory.ErrReflectionPolicyChanged or
// memory.ErrReflectionObjectiveChanged.
func CheckRepairAdoption(ctx context.Context, repository, repair, current string) error {
	if _, err := git(ctx, repository, "merge-base", "--is-ancestor", repair, current); err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return memory.ErrReflectionPolicyChanged
		}
		return err
	}
	if err := memory.CheckReflectionHealth(ctx, repository, repair, current); err != nil {
		return err
	}
	commits, err := gitText(ctx, repository, "rev-list", "--first-parent", repair+".."+current)
	if err != nil {
		return err
	}
	for _, commit := range strings.Fields(commits) {
		message, err := git(ctx, repository, "show", "-s", "--format=%B", commit)
		if err != nil {
			return err
		}
		if !memory.IsHealthJudgeCommit(string(message)) {
			continue
		}
		changed, err := git(ctx, repository, "diff", "--name-only", "--no-renames", "-z", commit+"^", commit, "--")
		if err != nil {
			return err
		}
		for _, p := range strings.Split(string(changed), "\x00") {
			if p != "" && !judgeOwns(p) {
				return fmt.Errorf("%w: commit %s carries health-judge trailers but changed %s, which the judge does not own", memory.ErrReflectionPolicyChanged, commit, p)
			}
		}
	}
	return nil
}

// judgeOwns is the ownership table's answer for one path: the files the health
// judge may change in the phase its refreshes are accepted in.
func judgeOwns(p string) bool {
	kind, id := kindSource, ""
	if layout.IsMemoryPath(p) {
		var ok bool
		if kind, id, ok = layout.Classify(p); !ok {
			return false
		}
	}
	return allowed(HealthJudge, InitialPhase, kind, id)
}
