package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// HealthRevision binds health policy to application inputs, the human objective,
// and health detector bytes. Incident memory and rendered locks do not affect it.
// Provenance comes from broker-validated judge commits, never an agent assertion.
type HealthRevision struct {
	Commit, JudgeCommit, SourceInputs, Objective, DetectorRevision string
	NeedsRefresh                                                   bool
}

func (r HealthRevision) RefreshID() string {
	digest := sha256.Sum256([]byte(r.SourceInputs + "\x00" + r.Objective + "\x00" + r.DetectorRevision))
	return fmt.Sprintf("health-refresh-%x", digest)
}

func healthGit(ctx context.Context, repository string, args ...string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, "git", append([]string{"-C", repository}, args...)...).Output()
	if err != nil {
		return nil, fmt.Errorf("read health provenance (%s): %w", strings.Join(args, " "), err)
	}
	return out, nil
}

// HealthInputs reads one immutable revision. ls-tree includes modes, symlinks,
// and submodule IDs: builds can consume any tracked input outside memory.
func HealthInputs(ctx context.Context, repository, revision string) (HealthRevision, error) {
	commit, err := healthGit(ctx, repository, "rev-parse", "--verify", revision+"^{commit}")
	if err != nil {
		return HealthRevision{}, err
	}
	revision = strings.TrimSpace(string(commit))
	entries, err := healthGit(ctx, repository, "ls-tree", "-r", "-z", "--full-tree", revision)
	if err != nil {
		return HealthRevision{}, err
	}
	source, detectors := sha256.New(), sha256.New()
	for _, entry := range bytes.Split(entries, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		_, path, ok := bytes.Cut(entry, []byte{'\t'})
		if !ok {
			return HealthRevision{}, errors.New("invalid health provenance tree")
		}
		name := string(path)
		if name != layout.Dir && !layout.IsMemoryPath(name) {
			source.Write(entry)
			source.Write([]byte{0})
		}
		if strings.HasPrefix(name, layout.DetectorsDir+"/health/") || strings.HasPrefix(name, layout.DiagnosticsDir+"/traffic/") || strings.HasPrefix(name, layout.LibrariesDir+"/") || name == layout.DiagnosticsGoModFile {
			detectors.Write(entry)
			detectors.Write([]byte{0})
		}
	}
	goal, err := healthGit(ctx, repository, "show", revision+":"+layout.GoalFile)
	if err != nil {
		return HealthRevision{}, err
	}
	parts := strings.SplitN(string(goal), "---\n", 3)
	if len(parts) != 3 || parts[0] != "" {
		return HealthRevision{}, errors.New("accepted human objective is invalid")
	}
	objective := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(parts[2]), "# Health objective\n"))
	if objective == "" {
		return HealthRevision{}, errors.New("accepted human objective is empty")
	}
	return HealthRevision{Commit: revision, SourceInputs: fmt.Sprintf("%x", source.Sum(nil)), Objective: objective, DetectorRevision: fmt.Sprintf("%x", detectors.Sum(nil))}, nil
}

func AcceptedHealthRevision(ctx context.Context, repository string) (HealthRevision, error) {
	current, err := HealthInputs(ctx, repository, "HEAD")
	if err != nil {
		return HealthRevision{}, err
	}
	current.NeedsRefresh = true
	judgeCommit, err := healthGit(ctx, repository, "log", "--first-parent", "-1", "--format=%H", "--all-match", "--grep=^SDO-Actor: health_judge$", "--grep=^SDO-Phase: initial$", "--grep=^SDO-Validation: passed$", current.Commit)
	if err != nil {
		return HealthRevision{}, err
	}
	current.JudgeCommit = strings.TrimSpace(string(judgeCommit))
	if current.JudgeCommit != "" {
		judge, err := HealthInputs(ctx, repository, current.JudgeCommit)
		if err != nil {
			return HealthRevision{}, err
		}
		// Detector identity binds refresh work and binary adoption; memory-only
		// detector/library publication does not itself schedule another judge.
		current.NeedsRefresh = current.SourceInputs != judge.SourceInputs || current.Objective != judge.Objective
	}
	return current, nil
}

// HealthRefreshExitCode is the builder/runtime handoff for a required rebuild.
// It is distinct from normal completion and from failed controller execution.
const HealthRefreshExitCode = 76

var ErrHealthRefreshRequired = errors.New("health unverified: accepted inputs require judge refresh and detector adoption")

var ErrReflectionObjectiveChanged = errors.New("objective changed")
var ErrReflectionPolicyChanged = errors.New("policy changed")

// CheckReflectionHealth permits source adoption and broker-validated judge
// refreshes, while retaining the frozen incident's objective and policy authority.
func CheckReflectionHealth(ctx context.Context, repository, frozen, current string) error {
	protected := []string{layout.GoalFile, layout.DetectorsDir + "/health", layout.DiagnosticsDir + "/traffic", layout.LibrariesDir, layout.DiagnosticsGoModFile}
	args := append([]string{"diff", "--name-only", "--no-renames", frozen, current, "--"}, protected...)
	changes, err := healthGit(ctx, repository, args...)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(changes)) == 0 {
		return nil
	}
	before, err := HealthInputs(ctx, repository, frozen)
	if err != nil {
		return err
	}
	after, err := HealthInputs(ctx, repository, current)
	if err != nil {
		return err
	}
	if before.Objective != after.Objective {
		return ErrReflectionObjectiveChanged
	}
	if before.DetectorRevision == after.DetectorRevision {
		return nil
	}
	// A source-driven judge refresh may legitimately replace its health
	// detectors. Every intervening policy writer must have broker provenance;
	// a later judge refresh cannot launder an unrelated policy/library edit.
	data, err := healthGit(ctx, repository, "log", "--first-parent", "--format=%B%x00", frozen+".."+after.Commit, "--", layout.DetectorsDir+"/health", layout.DiagnosticsDir+"/traffic", layout.LibrariesDir, layout.DiagnosticsGoModFile)
	if err != nil {
		return err
	}
	seen := false
	for _, entry := range bytes.Split(data, []byte{0}) {
		if len(bytes.TrimSpace(entry)) == 0 {
			continue
		}
		seen = true
		message := "\n" + strings.TrimSpace(string(entry)) + "\n"
		for _, trailer := range []string{"SDO-Actor: health_judge", "SDO-Phase: initial", "SDO-Validation: passed"} {
			if !strings.Contains(message, "\n"+trailer+"\n") {
				return ErrReflectionPolicyChanged
			}
		}
	}
	if !seen {
		return ErrReflectionPolicyChanged
	}
	return nil
}
