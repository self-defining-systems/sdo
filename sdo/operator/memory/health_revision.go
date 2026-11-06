package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"sdo.dev/operator/memory/internal/memoryio"
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
	out, err := memoryio.Git(ctx, repository, false, nil, args...)
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
		if !layout.IsMemoryPath(name) {
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

// ShapeOf returns the desired-state shape digest of repository at revision
// (rendered.Source.ShapeOf). It is injected so memory does not import the
// renderer stack.
type ShapeOf func(ctx context.Context, repository, revision string) (string, error)

// AcceptedHealthRevision reads the accepted HEAD's health inputs. NeedsRefresh
// (a judge rerun) is set when the human objective changed since the last
// health_judge commit or, with shapeOf, when the desired-state shape changed;
// a source byte change that leaves the shape alone does not. SourceInputs
// still reports the bytes: they bind detector adoption (the runtime guard),
// not the judge. A nil shapeOf is the transitional bytes fallback for installs
// without a DeploymentSource (removed with S3-11c). A shapeOf error is
// returned: without a shape the need for a refresh is unknown.
func AcceptedHealthRevision(ctx context.Context, repository string, shapeOf ShapeOf) (HealthRevision, error) {
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
		sourceChanged := current.SourceInputs != judge.SourceInputs
		if shapeOf != nil && sourceChanged {
			sourceChanged, err = shapeChanged(ctx, shapeOf, repository, current.Commit, judge.Commit)
			if err != nil {
				return HealthRevision{}, err
			}
		}
		current.NeedsRefresh = sourceChanged || current.Objective != judge.Objective
	}
	return current, nil
}

func shapeChanged(ctx context.Context, shapeOf ShapeOf, repository, accepted, judged string) (bool, error) {
	was, err := shapeOf(ctx, repository, judged)
	if err != nil {
		return false, fmt.Errorf("desired-state shape of judged revision %s: %w", judged, err)
	}
	now, err := shapeOf(ctx, repository, accepted)
	if err != nil {
		return false, fmt.Errorf("desired-state shape of accepted revision %s: %w", accepted, err)
	}
	return was != now, nil
}

// HealthRefreshExitCode is the builder/runtime handoff for a required rebuild.
// It is distinct from normal completion and from failed controller execution.
const HealthRefreshExitCode = 76

var ErrHealthRefreshRequired = errors.New("health unverified: accepted inputs require judge refresh and detector adoption")
