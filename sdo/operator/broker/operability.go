package broker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operability"
)

// ErrOperabilityUnbound means an operability report does not bind to what the
// broker recomputed (another commit, tree, patch series, render, environment or
// diagnostics tree), is not of the required stage, or is not accepted by
// operability.Decide. A report is never trusted: the broker recomputes the
// subject from its own repository and evidence and the decision from the
// checks.
var ErrOperabilityUnbound = errors.New("operability report does not bind to the accepted state")

// ErrSourceEvidencePredatesOperability marks accepted source evidence recorded
// without an operability report where one is required. It has no reader: the
// lane must be reinstalled.
var ErrSourceEvidencePredatesOperability = errors.New("deployment evidence carries no operability report (written by an older operator); persistent lanes must reinstall the operator (`sdo-operator install --fresh` / lane reset)")

// emptyDiagnosticsTree is the diagnostics tree of a commit that has no
// .sdo/diagnostics directory.
const emptyDiagnosticsTree = "empty"

// DiagnosticsTree identifies the diagnostics artifact class of a commit in
// repository: the Git tree object of .sdo/diagnostics (health detectors,
// traffic probes, telemetry bindings). It is the subject field that binds the
// detector evaluation of a complete-stage report to the commit holding those
// detectors.
func DiagnosticsTree(ctx context.Context, repository, commit string) (string, error) {
	out, err := runGit(ctx, repository, "ls-tree", "-d", commit, layout.DiagnosticsDir)
	if err != nil {
		return "", fmt.Errorf("read diagnostics tree of %s: %w", commit, err)
	}
	if len(out) == 0 {
		return emptyDiagnosticsTree, nil
	}
	return gitText(ctx, repository, "rev-parse", commit+":"+layout.DiagnosticsDir)
}

func operabilityTrailer(stage operability.Stage, digest string) string {
	return fmt.Sprintf("SDO-Operability: %s sha256:%s", stage, digest)
}

// checkOperability requires report to be valid, of the stage, bound to want
// and accepted by Decide for the stage's items, and returns its digest.
func checkOperability(report operability.Report, stage operability.Stage, want operability.Subject) (string, error) {
	digest, err := report.Digest()
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrOperabilityUnbound, err)
	}
	if report.Stage != stage {
		return "", fmt.Errorf("%w: report is stage %q, want %q", ErrOperabilityUnbound, report.Stage, stage)
	}
	if err := report.Subject.BindsTo(want); err != nil {
		return "", fmt.Errorf("%w: %w", ErrOperabilityUnbound, err)
	}
	if decision := report.Decide(operability.StageItems(stage)...); decision.Kind != operability.Accepted {
		return "", fmt.Errorf("%w: %s stage decision is %s (owner %s):\n%s", ErrOperabilityUnbound, stage, decision.Kind, decision.Owner, decision.Feedback())
	}
	return digest, nil
}

// deploymentReportDigest checks a stage-deployment report against the
// evidence: the verified deployment's trees and digests. The report's
// AcceptedCommit, the proposal's base commit, is compared at publication
// (requireDeploymentSubject) when the proposal is known.
func deploymentReportDigest(report operability.Report, evidence SourceEvidence) (string, error) {
	want := operability.Subject{
		AcceptedCommit: report.Subject.AcceptedCommit, BaseTree: evidence.BaseTree, EffectiveTree: evidence.EffectiveTree,
		PatchesDigest: evidence.PatchesDigest, RenderDigest: evidence.RenderDigest, EnvDigest: evidence.EnvDigest,
	}
	return checkOperability(report, operability.StageDeployment, want)
}

// requireDeploymentSubject binds a stage-deployment report to the commit the
// proposal was based on: the accepted source commit does not exist yet, so the
// report names the head the source was deployed from.
func requireDeploymentSubject(report operability.Report, baseCommit string) error {
	if report.Subject.AcceptedCommit != baseCommit {
		return fmt.Errorf("%w: accepted_commit %q, the proposal is based on %s", ErrOperabilityUnbound, report.Subject.AcceptedCommit, baseCommit)
	}
	return nil
}

func (b *Broker) operabilityPath(key string, stage operability.Stage) string {
	digest := sha256.Sum256([]byte(key))
	return filepath.Join(b.workspaceRoot, "source-evidence", fmt.Sprintf("%x.operability-%s.json", digest, stage))
}

// writeOperabilityReport durably records the report next to the evidence
// records. Call while holding the broker lock.
func (b *Broker) writeOperabilityReport(key string, stage operability.Stage, report operability.Report) error {
	data, err := report.Encode()
	if err != nil {
		return err
	}
	return writeDurable(b.operabilityPath(key, stage), data)
}

// OperabilityReport returns the operability report of the stage that accepted
// key's commit (key is the operation ID of the source commit for stage
// deployment, the judge's operation ID for stage complete). found is false when
// the key has no accepted commit. The report is verified again at every read:
// its digest is the evidence's (deployment), its subject names the accepted
// commit and the diagnostics tree there (complete), and Decide accepts it.
func (b *Broker) OperabilityReport(ctx context.Context, key string, stage operability.Stage) (operability.Report, bool, error) {
	var report operability.Report
	var found bool
	err := b.locked(func() error {
		receipt, accepted, err := b.findAccepted(ctx, key, InitialPhase)
		if err != nil || !accepted {
			return err
		}
		data, err := os.ReadFile(b.operabilityPath(key, stage))
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("accepted commit %s of %s has no %s operability report: %w", receipt.Commit, key, stage, ErrSourceEvidencePredatesOperability)
		}
		if err != nil {
			return err
		}
		if report, err = operability.Decode(data); err != nil {
			return err
		}
		switch stage {
		case operability.StageDeployment:
			evidence, evidenceFound, err := b.sourceEvidenceLocked(ctx, key, receipt)
			if err != nil {
				return err
			}
			if !evidenceFound {
				return errors.New("accepted source commit has no trusted deployment evidence")
			}
			if evidence.OperabilityDigest == "" {
				return fmt.Errorf("source evidence of %s names no operability report: %w", key, ErrSourceEvidencePredatesOperability)
			}
			digest, err := deploymentReportDigest(report, evidence)
			if err != nil {
				return err
			}
			if digest != evidence.OperabilityDigest {
				return fmt.Errorf("%w: stored report %s, evidence names %s", ErrOperabilityUnbound, digest, evidence.OperabilityDigest)
			}
		case operability.StageComplete:
			tree, err := DiagnosticsTree(ctx, b.target, receipt.Commit)
			if err != nil {
				return err
			}
			want := report.Subject
			want.AcceptedCommit, want.DiagnosticsTree = receipt.Commit, tree
			if _, err := checkOperability(report, stage, want); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown operability stage %q", stage)
		}
		found = true
		return nil
	})
	return report, found, err
}
