package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/rendered"
)

// RecordRenderedLock accepts only a controller-produced deployment snapshot,
// independently checked against a fresh render under the publication lock.
// The expected head prevents a rollout observation from racing source acceptance.
func (b *Broker) RecordRenderedLock(ctx context.Context, expected string, snapshot rendered.Snapshot) (string, error) {
	var commit string
	err := b.locked(func() error {
		if err := b.requireClean(ctx); err != nil {
			return err
		}
		head, err := gitText(ctx, b.target, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		if head != expected {
			return errors.New("rendered lock accepted head changed")
		}
		evidence, err := b.renderedDeploymentEvidenceLocked(ctx, head)
		if err != nil {
			return err
		}
		if err := snapshot.ValidateWithEvidence(ctx, b.target, head, evidence); err != nil {
			return err
		}
		stage, cleanup, err := b.privateClone(ctx)
		if err != nil {
			return err
		}
		defer cleanup()
		if err := snapshot.Write(stage); err != nil {
			return err
		}
		paths, err := validate.ChangedPaths(ctx, stage, head)
		if err != nil {
			return err
		}
		if len(paths) == 0 {
			commit = head
			return nil
		}
		receipt, err := b.accept(ctx, stage, "rendered-"+snapshot.Lock.SourceCommit, "controller", Phase("rendered"), paths)
		commit = receipt.Commit
		return err
	})
	return commit, err
}

// Head returns the current broker-accepted revision, including a publication
// whose reply was lost before the controller persisted its next revision.
func (b *Broker) Head(ctx context.Context) (string, error) {
	var head string
	err := b.locked(func() error { var err error; head, err = gitText(ctx, b.target, "rev-parse", "HEAD"); return err })
	return head, err
}

// RenderedDeploymentEvidence reads only finalized, accepted source records from
// the private broker ledger. Losing this ledger yields gaps, never live evidence.
func (b *Broker) RenderedDeploymentEvidence(ctx context.Context, revision string) ([]rendered.DeploymentEvidence, error) {
	var evidence []rendered.DeploymentEvidence
	err := b.locked(func() error {
		var err error
		evidence, err = b.renderedDeploymentEvidenceLocked(ctx, revision)
		return err
	})
	return evidence, err
}

func (b *Broker) renderedDeploymentEvidenceLocked(ctx context.Context, revision string) ([]rendered.DeploymentEvidence, error) {
	paths, err := filepath.Glob(filepath.Join(b.workspaceRoot, "source-evidence", "*.json"))
	if err != nil {
		return nil, err
	}
	result := []rendered.DeploymentEvidence{}
	source := rendered.Source{Repository: b.target}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var value SourceEvidence
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, fmt.Errorf("read deployment image evidence: %w", err)
		}
		commit := value.AcceptedCommit
		if commit == "" {
			continue
		} // Pending publication is not accepted evidence.
		value.AcceptedCommit = ""
		if err := validateSourceEvidence(value); err != nil {
			return nil, err
		}
		tree, err := gitText(ctx, b.target, "rev-parse", commit+"^{tree}")
		if err != nil || tree != value.ProposalTreeDigest {
			return nil, errors.New("deployment image evidence has no matching accepted tree")
		}
		if _, err := runGit(ctx, b.target, "merge-base", "--is-ancestor", commit, revision); err != nil {
			continue
		}
		same, err := source.SameInputs(ctx, tree, revision)
		if err != nil {
			return nil, err
		}
		if same {
			result = append(result, rendered.DeploymentEvidence{SourceTree: tree, Images: value.ContainerImages})
		}
	}
	return result, nil
}
