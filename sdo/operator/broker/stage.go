package broker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/rendered"
)

// ErrNoCandidate means no staged candidate exists for the incident and phase:
// it was never staged, was rejected, discarded or replaced by a newer stage.
var ErrNoCandidate = errors.New("no staged candidate")

// ErrCandidateStale means the accepted head moved after the candidate was
// validated on it (a local or peer publication). The candidate is never
// accepted on top: stage it again (the proposal is replayed) and re-verify.
var ErrCandidateStale = errors.New("staged candidate is stale: the accepted head moved after staging")

// ErrCandidateUnbound means the caller's candidate or evidence does not bind
// to what the broker staged: another commit, checkout, tree or effective
// deployment identity.
var ErrCandidateUnbound = errors.New("evidence does not bind to the staged candidate")

// Candidate is a validated broker commit held in a private stage. The
// accepted head does not point at it, and the accepted object store does not
// contain it, until Accept. Path is a clean checkout whose HEAD is Commit, for
// independent verification; only the broker's own record of the stage is
// trusted at acceptance, never these fields.
type Candidate struct {
	IncidentID string
	Actor      Actor
	Phase      Phase
	Path       string
	// Parent is the accepted head the candidate was validated on, and its
	// commit's only parent.
	Parent string
	// Commit is empty when the proposal had nothing to publish.
	Commit string
	Tree   string
	// Accepted reports the incident and phase already accepted as Commit (a
	// redelivery); there is no stage.
	Accepted     bool
	ChangedPaths []string
	CoverageGaps []validate.Diagnostic
}

// CandidateEvidence is what Go independently verified about a staged
// candidate: the commit and tree it judged and the effective deployment
// identity (rendered.Effective of that tree) it observed. Accept recomputes
// every field from the stage; a claim is never trusted.
type CandidateEvidence struct {
	Commit        string
	Tree          string
	EffectiveTree string
	PatchesDigest string
}

// StageCandidate runs every publish check (ownership, the shared validator,
// verified-source and advance rules, replay onto the current head) and commits
// the result in a private stage, exactly as Publish would, without moving the
// accepted head. A newer stage for the same incident and phase replaces the
// previous one. A rejection stages nothing; the proposal checkout keeps the
// agent's work.
func (b *Broker) StageCandidate(ctx context.Context, incidentID string, actor Actor, phase Phase) (Candidate, error) {
	if err := checkIncidentID(incidentID); err != nil {
		return Candidate{}, err
	}
	var staged Candidate
	receipt, err := b.publishOrStage(ctx, incidentID, actor, phase, "", nil, nil, &staged)
	if err != nil {
		return Candidate{}, err
	}
	if staged.Commit == "" {
		// Nothing to publish, or a redelivery of an accepted incident.
		staged = Candidate{IncidentID: incidentID, Actor: actor, Phase: phase, Commit: receipt.Commit, Accepted: receipt.Commit != "", ChangedPaths: receipt.ChangedPaths}
	}
	return staged, nil
}

// Accept fast-forwards the accepted head to a staged candidate whose evidence
// binds to it. It refuses, leaving the head untouched, a candidate the broker
// did not stage (ErrNoCandidate), evidence for anything else
// (ErrCandidateUnbound), and a candidate whose base is no longer the accepted
// head, locally or on the memory remote (ErrCandidateStale). Accepting an
// already accepted candidate returns its receipt. A candidate with nothing to
// publish accepts nothing.
func (b *Broker) Accept(ctx context.Context, candidate Candidate, evidence CandidateEvidence) (Receipt, error) {
	return b.acceptStaged(ctx, candidate, evidence.Commit, func(stored Candidate) error {
		return requireBoundEvidence(ctx, stored, evidence)
	}, nil)
}

// AcceptDeployed is Accept for a candidate whose effective tree the trusted
// deployer applied to the cluster and verified: the evidence is the full
// deployment record, not only a judged identity. It refuses evidence that
// does not bind to the stage (another tree, effective tree or patch series,
// or a deployment source other than the one the tree's plan declares), and on
// acceptance records the evidence for the accepted commit before the head
// moves, so the commit is the newest deployment: image pins follow it and the
// next upstream advance builds on it. A redelivery with the same evidence is
// idempotent.
func (b *Broker) AcceptDeployed(ctx context.Context, candidate Candidate, evidence SourceEvidence) (Receipt, error) {
	if err := validateSourceEvidence(evidence); err != nil {
		return Receipt{}, err
	}
	return b.acceptStaged(ctx, candidate, candidate.Commit, func(stored Candidate) error {
		if evidence.BaseTree != stored.Tree {
			return fmt.Errorf("%w: deployment evidence is of tree %s, the candidate is tree %s", ErrCandidateUnbound, evidence.BaseTree, stored.Tree)
		}
		if err := requireBoundEvidence(ctx, stored, CandidateEvidence{Commit: stored.Commit, Tree: stored.Tree, EffectiveTree: evidence.EffectiveTree, PatchesDigest: evidence.PatchesDigest}); err != nil {
			return err
		}
		if err := requirePlanEvidence(ctx, stored.Path, evidence, b.PlanCatalogs); err != nil {
			return fmt.Errorf("%w: %w", ErrCandidateUnbound, err)
		}
		return nil
	}, &evidence)
}

// acceptStaged fast-forwards the head to the staged candidate once bind (the
// caller's evidence check against the stored stage) passes. With deployed
// non-nil its evidence is staged in the ledger before the head moves and
// finalized with the accepted commit after.
func (b *Broker) acceptStaged(ctx context.Context, candidate Candidate, evidenceCommit string, bind func(Candidate) error, deployed *SourceEvidence) (Receipt, error) {
	if err := checkIncidentID(candidate.IncidentID); err != nil {
		return Receipt{}, err
	}
	if candidate.Commit == "" {
		return Receipt{IncidentID: candidate.IncidentID, Phase: candidate.Phase}, nil
	}
	var receipt Receipt
	err := b.locked(func() error {
		if prior, ok, err := b.findAccepted(ctx, candidate.IncidentID, candidate.Phase); err != nil {
			return err
		} else if ok {
			if prior.Commit != candidate.Commit || evidenceCommit != prior.Commit {
				return fmt.Errorf("%w: %s %s was accepted as %s", ErrCandidateUnbound, candidate.Phase, candidate.IncidentID, prior.Commit)
			}
			if deployed != nil {
				stored, found, err := b.sourceEvidenceLocked(ctx, candidate.IncidentID, prior)
				if err != nil {
					return err
				}
				claimed := *deployed
				claimed.AcceptedCommit = prior.Commit
				if !found || !sameSourceEvidence(stored, claimed) {
					return fmt.Errorf("%w: accepted %s %s has other deployment evidence", ErrCandidateUnbound, candidate.Phase, candidate.IncidentID)
				}
			}
			receipt = prior
			return os.RemoveAll(b.candidateRoot(candidate.IncidentID, candidate.Phase))
		}
		stored, err := b.loadCandidate(candidate.IncidentID, candidate.Phase)
		if err != nil {
			return err
		}
		if candidate.Commit != stored.Commit || candidate.Path != stored.Path {
			return fmt.Errorf("%w: candidate %s at %s is not the staged %s", ErrCandidateUnbound, candidate.Commit, candidate.Path, stored.Commit)
		}
		if err := requireCandidateCommit(ctx, stored); err != nil {
			return err
		}
		if err := bind(stored); err != nil {
			return err
		}
		if err := b.syncUpstream(ctx); err != nil {
			return err
		}
		if err := b.requireClean(ctx); err != nil {
			return err
		}
		head, err := gitText(ctx, b.target, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		if head != stored.Parent {
			return fmt.Errorf("%w: validated on %s, accepted head is %s", ErrCandidateStale, stored.Parent, head)
		}
		if deployed != nil {
			if err := b.writeSourceEvidence(stored.IncidentID, *deployed); err != nil {
				return err
			}
		}
		if err := b.advanceHead(ctx, stored.Path, stored.Commit); errors.Is(err, errUpstreamMoved) {
			return fmt.Errorf("%w: %w", ErrCandidateStale, err)
		} else if err != nil {
			return err
		}
		receipt = Receipt{IncidentID: stored.IncidentID, Phase: stored.Phase, Commit: stored.Commit, ChangedPaths: stored.ChangedPaths, CoverageGaps: stored.CoverageGaps}
		if deployed != nil {
			accepted := *deployed
			accepted.AcceptedCommit = stored.Commit
			if err := b.writeSourceEvidence(stored.IncidentID, accepted); err != nil {
				return fmt.Errorf("source accepted but evidence finalization needs reconciliation: %w", err)
			}
		}
		return os.RemoveAll(b.candidateRoot(stored.IncidentID, stored.Phase))
	})
	return receipt, err
}

// DiscardCandidate removes a staged candidate that verification rejected. The
// accepted head and the proposal checkout are untouched.
func (b *Broker) DiscardCandidate(incidentID string, phase Phase) error {
	if err := checkIncidentID(incidentID); err != nil {
		return err
	}
	return b.locked(func() error { return os.RemoveAll(b.candidateRoot(incidentID, phase)) })
}

func (b *Broker) candidateRoot(incidentID string, phase Phase) string {
	sum := sha256.Sum256([]byte(incidentID + "\x00" + string(phase)))
	return filepath.Join(b.workspaceRoot, "candidates", fmt.Sprintf("%x", sum))
}

// stageCheckout is the private clone a publication commits in. A staged
// candidate's clone outlives the publication lock; it is removed unless the
// candidate record was written.
func (b *Broker) stageCheckout(ctx context.Context, incidentID string, phase Phase, held *Candidate) (string, func(), error) {
	if held == nil {
		return b.privateClone(ctx)
	}
	root := b.candidateRoot(incidentID, phase)
	if err := os.RemoveAll(root); err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", nil, err
	}
	cleanup := func() {
		if _, err := os.Stat(filepath.Join(root, "candidate.json")); err != nil {
			os.RemoveAll(root)
		}
	}
	stage := filepath.Join(root, "checkout")
	if err := b.cloneStage(ctx, stage); err != nil {
		cleanup()
		return "", nil, err
	}
	return stage, cleanup, nil
}

// holdCandidate records the committed stage as the incident's candidate. The
// record is written last, so a stage without one is never accepted.
func (b *Broker) holdCandidate(ctx context.Context, stage string, actor Actor, parent string, receipt Receipt) (Candidate, error) {
	tree, err := gitText(ctx, stage, "rev-parse", receipt.Commit+"^{tree}")
	if err != nil {
		return Candidate{}, err
	}
	candidate := Candidate{IncidentID: receipt.IncidentID, Actor: actor, Phase: receipt.Phase, Path: stage, Parent: parent, Commit: receipt.Commit, Tree: tree, ChangedPaths: receipt.ChangedPaths, CoverageGaps: receipt.CoverageGaps}
	data, err := json.Marshal(candidate)
	if err != nil {
		return Candidate{}, err
	}
	root := filepath.Dir(stage)
	temporary := filepath.Join(root, ".candidate.json")
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return Candidate{}, err
	}
	if err := os.Rename(temporary, filepath.Join(root, "candidate.json")); err != nil {
		return Candidate{}, err
	}
	return candidate, nil
}

func (b *Broker) loadCandidate(incidentID string, phase Phase) (Candidate, error) {
	root := b.candidateRoot(incidentID, phase)
	data, err := os.ReadFile(filepath.Join(root, "candidate.json"))
	if os.IsNotExist(err) {
		return Candidate{}, fmt.Errorf("%w for %s %s", ErrNoCandidate, phase, incidentID)
	}
	if err != nil {
		return Candidate{}, err
	}
	var candidate Candidate
	if err := json.Unmarshal(data, &candidate); err != nil {
		return Candidate{}, fmt.Errorf("candidate record: %w", err)
	}
	if candidate.IncidentID != incidentID || candidate.Phase != phase || candidate.Path != filepath.Join(root, "checkout") || !validGitID(candidate.Commit) || !validGitID(candidate.Parent) || !validGitID(candidate.Tree) {
		return Candidate{}, errors.New("invalid candidate record")
	}
	return candidate, nil
}

// requireCandidateCommit checks the stage still holds the broker's commit as
// recorded: the recorded tree, the accepted head it was validated on as its
// only parent, and the broker identity.
func requireCandidateCommit(ctx context.Context, c Candidate) error {
	fields, err := gitText(ctx, c.Path, "log", "-1", "--format=%T %P%x00%ce", c.Commit)
	if err != nil {
		return fmt.Errorf("%w: staged commit %s: %w", ErrCandidateUnbound, c.Commit, err)
	}
	if fields != c.Tree+" "+c.Parent+"\x00"+brokerCommitterEmail {
		return fmt.Errorf("%w: staged commit %s is not the broker's commit of tree %s on %s", ErrCandidateUnbound, c.Commit, c.Tree, c.Parent)
	}
	message, err := gitText(ctx, c.Path, "log", "-1", "--format=%B", c.Commit)
	if err != nil {
		return err
	}
	message = "\n" + message + "\n"
	for _, trailer := range []string{"SDO-Incident: " + c.IncidentID, "SDO-Actor: " + string(c.Actor), "SDO-Phase: " + string(c.Phase)} {
		if !strings.Contains(message, "\n"+trailer+"\n") {
			return fmt.Errorf("%w: staged commit %s lacks %q", ErrCandidateUnbound, c.Commit, trailer)
		}
	}
	return nil
}

// requireBoundEvidence recomputes the evidence's subject from the stage.
func requireBoundEvidence(ctx context.Context, c Candidate, evidence CandidateEvidence) error {
	if evidence.Commit != c.Commit || evidence.Tree != c.Tree {
		return fmt.Errorf("%w: evidence judged commit %q tree %q, the candidate is %s tree %s", ErrCandidateUnbound, evidence.Commit, evidence.Tree, c.Commit, c.Tree)
	}
	effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: c.Path}, rendered.BaseTree(c.Tree))
	if err != nil {
		return fmt.Errorf("compute effective tree of the staged candidate: %w", err)
	}
	defer effective.Close()
	if effective.ID != evidence.EffectiveTree || effective.PatchesDigest != evidence.PatchesDigest {
		return fmt.Errorf("%w: candidate effective tree %s (patches %s), evidence observed %s (patches %s)", ErrCandidateUnbound, effective.ID, effective.PatchesDigest, evidence.EffectiveTree, evidence.PatchesDigest)
	}
	return nil
}
