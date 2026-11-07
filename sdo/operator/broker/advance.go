package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/upstream"
)

const (
	// UpstreamActor is the broker's own advance of accepted source to a
	// followed upstream revision. (Upstream names the memory remote.)
	UpstreamActor = validate.Upstream
	// AdvancePhase is the phase of an upstream advance.
	AdvancePhase = validate.AdvancePhase
)

// Trailers of an accepted advance commit. The accepted history stays linear:
// upstream history never enters the accepted repository, and the merge base
// of the next advance is the newest SDO-Upstream trailer.
const (
	upstreamTrailer     = "SDO-Upstream"
	upstreamBaseTrailer = "SDO-Upstream-Base"
)

// UpstreamChange reports, in the accepted object store, whether base is an
// ancestor of upstream and, when it is, which paths upstream changed relative
// to base. Both must be full commit IDs present in the accepted repository
// (the watcher materializes upstream there).
func (b *Broker) UpstreamChange(ctx context.Context, base, upstream string) (bool, []string, error) {
	return upstreamChange(ctx, b.target, base, upstream)
}

// upstreamChange is the one ancestry and changed-path procedure: the advance
// decides holds with it and PublishAdvance repeats it before accepting.
func upstreamChange(ctx context.Context, repository, base, upstream string) (bool, []string, error) {
	for _, commit := range []string{base, upstream} {
		if err := canonicalCommit(ctx, repository, commit); err != nil {
			return false, nil, err
		}
	}
	// base is an ancestor of upstream exactly when no commit is reachable
	// from base but not from upstream (also for unrelated histories).
	unreached, err := gitText(ctx, repository, "rev-list", "--max-count=1", upstream+".."+base)
	if err != nil {
		return false, nil, err
	}
	if unreached != "" {
		return false, nil, nil
	}
	out, err := runGit(ctx, repository, "diff", "--name-only", "--no-renames", "-z", base, upstream, "--")
	if err != nil {
		return false, nil, err
	}
	paths := []string{}
	for _, path := range strings.Split(string(out), "\x00") {
		if path != "" {
			paths = append(paths, path)
		}
	}
	return true, paths, nil
}

func canonicalCommit(ctx context.Context, repository, commit string) error {
	if !validGitID(commit) {
		return fmt.Errorf("upstream commit %q is not a full commit ID", commit)
	}
	resolved, err := gitText(ctx, repository, "rev-parse", "--verify", "--quiet", commit+"^{commit}")
	if err != nil || resolved != commit {
		return fmt.Errorf("commit %s is not in the accepted object store: %v", commit, err)
	}
	return nil
}

// checkUpstreamChange refuses an advance whose upstream rewrote history or
// changes operational memory.
func checkUpstreamChange(ctx context.Context, repository, base, upstream string) error {
	ancestor, paths, err := upstreamChange(ctx, repository, base, upstream)
	if err != nil {
		return err
	}
	if !ancestor {
		return fmt.Errorf("previous upstream %s is not an ancestor of %s", base, upstream)
	}
	for _, path := range paths {
		if layout.IsMemoryPath(path) {
			return fmt.Errorf("upstream %s changes operational memory (%s)", upstream, path)
		}
	}
	return nil
}

// LastUpstream returns the newest SDO-Upstream trailer on the accepted
// first-parent history: the upstream revision the accepted source is at.
// found is false when no broker commit records one (nothing is followed).
func (b *Broker) LastUpstream(ctx context.Context) (commit string, found bool, err error) {
	return lastUpstream(ctx, b.target, "HEAD")
}

func lastUpstream(ctx context.Context, repository, revision string) (string, bool, error) {
	out, err := runGit(ctx, repository, "log", "--first-parent", "--format=%H%x1f%ce%x1f%(trailers:key="+upstreamTrailer+",valueonly,separator=%x1d)%x1e", revision)
	if err != nil {
		return "", false, err
	}
	for _, record := range strings.Split(string(out), "\x1e") {
		fields := strings.Split(strings.TrimSpace(record), "\x1f")
		if len(fields) != 3 || fields[1] != brokerCommitterEmail || strings.TrimSpace(fields[2]) == "" {
			continue
		}
		values := strings.Split(strings.TrimSpace(fields[2]), "\x1d")
		value := strings.TrimSpace(values[0])
		if len(values) != 1 || !validGitID(value) {
			return "", false, fmt.Errorf("accepted commit %s has a malformed %s trailer", fields[0], upstreamTrailer)
		}
		return value, true, nil
	}
	return "", false, nil
}

// AcceptedSource is the accepted deployment an advance builds on: the
// evidence of the newest deployment and the plan the accepted HEAD declares
// in .sdo/deployment/plan.yaml.
type AcceptedSource struct {
	Evidence SourceEvidence
	Plan     *contracts.DeploymentPlan
}

// AcceptedSourceEvidence returns the trusted deployment evidence that is
// current for the accepted HEAD: the record of the newest source deployment
// on the first-parent history (onboarding's deployer commit, an upstream
// advance, or a responder source repair accepted with AcceptDeployed). Later
// memory commits do not change it; a responder repair accepted without a
// deployment (Publish, or a plain Accept) is not a deployment and leaves it as
// it was. found is false when no
// such deployment is recorded (a seeded install). The plan is read from the
// accepted HEAD's tree, not from the evidence.
func (b *Broker) AcceptedSourceEvidence(ctx context.Context) (AcceptedSource, bool, error) {
	var value SourceEvidence
	var found bool
	var plan *contracts.DeploymentPlan
	err := b.locked(func() error {
		var err error
		value, found, err = b.acceptedEvidenceLocked(ctx)
		if err != nil || !found {
			return err
		}
		plan, err = planAtTree(ctx, b.target, "HEAD^{tree}", b.PlanCatalogs)
		return err
	})
	return AcceptedSource{Evidence: value, Plan: plan}, found, err
}

// acceptedEvidenceLocked is the evidence of the newest source deployment.
func (b *Broker) acceptedEvidenceLocked(ctx context.Context) (SourceEvidence, bool, error) {
	commit, incidentID, ok, err := b.newestDeployment(ctx)
	if err != nil || !ok {
		return SourceEvidence{}, false, err
	}
	return b.sourceEvidenceLocked(ctx, incidentID, Receipt{IncidentID: incidentID, Commit: commit})
}

// AcceptedImagePins are the images the newest accepted deployment applied,
// bound to the effective tree it deployed. They are the image part of desired
// state (rendered.PinProvider): the controller reads them from the same
// ledger record that backs the accepted commit, so a source change that moves
// images gets new pins only through a new accepted deployment (onboarding, an
// advance or an AcceptDeployed responder repair), and a memory-only commit leaves them as they were. With no
// recorded deployment (a seeded install) there are no pins.
func (b *Broker) AcceptedImagePins(ctx context.Context) (rendered.ReceiptPins, error) {
	var pins rendered.ReceiptPins
	err := b.locked(func() error {
		evidence, found, err := b.acceptedEvidenceLocked(ctx)
		if err != nil || !found {
			return err
		}
		pins = rendered.ReceiptPins{EffectiveTree: evidence.EffectiveTree, BaseTree: evidence.BaseTree, Images: evidence.ContainerImages}
		return nil
	})
	return pins, err
}

func (b *Broker) newestDeployment(ctx context.Context) (commit, incidentID string, found bool, err error) {
	out, err := runGit(ctx, b.target, "log", "--first-parent", "--format=%H%x1f%ce%x1f%(trailers:key=SDO-Incident,valueonly)%x1f%(trailers:key=SDO-Actor,valueonly)%x1f%(trailers:key=SDO-Phase,valueonly)%x1e", "HEAD")
	if err != nil {
		return "", "", false, err
	}
	for _, record := range strings.Split(string(out), "\x1e") {
		fields := strings.Split(strings.TrimSpace(record), "\x1f")
		if len(fields) != 5 || fields[1] != brokerCommitterEmail {
			continue
		}
		actor, phase := Actor(strings.TrimSpace(fields[3])), Phase(strings.TrimSpace(fields[4]))
		if (actor == Deployer && phase == InitialPhase) || (actor == UpstreamActor && phase == AdvancePhase) {
			return fields[0], strings.TrimSpace(fields[2]), true, nil
		}
		if actor == Responder && phase == ProposalPhase {
			// A responder repair is a deployment only when AcceptDeployed
			// recorded the deployment of exactly this commit.
			incident := strings.TrimSpace(fields[2])
			deployed, err := b.deployedBy(ctx, incident, fields[0])
			if err != nil {
				return "", "", false, err
			}
			if deployed {
				return fields[0], incident, true, nil
			}
		}
	}
	return "", "", false, nil
}

// deployedBy reports whether the deployment evidence of incidentID is bound
// to commit. Another commit's record, or a pending record (staged, written
// before the head moved) of another tree, is not a deployment of commit. A
// pending record of commit's own tree is: the commit is in accepted history,
// so the head moved after the record was written and only the finalizing
// write was lost (a crash or a failed write between the two);
// sourceEvidenceLocked finalizes it on read.
func (b *Broker) deployedBy(ctx context.Context, incidentID, commit string) (bool, error) {
	data, err := os.ReadFile(b.sourceEvidencePath(incidentID))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var record struct {
		AcceptedCommit string `json:"accepted_commit"`
		BaseTree       string `json:"base_tree"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return false, fmt.Errorf("decode source evidence of %s: %w", incidentID, err)
	}
	if record.AcceptedCommit != "" {
		return record.AcceptedCommit == commit, nil
	}
	if record.BaseTree == "" {
		return false, nil
	}
	tree, err := gitText(ctx, b.target, "rev-parse", commit+"^{tree}")
	if err != nil {
		return false, err
	}
	return record.BaseTree == tree, nil
}

// ProposeAdvance creates the isolated advance checkout at accepted HEAD and
// merges upstream into it three-way with base as the merge base. Without
// conflicts the result is committed as the checkout's HEAD (candidate) and
// the checkout is left clean. Conflicting paths are returned, not an error,
// and the checkout is reset to the accepted HEAD. base must be the accepted
// source's upstream revision (LastUpstream), an ancestor of upstream, and
// upstream may not change operational memory. A candidate whose patch series
// does not apply keeps its checkout and is returned with the patch error (a
// *patchset.PatchError inside), for a deployment agent to correct
// (CorrectAdvance); the caller releases it otherwise.
func (b *Broker) ProposeAdvance(ctx context.Context, id, base, upstream string) (Proposal, string, []string, error) {
	if err := checkIncidentID(id); err != nil {
		return Proposal{}, "", nil, err
	}
	var proposal Proposal
	var candidate string
	var conflicts []string
	var keptHold error
	err := b.locked(func() error {
		if err := checkUpstreamChange(ctx, b.target, base, upstream); err != nil {
			return err
		}
		// A retry starts from the current accepted HEAD, never from an older
		// checkout.
		if err := b.lockedAt(b.proposalRoot(id, AdvancePhase)+".lock", func() error {
			return os.RemoveAll(b.proposalRoot(id, AdvancePhase))
		}); err != nil {
			return err
		}
		var err error
		if proposal, err = b.createProposal(ctx, id, AdvancePhase, ""); err != nil {
			return err
		}
		if err := b.proposeCandidate(ctx, proposal, id, base, upstream, &candidate, &conflicts); err != nil {
			var patchErr *patchset.PatchError
			if candidate != "" && errors.As(err, &patchErr) {
				// A patch series that does not apply is the deployment agent's to
				// correct: keep the checkout and its committed candidate.
				keptHold = err
				return nil
			}
			return errors.Join(err, b.lockedAt(b.proposalRoot(id, AdvancePhase)+".lock", func() error {
				return os.RemoveAll(b.proposalRoot(id, AdvancePhase))
			}))
		}
		return nil
	})
	if err != nil {
		return Proposal{}, "", nil, err
	}
	if keptHold != nil {
		return proposal, candidate, nil, keptHold
	}
	return proposal, candidate, conflicts, nil
}

func (b *Broker) proposeCandidate(ctx context.Context, proposal Proposal, id, base, upstream string, candidate *string, conflicts *[]string) error {
	if err := b.requireUpstreamBase(ctx, proposal.BaseCommit, base); err != nil {
		return err
	}
	var err error
	if *conflicts, err = b.mergeUpstream(ctx, proposal.Path, base, upstream); err != nil || len(*conflicts) > 0 {
		return err
	}
	message := fmt.Sprintf("sdo(%s): upstream advance candidate\n\n%s: %s\n%s: %s", id, upstreamTrailer, upstream, upstreamBaseTrailer, base)
	if _, err := runGitEnv(ctx, proposal.Path, brokerGitEnv(), "commit", "--quiet", "--no-verify", "--allow-empty", "-m", message); err != nil {
		return err
	}
	if *candidate, err = gitText(ctx, proposal.Path, "rev-parse", "HEAD"); err != nil {
		return err
	}
	status, err := gitText(ctx, proposal.Path, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return err
	}
	if status != "" {
		return fmt.Errorf("advance checkout is not clean at its candidate:\n%s", status)
	}
	tree, err := gitText(ctx, proposal.Path, "rev-parse", *candidate+"^{tree}")
	if err != nil {
		return err
	}
	if err := checkEffective(ctx, proposal.Path, tree); err != nil {
		return effectiveError(ctx, proposal.Path, *candidate, tree, base, upstream, err)
	}
	return nil
}

// requireUpstreamBase binds the merge base to the accepted history: the
// accepted source at commit must be at upstream revision base.
func (b *Broker) requireUpstreamBase(ctx context.Context, commit, base string) error {
	last, found, err := lastUpstream(ctx, b.target, commit)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("accepted commit %s records no upstream revision; nothing is followed", commit)
	}
	if last != base {
		return fmt.Errorf("advance base %s is not the accepted upstream revision %s", base, last)
	}
	return nil
}

// mergeUpstream is the one three-way merge of an advance, used both for the
// proposal and, on publish, for the broker's own recomputation. checkout must
// be a clean checkout of the accepted HEAD. Git 2.39 (the runtime image) has
// no merge-tree --merge-base, so the merge is read-tree with the explicit
// base plus merge-index; unmerged index entries are the conflicts, after
// which the checkout is reset to HEAD.
func (b *Broker) mergeUpstream(ctx context.Context, checkout, base, upstream string) ([]string, error) {
	// The upstream commit is reachable only from the watcher's ref in the
	// accepted repository, which a clone does not copy.
	if _, err := runGit(ctx, checkout, "-c", "uploadpack.allowAnySHA1InWant=true", "fetch", "--quiet", "--no-tags", b.target, upstream); err != nil {
		return nil, fmt.Errorf("fetch upstream %s into the advance checkout: %w", upstream, err)
	}
	if _, err := runGit(ctx, checkout, "read-tree", "-m", "-u", base, "HEAD", upstream); err != nil {
		return nil, fmt.Errorf("three-way merge of upstream %s: %w", upstream, err)
	}
	_, mergeErr := runGit(ctx, checkout, "merge-index", "-o", "git-merge-one-file", "-a")
	out, err := runGit(ctx, checkout, "ls-files", "--unmerged", "-z")
	if err != nil {
		return nil, err
	}
	var conflicts []string
	for _, entry := range strings.Split(string(out), "\x00") {
		if _, path, ok := strings.Cut(entry, "\t"); ok && !slices.Contains(conflicts, path) {
			conflicts = append(conflicts, path)
		}
	}
	if len(conflicts) > 0 {
		slices.Sort(conflicts)
		if _, err := runGit(ctx, checkout, "read-tree", "--reset", "-u", "HEAD"); err != nil {
			return nil, err
		}
		if _, err := runGit(ctx, checkout, "clean", "-fdq"); err != nil {
			return nil, err
		}
		return conflicts, nil
	}
	if mergeErr != nil {
		return nil, fmt.Errorf("three-way merge of upstream %s: %w", upstream, mergeErr)
	}
	return nil, nil
}

// PublishAdvance accepts the candidate of advance id. Under the publication
// lock the broker repeats the ancestry and changed-path checks, requires the
// accepted HEAD to still be the proposal's base and base to be its upstream
// revision, recomputes the three-way merge in a private clone, and requires
// that tree to be the deployed tree in evidence. The accepted commit is
// single-parent with SDO-Upstream and SDO-Upstream-Base trailers, and
// evidence (with its plan) becomes the accepted deployment evidence.
//
// The stage-advance operability report must be accepted by operability.Decide
// (recomputed here, never trusted) and bind to the evidence's trees and digests
// and to the accepted head the advance was merged onto. The broker records it in
// the evidence store, its digest in the evidence and an SDO-Operability trailer
// on the advance commit.
func (b *Broker) PublishAdvance(ctx context.Context, id, upstream, base string, evidence SourceEvidence, report operability.Report) (Receipt, error) {
	if err := checkIncidentID(id); err != nil {
		return Receipt{}, err
	}
	if err := validateSourceEvidence(evidence); err != nil {
		return Receipt{}, err
	}
	digest, err := advanceReportDigest(report, evidence)
	if err != nil {
		return Receipt{}, err
	}
	if evidence.OperabilityDigest != "" && evidence.OperabilityDigest != digest {
		return Receipt{}, fmt.Errorf("%w: evidence names report %s, the report is %s", ErrOperabilityUnbound, evidence.OperabilityDigest, digest)
	}
	evidence.OperabilityDigest = digest
	var receipt Receipt
	err = b.locked(b.withUpstream(ctx, func() error {
		if prior, ok, err := b.findAccepted(ctx, id, AdvancePhase); err != nil {
			return err
		} else if ok {
			return b.samePublishedAdvance(ctx, prior, id, upstream, base, evidence, &receipt)
		}
		if err := b.requireClean(ctx); err != nil {
			return err
		}
		if err := b.stageAdvance(ctx, id, upstream, base, &evidence, &report, func(st stagedAdvance) error {
			if err := b.writeOperabilityReport(id, operability.StageAdvance, report); err != nil {
				return err
			}
			if err := b.writeSourceEvidence(id, evidence); err != nil {
				return err
			}
			trailers := []string{upstreamTrailer + ": " + upstream, upstreamBaseTrailer + ": " + base, mirrorTrailer + ": " + st.mirror, operabilityTrailer(operability.StageAdvance, digest)}
			if st.correction != nil {
				trailers = append(trailers, advanceCorrectionTrailer+": "+string(Deployer))
			}
			receipt, err = b.acceptWithTrailers(ctx, st.stage, id, UpstreamActor, AdvancePhase, st.paths, trailers, st.gaps...)
			if err != nil {
				return err
			}
			accepted := evidence
			accepted.AcceptedCommit = receipt.Commit
			if err := b.writeSourceEvidence(id, accepted); err != nil {
				return fmt.Errorf("advance accepted but evidence finalization needs reconciliation: %w", err)
			}
			if err := b.pruneUpstreamHolds(ctx, upstream, base); err != nil {
				return fmt.Errorf("advance accepted but pruning upstream hold refs failed: %w", err)
			}
			return nil
		}); err != nil {
			return err
		}
		return b.lockedAt(b.proposalRoot(id, AdvancePhase)+".lock", func() error {
			return os.RemoveAll(b.proposalRoot(id, AdvancePhase))
		})
	}))
	return receipt, err
}

// stagedAdvance is a merged and validated advance candidate in a private
// clone, handed to the publication step while the clone exists.
type stagedAdvance struct {
	stage      string
	mirror     string
	paths      []string
	gaps       []validate.Diagnostic
	correction *advanceCorrectionCommits
}

// stageAdvance recomputes the advance candidate in a private clone and runs
// the validation PublishAdvance applies, then calls accept with the clone.
// Without evidence and report (CheckAdvance) the checks that bind the candidate
// to a finished deployment are skipped: everything the validator would reject
// is still rejected. The caller holds the publication lock.
func (b *Broker) stageAdvance(ctx context.Context, id, upstream, base string, evidence *SourceEvidence, report *operability.Report, accept func(stagedAdvance) error) error {
	proposal, err := b.proposal(id, AdvancePhase)
	if err != nil {
		return fmt.Errorf("advance proposal %s: %w", id, err)
	}
	current, err := gitText(ctx, b.target, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if current != proposal.BaseCommit {
		return fmt.Errorf("accepted HEAD moved from %s to %s since the advance was proposed", proposal.BaseCommit, current)
	}
	if report != nil {
		if err := requireDeploymentSubject(*report, current); err != nil {
			return err
		}
	}
	if err := b.requireUpstreamBase(ctx, current, base); err != nil {
		return err
	}
	if err := checkUpstreamChange(ctx, b.target, base, upstream); err != nil {
		return err
	}
	if b.validator == nil && b.codePath != "shell" {
		return errors.New("proposal validation is not configured")
	}
	stage, cleanup, err := b.privateClone(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	conflicts, err := b.mergeUpstream(ctx, stage, base, upstream)
	if err != nil {
		return err
	}
	if len(conflicts) > 0 {
		return fmt.Errorf("upstream %s conflicts with accepted source: %s", upstream, strings.Join(conflicts, ", "))
	}
	var gaps []validate.Diagnostic
	var diagnostics []validate.Diagnostic
	// checkAs judges the stage's changes since base as actor, whose grants
	// decide them. The merge is the upstream actor's; a correction replayed
	// onto it is the deployer's, judged against the merge it corrects.
	checkAs := func(actor Actor, base, tree string) error {
		changed, err := validate.ChangedPaths(ctx, stage, base)
		if err != nil {
			return err
		}
		publication := &validate.PublicationContext{BaseCommit: base, IncidentID: id, VerifiedSourceTree: tree}
		more, err := b.checkAttestedCandidate(ctx, stage, base, actor, AdvancePhase, changed, publication, nil, &gaps)
		diagnostics = append(diagnostics, more...)
		return err
	}
	correction, err := b.advanceCorrection(ctx, proposal)
	if err != nil {
		return err
	}
	if correction != nil {
		merged, err := advanceTreeDigest(ctx, stage)
		if err != nil {
			return err
		}
		if err := checkAs(UpstreamActor, current, merged); err != nil {
			return fmt.Errorf("advance validation: %w", err)
		}
		if err := b.replayCorrection(ctx, stage, proposal, correction); err != nil {
			return err
		}
	}
	tree, err := advanceTreeDigest(ctx, stage)
	if err != nil {
		return err
	}
	if evidence != nil && tree != evidence.BaseTree {
		return fmt.Errorf("merged advance tree %s differs from the deployed tree %s", tree, evidence.BaseTree)
	}
	// The deployed tree is the effective one: the patch series must apply
	// to the tree being accepted, however the proposal got there.
	if err := checkEffective(ctx, stage, tree); err != nil {
		return effectiveError(ctx, stage, "", tree, base, upstream, err)
	}
	if evidence != nil {
		if err := requireEffectiveEvidence(ctx, stage, tree, *evidence); err != nil {
			return err
		}
		if err := requirePlanEvidence(ctx, stage, *evidence, b.PlanCatalogs); err != nil {
			return err
		}
	}
	mirror, err := mirrorState(ctx, stage, current, base, upstream)
	if err != nil {
		return err
	}
	paths, err := validate.ChangedPaths(ctx, stage, current)
	if err != nil {
		return err
	}
	if correction != nil {
		err = checkAs(Deployer, correction.merge, tree)
	} else {
		err = checkAs(UpstreamActor, current, tree)
	}
	if err != nil {
		return fmt.Errorf("advance validation: %w", err)
	}
	if err := validate.Err(diagnostics); err != nil {
		return fmt.Errorf("advance validation: %w", err)
	}
	return accept(stagedAdvance{stage: stage, mirror: mirror, paths: paths, gaps: gaps, correction: correction})
}

// CheckAdvance runs, without publishing, the validation PublishAdvance runs on
// the candidate of advance id: the same merge, patch application and
// validate.Check as the upstream actor (or the deployer after a correction).
// A rejection is returned as text (empty when the candidate would validate), so
// a caller can hold before any cluster effect instead of deploying a candidate
// the broker would refuse to accept. Evidence-bound checks wait for the
// deployment: they need its result.
func (b *Broker) CheckAdvance(ctx context.Context, id, upstream, base string) (rejection string, err error) {
	if err := checkIncidentID(id); err != nil {
		return "", err
	}
	err = b.locked(b.withUpstream(ctx, func() error {
		return b.stageAdvance(ctx, id, upstream, base, nil, nil, func(stagedAdvance) error { return nil })
	}))
	var failure *validate.Failure
	if errors.As(err, &failure) {
		return err.Error(), nil
	}
	return "", err
}

// pruneUpstreamHolds deletes the watcher's hold refs except the accepted
// upstream and its base: the next advance needs the accepted upstream as its
// base, and the base stays so the last advance can still be re-derived.
// Revisions that were held back or superseded are not retried, so their refs
// are no longer needed.
func (b *Broker) pruneUpstreamHolds(ctx context.Context, upstreamCommit, base string) error {
	out, err := runGit(ctx, b.target, "for-each-ref", "--format=%(refname)", upstream.HoldRefPrefix)
	if err != nil {
		return err
	}
	for _, ref := range strings.Fields(string(out)) {
		sha := strings.TrimPrefix(ref, upstream.HoldRefPrefix)
		if sha == upstreamCommit || sha == base {
			continue
		}
		if _, err := runGit(ctx, b.target, "update-ref", "-d", ref); err != nil {
			return err
		}
	}
	return nil
}

// samePublishedAdvance makes a repeated publish idempotent: the accepted
// commit must record the same upstream and base and its evidence must equal
// the claimed evidence.
func (b *Broker) samePublishedAdvance(ctx context.Context, prior Receipt, id, upstream, base string, evidence SourceEvidence, receipt *Receipt) error {
	trailers, err := gitText(ctx, b.target, "log", "-1", "--format=%(trailers:key="+upstreamTrailer+",valueonly)%x1f%(trailers:key="+upstreamBaseTrailer+",valueonly)", prior.Commit)
	if err != nil {
		return err
	}
	recorded, recordedBase, _ := strings.Cut(trailers, "\x1f")
	if strings.TrimSpace(recorded) != upstream || strings.TrimSpace(recordedBase) != base {
		return fmt.Errorf("advance %s was accepted for another upstream revision", id)
	}
	stored, found, err := b.sourceEvidenceLocked(ctx, id, prior)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("accepted advance has no trusted deployment evidence")
	}
	claimed := evidence
	claimed.AcceptedCommit = prior.Commit
	// A redelivery keeps the first report: live observations differ per attempt.
	claimed.OperabilityDigest = stored.OperabilityDigest
	if !sameSourceEvidence(stored, claimed) {
		return errors.New("accepted advance evidence differs from the verified deployment")
	}
	*receipt = prior
	return nil
}

// ReleaseAdvance discards the advance checkout id.
func (b *Broker) ReleaseAdvance(id string) error {
	return b.ReleaseProposal(id, AdvancePhase)
}
