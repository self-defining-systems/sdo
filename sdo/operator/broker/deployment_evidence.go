package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/rendered"
)

const (
	// UpstreamActor is the actor of an upstream advance commit. Old accepted
	// histories carry them; nothing writes new ones.
	UpstreamActor = validate.Upstream
	// AdvancePhase is the phase of an upstream advance commit.
	AdvancePhase = validate.AdvancePhase
)

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
		if len(fields) != 5 {
			continue
		}
		if fields[1] != brokerCommitterEmail {
			// Someone else's commit: a deployment only when a head deployment
			// of exactly this commit was recorded.
			if commit := strings.TrimSpace(fields[0]); validGitID(commit) {
				deployed, err := b.headDeployed(ctx, commit)
				if err != nil {
					return "", "", false, err
				}
				if deployed {
					return commit, headDeploymentID(commit), true, nil
				}
			}
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
