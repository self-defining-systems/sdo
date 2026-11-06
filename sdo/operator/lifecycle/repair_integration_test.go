//go:build integration

package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// repairReadiness is the Deployer's contract without a cluster: it renders
// nothing, but reports the deployment of exactly the checkout it was handed,
// bound the way the real Deployer binds it (the checkout's tree and its
// effective tree). It records the checkouts it applied.
type repairDeployerFake struct {
	SourceReadinessVerifier
	source  *contracts.DeploymentSource
	applied []string
	err     error
}

func (d *repairDeployerFake) ApplyAndVerify(ctx context.Context, checkout string, plan *contracts.DeploymentPlan) (DeploymentReceipt, error) {
	d.applied = append(d.applied, checkout)
	if d.err != nil {
		return DeploymentReceipt{}, d.err
	}
	tree, err := broker.ProposalTreeDigest(ctx, checkout)
	if err != nil {
		return DeploymentReceipt{}, err
	}
	effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: checkout}, rendered.BaseTree(tree))
	if err != nil {
		return DeploymentReceipt{}, err
	}
	defer effective.Close()
	return DeploymentReceipt{
		Source: proto.Clone(plan.GetSource()).(*contracts.DeploymentSource), BaseTree: tree, EffectiveTree: effective.ID, PatchesDigest: effective.PatchesDigest,
		ObservedRevision: effective.ID, RenderDigest: strings.Repeat("a", 64), EnvDigest: strings.Repeat("b", 64),
		ContainerImages: []rendered.DeployedImage{{Ref: sdk.ObjectRef{Kind: "Deployment", Namespace: "ns", Name: "app"}, Name: "app", Applied: "app@sha256:" + strings.Repeat("c", 64), Digest: "sha256:" + strings.Repeat("c", 64), Inputs: rendered.ImageInputs{SourceImage: "app", ContextPath: ".", DockerfilePath: "Dockerfile"}}},
		Images:          []string{"app@sha256:" + strings.Repeat("c", 64)}, ResourceIDs: []string{"Deployment/ns/app"}, Ready: true,
	}, nil
}

type repairVerifierFake struct {
	CandidateVerifier
	roots   []string
	verdict HealthVerdict
}

func (v *repairVerifierFake) VerifyCandidate(_ context.Context, root, _, _, _ string, _ DeploymentReceipt) (HealthVerdict, error) {
	v.roots = append(v.roots, root)
	return v.verdict, nil
}

// repairWorld is a real accepted repository and broker onboarded with a plan
// and its deployment evidence, plus a responder proposal whose patch changes
// the vendored app file.
type repairGit struct {
	*gateWorld
	source *contracts.DeploymentSource
}

func newRepairGit(t *testing.T) *repairGit {
	t.Helper()
	w := &repairGit{gateWorld: newGateWorld(t)}
	config, err := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
	if err != nil {
		t.Fatal(err)
	}
	w.source = &contracts.DeploymentSource{RendererId: "raw", Config: config}
	p, err := w.broker.CreateProposalForPhase(context.Background(), "onboard", broker.InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := planfile.Write(p.Path, &contracts.DeploymentPlan{Source: w.source, ExpectedWorkloadIds: []string{"Deployment/ns/app"}}); err != nil {
		t.Fatal(err)
	}
	deployer := &repairDeployerFake{}
	receipt, err := deployer.ApplyAndVerify(context.Background(), p.Path, &contracts.DeploymentPlan{Source: w.source})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.broker.PublishVerifiedSource(context.Background(), "onboard", deploymentToBroker(receipt)); err != nil {
		t.Fatalf("onboard: %v", err)
	}
	return w
}

// responderPatch captures an edit of app.txt as the responder's patch.
func (w *repairGit) responderPatch(t *testing.T, id string) {
	t.Helper()
	p, err := w.broker.CreateProposalForPhase(context.Background(), id, broker.ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.Path, "app.txt"), []byte("synthetic app, repaired\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := patchset.Create(context.Background(), p.Path, patchset.CreateRequest{Owner: patchset.OwnerResponder, Incident: id, Reason: "Repair the synthetic application the deployment ships with this change"}); err != nil {
		t.Fatal(err)
	}
}

func (w *repairGit) repairer(d *repairDeployerFake, v *repairVerifierFake) RepairDeployer {
	return RepairDeployer{Broker: w.broker, Readiness: d, Verifier: v}
}

var realRepairConfig = RepairConfig{Namespace: "ns", HealthObjective: "stay up", PendingEvaluations: 1, InfrastructureTries: 1}

// The one path against a real broker: a responder's captured patch is staged,
// its effective tree is deployed from the staged checkout (never the agent's
// proposal checkout), verified there, and only then accepted with new
// evidence, which becomes the newest deployment (pins).
func TestRealRepairIsDeployedFromTheStagedCandidateThenAccepted(t *testing.T) {
	ctx := context.Background()
	w := newRepairGit(t)
	onboarded, found, err := w.broker.AcceptedSourceEvidence(ctx)
	if err != nil || !found {
		t.Fatalf("onboarding evidence: %v %v", found, err)
	}
	w.responderPatch(t, "inc-1")
	before := w.head(t)
	proposal, err := w.broker.ProposalInfo("inc-1", broker.ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	d, v := &repairDeployerFake{}, &repairVerifierFake{verdict: HealthVerdict{Outcome: VerificationClear}}
	result, err := w.repairer(d, v).Repair(ctx, realRepairConfig, "inc-1")
	if err != nil || result.Outcome != RepairDeployed {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	if len(d.applied) != 1 || d.applied[0] == proposal.Path || !strings.Contains(d.applied[0], "candidates") || len(v.roots) != 1 || v.roots[0] != d.applied[0] {
		t.Fatalf("applied %v and verified %v, want one deployment of the staged candidate, not the proposal checkout %s", d.applied, v.roots, proposal.Path)
	}
	if w.head(t) == before || w.head(t) != result.Receipt.Commit {
		t.Fatalf("head = %s, want the accepted repair %s", w.head(t), result.Receipt.Commit)
	}
	accepted, _, err := w.broker.AcceptedSourceEvidence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Evidence.EffectiveTree == onboarded.Evidence.EffectiveTree || accepted.Evidence.AcceptedCommit != result.Receipt.Commit {
		t.Fatalf("accepted evidence %+v is not the repair's deployment", accepted.Evidence)
	}
	pins, err := w.broker.AcceptedImagePins(ctx)
	if err != nil || pins.EffectiveTree != accepted.Evidence.EffectiveTree {
		t.Fatalf("pins = %+v, %v, want the repair deployment's", pins, err)
	}
}

// Nothing is accepted that the cluster or the detectors refused, and the
// candidate stage does not outlive the refusal.
func TestRealRepairIsNotAcceptedWhenItsDeploymentIsRefused(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		deployer *repairDeployerFake
		verdict  HealthVerdict
		hold     HoldReason
	}{
		"rollout":  {deployer: &repairDeployerFake{err: errors.New("deployment never became ready")}, hold: HoldRollout},
		"detector": {deployer: &repairDeployerFake{}, verdict: HealthVerdict{Outcome: VerificationSourceFault, Summary: "health detector fires"}, hold: HoldHealth},
	} {
		t.Run(name, func(t *testing.T) {
			w := newRepairGit(t)
			w.responderPatch(t, "inc-1")
			before := w.head(t)
			onboarded, _, err := w.broker.AcceptedSourceEvidence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			result, err := w.repairer(tc.deployer, &repairVerifierFake{verdict: tc.verdict}).Repair(ctx, realRepairConfig, "inc-1")
			if err != nil || result.Outcome != RepairHeld || result.Hold != tc.hold {
				t.Fatalf("result = %+v, err = %v, want a %s hold", result, err, tc.hold)
			}
			if w.head(t) != before {
				t.Fatal("a refused repair moved the accepted head")
			}
			if _, found, err := w.broker.AcceptedCommit(ctx, "inc-1", broker.ProposalPhase); err != nil || found {
				t.Fatalf("a refused repair is reported accepted: %v %v", found, err)
			}
			after, _, err := w.broker.AcceptedSourceEvidence(ctx)
			if err != nil || after.Evidence.EffectiveTree != onboarded.Evidence.EffectiveTree {
				t.Fatalf("a refused repair changed the accepted deployment: %+v %v", after.Evidence, err)
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(w.accepted), "workspaces", "candidates")); err == nil {
				entries, _ := os.ReadDir(filepath.Join(filepath.Dir(w.accepted), "workspaces", "candidates"))
				if len(entries) != 0 {
					t.Fatalf("a refused repair left %d staged candidates", len(entries))
				}
			}
		})
	}
}
