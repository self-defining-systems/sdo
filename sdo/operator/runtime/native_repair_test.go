package runtime

import (
	"context"
	"errors"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
)

// repairFakeBroker answers the lifecycle.RepairBroker calls of one repair:
// no deployment is recorded, so a deployment-input change is published
// undeployed; a proposal without a source repair is none.
type repairFakeBroker struct {
	lifecycle.RepairBroker
	paths []string
}

func (b repairFakeBroker) ChangedPaths(context.Context, string, broker.Phase) ([]string, error) {
	return b.paths, nil
}

func (b repairFakeBroker) Publish(_ context.Context, id string, _ broker.Actor, phase broker.Phase) (broker.Receipt, error) {
	return broker.Receipt{IncidentID: id, Phase: phase, Commit: "accepted"}, nil
}

func (b repairFakeBroker) AcceptedSourceEvidence(context.Context) (broker.AcceptedSource, bool, error) {
	return broker.AcceptedSource{}, false, nil
}

type unusedReadiness struct {
	lifecycle.SourceReadinessVerifier
}
type unusedVerifier struct{ lifecycle.CandidateVerifier }

func TestDeploymentRepairerMapsEveryRepairOutcome(t *testing.T) {
	root := t.TempDir()
	if err := memory.WriteGoal(root, "toy", "serve every request", nil); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		paths []string
		want  SourceRepair
	}{
		{"no repair", nil, SourceRepair{}},
		{"no deployment input", []string{".sdo/deployment/plan.yaml"}, SourceRepair{Commit: "accepted"}},
		{"no recorded deployment", []string{".sdo/deployment/patches/0001-x.patch"}, SourceRepair{Commit: "accepted", Undeployed: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repairer := DeploymentRepairer{
				Deployer:  lifecycle.RepairDeployer{Broker: repairFakeBroker{paths: tc.paths}, Readiness: unusedReadiness{}, Verifier: unusedVerifier{}},
				Namespace: "toy", AppRoot: root,
			}
			if got, err := repairer.Repair(context.Background(), "incident-1"); err != nil || got != tc.want {
				t.Fatalf("Repair = %+v, %v, want %+v", got, err, tc.want)
			}
		})
	}
}

func TestDeploymentRepairerNeedsTheAcceptedObjective(t *testing.T) {
	repairer := DeploymentRepairer{Deployer: lifecycle.RepairDeployer{Broker: repairFakeBroker{}, Readiness: unusedReadiness{}, Verifier: unusedVerifier{}}, Namespace: "toy", AppRoot: t.TempDir()}
	if _, err := repairer.Repair(context.Background(), "incident-1"); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("Repair without a goal = %v", err)
	}
	var _ SourceRepairer = repairer
}

func TestPlainPublicationNeedsADeclaredReason(t *testing.T) {
	if _, err := (PlainPublication{Repository: repairFakePublisher{}}).Repair(context.Background(), "incident-1"); err == nil {
		t.Fatal("a plain publication without a reason was accepted")
	}
	got, err := PlainPublication{Repository: repairFakePublisher{paths: []string{".sdo/deployment/values/values.yaml"}}, Reason: PublishesUndeployedFrozenMemory}.Repair(context.Background(), "incident-1")
	if err != nil || got != (SourceRepair{Commit: "accepted", Undeployed: true}) {
		t.Fatalf("Repair = %+v, %v", got, err)
	}
	got, err = PlainPublication{Repository: repairFakePublisher{paths: []string{".sdo/playbooks/p/program.go"}}, Reason: PublishesUndeployedFrozenMemory}.Repair(context.Background(), "incident-1")
	if err != nil || got != (SourceRepair{Commit: "accepted"}) {
		t.Fatalf("Repair of a pure .sdo change = %+v, %v", got, err)
	}
}

type repairFakePublisher struct{ paths []string }

func (p repairFakePublisher) ChangedPaths(context.Context, string, broker.Phase) ([]string, error) {
	return p.paths, nil
}

func (p repairFakePublisher) Publish(_ context.Context, id string, _ broker.Actor, phase broker.Phase) (broker.Receipt, error) {
	return broker.Receipt{IncidentID: id, Phase: phase, Commit: "accepted"}, nil
}
