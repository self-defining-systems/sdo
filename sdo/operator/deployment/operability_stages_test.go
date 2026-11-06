package deployment

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operability/offline"
)

// Each operability check ID has one producer per report stage. The offline
// gate and the live phase both evaluate O1.apis against a recorded discovery;
// a stage report built from the REAL producers of a deployment with discovery
// (the case that onboarded GitLab) must carry every ID once and validate, for
// every combination of stage assemblers: deployment (offline + live), advance
// (offline + live + detectors) and complete candidate (live + memory + detectors).
func TestStageReportsCarryEachCheckIDOnce(t *testing.T) {
	root, source := depRawFixture(t)
	plan := depPlan(t, source, depRawWeb)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	deployer.Environment = &contracts.RenderEnvironment{KubeVersion: "v1.34.0", ApiVersions: []string{"apps/v1", "apps/v1/Deployment", "v1", "v1/ConfigMap", "v1/Service", "v1/Secret"}}
	ctx := context.Background()
	off, err := deployer.CheckOffline(ctx, root, plan)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := deployer.ApplyAndVerify(ctx, root, plan)
	if err != nil {
		t.Fatal(err)
	}
	live, err := deployer.CheckLive(ctx, root, plan, receipt, nil)
	if err != nil {
		t.Fatal(err)
	}
	if statusOf(off.Checks, "O1.apis").Status != operability.StatusPass || statusOf(live, "O1.apis").Status != operability.StatusPass {
		t.Fatal("the fixture must evaluate O1.apis in both phases, or this test does not cover the overlap")
	}
	detectors := operability.Check{ID: "O4.detectors", Phase: operability.PhaseLive, Status: operability.StatusPass, Detail: "clear"}
	memory := offline.CheckMemory(offline.Memory{OwnershipChecked: true})
	subject := operability.Subject{AcceptedCommit: "c", BaseTree: off.BaseTree, EffectiveTree: off.EffectiveTree, PatchesDigest: off.PatchesDigest, RenderDigest: off.RenderDigest, EnvDigest: off.EnvDigest, DiagnosticsTree: "d"}

	stages := map[operability.Stage][]operability.Check{
		operability.StageDeployment: operability.Combine(off.Checks, live),
		operability.StageAdvance:    operability.Combine(operability.Combine(off.Checks, live), []operability.Check{detectors}),
		operability.StageComplete:   operability.Combine(operability.Combine(live, memory), []operability.Check{detectors}),
	}
	for stage, checks := range stages {
		report, err := operability.New(stage, subject).With(checks...)
		if err != nil {
			t.Fatal(err)
		}
		if err := report.Validate(); err != nil {
			t.Errorf("stage %s: %v", stage, err)
		}
	}
	if got := statusOf(stages[operability.StageDeployment], "O1.apis"); got.Phase != operability.PhaseLive {
		t.Errorf("O1.apis = %+v, want the live result to supersede the offline one", got)
	}
}
