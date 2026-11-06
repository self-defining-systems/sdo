package deployment

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/patchset"
)

func checksByID(t *testing.T, checks []operability.Check) map[string]operability.Check {
	t.Helper()
	out := map[string]operability.Check{}
	for _, c := range checks {
		out[c.ID] = c
	}
	return out
}

func failed(checks []operability.Check) []string {
	var ids []string
	for _, c := range checks {
		if c.Status == operability.StatusFail {
			ids = append(ids, c.ID)
		}
	}
	return ids
}

// The offline checks are computed from the same tree, environment and shared
// functions as apply: for a passing plan every digest equals the one the
// receipt records, with and without a patch series.
func TestCheckOfflineAgreesWithApplyAndVerify(t *testing.T) {
	for name, patch := range map[string]bool{"no patches": false, "with a patch": true} {
		t.Run(name, func(t *testing.T) {
			root, source := depRawFixture(t)
			if patch {
				patchWeb(t, root)
			}
			plan := depPlan(t, source, depRawWeb)
			deployer, _ := depDeployer(t, root, &clusterFake{})
			offlineChecks, err := deployer.CheckOffline(context.Background(), root, plan)
			if err != nil {
				t.Fatal(err)
			}
			if bad := failed(offlineChecks.Checks); len(bad) != 0 {
				t.Fatalf("offline failures %v: %+v", bad, offlineChecks.Checks)
			}
			receipt, err := deployer.ApplyAndVerify(context.Background(), root, plan)
			if err != nil {
				t.Fatal(err)
			}
			if offlineChecks.BaseTree != receipt.BaseTree || offlineChecks.EffectiveTree != receipt.EffectiveTree ||
				offlineChecks.PatchesDigest != receipt.PatchesDigest || offlineChecks.RenderDigest != receipt.RenderDigest || offlineChecks.EnvDigest != receipt.EnvDigest {
				t.Fatalf("offline identities %+v differ from the receipt %+v", offlineChecks, receipt)
			}
			if patch && offlineChecks.EffectiveTree == offlineChecks.BaseTree {
				t.Fatal("a patched tree must have its own effective tree")
			}
		})
	}
}

// Every plan ApplyAndVerify rejects before touching the cluster has a failing
// offline check, so the gate catches it first; an image-resolution fault is
// not knowable offline and is not claimed to be.
func TestCheckOfflineFailsWhereApplyRejectsBeforeTheCluster(t *testing.T) {
	root, source := depRawFixture(t)
	for name, tc := range map[string]struct {
		plan *contracts.DeploymentPlan
		want string
	}{
		"no expected workloads":     {&contracts.DeploymentPlan{Source: source}, "O1.workloads"},
		"unexpected workload":       {depPlan(t, source, depRawWeb, "Deployment/demo/ghost"), "O1.workloads"},
		"missing workload":          {depPlan(t, source, "Deployment/demo/ghost"), "O1.workloads"},
		"duplicate expected":        {depPlan(t, source, depRawWeb, depRawWeb), "O1.workloads"},
		"image declared but unused": {&contracts.DeploymentPlan{Source: source, ExpectedWorkloadIds: []string{depRawWeb}, Images: []*contracts.ImageSource{{Image: "never:1", SourceId: depImages, Config: depAny(t, &emptypb.Empty{})}}}, "O2.images"},
		"unknown renderer":          {&contracts.DeploymentPlan{Source: &contracts.DeploymentSource{RendererId: "nope"}, ExpectedWorkloadIds: []string{depRawWeb}}, "O1.render"},
	} {
		t.Run(name, func(t *testing.T) {
			deployer, _ := depDeployer(t, root, &clusterFake{})
			if _, err := deployer.ApplyAndVerify(context.Background(), root, tc.plan); err == nil {
				t.Fatal("apply accepted the plan; the case is not a rejection")
			}
			got, err := deployer.CheckOffline(context.Background(), root, tc.plan)
			if err != nil {
				t.Fatal(err)
			}
			if bad := failed(got.Checks); len(bad) == 0 || bad[0] != tc.want {
				t.Fatalf("failing checks = %v, want %s first: %+v", bad, tc.want, got.Checks)
			}
		})
	}
	deployer, _ := depDeployer(t, root, &clusterFake{})
	if _, err := deployer.CheckOffline(context.Background(), root, &contracts.DeploymentPlan{ExpectedWorkloadIds: []string{depRawWeb}}); err == nil {
		t.Fatal("a plan with no source is a wiring error, like apply's")
	}
}

func TestCheckOfflineReportsAPatchSeriesThatDoesNotApply(t *testing.T) {
	root, source := depRawFixture(t)
	patchWeb(t, root)
	// A later change moves the patched line, so the committed patch conflicts.
	file := filepath.Join(root, "k8s", "web.yaml")
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(strings.ReplaceAll(string(data), "web:1", "web:3")), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, root, "commit", "-aqm", "move the line")
	deployer, _ := depDeployer(t, root, &clusterFake{})
	got, err := deployer.CheckOffline(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	checks := checksByID(t, got.Checks)
	c := checks["O1.patches"]
	if c.Status != operability.StatusFail || c.Cause != operability.CausePatch || len(c.Paths) == 0 || !strings.HasPrefix(c.Paths[0], patchset.Directory) {
		t.Fatalf("O1.patches = %+v", c)
	}
	if got.EffectiveTree != "" || got.RenderDigest != "" || got.BaseTree == "" {
		t.Fatalf("identities that could not be computed must stay empty: %+v", got)
	}
	if checks["O1.render"].Status != operability.StatusSkipped {
		t.Fatalf("render ran after a patch failure: %+v", checks["O1.render"])
	}
}

func TestCheckOfflineAppliesTheInstallPolicy(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	plan := depPlan(t, source, depRawWeb)
	got, err := deployer.CheckOffline(context.Background(), root, plan)
	if err != nil {
		t.Fatal(err)
	}
	if c := checksByID(t, got.Checks)["O6.allowlist"]; c.Status != operability.StatusSkipped {
		t.Fatalf("no allowlist configured: %+v", c)
	}
	deployer.Policy = &contracts.DeploymentPolicy{AllowedRegistries: []string{"ghcr.io"}}
	got, err = deployer.CheckOffline(context.Background(), root, plan)
	if err != nil {
		t.Fatal(err)
	}
	if c := checksByID(t, got.Checks)["O6.allowlist"]; c.Status != operability.StatusFail || c.Cause != operability.CauseAllowlist {
		t.Fatalf("image outside the allowlist: %+v", c)
	}
}
