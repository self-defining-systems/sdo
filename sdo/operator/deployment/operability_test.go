package deployment

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/operability"
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
// receipt records, with and without a committed source change.
func TestCheckOfflineAgreesWithApplyAndVerify(t *testing.T) {
	for name, change := range map[string]bool{"base": false, "with a commit": true} {
		t.Run(name, func(t *testing.T) {
			root, source := depRawFixture(t)
			if change {
				commitWeb2(t, root)
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
				offlineChecks.RenderDigest != receipt.RenderDigest || offlineChecks.EnvDigest != receipt.EnvDigest {
				t.Fatalf("offline identities %+v differ from the receipt %+v", offlineChecks, receipt)
			}
			if offlineChecks.EffectiveTree == "" {
				t.Fatal("the offline checks name the effective tree")
			}
		})
	}
}

// A repository-form declaration no rendered container uses is reported to the
// advance, which drops it; an exact-tag one is not (it stays a hold).
func TestCheckOfflineReportsUnusedRepositoryDeclarations(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	plan := func(image string) *contracts.DeploymentPlan {
		return &contracts.DeploymentPlan{Source: source, Images: []*contracts.ImageSource{{Image: image, SourceId: depImages, Config: depAny(t, &emptypb.Empty{})}}}
	}
	got, err := deployer.CheckOffline(context.Background(), root, plan("registry.example/never"))
	if err != nil || len(got.UnusedImages) != 1 || got.UnusedImages[0] != "registry.example/never" {
		t.Fatalf("unused = %v, err = %v", got.UnusedImages, err)
	}
	got, err = deployer.CheckOffline(context.Background(), root, plan("registry.example/never:1"))
	if err != nil || len(got.UnusedImages) != 0 {
		t.Fatalf("an exact-tag declaration was reported unused: %v, err = %v", got.UnusedImages, err)
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
		"image declared but unused": {&contracts.DeploymentPlan{Source: source, Images: []*contracts.ImageSource{{Image: "never:1", SourceId: depImages, Config: depAny(t, &emptypb.Empty{})}}}, "O2.images"},
		"unknown renderer":          {&contracts.DeploymentPlan{Source: &contracts.DeploymentSource{RendererId: "nope"}}, "O1.render"},
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
}

// A tree with no deployment files fails O1.render offline, owned by the
// deployer: case A's first failure costs no cluster or build time.
func TestCheckOfflineReportsATreeWithNoDeploymentFiles(t *testing.T) {
	root, _ := depRawFixture(t)
	testutil.Git(t, root, "rm", "-rq", "k8s")
	testutil.Git(t, root, "commit", "-qm", "no deployment files")
	deployer, fake := depDeployer(t, root, &clusterFake{})
	got, err := deployer.CheckOffline(context.Background(), root, nil)
	if err != nil {
		t.Fatal(err)
	}
	checks := checksByID(t, got.Checks)
	c := checks["O1.render"]
	if c.Status != operability.StatusFail || operability.Route(c.Cause) != operability.OwnerDeployer || !strings.Contains(c.Detail, "no deployment files") {
		t.Fatalf("O1.render = %+v", c)
	}
	if len(fake.Calls()) != 0 {
		t.Fatalf("an offline check built images: %v", fake.Calls())
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
