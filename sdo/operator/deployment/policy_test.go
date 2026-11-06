package deployment

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/render/rendertest"
)

// The install-time policy binds the production deployer, not an advisory
// check the agent chooses flags for: an off-allowlist image is refused before
// any build or apply.
func TestDeployerEnforcesTheInstallRegistryAllowlist(t *testing.T) {
	root, source := depRawFixture(t)
	cluster := &clusterFake{}
	deployer, fake := depDeployer(t, root, cluster)
	deployer.Policy = &contracts.DeploymentPolicy{AllowedRegistries: []string{"ghcr.io"}}
	_, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	var policy *PolicyError
	if !errors.As(err, &policy) || len(policy.Checks) != 1 || policy.Checks[0].ID != "O6.allowlist" {
		t.Fatalf("err = %v, want a PolicyError on O6.allowlist", err)
	}
	if ClassifyDeploymentError(err) != operability.CauseAllowlist {
		t.Fatalf("cause = %q", ClassifyDeploymentError(err))
	}
	if !strings.Contains(err.Error(), "web:1") {
		t.Fatalf("error does not name the image: %v", err)
	}
	if len(cluster.events) != 0 || len(fake.Calls()) != 0 {
		t.Fatalf("policy violation reached the cluster or the image sources: %v %v", cluster.events, fake.Calls())
	}
}

func TestDeployerWithAnAllowedRegistryRecordsThePolicyInTheReceipt(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	deployer.Policy = &contracts.DeploymentPolicy{AllowedRegistries: []string{"docker.io"}}
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Policy == nil || receipt.Policy == deployer.Policy || !proto.Equal(receipt.Policy, deployer.Policy) {
		t.Fatalf("receipt policy = %v, want an equal clone of %v", receipt.Policy, deployer.Policy)
	}
}

// Existing installs carry no policy: today's behavior, documented on
// DeploymentPolicy. Nothing is refused and the receipt records none.
func TestDeployerWithoutAPolicyRefusesNothing(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatalf("default policy refused a deployment: %v", err)
	}
	if receipt.Policy != nil {
		t.Fatalf("receipt policy = %v, want none", receipt.Policy)
	}
}

func TestDeployerEnforcesInstallScopeGrants(t *testing.T) {
	root := depCommit(t, rendertest.MapFS{"k8s/web.yaml": depDeployment,
		"k8s/foreign.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: other, namespace: elsewhere}\n",
		"k8s/role.yaml":    "apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata: {name: reader}\n"})
	source := depSource(t, "raw", &contracts.RawRenderConfig{Paths: []string{"k8s"}})
	for name, tc := range map[string]struct {
		policy  *contracts.DeploymentPolicy
		refused bool
	}{
		"none":               {nil, false},
		"blocking":           {&contracts.DeploymentPolicy{ScopeBlocking: true}, true},
		"namespace only":     {&contracts.DeploymentPolicy{ScopeBlocking: true, GrantedNamespaces: []string{"elsewhere"}}, true},
		"kind only":          {&contracts.DeploymentPolicy{ScopeBlocking: true, GrantedClusterKinds: []string{"ClusterRole"}}, true},
		"namespace and kind": {&contracts.DeploymentPolicy{ScopeBlocking: true, GrantedNamespaces: []string{"elsewhere"}, GrantedClusterKinds: []string{"ClusterRole"}}, false},
		"other namespace":    {&contracts.DeploymentPolicy{ScopeBlocking: true, GrantedNamespaces: []string{"nope"}, GrantedClusterKinds: []string{"ClusterRole"}}, true},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := &clusterFake{}
			deployer, _ := depDeployer(t, root, cluster)
			deployer.Policy = tc.policy
			_, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
			var policy *PolicyError
			switch {
			case tc.refused && (!errors.As(err, &policy) || policy.Checks[0].ID != "O6.scope"):
				t.Fatalf("err = %v, want a PolicyError on O6.scope", err)
			case !tc.refused && err != nil:
				t.Fatal(err)
			}
			if tc.refused && len(cluster.events) != 0 {
				t.Fatalf("applied despite the policy: %v", cluster.events)
			}
		})
	}
}

// A restart re-checks the accepted deployment against the policy now in force:
// tightening the policy cannot leave a violating deployment accepted.
func TestVerifyAcceptedEnforcesTheCurrentPolicy(t *testing.T) {
	root, source := depRawFixture(t)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, depPlan(t, source, depRawWeb))
	if err != nil {
		t.Fatal(err)
	}
	commit := depHead(t, root)
	deployer.Policy = &contracts.DeploymentPolicy{AllowedRegistries: []string{"docker.io"}}
	if _, err := deployer.VerifyAccepted(context.Background(), commit, depNamespace, receipt); err != nil {
		t.Fatalf("a deployment inside the policy was refused: %v", err)
	}
	deployer.Policy = &contracts.DeploymentPolicy{AllowedRegistries: []string{"ghcr.io"}}
	_, err = deployer.VerifyAccepted(context.Background(), commit, depNamespace, receipt)
	var policy *PolicyError
	if !errors.As(err, &policy) || policy.Checks[0].ID != "O6.allowlist" {
		t.Fatalf("err = %v, want a PolicyError on O6.allowlist", err)
	}
	// The start-up check is the same policy decision without the cluster, so a
	// restart with no health refresh due still finds the violation.
	if err := deployer.CheckAcceptedPolicy(context.Background(), commit, depNamespace, receipt); !errors.As(err, &policy) {
		t.Fatalf("CheckAcceptedPolicy err = %v, want a PolicyError", err)
	}
	deployer.Policy = &contracts.DeploymentPolicy{GrantedNamespaces: []string{"x"}}
	if _, err := deployer.VerifyAccepted(context.Background(), commit, depNamespace, receipt); err == nil {
		t.Fatal("an invalid policy was accepted at start")
	}
}
