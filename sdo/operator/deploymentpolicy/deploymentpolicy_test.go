package deploymentpolicy_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deploymentpolicy"
)

func TestNoFlagsIsNoPolicy(t *testing.T) {
	policy, err := deploymentpolicy.FromFlags(deploymentpolicy.Flags{})
	if err != nil || policy != nil {
		t.Fatalf("FromFlags() = %v, %v; want nil (today's behavior)", policy, err)
	}
	if encoded, err := deploymentpolicy.Format(nil); err != nil || encoded != "" {
		t.Fatalf("Format(nil) = %q, %v", encoded, err)
	}
	if parsed, err := deploymentpolicy.Parse(""); err != nil || parsed != nil {
		t.Fatalf("Parse(\"\") = %v, %v", parsed, err)
	}
	if got := deploymentpolicy.Offline(nil); got.ScopeBlocking || len(got.AllowedRegistries) != 0 || len(got.GrantedNamespaces) != 0 || len(got.GrantedClusterKinds) != 0 {
		t.Fatalf("nil policy must grant and limit nothing: %+v", got)
	}
}

func TestPolicyRoundTripsThroughTheControllerFlag(t *testing.T) {
	want, err := deploymentpolicy.FromFlags(deploymentpolicy.Flags{
		AllowedRegistries: "ghcr.io, registry.example:5000", ScopeBlocking: true, GrantedNamespaces: "monitoring", GrantedClusterKinds: "ClusterRole,Namespace",
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := deploymentpolicy.Format(want)
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(encoded, " \n") && !strings.Contains(encoded, "registry.example") {
		t.Fatalf("encoded = %q", encoded)
	}
	got, err := deploymentpolicy.Parse(encoded)
	if err != nil || !proto.Equal(got, want) {
		t.Fatalf("Parse(Format(p)) = %v, %v; want %v", got, err, want)
	}
	offline := deploymentpolicy.Offline(got)
	if !offline.ScopeBlocking || len(offline.AllowedRegistries) != 2 || offline.GrantedNamespaces[0] != "monitoring" || offline.GrantedClusterKinds[1] != "Namespace" {
		t.Fatalf("offline = %+v", offline)
	}
}

func TestInvalidPoliciesAreRefused(t *testing.T) {
	for name, policy := range map[string]*contracts.DeploymentPolicy{
		"uppercase registry":   {AllowedRegistries: []string{"GHCR.io"}},
		"registry with path":   {AllowedRegistries: []string{"ghcr.io/org"}},
		"grant without block":  {GrantedNamespaces: []string{"monitoring"}},
		"kind grant no block":  {GrantedClusterKinds: []string{"ClusterRole"}},
		"empty namespace":      {ScopeBlocking: true, GrantedNamespaces: []string{""}},
		"namespace with comma": {ScopeBlocking: true, GrantedNamespaces: []string{"a,b"}},
	} {
		if err := deploymentpolicy.Validate(policy); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := deploymentpolicy.Parse(`{"allowedRegistries":["ghcr.io"],"surprise":1}`); err == nil {
		t.Error("unknown field accepted")
	}
}
