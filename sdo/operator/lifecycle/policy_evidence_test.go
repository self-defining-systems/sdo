package lifecycle

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The policy a deployment was checked against is recorded with its evidence
// and survives the ledger round trip; evidence without one reads back as none.
func TestReceiptPolicyIsRecordedInTheEvidence(t *testing.T) {
	config, err := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
	if err != nil {
		t.Fatal(err)
	}
	receipt := DeploymentReceipt{
		Source:   &contracts.DeploymentSource{RendererId: "raw", Config: config},
		BaseTree: strings.Repeat("a", 40), EffectiveTree: strings.Repeat("f", 40), PatchesDigest: strings.Repeat("e", 64), ObservedRevision: strings.Repeat("f", 40),
		RenderDigest: strings.Repeat("b", 64), EnvDigest: strings.Repeat("c", 64),
		Images: []string{"app@sha256:" + strings.Repeat("d", 64)}, ResourceIDs: []string{"Deployment/ns/app"}, Ready: true,
		Policy: &contracts.DeploymentPolicy{AllowedRegistries: []string{"ghcr.io"}, ScopeBlocking: true},
	}
	for name, policy := range map[string]*contracts.DeploymentPolicy{"set": receipt.Policy, "none": nil} {
		receipt.Policy = policy
		encoded, err := json.Marshal(deploymentToBroker(receipt))
		if err != nil {
			t.Fatal(err)
		}
		var evidence broker.SourceEvidence
		if err := json.Unmarshal(encoded, &evidence); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := deploymentFromBroker(evidence).Policy; !proto.Equal(got, policy) || (policy == nil) != (got == nil) {
			t.Errorf("%s: policy after round trip = %v, want %v", name, got, policy)
		}
		if policy == nil && strings.Contains(string(encoded), "deployment_policy") {
			t.Errorf("no policy must leave the record unchanged: %s", encoded)
		}
	}
}
