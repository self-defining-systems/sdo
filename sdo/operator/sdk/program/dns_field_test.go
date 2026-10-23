package program_test

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestSourceBackedDNSPolicyField(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "frontend"}
	a := program.SetField{Ref: ref, Path: "spec.template.spec.dnsPolicy", Value: program.FromSource(ref, "spec.template.spec.dnsPolicy"), Reason: "restore cluster resolver policy"}
	if err := program.ValidateResolvedSetField(a, "ClusterFirst"); err != nil {
		t.Fatal(err)
	}
	object := map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"dnsPolicy": "Default", "hostNetwork": false}}}}
	if err := program.ApplySetFieldAction(object, a, "ClusterFirst"); err != nil {
		t.Fatal(err)
	}
	got, err := program.FieldValue(object, a.Path)
	if err != nil || got != "ClusterFirst" {
		t.Fatalf("%v %v", got, err)
	}
	if err := program.ValidateResolvedSetField(a, map[string]any{"policy": "ClusterFirst"}); err == nil {
		t.Fatal("nonscalar DNS policy accepted")
	}
	if err := program.ValidateSetField(program.SetField{Ref: ref, Path: "spec.template.spec.hostNetwork", Value: a.Value}); err == nil {
		t.Fatal("unrelated privilege field permitted")
	}
}
