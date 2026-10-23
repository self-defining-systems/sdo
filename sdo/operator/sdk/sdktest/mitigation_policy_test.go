package sdktest

import (
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/targetport"
	"testing"
)

func TestUnreplayableMitigationAcceptedWithTypedNote(t *testing.T) {
	c := renameFixture(t)
	ref := c.Bindings.Objects["service"]
	for _, actions := range [][]program.Action{
		{program.ReconcileFromSource{Ref: ref}},
		{program.Rollback{Ref: ref, ToRevision: "accepted"}},
		{program.RestartRollout{Ref: ref}},
		{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: program.FromSource(ref, "spec.ports[0].targetPort")}},
		{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(9999)}, program.ReconcileFromSource{Ref: ref}},
	} {
		result, err := CheckMitigation(targetport.Program{}, fixtureMitigator{actions: actions}, c)
		if err != nil || result.Status != MitigationReplaySkippedUnreplayableEffect || len(result.Effects) == 0 {
			t.Fatalf("best effort result %+v %v", result, err)
		}
	}
	// Plan validation precedes the effect skip, including for mixed plans.
	_, err := CheckMitigation(targetport.Program{}, fixtureMitigator{actions: []program.Action{program.ReconcileFromSource{Ref: ref}, program.SetField{Ref: ref, Path: "metadata.name", Value: testPlanValue("unsafe")}}}, c)
	if err == nil {
		t.Fatal("invalid mixed plan accepted")
	}
	result, err := CheckMitigation(targetport.Program{}, fixtureMitigator{actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)}}}, c)
	if err != nil || result.Status != MitigationReplayPassed {
		t.Fatalf("SetField replay %+v %v", result, err)
	}
	_, err = CheckMitigation(targetport.Program{}, fixtureMitigator{actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(9999)}}}, c)
	if err == nil {
		t.Fatal("fault-preserving SetField accepted")
	}
}
