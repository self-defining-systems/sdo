package program_test

import (
	"context"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestProgramDetectorKeepsDeclaredPlaybookIDs(t *testing.T) {
	p := diagnostic{fn: func(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
		return program.Verdict{Result: program.Match}, nil
	}}
	locate := func(sdk.DetectionContext) []program.Bindings {
		return []program.Bindings{{Objects: program.ObjectBindings{"service": serviceRef}}}
	}
	spec := sdk.DetectorSpec{ID: "incident-port-fault", Playbooks: []string{"repair-port"}}
	d := program.DetectorFromProgram(p, locate, spec)
	findings, err := d.Detect(context.Background(), sdktest.Snapshot{})
	if err != nil || len(findings) != 1 {
		t.Fatalf("findings %v: %v", findings, err)
	}
	if !reflect.DeepEqual(findings[0].Playbooks, spec.Playbooks) {
		t.Fatalf("playbooks = %v, want %v", findings[0].Playbooks, spec.Playbooks)
	}
	findings[0].Playbooks[0] = "changed"
	if d.Spec().Playbooks[0] != "repair-port" {
		t.Fatal("finding aliases detector registration")
	}
}
