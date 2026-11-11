package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func procedureView() program.ReadView {
	m := program.AppMap{
		Components: []program.AppComponent{{Name: "front", Workload: "Deployment/front"}},
		Procedures: []program.Procedure{{ID: "front-reindex", Component: "front", Container: "web", Command: []string{"front-admin", "reindex"}, Effect: program.ProcedureRepair, Source: "docs/ops.md"}},
	}
	return sdktest.Snapshot{NamespaceName: "app", ApplicationMap: &m}.RuntimeReadView()
}

// A learned program's procedure step takes its authority from the accepted
// application map, so it needs no original receipt; any other script still
// does.
func TestProcedureScriptsNeedNoOriginalReceipt(t *testing.T) {
	view := procedureView()
	action, err := program.RunProcedure(view, "front-reindex", nil)
	if err != nil {
		t.Fatal(err)
	}
	plan := program.Plan{Actions: []program.Action{action}}
	if err := checkPlanScriptProvenance(context.Background(), "pb", plan, view, nil); err != nil {
		t.Fatalf("accepted procedure script refused: %v", err)
	}
	edited := action.(program.RunScript)
	edited.Script = strings.Replace(edited.Script, "'reindex'", "'drop-all'", 1)
	var refused *ScriptUnprovenancedError
	if err := checkPlanScriptProvenance(context.Background(), "pb", program.Plan{Actions: []program.Action{edited}}, view, nil); !errors.As(err, &refused) {
		t.Fatalf("edited procedure script err = %v", err)
	}
	plain := program.RunScript{Script: "echo hi\n"}
	if err := checkPlanScriptProvenance(context.Background(), "pb", program.Plan{Actions: []program.Action{plain}}, view, nil); !errors.As(err, &refused) {
		t.Fatalf("plain script err = %v", err)
	}
}

// Each attempt records the procedure and effect class of the action it runs.
func TestPlanReportsRecordTheProcedureEffectClass(t *testing.T) {
	action, err := program.RunProcedure(procedureView(), "front-reindex", nil)
	if err != nil {
		t.Fatal(err)
	}
	reports := newPlanReports(program.Plan{Actions: []program.Action{program.DeletePod{}, action}})
	if reports[0].Procedure != nil {
		t.Fatalf("a non-procedure action records %+v", reports[0].Procedure)
	}
	if run := reports[1].Procedure; run == nil || run.ID != "front-reindex" || run.Effect != program.ProcedureRepair {
		t.Fatalf("procedure report = %+v", reports[1].Procedure)
	}
}
