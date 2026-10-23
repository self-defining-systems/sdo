package builder

import (
	"context"
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestUnreplayableMitigatorPassesDiagnosisGateWithTypedNote(t *testing.T) {
	root := programFixture(t)
	path := filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch/mitigate.go")
	source := `package targetport
import ("context";"sdo.dev/operator/sdk/program")
func (p Program) Mitigate(ctx context.Context,v program.ReadView,b program.Bindings)(program.Plan,error){
 verdict,err:=p.Diagnose(ctx,v,b);if err!=nil || verdict.Result!=program.Match{return program.Plan{},err}
 return program.Plan{Actions:[]program.Action{program.ReconcileFromSource{Ref:b.Objects["service"]}}},nil
}
`
	if err := os.WriteFile(path, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	notes, err := w.MitigationReplayResults()
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 1 || notes[0].Status != sdktest.MitigationReplaySkippedUnreplayableEffect || notes[0].ProgramID != "service-targetport-mismatch" || notes[0].CaseID != "targetport-mismatch" {
		t.Fatalf("typed replay notes: %+v", notes)
	}
}
