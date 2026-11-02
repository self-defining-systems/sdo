package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

)

const syntheticWorkingProgram = `package repair
import("context";"sdo.dev/operator/sdk/program")
type Program struct{}
func(Program) Spec()program.ProgramSpec{return program.ProgramSpec{ID:"service-targetport-mismatch",Params:[]program.ParamSpec{{Name:"service",Kind:"Service",Required:true},{Name:"port",ValueKind:program.IntValue,Default:8080}}}}
func(Program) Diagnose(_ context.Context,v program.ReadView,b program.Bindings)(program.Verdict,error){for _,s:=range v.Services(){if s.Name==b.Objects["service"].Name && s.Spec.Ports[0].TargetPort.IntVal==8080{return program.Verdict{Result:program.NoMatch},nil}};return program.Verdict{Result:program.Match},nil}
func(Program) Mitigate(_ context.Context,v program.ReadView,b program.Bindings)(program.Plan,error){return program.Plan{Rationale:"VERSION",Actions:[]program.Action{program.SetField{Ref:b.Objects["service"],Path:"spec.ports[0].targetPort",Value:program.FromParam("port"),Reason:"synthetic repair"}}},nil}
`

func TestWorkingProgramRevisionReceiptsStayBounded(t *testing.T) {
	response := ToolResponse{Command: "mitigate", Summary: "synthetic many-program response", ProgramRevisions: map[string]string{}}
	for i := 0; i < 32; i++ {
		response.ProgramRevisions[fmt.Sprintf("playbook-%02d-%s", i, strings.Repeat("a", 100))] = workingTreeRevisionPrefix + strings.Repeat("b", 64)
	}
	recorded := recordedToolResponseJSON(response, nil)
	if len(recorded) > maxRecordedToolResponse {
		t.Fatalf("revision evidence overflowed bounded record: %d", len(recorded))
	}
	var counts struct {
		Truncated             bool `json:"truncated"`
		ProgramRevisionsCount int  `json:"program_revisions_count"`
	}
	if err := json.Unmarshal(recorded, &counts); err != nil {
		t.Fatal(err)
	}
	if !counts.Truncated || counts.ProgramRevisionsCount != 32 {
		t.Fatalf("revision omission must be explicit: %s", recorded)
	}
}

