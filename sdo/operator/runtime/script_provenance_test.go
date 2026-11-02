package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// These are REV44's two constructions. The guard must inspect returned values,
// including pointers and helper output, rather than source spellings.
func TestMitigationPlanRefusesUnprovenancedScriptValues(t *testing.T) {
	for _, construction := range []string{"field-built", "reassigned", "pointer", "helper"} {
		t.Run(construction, func(t *testing.T) {
			m := funcMitigator{id: "script", plan: func(program.ReadView, program.Bindings) (program.Plan, error) {
				var step program.RunScript
				if construction == "reassigned" {
					step = program.RunScript{Script: "echo recorded"}
				}
				step.Script = "echo unrecorded-script"
				var action program.Action = step
				if construction == "pointer" {
					action = &step
				}
				if construction == "helper" {
					action = scriptFromHelper()
				}
				return program.Plan{Actions: []program.Action{action}}, nil
			}}
			_, err := BuildMitigationPlan(context.Background(), m, &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}, program.Bindings{}, time.Second)
			if err == nil || !strings.Contains(err.Error(), "script_unprovenanced") {
				t.Fatalf("unrecorded returned bytes accepted: %v", err)
			}
		})
	}
}

func scriptFromHelper() program.Action { return program.RunScript{Script: "echo unrecorded-script"} }

type provenancePipelineProgram struct {
	pipelinePortProgram
	construction string
	calls        *int
}

func (p provenancePipelineProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	*p.calls++
	var step program.RunScript
	if p.construction != "field-built" {
		step = program.RunScript{Script: "echo recorded"}
	}
	if p.construction != "provenanced" {
		step.Script = "echo unrecorded-script"
	}
	return program.Plan{Actions: []program.Action{step}}, nil
}
