package runtime

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk/program"
)

type scriptPlaybook struct {
	verdict program.Result
}

func (p scriptPlaybook) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "script-fix"} }

func (p scriptPlaybook) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: p.verdict, CauseClass: "other"}, nil
}

func (p scriptPlaybook) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.RunScript{Script: "true", Reason: "repair synthetic fault"}}}, nil
}

type countingScriptClient struct {
	TypedPlanClient
	runs    int
	onStart func()
}

func (c *countingScriptClient) ScriptReady() error { return nil }

func (c *countingScriptClient) RunScript(_ context.Context, request ScriptExecutionRequest) (ScriptExecutionResult, error) {
	c.runs++
	if c.onStart != nil {
		c.onStart()
	}
	exit := int32(0)
	return ScriptExecutionResult{ApplicationID: request.ApplicationID, JobName: "synthetic-job", ScriptDigest: program.ScriptDigest(request.Script), ExitCode: &exit}, nil
}

func stageScriptRegistry(t *testing.T, verdict program.Result) program.ProgramRegistry {
	t.Helper()
	playbook := scriptPlaybook{verdict: verdict}
	registry, err := program.NewRegistry([]program.Diagnoser{playbook}, []program.Mitigator{playbook})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestStageUnreceiptedScriptRequiresMatch(t *testing.T) {
	for _, test := range []struct {
		name    string
		verdict program.Result
		want    int
	}{
		{"Match", program.Match, 1},
		{"NoMatch", program.NoMatch, 0},
		{"Unknown", program.Unknown, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			world, stage, state := keyedStageLoop(t, nil)
			loop := stage.NewLoop(state.Request)
			loop.Programs = stageScriptRegistry(t, test.verdict)
			client := &countingScriptClient{TypedPlanClient: TypedPlanClient{Client: world.client, ScriptImage: "synthetic"}}
			loop.Executor.Client = client
			report, err := stage.Run(t.Context(), state)
			if err != nil {
				t.Fatal(err)
			}
			if client.runs != test.want {
				t.Fatalf("script invocations = %d, want %d: %v", client.runs, test.want, report)
			}
		})
	}
}

// Reopen the stage with exactly the journal that existed when the Job
// started, before the first controller could record the script result.
func TestStageRestartDoesNotReapplyInFlightScript(t *testing.T) {
	world, stage, state := keyedStageLoop(t, nil)
	loop := stage.NewLoop(state.Request)
	loop.Programs = stageScriptRegistry(t, program.Match)
	var persisted, atJobStart []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error {
		persisted = payload
		return nil
	}
	script := &countingScriptClient{
		TypedPlanClient: TypedPlanClient{Client: world.client, ScriptImage: "synthetic"},
		onStart:         func() { atJobStart = persisted },
	}
	loop.Executor.Client = script
	if _, err := stage.Run(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	if script.runs != 1 || len(atJobStart) == 0 {
		t.Fatal("expected the script to start after a durable reservation")
	}

	_, restarted, reloaded := keyedStageLoop(t, nil)
	fresh := restarted.NewLoop(reloaded.Request)
	fresh.Programs = stageScriptRegistry(t, program.Match)
	fresh.Executor.Client = script
	restarted.Restore = func(_ context.Context, _ IncidentRequest, loop *ResponderLoop) error {
		return loop.restoreToolSnapshot(atJobStart)
	}
	report, err := restarted.Run(t.Context(), reloaded)
	if err != nil {
		t.Fatal(err)
	}
	if script.runs != 1 || len(report.GetMitigations()) != 1 {
		t.Fatalf("restart repeated a reserved script: %d script invocations", script.runs)
	}
}
