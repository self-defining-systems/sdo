package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestControllerInconclusiveProgramCannotClearAtDeadlineOrClosure(t *testing.T) {
	for _, verdict := range []program.Result{program.NeedsJudgement, program.Unknown} {
		for _, interruptedClear := range []bool{false, true} {
			name := string(verdict)
			if interruptedClear {
				name += "/interrupted-clear"
			}
			t.Run(name, func(t *testing.T) {
				start := time.Unix(100, 0).UTC()
				c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
				if err != nil {
					t.Fatal(err)
				}
				result := program.Match
				registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{
					spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}},
					run: func(context.Context, program.Bindings) program.Verdict {
						if result == program.NeedsJudgement {
							return program.Verdict{Result: result, Question: "which port is intended?"}
						}
						return program.Verdict{Result: result}
					},
				}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				c.ConfigureDrift(DriftControllerConfig{Programs: registry, ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1, MinDuration: 10 * time.Second}, View: func(sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }})
				run := ProgramRun{ProgramID: "targetport", Bindings: program.Bindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}, Verdict: program.Verdict{Result: program.Match}}
				c.observeProgramRuns(start, nil, []ProgramRun{run})
				c.confirmPrograms(context.Background(), start.Add(10*time.Second), &sdktest.ReplayReadView{})
				findings := c.tracker.ActiveFindings([]string{"drift.targetport"}, nil)
				if len(findings) != 1 {
					t.Fatal("match never activated")
				}
				c.currentIncidentRequest = &IncidentRequest{IncidentID: "incident", Findings: findings}
				c.incidentFindingKeys = findingKeys(findings)
				c.incidentOpen = true
				c.responderCompletedAt = start.Add(10 * time.Second)
				if interruptedClear {
					run.Verdict.Result = program.NoMatch
					c.observeProgramRuns(start.Add(11*time.Second), nil, []ProgramRun{run})
				}
				result = verdict
				c.refreshRetainedDrift(context.Background(), start.Add(12*time.Second), sdktest.Snapshot{})
				c.refreshRetainedDrift(context.Background(), start.Add(23*time.Second), sdktest.Snapshot{})
				if !c.tracker.HasActiveDetector("drift.targetport") {
					t.Fatal("inconclusive diagnosis cleared active binding across deadline")
				}
				if _, clear := c.finalVerificationStates(); clear {
					t.Fatal("inconclusive diagnosis passed incident closure gate")
				}
				// Even explicit NoMatch must start a new clearing streak.
				result = program.NoMatch
				c.refreshRetainedDrift(context.Background(), start.Add(24*time.Second), sdktest.Snapshot{})
				if !c.tracker.HasActiveDetector("drift.targetport") {
					t.Fatal("inconclusive observation preserved an earlier clearing streak")
				}
				c.refreshRetainedDrift(context.Background(), start.Add(34*time.Second), sdktest.Snapshot{})
				if _, clear := c.finalVerificationStates(); !clear {
					t.Fatal("sustained explicit NoMatch did not permit closure")
				}
			})
		}
	}
}
