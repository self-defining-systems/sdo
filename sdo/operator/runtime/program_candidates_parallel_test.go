package runtime

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"sdo.dev/operator/runtime/internal/lifecycleio"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// slowProgram diagnoses after its own delay; it matches when match is set.
type slowProgram struct {
	id    string
	delay time.Duration
	match bool
}

func (p slowProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}

func (p slowProgram) Diagnose(ctx context.Context, _ program.ReadView, _ program.Bindings) (program.Verdict, error) {
	select {
	case <-lifecycleio.After(p.delay):
	case <-ctx.Done():
		return program.Verdict{}, ctx.Err()
	}
	if p.match {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

// The opening diagnosis runs every candidate's Diagnose at once: three
// programs of 3 s, 2 s and 1 s take 3 s, not 6 s, and the runs come back in
// the same order whatever order they finished in.
func TestOpeningDiagnosesRunInParallelAndStayDeterministic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var orders [][]string
		for _, delays := range [][3]time.Duration{{3 * time.Second, 2 * time.Second, time.Second}, {time.Second, 2 * time.Second, 3 * time.Second}} {
			programs := []program.Diagnoser{
				slowProgram{id: "first", delay: delays[0]}, slowProgram{id: "second", delay: delays[1], match: true}, slowProgram{id: "third", delay: delays[2]},
			}
			registry, err := program.NewRegistry(programs, nil)
			if err != nil {
				t.Fatal(err)
			}
			finding := sdk.Finding{DetectorID: "store-state", RuleID: "drift", Status: sdk.FindingActive,
				PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"},
				Playbooks:       []string{"first", "second", "third"}}
			started := lifecycleio.Now()
			runs := SelectKeyedProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{}, []sdk.Finding{finding}, DefaultDiagnosisDeadline, KeyedMatching{Stage: true})
			if took := lifecycleio.Since(started); took != 3*time.Second {
				t.Fatalf("diagnosis took %s, want the slowest program's 3s", took)
			}
			var order []string
			for _, run := range runs {
				order = append(order, run.ProgramID+":"+string(run.Verdict.Result))
			}
			orders = append(orders, order)
		}
		if len(orders[0]) != 3 || !slices.Equal(orders[0], orders[1]) {
			t.Fatalf("run orders %v and %v differ", orders[0], orders[1])
		}
	})
}
