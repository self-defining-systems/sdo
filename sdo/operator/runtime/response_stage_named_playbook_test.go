package runtime

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// rolelessRestart is a role-less playbook: it finds its own subject and
// matches until that Deployment was restarted.
type rolelessRestart struct{ id, target string }

func (p rolelessRestart) Spec() program.ProgramSpec { return program.ProgramSpec{ID: p.id} }

func (p rolelessRestart) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	for _, deployment := range view.Deployments() {
		if deployment.Name == p.target && deployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] == "" {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (p rolelessRestart) Mitigate(_ context.Context, view program.ReadView, _ program.Bindings) (program.Plan, error) {
	for _, deployment := range view.Deployments() {
		if deployment.Name == p.target {
			return program.Plan{Actions: []program.Action{program.RestartRollout{Ref: sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment), Reason: "restart"}}}, nil
		}
	}
	return program.Plan{}, nil
}

func namedStageOrder(t *testing.T, names []string) []string {
	t.Helper()
	_, stage, state := keyedStageLoop(t, nil)
	loop := stage.NewLoop(state.Request)
	generic, cause := rolelessRestart{id: "aaa-generic", target: "api"}, rolelessRestart{id: "zzz-cause", target: "web"}
	registry, err := program.NewRegistry([]program.Diagnoser{generic, cause}, []program.Mitigator{generic, cause})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	for i := range state.Findings {
		state.Findings[i].Playbooks = names
	}
	state.Request.Findings = state.Findings
	loop.Request = state.Request
	loop.findings = state.Findings
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, attempt := range report.Mitigations {
		order = append(order, attempt.PlaybookId)
	}
	return order
}

// Both role-less playbooks Match every finding. The stage tries the one the
// finding names first, ahead of the one that sorts first by ID.
func TestStageTriesTheDetectorNamedPlaybookFirst(t *testing.T) {
	if order := namedStageOrder(t, []string{"zzz-cause"}); len(order) == 0 || order[0] != "zzz-cause" {
		t.Fatalf("stage order %v, want the named zzz-cause first", order)
	}
}

// With no name on the finding the order is unchanged: by playbook ID.
func TestStageOrderIsByIDWhenNoFindingNamesAPlaybook(t *testing.T) {
	if order := namedStageOrder(t, nil); len(order) == 0 || order[0] != "aaa-generic" {
		t.Fatalf("stage order %v, want aaa-generic first", order)
	}
}

func TestRolelessPlaybookNamedByAFindingBindsAsHint(t *testing.T) {
	spec := program.ProgramSpec{ID: "zzz-cause"}
	finding := sdk.Finding{Status: sdk.FindingActive, Playbooks: []string{"zzz-cause"}}
	if _, source, ok := findingProgramBindings(spec, finding); !ok || source != BindingSourceHint {
		t.Fatalf("source %q ok %v, want hint", source, ok)
	}
	finding.Playbooks = nil
	if _, source, ok := findingProgramBindings(spec, finding); !ok || source != BindingSourceFinding {
		t.Fatalf("source %q ok %v, want finding", source, ok)
	}
}
