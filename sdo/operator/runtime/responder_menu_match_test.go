package runtime

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// matchOptions lists the program options; the menu also carries generic actions.
func matchOptions(response ToolResponse, programID string) []string {
	var ids []string
	for _, option := range response.Menu.GetOptions() {
		if option.ProgramId == programID {
			ids = append(ids, option.OptionId)
		}
	}
	return ids
}

func TestMenuMatchOffersOnlyReadyCoverageMatchesOfThePlaybook(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Request.PlaybookCoverage = &contracts.PlaybookCoverage{Matches: []*contracts.PlaybookMatch{
		{PlaybookId: "routing-then-restart", CanMitigate: true, Bindings: []*contracts.Binding{
			{Name: "service", Object: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "backend"}},
			{Name: "deployment", Object: &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "frontend"}},
		}},
		{PlaybookId: "restart-only", CanMitigate: false},
	}}
	ready := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "routing-then-restart", Match: true})
	if ready.Error != "" || len(matchOptions(ready, "routing-then-restart")) != 1 {
		t.Fatalf("ready match was not offered exactly once: %+v", matchOptions(ready, "routing-then-restart"))
	}
	if diagnosisOnly := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "restart-only", Match: true}); len(matchOptions(diagnosisOnly, "restart-only")) != 0 {
		t.Fatalf("a match that cannot mitigate was offered: %+v", matchOptions(diagnosisOnly, "restart-only"))
	}
	if absent := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "never-matched", Match: true}); len(matchOptions(absent, "never-matched")) != 0 {
		t.Fatalf("a playbook without a coverage match was offered: %+v", matchOptions(absent, "never-matched"))
	}
	// The offered option is the same option incident-open coverage advertises.
	option := matchOptions(ready, "routing-then-restart")[0]
	applied := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", SelectionSet: &contracts.MitigationSelectionSet{Version: 1, Options: []*contracts.MitigationOptionSelection{{OptionId: option}}}})
	if applied.Error != "" || len(applied.OptionReports) != 1 || len(cluster.patches) == 0 {
		t.Fatalf("matched option did not apply: %+v; patches=%d", applied, len(cluster.patches))
	}
}

func TestMenuMatchNarrowsByRequestedBinding(t *testing.T) {
	loop, _ := menuB3Loop(t)
	bind := func(name string) []*contracts.Binding {
		return []*contracts.Binding{{Name: "workload", Object: &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name}}}
	}
	loop.Request.PlaybookCoverage = &contracts.PlaybookCoverage{Matches: []*contracts.PlaybookMatch{
		{PlaybookId: "restart-only", CanMitigate: true, Bindings: bind("api")},
		{PlaybookId: "restart-only", CanMitigate: true, Bindings: bind("web")},
	}}
	all := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "restart-only", Match: true})
	if len(matchOptions(all, "restart-only")) != 2 {
		t.Fatalf("both matches should be offered: %+v", matchOptions(all, "restart-only"))
	}
	one := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "restart-only", Match: true, Bindings: programBindingsState(bind("web"))})
	if len(matchOptions(one, "restart-only")) != 1 {
		t.Fatalf("binding should select one match: %+v", matchOptions(one, "restart-only"))
	}
}
