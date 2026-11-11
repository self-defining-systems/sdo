package runtime

import (
	"context"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const apiBinding = "Deployment/app/api"

func targetLoop(t *testing.T) (*sameWriteWorld, *ResponderLoop) {
	t.Helper()
	world, loop, _ := sameWriteLoop(t)
	loop.MenuConfig = &ResponderMenuConfig{}
	return world, loop
}

func TestDiagnoseWrongOrMissingRoleNamesWhatThePlaybookTakes(t *testing.T) {
	_, loop := targetLoop(t)
	usage := "aware-restart has no parameter 'redis'; it takes deployment=Deployment/NAMESPACE/NAME"
	for name, request := range map[string]ToolRequest{
		"wrong role": {Command: "diagnose", ProgramID: "aware-restart", Parameters: map[string]string{"redis": apiBinding}},
		"missing":    {Command: "diagnose", ProgramID: "aware-restart"},
	} {
		response := loop.Handle(context.Background(), request)
		if response.Error == "" || len(response.ProgramRuns) != 0 {
			t.Fatalf("%s: diagnose must fail instead of returning a verdict: %+v", name, response)
		}
		if !strings.Contains(response.Error, "deployment=Deployment/NAMESPACE/NAME") {
			t.Fatalf("%s: error must say what the playbook takes: %q", name, response.Error)
		}
		if name == "wrong role" && response.Error != usage {
			t.Fatalf("error = %q, want %q", response.Error, usage)
		}
	}
}

func TestDiagnoseMatchNamesTheCommandThatFixesIt(t *testing.T) {
	_, loop := targetLoop(t)
	response := loop.Handle(context.Background(), ToolRequest{Command: "diagnose", ProgramID: "aware-restart", Parameters: map[string]string{"deployment": apiBinding}})
	if response.Error != "" || len(response.ProgramRuns) != 1 {
		t.Fatalf("diagnose failed: %+v", response)
	}
	if got, want := response.ProgramRuns[0].MitigateCommand, "sdo mitigate aware-restart --bind deployment=Deployment/app/api"; got != want {
		t.Fatalf("mitigate command = %q, want %q", got, want)
	}
}

func TestMitigateByNameAppliesTheExactProgramAndBindings(t *testing.T) {
	for name, parameters := range map[string]map[string]BindValues{
		"explicit binding":         {"deployment": {apiBinding}},
		"the one recorded binding": nil,
	} {
		t.Run(name, func(t *testing.T) {
			world, loop := targetLoop(t)
			response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Targets: []MitigateTarget{{Name: "aware-restart", Parameters: parameters}}})
			if response.Error != "" {
				t.Fatalf("mitigate by name failed: %s", response.Error)
			}
			if got := world.restarts.Load(); got != 1 {
				t.Fatalf("restarts = %d, want 1", got)
			}
			if len(response.MenuExecution) != 1 || response.MenuExecution[0].ProgramID != "aware-restart" {
				t.Fatalf("exact program was not executed: %+v", response.MenuExecution)
			}
		})
	}
}

func TestMitigateWrongRoleIsRefusedBeforeAnyWrite(t *testing.T) {
	world, loop := targetLoop(t)
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Targets: []MitigateTarget{{Name: "aware-restart", Parameters: map[string]BindValues{"redis": {apiBinding}}}}})
	if want := "aware-restart has no parameter 'redis'; it takes deployment=Deployment/NAMESPACE/NAME"; response.Error != want {
		t.Fatalf("error = %q, want %q", response.Error, want)
	}
	if world.restarts.Load() != 0 {
		t.Fatal("a refused mitigation wrote")
	}
}

func TestMitigateUnknownNameIsRefused(t *testing.T) {
	_, loop := targetLoop(t)
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Targets: []MitigateTarget{{Name: "no-such-playbook"}}})
	if !strings.Contains(response.Error, "unknown playbook or action 'no-such-playbook'") {
		t.Fatalf("error = %q", response.Error)
	}
}

func TestMitigateAmbiguousRecordedMatchListsCandidatesAsCommands(t *testing.T) {
	_, loop := targetLoop(t)
	loop.Request.Findings = nil
	coverage := &contracts.PlaybookCoverage{}
	for _, name := range []string{"api", "web"} {
		coverage.Matches = append(coverage.Matches, &contracts.PlaybookMatch{PlaybookId: "aware-restart", CanMitigate: true, Bindings: []*contracts.Binding{{Name: "deployment", Object: &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name}}}})
	}
	loop.Request.PlaybookCoverage = coverage
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Targets: []MitigateTarget{{Name: "aware-restart"}}})
	for _, want := range []string{"sdo mitigate aware-restart --bind deployment=Deployment/app/api", "sdo mitigate aware-restart --bind deployment=Deployment/app/web"} {
		if !strings.Contains(response.Error, want) {
			t.Fatalf("error must list %q: %s", want, response.Error)
		}
	}
	if strings.Contains(response.Error, "program-") {
		t.Fatalf("hash identifiers must stay hidden: %s", response.Error)
	}
}

func TestMenuNamesCommandsForEveryOption(t *testing.T) {
	_, loop := targetLoop(t)
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "aware-restart", Parameters: map[string]string{"deployment": apiBinding}})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	found := false
	for _, command := range response.MenuCommands {
		found = found || command == "sdo mitigate aware-restart --bind deployment=Deployment/app/api"
	}
	if !found {
		t.Fatalf("menu commands = %v", response.MenuCommands)
	}
}

func TestSeveralFixesRunTogetherAndOverlappingWritesAreRefused(t *testing.T) {
	world, loop := targetLoop(t)
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Targets: []MitigateTarget{
		{Name: "aware-restart", Parameters: map[string]BindValues{"deployment": {apiBinding}}},
		{Name: "blind-restart", Parameters: map[string]BindValues{"deployment": {apiBinding}}},
	}})
	if response.Report == nil || response.Report.Status != contracts.MitigationReport_STATUS_REFUSED {
		t.Fatalf("two fixes writing one object must be refused: %+v", response)
	}
	if world.restarts.Load() != 0 {
		t.Fatal("a refused batch wrote")
	}
}

func mitigateReport(t *testing.T, name string) ToolResponse {
	t.Helper()
	world, loop := targetLoop(t)
	world.observed.Store(1) // the rollout has already completed; traffic has not recovered
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Targets: []MitigateTarget{{Name: name, Parameters: map[string]BindValues{"deployment": {apiBinding}}}}})
	if response.Error != "" || response.Report == nil {
		t.Fatalf("mitigate failed: %+v", response)
	}
	return response
}

func TestAppliedFixWithClearedDiagnosisReportsWaitingNotUnresolved(t *testing.T) {
	response := mitigateReport(t, "aware-restart")
	if response.Report.ReasonCode != AppliedAwaitingRecovery {
		t.Fatalf("reason = %q, want %q (status %s)", response.Report.ReasonCode, AppliedAwaitingRecovery, response.Report.Status)
	}
	if want := "fix applied; waiting for traffic to recover; next: sdo verify --wait 60"; response.Summary != want {
		t.Fatalf("summary = %q, want %q", response.Summary, want)
	}
	if response.TargetedHealthy {
		t.Fatal("reporting only: independent verification must still decide recovery")
	}
}

func TestAppliedFixWhoseDiagnosisStillMatchesStaysAFailure(t *testing.T) {
	response := mitigateReport(t, "blind-restart")
	if response.Report.ReasonCode == AppliedAwaitingRecovery || strings.Contains(response.Summary, "waiting for traffic") {
		t.Fatalf("a diagnosis that still matches after writes must not read as waiting: %+v", response.Report)
	}
}
