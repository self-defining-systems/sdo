package runtime

import (
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// A receipt reads FAILED only when it carries a failure_reason, so every path
// that leaves a controller receipt unsuccessful names why. The one exception
// is an application whose own per-application verification never ran: the
// responder verifies the incident as a whole, so the action is applied, not
// failed.
func TestEveryUnsuccessfulProgramReceiptNamesItsFailure(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	script := program.RunScript{Script: "echo repair", Reason: "repair"}
	at := time.Unix(1_000, 0).UTC()
	applied := func(change func(*RecordedPlanApplication)) RecordedPlanApplication {
		restart := program.RestartRollout{Ref: ref, Reason: "replace Pods"}
		application := RecordedPlanApplication{
			ID: "plan-s-1", ProgramIDs: []string{"restart"}, StartedAt: at, CompletedAt: at.Add(time.Second),
			Result: PlanApplicationResult{
				Applied: true, VerificationCompleted: true, VerificationPassed: true,
				Plan:           program.Plan{Actions: []program.Action{restart, script}},
				ActionsApplied: []program.Action{restart, script},
				ActionReports: []PlanActionReport{
					{Status: "applied"},
					{Status: "applied", Script: &ScriptExecutionResult{ScriptDigest: program.ScriptDigest(script.Script)}},
				},
				ObjectEffects: []PlanObjectEffect{{Ref: ref, State: "applied", Confirmed: true}},
			},
		}
		change(&application)
		return application
	}
	pending := 0
	for name, test := range map[string]struct {
		application RecordedPlanApplication
		success     bool
		reason      string
		// script is the reason on the run-script receipt; "-" when the
		// path writes none.
		script string
	}{
		"verified": {applied(func(*RecordedPlanApplication) {}), true, "", ""},
		"verification not run": {applied(func(a *RecordedPlanApplication) {
			a.Result.VerificationCompleted, a.Result.VerificationPassed = false, false
		}), false, "", ""},
		"verification failed": {applied(func(a *RecordedPlanApplication) { a.Result.VerificationPassed = false }), false, "verification_failed", "verification_failed"},
		"rolled back":         {applied(func(a *RecordedPlanApplication) { a.RolledBack = true }), false, "rolled_back", "-"},
		"rollback error":      {applied(func(a *RecordedPlanApplication) { a.RollbackError = "patch refused" }), false, "rollback_failed", "-"},
		"settlement rejected": {applied(func(a *RecordedPlanApplication) {
			a.Result.SettlementRejectionReason = contracts.SettlementRejectionReason(1)
		}), false, "settlement_rejected", "settlement_rejected"},
		"application error":   {applied(func(a *RecordedPlanApplication) { a.Error = "executor lost" }), false, "application_error", "application_error"},
		"application pending": {applied(func(a *RecordedPlanApplication) { a.Pending = true }), false, "application_pending", "application_pending"},
		"rollout pending": {applied(func(a *RecordedPlanApplication) {
			a.Result.RolloutPending = &contracts.RolloutPending{}
		}), false, "application_pending", "application_pending"},
		"action pending":   {applied(func(a *RecordedPlanApplication) { a.Result.PendingAction = &pending }), false, "application_pending", "application_pending"},
		"rolling back":     {applied(func(a *RecordedPlanApplication) { a.Result.RollingBack = true }), false, "application_pending", "application_pending"},
		"error unverified": {applied(func(a *RecordedPlanApplication) { a.Error, a.Result.VerificationCompleted = "executor lost", false }), false, "application_error", "application_error"},
	} {
		t.Run(name, func(t *testing.T) {
			receipts := ProgramRepairReceipts([]RecordedPlanApplication{test.application}, nil)
			var plan, run *RepairActionReceipt
			for i := range receipts {
				switch receipts[i].ActionID {
				case "sdo-program-plan-s-1":
					plan = &receipts[i]
				case "sdo-program-plan-s-1-script-2":
					run = &receipts[i]
				}
			}
			if plan == nil {
				t.Fatalf("no program-plan receipt: %+v", receipts)
			}
			if plan.Success != test.success || plan.FailureReason != test.reason {
				t.Fatalf("program-plan receipt success=%v reason=%q, want %v %q", plan.Success, plan.FailureReason, test.success, test.reason)
			}
			if test.script == "-" {
				if run != nil {
					t.Fatalf("run-script receipt for a rolled-back application: %+v", run)
				}
				return
			}
			if run == nil {
				t.Fatalf("no run-script receipt: %+v", receipts)
			}
			if run.Success != test.success || run.FailureReason != test.script {
				t.Fatalf("run-script receipt success=%v reason=%q, want %v %q", run.Success, run.FailureReason, test.success, test.script)
			}
		})
	}
}

// A rolled-back menu action is a failure, so its receipt names why.
func TestRolledBackMenuReceiptNamesItsFailure(t *testing.T) {
	loop := &ResponderLoop{menuRepairs: []RepairActionReceipt{{ActionID: "plan-s-1-a1-r1-1", Kind: "mitigation-menu", Success: true}, {ActionID: "plan-s-2-a1-r1-1", Kind: "mitigation-menu", Success: true}}}
	loop.updateMenuRollback("plan-s-1", PlanApplicationResult{Applied: true, RolledBack: true})
	rolled, kept := loop.menuRepairs[0], loop.menuRepairs[1]
	if rolled.Success || rolled.FailureReason != "rolled_back" {
		t.Fatalf("rolled-back menu receipt = %+v", rolled)
	}
	if !kept.Success || kept.FailureReason != "" {
		t.Fatalf("another application's receipt changed: %+v", kept)
	}
}

// Readers show FAILED only for a receipt with a failure_reason. A record
// written before every failure path set one falls back to success, unless the
// receipt's plan application in the same record shows it applied cleanly and
// never ran its own verification.
func TestRepairActionFailedReadsTheReasonAndFallsBackForOlderRecords(t *testing.T) {
	clean := func(id string) *contracts.PlanApplication {
		return &contracts.PlanApplication{ApplicationId: id, Applied: true}
	}
	with := func(change func(*contracts.PlanApplication)) []*contracts.PlanApplication {
		application := clean("plan-s-1")
		change(application)
		return []*contracts.PlanApplication{application}
	}
	unverified := &contracts.RepairActionReceipt{ActionId: "sdo-program-plan-s-1", Kind: "program-plan"}
	for name, test := range map[string]struct {
		action       *contracts.RepairActionReceipt
		applications []*contracts.PlanApplication
		failed       bool
	}{
		"reason":                  {&contracts.RepairActionReceipt{ActionId: "x", Kind: "program-plan", FailureReason: "rolled_back"}, nil, true},
		"success":                 {&contracts.RepairActionReceipt{ActionId: "x", Kind: "mitigation-menu", Success: true}, nil, false},
		"applied, not verified":   {unverified, []*contracts.PlanApplication{clean("plan-s-1")}, false},
		"deduplicated receipt ID": {&contracts.RepairActionReceipt{ActionId: "sdo-program-plan-s-1-2", Kind: "program-plan"}, []*contracts.PlanApplication{clean("plan-s-1")}, false},
		"run-script receipt":      {&contracts.RepairActionReceipt{ActionId: "sdo-program-plan-s-1-script-2", Kind: "run-script"}, []*contracts.PlanApplication{clean("plan-s-1")}, false},
		"longest application ID":  {&contracts.RepairActionReceipt{ActionId: "sdo-program-plan-s-1-2", Kind: "program-plan"}, []*contracts.PlanApplication{clean("plan-s-1"), {ApplicationId: "plan-s-1-2", Applied: true, RolledBack: true}}, true},
		"older rollback":          {unverified, with(func(a *contracts.PlanApplication) { a.RolledBack = true }), true},
		"older rollback error":    {unverified, with(func(a *contracts.PlanApplication) { a.RollbackError = "refused" }), true},
		"older application error": {unverified, with(func(a *contracts.PlanApplication) { a.Error = "lost" }), true},
		"older rollout pending":   {unverified, with(func(a *contracts.PlanApplication) { a.RolloutPending = &contracts.RolloutPending{} }), true},
		"older verification ran":  {unverified, with(func(a *contracts.PlanApplication) { a.VerificationCompleted = true }), true},
		"older settlement refusal": {unverified, with(func(a *contracts.PlanApplication) {
			a.SettlementRejectionReason = contracts.SettlementRejectionReason(1)
		}), true},
		"older not applied":   {unverified, with(func(a *contracts.PlanApplication) { a.Applied = false }), true},
		"no application":      {unverified, nil, true},
		"older menu rollback": {&contracts.RepairActionReceipt{ActionId: "plan-s-1-a1-r1-1", Kind: "mitigation-menu"}, []*contracts.PlanApplication{clean("plan-s-1")}, true},
	} {
		if got := RepairActionFailed(test.action, test.applications); got != test.failed {
			t.Errorf("%s: failed = %v, want %v", name, got, test.failed)
		}
	}
}
