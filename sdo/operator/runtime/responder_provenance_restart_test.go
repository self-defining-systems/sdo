package runtime

import (
	"context"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Real command execution -> checkpoint -> restarted loop -> wire projection ->
// learning gate. Receipt kinds alone must never stand in for program ownership.
func TestSavedProgramOwnershipSurvivesResponderRestart(t *testing.T) {
	for _, path := range []string{"apply", "menu", "stage", "raw"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			loop, _ := trustedPortLoop(t, false, false, true)
			loop.Executor.Verifier = PlanHealthVerifier{Detectors: loop.Detectors, Provider: loop.Provider}
			loop.MenuConfig = productionResponderMenu()
			var saved []byte
			loop.saveToolState = func(_ context.Context, data []byte) error { saved = append([]byte(nil), data...); return nil }
			bindings := program.Bindings{Objects: program.ObjectBindings{"service": loop.Request.Findings[0].PrimaryResource}}
			switch path {
			case "raw":
				if menu := loop.Handle(ctx, ToolRequest{Command: "menu"}); menu.Error != "" {
					t.Fatal(menu.Error)
				}
				applied := loop.Handle(ctx, ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "SetField", RoundLimit: 1}, Parameters: map[string]string{"ref": "Service/app/api", "path": "spec.ports[0].targetPort", "value": "8080", "reason": "synthetic hand repair"}})
				if applied.Error != "" || !applied.TargetedHealthy {
					t.Fatalf("raw apply: %+v", applied)
				}
			case "stage":
				state := &IncidentState{Request: loop.Request, Findings: loop.Request.Findings}
				if _, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(ctx, state); err != nil {
					t.Fatal(err)
				}
			case "menu":
				menu := loop.Handle(ctx, ToolRequest{Command: "menu", ProgramID: "service-targetport-mismatch", Parameters: map[string]string{"service": "Service/app/api"}})
				if menu.Error != "" {
					t.Fatal(menu.Error)
				}
				option := ""
				for _, item := range menu.Menu.GetOptions() {
					if item.ProgramId == "service-targetport-mismatch" {
						option = item.OptionId
					}
				}
				applied := loop.Handle(ctx, ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
				if applied.Error != "" || !applied.TargetedHealthy {
					t.Fatalf("menu apply: %+v", applied)
				}
			default:
				planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
				applied := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
				if applied.Error != "" {
					t.Fatal(applied.Error)
				}
			}
			before, err := incidentResultContract(IncidentResult{PlanApplications: loop.ApplicationEvidence()})
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch"), 0755); err != nil {
				t.Fatal(err)
			}
			fresh, _ := trustedPortLoop(t, false, false, true)
			if err := fresh.restoreToolSnapshot(saved); err != nil {
				t.Fatal(err)
			}
			if len(fresh.PlanApplications()) != 0 || fresh.LoopsUsed() != loop.LoopsUsed() {
				t.Fatal("historical evidence restored live applications or reset repair budget")
			}
			// Reproduce the p45 shape: a pre-restart application followed by
			// another accepted program through the responder menu. Both owners
			// must survive, even though the first is no longer a live handle.
			if path == "apply" {
				fresh.Executor.Verifier = PlanHealthVerifier{Detectors: fresh.Detectors, Provider: fresh.Provider}
				fresh.MenuConfig = productionResponderMenu()
				menu := fresh.Handle(ctx, ToolRequest{Command: "menu", ProgramID: "service-targetport-mismatch", Parameters: map[string]string{"service": "Service/app/api"}})
				if menu.Error != "" || len(menu.Menu.GetOptions()) == 0 {
					t.Fatalf("new menu: %+v", menu)
				}
				option := ""
				for _, item := range menu.Menu.Options {
					if item.ProgramId == "service-targetport-mismatch" {
						option = item.OptionId
					}
				}
				applied := fresh.Handle(ctx, ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
				if applied.Error != "" || !applied.TargetedHealthy {
					t.Fatalf("second program: %+v", applied)
				}
				live, err := incidentResultContract(IncidentResult{PlanApplications: fresh.ApplicationEvidence()})
				if err != nil {
					t.Fatal(err)
				}
				before = live
			}
			// Repeat the storage round trip: recovered provenance must remain durable.
			fresh.saveToolState = loop.saveToolState
			if err := fresh.persistToolState(ctx); err != nil {
				t.Fatal(err)
			}
			again, _ := trustedPortLoop(t, false, false, true)
			if err := again.restoreToolSnapshot(saved); err != nil {
				t.Fatal(err)
			}
			result, err := incidentResultContract(IncidentResult{PlanApplications: again.ApplicationEvidence(), RepairActions: append(again.ApplicationRepairReceipts(nil), again.MenuRepairActions()...)})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.PlanApplications) != len(before.PlanApplications) {
				t.Fatal("recovery dropped application evidence")
			}
			for i, application := range before.PlanApplications {
				if !proto.Equal(application, result.PlanApplications[i]) {
					t.Fatalf("recovery changed application facts: before=%v after=%v", application, result.PlanApplications[i])
				}
			}
			if len(result.RepairActions) == 0 {
				t.Fatal("missing real executor receipts")
			}
			outcome := &contracts.OutcomeRecord{PlanApplications: result.PlanApplications, RepairActions: result.RepairActions}
			decision := &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "accepted program repaired the incident"}
			if path == "raw" {
				ds := validate.ReflectionRepairDiagnostics(root, outcome, decision, nil, "off")
				if len(ds) != 1 || ds[0].Rule != validate.RuleReflectionProgramRequired {
					t.Fatalf("real hand repair escaped learning: %v", ds)
				}
				return
			}
			if ds := validate.ReflectionRepairDiagnostics(root, outcome, decision, nil, "off"); len(ds) > 0 {
				t.Fatalf("saved repair misclassified after restart: %v", ds)
			}
			early := earlyResultSnapshot(again, again.Request, time.Now())
			if len(early.PlanApplications) != len(result.PlanApplications) {
				t.Fatal("early closure dropped recovered program ownership")
			}
			want := 1
			if path == "apply" {
				want = 2
			}
			if len(result.PlanApplications) != want {
				t.Fatalf("lost/duplicated application: %v", result.PlanApplications)
			}
			if response := again.Handle(ctx, ToolRequest{Command: "rollback", ApplicationID: result.PlanApplications[0].ApplicationId}); response.Error == "" {
				t.Fatal("restored evidence granted rollback authority")
			}
			// Failed verification still requires repairing the existing program,
			// without relabeling its writes as a new hand repair.
			result.PlanApplications[0].VerificationPassed = false
			result.PlanApplications[0].Verification = nil
			ds := validate.ReflectionRepairDiagnostics(root, outcome, decision, nil, "off")
			if len(ds) != 1 || ds[0].Rule != validate.RuleReflectionStalePlaybook {
				t.Fatalf("failed saved program lost ownership: %v", ds)
			}
			result.PlanApplications[0].VerificationPassed = true
			// Removing accepted ownership makes the exact same executor action a hand
			// repair. A program-shaped action ID cannot bypass the learning gate.
			for _, application := range result.PlanApplications {
				application.ProgramIds = []string{"action:SetField"}
			}
			if ds := validate.ReflectionRepairDiagnostics(root, outcome, decision, nil, "off"); len(ds) == 0 {
				t.Fatal("raw action escaped executable-learning requirement")
			}
		})
	}
}

func TestPendingApplicationOwnershipSurvivesCheckpoint(t *testing.T) {
	ctx := context.Background()
	loop, _ := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.MenuConfig = productionResponderMenu()
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Executor.RolloutTimeout = time.Millisecond
	loop.Executor.Rollouts = rolloutFunc(func(ctx context.Context, _ []sdk.ObjectRef) error { <-ctx.Done(); return ctx.Err() })
	var saved []byte
	loop.saveToolState = func(_ context.Context, data []byte) error { saved = append([]byte(nil), data...); return nil }
	menu := loop.Handle(ctx, ToolRequest{Command: "menu"})
	option := ""
	for _, item := range menu.Menu.GetOptions() {
		if item.ProgramId == "restart-only" {
			option = item.OptionId
		}
	}
	response := loop.Handle(ctx, ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}})
	if response.RolloutPending == nil || len(loop.ApplicationEvidence()) != 1 || !loop.ApplicationEvidence()[0].Pending {
		t.Fatalf("expected real pending execution: %+v", response)
	}
	fresh, _ := menuB3Loop(t)
	if err := fresh.restoreToolSnapshot(saved); err != nil {
		t.Fatal(err)
	}
	applications := fresh.ApplicationEvidence()
	if len(applications) != 1 || !applications[0].Pending || applications[0].Result.VerificationPassed {
		t.Fatalf("recovery changed pending status: %+v", applications)
	}
	for _, receipt := range ProgramRepairReceipts(applications, nil) {
		if receipt.Success {
			t.Fatal("pending recovered application claimed completed repair")
		}
	}
}

func TestRecoveredProgramOwnersReachEarlyClosure(t *testing.T) {
	ctx := context.Background()
	loop, client := trustedPortLoop(t, false, false, true)
	loop.Executor.Verifier = PlanHealthVerifier{Detectors: loop.Detectors, Provider: loop.Provider}
	service, err := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller := true
	service.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "synthetic-owner", UID: "synthetic-owner-uid", Controller: &controller}}
	if _, err := client.CoreV1().Services("app").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	var saved []byte
	loop.saveToolState = func(_ context.Context, data []byte) error { saved = append([]byte(nil), data...); return nil }
	planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: program.Bindings{Objects: program.ObjectBindings{"service": loop.Request.Findings[0].PrimaryResource}}})
	applied := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" {
		t.Fatal(applied.Error)
	}
	before := loop.ApplicationRepairReceipts(nil)
	if len(before) != 1 || len(before[0].Owners) != 1 {
		t.Fatalf("executor did not produce ownership evidence: %+v", before)
	}
	fresh, _ := trustedPortLoop(t, false, false, true)
	if err := fresh.restoreToolSnapshot(saved); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, fresh.ApplicationRepairReceipts(nil)) {
		t.Fatal("checkpoint dropped original receipt ownership")
	}
	feedback, err := continuationFeedback(1, ToolResponse{}, fresh.continuationRepairs(IncidentResult{})...)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(feedback), "synthetic-owner") {
		t.Fatal("controller-only owner chain leaked into responder feedback")
	}
	early := earlyResultSnapshot(fresh, fresh.Request, time.Now())
	if !reflect.DeepEqual(early.RepairActions, fresh.ApplicationRepairReceipts(nil)) {
		t.Fatalf("early closure lost original receipt ownership: early=%+v final=%+v", early.RepairActions, fresh.ApplicationRepairReceipts(nil))
	}
}
