package validate

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory/layout"
)

// These are controller facts, not a model's assertion that memory is complete.
func repairCoverageOutcome() *contracts.OutcomeRecord {
	at := timestamppb.New(time.Unix(100, 0))
	ref := &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	app := &contracts.PlanApplication{ApplicationId: "apply-1", ProgramIds: []string{"base"}, Applied: true, VerificationCompleted: true, VerificationPassed: true, CompletedAt: at, Plan: &contracts.Plan{}}
	return &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "coverage", Classification: "success",
		AppliedPlaybooks: []string{"base"},
		RepairActions:    []*contracts.RepairActionReceipt{{ActionId: "sdo-program-apply-1", Kind: "program-plan", Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: at, CompletedAt: at}},
		PlanApplications: []*contracts.PlanApplication{app},
		PriorStages:      []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{PlaybookId: "base", Result: "applied_cleared", ApplyReceipt: app}}}},
	}
}

func TestCheckReflectionRequiresRepairCoverage(t *testing.T) {
	for _, test := range []struct {
		name, rule, named string
		change            func(*contracts.OutcomeRecord)
	}{
		{"all from working playbook", "", "", func(*contracts.OutcomeRecord) {}},
		{"uncovered hand repair", "reflection.program-required", "manual-port", func(o *contracts.OutcomeRecord) {
			o.RepairActions = append(o.RepairActions, &contracts.RepairActionReceipt{ActionId: "manual-port", Kind: "exec", Success: true})
		}},
		{"failed applied playbook", "reflection.stale-playbook", "base", func(o *contracts.OutcomeRecord) { o.PriorStages[0].Mitigations[0].Result = "apply_error" }},
		{"unresolved applied playbook", "reflection.stale-playbook", "base", func(o *contracts.OutcomeRecord) {
			o.PriorStages[0].Mitigations[0].RemainingFindingFingerprints = []string{"still-broken"}
		}},
		{"application failed outside stages", "reflection.stale-playbook", "base", func(o *contracts.OutcomeRecord) {
			o.PriorStages = nil
			o.PlanApplications[0].VerificationPassed = false
		}},
		{"hand repair after playbook", "reflection.stale-playbook", "base", func(o *contracts.OutcomeRecord) {
			a := proto.Clone(o.RepairActions[0]).(*contracts.RepairActionReceipt)
			a.ActionId = "manual-port"
			a.Kind = "exec"
			a.StartedAt = timestamppb.New(time.Unix(101, 0))
			a.CompletedAt = a.StartedAt
			o.RepairActions = append(o.RepairActions, a)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := goodTree(t)
			o := repairCoverageOutcome()
			test.change(o)
			for _, a := range o.RepairActions {
				a.Target = "Service/app/api"
				a.Summary = "synthetic repair"
				a.Details = "synthetic receipt"
				if a.StartedAt == nil {
					a.StartedAt = timestamppb.New(time.Unix(101, 0))
					a.CompletedAt = a.StartedAt
				}
			}
			raw, err := protojson.Marshal(o)
			if err != nil {
				t.Fatal(err)
			}
			write(t, root, layout.OutcomePath(o.IncidentId), string(raw)+"\n")
			commit(t, root, "outcome\n\nSDO-Incident: coverage\nSDO-Phase: outcome")
			d := &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already covered"}
			ds, err := Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, Decision: d, Publication: &PublicationContext{BaseCommit: "HEAD", IncidentID: o.IncidentId}}, func(context.Context, Request) ([]Diagnostic, error) {
				t.Fatal("no-op must not compile")
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if test.rule == "" {
				if len(ds) != 0 {
					t.Fatalf("working playbook rejected: %v", ds)
				}
				return
			}
			for _, d := range ds {
				if d.Rule == test.rule && strings.Contains(d.Message, test.named) {
					return
				}
			}
			t.Fatalf("missing typed rejection %s naming %s: %v", test.rule, test.named, ds)
		})
	}
}

func TestReflectionCoverageCannotExcusePeersOrRelabelHandRepairs(t *testing.T) {
	ref := &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	peer := &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "peer"}
	gap := &contracts.SdkGap{FaultClass: "routing", Primitive: "unavailable", Reason: "missing capability", ScratchScript: "scratch", Resources: []*contracts.ObjectRef{ref}}
	for _, test := range []struct {
		name                  string
		decision              *contracts.ReflectionDecision
		paths                 []string
		wantRepair, wantStale bool
		change                func(*contracts.OutcomeRecord)
	}{
		{name: "hand repair updated", decision: &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}}, paths: []string{".sdo/playbooks/base/README.md"}},
		{name: "hand repair scoped gap", decision: &contracts.ReflectionDecision{LearningDecision: "sdk_gap", NoChangeReason: "missing", SdkGaps: []*contracts.SdkGap{gap}}},
		{name: "gap does not excuse peer", decision: &contracts.ReflectionDecision{LearningDecision: "sdk_gap", NoChangeReason: "missing", SdkGaps: []*contracts.SdkGap{gap}}, wantRepair: true, change: func(o *contracts.OutcomeRecord) { o.RepairActions[1].Resources = []*contracts.ObjectRef{peer} }},
		{name: "unrelated SDK gap with update but no playbook", decision: &contracts.ReflectionDecision{LearningDecision: "updated", SdkGaps: []*contracts.SdkGap{gap}}, paths: []string{".sdo/diagnostics/incident/learned/detector.go"}, wantRepair: true, change: func(o *contracts.OutcomeRecord) { o.RepairActions[1].Resources = []*contracts.ObjectRef{peer} }},
		{name: "claimed ID absent from diff", decision: &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}}, paths: []string{".sdo/diagnostics/incident/learned/detector.go"}, wantRepair: true},
		{name: "stale cannot be excused by peer edit", decision: &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"peer"}}, paths: []string{".sdo/playbooks/peer/README.md"}, wantStale: true, change: func(o *contracts.OutcomeRecord) { o.PriorStages[0].Mitigations[0].Result = "rollout_failed" }},
		{name: "stale cannot be excused by gap", decision: &contracts.ReflectionDecision{LearningDecision: "sdk_gap", NoChangeReason: "missing", SdkGaps: []*contracts.SdkGap{gap}}, wantStale: true, change: func(o *contracts.OutcomeRecord) { o.PriorStages[0].Mitigations[0].Result = "applied_still_matching" }},
		{name: "stale changed in place", decision: &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}}, paths: []string{".sdo/playbooks/base/README.md"}, change: func(o *contracts.OutcomeRecord) { o.PriorStages[0].Mitigations[0].Result = "apply_error" }},
		{name: "fake program kind is hand repair", decision: &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "covered"}, wantRepair: true, change: func(o *contracts.OutcomeRecord) { o.RepairActions[1].Kind = "program-plan" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := repairCoverageOutcome()
			o.RepairActions = append(o.RepairActions, &contracts.RepairActionReceipt{ActionId: "manual", Kind: "exec", Success: true, Resources: []*contracts.ObjectRef{ref}})
			if test.change != nil {
				test.change(o)
			}
			before := proto.Clone(o)
			ds := ReflectionRepairDiagnostics(repairTree(t, "peer"), o, test.decision, test.paths)
			gotRepair, gotStale := false, false
			for _, d := range ds {
				gotRepair = gotRepair || d.Rule == "reflection.program-required"
				gotStale = gotStale || d.Rule == "reflection.stale-playbook"
			}
			if gotRepair != true || gotStale != test.wantStale {
				t.Fatalf("repair/stale=%v/%v want %v/%v: %v", gotRepair, gotStale, test.wantRepair, test.wantStale, ds)
			}
			if !proto.Equal(o, before) {
				t.Fatal("coverage validation mutated accepted evidence")
			}
		})
	}
}

func TestReflectionStageResultCoverageSweep(t *testing.T) {
	// A typed ineffective skip names a playbook that already failed to clear
	// its findings in this incident, so it is stale like its applied attempt.
	// Every result the contract allows is swept, so a new one must decide.
	results := append(wire.StringIn(&contracts.StageMitigation{}, "result"), "skipped/stage_budget", "skipped/ineffective")
	if !slices.Contains(results, "reverted_ineffective") {
		t.Fatalf("stage results: %v", results)
	}
	for _, result := range results {
		t.Run(result, func(t *testing.T) {
			o := repairCoverageOutcome()
			o.PriorStages[0].Mitigations[0].Result, o.PriorStages[0].Mitigations[0].SkipReason, _ = strings.Cut(result, "/")
			ds := ReflectionRepairDiagnostics(repairTree(t, "peer"), o, &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "covered"}, nil)
			want := result != "applied_cleared" && result != "skipped" && result != "skipped/stage_budget"
			if (len(ds) > 0) != want {
				t.Fatalf("result %s: %v", result, ds)
			}
			if result == "reverted_ineffective" && !strings.Contains(ds[0].Message, "reverted it as ineffective") {
				t.Fatalf("reflection is not told the stage reverted the playbook: %v", ds)
			}
		})
	}
}

func TestReflectionLaterRepairDoesNotStaleUnrelatedBatchedPlaybook(t *testing.T) {
	o := repairCoverageOutcome()
	ref := o.RepairActions[0].Resources[0]
	peer := &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "peer"}
	o.PlanApplications[0].ProgramIds = []string{"base", "peer"}
	o.PlaybookCoverage = &contracts.PlaybookCoverage{Matches: []*contracts.PlaybookMatch{
		{PlaybookId: "base", Bindings: []*contracts.Binding{{Name: "service", Object: ref}}},
		{PlaybookId: "peer", Bindings: []*contracts.Binding{{Name: "service", Object: peer}}},
	}}
	a := proto.Clone(o.RepairActions[0]).(*contracts.RepairActionReceipt)
	a.ActionId = "manual"
	a.Kind = "exec"
	a.StartedAt = timestamppb.New(time.Unix(101, 0))
	a.CompletedAt = a.StartedAt
	o.RepairActions = append(o.RepairActions, a)
	ds := ReflectionRepairDiagnostics(repairTree(t, "peer"), o, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}}, []string{".sdo/playbooks/base/README.md"})
	for _, d := range ds {
		if d.Rule != RuleReflectionProgramRequired {
			t.Fatalf("unrelated batched peer was marked stale: %v", ds)
		}
	}
}

func TestReflectionStageBindingsKeepLaterHandRepairVisibleWithoutReceipt(t *testing.T) {
	o := repairCoverageOutcome()
	ref := o.RepairActions[0].Resources[0]
	o.PlanApplications = nil
	m := o.PriorStages[0].Mitigations[0]
	m.ApplyReceipt = nil
	m.Bindings = []*contracts.Binding{{Name: "service", Object: ref}}
	o.RepairActions[0].ActionId = "manual"
	o.RepairActions[0].Kind = "exec"
	ds := ReflectionRepairDiagnostics(repairTree(t, "peer"), o, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"peer"}}, []string{".sdo/playbooks/peer/README.md"})
	if !slices.ContainsFunc(ds, func(d Diagnostic) bool {
		return d.Rule == RuleReflectionStalePlaybook && strings.Contains(d.Message, "base")
	}) {
		t.Fatalf("stale stage playbook lost with its missing application receipt: %v", ds)
	}
}

func TestReflectionFinalFindingCoverageIsPlaybookScoped(t *testing.T) {
	for _, tc := range []struct {
		name, fingerprint, status string
		want                      bool
	}{
		{"own finding remains", "own", "firing", true},
		{"own finding pending", "own", "pending", true},
		{"peer remains", "peer", "firing", false},
		{"own finding cleared", "own", "clear", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := repairCoverageOutcome()
			o.PlaybookCoverage = &contracts.PlaybookCoverage{Matches: []*contracts.PlaybookMatch{{PlaybookId: "base", ExplainedFindingFingerprints: []string{"own"}}}}
			o.FinalHealthDetectorState = []*contracts.DetectorEvaluation{{DetectorId: "detector", Status: tc.status, Fingerprints: []string{tc.fingerprint}}}
			ds := ReflectionRepairDiagnostics(repairTree(t, "peer"), o, &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "covered"}, nil)
			if (len(ds) > 0) != tc.want {
				t.Fatalf("final findings: %v", ds)
			}
			for _, d := range ds {
				if d.Rule != "reflection.stale-playbook" || !strings.Contains(d.Message, "base") {
					t.Fatalf("wrong rejection: %v", d)
				}
			}
		})
	}
}

// repairTree is accepted memory holding the base playbook and the named peers.
func repairTree(t *testing.T, ids ...string) string {
	t.Helper()
	root := goodTree(t)
	for _, id := range ids {
		write(t, root, layout.PlaybookDir(id)+"/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: "+id+"\noriginating_incident: synthetic\n---\nRepair <RESOURCE_NAME>.\n")
	}
	return root
}

// Applications also attribute generic menu actions and removals. Only an
// accepted playbook owns its menu-applied repairs or can be stale; every other
// application program ID leaves the repair a hand repair.
func TestReflectionMenuRepairOwnershipFollowsAcceptedPlaybooks(t *testing.T) {
	for _, test := range []struct {
		program    string
		failed     bool
		wantRepair bool
		wantStale  bool
	}{
		{program: "base"},
		{program: "base", failed: true, wantStale: true},
		{program: "action:SetField", wantRepair: true},
		{program: "action:SetField", failed: true, wantRepair: true},
		{program: "remove-unexplained", wantRepair: true},
		{program: "remove-unexplained", failed: true, wantRepair: true},
	} {
		t.Run(fmt.Sprintf("%s/failed=%t", test.program, test.failed), func(t *testing.T) {
			o := repairCoverageOutcome()
			o.PriorStages = nil
			app := o.PlanApplications[0]
			app.ApplicationId = "mitigation-0123-1-r1"
			app.ProgramIds = []string{test.program}
			app.VerificationPassed = !test.failed
			o.RepairActions[0].ActionId = app.ApplicationId + "-a1"
			o.RepairActions[0].Kind = "mitigation-menu"
			ds := ReflectionRepairDiagnostics(repairTree(t), o, &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "covered"}, nil)
			gotRepair, gotStale := false, false
			for _, d := range ds {
				gotRepair = gotRepair || d.Rule == RuleReflectionProgramRequired && strings.Contains(d.Message, o.RepairActions[0].ActionId)
				gotStale = gotStale || d.Rule == RuleReflectionStalePlaybook
				if d.Rule == RuleReflectionStalePlaybook && !strings.Contains(d.Message, `"base"`) {
					t.Fatalf("non-playbook program marked stale: %v", d)
				}
			}
			if gotRepair != test.wantRepair || gotStale != test.wantStale || len(ds) != btoi(test.wantRepair)+btoi(test.wantStale) {
				t.Fatalf("repair/stale=%v/%v want %v/%v: %v", gotRepair, gotStale, test.wantRepair, test.wantStale, ds)
			}
		})
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
