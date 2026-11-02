package runtime

// Independent T1 oracle: expectations below come from decisions 162–176/186,
// not R6's verifiedRepeat/verifiedTrustOutcome fixtures. Only repository and
// closure setup helpers are shared with the older integration tests.
import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const t1Program = "contract-repair"

func t1Ref(name string) *contracts.ObjectRef {
	return &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: name}
}

func t1JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func t1Write(t *testing.T, root, name string, raw []byte) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatal(err)
	}
}

func t1Outcome(t *testing.T, revision string) *contracts.OutcomeRecord {
	t.Helper()
	at := timestamppb.New(time.Unix(100, 0))
	bound := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
	// Hand-written controller transcript: an open Match, selected plan, completed
	// apply and binding-scoped post-apply NoMatch, plus affirmative final proof.
	return &contracts.OutcomeRecord{
		SchemaVersion: 1, IncidentId: "contract", SourceCommit: "source", Classification: "success",
		Timestamps:          &contracts.OutcomeTimestamps{CompletedAt: at},
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "wrong target port", Resources: []*contracts.ObjectRef{t1Ref("api")}}},
		RepairActions:       []*contracts.RepairActionReceipt{{Target: "Service/app/api", Summary: "restore target port", Details: "set targetPort to accepted value", StartedAt: at, CompletedAt: at, ActionId: "sdo-program-apply-1", Kind: "program-plan", Success: true, Resources: []*contracts.ObjectRef{t1Ref("api")}}},
		ProgramRuns:         []*contracts.ProgramRun{{ProgramId: t1Program, ProgramRevision: revision, Duration: durationpb.New(time.Millisecond), Bindings: []*contracts.Binding{{Name: "service", Object: t1Ref("api")}}, Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH}}},
		PlanApplications:    []*contracts.PlanApplication{{Plan: &contracts.Plan{Rationale: "restore accepted target port", Actions: []*contracts.Action{{Kind: &contracts.Action_SetField{SetField: &contracts.SetField{Ref: t1Ref("api"), Path: "spec.ports[0].targetPort", Value: &contracts.PlanValue{Origin: &contracts.PlanValue_Parameter{Parameter: "target-port"}}, Reason: "restore accepted field"}}}}}, ApplicationId: "apply-1", ProgramIds: []string{t1Program}, Applied: true, CompletedAt: at, VerificationCompleted: true, VerificationPassed: true, FinalEquivalenceVerified: proto.Bool(true), Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}},
		ProgramToolCalls: []*contracts.ProgramToolCall{
			{Command: "plan", At: timestamppb.New(time.Unix(99, 0)), RequestJson: string(t1JSON(t, ToolRequest{Command: "plan", ProgramID: t1Program, Bindings: bound})), ResponseJson: `{"command":"plan","plan_id":"plan-1"}`},
			{Command: "apply", At: timestamppb.New(time.Unix(101, 0)), RequestJson: `{"command":"apply","plan_ids":["plan-1"]}`, ResponseJson: string(t1JSON(t, ToolResponse{Command: "apply", ApplicationID: "apply-1", ProgramRuns: []ProgramRun{{ProgramID: t1Program, ProgramRevision: revision, Bindings: bound, Verdict: program.Verdict{Result: program.NoMatch}}}}))},
		},
	}
}

type t1Class struct {
	name                       string
	change                     func(*contracts.OutcomeRecord)
	fixed                      program.Result
	items                      []string
	model                      bool
	positive, negative, credit int
	decline                    bool
	prior                      bool
	unverified                 bool
}

func t1Classes() []t1Class {
	return []t1Class{
		{name: "covered_exact", fixed: program.NoMatch, items: []string{"0/covered/verified_equivalent_repair/contract-repair"}, positive: 1, credit: 1},
		{name: "fixed_still_matches", fixed: program.Match, items: []string{"0/false_match/fixed_capture_match/contract-repair"}, model: true, negative: 1},
		{name: "binding_never_written", fixed: program.NoMatch, change: func(o *contracts.OutcomeRecord) {
			o.RepairActions = nil
			o.PlanApplications = nil
			o.ProgramToolCalls = nil
		}, items: []string{"0/question/binding_not_written/contract-repair"}, model: true, unverified: true},
		{name: "different_fields", fixed: program.NoMatch, change: func(o *contracts.OutcomeRecord) {
			o.PlanApplications[0].FinalEquivalenceVerified = proto.Bool(false)
			o.RepairActions[0].Kind = "manual"
			o.RepairActions[0].Details = "changed spec.selector.app; the plan proposed spec.ports[0].targetPort"
		}, items: []string{"0/question/repair_equivalence_question/contract-repair"}, model: true},
		{name: "unbound_write", fixed: program.NoMatch, change: func(o *contracts.OutcomeRecord) {
			o.ProgramRuns = nil
			o.PlanApplications = nil
			o.ProgramToolCalls = nil
		}, items: []string{"0/question/unbound_write/"}, model: true},
		{name: "no_program", change: func(o *contracts.OutcomeRecord) {
			o.ProgramRuns = nil
			o.PlanApplications = nil
			o.ProgramToolCalls = nil
			o.RepairActions = nil
		}, items: []string{"0/question/uncertain/"}, model: true, unverified: true},
		{name: "partly_covered", fixed: program.NoMatch, change: func(o *contracts.OutcomeRecord) {
			o.ConfirmedRootCauses = append(o.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Summary: "another cause", Resources: []*contracts.ObjectRef{t1Ref("other")}})
			o.RepairActions = append(o.RepairActions, &contracts.RepairActionReceipt{ActionId: "manual-other", Kind: "manual", Target: "Service/app/other", Summary: "repair another cause", Details: "manual repair", StartedAt: timestamppb.New(time.Unix(99, 0)), CompletedAt: timestamppb.New(time.Unix(100, 0)), Success: true, Resources: []*contracts.ObjectRef{t1Ref("other")}})
		}, items: []string{"0/covered/verified_equivalent_repair/contract-repair", "1/question/unbound_write/"}, model: true, positive: 1, credit: 1},
		{name: "nil_final_proof", fixed: program.NoMatch, change: func(o *contracts.OutcomeRecord) { o.PlanApplications[0].FinalEquivalenceVerified = nil }, items: []string{"0/question/repair_equivalence_question/contract-repair"}, model: true},
		{name: "absent_fixed_verdict", fixed: "", items: []string{"0/question/uncertain/contract-repair"}, model: true},
		{name: "similar_repeat_current_false_match", fixed: program.Match, items: []string{"0/false_match/fixed_capture_match/contract-repair"}, model: true, negative: 1, prior: true},
	}
}

// The tiny accepted program uses a real recorded primitive. It intentionally
// keeps matching when a fixed recording still reports a source difference.
type t1Diagnoser struct{ id string }

func (d t1Diagnoser) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: d.id, Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}

func (d t1Diagnoser) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	diff, err := v.DiffAgainstSource(b.Objects["service"])
	result := program.NoMatch
	if diff.Changed {
		result = program.Match
	}
	return program.Verdict{Result: result}, err
}

func t1Captures(t *testing.T, dir string, c t1Class) {
	t.Helper()
	for _, file := range []string{"open@detect.json", "fixed.json"} {
		t1Write(t, dir, file, capturefixture.FromView(t, `{"namespace":"app"}`))
	}
	calls := []sdktest.PrimitiveCall{}
	for _, name := range []string{"api", "peer", "other"} {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: name}
		for _, phase := range []string{"open", "fixed"} {
			if phase == "fixed" && name == "api" && c.name == "absent_fixed_verdict" {
				continue
			}
			calls = append(calls, sdktest.PrimitiveCall{Phase: phase, Method: "DiffAgainstSource", Arguments: t1JSON(t, []any{ref}), Result: t1JSON(t, program.Diff{Changed: name == "api" && (phase == "open" || c.fixed == program.Match)})})
		}
	}
	t1Write(t, dir, "open@detect-recordings.json", t1JSON(t, calls))
}

func t1Items(work []*contracts.ReflectionCauseWork) []string {
	items := []string{}
	for _, cause := range work {
		for _, item := range cause.Items {
			items = append(items, fmt.Sprintf("%d/%s/%s/%s", cause.CauseIndex, item.Disposition, item.Reason, item.ProgramId))
		}
	}
	sort.Strings(items)
	return items
}

func t1AssertWork(t *testing.T, work []*contracts.ReflectionCauseWork, c t1Class) {
	t.Helper()
	want := append([]string(nil), c.items...)
	sort.Strings(want)
	if got := t1Items(work); !reflect.DeepEqual(got, want) {
		t.Errorf("typed work = %v; want %v", got, want)
	}
	for _, cause := range work {
		expectedResource := "api"
		if cause.CauseIndex == 1 {
			expectedResource = "other"
		}
		if len(cause.Resources) != 1 || !proto.Equal(cause.Resources[0], t1Ref(expectedResource)) {
			t.Errorf("confirmed cause identity changed: %v", cause)
		}
		for _, item := range cause.Items {
			if item.ProgramId != "" && (len(item.Bindings) != 1 || item.Bindings[0].Name != "service" || !proto.Equal(item.Bindings[0].Object, t1Ref("api"))) {
				t.Errorf("matched binding identity changed: %v", item)
			}
			if item.Reason == "unbound_write" && (len(item.Resources) != 1 || !proto.Equal(item.Resources[0], t1Ref(expectedResource))) {
				t.Errorf("unbound resource identity changed: %v", item)
			}
			if item.Disposition == "question" && item.Question == "" {
				t.Error("question has no actionable text")
			}
		}
	}
}

func TestT1ClassificationMatrix(t *testing.T) {
	for _, c := range t1Classes() {
		t.Run(c.name, func(t *testing.T) {
			o := t1Outcome(t, "revision")
			if c.change != nil {
				c.change(o)
			}
			fixed := map[string]program.Result{}
			if c.fixed != "" {
				fixed[matchedViewKey(t1Program, program.Bindings{Objects: program.ObjectBindings{"service": objectRefState(t1Ref("api"))}})] = c.fixed
			}
			work := classifyCauseWork(o, fixed)
			capture := t.TempDir()
			t1Captures(t, capture, c)
			// An absent verdict can also mean no registry; its recordings are still
			// present. This case isolates classification from evidence indexing.
			if c.name == "absent_fixed_verdict" {
				t1Captures(t, capture, t1Class{fixed: program.NoMatch})
			}
			t1AssertWork(t, work, c)
		})
	}
}

type t1Run struct {
	calls, positive, negative int
	work                      []*contracts.ReflectionCauseWork
	unassigned                []*contracts.ReflectionWorkItem
	outcome                   *contracts.OutcomeRecord
	ledger                    *contracts.BrokerLedger
}

func TestT1EveryUnboundWriteGetsAQuestion(t *testing.T) {
	for _, grouped := range []bool{true, false} {
		t.Run(fmt.Sprintf("grouped=%t", grouped), func(t *testing.T) {
			o := t1Outcome(t, "revision")
			o.ProgramRuns = nil
			o.PlanApplications = nil
			o.ProgramToolCalls = nil
			o.RepairActions[0].Resources = []*contracts.ObjectRef{t1Ref("first"), t1Ref("second"), t1Ref("first")}
			var items []*contracts.ReflectionWorkItem
			if grouped {
				o.ConfirmedRootCauses[0].Resources = []*contracts.ObjectRef{t1Ref("first"), t1Ref("second")}
				items = classifyCauseWork(o, nil)[0].Items
			} else {
				o.ConfirmedRootCauses = nil
				items = unassignedReflectionQuestions(o, nil)
			}
			seen := map[string]int{}
			for _, item := range items {
				if item.Reason == "unbound_write" {
					for _, ref := range item.Resources {
						seen[ref.Name]++
					}
				}
			}
			if !reflect.DeepEqual(seen, map[string]int{"first": 1, "second": 1}) {
				t.Errorf("work list omitted or duplicated repaired resources: %v", seen)
			}
		})
	}
}

func TestT1FixedReplayIsIndependentOfProgramOrder(t *testing.T) {
	ctx := context.Background()
	o := t1Outcome(t, "revision")
	second := proto.Clone(o.ProgramRuns[0]).(*contracts.ProgramRun)
	second.ProgramId = "another-repair"
	o.ProgramRuns = append(o.ProgramRuns, second)
	registry, err := program.NewRegistry([]program.Diagnoser{t1Diagnoser{t1Program}, t1Diagnoser{"another-repair"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	capture := t.TempDir()
	t1Captures(t, capture, t1Class{fixed: program.Match})
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reversed=%t", reverse), func(t *testing.T) {
			if reverse {
				o.ProgramRuns[0], o.ProgramRuns[1] = o.ProgramRuns[1], o.ProgramRuns[0]
			}
			verdicts := fixedCaptureVerdicts(ctx, o, registry, capture)
			for _, run := range o.ProgramRuns {
				key := matchedViewKey(run.ProgramId, program.Bindings{Objects: program.ObjectBindings{"service": objectRefState(t1Ref("api"))}})
				if verdicts[key] != program.Match {
					t.Errorf("program %s lost shared immutable observation: %s", run.ProgramId, verdicts[key])
				}
			}
		})
	}
}

func TestT1L2MinimalRegressionShapes(t *testing.T) {
	// Deliberately hand-written reductions, not exported run data. These retain
	// only the ownership/capture relationships identified by LM1.
	for _, readiness := range []bool{true, false} {
		t.Run(fmt.Sprintf("readiness=%t", readiness), func(t *testing.T) {
			o := t1Outcome(t, "unused")
			o.ProgramRuns, o.PlanApplications, o.ProgramToolCalls = nil, nil, nil
			deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			service := t1Ref("api")
			configMap := &contracts.ObjectRef{ApiVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "startup"}
			o.ConfirmedRootCauses[0].Resources = []*contracts.ObjectRef{deployment, service}
			o.RepairActions[0].Resources = []*contracts.ObjectRef{deployment}
			refs := []*contracts.ObjectRef{deployment, service}
			if !readiness {
				o.ConfirmedRootCauses[0].Resources = append(o.ConfirmedRootCauses[0].Resources, configMap)
				o.RepairActions[0].Resources = []*contracts.ObjectRef{configMap}
				refs = []*contracts.ObjectRef{deployment}
			}
			calls := []sdktest.PrimitiveCall{}
			for _, ref := range refs {
				for _, phase := range []string{"open", "fixed"} {
					calls = append(calls, sdktest.PrimitiveCall{Phase: phase, Method: "DiffAgainstSource", Arguments: t1JSON(t, []any{objectRefState(ref)}), Result: t1JSON(t, program.Diff{Changed: true, Text: `accepted: {"probePort":8080}
observed: {"probePort":8080}`})})
				}
			}
			capture := t.TempDir()
			t1Write(t, capture, "open@detect-recordings.json", t1JSON(t, calls))
			work := classifyCauseWork(o, nil)
			if work[0].Items[0].Disposition != "question" || work[0].Items[0].Reason != "unbound_write" {
				t.Errorf("unproven learning was silently covered: %v", work)
			}
			if state := ProgramTrustStats([]*contracts.OutcomeRecord{o})[t1Program]; state.VerifiedSuccesses != 0 {
				t.Errorf("unproven historical shape earned credit: %+v", state)
			}
		})
	}
}

const t1ProgramSource = `package contractrepair
import ("context"; "sdo.dev/operator/sdk/program")
type Program struct{}
func (Program) Spec() program.ProgramSpec {return program.ProgramSpec{ID:"contract-repair",Params:[]program.ParamSpec{{Name:"service",Kind:"Service",Required:true}}}}
func (Program) Mitigate(_ context.Context, _ program.ReadView,b program.Bindings)(program.Plan,error){return program.Plan{Actions:[]program.Action{program.ReconcileFromSource{Ref:b.Objects["service"],Reason:"restore accepted source"}},Rationale:"restore accepted source"},nil}
func (Program) Diagnose(_ context.Context, v program.ReadView,b program.Bindings)(program.Verdict,error){d,e:=v.DiffAgainstSource(b.Objects["service"]);r:=program.NoMatch;if d.Changed {r=program.Match};return program.Verdict{Result:r},e}
`

