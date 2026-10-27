package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// programCountsFor sums one program's counts across revisions.
func programCountsFor(counts map[string]ProgramCount, id string) ProgramCount {
	var total ProgramCount
	for key, count := range counts {
		if len(key) > len(id) && key[:len(id)+1] == id+"@" {
			total.Successes += count.Successes
			total.Failures += count.Failures
		}
	}
	return total
}

func countedOutcome(classification, revision string, application *contracts.PlanApplication, signals ...string) *contracts.OutcomeRecord {
	application.ProgramIds = []string{"repair"}
	return &contracts.OutcomeRecord{Classification: classification, SourceCommit: "source", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "repair", ProgramRevision: revision}}, PlanApplications: []*contracts.PlanApplication{application}, ProgramFailureSignals: signals}
}

func TestProgramCountsAreInformationPerProgramRevision(t *testing.T) {
	applied := func() *contracts.PlanApplication { return &contracts.PlanApplication{Applied: true} }
	rolledBack := &contracts.PlanApplication{Applied: true, RolledBack: true}
	dryRun := &contracts.PlanApplication{Applied: true, DryRun: true}
	pending := &contracts.PlanApplication{Applied: true, RolloutPending: &contracts.RolloutPending{Reason: "peer admission blocks rollout"}}
	notApplied := &contracts.PlanApplication{}
	for _, test := range []struct {
		name     string
		outcomes []*contracts.OutcomeRecord
		want     map[string]ProgramCount
	}{
		{"healthy close is a success", []*contracts.OutcomeRecord{countedOutcome("success", "a", applied())}, map[string]ProgramCount{"repair@a": {Successes: 1}}},
		{"unsuccessful close is a failure", []*contracts.OutcomeRecord{countedOutcome("failed", "a", applied())}, map[string]ProgramCount{"repair@a": {Failures: 1}}},
		{"rollback is a failure", []*contracts.OutcomeRecord{countedOutcome("success", "a", rolledBack)}, map[string]ProgramCount{"repair@a": {Failures: 1}}},
		{"dry run, pending rollout and unapplied plans count nothing", []*contracts.OutcomeRecord{countedOutcome("success", "a", dryRun), countedOutcome("failed", "a", pending), countedOutcome("success", "a", notApplied)}, map[string]ProgramCount{}},
		{"revisions count apart", []*contracts.OutcomeRecord{countedOutcome("success", "a", applied()), countedOutcome("failed", "b", applied()), countedOutcome("success", "b", applied())}, map[string]ProgramCount{"repair@a": {Successes: 1}, "repair@b": {Successes: 1, Failures: 1}}},
		{"a recorded verification failure counts once with its failed application", []*contracts.OutcomeRecord{countedOutcome("success", "a", rolledBack, "application_verification_failure:repair@a")}, map[string]ProgramCount{"repair@a": {Failures: 1}}},
		{"a verification failure with no failed application still counts", []*contracts.OutcomeRecord{countedOutcome("success", "a", applied(), "application_verification_failure:repair@a")}, map[string]ProgramCount{"repair@a": {Successes: 1, Failures: 1}}},
		{"a failure signal alone counts against its revision", []*contracts.OutcomeRecord{{SourceCommit: "source", ProgramFailureSignals: []string{"application_verification_failure:repair@a"}}}, map[string]ProgramCount{"repair@a": {Failures: 1}}},
		{"a run-less outcome falls back to its source commit", []*contracts.OutcomeRecord{{Classification: "success", SourceCommit: "source", PlanApplications: []*contracts.PlanApplication{{Applied: true, ProgramIds: []string{"repair"}}}}}, map[string]ProgramCount{"repair@source": {Successes: 1}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := ProgramCounts(test.outcomes)
			if len(got) != len(test.want) {
				t.Fatalf("counts=%v want %v", got, test.want)
			}
			for key, want := range test.want {
				if got[key] != want {
					t.Fatalf("%s=%+v want %+v (all %v)", key, got[key], want, got)
				}
			}
		})
	}
}

type failingOutcomeReader struct{ priorOutcomeReader }

func (failingOutcomeReader) List(context.Context) ([]memory.Outcome, error) {
	return nil, errors.New("outcome store unavailable")
}

func TestProgramCountsReaderSkipsUnreadableOutcomesAndNeverBlocksDispatch(t *testing.T) {
	good := countedOutcome("success", "a", &contracts.PlanApplication{Applied: true})
	dispatcher := KubernetesJobDispatcher{Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "unreadable"}, {IncidentID: "good", Record: good}}}}
	if got := dispatcher.programCounts(context.Background())["repair@a"]; got != (ProgramCount{Successes: 1}) {
		t.Fatalf("unreadable outcome changed the counts: %+v", got)
	}
	if counts := (KubernetesJobDispatcher{Outcomes: failingOutcomeReader{}}).programCounts(context.Background()); len(counts) != 0 {
		t.Fatalf("failed listing produced counts: %v", counts)
	}
	if counts := (KubernetesJobDispatcher{}).programCounts(context.Background()); counts != nil {
		t.Fatalf("no outcome store produced counts: %v", counts)
	}
}

// preparingDispatcher stands in for the agent: it receives the request exactly
// as the real dispatcher freezes it into the agent task.
type preparingDispatcher struct {
	KubernetesJobDispatcher
	requests chan IncidentRequest
}

func (d preparingDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	prepared, err := d.PrepareDispatch(ctx, request)
	if err != nil {
		return IncidentResult{}, err
	}
	d.requests <- prepared
	return completedResult(request.IncidentID), nil
}

// The controller never applies a learned program itself: an incident whose
// finding matches a learned playbook goes straight to the agent, carrying the
// match and its past counts, with no write made beforehand.
func TestMatchedLearnedPlaybookReachesTheAgentWithCountsAndNoPriorWrite(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	p := loopProgram{candidateProgram: candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}, ref: ref}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"ports": []any{map[string]any{"targetPort": float64(9999)}}})}}
	rollouts := &planFakeRollout{}
	provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}
	finding := stateFinding("health")
	finding.PrimaryResource = ref
	detector := controllerDetector("health", time.Second, finding)
	detector.spec.Playbooks = []string{"targetport"}

	config := testControllerConfig()
	config.Namespace, config.FiringThreshold = "app", 1
	earlier := func(classification string) memory.Outcome {
		return memory.Outcome{IncidentID: "earlier-" + classification, Record: &contracts.OutcomeRecord{Classification: classification, ProgramRuns: []*contracts.ProgramRun{{ProgramId: "targetport", ProgramRevision: config.SourceCommit}}, PlanApplications: []*contracts.PlanApplication{{Applied: true, ProgramIds: []string{"targetport"}}}, Timestamps: &contracts.OutcomeTimestamps{CompletedAt: timestamppb.New(time.Unix(1, 0))}}}
	}
	dispatcher := preparingDispatcher{requests: make(chan IncidentRequest, 1), KubernetesJobDispatcher: KubernetesJobDispatcher{
		Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), Namespace: "app",
		Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{earlier("success"), earlier("success"), earlier("failed")}},
		LoopFactory: func(request IncidentRequest) *ResponderLoop {
			loop := NewResponderLoop(request, registry, provider, []sdk.Detector{detector}, PlanExecutor{Client: client, Rollouts: rollouts, Verifier: planFakeVerifier{passed: true}}, 5)
			loop.ReadView = func(sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
			return loop
		},
	}}
	controller, err := NewController(config, []sdk.Detector{detector}, provider, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Step(context.Background(), time.Unix(0, 0), nil); err != nil {
		t.Fatal(err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)

	matches := request.PlaybookCoverage.GetMatches()
	if len(matches) != 1 || matches[0].GetPlaybookId() != "targetport" || !matches[0].GetCanMitigate() ||
		matches[0].GetVerifiedSuccesses() != 2 || matches[0].GetVerifiedFailures() != 1 {
		t.Fatalf("agent request lacks the learned match with its counts: %v", request.PlaybookCoverage)
	}
	if len(client.patches) != 0 || rollouts.calls != 0 {
		t.Fatalf("SDO wrote before the agent: patches=%d rollouts=%d", len(client.patches), rollouts.calls)
	}
}
