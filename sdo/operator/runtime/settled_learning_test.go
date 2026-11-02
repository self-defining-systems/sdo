package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"sdo.dev/operator/internal/capturefixture"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestAllUnboundWritesSurviveGroupingAndDuplicateReceipts(t *testing.T) {
	a := &contracts.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "a"}
	b := &contracts.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "b"}
	c := &contracts.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "c"}
	d := &contracts.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "d"}
	o := &contracts.OutcomeRecord{Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{a, b, a}}}, RepairActions: []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{a, b, c}}, {Success: true, Resources: []*contracts.ObjectRef{b, c, d}}}}
	grouped := classifyCauseWork(o, nil)
	if len(grouped) != 1 || len(grouped[0].Items) != 2 {
		t.Fatalf("lost grouped writes: %v", grouped)
	}
	unassigned := unassignedReflectionQuestions(o, nil)
	if len(unassigned) != 2 || !proto.Equal(unassigned[0].Resources[0], c) || !proto.Equal(unassigned[1].Resources[0], d) {
		t.Fatalf("lost unassigned writes: %v", unassigned)
	}
}

type sharedProbeProgram struct{ id string }

func (p sharedProbeProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}

func (p sharedProbeProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	_, err := v.ProbeTCP(b.Objects["service"], 80)
	if err != nil {
		return program.Verdict{Result: program.Unknown}, err
	}
	return program.Verdict{Result: program.Match}, nil
}

func TestIndependentFixedDiagnosesShareImmutableRecordingWithoutConsumption(t *testing.T) {
	capture := t.TempDir()
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	args, _ := json.Marshal([]any{ref, 80})
	calls, _ := json.Marshal([]sdktest.PrimitiveCall{{Phase: "fixed", Method: "ProbeTCP", Arguments: args, Result: json.RawMessage(`{}`)}})
	// A pre-apply-only observation must be available to the same classification.
	if err := os.WriteFile(filepath.Join(capture, "open@pre-apply-recordings.json"), calls, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(capture, "fixed.json"), capturefixture.Raw(t, "empty fixed view: only recordings answer", []byte(`{}`)), 0600); err != nil {
		t.Fatal(err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{sharedProbeProgram{"probe-a"}, sharedProbeProgram{"probe-b"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	o := &contracts.OutcomeRecord{}
	for _, id := range []string{"probe-a", "probe-b"} {
		o.ProgramRuns = append(o.ProgramRuns, programRunContract(ProgramRun{ProgramID: id, Bindings: program.Bindings{Objects: program.ObjectBindings{"service": ref}}, Verdict: program.Verdict{Result: program.Match}}))
	}
	for range 2 {
		verdicts := fixedCaptureVerdicts(context.Background(), o, registry, capture)
		for _, run := range o.ProgramRuns {
			if verdicts[matchedViewKey(run.ProgramId, program.Bindings{Objects: program.ObjectBindings{"service": ref}})] != program.Match {
				t.Fatalf("shared read consumed: %v", verdicts)
			}
		}
		slices.Reverse(o.ProgramRuns)
	}
}

type failingFinalSnapshot struct{}

func (failingFinalSnapshot) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return nil, errors.New("final snapshot unavailable")
}

func TestUnavailableCompletionSnapshotClearsExistingFinalProof(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, false, true)
	loop.applicationsLog = []RecordedPlanApplication{{Result: PlanApplicationResult{Applied: true, FinalEquivalenceVerified: proto.Bool(true)}}}
	loop.Provider = failingFinalSnapshot{}
	loop.CreditEquivalentRepairs(context.Background(), []RepairActionReceipt{{Success: true}})
	if loop.PlanApplications()[0].Result.FinalEquivalenceVerified != nil {
		t.Fatal("snapshot failure retained an affirmative proof")
	}
}

type countingCompletionClient struct {
	PlanObjectClient
	reads map[sdk.ObjectRef]int
}

func (c countingCompletionClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	c.reads[ref]++
	return c.PlanObjectClient.Get(ctx, ref)
}

func TestCompletionReadsEachFinalObjectOnceAcrossApplications(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, false, true)
	run := loop.ProgramRuns()[0]
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: run.ProgramID, Bindings: run.Bindings})
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" {
		t.Fatal(applied.Error)
	}
	// Repeated historical applications share a final object and its read error.
	app := loop.applicationsLog[0]
	loop.applicationsLog = append(loop.applicationsLog, app, app)
	counted := countingCompletionClient{PlanObjectClient: loop.Executor.Client, reads: map[sdk.ObjectRef]int{}}
	loop.Executor.Client = counted
	loop.CreditEquivalentRepairs(context.Background(), []RepairActionReceipt{{Success: true, Resources: []sdk.ObjectRef{run.Bindings.Objects["service"]}}})
	if len(counted.reads) != 1 {
		t.Fatalf("final objects: %v", counted.reads)
	}
	for _, n := range counted.reads {
		if n != 1 {
			t.Fatalf("repeated final GETs: %v", counted.reads)
		}
	}
}

func TestHistoricalOutcomeWithoutSettlementCannotCreateSuccessCredit(t *testing.T) {
	o := verifiedTrustOutcome(t, "targetport", "revision")
	o.ReflectionEligibility = nil
	if got := ProgramTrustStats([]*contracts.OutcomeRecord{o})["targetport"]; got.VerifiedSuccesses != 0 || (got.VerifiedSuccesses > 0 && got.Failures == 0) {
		t.Fatalf("historical compatibility created proof: %+v", got)
	}
}

