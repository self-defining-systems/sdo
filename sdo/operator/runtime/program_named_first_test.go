package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func deployRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name}
}

func deployRole() []program.ParamSpec {
	return []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}
}

// A detector-named candidate is diagnosed before generic fan-out starts, and
// generic fan-out is bounded, so a wide set of generic findings cannot starve it.
func TestNamedCandidateRunsBeforeBoundedGenericFanOut(t *testing.T) {
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "app"}}}}
	var findings []sdk.Finding
	for index := range 30 {
		name := fmt.Sprintf("worker-%02d", index)
		snapshot.DeploymentList = append(snapshot.DeploymentList, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}})
		findings = append(findings, sdk.Finding{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Fingerprint: name, PrimaryResource: deployRef(name)})
	}
	var namedDone atomic.Bool
	var inFlight, peak, startedBeforeNamed atomic.Int32
	diagnosers := []program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "named-pb", Params: deployRole()}, run: func(context.Context, program.Bindings) program.Verdict {
		namedDone.Store(true)
		return program.Verdict{Result: program.Match}
	}}}
	for _, id := range []string{"generic-a", "generic-b", "generic-c"} {
		diagnosers = append(diagnosers, candidateProgram{spec: program.ProgramSpec{ID: id, Params: deployRole()}, run: func(ctx context.Context, _ program.Bindings) program.Verdict {
			if !namedDone.Load() {
				startedBeforeNamed.Add(1)
			}
			now := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				old := peak.Load()
				if now <= old || peak.CompareAndSwap(old, now) {
					break
				}
			}
			<-ctx.Done()
			return program.Verdict{Result: program.Unknown}
		}})
	}
	registry, err := program.NewRegistry(diagnosers, nil)
	if err != nil {
		t.Fatal(err)
	}
	findings = append(findings, sdk.Finding{DetectorID: "journey", RuleID: "journey", Status: sdk.FindingActive, Fingerprint: "journey", PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "gateway"},
		Playbooks: []string{"named-pb"}, ParameterBindings: map[string]sdk.ObjectRef{"deployment": deployRef("worker-07")}})
	runs := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{Snapshot: snapshot}, findings, 400*time.Millisecond)
	var named *ProgramRun
	generic := 0
	for index, run := range runs {
		if run.ProgramID == "named-pb" && run.Bindings.Objects["deployment"].Name == "worker-07" && run.FindingFingerprint == "journey" {
			named = &runs[index]
		} else {
			generic++
		}
	}
	if named == nil || named.Verdict.Result != program.Match || named.Bindings.Objects["deployment"].Name != "worker-07" || named.BindingSource != "hint" {
		t.Fatalf("named run = %+v, want a MATCH on worker-07 with source hint", named)
	}
	if generic < 90 {
		t.Fatalf("generic runs = %d, want generic fan-out still reported", generic)
	}
	if startedBeforeNamed.Load() != 0 {
		t.Fatalf("%d generic diagnoses started before the named one finished", startedBeforeNamed.Load())
	}
	if peak.Load() > GenericDiagnosisWorkers {
		t.Fatalf("generic concurrency peaked at %d, want at most %d", peak.Load(), GenericDiagnosisWorkers)
	}
}

// A finding that lists playbook A binds A, never program B by resource.
func TestFindingListingPlaybookBindsOnlyThatProgram(t *testing.T) {
	var diagnosers []program.Diagnoser
	for _, id := range []string{"pb-a", "pb-b"} {
		diagnosers = append(diagnosers, candidateProgram{spec: program.ProgramSpec{ID: id, Params: deployRole()}, run: func(context.Context, program.Bindings) program.Verdict {
			return program.Verdict{Result: program.Match}
		}})
	}
	registry, err := program.NewRegistry(diagnosers, nil)
	if err != nil {
		t.Fatal(err)
	}
	finding := sdk.Finding{DetectorID: "d", RuleID: "d", Status: sdk.FindingActive, Fingerprint: "d", PrimaryResource: deployRef("worker-01"), Playbooks: []string{"pb-a"}}
	if _, err := program.BindFinding(program.ProgramSpec{ID: "pb-b", Params: deployRole()}, finding); err == nil {
		t.Fatal("BindFinding bound a program the finding does not list")
	}
	if _, err := program.BindFinding(program.ProgramSpec{ID: "pb-a", Params: deployRole()}, finding); err != nil {
		t.Fatal(err)
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "worker-01", Namespace: "app"}}}}
	runs := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{Snapshot: snapshot}, []sdk.Finding{finding}, time.Second)
	if len(runs) != 1 || runs[0].ProgramID != "pb-a" {
		t.Fatalf("runs = %+v, want only pb-a", runs)
	}
}

// The automatic stage applies a detector-named match, never a generic one.
func TestAutomaticStageAppliesOnlyDetectorNamedMatches(t *testing.T) {
	named := sdk.Finding{DetectorID: "journey", RuleID: "journey", Status: sdk.FindingActive, Fingerprint: "named", PrimaryResource: deployRef("worker-01"), Playbooks: []string{"pb-named"}}
	other := sdk.Finding{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Fingerprint: "other", PrimaryResource: deployRef("worker-02")}
	bindings := func(name string) program.Bindings {
		return program.Bindings{Objects: program.ObjectBindings{"deployment": deployRef(name)}}
	}
	match := func(id, name string) *contracts.PlaybookMatch {
		return &contracts.PlaybookMatch{PlaybookId: id, Bindings: programBindingsContract(bindings(name)), CanMitigate: true}
	}
	runs := []ProgramRun{
		{ProgramID: "pb-named", Bindings: bindings("worker-01"), Verdict: program.Verdict{Result: program.Match}, FindingFingerprint: FindingFingerprint(named)},
		{ProgramID: "pb-generic", Bindings: bindings("worker-02"), Verdict: program.Verdict{Result: program.Match}, FindingFingerprint: FindingFingerprint(other)},
	}
	state := &IncidentState{
		Request:  IncidentRequest{Findings: []sdk.Finding{named, other}, InitialProgramRuns: runs},
		Findings: []sdk.Finding{named, other},
		Coverage: &contracts.PlaybookCoverage{Matches: []*contracts.PlaybookMatch{match("pb-generic", "worker-02"), match("pb-named", "worker-01")}},
	}
	got := stageCandidates(state)
	if len(got) != 1 || got[0].PlaybookId != "pb-named" {
		t.Fatalf("stage candidates = %+v, want only pb-named", got)
	}
}
