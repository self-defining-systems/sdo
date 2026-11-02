//go:build integration

package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func podRestartLoop(t *testing.T) (*ResponderLoop, *IncidentState, *atomic.Int32) {
	t.Helper()
	loop, state := pipelineTestLoop(t, true)
	registry, err := program.NewRegistry([]program.Diagnoser{podRestartProgram{}}, []program.Mitigator{podRestartProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	client := loop.Executor.Client.(TypedPlanClient).Client.(interface {
		PrependReactor(string, string, ktesting.ReactionFunc)
		Tracker() ktesting.ObjectTracker
	})
	// The ReplicaSet controller's part: a deleted Pod is replaced by one that
	// fails the same way.
	deletes := &atomic.Int32{}
	client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
		n := deletes.Add(1)
		replacement := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("api-pod-%d", n), Namespace: "app", UID: types.UID(fmt.Sprintf("synthetic-pod-%d", n)), ResourceVersion: "1", Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "missing:v1"}}}, Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: "api", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}}}}
		return false, nil, client.Tracker().Add(replacement)
	})
	pods := loop.Executor.Client.(TypedPlanClient).Client.CoreV1().Pods("app")
	pod, err := pods.Get(context.Background(), "api-pod", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "synthetic-pod"
	if _, err := pods.Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	detector := podRestartDetector{}
	loop.Programs = registry
	loop.Detectors = []sdk.Detector{detector}
	loop.Executor.Verifier = PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: loop.Provider}
	snapshot, err := loop.Provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	request := state.Request
	request.Findings = findings
	loop.Request, loop.findings = request, findings
	request.InitialProgramRuns = loop.InitialDiagnose(context.Background())
	request.PlaybookCoverage = buildPlaybookCoverage(findings, request.InitialProgramRuns, loop.lastDiagnosisView, nil)
	loop.Request = request
	return loop, &IncidentState{Request: request, Findings: findings, Coverage: request.PlaybookCoverage}, deletes
}

// A program that applied while its finding stayed is ineffective for that
// finding in this incident. The restart's own replacement Pod is a new
// binding, not new evidence, so a later pass skips it as typed "ineffective"
// instead of restarting the next Pod.
func TestIneffectiveProgramRunsOnceThenHandsOff(t *testing.T) {
	ctx := context.Background()
	loop, state, deletes := podRestartLoop(t)
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
	report, err := stage.Run(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mitigations) != 1 || report.Mitigations[0].Result != "applied_still_matching" || deletes.Load() != 1 {
		t.Fatalf("opening pass: deletes=%d report=%v", deletes.Load(), report)
	}
	state.PriorStages = []*contracts.StageReport{report}
	for pass := 0; pass < 2; pass++ {
		again, err := RunStageAgain(ctx, state, stage)
		if err != nil {
			t.Fatal(err)
		}
		if deletes.Load() != 1 {
			t.Fatalf("pass %d restarted the replacement Pod: deletes=%d report=%v", pass, deletes.Load(), again)
		}
		skipped := 0
		for _, attempt := range again.Mitigations {
			if attempt.Result == "skipped" && attempt.SkipReason == "ineffective" {
				skipped++
			}
		}
		if skipped == 0 {
			t.Fatalf("pass %d: the replacement-Pod match is not typed ineffective: %v", pass, again)
		}
	}
	open := false
	for _, finding := range state.Findings {
		open = open || finding.RuleID == "image-pull"
	}
	if !open {
		t.Fatalf("the finding must stay open for the responder: %v", state.Findings)
	}
}

// A turn over the matches that writes nothing and clears nothing leaves the
// cluster as it was, so a second turn would diagnose the same state again.
// The stage hands off after the first.
func TestNoProgressStageHandsOffAfterOneTurn(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	calls := &atomic.Int32{}
	port := countingUnknownProgram{Diagnoser: pipelinePortProgram{}, Mitigator: pipelinePortProgram{}, calls: calls}
	image := countingUnknownProgram{Diagnoser: pipelineImageProgram{}, Mitigator: pipelineImageProgram{}, calls: calls}
	registry, err := program.NewRegistry([]program.Diagnoser{port, image}, []program.Mitigator{port, image})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mitigations) != 2 || len(loop.PlanApplications()) != 0 || len(state.Findings) != 2 {
		t.Fatalf("report=%v findings=%v", report, state.Findings)
	}
	if calls.Load() != 2 {
		t.Fatalf("a no-progress turn was repeated: %d diagnoses for 2 matches", calls.Load())
	}
}

