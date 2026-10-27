package runtime

import (
	"context"
	"errors"
	"fmt"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

func admissionRollouts() KubernetesPlanRollouts { return admissionRolloutsFor("frontend") }

func admissionRolloutsFor(name string) KubernetesPlanRollouts {
	n := int32(1)
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: "d", Generation: 2}, Spec: appsv1.DeploymentSpec{Replicas: &n}, Status: appsv1.DeploymentStatus{ObservedGeneration: 2}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "frontend-next", Namespace: "app", UID: "rs", OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", UID: d.UID}}}, Spec: appsv1.ReplicaSetSpec{Replicas: &n}}
	event := &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "denied", Namespace: "app"}, Type: corev1.EventTypeWarning, InvolvedObject: corev1.ObjectReference{Kind: "ReplicaSet", Name: rs.Name, Namespace: "app", UID: rs.UID}, LastTimestamp: metav1.NewTime(time.Now().Add(time.Minute)), Reason: "FailedCreate", Message: `Error creating: admission webhook "validate.example.test" denied the request: policy rejects Pods`}
	config := &admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "app-policy"}, Webhooks: []admissionv1.ValidatingWebhook{{Name: "validate.example.test"}}}
	return KubernetesPlanRollouts{Client: fake.NewSimpleClientset(d, rs, event, config)}
}

func TestAdmissionBlockedStopsRolloutAndPreservesAppliedRepair(t *testing.T) {
	for _, poll := range []bool{false, true} {
		t.Run(map[bool]string{false: "stage", true: "agent"}[poll], func(t *testing.T) {
			loop, cluster := menuB3Loop(t)
			e := loop.Executor
			e.Verifier = planFakeVerifier{true}
			e.Rollouts = admissionRollouts()
			e.PollRollouts = poll
			e.RolloutTimeout = 2 * time.Second
			started := time.Now()
			result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions", ResponseDeadline: time.Now().Add(3 * time.Second)}, program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: menuDeployment, Replicas: testPlanValue(1)}}}, false)
			var blocked *AdmissionBlockedError
			if !errors.As(err, &blocked) || !strings.Contains(err.Error(), "validate.example.test") || result.RolloutPending == nil || len(result.RolloutPending.GetAdmissionConfigurations()) != 1 || result.RolloutPending.AdmissionConfigurations[0].Name != "app-policy" {
				t.Fatalf("missing typed admission blocker: %+v %v", result, err)
			}
			if time.Since(started) > time.Second {
				t.Fatal("waited on an admission prerequisite")
			}
			if !result.Applied || result.RolledBack || result.VerificationCompleted {
				t.Fatalf("applied repair lost or credited as healthy: %+v", result)
			}
			if value, _ := testField(cluster.objects[menuDeployment], "spec.replicas"); value != float64(1) {
				t.Fatal("write undone")
			}
		})
	}
}

func TestAdmissionBlockedStageHandsOffWithoutUndoOrRepeat(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	loop.Executor.Rollouts = admissionRolloutsFor("api")
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, Budget: 20 * time.Second}
	started := time.Now()
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("stage waited on admission prerequisite")
	}
	applied := 0
	for _, attempt := range report.Mitigations {
		if attempt.GetApplyReceipt().GetApplied() {
			applied++
			if attempt.Result != "admission_blocked" || attempt.ApplyReceipt.GetRolledBack() || !strings.Contains(attempt.Error, "app-policy") {
				t.Fatalf("stage lost admission handoff or repair: %v", attempt)
			}
		}
	}
	if applied == 0 {
		t.Fatal("stage never applied repair")
	}
	before := len(loop.PlanApplications())
	state.PriorStages = append(state.PriorStages, report)
	if _, err := stage.Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if len(loop.PlanApplications()) != before {
		t.Fatal("stage repeated a previously applied repair")
	}
}

func TestAdmissionRejectionClassifiesPrerequisitesOnly(t *testing.T) {
	for _, message := range []string{
		`failed calling webhook "validate.example.test": x509: certificate signed by unknown authority`,
		`admission webhook "validate.example.test" denied the request: policy`,
		`pods denied by policy require-labels`,
	} {
		if !admissionRejection(message) {
			t.Errorf("missed admission prerequisite: %s", message)
		}
	}
	for _, message := range []string{"FailedScheduling: insufficient cpu", "image pull access denied", "webhook backend container crashed", "quota exceeded"} {
		if admissionRejection(message) {
			t.Errorf("false admission prerequisite: %s", message)
		}
	}
}

func TestAdmissionBlockedIgnoresForeignAndOldReplicaSetEvents(t *testing.T) {
	for _, old := range []bool{false, true} {
		rollout := admissionRollouts()
		rs, err := rollout.Client.AppsV1().ReplicaSets("app").Get(context.Background(), "frontend-next", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if old {
			rs.Spec.Template.Labels = map[string]string{"old": "template"}
		} else {
			rs.OwnerReferences[0].UID = "foreign"
		}
		if _, err := rollout.Client.AppsV1().ReplicaSets("app").Update(context.Background(), rs, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		err = rollout.Observe(context.Background(), []sdk.ObjectRef{menuDeployment})
		var blocked *AdmissionBlockedError
		if errors.As(err, &blocked) {
			t.Fatalf("foreign/old ReplicaSet denied current rollout: %v", err)
		}
	}
}

func TestAdmissionDenialAppearingDuringRolloutStopsNextObservation(t *testing.T) {
	rollout := admissionRollouts()
	client := rollout.Client.(*fake.Clientset)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	event, err := client.CoreV1().Events("app").Get(ctx, "denied", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CoreV1().Events("app").Delete(ctx, event.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	reads := 0
	client.PrependReactor("get", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		reads++
		if reads == 2 {
			if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("events"), event, "app"); err != nil {
				return true, nil, err
			}
		}
		return false, nil, nil
	})
	started := time.Now()
	err = rollout.Wait(ctx, []sdk.ObjectRef{menuDeployment})
	var blocked *AdmissionBlockedError
	if !errors.As(err, &blocked) || reads != 2 || time.Since(started) > 3*time.Second {
		t.Fatalf("new admission evidence did not stop polling promptly: reads=%d elapsed=%s error=%v", reads, time.Since(started), err)
	}
}

type admissionClearDetector struct{ pipelineDetector }

func (admissionClearDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

// Old Ready replicas may clear health while admission rejects new Pods. The
// real pipeline must still send the prerequisite to the agent immediately.
func TestAdmissionBlockedStageHandsOffEvenWhenHealthDetectorsClear(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	loop.Detectors = []sdk.Detector{admissionClearDetector{}}
	loop.Executor.Rollouts = admissionRolloutsFor("api")
	applications := 0
	for pass := 0; pass < 2; pass++ {
		dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
		err := RunResponseStages(context.Background(), state, []ResponseStage{
			&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }},
			AgentResponseStage{Dispatcher: dispatcher},
		})
		if err != nil {
			t.Fatal(err)
		}
		select {
		case request := <-dispatcher.requests:
			found := false
			for _, finding := range request.Findings {
				if finding.DetectorID == "controller.rollout" && strings.Contains(finding.Evidence, "validate.example.test") {
					found = true
				}
			}
			if !found || len(request.PriorStages) != 1 {
				t.Fatalf("pass %d agent lost admission prerequisite: %+v", pass, request)
			}
		default:
			t.Fatalf("pass %d clear health hid the admission prerequisite and skipped agent handoff", pass)
		}
		if pass == 0 {
			applications = len(loop.PlanApplications())
		} else if len(loop.PlanApplications()) != applications {
			t.Fatal("rejoin repeated the repair")
		}
		// Rejoin from the durable completed stage checkpoint, with fresh health.
		state = &IncidentState{Request: state.Request, Findings: append([]sdk.Finding(nil), state.Request.Findings...), Coverage: state.Coverage}
	}
}

func TestAdmissionStaleEvidenceDoesNotAbortCurrentScaleUp(t *testing.T) {
	for _, source := range []string{"replicaset-event", "deployment-event", "replicaset-condition", "deployment-condition"} {
		for _, wait := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/wait=%v", source, wait), func(t *testing.T) {
				rollout := admissionRollouts()
				client := rollout.Client.(*fake.Clientset)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
				defer cancel()
				event, _ := client.CoreV1().Events("app").Get(ctx, "denied", metav1.GetOptions{})
				old := metav1.NewTime(time.Now().Add(-time.Hour))
				event.LastTimestamp = old
				event.FirstTimestamp = old
				d, _ := client.AppsV1().Deployments("app").Get(ctx, "frontend", metav1.GetOptions{})
				d.Status.ObservedGeneration = d.Generation
				rs, _ := client.AppsV1().ReplicaSets("app").Get(ctx, "frontend-next", metav1.GetOptions{})
				rs.Generation = 2
				rs.Status.ObservedGeneration = 2
				two := int32(2)
				rs.Spec.Replicas = &two
				rs.Status.Replicas = 1
				if source == "deployment-event" {
					event.InvolvedObject = corev1.ObjectReference{Kind: "Deployment", Namespace: "app", Name: d.Name, UID: d.UID}
				}
				if strings.HasSuffix(source, "condition") {
					event.Reason = "HistoricalDiagnostic"
				}
				if source == "replicaset-condition" {
					rs.Status.Conditions = []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: event.Message, LastTransitionTime: old}}
				}
				if source == "deployment-condition" {
					d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: event.Message, LastTransitionTime: old, LastUpdateTime: old}}
				}
				client.AppsV1().Deployments("app").Update(ctx, d, metav1.UpdateOptions{})
				client.AppsV1().ReplicaSets("app").Update(ctx, rs, metav1.UpdateOptions{})
				client.CoreV1().Events("app").Update(ctx, event, metav1.UpdateOptions{})
				var err error
				if wait {
					err = rollout.Wait(ctx, []sdk.ObjectRef{menuDeployment})
				} else {
					err = rollout.Observe(ctx, []sdk.ObjectRef{menuDeployment})
				}
				var blocked *AdmissionBlockedError
				if errors.As(err, &blocked) {
					t.Fatalf("retained %s aborted healthy admission scale-up: %v", source, err)
				}
				if err == nil {
					t.Fatal("pending rollout incorrectly complete")
				}
			})
		}
	}
}

func TestAdmissionFreshEvidenceRequiresObservedCurrentGeneration(t *testing.T) {
	for _, source := range []string{"replicaset-event", "deployment-event", "replicaset-condition", "deployment-condition"} {
		for _, observed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/current=%v", source, observed), func(t *testing.T) {
				rollout := admissionRollouts()
				client := rollout.Client.(*fake.Clientset)
				ctx := context.Background()
				event, _ := client.CoreV1().Events("app").Get(ctx, "denied", metav1.GetOptions{})
				d, _ := client.AppsV1().Deployments("app").Get(ctx, "frontend", metav1.GetOptions{})
				rs, _ := client.AppsV1().ReplicaSets("app").Get(ctx, "frontend-next", metav1.GetOptions{})
				rs.Generation = 2
				if observed {
					rs.Status.ObservedGeneration = 2
				} else {
					d.Status.ObservedGeneration = 1
				}
				fresh := metav1.NewTime(time.Now().Add(time.Minute))
				if source == "deployment-event" {
					event.InvolvedObject = corev1.ObjectReference{Kind: "Deployment", Namespace: "app", Name: d.Name, UID: d.UID}
				}
				if strings.HasSuffix(source, "condition") {
					event.Reason = "Diagnostic"
				}
				if source == "replicaset-condition" {
					rs.Status.Conditions = []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: event.Message, LastTransitionTime: fresh}}
				}
				if source == "deployment-condition" {
					d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: event.Message, LastTransitionTime: fresh, LastUpdateTime: fresh}}
				}
				client.AppsV1().Deployments("app").Update(ctx, d, metav1.UpdateOptions{})
				client.AppsV1().ReplicaSets("app").Update(ctx, rs, metav1.UpdateOptions{})
				client.CoreV1().Events("app").Update(ctx, event, metav1.UpdateOptions{})
				err := rollout.Observe(ctx, []sdk.ObjectRef{menuDeployment})
				var blocked *AdmissionBlockedError
				if errors.As(err, &blocked) != observed {
					t.Fatalf("current-generation admission=%v: %v", observed, err)
				}
			})
		}
	}
}
