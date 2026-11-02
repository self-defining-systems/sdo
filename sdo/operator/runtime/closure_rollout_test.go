package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Healthy old Pods cannot prove that a corrected Deployment template is live.
// Ownership is by UID, never a shared label or a resource-name prefix.
func TestClosureNamesRolloutBlockersWithoutProgramApplication(t *testing.T) {
	now := time.Now().UTC()
	one := int32(1)
	controller := true
	owner := func(kind, uid string) []metav1.OwnerReference {
		return []metav1.OwnerReference{{Kind: kind, UID: types.UID(uid), Controller: &controller}}
	}
	d := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "demo", UID: "d", Generation: 2}, Spec: appsv1.DeploymentSpec{Replicas: &one, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "desired"}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, UpdatedReplicas: 0, AvailableReplicas: 1, ReadyReplicas: 1}}
	old := appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "api-old", Namespace: "demo", UID: "old", OwnerReferences: owner("Deployment", "d")}, Spec: appsv1.ReplicaSetSpec{Replicas: &one, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "obsolete"}}}}}}
	next := *old.DeepCopy()
	next.Name, next.UID = "api-next", "next"
	next.Spec.Template = d.Spec.Template
	next.Status.Conditions = []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: `admission webhook "policy.example" denied Pod creation`}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-old-running", Namespace: "demo", UID: "pod-old", OwnerReferences: owner("ReplicaSet", "old")}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	blocked := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-next-pending", Namespace: "demo", UID: "pod-next", OwnerReferences: owner("ReplicaSet", "next")}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "api", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "registry unavailable"}}}}}}
	event := corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "pull", Namespace: "demo"}, Type: corev1.EventTypeWarning, InvolvedObject: corev1.ObjectReference{UID: "pod-next", Kind: "Pod", Namespace: "demo", Name: blocked.Name}, Reason: "Failed", Message: "image pull denied"}
	foreign := event
	foreign.InvolvedObject.UID = "unrelated"
	foreign.Message = "foreign evidence"
	snapshot := sdktest.Snapshot{NamespaceName: "demo", DeploymentList: []appsv1.Deployment{d}, ReplicaSetList: []appsv1.ReplicaSet{old, next}, PodList: []corev1.Pod{pod, blocked}, EventList: []corev1.Event{event, foreign}}
	c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: snapshot}, &verificationRetryDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "synthetic", Namespace: "demo", Findings: []sdk.Finding{{PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "demo", Name: "api"}}}}
	c.incidentOpen = true
	c.responderDone = true
	c.responderCompletedAt = now.Add(-time.Second)
	c.healthDetectorIDs = []string{"health"}
	c.history = []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationClear, EvaluatedAt: now}}
	response := c.responderVerification(context.Background(), "synthetic")
	if response.Healthy || len(response.Findings) != 1 {
		t.Fatalf("old readiness passed closure or missing named blocker: %+v", response)
	}
	f := response.Findings[0]
	if !strings.Contains(f.Summary, "policy.example") {
		t.Fatalf("compact finding omitted named admission blocker: %s", f.Summary)
	}
	for _, want := range []string{"api-next", "FailedCreate", "policy.example", "api-old-running", "old template", "ImagePullBackOff", "registry unavailable", "image pull denied"} {
		if !strings.Contains(f.Evidence, want) {
			t.Errorf("missing %q in %s", want, f.Evidence)
		}
	}
	if strings.Contains(f.Evidence, "foreign evidence") {
		t.Fatal("unowned event attached to rollout")
	}
	payload, err := continuationFeedback(1, response)
	if err != nil || !strings.Contains(string(payload), "policy.example") {
		t.Fatalf("continuation lost blocker: %s %v", payload, err)
	}
	clear, findings := closureRollouts(c, now, snapshot)
	if clear || len(findings) != 1 {
		t.Fatalf("closure and self-check disagree: %v %+v", clear, findings)
	}
	d.Status.UpdatedReplicas = 1
	snapshot.DeploymentList = []appsv1.Deployment{d}
	clear, findings = closureRollouts(c, now, snapshot)
	if clear || len(findings) != 1 {
		t.Fatal("old live Pod escaped newer deployment counters")
	}
	snapshot.PodList = []corev1.Pod{blocked}
	clear, findings = closureRollouts(c, now, snapshot)
	if clear || len(findings) != 1 {
		t.Fatal("live image pull failure escaped newer deployment counters")
	}
	snapshot.PodList[0].Status = corev1.PodStatus{Phase: corev1.PodRunning}
	clear, findings = closureRollouts(c, now, snapshot)
	if !clear || len(findings) != 0 {
		t.Fatalf("stale rollout evidence survived completed rollout: %+v", findings)
	}
}

func TestClosureRolloutIgnoresOtherNamespacesAndHelpers(t *testing.T) {
	d := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "elsewhere"}}
	helper := *d.DeepCopy()
	helper.Namespace = "demo"
	helper.Labels = map[string]string{"sdo.dev/responder-helper": "true"}
	unrelated := *d.DeepCopy()
	unrelated.Namespace = "demo"
	unrelated.Name = "untouched"
	snapshot := sdktest.Snapshot{DeploymentList: []appsv1.Deployment{d, helper, unrelated}}
	c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: snapshot}, &verificationRetryDispatcher{}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "synthetic", Namespace: "demo", Findings: []sdk.Finding{{PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "demo", Name: "api"}}}}
	clear, findings := closureRollouts(c, time.Now(), snapshot)
	if !clear || len(findings) != 0 {
		t.Fatalf("unrelated rollout prevented closure: %+v", findings)
	}
}

func TestClosureRolloutBlockerClasses(t *testing.T) {
	one := int32(1)
	for _, scenario := range []string{"quota", "scheduling", "init-image", "old-pod", "foreign-owner", "terminating-old", "template-hash", "service-selection", "pod-write", "replicaset-write", "unmatched-service"} {
		t.Run(scenario, func(t *testing.T) {
			yes, no := true, false
			d := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "demo", UID: "deployment", Generation: 2}, Spec: appsv1.DeploymentSpec{Replicas: &one, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "desired"}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1, ReadyReplicas: 1}}
			rs := appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "api-revision", Namespace: "demo", UID: "replicaset", OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", UID: d.UID, Controller: &yes}}}, Spec: appsv1.ReplicaSetSpec{Replicas: &one, Template: *d.Spec.Template.DeepCopy()}}
			pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-instance", Namespace: "demo", UID: "pod", OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", UID: rs.UID, Controller: &yes}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
			want := ""
			refs := []sdk.ObjectRef{{Kind: "Deployment", Namespace: "demo", Name: "api"}}
			pending := true
			switch scenario {
			case "quota":
				d.Status.UpdatedReplicas = 0
				rs.Status.Conditions = []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: "exceeded quota: compute-limit"}}
				want = "compute-limit"
			case "scheduling":
				d.Status.AvailableReplicas = 0
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", Message: "insufficient cpu"}}
				want = "insufficient cpu"
			case "init-image":
				d.Status.AvailableReplicas = 0
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "init", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull", Message: "init image unavailable"}}}}
				want = "init image unavailable"
			case "old-pod", "foreign-owner", "terminating-old":
				rs.Spec.Template.Spec.Containers[0].Image = "obsolete"
				want = "old template"
				if scenario == "foreign-owner" {
					rs.OwnerReferences[0].Controller = &no
					pending = false
				}
				if scenario == "terminating-old" {
					stamp := metav1.Now()
					pod.DeletionTimestamp = &stamp
					pending = false
				}
			case "service-selection", "pod-write", "replicaset-write", "unmatched-service":
				d.Status.AvailableReplicas = 0
				want = "0 available"
				switch scenario {
				case "service-selection", "unmatched-service":
					refs = []sdk.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "route"}}
					if scenario == "unmatched-service" {
						pending = false
					}
				case "pod-write":
					refs = []sdk.ObjectRef{{Kind: "Pod", Namespace: "demo", Name: pod.Name}}
				case "replicaset-write":
					refs = []sdk.ObjectRef{{Kind: "ReplicaSet", Namespace: "demo", Name: rs.Name}}
				}
			case "template-hash":
				rs.Spec.Template.Labels[appsv1.DefaultDeploymentUniqueLabelKey] = "revision-hash"
				pending = false
			}
			snapshot := sdktest.Snapshot{DeploymentList: []appsv1.Deployment{d}, ReplicaSetList: []appsv1.ReplicaSet{rs}, PodList: []corev1.Pod{pod}}
			snapshot.ServiceList = []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "route", Namespace: "demo"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}}}}
			if scenario == "unmatched-service" {
				snapshot.ServiceList[0].Spec.Selector["app"] = "peer"
			}
			findings := closureRolloutFindings(snapshot, "demo", refs)
			if (len(findings) > 0) != pending {
				t.Fatalf("pending=%v, findings=%+v", pending, findings)
			}
			if pending && !strings.Contains(findings[0].Evidence, want) {
				t.Fatalf("missing named cause %q: %+v", want, findings)
			}
		})
	}
}

// A deterministic closure schedule: a source repair and clear detector sample
// precede the deployment-controller rollout, then a later sample settles it.
func TestClosureRolloutWaitsBeforePublishingSuccess(t *testing.T) {
	f := newClosureDriftFixture(t, true, true, "app")
	f.restoreSource(t)
	d, err := f.client.AppsV1().Deployments("app").Get(context.Background(), "frontend", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d.Status.UpdatedReplicas = 0
	if _, err = f.client.AppsV1().Deployments("app").Update(context.Background(), d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.step(t, 1)
	if len(f.closed) != 0 {
		t.Fatal("clear health published success before desired rollout")
	}
	d.Status.UpdatedReplicas = 1
	if _, err = f.client.AppsV1().Deployments("app").Update(context.Background(), d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.step(t, 2)
	if len(f.closed) != 0 {
		t.Fatal("success published before the closure stability window held")
	}
	f.step(t, 2+int(f.c.config.ClosureStabilityWindow/time.Second))
	if len(f.closed) != 1 || closureOutcome(t, f.closed[0]) != "success" {
		t.Fatalf("settled rollout did not release closure: %+v", f.closed)
	}
}

func TestClosureRolloutRecordedWriteReachesCachedVerification(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	one := int32(1)
	d := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "changed", Namespace: "demo", Generation: 2}, Spec: appsv1.DeploymentSpec{Replicas: &one}, Status: appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, AvailableReplicas: 1, ReadyReplicas: 1}}
	c.provider = staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo", DeploymentList: []appsv1.Deployment{d}}}
	loop := NewResponderLoop(*c.currentIncidentRequest, nil, c.provider, nil, PlanExecutor{}, 3)
	c.responderLoop = loop
	receipt := RepairActionReceipt{ActionID: "recorded-write", Resources: []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "changed"}}, Success: true}
	loop.ExternalReceipts = func() ([]RepairActionReceipt, bool, string, error) {
		return []RepairActionReceipt{receipt}, false, "revision", nil
	}
	// The opening finding concerns another resource; only the recorded write
	// links this workload to the incident before the model's final report.
	step(2)
	response := c.cachedResponderVerification(context.Background(), c.currentIncidentRequest.IncidentID, 0)
	found := false
	for _, f := range response.Findings {
		found = found || f.DetectorID == "controller.rollout" && f.PrimaryResource.Name == "changed"
	}
	if !found || response.Healthy {
		t.Fatalf("recorded write blocker absent from cached tool response: %+v", response)
	}
}

// closureRollouts reports whether pending rollouts hold the closure.
func closureRollouts(c *Controller, now time.Time, snapshot sdk.DetectionContext) (bool, []sdk.Finding) {
	report := c.verifyClosureSource(context.Background(), now, snapshot, nil)
	return len(report.Rollouts) == 0, report.Rollouts
}
