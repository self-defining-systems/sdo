package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

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
