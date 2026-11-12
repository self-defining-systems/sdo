package workloadhealth

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

var observed = time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)

func ago(d time.Duration) metav1.Time { return metav1.NewTime(observed.Add(-d)) }

func controllerRef(kind, name string) []metav1.OwnerReference {
	yes := true
	return []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kind, Name: name, Controller: &yes}}
}

func api(replicas int32, available int32, conditions ...appsv1.DeploymentCondition) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", CreationTimestamp: ago(72 * time.Hour)},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status:     appsv1.DeploymentStatus{AvailableReplicas: available, ReadyReplicas: available, UpdatedReplicas: replicas, Conditions: conditions},
	}
}

func apiReplicaSet() appsv1.ReplicaSet {
	return appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "app", OwnerReferences: controllerRef("Deployment", "api")}}
}

// crashing is an api Pod whose container crash-loops; ContainersReady turned
// False `since` before the observation.
func crashing(name string, since time.Duration) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", CreationTimestamp: ago(48 * time.Hour), OwnerReferences: controllerRef("ReplicaSet", "api-1")},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: ago(since)}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "api", RestartCount: 4,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			}},
		},
	}
}

func healthy(name string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", CreationTimestamp: ago(48 * time.Hour), OwnerReferences: controllerRef("ReplicaSet", "api-1")},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			Conditions:        []corev1.PodCondition{{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: ago(48 * time.Hour)}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "api", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(48 * time.Hour)}}}},
		},
	}
}

func detect(t *testing.T, hold *time.Duration, deployments []appsv1.Deployment, pods ...corev1.Pod) []sdk.Finding {
	t.Helper()
	at := observed
	snapshot := sdktest.Snapshot{
		NamespaceName: "app", ObservedAtTime: &at, SelfHealHoldTime: hold,
		DeploymentList: deployments, ReplicaSetList: []appsv1.ReplicaSet{apiReplicaSet()}, PodList: pods,
	}
	findings, err := NewDetector(sdk.DetectorSpec{ID: "workload-health"}).Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

func rules(findings []sdk.Finding) []string {
	var out []string
	for _, finding := range findings {
		out = append(out, finding.RuleID+" "+finding.PrimaryResource.Kind+"/"+finding.PrimaryResource.Name)
	}
	return out
}

// A restart that Kubernetes heals within its own back-off reports nothing.
func TestCrashThatHealsWithinTheHoldReportsNothing(t *testing.T) {
	found := detect(t, nil, []appsv1.Deployment{api(2, 1)}, crashing("api-a", 30*time.Second), healthy("api-b"))
	if len(found) != 0 {
		t.Fatalf("a 30 s old crash loop was reported: %v", rules(found))
	}
}

// A crash loop on one of two replicas that outlived the hold is one finding
// on the Deployment.
func TestCrashLoopHeldPastTheHoldIsReported(t *testing.T) {
	found := detect(t, nil, []appsv1.Deployment{api(2, 1)}, crashing("api-a", 5*time.Minute), healthy("api-b"))
	if got := rules(found); len(got) != 1 || got[0] != RuleCrashLooping+" Deployment/api" {
		t.Fatalf("findings = %v, want one crash-looping finding on Deployment/api", got)
	}
}

// A small configured hold (a benchmark's) is honoured.
func TestConfiguredSelfHealHoldIsHonoured(t *testing.T) {
	hold := 20 * time.Second
	if got := rules(detect(t, &hold, []appsv1.Deployment{api(2, 1)}, crashing("api-a", 30*time.Second))); len(got) != 1 {
		t.Fatalf("a crash loop held 30 s past a 20 s hold: findings = %v", got)
	}
	if got := rules(detect(t, &hold, []appsv1.Deployment{api(2, 1)}, crashing("api-a", 10*time.Second))); len(got) != 0 {
		t.Fatalf("a crash loop held 10 s under a 20 s hold: findings = %v", got)
	}
}

// Deployment unavailability is held from its Available=False transition, never
// from a Progressing timestamp that can be days old.
func TestOldProgressingTimestampDoesNotSkipTheHold(t *testing.T) {
	progressing := appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "NewReplicaSetAvailable", LastUpdateTime: ago(72 * time.Hour), LastTransitionTime: ago(72 * time.Hour)}
	fresh := appsv1.DeploymentCondition{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionFalse, Reason: "MinimumReplicasUnavailable", LastTransitionTime: ago(time.Minute)}
	if got := rules(detect(t, nil, []appsv1.Deployment{api(1, 0, progressing, fresh)})); len(got) != 0 {
		t.Fatalf("a one-minute outage with an old Progressing time was reported: %v", got)
	}
	held := fresh
	held.LastTransitionTime = ago(6 * time.Minute)
	if got := rules(detect(t, nil, []appsv1.Deployment{api(1, 0, progressing, held)})); len(got) != 1 || got[0] != RuleDeploymentUnavailable+" Deployment/api" {
		t.Fatalf("findings = %v, want deployment-unavailable after the hold", got)
	}
}

// ReplicaFailure is set on the first refused create, so it is held; the
// progress deadline is Kubernetes' give-up signal and is reported at once.
func TestReplicaFailureIsHeldAndProgressDeadlineIsNot(t *testing.T) {
	failure := appsv1.DeploymentCondition{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: "admission webhook denied", LastTransitionTime: ago(time.Minute)}
	if got := rules(detect(t, nil, []appsv1.Deployment{api(1, 0, failure)})); len(got) != 0 {
		t.Fatalf("a one-minute ReplicaFailure was reported: %v", got)
	}
	failure.LastTransitionTime = ago(5 * time.Minute)
	if got := rules(detect(t, nil, []appsv1.Deployment{api(1, 0, failure)})); len(got) != 1 || got[0] != RuleReplicaFailure+" Deployment/api" {
		t.Fatalf("findings = %v, want replica-failure after the hold", got)
	}
	deadline := appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded", LastTransitionTime: ago(time.Second)}
	if got := rules(detect(t, nil, []appsv1.Deployment{api(1, 0, deadline)})); len(got) != 1 || got[0] != RuleProgressDeadlineExceeded+" Deployment/api" {
		t.Fatalf("findings = %v, want progress-deadline-exceeded at once", got)
	}
}
