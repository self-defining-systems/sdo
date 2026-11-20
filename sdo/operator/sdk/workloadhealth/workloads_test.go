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

// slowStarting is an api Pod whose container started `age` ago, is not ready
// and declares a startup probe allowing 300 s.
func slowStarting(name string, age time.Duration) corev1.Pod {
	pod := healthy(name)
	pod.Spec.Containers = []corev1.Container{{Name: "api", StartupProbe: &corev1.Probe{FailureThreshold: 30, PeriodSeconds: 10}}}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: ago(age)}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "api", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(age)}}}}
	return pod
}

// A pod still inside the startup allowance it declares is starting, not
// outliving Kubernetes' self-healing, even past the self-heal hold.
func TestUnreadyPodInsideItsStartupAllowanceReportsNothing(t *testing.T) {
	hold := 60 * time.Second
	if got := rules(detect(t, &hold, []appsv1.Deployment{api(1, 0)}, slowStarting("api-a", 90*time.Second))); len(got) != 0 {
		t.Fatalf("a pod 90 s into a 300 s startup allowance was reported: %v", got)
	}
}

// A real outage still opens: never ready past the allowance.
func TestUnreadyPodPastItsStartupAllowanceIsReported(t *testing.T) {
	hold := 60 * time.Second
	got := rules(detect(t, &hold, []appsv1.Deployment{api(1, 0)}, slowStarting("api-a", 400*time.Second)))
	if len(got) != 1 || got[0] != RuleNotReady+" Deployment/api" {
		t.Fatalf("findings = %v, want one not-ready finding", got)
	}
}

// bootingNoProbe is an api Pod that declares no probe and has been running and
// unready for `age`, as after a node drain recreated it.
func bootingNoProbe(name string, age time.Duration) corev1.Pod {
	pod := slowStarting(name, age)
	pod.Spec.Containers = []corev1.Container{{Name: "api"}}
	return pod
}

func detectObserved(t *testing.T, hold time.Duration, observedStartup time.Duration, deployments []appsv1.Deployment, pods ...corev1.Pod) []sdk.Finding {
	t.Helper()
	at := observed
	snapshot := sdktest.Snapshot{
		NamespaceName: "app", ObservedAtTime: &at, SelfHealHoldTime: &hold,
		ObservedStartupTimes: map[string]time.Duration{"Deployment/api": observedStartup},
		DeploymentList:       deployments, ReplicaSetList: []appsv1.ReplicaSet{apiReplicaSet()}, PodList: pods,
	}
	findings, err := NewDetector(sdk.DetectorSpec{ID: "workload-health"}).Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

// A workload with no probe that was seen taking about two minutes to become
// ready is starting, not failing, a minute and a half after its pods were
// recreated, although the self-heal hold passed.
func TestSlowBootingWorkloadWithoutProbeOpensNothingWithinItsObservedStartup(t *testing.T) {
	got := detectObserved(t, 30*time.Second, 135*time.Second, []appsv1.Deployment{api(1, 0)}, bootingNoProbe("api-a", 90*time.Second))
	if len(got) != 0 {
		t.Fatalf("a pod 90 s into an observed 135 s startup was reported: %v", rules(got))
	}
	if got := detect(t, ptr(30*time.Second), []appsv1.Deployment{api(1, 0)}, bootingNoProbe("api-a", 90*time.Second)); len(got) != 1 {
		t.Fatalf("without an observation the pod must be reported after the hold, got %v", rules(got))
	}
}

// The same workload stuck past its observed startup opens.
func TestWorkloadStuckPastItsObservedStartupIsReported(t *testing.T) {
	got := rules(detectObserved(t, 30*time.Second, 135*time.Second, []appsv1.Deployment{api(1, 0)}, bootingNoProbe("api-a", 200*time.Second)))
	if len(got) != 1 || got[0] != RuleNotReady+" Deployment/api" {
		t.Fatalf("findings = %v, want one not-ready finding", got)
	}
}

// A give-up signal is never masked by the observed startup.
func TestCrashLoopIsReportedDespiteAnObservedStartup(t *testing.T) {
	got := rules(detectObserved(t, 30*time.Second, 15*time.Minute, []appsv1.Deployment{api(1, 0)}, crashing("api-a", time.Minute)))
	if len(got) != 1 || got[0] != RuleCrashLooping+" Deployment/api" {
		t.Fatalf("findings = %v, want crash-looping at once", got)
	}
}

func ptr[T any](v T) *T { return &v }
