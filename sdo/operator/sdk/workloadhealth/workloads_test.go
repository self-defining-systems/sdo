package workloadhealth_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/workloadhealth"
)

const namespace = "shop"

var observed = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

func ago(duration time.Duration) metav1.Time { return metav1.NewTime(observed.Add(-duration)) }

func spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "workload-health", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches:     workloadhealth.Watches(),
		Interval:    15 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:   []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap",
	}
}

func deploymentRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: namespace, Name: name}
}

func podRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: namespace, Name: name}
}

func controller(apiVersion, kind, name string) []metav1.OwnerReference {
	yes := true
	return []metav1.OwnerReference{{APIVersion: apiVersion, Kind: kind, Name: name, Controller: &yes}}
}

func deployment(name string, replicas, available int32) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, CreationTimestamp: ago(2 * time.Hour)},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status: appsv1.DeploymentStatus{
			Replicas: replicas, ReadyReplicas: available, AvailableReplicas: available, UnavailableReplicas: replicas - available,
			Conditions: []appsv1.DeploymentCondition{{
				Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "NewReplicaSetAvailable",
				LastUpdateTime: ago(time.Hour), LastTransitionTime: ago(time.Hour),
			}},
		},
	}
}

func replicaSet(name, owner string) appsv1.ReplicaSet {
	return appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: namespace, OwnerReferences: controller("apps/v1", "Deployment", owner),
	}}
}

// runningPod is a ready pod whose single container has run for an hour.
func runningPod(name, replicaSet string) corev1.Pod {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, CreationTimestamp: ago(time.Hour)},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(time.Hour)}},
			}},
		},
	}
	if replicaSet != "" {
		pod.OwnerReferences = controller("apps/v1", "ReplicaSet", replicaSet)
	}
	return pod
}

func warning(name, kind, object, reason, message string) corev1.Event {
	return corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: name, Namespace: namespace},
		InvolvedObject: corev1.ObjectReference{Kind: kind, Namespace: namespace, Name: object},
		Type:           corev1.EventTypeWarning, Reason: reason, Message: message, Count: 4,
		FirstTimestamp: ago(5 * time.Minute), LastTimestamp: ago(30 * time.Second),
	}
}

func healthy() sdktest.Snapshot {
	clock := observed
	return sdktest.Snapshot{
		NamespaceName:  namespace,
		ObservedAtTime: &clock,
		ServiceList: []corev1.Service{{
			ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: namespace},
			Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "frontend"}},
		}},
		DeploymentList: []appsv1.Deployment{deployment("frontend", 1, 1)},
		ReplicaSetList: []appsv1.ReplicaSet{replicaSet("frontend-7d9", "frontend")},
		PodList:        []corev1.Pod{runningPod("frontend-7d9-abc", "frontend-7d9")},
	}
}

// waiting makes the frontend pod's container wait with reason; the pod is
// unready and its Deployment has no available replica.
func waiting(reason, message string, restarts int32) func(*sdktest.Snapshot) {
	return func(s *sdktest.Snapshot) {
		unavailable(s)
		status := &s.PodList[0].Status.ContainerStatuses[0]
		status.Ready = false
		status.RestartCount = restarts
		status.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: message}}
	}
}

func unavailable(s *sdktest.Snapshot) {
	s.DeploymentList[0].Status.AvailableReplicas = 0
	s.DeploymentList[0].Status.ReadyReplicas = 0
	s.PodList[0].Status.Conditions[0].Status = corev1.ConditionFalse
}

func lastTerminated(reason string, exitCode int32, finished time.Duration) func(*sdktest.Snapshot) {
	return func(s *sdktest.Snapshot) {
		s.PodList[0].Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason: reason, ExitCode: exitCode, Message: "last words", StartedAt: ago(finished + 5*time.Second), FinishedAt: ago(finished),
		}}
	}
}

func both(mutations ...func(*sdktest.Snapshot)) func(*sdktest.Snapshot) {
	return func(s *sdktest.Snapshot) {
		for _, mutate := range mutations {
			mutate(s)
		}
	}
}

func pending(created time.Duration) func(*sdktest.Snapshot) {
	return func(s *sdktest.Snapshot) {
		unavailable(s)
		pod := &s.PodList[0]
		pod.CreationTimestamp = ago(created)
		pod.Status.Phase = corev1.PodPending
		pod.Status.ContainerStatuses[0].Ready = false
		pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}
		s.DeploymentList[0].Status.Conditions[0].LastUpdateTime = ago(created)
	}
}

func unschedulable(s *sdktest.Snapshot) {
	pending(5 * time.Minute)(s)
	pod := &s.PodList[0]
	pod.Status.ContainerStatuses = nil
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
		Message: "0/3 nodes are available: 3 Insufficient memory.", LastTransitionTime: ago(5 * time.Minute),
	})
}

func notReady(started time.Duration) func(*sdktest.Snapshot) {
	return func(s *sdktest.Snapshot) {
		unavailable(s)
		status := &s.PodList[0].Status.ContainerStatuses[0]
		status.Ready = false
		status.State.Running.StartedAt = ago(started)
		s.DeploymentList[0].Status.Conditions[0].LastUpdateTime = ago(started)
	}
}

func initContainer(state corev1.ContainerState) func(*sdktest.Snapshot) {
	return func(s *sdktest.Snapshot) {
		pending(10 * time.Minute)(s)
		pod := &s.PodList[0]
		pod.Spec.InitContainers = []corev1.Container{{Name: "wait-for-db"}}
		pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "wait-for-db", State: state}}
		pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}
	}
}

func churn(restarts int32, started, finished time.Duration, ready bool) func(*sdktest.Snapshot) {
	return func(s *sdktest.Snapshot) {
		status := &s.PodList[0].Status.ContainerStatuses[0]
		status.RestartCount = restarts
		status.Ready = ready
		status.State.Running.StartedAt = ago(started)
		lastTerminated("Error", 1, finished)(s)
		if !ready {
			unavailable(s)
		}
	}
}

func jobPod(name string, phase corev1.PodPhase) corev1.Pod {
	pod := runningPod(name, "")
	pod.OwnerReferences = controller("batch/v1", "Job", "migrate")
	pod.Status.Phase = phase
	pod.Status.ContainerStatuses[0].Ready = false
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"}}
	if phase == corev1.PodFailed {
		pod.Status.ContainerStatuses[0].State.Terminated = &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1}
	}
	return pod
}

func detect(t *testing.T, snapshot sdktest.Snapshot) []sdk.Finding {
	t.Helper()
	findings, err := workloadhealth.NewDetector(spec()).Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	return findings
}

func TestWorkloadRules(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*sdktest.Snapshot)
		rule     string
		primary  sdk.ObjectRef
		evidence []string
	}{
		{name: "healthy workload with a Service", mutate: func(*sdktest.Snapshot) {}},
		{
			name: "pod failed", rule: "workload-pod-failed", primary: podRef("report"),
			mutate: func(s *sdktest.Snapshot) {
				pod := runningPod("report", "")
				pod.Status.Phase, pod.Status.Reason, pod.Status.Message = corev1.PodFailed, "Error", "pod exited"
				pod.Status.ContainerStatuses[0].Ready = false
				pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					Reason: "Error", ExitCode: 2, Message: "bad flag", FinishedAt: ago(time.Minute),
				}}
				s.PodList = append(s.PodList, pod)
			},
			evidence: []string{"report", "phase Failed", "container app", "exit code 2", "bad flag"},
		},
		{
			name: "pod unknown", rule: "workload-pod-unknown", primary: deploymentRef("frontend"),
			mutate: func(s *sdktest.Snapshot) {
				unavailable(s)
				s.PodList[0].Status.Phase, s.PodList[0].Status.Reason = corev1.PodUnknown, "NodeLost"
			},
			evidence: []string{"frontend-7d9-abc", "phase Unknown", "NodeLost"},
		},
		{
			name: "pod pending past grace", rule: "workload-pod-pending", primary: deploymentRef("frontend"),
			mutate: both(pending(5*time.Minute), func(s *sdktest.Snapshot) {
				s.EventList = []corev1.Event{warning("mount", "Pod", "frontend-7d9-abc", "FailedMount", `configmap "settings" not found`)}
			}),
			evidence: []string{"frontend-7d9-abc", "Pending", "ContainerCreating", "FailedMount", `configmap "settings" not found`},
		},
		{name: "pod pending within grace", mutate: pending(10 * time.Second)},
		{
			name: "pod unschedulable", rule: "workload-pod-unschedulable", primary: deploymentRef("frontend"),
			mutate:   unschedulable,
			evidence: []string{"frontend-7d9-abc", "Unschedulable", "0/3 nodes are available: 3 Insufficient memory."},
		},
		{
			name: "crash loop", rule: "workload-container-crash-looping", primary: deploymentRef("frontend"),
			mutate: both(waiting("CrashLoopBackOff", "back-off 40s restarting failed container", 2), lastTerminated("Error", 1, 20*time.Second)),
			evidence: []string{"frontend-7d9-abc", "container app", "CrashLoopBackOff", "back-off 40s", "restarts 2",
				"exit code 1", "last words", observed.Add(-20 * time.Second).Format(time.RFC3339)},
		},
		{
			name: "image pull backoff", rule: "workload-container-image-pull-failing", primary: deploymentRef("frontend"),
			mutate:   waiting("ImagePullBackOff", `Back-off pulling image "shop/frontend:typo"`, 0),
			evidence: []string{"ImagePullBackOff", "shop/frontend:typo"},
		},
		{
			name: "image pull error", rule: "workload-container-image-pull-failing", primary: deploymentRef("frontend"),
			mutate: waiting("ErrImagePull", "manifest unknown", 0), evidence: []string{"ErrImagePull", "manifest unknown"},
		},
		{
			name: "config error", rule: "workload-container-config-error", primary: deploymentRef("frontend"),
			mutate:   waiting("CreateContainerConfigError", `secret "db" not found`, 0),
			evidence: []string{"CreateContainerConfigError", `secret "db" not found`},
		},
		{
			name: "invalid image name", rule: "workload-container-config-error", primary: deploymentRef("frontend"),
			mutate: waiting("InvalidImageName", "couldn't parse image name", 0), evidence: []string{"InvalidImageName"},
		},
		{
			name: "other waiting reason", rule: "workload-container-waiting", primary: deploymentRef("frontend"),
			mutate: waiting("RunContainerError", "exec format error", 0), evidence: []string{"RunContainerError", "exec format error"},
		},
		{
			name: "oom killed subsumes crash loop and churn", rule: "workload-container-oom-killed", primary: deploymentRef("frontend"),
			mutate:   both(waiting("CrashLoopBackOff", "back-off", 5), lastTerminated("OOMKilled", 137, time.Minute)),
			evidence: []string{"OOMKilled", "exit code 137", "restarts 5"},
		},
		{
			name: "old oom kill on a stable container", mutate: both(lastTerminated("OOMKilled", 137, time.Hour), func(s *sdktest.Snapshot) {
				s.PodList[0].Status.ContainerStatuses[0].RestartCount = 1
			}),
		},
		{
			name: "terminated with error", rule: "workload-container-terminated-error", primary: deploymentRef("frontend"),
			mutate: func(s *sdktest.Snapshot) {
				unavailable(s)
				status := &s.PodList[0].Status.ContainerStatuses[0]
				status.Ready, status.RestartCount = false, 1
				status.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					Reason: "Error", ExitCode: 3, Message: "panic: nil map", FinishedAt: ago(5 * time.Second),
				}}
			},
			evidence: []string{"Error", "exit code 3", "panic: nil map"},
		},
		{
			name: "not ready past grace", rule: "workload-container-not-ready", primary: deploymentRef("frontend"),
			mutate: both(notReady(5*time.Minute), func(s *sdktest.Snapshot) {
				s.EventList = []corev1.Event{warning("probe", "Pod", "frontend-7d9-abc", "Unhealthy", "Readiness probe failed: HTTP probe failed with statuscode: 503")}
			}),
			evidence: []string{"container app", "not ready", "Unhealthy", "statuscode: 503"},
		},
		{name: "not ready within grace", mutate: notReady(10 * time.Second)},
		{
			name: "init container stuck", rule: "workload-init-container-stuck", primary: deploymentRef("frontend"),
			mutate:   initContainer(corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(10 * time.Minute)}}),
			evidence: []string{"init container wait-for-db", "running since"},
		},
		{
			name: "init container crash loop", rule: "workload-container-crash-looping", primary: deploymentRef("frontend"),
			mutate:   initContainer(corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off"}}),
			evidence: []string{"init container wait-for-db", "CrashLoopBackOff"},
		},
		{
			name: "restart churn while running", rule: "workload-container-restart-churn", primary: deploymentRef("frontend"),
			mutate:   churn(4, 20*time.Second, 25*time.Second, true),
			evidence: []string{"restarts 4", "exit code 1", observed.Add(-25 * time.Second).Format(time.RFC3339)},
		},
		{name: "restarts long ago", mutate: churn(10, time.Hour, time.Hour+time.Minute, true)},
		{name: "recent restarts then stable and ready", mutate: churn(4, 3*time.Minute, 3*time.Minute+5*time.Second, true)},
		{
			name: "deployment unavailable without pods", rule: "workload-deployment-unavailable", primary: deploymentRef("frontend"),
			mutate: func(s *sdktest.Snapshot) {
				unavailable(s)
				s.PodList = nil
				condition := &s.DeploymentList[0].Status.Conditions[0]
				condition.Reason, condition.LastUpdateTime = "ReplicaSetUpdated", ago(5*time.Minute)
				s.DeploymentList[0].Status.Conditions = append(s.DeploymentList[0].Status.Conditions, appsv1.DeploymentCondition{
					Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate",
					Message: `pods "frontend-7d9-x" is forbidden: exceeded quota`,
				})
				s.EventList = []corev1.Event{warning("create", "ReplicaSet", "frontend-7d9", "FailedCreate", "exceeded quota: compute")}
			},
			evidence: []string{"0/1", "exceeded quota", "FailedCreate", "exceeded quota: compute"},
		},
		{
			name: "deployment unavailable within grace", mutate: func(s *sdktest.Snapshot) {
				unavailable(s)
				s.PodList = nil
				s.DeploymentList[0].Status.Conditions[0].LastUpdateTime = ago(10 * time.Second)
			},
		},
		{
			name: "progress deadline exceeded", rule: "workload-deployment-progress-deadline-exceeded", primary: deploymentRef("frontend"),
			mutate: func(s *sdktest.Snapshot) {
				s.PodList = nil
				condition := &s.DeploymentList[0].Status.Conditions[0]
				condition.Status, condition.Reason = corev1.ConditionFalse, "ProgressDeadlineExceeded"
				condition.Message, condition.LastUpdateTime = `ReplicaSet "frontend-8f1" has timed out progressing.`, ago(time.Minute)
			},
			evidence: []string{"ProgressDeadlineExceeded", `ReplicaSet "frontend-8f1" has timed out progressing.`},
		},
		{
			name: "deployment scaled to zero", mutate: both(waiting("CrashLoopBackOff", "back-off", 1), func(s *sdktest.Snapshot) {
				zero := int32(0)
				s.DeploymentList[0].Spec.Replicas = &zero
				s.DeploymentList[0].Status.Replicas = 0
			}),
		},
		{
			name: "pod being deleted", mutate: both(waiting("CrashLoopBackOff", "back-off", 1), func(s *sdktest.Snapshot) {
				deleted := ago(time.Second)
				s.PodList[0].DeletionTimestamp = &deleted
				s.DeploymentList[0].Status.AvailableReplicas = 1
			}),
		},
		{
			name: "completed job pods", mutate: func(s *sdktest.Snapshot) {
				s.PodList = append(s.PodList, jobPod("migrate-1", corev1.PodFailed), jobPod("migrate-2", corev1.PodSucceeded))
			},
		},
		{
			name: "evicted pod of a fully available deployment", mutate: func(s *sdktest.Snapshot) {
				evicted := runningPod("frontend-7d9-old", "frontend-7d9")
				evicted.Status.Phase, evicted.Status.Reason = corev1.PodFailed, "Evicted"
				evicted.Status.ContainerStatuses = nil
				s.PodList = append(s.PodList, evicted)
			},
		},
		{
			name: "pod outside the namespace", mutate: func(s *sdktest.Snapshot) {
				pod := runningPod("intruder", "")
				pod.Namespace = "elsewhere"
				pod.Status.Phase = corev1.PodFailed
				s.PodList = append(s.PodList, pod)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := healthy()
			tc.mutate(&snapshot)
			findings := detect(t, snapshot)
			if tc.rule == "" {
				if len(findings) != 0 {
					t.Fatalf("expected no finding, got %+v", findings)
				}
				return
			}
			if len(findings) != 1 {
				t.Fatalf("expected one %s finding, got %+v", tc.rule, findings)
			}
			finding := findings[0]
			if finding.RuleID != tc.rule || finding.PrimaryResource != tc.primary {
				t.Fatalf("expected %s on %+v, got %s on %+v", tc.rule, tc.primary, finding.RuleID, finding.PrimaryResource)
			}
			if finding.DetectorID != "workload-health" || finding.Status != sdk.FindingActive ||
				finding.Severity != sdk.SeverityCritical || finding.Summary == "" ||
				!reflect.DeepEqual(finding.Playbooks, []string{"health-objective"}) {
				t.Fatalf("finding shape: %+v", finding)
			}
			for _, want := range tc.evidence {
				if !strings.Contains(finding.Evidence, want) {
					t.Fatalf("evidence %q must contain %q", finding.Evidence, want)
				}
			}
			if tc.primary.Kind == "Deployment" && tc.rule != "workload-deployment-progress-deadline-exceeded" &&
				tc.rule != "workload-deployment-unavailable" {
				if !reflect.DeepEqual(finding.RelatedResources, []sdk.ObjectRef{podRef("frontend-7d9-abc")}) {
					t.Fatalf("finding must relate the offending pod, got %v", finding.RelatedResources)
				}
			}
		})
	}
}

func TestCrashLoopAndRestartChurnAreBothReported(t *testing.T) {
	snapshot := healthy()
	both(waiting("CrashLoopBackOff", "back-off", 4), lastTerminated("Error", 1, 30*time.Second))(&snapshot)
	findings := detect(t, snapshot)
	var rules []string
	for _, finding := range findings {
		rules = append(rules, finding.RuleID)
	}
	// Churn is derived from the last termination, so it stays active through
	// the whole back-off cycle while the waiting reason comes and goes.
	if !reflect.DeepEqual(rules, []string{"workload-container-crash-looping", "workload-container-restart-churn"}) {
		t.Fatalf("expected crash loop and churn, got %v", rules)
	}
}

func TestOneFindingPerRuleAndWorkloadNamesEveryPod(t *testing.T) {
	snapshot := healthy()
	waiting("CrashLoopBackOff", "back-off", 1)(&snapshot)
	second := snapshot.PodList[0]
	second.Name = "frontend-7d9-aaa"
	snapshot.PodList = append(snapshot.PodList, second)
	findings := detect(t, snapshot)
	if len(findings) != 1 {
		t.Fatalf("pods of one Deployment share a finding, got %+v", findings)
	}
	want := []sdk.ObjectRef{podRef("frontend-7d9-aaa"), podRef("frontend-7d9-abc")}
	if !reflect.DeepEqual(findings[0].RelatedResources, want) {
		t.Fatalf("related pods %v, want %v", findings[0].RelatedResources, want)
	}
	if strings.Index(findings[0].Evidence, "frontend-7d9-aaa") > strings.Index(findings[0].Evidence, "frontend-7d9-abc") {
		t.Fatalf("evidence must list pods in name order: %s", findings[0].Evidence)
	}
}

func TestPodWithoutResolvableDeploymentIsItsOwnPrimary(t *testing.T) {
	snapshot := healthy()
	waiting("ImagePullBackOff", "back-off", 0)(&snapshot)
	snapshot.ReplicaSetList = nil
	snapshot.DeploymentList = nil
	findings := detect(t, snapshot)
	if len(findings) != 1 || findings[0].PrimaryResource != podRef("frontend-7d9-abc") || len(findings[0].RelatedResources) != 0 {
		t.Fatalf("an unresolvable owner must leave the pod primary, got %+v", findings)
	}
}

func TestFindingsAreDeterministicAndSorted(t *testing.T) {
	snapshot := healthy()
	waiting("CrashLoopBackOff", "back-off", 1)(&snapshot)
	api := deployment("api", 1, 0)
	api.Status.Conditions[0].LastUpdateTime = ago(10 * time.Minute)
	apiPod := runningPod("api-5c4-xyz", "api-5c4")
	apiPod.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull"}}
	orphan := runningPod("adhoc", "")
	orphan.Status.Phase = corev1.PodUnknown
	snapshot.DeploymentList = append(snapshot.DeploymentList, api)
	snapshot.ReplicaSetList = append([]appsv1.ReplicaSet{replicaSet("api-5c4", "api")}, snapshot.ReplicaSetList...)
	snapshot.PodList = append([]corev1.Pod{orphan}, append(snapshot.PodList, apiPod)...)
	first := detect(t, snapshot)
	snapshot.PodList[0], snapshot.PodList[2] = snapshot.PodList[2], snapshot.PodList[0]
	second := detect(t, snapshot)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("findings depend on input order:\n%+v\n%+v", first, second)
	}
	var got []string
	for _, finding := range first {
		got = append(got, finding.PrimaryResource.Kind+"/"+finding.PrimaryResource.Name+"/"+finding.RuleID)
	}
	want := []string{
		"Deployment/api/workload-container-image-pull-failing",
		"Deployment/frontend/workload-container-crash-looping",
		"Pod/adhoc/workload-pod-unknown",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("findings order %v, want %v", got, want)
	}
}

func TestWithoutRecordedClockAgesUseTheLatestObjectTime(t *testing.T) {
	snapshot := healthy()
	snapshot.ObservedAtTime = nil
	pending(5 * time.Minute)(&snapshot)
	// The newest recorded timestamp is the warning event's last occurrence.
	snapshot.EventList = []corev1.Event{warning("mount", "Pod", "frontend-7d9-abc", "FailedMount", "timed out")}
	findings := detect(t, snapshot)
	if len(findings) != 1 || findings[0].RuleID != "workload-pod-pending" {
		t.Fatalf("expected pending finding from snapshot-derived time, got %+v", findings)
	}
	snapshot.EventList = nil
	if findings := detect(t, snapshot); len(findings) != 0 {
		t.Fatalf("with no later evidence the pod cannot be proven past its grace period, got %+v", findings)
	}
}

func TestPolicyValidation(t *testing.T) {
	if _, err := workloadhealth.NewDetectorWithPolicy(spec(), workloadhealth.Policy{}); err == nil {
		t.Fatal("zero policy must be rejected")
	}
	policy := workloadhealth.DefaultPolicy()
	policy.RestartThreshold = 0
	if _, err := workloadhealth.NewDetectorWithPolicy(spec(), policy); err == nil {
		t.Fatal("non-positive restart threshold must be rejected")
	}
	if _, err := workloadhealth.NewDetectorWithPolicy(spec(), workloadhealth.DefaultPolicy()); err != nil {
		t.Fatal(err)
	}
}
