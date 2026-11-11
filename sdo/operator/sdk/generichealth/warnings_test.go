package generichealth

import (
	"context"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/workloadhealth"
)

var warningNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// podState is the pod's current readiness: ready since the given time, not
// ready since it, or completed.
type podState struct {
	ready     bool
	succeeded bool
	since     time.Time
}

// warningSnapshot is one Service backed by one pod that has a repeated Warning
// Event with the given reason: first three minutes ago, last 30 seconds ago.
func warningSnapshot(reason, message string, state podState) sdktest.Snapshot {
	labels := map[string]string{"app": "example"}
	status := core.ConditionFalse
	if state.ready {
		status = core.ConditionTrue
	}
	started := meta.NewTime(warningNow.Add(-time.Hour))
	pod := core.Pod{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pod-1", Labels: labels, CreationTimestamp: started},
		Spec:       core.PodSpec{Containers: []core.Container{{Name: "main"}}},
		Status: core.PodStatus{
			Phase:      core.PodRunning,
			Conditions: []core.PodCondition{{Type: core.PodReady, Status: status, LastTransitionTime: meta.NewTime(state.since)}},
			ContainerStatuses: []core.ContainerStatus{{
				Name: "main", Ready: state.ready,
				State: core.ContainerState{Running: &core.ContainerStateRunning{StartedAt: started}},
			}},
		},
	}
	if state.succeeded {
		pod.Status.Phase = core.PodSucceeded
		pod.Status.ContainerStatuses[0].State = core.ContainerState{Terminated: &core.ContainerStateTerminated{Reason: "Completed"}}
	}
	endpoints := core.Endpoints{ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "svc"}}
	if state.ready {
		endpoints.Subsets = []core.EndpointSubset{{Addresses: []core.EndpointAddress{{IP: "10.0.0.2"}}}}
	}
	now := warningNow
	return sdktest.Snapshot{
		NamespaceName:  "ns",
		ObservedAtTime: &now,
		ServiceList: []core.Service{{
			ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "svc"},
			Spec:       core.ServiceSpec{ClusterIP: "10.0.0.1", Selector: labels, Ports: []core.ServicePort{{Port: 80}}},
		}},
		PodList:      []core.Pod{pod},
		EndpointList: []core.Endpoints{endpoints},
		EventList: []core.Event{{
			ObjectMeta:     meta.ObjectMeta{Namespace: "ns", Name: "pod-1.event"},
			InvolvedObject: core.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: "ns", Name: "pod-1"},
			Type:           core.EventTypeWarning, Reason: reason, Message: message, Count: 5,
			FirstTimestamp: meta.NewTime(warningNow.Add(-3 * time.Minute)),
			LastTimestamp:  meta.NewTime(warningNow.Add(-30 * time.Second)),
		}},
	}
}

func ruleIDs(t *testing.T, d sdk.Detector, s sdktest.Snapshot) []string {
	t.Helper()
	findings, err := d.Detect(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, f := range findings {
		ids = append(ids, f.RuleID)
	}
	return ids
}

func has(ids []string, id string) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

const readinessTimeout = `Readiness probe failed: Get "http://10.0.0.2:8080/ready": context deadline exceeded`

// Every pod-scoped warning reason follows one rule: the Event explains a pod
// that is unhealthy now, and the finding clears as soon as the pod's current
// state shows health, without replacing the pod or editing its spec.
func TestPodWarningEventsFireOnlyWhileThePodIsUnhealthy(t *testing.T) {
	detector := NewWarnings(sdk.DetectorSpec{ID: "warning-events"})
	reasons := map[string]string{
		"Unhealthy":              readinessTimeout,
		"Unhealthy/liveness":     `Liveness probe failed: Get "http://10.0.0.2:8080/live": context deadline exceeded`,
		"BackOff":                "Back-off restarting failed container main",
		"FailedMount":            `MountVolume.SetUp failed for volume "config": configmap "config" not found`,
		"FailedAttachVolume":     "AttachVolume.Attach failed",
		"FailedScheduling":       "0/3 nodes are available",
		"FailedCreatePodSandBox": "failed to create pod sandbox",
		"Evicted":                "The node was low on resource: memory",
	}
	cases := []struct {
		name  string
		state podState
		fire  bool
	}{
		{"ready throughout the events", podState{ready: true, since: warningNow.Add(-time.Hour)}, false},
		{"ready again after the last event", podState{ready: true, since: warningNow.Add(-10 * time.Second)}, false},
		{"ready again between the events", podState{ready: true, since: warningNow.Add(-time.Minute)}, false},
		{"not ready since the events began", podState{ready: false, since: warningNow.Add(-2 * time.Minute)}, true},
		{"not ready for longer than the events", podState{ready: false, since: warningNow.Add(-time.Hour)}, true},
		{"completed", podState{succeeded: true, since: warningNow.Add(-10 * time.Second)}, false},
	}
	for key, message := range reasons {
		reason := key
		if i := len("Unhealthy"); len(key) > i && key[:i] == "Unhealthy" {
			reason = "Unhealthy"
		}
		for _, c := range cases {
			t.Run(key+"/"+c.name, func(t *testing.T) {
				got := has(ruleIDs(t, detector, warningSnapshot(reason, message, c.state)), "warning-"+reason)
				if got != c.fire {
					t.Fatalf("warning-%s fired=%t, want %t", reason, got, c.fire)
				}
			})
		}
	}
}

// Readiness failures that remove serving capacity stay reported after the
// warning-events change: the workload detector reports the unready container
// and the endpoint detector reports a Service left without a ready endpoint.
// Load-induced probe timeouts on a pod that stays Ready are reported by none.
func TestReadinessFailuresThatRemoveCapacityAreReportedByStateDetectors(t *testing.T) {
	workloads := workloadhealth.NewDetector(sdk.DetectorSpec{ID: "workload-health"})
	endpoints := servicehealth.NewReadyEndpointsDetector(sdk.DetectorSpec{ID: "service-endpoints"})
	warnings := NewWarnings(sdk.DetectorSpec{ID: "warning-events"})

	unready := warningSnapshot("Unhealthy", readinessTimeout, podState{ready: false, since: warningNow.Add(-2 * time.Minute)})
	if ids := ruleIDs(t, workloads, unready); !has(ids, workloadhealth.RuleNotReady) {
		t.Fatalf("workload-health on an unready pod = %v, want %s", ids, workloadhealth.RuleNotReady)
	}
	if ids := ruleIDs(t, endpoints, unready); !has(ids, servicehealth.RuleNoReadyEndpoints) {
		t.Fatalf("service-endpoints with no ready pod = %v, want %s", ids, servicehealth.RuleNoReadyEndpoints)
	}

	ready := warningSnapshot("Unhealthy", readinessTimeout, podState{ready: true, since: warningNow.Add(-time.Hour)})
	for name, d := range map[string]sdk.Detector{"workload-health": workloads, "service-endpoints": endpoints, "warning-events": warnings} {
		if ids := ruleIDs(t, d, ready); len(ids) != 0 {
			t.Fatalf("%s on a pod that stayed Ready = %v, want none", name, ids)
		}
	}
}
