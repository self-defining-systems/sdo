package servicehealth_test

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"testing"
	"time"
)

func TestDefaultCadenceRejectsShortStallsAndDetectsSustainedFailure(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	success := start.Add(-time.Second)
	window := servicehealth.ReachabilityWindow{Targets: []servicehealth.TargetObservations{{Target: servicehealth.ReachabilityTarget{Service: "app", Port: 8080}, LastSuccess: &success}}}
	snapshot := sdktest.Snapshot{NamespaceName: "app", Reachability: &window,
		ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1", Selector: map[string]string{"app": "test"}, Ports: []corev1.ServicePort{{Port: 8080}}}}},
		PodList:     []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app", Labels: map[string]string{"app": "test"}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(success)}}}}}}
	d := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "reachability"})
	// Include successes between several short stalls: they must reset the run.
	for _, seconds := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12} {
		at := start.Add(time.Duration(seconds) * time.Second)
		ok := seconds == 3 || seconds == 7
		window.Targets[0].Samples = append(window.Targets[0].Samples, servicehealth.DialSample{At: at, OK: ok, Class: servicehealth.DialRefused})
		window.ObservedAt = at
		// Sample only on the production default cadence.
		if at.Sub(start)%servicehealth.DefaultReachabilityInterval != 0 {
			window.Targets[0].Samples = window.Targets[0].Samples[:len(window.Targets[0].Samples)-1]
			continue
		}
		findings, err := d.Detect(context.Background(), snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if seconds < 12 && len(findings) > 0 {
			t.Fatalf("short or incomplete stall fired at %ds", seconds)
		}
		if seconds == 12 && len(findings) != 1 {
			t.Fatal("sustained failure not detected within five seconds")
		}
	}
	findings, err := d.Detect(context.Background(), snapshot)
	if err != nil || len(findings) != 1 {
		t.Fatalf("sustained failure not detected within five seconds: findings=%v err=%v", findings, err)
	}
}
