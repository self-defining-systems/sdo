package servicehealth_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
)

func TestConfirmedAcceptsIdleSpacedHealthySamples(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	ready := now.Add(-time.Hour)
	var samples []servicehealth.DialSample
	for i := 5; i >= 1; i-- {
		samples = append(samples, servicehealth.DialSample{At: now.Add(-time.Duration(i) * 10 * time.Second), OK: true})
	}
	window := servicehealth.ReachabilityWindow{ObservedAt: now, Interval: time.Second, Timeout: time.Second, Failures: 5,
		Targets: []servicehealth.TargetObservations{{Target: servicehealth.ReachabilityTarget{Service: "app", Port: 8080}, Samples: samples}}}
	snapshot := sdktest.Snapshot{NamespaceName: "app", Reachability: &window,
		ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1", Selector: map[string]string{"app": "test"}, Ports: []corev1.ServicePort{{Port: 8080}}}}},
		PodList:     []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app", Labels: map[string]string{"app": "test"}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(ready)}}}}}}
	d := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "reachability"})
	confirmed, ok := d.(interface{ Confirmed(sdk.DetectionContext) bool })
	if !ok {
		t.Fatal("detector lacks Confirmed")
	}
	if confirmed.Confirmed(snapshot) {
		t.Fatal("a 10s-old healthy sample confirmed without an idle interval")
	}
	window.IdleInterval = servicehealth.DefaultReachabilityIdleInterval
	if !confirmed.Confirmed(snapshot) {
		t.Fatal("idle-spaced healthy samples must confirm the window")
	}
}
