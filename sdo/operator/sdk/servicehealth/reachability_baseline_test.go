package servicehealth_test

import (
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"strings"
	"testing"
	"time"
)

func TestReachabilityBaselineExcludesMeasuredFailureButNotChangedTarget(t *testing.T) {
	s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
	auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}
	baseline, err := servicehealth.CaptureReachabilityBaseline(s, auth)
	if err != nil {
		t.Fatal(err)
	}
	if len(baseline.Exclusions) != 1 {
		t.Fatalf("expected one measured exclusion, got %+v", baseline)
	}
	s.window.Baseline = baseline
	s.window.SourceInputs = auth.SourceInputs
	s.window.ObjectiveDigest = auth.ObjectiveDigest
	if got := detectReachability(t, s); len(got) != 0 {
		t.Fatalf("healthy baseline failure must be explicitly excluded: %+v", got)
	}
	s.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8888)
	if got := detectReachability(t, s); len(got) != 1 {
		t.Fatal("new wrong targetPort must invalidate exclusion and fire")
	}
	s.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8083)
	s.window.SourceInputs = strings.Repeat("c", 64)
	if got := detectReachability(t, s); len(got) != 1 {
		t.Fatal("changed source must invalidate exclusion")
	}
}

func TestReachabilityBaselineRequiresHealthyAuthorityAndMeasuredFailures(t *testing.T) {
	s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
	auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "incident", GoalHealthy: true}
	if _, err := servicehealth.CaptureReachabilityBaseline(s, auth); err == nil {
		t.Fatal("ordinary incident cannot authorize baseline")
	}
	auth.Reason = "healthy-baseline"
	auth.GoalHealthy = false
	if _, err := servicehealth.CaptureReachabilityBaseline(s, auth); err == nil {
		t.Fatal("unhealthy goal cannot authorize baseline")
	}
	auth.GoalHealthy = true
	s.window.Targets[0].Samples = nil
	b, err := servicehealth.CaptureReachabilityBaseline(s, auth)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Exclusions) != 0 {
		t.Fatal("absence of prior success/fresh samples is not exclusion evidence")
	}
}

func TestReachabilityBaselineConfigurationAndRecordInvalidation(t *testing.T) {
	for _, change := range []string{"objective", "backend", "configmap", "service-identity", "corrupt-record", "new-port"} {
		t.Run(change, func(t *testing.T) {
			s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
			s.PodList[0].Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "config"}}}}
			auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}
			b, err := servicehealth.CaptureReachabilityBaseline(s, auth)
			if err != nil {
				t.Fatal(err)
			}
			// A process restart reads exactly the accepted JSON, with no live pointer history.
			raw, _ := json.Marshal(b)
			var restarted servicehealth.ReachabilityBaseline
			if err := json.Unmarshal(raw, &restarted); err != nil {
				t.Fatal(err)
			}
			s.window.Baseline = &restarted
			s.window.SourceInputs = auth.SourceInputs
			s.window.ObjectiveDigest = auth.ObjectiveDigest
			switch change {
			case "objective":
				s.window.ObjectiveDigest = strings.Repeat("c", 64)
			case "backend":
				s.PodList[0].Spec.Containers[0].Image = "changed"
			case "configmap":
				s.ConfigMapList = []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "config"}, Data: map[string]string{"mode": "changed"}}}
			case "service-identity":
				s.ServiceList[0].UID = "new-service"
			case "corrupt-record":
				restarted.Authorization.GoalHealthy = false
			case "new-port":
				s.ServiceList[0].Spec.Ports[0].Port = 9600
				s.window.Targets[0].Target.Port = 9600
			}
			if got := detectReachability(t, s); len(got) != 1 {
				t.Fatalf("%s stale baseline hid a new failure: %+v", change, got)
			}
		})
	}
}

func TestUnrelatedConfigurationFaultDoesNotInvalidateBaselineTarget(t *testing.T) {
	s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
	auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}
	b, err := servicehealth.CaptureReachabilityBaseline(s, auth)
	if err != nil {
		t.Fatal(err)
	}
	s.window.Baseline = b
	s.window.SourceInputs = auth.SourceInputs
	s.window.ObjectiveDigest = auth.ObjectiveDigest
	s.ConfigMapList = []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "unrelated"}, Data: map[string]string{"flag": "changed"}}}
	s.NetworkPolicyList = []networkingv1.NetworkPolicy{{ObjectMeta: metav1.ObjectMeta{Name: "other-service"}, Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "unrelated"}}}}}
	if got := detectReachability(t, s); len(got) != 0 {
		t.Fatalf("unrelated live fault revived an accepted baseline false positive: %+v", got)
	}
}

// A successful dial cannot roll out of the never-served reference evidence.
func TestReachabilityBaselineNeverExcludesPreviouslyServingPort(t *testing.T) {
	s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
	at := s.window.Targets[0].Samples[0].At.Add(-time.Second)
	s.window.Targets[0].LastSuccess = &at
	auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}
	b, err := servicehealth.CaptureReachabilityBaseline(s, auth)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Exclusions) != 0 {
		t.Fatal("a port that served must still fire after three refusals")
	}
}

func TestReachabilityReferenceObservationDoesNotHideLaterServingFailure(t *testing.T) {
	s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
	auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}
	reference, err := servicehealth.CaptureReachabilityBaseline(s, auth)
	if err != nil {
		t.Fatal(err)
	}
	s.window.Baseline = reference
	s.window.SourceInputs = auth.SourceInputs
	s.window.ObjectiveDigest = auth.ObjectiveDigest
	observations := servicehealth.ReachabilityObservations(s)
	if len(observations) != 1 || observations[0].Status != "declared-but-never-served" {
		t.Fatalf("reference must remain visible: %+v", observations)
	}
	at := reference.CapturedAt.Add(time.Second)
	s.window.Targets[0].LastSuccess = &at
	s.window.ObservedAt = at.Add(30 * time.Second)
	for i := range s.window.Targets[0].Samples {
		s.window.Targets[0].Samples[i].At = at.Add(time.Duration(i+1) * 10 * time.Second)
	}
	if findings := detectReachability(t, s); len(findings) != 1 {
		t.Fatal("a later success must restore the ordinary failure gate")
	}
	if len(servicehealth.ReachabilityObservations(s)) != 0 {
		t.Fatal("served port still described as never served")
	}
}

func TestReachabilityConfirmedRequiresCompleteFreshProbeWindow(t *testing.T) {
	for _, mode := range []string{"complete", "missing", "two", "stale", "repeated", "omitted", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
			switch mode {
			case "mixed":
				s.window.Targets[0].Samples[0].OK = true
			case "missing":
				s.window.Targets = nil
			case "two":
				s.window.Targets[0].Samples = s.window.Targets[0].Samples[1:]
			case "stale":
				s.window.ObservedAt = s.window.ObservedAt.Add(time.Hour)
			case "repeated":
				s.window.Targets[0].Samples[1].At = s.window.Targets[0].Samples[0].At
			case "omitted":
				s.window.Omitted = 1
			}
			detector := servicehealth.NewReachabilityDetector(reachabilitySpec()).(interface {
				Confirmed(sdk.DetectionContext) bool
			})
			if got := detector.Confirmed(s); got != (mode == "complete") {
				t.Fatalf("confirmed=%v", got)
			}
		})
	}
}
