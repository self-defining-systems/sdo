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
	for _, change := range []string{"objective", "backend", "configmap", "selector", "container-port", "command", "corrupt-record", "new-port"} {
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
			case "selector":
				s.ServiceList[0].Spec.Selector = map[string]string{"app": "other"}
			case "container-port":
				s.PodList[0].Spec.Containers[0].Ports[0].ContainerPort = 8888
			case "command":
				s.PodList[0].Spec.Containers[0].Command = []string{"changed-server"}
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

// API-assigned incarnation/routing values do not change what the app serves.
func TestReachabilityReferenceSurvivesServiceRecreation(t *testing.T) {
	for _, change := range []string{"cluster-ip", "uid", "resource-version", "node-port", "health-check-node-port", "ip-family-defaults", "pod-endpoint-identity", "all"} {
		t.Run(change, func(t *testing.T) {
			s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
			s.ServiceList[0].UID = "before-service"
			s.ServiceList[0].Spec.ClusterIPs = []string{s.ServiceList[0].Spec.ClusterIP}
			auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}
			b, err := servicehealth.CaptureReachabilityBaseline(s, auth)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(b)
			if err != nil {
				t.Fatal(err)
			}
			var reference servicehealth.ReachabilityBaseline
			if err := json.Unmarshal(raw, &reference); err != nil {
				t.Fatal(err)
			}
			s.window.Baseline, s.window.SourceInputs, s.window.ObjectiveDigest = &reference, auth.SourceInputs, auth.ObjectiveDigest
			service := &s.ServiceList[0]
			if change == "cluster-ip" || change == "all" {
				service.Spec.ClusterIP = "10.96.21.87"
				service.Spec.ClusterIPs = []string{service.Spec.ClusterIP}
			}
			if change == "uid" || change == "all" {
				service.UID = "after-service"
			}
			if change == "resource-version" || change == "all" {
				service.ResourceVersion = "200"
			}
			if change == "node-port" || change == "all" {
				service.Spec.Ports[0].NodePort = 32001
			}
			if change == "health-check-node-port" || change == "all" {
				service.Spec.HealthCheckNodePort = 32002
			}
			if change == "ip-family-defaults" || change == "all" {
				service.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
				policy := corev1.IPFamilyPolicySingleStack
				service.Spec.IPFamilyPolicy = &policy
			}
			if change == "pod-endpoint-identity" || change == "all" {
				s.PodList[0].Name, s.PodList[0].UID, s.PodList[0].ResourceVersion = "geo-recreated", "new-pod", "201"
				s.PodList[0].Status.PodIP = "10.244.2.3"
				s.EndpointList[0].UID = "new-endpoints"
				s.EndpointList[0].Subsets = []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "10.244.2.3"}}, Ports: []corev1.EndpointPort{{Port: 8083}}}}
			}
			if got := detectReachability(t, s); len(got) != 0 {
				t.Fatalf("recreated target lost exclusion: %+v", got)
			}
			if got := servicehealth.ReachabilityObservations(s); len(got) != 1 {
				t.Fatalf("reference lost after recreation: %+v", got)
			}
		})
	}
}

func TestReachabilityConfigurationRetainsProtocolAndHeadlessRouting(t *testing.T) {
	for _, change := range []string{"protocol", "headless"} {
		t.Run(change, func(t *testing.T) {
			s := geoSnapshot(intstr.FromInt32(8083), true)
			target := geoTarget(nil).Target
			before, ok := servicehealth.ReachabilityTargetConfiguration(s, target)
			if !ok {
				t.Fatal("missing original target")
			}
			if change == "protocol" {
				s.ServiceList[0].Spec.Ports[0].Protocol = corev1.ProtocolUDP
			} else {
				s.ServiceList[0].Spec.ClusterIP = corev1.ClusterIPNone
			}
			after, ok := servicehealth.ReachabilityTargetConfiguration(s, target)
			if ok && before == after {
				t.Fatal("routing configuration change retained reference identity")
			}
		})
	}
}

func TestRecreatedServiceRealFaultRetainsThirdSampleGate(t *testing.T) {
	s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
	auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}
	b, err := servicehealth.CaptureReachabilityBaseline(s, auth)
	if err != nil {
		t.Fatal(err)
	}
	s.window.Baseline, s.window.SourceInputs, s.window.ObjectiveDigest = b, auth.SourceInputs, auth.ObjectiveDigest
	s.ServiceList[0].UID, s.ServiceList[0].Spec.ClusterIP = "recreated", "10.96.21.87"
	// A later serving observation ends the exclusion, even after recreation.
	served := observedAt.Add(time.Minute)
	s.window.Targets[0].LastSuccess = &served
	s.window.Targets[0].Samples = nil
	for i := 0; i < 3; i++ {
		at := served.Add(time.Duration(i+1) * 10 * time.Second)
		s.window.ObservedAt = at
		s.window.Targets[0].Samples = append(s.window.Targets[0].Samples, servicehealth.DialSample{At: at, Class: servicehealth.DialRefused})
		findings := detectReachability(t, s)
		if i < 2 && len(findings) != 0 {
			t.Fatal("real fault fired before third sample")
		}
		if i == 2 && (len(findings) != 1 || !strings.Contains(findings[0].Evidence, "over 20s")) {
			t.Fatalf("real fault must fire on third sample over 20s: %+v", findings)
		}
	}
}

func TestStaleExclusionsNameOnlyTargetsWhoseConfigurationChanged(t *testing.T) {
	s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
	auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}
	baseline, err := servicehealth.CaptureReachabilityBaseline(s, auth)
	if err != nil || len(baseline.Exclusions) != 1 {
		t.Fatalf("no exclusion: %+v %v", baseline, err)
	}
	if got := baseline.StaleExclusions(s); len(got) != 0 {
		t.Fatalf("unchanged configuration is stale: %+v", got)
	}
	s.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8888)
	if got := baseline.StaleExclusions(s); len(got) != 1 {
		t.Fatalf("changed configuration is not stale: %+v", got)
	}
	s.ServiceList = nil
	if got := baseline.StaleExclusions(s); len(got) != 0 {
		t.Fatalf("a removed target has nothing to refresh: %+v", got)
	}
	if got := (*servicehealth.ReachabilityBaseline)(nil).StaleExclusions(s); len(got) != 0 {
		t.Fatalf("no record cannot be stale: %+v", got)
	}
}

// Without a current baseline the detector cannot tell a declared-but-never-served
// port from one broken before observation began: such a port is unknown, not an
// incident. A port seen serving still fires, the detector stays confirmed (closure
// is never held on a measurement that waits for closure), and capture judges the
// raw window.
func TestReachabilityWithoutBaselineReportsNeverServedTargetsUnknown(t *testing.T) {
	s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
	s.window.BaselinePending = true
	detector := servicehealth.NewReachabilityDetector(reachabilitySpec()).(interface {
		Confirmed(sdk.DetectionContext) bool
		ConfirmationReason(sdk.DetectionContext) string
	})
	if got := detectReachability(t, s); len(got) != 0 {
		t.Fatalf("never-served port fired without a baseline: %+v", got)
	}
	if !detector.Confirmed(s) {
		t.Fatal("an unknown target held the detector unconfirmed")
	}
	if unknown := servicehealth.ReachabilityUnknown(s); len(unknown) != 1 || unknown[0].Service != "geo" {
		t.Fatalf("unknown targets: %+v", unknown)
	}
	if reason := detector.ConfirmationReason(s); !strings.Contains(reason, "unknown") || !strings.Contains(reason, "geo:8083") {
		t.Fatalf("unknown not reported: %q", reason)
	}
	auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}
	if b, err := servicehealth.CaptureReachabilityBaseline(s, auth); err != nil || len(b.Exclusions) != 1 {
		t.Fatalf("capture judged the pending window instead of the measured one: %+v %v", b, err)
	}
	at := s.window.Targets[0].Samples[0].At.Add(-time.Second)
	s.window.Targets[0].LastSuccess = &at
	if got := detectReachability(t, s); len(got) != 1 {
		t.Fatalf("a port seen serving was hidden without a baseline: %+v", got)
	}
	if unknown := servicehealth.ReachabilityUnknown(s); len(unknown) != 0 {
		t.Fatalf("a served port reported unknown: %+v", unknown)
	}
	s.window.BaselinePending = false
	s.window.Targets[0].LastSuccess = nil
	if reason := detector.ConfirmationReason(s); reason != "" {
		t.Fatalf("a confirmed window with no pending baseline gave a reason: %q", reason)
	}
}
