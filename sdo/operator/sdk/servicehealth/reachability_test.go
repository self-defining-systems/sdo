package servicehealth_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
)

var (
	readyAt    = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	observedAt = readyAt.Add(5 * time.Minute)
	prober     = servicehealth.ProberIdentity{
		Namespace: "sdo-control", Labels: map[string]string{"app.kubernetes.io/name": "sdo-prober"},
	}
)

// reachabilitySnapshot pairs a namespace snapshot with the controller's dial
// observations, the shape the controller runtime hands to detectors.
type reachabilitySnapshot struct {
	sdktest.Snapshot
	window *servicehealth.ReachabilityWindow
}

func (s reachabilitySnapshot) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	if s.window == nil {
		return servicehealth.ReachabilityWindow{}, false
	}
	return *s.window, true
}

func reachabilitySpec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "service-reachability", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: servicehealth.ReachabilityWatches(), Interval: 10 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:   []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap",
	}
}

var geoLabels = map[string]string{"io.kompose.service": "geo"}

func geoService(targetPort intstr.IntOrString) corev1.Service {
	return corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "geo", Namespace: namespace},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.96.0.20", Selector: geoLabels,
			Ports: []corev1.ServicePort{{Name: "grpc", Port: 8083, TargetPort: targetPort, Protocol: corev1.ProtocolTCP}},
		},
	}
}

func geoPod(name string, ready bool, containerPort int32) corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: geoLabels},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "hotel-reserv-geo", Ports: []corev1.ContainerPort{{Name: "grpc", ContainerPort: containerPort, Protocol: corev1.ProtocolTCP}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: status, LastTransitionTime: metav1.NewTime(readyAt),
		}}},
	}
}

func geoSnapshot(targetPort intstr.IntOrString, ready bool) sdktest.Snapshot {
	readyEndpoints := 0
	if ready {
		readyEndpoints = 1
	}
	return sdktest.Snapshot{
		NamespaceName:  namespace,
		ServiceList:    []corev1.Service{geoService(targetPort)},
		DeploymentList: []appsv1.Deployment{deployment("geo", 1, geoLabels)},
		PodList:        []corev1.Pod{geoPod("geo-5f7", ready, 8083)},
		EndpointList:   []corev1.Endpoints{endpoints("geo", readyEndpoints)},
	}
}

// samples returns one dial every 10s ending at observedAt; ok marks each.
func samples(class servicehealth.DialErrorClass, ok ...bool) []servicehealth.DialSample {
	result := make([]servicehealth.DialSample, 0, len(ok))
	for index, success := range ok {
		at := observedAt.Add(-time.Duration(len(ok)-1-index) * 10 * time.Second)
		sample := servicehealth.DialSample{At: at, Duration: 2 * time.Millisecond, OK: success}
		if !success {
			sample.Class = class
			sample.Error = "dial tcp 10.96.0.20:8083: " + string(class)
		}
		result = append(result, sample)
	}
	return result
}

func window(observations ...servicehealth.TargetObservations) *servicehealth.ReachabilityWindow {
	return &servicehealth.ReachabilityWindow{
		ObservedAt: observedAt, Interval: 10 * time.Second, Timeout: time.Second, Failures: 3,
		Targets: observations, Prober: prober,
	}
}

func geoTarget(sampled []servicehealth.DialSample) servicehealth.TargetObservations {
	return servicehealth.TargetObservations{
		Target:  servicehealth.ReachabilityTarget{Service: "geo", Port: 8083, PortName: "grpc"},
		Samples: sampled,
	}
}

func detectReachability(t *testing.T, snapshot sdk.DetectionContext) []sdk.Finding {
	t.Helper()
	findings, err := servicehealth.NewReachabilityDetector(reachabilitySpec()).Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	return findings
}

func TestReachabilityFiresWhenReadyPodsRefuseFromTheFirstDial(t *testing.T) {
	snapshot := reachabilitySnapshot{
		Snapshot: geoSnapshot(intstr.FromInt32(8083), true),
		window:   window(geoTarget(samples(servicehealth.DialRefused, false, false, false))),
	}
	// Applying app source through SDO must not hide a real application fault.
	snapshot.ServiceList[0].ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply}}
	findings := detectReachability(t, snapshot)
	if len(findings) != 1 {
		t.Fatalf("ready pods refusing every fresh dial must fire without a prior success, got %v", findings)
	}
	finding := findings[0]
	if finding.RuleID != servicehealth.RuleServicePortUnreachable || finding.DetectorID != "service-reachability" ||
		finding.PrimaryResource != (sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: "geo"}) {
		t.Fatalf("finding must name the Service, got %+v", finding)
	}
	wantRelated := []sdk.ObjectRef{
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: namespace, Name: "geo"},
		{APIVersion: "v1", Kind: "Pod", Namespace: namespace, Name: "geo-5f7"},
	}
	if !reflect.DeepEqual(finding.RelatedResources, wantRelated) {
		t.Fatalf("related resources: got %v want %v", finding.RelatedResources, wantRelated)
	}
	for _, want := range []string{"port 8083", "grpc", "targetPort 8083", "refused", "3 consecutive", "never", "hotel-reserv-geo grpc:8083/TCP"} {
		if !strings.Contains(finding.Summary+" "+finding.Evidence, want) {
			t.Fatalf("finding must mention %q:\n%s\n%s", want, finding.Summary, finding.Evidence)
		}
	}
	if strings.Contains(finding.Evidence, "matches no containerPort") {
		t.Fatalf("a declared targetPort must not carry a mismatch hint: %s", finding.Evidence)
	}
}

func TestReachabilityStaysQuietWithoutReadyBackends(t *testing.T) {
	snapshot := reachabilitySnapshot{
		Snapshot: geoSnapshot(intstr.FromInt32(8083), false),
		window:   window(geoTarget(samples(servicehealth.DialRefused, false, false, false, false))),
	}
	if findings := detectReachability(t, snapshot); len(findings) != 0 {
		t.Fatalf("a Service without ready backends belongs to service-endpoints, got %v", findings)
	}
}

func TestReachabilityNeedsConsecutiveFreshFailuresWhileReady(t *testing.T) {
	for name, sampled := range map[string][]servicehealth.DialSample{
		"too few":            samples(servicehealth.DialRefused, false, false),
		"recovered":          samples(servicehealth.DialRefused, false, false, false, true),
		"interleaved":        samples(servicehealth.DialRefused, false, true, false, false),
		"no samples":         nil,
		"stale observations": shift(samples(servicehealth.DialRefused, false, false, false), -2*time.Minute),
	} {
		snapshot := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window(geoTarget(sampled))}
		if findings := detectReachability(t, snapshot); len(findings) != 0 {
			t.Fatalf("%s: must not fire, got %v", name, findings)
		}
	}
}

func TestReachabilityIgnoresDialsBeforeTheBackendBecameReady(t *testing.T) {
	snapshot := geoSnapshot(intstr.FromInt32(8083), true)
	// The pod became ready 15s ago: only the last two of four failed dials
	// happened while the Service had a ready backend.
	snapshot.PodList[0].Status.Conditions[0].LastTransitionTime = metav1.NewTime(observedAt.Add(-15 * time.Second))
	quiet := reachabilitySnapshot{Snapshot: snapshot, window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false, false)))}
	if findings := detectReachability(t, quiet); len(findings) != 0 {
		t.Fatalf("dials refused while the pod was still starting must not count, got %v", findings)
	}
}

func TestReachabilityClassifiesTimeoutsAndDNS(t *testing.T) {
	for class, word := range map[servicehealth.DialErrorClass]string{
		servicehealth.DialTimeout: "timed out", servicehealth.DialDNS: "did not resolve",
	} {
		snapshot := reachabilitySnapshot{
			Snapshot: geoSnapshot(intstr.FromInt32(8083), true),
			window:   window(geoTarget(samples(class, false, false, false))),
		}
		findings := detectReachability(t, snapshot)
		if len(findings) != 1 || !strings.Contains(findings[0].Summary, word) {
			t.Fatalf("%s: summary must say %q, got %v", class, word, findings)
		}
		ports, _ := findings[0].Metadata["ports"].([]map[string]any)
		if len(ports) != 1 || ports[0]["error_class"] != string(class) {
			t.Fatalf("%s: metadata must carry the error class, got %v", class, findings[0].Metadata)
		}
	}
}

func TestReachabilityHintsAtATargetPortNoContainerDeclares(t *testing.T) {
	snapshot := reachabilitySnapshot{
		Snapshot: geoSnapshot(intstr.FromInt32(9999), true),
		window:   window(geoTarget(samples(servicehealth.DialRefused, false, false, false))),
	}
	findings := detectReachability(t, snapshot)
	if len(findings) != 1 || !strings.Contains(findings[0].Evidence, "targetPort 9999 matches no containerPort") {
		t.Fatalf("expected a targetPort mismatch hint, got %v", findings)
	}
	named := reachabilitySnapshot{
		Snapshot: geoSnapshot(intstr.FromString("http"), true),
		window:   window(geoTarget(samples(servicehealth.DialRefused, false, false, false))),
	}
	findings = detectReachability(t, named)
	if len(findings) != 1 || !strings.Contains(findings[0].Evidence, `named targetPort "http" matches no containerPort`) {
		t.Fatalf("expected a named targetPort mismatch hint, got %v", findings)
	}
}

func TestReachabilityTargetsSkipHeadlessExternalNameUDPAndSelectorless(t *testing.T) {
	snapshot := geoSnapshot(intstr.FromInt32(8083), true)
	snapshot.ServiceList[0].Spec.Ports = append(snapshot.ServiceList[0].Spec.Ports,
		corev1.ServicePort{Name: "metrics", Port: 9090}, corev1.ServicePort{Name: "dns", Port: 53, Protocol: corev1.ProtocolUDP})
	headless := service("mongodb-geo", map[string]string{"app": "mongodb-geo"})
	headless.Spec.ClusterIP = corev1.ClusterIPNone
	external := service("search-alias", map[string]string{"app": "search"})
	external.Spec.Type = corev1.ServiceTypeExternalName
	managed := service("sdo-prober", map[string]string{"app.kubernetes.io/name": "sdo-prober"})
	managed.Labels = map[string]string{"app.kubernetes.io/managed-by": "sdo"}
	snapshot.ServiceList = append(snapshot.ServiceList, headless, external, service("manual", nil), managed, service("frontend", frontendLabels))
	targets, omitted := servicehealth.ReachabilityTargets(snapshot)
	want := []servicehealth.ReachabilityTarget{
		{Service: "frontend", Port: 5000, PortName: "http"},
		{Service: "geo", Port: 8083, PortName: "grpc"},
		{Service: "geo", Port: 9090, PortName: "metrics"},
	}
	if omitted != 0 || !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets: got %v (omitted %d) want %v", targets, omitted, want)
	}
}

func TestReachabilityTargetsAreBounded(t *testing.T) {
	snapshot := sdktest.Snapshot{NamespaceName: namespace}
	for index := 0; index < servicehealth.MaxReachabilityTargets+5; index++ {
		name := "svc-" + string(rune('a'+index/26)) + string(rune('a'+index%26))
		snapshot.ServiceList = append(snapshot.ServiceList, service(name, map[string]string{"app": name}))
	}
	targets, omitted := servicehealth.ReachabilityTargets(snapshot)
	if len(targets) != servicehealth.MaxReachabilityTargets || omitted != 5 || targets[0].Service != "svc-aa" {
		t.Fatalf("expected the first %d targets by name and 5 omitted, got %d (omitted %d)", servicehealth.MaxReachabilityTargets, len(targets), omitted)
	}
}

func TestReachabilityIgnoresTargetsThatAreNoLongerEligible(t *testing.T) {
	snapshot := geoSnapshot(intstr.FromInt32(8083), true)
	snapshot.ServiceList[0].Spec.ClusterIP = corev1.ClusterIPNone
	headless := reachabilitySnapshot{Snapshot: snapshot, window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)))}
	if findings := detectReachability(t, headless); len(findings) != 0 {
		t.Fatalf("a Service that became headless is not dialed through its cluster IP, got %v", findings)
	}
	violations := sdktest.ExternalNameEndpointViolations(servicehealth.NewReachabilityDetector(reachabilitySpec()), []string{"geo"})
	if len(violations) != 0 {
		t.Fatalf("detector must exempt ExternalName Services: %v", violations)
	}
}

func TestReachabilitySuppressedWhenANetworkPolicyExcludesTheProber(t *testing.T) {
	base := geoSnapshot(intstr.FromInt32(8083), true)
	appOnly := networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "geo-from-frontend", Namespace: namespace},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: geoLabels},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{From: []networkingv1.NetworkPolicyPeer{{
				PodSelector: &metav1.LabelSelector{MatchLabels: frontendLabels},
			}}}},
		},
	}
	base.NetworkPolicyList = []networkingv1.NetworkPolicy{appOnly}
	blocked := reachabilitySnapshot{Snapshot: base, window: window(geoTarget(samples(servicehealth.DialTimeout, false, false, false)))}
	if findings := detectReachability(t, blocked); len(findings) != 0 {
		t.Fatalf("a policy that admits only application pods makes the prober's dials no evidence, got %v", findings)
	}

	admitting := base
	admitting.NetworkPolicyList = []networkingv1.NetworkPolicy{*appOnly.DeepCopy()}
	admitting.NetworkPolicyList[0].Spec.Ingress[0].From = append(admitting.NetworkPolicyList[0].Spec.Ingress[0].From,
		networkingv1.NetworkPolicyPeer{NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"kubernetes.io/metadata.name": "sdo-control"},
		}})
	findings := detectReachability(t, reachabilitySnapshot{Snapshot: admitting, window: blocked.window})
	if len(findings) != 1 || !strings.Contains(findings[0].Evidence, "geo-from-frontend") {
		t.Fatalf("a policy that admits the prober keeps the finding and names the policy, got %v", findings)
	}

	otherPort := base
	otherPort.NetworkPolicyList = []networkingv1.NetworkPolicy{*admitting.NetworkPolicyList[0].DeepCopy()}
	port := intstr.FromInt32(9090)
	otherPort.NetworkPolicyList[0].Spec.Ingress[0].Ports = []networkingv1.NetworkPolicyPort{{Port: &port}}
	if findings := detectReachability(t, reachabilitySnapshot{Snapshot: otherPort, window: blocked.window}); len(findings) != 0 {
		t.Fatalf("a policy that admits the prober only on another port excludes this one, got %v", findings)
	}
}

func TestReachabilityReportsAnUnavailableProber(t *testing.T) {
	snapshot := reachabilitySnapshot{
		Snapshot: geoSnapshot(intstr.FromInt32(8083), true),
		window:   &servicehealth.ReachabilityWindow{LoadError: "prober unavailable: connection refused"},
	}
	if _, err := servicehealth.NewReachabilityDetector(reachabilitySpec()).Detect(context.Background(), snapshot); err == nil {
		t.Fatal("an unobservable verdict must be a detector error, not a clear")
	}
	if findings := detectReachability(t, geoSnapshot(intstr.FromInt32(8083), true)); len(findings) != 0 {
		t.Fatalf("a context without reachability observations reports nothing, got %v", findings)
	}
}

func TestReachabilityIsDeterministicAndGroupsPortsPerService(t *testing.T) {
	snapshot := geoSnapshot(intstr.FromInt32(8083), true)
	snapshot.ServiceList[0].Spec.Ports = append(snapshot.ServiceList[0].Spec.Ports, corev1.ServicePort{Name: "metrics", Port: 9090})
	snapshot.ServiceList = append(snapshot.ServiceList, service("frontend", frontendLabels))
	snapshot.PodList = append(snapshot.PodList, pod("frontend-7d9", frontendLabels, true))
	success := time.Date(2026, 10, 1, 11, 0, 0, 0, time.UTC)
	frontend := servicehealth.TargetObservations{
		Target: servicehealth.ReachabilityTarget{Service: "frontend", Port: 5000}, Samples: samples(servicehealth.DialTimeout, false, false, false),
		LastSuccess: &success,
	}
	metrics := servicehealth.TargetObservations{
		Target: servicehealth.ReachabilityTarget{Service: "geo", Port: 9090}, Samples: samples(servicehealth.DialRefused, false, false, false),
	}
	first := detectReachability(t, reachabilitySnapshot{Snapshot: snapshot, window: window(metrics, frontend, geoTarget(samples(servicehealth.DialRefused, false, false, false)))})
	second := detectReachability(t, reachabilitySnapshot{Snapshot: snapshot, window: window(geoTarget(samples(servicehealth.DialRefused, false, false, false)), frontend, metrics)})
	encodedFirst, _ := json.Marshal(first)
	encodedSecond, _ := json.Marshal(second)
	if string(encodedFirst) != string(encodedSecond) {
		t.Fatalf("findings must not depend on observation order:\n%s\n%s", encodedFirst, encodedSecond)
	}
	if len(first) != 2 || first[0].PrimaryResource.Name != "frontend" || first[1].PrimaryResource.Name != "geo" {
		t.Fatalf("expected one finding per Service sorted by name, got %v", first)
	}
	if !strings.Contains(first[0].Evidence, success.Format(time.RFC3339)) {
		t.Fatalf("evidence must carry the last successful dial: %s", first[0].Evidence)
	}
	if ports, _ := first[1].Metadata["ports"].([]map[string]any); len(ports) != 2 {
		t.Fatalf("both failing geo ports belong to one finding, got %v", first[1].Metadata)
	}
}

func shift(sampled []servicehealth.DialSample, by time.Duration) []servicehealth.DialSample {
	for index := range sampled {
		sampled[index].At = sampled[index].At.Add(by)
	}
	return sampled
}

// TestReachabilityCatchingUpNamesTargetsWhoseWindowRestartedOnReadiness covers
// the dial schedule's input: a replaced ready pod restarts the clearing
// window, which needs Failures successful dials after the transition.
func TestReachabilityCatchingUpNamesTargetsWhoseWindowRestartedOnReadiness(t *testing.T) {
	geo := servicehealth.ReachabilityTarget{Service: "geo", Port: 8083, PortName: "grpc"}
	snapshot := geoSnapshot(intstr.FromInt32(8083), true)
	readyAgain := observedAt.Add(-15 * time.Second)
	snapshot.PodList[0].Status.Conditions[0].LastTransitionTime = metav1.NewTime(readyAgain)
	detector := servicehealth.NewReachabilityDetector(reachabilitySpec()).(interface {
		Confirmed(sdk.DetectionContext) bool
	})
	for name, test := range map[string]struct {
		sampled []servicehealth.DialSample
		pending bool
	}{
		"two dials after Ready":             {samples("", true, true, true, true), true},
		"three dials after Ready":           {shift(samples("", true, true, true), 0), false},
		"a failed dial after Ready":         {samples(servicehealth.DialRefused, true, true, true, false), false},
		"no dial yet after Ready":           {shift(samples("", true, true), -20*time.Second), true},
		"only older dials, one failed then": {shift(samples(servicehealth.DialRefused, false), -20*time.Second), true},
	} {
		if name == "three dials after Ready" {
			for index := range test.sampled {
				test.sampled[index].At = readyAgain.Add(time.Duration(index+1) * time.Second)
			}
		}
		view := reachabilitySnapshot{Snapshot: snapshot, window: window(geoTarget(test.sampled))}
		got := servicehealth.ReachabilityCatchingUp(view, *view.window)
		if test.pending != (len(got) == 1 && got[0] == geo) || len(got) > 1 {
			t.Errorf("%s: catching up %v, want pending=%v", name, got, test.pending)
		}
		if test.pending && detector.Confirmed(view) {
			t.Errorf("%s: a target still catching up must leave the window unconfirmed", name)
		}
	}
	unready := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), false), window: window(geoTarget(nil))}
	if got := servicehealth.ReachabilityCatchingUp(unready, *unready.window); len(got) != 0 {
		t.Fatalf("a Service without a ready backend has no window to fill, got %v", got)
	}
}
