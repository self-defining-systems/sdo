package localize_test

import (
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/localize"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// A synthetic three-journey application: a gateway serves a landing page and
// two reads that share a storage tier and each have their own handler and cache.
var paths = map[string][]string{
	"landing": {"gateway"},
	"read-a":  {"gateway", "handler-a", "cache-a", "storage", "storage-db"},
	"read-b":  {"gateway", "handler-b", "cache-b", "storage", "storage-db"},
}

var now = time.Unix(1000, 0)

func window(failing ...string) traffic.Window {
	w := traffic.Window{Workload: traffic.Workload{Name: "health"}, ObservedAt: now}
	ids := []string{"landing", "read-a", "read-b"}
	for _, id := range ids {
		observed := traffic.ScenarioObservations{Qualified: true, Scenario: traffic.Descriptor{ID: id, Target: traffic.Target{Service: paths[id][0]}, DependsOn: paths[id][1:]}}
		outcome := traffic.OutcomeOK
		if slices.Contains(failing, id) {
			outcome = traffic.OutcomeTimeout
		}
		for i := 0; i < 20; i++ {
			observed.Samples = append(observed.Samples, traffic.Sample{At: now.Add(-time.Duration(20-i) * time.Second), Outcome: outcome, Latency: 10 * time.Millisecond})
		}
		w.Scenarios = append(w.Scenarios, observed)
	}
	return w
}

func snapshot() sdktest.Snapshot {
	s := sdktest.Snapshot{NamespaceName: "app"}
	for _, name := range []string{"gateway", "handler-a", "cache-a", "handler-b", "cache-b", "storage", "storage-db"} {
		labels := map[string]string{"app": name}
		s.ServiceList = append(s.ServiceList, corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: labels}})
		template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}
		if name == "storage-db" {
			s.StatefulSetList = append(s.StatefulSetList, appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: appsv1.StatefulSetSpec{Template: template}})
			continue
		}
		s.DeploymentList = append(s.DeploymentList, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: template}})
	}
	return s
}

func unreachable(service string) servicehealth.ReachabilityWindow {
	w := servicehealth.ReachabilityWindow{ObservedAt: now, Failures: 3}
	target := servicehealth.TargetObservations{Target: servicehealth.ReachabilityTarget{Service: service, Port: 80}}
	for i := 0; i < 4; i++ {
		target.Samples = append(target.Samples, servicehealth.DialSample{At: now, Class: servicehealth.DialTimeout})
	}
	w.Targets = append(w.Targets, target)
	return w
}

func suspects(r localize.Result) []string {
	var names []string
	for _, c := range r.Suspects() {
		names = append(names, c.Service)
	}
	return names
}

func TestSuspectsFollowTheFailingJourneys(t *testing.T) {
	for _, tc := range []struct {
		name         string
		failing      []string
		reachability servicehealth.ReachabilityWindow
		want         []string
	}{
		{"one journey fails: its own components", []string{"read-a"}, servicehealth.ReachabilityWindow{}, []string{"cache-a", "handler-a"}},
		{"both reads fail: the shared tier", []string{"read-a", "read-b"}, servicehealth.ReachabilityWindow{}, []string{"storage", "storage-db"}},
		{"dial failures narrow the shared tier", []string{"read-a", "read-b"}, unreachable("storage"), []string{"storage"}},
		{"everything fails: the gateway is on every path", []string{"landing", "read-a", "read-b"}, servicehealth.ReachabilityWindow{}, []string{"gateway"}},
		{"nothing fails: no suspects", nil, servicehealth.ReachabilityWindow{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := suspects(localize.Localize(window(tc.failing...), tc.reachability, snapshot()))
			if !slices.Equal(got, tc.want) {
				t.Fatalf("suspects = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUnevaluatedAndUnqualifiedScenariosAreNotHealthyEvidence(t *testing.T) {
	w := window("read-a")
	for i := range w.Scenarios {
		if w.Scenarios[i].Scenario.ID == "read-b" {
			w.Scenarios[i].Qualified = false
		}
		if w.Scenarios[i].Scenario.ID == "landing" {
			w.Scenarios[i].Samples = w.Scenarios[i].Samples[:1]
		}
	}
	r := localize.Localize(w, servicehealth.ReachabilityWindow{}, snapshot())
	if len(r.Healthy) != 0 {
		t.Fatalf("healthy = %v, want none", r.Healthy)
	}
	// With no healthy path to subtract, every component of the failing path ties.
	if got := suspects(r); len(got) != len(paths["read-a"]) {
		t.Fatalf("suspects = %v, want the whole read-a path", got)
	}
}

func TestImplicatesAndBindSuspectsUseBackingWorkloads(t *testing.T) {
	r := localize.Localize(window("read-a", "read-b"), servicehealth.ReachabilityWindow{}, snapshot())
	db := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "storage-db"}
	if ok, why := r.Implicates(db); !ok {
		t.Fatalf("storage-db not implicated: %s", why)
	}
	if ok, why := r.Implicates(sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "handler-a"}); ok || why == "" {
		t.Fatalf("handler-a implicated (%s), want not and a reason", why)
	}
	finding := sdk.Finding{RuleID: "stall", Status: sdk.FindingActive, Fingerprint: "stall", Playbooks: []string{"workload-restart"}}
	bound := localize.BindSuspects(finding, r, "workload")
	var targets []string
	for _, f := range bound {
		targets = append(targets, f.ParameterBindings["workload"].Kind+"/"+f.ParameterBindings["workload"].Name+"="+f.Fingerprint)
	}
	want := []string{"Deployment/storage=stall/Deployment/storage", "StatefulSet/storage-db=stall/StatefulSet/storage-db"}
	if !slices.Equal(targets, want) {
		t.Fatalf("bound = %v, want %v", targets, want)
	}
}

// A detector embedding Inputs asks the controller for the observations, and
// FromContext reports a missing window as unavailable evidence.
func TestFromContextNeedsTheTrafficWindow(t *testing.T) {
	var inputs any = localize.Inputs{}
	if consumer, ok := inputs.(traffic.Consumer); !ok || !slices.Equal(consumer.TrafficWorkloads(), []string{"health"}) {
		t.Fatal("Inputs must declare the health traffic workload")
	}
	if consumer, ok := inputs.(servicehealth.ReachabilityConsumer); !ok || !consumer.ConsumesServiceReachability() {
		t.Fatal("Inputs must consume Service reachability")
	}
	if _, err := localize.FromContext(snapshot(), ""); err != localize.ErrNoTraffic {
		t.Fatalf("err = %v, want ErrNoTraffic", err)
	}
	s := snapshot()
	s.Traffic = map[string]traffic.Window{"health": window("read-a")}
	r, err := localize.FromContext(s, "")
	if err != nil || !slices.Equal(suspects(r), []string{"cache-a", "handler-a"}) {
		t.Fatalf("FromContext = %v, %v", suspects(r), err)
	}
}
