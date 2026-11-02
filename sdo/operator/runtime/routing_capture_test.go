package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/testprober"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type routingTestProber struct {
	mu        sync.Mutex
	calls     []string
	deadlines []time.Duration
}

func (p *routingTestProber) Probe(ctx context.Context, ref sdk.ObjectRef, port int32, _ *program.HTTPProbe) (program.ProbeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, fmt.Sprintf("%s/%s:%d", ref.Kind, ref.Name, port))
	if deadline, ok := ctx.Deadline(); ok {
		// Unit callers run inside synctest; process conformance callers retain host time.
		p.deadlines = append(p.deadlines, deadline.Sub(timerClock(nil).Now()))
	}
	return program.ProbeResult{Success: false, Detail: "refused"}, nil
}

func (p *routingTestProber) ProbePod(ctx context.Context, ref sdk.ObjectRef, ip string, port int32) (program.ProbeResult, error) {
	wanted := "10.0.0.5"
	if ref.Name == "peer-pod" {
		wanted = "10.0.0.6"
	}
	if ip != wanted {
		return program.ProbeResult{}, fmt.Errorf("wrong captured PodIP %s", ip)
	}
	result, err := p.Probe(ctx, ref, port, nil)
	result.Success = port == 8080
	return result, err
}

func TestLiveReadViewConcurrentPodTCPProbes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		probe := &routingTestProber{}
		view := LiveReadView{Carrier: inputs.New(routingCaptureSnapshot()), Prober: probe}
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
		start := make(chan struct{})
		var workers sync.WaitGroup
		for range 32 {
			workers.Go(func() {
				<-start
				result, err := view.ProbeTCP(ref, 8080)
				if err != nil || !result.Success {
					t.Errorf("concurrent Pod probe: %+v %v", result, err)
				}
			})
		}
		close(start)
		workers.Wait()
		if len(probe.calls) != 32 || len(probe.deadlines) != 32 {
			t.Fatalf("lost probe observations: calls=%d deadlines=%d", len(probe.calls), len(probe.deadlines))
		}
	})
}

func routingCaptureSnapshot() sdktest.Snapshot {
	return sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api-pod", Namespace: "app", ResourceVersion: "fixture", Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}}}}, Status: corev1.PodStatus{PodIP: "10.0.0.5", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}}}
}

func TestLiveReadViewPodTCPUsesCapturedAddress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		probe := &routingTestProber{}
		view := LiveReadView{Carrier: inputs.New(routingCaptureSnapshot()), Prober: probe}
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
		result, err := view.ProbeTCP(ref, 8080)
		if err != nil || !result.Success {
			t.Fatalf("pod listener observation %+v %v", result, err)
		}
		ref.Namespace = "elsewhere"
		if _, err := view.ProbeTCP(ref, 8080); err == nil {
			t.Fatal("out-of-namespace Pod probed")
		}
		ref.Namespace = "app"
		ref.Name = "absent"
		if _, err := view.ProbeTCP(ref, 8080); err == nil {
			t.Fatal("missing Pod probed")
		}
	})
}

func TestIncidentCaptureRecordsRoutingTCPBeforeAnyProgramExists(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		snapshot := routingCaptureSnapshot()
		probe := &routingTestProber{}
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
		loop := NewResponderLoop(IncidentRequest{IncidentID: "routing", Namespace: "app", Findings: []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: ref}}}, nil, &changingProvider{snapshot: snapshot}, nil, PlanExecutor{}, 1)
		loop.CorpusSessionRoot = t.TempDir()
		loop.ReadView = func(s sdk.DetectionContext) program.ReadView {
			return LiveReadView{Carrier: inputs.New(s), Prober: probe}
		}
		// The controller owns the sweep: the responder's own diagnosis never probes.
		loop.InitialDiagnose(ctx)
		if len(probe.calls) != 0 {
			t.Fatalf("responder diagnosis probed: %+v", probe.calls)
		}
		capture := func() {
			if err := captureRoutingEvidence(ctx, loop.CorpusSessionRoot, "routing", "open@detect", loop.Request.Findings, loop.ReadView(snapshot)); err != nil {
				t.Fatal(err)
			}
		}
		capture()
		dir, err := memory.IncidentCorpusDir(loop.CorpusSessionRoot, "routing")
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
		if err != nil {
			t.Fatal(err)
		}
		var calls []sdktest.PrimitiveCall
		if err := json.Unmarshal(data, &calls); err != nil {
			t.Fatal(err)
		}
		view := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
		pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
		for _, port := range []int32{8080, 9999} {
			result, err := view.ProbeTCP(pod, port)
			if err != nil || result.Success != (port == 8080) {
				t.Fatalf("captured port %d: %+v %v; calls=%+v", port, result, err, calls)
			}
		}
		if _, err := view.ProbeTCP(pod, 12345); err == nil {
			t.Fatal("unrecorded probe invented")
		}
		if len(probe.calls) == 0 || len(probe.calls) > 24 {
			t.Fatalf("unbounded or missing capture: %+v", probe.calls)
		}
		for _, deadline := range probe.deadlines {
			if deadline < time.Second || deadline > routingCaptureProbeTimeout {
				t.Fatalf("routing probe deadline %s must be generous yet bounded by %s", deadline, routingCaptureProbeTimeout)
			}
		}
		prior := len(probe.calls)
		loop.InitialDiagnose(ctx)
		capture()
		if len(probe.calls) != prior {
			t.Fatal("repeated detect capture probed again")
		}
	})
}

// incidentOneSnapshot has the shape of a co-injected fault: only Service/user
// is named by findings, while reservation routes 8087 to a port no Pod
// listens on. consul and frontend are alphabetically-first healthy peers.
func incidentOneSnapshot() sdktest.Snapshot {
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	add := func(name string, port, targetPort, containerPort int32, ready bool) {
		snapshot.ServiceList = append(snapshot.ServiceList, corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": name}, Ports: []corev1.ServicePort{{Port: port, TargetPort: intstr.FromInt32(targetPort)}}}})
		status := corev1.ConditionFalse
		if ready {
			status = corev1.ConditionTrue
		}
		snapshot.PodList = append(snapshot.PodList, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-pod", Namespace: "app", Labels: map[string]string{"app": name}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Ports: []corev1.ContainerPort{{ContainerPort: containerPort}}}}}, Status: corev1.PodStatus{PodIP: "10.0.0." + fmt.Sprint(len(snapshot.PodList)+1), Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}}})
	}
	add("consul", 8500, 8500, 8500, true)
	add("frontend", 5000, 5000, 5000, true)
	add("reservation", 8087, 9999, 8087, true)
	add("user", 8086, 8086, 8086, false)
	return snapshot
}

func serviceRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: name}
}

func TestRoutingCaptureProbesSuspectServiceNamedByNoFinding(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapshot := incidentOneSnapshot()
		findings := []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: serviceRef("user")}}
		root := t.TempDir()
		if err := memory.CaptureIncidentSnapshot(root, "incident-one", "open@detect", snapshot, "app"); err != nil {
			t.Fatal(err)
		}
		probe := &boundedRoutingProber{}
		if err := captureRoutingEvidence(context.Background(), root, "incident-one", "open@detect", findings, LiveReadView{Carrier: inputs.New(snapshot), Prober: probe}); err != nil {
			t.Fatal(err)
		}
		dir, err := memory.IncidentCorpusDir(root, "incident-one")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
		if err != nil {
			t.Fatal(err)
		}
		var calls []sdktest.PrimitiveCall
		if err := json.Unmarshal(raw, &calls); err != nil {
			t.Fatal(err)
		}
		replay := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
		pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "reservation-pod"}
		for _, observed := range []struct {
			ref  sdk.ObjectRef
			port int32
		}{{serviceRef("reservation"), 8087}, {serviceRef("reservation"), 9999}, {pod, 8087}, {pod, 9999}} {
			if _, err := replay.ProbeTCP(observed.ref, observed.port); err != nil {
				t.Errorf("no replayable %s/%s:%d observation: %v", observed.ref.Kind, observed.ref.Name, observed.port, err)
			}
		}
	})
}

func TestRoutingProbeTargetsPrioritizeFindingsAndSuspectsOverPeers(t *testing.T) {
	snapshot := incidentOneSnapshot()
	names := func(targets []routingProbeTarget) []string {
		out := []string{}
		for _, target := range targets {
			name := strings.TrimSuffix(target.ref.Name, "-pod")
			if len(out) == 0 || out[len(out)-1] != name {
				out = append(out, name)
			}
		}
		return out
	}
	t.Run("finding-named service then suspect then peers", func(t *testing.T) {
		got := names(routingProbeTargets(snapshot, []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: serviceRef("user")}}))
		want := []string{"user", "reservation", "consul", "frontend"}
		if !slices.Equal(got, want) {
			t.Fatalf("probe order %v, want %v", got, want)
		}
	})
	t.Run("drift binding without a health finding", func(t *testing.T) {
		got := names(routingProbeTargets(snapshot, []sdk.Finding{{Status: sdk.FindingActive, ParameterBindings: map[string]sdk.ObjectRef{"resource": serviceRef("reservation")}}}))
		if len(got) < 2 || got[0] != "reservation" || got[1] != "user" {
			t.Fatalf("probe order %v: drift-named reservation first, suspect user next", got)
		}
	})
	t.Run("healthy namespace without findings probes nothing", func(t *testing.T) {
		healthy := incidentOneSnapshot()
		healthy.ServiceList[2].Spec.Ports[0].TargetPort = intstr.FromInt32(8087)
		healthy.PodList[3].Status.Conditions[0].Status = corev1.ConditionTrue
		if targets := routingProbeTargets(healthy, nil); len(targets) != 0 {
			t.Fatalf("healthy snapshot probed: %+v", targets)
		}
	})
}

func TestRoutingCaptureHasCountAndTimeBudgets(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(fmt.Sprint(blocked), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				snapshot := routingCaptureSnapshot()
				base := snapshot.ServiceList[0]
				snapshot.ServiceList = nil
				findings := []sdk.Finding{}
				for i := 0; i < 40; i++ {
					service := *base.DeepCopy()
					service.Name = fmt.Sprintf("api-%02d", i)
					snapshot.ServiceList = append(snapshot.ServiceList, service)
					findings = append(findings, sdk.Finding{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: service.Name}})
				}
				probe := &boundedRoutingProber{blocked: blocked}
				root := t.TempDir()
				if err := memory.CaptureIncidentSnapshot(root, "bounded", "open@detect", snapshot, "app"); err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				err := captureRoutingEvidence(context.Background(), root, "bounded", "open@detect", findings, LiveReadView{Carrier: inputs.New(snapshot), Prober: probe})
				if err != nil {
					t.Fatal(err)
				}
				if probe.calls == 0 || probe.calls > maxRoutingCaptureProbes || time.Since(start) > routingCaptureBudget {
					t.Fatalf("capture exceeded bounds: calls=%d duration=%s", probe.calls, time.Since(start))
				}
			})
		})
	}
}

// Probes skipped for lack of budget are missing evidence, not a complete sample.
func TestRoutingCaptureNotesProbesSkippedForBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapshot := routingCaptureSnapshot()
		base := snapshot.ServiceList[0]
		snapshot.ServiceList = nil
		findings := []sdk.Finding{}
		for i := 0; i < 10; i++ {
			service := *base.DeepCopy()
			service.Name = fmt.Sprintf("api-%02d", i)
			snapshot.ServiceList = append(snapshot.ServiceList, service)
			findings = append(findings, sdk.Finding{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: service.Name}})
		}
		probe := &boundedRoutingProber{delay: routingCaptureBudget / 4}
		root := t.TempDir()
		if err := memory.CaptureIncidentSnapshot(root, "skipped", "open@detect", snapshot, "app"); err != nil {
			t.Fatal(err)
		}
		if err := captureRoutingEvidence(context.Background(), root, "skipped", "open@detect", findings, LiveReadView{Carrier: inputs.New(snapshot), Prober: probe}); err != nil {
			t.Fatal(err)
		}
		dir, err := memory.IncidentCorpusDir(root, "skipped")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "routing-probe-note.json"))
		if err != nil {
			t.Fatalf("budget-truncated capture left no note: %v", err)
		}
		var note routingCaptureNote
		if err := json.Unmarshal(raw, &note); err != nil || note.Reason != "budget_exhausted" {
			t.Fatalf("wrong note %s %v", raw, err)
		}
	})
}

type boundedRoutingProber struct {
	mu      sync.Mutex
	blocked bool
	delay   time.Duration
	calls   int
}

func (p *boundedRoutingProber) Probe(ctx context.Context, _ sdk.ObjectRef, _ int32, _ *program.HTTPProbe) (program.ProbeResult, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.blocked {
		<-ctx.Done()
		return program.ProbeResult{}, ctx.Err()
	}
	timerClock(nil).Sleep(p.delay)
	return program.ProbeResult{Success: true}, nil
}

func (p *boundedRoutingProber) ProbePod(ctx context.Context, ref sdk.ObjectRef, _ string, port int32) (program.ProbeResult, error) {
	return p.Probe(ctx, ref, port, nil)
}

func TestRoutingCaptureRecordsUnavailableCapabilityOnce(t *testing.T) {
	unavailable := HTTPProberClient{BaseURL: func(context.Context, bool) (string, error) {
		return "", errors.New("prober pod is not ready")
	}}
	for _, test := range []struct {
		name   string
		view   func(sdk.DetectionContext) program.ReadView
		reason string
	}{
		{"missing", func(s sdk.DetectionContext) program.ReadView { return LiveReadView{Carrier: inputs.New(s)} }, "prober_not_configured"},
		{"non-live", func(s sdk.DetectionContext) program.ReadView {
			return &sdktest.ReplayReadView{Snapshot: s.(sdktest.Snapshot)}
		}, "read_view_not_live"},
		{"unreachable", func(s sdk.DetectionContext) program.ReadView {
			return LiveReadView{Carrier: inputs.New(s), Prober: unavailable}
		}, "probe_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := routingCaptureSnapshot()
			root := t.TempDir()
			findings := []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}
			loop := NewResponderLoop(IncidentRequest{IncidentID: "no-prober", Namespace: "app", Findings: findings}, nil, &changingProvider{snapshot: snapshot}, nil, PlanExecutor{}, 1)
			loop.CorpusSessionRoot, loop.ReadView = root, test.view
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			defer slog.SetDefault(previous)
			loop.InitialDiagnose(context.Background())
			for i := 0; i < 2; i++ {
				if err := captureRoutingEvidence(context.Background(), root, "no-prober", "open@detect", findings, test.view(snapshot)); err != nil {
					t.Fatal(err)
				}
			}
			// A later capture phase must preserve and not log the same gap again.
			if err := memory.CaptureIncidentSnapshot(root, "no-prober", "fixed", snapshot, "app"); err != nil {
				t.Fatal(err)
			}
			if err := captureRoutingEvidence(context.Background(), root, "no-prober", "fixed", findings, test.view(snapshot)); err != nil {
				t.Fatal(err)
			}
			dir, err := memory.IncidentCorpusDir(root, "no-prober")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "routing-probe-note.json"))
			if err != nil {
				t.Fatalf("missing typed unavailable capture note: %v", err)
			}
			var note routingCaptureNote
			if err := json.Unmarshal(raw, &note); err != nil {
				t.Fatal(err)
			}
			if note.Capability != "routing_probe" || note.Status != "unavailable" || note.Reason != test.reason || note.Phase != "open@detect" {
				t.Fatalf("wrong capability note: %s", raw)
			}
			if test.reason == "probe_failed" && !strings.Contains(note.Detail, "prober pod is not ready") {
				t.Fatalf("missing failure detail: %s", raw)
			}
			if strings.Count(logs.String(), "routing_probe=unavailable:"+test.reason) != 1 {
				t.Fatalf("capability must be logged once per incident: %s", &logs)
			}
			calls, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
			if err != nil {
				t.Fatal(err)
			}
			var recordings []sdktest.PrimitiveCall
			if err := json.Unmarshal(calls, &recordings); err != nil {
				t.Fatal(err)
			}
			for _, call := range recordings {
				if call.Method == "ProbeTCP" && call.Error == "" {
					t.Fatalf("unavailable probe invented an observation: %+v", call)
				}
			}
		})
	}
}

func TestRoutingCaptureRecordsHangingDialAsFailedObservation(t *testing.T) {
	for _, kind := range []string{"Service", "Pod"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, err := prober.New(prober.Config{Namespace: "app", DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
					if strings.HasSuffix(address, ":9999") {
						if (kind == "Pod") == strings.HasPrefix(address, "10.0.0.5:") {
							<-ctx.Done()
							return nil, &net.OpError{Op: "dial", Net: "tcp", Err: ctx.Err()}
						}
						return nil, errors.New("connection refused")
					}
					left, right := net.Pipe()
					_ = right.Close()
					return left, nil
				}})
				if err != nil {
					t.Fatal(err)
				}
				snapshot := routingCaptureSnapshot()
				root := t.TempDir()
				const incident = "hanging-dial"
				if err := memory.CaptureIncidentSnapshot(root, incident, "open@detect", snapshot, "app"); err != nil {
					t.Fatal(err)
				}
				service := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
				findings := []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: service}}
				view := LiveReadView{Carrier: inputs.New(snapshot), Prober: inProcessProberClient(testprober.Handler(p))}
				if err := captureRoutingEvidence(context.Background(), root, incident, "open@detect", findings, view); err != nil {
					t.Fatal(err)
				}
				dir, err := memory.IncidentCorpusDir(root, incident)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
				if err != nil {
					t.Fatal(err)
				}
				var calls []sdktest.PrimitiveCall
				if err := json.Unmarshal(raw, &calls); err != nil {
					t.Fatal(err)
				}
				if len(calls) != 4 {
					t.Fatalf("lost other routing results: %+v", calls)
				}
				replay := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
				pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
				for _, ref := range []sdk.ObjectRef{pod, service} {
					result, err := replay.ProbeTCP(ref, 9999)
					if err != nil || result.Success || result.Detail == "" {
						t.Fatalf("failed dial must replay as an observation: %+v %v", result, err)
					}
					if ref.Kind == kind && !strings.Contains(result.Detail, "deadline exceeded") {
						t.Fatalf("missing dial timeout detail: %+v", result)
					}
				}
				if result, err := replay.ProbeTCP(pod, 8080); err != nil || !result.Success {
					t.Fatalf("lost successful peer probe: %+v %v", result, err)
				}
				if _, err := os.Stat(filepath.Join(dir, "routing-probe-note.json")); !os.IsNotExist(err) {
					t.Fatalf("dial timeout marked routing unavailable: %v", err)
				}
			})
		})
	}
}

// inProcessProber serves the real prober API through the client's transport, so
// tests exercise the HTTP contract without sockets and run on fake time.
type inProcessProber struct{ handler http.Handler }

func (p inProcessProber) RoundTrip(request *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	p.handler.ServeHTTP(recorder, request)
	return recorder.Result(), nil
}

func (p inProcessProber) Do(request *http.Request) (*http.Response, error) {
	return p.RoundTrip(request)
}

func inProcessProberClient(handler http.Handler) HTTPProberClient {
	return HTTPProberClient{BaseURL: StaticProberURL("http://prober.invalid"), Client: inProcessProber{handler}}
}

// A healthy but CPU-starved prober answers slowly. That is wall-clock noise,
// not routing evidence: the capture must wait for the real answer instead of
// recording the probe as unavailable.
func TestRoutingCaptureWaitsForSlowHealthyProber(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, err := prober.New(prober.Config{Namespace: "app", DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			select {
			case <-time.After(750 * time.Millisecond): // a starved dial that still completes
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if !strings.HasSuffix(address, ":8080") {
				return nil, errors.New("connection refused")
			}
			left, right := net.Pipe()
			_ = right.Close()
			return left, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		snapshot := routingCaptureSnapshot()
		root := t.TempDir()
		const incident = "slow-healthy"
		if err := memory.CaptureIncidentSnapshot(root, incident, "open@detect", snapshot, "app"); err != nil {
			t.Fatal(err)
		}
		findings := []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
		view := LiveReadView{Carrier: inputs.New(snapshot), Prober: inProcessProberClient(testprober.Handler(p))}
		if err := captureRoutingEvidence(context.Background(), root, incident, "open@detect", findings, view); err != nil {
			t.Fatal(err)
		}
		dir, err := memory.IncidentCorpusDir(root, incident)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(dir, "routing-probe-note.json")); !os.IsNotExist(err) {
			note, _ := os.ReadFile(filepath.Join(dir, "routing-probe-note.json"))
			t.Fatalf("slow healthy prober marked routing unavailable: %s", note)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
		if err != nil {
			t.Fatal(err)
		}
		var calls []sdktest.PrimitiveCall
		if err := json.Unmarshal(raw, &calls); err != nil {
			t.Fatal(err)
		}
		replay := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
		pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
		if result, err := replay.ProbeTCP(pod, 8080); err != nil || !result.Success {
			t.Fatalf("slow listener not observed: %+v %v calls=%+v", result, err, calls)
		}
		if result, err := replay.ProbeTCP(pod, 9999); err != nil || result.Success || result.Detail != "connection refused" {
			t.Fatalf("slow refusal must replay as the dial's own answer: %+v %v", result, err)
		}
	})
}

func TestRoutingCaptureContinuesAfterProberTransportFailure(t *testing.T) {
	p, err := prober.New(prober.Config{Namespace: "app", DialContext: func(context.Context, string, string) (net.Conn, error) {
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	api := inProcessProberClient(testprober.Handler(p))
	resolutions := 0
	client := HTTPProberClient{BaseURL: func(context.Context, bool) (string, error) {
		resolutions++
		if resolutions <= 2 { // Both attempts for the first probe fail.
			return "", errors.New("prober unreachable")
		}
		return "http://prober.invalid", nil
	}, Client: api.Client}
	snapshot := routingCaptureSnapshot()
	root := t.TempDir()
	const incident = "transport-failure"
	if err := memory.CaptureIncidentSnapshot(root, incident, "open@detect", snapshot, "app"); err != nil {
		t.Fatal(err)
	}
	findings := []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}
	if err := captureRoutingEvidence(context.Background(), root, incident, "open@detect", findings, LiveReadView{Carrier: inputs.New(snapshot), Prober: client}); err != nil {
		t.Fatal(err)
	}
	dir, err := memory.IncidentCorpusDir(root, incident)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(raw, &calls); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 4 || !strings.Contains(calls[0].Error, "prober unreachable") {
		t.Fatalf("missing transport failure: %+v", calls)
	}
	for _, call := range calls[1:] {
		var result program.ProbeResult
		if err := json.Unmarshal(call.Result, &result); err != nil || call.Error != "" || !result.Success {
			t.Fatalf("transport failure lost later result: %+v %v", call, err)
		}
	}
	raw, err = os.ReadFile(filepath.Join(dir, "routing-probe-note.json"))
	if err != nil || !bytes.Contains(raw, []byte("prober unreachable")) {
		t.Fatalf("missing transport capability note: %s %v", raw, err)
	}
}

// HTTPProberClient calls a prober's HTTP API.
type HTTPProberClient struct {
	// BaseURL resolves the prober's address, for example the pod IP. After a
	// transport error the client asks again with refresh set, so a replaced
	// or deleted prober pod is found or recreated.
	BaseURL func(ctx context.Context, refresh bool) (string, error)
	Client  traffic.Doer
}

// StaticProberURL addresses a prober at a fixed URL.
func StaticProberURL(url string) func(context.Context, bool) (string, error) {
	return func(context.Context, bool) (string, error) { return url, nil }
}

func (c HTTPProberClient) do(ctx context.Context, method string, path string, body any, into any) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	response, err := c.send(ctx, method, path, body, false)
	if err != nil && ctx.Err() == nil {
		response, err = c.send(ctx, method, path, body, true)
	}
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("prober %s %s: HTTP %d: %s", method, path, response.StatusCode, strings.TrimSpace(string(message)))
	}
	if into == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(response.Body, 32<<20)).Decode(into)
}

func (c HTTPProberClient) send(ctx context.Context, method string, path string, body any, refresh bool) (*http.Response, error) {
	base, err := c.BaseURL(ctx, refresh)
	if err != nil {
		return nil, fmt.Errorf("locate prober: %w", err)
	}
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		payload = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(base, "/")+path, payload)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	client := c.Client
	if client == nil {
		return nil, errors.New("prober client transport is required")
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("prober %s %s: %w", method, path, err)
	}
	return response, nil
}

func (c HTTPProberClient) Windows(ctx context.Context) (map[string]traffic.Window, error) {
	var response testprober.WindowsResponse
	if err := c.do(ctx, http.MethodGet, testprober.PathWindows, nil, &response); err != nil {
		return nil, err
	}
	return response.Windows, nil
}

func (c HTTPProberClient) Burst(ctx context.Context, request prober.BurstRequest) (prober.BurstResult, error) {
	var result prober.BurstResult
	err := c.do(ctx, http.MethodPost, testprober.PathBursts, request, &result)
	return result, err
}

func (c HTTPProberClient) Probe(ctx context.Context, ref sdk.ObjectRef, port int32, request *program.HTTPProbe) (program.ProbeResult, error) {
	return c.primitive(ctx, prober.PrimitiveRequest{To: ref, Port: port, HTTP: request})
}

func (c HTTPProberClient) primitive(ctx context.Context, request prober.PrimitiveRequest) (program.ProbeResult, error) {
	if err := ctx.Err(); err != nil {
		return program.ProbeResult{}, err
	}
	// Leave half the caller's remaining time for locating the prober and the
	// HTTP round trip, so a hanging probe returns an observation before the
	// client cancels its request. HTTP does not carry context deadlines.
	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, deadline.Sub(timerClock(nil).Now())/2)
	}
	if timeout <= 0 {
		return program.ProbeResult{}, context.DeadlineExceeded
	}
	request.Timeout = timeout
	var result program.ProbeResult
	err := c.do(ctx, http.MethodPost, testprober.PathPrimitive, request, &result)
	return result, err
}

func (c HTTPProberClient) Reset(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, testprober.PathReset, map[string]string{}, nil)
}

// DialTargets runs one round of reachability dials in the prober.
func (c HTTPProberClient) DialTargets(ctx context.Context, request prober.DialRequest) (prober.DialResponse, error) {
	var response prober.DialResponse
	err := c.do(ctx, http.MethodPost, testprober.PathDials, request, &response)
	return response, err
}

func (c HTTPProberClient) ProbePod(ctx context.Context, ref sdk.ObjectRef, ip string, port int32) (program.ProbeResult, error) {
	return c.primitive(ctx, prober.PrimitiveRequest{To: ref, Port: port, PodIP: ip})
}
