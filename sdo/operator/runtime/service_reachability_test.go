package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

type mutableSnapshots struct {
	mu       sync.Mutex
	snapshot sdktest.Snapshot
}

func (m *mutableSnapshots) Snapshot(context.Context) (sdk.DetectionContext, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshot, nil
}

func (m *mutableSnapshots) set(snapshot sdktest.Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshot = snapshot
}

type reachabilityDirect struct{ p *prober.Prober }

func (d reachabilityDirect) DialTargets(ctx context.Context, request prober.DialRequest) (prober.DialResponse, error) {
	return d.p.DialTargets(ctx, request)
}

type failingDialer struct{}

func (failingDialer) DialTargets(context.Context, prober.DialRequest) (prober.DialResponse, error) {
	return prober.DialResponse{}, errors.New("connection refused")
}

func reachabilityApp(port int32) sdktest.Snapshot {
	labels := map[string]string{"app": "geo"}
	return sdktest.Snapshot{
		NamespaceName: "shop",
		ServiceList: []corev1.Service{{
			ObjectMeta: metav1.ObjectMeta{Name: "geo", Namespace: "shop"},
			Spec: corev1.ServiceSpec{ClusterIP: "10.96.0.20", Selector: labels, Ports: []corev1.ServicePort{{
				Name: "grpc", Port: port, TargetPort: intstr.FromInt32(port),
			}}},
		}},
		PodList: []corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{Name: "geo-1", Namespace: "shop", Labels: labels},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "geo", Ports: []corev1.ContainerPort{{ContainerPort: port}}}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{
				Type: corev1.PodReady, Status: corev1.ConditionTrue,
			}}},
		}},
	}
}

// TestReachabilityObserverFeedsTheDetectorFromSnapshotDerivedTargets covers
// the controller side: targets come from the namespace snapshot, the prober
// only dials, and the wrapped snapshot carries the samples the generic
// detector judges.
func TestReachabilityObserverFeedsTheDetectorFromSnapshotDerivedTargets(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	var refusing atomic.Bool
	p, err := prober.New(prober.Config{
		Namespace:   "shop",
		LinkAddress: func(link traffic.Link) string { return link.To },
		DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
			if address != "geo" {
				t.Errorf("dialed an underived target %q", address)
			}
			if refusing.Load() {
				return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
			}
			return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshots := &mutableSnapshots{snapshot: reachabilityApp(8083)}
	var notified atomic.Int32
	identity := servicehealth.ProberIdentity{Namespace: "sdo-control", Labels: map[string]string{"app.kubernetes.io/name": "sdo-prober"}}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{
		Prober: reachabilityDirect{p}, Snapshots: snapshots, Notify: func() { notified.Add(1) }, Identity: identity,
	})
	if observer == nil {
		t.Fatal("observer must be created with a prober and snapshots")
	}
	ctx := context.Background()
	if observer.Cycle(ctx) {
		t.Fatal("a healthy round must not wake the controller")
	}
	refusing.Store(true)
	for range 3 {
		if !observer.Cycle(ctx) {
			t.Fatal("a failing round must wake the controller")
		}
	}
	window, ok := observer.Window()
	if !ok || window.Prober.Namespace != "sdo-control" || window.Failures != servicehealth.DefaultReachabilityFailures ||
		len(window.Targets) != 1 || len(window.Targets[0].Samples) != 4 || window.Targets[0].LastSuccess == nil {
		t.Fatalf("window: %+v", window)
	}
	provider := TrafficSnapshotProvider{Base: snapshots, Reachability: observer}
	snapshot, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	detector := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", Interval: 10 * time.Second})
	if !ConsumesServiceReachability([]sdk.Detector{detector}) {
		t.Fatal("the generic detector must be recognized as a reachability consumer")
	}
	findings, err := detector.Detect(ctx, snapshot)
	if err != nil || len(findings) != 1 || findings[0].PrimaryResource.Name != "geo" {
		t.Fatalf("findings %v err %v", findings, err)
	}

	// A Service that disappears is no longer dialed and its history goes.
	snapshots.set(sdktest.Snapshot{NamespaceName: "shop"})
	observer.Cycle(ctx)
	if window, _ := observer.Window(); len(window.Targets) != 0 {
		t.Fatalf("removed Service still observed: %+v", window.Targets)
	}
}

func TestReachabilityObserverReportsAnUnavailableProber(t *testing.T) {
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{
		Prober: failingDialer{}, Snapshots: &mutableSnapshots{snapshot: reachabilityApp(8083)},
	})
	observer.Cycle(context.Background())
	window, ok := observer.Window()
	if !ok || window.LoadError == "" {
		t.Fatalf("an unreadable prober must surface as a load error, got %+v", window)
	}
	if NewServiceReachabilityObserver(ServiceReachabilityConfig{Snapshots: &mutableSnapshots{}}) != nil {
		t.Fatal("without a prober there is no observer")
	}
}

func TestHTTPProberClientDialsTargets(t *testing.T) {
	p, err := prober.New(prober.Config{
		Namespace:   "shop",
		LinkAddress: func(link traffic.Link) string { return link.To },
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			_ = server.Close()
			return client, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(prober.Handler(p))
	defer server.Close()
	client := HTTPProberClient{BaseURL: StaticProberURL(server.URL)}
	response, err := client.DialTargets(context.Background(), prober.DialRequest{
		Targets: []servicehealth.ReachabilityTarget{{Service: "geo", Port: 8083}},
	})
	if err != nil || len(response.Results) != 1 || !response.Results[0].Sample.OK {
		t.Fatalf("response %+v err %v", response, err)
	}
}

// TestEvaluateOnceCollectsReachabilityRounds covers the bounded health
// command: --evaluate-once dials the snapshot-derived targets through the
// prober's /v1/dials endpoint until the gate can be judged, so a refusing
// port is reported and a listening one stays clear.
func TestEvaluateOnceCollectsReachabilityRounds(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refusing := closed.Addr().String()
	_ = closed.Close()
	detector := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{
		ID: "service-reachability", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: servicehealth.ReachabilityWatches(), Interval: 10 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:   []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap",
	})
	for name, test := range map[string]struct {
		address string
		status  DetectorEvaluationStatus
	}{
		"refusing":  {address: refusing, status: DetectorEvaluationFiring},
		"listening": {address: listener.Addr().String(), status: DetectorEvaluationClear},
	} {
		t.Run(name, func(t *testing.T) {
			p, err := prober.New(prober.Config{Namespace: "shop", LinkAddress: func(traffic.Link) string { return test.address }})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(prober.Handler(p))
			defer server.Close()
			var stdout bytes.Buffer
			err = RunWithOptions(context.Background(), []sdk.Detector{detector}, RuntimeOptions{
				Args:     []string{"--evaluate-once", "--namespace", "shop", "--prober-url", server.URL},
				Stdout:   &stdout,
				Provider: staticProvider{snapshot: reachabilityApp(8083)},
			})
			if err != nil {
				t.Fatalf("evaluate once: %v", err)
			}
			var output struct {
				Status   DetectorEvaluationStatus `json:"status"`
				Findings []sdk.Finding            `json:"findings"`
			}
			if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
				t.Fatalf("decode %q: %v", stdout.String(), err)
			}
			if output.Status != test.status {
				t.Fatalf("status %q, want %q: %s", output.Status, test.status, stdout.String())
			}
			if test.status == DetectorEvaluationFiring && (len(output.Findings) != 1 ||
				output.Findings[0].RuleID != servicehealth.RuleServicePortUnreachable || output.Findings[0].PrimaryResource.Name != "geo") {
				t.Fatalf("expected one service-port-unreachable finding for geo, got %+v", output.Findings)
			}
		})
	}
}

func TestEvaluateOnceSkipsReachabilityWithoutAProber(t *testing.T) {
	var stdout, stderr bytes.Buffer
	detector := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", Interval: 10 * time.Second})
	err := RunWithOptions(context.Background(), []sdk.Detector{detector}, RuntimeOptions{
		Args: []string{"--evaluate-once", "--namespace", "shop"}, Stdout: &stdout, Stderr: &stderr,
		Provider: staticProvider{snapshot: reachabilityApp(8083)},
	})
	if err != nil || !bytes.Contains(stdout.Bytes(), []byte(`"status":"clear"`)) ||
		!bytes.Contains(stderr.Bytes(), []byte("service reachability skipped")) {
		t.Fatalf("err %v stdout %q stderr %q", err, stdout.String(), stderr.String())
	}
}
