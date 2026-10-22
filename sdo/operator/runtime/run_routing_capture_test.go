package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// The real prober HTTP API has no generators or workloads. Only its network
// edge is replaced: our hand-written app listens on 8080 and refuses other ports.
func noTrafficRoutingProber(t *testing.T) HTTPProberClient {
	t.Helper()
	p, err := prober.New(prober.Config{Namespace: "app", DialContext: func(_ context.Context, _, address string) (net.Conn, error) {
		if !strings.HasSuffix(address, ":8080") {
			return nil, errors.New("connection refused")
		}
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(prober.Handler(p))
	t.Cleanup(api.Close)
	return HTTPProberClient{BaseURL: StaticProberURL(api.URL)}
}

type routingCaptureDispatcher struct{ requests chan IncidentRequest }

func (d routingCaptureDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	<-ctx.Done()
	return IncidentResult{}, ctx.Err()
}

func TestRunCapturesRoutingWithoutTrafficWorkloads(t *testing.T) {
	for _, synthetic := range []bool{true, false} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[synthetic], func(t *testing.T) {
			probe := noTrafficRoutingProber(t)
			address, err := probe.BaseURL(context.Background(), false)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := routingCaptureSnapshot()
			client := fake.NewSimpleClientset(&snapshot.ServiceList[0], &snapshot.PodList[0])
			finding := sdk.Finding{RuleID: "routing", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
				PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Summary: "routing unavailable", Evidence: "connection refused"}
			detector := controllerDetector("health", 20*time.Millisecond, finding)
			detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
			detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
			detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
			detector.spec.OriginatingCommit = "health-objective"
			if len(TrafficWorkloadNames([]sdk.Detector{detector})) != 0 {
				t.Fatal("fixture must have no traffic consumers")
			}
			root := gitRepository(t)
			sessionRoot := t.TempDir()
			repository, err := broker.New(root, t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			ledgerRoot := t.TempDir()
			launch := &contracts.ControllerConfig{Namespace: "app", ControlNamespace: "app-sdo", AppRoot: root,
				Application: "routing", ProberUrl: address, SyntheticTraffic: proto.Bool(synthetic), StateBaseline: proto.Bool(false),
				CleanResponderHelpers: proto.Bool(false), LeaseDuration: durationpb.New(3 * time.Second)}
			dispatcher := routingCaptureDispatcher{requests: make(chan IncidentRequest, 1)}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			exited := make(chan error, 1)
			go func() {
				exited <- RunWithOptions(ctx, []sdk.Detector{detector}, RuntimeOptions{
					Args: controllerConfigToArgs(launch), Client: client, Dispatcher: dispatcher, Stdout: io.Discard, Stderr: io.Discard,
					NativeBroker: &NativeIncidentBroker{Repository: repository, LedgerRoot: ledgerRoot, SessionRoot: sessionRoot},
				})
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-exited:
				case <-time.After(5 * time.Second):
					t.Error("controller did not stop")
				}
			})
			var request IncidentRequest
			select {
			case request = <-dispatcher.requests:
			case err := <-exited:
				exited <- err
				t.Fatalf("controller exited before capture: %v", err)
			case <-ctx.Done():
				t.Fatal("controller did not dispatch")
			}
			dir, err := memory.IncidentCorpusDir(sessionRoot, request.IncidentID)
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
			result, err := replay.ProbeTCP(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}, 8080)
			if err != nil || !result.Success {
				t.Fatalf("no-traffic generated config lost routing observations: %+v %v; calls=%s", result, err, raw)
			}
			result, err = replay.ProbeTCP(finding.PrimaryResource, 80)
			if err != nil || result.Success {
				t.Fatalf("finding Service observation missing: %+v %v", result, err)
			}
		})
	}
}

func TestRunStartsRoutingProberWithoutTrafficWorkloads(t *testing.T) {
	client := fake.NewSimpleClientset()
	podConfig := proberPodConfig(t, client)
	podConfig.AppNamespace = "app"
	_, pod, err := podConfig.Manifests()
	if err != nil {
		t.Fatal(err)
	}
	// Only the fake API marks it ready: no process, cluster or image is started.
	client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
		created := action.(ktesting.CreateAction).GetObject().(*corev1.Pod)
		created.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.7", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		return false, nil, nil
	})
	config := &contracts.ControllerConfig{Namespace: "app", ControlNamespace: pod.Namespace, AppRoot: gitRepository(t),
		ProberBinary: podConfig.Binary, ProberImage: podConfig.Image, RepositoryPvc: podConfig.RepositoryPVC,
		RepositoryMountPath: podConfig.RepositoryMountPath, StateBaseline: proto.Bool(false), CleanResponderHelpers: proto.Bool(false),
		Duration: durationpb.New(2 * time.Second)}
	if err := RunWithOptions(context.Background(), nil, RuntimeOptions{Args: controllerConfigToArgs(config), Client: client,
		Dispatcher: routingCaptureDispatcher{}, Stdout: io.Discard, Stderr: io.Discard}); err != nil {
		t.Fatal(err)
	}
	actual, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), ProberName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("no-traffic startup did not deploy isolated prober: %v", err)
	}
	if actual.Labels[proberFingerprintLabel] != pod.Labels[proberFingerprintLabel] {
		t.Fatalf("wrong prober deployed: %+v", actual)
	}
	if _, err := client.NetworkingV1().NetworkPolicies(pod.Namespace).Get(context.Background(), ProberName, metav1.GetOptions{}); err != nil {
		t.Fatalf("prober deployed without isolation: %v", err)
	}
}
