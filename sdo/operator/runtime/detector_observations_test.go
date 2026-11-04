package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/usage"
	"sdo.dev/operator/sdk/workloadhealth"
)

func syntheticAdmissionDetector() sdk.Detector {
	return workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{
		ID: "pod-admission", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Watches:     []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
		Interval:    15 * time.Second, Playbooks: []string{"health-objective"}, OriginatingCommit: "synthetic",
	})
}

func syntheticShopSnapshot() sdktest.Snapshot {
	return sdktest.Snapshot{NamespaceName: "shop", PodList: []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "shop"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "example.invalid/web@sha256:" + strings.Repeat("0", 64)}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}}}
}

func evaluateOnceStatus(t *testing.T, client *fake.Clientset) evaluationResult {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := RunWithOptions(context.Background(), []sdk.Detector{syntheticAdmissionDetector()}, RuntimeOptions{
		Args:     []string{"--evaluate-once", "--namespace", "shop"},
		Provider: staticProvider{snapshot: syntheticShopSnapshot()},
		Client:   client,
		Stdout:   &stdout, Stderr: &stderr,
	})
	if err != nil {
		t.Fatalf("evaluate-once: %v (stderr=%s)", err, stderr.String())
	}
	var result evaluationResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode %q: %v", stdout.String(), err)
	}
	return result
}

// Independent verification must observe what each accepted detector declares,
// exactly as the controller loop does.
func TestEvaluateOnceObservesDeclaredPodAdmission(t *testing.T) {
	t.Run("admitted", func(t *testing.T) {
		if got := evaluateOnceStatus(t, fake.NewSimpleClientset()); got.Status != "clear" {
			t.Fatalf("status = %q, want clear: %+v", got.Status, got)
		}
	})
	t.Run("rejected", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		client.PrependReactor("create", "pods", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", errSyntheticQuota)
		})
		got := evaluateOnceStatus(t, client)
		if got.Status != "firing" || len(got.Findings) != 1 || got.Findings[0].RuleID != workloadhealth.RulePodAdmissionRejected {
			t.Fatalf("result = %+v, want one firing pod-admission-rejected finding", got)
		}
	})
}

var errSyntheticQuota = errorString("exceeded synthetic quota")

type errorString string

func (e errorString) Error() string { return string(e) }

// specOnly hides the admission detector's optional declarations.
type specOnly struct{ spec sdk.DetectorSpec }

func (d specOnly) Spec() sdk.DetectorSpec                                            { return d.spec }
func (specOnly) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) { return nil, nil }

// unusedMemoryRPC satisfies the transport; composing observations never calls it.
type unusedMemoryRPC struct{ MirroredMemoryRPC }

type usageOnlyDetector struct{ specOnly }

func (usageOnlyDetector) UsageRequirements() usage.Requirements { return usage.Requirements{} }

type logsOnlyDetector struct{ specOnly }

func (logsOnlyDetector) LogQueries() []logs.Query {
	return []logs.Query{{ID: "synthetic", Patterns: []logs.Pattern{{ID: "boom", RE2: "boom"}}}}
}

// Every Kubernetes-backed observation a detector declares needs a client; a
// path that cannot supply one must refuse the bundle when it is composed, not
// report the observation unavailable on each evaluation.
func TestDetectorObservationsRequireTheirTransport(t *testing.T) {
	base := syntheticAdmissionDetector()
	plain := specOnly{base.Spec()}
	for observation, local := range map[string]sdk.Detector{
		"pod admission": base,
		"usage":         usageOnlyDetector{plain},
		"logs":          logsOnlyDetector{plain},
	} {
		// The same declaration must reach the controller from accepted memory
		// served by the memory host as from a compiled-in detector.
		server, err := memoryhost.NewServer("synthetic-commit", "synthetic-digest", []sdk.Detector{local})
		if err != nil {
			t.Fatal(err)
		}
		ready, err := server.Ready(context.Background(), &pb.ReadyRequest{})
		if err != nil {
			t.Fatal(err)
		}
		remote, err := NewRemoteMemory(ready, unusedMemoryRPC{})
		if err != nil {
			t.Fatal(err)
		}
		for path, detector := range map[string]sdk.Detector{"compiled": local, "memory host": remote.Detectors[0]} {
			t.Run(observation+"/"+path, func(t *testing.T) {
				_, _, err := (detectorObservations{}).compose(staticProvider{}, []sdk.Detector{detector})
				if err == nil || !strings.Contains(err.Error(), `"pod-admission" declares the `+observation) || !strings.Contains(err.Error(), "Kubernetes client") {
					t.Fatalf("compose without client = %v, want refusal naming the detector, the %s observation and the missing client", err, observation)
				}
			})
		}
	}
	if _, _, err := (detectorObservations{}).compose(staticProvider{}, []sdk.Detector{plain}); err != nil {
		t.Fatalf("a bundle without client-backed declarations needs no client: %v", err)
	}
	provider, _, err := (detectorObservations{Client: fake.NewSimpleClientset()}).compose(staticProvider{snapshot: syntheticShopSnapshot()}, []sdk.Detector{base})
	if err != nil {
		t.Fatal(err)
	}
	view, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := view.PodAdmission(); !ok || !got.Admitted || got.SourcePod != "web-0" {
		t.Fatalf("PodAdmission() = %+v, %v; want admitted from web-0", got, ok)
	}
}
