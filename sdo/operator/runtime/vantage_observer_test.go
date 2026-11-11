package runtime

import (
	"context"
	"io"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/vantage"
)

type vantageStateDetector struct{ probe vantage.Probe }

func (d vantageStateDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "process-runtime", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Minute, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}}
}
func (d vantageStateDetector) VantageProbes() []vantage.Probe { return []vantage.Probe{d.probe} }
func (vantageStateDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

func vantagePod(ephemeral ...corev1.ContainerStatus) corev1.Pod {
	pod := runningStorePod("a")
	pod.Status.EphemeralContainerStatuses = ephemeral
	return pod
}

func TestVantageObserverAttachesAHelperThenProbesThroughIt(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	probe := vantage.Probe{ID: "limits", Kind: vantage.TCPConnect, From: podread.Target{Image: "synthetic-store"}, To: &vantage.Peer{Host: "db", Port: 5432}, Interval: time.Minute}
	pod := vantagePod()
	client := fake.NewSimpleClientset(pod.DeepCopy())
	var execContainer string
	exec := &scriptedPodExec{answer: func(_ context.Context, _ sdk.ObjectRef, container string, stdout io.Writer) error {
		execContainer = container
		_, err := io.WriteString(stdout, "dial 0 999 0 999 2000 \npsi 3.00\n")
		return err
	}}
	state := vantage.Probe{ID: "state", Kind: vantage.TCPConnect, From: probe.From, To: &vantage.Peer{Host: "cache", Port: 6379}, Interval: time.Minute}
	observer, err := NewVantageObserver(client, exec, "", "", []sdk.Detector{vantageStateDetector{probe}, vantageStateDetector{state}}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	observer.get = func(_ context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
		object := &unstructured.Unstructured{}
		object.SetUID(types.UID("uid-" + ref.Name))
		return object, nil
	}
	first := observer.collect(context.Background(), sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{pod}})["limits"]
	if len(first.Readings) != 1 || first.Readings[0].Error != errVantageHelperStarting.Error() || exec.calls.Load() != 0 {
		t.Fatalf("first cycle must attach the helper and report it starting: %+v", first)
	}
	stored, err := client.CoreV1().Pods("app").Get(context.Background(), "a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Spec.EphemeralContainers) != 1 {
		t.Fatalf("two probes of one Pod must attach one helper: %+v", stored.Spec.EphemeralContainers)
	}
	helper := stored.Spec.EphemeralContainers[0]
	if helper.Name != "sdo-vantage-store" || helper.TargetContainerName != "store" || helper.Image != DefaultVantageImage {
		t.Fatalf("helper = %+v", helper)
	}

	running := vantagePod(corev1.ContainerStatus{Name: "sdo-vantage-store", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
	second := observer.collect(context.Background(), sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{running}})["limits"]
	if len(second.Readings) != 1 || second.Readings[0].Error != "" || execContainer != "sdo-vantage-store" {
		t.Fatalf("second cycle = %+v (exec in %q)", second, execContainer)
	}
	if dials := second.Readings[0].Dials; len(dials) != 1 || !dials[0].OK || dials[0].Duration != 2*time.Millisecond || dials[0].HostCPUStall != 3 {
		t.Fatalf("dials = %+v", dials)
	}
}

func TestVantageHelperThatExitedIsReplacedUnderANewName(t *testing.T) {
	pod := vantagePod(corev1.ContainerStatus{Name: "sdo-vantage-store", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137}}})
	pod.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "sdo-vantage-store"}}}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	observer := &VantageObserver{client: client, image: DefaultVantageImage}
	if _, err := observer.helper(context.Background(), pod, "store"); err != errVantageHelperStarting {
		t.Fatalf("err = %v", err)
	}
	stored, _ := client.CoreV1().Pods("app").Get(context.Background(), "a", metav1.GetOptions{})
	if len(stored.Spec.EphemeralContainers) != 2 || stored.Spec.EphemeralContainers[1].Name != "sdo-vantage-store-2" {
		t.Fatalf("ephemeral containers = %+v", stored.Spec.EphemeralContainers)
	}
}

func TestVantageBaselineSettlesThenAttaches(t *testing.T) {
	probe := vantage.Probe{ID: "limits", Kind: vantage.TCPConnect, From: podread.Target{Image: "x"}, To: &vantage.Peer{Host: "db", Port: 5432}, Interval: time.Minute}
	healthy := vantageResults{"limits": {Probe: probe, Readings: []vantage.Reading{{Pod: "a", Kind: vantage.TCPConnect, Dials: []vantage.Dial{{At: time.Unix(990, 0), Duration: time.Millisecond, OK: true}}}}}}
	baseline := NewVantageBaseline(time.Minute)
	now := time.Unix(1000, 0)
	baseline.observe(now, true, sdktest.Snapshot{NamespaceName: "app", Vantage: healthy})
	baseline.observe(now.Add(2*time.Minute), true, sdktest.Snapshot{NamespaceName: "app", Vantage: healthy})
	current := map[string]vantage.Result{"limits": {Probe: probe, Readings: []vantage.Reading{{Pod: "a", Kind: vantage.TCPConnect}}}}
	baseline.attach(current)
	if len(current["limits"].Baseline) != 1 {
		t.Fatalf("baseline not attached: %+v", current)
	}
}
