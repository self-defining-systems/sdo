package runtime

import (
	"context"
	"io"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	probe := vantage.Probe{ID: "limits", Kind: vantage.ProcLimits, From: podread.Target{Image: "synthetic-store"}, Interval: time.Minute}
	pod := vantagePod()
	client := fake.NewSimpleClientset(pod.DeepCopy())
	var execContainer string
	exec := &scriptedPodExec{answer: func(_ context.Context, _ sdk.ObjectRef, container string, stdout io.Writer) error {
		execContainer = container
		_, err := io.WriteString(stdout, "pid 1 store\nMax open files            16                   16                   files\nfds 15\n")
		return err
	}}
	state := vantage.Probe{ID: "state", Kind: vantage.ProcState, From: probe.From, Interval: time.Minute}
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
	if got := second.Readings[0].Facts[vantage.FactNofileSoftMin]; got != "16" {
		t.Fatalf("facts = %v", second.Readings[0].Facts)
	}
	if judged := vantage.Judge(second.Readings[0], vantage.Reading{}); len(judged) != 2 {
		t.Fatalf("judged = %+v", judged)
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
	probe := vantage.Probe{ID: "limits", Kind: vantage.ProcLimits, From: podread.Target{Image: "x"}, Interval: time.Minute}
	healthy := vantageResults{"limits": {Probe: probe, Readings: []vantage.Reading{{Pod: "a", Kind: vantage.ProcLimits, Facts: map[string]string{vantage.FactNofileSoftMin: "1048576"}}}}}
	baseline := NewVantageBaseline(time.Minute)
	now := time.Unix(1000, 0)
	baseline.observe(now, true, sdktest.Snapshot{NamespaceName: "app", Vantage: healthy})
	baseline.observe(now.Add(2*time.Minute), true, sdktest.Snapshot{NamespaceName: "app", Vantage: healthy})
	current := map[string]vantage.Result{"limits": {Probe: probe, Readings: []vantage.Reading{{Pod: "a", Kind: vantage.ProcLimits}}}}
	baseline.attach(current)
	if len(current["limits"].Baseline) != 1 {
		t.Fatalf("baseline not attached: %+v", current)
	}
}

type platformDetector struct{}

func (platformDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "platform", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Minute}
}
func (platformDetector) ConsumesPlatform() bool { return true }
func (platformDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

func TestPlatformSnapshotCarriesPolicyInventoryAndBaseline(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app", Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"}}},
		&corev1.LimitRange{ObjectMeta: metav1.ObjectMeta{Name: "container-limits", Namespace: "app"}, Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{Type: corev1.LimitTypeContainer, Max: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("16Mi")}}}}},
		&corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "compute-quota", Namespace: "app"}, Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("1")}}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "app"}},
	)
	state := NewPlatformState(time.Minute)
	state.SetInventory(func() ([]sdk.ObjectRef, error) {
		return []sdk.ObjectRef{{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}, nil
	})
	observations := detectorObservations{Client: client, Clock: func() time.Time { return now }, Platform: state}
	provider, _, err := observations.compose(fixedViewProvider{sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{platformDetector{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	platform, err := sdk.Platform(view)
	if err != nil {
		t.Fatal(err)
	}
	if limits, err := platform.LimitRanges(); err != nil || len(limits) != 1 {
		t.Fatalf("limit ranges = %+v, %v", limits, err)
	}
	if quotas, err := platform.ResourceQuotas(); err != nil || len(quotas) != 1 {
		t.Fatalf("quotas = %+v, %v", quotas, err)
	}
	if meta, err := platform.NamespaceMeta(); err != nil || meta.Labels["pod-security.kubernetes.io/enforce"] != "restricted" {
		t.Fatalf("namespace = %+v, %v", meta, err)
	}
	if inventory, err := platform.SourceInventory(); err != nil || len(inventory) != 1 {
		t.Fatalf("inventory = %+v, %v", inventory, err)
	}
	if _, err := platform.AdmissionWebhooks(); err != nil {
		t.Fatal(err)
	}
	if _, ok := platform.Changes(); ok {
		t.Fatal("no baseline before a settled healthy stretch")
	}
	state.observe(now, true, view)
	state.observe(now.Add(2*time.Minute), true, view)
	now = now.Add(time.Hour)
	view, err = provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	platform, _ = sdk.Platform(view)
	if changes, ok := platform.Changes(); !ok || len(changes) != 0 {
		t.Fatalf("an unchanged platform against its baseline = %+v, %v", changes, ok)
	}
}

type fixedViewProvider struct{ view sdk.DetectionContext }

func (p fixedViewProvider) Snapshot(context.Context) (sdk.DetectionContext, error) { return p.view, nil }
