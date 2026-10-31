package runtime

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/usage"
	"testing"
	"time"
)

type usageDetector struct{ scheduledDetector }

func (usageDetector) UsageRequirements() usage.Requirements {
	return usage.Requirements{Throttling: true}
}
func TestUsageCollection(t *testing.T) {
	calls := 0
	fail := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if fail {
			http.Error(w, "denied", 403)
			return
		}
		switch r.URL.Path {
		case "/api/v1/nodes/node/proxy/stats/summary":
			w.Write([]byte(`{"pods":[{"podRef":{"namespace":"app","name":"pod","uid":"uid"},"containers":[{"name":"web","startTime":"2026-01-01T00:00:00Z","cpu":{"time":"2026-01-01T00:01:00Z","usageNanoCores":500000000},"memory":{"workingSetBytes":123,"rssBytes":100}}],"volume":[{"pvcRef":{"name":"disk","namespace":"app"},"capacityBytes":1000,"usedBytes":400,"availableBytes":600}]}]}`))
		case "/api/v1/nodes/node/proxy/metrics/cadvisor":
			w.Write([]byte("container_cpu_cfs_periods_total{namespace=\"app\",pod=\"pod\",container=\"web\"} 100\ncontainer_cpu_cfs_throttled_periods_total{namespace=\"app\",pod=\"pod\",container=\"web\"} 20\n"))
		default:
			t.Error(r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	base := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "app", UID: types.UID("uid")}, Spec: corev1.PodSpec{NodeName: "node", Containers: []corev1.Container{{Name: "web"}}}}}}}
	p := withUsage(base, nil, client, func() time.Time { return now })
	p.Snapshot(context.Background())
	if calls != 0 {
		t.Fatal(calls)
	}
	p = withUsage(base, []sdk.Detector{usageDetector{}}, client, func() time.Time { return now })
	ctx, _ := p.Snapshot(context.Background())
	r, err := usage.From(ctx)
	if err != nil || len(r.Containers) != 1 || *r.Containers[0].Samples[0].CPUCores != .5 || len(r.Volumes) != 1 {
		t.Fatal(r, err)
	}
	if r.Containers[0].Samples[0].Periods != 100 {
		t.Fatal(r)
	}
	usage.From(ctx)
	p.Snapshot(context.Background())
	if calls != 2 {
		t.Fatal(calls)
	}
	now = now.Add(15 * time.Second)
	fail = true
	ctx, _ = p.Snapshot(context.Background())
	if findings, err := (usageDetector{}).Detect(context.Background(), ctx); err == nil || len(findings) != 0 {
		t.Fatal("failure must be detector error")
	}
}

func TestUsageParserBoundsAndCounters(t *testing.T) {
	for _, data := range []string{`broken`, string(make([]byte, maxUsageBytes+1))} {
		if _, err := parseUsageSummary([]byte(data), "app", nil, time.Time{}); err == nil {
			t.Fatal("invalid summary accepted")
		}
	}
	text := "container_cpu_cfs_periods_total{namespace=\"other\",pod=\"pod\",container=\"web\"} 10\ncontainer_cpu_cfs_periods_total{namespace=\"app\",pod=\"pod\",container=\"web\"} 100\ncontainer_cpu_cfs_throttled_periods_total{namespace=\"app\",pod=\"pod\",container=\"web\"} 12\n"
	c, err := parseUsageCounters([]byte(text), "app")
	if err != nil || len(c) != 1 || c["pod/web"].throttled != 12 {
		t.Fatal(c, err)
	}
	if _, err := parseUsageCounters([]byte(text+text), "app"); err == nil {
		t.Fatal("duplicate counters")
	}
	if _, err := parseUsageCounters([]byte("container_cpu_cfs_periods_total{broken} x\n"), "app"); err == nil {
		t.Fatal("malformed counters")
	}
}
func TestUsageRollingWindow(t *testing.T) {
	var r usage.Result
	for i := 0; i < 100; i++ {
		r = mergeUsage(r, usage.Result{Containers: []usage.ContainerWindow{{Identity: usage.Identity{PodUID: "one"}, Samples: []usage.Sample{{At: time.Unix(int64(i+1), 0)}}}}})
	}
	if len(r.Containers[0].Samples) != usage.MaxSamples {
		t.Fatal(len(r.Containers[0].Samples))
	}
	next := mergeUsage(r, usage.Result{Containers: []usage.ContainerWindow{{Identity: usage.Identity{PodUID: "two"}, Samples: []usage.Sample{{At: time.Unix(200, 0)}}}}})
	if len(next.Containers[0].Samples) != 1 {
		t.Fatal("mixed replacement histories")
	}
}

func (d usageDetector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	_, err := usage.From(ctx)
	return nil, err
}

func TestSummaryMissingRunningContainerIsError(t *testing.T) {
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "pod", UID: "uid"}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "web", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}}
	if _, err := parseUsageSummary([]byte(`{"pods":[]}`), "app", pods, time.Time{}); err == nil {
		t.Fatal("missing observation looked healthy")
	}
}

type summaryUsageDetector struct{ usageDetector }

func (summaryUsageDetector) UsageRequirements() usage.Requirements { return usage.Requirements{} }
func TestUsageSummaryOnlyOnceForMultipleConsumers(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/api/v1/nodes/node/proxy/stats/summary" {
			t.Error(r.URL.Path)
		}
		w.Write([]byte(`{"pods":[]}`))
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	base := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", UID: "uid"}, Spec: corev1.PodSpec{NodeName: "node"}}}}}
	p := withUsage(base, []sdk.Detector{summaryUsageDetector{}, summaryUsageDetector{}}, client, nil)
	ctx, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := usage.From(ctx); err != nil {
		t.Fatal(err)
	}
	usage.From(ctx)
	if calls != 1 {
		t.Fatal(calls)
	}
}
func TestUsageSummaryJoinsLimits(t *testing.T) {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "pod", UID: "api-uid", Annotations: map[string]string{"kubernetes.io/config.mirror": "uid"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}, Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")}}}}}}
	data := []byte(`{"pods":[{"podRef":{"namespace":"app","name":"pod","uid":"uid"},"ephemeral-storage":{"usedBytes":30},"containers":[{"name":"web","rootfs":{"usedBytes":10},"logs":{"usedBytes":5},"memory":{"workingSetBytes":42,"rssBytes":40}}],"volume":[{"pvcRef":{"namespace":"app","name":"disk"},"capacityBytes":1000,"usedBytes":50,"availableBytes":950,"inodes":100,"inodesUsed":10,"inodesFree":90}]}]}`)
	r, err := parseUsageSummary(data, "app", []corev1.Pod{pod}, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if r.Containers[1].Identity.PodUID != "api-uid" {
		t.Fatal("mirror UID not canonicalized")
	}
	s := r.Containers[1].Samples[0]
	if s.CPULimit != nil || *s.CPURequest != .1 || *s.MemoryLimit != 128*1024*1024 || *s.EphemeralBytes != 15 || *s.WorkingSetBytes != 42 || *s.RSSBytes != 40 || *r.Volumes[0].Samples[0].InodesUsed != 10 {
		t.Fatal(r)
	}
}
