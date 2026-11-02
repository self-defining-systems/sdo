//go:build integration

package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/runtime/internal/testprotocol"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// One HTTP fake is shared with Python fastloop; the pipe only controls fixture state.
func newInputsAPI(t *testing.T, ctx context.Context, snapshot any) (string, func(any)) {
	t.Helper()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "python3", "-c", "import runpy; runpy.run_path('tests/fixtures/sdo/fake_api_bridge.py', run_name='__main__')")
	cmd.Dir = root
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		in.Close()
		if err := cmd.Wait(); err != nil {
			t.Error(err)
		}
	})
	reader := bufio.NewReader(out)
	encoder := json.NewEncoder(in)
	exchange := func(v any) map[string]string {
		t.Helper()
		if err := encoder.Encode(v); err != nil {
			t.Fatal(err)
		}
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]string
		if err := json.Unmarshal(line, &result); err != nil {
			t.Fatalf("bridge %q: %v", line, err)
		}
		return result
	}
	first := exchange(snapshot)
	return first["url"], func(v any) { exchange(v) }
}

func TestHermeticNewDetectorInputs(t *testing.T) {
	started := time.Now()
	t.Cleanup(func() { t.Logf("new-input hermetic wall time including cleanup: %s", time.Since(started)) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	var snapshot any
	if err := json.Unmarshal(capturefixture.Raw(t, "fake API server serves a raw snapshot view, not a capture boundary", []byte(`{"namespace":"app","statefulSets":[{"metadata":{"name":"db","namespace":"app"},"spec":{"replicas":2},"status":{"readyReplicas":2}}],"pods":[{"metadata":{"name":"db-0","namespace":"app","uid":"uid"},"spec":{"nodeName":"node","containers":[{"name":"db","resources":{"limits":{"memory":"1000"}}}]}}]}`)), &snapshot); err != nil {
		t.Fatal(err)
	}
	url, control := newInputsAPI(t, ctx, snapshot)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: url, ContentConfig: rest.ContentConfig{ContentType: "application/json"}})
	if err != nil {
		t.Fatal(err)
	}
	detectors := []sdk.Detector{newInputsDetector{}}
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: client}, detectors)
	if err != nil {
		t.Fatal(err)
	}
	cache.Start(ctx)
	defer cache.Stop()
	if err := cache.WaitForSync(ctx); err != nil {
		t.Fatal(err)
	}
	var mode atomic.Value
	mode.Store("healthy")
	workloads, err := prober.ParseWorkloads(map[string][]byte{"links": []byte(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"links","purpose":"link-probe","interval":"100ms","timeout":"100ms","links":[{"from":"web","to":"db","port":27017,"protocol":"mongodb"}]}`)})
	if err != nil {
		t.Fatal(err)
	}
	p, err := prober.New(prober.Config{Namespace: "app", Catalog: traffic.Catalog{{ID: "dummy", Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead, Steps: []traffic.Step{{Name: "dummy", Endpoint: traffic.GET("/", nil)}}}}, Workloads: workloads, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return testprotocol.Dial(ctx, "mongodb", mode.Load().(string))
	}})
	if err != nil {
		t.Fatal(err)
	}
	probeCtx, stop := context.WithCancel(ctx)
	probeDone := make(chan struct{})
	go func() { defer close(probeDone); p.Run(probeCtx) }()
	defer func() { stop(); <-probeDone }()
	waitLink := func(ok bool) {
		t.Helper()
		for {
			w := p.Windows()["links"]
			if len(w.Links) > 0 && len(w.Links[0].Samples) > 0 {
				last := w.Links[0].Samples[len(w.Links[0].Samples)-1]
				if last.OK == ok {
					return
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal("link observation timeout")
			case <-time.After(time.Millisecond):
			}
		}
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	setUsage := func(memory int) {
		control(map[string]any{"node": "node", "summary": map[string]any{"pods": []any{map[string]any{"podRef": map[string]string{"namespace": "app", "name": "db-0", "uid": "uid"}, "containers": []any{map[string]any{"name": "db", "memory": map[string]int{"workingSetBytes": memory}}}}}}, "cadvisor": "container_cpu_cfs_periods_total{namespace=\"app\",pod=\"db-0\",container=\"db\"} 100\ncontainer_cpu_cfs_throttled_periods_total{namespace=\"app\",pod=\"db-0\",container=\"db\"} 0\n"})
	}
	provider := observedLinks{SnapshotProvider: withUsage(cache, detectors, client, func() time.Time { return now }), p: p}
	config := testControllerConfig()
	config.Namespace = "app"
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(config, detectors, provider, dispatcher, now)
	if err != nil {
		t.Fatal(err)
	}
	step := func() {
		t.Helper()
		if err := controller.Step(ctx, now, nil); err != nil {
			t.Fatal(err)
		}
		if len(controller.history) == 0 || controller.history[len(controller.history)-1].Error != "" {
			t.Fatalf("evaluation: %+v", controller.history)
		}
	}
	waitLink(true)
	setUsage(100)
	step()
	now = now.Add(15 * time.Second)
	step()
	if controller.incidentOpen || controller.history[len(controller.history)-1].Status != DetectorEvaluationClear {
		t.Fatal("healthy state raised a finding")
	}
	sts := map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": map[string]string{"name": "db", "namespace": "app"}, "spec": map[string]int{"replicas": 2}, "status": map[string]int{"readyReplicas": 1}}
	control(map[string]any{"plural": "statefulsets", "object": sts})
	for {
		view, _ := cache.Snapshot(ctx)
		if view.StatefulSets()[0].Status.ReadyReplicas == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("watch timeout")
		case <-time.After(time.Millisecond):
		}
	}
	mode.Store("protocol_error")
	waitLink(false)
	setUsage(900)
	now = now.Add(15 * time.Second)
	step()
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if len(request.Findings) != 3 {
		t.Fatalf("findings: %+v", request.Findings)
	}
	evidence := map[string]string{}
	for _, f := range request.Findings {
		evidence[f.RuleID] = f.Evidence
		expectedName := "db"
		if f.RuleID == "memory-forecast" {
			expectedName = "db-0"
		}
		if f.PrimaryResource.Name != expectedName || f.PrimaryResource.Namespace != "app" {
			t.Fatalf("unexpected primary resource: %+v", f)
		}
	}
	for rule, want := range map[string]string{"replicas": "ready=1 desired=2", "memory-forecast": "observed=900 limit=1000 window=1m0s forecast=", "mongodb-link": "class=protocol_error"} {
		if !strings.Contains(evidence[rule], want) {
			t.Fatalf("%s evidence: %s", rule, evidence[rule])
		}
	}
	awaitDispatchCompletionQueued(t, controller)
	sts["status"] = map[string]int{"readyReplicas": 2}
	control(map[string]any{"plural": "statefulsets", "object": sts})
	for {
		view, _ := cache.Snapshot(ctx)
		if view.StatefulSets()[0].Status.ReadyReplicas == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("recovery watch timeout")
		case <-time.After(time.Millisecond):
		}
	}
	mode.Store("healthy")
	waitLink(true)
	setUsage(100)
	now = now.Add(15 * time.Second)
	step()
	now = now.Add(15 * time.Second)
	step()
	if h := controller.history[len(controller.history)-1]; h.Status != DetectorEvaluationClear {
		t.Fatalf("recovery: %+v", h)
	}
	if controller.incidentOpen {
		t.Fatal("recovered incident remains open")
	}
}

func TestHermeticNewSnapshotKinds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	// These projections check both the typed informer decoder and the frozen view.
	cases := []struct {
		version, kind, plural, collection string
		labels                            func(sdk.DetectionContext) map[string]string
	}{
		{"apps/v1", "StatefulSet", "statefulsets", "statefulSets", func(v sdk.DetectionContext) map[string]string { return v.StatefulSets()[0].Labels }},
		{"apps/v1", "DaemonSet", "daemonsets", "daemonSets", func(v sdk.DetectionContext) map[string]string { return v.DaemonSets()[0].Labels }},
		{"batch/v1", "Job", "jobs", "jobs", func(v sdk.DetectionContext) map[string]string { return v.Jobs()[0].Labels }},
		{"batch/v1", "CronJob", "cronjobs", "cronJobs", func(v sdk.DetectionContext) map[string]string { return v.CronJobs()[0].Labels }},
		{"v1", "PersistentVolumeClaim", "persistentvolumeclaims", "persistentVolumeClaims", func(v sdk.DetectionContext) map[string]string { return v.PersistentVolumeClaims()[0].Labels }},
		{"v1", "ServiceAccount", "serviceaccounts", "serviceAccounts", func(v sdk.DetectionContext) map[string]string { return v.ServiceAccounts()[0].Labels }},
		{"autoscaling/v2", "HorizontalPodAutoscaler", "horizontalpodautoscalers", "horizontalPodAutoscalers", func(v sdk.DetectionContext) map[string]string { return v.HorizontalPodAutoscalers()[0].Labels }},
		{"policy/v1", "PodDisruptionBudget", "poddisruptionbudgets", "podDisruptionBudgets", func(v sdk.DetectionContext) map[string]string { return v.PodDisruptionBudgets()[0].Labels }},
		{"networking.k8s.io/v1", "Ingress", "ingresses", "ingresses", func(v sdk.DetectionContext) map[string]string { return v.Ingresses()[0].Labels }},
		{"v1", "Node", "nodes", "nodes", func(v sdk.DetectionContext) map[string]string { return v.Nodes()[0].Labels }},
		{"v1", "Secret", "secrets", "secrets", func(v sdk.DetectionContext) map[string]string { return v.Secrets()[0].Labels }},
	}
	snapshot := map[string]any{"namespace": "app"}
	var watches []sdk.WatchKind
	object := func(version, kind, phase string) map[string]any {
		metadata := map[string]any{"name": "test", "labels": map[string]string{"phase": phase}}
		if kind != "Node" {
			metadata["namespace"] = "app"
		}
		result := map[string]any{"apiVersion": version, "kind": kind, "metadata": metadata}
		if kind == "Secret" {
			metadata["annotations"] = map[string]string{"unsafe": "CANARY"}
			result["data"] = map[string]string{"password": "CANARY"}
			result["stringData"] = map[string]string{"token": "CANARY"}
		}
		return result
	}
	for _, c := range cases {
		snapshot[c.collection] = []any{object(c.version, c.kind, "initial")}
		watches = append(watches, sdk.WatchKind{APIVersion: c.version, Kind: c.kind})
	}
	url, control := newInputsAPI(t, ctx, snapshot)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: url, ContentConfig: rest.ContentConfig{ContentType: "application/json"}})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: client}, []sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{Watches: watches}}})
	if err != nil {
		t.Fatal(err)
	}
	cache.Start(ctx)
	defer cache.Stop()
	if err := cache.WaitForSync(ctx); err != nil {
		t.Fatal(err)
	}
	view, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if c.labels(view)["phase"] != "initial" {
			t.Fatalf("%s missing from initial list", c.kind)
		}
	}
	for _, c := range cases {
		control(map[string]any{"plural": c.plural, "object": object(c.version, c.kind, "recovered")})
	}
	for _, c := range cases {
		for {
			view, err = cache.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if c.labels(view)["phase"] == "recovered" {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("%s watch timeout", c.kind)
			case <-time.After(time.Millisecond):
			}
		}
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "CANARY") {
		t.Fatal("Secret value escaped into frozen snapshot")
	}
	secrets := view.Secrets()
	if len(secrets) != 1 || strings.Join(secrets[0].DataKeys, ",") != "password,token" {
		t.Fatal("Secret key names were lost")
	}
}
