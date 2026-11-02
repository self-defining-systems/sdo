package runtime

import (
	"context"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// The real watch classifier publishes into the real controller finding queue;
// the agent stage receives that request and repairs through the real executor.
func TestAddedFieldDriftProducerReachesRepairStage(t *testing.T) {
	cases := map[string]func(*unstructured.Unstructured){
		"cpu-limit": func(o *unstructured.Unstructured) {
			noiseContainer(o)["resources"] = map[string]any{"limits": map[string]any{"cpu": "100m"}}
		},
		"rollout-strategy": func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedMap(o.Object, map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxUnavailable": "100%", "maxSurge": int64(0)}}, "spec", "strategy")
		},
		"host-port": setHostPort,
		"unowned-sidecar": func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(o.Object, append(addedFieldsContainers(o), map[string]any{"name": "unknown-proxy", "image": "synthetic:v1"}), "spec", "template", "spec", "containers")
		},
		"changed-default": func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(o.Object, int64(1), "spec", "progressDeadlineSeconds")
		},
		"node-selector": func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedStringMap(o.Object, map[string]string{"disk": "ssd"}, "spec", "template", "spec", "nodeSelector")
		},
	}
	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			source, old := addedFieldsSource(), addedFieldsLive()
			_ = unstructured.SetNestedMap(old.Object, map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxUnavailable": "25%", "maxSurge": "25%"}}, "spec", "strategy")
			acceptAddedFieldsLive(old)
			live := old.DeepCopy()
			inject(live)
			snapshot := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*live}}
			view := LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source}, SourceCommit: "accepted"}
			start := time.Now()
			config := testControllerConfig()
			config.Namespace = "app"
			c, err := NewController(config, nil, staticProvider{snapshot: snapshot}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
			if err != nil {
				t.Fatal(err)
			}
			c.ConfigureDrift(DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView { return view }})
			// A deleted, unowned foreign object has no surviving additions.
			// It must not turn this batch into a source-capture retry while
			// the unrelated post-acceptance addition still reaches repair.
			c.OnError = func(err error) { t.Errorf("drift batch failed: %v", err) }
			deleted := driftObject(foreignPolicy("deleted-policy"), sdk.WatchKind{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app"})
			c.processDrift(context.Background(), start, snapshot, []ObjectChange{{Old: deleted}, {Old: old, New: live}})
			findings := c.batcher.Snapshot().Findings
			if len(findings) != 1 {
				t.Fatalf("addition lost before stage: findings=%+v", findings)
			}
			request := c.incidentRequest(start, findings)
			request.SourceCommit = "accepted"
			request.RepairPolicy = "recorded-actions"
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{addedFieldsRef(): live}}
			dispatcher := &additionRepairDispatcher{t: t, client: client, source: source}
			state := &IncidentState{Request: request, Findings: findings}
			if err := RunResponseStages(context.Background(), state, []ResponseStage{AgentResponseStage{Dispatcher: dispatcher}}); err != nil {
				t.Fatal(err)
			}
			if !dispatcher.called {
				t.Fatal("repair stage did not receive drift")
			}
			view.DetectionContext = sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*client.objects[addedFieldsRef()]}}
			diff, err := view.DiffAgainstSource(addedFieldsRef())
			if err != nil || diff.Changed {
				t.Fatalf("repair left drift: %+v %v", diff, err)
			}
		})
	}
}

type additionRepairDispatcher struct {
	t      *testing.T
	client *planFakeClient
	source *unstructured.Unstructured
	called bool
}

func (d *additionRepairDispatcher) Dispatch(ctx context.Context, r IncidentRequest) (IncidentResult, error) {
	d.called = true
	if len(r.Findings) != 1 || r.Findings[0].DetectorID != "drift.unexplained" || len(r.DriftEvents) != 1 || len(r.DriftEvents[0].Fields) == 0 {
		d.t.Fatalf("stage brief lost producer evidence: %+v", r)
	}
	executor := PlanExecutor{Client: d.client, Source: planFakeSource{source: d.source}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(ctx, r, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: r.Findings[0].PrimaryResource}}}, false)
	if err != nil || !result.Applied {
		d.t.Fatalf("stage repair: %+v %v", result, err)
	}
	return IncidentResult{}, nil
}

func TestAddedFieldControllerOwnershipIsNotDrift(t *testing.T) {
	for _, declared := range []bool{false, true} {
		t.Run(map[bool]string{false: "added", true: "declared"}[declared], func(t *testing.T) {
			source, old := addedFieldsSource(), addedFieldsLive()
			if !declared {
				unstructured.RemoveNestedField(source.Object, "spec", "replicas")
				unstructured.RemoveNestedField(old.Object, "spec", "replicas")
			}
			if err := RecordAcceptance(old, source); err != nil {
				t.Fatal(err)
			}
			live := old.DeepCopy()
			_ = unstructured.SetNestedField(live.Object, int64(3), "spec", "replicas")
			live.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "horizontal-pod-autoscaler", Subresource: "scale", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)}}})
			view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*live}}), Source: planFakeSource{source: source}, SourceCommit: "accepted"}
			event, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{Old: old, New: live}, view, time.Now())
			if err != nil || event != nil {
				t.Fatalf("controller churn admitted: %+v %v", event, err)
			}
		})
	}
}

// Scripted healthy baselines cover the workload topologies listed in the hotel
// and social seed architectures. These are synthetic objects, not run captures:
// the script accepts each API-defaulted object, then advances only Kubernetes
// bookkeeping/status. No benchmark package is imported into the runtime.
func TestHealthySeedTopologyAddsNoDriftFindings(t *testing.T) {
	topologies := map[string][]string{
		"hotel-reservation": {"frontend", "geo", "profile", "rate", "recommendation", "reservation", "search", "user", "mongodb-geo", "mongodb-profile", "mongodb-rate", "mongodb-recommendation", "mongodb-reservation", "mongodb-user", "memcached-profile", "memcached-rate", "memcached-reserve", "consul", "jaeger"},
		"social-network":    {"nginx-thrift", "media-frontend", "compose-post-service", "home-timeline-service", "media-service", "post-storage-service", "social-graph-service", "text-service", "unique-id-service", "url-shorten-service", "user-mention-service", "user-service", "user-timeline-service", "media-mongodb", "post-storage-mongodb", "social-graph-mongodb", "url-shorten-mongodb", "user-mongodb", "user-timeline-mongodb", "home-timeline-redis", "social-graph-redis", "user-timeline-redis", "media-memcached", "post-storage-memcached", "url-shorten-memcached", "user-memcached", "jaeger"},
	}
	for namespace, names := range topologies {
		t.Run(namespace, func(t *testing.T) {
			for _, name := range names {
				t.Run(name, func(t *testing.T) {
					source := addedFieldsSource()
					source.SetNamespace(namespace)
					source.SetName(name)
					old := source.DeepCopy()
					syntheticContainerDefaults(old)
					_ = unstructured.SetNestedField(old.Object, "ClusterFirst", "spec", "template", "spec", "dnsPolicy")
					if err := RecordAcceptance(old, source); err != nil {
						t.Fatal(err)
					}
					next := old.DeepCopy()
					addAnnotation(next, "deployment.kubernetes.io/revision", "2")
					_ = unstructured.SetNestedField(next.Object, int64(1), "status", "readyReplicas")
					snapshot := sdktest.Snapshot{NamespaceName: namespace, Objects: []unstructured.Unstructured{*next}}
					view := LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source}, SourceCommit: "accepted"}
					config := testControllerConfig()
					config.Namespace = namespace
					c, err := NewController(config, nil, staticProvider{snapshot: snapshot}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Now())
					if err != nil {
						t.Fatal(err)
					}
					c.ConfigureDrift(DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView { return view }})
					c.processDrift(context.Background(), time.Now(), snapshot, []ObjectChange{{Old: old, New: next}})
					if findings := c.batcher.Snapshot().Findings; len(findings) != 0 {
						t.Fatalf("healthy baseline produced findings: %+v", findings)
					}
				})
			}
		})
	}
}

// Filtering the HPA-owned replica from comparison sides must not change the
// accepted-source identity used to detect unrelated additions on that workload.
func TestAddedFieldDriftOnAutoscaledWorkload(t *testing.T) {
	source, old := addedFieldsSource(), addedFieldsLive()
	live := old.DeepCopy()
	containers := addedFieldsContainers(live)
	containers[0].(map[string]any)["resources"] = map[string]any{"limits": map[string]any{"cpu": "100m"}}
	_ = unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers")
	snapshot := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*live}, HPAList: []autoscalingv2.HorizontalPodAutoscaler{{ObjectMeta: metav1.ObjectMeta{Name: "autoscaler", Namespace: "app"}, Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "search"}}}}}
	view := LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source}, SourceCommit: "accepted"}
	event, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{Old: old, New: live}, view, time.Now())
	if err != nil || event == nil || len(event.Fields) != 1 || event.Fields[0].Path != "spec.template.spec.containers[0].resources.limits.cpu" {
		t.Fatalf("HPA hid unrelated addition: %+v %v", event, err)
	}
}
