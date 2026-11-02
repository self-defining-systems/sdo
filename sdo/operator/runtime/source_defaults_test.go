package runtime

import (
	"context"
	"encoding/json"
	appsv1 "k8s.io/api/apps/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
)

// The fake API fills defaults after PATCH exactly where Kubernetes does. The
// executor consumes the same independently defaulted source, never a local list
// of ignored field paths. Different values and new containers remain residuals.
type sourceDefaultsClient struct {
	reconcilingAdmissionClient
	source *unstructured.Unstructured
}

func (c sourceDefaultsClient) SourceDefaults(context.Context, sdk.ObjectRef, *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	defaults := c.source.DeepCopy()
	syntheticContainerDefaults(defaults)
	return defaults, nil
}
func syntheticContainerDefaults(o *unstructured.Unstructured) {
	c := noiseContainer(o)
	c["terminationMessagePath"] = "/dev/termination-log"
	c["terminationMessagePolicy"] = "File"
	ports, _ := c["ports"].([]any)
	for _, p := range ports {
		p.(map[string]any)["protocol"] = "TCP"
	}
}
func TestSourceDefaultReadbackProducer(t *testing.T) {
	for _, real := range []bool{false, true} {
		t.Run(map[bool]string{false: "defaults", true: "real-residual"}[real], func(t *testing.T) {
			source := addedFieldsSource()
			noiseContainer(source)["ports"] = []any{map[string]any{"containerPort": int64(8080)}}
			live := source.DeepCopy()
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			noiseContainer(live)["image"] = "broken"
			client := sourceDefaultsClient{source: source, reconcilingAdmissionClient: reconcilingAdmissionClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{addedFieldsRef(): live}}, mutate: func(o *unstructured.Unstructured) {
				syntheticContainerDefaults(o)
				if real {
					noiseContainer(o)["terminationMessagePath"] = "/real-residual"
				}
			}}}
			executor := PlanExecutor{Client: client, Source: planFakeSource{source: source}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: addedFieldsRef()}}}, false)
			receipt, encodeErr := result.Proto()
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			if real {
				if err == nil || receipt.GetVerificationPassed() || len(receipt.ActionReports[0].SourceReconcile.ResidualPaths) != 1 {
					t.Fatalf("real residual lost: %v %v", receipt, err)
				}
			} else if err != nil || !receipt.GetVerificationPassed() || len(receipt.ActionReports[0].SourceReconcile.ResidualPaths) != 0 {
				t.Fatalf("defaults became failed work: %v %v", receipt, err)
			}
		})
	}
}

type settlementImageProgram struct{ pipelineImageProgram }

func (settlementImageProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: b.Objects["deployment"]}}}, nil
}

type settlementSource struct {
	PlanSource
	source *unstructured.Unstructured
}

func (s settlementSource) AcceptedManifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind == "Deployment" {
		return s.source.DeepCopy(), nil
	}
	return s.PlanSource.AcceptedManifest(ctx, revision, ref)
}

type settlementObjectClient struct {
	PlanObjectClient
	workload sourceDefaultsClient
}

func (c settlementObjectClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind == "Deployment" {
		return c.workload.Get(ctx, ref)
	}
	return c.PlanObjectClient.Get(ctx, ref)
}
func (c settlementObjectClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if ref.Kind == "Deployment" {
		return c.workload.Patch(ctx, ref, kind, patch)
	}
	return c.PlanObjectClient.Patch(ctx, ref, kind, patch)
}
func (c settlementObjectClient) SourceDefaults(ctx context.Context, ref sdk.ObjectRef, source *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if ref.Kind == "Deployment" {
		return c.workload.SourceDefaults(ctx, ref, source)
	}
	return source.DeepCopy(), nil
}

type settlementSnapshot struct {
	SnapshotProvider
	client settlementObjectClient
	ref    sdk.ObjectRef
}

func (p settlementSnapshot) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	snapshot, err := p.SnapshotProvider.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	current, err := p.client.Get(ctx, p.ref)
	if err != nil {
		return nil, err
	}
	var deployment appsv1.Deployment
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(current.Object, &deployment); err != nil {
		return nil, err
	}
	stored := snapshot.(sdktest.Snapshot)
	stored.DeploymentList = []appsv1.Deployment{deployment}
	stored.Objects = []unstructured.Unstructured{*current}
	return stored, nil
}

// Real executor and deterministic stage -> typed outcome -> broker no-change
// check. The diagnosis clears in both cases; source residuals must still decide
// whether there is new work, independently of global health success.
func TestSourceDefaultStageProducerReachesNoveltyConsumer(t *testing.T) {
	for _, scenario := range []string{"default-only", "real-residual", "delayed-residual"} {
		t.Run(scenario, func(t *testing.T) {
			real := scenario != "default-only"
			loop, state := pipelineTestLoop(t, false)
			source, err := loop.Executor.Source.AcceptedManifest(context.Background(), state.Request.SourceCommit, sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"})
			if err != nil {
				t.Fatal(err)
			}
			c := noiseContainer(source)
			delete(c, "terminationMessagePath")
			delete(c, "terminationMessagePolicy")
			for _, port := range c["ports"].([]any) {
				delete(port.(map[string]any), "protocol")
			}
			live := source.DeepCopy()
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			noiseContainer(live)["image"] = "missing:v1"
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			workload := sourceDefaultsClient{source: source, reconcilingAdmissionClient: reconcilingAdmissionClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}, mutate: func(o *unstructured.Unstructured) {
				syntheticContainerDefaults(o)
				if real {
					noiseContainer(o)["terminationMessagePath"] = "/real-residual"
				}
			}}}
			client := settlementObjectClient{PlanObjectClient: loop.Executor.Client, workload: workload}
			provider := settlementSnapshot{SnapshotProvider: loop.Provider, client: client, ref: ref}
			loop.Provider = provider
			loop.Executor.Client = client
			loop.Executor.Source = settlementSource{PlanSource: loop.Executor.Source, source: source}
			loop.Executor.Rollouts = &planFakeRollout{}
			loop.Executor.Verifier = PlanHealthVerifier{Detectors: loop.Detectors, Provider: provider}
			registry, err := program.NewRegistry([]program.Diagnoser{pipelinePortProgram{}, settlementImageProgram{}}, []program.Mitigator{pipelinePortProgram{}, settlementImageProgram{}})
			if err != nil {
				t.Fatal(err)
			}
			loop.Programs = registry
			if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
				t.Fatal(err)
			}
			if scenario == "delayed-residual" {
				real = false
				syntheticContainerDefaults(workload.objects[ref])
			}
			result, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			result, stages := loop.SettleRecoveryApplications(context.Background(), result, state.PriorStages)
			closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}, Frozen: true}
			closure.Request.PriorStages = stages
			if calls := settlementBrokerWorkers(t, loop, closure); (calls > 0) != real {
				t.Fatalf("reflection workers=%d real residual=%v applications=%+v stages=%v", calls, real, result.PlanApplications, stages)
			}
		})
	}
}

// Real API default rendering feeds the real drift comparison. Both the
// declared-field dry run and isolated-default render are observation-only.
func TestAPIDefaultRenderFeedsSourceComparison(t *testing.T) {
	for _, real := range []bool{false, true} {
		t.Run(map[bool]string{false: "defaults", true: "different-value"}[real], func(t *testing.T) {
			source := addedFieldsSource()
			noiseContainer(source)["ports"] = []any{map[string]any{"containerPort": int64(8080)}}
			live := source.DeepCopy()
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			syntheticContainerDefaults(live)
			// A known admission injector is represented by the independent
			// API render, not a blanket exemption for arbitrary manager names.
			injectProxy := func(object *unstructured.Unstructured) {
				containers := append(addedFieldsContainers(object), map[string]any{"name": "injected-proxy", "image": "synthetic-proxy:v1"})
				_ = unstructured.SetNestedSlice(object.Object, containers, "spec", "template", "spec", "containers")
			}
			injectProxy(live)
			if real {
				noiseContainer(live)["terminationMessagePath"] = "/different"
			}
			renders := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if serveTestDiscovery(w, r) {
					return
				}
				if (r.Method != http.MethodPatch && r.Method != http.MethodPost) || r.URL.Query().Get("dryRun") != "All" {
					t.Errorf("unexpected non-observation API request: %s %s", r.Method, r.URL)
					http.Error(w, "unexpected request", 500)
					return
				}
				response := live.DeepCopy()
				if r.Method == http.MethodPost {
					var submitted unstructured.Unstructured
					if err := json.NewDecoder(r.Body).Decode(&submitted); err != nil {
						t.Error(err)
					}
					if submitted.GetAPIVersion() != source.GetAPIVersion() || submitted.GetKind() != source.GetKind() || submitted.GetNamespace() != source.GetNamespace() {
						t.Errorf("source render lost API identity: %s %s %s", submitted.GetAPIVersion(), submitted.GetKind(), submitted.GetNamespace())
					}
					renders++
					response = source.DeepCopy()
					response.SetName(source.GetName() + defaultRenderSuffix)
					syntheticContainerDefaults(response)
					injectProxy(response)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			client := kubernetes.NewForConfigOrDie(&rest.Config{Host: server.URL})
			view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: source.GetNamespace(), Objects: []unstructured.Unstructured{*live}}), Client: client, Source: planFakeSource{source: source}, SourceCommit: "accepted"}
			diff, err := view.DiffAgainstSource(addedFieldsRef())
			if err != nil || diff.Changed != real || renders != 1 {
				t.Fatalf("API defaults not used: diff=%+v err=%v renders=%d", diff, err, renders)
			}
			event, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{Old: source, New: live}, view, time.Now())
			if real && event != nil && (len(event.Fields) != 1 || event.Fields[0].Path != "spec.template.spec.containers[0].terminationMessagePath") {
				t.Fatalf("default/injected fields leaked into mixed drift evidence: %+v", event.Fields)
			}
			if err != nil || (event != nil) != real {
				t.Fatalf("default normalization lost at drift admission: event=%+v err=%v", event, err)
			}
			rendersBeforePlan := renders
			defaults, err := (TypedPlanClient{Client: client}).SourceDefaults(context.Background(), addedFieldsRef(), source)
			if err != nil || defaults == nil || renders != rendersBeforePlan+1 {
				t.Fatalf("plan source default transport: %v %v renders=%d", defaults, err, renders)
			}
		})
	}
}

// A dry-run apply over a coincidentally existing probe can retain a writer's
// fields and mislabel them as API defaults. Create must fail on collision.
func TestDefaultRenderUsesIsolatedCreate(t *testing.T) {
	source := addedFieldsSource()
	creates := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if serveTestDiscovery(w, r) {
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/apis/apps/v1/namespaces/app/deployments" || r.URL.Query().Get("dryRun") != "All" {
			http.Error(w, "default rendering must create in isolation", http.StatusConflict)
			return
		}
		var object unstructured.Unstructured
		if err := json.NewDecoder(r.Body).Decode(&object); err != nil {
			t.Error(err)
		}
		if object.GetName() != source.GetName()+defaultRenderSuffix {
			t.Errorf("probe name=%s", object.GetName())
		}
		if object.GetUID() != "" || object.GetResourceVersion() != "" {
			t.Error("probe retained object instance identity")
		}
		creates++
		syntheticContainerDefaults(&object)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&object)
	}))
	defer server.Close()
	client := kubernetes.NewForConfigOrDie(&rest.Config{Host: server.URL})
	_, err := (TypedPlanClient{Client: client}).SourceDefaults(context.Background(), addedFieldsRef(), source)
	if err != nil || creates != 1 {
		t.Fatalf("isolated create: creates=%d err=%v", creates, err)
	}
}
