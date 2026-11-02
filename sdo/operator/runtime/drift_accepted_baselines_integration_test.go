//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// TestAcceptedAppBaselineFixturesAddNoDriftFindings runs source app inputs kept
// outside the repository. The fixture contains accepted source objects only,
// never learned programs or captured run output. The scripted API introduces
// defaults after acceptance, so this tests normalization rather than an initial
// informer event or an unchanged-object shortcut.
func TestAcceptedAppBaselineFixturesAddNoDriftFindings(t *testing.T) {
	path := os.Getenv("SDO_DRIFT_BASELINE_FIXTURES")
	if path == "" {
		t.Skip("set SDO_DRIFT_BASELINE_FIXTURES to source-only hotel/social object fixtures")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var apps map[string][]unstructured.Unstructured
	if err := json.Unmarshal(data, &apps); err != nil {
		t.Fatal(err)
	}
	for _, app := range []string{"hotel_reservation", "social_network"} {
		t.Run(app, func(t *testing.T) {
			objects := apps[app]
			if len(objects) == 0 {
				t.Fatal("accepted app source fixture is empty")
			}
			findings := 0
			for _, object := range objects {
				if object.GetKind() == "Secret" {
					t.Fatal("Secret objects are excluded from these fixtures")
				}
				source := object.DeepCopy()
				old := source.DeepCopy()
				if err := RecordAcceptance(old, source); err != nil {
					t.Fatal(err)
				}
				live := scriptedBaselineDefaults(old)
				ref := sdk.ObjectRef{APIVersion: source.GetAPIVersion(), Kind: source.GetKind(), Namespace: source.GetNamespace(), Name: source.GetName()}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if serveTestDiscovery(w, r) {
						return
					}
					if r.URL.Query().Get("dryRun") != "All" || (r.Method != http.MethodPatch && r.Method != http.MethodPost) {
						t.Errorf("unexpected API request: %s %s", r.Method, r.URL)
						http.Error(w, "dry run required", 500)
						return
					}
					rendered := live.DeepCopy()
					if r.Method == http.MethodPost {
						rendered = scriptedBaselineDefaults(source)
						rendered.SetName(source.GetName() + defaultRenderSuffix)
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(rendered)
				}))
				client := kubernetes.NewForConfigOrDie(&rest.Config{Host: server.URL})
				snapshot := sdktest.Snapshot{NamespaceName: ref.Namespace, Objects: []unstructured.Unstructured{*live}}
				view := LiveReadView{Carrier: inputs.New(snapshot), Client: client, Source: planFakeSource{source: source}, SourceCommit: "accepted"}
				diff, err := view.DiffAgainstSource(ref)
				if err != nil || diff.Changed {
					server.Close()
					t.Fatalf("healthy %s/%s source comparison changed=%v err=%v", ref.Kind, ref.Name, diff.Changed, err)
				}
				config := testControllerConfig()
				config.Namespace = ref.Namespace
				controller, err := NewController(config, nil, staticProvider{snapshot: snapshot}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Now())
				if err != nil {
					server.Close()
					t.Fatal(err)
				}
				controller.ConfigureDrift(DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView { return view }})
				controller.OnError = func(err error) { t.Error(err) }
				controller.processDrift(context.Background(), time.Now(), snapshot, []ObjectChange{{Old: old, New: live}})
				findings += len(controller.batcher.Snapshot().Findings)
				server.Close()
			}
			t.Logf("accepted_app=%s objects=%d new_drift_findings=%d", app, len(objects), findings)
			if findings != 0 {
				t.Fatal("healthy accepted app gained drift findings")
			}
		})
	}
}

// Actual accepted app inputs exercise the two Astronomy source contradictions;
// no benchmark verdict or injected-fault information enters production code.
func TestAcceptedAstronomyAddedFieldsReachRestoration(t *testing.T) {
	path := os.Getenv("SDO_DRIFT_BASELINE_FIXTURES")
	if path == "" {
		t.Skip("set SDO_DRIFT_BASELINE_FIXTURES to source-only accepted app fixtures")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var apps map[string][]unstructured.Unstructured
	if err := json.Unmarshal(data, &apps); err != nil {
		t.Fatal(err)
	}
	objects := apps["astronomy_shop"]
	if len(objects) != 2 {
		t.Fatal("expected ad and frontend source deployments")
	}
	for _, object := range objects {
		t.Run(object.GetName(), func(t *testing.T) {
			source := object.DeepCopy()
			old := scriptedBaselineDefaults(source)
			if err := RecordAcceptance(old, source); err != nil {
				t.Fatal(err)
			}
			live := old.DeepCopy()
			switch source.GetName() {
			case "ad":
				containers := addedFieldsContainers(live)
				container := containers[0].(map[string]any)
				resources, _ := container["resources"].(map[string]any)
				if resources == nil {
					resources = map[string]any{}
					container["resources"] = resources
				}
				limits, _ := resources["limits"].(map[string]any)
				if limits == nil {
					limits = map[string]any{}
					resources["limits"] = limits
				}
				if _, exists := limits["cpu"]; exists {
					t.Fatal("accepted source already declares CPU limit")
				}
				limits["cpu"] = "100m"
				_ = unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers")
			case "frontend":
				if _, found, _ := unstructured.NestedFieldNoCopy(source.Object, "spec", "strategy"); found {
					t.Fatal("accepted source already declares rollout strategy")
				}
				_ = unstructured.SetNestedMap(live.Object, map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": int64(0), "maxUnavailable": "100%"}}, "spec", "strategy")
			default:
				t.Fatal("unexpected source deployment")
			}
			ref := sdk.ObjectRef{APIVersion: source.GetAPIVersion(), Kind: source.GetKind(), Namespace: source.GetNamespace(), Name: source.GetName()}
			snapshot := sdktest.Snapshot{NamespaceName: ref.Namespace, Objects: []unstructured.Unstructured{*live}}
			view := LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source}, SourceCommit: "accepted"}
			config := testControllerConfig()
			config.Namespace = ref.Namespace
			c, err := NewController(config, nil, staticProvider{snapshot: snapshot}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			c.ConfigureDrift(DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView { return view }})
			c.OnError = func(err error) { t.Error(err) }
			c.processDrift(context.Background(), time.Now(), snapshot, []ObjectChange{{Old: old, New: live}})
			findings := c.batcher.Snapshot().Findings
			if len(findings) != 1 {
				t.Fatalf("source contradiction lost: findings=%d", len(findings))
			}
			request := c.incidentRequest(time.Now(), findings)
			request.SourceCommit = "accepted"
			request.RepairPolicy = "recorded-actions"
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}
			stage := AgentResponseStage{Dispatcher: &additionRepairDispatcher{t: t, client: client, source: source}}
			if err := RunResponseStages(context.Background(), &IncidentState{Request: request, Findings: findings}, []ResponseStage{stage}); err != nil {
				t.Fatal(err)
			}
			view.DetectionContext = sdktest.Snapshot{NamespaceName: ref.Namespace, Objects: []unstructured.Unstructured{*client.objects[ref]}}
			diff, err := view.DiffAgainstSource(ref)
			if err != nil || diff.Changed {
				t.Fatalf("accepted app source not restored: changed=%v err=%v", diff.Changed, err)
			}
			t.Logf("accepted_app=astronomy_shop deployment=%s findings=%d restored=true", ref.Name, len(findings))
		})
	}
}

