package runtime

import (
	"context"
	"encoding/json"
	"google.golang.org/protobuf/encoding/protojson"
	"os"
	"os/exec"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type cleanupInventory struct {
	client dynamic.Interface
	refs   map[sdk.ObjectRef]schema.GroupVersionResource
}

func (s cleanupInventory) TakeObjectChanges() []ObjectChange { return nil }
func (s cleanupInventory) SourceDriftObjects() []*unstructured.Unstructured {
	var result []*unstructured.Unstructured
	for ref, gvr := range s.refs {
		resource := s.client.Resource(gvr)
		var object *unstructured.Unstructured
		var err error
		if ref.Namespace != "" {
			object, err = resource.Namespace(ref.Namespace).Get(context.Background(), ref.Name, metav1.GetOptions{})
		} else {
			object, err = resource.Get(context.Background(), ref.Name, metav1.GetOptions{})
		}
		if err == nil {
			result = append(result, object)
		}
	}
	return result
}

type cleanupVerifier struct{ controller *Controller }

func (v cleanupVerifier) Verify(ctx context.Context) (bool, error) {
	return v.controller.responderVerification(ctx, v.controller.currentIncidentRequest.IncidentID).Healthy, nil
}

// Handwritten fixture, real generic Kubernetes transport, CLI/session transport,
// registered dispatcher, same-session continuation, and independent closure.
func TestHermeticUnexplainedCleanupRemediation(t *testing.T) {
	for _, denied := range []bool{false, true} {
		name := "removed"
		if denied {
			name = "authorization-denied"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			kube := kubefake.NewSimpleClientset()
			discovery := kube.Discovery().(*fake.FakeDiscovery)
			discovery.Resources = []*metav1.APIResourceList{
				{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "serviceaccounts", Kind: "ServiceAccount", Namespaced: true, Verbs: []string{"get", "list", "delete"}}}},
				{GroupVersion: "apps/v1", APIResources: []metav1.APIResource{{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: []string{"get", "list", "delete"}}}},
				{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: "ClusterRole", Verbs: []string{"get", "list", "delete"}}, {Name: "clusterrolebindings", Kind: "ClusterRoleBinding", Verbs: []string{"get", "list", "delete"}}}},
			}
			refs := []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "cleanup-controller"}, {APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "configmap-cleanup-controller"}, {APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding", Name: "configmap-cleanup-controller"}, {APIVersion: "v1", Kind: "ServiceAccount", Namespace: "app", Name: "cleanup-controller"}}
			var objects []kruntime.Object
			paths := map[sdk.ObjectRef]schema.GroupVersionResource{}
			listKinds := map[schema.GroupVersionResource]string{}
			for _, ref := range refs {
				object := urObject(ref)
				object.SetLabels(map[string]string{"platform.io/injected": "true"})
				if ref.Kind == "ClusterRole" {
					object.Object["rules"] = []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get", "list", "watch"}}, map[string]any{"apiGroups": []any{"apps"}, "resources": []any{"deployments"}, "verbs": []any{"get", "patch"}}}
				}
				if ref.Kind == "ClusterRoleBinding" {
					object.Object["subjects"] = []any{map[string]any{"kind": "ServiceAccount", "name": "cleanup-controller", "namespace": "app"}}
					object.Object["roleRef"] = map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "configmap-cleanup-controller"}
				}
				objects = append(objects, object)
				gvk := schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind)
				resource := map[string]string{"Deployment": "deployments", "ClusterRole": "clusterroles", "ClusterRoleBinding": "clusterrolebindings", "ServiceAccount": "serviceaccounts"}[ref.Kind]
				gvr := gvk.GroupVersion().WithResource(resource)
				paths[ref] = gvr
				listKinds[gvr] = ref.Kind + "List"
			}
			live := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(kruntime.NewScheme(), listKinds, objects...)
			live.PrependReactor("delete", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
				deletion := action.(ktesting.DeleteAction)
				if denied && action.GetResource().Resource != "deployments" {
					return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), deletion.GetName(), nil)
				}
				return false, nil, nil
			})
			checkedLive := checkedCleanupDynamic{Interface: live, t: t}
			store := cleanupInventory{client: checkedLive, refs: paths}
			request := goldenRequest(t)
			request.Namespace = "app"
			request.RepairPolicy = "recorded-actions"
			request.RepositoryWorktree = filepath.Join(t.TempDir(), "proposal")
			request.ResponseDeadline = time.Now().Add(time.Minute)
			request.MaxResponseLoops = 10
			index := NewSourceInventory("app", request.SourceCommit, nil)
			request.Findings = index.Findings(store.SourceDriftObjects(), time.Now())
			if len(request.Findings) != 4 {
				t.Fatalf("fixture failed to expose all four injected objects: %+v", request.Findings)
			}
			config := testControllerConfig()
			config.Namespace = "app"
			config.SourceCommit = request.SourceCommit
			config.VerificationTimeout = 5 * time.Minute
			provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}
			controller, err := NewController(config, []sdk.Detector{inventoryHealth()}, provider, &verificationRetryDispatcher{}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			controller.currentIncidentRequest = &request
			controller.incidentOpen = true
			controller.sourceInventory = index
			controller.drift = &DriftControllerConfig{Inventory: true, Events: store}
			controller.history = []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}
			loop := NewResponderLoop(request, nil, provider, nil, PlanExecutor{Client: GenericPlanClient{TypedPlanClient: TypedPlanClient{Client: kube}, Objects: &core.GenericCapture{Namespace: "app", Client: checkedLive, Discovery: discovery}}, Rollouts: KubernetesPlanRollouts{Client: kube}, Verifier: cleanupVerifier{controller}}, 10)
			loop.MenuConfig = productionResponderMenu()
			loop.MenuConfig.Unexplained = func(context.Context) (UnexplainedRemovalAssessment, error) {
				return controller.unexplainedRemovalAssessment(request.IncidentID, "demo")
			}
			loop.MenuConfig.RemoveUnexplained = func(ctx context.Context, selected UnexplainedRemoval, client PlanLifecycleClient, ref sdk.ObjectRef, uid types.UID, rv string) error {
				return controller.removeUnexplained(ctx, request.IncidentID, "demo", selected, client, ref, uid, rv)
			}
			loop.VerifyClosure = func(ctx context.Context) ToolResponse {
				return controller.responderVerification(ctx, request.IncidentID)
			}
			loop.VerifyResult = func(ctx context.Context, result IncidentResult) ToolResponse {
				return controller.responderResultVerification(ctx, request.IncidentID, &result)
			}
			dispatcher := KubernetesJobDispatcher{Client: kube, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, RequireMemorySnapshot: true, Catalog: fixedMemoryCatalog{request.RepositoryBaseCommit}, LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
			done := make(chan IncidentResult, 1)
			errs := make(chan error, 1)
			go func() {
				result, err := dispatcher.Dispatch(ctx, request)
				if err != nil {
					t.Logf("scripted dispatcher failed: %v", err)
				}
				done <- result
				errs <- err
			}()
			first := awaitJob(t, kube, IncidentJobName(request.IncidentID))
			tools := ""
			for _, mount := range first.Spec.Template.Spec.Containers[0].VolumeMounts {
				if mount.MountPath == "/sdo/session" {
					tools = filepath.Join(dispatcher.RepositoryMountPath, mount.SubPath, "tools")
				}
			}
			cleanupTool(t, tools, "mitigate", "DeleteObject", "--ref", "Deployment/app/cleanup-controller", "--reason", "remove injected source-absent controller")
			menu := cleanupTool(t, tools, "menu")
			for _, kind := range []string{"ClusterRole", "ClusterRoleBinding", "ServiceAccount"} {
				option := ""
				for _, item := range menu.Menu.GetOptions() {
					if item.ProgramId == "remove-unexplained" {
						for _, binding := range item.Bindings {
							if binding.GetObject().GetKind() == kind {
								option = item.OptionId
							}
						}
					}
				}
				if option == "" {
					t.Fatalf("no removal option for %s", kind)
				}
				response := cleanupTool(t, tools, "mitigate", option)
				if denied && response.Report.GetReasonCode() != "authorization_denied" {
					t.Fatalf("denial not typed: %+v", response.Report)
				}
				if !denied && len(response.Report.GetRounds()[0].GetActions()) != 1 {
					t.Fatal("missing exact action receipt")
				}
			}
			result := completedResult(request.IncidentID)
			result.ResponderSessionID = "cleanup-scripted-session"
			publishContinuationResult(t, kube, first, result)
			if denied {
				second := awaitJob(t, kube, IncidentJobName(request.IncidentID+"-continue-1"))
				feedback := ""
				for _, env := range second.Spec.Template.Spec.Containers[0].Env {
					if env.Name == "SDO_RESPONSE_CONTINUATION" {
						feedback = env.Value
					}
				}
				checkURContinuationRenderer(t, feedback)
				var parsed responderFeedback
				if err := json.Unmarshal([]byte(feedback), &parsed); err != nil || len(parsed.StillBroken) != 3 {
					t.Fatalf("continuation lost blocker set: %s", feedback)
				}
				typed := 0
				for _, action := range parsed.AlreadyDone {
					if action.Receipt.FailureReason == "authorization_denied" && !action.Receipt.Success {
						typed++
					}
				}
				if typed != 3 {
					t.Fatalf("continuation lost truthful denials: %+v", parsed.AlreadyDone)
				}
				publishContinuationResult(t, kube, second, result)
			}
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
			final := <-done
			if denied {
				if final.Status != IncidentFailed || final.Error != "authorization_denied_no_progress" || final.ResponseLoopExhausted || final.ResponseLoopsUsed != 2 {
					t.Fatalf("did not stop after one continuation: %+v", final)
				}
			} else if final.Status != IncidentCompleted || len(store.SourceDriftObjects()) != 0 {
				t.Fatalf("cleanup did not verify: %+v", final)
			}
			controller.currentIncidentResult = &final
			controller.responderDone = true
			controller.responderCompletedAt = time.Now().Add(-time.Second)
			controller.history = []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}
			var closed *IncidentClosure
			controller.OnIncidentClosed = func(closure IncidentClosure) { closed = &closure }
			now := time.Now()
			controller.maybeCloseIncident(ctx, now, nil)
			if closed == nil && !denied {
				// A verified success commits once health holds for the window;
				// the terminal denial above must not wait for it.
				later := now.Add(controller.config.ClosureStabilityWindow)
				controller.history = append(controller.history, DetectorEvaluation{DetectorID: "health", Status: DetectorEvaluationClear, EvaluatedAt: later})
				controller.maybeCloseIncident(ctx, later, nil)
			}
			if closed == nil {
				t.Fatal("closure waited despite terminal independently checked inventory")
			}
			if denied && len(closed.FinalDetectorStates) != 0 {
				t.Fatal("failed blocker claimed recovery")
			}
			for _, receipt := range final.RepairActions {
				if strings.HasPrefix(receipt.ActionID, "sdo-program-") && receipt.FailureReason == "authorization_denied" && receipt.Success {
					t.Fatal("denial claimed success")
				}
			}
			jobs, _ := kube.BatchV1().Jobs("demo").List(ctx, metav1.ListOptions{})
			expected := 1
			if denied {
				expected = 2
			}
			if len(jobs.Items) != expected {
				t.Fatalf("created %d jobs, expected %d", len(jobs.Items), expected)
			}
		})
	}
}

// client-go v0.30's dynamic fake drops DeleteOptions. Check them before that
// boundary instead of pretending its reactor enforces Kubernetes preconditions.
type checkedCleanupDynamic struct {
	dynamic.Interface
	t *testing.T
}

func (c checkedCleanupDynamic) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return checkedCleanupResource{NamespaceableResourceInterface: c.Interface.Resource(gvr), t: c.t}
}

type checkedCleanupResource struct {
	dynamic.NamespaceableResourceInterface
	t *testing.T
}

func (c checkedCleanupResource) Namespace(ns string) dynamic.ResourceInterface {
	return checkedCleanupNamespaced{ResourceInterface: c.NamespaceableResourceInterface.Namespace(ns), t: c.t}
}
func (c checkedCleanupResource) Delete(ctx context.Context, name string, options metav1.DeleteOptions, sub ...string) error {
	checkCleanupPreconditions(c.t, options)
	return c.NamespaceableResourceInterface.Delete(ctx, name, options, sub...)
}

type checkedCleanupNamespaced struct {
	dynamic.ResourceInterface
	t *testing.T
}

func (c checkedCleanupNamespaced) Delete(ctx context.Context, name string, options metav1.DeleteOptions, sub ...string) error {
	checkCleanupPreconditions(c.t, options)
	return c.ResourceInterface.Delete(ctx, name, options, sub...)
}
func checkCleanupPreconditions(t *testing.T, options metav1.DeleteOptions) {
	t.Helper()
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != "injected-uid" || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != "7" {
		t.Error("delete lacked exact UID/RV")
	}
}

func cleanupTool(t *testing.T, directory string, args ...string) ToolResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "uv", append([]string{"run", "python", "-m", "sdo.agents.responder.tools_cli", "--json"}, args...)...)
	command.Dir = filepath.Join("..", "..", "..")
	command.Env = append(os.Environ(), "SDO_TOOLS_DIR="+directory)
	raw, err := command.Output()
	if err != nil {
		t.Fatalf("scripted CLI %v: %v %s", args, err, raw)
	}
	var pointer struct {
		DetailsFile string `json:"details_file"`
	}
	if json.Unmarshal(raw, &pointer) == nil && pointer.DetailsFile != "" {
		raw, err = os.ReadFile(pointer.DetailsFile)
		if err != nil {
			t.Fatal(err)
		}
		raw = fullCommandReportPayload(t, raw)
		t.Cleanup(func() { _ = os.Remove(pointer.DetailsFile) })
	}
	var response ToolResponse
	var envelope map[string]json.RawMessage
	if err = json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if menu := envelope["menu"]; len(menu) > 0 {
		response.Menu = &contracts.MitigationMenu{}
		if err = (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(menu, response.Menu); err != nil {
			t.Fatal(err)
		}
	}
	if report := envelope["report"]; len(report) > 0 {
		response.Report = &contracts.MitigationReport{}
		if err = (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(report, response.Report); err != nil {
			t.Fatal(err)
		}
	}
	return response
}

// Exercise CC's merged renderer on UR's actual persisted transport payload.
func checkURContinuationRenderer(t *testing.T, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	script := `import sys
from sdo.agents.responder.continuation import continuation_prompt
prompt = continuation_prompt(sys.stdin.read())
broken = prompt.split("still_broken:", 1)[1].split("already_done:", 1)[0]
assert broken.count("authorization_denied") == 3, prompt
assert broken.count("failed") == 3, prompt
assert len(prompt) <= 1500, len(prompt)
print("UR+CC actual Go payload: three typed denials under still_broken, <=1500 chars")
`
	command := exec.CommandContext(ctx, "uv", "run", "python", "-c", script)
	command.Dir = filepath.Join("..", "..", "..")
	command.Stdin = strings.NewReader(payload)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("CC renderer rejected UR payload: %v %s", err, output)
	}
	t.Log(strings.TrimSpace(string(output)))
}
