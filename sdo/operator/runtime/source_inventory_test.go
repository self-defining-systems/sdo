package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func inventoryObject(api, kind, namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": api, "kind": kind, "metadata": map[string]any{"namespace": namespace, "name": name}, "spec": map[string]any{}}}
}
func inventoryChild(api, kind, name, parentKind, parent string) *unstructured.Unstructured {
	o := inventoryObject(api, kind, "app", name)
	ownerAPI := "apps/v1"
	if parentKind == "Job" {
		ownerAPI = "batch/v1"
	}
	_ = unstructured.SetNestedSlice(o.Object, []any{map[string]any{"apiVersion": ownerAPI, "kind": parentKind, "name": parent, "uid": "owner"}}, "metadata", "ownerReferences")
	return o
}
func hotelInventory() ([]*unstructured.Unstructured, []*unstructured.Unstructured) {
	declared := []*unstructured.Unstructured{}
	live := []*unstructured.Unstructured{}
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("service-%02d", i)
		deployment := inventoryObject("apps/v1", "Deployment", "app", name)
		_ = unstructured.SetNestedMap(deployment.Object, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "example.test/app:v1"}}}}}, "spec")
		service := inventoryObject("v1", "Service", "app", name)
		config := inventoryObject("v1", "ConfigMap", "app", name)
		declared = append(declared, deployment, service, config)
		live = append(live, deployment.DeepCopy(), service.DeepCopy(), config.DeepCopy(),
			inventoryChild("apps/v1", "ReplicaSet", name+"-rs", "Deployment", name), inventoryChild("v1", "Pod", name+"-pod", "ReplicaSet", name+"-rs"),
			inventoryObject("v1", "Endpoints", "app", name), inventoryObject("discovery.k8s.io/v1", "EndpointSlice", "app", name+"-eps"), inventoryObject("v1", "Event", "app", name+"-event"))
	}
	for _, name := range []string{"sdo-controller", "sdo-responder-1", "sdo-reflection-1", "sdo-validate-1"} {
		live = append(live, inventoryObject("batch/v1", "Job", "app", name), inventoryChild("v1", "Pod", name+"-pod", "Job", name))
	}
	managed := inventoryObject("apps/v1", "Deployment", "app", "operator-worker")
	managed.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "sdo"})
	helper := inventoryObject("v1", "Pod", "app", "read-only-inspection")
	helper.SetLabels(map[string]string{"sdo.dev/responder-helper": "true"})
	live = append(live, helper)
	live = append(live, managed, inventoryObject("v1", "ConfigMap", "app", "kube-root-ca.crt"), inventoryObject("v1", "ServiceAccount", "app", "default"))
	return declared, live
}
func latentInventoryObjects() []*unstructured.Unstructured {
	controller := inventoryObject("apps/v1", "Deployment", "app", "extra-controller")
	script := inventoryObject("v1", "ConfigMap", "app", "controller-script")
	token := inventoryObject("v1", "ConfigMap", "app", "cleanup-token")
	_ = unstructured.SetNestedField(token.Object, "2026-10-25T01:00:00Z", "metadata", "deletionTimestamp")
	token.SetFinalizers([]string{"example.test/cleanup"})
	role := inventoryObject("rbac.authorization.k8s.io/v1", "ClusterRole", "", "cleanup-authority")
	delete(role.Object, "spec")
	role.Object["rules"] = []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get"}}}
	binding := inventoryObject("rbac.authorization.k8s.io/v1", "ClusterRoleBinding", "", "cleanup-authority")
	delete(binding.Object, "spec")
	binding.Object["subjects"] = []any{map[string]any{"kind": "ServiceAccount", "namespace": "app", "name": "cleanup"}}
	binding.Object["roleRef"] = map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cleanup-authority"}
	return []*unstructured.Unstructured{controller, script, token, role, binding}
}
func TestSourceInventoryCleanHotelAndGeneratedObjects(t *testing.T) {
	declared, live := hotelInventory()
	index := NewSourceInventory("app", "accepted", declared)
	for _, o := range live {
		o.Object["status"] = map[string]any{"readyReplicas": int64(1)}
		o.SetResourceVersion("changed")
	}
	if findings := index.Findings(live, time.Unix(100, 0)); len(findings) != 0 {
		t.Fatalf("clean hotel created response work: %+v", findings)
	}
}
func TestSourceInventorySeesLatentControllerConfigDeletionAndBoundRBAC(t *testing.T) {
	declared, live := hotelInventory()
	index := NewSourceInventory("app", "accepted", declared)
	live = append(live, latentInventoryObjects()...)
	unrelated := latentInventoryObjects()
	unrelated[4].Object["subjects"] = []any{map[string]any{"kind": "ServiceAccount", "namespace": "other", "name": "cleanup"}}
	unrelated[3].SetName("unrelated")
	unrelated[4].SetName("unrelated")
	unrelated[4].Object["roleRef"].(map[string]any)["name"] = "unrelated"
	live = append(live, unrelated[3:]...)
	findings := index.Findings(live, time.Unix(100, 0))
	if len(findings) != 5 {
		t.Fatalf("latent findings=%d want5: %+v", len(findings), findings)
	}
	for _, f := range findings {
		if f.PrimaryResource.Name == "unrelated" {
			t.Fatal("foreign RBAC was admitted")
		}
	}
	if !strings.Contains(findings[0].Evidence, "accepted") {
		t.Fatal("source revision omitted")
	}
}
func TestSourceInventoryDeletionPendingDeclaredObject(t *testing.T) {
	object := inventoryObject("v1", "ConfigMap", "app", "token")
	index := NewSourceInventory("app", "accepted", []*unstructured.Unstructured{object})
	deleting := object.DeepCopy()
	_ = unstructured.SetNestedField(deleting.Object, "2026-10-25T01:00:00Z", "metadata", "deletionTimestamp")
	deleting.SetFinalizers([]string{"example.test/pending"})
	findings := index.Findings([]*unstructured.Unstructured{deleting}, time.Unix(100, 0))
	if len(findings) != 1 || inventoryEvent(findings[0]).Source != "deletion pending relative to accepted source" {
		t.Fatalf("declared deletion invisible: %+v", findings)
	}
}

type inventoryStore struct{ objects []*unstructured.Unstructured }

func (s *inventoryStore) SourceDriftObjects() []*unstructured.Unstructured { return s.objects }
func (s *inventoryStore) TakeObjectChanges() []ObjectChange                { return nil }

type inventorySource struct {
	objects []*unstructured.Unstructured
	calls   int
	err     error
}

func (s *inventorySource) AcceptedManifests(context.Context, string, string) ([]*unstructured.Unstructured, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.objects, nil
}
func inventoryHealth() sdk.Detector {
	health := controllerDetector("health", time.Second, sdk.Finding{})
	health.spec.Class = sdk.DetectorClassHealth
	health.spec.Owner = sdk.DetectorOwnerHealthJudge
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	health.spec.OriginatingCommit = "health-objective"
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	return health
}
func acceptInventoryFindings(findings []sdk.Finding) []sdk.DriftEvent {
	labels := []sdk.DriftEvent{}
	for _, finding := range findings {
		event := inventoryEvent(finding)
		event.InvestigationVerdict = "benign"
		event.InvestigationReason = "The operator explicitly retains this independently owned resource"
		event.InvestigationEvidence = []sdk.DriftEvidence{{Kind: "observed_impact", Observation: "Read-only inspection establishes this object's intended independent ownership"}}
		labels = append(labels, event)
	}
	return labels
}
func TestLatentAllHealthClearClosureRequiresRemovalOrRecordedAcceptance(t *testing.T) {
	for _, mode := range []string{"removed", "accepted"} {
		t.Run(mode, func(t *testing.T) {
			start := time.Unix(100, 0).UTC()
			config := testControllerConfig()
			config.Namespace = "app"
			config.VerificationTimeout = time.Minute
			declared, live := hotelInventory()
			store := &inventoryStore{objects: append(live, latentInventoryObjects()...)}
			source := &inventorySource{objects: declared}
			dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
			c, err := NewController(config, []sdk.Detector{inventoryHealth()}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, dispatcher, start)
			if err != nil {
				t.Fatal(err)
			}
			c.ConfigureDrift(DriftControllerConfig{Inventory: true, Events: store, Source: source, BatchWindow: time.Nanosecond})
			closed := []IncidentClosure{}
			c.OnIncidentClosed = func(closure IncidentClosure) { closed = append(closed, closure) }
			step := func(n int) {
				t.Helper()
				if err := c.Step(context.Background(), start.Add(time.Duration(n)*time.Second), nil); err != nil {
					t.Fatal(err)
				}
			}
			step(0)
			step(1)
			executePendingEffect(t, c)
			request := awaitRequest(t, dispatcher.requests)
			awaitDispatchCompletionQueued(t, c)
			step(2)
			if len(request.Findings) != 5 || !c.IncidentOpen() || len(closed) != 0 {
				t.Fatalf("health clear hid latent work: request=%+v closed=%+v", request, closed)
			}
			check := c.responderVerification(context.Background(), request.IncidentID)
			if check.Healthy || len(check.Findings) != 5 {
				t.Fatalf("continuation lost latent drift: %+v", check)
			}
			if mode == "removed" {
				store.objects = live
			} else {
				c.currentIncidentResult.DriftEvents = acceptInventoryFindings(c.sourceInventoryFindings)
				// Empty reasons cannot authorize closure.
				saved := c.currentIncidentResult.DriftEvents[0].InvestigationReason
				c.currentIncidentResult.DriftEvents[0].InvestigationReason = ""
				if c.responderVerification(context.Background(), request.IncidentID).Healthy {
					t.Fatal("empty acceptance reason authorized closure")
				}
				c.currentIncidentResult.DriftEvents[0].InvestigationReason = saved
			}
			step(3)
			if len(closed) != 1 || len(closed[0].FinalDetectorStates) != 1 {
				t.Fatalf("resolved latent work did not verify: %+v", closed)
			}
			if source.calls != 1 {
				t.Fatalf("closure reread accepted source: calls=%d", source.calls)
			}
			if mode == "accepted" {
				record, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closed[0], "", "codex", "fixture")
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range record.Record.GetDriftEvents() {
					if event.GetInvestigationReason() == "" {
						t.Fatal("outcome lost explicit acceptance reason")
					}
				}
				if recheck := c.revalidateClosure(context.Background(), sdktest.Snapshot{NamespaceName: "app"}, closed[0]); recheck.Verdict != closureRecheckConfirmed {
					t.Fatalf("broker rejected the same verified acceptance at handoff: %+v", recheck)
				}
				state := exportState(t, c)
				restored, err := NewController(config, []sdk.Detector{inventoryHealth()}, c.provider, dispatcher, start)
				if err != nil {
					t.Fatal(err)
				}
				restored.ConfigureDrift(DriftControllerConfig{Inventory: true, Events: store, Source: source, BatchWindow: time.Nanosecond})
				if err := restored.RestoreState(state); err != nil {
					t.Fatal(err)
				}
				if err := restored.Step(context.Background(), start.Add(4*time.Second), nil); err != nil {
					t.Fatal(err)
				}
				if restored.IncidentOpen() || !restored.tracker.Quiet() {
					t.Fatal("unchanged accepted drift reopened after restart")
				}
				// Publishing memory alone keeps accepted application-source identity.
				restored.config.SourceCommit = "memory-only-advance"
				if err := restored.Step(context.Background(), start.Add(5*time.Second), nil); err != nil {
					t.Fatal(err)
				}
				if restored.IncidentOpen() || !restored.tracker.Quiet() {
					t.Fatal("memory-only publication reopened accepted inventory")
				}
				changed := store.objects[len(store.objects)-5].DeepCopy()
				changed.Object["spec"] = map[string]any{"replicas": int64(0)}
				store.objects[len(store.objects)-5] = changed
				if err := restored.Step(context.Background(), start.Add(6*time.Second), nil); err != nil {
					t.Fatal(err)
				}
				if restored.tracker.Quiet() {
					t.Fatal("acceptance survived a new object configuration")
				}
			}
		})
	}
}

func BenchmarkSourceInventoryHotel(b *testing.B) {
	declared, live := hotelInventory()
	index := NewSourceInventory("app", "accepted", declared)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(index.Findings(live, time.Unix(100, 0))) != 0 {
			b.Fatal("false positives")
		}
	}
}

func TestSourceInventoryDeclaredRBACChangeAndEquivalentOrdering(t *testing.T) {
	role := latentInventoryObjects()[3]
	binding := latentInventoryObjects()[4]
	role.Object["rules"] = []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps", "configmaps/finalizers"}, "verbs": []any{"patch", "update"}}}
	index := NewSourceInventory("app", "accepted", []*unstructured.Unstructured{role, binding})
	equivalent := role.DeepCopy()
	equivalent.Object["rules"] = []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps/finalizers", "configmaps"}, "verbs": []any{"update", "patch"}}}
	if findings := index.Findings([]*unstructured.Unstructured{equivalent, binding}, time.Unix(100, 0)); len(findings) != 0 {
		t.Fatalf("RBAC ordering caused a response: %+v", findings)
	}
	broken := role.DeepCopy()
	broken.Object["rules"] = []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get"}}}
	if findings := index.Findings([]*unstructured.Unstructured{broken, binding}, time.Unix(100, 0)); len(findings) != 1 || inventoryEvent(findings[0]).Source != "RBAC differs from accepted source" {
		t.Fatalf("lost source RBAC authorization: %+v", findings)
	}
}
func TestSourceInventoryIdentityIgnoresMemoryOnlyCommit(t *testing.T) {
	declared, _ := hotelInventory()
	before, after := NewSourceInventory("app", "before", declared), NewSourceInventory("app", "after", declared)
	if before.Identity != after.Identity || before.Revision == after.Revision {
		t.Fatal("memory-only commit invalidates source identity")
	}
	changed := declared[0].DeepCopy()
	changed.Object["spec"] = map[string]any{"replicas": int64(0)}
	altered := append([]*unstructured.Unstructured(nil), declared...)
	altered[0] = changed
	if NewSourceInventory("app", "after", altered).Identity == before.Identity {
		t.Fatal("application source change retained identity")
	}
}

func TestSourceInventoryCacheOnlyExposesNamespaceBoundClusterRBAC(t *testing.T) {
	c, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	objects := latentInventoryObjects()
	for _, o := range objects[3:] {
		if err := c.informers[o.GetAPIVersion()+"/"+o.GetKind()].GetStore().Add(o); err != nil {
			t.Fatal(err)
		}
	}
	unrelated := objects[3].DeepCopy()
	unrelated.SetName("other-namespace-role")
	if err := c.informers[unrelated.GetAPIVersion()+"/"+unrelated.GetKind()].GetStore().Add(unrelated); err != nil {
		t.Fatal(err)
	}
	got := c.SourceDriftObjects()
	if len(got) != 2 {
		t.Fatalf("inventory exposed unrelated cluster authorization: %+v", got)
	}
	for _, o := range got {
		if o.GetName() == unrelated.GetName() {
			t.Fatal("unrelated cluster role leaked into app inventory")
		}
	}
}

func TestSourceInventoryMissingAuthorityPreservesMonitoringButNeverVerifies(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	config := testControllerConfig()
	config.Namespace = "app"
	c, err := NewController(config, []sdk.Detector{inventoryHealth()}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{Inventory: true, Events: &inventoryStore{}, Source: &inventorySource{err: errors.New("accepted deployment evidence unavailable")}})
	if err := c.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("source unavailability stopped independent health observation: %v", err)
	}
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "unverified", Namespace: "app", ResponseDeadline: start.Add(time.Minute)}
	c.incidentOpen = true
	c.responderDone = true
	c.responderCompletedAt = start
	if check := c.responderVerification(context.Background(), "unverified"); check.Healthy || len(check.Findings) != 1 || !strings.Contains(check.Findings[0].Evidence, "unavailable") {
		t.Fatalf("missing source authority became clear: %+v", check)
	}
	closed := false
	c.OnIncidentClosed = func(IncidentClosure) { closed = true }
	c.maybeCloseIncident(context.Background(), start.Add(time.Second), sdktest.Snapshot{NamespaceName: "app"})
	if closed {
		t.Fatal("health clearance verified without accepted source authority")
	}
}
func TestSourceInventoryAcceptanceBindsObjectIncarnationAndOwnership(t *testing.T) {
	object := inventoryObject("apps/v1", "Deployment", "app", "extra")
	object.SetUID("first-incarnation")
	index := NewSourceInventory("app", "accepted", nil)
	original := index.Findings([]*unstructured.Unstructured{object}, time.Unix(100, 0))
	accepted := acceptInventoryFindings(original)
	for _, change := range []string{"new-incarnation", "new-owner"} {
		t.Run(change, func(t *testing.T) {
			current := object.DeepCopy()
			if change == "new-incarnation" {
				current.SetUID("second-incarnation")
			} else {
				_ = unstructured.SetNestedSlice(current.Object, []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "another-controller", "uid": "another"}}, "metadata", "ownerReferences")
			}
			findings := index.Findings([]*unstructured.Unstructured{current}, time.Unix(101, 0))
			if len(findings) != 1 || checkedInventoryAcceptance(findings[0], accepted) {
				t.Fatalf("accepted a different ownership/incarnation: %+v", findings)
			}
		})
	}
}

func TestSourceInventoryRBACDefaultsAreNotDrift(t *testing.T) {
	role := inventoryObject("rbac.authorization.k8s.io/v1", "Role", "app", "reader")
	delete(role.Object, "spec")
	role.Object["rules"] = []any{}
	binding := inventoryObject("rbac.authorization.k8s.io/v1", "RoleBinding", "app", "reader")
	delete(binding.Object, "spec")
	binding.Object["roleRef"] = map[string]any{"kind": "Role", "name": "reader"}
	binding.Object["subjects"] = []any{map[string]any{"kind": "ServiceAccount", "name": "api"}}
	index := NewSourceInventory("app", "accepted", []*unstructured.Unstructured{role, binding})
	liveRole := role.DeepCopy()
	liveRole.Object["rules"] = nil
	liveBinding := binding.DeepCopy()
	liveBinding.Object["roleRef"].(map[string]any)["apiGroup"] = "rbac.authorization.k8s.io"
	liveBinding.Object["subjects"] = []any{map[string]any{"kind": "ServiceAccount", "name": "api", "namespace": "app"}}
	if findings := index.Findings([]*unstructured.Unstructured{liveRole, liveBinding}, time.Unix(100, 0)); len(findings) != 0 {
		t.Fatalf("API-defaulted authorization caused an extra response: %+v", findings)
	}
}

func TestSourceInventoryRBACSubjectAPIGroupDefaultsAreNotDrift(t *testing.T) {
	for _, kind := range []string{"User", "Group", "ServiceAccount"} {
		t.Run(kind, func(t *testing.T) {
			binding := inventoryObject("rbac.authorization.k8s.io/v1", "RoleBinding", "app", "reader")
			delete(binding.Object, "spec")
			binding.Object["roleRef"] = map[string]any{"kind": "Role", "name": "reader"}
			binding.Object["subjects"] = []any{map[string]any{"kind": kind, "name": "readers"}}
			live := binding.DeepCopy()
			live.Object["subjects"] = []any{map[string]any{"kind": kind, "name": "readers", "apiGroup": "rbac.authorization.k8s.io"}}
			if kind == "ServiceAccount" {
				binding.Object["subjects"] = []any{map[string]any{"kind": kind, "name": "readers", "apiGroup": ""}}
				live.Object["subjects"] = []any{map[string]any{"kind": kind, "name": "readers"}}
			}
			index := NewSourceInventory("app", "accepted", []*unstructured.Unstructured{binding})
			if findings := index.Findings([]*unstructured.Unstructured{live}, time.Unix(100, 0)); len(findings) != 0 {
				t.Fatalf("defaulted subject API group caused a response: %+v", findings)
			}
		})
	}
}

func TestSourceInventoryHelperLabelDoesNotHideApplicationControllers(t *testing.T) {
	object := inventoryObject("apps/v1", "Deployment", "app", "extra-controller")
	object.SetLabels(map[string]string{"sdo.dev/responder-helper": "true"})
	if findings := NewSourceInventory("app", "accepted", nil).Findings([]*unstructured.Unstructured{object}, time.Unix(100, 0)); len(findings) != 1 {
		t.Fatal("Pod/Job helper label hid a persistent application controller")
	}
}
