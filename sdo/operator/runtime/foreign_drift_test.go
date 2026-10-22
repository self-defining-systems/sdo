package runtime

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func foreignPolicy(name string) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
	}}
}

// Use the production accepted-manifest reader: absence must differ from a bad
// revision, unavailable source, or failed rendering, even without an API server.
func foreignSource(t *testing.T) (GitPlanSource, string) {
	t.Helper()
	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "app.yaml"), []byte("apiVersion: v1\nkind: Service\nmetadata:\n  name: api\nspec:\n  selector:\n    app: api\n  ports:\n  - port: 80\n    targetPort: 8080\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}, {"add", "app.yaml"}, {"commit", "-m", "accepted source"}} {
		if out, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatal(string(out), err)
		}
	}
	out, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return GitPlanSource{Repository: repo, ManifestPaths: []string{"app.yaml"}}, strings.TrimSpace(string(out))
}

func TestForeignNetworkPolicyAdmission(t *testing.T) {
	source, commit := foreignSource(t)
	policy := foreignPolicy("deny-api")
	watch := sdk.WatchKind{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app"}
	controlling := true
	derived := policy.DeepCopy()
	derived.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "api", Controller: &controlling}}
	noncontrolling := policy.DeepCopy()
	noncontrolling.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "api"}}
	otherNamespace := policy.DeepCopy()
	otherNamespace.Namespace = "elsewhere"
	unrelated := policy.DeepCopy()
	unrelated.Spec.PodSelector.MatchLabels = map[string]string{"app": "elsewhere"}
	metadataOnly := policy.DeepCopy()
	metadataOnly.Labels = map[string]string{"operator": "changed"}
	modified := policy.DeepCopy()
	modified.Spec.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}
	empty := policy.DeepCopy()
	empty.Spec.PodSelector = metav1.LabelSelector{}
	for _, tc := range []struct {
		name          string
		old, next     *networkingv1.NetworkPolicy
		initial, want bool
	}{
		{"selects app", nil, policy, false, true}, {"unrelated is a question too", nil, unrelated, false, true},
		{"empty selector", nil, empty, false, true}, {"controller derived", nil, derived, false, false},
		{"noncontrolling reference", nil, noncontrolling, false, true}, {"other namespace", nil, otherNamespace, false, false},
		{"metadata only", policy, metadataOnly, false, false}, {"modified spec", policy, modified, false, true},
		{"deleted", policy, nil, false, false}, {"initial list", nil, policy, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api-pod", Namespace: "app", Labels: map[string]string{"app": "api"}}}}}
			if tc.next != nil {
				snapshot.NetworkPolicyList = []networkingv1.NetworkPolicy{*tc.next}
			}
			view := LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: commit}
			var change ObjectChange
			if tc.old != nil {
				change.Old = driftObject(tc.old, watch)
			}
			if tc.next != nil {
				change.New = driftObject(tc.next, watch)
			}
			change.Initial = tc.initial
			event, _, err := (DriftClassifier{}).Classify(context.Background(), change, view, time.Unix(10, 0))
			if err != nil || (event != nil) != tc.want {
				t.Fatalf("event=%+v error=%v; want admitted=%v", event, err, tc.want)
			}
			if tc.want {
				if event.Manager != "unknown" {
					t.Fatalf("invented deployer attribution for foreign object: %+v", event)
				}
				if tc.old == nil && event.Fields[0].Old != "" {
					t.Fatalf("foreign add has an old value: %+v", event)
				}
				if event.Source != "absent from accepted source" || len(event.Fields) != 1 || event.Fields[0].Path != "spec" || event.Fields[0].Source != "" {
					t.Fatalf("missing foreign spec and absence: %+v", event)
				}
				var spec networkingv1.NetworkPolicySpec
				if err := json.Unmarshal([]byte(event.Fields[0].New), &spec); err != nil || len(spec.PolicyTypes) != 1 {
					t.Fatalf("policy spec not readable: %+v %v", event, err)
				}
			}
		})
	}
}

func TestForeignReadViewDiffAndSourceFailures(t *testing.T) {
	source, commit := foreignSource(t)
	policy := foreignPolicy("deny-api")
	ref := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: policy.Name}
	client := fake.NewSimpleClientset(policy)
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", NetworkPolicyList: []networkingv1.NetworkPolicy{*policy}}, Source: source, SourceCommit: commit, Client: client}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || !diff.Changed || !diff.SourceAbsent || !strings.Contains(diff.Text, "accepted: null") || !strings.Contains(diff.Text, "podSelector") {
		t.Fatalf("foreign diff=%+v error=%v", diff, err)
	}
	if len(client.Actions()) != 0 {
		t.Fatalf("foreign read tried Kubernetes writes/dry run: %+v", client.Actions())
	}
	view.DetectionContext = sdktest.Snapshot{NamespaceName: "app"}
	if diff, err := view.DiffAgainstSource(ref); err != nil || diff.Changed || !diff.SourceAbsent {
		t.Fatalf("removed foreign object=%+v error=%v", diff, err)
	}
	view.SourceCommit = "bad-revision"
	if _, err := view.DiffAgainstSource(ref); err == nil {
		t.Fatal("invalid revision interpreted as absence")
	}
	view.Source = nil
	if _, err := view.DiffAgainstSource(ref); err == nil {
		t.Fatal("unavailable source interpreted as absence")
	}
	// Wrong source identity is never absence.
	view.SourceCommit = commit
	view.Source = planFakeSource{source: planObject(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "other"}, nil)}
	if _, err := view.DiffAgainstSource(ref); err == nil {
		t.Fatal("wrong identity interpreted as absence")
	}
}

func TestForeignDriftNormalChurn(t *testing.T) {
	source, commit := foreignSource(t)
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, []sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{ID: "events", Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Event"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	controlling := true
	owner := []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "api", Controller: &controlling}}
	meta := metav1.ObjectMeta{Namespace: "app", Name: "derived", OwnerReferences: owner}
	objects := map[string]any{
		"Event":         &corev1.Event{ObjectMeta: meta, Reason: "Scheduled", Message: "Assigned pod"},
		"ConfigMap":     &corev1.ConfigMap{ObjectMeta: meta, Data: map[string]string{"key": "value"}},
		"Service":       &corev1.Service{ObjectMeta: meta, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1"}},
		"Pod":           &corev1.Pod{ObjectMeta: meta, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app"}}}},
		"Deployment":    &appsv1.Deployment{ObjectMeta: meta},
		"ReplicaSet":    &appsv1.ReplicaSet{ObjectMeta: meta},
		"Endpoints":     &corev1.Endpoints{ObjectMeta: meta},
		"EndpointSlice": &discoveryv1.EndpointSlice{ObjectMeta: meta, AddressType: discoveryv1.AddressTypeIPv4},
		"NetworkPolicy": foreignPolicy("operator-policy"),
	}
	if len(cache.declared) != len(objects) {
		t.Fatalf("watch inventory changed: %+v", cache.declared)
	}
	for _, watch := range cache.declared {
		obj, ok := objects[watch.Kind]
		if !ok {
			t.Fatalf("unexamined watch: %+v", watch)
		}
		if watch.Kind == "NetworkPolicy" {
			obj.(*networkingv1.NetworkPolicy).OwnerReferences = owner
		}
		cache.eventHandler(watch).OnAdd(obj, false)
	}
	snapshot := sdktest.Snapshot{
		NamespaceName:     "app",
		ConfigMapList:     []corev1.ConfigMap{*objects["ConfigMap"].(*corev1.ConfigMap)},
		ServiceList:       []corev1.Service{*objects["Service"].(*corev1.Service)},
		PodList:           []corev1.Pod{*objects["Pod"].(*corev1.Pod)},
		DeploymentList:    []appsv1.Deployment{*objects["Deployment"].(*appsv1.Deployment)},
		ReplicaSetList:    []appsv1.ReplicaSet{*objects["ReplicaSet"].(*appsv1.ReplicaSet)},
		EndpointList:      []corev1.Endpoints{*objects["Endpoints"].(*corev1.Endpoints)},
		EndpointSliceList: []discoveryv1.EndpointSlice{*objects["EndpointSlice"].(*discoveryv1.EndpointSlice)},
		NetworkPolicyList: []networkingv1.NetworkPolicy{*objects["NetworkPolicy"].(*networkingv1.NetworkPolicy)},
		EventList:         []corev1.Event{*objects["Event"].(*corev1.Event)},
	}
	for _, change := range cache.TakeObjectChanges() {
		event, _, err := (DriftClassifier{}).Classify(context.Background(), change, LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: commit}, time.Unix(10, 0))
		t.Logf("%s: event=%v", change.New.GetKind(), event != nil)
		if err != nil || event != nil {
			t.Fatalf("controller churn admitted: %+v %v", event, err)
		}
	}
	// Assert the baseline test still checks non-ownership, not a source failure.
	if _, err := source.AcceptedManifest(context.Background(), commit, sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}); err != nil {
		t.Fatal(err)
	}
}

var _ program.ReadView = LiveReadView{}

func TestHermeticForeignNetworkPolicyOpensLatentIncident(t *testing.T) {
	runForeignPolicyIncident(t)
}

func runForeignPolicyIncident(t *testing.T) (string, string, GitPlanSource, string) {
	t.Helper()
	start := time.Unix(100, 0).UTC()
	root := t.TempDir()
	source, commit := foreignSource(t)
	policy := foreignPolicy("first-deny")
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	watch := sdk.WatchKind{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app"}
	cache.eventHandler(watch).OnAdd(policy, false)
	peer := foreignPolicy("healthy-peer")
	peer.Spec.PodSelector.MatchLabels = map[string]string{"app": "unrelated"}
	provider := &changingProvider{snapshot: policyWorkloadSnapshot(*policy, *peer)}
	config := testControllerConfig()
	config.Namespace, config.SourceCommit, config.CorpusSessionRoot, config.FiringThreshold, config.ClearThreshold = "app", commit, root, 1, 1
	dispatcher := &fixedResultDispatcher{requests: make(chan IncidentRequest, 1), result: func(request IncidentRequest) IncidentResult { return completedResult(request.IncidentID) }}
	controller, err := NewController(config, nil, provider, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(controller.routingSweeps.Wait)
	controller.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: commit}
	}, BatchWindow: time.Second})
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if controller.IncidentOpen() {
		t.Fatal("admission alone opened an incident")
	}
	if err := controller.StepEvents(context.Background(), start.Add(2*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	loop := NewResponderLoop(request, nil, provider, nil, PlanExecutor{Source: source}, 1)
	loop.CorpusSessionRoot = root
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: commit}
	}
	loop.InitialDiagnose(context.Background())
	for _, ref := range []sdk.ObjectRef{
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: policy.Name},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: peer.Name},
		{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"},
	} {
		if response := loop.Handle(context.Background(), ToolRequest{Command: "source", Ref: &ref}); response.Error != "" {
			t.Fatal(response.Error)
		}
	}

	if len(request.Findings) != 1 || request.Findings[0].DetectorID != "drift.unexplained" || request.Findings[0].Severity != sdk.SeverityLatent {
		t.Fatalf("unexpected finding: %+v", request)
	}
	capture, _ := memory.IncidentCorpusDir(root, request.IncidentID)
	data, err := os.ReadFile(filepath.Join(capture, "open@detect.json"))
	if err != nil {
		t.Fatal(err)
	}
	var captured sdktest.Snapshot
	if err := json.Unmarshal(data, &captured); err != nil || len(captured.NetworkPolicyList) != 2 {
		t.Fatalf("corpus lost policy: %s %v", data, err)
	}
	awaitDispatchCompletionQueued(t, controller)
	fixed := provider.snapshot.(sdktest.Snapshot)
	fixed.NetworkPolicyList = []networkingv1.NetworkPolicy{*peer}
	provider.snapshot = fixed
	cache.eventHandler(watch).OnDelete(policy)
	if err := controller.StepEvents(context.Background(), start.Add(3*time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if _, ok := controller.PendingIncidentClosure(); !ok {
		t.Fatal("deleting the foreign policy did not clear the incident")
	}
	if err := memory.CaptureIncidentSnapshot(root, request.IncidentID, "fixed", fixed, "app"); err != nil {
		t.Fatal(err)
	}
	return root, request.IncidentID, source, commit
}

// Read-view absence must not broaden the existing declared-field path to kinds
// outside the reviewed foreign list, even if an object retains old ownership.
func TestForeignNonPolicyWithStaleDeployerOwnershipIsIgnored(t *testing.T) {
	source, commit := foreignSource(t)
	old := driftService(8080)
	old.Name = "source-absent-service"
	next := old.DeepCopy()
	next.Spec.Ports[0].TargetPort = driftService(9999).Spec.Ports[0].TargetPort
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*next}}, Source: source, SourceCommit: commit}
	event, _, err := (DriftClassifier{}).Classify(context.Background(), objectChange(t, old, next, "v1", "Service"), view, time.Unix(10, 0))
	if err != nil || event != nil {
		t.Fatalf("unlisted foreign kind admitted using stale ownership: %+v %v", event, err)
	}
}
