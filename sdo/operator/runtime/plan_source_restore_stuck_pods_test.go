package runtime

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// templateSource serves one accepted StatefulSet manifest.
type templateSource struct{ manifest *unstructured.Unstructured }

func (s templateSource) AcceptedManifest(context.Context, string, sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.manifest.DeepCopy(), nil
}
func (templateSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, nil
}

func syntheticSet(image string, mutate func(*appsv1.StatefulSet)) *appsv1.StatefulSet {
	template := restartTemplate()
	template.Spec.Containers[0].Image = image
	set := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "store", UID: types.UID("uid-store"), ResourceVersion: "1"},
		Spec:   appsv1.StatefulSetSpec{Template: template},
		Status: appsv1.StatefulSetStatus{UpdateRevision: "store-new", CurrentRevision: "store-old"}}
	set.Kind, set.APIVersion = "StatefulSet", "apps/v1"
	if mutate != nil {
		mutate(set)
	}
	return set
}

type stuckPod struct {
	name, revision, waiting string
	ready                   bool
	notReadyFor             time.Duration
}

func syntheticStorePod(owner *appsv1.StatefulSet, spec stuckPod, now time.Time) *corev1.Pod {
	pod := restartPod(owner, spec.name, spec.ready)
	pod.Labels = map[string]string{"controller-revision-hash": spec.revision}
	pod.Status.Conditions[0].LastTransitionTime = metav1.NewTime(now.Add(-spec.notReadyFor))
	if spec.waiting != "" {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: spec.waiting}}}}
	}
	return pod
}

// Restore helps a blocked old revision one Pod at a time and leaves healthy
// RollingUpdate replicas to Kubernetes.
func TestReconcileFromSourceReplacesStatefulSetPodsOffTheUpdateRevision(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, test := range []struct {
		name        string
		mutate      func(*appsv1.StatefulSet)
		pods        []stuckPod
		wantDeleted []string
	}{
		{"image pull failure", nil,
			[]stuckPod{{name: "store-0", revision: "store-old", waiting: "ImagePullBackOff", notReadyFor: time.Second}}, []string{"store-0"}},
		{"highest blocked ordinal first", func(set *appsv1.StatefulSet) {
			replicas := int32(11)
			set.Spec.Replicas = &replicas
		},
			[]stuckPod{
				{name: "store-10", revision: "store-old", waiting: "CrashLoopBackOff", notReadyFor: time.Second},
				{name: "store-2", revision: "store-old", waiting: "ErrImagePull", notReadyFor: time.Second},
				{name: "store-1", revision: "store-old", ready: true},
				{name: "store-3", revision: "store-new", waiting: "ImagePullBackOff"},
				{name: "store-4", revision: "store-old", notReadyFor: time.Second},
				{name: "store-5", revision: "store-old", notReadyFor: 2 * time.Minute},
			}, []string{"store-10"}},
		{"Pods below a rolling-update partition stay", func(set *appsv1.StatefulSet) {
			replicas, partition := int32(2), int32(1)
			set.Spec.Replicas = &replicas
			set.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &partition}
		}, []stuckPod{{name: "store-0", revision: "store-old", waiting: "ImagePullBackOff"}, {name: "store-1", revision: "store-old", waiting: "ImagePullBackOff"}}, []string{"store-1"}},
		{"parallel policy leaves Ready replicas to Kubernetes", func(set *appsv1.StatefulSet) {
			replicas := int32(2)
			set.Spec.Replicas = &replicas
			set.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		}, []stuckPod{{name: "store-0", revision: "store-old", waiting: "ImagePullBackOff"},
			{name: "store-1", revision: "store-old", ready: true}}, []string{"store-0"}},
		{"OnDelete replaces one Ready replica", func(set *appsv1.StatefulSet) {
			replicas := int32(2)
			set.Spec.Replicas = &replicas
			set.Spec.UpdateStrategy.Type = appsv1.OnDeleteStatefulSetStrategyType
		}, []stuckPod{{name: "store-0", revision: "store-old", ready: true},
			{name: "store-1", revision: "store-old", ready: true}}, []string{"store-1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			live := syntheticSet("registry.invalid/store:bad", test.mutate)
			accepted := syntheticSet("registry.invalid/store:good", test.mutate)
			manifest, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(accepted)
			if err != nil {
				t.Fatal(err)
			}
			objects := []kruntime.Object{live}
			for _, spec := range test.pods {
				objects = append(objects, syntheticStorePod(live, spec, now))
			}
			objects = append(objects, syntheticStorePod(&appsv1.StatefulSet{TypeMeta: metav1.TypeMeta{Kind: "StatefulSet"}, ObjectMeta: metav1.ObjectMeta{Name: "other", UID: "uid-other"}}, stuckPod{name: "other-0", revision: "other-old", waiting: "ImagePullBackOff"}, now))
			client := fake.NewSimpleClientset(objects...)
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "store"}
			executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: templateSource{&unstructured.Unstructured{Object: manifest}},
				Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}, Now: func() time.Time { return now }}
			incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "0123456789abcdef0123456789abcdef01234567",
				Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
			result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
			if err != nil || !result.Applied {
				t.Fatalf("Apply = %+v, %v", result, err)
			}
			got, _ := client.AppsV1().StatefulSets("app").Get(context.Background(), "store", metav1.GetOptions{})
			if image := got.Spec.Template.Spec.Containers[0].Image; image != "registry.invalid/store:good" {
				t.Fatalf("template image %q was not restored", image)
			}
			var deleted []string
			for _, effect := range result.ObjectEffects {
				if effect.Operation != "delete" || effect.Ref.Kind != "Pod" {
					continue
				}
				if !effect.Confirmed || !effect.Irreversible || !strings.Contains(effect.Reason, "revision") {
					t.Fatalf("receipt %+v must be a confirmed delete whose reason names the revision", effect)
				}
				deleted = append(deleted, effect.Ref.Name)
			}
			if strings.Join(deleted, ",") != strings.Join(test.wantDeleted, ",") {
				t.Fatalf("deleted %v, want %v", deleted, test.wantDeleted)
			}
			remaining, _ := client.CoreV1().Pods("app").List(context.Background(), metav1.ListOptions{})
			names := map[string]bool{}
			for _, pod := range remaining.Items {
				names[pod.Name] = true
			}
			for _, name := range test.wantDeleted {
				if names[name] {
					t.Fatalf("Pod %s still exists", name)
				}
			}
			if !names["other-0"] {
				t.Fatal("a Pod of another workload was deleted")
			}
			if len(remaining.Items) != len(objects)-1-len(test.wantDeleted) {
				t.Fatalf("%d Pods remain, want only the stuck ones gone", len(remaining.Items))
			}
		})
	}
}

func TestReconcileFromSourceOfADeploymentDeletesNoPods(t *testing.T) {
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web", UID: "uid-web", ResourceVersion: "1"}, Spec: appsv1.DeploymentSpec{Template: restartTemplate()}}
	deployment.Kind, deployment.APIVersion = "Deployment", "apps/v1"
	accepted := deployment.DeepCopy()
	accepted.Spec.Template.Spec.Containers[0].Image = "registry.invalid/web:good"
	manifest, _ := kruntime.DefaultUnstructuredConverter.ToUnstructured(accepted)
	pod := restartPod(deployment, "web-a", false)
	client := fake.NewSimpleClientset(deployment, pod)
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: templateSource{&unstructured.Unstructured{Object: manifest}},
		Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}}
	incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "0123456789abcdef0123456789abcdef01234567",
		Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
	result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
	if err != nil || len(result.ObjectEffects) != 0 {
		t.Fatalf("Apply = %+v, %v", result, err)
	}
}

func TestStatefulSetSourceRestoreWithUnchangedTemplateKeepsPods(t *testing.T) {
	now := verifyT0
	live := syntheticSet("registry.invalid/store:good", nil)
	accepted := live.DeepCopy()
	replicas := int32(2)
	accepted.Spec.Replicas = &replicas
	pod := syntheticStorePod(live, stuckPod{name: "store-0", revision: "store-old", ready: true}, now)
	client := fake.NewSimpleClientset(live, pod)
	manifest, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(accepted)
	if err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "store"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: templateSource{&unstructured.Unstructured{Object: manifest}},
		Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}, Now: func() time.Time { return now }}
	incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "0123456789abcdef0123456789abcdef01234567",
		Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
	result, err := executor.Apply(t.Context(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
	if err != nil || !result.Applied {
		t.Fatalf("Apply = %+v, %v", result, err)
	}
	if _, err := client.CoreV1().Pods("app").Get(t.Context(), pod.Name, metav1.GetOptions{}); err != nil || len(result.ObjectEffects) != 0 {
		t.Fatalf("a restore with an unchanged template replaced a Pod: effects=%v error=%v", result.ObjectEffects, err)
	}
}

func TestAutomaticStatefulSetRestoreContinuesAfterControllerObservation(t *testing.T) {
	testAutomaticStatefulSetRestore(t, false)
}

func TestRecoveredIncidentClosesWithAnUnavailableRestoreReplacement(t *testing.T) {
	testAutomaticStatefulSetRestore(t, true)
}

func testAutomaticStatefulSetRestore(t *testing.T, replacementUnavailable bool) {
	t.Helper()
	ctx := t.Context()
	tc := settlingCase{}
	if replacementUnavailable {
		tc.healAfter = 22 * time.Second
	}
	c, world, closed := automaticRecoveryWorld(t, tc, "agent")
	now := world.clock.Now()
	sets, pods := world.client.AppsV1().StatefulSets("app"), world.client.CoreV1().Pods("app")
	live := syntheticSet("registry.invalid/store:bad", func(set *appsv1.StatefulSet) {
		replicas := int32(2)
		set.Spec.Replicas = &replicas
		set.Generation, set.Status.ObservedGeneration = 2, 1
	})
	accepted := live.DeepCopy()
	accepted.Spec.Template.Spec.Containers[0].Image = "registry.invalid/store:good"
	if _, err := sets.Create(ctx, live, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"store-0", "store-1"} {
		pod := syntheticStorePod(live, stuckPod{name: name, revision: "store-old"}, now)
		if _, err := pods.Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(accepted)
	if err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "store"}
	loop := c.responderLoop
	executor := loop.Executor
	executor.Source = templateSource{&unstructured.Unstructured{Object: manifest}}
	executor.Rollouts = recordedRollouts{new([]sdk.ObjectRef)}
	result, err := executor.Apply(ctx, *c.currentIncidentRequest,
		program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
	if err != nil || len(result.ObjectEffects) != 0 {
		t.Fatalf("apply before observed generation: effects=%v error=%v", result.ObjectEffects, err)
	}
	// Model the automatic stage publishing the template write as its outcome.
	app := loop.applicationsLog[0]
	app.Result = result
	loop.applicationsLog[0] = app
	c.currentIncidentResult.PlanApplications = []RecordedPlanApplication{app}
	receipt, err := recordedPlanContract(app)
	if err != nil {
		t.Fatal(err)
	}
	attempt := autoReport(c).GetMitigations()[0]
	attempt.ApplyReceipt = receipt
	attempt.TouchedObjects = touchedObjects(result)

	present := func(name string, want bool) {
		t.Helper()
		_, err := pods.Get(ctx, name, metav1.GetOptions{})
		if want && err != nil || !want && !apierrors.IsNotFound(err) {
			t.Fatalf("Pod %s present want %v, lookup error %v", name, want, err)
		}
	}
	autoStep(t, c, world.clock, 20*time.Second)
	present("store-1", true)
	observed, err := sets.Get(ctx, live.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	observed.Status.ObservedGeneration = observed.Generation
	if _, err := sets.UpdateStatus(ctx, observed, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	autoStep(t, c, world.clock, time.Second)
	present("store-1", false)
	present("store-0", true)
	autoStep(t, c, world.clock, time.Second)
	present("store-0", true)
	replacement := syntheticStorePod(live, stuckPod{name: "store-1", revision: "store-new", ready: !replacementUnavailable}, world.clock.Now())
	replacement.UID = "replacement"
	if replacementUnavailable {
		replacement.Status.Phase = corev1.PodPending
		replacement.Status.Conditions = append(replacement.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
		})
	}
	if _, err := pods.Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if replacementUnavailable {
		observed.Status.UpdatedReplicas = 1 // Kubernetes has created the new revision, although it cannot schedule.
		if _, err := sets.UpdateStatus(ctx, observed, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		for range 7 {
			autoStep(t, c, world.clock, time.Second)
		}
		present("store-0", true)
		present("store-1", true)
		if closed.Result == nil || !closed.EscalatedAt.IsZero() {
			t.Fatalf("an unavailable replacement held a recovered incident: open=%v stable since %v now %v escalated at %v", c.IncidentOpen(), c.closureStableSince, world.clock.Now(), closed.EscalatedAt)
		}
		report := deterministicStageReport(closed.Request.PriorStages)
		effects := report.GetMitigations()[0].GetApplyReceipt().GetObjectEffects()
		if !strings.Contains(report.GetError(), "StatefulSet restore continuation") || !slices.ContainsFunc(effects, func(effect *contracts.PlanObjectEffect) bool {
			return effect.GetOperation() == "delete" && effect.GetRef().GetName() == "store-1"
		}) {
			t.Fatalf("closure lost unfinished restore evidence: %v", report)
		}
		return
	}
	autoStep(t, c, world.clock, time.Second)
	present("store-0", false)
	present("store-1", true)
	effects := autoReport(c).GetMitigations()[0].GetApplyReceipt().GetObjectEffects()
	if len(effects) != 2 || effects[0].GetRef().GetName() != "store-1" || effects[1].GetRef().GetName() != "store-0" {
		t.Fatalf("application did not retain ordered replacement receipts: %v", effects)
	}
}
