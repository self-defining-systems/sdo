package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
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

// After ReconcileFromSource restores a StatefulSet's template, a Pod still on
// another revision and not Ready is deleted so the controller recreates it at
// the restored revision (Kubernetes "forced rollback"); nothing else is.
func TestReconcileFromSourceReplacesStatefulSetPodsStuckOnTheBadRevision(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, test := range []struct {
		name        string
		mutate      func(*appsv1.StatefulSet)
		pods        []stuckPod
		wantDeleted []string
	}{
		{"image pull failure", nil,
			[]stuckPod{{name: "store-0", revision: "store-old", waiting: "ImagePullBackOff", notReadyFor: time.Second}}, []string{"store-0"}},
		{"only the stuck Pods, in ordinal order, never a Ready, new-revision or briefly unready Pod", nil,
			[]stuckPod{
				{name: "store-10", revision: "store-old", waiting: "CrashLoopBackOff", notReadyFor: time.Second},
				{name: "store-2", revision: "store-old", waiting: "ErrImagePull", notReadyFor: time.Second},
				{name: "store-1", revision: "store-old", ready: true},
				{name: "store-3", revision: "store-new", waiting: "ImagePullBackOff"},
				{name: "store-4", revision: "store-old", notReadyFor: time.Second},
				{name: "store-5", revision: "store-old", notReadyFor: 2 * time.Minute},
			}, []string{"store-2", "store-5", "store-10"}},
		{"Pods below a rolling-update partition stay", func(set *appsv1.StatefulSet) {
			partition := int32(1)
			set.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &partition}
		}, []stuckPod{{name: "store-0", revision: "store-old", waiting: "ImagePullBackOff"}, {name: "store-1", revision: "store-old", waiting: "ImagePullBackOff"}}, []string{"store-1"}},
		{"a Parallel StatefulSet replaces its own Pods", func(set *appsv1.StatefulSet) { set.Spec.PodManagementPolicy = appsv1.ParallelPodManagement },
			[]stuckPod{{name: "store-0", revision: "store-old", waiting: "ImagePullBackOff"}}, nil},
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
