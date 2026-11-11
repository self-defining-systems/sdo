package runtime

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func restartPod(owner kruntime.Object, name string, ready bool) *corev1.Pod {
	meta := owner.(metav1.Object)
	kind := owner.GetObjectKind().GroupVersionKind().Kind
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name, UID: types.UID("uid-" + name), ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kind, Name: meta.GetName(), UID: meta.GetUID(), Controller: &controller}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
	return pod
}

// RestartRollout must replace the Pods the workload controller will not
// replace after a template change, and say which and why in its receipt.
func TestRestartRolloutReplacesPodsTheControllerWillNotRoll(t *testing.T) {
	meta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Namespace: "app", Name: name, UID: types.UID("uid-" + name), ResourceVersion: "1"}
	}
	statefulSet := func(policy appsv1.PodManagementPolicyType, strategy appsv1.StatefulSetUpdateStrategyType) *appsv1.StatefulSet {
		set := &appsv1.StatefulSet{ObjectMeta: meta("store"), Spec: appsv1.StatefulSetSpec{Template: restartTemplate(), PodManagementPolicy: policy, UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: strategy}}}
		set.Kind = "StatefulSet"
		return set
	}
	daemonSet := func(strategy appsv1.DaemonSetUpdateStrategyType) *appsv1.DaemonSet {
		set := &appsv1.DaemonSet{ObjectMeta: meta("agent"), Spec: appsv1.DaemonSetSpec{Template: restartTemplate(), UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: strategy}}}
		set.Kind = "DaemonSet"
		return set
	}
	deployment := &appsv1.Deployment{ObjectMeta: meta("web"), Spec: appsv1.DeploymentSpec{Template: restartTemplate()}}
	deployment.Kind = "Deployment"
	for _, test := range []struct {
		name        string
		workload    kruntime.Object
		ready       map[string]bool
		wantDeleted []string
		wantReason  string
	}{
		{"ordered StatefulSet blocked on an unready Pod", statefulSet(appsv1.OrderedReadyPodManagement, appsv1.RollingUpdateStatefulSetStrategyType), map[string]bool{"store-0": false, "store-1": true}, []string{"store-0"}, "not Ready"},
		{"defaulted StatefulSet blocked on an unready Pod", statefulSet("", ""), map[string]bool{"store-0": false}, []string{"store-0"}, "not Ready"},
		{"ordered StatefulSet with every Pod Ready rolls itself", statefulSet(appsv1.OrderedReadyPodManagement, appsv1.RollingUpdateStatefulSetStrategyType), map[string]bool{"store-0": true, "store-1": true}, nil, ""},
		{"parallel StatefulSet rolls unready Pods itself", statefulSet(appsv1.ParallelPodManagement, appsv1.RollingUpdateStatefulSetStrategyType), map[string]bool{"store-0": false}, nil, ""},
		{"OnDelete StatefulSet", statefulSet(appsv1.OrderedReadyPodManagement, appsv1.OnDeleteStatefulSetStrategyType), map[string]bool{"store-0": true, "store-1": false}, []string{"store-0", "store-1"}, "OnDelete"},
		{"OnDelete DaemonSet", daemonSet(appsv1.OnDeleteDaemonSetStrategyType), map[string]bool{"agent-a": true}, []string{"agent-a"}, "OnDelete"},
		{"rolling DaemonSet rolls itself", daemonSet(appsv1.RollingUpdateDaemonSetStrategyType), map[string]bool{"agent-a": false}, nil, ""},
		{"Deployment rolls itself", deployment, map[string]bool{"web-a": false}, nil, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := []kruntime.Object{test.workload}
			for name, ready := range test.ready {
				objects = append(objects, restartPod(test.workload, name, ready))
			}
			// A Pod of another owner in the namespace is never touched.
			stranger := restartPod(&appsv1.StatefulSet{TypeMeta: metav1.TypeMeta{Kind: "StatefulSet"}, ObjectMeta: meta("other")}, "other-0", false)
			objects = append(objects, stranger)
			client := fake.NewSimpleClientset(objects...)
			workload := test.workload.(metav1.Object)
			kind := test.workload.GetObjectKind().GroupVersionKind().Kind
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: kind, Namespace: "app", Name: workload.GetName()}
			executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}, Now: func() time.Time { return time.Unix(0, 0) }}
			incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
			result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.RestartRollout{Ref: ref, Reason: "replace Pods"}}}, false)
			if err != nil || !result.Applied {
				t.Fatalf("Apply = %+v, %v", result, err)
			}
			pods, err := client.CoreV1().Pods("app").List(context.Background(), metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			remaining := map[string]bool{}
			for _, pod := range pods.Items {
				remaining[pod.Name] = true
			}
			if !remaining["other-0"] {
				t.Fatal("a Pod of another workload was deleted")
			}
			var deleted []string
			for name := range test.ready {
				if !remaining[name] {
					deleted = append(deleted, name)
				}
			}
			sort.Strings(deleted)
			if strings.Join(deleted, ",") != strings.Join(test.wantDeleted, ",") {
				t.Fatalf("deleted Pods = %v, want %v", deleted, test.wantDeleted)
			}
			var receipts []string
			for _, effect := range result.ObjectEffects {
				if effect.Operation != "delete" || effect.Ref.Kind != "Pod" {
					continue
				}
				if !effect.Confirmed || effect.State != "applied" || !effect.Irreversible || effect.UID != "uid-"+effect.Ref.Name || !strings.Contains(effect.Reason, test.wantReason) {
					t.Fatalf("Pod delete receipt = %+v, want a confirmed irreversible delete with a reason naming %q", effect, test.wantReason)
				}
				receipts = append(receipts, effect.Ref.Name)
			}
			sort.Strings(receipts)
			if strings.Join(receipts, ",") != strings.Join(test.wantDeleted, ",") {
				t.Fatalf("Pod delete receipts = %v, want %v", receipts, test.wantDeleted)
			}
			// The receipt survives the durable plan contract.
			encoded, err := result.Proto()
			if err != nil {
				t.Fatal(err)
			}
			decoded := planApplicationState(encoded).Result
			var durable []string
			for _, effect := range decoded.ObjectEffects {
				if effect.Ref.Kind == "Pod" && effect.Reason != "" {
					durable = append(durable, effect.Ref.Name)
				}
			}
			sort.Strings(durable)
			if strings.Join(durable, ",") != strings.Join(test.wantDeleted, ",") {
				t.Fatalf("durable Pod delete reasons = %v, want %v", durable, test.wantDeleted)
			}
		})
	}
}
