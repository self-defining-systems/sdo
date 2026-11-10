package runtime

import (
	"context"
	"errors"
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

type recordedRollouts struct{ refs *[]sdk.ObjectRef }

func (r recordedRollouts) Wait(_ context.Context, refs []sdk.ObjectRef) error {
	*r.refs = append(*r.refs, refs...)
	return nil
}

type passingVerifier struct{}

func (passingVerifier) Verify(context.Context) (bool, error) { return true, nil }

func restartTemplate() corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "synthetic"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example/synthetic:1"}}},
	}
}

func TestRestartRolloutStampsPodTemplateOfEveryRestartableWorkload(t *testing.T) {
	meta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Namespace: "app", Name: name, UID: types.UID("uid-" + name), ResourceVersion: "1"}
	}
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, test := range []struct {
		kind   string
		object kruntime.Object
		read   func(*fake.Clientset) (map[string]string, error)
	}{
		{"Deployment", &appsv1.Deployment{ObjectMeta: meta("web"), Spec: appsv1.DeploymentSpec{Template: restartTemplate()}}, func(c *fake.Clientset) (map[string]string, error) {
			o, err := c.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			return o.Spec.Template.Annotations, nil
		}},
		{"StatefulSet", &appsv1.StatefulSet{ObjectMeta: meta("store"), Spec: appsv1.StatefulSetSpec{Template: restartTemplate()}}, func(c *fake.Clientset) (map[string]string, error) {
			o, err := c.AppsV1().StatefulSets("app").Get(context.Background(), "store", metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			return o.Spec.Template.Annotations, nil
		}},
		{"DaemonSet", &appsv1.DaemonSet{ObjectMeta: meta("agent"), Spec: appsv1.DaemonSetSpec{Template: restartTemplate()}}, func(c *fake.Clientset) (map[string]string, error) {
			o, err := c.AppsV1().DaemonSets("app").Get(context.Background(), "agent", metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			return o.Spec.Template.Annotations, nil
		}},
	} {
		t.Run(test.kind, func(t *testing.T) {
			client := fake.NewSimpleClientset(test.object)
			name := test.object.(metav1.Object).GetName()
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: test.kind, Namespace: "app", Name: name}
			var waited []sdk.ObjectRef
			executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: recordedRollouts{&waited}, Verifier: passingVerifier{}, Now: func() time.Time { return at }}
			incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
			result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.RestartRollout{Ref: ref, Reason: "replace Pods"}}}, false)
			if err != nil || !result.Applied {
				t.Fatalf("Apply = %+v, %v", result, err)
			}
			annotations, err := test.read(client)
			if err != nil {
				t.Fatal(err)
			}
			if got := annotations["kubectl.kubernetes.io/restartedAt"]; got != at.Format(time.RFC3339Nano) {
				t.Fatalf("restartedAt = %q, want %q", got, at.Format(time.RFC3339Nano))
			}
			found := false
			for _, waitedRef := range waited {
				found = found || waitedRef == ref
			}
			if !found {
				t.Fatalf("rollout wait refs = %+v, want %+v", waited, ref)
			}
		})
	}
}

func TestRestartRolloutRefusesNonRestartableKinds(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web-1", UID: "uid", ResourceVersion: "1"}}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "web-1"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: fake.NewSimpleClientset(pod)}, Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}}
	incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
	if result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.RestartRollout{Ref: ref, Reason: "replace Pods"}}}, false); err == nil || result.Applied {
		t.Fatalf("Apply = %+v, %v; want refusal", result, err)
	}
}

func TestKubernetesPlanRolloutsObserveStatefulSetAndDaemonSetRollouts(t *testing.T) {
	replicas := int32(2)
	settled := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "settled", Generation: 2}, Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 2, ReadyReplicas: 2, UpdatedReplicas: 2, CurrentRevision: "r2", UpdateRevision: "r2"}}
	rolling := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "rolling", Generation: 2}, Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 2, ReadyReplicas: 2, UpdatedReplicas: 1, CurrentRevision: "r1", UpdateRevision: "r2"}}
	daemon := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "agent", Generation: 3},
		Status: appsv1.DaemonSetStatus{ObservedGeneration: 2, DesiredNumberScheduled: 1, UpdatedNumberScheduled: 1, NumberAvailable: 1}}
	rollouts := KubernetesPlanRollouts{Client: fake.NewSimpleClientset(settled, rolling, daemon)}
	ref := func(kind, name string) sdk.ObjectRef {
		return sdk.ObjectRef{APIVersion: "apps/v1", Kind: kind, Namespace: "app", Name: name}
	}
	if err := rollouts.Observe(context.Background(), []sdk.ObjectRef{ref("StatefulSet", "settled")}); err != nil {
		t.Fatalf("settled StatefulSet: %v", err)
	}
	err := rollouts.Observe(context.Background(), []sdk.ObjectRef{ref("StatefulSet", "settled"), ref("StatefulSet", "rolling"), ref("DaemonSet", "agent")})
	var blocked *rolloutBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("Observe = %v, want pending rollout blockers", err)
	}
	var names []string
	for _, blocker := range blocked.blockers {
		names = append(names, blocker.GetRef().GetKind()+"/"+blocker.GetRef().GetName())
	}
	if len(names) != 2 || names[0] != "StatefulSet/rolling" || names[1] != "DaemonSet/agent" {
		t.Fatalf("blockers = %v, want the rolling StatefulSet and the unobserved DaemonSet", names)
	}
	if errors.Is(err, errRolloutStalled) {
		t.Fatal("a StatefulSet or DaemonSet rollout is pending, never proven stalled")
	}
}
