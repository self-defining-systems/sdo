package kubebreak

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/memory/appmap"
)

func replicas(n int32) *int32 { return &n }

func deployment(name string, n int32) *appsv1.Deployment {
	labels := map[string]string{"app": name}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"},
		Spec:       appsv1.DeploymentSpec{Replicas: replicas(n), Selector: &metav1.LabelSelector{MatchLabels: labels}},
		Status:     appsv1.DeploymentStatus{Replicas: n, ReadyReplicas: n, ObservedGeneration: 0},
	}
}

func statefulSet(name string, n int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"},
		Spec:       appsv1.StatefulSetSpec{Replicas: replicas(n), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}}},
		Status:     appsv1.StatefulSetStatus{Replicas: n, ReadyReplicas: n},
	}
}

// kubelet makes the fake cluster converge: after each wait, ready replicas
// follow the desired count.
func converge(client *fake.Clientset) func(context.Context, time.Duration) error {
	return func(ctx context.Context, _ time.Duration) error {
		deps, _ := client.AppsV1().Deployments("app").List(ctx, metav1.ListOptions{})
		for i := range deps.Items {
			d := deps.Items[i]
			d.Status.ReadyReplicas, d.Status.Replicas = *d.Spec.Replicas, *d.Spec.Replicas
			_, _ = client.AppsV1().Deployments("app").UpdateStatus(ctx, &d, metav1.UpdateOptions{})
		}
		sets, _ := client.AppsV1().StatefulSets("app").List(ctx, metav1.ListOptions{})
		for i := range sets.Items {
			s := sets.Items[i]
			s.Status.ReadyReplicas, s.Status.Replicas = *s.Spec.Replicas, *s.Spec.Replicas
			_, _ = client.AppsV1().StatefulSets("app").UpdateStatus(ctx, &s, metav1.UpdateOptions{})
		}
		return nil
	}
}

func newBreaker(t *testing.T, client *fake.Clientset, mode string) *Breaker {
	t.Helper()
	b, err := New(context.Background(), Config{Client: client, Namespace: "app", Mode: mode, Bound: 30 * time.Second, Interval: time.Second, Wait: converge(client)})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestScaleToZeroAndRestoreDeployment(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("web", 3))
	b := newBreaker(t, client, ModeScale)
	c := appmap.Component{Name: "web", Workload: "web"}
	if err := b.Break(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	got, _ := client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	if *got.Spec.Replicas != 0 {
		t.Fatalf("replicas %d", *got.Spec.Replicas)
	}
	if err := b.Restore(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	got, _ = client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	if *got.Spec.Replicas != 3 || got.Status.ReadyReplicas != 3 {
		t.Fatalf("not restored: spec %d ready %d", *got.Spec.Replicas, got.Status.ReadyReplicas)
	}
	if _, left := got.Annotations[restoreAnnotation]; left {
		t.Fatal("restore annotation left behind")
	}
}

func TestScaleToZeroStatefulSet(t *testing.T) {
	client := fake.NewSimpleClientset(statefulSet("db", 1))
	b := newBreaker(t, client, ModeScale)
	c := appmap.Component{Name: "db", Workload: "db"}
	if err := b.Break(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if err := b.Restore(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	got, _ := client.AppsV1().StatefulSets("app").Get(context.Background(), "db", metav1.GetOptions{})
	if *got.Spec.Replicas != 1 || got.Status.ReadyReplicas != 1 {
		t.Fatalf("not restored: %d/%d", *got.Spec.Replicas, got.Status.ReadyReplicas)
	}
}

func TestRestoreIsUncertainWhenPodsNeverBecomeReady(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("web", 2))
	b, err := New(context.Background(), Config{Client: client, Namespace: "app", Mode: ModeScale, Bound: 5 * time.Second, Interval: time.Second,
		Wait: func(context.Context, time.Duration) error { return nil }}) // nothing converges
	if err != nil {
		t.Fatal(err)
	}
	c := appmap.Component{Name: "web", Workload: "web"}
	if err := b.Break(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	// Spec is back at 2 but status never reports ready.
	got, _ := client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	got.Status.ReadyReplicas = 0
	_, _ = client.AppsV1().Deployments("app").UpdateStatus(context.Background(), got, metav1.UpdateOptions{})
	err = b.Restore(context.Background(), c)
	if err == nil || !strings.Contains(err.Error(), "ready") {
		t.Fatalf("expected an uncertain restore, got %v", err)
	}
}

func TestRestoreWorksFromTheAnnotationAfterARestart(t *testing.T) {
	d := deployment("web", 0)
	d.Annotations = map[string]string{restoreAnnotation: "4"}
	client := fake.NewSimpleClientset(d)
	b := newBreaker(t, client, ModeScale)
	if err := b.Restore(context.Background(), appmap.Component{Name: "web", Workload: "web"}); err != nil {
		t.Fatal(err)
	}
	got, _ := client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	if *got.Spec.Replicas != 4 {
		t.Fatalf("replicas %d", *got.Spec.Replicas)
	}
}

func TestBreakRefusesAnAlreadyBrokenWorkload(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("web", 1))
	b := newBreaker(t, client, ModeScale)
	c := appmap.Component{Name: "web", Workload: "web"}
	if err := b.Break(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if err := b.Break(context.Background(), c); err == nil {
		t.Fatal("second break would lose the original replica count")
	}
}

func TestUnknownWorkloadFailsToBreak(t *testing.T) {
	client := fake.NewSimpleClientset()
	b := newBreaker(t, client, ModeScale)
	if err := b.Break(context.Background(), appmap.Component{Name: "web", Workload: "web"}); err == nil {
		t.Fatal("expected an error")
	}
}

func daemonSet(name string) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "kube-system"}}
}

func TestNetworkModeFallsBackToScaleWhenCNIDoesNotEnforce(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("web", 2), daemonSet("kindnet"))
	b := newBreaker(t, client, ModeNetwork)
	if !strings.Contains(b.Mode(), "scale") || !strings.Contains(b.Mode(), "NetworkPolicy") {
		t.Fatalf("mode must say it fell back and why: %q", b.Mode())
	}
	c := appmap.Component{Name: "web", Workload: "web"}
	if err := b.Break(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	policies, _ := client.NetworkingV1().NetworkPolicies("app").List(context.Background(), metav1.ListOptions{})
	if len(policies.Items) != 0 {
		t.Fatal("created a policy the cluster would not enforce")
	}
	got, _ := client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	if *got.Spec.Replicas != 0 {
		t.Fatal("fallback did not scale")
	}
}

func TestNetworkModeDeniesIngressWhenCNIEnforces(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("web", 2), daemonSet("calico-node"))
	b := newBreaker(t, client, ModeNetwork)
	if !strings.Contains(b.Mode(), "NetworkPolicy") || strings.Contains(b.Mode(), "scale") {
		t.Fatalf("mode %q", b.Mode())
	}
	c := appmap.Component{Name: "web", Workload: "web"}
	if err := b.Break(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	policies, _ := client.NetworkingV1().NetworkPolicies("app").List(context.Background(), metav1.ListOptions{})
	if len(policies.Items) != 1 {
		t.Fatalf("policies %d", len(policies.Items))
	}
	p := policies.Items[0]
	if p.Spec.PodSelector.MatchLabels["app"] != "web" || len(p.Spec.Ingress) != 0 || len(p.Spec.PolicyTypes) != 1 {
		t.Fatalf("not a deny-all ingress policy for the workload: %+v", p.Spec)
	}
	got, _ := client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	if *got.Spec.Replicas != 2 {
		t.Fatal("network break must leave the pods running")
	}
	if err := b.Restore(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	policies, _ = client.NetworkingV1().NetworkPolicies("app").List(context.Background(), metav1.ListOptions{})
	if len(policies.Items) != 0 {
		t.Fatal("policy left behind")
	}
}

func TestEnforcingCNIDetection(t *testing.T) {
	for name, want := range map[string]bool{"calico-node": true, "cilium": true, "kube-router": true, "antrea-agent": true, "kindnet": false, "kube-proxy": false} {
		client := fake.NewSimpleClientset(daemonSet(name))
		got, _ := enforcesNetworkPolicy(context.Background(), client)
		if got != want {
			t.Fatalf("%s: got %v want %v", name, got, want)
		}
	}
	_ = corev1.Pod{}
}

func TestUnknownModeIsRejected(t *testing.T) {
	if _, err := New(context.Background(), Config{Client: fake.NewSimpleClientset(), Namespace: "app", Mode: "burn", Bound: time.Second, Interval: time.Second, Wait: func(context.Context, time.Duration) error { return nil }}); err == nil {
		t.Fatal("unknown mode accepted")
	}
}
