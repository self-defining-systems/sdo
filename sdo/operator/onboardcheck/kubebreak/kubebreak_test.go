package kubebreak

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

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

func TestWorkloadReferenceMayCarryItsKind(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("web", 2), statefulSet("db", 1))
	b := newBreaker(t, client, ModeScale)
	for _, ref := range []string{"Deployment/web", "StatefulSet/db"} {
		c := appmap.Component{Name: ref, Workload: ref}
		if err := b.Break(context.Background(), c); err != nil {
			t.Fatalf("%s: %v", ref, err)
		}
		if err := b.Restore(context.Background(), c); err != nil {
			t.Fatalf("%s: %v", ref, err)
		}
	}
	if err := b.Break(context.Background(), appmap.Component{Name: "x", Workload: "Deployment/db"}); err == nil {
		t.Fatal("a StatefulSet must not match a Deployment reference")
	}
	if err := b.Break(context.Background(), appmap.Component{Name: "x", Workload: "DaemonSet/web"}); err == nil {
		t.Fatal("only Deployments and StatefulSets can be scaled")
	}
}

func TestNetworkPolicyNameUsesTheBareWorkloadName(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("web", 2), daemonSet("calico-node"))
	b := newBreaker(t, client, ModeNetwork)
	c := appmap.Component{Name: "web", Workload: "Deployment/web"}
	if err := b.Break(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if err := b.Restore(context.Background(), c); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultModeDeniesIngressWhereTheCNIEnforces(t *testing.T) {
	for _, mode := range []string{"", ModeAuto} {
		b := newBreaker(t, fake.NewSimpleClientset(deployment("web", 2), daemonSet("calico-node")), mode)
		if !strings.Contains(b.Mode(), "deny-ingress NetworkPolicy") || strings.Contains(b.Mode(), "scale") {
			t.Fatalf("mode %q: default must be the network break on an enforcing CNI, said in the mode: %q", mode, b.Mode())
		}
	}
}

func TestDefaultModeScalesAndSaysWhyWhereTheCNIDoesNotEnforce(t *testing.T) {
	for _, mode := range []string{"", ModeAuto} {
		b := newBreaker(t, fake.NewSimpleClientset(deployment("web", 2), daemonSet("kindnet")), mode)
		if !strings.Contains(b.Mode(), "scale to zero") || !strings.Contains(b.Mode(), "does not enforce") {
			t.Fatalf("mode %q: %q", mode, b.Mode())
		}
	}
}

func TestExplicitScaleModeStaysScaleOnAnEnforcingCNI(t *testing.T) {
	b := newBreaker(t, fake.NewSimpleClientset(deployment("web", 2), daemonSet("calico-node")), ModeScale)
	if strings.Contains(b.Mode(), "NetworkPolicy") {
		t.Fatalf("%q", b.Mode())
	}
}

func TestBreakPoliciesAreRecognisedByName(t *testing.T) {
	if !IsBreakPolicy(policyName("web")) || !IsBreakPolicy("sdo-onboarding-check-worker-egress-sidekiq") || IsBreakPolicy("allow-web") {
		t.Fatal("only the policies a break creates are break policies")
	}
}

// conflictOn makes the first n writes of verb on deployments fail with 409.
func conflictOn(client *fake.Clientset, verb string, n int) *int {
	seen := 0
	client.PrependReactor(verb, "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		if seen < n {
			seen++
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, "web", nil)
		}
		return false, nil, nil
	})
	return &seen
}

func TestRestoreRetriesAConflictAndSucceeds(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("web", 3))
	b := newBreaker(t, client, ModeScale)
	c := appmap.Component{Name: "web", Workload: "web"}
	if err := b.Break(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	seen := conflictOn(client, "patch", 1)
	if err := b.Restore(context.Background(), c); err != nil {
		t.Fatalf("a single conflict must not make the restore uncertain: %v", err)
	}
	if *seen != 1 {
		t.Fatalf("conflicts injected %d", *seen)
	}
	got, _ := client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	if *got.Spec.Replicas != 3 {
		t.Fatalf("replicas %d", *got.Spec.Replicas)
	}
	if _, left := got.Annotations[restoreAnnotation]; left {
		t.Fatal("restore annotation left behind")
	}
}

func TestRestoreIsUncertainOnlyAfterTheConflictRetriesFail(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("web", 3))
	b := newBreaker(t, client, ModeScale)
	c := appmap.Component{Name: "web", Workload: "web"}
	if err := b.Break(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	seen := conflictOn(client, "patch", 100)
	err := b.Restore(context.Background(), c)
	if err == nil || !apierrors.IsConflict(err) || !strings.Contains(err.Error(), "still conflicting after 5 tries") {
		t.Fatalf("expected an uncertain restore, got %v", err)
	}
	got, _ := client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	if _, kept := got.Annotations[restoreAnnotation]; !kept || *got.Spec.Replicas != 0 {
		t.Fatal("a failed restore must leave the annotation so a later restore can recover")
	}
	if *seen != 5 {
		t.Fatalf("tries %d, want 5", *seen)
	}
}

func TestRestoreDoesNotOverwriteConcurrentChanges(t *testing.T) {
	d := deployment("web", 0)
	d.Annotations = map[string]string{restoreAnnotation: "2", "other": "keep"}
	d.Spec.Paused = true
	client := fake.NewSimpleClientset(d)
	b := newBreaker(t, client, ModeScale)
	if err := b.Restore(context.Background(), appmap.Component{Name: "web", Workload: "web"}); err != nil {
		t.Fatal(err)
	}
	got, _ := client.AppsV1().Deployments("app").Get(context.Background(), "web", metav1.GetOptions{})
	if got.Annotations["other"] != "keep" || !got.Spec.Paused || *got.Spec.Replicas != 2 {
		t.Fatalf("restore touched more than its own fields: %+v", got)
	}
}
