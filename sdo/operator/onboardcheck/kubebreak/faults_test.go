package kubebreak

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/memory/appmap"
)

func calico() *appsv1.DaemonSet {
	return &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "calico-node", Namespace: metav1.NamespaceSystem}}
}

func TestWorkerQuietDeniesEgressExceptDNSAndRestores(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("sidekiq", 1), calico())
	b := newBreaker(t, client, "")
	f := b.WorkerQuiet(appmap.Component{Name: "sidekiq", Workload: "Deployment/sidekiq"}, []string{"send-mail"})
	if why := f.NotApplicable(context.Background()); why != "" {
		t.Fatalf("enforcing CNI: %s", why)
	}
	if err := f.Break(context.Background()); err != nil {
		t.Fatal(err)
	}
	p, err := client.NetworkingV1().NetworkPolicies("app").Get(context.Background(), policyPrefix+"worker-egress-sidekiq", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Spec.Egress) != 1 || len(p.Spec.Egress[0].Ports) != 2 || p.Spec.Egress[0].To != nil || p.Spec.PodSelector.MatchLabels["app"] != "sidekiq" {
		t.Fatalf("policy %+v", p.Spec)
	}
	if p.Labels[checkLabel] != "true" {
		t.Fatalf("leftover policies must be findable: %v", p.Labels)
	}
	// Still scheduled and ready: only the network changed.
	d, _ := client.AppsV1().Deployments("app").Get(context.Background(), "sidekiq", metav1.GetOptions{})
	if *d.Spec.Replicas != 1 {
		t.Fatal("the worker must stay up")
	}
	if err := f.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NetworkingV1().NetworkPolicies("app").Get(context.Background(), p.Name, metav1.GetOptions{}); err == nil {
		t.Fatal("policy left behind")
	}
	if err := f.Restore(context.Background()); err != nil {
		t.Fatalf("restore must be safe to repeat: %v", err)
	}
}

func TestWorkerQuietNeedsAnEnforcingCNI(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("sidekiq", 1))
	b := newBreaker(t, client, "")
	f := b.WorkerQuiet(appmap.Component{Name: "sidekiq", Workload: "sidekiq"}, nil)
	if why := f.NotApplicable(context.Background()); !strings.Contains(why, "does not enforce") {
		t.Fatalf("expected a reason, got %q", why)
	}
}

type recordedExec struct {
	calls []string
	out   map[string]string
	err   error
	pod   string
	cont  string
}

func (r *recordedExec) exec(_ context.Context, _, pod, container string, command []string, stdin string) (string, error) {
	mode := command[len(command)-1]
	r.calls = append(r.calls, mode)
	r.pod, r.cont = pod, container
	if !strings.Contains(stdin, "sdo_onboarding_check_backup") {
		return "", errors.New("script missing")
	}
	return r.out[mode], r.err
}

func store(t *testing.T) (*fake.Clientset, *Breaker) {
	t.Helper()
	sts := statefulSet("pg", 1)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-0", Namespace: "app", Labels: map[string]string{"app": "pg"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar", Image: "busybox"}, {Name: "db", Image: "postgres:17@sha256:abc"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewSimpleClientset(sts, pod)
	return client, newBreaker(t, client, "")
}

func TestDataWrongBreaksAndRestoresInTheStoresContainer(t *testing.T) {
	_, b := store(t)
	r := &recordedExec{out: map[string]string{"break": "sdo-wrong public.issues title 40\n", "restore": "sdo-restored ok\n"}}
	f := b.DataWrong(appmap.Component{Name: "pg", Workload: "StatefulSet/pg", Data: &appmap.Data{Engine: "postgres", Role: "authoritative"}}, []string{"list-issues"}, r.exec)
	if why := f.NotApplicable(context.Background()); why != "" {
		t.Fatal(why)
	}
	if err := f.Break(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(r.calls, ",") != "break,restore" || r.pod != "pg-0" || r.cont != "db" {
		t.Fatalf("calls %v pod %s container %s", r.calls, r.pod, r.cont)
	}
}

func TestDataWrongBreakWithoutAChangedRowFails(t *testing.T) {
	_, b := store(t)
	r := &recordedExec{out: map[string]string{"break": ""}}
	f := b.DataWrong(appmap.Component{Name: "pg", Workload: "StatefulSet/pg", Data: &appmap.Data{Engine: "postgres"}}, nil, r.exec)
	if err := f.Break(context.Background()); err == nil {
		t.Fatal("a break that changed nothing must fail")
	}
}

func TestDataWrongRestoreFailureIsReported(t *testing.T) {
	_, b := store(t)
	r := &recordedExec{err: errors.New("exec failed")}
	f := b.DataWrong(appmap.Component{Name: "pg", Workload: "StatefulSet/pg", Data: &appmap.Data{Engine: "postgres"}}, nil, r.exec)
	if err := f.Restore(context.Background()); err == nil {
		t.Fatal("an uncertain restore must be an error")
	}
}

func TestDataWrongOnlyForPostgres(t *testing.T) {
	_, b := store(t)
	f := b.DataWrong(appmap.Component{Name: "cache", Workload: "StatefulSet/pg", Data: &appmap.Data{Engine: "redis"}}, nil, (&recordedExec{}).exec)
	if why := f.NotApplicable(context.Background()); !strings.Contains(why, "postgres") {
		t.Fatalf("got %q", why)
	}
}
