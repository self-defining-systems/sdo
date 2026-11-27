package operatorcli

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/telemetry/prometheus/recipe"
)

// markReady is a wait that plays the Deployment controller: the recipe
// Deployment becomes available on the first poll.
func markReady(t *testing.T, client kubernetes.Interface, namespace string) func(context.Context, time.Duration) error {
	return func(ctx context.Context, _ time.Duration) error {
		d, err := client.AppsV1().Deployments(namespace).Get(ctx, recipe.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("recipe deployment: %v", err)
		}
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, UpdatedReplicas: 1, AvailableReplicas: 1, ReadyReplicas: 1, Replicas: 1}
		_, err = client.AppsV1().Deployments(namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{})
		return err
	}
}

func TestOnboardingCheckWithoutPrometheusAppliesSDOsOwnInTheControlNamespace(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	var notice strings.Builder
	target, err := resolvePrometheus(ctx, client, "", "app-ns", "ctl-ns", markReady(t, client, "ctl-ns"), &notice)
	if err != nil {
		t.Fatalf("resolvePrometheus: %v", err)
	}
	if target != (promTarget{Namespace: "ctl-ns", Service: recipe.Name, Port: 9090}) {
		t.Fatalf("target = %+v, want the recipe Service in the control namespace", target)
	}
	if _, err := client.CoreV1().Namespaces().Get(ctx, "ctl-ns", metav1.GetOptions{}); err != nil {
		t.Errorf("control namespace not created: %v", err)
	}
	for _, get := range []func() error{
		func() error {
			_, err := client.CoreV1().ConfigMaps("ctl-ns").Get(ctx, recipe.Name, metav1.GetOptions{})
			return err
		},
		func() error {
			_, err := client.CoreV1().ServiceAccounts("ctl-ns").Get(ctx, recipe.Name, metav1.GetOptions{})
			return err
		},
		func() error {
			_, err := client.CoreV1().Services("ctl-ns").Get(ctx, recipe.Name, metav1.GetOptions{})
			return err
		},
		func() error {
			_, err := client.RbacV1().ClusterRoleBindings().Get(ctx, recipe.Name+"-ctl-ns", metav1.GetOptions{})
			return err
		},
	} {
		if err := get(); err != nil {
			t.Errorf("recipe object missing: %v", err)
		}
	}
	d, err := client.AppsV1().Deployments("ctl-ns").Get(ctx, recipe.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("recipe deployment: %v", err)
	}
	if d.Spec.Template.Spec.SecurityContext == nil || d.Spec.Template.Spec.Containers[0].SecurityContext == nil {
		t.Errorf("recipe deployment lacks the installer's security contexts")
	}
	if !strings.Contains(notice.String(), "SDO's own Prometheus") || !strings.Contains(notice.String(), "ctl-ns") {
		t.Errorf("notice = %q", notice.String())
	}
}

func TestOnboardingCheckWithExplicitPrometheusAppliesNothing(t *testing.T) {
	for _, given := range []string{"monitoring/prom:9090", "http://prom.example:9090"} {
		client := fake.NewSimpleClientset()
		target, err := resolvePrometheus(context.Background(), client, given, "app-ns", "ctl-ns", nil, io.Discard)
		if err != nil {
			t.Fatalf("%s: %v", given, err)
		}
		if len(client.Actions()) != 0 {
			t.Errorf("%s: cluster actions %v, want none", given, client.Actions())
		}
		if given == "monitoring/prom:9090" && target != (promTarget{Namespace: "monitoring", Service: "prom", Port: 9090}) {
			t.Errorf("target = %+v", target)
		}
		if strings.HasPrefix(given, "http") && target.URL != given {
			t.Errorf("target = %+v", target)
		}
	}
	if _, err := resolvePrometheus(context.Background(), fake.NewSimpleClientset(), "prom:9090", "a", "c", nil, io.Discard); err == nil {
		t.Error("malformed --prometheus accepted")
	}
}

func TestOnboardingCheckOwnPrometheusFailsWithWhatTheClusterSays(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	stall := func(ctx context.Context, _ time.Duration) error {
		d, _ := client.AppsV1().Deployments("ctl").Get(ctx, recipe.Name, metav1.GetOptions{})
		d.Status.ObservedGeneration = d.Generation
		d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded", Message: "replica set has timed out progressing"}}
		_, err := client.AppsV1().Deployments("ctl").UpdateStatus(ctx, d, metav1.UpdateOptions{})
		return err
	}
	_, err := resolvePrometheus(ctx, client, "", "app", "ctl", stall, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "ProgressDeadlineExceeded") || !strings.Contains(err.Error(), "timed out progressing") {
		t.Fatalf("err = %v, want the Deployment's own condition", err)
	}
}

func TestOnboardingCheckFlagsNeedNoPrometheus(t *testing.T) {
	code, err := onboardingCheck(context.Background(), []string{"--namespace", "ns"}, io.Discard)
	if code != 0 || err == nil || strings.Contains(err.Error(), "--prometheus") {
		t.Fatalf("missing --repo: code=%d err=%v; the message must not ask for --prometheus", code, err)
	}
}
