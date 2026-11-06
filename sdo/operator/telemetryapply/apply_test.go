package telemetryapply

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/telemetry"
)

const pinned = "registry.test/backend@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// fakeRecipe is a synthetic backend: one Deployment under its own
// ServiceAccount. mutate lets a test make the pod violate a trust rule.
type fakeRecipe struct {
	mutate func(*appsv1.Deployment)
}

func (fakeRecipe) Name() string { return "fake" }
func (fakeRecipe) Capabilities() []telemetry.ClusterCapability {
	return []telemetry.ClusterCapability{telemetry.CapPodDiscovery}
}
func (r fakeRecipe) Build(p telemetry.BackendParams) telemetry.BackendObjects {
	labels := map[string]string{"app": "fake"}
	one := int32(1)
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "fake", Namespace: p.ControlNamespace},
		Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: corev1.PodSpec{ServiceAccountName: "fake", Containers: []corev1.Container{{Name: "c", Image: pinned, Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
			}}}},
		}},
	}
	if r.mutate != nil {
		r.mutate(d)
	}
	return telemetry.BackendObjects{
		ServiceAccounts: []*corev1.ServiceAccount{{ObjectMeta: metav1.ObjectMeta{Name: "fake", Namespace: p.ControlNamespace}}},
		Services:        []*corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "fake", Namespace: p.ControlNamespace}}},
		Deployments:     []*appsv1.Deployment{d},
	}
}

func policy() telemetry.Policy {
	return telemetry.Policy{
		Mode: telemetry.ModeOpen, ControlNamespace: "sdo-ctl", ApplicationNamespace: "shop",
		Capabilities: []telemetry.ClusterCapability{telemetry.CapPodDiscovery},
		Recipes:      []telemetry.Recipe{{Name: "fake", Declares: []telemetry.ClusterCapability{telemetry.CapPodDiscovery}}},
	}
}

func checkoutWith(t *testing.T, backends string) string {
	t.Helper()
	root := t.TempDir()
	if backends != "" {
		path := filepath.Join(root, layout.TelemetryBackendsFile)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(backends), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

const oneBackend = "version: 1\nbackends:\n  - id: stack\n    recipe: fake\n    uses: [pod-discovery]\n"

// becomesReady is a Wait that marks every deployment available, as a rollout
// would while the applier polls.
func becomesReady(client *fake.Clientset) func(context.Context, time.Duration) error {
	return func(ctx context.Context, _ time.Duration) error {
		list, _ := client.AppsV1().Deployments("sdo-ctl").List(ctx, metav1.ListOptions{})
		for _, d := range list.Items {
			d.Status.UpdatedReplicas, d.Status.AvailableReplicas = 1, 1
			if _, err := client.AppsV1().Deployments("sdo-ctl").UpdateStatus(ctx, &d, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestApplyDeploysTheRecipeAndRecordsWhatItApplied(t *testing.T) {
	client := fake.NewSimpleClientset()
	applier := Applier{Client: client, Policy: policy(), Recipes: []telemetry.BackendRecipe{fakeRecipe{}}, Wait: becomesReady(client)}
	applied, err := applier.Apply(context.Background(), checkoutWith(t, oneBackend))
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Backends) != 1 || applied.Backends[0].ID != "stack" || applied.Backends[0].Recipe != "fake" {
		t.Fatalf("applied = %+v", applied)
	}
	if got := applied.Summary(); !strings.Contains(got, "Deployment/fake") || !strings.Contains(got, "stack") {
		t.Fatalf("summary = %q", got)
	}
	if _, err := client.AppsV1().Deployments("sdo-ctl").Get(context.Background(), "fake", metav1.GetOptions{}); err != nil {
		t.Fatalf("the deployment must exist in the control namespace: %v", err)
	}
}

func TestApplyWithoutABackendsFileAppliesNothing(t *testing.T) {
	client := fake.NewSimpleClientset()
	applied, err := Applier{Client: client, Policy: policy()}.Apply(context.Background(), checkoutWith(t, ""))
	if err != nil || len(applied.Backends) != 0 || len(client.Actions()) != 0 {
		t.Fatalf("applied = %+v, err = %v, actions = %v", applied, err, client.Actions())
	}
}

func TestApplyFailsClosedWithATypedReason(t *testing.T) {
	deny := func(mutate func(*telemetry.Policy)) Applier {
		p := policy()
		mutate(&p)
		return Applier{Client: fake.NewSimpleClientset(), Policy: p, Recipes: []telemetry.BackendRecipe{fakeRecipe{}}}
	}
	for name, test := range map[string]struct {
		applier Applier
		backend string
		want    string
	}{
		"recipe not offered":      {deny(func(p *telemetry.Policy) { p.Recipes = nil }), oneBackend, "not offered"},
		"pinned environment":      {deny(func(p *telemetry.Policy) { p.Mode = telemetry.ModePinned }), oneBackend, "pinned"},
		"floor environment":       {deny(func(p *telemetry.Policy) { p.Mode = telemetry.ModeFloor }), oneBackend, "floor"},
		"capability not granted":  {deny(func(p *telemetry.Policy) { p.Capabilities = nil }), oneBackend, "not granted"},
		"capability not declared": {deny(func(p *telemetry.Policy) { p.Recipes[0].Declares = nil }), oneBackend, "does not declare"},
		"unknown recipe":          {deny(func(*telemetry.Policy) {}), strings.Replace(oneBackend, "fake", "ghost", 1), "not offered"},
		"unknown capability":      {deny(func(*telemetry.Policy) {}), strings.Replace(oneBackend, "pod-discovery", "root", 1), "unknown capability"},
		"malformed file":          {deny(func(*telemetry.Policy) {}), "version: 9\n", "version"},
	} {
		_, err := test.applier.Apply(context.Background(), checkoutWith(t, test.backend))
		var typed *Error
		if !errors.As(err, &typed) || typed.Reason != ReasonDenied || !strings.Contains(typed.Detail, test.want) || typed.Path != layout.TelemetryBackendsFile {
			t.Errorf("%s: err = %v, want a denied *Error mentioning %q", name, err, test.want)
		}
		if len(test.applier.Client.(*fake.Clientset).Actions()) != 0 {
			t.Errorf("%s: a denied backend must touch nothing: %v", name, test.applier.Client.(*fake.Clientset).Actions())
		}
	}
}

func TestApplyRefusesObjectsThatBreakTheTrustRules(t *testing.T) {
	privileged := true
	for name, test := range map[string]struct {
		mutate func(*appsv1.Deployment)
		want   string
	}{
		"other namespace": {func(d *appsv1.Deployment) { d.Namespace = "kube-system" }, "control namespace"},
		"host network":    {func(d *appsv1.Deployment) { d.Spec.Template.Spec.HostNetwork = true }, "host namespaces"},
		"hostPath": {func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "h", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}}}
		}, "hostPath"},
		"privileged": {func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: &privileged}
		}, "privileged"},
		"unpinned image": {func(d *appsv1.Deployment) { d.Spec.Template.Spec.Containers[0].Image = "registry.test/backend:latest" }, "digest"},
		"no limits": {func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
		}, "limits"},
		"foreign account": {func(d *appsv1.Deployment) { d.Spec.Template.Spec.ServiceAccountName = "sdo-controller" }, "ServiceAccount"},
	} {
		client := fake.NewSimpleClientset()
		applier := Applier{Client: client, Policy: policy(), Recipes: []telemetry.BackendRecipe{fakeRecipe{mutate: test.mutate}}}
		_, err := applier.Apply(context.Background(), checkoutWith(t, oneBackend))
		var typed *Error
		if !errors.As(err, &typed) || typed.Reason != ReasonDenied || !strings.Contains(typed.Detail, test.want) {
			t.Errorf("%s: err = %v, want denied mentioning %q", name, err, test.want)
		}
		if len(client.Actions()) != 0 {
			t.Errorf("%s: nothing may be applied: %v", name, client.Actions())
		}
	}
}

func TestApplyReportsABackendThatNeverBecomesReady(t *testing.T) {
	client := fake.NewSimpleClientset()
	applier := Applier{Client: client, Policy: policy(), Recipes: []telemetry.BackendRecipe{fakeRecipe{}},
		ReadyTimeout: 3 * time.Second, PollInterval: time.Second, Wait: func(context.Context, time.Duration) error { return nil }}
	_, err := applier.Apply(context.Background(), checkoutWith(t, oneBackend))
	var typed *Error
	if !errors.As(err, &typed) || typed.Reason != ReasonNotReady || typed.Backend != "stack" || !strings.Contains(typed.Detail, "not available after 3s") {
		t.Fatalf("err = %v, want a not_ready *Error naming the backend", err)
	}
}
