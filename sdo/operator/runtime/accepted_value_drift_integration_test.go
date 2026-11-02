//go:build integration

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func adoptValueApp(t *testing.T) adoptedValueApp {
	t.Helper()
	repo := nativeRepository(t)
	if err := os.MkdirAll(filepath.Join(repo, "kubernetes"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, manifest := range map[string]string{"deployment.yaml": acceptedValueDeployment, "service.yaml": acceptedValueService} {
		if err := os.WriteFile(filepath.Join(repo, "kubernetes", name), []byte(manifest), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	nativeGit(t, repo, "add", ".")
	nativeGit(t, repo, "commit", "-qm", "adopted app")
	commit := nativeGit(t, repo, "rev-parse", "HEAD")

	one, grace, links := int32(1), int64(30), true
	labels := map[string]string{"app": "web"}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app", Generation: 1, UID: "web-uid", ResourceVersion: "1"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labels},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{
				DNSPolicy: corev1.DNSClusterFirst, RestartPolicy: corev1.RestartPolicyAlways, SchedulerName: "default-scheduler",
				TerminationGracePeriodSeconds: &grace, EnableServiceLinks: &links,
				Containers: []corev1.Container{{Name: "web", Image: "example/web:1", ImagePullPolicy: corev1.PullIfNotPresent}},
			}},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app", UID: "web-svc-uid", ResourceVersion: "1"},
		Spec: corev1.ServiceSpec{
			Selector: labels, Type: corev1.ServiceTypeClusterIP, ClusterIP: "10.0.0.7", ClusterIPs: []string{"10.0.0.7"},
			SessionAffinity: corev1.ServiceAffinityNone, InternalTrafficPolicy: &[]corev1.ServiceInternalTrafficPolicy{corev1.ServiceInternalTrafficPolicyCluster}[0],
			Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: corev1.ProtocolTCP}},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "app", Labels: labels}, Spec: deployment.Spec.Template.Spec,
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "web", Image: "example/web:1", Ready: true}}}}
	client := fake.NewSimpleClientset(deployment, service, pod)
	installSyntheticAdoptedDefaults(t, client)
	bridge := bridgeFake(t, client)
	source := GitPlanSource{Repository: repo, ManifestPaths: []string{"kubernetes/deployment.yaml", "kubernetes/service.yaml"}}

	config := testControllerConfig()
	config.Namespace, config.SourceCommit = "app", commit
	controller, err := NewController(config, nil, fastloopObjects{client: client}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Source: source, Client: bridge, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(snapshot), Source: source, SourceCommit: commit, Client: bridge}
	}})
	if err := controller.adoptOwnership(context.Background(), time.Unix(11, 0).UTC(), sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatalf("ownership adoption: %v", err)
	}
	app := adoptedValueApp{client: client, sourceClient: bridge, source: source, commit: commit}
	app.view = func(t *testing.T) program.ReadView {
		t.Helper()
		snapshot, err := (fastloopObjects{client: client}).Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		captured, err := sdktest.CaptureSnapshot(snapshot, "app")
		if err != nil {
			t.Fatal(err)
		}
		return LiveReadView{Carrier: inputs.New(captured), Source: source, SourceCommit: commit, Client: bridge}
	}
	app.liveView = func(t *testing.T) program.ReadView {
		t.Helper()
		snapshot, err := (fastloopObjects{client: client}).Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return LiveReadView{Carrier: inputs.New(snapshot), Source: source, SourceCommit: commit, Client: bridge}
	}
	return app
}

// TestAdoptedServerDefaultedFieldDriftIsNamedAndRestored: accepted source
// leaves most configuration to the API server. Whichever such field a fault
// changes, the drift names its path and one ReconcileFromSource restores the
// accepted value; no field is singled out in advance.
func TestAdoptedServerDefaultedFieldDriftIsNamedAndRestored(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ref    sdk.ObjectRef
		path   string
		mutate func(*appsv1.Deployment, *corev1.Service)
	}{
		{"dnsPolicy", acceptedValueDeploymentRef, "spec.template.spec.dnsPolicy", func(d *appsv1.Deployment, _ *corev1.Service) { d.Spec.Template.Spec.DNSPolicy = corev1.DNSDefault }},
		{"restartPolicy", acceptedValueDeploymentRef, "spec.template.spec.restartPolicy", func(d *appsv1.Deployment, _ *corev1.Service) {
			d.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
		}},
		{"terminationGracePeriodSeconds", acceptedValueDeploymentRef, "spec.template.spec.terminationGracePeriodSeconds", func(d *appsv1.Deployment, _ *corev1.Service) {
			zero := int64(0)
			d.Spec.Template.Spec.TerminationGracePeriodSeconds = &zero
		}},
		{"schedulerName", acceptedValueDeploymentRef, "spec.template.spec.schedulerName", func(d *appsv1.Deployment, _ *corev1.Service) { d.Spec.Template.Spec.SchedulerName = "missing" }},
		{"enableServiceLinks", acceptedValueDeploymentRef, "spec.template.spec.enableServiceLinks", func(d *appsv1.Deployment, _ *corev1.Service) {
			off := false
			d.Spec.Template.Spec.EnableServiceLinks = &off
		}},
		{"imagePullPolicy", acceptedValueDeploymentRef, "spec.template.spec.containers[0].imagePullPolicy", func(d *appsv1.Deployment, _ *corev1.Service) {
			d.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullNever
		}},
		{"strategy", acceptedValueDeploymentRef, "spec.strategy.type", func(d *appsv1.Deployment, _ *corev1.Service) {
			d.Spec.Strategy.Type = appsv1.RecreateDeploymentStrategyType
		}},
		{"replicas", acceptedValueDeploymentRef, "spec.replicas", func(d *appsv1.Deployment, _ *corev1.Service) {
			three := int32(3)
			d.Spec.Replicas = &three
		}},
		{"sessionAffinity", acceptedValueServiceRef, "spec.sessionAffinity", func(_ *appsv1.Deployment, s *corev1.Service) { s.Spec.SessionAffinity = corev1.ServiceAffinityClientIP }},
		{"internalTrafficPolicy", acceptedValueServiceRef, "spec.internalTrafficPolicy", func(_ *appsv1.Deployment, s *corev1.Service) {
			local := corev1.ServiceInternalTrafficPolicyLocal
			s.Spec.InternalTrafficPolicy = &local
		}},
		{"port protocol", acceptedValueServiceRef, "spec.ports[0].protocol", func(_ *appsv1.Deployment, s *corev1.Service) { s.Spec.Ports[0].Protocol = corev1.ProtocolUDP }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := adoptValueApp(t)
			ctx := context.Background()
			accepted := app.liveLeaf(t, tc.ref, tc.path)
			if accepted == nil {
				t.Fatalf("fixture lacks %s", tc.path)
			}
			if diff, err := app.view(t).DiffAgainstSource(tc.ref); err != nil || diff.Changed {
				t.Fatalf("adopted %s differs from source before the fault: %+v %v", tc.ref.Kind, diff, err)
			}
			deployment, err := app.client.AppsV1().Deployments("app").Get(ctx, "web", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			service, err := app.client.CoreV1().Services("app").Get(ctx, "web", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(deployment, service)
			if tc.ref.Kind == "Deployment" {
				_, err = app.client.AppsV1().Deployments("app").Update(ctx, deployment, metav1.UpdateOptions{})
			} else {
				_, err = app.client.CoreV1().Services("app").Update(ctx, service, metav1.UpdateOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
			if encodedLeaf(app.liveLeaf(t, tc.ref, tc.path)) == encodedLeaf(accepted) {
				t.Fatalf("fault did not change %s", tc.path)
			}
			diff, err := app.view(t).DiffAgainstSource(tc.ref)
			if err != nil {
				t.Fatalf("faulted %s cannot be compared with accepted source: %v", tc.ref.Kind, err)
			}
			if !diff.Changed || !strings.Contains(diff.Text, "changed since acceptance: "+tc.path) {
				t.Fatalf("drift does not name %s: %+v", tc.path, diff)
			}
			app.reconcile(t, tc.ref)
			if got := app.liveLeaf(t, tc.ref, tc.path); encodedLeaf(got) != encodedLeaf(accepted) {
				t.Fatalf("one ReconcileFromSource left %s = %v, accepted %v", tc.path, got, accepted)
			}
			if diff, err := app.view(t).DiffAgainstSource(tc.ref); err != nil || diff.Changed {
				t.Fatalf("restored %s still differs from source: %+v %v", tc.ref.Kind, diff, err)
			}
		})
	}
}

// TestFieldOwnedByAnotherControllerIsNotDrift: an autoscaler that scales the
// Deployment owns spec.replicas through the scale subresource, so its changes
// are not drift even though accepted source never declared the field.
func TestFieldOwnedByAnotherControllerIsNotDrift(t *testing.T) {
	app := adoptValueApp(t)
	ctx := context.Background()
	deployment, err := app.client.AppsV1().Deployments("app").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	three := int32(3)
	deployment.Spec.Replicas = &three
	deployment.ManagedFields = []metav1.ManagedFieldsEntry{{
		Manager: "kube-controller-manager", Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: "apps/v1", Subresource: "scale",
		FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)},
	}}
	if _, err := app.client.AppsV1().Deployments("app").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	diff, err := app.liveView(t).DiffAgainstSource(acceptedValueDeploymentRef)
	if err != nil || diff.Changed {
		t.Fatalf("autoscaler-owned replicas reported as drift: %+v %v", diff, err)
	}
}
