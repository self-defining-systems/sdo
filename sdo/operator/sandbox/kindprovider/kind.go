// Package kindprovider creates sandboxes as kind clusters on the host, each
// with SDO's Prometheus recipe and a tracing store, reachable on host ports.
package kindprovider

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sdo.dev/operator/install"
	"sdo.dev/operator/sandbox"
	"sdo.dev/operator/sandbox/internal/kindio"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/prometheus/recipe"
)

const (
	prometheusNodePort = 30901
	tracesNodePort     = 30902
	// TracesImage is the in-memory tracing store; it accepts OTLP directly.
	TracesImage  = "docker.io/jaegertracing/all-in-one:1.62.0"
	tracesName   = "sdo-traces"
	readyTimeout = 5 * time.Minute
)

// Provider runs kind and docker from PATH.
type Provider struct {
	// NodeImage, when set, is passed to kind (--image).
	NodeImage string
}

// Names implements sandbox.Provider.
func (Provider) Names(ctx context.Context) ([]string, error) {
	out, err := kindio.Run(ctx, "kind", "get", "clusters")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, line := range strings.Fields(string(out)) {
		if strings.HasPrefix(line, sandbox.ClusterPrefix) {
			names = append(names, line)
		}
	}
	return names, nil
}

// Delete implements sandbox.Provider. It never touches a cluster without the
// sandbox prefix.
func (Provider) Delete(ctx context.Context, name string) error {
	if !strings.HasPrefix(name, sandbox.ClusterPrefix) {
		return fmt.Errorf("refusing to delete %q: not a sandbox cluster", name)
	}
	_, err := kindio.Run(ctx, "kind", "delete", "cluster", "--name", name)
	return err
}

func kindConfig(spec sandbox.Spec) string {
	return fmt.Sprintf(`kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - {containerPort: %d, hostPort: %d, listenAddress: "127.0.0.1"}
      - {containerPort: %d, hostPort: %d, listenAddress: "127.0.0.1"}
`, prometheusNodePort, spec.PrometheusPort, tracesNodePort, spec.TracesPort)
}

// Create implements sandbox.Provider.
func (p Provider) Create(ctx context.Context, spec sandbox.Spec) error {
	if spec.ControlNamespace == "" {
		return fmt.Errorf("sandbox spec needs a control namespace")
	}
	config := filepath.Join(filepath.Dir(spec.Kubeconfig), "kind.yaml")
	if err := os.WriteFile(config, []byte(kindConfig(spec)), 0o600); err != nil {
		return err
	}
	args := []string{"create", "cluster", "--name", spec.Name, "--kubeconfig", spec.Kubeconfig, "--config", config, "--wait", "180s"}
	if p.NodeImage != "" {
		args = append(args, "--image", p.NodeImage)
	}
	if _, err := kindio.Run(ctx, "kind", args...); err != nil {
		return err
	}
	for _, image := range []string{recipe.Image, TracesImage} {
		// An image the host already has is loaded; otherwise the node pulls it.
		if _, err := kindio.Run(ctx, "docker", "image", "inspect", image); err == nil {
			_, _ = kindio.Run(ctx, "kind", "load", "docker-image", "--name", spec.Name, image)
		}
	}
	config2, err := clientcmd.BuildConfigFromFlags("", spec.Kubeconfig)
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config2)
	if err != nil {
		return err
	}
	return setup(ctx, client, spec)
}

var _ sandbox.Provider = Provider{}

// observabilityObjects are the Prometheus recipe's objects plus the NodePort
// Services and the tracing store.
func observabilityObjects(spec sandbox.Spec) (telemetry.BackendObjects, []*corev1.Service, *appsv1.Deployment) {
	objects := recipe.Recipe{}.Build(telemetry.BackendParams{
		ApplicationNamespace: spec.Namespace, ControlNamespace: spec.ControlNamespace,
		PodSecurity: install.PodSecurityContext(), ContainerSecurity: install.ContainerSecurityContext(),
	})
	prometheusLabels := map[string]string{"app.kubernetes.io/name": recipe.Name, "app.kubernetes.io/managed-by": "sdo"}
	tracesLabels := map[string]string{"app.kubernetes.io/name": tracesName}
	one := int32(1)
	traces := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: tracesName, Namespace: spec.ControlNamespace, Labels: tracesLabels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: tracesLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: tracesLabels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name: "jaeger", Image: TracesImage,
					Ports: []corev1.ContainerPort{{Name: "ui", ContainerPort: 16686}, {Name: "otlp-grpc", ContainerPort: 4317}, {Name: "otlp-http", ContainerPort: 4318}},
					Env:   []corev1.EnvVar{{Name: "COLLECTOR_OTLP_ENABLED", Value: "true"}},
				}}},
			},
		},
	}
	services := []*corev1.Service{
		{
			ObjectMeta: metav1.ObjectMeta{Name: recipe.Name + "-host", Namespace: spec.ControlNamespace},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort, Selector: prometheusLabels, Ports: []corev1.ServicePort{
				{Name: "http", Port: 9090, TargetPort: intstr.FromInt32(9090), NodePort: prometheusNodePort}}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: tracesName, Namespace: spec.ControlNamespace},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort, Selector: tracesLabels, Ports: []corev1.ServicePort{
				{Name: "ui", Port: 16686, TargetPort: intstr.FromInt32(16686), NodePort: tracesNodePort},
				{Name: "otlp-grpc", Port: 4317, TargetPort: intstr.FromInt32(4317)},
				{Name: "otlp-http", Port: 4318, TargetPort: intstr.FromInt32(4318)}}},
		},
	}
	return objects, services, traces
}

// ensureNamespace creates a namespace if missing.
func ensureNamespace(ctx context.Context, client kubernetes.Interface, name string) error {
	_, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// setup installs the observability stack and waits for Prometheus.
func setup(ctx context.Context, client kubernetes.Interface, spec sandbox.Spec) error {
	for _, ns := range []string{spec.ControlNamespace, spec.Namespace} {
		if ns == "" {
			continue
		}
		if err := ensureNamespace(ctx, client, ns); err != nil {
			return err
		}
	}
	objects, services, traces := observabilityObjects(spec)
	if err := install.ApplyBackend(ctx, client, objects); err != nil {
		return err
	}
	for _, svc := range services {
		if _, err := client.CoreV1().Services(svc.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	if _, err := client.AppsV1().Deployments(traces.Namespace).Create(ctx, traces, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	// Only Prometheus gates readiness; the tracing store may still be pulling.
	deadline, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	for {
		d, err := client.AppsV1().Deployments(spec.ControlNamespace).Get(deadline, recipe.Name, metav1.GetOptions{})
		if err == nil && d.Status.AvailableReplicas >= 1 {
			return nil
		}
		if err := kindio.Sleep(deadline, 2*time.Second); err != nil {
			return fmt.Errorf("wait for %s: %w", recipe.Name, err)
		}
	}
}
