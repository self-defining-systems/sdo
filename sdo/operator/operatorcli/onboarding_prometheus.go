package operatorcli

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/install"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/prometheus/recipe"
)

// promTarget is where the onboarding check reads metrics: a URL, or a Service
// port reached through the API server proxy.
type promTarget struct {
	URL       string
	Namespace string
	Service   string
	Port      int
}

const ownPrometheusPoll = 2 * time.Second

// stuckWaitingReasons are container waiting reasons that are not progress.
var stuckWaitingReasons = map[string]bool{
	"ErrImagePull": true, "ImagePullBackOff": true, "InvalidImageName": true,
	"CrashLoopBackOff": true, "CreateContainerConfigError": true, "CreateContainerError": true,
}

// resolvePrometheus turns --prometheus into a target. Given, it is used as is.
// Empty, SDO's own Prometheus recipe is applied into the control namespace
// with the installer's write path (a later install adopts the same objects)
// and the check waits until its Deployment is ready.
func resolvePrometheus(ctx context.Context, client kubernetes.Interface, given, appNamespace, controlNamespace string, wait func(context.Context, time.Duration) error, notice io.Writer) (promTarget, error) {
	if given != "" {
		if strings.HasPrefix(given, "http://") || strings.HasPrefix(given, "https://") {
			return promTarget{URL: given}, nil
		}
		ns, rest, ok := strings.Cut(given, "/")
		name, port, ok2 := strings.Cut(rest, ":")
		number, convErr := strconv.Atoi(port)
		if !ok || !ok2 || convErr != nil || ns == "" || name == "" {
			return promTarget{}, fmt.Errorf("--prometheus %q is neither a URL nor NS/SERVICE:PORT", given)
		}
		return promTarget{Namespace: ns, Service: name, Port: number}, nil
	}
	fmt.Fprintf(notice, "no --prometheus given: using SDO's own Prometheus in namespace %s (%s:9090)\n", controlNamespace, recipe.Name)
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: controlNamespace}}
	if _, err := client.CoreV1().Namespaces().Create(ctx, namespace, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return promTarget{}, fmt.Errorf("create control namespace %s: %w", controlNamespace, err)
	}
	objects := recipe.New().Build(telemetry.BackendParams{
		ApplicationNamespace: appNamespace, ControlNamespace: controlNamespace,
		PodSecurity: install.PodSecurityContext(), ContainerSecurity: install.ContainerSecurityContext(),
	})
	if err := install.ApplyBackend(ctx, client, objects); err != nil {
		return promTarget{}, fmt.Errorf("apply SDO's Prometheus: %w", err)
	}
	for _, d := range objects.Deployments {
		if err := awaitOwnPrometheus(ctx, client, d.Namespace, d.Name, wait); err != nil {
			return promTarget{}, err
		}
	}
	return promTarget{Namespace: controlNamespace, Service: recipe.Name, Port: 9090}, nil
}

// awaitOwnPrometheus waits while the Deployment makes progress and fails with
// what the cluster says once Kubernetes gives up or a container is stuck.
func awaitOwnPrometheus(ctx context.Context, client kubernetes.Interface, namespace, name string, wait func(context.Context, time.Duration) error) error {
	for {
		d, err := client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read SDO's Prometheus deployment %s/%s: %w", namespace, name, err)
		}
		want := int32(1)
		if d.Spec.Replicas != nil {
			want = *d.Spec.Replicas
		}
		if d.Status.ObservedGeneration >= d.Generation && d.Status.UpdatedReplicas >= want && d.Status.AvailableReplicas >= want {
			return nil
		}
		if reasons := ownPrometheusStuck(ctx, client, d); len(reasons) > 0 {
			return fmt.Errorf("SDO's Prometheus deployment %s/%s is not becoming ready: %s", namespace, name, strings.Join(reasons, "; "))
		}
		if err := wait(ctx, ownPrometheusPoll); err != nil {
			return err
		}
	}
}

func ownPrometheusStuck(ctx context.Context, client kubernetes.Interface, d *appsv1.Deployment) []string {
	var reasons []string
	if d.Status.ObservedGeneration >= d.Generation {
		for _, c := range d.Status.Conditions {
			if (c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse) || (c.Type == appsv1.DeploymentReplicaFailure && c.Status == corev1.ConditionTrue) {
				reasons = append(reasons, fmt.Sprintf("%s=%s %s: %s", c.Type, c.Status, c.Reason, c.Message))
			}
		}
	}
	if d.Spec.Selector == nil {
		return reasons
	}
	pods, err := client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{LabelSelector: metav1.FormatLabelSelector(d.Spec.Selector)})
	if err != nil {
		return reasons
	}
	for _, pod := range pods.Items {
		for _, status := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
			if w := status.State.Waiting; w != nil && stuckWaitingReasons[w.Reason] {
				reasons = append(reasons, fmt.Sprintf("pod %s container %s %s: %s", pod.Name, status.Name, w.Reason, w.Message))
			}
		}
	}
	return reasons
}
