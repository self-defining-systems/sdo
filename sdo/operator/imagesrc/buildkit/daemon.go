package buildkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk/podtext"
)

// DaemonName names the install's BuildKit daemon Pod and its Service.
const DaemonName = "sdo-buildkitd"

const (
	daemonPort           = 1234
	daemonSpecAnnotation = "sdo.dev/buildkitd-spec"
	daemonSocket         = "unix:///run/user/1000/buildkit/buildkitd.sock"
)

// daemonMu serializes ensureDaemon within the operate process: concurrent
// builds of one deploy all ensure the same daemon.
var daemonMu sync.Mutex

func daemonLabels() map[string]string {
	return map[string]string{"app.kubernetes.io/managed-by": "sdo", "app.kubernetes.io/name": DaemonName, "sdo.dev/component": "image-build"}
}

// daemonAddress is where build Jobs reach the daemon.
func (b Builder) daemonAddress() string {
	return fmt.Sprintf("tcp://%s.%s.svc:%d", DaemonName, b.Namespace, daemonPort)
}

// daemonPod is the install's one rootless BuildKit daemon. Its layer store is
// node-local (an emptyDir), so every build of the install, including a failed
// one, leaves its finished steps and pulled base images for the next; the
// registry cache still seeds a fresh daemon. It holds no credentials: each
// client Job authenticates its own pulls and pushes through the build session.
// The spec annotation names everything that shapes the Pod, so a new BuildKit
// image or registry setting replaces it.
func (b Builder) daemonPod() *corev1.Pod {
	no := false
	// BuildKit's default collector prunes to 10GB whenever the node has less
	// than 20% of its disk free, which on a busy node evicts an app's working
	// set between two builds. Keep 20GB and prune only near the kubelet's
	// eviction line (10% free).
	config := "[worker.oci]\n  reservedSpace = \"20GB\"\n  minFreeSpace = \"15%\"\n  maxUsedSpace = \"80GB\"\n"
	if b.RegistryInsecure {
		config += fmt.Sprintf("[registry.%q]\n  http = true\n  insecure = true\n", registryHostOf(b.AllowedRegistryHost))
	}
	script := `printf '%s' "$SDO_BUILDKITD_CONFIG" > /tmp/buildkitd.toml
exec rootlesskit buildkitd --addr "tcp://0.0.0.0:$SDO_BUILDKITD_PORT" --addr ` + daemonSocket + ` --oci-worker-no-process-sandbox --config /tmp/buildkitd.toml`
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: DaemonName, Namespace: b.Namespace, Labels: daemonLabels(), Annotations: map[string]string{}},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyAlways, ServiceAccountName: b.ServiceAccount, AutomountServiceAccountToken: &no,
			SecurityContext: &corev1.PodSecurityContext{RunAsUser: ptr64(1000), RunAsGroup: ptr64(1000), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}},
			Containers: []corev1.Container{{Name: "buildkitd", Image: b.BuildKitImage, Command: []string{"/bin/sh", "-ec"}, Args: []string{script},
				Env:   []corev1.EnvVar{{Name: "SDO_BUILDKITD_CONFIG", Value: config}, {Name: "SDO_BUILDKITD_PORT", Value: fmt.Sprint(daemonPort)}},
				Ports: []corev1.ContainerPort{{Name: "buildkitd", ContainerPort: daemonPort}},
				ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"buildctl", "--addr", daemonSocket, "debug", "workers"}}}, PeriodSeconds: 2},
				// Enough to stay off the eviction list; builds burst to the node.
				Resources:    corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")}},
				VolumeMounts: []corev1.VolumeMount{{Name: "buildkit-state", MountPath: "/home/user/.local/share/buildkit"}},
				// RootlessKit needs newuidmap's setuid transition (see the Job).
				SecurityContext: &corev1.SecurityContext{RunAsNonRoot: ptrBool(true), AppArmorProfile: &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined}},
			}},
			Volumes: []corev1.Volume{{Name: "buildkit-state", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
		},
	}
	podtext.Literal(&pod.Spec)
	spec, _ := json.Marshal(pod.Spec)
	sum := sha256.Sum256(spec)
	pod.Annotations[daemonSpecAnnotation] = hex.EncodeToString(sum[:8])
	return pod
}

func (b Builder) daemonService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: DaemonName, Namespace: b.Namespace, Labels: daemonLabels()},
		Spec: corev1.ServiceSpec{Selector: map[string]string{"app.kubernetes.io/name": DaemonName, "sdo.dev/component": "image-build"},
			Ports: []corev1.ServicePort{{Name: "buildkitd", Port: daemonPort, TargetPort: intstr.FromInt32(daemonPort)}}},
	}
}

// ensureDaemon makes the install's daemon Pod and Service exist with the
// current spec. A running current daemon is kept, with its layer store; a
// stale or stopped one is replaced. Build Jobs wait for it to answer.
func (b Builder) ensureDaemon(ctx context.Context) error {
	daemonMu.Lock()
	defer daemonMu.Unlock()
	pods := b.Client.CoreV1().Pods(b.Namespace)
	want := b.daemonPod()
	got, err := pods.Get(ctx, DaemonName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return fmt.Errorf("read BuildKit daemon: %w", err)
	case got.DeletionTimestamp == nil && got.Annotations[daemonSpecAnnotation] == want.Annotations[daemonSpecAnnotation] && got.Status.Phase != corev1.PodFailed && got.Status.Phase != corev1.PodSucceeded:
		return b.ensureDaemonService(ctx)
	default:
		zero := int64(0)
		uid := got.UID
		if err := pods.Delete(ctx, DaemonName, metav1.DeleteOptions{GracePeriodSeconds: &zero, Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return fmt.Errorf("replace BuildKit daemon: %w", err)
		}
		if err := b.waitDaemonGone(ctx, uid); err != nil {
			return err
		}
	}
	if _, err := pods.Create(ctx, want, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("start BuildKit daemon: %w", err)
	}
	return b.ensureDaemonService(ctx)
}

func (b Builder) waitDaemonGone(ctx context.Context, uid any) error {
	bounded, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		got, err := b.Client.CoreV1().Pods(b.Namespace).Get(bounded, DaemonName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) || err == nil && got.UID != uid {
			return nil
		}
		if err != nil {
			return fmt.Errorf("wait for the old BuildKit daemon to stop: %w", err)
		}
		if err := b.pause(bounded, time.Second); err != nil {
			return fmt.Errorf("wait for the old BuildKit daemon to stop: %w", err)
		}
	}
}

func (b Builder) ensureDaemonService(ctx context.Context) error {
	if _, err := b.Client.CoreV1().Services(b.Namespace).Create(ctx, b.daemonService(), metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("expose BuildKit daemon: %w", err)
	}
	return nil
}
