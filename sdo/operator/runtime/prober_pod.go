package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/runtime/prober"
)

const (
	ProberName              = "sdo-prober"
	proberFingerprintLabel  = "sdo.dev/prober-fingerprint"
	proberBinaryMountPath   = "/opt/sdo-prober"
	defaultProberReadyAfter = 90 * time.Second
)

// ProberPod runs the application's compiled generators as an isolated pod in
// the control namespace: no service-account token, a read-only mount of only
// the prober binary, CPU and memory limits, and a NetworkPolicy that allows
// egress only to the application namespace and cluster DNS and ingress only
// from the control namespace. Kubernetes restarts it if it crashes; the
// controller only reads its API.
type ProberPod struct {
	Client               kubernetes.Interface
	Namespace            string
	AppNamespace         string
	Image                string
	RepositoryPVC        string
	RepositoryMountPath  string
	RepositoryPVCSubPath string
	// Binary is the prober executable on the shared repository volume, as
	// seen from the controller at RepositoryMountPath.
	Binary       string
	ReadyTimeout time.Duration

	mu          sync.Mutex
	fingerprint string
	address     string
}

// Fingerprint identifies the prober binary, so an unchanged prober pod
// survives controller relaunches and keeps its observations.
func (p *ProberPod) Fingerprint() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fingerprint != "" {
		return p.fingerprint, nil
	}
	file, err := os.Open(p.Binary)
	if err != nil {
		return "", fmt.Errorf("open prober binary: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash prober binary: %w", err)
	}
	p.fingerprint = hex.EncodeToString(hash.Sum(nil))[:16]
	return p.fingerprint, nil
}

func (p *ProberPod) binarySubPath() (string, string, error) {
	mountRoot := path.Clean(p.RepositoryMountPath)
	binary := path.Clean(filepath.ToSlash(p.Binary))
	if !path.IsAbs(binary) || !strings.HasPrefix(binary, mountRoot+"/") {
		return "", "", fmt.Errorf("prober binary %q must be inside the shared repository mount %q", p.Binary, p.RepositoryMountPath)
	}
	relative := strings.TrimPrefix(path.Dir(binary), mountRoot+"/")
	if p.RepositoryPVCSubPath != "" {
		relative = path.Join(p.RepositoryPVCSubPath, relative)
	}
	return relative, path.Base(binary), nil
}

// Manifests returns the prober's NetworkPolicy and Pod.
func (p *ProberPod) Manifests() (*networkingv1.NetworkPolicy, *corev1.Pod, error) {
	if p.Namespace == "" || p.AppNamespace == "" || p.Image == "" || p.RepositoryPVC == "" {
		return nil, nil, fmt.Errorf("prober pod needs namespaces, image, and repository PVC")
	}
	fingerprint, err := p.Fingerprint()
	if err != nil {
		return nil, nil, err
	}
	subPath, executable, err := p.binarySubPath()
	if err != nil {
		return nil, nil, err
	}
	labels := map[string]string{"app.kubernetes.io/name": ProberName, "app.kubernetes.io/managed-by": "sdo"}
	tcp, udp := corev1.ProtocolTCP, corev1.ProtocolUDP
	dns := intstr.FromInt32(53)
	policy := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: ProberName, Namespace: p.Namespace, Labels: labels},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": ProberName}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}},
			}},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{To: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"kubernetes.io/metadata.name": p.AppNamespace},
				}}}},
				{
					To: []networkingv1.NetworkPolicyPeer{{
						NamespaceSelector: &metav1.LabelSelector{},
						PodSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{"k8s-app": "kube-dns"}},
					}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: &udp, Port: &dns}, {Protocol: &tcp, Port: &dns}},
				},
			},
		},
	}
	falseValue, trueValue := false, true
	user := int64(65532)
	podLabels := map[string]string{proberFingerprintLabel: fingerprint}
	for key, value := range labels {
		podLabels[key] = value
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: ProberName, Namespace: p.Namespace, Labels: podLabels},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: &falseValue,
			EnableServiceLinks:           &falseValue,
			RestartPolicy:                corev1.RestartPolicyAlways,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: &trueValue, RunAsUser: &user,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:    "prober",
				Image:   p.Image,
				Command: []string{path.Join(proberBinaryMountPath, executable)},
				Args: []string{
					"--namespace", p.AppNamespace, "--listen", ":" + strconv.Itoa(prober.DefaultPort),
				},
				ImagePullPolicy: corev1.PullIfNotPresent,
				Ports:           []corev1.ContainerPort{{Name: "api", ContainerPort: prober.DefaultPort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: prober.PathHealth, Port: intstr.FromInt32(prober.DefaultPort),
					}},
					PeriodSeconds: 1,
				},
				VolumeMounts: []corev1.VolumeMount{{
					Name: "prober", MountPath: proberBinaryMountPath, SubPath: subPath, ReadOnly: true,
				}},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &falseValue,
					ReadOnlyRootFilesystem:   &trueValue,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("25m"), corev1.ResourceMemory: resource.MustParse("32Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
				},
			}},
			Volumes: []corev1.Volume{{
				Name: "prober", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: p.RepositoryPVC, ReadOnly: true,
				}},
			}},
		},
	}
	return policy, pod, nil
}

// Ensure creates or replaces the prober so it runs the current binary and
// returns its API address once it is ready. An existing ready prober with
// the same fingerprint is reused.
func (p *ProberPod) Ensure(ctx context.Context) (string, error) {
	policy, desired, err := p.Manifests()
	if err != nil {
		return "", err
	}
	policies := p.Client.NetworkingV1().NetworkPolicies(p.Namespace)
	if existing, err := policies.Get(ctx, ProberName, metav1.GetOptions{}); err == nil {
		existing.Spec = policy.Spec
		existing.Labels = policy.Labels
		if _, err := policies.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return "", fmt.Errorf("update prober NetworkPolicy: %w", err)
		}
	} else if apierrors.IsNotFound(err) {
		if _, err := policies.Create(ctx, policy, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("create prober NetworkPolicy: %w", err)
		}
	} else {
		return "", fmt.Errorf("get prober NetworkPolicy: %w", err)
	}
	pods := p.Client.CoreV1().Pods(p.Namespace)
	existing, err := pods.Get(ctx, ProberName, metav1.GetOptions{})
	switch {
	case err == nil && existing.DeletionTimestamp == nil &&
		existing.Labels[proberFingerprintLabel] == desired.Labels[proberFingerprintLabel] &&
		existing.Status.Phase != corev1.PodFailed && existing.Status.Phase != corev1.PodSucceeded:
	case err == nil:
		if err := p.deleteAndWait(ctx); err != nil {
			return "", err
		}
		fallthrough
	case apierrors.IsNotFound(err):
		if _, err := pods.Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("create prober pod: %w", err)
		}
	default:
		return "", fmt.Errorf("get prober pod: %w", err)
	}
	return p.waitReady(ctx)
}

func (p *ProberPod) deleteAndWait(ctx context.Context) error {
	pods := p.Client.CoreV1().Pods(p.Namespace)
	grace := int64(0)
	if err := pods.Delete(ctx, ProberName, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete stale prober pod: %w", err)
	}
	for {
		if _, err := pods.Get(ctx, ProberName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (p *ProberPod) waitReady(ctx context.Context) (string, error) {
	timeout := p.ReadyTimeout
	if timeout <= 0 {
		timeout = defaultProberReadyAfter
	}
	deadline := time.Now().Add(timeout)
	for {
		pod, err := p.Client.CoreV1().Pods(p.Namespace).Get(ctx, ProberName, metav1.GetOptions{})
		if err == nil && podReady(pod) && pod.Status.PodIP != "" {
			address := fmt.Sprintf("http://%s:%d", pod.Status.PodIP, prober.DefaultPort)
			p.mu.Lock()
			p.address = address
			p.mu.Unlock()
			return address, nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return "", fmt.Errorf("prober pod not ready after %s: %w", timeout, err)
			}
			return "", fmt.Errorf("prober pod not ready after %s: phase %s", timeout, pod.Status.Phase)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Address returns the ready prober's API address. With refresh, or before
// the pod has been located, it re-ensures the pod, which also replaces a
// deleted or failed prober.
func (p *ProberPod) Address(ctx context.Context, refresh bool) (string, error) {
	p.mu.Lock()
	address := p.address
	p.mu.Unlock()
	if address != "" && !refresh {
		return address, nil
	}
	return p.Ensure(ctx)
}

// ServiceAddress ensures the prober runs and returns its Service address.
// Responders get this address, not the pod IP: they may run for many minutes,
// and the controller replaces a deleted or failed prober pod with a new IP.
func (p *ProberPod) ServiceAddress(ctx context.Context, refresh bool) (string, error) {
	if _, err := p.Address(ctx, refresh); err != nil {
		return "", err
	}
	if err := p.ensureService(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s.%s.svc:%d", ProberName, p.Namespace, prober.DefaultPort), nil
}

func (p *ProberPod) ensureService(ctx context.Context) error {
	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: ProberName, Namespace: p.Namespace,
			Labels: map[string]string{"app.kubernetes.io/name": ProberName, "app.kubernetes.io/managed-by": "sdo"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app.kubernetes.io/name": ProberName},
			Ports: []corev1.ServicePort{{
				Name: "api", Port: prober.DefaultPort, TargetPort: intstr.FromInt32(prober.DefaultPort),
			}},
		},
	}
	services := p.Client.CoreV1().Services(p.Namespace)
	if _, err := services.Get(ctx, ProberName, metav1.GetOptions{}); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get prober Service: %w", err)
	}
	if _, err := services.Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create prober Service: %w", err)
	}
	return nil
}

func podReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
