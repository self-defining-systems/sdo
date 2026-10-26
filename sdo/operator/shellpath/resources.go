package shellpath

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

type RunnerConfig struct{ Namespace, ControlNamespace, Image, RepositoryPVC, RepositoryMountPath, RepositorySubPath, AcceptedRoot string }

func RunnerResources(c RunnerConfig) ([]kruntime.Object, error) {
	if c.Namespace == "" || c.ControlNamespace == "" || c.Image == "" || c.RepositoryPVC == "" || !filepath.IsAbs(c.RepositoryMountPath) || !filepath.IsAbs(c.AcceptedRoot) {
		return nil, fmt.Errorf("shell runner requires app/control namespace, image and absolute repository mount/root")
	}
	relative, err := filepath.Rel(c.RepositoryMountPath, c.AcceptedRoot)
	if err != nil || !filepath.IsLocal(relative) {
		return nil, fmt.Errorf("shell accepted repository must be inside its read-only mount")
	}
	yes, no := true, false
	user := int64(65532)
	labels := map[string]string{"app.kubernetes.io/managed-by": "sdo", "sdo.dev/component": RunnerName}
	account := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: RunnerName, Namespace: c.ControlNamespace,Labels:labels}}
	rule := func(group string, resources ...string) rbacv1.PolicyRule {
		return rbacv1.PolicyRule{APIGroups: []string{group}, Resources: resources, Verbs: []string{"get", "list", "watch"}}
	}
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: RunnerName, Namespace: c.Namespace, Labels:labels}, Rules: []rbacv1.PolicyRule{
		rule("", "pods", "pods/log", "services", "endpoints", "events", "configmaps", "persistentvolumeclaims", "serviceaccounts", "resourcequotas", "limitranges"),
		rule("apps", "deployments", "statefulsets", "daemonsets", "replicasets", "controllerrevisions"),
		rule("batch", "jobs", "cronjobs"), rule("networking.k8s.io", "networkpolicies", "ingresses"), rule("discovery.k8s.io", "endpointslices"),
		rule("autoscaling", "horizontalpodautoscalers"), rule("policy", "poddisruptionbudgets"), rule("rbac.authorization.k8s.io", "roles", "rolebindings"), rule("metrics.k8s.io", "pods"),
	}}
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: RunnerName, Namespace: c.Namespace, Labels:labels}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: RunnerName, Namespace: c.ControlNamespace,Labels:labels}}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: RunnerName}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: RunnerName, Namespace: c.ControlNamespace, Labels: labels}, Spec: corev1.PodSpec{
		ServiceAccountName: RunnerName, RestartPolicy: corev1.RestartPolicyAlways,
		SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &yes, RunAsUser: &user, FSGroup: &user, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
		Containers: []corev1.Container{{Name: "runner", Image: c.Image, ImagePullPolicy: corev1.PullIfNotPresent, Command: []string{"/usr/local/bin/sdo-operator", "shell-detector-runner"},
			Env:             []corev1.EnvVar{{Name: "SDO_SHELL_NAMESPACE", Value: c.Namespace}, {Name: "SDO_SHELL_REPOSITORY", Value: c.AcceptedRoot}, {Name: "HOME", Value: "/tmp"}},
			Ports:           []corev1.ContainerPort{{Name: "results", ContainerPort: RunnerPort}},
			SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
			VolumeMounts:    []corev1.VolumeMount{{Name: "repository", MountPath: c.RepositoryMountPath, SubPath: c.RepositorySubPath, ReadOnly: true}, {Name: "scratch", MountPath: "/tmp"}},
		}},
		Volumes: []corev1.Volume{{Name: "repository", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: c.RepositoryPVC, ReadOnly: true}}}, {Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
	}}
	encoded, _ := json.Marshal(pod.Spec)
	digest := sha256.Sum256(encoded)
	pod.Annotations = map[string]string{"sdo.dev/runner-config": fmt.Sprintf("%x", digest)}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: RunnerName, Namespace: c.ControlNamespace, Labels: labels}, Spec: corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{{Name: "results", Port: RunnerPort, TargetPort: intstr.FromInt(RunnerPort)}}}}
	return []kruntime.Object{account, role, binding, service, pod}, nil
}
func ensureRBAC(ctx context.Context, client kubernetes.Interface, object kruntime.Object) error {
	switch want := object.(type) {
	case *rbacv1.Role:
		existing, err := client.RbacV1().Roles(want.Namespace).Get(ctx, want.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = client.RbacV1().Roles(want.Namespace).Create(ctx, want, metav1.CreateOptions{})
			return err
		}
		if err != nil {
			return err
		}
		want.ResourceVersion = existing.ResourceVersion
		_, err = client.RbacV1().Roles(want.Namespace).Update(ctx, want, metav1.UpdateOptions{})
		return err
	case *rbacv1.RoleBinding:
		existing, err := client.RbacV1().RoleBindings(want.Namespace).Get(ctx, want.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = client.RbacV1().RoleBindings(want.Namespace).Create(ctx, want, metav1.CreateOptions{})
			return err
		}
		if err != nil {
			return err
		}
		want.ResourceVersion = existing.ResourceVersion
		_, err = client.RbacV1().RoleBindings(want.Namespace).Update(ctx, want, metav1.UpdateOptions{})
		return err
	default:
		return fmt.Errorf("unexpected shell runner object %T", object)
	}
}
