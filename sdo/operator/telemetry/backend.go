package telemetry

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
)

// BackendParams tells a recipe where it runs and what it may observe. The
// security contexts are the installer's restricted contexts for its own Pods.
type BackendParams struct {
	ApplicationNamespace string
	ControlNamespace     string
	PodSecurity          *corev1.PodSecurityContext
	ContainerSecurity    *corev1.SecurityContext
}

// BackendObjects is what a recipe asks the installer to apply. Namespaced
// objects live in the control namespace; the installer applies them with its
// own identity, so a recipe is code reviewed with SDO, never designer-authored.
type BackendObjects struct {
	ConfigMaps      []*corev1.ConfigMap
	ServiceAccounts []*corev1.ServiceAccount
	ClusterRoles    []*rbacv1.ClusterRole
	ClusterBindings []*rbacv1.ClusterRoleBinding
	Services        []*corev1.Service
	Deployments     []*appsv1.Deployment
}

// BackendRecipe is a named, SDO-owned backend object set (for example the
// Prometheus recipe). Build is pure.
type BackendRecipe interface {
	Name() string
	Build(BackendParams) BackendObjects
}
