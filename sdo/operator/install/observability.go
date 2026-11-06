package install

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/telemetry"
)

// addObservability applies the backend recipes the telemetry profile selects.
func addObservability(p *Plan, c Config) error {
	for _, name := range c.TelemetryProfile.Recipes {
		var recipe telemetry.BackendRecipe
		for _, r := range c.BackendRecipes {
			if r.Name() == name {
				recipe = r
			}
		}
		if recipe == nil {
			return fmt.Errorf("install: telemetry profile %q applies backend recipe %q, which is not in Config.BackendRecipes", c.TelemetryProfile.Name, name)
		}
		objects := recipe.Build(telemetry.BackendParams{ApplicationNamespace: c.Namespace, ControlNamespace: c.controlNamespace(), PodSecurity: podSecurity(), ContainerSecurity: containerSecurity()})
		p.Backend.ConfigMaps = append(p.Backend.ConfigMaps, objects.ConfigMaps...)
		p.Backend.ServiceAccounts = append(p.Backend.ServiceAccounts, objects.ServiceAccounts...)
		p.Backend.ClusterRoles = append(p.Backend.ClusterRoles, objects.ClusterRoles...)
		p.Backend.ClusterBindings = append(p.Backend.ClusterBindings, objects.ClusterBindings...)
		p.Backend.Services = append(p.Backend.Services, objects.Services...)
		p.Backend.Deployments = append(p.Backend.Deployments, objects.Deployments...)
		p.ServiceAccounts = append(p.ServiceAccounts, objects.ServiceAccounts...)
		p.ClusterRoles = append(p.ClusterRoles, objects.ClusterRoles...)
		p.ClusterBindings = append(p.ClusterBindings, objects.ClusterBindings...)
	}
	return nil
}

func (i Installer) ensureObservability(ctx context.Context, p Plan) error {
	return applyBackendWorkloads(ctx, i.Client, p.Backend)
}

// ContainerSecurityContext is the restricted container context the installer
// gives its own Pods, offered to recipes through telemetry.BackendParams.
func ContainerSecurityContext() *corev1.SecurityContext { return containerSecurity() }

// ApplyBackend applies a recipe's complete object set with the installer's own
// create-or-update semantics: ServiceAccounts, cluster grants, then the
// namespaced workloads. It is the trusted applier's write path for backends
// chosen after install; the objects come from a reviewed recipe, never from
// designer files.
func ApplyBackend(ctx context.Context, client kubernetes.Interface, objects telemetry.BackendObjects) error {
	for _, sa := range objects.ServiceAccounts {
		accounts := client.CoreV1().ServiceAccounts(sa.Namespace)
		if _, err := accounts.Create(ctx, sa, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("ensure backend ServiceAccount %s: %w", sa.Name, err)
		}
	}
	if err := (Installer{Client: client}).ensureClusterGrants(ctx, Plan{ClusterRoles: objects.ClusterRoles, ClusterBindings: objects.ClusterBindings}); err != nil {
		return err
	}
	return applyBackendWorkloads(ctx, client, objects)
}

func applyBackendWorkloads(ctx context.Context, client kubernetes.Interface, backend telemetry.BackendObjects) error {
	i := Installer{Client: client}
	p := Plan{Backend: backend}
	for _, cm := range p.Backend.ConfigMaps {
		cms := i.Client.CoreV1().ConfigMaps(cm.Namespace)
		prior, err := cms.Get(ctx, cm.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = cms.Create(ctx, cm, metav1.CreateOptions{})
		} else if err == nil {
			want := cm.DeepCopy()
			want.ResourceVersion = prior.ResourceVersion
			_, err = cms.Update(ctx, want, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("ensure backend config %s: %w", cm.Name, err)
		}
	}
	for _, svc := range p.Backend.Services {
		services := i.Client.CoreV1().Services(svc.Namespace)
		prior, err := services.Get(ctx, svc.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = services.Create(ctx, svc, metav1.CreateOptions{})
		} else if err == nil {
			want := prior.DeepCopy()
			want.Spec.Selector = svc.Spec.Selector
			want.Spec.Ports = svc.Spec.Ports
			_, err = services.Update(ctx, want, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("ensure backend service %s: %w", svc.Name, err)
		}
	}
	for _, d := range p.Backend.Deployments {
		deployments := i.Client.AppsV1().Deployments(d.Namespace)
		prior, err := deployments.Get(ctx, d.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = deployments.Create(ctx, d, metav1.CreateOptions{})
		} else if err == nil {
			want := d.DeepCopy()
			want.ResourceVersion = prior.ResourceVersion
			_, err = deployments.Update(ctx, want, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("ensure backend deployment %s: %w", d.Name, err)
		}
	}
	return nil
}

const (
	// TelemetryEnvironmentName is the ConfigMap holding the controller's
	// telemetry environment document.
	TelemetryEnvironmentName = "sdo-telemetry-environment"
	// TelemetryEnvironmentDir is where the controller pod mounts it.
	TelemetryEnvironmentDir  = "/sdo/telemetry"
	telemetryEnvironmentFile = "environment.json"
)

// TelemetryEnvironmentFile is the path of the environment document in the
// controller pod.
func TelemetryEnvironmentFile() string {
	return TelemetryEnvironmentDir + "/" + telemetryEnvironmentFile
}

// TelemetryEnvironment is the environment document the controller reads,
// derived from the telemetry profile. An unset profile provides nothing.
func (c Config) TelemetryEnvironment() telemetry.Environment {
	env := c.TelemetryProfile.Environment()
	env.ControlNamespace, env.ApplicationNamespace = c.controlNamespace(), c.Namespace
	return env
}

func telemetryEnvironmentConfigMap(c Config) (*corev1.ConfigMap, error) {
	document, err := c.TelemetryEnvironment().Marshal()
	if err != nil {
		return nil, fmt.Errorf("telemetry environment: %w", err)
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: TelemetryEnvironmentName, Namespace: c.controlNamespace()},
		Data:       map[string]string{telemetryEnvironmentFile: string(document)},
	}, nil
}

func (i Installer) ensureTelemetryEnvironment(ctx context.Context, p Plan) error {
	if p.TelemetryEnvironment == nil {
		return nil
	}
	configMaps := i.Client.CoreV1().ConfigMaps(p.TelemetryEnvironment.Namespace)
	prior, err := configMaps.Get(ctx, TelemetryEnvironmentName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = configMaps.Create(ctx, p.TelemetryEnvironment, metav1.CreateOptions{})
	} else if err == nil {
		want := p.TelemetryEnvironment.DeepCopy()
		want.ResourceVersion = prior.ResourceVersion
		_, err = configMaps.Update(ctx, want, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure telemetry environment: %w", err)
	}
	return nil
}

func rule(group string, resources []string, verbs ...string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{APIGroups: []string{group}, Resources: resources, Verbs: verbs}
}
