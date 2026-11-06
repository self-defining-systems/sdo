package install

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/telemetry"
)

// PrometheusName is the Service name of the SDO-owned Prometheus recipe.
const PrometheusName = "sdo-prometheus"

// prometheusRecipe is the recipe the observability stack applies.
const prometheusRecipe = "prometheus"

// EffectiveMetricsURL preserves an explicit external source even when the stack is enabled.
func (c Config) EffectiveMetricsURL() string {
	if c.MetricsURL != "" {
		return c.MetricsURL
	}
	if c.DisableObservability {
		return ""
	}
	return fmt.Sprintf("http://%s.%s.svc:9090", PrometheusName, c.controlNamespace())
}

// addObservability asks the configured Prometheus recipe for its objects.
func addObservability(p *Plan, c Config) error {
	var recipe telemetry.BackendRecipe
	for _, r := range c.BackendRecipes {
		if r.Name() == prometheusRecipe {
			recipe = r
		}
	}
	if recipe == nil {
		return fmt.Errorf("install: the observability stack needs the %q backend recipe in Config.BackendRecipes", prometheusRecipe)
	}
	p.Backend = recipe.Build(telemetry.BackendParams{ApplicationNamespace: c.Namespace, ControlNamespace: c.controlNamespace(), PodSecurity: podSecurity(), ContainerSecurity: containerSecurity()})
	p.ServiceAccounts = append(p.ServiceAccounts, p.Backend.ServiceAccounts...)
	p.ClusterRoles = append(p.ClusterRoles, p.Backend.ClusterRoles...)
	p.ClusterBindings = append(p.ClusterBindings, p.Backend.ClusterBindings...)
	return nil
}

func (i Installer) ensureObservability(ctx context.Context, p Plan) error {
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

// TelemetryEnvironment is the environment document the controller reads: the
// effective metrics URL as the operator-provided source `default` and the
// SDO-owned publishing base. Until the telemetry profile replaces the legacy
// install fields, this is the single place they become controller input.
func (c Config) TelemetryEnvironment() telemetry.Environment {
	env := telemetry.Environment{Version: telemetry.EnvironmentVersion}
	if url := c.EffectiveMetricsURL(); url != "" {
		env.Provided = []telemetry.Binding{{
			ID: "default", Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics},
			Endpoint: telemetry.Endpoint{URL: url}, Default: true, Origin: telemetry.OriginOperator,
		}}
	}
	if !c.DisableObservability {
		env.MetricsPublishURL = fmt.Sprintf("http://%s.%s.svc:9090", PrometheusName, c.controlNamespace())
	}
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

func (c Config) MetricsSource() *contracts.MetricsSource {
	source := &contracts.MetricsSource{QueryUrl: c.EffectiveMetricsURL()}
	if !c.DisableObservability {
		source.AnnotatedPodScraping = true
		source.OtlpMetricsUrl = fmt.Sprintf("http://%s.%s.svc:9090/api/v1/otlp/v1/metrics", PrometheusName, c.controlNamespace())
	}
	return source
}

func rule(group string, resources []string, verbs ...string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{APIGroups: []string{group}, Resources: resources, Verbs: verbs}
}
