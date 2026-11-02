package install

import (
	"context"
	"crypto/sha256"
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const PrometheusName = "sdo-prometheus"
const PrometheusImage = "quay.io/prometheus/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996"

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

func addObservability(p *Plan, c Config) {
	ns := c.controlNamespace()
	labels := map[string]string{"app.kubernetes.io/name": PrometheusName, "app.kubernetes.io/managed-by": "sdo"}
	meta := metav1.ObjectMeta{Name: PrometheusName, Namespace: ns, Labels: labels}
	config := prometheusConfig(c.Namespace)
	p.PrometheusConfig = &corev1.ConfigMap{ObjectMeta: meta, Data: map[string]string{"prometheus.yml": config}}
	p.ServiceAccounts = append(p.ServiceAccounts, &corev1.ServiceAccount{ObjectMeta: meta})
	roleName := PrometheusName + "-" + ns
	p.ClusterRoles = append(p.ClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: roleName}, Rules: []rbacv1.PolicyRule{rule("", []string{"pods", "nodes", "nodes/proxy", "nodes/metrics"}, "get", "list", "watch")}})
	p.ClusterBindings = append(p.ClusterBindings, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: roleName}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: roleName}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: PrometheusName, Namespace: ns}}})
	p.PrometheusService = &corev1.Service{ObjectMeta: meta, Spec: corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{{Name: "http", Port: 9090, TargetPort: intstr.FromInt32(9090)}}}}
	replicas := int32(1)
	p.Prometheus = &appsv1.Deployment{
		ObjectMeta: meta,
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{
					"sdo.dev/config-hash": fmt.Sprintf("%x", sha256.Sum256([]byte(config))),
				}},
				Spec: corev1.PodSpec{
					ServiceAccountName: PrometheusName, SecurityContext: podSecurity(),
					Containers: []corev1.Container{{
						Name: "prometheus", Image: PrometheusImage,
						Args: []string{
							"--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.path=/prometheus",
							"--storage.tsdb.retention.time=6h", "--storage.tsdb.retention.size=1GB",
							"--web.enable-otlp-receiver",
						},
						Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: 9090}},
						SecurityContext: containerSecurity(), Resources: limits("100m", "128Mi", "1", "1Gi"),
						ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
							Path: "/-/ready", Port: intstr.FromInt32(9090),
						}}},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "config", MountPath: "/etc/prometheus", ReadOnly: true},
							{Name: "data", MountPath: "/prometheus"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: PrometheusName},
						}}},
						{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: quantity("2Gi")}}},
					},
				},
			},
		},
	}
}

// Pod discovery also supplies the distinct node proxy targets. Dropping pod-specific
// labels makes all pods on one node deduplicate to one kubelet/cAdvisor scrape.
func prometheusConfig(namespace string) string {
	return fmt.Sprintf(`global:
  scrape_interval: 15s
scrape_configs:
- job_name: application-pods
  kubernetes_sd_configs:
  - role: pod
    namespaces: {names: [%s]}
  relabel_configs:
  - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
    regex: "true"
    action: keep
  - source_labels: [__meta_kubernetes_pod_phase]
    regex: Running
    action: keep
  - source_labels: [__meta_kubernetes_pod_ip, __meta_kubernetes_pod_annotation_prometheus_io_port]
    regex: '(.+);([0-9]+)'
    replacement: '$1:$2'
    target_label: __address__
  - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
    regex: '[0-9]+'
    action: keep
  - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
    regex: '(/.*)'
    target_label: __metrics_path__
  - source_labels: [__meta_kubernetes_namespace]
    target_label: namespace
  - source_labels: [__meta_kubernetes_pod_name]
    target_label: pod
- job_name: application-kubelet
  scheme: https
  authorization:
    credentials_file: /var/run/secrets/kubernetes.io/serviceaccount/token
  tls_config:
    ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  kubernetes_sd_configs:
  - role: pod
    namespaces: {names: [%s]}
  relabel_configs:
  - source_labels: [__meta_kubernetes_pod_phase]
    regex: Running
    action: keep
  - source_labels: [__meta_kubernetes_pod_node_name]
    regex: '.+'
    action: keep
  - target_label: __address__
    replacement: kubernetes.default.svc:443
  - source_labels: [__meta_kubernetes_pod_node_name]
    target_label: instance
  - source_labels: [__meta_kubernetes_pod_node_name]
    target_label: __metrics_path__
    replacement: /api/v1/nodes/$1/proxy/metrics
- job_name: application-cadvisor
  scheme: https
  authorization:
    credentials_file: /var/run/secrets/kubernetes.io/serviceaccount/token
  tls_config:
    ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  kubernetes_sd_configs:
  - role: pod
    namespaces: {names: [%s]}
  relabel_configs:
  - source_labels: [__meta_kubernetes_pod_phase]
    regex: Running
    action: keep
  - source_labels: [__meta_kubernetes_pod_node_name]
    regex: '.+'
    action: keep
  - target_label: __address__
    replacement: kubernetes.default.svc:443
  - source_labels: [__meta_kubernetes_pod_node_name]
    target_label: instance
  - source_labels: [__meta_kubernetes_pod_node_name]
    target_label: __metrics_path__
    replacement: /api/v1/nodes/$1/proxy/metrics/cadvisor
  metric_relabel_configs:
  - source_labels: [namespace]
    regex: %s
    action: keep
`, namespace, namespace, namespace, namespace)
}

func (i Installer) ensureObservability(ctx context.Context, p Plan) error {
	if p.Prometheus == nil {
		return nil
	}
	ns := p.Prometheus.Namespace
	cms := i.Client.CoreV1().ConfigMaps(ns)
	priorCM, err := cms.Get(ctx, PrometheusName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = cms.Create(ctx, p.PrometheusConfig, metav1.CreateOptions{})
	} else if err == nil {
		want := p.PrometheusConfig.DeepCopy()
		want.ResourceVersion = priorCM.ResourceVersion
		_, err = cms.Update(ctx, want, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure Prometheus config: %w", err)
	}
	services := i.Client.CoreV1().Services(ns)
	priorService, err := services.Get(ctx, PrometheusName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = services.Create(ctx, p.PrometheusService, metav1.CreateOptions{})
	} else if err == nil {
		want := priorService.DeepCopy()
		want.Spec.Selector = p.PrometheusService.Spec.Selector
		want.Spec.Ports = p.PrometheusService.Spec.Ports
		_, err = services.Update(ctx, want, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure Prometheus service: %w", err)
	}
	deployments := i.Client.AppsV1().Deployments(ns)
	prior, err := deployments.Get(ctx, PrometheusName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = deployments.Create(ctx, p.Prometheus, metav1.CreateOptions{})
	} else if err == nil {
		want := p.Prometheus.DeepCopy()
		want.ResourceVersion = prior.ResourceVersion
		_, err = deployments.Update(ctx, want, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure Prometheus deployment: %w", err)
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
