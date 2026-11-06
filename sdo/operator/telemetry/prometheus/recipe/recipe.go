// Package recipe holds the object set of the SDO-owned Prometheus backend as typed
// Kubernetes objects. The installer applies them; nothing here talks to a cluster.
package recipe

import (
	"crypto/sha256"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/telemetry"
)

// Name is the object name of every recipe object and the SDO Prometheus Service.
const Name = "sdo-prometheus"

// Image is pinned by digest.
const Image = "quay.io/prometheus/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996"

// Recipe is the SDO-owned Prometheus backend (recipe name "prometheus").
type Recipe struct{}

// New returns the Prometheus recipe.
func New() telemetry.BackendRecipe { return Recipe{} }

// RecipeName is the name a profile or binding uses to select this recipe.
const RecipeName = "prometheus"

// Name implements telemetry.BackendRecipe.
func (Recipe) Name() string { return RecipeName }

// QueryURL is the in-cluster query endpoint of the recipe's Service.
func QueryURL(controlNamespace string) string {
	return fmt.Sprintf("http://%s.%s.svc:9090", Name, controlNamespace)
}

// OTLPMetricsURL is where applications may push OTLP metrics.
func OTLPMetricsURL(controlNamespace string) string {
	return QueryURL(controlNamespace) + "/api/v1/otlp/v1/metrics"
}

// Build implements telemetry.BackendRecipe.
func (Recipe) Build(p telemetry.BackendParams) telemetry.BackendObjects {
	ns := p.ControlNamespace
	labels := map[string]string{"app.kubernetes.io/name": Name, "app.kubernetes.io/managed-by": "sdo"}
	meta := metav1.ObjectMeta{Name: Name, Namespace: ns, Labels: labels}
	config := scrapeConfig(p.ApplicationNamespace)
	roleName := Name + "-" + ns
	replicas := int32(1)
	size := resource.MustParse("2Gi")
	return telemetry.BackendObjects{
		ConfigMaps:      []*corev1.ConfigMap{{ObjectMeta: meta, Data: map[string]string{"prometheus.yml": config}}},
		ServiceAccounts: []*corev1.ServiceAccount{{ObjectMeta: meta}},
		ClusterRoles: []*rbacv1.ClusterRole{{ObjectMeta: metav1.ObjectMeta{Name: roleName}, Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods", "nodes", "nodes/proxy", "nodes/metrics"}, Verbs: []string{"get", "list", "watch"}},
		}}},
		ClusterBindings: []*rbacv1.ClusterRoleBinding{{ObjectMeta: metav1.ObjectMeta{Name: roleName}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: roleName}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: Name, Namespace: ns}}}},
		Services:        []*corev1.Service{{ObjectMeta: meta, Spec: corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{{Name: "http", Port: 9090, TargetPort: intstr.FromInt32(9090)}}}}},
		Deployments: []*appsv1.Deployment{{
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
						ServiceAccountName: Name, SecurityContext: p.PodSecurity,
						Containers: []corev1.Container{{
							Name: "prometheus", Image: Image,
							Args: []string{
								"--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.path=/prometheus",
								"--storage.tsdb.retention.time=6h", "--storage.tsdb.retention.size=1GB",
								"--web.enable-otlp-receiver",
							},
							Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: 9090}},
							SecurityContext: p.ContainerSecurity,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
								Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
							},
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
								LocalObjectReference: corev1.LocalObjectReference{Name: Name},
							}}},
							{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &size}}},
						},
					},
				},
			},
		}},
	}
}

// Pod discovery also supplies the distinct node proxy targets. Dropping pod-specific
// labels makes all pods on one node deduplicate to one kubelet/cAdvisor scrape.
func scrapeConfig(namespace string) string {
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
