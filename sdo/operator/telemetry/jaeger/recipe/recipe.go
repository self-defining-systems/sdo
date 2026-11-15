// Package recipe holds the object set of the SDO-owned trace store: Jaeger
// all-in-one with in-memory storage. It receives OTLP traces from applications
// directly and answers the query API `sdo traces` reads. Nothing here talks to
// a cluster; the installer applies the objects.
package recipe

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/telemetry"
)

// Name is the object name of every recipe object and the trace store Service.
const Name = "sdo-jaeger"

// Image is pinned by digest.
const Image = "docker.io/jaegertracing/all-in-one:1.62.0@sha256:836e9b69c88afbedf7683ea7162e179de63b1f981662e83f5ebb68badadc710f"

// RecipeName is the name a profile or binding uses to select this recipe.
const RecipeName = "jaeger"

// maxTraces bounds the in-memory store; the oldest traces are dropped first.
const maxTraces = "20000"

// Recipe is the SDO-owned trace store.
type Recipe struct{}

// New returns the Jaeger recipe.
func New() telemetry.BackendRecipe { return Recipe{} }

// Name implements telemetry.BackendRecipe.
func (Recipe) Name() string { return RecipeName }

// QueryURL is the in-cluster query endpoint of the recipe's Service.
func QueryURL(controlNamespace string) string {
	return fmt.Sprintf("http://%s.%s.svc:16686", Name, controlNamespace)
}

// OTLPTracesURL is the base URL applications push OTLP/HTTP traces to
// (OTEL_EXPORTER_OTLP_ENDPOINT; the SDK appends /v1/traces).
func OTLPTracesURL(controlNamespace string) string {
	return fmt.Sprintf("http://%s.%s.svc:4318", Name, controlNamespace)
}

// ThriftTracesURL is the Jaeger Thrift HTTP collector endpoint, for
// applications whose tracing library speaks Jaeger rather than OTLP.
func ThriftTracesURL(controlNamespace string) string {
	return fmt.Sprintf("http://%s.%s.svc:14268/api/traces", Name, controlNamespace)
}

// Capabilities implements telemetry.BackendRecipe: the trace store reads
// nothing from the cluster.
func (Recipe) Capabilities() []telemetry.ClusterCapability { return nil }

// Build implements telemetry.BackendRecipe.
func (Recipe) Build(p telemetry.BackendParams) telemetry.BackendObjects {
	labels := map[string]string{"app.kubernetes.io/name": Name, "app.kubernetes.io/managed-by": "sdo"}
	meta := metav1.ObjectMeta{Name: Name, Namespace: p.ControlNamespace, Labels: labels}
	replicas := int32(1)
	noToken := false
	return telemetry.BackendObjects{
		ServiceAccounts: []*corev1.ServiceAccount{{ObjectMeta: meta, AutomountServiceAccountToken: &noToken}},
		Services: []*corev1.Service{{ObjectMeta: meta, Spec: corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{
			{Name: "query", Port: 16686, TargetPort: intstr.FromInt32(16686)},
			{Name: "otlp-grpc", Port: 4317, TargetPort: intstr.FromInt32(4317)},
			{Name: "otlp-http", Port: 4318, TargetPort: intstr.FromInt32(4318)},
			{Name: "thrift-http", Port: 14268, TargetPort: intstr.FromInt32(14268)},
		}}}},
		Deployments: []*appsv1.Deployment{{
			ObjectMeta: meta,
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						ServiceAccountName:           Name,
						AutomountServiceAccountToken: &noToken,
						SecurityContext:              p.PodSecurity,
						Containers: []corev1.Container{{
							Name: "jaeger", Image: Image,
							Env: []corev1.EnvVar{
								{Name: "COLLECTOR_OTLP_ENABLED", Value: "true"},
								{Name: "SPAN_STORAGE_TYPE", Value: "memory"},
								{Name: "MEMORY_MAX_TRACES", Value: maxTraces},
							},
							Ports: []corev1.ContainerPort{
								{Name: "query", ContainerPort: 16686},
								{Name: "otlp-grpc", ContainerPort: 4317},
								{Name: "otlp-http", ContainerPort: 4318},
								{Name: "thrift-http", ContainerPort: 14268},
							},
							SecurityContext: p.ContainerSecurity,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
								Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
							},
							ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
								Path: "/", Port: intstr.FromInt32(14269),
							}}},
						}},
					},
				},
			},
		}},
	}
}
